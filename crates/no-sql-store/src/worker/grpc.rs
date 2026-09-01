mod input;
mod manager_connection;
mod output;
mod session;
mod worker_connection;

use crate::worker::runtime_store::{Key, Record};
use crate::{
    common::{CommunicationStreamEither, Me, NodeId},
    conversions::{
        common::v1::Addr,
        worker_api::v1::{
            ClientEvent, ClientRequest, Connect, ConnectResponse, RequestType, WorkerEvent,
            WorkerResponse,
            client_event::Payload,
            worker_api_server::{WorkerApi, WorkerApiServer},
            worker_event,
        },
    },
    worker::runtime_store::RuntimeStore,
    worker::{
        domain::WorkerProtocol,
        grpc::{input::input_from_worker, output::output},
    },
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, Streaming, transport::Server};

const GRPC_CONNECTION_CHANNEL_BUFFER_SIZE: usize = 32;

pub(super) type ClientApiWorkerIOStream =
    CommunicationStreamEither<Sender<Result<WorkerEvent, Status>>, Sender<WorkerEvent>>;

type OpenWorkerConnectionStream = ReceiverStream<Result<WorkerEvent, Status>>;
type OpenClientConnectionStream = ReceiverStream<Result<ClientEvent, Status>>;

pub struct WorkerApiService {
    me: Me,
    worker_sessions: Arc<RwLock<HashMap<NodeId, ClientApiWorkerIOStream>>>,
    tx: Sender<WorkerProtocol>,
    runtime_store: RuntimeStore,
}

impl WorkerApiService {
    pub fn new(
        (tx, rx): (Sender<WorkerProtocol>, Receiver<WorkerProtocol>),
        me: Me,
        runtime_store: RuntimeStore,
    ) -> Self {
        let worker_sessions: Arc<RwLock<HashMap<NodeId, ClientApiWorkerIOStream>>> =
            Default::default();
        let worker_sessions_clone = worker_sessions.clone();
        let tx_clone = tx.clone();
        let service = Self {
            me: me.clone(),
            worker_sessions,
            tx,
            runtime_store,
        };
        tokio::spawn(output(
            me.clone(),
            tx_clone,
            rx,
            Arc::new(RwLock::new(HashMap::new())),
            worker_sessions_clone,
        ));
        service
    }
}

#[tonic::async_trait]
impl WorkerApi for WorkerApiService {
    type OpenClientConnectionStream = OpenClientConnectionStream;

    async fn open_client_connection(
        &self,
        request: Request<Streaming<ClientEvent>>,
    ) -> Result<Response<Self::OpenClientConnectionStream>, Status> {
        tracing::info!("Received open_client_connection request");

        let mut input_stream: Streaming<ClientEvent> = request.into_inner();
        let (grpc_tx, rx): (
            Sender<Result<ClientEvent, Status>>,
            Receiver<Result<ClientEvent, Status>>,
        ) = tokio::sync::mpsc::channel(GRPC_CONNECTION_CHANNEL_BUFFER_SIZE);

        let runtime_store = self.runtime_store.clone();
        tokio::spawn(async move {
            while let Ok(Some(event)) = input_stream.message().await {
                if let ClientEvent {
                    payload: Some(Payload::Request(request)),
                } = event
                {
                    let (request_id, value) = match request {
                        ClientRequest {
                            id,
                            request_type,
                            key,
                            value: Some(bytes),
                            ttl,
                            creation_time: Some(creation_time_ms),
                        } if request_type == RequestType::Put as i32 => {
                            runtime_store.put(Key(key), Record {
                                value: bytes,
                                expiration_time_ms: ttl.unwrap_or(0),
                                creation_time_ms
                            });
                            (id, None)
                        },
                        ClientRequest {
                            id,
                            request_type,
                            key,
                            ..
                        } if request_type == RequestType::Delete as i32 => {
                            runtime_store.delete(Key(key));
                            (id, None)
                        },
                        ClientRequest {
                            id,
                            key,
                            ..
                        } /* considered as Get request */ => {
                            let option = runtime_store.get(Key(key));
                            (id, option.map(|r| r.value.clone()))
                        },
                    };

                    if let Err(e) = grpc_tx
                        .send(Ok(ClientEvent {
                            payload: Some(Payload::Response(WorkerResponse { request_id, value })),
                        }))
                        .await
                    {
                        tracing::error!("Failed to send response: {:?}", e);
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type OpenWorkerConnectionStream = OpenWorkerConnectionStream;

    async fn open_worker_connection(
        &self,
        request: Request<Streaming<WorkerEvent>>,
    ) -> Result<Response<Self::OpenWorkerConnectionStream>, Status> {
        tracing::info!("Received open_worker_connection request");

        let remote_addr = request.remote_addr();
        let mut input_stream: Streaming<WorkerEvent> = request.into_inner();

        let (grpc_tx, rx): (
            Sender<Result<WorkerEvent, Status>>,
            Receiver<Result<WorkerEvent, Status>>,
        ) = tokio::sync::mpsc::channel(GRPC_CONNECTION_CHANNEL_BUFFER_SIZE);

        let worker_sessions = self.worker_sessions.clone();
        let tx = self.tx.clone();
        let me = self.me.clone();

        tokio::spawn(async move {
            if let Ok(Some(WorkerEvent {
                payload:
                    Some(worker_event::Payload::Connect(Connect {
                        id,
                        addr: Some(Addr { host, port }),
                    })),
            })) = input_stream.message().await
            {
                let id: NodeId = id.into();
                worker_sessions
                    .write()
                    .await
                    .insert(id.clone(), ClientApiWorkerIOStream::Input(grpc_tx.clone()));

                tokio::spawn(async move {
                    if grpc_tx
                        .send(Ok(WorkerEvent {
                            payload: Some(worker_event::Payload::ConnectResponse(
                                ConnectResponse {
                                    id: me.id.to_string(),
                                },
                            )),
                        }))
                        .await
                        .is_ok()
                    {
                        input_from_worker(input_stream, &id, host, port, tx.clone(), &me).await;
                    }
                    worker_sessions.write().await.remove(&id);
                    tracing::info!("Node {} is disconnected", id);
                    let _ = tx.send(WorkerProtocol::NodeDisconnected { id }).await;
                });
            } else {
                tracing::error!("Failed to read Connect message from {:?}", remote_addr);
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

pub async fn start_server(
    config: crate::common::Config,
    me: Me,
    channel: (Sender<WorkerProtocol>, Receiver<WorkerProtocol>),
    cancellation_token: CancellationToken,
    runtime_store: RuntimeStore,
) -> anyhow::Result<()> {
    let grpc_address = format!("0.0.0.0:{}", config.grpc_port()).parse()?;

    tracing::info!("GRPC Server is starting at {}", grpc_address);

    Server::builder()
        .add_service(WorkerApiServer::new(WorkerApiService::new(
            channel,
            me,
            runtime_store,
        )))
        .serve_with_shutdown(grpc_address, cancellation_token.cancelled())
        .await?;

    tracing::info!("GRPC Server is stopped");

    Ok(())
}
