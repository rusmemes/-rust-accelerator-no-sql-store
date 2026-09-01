use crate::{
    common::{Me, NodeId},
    conversions::{
        common::v1::Addr,
        manager_api::v1::{
            Config, Connect, ConnectResponse, ManagerEvent, WorkerEvent,
            manager_api_server::{ManagerApi, ManagerApiServer},
            manager_event::Payload,
            worker_event,
        },
    },
    manager::domain::ManagerProtocol,
};
use input::{input_from_manager, input_from_worker};
use manager_connection::new_manager_connection;
use output::output;
use session::{ManagerIOStream, WorkerIOStream};
use std::{collections::HashMap, sync::Arc};
use tokio::{
    sync::RwLock,
    sync::mpsc::{Receiver, Sender},
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, Streaming, transport::Server};

mod input;
mod manager_connection;
mod output;
mod session;

const GRPC_CONNECTION_CHANNEL_BUFFER_SIZE: usize = 32;

type OpenConnectionStream = ReceiverStream<Result<ManagerEvent, Status>>;
type OpenWorkerConnectionStream = ReceiverStream<Result<WorkerEvent, Status>>;

pub struct ManagerApiService {
    me: Me,
    manager_sessions: Arc<RwLock<HashMap<NodeId, ManagerIOStream>>>,
    worker_sessions: Arc<RwLock<HashMap<NodeId, WorkerIOStream>>>,
    tx: Sender<ManagerProtocol>,
    config: Arc<RwLock<crate::common::Config>>,
}
impl ManagerApiService {
    pub fn new(
        (tx, rx): (Sender<ManagerProtocol>, Receiver<ManagerProtocol>),
        me: Me,
        config: Arc<RwLock<crate::common::Config>>,
    ) -> Self {
        let manager_sessions: Arc<RwLock<HashMap<NodeId, ManagerIOStream>>> = Default::default();
        let manager_sessions_clone = manager_sessions.clone();
        let worker_sessions: Arc<RwLock<HashMap<NodeId, WorkerIOStream>>> = Default::default();
        let worker_sessions_clone = worker_sessions.clone();
        let tx_clone = tx.clone();
        let service = Self {
            me: me.clone(),
            manager_sessions,
            worker_sessions,
            tx,
            config: config.clone(),
        };
        tokio::spawn(output(
            me.clone(),
            tx_clone,
            rx,
            manager_sessions_clone,
            worker_sessions_clone,
            config,
        ));
        service
    }
}

#[tonic::async_trait]
impl ManagerApi for ManagerApiService {
    type OpenConnectionStream = OpenConnectionStream;

    async fn open_connection(
        &self,
        request: Request<Streaming<ManagerEvent>>,
    ) -> Result<Response<Self::OpenConnectionStream>, Status> {
        tracing::info!("Received open_connection request");

        let remote_addr = request.remote_addr();
        let mut input_stream: Streaming<ManagerEvent> = request.into_inner();
        let (grpc_tx, rx) = tokio::sync::mpsc::channel(GRPC_CONNECTION_CHANNEL_BUFFER_SIZE);

        let manager_sessions = self.manager_sessions.clone();
        let tx = self.tx.clone();
        let me = self.me.clone();
        let config = self.config.clone();

        tokio::spawn(async move {
            if let Ok(Some(ManagerEvent {
                payload:
                    Some(Payload::Connect(Connect {
                        id,
                        addr: Some(Addr { host, port }),
                        config: Some(Config { replication_factor }),
                    })),
            })) = input_stream.message().await
            {
                {
                    let mut guard = config.write().await;
                    *guard.replication_factor_mut() = replication_factor as usize;
                }

                let id: NodeId = id.into();
                manager_sessions
                    .write()
                    .await
                    .insert(id.clone(), ManagerIOStream::Input(grpc_tx.clone()));

                tokio::spawn(async move {
                    if grpc_tx
                        .send(Ok(ManagerEvent {
                            payload: Some(Payload::ConnectResponse(ConnectResponse {
                                id: me.id.to_string(),
                            })),
                        }))
                        .await
                        .is_ok()
                    {
                        input_from_manager(me, input_stream, &id, host, port, tx.clone()).await;
                    }
                    manager_sessions.write().await.remove(&id);
                    tracing::info!("Node {} is disconnected", id);
                    let _ = tx.send(ManagerProtocol::NodeDisconnected { id }).await;
                });
            } else {
                tracing::error!("Failed to read Connect message from {:?}", remote_addr);
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
                        ..
                    })),
            })) = input_stream.message().await
            {
                let id: NodeId = id.into();
                worker_sessions
                    .write()
                    .await
                    .insert(id.clone(), WorkerIOStream::Input(grpc_tx.clone()));

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
                        input_from_worker(input_stream, &id, host, port, tx.clone()).await;
                    }
                    worker_sessions.write().await.remove(&id);
                    tracing::info!("Node {} is disconnected", id);
                    let _ = tx.send(ManagerProtocol::NodeDisconnected { id }).await;
                });
            } else {
                tracing::error!("Failed to read Connect message from {:?}", remote_addr);
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

pub async fn start_server(
    config: Arc<RwLock<crate::common::Config>>,
    me: Me,
    channel: (Sender<ManagerProtocol>, Receiver<ManagerProtocol>),
    cancellation_token: CancellationToken,
) -> anyhow::Result<()> {
    let grpc_port = { config.read().await.grpc_port() };
    let grpc_address = format!("0.0.0.0:{grpc_port}").parse()?;

    tracing::info!("GRPC Server is starting at {}", grpc_address);

    Server::builder()
        .add_service(ManagerApiServer::new(ManagerApiService::new(
            channel, me, config,
        )))
        .serve_with_shutdown(grpc_address, cancellation_token.cancelled())
        .await?;

    tracing::info!("GRPC Server is stopped");

    Ok(())
}

#[cfg(test)]
mod test_support;
