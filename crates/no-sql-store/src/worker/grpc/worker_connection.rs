use crate::conversions::worker_api::v1::worker_api_client::WorkerApiClient;
use crate::conversions::worker_api::v1::{Connect, ConnectResponse, worker_event};
use crate::worker::grpc::ClientApiWorkerIOStream;
use crate::worker::grpc::input::input_from_worker;
use crate::{
    common::{Me, NodeId},
    conversions::{common::v1::Addr, worker_api::v1::WorkerEvent as ClientApiWorkerWorkerEvent},
    worker::{
        domain::WorkerProtocol,
        grpc::{GRPC_CONNECTION_CHANNEL_BUFFER_SIZE, session::IOStreamExt},
    },
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Streaming};

pub(super) async fn new_worker_connection(
    me: &Me,
    tx: &Sender<WorkerProtocol>,
    sessions: &Arc<RwLock<HashMap<NodeId, ClientApiWorkerIOStream>>>,
    host: String,
    port: u32,
) {
    let client = WorkerApiClient::connect(format!("http://{host}:{port}")).await;
    match client {
        Ok(mut client) => {
            tracing::debug!("Connecting to manager");
            let (grpc_output, rx) = tokio::sync::mpsc::channel::<ClientApiWorkerWorkerEvent>(
                GRPC_CONNECTION_CHANNEL_BUFFER_SIZE,
            );
            let outbound = ReceiverStream::new(rx);

            let response = client.open_worker_connection(Request::new(outbound)).await;
            match response {
                Ok(response) => {
                    tracing::debug!("Connected to manager");
                    start_communication_with_worker(
                        me,
                        tx,
                        sessions,
                        host,
                        port,
                        grpc_output,
                        response,
                    )
                    .await;
                }
                Err(e) => {
                    tracing::error!("Failed to open connection to manager: {}", e);
                }
            }
        }
        Err(e) => {
            tracing::error!("Failed to connect to worker: {}", e);
        }
    }
}

async fn start_communication_with_worker(
    me: &Me,
    tx: &Sender<WorkerProtocol>,
    sessions: &Arc<RwLock<HashMap<NodeId, ClientApiWorkerIOStream>>>,
    host: String,
    port: u32,
    grpc_output: Sender<ClientApiWorkerWorkerEvent>,
    response: Response<Streaming<ClientApiWorkerWorkerEvent>>,
) {
    let mut input_stream = response.into_inner();
    let sender = ClientApiWorkerIOStream::Output(grpc_output);
    if sender
        .send(ClientApiWorkerWorkerEvent {
            payload: Some(worker_event::Payload::Connect(Connect {
                id: me.id.to_string(),
                addr: Some(Addr {
                    host: me.host.clone(),
                    port: me.port,
                }),
            })),
        })
        .await
        .is_ok()
    {
        if let Ok(Some(ClientApiWorkerWorkerEvent {
            payload: Some(worker_event::Payload::ConnectResponse(ConnectResponse { id })),
        })) = input_stream.message().await
        {
            let id: NodeId = id.into();
            sessions.write().await.insert(id.clone(), sender);
            tracing::info!("Node {} is connected", id);

            let sessions = sessions.clone();
            let tx = tx.clone();
            let me = me.clone();

            tokio::spawn(async move {
                input_from_worker(input_stream, &id, host, port, tx.clone(), &me).await;
                sessions.write().await.remove(&id);
                tracing::info!("Node {} is disconnected", id);
                let _ = tx.send(WorkerProtocol::NodeDisconnected { id }).await;
            });
        } else {
            tracing::error!("Node {}:{} is not connected ", host, port);
        }
    } else {
        tracing::error!("Failed to open connection to worker");
    }
}
