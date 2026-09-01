use crate::{
    common::{Me, NodeId},
    conversions::{
        common::v1::Addr,
        manager_api::v1::{
            Config,
            Connect,
            ConnectResponse,
            ManagerEvent
            ,
            manager_api_client::ManagerApiClient,
            manager_event::Payload},
    },
    manager::{
        domain::ManagerProtocol,
        grpc::GRPC_CONNECTION_CHANNEL_BUFFER_SIZE
        ,
        grpc::input::input_from_manager,
        grpc::session::{IOStreamExt, ManagerIOStream}},
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Streaming};

pub(super) async fn new_manager_connection(
    me: &Me,
    tx: &Sender<ManagerProtocol>,
    sessions: &Arc<RwLock<HashMap<NodeId, ManagerIOStream>>>,
    host: String,
    port: u32,
    replication_factor: usize,
) {
    let client = ManagerApiClient::connect(format!("http://{host}:{port}")).await;
    match client {
        Ok(mut client) => {
            tracing::debug!("Connecting to manager");
            let (grpc_output, rx) =
                tokio::sync::mpsc::channel::<ManagerEvent>(GRPC_CONNECTION_CHANNEL_BUFFER_SIZE);
            let outbound = ReceiverStream::new(rx);

            let response = client.open_connection(Request::new(outbound)).await;
            match response {
                Ok(response) => {
                    tracing::debug!("Connected to manager");
                    start_communication_with_manager(
                        me,
                        tx,
                        sessions,
                        host,
                        port,
                        grpc_output,
                        response,
                        replication_factor,
                    )
                    .await;
                }
                Err(e) => {
                    tracing::error!("Failed to open connection to manager: {}", e);
                }
            }
        }
        Err(e) => {
            tracing::error!("Failed to connect to manager: {}", e);
        }
    }
}

async fn start_communication_with_manager(
    me: &Me,
    tx: &Sender<ManagerProtocol>,
    sessions: &Arc<RwLock<HashMap<NodeId, ManagerIOStream>>>,
    host: String,
    port: u32,
    grpc_output: Sender<ManagerEvent>,
    response: Response<Streaming<ManagerEvent>>,
    replication_factor: usize,
) {
    let mut input_stream = response.into_inner();
    let sender = ManagerIOStream::Output(grpc_output);
    if sender
        .send(ManagerEvent {
            payload: Some(Payload::Connect(Connect {
                id: me.id.to_string(),
                addr: Some(Addr {
                    host: me.host.clone(),
                    port: me.port,
                }),
                config: Some(Config {
                    replication_factor: replication_factor as u32,
                }),
            })),
        })
        .await
        .is_ok()
    {
        if let Ok(Some(ManagerEvent {
            payload: Some(Payload::ConnectResponse(ConnectResponse { id })),
        })) = input_stream.message().await
        {
            let id: NodeId = id.into();
            sessions.write().await.insert(id.clone(), sender);
            tracing::info!("Node {} is connected", id);

            let sessions = sessions.clone();
            let tx = tx.clone();
            let me = me.clone();
            tokio::spawn(async move {
                input_from_manager(me, input_stream, &id, host, port, tx.clone()).await;
                sessions.write().await.remove(&id);
                tracing::info!("Node {} is disconnected", id);
                let _ = tx.send(ManagerProtocol::NodeDisconnected { id }).await;
            });
        } else {
            tracing::error!("Node {}:{} is not connected ", host, port);
        }
    } else {
        tracing::error!("Failed to open connection to manager");
    }
}
