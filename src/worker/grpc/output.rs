use crate::common::PartitionId;
use crate::conversions::worker_api;
use crate::conversions::worker_api::v1::Record;
use crate::conversions::worker_api::v1::worker_event::Payload::{
    SyncBatchRequest, SyncBatchResponse,
};
use crate::worker::domain;
use crate::worker::grpc::ClientApiWorkerIOStream;
use crate::worker::grpc::worker_connection::new_worker_connection;
use crate::worker::runtime_store::Key;
use crate::{
    common::{Heartbeat, Me, NodeId},
    conversions::{
        self,
        common::v1::GetState,
        manager_api::v1::{RemovePartitionFromReplica, WorkerEvent, worker_event},
        worker_api::v1::WorkerEvent as ClientApiWorkerEvent,
    },
    worker::{
        domain::WorkerProtocol,
        grpc::manager_connection::new_manager_connection,
        grpc::session::{IOStreamExt, WorkerIOStream},
    },
};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::mpsc::{Receiver, Sender};

pub(super) async fn output(
    me: Me,
    tx: Sender<WorkerProtocol>,
    mut rx: Receiver<WorkerProtocol>,
    manager_sessions: Arc<RwLock<HashMap<NodeId, WorkerIOStream>>>,
    worker_sessions: Arc<RwLock<HashMap<NodeId, ClientApiWorkerIOStream>>>,
) {
    while let Some(message) = rx.recv().await {
        tracing::debug!("output: {:?}", message);
        match message {
            WorkerProtocol::SyncBatchResponse {
                recipient_id,
                partition_id_to_max_applied_key,
            } => {
                handle_sync_batch_response(&tx, &worker_sessions, recipient_id, partition_id_to_max_applied_key).await;
            }
            WorkerProtocol::SyncBatch {
                recipient_id,
                request,
            } => {
                handle_sync_batch(&tx, &worker_sessions, recipient_id, request).await;
            }
            WorkerProtocol::RemovePartitionFromReplica {
                id,
                replica_id,
                partition_id,
            } => {
                handle_output_remove_partition_from_replica(
                    &tx,
                    &manager_sessions,
                    id,
                    replica_id,
                    partition_id,
                )
                .await;
            }
            WorkerProtocol::Heartbeat {
                id,
                heartbeat: Heartbeat { id: node_id, ts },
            } => {
                handle_output_heartbeat(&tx, &manager_sessions, id, node_id, ts).await;
            }
            WorkerProtocol::GetClusterState { id } => {
                handle_output_get_cluster_state(&tx, &manager_sessions, id).await;
            }
            WorkerProtocol::NewConnection {
                id: _,
                host,
                port,
                manager,
            } => {
                if manager {
                    new_manager_connection(&me, &tx, &manager_sessions, host, port).await;
                } else {
                    new_worker_connection(&me, &tx, &worker_sessions, host, port).await;
                }
            }
            WorkerProtocol::ClusterState { .. } => {
                tracing::error!("ClusterState is not expected to be sent by workers");
            }
            WorkerProtocol::Leader { .. } => {
                tracing::error!("Leader is not expected to be sent by workers");
            }
            WorkerProtocol::NodeDisconnected { .. } => {
                unreachable!("NodeDisconnected is not expected to be sent");
            }
        }
    }
}

async fn handle_sync_batch(
    tx: &Sender<WorkerProtocol>,
    worker_sessions: &Arc<RwLock<HashMap<NodeId, ClientApiWorkerIOStream>>>,
    recipient_id: NodeId,
    request: Arc<domain::SyncBatchRequest>,
) {
    handle_common(
        "SyncBatch",
        || ClientApiWorkerEvent {
            payload: Some(SyncBatchRequest(worker_api::v1::SyncBatchRequest {
                records: request
                    .records
                    .iter()
                    .map(|r| Record {
                        key: r.key.0,
                        value: r.value.clone(),
                        ttl: r.ttl,
                        creation_time: r.creation_time_ms,
                    })
                    .collect(),
            })),
        },
        tx,
        worker_sessions,
        recipient_id,
    )
    .await;
}

async fn handle_sync_batch_response(
    tx: &Sender<WorkerProtocol>,
    worker_sessions: &Arc<RwLock<HashMap<NodeId, ClientApiWorkerIOStream>>>,
    recipient_id: NodeId,
    partition_id_to_max_applied_key: HashMap<PartitionId, Key>,
) {
    handle_common(
        "SyncBatchResponse",
        || ClientApiWorkerEvent {
            payload: Some(SyncBatchResponse(worker_api::v1::SyncBatchResponse {
                partition_id_to_max_applied_key: partition_id_to_max_applied_key
                    .iter()
                    .map(|(partition_id, max_applied_key)| {
                        (partition_id.0 as u32, max_applied_key.0)
                    })
                    .collect(),
            })),
        },
        tx,
        worker_sessions,
        recipient_id,
    )
    .await;
}

pub(super) async fn handle_output_remove_partition_from_replica(
    tx: &Sender<WorkerProtocol>,
    manager_sessions: &RwLock<HashMap<NodeId, WorkerIOStream>>,
    recipient_id: NodeId,
    replica_id: NodeId,
    partition_id: PartitionId,
) {
    handle_common(
        "RemovePartitionFromReplica",
        || WorkerEvent {
            payload: Some(worker_event::Payload::RemovePartitionFromReplica(
                RemovePartitionFromReplica {
                    partition_id: partition_id.0 as u32,
                    replica_id: replica_id.to_string(),
                },
            )),
        },
        tx,
        manager_sessions,
        recipient_id,
    )
    .await;
}

pub(super) async fn handle_output_get_cluster_state(
    tx: &Sender<WorkerProtocol>,
    sessions: &RwLock<HashMap<NodeId, WorkerIOStream>>,
    id: NodeId,
) {
    handle_common(
        "GetClusterState",
        || WorkerEvent {
            payload: Some(worker_event::Payload::GetClusterState(GetState {})),
        },
        tx,
        sessions,
        id,
    )
    .await;
}

pub(super) async fn handle_output_heartbeat(
    tx: &Sender<WorkerProtocol>,
    manager_sessions: &RwLock<HashMap<NodeId, WorkerIOStream>>,
    id: NodeId,
    node_id: NodeId,
    ts: u64,
) {
    handle_common(
        "Heartbeat",
        || WorkerEvent {
            payload: Some(worker_event::Payload::Heartbeat(
                conversions::manager_api::v1::Heartbeat {
                    id: node_id.to_string(),
                    ts,
                },
            )),
        },
        tx,
        manager_sessions,
        id,
    )
    .await;
}

pub(super) async fn handle_common<Event, Error, Stream>(
    event_type: &'static str,
    event: impl FnOnce() -> Event,
    tx: &Sender<WorkerProtocol>,
    sessions: &RwLock<HashMap<NodeId, Stream>>,
    id: NodeId,
) where
    Error: Debug,
    Stream: IOStreamExt<Event, Error> + Clone,
{
    let is_closed = {
        sessions
            .read()
            .await
            .get(&id)
            .is_some_and(|sender| sender.is_closed())
    };

    if is_closed {
        tracing::debug!("Node {} is disconnected", id);
        sessions.write().await.remove(&id);
        let _ = tx.send(WorkerProtocol::NodeDisconnected { id }).await;
    } else if let Some(sender) = { sessions.read().await.get(&id).cloned() } {
        if let Err(e) = sender.send(event()).await {
            tracing::error!("Error sending {event_type} to {}: {:?}", id, e);
        }
    }
}
