use crate::common::PartitionId;
use crate::{
    common::Config,
    common::{ClusterNode, ClusterState, Heartbeat, Me, NodeId, Partitions},
    conversions::{
        common::v1::{Addr, ClusterState as GrpcClusterState, GetState, Node},
        domain_node_type_to_grpc,
        domain_partitions_to_grpc,
        manager_api::v1::{Heartbeat as GrpcHeartbeat, Leader as GrpcLeader, ManagerEvent, RemovePartitionFromReplica, VoteRequest as GrpcVoteRequest, VoteResponse as GrpcVoteResponse, WorkerEvent, manager_event::Payload, worker_event},
    },
    manager::{
        domain::ManagerProtocol,
        grpc::session::{ManagerIOStream, WorkerIOStream, handle_common},
    },
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::mpsc::{Receiver, Sender};

pub(super) async fn output(
    me: Me,
    tx: Sender<ManagerProtocol>,
    mut rx: Receiver<ManagerProtocol>,
    manager_sessions: Arc<RwLock<HashMap<NodeId, ManagerIOStream>>>,
    worker_sessions: Arc<RwLock<HashMap<NodeId, WorkerIOStream>>>,
    config: Arc<RwLock<Config>>,
) {
    while let Some(message) = rx.recv().await {
        tracing::debug!("output: {:?}", message);
        match message {
            ManagerProtocol::RemovePartitionFromReplica {
                id,
                replica_id,
                partition_id,
            } => {
                handle_output_remove_partition_from_replica(
                    &tx,
                    &manager_sessions,
                    &worker_sessions,
                    id,
                    replica_id,
                    partition_id,
                )
                .await;
            }
            ManagerProtocol::Heartbeat {
                id,
                heartbeat: Heartbeat { id: node_id, ts },
            } => {
                handle_output_heartbeat(&tx, &manager_sessions, &worker_sessions, id, node_id, ts)
                    .await;
            }
            ManagerProtocol::NewConnection {
                id: _,
                host,
                port,
                manager,
            } => {
                let replication_factor = { config.read().await.replication_factor() };
                if manager {
                    super::new_manager_connection(
                        &me,
                        &tx,
                        &manager_sessions,
                        host,
                        port,
                        replication_factor,
                    )
                    .await;
                } else {
                    tracing::error!("NewConnection is not expected to be received for worker");
                }
            }
            ManagerProtocol::GetClusterState { id } => {
                handle_output_get_cluster_state(&tx, &manager_sessions, id).await;
            }
            ManagerProtocol::ClusterState {
                recipient_id,
                state:
                    ClusterState {
                        epoch,
                        leader_id,
                        nodes: items,
                        partitions,
                    },
            } => {
                handle_output_cluster_state(
                    &tx,
                    &manager_sessions,
                    &worker_sessions,
                    recipient_id,
                    epoch,
                    leader_id,
                    items,
                    partitions,
                )
                .await;
            }
            ManagerProtocol::VoteRequest { id, epoch, ts } => {
                handle_output_vote_request(&tx, &manager_sessions, id, epoch, ts).await;
            }
            ManagerProtocol::VoteResponse { id, leader_id, ts } => {
                handle_output_vote_response(&tx, &manager_sessions, id, leader_id, ts).await;
            }
            ManagerProtocol::Leader { id, epoch, ts } => {
                handle_output_leader(&me, &tx, &manager_sessions, &worker_sessions, id, epoch, ts)
                    .await;
            }
            ManagerProtocol::NodeDisconnected { .. } => {
                unreachable!("NodeDisconnected is not expected to be sent");
            }
        }
    }
}

pub(super) async fn handle_output_leader(
    me: &Me,
    tx: &Sender<ManagerProtocol>,
    manager_sessions: &RwLock<HashMap<NodeId, ManagerIOStream>>,
    worker_sessions: &RwLock<HashMap<NodeId, WorkerIOStream>>,
    id: NodeId,
    epoch: u64,
    ts: u64,
) {
    // only leader node can send it
    let leader = || GrpcLeader {
        id: me.id.to_string(),
        epoch,
        ts,
    };
    let is_worker = worker_sessions.read().await.contains_key(&id);
    if is_worker {
        handle_common(
            "Leader",
            || WorkerEvent {
                payload: Some(worker_event::Payload::ManagerLeader(leader())),
            },
            tx,
            worker_sessions,
            id,
        )
        .await
    } else {
        handle_common(
            "Leader",
            || ManagerEvent {
                payload: Some(Payload::Leader(leader())),
            },
            tx,
            manager_sessions,
            id,
        )
        .await
    }
}

pub(super) async fn handle_output_vote_response(
    tx: &Sender<ManagerProtocol>,
    sessions: &RwLock<HashMap<NodeId, ManagerIOStream>>,
    id: NodeId,
    leader_id: NodeId,
    ts: u64,
) {
    handle_common(
        "VoteResponse",
        || ManagerEvent {
            payload: Some(Payload::VoteResponse(GrpcVoteResponse {
                leader_id: leader_id.to_string(),
                ts,
            })),
        },
        tx,
        sessions,
        id,
    )
    .await;
}

pub(super) async fn handle_output_vote_request(
    tx: &Sender<ManagerProtocol>,
    sessions: &RwLock<HashMap<NodeId, ManagerIOStream>>,
    id: NodeId,
    epoch: u64,
    ts: u64,
) {
    handle_common(
        "VoteRequest",
        || ManagerEvent {
            payload: Some(Payload::VoteRequest(GrpcVoteRequest { epoch, ts })),
        },
        tx,
        sessions,
        id,
    )
    .await;
}

pub(super) async fn handle_output_cluster_state(
    tx: &Sender<ManagerProtocol>,
    manager_sessions: &RwLock<HashMap<NodeId, ManagerIOStream>>,
    worker_sessions: &RwLock<HashMap<NodeId, WorkerIOStream>>,
    id: NodeId,
    epoch: u64,
    leader_id: NodeId,
    items: Vec<ClusterNode>,
    partitions: Partitions,
) {
    let state = || GrpcClusterState {
        epoch,
        leader_id: leader_id.to_string(),
        nodes: items
            .into_iter()
            .map(
                |ClusterNode {
                     id,
                     host,
                     port,
                     last_heartbeat,
                     node_type,
                 }| Node {
                    id: id.to_string(),
                    addr: Some(Addr { host, port }),
                    last_heartbeat,
                    node_type: domain_node_type_to_grpc(node_type),
                },
            )
            .collect(),
        partitions: Some(domain_partitions_to_grpc(partitions)),
    };

    let is_worker = worker_sessions.read().await.contains_key(&id);
    if is_worker {
        handle_common(
            "ClusterState",
            || WorkerEvent {
                payload: Some(worker_event::Payload::ClusterState(state())),
            },
            tx,
            worker_sessions,
            id,
        )
        .await;
    } else {
        handle_common(
            "ClusterState",
            || ManagerEvent {
                payload: Some(Payload::ClusterState(state())),
            },
            tx,
            manager_sessions,
            id,
        )
        .await;
    }
}

pub(super) async fn handle_output_get_cluster_state(
    tx: &Sender<ManagerProtocol>,
    sessions: &RwLock<HashMap<NodeId, ManagerIOStream>>,
    id: NodeId,
) {
    handle_common(
        "GetClusterState",
        || ManagerEvent {
            payload: Some(Payload::GetClusterState(GetState {})),
        },
        tx,
        sessions,
        id,
    )
    .await;
}

pub(super) async fn handle_output_heartbeat(
    tx: &Sender<ManagerProtocol>,
    manager_sessions: &RwLock<HashMap<NodeId, ManagerIOStream>>,
    worker_sessions: &RwLock<HashMap<NodeId, WorkerIOStream>>,
    id: NodeId,
    node_id: NodeId,
    ts: u64,
) {
    let heartbeat = || GrpcHeartbeat {
        id: node_id.to_string(),
        ts,
    };

    let is_worker = worker_sessions.read().await.contains_key(&id);
    if is_worker {
        handle_common(
            "Heartbeat",
            || WorkerEvent {
                payload: Some(worker_event::Payload::Heartbeat(heartbeat())),
            },
            tx,
            worker_sessions,
            id,
        )
        .await;
    } else {
        handle_common(
            "Heartbeat",
            || ManagerEvent {
                payload: Some(Payload::Heartbeat(heartbeat())),
            },
            tx,
            manager_sessions,
            id,
        )
        .await;
    }
}

pub(super) async fn handle_output_remove_partition_from_replica(
    tx: &Sender<ManagerProtocol>,
    manager_sessions: &RwLock<HashMap<NodeId, ManagerIOStream>>,
    worker_sessions: &RwLock<HashMap<NodeId, WorkerIOStream>>,
    recipient_id: NodeId,
    replica_id: NodeId,
    partition_id: PartitionId,
) {
    let remove_partition_from_replica = || RemovePartitionFromReplica {
        partition_id: partition_id.0 as u32,
        replica_id: replica_id.to_string(),
    };
    let is_worker = worker_sessions.read().await.contains_key(&recipient_id);
    if is_worker {
        handle_common(
            "RemovePartitionFromReplica",
            || WorkerEvent {
                payload: Some(worker_event::Payload::RemovePartitionFromReplica(
                    remove_partition_from_replica(),
                )),
            },
            tx,
            worker_sessions,
            recipient_id,
        )
        .await;
    } else {
        handle_common(
            "RemovePartitionFromReplica",
            || ManagerEvent {
                payload: Some(Payload::RemovePartitionFromReplica(
                    remove_partition_from_replica(),
                )),
            },
            tx,
            manager_sessions,
            recipient_id,
        )
        .await;
    }
}

#[cfg(test)]
mod tests;
