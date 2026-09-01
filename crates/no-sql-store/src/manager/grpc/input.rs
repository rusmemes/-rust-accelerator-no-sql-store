use crate::common::PartitionId;
use crate::{
    common::{ClusterNode, ClusterState, Heartbeat, Me, NodeId},
    conversions::{
        common::v1::{Addr, ClusterState as GrpcClusterState, Node},
        grpc_node_type_to_domain,
        grpc_partitions_to_domain,
        manager_api::v1::{
            Connect as GrpcConnect,
            Heartbeat as GrpcHeartbeat,
            Leader as GrpcLeader,
            ManagerEvent,
            RemovePartitionFromReplica,
            VoteRequest as GrpcVoteRequest,
            VoteResponse as GrpcVoteResponse,
            WorkerEvent
            ,
            manager_event::Payload,
            worker_event},
    },
    manager::domain::ManagerProtocol,
};
use tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;
use tonic::{Status, Streaming};

pub(super) async fn input_from_worker<S>(
    mut input: S,
    id: &NodeId,
    host: String,
    port: u32,
    tx: Sender<ManagerProtocol>,
) where
    S: tokio_stream::Stream<Item = Result<WorkerEvent, Status>> + Unpin,
{
    if tx
        .send(ManagerProtocol::NewConnection {
            id: Some(id.clone()),
            host,
            port,
            manager: false,
        })
        .await
        .is_ok()
    {
        tracing::info!("Worker with ID {} is connected", id);
        while let Some(Ok(WorkerEvent {
            payload: Some(payload),
        })) = input.next().await
        {
            match payload {
                worker_event::Payload::RemovePartitionFromReplica(RemovePartitionFromReplica {
                    replica_id,
                    partition_id,
                }) => {
                    if let Err(e) = tx
                        .send(ManagerProtocol::RemovePartitionFromReplica {
                            id: id.clone(),
                            replica_id: replica_id.into(),
                            partition_id: PartitionId(partition_id as u16),
                        })
                        .await
                    {
                        tracing::error!("Error processing RemoveOldPartition signal: {}", e);
                        break;
                    }
                }
                worker_event::Payload::Heartbeat(GrpcHeartbeat { id: node_id, ts }) => {
                    if let Err(e) = tx
                        .send(ManagerProtocol::Heartbeat {
                            id: id.clone(),
                            heartbeat: Heartbeat {
                                id: node_id.into(),
                                ts,
                            },
                        })
                        .await
                    {
                        tracing::error!("Error processing Heartbeat signal: {}", e);
                        break;
                    }
                }
                worker_event::Payload::GetClusterState(_) => {
                    if let Err(e) = tx
                        .send(ManagerProtocol::GetClusterState { id: id.clone() })
                        .await
                    {
                        tracing::error!("Error processing GetClusterState request: {}", e);
                        break;
                    }
                }
                worker_event::Payload::ClusterState(_) => {
                    tracing::error!("ClusterState is not expected to be received");
                    break;
                }
                worker_event::Payload::ManagerLeader(_) => {
                    tracing::error!("ManagerLeader is not expected to be received");
                    break;
                }
                worker_event::Payload::Connect(GrpcConnect { id: request_id, .. }) => {
                    tracing::error!(
                        "Received duplicated connect request from {}: id {}",
                        id,
                        request_id
                    );
                    break;
                }
                worker_event::Payload::ConnectResponse(_) => {
                    tracing::error!("ConnectResponse is not expected to be received");
                    break;
                }
            }
        }
    }
}

pub(super) async fn input_from_manager(
    me: Me,
    mut input: Streaming<ManagerEvent>,
    id: &NodeId,
    host: String,
    port: u32,
    tx: Sender<ManagerProtocol>,
) {
    if tx
        .send(ManagerProtocol::NewConnection {
            id: Some(id.clone()),
            host,
            port,
            manager: true,
        })
        .await
        .is_ok()
    {
        tracing::info!("Manager with ID {} is connected", id);
        while let Ok(Some(ManagerEvent {
            payload: Some(payload),
        })) = input.message().await
        {
            match payload {
                Payload::RemovePartitionFromReplica(RemovePartitionFromReplica {
                    replica_id,
                    partition_id,
                }) => {
                    if let Err(e) = tx
                        .send(ManagerProtocol::RemovePartitionFromReplica {
                            id: id.clone(),
                            replica_id: replica_id.into(),
                            partition_id: PartitionId(partition_id as u16),
                        })
                        .await
                    {
                        tracing::error!("Error processing RemoveOldPartition signal: {}", e);
                        break;
                    }
                }
                Payload::Heartbeat(GrpcHeartbeat { id: node_id, ts }) => {
                    if let Err(e) = tx
                        .send(ManagerProtocol::Heartbeat {
                            id: id.clone(),
                            heartbeat: Heartbeat {
                                id: node_id.into(),
                                ts,
                            },
                        })
                        .await
                    {
                        tracing::error!("Error processing Heartbeat signal: {}", e);
                        break;
                    }
                }
                Payload::GetClusterState(_) => {
                    if let Err(e) = tx
                        .send(ManagerProtocol::GetClusterState { id: id.clone() })
                        .await
                    {
                        tracing::error!("Error processing GetClusterState request: {}", e);
                        break;
                    }
                }
                Payload::ClusterState(GrpcClusterState {
                    epoch,
                    leader_id,
                    nodes,
                    partitions,
                }) => {
                    if let Some(partitions) = partitions {
                        if let Err(e) = tx
                            .send(ManagerProtocol::ClusterState {
                                recipient_id: me.id.clone(),
                                state: ClusterState {
                                    epoch,
                                    leader_id: leader_id.into(),
                                    partitions: grpc_partitions_to_domain(partitions),
                                    nodes: nodes
                                        .into_iter()
                                        .filter_map(|node| {
                                            if let Node {
                                                id,
                                                addr: Some(Addr { host, port }),
                                                last_heartbeat,
                                                node_type,
                                            } = node
                                            {
                                                Some(ClusterNode {
                                                    id: id.into(),
                                                    host,
                                                    port,
                                                    last_heartbeat,
                                                    node_type: grpc_node_type_to_domain(node_type),
                                                })
                                            } else {
                                                None
                                            }
                                        })
                                        .collect(),
                                },
                            })
                            .await
                        {
                            tracing::error!("Error processing ClusterState response: {}", e);
                            break;
                        }
                    } else {
                        tracing::error!("Received ClusterState response with no partitions");
                        break;
                    }
                }
                Payload::VoteRequest(GrpcVoteRequest { epoch, ts }) => {
                    if let Err(e) = tx
                        .send(ManagerProtocol::VoteRequest {
                            id: id.clone(),
                            epoch,
                            ts,
                        })
                        .await
                    {
                        tracing::error!("Error processing VoteRequest request: {}", e);
                        break;
                    }
                }
                Payload::VoteResponse(GrpcVoteResponse { leader_id, ts }) => {
                    if let Err(e) = tx
                        .send(ManagerProtocol::VoteResponse {
                            id: id.clone(),
                            leader_id: leader_id.into(),
                            ts,
                        })
                        .await
                    {
                        tracing::error!("Error processing VoteResponse response: {}", e);
                        break;
                    }
                }
                Payload::Leader(GrpcLeader { id, epoch, ts }) => {
                    if let Err(e) = tx
                        .send(ManagerProtocol::Leader {
                            id: id.into(),
                            epoch,
                            ts,
                        })
                        .await
                    {
                        tracing::error!("Error processing Leader notification: {}", e);
                        break;
                    }
                }
                Payload::Connect(GrpcConnect { id: request_id, .. }) => {
                    tracing::error!(
                        "Received duplicated connect request from {}: id {}",
                        id,
                        request_id
                    );
                    break;
                }
                Payload::ConnectResponse(_) => {
                    tracing::error!("ConnectResponse is not expected to be received");
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
