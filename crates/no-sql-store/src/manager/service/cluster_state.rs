use super::{Node, State};
use crate::common::{ClusterNode, ClusterState, Me, NodeId, NodeType, PartitionId, Partitions};
use crate::manager::domain::ManagerProtocol;
use std::collections::HashSet;

pub(super) fn handle_cluster_state(
    output: &mut Vec<ManagerProtocol>,
    state: &mut State,
    epoch: u64,
    leader_id: NodeId,
    items: Vec<ClusterNode>,
    partitions: Partitions,
    me: &Me,
) {
    let accept: bool = if state.epoch.is_none() || state.epoch < Some(epoch) {
        state.epoch = Some(epoch);
        state.elected_leader_id = Some(leader_id);
        true
    } else if state.epoch == Some(epoch) && state.elected_leader_id == Some(leader_id) {
        true
    } else {
        false
    };

    if accept {
        state.partitions = partitions;

        // An empty node list is a partitions-only update. A non-empty list is
        // a complete membership snapshot and supersedes the local view.
        if !items.is_empty() {
            let snapshot_node_ids: HashSet<_> = items.iter().map(|item| item.id.clone()).collect();
            state
                .nodes
                .retain(|id, _| id == &me.id || snapshot_node_ids.contains(id));
        }

        for item in items {
            match item {
                ClusterNode {
                    id,
                    host,
                    port,
                    last_heartbeat,
                    node_type,
                } => {
                    if let Some(Node {
                        last_heartbeat: node_last_heartbeat,
                        ..
                    }) = state.nodes.get_mut(&id)
                    {
                        if *node_last_heartbeat < last_heartbeat {
                            *node_last_heartbeat = last_heartbeat;
                        }
                    } else {
                        match node_type {
                            NodeType::Manager => output.push(ManagerProtocol::NewConnection {
                                id: None,
                                host,
                                port,
                                manager: true,
                            }),
                            NodeType::Worker => {
                                state.nodes.insert(
                                    id,
                                    Node {
                                        host,
                                        port,
                                        last_heartbeat,
                                        node_type,
                                    },
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

pub(super) fn handle_get_cluster_state(
    output: &mut Vec<ManagerProtocol>,
    state: &mut State,
    id: NodeId,
) {
    if let Some((epoch, leader_id)) = state.epoch.zip(state.elected_leader_id.clone()) {
        output.push(ManagerProtocol::ClusterState {
            recipient_id: id.clone(),
            state: ClusterState {
                epoch,
                leader_id,
                nodes: state
                    .nodes
                    .iter()
                    .map(
                        |(
                            id,
                            Node {
                                host,
                                port,
                                last_heartbeat,
                                node_type,
                            },
                        )| ClusterNode {
                            id: id.clone(),
                            host: host.clone(),
                            port: *port,
                            last_heartbeat: *last_heartbeat,
                            node_type: *node_type,
                        },
                    )
                    .collect(),
                partitions: Partitions {
                    mapping: state.partitions.mapping.clone(),
                    old_replicas: state.partitions.old_replicas.clone(),
                    new_replicas: state.partitions.new_replicas.clone(),
                },
            },
        });
    }
}

pub(super) fn handle_remove_old_partition(
    output: &mut Vec<ManagerProtocol>,
    state: &mut State,
    id: NodeId,
    replica_id: NodeId,
    partition_id: PartitionId,
    me: &Me,
) {
    if let Some(old_replicas) = state.partitions.old_replicas.get_mut(&partition_id) {
        if old_replicas.remove(&replica_id) && old_replicas.is_empty() {
            state.partitions.old_replicas.remove(&partition_id);
        }
    }

    if state.elected_leader_id.as_ref() == Some(&me.id) {
        for recipient_id in state
            .nodes
            .keys()
            .filter(|key| *key != &replica_id && *key != &id && *key != &me.id)
        {
            output.push(ManagerProtocol::RemovePartitionFromReplica {
                id: recipient_id.clone(),
                replica_id: replica_id.clone(),
                partition_id,
            });
        }
    }
}

#[cfg(test)]
mod tests;
