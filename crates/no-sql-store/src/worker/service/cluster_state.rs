use crate::common::{ClusterNode, Me, Node, NodeId, NodeType, Partitions, now_millis};
use crate::worker::domain::WorkerProtocol;
use crate::worker::service::state::State;
use std::collections::HashSet;

pub(super) fn handle_cluster_state(
    output: &mut Vec<WorkerProtocol>,
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
        if state.partitions != partitions {
            state.partitions_last_update_time =
                now_millis().max(state.partitions_last_update_time.saturating_add(1));
        }
        state.partitions = partitions;

        // Managers also send partitions-only updates with an empty node list.
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
                        output.push(WorkerProtocol::NewConnection {
                            id: None,
                            host,
                            port,
                            manager: match node_type {
                                NodeType::Manager => true,
                                NodeType::Worker => false,
                            },
                        });
                    }
                }
            }
        }
    }
}

pub(super) fn handle_remove_old_partition(
    state: &mut State,
    replica_id: NodeId,
    output: &mut Vec<WorkerProtocol>,
    me: &Me,
) {
    if !state.nodes.get(&replica_id).is_none() {
        output.extend(
            state
                .nodes
                .iter()
                .filter(|(key, node)| *key != &me.id && node.is_manager())
                .map(|(key, _)| WorkerProtocol::GetClusterState { id: key.clone() }),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{Partition, PartitionId};
    use crate::worker::service::state::State;
    use std::collections::{HashMap, HashSet};

    fn node_id(value: u128) -> NodeId {
        NodeId::from_string(&uuid::Uuid::from_u128(value).to_string())
    }

    fn worker_node() -> Node {
        Node {
            host: "worker.local".to_owned(),
            port: 9000,
            last_heartbeat: 0,
            node_type: NodeType::Worker,
        }
    }

    #[test]
    fn full_snapshot_removes_absent_nodes_but_keeps_self() {
        let me = Me {
            id: node_id(1),
            host: "me.local".to_owned(),
            port: 9000,
        };
        let retained = node_id(2);
        let stale = node_id(3);
        let mut state = State::new(HashMap::from([
            (me.id.clone(), worker_node()),
            (retained.clone(), worker_node()),
            (stale.clone(), worker_node()),
        ]));

        handle_cluster_state(
            &mut vec![],
            &mut state,
            1,
            retained.clone(),
            vec![ClusterNode {
                id: retained.clone(),
                host: "manager.local".to_owned(),
                port: 9001,
                last_heartbeat: 10,
                node_type: NodeType::Manager,
            }],
            Partitions::default(),
            &me,
        );

        assert!(state.nodes.contains_key(&me.id));
        assert!(state.nodes.contains_key(&retained));
        assert!(!state.nodes.contains_key(&stale));
    }

    #[test]
    fn partitions_only_snapshot_preserves_membership() {
        let me = Me {
            id: node_id(1),
            host: "me.local".to_owned(),
            port: 9000,
        };
        let peer = node_id(2);
        let mut state = State::new(HashMap::from([
            (me.id.clone(), worker_node()),
            (peer.clone(), worker_node()),
        ]));
        let partitions = Partitions {
            mapping: HashMap::from([(
                PartitionId(1),
                Partition {
                    master: me.id.clone(),
                    replicas: HashSet::new(),
                },
            )]),
            ..Default::default()
        };

        handle_cluster_state(
            &mut vec![],
            &mut state,
            1,
            peer.clone(),
            vec![],
            partitions,
            &me,
        );

        assert!(state.nodes.contains_key(&peer));
        assert!(state.partitions.mapping.contains_key(&PartitionId(1)));
    }
}
