use crate::common::{Node, NodeId, PartitionId, Partitions};
use crate::worker::runtime_store::Key;
use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub struct State {
    pub epoch: Option<u64>,
    pub elected_leader_id: Option<NodeId>,
    pub nodes: HashMap<NodeId, Node>,
    pub partitions: Partitions,
    pub partitions_last_update_time: u64,
    pub sync: HashMap<PartitionId, HashMap<NodeId, SyncState>>,
    pub actual_nodes_sync_completion: HashMap<PartitionId, HashMap<NodeId, u64>>,
}

impl State {
    pub fn new(nodes: HashMap<NodeId, Node>) -> Self {
        Self {
            epoch: None,
            elected_leader_id: None,
            nodes,
            partitions: Partitions::default(),
            partitions_last_update_time: 0,
            sync: Default::default(),
            actual_nodes_sync_completion: Default::default(),
        }
    }

    pub fn get_currently_synced_actual_nodes(&self) -> HashMap<PartitionId, HashSet<NodeId>> {
        self.actual_nodes_sync_completion
            .iter()
            .flat_map(|(partition_id, node_id_to_time)| {
                let currently_synced_nodes = node_id_to_time
                    .iter()
                    .filter_map(|(node_id, time)| {
                        if *time == self.partitions_last_update_time {
                            Some(node_id.clone())
                        } else {
                            None
                        }
                    })
                    .collect::<HashSet<NodeId>>();
                if currently_synced_nodes.is_empty() {
                    None
                } else {
                    Some((partition_id.clone(), currently_synced_nodes))
                }
            })
            .collect()
    }
}

#[derive(Debug)]
pub struct SyncState {
    pub prev_max_key: Option<Key>,
    pub curr_max_key: Key,
    pub confirmed: bool,
    pub last_start_time: u64,
}
