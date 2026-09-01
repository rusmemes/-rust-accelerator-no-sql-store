use crate::common::{Node, NodeId, Partitions};
use std::collections::{BTreeSet, HashMap};

#[derive(Debug)]
pub(super) struct State {
    pub(super) epoch: Option<u64>,
    pub(super) elected_leader_id: Option<NodeId>,
    pub(super) nodes: HashMap<NodeId, Node>,
    pub(super) partitions: Partitions,
    pub(super) workers_with_calculated_partitions: BTreeSet<NodeId>,
}

impl State {
    pub(super) fn with_epoch(nodes: HashMap<NodeId, Node>, epoch: u64) -> Self {
        Self::new(nodes, Some(epoch))
    }

    pub(super) fn without_epoch(nodes: HashMap<NodeId, Node>) -> Self {
        Self::new(nodes, None)
    }

    pub(super) fn new(nodes: HashMap<NodeId, Node>, epoch: Option<u64>) -> Self {
        Self {
            epoch,
            elected_leader_id: None,
            nodes,
            partitions: Partitions::default(),
            workers_with_calculated_partitions: BTreeSet::new(),
        }
    }
}
