use crate::common::{ClusterState, Heartbeat, NodeId, PartitionId};
use crate::worker::runtime_store::Key;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub enum WorkerProtocol {
    NewConnection {
        id: Option<NodeId>,
        host: String,
        port: u32,
        manager: bool,
    },
    Heartbeat {
        id: NodeId,
        heartbeat: Heartbeat,
    },
    GetClusterState {
        id: NodeId,
    },
    ClusterState {
        state: ClusterState,
    },
    NodeDisconnected {
        id: NodeId,
    },
    Leader {
        id: NodeId,
        epoch: u64,
        ts: u64,
    },
    RemovePartitionFromReplica {
        id: NodeId,
        replica_id: NodeId,
        partition_id: PartitionId,
    },
    SyncBatch {
        recipient_id: NodeId,
        request: Arc<SyncBatchRequest>,
    },
    SyncBatchResponse {
        recipient_id: NodeId,
        partition_id_to_max_applied_key: HashMap<PartitionId, Key>,
    },
}

#[derive(Debug, Clone)]
pub struct SyncBatchRequest {
    pub sender_id: NodeId,
    pub records: Vec<Record>,
}

#[derive(Debug, Clone)]
pub struct Record {
    pub key: Key,
    pub value: Vec<u8>,
    pub ttl: u64,
    pub creation_time_ms: u64,
}
