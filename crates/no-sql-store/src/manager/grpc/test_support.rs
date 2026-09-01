use super::session::{ManagerIOStream, WorkerIOStream};
use crate::common::{Me, NodeId};
use crate::conversions::manager_api::v1::{ManagerEvent, WorkerEvent};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::mpsc::{Receiver, Sender};

pub(super) fn node_id(id: &str) -> NodeId {
    NodeId::from_string(id)
}

pub(super) fn me(id: &str) -> Arc<Me> {
    Arc::new(Me {
        id: node_id(id),
        host: "127.0.0.1".to_string(),
        port: 7000,
    })
}

pub(super) fn manager_output_sender() -> (Sender<ManagerEvent>, Receiver<ManagerEvent>) {
    tokio::sync::mpsc::channel(4)
}

pub(super) fn worker_output_sender() -> (Sender<WorkerEvent>, Receiver<WorkerEvent>) {
    tokio::sync::mpsc::channel(4)
}

pub(super) fn manager_session(
    id: &NodeId,
    stream: ManagerIOStream,
) -> Arc<RwLock<HashMap<NodeId, ManagerIOStream>>> {
    Arc::new(RwLock::new(HashMap::from([(id.clone(), stream)])))
}

pub(super) fn worker_session(
    id: &NodeId,
    stream: WorkerIOStream,
) -> Arc<RwLock<HashMap<NodeId, WorkerIOStream>>> {
    Arc::new(RwLock::new(HashMap::from([(id.clone(), stream)])))
}
