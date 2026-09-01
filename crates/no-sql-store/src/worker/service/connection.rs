use crate::common::{Me, Node, NodeId, NodeType, now_millis};
use crate::worker::domain::WorkerProtocol;
use crate::worker::service::state::State;

pub(super) fn handle_node_disconnected(state: &mut State, id: NodeId, me: &Me) {
    if let Some(_) = state.nodes.remove(&id) {
        tracing::info!("Node disconnected: {:?}", id);
        tracing::info!("Me: {:?}", me);
        tracing::info!("State: {:?}", state);
    }
}

pub(super) fn handle_new_connection(
    output: &mut Vec<WorkerProtocol>,
    state: &mut State,
    id: Option<NodeId>,
    host: String,
    port: u32,
    me: &Me,
    manager: bool,
) {
    if let Some(id) = id {
        tracing::info!("New connection: {:?}", id);
        state.nodes.insert(
            id.clone(),
            if manager {
                output.push(WorkerProtocol::GetClusterState { id });
                Node {
                    host,
                    port,
                    last_heartbeat: now_millis(),
                    node_type: NodeType::Manager,
                }
            } else {
                Node {
                    host,
                    port,
                    last_heartbeat: now_millis(),
                    node_type: NodeType::Worker,
                }
            },
        );
        tracing::info!("Me: {:?}", me);
        tracing::info!("State: {:?}", state);
    }
}
