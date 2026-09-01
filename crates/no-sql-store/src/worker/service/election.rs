use crate::common::{Me, Node, NodeId, NodeType};
use crate::worker::domain::WorkerProtocol;
use crate::worker::service::state::State;

pub(super) fn handle_leader(
    output: &mut Vec<WorkerProtocol>,
    state: &mut State,
    id: NodeId,
    epoch: u64,
    ts: u64,
    me: &Me,
) {
    if state.epoch < Some(epoch) {
        if let Some(Node {
            last_heartbeat,
            node_type: NodeType::Manager,
            ..
        }) = state.nodes.get_mut(&id)
        {
            *last_heartbeat = ts;
            state.elected_leader_id = Some(id);
            state.epoch = Some(epoch);
            tracing::info!("Me: {:?}", me);
            tracing::info!("Leader elected, State: {:?}", state);
        } else {
            output.extend(
                state
                    .nodes
                    .iter()
                    .filter(|(key, node)| *key != &me.id && node.is_manager())
                    .map(|(key, _)| WorkerProtocol::GetClusterState { id: key.clone() }),
            );
        }
    }
}
