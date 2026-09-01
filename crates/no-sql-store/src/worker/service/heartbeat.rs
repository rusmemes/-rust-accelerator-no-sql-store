use crate::common::{Heartbeat, Me, Node, NodeId, NodeType, now_millis};
use crate::worker::domain::WorkerProtocol;
use crate::worker::service::state::State;

const HEARTBEAT_INTERVAL_MS: u64 = 200;

pub(super) fn heartbeats(state: &mut State, output: &mut Vec<WorkerProtocol>, me: &Me) {
    if let Some(Node {
        node_type: NodeType::Worker,
        last_heartbeat,
        ..
    }) = state.nodes.get_mut(&me.id)
    {
        let now = now_millis();
        if *last_heartbeat + HEARTBEAT_INTERVAL_MS <= now {
            *last_heartbeat = now;
            output.extend(state.nodes.keys().filter(|key| **key != me.id).map(|key| {
                WorkerProtocol::Heartbeat {
                    id: key.clone(),
                    heartbeat: Heartbeat {
                        id: me.id.clone(),
                        ts: now,
                    },
                }
            }));
        }
    }
}

pub(super) fn handle_heartbeat(
    output: &mut Vec<WorkerProtocol>,
    state: &mut State,
    id: NodeId,
    ts: u64,
    me: &Me,
) {
    match state.nodes.get_mut(&id) {
        None => {
            output.extend(
                state
                    .nodes
                    .iter()
                    .filter(|(key, node)| *key != &me.id && node.is_manager())
                    .map(|(key, _)| WorkerProtocol::GetClusterState { id: key.clone() }),
            );
        }
        Some(node) => {
            node.last_heartbeat = node.last_heartbeat.max(ts);
        }
    }
}
