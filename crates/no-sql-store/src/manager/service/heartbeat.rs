use super::{Node, State, get_random_number};
use crate::common::{Heartbeat, Me, NodeId, NodeType, now_millis};
use crate::manager::domain::ManagerProtocol;

const HEARTBEAT_INTERVAL_MS: u64 = 200;

pub(super) fn heartbeats(state: &mut State, output: &mut Vec<ManagerProtocol>, me: &Me) {
    if let Some(Node {
        node_type: NodeType::Manager,
        last_heartbeat,
        ..
    }) = state.nodes.get_mut(&me.id)
    {
        let now = now_millis();
        if *last_heartbeat + HEARTBEAT_INTERVAL_MS <= now {
            *last_heartbeat = now;
            output.extend(state.nodes.keys().filter(|key| **key != me.id).map(|key| {
                ManagerProtocol::Heartbeat {
                    id: key.clone(),
                    heartbeat: Heartbeat {
                        id: me.id.clone(),
                        ts: now,
                    },
                }
            }));
        }

        if state.elected_leader_id.is_some() && state.elected_leader_id.as_ref() != Some(&me.id) {
            if let Some(Node {
                node_type: NodeType::Manager,
                last_heartbeat,
                ..
            }) = state.nodes.get_mut(
                &state
                    .elected_leader_id
                    .as_ref()
                    .expect("elected leader id is Some"),
            ) {
                if *last_heartbeat + get_random_number() < now {
                    state.elected_leader_id = None;
                }
            }
        }
    }
}

pub(super) fn handle_heartbeat(
    output: &mut Vec<ManagerProtocol>,
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
                    .map(|(key, _)| ManagerProtocol::GetClusterState { id: key.clone() }),
            );
        }
        Some(node) => {
            node.last_heartbeat = node.last_heartbeat.max(ts);
            if state.elected_leader_id.as_ref() == Some(&me.id) {
                output.extend(
                    state
                        .nodes
                        .keys()
                        .filter(|key| *key != &id && *key != &me.id)
                        .map(|key| ManagerProtocol::Heartbeat {
                            id: key.clone(),
                            heartbeat: Heartbeat { id: id.clone(), ts },
                        }),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests;
