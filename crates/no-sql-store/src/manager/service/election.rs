use super::{Node, State, get_random_number};
use crate::common::{Me, NodeId, NodeType};
use crate::manager::domain::ManagerProtocol;
use std::cmp::max;
use std::collections::{BTreeMap, HashSet};

#[derive(Debug)]
pub(super) enum Election {
    Mine { ts: u64, approvers: HashSet<NodeId> },
    Other { ts: u64, candidate_id: NodeId },
}

impl Election {
    fn ts(&self) -> u64 {
        match self {
            Election::Mine { ts, .. } => *ts,
            Election::Other { ts, .. } => *ts,
        }
    }
}

pub(super) fn start_election_if_needed(
    state: &mut State,
    elections: &mut BTreeMap<u64, Election>,
    me: &Me,
    output: &mut Vec<ManagerProtocol>,
) {
    if state.elected_leader_id.is_none()
        && state.nodes.len() > 1
        && let Some(epoch) = state.epoch
    {
        let curr_ts = crate::common::now_millis();
        let election = elections.last_key_value();
        let start_new = if let Some((_, last_election)) = election {
            match last_election {
                Election::Mine { ts, .. } => ts + get_random_number() < curr_ts,
                Election::Other { ts, .. } => ts + get_random_number() < curr_ts,
            }
        } else {
            true
        };

        if start_new {
            let next_epoch = election
                .map(|(last_epoch, _)| max(*last_epoch + 1, epoch + 1))
                .unwrap_or_else(|| epoch + 1);

            let election = Election::Mine {
                ts: curr_ts,
                approvers: HashSet::new(),
            };

            elections.clear();
            elections.insert(next_epoch, election);
            state
                .nodes
                .iter()
                .filter(|(key, value)| *key != &me.id && value.is_manager())
                .for_each(|(node_id, _)| {
                    output.push(ManagerProtocol::VoteRequest {
                        id: node_id.clone(),
                        epoch: next_epoch,
                        ts: curr_ts,
                    });
                });

            tracing::info!("New election started: {:?}", elections);
        }
    }
}

pub(super) fn handle_leader(
    output: &mut Vec<ManagerProtocol>,
    state: &mut State,
    id: NodeId,
    epoch: u64,
    ts: u64,
    me: &Me,
    elections: &mut BTreeMap<u64, Election>,
) {
    if state.epoch < Some(epoch) {
        elections.clear();
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
                    .map(|(key, _)| ManagerProtocol::GetClusterState { id: key.clone() }),
            );
        }
    }
}

pub(super) fn handle_vote_response(
    output: &mut Vec<ManagerProtocol>,
    state: &mut State,
    id: NodeId,
    leader_id: NodeId,
    ts: u64,
    me: &Me,
    elections: &mut BTreeMap<u64, Election>,
) {
    let approver = if let Some((
        epoch,
        Election::Mine {
            ts: election_ts, ..
        },
    )) = elections.last_key_value()
        && *election_ts == ts
        && &leader_id == &me.id
    {
        Some((*epoch, id))
    } else {
        None
    };

    if let Some((epoch, approver)) = approver {
        if let Some(Election::Mine { approvers, .. }) = elections.get_mut(&epoch) {
            approvers.insert(approver);
            let manager_count = state
                .nodes
                .iter()
                .filter(|(node_id, node)| *node_id != &me.id && node.is_manager())
                .count();

            if approvers.len() == manager_count
                && state
                    .nodes
                    .iter()
                    .filter(|(node_id, node)| *node_id != &me.id && node.is_manager())
                    .all(|(node_id, _)| approvers.contains(node_id))
            {
                state.elected_leader_id = Some(me.id.clone());
                state.epoch = Some(epoch);

                state
                    .nodes
                    .keys()
                    .filter(|&key| *key != me.id)
                    .for_each(|key| {
                        output.push(ManagerProtocol::Leader {
                            id: key.clone(),
                            epoch,
                            ts,
                        });
                    });

                elections.clear();
                tracing::info!("Me: {:?}", me);
                tracing::info!("Leader elected, State: {:?}", state);
            } else {
                tracing::info!("Leader not elected: {:?}", epoch);
            }
        }
    } else if !state.nodes.contains_key(&leader_id) {
        output.extend(
            state
                .nodes
                .iter()
                .filter(|(key, node)| *key != &me.id && node.is_manager())
                .map(|(key, _)| ManagerProtocol::GetClusterState { id: key.clone() }),
        );
    }
}

pub(super) fn handle_vote_request(
    output: &mut Vec<ManagerProtocol>,
    state: &mut State,
    id: NodeId,
    epoch: u64,
    ts: u64,
    elections: &mut BTreeMap<u64, Election>,
) {
    if state.epoch < Some(epoch) {
        let add_new = if let Some((last_epoch, last_election)) = elections.last_key_value() {
            let res = epoch > *last_epoch || ts < last_election.ts();
            if res {
                elections.clear();
            }
            res
        } else {
            true
        };
        if add_new {
            elections.insert(
                epoch,
                Election::Other {
                    ts,
                    candidate_id: id.clone(),
                },
            );
            output.push(ManagerProtocol::VoteResponse {
                id: id.clone(),
                leader_id: id,
                ts,
            });
        } else if let Some(Election::Other { ts, candidate_id }) = elections.get(&epoch) {
            output.push(ManagerProtocol::VoteResponse {
                id,
                leader_id: candidate_id.clone(),
                ts: *ts,
            });
        }
    }
}

#[cfg(test)]
mod tests;
