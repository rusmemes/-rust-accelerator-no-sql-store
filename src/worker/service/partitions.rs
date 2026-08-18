use crate::common::{Me, Node, NodeId, PARTITIONS_AMOUNT, PartitionId, Partitions, now_millis};
use crate::worker::domain::{SyncBatchRequest, WorkerProtocol};
use crate::worker::runtime_store::{Key, RuntimeStore};
use crate::worker::service::state::{State, SyncState};
use crate::worker::{domain, runtime_store};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

const SYNC_TIMEOUT_MS: u64 = 60000;

pub fn handle_sync_batch_response(
    state: &mut State,
    partition_id_to_max_applied_key: HashMap<PartitionId, Key>,
    sender_id: NodeId,
) {
    for (partition_id, max_applied_record_key) in partition_id_to_max_applied_key {
        if let Some(node_id_to_state) = state.sync.get_mut(&partition_id) {
            if let Some(SyncState {
                curr_max_key,
                confirmed,
                ..
            }) = node_id_to_state.get_mut(&sender_id)
            {
                if *curr_max_key == max_applied_record_key {
                    *confirmed = true;
                }
            }
        }
    }
}

pub fn sync_partitions(
    state: &mut State,
    output: &mut Vec<WorkerProtocol>,
    runtime_store: &RuntimeStore,
    me: &Me,
) {
    let currently_synced_nodes = state.get_currently_synced_actual_nodes();

    for partition_id in 0..PARTITIONS_AMOUNT {
        let partition_id = PartitionId(partition_id as u16);
        if runtime_store
            .get_partition_records(&partition_id, 1, None)
            .is_empty()
        {
            continue;
        }

        let recipient_ids = get_node_ids_curr_node_has_to_sync_the_partition_to(
            partition_id,
            &me.id,
            &state.partitions,
            &state.nodes,
            currently_synced_nodes.get(&partition_id),
        )
        .into_iter()
        .cloned()
        .collect();

        sync_partition(
            state,
            output,
            runtime_store,
            me,
            partition_id,
            recipient_ids,
        );
    }

    remove_completed_syncs_and_obsolete_partitions(state, runtime_store, me);
}

fn sync_partition(
    state: &mut State,
    output: &mut Vec<WorkerProtocol>,
    runtime_store: &RuntimeStore,
    me: &Me,
    partition_id: PartitionId,
    recipient_ids: HashSet<NodeId>,
) {
    for recipient_id in recipient_ids {
        let completed = sync_partition_to_recipient(
            state.sync.entry(partition_id).or_default(),
            output,
            runtime_store,
            me,
            partition_id,
            &recipient_id,
        );

        if completed {
            record_completed_sync(state, output, me, partition_id, recipient_id);
        }
    }

    if let Some(recipient_states) = state.sync.get_mut(&partition_id) {
        recipient_states.retain(|_, sync_state| !sync_state.confirmed);
    }
}

fn sync_partition_to_recipient(
    recipient_states: &mut HashMap<NodeId, SyncState>,
    output: &mut Vec<WorkerProtocol>,
    runtime_store: &RuntimeStore,
    me: &Me,
    partition_id: PartitionId,
    recipient_id: &NodeId,
) -> bool {
    let Some(sync_state) = recipient_states.get_mut(recipient_id) else {
        start_sync(
            recipient_states,
            output,
            runtime_store,
            me,
            partition_id,
            recipient_id,
        );
        return false;
    };

    if sync_state.confirmed {
        return continue_confirmed_sync(
            sync_state,
            output,
            runtime_store,
            me,
            partition_id,
            recipient_id,
        );
    }

    retry_timed_out_sync(
        sync_state,
        output,
        runtime_store,
        me,
        partition_id,
        recipient_id,
    );
    false
}

fn start_sync(
    recipient_states: &mut HashMap<NodeId, SyncState>,
    output: &mut Vec<WorkerProtocol>,
    runtime_store: &RuntimeStore,
    me: &Me,
    partition_id: PartitionId,
    recipient_id: &NodeId,
) {
    if let Some(curr_max_key) =
        sync_batch(output, runtime_store, recipient_id, &partition_id, None, me)
    {
        recipient_states.insert(
            recipient_id.clone(),
            SyncState {
                prev_max_key: None,
                curr_max_key,
                confirmed: false,
                last_start_time: now_millis(),
            },
        );
    }
}

fn continue_confirmed_sync(
    sync_state: &mut SyncState,
    output: &mut Vec<WorkerProtocol>,
    runtime_store: &RuntimeStore,
    me: &Me,
    partition_id: PartitionId,
    recipient_id: &NodeId,
) -> bool {
    if let Some(new_max_key) = sync_batch(
        output,
        runtime_store,
        recipient_id,
        &partition_id,
        Some(&sync_state.curr_max_key),
        me,
    ) {
        sync_state.prev_max_key = Some(sync_state.curr_max_key);
        sync_state.curr_max_key = new_max_key;
        sync_state.confirmed = false;
        sync_state.last_start_time = now_millis();
        false
    } else {
        sync_state.confirmed = true;
        true
    }
}

fn retry_timed_out_sync(
    sync_state: &mut SyncState,
    output: &mut Vec<WorkerProtocol>,
    runtime_store: &RuntimeStore,
    me: &Me,
    partition_id: PartitionId,
    recipient_id: &NodeId,
) {
    if now_millis() - sync_state.last_start_time < SYNC_TIMEOUT_MS {
        return;
    }

    if let Some(new_max_key) = sync_batch(
        output,
        runtime_store,
        recipient_id,
        &partition_id,
        sync_state.prev_max_key.as_ref(),
        me,
    ) {
        sync_state.curr_max_key = new_max_key;
        sync_state.confirmed = false;
        sync_state.last_start_time = now_millis();
    }
}

fn record_completed_sync(
    state: &mut State,
    output: &mut Vec<WorkerProtocol>,
    me: &Me,
    partition_id: PartitionId,
    recipient_id: NodeId,
) {
    if node_owns_partition(&state.partitions, partition_id, &me.id) {
        state
            .actual_nodes_sync_completion
            .entry(partition_id)
            .or_default()
            .insert(recipient_id, state.partitions_last_update_time);
    }

    if let Some(leader_id) = state.elected_leader_id.as_ref() {
        output.push(WorkerProtocol::RemovePartitionFromReplica {
            id: leader_id.clone(),
            replica_id: me.id.clone(),
            partition_id,
        });
    }
}

fn remove_completed_syncs_and_obsolete_partitions(
    state: &mut State,
    runtime_store: &RuntimeStore,
    me: &Me,
) {
    let partitions = &state.partitions;
    state.sync.retain(|partition_id, sync_state| {
        if sync_state.is_empty() {
            if !node_owns_partition(partitions, *partition_id, &me.id) {
                runtime_store.remove_partition(partition_id);
            }
            false
        } else {
            true
        }
    });
}

fn node_owns_partition(
    partitions: &Partitions,
    partition_id: PartitionId,
    node_id: &NodeId,
) -> bool {
    partitions
        .mapping
        .get(&partition_id)
        .is_some_and(|mapping| &mapping.master == node_id || mapping.replicas.contains(node_id))
}

fn sync_batch(
    output: &mut Vec<WorkerProtocol>,
    runtime_store: &RuntimeStore,
    recipient: &NodeId,
    partition: &PartitionId,
    after_key: Option<&Key>,
    me: &Me,
) -> Option<Key> {
    const SYNC_BATCH_SIZE: usize = 1000;
    let vec = runtime_store.get_partition_records(partition, SYNC_BATCH_SIZE, after_key);
    if vec.is_empty() {
        None
    } else {
        let max_last_key = vec.last().map(|(key, _)| key.clone());
        output.push(WorkerProtocol::SyncBatch {
            recipient_id: recipient.clone(),
            request: Arc::new(SyncBatchRequest {
                sender_id: me.id.clone(),
                records: vec
                    .into_iter()
                    .map(|(k, r)| domain::Record {
                        key: k.clone(),
                        value: r.value.clone(),
                        ttl: r.expiration_time_ms,
                        creation_time_ms: r.creation_time_ms,
                    })
                    .collect(),
            }),
        });
        max_last_key
    }
}

fn get_node_ids_curr_node_has_to_sync_the_partition_to<'a>(
    partition: PartitionId,
    me: &NodeId,
    partitions: &'a Partitions,
    cluster_nodes: &HashMap<NodeId, Node>,
    currently_synced_actual_nodes: Option<&HashSet<NodeId>>,
) -> HashSet<&'a NodeId> {
    let mut node_ids: HashSet<&'a NodeId> = HashSet::new();

    if partitions
        .old_replicas
        .get(&partition)
        .map(|old| old.contains(me))
        .unwrap_or(false)
    {
        if let Some(mapping) = partitions.mapping.get(&partition) {
            if &mapping.master != me {
                node_ids.insert(&mapping.master);
            }
            node_ids.extend(mapping.replicas.iter().filter(|&node_id| node_id != me));
        }
    } else if partitions
        .old_replicas
        .get(&partition)
        .map(|old| old.is_empty())
        .unwrap_or(true)
        && let Some(mapping) = partitions.mapping.get(&partition)
        && (&mapping.master == me || mapping.replicas.contains(me))
        && let Some(new_replicas) = partitions.new_replicas.get(&partition)
        && !new_replicas.contains(me)
    {
        if let Some(currently_synced_actual_nodes) = currently_synced_actual_nodes {
            let new_replicas: HashSet<&NodeId> = new_replicas
                .iter()
                .filter(|&node_id| !currently_synced_actual_nodes.contains(node_id))
                .collect();
            if !new_replicas.is_empty() {
                node_ids.extend(new_replicas);
            }
        } else {
            node_ids.extend(new_replicas);
        }
    }

    node_ids
        .into_iter()
        .filter(|&node_id| cluster_nodes.contains_key(node_id))
        .collect()
}

pub fn handle_sync_batch(
    output: &mut Vec<WorkerProtocol>,
    sync_batch_request: &SyncBatchRequest,
    runtime_store: &RuntimeStore,
) {
    let mut partition_id_to_max_applied_key = HashMap::new();

    for record in &sync_batch_request.records {
        runtime_store.put(
            record.key,
            runtime_store::Record {
                value: record.value.clone(),
                expiration_time_ms: record.ttl,
                creation_time_ms: record.creation_time_ms,
            },
        );

        match partition_id_to_max_applied_key.entry(record.key.partition()) {
            Entry::Occupied(mut occupied) => {
                if occupied.get() < &record.key {
                    occupied.insert(record.key);
                }
            }
            Entry::Vacant(occupied) => {
                occupied.insert(record.key);
            }
        }
    }

    output.push(WorkerProtocol::SyncBatchResponse {
        recipient_id: sync_batch_request.sender_id.clone(),
        partition_id_to_max_applied_key,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{NodeType, Partition};
    use crate::worker::runtime_store::Record as StoredRecord;

    const PARTITION: PartitionId = PartitionId(17);

    fn node_id(value: u128) -> NodeId {
        NodeId::from_string(&uuid::Uuid::from_u128(value).to_string())
    }

    fn worker() -> Node {
        Node {
            host: "worker.local".to_owned(),
            port: 9000,
            last_heartbeat: 0,
            node_type: NodeType::Worker,
        }
    }

    fn cluster_nodes(ids: &[NodeId]) -> HashMap<NodeId, Node> {
        ids.iter().cloned().map(|id| (id, worker())).collect()
    }

    fn mapping(master: NodeId, replicas: &[NodeId]) -> HashMap<PartitionId, Partition> {
        HashMap::from([(
            PARTITION,
            Partition {
                master,
                replicas: replicas.iter().cloned().collect(),
            },
        )])
    }

    fn owned_node_ids(actual: HashSet<&NodeId>) -> HashSet<NodeId> {
        actual.into_iter().cloned().collect()
    }

    fn me(id: NodeId) -> Me {
        Me {
            id,
            host: "worker.local".to_owned(),
            port: 9000,
        }
    }

    fn state(partitions: Partitions, nodes: HashMap<NodeId, Node>) -> State {
        State {
            epoch: Some(1),
            elected_leader_id: Some(node_id(99)),
            nodes,
            partitions,
            partitions_last_update_time: 42,
            sync: HashMap::new(),
            actual_nodes_sync_completion: HashMap::new(),
        }
    }

    fn put_partition_record(store: &RuntimeStore) -> Key {
        let key = Key(PARTITION.0 as u64);
        store.put(
            key,
            StoredRecord {
                value: vec![1],
                expiration_time_ms: 0,
                creation_time_ms: 1,
            },
        );
        key
    }

    fn complete_first_batch(
        state: &mut State,
        output: &mut Vec<WorkerProtocol>,
        store: &RuntimeStore,
        me: &Me,
        recipient: &NodeId,
        key: Key,
    ) {
        sync_partitions(state, output, store, me);
        handle_sync_batch_response(state, HashMap::from([(PARTITION, key)]), recipient.clone());
        output.clear();
        sync_partitions(state, output, store, me);
    }

    #[test]
    fn old_replica_syncs_to_every_available_node_in_the_current_mapping_except_itself() {
        let me = node_id(1);
        let master = node_id(2);
        let replica = node_id(3);
        let disconnected_replica = node_id(4);
        let partitions = Partitions {
            mapping: mapping(
                master.clone(),
                &[me.clone(), replica.clone(), disconnected_replica],
            ),
            old_replicas: HashMap::from([(PARTITION, HashSet::from([me.clone()]))]),
            new_replicas: HashMap::from([(PARTITION, HashSet::from([replica.clone()]))]),
        };
        let nodes = cluster_nodes(&[me.clone(), master.clone(), replica.clone()]);

        let actual = get_node_ids_curr_node_has_to_sync_the_partition_to(
            PARTITION,
            &me,
            &partitions,
            &nodes,
            None,
        );

        assert_eq!(owned_node_ids(actual), HashSet::from([master, replica]));
    }

    #[test]
    fn node_does_not_start_second_phase_while_an_old_replica_remains() {
        let me = node_id(1);
        let new_replica = node_id(2);
        let old_replica = node_id(3);
        let partitions = Partitions {
            mapping: mapping(me.clone(), &[new_replica.clone()]),
            old_replicas: HashMap::from([(PARTITION, HashSet::from([old_replica.clone()]))]),
            new_replicas: HashMap::from([(PARTITION, HashSet::from([new_replica.clone()]))]),
        };
        let nodes = cluster_nodes(&[me.clone(), new_replica, old_replica]);

        let actual = get_node_ids_curr_node_has_to_sync_the_partition_to(
            PARTITION,
            &me,
            &partitions,
            &nodes,
            None,
        );

        assert!(actual.is_empty());
    }

    #[test]
    fn retained_current_node_syncs_to_available_new_replicas_after_old_replicas_are_done() {
        let me = node_id(1);
        let new_replica = node_id(2);
        let disconnected_new_replica = node_id(3);
        let partitions = Partitions {
            mapping: mapping(
                me.clone(),
                &[new_replica.clone(), disconnected_new_replica.clone()],
            ),
            old_replicas: HashMap::from([(PARTITION, HashSet::new())]),
            new_replicas: HashMap::from([(
                PARTITION,
                HashSet::from([new_replica.clone(), disconnected_new_replica]),
            )]),
        };
        let nodes = cluster_nodes(&[me.clone(), new_replica.clone()]);

        let actual = get_node_ids_curr_node_has_to_sync_the_partition_to(
            PARTITION,
            &me,
            &partitions,
            &nodes,
            None,
        );

        assert_eq!(owned_node_ids(actual), HashSet::from([new_replica]));
    }

    #[test]
    fn absent_old_replicas_entry_also_means_the_first_phase_is_done() {
        let me = node_id(1);
        let new_master = node_id(2);
        let partitions = Partitions {
            mapping: mapping(new_master.clone(), &[me.clone()]),
            old_replicas: HashMap::new(),
            new_replicas: HashMap::from([(PARTITION, HashSet::from([new_master.clone()]))]),
        };
        let nodes = cluster_nodes(&[me.clone(), new_master.clone()]);

        let actual = get_node_ids_curr_node_has_to_sync_the_partition_to(
            PARTITION,
            &me,
            &partitions,
            &nodes,
            None,
        );

        assert_eq!(owned_node_ids(actual), HashSet::from([new_master]));
    }

    #[test]
    fn newly_added_node_does_not_relay_the_partition_to_other_new_nodes() {
        let me = node_id(1);
        let retained_master = node_id(2);
        let other_new_replica = node_id(3);
        let partitions = Partitions {
            mapping: mapping(
                retained_master.clone(),
                &[me.clone(), other_new_replica.clone()],
            ),
            old_replicas: HashMap::new(),
            new_replicas: HashMap::from([(
                PARTITION,
                HashSet::from([me.clone(), other_new_replica.clone()]),
            )]),
        };
        let nodes = cluster_nodes(&[me.clone(), retained_master, other_new_replica]);

        let actual = get_node_ids_curr_node_has_to_sync_the_partition_to(
            PARTITION,
            &me,
            &partitions,
            &nodes,
            None,
        );

        assert!(actual.is_empty());
    }

    #[test]
    fn node_outside_both_old_and_current_mapping_has_no_sync_targets() {
        let me = node_id(1);
        let master = node_id(2);
        let new_replica = node_id(3);
        let partitions = Partitions {
            mapping: mapping(master.clone(), &[new_replica.clone()]),
            old_replicas: HashMap::new(),
            new_replicas: HashMap::from([(PARTITION, HashSet::from([new_replica.clone()]))]),
        };
        let nodes = cluster_nodes(&[me.clone(), master, new_replica]);

        let actual = get_node_ids_curr_node_has_to_sync_the_partition_to(
            PARTITION,
            &me,
            &partitions,
            &nodes,
            None,
        );

        assert!(actual.is_empty());
    }

    #[test]
    fn retained_node_keeps_partition_and_remembers_completed_new_node() {
        let current = node_id(1);
        let new_replica = node_id(2);
        let partitions = Partitions {
            mapping: mapping(current.clone(), &[new_replica.clone()]),
            old_replicas: HashMap::new(),
            new_replicas: HashMap::from([(PARTITION, HashSet::from([new_replica.clone()]))]),
        };
        let mut state = state(
            partitions,
            cluster_nodes(&[current.clone(), new_replica.clone()]),
        );
        let me = me(current);
        let store = RuntimeStore::new();
        let key = put_partition_record(&store);
        let mut output = vec![];

        complete_first_batch(&mut state, &mut output, &store, &me, &new_replica, key);

        assert!(store.get(key).is_some());
        assert_eq!(
            state.actual_nodes_sync_completion[&PARTITION][&new_replica],
            state.partitions_last_update_time
        );
        assert!(
            output.iter().any(|message| matches!(
                message,
                WorkerProtocol::RemovePartitionFromReplica { .. }
            ))
        );

        output.clear();
        sync_partitions(&mut state, &mut output, &store, &me);
        assert!(output.is_empty(), "completed sync must not restart");
    }

    #[test]
    fn obsolete_node_removes_partition_after_all_current_nodes_are_synced() {
        let obsolete = node_id(1);
        let new_master = node_id(2);
        let partitions = Partitions {
            mapping: mapping(new_master.clone(), &[]),
            old_replicas: HashMap::from([(PARTITION, HashSet::from([obsolete.clone()]))]),
            new_replicas: HashMap::new(),
        };
        let mut state = state(
            partitions,
            cluster_nodes(&[obsolete.clone(), new_master.clone()]),
        );
        let me = me(obsolete.clone());
        let store = RuntimeStore::new();
        let key = put_partition_record(&store);
        let mut output = vec![];

        complete_first_batch(&mut state, &mut output, &store, &me, &new_master, key);

        assert!(store.get(key).is_none());
        assert!(output.iter().any(|message| matches!(
            message,
            WorkerProtocol::RemovePartitionFromReplica {
                replica_id,
                partition_id,
                ..
            } if replica_id == &obsolete && partition_id == &PARTITION
        )));
    }
}
