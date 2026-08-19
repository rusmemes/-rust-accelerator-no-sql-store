use super::*;
use crate::common::now_millis;
use crate::manager::domain::ManagerProtocol;
use crate::manager::service::State;
use crate::manager::service::test_support::*;
use std::collections::{HashMap, HashSet};

fn expected_master_for(partition: PartitionId, workers: &[NodeId]) -> NodeId {
    workers[partition.0 as usize % workers.len()].clone()
}

fn expected_replicas_for(
    partition: PartitionId,
    replication_factor: usize,
    workers: &[NodeId],
) -> Vec<NodeId> {
    let master_index = partition.0 as usize % workers.len();
    (1..replication_factor)
        .map(|replica| workers[calc_replica_index(workers.len(), master_index, replica)].clone())
        .collect()
}

fn replicas(replicas: Vec<NodeId>) -> HashSet<NodeId> {
    replicas.into_iter().collect()
}

#[tokio::test]
async fn worker_partitions_recomputes_cluster_partition_mapping_and_broadcasts_cluster_state() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let worker_a = node_id("22222222-2222-2222-2222-222222222222");
    let worker_b = node_id("33333333-3333-3333-3333-333333333333");
    let workers = vec![worker_a.clone(), worker_b.clone()];
    let (mut service, _config) = service(me.clone());
    service.state = Some(State {
        epoch: Some(1),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now_millis())),
            (worker_a.clone(), worker_node("worker-a.local", 9100, 0)),
            (worker_b.clone(), worker_node("worker-b.local", 9101, 0)),
        ]),
        partitions: Default::default(),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    worker_partitions(
        service.state.as_mut().unwrap(),
        &mut output,
        &service.me,
        3,
    );

    assert_eq!(output.len(), 2);
    assert!(output.iter().all(|msg| matches!(
        msg,
        ManagerProtocol::ClusterState { recipient_id, state }
            if (recipient_id == &worker_a || recipient_id == &worker_b)
                && state.nodes.is_empty()
                && state.partitions.mapping.len() == PARTITIONS_AMOUNT
                && state.partitions.old_replicas.is_empty()
    )));

    let state = service.state.as_ref().expect("state exists");
    assert_eq!(state.partitions.mapping.len(), PARTITIONS_AMOUNT);
    assert!(state.partitions.old_replicas.is_empty());

    for partition in [PartitionId(0), PartitionId(1), PartitionId(2), PartitionId(4095)] {
        let actual = state.partitions.mapping.get(&partition).unwrap();
        assert_eq!(actual.master, expected_master_for(partition, &workers));
        assert_eq!(
            actual.replicas,
            replicas(expected_replicas_for(partition, 3, &workers))
        );
    }
}

#[tokio::test]
async fn worker_partitions_moves_previous_mapping_to_old_mapping_when_worker_layout_changes() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let worker_a = node_id("22222222-2222-2222-2222-222222222222");
    let worker_b = node_id("33333333-3333-3333-3333-333333333333");
    let worker_c = node_id("44444444-4444-4444-4444-444444444444");
    let initial_workers = vec![worker_a.clone(), worker_b.clone()];
    let new_workers = vec![worker_a.clone(), worker_b.clone(), worker_c.clone()];
    let (mut service, _config) = service(me.clone());
    service.state = Some(State {
        epoch: Some(1),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now_millis())),
            (worker_a.clone(), worker_node("worker-a.local", 9100, 0)),
            (worker_b.clone(), worker_node("worker-b.local", 9101, 0)),
        ]),
        partitions: Default::default(),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    worker_partitions(
        service.state.as_mut().unwrap(),
        &mut output,
        &service.me,
        1,
    );

    assert_eq!(output.len(), 2);
    {
        let state = service.state.as_ref().expect("state exists");
        assert_eq!(state.partitions.mapping.len(), PARTITIONS_AMOUNT);
        assert!(state.partitions.old_replicas.is_empty());
        for partition in [PartitionId(0), PartitionId(1), PartitionId(4095)] {
            assert_eq!(
                state.partitions.mapping.get(&partition).unwrap().master,
                expected_master_for(partition, &initial_workers)
            );
        }
    }

    service
        .state
        .as_mut()
        .unwrap()
        .nodes
        .insert(worker_c.clone(), worker_node("worker-c.local", 9102, 0));

    output.clear();
    worker_partitions(
        service.state.as_mut().unwrap(),
        &mut output,
        &service.me,
        1,
    );

    assert_eq!(output.len(), 3);
    assert!(output.iter().all(|msg| matches!(
        msg,
        ManagerProtocol::ClusterState { state, .. }
            if state.partitions.mapping.len() == PARTITIONS_AMOUNT
                && state.partitions.old_replicas.len() == PARTITIONS_AMOUNT
    )));

    let state = service.state.as_ref().expect("state exists");
    for partition in [PartitionId(0), PartitionId(1), PartitionId(2), PartitionId(4095)] {
        assert_eq!(
            state.partitions.mapping.get(&partition).unwrap().master,
            expected_master_for(partition, &new_workers)
        );
    }
    assert_eq!(
        state.partitions.old_replicas.get(&PartitionId(0)),
        Some(&replicas(vec![worker_a.clone()]))
    );
    assert_eq!(
        state.partitions.old_replicas.get(&PartitionId(1)),
        Some(&replicas(vec![worker_b.clone()]))
    );
    assert_eq!(
        state.partitions.old_replicas.get(&PartitionId(2)),
        Some(&replicas(vec![worker_a.clone()]))
    );
    assert_eq!(
        state.partitions.old_replicas.get(&PartitionId(4095)),
        Some(&replicas(vec![worker_b.clone()]))
    );
    assert!(state.partitions.new_replicas.get(&PartitionId(0)).unwrap().is_empty());
    assert!(state.partitions.new_replicas.get(&PartitionId(1)).unwrap().is_empty());
    assert_eq!(
        state.partitions.new_replicas.get(&PartitionId(2)),
        Some(&replicas(vec![worker_c.clone()]))
    );
    assert_eq!(
        state.partitions.new_replicas.get(&PartitionId(4095)),
        Some(&replicas(vec![worker_a.clone()]))
    );
}

#[tokio::test]
async fn worker_partitions_recomputes_while_old_mapping_is_present_and_merges_transition_mapping() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let worker_a = node_id("22222222-2222-2222-2222-222222222222");
    let worker_b = node_id("33333333-3333-3333-3333-333333333333");
    let worker_c = node_id("44444444-4444-4444-4444-444444444444");
    let (mut service, _config) = service(me.clone());

    service.state = Some(State {
        epoch: Some(1),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now_millis())),
            (worker_a.clone(), worker_node("worker-a.local", 9100, 0)),
            (worker_b.clone(), worker_node("worker-b.local", 9101, 0)),
            (worker_c.clone(), worker_node("worker-c.local", 9102, 0)),
        ]),
        partitions: Partitions {
            mapping: HashMap::from([(
                PartitionId(1),
                Partition {
                    master: worker_a.clone(),
                    replicas: HashSet::new(),
                },
            )]),
            old_replicas: HashMap::from([(PartitionId(1), replicas(vec![worker_b.clone()]))]),
            new_replicas: Default::default(),
        },
        workers_with_calculated_partitions: [worker_a.clone(), worker_b.clone()]
            .into_iter()
            .collect(),
    });

    let mut output = vec![];
    worker_partitions(
        service.state.as_mut().unwrap(),
        &mut output,
        &service.me,
        1,
    );

    assert_eq!(output.len(), 3);
    let state = service.state.as_ref().expect("state exists");
    assert_eq!(state.partitions.mapping.len(), PARTITIONS_AMOUNT);
    assert_eq!(state.partitions.old_replicas.len(), 1);
    assert_eq!(
        state
            .partitions
            .mapping
            .get(&PartitionId(1))
            .map(|partition| &partition.master),
        Some(&worker_b)
    );
    assert_eq!(
        state.partitions.old_replicas.get(&PartitionId(1)),
        Some(&replicas(vec![worker_a, worker_b]))
    );
    assert!(state.workers_with_calculated_partitions.contains(&worker_c));
}


#[test]
fn move_current_mapping_to_old_merges_existing_old_mapping_and_filters_stale_replicas() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let current_master = node_id("22222222-2222-2222-2222-222222222222");
    let current_replica = node_id("33333333-3333-3333-3333-333333333333");
    let old_master = node_id("44444444-4444-4444-4444-444444444444");
    let old_replica = node_id("55555555-5555-5555-5555-555555555555");
    let stale_worker = node_id("66666666-6666-6666-6666-666666666666");
    let mut state = State {
        epoch: Some(1),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now_millis())),
            (current_master.clone(), worker_node("worker.local", 9100, 0)),
            (
                current_replica.clone(),
                worker_node("replica.local", 9101, 0),
            ),
            (
                old_replica.clone(),
                worker_node("old-replica.local", 9102, 0),
            ),
        ]),
        partitions: Partitions {
            mapping: HashMap::from([(
                PartitionId(1),
                Partition {
                    master: current_master.clone(),
                    replicas: replicas(vec![current_replica.clone(), old_replica.clone()]),
                },
            )]),
            old_replicas: HashMap::from([
                (PartitionId(1), replicas(vec![old_master.clone(), old_replica.clone()])),
                (PartitionId(2), replicas(vec![stale_worker.clone()])),
            ]),
            new_replicas: Default::default(),
        },
        workers_with_calculated_partitions: Default::default(),
    };

    let current_keys = vec![
        current_master.clone(),
        current_replica.clone(),
        old_replica.clone(),
    ];
    copy_current_mapping_to_old(&mut state, &current_keys);

    assert!(!state.partitions.mapping.is_empty());
    assert_eq!(
        state.partitions.old_replicas.get(&PartitionId(1)),
        Some(&replicas(vec![
            current_master,
            current_replica,
            old_replica,
        ]))
    );
    assert!(
        !state
            .partitions
            .old_replicas
            .get(&PartitionId(1))
            .unwrap()
            .contains(&old_master)
    );
    assert!(!state.partitions.old_replicas.contains_key(&PartitionId(2)));
    assert!(
        !state
            .partitions
            .old_replicas
            .values()
            .any(|replicas| replicas.contains(&stale_worker))
    );
}
