use super::*;
use crate::common::{Partition, now_millis};
use crate::manager::domain::ManagerProtocol;
use crate::manager::service::State;
use crate::manager::service::test_support::*;
use std::collections::{HashMap, HashSet};

fn cluster_node(
    id: NodeId,
    host: &str,
    port: u32,
    last_heartbeat: u64,
    node_type: NodeType,
) -> ClusterNode {
    ClusterNode {
        id,
        host: host.to_string(),
        port,
        last_heartbeat,
        node_type,
    }
}

fn replicas(replicas: Vec<NodeId>) -> HashSet<NodeId> {
    replicas.into_iter().collect()
}

#[tokio::test]
async fn get_cluster_state_returns_current_cluster_snapshot() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let peer_id = node_id("22222222-2222-2222-2222-222222222222");
    let (mut service, _config) = service(me.clone());
    let now = now_millis();
    service.state = Some(State {
        epoch: Some(3),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (peer_id.clone(), node("peer.local", 9001, 120)),
        ]),
        partitions: Default::default(),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    service
        .process(
            ManagerProtocol::GetClusterState {
                id: peer_id.clone(),
            },
            &mut output,
        )
        .await;

    let cluster_state = match output.as_slice() {
        [
            ManagerProtocol::ClusterState {
                recipient_id,
                state,
            },
        ] => {
            assert_eq!(recipient_id, &peer_id);
            state
        }
        other => panic!("unexpected output: {:?}", other),
    };

    assert_eq!(cluster_state.epoch, 3);
    assert_eq!(cluster_state.leader_id, me.id);
    assert_eq!(cluster_state.nodes.len(), 2);
    assert!(cluster_state.partitions.mapping.is_empty());
    assert!(cluster_state.partitions.old_replicas.is_empty());
}

#[tokio::test]
async fn get_cluster_state_returns_worker_items_and_partition_mapping() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let worker_id = node_id("22222222-2222-2222-2222-222222222222");
    let (mut service, _config) = service(me.clone());
    let now = now_millis();
    service.state = Some(State {
        epoch: Some(3),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (
                worker_id.clone(),
                worker_node("worker.local", 9100, now - 5),
            ),
        ]),
        partitions: Partitions {
            mapping: HashMap::from([(
                PartitionId(7),
                Partition {
                    master: worker_id.clone(),
                    replicas: replicas(vec![me.id.clone()]),
                },
            )]),
            old_replicas: Default::default(),
            new_replicas: Default::default(),
        },
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    handle_get_cluster_state(
        &mut output,
        service.state.as_mut().unwrap(),
        worker_id.clone(),
    );

    let cluster_state = match output.as_slice() {
        [
            ManagerProtocol::ClusterState {
                recipient_id,
                state,
            },
        ] => {
            assert_eq!(recipient_id, &worker_id);
            state
        }
        other => panic!("unexpected output: {:?}", other),
    };

    assert!(cluster_state.nodes.iter().any(|item| {
        item.id == worker_id
            && item.host == "worker.local"
            && item.port == 9100
            && item.last_heartbeat == now - 5
            && item.node_type == NodeType::Worker
    }));
    assert_eq!(
        cluster_state
            .partitions
            .mapping
            .get(&PartitionId(7))
            .map(|partition| (&partition.master, &partition.replicas)),
        Some((&worker_id, &replicas(vec![me.id.clone()])))
    );
}

#[tokio::test]
async fn cluster_state_updates_known_nodes_and_requests_unknown_managers() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let peer = node_id("22222222-2222-2222-2222-222222222222");
    let unknown = node_id("33333333-3333-3333-3333-333333333333");
    let (mut service, _config) = service(me.clone());
    let now = now_millis();
    service.state = Some(State {
        epoch: Some(1),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (peer.clone(), node("peer.local", 9001, now)),
        ]),
        partitions: Default::default(),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    handle_cluster_state(
        &mut output,
        service.state.as_mut().unwrap(),
        2,
        me.id.clone(),
        vec![
            cluster_node(me.id.clone(), &me.host, me.port, now + 1, NodeType::Manager),
            cluster_node(
                unknown.clone(),
                "unknown.local",
                9002,
                now + 2,
                NodeType::Manager,
            ),
        ],
        Partitions::default(),
        &me,
    );

    assert!(matches!(
        output.as_slice(),
        [ManagerProtocol::NewConnection { id, host, port, manager }]
            if id.is_none() && host == "unknown.local" && *port == 9002 && *manager
    ));

    let state = service.state.as_mut().unwrap();
    assert_eq!(state.epoch, Some(2));
    assert_eq!(state.elected_leader_id, Some(me.id.clone()));
    assert_eq!(state.nodes.get(&me.id).unwrap().last_heartbeat, now + 1);
    assert!(!state.nodes.contains_key(&peer));
}

#[tokio::test]
async fn cluster_state_accepts_new_epoch_same_leader_and_rejects_conflicts() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let leader = node_id("22222222-2222-2222-2222-222222222222");
    let other_leader = node_id("33333333-3333-3333-3333-333333333333");
    let (mut service, _config) = service(me.clone());
    let now = now_millis();
    service.state = Some(State {
        epoch: None,
        elected_leader_id: None,
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (leader.clone(), node("leader.local", 9001, now)),
        ]),
        partitions: Default::default(),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut first = vec![];
    handle_cluster_state(
        &mut first,
        service.state.as_mut().unwrap(),
        4,
        leader.clone(),
        vec![
            cluster_node(me.id.clone(), &me.host, me.port, now, NodeType::Manager),
            cluster_node(leader.clone(), "leader.local", 9001, now, NodeType::Manager),
        ],
        Partitions::default(),
        &me,
    );
    assert!(first.is_empty());
    assert_eq!(service.state.as_ref().unwrap().epoch, Some(4));

    let mut same_epoch_same_leader = vec![];
    handle_cluster_state(
        &mut same_epoch_same_leader,
        service.state.as_mut().unwrap(),
        4,
        leader.clone(),
        vec![cluster_node(
            me.id.clone(),
            &me.host,
            me.port,
            now + 1,
            NodeType::Manager,
        )],
        Partitions::default(),
        &me,
    );
    assert!(same_epoch_same_leader.is_empty());
    assert_eq!(
        service
            .state
            .as_ref()
            .unwrap()
            .nodes
            .get(&me.id)
            .unwrap()
            .last_heartbeat,
        now + 1
    );

    let mut conflict = vec![];
    handle_cluster_state(
        &mut conflict,
        service.state.as_mut().unwrap(),
        4,
        other_leader,
        vec![cluster_node(
            me.id.clone(),
            &me.host,
            me.port,
            now + 2,
            NodeType::Manager,
        )],
        Partitions::default(),
        &me,
    );
    assert!(conflict.is_empty());
    assert_eq!(service.state.as_ref().unwrap().epoch, Some(4));
    assert_eq!(
        service
            .state
            .as_ref()
            .unwrap()
            .nodes
            .get(&me.id)
            .unwrap()
            .last_heartbeat,
        now + 1
    );
}

#[tokio::test]
async fn cluster_state_applies_partition_mapping_and_adds_unknown_workers() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let worker = node_id("22222222-2222-2222-2222-222222222222");
    let other_worker = node_id("33333333-3333-3333-3333-333333333333");
    let (mut service, _config) = service(me.clone());
    let now = now_millis();
    service.state = Some(State {
        epoch: Some(1),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (worker.clone(), worker_node("worker.local", 9100, now)),
        ]),
        partitions: Default::default(),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    handle_cluster_state(
        &mut output,
        service.state.as_mut().unwrap(),
        2,
        me.id.clone(),
        vec![
            cluster_node(
                worker.clone(),
                "worker.local",
                9100,
                now + 10,
                NodeType::Worker,
            ),
            cluster_node(
                other_worker.clone(),
                "worker-two.local",
                9101,
                now + 20,
                NodeType::Worker,
            ),
        ],
        Partitions {
            mapping: HashMap::from([(
                PartitionId(9),
                Partition {
                    master: worker.clone(),
                    replicas: replicas(vec![other_worker.clone()]),
                },
            )]),
            old_replicas: HashMap::from([(
                PartitionId(8),
                replicas(vec![worker.clone(), other_worker.clone()]),
            )]),
            new_replicas: Default::default(),
        },
        &me,
    );

    assert!(output.is_empty());
    let state = service.state.as_ref().unwrap();
    assert_eq!(state.nodes.get(&worker).unwrap().last_heartbeat, now + 10);
    assert_eq!(
        state
            .nodes
            .get(&other_worker)
            .map(|node| (&node.host, node.port, node.node_type)),
        Some((&"worker-two.local".to_string(), 9101, NodeType::Worker))
    );
    assert_eq!(
        state
            .partitions
            .mapping
            .get(&PartitionId(9))
            .map(|partition| (&partition.master, &partition.replicas)),
        Some((&worker, &replicas(vec![other_worker.clone()])))
    );
    assert_eq!(
        state.partitions.old_replicas.get(&PartitionId(8)),
        Some(&replicas(vec![worker, other_worker]))
    );
}

#[tokio::test]
async fn get_cluster_state_is_ignored_until_the_cluster_is_known() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let (mut service, _config) = service(me.clone());
    service.state = Some(State {
        epoch: None,
        elected_leader_id: None,
        nodes: HashMap::from([(me.id.clone(), fresh_node(&me, now_millis()))]),
        partitions: Default::default(),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    handle_get_cluster_state(
        &mut output,
        service.state.as_mut().unwrap(),
        node_id("22222222-2222-2222-2222-222222222222"),
    );

    assert!(output.is_empty());
}

#[tokio::test]
async fn stale_cluster_state_is_ignored() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let leader = node_id("22222222-2222-2222-2222-222222222222");
    let (mut service, _config) = service(me.clone());
    let now = now_millis();

    service.state = Some(State {
        epoch: Some(3),
        elected_leader_id: Some(leader.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (leader.clone(), node("leader.local", 9001, now)),
        ]),
        partitions: Default::default(),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    handle_cluster_state(
        &mut output,
        service.state.as_mut().unwrap(),
        2,
        me.id.clone(),
        vec![cluster_node(
            me.id.clone(),
            &me.host,
            me.port,
            now + 50,
            NodeType::Manager,
        )],
        Partitions::default(),
        &me,
    );

    assert!(output.is_empty());
    let state = service.state.as_ref().unwrap();
    assert_eq!(state.epoch, Some(3));
    assert_eq!(state.elected_leader_id, Some(leader));
    assert_eq!(state.nodes.get(&me.id).unwrap().last_heartbeat, now);
}
