use super::*;
use crate::common::{NodeType, Partition};
use crate::conversions;
use crate::conversions::manager_api::v1::Leader;
use crate::manager::grpc::test_support::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

#[tokio::test]
async fn output_routes_leader_to_worker_session() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let worker_id = node_id("22222222-2222-2222-2222-222222222222");
    let (tx, _rx) = tokio::sync::mpsc::channel(4);
    let (worker_tx, mut worker_rx) = worker_output_sender();
    let manager_sessions = Arc::new(RwLock::new(HashMap::new()));
    let worker_sessions = worker_session(&worker_id, WorkerIOStream::Output(worker_tx));

    handle_output_leader(
        &me,
        &tx,
        &manager_sessions,
        &worker_sessions,
        worker_id.clone(),
        3,
        44,
    )
    .await;

    let event = worker_rx.recv().await.expect("worker event");
    assert!(matches!(
        event.payload,
        Some(worker_event::Payload::ManagerLeader(Leader { id, epoch, ts }))
            if id == me.id.to_string() && epoch == 3 && ts == 44
    ));
}

#[tokio::test]
async fn output_routes_leader_to_manager_session() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let manager_id = node_id("22222222-2222-2222-2222-222222222222");
    let (tx, _rx) = tokio::sync::mpsc::channel(4);
    let (manager_tx, mut manager_rx) = manager_output_sender();
    let manager_sessions = manager_session(&manager_id, ManagerIOStream::Output(manager_tx));
    let worker_sessions = Arc::new(RwLock::new(HashMap::new()));

    handle_output_leader(
        &me,
        &tx,
        &manager_sessions,
        &worker_sessions,
        manager_id.clone(),
        7,
        99,
    )
    .await;

    let event = manager_rx.recv().await.expect("manager event");
    assert!(matches!(
        event.payload,
        Some(Payload::Leader(Leader { id, epoch, ts }))
            if id == me.id.to_string() && epoch == 7 && ts == 99
    ));
}

#[tokio::test]
async fn output_routes_cluster_state_to_worker_session_includes_partitions() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let worker_id = node_id("22222222-2222-2222-2222-222222222222");
    let manager_node_id = node_id("33333333-3333-3333-3333-333333333333");
    let worker_node_id = node_id("44444444-4444-4444-4444-444444444444");
    let (tx, _rx) = tokio::sync::mpsc::channel(4);
    let (worker_tx, mut worker_rx) = worker_output_sender();
    let manager_sessions = Arc::new(RwLock::new(HashMap::new()));
    let worker_sessions = worker_session(&worker_id, WorkerIOStream::Output(worker_tx));

    handle_output_cluster_state(
        &tx,
        &manager_sessions,
        &worker_sessions,
        worker_id.clone(),
        5,
        manager_node_id.clone(),
        vec![
            ClusterNode {
                id: manager_node_id.clone(),
                host: "manager.local".to_string(),
                port: 9001,
                last_heartbeat: 10,
                node_type: NodeType::Manager,
            },
            ClusterNode {
                id: worker_node_id.clone(),
                host: "worker.local".to_string(),
                port: 9100,
                last_heartbeat: 11,
                node_type: NodeType::Worker,
            },
        ],
        Partitions {
            mapping: HashMap::from([(
                PartitionId(7),
                Partition {
                    master: worker_node_id.clone(),
                    replicas: HashSet::from([manager_node_id.clone()]),
                },
            )]),
            old_replicas: HashMap::from([(
                PartitionId(6),
                HashSet::from([manager_node_id.clone(), worker_node_id.clone()]),
            )]),
            new_replicas: Default::default(),
        },
    )
    .await;

    let event = worker_rx.recv().await.expect("worker cluster state");
    let cluster_state = match event.payload {
        Some(worker_event::Payload::ClusterState(cluster_state)) => cluster_state,
        other => panic!("unexpected payload: {:?}", other),
    };

    assert_eq!(cluster_state.epoch, 5);
    assert_eq!(cluster_state.leader_id, manager_node_id.to_string());
    assert_eq!(cluster_state.nodes.len(), 2);
    assert!(cluster_state.nodes.iter().any(|node| {
        node.id == manager_node_id.to_string()
            && node
                .addr
                .as_ref()
                .is_some_and(|addr| addr.host == "manager.local" && addr.port == 9001)
            && node.last_heartbeat == 10
            && node.node_type == conversions::common::v1::NodeType::Manager as i32
    }));
    assert!(cluster_state.nodes.iter().any(|node| {
        node.id == worker_node_id.to_string()
            && node
                .addr
                .as_ref()
                .is_some_and(|addr| addr.host == "worker.local" && addr.port == 9100)
            && node.last_heartbeat == 11
            && node.node_type == conversions::common::v1::NodeType::Worker as i32
    }));
    let partitions = cluster_state.partitions.expect("partitions");
    let mapping = partitions.mapping.get(&7).expect("mapping");
    assert_eq!(mapping.master, worker_node_id.to_string());
    assert_eq!(
        mapping.replicas.iter().cloned().collect::<HashSet<_>>(),
        HashSet::from([manager_node_id.to_string()])
    );
    let old_replicas = partitions.old_replicas.get(&6).expect("old replicas");
    assert_eq!(
        old_replicas
            .replicas
            .iter()
            .cloned()
            .collect::<HashSet<_>>(),
        HashSet::from([manager_node_id.to_string(), worker_node_id.to_string()])
    );

    let _ = me;
}

#[tokio::test]
async fn output_routes_cluster_state_to_manager_session_includes_partitions() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let manager_id = node_id("22222222-2222-2222-2222-222222222222");
    let (tx, _rx) = tokio::sync::mpsc::channel(4);
    let (manager_tx, mut manager_rx) = manager_output_sender();
    let manager_sessions = manager_session(&manager_id, ManagerIOStream::Output(manager_tx));
    let worker_sessions: Arc<RwLock<HashMap<NodeId, WorkerIOStream>>> =
        Arc::new(RwLock::new(HashMap::new()));

    handle_output_cluster_state(
        &tx,
        &manager_sessions,
        &worker_sessions,
        manager_id.clone(),
        2,
        me.id.clone(),
        vec![ClusterNode {
            id: me.id.clone(),
            host: "self.local".to_string(),
            port: 7000,
            last_heartbeat: 77,
            node_type: NodeType::Manager,
        }],
        Partitions::default(),
    )
    .await;

    let event = manager_rx.recv().await.expect("manager cluster state");
    let cluster_state = match event.payload {
        Some(Payload::ClusterState(cluster_state)) => cluster_state,
        other => panic!("unexpected payload: {:?}", other),
    };

    assert_eq!(cluster_state.epoch, 2);
    assert_eq!(cluster_state.leader_id, me.id.to_string());
    assert_eq!(cluster_state.nodes.len(), 1);
    let node = cluster_state.nodes.first().expect("node");
    assert_eq!(node.id, me.id.to_string());
    assert!(
        node.addr
            .as_ref()
            .is_some_and(|addr| addr.host == "self.local" && addr.port == 7000)
    );
    assert_eq!(node.last_heartbeat, 77);
    assert_eq!(node.node_type, conversions::common::v1::NodeType::Manager as i32);
    assert!(
        cluster_state
            .partitions
            .expect("partitions")
            .mapping
            .is_empty()
    );
}

#[tokio::test]
async fn output_removes_closed_manager_session() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let manager_id = node_id("22222222-2222-2222-2222-222222222222");
    let (tx, mut rx) = tokio::sync::mpsc::channel(4);
    let (manager_tx, manager_rx) = manager_output_sender();
    drop(manager_rx);
    let manager_sessions = manager_session(&manager_id, ManagerIOStream::Output(manager_tx.clone()));
    // Heartbeat routing now checks worker sessions first. Keep this empty so we exercise
    // the manager-session path and verify manager session cleanup on send failure.
    let worker_sessions: Arc<RwLock<HashMap<NodeId, WorkerIOStream>>> =
        Arc::new(RwLock::new(HashMap::new()));

    handle_output_heartbeat(
        &tx,
        &manager_sessions,
        &worker_sessions,
        manager_id.clone(),
        me.id.clone(),
        123,
    )
    .await;

    assert!(!manager_sessions.read().await.contains_key(&manager_id));
    assert!(matches!(
        rx.recv().await.expect("protocol message"),
        ManagerProtocol::NodeDisconnected { id } if id == manager_id
    ));
}
