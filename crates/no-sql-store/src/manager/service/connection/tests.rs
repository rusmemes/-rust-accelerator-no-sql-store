use super::*;
use crate::common::now_millis;
use crate::manager::domain::ManagerProtocol;
use crate::manager::service::State;
use crate::manager::service::test_support::*;
use std::collections::HashMap;

#[tokio::test]
async fn new_connection_adds_node_and_requests_cluster_state() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let peer_id = node_id("22222222-2222-2222-2222-222222222222");
    let (mut service, _config) = service(me.clone());
    service.state = Some(State {
        epoch: None,
        elected_leader_id: None,
        nodes: HashMap::from([(me.id.clone(), fresh_node(&me, now_millis()))]),
        partitions: Default::default(),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    service
        .process(
            ManagerProtocol::NewConnection {
                id: Some(peer_id.clone()),
                host: "peer.local".to_string(),
                port: 9001,
                manager: true,
            },
            &mut output,
        )
        .await;

    assert!(matches!(
        output.as_slice(),
        [ManagerProtocol::GetClusterState { id }] if id == &peer_id
    ));

    let state = service.state.as_ref().expect("state exists");
    assert!(state.nodes.contains_key(&peer_id));
}

#[tokio::test]
async fn new_connection_while_we_are_leader_does_not_request_cluster_state() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let peer = node_id("22222222-2222-2222-2222-222222222222");
    let now = now_millis();
    let (mut service, _config) = service(me.clone());
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
    handle_new_connection(
        &mut output,
        service.state.as_mut().unwrap(),
        Some(node_id("33333333-3333-3333-3333-333333333333")),
        "third.local".to_string(),
        9002,
        &me,
        true,
    );

    assert!(output.is_empty());
}

#[tokio::test]
async fn node_disconnected_for_unknown_node_is_a_noop() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let leader = node_id("22222222-2222-2222-2222-222222222222");
    let (mut service, _config) = service(me.clone());
    service.state = Some(State {
        epoch: Some(1),
        elected_leader_id: Some(leader.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now_millis())),
            (leader.clone(), node("leader.local", 9001, now_millis())),
        ]),
        partitions: Default::default(),
        workers_with_calculated_partitions: Default::default(),
    });

    handle_node_disconnected(
        service.state.as_mut().unwrap(),
        node_id("33333333-3333-3333-3333-333333333333"),
        &me,
    );

    let state = service.state.as_ref().unwrap();
    assert!(state.nodes.contains_key(&me.id));
    assert!(state.nodes.contains_key(&leader));
    assert_eq!(state.elected_leader_id, Some(leader));
}

#[tokio::test]
async fn node_disconnected_clears_current_leader() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let leader = node_id("22222222-2222-2222-2222-222222222222");
    let (mut service, _config) = service(me);
    service.state = Some(State {
        epoch: Some(7),
        elected_leader_id: Some(leader.clone()),
        nodes: HashMap::from([(leader.clone(), node("leader.local", 9001, 0))]),
        partitions: Default::default(),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    service
        .process(
            ManagerProtocol::NodeDisconnected { id: leader.clone() },
            &mut output,
        )
        .await;

    assert!(output.is_empty());
    let state = service.state.as_ref().unwrap();
    assert_eq!(state.elected_leader_id, None);
    assert!(!state.nodes.contains_key(&leader));
}
