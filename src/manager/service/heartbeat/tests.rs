use super::*;
use crate::common::now_millis;
use crate::manager::domain::ManagerProtocol;
use crate::manager::service::State;
use crate::manager::service::test_support::*;
use std::collections::HashMap;

#[tokio::test]
async fn heartbeat_from_unknown_node_requests_cluster_state_from_peers() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let peer_id = node_id("22222222-2222-2222-2222-222222222222");
    let (mut service, _config) = service(me.clone());
    let now = now_millis();
    service.state = Some(State {
        epoch: Some(1),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (peer_id.clone(), node("peer.local", 9001, 0)),
        ]),
        partitions: Default::default(),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    service
        .process(
            ManagerProtocol::Heartbeat {
                id: me.id.clone(),
                heartbeat: Heartbeat {
                    id: node_id("33333333-3333-3333-3333-333333333333"),
                    ts: 42,
                },
            },
            &mut output,
        )
        .await;

    assert!(matches!(
        output.as_slice(),
        [ManagerProtocol::GetClusterState { id }] if id == &peer_id
    ));
}

#[tokio::test]
async fn heartbeat_from_known_peer_is_forwarded_only_when_we_are_leader() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let peer_one = node_id("22222222-2222-2222-2222-222222222222");
    let peer_two = node_id("33333333-3333-3333-3333-333333333333");
    let (mut service, _config) = service(me.clone());
    let now = now_millis();
    service.state = Some(State {
        epoch: Some(1),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (peer_one.clone(), node("peer-one.local", 9001, 0)),
            (peer_two.clone(), node("peer-two.local", 9002, 0)),
        ]),
        partitions: Default::default(),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut forwarded = vec![];
    handle_heartbeat(
        &mut forwarded,
        service.state.as_mut().unwrap(),
        peer_one.clone(),
        42,
        &me,
    );

    assert!(matches!(
        forwarded.as_slice(),
        [ManagerProtocol::Heartbeat { id, heartbeat }]
            if id == &peer_two
                && heartbeat.id == peer_one
                && heartbeat.ts == 42
    ));

    service.state.as_mut().unwrap().elected_leader_id = Some(peer_two.clone());

    let mut not_forwarded = vec![];
    handle_heartbeat(
        &mut not_forwarded,
        service.state.as_mut().unwrap(),
        peer_one.clone(),
        43,
        &me,
    );

    assert!(not_forwarded.is_empty());
    assert_eq!(
        service
            .state
            .as_mut()
            .unwrap()
            .nodes
            .get_mut(&peer_one)
            .unwrap()
            .last_heartbeat,
        43
    );
}

#[tokio::test]
async fn heartbeat_from_worker_updates_worker_without_forwarding() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let worker = node_id("22222222-2222-2222-2222-222222222222");
    let manager_peer = node_id("33333333-3333-3333-3333-333333333333");
    let (mut service, _config) = service(me.clone());
    service.state = Some(State {
        epoch: Some(1),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now_millis())),
            (worker.clone(), worker_node("worker.local", 9100, 12)),
            (manager_peer.clone(), node("manager.local", 9001, 0)),
        ]),
        partitions: Default::default(),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    handle_heartbeat(
        &mut output,
        service.state.as_mut().unwrap(),
        worker.clone(),
        44,
        &me,
    );

    assert!(matches!(
        output.as_slice(),
        [ManagerProtocol::Heartbeat { id, heartbeat }]
            if id == &manager_peer && heartbeat.id == worker && heartbeat.ts == 44
    ));
    assert_eq!(
        service
            .state
            .as_mut()
            .unwrap()
            .nodes
            .get_mut(&worker)
            .unwrap()
            .last_heartbeat,
        44
    );
}

#[tokio::test]
async fn older_heartbeat_does_not_move_last_heartbeat_backwards() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let peer = node_id("22222222-2222-2222-2222-222222222222");
    let (mut service, _config) = service(me.clone());
    service.state = Some(State {
        epoch: Some(1),
        elected_leader_id: Some(peer.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, 100)),
            (peer.clone(), node("peer.local", 9001, 200)),
        ]),
        partitions: Default::default(),
        workers_with_calculated_partitions: Default::default(),
    });

    handle_heartbeat(
        &mut vec![],
        service.state.as_mut().unwrap(),
        peer.clone(),
        150,
        &me,
    );

    assert_eq!(service.state.unwrap().nodes[&peer].last_heartbeat, 200);
}

#[tokio::test]
async fn tick_emits_heartbeats_for_stale_self_heartbeat() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let peer_id = node_id("22222222-2222-2222-2222-222222222222");
    let (mut service, _config) = service(me.clone());
    let now = now_millis() - 1_000;
    service.state = Some(State {
        epoch: Some(1),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (peer_id.clone(), node("peer.local", 9001, 0)),
        ]),
        partitions: Default::default(),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    service.tick(&mut output).await;

    assert!(matches!(
        output.as_slice(),
        [ManagerProtocol::Heartbeat { id, heartbeat }] if id == &peer_id
            && heartbeat.id == me.id
    ));
}

#[tokio::test]
async fn tick_clears_stale_remote_leader_without_emitting_messages() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let leader = node_id("22222222-2222-2222-2222-222222222222");
    let now = now_millis();
    let (mut service, _config) = service(me.clone());
    service.state = Some(State {
        epoch: None,
        elected_leader_id: Some(leader.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (leader.clone(), node("leader.local", 9001, now - 1_000)),
        ]),
        partitions: Default::default(),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    service.tick(&mut output).await;

    assert!(output.is_empty());
    assert_eq!(service.state.as_ref().unwrap().elected_leader_id, None);
}
