use super::*;
use crate::common::now_millis;
use crate::manager::domain::ManagerProtocol;
use crate::manager::service::State;
use crate::manager::service::test_support::*;
use std::collections::{HashMap, HashSet};

#[tokio::test]
async fn vote_request_adds_new_election_reuses_candidate_and_ignores_stale_epochs() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let peer_one = node_id("22222222-2222-2222-2222-222222222222");
    let peer_two = node_id("33333333-3333-3333-3333-333333333333");
    let (mut service, _config) = service(me.clone());
    let now = now_millis();
    service.state = Some(State {
        epoch: Some(0),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (peer_one.clone(), node("peer-one.local", 9001, now)),
            (peer_two.clone(), node("peer-two.local", 9002, now)),
        ]),
        partitions: Default::default(),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut first = vec![];
    service
        .process(
            ManagerProtocol::VoteRequest {
                id: peer_one.clone(),
                epoch: 1,
                ts: 100,
            },
            &mut first,
        )
        .await;
    assert!(matches!(
        first.as_slice(),
        [ManagerProtocol::VoteResponse { id, leader_id, ts }]
            if id == &peer_one && leader_id == &peer_one && *ts == 100
    ));

    let mut second = vec![];
    service
        .process(
            ManagerProtocol::VoteRequest {
                id: peer_two.clone(),
                epoch: 1,
                ts: 200,
            },
            &mut second,
        )
        .await;

    assert!(matches!(
        second.as_slice(),
        [ManagerProtocol::VoteResponse { id, leader_id, ts }]
            if id == &peer_two && leader_id == &peer_one && *ts == 100
    ));

    let mut stale = vec![];
    service
        .process(
            ManagerProtocol::VoteRequest {
                id: peer_two.clone(),
                epoch: 0,
                ts: 300,
            },
            &mut stale,
        )
        .await;
    assert!(stale.is_empty());
}

#[tokio::test]
async fn vote_response_for_unknown_leader_requests_cluster_state() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let peer = node_id("22222222-2222-2222-2222-222222222222");
    let unknown_leader = node_id("33333333-3333-3333-3333-333333333333");
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
    service.elections.insert(
        1,
        Election::Mine {
            ts: 100,
            approvers: HashSet::new(),
        },
    );

    let mut output = vec![];
    handle_vote_response(
        &mut output,
        service.state.as_mut().unwrap(),
        peer.clone(),
        unknown_leader,
        100,
        &me,
        &mut service.elections,
    );

    assert!(matches!(
        output.as_slice(),
        [ManagerProtocol::GetClusterState { id }] if id == &peer
    ));
}

#[tokio::test]
async fn vote_responses_elect_self_and_broadcast_leader() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let peer_one = node_id("22222222-2222-2222-2222-222222222222");
    let peer_two = node_id("33333333-3333-3333-3333-333333333333");
    let (mut service, _config) = service(me.clone());
    let now = now_millis();
    service.state = Some(State {
        epoch: Some(0),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (peer_one.clone(), node("peer-one.local", 9001, 0)),
            (peer_two.clone(), node("peer-two.local", 9002, 0)),
        ]),
        partitions: Default::default(),
        workers_with_calculated_partitions: Default::default(),
    });
    service.elections.insert(
        1,
        Election::Mine {
            ts: 100,
            approvers: HashSet::new(),
        },
    );

    let mut first = vec![];
    handle_vote_response(
        &mut first,
        service.state.as_mut().unwrap(),
        peer_one.clone(),
        me.id.clone(),
        100,
        &me,
        &mut service.elections,
    );
    assert!(first.is_empty());

    let mut second = vec![];
    handle_vote_response(
        &mut second,
        service.state.as_mut().unwrap(),
        peer_two.clone(),
        me.id.clone(),
        100,
        &me,
        &mut service.elections,
    );

    assert_eq!(
        service.state.as_ref().unwrap().elected_leader_id,
        Some(me.id.clone())
    );
    assert_eq!(service.state.as_ref().unwrap().epoch, Some(1));
    assert!(matches!(
        second.as_slice(),
        [
            ManagerProtocol::Leader { id, epoch, ts },
            ManagerProtocol::Leader { id: id2, epoch: epoch2, ts: ts2 }
        ] if *epoch == 1
            && *ts == 100
            && *epoch2 == 1
            && *ts2 == 100
            && ((id == &peer_one && id2 == &peer_two) || (id == &peer_two && id2 == &peer_one))
    ));
}

#[tokio::test]
async fn vote_responses_complete_election_with_worker_nodes_present() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let manager_peer = node_id("22222222-2222-2222-2222-222222222222");
    let worker_peer = node_id("33333333-3333-3333-3333-333333333333");
    let (mut service, _config) = service(me.clone());
    let now = now_millis();
    service.state = Some(State {
        epoch: Some(0),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (manager_peer.clone(), node("manager.local", 9001, 0)),
            (worker_peer.clone(), worker_node("worker.local", 9100, 0)),
        ]),
        partitions: Default::default(),
        workers_with_calculated_partitions: Default::default(),
    });
    service.elections.insert(
        1,
        Election::Mine {
            ts: 100,
            approvers: HashSet::new(),
        },
    );

    let mut output = vec![];
    handle_vote_response(
        &mut output,
        service.state.as_mut().unwrap(),
        manager_peer.clone(),
        me.id.clone(),
        100,
        &me,
        &mut service.elections,
    );

    assert_eq!(
        service.state.as_ref().unwrap().elected_leader_id,
        Some(me.id.clone())
    );
    assert_eq!(service.state.as_ref().unwrap().epoch, Some(1));
    assert_eq!(output.len(), 2);
    assert!(
        output
            .iter()
            .any(|msg| matches!(msg, ManagerProtocol::Leader { id, .. } if id == &manager_peer))
    );
    assert!(
        output
            .iter()
            .any(|msg| matches!(msg, ManagerProtocol::Leader { id, .. } if id == &worker_peer))
    );
}

#[tokio::test]
async fn leader_message_updates_epoch_and_clears_pending_elections() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let leader = node_id("22222222-2222-2222-2222-222222222222");
    let (mut service, _config) = service(me.clone());
    let now = now_millis();
    service.state = Some(State {
        epoch: Some(1),
        elected_leader_id: None,
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (leader.clone(), node("leader.local", 9001, now)),
        ]),
        partitions: Default::default(),
        workers_with_calculated_partitions: Default::default(),
    });
    service.elections.insert(
        2,
        Election::Mine {
            ts: now,
            approvers: HashSet::from([leader.clone()]),
        },
    );

    handle_leader(
        &mut Vec::new(),
        service.state.as_mut().unwrap(),
        leader.clone(),
        2,
        now + 10,
        &me,
        &mut service.elections,
    );

    let state = service.state.as_mut().unwrap();
    assert_eq!(state.epoch, Some(2));
    assert_eq!(state.elected_leader_id, Some(leader.clone()));
    assert_eq!(
        state.nodes.get_mut(&leader).unwrap().last_heartbeat,
        now + 10
    );
    assert!(service.elections.is_empty());
}

#[tokio::test]
async fn leader_with_unknown_id_requests_cluster_state_from_manager_peers_only() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let manager_peer = node_id("22222222-2222-2222-2222-222222222222");
    let worker_peer = node_id("33333333-3333-3333-3333-333333333333");
    let leader = node_id("44444444-4444-4444-4444-444444444444");
    let now = now_millis();
    let (mut service, _config) = service(me.clone());
    service.state = Some(State {
        epoch: Some(1),
        elected_leader_id: None,
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (manager_peer.clone(), node("manager.local", 9001, now)),
            (worker_peer.clone(), worker_node("worker.local", 9100, now)),
        ]),
        partitions: Default::default(),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    handle_leader(
        &mut output,
        service.state.as_mut().unwrap(),
        leader,
        2,
        now + 10,
        &me,
        &mut service.elections,
    );

    assert!(matches!(
        output.as_slice(),
        [ManagerProtocol::GetClusterState { id }] if id == &manager_peer
    ));
}

#[tokio::test]
async fn leader_messages_only_move_state_forward() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let leader = node_id("22222222-2222-2222-2222-222222222222");
    let now = now_millis();
    let (mut service, _config) = service(me.clone());
    service.state = Some(State {
        epoch: Some(1),
        elected_leader_id: None,
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (leader.clone(), node("leader.local", 9001, now)),
        ]),
        partitions: Default::default(),
        workers_with_calculated_partitions: Default::default(),
    });

    handle_leader(
        &mut vec![],
        service.state.as_mut().unwrap(),
        leader.clone(),
        2,
        now,
        &me,
        &mut service.elections,
    );
    assert_eq!(service.state.as_ref().unwrap().epoch, Some(2));
    assert_eq!(
        service.state.as_ref().unwrap().elected_leader_id,
        Some(leader.clone())
    );
    assert_eq!(
        service
            .state
            .as_mut()
            .unwrap()
            .nodes
            .get_mut(&leader)
            .unwrap()
            .last_heartbeat,
        now
    );

    handle_leader(
        &mut vec![],
        service.state.as_mut().unwrap(),
        me.id.clone(),
        1,
        88,
        &me,
        &mut service.elections,
    );
    assert_eq!(service.state.as_ref().unwrap().epoch, Some(2));
    assert_eq!(
        service.state.as_ref().unwrap().elected_leader_id,
        Some(leader)
    );
}

#[tokio::test]
async fn tick_starts_election_when_leader_is_missing() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let peer = node_id("22222222-2222-2222-2222-222222222222");
    let (mut service, _config) = service(me.clone());
    service.state = Some(State {
        epoch: Some(4),
        elected_leader_id: None,
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now_millis())),
            (peer.clone(), node("peer.local", 9001, 0)),
        ]),
        partitions: Default::default(),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    service.tick(&mut output).await;

    assert!(matches!(
        output.as_slice(),
        [ManagerProtocol::VoteRequest { id, epoch, .. }] if id == &peer && *epoch == 5
    ));
    assert!(matches!(
        service.elections.last_key_value(),
        Some((5, Election::Mine { .. }))
    ));
}

#[tokio::test]
async fn tick_starts_a_new_election_only_for_manager_peers() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let manager_peer = node_id("22222222-2222-2222-2222-222222222222");
    let worker_peer = node_id("33333333-3333-3333-3333-333333333333");
    let (mut service, _config) = service(me.clone());
    let future = now_millis() + 10_000;
    service.state = Some(State {
        epoch: Some(7),
        elected_leader_id: None,
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, future)),
            (manager_peer.clone(), node("manager.local", 9001, 0)),
            (worker_peer.clone(), worker_node("worker.local", 9100, 0)),
        ]),
        partitions: Default::default(),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    service.tick(&mut output).await;

    assert!(matches!(
        output.as_slice(),
        [ManagerProtocol::VoteRequest { id, epoch, .. }] if id == &manager_peer && *epoch == 8
    ));
    assert!(matches!(
        service.elections.last_key_value(),
        Some((8, Election::Mine { .. }))
    ));
}
