use super::*;
use crate::conversions::common::v1::GetState;
use crate::manager::domain::ManagerProtocol;
use crate::manager::grpc::test_support::*;
use std::time::Duration;
use tokio::time::timeout;
use tokio_stream::wrappers::ReceiverStream;

#[tokio::test]
async fn input_from_worker_forwards_messages_and_stops_when_stream_ends() {
    let worker_id = node_id("22222222-2222-2222-2222-222222222222");
    let (protocol_tx, mut protocol_rx) = tokio::sync::mpsc::channel(8);
    let (request_tx, request_rx) = tokio::sync::mpsc::channel(4);
    let stream = ReceiverStream::new(request_rx);
    let worker_id_clone = worker_id.clone();
    let worker_task = tokio::spawn(async move {
        input_from_worker(
            stream,
            &worker_id_clone,
            "worker.local".to_string(),
            9100,
            protocol_tx,
        )
        .await;
    });

    let new_connection = timeout(Duration::from_secs(1), protocol_rx.recv())
        .await
        .expect("new connection timeout")
        .expect("new connection");
    assert!(matches!(
        new_connection,
        ManagerProtocol::NewConnection {
            id: Some(id),
            host,
            port,
            manager: false,
        } if id == worker_id && host == "worker.local" && port == 9100
    ));

    request_tx
        .send(Ok(WorkerEvent {
            payload: Some(worker_event::Payload::Heartbeat(GrpcHeartbeat {
                id: worker_id.to_string(),
                ts: 44,
            })),
        }))
        .await
        .expect("heartbeat message");

    request_tx
        .send(Ok(WorkerEvent {
            payload: Some(worker_event::Payload::GetClusterState(GetState {})),
        }))
        .await
        .expect("cluster state message");
    drop(request_tx);

    let heartbeat = timeout(Duration::from_secs(1), protocol_rx.recv())
        .await
        .expect("heartbeat timeout")
        .expect("heartbeat");
    assert!(matches!(
        heartbeat,
        ManagerProtocol::Heartbeat {
            id: recipient_id,
            heartbeat: Heartbeat { id, ts },
        } if recipient_id == worker_id && id == worker_id && ts == 44
    ));

    let get_cluster_state = timeout(Duration::from_secs(1), protocol_rx.recv())
        .await
        .expect("cluster state timeout")
        .expect("cluster state");
    assert!(matches!(
        get_cluster_state,
        ManagerProtocol::GetClusterState { id } if id == worker_id
    ));

    timeout(Duration::from_secs(1), worker_task)
        .await
        .expect("worker task timeout")
        .expect("worker task");
}
