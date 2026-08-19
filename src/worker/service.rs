use crate::common::{ClusterState, Config, Heartbeat, Me, Node, NodeType, now_millis};
use crate::worker::domain::WorkerProtocol;
use crate::worker::runtime_store::RuntimeStore;
use crate::worker::service::cluster_state::{handle_cluster_state, handle_remove_old_partition};
use crate::worker::service::connection::{handle_new_connection, handle_node_disconnected};
use crate::worker::service::election::handle_leader;
use crate::worker::service::heartbeat::{handle_heartbeat, heartbeats};
use crate::worker::service::partitions::{handle_sync_batch, handle_sync_batch_response, sync_partitions};
use crate::worker::service::state::State;
use std::collections::HashMap;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::sync::CancellationToken;

mod cluster_state;
mod connection;
mod election;
mod heartbeat;
mod state;
mod partitions;

struct WorkerService {
    me: Me,
    state: Option<State>,
    config: Config,
    runtime_store: RuntimeStore,
}

impl WorkerService {
    pub fn new(me: Me, config: Config, runtime_store: RuntimeStore) -> Self {
        Self {
            me,
            state: Default::default(),
            config,
            runtime_store,
        }
    }

    async fn get_init_messages(&mut self) -> Vec<WorkerProtocol> {
        let mut output = vec![];
        if let None = self.state {
            let mut nodes = HashMap::new();
            nodes.insert(
                self.me.id.clone(),
                Node {
                    host: self.me.host.clone(),
                    port: self.me.port,
                    last_heartbeat: now_millis(),
                    node_type: NodeType::Worker,
                },
            );

            let (host, port) = self
                .config
                .manager_host_port()
                .expect("Worker cannot run without connection options");

            output.push(WorkerProtocol::NewConnection {
                id: None,
                host: host.clone(),
                port: (*port) as u32,
                manager: true,
            })
        }
        output
    }

    async fn tick(&mut self, output: &mut Vec<WorkerProtocol>) {
        if let Some(state) = self.state.as_mut() {
            heartbeats(state, output, &self.me);
            sync_partitions(state, output, &self.runtime_store, &self.me);
        }
        tracing::debug!("state: {:?}", self.state);
    }

    async fn process(&mut self, msg: WorkerProtocol, output: &mut Vec<WorkerProtocol>) {
        if let Some(state) = self.state.as_mut() {
            match msg {
                WorkerProtocol::NewConnection {
                    id,
                    host,
                    port,
                    manager,
                } => handle_new_connection(output, state, id, host, port, &self.me, manager),
                WorkerProtocol::Heartbeat {
                    heartbeat: Heartbeat { id, ts },
                    ..
                } => handle_heartbeat(output, state, id, ts, &self.me),
                WorkerProtocol::GetClusterState { .. } => {
                    tracing::error!("GetClusterState received on the worker {}", self.me.id);
                }
                WorkerProtocol::ClusterState {
                    state:
                        ClusterState {
                            epoch,
                            leader_id,
                            nodes: items,
                            partitions,
                        }
                } => handle_cluster_state(output, state, epoch, leader_id, items, partitions),
                WorkerProtocol::NodeDisconnected { id } => {
                    handle_node_disconnected(state, id, &self.me)
                }
                WorkerProtocol::Leader { id, epoch, ts } => {
                    handle_leader(output, state, id, epoch, ts, &self.me)
                }
                WorkerProtocol::RemovePartitionFromReplica { replica_id, .. } => {
                    handle_remove_old_partition(state, replica_id, output, &self.me)
                }
                WorkerProtocol::SyncBatch { request, .. } => {
                    handle_sync_batch(output, &request, &self.runtime_store);
                }
                WorkerProtocol::SyncBatchResponse { recipient_id, partition_id_to_max_applied_key } => {
                    handle_sync_batch_response(state, partition_id_to_max_applied_key, recipient_id);
                }
            }
        }
        self.tick(output).await
    }
}

/// Runs the worker service event loop.
///
/// The service owns the in-memory cluster state machine. It consumes protocol
/// messages from gRPC, emits outbound protocol messages, and performs periodic heartbeat.
pub async fn start_service(
    me: Me,
    config: Config,
    (tx, mut rx): (Sender<WorkerProtocol>, Receiver<WorkerProtocol>),
    cancellation_token: CancellationToken,
    runtime_store: RuntimeStore,
) {
    let mut service = WorkerService::new(me, config, runtime_store);
    for msg in service.get_init_messages().await {
        if let Err(e) = tx.send(msg).await {
            tracing::error!("Error sending response: {}", e);
            return;
        }
    }

    tracing::info!("Manager service started");
    let mut ticker = tokio::time::interval(Duration::from_millis(100));
    let mut output = vec![];
    loop {
        select! {
            biased;
            _ = cancellation_token.cancelled() => {
                tracing::info!("Manager service stopped");
                break;
            }
            node_protocol = rx.recv() => {
                if let Some(message) = node_protocol {
                    tracing::debug!("input: {:?}", message);
                    service.process(message, &mut output).await;
                    for msg in output.drain(..) {
                        if let Err(e) = tx.send(msg).await {
                            tracing::error!("Error sending response: {}", e);
                        }
                    }
                }
            }
            _ = ticker.tick() => {
                service.tick(&mut output).await;
                for msg in output.drain(..) {
                    if let Err(e) = tx.send(msg).await {
                        tracing::error!("Error sending response: {}", e);
                    }
                }
            }
        }
    }
}
