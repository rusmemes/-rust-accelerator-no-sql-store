//! Manager orchestration for the node cluster.
//!
//! The manager is responsible for starting the gRPC server, running the
//! cluster state machine, and coordinating shutdown. It owns the service
//! loop that reacts to cluster events such as node joins, heartbeats, leader
//! elections, and disconnections.

use crate::common::{Config, Me};
use crate::manager::grpc::start_server;
use crate::manager::service::start_service;
use std::sync::Arc;
use tokio::select;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

mod grpc;

mod domain;
mod service;

/// Runs the manager process for the current node.
///
/// This initializes the local identity, starts the gRPC server and the
/// internal service loop, then waits until one of the components stops or
/// the process receives a termination signal.
pub async fn run(config: Config) -> anyhow::Result<()> {
    let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("SIGINT handler");

    const CHANNEL_BUFFER_SIZE: usize = 100;
    let (to_gprc, from_manager) = tokio::sync::mpsc::channel(CHANNEL_BUFFER_SIZE);
    let (to_manager, from_grpc) = tokio::sync::mpsc::channel(CHANNEL_BUFFER_SIZE);

    let (host, port) = &config.self_host_port();
    let me = Me::new(host.clone(), *port as u32);

    tracing::info!("Starting manager {:?}", me);

    let config = Arc::new(RwLock::new(config));

    let cancellation_token = CancellationToken::new();
    let grpc_join_handle = start_server(
        config.clone(),
        me.clone(),
        (to_manager, from_manager),
        cancellation_token.child_token(),
    );

    let service_join_handle = start_service(
        me,
        config,
        (to_gprc, from_grpc),
        cancellation_token.child_token(),
    );

    select! {
        res = grpc_join_handle => {
            if let Err(e) = res {
                tracing::error!("GRPC server failed: {}", e);
            }
        },
        _ = service_join_handle => tracing::info!("Manager service stopped"),
        _ = sigterm.recv() => tracing::info!("SIGTERM received"),
        _ = sigint.recv() => tracing::info!("SIGINT received"),
        _ = cancellation_token.cancelled() => {},
    }

    cancellation_token.cancel();

    tracing::info!("Stopping manager");

    Ok(())
}
