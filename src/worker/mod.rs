mod domain;
mod grpc;
mod runtime_store;
mod service;

use crate::common::{Config, Me};
use crate::worker::grpc::start_server;
use crate::worker::runtime_store::RuntimeStore;
use crate::worker::service::start_service;
use tokio::select;
use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;

pub async fn run(config: Config) -> anyhow::Result<()> {
    let _ = config
        .manager_host_port()
        .ok_or_else(|| anyhow::anyhow!("Manager host and port are not specified"))?;

    let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("SIGINT handler");

    const CHANNEL_BUFFER_SIZE: usize = 100;
    let (to_grpc, from_worker) = tokio::sync::mpsc::channel(CHANNEL_BUFFER_SIZE);
    let (to_worker, from_grpc) = tokio::sync::mpsc::channel(CHANNEL_BUFFER_SIZE);

    let (host, port) = config.self_host_port();
    let me = Me::new(host.clone(), *port as u32);

    tracing::info!("Starting worker {:?}", me);

    let runtime_store = RuntimeStore::new();
    let cancellation_token = CancellationToken::new();
    let grpc_join_handle = start_server(
        config.clone(),
        me.clone(),
        (to_worker, from_worker),
        cancellation_token.child_token(),
        runtime_store.clone(),
    );
    let service_join_handle = start_service(
        me,
        config,
        (to_grpc, from_grpc),
        cancellation_token.child_token(),
        runtime_store,
    );

    select! {
        res = grpc_join_handle => {
            if let Err(e) = res {
                tracing::error!("GRPC server failed: {}", e);
            }
        },
        _ = service_join_handle => tracing::info!("Worker service stopped"),
        _ = sigterm.recv() => tracing::info!("SIGTERM received"),
        _ = sigint.recv() => tracing::info!("SIGINT received"),
        _ = cancellation_token.cancelled() => {},
    }

    cancellation_token.cancel();

    tracing::info!("Stopping worker");

    Ok(())
}
