mod domain;
mod grpc;
mod runtime_store;
mod service;

use crate::common::{Config, Me};
use crate::worker::grpc::start_server;
use crate::worker::runtime_store::RuntimeStore;
use crate::worker::service::start_service;
use std::time::Duration;
use tokio::select;
use tokio::signal::unix::{SignalKind, signal};
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

fn start_expired_cleanup(
    runtime_store: RuntimeStore,
    interval: Duration,
    cancellation_token: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval_at(Instant::now() + interval, interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            select! {
                _ = cancellation_token.cancelled() => break,
                _ = ticker.tick() => {
                    let store = runtime_store.clone();
                    match tokio::task::spawn_blocking(move || {
                        store.remove_expired(crate::common::now_millis())
                    }).await {
                        Ok(removed) if removed > 0 => {
                            tracing::info!("Removed {removed} expired worker records");
                        }
                        Ok(_) => {}
                        Err(error) => {
                            tracing::error!("Expired-record cleanup task failed: {error}");
                        }
                    }
                }
            }
        }
    })
}

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
    let expired_cleanup_interval = Duration::from_secs(config.expired_cleanup_interval_secs());
    let cancellation_token = CancellationToken::new();
    let cleanup_join_handle = start_expired_cleanup(
        runtime_store.clone(),
        expired_cleanup_interval,
        cancellation_token.child_token(),
    );
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
        _ = cleanup_join_handle => tracing::info!("Expired-record cleanup stopped"),
        _ = sigterm.recv() => tracing::info!("SIGTERM received"),
        _ = sigint.recv() => tracing::info!("SIGINT received"),
        _ = cancellation_token.cancelled() => {},
    }

    cancellation_token.cancel();

    tracing::info!("Stopping worker");

    Ok(())
}
