use anyhow::{Context, Result, anyhow, bail};
use no_sql_store_client::{Client, ClientConfig};
use std::collections::HashSet;
use std::future::Future;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;

use no_sql_store_proto::common::v1::{ClusterState, NodeType};
use no_sql_store_proto::manager_api::v1::{ClientConnect, manager_api_client::ManagerApiClient};
use no_sql_store_proto::worker_api::v1::{
    ClientEvent, ClientRequest, Record, RequestType, client_event::Payload,
    worker_api_client::WorkerApiClient,
};

const HOST: &str = "127.0.0.1";

struct Process(Child);

impl Process {
    fn stop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        self.stop();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn all_public_client_scenarios() -> Result<()> {
    let binary = server_binary()?;
    let manager_port = free_port()?;
    let second_manager_port = free_port()?;
    let third_manager_port = free_port()?;
    let worker_one_port = free_port()?;
    let worker_two_port = free_port()?;

    let _manager = start_manager(&binary, manager_port, None)?;
    let manager_endpoint = endpoint(manager_port);
    wait_for_endpoint(&manager_endpoint).await?;

    let _second_manager = start_manager(&binary, second_manager_port, Some((HOST, manager_port)))?;
    let _third_manager = start_manager(&binary, third_manager_port, Some((HOST, manager_port)))?;
    wait_for_state(&manager_endpoint, Duration::from_secs(15), |state| {
        state
            .nodes
            .iter()
            .filter(|node| node.node_type == NodeType::Manager as i32)
            .count()
            >= 3
    })
    .await
    .context("manager topology did not become ready")?;

    let mut worker_one = start_worker(&binary, worker_one_port, manager_port)?;
    let _worker_two = start_worker(&binary, worker_two_port, manager_port)?;

    let config = ClientConfig {
        retry_attempts: 3,
        initial_retry_delay: Duration::from_millis(30),
        request_timeout: Duration::from_millis(500),
        manager_connect_timeout: Duration::from_secs(2),
    };
    let client = connect_eventually(&manager_endpoint, config.clone()).await?;
    let state = wait_for_state(&manager_endpoint, Duration::from_secs(15), |state| {
        worker_nodes(state).count() == 2
            && state
                .partitions
                .as_ref()
                .is_some_and(|p| !p.mapping.is_empty())
    })
    .await
    .context("worker topology did not become ready")?;

    client.put(42_u64, b"numeric".to_vec(), None).await?;
    assert_eq!(client.get(42_u64).await?, Some(b"numeric".to_vec()));

    client.put("customer:42", b"string".to_vec(), None).await?;
    assert_eq!(client.get("customer:42").await?, Some(b"string".to_vec()));
    client.delete("customer:42").await?;
    assert_eq!(client.get("customer:42").await?, None);

    client
        .put(
            "short-lived",
            b"ttl".to_vec(),
            Some(Duration::from_millis(100)),
        )
        .await?;
    sleep(Duration::from_millis(150)).await;
    assert_eq!(client.get("short-lived").await?, None);

    let partition = state
        .partitions
        .as_ref()
        .and_then(|partitions| {
            partitions
                .mapping
                .iter()
                .find(|(_, p)| !p.replicas.is_empty())
        })
        .map(|(id, _)| *id)
        .context("no replicated partition")?;
    let key = u64::from(partition);
    let mapping = state
        .partitions
        .as_ref()
        .unwrap()
        .mapping
        .get(&partition)
        .unwrap();
    client.put(key, b"replicated".to_vec(), None).await?;

    let mut holders: HashSet<_> = mapping.replicas.iter().cloned().collect();
    holders.insert(mapping.master.clone());
    for node_id in &holders {
        let address = worker_endpoint(&state, node_id)?;
        let record = raw_worker_request(&address, RequestType::Get, key, None, None).await?;
        assert_eq!(
            record.map(|record| record.value),
            Some(b"replicated".to_vec())
        );
    }

    client.delete(key).await?;
    for node_id in &holders {
        let address = worker_endpoint(&state, node_id)?;
        assert!(
            raw_worker_request(&address, RequestType::Get, key, None, None)
                .await?
                .is_none()
        );
    }

    wait_for_state(&manager_endpoint, Duration::from_secs(15), |state| {
        state
            .nodes
            .iter()
            .filter(|node| node.node_type == NodeType::Manager as i32)
            .count()
            >= 3
    })
    .await?;
    let client_via_discovered_manager =
        connect_eventually(&endpoint(second_manager_port), config.clone()).await?;
    assert_eq!(
        client_via_discovered_manager.get(42_u64).await?,
        Some(b"numeric".to_vec())
    );

    let worker_three_port = free_port()?;
    let _worker_three = start_worker(&binary, worker_three_port, manager_port)?;
    let expanded = wait_for_state(&manager_endpoint, Duration::from_secs(15), |state| {
        worker_nodes(state).count() == 3
    })
    .await?;
    let worker_three_id = expanded
        .nodes
        .iter()
        .find(|node| {
            node.addr
                .as_ref()
                .is_some_and(|addr| addr.port == u32::from(worker_three_port))
        })
        .map(|node| node.id.clone())
        .context("third worker missing from topology")?;
    let moved_partition = expanded
        .partitions
        .as_ref()
        .unwrap()
        .mapping
        .iter()
        .find(|(_, mapping)| {
            mapping.master == worker_three_id || mapping.replicas.contains(&worker_three_id)
        })
        .map(|(id, _)| *id)
        .context("third worker has no partition")?;
    let moved_key = u64::from(moved_partition);
    retry_until(Duration::from_secs(10), || {
        let client = client.clone();
        async move {
            client
                .put(moved_key, b"topology-update".to_vec(), None)
                .await
                .map_err(anyhow::Error::from)
        }
    })
    .await?;
    assert_eq!(
        client.get(moved_key).await?,
        Some(b"topology-update".to_vec())
    );

    // A failed replica/master must make a replicated write fail after retries.
    worker_one.stop();
    let key_on_stopped_worker = expanded
        .partitions
        .as_ref()
        .unwrap()
        .mapping
        .iter()
        .find(|(_, mapping)| {
            let ids = std::iter::once(&mapping.master).chain(mapping.replicas.iter());
            ids.filter_map(|id| expanded.nodes.iter().find(|node| &node.id == id))
                .any(|node| {
                    node.addr
                        .as_ref()
                        .is_some_and(|addr| addr.port == u32::from(worker_one_port))
                })
        })
        .map(|(id, _)| u64::from(*id))
        .context("stopped worker has no partition")?;
    assert!(
        client
            .put(key_on_stopped_worker, b"must-fail".to_vec(), None)
            .await
            .is_err()
    );

    Ok(())
}

fn server_binary() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("NO_SQL_STORE_BIN") {
        return Ok(path.into());
    }
    let mut path = std::env::current_exe()?;
    path.pop();
    if path.ends_with("deps") {
        path.pop();
    }
    path.push(format!("no-sql-store{}", std::env::consts::EXE_SUFFIX));
    if !path.is_file() {
        let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|crates| crates.parent())
            .context("could not find workspace root")?
            .to_owned();
        let status = Command::new(env!("CARGO"))
            .args(["build", "-p", "no-sql-store"])
            .current_dir(workspace)
            .status()
            .context("could not build no-sql-store server binary")?;
        if !status.success() || !path.is_file() {
            bail!("server binary was not produced at {}", path.display());
        }
    }
    Ok(path)
}

fn start_manager(binary: &PathBuf, port: u16, peer: Option<(&str, u16)>) -> Result<Process> {
    let mut command = Command::new(binary);
    command.args([
        "manager",
        "--grpc-port",
        &port.to_string(),
        "--self-host",
        HOST,
        "--replication-factor",
        "2",
    ]);
    if let Some((host, peer_port)) = peer {
        command.args([
            "--manager-host",
            host,
            "--manager-port",
            &peer_port.to_string(),
        ]);
    }
    spawn(command)
}

fn start_worker(binary: &PathBuf, port: u16, manager_port: u16) -> Result<Process> {
    let mut command = Command::new(binary);
    command.args([
        "worker",
        "--grpc-port",
        &port.to_string(),
        "--self-host",
        HOST,
        "--manager-host",
        HOST,
        "--manager-port",
        &manager_port.to_string(),
        "--expired-cleanup-interval-secs",
        "1",
    ]);
    spawn(command)
}

fn spawn(mut command: Command) -> Result<Process> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    Ok(Process(command.spawn()?))
}

fn free_port() -> Result<u16> {
    Ok(TcpListener::bind((HOST, 0))?.local_addr()?.port())
}

fn endpoint(port: u16) -> String {
    format!("http://{HOST}:{port}")
}

async fn connect_eventually(endpoint: &str, config: ClientConfig) -> Result<Client> {
    retry_until(Duration::from_secs(15), || {
        let endpoint = endpoint.to_owned();
        let config = config.clone();
        async move {
            Client::connect_with_config([endpoint], config)
                .await
                .map_err(anyhow::Error::from)
        }
    })
    .await
}

async fn wait_for_endpoint(endpoint: &str) -> Result<()> {
    retry_until(Duration::from_secs(15), || async {
        tonic::transport::Endpoint::from_shared(endpoint.to_owned())?
            .connect()
            .await?;
        Ok(())
    })
    .await
    .context("manager gRPC endpoint did not become ready")
}

async fn wait_for_state(
    endpoint: &str,
    duration: Duration,
    predicate: impl Fn(&ClusterState) -> bool,
) -> Result<ClusterState> {
    retry_until(duration, || async {
        let state = manager_snapshot(endpoint).await?;
        if predicate(&state) {
            Ok(state)
        } else {
            Err(anyhow!("topology not ready"))
        }
    })
    .await
}

async fn manager_snapshot(endpoint: &str) -> Result<ClusterState> {
    let channel = tonic::transport::Channel::from_shared(endpoint.to_owned())?
        .connect()
        .await?;
    let mut stream = ManagerApiClient::new(channel)
        .open_client_connection(ClientConnect {
            id: uuid::Uuid::now_v7().to_string(),
        })
        .await?
        .into_inner();
    stream
        .message()
        .await?
        .and_then(|event| event.cluster_state)
        .context("manager returned no state")
}

fn worker_nodes(
    state: &ClusterState,
) -> impl Iterator<Item = &no_sql_store_proto::common::v1::Node> {
    state
        .nodes
        .iter()
        .filter(|node| node.node_type == NodeType::Worker as i32)
}

fn worker_endpoint(state: &ClusterState, id: &str) -> Result<String> {
    let address = state
        .nodes
        .iter()
        .find(|node| node.id == id)
        .and_then(|node| node.addr.as_ref())
        .context("worker address missing")?;
    Ok(format!("http://{}:{}", address.host, address.port))
}

async fn raw_worker_request(
    endpoint: &str,
    request_type: RequestType,
    key: u64,
    value: Option<Vec<u8>>,
    creation_time: Option<u64>,
) -> Result<Option<Record>> {
    let channel = tonic::transport::Channel::from_shared(endpoint.to_owned())?
        .connect()
        .await?;
    let (tx, rx) = mpsc::channel(1);
    let mut stream = WorkerApiClient::new(channel)
        .open_client_connection(ReceiverStream::new(rx))
        .await?
        .into_inner();
    tx.send(ClientEvent {
        payload: Some(Payload::Request(ClientRequest {
            id: 1,
            request_type: request_type as i32,
            key,
            creation_time,
            value,
            ttl: None,
        })),
    })
    .await?;
    match stream.message().await? {
        Some(ClientEvent {
            payload: Some(Payload::Response(response)),
        }) => Ok(response.record),
        other => bail!("unexpected worker response: {other:?}"),
    }
}

async fn retry_until<T, F, Fut>(duration: Duration, mut operation: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let deadline = Instant::now() + duration;
    let mut last_error = None;
    while Instant::now() < deadline {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) => last_error = Some(error),
        }
        sleep(Duration::from_millis(100)).await;
    }
    Err(last_error.unwrap_or_else(|| anyhow!("operation timed out")))
}
