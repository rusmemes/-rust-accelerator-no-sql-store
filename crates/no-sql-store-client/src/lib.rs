//! Asynchronous topology-aware client for a `no-sql-store` cluster.

mod connection;
mod proto;

use connection::WorkerConnection;
use futures::future::join_all;
use proto::common::v1::{ClusterState, NodeType, Partition};
use proto::manager_api::v1::{ClientConnect, manager_api_client::ManagerApiClient};
use proto::worker_api::v1::{ClientRequest, Record, RequestType};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, RwLock};
use tokio::time::{sleep, timeout};
use tonic::transport::Channel;

const PARTITIONS_AMOUNT: u64 = 4096;
const KEY_HASH_DOMAIN: &[u8] = b"no-sql-store:key:v1\0";

/// A key that can be mapped to the `u64` representation used by worker nodes.
///
/// Integer keys retain their original value. Strings and byte slices use a
/// stable, domain-separated BLAKE3 hash truncated to 64 bits.
pub trait StoreKey {
    fn to_store_key(&self) -> u64;
}

impl StoreKey for u64 {
    fn to_store_key(&self) -> u64 {
        *self
    }
}

macro_rules! integer_store_keys {
    ($($type:ty),+ $(,)?) => {
        $(
            impl StoreKey for $type {
                fn to_store_key(&self) -> u64 {
                    *self as u64
                }
            }
        )+
    };
}

integer_store_keys!(u8, u16, u32, usize, i8, i16, i32, i64, isize);

impl StoreKey for str {
    fn to_store_key(&self) -> u64 {
        hash_key(self.as_bytes())
    }
}

impl StoreKey for String {
    fn to_store_key(&self) -> u64 {
        self.as_str().to_store_key()
    }
}

impl StoreKey for [u8] {
    fn to_store_key(&self) -> u64 {
        hash_key(self)
    }
}

impl<const N: usize> StoreKey for [u8; N] {
    fn to_store_key(&self) -> u64 {
        self.as_slice().to_store_key()
    }
}

impl<T: StoreKey + ?Sized> StoreKey for &T {
    fn to_store_key(&self) -> u64 {
        (*self).to_store_key()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("at least one manager address is required")]
    NoManagerAddress,
    #[error("could not connect to any configured manager: {0}")]
    ManagerUnavailable(String),
    #[error("cluster has no mapping for partition {0}")]
    PartitionUnavailable(u32),
    #[error("cluster state references unknown worker {0}")]
    UnknownWorker(String),
    #[error("worker {node_id} request failed: {reason}")]
    WorkerRequest { node_id: String, reason: String },
    #[error("operation timed out")]
    Timeout,
    #[error("system clock is before the Unix epoch")]
    InvalidSystemTime,
    #[error("TTL is too large")]
    TtlOverflow,
}

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub retry_attempts: usize,
    pub initial_retry_delay: Duration,
    pub request_timeout: Duration,
    pub manager_connect_timeout: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            retry_attempts: 3,
            initial_retry_delay: Duration::from_millis(50),
            request_timeout: Duration::from_secs(5),
            manager_connect_timeout: Duration::from_secs(5),
        }
    }
}

#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

struct Inner {
    id: String,
    config: ClientConfig,
    state: RwLock<Topology>,
    workers: RwLock<HashMap<String, Arc<WorkerConnection>>>,
    managers: Mutex<HashSet<String>>,
    next_request_id: AtomicI32,
}

#[derive(Default)]
struct Topology {
    epoch: u64,
    leader_id: String,
    nodes: HashMap<String, Node>,
    mapping: HashMap<u32, Partition>,
    old_replicas: HashMap<u32, Vec<String>>,
    new_replicas: HashMap<u32, Vec<String>>,
}

#[derive(Clone)]
struct Node {
    endpoint: String,
    node_type: i32,
}

impl Client {
    pub async fn connect<I, A>(manager_addresses: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = A>,
        A: Into<String>,
    {
        Self::connect_with_config(manager_addresses, ClientConfig::default()).await
    }

    pub async fn connect_with_config<I, A>(
        manager_addresses: I,
        config: ClientConfig,
    ) -> Result<Self, Error>
    where
        I: IntoIterator<Item = A>,
        A: Into<String>,
    {
        let addresses: Vec<_> = manager_addresses
            .into_iter()
            .map(|address| normalize_endpoint(&address.into()))
            .collect();
        if addresses.is_empty() {
            return Err(Error::NoManagerAddress);
        }

        let id = uuid::Uuid::now_v7().to_string();
        let mut errors = Vec::new();
        let mut bootstrap = None;
        for address in &addresses {
            match subscribe(address, &id, config.manager_connect_timeout).await {
                Ok(mut stream) => match timeout(config.manager_connect_timeout, stream.message())
                    .await
                {
                    Ok(Ok(Some(event))) if event.cluster_state.is_some() => {
                        bootstrap = Some((address.clone(), stream, event.cluster_state.unwrap()));
                        break;
                    }
                    Ok(Ok(_)) => {
                        errors.push(format!("{address}: stream closed before initial state"))
                    }
                    Ok(Err(error)) => errors.push(format!("{address}: {error}")),
                    Err(_) => errors.push(format!("{address}: initial state timeout")),
                },
                Err(error) => errors.push(format!("{address}: {error}")),
            }
        }

        let (bootstrap_address, bootstrap_stream, initial_state) =
            bootstrap.ok_or_else(|| Error::ManagerUnavailable(errors.join("; ")))?;
        let inner = Arc::new(Inner {
            id,
            config,
            state: RwLock::new(Topology::default()),
            workers: RwLock::new(HashMap::new()),
            managers: Mutex::new(HashSet::from([bootstrap_address.clone()])),
            next_request_id: AtomicI32::new(1),
        });
        inner.apply_state(initial_state).await;
        spawn_manager_stream(inner.clone(), bootstrap_address, bootstrap_stream);
        inner.ensure_connections().await;
        Ok(Self { inner })
    }

    pub async fn get(&self, key: impl StoreKey) -> Result<Option<Vec<u8>>, Error> {
        let key = key.to_store_key();
        let (primary, fallback) = self.inner.read_targets(key).await?;
        if let Some(record) = self
            .inner
            .request_with_retry(&primary, request(RequestType::Get, key, None, None, None))
            .await?
        {
            return Ok(Some(record.value));
        }
        if fallback.is_empty() {
            return Ok(None);
        }

        let responses = join_all(fallback.into_iter().map(|node_id| {
            let inner = self.inner.clone();
            async move {
                inner
                    .request_with_retry(&node_id, request(RequestType::Get, key, None, None, None))
                    .await
            }
        }))
        .await;
        let mut newest: Option<Record> = None;
        let mut successful = false;
        let mut last_error = None;
        for response in responses {
            match response {
                Ok(record) => {
                    successful = true;
                    if let Some(record) = record
                        && newest
                            .as_ref()
                            .is_none_or(|current| record.creation_time > current.creation_time)
                    {
                        newest = Some(record);
                    }
                }
                Err(error) => last_error = Some(error),
            }
        }
        if successful {
            Ok(newest.map(|record| record.value))
        } else {
            Err(last_error.unwrap_or(Error::Timeout))
        }
    }

    pub async fn put(
        &self,
        key: impl StoreKey,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), Error> {
        let key = key.to_store_key();
        let creation_time = now_millis()?;
        let expiration_time = ttl
            .map(|ttl| {
                u64::try_from(ttl.as_millis())
                    .ok()
                    .and_then(|ttl| creation_time.checked_add(ttl))
                    .ok_or(Error::TtlOverflow)
            })
            .transpose()?;
        self.inner
            .write_all(
                key,
                request(
                    RequestType::Put,
                    key,
                    Some(value),
                    Some(creation_time),
                    expiration_time,
                ),
            )
            .await
    }

    pub async fn delete(&self, key: impl StoreKey) -> Result<(), Error> {
        let key = key.to_store_key();
        self.inner
            .write_all(key, request(RequestType::Delete, key, None, None, None))
            .await
    }
}

impl Inner {
    async fn apply_state(self: &Arc<Self>, state: ClusterState) {
        let mut topology = self.state.write().await;
        if state.epoch < topology.epoch
            || (state.epoch == topology.epoch
                && !topology.leader_id.is_empty()
                && state.leader_id != topology.leader_id)
        {
            return;
        }
        topology.epoch = state.epoch;
        topology.leader_id = state.leader_id;
        if !state.nodes.is_empty() {
            topology.nodes = state
                .nodes
                .into_iter()
                .filter_map(|node| {
                    let address = node.addr?;
                    Some((
                        node.id,
                        Node {
                            endpoint: endpoint(&address.host, address.port),
                            node_type: node.node_type,
                        },
                    ))
                })
                .collect();
        }
        if let Some(partitions) = state.partitions {
            topology.mapping = partitions.mapping;
            topology.old_replicas = partitions
                .old_replicas
                .into_iter()
                .map(|(id, replicas)| (id, replicas.replicas))
                .collect();
            topology.new_replicas = partitions
                .new_replicas
                .into_iter()
                .map(|(id, replicas)| (id, replicas.replicas))
                .collect();
        }
    }

    async fn ensure_connections(self: &Arc<Self>) {
        let nodes = self.state.read().await.nodes.clone();
        for (id, node) in nodes {
            if node.node_type == NodeType::Worker as i32 {
                if !self.workers.read().await.contains_key(&id)
                    && let Ok(connection) = WorkerConnection::connect(&node.endpoint).await
                {
                    self.workers.write().await.insert(id, Arc::new(connection));
                }
            } else if node.node_type == NodeType::Manager as i32 {
                self.ensure_manager(node.endpoint).await;
            }
        }
    }

    async fn ensure_manager(self: &Arc<Self>, endpoint: String) {
        if !self.managers.lock().await.insert(endpoint.clone()) {
            return;
        }
        let inner = self.clone();
        tokio::spawn(async move {
            match subscribe(&endpoint, &inner.id, inner.config.manager_connect_timeout).await {
                Ok(stream) => spawn_manager_stream(inner.clone(), endpoint.clone(), stream),
                Err(_) => {
                    inner.managers.lock().await.remove(&endpoint);
                }
            }
        });
    }

    async fn read_targets(&self, key: u64) -> Result<(String, Vec<String>), Error> {
        let partition_id = partition(key);
        let topology = self.state.read().await;
        let current = topology
            .mapping
            .get(&partition_id)
            .ok_or(Error::PartitionUnavailable(partition_id))?;
        let mut fallback: HashSet<_> = current.replicas.iter().cloned().collect();
        fallback.extend(
            topology
                .old_replicas
                .get(&partition_id)
                .into_iter()
                .flatten()
                .cloned(),
        );
        fallback.extend(
            topology
                .new_replicas
                .get(&partition_id)
                .into_iter()
                .flatten()
                .cloned(),
        );
        fallback.remove(&current.master);
        Ok((current.master.clone(), fallback.into_iter().collect()))
    }

    async fn write_all(self: &Arc<Self>, key: u64, template: ClientRequest) -> Result<(), Error> {
        let partition_id = partition(key);
        let topology = self.state.read().await;
        let mapping = topology
            .mapping
            .get(&partition_id)
            .ok_or(Error::PartitionUnavailable(partition_id))?;
        let mut targets: HashSet<_> = mapping.replicas.iter().cloned().collect();
        targets.insert(mapping.master.clone());
        drop(topology);

        let results = join_all(targets.into_iter().map(|node_id| {
            let inner = self.clone();
            let request = template.clone();
            async move { inner.request_with_retry(&node_id, request).await }
        }))
        .await;
        for result in results {
            result?;
        }
        Ok(())
    }

    async fn request_with_retry(
        self: &Arc<Self>,
        node_id: &str,
        mut request: ClientRequest,
    ) -> Result<Option<Record>, Error> {
        let attempts = self.config.retry_attempts.max(1);
        let mut delay = self.config.initial_retry_delay;
        let mut last_reason = String::new();
        for attempt in 0..attempts {
            request.id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
            match self.worker(node_id).await {
                Ok(worker) => {
                    match timeout(self.config.request_timeout, worker.request(request.clone()))
                        .await
                    {
                        Ok(Ok(response)) => return Ok(response),
                        Ok(Err(reason)) => last_reason = reason,
                        Err(_) => last_reason = "request timeout".into(),
                    }
                }
                Err(error) => last_reason = error.to_string(),
            }
            self.workers.write().await.remove(node_id);
            if attempt + 1 < attempts {
                sleep(delay).await;
                delay = delay.saturating_mul(2);
            }
        }
        Err(Error::WorkerRequest {
            node_id: node_id.to_owned(),
            reason: last_reason,
        })
    }

    async fn worker(&self, node_id: &str) -> Result<Arc<WorkerConnection>, Error> {
        if let Some(worker) = self.workers.read().await.get(node_id).cloned() {
            return Ok(worker);
        }
        let endpoint = self
            .state
            .read()
            .await
            .nodes
            .get(node_id)
            .map(|node| node.endpoint.clone())
            .ok_or_else(|| Error::UnknownWorker(node_id.to_owned()))?;
        let worker = Arc::new(
            WorkerConnection::connect(&endpoint)
                .await
                .map_err(|reason| Error::WorkerRequest {
                    node_id: node_id.to_owned(),
                    reason,
                })?,
        );
        self.workers
            .write()
            .await
            .insert(node_id.to_owned(), worker.clone());
        Ok(worker)
    }
}

fn spawn_manager_stream(
    inner: Arc<Inner>,
    endpoint: String,
    stream: tonic::Streaming<proto::manager_api::v1::ClientEvent>,
) {
    tokio::spawn(async move {
        let mut current = Some(stream);
        loop {
            let mut stream = match current.take() {
                Some(stream) => stream,
                None => match subscribe(&endpoint, &inner.id, inner.config.manager_connect_timeout)
                    .await
                {
                    Ok(stream) => stream,
                    Err(_) => {
                        sleep(inner.config.initial_retry_delay).await;
                        continue;
                    }
                },
            };
            while let Ok(Some(event)) = stream.message().await {
                if let Some(state) = event.cluster_state {
                    inner.apply_state(state).await;
                    inner.ensure_connections().await;
                }
            }
            sleep(inner.config.initial_retry_delay).await;
        }
    });
}

async fn subscribe(
    endpoint: &str,
    id: &str,
    connect_timeout: Duration,
) -> Result<tonic::Streaming<proto::manager_api::v1::ClientEvent>, String> {
    let channel = timeout(
        connect_timeout,
        Channel::from_shared(endpoint.to_owned())
            .map_err(|e| e.to_string())?
            .connect(),
    )
    .await
    .map_err(|_| "connection timeout".to_string())?
    .map_err(|error| error.to_string())?;
    ManagerApiClient::new(channel)
        .open_client_connection(ClientConnect { id: id.to_owned() })
        .await
        .map(|response| response.into_inner())
        .map_err(|error| error.to_string())
}

fn request(
    request_type: RequestType,
    key: u64,
    value: Option<Vec<u8>>,
    creation_time: Option<u64>,
    ttl: Option<u64>,
) -> ClientRequest {
    ClientRequest {
        id: 0,
        request_type: request_type as i32,
        key,
        creation_time,
        value,
        ttl,
    }
}

fn partition(key: u64) -> u32 {
    (key % PARTITIONS_AMOUNT) as u32
}

fn hash_key(key: &[u8]) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(KEY_HASH_DOMAIN);
    hasher.update(key);
    let hash = hasher.finalize();
    u64::from_le_bytes(hash.as_bytes()[..8].try_into().expect("eight-byte prefix"))
}

fn endpoint(host: &str, port: u32) -> String {
    normalize_endpoint(&format!("{host}:{port}"))
}

fn normalize_endpoint(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_owned()
    } else {
        format!("http://{endpoint}")
    }
}

fn now_millis() -> Result<u64, Error> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::InvalidSystemTime)?
        .as_millis();
    u64::try_from(millis).map_err(|_| Error::InvalidSystemTime)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::common::v1::{Addr, Partitions, Replicas};

    fn inner() -> Arc<Inner> {
        Arc::new(Inner {
            id: uuid::Uuid::now_v7().to_string(),
            config: ClientConfig::default(),
            state: RwLock::new(Topology::default()),
            workers: RwLock::new(HashMap::new()),
            managers: Mutex::new(HashSet::new()),
            next_request_id: AtomicI32::new(1),
        })
    }

    #[test]
    fn partition_matches_worker_algorithm() {
        assert_eq!(partition(0), 0);
        assert_eq!(partition(4095), 4095);
        assert_eq!(partition(4096), 0);
        assert_eq!(partition(8193), 1);
    }

    #[test]
    fn store_keys_are_stable_and_preserve_u64_values() {
        assert_eq!(42_u64.to_store_key(), 42);
        assert_eq!(
            "customer:42".to_store_key(),
            "customer:42".to_string().to_store_key()
        );
        assert_eq!("customer:42".to_store_key(), b"customer:42".to_store_key());
        assert_ne!("customer:42".to_store_key(), "customer:43".to_store_key());
    }

    #[tokio::test]
    async fn read_fallback_includes_current_old_and_new_replicas() {
        let inner = inner();
        inner
            .apply_state(ClusterState {
                epoch: 1,
                leader_id: "leader".into(),
                nodes: vec![proto::common::v1::Node {
                    id: "master".into(),
                    addr: Some(Addr {
                        host: "localhost".into(),
                        port: 1,
                    }),
                    last_heartbeat: 0,
                    node_type: NodeType::Worker as i32,
                }],
                partitions: Some(Partitions {
                    mapping: HashMap::from([(
                        7,
                        Partition {
                            master: "master".into(),
                            replicas: vec!["current".into()],
                        },
                    )]),
                    old_replicas: HashMap::from([(
                        7,
                        Replicas {
                            replicas: vec!["old".into()],
                        },
                    )]),
                    new_replicas: HashMap::from([(
                        7,
                        Replicas {
                            replicas: vec!["new".into()],
                        },
                    )]),
                }),
            })
            .await;

        let (master, fallback) = inner.read_targets(7).await.unwrap();
        assert_eq!(master, "master");
        assert_eq!(
            fallback.into_iter().collect::<HashSet<_>>(),
            HashSet::from(["current".into(), "old".into(), "new".into()])
        );
    }

    #[tokio::test]
    async fn stale_cluster_state_is_ignored() {
        let inner = inner();
        inner
            .apply_state(ClusterState {
                epoch: 2,
                leader_id: "new".into(),
                nodes: vec![],
                partitions: None,
            })
            .await;
        inner
            .apply_state(ClusterState {
                epoch: 1,
                leader_id: "old".into(),
                nodes: vec![],
                partitions: None,
            })
            .await;
        let state = inner.state.read().await;
        assert_eq!(state.epoch, 2);
        assert_eq!(state.leader_id, "new");
    }
}
