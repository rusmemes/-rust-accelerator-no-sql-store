# no-sql-store

`no-sql-store` is an experimental distributed, in-memory key-value store written in Rust. It consists of manager nodes that maintain cluster topology and partition ownership, worker nodes that store records, and a topology-aware Rust client that routes operations directly to the appropriate workers.

The project uses asynchronous I/O with Tokio and bidirectional gRPC streams with Tonic. Keys are distributed across 4,096 partitions, partitions are assigned to a master and zero or more replicas, and cluster membership changes trigger partition synchronization between workers.

> This project is under active development. It currently provides in-memory storage only and should not be treated as a production-ready database.

## Features

- Manager and worker nodes in a single server binary.
- Leader election and topology distribution between managers.
- Fixed 4,096-partition key space.
- Configurable replication factor.
- Direct client connections to every known manager and worker.
- Automatic client topology updates through a dedicated manager stream.
- Concurrent writes to a partition master and all current replicas.
- Configurable retries with exponential backoff.
- Master-first reads with replica fallback during partition transitions.
- Last-write-wins record application based on `creation_time`.
- Optional record expiration.
- Numeric, string, byte-array, and application-defined keys.
- End-to-end tests that run a real multi-process cluster.

## Workspace layout

```text
.
|-- Cargo.toml
|-- README.md
`-- crates
    |-- no-sql-store
    |   |-- proto
    |   `-- src
    |-- no-sql-store-client
    |   |-- proto
    |   `-- src
    `-- no-sql-store-e2e-tests
        |-- proto
        `-- tests
```

| Crate | Purpose | Published |
| --- | --- | --- |
| `no-sql-store` | Binary that runs either a manager or a worker node. | Not currently configured for publication. |
| `no-sql-store-client` | Public asynchronous Rust client library. | Intended for crates.io publication. |
| `no-sql-store-e2e-tests` | Multi-process end-to-end test harness. | No (`publish = false`). |

## Architecture

### Managers

Manager nodes maintain the authoritative view of:

- cluster membership;
- the current leader and epoch;
- worker addresses;
- partition masters and replicas;
- old and new replicas involved in partition migration.

Managers communicate over the `ManagerApi` gRPC service. The elected leader calculates partition assignments and distributes changes to managers, workers, and subscribed clients.

Clients use a dedicated manager subscription and are not added to cluster membership. A client needs at least one reachable manager address at startup. After receiving its first full snapshot, it discovers and connects to all managers and workers advertised by the cluster.

For a resilient deployment, run three or more manager nodes and point additional managers at an existing bootstrap manager.

### Workers

Worker nodes store records in memory. Each worker maintains partition-local ordered maps backed by `DashMap` and `crossbeam-skiplist`.

Workers expose two streaming APIs:

- a worker-to-worker stream for partition synchronization;
- a client-to-worker stream for `Get`, `Put`, and `Delete` operations.

Records contain:

- a `u64` storage key;
- arbitrary bytes;
- a creation timestamp;
- an optional absolute expiration timestamp.

When two versions of the same key are applied to a worker, the record with the greater `creation_time` wins. Equal timestamps allow the later applied value to replace the earlier value.

### Smart client

The Rust client owns cluster routing and replication behavior:

1. It connects to the first available configured manager.
2. It waits for a complete cluster snapshot.
3. It opens and maintains streams to every discovered manager and worker.
4. It applies newer topology updates pushed by managers.
5. It maps the key to a partition and selects the relevant workers.
6. It retries failed operations according to `ClientConfig`.

The client does not expose a network server. Applications import it as a normal Rust library.

## Partitioning and keys

Workers receive only `u64` keys over gRPC. The partition calculation is identical in the client and worker:

```text
partition = key % 4096
```

The client accepts any value implementing `StoreKey`:

- `u64` values are preserved;
- other built-in integer values are converted to `u64`;
- strings and byte sequences are hashed with domain-separated BLAKE3 and truncated to 64 bits;
- application types can implement `StoreKey` directly.

```rust
use no_sql_store_client::StoreKey;

struct UserId(u64);

impl StoreKey for UserId {
    fn to_store_key(&self) -> u64 {
        self.0
    }
}
```

Because the wire and storage representation is only 64 bits, distinct string or byte keys can theoretically collide. BLAKE3 makes accidental collisions extremely unlikely, but collision-free arbitrary keys would require changing the worker protocol and storage model.

## Requirements

- A Rust toolchain with Rust 2024 edition support.
- Cargo.
- A working Protocol Buffers compiler (`protoc`) if it is not otherwise provided by the build environment.
- A Unix-like environment for the current manager and worker signal handling.

## Building

Build the entire workspace:

```shell
cargo build --workspace
```

Build an optimized server binary:

```shell
cargo build --release -p no-sql-store
```

The binary is written to `target/release/no-sql-store`.

## Running a local cluster

The following example starts three managers and two workers on localhost. Run every command in a separate terminal.

Start the bootstrap manager:

```shell
cargo run -p no-sql-store -- manager \
  --grpc-port 7001 \
  --self-host 127.0.0.1 \
  --replication-factor 2
```

Start two additional managers:

```shell
cargo run -p no-sql-store -- manager \
  --grpc-port 7002 \
  --self-host 127.0.0.1 \
  --manager-host 127.0.0.1 \
  --manager-port 7001 \
  --replication-factor 2
```

```shell
cargo run -p no-sql-store -- manager \
  --grpc-port 7003 \
  --self-host 127.0.0.1 \
  --manager-host 127.0.0.1 \
  --manager-port 7001 \
  --replication-factor 2
```

Start two workers:

```shell
cargo run -p no-sql-store -- worker \
  --grpc-port 7101 \
  --self-host 127.0.0.1 \
  --manager-host 127.0.0.1 \
  --manager-port 7001
```

```shell
cargo run -p no-sql-store -- worker \
  --grpc-port 7102 \
  --self-host 127.0.0.1 \
  --manager-host 127.0.0.1 \
  --manager-port 7001
```

Use `--self-port` when the address advertised to other nodes differs from the local gRPC listening port.

The worker option `--expired-cleanup-interval-secs` controls the background expiration scan interval and defaults to 300 seconds. Expired values are also removed lazily when read.

Show all CLI options with:

```shell
cargo run -p no-sql-store -- --help
cargo run -p no-sql-store -- manager --help
cargo run -p no-sql-store -- worker --help
```

## Using the Rust client

Until the client is published, add it as a path or Git dependency:

```toml
[dependencies]
no-sql-store-client = { path = "../no-sql-store/crates/no-sql-store-client" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

### Basic operations

```rust
use no_sql_store_client::Client;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::connect([
        "http://127.0.0.1:7001",
        "http://127.0.0.1:7002",
        "http://127.0.0.1:7003",
    ])
    .await?;

    client.put(42_u64, b"numeric value".to_vec(), None).await?;
    assert_eq!(client.get(42_u64).await?, Some(b"numeric value".to_vec()));

    client
        .put(
            "users:alice",
            b"string-key value".to_vec(),
            Some(Duration::from_secs(60)),
        )
        .await?;

    let value = client.get("users:alice").await?;
    println!("{value:?}");

    client.delete("users:alice").await?;
    assert_eq!(client.get("users:alice").await?, None);

    Ok(())
}
```

Plaintext manager addresses may be passed with or without an `http://` prefix. Addresses without a scheme default to `http://`. TLS is not currently configured by the client.

### Client configuration

```rust
use no_sql_store_client::{Client, ClientConfig};
use std::time::Duration;

# async fn example() -> Result<(), no_sql_store_client::Error> {
let config = ClientConfig {
    retry_attempts: 5,
    initial_retry_delay: Duration::from_millis(25),
    request_timeout: Duration::from_secs(2),
    manager_connect_timeout: Duration::from_secs(3),
};

let client = Client::connect_with_config(["127.0.0.1:7001"], config).await?;
# Ok(())
# }
```

The default configuration performs three total attempts with delays starting at 50 milliseconds and doubling after every failed attempt. Worker requests and initial manager connections use five-second timeouts by default.

## Operation semantics

### Writes

`put` calculates one creation timestamp, then concurrently sends the same record to the current partition master and every current replica. The operation succeeds only when every target acknowledges it. Failed requests are retried with exponential backoff.

An error does not imply that no write occurred. Some workers may have accepted the value before another target failed, and the client does not perform rollback. Retrying the same application operation may generate a newer creation timestamp.

The public TTL value is a relative `Duration`. The client converts it to an absolute expiration timestamp before sending it to workers. Passing `None` stores a record without expiration.

### Reads

`get` first queries the master from the latest accepted topology. When the master returns a record, that value is returned immediately.

When the master reports that the key is absent, the client concurrently queries every worker that may still contain the record:

- current replicas;
- old replicas from an active partition transition;
- new replicas from an active partition transition.

If several fallback workers return a value, the client selects the record with the greatest `creation_time`.

This is a master-first policy, not a quorum read. Replicas are not consulted when the master already returned a value, even if a replica could theoretically hold a newer version.

### Deletes

`delete` is concurrently sent to the current master and all current replicas, and all targets must acknowledge it. Deletes currently remove records directly; the store does not retain tombstones.

## Cluster changes

When worker membership changes, the manager leader recalculates partition ownership and records the previous owners as old replicas. Workers synchronize partition batches to the new owners. During this interval, the client retains both old and new replica sets from topology updates and uses them for fallback reads.

Manager streams are maintained in the background. A disconnected manager is retried, and topology snapshots from stale epochs or conflicting leaders are ignored. Newly discovered workers are connected lazily and during topology reconciliation.

## Testing

Run unit tests for every workspace crate:

```shell
cargo test --workspace
```

The E2E crate starts real manager and worker processes over loopback TCP. Build the server first, then run the test:

```shell
cargo build -p no-sql-store
cargo test -p no-sql-store-e2e-tests -- --nocapture
```

The E2E suite covers:

- numeric and string keys;
- `put`, `get`, and `delete`;
- TTL expiration;
- physical replication to the master and replicas;
- fallback reads after a record is removed from the master;
- startup through a non-bootstrap manager;
- topology updates after adding a worker;
- write failure when a required master or replica is unavailable;
- retry behavior.

Set `NO_SQL_STORE_BIN` to run the tests against a specific server executable:

```shell
NO_SQL_STORE_BIN=/path/to/no-sql-store \
  cargo test -p no-sql-store-e2e-tests -- --nocapture
```

The E2E tests require permission to spawn local processes and bind loopback ports.

## Protobuf contracts

The gRPC contracts are stored under each crate's `proto` directory because the client must package its protocol sources independently for crates.io builds. When changing a contract, keep the server, client, and E2E copies synchronized.

Current services:

- `manager_api.v1.ManagerApi`
  - manager-to-manager connection;
  - worker-to-manager connection;
  - client cluster-state subscription.
- `worker_api.v1.WorkerApi`
  - client request stream;
  - worker partition-synchronization stream.

The generated gRPC types are internal implementation details of the smart client and are not part of its public application API.

## Current limitations

- All data is stored in memory and is lost when a worker exits.
- There is no write-ahead log, snapshot, or disk persistence.
- There is no authentication, authorization, or transport security configuration.
- The client implements all-target replication rather than configurable quorum reads and writes.
- Failed writes can be partially applied and are not rolled back.
- Deletes do not create tombstones, which limits delete conflict handling during concurrent migration.
- Arbitrary keys are reduced to 64 bits and can theoretically collide.
- Client timestamps assume reasonably synchronized system clocks.
- The partition count is currently fixed at 4,096.
- Manager and worker shutdown signal handling is currently Unix-specific.
- Protocol definitions are duplicated between crates and must be updated together.

## Development checks

Useful checks before submitting a change:

```shell
cargo fmt --all -- --check
cargo test --workspace
cargo clippy -p no-sql-store-client --all-targets -- -D warnings
cargo clippy -p no-sql-store-e2e-tests --all-targets -- -D warnings
```

The server crate contains pre-existing formatting and Clippy findings that may need to be addressed separately before enforcing workspace-wide strict linting.
