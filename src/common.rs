use crate::cli::{Cli, Command};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

pub const PARTITIONS_AMOUNT: usize = 4096;

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Hash, Debug)]
pub struct NodeId(String);

impl NodeId {
    pub fn new() -> Self {
        Self(Uuid::now_v7().to_string())
    }

    pub fn from_string(id: &str) -> Self {
        Self(
            Uuid::parse_str(id)
                .expect(format!("Unexpected NodeId format: {}", id).as_str())
                .into(),
        )
    }

    pub fn to_string(&self) -> String {
        self.0.clone()
    }
}

impl From<String> for NodeId {
    fn from(id: String) -> Self {
        Self::from_string(&id)
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeType {
    Manager,
    Worker,
}

#[derive(Debug)]
pub struct Node {
    pub host: String,
    pub port: u32,
    pub last_heartbeat: u64,
    pub node_type: NodeType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PartitionId(pub u16);

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Partitions {
    pub mapping: HashMap<PartitionId, Partition>,
    pub old_replicas: HashMap<PartitionId, HashSet<NodeId>>,
    pub new_replicas: HashMap<PartitionId, HashSet<NodeId>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Partition {
    pub master: NodeId,
    pub replicas: HashSet<NodeId>,
}

impl Node {
    pub fn is_manager(&self) -> bool {
        matches!(self.node_type, NodeType::Manager)
    }

    pub fn is_worker(&self) -> bool {
        matches!(self.node_type, NodeType::Worker)
    }
}

#[derive(Debug)]
pub struct Heartbeat {
    pub id: NodeId,
    pub ts: u64,
}

#[derive(Debug, Clone)]
pub struct ClusterNode {
    pub id: NodeId,
    pub host: String,
    pub port: u32,
    pub last_heartbeat: u64,
    pub node_type: NodeType,
}

#[derive(Debug, Clone)]
pub struct ClusterState {
    pub epoch: u64,
    pub leader_id: NodeId,
    pub nodes: Vec<ClusterNode>,
    pub partitions: Partitions,
}

#[derive(Debug, Clone)]
pub struct Me {
    pub id: NodeId,
    pub host: String,
    pub port: u32,
}

impl Me {
    pub fn new(host: String, port: u32) -> Self {
        Self {
            id: NodeId::new(),
            host,
            port,
        }
    }
}

#[derive(Debug, Clone)]
pub enum Config
{
    Manager {
        grpc_port: u16,
        self_host_port: (String, u16),
        manager_host_port: Option<(String, u16)>,
        replication_factor: usize,
    },
    Worker {
        grpc_port: u16,
        self_host_port: (String, u16),
        manager_host_port: (String, u16),
    },
}

impl Config {

    pub fn grpc_port(&self) -> u16 {
        match self {
            Config::Manager { grpc_port, .. } => *grpc_port,
            Config::Worker { grpc_port, .. } => *grpc_port,
        }
    }

    pub fn manager_host_port(&self) -> Option<&(String, u16)> {
        match self {
            Config::Manager { manager_host_port, .. } => manager_host_port.as_ref(),
            Config::Worker { manager_host_port, .. } => Some(manager_host_port),
        }
    }

    pub fn self_host_port(&self) -> &(String, u16) {
        match self {
            Config::Manager { self_host_port, .. } => self_host_port,
            Config::Worker { self_host_port, .. } => self_host_port,
        }
    }

    pub fn replication_factor_mut(&mut self) -> &mut usize {
        match self {
            Config::Manager {
                replication_factor, ..
            } => replication_factor,
            Config::Worker { .. } => {
                unreachable!("Partitions and replication factor should be defined")
            }
        }
    }

    pub fn replication_factor(&self) -> usize {
        match self {
            Config::Manager {
                replication_factor, ..
            } => *replication_factor,
            Config::Worker { .. } => {
                unreachable!("Partitions and replication factor should be defined")
            }
        }
    }
}

impl From<Cli> for Config {
    fn from(value: Cli) -> Self {
        match value.command {
            Command::Manager {
                common,
                manager_host,
                manager_port,
                replication_factor,
            } => Config::Manager {
                grpc_port: common.grpc_port,
                self_host_port: (common.self_host.clone(), common.self_port()),
                manager_host_port: manager_host.zip(manager_port),
                replication_factor,
            },
            Command::Worker {
                common,
                manager_host,
                manager_port,
            } => Config::Worker {
                grpc_port: common.grpc_port,
                self_host_port: (common.self_host.clone(), common.self_port()),
                manager_host_port: (manager_host, manager_port),
            },
        }
    }
}

use std::time::{SystemTime, UNIX_EPOCH};

#[inline]
pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64
}

/**
EitherStream is a stream that can send messages to either a Sender<Result<Message, Status>> or a Sender<Message>.
It is used to send messages to either the gRPC output stream or the gRPC input stream.
*/
#[derive(Clone)]
pub(super) enum CommunicationStreamEither<A: Clone, B: Clone> {
    Input(A),
    Output(B),
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn node_id_roundtrips_uuid_string() {
        let id = NodeId::new();
        let s = id.to_string();
        let parsed = NodeId::from_string(&s);
        assert_eq!(parsed.to_string(), s);
        assert_eq!(parsed.to_string(), format!("{}", parsed));
    }

    #[test]
    fn node_id_from_string_from_string_impl() {
        let id = NodeId::new().to_string();
        let parsed: NodeId = id.clone().into();
        assert_eq!(parsed.to_string(), id);
    }

    #[test]
    fn config_from_cli_manager_sets_replication_factor_and_optional_manager_addr() {
        let cli = Cli::try_parse_from([
            "bin",
            "manager",
            "--grpc-port",
            "5000",
            "--self-host",
            "127.0.0.1",
            "--replication-factor",
            "5",
        ])
        .unwrap();

        let cfg: Config = cli.into();
        match cfg {
            Config::Manager {
                grpc_port,
                self_host_port,
                manager_host_port,
                replication_factor,
            } => {
                assert_eq!(grpc_port, 5000);
                assert_eq!(self_host_port, ("127.0.0.1".to_string(), 5000));
                assert_eq!(manager_host_port, None);
                assert_eq!(replication_factor, 5);
            }
            other => panic!("unexpected config: {:?}", other),
        }
    }

    #[test]
    fn config_from_cli_worker_sets_manager_addr_and_has_no_replication_factor() {
        let cli = Cli::try_parse_from([
            "bin",
            "worker",
            "--grpc-port",
            "5001",
            "--self-host",
            "127.0.0.1",
            "--self-port",
            "7777",
            "--manager-host",
            "10.0.0.1",
            "--manager-port",
            "6000",
        ])
        .unwrap();

        let cfg: Config = cli.into();
        match cfg {
            Config::Worker {
                grpc_port,
                self_host_port,
                manager_host_port,
            } => {
                assert_eq!(grpc_port, 5001);
                assert_eq!(self_host_port, ("127.0.0.1".to_string(), 7777));
                assert_eq!(manager_host_port, ("10.0.0.1".to_string(), 6000));
            }
            other => panic!("unexpected config: {:?}", other),
        }
    }

    #[test]
    fn now_millis_is_non_decreasing_over_time() {
        let a = now_millis();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let b = now_millis();
        assert!(b >= a);
    }
}
