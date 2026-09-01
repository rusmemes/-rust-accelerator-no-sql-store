use super::{ManagerService, Node};
use crate::common::{Config, Me, NodeId, NodeType};
use std::sync::Arc;
use tokio::sync::RwLock;

pub(super) fn node_id(id: &str) -> NodeId {
    NodeId::from_string(id)
}

pub(super) fn me(id: &str) -> Me {
    Me {
        id: node_id(id),
        host: "127.0.0.1".to_string(),
        port: 7000,
    }
}

pub(super) fn shared_config(manager_host_port: Option<(String, u16)>) -> Arc<RwLock<Config>> {
    Arc::new(RwLock::new(Config::Manager {
        grpc_port: 8080,
        self_host_port: ("127.0.0.1".to_string(), 7000),
        manager_host_port,
        replication_factor: 3,
    }))
}

pub(super) fn service(me: Me) -> (ManagerService, Arc<RwLock<Config>>) {
    let config = shared_config(Some(("manager.local".to_string(), 9000)));
    (ManagerService::new(me, config.clone()), config)
}

pub(super) fn fresh_node(me: &Me, last_heartbeat: u64) -> Node {
    Node {
        host: me.host.clone(),
        port: me.port,
        last_heartbeat,
        node_type: NodeType::Manager,
    }
}

pub(super) fn node(host: &str, port: u32, last_heartbeat: u64) -> Node {
    Node {
        host: host.to_string(),
        port,
        last_heartbeat,
        node_type: NodeType::Manager,
    }
}

pub(super) fn worker_node(host: &str, port: u32, last_heartbeat: u64) -> Node {
    Node {
        host: host.to_string(),
        port,
        last_heartbeat,
        node_type: NodeType::Worker,
    }
}
