use crate::common::{NodeType, Partition, PartitionId, Partitions};
use crate::conversions::common::v1;
use std::collections::HashMap;

pub mod manager_api {
    pub mod v1 {
        tonic::include_proto!("manager_api.v1");
    }
}

pub mod worker_api {
    pub mod v1 {
        tonic::include_proto!("worker_api.v1");
    }
}

pub mod common {
    pub mod v1 {
        tonic::include_proto!("common.v1");
    }
}

pub(crate) fn grpc_node_type_to_domain(node_type: i32) -> NodeType {
    match v1::NodeType::try_from(node_type).ok() {
        Some(v1::NodeType::Worker) => NodeType::Worker,
        _ => NodeType::Manager,
    }
}

pub(crate) fn domain_node_type_to_grpc(node_type: NodeType) -> i32 {
    match node_type {
        NodeType::Manager => v1::NodeType::Manager as i32,
        NodeType::Worker => v1::NodeType::Worker as i32,
    }
}

pub(crate) fn grpc_partitions_to_domain(partitions: v1::Partitions) -> Partitions {
    Partitions {
        mapping: grpc_partition_mapping_to_domain(partitions.mapping),
        old_replicas: grpc_old_replicas_to_domain(partitions.old_replicas),
        new_replicas: grpc_old_replicas_to_domain(partitions.new_replicas),
    }
}

pub(crate) fn domain_partitions_to_grpc(partitions: Partitions) -> v1::Partitions {
    v1::Partitions {
        mapping: domain_partition_mapping_to_grpc(partitions.mapping),
        old_replicas: domain_old_replicas_to_grpc(partitions.old_replicas),
        new_replicas: domain_old_replicas_to_grpc(partitions.new_replicas),
    }
}

fn grpc_partition_mapping_to_domain(
    mapping: HashMap<u32, v1::Partition>,
) -> HashMap<PartitionId, Partition> {
    mapping
        .into_iter()
        .map(|(partition_id, partition)| {
            (
                PartitionId(partition_id as u16),
                Partition {
                    master: partition.master.into(),
                    replicas: partition
                        .replicas
                        .into_iter()
                        .map(|replica| replica.into())
                        .collect(),
                },
            )
        })
        .collect()
}

fn domain_partition_mapping_to_grpc(
    mapping: HashMap<PartitionId, Partition>,
) -> HashMap<u32, v1::Partition> {
    mapping
        .into_iter()
        .map(|(partition_id, partition)| {
            (
                partition_id.0 as u32,
                v1::Partition {
                    master: partition.master.to_string(),
                    replicas: partition
                        .replicas
                        .into_iter()
                        .map(|node| node.to_string())
                        .collect(),
                },
            )
        })
        .collect()
}

fn grpc_old_replicas_to_domain(
    mapping: HashMap<u32, v1::Replicas>,
) -> HashMap<PartitionId, std::collections::HashSet<crate::common::NodeId>> {
    mapping
        .into_iter()
        .map(|(partition_id, old_replicas)| {
            (
                PartitionId(partition_id as u16),
                old_replicas
                    .replicas
                    .into_iter()
                    .map(|replica| replica.into())
                    .collect(),
            )
        })
        .collect()
}

fn domain_old_replicas_to_grpc(
    mapping: HashMap<PartitionId, std::collections::HashSet<crate::common::NodeId>>,
) -> HashMap<u32, v1::Replicas> {
    mapping
        .into_iter()
        .map(|(partition_id, replicas)| {
            (
                partition_id.0 as u32,
                v1::Replicas {
                    replicas: replicas.into_iter().map(|node| node.to_string()).collect(),
                },
            )
        })
        .collect()
}
