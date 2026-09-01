//! Shared protobuf messages and gRPC services for `no-sql-store`.

pub mod common {
    pub mod v1 {
        tonic::include_proto!("common.v1");
    }
}

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
