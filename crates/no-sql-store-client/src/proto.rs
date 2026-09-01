pub(crate) mod common {
    pub(crate) mod v1 {
        tonic::include_proto!("common.v1");
    }
}

pub(crate) mod manager_api {
    pub(crate) mod v1 {
        tonic::include_proto!("manager_api.v1");
    }
}

pub(crate) mod worker_api {
    pub(crate) mod v1 {
        tonic::include_proto!("worker_api.v1");
    }
}
