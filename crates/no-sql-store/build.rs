use std::io::Result;

fn main() -> Result<()> {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &["proto/manager-api.proto", "proto/worker-api.proto"],
            &["proto"],
        )?;

    Ok(())
}
