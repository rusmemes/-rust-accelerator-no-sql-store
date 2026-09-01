# no-sql-store end-to-end tests

The tests start real manager and worker processes and exercise the public Rust
client over gRPC.

Run from the workspace root:

```shell
cargo build -p no-sql-store
cargo test -p no-sql-store-e2e-tests -- --nocapture
```

Set `NO_SQL_STORE_BIN` to test a specific server binary.
