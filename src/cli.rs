use clap_derive::{Args, Parser, Subcommand};

fn parse_replication_factor(value: &str) -> Result<usize, String> {
    let value = value
        .parse::<usize>()
        .map_err(|_| "replication_factor must be a positive integer".to_string())?;

    if value == 0 {
        Err("replication_factor must be at least 1".to_string())
    } else {
        Ok(value)
    }
}

fn parse_positive_u64(value: &str) -> Result<u64, String> {
    let value = value
        .parse::<u64>()
        .map_err(|_| "value must be a positive integer".to_string())?;

    if value == 0 {
        Err("value must be at least 1".to_string())
    } else {
        Ok(value)
    }
}

#[derive(Args)]
pub struct CommonArgs {
    #[arg(long)]
    pub grpc_port: u16,

    #[arg(long)]
    pub self_host: String,

    #[arg(long)]
    self_port: Option<u16>,
}

impl CommonArgs {
    pub fn self_port(&self) -> u16 {
        self.self_port.unwrap_or(self.grpc_port)
    }
}

#[derive(Subcommand)]
pub enum Command {
    Manager {
        #[command(flatten)]
        common: CommonArgs,

        #[arg(long, requires = "manager_port")]
        manager_host: Option<String>,

        #[arg(long, requires = "manager_host")]
        manager_port: Option<u16>,

        #[arg(
            long,
            default_value_t = 3,
            value_parser = parse_replication_factor
        )]
        replication_factor: usize,
    },

    Worker {
        #[command(flatten)]
        common: CommonArgs,

        #[arg(long)]
        manager_host: String,

        #[arg(long)]
        manager_port: u16,

        #[arg(
            long,
            default_value_t = 300,
            value_parser = parse_positive_u64
        )]
        expired_cleanup_interval_secs: u64,
    },
}

#[derive(Parser)]
#[command(version, about)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn parse_replication_factor_rejects_zero() {
        assert!(parse_replication_factor("0").is_err());
    }

    #[test]
    fn parse_replication_factor_rejects_non_positive_or_non_int() {
        assert!(parse_replication_factor("-1").is_err());
        assert!(parse_replication_factor("abc").is_err());
    }

    #[test]
    fn parse_replication_factor_accepts_positive_int() {
        assert_eq!(parse_replication_factor("1").unwrap(), 1);
        assert_eq!(parse_replication_factor("3").unwrap(), 3);
    }

    #[test]
    fn common_args_self_port_defaults_to_grpc_port() {
        let args = CommonArgs {
            grpc_port: 1234,
            self_host: "127.0.0.1".to_string(),
            self_port: None,
        };
        assert_eq!(args.self_port(), 1234);
    }

    #[test]
    fn common_args_self_port_overrides_grpc_port() {
        let args = CommonArgs {
            grpc_port: 1234,
            self_host: "127.0.0.1".to_string(),
            self_port: Some(5678),
        };
        assert_eq!(args.self_port(), 5678);
    }

    #[test]
    fn manager_requires_host_and_port_together() {
        // manager_host without manager_port should fail due to `requires = "manager_port"`.
        let res = Cli::try_parse_from([
            "bin",
            "manager",
            "--grpc-port",
            "5000",
            "--self-host",
            "127.0.0.1",
            "--manager-host",
            "10.0.0.1",
        ]);
        assert!(res.is_err());

        // manager_port without manager_host should fail due to `requires = "manager_host"`.
        let res = Cli::try_parse_from([
            "bin",
            "manager",
            "--grpc-port",
            "5000",
            "--self-host",
            "127.0.0.1",
            "--manager-port",
            "6000",
        ]);
        assert!(res.is_err());
    }

    #[test]
    fn manager_replication_factor_defaults_to_3() {
        let cli = Cli::try_parse_from([
            "bin",
            "manager",
            "--grpc-port",
            "5000",
            "--self-host",
            "127.0.0.1",
        ])
        .unwrap();

        match cli.command {
            Command::Manager {
                replication_factor, ..
            } => assert_eq!(replication_factor, 3),
            _ => panic!("unexpected command"),
        }
    }

    #[test]
    fn manager_replication_factor_is_validated_by_clap_value_parser() {
        let res = Cli::try_parse_from([
            "bin",
            "manager",
            "--grpc-port",
            "5000",
            "--self-host",
            "127.0.0.1",
            "--replication-factor",
            "0",
        ]);
        assert!(res.is_err());
    }

    #[test]
    fn parsed_common_args_self_port_defaults_to_grpc_port() {
        let cli = Cli::try_parse_from([
            "bin",
            "worker",
            "--grpc-port",
            "5001",
            "--self-host",
            "127.0.0.1",
            "--manager-host",
            "10.0.0.1",
            "--manager-port",
            "6000",
        ])
        .unwrap();

        match cli.command {
            Command::Worker { common, .. } => assert_eq!(common.self_port(), 5001),
            _ => panic!("unexpected command"),
        }
    }

    #[test]
    fn worker_expired_cleanup_interval_defaults_to_five_minutes() {
        let cli = Cli::try_parse_from([
            "bin",
            "worker",
            "--grpc-port",
            "5001",
            "--self-host",
            "127.0.0.1",
            "--manager-host",
            "10.0.0.1",
            "--manager-port",
            "6000",
        ])
        .unwrap();

        match cli.command {
            Command::Worker {
                expired_cleanup_interval_secs,
                ..
            } => assert_eq!(expired_cleanup_interval_secs, 300),
            _ => panic!("unexpected command"),
        }
    }

    #[test]
    fn worker_expired_cleanup_interval_must_be_positive() {
        let result = Cli::try_parse_from([
            "bin",
            "worker",
            "--grpc-port",
            "5001",
            "--self-host",
            "127.0.0.1",
            "--manager-host",
            "10.0.0.1",
            "--manager-port",
            "6000",
            "--expired-cleanup-interval-secs",
            "0",
        ]);

        assert!(result.is_err());
    }
}
