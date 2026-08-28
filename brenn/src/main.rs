mod build_info;

use std::process::ExitCode;

use brenn_bootstrap::{self as bootstrap, cli};

#[tokio::main]
async fn main() -> ExitCode {
    use clap::Parser as _;
    let cli = cli::Cli::parse();

    // The two config tools name the files they read, so neither reads
    // `--config` and neither loads a third config to throw away.
    match &cli.command {
        Some(cli::Commands::ConfigDiff { a, b }) => {
            return verdict(bootstrap::run_config_diff(a, b, cli.modules.as_deref()));
        }
        Some(cli::Commands::ConfigCheck { file }) => {
            return verdict(bootstrap::run_config_check(file, cli.modules.as_deref()));
        }
        _ => {}
    }

    let config = brenn_lib::config::load_config(cli.config.as_deref(), cli.modules.as_deref());

    match cli.command.unwrap_or(cli::Commands::Serve) {
        cli::Commands::Invite => bootstrap::run_invite(&config).await,
        cli::Commands::Serve => {
            bootstrap::run_server(config, cli.config, build_info::BUILD_ID).await;
        }
        cli::Commands::ConfigDiff { .. } | cli::Commands::ConfigCheck { .. } => {
            unreachable!("handled above, before the config loads")
        }
    }
    ExitCode::SUCCESS
}

/// A tool's boolean answer as the process's.
fn verdict(ok: bool) -> ExitCode {
    if ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
