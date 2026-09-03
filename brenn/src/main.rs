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
            return verdict(bootstrap::run_config_diff(a, b, &cli.modules));
        }
        Some(cli::Commands::ConfigCheck { file }) => {
            return verdict(bootstrap::run_config_check(file, &cli.modules));
        }
        _ => {}
    }

    let config = brenn_lib::config::load_config(cli.config.as_deref(), &cli.modules);

    match cli.command.unwrap_or(cli::Commands::Serve {
        components: Vec::new(),
        surface: Vec::new(),
    }) {
        cli::Commands::Invite => bootstrap::run_invite(&config).await,
        cli::Commands::Serve {
            components,
            surface,
        } => {
            let install_roots = cli::InstallRoots {
                components,
                surface,
            };
            bootstrap::run_server(config, cli.config, install_roots, build_info::BUILD_ID).await;
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
