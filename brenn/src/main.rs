mod build_info;

use std::process::ExitCode;

use brenn_bootstrap::{self as bootstrap, cli};

#[tokio::main]
async fn main() -> ExitCode {
    use clap::Parser as _;
    let cli = cli::Cli::parse();
    if let Err(error) = cli.validate() {
        error.exit();
    }

    // The four tools that read no config run before anything loads one: the two
    // config tools name the files they read, so neither reads `--config`,
    // `mounts` reads only the mounts document, and `config-status` reads only
    // the store.
    match &cli.command {
        Some(cli::Commands::Mounts) => {
            let path = cli
                .mounts
                .as_deref()
                .expect("`mounts` without --mounts is refused by Cli::validate");
            return verdict(bootstrap::run_mounts(path));
        }
        Some(cli::Commands::ConfigStatus { db }) => {
            return ExitCode::from(bootstrap::run_config_status(db));
        }
        Some(cli::Commands::ConfigDiff { a, b }) => {
            let roots = check_roots(&cli);
            return verdict(bootstrap::run_config_diff(a, b, &roots));
        }
        Some(cli::Commands::ConfigCheck { file }) => {
            let roots = check_roots(&cli);
            return verdict(bootstrap::run_config_check(file, &roots));
        }
        _ => {}
    }

    // A server's roots are the mounts', so the mounts document is read first:
    // the deployment document's packaged imports resolve against roots it
    // derives, and a mount that is declared but not installed is a boot panic
    // rather than a document that compiles against half a host.
    let mounts = brenn_lib::config::load_mounts(cli.mounts.as_deref());
    let document = brenn_lib::config::load_config(cli.config.as_deref(), &mounts.roots);

    match cli.command.unwrap_or(cli::Commands::Serve) {
        cli::Commands::Invite => bootstrap::run_invite(&document.config).await,
        cli::Commands::Serve => {
            bootstrap::run_server(document, cli.config, mounts, build_info::BUILD_ID).await;
        }
        cli::Commands::Mounts
        | cli::Commands::ConfigStatus { .. }
        | cli::Commands::ConfigDiff { .. }
        | cli::Commands::ConfigCheck { .. } => {
            unreachable!("handled above, before the config loads")
        }
    }
    ExitCode::SUCCESS
}

/// The roots a config tool checks against, as [`bootstrap::tool_roots`]
/// derives them, with a fault printed and the process ended.
///
/// Exits rather than returning a verdict — the tool has not read its document
/// yet, so there is no diff and no `ok` line to withhold.
fn check_roots(cli: &cli::Cli) -> brenn_lib::config::Roots {
    match bootstrap::tool_roots(cli.mounts.as_deref(), &cli.modules, &cli.mounted) {
        Ok(roots) => roots,
        Err(report) => {
            eprintln!("{report}");
            std::process::exit(1);
        }
    }
}

/// A tool's boolean answer as the process's.
fn verdict(ok: bool) -> ExitCode {
    if ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
