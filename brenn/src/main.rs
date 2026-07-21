mod build_info;

use brenn_server::{bootstrap, cli};

#[tokio::main]
async fn main() {
    use clap::Parser as _;
    let cli = cli::Cli::parse();
    let config = brenn_lib::config::load_config(cli.config.as_deref());

    match cli.command.unwrap_or(cli::Commands::Serve) {
        cli::Commands::Invite => bootstrap::run_invite(&config).await,
        cli::Commands::Serve => {
            bootstrap::run_server(config, cli.config, build_info::BUILD_ID).await
        }
    }
}
