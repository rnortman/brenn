use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "brenn", about = "Brenn application server")]
pub struct Cli {
    /// Path to the configuration file.
    #[arg(long)]
    pub config: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Start the web server (default if no subcommand given).
    Serve,
    /// Generate an invite code and print it to stdout.
    Invite,
}
