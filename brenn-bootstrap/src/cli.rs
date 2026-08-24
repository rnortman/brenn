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
    /// Compare two `.brenn` config documents as configurations, not as
    /// documents. Exits 0 when they are the same config, 1 with a unified diff
    /// when they are not.
    ConfigDiff { a: PathBuf, b: PathBuf },
    /// Validate a `.brenn` config document the way the server loads it: parsed,
    /// resolved, derived and lowered. Exits 0 when the file would load, 1 with
    /// the diagnostics when it would not.
    /// Environment facts are not checked — container home directories, the
    /// integration registry and the runtime dir are the boot's business — so
    /// `ok` means the file is a config, not that it will boot on every host.
    ConfigCheck { file: PathBuf },
}
