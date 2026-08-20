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
    /// Compare two config files as configurations, not as documents. Each side
    /// may be `.toml` or `.brenn`. Exits 0 when they are the same config, 1 with
    /// a unified diff when they are not.
    ConfigDiff { a: PathBuf, b: PathBuf },
    /// Validate a config file the way the server loads it: a `.brenn` document
    /// is parsed, resolved, derived and lowered, a `.toml` file is parsed. Exits
    /// 0 when the file would load, 1 with the diagnostics when it would not.
    /// Environment facts are not checked — container home directories, the
    /// integration registry and the runtime dir are the boot's business — so
    /// `ok` means the file is a config, not that it will boot on every host.
    ConfigCheck { file: PathBuf },
}
