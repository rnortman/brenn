//! `brenn-usage-obs` — CLI for exporting Brenn usage sessions and events.
//!
//! Opens the Brenn SQLite DB read-only and writes CSV or JSON to stdout (or
//! an `--out` file). Path is resolved from `--db` or the default
//! `BrennConfig` database path.
//!
//! ## Subcommands
//!
//! - `sessions --from <ts> --to <ts> [--user] [--device] [--app] [--format] [--out]`
//! - `events   --from <ts> --to <ts> [--user] [--device] [--app] [--type] [--format] [--out]`
//!
//! Timestamps are ISO-8601; bare `YYYY-MM-DD` is interpreted as UTC midnight.
//!
//! Exit 0 on success; non-zero on argument or I/O error. DB errors panic per
//! the project "BETTER DEAD THAN WRONG" policy.

use std::fs::OpenOptions;
use std::io::{self, BufWriter, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};
use rusqlite::{Connection, OpenFlags};

use brenn_lib::config;
use brenn_lib::usage::{EventsFilter, SessionsFilter, query_events, query_sessions};
use brenn_lib::usage_export::{
    write_events_csv, write_events_json, write_sessions_csv, write_sessions_json,
};
use brenn_usage_obs::parse_ts;

// ---------------------------------------------------------------------------
// CLI types
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(
    name = "brenn-usage-obs",
    about = "Export Brenn usage sessions and events (CSV/JSON)"
)]
struct Cli {
    /// Path to the Brenn SQLite database. Defaults to value from brenn.toml.
    #[arg(long)]
    db: Option<PathBuf>,

    /// Path to brenn.toml config file. Only used to resolve --db default.
    #[arg(long)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Export usage sessions.
    Sessions(SessionsArgs),
    /// Export usage events.
    Events(EventsArgs),
}

#[derive(clap::Args)]
struct SessionsArgs {
    /// Start of time window, inclusive. ISO-8601 or YYYY-MM-DD (UTC midnight).
    #[arg(long)]
    from: String,

    /// End of time window, exclusive. ISO-8601 or YYYY-MM-DD (UTC midnight).
    #[arg(long)]
    to: String,

    /// Filter by username (exact match).
    #[arg(long)]
    user: Option<String>,

    /// Filter by device slug.
    #[arg(long)]
    device: Option<String>,

    /// Filter by app slug.
    #[arg(long)]
    app: Option<String>,

    /// Output format.
    #[arg(long, default_value = "csv")]
    format: Format,

    /// Output file path. Defaults to stdout.
    #[arg(long)]
    out: Option<PathBuf>,
}

#[derive(clap::Args)]
struct EventsArgs {
    /// Start of time window, inclusive. ISO-8601 or YYYY-MM-DD (UTC midnight).
    #[arg(long)]
    from: String,

    /// End of time window, exclusive. ISO-8601 or YYYY-MM-DD (UTC midnight).
    #[arg(long)]
    to: String,

    /// Filter by username (exact match).
    #[arg(long)]
    user: Option<String>,

    /// Filter by device slug.
    #[arg(long)]
    device: Option<String>,

    /// Filter by app slug.
    #[arg(long)]
    app: Option<String>,

    /// Filter by event type (e.g. todo_done, llm_turn).
    #[arg(long = "type")]
    event_type: Option<String>,

    /// Output format.
    #[arg(long, default_value = "csv")]
    format: Format,

    /// Output file path. Defaults to stdout.
    #[arg(long)]
    out: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Format {
    Csv,
    Json,
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn main() {
    let cli = Cli::parse();
    if let Err(e) = run(cli) {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let db_path = resolve_db_path(cli.db, cli.config)?;
    let conn = Connection::open_with_flags(&db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .unwrap_or_else(|e| panic!("failed to open DB read-only at {}: {e}", db_path.display()));

    match cli.command {
        Command::Sessions(args) => run_sessions(&conn, args),
        Command::Events(args) => run_events(&conn, args),
    }
}

fn run_sessions(conn: &Connection, args: SessionsArgs) -> Result<(), Box<dyn std::error::Error>> {
    let filter = SessionsFilter {
        from: parse_ts(&args.from)?,
        to: parse_ts(&args.to)?,
        user: args.user,
        device: args.device,
        app: args.app,
    };
    let rows = query_sessions(conn, &filter);

    let writer = open_output(args.out.as_deref())?;
    match args.format {
        Format::Csv => {
            write_sessions_csv(writer, &rows)?;
        }
        Format::Json => {
            write_sessions_json(writer, &rows)?;
        }
    }
    Ok(())
}

fn run_events(conn: &Connection, args: EventsArgs) -> Result<(), Box<dyn std::error::Error>> {
    let event_type = match args.event_type {
        None => None,
        Some(ref s) => {
            let et = brenn_lib::usage::EventType::try_from_str(s).ok_or_else(|| {
                format!("unknown event type: {s}; valid values: ws_connect, ws_disconnect, llm_turn, send_message, stop_request, todo_refresh, todo_done, todo_schedule, todo_reorder, switch_conversation, new_conversation, request_compaction, run_target, set_conversation_privacy")
            })?;
            Some(et)
        }
    };

    let filter = EventsFilter {
        from: parse_ts(&args.from)?,
        to: parse_ts(&args.to)?,
        user: args.user,
        device: args.device,
        app: args.app,
        event_type,
    };
    let rows = query_events(conn, &filter);

    let writer = open_output(args.out.as_deref())?;
    match args.format {
        Format::Csv => {
            write_events_csv(writer, &rows)?;
        }
        Format::Json => {
            write_events_json(writer, &rows)?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Resolve the DB path: explicit `--db` wins, else load config and use
/// `config.database.path`.
fn resolve_db_path(
    explicit: Option<PathBuf>,
    config_path: Option<PathBuf>,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    if let Some(p) = explicit {
        return Ok(p);
    }
    let cfg = config::load_config(config_path.as_deref());
    Ok(cfg.database.path)
}

/// Open a `BufWriter` over a file (if `path` is Some) or stdout.
fn open_output(
    path: Option<&std::path::Path>,
) -> Result<Box<dyn Write>, Box<dyn std::error::Error>> {
    match path {
        Some(p) => {
            let f = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(p)?;
            Ok(Box::new(BufWriter::new(f)))
        }
        None => Ok(Box::new(BufWriter::new(io::stdout().lock()))),
    }
}
