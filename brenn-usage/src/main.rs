mod cli;

use std::io::{self, Write};

use clap::Parser;

use cc_usage::config;
use cc_usage::format::{csv, json, markdown};
use cc_usage::report::ReportType;
use cc_usage::{RunOptions, run};

use crate::cli::{Cli, Command, Format, resolve_scope};

fn main() {
    let cli = Cli::parse();
    if let Err(e) = run_main(cli) {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn run_main(cli: Cli) -> cc_usage::error::Result<()> {
    // Load config
    let config = config::load(cli.config.as_deref())?;

    let format = cli.format;
    let verbose = cli.verbose;

    let (report_type, session_ids, scope_args, include_invocations) = match cli.command {
        None => {
            // Default: session mode with top-level scope args
            (ReportType::Session, vec![], cli.scope, true)
        }
        Some(Command::Session(cmd)) => {
            let ids = cmd.session_ids;
            // Prefer subcommand-level scope flags; fall back to top-level flags
            // so that `brenn-usage -v --since 2d session` works as expected.
            let scope = if cmd.scope.is_empty() {
                cli.scope
            } else {
                cmd.scope
            };
            (ReportType::Session, ids, scope, !cmd.no_invocations)
        }
        Some(Command::Aggregate(cmd)) => {
            let ids = cmd.session_ids;
            // Prefer subcommand-level scope flags; fall back to top-level flags
            // so that `brenn-usage -v --since 2d aggregate` works as expected.
            let scope = if cmd.scope.is_empty() {
                cli.scope
            } else {
                cmd.scope
            };
            (ReportType::Aggregate, ids, scope, false)
        }
    };

    let scope = resolve_scope(session_ids, &scope_args)?;

    let mut opts = RunOptions::new(config, scope, report_type);
    opts.include_invocations = include_invocations;

    // Collect output into a buffer (no partial output on error)
    let report = run(opts)?;

    // Print warnings to stderr
    if verbose > 0 {
        for w in &report.warnings {
            eprintln!("warning: {}", w.message);
        }
    }

    let mut buf: Vec<u8> = Vec::new();

    match format {
        Format::Md => markdown::write(&report, &mut buf).map_err(stdout_io_err)?,
        Format::Csv => csv::write(&report, &mut buf).map_err(stdout_io_err)?,
        Format::Json => json::write(&report, &mut buf).map_err(stdout_io_err)?,
    }

    io::stdout().write_all(&buf).map_err(stdout_io_err)?;

    Ok(())
}

/// Wrap an `io::Error` writing to stdout into our `Error::Io`.
fn stdout_io_err(e: io::Error) -> cc_usage::error::Error {
    cc_usage::error::Error::Io {
        path: std::path::PathBuf::from("<stdout>"),
        source: e,
    }
}
