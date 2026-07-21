use std::num::NonZeroU32;
use std::path::PathBuf;

use chrono::{DateTime, Duration, Local, NaiveDate, TimeZone, Utc};
use clap::{Args, Parser, Subcommand};

use cc_usage::error::{Error, Result};
use cc_usage::report::Window;

#[derive(Parser)]
#[command(
    name = "brenn-usage",
    about = "Claude Code subagent usage analyzer",
    long_about = "Parse Claude Code session logs and report usage broken out by parent agent and subagent."
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,

    /// Scope flags (used when no subcommand given — implies 'session')
    #[command(flatten)]
    pub scope: ScopeArgs,

    /// Output format.
    #[arg(long, global = true, value_name = "FORMAT", default_value = "md")]
    pub format: Format,

    /// Path to TOML config file.
    #[arg(long, global = true, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// Verbosity (-v = warnings, -vv = debug).
    #[arg(short, long, global = true, action = clap::ArgAction::Count)]
    pub verbose: u8,
}

#[derive(Subcommand)]
pub enum Command {
    /// Per-session usage report (default when no subcommand given).
    Session(SessionCmd),
    /// Aggregate usage report across sessions.
    Aggregate(AggregateCmd),
}

#[derive(Args)]
pub struct SessionCmd {
    /// Explicit session IDs (mutually exclusive with --from/--to/--since/--last).
    #[arg(value_name = "SESSION_ID", conflicts_with_all = ["from", "to", "since", "last"])]
    pub session_ids: Vec<String>,

    /// Scope flags.
    #[command(flatten)]
    pub scope: ScopeArgs,

    /// Suppress per-invocation rows.
    #[arg(long)]
    pub no_invocations: bool,
}

#[derive(Args)]
pub struct AggregateCmd {
    /// Explicit session IDs (mutually exclusive with --from/--to/--since/--last).
    #[arg(value_name = "SESSION_ID", conflicts_with_all = ["from", "to", "since", "last"])]
    pub session_ids: Vec<String>,

    /// Scope flags.
    #[command(flatten)]
    pub scope: ScopeArgs,
}

/// Shared scope flags (used on both subcommands and the top-level).
#[derive(Args, Default)]
pub struct ScopeArgs {
    /// Start of time window (inclusive), format YYYY-MM-DD.
    #[arg(long, value_name = "DATE", group = "scope_flag")]
    pub from: Option<String>,

    /// End of time window (inclusive day), format YYYY-MM-DD.
    #[arg(long, value_name = "DATE", requires = "from")]
    pub to: Option<String>,

    /// Time window as duration from now (e.g. 7d, 30d, 2w, 12h).
    #[arg(long, value_name = "DURATION", group = "scope_flag")]
    pub since: Option<String>,

    /// Last N sessions by most-recent usage timestamp.
    #[arg(long, short = 'n', value_name = "N", value_parser = clap::value_parser!(NonZeroU32), group = "scope_flag")]
    pub last: Option<NonZeroU32>,
}

impl ScopeArgs {
    /// Returns true when no scope flag was provided.
    pub fn is_empty(&self) -> bool {
        self.from.is_none() && self.to.is_none() && self.since.is_none() && self.last.is_none()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Format {
    Md,
    Csv,
    Json,
}

/// Resolve scope args into a `cc_usage::aggregate::Scope`.
pub fn resolve_scope(
    session_ids: Vec<String>,
    scope: &ScopeArgs,
) -> Result<cc_usage::aggregate::Scope> {
    use cc_usage::aggregate::Scope;

    if !session_ids.is_empty() {
        return Ok(Scope::Explicit(session_ids));
    }

    if let Some(since) = &scope.since {
        let dur = parse_duration(since)?;
        let now = Utc::now();
        let from = now - dur;
        return Ok(Scope::Window(Window {
            from: Some(from),
            to: Some(now),
        }));
    }

    if let Some(from_str) = &scope.from {
        let from = parse_date_to_utc_start(from_str)?;
        let to = if let Some(to_str) = &scope.to {
            // to = start of (to_date + 1 day) = exclusive upper bound
            let to_date = parse_date(to_str)?;
            let to_date_next = to_date
                .succ_opt()
                .ok_or_else(|| Error::Date("date overflow computing end of window".into()))?;
            date_to_utc_start(to_date_next)?
        } else {
            // No --to: open-ended upper bound
            return Ok(Scope::Window(Window {
                from: Some(from),
                to: None,
            }));
        };
        return Ok(Scope::Window(Window {
            from: Some(from),
            to: Some(to),
        }));
    }

    if let Some(n) = scope.last {
        return Ok(Scope::LastN(n));
    }

    // Default: last 1
    Ok(Scope::LastN(NonZeroU32::new(1).unwrap()))
}

/// Parse a duration string like "7d", "30d", "2w", "12h". Negative numbers and
/// non-ASCII characters are rejected.
pub fn parse_duration(s: &str) -> Result<Duration> {
    if s.is_empty() {
        return Err(Error::Duration("empty duration string".to_string()));
    }
    // Use `chars().last()` so we don't slice mid-codepoint on non-ASCII input.
    let last_char = s
        .chars()
        .last()
        .ok_or_else(|| Error::Duration(format!("invalid duration '{s}': expected NUMu")))?;
    let num_str = &s[..s.len() - last_char.len_utf8()];
    // Parse as u64 — negative durations are not meaningful for `--since` and
    // would silently produce empty reports (`from > to`).
    let n: u64 = num_str.parse().map_err(|_| {
        Error::Duration(format!(
            "invalid duration '{s}': expected non-negative NUMu where u is d/h/w"
        ))
    })?;
    let n_i64 =
        i64::try_from(n).map_err(|_| Error::Duration(format!("duration '{s}' is too large")))?;
    match last_char {
        'd' => Ok(Duration::days(n_i64)),
        'h' => Ok(Duration::hours(n_i64)),
        'w' => Ok(Duration::weeks(n_i64)),
        _ => Err(Error::Duration(format!(
            "invalid duration unit '{last_char}' in '{s}': expected d (days), h (hours), or w (weeks)"
        ))),
    }
}

fn parse_date(s: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map_err(|e| Error::Date(format!("invalid date '{s}': {e}")))
}

fn parse_date_to_utc_start(s: &str) -> Result<DateTime<Utc>> {
    let date = parse_date(s)?;
    date_to_utc_start(date)
}

fn date_to_utc_start(date: NaiveDate) -> Result<DateTime<Utc>> {
    let local_midnight = Local
        .from_local_datetime(&date.and_hms_opt(0, 0, 0).unwrap())
        .single()
        .ok_or_else(|| Error::Date(format!("ambiguous or non-existent local time for {date}")))?;
    Ok(local_midnight.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_duration_valid() {
        assert_eq!(parse_duration("7d").unwrap(), Duration::days(7));
        assert_eq!(parse_duration("12h").unwrap(), Duration::hours(12));
        assert_eq!(parse_duration("2w").unwrap(), Duration::weeks(2));
    }

    #[test]
    fn parse_duration_rejects_negative() {
        let err = parse_duration("-7d").unwrap_err();
        assert!(matches!(err, Error::Duration(_)), "got {err:?}");
    }

    #[test]
    fn parse_duration_rejects_multibyte_unit() {
        // Last char is multi-byte UTF-8 — must not panic.
        let err = parse_duration("7\u{65E5}").unwrap_err(); // 7日
        assert!(matches!(err, Error::Duration(_)), "got {err:?}");
    }

    #[test]
    fn parse_duration_rejects_empty() {
        let err = parse_duration("").unwrap_err();
        assert!(matches!(err, Error::Duration(_)));
    }

    #[test]
    fn parse_duration_rejects_unknown_unit() {
        let err = parse_duration("7x").unwrap_err();
        assert!(matches!(err, Error::Duration(_)));
    }
}
