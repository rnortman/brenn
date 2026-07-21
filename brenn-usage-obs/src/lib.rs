//! Library portion of `brenn-usage-obs` — shared helpers callable from tests.

use chrono::{DateTime, Utc};

/// Parse an ISO-8601 timestamp or a bare `YYYY-MM-DD` date (UTC midnight).
///
/// Delegates to `brenn_lib::usage::parse_ts_str`; wraps the error type for
/// callers that need `Box<dyn Error>`.
pub fn parse_ts(s: &str) -> Result<DateTime<Utc>, Box<dyn std::error::Error>> {
    brenn_lib::usage::parse_ts_str(s).map_err(|e| e.into())
}
