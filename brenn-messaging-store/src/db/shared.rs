//! Shared timestamp helpers used by multiple db submodules.

use chrono::{DateTime, Utc};

/// Parse an RFC3339 timestamp into UTC. Returns `None` on parse failure.
/// Shared by the query path and the row-decoder path so we don't have
/// two slightly different inline copies of the same conversion.
pub(crate) fn parse_rfc3339(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|d| d.with_timezone(&Utc))
}

/// Convert a nanoseconds-since-epoch i64 to a chrono `DateTime<Utc>`.
pub fn ns_to_utc(ns: i64) -> DateTime<Utc> {
    let secs = ns.div_euclid(1_000_000_000);
    let nanos = ns.rem_euclid(1_000_000_000) as u32;
    DateTime::<Utc>::from_timestamp(secs, nanos).unwrap_or_else(|| {
        panic!("messaging: publish_ts_ns {ns} is out of range for DateTime<Utc>")
    })
}

/// Convert a `DateTime<Utc>` to nanoseconds-since-epoch.
pub fn utc_to_ns(dt: DateTime<Utc>) -> i64 {
    dt.timestamp() * 1_000_000_000 + dt.timestamp_subsec_nanos() as i64
}
