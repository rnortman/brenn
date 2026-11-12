//! Recognising a turn Claude refused for lack of quota or credit.
//!
//! Pure: no bridge, no clock, no I/O. Two inputs are classified:
//!
//! - a `rate_limit_event`'s `rate_limit_info`, read once into
//!   [`RateLimitInfo`], which under a subscription profile names the bucket
//!   that refused and when it lifts;
//! - a `result` frame's error text, which under an API-key profile carries the
//!   exhaustion message the profile's `exhausted_markers` match.

use brenn_envelope::agent::{AccountFlag, bucket};

/// How long a bucket refusal is assumed to last when the event does not say.
pub(in crate::active_bridge) const DEFAULT_REFUSAL_MS: u64 = 3_600_000;

/// The refusal text kept on a flag is cut to this many characters.
pub(in crate::active_bridge) const MESSAGE_CHARS: usize = 200;

/// What `rate_limit_info.status` says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::active_bridge) enum RateLimitStatus {
    /// The request went through.
    Allowed,
    /// The request went through, near a limit.
    AllowedWarning,
    /// The request was refused.
    Rejected,
    /// No `status` string.
    Missing,
    /// A `status` string this build does not know.
    Unrecognized(String),
}

/// The fields of a `rate_limit_event`'s `rate_limit_info` Brenn reads. Each is
/// `None` when absent or of the wrong type.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::active_bridge) struct RateLimitInfo<'a> {
    pub status: RateLimitStatus,
    /// Claude Code's name for the limit, e.g. `five_hour`.
    pub rate_limit_type: Option<&'a str>,
    /// When the limit lifts, epoch seconds. `None` also when its milliseconds
    /// do not fit an `i64`: no reader of a flag can hold that instant, so it
    /// reads as absent rather than reaching the account record.
    pub resets_at_s: Option<u64>,
    /// How much of the limit is used, as a fraction.
    pub utilization: Option<f64>,
}

impl<'a> RateLimitInfo<'a> {
    /// Read `rate_limit_info`.
    pub(in crate::active_bridge) fn of(info: &'a serde_json::Value) -> Self {
        let status = match info.get("status").and_then(|s| s.as_str()) {
            Some("allowed") => RateLimitStatus::Allowed,
            Some("allowed_warning") => RateLimitStatus::AllowedWarning,
            Some("rejected") => RateLimitStatus::Rejected,
            Some(other) => RateLimitStatus::Unrecognized(other.to_string()),
            None => RateLimitStatus::Missing,
        };
        Self {
            status,
            rate_limit_type: info.get("rateLimitType").and_then(|v| v.as_str()),
            resets_at_s: info.get("resetsAt").and_then(|v| v.as_u64()).filter(|s| {
                s.checked_mul(1000)
                    .is_some_and(|ms| i64::try_from(ms).is_ok())
            }),
            utilization: info.get("utilization").and_then(|v| v.as_f64()),
        }
    }
}

/// The bucket a refusal of `rate_limit_type` lands in, for a bridge running
/// `model`: `five_hour` → `five_hour`; `seven_day` → `general`; anything else
/// → `fable` when the model slug contains `fable` (ASCII case-insensitive),
/// else `general`.
pub(in crate::active_bridge) fn bucket_for(
    rate_limit_type: Option<&str>,
    model: Option<&str>,
) -> &'static str {
    match rate_limit_type {
        Some("five_hour") => bucket::FIVE_HOUR,
        Some("seven_day") => bucket::GENERAL,
        _ if model.is_some_and(|m| m.to_ascii_lowercase().contains("fable")) => bucket::FABLE,
        _ => bucket::GENERAL,
    }
}

/// A subscription bucket refusal read from a rejected event's
/// `rate_limit_info`.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::active_bridge) struct BucketRefusal {
    pub bucket: &'static str,
    pub flag: AccountFlag,
}

/// The refusal a rejected `rate_limit_info` describes, observed at `now_ms` in
/// the conversation `session`.
///
/// `until_ms` is `resetsAt` (epoch seconds) in milliseconds, or one hour after
/// `now_ms` when `resetsAt` is absent so the flag cannot outlive a missing
/// field. `kind` is the raw `rateLimitType`.
pub(in crate::active_bridge) fn bucket_refusal(
    info: &RateLimitInfo<'_>,
    model: Option<&str>,
    session: &str,
    now_ms: u64,
) -> BucketRefusal {
    let until_ms = info
        .resets_at_s
        .map_or(now_ms + DEFAULT_REFUSAL_MS, |s| s * 1000);
    BucketRefusal {
        bucket: bucket_for(info.rate_limit_type, model),
        flag: AccountFlag {
            at_ms: now_ms,
            session: Some(session.to_string()),
            message: None,
            until_ms: Some(until_ms),
            kind: info.rate_limit_type.map(str::to_string),
        },
    }
}

/// Whether a `result` frame is an API key's exhaustion refusal: it reports an
/// error and its text contains one of `markers` (ASCII case-insensitive).
pub(in crate::active_bridge) fn is_key_refusal(
    is_error: bool,
    text: Option<&str>,
    markers: &[String],
) -> bool {
    let Some(text) = text.filter(|_| is_error) else {
        return false;
    };
    let text = text.to_ascii_lowercase();
    markers
        .iter()
        .any(|marker| text.contains(&marker.to_ascii_lowercase()))
}

/// The account-wide flag a key refusal raises.
pub(in crate::active_bridge) fn key_refusal_flag(
    text: &str,
    session: &str,
    now_ms: u64,
) -> AccountFlag {
    AccountFlag {
        at_ms: now_ms,
        session: Some(session.to_string()),
        message: Some(text.chars().take(MESSAGE_CHARS).collect()),
        until_ms: None,
        kind: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const NOW: u64 = 1_794_484_800_000;

    #[test]
    fn status_table() {
        let of = |v: serde_json::Value| RateLimitInfo::of(&v).status;
        assert_eq!(of(json!({"status": "allowed"})), RateLimitStatus::Allowed);
        assert_eq!(
            of(json!({"status": "allowed_warning"})),
            RateLimitStatus::AllowedWarning
        );
        assert_eq!(of(json!({"status": "rejected"})), RateLimitStatus::Rejected);
        assert_eq!(
            of(json!({"status": "throttled"})),
            RateLimitStatus::Unrecognized("throttled".to_string())
        );
        assert_eq!(of(json!({"status": 3})), RateLimitStatus::Missing);
        assert_eq!(of(json!({})), RateLimitStatus::Missing);
    }

    #[test]
    fn the_read_fields_are_typed_or_absent() {
        let info = json!({
            "status": "allowed_warning",
            "rateLimitType": "five_hour",
            "resetsAt": 1_794_490_000,
            "utilization": 0.9,
        });
        assert_eq!(
            RateLimitInfo::of(&info),
            RateLimitInfo {
                status: RateLimitStatus::AllowedWarning,
                rate_limit_type: Some("five_hour"),
                resets_at_s: Some(1_794_490_000),
                utilization: Some(0.9),
            }
        );
        let wrong = json!({"rateLimitType": 5, "resetsAt": "soon", "utilization": "most"});
        let read = RateLimitInfo::of(&wrong);
        assert_eq!(read.rate_limit_type, None);
        assert_eq!(read.resets_at_s, None);
        assert_eq!(read.utilization, None);
    }

    #[test]
    fn bucket_rule() {
        assert_eq!(bucket_for(Some("five_hour"), None), bucket::FIVE_HOUR);
        assert_eq!(
            bucket_for(Some("five_hour"), Some("claude-fable-1")),
            bucket::FIVE_HOUR
        );
        assert_eq!(bucket_for(Some("seven_day"), None), bucket::GENERAL);
        assert_eq!(
            bucket_for(Some("seven_day"), Some("claude-fable-1")),
            bucket::GENERAL
        );
        assert_eq!(
            bucket_for(Some("seven_day_opus"), Some("claude-FABLE-1")),
            bucket::FABLE
        );
        assert_eq!(
            bucket_for(Some("seven_day_opus"), Some("claude-opus-4")),
            bucket::GENERAL
        );
        assert_eq!(bucket_for(None, Some("fable")), bucket::FABLE);
        assert_eq!(bucket_for(None, None), bucket::GENERAL);
    }

    #[test]
    fn a_rejection_with_resets_at_lifts_then() {
        let info =
            json!({"status": "rejected", "rateLimitType": "five_hour", "resetsAt": 1_794_490_000});
        let refusal = bucket_refusal(&RateLimitInfo::of(&info), Some("claude-opus-4"), "7", NOW);
        assert_eq!(refusal.bucket, bucket::FIVE_HOUR);
        assert_eq!(
            refusal.flag,
            AccountFlag {
                at_ms: NOW,
                session: Some("7".to_string()),
                message: None,
                until_ms: Some(1_794_490_000_000),
                kind: Some("five_hour".to_string()),
            }
        );
    }

    #[test]
    fn a_rejection_without_resets_at_lifts_an_hour_later() {
        let info = json!({"status": "rejected", "rateLimitType": "seven_day"});
        let refusal = bucket_refusal(&RateLimitInfo::of(&info), None, "7", NOW);
        assert_eq!(refusal.bucket, bucket::GENERAL);
        assert_eq!(refusal.flag.until_ms, Some(NOW + DEFAULT_REFUSAL_MS));
    }

    #[test]
    fn a_resets_at_past_a_signed_millisecond_instant_reads_as_absent() {
        let last = i64::MAX as u64 / 1000;
        let fits = json!({"status": "rejected", "resetsAt": last});
        assert_eq!(RateLimitInfo::of(&fits).resets_at_s, Some(last));
        for past in [last + 1, u64::MAX] {
            let info =
                json!({"status": "rejected", "rateLimitType": "five_hour", "resetsAt": past});
            let read = RateLimitInfo::of(&info);
            assert_eq!(read.resets_at_s, None, "{past}");
            let refusal = bucket_refusal(&read, None, "7", NOW);
            assert_eq!(refusal.flag.until_ms, Some(NOW + DEFAULT_REFUSAL_MS));
        }
    }

    #[test]
    fn a_rejection_without_a_type_has_no_kind() {
        let info = json!({"status": "rejected", "resetsAt": 1});
        let refusal = bucket_refusal(&RateLimitInfo::of(&info), None, "7", NOW);
        assert_eq!(refusal.flag.kind, None);
        assert_eq!(refusal.flag.until_ms, Some(1000));
    }

    fn markers() -> Vec<String> {
        vec!["credit balance is too low".to_string()]
    }

    #[test]
    fn a_marker_matches_case_insensitively() {
        assert!(is_key_refusal(
            true,
            Some("API Error: Credit Balance Is Too Low to access the API"),
            &markers()
        ));
    }

    #[test]
    fn a_marker_needs_is_error() {
        assert!(!is_key_refusal(
            false,
            Some("credit balance is too low"),
            &markers()
        ));
    }

    #[test]
    fn an_error_without_a_marker_is_not_a_refusal() {
        assert!(!is_key_refusal(true, Some("overloaded"), &markers()));
        assert!(!is_key_refusal(true, None, &markers()));
    }

    #[test]
    fn any_of_several_markers_matches() {
        let markers = vec!["first".to_string(), "second".to_string()];
        assert!(is_key_refusal(true, Some("the SECOND one"), &markers));
    }

    #[test]
    fn the_flag_message_is_cut_to_200_characters() {
        let text = "é".repeat(300);
        let flag = key_refusal_flag(&text, "9", NOW);
        assert_eq!(
            flag.message.as_deref().map(|m| m.chars().count()),
            Some(200)
        );
        assert_eq!(flag.until_ms, None);
        assert_eq!(flag.session.as_deref(), Some("9"));
        assert_eq!(flag.at_ms, NOW);
    }
}
