//! MQTT address parsing: `mqtt:<client-slug>:<topic>`.
//!
//! Two public entry points:
//! - `parse_mqtt_address` — base parse; no wildcard rules enforced.
//! - `parse_topic_filter` — adds MQTT wildcard validation (`+`, `#`).
//! - `parse_topic_name`   — rejects any `+` or `#` (publish context).

use crate::mqtt::config::is_valid_client_slug;
use crate::mqtt::error::MqttError;

/// The MQTT address prefix.
const MQTT_PREFIX: &str = "mqtt:";

/// True iff `addr` is in the `mqtt:` address scheme (prefix check only, no
/// structural validation). Used to select which channel addresses feed the MQTT
/// ingress path before the full `parse_mqtt_address` runs.
pub fn is_mqtt_address(addr: &str) -> bool {
    addr.starts_with(MQTT_PREFIX)
}

/// A parsed MQTT address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqttAddress {
    /// Client slug (validated charset `[A-Za-z0-9._~-]+`).
    pub client: String,
    /// Topic string (validated UTF-8, 1..=65535 bytes, no NUL).
    pub topic: String,
}

impl MqttAddress {
    /// Produce canonical `mqtt:<client>:<topic>` string.
    pub fn format(&self) -> String {
        format!("mqtt:{}:{}", self.client, self.topic)
    }
}

impl std::fmt::Display for MqttAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.format())
    }
}

/// Parse a raw address string into `(client, topic)` without wildcard checks.
///
/// Validates:
/// - `mqtt:` prefix present.
/// - First `:` after prefix separates client from topic.
/// - Client slug matches `[A-Za-z0-9._~-]+`.
/// - Topic is 1..=65535 bytes, no embedded NUL.
pub fn parse_mqtt_address(addr: &str) -> Result<MqttAddress, MqttError> {
    let rest = addr
        .strip_prefix(MQTT_PREFIX)
        .ok_or_else(|| MqttError::WrongProtocol {
            address: addr.to_string(),
        })?;

    // Split on the first `:` to separate client from topic.
    let colon_pos = rest.find(':').ok_or_else(|| MqttError::AddressInvalid {
        address: addr.to_string(),
        detail: "missing separator between client and topic (expected `mqtt:<client>:<topic>`)"
            .to_string(),
    })?;

    let client = &rest[..colon_pos];
    let topic = &rest[colon_pos + 1..];

    // Validate client slug.
    if !is_valid_client_slug(client) {
        return Err(MqttError::AddressInvalid {
            address: addr.to_string(),
            detail: format!(
                "client slug {client:?} is invalid; must match [A-Za-z0-9._~-]+ and be non-empty",
            ),
        });
    }

    // Validate topic.
    if topic.is_empty() {
        return Err(MqttError::AddressInvalid {
            address: addr.to_string(),
            detail: "topic must be non-empty".to_string(),
        });
    }
    let topic_bytes = topic.len();
    if topic_bytes > 65535 {
        return Err(MqttError::AddressInvalid {
            address: addr.to_string(),
            detail: format!("topic is {topic_bytes} bytes; max is 65535"),
        });
    }
    if topic.contains('\0') {
        return Err(MqttError::AddressInvalid {
            address: addr.to_string(),
            detail: "topic must not contain embedded NUL bytes".to_string(),
        });
    }

    Ok(MqttAddress {
        client: client.to_string(),
        topic: topic.to_string(),
    })
}

/// Validate that `+` appears only as a complete level segment and `#` is
/// a terminal segment preceded by `/` or the string boundary.
///
/// Called after `parse_mqtt_address` in subscription / topic-filter contexts.
pub fn parse_topic_filter(addr: &str) -> Result<MqttAddress, MqttError> {
    let parsed = parse_mqtt_address(addr)?;
    validate_topic_filter(&parsed.topic).map_err(|detail| MqttError::AddressInvalid {
        address: addr.to_string(),
        detail,
    })?;
    Ok(parsed)
}

/// Reject any `+` or `#` in the topic. Called for `MqttSend` (publish) context.
pub fn parse_topic_name(addr: &str) -> Result<MqttAddress, MqttError> {
    let parsed = parse_mqtt_address(addr)?;
    if parsed.topic.contains('+') || parsed.topic.contains('#') {
        return Err(MqttError::WildcardNotAllowed {
            address: addr.to_string(),
        });
    }
    Ok(parsed)
}

/// Validate MQTT wildcard rules in a bare topic filter string (no `mqtt:` prefix).
///
/// Used by `mqtt::config` to validate subscription topic filters without
/// constructing a full `mqtt:<client>:<topic>` address.
///
/// Returns `Err(detail_string)` on violation.
pub fn validate_topic_filter_str(topic: &str) -> Result<(), String> {
    validate_topic_filter(topic)
}

/// Validate MQTT wildcard rules in a topic filter string.
///
/// Rules:
/// - `+` must be a complete level segment (preceded by `/` or start, followed
///   by `/` or end).
/// - `#` must be the terminal segment, preceded by `/` or start, end of string
///   after `#`.
///
/// Returns `Err(detail_string)` on violation.
fn validate_topic_filter(topic: &str) -> Result<(), String> {
    let segments: Vec<&str> = topic.split('/').collect();

    for (i, seg) in segments.iter().enumerate() {
        let is_last = i == segments.len() - 1;

        if seg.contains('#') {
            // `#` must be the only character in its segment AND must be terminal.
            if *seg != "#" {
                return Err(format!(
                    "wildcard '#' must be a complete segment (got {seg:?})",
                ));
            }
            if !is_last {
                return Err(
                    "wildcard '#' must be the terminal segment in a topic filter".to_string(),
                );
            }
        }

        if seg.contains('+') {
            // `+` must be the only character in its segment.
            if *seg != "+" {
                return Err(format!(
                    "wildcard '+' must be a complete segment (got {seg:?})",
                ));
            }
        }
    }

    Ok(())
}

/// Match a concrete published topic against an MQTT topic filter.
///
/// Implements the standard MQTT subscription wildcard semantics:
/// - A `+` filter segment matches exactly one topic level (any value), and
///   never crosses a `/` boundary.
/// - A terminal `#` filter segment matches the parent level and all of its
///   descendant levels (zero or more remaining topic segments). Per the MQTT
///   spec, `sport/#` matches both `sport` and `sport/anything`.
/// - Any other filter segment must equal the corresponding topic segment.
/// - A filter whose *first* segment is a wildcard (`#` or `+`) does not match a
///   topic whose *first* segment starts with `$` (system topics such as
///   `$SYS/...`). The rule applies only to the first level: `a/#` still matches
///   `a/$weird`, and `$SYS/#` still matches `$SYS/broker/load` because its first
///   segment is the literal `$SYS`.
///
/// `filter` is assumed already validated by `validate_topic_filter` (so `#` is
/// terminal and `+`/`#` occupy complete segments); this function does not
/// re-validate filter syntax. `topic` is a concrete topic name (no wildcards).
///
/// Returns `true` iff `topic` matches `filter`.
pub fn mqtt_topic_matches(filter: &str, topic: &str) -> bool {
    let filter_segments: Vec<&str> = filter.split('/').collect();
    let topic_segments: Vec<&str> = topic.split('/').collect();

    // A leading wildcard must not match a `$`-prefixed system topic. `split`
    // on a non-empty pattern always yields at least one segment.
    let first_filter = filter_segments[0];
    if (first_filter == "#" || first_filter == "+") && topic_segments[0].starts_with('$') {
        return false;
    }

    let mut fi = 0;
    let mut ti = 0;

    while fi < filter_segments.len() {
        let fseg = filter_segments[fi];

        if fseg == "#" {
            // Terminal multi-level wildcard: matches all remaining levels,
            // including zero (so `sport/#` matches `sport`).
            return true;
        }

        // For every non-`#` filter segment we need a topic segment to match.
        if ti >= topic_segments.len() {
            return false;
        }

        if fseg == "+" {
            // Single-level wildcard: matches exactly one topic segment.
        } else if fseg != topic_segments[ti] {
            return false;
        }

        fi += 1;
        ti += 1;
    }

    // Filter exhausted: match iff topic is also exhausted (no trailing levels).
    ti == topic_segments.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- parse_mqtt_address ---

    #[test]
    fn valid_simple_address() {
        let addr = parse_mqtt_address("mqtt:homeassistant:home/switch/state").unwrap();
        assert_eq!(addr.client, "homeassistant");
        assert_eq!(addr.topic, "home/switch/state");
    }

    #[test]
    fn address_format_roundtrip() {
        let raw = "mqtt:broker1:sensor/temp";
        let addr = parse_mqtt_address(raw).unwrap();
        assert_eq!(addr.format(), raw);
    }

    #[test]
    fn topic_may_contain_colon() {
        // e.g. device:aa:bb:cc/state — only the first `:` after `mqtt:` splits.
        let addr = parse_mqtt_address("mqtt:ha:device:aa:bb:cc/state").unwrap();
        assert_eq!(addr.client, "ha");
        assert_eq!(addr.topic, "device:aa:bb:cc/state");
    }

    #[test]
    fn wrong_protocol_prefix() {
        let err = parse_mqtt_address("brenn:foo:bar").unwrap_err();
        assert!(matches!(err, MqttError::WrongProtocol { .. }));
    }

    #[test]
    fn missing_separator_after_prefix() {
        // `mqtt:foo` — no second `:`.
        let err = parse_mqtt_address("mqtt:foo").unwrap_err();
        assert!(matches!(err, MqttError::AddressInvalid { .. }));
    }

    #[test]
    fn missing_client_segment_is_error() {
        // `mqtt:home/x` — no `:` after the prefix, so there is no client segment.
        // The client is mandatory (design §1 decision 1); this is a parse error,
        // NOT a client-omission form that yields `client = ""` / topic `home/x`.
        let err = parse_mqtt_address("mqtt:home/x").unwrap_err();
        assert!(matches!(err, MqttError::AddressInvalid { .. }));
    }

    #[test]
    fn empty_client() {
        // `mqtt::topic`
        let err = parse_mqtt_address("mqtt::topic").unwrap_err();
        assert!(matches!(err, MqttError::AddressInvalid { .. }));
    }

    #[test]
    fn empty_topic() {
        // `mqtt:broker:`
        let err = parse_mqtt_address("mqtt:broker:").unwrap_err();
        assert!(matches!(err, MqttError::AddressInvalid { .. }));
    }

    #[test]
    fn nul_byte_in_topic() {
        let addr = "mqtt:broker:top\0ic";
        let err = parse_mqtt_address(addr).unwrap_err();
        assert!(matches!(err, MqttError::AddressInvalid { .. }));
    }

    #[test]
    fn oversize_topic() {
        let long_topic: String = "a".repeat(65536);
        let addr = format!("mqtt:broker:{long_topic}");
        let err = parse_mqtt_address(&addr).unwrap_err();
        assert!(matches!(err, MqttError::AddressInvalid { .. }));
    }

    #[test]
    fn max_size_topic_ok() {
        let topic: String = "a".repeat(65535);
        let addr_str = format!("mqtt:broker:{topic}");
        let addr = parse_mqtt_address(&addr_str).unwrap();
        assert_eq!(addr.topic.len(), 65535);
    }

    // `mqtt:bad:slug:topic` → client=`bad`, topic=`slug:topic` — valid.
    // Colons in the topic are legal (e.g. MAC-address identifiers).
    #[test]
    fn client_colon_lands_in_topic() {
        let addr = parse_mqtt_address("mqtt:ha:a:b:c").unwrap();
        assert_eq!(addr.client, "ha");
        assert_eq!(addr.topic, "a:b:c");
    }

    #[test]
    fn client_slug_with_all_valid_chars() {
        let addr = parse_mqtt_address("mqtt:client.name_with~dashes-ok:t").unwrap();
        assert_eq!(addr.client, "client.name_with~dashes-ok");
    }

    #[test]
    fn client_slug_with_space_invalid() {
        let err = parse_mqtt_address("mqtt:bad slug:topic").unwrap_err();
        assert!(matches!(err, MqttError::AddressInvalid { .. }));
    }

    // --- parse_topic_filter ---

    #[test]
    fn filter_single_level_wildcard() {
        let addr = parse_topic_filter("mqtt:ha:home/+/state").unwrap();
        assert_eq!(addr.topic, "home/+/state");
    }

    #[test]
    fn filter_multi_level_wildcard_terminal() {
        let addr = parse_topic_filter("mqtt:ha:home/#").unwrap();
        assert_eq!(addr.topic, "home/#");
    }

    #[test]
    fn filter_only_hash() {
        let addr = parse_topic_filter("mqtt:ha:#").unwrap();
        assert_eq!(addr.topic, "#");
    }

    #[test]
    fn filter_only_plus() {
        let addr = parse_topic_filter("mqtt:ha:+").unwrap();
        assert_eq!(addr.topic, "+");
    }

    #[test]
    fn filter_multiple_plus() {
        let addr = parse_topic_filter("mqtt:ha:+/+").unwrap();
        assert_eq!(addr.topic, "+/+");
    }

    #[test]
    fn filter_hash_not_terminal_rejected() {
        let err = parse_topic_filter("mqtt:ha:home/#/extra").unwrap_err();
        assert!(matches!(err, MqttError::AddressInvalid { .. }));
    }

    #[test]
    fn filter_plus_not_complete_segment_rejected() {
        let err = parse_topic_filter("mqtt:ha:home/+x").unwrap_err();
        assert!(matches!(err, MqttError::AddressInvalid { .. }));
    }

    #[test]
    fn filter_hash_not_complete_segment_rejected() {
        let err = parse_topic_filter("mqtt:ha:home/#extra").unwrap_err();
        assert!(matches!(err, MqttError::AddressInvalid { .. }));
    }

    // --- parse_topic_name ---

    #[test]
    fn topic_name_no_wildcards_ok() {
        let addr = parse_topic_name("mqtt:ha:home/switch/state").unwrap();
        assert_eq!(addr.topic, "home/switch/state");
    }

    #[test]
    fn topic_name_plus_wildcard_rejected() {
        let err = parse_topic_name("mqtt:ha:home/+/state").unwrap_err();
        assert!(matches!(err, MqttError::WildcardNotAllowed { .. }));
    }

    #[test]
    fn topic_name_hash_wildcard_rejected() {
        let err = parse_topic_name("mqtt:ha:home/#").unwrap_err();
        assert!(matches!(err, MqttError::WildcardNotAllowed { .. }));
    }

    // --- mqtt_topic_matches ---

    #[test]
    fn match_exact() {
        assert!(mqtt_topic_matches("home/switch/state", "home/switch/state"));
        assert!(!mqtt_topic_matches(
            "home/switch/state",
            "home/switch/other"
        ));
    }

    #[test]
    fn match_exact_differing_length_non_match() {
        // Same prefix but topic has an extra trailing level.
        assert!(!mqtt_topic_matches("home/switch", "home/switch/state"));
        // Topic shorter than filter.
        assert!(!mqtt_topic_matches("home/switch/state", "home/switch"));
    }

    #[test]
    fn match_single_level_plus() {
        assert!(mqtt_topic_matches("home/+/state", "home/switch/state"));
        assert!(mqtt_topic_matches("home/+/state", "home/lamp/state"));
        // Wrong tail.
        assert!(!mqtt_topic_matches("home/+/state", "home/switch/level"));
    }

    #[test]
    fn match_plus_does_not_cross_slash() {
        // `+` matches exactly one level; `a/b` is two levels, so a single `+`
        // in that position must not match.
        assert!(!mqtt_topic_matches("home/+/state", "home/a/b/state"));
        assert!(!mqtt_topic_matches("home/+", "home/a/b"));
    }

    #[test]
    fn match_plus_requires_a_level() {
        // `+` must consume exactly one level; a missing level is not a match.
        assert!(!mqtt_topic_matches("home/+/state", "home/state"));
    }

    #[test]
    fn match_multi_level_hash_terminal() {
        assert!(mqtt_topic_matches("home/#", "home/switch/state"));
        assert!(mqtt_topic_matches("home/#", "home/switch"));
        // Non-match: different root.
        assert!(!mqtt_topic_matches("home/#", "office/switch"));
    }

    #[test]
    fn match_hash_matches_parent_and_descendants() {
        // Per MQTT spec, `sport/#` matches the parent level `sport` itself,
        // and every descendant level.
        assert!(mqtt_topic_matches("sport/#", "sport"));
        assert!(mqtt_topic_matches("sport/#", "sport/tennis"));
        assert!(mqtt_topic_matches("sport/#", "sport/tennis/player1"));
        // But not a sibling of the parent.
        assert!(!mqtt_topic_matches("sport/#", "sporting"));
    }

    #[test]
    fn match_bare_hash_matches_everything() {
        assert!(mqtt_topic_matches("#", "anything"));
        assert!(mqtt_topic_matches("#", "a/b/c"));
        assert!(mqtt_topic_matches("#", "a"));
    }

    #[test]
    fn match_bare_plus() {
        assert!(mqtt_topic_matches("+", "a"));
        assert!(!mqtt_topic_matches("+", "a/b"));
    }

    #[test]
    fn match_leading_plus() {
        assert!(mqtt_topic_matches("+/state", "home/state"));
        assert!(!mqtt_topic_matches("+/state", "home/switch/state"));
    }

    #[test]
    fn match_leading_wildcard_excludes_dollar_topics() {
        // A leading `#`/`+` must not match a `$`-prefixed system topic.
        assert!(!mqtt_topic_matches("#", "$SYS/broker/load"));
        assert!(!mqtt_topic_matches("#", "$SYS"));
        assert!(!mqtt_topic_matches("+", "$SYS"));
        assert!(!mqtt_topic_matches("+/monitor", "$SYS/monitor"));
    }

    #[test]
    fn match_literal_dollar_anchor_bypasses_guard() {
        // A literal `$SYS` first segment lets an operator deliberately bridge
        // system topics; the guard only fires on a leading wildcard.
        assert!(mqtt_topic_matches("$SYS/#", "$SYS/broker/load"));
        assert!(mqtt_topic_matches("$SYS/+", "$SYS/broker"));
        assert!(mqtt_topic_matches("$SYS/broker", "$SYS/broker"));
    }

    #[test]
    fn match_dollar_guard_is_first_level_only() {
        // The rule applies only to the first level; `$` deeper in the topic is
        // unaffected, and a broad filter still matches normal topics.
        assert!(mqtt_topic_matches("a/#", "a/$weird"));
        assert!(mqtt_topic_matches("#", "sport/tennis"));
    }
}
