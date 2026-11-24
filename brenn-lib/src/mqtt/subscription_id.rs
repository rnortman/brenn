//! The MQTT 5 Subscription Identifier a topic filter is subscribed with.

use sha2::{Digest, Sha256};

/// The largest Subscription Identifier MQTT 5.0 section 3.8.2.1.2 allows: the
/// largest value a variable byte integer holds.
pub const MAX_SUBSCRIPTION_IDENTIFIER: u32 = 268_435_455;

/// The Subscription Identifier every Brenn process subscribes `topic_filter`
/// with: the first four bytes of the filter's SHA-256, big-endian, reduced into
/// `1..=MAX_SUBSCRIPTION_IDENTIFIER`.
///
/// The value is shared with the broker's session, which can outlive the
/// process: a resumed session delivers what it queued under the identifier the
/// filter had when the copy was matched, and a later process resolves it only
/// because it derives the same identifier from the same filter. Changing this
/// function drops the queued messages of every resumed session once.
///
/// Two different filters can share an identifier; the space is 28 bits and
/// filter strings are unbounded.
pub fn subscription_identifier(topic_filter: &str) -> u32 {
    let digest = Sha256::digest(topic_filter.as_bytes());
    let head = u32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]]);
    head % MAX_SUBSCRIPTION_IDENTIFIER + 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_are_pinned() {
        assert_eq!(subscription_identifier("a/#"), 73_469_377);
        assert_eq!(subscription_identifier("a/b"), 21_814_733);
        assert_eq!(subscription_identifier("home/state"), 167_517_825);
    }

    #[test]
    fn identifiers_are_within_the_range_mqtt_allows() {
        let long = "level/".repeat(2_000) + "#";
        for filter in ["", long.as_str()] {
            let id = subscription_identifier(filter);
            assert!(
                (1..=MAX_SUBSCRIPTION_IDENTIFIER).contains(&id),
                "{id} for a filter of {} bytes",
                filter.len()
            );
        }
    }
}
