//! Envelope frames for tests, built through the one canonical serializer.
//!
//! A test host — the page harness, an out-of-tree component's own suite —
//! windows envelope text at a guest exactly as a real host does. That text is
//! [`MessageEnvelope::to_envelope_json`] and nothing else: a hand-written JSON
//! object beside it is a second wire format that no host produces and that
//! stops matching the moment the frame gains or renames a field.
//!
//! Placement-neutral on purpose. A component that runs on the backend and on
//! the page is tested at both placements, and neither suite should reach into
//! the other's fixture crate for the frame both hosts hand it.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::{ChannelScheme, MessageEnvelope, Urgency};

/// The activation clock every fixture here stamps. Fixed: a moving one would be
/// a transcript that changes for no reason.
pub const NOW_MS: u64 = 1_767_225_600_000;

/// The channel every fixture envelope claims to have arrived on. Test fixtures
/// assert on bodies, not on provenance; a component that reads this field is
/// reading a fixture's literal and should be handed [`envelope_on`] instead.
pub const FIXTURE_CHANNEL: &str = "brenn:fixture";

/// A message id in the shape the envelope parser demands, derived from a
/// readable label so a failure names the delivery it came from.
pub fn message_id(label: &str) -> Uuid {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in label.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let text = format!("00000000-0000-4000-8000-{:012x}", hash & 0xffff_ffff_ffff);
    Uuid::parse_str(&text).expect("a v4-shaped literal parses as a uuid")
}

/// The fixed non-identifying frame a fixture delivery wears, carrying `body`.
pub fn envelope(id: &str, body: &str) -> String {
    envelope_on(FIXTURE_CHANNEL, id, body)
}

/// [`envelope`] on a named channel, for a component that reads the channel it
/// was delivered on.
///
/// # Panics
///
/// If `channel` carries no recognized scheme prefix — the frame's
/// `envelope_type` is the channel's scheme, and a fixture that disagrees with
/// itself is worse than no fixture.
pub fn envelope_on(channel: &str, id: &str, body: &str) -> String {
    let scheme = ChannelScheme::of(channel)
        .unwrap_or_else(|| panic!("fixture channel {channel:?} carries no scheme prefix"));
    MessageEnvelope {
        message_id: message_id(id),
        source: "fixture".to_string(),
        channel: channel.to_string(),
        sender: "fixture".to_string(),
        publish_ts: publish_ts(),
        body: body.to_string(),
        reply_to: None,
        delivery_deadline: None,
        deliver_after: None,
        impetus: None,
        urgency: Urgency::Normal,
        envelope_type: scheme,
    }
    .to_envelope_json()
}

/// The fixed publish timestamp, [`NOW_MS`] read as an instant.
fn publish_ts() -> DateTime<Utc> {
    DateTime::from_timestamp_millis(NOW_MS as i64).expect("NOW_MS is a representable instant")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn a_fixture_frame_parses_as_the_envelope_it_was_built_from() {
        let text = envelope("delivery", "{\"n\":1}");
        let parsed: MessageEnvelope = serde_json::from_str(&text).expect("frame round-trips");
        assert_eq!(parsed.body, "{\"n\":1}");
        assert_eq!(parsed.channel, FIXTURE_CHANNEL);
        assert_eq!(parsed.envelope_type, ChannelScheme::Brenn);
        assert_eq!(parsed.message_id, message_id("delivery"));
    }

    #[test]
    fn the_scheme_follows_the_channel() {
        let text = envelope_on("ephemeral:page.total", "d", "{}");
        let parsed: Value = serde_json::from_str(&text).expect("frame is JSON");
        assert_eq!(parsed["envelope_type"], "ephemeral");
        assert_eq!(parsed["channel"], "ephemeral:page.total");
    }

    #[test]
    #[should_panic(expected = "carries no scheme prefix")]
    fn a_channel_with_no_scheme_is_refused() {
        // The frame's `envelope_type` is the channel's scheme; a channel that
        // names none leaves nothing to derive it from, and a fixture built on a
        // default would carry a type no host produces.
        envelope_on("nope/total", "d", "{}");
    }

    #[test]
    fn one_label_is_one_id() {
        assert_eq!(message_id("m0"), message_id("m0"));
        assert_ne!(message_id("m0"), message_id("m1"));
    }
}
