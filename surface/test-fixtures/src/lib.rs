//! The canonical delivered-envelope fixture shared by the surface test suites.
//!
//! Chrome, component-support, the in-tree components and the kernel all parse
//! the same delivered `MessageEnvelope` in their tests. Keeping one copy here
//! means a struct/serde change breaks exactly one place, loudly, instead of
//! silently rotting a hand-copied literal. The envelope is kept as JSON text on
//! purpose: the tests exercise the parse boundary, so they read the same bytes a
//! delivery would carry.

use brenn_envelope::MessageEnvelope;

/// Re-exported so consumers can name `Uuid` parameter types (page-load epochs,
/// message ids) without pinning `uuid` themselves.
pub use uuid::Uuid;

/// The canonical minimal ephemeral `MessageEnvelope` as JSON text, with `body`
/// substituted and `publish_ts` chosen. Everything else is fixed so a fixture
/// reads the same in every suite. Suites that must control staleness (e.g. ack
/// pruning judged on the publish timestamp) vary `publish_ts`.
pub fn sample_envelope_json_at(body: &str, publish_ts: &str) -> String {
    serde_json::json!({
        "message_id": "00000000-0000-0000-0000-000000000001",
        "source": "src",
        "channel": "ephemeral:demo",
        "sender": "surface:deskbar",
        "publish_ts": publish_ts,
        "body": body,
        "urgency": "normal",
        "envelope_type": "ephemeral",
    })
    .to_string()
}

/// The canonical minimal ephemeral `MessageEnvelope` as JSON text, with `body`
/// substituted and a fixed `publish_ts`.
pub fn sample_envelope_json(body: &str) -> String {
    sample_envelope_json_at(body, "2023-11-14T22:13:20Z")
}

/// The canonical ephemeral envelope, parsed. Panics if the literal no longer
/// deserializes — a fixture is a test dependency, so a break here should fail
/// loud.
pub fn sample_envelope(body: &str) -> MessageEnvelope {
    serde_json::from_str(&sample_envelope_json(body))
        .expect("surface test fixture: sample envelope JSON deserializes")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_envelope_json_deserializes() {
        let envelope = sample_envelope("hello");
        assert_eq!(envelope.body, "hello");
        assert_eq!(
            envelope.envelope_type,
            brenn_envelope::ChannelScheme::Ephemeral
        );
    }
}
