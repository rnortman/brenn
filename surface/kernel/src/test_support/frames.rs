//! Server frames as a scripted peer writes them.
//!
//! The opening of an attachment is a fixed four-frame ladder — `Hello`,
//! `Welcome`, the config channel's `SubscribeResult`, the document's one-row
//! `Deliver` —
//! and every suite that drives a runner against a socket has to write all four
//! before anything it actually cares about can happen. They live here so the
//! browser suite and the native one script the same peer, and so a frame that
//! gains a field lands in one place.

use brenn_attach_client::conn::AttachmentFacts;
use brenn_attach_proto::{DeliverRow, SUPPORTED_VERSIONS, ServerFrame, SubscribeOutcome};
use brenn_envelope::{ChannelScheme, MessageEnvelope, Urgency};
use uuid::Uuid;

/// The peer's opening `Hello`, stating this build's whole supported range — a
/// peer that speaks what we speak, which is what every test but a negotiation
/// one wants.
pub(crate) fn server_hello() -> String {
    frame(&ServerFrame::Hello {
        versions: SUPPORTED_VERSIONS,
        ident: "peer".to_string(),
    })
}

/// The `Welcome` stating `facts`. Every field of the frame is one of the
/// attachment facts, so the two cannot drift.
pub(crate) fn welcome(facts: AttachmentFacts) -> String {
    frame(&ServerFrame::Welcome {
        version: facts.version,
        participant_id: facts.participant_id,
        session_id: facts.session_id,
        heartbeat_secs: facts.heartbeat_secs,
        max_body_bytes: facts.max_body_bytes,
        max_frame_bytes: facts.max_frame_bytes,
        alert_granted: facts.alert_granted,
    })
}

/// An accepted subscribe, replaying `replay_count` messages behind it.
pub(crate) fn subscribe_result(channel: &str, replay_count: u32) -> String {
    frame(&ServerFrame::SubscribeResult {
        channel: channel.to_string(),
        outcome: SubscribeOutcome::Ok,
        replay_count,
        gap: None,
    })
}

/// One row of a delivery pass on `channel`, under a stated position and
/// identity: a store dedups by message id and a span refuses a `seq` it has
/// already seen, so two rows on one channel must differ in both. The cursor is
/// `c{seq}`, which is what a suite asserting a resume claim reads back.
pub(crate) fn row(channel: &str, body: &str, seq: u64, id: u128) -> DeliverRow {
    DeliverRow {
        envelope: MessageEnvelope {
            message_id: Uuid::from_u128(id),
            source: "test".into(),
            channel: channel.into(),
            sender: "system:surface-config".into(),
            publish_ts: chrono::DateTime::from_timestamp(0, 0).expect("a representable instant"),
            body: body.into(),
            reply_to: None,
            delivery_deadline: None,
            deliver_after: None,
            impetus: None,
            urgency: Urgency::Normal,
            envelope_type: ChannelScheme::Ephemeral,
        },
        seq,
        cursor: serde_json::from_value(serde_json::Value::String(format!("c{seq}")))
            .expect("a cursor is a JSON string"),
        dropped: 0,
    }
}

/// A whole delivery pass on `channel`, one row per `(body, seq, id)`, as the
/// peer writes it — one frame, however many rows.
pub(crate) fn deliver_pass(channel: &str, rows: &[(&str, u64, u128)]) -> String {
    frame(&ServerFrame::Deliver {
        channel: channel.to_string(),
        rows: rows
            .iter()
            .map(|&(body, seq, id)| row(channel, body, seq, id))
            .collect(),
    })
}

/// A one-row pass at a stated position and identity.
pub(crate) fn deliver_at(channel: &str, body: &str, seq: u64, id: u128) -> String {
    deliver_pass(channel, &[(body, seq, id)])
}

/// A one-row pass at the first position, for a channel a test delivers on once.
pub(crate) fn deliver(channel: &str, body: &str) -> String {
    deliver_at(channel, body, 1, 0x9001)
}

/// Serialize a server frame the way a peer writes it.
pub(crate) fn frame(frame: &ServerFrame) -> String {
    serde_json::to_string(frame).expect("a server frame serializes")
}
