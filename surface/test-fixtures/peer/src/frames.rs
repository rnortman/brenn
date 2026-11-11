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
use brenn_attach_proto::{
    ClientFrame, Codec, DeliverRow, ServerFrame, SubscribeOutcome, VersionRange, WireMessage,
};
use brenn_envelope::{ChannelScheme, MessageEnvelope, Urgency};
use brenn_surface_schema::SURFACE_ATTACH_VERSION;
use uuid::Uuid;

/// The peer's opening `Hello`, stating the surface route's range — a peer that
/// speaks what the page speaks, which is what every test but a negotiation one
/// wants.
pub fn server_hello() -> String {
    frame(&ServerFrame::Hello {
        versions: VersionRange::exactly(SURFACE_ATTACH_VERSION),
        ident: "peer".to_string(),
    })
}

/// The `Welcome` stating `facts`. Every field of the frame is one of the
/// attachment facts, so the two cannot drift.
pub fn welcome(facts: AttachmentFacts) -> String {
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
pub fn subscribe_result(channel: &str, replay_count: u32) -> String {
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
pub fn row(channel: &str, body: &str, seq: u64, id: u128) -> DeliverRow {
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
pub fn deliver_pass(channel: &str, rows: &[(&str, u64, u128)]) -> String {
    frame(&ServerFrame::Deliver {
        channel: channel.to_string(),
        rows: rows
            .iter()
            .map(|&(body, seq, id)| row(channel, body, seq, id))
            .collect(),
    })
}

/// A one-row pass at a stated position and identity.
pub fn deliver_at(channel: &str, body: &str, seq: u64, id: u128) -> String {
    deliver_pass(channel, &[(body, seq, id)])
}

/// A one-row pass at the first position, for a channel a test delivers on once.
pub fn deliver(channel: &str, body: &str) -> String {
    deliver_at(channel, body, 1, 0x9001)
}

/// The codec of the version the surface route serves.
fn surface_codec() -> Codec {
    Codec::negotiated(SURFACE_ATTACH_VERSION)
}

/// Serialize a server frame the way a peer writes it: a text message at the
/// surface route's version.
pub fn frame(frame: &ServerFrame) -> String {
    match surface_codec().encode_server(frame) {
        WireMessage::Text(text) => text,
        WireMessage::Binary(_) => panic!("a surface peer frame is a text message: {frame:?}"),
    }
}

/// Serialize a client frame the way the page writes it, at the surface
/// route's version.
pub fn client_frame(frame: &ClientFrame) -> String {
    match surface_codec().encode_client(frame) {
        WireMessage::Text(text) => text,
        WireMessage::Binary(_) => panic!("a page frame is a text message: {frame:?}"),
    }
}

/// Read a server frame a peer wrote at the surface route's version.
pub fn read_server_frame(text: &str) -> ServerFrame {
    surface_codec()
        .decode_server(WireMessage::Text(text.to_string()))
        .unwrap_or_else(|err| panic!("not a server frame: {err}"))
}

/// Read a client frame the page wrote at the surface route's version.
pub fn read_client_frame(text: &str) -> ClientFrame {
    surface_codec()
        .decode_client(WireMessage::Text(text.to_string()))
        .unwrap_or_else(|err| panic!("not a client frame: {err}"))
}
