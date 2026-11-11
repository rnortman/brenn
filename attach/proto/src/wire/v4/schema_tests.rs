//! v4's schema at the value level: every frame kind pinned as parsed JSON and
//! read back, and the strictness every v4 frame is read with. The byte-exact
//! pins are in `fixtures`.

use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::wire::{Codec, DecodeError, WireMessage};
use crate::*;
use brenn_envelope::ChannelScheme;

fn v4() -> Codec {
    Codec::negotiated(4)
}

fn decode_client(text: &str) -> Result<ClientFrame, DecodeError> {
    v4().decode_client(WireMessage::Text(text.to_string()))
}

fn decode_server(text: &str) -> Result<ServerFrame, DecodeError> {
    v4().decode_server(WireMessage::Text(text.to_string()))
}

fn sample_cursor() -> Cursor {
    serde_json::from_value(json!("opaque-token-7")).unwrap()
}

fn sample_envelope() -> MessageEnvelope {
    MessageEnvelope {
        message_id: Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap(),
        source: "src".to_string(),
        channel: "ephemeral:demo".to_string(),
        sender: "surface:deskbar".to_string(),
        publish_ts: DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap(),
        body: "hello".into(),
        reply_to: None,
        delivery_deadline: None,
        deliver_after: None,
        impetus: None,
        urgency: Urgency::Normal,
        envelope_type: ChannelScheme::Ephemeral,
    }
}

/// Golden-plus-round-trip: a frame cannot be pinned without also proving it
/// survives the trip.
fn pin_client(frame: &ClientFrame, golden: Value) {
    let WireMessage::Text(text) = v4().encode_client(frame) else {
        panic!("a v4 frame is text");
    };
    assert_eq!(serde_json::from_str::<Value>(&text).unwrap(), golden);
    assert_eq!(&decode_client(&golden.to_string()).unwrap(), frame);
}

fn pin_server(frame: &ServerFrame, golden: Value) {
    let WireMessage::Text(text) = v4().encode_server(frame) else {
        panic!("a v4 frame is text");
    };
    assert_eq!(serde_json::from_str::<Value>(&text).unwrap(), golden);
    assert_eq!(&decode_server(&golden.to_string()).unwrap(), frame);
}

#[test]
fn client_hello_is_pinned() {
    pin_client(
        &ClientFrame::Hello {
            versions: VersionRange { min: 1, max: 1 },
            ident: "attach-client/0.1.0".to_string(),
        },
        json!({
            "type": "Hello",
            "versions": {"min": 1, "max": 1},
            "ident": "attach-client/0.1.0",
        }),
    );
}

#[test]
fn client_subscribe_is_pinned() {
    pin_client(
        &ClientFrame::Subscribe {
            channel: "ephemeral:demo".to_string(),
            push_depth: 5,
            retain_depth: 20,
            resume: Some(sample_cursor()),
        },
        json!({
            "type": "Subscribe",
            "channel": "ephemeral:demo",
            "push_depth": 5,
            "retain_depth": 20,
            "resume": "opaque-token-7",
        }),
    );
}

/// A first subscribe omits `resume` rather than sending null: absent is the
/// claim "I hold no position", and a client should not have to distinguish it
/// from an explicit null.
#[test]
fn client_subscribe_omits_an_absent_resume() {
    pin_client(
        &ClientFrame::Subscribe {
            channel: "brenn:orders".to_string(),
            push_depth: 0,
            retain_depth: 1,
            resume: None,
        },
        json!({
            "type": "Subscribe",
            "channel": "brenn:orders",
            "push_depth": 0,
            "retain_depth": 1,
        }),
    );
}

#[test]
fn client_unsubscribe_is_pinned() {
    pin_client(
        &ClientFrame::Unsubscribe {
            channel: "ephemeral:demo".to_string(),
        },
        json!({"type": "Unsubscribe", "channel": "ephemeral:demo"}),
    );
}

#[test]
fn client_publish_is_pinned() {
    pin_client(
        &ClientFrame::Publish {
            channel: "brenn:orders".to_string(),
            attribution: Some("ticker".to_string()),
            body: "{}".into(),
            urgency: Urgency::High,
            correlation: Some(9),
        },
        json!({
            "type": "Publish",
            "channel": "brenn:orders",
            "attribution": "ticker",
            "body": "{}",
            "urgency": "high",
            "correlation": 9,
        }),
    );
}

/// The attacher's own identity is the absence of an attribution, not a
/// reserved string: nothing a client could spell may name a principal.
#[test]
fn client_publish_omits_an_absent_attribution() {
    pin_client(
        &ClientFrame::Publish {
            channel: "ephemeral:status".to_string(),
            attribution: None,
            body: "{}".into(),
            urgency: Urgency::Normal,
            correlation: None,
        },
        json!({
            "type": "Publish",
            "channel": "ephemeral:status",
            "body": "{}",
            "urgency": "normal",
        }),
    );
}

#[test]
fn client_publish_batch_is_pinned() {
    pin_client(
        &ClientFrame::PublishBatch {
            attribution: Some("ticker".to_string()),
            correlation: 4,
            publishes: vec![
                BatchEntry {
                    channel: "brenn:orders".to_string(),
                    body: "now".into(),
                    urgency: Urgency::Normal,
                    deliver_after: None,
                },
                BatchEntry {
                    channel: "brenn:orders".to_string(),
                    body: "later".into(),
                    urgency: Urgency::Low,
                    deliver_after: Some(1_700_000_000_000),
                },
            ],
            deferred_ops: vec![
                BatchDeferredOp {
                    channel: "brenn:orders".to_string(),
                    message_id: Uuid::parse_str("00000000-0000-0000-0000-00000000000a").unwrap(),
                    op: DeferredOpKind::Cancel,
                },
                BatchDeferredOp {
                    channel: "brenn:orders".to_string(),
                    message_id: Uuid::parse_str("00000000-0000-0000-0000-00000000000b").unwrap(),
                    op: DeferredOpKind::Edit {
                        body: Some("revised".into()),
                        deliver_after: None,
                    },
                },
            ],
        },
        json!({
            "type": "PublishBatch",
            "attribution": "ticker",
            "correlation": 4,
            "publishes": [
                {"channel": "brenn:orders", "body": "now", "urgency": "normal"},
                {
                    "channel": "brenn:orders",
                    "body": "later",
                    "urgency": "low",
                    "deliver_after": 1_700_000_000_000u64,
                },
            ],
            "deferred_ops": [
                {
                    "channel": "brenn:orders",
                    "message_id": "00000000-0000-0000-0000-00000000000a",
                    "op": {"kind": "cancel"},
                },
                {
                    "channel": "brenn:orders",
                    "message_id": "00000000-0000-0000-0000-00000000000b",
                    "op": {"kind": "edit", "body": "revised"},
                },
            ],
        }),
    );
}

/// An ops-only flush is a whole batch, and a publishes-only flush omits the
/// ops list entirely.
#[test]
fn client_publish_batch_omits_an_empty_op_list() {
    pin_client(
        &ClientFrame::PublishBatch {
            attribution: None,
            correlation: 1,
            publishes: vec![BatchEntry {
                channel: "ephemeral:demo".to_string(),
                body: "x".into(),
                urgency: Urgency::VeryLow,
                deliver_after: None,
            }],
            deferred_ops: vec![],
        },
        json!({
            "type": "PublishBatch",
            "correlation": 1,
            "publishes": [
                {"channel": "ephemeral:demo", "body": "x", "urgency": "very-low"},
            ],
        }),
    );
}

#[test]
fn client_alert_is_pinned() {
    pin_client(
        &ClientFrame::Alert {
            attribution: None,
            severity: AlertSeverity::Critical,
            title: "chrome died".to_string(),
            body: "the mount plan failed".to_string(),
        },
        json!({
            "type": "Alert",
            "severity": "critical",
            "title": "chrome died",
            "body": "the mount plan failed",
        }),
    );
}

#[test]
fn an_attributed_alert_carries_its_sub_identity() {
    pin_client(
        &ClientFrame::Alert {
            attribution: Some("meeting".to_string()),
            severity: AlertSeverity::Warning,
            title: "camera unreachable".to_string(),
            body: "no frames for 30s".to_string(),
        },
        json!({
            "type": "Alert",
            "attribution": "meeting",
            "severity": "warning",
            "title": "camera unreachable",
            "body": "no frames for 30s",
        }),
    );
}

#[test]
fn server_hello_is_pinned() {
    pin_server(
        &ServerFrame::Hello {
            versions: VersionRange { min: 1, max: 1 },
            ident: "brenn-server/0.15.0".to_string(),
        },
        json!({
            "type": "Hello",
            "versions": {"min": 1, "max": 1},
            "ident": "brenn-server/0.15.0",
        }),
    );
}

/// The whole `Welcome`, field by field: this is the transport contract, and
/// every field on it is a fact of the attachment rather than application
/// config. A field that is not such a fact does not belong here.
#[test]
fn server_welcome_is_pinned() {
    pin_server(
        &ServerFrame::Welcome {
            version: 1,
            participant_id: "surface:deskbar".to_string(),
            session_id: "sess-1".to_string(),
            heartbeat_secs: 20,
            max_body_bytes: 65_536,
            max_frame_bytes: 532_480,
            alert_granted: true,
        },
        json!({
            "type": "Welcome",
            "version": 1,
            "participant_id": "surface:deskbar",
            "session_id": "sess-1",
            "heartbeat_secs": 20,
            "max_body_bytes": 65_536,
            "max_frame_bytes": 532_480,
            "alert_granted": true,
        }),
    );
}

#[test]
fn server_heartbeat_is_pinned() {
    pin_server(&ServerFrame::Heartbeat, json!({"type": "Heartbeat"}));
}

#[test]
fn server_subscribe_result_is_pinned() {
    pin_server(
        &ServerFrame::SubscribeResult {
            channel: "ephemeral:demo".to_string(),
            outcome: SubscribeOutcome::Ok,
            replay_count: 3,
            gap: Some(GapInfo {
                reason: GapReason::BeyondRetained,
            }),
        },
        json!({
            "type": "SubscribeResult",
            "channel": "ephemeral:demo",
            "outcome": {"kind": "Ok"},
            "replay_count": 3,
            "gap": {"reason": {"kind": "BeyondRetained"}},
        }),
    );
    pin_server(
        &ServerFrame::SubscribeResult {
            channel: "ephemeral:demo".to_string(),
            outcome: SubscribeOutcome::Ok,
            replay_count: 0,
            gap: None,
        },
        json!({
            "type": "SubscribeResult",
            "channel": "ephemeral:demo",
            "outcome": {"kind": "Ok"},
            "replay_count": 0,
        }),
    );
    pin_server(
        &ServerFrame::SubscribeResult {
            channel: "brenn:chat.app.home.out.42".to_string(),
            outcome: SubscribeOutcome::Unavailable,
            replay_count: 0,
            gap: None,
        },
        json!({
            "type": "SubscribeResult",
            "channel": "brenn:chat.app.home.out.42",
            "outcome": {"kind": "Unavailable"},
            "replay_count": 0,
        }),
    );
}

/// Per-channel, single-target: the position facts sit on each row, not on a
/// list of targets, because the wire carries a message once per attachment
/// and fan-out is the attacher's own business.
#[test]
fn server_deliver_is_pinned() {
    pin_server(
        &ServerFrame::Deliver {
            channel: "ephemeral:demo".to_string(),
            rows: vec![DeliverRow {
                envelope: sample_envelope(),
                seq: 12,
                cursor: sample_cursor(),
                dropped: 2,
            }],
        },
        json!({
            "type": "Deliver",
            "channel": "ephemeral:demo",
            "rows": [{
                "envelope": {
                    "message_id": "00000000-0000-0000-0000-000000000001",
                    "source": "src",
                    "channel": "ephemeral:demo",
                    "sender": "surface:deskbar",
                    "publish_ts": "2023-11-14T22:13:20Z",
                    "body": "hello",
                    "urgency": "normal",
                    "envelope_type": "ephemeral",
                },
                "seq": 12,
                "cursor": "opaque-token-7",
                "dropped": 2,
            }],
        }),
    );
}

/// A whole catch-up pass in one frame: the rows are oldest first, their
/// `seq`s ascend across the pass, and the loss rides only the row that
/// follows it.
#[test]
fn a_multi_row_deliver_pass_is_pinned() {
    pin_server(
        &ServerFrame::Deliver {
            channel: "ephemeral:demo".to_string(),
            rows: vec![
                DeliverRow {
                    envelope: sample_envelope(),
                    seq: 1,
                    cursor: sample_cursor(),
                    dropped: 3,
                },
                DeliverRow {
                    envelope: sample_envelope(),
                    seq: 2,
                    cursor: sample_cursor(),
                    dropped: 0,
                },
            ],
        },
        json!({
            "type": "Deliver",
            "channel": "ephemeral:demo",
            "rows": [
                {
                    "envelope": {
                        "message_id": "00000000-0000-0000-0000-000000000001",
                        "source": "src",
                        "channel": "ephemeral:demo",
                        "sender": "surface:deskbar",
                        "publish_ts": "2023-11-14T22:13:20Z",
                        "body": "hello",
                        "urgency": "normal",
                        "envelope_type": "ephemeral",
                    },
                    "seq": 1,
                    "cursor": "opaque-token-7",
                    "dropped": 3,
                },
                {
                    "envelope": {
                        "message_id": "00000000-0000-0000-0000-000000000001",
                        "source": "src",
                        "channel": "ephemeral:demo",
                        "sender": "surface:deskbar",
                        "publish_ts": "2023-11-14T22:13:20Z",
                        "body": "hello",
                        "urgency": "normal",
                        "envelope_type": "ephemeral",
                    },
                    "seq": 2,
                    "cursor": "opaque-token-7",
                    "dropped": 0,
                },
            ],
        }),
    );
}

/// A row is as strict as a frame: an unknown key inside one is a protocol
/// violation, not something to skip past.
#[test]
fn an_unknown_key_in_a_deliver_row_is_refused() {
    let json = json!({
        "type": "Deliver",
        "channel": "ephemeral:demo",
        "rows": [{
            "envelope": {
                "message_id": "00000000-0000-0000-0000-000000000001",
                "source": "src",
                "channel": "ephemeral:demo",
                "sender": "surface:deskbar",
                "publish_ts": "2023-11-14T22:13:20Z",
                "body": "hello",
                "urgency": "normal",
                "envelope_type": "ephemeral",
            },
            "seq": 12,
            "cursor": "opaque-token-7",
            "dropped": 0,
            "targets": [],
        }],
    });
    assert!(decode_server(&json.to_string()).is_err());
}

#[test]
fn server_publish_result_is_pinned() {
    pin_server(
        &ServerFrame::PublishResult {
            correlation: Some(9),
            outcome: PublishOutcome::Ok,
        },
        json!({
            "type": "PublishResult",
            "correlation": 9,
            "outcome": {"kind": "Ok"},
        }),
    );
    pin_server(
        &ServerFrame::PublishResult {
            correlation: None,
            outcome: PublishOutcome::BodyTooLarge { len: 100, max: 64 },
        },
        json!({
            "type": "PublishResult",
            "outcome": {"kind": "BodyTooLarge", "len": 100, "max": 64},
        }),
    );
}

#[test]
fn server_publish_batch_result_is_pinned() {
    pin_server(
        &ServerFrame::PublishBatchResult {
            correlation: 4,
            outcome: PublishBatchOutcome::RateLimited,
        },
        json!({
            "type": "PublishBatchResult",
            "correlation": 4,
            "outcome": {"kind": "RateLimited"},
        }),
    );
}

#[test]
fn server_deferred_view_is_pinned() {
    pin_server(
        &ServerFrame::DeferredView {
            channel: "brenn:orders".to_string(),
            attribution: Some("ticker".to_string()),
            entries: vec![DeferredViewEntry {
                message_id: Uuid::parse_str("00000000-0000-0000-0000-00000000000a").unwrap(),
                body: "parked".into(),
                deliver_after: 1_700_000_000_000,
            }],
        },
        json!({
            "type": "DeferredView",
            "channel": "brenn:orders",
            "attribution": "ticker",
            "entries": [{
                "message_id": "00000000-0000-0000-0000-00000000000a",
                "body": "parked",
                "deliver_after": 1_700_000_000_000u64,
            }],
        }),
    );
}

/// An empty view is the "nothing parked" snapshot, and it is a frame the
/// server does send — a set that emptied must be told, or the client's mirror
/// keeps showing a schedule that is gone.
#[test]
fn server_deferred_view_carries_an_empty_set() {
    pin_server(
        &ServerFrame::DeferredView {
            channel: "brenn:orders".to_string(),
            attribution: None,
            entries: vec![],
        },
        json!({
            "type": "DeferredView",
            "channel": "brenn:orders",
            "entries": [],
        }),
    );
}

/// An unknown field inside a known variant does not parse. Negotiation has
/// already settled which schema is in force, so an extra field is a
/// non-conforming peer, not a newer one.
#[test]
fn an_unknown_field_in_a_known_variant_is_rejected() {
    let err = decode_client(r#"{"type":"Unsubscribe","channel":"ephemeral:demo","extra":1}"#)
        .unwrap_err()
        .to_string();
    assert!(err.contains("unknown field"), "{err}");

    let err = decode_server(
        r#"{"type":"SubscribeResult","channel":"ephemeral:demo",
            "outcome":{"kind":"Ok"},"replay_count":0,"instance":"ticker"}"#,
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("unknown field"), "{err}");
}

/// The boundary of that strictness, pinned so it is a known shape rather than
/// a surprise: a variant with **no fields** has nothing an extra key could be
/// a misspelling of, and serde's internally-tagged representation carries no
/// field list to check trailing keys against. Junk beside such a tag parses.
/// Every variant that has fields is strict, which is the whole of the
/// misnamed-field class this crate is defending against.
#[test]
fn a_fieldless_variant_tolerates_trailing_keys() {
    assert_eq!(
        decode_server(r#"{"type":"Heartbeat","tick":1}"#).unwrap(),
        ServerFrame::Heartbeat
    );
}

/// Strictness reaches nested payloads too, not just the frame's own fields.
#[test]
fn an_unknown_field_in_a_nested_payload_is_rejected() {
    let err = decode_client(
        r#"{"type":"PublishBatch","correlation":1,"publishes":[
             {"channel":"ephemeral:demo","body":"x","urgency":"normal","port":"out"}]}"#,
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("unknown field"), "{err}");
}

#[test]
fn an_unknown_frame_type_is_rejected() {
    for raw in [
        r#"{"type":"Geometry","width":800,"height":600}"#,
        r#"{"type":"Status"}"#,
    ] {
        let err = decode_client(raw).unwrap_err().to_string();
        assert!(err.contains("unknown variant"), "{raw}: {err}");
    }
    let err = decode_server(r#"{"type":"Bindings"}"#)
        .unwrap_err()
        .to_string();
    assert!(err.contains("unknown variant"), "{err}");
}

#[test]
fn an_unknown_inner_kind_is_rejected() {
    let err = decode_server(r#"{"type":"PublishResult","outcome":{"kind":"Nope"}}"#)
        .unwrap_err()
        .to_string();
    assert!(err.contains("unknown variant"), "{err}");
}

/// A missing required field is rejected, so the absent-means-default rule is
/// confined to the fields that declare it.
#[test]
fn a_missing_required_field_is_rejected() {
    let err = decode_client(r#"{"type":"Subscribe","channel":"ephemeral:demo","push_depth":1}"#)
        .unwrap_err()
        .to_string();
    assert!(err.contains("retain_depth"), "{err}");
}

/// Neither `TextOnlyChannel` outcome exists in v4: a v4 peer cannot send the
/// body that earns one, so reading one is reading a non-conforming peer.
#[test]
fn a_text_only_channel_outcome_is_not_a_v4_frame() {
    for raw in [
        r#"{"type":"PublishResult","outcome":{"kind":"TextOnlyChannel"}}"#,
        r#"{"type":"PublishBatchResult","correlation":1,"outcome":{"kind":"TextOnlyChannel"}}"#,
    ] {
        let err = decode_server(raw).unwrap_err();
        assert_eq!(err.rule(), "unparseable server frame", "{raw}");
        assert!(err.to_string().contains("unknown variant"), "{err}");
    }
}

#[test]
#[should_panic(expected = "cannot write a TextOnlyChannel publish outcome")]
fn writing_a_text_only_channel_outcome_under_v4_panics() {
    v4().encode_server(&ServerFrame::PublishResult {
        correlation: None,
        outcome: PublishOutcome::TextOnlyChannel,
    });
}

#[test]
#[should_panic(expected = "cannot write a TextOnlyChannel batch outcome")]
fn writing_a_text_only_channel_batch_outcome_under_v4_panics() {
    v4().encode_server(&ServerFrame::PublishBatchResult {
        correlation: 1,
        outcome: PublishBatchOutcome::TextOnlyChannel,
    });
}

fn typed_text() -> MessageBody {
    MessageBody::from_parts(Some("text/markdown"), b"# hi").unwrap()
}

fn non_text() -> MessageBody {
    MessageBody::from_parts(Some("image/jpeg"), &[0xff, 0xd8, 0xff]).unwrap()
}

/// A client can state no content type under v4, so only untyped text is
/// carried in its direction.
#[test]
fn v4_carries_untyped_text_only() {
    assert!(v4().carries(&"plain".into()));
    assert!(v4().carries(&"".into()));
    assert!(!v4().carries(&typed_text()));
    assert!(!v4().carries(&non_text()));
}

fn publish(body: MessageBody) -> ClientFrame {
    ClientFrame::Publish {
        channel: "ephemeral:demo".to_string(),
        attribution: None,
        body,
        urgency: Urgency::Normal,
        correlation: None,
    }
}

#[test]
#[should_panic(
    expected = "attach v4 cannot carry a client body that is not untyped text (text/markdown, 4 bytes)"
)]
fn a_typed_client_body_panics_at_the_v4_encoder() {
    v4().encode_client(&publish(typed_text()));
}

#[test]
#[should_panic(
    expected = "attach v4 cannot carry a client body that is not untyped text (image/jpeg, 3 bytes)"
)]
fn a_non_text_batch_edit_panics_at_the_v4_encoder() {
    v4().encode_client(&ClientFrame::PublishBatch {
        attribution: None,
        correlation: 1,
        publishes: vec![],
        deferred_ops: vec![BatchDeferredOp {
            channel: "brenn:orders".to_string(),
            message_id: Uuid::nil(),
            op: DeferredOpKind::Edit {
                body: Some(non_text()),
                deliver_after: None,
            },
        }],
    });
}

/// A server writes a typed text body as its text: the type is what a v4 peer
/// does not get.
#[test]
fn a_typed_view_entry_is_written_as_its_text() {
    let frame = ServerFrame::DeferredView {
        channel: "brenn:orders".to_string(),
        attribution: None,
        entries: vec![DeferredViewEntry {
            message_id: Uuid::nil(),
            body: typed_text(),
            deliver_after: 1,
        }],
    };
    assert_eq!(
        v4().encode_server(&frame),
        WireMessage::Text(
            r##"{"type":"DeferredView","channel":"brenn:orders","entries":[{"message_id":"00000000-0000-0000-0000-000000000000","body":"# hi","deliver_after":1}]}"##
                .to_string()
        )
    );
}

#[test]
#[should_panic(
    expected = "attach v4 DeferredView cannot carry a non-text body (image/jpeg, 3 bytes)"
)]
fn a_non_text_view_entry_panics_at_the_v4_encoder() {
    v4().encode_server(&ServerFrame::DeferredView {
        channel: "brenn:orders".to_string(),
        attribution: None,
        entries: vec![DeferredViewEntry {
            message_id: Uuid::nil(),
            body: non_text(),
            deliver_after: 1,
        }],
    });
}

#[test]
#[should_panic(expected = "attach v4 Deliver cannot carry a non-text body")]
fn a_non_text_row_panics_at_the_v4_encoder() {
    let mut envelope = sample_envelope();
    envelope.body = non_text();
    v4().encode_server(&ServerFrame::Deliver {
        channel: "ephemeral:demo".to_string(),
        rows: vec![DeliverRow {
            envelope,
            seq: 1,
            cursor: sample_cursor(),
            dropped: 0,
        }],
    });
}
