//! Byte-exact v4 fixtures: for every frame kind in both directions, the JSON
//! text a v4 frame is written as, literally, and the frame it reads back as.
//!
//! The value-level pins beside these compare parsed JSON, which leaves key
//! order and number spelling free; these leave nothing free, so a deployed v4
//! peer is shown the same bytes whatever the frame types become.

use crate::wire::{Codec, WireMessage};
use crate::*;
use brenn_envelope::{ChannelScheme, Impetus};
use chrono::{DateTime, Utc};

fn uuid(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

fn cursor(token: &str) -> Cursor {
    serde_json::from_value(serde_json::Value::String(token.to_string())).unwrap()
}

fn at(secs: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(secs, 0).unwrap()
}

fn bare_envelope() -> MessageEnvelope {
    MessageEnvelope {
        message_id: uuid(1),
        source: "src".to_string(),
        channel: "ephemeral:demo".to_string(),
        sender: "remote:alice".to_string(),
        publish_ts: at(1_700_000_000),
        body: "hello".into(),
        reply_to: None,
        delivery_deadline: None,
        deliver_after: None,
        impetus: None,
        urgency: Urgency::Normal,
        envelope_type: ChannelScheme::Ephemeral,
    }
}

/// Every optional field present, and a typed text body, which v4 writes as
/// its bare text.
fn full_envelope() -> MessageEnvelope {
    MessageEnvelope {
        message_id: uuid(2),
        source: "src".to_string(),
        channel: "brenn:orders".to_string(),
        sender: "remote:alice".to_string(),
        publish_ts: at(1_700_000_001),
        body: brenn_envelope::MessageBody::from_parts(Some("text/markdown"), b"# \"hi\"\n")
            .unwrap(),
        reply_to: Some("brenn:replies".to_string()),
        delivery_deadline: Some(at(1_700_000_100)),
        deliver_after: Some(at(1_700_000_050)),
        impetus: Some(Impetus::Replenish),
        urgency: Urgency::High,
        envelope_type: ChannelScheme::Brenn,
    }
}

/// Every client frame kind, in each optional field's present and absent form.
fn client_fixtures() -> Vec<(ClientFrame, &'static str)> {
    vec![
        (
            ClientFrame::Hello {
                versions: VersionRange { min: 4, max: 5 },
                ident: "attach-client/0.1.0".to_string(),
            },
            r#"{"type":"Hello","versions":{"min":4,"max":5},"ident":"attach-client/0.1.0"}"#,
        ),
        (
            ClientFrame::Subscribe {
                channel: "ephemeral:demo".to_string(),
                push_depth: 5,
                retain_depth: 20,
                resume: Some(cursor("opaque-token-7")),
            },
            r#"{"type":"Subscribe","channel":"ephemeral:demo","push_depth":5,"retain_depth":20,"resume":"opaque-token-7"}"#,
        ),
        (
            ClientFrame::Subscribe {
                channel: "brenn:orders".to_string(),
                push_depth: 0,
                retain_depth: 1,
                resume: None,
            },
            r#"{"type":"Subscribe","channel":"brenn:orders","push_depth":0,"retain_depth":1}"#,
        ),
        (
            ClientFrame::Unsubscribe {
                channel: "ephemeral:demo".to_string(),
            },
            r#"{"type":"Unsubscribe","channel":"ephemeral:demo"}"#,
        ),
        (
            ClientFrame::Publish {
                channel: "brenn:orders".to_string(),
                attribution: Some("ticker".to_string()),
                body: "{\"n\":1}\n\u{1}é".into(),
                urgency: Urgency::High,
                correlation: Some(9),
            },
            r#"{"type":"Publish","channel":"brenn:orders","attribution":"ticker","body":"{\"n\":1}\n\u0001é","urgency":"high","correlation":9}"#,
        ),
        (
            ClientFrame::Publish {
                channel: "ephemeral:status".to_string(),
                attribution: None,
                body: "".into(),
                urgency: Urgency::VeryLow,
                correlation: None,
            },
            r#"{"type":"Publish","channel":"ephemeral:status","body":"","urgency":"very-low"}"#,
        ),
        (
            ClientFrame::PublishBatch {
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
                        message_id: uuid(10),
                        op: DeferredOpKind::Cancel,
                    },
                    BatchDeferredOp {
                        channel: "brenn:orders".to_string(),
                        message_id: uuid(11),
                        op: DeferredOpKind::Edit {
                            body: Some("revised".into()),
                            deliver_after: Some(1_700_000_000_500),
                        },
                    },
                    BatchDeferredOp {
                        channel: "brenn:orders".to_string(),
                        message_id: uuid(12),
                        op: DeferredOpKind::Edit {
                            body: None,
                            deliver_after: None,
                        },
                    },
                ],
            },
            concat!(
                r#"{"type":"PublishBatch","attribution":"ticker","correlation":4,"publishes":["#,
                r#"{"channel":"brenn:orders","body":"now","urgency":"normal"},"#,
                r#"{"channel":"brenn:orders","body":"later","urgency":"low","deliver_after":1700000000000}],"#,
                r#""deferred_ops":["#,
                r#"{"channel":"brenn:orders","message_id":"00000000-0000-0000-0000-00000000000a","op":{"kind":"cancel"}},"#,
                r#"{"channel":"brenn:orders","message_id":"00000000-0000-0000-0000-00000000000b","op":{"kind":"edit","body":"revised","deliver_after":1700000000500}},"#,
                r#"{"channel":"brenn:orders","message_id":"00000000-0000-0000-0000-00000000000c","op":{"kind":"edit"}}]}"#,
            ),
        ),
        (
            ClientFrame::PublishBatch {
                attribution: None,
                correlation: 1,
                publishes: vec![],
                deferred_ops: vec![],
            },
            r#"{"type":"PublishBatch","correlation":1,"publishes":[]}"#,
        ),
        (
            ClientFrame::Alert {
                attribution: Some("meeting".to_string()),
                severity: AlertSeverity::Warning,
                title: "camera unreachable".to_string(),
                body: "no frames for 30s".into(),
            },
            r#"{"type":"Alert","attribution":"meeting","severity":"warning","title":"camera unreachable","body":"no frames for 30s"}"#,
        ),
        (
            ClientFrame::Alert {
                attribution: None,
                severity: AlertSeverity::Info,
                title: "t".to_string(),
                body: "b".into(),
            },
            r#"{"type":"Alert","severity":"info","title":"t","body":"b"}"#,
        ),
    ]
}

/// Every server frame kind and every outcome variant, in each optional
/// field's present and absent form.
fn server_fixtures() -> Vec<(ServerFrame, &'static str)> {
    vec![
        (
            ServerFrame::Hello {
                versions: VersionRange::exactly(4),
                ident: "brenn-server/0.15.0".to_string(),
            },
            r#"{"type":"Hello","versions":{"min":4,"max":4},"ident":"brenn-server/0.15.0"}"#,
        ),
        (
            ServerFrame::Welcome {
                version: 4,
                participant_id: "remote:alice".to_string(),
                session_id: "sess-1".to_string(),
                heartbeat_secs: 20,
                max_body_bytes: 65_536,
                max_frame_bytes: 532_480,
                alert_granted: false,
            },
            r#"{"type":"Welcome","version":4,"participant_id":"remote:alice","session_id":"sess-1","heartbeat_secs":20,"max_body_bytes":65536,"max_frame_bytes":532480,"alert_granted":false}"#,
        ),
        (ServerFrame::Heartbeat, r#"{"type":"Heartbeat"}"#),
        (
            ServerFrame::SubscribeResult {
                channel: "ephemeral:demo".to_string(),
                outcome: SubscribeOutcome::Ok,
                replay_count: 3,
                gap: Some(GapInfo {
                    reason: GapReason::BeyondRetained,
                }),
            },
            r#"{"type":"SubscribeResult","channel":"ephemeral:demo","outcome":{"kind":"Ok"},"replay_count":3,"gap":{"reason":{"kind":"BeyondRetained"}}}"#,
        ),
        (
            ServerFrame::SubscribeResult {
                channel: "brenn:orders".to_string(),
                outcome: SubscribeOutcome::Ok,
                replay_count: 0,
                gap: Some(GapInfo {
                    reason: GapReason::EpochChanged,
                }),
            },
            r#"{"type":"SubscribeResult","channel":"brenn:orders","outcome":{"kind":"Ok"},"replay_count":0,"gap":{"reason":{"kind":"EpochChanged"}}}"#,
        ),
        (
            ServerFrame::SubscribeResult {
                channel: "brenn:orders".to_string(),
                outcome: SubscribeOutcome::Unavailable,
                replay_count: 0,
                gap: None,
            },
            r#"{"type":"SubscribeResult","channel":"brenn:orders","outcome":{"kind":"Unavailable"},"replay_count":0}"#,
        ),
        (
            ServerFrame::Deliver {
                channel: "ephemeral:demo".to_string(),
                rows: vec![
                    DeliverRow {
                        envelope: bare_envelope(),
                        seq: 1,
                        cursor: cursor("c-1"),
                        dropped: 3,
                    },
                    DeliverRow {
                        envelope: full_envelope(),
                        seq: 2,
                        cursor: cursor("c-2"),
                        dropped: 0,
                    },
                ],
            },
            concat!(
                r#"{"type":"Deliver","channel":"ephemeral:demo","rows":["#,
                r#"{"envelope":{"message_id":"00000000-0000-0000-0000-000000000001","source":"src","#,
                r#""channel":"ephemeral:demo","sender":"remote:alice","publish_ts":"2023-11-14T22:13:20Z","#,
                r#""body":"hello","urgency":"normal","envelope_type":"ephemeral"},"#,
                r#""seq":1,"cursor":"c-1","dropped":3},"#,
                r#"{"envelope":{"message_id":"00000000-0000-0000-0000-000000000002","source":"src","#,
                r#""channel":"brenn:orders","sender":"remote:alice","publish_ts":"2023-11-14T22:13:21Z","#,
                r##""body":"# \"hi\"\n","reply_to":"brenn:replies","delivery_deadline":"2023-11-14T22:15:00Z","##,
                r#""deliver_after":"2023-11-14T22:14:10Z","impetus":"replenish","urgency":"high","#,
                r#""envelope_type":"brenn"},"#,
                r#""seq":2,"cursor":"c-2","dropped":0}]}"#,
            ),
        ),
        (
            ServerFrame::PublishResult {
                correlation: Some(9),
                outcome: PublishOutcome::Ok,
            },
            r#"{"type":"PublishResult","correlation":9,"outcome":{"kind":"Ok"}}"#,
        ),
        (
            ServerFrame::PublishResult {
                correlation: Some(10),
                outcome: PublishOutcome::RateLimited,
            },
            r#"{"type":"PublishResult","correlation":10,"outcome":{"kind":"RateLimited"}}"#,
        ),
        (
            ServerFrame::PublishResult {
                correlation: None,
                outcome: PublishOutcome::BodyTooLarge { len: 100, max: 64 },
            },
            r#"{"type":"PublishResult","outcome":{"kind":"BodyTooLarge","len":100,"max":64}}"#,
        ),
        (
            ServerFrame::PublishResult {
                correlation: Some(11),
                outcome: PublishOutcome::Failed,
            },
            r#"{"type":"PublishResult","correlation":11,"outcome":{"kind":"Failed"}}"#,
        ),
        (
            ServerFrame::PublishBatchResult {
                correlation: 4,
                outcome: PublishBatchOutcome::Ok,
            },
            r#"{"type":"PublishBatchResult","correlation":4,"outcome":{"kind":"Ok"}}"#,
        ),
        (
            ServerFrame::PublishBatchResult {
                correlation: 5,
                outcome: PublishBatchOutcome::RateLimited,
            },
            r#"{"type":"PublishBatchResult","correlation":5,"outcome":{"kind":"RateLimited"}}"#,
        ),
        (
            ServerFrame::DeferredView {
                channel: "brenn:orders".to_string(),
                attribution: Some("ticker".to_string()),
                entries: vec![
                    DeferredViewEntry {
                        message_id: uuid(10),
                        body: "parked".into(),
                        deliver_after: 1_700_000_000_000,
                    },
                    DeferredViewEntry {
                        message_id: uuid(11),
                        body: "\"also\"".into(),
                        deliver_after: 1_700_000_000_001,
                    },
                ],
            },
            concat!(
                r#"{"type":"DeferredView","channel":"brenn:orders","attribution":"ticker","entries":["#,
                r#"{"message_id":"00000000-0000-0000-0000-00000000000a","body":"parked","deliver_after":1700000000000},"#,
                r#"{"message_id":"00000000-0000-0000-0000-00000000000b","body":"\"also\"","deliver_after":1700000000001}]}"#,
            ),
        ),
        (
            ServerFrame::DeferredView {
                channel: "brenn:orders".to_string(),
                attribution: None,
                entries: vec![],
            },
            r#"{"type":"DeferredView","channel":"brenn:orders","entries":[]}"#,
        ),
    ]
}

/// The frame a v4 peer reads back from a fixture's text.
///
/// A delivered typed text body reads back untyped: v4 writes no content
/// type, so the type is the one thing a v4 row loses.
fn as_read_back(frame: &ServerFrame) -> ServerFrame {
    let mut frame = frame.clone();
    if let ServerFrame::Deliver { rows, .. } = &mut frame {
        for row in rows {
            let text = row.envelope.require_text_body("v4 fixture").to_string();
            row.envelope.body = brenn_envelope::MessageBody::text(text);
        }
    }
    frame
}

#[test]
fn every_client_frame_is_written_as_its_v4_text_byte_for_byte() {
    for (frame, text) in client_fixtures() {
        assert_eq!(
            Codec::negotiated(4).encode_client(&frame),
            WireMessage::Text(text.to_string()),
            "{frame:?}"
        );
        assert_eq!(
            Codec::negotiated(4)
                .decode_client(WireMessage::Text(text.to_string()))
                .unwrap(),
            frame,
            "{text}"
        );
    }
}

#[test]
fn every_server_frame_is_written_as_its_v4_text_byte_for_byte() {
    for (frame, text) in server_fixtures() {
        assert_eq!(
            Codec::negotiated(4).encode_server(&frame),
            WireMessage::Text(text.to_string()),
            "{frame:?}"
        );
        assert_eq!(
            Codec::negotiated(4)
                .decode_server(WireMessage::Text(text.to_string()))
                .unwrap(),
            as_read_back(&frame),
            "{text}"
        );
    }
}

/// The fixtures cover every frame kind; a kind added without a fixture fails
/// to compile here.
#[test]
fn the_fixtures_cover_every_frame_kind() {
    let mut client = [false; 6];
    for (frame, _) in client_fixtures() {
        client[match frame {
            ClientFrame::Hello { .. } => 0,
            ClientFrame::Subscribe { .. } => 1,
            ClientFrame::Unsubscribe { .. } => 2,
            ClientFrame::Publish { .. } => 3,
            ClientFrame::PublishBatch { .. } => 4,
            ClientFrame::Alert { .. } => 5,
        }] = true;
    }
    assert_eq!(client, [true; 6]);
    let mut server = [false; 8];
    for (frame, _) in server_fixtures() {
        server[match frame {
            ServerFrame::Hello { .. } => 0,
            ServerFrame::Welcome { .. } => 1,
            ServerFrame::Heartbeat => 2,
            ServerFrame::SubscribeResult { .. } => 3,
            ServerFrame::Deliver { .. } => 4,
            ServerFrame::PublishResult { .. } => 5,
            ServerFrame::PublishBatchResult { .. } => 6,
            ServerFrame::DeferredView { .. } => 7,
        }] = true;
    }
    assert_eq!(server, [true; 8]);
}
