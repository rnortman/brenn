//! v5: the binary layout byte for byte, round trips of every kind, each
//! malformation and the rule it is refused under, and the batch law against
//! the frame cap.

use brenn_envelope::{ChannelScheme, Impetus};
use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::escaped_len;
use crate::wire::{Codec, DecodeError, WireMessage};
use crate::*;

fn v5() -> Codec {
    Codec::negotiated(5)
}

fn body(content_type: Option<&str>, bytes: &[u8]) -> MessageBody {
    MessageBody::from_parts(content_type, bytes).unwrap()
}

fn jpeg() -> MessageBody {
    body(Some("image/jpeg"), &[0xff, 0xd8, 0xff, 0x00])
}

fn markdown() -> MessageBody {
    body(Some("text/markdown"), "# \"hi\"\n".as_bytes())
}

fn cursor(token: &str) -> Cursor {
    serde_json::from_value(serde_json::Value::String(token.to_string())).unwrap()
}

fn at(secs: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(secs, 0).unwrap()
}

fn bare_envelope() -> MessageEnvelope {
    MessageEnvelope {
        message_id: Uuid::from_u128(1),
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

fn full_envelope(body: MessageBody) -> MessageEnvelope {
    MessageEnvelope {
        message_id: Uuid::from_u128(2),
        source: "src".to_string(),
        channel: "brenn:frames".to_string(),
        sender: "remote:alice".to_string(),
        publish_ts: at(1_700_000_001),
        body,
        reply_to: Some("brenn:replies".to_string()),
        delivery_deadline: Some(at(1_700_000_100)),
        deliver_after: Some(at(1_700_000_050)),
        impetus: Some(Impetus::Replenish),
        urgency: Urgency::High,
        envelope_type: ChannelScheme::Brenn,
    }
}

/// A binary message from a literal header and a body section.
fn raw(header: &str, bodies: &[u8]) -> WireMessage {
    let mut out = u32::try_from(header.len()).unwrap().to_be_bytes().to_vec();
    out.extend_from_slice(header.as_bytes());
    out.extend_from_slice(bodies);
    WireMessage::Binary(out)
}

fn text(s: &str) -> WireMessage {
    WireMessage::Text(s.to_string())
}

fn client_rule(msg: WireMessage) -> &'static str {
    v5().decode_client(msg).unwrap_err().rule()
}

fn server_rule(msg: WireMessage) -> &'static str {
    v5().decode_server(msg).unwrap_err().rule()
}

#[test]
fn a_publish_is_written_byte_for_byte() {
    let frame = ClientFrame::Publish {
        channel: "brenn:frames".to_string(),
        attribution: Some("cam".to_string()),
        body: jpeg(),
        urgency: Urgency::High,
        correlation: Some(7),
    };
    let header = r#"{"type":"Publish","channel":"brenn:frames","attribution":"cam","body":{"len":4,"content_type":"image/jpeg"},"urgency":"high","correlation":7}"#;
    let mut expected = vec![0, 0, 0, 141];
    assert_eq!(header.len(), 141);
    expected.extend_from_slice(header.as_bytes());
    expected.extend_from_slice(&[0xff, 0xd8, 0xff, 0x00]);
    assert_eq!(
        v5().encode_client(&frame),
        WireMessage::Binary(expected.clone())
    );
    assert_eq!(
        v5().decode_client(WireMessage::Binary(expected)).unwrap(),
        frame
    );
}

#[test]
fn a_two_row_deliver_is_written_byte_for_byte() {
    let png = body(Some("image/png"), &[0x89, b'P', b'N', b'G']);
    let frame = ServerFrame::Deliver {
        channel: "ephemeral:demo".to_string(),
        rows: vec![
            DeliverRow {
                envelope: bare_envelope(),
                seq: 1,
                cursor: cursor("c-1"),
                dropped: 3,
            },
            DeliverRow {
                envelope: full_envelope(png),
                seq: 2,
                cursor: cursor("c-2"),
                dropped: 0,
            },
        ],
    };
    let header = concat!(
        r#"{"type":"Deliver","channel":"ephemeral:demo","rows":["#,
        r#"{"envelope":{"header":{"message_id":"00000000-0000-0000-0000-000000000001","source":"src","#,
        r#""channel":"ephemeral:demo","sender":"remote:alice","publish_ts":"2023-11-14T22:13:20Z","#,
        r#""urgency":"normal","envelope_type":"ephemeral"},"body":{"len":5}},"#,
        r#""seq":1,"cursor":"c-1","dropped":3},"#,
        r#"{"envelope":{"header":{"message_id":"00000000-0000-0000-0000-000000000002","source":"src","#,
        r#""channel":"brenn:frames","sender":"remote:alice","publish_ts":"2023-11-14T22:13:21Z","#,
        r#""reply_to":"brenn:replies","delivery_deadline":"2023-11-14T22:15:00Z","#,
        r#""deliver_after":"2023-11-14T22:14:10Z","impetus":"replenish","urgency":"high","#,
        r#""envelope_type":"brenn"},"body":{"len":4,"content_type":"image/png"}},"#,
        r#""seq":2,"cursor":"c-2","dropped":0}]}"#,
    );
    let mut expected = u32::try_from(header.len()).unwrap().to_be_bytes().to_vec();
    expected.extend_from_slice(header.as_bytes());
    expected.extend_from_slice(b"hello");
    expected.extend_from_slice(&[0x89, b'P', b'N', b'G']);
    assert_eq!(
        v5().encode_server(&frame),
        WireMessage::Binary(expected.clone())
    );
    assert_eq!(
        v5().decode_server(WireMessage::Binary(expected)).unwrap(),
        frame
    );
}

/// The kinds that carry no body are v4's text, with the outcomes' new kinds.
#[test]
fn the_text_kinds_are_text() {
    for (frame, expected) in [
        (
            ServerFrame::PublishResult {
                correlation: Some(3),
                outcome: PublishOutcome::TextOnlyChannel,
            },
            r#"{"type":"PublishResult","correlation":3,"outcome":{"kind":"TextOnlyChannel"}}"#,
        ),
        (
            ServerFrame::PublishBatchResult {
                correlation: 4,
                outcome: PublishBatchOutcome::TextOnlyChannel,
            },
            r#"{"type":"PublishBatchResult","correlation":4,"outcome":{"kind":"TextOnlyChannel"}}"#,
        ),
        (
            ServerFrame::PublishResult {
                correlation: None,
                outcome: PublishOutcome::BodyTooLarge { len: 9, max: 8 },
            },
            r#"{"type":"PublishResult","outcome":{"kind":"BodyTooLarge","len":9,"max":8}}"#,
        ),
        (ServerFrame::Heartbeat, r#"{"type":"Heartbeat"}"#),
        (
            ServerFrame::SubscribeResult {
                channel: "brenn:frames".to_string(),
                outcome: SubscribeOutcome::Ok,
                replay_count: 2,
                gap: Some(GapInfo {
                    reason: GapReason::BeyondRetained,
                }),
            },
            r#"{"type":"SubscribeResult","channel":"brenn:frames","outcome":{"kind":"Ok"},"replay_count":2,"gap":{"reason":{"kind":"BeyondRetained"}}}"#,
        ),
        (
            ServerFrame::Welcome {
                version: 5,
                participant_id: "remote:alice".to_string(),
                session_id: "s-1".to_string(),
                heartbeat_secs: 30,
                max_body_bytes: 65_536,
                max_frame_bytes: 204_800,
                alert_granted: true,
            },
            r#"{"type":"Welcome","version":5,"participant_id":"remote:alice","session_id":"s-1","heartbeat_secs":30,"max_body_bytes":65536,"max_frame_bytes":204800,"alert_granted":true}"#,
        ),
    ] {
        assert_eq!(v5().encode_server(&frame), text(expected), "{expected}");
        assert_eq!(v5().decode_server(text(expected)).unwrap(), frame);
    }
    for (frame, expected) in [
        (
            ClientFrame::Subscribe {
                channel: "brenn:frames".to_string(),
                push_depth: 5,
                retain_depth: 20,
                resume: Some(cursor("c-9")),
            },
            r#"{"type":"Subscribe","channel":"brenn:frames","push_depth":5,"retain_depth":20,"resume":"c-9"}"#,
        ),
        (
            ClientFrame::Unsubscribe {
                channel: "brenn:frames".to_string(),
            },
            r#"{"type":"Unsubscribe","channel":"brenn:frames"}"#,
        ),
        (
            ClientFrame::Alert {
                attribution: None,
                severity: AlertSeverity::Warning,
                title: "t".to_string(),
                body: "b".to_string(),
            },
            r#"{"type":"Alert","severity":"warning","title":"t","body":"b"}"#,
        ),
    ] {
        assert_eq!(v5().encode_client(&frame), text(expected), "{expected}");
        assert_eq!(v5().decode_client(text(expected)).unwrap(), frame);
    }
}

fn bodies() -> Vec<MessageBody> {
    vec![
        "plain text".into(),
        "".into(),
        markdown(),
        jpeg(),
        body(Some("application/octet-stream"), &[]),
        body(Some("application/octet-stream"), &[0x00, 0xc3, 0x28, 0xff]),
    ]
}

fn client_round_trip(frame: ClientFrame) {
    let msg = v5().encode_client(&frame);
    assert!(matches!(msg, WireMessage::Binary(_)), "{frame:?}");
    assert_eq!(v5().decode_client(msg).unwrap(), frame);
}

fn server_round_trip(frame: ServerFrame) {
    let msg = v5().encode_server(&frame);
    assert!(matches!(msg, WireMessage::Binary(_)), "{frame:?}");
    assert_eq!(v5().decode_server(msg).unwrap(), frame);
}

#[test]
fn every_body_round_trips_in_every_binary_kind() {
    for body in bodies() {
        client_round_trip(ClientFrame::Publish {
            channel: "brenn:frames".to_string(),
            attribution: None,
            body: body.clone(),
            urgency: Urgency::Normal,
            correlation: None,
        });
        client_round_trip(ClientFrame::PublishBatch {
            attribution: Some("cam".to_string()),
            correlation: 1,
            publishes: vec![BatchEntry::new("brenn:frames", body.clone(), Urgency::Low)],
            deferred_ops: vec![],
        });
        server_round_trip(ServerFrame::Deliver {
            channel: "brenn:frames".to_string(),
            rows: vec![DeliverRow {
                envelope: full_envelope(body.clone()),
                seq: 1,
                cursor: cursor("c"),
                dropped: 0,
            }],
        });
        server_round_trip(ServerFrame::DeferredView {
            channel: "brenn:frames".to_string(),
            attribution: None,
            entries: vec![DeferredViewEntry {
                message_id: Uuid::from_u128(5),
                body,
                deliver_after: 1_700_000_000_000,
            }],
        });
    }
}

/// Entries' bodies first, then the edits that carry one, each in order: what
/// lands where is decided by the descriptors' order alone, so a mixed batch
/// whose every body differs shows the order is kept.
#[test]
fn a_mixed_batch_keeps_frame_order() {
    let all = bodies();
    let frame = ClientFrame::PublishBatch {
        attribution: None,
        correlation: 9,
        publishes: vec![
            BatchEntry::new("brenn:a", all[0].clone(), Urgency::Normal),
            BatchEntry::new("brenn:b", all[3].clone(), Urgency::High).deliver_after(77),
        ],
        deferred_ops: vec![
            BatchDeferredOp {
                channel: "brenn:a".to_string(),
                message_id: Uuid::from_u128(1),
                op: DeferredOpKind::Edit {
                    body: Some(all[5].clone()),
                    deliver_after: None,
                },
            },
            BatchDeferredOp {
                channel: "brenn:a".to_string(),
                message_id: Uuid::from_u128(2),
                op: DeferredOpKind::Cancel,
            },
            BatchDeferredOp {
                channel: "brenn:b".to_string(),
                message_id: Uuid::from_u128(3),
                op: DeferredOpKind::Edit {
                    body: None,
                    deliver_after: Some(5),
                },
            },
            BatchDeferredOp {
                channel: "brenn:b".to_string(),
                message_id: Uuid::from_u128(4),
                op: DeferredOpKind::Edit {
                    body: Some(all[2].clone()),
                    deliver_after: Some(6),
                },
            },
        ],
    };
    let WireMessage::Binary(bytes) = v5().encode_client(&frame) else {
        panic!("a batch is binary");
    };
    let mut section = Vec::new();
    for body in [&all[0], &all[3], &all[5], &all[2]] {
        section.extend_from_slice(body.as_bytes());
    }
    assert!(bytes.ends_with(&section));
    assert_eq!(
        v5().decode_client(WireMessage::Binary(bytes)).unwrap(),
        frame
    );
}

/// The body-carrying kinds are binary whether or not a body is in them.
#[test]
fn a_frame_with_no_bodies_is_still_binary() {
    client_round_trip(ClientFrame::PublishBatch {
        attribution: None,
        correlation: 0,
        publishes: vec![],
        deferred_ops: vec![BatchDeferredOp {
            channel: "brenn:a".to_string(),
            message_id: Uuid::nil(),
            op: DeferredOpKind::Cancel,
        }],
    });
    server_round_trip(ServerFrame::DeferredView {
        channel: "brenn:a".to_string(),
        attribution: Some("cam".to_string()),
        entries: vec![],
    });
    assert_eq!(
        v5().encode_server(&ServerFrame::DeferredView {
            channel: "brenn:a".to_string(),
            attribution: None,
            entries: vec![],
        }),
        raw(
            r#"{"type":"DeferredView","channel":"brenn:a","entries":[]}"#,
            &[]
        )
    );
}

/// A typed text body keeps its type under v5, in both directions.
#[test]
fn typed_text_keeps_its_type() {
    let msg = v5().encode_client(&ClientFrame::Publish {
        channel: "brenn:notes".to_string(),
        attribution: None,
        body: markdown(),
        urgency: Urgency::Normal,
        correlation: None,
    });
    let Ok(ClientFrame::Publish { body, .. }) = v5().decode_client(msg) else {
        panic!("a publish reads back");
    };
    assert_eq!(body.content_type(), Some("text/markdown"));
    assert_eq!(body.as_text(), Some("# \"hi\"\n"));
}

#[test]
fn v5_carries_every_body() {
    for body in bodies() {
        assert!(v5().carries(&body), "{}", body.summary());
    }
}

// ---------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------

const PUBLISH_HEADER_4: &str =
    r#"{"type":"Publish","channel":"brenn:a","body":{"len":4},"urgency":"normal"}"#;

#[test]
fn a_kind_in_the_wrong_message_type_is_refused() {
    assert_eq!(
        client_rule(text(
            r#"{"type":"Publish","channel":"brenn:a","body":"x","urgency":"normal"}"#
        )),
        "text frame of a binary kind"
    );
    assert_eq!(
        client_rule(text(r#"{"type":"PublishBatch"}"#)),
        "text frame of a binary kind"
    );
    assert_eq!(
        server_rule(text(r#"{"type":"Deliver","channel":"a","rows":[]}"#)),
        "text frame of a binary kind"
    );
    assert_eq!(
        server_rule(text(r#"{"type":"DeferredView"}"#)),
        "text frame of a binary kind"
    );
    assert_eq!(
        client_rule(raw(r#"{"type":"Unsubscribe","channel":"brenn:a"}"#, &[])),
        "binary frame of a text kind"
    );
    assert_eq!(
        client_rule(raw(
            r#"{"type":"Hello","versions":{"min":5,"max":5},"ident":"x"}"#,
            &[]
        )),
        "binary frame of a text kind"
    );
    assert_eq!(
        server_rule(raw(r#"{"type":"Heartbeat"}"#, &[])),
        "binary frame of a text kind"
    );
    // A wrong-kind message that is malformed besides is named by the kind.
    assert_eq!(
        client_rule(text(r#"{"type":"Publish","nonsense":true}"#)),
        "text frame of a binary kind"
    );
    assert_eq!(
        server_rule(raw(r#"{"type":"Heartbeat","nonsense":true}"#, &[])),
        "binary frame of a text kind"
    );
    // A right-kind message that is malformed is unparseable.
    assert_eq!(
        client_rule(text(r#"{"type":"Unsubscribe","nonsense":true}"#)),
        "unparseable client frame"
    );
    assert_eq!(
        server_rule(raw(r#"{"type":"Deliver","nonsense":true}"#, &[])),
        "unparseable server frame"
    );
}

#[test]
fn a_prefix_or_header_length_that_does_not_fit_disagrees() {
    for bytes in [vec![], vec![0], vec![0, 0, 0]] {
        assert_eq!(
            client_rule(WireMessage::Binary(bytes.clone())),
            "frame header and bodies disagree"
        );
        assert_eq!(
            server_rule(WireMessage::Binary(bytes)),
            "frame header and bodies disagree"
        );
    }
    let mut past_the_end = 200_u32.to_be_bytes().to_vec();
    past_the_end.extend_from_slice(PUBLISH_HEADER_4.as_bytes());
    assert_eq!(
        client_rule(WireMessage::Binary(past_the_end)),
        "frame header and bodies disagree"
    );
    let mut huge = u32::MAX.to_be_bytes().to_vec();
    huge.extend_from_slice(b"{}");
    assert_eq!(
        server_rule(WireMessage::Binary(huge)),
        "frame header and bodies disagree"
    );
}

#[test]
fn lengths_that_under_or_overrun_the_section_disagree() {
    assert!(v5().decode_client(raw(PUBLISH_HEADER_4, b"abcd")).is_ok());
    for section in [&b"abc"[..], b"abcde", b""] {
        assert_eq!(
            client_rule(raw(PUBLISH_HEADER_4, section)),
            "frame header and bodies disagree",
            "{section:?}"
        );
    }
}

/// The peer's lengths are never summed: lengths whose sum wraps a `u64` or a
/// 32-bit `usize` back to the section's length are refused where the first
/// one that does not fit is read, and nothing panics.
#[test]
fn lengths_chosen_to_wrap_a_sum_disagree_without_panicking() {
    let batch = |lens: [u64; 2]| {
        format!(
            r#"{{"type":"PublishBatch","correlation":1,"publishes":[{{"channel":"a","body":{{"len":{}}},"urgency":"normal"}},{{"channel":"a","body":{{"len":{}}},"urgency":"normal"}}]}}"#,
            lens[0], lens[1]
        )
    };
    for lens in [
        [u64::MAX, 5],
        [1 << 32, 4],
        [(1 << 32) - 1, 5],
        [3, u64::MAX - 2],
    ] {
        assert_eq!(
            client_rule(raw(&batch(lens), b"abcd")),
            "frame header and bodies disagree",
            "{lens:?}"
        );
    }
    let view = format!(
        r#"{{"type":"DeferredView","channel":"a","entries":[{{"message_id":"00000000-0000-0000-0000-000000000001","body":{{"len":{}}},"deliver_after":1}}]}}"#,
        u64::from(u32::MAX) + 1
    );
    assert_eq!(
        server_rule(raw(&view, b"abcd")),
        "frame header and bodies disagree"
    );
}

#[test]
fn a_body_from_parts_refuses_is_malformed() {
    let bad_type = r#"{"type":"Publish","channel":"a","body":{"len":1,"content_type":"no-slash"},"urgency":"normal"}"#;
    assert_eq!(client_rule(raw(bad_type, b"x")), "malformed body");
    assert_eq!(
        client_rule(raw(
            r#"{"type":"Publish","channel":"a","body":{"len":2},"urgency":"normal"}"#,
            &[0xc3, 0x28]
        )),
        "malformed body"
    );
    let typed_text = r#"{"type":"DeferredView","channel":"a","entries":[{"message_id":"00000000-0000-0000-0000-000000000001","body":{"len":1,"content_type":"text/plain"},"deliver_after":1}]}"#;
    let err = v5().decode_server(raw(typed_text, &[0xff])).unwrap_err();
    assert_eq!(err.rule(), "malformed body");
    assert!(err.detail().unwrap().contains("UTF-8"), "{err}");
}

fn deliver_header(row_extra: &str, header_extra: &str) -> String {
    format!(
        concat!(
            r#"{{"type":"Deliver","channel":"ephemeral:demo","rows":[{{"envelope":{{"header":{{"#,
            r#""message_id":"00000000-0000-0000-0000-000000000001","source":"src","#,
            r#""channel":"ephemeral:demo","sender":"remote:alice","publish_ts":"2023-11-14T22:13:20Z","#,
            r#""urgency":"normal","envelope_type":"ephemeral"{}}},"body":{{"len":5}}}},"#,
            r#""seq":1,"cursor":"c-1","dropped":0{}}}]}}"#
        ),
        header_extra, row_extra
    )
}

/// The envelope's `header` is open; every other object is strict.
#[test]
fn only_the_envelope_header_is_open() {
    let ok = v5()
        .decode_server(raw(
            &deliver_header("", r#","blob":{"id":"b-1"}"#),
            b"hello",
        ))
        .unwrap();
    let ServerFrame::Deliver { rows, .. } = ok else {
        panic!("a Deliver");
    };
    assert_eq!(rows[0].envelope, bare_envelope());
    assert_eq!(
        server_rule(raw(&deliver_header(r#","extra":1"#, ""), b"hello")),
        "unparseable server frame"
    );
    assert_eq!(
        server_rule(raw(
            &deliver_header("", "").replace(r#""body":{"len":5}"#, r#""body":{"len":5,"x":1}"#),
            b"hello"
        )),
        "unparseable server frame"
    );
    assert_eq!(
        server_rule(raw(
            &deliver_header("", "")
                .replace(r#"{"envelope":{"header""#, r#"{"envelope":{"x":0,"header""#),
            b"hello"
        )),
        "unparseable server frame"
    );
    assert_eq!(
        client_rule(raw(
            r#"{"type":"Publish","channel":"a","body":{"len":0},"urgency":"normal","extra":true}"#,
            &[]
        )),
        "unparseable client frame"
    );
}

#[test]
fn a_header_that_is_not_json_of_the_schema_is_unparseable() {
    for header in [
        "not json",
        r#"{"no":"tag"}"#,
        r#"{"type":"Nonsense"}"#,
        r#"{"type":"Publish","channel":"a","body":{"len":-1},"urgency":"normal"}"#,
        r#"{"type":"Publish","channel":"a","body":"x","urgency":"normal"}"#,
    ] {
        assert_eq!(
            client_rule(raw(header, &[])),
            "unparseable client frame",
            "{header}"
        );
    }
    let mut not_utf8 = 3_u32.to_be_bytes().to_vec();
    not_utf8.extend_from_slice(&[b'"', 0xff, b'"']);
    assert_eq!(
        server_rule(WireMessage::Binary(not_utf8)),
        "unparseable server frame"
    );
    let err: DecodeError = v5()
        .decode_client(text(r#"{"type":"Subscribe","channel":"a"}"#))
        .unwrap_err();
    assert_eq!(err.rule(), "unparseable client frame");
}

#[test]
fn hello_decodes_as_text_under_v5() {
    let hello = r#"{"type":"Hello","versions":{"min":4,"max":5},"ident":"x"}"#;
    assert!(matches!(
        v5().decode_client(text(hello)),
        Ok(ClientFrame::Hello { .. })
    ));
    assert!(matches!(
        v5().decode_server(text(hello)),
        Ok(ServerFrame::Hello { .. })
    ));
}

// ---------------------------------------------------------------------------
// The batch law and the frame cap
// ---------------------------------------------------------------------------

#[test]
fn the_frame_cap_is_the_body_cap_plus_fixed_allowances() {
    assert_eq!(v5().max_client_frame_bytes(64 * 1024), 204_800);
    assert_eq!(
        v5().max_client_frame_bytes(1024),
        1024 + MAX_BATCH_OPS * BATCH_OP_OVERHEAD_BYTES + PUBLISH_FRAME_OVERHEAD_BYTES
    );
    assert_eq!(
        max_transport_frame_bytes(SUPPORTED_VERSIONS, 64 * 1024),
        Codec::negotiated(4).max_client_frame_bytes(64 * 1024)
    );
}

#[test]
#[should_panic(expected = "WS frame-cap derivation overflowed")]
fn an_unrepresentable_body_cap_panics() {
    v5().max_client_frame_bytes(usize::MAX);
}

/// What `escaped_len` counts is what the serializer writes.
#[test]
fn escaped_length_is_what_json_spends() {
    let mut every_ascii = String::new();
    for byte in 0_u8..0x80 {
        let s = char::from(byte).to_string();
        let written = serde_json::to_string(&s).unwrap().len() - 2;
        assert_eq!(escaped_len(&s), written, "byte {byte:#x}");
        every_ascii.push(char::from(byte));
    }
    for s in [every_ascii.as_str(), "é漢🙂", "brenn:frames", ""] {
        assert_eq!(
            escaped_len(s),
            serde_json::to_string(s).unwrap().len() - 2,
            "{s:?}"
        );
    }
}

#[test]
fn the_charge_is_escaped_strings_plus_raw_bodies() {
    let entry = BatchEntry::new("brenn:\"x\"", jpeg(), Urgency::Normal);
    assert_eq!(
        v5().batch_charged_bytes(&[entry], &[]),
        "brenn:\\\"x\\\"".len() + "image/jpeg".len() + 4
    );
    let untyped = BatchEntry::new("brenn:x", "\u{1}".into(), Urgency::Normal);
    assert_eq!(
        v5().batch_charged_bytes(&[untyped], &[]),
        "brenn:x".len() + 1
    );
    let ops = [
        BatchDeferredOp {
            channel: "brenn:x".to_string(),
            message_id: Uuid::nil(),
            op: DeferredOpKind::Cancel,
        },
        BatchDeferredOp {
            channel: "brenn:x".to_string(),
            message_id: Uuid::nil(),
            op: DeferredOpKind::Edit {
                body: Some(markdown()),
                deliver_after: None,
            },
        },
    ];
    assert_eq!(
        v5().batch_charged_bytes(&[], &ops),
        7 + 7 + "text/markdown".len() + markdown().len()
    );
    assert_eq!(v5().check_batch_legality(&[], &ops, 7 + 7 + 13 + 7), Ok(()));
    assert_eq!(
        v5().check_batch_legality(&[], &ops, 7 + 7 + 13 + 6),
        Err(BatchIllegal::TooManyBytes { bytes: 34, cap: 33 })
    );
}

/// The operation charging `charge` bytes in its widest shape: a channel of a
/// character JSON escapes six-for-one, a content type when the charge has room
/// for one, the rest raw body bytes.
fn charged_parts(charge: usize) -> (String, MessageBody) {
    const CT: &str = "application/x.y";
    let (content_type, rest) = if charge >= CT.len() {
        (Some(CT), charge - CT.len())
    } else {
        (None, charge)
    };
    let channel = "\u{1}".repeat(rest / 12);
    let body_len = rest - 6 * (rest / 12);
    let body = match content_type {
        Some(ct) => MessageBody::from_parts(Some(ct), &vec![0xff; body_len]).unwrap(),
        None => "a".repeat(body_len).into(),
    };
    (channel, body)
}

fn worst_entry(charge: usize) -> BatchEntry {
    let (channel, body) = charged_parts(charge);
    BatchEntry::new(channel, body, Urgency::VeryLow).deliver_after(u64::MAX)
}

fn worst_op(charge: usize) -> BatchDeferredOp {
    let (channel, body) = charged_parts(charge);
    BatchDeferredOp {
        channel,
        message_id: Uuid::from_u128(u128::MAX),
        op: DeferredOpKind::Edit {
            body: Some(body),
            deliver_after: Some(u64::MAX),
        },
    }
}

/// Every legal batch encodes within the cap: swept over body caps and the mix
/// of the two operation shapes, with channels made of characters JSON escapes.
#[test]
fn the_frame_cap_bounds_the_worst_legal_batch() {
    let attribution = "instance-that-a-document-declared";
    for max_body_bytes in [1_024_usize, 16 * 1024, 64 * 1024, 1024 * 1024] {
        let charge = max_body_bytes / MAX_BATCH_OPS;
        for entry_count in [MAX_BATCH_OPS, MAX_BATCH_OPS / 2, 0] {
            let publishes: Vec<BatchEntry> =
                (0..entry_count).map(|_| worst_entry(charge)).collect();
            let deferred_ops: Vec<BatchDeferredOp> = (0..MAX_BATCH_OPS - entry_count)
                .map(|_| worst_op(charge))
                .collect();
            assert_eq!(
                v5().batch_charged_bytes(&publishes, &deferred_ops),
                charge * MAX_BATCH_OPS
            );
            v5().check_batch_legality(&publishes, &deferred_ops, max_body_bytes)
                .expect("the batch is built to the law");
            let measured = v5().batch_frame_bytes(Some(attribution), &publishes, &deferred_ops);
            let msg = v5().encode_client(&ClientFrame::PublishBatch {
                attribution: Some(attribution.to_string()),
                correlation: u64::MAX,
                publishes,
                deferred_ops,
            });
            assert_eq!(measured, msg.len());
            let cap = v5().max_client_frame_bytes(max_body_bytes);
            assert!(
                msg.len() <= cap,
                "{entry_count} entries at a {max_body_bytes}-byte cap encode to {} bytes, over {cap}",
                msg.len()
            );
        }
    }
}

/// One `Publish` with a body at the cap fits the frame cap.
#[test]
fn a_publish_at_the_body_cap_fits() {
    let max_body_bytes = 64 * 1024;
    let msg = v5().encode_client(&ClientFrame::Publish {
        channel: "brenn:frames".to_string(),
        attribution: Some("cam".to_string()),
        body: MessageBody::from_parts(Some("image/jpeg"), &vec![0; max_body_bytes]).unwrap(),
        urgency: Urgency::VeryLow,
        correlation: Some(u64::MAX),
    });
    assert!(msg.len() <= v5().max_client_frame_bytes(max_body_bytes));
}
