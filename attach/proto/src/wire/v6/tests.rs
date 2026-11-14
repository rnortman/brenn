//! v6: the blob frames and the new fields byte for byte, round trips of every
//! new kind, v5's bytes for every frame that carries none of them, each
//! malformation and its rule, the batch law against the frame cap, and the
//! older encoders' refusals.

use std::panic::{AssertUnwindSafe, catch_unwind};

use uuid::Uuid;

use crate::wire::{Codec, WireMessage};
use crate::*;

fn v4() -> Codec {
    Codec::negotiated(4)
}

fn v5() -> Codec {
    Codec::negotiated(5)
}

fn v6() -> Codec {
    Codec::negotiated(6)
}

fn name(s: &str) -> AttachmentName {
    AttachmentName::new(s).unwrap()
}

fn photo(handle: u64) -> PublishAttachment {
    PublishAttachment::new(name("photo"), BlobHandle(handle))
}

fn jpeg() -> MessageBody {
    MessageBody::from_parts(Some("image/jpeg"), &[0xff, 0xd8]).unwrap()
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
    v6().decode_client(msg).unwrap_err().rule()
}

fn server_rule(msg: WireMessage) -> &'static str {
    v6().decode_server(msg).unwrap_err().rule()
}

/// `frame` is written as `msg` under v6 and read back as itself.
fn client_pinned(frame: ClientFrame, msg: WireMessage) {
    assert_eq!(v6().encode_client(&frame), msg, "{frame:?}");
    assert_eq!(v6().decode_client(msg).unwrap(), frame);
}

fn server_pinned(frame: ServerFrame, msg: WireMessage) {
    assert_eq!(v6().encode_server(&frame), msg, "{frame:?}");
    assert_eq!(v6().decode_server(msg).unwrap(), frame);
}

fn welcome(blobs: Option<BlobFacts>) -> ServerFrame {
    ServerFrame::Welcome {
        version: 6,
        participant_id: "remote:alice".to_string(),
        session_id: "s1".to_string(),
        heartbeat_secs: 30,
        max_body_bytes: 65_536,
        max_frame_bytes: 274_432,
        alert_granted: false,
        blobs,
    }
}

fn facts() -> BlobFacts {
    BlobFacts {
        max_blob_bytes: 67_108_864,
        max_chunk_bytes: 262_144,
        max_handles: 64,
        max_bytes_held: 268_435_456,
        max_inflight_reads: 4,
        upload: true,
    }
}

#[test]
fn the_text_blob_frames_are_written_byte_for_byte() {
    for (frame, expected) in [
        (
            ClientFrame::BlobCreate {
                handle: BlobHandle(7),
                content_type: "image/jpeg".to_string(),
                size: 300_000,
            },
            r#"{"type":"BlobCreate","handle":7,"content_type":"image/jpeg","size":300000}"#,
        ),
        (
            ClientFrame::BlobSeal {
                handle: BlobHandle(7),
            },
            r#"{"type":"BlobSeal","handle":7}"#,
        ),
        (
            ClientFrame::BlobOpen {
                handle: BlobHandle(9),
                channel: "ephemeral:cam".to_string(),
                message_id: Uuid::from_u128(1),
                name: name("frame"),
            },
            r#"{"type":"BlobOpen","handle":9,"channel":"ephemeral:cam","message_id":"00000000-0000-0000-0000-000000000001","name":"frame"}"#,
        ),
        (
            ClientFrame::BlobRead {
                correlation: 3,
                handle: BlobHandle(9),
                offset: u64::MAX,
                len: 262_144,
            },
            r#"{"type":"BlobRead","correlation":3,"handle":9,"offset":18446744073709551615,"len":262144}"#,
        ),
        (
            ClientFrame::BlobClose {
                handle: BlobHandle(u64::MAX),
            },
            r#"{"type":"BlobClose","handle":18446744073709551615}"#,
        ),
    ] {
        client_pinned(frame, text(expected));
    }
}

#[test]
fn the_text_blob_answers_are_written_byte_for_byte() {
    let h = BlobHandle(7);
    for (frame, expected) in [
        (
            ServerFrame::BlobCreateResult {
                handle: h,
                outcome: BlobCreateOutcome::Ok,
            },
            r#"{"type":"BlobCreateResult","handle":7,"outcome":{"kind":"Ok"}}"#,
        ),
        (
            ServerFrame::BlobCreateResult {
                handle: h,
                outcome: BlobCreateOutcome::BlobTooLarge { size: 9, max: 8 },
            },
            r#"{"type":"BlobCreateResult","handle":7,"outcome":{"kind":"BlobTooLarge","size":9,"max":8}}"#,
        ),
        (
            ServerFrame::BlobSealResult {
                handle: h,
                outcome: BlobSealOutcome::Incomplete {
                    offset: 262_144,
                    len: 37_856,
                },
            },
            r#"{"type":"BlobSealResult","handle":7,"outcome":{"kind":"Incomplete","offset":262144,"len":37856}}"#,
        ),
        (
            ServerFrame::BlobOpenResult {
                handle: h,
                outcome: BlobOpenOutcome::Ok {
                    size: 300_000,
                    content_type: "image/jpeg".to_string(),
                },
            },
            r#"{"type":"BlobOpenResult","handle":7,"outcome":{"kind":"Ok","size":300000,"content_type":"image/jpeg"}}"#,
        ),
        (
            ServerFrame::BlobOpenResult {
                handle: h,
                outcome: BlobOpenOutcome::ToolChannel,
            },
            r#"{"type":"BlobOpenResult","handle":7,"outcome":{"kind":"ToolChannel"}}"#,
        ),
        (
            ServerFrame::BlobCreateResult {
                handle: h,
                outcome: BlobCreateOutcome::BytesHeldLimit,
            },
            r#"{"type":"BlobCreateResult","handle":7,"outcome":{"kind":"BytesHeldLimit"}}"#,
        ),
        (
            ServerFrame::BlobOpenResult {
                handle: h,
                outcome: BlobOpenOutcome::BytesHeldLimit { size: 300_000 },
            },
            r#"{"type":"BlobOpenResult","handle":7,"outcome":{"kind":"BytesHeldLimit","size":300000}}"#,
        ),
    ] {
        server_pinned(frame, text(expected));
    }
}

#[test]
fn every_blob_answer_outcome_round_trips() {
    let h = BlobHandle(1);
    let frames = [
        BlobCreateOutcome::Ok,
        BlobCreateOutcome::BlobTooLarge { size: 2, max: 1 },
        BlobCreateOutcome::BudgetExhausted,
        BlobCreateOutcome::HandleLimit,
        BlobCreateOutcome::BytesHeldLimit,
        BlobCreateOutcome::StorageFailed,
    ]
    .map(|outcome| ServerFrame::BlobCreateResult { handle: h, outcome })
    .into_iter()
    .chain(
        [
            BlobSealOutcome::Ok,
            BlobSealOutcome::Incomplete { offset: 0, len: 1 },
            BlobSealOutcome::UnknownHandle,
        ]
        .map(|outcome| ServerFrame::BlobSealResult { handle: h, outcome }),
    )
    .chain(
        [
            BlobOpenOutcome::NotRetained,
            BlobOpenOutcome::NoSuchAttachment,
            BlobOpenOutcome::ToolChannel,
            BlobOpenOutcome::HandleLimit,
            BlobOpenOutcome::BytesHeldLimit { size: u64::MAX },
        ]
        .map(|outcome| ServerFrame::BlobOpenResult { handle: h, outcome }),
    )
    .chain(
        [
            BlobReadOutcome::Ok { bytes: Vec::new() },
            BlobReadOutcome::Ok {
                bytes: vec![0, 255, 7],
            },
            BlobReadOutcome::OutOfRange { size: u64::MAX },
            BlobReadOutcome::UnknownHandle,
            BlobReadOutcome::NotSealed,
            BlobReadOutcome::TooManyReads,
            BlobReadOutcome::StorageFailed,
        ]
        .map(|outcome| ServerFrame::BlobReadResult {
            correlation: 4,
            outcome,
        }),
    )
    .chain([
        ServerFrame::PublishResult {
            correlation: Some(1),
            outcome: PublishOutcome::AttachmentNotHeld,
        },
        ServerFrame::PublishBatchResult {
            correlation: 1,
            outcome: PublishBatchOutcome::AttachmentNotHeld,
        },
    ]);
    for frame in frames {
        let msg = v6().encode_server(&frame);
        assert_eq!(v6().decode_server(msg).unwrap(), frame);
    }
}

#[test]
fn a_blob_write_is_written_byte_for_byte() {
    let header = r#"{"type":"BlobWrite","handle":7,"offset":262144,"len":3}"#;
    client_pinned(
        ClientFrame::BlobWrite {
            handle: BlobHandle(7),
            offset: 262_144,
            bytes: vec![0xff, 0x00, 0xfe],
        },
        raw(header, &[0xff, 0x00, 0xfe]),
    );
}

#[test]
fn a_read_answer_carries_its_bytes_in_the_body_section_and_a_refusal_none() {
    server_pinned(
        ServerFrame::BlobReadResult {
            correlation: 3,
            outcome: BlobReadOutcome::Ok {
                bytes: vec![1, 2, 3, 4],
            },
        },
        raw(
            r#"{"type":"BlobReadResult","correlation":3,"outcome":{"kind":"Ok","len":4}}"#,
            &[1, 2, 3, 4],
        ),
    );
    server_pinned(
        ServerFrame::BlobReadResult {
            correlation: 3,
            outcome: BlobReadOutcome::NotSealed,
        },
        raw(
            r#"{"type":"BlobReadResult","correlation":3,"outcome":{"kind":"NotSealed"}}"#,
            &[],
        ),
    );
}

#[test]
fn a_publish_names_its_attachments_in_a_list() {
    let header = r#"{"type":"Publish","channel":"ephemeral:cam","body":{"len":2,"content_type":"image/jpeg"},"urgency":"normal","correlation":1,"attachments":[{"name":"photo","handle":7},{"name":"photo","handle":8}]}"#;
    client_pinned(
        ClientFrame::Publish {
            channel: "ephemeral:cam".to_string(),
            attribution: None,
            body: jpeg(),
            urgency: Urgency::Normal,
            correlation: Some(1),
            attachments: vec![photo(7), photo(8)],
        },
        raw(header, &[0xff, 0xd8]),
    );
}

#[test]
fn a_batch_entry_names_its_attachments_in_a_list() {
    let header = r#"{"type":"PublishBatch","correlation":2,"publishes":[{"channel":"brenn:a","body":{"len":1},"urgency":"normal","attachments":[{"name":"photo","handle":7}]},{"channel":"brenn:b","body":{"len":1},"urgency":"normal"}]}"#;
    client_pinned(
        ClientFrame::PublishBatch {
            attribution: None,
            correlation: 2,
            publishes: vec![
                BatchEntry::new("brenn:a", "x".into(), Urgency::Normal).attachments(vec![photo(7)]),
                BatchEntry::new("brenn:b", "y".into(), Urgency::Normal),
            ],
            deferred_ops: Vec::new(),
        },
        raw(header, b"xy"),
    );
}

#[test]
fn a_welcome_states_its_blob_door() {
    server_pinned(
        welcome(Some(facts())),
        text(
            r#"{"type":"Welcome","version":6,"participant_id":"remote:alice","session_id":"s1","heartbeat_secs":30,"max_body_bytes":65536,"max_frame_bytes":274432,"alert_granted":false,"blobs":{"max_blob_bytes":67108864,"max_chunk_bytes":262144,"max_handles":64,"max_bytes_held":268435456,"max_inflight_reads":4,"upload":true}}"#,
        ),
    );
}

/// Every v5 frame kind, carrying none of v6's fields.
fn v5_shaped_frames() -> (Vec<ClientFrame>, Vec<ServerFrame>) {
    let envelope = |body: MessageBody| MessageEnvelope {
        message_id: Uuid::from_u128(5),
        source: "src".to_string(),
        channel: "ephemeral:demo".to_string(),
        sender: "remote:alice".to_string(),
        publish_ts: chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_000, 0).unwrap(),
        body,
        reply_to: None,
        delivery_deadline: None,
        deliver_after: None,
        impetus: None,
        urgency: Urgency::Normal,
        envelope_type: brenn_envelope::ChannelScheme::Ephemeral,
        transport: None,
        attachments: Default::default(),
    };
    let cursor: Cursor = serde_json::from_value(serde_json::json!("c1")).unwrap();
    let client = vec![
        ClientFrame::Hello {
            versions: SUPPORTED_VERSIONS,
            ident: "x".to_string(),
        },
        ClientFrame::Subscribe {
            channel: "brenn:a".to_string(),
            push_depth: 1,
            retain_depth: 2,
            resume: Some(cursor.clone()),
        },
        ClientFrame::Unsubscribe {
            channel: "brenn:a".to_string(),
        },
        ClientFrame::Publish {
            channel: "brenn:a".to_string(),
            attribution: Some("cam".to_string()),
            body: jpeg(),
            urgency: Urgency::High,
            correlation: None,
            attachments: Vec::new(),
        },
        ClientFrame::PublishBatch {
            attribution: None,
            correlation: 9,
            publishes: vec![BatchEntry::new("brenn:a", jpeg(), Urgency::Low).deliver_after(5)],
            deferred_ops: vec![BatchDeferredOp {
                channel: "brenn:a".to_string(),
                message_id: Uuid::from_u128(3),
                op: DeferredOpKind::Edit {
                    body: Some("e".into()),
                    deliver_after: None,
                },
            }],
        },
        ClientFrame::Alert {
            attribution: None,
            severity: AlertSeverity::Warning,
            title: "t".to_string(),
            body: "b".to_string(),
        },
    ];
    let server = vec![
        ServerFrame::Hello {
            versions: SUPPORTED_VERSIONS,
            ident: "x".to_string(),
        },
        welcome(None),
        ServerFrame::Heartbeat,
        ServerFrame::SubscribeResult {
            channel: "brenn:a".to_string(),
            outcome: SubscribeOutcome::Ok,
            replay_count: 1,
            gap: Some(GapInfo {
                reason: GapReason::BeyondRetained,
            }),
        },
        ServerFrame::Deliver {
            channel: "ephemeral:demo".to_string(),
            rows: vec![DeliverRow {
                envelope: envelope(jpeg()),
                seq: 1,
                cursor,
                dropped: 0,
            }],
        },
        ServerFrame::PublishResult {
            correlation: Some(1),
            outcome: PublishOutcome::TextOnlyChannel,
        },
        ServerFrame::PublishBatchResult {
            correlation: 1,
            outcome: PublishBatchOutcome::RateLimited,
        },
        ServerFrame::DeferredView {
            channel: "brenn:a".to_string(),
            attribution: Some("cam".to_string()),
            entries: vec![DeferredViewEntry {
                message_id: Uuid::from_u128(4),
                body: "later".into(),
                deliver_after: 99,
            }],
        },
    ];
    (client, server)
}

#[test]
fn a_frame_with_none_of_v6s_fields_keeps_its_v5_bytes() {
    let (client, server) = v5_shaped_frames();
    for frame in client {
        let msg = v5().encode_client(&frame);
        assert_eq!(v6().encode_client(&frame), msg, "{frame:?}");
        assert_eq!(v6().decode_client(msg).unwrap(), frame);
    }
    for frame in server {
        let msg = v5().encode_server(&frame);
        assert_eq!(v6().encode_server(&frame), msg, "{frame:?}");
        assert_eq!(v6().decode_server(msg).unwrap(), frame);
    }
}

#[test]
fn an_unknown_field_in_a_new_frame_is_unparseable() {
    for bad in [
        r#"{"type":"BlobCreate","handle":7,"content_type":"a/b","size":1,"x":0}"#,
        r#"{"type":"BlobSeal","handle":7,"x":0}"#,
        r#"{"type":"BlobOpen","handle":9,"channel":"c","message_id":"00000000-0000-0000-0000-000000000001","name":"n","x":0}"#,
        r#"{"type":"BlobRead","correlation":1,"handle":9,"offset":0,"len":1,"x":0}"#,
        r#"{"type":"BlobClose","handle":7,"x":0}"#,
        r#"{"type":"BlobClose"}"#,
        r#"{"type":"BlobClose","handle":-1}"#,
    ] {
        assert_eq!(client_rule(text(bad)), "unparseable client frame", "{bad}");
    }
    assert_eq!(
        client_rule(raw(
            r#"{"type":"BlobWrite","handle":7,"offset":0,"len":1,"x":0}"#,
            &[1]
        )),
        "unparseable client frame"
    );
    assert_eq!(
        client_rule(raw(
            r#"{"type":"Publish","channel":"c","body":{"len":0},"urgency":"normal","attachments":[{"name":"a","handle":1,"x":0}]}"#,
            &[]
        )),
        "unparseable client frame"
    );
    for bad in [
        r#"{"type":"BlobCreateResult","handle":7,"outcome":{"kind":"BlobTooLarge","size":2,"max":1,"x":0}}"#,
        r#"{"type":"BlobCreateResult","handle":7,"x":0,"outcome":{"kind":"Ok"}}"#,
        r#"{"type":"BlobSealResult","handle":7,"outcome":{"kind":"Nope"}}"#,
        r#"{"type":"BlobOpenResult","handle":7,"outcome":{"kind":"Ok","size":1}}"#,
        r#"{"type":"Welcome","version":6,"participant_id":"p","session_id":"s","heartbeat_secs":1,"max_body_bytes":1,"max_frame_bytes":1,"alert_granted":false,"blobs":{"upload":true}}"#,
    ] {
        assert_eq!(server_rule(text(bad)), "unparseable server frame", "{bad}");
    }
    assert_eq!(
        server_rule(raw(
            r#"{"type":"BlobReadResult","correlation":1,"outcome":{"kind":"OutOfRange","size":1,"x":0}}"#,
            &[]
        )),
        "unparseable server frame"
    );
}

/// Every tagged variant without fields refuses a key beside its tag: the
/// same object with the key removed decodes.
#[test]
fn a_key_beside_a_field_less_tag_is_unparseable() {
    let op = |kind: &str| {
        format!(
            r#"{{"type":"PublishBatch","correlation":1,"publishes":[],"deferred_ops":[{{"channel":"c","message_id":"00000000-0000-0000-0000-000000000001","op":{kind}}}]}}"#
        )
    };
    v6().decode_client(raw(&op(r#"{"kind":"cancel"}"#), &[]))
        .unwrap();
    assert_eq!(
        client_rule(raw(&op(r#"{"kind":"cancel","x":0}"#), &[])),
        "unparseable client frame"
    );

    let mut server: Vec<String> = vec![r#"{"type":"Heartbeat"}"#.to_string()];
    for kind in ["Ok", "Unavailable"] {
        server.push(format!(
            r#"{{"type":"SubscribeResult","channel":"c","outcome":{{"kind":"{kind}"}},"replay_count":0}}"#
        ));
    }
    for kind in ["EpochChanged", "BeyondRetained"] {
        server.push(format!(
            r#"{{"type":"SubscribeResult","channel":"c","outcome":{{"kind":"Ok"}},"replay_count":0,"gap":{{"reason":{{"kind":"{kind}"}}}}}}"#
        ));
    }
    for kind in [
        "Ok",
        "RateLimited",
        "Failed",
        "TextOnlyChannel",
        "AttachmentNotHeld",
    ] {
        server.push(format!(
            r#"{{"type":"PublishResult","outcome":{{"kind":"{kind}"}}}}"#
        ));
    }
    for kind in ["Ok", "RateLimited", "TextOnlyChannel", "AttachmentNotHeld"] {
        server.push(format!(
            r#"{{"type":"PublishBatchResult","correlation":1,"outcome":{{"kind":"{kind}"}}}}"#
        ));
    }
    for kind in [
        "Ok",
        "BudgetExhausted",
        "HandleLimit",
        "BytesHeldLimit",
        "StorageFailed",
    ] {
        server.push(format!(
            r#"{{"type":"BlobCreateResult","handle":7,"outcome":{{"kind":"{kind}"}}}}"#
        ));
    }
    for kind in ["Ok", "UnknownHandle"] {
        server.push(format!(
            r#"{{"type":"BlobSealResult","handle":7,"outcome":{{"kind":"{kind}"}}}}"#
        ));
    }
    for kind in [
        "NotRetained",
        "NoSuchAttachment",
        "ToolChannel",
        "HandleLimit",
    ] {
        server.push(format!(
            r#"{{"type":"BlobOpenResult","handle":7,"outcome":{{"kind":"{kind}"}}}}"#
        ));
    }
    // A field-carrying variant is as strict, and its field is required.
    let held =
        r#"{"type":"BlobOpenResult","handle":7,"outcome":{"kind":"BytesHeldLimit","size":1}}"#;
    server.push(held.to_string());
    assert_eq!(
        server_rule(text(
            r#"{"type":"BlobOpenResult","handle":7,"outcome":{"kind":"BytesHeldLimit"}}"#
        )),
        "unparseable server frame"
    );
    for good in server {
        v6().decode_server(text(&good)).unwrap();
        let bad = with_extra_key(&good);
        assert_eq!(server_rule(text(&bad)), "unparseable server frame", "{bad}");
    }
    for kind in [
        "UnknownHandle",
        "NotSealed",
        "TooManyReads",
        "StorageFailed",
    ] {
        let good =
            format!(r#"{{"type":"BlobReadResult","correlation":1,"outcome":{{"kind":"{kind}"}}}}"#);
        v6().decode_server(raw(&good, &[])).unwrap();
        let bad = with_extra_key(&good);
        assert_eq!(
            server_rule(raw(&bad, &[])),
            "unparseable server frame",
            "{bad}"
        );
    }
}

/// `json` with `"x":0` added to the first object that closes.
fn with_extra_key(json: &str) -> String {
    let close = json.find('}').unwrap();
    format!("{},\"x\":0{}", &json[..close], &json[close..])
}

#[test]
fn attachments_given_as_an_object_are_unparseable() {
    assert_eq!(
        client_rule(raw(
            r#"{"type":"Publish","channel":"c","body":{"len":0},"urgency":"normal","attachments":{"photo":7}}"#,
            &[]
        )),
        "unparseable client frame"
    );
}

#[test]
fn a_name_outside_the_grammar_is_a_malformed_attachment_name() {
    assert_eq!(
        client_rule(text(
            r#"{"type":"BlobOpen","handle":9,"channel":"c","message_id":"00000000-0000-0000-0000-000000000001","name":"-x"}"#
        )),
        "malformed attachment name"
    );
    assert_eq!(
        client_rule(raw(
            r#"{"type":"Publish","channel":"c","body":{"len":0},"urgency":"normal","attachments":[{"name":"","handle":1}]}"#,
            &[]
        )),
        "malformed attachment name"
    );
    let long = "a".repeat(MAX_ATTACHMENT_NAME_LEN + 1);
    assert_eq!(
        client_rule(raw(
            &format!(
                r#"{{"type":"PublishBatch","correlation":1,"publishes":[{{"channel":"c","body":{{"len":0}},"urgency":"normal","attachments":[{{"name":"{long}","handle":1}}]}}]}}"#
            ),
            &[]
        )),
        "malformed attachment name"
    );
}

#[test]
fn a_length_that_disagrees_with_the_body_section_is_refused() {
    const DISAGREE: &str = "frame header and bodies disagree";
    let write = |len: u64| format!(r#"{{"type":"BlobWrite","handle":7,"offset":0,"len":{len}}}"#);
    assert_eq!(client_rule(raw(&write(3), &[1, 2])), DISAGREE);
    assert_eq!(client_rule(raw(&write(1), &[1, 2])), DISAGREE);
    assert_eq!(client_rule(raw(&write(u64::MAX), &[1, 2])), DISAGREE);
    let read = |len: u64| {
        format!(
            r#"{{"type":"BlobReadResult","correlation":1,"outcome":{{"kind":"Ok","len":{len}}}}}"#
        )
    };
    assert_eq!(server_rule(raw(&read(3), &[1, 2])), DISAGREE);
    assert_eq!(server_rule(raw(&read(1), &[1, 2])), DISAGREE);
    assert_eq!(server_rule(raw(&read(u64::MAX), &[1, 2])), DISAGREE);
    assert_eq!(
        server_rule(raw(
            r#"{"type":"BlobReadResult","correlation":1,"outcome":{"kind":"UnknownHandle"}}"#,
            &[0]
        )),
        DISAGREE
    );
}

#[test]
fn a_blob_frame_of_the_wrong_message_type_is_named_by_its_rule() {
    assert_eq!(
        client_rule(text(
            r#"{"type":"BlobWrite","handle":7,"offset":0,"len":0}"#
        )),
        "text frame of a binary kind"
    );
    assert_eq!(
        client_rule(raw(
            r#"{"type":"BlobRead","correlation":1,"handle":9,"offset":0,"len":1}"#,
            &[]
        )),
        "binary frame of a text kind"
    );
    assert_eq!(
        server_rule(text(
            r#"{"type":"BlobReadResult","correlation":1,"outcome":{"kind":"NotSealed"}}"#
        )),
        "text frame of a binary kind"
    );
    assert_eq!(
        server_rule(raw(
            r#"{"type":"BlobOpenResult","handle":7,"outcome":{"kind":"NotRetained"}}"#,
            &[]
        )),
        "binary frame of a text kind"
    );
}

#[test]
fn a_v5_session_cannot_read_v6s_additions() {
    assert_eq!(
        v5().decode_client(text(r#"{"type":"BlobClose","handle":7}"#))
            .unwrap_err()
            .rule(),
        "unparseable client frame"
    );
    assert_eq!(
        v5().decode_client(raw(
            r#"{"type":"Publish","channel":"c","body":{"len":0},"urgency":"normal","attachments":[{"name":"a","handle":1}]}"#,
            &[]
        ))
        .unwrap_err()
        .rule(),
        "unparseable client frame"
    );
}

#[test]
fn an_attachment_is_charged_its_name_and_the_fixed_allowance() {
    assert_eq!(v6().attachments_charged_bytes(&[]), 0);
    assert_eq!(
        v6().attachments_charged_bytes(&[photo(1), photo(u64::MAX)]),
        2 * ("photo".len() + ATTACHMENT_OVERHEAD_BYTES)
    );
    assert_eq!(v5().attachments_charged_bytes(&[]), 0);
    let entry = BatchEntry::new("brenn:a", "abc".into(), Urgency::Normal);
    let with = entry.clone().attachments(vec![photo(1)]);
    assert_eq!(
        v6().batch_charged_bytes(std::slice::from_ref(&with), &[]),
        v6().batch_charged_bytes(std::slice::from_ref(&entry), &[]) + 5 + ATTACHMENT_OVERHEAD_BYTES
    );
}

#[test]
fn attachments_can_make_a_legal_batch_illegal() {
    let max = 100;
    let entry = BatchEntry::new("brenn:a", "x".repeat(max - 7).into(), Urgency::Normal);
    assert_eq!(
        v6().batch_charged_bytes(std::slice::from_ref(&entry), &[]),
        max
    );
    assert_eq!(
        v6().check_batch_legality(std::slice::from_ref(&entry), &[], max),
        Ok(())
    );
    let with = entry.attachments(vec![photo(1)]);
    assert_eq!(
        v6().check_batch_legality(&[with], &[], max),
        Err(BatchIllegal::TooManyBytes {
            bytes: max + 5 + ATTACHMENT_OVERHEAD_BYTES,
            cap: max
        })
    );
}

#[test]
fn the_v6_frame_cap_is_derived_from_the_chunk_bound_and_the_body_cap() {
    assert_eq!(v6().max_client_frame_bytes(65_536), 274_432);
    // Under a body cap whose batch term passes the chunk bound, the body cap
    // governs, as in v5, plus the attachments term.
    assert_eq!(
        v6().max_client_frame_bytes(1 << 20),
        v5().max_client_frame_bytes(1 << 20) + 32 * 128
    );
    // At the default body cap the route's read cap is still v4's.
    assert_eq!(
        max_transport_frame_bytes(SUPPORTED_VERSIONS, 65_536),
        v4().max_client_frame_bytes(65_536)
    );
}

#[test]
fn a_maximal_blob_write_and_a_maximal_publish_fit_the_cap() {
    let cap = v6().max_client_frame_bytes(65_536);
    let write = v6().encode_client(&ClientFrame::BlobWrite {
        handle: BlobHandle(u64::MAX),
        offset: u64::MAX,
        bytes: vec![0; BLOB_CHUNK_BYTES],
    });
    assert!(write.len() <= cap, "{} > {cap}", write.len());
    let attachments = (0..MAX_ATTACHMENTS_PER_MESSAGE)
        .map(|i| {
            let mut s = format!("{i:02}");
            s.push_str(&"n".repeat(MAX_ATTACHMENT_NAME_LEN - 2));
            PublishAttachment::new(name(&s), BlobHandle(u64::MAX))
        })
        .collect();
    let publish = v6().encode_client(&ClientFrame::Publish {
        channel: "c".repeat(1024),
        attribution: Some("a".repeat(256)),
        body: MessageBody::from_parts(Some("application/octet-stream"), &[0; 65_536]).unwrap(),
        urgency: Urgency::VeryLow,
        correlation: Some(u64::MAX),
        attachments,
    });
    assert!(publish.len() <= cap, "{} > {cap}", publish.len());
}

#[test]
fn a_maximal_legal_batch_with_attachments_fits_the_cap() {
    let max = 65_536;
    let cap = v6().max_client_frame_bytes(max);
    let per_entry = 2 * (MAX_ATTACHMENT_NAME_LEN + ATTACHMENT_OVERHEAD_BYTES);
    let entries: Vec<BatchEntry> = (0..max / per_entry)
        .map(|i| {
            let attachments = (0..2)
                .map(|j| {
                    let mut s = format!("{i:04}{j}");
                    s.push_str(&"n".repeat(MAX_ATTACHMENT_NAME_LEN - 5));
                    PublishAttachment::new(name(&s), BlobHandle(u64::MAX))
                })
                .collect();
            BatchEntry::new(
                "",
                MessageBody::from_parts(None, &[]).unwrap(),
                Urgency::VeryLow,
            )
            .deliver_after(u64::MAX)
            .attachments(attachments)
        })
        .collect();
    assert_eq!(v6().check_batch_legality(&entries, &[], max), Ok(()));
    let frame = v6().batch_frame_bytes(Some(&"a".repeat(256)), &entries, &[]);
    assert!(frame <= cap, "{frame} > {cap}");
}

/// Whether `f` panics with a message containing `needle`.
fn panics_with(needle: &str, f: impl FnOnce()) -> bool {
    let Err(payload) = catch_unwind(AssertUnwindSafe(f)) else {
        return false;
    };
    let message = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or_default();
    message.contains(needle)
}

#[test]
fn the_older_encoders_refuse_every_v6_addition() {
    let client = [
        ClientFrame::BlobCreate {
            handle: BlobHandle(1),
            content_type: "a/b".to_string(),
            size: 1,
        },
        ClientFrame::BlobWrite {
            handle: BlobHandle(1),
            offset: 0,
            bytes: vec![1],
        },
        ClientFrame::BlobSeal {
            handle: BlobHandle(1),
        },
        ClientFrame::BlobOpen {
            handle: BlobHandle(1),
            channel: "c".to_string(),
            message_id: Uuid::nil(),
            name: name("a"),
        },
        ClientFrame::BlobRead {
            correlation: 1,
            handle: BlobHandle(1),
            offset: 0,
            len: 1,
        },
        ClientFrame::BlobClose {
            handle: BlobHandle(1),
        },
        ClientFrame::Publish {
            channel: "c".to_string(),
            attribution: None,
            body: "x".into(),
            urgency: Urgency::Normal,
            correlation: None,
            attachments: vec![photo(1)],
        },
        ClientFrame::PublishBatch {
            attribution: None,
            correlation: 1,
            publishes: vec![
                BatchEntry::new("c", "x".into(), Urgency::Normal).attachments(vec![photo(1)]),
            ],
            deferred_ops: Vec::new(),
        },
    ];
    let server = [
        welcome(Some(facts())),
        ServerFrame::BlobCreateResult {
            handle: BlobHandle(1),
            outcome: BlobCreateOutcome::Ok,
        },
        ServerFrame::BlobSealResult {
            handle: BlobHandle(1),
            outcome: BlobSealOutcome::Ok,
        },
        ServerFrame::BlobOpenResult {
            handle: BlobHandle(1),
            outcome: BlobOpenOutcome::NotRetained,
        },
        ServerFrame::BlobReadResult {
            correlation: 1,
            outcome: BlobReadOutcome::NotSealed,
        },
        ServerFrame::PublishResult {
            correlation: None,
            outcome: PublishOutcome::AttachmentNotHeld,
        },
        ServerFrame::PublishBatchResult {
            correlation: 1,
            outcome: PublishBatchOutcome::AttachmentNotHeld,
        },
    ];
    for codec in [v4(), v5()] {
        for frame in &client {
            assert!(
                panics_with("cannot", || {
                    codec.encode_client(frame);
                }),
                "v{} wrote {frame:?}",
                codec.version()
            );
        }
        for frame in &server {
            assert!(
                panics_with("cannot", || {
                    codec.encode_server(frame);
                }),
                "v{} wrote {frame:?}",
                codec.version()
            );
        }
        let with = [BatchEntry::new("c", "x".into(), Urgency::Normal).attachments(vec![photo(1)])];
        assert!(panics_with("cannot carry publish attachments", || {
            codec.batch_charged_bytes(&with, &[]);
        }));
        assert!(panics_with("cannot carry publish attachments", || {
            codec.attachments_charged_bytes(&[photo(1)]);
        }));
    }
}
