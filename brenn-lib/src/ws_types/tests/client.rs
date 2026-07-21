use super::super::*;

use crate::pwa_push::test_helpers::{fake_auth, fake_p256dh};

#[test]
fn client_send_message_round_trip() {
    let msg = WsClientMessage::SendMessage {
        text: "hello".into(),
        attachments: vec![],
        model: None,
        selected_tasks: vec![],
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert!(json.contains("\"type\":\"SendMessage\""));
    let parsed: WsClientMessage = serde_json::from_str(&json).unwrap();
    match parsed {
        WsClientMessage::SendMessage { text, .. } => assert_eq!(text, "hello"),
        _ => panic!("wrong variant"),
    }
}

#[test]
fn client_send_message_with_model_round_trip() {
    let msg = WsClientMessage::SendMessage {
        text: "hello".into(),
        attachments: vec![],
        model: Some("opus".into()),
        selected_tasks: vec![],
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert!(json.contains("\"model\":\"opus\""));
    let parsed: WsClientMessage = serde_json::from_str(&json).unwrap();
    match parsed {
        WsClientMessage::SendMessage { model, .. } => {
            assert_eq!(model.as_deref(), Some("opus"));
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn client_send_message_without_attachments_deserializes() {
    // Old frontend sends SendMessage without attachments field.
    // #[serde(default)] on attachments must handle this gracefully.
    let json = r#"{"type":"SendMessage","text":"hello"}"#;
    let parsed: WsClientMessage = serde_json::from_str(json).unwrap();
    match parsed {
        WsClientMessage::SendMessage {
            text,
            attachments,
            model,
            selected_tasks,
        } => {
            assert_eq!(text, "hello");
            assert!(attachments.is_empty());
            assert!(model.is_none());
            assert!(selected_tasks.is_empty());
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn client_permission_response_deny_round_trip() {
    let msg = WsClientMessage::PermissionResponse {
        request_id: "req_1".into(),
        decision: PermissionDecision::Deny {
            reason: Some("no".into()),
        },
    };
    let json = serde_json::to_string(&msg).unwrap();
    let parsed: WsClientMessage = serde_json::from_str(&json).unwrap();
    match parsed {
        WsClientMessage::PermissionResponse {
            request_id,
            decision,
        } => {
            assert_eq!(request_id, "req_1");
            match decision {
                PermissionDecision::Deny { reason } => {
                    assert_eq!(reason.as_deref(), Some("no"))
                }
                _ => panic!("wrong decision variant"),
            }
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn client_tool_card_response_deny_round_trip() {
    let msg = WsClientMessage::ToolCardResponse {
        request_id: "req_2".into(),
        decision: ToolResponseDecision::Deny {
            reason: Some("bad".into()),
        },
    };
    let json = serde_json::to_string(&msg).unwrap();
    let parsed: WsClientMessage = serde_json::from_str(&json).unwrap();
    match parsed {
        WsClientMessage::ToolCardResponse {
            request_id,
            decision,
        } => {
            assert_eq!(request_id, "req_2");
            match decision {
                ToolResponseDecision::Deny { reason } => {
                    assert_eq!(reason.as_deref(), Some("bad"))
                }
                _ => panic!("wrong decision variant"),
            }
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn client_new_message_variants_round_trip() {
    let variants = vec![
        WsClientMessage::ListConversations,
        WsClientMessage::SwitchConversation {
            conversation_id: 42,
        },
        WsClientMessage::NewConversation,
        WsClientMessage::Reconnect {
            conversation_id: 7,
            last_seq: None,
        },
        WsClientMessage::Reconnect {
            conversation_id: 7,
            last_seq: Some(15),
        },
        WsClientMessage::ReopenArtifact {
            file_path: "docs/plan.md".into(),
            message_id: None,
        },
        WsClientMessage::ReopenArtifact {
            file_path: "docs/plan.md".into(),
            message_id: Some(42),
        },
        WsClientMessage::LoadArtifactSnapshot { message_id: 42 },
        WsClientMessage::StealApp,
        WsClientMessage::SetTimezone {
            timezone: "America/New_York".into(),
        },
        WsClientMessage::SetViewportClass {
            viewport_class: ViewportClass::Compact,
        },
        WsClientMessage::SetViewportClass {
            viewport_class: ViewportClass::Wide,
        },
        WsClientMessage::SetConversationPrivacy {
            conversation_id: 42,
            shared: true,
        },
        WsClientMessage::TodoRefresh,
        WsClientMessage::TodoDone {
            path: "todo/buy-groceries.md".into(),
            repo: Some("life".into()),
            completion_date: chrono::NaiveDate::from_ymd_opt(2026, 4, 20).unwrap(),
        },
        WsClientMessage::TodoDone {
            path: "todo/buy-groceries.md".into(),
            repo: None,
            completion_date: chrono::NaiveDate::from_ymd_opt(2026, 4, 20).unwrap(),
        },
        WsClientMessage::TodoSchedule {
            path: "todo/buy-groceries.md".into(),
            repo: Some("eng".into()),
            date: chrono::NaiveDate::from_ymd_opt(2026, 4, 20).unwrap(),
        },
        WsClientMessage::TodoReorder {
            path: "todo/buy-groceries.md".into(),
            repo: Some("life".into()),
            after: Some(TodoAnchor {
                path: "todo/walk-dog.md".into(),
                repo: Some("life".into()),
            }),
            before: Some(TodoAnchor {
                path: "todo/clean-house.md".into(),
                repo: Some("life".into()),
            }),
        },
        WsClientMessage::TodoReorder {
            path: "todo/buy-groceries.md".into(),
            repo: None,
            after: Some(TodoAnchor {
                path: "todo/walk-dog.md".into(),
                repo: None,
            }),
            before: None,
        },
    ];

    for msg in &variants {
        let json = serde_json::to_string(msg).unwrap();
        assert!(json.contains("\"type\":"));
        let parsed: WsClientMessage = serde_json::from_str(&json).unwrap();
        let _ = serde_json::to_string(&parsed).unwrap();
    }
}

// Live browser→server variants deserialized from literals copied verbatim from
// the frontend send sites. This is the direction that feeds fail2ban: a Rust
// field rename stops the frontend-shaped literal from deserializing, so the
// test fails instead of silently rejecting real traffic as an attack.

#[test]
fn push_subscribe_deserializes_frontend_wire_shape() {
    // SYNC: frontend send site push.ts `ws.send({ type: "PushSubscribe", ... })`.
    // p256dh: 65-byte P-256 public key (~87 base64url chars); auth: 16-byte
    // secret (~22 base64url chars). Values come from the shared pwa_push fake
    // helpers so no token-shaped literal is checked in; only the wire shape is
    // pinned.
    let endpoint = "https://fcm.googleapis.com/fcm/send/eYxabc123";
    let p256dh = fake_p256dh();
    let auth = fake_auth();
    let json = format!(
        r#"{{"type":"PushSubscribe","endpoint":"{endpoint}","p256dh":"{p256dh}","auth":"{auth}"}}"#
    );
    match serde_json::from_str::<WsClientMessage>(&json).unwrap() {
        WsClientMessage::PushSubscribe {
            endpoint: got_endpoint,
            p256dh: got_p256dh,
            auth: got_auth,
        } => {
            assert_eq!(got_endpoint, endpoint);
            assert_eq!(got_p256dh, p256dh);
            assert_eq!(got_auth, auth);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn push_unsubscribe_deserializes_frontend_wire_shape() {
    // SYNC: frontend send site push.ts `ws.send({ type: "PushUnsubscribe" })`.
    let json = r#"{"type":"PushUnsubscribe"}"#;
    assert!(matches!(
        serde_json::from_str::<WsClientMessage>(json).unwrap(),
        WsClientMessage::PushUnsubscribe
    ));
}

#[test]
fn push_vapid_key_request_deserializes_frontend_wire_shape() {
    // SYNC: frontend send site push.ts `ws.send({ type: "PushVapidKeyRequest" })`.
    let json = r#"{"type":"PushVapidKeyRequest"}"#;
    assert!(matches!(
        serde_json::from_str::<WsClientMessage>(json).unwrap(),
        WsClientMessage::PushVapidKeyRequest
    ));
}

#[test]
fn run_target_deserializes_frontend_wire_shape() {
    // SYNC: frontend send site app.ts `ws.send({ type: "RunTarget", target, upload_ids })`.
    let json = r#"{"type":"RunTarget","target":"import","upload_ids":["550e8400-e29b-41d4-a716-446655440000","6ba7b810-9dad-11d1-80b4-00c04fd430c8"]}"#;
    match serde_json::from_str::<WsClientMessage>(json).unwrap() {
        WsClientMessage::RunTarget { target, upload_ids } => {
            assert_eq!(target, "import");
            assert_eq!(
                upload_ids,
                vec![
                    "550e8400-e29b-41d4-a716-446655440000",
                    "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
                ]
            );
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn push_subscribe_missing_field_rejected() {
    // `p256dh` absent. PushSubscribe fields carry no #[serde(default)], so a
    // missing required key must fail deserialization — pinning the strictness
    // contract that makes the fail2ban path fire only on genuinely malformed
    // input.
    let auth = fake_auth();
    let json = format!(
        r#"{{"type":"PushSubscribe","endpoint":"https://fcm.googleapis.com/fcm/send/eYxabc123","auth":"{auth}"}}"#
    );
    assert!(serde_json::from_str::<WsClientMessage>(&json).is_err());
}

#[test]
fn run_target_missing_field_rejected() {
    // `upload_ids` absent. No #[serde(default)], so this must fail to deserialize.
    let json = r#"{"type":"RunTarget","target":"import"}"#;
    assert!(serde_json::from_str::<WsClientMessage>(json).is_err());
}
