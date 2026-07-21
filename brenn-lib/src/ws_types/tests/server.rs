use super::super::*;

/// Serialize `msg` and assert it equals `expected` (a literal JSON string),
/// compared as `serde_json::Value`. `Value` equality ignores key order (a Rust
/// field reordering is not a wire break) but is exact on the key set: a field
/// omitted via `skip_serializing_if` must be absent from `expected`, and a
/// `None` serialized as `null` must appear as `null`. This pins presence/absence
/// semantics that ts-rs cannot check, without needing `PartialEq` on the types.
///
/// Also asserts the literal deserializes back into `T`, so each fixture pins
/// both wire directions in one place — a renamed field or an asymmetric serde
/// attribute (e.g. `skip_serializing_if` without a matching `#[serde(default)]`)
/// fails the fixture itself rather than only a distant smoke-vec entry.
fn assert_wire<T: serde::Serialize + serde::de::DeserializeOwned>(msg: &T, expected: &str) {
    let got = serde_json::to_value(msg).expect("serialize");
    let want: serde_json::Value = serde_json::from_str(expected).expect("parse expected literal");
    assert_eq!(
        got, want,
        "wire format mismatch\n got:  {got}\n want: {want}"
    );
    let _: T = serde_json::from_value(want).expect("expected literal must deserialize back into T");
}

#[test]
fn server_message_variants_serialize() {
    let messages = vec![
        WsServerMessage::StreamToken { token: "hi".into() },
        WsServerMessage::ThinkingToken {
            token: "hmm".into(),
        },
        WsServerMessage::AssistantMessage {
            content: "hello world".into(),
            seq: None,
        },
        WsServerMessage::PermissionRequest {
            request_id: "req_1".into(),
            tool_name: "Bash".into(),
            tool_input: serde_json::json!({"command": "ls"}),
            formatted_display: "<pre>ls</pre>".into(),
        },
        WsServerMessage::PermissionCancelled {
            request_id: "req_1".into(),
        },
        WsServerMessage::PermissionResolved {
            request_id: "req_2".into(),
            decision: PermissionDecision::Allow {
                updated_input: None,
            },
        },
        WsServerMessage::ToolCardRequest {
            request_id: "req_3".into(),
            tool_name: "mcp__pfin__reconcile".into(),
            tool_input: serde_json::json!({"proposals": []}),
            formatted_display: "<div>card</div>".into(),
        },
        WsServerMessage::ToolCardResolved {
            request_id: "req_3".into(),
            decision: ToolResponseDecision::Allow {
                updated_input: None,
            },
        },
        WsServerMessage::Status {
            state: CcState::Thinking,
        },
        WsServerMessage::Error {
            message: "CC died".into(),
        },
        WsServerMessage::ConversationList {
            conversations: vec![ConversationSummary {
                id: 1,
                title: Some("Test".into()),
                status: ConversationListStatus::Active,
                model: Some("sonnet".into()),
                updated_at: "2024-01-01".into(),
                message_count: 5,
                shared: false,
                owner: None,
            }],
        },
        WsServerMessage::ConversationSwitched {
            conversation_id: Some(42),
            state: CcState::Idle,
            is_owner: true,
            shared: false,
            reload: false,
        },
        WsServerMessage::ConversationSwitched {
            conversation_id: None,
            state: CcState::Idle,
            is_owner: true,
            shared: false,
            reload: false,
        },
        WsServerMessage::HistoryComplete {
            oldest_loaded_seq: None,
        },
        WsServerMessage::UserMessageEcho {
            text: "hello".into(),
            username: "alice".into(),
            timestamp: "2026-03-26T14:32:00+09:00".into(),
            attachments: vec![],
            selected_tasks: vec![],
            seq: None,
        },
        WsServerMessage::UserMessageEcho {
            text: "check this receipt".into(),
            username: "bob".into(),
            timestamp: "2026-03-26T14:33:00+09:00".into(),
            attachments: vec![AttachmentMeta {
                upload_id: "550e8400-e29b-41d4-a716-446655440000".into(),
                filename: "receipt.jpg".into(),
                media_type: "image/jpeg".into(),
                size: 123456,
            }],
            selected_tasks: vec![],
            seq: Some(7),
        },
        WsServerMessage::ArtifactContent {
            file_path: "docs/plan.md".into(),
            rendered_html: "<h1>Plan</h1>".into(),
            raw_content: "# Plan".into(),
            snapshot: None,
            seq: None,
        },
        WsServerMessage::ArtifactContent {
            file_path: "docs/plan.md".into(),
            rendered_html: "<h1>Plan</h1>".into(),
            raw_content: "# Plan".into(),
            snapshot: Some(SnapshotMetadata {
                message_id: 42,
                version: 1,
                total_versions: 2,
                seq: 5,
                stable_url: None,
            }),
            seq: Some(5),
        },
        WsServerMessage::ToolUseSummary {
            tool_name: "Read".into(),
            rendered_summary: "<div>src/main.rs</div>".into(),
            detail_html: None,
            seq: None,
        },
        WsServerMessage::ArtifactIndex {
            files: vec![ArtifactFileInfo {
                file_path: "docs/plan.md".into(),
                versions: vec![ArtifactVersionInfo {
                    message_id: 1,
                    version: 1,
                    seq: 3,
                }],
            }],
        },
        WsServerMessage::ArtifactIndex { files: vec![] },
        WsServerMessage::SessionStolen {
            message: "Session stolen by another tab".into(),
        },
        WsServerMessage::AppBusy {
            message: "App is in use".into(),
        },
        WsServerMessage::Welcome {
            username: "alice".into(),
            user_id: 1,
            multiuser: true,
            singleton: false,
            available_models: vec![ModelInfo {
                value: "sonnet".into(),
                display_name: "Sonnet".into(),
                description: "Sonnet 4.6".into(),
            }],
            default_model: "sonnet".into(),
            attachment_targets: vec![],
            pwa_push_enabled: false,
        },
        WsServerMessage::ModelsAvailable {
            available_models: vec![ModelInfo {
                value: "opus".into(),
                display_name: "Opus".into(),
                description: "Opus 4.6".into(),
            }],
        },
        WsServerMessage::PresenceUpdate {
            conversation_id: 42,
            users: vec![PresenceUser {
                username: "alice".into(),
            }],
        },
        WsServerMessage::SetLayout {
            layout: PaneLayout::SinglePane,
        },
        WsServerMessage::SetLayout {
            layout: PaneLayout::TwoColumn,
        },
        WsServerMessage::ApprovalRuleError {
            request_id: "req_1".into(),
            error: "invalid regex: unclosed group".into(),
        },
        WsServerMessage::PrivacyChanged {
            conversation_id: 42,
            shared: false,
        },
        WsServerMessage::TodoState {
            tasks: vec![TodoItem {
                path: "todo/buy-groceries.md".into(),
                tldr: "Buy groceries".into(),
                repo: Some("life".into()),
                domain: Some("example.org/personal".into()),
                status: Some("todo".into()),
                effective_date: chrono::NaiveDate::from_ymd_opt(2026, 4, 12).unwrap(),
                tentative_date: Some(chrono::NaiveDate::from_ymd_opt(2026, 4, 12).unwrap()),
                check_in_date: None,
                due_date: None,
                sort_order: Some(1.0),
                priority: Some(3),
                effort: Some("small".into()),
                rrule: None,
                labels: vec!["home".into()],
            }],
            lint_errors: vec![TodoLintError {
                path: "todo/broken.md".into(),
                message: "missing tldr".into(),
                repo: Some("life".into()),
            }],
            domains: Some(vec!["example.org/personal".into()]),
            today: chrono::NaiveDate::from_ymd_opt(2026, 4, 11).unwrap(),
        },
        WsServerMessage::TodoState {
            tasks: vec![],
            lint_errors: vec![],
            domains: None,
            today: chrono::NaiveDate::from_ymd_opt(2026, 4, 11).unwrap(),
        },
        WsServerMessage::TodoDoneResult {
            path: "todo/buy-groceries.md".into(),
            repo: None,
            success: true,
            error: None,
            completion_date: Some(chrono::NaiveDate::from_ymd_opt(2026, 4, 20).unwrap()),
            terminal: Some(false),
            next_check_in_date: None,
            next_due_date: None,
            already_done: None,
            existing_entry: None,
            comment_discarded: None,
        },
        WsServerMessage::TodoDoneResult {
            path: "todo/buy-groceries.md".into(),
            repo: None,
            success: false,
            error: Some("Stored anchor (2026-01-03) is far enough behind…".into()),
            completion_date: None,
            terminal: None,
            next_check_in_date: None,
            next_due_date: None,
            already_done: None,
            existing_entry: None,
            comment_discarded: None,
        },
        WsServerMessage::TodoMutationResult {
            path: "todo/buy-groceries.md".into(),
            repo: None,
            success: true,
            error: None,
        },
        WsServerMessage::SystemMessageBroadcast {
            rendered_html: "<details class=\"brenn-system\">sys</details>".into(),
            category: SystemMessageCategory::UiError,
            timestamp: "2026-03-26T14:32:00+09:00".into(),
            seq: None,
        },
        WsServerMessage::TargetResult {
            target: "import".into(),
            success: true,
            summary: "Imported 12 transactions".into(),
            detail: Some("ok".into()),
            files: vec!["statement.ofx".into()],
            seq: Some(9),
        },
        WsServerMessage::PermissionMode {
            mode: Some(PermissionModeValue::Auto),
        },
        WsServerMessage::PermissionMode {
            mode: Some(PermissionModeValue::Other("default".into())),
        },
        WsServerMessage::PermissionMode { mode: None },
        WsServerMessage::ContextUsage {
            usage_pct: 42,
            current_tokens: 84000,
            max_tokens: 200000,
            reminder_pct: 70,
            red_pct: 90,
            reminder_tokens: Some(140000),
            red_tokens: None,
        },
        WsServerMessage::HistoryPage {
            messages: vec![HistoryPageMessage {
                seq: 3,
                role: "user".into(),
                rendered_html: "<p>hi</p>".into(),
                timestamp: "2026-03-26T14:32:00+09:00".into(),
                username: Some("bob".into()),
                category: None,
                attachments: vec![],
            }],
            has_more: false,
        },
        WsServerMessage::CostUsage {
            last_turn_usd: 0.25,
            since_last_compaction_usd: 1.5,
            last_24h_usd: 12.5,
        },
        WsServerMessage::PushVapidKey {
            public_key_b64url: "BEl1example".into(),
        },
        WsServerMessage::PushEnabled { enabled: true },
    ];

    for msg in &messages {
        let json = serde_json::to_string(msg).unwrap();
        let parsed: WsServerMessage = serde_json::from_str(&json).unwrap();
        // Verify type tag is present.
        assert!(json.contains("\"type\":"));
        // Verify round-trip doesn't panic.
        let _ = serde_json::to_string(&parsed).unwrap();
    }
}

#[test]
fn cc_state_round_trip() {
    for state in [
        CcState::Idle,
        CcState::Thinking,
        CcState::AwaitingApproval,
        CcState::Error,
    ] {
        let json = serde_json::to_string(&state).unwrap();
        let parsed: CcState = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, state);
    }
}

#[test]
fn conversation_list_status_round_trip() {
    for status in [
        ConversationListStatus::Active,
        ConversationListStatus::Completed,
        ConversationListStatus::Error,
    ] {
        let json = serde_json::to_string(&status).unwrap();
        let parsed: ConversationListStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, status);
    }
}

#[test]
fn conversation_switched_reload_false_omitted_from_json() {
    let msg = WsServerMessage::ConversationSwitched {
        conversation_id: Some(1),
        state: CcState::Idle,
        is_owner: true,
        shared: false,
        reload: false,
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert!(
        !json.contains("reload"),
        "reload: false should be omitted via skip_serializing_if, got: {json}"
    );
}

#[test]
fn conversation_switched_reload_true_present_in_json() {
    let msg = WsServerMessage::ConversationSwitched {
        conversation_id: Some(1),
        state: CcState::Idle,
        is_owner: true,
        shared: false,
        reload: true,
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert!(
        json.contains("\"reload\":true"),
        "reload: true should be present, got: {json}"
    );
}

#[test]
fn conversation_switched_reload_defaults_to_false_on_deserialize() {
    // Simulate an old message without the reload field.
    let json = r#"{"type":"ConversationSwitched","conversation_id":1,"state":"Idle","is_owner":true,"shared":false}"#;
    let msg: WsServerMessage = serde_json::from_str(json).unwrap();
    match msg {
        WsServerMessage::ConversationSwitched { reload, .. } => {
            assert!(!reload, "missing reload field should default to false");
        }
        _ => panic!("expected ConversationSwitched"),
    }
}

#[test]
fn system_message_broadcast_wire_format() {
    // Live broadcast: `seq` omitted (skip_serializing_if). `category`
    // serializes as the bare PascalCase variant name (no serde rename attrs on
    // SystemMessageCategory) — the kebab-case seen in the frontend is a CSS
    // transformation, not the wire format.
    let live = WsServerMessage::SystemMessageBroadcast {
        rendered_html: "<details class=\"brenn-system\">card</details>".into(),
        category: SystemMessageCategory::UiError,
        timestamp: "2026-03-26T14:32:00+09:00".into(),
        seq: None,
    };
    assert_wire(
        &live,
        r#"{"type":"SystemMessageBroadcast","rendered_html":"<details class=\"brenn-system\">card</details>","category":"UiError","timestamp":"2026-03-26T14:32:00+09:00"}"#,
    );
    // Replay: `seq` present.
    let replay = WsServerMessage::SystemMessageBroadcast {
        rendered_html: "<details class=\"brenn-system\">card</details>".into(),
        category: SystemMessageCategory::EventDrain,
        timestamp: "2026-03-26T14:32:00+09:00".into(),
        seq: Some(7),
    };
    assert_wire(
        &replay,
        r#"{"type":"SystemMessageBroadcast","rendered_html":"<details class=\"brenn-system\">card</details>","category":"EventDrain","timestamp":"2026-03-26T14:32:00+09:00","seq":7}"#,
    );
}

#[test]
fn target_result_wire_format() {
    // Success: `detail` and `seq` present.
    let success = WsServerMessage::TargetResult {
        target: "import".into(),
        success: true,
        summary: "Imported 12 transactions".into(),
        detail: Some("stdout: ok\nstderr:".into()),
        files: vec!["statement.ofx".into()],
        seq: Some(9),
    };
    assert_wire(
        &success,
        r#"{"type":"TargetResult","target":"import","success":true,"summary":"Imported 12 transactions","detail":"stdout: ok\nstderr:","files":["statement.ofx"],"seq":9}"#,
    );
    // Failure: `detail` and `seq` both absent (skip_serializing_if).
    let failure = WsServerMessage::TargetResult {
        target: "import".into(),
        success: false,
        summary: "Import failed".into(),
        detail: None,
        files: vec!["statement.ofx".into()],
        seq: None,
    };
    assert_wire(
        &failure,
        r#"{"type":"TargetResult","target":"import","success":false,"summary":"Import failed","files":["statement.ofx"]}"#,
    );
}

#[test]
fn permission_mode_wire_format() {
    // No skip_serializing_if on `mode`: None must serialize as `null`.
    assert_wire(
        &WsServerMessage::PermissionMode {
            mode: Some(PermissionModeValue::Auto),
        },
        r#"{"type":"PermissionMode","mode":"auto"}"#,
    );
    assert_wire(
        &WsServerMessage::PermissionMode {
            mode: Some(PermissionModeValue::Other("default".into())),
        },
        r#"{"type":"PermissionMode","mode":"other"}"#,
    );
    assert_wire(
        &WsServerMessage::PermissionMode { mode: None },
        r#"{"type":"PermissionMode","mode":null}"#,
    );
}

#[test]
fn permission_mode_other_deserializes_lossy() {
    // `"other"` on the wire deserializes to Other("other"); the original
    // unknown string is not recoverable from the wire (Other is lossy on
    // serialize). Assert serialize and deserialize as separate directed cases
    // — never round-trip equality on Other.
    let json = r#"{"type":"PermissionMode","mode":"other"}"#;
    match serde_json::from_str::<WsServerMessage>(json).unwrap() {
        WsServerMessage::PermissionMode { mode } => {
            assert_eq!(mode, Some(PermissionModeValue::Other("other".into())));
        }
        _ => panic!("expected PermissionMode"),
    }
}

#[test]
fn context_usage_wire_format() {
    let with_thresholds = WsServerMessage::ContextUsage {
        usage_pct: 42,
        current_tokens: 84000,
        max_tokens: 200000,
        reminder_pct: 70,
        red_pct: 90,
        reminder_tokens: Some(140000),
        red_tokens: Some(180000),
    };
    assert_wire(
        &with_thresholds,
        r#"{"type":"ContextUsage","usage_pct":42,"current_tokens":84000,"max_tokens":200000,"reminder_pct":70,"red_pct":90,"reminder_tokens":140000,"red_tokens":180000}"#,
    );
    // No skip_serializing_if on the threshold fields: None serializes as `null`,
    // not omitted (unlike `seq` elsewhere).
    let no_thresholds = WsServerMessage::ContextUsage {
        usage_pct: 10,
        current_tokens: 20000,
        max_tokens: 200000,
        reminder_pct: 70,
        red_pct: 90,
        reminder_tokens: None,
        red_tokens: None,
    };
    assert_wire(
        &no_thresholds,
        r#"{"type":"ContextUsage","usage_pct":10,"current_tokens":20000,"max_tokens":200000,"reminder_pct":70,"red_pct":90,"reminder_tokens":null,"red_tokens":null}"#,
    );
}

#[test]
fn history_page_wire_format() {
    let page = WsServerMessage::HistoryPage {
        messages: vec![
            // System-origin row: `category` present; `username`/`attachments` absent.
            HistoryPageMessage {
                seq: 3,
                role: "user".into(),
                rendered_html: "<details class=\"brenn-system\">sys</details>".into(),
                timestamp: "2026-03-26T14:32:00+09:00".into(),
                username: None,
                category: Some(SystemMessageCategory::EventDrain),
                attachments: vec![],
            },
            // Chat user row: `username` + `attachments` present; `category` absent.
            HistoryPageMessage {
                seq: 4,
                role: "user".into(),
                rendered_html: "<p>check this receipt</p>".into(),
                timestamp: "2026-03-26T14:33:00+09:00".into(),
                username: Some("bob".into()),
                category: None,
                attachments: vec![AttachmentMeta {
                    upload_id: "550e8400-e29b-41d4-a716-446655440000".into(),
                    filename: "receipt.jpg".into(),
                    media_type: "image/jpeg".into(),
                    size: 123456,
                }],
            },
        ],
        has_more: true,
    };
    assert_wire(
        &page,
        r#"{"type":"HistoryPage","messages":[{"seq":3,"role":"user","rendered_html":"<details class=\"brenn-system\">sys</details>","timestamp":"2026-03-26T14:32:00+09:00","category":"EventDrain"},{"seq":4,"role":"user","rendered_html":"<p>check this receipt</p>","timestamp":"2026-03-26T14:33:00+09:00","username":"bob","attachments":[{"upload_id":"550e8400-e29b-41d4-a716-446655440000","filename":"receipt.jpg","media_type":"image/jpeg","size":123456}]}],"has_more":true}"#,
    );
}

#[test]
fn cost_usage_wire_format() {
    // Exactly-representable f64 values so `Value::Number` comparison is exact.
    let msg = WsServerMessage::CostUsage {
        last_turn_usd: 0.25,
        since_last_compaction_usd: 1.5,
        last_24h_usd: 12.5,
    };
    assert_wire(
        &msg,
        r#"{"type":"CostUsage","last_turn_usd":0.25,"since_last_compaction_usd":1.5,"last_24h_usd":12.5}"#,
    );
}

#[test]
fn push_vapid_key_wire_format() {
    assert_wire(
        &WsServerMessage::PushVapidKey {
            public_key_b64url: "BEl1qL0lXx3fZ9example87charsbase64urlpublickeyvalue".into(),
        },
        r#"{"type":"PushVapidKey","public_key_b64url":"BEl1qL0lXx3fZ9example87charsbase64urlpublickeyvalue"}"#,
    );
}

#[test]
fn push_enabled_wire_format() {
    assert_wire(
        &WsServerMessage::PushEnabled { enabled: true },
        r#"{"type":"PushEnabled","enabled":true}"#,
    );
    assert_wire(
        &WsServerMessage::PushEnabled { enabled: false },
        r#"{"type":"PushEnabled","enabled":false}"#,
    );
}
