use super::super::*;

#[test]
fn permission_allow_without_input_round_trip() {
    let decision = PermissionDecision::Allow {
        updated_input: None,
    };
    let json = serde_json::to_string(&decision).unwrap();
    // skip_serializing_if should omit updated_input when None.
    assert!(
        !json.contains("updated_input"),
        "None updated_input should be omitted, got: {json}"
    );
    assert!(json.contains("\"decision\":\"Allow\""));

    let parsed: PermissionDecision = serde_json::from_str(&json).unwrap();
    match parsed {
        PermissionDecision::Allow { updated_input } => {
            assert!(updated_input.is_none());
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn tool_response_allow_without_input_round_trip() {
    let decision = ToolResponseDecision::Allow {
        updated_input: None,
    };
    let json = serde_json::to_string(&decision).unwrap();
    assert!(
        !json.contains("updated_input"),
        "None updated_input should be omitted, got: {json}"
    );
    assert!(json.contains("\"decision\":\"Allow\""));

    let parsed: ToolResponseDecision = serde_json::from_str(&json).unwrap();
    match parsed {
        ToolResponseDecision::Allow { updated_input } => {
            assert!(updated_input.is_none());
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn permission_always_allow_round_trip() {
    let decision = PermissionDecision::AlwaysAllow {
        patterns: vec!["git status\\b.*".into()],
        scope: RuleScope::Permanent,
        tool_name: "Bash".into(),
    };
    let json = serde_json::to_string(&decision).unwrap();
    assert!(json.contains("\"decision\":\"AlwaysAllow\""));
    assert!(json.contains("\"patterns\""));
    assert!(json.contains("\"scope\":\"Permanent\""));
    assert!(json.contains("\"tool_name\":\"Bash\""));

    let parsed: PermissionDecision = serde_json::from_str(&json).unwrap();
    match parsed {
        PermissionDecision::AlwaysAllow {
            patterns,
            scope,
            tool_name,
        } => {
            assert_eq!(patterns, vec!["git status\\b.*"]);
            assert_eq!(scope, RuleScope::Permanent);
            assert_eq!(tool_name, "Bash");
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn permission_always_allow_conversation_scope() {
    let json = r#"{"decision":"AlwaysAllow","patterns":["cargo test\\b.*"],"scope":"Conversation","tool_name":"Bash"}"#;
    let parsed: PermissionDecision = serde_json::from_str(json).unwrap();
    match parsed {
        PermissionDecision::AlwaysAllow { scope, .. } => {
            assert_eq!(scope, RuleScope::Conversation);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn permission_allow_with_input_round_trip() {
    let input = serde_json::json!({
        "questions": [{"question": "test", "header": "Q", "options": [], "multiSelect": false}],
        "answers": {"test": "yes"}
    });
    let decision = PermissionDecision::Allow {
        updated_input: Some(input.clone()),
    };
    let json = serde_json::to_string(&decision).unwrap();
    assert!(json.contains("updated_input"));
    assert!(json.contains("answers"));

    let parsed: PermissionDecision = serde_json::from_str(&json).unwrap();
    match parsed {
        PermissionDecision::Allow { updated_input } => {
            let ui = updated_input.expect("should have updated_input");
            assert_eq!(ui["answers"]["test"], "yes");
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn tool_response_allow_with_input_round_trip() {
    let input = serde_json::json!({ "selected": 2 });
    let decision = ToolResponseDecision::Allow {
        updated_input: Some(input.clone()),
    };
    let json = serde_json::to_string(&decision).unwrap();
    assert!(json.contains("updated_input"));
    assert!(json.contains("selected"));

    let parsed: ToolResponseDecision = serde_json::from_str(&json).unwrap();
    match parsed {
        ToolResponseDecision::Allow { updated_input } => {
            let ui = updated_input.expect("should have updated_input");
            assert_eq!(ui["selected"], 2);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn tool_response_deny_without_reason_round_trip() {
    let decision = ToolResponseDecision::Deny { reason: None };
    let json = serde_json::to_string(&decision).unwrap();
    assert!(json.contains("\"decision\":\"Deny\""));

    let parsed: ToolResponseDecision = serde_json::from_str(&json).unwrap();
    match parsed {
        ToolResponseDecision::Deny { reason } => assert!(reason.is_none()),
        _ => panic!("wrong variant"),
    }
}

#[test]
fn permission_mode_value_round_trip() {
    // Named variant serializes to the expected bare string.
    let json = serde_json::to_string(&PermissionModeValue::Auto).unwrap();
    assert_eq!(json, r#""auto""#);
    // Deserializes back to the named variant.
    let parsed: PermissionModeValue = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, PermissionModeValue::Auto);
    // Unknown string → Other (raw string retained for backend alerting).
    let other: PermissionModeValue = serde_json::from_str(r#""strict""#).unwrap();
    assert_eq!(other, PermissionModeValue::Other("strict".into()));
    // Other serializes as "other" on the wire (not the raw string),
    // matching the closed TS union declared in the #[ts(type)] override.
    let other_json = serde_json::to_string(&other).unwrap();
    assert_eq!(other_json, r#""other""#);
}
