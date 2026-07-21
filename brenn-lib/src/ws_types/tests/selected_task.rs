use super::super::*;

#[test]
fn selected_task_serde_round_trip() {
    let task = SelectedTask {
        task_ref: "life:todo/buy-groceries.md".into(),
    };
    let json = serde_json::to_string(&task).unwrap();
    // Field should serialize as "ref", not "task_ref".
    assert!(
        json.contains("\"ref\":"),
        "task_ref should serialize as 'ref': {json}"
    );
    assert!(
        !json.contains("task_ref"),
        "should not contain 'task_ref': {json}"
    );
    assert!(
        !json.contains("tldr"),
        "SelectedTask should not carry tldr: {json}"
    );
    let parsed: SelectedTask = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.task_ref, "life:todo/buy-groceries.md");
}

#[test]
fn selected_task_deserializes_from_ref_key() {
    // Frontend sends "ref" not "task_ref".
    let json = r#"{"ref":"todo/task.md"}"#;
    let task: SelectedTask = serde_json::from_str(json).unwrap();
    assert_eq!(task.task_ref, "todo/task.md");
}

#[test]
fn send_message_with_selected_tasks_round_trip() {
    let msg = WsClientMessage::SendMessage {
        text: "Which first?".into(),
        attachments: vec![],
        model: None,
        selected_tasks: vec![
            SelectedTask {
                task_ref: "life:todo/buy-groceries.md".into(),
            },
            SelectedTask {
                task_ref: "todo/call-dentist.md".into(),
            },
        ],
    };
    let json = serde_json::to_string(&msg).unwrap();
    let parsed: WsClientMessage = serde_json::from_str(&json).unwrap();
    match parsed {
        WsClientMessage::SendMessage { selected_tasks, .. } => {
            assert_eq!(selected_tasks.len(), 2);
            assert_eq!(selected_tasks[0].task_ref, "life:todo/buy-groceries.md");
            assert_eq!(selected_tasks[1].task_ref, "todo/call-dentist.md");
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn user_message_echo_omits_empty_selected_tasks() {
    let msg = WsServerMessage::UserMessageEcho {
        text: "hello".into(),
        username: "alice".into(),
        timestamp: "2026-04-15T10:00:00Z".into(),
        attachments: vec![],
        selected_tasks: vec![],
        seq: None,
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert!(
        !json.contains("selected_tasks"),
        "empty selected_tasks should be omitted: {json}"
    );
}

#[test]
fn user_message_echo_includes_non_empty_selected_tasks() {
    let msg = WsServerMessage::UserMessageEcho {
        text: "Which first?".into(),
        username: "alice".into(),
        timestamp: "2026-04-15T10:00:00Z".into(),
        attachments: vec![],
        selected_tasks: vec![SelectedTask {
            task_ref: "todo/task.md".into(),
        }],
        seq: None,
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert!(
        json.contains("selected_tasks"),
        "non-empty selected_tasks should be present: {json}"
    );
    assert!(json.contains("\"ref\":"), "should use 'ref' key: {json}");

    // Deserialize back.
    let parsed: WsServerMessage = serde_json::from_str(&json).unwrap();
    match parsed {
        WsServerMessage::UserMessageEcho { selected_tasks, .. } => {
            assert_eq!(selected_tasks.len(), 1);
            assert_eq!(selected_tasks[0].task_ref, "todo/task.md");
        }
        _ => panic!("expected UserMessageEcho"),
    }
}

#[test]
fn user_message_echo_without_selected_tasks_deserializes() {
    // Old messages in the DB or from old servers won't have selected_tasks.
    let json = r#"{"type":"UserMessageEcho","text":"hi","username":"bob","timestamp":"2026-04-15T10:00:00Z"}"#;
    let parsed: WsServerMessage = serde_json::from_str(json).unwrap();
    match parsed {
        WsServerMessage::UserMessageEcho { selected_tasks, .. } => {
            assert!(
                selected_tasks.is_empty(),
                "missing selected_tasks should default to empty vec"
            );
        }
        _ => panic!("expected UserMessageEcho"),
    }
}
