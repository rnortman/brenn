use super::super::*;

/// TodoItem deserializes from graf's JSON where optional fields are
/// absent. `effective_date` is non-optional and must be present — the
/// "minimal" fixture now includes it.
#[test]
fn todo_item_deserializes_with_minimal_fields() {
    let json = r#"{"path":"todo/foo.md","tldr":"Buy groceries","effective_date":"2026-04-12"}"#;
    let item: TodoItem = serde_json::from_str(json).unwrap();
    assert_eq!(item.path, "todo/foo.md");
    assert_eq!(item.tldr, "Buy groceries");
    assert_eq!(
        item.effective_date,
        chrono::NaiveDate::from_ymd_opt(2026, 4, 12).unwrap()
    );
    assert!(item.repo.is_none());
    assert!(item.domain.is_none());
    assert!(item.status.is_none());
    assert!(item.tentative_date.is_none());
    assert!(item.check_in_date.is_none());
    assert!(item.due_date.is_none());
    assert!(item.sort_order.is_none());
    assert!(item.priority.is_none());
    assert!(item.effort.is_none());
    assert!(item.rrule.is_none());
    assert!(item.labels.is_empty());
}

/// Missing `effective_date` is a schema violation. The boundary between
/// graf and brenn enforces the invariant at deserialization — this is
/// the fail-fast site. Regression test against drift.
#[test]
fn todo_item_rejects_missing_effective_date() {
    let json = r#"{"path":"todo/foo.md","tldr":"Buy groceries"}"#;
    let err = serde_json::from_str::<TodoItem>(json)
        .expect_err("missing effective_date must fail deserialization");
    assert!(
        err.to_string().contains("effective_date"),
        "error should reference effective_date, got: {err}"
    );
}

/// Null `effective_date` is rejected (invariant cannot be subverted by
/// emitting explicit null instead of omitting the field).
#[test]
fn todo_item_rejects_null_effective_date() {
    let json = r#"{"path":"todo/foo.md","tldr":"Buy groceries","effective_date":null}"#;
    serde_json::from_str::<TodoItem>(json)
        .expect_err("null effective_date must fail deserialization");
}

/// Verify TodoItem deserializes from graf's full JSON output.
#[test]
fn todo_item_deserializes_with_all_fields() {
    let json = r#"{
        "path": "todo/foo.md",
        "tldr": "Buy groceries",
        "repo": "life",
        "domain": "example.org/personal",
        "status": "todo",
        "effective_date": "2026-04-12",
        "tentative_date": "2026-04-12",
        "check_in_date": "2026-04-10",
        "due_date": "2026-04-15",
        "sort_order": 1.5,
        "priority": 2,
        "effort": "small",
        "rrule": "FREQ=WEEKLY",
        "labels": ["home", "errands"]
    }"#;
    let item: TodoItem = serde_json::from_str(json).unwrap();
    assert_eq!(item.repo.as_deref(), Some("life"));
    assert_eq!(item.domain.as_deref(), Some("example.org/personal"));
    assert_eq!(item.status.as_deref(), Some("todo"));
    assert_eq!(
        item.effective_date,
        chrono::NaiveDate::from_ymd_opt(2026, 4, 12).unwrap()
    );
    assert_eq!(item.priority, Some(2));
    assert_eq!(item.effort.as_deref(), Some("small"));
    assert_eq!(item.rrule.as_deref(), Some("FREQ=WEEKLY"));
    assert_eq!(item.labels, vec!["home", "errands"]);
}

/// TodoItem serialization omits None/empty optional fields (matches
/// graf's convention); `effective_date` always serializes because it
/// is non-optional.
#[test]
fn todo_item_omits_absent_fields_in_json() {
    let item = TodoItem {
        path: "todo/foo.md".into(),
        tldr: "Test".into(),
        repo: None,
        domain: None,
        status: None,
        effective_date: chrono::NaiveDate::from_ymd_opt(2026, 4, 12).unwrap(),
        tentative_date: None,
        check_in_date: None,
        due_date: None,
        sort_order: None,
        priority: None,
        effort: None,
        rrule: None,
        labels: vec![],
    };
    let json = serde_json::to_string(&item).unwrap();
    assert!(!json.contains("repo"), "None fields should be omitted");
    assert!(!json.contains("domain"), "None fields should be omitted");
    assert!(!json.contains("status"), "None fields should be omitted");
    assert!(!json.contains("labels"), "empty vec should be omitted");
    assert!(json.contains("path"));
    assert!(json.contains("tldr"));
    assert!(
        json.contains("effective_date"),
        "effective_date is non-optional and must serialize"
    );
}

/// TodoLintError deserializes without repo field (single-repo / no manifest).
#[test]
fn todo_lint_error_deserializes_without_repo() {
    let json = r#"{"path":"todo/broken.md","message":"missing tldr"}"#;
    let err: TodoLintError = serde_json::from_str(json).unwrap();
    assert_eq!(err.path, "todo/broken.md");
    assert_eq!(err.message, "missing tldr");
    assert!(err.repo.is_none());
}

/// TodoLintError deserializes with repo field (multi-repo manifest).
#[test]
fn todo_lint_error_deserializes_with_repo() {
    let json = r#"{"path":"todo/broken.md","message":"missing tldr","repo":"life"}"#;
    let err: TodoLintError = serde_json::from_str(json).unwrap();
    assert_eq!(err.repo.as_deref(), Some("life"));
}

/// TodoState without domains field deserializes (domains is optional).
/// `today` is required — omitted only in legacy payloads pre-dating
/// graf-user-tz.md; those wouldn't round-trip anyway.
#[test]
fn todo_state_without_domains_deserializes() {
    let json = r#"{"type":"TodoState","tasks":[],"lint_errors":[],"today":"2026-04-11"}"#;
    let msg: WsServerMessage = serde_json::from_str(json).unwrap();
    match msg {
        WsServerMessage::TodoState { domains, today, .. } => {
            assert!(domains.is_none(), "missing domains should default to None");
            assert_eq!(today, chrono::NaiveDate::from_ymd_opt(2026, 4, 11).unwrap());
        }
        _ => panic!("expected TodoState"),
    }
}

/// TodoState with domains=None omits it from JSON.
#[test]
fn todo_state_none_domains_omitted_from_json() {
    let msg = WsServerMessage::TodoState {
        tasks: vec![],
        lint_errors: vec![],
        domains: None,
        today: chrono::NaiveDate::from_ymd_opt(2026, 4, 11).unwrap(),
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert!(
        !json.contains("domains"),
        "None domains should be omitted, got: {json}"
    );
    assert!(
        json.contains("\"today\":\"2026-04-11\""),
        "today should be serialized, got: {json}"
    );
}

/// TodoDone without repo field deserializes (backward compat with old frontends).
#[test]
fn todo_done_without_repo_deserializes() {
    let json = r#"{"type":"TodoDone","path":"todo/foo.md","completion_date":"2026-04-20"}"#;
    let msg: WsClientMessage = serde_json::from_str(json).unwrap();
    match msg {
        WsClientMessage::TodoDone {
            path,
            repo,
            completion_date,
        } => {
            assert_eq!(path, "todo/foo.md");
            assert!(repo.is_none(), "missing repo should default to None");
            assert_eq!(
                completion_date,
                chrono::NaiveDate::from_ymd_opt(2026, 4, 20).unwrap(),
            );
        }
        _ => panic!("expected TodoDone"),
    }
}

/// TodoDone without completion_date must fail to deserialize — field is required.
#[test]
fn todo_done_missing_completion_date_deserialize_fails() {
    let json = r#"{"type":"TodoDone","path":"todo/foo.md"}"#;
    let result: Result<WsClientMessage, _> = serde_json::from_str(json);
    assert!(
        result.is_err(),
        "missing completion_date must fail to deserialize"
    );
}

/// TodoSchedule without repo field deserializes.
#[test]
fn todo_schedule_without_repo_deserializes() {
    let json = r#"{"type":"TodoSchedule","path":"todo/foo.md","date":"2026-04-20"}"#;
    let msg: WsClientMessage = serde_json::from_str(json).unwrap();
    match msg {
        WsClientMessage::TodoSchedule { repo, .. } => {
            assert!(repo.is_none());
        }
        _ => panic!("expected TodoSchedule"),
    }
}

/// TodoReorder without repo field deserializes.
#[test]
fn todo_reorder_without_repo_deserializes() {
    let json = r#"{"type":"TodoReorder","path":"todo/foo.md","after":{"path":"todo/bar.md"}}"#;
    let msg: WsClientMessage = serde_json::from_str(json).unwrap();
    match msg {
        WsClientMessage::TodoReorder {
            path,
            repo,
            after,
            before,
        } => {
            assert_eq!(path, "todo/foo.md");
            assert!(repo.is_none());
            assert_eq!(after.unwrap().path, "todo/bar.md");
            assert!(before.is_none());
        }
        _ => panic!("expected TodoReorder"),
    }
}

/// TodoReorder with both anchors deserializes.
#[test]
fn todo_reorder_with_both_anchors_deserializes() {
    let json = r#"{"type":"TodoReorder","path":"todo/foo.md","repo":"life","after":{"path":"todo/a.md","repo":"life"},"before":{"path":"todo/b.md","repo":"life"}}"#;
    let msg: WsClientMessage = serde_json::from_str(json).unwrap();
    match msg {
        WsClientMessage::TodoReorder {
            repo,
            after,
            before,
            ..
        } => {
            assert_eq!(repo.as_deref(), Some("life"));
            let after = after.unwrap();
            assert_eq!(after.path, "todo/a.md");
            assert_eq!(after.repo.as_deref(), Some("life"));
            let before = before.unwrap();
            assert_eq!(before.path, "todo/b.md");
            assert_eq!(before.repo.as_deref(), Some("life"));
        }
        _ => panic!("expected TodoReorder"),
    }
}

/// TodoReorder with only `before` anchor (no `after`).
#[test]
fn todo_reorder_before_only_deserializes() {
    let json = r#"{"type":"TodoReorder","path":"todo/foo.md","before":{"path":"todo/bar.md"}}"#;
    let msg: WsClientMessage = serde_json::from_str(json).unwrap();
    match msg {
        WsClientMessage::TodoReorder { after, before, .. } => {
            assert!(after.is_none(), "after should be None when omitted");
            assert_eq!(before.unwrap().path, "todo/bar.md");
        }
        _ => panic!("expected TodoReorder"),
    }
}

/// TodoReorder with no anchors at all deserializes (validation is server-side, not serde).
#[test]
fn todo_reorder_no_anchors_deserializes() {
    let json = r#"{"type":"TodoReorder","path":"todo/foo.md"}"#;
    let msg: WsClientMessage = serde_json::from_str(json).unwrap();
    match msg {
        WsClientMessage::TodoReorder { after, before, .. } => {
            assert!(after.is_none());
            assert!(before.is_none());
        }
        _ => panic!("expected TodoReorder"),
    }
}

/// TodoDoneResult without error field deserializes correctly
/// (tests the #[serde(default)] fix).
#[test]
fn todo_done_result_without_error_deserializes() {
    let json = r#"{"type":"TodoDoneResult","path":"todo/foo.md","success":true}"#;
    let msg: WsServerMessage = serde_json::from_str(json).unwrap();
    match msg {
        WsServerMessage::TodoDoneResult {
            path,
            success,
            error,
            ..
        } => {
            assert_eq!(path, "todo/foo.md");
            assert!(success);
            assert!(
                error.is_none(),
                "missing error field should default to None"
            );
        }
        _ => panic!("expected TodoDoneResult"),
    }
}

/// TodoDoneResult success=true omits error field from JSON.
#[test]
fn todo_done_result_success_omits_error() {
    let msg = WsServerMessage::TodoDoneResult {
        path: "todo/foo.md".into(),
        repo: None,
        success: true,
        error: None,
        completion_date: None,
        terminal: None,
        next_check_in_date: None,
        next_due_date: None,
        already_done: None,
        existing_entry: None,
        comment_discarded: None,
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert!(
        !json.contains("error"),
        "None error should be omitted, got: {json}"
    );
}

/// TodoDoneResult with repo: Some(...) serializes "repo" field on the wire
/// and round-trips back. The frontend's exact-key lookup depends on this.
#[test]
fn todo_done_result_repo_serializes_and_round_trips() {
    let msg = WsServerMessage::TodoDoneResult {
        path: "todo/foo.md".into(),
        repo: Some("life".into()),
        success: true,
        error: None,
        completion_date: None,
        terminal: None,
        next_check_in_date: None,
        next_due_date: None,
        already_done: None,
        existing_entry: None,
        comment_discarded: None,
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert!(
        json.contains("\"repo\":\"life\""),
        "repo field must appear in JSON, got: {json}"
    );
    let back: WsServerMessage = serde_json::from_str(&json).unwrap();
    match back {
        WsServerMessage::TodoDoneResult { repo, .. } => {
            assert_eq!(repo.as_deref(), Some("life"));
        }
        _ => panic!("wrong variant after round-trip"),
    }
}

/// TodoMutationResult round-trips correctly and does not contain done-only fields.
#[test]
fn todo_mutation_result_round_trips() {
    let msg = WsServerMessage::TodoMutationResult {
        path: "todo/foo.md".into(),
        repo: Some("life".into()),
        success: true,
        error: None,
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert!(
        json.contains("\"type\":\"TodoMutationResult\""),
        "wrong type tag, got: {json}"
    );
    assert!(
        json.contains("\"repo\":\"life\""),
        "repo field must appear in JSON, got: {json}"
    );
    // Done-only fields must not appear on the wire.
    for done_field in &[
        "completion_date",
        "terminal",
        "next_check_in_date",
        "next_due_date",
        "already_done",
        "existing_entry",
        "comment_discarded",
    ] {
        assert!(
            !json.contains(done_field),
            "done-only field {done_field} must not appear in TodoMutationResult JSON: {json}"
        );
    }
    let back: WsServerMessage = serde_json::from_str(&json).unwrap();
    match back {
        WsServerMessage::TodoMutationResult {
            path,
            repo,
            success,
            error,
        } => {
            assert_eq!(path, "todo/foo.md");
            assert_eq!(repo.as_deref(), Some("life"));
            assert!(success);
            assert!(error.is_none());
        }
        _ => panic!("wrong variant after round-trip"),
    }
}

#[test]
fn todo_mutation_result_error_serializes() {
    // Confirm that `error: Some(...)` on failure is not suppressed by skip_serializing_if.
    let msg = WsServerMessage::TodoMutationResult {
        path: "todo/bar.md".into(),
        repo: None,
        success: false,
        error: Some("oops".into()),
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert!(
        json.contains("\"error\":\"oops\""),
        "error field must appear in JSON on failure, got: {json}"
    );
    assert!(
        json.contains("\"success\":false"),
        "success must be false, got: {json}"
    );
}

#[test]
fn todo_mutation_result_without_error_deserializes() {
    // Confirm that absent `error` key deserializes to None (not a panic).
    let json = r#"{"type":"TodoMutationResult","path":"todo/bar.md","success":false}"#;
    let msg: WsServerMessage = serde_json::from_str(json).unwrap();
    match msg {
        WsServerMessage::TodoMutationResult {
            path,
            repo,
            success,
            error,
        } => {
            assert_eq!(path, "todo/bar.md");
            assert!(repo.is_none());
            assert!(!success);
            assert!(
                error.is_none(),
                "error should be None when absent from JSON"
            );
        }
        _ => panic!("wrong variant"),
    }
}

/// `todo_done_failure` constructor sets success=false, populates error,
/// leaves all done-specific fields None, and threads path/repo correctly.
#[test]
fn todo_done_failure_constructor_fields() {
    // With repo.
    let msg = WsServerMessage::todo_done_failure("todo/task.md", Some("life"), "bad".into());
    match msg {
        WsServerMessage::TodoDoneResult {
            path,
            repo,
            success,
            error,
            completion_date,
            terminal,
            next_check_in_date,
            next_due_date,
            already_done,
            existing_entry,
            comment_discarded,
        } => {
            assert_eq!(path, "todo/task.md");
            assert_eq!(repo.as_deref(), Some("life"));
            assert!(!success, "success must be false on failure");
            assert_eq!(error.as_deref(), Some("bad"));
            assert!(
                completion_date.is_none(),
                "completion_date must be None on failure"
            );
            assert!(terminal.is_none(), "terminal must be None on failure");
            assert!(
                next_check_in_date.is_none(),
                "next_check_in_date must be None on failure"
            );
            assert!(
                next_due_date.is_none(),
                "next_due_date must be None on failure"
            );
            assert!(
                already_done.is_none(),
                "already_done must be None on failure"
            );
            assert!(
                existing_entry.is_none(),
                "existing_entry must be None on failure"
            );
            assert!(
                comment_discarded.is_none(),
                "comment_discarded must be None on failure"
            );
        }
        _ => panic!("wrong variant"),
    }

    // Without repo.
    let msg_no_repo = WsServerMessage::todo_done_failure("todo/other.md", None, "oops".into());
    match msg_no_repo {
        WsServerMessage::TodoDoneResult {
            path,
            repo,
            success,
            error,
            ..
        } => {
            assert_eq!(path, "todo/other.md");
            assert!(repo.is_none(), "repo must be None when not provided");
            assert!(!success);
            assert_eq!(error.as_deref(), Some("oops"));
        }
        _ => panic!("wrong variant"),
    }
}

/// `todo_done_success` constructor sets success=true, error=None, and
/// threads all done-specific fields through to the variant correctly.
#[test]
fn todo_done_success_constructor_fields() {
    use chrono::NaiveDate;

    let date = NaiveDate::from_ymd_opt(2026, 5, 25).unwrap();
    // With all fields populated.
    let msg = WsServerMessage::todo_done_success(
        "todo/task.md",
        Some("life"),
        Some(date),
        Some(false),
        Some(date),
        Some(date),
        Some(true),
        None, // existing_entry: None is a valid success state
        Some(false),
    );
    match msg {
        WsServerMessage::TodoDoneResult {
            path,
            repo,
            success,
            error,
            completion_date,
            terminal,
            next_check_in_date,
            next_due_date,
            already_done,
            existing_entry,
            comment_discarded,
        } => {
            assert_eq!(path, "todo/task.md");
            assert_eq!(repo.as_deref(), Some("life"));
            assert!(success, "success must be true");
            assert!(error.is_none(), "error must be None on success");
            assert_eq!(completion_date, Some(date));
            assert_eq!(terminal, Some(false));
            assert_eq!(next_check_in_date, Some(date));
            assert_eq!(next_due_date, Some(date));
            assert_eq!(already_done, Some(true));
            assert!(existing_entry.is_none());
            assert_eq!(comment_discarded, Some(false));
        }
        _ => panic!("wrong variant"),
    }

    // Without repo — all optional fields None.
    let msg_no_repo = WsServerMessage::todo_done_success(
        "todo/other.md",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    );
    match msg_no_repo {
        WsServerMessage::TodoDoneResult {
            path,
            repo,
            success,
            error,
            ..
        } => {
            assert_eq!(path, "todo/other.md");
            assert!(repo.is_none(), "repo must be None when not provided");
            assert!(success);
            assert!(error.is_none());
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn todo_error_code_round_trip() {
    let cases: &[(&str, TodoErrorCode)] = &[
        ("stale_anchor", TodoErrorCode::StaleAnchor),
        ("skip_past_unnecessary", TodoErrorCode::SkipPastUnnecessary),
        ("skip_past_on_slip_rule", TodoErrorCode::SkipPastOnSlipRule),
        (
            "skip_past_on_non_recurring",
            TodoErrorCode::SkipPastOnNonRecurring,
        ),
        (
            "already_done_or_not_due_yet",
            TodoErrorCode::AlreadyDoneOrNotDueYet,
        ),
        (
            "replacing_on_recurring",
            TodoErrorCode::ReplacingOnRecurring,
        ),
        ("already_terminal", TodoErrorCode::AlreadyTerminal),
        ("invalid", TodoErrorCode::Invalid),
        ("infra", TodoErrorCode::Infra),
    ];
    for (wire, variant) in cases {
        let json = serde_json::to_string(variant).unwrap();
        assert_eq!(json, format!(r#""{wire}""#));
        let parsed: TodoErrorCode = serde_json::from_str(&json).unwrap();
        assert_eq!(&parsed, variant);
    }
    // Unknown string → Other unit variant.
    let other: TodoErrorCode = serde_json::from_str(r#""future_code""#).unwrap();
    assert_eq!(other, TodoErrorCode::Other);
    // Other serializes as "other", not the original unknown string.
    let other_json = serde_json::to_string(&other).unwrap();
    assert_eq!(other_json, r#""other""#);
    // Display must match wire strings for all variants (used in log macros
    // and DoneFailure::as_string; must not diverge from serde rename_all).
    let display_cases: &[(&str, TodoErrorCode)] = &[
        ("stale_anchor", TodoErrorCode::StaleAnchor),
        ("skip_past_unnecessary", TodoErrorCode::SkipPastUnnecessary),
        ("skip_past_on_slip_rule", TodoErrorCode::SkipPastOnSlipRule),
        (
            "skip_past_on_non_recurring",
            TodoErrorCode::SkipPastOnNonRecurring,
        ),
        (
            "already_done_or_not_due_yet",
            TodoErrorCode::AlreadyDoneOrNotDueYet,
        ),
        (
            "replacing_on_recurring",
            TodoErrorCode::ReplacingOnRecurring,
        ),
        ("already_terminal", TodoErrorCode::AlreadyTerminal),
        ("invalid", TodoErrorCode::Invalid),
        ("infra", TodoErrorCode::Infra),
        ("other", TodoErrorCode::Other),
    ];
    for (expected, variant) in display_cases {
        assert_eq!(
            format!("{variant}"),
            *expected,
            "Display mismatch for {variant:?}"
        );
    }
}
