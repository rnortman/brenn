//! `send_todo_state`, `reject_todo_no_graf`,
//! `handle_todo_refresh`, `handle_todo_done`, `inject_todo_error`,
//! `handle_todo_schedule`, `handle_todo_reorder`.

use brenn_lib::obs::security::{SecurityEventType, log_and_alert_security_event};
use brenn_lib::ws_types::WsServerMessage;
use tracing::warn;

use super::connection::WsConnection;

// impl WsConnection — todo state, mutations (done, schedule, reorder), and error injection
impl WsConnection {
    /// Query graf for the current todo state and broadcast it to the connection.
    /// `config` must already be resolved; returns without sending if the graf query fails.
    //
    // TODO(todo-ui-refresh-on-state-change): currently only called from
    // Brenn-originated mutations (frontend TodoDone/Snooze/Schedule/Reorder).
    // LLM graf-MCP mutations, git pulls, and graf reindexes all leave the UI
    // stale until the user reconnects. Wire up a single invalidate_todo_state()
    // entry point that fires on all these trigger sources.
    pub(super) async fn send_todo_state(&self, config: &brenn_graf::GrafConfig) {
        let ac = self.app_config();
        // One lock, one load_device_user, one Utc::now() — both env and today derived
        // from the same effective TZ snapshot so they can never disagree (design §2.2).
        // Two independent wrappers would leave a TOCTOU window where a TZ override write
        // or expiry crossing between the two DB reads produces an env and today from
        // different zones — the exact divergence this feature exists to close.
        let (env, today) = self.build_graf_env_and_today().await;
        match brenn_graf::subprocess::query_todos(config, ac, &env).await {
            Ok(result) => {
                let lint_errors = result.lint_errors;
                let _ = self.send_ws(WsServerMessage::TodoState {
                    tasks: result.tasks,
                    lint_errors: lint_errors.clone(),
                    domains: result.domains,
                    today,
                });
                if !lint_errors.is_empty() {
                    self.maybe_inject_lint_errors(&lint_errors).await;
                }
            }
            Err(e) => {
                warn!(error = %e, "graf todo query failed");
                let _ = self.send_ws(WsServerMessage::TodoState {
                    tasks: vec![],
                    lint_errors: vec![],
                    domains: None,
                    today,
                });
                self.inject_graf_query_error(&e).await;
            }
        }
    }

    /// Reject a todo message for an app that doesn't have graf enabled.
    /// This is a protocol error — the frontend shouldn't send todo messages
    /// to apps without the graf integration. Security event + alert because
    /// it could indicate probing (though more likely a stale browser tab
    /// after config change).
    fn reject_todo_no_graf(&self, msg_type: &str) {
        log_and_alert_security_event(
            &self.state.alert_dispatcher,
            SecurityEventType::SchemaViolation,
            self.client_ip,
            &format!(
                "{msg_type} for app {:?} which has no graf integration",
                self.app_slug
            ),
        );
        let _ = self.send_ws(WsServerMessage::Error {
            message: "This app does not support todo operations".to_string(),
        });
    }

    pub(super) async fn handle_todo_refresh(&self) {
        self.touch_ui_activity("TodoRefresh").await;
        let Some(config) = brenn_graf::graf_config(self.app_config()) else {
            self.reject_todo_no_graf("TodoRefresh");
            return;
        };
        self.send_todo_state(config).await;
    }

    /// Shared skeleton for todo mutations: checks graf config, calls `f`,
    /// sends `TodoMutationResult`, injects an error event and refreshes on
    /// failure, always refreshes on success. Returns `true` if the mutation
    /// succeeded.
    ///
    /// `msg_type` — the WS message-type tag (e.g. `"TodoSchedule"`) used for
    ///   `reject_todo_no_graf` and the `warn!` log on failure.
    /// `tool_name` — the subprocess tool name (e.g. `"graf_todo_schedule"`)
    ///   used for `inject_todo_error`.
    /// `extra_args` — extra key/value pairs forwarded to `inject_todo_error`.
    async fn run_todo_mutation<F, Fut, T>(
        &self,
        path: &str,
        repo: Option<&str>,
        msg_type: &str,
        tool_name: &str,
        extra_args: &[(&str, &str)],
        f: F,
    ) -> bool
    where
        F: FnOnce(brenn_graf::GrafConfig, Vec<(String, String)>) -> Fut,
        Fut: std::future::Future<Output = Result<T, String>>,
    {
        let ac = self.app_config();
        let Some(config) = brenn_graf::graf_config(ac) else {
            self.reject_todo_no_graf(msg_type);
            return false;
        };
        let env = self.build_graf_env().await;
        match f(config.clone(), env).await {
            Ok(_) => {
                let _ = self.send_ws(WsServerMessage::todo_mutation_result(
                    path, repo, true, None,
                ));
            }
            Err(e) => {
                warn!(path = %path, error = %e, "{msg_type} failed");
                let capped = brenn_lib::util::truncate_with_marker(
                    &e,
                    brenn_lib::util::GRAF_ERROR_MAX_BYTES,
                );
                let _ = self.send_ws(WsServerMessage::todo_mutation_result(
                    path,
                    repo,
                    false,
                    Some(capped.clone()),
                ));
                let payload = serde_json::Value::String(capped);
                self.inject_todo_error(tool_name, path, extra_args, &payload)
                    .await;
                self.send_todo_state(config).await;
                return false;
            }
        }
        self.send_todo_state(config).await;
        true
    }

    pub(super) async fn handle_todo_done(
        &self,
        path: &str,
        repo: Option<&str>,
        completion_date: chrono::NaiveDate,
    ) {
        self.touch_ui_activity("TodoDone").await;
        let ac = self.app_config();
        let Some(config) = brenn_graf::graf_config(ac) else {
            self.reject_todo_no_graf("TodoDone");
            return;
        };
        // `completion_date` is authoritative — the browser is the only
        // reliable source of the user's local date. The field is required
        // in the wire format; serde rejects the message at deserialize if
        // the field is absent, so no defensive Option-unwrap is needed here.
        let env = self.build_graf_env().await;
        match brenn_graf::subprocess::todo_done(config, path, repo, completion_date, ac, &env).await
        {
            Ok(result) => {
                // Prefer on_date, fall back to end_date for the
                // `completion_date` envelope field.
                let completion_date = result.on_date.or(result.end_date);
                let _ = self.send_ws(WsServerMessage::todo_done_success(
                    path,
                    repo,
                    completion_date,
                    result.terminal,
                    result.next_check_in_date,
                    result.next_due_date,
                    result.already_done,
                    result.existing_entry,
                    result.comment_discarded,
                ));
            }
            Err(brenn_graf::subprocess::DoneFailure::Structured {
                code,
                reason,
                envelope,
            }) => {
                warn!(
                    path = %path,
                    code = %code,
                    reason = %reason,
                    "graf todo done returned structured error",
                );
                let _ = self.send_ws(WsServerMessage::todo_done_failure(path, repo, reason));
                // §7.1: inject a system message describing the failure so
                // the LLM can help the user recover. The chat is the
                // surface; the row badge is just a pointer. The envelope
                // is forwarded to the LLM only — not transmitted to the
                // browser (error_code/error_payload were removed in the
                // strict-wire-protocol refactor).
                let completion_str = completion_date.to_string();
                self.inject_todo_error(
                    "graf_todo_done",
                    path,
                    &[("completion_date", &completion_str)],
                    &envelope,
                )
                .await;
            }
            Err(brenn_graf::subprocess::DoneFailure::Opaque(msg)) => {
                warn!(path = %path, error = %msg, "graf todo done failed");
                let _ = self.send_ws(WsServerMessage::todo_done_failure(path, repo, msg.clone()));
                // Opaque failures (infra errors) also get injected so the
                // LLM sees what happened. Structured envelope unavailable —
                // wrap the flat string as a JSON string value (design §9
                // fallback) so the payload is always valid JSON.
                let payload = serde_json::Value::String(msg);
                let completion_str = completion_date.to_string();
                self.inject_todo_error(
                    "graf_todo_done",
                    path,
                    &[("completion_date", &completion_str)],
                    &payload,
                )
                .await;
            }
        }
        // Always refresh state after mutation.
        self.send_todo_state(config).await;
    }

    /// Inject a system message describing a failed graf query into the active
    /// bridge's chat. The LLM sees the error and can help the user diagnose.
    /// The browser receives a visible error card (red, expanded by default).
    ///
    /// No active bridge means no chat — skip the inject (errors are ephemeral;
    /// not queued for offline bridges).
    async fn inject_graf_query_error(&self, error_message: &str) {
        let Some(conv_id) = self.current_conversation_id else {
            return;
        };
        let Some(bridge) = self.state.active_bridges.get(conv_id).await else {
            return;
        };
        let device_slug_owned = self.fetch_device_slug().await;
        let capped = brenn_lib::util::truncate_with_marker(
            error_message,
            brenn_lib::util::GRAF_ERROR_MAX_BYTES,
        );
        let rendered =
            crate::system_message::render_graf_query_error(&capped, Some(&device_slug_owned));
        if let Err(inject_err) = bridge.send_system_message(rendered, None).await {
            warn!(error = %inject_err, "graf query error inject failed");
        }
    }

    /// Inject lint errors to CC only (no browser card, no DB persist) if the
    /// set has changed since the last inject. Dedup prevents a CC user turn
    /// on every refresh when graf reports the same persistent lint errors.
    ///
    /// No active bridge means no CC to inject into — skip silently.
    async fn maybe_inject_lint_errors(&self, lint_errors: &[brenn_lib::ws_types::TodoLintError]) {
        let Some(conv_id) = self.current_conversation_id else {
            return;
        };
        let Some(bridge) = self.state.active_bridges.get(conv_id).await else {
            return;
        };
        // Sort for stable comparison. Build an owned sorted copy for snapshot storage.
        let mut sorted = lint_errors.to_vec();
        sorted.sort();

        {
            let snapshot = bridge
                .last_lint_snapshot
                .lock()
                .expect("last_lint_snapshot poisoned");
            if snapshot.as_ref() == Some(&sorted) {
                // Unchanged — skip to avoid a spurious CC user turn.
                return;
            }
        }

        // Format must remain after the dedup check — O(N) String build that is a
        // no-op on the fast (unchanged) path.
        let text = crate::system_message::format_lint_errors_for_cc(lint_errors);

        match bridge.send_cc_only_system_text(&text).await {
            Ok(()) => {
                // Commit the new snapshot only on success — a failed send should
                // not mark the set as "injected", so the next query retries.
                let mut snapshot = bridge
                    .last_lint_snapshot
                    .lock()
                    .expect("last_lint_snapshot poisoned");
                *snapshot = Some(sorted);
            }
            Err(e) => warn!(error = %e, "lint error inject to CC failed"),
        }
    }

    /// Phase 4 §7.1: inject a system message describing a failed todo
    /// mutation into the active bridge's chat. The LLM sees the attempted
    /// call + the server's structured response and can offer recovery.
    ///
    /// `extra_args` carries attributes other than `path` (e.g.
    /// `[("completion_date", "2026-04-22")]`) — the helper formats them
    /// with JSON-escaped values so both the arg render and the payload
    /// render use the same escape rules. `payload` is the raw response
    /// body; the helper serializes it once.
    ///
    /// No bridge means no chat — skip the inject (domain errors are
    /// ephemeral; we don't queue them for offline bridges).
    async fn inject_todo_error(
        &self,
        tool_name: &str,
        path: &str,
        extra_args: &[(&str, &str)],
        payload: &serde_json::Value,
    ) {
        let Some(conv_id) = self.current_conversation_id else {
            return;
        };
        let Some(bridge) = self.state.active_bridges.get(conv_id).await else {
            return;
        };
        let device_slug_owned = self.fetch_device_slug().await;
        // `to_string` on an owned `serde_json::Value` is infallible —
        // serde_json only fails on I/O or non-string keys, neither of
        // which apply here. Per CLAUDE.md "better dead than wrong", panic if
        // that assumption ever breaks rather than emit hand-rolled JSON.
        let payload_json = serde_json::to_string(payload)
            .expect("serde_json::to_string on owned Value is infallible");
        let rendered = crate::system_message::render_ui_error(
            tool_name,
            path,
            extra_args,
            &payload_json,
            Some(&device_slug_owned),
        );
        if let Err(inject_err) = bridge.send_system_message(rendered, None).await {
            warn!(error = %inject_err, path = %path, tool = %tool_name, "todo error inject failed");
        }
    }

    pub(super) async fn handle_todo_schedule(
        &self,
        path: &str,
        repo: Option<&str>,
        date: chrono::NaiveDate,
    ) {
        self.touch_ui_activity("TodoSchedule").await;
        let date_str = date.format("%Y-%m-%d").to_string();
        let path_owned = path.to_string();
        let repo_owned = repo.map(str::to_string);
        let ac = self.app_config().clone();
        self.run_todo_mutation(
            path,
            repo,
            "TodoSchedule",
            "graf_todo_schedule",
            &[("date", &date_str)],
            move |config, env| async move {
                brenn_graf::subprocess::todo_schedule(
                    &config,
                    &path_owned,
                    repo_owned.as_deref(),
                    date,
                    &ac,
                    &env,
                )
                .await
            },
        )
        .await;
    }

    pub(super) async fn handle_todo_reorder(
        &self,
        path: &str,
        repo: Option<&str>,
        after: Option<(&str, Option<&str>)>,
        before: Option<(&str, Option<&str>)>,
    ) {
        self.touch_ui_activity("TodoReorder").await;
        if after.is_none() && before.is_none() {
            warn!(path = %path, "TodoReorder with no anchors — rejecting");
            let _ = self.send_ws(WsServerMessage::todo_mutation_result(
                path,
                repo,
                false,
                Some("at least one of after/before must be provided".into()),
            ));
            return;
        }
        let path_owned = path.to_string();
        let repo_owned = repo.map(str::to_string);
        // Clone anchor strings so they can move into the async closure.
        let after_owned: Option<(String, Option<String>)> =
            after.map(|(p, r)| (p.to_string(), r.map(str::to_string)));
        let before_owned: Option<(String, Option<String>)> =
            before.map(|(p, r)| (p.to_string(), r.map(str::to_string)));
        let ac = self.app_config().clone();
        self.run_todo_mutation(
            path,
            repo,
            "TodoReorder",
            "graf_todo_reorder",
            &[],
            move |config, env| async move {
                let after_ref = after_owned
                    .as_ref()
                    .map(|(p, r)| (p.as_str(), r.as_deref()));
                let before_ref = before_owned
                    .as_ref()
                    .map(|(p, r)| (p.as_str(), r.as_deref()));
                brenn_graf::subprocess::todo_reorder(
                    &config,
                    &path_owned,
                    repo_owned.as_deref(),
                    after_ref,
                    before_ref,
                    &ac,
                    &env,
                )
                .await
            },
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use brenn_lib::ws_types::{TodoLintError, WsServerMessage};

    use super::super::testing::{
        self as testing, test_apps_with_failing_graf, test_ws_conn_with_active_bridge,
        test_ws_conn_with_active_bridge_and_apps, test_ws_conn_with_resume_conv,
        test_ws_conn_with_resume_conv_and_apps,
    };

    /// Guard path: `handle_todo_reorder` with neither `after` nor `before` must
    /// reject with `TodoMutationResult { success: false }` before calling graf.
    #[tokio::test]
    async fn todo_reorder_no_anchors_returns_error() {
        let (conn, mut ws_rx, _, _, _) = test_ws_conn_with_resume_conv().await;
        conn.handle_todo_reorder("todo/my-task.md", None, None, None)
            .await;
        let msg = ws_rx.try_recv().expect("TodoMutationResult must be sent");
        match msg {
            WsServerMessage::TodoMutationResult {
                success,
                error: Some(error_msg),
                path,
                ..
            } => {
                assert!(!success, "must be failure");
                assert_eq!(path, "todo/my-task.md");
                assert!(
                    error_msg.contains("after") || error_msg.contains("before"),
                    "error must mention after/before anchors, got: {error_msg}",
                );
            }
            other => panic!("expected TodoMutationResult, got: {other:?}"),
        }
        // No second message — reorder with no anchors returns before resolving
        // graf config, so no send_todo_state call.
        assert!(
            ws_rx.try_recv().is_err(),
            "must not send additional messages after anchor-missing rejection"
        );
    }

    // --- run_todo_mutation unit tests ---

    /// Success path: `f` returns `Ok(())` → `TodoMutationResult{success:true}` sent,
    /// then `TodoState` sent (graf subprocess fails → empty state; that's OK).
    #[tokio::test]
    async fn run_todo_mutation_ok_closure_sends_success_result() {
        let (conn, mut ws_rx, _, _, _) =
            test_ws_conn_with_resume_conv_and_apps(test_apps_with_failing_graf()).await;

        let succeeded = conn
            .run_todo_mutation(
                "todo/my-task.md",
                None,
                "TodoSchedule",
                "graf_todo_schedule",
                &[],
                |_config, _env| async { Ok::<(), String>(()) },
            )
            .await;

        assert!(succeeded, "run_todo_mutation must return true on Ok(())");

        let msg = ws_rx.try_recv().expect("TodoMutationResult must be sent");
        match msg {
            WsServerMessage::TodoMutationResult {
                success,
                error,
                path,
                ..
            } => {
                assert!(success, "result must be success");
                assert!(error.is_none(), "error must be None on success");
                assert_eq!(path, "todo/my-task.md");
            }
            other => panic!("expected TodoMutationResult, got: {other:?}"),
        }

        // send_todo_state follows — it will produce a TodoState (empty, since
        // the test graf binary doesn't exist).
        let state_msg = ws_rx
            .try_recv()
            .expect("TodoState must follow success result");
        assert!(
            matches!(state_msg, WsServerMessage::TodoState { .. }),
            "second message must be TodoState, got: {state_msg:?}"
        );
    }

    /// Failure path: `f` returns `Err("boom")` → `TodoMutationResult{success:false, error:Some("boom")}` sent,
    /// then `TodoState` sent. `inject_todo_error` is a no-op (no active bridge in test harness).
    #[tokio::test]
    async fn run_todo_mutation_err_closure_sends_failure_result() {
        let (conn, mut ws_rx, _, _, _) =
            test_ws_conn_with_resume_conv_and_apps(test_apps_with_failing_graf()).await;

        let succeeded = conn
            .run_todo_mutation(
                "todo/my-task.md",
                None,
                "TodoSchedule",
                "graf_todo_schedule",
                &[],
                |_config, _env| async { Err::<(), String>("boom".to_string()) },
            )
            .await;

        assert!(!succeeded, "run_todo_mutation must return false on Err");

        let msg = ws_rx.try_recv().expect("TodoMutationResult must be sent");
        match msg {
            WsServerMessage::TodoMutationResult {
                success,
                error,
                path,
                ..
            } => {
                assert!(!success, "result must be failure");
                assert_eq!(
                    error.as_deref(),
                    Some("boom"),
                    "error payload must be 'boom'"
                );
                assert_eq!(path, "todo/my-task.md");
            }
            other => panic!("expected TodoMutationResult, got: {other:?}"),
        }

        // send_todo_state follows on the failure path too.
        let state_msg = ws_rx
            .try_recv()
            .expect("TodoState must follow failure result");
        assert!(
            matches!(state_msg, WsServerMessage::TodoState { .. }),
            "second message must be TodoState, got: {state_msg:?}"
        );
    }

    /// No-graf-config path: app without graf integration → `reject_todo_no_graf`
    /// sends `WsServerMessage::Error` with `msg_type` in the payload, returns false.
    /// Verified indirectly: default test app has no integrations; calling
    /// `run_todo_mutation` on it triggers the guard path.
    #[tokio::test]
    async fn run_todo_mutation_no_graf_config_rejects() {
        // Default test_apps() has no graf integration — graf_config() returns None.
        let (conn, mut ws_rx, _, _, _) = test_ws_conn_with_resume_conv().await;

        let succeeded = conn
            .run_todo_mutation(
                "todo/my-task.md",
                None,
                "TodoMutation",
                "graf_todo_mutation",
                &[],
                |_config, _env| async { Ok::<(), String>(()) },
            )
            .await;

        assert!(!succeeded, "must return false when no graf config");

        let msg = ws_rx.try_recv().expect("Error must be sent");
        match msg {
            WsServerMessage::Error { message } => {
                assert_eq!(
                    message, "This app does not support todo operations",
                    "unexpected error message"
                );
            }
            other => panic!("expected Error, got: {other:?}"),
        }
        // No additional messages — no TodoActionResult or TodoState when rejected.
        assert!(
            ws_rx.try_recv().is_err(),
            "must not send additional messages after reject_todo_no_graf"
        );
    }

    /// Error path: `send_todo_state` when graf query fails sends empty `TodoState`
    /// and attempts `inject_graf_query_error`. No active bridge in the test
    /// harness, so the inject is a no-op (no active bridge → `active_bridges.get`
    /// returns None → early return). Verifies the degradation path: empty state
    /// still reaches the browser.
    #[tokio::test]
    async fn send_todo_state_error_sends_empty_state() {
        // Use test_apps_with_failing_graf() so send_todo_state reaches the subprocess call,
        // which will fail since the 'graf' binary doesn't exist in the test env.
        let (conn, mut ws_rx, _, _, _) =
            test_ws_conn_with_resume_conv_and_apps(test_apps_with_failing_graf()).await;

        // Use handle_todo_refresh to drive send_todo_state.
        conn.handle_todo_refresh().await;

        let msg = ws_rx
            .try_recv()
            .expect("TodoState must be sent on error path");
        match msg {
            WsServerMessage::TodoState {
                tasks,
                lint_errors,
                domains,
                ..
            } => {
                assert!(tasks.is_empty(), "tasks must be empty on graf error");
                assert!(
                    lint_errors.is_empty(),
                    "lint_errors must be empty on graf error"
                );
                assert!(domains.is_none(), "domains must be None on graf error");
            }
            other => panic!("expected TodoState, got: {other:?}"),
        }
    }

    /// Full injection path: `send_todo_state` → graf query fails → `inject_graf_query_error`
    /// → `bridge.send_system_message` → message reaches the recording CC session.
    ///
    /// Uses `test_ws_conn_with_active_bridge_and_apps` so the bridge is in
    /// `state.active_bridges` (unlike the no-op path tested by
    /// `send_todo_state_error_sends_empty_state`). The nonexistent graf binary causes
    /// the query to fail, exercising the inject side-effect.
    #[tokio::test]
    async fn send_todo_state_error_injects_to_cc() {
        let (conn, mut ws_rx, _db, _user_id, conv_id) =
            test_ws_conn_with_active_bridge_and_apps(test_apps_with_failing_graf()).await;
        let bridge = conn
            .state
            .active_bridges
            .get(conv_id)
            .await
            .expect("bridge must be in active_bridges");
        let mut cc_rx = bridge.install_recording_session_for_test().await;

        conn.handle_todo_refresh().await;

        // Empty TodoState sent to browser on graf query error.
        let ws_msg = ws_rx
            .try_recv()
            .expect("TodoState must be sent on graf query error");
        match ws_msg {
            WsServerMessage::TodoState {
                tasks,
                lint_errors,
                domains,
                ..
            } => {
                assert!(tasks.is_empty(), "tasks must be empty on graf error");
                assert!(
                    lint_errors.is_empty(),
                    "lint_errors must be empty on graf error"
                );
                assert!(domains.is_none(), "domains must be None on graf error");
            }
            other => panic!("expected TodoState, got: {other:?}"),
        }

        // inject_graf_query_error sent a message to the CC session.
        assert!(
            cc_rx.try_recv().is_ok(),
            "inject_graf_query_error must send CcOutgoing to CC when active bridge exists"
        );
    }

    /// Full injection path: `run_todo_mutation` with a failing closure →
    /// `inject_todo_error` sends first `CcOutgoing`, then `send_todo_state`
    /// (graf query also fails) → `inject_graf_query_error` sends second
    /// `CcOutgoing`.
    ///
    /// Uses `test_ws_conn_with_active_bridge_and_apps` so the bridge is in
    /// `state.active_bridges`. The nonexistent binary causes both the mutation
    /// closure failure and the subsequent state-refresh query to fail, producing
    /// two injected CC messages.
    #[tokio::test]
    async fn run_todo_mutation_err_injects_to_cc() {
        let (conn, mut ws_rx, _db, _user_id, conv_id) =
            test_ws_conn_with_active_bridge_and_apps(test_apps_with_failing_graf()).await;
        let bridge = conn
            .state
            .active_bridges
            .get(conv_id)
            .await
            .expect("bridge must be in active_bridges");
        let mut cc_rx = bridge.install_recording_session_for_test().await;

        conn.run_todo_mutation(
            "todo/my-task.md",
            None,
            "TodoSchedule",
            "graf_todo_schedule",
            &[],
            |_config, _env| async { Err::<(), String>("boom".to_string()) },
        )
        .await;

        // TodoMutationResult{success:false, error:"boom"} sent first.
        let msg = ws_rx
            .try_recv()
            .expect("TodoMutationResult must be sent on error");
        match msg {
            WsServerMessage::TodoMutationResult {
                success,
                error,
                path,
                ..
            } => {
                assert!(!success, "result must be failure");
                assert_eq!(
                    error.as_deref(),
                    Some("boom"),
                    "error payload must be 'boom'"
                );
                assert_eq!(path, "todo/my-task.md");
            }
            other => panic!("expected TodoMutationResult, got: {other:?}"),
        }

        // inject_todo_error sent first CcOutgoing (mutation failure inject).
        assert!(
            cc_rx.try_recv().is_ok(),
            "inject_todo_error must send CcOutgoing to CC when active bridge exists"
        );

        // send_todo_state follows on the error path — graf query also fails
        // (nonexistent binary) → empty TodoState.
        let state_msg = ws_rx
            .try_recv()
            .expect("TodoState must follow failure result");
        assert!(
            matches!(state_msg, WsServerMessage::TodoState { .. }),
            "second ws message must be TodoState, got: {state_msg:?}"
        );

        // inject_graf_query_error inside send_todo_state's error branch sends
        // second CcOutgoing.
        assert!(
            cc_rx.try_recv().is_ok(),
            "inject_graf_query_error must send second CcOutgoing from send_todo_state error branch"
        );
    }

    /// Guard path: any handler when `graf_config()` returns `None` (no graf
    /// integration in app config) must call `reject_todo_no_graf`, which sends
    /// `WsServerMessage::Error`. Default test app has no integrations.
    #[tokio::test]
    async fn todo_handler_no_graf_config_sends_error() {
        let (conn, mut ws_rx, _, _, _) = test_ws_conn_with_resume_conv().await;
        // Use handle_todo_refresh: it's the simplest handler with the
        // graf_config guard and no extra input constraints.
        conn.handle_todo_refresh().await;
        let msg = ws_rx.try_recv().expect("Error must be sent");
        match msg {
            WsServerMessage::Error { message } => {
                assert_eq!(
                    message, "This app does not support todo operations",
                    "unexpected error message",
                );
            }
            other => panic!("expected Error, got: {other:?}"),
        }
        assert!(
            ws_rx.try_recv().is_err(),
            "must not send additional messages after reject_todo_no_graf"
        );
    }

    // --- maybe_inject_lint_errors unit tests ---

    /// Dedup path: same lint set on a second call must not send to CC.
    /// First call sends; second call with identical errors is a no-op (snapshot
    /// matches). Exercises the `snapshot.as_ref() == Some(&sorted)` early-return.
    #[tokio::test]
    async fn maybe_inject_lint_errors_dedup_same_set_no_second_send() {
        let (conn, _ws_rx, _db, _user_id, conv_id) = test_ws_conn_with_active_bridge().await;
        let bridge = conn
            .state
            .active_bridges
            .get(conv_id)
            .await
            .expect("bridge must be in active_bridges");
        let mut rx = bridge.install_recording_session_for_test().await;

        let errors = vec![TodoLintError {
            path: "todo/task.md".to_string(),
            message: "missing status".to_string(),
            repo: None,
        }];

        // First call — snapshot is None; send must occur.
        conn.maybe_inject_lint_errors(&errors).await;
        assert!(
            rx.try_recv().is_ok(),
            "first call must send lint errors to CC"
        );

        // Second call — snapshot now matches; must not send.
        conn.maybe_inject_lint_errors(&errors).await;
        assert!(
            rx.try_recv().is_err(),
            "second call with identical lint set must not send to CC (dedup)"
        );

        // Snapshot must be set to the sorted error set.
        let snapshot = bridge
            .last_lint_snapshot
            .lock()
            .expect("last_lint_snapshot poisoned");
        assert_eq!(
            snapshot.as_ref(),
            Some(&errors),
            "snapshot must be updated to the injected lint set"
        );
    }

    /// Changed-set path: after a first inject, a different lint set must send
    /// and update the snapshot. Exercises the post-success snapshot write.
    #[tokio::test]
    async fn maybe_inject_lint_errors_changed_set_sends_and_updates_snapshot() {
        let (conn, _ws_rx, _db, _user_id, conv_id) = test_ws_conn_with_active_bridge().await;
        let bridge = conn
            .state
            .active_bridges
            .get(conv_id)
            .await
            .expect("bridge must be in active_bridges");
        let mut rx = bridge.install_recording_session_for_test().await;

        let errors_a = vec![TodoLintError {
            path: "todo/task-a.md".to_string(),
            message: "missing status".to_string(),
            repo: None,
        }];
        let errors_b = vec![
            TodoLintError {
                path: "todo/task-a.md".to_string(),
                message: "missing status".to_string(),
                repo: None,
            },
            TodoLintError {
                path: "todo/task-b.md".to_string(),
                message: "invalid date".to_string(),
                repo: None,
            },
        ];

        // First call with set A — must send.
        conn.maybe_inject_lint_errors(&errors_a).await;
        assert!(
            rx.try_recv().is_ok(),
            "first call must send lint errors to CC"
        );

        // Second call with set B (different) — must send again and update snapshot.
        conn.maybe_inject_lint_errors(&errors_b).await;
        assert!(rx.try_recv().is_ok(), "changed lint set must send to CC");

        // Snapshot must reflect set B (sorted).
        let mut expected_b = errors_b.clone();
        expected_b.sort();
        let snapshot = bridge
            .last_lint_snapshot
            .lock()
            .expect("last_lint_snapshot poisoned");
        assert_eq!(
            snapshot.as_ref(),
            Some(&expected_b),
            "snapshot must be updated to the new lint set after changed-set send"
        );
    }

    /// `handle_todo_done` opaque error: nonexistent binary produces
    /// `DoneFailure::Opaque` → `TodoDoneResult { success: false }` on `ws_rx`
    /// (not `TodoMutationResult`) + `CcOutgoing` from `inject_todo_error` +
    /// `TodoState` refresh + second `CcOutgoing` from `inject_graf_query_error`
    /// inside `send_todo_state`'s error branch.
    ///
    /// Verifies the variant asymmetry: done failures emit `TodoDoneResult`,
    /// not `TodoMutationResult`.
    #[tokio::test]
    async fn handle_todo_done_opaque_error_injects_to_cc() {
        let (conn, mut ws_rx, _db, _user_id, conv_id) =
            test_ws_conn_with_active_bridge_and_apps(test_apps_with_failing_graf()).await;
        let bridge = conn
            .state
            .active_bridges
            .get(conv_id)
            .await
            .expect("bridge must be in active_bridges");
        let mut cc_rx = bridge.install_recording_session_for_test().await;

        conn.handle_todo_done(
            "todo/my-task.md",
            None,
            chrono::NaiveDate::from_ymd_opt(2026, 5, 27).unwrap(),
        )
        .await;

        // First WS message must be TodoDoneResult (not TodoMutationResult) —
        // this asserts the variant asymmetry that was previously unverified.
        let msg = ws_rx
            .try_recv()
            .expect("TodoDoneResult must be sent on opaque error");
        match msg {
            WsServerMessage::TodoDoneResult { success, error, .. } => {
                assert!(!success, "result must be failure");
                assert!(error.is_some(), "error field must be populated on failure");
            }
            WsServerMessage::TodoMutationResult { .. } => {
                panic!("handle_todo_done must send TodoDoneResult, not TodoMutationResult");
            }
            other => panic!("expected TodoDoneResult, got: {other:?}"),
        }

        // inject_todo_error sent first CcOutgoing (done failure inject).
        assert!(
            cc_rx.try_recv().is_ok(),
            "inject_todo_error must send CcOutgoing to CC when active bridge exists"
        );

        // send_todo_state always follows after handle_todo_done (line 226).
        // Graf query also fails (nonexistent binary) → empty TodoState.
        let state_msg = ws_rx
            .try_recv()
            .expect("TodoState must follow TodoDoneResult");
        assert!(
            matches!(state_msg, WsServerMessage::TodoState { .. }),
            "second ws message must be TodoState, got: {state_msg:?}"
        );

        // inject_graf_query_error inside send_todo_state's error branch sends
        // second CcOutgoing.
        assert!(
            cc_rx.try_recv().is_ok(),
            "inject_graf_query_error must send second CcOutgoing from send_todo_state error branch"
        );
    }

    /// `handle_todo_done` — `DoneFailure::Structured` arm sends `TodoDoneResult`
    /// with the envelope `reason`, and `inject_todo_error` receives the parsed
    /// `serde_json::Value` envelope (not a flat string).
    ///
    /// Verifies: (a) `TodoDoneResult` carries the exact envelope `reason`; (b)
    /// `inject_todo_error` forwards the structured envelope (distinctive payload
    /// marker `next_anchor_if_skip_past_true` present in the `CcOutgoing`); (c)
    /// trailing `TodoState` + second `CcOutgoing` from `inject_graf_query_error`.
    #[cfg(unix)]
    #[tokio::test]
    async fn handle_todo_done_structured_error_injects_envelope_to_cc() {
        let (apps, _tmpdir) = testing::test_apps_with_structured_graf_error();
        let (conn, mut ws_rx, _db, _user_id, conv_id) =
            test_ws_conn_with_active_bridge_and_apps(apps).await;
        let bridge = conn
            .state
            .active_bridges
            .get(conv_id)
            .await
            .expect("bridge must be in active_bridges");
        let mut cc_rx = bridge.install_recording_session_for_test().await;

        conn.handle_todo_done(
            "todo/my-task.md",
            None,
            chrono::NaiveDate::from_ymd_opt(2026, 5, 27).unwrap(),
        )
        .await;

        // First WS message must be TodoDoneResult with the envelope's reason.
        let msg = ws_rx
            .try_recv()
            .expect("TodoDoneResult must be sent on structured error");
        match msg {
            WsServerMessage::TodoDoneResult { success, error, .. } => {
                assert!(!success, "result must be failure");
                // Exact reason from the stub envelope — proves Structured arm,
                // not Opaque (which would carry a spawn-error message).
                assert_eq!(
                    error.as_deref(),
                    Some("anchor shifted for test"),
                    "TodoDoneResult error must be the envelope reason, not an opaque error string"
                );
            }
            WsServerMessage::TodoMutationResult { .. } => {
                panic!("handle_todo_done must send TodoDoneResult, not TodoMutationResult");
            }
            other => panic!("expected TodoDoneResult, got: {other:?}"),
        }

        // inject_todo_error sent first CcOutgoing (done failure inject).
        // Assert it contains the distinctive payload field — proves the parsed
        // Value envelope (not a Value::String flat wrap) reached inject_todo_error.
        let cc_envelope = cc_rx
            .try_recv()
            .expect("inject_todo_error must send CcOutgoing to CC when active bridge exists");
        let serialized = serde_json::to_string(&cc_envelope.msg).unwrap();
        assert!(
            serialized.contains("next_anchor_if_skip_past_true"),
            "CcOutgoing must contain the structured envelope payload field; got: {serialized}"
        );

        // send_todo_state always follows after handle_todo_done (line 226).
        // Graf query also fails (stub exits 1) → empty TodoState.
        let state_msg = ws_rx
            .try_recv()
            .expect("TodoState must follow TodoDoneResult");
        assert!(
            matches!(state_msg, WsServerMessage::TodoState { .. }),
            "second ws message must be TodoState, got: {state_msg:?}"
        );

        // inject_graf_query_error inside send_todo_state's error branch sends
        // second CcOutgoing.
        assert!(
            cc_rx.try_recv().is_ok(),
            "inject_graf_query_error must send second CcOutgoing from send_todo_state error branch"
        );
    }
}
