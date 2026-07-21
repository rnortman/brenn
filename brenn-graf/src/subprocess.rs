//! Subprocess wrappers for calling the `graf` CLI.
//!
//! Each function spawns `graf todo [subcommand] --json` as a child process,
//! captures stdout, and parses the JSON output. Mutations include
//! `--repo <slug>` when a repo slug is provided. Timeouts prevent hung
//! processes from blocking the WS handler.
//!
//! Container-aware: when `container_spawn` is `Some`, commands run inside
//! a podman container via `brenn_lib::subprocess::run_in_app_env`.

use std::io;
use std::time::Duration;

use chrono::NaiveDate;
use tokio::time::timeout;

use crate::GrafConfig;
use brenn_lib::config::AppConfig;
use brenn_lib::subprocess::drain_stream;
use brenn_lib::ws_types::{CompletionLogEntry, TodoErrorCode, TodoItem, TodoLintError};

/// Result of a todo query, mirroring graf's `TodoQueryResult` JSON.
#[derive(Debug, serde::Deserialize)]
pub struct TodoQueryResult {
    pub tasks: Vec<TodoItem>,
    #[serde(default)]
    pub lint_errors: Vec<TodoLintError>,
    /// Sharing domains from the manifest. `None` when no manifest is active.
    #[serde(default)]
    pub domains: Option<Vec<String>>,
}

/// Result of `graf todo done`. Phase 2 response shape (PRD-done §7).
///
/// - Non-recurring advance: `on_date` or `end_date` set, `terminal: false`.
/// - Anchored rrule exhaustion: `on_date`/`end_date` set, `terminal: true`.
/// - Recurring advance: `next_check_in_date` set, `next_due_date` optional.
/// - Slip idempotent no-op: `already_done: true`, `existing_entry` set.
///
/// The Phase 4 UI will read these fields for the "Next: MM/DD" toast and
/// terminal-completion messaging. Phase 2 only plumbs them through.
#[derive(Debug, serde::Deserialize)]
pub struct DoneResult {
    pub path: String,
    /// Date written to `on_date` on completion (non-range tasks).
    #[serde(default)]
    pub on_date: Option<NaiveDate>,
    /// Date written to `end_date` on completion (range tasks with start_date).
    #[serde(default)]
    pub end_date: Option<NaiveDate>,
    /// True when this advance exhausted the recurrence (anchored) or
    /// completed a non-recurring task. On recurring advance, absent/false.
    #[serde(default)]
    pub terminal: Option<bool>,
    /// Next `check_in_date` after the advance — present on recurring
    /// (both slip and anchored) non-terminal completions.
    #[serde(default)]
    pub next_check_in_date: Option<NaiveDate>,
    /// Next `due_date` after the advance — present on recurring tasks
    /// whose schedule is anchored by `due_date`.
    #[serde(default)]
    pub next_due_date: Option<NaiveDate>,
    /// Slip no-op flag: task already had a log entry with
    /// `completed == completion_date`. No file write happened.
    #[serde(default)]
    pub already_done: Option<bool>,
    /// The existing log entry that triggered the no-op (slip only).
    #[serde(default)]
    pub existing_entry: Option<CompletionLogEntry>,
    /// Whether a supplied `comment` was discarded (slip no-op path only).
    #[serde(default)]
    pub comment_discarded: Option<bool>,
}

/// Failure shape for `todo_done`. The interesting branch is `Structured`,
/// which carries graf's PRD-done §7 `{error, reason, ...payload}` envelope
/// — the Phase 4 UI reads `code` to pick a dialog (stale_anchor → catch-up
/// vs. skip-ahead choice; already_done_or_not_due_yet → overwrite prompt).
///
/// `Opaque` is the escape hatch for infrastructure failures (spawn, timeout,
/// UTF-8 decode) and the "graf exited non-zero but stdout isn't JSON" case
/// that shouldn't happen per design §7 but we fail safely on anyway.
#[derive(Debug)]
pub enum DoneFailure {
    /// graf exited with a structured error envelope on stdout.
    Structured {
        /// PRD-done §7 error code. Known variants are in `TodoErrorCode`; unknown
        /// codes from future graf versions land in `Other`, preserving the raw string.
        code: TodoErrorCode,
        /// Human-readable reason string from graf.
        reason: String,
        /// The full envelope, including `error`, `reason`, and any
        /// code-specific payload fields. Forwarded to `inject_todo_error`
        /// for LLM chat injection only; not transmitted to the browser.
        envelope: serde_json::Value,
    },
    /// Infrastructure error or unparseable output.
    Opaque(String),
}

impl DoneFailure {
    /// Short string for logging / legacy error-as-string consumers.
    pub fn as_string(&self) -> String {
        match self {
            DoneFailure::Structured { code, reason, .. } => format!("{code}: {reason}"),
            DoneFailure::Opaque(s) => s.clone(),
        }
    }
}

impl std::fmt::Display for DoneFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.as_string())
    }
}

/// Result of `graf todo schedule`.
#[derive(Debug, serde::Deserialize)]
pub struct ScheduleResult {
    pub path: String,
    pub tentative_date: String,
    pub sort_order: Option<f64>,
}

const QUERY_TIMEOUT: Duration = Duration::from_secs(10);
const MUTATION_TIMEOUT: Duration = Duration::from_secs(15);

/// Byte cap for graf subprocess stdout and stderr. Valid graf output tops out
/// at ~30 KB for a 100-item query; 256 KiB provides ~8× headroom. A
/// subprocess exceeding this cap returns `Err` — the output is too large to be
/// valid JSON for any known graf command.
const GRAF_OUTPUT_BYTE_CAP: usize = 256 * 1024;

/// Per-field truncation cap for stdout/stderr in error messages.
/// Half of GRAF_ERROR_MAX_BYTES so that stdout + stderr together
/// fit within the overall cap before framing text is added.
const GRAF_ERROR_FIELD_MAX_BYTES: usize = brenn_lib::util::GRAF_ERROR_MAX_BYTES / 2;

/// Build an error string from a failed `GrafOutput`.
/// Truncates `stdout` and `stderr` individually with `GRAF_ERROR_FIELD_MAX_BYTES`.
/// `label` is the human-readable prefix (e.g. `"graf command failed"`).
/// No outer truncation — per-field caps bound the total to
/// `2 * GRAF_ERROR_FIELD_MAX_BYTES + framing`. Callers that require an
/// additional safety net apply `truncate_with_marker` at the call site.
fn build_graf_error(label: &str, exit_code: &str, stdout: &str, stderr: &str) -> String {
    let stdout = brenn_lib::util::truncate_with_marker(stdout, GRAF_ERROR_FIELD_MAX_BYTES);
    let stderr = brenn_lib::util::truncate_with_marker(stderr, GRAF_ERROR_FIELD_MAX_BYTES);
    format!("{label} (exit {exit_code}): stdout={stdout}, stderr={stderr}")
}

/// Query the todo list. Returns all active tasks (regardless of date)
/// including unscheduled ones. The frontend groups by date and shows
/// everything — the user's persistent todo view should be comprehensive.
pub async fn query_todos(
    config: &GrafConfig,
    app: &AppConfig,
    env: &[(String, String)],
) -> Result<TodoQueryResult, String> {
    let output = run_graf(config, &["todo", "--json"], QUERY_TIMEOUT, app, env).await?;

    // Fail-fast boundary for the `TodoItem.effective_date` non-null
    // invariant: `TodoItem` declares `effective_date` as a non-`Option`
    // `NaiveDate`, so a null (or missing) value from graf fails this
    // `from_str` call. The error propagates as `Err(String)` to
    // `send_todo_state` (brenn/src/routes/ws.rs), which logs via
    // `warn!` and sends an empty `TodoState` to the frontend. No
    // malformed item reaches downstream consumers.
    serde_json::from_str(&output)
        .map_err(|e| format!("failed to parse graf todo query output: {e}"))
}

/// Build the common prefix args for a mutation: `todo --json <subcommand> [--repo <slug>]`.
/// Note: `--json` is a flag on `graf todo`, not on the subcommands, so it must
/// come before the subcommand name.
fn mutation_args<'a>(subcommand: &'a str, repo: Option<&'a str>) -> Vec<&'a str> {
    let mut args = vec!["todo", "--json", subcommand];
    if let Some(r) = repo {
        args.push("--repo");
        args.push(r);
    }
    args
}

/// Mark a task as done. `repo` is the repo slug (from `TodoItem.repo`);
/// required when the manifest has multiple repos. `completion_date` is
/// required by graf — the caller must source it from browser-local today.
///
/// Errors arrive as [`DoneFailure::Structured`] when graf returned a PRD-done
/// §7 envelope (structured errors printed to stdout on non-zero exit), or
/// [`DoneFailure::Opaque`] for infrastructure failures or malformed output.
pub async fn todo_done(
    config: &GrafConfig,
    path: &str,
    repo: Option<&str>,
    completion_date: NaiveDate,
    app: &AppConfig,
    env: &[(String, String)],
) -> Result<DoneResult, DoneFailure> {
    let completion_str = completion_date.format("%Y-%m-%d").to_string();
    let mut args = mutation_args("done", repo);
    args.push(path);
    args.push("--completion-date");
    args.push(&completion_str);
    let raw = run_graf_raw(config, &args, MUTATION_TIMEOUT, app, env)
        .await
        .map_err(DoneFailure::Opaque)?;

    if raw.success {
        serde_json::from_str(&raw.stdout)
            .map_err(|e| DoneFailure::Opaque(format!("failed to parse graf todo done output: {e}")))
    } else {
        // Per design §7, graf prints the {error, reason, ...payload} envelope
        // to stdout and exits non-zero. Parse it and surface as structured so
        // the UI can branch on `code` (Phase 4: stale_anchor choice dialog).
        match parse_done_error_envelope(&raw.stdout) {
            Some(failure) => Err(failure),
            None => Err(DoneFailure::Opaque(
                // Outer safety-net truncation on todo_done only; run_graf omits
                // it because per-field caps already bound the total.
                brenn_lib::util::truncate_with_marker(
                    &build_graf_error(
                        "graf todo done failed",
                        &raw.exit_code,
                        &raw.stdout,
                        &raw.stderr,
                    ),
                    brenn_lib::util::GRAF_ERROR_MAX_BYTES,
                ),
            )),
        }
    }
}

/// Parse graf's PRD-done §7 error envelope from stdout. Returns
/// `Some(Structured{..})` on a well-formed envelope, `None` when the output
/// isn't the expected shape (caller falls back to `Opaque`).
fn parse_done_error_envelope(stdout: &str) -> Option<DoneFailure> {
    let value: serde_json::Value = serde_json::from_str(stdout.trim()).ok()?;
    let obj = value.as_object()?;
    let raw_code = obj.get("error")?.as_str()?;
    // Use serde_json::Value::String to drive deserialization without cloning the
    // whole envelope value, and log on unexpected codes (legitimate codes all
    // parse via #[serde(other)] → Other; failure means non-string JSON type
    // slipped past the as_str() guard, which shouldn't happen).
    let code = serde_json::from_value::<TodoErrorCode>(serde_json::Value::String(
        raw_code.to_owned(),
    ))
    .map_err(|e| {
        tracing::warn!(err = %e, raw = raw_code, "TodoErrorCode deser failed in error envelope");
        e
    })
    .ok()?;
    let reason = obj.get("reason")?.as_str()?.to_string();
    Some(DoneFailure::Structured {
        code,
        reason,
        envelope: value,
    })
}

/// Set a task's tentative date. `repo` is the repo slug (from
/// `TodoItem.repo`); required when the manifest has multiple repos.
pub async fn todo_schedule(
    config: &GrafConfig,
    path: &str,
    repo: Option<&str>,
    date: NaiveDate,
    app: &AppConfig,
    env: &[(String, String)],
) -> Result<ScheduleResult, String> {
    let date_str = date.format("%Y-%m-%d").to_string();
    let mut args = mutation_args("schedule", repo);
    args.push(path);
    args.push(&date_str);
    let output = run_graf(config, &args, MUTATION_TIMEOUT, app, env).await?;

    serde_json::from_str(&output)
        .map_err(|e| format!("failed to parse graf todo schedule output: {e}"))
}

/// Result of `graf todo reorder`.
#[derive(Debug, serde::Deserialize)]
pub struct ReorderResult {
    pub path: String,
    pub sort_order: f64,
    pub tentative_date: Option<String>,
}

/// Reorder a task relative to neighbor anchors. At least one of `after`/`before`
/// must be `Some`. Anchors are `(path, repo)` tuples; when `repo` is `Some`,
/// the CLI anchor uses `slug:path` format for cross-repo references.
pub async fn todo_reorder(
    config: &GrafConfig,
    path: &str,
    repo: Option<&str>,
    after: Option<(&str, Option<&str>)>,
    before: Option<(&str, Option<&str>)>,
    app: &AppConfig,
    env: &[(String, String)],
) -> Result<ReorderResult, String> {
    let mut args = mutation_args("reorder", repo);
    args.push(path);

    // Build anchor strings: bare `path` for same-repo, `slug:path` for cross-repo.
    let after_str;
    if let Some((anchor_path, anchor_repo)) = after {
        args.push("--after");
        after_str = format_anchor(anchor_path, anchor_repo);
        args.push(&after_str);
    }
    let before_str;
    if let Some((anchor_path, anchor_repo)) = before {
        args.push("--before");
        before_str = format_anchor(anchor_path, anchor_repo);
        args.push(&before_str);
    }

    let output = run_graf(config, &args, MUTATION_TIMEOUT, app, env).await?;

    serde_json::from_str(&output)
        .map_err(|e| format!("failed to parse graf todo reorder output: {e}"))
}

/// Format a reorder anchor for the graf CLI. Same-repo anchors use the bare path;
/// cross-repo anchors use `slug:path`.
fn format_anchor(path: &str, repo: Option<&str>) -> String {
    match repo {
        Some(slug) => format!("{slug}:{path}"),
        None => path.to_string(),
    }
}

/// Captured output of a graf child process, regardless of exit status.
/// Returned by [`run_graf_raw`] so callers that care about exit-dependent
/// output (like `todo_done`'s structured-error envelope on stdout with
/// exit 1) can inspect both streams without double-error-paths.
#[derive(Debug)]
pub(crate) struct GrafOutput {
    pub success: bool,
    pub exit_code: String,
    pub stdout: String,
    pub stderr: String,
}

/// Run a graf command with timeout, returning stdout on success. On any
/// non-success (exit non-zero, timeout, spawn failure, UTF-8) returns a
/// single error string — use [`run_graf_raw`] when the caller needs to
/// parse stdout even on non-zero exit (e.g. structured error envelopes).
async fn run_graf(
    config: &GrafConfig,
    args: &[&str],
    dur: Duration,
    app: &AppConfig,
    env: &[(String, String)],
) -> Result<String, String> {
    let raw = run_graf_raw(config, args, dur, app, env).await?;
    if raw.success {
        Ok(raw.stdout)
    } else {
        Err(build_graf_error(
            "graf command failed",
            &raw.exit_code,
            &raw.stdout,
            &raw.stderr,
        ))
    }
}

/// Returns `Err` if either stream exceeds `cap` bytes.
///
/// Relies on `drain_stream` returning exactly `cap + 1` bytes for over-cap
/// streams — see `brenn_lib::subprocess::drain_stream`.
fn check_overflow(stdout: &[u8], stderr: &[u8], cap: usize) -> Result<(), String> {
    if stdout.len() > cap {
        return Err(format!("graf stdout exceeded {cap} byte cap"));
    }
    if stderr.len() > cap {
        return Err(format!("graf stderr exceeded {cap} byte cap"));
    }
    Ok(())
}

/// Like [`run_graf`] but hands back both streams plus the exit-success
/// flag without collapsing non-zero-exit cases into an error. Used by
/// `todo_done` to recover graf's structured error envelope (printed to
/// stdout on exit 1 per PRD-done §7).
///
/// Infrastructure failures (spawn, timeout, UTF-8) still come back as
/// `Err(String)` — those have no useful stdout to inspect.
pub(crate) async fn run_graf_raw(
    config: &GrafConfig,
    args: &[&str],
    dur: Duration,
    app: &AppConfig,
    env: &[(String, String)],
) -> Result<GrafOutput, String> {
    // Downstream `run_in_app_env` wants `&[(&str, &str)]`. Collect once
    // at the boundary so callers (and the five WS handlers that each
    // spawn graf subprocesses) don't each reinvent this conversion.
    let env_borrowed: Vec<(&str, &str)> =
        env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let mut cmd = brenn_lib::subprocess::run_in_app_env(
        &config.command,
        args,
        &app.working_dir,
        app.container_spawn.as_ref(),
        &env_borrowed,
        &[],
    );
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    cmd.kill_on_drop(true);

    let mut child = cmd
        .spawn()
        .map_err(|e| format!("failed to spawn graf: {e}"))?;

    let stdout_handle = child.stdout.take().expect("stdout is piped");
    let stderr_handle = child.stderr.take().expect("stderr is piped");

    let drain_fut = async {
        let (stdout_bytes, stderr_bytes) = tokio::try_join!(
            drain_stream(stdout_handle, GRAF_OUTPUT_BYTE_CAP),
            drain_stream(stderr_handle, GRAF_OUTPUT_BYTE_CAP),
        )?;
        // On overflow the child is still blocked on write(2) into the now-unread
        // pipe. Kill it before wait() so we don't block until the full timeout
        // elapses. start_kill() is a legitimate let _ = here: the child may have
        // already exited, in which case the error is expected and irrelevant.
        let overflow =
            stdout_bytes.len() > GRAF_OUTPUT_BYTE_CAP || stderr_bytes.len() > GRAF_OUTPUT_BYTE_CAP;
        if overflow {
            let _ = child.start_kill();
        }
        let wait_result = child.wait().await.map_err(|e| {
            if overflow {
                tracing::warn!(error = %e, "wait() failed after overflow kill");
            }
            e
        })?;
        Ok::<_, io::Error>((stdout_bytes, stderr_bytes, wait_result))
    };

    let (stdout_bytes, stderr_bytes, status) = timeout(dur, drain_fut)
        .await
        .map_err(|_| format!("graf command timed out after {}s", dur.as_secs()))?
        .map_err(|e| format!("failed to read graf output: {e}"))?;

    if let Err(e) = check_overflow(&stdout_bytes, &stderr_bytes, GRAF_OUTPUT_BYTE_CAP) {
        tracing::error!("graf output overflow: args={args:?}: {e}");
        return Err(e);
    }

    let stdout = String::from_utf8(stdout_bytes)
        .map_err(|e| format!("graf stdout is not valid UTF-8: {e}"))?;
    let stderr = String::from_utf8_lossy(&stderr_bytes).into_owned();
    Ok(GrafOutput {
        success: status.success(),
        exit_code: status.to_string(),
        stdout,
        stderr,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::test_app_config;

    /// Write a shell script body to `dir/<name>`. Returns the absolute path.
    /// The script is run via `/bin/sh <path>`, not execve, so no shebang or
    /// executable permission is needed — and the ETXTBSY race cannot occur.
    fn write_script(dir: &std::path::Path, name: &str, body: &str) -> std::path::PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, format!("{body}\n")).unwrap();
        path
    }

    /// `run_graf_raw`: stdout overflow — spawns a script emitting cap+1 bytes
    /// on stdout; asserts `Err` containing "stdout exceeded" and the cap value.
    #[tokio::test]
    async fn run_graf_raw_overflow_stdout() {
        let tmp = tempfile::TempDir::new().unwrap();
        // 64 × 4096 = 262144 = GRAF_OUTPUT_BYTE_CAP; plus one extra byte.
        let full_blocks = GRAF_OUTPUT_BYTE_CAP / 4096;
        let script = write_script(
            tmp.path(),
            "fake-graf-overflow-stdout",
            &format!(
                "dd if=/dev/zero bs=4096 count={full_blocks} 2>/dev/null; \
                 dd if=/dev/zero bs=1 count=1 2>/dev/null; exit 0",
            ),
        );

        let config = GrafConfig {
            command: "/bin/sh".into(),
        };
        let app = test_app_config(tmp.path().to_path_buf(), vec![], false);
        let result = run_graf_raw(
            &config,
            &[script.to_str().unwrap()],
            Duration::from_secs(5),
            &app,
            &[],
        )
        .await;

        let err = result.expect_err("expected Err on stdout overflow");
        assert!(
            err.contains("graf stdout exceeded"),
            "error should mention 'graf stdout exceeded': {err}"
        );
        assert!(
            err.contains("byte cap"),
            "error should include 'byte cap': {err}"
        );
        assert!(
            err.contains(&GRAF_OUTPUT_BYTE_CAP.to_string()),
            "error should include the cap value {GRAF_OUTPUT_BYTE_CAP}: {err}"
        );
    }

    /// `run_graf_raw`: stderr overflow — spawns a script emitting cap+1 bytes
    /// on stderr; asserts `Err` containing "stderr exceeded".
    #[tokio::test]
    async fn run_graf_raw_overflow_stderr() {
        let tmp = tempfile::TempDir::new().unwrap();
        let full_blocks = GRAF_OUTPUT_BYTE_CAP / 4096;
        let script = write_script(
            tmp.path(),
            "fake-graf-overflow-stderr",
            &format!(
                "dd if=/dev/zero bs=4096 count={full_blocks} 1>&2 2>/dev/null; \
                 dd if=/dev/zero bs=1 count=1 1>&2 2>/dev/null; exit 0",
            ),
        );

        let config = GrafConfig {
            command: "/bin/sh".into(),
        };
        let app = test_app_config(tmp.path().to_path_buf(), vec![], false);
        let result = run_graf_raw(
            &config,
            &[script.to_str().unwrap()],
            Duration::from_secs(5),
            &app,
            &[],
        )
        .await;

        let err = result.expect_err("expected Err on stderr overflow");
        assert!(
            err.contains("graf stderr exceeded"),
            "error should mention 'graf stderr exceeded': {err}"
        );
        assert!(
            err.contains("byte cap"),
            "error should include 'byte cap': {err}"
        );
        assert!(
            err.contains(&GRAF_OUTPUT_BYTE_CAP.to_string()),
            "error should include the cap value {GRAF_OUTPUT_BYTE_CAP}: {err}"
        );
    }

    /// TodoQueryResult deserializes from single-repo output (no domains field).
    #[test]
    fn query_result_deserializes_without_domains() {
        let json = r#"{
            "tasks": [
                {"path": "todo/foo.md", "tldr": "Buy groceries", "effective_date": "2026-04-12"}
            ],
            "lint_errors": []
        }"#;
        let result: TodoQueryResult = serde_json::from_str(json).unwrap();
        assert_eq!(result.tasks.len(), 1);
        assert_eq!(result.tasks[0].path, "todo/foo.md");
        assert!(result.domains.is_none());
    }

    /// TodoQueryResult deserializes from multi-repo output (with domains).
    #[test]
    fn query_result_deserializes_with_domains() {
        let json = r#"{
            "tasks": [
                {"path": "todo/foo.md", "tldr": "Buy groceries", "effective_date": "2026-04-12", "repo": "life", "domain": "example.org/personal"},
                {"path": "todo/deploy.md", "tldr": "Fix deploy", "effective_date": "2026-04-14", "repo": "eng", "domain": "acme.example.com/confidential"}
            ],
            "lint_errors": [
                {"path": "todo/broken.md", "message": "missing tldr", "repo": "life"}
            ],
            "domains": ["example.org/personal", "acme.example.com/confidential"]
        }"#;
        let result: TodoQueryResult = serde_json::from_str(json).unwrap();
        assert_eq!(result.tasks.len(), 2);
        assert_eq!(result.tasks[0].repo.as_deref(), Some("life"));
        assert_eq!(result.tasks[1].repo.as_deref(), Some("eng"));
        assert_eq!(result.lint_errors.len(), 1);
        assert_eq!(result.lint_errors[0].repo.as_deref(), Some("life"));
        let domains = result.domains.unwrap();
        assert_eq!(domains.len(), 2);
        assert_eq!(domains[0], "example.org/personal");
    }

    /// TodoQueryResult handles absent lint_errors (defaults to empty vec).
    #[test]
    fn query_result_absent_lint_errors_defaults() {
        let json = r#"{"tasks": []}"#;
        let result: TodoQueryResult = serde_json::from_str(json).unwrap();
        assert!(result.tasks.is_empty());
        assert!(result.lint_errors.is_empty());
        assert!(result.domains.is_none());
    }

    /// DoneResult for a non-recurring completion (Phase 2 shape).
    #[test]
    fn done_result_deserializes_non_recurring() {
        let json = r#"{
            "path": "todo/foo.md",
            "on_date": "2026-04-12",
            "terminal": false
        }"#;
        let result: DoneResult = serde_json::from_str(json).unwrap();
        assert_eq!(result.path, "todo/foo.md");
        assert_eq!(
            result.on_date,
            Some(NaiveDate::from_ymd_opt(2026, 4, 12).unwrap())
        );
        assert_eq!(result.terminal, Some(false));
        assert!(result.next_check_in_date.is_none());
    }

    /// DoneResult for a recurring advance (Phase 2 shape).
    #[test]
    fn done_result_deserializes_recurring() {
        let json = r#"{
            "path": "todo/weekly.md",
            "terminal": false,
            "next_check_in_date": "2026-04-19",
            "next_due_date": "2026-04-25"
        }"#;
        let result: DoneResult = serde_json::from_str(json).unwrap();
        assert_eq!(result.path, "todo/weekly.md");
        assert!(result.on_date.is_none());
        assert_eq!(
            result.next_check_in_date,
            Some(NaiveDate::from_ymd_opt(2026, 4, 19).unwrap())
        );
        assert_eq!(
            result.next_due_date,
            Some(NaiveDate::from_ymd_opt(2026, 4, 25).unwrap())
        );
    }

    /// DoneResult for a slip idempotent no-op.
    #[test]
    fn done_result_deserializes_already_done() {
        let json = r#"{
            "path": "todo/weekly.md",
            "already_done": true,
            "existing_entry": {"completed": "2026-04-18"},
            "comment_discarded": false,
            "message": "already done"
        }"#;
        let result: DoneResult = serde_json::from_str(json).unwrap();
        assert_eq!(result.already_done, Some(true));
        assert_eq!(result.comment_discarded, Some(false));
        let entry = result.existing_entry.unwrap();
        assert_eq!(
            entry.completed,
            NaiveDate::from_ymd_opt(2026, 4, 18).unwrap()
        );
    }

    /// `parse_done_error_envelope` recognises a graf structured error.
    #[test]
    fn parse_done_error_envelope_stale_anchor() {
        let stdout = r#"{
            "error": "stale_anchor",
            "reason": "Stored anchor (2026-01-03) is far enough behind ...",
            "stored_anchor": "2026-01-03",
            "completion_date": "2026-04-22",
            "next_anchor_if_skip_past_false": "2026-01-10",
            "next_anchor_if_skip_past_true": "2026-04-25"
        }"#;
        let failure = parse_done_error_envelope(stdout).expect("structured envelope");
        match failure {
            DoneFailure::Structured {
                code,
                reason,
                envelope,
            } => {
                assert_eq!(code, TodoErrorCode::StaleAnchor);
                assert!(reason.contains("Stored anchor"));
                // Payload fields are preserved in the envelope so Phase 4 UI
                // can read `next_anchor_if_skip_past_*` directly.
                assert_eq!(
                    envelope["next_anchor_if_skip_past_true"],
                    serde_json::json!("2026-04-25")
                );
            }
            DoneFailure::Opaque(s) => panic!("expected Structured, got Opaque({s})"),
        }
    }

    /// Unknown error code lands in `TodoErrorCode::Other` — catch-all path.
    #[test]
    fn parse_done_error_envelope_unknown_code() {
        let stdout = r#"{
            "error": "future_code",
            "reason": "Some new error from a future graf version"
        }"#;
        let failure = parse_done_error_envelope(stdout).expect("structured envelope");
        match failure {
            DoneFailure::Structured { code, reason, .. } => {
                assert_eq!(code, TodoErrorCode::Other);
                assert!(reason.contains("future graf version"));
            }
            DoneFailure::Opaque(s) => panic!("expected Structured, got Opaque({s})"),
        }
    }

    /// Non-JSON stdout yields `None` so the caller falls back to `Opaque`.
    #[test]
    fn parse_done_error_envelope_rejects_non_json() {
        assert!(parse_done_error_envelope("graf: no such file").is_none());
    }

    /// JSON without an `error` field is not a structured error envelope.
    #[test]
    fn parse_done_error_envelope_rejects_unstructured() {
        assert!(parse_done_error_envelope(r#"{"path": "todo/foo.md"}"#).is_none());
    }

    /// `DoneFailure::as_string` collapses structured failures into
    /// `code: reason` form for the pre-existing `Err(String)` log paths.
    #[test]
    fn done_failure_as_string_for_structured() {
        let failure = DoneFailure::Structured {
            code: TodoErrorCode::StaleAnchor,
            reason: "anchor is stale".to_string(),
            envelope: serde_json::json!({}),
        };
        assert_eq!(failure.as_string(), "stale_anchor: anchor is stale");
    }

    /// `DoneFailure::as_string` for the `Other` variant shows `"other"` as the
    /// code (not the original unknown string, which is lost at deserialization).
    #[test]
    fn done_failure_as_string_for_structured_other() {
        let failure = DoneFailure::Structured {
            code: TodoErrorCode::Other,
            reason: "unknown problem".to_string(),
            envelope: serde_json::json!({}),
        };
        assert_eq!(failure.as_string(), "other: unknown problem");
    }

    /// ScheduleResult deserializes.
    #[test]
    fn schedule_result_deserializes() {
        let json = r#"{"path": "todo/foo.md", "tentative_date": "2026-04-15", "sort_order": 1.5}"#;
        let result: ScheduleResult = serde_json::from_str(json).unwrap();
        assert_eq!(result.path, "todo/foo.md");
        assert_eq!(result.tentative_date, "2026-04-15");
        assert_eq!(result.sort_order, Some(1.5));
    }

    /// ScheduleResult without sort_order.
    #[test]
    fn schedule_result_without_sort_order() {
        let json = r#"{"path": "todo/foo.md", "tentative_date": "2026-04-15"}"#;
        let result: ScheduleResult = serde_json::from_str(json).unwrap();
        assert!(result.sort_order.is_none());
    }

    /// ReorderResult deserializes with tentative_date.
    #[test]
    fn reorder_result_deserializes_with_date() {
        let json = r#"{"path": "todo/foo.md", "sort_order": 2.5, "tentative_date": "2026-04-15"}"#;
        let result: ReorderResult = serde_json::from_str(json).unwrap();
        assert_eq!(result.path, "todo/foo.md");
        assert_eq!(result.sort_order, 2.5);
        assert_eq!(result.tentative_date.as_deref(), Some("2026-04-15"));
    }

    /// ReorderResult without tentative_date (both anchors unscheduled).
    #[test]
    fn reorder_result_deserializes_without_date() {
        let json = r#"{"path": "todo/foo.md", "sort_order": 3.0}"#;
        let result: ReorderResult = serde_json::from_str(json).unwrap();
        assert!(result.tentative_date.is_none());
    }

    /// format_anchor produces bare path for same-repo.
    #[test]
    fn format_anchor_same_repo() {
        assert_eq!(format_anchor("todo/foo.md", None), "todo/foo.md");
    }

    /// format_anchor produces slug:path for cross-repo.
    #[test]
    fn format_anchor_cross_repo() {
        assert_eq!(
            format_anchor("todo/foo.md", Some("life")),
            "life:todo/foo.md"
        );
    }

    /// `--json` must come before the subcommand (it's a flag on `graf todo`,
    /// not on the subcommands).
    #[test]
    fn mutation_args_json_before_subcommand() {
        let args = mutation_args("done", None);
        assert_eq!(args, vec!["todo", "--json", "done"]);
    }

    /// `--repo` is appended when provided.
    #[test]
    fn mutation_args_with_repo() {
        let args = mutation_args("schedule", Some("life"));
        assert_eq!(args, vec!["todo", "--json", "schedule", "--repo", "life"]);
    }

    /// `build_todo_done_error` output is bounded when stdout is oversized (stderr is small).
    ///
    /// Exercises the full production path via `build_todo_done_error`: per-field pre-truncation
    /// of individual fields plus the outer safety-net truncation on the composed message.
    #[test]
    fn done_failure_opaque_safety_net_is_bounded() {
        // Build a GrafOutput whose stdout exceeds GRAF_ERROR_MAX_BYTES.
        // parse_done_error_envelope returns None for non-JSON, so the None
        // arm runs — that's the truncation site under test.
        let big_stdout = "x".repeat(brenn_lib::util::GRAF_ERROR_MAX_BYTES * 4);
        let raw = GrafOutput {
            success: false,
            exit_code: "exit status: 1".to_string(),
            stdout: big_stdout,
            stderr: "some stderr".to_string(),
        };

        let opaque = brenn_lib::util::truncate_with_marker(
            &build_graf_error(
                "graf todo done failed",
                &raw.exit_code,
                &raw.stdout,
                &raw.stderr,
            ),
            brenn_lib::util::GRAF_ERROR_MAX_BYTES,
        );

        // The marker overhead is bounded at ~50 bytes; allow generous headroom.
        const MARKER_OVERHEAD: usize = 80;
        assert!(
            opaque.len() <= brenn_lib::util::GRAF_ERROR_MAX_BYTES + MARKER_OVERHEAD,
            "Opaque error string length {} exceeds cap + overhead ({})",
            opaque.len(),
            brenn_lib::util::GRAF_ERROR_MAX_BYTES + MARKER_OVERHEAD
        );
        assert!(
            opaque.contains("[truncated,"),
            "expected truncation marker in: {opaque:?}"
        );
    }

    /// `check_overflow`: output below cap — both streams ok.
    #[test]
    fn check_overflow_below_cap_is_ok() {
        let stdout = b"hello".to_vec();
        let stderr = b"world".to_vec();
        assert!(check_overflow(&stdout, &stderr, 10).is_ok());
    }

    /// `check_overflow`: stdout exactly at cap — not an overflow.
    #[test]
    fn check_overflow_exactly_at_cap_is_ok() {
        let stdout = vec![b'x'; 10];
        let stderr = vec![];
        assert!(check_overflow(&stdout, &stderr, 10).is_ok());
    }

    /// `check_overflow`: stdout at cap+1 — overflow detected.
    /// This is the sentinel value that `take(cap+1).read_to_end()` produces
    /// when the stream exceeds `cap` bytes.
    #[test]
    fn check_overflow_stdout_sentinel_is_err() {
        let stdout = vec![b'x'; 11]; // cap+1 for cap=10
        let stderr = vec![];
        let err = check_overflow(&stdout, &stderr, 10).unwrap_err();
        assert!(
            err.contains("stdout"),
            "error should identify the stream: {err}"
        );
        assert!(err.contains("10"), "error should include the cap: {err}");
    }

    /// `check_overflow`: stderr at cap+1 — overflow detected on stderr.
    #[test]
    fn check_overflow_stderr_sentinel_is_err() {
        let stdout = vec![];
        let stderr = vec![b'x'; 11]; // cap+1 for cap=10
        let err = check_overflow(&stdout, &stderr, 10).unwrap_err();
        assert!(
            err.contains("stderr"),
            "error should identify the stream: {err}"
        );
    }

    /// `check_overflow`: both streams overflow simultaneously — stdout error wins
    /// (first branch). Documents the priority behavior so a future change to
    /// `check_overflow` that alters priority cannot pass silently.
    #[test]
    fn check_overflow_both_overflow_stdout_wins() {
        let stdout = vec![b'x'; 11]; // cap+1 for cap=10
        let stderr = vec![b'y'; 11]; // cap+1 for cap=10
        let err = check_overflow(&stdout, &stderr, 10).unwrap_err();
        assert!(
            err.contains("stdout"),
            "stdout overflow should take priority: {err}"
        );
        assert!(
            !err.contains("stderr"),
            "stderr should not appear when stdout overflows first: {err}"
        );
    }

    /// `run_graf` non-zero-exit error string is bounded when both stdout and
    /// stderr are oversized.
    ///
    /// Regression guard for the pre-truncation at Site 1. A regression
    /// removing the per-field `truncate_with_marker` calls would produce an
    /// unbounded `Err(String)`, re-enabling the large intermediate allocation
    /// described in the truncate-before-format design.
    #[test]
    fn run_graf_error_oversized_output_is_bounded() {
        let big = "x".repeat(brenn_lib::util::GRAF_ERROR_MAX_BYTES * 4);
        let raw = GrafOutput {
            success: false,
            exit_code: "exit status: 1".to_string(),
            stdout: big.clone(),
            stderr: big.clone(),
        };

        let err = build_graf_error(
            "graf command failed",
            &raw.exit_code,
            &raw.stdout,
            &raw.stderr,
        );

        // Two markers (~41 bytes each) + ~60 bytes framing + 200 headroom.
        assert!(
            err.len() <= brenn_lib::util::GRAF_ERROR_MAX_BYTES + 200,
            "run_graf error string length {} exceeds cap + overhead ({})",
            err.len(),
            brenn_lib::util::GRAF_ERROR_MAX_BYTES + 200
        );
        assert!(
            err.contains("[truncated,"),
            "expected truncation marker in: {err:?}"
        );
    }

    /// `DoneFailure::Opaque` from the `todo_done` None arm is bounded when
    /// both stdout and stderr are oversized.
    ///
    /// Complements `done_failure_opaque_oversized_stdout_is_bounded` by
    /// exercising the pre-truncation at Site 2 (both fields oversized,
    /// safety-net fires on the composed message).
    #[test]
    fn done_failure_opaque_oversized_both_fields_bounded() {
        let big = "x".repeat(brenn_lib::util::GRAF_ERROR_MAX_BYTES * 4);
        let raw = GrafOutput {
            success: false,
            exit_code: "exit status: 1".to_string(),
            stdout: big.clone(),
            stderr: big.clone(),
        };

        let opaque = brenn_lib::util::truncate_with_marker(
            &build_graf_error(
                "graf todo done failed",
                &raw.exit_code,
                &raw.stdout,
                &raw.stderr,
            ),
            brenn_lib::util::GRAF_ERROR_MAX_BYTES,
        );

        const MARKER_OVERHEAD: usize = 80;
        assert!(
            opaque.len() <= brenn_lib::util::GRAF_ERROR_MAX_BYTES + MARKER_OVERHEAD,
            "Opaque error string length {} exceeds cap + overhead ({})",
            opaque.len(),
            brenn_lib::util::GRAF_ERROR_MAX_BYTES + MARKER_OVERHEAD
        );
        assert!(
            opaque.contains("[truncated,"),
            "expected truncation marker in: {opaque:?}"
        );
    }
}
