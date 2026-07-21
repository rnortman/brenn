//! Shared git-subprocess helper for host-side and container-side `git` invocations.
//!
//! Both `repo_clone` and `repo_sync::git` spawn `git` as a child process.
//! This module centralises the common concerns:
//!
//! - **Timeout** — 60-second wall-clock budget per invocation (one place).
//! - **Bounded reads** — stdout and stderr are capped at `OUTPUT_BYTE_CAP`
//!   (256 KiB). An adversarial `git` binary writing GiB of output triggers
//!   `OutputTooLarge` and the child is killed; no OOM. Exception: an
//!   adversarial binary that floods one stream while holding the other open
//!   (asymmetric flood) causes the timeout to fire instead — the result is
//!   `Timeout`, not `OutputTooLarge`. No OOM in either case.
//! - **Strict stdout UTF-8** — `String::from_utf8` on stdout; non-UTF-8
//!   bytes produce `DecodeError`, never a U+FFFD-corrupted success string.
//! - **Lossy stderr on NonZero** — stderr in the `NonZero` error variant is
//!   lossily decoded. Success-path stdout stays strict; only the failure-path
//!   stderr is lossy, and so is stdout in `NonZero` (needed for callers that
//!   substring-match output on exit-1, e.g. "nothing to commit").
//! - **Trailing-newline strip** — at most one trailing `\n` (and the
//!   immediately preceding `\r` if present) is stripped from stdout.
//!   Embedded newlines are preserved (callers that split on `.lines()`
//!   need them).
//! - **Log-line sanitization** — `sanitize_log_line` replaces `\n`/`\r`
//!   with spaces in any string that will reach a `tracing` macro that
//!   fail2ban watches.
//!
//! ## Entry points
//!
//! - `run_git(clone_path, args)` — host-only; builds `git -C <clone_path>
//!   <args>` internally. Used by `repo_sync::git` for operations that only
//!   ever run on the host.
//! - `run_with_bounded_output(cmd, label)` — accepts a pre-built
//!   `tokio::process::Command` (from `brenn_lib::subprocess::run_in_app_env`
//!   or any other builder). Used by `repo_clone` where the caller already
//!   has a pre-formatted label string.
//! - `run_with_bounded_output_lazy(cmd, make_label)` / `run_with_lossy_output_lazy`
//!   — same but accept a closure; the label `String` is only allocated when
//!   an error is constructed. Used by `git_ops` to avoid per-call label
//!   allocations on the idle-hook success path.
//!
//! ## Cap sizing
//!
//! `OUTPUT_BYTE_CAP = 256 KiB`.
//!
//! Worst-case legitimate caller: `collect_oneline` runs
//! `git log --pretty=format:%h %s ... prev..new`. Each line is
//! `<short-sha> <subject>` ≈ 7 + ≤72 bytes typical, generous bound 256
//! bytes per commit. 256 KiB / 256 = 1024 commits per pull cycle, which
//! is far above normal. `cap_oneline` in `brenn_lib::event_queue` further
//! bounds the downstream parse. `git config --get remote.origin.url`
//! produces < 1 KiB; `git rev-parse HEAD` produces 41 bytes;
//! fetch/merge produce minimal stderr on success and a few KiB on failure.
//! 256 KiB is a defense-in-depth ceiling against adversarial output.

use std::ffi::OsStr;
use std::path::Path;
use std::time::Duration;

use tokio::process::Command;

use brenn_lib::subprocess::drain_stream;

/// Wall-clock budget for a single `git` invocation.
/// Sized to allow slow remote operations (fetch over a flaky link) while
/// still bounding hung child processes. Adjust here only; no other copy.
const GIT_CMD_TIMEOUT: Duration = Duration::from_secs(60);

/// Per-stream byte cap. Both stdout and stderr are bounded to this value.
const OUTPUT_BYTE_CAP: usize = 256 * 1024; // 256 KiB

/// Which output stream was involved in an `OutputTooLarge` failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OutputStream {
    Stdout,
    Stderr,
}

impl std::fmt::Display for OutputStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stdout => write!(f, "stdout"),
            Self::Stderr => write!(f, "stderr"),
        }
    }
}

/// Typed errors from the git subprocess helpers.
///
/// Every variant carries a `label` field that identifies the invocation
/// (e.g. `"git clone slug=foo"`, `"git status"`). `Display` prefixes every
/// rendering with `"{label}: "`.
#[derive(Debug)]
pub(crate) enum GitSubprocessError {
    /// The 60-second budget elapsed before the child exited.
    Timeout { label: String },
    /// `Command::spawn` failed (git not on PATH, permission denied, …).
    SpawnFailed {
        label: String,
        source: std::io::Error,
    },
    /// Post-spawn I/O error reading stdout/stderr pipes or waiting for child.
    /// Distinct from `SpawnFailed`: the child was running when this occurred.
    PipeError {
        label: String,
        source: std::io::Error,
    },
    /// git exited non-zero. `stdout` and `stderr` are lossy-UTF-8-decoded.
    /// `exit_code` is `None` when the child was killed by signal.
    /// `Display` renders only `stderr` (truncated to 512 bytes); callers
    /// that need `stdout` or `exit_code` destructure explicitly.
    NonZero {
        label: String,
        stdout: String,
        stderr: String,
        exit_code: Option<i32>,
    },
    /// stdout or stderr exceeded `OUTPUT_BYTE_CAP` before the child exited.
    OutputTooLarge { label: String, stream: OutputStream },
    /// stdout was not valid UTF-8. No substitution returned.
    DecodeError { label: String },
}

/// Truncate `s` to at most `max_bytes` UTF-8 bytes, appending `"…"` if
/// truncated. Used to bound `Display` output for alert/log strings without
/// pulling in `crate::repo_sync`.
fn truncate_for_display(s: &str, max_bytes: usize) -> std::borrow::Cow<'_, str> {
    if s.len() <= max_bytes {
        std::borrow::Cow::Borrowed(s)
    } else {
        // Truncate to a valid UTF-8 boundary at or before max_bytes.
        let mut end = max_bytes;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        std::borrow::Cow::Owned(format!("{}…", &s[..end]))
    }
}

impl std::fmt::Display for GitSubprocessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout { label } => write!(f, "{label}: timeout"),
            Self::SpawnFailed { label, source } => write!(f, "{label}: spawn failed: {source}"),
            Self::PipeError { label, source } => write!(f, "{label}: pipe I/O error: {source}"),
            Self::NonZero { label, stderr, .. } => {
                // Bound the rendered string to 512 bytes so callers that
                // format this into log/alert strings see bounded output.
                write!(
                    f,
                    "{label}: non-zero exit: {}",
                    truncate_for_display(stderr, 512)
                )
            }
            Self::OutputTooLarge { label, stream } => {
                write!(f, "{label}: git output too large ({stream})")
            }
            Self::DecodeError { label } => {
                write!(f, "{label}: git stdout was not valid UTF-8")
            }
        }
    }
}

/// Run `git -C <clone_path> <args>` with bounded reads, strict-UTF-8
/// stdout, and a trailing single-newline strip on stdout.
///
/// Returns the decoded stdout on success (exit 0, stdout valid UTF-8,
/// both streams within `OUTPUT_BYTE_CAP`).
///
/// See module-level doc for timeout, cap, and strip semantics.
///
/// # Non-UTF-8 paths
/// `Command::arg(&Path)` uses `OsStr` faithfully, so non-UTF-8 path
/// bytes in `clone_path` are not corrupted before reaching git's argv.
pub(crate) async fn run_git(
    clone_path: &Path,
    args: &[&str],
) -> Result<String, GitSubprocessError> {
    // Build ["-C", clone_path, args...] as &[&OsStr] without string allocation.
    let mut git_args: Vec<&OsStr> = Vec::with_capacity(2 + args.len());
    git_args.push(OsStr::new("-C"));
    git_args.push(clone_path.as_os_str());
    for arg in args {
        git_args.push(OsStr::new(arg));
    }
    // Label is deferred: the closure is only called when an error is
    // constructed inside `run_prepared`. On the success path (the norm)
    // no `String` is allocated for the label.
    let subcommand = args.first().copied().unwrap_or("");
    let mut cmd = Command::new("git");
    cmd.args(&git_args);
    run_prepared(
        cmd,
        || format!("git {subcommand}"),
        GIT_CMD_TIMEOUT,
        DecodeMode::Strict,
    )
    .await
}

/// Run a pre-built `tokio::process::Command` with bounded reads, strict-UTF-8
/// stdout, and a trailing single-newline strip on stdout.
///
/// `label` is a short human-readable identifier for the invocation (e.g.
/// `"git clone slug=foo"`, `"git status"`). It appears in all error
/// `Display` renderings and tracing warn messages.
///
/// The caller is responsible for building the `Command` (e.g. via
/// `brenn_lib::subprocess::run_in_app_env`). Stdin, stdout, stderr, and
/// `kill_on_drop` are overwritten by this function; the caller need not set
/// them.
pub(crate) async fn run_with_bounded_output(
    cmd: Command,
    label: &str,
) -> Result<String, GitSubprocessError> {
    run_prepared(
        cmd,
        || label.to_string(),
        GIT_CMD_TIMEOUT,
        DecodeMode::Strict,
    )
    .await
}

/// Like `run_with_bounded_output` but accepts a lazy `make_label` closure that
/// is called **only** when an error variant is constructed. On the success path
/// no label `String` is allocated. Use from callers (e.g. `git_ops`) that
/// would otherwise compute the label eagerly on every call regardless of
/// outcome.
pub(crate) async fn run_with_bounded_output_lazy(
    cmd: Command,
    make_label: impl Fn() -> String,
) -> Result<String, GitSubprocessError> {
    run_prepared(cmd, make_label, GIT_CMD_TIMEOUT, DecodeMode::Strict).await
}

/// Like `run_with_lossy_output` but accepts a lazy `make_label` closure.
/// See `run_with_bounded_output_lazy` for the motivation.
pub(crate) async fn run_with_lossy_output_lazy(
    cmd: Command,
    make_label: impl Fn() -> String,
) -> Result<String, GitSubprocessError> {
    run_prepared(cmd, make_label, GIT_CMD_TIMEOUT, DecodeMode::Lossy).await
}

/// Like `run_with_bounded_output` but accepts an explicit `timeout`, used
/// by tests to verify that the public API kills children on timeout without
/// waiting the full `GIT_CMD_TIMEOUT` wall-clock budget.
#[cfg(test)]
pub(super) async fn run_with_bounded_output_for_test(
    cmd: Command,
    label: &str,
    timeout: Duration,
) -> Result<String, GitSubprocessError> {
    run_prepared(cmd, || label.to_string(), timeout, DecodeMode::Strict).await
}

/// Stdout decode mode for the success path in `run_prepared`.
#[derive(Clone, Copy)]
enum DecodeMode {
    /// `String::from_utf8` — error on invalid bytes. Used for parsing callers.
    Strict,
    /// `String::from_utf8_lossy` — replace invalid bytes with U+FFFD. Used for
    /// pass-through callers that report raw git output to the LLM.
    Lossy,
}

/// Inner implementation: drive an already-configured `Command` with the
/// full bounded-drain / timeout / decode pipeline.
///
/// `run_git`, `run_with_bounded_output`, `run_with_bounded_output_lazy`, and
/// `run_with_lossy_output_lazy` all delegate here. Tests call this directly
/// with a short timeout; the `#[cfg(test)] mod tests` block inside this file
/// has access to private items in the parent module without any extra
/// visibility annotation.
///
/// `make_label` is a closure called lazily — only when an error variant is
/// constructed. On the success path no label `String` is allocated.
async fn run_prepared(
    mut cmd: Command,
    make_label: impl Fn() -> String,
    timeout: Duration,
    decode: DecodeMode,
) -> Result<String, GitSubprocessError> {
    cmd.stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);

    let mut child = cmd.spawn().map_err(|e| GitSubprocessError::SpawnFailed {
        label: make_label(),
        source: e,
    })?;

    let stdout_handle = child.stdout.take().expect("stdout is piped");
    let stderr_handle = child.stderr.take().expect("stderr is piped");

    // Drive both reads concurrently, then wait for the child.
    // Race the whole draining future against the timeout budget.
    let drain_fut = async {
        // try_join! short-circuits on the first I/O error, avoiding a full
        // drain of the second stream when one pipe has already failed.
        // drain_stream reads up to cap+1 bytes; buf.len() > OUTPUT_BYTE_CAP
        // detects overflow without a separate syscall.
        let (stdout_bytes, stderr_bytes) = tokio::try_join!(
            drain_stream(stdout_handle, OUTPUT_BYTE_CAP),
            drain_stream(stderr_handle, OUTPUT_BYTE_CAP),
        )?;
        // On overflow the child is still blocked on write(2) into the now-unread
        // pipe. Kill it before wait() so we don't block until the full timeout
        // elapses. start_kill() is a legitimate let _ = here: the child may have
        // already exited, in which case the error is expected and irrelevant.
        let overflow = stdout_bytes.len() > OUTPUT_BYTE_CAP || stderr_bytes.len() > OUTPUT_BYTE_CAP;
        if overflow {
            let _ = child.start_kill();
        }
        let wait_result = child.wait().await.map_err(|e| {
            if overflow {
                tracing::warn!(error = %e, "wait() failed after overflow kill");
            }
            e
        })?;
        Ok::<_, std::io::Error>((stdout_bytes, stderr_bytes, wait_result))
    };

    let (stdout_bytes, stderr_bytes, wait_result) =
        match tokio::time::timeout(timeout, drain_fut).await {
            Ok(Ok(triple)) => triple,
            Ok(Err(e)) => {
                return Err(GitSubprocessError::PipeError {
                    label: make_label(),
                    source: e,
                });
            }
            Err(_elapsed) => {
                // Compute label once for both the warn! and the Timeout error.
                let label_str = make_label();
                if let Err(e) = child.start_kill() {
                    tracing::warn!(
                        label = %label_str,
                        error = %e,
                        "failed to kill timed-out git child; process may be orphaned"
                    );
                }
                if let Err(e) = child.wait().await {
                    tracing::warn!(
                        label = %label_str,
                        error = %e,
                        "failed to reap timed-out git child"
                    );
                }
                return Err(GitSubprocessError::Timeout { label: label_str });
            }
        };

    // Overflow check: if either buffer reached cap+1 bytes, at least one
    // byte was emitted past the cap.
    // Note: child.wait() was already called inside drain_fut above (and
    // start_kill() was called before that if overflow was detected), so the
    // child is already reaped here; no kill needed.
    if stdout_bytes.len() > OUTPUT_BYTE_CAP {
        return Err(GitSubprocessError::OutputTooLarge {
            label: make_label(),
            stream: OutputStream::Stdout,
        });
    }
    if stderr_bytes.len() > OUTPUT_BYTE_CAP {
        return Err(GitSubprocessError::OutputTooLarge {
            label: make_label(),
            stream: OutputStream::Stderr,
        });
    }

    if !wait_result.success() {
        let stdout = String::from_utf8_lossy(&stdout_bytes).into_owned();
        let stderr = String::from_utf8_lossy(&stderr_bytes).into_owned();
        return Err(GitSubprocessError::NonZero {
            label: make_label(),
            stdout,
            stderr,
            exit_code: wait_result.code(),
        });
    }

    // Success path: decode stdout per the requested mode.
    let stdout = match decode {
        DecodeMode::Strict => {
            String::from_utf8(stdout_bytes).map_err(|_| GitSubprocessError::DecodeError {
                label: make_label(),
            })?
        }
        DecodeMode::Lossy => String::from_utf8_lossy(&stdout_bytes).into_owned(),
    };

    // Strip at most one trailing newline (and the \r before it if CRLF).
    let stdout = strip_trailing_newline(stdout);

    Ok(stdout)
}

/// Strip at most one trailing `\n` from `s`, plus the immediately preceding
/// `\r` if present (CRLF). Does not strip multiple trailing newlines.
pub(crate) fn strip_trailing_newline(mut s: String) -> String {
    if s.ends_with('\n') {
        s.pop();
        if s.ends_with('\r') {
            s.pop();
        }
    }
    s
}

/// Replace every `\n` and `\r` in `s` with a single space character.
///
/// Apply to any subprocess-derived string before it reaches a
/// `tracing::warn!`/`error!`/`info!` call that fail2ban watches. Embedded
/// newlines in log fields forge separate log records; this removes them.
///
/// Alert bodies (push notifications, email) do not need sanitization —
/// fail2ban does not parse those.
pub(crate) fn sanitize_log_line(s: &str) -> String {
    s.chars()
        .map(|c| if c == '\n' || c == '\r' { ' ' } else { c })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Write a shell script body to `dir/<name>` as plain data (no chmod).
    /// Tests invoke it via `/bin/sh <path>`, so the kernel never execve's
    /// the file — no ETXTBSY race is possible.
    fn write_script(dir: &std::path::Path, name: &str, body: &str) -> std::path::PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, format!("{body}\n")).unwrap();
        path
    }

    // ── Test 1: oversize stdout → OutputTooLarge { Stdout } ──────────────────

    #[tokio::test]
    async fn oversize_stdout_returns_output_too_large() {
        let tmp = tempfile::TempDir::new().unwrap();
        // Write cap+1 bytes to stdout, then exit 0.
        // Use block-aligned writes (bs=4096) plus one extra byte to minimise
        // syscall count; dd bs=1 takes ~256k syscalls and is too slow on CI.
        let full_blocks = OUTPUT_BYTE_CAP / 4096; // 64 full 4 KiB blocks = 256 KiB
        let script = write_script(
            tmp.path(),
            "fake-git-oversize-stdout",
            &format!(
                "dd if=/dev/zero bs=4096 count={full_blocks} 2>/dev/null; \
                 dd if=/dev/zero bs=1 count=1 2>/dev/null; exit 0",
            ),
        );

        let mut cmd = Command::new("/bin/sh");
        cmd.arg(&script);
        let result = run_prepared(
            cmd,
            || "test".to_string(),
            Duration::from_secs(30),
            DecodeMode::Strict,
        )
        .await;

        assert!(
            matches!(
                result,
                Err(GitSubprocessError::OutputTooLarge {
                    stream: OutputStream::Stdout,
                    ..
                })
            ),
            "expected OutputTooLarge(Stdout), got {result:?}"
        );
    }

    // ── Test 2: oversize stderr on non-zero exit → OutputTooLarge { Stderr } ─

    #[tokio::test]
    async fn oversize_stderr_on_nonzero_returns_output_too_large() {
        let tmp = tempfile::TempDir::new().unwrap();
        // Write cap+1 bytes to stderr then exit 1.
        // Redirect order matters: `>&2` first duplicates stdout onto the stderr
        // pipe; `2>/dev/null` then silences dd's progress output to fd2 (now
        // /dev/null). The data stream goes to fd1, which still points at the
        // original stderr pipe from the >&2 dup.
        // Use block writes to avoid ~256k syscalls from bs=1.
        let full_blocks = OUTPUT_BYTE_CAP / 4096; // 64 full 4 KiB blocks = 256 KiB
        let script = write_script(
            tmp.path(),
            "fake-git-oversize-stderr",
            &format!(
                "dd if=/dev/zero bs=4096 count={full_blocks} >&2 2>/dev/null; \
                 dd if=/dev/zero bs=1 count=1 >&2 2>/dev/null; exit 1",
            ),
        );

        let mut cmd = Command::new("/bin/sh");
        cmd.arg(&script);
        let result = run_prepared(
            cmd,
            || "test".to_string(),
            Duration::from_secs(30),
            DecodeMode::Strict,
        )
        .await;

        assert!(
            matches!(
                result,
                Err(GitSubprocessError::OutputTooLarge {
                    stream: OutputStream::Stderr,
                    ..
                })
            ),
            "expected OutputTooLarge(Stderr), got {result:?}"
        );
    }

    // ── Test 3: non-UTF-8 stdout → DecodeError (no U+FFFD success) ───────────

    #[tokio::test]
    async fn non_utf8_stdout_returns_decode_error() {
        let tmp = tempfile::TempDir::new().unwrap();
        // printf with octal escapes is POSIX-portable (unlike \xNN hex).
        // \377\376\375 = 0xff 0xfe 0xfd, which is not valid UTF-8.
        let script = write_script(
            tmp.path(),
            "fake-git-non-utf8",
            r"printf '\377\376\375'; exit 0",
        );

        let mut cmd = Command::new("/bin/sh");
        cmd.arg(&script);
        let result = run_prepared(
            cmd,
            || "test".to_string(),
            Duration::from_secs(5),
            DecodeMode::Strict,
        )
        .await;

        // Must be DecodeError, not Ok with U+FFFD substitution.
        assert!(
            matches!(result, Err(GitSubprocessError::DecodeError { .. })),
            "expected DecodeError, got {result:?}"
        );
    }

    // ── Test 4: trailing newline stripped ─────────────────────────────────────

    #[tokio::test]
    async fn trailing_newline_stripped() {
        let tmp = tempfile::TempDir::new().unwrap();
        let script = write_script(
            tmp.path(),
            "fake-git-trailing-nl",
            r#"printf 'foo\n'; exit 0"#,
        );

        let mut cmd = Command::new("/bin/sh");
        cmd.arg(&script);
        let result = run_prepared(
            cmd,
            || "test".to_string(),
            Duration::from_secs(5),
            DecodeMode::Strict,
        )
        .await;

        assert_eq!(result.unwrap(), "foo");
    }

    // ── Test 5: multi-line output, only trailing newline stripped ─────────────

    #[tokio::test]
    async fn multi_line_trailing_newline_only() {
        let tmp = tempfile::TempDir::new().unwrap();
        let script = write_script(
            tmp.path(),
            "fake-git-multi-line",
            r#"printf 'foo\nbar\n'; exit 0"#,
        );

        let mut cmd = Command::new("/bin/sh");
        cmd.arg(&script);
        let result = run_prepared(
            cmd,
            || "test".to_string(),
            Duration::from_secs(5),
            DecodeMode::Strict,
        )
        .await;

        assert_eq!(result.unwrap(), "foo\nbar");
    }

    // ── Test 6: timeout kills child ───────────────────────────────────────────

    #[tokio::test]
    async fn timeout_kills_child() {
        let tmp = tempfile::TempDir::new().unwrap();
        // Write PID to a file, then sleep a long time.
        let pid_file = tmp.path().join("child.pid");
        // Use `exec sleep 60` so the shell replaces itself with the sleep
        // process; SIGKILL on the spawned PID kills the right process and
        // does not leave a sleep grandchild orphaned.
        let script = write_script(
            tmp.path(),
            "fake-git-timeout",
            &format!(
                r#"echo $$ > "{pid_file}"; exec sleep 60"#,
                pid_file = pid_file.display(),
            ),
        );

        let mut cmd = Command::new("/bin/sh");
        cmd.arg(&script);
        let result = run_prepared(
            cmd,
            || "test".to_string(),
            Duration::from_millis(300),
            DecodeMode::Strict,
        )
        .await;

        assert!(
            matches!(result, Err(GitSubprocessError::Timeout { .. })),
            "expected Timeout, got {result:?}"
        );

        // Wait for the child PID file to appear (it writes before sleeping).
        // Then verify the child process is no longer alive.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !pid_file.exists() && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // The script writes its PID synchronously before sleeping, so the
        // file must exist by now (the helper ran to completion above).
        // Fail loudly if it doesn't — a missing pid_file means the
        // child-kill verification was vacuous.
        assert!(
            pid_file.exists(),
            "pid_file was not written — liveness check is vacuous; \
             this may indicate a race or a script execution failure"
        );

        let pid_str = std::fs::read_to_string(&pid_file).unwrap();
        let pid: libc::pid_t = pid_str.trim().parse().unwrap();

        // Poll until kill(pid, 0) returns ESRCH (no such process) or deadline.
        let liveness_deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            let r = unsafe { libc::kill(pid, 0) };
            if r != 0 {
                // ESRCH or EPERM (the expected outcomes when dead/reaped)
                break;
            }
            if std::time::Instant::now() >= liveness_deadline {
                panic!("child process {pid} still alive after timeout kill");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // ── Test 7a: sanitize_log_line replaces \n and \r with spaces ────────────

    #[test]
    fn sanitize_log_line_strips_newlines_and_carriage_returns() {
        let s = sanitize_log_line("line1\nline2\rline3\r\nline4");
        assert!(
            !s.contains('\n') && !s.contains('\r'),
            "sanitize_log_line left a newline/CR in: {s:?}"
        );
        assert_eq!(s, "line1 line2 line3  line4");
    }

    // ── Test 7b: sanitize_log_line on NonZero Display ─────────────────────────

    #[test]
    fn sanitize_log_line_strips_newlines_from_nonzero_display() {
        let err = GitSubprocessError::NonZero {
            label: "git status".to_string(),
            stdout: String::new(),
            stderr: "line1\nline2\n".to_string(),
            exit_code: Some(1),
        };
        let sanitized = sanitize_log_line(&err.to_string());
        assert!(
            !sanitized.contains('\n') && !sanitized.contains('\r'),
            "sanitize_log_line left a newline/CR in: {sanitized:?}"
        );
    }

    // ── Test 8: run_with_bounded_output caps stdout ───────────────────────────

    #[tokio::test]
    async fn run_with_bounded_output_caps_stdout() {
        let tmp = tempfile::TempDir::new().unwrap();
        let full_blocks = OUTPUT_BYTE_CAP / 4096;
        let script = write_script(
            tmp.path(),
            "fake-git-bounded-oversize",
            &format!(
                "dd if=/dev/zero bs=4096 count={full_blocks} 2>/dev/null; \
                 dd if=/dev/zero bs=1 count=1 2>/dev/null; exit 0",
            ),
        );

        let mut cmd = Command::new("/bin/sh");
        cmd.arg(&script);
        let result = run_with_bounded_output(cmd, "test").await;

        assert!(
            matches!(
                result,
                Err(GitSubprocessError::OutputTooLarge {
                    stream: OutputStream::Stdout,
                    ..
                })
            ),
            "expected OutputTooLarge(Stdout) via run_with_bounded_output, got {result:?}"
        );
    }

    // ── Test 9: run_with_bounded_output strict UTF-8 ─────────────────────────

    #[tokio::test]
    async fn run_with_bounded_output_strict_utf8() {
        let tmp = tempfile::TempDir::new().unwrap();
        let script = write_script(
            tmp.path(),
            "fake-git-bounded-non-utf8",
            r"printf '\377\376\375'; exit 0",
        );

        let mut cmd = Command::new("/bin/sh");
        cmd.arg(&script);
        let result = run_with_bounded_output(cmd, "test").await;

        assert!(
            matches!(result, Err(GitSubprocessError::DecodeError { .. })),
            "expected DecodeError via run_with_bounded_output, got {result:?}"
        );
    }

    // ── Test 10: run_with_bounded_output NonZero exposes stdout ──────────────
    //
    // Load-bearing for the repo_commit_and_push "nothing to commit" detection:
    // the helper must return stdout in the NonZero variant so callers can
    // substring-match it without a second subprocess call.

    #[tokio::test]
    async fn run_with_bounded_output_nonzero_exposes_stdout() {
        let tmp = tempfile::TempDir::new().unwrap();
        let script = write_script(
            tmp.path(),
            "fake-git-nonzero-stdout",
            r#"printf 'nothing to commit\n'; exit 1"#,
        );

        let mut cmd = Command::new("/bin/sh");
        cmd.arg(&script);
        let result = run_with_bounded_output(cmd, "test").await;

        match result {
            Err(GitSubprocessError::NonZero {
                stdout, exit_code, ..
            }) => {
                assert!(
                    stdout.contains("nothing to commit"),
                    "stdout should contain marker, got: {stdout:?}"
                );
                assert_eq!(exit_code, Some(1));
            }
            other => panic!("expected NonZero, got {other:?}"),
        }
    }

    // ── Test 11: Display label prefix present on each variant ─────────────────

    #[test]
    fn display_includes_label_prefix() {
        // Timeout
        let err = GitSubprocessError::Timeout {
            label: "git clone".to_string(),
        };
        assert!(
            err.to_string().starts_with("git clone: timeout"),
            "Timeout Display missing label prefix: {err}"
        );

        // NonZero — also verifies 512-byte stderr truncation path compiles and runs.
        let err = GitSubprocessError::NonZero {
            label: "git status".to_string(),
            stdout: String::new(),
            stderr: "fatal: not a git repository".to_string(),
            exit_code: Some(128),
        };
        let s = err.to_string();
        assert!(
            s.starts_with("git status: non-zero exit:"),
            "NonZero Display missing label prefix: {s}"
        );
        assert!(
            s.contains("fatal: not a git repository"),
            "NonZero Display missing stderr content: {s}"
        );

        // DecodeError
        let err = GitSubprocessError::DecodeError {
            label: "git log".to_string(),
        };
        assert!(
            err.to_string()
                .starts_with("git log: git stdout was not valid UTF-8"),
            "DecodeError Display missing label prefix: {err}"
        );
    }

    // ── Test 12: run_with_bounded_output kills child on timeout ──────────────
    //
    // Verifies the kill-on-timeout guarantee at the public API boundary.
    // `timeout_kills_child` (test 6) exercises `run_prepared` directly with
    // a short timeout; this test exercises `run_with_bounded_output` through
    // the `run_with_bounded_output_for_test` helper so that any future logic
    // interposed between the public entry and `run_prepared` is covered.

    #[tokio::test]
    async fn run_with_bounded_output_timeout_kills_child() {
        let tmp = tempfile::TempDir::new().unwrap();
        let pid_file = tmp.path().join("child.pid");
        let script = write_script(
            tmp.path(),
            "fake-git-public-timeout",
            &format!(
                r#"echo $$ > "{pid_file}"; exec sleep 60"#,
                pid_file = pid_file.display(),
            ),
        );

        let mut cmd = Command::new("/bin/sh");
        cmd.arg(&script);
        let result =
            run_with_bounded_output_for_test(cmd, "git status", Duration::from_millis(300)).await;

        assert!(
            matches!(result, Err(GitSubprocessError::Timeout { .. })),
            "expected Timeout from run_with_bounded_output_for_test, got {result:?}"
        );

        // Wait for PID file, then verify child is dead — same pattern as test 6.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !pid_file.exists() && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            pid_file.exists(),
            "pid_file was not written — liveness check is vacuous"
        );
        let pid_str = std::fs::read_to_string(&pid_file).unwrap();
        let pid: libc::pid_t = pid_str.trim().parse().unwrap();
        let liveness_deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            let r = unsafe { libc::kill(pid, 0) };
            if r != 0 {
                break;
            }
            if std::time::Instant::now() >= liveness_deadline {
                panic!(
                    "child process {pid} still alive after timeout kill via run_with_bounded_output"
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}
