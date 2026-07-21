//! Host-side git plumbing for the repo-sync manager.
//!
//! Distinct from `brenn/src/git_ops.rs`, which is the LLM-facing (possibly
//! containerized) tooling surface. The sync manager always runs pulls on
//! the host (see `docs/designs/repo-sync.md` — section "Host-side pull"),
//! so we don't route through `run_in_app_env` or pick a container.
//!
//! **MVP scope**: hard-coded upstream of `origin/main`. Prod is 100%
//! `main` today.

use std::path::Path;

use brenn_lib::messaging::cap_oneline;
use tracing::{debug, warn};

use super::truncate_detail;
use crate::git_subprocess::{GitSubprocessError, run_git};

/// Internal classification of the post-merge HEAD state when `new_head != remote_head`.
///
/// This is a module-private staging type for `pull_clone`'s mismatch branch;
/// it is not part of the public `PullOutcome` API.
#[derive(Debug, PartialEq)]
enum PostMergeOutcome {
    /// `remote_head` is an ancestor of `new_head`, and `new_head == prev_head`:
    /// the clone was already ahead of `origin/main` before the fetch, so the
    /// ff-only merge was a no-op. No new state to record.
    UpToDate,
    /// `remote_head` is a strict ancestor of `new_head` (Case A): a concurrent
    /// local commit landed between merge completion and `rev_parse HEAD`.
    /// The design intentionally uses `remote_head` as the `Advanced` payload
    /// here — see the `PullOutcome::Advanced` docstring and
    /// `reactor.rs:402-407`. Returning `Advanced { remote_head }` is correct.
    Advanced,
    /// `new_head` is not a descendant of `remote_head` (Case B): external
    /// interference (force-push, history rewrite, fs corruption, manual
    /// reset). Genuine invariant violation — caller must panic.
    ///
    /// `detail` carries the raw stderr from `is_ancestor` when the ancestry
    /// check itself failed (e.g., unknown SHA from concurrent gc). `None`
    /// when the check returned `Ok(false)` cleanly (genuine non-descendant).
    /// The caller includes it in the panic message so on-call can distinguish
    /// "ancestry check returned false" from "ancestry check crashed on
    /// unknown SHA / object-DB corruption".
    InvariantViolation { detail: Option<String> },
}

/// Classify a post-merge HEAD mismatch.
///
/// Called only when `new_head != remote_head` (the caller short-circuits the
/// common `new_head == remote_head` case before calling this function, so the
/// cost-free happy path is unaffected).
///
/// Decision table:
///
/// | `is_ancestor_result`     | `new_head == prev_head` | Returns              |
/// |--------------------------|-------------------------|----------------------|
/// | `Ok(true)`               | true                    | `UpToDate`           |
/// | `Ok(true)`               | false                   | `Advanced`           |
/// | `Ok(false)` or `Err(_)`  | any                     | `InvariantViolation` |
///
/// The `(Ok(true), new_head==prev_head)` row: `remote_head` is an ancestor of
/// `new_head == prev_head`, meaning the clone was already ahead of
/// `origin/main` before the fetch. The ff-only merge was a no-op. Correct
/// outcome: `UpToDate`. See §3.1 of the design doc.
///
/// `Ok(false)` and `Err(_)` both map to `InvariantViolation`: we cannot safely
/// continue with an unclassified HEAD state. See §3.2.
fn classify_post_merge(
    prev_head: &str,
    // Not examined inside this function — the ancestry relationship has already
    // been precomputed by `is_ancestor`. Present in the signature so the call
    // site is self-documenting and future variants that need `remote_head`
    // directly (e.g., for structured logging) don't silently discard it.
    // The caller already has it for the `panic_invariant_violation` message.
    _remote_head: &str,
    new_head: &str,
    is_ancestor_result: Result<bool, GitSubprocessError>,
) -> PostMergeOutcome {
    match is_ancestor_result {
        Ok(true) => {
            if new_head == prev_head {
                // §3.1: clone was already ahead of origin/main; merge was a no-op.
                PostMergeOutcome::UpToDate
            } else {
                // Case A: concurrent local commit landed between merge and rev-parse.
                PostMergeOutcome::Advanced
            }
        }
        Ok(false) => PostMergeOutcome::InvariantViolation { detail: None },
        Err(e) => PostMergeOutcome::InvariantViolation {
            detail: Some(e.to_string()),
        },
    }
}

/// Panic with full context on a Case B invariant violation.
///
/// Extracted as a named function so the `#[should_panic]` test in the
/// `tests` submodule can invoke the identical code path as production,
/// pinning the panic message wording against regressions. See §2.3 and §4.3
/// of the design doc.
///
/// `detail` is `Some(stderr)` when the ancestry check itself failed (e.g.,
/// unknown SHA from concurrent gc or repo replacement), allowing on-call to
/// distinguish a genuine non-descendant HEAD from object-database corruption.
/// `None` means the ancestry check returned `Ok(false)` cleanly.
fn panic_invariant_violation(
    clone_path: &Path,
    prev: &str,
    remote: &str,
    new: &str,
    detail: Option<&str>,
) -> ! {
    match detail {
        Some(d) => panic!(
            "repo_sync invariant violation: post-merge HEAD is not a descendant of \
             fetched origin/main. Indicates external interference with the managed \
             clone (force-push, history rewrite, fs corruption, or manual reset). \
             clone={clone_path:?} prev_head={prev} remote_head={remote} new_head={new} \
             ancestry_check_error={d:?}"
        ),
        None => panic!(
            "repo_sync invariant violation: post-merge HEAD is not a descendant of \
             fetched origin/main. Indicates external interference with the managed \
             clone (force-push, history rewrite, fs corruption, or manual reset). \
             clone={clone_path:?} prev_head={prev} remote_head={remote} new_head={new}"
        ),
    }
}

/// The outcome of a single clone's sync step.
///
/// We classify explicitly so the reactor can route each case to the right
/// downstream action — notifications only fire on `Advanced` and `Conflict`,
/// auth errors escalate fast, transient errors escalate only after a
/// consecutive-failure count, and `UpToDate` is a no-op.
#[derive(Debug, Clone)]
pub enum PullOutcome {
    /// No new commits. HEAD unchanged after fetch.
    UpToDate,
    /// Fast-forward merge succeeded. The commit range is re-derived by
    /// the reactor's advance-detection path against `last_notified_head`,
    /// which may span more commits than the one pull that triggered this
    /// outcome — so pull_clone doesn't carry the oneline list.
    ///
    /// Carries the post-fetch `origin/main` SHA. The reactor uses it as
    /// the range-end for the pulled oneline and as the value stored in
    /// `last_notified_head`, so a local commit that landed between the
    /// merge and advance-detection reading HEAD doesn't get mis-labeled
    /// as pulled. (The remaining `origin/main..HEAD` delta surfaces as
    /// `local` on the next cycle.) See
    /// `docs/designs/repo-sync-false-pulled-summary.md`.
    Advanced { remote_head: String },
    /// Network / unreachable-remote / server-side hiccup. Polling retries
    /// next cycle. Escalation to the operator only fires after several
    /// consecutive failures (see `reactor::PersistentFailureState`).
    TransientError(String),
    /// Unambiguous server-side auth rejection — wrong key, bad host key,
    /// "authentication failed". Retry is guaranteed not to help; escalates
    /// to the operator immediately.
    AuthError { reason: String, detail: String },
    /// Non-ff, diverged, dirty tree, or any merge failure. Primary pool
    /// gets notified; RO-only clones alert the operator.
    Conflict { reason: String, detail: String },
}

/// Serialize one clone's `PullOutcome` into its per-repo JSON object and the
/// optional advanced-slug signal. Pure: no I/O. The tool/adapter collects the
/// slug into an advanced list to fire repo-sync triggers. This is the shared
/// output contract for the `git-repo-pull` tool and the legacy MCP intercept.
pub(crate) fn pull_outcome_to_json(
    slug: String,
    outcome: PullOutcome,
) -> (serde_json::Value, Option<String>) {
    match outcome {
        PullOutcome::Advanced { .. } => (
            serde_json::json!({
                "slug": slug,
                "ok": true,
                "advanced": true,
            }),
            Some(slug),
        ),
        PullOutcome::UpToDate => (
            serde_json::json!({
                "slug": slug,
                "ok": true,
                "advanced": false,
            }),
            None,
        ),
        PullOutcome::TransientError(detail) => (
            serde_json::json!({
                "slug": slug,
                "ok": false,
                "error_type": "transient",
                "error": detail,
            }),
            None,
        ),
        PullOutcome::AuthError { reason, detail } => (
            serde_json::json!({
                "slug": slug,
                "ok": false,
                "error_type": "auth",
                "error": reason,
                "detail": detail,
            }),
            None,
        ),
        PullOutcome::Conflict { reason, detail } => (
            serde_json::json!({
                "slug": slug,
                "ok": false,
                "error_type": "conflict",
                "error": reason,
                "detail": detail,
            }),
            None,
        ),
    }
}

/// Perform one host-side sync cycle on a single clone.
///
/// Steps:
/// 1. `git fetch origin main` — classifies network/auth failures as
///    `TransientError` before we touch HEAD.
/// 2. `git rev-parse HEAD` before and after; if `fetch` advanced remote ref
///    but HEAD is unchanged, we distinguish "up-to-date" from "advanced" by
///    the post-merge rev-parse.
/// 3. `git merge --ff-only origin/main` — failure here means divergence or
///    a dirty tree → `Conflict`.
/// 4. Capture `git log --oneline prev..new` for the summary, cap at
///    `ONELINE_CAP`, newest-first.
///
/// Important: no `run_in_app_env`. Always calls the host `git` binary with
/// `-C <path>`. Even RO-mounted clones live as regular writable directories
/// on the host — `:ro` only affects the container side.
pub async fn pull_clone(clone_path: &Path) -> PullOutcome {
    if !clone_path.is_dir() {
        // This shouldn't happen — clones are created by `auto_clone_repos`
        // at startup — but we surface it as a transient error rather than
        // panic so one misconfigured slug doesn't kill the manager task.
        return PullOutcome::TransientError(format!(
            "clone path does not exist or is not a directory: {}",
            clone_path.display()
        ));
    }

    // 1. Capture pre-fetch HEAD. A failure here is suspicious — we own this
    //    path and the repo should be valid — so treat as Conflict (operator
    //    needs to look) rather than transient.
    let prev_head = match rev_parse(clone_path, "HEAD").await {
        Ok(sha) => sha,
        Err(e) => {
            return PullOutcome::Conflict {
                reason: "rev-parse HEAD failed".to_string(),
                detail: e,
            };
        }
    };

    // 2. Fetch origin main. Separate sub-process so we can class-match the
    //    stderr on transient-error patterns. No `--prune`; we don't mess
    //    with branches the poller didn't put there.
    //
    match run_git(
        clone_path,
        &["fetch", "origin", "main", "--no-tags", "--quiet"],
    )
    .await
    {
        Ok(_) => {}
        Err(GitSubprocessError::Timeout { .. }) => {
            return PullOutcome::TransientError("fetch timed out".to_string());
        }
        Err(GitSubprocessError::SpawnFailed { source: e, .. }) => {
            return PullOutcome::TransientError(format!("failed to spawn git fetch: {e}"));
        }
        Err(GitSubprocessError::PipeError { source: e, .. }) => {
            return PullOutcome::TransientError(format!("git fetch pipe I/O error: {e}"));
        }
        Err(GitSubprocessError::NonZero { stderr, .. }) => {
            return match classify_fetch_error(&stderr) {
                FetchErrorClass::Auth { reason } => PullOutcome::AuthError {
                    reason,
                    detail: truncate_detail(&stderr, 2048),
                },
                FetchErrorClass::Transient => {
                    PullOutcome::TransientError(truncate_detail(&stderr, 2048))
                }
                FetchErrorClass::Other => PullOutcome::Conflict {
                    reason: "fetch failed".to_string(),
                    detail: truncate_detail(&stderr, 2048),
                },
            };
        }
        Err(GitSubprocessError::OutputTooLarge { stream, .. }) => {
            // High-signal anomaly: > 256 KiB on a git fetch stream indicates
            // an adversarial remote or a corrupted/replaced git binary.
            // Log immediately so the operator sees it on first occurrence,
            // regardless of the reactor's consecutive-failure threshold.
            warn!(
                clone = %clone_path.display(),
                stream = %stream,
                "repo_sync: git fetch output exceeded byte cap — possible adversarial remote or unexpected git binary",
            );
            return PullOutcome::TransientError("git output exceeded byte cap".to_string());
        }
        Err(GitSubprocessError::DecodeError { .. }) => {
            // Non-UTF-8 stdout from git fetch is unexpected; log immediately.
            warn!(
                clone = %clone_path.display(),
                "repo_sync: git fetch stdout was not valid UTF-8 — possible adversarial remote or unexpected git binary",
            );
            return PullOutcome::TransientError("git stdout was not valid UTF-8".to_string());
        }
    }

    // 3. Capture post-fetch remote tip.
    let remote_head = match rev_parse(clone_path, "origin/main").await {
        Ok(sha) => sha,
        Err(e) => {
            return PullOutcome::Conflict {
                reason: "rev-parse origin/main failed".to_string(),
                detail: e,
            };
        }
    };

    if remote_head == prev_head {
        return PullOutcome::UpToDate;
    }

    // 4. Try ff-only merge. Failure here is always a conflict (we just
    //    confirmed the remote moved; local tree must be dirty or diverged).
    match run_git(
        clone_path,
        &["merge", "--ff-only", "origin/main", "--quiet"],
    )
    .await
    {
        Ok(_) => {}
        Err(GitSubprocessError::Timeout { .. }) => {
            // Unusual for a local merge. Report transient so we retry.
            return PullOutcome::TransientError("merge timed out".to_string());
        }
        Err(GitSubprocessError::SpawnFailed { source: e, .. }) => {
            return PullOutcome::TransientError(format!("failed to spawn git merge: {e}"));
        }
        Err(GitSubprocessError::PipeError { source: e, .. }) => {
            return PullOutcome::TransientError(format!("git merge pipe I/O error: {e}"));
        }
        Err(GitSubprocessError::NonZero { stderr, .. }) => {
            let reason = if stderr.contains("Not possible to fast-forward")
                || stderr.contains("not a fast-forward")
                || stderr.contains("diverged")
            {
                "non-fast-forward: local diverged from remote".to_string()
            } else if stderr.contains("local changes") || stderr.contains("would be overwritten") {
                "dirty working tree: local changes block fast-forward".to_string()
            } else {
                "merge --ff-only failed".to_string()
            };
            return PullOutcome::Conflict {
                reason,
                detail: truncate_detail(&stderr, 2048),
            };
        }
        Err(GitSubprocessError::OutputTooLarge { stream, .. }) => {
            warn!(
                clone = %clone_path.display(),
                stream = %stream,
                "repo_sync: git merge output exceeded byte cap — possible adversarial remote or unexpected git binary",
            );
            return PullOutcome::TransientError("git output exceeded byte cap".to_string());
        }
        Err(GitSubprocessError::DecodeError { .. }) => {
            warn!(
                clone = %clone_path.display(),
                "repo_sync: git merge stdout was not valid UTF-8 — possible adversarial remote or unexpected git binary",
            );
            return PullOutcome::TransientError("git stdout was not valid UTF-8".to_string());
        }
    }

    // 5. Post-merge HEAD. Expect it to equal remote_head; sanity-check.
    let new_head = match rev_parse(clone_path, "HEAD").await {
        Ok(sha) => sha,
        Err(e) => {
            // A successful merge followed by an inability to read HEAD is an
            // invariant violation — the repo is in an unknown state. Per
            // CLAUDE.md: panic rather than continuing with corrupted state.
            panic!(
                "invariant violation: git merge succeeded but rev-parse HEAD \
                 failed (clone={clone_path:?}): {e}"
            );
        }
    };
    if new_head != remote_head {
        // Mismatch: `new_head` is not the fetched `origin/main`. Distinguish
        // two cases via an ancestry check:
        //
        //   Case A — concurrent local commit landed between merge and rev-parse
        //   (e.g., `git_ops::repo_commit_and_push` running outside the sync
        //   mutex). `remote_head` is an ancestor of `new_head`. Benign by
        //   design — we still return `Advanced { remote_head }` so the local
        //   delta is not mislabeled as "pulled". See `PullOutcome::Advanced`
        //   docstring and `reactor.rs:402-407`.
        //
        //   Case B — `new_head` is not a descendant of `remote_head`. Requires
        //   external interference (force-push, history rewrite, fs corruption,
        //   manual reset). Per CLAUDE.md: panic immediately.
        //
        // The `new_head == remote_head` short-circuit above means this branch
        // is only reached on a mismatch; the `is_ancestor` subprocess call is
        // therefore not on the common path.
        let is_anc = is_ancestor(clone_path, &remote_head, &new_head).await;
        match classify_post_merge(&prev_head, &remote_head, &new_head, is_anc) {
            PostMergeOutcome::UpToDate => {
                // §3.1: clone was already ahead of origin/main; ff-only merge
                // was a no-op. No new state, no notification owed.
                debug!(
                    clone = %clone_path.display(),
                    "repo_sync: post-merge HEAD == prev_head (clone already ahead of origin/main); UpToDate"
                );
                return PullOutcome::UpToDate;
            }
            PostMergeOutcome::Advanced => {
                // Case A: concurrent local commit. Debug-level only — this is
                // design-intended and fires on every such concurrent write.
                debug!(
                    clone = %clone_path.display(),
                    remote_head = %remote_head,
                    new_head = %new_head,
                    "repo_sync: post-merge HEAD is a descendant of origin/main (concurrent local commit); using remote_head as Advanced payload"
                );
                // Fall through to `PullOutcome::Advanced { remote_head }` below.
                //
                // NOTE: this arm is not covered by any end-to-end test. A
                // genuine Case A requires a concurrent `repo_commit_and_push`
                // to land between `git merge --ff-only` and `rev_parse HEAD`,
                // a window that cannot be injected without altering the production
                // code path (§3.6). The classifier logic that routes here is
                // covered by `classify_case_a_classic` (§4.2), and the
                // fall-through behaviour (no early return) is intentional.
            }
            PostMergeOutcome::InvariantViolation { detail } => {
                panic_invariant_violation(
                    clone_path,
                    &prev_head,
                    &remote_head,
                    &new_head,
                    detail.as_deref(),
                );
            }
        }
    }

    debug!(
        clone = %clone_path.display(),
        prev = %prev_head,
        new = %new_head,
        "repo_sync: pull_clone fast-forwarded"
    );

    PullOutcome::Advanced { remote_head }
}

/// Capture `git log --oneline prev..new`, newest-first, capped at
/// `ONELINE_CAP`. When truncated, replace the last entry with
/// `"... N more (older)"`.
///
/// Exposed to sibling modules so the reactor's advance-detection path can
/// collect onelines for local-HEAD movements that `pull_clone` didn't
/// already surface (manual pull, external commit, etc.).
pub(super) async fn collect_oneline(
    clone_path: &Path,
    prev: &str,
    new: &str,
) -> Result<Vec<String>, String> {
    // `--pretty=format:%h %s` matches `git log --oneline` content without
    // decoration. Explicit format keeps us stable across local git configs.
    let range = format!("{prev}..{new}");
    let args = [
        "log",
        "--pretty=format:%h %s",
        "--no-decorate",
        "--no-merges",
        &range,
    ];
    let out = run_git(clone_path, &args)
        .await
        .map_err(|e| e.to_string())?;
    let mut lines: Vec<String> = out.lines().map(|l| l.to_string()).collect();
    // `git log` already newest-first; that's what the design calls for.
    cap_oneline(&mut lines);
    Ok(lines)
}

/// `git rev-parse <rev>` — returns the trimmed SHA or an error string.
/// Exposed to the crate so both the reactor's advance-detection and
/// the MCP-tool PostToolUse handlers can read HEAD without duplicating
/// subprocess plumbing.
pub(crate) async fn rev_parse(clone_path: &Path, rev: &str) -> Result<String, String> {
    run_git(clone_path, &["rev-parse", rev])
        .await
        .map(|s| s.trim().to_string())
        .map_err(|e| e.to_string())
}

/// `git merge-base --is-ancestor <ancestor> <descendant>` — returns
/// `Ok(true)` if `ancestor` is an ancestor of (or equal to) `descendant`,
/// `Ok(false)` if not, or a `GitSubprocessError` for subprocess failures.
///
/// Both inputs must be valid SHAs that exist in the clone's object database
/// (typically produced by `rev_parse` moments earlier). When both SHAs exist,
/// git exits 0 (ancestor) or 1 (not ancestor); a `NonZero` with exit-1
/// semantics ("not ancestor") is safe to treat as `Ok(false)`. However, git
/// also exits non-zero (typically 128) with a "not a valid object name" or
/// "unknown revision" message when a SHA is not found in the object database —
/// which can happen if a concurrent `git gc --prune=now` or repo replacement
/// invalidates a SHA between `rev_parse` and this call. We distinguish these
/// by inspecting stderr: a non-zero exit whose stderr contains those patterns
/// is propagated as `Err` so the caller treats it as `InvariantViolation` and
/// the panic message includes the raw stderr, making the failure diagnosable.
///
/// Routes through `run_git` (the same hardened path `rev_parse` uses), not
/// the untyped `GitSubprocessError`-flattening wrapper `rev_parse` uses,
/// because the caller (`classify_post_merge`) needs to distinguish `NonZero`
/// from other error variants.
async fn is_ancestor(
    clone_path: &Path,
    ancestor: &str,
    descendant: &str,
) -> Result<bool, GitSubprocessError> {
    match run_git(
        clone_path,
        &["merge-base", "--is-ancestor", ancestor, descendant],
    )
    .await
    {
        Ok(_) => Ok(true),
        Err(GitSubprocessError::NonZero {
            ref stderr,
            exit_code,
            ..
        }) if exit_code == Some(1)
            && !{
                let lc = stderr.to_lowercase();
                lc.contains("not a valid object name")
                    || lc.contains("unknown revision or path")
                    || lc.contains("unknown revision")
            } =>
        {
            // `git merge-base --is-ancestor` exits:
            //   0 → is ancestor (handled above)
            //   1 → not ancestor, no error stderr → Ok(false)
            //   128 → error (bad SHA, corrupt object db, …) → propagate (below)
            //
            // Use exit_code as the primary discriminator; belt-and-suspenders
            // stderr check guards against future git versions that might emit
            // object errors with exit 1.
            Ok(false)
        }
        Err(e) => Err(e),
    }
}

/// Classification of a git fetch failure's stderr.
///
/// Distinct from `PullOutcome` because the merge-step classifier reuses
/// neither the Auth vs Transient split nor the Other-as-Conflict fallback
/// (merge failures are always Conflict).
#[derive(Debug)]
enum FetchErrorClass {
    /// Unambiguous server-side auth rejection. Retry cannot help.
    Auth { reason: String },
    /// Network-shaped failure that may self-heal.
    Transient,
    /// Falls through to `PullOutcome::Conflict` — neither a known auth
    /// string nor a known transient string.
    Other,
}

/// Classify a fetch-step stderr blob.
///
/// **Match order is load-bearing.** Auth strings are checked before
/// transient strings because some server-side behaviors emit both in one
/// blob: a strict sshd rejecting a bad key can log
/// `"Permission denied (publickey). Connection closed by …"`. If
/// `"connection closed by"` matched first, that stderr would classify as
/// Transient and silent-retry forever — re-opening the auth-alert hole
/// this batch closes. See `classify_fetch_error_match_order` test.
///
/// Auth string list is intentionally narrow: only stderr patterns I've
/// seen in the wild from OpenSSH. False negatives here (a real auth
/// failure misclassified as Conflict) produce an operator-facing Warning
/// via the conflict path, which is preferable to false positives (adding
/// a string we've never seen just in case).
///
/// Transient list mirrors the pre-refactor `looks_transient` minus the
/// auth strings, plus `"connection closed by"` — which was the primary
/// misclassification bug this batch fixes (sshd MaxStartups throttling).
fn classify_fetch_error(stderr: &str) -> FetchErrorClass {
    let lc = stderr.to_ascii_lowercase();

    // Auth first. Order-sensitive vs Transient — see docstring.
    if lc.contains("permission denied (publickey") {
        return FetchErrorClass::Auth {
            reason: "ssh publickey rejected".to_string(),
        };
    }
    if lc.contains("authentication failed") {
        return FetchErrorClass::Auth {
            reason: "authentication failed".to_string(),
        };
    }
    if lc.contains("host key verification failed") {
        return FetchErrorClass::Auth {
            reason: "host key verification failed".to_string(),
        };
    }

    // Transient. Network, DNS, generic timeout, sshd-dropping-TCP
    // (covers MaxStartups throttling plus the post-auth-rejection drop
    // that can arrive without the "permission denied" prefix).
    if lc.contains("could not resolve host")
        || lc.contains("temporary failure in name resolution")
        || lc.contains("connection refused")
        || lc.contains("connection reset")
        || lc.contains("connection timed out")
        || lc.contains("connection closed by")
        || lc.contains("network is unreachable")
        || lc.contains("timed out")
    {
        return FetchErrorClass::Transient;
    }

    FetchErrorClass::Other
}

/// Short SHA for log/placeholder display (7 chars or fewer).
pub(super) fn short(sha: &str) -> &str {
    if sha.len() >= 7 { &sha[..7] } else { sha }
}

/// Synthesize an "oneline unavailable" placeholder for the range
/// `prev..new`. Used by both `pull_clone` (post-merge fallback) and the
/// reactor's advance-detection when we observe HEAD movement but can't
/// read the commit range (e.g., history rewrite, git-log failure).
/// Keeping one source of truth so the text the LLM sees is consistent.
pub(super) fn oneline_unavailable(prev: &str, new: &str) -> Vec<String> {
    vec![format!(
        "{}..{} <oneline unavailable>",
        short(prev),
        short(new)
    )]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo_sync::test_git_fixtures::{run_git, scratch_remote_and_clone};

    #[test]
    fn pull_outcome_to_json_shapes() {
        struct Case {
            label: &'static str,
            outcome: PullOutcome,
            expected_json: serde_json::Value,
            expected_advanced_slug: Option<&'static str>,
        }

        let slug = "myrepo";

        let cases = vec![
            Case {
                label: "UpToDate",
                outcome: PullOutcome::UpToDate,
                expected_json: serde_json::json!({
                    "slug": slug,
                    "ok": true,
                    "advanced": false,
                }),
                expected_advanced_slug: None,
            },
            Case {
                label: "Advanced",
                outcome: PullOutcome::Advanced {
                    remote_head: "abc123".to_string(),
                },
                expected_json: serde_json::json!({
                    "slug": slug,
                    "ok": true,
                    "advanced": true,
                }),
                expected_advanced_slug: Some(slug),
            },
            Case {
                label: "TransientError",
                outcome: PullOutcome::TransientError("connection refused".to_string()),
                expected_json: serde_json::json!({
                    "slug": slug,
                    "ok": false,
                    "error_type": "transient",
                    "error": "connection refused",
                }),
                expected_advanced_slug: None,
            },
            Case {
                label: "AuthError",
                outcome: PullOutcome::AuthError {
                    reason: "Permission denied (publickey).".to_string(),
                    detail: "fatal: Could not read from remote repository.".to_string(),
                },
                expected_json: serde_json::json!({
                    "slug": slug,
                    "ok": false,
                    "error_type": "auth",
                    "error": "Permission denied (publickey).",
                    "detail": "fatal: Could not read from remote repository.",
                }),
                expected_advanced_slug: None,
            },
            Case {
                label: "Conflict",
                outcome: PullOutcome::Conflict {
                    reason: "CONFLICT (content): Merge conflict in foo.txt".to_string(),
                    detail: "Automatic merge failed; fix conflicts and then commit.".to_string(),
                },
                expected_json: serde_json::json!({
                    "slug": slug,
                    "ok": false,
                    "error_type": "conflict",
                    "error": "CONFLICT (content): Merge conflict in foo.txt",
                    "detail": "Automatic merge failed; fix conflicts and then commit.",
                }),
                expected_advanced_slug: None,
            },
        ];

        for case in cases {
            let (json, advanced_slug) = pull_outcome_to_json(slug.to_string(), case.outcome);
            assert_eq!(
                json, case.expected_json,
                "JSON shape mismatch for variant {}",
                case.label,
            );
            assert_eq!(
                advanced_slug.as_deref(),
                case.expected_advanced_slug,
                "advanced-slug signal mismatch for variant {}",
                case.label,
            );
        }
    }

    #[tokio::test]
    async fn pull_clone_up_to_date() {
        let (_remote, clone) = scratch_remote_and_clone();
        let outcome = pull_clone(clone.path()).await;
        assert!(
            matches!(outcome, PullOutcome::UpToDate),
            "expected UpToDate, got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn pull_clone_classifies_fast_forward_as_advanced() {
        // `Advanced` is a unit variant now — the reactor's advance-detection
        // is responsible for the oneline range. Here we verify that a real
        // fast-forward pull lands the clone on the remote's HEAD and that
        // pull_clone reports `Advanced`. `collect_oneline` is tested
        // separately below.
        let (remote, clone) = scratch_remote_and_clone();
        let pusher = tempfile::tempdir().unwrap();
        std::fs::remove_dir_all(pusher.path()).unwrap();
        run_git(
            std::path::Path::new("/tmp"),
            &[
                "clone",
                &remote.path().display().to_string(),
                pusher.path().to_str().unwrap(),
            ],
        );
        std::fs::write(pusher.path().join("a.txt"), "a").unwrap();
        run_git(pusher.path(), &["add", "."]);
        run_git(pusher.path(), &["commit", "-m", "add a"]);
        run_git(pusher.path(), &["push", "origin", "main"]);

        let outcome = pull_clone(clone.path()).await;
        assert!(
            matches!(outcome, PullOutcome::Advanced { .. }),
            "expected Advanced, got {outcome:?}",
        );

        // Sanity: local HEAD now equals remote HEAD.
        let local = rev_parse(clone.path(), "HEAD").await.unwrap();
        let remote_ref = rev_parse(clone.path(), "origin/main").await.unwrap();
        assert_eq!(local, remote_ref);
    }

    #[tokio::test]
    async fn collect_oneline_captures_range_newest_first() {
        // Directly exercises the helper the reactor uses for advance-
        // detection's commit summary.
        let (remote, clone) = scratch_remote_and_clone();
        let pusher = tempfile::tempdir().unwrap();
        std::fs::remove_dir_all(pusher.path()).unwrap();
        run_git(
            std::path::Path::new("/tmp"),
            &[
                "clone",
                &remote.path().display().to_string(),
                pusher.path().to_str().unwrap(),
            ],
        );
        std::fs::write(pusher.path().join("a.txt"), "a").unwrap();
        run_git(pusher.path(), &["add", "."]);
        run_git(pusher.path(), &["commit", "-m", "add a"]);
        std::fs::write(pusher.path().join("b.txt"), "b").unwrap();
        run_git(pusher.path(), &["add", "."]);
        run_git(pusher.path(), &["commit", "-m", "add b"]);
        run_git(pusher.path(), &["push", "origin", "main"]);

        // Capture prev before pulling.
        let prev = rev_parse(clone.path(), "HEAD").await.unwrap();
        let outcome = pull_clone(clone.path()).await;
        assert!(matches!(outcome, PullOutcome::Advanced { .. }));
        let new = rev_parse(clone.path(), "HEAD").await.unwrap();

        let lines = collect_oneline(clone.path(), &prev, &new).await.unwrap();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].ends_with("add b"), "got {lines:?}");
        assert!(lines[1].ends_with("add a"), "got {lines:?}");
    }

    #[tokio::test]
    async fn pull_clone_conflict_on_divergence() {
        let (remote, clone) = scratch_remote_and_clone();

        // Push from a sibling.
        let pusher = tempfile::tempdir().unwrap();
        std::fs::remove_dir_all(pusher.path()).unwrap();
        run_git(
            std::path::Path::new("/tmp"),
            &[
                "clone",
                &remote.path().display().to_string(),
                pusher.path().to_str().unwrap(),
            ],
        );
        std::fs::write(pusher.path().join("upstream.txt"), "up").unwrap();
        run_git(pusher.path(), &["add", "."]);
        run_git(pusher.path(), &["commit", "-m", "upstream commit"]);
        run_git(pusher.path(), &["push", "origin", "main"]);

        // Create a diverging commit locally in the clone.
        std::fs::write(clone.path().join("local.txt"), "local").unwrap();
        run_git(clone.path(), &["add", "."]);
        run_git(clone.path(), &["commit", "-m", "local divergent"]);

        let outcome = pull_clone(clone.path()).await;
        match outcome {
            PullOutcome::Conflict { reason, .. } => {
                assert!(
                    reason.contains("non-fast-forward") || reason.contains("merge --ff-only"),
                    "unexpected reason: {reason}"
                );
            }
            other => panic!("expected Conflict, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn pull_clone_transient_on_unreachable_remote() {
        // Clone against a remote that will fail to resolve.
        let scratch = tempfile::tempdir().unwrap();
        run_git(scratch.path(), &["init", "-b", "main"]);
        std::fs::write(scratch.path().join("x"), "x").unwrap();
        run_git(scratch.path(), &["add", "."]);
        run_git(scratch.path(), &["commit", "-m", "initial"]);
        run_git(
            scratch.path(),
            &[
                "remote",
                "add",
                "origin",
                "ssh://git@definitely-nonexistent.example.invalid/none.git",
            ],
        );

        let outcome = pull_clone(scratch.path()).await;
        // Most CI / dev environments will fail DNS (transient). If somehow
        // we end up with a different failure shape, we still want the test
        // to acknowledge *some* non-success classification.
        match outcome {
            PullOutcome::TransientError(_) => {}
            PullOutcome::Conflict { .. } => {
                // Acceptable fallback — if resolver isn't available and git
                // classifies it as something we didn't allowlist, we want
                // the test to still observe a non-success path.
            }
            other => panic!("expected TransientError or Conflict, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn pull_clone_missing_directory_returns_transient_error() {
        // The manager shouldn't panic if a clone dir is somehow missing —
        // the right classification is TransientError so polling retries.
        let nonexistent = std::path::Path::new("/tmp/brenn-nonexistent-dir-for-test-xyz789");
        // Make sure it really doesn't exist.
        assert!(
            !nonexistent.exists(),
            "test requires /tmp/brenn-nonexistent-dir-for-test-xyz789 to NOT exist"
        );
        let outcome = pull_clone(nonexistent).await;
        match outcome {
            PullOutcome::TransientError(msg) => {
                assert!(
                    msg.contains("does not exist"),
                    "expected 'does not exist' in message, got {msg:?}"
                );
            }
            other => panic!("expected TransientError, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn pull_clone_dirty_tree_is_conflict_with_dirty_reason() {
        // If the working tree has uncommitted local changes to a tracked
        // file that conflicts with an incoming upstream change, merge
        // --ff-only fails with a "would be overwritten" message. The
        // classifier should tag this as a dirty-tree Conflict, not a
        // diverged-branch conflict, so the LLM surfaces the right advice.
        let (remote, clone) = scratch_remote_and_clone();

        // Push an upstream change to readme.md (which exists in the clone).
        let pusher = tempfile::tempdir().unwrap();
        std::fs::remove_dir_all(pusher.path()).unwrap();
        run_git(
            std::path::Path::new("/tmp"),
            &[
                "clone",
                &remote.path().display().to_string(),
                pusher.path().to_str().unwrap(),
            ],
        );
        std::fs::write(pusher.path().join("readme.md"), "upstream content").unwrap();
        run_git(pusher.path(), &["add", "."]);
        run_git(pusher.path(), &["commit", "-m", "upstream readme"]);
        run_git(pusher.path(), &["push", "origin", "main"]);

        // Dirty the same file locally — unstaged.
        std::fs::write(clone.path().join("readme.md"), "local dirty").unwrap();

        let outcome = pull_clone(clone.path()).await;
        match outcome {
            PullOutcome::Conflict { reason, detail } => {
                assert!(
                    reason.contains("dirty") || reason.contains("local changes"),
                    "expected dirty-tree reason, got {reason:?}; detail={detail:?}"
                );
            }
            other => panic!("expected Conflict, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn collect_oneline_caps_at_ten_with_overflow_marker() {
        // Verifies integration between `collect_oneline` and `cap_oneline`
        // end-to-end: the output is bounded at ONELINE_CAP with the
        // "... N more (older)" sentinel on overflow.
        use brenn_lib::messaging::ONELINE_CAP;

        let (remote, clone) = scratch_remote_and_clone();
        let pusher = tempfile::tempdir().unwrap();
        std::fs::remove_dir_all(pusher.path()).unwrap();
        run_git(
            std::path::Path::new("/tmp"),
            &[
                "clone",
                &remote.path().display().to_string(),
                pusher.path().to_str().unwrap(),
            ],
        );
        for i in 0..12 {
            std::fs::write(pusher.path().join(format!("f{i}.txt")), "x").unwrap();
            run_git(pusher.path(), &["add", "."]);
            run_git(pusher.path(), &["commit", "-m", &format!("commit {i}")]);
        }
        run_git(pusher.path(), &["push", "origin", "main"]);

        let prev = rev_parse(clone.path(), "HEAD").await.unwrap();
        let outcome = pull_clone(clone.path()).await;
        assert!(matches!(outcome, PullOutcome::Advanced { .. }));
        let new = rev_parse(clone.path(), "HEAD").await.unwrap();

        let lines = collect_oneline(clone.path(), &prev, &new).await.unwrap();
        assert_eq!(lines.len(), ONELINE_CAP);
        let last = lines.last().expect("at least one entry");
        assert!(
            last.starts_with("... ") && last.contains("more (older)"),
            "expected overflow marker, got {last:?}",
        );
    }

    #[test]
    fn oneline_unavailable_formats_short_range() {
        let lines = oneline_unavailable(
            "abcdef1234567890abcdef1234567890abcdef12",
            "1234567890abcdef1234567890abcdef12345678",
        );
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0], "abcdef1..1234567 <oneline unavailable>");
    }

    #[test]
    fn oneline_unavailable_handles_short_sha_input() {
        let lines = oneline_unavailable("abc", "def");
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0], "abc..def <oneline unavailable>");
    }

    #[test]
    fn truncate_handles_utf8_boundary() {
        // A multibyte char at the cut point shouldn't panic.
        let s = "héllo world";
        let t = truncate_detail(s, 5);
        assert!(t.ends_with("..."), "got {t:?}");
    }

    #[test]
    fn classify_fetch_error_auth_patterns() {
        // Every pattern we claim to recognize as Auth must classify as Auth.
        // If this list grows, `classify_fetch_error_match_order` must gain
        // a corresponding regression case.
        let auth_cases = [
            "fatal: Permission denied (publickey).",
            "Permission denied (publickey,keyboard-interactive).",
            "Authentication failed for 'ssh://git@example/repo'",
            "Host key verification failed.",
            // Case insensitivity.
            "AUTHENTICATION FAILED",
            "HOST KEY VERIFICATION FAILED",
        ];
        for stderr in auth_cases {
            assert!(
                matches!(classify_fetch_error(stderr), FetchErrorClass::Auth { .. }),
                "expected Auth for: {stderr:?}",
            );
        }
    }

    #[test]
    fn classify_fetch_error_transient_patterns() {
        // Every pattern we claim to recognize as Transient must classify
        // as Transient. The "connection closed by" string — newly added
        // for MaxStartups throttling — is the key one: it used to fall
        // through to Conflict, which is the bug this refactor fixes.
        let transient_cases = [
            "fatal: Could not resolve host: example.com",
            "Temporary failure in name resolution",
            "ssh: connect to host port 22: Connection refused",
            "Connection reset by peer",
            "Connection timed out",
            "Connection closed by 1.2.3.4 port 22",
            "Network is unreachable",
            "ssh_exchange_identification: read: timed out",
        ];
        for stderr in transient_cases {
            assert!(
                matches!(classify_fetch_error(stderr), FetchErrorClass::Transient),
                "expected Transient for: {stderr:?}",
            );
        }
    }

    #[test]
    fn classify_fetch_error_unmatched_falls_through_to_other() {
        // Anything we don't recognize as Auth or Transient must be Other,
        // which `pull_clone` routes to `PullOutcome::Conflict`. False
        // positives are worse than false negatives (see docstring): a real
        // operator incident classifying as Other surfaces it; a real
        // network blip classifying as Other alerts too early but still
        // visibly.
        let other_cases = [
            "fatal: not a git repository",
            "fatal: couldn't find remote ref main",
            "error: RPC failed; HTTP 500",
            "",
            "unexpected stderr we have never observed",
        ];
        for stderr in other_cases {
            assert!(
                matches!(classify_fetch_error(stderr), FetchErrorClass::Other),
                "expected Other for: {stderr:?}",
            );
        }
    }

    #[test]
    fn classify_fetch_error_match_order() {
        // Match order is load-bearing (see `classify_fetch_error`
        // docstring): a stderr matching both an Auth pattern and a
        // Transient pattern must classify as Auth. Concrete in-the-wild
        // example — a strict sshd denies auth then closes the TCP
        // session, emitting both strings.
        let combined = "ssh: Permission denied (publickey).\nConnection closed by 1.2.3.4 port 22";
        match classify_fetch_error(combined) {
            FetchErrorClass::Auth { .. } => {}
            other => panic!("expected Auth, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // is_ancestor helper tests (§4.1)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn is_ancestor_descendant_chain() {
        use crate::repo_sync::test_git_fixtures::{head, local_commit};
        let (_remote, clone) = scratch_remote_and_clone();
        // Capture the parent SHA, make a commit, then capture the child SHA.
        let parent = head(clone.path());
        local_commit(clone.path(), "child.txt", "child commit");
        let child = head(clone.path());
        // Forward direction: parent is ancestor of child.
        let result = is_ancestor(clone.path(), &parent, &child).await;
        assert!(result.unwrap(), "parent should be an ancestor of child");
        // Reverse direction: child is NOT ancestor of parent.
        // This also catches argument-order inversions in the run_git call.
        let result_rev = is_ancestor(clone.path(), &child, &parent).await;
        assert!(
            !result_rev.unwrap(),
            "child should not be an ancestor of parent"
        );
    }

    #[tokio::test]
    async fn is_ancestor_unrelated_tips() {
        use crate::repo_sync::test_git_fixtures::orphan_commit;
        let (_remote, clone) = scratch_remote_and_clone();
        let orphan_a = orphan_commit(clone.path(), "orphan-a", "orphan a commit");
        let orphan_b = orphan_commit(clone.path(), "orphan-b", "orphan b commit");
        // Unrelated orphan branches: neither is an ancestor of the other.
        // Test both directions to catch argument-order inversions in the run_git call.
        let result_a_b = is_ancestor(clone.path(), &orphan_a, &orphan_b).await;
        assert!(
            !result_a_b.unwrap(),
            "orphan_a should not be an ancestor of orphan_b"
        );
        let result_b_a = is_ancestor(clone.path(), &orphan_b, &orphan_a).await;
        assert!(
            !result_b_a.unwrap(),
            "orphan_b should not be an ancestor of orphan_a"
        );
    }

    #[tokio::test]
    async fn is_ancestor_reflexive() {
        // A commit is its own ancestor per git merge-base semantics.
        // Note: pull_clone short-circuits new_head==remote_head before calling
        // is_ancestor, so this case is only exercised by future callers.
        use crate::repo_sync::test_git_fixtures::head;
        let (_remote, clone) = scratch_remote_and_clone();
        let sha = head(clone.path());
        let result = is_ancestor(clone.path(), &sha, &sha).await;
        assert!(result.unwrap(), "commit should be its own ancestor");
    }

    // -----------------------------------------------------------------------
    // classify_post_merge unit tests (§4.2)
    // -----------------------------------------------------------------------

    const PREV: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const REMOTE: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const NEW: &str = "cccccccccccccccccccccccccccccccccccccccc";

    #[test]
    fn classify_case_a_classic() {
        // Distinct prev/remote/new, ancestor=Ok(true) → Advanced.
        let result = classify_post_merge(PREV, REMOTE, NEW, Ok(true));
        assert_eq!(result, PostMergeOutcome::Advanced);
    }

    #[test]
    fn classify_case_a_local_already_ahead() {
        // new == prev (clone already ahead), ancestor=Ok(true) → UpToDate.
        // §3.1: ff-only merge was a no-op because clone already contained origin/main.
        let result = classify_post_merge(PREV, REMOTE, PREV, Ok(true));
        assert_eq!(result, PostMergeOutcome::UpToDate);
    }

    #[test]
    fn classify_case_b_non_descendant() {
        // Distinct prev/remote/new, ancestor=Ok(false) → InvariantViolation { detail: None }.
        // Ok(false) means the ancestry check ran cleanly and returned "not an ancestor";
        // no stderr to propagate, so detail is None.
        let result = classify_post_merge(PREV, REMOTE, NEW, Ok(false));
        assert_eq!(
            result,
            PostMergeOutcome::InvariantViolation { detail: None }
        );
    }

    #[test]
    fn classify_case_b_non_descendant_no_move() {
        // new == prev, ancestor=Ok(false) → InvariantViolation { detail: None } (not UpToDate).
        // External interference reset HEAD back to prev; the ancestor check
        // distinguishes this from the benign "clone already ahead" case.
        let result = classify_post_merge(PREV, REMOTE, PREV, Ok(false));
        assert_eq!(
            result,
            PostMergeOutcome::InvariantViolation { detail: None }
        );
    }

    #[test]
    fn classify_case_b_subprocess_error() {
        // Classifier subprocess failure → InvariantViolation { detail: Some(...) }.
        // The Err payload is propagated into detail so the panic message can
        // distinguish "ancestry check returned false" from "ancestry check crashed".
        // Only `Timeout` is tested here because the `Err(_)` arm is structurally
        // exhaustive — all `GitSubprocessError` variants route to the same arm.
        // If a future refactor accidentally adds a variant-specific branch that
        // returns a different outcome, the compiler will enforce exhaustiveness;
        // no additional test variant is needed to catch that class of regression.
        let result = classify_post_merge(
            PREV,
            REMOTE,
            NEW,
            Err(GitSubprocessError::Timeout {
                label: "test".to_string(),
            }),
        );
        assert!(
            matches!(
                result,
                PostMergeOutcome::InvariantViolation { detail: Some(_) }
            ),
            "expected InvariantViolation with Some detail, got {result:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Panic site test (§4.3)
    // -----------------------------------------------------------------------

    #[test]
    #[should_panic(expected = "repo_sync invariant violation: post-merge HEAD is not a descendant")]
    fn panic_invariant_violation_emits_expected_message() {
        // Exercises the same code path production uses (detail=None, the clean
        // non-descendant case). Substring match so future field additions to
        // the message don't break this test.
        panic_invariant_violation(
            std::path::Path::new("/tmp/test-clone"),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "cccccccccccccccccccccccccccccccccccccccc",
            None,
        );
    }

    #[test]
    #[should_panic(expected = "ancestry_check_error=")]
    fn panic_invariant_violation_includes_detail_when_present() {
        // When detail is Some (ancestry check crashed, e.g., unknown SHA),
        // the panic message must include the raw stderr so on-call can
        // distinguish object-DB corruption from a genuine non-descendant HEAD.
        panic_invariant_violation(
            std::path::Path::new("/tmp/test-clone"),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "cccccccccccccccccccccccccccccccccccccccc",
            Some("fatal: not a valid object name 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'"),
        );
    }
}
