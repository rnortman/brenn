//! Hook mode resolves the repo from the write *destination*, not from where the
//! session sits. These drive the real binary through the destination cases: a
//! write into a different gated repo than the session's, an ungated destination,
//! a relative path, a brand-new file, and a per-repo path allowlist.
//!
//! The scan-reaching cases run the real pinned gitleaks against small custom
//! configs, so each repo catches a distinguishable token and nothing else.
//! Skipped with a message when the pinned gitleaks is absent, matching
//! `rules.rs` and `modes.rs`.
//!
//! Fixture tokens and the rule regexes that match them are assembled at runtime,
//! so this file never contains a literal a gate would flag.

mod common;

use std::path::Path;

use common::{Output, gated_repo, git_init, gitleaks_available};

/// A fixture token the matching config catches, assembled so the literal never
/// appears whole in this file.
fn token(tag: &str) -> String {
    format!("SCRUBTEST{tag}{}", "MARKER")
}

/// A standalone config (no `useDefault`) whose one rule matches `token(tag)` and
/// nothing else, so a repo built with it is distinguishable from any other.
fn config_for(tag: &str) -> String {
    let tok = token(tag);
    format!(
        "title = \"scrub test {tag}\"\n\n\
         [[rules]]\n\
         id = \"scrub-test-{id}\"\n\
         description = \"test token {tag}\"\n\
         regex = '''{tok}'''\n\
         keywords = [\"{tok}\"]\n",
        id = tag.to_lowercase(),
    )
}

/// Like `config_for`, plus a path allowlist exempting Markdown files.
fn config_with_md_allowlist(tag: &str) -> String {
    format!(
        "{}  [[rules.allowlists]]\n  paths = ['''\\.md$''']\n",
        config_for(tag)
    )
}

/// A git repo with no `.gitleaks.toml` — a real repo that never opted in, so a
/// destination inside it is ungated.
fn ungated_repo() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("temp dir");
    git_init(dir.path());
    dir
}

fn dirty_body(tag: &str) -> String {
    format!("let leak = \"{}\";\n", token(tag))
}

/// A PreToolUse Write payload. `cwd` is the session's directory (the top-level
/// field the hook reads only to absolutize a relative `file_path`); `file_path`
/// is passed through verbatim so callers can supply absolute or relative paths.
fn payload(cwd: Option<&Path>, file_path: &str, body: &str) -> String {
    let mut p = serde_json::json!({
        "tool_name": "Write",
        "tool_input": {"file_path": file_path, "content": body}
    });
    if let Some(c) = cwd {
        p["cwd"] = serde_json::json!(c.to_str().expect("utf-8 cwd"));
    }
    p.to_string()
}

/// One `brenn-scrub hook` run. `process_cwd` sets the child's working directory,
/// which the hook consults only as the fallback when the payload omits `cwd`.
fn run_hook(stdin: &str, process_cwd: Option<&Path>) -> Output {
    common::run(&["hook"], stdin, &[], None, process_cwd)
}

fn abs(dir: &Path, rel: &str) -> String {
    dir.join(rel).to_str().expect("utf-8 path").to_string()
}

/// A write is scanned against the repo it lands in, not the repo the session
/// sits in. Session in A, write into B: a string only B's rule catches blocks
/// (and is reported at a B-relative path); a string only A's rule catches
/// passes, because B's config is the one that runs.
#[test]
fn a_write_is_scanned_against_the_destination_repo_not_the_session_repo() {
    if !gitleaks_available() {
        return;
    }
    let repo_a = gated_repo(&config_for("AAAA"));
    let repo_b = gated_repo(&config_for("BBBB"));

    let blocked = run_hook(
        &payload(
            Some(repo_a.path()),
            &abs(repo_b.path(), "src/leak.rs"),
            &dirty_body("BBBB"),
        ),
        None,
    );
    assert_eq!(
        blocked.code,
        Some(2),
        "a violation of the destination repo's rule must block; stderr: {}",
        blocked.stderr
    );
    assert!(
        blocked.stderr.contains("src/leak.rs"),
        "the finding must be reported at a destination-repo-relative path: {}",
        blocked.stderr
    );
    assert!(
        blocked.stderr.contains("scrub-test-bbbb"),
        "the destination repo's rule must be the one that fired: {}",
        blocked.stderr
    );

    let passed = run_hook(
        &payload(
            Some(repo_a.path()),
            &abs(repo_b.path(), "src/leak.rs"),
            &dirty_body("AAAA"),
        ),
        None,
    );
    assert_eq!(
        passed.code,
        Some(0),
        "the session repo's rule must not apply to a write into another repo; stderr: {}",
        passed.stderr
    );
}

/// A write into a directory that is not a git repo passes untouched, with an
/// audit line naming the destination — even though the session sits in a gated
/// repo whose rule the content would violate.
#[test]
fn a_write_into_an_ungated_directory_passes_with_an_audit_line() {
    let session = gated_repo(&config_for("BBBB"));
    let scratch = tempfile::tempdir().expect("temp dir");

    let out = run_hook(
        &payload(
            Some(session.path()),
            &abs(scratch.path(), "note.rs"),
            &dirty_body("BBBB"),
        ),
        None,
    );
    assert_eq!(
        out.code,
        Some(0),
        "an ungated destination must pass regardless of the session repo; stderr: {}",
        out.stderr
    );
    assert!(
        out.stderr.contains("not inside a gated repo") && out.stderr.contains("note.rs"),
        "the pass must be audited and name the destination: {}",
        out.stderr
    );
}

/// The hook handles `Edit` as well as `Write`; this exercises the Edit path
/// (added text read from `new_string`) end to end.
#[test]
fn an_edit_into_an_ungated_directory_passes() {
    let scratch = tempfile::tempdir().expect("temp dir");
    let stdin = serde_json::json!({
        "tool_name": "Edit",
        "tool_input": {"file_path": abs(scratch.path(), "note.rs"), "new_string": "let x = 1;\n"}
    })
    .to_string();
    let out = run_hook(&stdin, None);
    assert_eq!(
        out.code,
        Some(0),
        "an Edit into an ungated destination must pass; stderr: {}",
        out.stderr
    );
    assert!(
        out.stderr.contains("not inside a gated repo"),
        "the ungated Edit pass must be audited: {}",
        out.stderr
    );
}

/// The Edit path reaches the scanner too: a dirty `new_string` written into a
/// gated repo is scanned against that repo's config and blocks, reported at the
/// repo-relative path. Guards the flow of Edit's added text from `hook::extract`
/// through the mirror into the scan, which the ungated Edit case cannot see.
#[test]
fn an_edit_into_a_gated_repo_is_scanned_and_blocks() {
    if !gitleaks_available() {
        return;
    }
    let repo = gated_repo(&config_for("BBBB"));
    let stdin = serde_json::json!({
        "tool_name": "Edit",
        "tool_input": {
            "file_path": abs(repo.path(), "src/leak.rs"),
            "new_string": dirty_body("BBBB"),
        }
    })
    .to_string();
    let out = run_hook(&stdin, None);
    assert_eq!(
        out.code,
        Some(2),
        "a dirty Edit into a gated repo must block; stderr: {}",
        out.stderr
    );
    assert!(
        out.stderr.contains("src/leak.rs"),
        "the Edit finding must be at the destination-repo-relative path: {}",
        out.stderr
    );
    assert!(
        out.stderr.contains("scrub-test-bbbb"),
        "the destination repo's rule must be the one that fired: {}",
        out.stderr
    );
}

/// A write aimed at a gated repo's `.git/` internals cannot resolve to a work
/// tree: `git rev-parse --show-toplevel` fails with an error distinct from
/// "not a git repository", so the gating probe panics and the hook blocks
/// rather than ungated-passing a write into repo internals.
#[test]
fn a_write_into_git_internals_blocks() {
    let repo = gated_repo(&config_for("BBBB"));
    let out = run_hook(
        &payload(
            None,
            &abs(repo.path(), ".git/hooks/pre-commit"),
            "let x = 1;\n",
        ),
        None,
    );
    assert_eq!(
        out.code,
        Some(2),
        "a write into .git internals must fail closed and block; stderr: {}",
        out.stderr
    );
}

/// A repo that never carried `.gitleaks.toml` is ungated the same way a
/// non-repo is: the config file is the opt-in marker, so its absence means
/// "not scrub's business", not "broken install".
#[test]
fn a_write_into_a_repo_without_a_config_passes() {
    let repo = ungated_repo();
    let out = run_hook(
        &payload(None, &abs(repo.path(), "src/note.rs"), "let x = 1;\n"),
        None,
    );
    assert_eq!(
        out.code,
        Some(0),
        "a repo without the opt-in config is ungated; stderr: {}",
        out.stderr
    );
    assert!(
        out.stderr.contains("not inside a gated repo"),
        "the ungated pass must be audited: {}",
        out.stderr
    );
}

/// Repo identity comes from the destination even when the session is nowhere
/// near a repo: process CWD outside any repo, absolute destination inside a
/// gated repo, and the write is still scanned (and blocks on a violation).
#[test]
fn a_write_from_a_session_outside_any_repo_is_scanned_against_the_destination() {
    if !gitleaks_available() {
        return;
    }
    let repo = gated_repo(&config_for("BBBB"));
    let outside = tempfile::tempdir().expect("temp dir");

    let out = run_hook(
        &payload(None, &abs(repo.path(), "src/leak.rs"), &dirty_body("BBBB")),
        Some(outside.path()),
    );
    assert_eq!(
        out.code,
        Some(2),
        "a gated destination must be scanned even from a session outside any repo; stderr: {}",
        out.stderr
    );
}

/// A relative `file_path` is absolutized against the payload `cwd`, landing it
/// inside that repo, where it is scanned.
#[test]
fn a_relative_file_path_is_resolved_against_the_payload_cwd() {
    if !gitleaks_available() {
        return;
    }
    let repo = gated_repo(&config_for("BBBB"));
    let out = run_hook(
        &payload(Some(repo.path()), "src/leak.rs", &dirty_body("BBBB")),
        None,
    );
    assert_eq!(
        out.code,
        Some(2),
        "a relative path must resolve into the cwd's repo and be scanned; stderr: {}",
        out.stderr
    );
    assert!(
        out.stderr.contains("src/leak.rs"),
        "the finding must be at the resolved repo-relative path: {}",
        out.stderr
    );
}

/// A brand-new file, whose full path does not yet exist, resolves via its
/// nearest existing ancestor and is scanned like any other write.
#[test]
fn a_new_file_in_a_gated_repo_is_scanned() {
    if !gitleaks_available() {
        return;
    }
    let repo = gated_repo(&config_for("BBBB"));
    let out = run_hook(
        &payload(
            Some(repo.path()),
            &abs(repo.path(), "src/does/not/exist/yet.rs"),
            &dirty_body("BBBB"),
        ),
        None,
    );
    assert_eq!(
        out.code,
        Some(2),
        "a non-existent destination tail must still resolve and scan; stderr: {}",
        out.stderr
    );
}

/// The destination repo's own path allowlist decides: the same content passes at
/// an allowlisted path and blocks at a non-allowlisted one. That the allowlist
/// applies at all requires the mirror path to be repo-relative.
#[test]
fn the_destination_repos_path_allowlist_is_honored() {
    if !gitleaks_available() {
        return;
    }
    let repo = gated_repo(&config_with_md_allowlist("BBBB"));

    let allowed = run_hook(
        &payload(
            Some(repo.path()),
            &abs(repo.path(), "docs/note.md"),
            &dirty_body("BBBB"),
        ),
        None,
    );
    assert_eq!(
        allowed.code,
        Some(0),
        "an allowlisted destination path must pass; stderr: {}",
        allowed.stderr
    );

    let blocked = run_hook(
        &payload(
            Some(repo.path()),
            &abs(repo.path(), "src/note.rs"),
            &dirty_body("BBBB"),
        ),
        None,
    );
    assert_eq!(
        blocked.code,
        Some(2),
        "the same content at a non-allowlisted path must block; stderr: {}",
        blocked.stderr
    );
}
