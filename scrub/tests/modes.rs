//! End-to-end behavior of the modes whose *contract* is consumed by something
//! other than a human reading stderr: `range`'s warn-only rollout switch,
//! `tree`'s captured stdout, and `staged`'s reliance on the git environment a
//! hook hands it.
//!
//! The first two were covered only at the argument-parsing layer, where an
//! inverted or dropped branch downstream leaves every test green. These drive
//! the real binary against a real repo instead.
//!
//! Skipped with a message when the pinned gitleaks is absent, matching
//! `rules.rs`.

mod common;

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use common::gitleaks_available;
use git_fixture::{git, init_repo, try_git};

const BIN: &str = env!("CARGO_BIN_EXE_brenn-scrub");

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("scrub crate has a parent directory")
        .to_path_buf()
}

/// A token the built-in rules catch, assembled at runtime so this file never
/// contains a literal the gate would flag.
fn canary() -> String {
    format!(
        "let gh = \"{}_{}\";\n",
        "ghp", "A1b2C3d4E5f6G7h8I9j0K1l2M3n4O5p6Q7r8"
    )
}

fn write_file(repo: &Path, rel: &str, body: &str) {
    let path = repo.join(rel);
    std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
    std::fs::write(path, body).expect("write fixture");
}

/// A repo carrying the real public config, so these exercise the shipped rules.
fn repo_with(files: &[(&str, &str)]) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("temp dir");
    let p = dir.path();
    init_repo(p);
    std::fs::copy(repo_root().join(".gitleaks.toml"), p.join(".gitleaks.toml"))
        .expect("copy public config");
    for (rel, body) in files {
        write_file(p, rel, body);
    }
    git(p, &["add", "-A"]);
    git(p, &["commit", "-qm", "fixture"]);
    dir
}

struct Output {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

fn run_in(repo: &Path, args: &[&str], stdin: &str) -> Output {
    run_in_env(repo, args, stdin, &[])
}

fn run_in_env(repo: &Path, args: &[&str], stdin: &str, env: &[(&str, &str)]) -> Output {
    let mut cmd = Command::new(BIN);
    cmd.current_dir(repo)
        // The overlay is a local convention; these assertions are about the
        // public rules only, so a machine's local overlay must not leak in.
        .env_remove("BRENN_SCRUB_DENYLIST")
        .envs(env.iter().copied())
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // The fixture repo is the whole subject here; a `GIT_DIR` from a hook
    // environment would point the spawned binary — and the `gitleaks` it
    // spawns — at some other repo entirely.
    git_fixture::hermetic(&mut cmd);
    let mut child = cmd.spawn().expect("failed to spawn brenn-scrub");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(stdin.as_bytes())
        .expect("write stdin");
    let out = child.wait_with_output().expect("wait");
    Output {
        code: out.status.code(),
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    }
}

/// Pre-push stdin for pushing `main` as a ref the remote does not have.
fn new_ref_line(repo: &Path) -> String {
    let sha = git(repo, &["rev-parse", "HEAD"]).trim().to_string();
    format!("refs/heads/main {sha} refs/heads/main {}\n", "0".repeat(40))
}

/// Pre-push ships warn-only and later flips to enforcing by deleting the flag.
/// Only the flag's *parsing* was asserted, so an inverted branch could leave
/// the push gate off with nothing red -- or block every push before the tree
/// is green.
#[test]
fn warn_only_reports_the_same_findings_it_would_have_blocked_on() {
    if !gitleaks_available() {
        return;
    }
    let dir = repo_with(&[("src/a.rs", &canary())]);
    let stdin = new_ref_line(dir.path());

    let warned = run_in(dir.path(), &["range", "--warn-only"], &stdin);
    assert_eq!(
        warned.code,
        Some(0),
        "warn-only must let the push through; stderr: {}",
        warned.stderr
    );
    assert!(
        warned.stderr.contains("would fail the scrub gate"),
        "the findings must still be visible: {}",
        warned.stderr
    );

    let blocked = run_in(dir.path(), &["range"], &stdin);
    assert_eq!(
        blocked.code,
        Some(1),
        "the same findings must block without the flag; stderr: {}",
        blocked.stderr
    );
    assert!(
        blocked.stderr.contains("blocked this push"),
        "{}",
        blocked.stderr
    );
}

#[test]
fn a_clean_repo_passes_range_either_way() {
    if !gitleaks_available() {
        return;
    }
    let dir = repo_with(&[("src/a.rs", "let user = \"alice\";\n")]);
    let stdin = new_ref_line(dir.path());
    for args in [&["range"][..], &["range", "--warn-only"][..]] {
        let out = run_in(dir.path(), args, &stdin);
        assert_eq!(out.code, Some(0), "stderr: {}", out.stderr);
    }
}

fn tree_json(out: &Output) -> serde_json::Value {
    serde_json::from_str(&out.stdout)
        .unwrap_or_else(|e| panic!("tree stdout must be JSON ({e}): {:?}", out.stdout))
}

/// Tree stdout is captured as the burndown worklist, and it self-documents its
/// scope so the artifact can never be read as covering more than it scanned.
/// A field rename or serializer change would silently break that.
#[test]
fn tree_stdout_carries_findings_and_an_empty_exclusion_list_by_default() {
    if !gitleaks_available() {
        return;
    }
    let dir = repo_with(&[("src/a.rs", &canary())]);
    let out = run_in(dir.path(), &["tree"], "");
    let json = tree_json(&out);

    assert_eq!(
        json["excluded"],
        serde_json::json!([]),
        "a bare scan excludes nothing"
    );
    let findings = json["findings"].as_array().expect("findings is an array");
    assert_eq!(findings.len(), 1, "{:?}", out.stdout);
    assert_eq!(findings[0]["File"], "src/a.rs");
    assert_eq!(out.code, Some(1), "findings must fail the scan");
}

/// Exclusion has to drop files *before* the mirror. Moving or dropping the
/// partition call would scan excluded content anyway, which surfaces as a
/// confusing red rather than as a failing test.
#[test]
fn an_excluded_prefix_is_neither_scanned_nor_counted() {
    if !gitleaks_available() {
        return;
    }
    let dir = repo_with(&[
        ("docs/adr/leak.rs", &canary()),
        ("src/clean.rs", "let user = \"alice\";\n"),
    ]);

    let out = run_in(dir.path(), &["tree", "--exclude", "docs/adr"], "");
    let json = tree_json(&out);
    assert_eq!(
        json["excluded"],
        serde_json::json!(["docs/adr"]),
        "the scope must be recorded verbatim"
    );
    assert_eq!(
        json["findings"].as_array().expect("array").len(),
        0,
        "excluded content must not be scanned: {:?}",
        out.stdout
    );
    assert_eq!(out.code, Some(0));
    assert!(
        out.stderr.contains("EXCLUDED: docs/adr"),
        "exclusion must be loud: {}",
        out.stderr
    );

    // Without the flag the same repo is red -- so the assertions above cannot
    // be passing merely because the scan found nothing anywhere.
    let bare = run_in(dir.path(), &["tree"], "");
    assert_eq!(bare.code, Some(1), "stderr: {}", bare.stderr);
    assert_eq!(
        tree_json(&bare)["findings"]
            .as_array()
            .expect("array")
            .len(),
        1
    );
}

/// `staged` runs as a pre-commit hook, and a hook is exactly where git exports
/// `GIT_DIR` and `GIT_INDEX_FILE`. Scrub must scan the index those name -- that
/// is how `git commit --only` and linked-worktree commits get scanned at all --
/// so its production spawns deliberately inherit the environment. Every other
/// harness here strips `GIT_*`, which would let that inheritance rot unnoticed.
///
/// The observable is staged *content*, not the resolved repo root: with
/// `GIT_DIR` set and no `GIT_WORK_TREE`, git treats the cwd as the work tree,
/// so the root follows the cwd while the index comes from `GIT_DIR`.
#[test]
fn staged_mode_scans_the_index_named_by_the_hook_environment() {
    if !gitleaks_available() {
        return;
    }
    let fixture = repo_with(&[("src/a.rs", "let user = \"alice\";\n")]);
    write_file(fixture.path(), "src/planted.rs", &canary());
    git(fixture.path(), &["add", "-A"]);

    // Somewhere else entirely: not a git repo, carrying only the public rules
    // so config resolution has something to load once the root lands here.
    let elsewhere = tempfile::tempdir().expect("temp dir");
    std::fs::copy(
        repo_root().join(".gitleaks.toml"),
        elsewhere.path().join(".gitleaks.toml"),
    )
    .expect("copy public config");

    // Hermetic first, then one explicit `GIT_DIR`: the contract under test is
    // that the environment passed to scrub overrides its cwd, and stripping
    // ambient `GIT_*` first means only this test's variable can decide that.
    let mut cmd = Command::new(BIN);
    git_fixture::hermetic(&mut cmd);
    cmd.current_dir(elsewhere.path())
        .env_remove("BRENN_SCRUB_DENYLIST")
        .env("GIT_DIR", fixture.path().join(".git"))
        .arg("staged")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let out = cmd.output().expect("failed to spawn brenn-scrub");
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();

    assert_eq!(out.status.code(), Some(1), "stderr: {stderr}");
    assert!(
        stderr.contains("blocked this commit"),
        "the planted finding must block: {stderr}"
    );
    assert!(
        stderr.contains("src/planted.rs"),
        "the finding must be attributed to the staged path: {stderr}"
    );
}

/// A tracked path absent from the worktree used to be skipped silently, which
/// reads as a narrower scan reported as a full green. A staged deletion is the
/// one legitimate case and is announced.
#[test]
fn a_staged_deletion_is_skipped_out_loud_and_does_not_fail_the_scan() {
    if !gitleaks_available() {
        return;
    }
    let dir = repo_with(&[
        ("src/a.rs", "let user = \"alice\";\n"),
        ("src/gone.rs", "let peer = \"bob\";\n"),
    ]);
    std::fs::remove_file(dir.path().join("src/gone.rs")).expect("remove");

    let out = run_in(dir.path(), &["tree"], "");
    assert_eq!(out.code, Some(0), "stderr: {}", out.stderr);
    assert!(
        out.stderr.contains("SKIPPED: src/gone.rs"),
        "an unmirrored tracked path must be named: {}",
        out.stderr
    );
}

/// The commit id a fixture submodule is pinned at. Nothing dereferences it, so
/// no object has to exist: git records a gitlink's id without resolving it,
/// which is what lets these fixtures stay hermetic and network-free.
const SUBMODULE_PIN: &str = "0123456789abcdef0123456789abcdef01234567";

/// Add a tracked gitlink at `rel`, the way a `git submodule add` would leave
/// the index, without a submodule repository behind it.
fn add_gitlink(repo: &Path, rel: &str) {
    git(
        repo,
        &[
            "update-index",
            "--add",
            "--cacheinfo",
            &format!("160000,{SUBMODULE_PIN},{rel}"),
        ],
    );
    git(repo, &["commit", "-qm", "gitlink"]);
}

/// An overlay whose one rule matches the fixture's pointer *line*, so a scan
/// that reads the pointer as the text git records reports a finding, and one
/// that skips it -- or renders it differently from a diff -- does not.
fn overlay_matching_the_pin(dir: &Path) -> PathBuf {
    let path = dir.join("overlay.toml");
    std::fs::write(
        &path,
        format!(
            "[[rules]]\nid = \"fixture-submodule-pin\"\n\
             description = \"fixture rule matching a submodule pointer\"\n\
             regex = '''Subproject commit {SUBMODULE_PIN}'''\n"
        ),
    )
    .expect("write overlay");
    path
}

/// The worktree states a submodule can legitimately be in. Typed rather than
/// stringly, so a label and the fixture it selects cannot drift apart: a
/// misspelled arm would silently run one state twice while every failure
/// message still named the other.
#[derive(Debug, Clone, Copy)]
enum WorktreeState {
    Absent,
    Empty,
    Populated,
}

/// Put `path` into `state` and assert it really is in it -- the setup is what
/// the case is about, so an assertion on the fixture belongs with it.
fn set_up_worktree_state(path: &Path, state: WorktreeState) {
    match state {
        WorktreeState::Absent => {
            assert!(!path.exists(), "the absent case must have no path at all");
        }
        WorktreeState::Empty => {
            std::fs::create_dir_all(path).expect("mkdir");
            assert!(path.is_dir(), "the empty case must be a directory");
            assert_eq!(
                std::fs::read_dir(path).expect("read_dir").count(),
                0,
                "the empty case must be an empty directory"
            );
        }
        WorktreeState::Populated => {
            std::fs::create_dir_all(path).expect("mkdir");
            std::fs::write(path.join("upstream.rs"), "let user = \"bob\";\n").expect("write");
            // Untracked, inside the checkout, and matching a built-in rule: the
            // one thing that separates "the pointer was scanned" from "the
            // submodule's whole tree was swept under our rules". Descending is
            // rejected by design -- it would run the operator's private overlay
            // across third-party code -- and only an assertion keeps a later
            // widening of the gitlink arm from doing it unnoticed.
            std::fs::write(path.join("leak.rs"), canary()).expect("write");
            assert!(
                path.join("upstream.rs").exists() && path.join("leak.rs").exists(),
                "the populated case must have a checkout in it"
            );
        }
    }
}

/// All three worktree states a submodule can be in (absent, empty directory,
/// populated checkout) are legitimate, so none of them may decide anything —
/// only the index pointer matters.
#[test]
fn a_tracked_submodule_is_scanned_as_its_pointer_in_every_worktree_state() {
    if !gitleaks_available() {
        return;
    }
    for state in [
        WorktreeState::Absent,
        WorktreeState::Empty,
        WorktreeState::Populated,
    ] {
        let dir = repo_with(&[("src/a.rs", "let user = \"alice\";\n")]);
        add_gitlink(dir.path(), "vendor/thing");
        let path = dir.path().join("vendor/thing");
        set_up_worktree_state(&path, state);
        let state = format!("{state:?}");

        let out = run_in(dir.path(), &["tree"], "");
        assert_eq!(
            out.code,
            Some(0),
            "a clean tree with a submodule must pass ({state}); stderr: {}",
            out.stderr
        );
        assert_eq!(
            tree_json(&out)["findings"].as_array().expect("array").len(),
            0,
            "only the pointer is scanned; nothing inside the checkout is ({state}): {:?}",
            out.stdout
        );
        assert!(
            out.stderr.contains("SUBMODULE: vendor/thing"),
            "the submodule must be named on stderr ({state}): {}",
            out.stderr
        );
        assert!(
            out.stderr.contains("separate repository"),
            "the announcement must say the contents are out of scope ({state}): {}",
            out.stderr
        );
        assert!(
            !out.stderr.contains("SKIPPED: vendor/thing"),
            "a submodule pointer is scanned, not skipped ({state}): {}",
            out.stderr
        );
    }
}

/// The announcement alone would be satisfied by skipping the entry with a
/// friendlier message. A rule matching the pointer id has to fire, which is
/// only possible if the pointer text reached the scan.
#[test]
fn a_rule_matching_the_pointer_reports_a_finding_against_the_submodule_path() {
    if !gitleaks_available() {
        return;
    }
    let dir = repo_with(&[("src/a.rs", "let user = \"alice\";\n")]);
    add_gitlink(dir.path(), "vendor/thing");
    let overlay = overlay_matching_the_pin(dir.path());

    let out = run_in_env(
        dir.path(),
        &["tree"],
        "",
        &[("BRENN_SCRUB_DENYLIST", overlay.to_str().expect("utf-8"))],
    );
    assert_eq!(
        out.code,
        Some(1),
        "a match must fail the scan: {:?}",
        out.stdout
    );
    let json = tree_json(&out);
    let findings = json["findings"].as_array().expect("findings is an array");
    assert_eq!(findings.len(), 1, "{:?}", out.stdout);
    assert_eq!(
        findings[0]["File"], "vendor/thing",
        "the finding must name the submodule path"
    );
    assert_eq!(findings[0]["RuleID"], "fixture-submodule-pin");
}

/// A gitlink under an excluded prefix is dropped and counted like any other
/// entry -- not announced, not scanned, and still credited to the prefix so the
/// inert-exclusion check does not misfire.
#[test]
fn an_excluded_submodule_is_dropped_and_counted_like_a_file() {
    if !gitleaks_available() {
        return;
    }
    let dir = repo_with(&[("src/a.rs", "let user = \"alice\";\n")]);
    add_gitlink(dir.path(), "vendor/thing");
    let overlay = overlay_matching_the_pin(dir.path());
    let env = [("BRENN_SCRUB_DENYLIST", overlay.to_str().expect("utf-8"))];

    let out = run_in_env(dir.path(), &["tree", "--exclude", "vendor"], "", &env);
    assert_eq!(out.code, Some(0), "stderr: {}", out.stderr);
    assert_eq!(
        tree_json(&out)["findings"].as_array().expect("array").len(),
        0,
        "the excluded pointer must not be scanned: {:?}",
        out.stdout
    );
    assert!(
        out.stderr
            .contains("EXCLUDED: vendor (1 files not scanned)"),
        "the gitlink must be counted against the prefix: {}",
        out.stderr
    );
    assert!(
        !out.stderr.contains("SUBMODULE:"),
        "an excluded entry is not announced as scanned: {}",
        out.stderr
    );
}

/// The gitlink arm is an exemption from the stat loop's refusal, and the
/// refusal is what holds "no tracked entry unscanned under a green". Widening
/// the arm to tolerate directories -- the plausible next edit, since a
/// populated submodule is one -- would turn every tracked non-file into a
/// silent skip inside a run still printing `clean`.
#[test]
fn a_tracked_path_that_is_neither_file_nor_symlink_still_refuses() {
    if !gitleaks_available() {
        return;
    }
    let dir = repo_with(&[
        ("src/a.rs", "let user = \"alice\";\n"),
        ("weird", "tracked as a regular file\n"),
    ]);
    // git cannot add a fifo or a directory, so the route to a tracked non-file
    // is to replace one in the worktree after the fact.
    let weird = dir.path().join("weird");
    std::fs::remove_file(&weird).expect("remove");
    std::fs::create_dir(&weird).expect("mkdir");

    let out = run_in(dir.path(), &["tree"], "");
    assert_eq!(
        out.code,
        Some(101),
        "an unmirrorable tracked path must abort the scan: {:?} / {}",
        out.stdout,
        out.stderr
    );
    assert!(
        out.stderr.contains("neither a regular file nor a symlink"),
        "the refusal must name what it will not scan: {}",
        out.stderr
    );
}

/// A path in an unresolved merge is three index entries, one per side. Without
/// the stage-zero guard, all three arrive: the first mirrors the worktree file
/// as a hardlink, the second falls through to `fs::copy` onto that same inode
/// and truncates the operator's conflicted file to zero, and the scan reports
/// the empty result as clean. Both halves are asserted here -- the content
/// survives, and no green is printed over it.
#[test]
fn an_unresolved_merge_refuses_instead_of_scanning_or_truncating() {
    if !gitleaks_available() {
        return;
    }
    let dir = repo_with(&[("src/a.rs", "let user = \"alice\";\n")]);
    let p = dir.path();
    git(p, &["checkout", "-q", "-b", "other"]);
    write_file(p, "src/a.rs", &format!("{}// other\n", canary()));
    git(p, &["commit", "-qam", "other side"]);
    git(p, &["checkout", "-q", "main"]);
    write_file(p, "src/a.rs", "let user = \"carol\";\n");
    git(p, &["commit", "-qam", "main side"]);
    assert!(
        !try_git(p, &["merge", "other"]),
        "the fixture merge must conflict"
    );

    let conflicted = std::fs::read_to_string(p.join("src/a.rs")).expect("read");
    assert!(
        conflicted.contains("<<<<<<<") && conflicted.contains(&canary()),
        "the fixture must leave a real conflict in the worktree: {conflicted:?}"
    );

    let out = run_in(p, &["tree"], "");
    assert_eq!(
        out.code,
        Some(101),
        "a mid-conflict tree must not be scanned: {:?} / {}",
        out.stdout,
        out.stderr
    );
    assert!(
        out.stderr.contains("unmerged entries"),
        "the refusal must name the unresolved merge: {}",
        out.stderr
    );
    assert_eq!(
        std::fs::read_to_string(p.join("src/a.rs")).expect("read"),
        conflicted,
        "the scan must not touch the conflicted worktree file"
    );
}
