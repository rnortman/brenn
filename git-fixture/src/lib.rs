//! Hermetic git spawning for test fixtures.
//!
//! `Command::new("git").current_dir(repo)` does not isolate anything: git's
//! environment variables (`GIT_DIR`, `GIT_INDEX_FILE`, `GIT_WORK_TREE`, the
//! `GIT_CONFIG_*` family, ...) silently override `current_dir`. A `git commit`
//! run from a linked worktree exports `GIT_DIR` and `GIT_INDEX_FILE` into its
//! pre-commit hook, so a test suite invoked from such a hook inherits them and
//! every fixture mutation lands in the real repository instead of the
//! fixture's tempdir.
//!
//! Every fixture git spawn goes through this crate, so isolation is a property
//! of construction rather than of whatever the ambient environment happened to
//! be. Dev-only: consumed exclusively through `[dev-dependencies]`.
//!
//! Two rules, and both are needed:
//!
//! 1. Fixture mutations are hermetic ([`hermetic`], [`git`]) — no inherited
//!    `GIT_*` reaches them, and the config sources are pinned.
//! 2. Every rooting of a fixture repo ends in a *non*-hermetic canary
//!    ([`assert_repo_is`]), which spawns git the way production code does and
//!    so also catches an environment that redirects the code under test.
//!    Hermetic fixtures alone would mask that: the fixture would build
//!    correctly while the production calls read some other repo.

use std::ffi::OsStr;
use std::ffi::OsString;
use std::path::Path;
use std::process::Command;

/// Fixture identity, applied to every hermetic spawn.
///
/// These strings — plus fixture commit messages such as `base`, `more`, and
/// `collide` — are the forensic signature of this crate. Finding them in a
/// *real* repository's `.git/config`, reflog, or commit history means fixtures
/// escaped their tempdir: some git spawn reached a repo it does not own. Grep
/// for them first, then look for a git spawn that did not come from here.
const IDENTITY: [(&str, &str); 4] = [
    ("GIT_AUTHOR_NAME", "alice"),
    ("GIT_AUTHOR_EMAIL", "a@example.com"),
    ("GIT_COMMITTER_NAME", "alice"),
    ("GIT_COMMITTER_EMAIL", "a@example.com"),
];

fn is_git_var(name: &OsStr) -> bool {
    name.as_encoded_bytes().starts_with(b"GIT_")
}

/// Strip every `GIT_*` variable from `cmd`'s environment and pin the rest.
///
/// Removal covers the union of the parent process environment and the keys
/// already set on `cmd` itself: `env_remove` only overrides an earlier `env`
/// call for the key it names, so a `GIT_*` set explicitly on the Command but
/// absent from the parent environment would otherwise survive. Removal is by
/// prefix rather than by an enumerated list, which also covers
/// `GIT_CONFIG_COUNT`/`GIT_CONFIG_KEY_n`/`GIT_CONFIG_VALUE_n`,
/// `GIT_CONFIG_PARAMETERS`, and any variable git grows later.
///
/// Applied last, this wins over earlier `.env()` calls on the same Command.
/// Nothing here touches the process environment, so it is safe under parallel
/// test threads.
pub fn hermetic(cmd: &mut Command) {
    hermetic_with_parent(cmd, std::env::vars_os().map(|(name, _)| name));
}

/// [`hermetic`] with the parent environment's variable names injected, so the
/// parent-env half of the removal is testable without touching the process
/// environment (`set_var` is unsafe under edition 2024 and racy under parallel
/// test threads).
fn hermetic_with_parent(cmd: &mut Command, parent: impl Iterator<Item = OsString>) {
    let mut names: Vec<OsString> = parent.filter(|name| is_git_var(name)).collect();
    names.extend(
        cmd.get_envs()
            .map(|(name, _)| name.to_os_string())
            .filter(|name| is_git_var(name)),
    );
    for name in names {
        cmd.env_remove(name);
    }

    // Fixtures must not inherit the operator's `init.defaultBranch`,
    // `commit.gpgsign` (which can hang on a pinentry prompt), `core.hooksPath`,
    // or anything else from global/system config.
    cmd.env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        // If a fixture's init failed or a path is wrong, upward repo discovery
        // stops at the temp root and git says "not a git repository" instead of
        // walking toward a real repo.
        .env("GIT_CEILING_DIRECTORIES", ceiling_dirs());
    for (name, value) in IDENTITY {
        cmd.env(name, value);
    }
}

/// The `GIT_CEILING_DIRECTORIES` value: the temp root, preceded by an empty
/// entry.
///
/// git compares ceiling entries against a symlink-resolved discovery path but
/// does not resolve symlinks *in the entries* — unless an entry follows an
/// empty one. Where `TMPDIR` runs through a symlink the bare entry would never
/// match and the ceiling would be inert.
fn ceiling_dirs() -> OsString {
    let mut value = OsString::from(":");
    value.push(std::env::temp_dir());
    value
}

/// Run `git <args>` hermetically in `dir`, returning stdout.
///
/// Panics with git's stderr on failure: a fixture step that did not do what it
/// said is a broken test, not a condition to recover from.
pub fn git(dir: &Path, args: &[&str]) -> String {
    let mut cmd = Command::new("git");
    cmd.current_dir(dir).args(args);
    hermetic(&mut cmd);
    let out = cmd
        .output()
        .unwrap_or_else(|e| panic!("git-fixture: failed to spawn git {args:?}: {e}"));
    assert!(
        out.status.success(),
        "git-fixture: `git {}` in {} failed: {}",
        args.join(" "),
        dir.display(),
        String::from_utf8_lossy(&out.stderr).trim()
    );
    String::from_utf8(out.stdout).unwrap_or_else(|_| {
        panic!(
            "git-fixture: `git {}` emitted non-UTF-8 output",
            args.join(" ")
        )
    })
}

/// Initialize a fixture repo in `dir` (which must already exist), on branch
/// `main`, with a local identity, and assert it is isolated.
///
/// `-b main` is explicit: with global config pinned to `/dev/null` the default
/// branch name would otherwise be git's own default, which moves between
/// versions. The local `user.name`/`user.email` exist for fixtures whose
/// *production code under test* commits without identity environment.
pub fn init_repo(dir: &Path) {
    git(dir, &["init", "-q", "-b", "main", "."]);
    set_identity(dir);
    assert_repo_is(dir);
}

/// Initialize a bare fixture repo in `dir` (which must already exist), on
/// branch `main`, and assert it is isolated. The push target of fixtures that
/// exercise remote choreography.
pub fn init_bare_repo(dir: &Path) {
    git(dir, &["init", "-q", "--bare", "-b", "main"]);
    assert_repo_is(dir);
}

/// Write the fixture identity into `dir`'s local config.
///
/// The identity environment of [`hermetic`] covers only the spawns this crate
/// makes; a repo whose *production code under test* runs `git commit` inherits
/// no such environment and needs the config.
fn set_identity(dir: &Path) {
    git(dir, &["config", "user.name", "alice"]);
    git(dir, &["config", "user.email", "a@example.com"]);
}

/// Hermetic `git clone src dest`, then assert `dest` is isolated.
///
/// The clone-rooted twin of [`init_repo`]: every way a fixture repo comes into
/// existence carries the fixture identity and ends in a canary.
pub fn clone_repo(src: &Path, dest: &Path) {
    let src = src
        .to_str()
        .unwrap_or_else(|| panic!("git-fixture: clone source path is not UTF-8: {src:?}"));
    let dest_path = dest;
    let dest = dest
        .to_str()
        .unwrap_or_else(|| panic!("git-fixture: clone destination path is not UTF-8: {dest:?}"));
    // Both paths are absolute, so the cwd only has to be a directory that
    // exists and is not itself a fixture.
    git(&std::env::temp_dir(), &["clone", "-q", src, dest]);
    set_identity(dest_path);
    assert_repo_is(dest_path);
}

/// Root a fixture repo in `dir` with a local identity and one commit, so
/// `HEAD` exists.
pub fn seed_repo(dir: &Path) {
    init_repo(dir);
    std::fs::write(dir.join("file.txt"), "initial")
        .unwrap_or_else(|e| panic!("git-fixture: cannot write the seed file in {dir:?}: {e}"));
    git(dir, &["add", "."]);
    git(dir, &["commit", "-qm", "initial"]);
}

/// Give the repo at `dir` a bare `origin` in a fresh tempdir, push `main` to
/// it, and set up tracking. The returned tempdir owns the remote: drop it and
/// the remote goes away, so callers bind it for as long as the fixture lives.
pub fn add_bare_origin(dir: &Path) -> tempfile::TempDir {
    let remote = tempfile::tempdir().expect("git-fixture: cannot create a tempdir for the remote");
    init_bare_repo(remote.path());
    let url = remote
        .path()
        .to_str()
        .unwrap_or_else(|| panic!("git-fixture: remote path is not UTF-8: {:?}", remote.path()));
    git(dir, &["remote", "add", "origin", url]);
    git(dir, &["push", "-q", "-u", "origin", "main"]);
    remote
}

/// Assert that git, spawned the way production code spawns it, resolves to the
/// repo at `dir`.
///
/// Deliberately *non*-hermetic — inherited environment, `current_dir` only. It
/// therefore catches two distinct failures with one assertion: a fixture whose
/// mutations would have landed elsewhere, and an environment that would
/// redirect the production calls under test.
pub fn assert_repo_is(dir: &Path) {
    assert_repo_is_with_env(dir, &[]);
}

/// The canary, with an environment overlay applied to its spawns.
///
/// The overlay is the test seam for redirection: an ambient `GIT_DIR` cannot be
/// simulated by setting the process environment (unsafe under edition 2024,
/// racy under parallel test threads), so tests hand the same variable to the
/// Command instead. Production callers pass nothing and get the inherited
/// environment they need.
fn assert_repo_is_with_env(dir: &Path, env: &[(&str, &OsStr)]) {
    let expected = std::fs::canonicalize(dir)
        .unwrap_or_else(|e| panic!("git-fixture: cannot canonicalize fixture dir {dir:?}: {e}"));

    let git_dir = canary_query(dir, &["rev-parse", "--absolute-git-dir"], env);
    let git_dir = canonicalize_leaf(Path::new(&git_dir));
    assert!(
        git_dir.starts_with(&expected),
        "git-fixture canary: fixture escape / environment redirection — git in {} \
         resolves its git dir to {}, which is outside the fixture. Something in \
         the environment (GIT_DIR, GIT_WORK_TREE, a gitfile, ...) is redirecting \
         git away from the directory this test owns; mutations would land in \
         someone else's repository.",
        expected.display(),
        git_dir.display()
    );

    // `GIT_INDEX_FILE` takes no part in git-dir resolution, so an absolute one
    // with no `GIT_DIR` passes the check above while still steering every index
    // operation — `git add` from production code under test would rewrite
    // another repository's index. `--git-path index` is the query that reflects
    // it; its output is relative to the cwd when the variable is unset.
    let index = canary_query(dir, &["rev-parse", "--git-path", "index"], env);
    let index = canonicalize_leaf(&dir.join(index));
    assert!(
        index.starts_with(&expected),
        "git-fixture canary: fixture escape / environment redirection — git in {} \
         names {} as its index, which is outside the fixture. GIT_INDEX_FILE in \
         the environment is redirecting index operations; staging from code \
         under test would rewrite someone else's index.",
        expected.display(),
        index.display()
    );
}

/// One canary spawn: production-shaped (inherited environment, `current_dir`
/// only) plus the overlay, with the trimmed stdout returned.
fn canary_query(dir: &Path, args: &[&str], env: &[(&str, &OsStr)]) -> String {
    let mut cmd = Command::new("git");
    cmd.current_dir(dir).args(args);
    for (name, value) in env {
        cmd.env(name, value);
    }
    let out = cmd
        .output()
        .unwrap_or_else(|e| panic!("git-fixture: failed to spawn git: {e}"));
    assert!(
        out.status.success(),
        "git-fixture canary: no git repo resolves at {} \
         (fixture escape / environment redirection): {}",
        dir.display(),
        String::from_utf8_lossy(&out.stderr).trim()
    );
    let stdout = String::from_utf8(out.stdout)
        .unwrap_or_else(|_| panic!("git-fixture: git rev-parse emitted non-UTF-8 output"));
    stdout.trim_end_matches('\n').to_string()
}

/// Canonicalize `path` through its parent, so a name that does not exist yet —
/// `index` in a repo nothing has staged into — still resolves its directory.
fn canonicalize_leaf(path: &Path) -> std::path::PathBuf {
    if let Ok(resolved) = std::fs::canonicalize(path) {
        return resolved;
    }
    let parent = path
        .parent()
        .unwrap_or_else(|| panic!("git-fixture: {path:?} has no parent to resolve"));
    let name = path
        .file_name()
        .unwrap_or_else(|| panic!("git-fixture: {path:?} has no file name"));
    let parent = std::fs::canonicalize(parent)
        .unwrap_or_else(|e| panic!("git-fixture: cannot canonicalize {parent:?}: {e}"));
    parent.join(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Names `hermetic` sets rather than removes.
    const PINNED: [&str; 3] = [
        "GIT_CONFIG_GLOBAL",
        "GIT_CONFIG_SYSTEM",
        "GIT_CEILING_DIRECTORIES",
    ];

    fn env_map(cmd: &Command) -> HashMap<OsString, Option<OsString>> {
        cmd.get_envs()
            .map(|(k, v)| (k.to_os_string(), v.map(OsStr::to_os_string)))
            .collect()
    }

    /// A parent environment shaped like the one a linked-worktree pre-commit
    /// hook exports, plus a control that must survive.
    fn synthetic_parent() -> Vec<OsString> {
        [
            "GIT_DIR",
            "GIT_INDEX_FILE",
            "GIT_WORK_TREE",
            "GIT_CONFIG_COUNT",
            "GIT_PREFIX",
            "PATH",
        ]
        .iter()
        .map(OsString::from)
        .collect()
    }

    /// T1: every `GIT_*` from the union of the parent env and the Command's own
    /// map is removed or pinned; nothing is left to inherit.
    #[test]
    fn hermetic_removes_the_union_and_pins_config_and_identity() {
        let mut cmd = Command::new("git");
        // Set on the Command only, not present in the parent environment: the
        // parent-env pass alone would miss it.
        cmd.env("GIT_DIR", "/decoy/.git");
        let parent = synthetic_parent();
        hermetic_with_parent(&mut cmd, parent.iter().cloned());
        let map = env_map(&cmd);

        assert_eq!(
            map.get(OsStr::new("GIT_DIR")),
            Some(&None),
            "an explicitly-set GIT_DIR must be removed, not merely shadowed"
        );

        let pinned: Vec<&OsStr> = PINNED
            .iter()
            .map(|n| OsStr::new(*n))
            .chain(IDENTITY.iter().map(|(n, _)| OsStr::new(*n)))
            .collect();
        for name in parent.iter().filter(|name| is_git_var(name)) {
            if pinned.contains(&name.as_os_str()) {
                continue;
            }
            assert_eq!(
                map.get(name),
                Some(&None),
                "inherited {name:?} must be removed"
            );
        }
        assert!(
            !map.contains_key(OsStr::new("PATH")),
            "a non-GIT_ parent variable must be left alone"
        );

        for name in PINNED {
            let value = map
                .get(OsStr::new(name))
                .unwrap_or_else(|| panic!("{name} not set"));
            assert!(value.is_some(), "{name} must be pinned to a value");
        }
        assert_eq!(
            map.get(OsStr::new("GIT_CONFIG_GLOBAL")),
            Some(&Some(OsString::from("/dev/null")))
        );
        let ceiling = map
            .get(OsStr::new("GIT_CEILING_DIRECTORIES"))
            .and_then(Option::as_ref)
            .expect("GIT_CEILING_DIRECTORIES must be pinned");
        assert_eq!(ceiling, &ceiling_dirs());
        assert!(
            ceiling.as_encoded_bytes().starts_with(b":"),
            "the empty leading entry is what makes git canonicalize the ceiling: {ceiling:?}"
        );
        for (name, want) in IDENTITY {
            assert_eq!(
                map.get(OsStr::new(name)),
                Some(&Some(OsString::from(want))),
                "{name} must carry the fixture identity"
            );
        }
    }

    /// T2: the incident mechanism. A `GIT_DIR` pointing at another repo — the
    /// shape a linked-worktree pre-commit hook exports — must not steer a
    /// hermetic fixture commit out of its tempdir.
    #[test]
    fn hermetic_commit_ignores_an_explicit_git_dir() {
        let decoy = tempfile::tempdir().unwrap();
        init_repo(decoy.path());
        std::fs::write(decoy.path().join("d.txt"), "decoy\n").unwrap();
        git(decoy.path(), &["add", "d.txt"]);
        git(decoy.path(), &["commit", "-qm", "decoy-base"]);
        let decoy_head = git(decoy.path(), &["rev-parse", "HEAD"]);

        let fixture = tempfile::tempdir().unwrap();
        init_repo(fixture.path());
        std::fs::write(fixture.path().join("f.txt"), "fixture\n").unwrap();
        git(fixture.path(), &["add", "f.txt"]);

        let mut cmd = Command::new("git");
        cmd.current_dir(fixture.path())
            .args(["commit", "-qm", "landed-here"])
            .env("GIT_DIR", decoy.path().join(".git"));
        hermetic(&mut cmd);
        let out = cmd.output().unwrap();
        assert!(
            out.status.success(),
            "commit failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );

        assert_eq!(
            git(fixture.path(), &["log", "-1", "--format=%s"]).trim(),
            "landed-here"
        );
        assert_eq!(
            git(decoy.path(), &["rev-parse", "HEAD"]),
            decoy_head,
            "the decoy repo named by GIT_DIR must be untouched"
        );
        assert_eq!(
            git(decoy.path(), &["log", "-1", "--format=%s"]).trim(),
            "decoy-base"
        );
    }

    /// T3 (positive): a freshly rooted fixture satisfies its own canary.
    #[test]
    fn canary_passes_for_a_fixture_repo() {
        let dir = tempfile::tempdir().unwrap();
        init_repo(dir.path());
        assert_repo_is(dir.path());
    }

    /// T3 (positive, clone-rooted): clones get the same canary.
    #[test]
    fn clone_repo_roots_a_working_copy_and_passes_the_canary() {
        let src = tempfile::tempdir().unwrap();
        init_repo(src.path());
        std::fs::write(src.path().join("f.txt"), "x\n").unwrap();
        git(src.path(), &["add", "f.txt"]);
        git(src.path(), &["commit", "-qm", "base"]);

        let parent = tempfile::tempdir().unwrap();
        let dest = parent.path().join("clone");
        clone_repo(src.path(), &dest);
        assert_eq!(git(&dest, &["log", "-1", "--format=%s"]).trim(), "base");
    }

    /// T3 (negative): a directory whose `.git` gitfile points at a repo
    /// elsewhere is exactly the escape shape the canary exists to catch.
    #[test]
    #[should_panic(expected = "fixture escape / environment redirection")]
    fn canary_panics_when_git_resolves_outside_the_fixture() {
        let decoy = tempfile::tempdir().unwrap();
        init_repo(decoy.path());

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(".git"),
            format!("gitdir: {}\n", decoy.path().join(".git").display()),
        )
        .unwrap();
        assert_repo_is(dir.path());
    }

    /// T3 (negative, redirection): the class the canary is non-hermetic for —
    /// a `GIT_DIR` in the environment steering git away from a perfectly good
    /// fixture. Making the canary hermetic would silence exactly this.
    #[test]
    #[should_panic(expected = "resolves its git dir to")]
    fn canary_panics_when_the_environment_redirects_the_git_dir() {
        let decoy = tempfile::tempdir().unwrap();
        init_repo(decoy.path());

        let dir = tempfile::tempdir().unwrap();
        init_repo(dir.path());
        let decoy_git_dir = decoy.path().join(".git");
        assert_repo_is_with_env(dir.path(), &[("GIT_DIR", decoy_git_dir.as_os_str())]);
    }

    /// T3 (negative, index): `GIT_INDEX_FILE` alone leaves git-dir resolution
    /// untouched, so only the index half of the canary sees it.
    #[test]
    #[should_panic(expected = "as its index")]
    fn canary_panics_when_the_environment_redirects_the_index() {
        let decoy = tempfile::tempdir().unwrap();
        init_repo(decoy.path());

        let dir = tempfile::tempdir().unwrap();
        init_repo(dir.path());
        let decoy_index = decoy.path().join(".git").join("index");
        assert_repo_is_with_env(dir.path(), &[("GIT_INDEX_FILE", decoy_index.as_os_str())]);
    }

    #[test]
    fn init_bare_repo_roots_a_bare_repo_on_main() {
        let dir = tempfile::tempdir().unwrap();
        init_bare_repo(dir.path());
        assert_eq!(
            git(dir.path(), &["rev-parse", "--is-bare-repository"]).trim(),
            "true"
        );
        assert_eq!(
            git(dir.path(), &["symbolic-ref", "HEAD"]).trim(),
            "refs/heads/main"
        );
    }

    #[test]
    fn seed_repo_leaves_a_resolvable_head_and_a_local_identity() {
        let dir = tempfile::tempdir().unwrap();
        seed_repo(dir.path());
        assert!(!git(dir.path(), &["rev-parse", "HEAD"]).trim().is_empty());
        assert_eq!(
            git(dir.path(), &["config", "user.email"]).trim(),
            "a@example.com"
        );
    }

    #[test]
    fn add_bare_origin_pushes_main_and_sets_tracking() {
        let dir = tempfile::tempdir().unwrap();
        seed_repo(dir.path());
        let remote = add_bare_origin(dir.path());
        assert_eq!(
            git(dir.path(), &["config", "branch.main.remote"]).trim(),
            "origin"
        );
        assert_eq!(
            git(remote.path(), &["rev-parse", "main"]).trim(),
            git(dir.path(), &["rev-parse", "HEAD"]).trim()
        );
    }
}
