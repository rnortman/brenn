//! Shared harness for the integration tests that drive the real binary.
//!
//! Each test binary compiles its own copy of this module and uses only the
//! helpers it needs, so items unused by one binary are not dead overall.
#![allow(dead_code)]

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

const BIN: &str = env!("CARGO_BIN_EXE_brenn-scrub");

/// Mirrors `gitleaks::PINNED_VERSION`; the crate is a binary, so the real
/// constant is not importable from an integration test.
pub const PINNED_VERSION: &str = "8.30.0";

/// Names the pinned gitleaks binary when the harness is handed one as a
/// declared input instead of looking it up on `PATH`.
const VENDORED_GITLEAKS: &str = "BRENN_SCRUB_TEST_GITLEAKS";

/// A path as given, or resolved against the process working directory.
///
/// Several cases give the child a working directory of its own, so a relative
/// program path would be resolved against *that* directory rather than the one
/// the harness was handed it in.
fn absolute(named: impl Into<PathBuf>) -> PathBuf {
    let named = named.into();
    if named.is_absolute() {
        named
    } else {
        std::env::current_dir()
            .expect("working directory")
            .join(named)
    }
}

/// The binary under test.
pub fn scrub_bin() -> PathBuf {
    absolute(BIN)
}

/// The vended gitleaks, as an absolute path.
fn vendored_gitleaks() -> Option<PathBuf> {
    let path = absolute(PathBuf::from(std::env::var_os(VENDORED_GITLEAKS)?));
    assert!(
        path.is_file(),
        "{VENDORED_GITLEAKS} names {}, which is not a file",
        path.display()
    );
    Some(path)
}

pub struct Output {
    pub code: Option<i32>,
    pub stderr: String,
}

/// One binary run. The scrub overlay env var is removed first so no test ever
/// inherits the operator's live overlay (the harness inherits the parent
/// environment, and the operator's shell exports it); the caller adds back
/// exactly what the case needs via `extra_env`. `path_prefix` prepends a
/// directory (e.g. a gitleaks stub) to `PATH`; `cwd` sets the working
/// directory, which only the non-hook modes read.
pub fn run(
    args: &[&str],
    stdin: &str,
    extra_env: &[(&str, &str)],
    path_prefix: Option<&Path>,
    cwd: Option<&Path>,
) -> Output {
    let mut cmd = Command::new(scrub_bin());
    cmd.args(args);
    cmd.env_remove("BRENN_SCRUB_DENYLIST");
    // These cases are "scrub, run against this fixture repo" — production
    // env-inheritance is not what they test. Without the strip, the spawned
    // binary (and the `gitleaks` it spawns, which itself runs git) resolves
    // whatever repo a hook environment names instead of the fixture.
    git_fixture::hermetic(&mut cmd);
    for (k, v) in extra_env {
        cmd.env(k, v);
    }
    prepend_path(&mut cmd, path_prefix);
    if let Some(dir) = cwd {
        cmd.current_dir(dir);
    }
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn brenn-scrub");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(stdin.as_bytes())
        .expect("write stdin");
    let out = child.wait_with_output().expect("wait");
    Output {
        code: out.status.code(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    }
}

/// Give a child the gitleaks these tests are pinned to, plus `prefix` ahead of
/// it when a case installs a stub that has to win the lookup.
///
/// The binary under test resolves `gitleaks` on `PATH` — that is the production
/// behavior these cases exercise — so vending it means putting its directory on
/// the child's `PATH`, not passing a path down.
pub fn prepend_path(cmd: &mut Command, prefix: Option<&Path>) {
    let mut entries: Vec<PathBuf> = Vec::new();
    if let Some(dir) = prefix {
        entries.push(dir.to_path_buf());
    }
    if let Some(bin) = vendored_gitleaks() {
        entries.push(
            bin.parent()
                .expect("gitleaks binary has a parent directory")
                .to_path_buf(),
        );
    }
    if entries.is_empty() {
        return;
    }
    let existing = std::env::var("PATH").unwrap_or_default();
    let joined: Vec<String> = entries.iter().map(|p| p.display().to_string()).collect();
    cmd.env("PATH", format!("{}:{existing}", joined.join(":")));
}

/// A run of the gitleaks these tests are pinned to, for the cases that scan
/// directly rather than through the binary under test.
pub fn gitleaks_command() -> Command {
    match vendored_gitleaks() {
        Some(bin) => Command::new(bin),
        None => Command::new("gitleaks"),
    }
}

/// Whether the pinned gitleaks is available, printing a skip reason when not so
/// a scan-reaching test can `return` early on a machine without it.
///
/// The skip exists for a machine that never installed gitleaks. When one is
/// vended, there is no such machine: an absent or off-pin binary is a broken
/// harness and fails rather than quietly reducing the suite to nothing.
pub fn gitleaks_available() -> bool {
    let vendored = vendored_gitleaks();
    let mut cmd = gitleaks_command();
    let out = cmd.arg("version").output();
    let Ok(out) = out else {
        assert!(
            vendored.is_none(),
            "the vended gitleaks at {} could not be run",
            vendored.expect("checked").display()
        );
        eprintln!("skipping: gitleaks not on PATH");
        return false;
    };
    assert!(
        out.status.success() || vendored.is_none(),
        "the vended gitleaks exited {} on `version`",
        out.status
    );
    if !out.status.success() {
        eprintln!("skipping: gitleaks not on PATH");
        return false;
    }
    let found = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if found == PINNED_VERSION {
        return true;
    }
    assert!(
        vendored.is_none(),
        "the vended gitleaks reports {found}, not the pinned {PINNED_VERSION}"
    );
    eprintln!("skipping: gitleaks {found} is not the pinned {PINNED_VERSION}");
    false
}

/// A git repo carrying the given `.gitleaks.toml` at its root -- gated, so a
/// destination inside it is scanned.
pub fn gated_repo(config: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("temp dir");
    git_fixture::init_repo(dir.path());
    std::fs::write(dir.path().join(".gitleaks.toml"), config).expect("write config");
    dir
}

/// A `gitleaks` on PATH reporting the given version and finding nothing, so a
/// write into a gated repo reaches the scan instead of stopping at the version
/// probe.
pub fn stub_gitleaks(version: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("gitleaks");
    std::fs::write(
        &path,
        format!("#!/bin/sh\nif [ \"$1\" = version ]; then echo {version}; fi\nexit 0\n"),
    )
    .expect("write stub");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).expect("chmod");
    }
    dir
}
