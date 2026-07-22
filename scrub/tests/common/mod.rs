//! Shared harness for the integration tests that drive the real binary.
//!
//! Each test binary compiles its own copy of this module and uses only the
//! helpers it needs, so items unused by one binary are not dead overall.
#![allow(dead_code)]

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

const BIN: &str = env!("CARGO_BIN_EXE_brenn-scrub");

/// Mirrors `gitleaks::PINNED_VERSION`; the crate is a binary, so the real
/// constant is not importable from an integration test.
pub const PINNED_VERSION: &str = "8.30.0";

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
    let mut cmd = Command::new(BIN);
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
    if let Some(dir) = path_prefix {
        let existing = std::env::var("PATH").unwrap_or_default();
        cmd.env("PATH", format!("{}:{existing}", dir.display()));
    }
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

/// Whether the pinned gitleaks is on PATH, printing a skip reason when not so a
/// scan-reaching test can `return` early on a machine without it.
pub fn gitleaks_available() -> bool {
    match Command::new("gitleaks").arg("version").output() {
        Ok(out) if out.status.success() => {
            let found = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if found == PINNED_VERSION {
                true
            } else {
                eprintln!("skipping: gitleaks {found} is not the pinned {PINNED_VERSION}");
                false
            }
        }
        _ => {
            eprintln!("skipping: gitleaks not on PATH");
            false
        }
    }
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
