//! Shared harness for the integration tests that drive the real binary.

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

/// One binary run. Both scrub file-discovery env vars are removed first so no
/// test ever inherits the operator's live overlay or exemption file (the
/// harness inherits the parent environment, and the operator's shell exports
/// both); the caller adds back exactly what the case needs via `extra_env`.
/// `path_prefix` prepends a directory (e.g. a gitleaks stub) to `PATH`; `cwd`
/// sets the working directory, which only the non-hook modes read.
pub fn run(
    args: &[&str],
    stdin: &str,
    extra_env: &[(&str, &str)],
    path_prefix: Option<&Path>,
    cwd: Option<&Path>,
) -> Output {
    let mut cmd = Command::new(BIN);
    cmd.args(args);
    cmd.env_remove("BRENN_SCRUB_WRITE_EXEMPT");
    cmd.env_remove("BRENN_SCRUB_DENYLIST");
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

/// A `gitleaks` on PATH reporting the given version and finding nothing, so a
/// non-exempt write reaches repo/config resolution instead of stopping at the
/// version probe.
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
