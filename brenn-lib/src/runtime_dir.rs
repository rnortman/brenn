use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;

fn validate_dir_owner(dir_uid: u32, euid: u32, path: &std::path::Path) {
    if dir_uid != euid {
        panic!(
            "XDG_RUNTIME_DIR validation failed: directory is not owned by the current euid \
             (path={}, observed_uid={dir_uid}, expected_euid={euid})",
            path.display(),
        );
    }
}

/// Validate a candidate `XDG_RUNTIME_DIR` path and return it if valid.
///
/// This is a pure function over its argument — it does not read the process
/// environment. Panics — with a message starting `"XDG_RUNTIME_DIR validation
/// failed:"` — if any of the following hold:
///
/// - The path is not absolute.
/// - The path does not exist (includes dangling symlinks, since `metadata`
///   follows symlinks).
/// - The path does not resolve to a directory.
/// - The resolved directory is not owned by the current effective uid.
/// - The resolved directory's mode (low 9 bits) is not exactly `0o700`.
///
/// Symlinks at the root are tolerated: `std::fs::metadata` follows them, so
/// what is checked is the ownership and mode of the final target. High bits
/// (setuid/setgid/sticky) are not checked. Canonicalisation is deliberately
/// skipped.
pub fn validate_runtime_dir(path: PathBuf) -> PathBuf {
    if !path.is_absolute() {
        panic!(
            "XDG_RUNTIME_DIR validation failed: path must be absolute \
             (path={}, got a relative path)",
            path.display(),
        );
    }

    let meta = match std::fs::metadata(&path) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            panic!(
                "XDG_RUNTIME_DIR validation failed: path does not exist (path={})",
                path.display(),
            );
        }
        Err(e) => {
            panic!(
                "XDG_RUNTIME_DIR validation failed: could not stat path (path={}, error={e})",
                path.display(),
            );
        }
    };

    if !meta.is_dir() {
        panic!(
            "XDG_RUNTIME_DIR validation failed: path is not a directory (path={})",
            path.display(),
        );
    }

    // SAFETY: geteuid() has no failure mode and no preconditions.
    let euid = unsafe { libc::geteuid() };
    let dir_uid = meta.uid();
    validate_dir_owner(dir_uid, euid, &path);

    let mode = meta.mode() & 0o777;
    if mode != 0o700 {
        panic!(
            "XDG_RUNTIME_DIR validation failed: directory mode must be 0700 \
             (path={}, observed_mode={mode:04o}, expected_mode=0700)",
            path.display(),
        );
    }

    path
}

/// Pure decision logic over the raw `XDG_RUNTIME_DIR` environment value.
///
/// `None` or `Some("")` → panic with the "unset or empty" message.
/// Otherwise → delegates to `validate_runtime_dir`.
///
/// This function is pure w.r.t. the process environment; tests can call it
/// directly with any `Option<&str>` without mutating `XDG_RUNTIME_DIR`.
pub(crate) fn resolve_xdg_value(v: Option<&str>) -> PathBuf {
    match v {
        Some(s) if !s.is_empty() => validate_runtime_dir(PathBuf::from(s)),
        _ => {
            panic!(
                "XDG_RUNTIME_DIR validation failed: unset or empty; brenn requires it to be set \
                 to a directory owned by the current uid with mode 0700 (systemd user sessions \
                 set this automatically; see your launcher)"
            );
        }
    }
}

/// Read `XDG_RUNTIME_DIR` from the process environment, validate it, and
/// return the validated `PathBuf`.
///
/// This is the sole impure function in this module — the only line that reads
/// the process environment. All validation and decision logic lives in the pure
/// helpers above; this wrapper has no logic of its own.
///
/// Panics — with a message starting `"XDG_RUNTIME_DIR validation failed:"` —
/// if the env var is unset, empty, non-UTF-8, or fails path validation. Both
/// `VarError::NotPresent` and `VarError::NotUnicode` collapse to the same
/// "unset or empty" panic via `resolve_xdg_value(None)` — a non-UTF-8
/// `XDG_RUNTIME_DIR` is itself a misconfiguration.
pub fn resolve_validated_xdg_runtime_dir() -> PathBuf {
    let v = std::env::var("XDG_RUNTIME_DIR").ok();
    resolve_xdg_value(v.as_deref())
}

/// Create a validated `XDG_RUNTIME_DIR`-like `PathBuf` exactly once per process
/// (via `OnceLock`) and return it, suitable as the `runtime_dir` argument to
/// `validate_and_resolve` in tests that include at least one bare app.
///
/// The tempdir is created with mode 0700, validated through
/// `validate_runtime_dir`, and leaked so the path remains alive for the full
/// binary lifetime. No environment variable is read or written.
///
/// Exposed when building with `--features testutils` (for downstream crates)
/// or under `#[cfg(test)]` (for intra-crate tests).
#[cfg(any(test, feature = "testutils"))]
pub fn test_runtime_dir_once() -> &'static PathBuf {
    use std::os::unix::fs::PermissionsExt;
    static TEST_XDG_DIR: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    TEST_XDG_DIR.get_or_init(|| {
        let tmp = tempfile::tempdir().expect("create test XDG tempdir");
        std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o700))
            .expect("chmod test XDG tempdir");
        let path = tmp.path().to_path_buf();
        // Leak the TempDir so the path outlives this call; the OS cleans it up
        // when the process exits.
        std::mem::forget(tmp);
        // Validate via the same pure validator production uses.
        validate_runtime_dir(path)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    use crate::test_utils::unwrap_panic_msg;

    /// Create a tempdir with mode 0700 (required by XDG validation).
    /// `tempfile::tempdir()` may produce 0755 depending on the system umask,
    /// so we chmod explicitly.
    fn tempdir_0700() -> tempfile::TempDir {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        tmp
    }

    /// Happy path: a tempdir (mode 0700, owned by the test runner) is accepted.
    /// Exercises `validate_runtime_dir` directly — no env mutation.
    #[test]
    fn happy_path_valid_dir() {
        let tmp = tempdir_0700();
        let result = validate_runtime_dir(tmp.path().to_owned());
        assert_eq!(result, tmp.path());
    }

    /// `resolve_xdg_value(None)` → "unset or empty" panic. Pure, no env mutation.
    #[test]
    fn unset_panics() {
        let result = std::panic::catch_unwind(|| resolve_xdg_value(None));
        let msg = unwrap_panic_msg(result.unwrap_err());
        assert!(
            msg.contains("XDG_RUNTIME_DIR validation failed"),
            "missing prefix in: {msg}"
        );
        assert!(msg.contains("unset or empty"), "missing reason in: {msg}");
    }

    /// `resolve_xdg_value(Some(""))` → same "unset or empty" panic. Pure, no env mutation.
    #[test]
    fn empty_string_panics() {
        let result = std::panic::catch_unwind(|| resolve_xdg_value(Some("")));
        let msg = unwrap_panic_msg(result.unwrap_err());
        assert!(
            msg.contains("XDG_RUNTIME_DIR validation failed"),
            "missing prefix in: {msg}"
        );
        assert!(msg.contains("unset or empty"), "missing reason in: {msg}");
    }

    /// Relative path → panic identifying "absolute" and the offending value.
    /// No env mutation.
    #[test]
    fn relative_path_panics() {
        let result = std::panic::catch_unwind(|| resolve_xdg_value(Some("relative/path")));
        let msg = unwrap_panic_msg(result.unwrap_err());
        assert!(
            msg.contains("XDG_RUNTIME_DIR validation failed"),
            "missing prefix in: {msg}"
        );
        assert!(msg.contains("absolute"), "missing 'absolute' in: {msg}");
        assert!(
            msg.contains("relative/path"),
            "missing offending value in: {msg}"
        );
    }

    /// Nonexistent path → panic identifying "does not exist" and a path fragment.
    /// No env mutation.
    #[test]
    fn nonexistent_path_panics() {
        let tmp = tempdir_0700();
        let nonexistent = tmp.path().join("does-not-exist");
        let result = std::panic::catch_unwind(|| validate_runtime_dir(nonexistent));
        let msg = unwrap_panic_msg(result.unwrap_err());
        assert!(
            msg.contains("XDG_RUNTIME_DIR validation failed"),
            "missing prefix in: {msg}"
        );
        assert!(msg.contains("does not exist"), "missing reason in: {msg}");
        assert!(
            msg.contains("does-not-exist"),
            "missing path fragment in: {msg}"
        );
    }

    /// Regular file → panic identifying "not a directory" and a fragment of the file name.
    /// No env mutation.
    #[test]
    fn regular_file_panics() {
        let tmp = tempdir_0700();
        let file_path = tmp.path().join("notadir.txt");
        std::fs::write(&file_path, b"").unwrap();
        let result = std::panic::catch_unwind(|| validate_runtime_dir(file_path));
        let msg = unwrap_panic_msg(result.unwrap_err());
        assert!(
            msg.contains("XDG_RUNTIME_DIR validation failed"),
            "missing prefix in: {msg}"
        );
        assert!(msg.contains("not a directory"), "missing reason in: {msg}");
        assert!(
            msg.contains("notadir.txt"),
            "missing file-name fragment in: {msg}"
        );
    }

    /// Wrong mode (0o755) → panic identifying "mode", observed and expected values, and
    /// a fragment of the offending path. No env mutation.
    #[test]
    fn wrong_mode_panics() {
        let tmp = tempdir_0700();
        // chmod 0o755 to trigger the mode check.
        std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        let path = tmp.path().to_owned();
        let result = std::panic::catch_unwind(|| validate_runtime_dir(path));
        // Restore permissions so tempdir cleanup doesn't fail.
        let _ = std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o700));
        let msg = unwrap_panic_msg(result.unwrap_err());
        assert!(
            msg.contains("XDG_RUNTIME_DIR validation failed"),
            "missing prefix in: {msg}"
        );
        assert!(msg.contains("mode"), "missing 'mode' in: {msg}");
        assert!(msg.contains("0755"), "missing observed mode in: {msg}");
        assert!(msg.contains("0700"), "missing expected mode in: {msg}");
        // The message must include a fragment that identifies the offending path.
        let path_str = tmp.path().to_string_lossy();
        let path_fragment = path_str.rsplit('/').next().unwrap_or(&path_str);
        assert!(
            msg.contains(path_fragment),
            "missing path fragment {path_fragment:?} in: {msg}"
        );
    }

    /// Wrong owner → panic identifying "directory is not owned by the current euid",
    /// the observed uid, and the expected euid.
    #[test]
    fn wrong_owner_panics() {
        let fake_path = std::path::Path::new("/tmp/xdg-wrong-owner-test");
        // `validate_dir_owner` takes `&Path`, which is `!UnwindSafe`, so
        // `AssertUnwindSafe` is required here. This is unlike the public API
        // functions (`validate_runtime_dir`, `resolve_xdg_value`) which take
        // `PathBuf`/`&str` (both `UnwindSafe`) and need no wrapper.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            validate_dir_owner(9999, 1000, fake_path)
        }));
        let msg = unwrap_panic_msg(result.unwrap_err());
        assert!(
            msg.contains("directory is not owned by the current euid"),
            "missing reason in: {msg}"
        );
        assert!(msg.contains("9999"), "missing observed_uid in: {msg}");
        assert!(msg.contains("1000"), "missing expected_euid in: {msg}");
    }
}
