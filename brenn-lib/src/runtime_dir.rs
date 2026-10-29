use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

fn validate_dir_owner(label: &str, dir_uid: u32, euid: u32, path: &Path) {
    if dir_uid != euid {
        panic!(
            "{label} validation failed: directory is not owned by the current euid \
             (path={}, observed_uid={dir_uid}, expected_euid={euid})",
            path.display(),
        );
    }
}

/// Refuse a directory the server keeps private to itself unless it is
/// absolute, exists, is a directory, is owned by the current effective uid,
/// and has mode (low 9 bits) exactly `0o700`.
///
/// Panics with a message starting `"<label> validation failed:"`. A dangling
/// symlink is "does not exist". Symlinks at the root are tolerated:
/// `std::fs::metadata` follows them, so what is checked is the ownership and
/// mode of the final target. High bits (setuid/setgid/sticky) are not checked.
/// Canonicalisation is deliberately skipped.
pub fn validate_private_dir(label: &str, path: &Path) {
    if !path.is_absolute() {
        panic!(
            "{label} validation failed: path must be absolute \
             (path={}, got a relative path)",
            path.display(),
        );
    }

    let meta = match std::fs::metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            panic!(
                "{label} validation failed: path does not exist (path={}); create it, owned \
                 by the server's user, mode 0700",
                path.display(),
            );
        }
        Err(e) => {
            panic!(
                "{label} validation failed: could not stat path (path={}, error={e})",
                path.display(),
            );
        }
    };

    if !meta.is_dir() {
        panic!(
            "{label} validation failed: path is not a directory (path={})",
            path.display(),
        );
    }

    // SAFETY: geteuid() has no failure mode and no preconditions.
    let euid = unsafe { libc::geteuid() };
    validate_dir_owner(label, meta.uid(), euid, path);

    let mode = meta.mode() & 0o777;
    if mode != 0o700 {
        panic!(
            "{label} validation failed: directory mode must be 0700 \
             (path={}, observed_mode={mode:04o}, expected_mode=0700)",
            path.display(),
        );
    }
}

/// Validate a candidate `XDG_RUNTIME_DIR` path and return it if valid.
///
/// This is a pure function over its argument — it does not read the process
/// environment. Panics — with a message starting `"XDG_RUNTIME_DIR validation
/// failed:"` — under every condition [`validate_private_dir`] refuses.
pub fn validate_runtime_dir(path: PathBuf) -> PathBuf {
    validate_private_dir("XDG_RUNTIME_DIR", &path);
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

/// Directory under the runtime dir holding every conversation's tool socket
/// directory. A sibling of the per-slug `brenn/` namespace, not a child, so no
/// app slug can name it.
pub const TOOL_SOCKET_ROOT_NAME: &str = "brenn-tool-sockets";

/// The socket's file name inside its per-conversation directory.
pub const TOOL_SOCKET_FILE_NAME: &str = "tool.sock";

/// Longest path a unix socket may be bound at: `sockaddr_un.sun_path` is 108
/// bytes on Linux, one of which is the terminating NUL.
pub const MAX_SOCKET_PATH_BYTES: usize = 107;

/// `<runtime_dir>/brenn-tool-sockets`.
pub fn tool_socket_root(runtime_dir: &Path) -> PathBuf {
    runtime_dir.join(TOOL_SOCKET_ROOT_NAME)
}

/// A conversation's tool socket directory: `<root>/<slug>-conv<id>`.
pub fn tool_socket_dir(tool_socket_root: &Path, app_slug: &str, conversation_id: i64) -> PathBuf {
    tool_socket_root.join(format!("{app_slug}-conv{conversation_id}"))
}

/// The socket inside a tool socket directory, on the host or as a container
/// sees it.
pub fn tool_socket_in(dir: &Path) -> PathBuf {
    dir.join(TOOL_SOCKET_FILE_NAME)
}

/// Length in bytes of the longest tool socket path a conversation of
/// `app_slug` can have: the socket of `tool_socket_dir` at `i64::MAX`, the
/// longest id.
pub fn longest_tool_socket_path(tool_socket_root: &Path, app_slug: &str) -> usize {
    tool_socket_in(&tool_socket_dir(tool_socket_root, app_slug, i64::MAX))
        .as_os_str()
        .len()
}

/// Remove whatever is at `dir` (symlinks are removed, not followed) and create
/// it empty at mode 0700. Errors name the path.
pub fn recreate_private_dir(dir: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    let with_path = |what: &str, e: std::io::Error| {
        std::io::Error::new(e.kind(), format!("{what} {}: {e}", dir.display()))
    };
    match std::fs::remove_dir_all(dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(with_path("clearing", e)),
    }
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(dir)
        .map_err(|e| with_path("creating", e))
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
            validate_dir_owner("XDG_RUNTIME_DIR", 9999, 1000, fake_path)
        }));
        let msg = unwrap_panic_msg(result.unwrap_err());
        assert!(
            msg.contains("directory is not owned by the current euid"),
            "missing reason in: {msg}"
        );
        assert!(msg.contains("9999"), "missing observed_uid in: {msg}");
        assert!(msg.contains("1000"), "missing expected_euid in: {msg}");
    }

    /// The socket path layout is `<root>/<slug>-conv<id>/tool.sock`, and the
    /// longest one is measured at the 19 digits of `i64::MAX`.
    #[test]
    fn tool_socket_layout_and_longest_path() {
        let root = Path::new("/run/user/1000/brenn-tool-sockets");
        assert_eq!(
            tool_socket_in(&tool_socket_dir(root, "pfin", 42)),
            Path::new("/run/user/1000/brenn-tool-sockets/pfin-conv42/tool.sock"),
        );
        assert_eq!(
            longest_tool_socket_path(root, "pfin"),
            "/run/user/1000/brenn-tool-sockets/pfin-conv9223372036854775807/tool.sock".len(),
        );
    }

    /// `recreate_private_dir` empties what is there, or creates an absent
    /// directory, at 0700.
    #[test]
    fn recreate_private_dir_leaves_an_empty_0700_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("d");
        recreate_private_dir(&dir).unwrap();
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
        std::fs::write(dir.join("stale"), b"x").unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        recreate_private_dir(&dir).unwrap();
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
        let mode = std::fs::metadata(&dir).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o700);
    }

    /// A symlink at `dir` is removed and replaced by a real directory; its
    /// target and the target's contents are untouched.
    #[test]
    fn recreate_private_dir_replaces_a_symlink_without_following_it() {
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("target");
        std::fs::create_dir(&target).unwrap();
        std::fs::write(target.join("keep"), b"x").unwrap();
        let dir = tmp.path().join("d");
        std::os::unix::fs::symlink(&target, &dir).unwrap();

        recreate_private_dir(&dir).unwrap();

        let meta = std::fs::symlink_metadata(&dir).unwrap();
        assert!(meta.is_dir() && !meta.file_type().is_symlink());
        assert_eq!(meta.permissions().mode() & 0o777, 0o700);
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
        assert!(target.join("keep").is_file());
    }

    /// A regular file in the way is an error naming the path, not replaced.
    #[test]
    fn recreate_private_dir_refuses_a_file() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("f");
        std::fs::write(&file, b"x").unwrap();
        let err = recreate_private_dir(&file).unwrap_err();
        assert!(
            err.to_string().contains(&file.display().to_string()),
            "{err}"
        );
        assert!(file.is_file());
    }
}
