//! Shared secret-file loading helper used by webhook and MQTT config paths.

use std::path::Path;

/// A secret that is handed onward rather than compared: it must survive in
/// plaintext and must never reach a log, a panic message, or a command line.
///
/// The redacting `Debug` is the point. A comparison secret can be stored as a
/// digest and printed freely; a bearer credential cannot, so the type carries
/// the discipline instead of every struct that holds one. `expose` is the only
/// way to the bytes, and it is spelled to be visible at the call site.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretString(String);

impl SecretString {
    pub fn new(value: String) -> Self {
        Self(value)
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretString(<redacted>)")
    }
}

/// Read a secret from a file: trim whitespace, panic on missing/unreadable/empty.
///
/// The `label` string is embedded verbatim in panic messages; callers should
/// provide enough context to identify the config field (e.g.
/// `"[[mqtt_client]] \"foo\" password_file"` or `"[[repo]] \"myrepo\""`).
pub(crate) fn load_secret_file(label: &str, path: &Path) -> String {
    let contents = std::fs::read_to_string(path).unwrap_or_else(|e| {
        panic!(
            "config: {label} — failed to read secret file at {}: {e}",
            path.display(),
        )
    });
    let trimmed = contents.trim();
    if trimmed.is_empty() {
        panic!(
            "config: {label} — secret file at {} is empty (whitespace-only). \
             Either omit the field or populate the file.",
            path.display(),
        );
    }
    trimmed.to_string()
}

/// Read a secret from a file that must also be unreadable to every other local
/// account: [`load_secret_file`] plus a Unix mode check, matching ssh's posture
/// on private keys.
///
/// Used for credentials that authenticate a network principal, where the file
/// *is* the identity and a group- or world-readable copy silently hands that
/// identity to any local process. The other secret-file callers keep the
/// unchecked reader; widening the check to them is a separate decision with its
/// own migration.
///
/// # Panics
///
/// Everything [`load_secret_file`] panics on, plus a mode with any group or
/// other bit set. Non-Unix targets have no mode to check and get the plain read.
pub(crate) fn load_secret_file_private(label: &str, path: &Path) -> String {
    if let Some(problem) = private_mode_error(path) {
        panic!(
            "config: {label} — secret file at {}: {problem}",
            path.display(),
        );
    }
    load_secret_file(label, path)
}

#[cfg(unix)]
fn private_mode_error(path: &Path) -> Option<String> {
    use std::os::unix::fs::PermissionsExt as _;
    // An unreadable path is not this check's failure to report: the read that
    // follows produces the message naming it.
    let mode = std::fs::metadata(path).ok()?.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        return Some(format!(
            "mode {mode:04o} is group/world-accessible; chmod 600 it"
        ));
    }
    None
}

#[cfg(not(unix))]
fn private_mode_error(_path: &Path) -> Option<String> {
    None
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::*;

    #[test]
    fn secret_string_debug_hides_the_value() {
        let secret = SecretString::new("sk-ant-oat01-tOkEn".to_string());
        assert_eq!(format!("{secret:?}"), "SecretString(<redacted>)");
        assert!(!format!("{secret:?}").contains("tOkEn"));
        assert_eq!(secret.expose(), "sk-ant-oat01-tOkEn");
    }

    #[test]
    fn reads_and_trims_secret() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(b"  my-secret\n").unwrap();
        let result = load_secret_file("test-label", f.path());
        assert_eq!(result, "my-secret");
    }

    #[test]
    #[should_panic(expected = "failed to read secret file")]
    fn panics_on_missing_file() {
        load_secret_file(
            "test-label",
            std::path::Path::new("/nonexistent/path/secret.txt"),
        );
    }

    #[test]
    #[should_panic(expected = "is empty (whitespace-only)")]
    fn panics_on_whitespace_only() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(b"   \n  \t  ").unwrap();
        load_secret_file("test-label", f.path());
    }

    #[cfg(unix)]
    #[test]
    fn private_reader_accepts_an_owner_only_file() {
        use std::os::unix::fs::PermissionsExt as _;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(b"tight\n").unwrap();
        std::fs::set_permissions(f.path(), std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(load_secret_file_private("test-label", f.path()), "tight");
    }

    #[cfg(unix)]
    #[test]
    #[should_panic(expected = "mode 0640 is group/world-accessible")]
    fn private_reader_rejects_a_group_readable_file() {
        use std::os::unix::fs::PermissionsExt as _;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(b"loose\n").unwrap();
        std::fs::set_permissions(f.path(), std::fs::Permissions::from_mode(0o640)).unwrap();
        load_secret_file_private("test-label", f.path());
    }

    /// A missing file reports the read failure, not a mode failure: there is no
    /// mode to speak of, and naming the wrong problem sends an operator to the
    /// wrong fix.
    #[test]
    #[should_panic(expected = "failed to read secret file")]
    fn private_reader_reports_a_missing_file_as_a_read_failure() {
        load_secret_file_private(
            "test-label",
            std::path::Path::new("/nonexistent/path/secret.txt"),
        );
    }
}
