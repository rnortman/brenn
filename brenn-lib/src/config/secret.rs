//! Shared secret-file loading helper used by webhook and MQTT config paths.

use std::path::Path;

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

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::*;

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
}
