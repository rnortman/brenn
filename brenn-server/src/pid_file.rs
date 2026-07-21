use std::path::Path;

use tracing::info;

/// Writes the current process PID to `path` atomically via a temp-file rename.
/// Panics on any filesystem error.
pub(crate) fn write_pid_file(path: &Path) {
    use std::io::Write;
    let pid = std::process::id();
    let tmp = path.with_extension("tmp");
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap_or_else(|e| {
            panic!(
                "failed to create PID file directory {}: {e}",
                parent.display()
            );
        });
    }
    let mut f = std::fs::File::create(&tmp).unwrap_or_else(|e| {
        panic!("failed to create temp PID file {}: {e}", tmp.display());
    });
    writeln!(f, "{pid}").unwrap_or_else(|e| {
        panic!("failed to write PID file {}: {e}", tmp.display());
    });
    std::fs::rename(&tmp, path).unwrap_or_else(|e| {
        panic!(
            "failed to rename PID file {} -> {}: {e}",
            tmp.display(),
            path.display()
        );
    });
    info!("PID file written: {}", path.display());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_pid_file_creates_correct_content() {
        let dir = tempfile::tempdir().unwrap();
        let pid_path = dir.path().join("test.pid");
        write_pid_file(&pid_path);

        let content = std::fs::read_to_string(&pid_path).unwrap();
        let written_pid: u32 = content.trim().parse().expect("PID should be a number");
        assert_eq!(written_pid, std::process::id());
    }

    #[test]
    fn write_pid_file_is_atomic_no_temp_left() {
        let dir = tempfile::tempdir().unwrap();
        let pid_path = dir.path().join("brenn.pid");
        write_pid_file(&pid_path);

        // Only the final PID file should exist — no .tmp left behind.
        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].file_name(), "brenn.pid");
    }

    #[test]
    fn write_pid_file_creates_parent_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let pid_path = dir.path().join("nested").join("dir").join("brenn.pid");
        write_pid_file(&pid_path);

        assert!(pid_path.exists());
    }
}
