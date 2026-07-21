use std::path::Path;
use std::sync::Arc;

/// Writer for raw CC NDJSON protocol messages.
///
/// NOT a tracing layer — CC protocol messages are a separate concern from
/// structured application logs. Each line is timestamped with direction:
///
/// ```text
/// 2026-03-19T14:30:00.123Z <<< {"type":"assistant",...}
/// 2026-03-19T14:30:01.456Z >>> {"type":"control_request",...}
/// ```
///
/// Lifecycle is managed by the CC subprocess manager (Phase 4), not by
/// `obs::init()`. One writer per CC session.
///
/// The inner file is backed by `reopen::Reopen<std::fs::File>` so that
/// `logrotate`'s rename+SIGHUP workflow is supported: after a SIGHUP the
/// next write transparently reopens the file at the canonical path. Signal
/// registration is opt-in via `register_sighup_reopen`; the CLI is a
/// short-lived process not managed by systemd and does not call that method.
pub struct TranscriptWriter {
    inner: Arc<std::sync::Mutex<reopen::Reopen<std::fs::File>>>,
    handle: reopen::Handle,
    sig_id: Option<signal_hook::SigId>,
}

impl TranscriptWriter {
    /// Create a new transcript writer. The file is created (or appended to) at
    /// `log_dir/file_name`. No SIGHUP handler is installed; call
    /// `register_sighup_reopen` after construction if the process should
    /// reopen this file on SIGHUP (server-side only; not the CLI).
    ///
    pub fn new(log_dir: &Path, file_name: &str) -> std::io::Result<Self> {
        std::fs::create_dir_all(log_dir)?;
        let path = log_dir.join(file_name);
        let opener = Box::new(move || super::layers::open_log_file(&path));
        let reopenable = reopen::Reopen::new(opener)?;
        let handle = reopenable.handle();
        Ok(Self {
            inner: Arc::new(std::sync::Mutex::new(reopenable)),
            handle,
            sig_id: None,
        })
    }

    /// Register this writer with the process-wide SIGHUP dispatcher so that
    /// logrotate's `kill -HUP` triggers a reopen on the next write.
    ///
    /// Only the server (bridge) invokes this. The CLI does not: the CLI is a
    /// short-lived process not run under systemd; registering it with SIGHUP
    /// dispatch would change the CLI's default SIGHUP behavior (terminate).
    ///
    /// Idempotent: calling this more than once on the same writer unregisters
    /// the prior SigId before installing the new one, preventing signal-hook
    /// table leaks from duplicate registrations.
    pub fn register_sighup_reopen(&mut self) -> std::io::Result<()> {
        // Unregister any prior registration before installing a new one.
        if let Some(old_id) = self.sig_id.take() {
            signal_hook::low_level::unregister(old_id);
        }
        let sig_id = self.handle.register_signal(libc::SIGHUP)?;
        self.sig_id = Some(sig_id);
        Ok(())
    }

    /// Log a message received from CC (CC → us).
    pub async fn log_received(&self, raw_line: &str) -> std::io::Result<()> {
        self.write_line("<<<", raw_line).await
    }

    /// Log a message sent to CC (us → CC).
    pub async fn log_sent(&self, raw_line: &str) -> std::io::Result<()> {
        self.write_line(">>>", raw_line).await
    }

    async fn write_line(&self, direction: &str, raw_line: &str) -> std::io::Result<()> {
        let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let line = format!("{timestamp} {direction} {raw_line}\n");
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            use std::io::Write as _;
            let mut guard = inner.lock().unwrap();
            guard.write_all(line.as_bytes())?;
            guard.flush()
        })
        .await
        .unwrap_or_else(|join_err| {
            if join_err.is_panic() {
                // A panic inside the blocking task means the write_all/flush
                // sequence panicked — a code bug. Re-panic in the calling task.
                std::panic::resume_unwind(join_err.into_panic());
            }
            // Runtime shutdown cancelled the blocking task. Surface as a write
            // error so the call-site tracing::error! path fires cleanly without
            // a spurious panic during graceful shutdown.
            Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                join_err,
            ))
        })
    }

    /// Signal the underlying `Reopen` to reopen on its next write, bypassing
    /// signal delivery. The reopen is deferred — it happens on the next
    /// `write_all` call, not synchronously here. Used in tests to exercise the
    /// reopen path without relying on SIGHUP delivery timing.
    #[cfg(test)]
    pub fn signal_reopen_for_test(&self) {
        self.handle.reopen();
    }
}

impl Drop for TranscriptWriter {
    fn drop(&mut self) {
        if let Some(id) = self.sig_id.take() {
            signal_hook::low_level::unregister(id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn transcript_write_and_read() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let writer =
            TranscriptWriter::new(dir.path(), "test_transcript.log").expect("create writer");

        writer
            .log_received(r#"{"type":"assistant","message":"hello"}"#)
            .await
            .expect("log received");
        writer
            .log_sent(r#"{"type":"user","message":"hi"}"#)
            .await
            .expect("log sent");

        let content = std::fs::read_to_string(dir.path().join("test_transcript.log"))
            .expect("read transcript");
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("<<<"));
        assert!(lines[0].contains(r#"{"type":"assistant","message":"hello"}"#));
        assert!(lines[1].contains(">>>"));
        assert!(lines[1].contains(r#"{"type":"user","message":"hi"}"#));
    }

    #[tokio::test]
    async fn reopen_after_rename_writes_to_new_file() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let canonical = dir.path().join("cc-test-conv1.ndjson");

        let writer =
            TranscriptWriter::new(dir.path(), "cc-test-conv1.ndjson").expect("create writer");

        writer.log_received("before").await.expect("log before");

        let before_content = std::fs::read_to_string(&canonical).expect("read before");
        assert!(
            before_content.contains("before"),
            "sanity: file has before line"
        );

        // Simulate logrotate: rename the file.
        let rotated = dir.path().join("cc-test-conv1.ndjson.1");
        std::fs::rename(&canonical, &rotated).expect("rename");

        // Trigger reopen (in production, SIGHUP does this).
        writer.signal_reopen_for_test();

        writer.log_sent("after").await.expect("log after");

        let rotated_content = std::fs::read_to_string(&rotated).expect("read rotated");
        assert!(
            rotated_content.contains("before"),
            "rotated file retains pre-rotation line"
        );
        assert!(
            !rotated_content.contains("after"),
            "rotated file must not have post-rotation line"
        );

        let new_content = std::fs::read_to_string(&canonical).expect("read new canonical");
        assert!(
            new_content.contains("after"),
            "canonical path has post-rotation line"
        );
        assert!(
            !new_content.contains("before"),
            "canonical path must not have pre-rotation line"
        );
    }

    #[tokio::test]
    async fn multiple_writers_all_reopen() {
        let dir = tempfile::tempdir().expect("create temp dir");

        let w1 = TranscriptWriter::new(dir.path(), "cc-app-conv1.ndjson").expect("w1");
        let w2 = TranscriptWriter::new(dir.path(), "cc-app-conv2.ndjson").expect("w2");
        let w3 = TranscriptWriter::new(dir.path(), "cc-app-conv3.ndjson").expect("w3");

        w1.log_received("w1-before").await.expect("w1 before");
        w2.log_received("w2-before").await.expect("w2 before");
        w3.log_received("w3-before").await.expect("w3 before");

        // Rename all three files.
        std::fs::rename(
            dir.path().join("cc-app-conv1.ndjson"),
            dir.path().join("cc-app-conv1.ndjson.1"),
        )
        .expect("rename w1");
        std::fs::rename(
            dir.path().join("cc-app-conv2.ndjson"),
            dir.path().join("cc-app-conv2.ndjson.1"),
        )
        .expect("rename w2");
        std::fs::rename(
            dir.path().join("cc-app-conv3.ndjson"),
            dir.path().join("cc-app-conv3.ndjson.1"),
        )
        .expect("rename w3");

        // Trigger reopen on each (SIGHUP would flip all atomics simultaneously).
        w1.signal_reopen_for_test();
        w2.signal_reopen_for_test();
        w3.signal_reopen_for_test();

        w1.log_sent("w1-after").await.expect("w1 after");
        w2.log_sent("w2-after").await.expect("w2 after");
        w3.log_sent("w3-after").await.expect("w3 after");

        for (name, before_tag, after_tag) in [
            ("cc-app-conv1.ndjson", "w1-before", "w1-after"),
            ("cc-app-conv2.ndjson", "w2-before", "w2-after"),
            ("cc-app-conv3.ndjson", "w3-before", "w3-after"),
        ] {
            let canon = std::fs::read_to_string(dir.path().join(name))
                .unwrap_or_else(|_| panic!("read canonical {name}"));
            let rotated = std::fs::read_to_string(dir.path().join(format!("{name}.1")))
                .unwrap_or_else(|_| panic!("read rotated {name}.1"));
            assert!(canon.contains(after_tag), "{name} canonical has after");
            assert!(
                !canon.contains(before_tag),
                "{name} canonical has no before"
            );
            assert!(rotated.contains(before_tag), "{name} rotated has before");
            assert!(!rotated.contains(after_tag), "{name} rotated has no after");
        }
    }

    #[tokio::test]
    async fn dropped_writer_does_not_break_handler() {
        let dir = tempfile::tempdir().expect("create temp dir");

        // Construct a *registered* writer and drop it. This exercises the
        // Some(id) branch of Drop — the real non-trivial path — verifying that
        // unregister is called without panic or double-free.
        let mut w1 = TranscriptWriter::new(dir.path(), "cc-dropped-conv1.ndjson").expect("w1");
        w1.register_sighup_reopen().expect("register w1");
        drop(w1); // Drop::drop must call unregister(sig_id) cleanly.

        // A second registered writer constructed after w1's drop must work
        // correctly — the dropped writer's unregister must not corrupt the
        // signal-hook table for subsequent registrations.
        let mut w2 = TranscriptWriter::new(dir.path(), "cc-dropped-conv2.ndjson").expect("w2");
        w2.register_sighup_reopen().expect("register w2");
        w2.log_received("line1").await.expect("line1");
        std::fs::rename(
            dir.path().join("cc-dropped-conv2.ndjson"),
            dir.path().join("cc-dropped-conv2.ndjson.1"),
        )
        .expect("rename");
        w2.signal_reopen_for_test();
        w2.log_sent("line2").await.expect("line2");

        let canon =
            std::fs::read_to_string(dir.path().join("cc-dropped-conv2.ndjson")).expect("read");
        assert!(canon.contains("line2"), "reopen worked after other drop");
        assert!(
            !canon.contains("line1"),
            "canonical has no pre-rotation line"
        );
    }

    #[tokio::test]
    async fn reopen_failure_surfaces_error() {
        // Skip when running as root — chmod-based permission denial is a no-op.
        if unsafe { libc::geteuid() } == 0 {
            eprintln!("reopen_failure_surfaces_error: skipped (running as root)");
            return;
        }

        let dir = tempfile::tempdir().expect("create temp dir");
        let file_name = "cc-perm-conv1.ndjson";
        let canonical = dir.path().join(file_name);

        let writer = TranscriptWriter::new(dir.path(), file_name).expect("create writer");

        writer.log_received("before").await.expect("before");

        // Rename so the file is gone from the canonical path.
        let rotated = dir.path().join(format!("{file_name}.1"));
        std::fs::rename(&canonical, &rotated).expect("rename");

        // Remove write/create permission from the directory so reopen fails.
        // This must happen *before* signal_reopen_for_test so the sequence
        // matches the documented design: rename → chmod → signal_reopen →
        // write (assert Err). Doing it in this order makes the test resilient
        // to eager-reopen implementations.
        let dir_path = dir.path().to_owned();
        std::fs::set_permissions(
            &dir_path,
            std::os::unix::fs::PermissionsExt::from_mode(0o555),
        )
        .expect("chmod dir to read-only");

        // Ensure we restore permissions even if assertions panic.
        struct RestorePerms(std::path::PathBuf);
        impl Drop for RestorePerms {
            fn drop(&mut self) {
                let _ = std::fs::set_permissions(
                    &self.0,
                    std::os::unix::fs::PermissionsExt::from_mode(0o755),
                );
            }
        }
        let _restore = RestorePerms(dir_path.clone());

        // Trigger reopen — the underlying Reopen will attempt to open on next write.
        writer.signal_reopen_for_test();

        // The next write must surface an error (cannot create file in read-only dir).
        let result = writer.log_sent("after-perm-fail").await;
        assert!(result.is_err(), "write after failed reopen must return Err");

        // Restore permissions; retry must succeed.
        std::fs::set_permissions(
            &dir_path,
            std::os::unix::fs::PermissionsExt::from_mode(0o755),
        )
        .expect("restore perms");

        writer
            .log_sent("after-restore")
            .await
            .expect("write after perm restore must succeed");
        let canon = std::fs::read_to_string(&canonical).expect("read after restore");
        assert!(
            canon.contains("after-restore"),
            "post-restore line in canonical"
        );
    }

    /// Verify that `TranscriptWriter::new` does not install a SIGHUP handler.
    /// The CLI relies on this: default SIGHUP disposition (terminate) must
    /// remain unchanged until `register_sighup_reopen` is explicitly called.
    #[tokio::test]
    async fn new_does_not_register_sighup() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let writer =
            TranscriptWriter::new(dir.path(), "cc-noreg-conv1.ndjson").expect("create writer");

        // sig_id being None is the invariant asserted by the design (§2.1):
        // "No signal handler is installed here."
        assert!(
            writer.sig_id.is_none(),
            "new() must not register a SIGHUP handler; sig_id must be None"
        );
    }
}
