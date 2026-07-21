use std::sync::atomic::{AtomicBool, Ordering};

use super::alerting::{AlertDispatcher, AlertSeverity, PanicMailConfig, send_panic_mail_sync};
use super::build_alert_context;
use super::config::{AlertBackend, ObsConfig};

/// Extract location and message strings from a `PanicHookInfo`.
///
/// Both hooks use identical extraction logic; this avoids duplication and
/// ensures a fix to payload-type handling applies to both.
fn extract_panic_info(info: &std::panic::PanicHookInfo<'_>) -> (String, String) {
    let location = info
        .location()
        .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
        .unwrap_or_else(|| "unknown".to_string());
    let message = info
        .payload()
        .downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| info.payload().downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string());
    (location, message)
}

/// Set when `install_panic_hook` runs. The pending hook reads this flag to
/// suppress its own send once the full hook has been installed, preventing a
/// double mail send on any panic that occurs after startup completes.
pub(crate) static FULL_HOOK_INSTALLED: AtomicBool = AtomicBool::new(false);

/// Install an early, dependency-free panic hook covering the window between
/// config-parse and full-hook installation.
///
/// Derives the mail recipient from `config` directly — no `AlertDispatcher`,
/// no tokio runtime, no rate limiter. Call this immediately after
/// `obs_config::build` returns, before `obs::init`.
///
/// **No-op** when the alert backend is not mail (nothing to send to); the
/// default hook stays in place.
///
/// When superseded by `install_panic_hook`, the pending hook's send is gated
/// behind `FULL_HOOK_INSTALLED` so it does not double-send on post-startup
/// panics (the full hook chains into this one via `take_hook`).
///
/// **Double-install:** Calling this function twice (mail backend) installs two
/// chained pending hooks. Both have `FULL_HOOK_INSTALLED` false at panic time,
/// so both attempt a send — two mails. The expected call pattern is a single
/// call in `run_server` immediately after `obs_config::build`; do not add
/// additional call sites without also adding a guard.
pub fn install_pending_panic_hook(config: &ObsConfig) {
    // Extract (to, subject_label) — only arm when backend is mail.
    let (to, subject_label) = match config.alert.as_ref() {
        Some(alert_cfg) => match &alert_cfg.backend {
            AlertBackend::Mail { to, subject_label } => (to.clone(), subject_label.clone()),
            AlertBackend::Ntfy { .. } => return,
        },
        None => return,
    };

    // Build instance-level context using the shared helper so pending-hook and
    // full-hook alert bodies are byte-identical for the same inputs.
    let context = build_alert_context(config);
    let cfg = PanicMailConfig { to, subject_label };

    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        // Gate send on FULL_HOOK_INSTALLED: once the full hook is installed it
        // chains into this pending hook, so we must not double-send.
        if !FULL_HOOK_INSTALLED.load(Ordering::Relaxed) {
            let (location, message) = extract_panic_info(info);

            // best-effort; tracing may not be initialized yet — no-ops if so.
            tracing::error!(panic = true, location = %location, "PANIC (pre-init): {message}");

            send_panic_mail_sync(
                &cfg,
                &context,
                AlertSeverity::Critical,
                "Brenn PANIC",
                &format!("{message} at {location}"),
            );
        }

        // Unconditional: always forward so the panic still surfaces and the
        // process still aborts — regardless of whether we sent mail.
        default_hook(info);
    }));
}

/// Install a custom panic hook that logs the panic and fires an alert.
///
/// The alert is best-effort — if the process is dying, the background alert
/// task may not complete. The diagnostic log always has the panic regardless.
pub fn install_panic_hook(alert_dispatcher: AlertDispatcher) {
    FULL_HOOK_INSTALLED.store(true, Ordering::Relaxed);
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let (location, message) = extract_panic_info(info);

        tracing::error!(
            panic = true,
            location = %location,
            "PANIC: {message}"
        );

        // Send synchronously via std::process::Command so the alert completes
        // before the process exits. The tokio runtime may be tearing down by
        // the time a panic hook fires, so the async channel path is unreliable.
        // send_panic_alert_sync is a no-op when the configured backend is not
        // mail (e.g. ntfy, noop). Errors inside it are logged and swallowed.
        alert_dispatcher.send_panic_alert_sync(
            AlertSeverity::Critical,
            "Brenn PANIC",
            &format!("{message} at {location}"),
        );

        // Also queue into the async channel as a best-effort fallback (e.g. if
        // the panic occurs early before the process starts unwinding, the
        // background task may still deliver it). try_alert never panics.
        alert_dispatcher.try_alert(
            AlertSeverity::Critical,
            "Brenn PANIC".to_string(),
            format!("{message} at {location}"),
        );

        default_hook(info);
    }));
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::sync::{Mutex, OnceLock};

    use super::super::alerting::SEND_PANIC_MAIL_SYNC_CALL_COUNT;
    use super::super::config::{AlertBackend, AlertConfig, ObsConfig, RateLimitConfig};
    use super::{
        FULL_HOOK_INSTALLED, build_alert_context, install_panic_hook, install_pending_panic_hook,
    };

    // Shared mutex serializing all tests that touch the global panic hook.
    // The panic hook is a process global; concurrent tests interleaving
    // install/take/set_hook produce undefined behavior.
    //
    // Every test that calls install_pending_panic_hook / install_panic_hook
    // or directly calls send_panic_mail_sync MUST:
    //   1. Acquire _guard = hook_mutex().lock().unwrap()
    //   2. Reset FULL_HOOK_INSTALLED.store(false, SeqCst)
    //   3. Reset SEND_PANIC_MAIL_SYNC_CALL_COUNT.store(0, SeqCst)
    //   4. Call restore_default_hook() before the guard drops.
    static HOOK_TEST_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();

    fn hook_mutex() -> &'static Mutex<()> {
        HOOK_TEST_MUTEX.get_or_init(|| Mutex::new(()))
    }

    fn mail_obs_config() -> ObsConfig {
        ObsConfig {
            log_dir: std::path::PathBuf::from("/tmp"),
            console_level: tracing::level_filters::LevelFilter::ERROR,
            file_level: tracing::level_filters::LevelFilter::ERROR,
            diagnostic_log_name: "test-diag.log".into(),
            security_log_name: "test-sec.log".into(),
            alert: Some(AlertConfig {
                backend: AlertBackend::Mail {
                    to: "test@example.com".into(),
                    subject_label: "test".into(),
                },
                rate_limit: RateLimitConfig {
                    max_alerts: 10,
                    window_secs: 60,
                },
            }),
            instance_name: Some("test-instance".into()),
        }
    }

    fn ntfy_obs_config() -> ObsConfig {
        ObsConfig {
            log_dir: std::path::PathBuf::from("/tmp"),
            console_level: tracing::level_filters::LevelFilter::ERROR,
            file_level: tracing::level_filters::LevelFilter::ERROR,
            diagnostic_log_name: "test-diag.log".into(),
            security_log_name: "test-sec.log".into(),
            alert: Some(AlertConfig {
                backend: AlertBackend::Ntfy {
                    url: "https://ntfy.example.com/test".into(),
                },
                rate_limit: RateLimitConfig {
                    max_alerts: 10,
                    window_secs: 60,
                },
            }),
            instance_name: None,
        }
    }

    fn no_alert_obs_config() -> ObsConfig {
        ObsConfig {
            log_dir: std::path::PathBuf::from("/tmp"),
            console_level: tracing::level_filters::LevelFilter::ERROR,
            file_level: tracing::level_filters::LevelFilter::ERROR,
            diagnostic_log_name: "test-diag.log".into(),
            security_log_name: "test-sec.log".into(),
            alert: None,
            instance_name: None,
        }
    }

    // Restore the panic hook to a no-op so each test cleans up after itself.
    // The real default (print to stderr + abort) must never re-arm mid-suite.
    fn restore_default_hook() {
        std::panic::set_hook(Box::new(|_| {}));
    }

    // -----------------------------------------------------------------------
    // Test 1a: install_pending_panic_hook is a no-op for ntfy backend (AC 5)
    // -----------------------------------------------------------------------

    #[test]
    fn pending_hook_noop_for_ntfy_backend() {
        let _guard = hook_mutex().lock().unwrap();
        FULL_HOOK_INSTALLED.store(false, Ordering::SeqCst);
        SEND_PANIC_MAIL_SYNC_CALL_COUNT.store(0, Ordering::SeqCst);

        // Install a sentinel hook; a no-op install_pending_panic_hook must not
        // replace it.
        static SENTINEL_NTFY: std::sync::atomic::AtomicBool =
            std::sync::atomic::AtomicBool::new(false);
        SENTINEL_NTFY.store(false, Ordering::SeqCst);
        std::panic::set_hook(Box::new(|_| {
            SENTINEL_NTFY.store(true, Ordering::SeqCst);
        }));

        install_pending_panic_hook(&ntfy_obs_config());

        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            panic!("test-ntfy-noop");
        }));
        assert!(
            SENTINEL_NTFY.load(Ordering::SeqCst),
            "sentinel hook was replaced — must be no-op for ntfy"
        );
        assert_eq!(SEND_PANIC_MAIL_SYNC_CALL_COUNT.load(Ordering::SeqCst), 0);

        restore_default_hook();
    }

    // -----------------------------------------------------------------------
    // Test 1b: install_pending_panic_hook is a no-op for no-alert backend (AC 5)
    // -----------------------------------------------------------------------

    #[test]
    fn pending_hook_noop_for_no_alert_backend() {
        let _guard = hook_mutex().lock().unwrap();
        FULL_HOOK_INSTALLED.store(false, Ordering::SeqCst);
        SEND_PANIC_MAIL_SYNC_CALL_COUNT.store(0, Ordering::SeqCst);

        static SENTINEL_NONE: std::sync::atomic::AtomicBool =
            std::sync::atomic::AtomicBool::new(false);
        SENTINEL_NONE.store(false, Ordering::SeqCst);
        std::panic::set_hook(Box::new(|_| {
            SENTINEL_NONE.store(true, Ordering::SeqCst);
        }));

        install_pending_panic_hook(&no_alert_obs_config());

        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            panic!("test-none-noop");
        }));
        assert!(
            SENTINEL_NONE.load(Ordering::SeqCst),
            "sentinel hook was replaced — must be no-op for no-alert"
        );
        assert_eq!(SEND_PANIC_MAIL_SYNC_CALL_COUNT.load(Ordering::SeqCst), 0);

        restore_default_hook();
    }

    // -----------------------------------------------------------------------
    // Test 2: pending hook attempts a send on in-window panic (AC 1, AC 2)
    // -----------------------------------------------------------------------

    #[test]
    fn pending_hook_attempts_send_for_mail_backend() {
        let _guard = hook_mutex().lock().unwrap();
        FULL_HOOK_INSTALLED.store(false, Ordering::SeqCst);
        SEND_PANIC_MAIL_SYNC_CALL_COUNT.store(0, Ordering::SeqCst);

        install_pending_panic_hook(&mail_obs_config());
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            panic!("test-in-window-panic");
        }));

        assert_eq!(
            SEND_PANIC_MAIL_SYNC_CALL_COUNT.load(Ordering::SeqCst),
            1,
            "pending hook must attempt exactly one send for a mail backend in-window panic"
        );

        restore_default_hook();
    }

    // -----------------------------------------------------------------------
    // Test 3: pending hook never masks the original panic (AC 4)
    // -----------------------------------------------------------------------

    #[test]
    fn pending_hook_does_not_mask_panic() {
        let _guard = hook_mutex().lock().unwrap();
        FULL_HOOK_INSTALLED.store(false, Ordering::SeqCst);
        SEND_PANIC_MAIL_SYNC_CALL_COUNT.store(0, Ordering::SeqCst);

        install_pending_panic_hook(&mail_obs_config());
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            panic!("original-panic-message");
        }));
        assert!(
            result.is_err(),
            "original panic must propagate through the pending hook"
        );

        let payload = result.unwrap_err();
        let msg = payload
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("");
        assert_eq!(msg, "original-panic-message");

        restore_default_hook();
    }

    // -----------------------------------------------------------------------
    // Test 4: at-most-one send across supersession (double-send edge case)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn at_most_one_send_after_full_hook_supersedes_pending() {
        use super::super::alerting::{PanicMailConfig as Pmc, noop_alert_dispatcher};

        // Acquire the mutex FIRST before any shared-state setup, so no other
        // hook-installing test can observe the resets or the intermediate hook states.
        let _guard = hook_mutex().lock().unwrap();
        FULL_HOOK_INSTALLED.store(false, Ordering::SeqCst);
        SEND_PANIC_MAIL_SYNC_CALL_COUNT.store(0, Ordering::SeqCst);

        // noop_alert_dispatcher requires the tokio runtime (spawns a drain task).
        // Constructed inside the lock so the resets above are already visible.
        let (dispatcher, _handle) = noop_alert_dispatcher();
        // Attach mail config so the full hook's send_panic_alert_sync path
        // actually calls send_panic_mail_sync (otherwise the None-guard short-circuits).
        let dispatcher = dispatcher.with_panic_mail(Pmc {
            to: "test@example.com".into(),
            subject_label: "test".into(),
        });

        // Step 1: pending hook.
        install_pending_panic_hook(&mail_obs_config());

        // Step 2: full hook (sets FULL_HOOK_INSTALLED = true; takes pending as predecessor).
        install_panic_hook(dispatcher);

        // Step 3: panic — full hook fires first (+1 send), chains to pending hook
        // which reads FULL_HOOK_INSTALLED = true and skips. Net: exactly 1.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            panic!("post-supersession-panic");
        }));

        assert_eq!(
            SEND_PANIC_MAIL_SYNC_CALL_COUNT.load(Ordering::SeqCst),
            1,
            "at most one sync send after full hook supersedes pending hook"
        );

        restore_default_hook();
    }

    // -----------------------------------------------------------------------
    // Test 5: context built by build_alert_context matches field ordering
    // -----------------------------------------------------------------------

    #[test]
    fn pending_hook_context_fields_match_full_hook() {
        // install_pending_panic_hook and install_panic_hook both call
        // build_alert_context — so their AlertContext values are byte-identical
        // by construction. This test guards against future changes that break
        // the expected field ordering (Host first, Instance second).
        let hostname = gethostname::gethostname().to_string_lossy().into_owned();
        let ctx = build_alert_context(&mail_obs_config());

        let rendered = ctx.render();
        assert!(
            rendered.starts_with(&format!("Host: {hostname}\n")),
            "Host field must be first: {rendered:?}"
        );
        assert!(
            rendered.contains("Instance: test-instance\n"),
            "Instance field must be present: {rendered:?}"
        );

        // Formatting parity (subject/body) with the full hook is verified by
        // alerting::tests::pending_hook_body_parity_with_full_hook.
    }

    // -----------------------------------------------------------------------
    // Test 4b: double-install behavior is documented, not silently surprising
    // -----------------------------------------------------------------------

    #[test]
    fn double_install_pending_hook_sends_twice() {
        // This test documents the known behavior: calling install_pending_panic_hook
        // twice (mail backend) installs two chained hooks and both attempt a send.
        // The expected production call pattern is a single call in run_server.
        // If this assertion ever becomes 1 (due to a guard being added), update
        // the doc comment on install_pending_panic_hook accordingly.
        let _guard = hook_mutex().lock().unwrap();
        FULL_HOOK_INSTALLED.store(false, Ordering::SeqCst);
        SEND_PANIC_MAIL_SYNC_CALL_COUNT.store(0, Ordering::SeqCst);

        install_pending_panic_hook(&mail_obs_config());
        install_pending_panic_hook(&mail_obs_config());

        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            panic!("double-install-test");
        }));

        assert_eq!(
            SEND_PANIC_MAIL_SYNC_CALL_COUNT.load(Ordering::SeqCst),
            2,
            "double install sends twice — see doc comment on install_pending_panic_hook"
        );

        restore_default_hook();
    }
}
