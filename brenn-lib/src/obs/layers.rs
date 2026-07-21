use super::alerting::{AlertDispatcher, MailAlerter, NtfyAlerter, PanicMailConfig, RateLimiter};
use super::build_alert_context;
use super::config::{AlertBackend, ObsConfig};
use super::panic_hook;
use std::fs::{File, OpenOptions};
use std::path::Path;
use tokio::task::JoinHandle;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

/// Holds everything that must stay alive for the duration of the program:
/// log file guards (flushed on drop), the alert background task handle,
/// and the alert dispatcher (for firing alerts from application code).
pub struct ObsGuard {
    _diagnostic_guard: WorkerGuard,
    _security_guard: WorkerGuard,
    _alert_task: JoinHandle<()>,
    /// The alert dispatcher. Clone this to pass to code that needs to fire alerts.
    pub alert_dispatcher: AlertDispatcher,
}

/// Initialize the observability subsystem.
///
/// Sets up:
/// - Layered tracing subscriber (console, diagnostic file, security JSON file)
/// - Alerting (ntfy if configured, no-op otherwise)
/// - Custom panic hook that logs + alerts
///
/// Returns an `ObsGuard` that must be held in scope for the lifetime of the
/// program. Dropping it flushes pending log writes.
pub fn init(config: &ObsConfig) -> ObsGuard {
    std::fs::create_dir_all(&config.log_dir).expect("failed to create log directory");

    // Console layer: human-readable, ANSI colors, filtered by console_level.
    let console_layer = fmt::layer().with_ansi(true).with_target(true).with_filter(
        EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new(config.console_level.to_string())),
    );

    // Diagnostic file layer: human-readable, no ANSI, stable filename.
    // Rotation is handled by logrotate; SIGHUP triggers file reopen via `reopen` crate.
    let diag_path = config.log_dir.join(&config.diagnostic_log_name);
    let diagnostic_file = reopen::Reopen::new(Box::new({
        let path = diag_path.clone();
        move || open_log_file(&path)
    }))
    .expect("failed to open diagnostic log");
    diagnostic_file
        .handle()
        .register_signal(libc::SIGHUP)
        .expect("failed to register SIGHUP for diagnostic log");
    let (diagnostic_writer, diagnostic_guard) = tracing_appender::non_blocking(diagnostic_file);
    let diagnostic_layer = fmt::layer()
        .with_ansi(false)
        .with_target(true)
        .with_writer(diagnostic_writer)
        .with_filter(EnvFilter::new(config.file_level.to_string()));

    // Security file layer: JSON format, only security_event=true events, stable filename.
    let sec_path = config.log_dir.join(&config.security_log_name);
    let security_file = reopen::Reopen::new(Box::new({
        let path = sec_path.clone();
        move || open_log_file(&path)
    }))
    .expect("failed to open security log");
    security_file
        .handle()
        .register_signal(libc::SIGHUP)
        .expect("failed to register SIGHUP for security log");
    let (security_writer, security_guard) = tracing_appender::non_blocking(security_file);
    let security_layer = fmt::layer()
        .json()
        .with_ansi(false)
        .with_target(false)
        .with_writer(security_writer)
        .with_filter(SecurityEventFilter);

    tracing_subscriber::registry()
        .with(console_layer)
        .with(diagnostic_layer)
        .with(security_layer)
        .init();

    // Set up alerting from config.
    // `panic_mail_cfg` is populated only for the `mail` backend so the panic
    // hook can send synchronously (std::process::Command) independent of the
    // tokio runtime state.
    let (raw_dispatcher, alert_task, panic_mail_cfg) = match &config.alert {
        Some(alert_config) => {
            let limiter = RateLimiter::new(
                alert_config.rate_limit.max_alerts,
                alert_config.rate_limit.window_secs,
            );
            match &alert_config.backend {
                AlertBackend::Ntfy { url } => {
                    let alerter = NtfyAlerter::new(url.clone());
                    let (d, h) = AlertDispatcher::new(alerter, limiter);
                    (d, h, None)
                }
                AlertBackend::Mail { to, subject_label } => {
                    // Fail fast: verify `mail` is on $PATH at startup.
                    let mail_check = std::process::Command::new("which")
                        .arg("mail")
                        .stdout(std::process::Stdio::null())
                        .stderr(std::process::Stdio::null())
                        .status();
                    match mail_check {
                        Ok(status) if status.success() => {}
                        Ok(status) => panic!(
                            "alerting backend is 'mail' but `mail` not found on $PATH \
                             (`which mail` exited {status})"
                        ),
                        Err(e) => panic!(
                            "alerting backend is 'mail' but could not run `which mail` \
                             to verify it is on $PATH: {e}"
                        ),
                    }
                    let alerter = MailAlerter::new(to.clone(), subject_label.clone());
                    let (d, h) = AlertDispatcher::new(alerter, limiter);
                    let cfg = PanicMailConfig {
                        to: to.clone(),
                        subject_label: subject_label.clone(),
                    };
                    (d, h, Some(cfg))
                }
            }
        }
        None => {
            let (d, h) = AlertDispatcher::noop();
            (d, h, None)
        }
    };

    // Add instance-level context via the shared helper (same fields, same order
    // as install_pending_panic_hook) so alert bodies are byte-identical for the
    // same inputs regardless of which hook fires.
    let alert_dispatcher = raw_dispatcher.with_context(build_alert_context(config));

    // Attach mail config for the synchronous panic send path. This must be
    // done after context fields (Host, Instance) are set so send_panic_alert_sync
    // renders the same body as the async path.
    let alert_dispatcher = if let Some(cfg) = panic_mail_cfg {
        alert_dispatcher.with_panic_mail(cfg)
    } else {
        alert_dispatcher
    };

    panic_hook::install_panic_hook(alert_dispatcher.clone());

    ObsGuard {
        _diagnostic_guard: diagnostic_guard,
        _security_guard: security_guard,
        _alert_task: alert_task,
        alert_dispatcher,
    }
}

/// Filter that only passes events with `security_event = true`.
///
/// This uses the visitor pattern to inspect structured fields on each event.
/// Only events that explicitly set `security_event = true` pass through to
/// the security log stream.
#[derive(Clone)]
struct SecurityEventFilter;

impl<S> tracing_subscriber::layer::Filter<S> for SecurityEventFilter {
    fn enabled(
        &self,
        meta: &tracing::Metadata<'_>,
        _cx: &tracing_subscriber::layer::Context<'_, S>,
    ) -> bool {
        // Only consider WARN and above for security events.
        meta.level() <= &tracing::Level::WARN
    }

    fn event_enabled(
        &self,
        event: &tracing::Event<'_>,
        _cx: &tracing_subscriber::layer::Context<'_, S>,
    ) -> bool {
        let mut visitor = SecurityFieldVisitor { is_security: false };
        event.record(&mut visitor);
        visitor.is_security
    }
}

struct SecurityFieldVisitor {
    is_security: bool,
}

impl tracing::field::Visit for SecurityFieldVisitor {
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        if field.name() == "security_event" && value {
            self.is_security = true;
        }
    }

    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}
}

/// Open a log file for appending, creating it if it doesn't exist.
pub(super) fn open_log_file(path: &Path) -> std::io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// A minimal layer that counts events it receives, for testing filters.
    #[derive(Clone)]
    struct CountingLayer {
        count: Arc<Mutex<u32>>,
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CountingLayer {
        fn on_event(
            &self,
            _event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            *self.count.lock().unwrap() += 1;
        }
    }

    #[test]
    fn open_log_file_creates_at_exact_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.log");
        let mut f = open_log_file(&path).unwrap();
        std::io::Write::write_all(&mut f, b"hello\n").unwrap();
        drop(f);

        // The file must exist at the exact path — no date suffix.
        assert!(path.exists(), "log file should exist at exact path");
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, "hello\n");

        // No other files should exist in the directory.
        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(
            entries.len(),
            1,
            "only one file should exist (no date-suffixed variants)"
        );
    }

    #[test]
    fn open_log_file_appends() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("append.log");

        let mut f1 = open_log_file(&path).unwrap();
        std::io::Write::write_all(&mut f1, b"first\n").unwrap();
        drop(f1);

        let mut f2 = open_log_file(&path).unwrap();
        std::io::Write::write_all(&mut f2, b"second\n").unwrap();
        drop(f2);

        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, "first\nsecond\n");
    }

    #[test]
    fn reopen_after_rename_writes_to_new_file() {
        // Simulates the logrotate flow:
        // 1. Write to log file
        // 2. Rename (logrotate moves it)
        // 3. Signal reopen
        // 4. Write again — should go to a new file at the original path
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("brenn.log");

        let reopenable = reopen::Reopen::new(Box::new({
            let p = log_path.clone();
            move || open_log_file(&p)
        }))
        .unwrap();
        let handle = reopenable.handle();

        // Wrap in a type that lets us write (Reopen needs &self for handle but &mut self for Write).
        // non_blocking does this in production; here we just use it directly since Reopen impls Write.
        let mut writer = reopenable;

        writer.write_all(b"before rotation\n").unwrap();
        writer.flush().unwrap();

        // Simulate logrotate: rename the file.
        let rotated = dir.path().join("brenn.log.1");
        std::fs::rename(&log_path, &rotated).unwrap();

        // Signal reopen (in production, SIGHUP does this).
        handle.reopen();

        writer.write_all(b"after rotation\n").unwrap();
        writer.flush().unwrap();

        // The rotated file has the old content.
        let old_content = std::fs::read_to_string(&rotated).unwrap();
        assert_eq!(old_content, "before rotation\n");

        // The original path has the new content.
        let new_content = std::fs::read_to_string(&log_path).unwrap();
        assert_eq!(new_content, "after rotation\n");
    }

    #[test]
    fn security_filter_passes_security_events() {
        let count = Arc::new(Mutex::new(0u32));
        let layer = CountingLayer {
            count: count.clone(),
        };
        let filtered = layer.with_filter(SecurityEventFilter);
        let subscriber = tracing_subscriber::registry().with(filtered);

        tracing::subscriber::with_default(subscriber, || {
            tracing::warn!(security_event = true, "this is a security event");
        });

        assert_eq!(*count.lock().unwrap(), 1);
    }

    #[test]
    fn security_filter_blocks_non_security_events() {
        let count = Arc::new(Mutex::new(0u32));
        let layer = CountingLayer {
            count: count.clone(),
        };
        let filtered = layer.with_filter(SecurityEventFilter);
        let subscriber = tracing_subscriber::registry().with(filtered);

        tracing::subscriber::with_default(subscriber, || {
            tracing::warn!("this is a normal warning");
            tracing::info!("this is info");
            tracing::error!("this is an error without security_event");
        });

        assert_eq!(*count.lock().unwrap(), 0);
    }

    #[test]
    fn security_filter_blocks_info_level_even_with_field() {
        let count = Arc::new(Mutex::new(0u32));
        let layer = CountingLayer {
            count: count.clone(),
        };
        let filtered = layer.with_filter(SecurityEventFilter);
        let subscriber = tracing_subscriber::registry().with(filtered);

        tracing::subscriber::with_default(subscriber, || {
            // INFO is below WARN — should be blocked by the level gate.
            tracing::info!(security_event = true, "info with security field");
        });

        assert_eq!(*count.lock().unwrap(), 0);
    }

    #[test]
    fn security_filter_blocks_security_event_false() {
        let count = Arc::new(Mutex::new(0u32));
        let layer = CountingLayer {
            count: count.clone(),
        };
        let filtered = layer.with_filter(SecurityEventFilter);
        let subscriber = tracing_subscriber::registry().with(filtered);

        tracing::subscriber::with_default(subscriber, || {
            tracing::warn!(security_event = false, "security_event is false");
        });

        assert_eq!(*count.lock().unwrap(), 0);
    }
}
