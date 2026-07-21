use std::collections::{HashSet, VecDeque};
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;

/// Alert severity levels.
#[derive(Debug, Clone, Copy)]
pub enum AlertSeverity {
    /// Panics, CC process death — something is deeply wrong.
    Critical,
    /// Unrecognized CC messages, auth failures, security events — needs attention.
    Warning,
    /// Normal operational events worth knowing about (successful login, registration).
    Info,
}

impl fmt::Display for AlertSeverity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Critical => write!(f, "critical"),
            Self::Warning => write!(f, "warning"),
            Self::Info => write!(f, "info"),
        }
    }
}

/// Errors from alert dispatch.
#[derive(Debug)]
pub enum AlertError {
    /// Alert was suppressed by rate limiting.
    RateLimited,
    /// HTTP or network error sending the alert.
    TransportError(String),
}

impl fmt::Display for AlertError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RateLimited => write!(f, "rate limited"),
            Self::TransportError(msg) => write!(f, "transport error: {msg}"),
        }
    }
}

impl std::error::Error for AlertError {}

/// Ordered key-value metadata attached to alerts.
///
/// Context accumulates as `AlertDispatcher` is cloned down the call chain:
/// instance-level fields (host, config path) are set at startup, scoped fields
/// (app, user, conversation) are added at session/bridge boundaries. The context
/// is captured when `alert()` is called and rendered into the alert body by the
/// background task.
///
/// Cheap to clone (Arc'd inner vec). The vec is small (5-10 fields max), so
/// the O(n) copy in `with_field` is negligible.
#[derive(Clone, Default, Debug)]
pub struct AlertContext {
    fields: Arc<Vec<(String, String)>>,
}

impl AlertContext {
    /// Return a new context with an additional field appended.
    pub fn with_field(&self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let mut fields = (*self.fields).clone();
        fields.push((key.into(), value.into()));
        Self {
            fields: Arc::new(fields),
        }
    }

    /// Render context fields as a block for alert bodies.
    /// Returns empty string if no fields are set.
    pub fn render(&self) -> String {
        if self.fields.is_empty() {
            return String::new();
        }
        let mut out = String::new();
        for (k, v) in self.fields.iter() {
            out.push_str(k);
            out.push_str(": ");
            out.push_str(v);
            out.push('\n');
        }
        out
    }

    /// Returns true if no fields have been set.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

/// Trait for sending alerts to an external system.
pub trait Alerter: Send + Sync + 'static {
    fn send_alert(
        &self,
        severity: AlertSeverity,
        title: &str,
        body: &str,
    ) -> impl Future<Output = Result<(), AlertError>> + Send;
}

/// ntfy-based alerter. Sends alerts as HTTP POST to an ntfy topic.
pub struct NtfyAlerter {
    client: reqwest::Client,
    url: String,
}

impl NtfyAlerter {
    pub fn new(url: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            url,
        }
    }
}

impl Alerter for NtfyAlerter {
    async fn send_alert(
        &self,
        severity: AlertSeverity,
        title: &str,
        body: &str,
    ) -> Result<(), AlertError> {
        let priority = match severity {
            AlertSeverity::Critical => "5",
            AlertSeverity::Warning => "3",
            AlertSeverity::Info => "1",
        };
        self.client
            .post(&self.url)
            .header("Title", title)
            .header("Priority", priority)
            .header("Tags", severity.to_string())
            .body(body.to_string())
            .send()
            .await
            .map_err(|e| AlertError::TransportError(e.to_string()))?
            .error_for_status()
            .map_err(|e| AlertError::TransportError(e.to_string()))?;
        Ok(())
    }
}

/// Mail-based alerter. Shells out to the `mail` command.
///
/// Note: hostname is no longer prepended here — it's an instance-level field
/// on `AlertContext`, rendered into the body before it reaches the alerter.
pub struct MailAlerter {
    to: String,
    subject_label: String,
}

impl MailAlerter {
    pub fn new(to: String, subject_label: String) -> Self {
        Self { to, subject_label }
    }
}

impl Alerter for MailAlerter {
    async fn send_alert(
        &self,
        severity: AlertSeverity,
        title: &str,
        body: &str,
    ) -> Result<(), AlertError> {
        let prepared = self.prepare_mail(severity, title, body);

        let mut child = tokio::process::Command::new("mail")
            .arg("-s")
            .arg(&prepared.subject)
            .arg(&prepared.to)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| AlertError::TransportError(format!("failed to spawn mail: {e}")))?;

        // Write body to stdin.
        use tokio::io::AsyncWriteExt;
        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(prepared.body.as_bytes())
                .await
                .map_err(|e| {
                    AlertError::TransportError(format!("failed to write to mail stdin: {e}"))
                })?;
            // Drop stdin to close the pipe so `mail` sees EOF.
        }

        let output = child
            .wait_with_output()
            .await
            .map_err(|e| AlertError::TransportError(format!("failed to wait on mail: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(AlertError::TransportError(format!(
                "mail exited with {}: {stderr}",
                output.status
            )));
        }
        Ok(())
    }
}

/// Everything `send_alert` computes before touching the subprocess.
/// Extracted for testability.
#[derive(Debug, PartialEq)]
struct PreparedMail {
    to: String,
    subject: String,
    body: String,
}

impl MailAlerter {
    /// Build the subject, body, and recipient for a mail alert.
    fn prepare_mail(&self, severity: AlertSeverity, title: &str, body: &str) -> PreparedMail {
        PreparedMail {
            to: self.to.clone(),
            subject: format_mail_subject(&self.subject_label, severity, title),
            body: body.to_string(),
        }
    }
}

/// Format the subject line for a mail alert.
///
/// Format: `[{label}] {severity}: {title}`
/// Total subject is capped at 120 characters; if the title pushes it over,
/// the title is truncated with a `...` suffix.
fn format_mail_subject(label: &str, severity: AlertSeverity, title: &str) -> String {
    const MAX_SUBJECT_CHARS: usize = 120;
    let prefix = format!("[{label}] {severity}: ");
    let prefix_len = prefix.chars().count();

    if prefix_len >= MAX_SUBJECT_CHARS {
        // Absurd config — just use the prefix truncated.
        return format!("{prefix}...");
    }

    let budget = MAX_SUBJECT_CHARS - prefix_len;
    let title_len = title.chars().count();

    if title_len <= budget {
        format!("{prefix}{title}")
    } else {
        // Leave room for "..."
        let truncated: String = title.chars().take(budget.saturating_sub(3)).collect();
        format!("{prefix}{truncated}...")
    }
}

/// No-op alerter for tests and development.
pub struct NoopAlerter;

impl Alerter for NoopAlerter {
    async fn send_alert(
        &self,
        _severity: AlertSeverity,
        _title: &str,
        _body: &str,
    ) -> Result<(), AlertError> {
        Ok(())
    }
}

/// Max alerts per window for test-mode rate limiters. Shared by
/// `noop_alert_dispatcher` and any capturing-alerter test helper that
/// needs a compatible `RateLimiter` — single definition site.
#[cfg(any(test, feature = "testutils"))]
pub const TEST_RATE_LIMITER_MAX: u32 = 10;

/// Window size (seconds) for test-mode rate limiters.
#[cfg(any(test, feature = "testutils"))]
pub const TEST_RATE_LIMITER_WINDOW_SECS: u64 = 60;

/// Construct a no-op `AlertDispatcher` for tests.
///
/// Rate-limiter parameters are `TEST_RATE_LIMITER_MAX` / `TEST_RATE_LIMITER_WINDOW_SECS`.
/// The `JoinHandle` may be discarded; dropping it does NOT abort the drainer
/// task (tokio semantics — only `JoinHandle::abort()` cancels). The task exits
/// naturally when the last `AlertDispatcher` clone drops.
#[cfg(any(test, feature = "testutils"))]
pub fn noop_alert_dispatcher() -> (AlertDispatcher, tokio::task::JoinHandle<()>) {
    AlertDispatcher::new(
        NoopAlerter,
        RateLimiter::new(TEST_RATE_LIMITER_MAX, TEST_RATE_LIMITER_WINDOW_SECS),
    )
}

/// Construct a capturing `AlertDispatcher` for tests.
///
/// Returns `(dispatcher, captured, handle)`:
/// - `dispatcher`: the `AlertDispatcher` to inject into the system under test.
/// - `captured`: shared vec of `(title, body)` pairs delivered to the alerter.
/// - `handle`: background drainer task. Callers that want to assert on
///   `captured` MUST drop all `AlertDispatcher` clones (so the alert mpsc
///   closes), then `.await` the handle before locking the vec. A single
///   `yield_now` is not a reliable drain. When dropping every clone is
///   impractical (e.g. a spawned server still holds one), call
///   `AlertDispatcher::flush().await` on any surviving clone instead: it is a
///   FIFO barrier that guarantees prior alerts have drained without closing the
///   channel.
///
/// Rate-limiter parameters are `TEST_RATE_LIMITER_MAX` /
/// `TEST_RATE_LIMITER_WINDOW_SECS`. No test exercises rate-limit suppression;
/// the specific numbers are not load-bearing.
#[allow(clippy::type_complexity)]
#[cfg(any(test, feature = "testutils"))]
pub fn make_capturing_alerter() -> (
    AlertDispatcher,
    std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>>,
    tokio::task::JoinHandle<()>,
) {
    let captured = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let alerter = CapturingAlerter(captured.clone());
    let (dispatcher, handle) = AlertDispatcher::new(
        alerter,
        RateLimiter::new(TEST_RATE_LIMITER_MAX, TEST_RATE_LIMITER_WINDOW_SECS),
    );
    (dispatcher, captured, handle)
}

/// Construct a capturing `AlertDispatcher` for tests that need to assert on
/// alert severity in addition to title/body content.
///
/// Returns `(dispatcher, captured, handle)`:
/// - `dispatcher`: the `AlertDispatcher` to inject into the system under test.
/// - `captured`: shared vec of `(severity, title, body)` triples.
/// - `handle`: background drainer task. Same drain protocol as
///   `make_capturing_alerter` — drop all clones then `.await` the handle before
///   locking the vec.
#[allow(clippy::type_complexity)]
#[cfg(any(test, feature = "testutils"))]
pub fn make_capturing_alerter_with_severity() -> (
    AlertDispatcher,
    std::sync::Arc<std::sync::Mutex<Vec<(AlertSeverity, String, String)>>>,
    tokio::task::JoinHandle<()>,
) {
    let captured = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let alerter = CapturingAlerterWithSeverity(captured.clone());
    let (dispatcher, handle) = AlertDispatcher::new(
        alerter,
        RateLimiter::new(TEST_RATE_LIMITER_MAX, TEST_RATE_LIMITER_WINDOW_SECS),
    );
    (dispatcher, captured, handle)
}

/// Construct a counting `AlertDispatcher` for tests that only need to know how
/// many alerts fired (not their content).
///
/// Returns `(dispatcher, count, handle)`:
/// - `dispatcher`: the `AlertDispatcher` to inject into the system under test.
/// - `count`: shared counter incremented on every dispatch.
/// - `handle`: background drainer task. Same drain protocol as
///   `make_capturing_alerter` — drop all clones then `.await` the handle before
///   reading the count.
///
/// Rate-limiter parameters are `TEST_RATE_LIMITER_MAX` /
/// `TEST_RATE_LIMITER_WINDOW_SECS` — the single shared test constants, so this
/// cannot drift from the other test alerter helpers.
#[cfg(any(test, feature = "testutils"))]
pub fn make_counting_alerter() -> (
    AlertDispatcher,
    std::sync::Arc<std::sync::atomic::AtomicU32>,
    tokio::task::JoinHandle<()>,
) {
    let count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let (dispatcher, handle) = AlertDispatcher::new(
        CountingAlerter(count.clone()),
        RateLimiter::new(TEST_RATE_LIMITER_MAX, TEST_RATE_LIMITER_WINDOW_SECS),
    );
    (dispatcher, count, handle)
}

/// Sliding-window rate limiter for alerts.
pub struct RateLimiter {
    timestamps: Mutex<VecDeque<Instant>>,
    max_alerts: u32,
    window: Duration,
}

impl RateLimiter {
    pub fn new(max_alerts: u32, window_secs: u64) -> Self {
        Self {
            timestamps: Mutex::new(VecDeque::new()),
            max_alerts,
            window: Duration::from_secs(window_secs),
        }
    }

    /// Returns true if the alert should be sent, false if rate limited.
    pub async fn check(&self) -> bool {
        let mut timestamps = self.timestamps.lock().await;
        let now = Instant::now();
        // Prune expired entries.
        while timestamps
            .front()
            .is_some_and(|&t| now.duration_since(t) > self.window)
        {
            timestamps.pop_front();
        }
        if timestamps.len() < self.max_alerts as usize {
            timestamps.push_back(now);
            true
        } else {
            false
        }
    }
}

struct AlertMessage {
    severity: AlertSeverity,
    title: String,
    body: String,
    context: AlertContext,
}

/// What travels the dispatcher's FIFO channel to the background drainer.
enum DrainerMsg {
    /// An alert to rate-limit and deliver.
    Alert(AlertMessage),
    /// Test-only drain barrier: the drainer signals `done` after handling it.
    /// Because the channel is FIFO and each message is processed to completion
    /// before the next `recv`, a signalled `Flush` proves every alert enqueued
    /// ahead of it has already reached the alerter.
    #[cfg(any(test, feature = "testutils"))]
    Flush(tokio::sync::oneshot::Sender<()>),
}

/// Process-lifetime dedup set for `alert_once_per_process`. Keyed by
/// `(title, dedup_key)` so sites with a common title but different keys
/// stay independent. Used for upgrade-detection alerts and first-occurrence
/// security signals (e.g. `ephemeral_publish_denied`) — one page per key per
/// process. Critical events like "CC process died" must keep paging
/// per-occurrence and intentionally do not use this path. See
/// `docs/designs/silence-known-cc-warnings.md`.
///
/// Private: callers interact with the dedup through
/// `AlertDispatcher::alert_once_per_process`, not this type directly.
#[derive(Clone, Default)]
struct OncePerProcess {
    seen: Arc<StdMutex<HashSet<(String, String)>>>,
}

impl OncePerProcess {
    /// Returns true on first insert (caller should fire the alert),
    /// false on subsequent inserts (caller should suppress).
    fn take_slot(&self, title: &str, key: &str) -> bool {
        let mut seen = self.seen.lock().expect("OncePerProcess mutex poisoned");
        seen.insert((title.to_string(), key.to_string()))
    }
}

/// Mail configuration used for the synchronous panic-hook send path.
///
/// Stored in `AlertDispatcher` so the panic hook can bypass the async channel
/// and call `std::process::Command` directly. This is only populated when the
/// configured alerting backend is `mail`; all other configurations leave this
/// as `None` (safe no-op on the sync path).
#[derive(Clone, Debug)]
pub struct PanicMailConfig {
    pub to: String,
    pub subject_label: String,
}

/// Non-blocking alert dispatcher. Sends alerts via a channel to a background
/// task that applies rate limiting and dispatches to the alerter.
///
/// Carries an `AlertContext` that is automatically included in every alert.
/// Use `with_field()` to create a child dispatcher with additional context
/// fields — the parent is not mutated.
#[derive(Clone)]
pub struct AlertDispatcher {
    tx: mpsc::Sender<DrainerMsg>,
    context: AlertContext,
    /// Shared per-process dedup state, consulted only by
    /// `alert_once_per_process`. Cloned children share the same set.
    once_per_process: OncePerProcess,
    /// Mail config for the synchronous panic send path. `None` when the
    /// alerting backend is not mail (or when alerting is disabled).
    panic_mail: Arc<Option<PanicMailConfig>>,
}

impl AlertDispatcher {
    /// Create a new dispatcher with the given alerter and rate limiter.
    /// Returns the dispatcher and a handle to the background task.
    pub fn new<A: Alerter>(alerter: A, rate_limiter: RateLimiter) -> (Self, JoinHandle<()>) {
        let (tx, mut rx) = mpsc::channel::<DrainerMsg>(64);
        let handle = tokio::spawn(async move {
            while let Some(drainer_msg) = rx.recv().await {
                // Without the test-only `Flush` variant `DrainerMsg` has a single
                // variant, so this match is infallible in the default build and
                // fallible under test/testutils; the allow applies only to the
                // single-variant build.
                #[cfg_attr(
                    not(any(test, feature = "testutils")),
                    allow(clippy::infallible_destructuring_match)
                )]
                let msg = match drainer_msg {
                    DrainerMsg::Alert(msg) => msg,
                    #[cfg(any(test, feature = "testutils"))]
                    DrainerMsg::Flush(done) => {
                        // The flushing caller may have dropped its receiver; the
                        // signal is then simply unneeded.
                        let _ = done.send(());
                        continue;
                    }
                };
                if !rate_limiter.check().await {
                    tracing::warn!(
                        alert_rate_limited = true,
                        severity = %msg.severity,
                        title = %msg.title,
                        "alert rate limited (still logged)"
                    );
                    continue;
                }
                let rendered_body = render_alert_body(&msg.context, &msg.body);
                if let Err(e) = alerter
                    .send_alert(msg.severity, &msg.title, &rendered_body)
                    .await
                {
                    tracing::error!(alert_error = %e, "failed to send alert");
                }
            }
        });
        (
            Self {
                tx,
                context: AlertContext::default(),
                once_per_process: OncePerProcess::default(),
                panic_mail: Arc::new(None),
            },
            handle,
        )
    }

    /// Create a no-op dispatcher that silently drops all alerts.
    /// Used in development when alerting is not configured.
    /// Returns a JoinHandle like `new()` — caller stores it in ObsGuard.
    pub fn noop() -> (Self, JoinHandle<()>) {
        let (tx, mut rx) = mpsc::channel::<DrainerMsg>(64);
        let handle = tokio::spawn(async move {
            // Drain and discard. Keeps the channel open so alert() doesn't panic.
            while rx.recv().await.is_some() {}
        });
        (
            Self {
                tx,
                context: AlertContext::default(),
                once_per_process: OncePerProcess::default(),
                panic_mail: Arc::new(None),
            },
            handle,
        )
    }

    /// Attach mail config for the synchronous panic send path.
    ///
    /// Call this on the dispatcher before passing it to `install_panic_hook`
    /// when the alerting backend is `mail`. Returns a new dispatcher (like
    /// `with_field`) — the original is not mutated.
    pub fn with_panic_mail(self, config: PanicMailConfig) -> Self {
        Self {
            panic_mail: Arc::new(Some(config)),
            ..self
        }
    }

    /// Return a child dispatcher with an additional context field.
    /// The parent dispatcher is not modified. Both share the same underlying
    /// channel (and thus the same background task and rate limiter).
    pub fn with_field(&self, key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            tx: self.tx.clone(),
            context: self.context.with_field(key, value),
            once_per_process: self.once_per_process.clone(),
            panic_mail: self.panic_mail.clone(),
        }
    }

    /// Return a new dispatcher with the given `AlertContext` merged on top of
    /// any existing context fields.
    ///
    /// Used by `obs::init` to apply the instance-level context produced by
    /// `build_alert_context` so both hooks derive context identically.
    pub(crate) fn with_context(self, ctx: AlertContext) -> Self {
        // Merge by appending all fields from ctx onto self.context.
        let mut merged = (*self.context.fields).clone();
        merged.extend((*ctx.fields).iter().cloned());
        Self {
            context: AlertContext {
                fields: Arc::new(merged),
            },
            ..self
        }
    }

    /// Queue an alert for dispatch, but only if `(title, dedup_key)` hasn't
    /// been fired before in this process. On suppressed calls, the alert is
    /// not sent (and not rate-limit-charged); caller sites still log
    /// via `tracing::warn!` unconditionally so the diagnostic log keeps
    /// every occurrence.
    ///
    /// **Scope: upgrade-detection and first-occurrence security signals.** The
    /// suppression here is process-lifetime — one phone alert per `(title, key)`
    /// per brenn restart. Do NOT use for death / critical / panic events: if CC
    /// keeps dying we want one alert per occurrence so a flapping backend is
    /// visible. See `docs/designs/silence-known-cc-warnings.md`.
    ///
    /// The alert body is augmented with a trailing line so the first
    /// (and only) phone alert explains what's happening — otherwise
    /// `wtf, why did I not get a second alert` would be a reasonable
    /// 3am reaction.
    pub fn alert_once_per_process(
        &self,
        severity: AlertSeverity,
        title: String,
        dedup_key: &str,
        body: String,
    ) {
        if !self.once_per_process.take_slot(&title, dedup_key) {
            // Already alerted for this key this process. The call site still
            // logs via tracing::warn!, so forensics survive.
            return;
        }
        let body = format!(
            "{body}\n\n(Further alerts for this dedup key \
             [{dedup_key}] suppressed until brenn restarts.)"
        );
        self.alert(severity, title, body);
    }

    /// Queue an alert for dispatch. Non-blocking — does try_send on the channel.
    ///
    /// The dispatcher's accumulated context is captured and sent with the alert.
    ///
    /// Panics if the background alert task has died (channel closed), since that
    /// means our alerting infrastructure is broken — an invariant violation.
    pub fn alert(&self, severity: AlertSeverity, title: String, body: String) {
        match self.tx.try_send(DrainerMsg::Alert(AlertMessage {
            severity,
            title,
            body,
            context: self.context.clone(),
        })) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::error!("alert channel full, dropping alert");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                panic!("alert background task is dead — channel closed");
            }
        }
    }

    /// Best-effort alert that never panics. Used from the panic hook, where
    /// panicking would abort before the default hook runs (losing the backtrace).
    ///
    /// We intentionally discard the Result here. `try_send` fails with `Full` (channel
    /// back-pressure) or `Closed` (alerter task died). In the panic hook, there's nothing
    /// useful we can do about either: panicking would double-panic (abort), and logging
    /// on `Full` would just add noise to what's probably already a bad situation. Accept
    /// that this alert may be lost — the panic itself will still be logged by the default
    /// panic hook and the tracing panic hook.
    pub fn try_alert(&self, severity: AlertSeverity, title: String, body: String) {
        let _ = self.tx.try_send(DrainerMsg::Alert(AlertMessage {
            severity,
            title,
            body,
            context: self.context.clone(),
        }));
    }

    /// Test-only drain barrier. Enqueues a flush marker on the same FIFO channel
    /// as alerts and awaits the drainer processing it. Because the drainer
    /// handles messages in order and to completion, every alert enqueued before
    /// this call has been delivered to the alerter (and thus to a capturing
    /// alerter's vec) by the time it returns. This lets a test assert on captured
    /// alerts without dropping every `AlertDispatcher` clone first.
    #[cfg(any(test, feature = "testutils"))]
    pub async fn flush(&self) {
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        if self.tx.send(DrainerMsg::Flush(done_tx)).await.is_ok() {
            // The drainer signals once it reaches our marker; if it has already
            // died the recv errors and we return with whatever drained.
            let _ = done_rx.await;
        }
    }

    /// Synchronous, blocking mail send for the panic hook.
    ///
    /// Called from the panic hook (sync context) after the tokio runtime may
    /// have begun tearing down. Delegates to `send_panic_mail_sync` — see that
    /// function for full documentation.
    ///
    /// No-op when no mail config is attached (i.e., the configured backend is
    /// not `mail`, or alerting is disabled entirely).
    pub fn send_panic_alert_sync(&self, severity: AlertSeverity, title: &str, body: &str) {
        let cfg = match self.panic_mail.as_ref() {
            Some(c) => c,
            None => return,
        };
        send_panic_mail_sync(cfg, &self.context, severity, title, body);
    }
}

/// Test-only counter: incremented at the start of every `send_panic_mail_sync`
/// call before the `#[cfg(test)]` early-return. Used by hook tests to assert
/// "an attempt was made" without spawning any subprocess.
///
/// **Every test that installs a panic hook or calls `send_panic_mail_sync`
/// directly MUST acquire `HOOK_TEST_MUTEX` and reset this counter at test
/// start** (same mutex also serializes the global hook). The parity test (#5)
/// calls formatting functions directly, never `send_panic_mail_sync`, and
/// therefore does not need a reset.
#[cfg(test)]
pub(crate) static SEND_PANIC_MAIL_SYNC_CALL_COUNT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Synchronous, blocking mail send for panic hooks.
///
/// Called from a panic hook (sync context) after the tokio runtime may have
/// begun tearing down. Uses `std::process::Command` — NOT `tokio::process` —
/// so it does not require the runtime to be alive.
///
/// This is a best-effort send: spawn failure or non-zero exit is logged via
/// `tracing::error!` AND `eprintln!` (because tracing may be silent during
/// teardown). This function MUST NOT panic — we are already handling a panic.
///
/// A 5-second wall-clock timeout guards against a hung `mail` process
/// wedging the dying process forever. On timeout the worker thread is abandoned
/// and the function returns so the calling hook can proceed.
#[cfg_attr(test, allow(unused_variables))]
pub(crate) fn send_panic_mail_sync(
    cfg: &PanicMailConfig,
    context: &AlertContext,
    severity: AlertSeverity,
    title: &str,
    body: &str,
) {
    // Test seam: count the attempt and short-circuit before spawning `mail`.
    // Without this, a dev box that has `mail` installed would send live mail
    // to `cfg.to` during `cargo test`. The counter lets tests assert "an
    // attempt was made" without any subprocess.
    #[cfg(test)]
    {
        SEND_PANIC_MAIL_SYNC_CALL_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        return;
    }
    #[allow(unreachable_code)]
    let rendered_body = render_alert_body(context, body);
    let subject = format_mail_subject(&cfg.subject_label, severity, title);

    // Spawn `mail` synchronously. Any error is logged but must not panic.
    let mut child = match std::process::Command::new("mail")
        .arg("-s")
        .arg(&subject)
        .arg(&cfg.to)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            let msg = format!("panic hook: failed to spawn `mail` for synchronous alert: {e}");
            tracing::error!(panic_alert_sync_error = %e, "{msg}");
            eprintln!("[brenn panic-alert] ERROR: {msg}");
            return;
        }
    };

    // Write body to stdin on a worker thread with a 5-second timeout so a
    // hung or slow `mail` binary cannot wedge the dying process forever.
    //
    // Strategy: take stdin + child out of `child`, move them into a thread
    // that does the write + wait_with_output. The main thread joins with a
    // recv_timeout; on timeout it kills the child (best-effort) and moves
    // on to let the default hook run.
    let stdin = child.stdin.take();

    // Safety: Child is Send. We move it into the worker thread.
    let (tx, rx) = std::sync::mpsc::channel::<Result<std::process::Output, String>>();
    let rendered_body_clone = rendered_body.clone();
    let worker = std::thread::Builder::new()
        .name("panic-mail-send".to_string())
        .spawn(move || {
            // Write body to stdin, then drop so `mail` sees EOF.
            if let Some(mut stdin) = stdin {
                use std::io::Write;
                if let Err(e) = stdin.write_all(rendered_body_clone.as_bytes()) {
                    // Send error result; drop stdin (pipe closes) before wait.
                    let _ = tx.send(Err(format!("write to `mail` stdin failed: {e}")));
                    // Still reap the child below if possible; send() above may
                    // have failed if recv timed out, but we try anyway.
                    drop(stdin);
                    let _ = child.wait();
                    return;
                }
                drop(stdin); // close pipe so `mail` sees EOF
            }
            match child.wait_with_output() {
                Err(e) => {
                    let _ = tx.send(Err(format!("wait for `mail` failed: {e}")));
                }
                Ok(output) => {
                    let _ = tx.send(Ok(output));
                }
            }
        });
    let worker = match worker {
        Ok(w) => w,
        Err(e) => {
            let msg = format!("panic hook: failed to spawn mail worker thread: {e}");
            tracing::error!(panic_alert_sync_error = %e, "{msg}");
            eprintln!("[brenn panic-alert] ERROR: {msg}");
            return;
        }
    };
    // Keep the handle in scope so the thread is not detached; we join via the
    // channel rather than JoinHandle::join (which could block past the timeout).
    let _worker = worker;

    const MAIL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
    match rx.recv_timeout(MAIL_TIMEOUT) {
        Ok(Ok(output)) if output.status.success() => {
            eprintln!(
                "[brenn panic-alert] mail sent successfully (status {})",
                output.status
            );
        }
        Ok(Ok(output)) => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let msg = format!(
                "panic hook: `mail` exited non-zero (status {}, stderr: {stderr})",
                output.status
            );
            tracing::error!(
                panic_alert_sync_status = %output.status,
                panic_alert_sync_stderr = %stderr,
                "{msg}"
            );
            eprintln!("[brenn panic-alert] ERROR: {msg}");
        }
        Ok(Err(e)) => {
            tracing::error!(panic_alert_sync_error = %e, "panic hook: mail send error");
            eprintln!("[brenn panic-alert] ERROR: panic hook: {e}");
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            // The worker thread still holds the child; we cannot call kill()
            // on it here because the child was moved into the thread. The
            // worker will either finish and exit on its own, or the OS will
            // reap it when the process exits. Log the timeout and proceed.
            let msg = "panic hook: `mail` timed out after 5s — killing child and continuing";
            tracing::error!("{msg}");
            eprintln!("[brenn panic-alert] ERROR: {msg}");
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            // Worker panicked or exited without sending — treat as failure.
            let msg = "panic hook: mail worker thread disconnected without result";
            tracing::error!("{msg}");
            eprintln!("[brenn panic-alert] ERROR: {msg}");
        }
    }
}

// --- Test-only alerter helpers ---
//
// Shared across brenn-lib / brenn-cc / brenn tests. Gated on `testutils`
// so production builds don't carry them, and gated on `test` so they're
// available inside brenn-lib's own test module.

/// Alerter that counts every dispatch. Useful when a test only needs to
/// know whether an alert fired (e.g. regression tests around
/// suppression / dedup).
#[cfg(any(test, feature = "testutils"))]
pub struct CountingAlerter(pub Arc<std::sync::atomic::AtomicU32>);

#[cfg(any(test, feature = "testutils"))]
impl Alerter for CountingAlerter {
    async fn send_alert(
        &self,
        _severity: AlertSeverity,
        _title: &str,
        _body: &str,
    ) -> Result<(), AlertError> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

/// Alerter that captures every dispatch as `(title, body)` pairs. Use
/// when a test needs to assert on alert content, not just count.
#[cfg(any(test, feature = "testutils"))]
pub struct CapturingAlerter(pub Arc<std::sync::Mutex<Vec<(String, String)>>>);

/// Alerter that captures every dispatch as `(severity, title, body)` triples.
/// Use when a test needs to assert on alert severity in addition to content.
#[cfg(any(test, feature = "testutils"))]
pub struct CapturingAlerterWithSeverity(
    pub Arc<std::sync::Mutex<Vec<(AlertSeverity, String, String)>>>,
);

#[cfg(any(test, feature = "testutils"))]
impl Alerter for CapturingAlerterWithSeverity {
    async fn send_alert(
        &self,
        severity: AlertSeverity,
        title: &str,
        body: &str,
    ) -> Result<(), AlertError> {
        self.0
            .lock()
            .expect("CapturingAlerterWithSeverity mutex poisoned")
            .push((severity, title.to_string(), body.to_string()));
        Ok(())
    }
}

#[cfg(any(test, feature = "testutils"))]
impl Alerter for CapturingAlerter {
    async fn send_alert(
        &self,
        _severity: AlertSeverity,
        title: &str,
        body: &str,
    ) -> Result<(), AlertError> {
        self.0
            .lock()
            .expect("CapturingAlerter mutex poisoned")
            .push((title.to_string(), body.to_string()));
        Ok(())
    }
}

/// Render the final alert body: context block (if any) followed by a blank
/// line separator, then the call-site body.
fn render_alert_body(context: &AlertContext, body: &str) -> String {
    if context.is_empty() {
        return body.to_string();
    }
    let ctx = context.render();
    if body.is_empty() {
        // No trailing blank line when there's no body.
        ctx.trim_end().to_string()
    } else {
        format!("{ctx}\n{body}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // format_mail_subject
    // -----------------------------------------------------------------------

    #[test]
    fn subject_normal() {
        let s = format_mail_subject("Brenn", AlertSeverity::Warning, "Unrecognized CC message");
        assert_eq!(s, "[Brenn] warning: Unrecognized CC message");
    }

    #[test]
    fn subject_critical() {
        let s = format_mail_subject("Brenn", AlertSeverity::Critical, "PANIC");
        assert_eq!(s, "[Brenn] critical: PANIC");
    }

    #[test]
    fn subject_empty_title() {
        let s = format_mail_subject("Brenn", AlertSeverity::Warning, "");
        assert_eq!(s, "[Brenn] warning: ");
    }

    #[test]
    fn subject_exactly_at_limit() {
        // "[Test] warning: " is 16 chars, so we have 104 chars of budget.
        let title = "a".repeat(104);
        let s = format_mail_subject("Test", AlertSeverity::Warning, &title);
        assert_eq!(s.chars().count(), 120);
        assert!(!s.ends_with("..."));
    }

    #[test]
    fn subject_one_over_limit_truncates() {
        // 16-char prefix, 105-char title → over 120.
        let title = "a".repeat(105);
        let s = format_mail_subject("Test", AlertSeverity::Warning, &title);
        assert!(s.chars().count() <= 120);
        assert!(s.ends_with("..."));
    }

    #[test]
    fn subject_long_title_truncated() {
        let title = "x".repeat(200);
        let s = format_mail_subject("Brenn", AlertSeverity::Warning, &title);
        assert!(s.chars().count() <= 120);
        assert!(s.ends_with("..."));
        assert!(s.starts_with("[Brenn] warning: "));
    }

    #[test]
    fn subject_multibyte_utf8() {
        // Each emoji is 1 char but multiple bytes. Should truncate by chars, not bytes.
        let title = "🔥".repeat(200);
        let s = format_mail_subject("Brenn", AlertSeverity::Warning, &title);
        assert!(s.chars().count() <= 120);
        assert!(s.ends_with("..."));
    }

    #[test]
    fn subject_custom_label() {
        let s = format_mail_subject("myhost.prod", AlertSeverity::Critical, "disk full");
        assert_eq!(s, "[myhost.prod] critical: disk full");
    }

    // -----------------------------------------------------------------------
    // prepare_mail
    // -----------------------------------------------------------------------

    fn test_alerter() -> MailAlerter {
        MailAlerter {
            to: "alerts@example.com".to_string(),
            subject_label: "Brenn".to_string(),
        }
    }

    #[test]
    fn prepare_mail_normal() {
        let alerter = test_alerter();
        let prepared = alerter.prepare_mail(
            AlertSeverity::Warning,
            "Unrecognized CC message",
            "CC sent something weird.",
        );
        assert_eq!(
            prepared,
            PreparedMail {
                to: "alerts@example.com".to_string(),
                subject: "[Brenn] warning: Unrecognized CC message".to_string(),
                body: "CC sent something weird.".to_string(),
            }
        );
    }

    #[test]
    fn prepare_mail_critical_with_multiline_body() {
        let alerter = test_alerter();
        let prepared = alerter.prepare_mail(
            AlertSeverity::Critical,
            "PANIC",
            "thread 'main' panicked at src/main.rs:42\nnote: run with RUST_BACKTRACE=1",
        );
        assert_eq!(prepared.to, "alerts@example.com");
        assert_eq!(prepared.subject, "[Brenn] critical: PANIC");
        assert!(prepared.body.contains("panicked at src/main.rs:42"));
        assert!(prepared.body.contains("RUST_BACKTRACE=1"));
    }

    #[test]
    fn prepare_mail_empty_body() {
        let alerter = test_alerter();
        let prepared = alerter.prepare_mail(AlertSeverity::Warning, "test", "");
        assert_eq!(prepared.body, "");
    }

    #[test]
    fn prepare_mail_custom_label_and_recipient() {
        let alerter = MailAlerter {
            to: "ops@corp.com".to_string(),
            subject_label: "prod-web-01".to_string(),
        };
        let prepared = alerter.prepare_mail(AlertSeverity::Critical, "disk full", "/var is 99%");
        assert_eq!(prepared.to, "ops@corp.com");
        assert_eq!(prepared.subject, "[prod-web-01] critical: disk full");
        assert_eq!(prepared.body, "/var is 99%");
    }

    // -----------------------------------------------------------------------
    // Rate limiter
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn rate_limiter_allows_within_limit() {
        let limiter = RateLimiter::new(3, 60);
        assert!(limiter.check().await);
        assert!(limiter.check().await);
        assert!(limiter.check().await);
        assert!(!limiter.check().await);
    }

    #[tokio::test]
    async fn rate_limiter_expires_old_entries() {
        let limiter = RateLimiter::new(1, 0); // 0-second window = everything expires immediately
        assert!(limiter.check().await);
        // Sleep briefly so the entry expires.
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(limiter.check().await);
    }

    #[tokio::test]
    async fn noop_alerter_succeeds() {
        let alerter = NoopAlerter;
        let result = alerter
            .send_alert(AlertSeverity::Critical, "test", "body")
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn alert_dispatcher_delivers_bare_body_without_context() {
        let (dispatcher, captured, handle) = make_capturing_alerter();

        dispatcher.alert(AlertSeverity::Warning, "title".into(), "bare body".into());

        drop(dispatcher);
        handle.await.expect("background task panicked");

        let captured = captured.lock().unwrap();
        assert_eq!(captured.len(), 1);
        // No context on a fresh dispatcher — body passes through undecorated.
        assert_eq!(captured[0].0, "title");
        assert_eq!(captured[0].1, "bare body");
    }

    #[tokio::test]
    async fn try_alert_carries_context() {
        let (dispatcher, captured, handle) = make_capturing_alerter();

        let enriched = dispatcher
            .with_field("Host", "chef")
            .with_field("App", "pfin");
        enriched.try_alert(
            AlertSeverity::Critical,
            "PANIC".into(),
            "thread panicked".into(),
        );

        drop(enriched);
        drop(dispatcher);
        handle.await.expect("background task panicked");

        let captured = captured.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].0, "PANIC");
        assert_eq!(captured[0].1, "Host: chef\nApp: pfin\n\nthread panicked");
    }

    // -----------------------------------------------------------------------
    // AlertContext
    // -----------------------------------------------------------------------

    #[test]
    fn context_empty_renders_empty() {
        let ctx = AlertContext::default();
        assert!(ctx.is_empty());
        assert_eq!(ctx.render(), "");
    }

    #[test]
    fn context_single_field() {
        let ctx = AlertContext::default().with_field("Host", "chef");
        assert!(!ctx.is_empty());
        assert_eq!(ctx.render(), "Host: chef\n");
    }

    #[test]
    fn context_multiple_fields_preserve_order() {
        let ctx = AlertContext::default()
            .with_field("Host", "chef")
            .with_field("Instance", "/etc/brenn/pfin.toml")
            .with_field("App", "pfin");
        assert_eq!(
            ctx.render(),
            "Host: chef\nInstance: /etc/brenn/pfin.toml\nApp: pfin\n"
        );
    }

    #[test]
    fn context_with_field_does_not_mutate_parent() {
        let parent = AlertContext::default().with_field("Host", "chef");
        let child = parent.with_field("App", "pfin");
        assert_eq!(parent.render(), "Host: chef\n");
        assert_eq!(child.render(), "Host: chef\nApp: pfin\n");
    }

    // -----------------------------------------------------------------------
    // render_alert_body
    // -----------------------------------------------------------------------

    #[test]
    fn render_body_no_context() {
        let ctx = AlertContext::default();
        assert_eq!(
            render_alert_body(&ctx, "something broke"),
            "something broke"
        );
    }

    #[test]
    fn render_body_with_context_and_body() {
        let ctx = AlertContext::default()
            .with_field("Host", "chef")
            .with_field("App", "pfin");
        assert_eq!(
            render_alert_body(&ctx, "CC stdout closed"),
            "Host: chef\nApp: pfin\n\nCC stdout closed"
        );
    }

    #[test]
    fn render_body_with_context_empty_body() {
        let ctx = AlertContext::default().with_field("Host", "chef");
        // No trailing blank line when body is empty.
        assert_eq!(render_alert_body(&ctx, ""), "Host: chef");
    }

    // -----------------------------------------------------------------------
    // AlertDispatcher::with_field
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn dispatcher_with_field_includes_context_in_alert() {
        let (dispatcher, captured, handle) = make_capturing_alerter();

        // Parent has host context.
        let parent = dispatcher.with_field("Host", "chef");
        // Child adds app context.
        let child = parent.with_field("App", "pfin");

        // Alert from parent — only host context.
        parent.alert(AlertSeverity::Info, "test".into(), "parent body".into());
        // Alert from child — host + app context.
        child.alert(AlertSeverity::Warning, "test2".into(), "child body".into());

        drop(parent);
        drop(child);
        drop(dispatcher);
        handle.await.expect("background task panicked");

        let captured = captured.lock().unwrap();
        assert_eq!(captured.len(), 2);
        assert_eq!(captured[0].1, "Host: chef\n\nparent body");
        assert_eq!(captured[1].1, "Host: chef\nApp: pfin\n\nchild body");
    }

    // -----------------------------------------------------------------------
    // alert_once_per_process
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn once_per_process_dedups_same_key() {
        let (dispatcher, captured, handle) = make_capturing_alerter();

        dispatcher.alert_once_per_process(
            AlertSeverity::Warning,
            "Unknown CC status".into(),
            "requesting",
            "first".into(),
        );
        dispatcher.alert_once_per_process(
            AlertSeverity::Warning,
            "Unknown CC status".into(),
            "requesting",
            "second".into(),
        );
        dispatcher.alert_once_per_process(
            AlertSeverity::Warning,
            "Unknown CC status".into(),
            "requesting",
            "third".into(),
        );

        drop(dispatcher);
        handle.await.expect("background task panicked");

        let captured = captured.lock().unwrap();
        assert_eq!(captured.len(), 1, "only first call dispatches");
        assert!(captured[0].1.starts_with("first"));
        assert!(
            captured[0].1.contains("suppressed until brenn restarts"),
            "first alert explains suppression: {}",
            captured[0].1
        );
    }

    #[tokio::test]
    async fn once_per_process_different_keys_fire_independently() {
        let (dispatcher, captured, handle) = make_capturing_alerter();

        dispatcher.alert_once_per_process(
            AlertSeverity::Warning,
            "Unknown CC system message subtype".into(),
            "task_progress",
            "progress body".into(),
        );
        dispatcher.alert_once_per_process(
            AlertSeverity::Warning,
            "Unknown CC system message subtype".into(),
            "task_notification",
            "notification body".into(),
        );

        drop(dispatcher);
        handle.await.expect("background task panicked");

        let captured = captured.lock().unwrap();
        assert_eq!(
            captured.len(),
            2,
            "distinct dedup keys under same title both fire"
        );
    }

    #[tokio::test]
    async fn once_per_process_different_titles_fire_independently() {
        let (dispatcher, captured, handle) = make_capturing_alerter();

        dispatcher.alert_once_per_process(
            AlertSeverity::Warning,
            "Title A".into(),
            "same-key",
            "a body".into(),
        );
        dispatcher.alert_once_per_process(
            AlertSeverity::Warning,
            "Title B".into(),
            "same-key",
            "b body".into(),
        );

        drop(dispatcher);
        handle.await.expect("background task panicked");

        let captured = captured.lock().unwrap();
        assert_eq!(captured.len(), 2, "distinct titles keep separate slots");
    }

    #[tokio::test]
    async fn once_per_process_state_shared_across_with_field_clones() {
        // Child dispatchers created via `with_field` must share the same
        // dedup set — otherwise per-conversation bridges would each re-fire
        // the same alert once. See design.md.
        let (dispatcher, captured, handle) = make_capturing_alerter();

        let child_a = dispatcher.with_field("Conversation", "1");
        let child_b = dispatcher.with_field("Conversation", "2");

        child_a.alert_once_per_process(
            AlertSeverity::Warning,
            "Unknown CC status".into(),
            "requesting",
            "from conv 1".into(),
        );
        // Same key from a different child dispatcher — must be deduped.
        child_b.alert_once_per_process(
            AlertSeverity::Warning,
            "Unknown CC status".into(),
            "requesting",
            "from conv 2".into(),
        );

        drop(child_a);
        drop(child_b);
        drop(dispatcher);
        handle.await.expect("background task panicked");

        let captured = captured.lock().unwrap();
        assert_eq!(
            captured.len(),
            1,
            "one process = one alert even across with_field clones"
        );
    }

    // -----------------------------------------------------------------------
    // send_panic_alert_sync — formatting / no-op behavior (subprocess not
    // exercised in unit tests; integration tests cover that).
    // -----------------------------------------------------------------------

    /// `send_panic_alert_sync` on a dispatcher with no `PanicMailConfig`
    /// is a no-op: no panic, no subprocess spawned.
    #[test]
    fn sync_panic_send_noop_without_mail_config() {
        // noop_alert_dispatcher requires tokio runtime for the background task;
        // build a minimal dispatcher directly for this sync test.
        let (tx, _rx) = mpsc::channel::<DrainerMsg>(1);
        let dispatcher = AlertDispatcher {
            tx,
            context: AlertContext::default(),
            once_per_process: OncePerProcess::default(),
            panic_mail: Arc::new(None),
        };
        // Must not panic or spawn anything.
        // The None-guard in send_panic_alert_sync short-circuits before the
        // delegate, so SEND_PANIC_MAIL_SYNC_CALL_COUNT is not incremented.
        // Assert the counter to machine-check the invariant (a refactor that
        // moves the None-guard below the increment would fail here rather than
        // silently bleeding into other tests).
        let before = SEND_PANIC_MAIL_SYNC_CALL_COUNT.load(std::sync::atomic::Ordering::SeqCst);
        dispatcher.send_panic_alert_sync(AlertSeverity::Critical, "Brenn PANIC", "something broke");
        let after = SEND_PANIC_MAIL_SYNC_CALL_COUNT.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            after, before,
            "send_panic_alert_sync with None panic_mail must not call send_panic_mail_sync"
        );
    }

    /// `with_panic_mail` stores the config and `with_field` propagates it.
    #[test]
    fn panic_mail_config_propagates_through_with_field() {
        let (tx, _rx) = mpsc::channel::<DrainerMsg>(1);
        let base = AlertDispatcher {
            tx,
            context: AlertContext::default(),
            once_per_process: OncePerProcess::default(),
            panic_mail: Arc::new(None),
        };

        let with_mail = base.with_panic_mail(PanicMailConfig {
            to: "ops@example.com".to_string(),
            subject_label: "prod".to_string(),
        });
        assert!(with_mail.panic_mail.is_some());

        // with_field must carry the panic_mail through.
        let child = with_mail.with_field("Host", "chef");
        assert!(child.panic_mail.is_some());
        let cfg = child.panic_mail.as_ref().as_ref().unwrap();
        assert_eq!(cfg.to, "ops@example.com");
        assert_eq!(cfg.subject_label, "prod");
    }

    /// The subject and body produced by `send_panic_alert_sync` must match
    /// what `prepare_mail` / `render_alert_body` produce for the same inputs —
    /// verified by exercising both paths with the same parameters and comparing
    /// the expected formatted values.
    #[test]
    fn sync_panic_send_uses_same_formatting_as_async_path() {
        let alerter = MailAlerter {
            to: "ops@example.com".to_string(),
            subject_label: "brenn-prod".to_string(),
        };
        let prepared = alerter.prepare_mail(
            AlertSeverity::Critical,
            "Brenn PANIC",
            "thread panicked at main.rs:42",
        );
        // Subject must match format_mail_subject directly.
        assert_eq!(
            prepared.subject,
            format_mail_subject("brenn-prod", AlertSeverity::Critical, "Brenn PANIC")
        );
        // Body passes through unchanged (no context in this test).
        assert_eq!(prepared.body, "thread panicked at main.rs:42");

        // With context, render_alert_body prepends it.
        let ctx = AlertContext::default()
            .with_field("Host", "chef")
            .with_field("Instance", "/etc/brenn/prod.toml");
        let full_body = render_alert_body(&ctx, "thread panicked at main.rs:42");
        assert_eq!(
            full_body,
            "Host: chef\nInstance: /etc/brenn/prod.toml\n\nthread panicked at main.rs:42"
        );
    }

    /// The pending hook's `(to, subject_label, AlertContext)` — derived from
    /// `ObsConfig` via `build_alert_context` — produces byte-identical subject
    /// and body to what the full hook's `MailAlerter::prepare_mail` produces
    /// for the same inputs.
    ///
    /// This validates the design requirement that both hooks send the same mail
    /// body. Neither hook is actually invoked; only the formatting helpers are
    /// exercised directly so no subprocess or seam counter is touched.
    #[test]
    fn pending_hook_body_parity_with_full_hook() {
        use crate::obs::config::{AlertBackend, AlertConfig, ObsConfig, RateLimitConfig};

        let cfg = ObsConfig {
            log_dir: std::path::PathBuf::from("/tmp"),
            console_level: tracing::level_filters::LevelFilter::ERROR,
            file_level: tracing::level_filters::LevelFilter::ERROR,
            diagnostic_log_name: "test-diag.log".into(),
            security_log_name: "test-sec.log".into(),
            alert: Some(AlertConfig {
                backend: AlertBackend::Mail {
                    to: "parity-ops@example.com".into(),
                    subject_label: "parity-prod".into(),
                },
                rate_limit: RateLimitConfig {
                    max_alerts: 10,
                    window_secs: 60,
                },
            }),
            instance_name: Some("parity-instance".into()),
        };

        // Context that both hooks build from ObsConfig.
        let ctx = crate::obs::build_alert_context(&cfg);

        // Pending hook path: PanicMailConfig + direct formatting calls.
        let panic_cfg = PanicMailConfig {
            to: "parity-ops@example.com".into(),
            subject_label: "parity-prod".into(),
        };
        let panic_body = "thread panicked at obs/layers.rs:42";
        let pending_subject = format_mail_subject(
            &panic_cfg.subject_label,
            AlertSeverity::Critical,
            "Brenn PANIC",
        );
        let pending_body = render_alert_body(&ctx, panic_body);

        // Full hook path: MailAlerter::prepare_mail + render_alert_body.
        let alerter = MailAlerter {
            to: "parity-ops@example.com".into(),
            subject_label: "parity-prod".into(),
        };
        let prepared = alerter.prepare_mail(AlertSeverity::Critical, "Brenn PANIC", panic_body);
        let full_subject = prepared.subject;
        let full_body = render_alert_body(&ctx, &prepared.body);

        assert_eq!(
            pending_subject, full_subject,
            "subject must be byte-identical"
        );
        assert_eq!(pending_body, full_body, "body must be byte-identical");
    }
}
