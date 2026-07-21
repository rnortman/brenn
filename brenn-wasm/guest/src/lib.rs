// brenn-guest — guest SDK for Brenn WASM processor components.
//
// Provides typed ergonomics on top of the raw WIT bindings:
// - `Error` / `Processor` trait / `Activation` / `PortWindow` — dispatch
// - `MessageEnvelopeExt` — typed envelope helpers
// - `publish` / `publish_json` / `publish_with_urgency` / `OutPort<T>` — ports
// - `store::Transaction` RAII guard — eliminates the leaked-tx trap footgun
// - `log` / `alert` modules — fire-and-forget diagnostics
// - `config` module — operator config access
// - `export_processor!` macro — wires `Processor` impl to the WIT export
//
// Host-enforced limits (documented, not re-implemented here):
// Port attenuation (not-permitted), per-sink publish token buckets (per output
// port and per MQTT client; default fill 1.0 + input amplification 1.0 per new
// envelope, capacity 1.0 carried over between activations — operator-tunable via
// publish_per_activation / publish_capacity / amplification), backed by global
// per-activation backstops (512 calls / 256 messages / 4 MiB), max_payload_bytes,
// fuel/epoch caps (trap), store size
// cap (quota-exceeded), single-transaction rule (nested begin → backend error),
// log/alert per-activation quotas (256 log / 4 alert), truncation caps
// (4 KiB message/body, 256 B title), config-map contract (process-lifetime-
// fixed, "brenn." prefix host-injected), publish provenance caveat
// (processor.wit:94-106). This crate propagates structured errors as `Error`;
// it never retries or falls back.
//
// **Publish-failure scope**: returning `Err` from `receive` discards ALL
// buffered publishes for the activation — across all port-windows (atomic
// activation-scoped flush, processor.wit:89-92). Log/alert emissions are NOT
// discarded (immediate by contract).

use core::marker::PhantomData;

pub mod bindings;

pub use bindings::export;

// ── re-exports ────────────────────────────────────────────────────────────────

pub use brenn_envelope::{ChannelScheme, MessageEnvelope, Urgency, WebhookEnvelope};
pub use serde_json;

// ── error ─────────────────────────────────────────────────────────────────────

/// Activation processing error. Maps 1:1 onto the WIT `receive-error` variant.
///
/// Returning `Err` from `receive` discards ALL buffered publishes for the
/// activation (across all port-windows). Log/alert emissions are kept.
#[derive(Debug)]
pub enum Error {
    /// Guest could not parse an envelope JSON value.
    MalformedEnvelope(String),
    /// Guest-defined processing failure with a diagnostic message.
    ProcessingFailed(String),
}

impl Error {
    /// Construct a `MalformedEnvelope` error with a formatted message.
    pub fn malformed(msg: impl core::fmt::Display) -> Self {
        Error::MalformedEnvelope(format!("{msg}"))
    }

    /// Construct a `ProcessingFailed` error with a formatted message.
    pub fn failed(msg: impl core::fmt::Display) -> Self {
        Error::ProcessingFailed(format!("{msg}"))
    }
}

impl From<Error> for bindings::ReceiveError {
    fn from(e: Error) -> Self {
        match e {
            Error::MalformedEnvelope(msg) => bindings::ReceiveError::MalformedEnvelope(msg),
            Error::ProcessingFailed(msg) => bindings::ReceiveError::ProcessingFailed(msg),
        }
    }
}

/// Map a `PublishError` to a `ProcessingFailed` with a per-port diagnostic.
///
/// Single canonical expansion of the `PublishError` variants — both `publish()`
/// and `publish_with_urgency()` delegate here so that adding a new variant
/// requires only one change.  The port name is included so multi-port
/// diagnostics are actionable ("publish to out1: not-permitted").
fn publish_error(port: &str, e: bindings::brenn::processor::ports::PublishError) -> Error {
    use bindings::brenn::processor::ports::PublishError;
    let variant = match e {
        PublishError::NotPermitted => String::from("not-permitted"),
        PublishError::InvalidPayload(m) => format!("invalid-payload: {m}"),
        PublishError::QuotaExceeded => String::from("quota-exceeded"),
    };
    Error::ProcessingFailed(format!("publish to {port}: {variant}"))
}

// ── activation / dispatch ─────────────────────────────────────────────────────

/// Trait for processor logic. Implement this and wire with `export_processor!`.
pub trait Processor {
    /// Process one activation. Published messages are buffered and flushed
    /// atomically iff this returns `Ok`; `Err` discards all buffered publishes.
    fn receive(activation: Activation) -> Result<(), Error>;
}

/// One activation: a snapshot of all bound input ports.
///
/// Contains one `PortWindow` per bound input port, in config order
/// (`cfg.inputs` order). Every bound port appears in every activation;
/// a port with no new messages arrives as a pure-context window
/// (`new_from == envelopes.len()`).
pub struct Activation {
    windows: Vec<PortWindow>,
}

impl Activation {
    /// Iterate over all port windows in config order.
    pub fn port_windows(&self) -> impl Iterator<Item = &PortWindow> {
        self.windows.iter()
    }
}

/// Messages and context for one logical input port.
pub struct PortWindow {
    port: String,
    dropped: u32,
    /// All envelopes: context (..new_from) then new (new_from..).
    envelopes: Vec<String>,
    new_from: usize,
}

impl PortWindow {
    /// Logical input port name from host config.
    pub fn port(&self) -> &str {
        &self.port
    }

    /// Within-host-lifetime gap signal only; always 0 for sampled ports.
    /// `dropped == 0` is NOT proof of no-gap across a host restart.
    /// See `processor.wit:37-48` caveats.
    pub fn dropped(&self) -> u32 {
        self.dropped
    }

    /// New (unprocessed) envelopes for this activation, parsed.
    ///
    /// Parse failure yields `Err(Error::MalformedEnvelope)` for that item;
    /// the caller decides whether to skip (`filter_map`) or fail the batch
    /// (`env?`).
    pub fn new_envelopes(&self) -> impl Iterator<Item = Result<MessageEnvelope, Error>> + '_ {
        self.envelopes[self.new_from..].iter().map(|json| {
            serde_json::from_str(json).map_err(|e| Error::malformed(format!("envelope JSON: {e}")))
        })
    }

    /// Retained context envelopes (channel-wide most-recent, NOT a
    /// per-subscriber delivery log — see `processor.wit:18-29`), parsed.
    pub fn context_envelopes(&self) -> impl Iterator<Item = Result<MessageEnvelope, Error>> + '_ {
        self.envelopes[..self.new_from].iter().map(|json| {
            serde_json::from_str(json)
                .map_err(|e| Error::malformed(format!("context envelope JSON: {e}")))
        })
    }

    /// Raw JSON strings for new envelopes (new_from..).
    pub fn new_raw(&self) -> &[String] {
        &self.envelopes[self.new_from..]
    }

    /// Raw JSON strings for context envelopes (..new_from).
    pub fn context_raw(&self) -> &[String] {
        &self.envelopes[..self.new_from]
    }
}

/// Validate and construct an `Activation` from WIT-generated types.
///
/// Returns `Err(ProcessingFailed)` if any port-window has `new_from >
/// envelopes.len()` (host contract violation, processor.wit:32-36). Called
/// inside `export_processor!` before user code runs, so all PortWindow
/// accessors may slice unconditionally.
///
/// `pub` is required here because `export_processor!` is a `#[macro_export]`
/// and expands in the downstream component's crate — `$crate::build_activation`
/// resolves to a cross-crate call, which needs `pub`. Direct use by component
/// authors is discouraged (use `export_processor!` instead).
#[doc(hidden)]
pub fn build_activation(raw: bindings::Activation) -> Result<Activation, Error> {
    let mut windows = Vec::with_capacity(raw.ports.len());
    for pw in raw.ports {
        let new_from = pw.new_from as usize;
        let len = pw.envelopes.len();
        if new_from > len {
            return Err(Error::failed(format!(
                "host invariant violation: new_from {new_from} > {len} on port {}",
                pw.port
            )));
        }
        windows.push(PortWindow {
            port: pw.port,
            dropped: pw.dropped,
            envelopes: pw.envelopes,
            new_from,
        });
    }
    Ok(Activation { windows })
}

/// Export glue macro: implements the WIT `Guest` trait for a shim that
/// validates the activation invariant and delegates to your `Processor` impl.
///
/// ```rust,ignore
/// struct MyProcessor;
/// impl brenn_guest::Processor for MyProcessor {
///     fn receive(a: brenn_guest::Activation) -> Result<(), brenn_guest::Error> {
///         // ...
///         Ok(())
///     }
/// }
/// brenn_guest::export_processor!(MyProcessor);
/// ```
#[macro_export]
macro_rules! export_processor {
    ($ty:ty) => {
        struct __BrennGuestShim;
        impl $crate::bindings::Guest for __BrennGuestShim {
            fn receive(
                a: $crate::bindings::Activation,
            ) -> ::core::result::Result<(), $crate::bindings::ReceiveError> {
                let activation = match $crate::build_activation(a) {
                    Ok(a) => a,
                    Err(e) => return ::core::result::Result::Err($crate::bindings::ReceiveError::from(e)),
                };
                <$ty as $crate::Processor>::receive(activation)
                    .map_err($crate::bindings::ReceiveError::from)
            }
        }
        $crate::export!(__BrennGuestShim with_types_in $crate::bindings);
    };
}

// ── envelope helpers ──────────────────────────────────────────────────────────

/// Extension methods for `MessageEnvelope`.
pub trait MessageEnvelopeExt {
    /// Deserialize the `body` field as `T`.
    ///
    /// Returns `Err(MalformedEnvelope)` with context on parse failure.
    fn json_body<T: serde::de::DeserializeOwned>(&self) -> Result<T, Error>;

    /// Parse `body` as a `WebhookEnvelope`.
    ///
    /// Returns `Err(ProcessingFailed)` if `envelope_type != "webhook"`;
    /// `Err(MalformedEnvelope)` if the body JSON is malformed.
    fn webhook_body(&self) -> Result<WebhookEnvelope, Error>;
}

impl MessageEnvelopeExt for MessageEnvelope {
    fn json_body<T: serde::de::DeserializeOwned>(&self) -> Result<T, Error> {
        serde_json::from_str(&self.body).map_err(|e| Error::malformed(format!("body JSON: {e}")))
    }

    fn webhook_body(&self) -> Result<WebhookEnvelope, Error> {
        if self.envelope_type != ChannelScheme::Webhook {
            return Err(Error::failed(format!(
                "expected webhook envelope, got {:?}",
                self.envelope_type
            )));
        }
        serde_json::from_str(&self.body)
            .map_err(|e| Error::malformed(format!("webhook body JSON: {e}")))
    }
}

// ── ports ─────────────────────────────────────────────────────────────────────

/// Buffer a message on the named output port using the port's configured
/// default urgency.
///
/// **Requires grant:** `"ports"` in `[[wasm_consumer]]` grants.
///
/// Diagnostic on error: `"publish to {port}: {variant}"`.
pub fn publish(port: &str, payload: &str) -> Result<(), Error> {
    bindings::brenn::processor::ports::publish(port, payload).map_err(|e| publish_error(port, e))
}

/// Serialize `value` to JSON and publish on the named output port.
///
/// **Requires grant:** `"ports"` in `[[wasm_consumer]]` grants.
pub fn publish_json<T: serde::Serialize>(port: &str, value: &T) -> Result<(), Error> {
    let payload =
        serde_json::to_string(value).map_err(|e| Error::failed(format!("serialize: {e}")))?;
    publish(port, &payload)
}

/// Buffer a message with an explicit urgency override.
///
/// **Requires grant:** `"ports"` in `[[wasm_consumer]]` grants.
///
/// Use when per-message urgency intent differs from the port default.
/// `Urgency` variants bridge exhaustively to WIT urgency — adding a variant
/// on either side fails compilation.
pub fn publish_with_urgency(port: &str, payload: &str, urgency: Urgency) -> Result<(), Error> {
    let wit_urgency = urgency_to_wit(urgency);
    bindings::brenn::processor::ports::publish_with_urgency(port, payload, wit_urgency)
        .map_err(|e| publish_error(port, e))
}

/// Convert `brenn_envelope::Urgency` to WIT urgency exhaustively.
/// Variant drift on either side fails compilation.
fn urgency_to_wit(u: Urgency) -> bindings::brenn::processor::ports::Urgency {
    use bindings::brenn::processor::ports::Urgency as WitUrgency;
    match u {
        Urgency::VeryLow => WitUrgency::VeryLow,
        Urgency::Low => WitUrgency::Low,
        Urgency::Normal => WitUrgency::Normal,
        Urgency::High => WitUrgency::High,
    }
}

/// Typed publish handle. Declare as a `const`; type parameter encodes the
/// payload type.
///
/// **Requires grant:** `"ports"` in `[[wasm_consumer]]` grants.
///
/// ```rust,ignore
/// const OUT: brenn_guest::OutPort<MyMessage> = brenn_guest::OutPort::new("out");
/// OUT.publish(&my_message)?;
/// ```
pub struct OutPort<T> {
    name: &'static str,
    _marker: PhantomData<fn() -> T>,
}

impl<T: serde::Serialize> OutPort<T> {
    /// Create a typed port handle. `name` is the logical output port name
    /// from host config.
    pub const fn new(name: &'static str) -> Self {
        OutPort {
            name,
            _marker: PhantomData,
        }
    }

    /// Serialize `value` and publish with the port's configured default urgency.
    pub fn publish(&self, value: &T) -> Result<(), Error> {
        publish_json(self.name, value)
    }

    /// Serialize `value` and publish with an explicit urgency override.
    pub fn publish_with_urgency(&self, value: &T, urgency: Urgency) -> Result<(), Error> {
        let payload =
            serde_json::to_string(value).map_err(|e| Error::failed(format!("serialize: {e}")))?;
        publish_with_urgency(self.name, &payload, urgency)
    }
}

// ── store ─────────────────────────────────────────────────────────────────────

pub mod store {
    //! Transactional KV store access.
    //!
    //! **Requires grant:** `"store"` in `[[wasm_consumer]]` grants. Also requires
    //! `store_path` in the consumer config. If the grant is absent the host does
    //! not link the `brenn:processor/store` interface and any import of it causes
    //! a load-time panic.
    //!
    //! # Footgun elimination
    //!
    //! `Transaction::drop` calls `rollback()` on a live transaction before the
    //! resource handle is released, preventing the host's "leaked-tx" trap on
    //! error-return paths. On a genuine guest trap, `Drop` does not run
    //! (no unwinding in release WASM); the host's existing leaked-tx cleanup
    //! covers that path.
    //!
    //! # Commit semantics
    //!
    //! `commit()` empties the guard before invoking the binding, so a failed
    //! commit does not trigger `Drop`-rollback against a transaction the host
    //! already rolled back — avoiding the host's rollback-after-commit warning.

    use super::Error;
    use crate::bindings::brenn::processor::store::{self as raw, StoreError};

    fn store_err(op: &str, e: StoreError) -> Error {
        Error::failed(format!("store {op}: {e:?}"))
    }

    /// RAII transaction guard. `Drop` rolls back a live transaction.
    pub struct Transaction {
        inner: Option<raw::Transaction>,
    }

    /// Begin a new transaction.
    ///
    /// Error diagnostic: `"store begin: {e:?}"`.
    pub fn begin() -> Result<Transaction, Error> {
        raw::begin()
            .map(|tx| Transaction { inner: Some(tx) })
            .map_err(|e| store_err("begin", e))
    }

    impl Transaction {
        /// Read a value for `(ns, key)`. `None` if absent.
        pub fn get(&self, ns: &str, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
            self.inner
                .as_ref()
                .unwrap()
                .get(ns, key)
                .map_err(|e| store_err("get", e))
        }

        /// Write or replace a value.
        pub fn put(&self, ns: &str, key: &[u8], value: &[u8]) -> Result<(), Error> {
            self.inner
                .as_ref()
                .unwrap()
                .put(ns, key, value)
                .map_err(|e| store_err("put", e))
        }

        /// Delete a key. Absent key is a no-op.
        pub fn delete(&self, ns: &str, key: &[u8]) -> Result<(), Error> {
            self.inner
                .as_ref()
                .unwrap()
                .delete(ns, key)
                .map_err(|e| store_err("delete", e))
        }

        /// List `(key, value)` pairs in `ns` in ascending key order,
        /// from `start` (inclusive) to `end` (exclusive). `end = None` means
        /// to-the-end; `limit = 0` means unlimited.
        #[allow(clippy::type_complexity)]
        pub fn scan(
            &self,
            ns: &str,
            start: &[u8],
            end: Option<&[u8]>,
            limit: u32,
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
            self.inner
                .as_ref()
                .unwrap()
                .scan(ns, start, end, limit)
                .map_err(|e| store_err("scan", e))
        }

        /// Commit the transaction, consuming the guard.
        ///
        /// Takes `inner` out before calling the binding so that a failed
        /// commit does not trigger `Drop`-rollback (which would emit a
        /// spurious host warning on a transaction the host already rolled back).
        pub fn commit(mut self) -> Result<(), Error> {
            // Invariant: `inner` is `Some` for the full lifetime of a live
            // `Transaction`. These consuming methods (`commit`/`rollback`) are
            // the only callers of `take()`; by-value `self` means they can
            // each be called at most once. `Drop` only clears via `take()` when
            // the guard is still live (Some), never after a consuming call.
            let tx = self.inner.take().unwrap();
            tx.commit().map_err(|e| store_err("commit", e))
        }

        /// Rollback the transaction, consuming the guard.
        pub fn rollback(mut self) {
            // Invariant: same as `commit` — `inner` is `Some` here because
            // `rollback` consumes `self` by value and cannot be called after
            // `commit`, `rollback`, or `Drop` has already cleared it.
            let tx = self.inner.take().unwrap();
            tx.rollback();
        }
    }

    impl Drop for Transaction {
        /// Roll back a live transaction. No-op if already committed or rolled back.
        fn drop(&mut self) {
            if let Some(tx) = self.inner.take() {
                tx.rollback();
            }
        }
    }
}

// ── log ───────────────────────────────────────────────────────────────────────

pub mod log {
    //! Guest-side logging.
    //!
    //! **Requires grant:** `"log"` in `[[wasm_consumer]]` grants.
    //!
    //! Fire-and-forget, IMMEDIATE: unlike `ports::publish`, log emission is
    //! NOT buffered and NOT transactional with the activation outcome. Lines
    //! emitted before a later trap/err are kept. Calls beyond the per-activation
    //! quota (256) are dropped silently; the host warns once per activation.
    //! The wrappers return `()` — inventing a `Result` would imply delivery
    //! tracking that the WIT contract deliberately omits.

    pub use crate::bindings::brenn::processor::log::Level;

    /// Emit one log line at the given level.
    pub fn log(level: Level, msg: impl core::fmt::Display) {
        crate::bindings::brenn::processor::log::log(level, &format!("{msg}"));
    }

    /// Emit a TRACE log line.
    pub fn trace(msg: impl core::fmt::Display) {
        log(Level::Trace, msg);
    }

    /// Emit a DEBUG log line.
    pub fn debug(msg: impl core::fmt::Display) {
        log(Level::Debug, msg);
    }

    /// Emit an INFO log line.
    pub fn info(msg: impl core::fmt::Display) {
        log(Level::Info, msg);
    }

    /// Emit a WARN log line.
    pub fn warn(msg: impl core::fmt::Display) {
        log(Level::Warn, msg);
    }

    /// Emit an ERROR log line.
    ///
    /// Note: `log::error` does NOT escalate to an alert. Use `alert::alert`
    /// for conditions a human should act on.
    pub fn error(msg: impl core::fmt::Display) {
        log(Level::Error, msg);
    }
}

// ── alert ─────────────────────────────────────────────────────────────────────

pub mod alert {
    //! Operator alerting.
    //!
    //! **Requires grant:** `"alert"` in `[[wasm_consumer]]` grants.
    //!
    //! Fire-and-forget: delivery is subject to the host's alert rate limiter
    //! and queue. Calls beyond the per-activation quota (4) are dropped; host
    //! logs the suppressed count. Use for human-actionable conditions only.
    //! There is NO implicit log-level→alert escalation.

    pub use crate::bindings::brenn::processor::alert::Severity;

    /// Page the operator.
    ///
    /// `title` and `body` are truncated and control-escaped by the host (256 B
    /// and 4 KiB limits respectively). The host prefixes title with the
    /// component identity.
    pub fn alert(
        severity: Severity,
        title: impl core::fmt::Display,
        body: impl core::fmt::Display,
    ) {
        crate::bindings::brenn::processor::alert::alert(
            severity,
            &format!("{title}"),
            &format!("{body}"),
        );
    }
}

// ── config ────────────────────────────────────────────────────────────────────

pub mod config {
    //! Operator config access.
    //!
    //! **Requires grant:** `"config"` in `[[wasm_consumer]]` grants.
    //!
    //! The config map is fixed for the process lifetime (seeded from host TOML
    //! at startup; changes require a host restart). Keys under the reserved
    //! prefix `"brenn."` are host-injected facts; operator TOML cannot set them.

    use super::Error;
    use core::str::FromStr;

    /// Raw config lookup. Absent key is a normal condition (`None`).
    pub fn get(key: &str) -> Option<String> {
        crate::bindings::brenn::processor::config::get(key)
    }

    /// Parse a config value as `T`. Absent key → `Ok(None)`; parse failure
    /// → `Err(ProcessingFailed("config {key}: {e}"))`.
    pub fn get_parsed<T: FromStr>(key: &str) -> Result<Option<T>, Error>
    where
        T::Err: core::fmt::Display,
    {
        match get(key) {
            None => Ok(None),
            Some(s) => s
                .parse::<T>()
                .map(Some)
                .map_err(|e| Error::failed(format!("config {key}: {e}"))),
        }
    }

    /// Like `get_parsed`, but absent key → `Err(ProcessingFailed("config {key}: missing"))`.
    pub fn require<T: FromStr>(key: &str) -> Result<T, Error>
    where
        T::Err: core::fmt::Display,
    {
        match get_parsed::<T>(key)? {
            Some(v) => Ok(v),
            None => Err(Error::failed(format!("config {key}: missing"))),
        }
    }
}

// ── tools ─────────────────────────────────────────────────────────────────────

pub mod tools {
    //! Granted-tool invocation.
    //!
    //! **Requires grant:** a `[[wasm_consumer.tool_grant]]` (the `tools`
    //! capability and the tool-result inbox are derived from the grant, never
    //! authored). If no grant is present the host does not link the
    //! `brenn:processor/tools` interface and any import causes a load-time panic.
    //!
    //! Only the async class is wrapped here: an async call is a message. The
    //! request is grant/ACL/arg-size checked synchronously; on success it rides
    //! the activation's transactional flush (reaches the bus iff `receive`
    //! returns `Ok`), and the result arrives later as a separate activation on
    //! the component's tool-result inbox, correlated by `call_id`.

    use super::Error;
    use crate::bindings::brenn::processor::tools::{self as raw, ToolError};

    /// Human-readable rendering of a `ToolError` for a diagnostic message.
    fn tool_err(tool: &str, e: ToolError) -> Error {
        let variant = match e {
            ToolError::NotGranted => String::from("not-granted"),
            ToolError::Denied(r) => format!("denied: {r}"),
            ToolError::InvalidArgs(m) => format!("invalid-args: {m}"),
            ToolError::RateLimited => String::from("rate-limited"),
            ToolError::WrongClass => String::from("wrong-class"),
            ToolError::Internal(t) => format!("internal: {t}"),
        };
        Error::failed(format!("call-async {tool}: {variant}"))
    }

    /// Enqueue an async-class tool call. `args_json` is the tool's argument
    /// object serialized as JSON; `call_id` is a caller-chosen opaque
    /// correlation string echoed verbatim in the result.
    ///
    /// Returns once the request is validated and buffered. A grant/ACL/arg
    /// failure maps to `Err(ProcessingFailed(...))`; returning it from `receive`
    /// discards the buffered request along with every other buffered publish.
    pub fn call_async(tool: &str, args_json: &str, call_id: &str) -> Result<(), Error> {
        raw::call_async(tool, args_json, call_id).map_err(|e| tool_err(tool, e))
    }

    /// Serialize `args` to JSON and enqueue an async-class tool call.
    pub fn call_async_json<T: serde::Serialize>(
        tool: &str,
        args: &T,
        call_id: &str,
    ) -> Result<(), Error> {
        let args_json = serde_json::to_string(args)
            .map_err(|e| Error::failed(format!("serialize args: {e}")))?;
        call_async(tool, &args_json, call_id)
    }
}
