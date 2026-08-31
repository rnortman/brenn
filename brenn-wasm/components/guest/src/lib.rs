// brenn-guest — guest SDK for Brenn WASM processor components.
//
// Provides typed ergonomics on top of the raw WIT bindings:
// - `Error` / `Processor` trait / `Activation` / `PortWindow` — dispatch
// - `MessageEnvelopeExt` — typed envelope helpers
// - `publish` / `publish_json` / `publish_with_urgency` / `OutPort<T>` — ports
// - `store::Transaction` RAII guard — eliminates the leaked-tx trap footgun
// - `log` / `alert` modules — fire-and-forget diagnostics
// - `config` module — operator config access
// - `dom` module — element handles and mutators, for a page-hosted component
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
pub use serde;
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
    /// A host refusal for want of quota: the activation's publish or defer bucket
    /// is empty, and buckets refill per activation.
    ///
    /// Named apart from the rest because it is the *only* refusal a conforming
    /// deployment produces transiently. Every other one — an unbound port, an
    /// unrepresentable instant, an index this activation's window never carried —
    /// is structural: no later activation repairs it, so a component that logs
    /// and carries on from it has hidden a permanent fault. Deciding between the
    /// two needs the variant, not a diagnostic string.
    QuotaExceeded(String),
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

    /// Whether this is the transient refusal — a bucket that refills — rather
    /// than a structural one.
    pub fn is_quota(&self) -> bool {
        matches!(self, Error::QuotaExceeded(_))
    }
}

impl From<Error> for bindings::ReceiveError {
    fn from(e: Error) -> Self {
        match e {
            Error::MalformedEnvelope(msg) => bindings::ReceiveError::MalformedEnvelope(msg),
            // The host has one failure arm for both: the distinction is the
            // guest's, drawn so a component can decide, and the diagnostic text
            // already names the variant.
            Error::ProcessingFailed(msg) | Error::QuotaExceeded(msg) => {
                bindings::ReceiveError::ProcessingFailed(msg)
            }
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
    let quota = matches!(e, PublishError::QuotaExceeded);
    let variant = match e {
        PublishError::NotPermitted => String::from("not-permitted"),
        PublishError::InvalidPayload(m) => format!("invalid-payload: {m}"),
        PublishError::QuotaExceeded => String::from("quota-exceeded"),
    };
    let diagnostic = format!("publish to {port}: {variant}");
    if quota {
        Error::QuotaExceeded(diagnostic)
    } else {
        Error::ProcessingFailed(diagnostic)
    }
}

// ── activation / dispatch ─────────────────────────────────────────────────────

/// Trait for processor logic. Implement this and wire with `export_processor!`.
pub trait Processor {
    /// Process one activation. Published messages are buffered and flushed
    /// atomically iff this returns `Ok`; `Err` discards all buffered publishes.
    ///
    /// The `Ok` payload is the reply to a **sync-call** activation — one whose
    /// [`Activation::sync`] names a port. Answer `Some` only there: a reply on
    /// an ordinary activation is a host trap, because a component replying to a
    /// cause that asked nothing has lost track of why it was called. Ordinary
    /// activations return `Ok(None)`.
    fn receive(activation: Activation) -> Result<Option<String>, Error>;
}

/// One activation: a snapshot of all bound input ports.
///
/// Contains one `PortWindow` per bound input port, in config order
/// (`cfg.inputs` order). Every bound port appears in every activation;
/// a port with no new messages arrives as a pure-context window
/// (`new_from == envelopes.len()`).
pub struct Activation {
    windows: Vec<PortWindow>,
    deferred: Vec<DeferredWindow>,
    now: Option<u64>,
    sync: Option<String>,
}

impl Activation {
    /// Iterate over all port windows in config order.
    pub fn port_windows(&self) -> impl Iterator<Item = &PortWindow> {
        self.windows.iter()
    }

    /// Iterate over all output-port deferred windows in config order. Each
    /// window holds this component's own parked messages on that output's
    /// channel, soonest release first.
    pub fn deferred_windows(&self) -> impl Iterator<Item = &DeferredWindow> {
        self.deferred.iter()
    }

    /// The deferred window for one output port by name, if that port is bound.
    pub fn deferred_for(&self, port: &str) -> Option<&DeferredWindow> {
        self.deferred.iter().find(|w| w.port == port)
    }

    /// The host's wall clock at drain, epoch milliseconds UTC. Use it to compute
    /// an absolute `deliver_after` for [`publish_deferred`] without holding a
    /// clock. `None` where the host exposes no UTC clock; decline to schedule
    /// rather than invent an instant.
    pub fn now(&self) -> Option<u64> {
        self.now
    }

    /// The live sync port's name when this is a sync-call activation, `None`
    /// otherwise — which is every activation a message delivery causes, and
    /// every activation a backend host mints.
    ///
    /// The named port is in [`Activation::port_windows`] like any other,
    /// carrying exactly the one live request. Answering it is the `Ok(Some(..))`
    /// return of [`Processor::receive`].
    pub fn sync(&self) -> Option<&str> {
        self.sync.as_deref()
    }

    /// Is this a sync-call activation on `port`?
    ///
    /// The comparison a handler writes against the same [`dom::SyncPort`] item
    /// it passed to [`dom::listen`], so the two halves of a gesture share one
    /// spelling instead of two string literals.
    pub fn sync_is(&self, port: dom::SyncPort) -> bool {
        self.sync() == Some(port.name())
    }

    /// The live request's window on a sync-call activation — the [`Self::sync`]
    /// port's entry among the windows — or `None` on an asynchronous one.
    ///
    /// Panics when `sync` names a port the activation does not carry. The host
    /// assembles both halves together, so their disagreement is a host bug, and
    /// windowing a request that is not there is not a state to carry on from.
    pub fn sync_window(&self) -> Option<&PortWindow> {
        let port = self.sync.as_deref()?;
        Some(
            self.windows
                .iter()
                .find(|window| window.port() == port)
                .expect("a sync-call activation carries the window of the port it names"),
        )
    }

    /// The live request on a sync-call activation — the sync port's name and the
    /// one envelope carrying the request — or `None` on an asynchronous one.
    ///
    /// This is half of the gesture idiom; [`Self::delivered_windows`] is the
    /// other half. A handler reads the request here and folds deliveries there,
    /// and never sees the request twice.
    ///
    /// Panics when the window carries other than exactly one new envelope. The
    /// host mints the request and windows it alone, so any other count is a host
    /// bug, and answering a gesture from the wrong request — or from none — is
    /// not a state to carry on from.
    pub fn sync_request(&self) -> Option<(&str, Result<MessageEnvelope, Error>)> {
        let window = self.sync_window()?;
        assert_eq!(
            window.new_raw().len(),
            1,
            "a sync-call activation's window on port {:?} carries the one live request, not {} \
             of them",
            window.port(),
            window.new_raw().len(),
        );
        let request = window
            .new_envelopes()
            .next()
            .expect("the window carries one new envelope");
        Some((window.port(), request))
    }

    /// Every window this activation *delivered*: its ports, minus the sync
    /// request's. The request is not a message anyone published, so it belongs
    /// in no delivery fold.
    ///
    /// The whole window list on an asynchronous activation, so a handler that
    /// folds through this reads the same worldview either way and cannot forget
    /// the exclusion the day it grows a gesture.
    pub fn delivered_windows(&self) -> impl Iterator<Item = &PortWindow> {
        let sync = self.sync.as_deref();
        self.windows
            .iter()
            .filter(move |window| Some(window.port()) != sync)
    }
}

/// One output port's view onto this component's own parked (deferred) messages,
/// soonest release first — a snapshot at drain.
pub struct DeferredWindow {
    port: String,
    entries: Vec<DeferredEntry>,
}

impl DeferredWindow {
    /// Logical output port name from host config.
    pub fn port(&self) -> &str {
        &self.port
    }

    /// This component's parked messages on the port's channel, release-ordered.
    pub fn entries(&self) -> &[DeferredEntry] {
        &self.entries
    }
}

/// One parked message in a [`DeferredWindow`].
pub struct DeferredEntry {
    index: u32,
    payload: String,
    deliver_after: u64,
}

impl DeferredEntry {
    /// Position within the window (release-ordered) — the handle a future
    /// cancel/edit will name. Snapshot-relative to the window it arrived in.
    pub fn index(&self) -> u32 {
        self.index
    }

    /// The message body this component published for deferred delivery.
    pub fn payload(&self) -> &str {
        &self.payload
    }

    /// Scheduled release time, epoch milliseconds UTC.
    pub fn deliver_after(&self) -> u64 {
        self.deliver_after
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
    let deferred = raw
        .deferred
        .into_iter()
        .map(|dw| DeferredWindow {
            port: dw.port,
            entries: dw
                .entries
                .into_iter()
                .map(|e| DeferredEntry {
                    index: e.index,
                    payload: e.payload,
                    deliver_after: e.deliver_after,
                })
                .collect(),
        })
        .collect();
    Ok(Activation {
        windows,
        deferred,
        now: raw.now,
        sync: raw.sync,
    })
}

/// Export glue macro: implements the WIT `Guest` trait for a shim that
/// validates the activation invariant and delegates to your `Processor` impl.
///
/// ```rust,ignore
/// struct MyProcessor;
/// impl brenn_guest::Processor for MyProcessor {
///     fn receive(a: brenn_guest::Activation) -> Result<Option<String>, brenn_guest::Error> {
///         // ...
///         Ok(None)
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
            ) -> ::core::result::Result<
                ::core::option::Option<::std::string::String>,
                $crate::bindings::ReceiveError,
            > {
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

/// Buffer a message on the named output port to become observable only at
/// `deliver_after` (epoch milliseconds UTC). A `deliver_after` at or before the
/// activation's `now` publishes immediately. This is the timer idiom: schedule
/// a message to yourself at a future instant, most often to wake yourself again.
///
/// **Requires grant:** `"ports"` in `[[wasm_consumer]]` grants.
///
/// Compute `deliver_after` from [`Activation::now`] plus your delay; a guest has
/// no clock of its own.
pub fn publish_deferred(port: &str, payload: &str, deliver_after: u64) -> Result<(), Error> {
    bindings::brenn::processor::ports::publish_deferred(port, payload, deliver_after)
        .map_err(|e| publish_error(port, e))
}

/// Map a `DeferError` to a `ProcessingFailed` with a per-port diagnostic.
fn defer_error(port: &str, e: bindings::brenn::processor::ports::DeferError) -> Error {
    use bindings::brenn::processor::ports::DeferError;
    let variant = match e {
        DeferError::NotPermitted => "not-permitted",
        DeferError::OutOfRange => "out-of-range",
        DeferError::QuotaExceeded => "quota-exceeded",
        DeferError::InvalidDeliverAfter => "invalid-deliver-after",
    };
    let diagnostic = format!("defer control on {port}: {variant}");
    if matches!(e, DeferError::QuotaExceeded) {
        Error::QuotaExceeded(diagnostic)
    } else {
        Error::ProcessingFailed(diagnostic)
    }
}

/// Cancel one of this component's own parked messages on `port`, named by its
/// `index` into the deferred window this activation delivered
/// ([`DeferredWindow::entries`]). Buffered and applied atomically iff `receive`
/// returns `Ok`; a message that released between drain and flush is a benign
/// no-op, not an error.
///
/// **Requires grant:** `"ports"` in `[[wasm_consumer]]` grants.
pub fn defer_cancel(port: &str, index: u32) -> Result<(), Error> {
    bindings::brenn::processor::ports::defer_cancel(port, index).map_err(|e| defer_error(port, e))
}

/// Edit one of this component's own parked messages on `port`, named by its
/// `index` into the deferred window this activation delivered. `payload` and
/// `deliver_after` are each `Some` to change, `None` to leave alone. Buffered and
/// applied atomically iff `receive` returns `Ok`; same index and race semantics
/// as [`defer_cancel`].
///
/// **Requires grant:** `"ports"` in `[[wasm_consumer]]` grants.
pub fn defer_edit(
    port: &str,
    index: u32,
    payload: Option<&str>,
    deliver_after: Option<u64>,
) -> Result<(), Error> {
    bindings::brenn::processor::ports::defer_edit(port, index, payload, deliver_after)
        .map_err(|e| defer_error(port, e))
}

/// Cancel every standing wake this activation's window carries on `port`, and
/// park the next one where there is one.
///
/// The timer idiom in one call: a component that wakes itself keeps at most one
/// parked message on its own port, so it cancels what the window shows before it
/// parks the next. `release_at` is `None` where there is no next instant — a
/// fixed mode with no boundary, a display with no expiry — and then nothing is
/// parked.
///
/// **Requires grant:** `"ports"` in `[[wasm_consumer]]` grants.
///
/// # Panics
///
/// On any refusal but an exhausted quota. A ticker that fails to re-park stops
/// ticking, and for every reason but the quota it stops *forever*: the port is
/// not a bound output of this instance, or the index is not one this
/// activation's own window carried, or the release instant is not
/// representable. Each is a deployment or a component fault that no later
/// activation repairs, and a log line on a page that then silently stops
/// tracking time is that fault hidden rather than reported. A quota is the one
/// refusal a conforming deployment produces transiently — buckets refill per
/// activation — so it is logged and the activation carries on.
pub fn repark(activation: &Activation, port: &str, payload: &str, release_at: Option<u64>) {
    if let Some(window) = activation.deferred_for(port) {
        for entry in window.entries() {
            if let Err(err) = defer_cancel(port, entry.index()) {
                assert!(
                    err.is_quota(),
                    "brenn-guest: the stale tick on {port:?} could not be cancelled: {err:?}"
                );
                log::error(format!(
                    "stale tick on {port:?} could not be cancelled: quota exceeded"
                ));
            }
        }
    }
    let Some(deliver_after) = release_at else {
        return;
    };
    if let Err(err) = publish_deferred(port, payload, deliver_after) {
        assert!(
            err.is_quota(),
            "brenn-guest: the tick on {port:?} could not be scheduled: {err:?}"
        );
        log::error(format!(
            "tick on {port:?} could not be scheduled: quota exceeded"
        ));
    }
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

/// Typed publish handle: the type parameter is the payload type, and a handle
/// publishes nothing else.
///
/// **Requires grant:** `"ports"` in `[[wasm_consumer]]` grants.
///
/// An owned payload binds through a `const`:
///
/// ```rust,ignore
/// const OUT: brenn_guest::OutPort<MyMessage> = brenn_guest::OutPort::new("out");
/// OUT.publish(&my_message)?;
/// ```
///
/// A payload that borrows cannot be named in a `const` — its lifetime has no
/// spelling there — so construct the handle at the call instead, where the
/// lifetime is inferred:
///
/// ```rust,ignore
/// brenn_guest::OutPort::new("out").publish(&MyMessage { text })?;
/// ```
///
/// A guest with a specification-generated module reaches both idioms through
/// its handle accessors rather than through `new`, and there the type parameter
/// is bounded by the port's own payload marker trait — so the payload type is
/// bound to the port once, by an impl, and every publish site is checked
/// against that binding.
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
    //! **Requires grant:** `"config"` in the consumer's `grants`.
    //!
    //! The config map is fixed for the process lifetime (seeded from the host
    //! config at startup; changes require a host restart). Keys under the
    //! reserved prefix `"brenn."` are host-injected facts; an operator config
    //! cannot set them.

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

// ── mqtt ──────────────────────────────────────────────────────────────────────

pub mod mqtt {
    //! Direct MQTT publishing.
    //!
    //! **Requires grant:** `"mqtt"` in `[[wasm_consumer]]` grants, and an
    //! `mqtt_publish` ACL listing every client this component may reach. If the
    //! grant is absent the host does not link the `brenn:processor/mqtt`
    //! interface and any import of it causes a load-time panic.
    //!
    //! **Not transactional with the activation outcome.** A send here goes
    //! straight to the broker and returns the broker's outcome inline; it is
    //! NOT discarded by a later err or trap the way a buffered
    //! [`crate::publish`] is. A guest that needs all-or-nothing semantics
    //! across MQTT and the bus must not mix the two.
    //!
    //! `client` is the `[[mqtt_client]]` slug; `topic` is a concrete topic
    //! under that client and must contain no wildcard. For QoS >= 1 the call
    //! awaits the PUBACK before returning.

    use super::Error;
    use crate::bindings::brenn::processor::mqtt::{self as raw};

    /// The broker outcome of a failed publish, for a guest that classifies
    /// before it gives up. [`publish`] flattens this into an [`Error`]; reach
    /// for [`try_publish`] to keep it.
    pub use crate::bindings::brenn::processor::mqtt::MqttPublishError as PublishError;

    /// Whether a later activation may retry this failure. Only a broker
    /// disconnect / submit failure / dropped ack is transient; a denial, an
    /// absent service, a malformed address, an exhausted quota and a
    /// broker-side rejection are all permanent for this config.
    pub fn is_transient(e: &PublishError) -> bool {
        matches!(e, PublishError::Broker(_))
    }

    /// Human-readable rendering of a `PublishError` for a diagnostic message.
    fn mqtt_err(client: &str, topic: &str, e: PublishError) -> Error {
        let variant = match e {
            PublishError::NotPermitted => String::from("not-permitted"),
            PublishError::NoConnector => String::from("no-connector"),
            PublishError::InvalidPayload(m) => format!("invalid-payload: {m}"),
            PublishError::QuotaExceeded => String::from("quota-exceeded"),
            PublishError::Broker(m) => format!("broker: {m}"),
            PublishError::BrokerRejected(m) => format!("broker-rejected: {m}"),
        };
        Error::failed(format!("mqtt publish {client}/{topic}: {variant}"))
    }

    /// Publish `payload` to `topic` on `client`, keeping the broker outcome.
    ///
    /// The whole WIT surface, for the guest that needs `content_type` or wants
    /// to classify the failure with [`is_transient`].
    pub fn try_publish(
        client: &str,
        topic: &str,
        payload: &[u8],
        content_type: Option<&str>,
        qos: u8,
        retain: bool,
    ) -> Result<(), PublishError> {
        raw::mqtt_publish(client, topic, payload, content_type, qos, retain)
    }

    /// Publish `payload` to `topic` on `client`.
    ///
    /// Diagnostic on error: `"mqtt publish {client}/{topic}: {variant}"`.
    pub fn publish(
        client: &str,
        topic: &str,
        payload: &[u8],
        content_type: Option<&str>,
        qos: u8,
        retain: bool,
    ) -> Result<(), Error> {
        try_publish(client, topic, payload, content_type, qos, retain)
            .map_err(|e| mqtt_err(client, topic, e))
    }

    /// Publish a UTF-8 body as `text/plain`.
    pub fn publish_text(
        client: &str,
        topic: &str,
        body: &str,
        qos: u8,
        retain: bool,
    ) -> Result<(), Error> {
        publish(
            client,
            topic,
            body.as_bytes(),
            Some("text/plain"),
            qos,
            retain,
        )
    }

    /// Serialize `value` to JSON and publish it as `application/json`.
    ///
    /// A serialization failure is a guest bug, not a broker outcome, and maps
    /// to `Err(ProcessingFailed("serialize: {e}"))` before anything is sent.
    pub fn publish_json<T: serde::Serialize>(
        client: &str,
        topic: &str,
        value: &T,
        qos: u8,
        retain: bool,
    ) -> Result<(), Error> {
        let payload = crate::serde_json::to_vec(value)
            .map_err(|e| Error::failed(format!("serialize: {e}")))?;
        publish(
            client,
            topic,
            &payload,
            Some("application/json"),
            qos,
            retain,
        )
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

// ── dom ───────────────────────────────────────────────────────────────────────

pub mod dom {
    //! Element handles and mutators, for a component the page hosts.
    //!
    //! **Requires grant:** `"dom"`. The page-wide reads and the wrappers of
    //! other instances are [`crate::page_dom`], a capability of its own that
    //! only a surface's designated chrome instance holds.
    //!
    //! A [`Node`] is a handle the host mints, canonical per element within one
    //! instance: the same element is always the same handle, so comparing
    //! handles compares elements. Handle tables are per instance, so a handle is
    //! meaningless anywhere but where it was minted.
    //!
    //! **A handle lives until its element is destroyed.** [`remove`] destroys
    //! the node and its subtree; [`set_text`] destroys the children of the node
    //! it clears. Every handle naming a destroyed element is invalidated, in
    //! every instance's table — a chrome removing page furniture invalidates
    //! whatever another instance named inside it. Moves do not invalidate:
    //! [`append`] and [`insert_before`] reparent, and the handle stays good.
    //!
    //! **Misuse traps.** An unknown handle, a handle another instance minted, a
    //! handle whose element was destroyed, a tag the host will not create — none
    //! of these is a runtime condition a component could recover from, so none
    //! of them is an error variant. The instance dies with an error card,
    //! exactly as it would on any other trap.
    //!
    //! **What the host will create.** Reach is confined to the subtree by the
    //! handle table; authority is confined by an allow-list, because markup can
    //! name script and script in this page runs with the page's authority.
    //! [`create_element`] takes only a listed tag and [`set_attribute`] only a
    //! listed attribute name; anything else traps. The lists themselves live on
    //! the `brenn:processor/dom` interface doc, which is the one place they are
    //! written for a component author to read. A kind reaching for something off
    //! them is reaching for the page, which is not what this capability is.
    //!
    //! State between activations is ordinary struct state: the component is
    //! instantiated once per instance and lives for the page, so a handle held
    //! in a field is still that element on the next activation.

    use crate::bindings::brenn::processor::dom as raw;

    /// A page element this instance may act on.
    ///
    /// `Copy`, and compared by identity: the host mints one handle per element,
    /// so equality here is element equality.
    ///
    /// Serializes as the bare handle: the host synthesizes and reads handles as
    /// integers, and the newtype is the guest's own typing of them.
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        serde::Serialize,
        serde::Deserialize,
    )]
    #[serde(transparent)]
    pub struct Node(pub(crate) u64);

    impl Node {
        /// Wrap a handle the host minted. For the page-wide capability and for a
        /// component bridging to hand-written bindings.
        pub fn from_raw(handle: u64) -> Node {
            Node(handle)
        }

        /// The raw host handle. For a component bridging to hand-written
        /// bindings; ordinary code never needs it.
        pub fn raw(self) -> u64 {
            self.0
        }
    }

    /// This instance's own host element — the root of the subtree it owns.
    pub fn root() -> Node {
        Node(raw::root())
    }

    /// Create a detached element with the given tag. A tag the host refuses
    /// traps.
    pub fn create_element(tag: &str) -> Node {
        Node(raw::create_element(tag))
    }

    /// Create an element carrying a bare marker attribute — the house pattern for
    /// a stylesheet or test anchor, since `id` and `style` are off the
    /// allow-list and a `data-` marker is what a rule selects on.
    pub fn marked(tag: &str, marker: &str) -> Node {
        let node = create_element(tag);
        set_attribute(node, marker, "");
        node
    }

    /// Set an attribute. An empty value is the idiom for a boolean attribute
    /// (`hidden`, `disabled`). A name or value the host refuses traps.
    pub fn set_attribute(node: Node, name: &str, value: &str) {
        raw::set_attribute(node.0, name, value);
    }

    /// Remove an attribute. Removing one that is not present is a no-op.
    pub fn remove_attribute(node: Node, name: &str) {
        raw::remove_attribute(node.0, name);
    }

    /// Set a boolean attribute's presence in one call.
    pub fn set_attribute_present(node: Node, name: &str, present: bool) {
        if present {
            set_attribute(node, name, "");
        } else {
            remove_attribute(node, name);
        }
    }

    /// Replace the node's children with one inert text node. Text only — this
    /// never parses markup, so there is no markup-injection surface here.
    ///
    /// The replaced children are destroyed: every handle naming an element
    /// strictly inside `node` is invalidated. `node` itself stays valid.
    pub fn set_text(node: Node, text: &str) {
        raw::set_text(node.0, text);
    }

    /// Set one inline style property.
    pub fn set_style_property(node: Node, name: &str, value: &str) {
        raw::set_style_property(node.0, name, value);
    }

    /// Remove one inline style property.
    pub fn remove_style_property(node: Node, name: &str) {
        raw::remove_style_property(node.0, name);
    }

    /// Append `child` as `parent`'s last child, detaching it from wherever it
    /// was.
    pub fn append(parent: Node, child: Node) {
        raw::append(parent.0, child.0);
    }

    /// Insert `child` before `reference` under `parent`, or append when
    /// `reference` is `None`.
    pub fn insert_before(parent: Node, child: Node, reference: Option<Node>) {
        raw::insert_before(parent.0, child.0, reference.map(|n| n.0));
    }

    /// Destroy the node: detach it from its parent, and invalidate its handle
    /// along with every handle naming an element inside the removed subtree.
    /// There is no re-appending it — to hide a node and keep it, use the
    /// `hidden` attribute.
    pub fn remove(node: Node) {
        raw::remove(node.0);
    }

    /// A form control's current value; the empty string for a node that has
    /// none.
    pub fn value(node: Node) -> String {
        raw::value(node.0)
    }

    /// Set a form control's value.
    pub fn set_value(node: Node, value: &str) {
        raw::set_value(node.0, value);
    }

    /// A reserved port a sync-call activation can name.
    ///
    /// Gesture ports are sync-only vocabulary — no specification declares them,
    /// so nothing generates them. Naming one as a `const` of this type is how a
    /// kind keeps [`listen`] and its
    /// [`crate::Activation::sync_is`] match on one item rather than two
    /// literals that can drift apart silently.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
    pub struct SyncPort(pub &'static str);

    impl SyncPort {
        /// The port name as the host spells it.
        pub fn name(self) -> &'static str {
            self.0
        }
    }

    /// Compare a host-spelled port name against a declared port, so a kind
    /// dispatching on [`crate::Activation::sync`] matches on the same item it
    /// wired with [`listen`] rather than unwrapping the newtype at every arm.
    impl PartialEq<&str> for SyncPort {
        fn eq(&self, other: &&str) -> bool {
            self.0 == *other
        }
    }

    impl PartialEq<SyncPort> for &str {
        fn eq(&self, other: &SyncPort) -> bool {
            *self == other.0
        }
    }

    // TODO(surface-guest-mount-idiom): every UI kind hand-copies the same mount
    // bookkeeping around this constant — an `Option<View>` field, an identical
    // `expect`, and a mount arm in its handler. The SDK should own the
    // lifecycle; which shape it takes is a `Processor`-trait decision.

    /// The port a mount activation names: the instance's first call, where it
    /// builds its UI. Reserved by its colon, which no specification identifier
    /// can spell.
    pub const MOUNT: SyncPort = SyncPort("brenn:mount");

    /// Wire a gesture: the host listens for `event` on the node and answers it
    /// with a sync-call activation on `port`.
    ///
    /// A listener dies with its element: a detached element receives no UI
    /// events, and detaching is what destroys it. The activation runs on the
    /// event's own stack, so what the handler reads through [`value`] is the
    /// state at event time.
    pub fn listen(node: Node, event: &str, port: SyncPort) {
        raw::listen(node.0, event, port.name());
    }

    /// The page environment's UTC offset in minutes at `epoch_ms` — the one
    /// page fact a component cannot compute from the activation clock.
    pub fn utc_offset_minutes(epoch_ms: u64) -> i32 {
        raw::utc_offset_minutes(epoch_ms)
    }

    /// The synthesized body of a gesture's sync-call request.
    ///
    /// The host names the event, the node whose listener fired, and the nearest
    /// handle-mapped ancestor of the event target — which is how a delegated
    /// listener on a container tells apart which child was hit.
    ///
    /// The three field names are a wire contract with the host that synthesizes
    /// them, spelled once on each side: `GESTURE_*_FIELD` in
    /// `brenn-surface-contract` is the host's half, pinned to these literals
    /// there.
    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    pub struct Gesture {
        /// The DOM event type, as passed to [`listen`].
        pub event: String,
        /// The node the listener is attached to.
        pub listener: Node,
        /// The nearest handle-mapped ancestor of the event target.
        pub target: Node,
    }

    /// The one key the gesture reply dialect has: whether the host should call
    /// `preventDefault()` on the event that caused the activation.
    const CANCEL_KEY: &str = "cancel";

    /// The reply that asks the host to `preventDefault` the gesture's event.
    ///
    /// A gesture's reply dialect is exactly this one field: present and true
    /// cancels, present and false does not. Returned as the `Ok` payload of
    /// [`crate::Processor::receive`] on a sync-call activation.
    pub fn cancel_reply() -> String {
        format!("{{\"{CANCEL_KEY}\":true}}")
    }

    /// The reply that lets the gesture's event proceed.
    ///
    /// A handler that has no opinion answers `Ok(None)` instead — no reply at
    /// all, which the host also reads as "do not cancel". This is the one that
    /// has decided, so it is spelled in the dialect rather than as an empty
    /// object the reader would refuse.
    pub fn proceed_reply() -> String {
        format!("{{\"{CANCEL_KEY}\":false}}")
    }
}

// ── page-dom ──────────────────────────────────────────────────────────────────

pub mod page_dom {
    //! Page-wide reads and mutations, for the one instance that arranges the
    //! page.
    //!
    //! **Requires grant:** `"page-dom"`, and `"dom"` with it — every function
    //! here reaches outside the calling instance's own subtree, which is the
    //! whole reason it is a separate capability rather than more of the same
    //! one, and an instance that arranges the page also mutates what it finds.
    //!
    //! Its own module, not a submodule of [`crate::dom`], because the generated
    //! `pub use brenn_guest::<module>;` re-export is what holds a class's
    //! declared capabilities and the code it can write equal at compile time. A
    //! class granted only `"dom"` cannot name anything here.

    use crate::bindings::brenn::processor::page_dom as raw;
    use crate::dom::Node;

    /// The surface root, which holds every instance wrapper.
    pub fn root() -> Node {
        Node::from_raw(raw::page_root())
    }

    /// The document body, where page-level state is stamped.
    pub fn body() -> Node {
        Node::from_raw(raw::page_body())
    }

    /// The wrapper element the host created for another instance, or `None`
    /// when that instance has not registered yet.
    ///
    /// A `None` is the ordinary transient of a page still coming up, not an
    /// error: registration emits a state message, so the next delivery is the
    /// cue to look again.
    pub fn instance_wrapper(instance: &str) -> Option<Node> {
        raw::instance_wrapper(instance).map(Node::from_raw)
    }

    /// The node's parent, or `None` when it is detached or is the document
    /// root.
    pub fn parent(node: Node) -> Option<Node> {
        raw::parent(node.raw()).map(Node::from_raw)
    }
}
