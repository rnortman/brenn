//! Component contract v1 — the kernel ↔ component seam.
//!
//! These names and shapes never cross the WS wire; they are the DOM-CustomEvent
//! contract between the surface kernel and the component modules it mounts, plus
//! the `window`-event seam between the kernel and the TS bootstrap. They live in
//! their own crate because they are the seam: contract surface as load-bearing
//! as the wire frames, which both the kernel and every component crate compile
//! against, and which out-of-tree component authors depend on directly. The wire
//! frames themselves are `brenn-surface-proto`; this crate depends on it for the
//! types the seam's details carry as JSON strings, never the other way round.
//!
//! Envelopes cross the kernel↔component boundary as CustomEvents carrying JSON
//! **strings** and other **primitives only** — no structured objects — so the
//! boundary stays serialization-clean across independently-built wasm modules.
//! All rendered text reaches the DOM as `textContent`, never `innerHTML`.
//!
//! # The invariant
//!
//! > **There is one component model. Any component runs on any host that can
//! > satisfy its imports. Hosting eligibility is an import profile, not a
//! > component kind.**
//!
//! Every rule in this crate is subordinate to that sentence, and it is the test
//! a change to this seam has to pass. A component importing `store`/`mqtt`/
//! `tools` is backend-only; a component importing DOM capability — everything
//! this seam serves — is surface-only. Both are the *same* rule reading a
//! different import profile, not two kinds of thing. Components see exactly one
//! mechanism: **messages on named ports**.
//!
//! # Component ABIs
//!
//! A component instance's `abi` is a **build/loading fact only** — which
//! toolchain artifact the kernel loads and how. It is never an execution mode and
//! never a capability statement; hosting eligibility is the import profile
//! above. The set (`brenn_surface_proto::Abi`):
//!
//! - `dom` — a wasm-bindgen module defining a custom element, speaking the seam
//!   this crate defines. Imports DOM capability via wasm-bindgen/web-sys, hence
//!   surface-only by profile.
//! - `processor` — a `brenn:processor` component-model artifact: the same
//!   artifact that deploys backend-side under `[[wasm_consumer]]`. Headless by
//!   profile (its world has no DOM imports), so it uses no DOM events at all —
//!   its imports are direct host-supplied calls. Two transports, one vocabulary.
//! - `dom-ts`, `html` — reserved names, so v1 does not freeze them out.
//!
//! `dom` and `processor` are loadable; `dom-ts` and `html` are reserved names
//! that resolve to a named boot panic rather than a value that half-works.
//!
//! # Delivery: the activation is the only shape
//!
//! A component on any hosting and any ABI sees exactly one delivery shape, the
//! **activation**: every bound input port windowed — retained context first, new
//! messages after, split by `new_from`, with a `dropped` delta — the whole thing
//! delivered by one call through the registration seam below, publishes buffered
//! during the call and flushed atomically iff it returns ok. There is no
//! per-envelope event, no drop marker, and no component-visible gap.
//!
//! The doctrine that shape encodes, because a port author must be able to read it
//! somewhere:
//!
//! - **The port is a view, not a pipe.** An input port views a sliding window of
//!   its channel's stream. Messages before `new_from` are **seen** — still in the
//!   view because retention has not displaced them yet. Seeing a message again is
//!   not an error and not "duplicate delivery"; it is what "seen" means. A
//!   component needing exactly-once-seen tracks its own high-water by
//!   `message_id`.
//! - **`dropped` is a counter, not a marker in the stream.** It is the delivery
//!   loss on that binding since the port's previous activation. The lost message
//!   itself is not gone: it remains visible as retained context in this or any
//!   later activation whose `retain_depth` still covers it. Recovery *is*
//!   retention — there is no gap-and-replay choreography and no terminal port
//!   failure.
//! - **Err consumes.** The messages an activation was assembled for are acked
//!   when it is assembled, so returning err (or trapping) does not redeliver
//!   them; they reappear only as retained context. That is backend parity.
//! - **A page reload is the one legitimate everything-is-new event.** Cursors,
//!   rings and registrations die with the page, so everything in the first
//!   windows after a reload is legitimately new. That is not a bug.
//!
//! Typed gaps (`EpochChanged`, `HoleExceedsRing`, `BeyondRetained`) survive only
//! at the websocket/resume layer, where the kernel handles them by re-resuming;
//! the component observes at most a first-window-after-resubscription. `GapReason`
//! is not part of the component seam.
//!
//! One asymmetry stands outside the activation boundary and is **not** a defect: a
//! gesture
//! handler (click, input) publishes with no activation in flight — no boundary
//! to attach a flush rule to — so gesture publishes stay immediate until
//! sync-call activations land. That is a named, bounded gap, and no SDK surface
//! may be shaped in a way that assumes immediate gesture publish is permanent.
//!
//! # The activation seam
//!
//! [`ACTIVATION_REGISTER`] is how a `dom` component joins activation delivery:
//! once per instance, from its element's first `connectedCallback`, it hands the
//! kernel an entry function; the kernel calls that entry once per activation with
//! the [`Activation`] as JSON and reads its return for the flush rule. See
//! [`ACTIVATION_REGISTER`] for the call convention.
//!
//! Publishes made from inside an entry are **buffered**: they ride the ordinary
//! [`PORT_PUBLISH`] event, and the kernel routes one to the in-flight buffer iff
//! the dispatching instance is the one whose entry is on the stack — activations
//! are serialized per instance and synchronous on the one JS thread, so exactly
//! one instance can be mid-activation. A buffered publish is answered
//! synchronously on the event detail's [`PUBLISH_STATUS_FIELD`]. A publish from
//! any other context is a **gesture publish**: immediate, unanswered, the named
//! bounded gap above.
//!
//! # Component-contract events (kernel ↔ component)
//!
//! Delivery is not on this list: it is the direct entry call described above, not
//! an event. What rides events is the fire-and-forget plumbing.
//!
//! Component → kernel:
//!
//! - [`PORT_PUBLISH`] — a component's intent to publish. **Must be dispatched
//!   with `bubbles: true, composed: true` AND on the component's mounted element
//!   itself or from within its shadow root.** The kernel derives component
//!   identity from `event.target` at a delegated `#surface-root` listener; after
//!   shadow retargeting that target is the host element in both permitted cases.
//!   Publishes dispatched elsewhere (e.g. on an inner light-DOM button) present
//!   the wrong target, are unroutable, and are dropped and reported. `detail =
//!   { port, body, urgency? }`; `body` is a string. Components see **ports
//!   only** — logical config names — never channel addresses, mirroring the
//!   backend WASM port model for exact policy symmetry.
//! - [`COMPONENT_LOG`] — a component's intent to log. Same dispatch rule as
//!   [`PORT_PUBLISH`] (`bubbles: true, composed: true`, on the mounted element
//!   or from within its shadow root), so the kernel derives component identity
//!   from the retargeted `event.target` at the delegated `#surface-root`
//!   listener. `detail = { level, message }`; `level` is a lowercase log-level
//!   wire string (`"trace"`…`"error"`, see
//!   [`brenn_surface_proto::LogLevel::from_wire_str`]) fixed at the component
//!   call site, `message` a string. The kernel stamps `source =
//!   "component:<kind>"` and forwards a `Log` frame; a missing/non-string field
//!   or an unrecognized `level` is dropped and reported as malformed rather than
//!   coerced.
//! - [`COMPONENT_ALERT`] — a component's intent to page an operator. Same
//!   dispatch rule as [`PORT_PUBLISH`] (`bubbles: true, composed: true`, on the
//!   mounted element or from within its shadow root), so the kernel derives
//!   component identity from the retargeted `event.target`. `detail =
//!   { severity, title, body }`; `severity` is a lowercase alert-severity wire
//!   string (`"info"`/`"warning"`/`"critical"`, see
//!   [`brenn_surface_proto::AlertSeverity::from_wire_str`]) fixed at the
//!   component call site, `title`/`body` strings. Forwarded as an `Alert` frame
//!   **only** on an alert-granted surface; on an ungranted surface the kernel
//!   drops it and logs a `warn` breadcrumb naming the component, never sending
//!   an ungranted `Alert`. A missing/non-string field or an unrecognized
//!   `severity` is dropped and reported as malformed rather than coerced.
//! - [`COMPONENT_PANIC`] — dispatched on `window` from the component module's
//!   panic hook, which knows its own kind but not its element. `detail =
//!   { component, message }`, both strings. A module-level panic hook cannot
//!   know which instance panicked, and a poisoned wasm module poisons every
//!   instance it backs, so the kernel error-cards **every** mounted instance of
//!   that kind and reports each one under its own identity — not just one
//!   section.
//!
//! # Bootstrap-seam events (kernel → bootstrap, on `window`)
//!
//! A different audience — the permanent TS bootstrap floor, not component
//! modules — but the same frozen-contract discipline:
//!
//! - [`SURFACE_RELOAD`] — `detail = { reason }` (a string). The kernel requests a
//!   page reload; the bootstrap funnels it through its capped reload guard. The
//!   kernel's panic hook dispatches this with the panic message as `reason`.
//! - [`SURFACE_READY`] — no detail. First successful connect after load; the
//!   bootstrap resets its reload-loop counter on this.
//!
//! # Why DOM events are the transport
//!
//! Each component is its own wasm module, because that is what contains a panic
//! to one component. Separate modules cannot call each other in Rust, so every
//! cross-module hop pays the JS boundary regardless of what rides it —
//! CustomEvents are then the framework-neutral choice that hands us delegation
//! and retargeting-based element identity for free. That is the whole argument:
//! events are **transport**, never vocabulary. Components reason about ports and
//! messages; the event names below are the plumbing underneath, and a component
//! is never asked to understand them as anything else.
//!
//! The corollary matters as much: the transport is replaceable. `processor`-abi
//! instances already use none of it (the kernel holds their call handles, so it
//! calls them directly), and even this seam could become direct calls via a
//! registration API if events ever became a problem. Swapping it would not
//! change one word of the vocabulary.
//!
//! # The side-effect gradient
//!
//! "Atomic flush" never meant "nothing happened." An activation that returns err
//! or traps unwinds only the transactional effects; everything else has already
//! happened and stays happened:
//!
//! | Effect | On err/trap |
//! |---|---|
//! | Port publishes (buffered during the activation) | Discarded — transactional |
//! | `store` writes (backend hosting) | Rolled back — transactional |
//! | `log` / `alert` / `sync` tool imports | Immediate, unrollbackable |
//! | DOM mutation (surface hosting) | Immediate, unrollbackable |
//!
//! DOM mutation joins the non-transactional bucket: pixels the entry painted
//! before it erred stay painted. An author who needs the rendered state to match
//! the flushed state must paint last, after every fallible step.
//!
//! # Rendering is not a port
//!
//! DOM access is an **import capability**, not a port and not an output binding.
//! A component does not "publish to the screen": it holds DOM capability by
//! virtue of its import profile (the invariant above), and it mutates. That
//! mutation is non-transactional in every activation flavor — see the gradient
//! table. The only per-flavor difference is the **gesture token**: it is live
//! during a sync activation (a gesture handler's call stack, where the browser
//! still honours user-activation-gated APIs) and absent during an async one.
//! Nothing else about rendering changes between flavors.
//!
//! # Reserved names
//!
//! Two reservation families exist so that a name cannot be squatted before the
//! machinery behind it lands:
//!
//! - **`local:brenn/*` control channels** — exhaustively enumerated by
//!   [`brenn_surface_proto::RESERVED_LOCAL_CHANNELS`]; a `local:brenn/*` address
//!   absent from that table is undefined vocabulary and boot rejects it.
//! - **`<prefix>.surface.<slug>.instance.<name>.config`** — the future
//!   per-instance runtime config channel. The address builder exists and its
//!   grammar is pinned by test; nothing publishes it, no `[[channel]]` block
//!   declares it, and it is not special-cased anywhere. The reservation is a
//!   naming fact, not machinery.
//!
//! # Light DOM and skinning
//!
//! Components render into the light DOM. Shadow DOM is permitted internally, but
//! it opts a component out of skinning: `data-*` hooks plus global stylesheets
//! are exactly what make "new skin = one CSS file" cheap, and a shadow root is
//! opaque to them. The event seam survives shadow DOM either way (`composed:
//! true` plus host-element retargeting), which is why this is a skinning
//! trade-off and not a contract violation. CSS collisions in the light DOM are
//! managed by component-prefixed naming.
//!
//! # In-page separation is never a security boundary
//!
//! A surface component module runs **unsandboxed** in the authenticated page's
//! JS realm with the full authority of the logged-in page: the DOM, the session
//! WS channel, every other component's ports and rendered data. It is *not*
//! capability-gated the way a backend wasmtime guest is, so installing an
//! out-of-tree component trusts it with that full authority. Everything this
//! seam enforces — identity from `event.target`, the ungranted-alert drop, the
//! reserved names — is **bug containment**: it keeps an honest component's bug
//! inside that component, and it stops nothing a malicious module wants to do.
//!
//! Real enforcement is server-side, without exception: every effect a component
//! can have off this page travels through the kernel → WS → server gates, which
//! trust nothing the page says about itself.
//!
//! # Naming conventions
//!
//! A component's config `kind` determines its element tag and module artifact:
//!
//! - `kind` ↦ custom element `brenn-<kind>` (see [`element_name`]).
//! - `kind` ↦ module artifact `brenn_<kind with - → _>.js` (see
//!   [`module_artifact`]) — wasm-bindgen derives artifact names from crate names,
//!   so crate `brenn-protobar` → element `brenn-protobar` → `brenn_protobar.js`.
//!
//! `kind` is boot-validated to `^[a-z0-9][a-z0-9-]*$`, which is a valid custom
//! element name stem and a valid filename stem.
//!
//! # Module shape
//!
//! Each `dom`-abi component is its own wasm-bindgen `--target web` module whose
//! init registers its custom element(s) and installs its panic hook (dispatching
//! [`COMPONENT_PANIC`]). The recommended — not mandated — pattern for the
//! custom-element class shim is a few lines of `#[wasm_bindgen(inline_js)]`
//! defining an `HTMLElement` subclass whose lifecycle callbacks delegate to
//! exported Rust functions. The `brenn-surface-component-support` crate is an
//! optional in-tree implementation of this pattern (panic hook, element
//! registration, DOM helpers, untrusted-detail readers, conformant publish);
//! in-tree components use it, but it is a convenience, not contract surface —
//! an out-of-tree component may implement the shape directly against this crate.
//!
//! `connectedCallback` fires on **every** insertion of the element into a
//! connected tree, not once per element, so a component's build-the-UI step
//! must guard against re-entry (e.g. a marker attribute set before building)
//! or a reparent will duplicate its UI and listeners.
//!
//! # Instances
//!
//! One component `kind` may be mounted several times on one surface, each mount
//! a distinct **instance** with its own id, its own element, and its own port
//! bindings. The instance is the principal: it owns the bindings, the send
//! budget, and the attribution, exactly as a backend `[[app]]` slug does. The
//! kind is the manifest — what the module needs — and holds no authority.
//!
//! The kernel stamps the instance id on the mounted element and its wrapper as a
//! `data-instance` attribute; a component MAY read it (e.g. for debugging) but
//! MUST NOT need it — its activation entry is its own, and everything it
//! dispatches goes out on its own element, so identity is already implicit on
//! both sides of the seam.
//!
//! # Mount and arrange
//!
//! Mounting and arranging are two jobs with two owners, and the boundary between
//! them is one element:
//!
//! - **The kernel mounts.** It creates one **wrapper** element per `dom`
//!   instance — `data-instance="<instance>"`, `data-kind="<kind>"` — and mounts
//!   the component's custom element inside it. The kernel owns the wrapper and
//!   everything in it: the element while the instance lives, an error card once
//!   it dies. Wrappers are born in a hidden kernel-owned staging container under
//!   `#surface-root`, and the kernel never moves one again.
//! - **Chrome arranges.** Chrome reparents wrappers into its own layout sections
//!   and stamps layout state (`data-panel`, a panel label header) on wrappers and
//!   sections — **never inside a wrapper**. An instance no layout places is
//!   mounted, warm, and pumping, with no pixels; whether that is expressed by
//!   leaving it staged or by a section chrome hides is chrome's business, not the
//!   contract's.
//!
//! Reparenting preserves element identity, so a component's registered activation
//! entry, its delegated events, and its mounted-instance identity survive
//! arrangement untouched — a reparent never deregisters, and the registration
//! fires once per instance lifetime regardless of how often the element moves.
//! But `connectedCallback` fires again on each move, which is why the
//! re-entry guard above is a requirement rather than a nicety. A component MUST
//! NOT assume it is arranged only once, and MUST NOT assume it is ever arranged
//! at all: it may be mounted with no pixels for the whole page's life.
//!
//! Chrome holds a **page-DOM authority grant**: it is the one component allowed
//! to touch DOM outside its own subtree (`body` attributes, `#surface-root`
//! attributes, and other components' wrappers). The grant is named here so the
//! authority is contract rather than folklore, and so review can hold every
//! non-chrome component to never exercising it. It is not mechanically
//! enforceable: in-page separation is never a security boundary, and a component
//! that reaches outside its subtree is a bug the page cannot prevent, only
//! contain.
//!
//! Because one wasm module backs every instance of its kind, a component's
//! per-element state **must** live per element (constructed in the element's
//! own lifecycle, e.g. `connectedCallback`), never in module-level statics.
//! Module-level mutable state is shared across every instance and will corrupt
//! a multi-instance surface. This is a hard requirement, not a suggestion.

use brenn_envelope::MessageEnvelope;

// ── Activation delivery (kernel → component) ───────────────────────────────

/// One activation: every bound input port of one instance, windowed.
///
/// This is the only delivery shape. The kernel batches deliveries into
/// activations, assembles the windows, and invokes the instance's registered
/// activation entry once per activation, buffering its publishes and flushing
/// them atomically iff the entry returns ok.
///
/// Every bound input port appears in **every** activation, in config order,
/// whether or not it has new messages — a port with nothing new arrives as a
/// pure-context window. A component must not assume `ports.len() == 1`, and must
/// not assume a port's presence means that port is why it woke.
///
/// Semantics are `processor.wit`'s, verbatim, and so is the carrier: this is
/// `brenn_activation::Activation` at the envelope type a surface component is
/// handed. The same shape reaches a component under wasmtime on the backend,
/// where the host names it `ProcessorActivation` and carries envelope JSON.
pub type Activation = brenn_activation::Activation<MessageEnvelope>;

/// One input port's view onto its channel at activation time: retained context
/// followed by new messages. See [`brenn_activation::PortWindow`] for what the
/// fields mean — the port is a view, not a pipe.
pub type PortWindow = brenn_activation::PortWindow<MessageEnvelope>;

/// Why a buffered publish was refused, returned synchronously to the component
/// from inside its activation entry.
///
/// The `processor.wit` triple verbatim — a component's publish-error vocabulary
/// does not change with its hosting. Refusal is an answer, never a failure: a
/// refused publish is simply not buffered, the rest of the buffer is intact, and
/// the activation continues. What to do about it is the component's decision, as
/// it is on the backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishError {
    /// The port is not a bound output of this instance. The component named a
    /// port its config does not give it.
    NotPermitted,
    /// The body exceeds the surface's publish-body cap. The cap applies to every
    /// class, `local:` included: a component's body-size contract must not change
    /// because an operator rebound its output.
    InvalidPayload,
    /// A budget is exhausted — this activation's per-activation cap
    /// (publishes / bytes / calls) or the port's own millitoken sink bucket.
    /// Buckets refill per activation, so the next activation may well succeed.
    QuotaExceeded,
}

/// Why an activation entry returned unsuccessfully.
///
/// An err is a **failed activation, not a death**: the buffer is discarded, a
/// failure is counted, and the instance keeps running and keeps being delivered.
/// The messages the failed activation consumed reappear only as retained
/// context — the same recovery every other drop has, and the same contract the
/// backend gives a guest that returns `err`.
///
/// A *trap* is the other thing entirely, and is not this type: a panic (a JS
/// exception in the browser, a `catch_unwind` natively) leaves the instance's
/// memory presumed poisoned, so it is terminal for that one instance. A
/// component cannot express a trap by returning; it expresses one by panicking.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivationError {
    /// The component's own description of what went wrong. Diagnostic detail for
    /// the operator, never parsed: the kernel treats every err identically.
    pub message: String,
}

// ── The activation seam (component ↔ kernel) ───────────────────────────────

/// Component → kernel, on the component's mounted element. Must be
/// `bubbles: true, composed: true`, dispatched once per instance from the
/// element's first `connectedCallback`. `detail = { entry }` where `entry` is a
/// JS function — an in-page event, never serialized, so carrying a function is
/// exactly what this seam is for.
///
/// The kernel resolves *which* instance registered from the retargeted
/// `event.target` over its mounted-instance registry, never from the detail: a
/// component cannot claim an instance the kernel did not mount it as. A
/// registration whose target resolves to no mounted instance, or to an instance
/// already registered, is dropped and reported — an in-page component bug, never
/// a page-killing panic.
///
/// **Call convention.** The kernel invokes `entry` once per activation with one
/// argument: the serde-JSON string of the [`Activation`]. The return value says
/// what happened, and the three answers are the three outcomes:
///
/// - `undefined`/`null` → **ok**. Every publish the entry buffered is flushed,
///   in call order.
/// - a string → **err**, the string being the component's own account. The
///   buffer is discarded, a failure is counted, the instance keeps running.
/// - a thrown exception → **trap**. The buffer is discarded and the instance is
///   terminal — error card, `failed`, one death report. One subject, never the
///   page.
///
/// One encode/decode per activation, not per message: the JS boundary is paid
/// regardless, and paying it once per activation is strictly cheaper than the
/// per-envelope events this replaces.
pub const ACTIVATION_REGISTER: &str = "brenn-activation-register";

/// The [`PORT_PUBLISH`] detail field the kernel writes a **buffered** publish's
/// answer into, synchronously, before the dispatch returns.
///
/// Present iff the publish was routed into an in-flight activation's buffer —
/// i.e. the dispatching instance is the one whose entry is currently on the
/// stack. Absent means the publish took the immediate gesture path, which has no
/// synchronous answer (the named, bounded gap until sync-call activations land).
///
/// The value is [`publish_status_str`]'s wire string: `"ok"`, or one of the
/// [`PublishError`] triple's spellings.
pub const PUBLISH_STATUS_FIELD: &str = "status";

/// The [`PUBLISH_STATUS_FIELD`] wire string for a buffered publish's answer. The
/// single executable definition of the values, shared by the kernel that writes
/// them and the SDK that reads them, so the seam cannot drift by hand-copied
/// literal.
pub fn publish_status_str(status: Result<(), PublishError>) -> &'static str {
    match status {
        Ok(()) => "ok",
        Err(PublishError::NotPermitted) => "not-permitted",
        Err(PublishError::InvalidPayload) => "invalid-payload",
        Err(PublishError::QuotaExceeded) => "quota-exceeded",
    }
}

/// The inverse of [`publish_status_str`]: parse a [`PUBLISH_STATUS_FIELD`] value,
/// or `None` for a string this contract never spells.
pub fn parse_publish_status(status: &str) -> Option<Result<(), PublishError>> {
    match status {
        "ok" => Some(Ok(())),
        "not-permitted" => Some(Err(PublishError::NotPermitted)),
        "invalid-payload" => Some(Err(PublishError::InvalidPayload)),
        "quota-exceeded" => Some(Err(PublishError::QuotaExceeded)),
        _ => None,
    }
}

// ── Component-contract events (kernel ↔ component) ──────────────────────────

/// Component → kernel. Must be `bubbles: true, composed: true` and dispatched on
/// the mounted element or from within its shadow root. `detail = { port, body,
/// urgency? }`; `port`/`body` are strings.
///
/// `urgency` is optional: a lowercase RFC 8030 urgency wire string
/// (`"very-low"`/`"low"`/`"normal"`/`"high"`, parsed by
/// [`brenn_surface_proto::Urgency::parse`]), the component's per-message
/// override. Absent ⇒ the port's configured default applies, which the server
/// resolves. An unrecognized value is dropped and reported as malformed rather
/// than coerced — same rule as every other enum-valued detail field on this seam
/// (`level`, `severity`): silently downgrading a component's stated intent to
/// `normal` would be a fallback that hides the bug.
pub const PORT_PUBLISH: &str = "brenn-port-publish";

/// Component → kernel. Same dispatch rule as [`PORT_PUBLISH`] (`bubbles: true,
/// composed: true`, on the mounted element or from within its shadow root).
/// `detail = { level, message }` where `level` is a lowercase log-level wire
/// string (see [`brenn_surface_proto::LogLevel::from_wire_str`]) and `message` a
/// string.
pub const COMPONENT_LOG: &str = "brenn-log";

/// Component → kernel. Same dispatch rule as [`PORT_PUBLISH`] (`bubbles: true,
/// composed: true`, on the mounted element or from within its shadow root).
/// `detail = { severity, title, body }` where `severity` is a lowercase
/// alert-severity wire string (see
/// [`brenn_surface_proto::AlertSeverity::from_wire_str`]) and `title`/`body` are
/// strings. Forwarded as an `Alert` frame only on an alert-granted surface.
pub const COMPONENT_ALERT: &str = "brenn-alert";

/// Component → kernel, dispatched on `window` from the component's panic hook.
/// `detail = { component, message }` (both strings).
pub const COMPONENT_PANIC: &str = "brenn-component-panic";

// ── Bootstrap-seam events (kernel → bootstrap, on `window`) ─────────────────

/// Kernel → bootstrap, on `window`. `detail = { reason }` (a string). Funnelled
/// through the bootstrap's capped reload guard.
pub const SURFACE_RELOAD: &str = "brenn-surface-reload";

/// Kernel → bootstrap, on `window`. No detail. First successful connect after
/// load; resets the bootstrap's reload-loop counter.
pub const SURFACE_READY: &str = "brenn-surface-ready";

/// Kernel → bootstrap, on `window`. `detail = { instances }` (an array of
/// instance-id strings). Asks the bootstrap to load and instantiate the
/// transpiled module of every named headless processor instance.
///
/// Processor instantiation cannot ride the bootstrap's own module-loading pass:
/// an instance's config map and its bindings row arrive with `Welcome`, i.e.
/// after `start()`, and both the `config` import and registration admission
/// resolve against them. The kernel therefore names its processor instances once
/// its first bindings land, and the loader answers — kernel-decided, exactly like
/// every other mount-plan outcome.
pub const PROCESSOR_START: &str = "brenn-processor-start";

// ── Naming conventions ─────────────────────────────────────────────────────

/// The `brenn-` prefix shared by every component's custom-element tag. The one
/// home for this literal: [`element_name`] and [`element_name_for_instance`]
/// prepend it when building a tag.
pub const ELEMENT_PREFIX: &str = "brenn-";

/// The id of the surface DOM root element. A page ↔ kernel contract point: the
/// backend page renders `<div id="surface-root">`, the kernel mounts components
/// and its banner inside it, and the TS bootstrap renders pre-kernel failures
/// into it. One definition all Rust consumers compile against.
pub const SURFACE_ROOT_ID: &str = "surface-root";

/// The custom element tag stem for a component `kind`: `brenn-<kind>`.
///
/// `kind` is boot-validated to [`is_valid_kind`], so the result is always a valid
/// custom-element name. This is the *kind's* name and is not a tag any element
/// carries: every mounted element is an instance, and instances are tagged by
/// [`element_name_for_instance`]. It survives as the stem that mapping builds on
/// and as the module-artifact key.
pub fn element_name(kind: &str) -> String {
    format!("{ELEMENT_PREFIX}{kind}")
}

/// The custom element tag for one declared instance: `brenn-<kind>--<instance>`.
///
/// One instance, one module evaluation, one linear memory, one element
/// definition — the tag is per-instance because the module behind it is. The
/// `--` separator is unambiguous by validation, not by luck: [`is_valid_kind`]
/// rejects `--` anywhere in a kind or an instance id, so the split point is the
/// only `--` in the tag and the mapping is collision-free and deterministic.
///
/// Both halves are boot-validated to [`is_valid_kind`], so the result is always a
/// valid custom-element name (a `-`-containing name with an ASCII-lowercase
/// first character).
pub fn element_name_for_instance(kind: &str, instance: &str) -> String {
    format!("{ELEMENT_PREFIX}{kind}--{instance}")
}

/// The name of the wasm-bindgen export every `dom` component module carries: the
/// loader calls it once, immediately after the module's `default` init, passing
/// the manifest entry's instance id.
///
/// Identity has to arrive this way. A wasm-bindgen `--target web` module cannot
/// read the glue module's `import.meta.url` from Rust — an `inline_js` shim is
/// emitted as its own snippet module, whose `import.meta.url` is the snippet's,
/// so the specifier's `?instance=` query is invisible in-module. The query's only
/// job is forcing the browser to mint distinct module records; the identity
/// itself is handed over by this call. It is a loading-shim parameter — the TS
/// layer moves one string from the manifest into the module it just loaded — and
/// carries no message logic.
pub const BIND_INSTANCE_EXPORT: &str = "brenn_bind_instance";

/// The wasm-bindgen `--target web` module artifact for a component `kind`:
/// `brenn_<kind with - → _>.js`, matching wasm-bindgen's crate-name-derived
/// artifact naming.
pub fn module_artifact(kind: &str) -> String {
    format!("brenn_{}.js", kind.replace('-', "_"))
}

/// The jco-transpiled module path for a processor `kind`, relative to the
/// surface asset root: `processor/<kind>/<kind>.js`.
///
/// Unlike [`module_artifact`]'s flat wasm-bindgen naming, a transpiled component
/// is a directory — the entry JS plus one or more core wasm files jco emits
/// beside it, whose exact set is jco-version-dependent. The entry module resolves
/// its siblings relative to its own URL, so the directory is the unit and this
/// names only its entry point. The single home for the layout the transpile rule
/// writes and the page manifest reads.
pub fn processor_module_path(kind: &str) -> String {
    format!("processor/{kind}/{kind}.js")
}

/// Reserved instance id addressing the kernel's error-report output port. A
/// surface error report rides an ordinary
/// [`brenn_surface_proto::ClientFrame::Publish`] to `(ERROR_REPORT_INSTANCE,
/// ERROR_REPORT_PORT)`. The `#` prefix makes the id operator-unusable — it can
/// never satisfy [`is_valid_kind`], the charset every configured instance id is
/// boot-validated against — so the reservation cannot collide with a configured
/// component instance.
pub const ERROR_REPORT_INSTANCE: &str = "#brenn";

/// Reserved port name on [`ERROR_REPORT_INSTANCE`] the kernel publishes surface
/// error reports to (bound to the operator's `surface_error_channel` when it is
/// configured; absent otherwise).
pub const ERROR_REPORT_PORT: &str = "error-reports";

/// Whether `(instance, port)` names the reserved error-report output port. The
/// single executable definition of that predicate, shared by every site that
/// keys on the reserved port (the client gate, the core, and the server's
/// publish handler) so the wire meaning of "the reserved port" has one home and
/// cannot split across sites. Floor-aware callers gate this behind their
/// advertised-floor check.
pub fn is_error_report_port(instance: &str, port: &str) -> bool {
    instance == ERROR_REPORT_INSTANCE && port == ERROR_REPORT_PORT
}

/// The kernel's own wasm-bindgen `--target web` module artifact. Unlike component
/// modules (keyed by `kind` via [`module_artifact`]), the kernel is a single fixed
/// artifact every surface page references; this is its one canonical name, shared
/// by the page manifest and the boot asset-existence check.
pub const KERNEL_ARTIFACT: &str = "brenn_surface_kernel.js";

/// Whether a component `kind` or instance id matches the frozen
/// `^[a-z0-9][a-z0-9-]*$` charset **with no `--` run** — the invariant
/// [`element_name`]/[`element_name_for_instance`]/[`module_artifact`] depend on to
/// emit a valid custom-element name and filename. The single executable
/// definition of the rule the crate docs describe; callers enforcing it at boot
/// call here.
///
/// The `--` rejection is what makes [`element_name_for_instance`]'s separator
/// unambiguous: with consecutive hyphens permitted, `brenn-a--b--c` could split
/// two ways and the kind↦tag mapping would not be a function. No in-tree name
/// uses `--` and zero out-of-tree components exist, so the charset tightens
/// freely.
pub fn is_valid_kind(kind: &str) -> bool {
    let mut chars = kind.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_lowercase() || c.is_ascii_digit())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && !kind.contains("--")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_names_frozen() {
        assert_eq!(PORT_PUBLISH, "brenn-port-publish");
        assert_eq!(COMPONENT_LOG, "brenn-log");
        assert_eq!(COMPONENT_ALERT, "brenn-alert");
        assert_eq!(COMPONENT_PANIC, "brenn-component-panic");
        assert_eq!(SURFACE_RELOAD, "brenn-surface-reload");
        assert_eq!(SURFACE_READY, "brenn-surface-ready");
        assert_eq!(ACTIVATION_REGISTER, "brenn-activation-register");
    }

    #[test]
    fn publish_status_strings_round_trip() {
        // The kernel writes these and the SDK reads them across a wasm-module
        // boundary, so the two halves only agree if the mapping is one function.
        for status in [
            Ok(()),
            Err(PublishError::NotPermitted),
            Err(PublishError::InvalidPayload),
            Err(PublishError::QuotaExceeded),
        ] {
            assert_eq!(
                parse_publish_status(publish_status_str(status.clone())),
                Some(status)
            );
        }
        assert_eq!(PUBLISH_STATUS_FIELD, "status");
        assert_eq!(parse_publish_status("nope"), None);
        assert_eq!(parse_publish_status(""), None);
    }

    #[test]
    fn element_name_prefixes_kind() {
        assert_eq!(element_name("protobar"), "brenn-protobar");
        assert_eq!(element_name("echo-stub"), "brenn-echo-stub");
    }

    #[test]
    fn processor_module_path_is_the_kind_directory_entry() {
        assert_eq!(
            processor_module_path("transplant"),
            "processor/transplant/transplant.js"
        );
        // Dashes survive: a transpiled tree is named by the kind verbatim, unlike
        // wasm-bindgen's crate-name-derived artifact.
        assert_eq!(
            processor_module_path("echo-stub"),
            "processor/echo-stub/echo-stub.js"
        );
    }

    #[test]
    fn module_artifact_maps_dashes_to_underscores() {
        assert_eq!(module_artifact("protobar"), "brenn_protobar.js");
        assert_eq!(module_artifact("echo-stub"), "brenn_echo_stub.js");
    }

    #[test]
    fn error_report_instance_is_operator_unusable() {
        // The reserved error-report instance id must never pass the charset every
        // configured instance is validated against, so it can never collide with
        // an operator-configured instance. Pinned so a charset loosening cannot
        // silently make the reservation collidable.
        assert!(!is_valid_kind(ERROR_REPORT_INSTANCE));
        assert_eq!(ERROR_REPORT_INSTANCE, "#brenn");
        assert_eq!(ERROR_REPORT_PORT, "error-reports");
    }

    #[test]
    fn is_valid_kind_matches_frozen_charset() {
        assert!(is_valid_kind("protobar"));
        assert!(is_valid_kind("echo-stub"));
        assert!(is_valid_kind("a1"));
        assert!(is_valid_kind("9"));
        // Rejected: empty, uppercase, underscore, leading hyphen, dot, tilde.
        assert!(!is_valid_kind(""));
        assert!(!is_valid_kind("Echo"));
        assert!(!is_valid_kind("echo_stub"));
        assert!(!is_valid_kind("-echo"));
        assert!(!is_valid_kind("echo.stub"));
        assert!(!is_valid_kind("echo~stub"));
        // Rejected: a `--` run anywhere. This is what makes
        // `element_name_for_instance`'s separator the only `--` in a tag, so the
        // instance tag splits exactly one way.
        assert!(!is_valid_kind("echo--stub"));
        assert!(!is_valid_kind("a--"));
    }

    #[test]
    fn element_name_for_instance_is_collision_free() {
        assert_eq!(
            element_name_for_instance("protobar", "p1"),
            "brenn-protobar--p1"
        );
        assert_eq!(
            element_name_for_instance("echo-stub", "echo-stub"),
            "brenn-echo-stub--echo-stub"
        );
        // The pair every hyphen-based scheme gets wrong when `--` is legal:
        // ("a-b", "c") and ("a", "b-c") are distinct declarations and must not
        // share a tag. `is_valid_kind` forbids the `--` that would make them
        // collide, and the mapping keeps them apart regardless.
        assert_ne!(
            element_name_for_instance("a-b", "c"),
            element_name_for_instance("a", "b-c")
        );
    }

    #[test]
    fn bind_instance_export_name_frozen() {
        // The TS loader looks this up on the module object by name; a rename here
        // that the loader does not make is a component that never learns its
        // identity.
        assert_eq!(BIND_INSTANCE_EXPORT, "brenn_bind_instance");
    }
}
