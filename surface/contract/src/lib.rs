//! Component contract v1 — the kernel ↔ component seam.
//!
//! These names and shapes never cross the WS wire; they are the contract between
//! the surface kernel and the component instances it hosts, plus the
//! `window`-event seam between the kernel and the TS bootstrap. They live in
//! their own crate because they are the seam: contract surface as load-bearing
//! as the wire frames, which both the kernel and every component crate compile
//! against, and which out-of-tree component authors depend on directly. The wire
//! frames themselves are `brenn-surface-schema`; this crate depends on it for the
//! types the seam's details carry as JSON strings, never the other way round.
//!
//! Envelopes cross the kernel↔component boundary as JSON **strings**, so the
//! boundary stays serialization-clean across independently-built components. All
//! rendered text reaches the DOM as `textContent`, never `innerHTML`.
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
//! - **Attach is a delivery point.** When a port's queue comes into existence —
//!   the instance's first registration, a re-registration, a binding added or a
//!   port rebound by a later bindings document — the channel's retained tail,
//!   capped at
//!   the binding's `push_depth`, arrives as **new**, not as context. So a message
//!   published on a `local:` channel before its consumer existed still reaches
//!   that consumer and still wakes it; a component may rely on `new` alone to
//!   catch up on attach. Wire channels get the same thing from the server's
//!   fresh-attach replay. The symmetric cost is that a re-attach re-delivers what
//!   the component already folded, so a side-effecting fold owes itself
//!   at-most-once handling by `message_id`.
//! - **`dropped` is a counter, not a marker in the stream.** It is the delivery
//!   loss on that binding since the port's previous activation. The lost message
//!   itself is not gone: it remains visible as retained context in this or any
//!   later activation whose `retain_depth` still covers it. Recovery *is*
//!   retention — there is no gap-and-replay choreography and no terminal port
//!   failure.
//! - **Err consumes.** The messages an activation was assembled for are acked
//!   when it is assembled, so returning err (or trapping) does not redeliver
//!   them; they reappear only as retained context. That is backend parity.
//! - **Attach events are legitimately everything-is-new.** A page reload is the
//!   widest of them: cursors, rings and registrations die with the page, so
//!   everything in the first windows after a reload is new. A fresh attach that
//!   finds a ring already populated — the priming rule above — is the narrower
//!   one. Neither is a bug.
//! - **Every mount gets one activation, guaranteed.** An activation with nothing
//!   to deliver is otherwise never assembled; the **mount activation** is the
//!   deliberate, once-per-mount exception. It arrives as soon as the instance is
//!   both registered and wired (a registration made before the page's first
//!   bindings document waits for that document), it is an ordinary async
//!   activation in every respect, and its windows carry whatever retained context
//!   and new messages exist — possibly nothing at all. It carries no marker: a
//!   component that needs to know whether this is its first activation tracks that
//!   itself, and most simply recompute from their windows.
//!
//!   It exists so that a component's first output — its first state report, the
//!   first tick of a deferred self-publish chain — has somewhere to come from
//!   that is inside an activation, where the buffered publish seam and the
//!   deferred-message ops live. An activation that only happens when a bound
//!   channel happens to hold history is not something a component can build on.
//!   Exactly one is delivered per mount: an instance that deregisters and
//!   registers again is a new mount and is owed a new one; a second bindings
//!   document mid-attachment is not.
//!
//! Typed gaps (`EpochChanged`, `BeyondRetained`) survive only
//! at the websocket/resume layer, where the kernel handles them by re-resuming;
//! the component observes at most a first-window-after-resubscription. `GapReason`
//! is not part of the component seam.
//!
//! **Nothing a component sends stands outside the activation boundary.** A
//! browser event handler — click, input — causes an activation rather than
//! publishing beside one: it asks for a **sync-call activation** ([the sync port
//! class](#the-sync-port-class) below), which is assembled and run inside the
//! handler's own `dispatchEvent`. There is exactly one publish path for a
//! component, the in-flight activation's buffer, and the only other publisher on
//! the page is the kernel itself.
//!
//! # The sync port class
//!
//! A **sync port** carries one live request into one activation and carries a
//! reply back out of it. That is the whole class: a sync-call activation is the
//! ordinary activation shape plus a return obligation, which is why a component
//! reads its request through the same window API as everything else.
//!
//! - **One live request, no history.** A sync port has no queue, no retention and
//!   no position. Its window is exactly the one minted request (`new_from == 0`,
//!   `dropped == 0`), and it appears in `ports` only on the activation it caused.
//!   None of the gap/replay/`dropped` vocabulary above applies to it — there is no
//!   stream to have a view onto.
//! - **The rest of the activation is ordinary.** Every bound input port windows as
//!   usual, the deferred windows ride along, `now` is set, publishes buffer and
//!   flush iff the entry returns ok. A sync activation consumes queued input like
//!   any other, so no async activation follows it for input it already drained.
//! - **Kernel-originated in v1.** The two callers are the kernel's own gesture
//!   listeners, installed by a component's `dom.listen`, and the mount call
//!   ([`MOUNT_SYNC_PORT`]). A gesture is why the class exists: the same-task
//!   reply gives a live gesture token and the chance to suppress the browser's
//!   default action.
//!   Component-to-component sync bindings are not representable — the bindings
//!   document has no class field and no sync vocabulary — and are reserved for the
//!   follow-on that builds them.
//! - **Port names are the component's, and must not collide.** A component chooses
//!   a sync port name at request time; it is bound to nothing and configured
//!   nowhere. A name that collides with one of the instance's bound input ports is
//!   refused, because the activation's `ports` list must be unambiguous.
//! - **The request envelope.** Freshly minted by the kernel: sender is the
//!   requesting instance, `channel` is `local:brenn/sync/<port>` (see
//!   [`SYNC_CHANNEL_PREFIX`]) and `envelope_type` is `local`. Sync-ness is the
//!   activation's `sync` field and never a property of the envelope.
//! - **Ok returns before the wire does.** A sync `ok` says the entry returned and
//!   its buffer flushed into the page; a transportable publish in that buffer is
//!   committed by the server later, and while detached it parks until reconnect.
//!   An author who needs server confirmation waits for its own output channel to
//!   say so, exactly as in an async activation.
//!
//! # The timer idiom
//!
//! There is no timer concept on this seam, and no arming API. **A timer is a
//! deferred self-publish**: a component declares an in/out port
//! (`[[surface.io_port]]`, whose two halves resolve to one channel by
//! construction), publishes its next tick to itself with a `deliver_after`
//! computed from the activation's own `now`, and the tick arrives as an ordinary
//! message on an ordinary input port. Rescheduling and cancelling are the
//! cancel/edit ops against the [`DeferredWindow`] the activation is handed, so they
//! ride the same flush rule: an entry that errs schedules nothing.
//!
//! The behavioral delta worth naming: a tick dispatches as a **normal async
//! activation** — a later task, subject to the kernel's ordinary pacing — not
//! synchronously inside a `setTimeout` callback. At the cadences components
//! actually tick on (a minute boundary, a toast lifetime) that is immaterial, but
//! it is a difference, and a component that assumed same-task fire would be assuming
//! something this seam never promised.
//!
//! # The context rule
//!
//! Exactly three contexts exist in a component, and what may happen in each is
//! fixed:
//!
//! - **Inside an activation entry** (async- or sync-caused): the full worldview,
//!   the buffered publisher, the deferred ops, and rendering. Every decision that
//!   sends anything belongs here.
//! - **Browser event listeners**: encode and request, nothing else. A gesture
//!   wiring turns an event into a sync request; a non-publishing listener may of
//!   course just render or read. Publishing from a listener is impossible — no API
//!   exists — and a listener must not fire during an activation (a programmatic
//!   `element.click()` from inside an entry is refused as re-entrant).
//! - **Instantiation**: nothing. A component's module-level initialization runs
//!   before its registration is admitted, so it has no ports, no DOM handles and
//!   no kernel to talk to. The mount activation ([`MOUNT_SYNC_PORT`]) is where a
//!   rendering component builds its UI and where any component's first output
//!   belongs.
//!
//! # The activation seam
//!
//! A component joins activation delivery through its host, not through this
//! crate: the page's loader instantiates the component once per instance and
//! registers its `receive` export with the kernel, which then calls it once per
//! activation with the [`Activation`] as JSON and reads its return for the flush
//! rule. [`ENTRY_REPLY_FIELD`] is the one name that convention needs here — the
//! key a sync reply rides back on.
//!
//! The kernel mints a sync-call activation for two things: a gesture the
//! component asked for with `dom.listen`, and the mount call
//! ([`MOUNT_SYNC_PORT`]) a rendering instance gets as its first invocation. Both
//! run synchronously, so a gesture's whole activation completes inside the
//! browser's event handler.
//!
//! Publishes made from inside an entry are **buffered**: the component's
//! `ports.*` imports route into the in-flight activation's buffer, which flushes
//! iff the entry returns ok. A publish outside an activation has no buffer to
//! join and is refused; a component's world gives it no way to try.
//!
//! The deferred-message ops — park a message for later, cancel one, edit one —
//! are buffered the same way, because a schedule that escaped the flush-iff-ok
//! boundary would outlive the activation that failed to stage it.
//!
//! # The privileged entries (component → kernel)
//!
//! Delivery is not on this list: it is the direct entry call described above.
//! What a component reaches the other way is its own world's imports, each gated
//! on that instance's own grant word and each answered by the kernel:
//!
//! - `ports.*` — publish, publish-with-urgency, publish-deferred, defer-cancel,
//!   defer-edit. Components see **ports only** — logical config names — never
//!   channel addresses, mirroring the backend WASM port model for exact policy
//!   symmetry. Buffered into the in-flight activation, as above.
//! - `log.log` — one leveled line. The kernel stamps `source =
//!   "component:<instance>"` and forwards a `Log` frame.
//! - `alert.alert` — a page to an operator, forwarded as an `Alert` frame **only**
//!   for an instance granted `alert`; an ungranted one is dropped with a `warn`
//!   breadcrumb naming it.
//! - `config.get` — one key of the instance's own static config map, gated on
//!   `config`. The map is fixed for the page's lifetime, so a component may read
//!   it once at mount and keep the answer.
//! - `dom.*` / `page-dom.*` — the DOM capability. Element vocabulary is an
//!   allow-list ([`DOM_ALLOWED_TAGS`], [`DOM_ALLOWED_ATTRIBUTES`]); misuse traps.
//!
//! A component's death is not on this list either: it panics, the host catches
//! the trap, and the kernel error-cards that one instance. There is no way for a
//! component to report anyone else's death.
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
//! - [`PROCESSOR_START`] — `detail = { instances }` (an array of instance-id
//!   strings). The kernel names this page's component instances once its first
//!   bindings land, and the loader instantiates and registers each one.
//!
//! # Why the seam is imports and one entry call
//!
//! Each component is its own wasm component, because that is what contains a
//! trap to one component. The host holds every instance's call handles, so the
//! kernel calls a component's `receive` directly and a component calls the
//! kernel's entries directly; nothing rides an in-page message bus. Vocabulary is
//! ports and messages, and the import list underneath is plumbing a component is
//! never asked to understand as anything else.
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
//! | Deferred-message ops (park / cancel / edit) | Discarded — transactional |
//! | `store` writes (backend hosting) | Rolled back — transactional |
//! | `log` / `alert` / filesystem-`sync` tool imports | Immediate, unrollbackable |
//! | DOM mutation (surface hosting) | Immediate, unrollbackable |
//! | The sync-call reply (surface hosting) | Never sent — err and trap are answered as themselves |
//!
//! The `sync` in the third row is the backend's filesystem-sync tool import and
//! has nothing to do with the sync port class: they share a word and no
//! machinery.
//!
//! A sync-call requester learns the outcome either way — `err` carries the
//! entry's own account and `trap` says the instance is terminal — so an
//! unsuccessful activation cannot be read as a silent ok. What it cannot learn is
//! a reply, because an entry that did not return ok did not answer.
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
//!   [`brenn_surface_schema::RESERVED_LOCAL_CHANNELS`]; a `local:brenn/*` address
//!   absent from that table is undefined vocabulary and boot rejects it. The
//!   [`SYNC_CHANNEL_PREFIX`] family is the one address family inside
//!   `local:brenn/` that is deliberately *not* in the table: it names no routable
//!   channel, so its absence is what keeps it unbindable and unwritable.
//! - **`<prefix>.surface.<slug>.instance.<name>.config`** — the future
//!   per-instance runtime config channel. The address builder exists and its
//!   grammar is pinned by test; nothing publishes it, no `[[channel]]` block
//!   declares it, and it is not special-cased anywhere. The reservation is a
//!   naming fact, not machinery.
//!
//! # Light DOM and skinning
//!
//! Components render into the light DOM: the `dom` capability creates elements
//! under the instance's own host element and offers no way to attach a shadow
//! root. That is deliberate — `data-*` hooks plus global stylesheets are exactly
//! what make "new skin = one CSS file" cheap, and a shadow root is opaque to
//! them. CSS collisions in the light DOM are managed by component-prefixed
//! naming.
//!
//! # In-page separation is never a security boundary
//!
//! A surface component module runs **unsandboxed** in the authenticated page's
//! JS realm with the full authority of the logged-in page: the DOM, the session
//! WS channel, every other component's ports and rendered data. It is *not*
//! capability-gated the way a backend wasmtime guest is, so installing an
//! out-of-tree component trusts it with that full authority. Everything this
//! seam enforces — identity from the loader's own closure, the ungranted-alert
//! drop, the element allow-list, the reserved names — is **bug containment**: it
//! keeps an honest component's bug inside that component, and it stops nothing a
//! malicious module wants to do.
//!
//! Real enforcement is server-side, without exception: every effect a component
//! can have off this page travels through the kernel → WS → server gates, which
//! trust nothing the page says about itself.
//!
//! # Naming conventions
//!
//! A component's config `kind` names its slice of the served asset tree:
//! `processor/<kind>/` holds the transpiled module, its binding record and its
//! packaged specification (see [`processor_module_path`]), and the kind's
//! documentation sidecar ships flat beside it.
//!
//! `kind` is boot-validated to `^[a-z0-9][a-z0-9-]*$`, which is a valid filename
//! stem.
//!
//! # Instances
//!
//! One component `kind` may be hosted several times on one surface, each an
//! **instance** with its own id, its own module instantiation, and its own port
//! bindings. The instance is the principal: it owns the bindings, the send
//! budget, and the attribution, exactly as a backend `[[app]]` slug does. The
//! kind is the manifest — what the module needs — and holds no authority.
//!
//! Identity is never something a component states. The loader closes over the
//! instance it instantiated for and supplies it to every kernel entry; a
//! component has no way to name another instance and no need to name itself.
//!
//! # Mount and arrange
//!
//! Mounting and arranging are two jobs with two owners, and the boundary between
//! them is one element:
//!
//! - **The kernel mounts.** It creates one **wrapper** element per instance —
//!   `data-instance="<instance>"`, `data-kind="<kind>"` — and, for an instance
//!   granted `dom`, one plain host `div` inside it, which is what that
//!   instance's `dom.root` resolves to. The kernel owns the wrapper: the host
//!   element while the instance lives, an error card once it dies. The host
//!   element carries no stamp of its own, because it is handed to the component,
//!   which may write any `data-` name on it. Wrappers are born in a hidden
//!   kernel-owned staging container under `#surface-root`, and the kernel never
//!   moves one again. A wrapper declares `contain: paint`, which bounds an
//!   instance's own style declarations to its own box.
//! - **Chrome arranges.** Chrome reparents wrappers into its own layout sections
//!   and stamps layout state (`data-panel`, a panel label header) on wrappers and
//!   sections — **never inside a wrapper**. An instance no layout places is
//!   live, warm, and pumping, with no pixels; whether that is expressed by
//!   leaving it staged or by a section chrome hides is chrome's business, not the
//!   contract's.
//!
//! Reparenting preserves element identity, so an instance's host element and
//! every DOM handle it holds survive arrangement untouched. A component MUST NOT
//! assume it is arranged only once, and MUST NOT assume it is ever arranged at
//! all: it may run with no pixels for the whole page's life.
//!
//! Chrome holds a **page-DOM authority grant** (`page-dom`): it is the one
//! component that can touch DOM outside its own subtree — `body` attributes,
//! `#surface-root` attributes, and other instances' wrappers. Every other
//! instance is confined by construction: its handles all descend from its own
//! `dom.root`, and handle tables are per instance, so a foreign handle traps.
//!
//! Per-instance state is ordinary state in the component's own linear memory:
//! one instantiation per instance, page-lifetime, so a module-level static is
//! per instance too and cannot bleed across siblings.

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
/// where the host names it `ProcessorActivation` and carries the same envelope
/// JSON.
pub type Activation = brenn_activation::ProcessorActivation;

/// One input port's view onto its channel at activation time: retained context
/// followed by new messages. See [`brenn_activation::PortWindow`] for what the
/// fields mean — the port is a view, not a pipe.
///
/// An element is one canonical [`brenn_envelope::MessageEnvelope`] serialized as
/// JSON — the `envelope-json` of `processor.wit`, not a decoded struct.
pub type PortWindow = brenn_activation::ProcessorPortWindow;

/// The address family the kernel mints a sync-call activation's request envelope
/// on: `local:brenn/sync/<port>`.
///
/// **Never routed and never bindable.** The envelope exists only inside the
/// activation's sync window — it never enters a ring, never passes the router and
/// never crosses the wire — so these addresses are deliberately absent from
/// [`brenn_surface_schema::RESERVED_LOCAL_CHANNELS`], which enumerates the
/// *routable* reserved channels. Absence is the enforcement: boot rejects a
/// `local:brenn/*` binding the table does not name, and the kernel's plane policy
/// admits nobody to a reserved-but-undefined address, so a component can neither
/// bind nor write one and no new check was needed to make that true.
pub const SYNC_CHANNEL_PREFIX: &str = "local:brenn/sync/";

/// The channel a sync-call activation's request envelope carries for `port` —
/// [`SYNC_CHANNEL_PREFIX`] plus the port name.
///
/// Sync-ness is the activation's own `sync` field, never a property of the
/// envelope: the envelope's `envelope_type` is `local`, which is truthful on
/// every axis that field answers (page-local, never on the wire, no durable row).
pub fn sync_channel(port: &str) -> String {
    format!("{SYNC_CHANNEL_PREFIX}{port}")
}

/// One output port's view onto the messages this component itself has parked on
/// that port's channel, soonest release first.
///
/// Re-exported rather than aliased at an envelope type: a parked message is a body
/// and a release time, so the carrier is the same on every hosting.
pub use brenn_activation::{DeferredEntry, DeferredWindow};

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

/// Why a buffered deferred-message control op (cancel / edit) was refused,
/// returned synchronously to the component from inside its activation entry.
///
/// The `processor.wit` `defer-error` variants verbatim. Note what is *not* here: a
/// drain-vs-release race is not a refusal. The op names a message by an index into
/// the deferred window this activation was handed, and the message may release
/// before the flush applies it — the kernel logs and counts that and the op still
/// returned ok, because the component had already returned by the time the race
/// was resolvable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeferError {
    /// The port is not a bound output of this instance.
    NotPermitted,
    /// The index is outside the deferred window this activation delivered for the
    /// port. Snapshot-relative: an index valid against another activation's window
    /// is still out of range here. A component bug, so it fails while the
    /// component still holds the error channel.
    OutOfRange,
    /// A budget is exhausted — the per-activation call budget (shared with
    /// publishes), the buffered-op ceiling, or, for an edit that replaces the
    /// body, the publish-body cap or the activation's aggregate body bytes, both
    /// shared with publishes. An edit body is weighed exactly as a published body.
    QuotaExceeded,
    /// An edit's release time is not a representable timestamp. Refused here
    /// rather than left to collapse into an immediate release downstream.
    InvalidDeliverAfter,
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

/// The field an activation entry's **return object** carries its sync reply on.
///
/// The entry is a JS function the loader registers with the kernel; it returns
/// `undefined` for ok-with-no-reply, this object for ok-with-a-reply, a string
/// for err, and throws for a trap. The reply is an opaque string the kernel never
/// parses; it hands it straight back to whoever asked. An object carrying no
/// string under this key is a non-conformant return and reads as a trap: a
/// returned object means "here is my answer", and one with no answer in it is
/// gibberish rather than an ok.
pub const ENTRY_REPLY_FIELD: &str = "reply";

/// The WIT `publish-error` wire string for a buffered publish's answer. The
/// single executable definition of the values, shared by the kernel that writes
/// them and the guest glue that reads them, so the seam cannot drift by
/// hand-copied literal.
pub fn publish_status_str(status: Result<(), PublishError>) -> &'static str {
    match status {
        Ok(()) => "ok",
        Err(PublishError::NotPermitted) => "not-permitted",
        Err(PublishError::InvalidPayload) => "invalid-payload",
        Err(PublishError::QuotaExceeded) => "quota-exceeded",
    }
}

// ── The gesture seam (kernel → component, and back) ─────────────────────────

/// The gesture request's field naming the DOM event type that fired.
///
/// A gesture request is a body the kernel synthesizes and the guest SDK parses,
/// so its field names are a seam between two crates that cannot share a type:
/// the SDK is built for wasm32 and links none of this. They are spelled here,
/// pinned to their literals by `gesture_body_field_names_frozen`, and spelled
/// once more on the guest's `Gesture` struct.
pub const GESTURE_EVENT_FIELD: &str = "event";

/// The gesture request's field naming the node whose listener fired, as that
/// instance's own handle.
pub const GESTURE_LISTENER_FIELD: &str = "listener";

/// The gesture request's field naming the nearest handle-mapped ancestor of the
/// event's target — how a delegated listener tells apart which child was hit.
pub const GESTURE_TARGET_FIELD: &str = "target";

/// The one key a gesture reply has: `true` asks the kernel to `preventDefault`
/// the event that caused the activation, `false` lets it proceed.
///
/// A reply outside this dialect — including an empty object — is a component
/// talking to itself in two languages, and the reader faults on it rather than
/// reading it as "do not cancel". No reply at all is the other way to let the
/// event proceed.
pub const GESTURE_CANCEL_FIELD: &str = "cancel";

/// The port a mount activation names — the sync-call activation the kernel
/// mints for a `dom`-granted instance's first invocation, where it builds its
/// UI.
///
/// Reserved by its colon: no specification identifier can spell one, so the
/// name cannot collide with a bound input port. The guest SDK spells the same
/// string as `dom::MOUNT`, held to this constant by
/// `the_guest_half_of_the_mount_port_spells_the_same_string`.
pub const MOUNT_SYNC_PORT: &str = "brenn:mount";

// ── The `dom` element vocabulary ────────────────────────────────────────────

/// Every tag a `dom`-granted component may create. Anything else traps.
///
/// An allow-list, not a filter: containment is by construction, so the question
/// asked of a candidate entry is not "can this be abused" but "does this cause
/// script execution, navigation, a resource fetch, or any effect outside the
/// instance's own subtree" — a `no` on all four admits it, and nothing else
/// does. That rule excludes `script`, `iframe`, `object`, `embed`, `template`,
/// `meta`, `base`, `style`, `link`, `form` and `a` without enumerating them.
///
/// Growing the list is a one-line, reviewed change that travels with the WIT
/// doc, which is what an out-of-tree author reads.
pub const DOM_ALLOWED_TAGS: &[&str] = &[
    "blockquote",
    "br",
    "button",
    "code",
    "div",
    "em",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "header",
    "hr",
    "input",
    "li",
    "ol",
    "p",
    "pre",
    "s",
    "section",
    "span",
    "strong",
    "ul",
];

/// Every attribute name a `dom`-granted component may set, beyond the prefixes
/// of [`DOM_ALLOWED_ATTRIBUTE_PREFIXES`]. Anything else traps.
///
/// Admitted by the same rule as [`DOM_ALLOWED_TAGS`], which is why no
/// URL-bearing name and no `on*` name is here — and why attribute *values* are
/// never inspected: with no name that can carry a URL or a handler, there is
/// nothing in a value to parse.
///
/// `id` is absent deliberately. Ids live in the page-global namespace and the
/// surface's chrome resolves its own furniture through it, so an id set inside
/// a confined subtree is an effect outside that subtree.
pub const DOM_ALLOWED_ATTRIBUTES: &[&str] = &[
    "class",
    "disabled",
    "hidden",
    "placeholder",
    "role",
    "start",
    "title",
    "type",
];

/// The attribute-name prefixes a `dom`-granted component may set: the two
/// families that are inert by specification.
pub const DOM_ALLOWED_ATTRIBUTE_PREFIXES: &[&str] = &["aria-", "data-"];

/// Whether the host will create `tag`.
///
/// Exact ASCII-lowercase match against [`DOM_ALLOWED_TAGS`] — no case folding,
/// so a guest sending `DIV` traps rather than being quietly repaired. One
/// spelling per element keeps the contract's list and the DOM the component
/// gets in the same vocabulary.
pub fn dom_tag_allowed(tag: &str) -> bool {
    DOM_ALLOWED_TAGS.contains(&tag)
}

/// Whether the host will set an attribute named `name`, by the same exact-match
/// rule as [`dom_tag_allowed`] plus the allowed prefixes.
pub fn dom_attribute_allowed(name: &str) -> bool {
    DOM_ALLOWED_ATTRIBUTES.contains(&name)
        || DOM_ALLOWED_ATTRIBUTE_PREFIXES
            .iter()
            .any(|prefix| name.starts_with(prefix))
}

// ── The deferred-message ops (component → kernel) ───────────────────────────

/// The WIT `defer-error` wire string for a cancel's or an edit's answer.
///
/// A deferred *publish* is a publish and adds no error vocabulary, so it answers
/// in [`publish_status_str`]'s spellings instead; a caller knows which op it made,
/// so it knows which vocabulary to read the answer in.
///
/// The single executable definition of the values, shared by the kernel that writes
/// them and the SDK that reads them, so the seam cannot drift by hand-copied
/// literal.
pub fn defer_status_str(status: Result<(), DeferError>) -> &'static str {
    match status {
        Ok(()) => "ok",
        Err(DeferError::NotPermitted) => "not-permitted",
        Err(DeferError::OutOfRange) => "out-of-range",
        Err(DeferError::QuotaExceeded) => "quota-exceeded",
        Err(DeferError::InvalidDeliverAfter) => "invalid-deliver-after",
    }
}

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
/// an instance's config map and its bindings row arrive in the bindings
/// document, i.e. after `start()`, and both the `config` import and registration
/// admission resolve against them. The kernel therefore names its processor instances once
/// its first bindings land, and the loader answers — kernel-decided, exactly like
/// every other mount-plan outcome.
pub const PROCESSOR_START: &str = "brenn-processor-start";

// ── Naming conventions ─────────────────────────────────────────────────────

/// The id of the surface DOM root element. A page ↔ kernel contract point: the
/// backend page renders `<div id="surface-root">`, the kernel mounts components
/// and its banner inside it, and the TS bootstrap renders pre-kernel failures
/// into it. One definition all Rust consumers compile against.
pub const SURFACE_ROOT_ID: &str = "surface-root";

/// First line of every in-tree help sidecar, which is generated rather than
/// hand-written: an HTML comment, so it is invisible in rendered markdown and
/// merely informative in the raw text an LLM reads. Nothing at runtime parses
/// it; the in-tree drift gate asserts a generator emits it, and the repo's
/// help-sidecar guard matches on the `<!-- AUTO-GENERATED` prefix.
pub const HELP_SIDECAR_HEADER: &str =
    "<!-- AUTO-GENERATED from this component's src/help.rs. Do not edit. -->\n";

/// The jco-transpiled module path for a component `kind`, relative to the
/// surface asset root: `processor/<kind>/<kind>.js`.
///
/// A transpiled component is a directory — the entry JS plus one or more core
/// wasm files jco emits beside it, whose exact set is jco-version-dependent. The
/// entry module resolves its siblings relative to its own URL, so the directory
/// is the unit and this names only its entry point. The single home for the
/// layout the transpile rule writes and the page manifest reads.
pub fn processor_module_path(kind: &str) -> String {
    format!("{PROCESSOR_DIR}/{kind}/{kind}.js")
}

/// The one directory every transpiled kind is staged under, in the served tree
/// and in every installed surface root alike.
pub const PROCESSOR_DIR: &str = "processor";

/// The kind a `processor/<kind>/…` path names, or `None` for a path that is not
/// under [`PROCESSOR_DIR`] at all.
///
/// `path` is root-relative and carries no leading slash.
pub fn processor_kind_from_path(path: &str) -> Option<&str> {
    let rest = path.strip_prefix(PROCESSOR_DIR)?.strip_prefix('/')?;
    let kind = rest.split('/').next()?;
    (!kind.is_empty()).then_some(kind)
}

/// The `brenn_<kind with - → _>` stem a kind's documentation sidecars ship
/// under, flat in the surface asset root.
///
/// Documentation is served by kind-derived name and lives beside the kernel
/// rather than inside the kind's transpile directory, so the generator, the
/// staging rule and the description reader all derive it here.
pub fn sidecar_stem(kind: &str) -> String {
    format!("brenn_{}", kind.replace('-', "_"))
}

/// The kernel's own wasm-bindgen `--target web` module artifact. Unlike component
/// modules (keyed by `kind` under [`processor_module_path`]), the kernel is a
/// single fixed artifact every surface page references; this is its one canonical
/// name, shared by the page manifest and the boot asset-existence check.
pub const KERNEL_ARTIFACT: &str = "brenn_surface_kernel.js";

/// Whether a component `kind` or instance id matches the frozen
/// `^[a-z0-9][a-z0-9-]*$` charset **with no `--` run** — the invariant
/// [`processor_module_path`] depends on to emit a valid path segment and
/// filename. The single executable definition of the rule the crate docs
/// describe; callers enforcing it at boot call here.
///
/// The `--` rejection survives because a name with a `--` run reads as a
/// compound and nothing needs one: no in-tree name uses it and zero out-of-tree
/// components exist.
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
    fn processor_path_round_trips() {
        let path = processor_module_path("mode-clock");
        assert_eq!(path, "processor/mode-clock/mode-clock.js");
        assert_eq!(processor_kind_from_path(&path), Some("mode-clock"));
        assert_eq!(
            processor_kind_from_path("processor/echo-stub"),
            Some("echo-stub")
        );
        assert_eq!(
            processor_kind_from_path("processor/echo-stub/"),
            Some("echo-stub")
        );
        assert_eq!(processor_kind_from_path("processor/"), None);
        assert_eq!(processor_kind_from_path("processor"), None);
        assert_eq!(processor_kind_from_path("brenn_surface_kernel.js"), None);
        assert_eq!(processor_kind_from_path("processors/x/y"), None);
    }

    #[test]
    fn event_names_frozen() {
        assert_eq!(SURFACE_RELOAD, "brenn-surface-reload");
        assert_eq!(SURFACE_READY, "brenn-surface-ready");
        assert_eq!(PROCESSOR_START, "brenn-processor-start");
        assert_eq!(ENTRY_REPLY_FIELD, "reply");
    }

    #[test]
    fn defer_status_strings_are_frozen() {
        // Same argument as the publish status: the kernel writes these and the
        // guest glue lifts them across the component boundary, so the two halves
        // agree only if the mapping is one function.
        assert_eq!(defer_status_str(Ok(())), "ok");
        assert_eq!(
            defer_status_str(Err(DeferError::NotPermitted)),
            "not-permitted"
        );
        assert_eq!(
            defer_status_str(Err(DeferError::OutOfRange)),
            "out-of-range"
        );
        assert_eq!(
            defer_status_str(Err(DeferError::QuotaExceeded)),
            "quota-exceeded"
        );
        assert_eq!(
            defer_status_str(Err(DeferError::InvalidDeliverAfter)),
            "invalid-deliver-after"
        );
    }

    #[test]
    fn gesture_body_field_names_frozen() {
        // The kernel writes this body and a guest SDK it cannot link parses it;
        // a rename on one side alone is a gesture that silently stops carrying
        // its target.
        assert_eq!(GESTURE_EVENT_FIELD, "event");
        assert_eq!(GESTURE_LISTENER_FIELD, "listener");
        assert_eq!(GESTURE_TARGET_FIELD, "target");
        assert_eq!(GESTURE_CANCEL_FIELD, "cancel");
    }

    /// The other half of the seam, which cannot assert itself.
    ///
    /// The guest SDK is built for wasm32, links none of this crate, and has no
    /// test target of its own, so its `Gesture` field names and its cancel key
    /// are independent literals held to these constants by nothing but prose.
    /// Its source is compile data here, and the needles are built from the
    /// constants: rename either side alone and this fails, instead of every
    /// gesture body silently losing a field the day the kernel synthesizer
    /// lands.
    #[test]
    fn the_guest_half_of_the_gesture_seam_spells_the_same_names() {
        const GUEST: &str = include_str!("../../../brenn-wasm/components/guest/src/lib.rs");
        for field in [
            GESTURE_EVENT_FIELD,
            GESTURE_LISTENER_FIELD,
            GESTURE_TARGET_FIELD,
        ] {
            let needle = format!("pub {field}: ");
            assert!(
                GUEST.contains(&needle),
                "the guest's `Gesture` declares no `{needle}`; the two halves of the gesture                  body have drifted"
            );
        }
        let key = format!("const CANCEL_KEY: &str = \"{GESTURE_CANCEL_FIELD}\";");
        assert!(
            GUEST.contains(&key),
            "the guest spells no `{key}`; the two halves of the reply dialect have drifted"
        );
    }

    #[test]
    fn the_guest_half_of_the_mount_port_spells_the_same_string() {
        // The kernel mints mount activations on this port and the guest matches
        // on its own copy; a drift means every migrated UI kind renders nothing,
        // silently, in the stack CI does not run.
        const GUEST: &str = include_str!("../../../brenn-wasm/components/guest/src/lib.rs");
        let needle = format!("pub const MOUNT: SyncPort = SyncPort(\"{MOUNT_SYNC_PORT}\");");
        assert!(
            GUEST.contains(&needle),
            "the guest spells no `{needle}`; the two halves of the mount port have drifted"
        );
        // The colon is what reserves it: `assemble_sync` panics on a sync port
        // that collides with a bound input port, and no specification identifier
        // can spell one.
        assert!(MOUNT_SYNC_PORT.contains(':'));
    }

    #[test]
    fn the_dom_vocabulary_admits_only_what_it_lists() {
        assert!(dom_tag_allowed("div"));
        assert!(dom_tag_allowed("h6"));
        // Exact ASCII-lowercase, not case-folded: a guest that shouts traps.
        assert!(!dom_tag_allowed("DIV"));
        for refused in [
            "script", "iframe", "object", "embed", "template", "meta", "base", "style", "link",
            "form", "a", "",
        ] {
            assert!(!dom_tag_allowed(refused), "`{refused}` is creatable");
        }
        assert!(dom_attribute_allowed("class"));
        assert!(dom_attribute_allowed("data-instance"));
        assert!(dom_attribute_allowed("aria-label"));
        // The prefix is a prefix, not a substring.
        assert!(!dom_attribute_allowed("x-data-instance"));
        for refused in [
            "onclick", "ONCLICK", "srcdoc", "href", "src", "id", "style", "",
        ] {
            assert!(!dom_attribute_allowed(refused), "`{refused}` is settable");
        }
    }

    /// The lists are contract vocabulary an out-of-tree author reads off the WIT
    /// interface doc, so the doc is data of this test rather than prose beside
    /// the constants: the two cannot drift.
    #[test]
    fn the_wit_doc_carries_the_dom_vocabulary_verbatim() {
        const WIT: &str = include_str!("../../../brenn-wasm/wit/processor.wit");
        const TAGS_MARKER: &str = "Allowed tags";
        const ATTRS_MARKER: &str = "Allowed attribute names";
        const END_MARKER: &str = "interface dom {";

        fn quoted(region: &str) -> Vec<&str> {
            region.split('`').skip(1).step_by(2).collect()
        }
        fn region<'a>(wit: &'a str, from: &str, to: &str) -> &'a str {
            let start = wit.find(from).unwrap_or_else(|| {
                panic!("the WIT carries no `{from}` heading for the dom vocabulary")
            });
            let rest = &wit[start..];
            let end = rest
                .find(to)
                .unwrap_or_else(|| panic!("no `{to}` after `{from}` in the WIT"));
            &rest[..end]
        }

        let mut tags = quoted(region(WIT, TAGS_MARKER, ATTRS_MARKER));
        tags.sort_unstable();
        let mut expected_tags = DOM_ALLOWED_TAGS.to_vec();
        expected_tags.sort_unstable();
        assert_eq!(
            tags, expected_tags,
            "the WIT's tag list and DOM_ALLOWED_TAGS have drifted"
        );

        let mut names = quoted(region(WIT, ATTRS_MARKER, END_MARKER));
        names.sort_unstable();
        let mut expected_names = DOM_ALLOWED_ATTRIBUTES.to_vec();
        expected_names.extend_from_slice(DOM_ALLOWED_ATTRIBUTE_PREFIXES);
        expected_names.sort_unstable();
        assert_eq!(
            names, expected_names,
            "the WIT's attribute list and DOM_ALLOWED_ATTRIBUTE{{S,_PREFIXES}} have drifted"
        );
    }

    #[test]
    fn publish_status_strings_are_frozen() {
        // The kernel writes these and the guest glue lifts them across the
        // component boundary, so the two halves only agree if the mapping is one
        // function.
        assert_eq!(publish_status_str(Ok(())), "ok");
        assert_eq!(
            publish_status_str(Err(PublishError::NotPermitted)),
            "not-permitted"
        );
        assert_eq!(
            publish_status_str(Err(PublishError::InvalidPayload)),
            "invalid-payload"
        );
        assert_eq!(
            publish_status_str(Err(PublishError::QuotaExceeded)),
            "quota-exceeded"
        );
    }

    #[test]
    fn sync_channel_addresses_sit_under_the_reserved_prefix() {
        assert_eq!(SYNC_CHANNEL_PREFIX, "local:brenn/sync/");
        assert_eq!(sync_channel("ack"), "local:brenn/sync/ack");
        // The reservation works by *absence*: boot rejects a `local:brenn/*`
        // binding the routable table does not name, so a sync address staying out
        // of that table is what makes it unbindable.
        assert!(
            !brenn_surface_schema::RESERVED_LOCAL_CHANNELS
                .iter()
                .any(|channel| channel.address.starts_with(SYNC_CHANNEL_PREFIX)),
            "a sync address is never a routable channel"
        );
    }

    #[test]
    fn sidecar_stem_underscores_a_hyphenated_kind() {
        // The generator, the staging rule and the description reader all reach a
        // kind's documentation by this name; the served tree spells it by hand.
        assert_eq!(sidecar_stem("chrome"), "brenn_chrome");
        assert_eq!(sidecar_stem("mode-clock"), "brenn_mode_clock");
        assert_eq!(sidecar_stem("echo-stub"), "brenn_echo_stub");
    }

    /// The other half of the sidecar seam, which the function cannot assert.
    ///
    /// A kind's `help.md` is staged under a basename spelled by hand at its
    /// stage, and resolved at serve time through [`sidecar_stem`]. A
    /// disagreement is a warning and a stub help page, not a failure, so the
    /// served file set is data of this test: a staged name no kind's stem
    /// spells fails here instead of silently unpublishing that kind's
    /// documentation.
    #[test]
    fn every_served_sidecar_is_named_for_the_kind_it_documents() {
        const PATHS: &str = include_str!("../../dist-paths.txt");
        let kinds: Vec<&str> = PATHS
            .lines()
            .filter_map(|line| line.strip_prefix("processor/"))
            .filter_map(|rest| rest.split('/').next())
            .collect();
        assert!(kinds.contains(&"mode-clock"), "{kinds:?}");
        let mut documented = 0;
        for line in PATHS.lines() {
            let Some(stem) = line.strip_suffix(".help.md") else {
                continue;
            };
            documented += 1;
            assert!(
                kinds.iter().any(|kind| sidecar_stem(kind) == stem),
                "`{line}` is served under a stem no staged kind spells; the \
                 description endpoint resolves it for nobody"
            );
        }
        assert!(documented > 0, "no sidecar is served at all");
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
        // Rejected: a `--` run anywhere.
        assert!(!is_valid_kind("echo--stub"));
        assert!(!is_valid_kind("a--"));
    }
}
