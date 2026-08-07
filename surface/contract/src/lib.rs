//! Component contract v1 — the kernel ↔ component seam.
//!
//! These names and shapes never cross the WS wire; they are the DOM-CustomEvent
//! contract between the surface kernel and the component modules it mounts, plus
//! the `window`-event seam between the kernel and the TS bootstrap. They live in
//! their own crate because they are the seam: contract surface as load-bearing
//! as the wire frames, which both the kernel and every component crate compile
//! against, and which out-of-tree component authors depend on directly. The wire
//! frames themselves are `brenn-surface-schema`; this crate depends on it for the
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
//! above. The set (`brenn_surface_schema::Abi`):
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
//! - **Kernel-originated, gestures-only in v1.** The one caller is the kernel's own
//!   DOM seam, on behalf of a component's gesture wiring: the reason the class
//!   exists is the same-task reply (a live gesture token, and the chance to
//!   suppress the browser's default action), which nothing but a gesture needs.
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
//! construction), publishes its next tick to itself with a `deliver_after` computed
//! from the activation's own `now` ([`PORT_DEFER`]), and the tick arrives as an
//! ordinary message on an ordinary input port. Rescheduling and cancelling are the
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
//! - **Connect-time (`connectedCallback`)**: render and set up only. Build the UI,
//!   install listeners, install gesture wirings. **No publish, no deferred op, no
//!   sync request** — the entry is not registered until connect-time code returns,
//!   so a sync request from there is refused and the requester faults. The mount
//!   activation is where a component's first output belongs.
//!
//! # The activation seam
//!
//! [`ACTIVATION_REGISTER`] is how a `dom` component joins activation delivery:
//! once per instance, from its element's first `connectedCallback`, it hands the
//! kernel an entry function; the kernel calls that entry once per activation with
//! the [`Activation`] as JSON and reads its return for the flush rule. See
//! [`ACTIVATION_REGISTER`] for the call convention.
//!
//! [`ACTIVATION_SYNC`] is how a browser event becomes an activation of the
//! instance whose element it fired on — the sync port class above, dispatched
//! synchronously so the whole activation completes inside the event handler.
//!
//! Publishes made from inside an entry are **buffered**: they ride the ordinary
//! [`PORT_PUBLISH`] event, and the kernel routes one to the in-flight buffer iff
//! the dispatching instance is the one whose entry is on the stack — activations
//! are serialized per instance and synchronous on the one JS thread, so exactly
//! one instance can be mid-activation. A buffered publish is answered
//! synchronously on the event detail's [`PUBLISH_STATUS_FIELD`]. A publish
//! dispatched from any other context is answered `not-permitted` there: it is a
//! publish with no activation to belong to, and there is no second path for it.
//!
//! The deferred-message ops — park a message for later, cancel one, edit one —
//! ride [`PORT_DEFER`] on the same routing rule and are buffered the same way,
//! because a schedule that escaped the flush-iff-ok boundary would outlive the
//! activation that failed to stage it.
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
//! - [`PORT_DEFER`] — a component's intent to park a message for later, or to
//!   cancel or edit one it already parked. Same dispatch rule and same identity
//!   resolution as [`PORT_PUBLISH`]; `detail = { op, port, index?, body?,
//!   deliver_after? }`, all strings. Buffered only — see [`PORT_DEFER`].
//! - [`ACTIVATION_SYNC`] — a component's request for a sync-call activation of
//!   itself. Same dispatch rule and same identity resolution as [`PORT_PUBLISH`],
//!   and answered synchronously on the detail. SDK↔kernel internal: a component
//!   author reaches it through a gesture wiring and never dispatches it by hand.
//! - [`COMPONENT_LOG`] — a component's intent to log. Same dispatch rule as
//!   [`PORT_PUBLISH`] (`bubbles: true, composed: true`, on the mounted element
//!   or from within its shadow root), so the kernel derives component identity
//!   from the retargeted `event.target` at the delegated `#surface-root`
//!   listener. `detail = { level, message }`; `level` is a lowercase log-level
//!   wire string (`"trace"`…`"error"`, see
//!   [`brenn_surface_schema::LogLevel::from_wire_str`]) fixed at the component
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
//!   [`brenn_surface_schema::AlertSeverity::from_wire_str`]) fixed at the
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
/// what happened, in four shapes:
///
/// - `undefined`/`null` → **ok**, with no reply. Every publish the entry
///   buffered is flushed, in call order.
/// - an object carrying a string [`ENTRY_REPLY_FIELD`] → **ok** with that reply,
///   flushed the same way. Legal only on an activation whose `sync` field names
///   a port: a reply on an async activation is an answer nobody asked for and is
///   read as a trap, since an entry that answered a question it was not asked
///   did not tell us it succeeded.
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

/// The field an activation entry's **return object** carries its sync reply on,
/// per [`ACTIVATION_REGISTER`]'s call convention.
///
/// The reply is an opaque string the kernel never parses; it hands it straight
/// back to the requester on [`SYNC_REPLY_FIELD`]. An object carrying no string
/// under this key is a non-conformant return and reads as a trap: a returned
/// object means "here is my answer", and one with no answer in it is gibberish
/// rather than an ok.
pub const ENTRY_REPLY_FIELD: &str = "reply";

/// The [`PORT_PUBLISH`] detail field the kernel writes the publish's answer into,
/// synchronously, before the dispatch returns.
///
/// Present on every publish the kernel heard. The dispatching instance is the one
/// whose entry is currently on the stack ⇒ the publish was routed into that
/// activation's buffer and this is the buffer's verdict; it is any other instance,
/// or none ⇒ [`PublishError::NotPermitted`], because a publish outside an
/// activation has no buffer to join and no path of its own. A *missing* status
/// means the event never reached the kernel's listener, which is a broken page
/// rather than an outcome.
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

// ── The deferred-message ops (component → kernel) ───────────────────────────

/// The [`PORT_DEFER`] detail's `op` field for a deferred publish: park `body` on
/// the port's channel until `deliver_after`.
pub const DEFER_OP_PUBLISH: &str = "publish";

/// The [`PORT_DEFER`] detail's `op` field for a cancel: unpark the message the
/// `index` names.
pub const DEFER_OP_CANCEL: &str = "cancel";

/// The [`PORT_DEFER`] detail's `op` field for an edit: rewrite the body and/or
/// the release time of the message the `index` names.
pub const DEFER_OP_EDIT: &str = "edit";

/// The [`PORT_DEFER`] detail field the kernel writes the op's answer into,
/// synchronously, before the dispatch returns — the deferred family's twin of
/// [`PUBLISH_STATUS_FIELD`].
///
/// Unlike the publish seam, the *vocabulary* on this field depends on the op,
/// because the WIT's does: a [`DEFER_OP_PUBLISH`] answers in
/// [`publish_status_str`]'s spellings (a deferred publish is a publish and adds no
/// error vocabulary), while [`DEFER_OP_CANCEL`] and [`DEFER_OP_EDIT`] answer in
/// [`defer_status_str`]'s. A caller knows which op it dispatched, so it knows
/// which parser to read the answer with.
///
/// Absent means the kernel did not route the op into an in-flight activation's
/// buffer. For this event that is not a second path but a refusal: every op here
/// is buffered-only, so the kernel drops and reports it (see [`PORT_DEFER`]).
pub const DEFER_STATUS_FIELD: &str = "status";

/// The [`DEFER_STATUS_FIELD`] wire string for a cancel's or an edit's answer. The
/// single executable definition of the values, shared by the kernel that writes
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

/// The inverse of [`defer_status_str`]: parse a [`DEFER_STATUS_FIELD`] value from
/// a cancel or an edit, or `None` for a string this contract never spells.
pub fn parse_defer_status(status: &str) -> Option<Result<(), DeferError>> {
    match status {
        "ok" => Some(Ok(())),
        "not-permitted" => Some(Err(DeferError::NotPermitted)),
        "out-of-range" => Some(Err(DeferError::OutOfRange)),
        "quota-exceeded" => Some(Err(DeferError::QuotaExceeded)),
        "invalid-deliver-after" => Some(Err(DeferError::InvalidDeliverAfter)),
        _ => None,
    }
}

// ── The sync-call seam (component → kernel) ─────────────────────────────────

/// Component → kernel, dispatched **synchronously** on the component's mounted
/// element. Must be `bubbles: true, composed: true`, exactly as
/// [`PORT_PUBLISH`]. `detail = { port, body }`, both strings: `port` names the
/// sync port the request arrives on, `body` is the component's own payload.
///
/// The kernel resolves *which* instance from the retargeted `event.target` over
/// its mounted-instance registry, never from the detail — the trust posture
/// [`ACTIVATION_REGISTER`] takes. A component cannot sync-activate any instance
/// but itself.
///
/// **What it causes.** One ordinary activation of that instance, assembled and
/// invoked inside this `dispatchEvent` call: every bound input port windowed as
/// usual, plus the minted request as the window named by
/// `Activation::sync`. Publishes and deferred-message ops buffer and flush on
/// the same terms as any other activation. Because the whole activation
/// completes before the dispatch returns, the browser's gesture token is still
/// live for the entry, and the caller can still `preventDefault()` on the
/// originating event afterwards.
///
/// **The answer** is written onto the same `detail` object before the dispatch
/// returns: [`SYNC_STATUS_FIELD`] always, plus [`SYNC_REPLY_FIELD`] on `ok` when
/// the entry answered with one and [`SYNC_ERROR_FIELD`] on `err`. There is no
/// second path and no unanswered case — a request the kernel would not admit is
/// answered [`SyncStatus::Refused`], which is always a bug in the requester or
/// the kernel.
///
/// SDK↔kernel internal: component authors reach this through the SDK's gesture
/// wiring and never dispatch it themselves.
pub const ACTIVATION_SYNC: &str = "brenn-activation-sync";

/// The [`ACTIVATION_SYNC`] detail field the kernel writes the request's outcome
/// into, synchronously, before the dispatch returns.
///
/// Always present on a request the kernel heard at all; the value is
/// [`sync_status_str`]'s wire string. A *missing* one means the event never
/// reached the kernel's listener, which is a broken page rather than an outcome.
pub const SYNC_STATUS_FIELD: &str = "status";

/// The [`ACTIVATION_SYNC`] detail field carrying the entry's reply, present only
/// alongside a [`SyncStatus::Ok`] status and only when the entry answered with
/// one. An opaque string: the kernel never parses it, and what it means is a
/// dialect between a component and its own gesture wiring.
pub const SYNC_REPLY_FIELD: &str = "reply";

/// The [`ACTIVATION_SYNC`] detail field carrying the [`ActivationError`] message,
/// present only alongside a [`SyncStatus::Err`] status.
///
/// Informational: the entry already saw its own error and the kernel already
/// counted the failed activation. It exists so a requester's diagnostic can name
/// what the entry said rather than only that it said something.
pub const SYNC_ERROR_FIELD: &str = "error";

/// How a sync-call request finished, as written on [`SYNC_STATUS_FIELD`].
///
/// Four values because the fourth is not an activation outcome at all: `Ok`,
/// `Err` and `Trap` are the three [`ActivationError`]-adjacent shapes an entry can
/// finish in, and `Refused` says no entry ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncStatus {
    /// The entry returned ok. Its buffer flushed, and [`SYNC_REPLY_FIELD`] carries
    /// the reply if it answered with one.
    Ok,
    /// The entry returned err. The buffer was discarded, a failure was counted,
    /// and the instance keeps running; [`SYNC_ERROR_FIELD`] carries its account.
    Err,
    /// The instance is terminal without having answered — its entry trapped, or
    /// the assembly's own loud-rung verdict killed it before the entry could run.
    ///
    /// Answered explicitly because the requesting closure survives the dispatch:
    /// the wasm instance is not torn down mid-stack even though the kernel has
    /// already marked it dead, so it has to be told to stop.
    Trap,
    /// The request was not admissible and nothing was assembled: it arrived from
    /// inside an activation, or named an instance that is unregistered, terminal
    /// or unwired, or its port name collides with a bound input port. Every one of
    /// those is a bug, never a configured outcome.
    Refused,
}

/// The [`SYNC_STATUS_FIELD`] wire string. The single executable definition of the
/// values, shared by the kernel that writes them and the SDK that reads them, so
/// the seam cannot drift by hand-copied literal.
pub fn sync_status_str(status: SyncStatus) -> &'static str {
    match status {
        SyncStatus::Ok => "ok",
        SyncStatus::Err => "err",
        SyncStatus::Trap => "trap",
        SyncStatus::Refused => "refused",
    }
}

/// The inverse of [`sync_status_str`]: parse a [`SYNC_STATUS_FIELD`] value, or
/// `None` for a string this contract never spells.
pub fn parse_sync_status(status: &str) -> Option<SyncStatus> {
    match status {
        "ok" => Some(SyncStatus::Ok),
        "err" => Some(SyncStatus::Err),
        "trap" => Some(SyncStatus::Trap),
        "refused" => Some(SyncStatus::Refused),
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
/// [`brenn_surface_schema::Urgency::parse`]), the component's per-message
/// override. Absent ⇒ the port's configured default applies, which the server
/// resolves. An unrecognized value is dropped and reported as malformed rather
/// than coerced — same rule as every other enum-valued detail field on this seam
/// (`level`, `severity`): silently downgrading a component's stated intent to
/// `normal` would be a fallback that hides the bug.
pub const PORT_PUBLISH: &str = "brenn-port-publish";

/// Component → kernel. Same dispatch rule as [`PORT_PUBLISH`] (`bubbles: true,
/// composed: true`, on the mounted element or from within its shadow root). One
/// event for the whole deferred-message family: park a message for later, cancel
/// one already parked, or edit one already parked.
///
/// `detail = { op, port, index?, body?, deliver_after? }`, every field a string:
///
/// - `op` — [`DEFER_OP_PUBLISH`], [`DEFER_OP_CANCEL`] or [`DEFER_OP_EDIT`].
/// - `port` — a bound output port of this instance, as everywhere on this seam.
/// - `index` — required by cancel and edit, unused by publish: the position of
///   the message in the [`DeferredWindow`] *this activation* delivered for the
///   port. Snapshot-relative, so an index from another activation is out of range
///   here, not a wrong message.
/// - `body` — required by publish; on an edit, present to replace the body and
///   absent to leave it alone.
/// - `deliver_after` — required by publish; on an edit, present to reschedule and
///   absent to leave the release time alone.
///
/// `index` and `deliver_after` are **decimal strings**, not JS numbers:
/// `deliver_after` is epoch milliseconds UTC as a `u64`, and every other value on
/// this seam is already a string, so a string keeps the boundary uniform and the
/// integer exact rather than routed through a float. An unparseable one is
/// malformed detail, dropped and reported like any other.
///
/// **Buffered only, with no second path.** Each op is answered synchronously on
/// [`DEFER_STATUS_FIELD`] by the in-flight activation's buffer: a schedule staged
/// outside an activation would escape the flush-iff-ok rule that makes a failed
/// activation schedule nothing. Dispatched with no activation of this instance in
/// flight, the op is dropped and reported.
///
/// **The timer idiom lives here.** A component's own periodic wakeup is a
/// deferred publish to itself on an in/out port, rescheduled or cancelled through
/// [`DEFER_OP_EDIT`] / [`DEFER_OP_CANCEL`] against the [`DeferredWindow`] the
/// activation delivered. See the crate-level timer idiom section for what that
/// buys and the one behavioral difference from a `setTimeout`.
pub const PORT_DEFER: &str = "brenn-port-defer";

/// Component → kernel. Same dispatch rule as [`PORT_PUBLISH`] (`bubbles: true,
/// composed: true`, on the mounted element or from within its shadow root).
/// `detail = { level, message }` where `level` is a lowercase log-level wire
/// string (see [`brenn_surface_schema::LogLevel::from_wire_str`]) and `message` a
/// string.
pub const COMPONENT_LOG: &str = "brenn-log";

/// Component → kernel. Same dispatch rule as [`PORT_PUBLISH`] (`bubbles: true,
/// composed: true`, on the mounted element or from within its shadow root).
/// `detail = { severity, title, body }` where `severity` is a lowercase
/// alert-severity wire string (see
/// [`brenn_surface_schema::AlertSeverity::from_wire_str`]) and `title`/`body` are
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
/// an instance's config map and its bindings row arrive in the bindings
/// document, i.e. after `start()`, and both the `config` import and registration
/// admission resolve against them. The kernel therefore names its processor instances once
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

/// First line of every in-tree help sidecar, which is generated rather than
/// hand-written: an HTML comment, so it is invisible in rendered markdown and
/// merely informative in the raw text an LLM reads. Nothing at runtime parses
/// it; the in-tree drift gate asserts a generator emits it, and the repo's
/// help-sidecar guard matches on the `<!-- AUTO-GENERATED` prefix.
pub const HELP_SIDECAR_HEADER: &str = "<!-- AUTO-GENERATED from this component's src/help.rs. Do not edit; run `make regen-surface-help`. -->\n";

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
        assert_eq!(PORT_DEFER, "brenn-port-defer");
    }

    #[test]
    fn defer_status_strings_round_trip() {
        // Same argument as the publish status: the kernel writes these and the SDK
        // reads them across a wasm-module boundary, so the halves agree only if the
        // mapping is one function.
        for status in [
            Ok(()),
            Err(DeferError::NotPermitted),
            Err(DeferError::OutOfRange),
            Err(DeferError::QuotaExceeded),
            Err(DeferError::InvalidDeliverAfter),
        ] {
            assert_eq!(
                parse_defer_status(defer_status_str(status.clone())),
                Some(status)
            );
        }
        assert_eq!(DEFER_STATUS_FIELD, "status");
        assert_eq!(parse_defer_status("nope"), None);
        assert_eq!(parse_defer_status(""), None);
        // The two vocabularies share a field and overlap on two spellings, so each
        // parser must refuse the other's exclusive ones rather than mapping them to
        // a neighbouring variant.
        assert_eq!(parse_defer_status("invalid-payload"), None);
        assert_eq!(parse_publish_status("out-of-range"), None);
        assert_eq!(parse_publish_status("invalid-deliver-after"), None);
    }

    #[test]
    fn defer_op_names_frozen() {
        // The op selector is read by the kernel's router and written by every SDK;
        // a rename on one side alone is a component whose schedules silently become
        // malformed detail.
        assert_eq!(DEFER_OP_PUBLISH, "publish");
        assert_eq!(DEFER_OP_CANCEL, "cancel");
        assert_eq!(DEFER_OP_EDIT, "edit");
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

    /// The four values are the seam's whole vocabulary and the SDK panics on a
    /// string it cannot parse, so writer and reader must agree letter for letter.
    #[test]
    fn every_sync_status_round_trips_through_its_wire_string() {
        for status in [
            SyncStatus::Ok,
            SyncStatus::Err,
            SyncStatus::Trap,
            SyncStatus::Refused,
        ] {
            assert_eq!(parse_sync_status(sync_status_str(status)), Some(status));
        }
        assert_eq!(sync_status_str(SyncStatus::Ok), "ok");
        assert_eq!(sync_status_str(SyncStatus::Refused), "refused");
        assert_eq!(parse_sync_status("not-permitted"), None);
        assert_eq!(parse_sync_status(""), None);
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
