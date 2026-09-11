//! The component contract: the `processor.wit` activation carrier, the rules a
//! component is owed under it, and the one scheduling predicate both hosts
//! share.
//!
//! An activation is the only delivery shape a component sees: every bound input
//! port of one instance, windowed, handed to the instance's entry in one call.
//! Both hosts mint it — the wasmtime host on the backend, the kernel on the
//! surface — and a component sees the same shape under either.
//!
//! An envelope is its canonical JSON text — the `envelope-json` of
//! `processor.wit` — at both placements, and [`ProcessorActivation`] /
//! [`ProcessorPortWindow`] are that one carrier, named here so neither host
//! re-declares it. The generic parameter `E` survives only so this crate's own
//! tests can window `&'static str` bodies; it has exactly one production
//! instantiation.
//!
//! This crate carries the *rules* as well as the shapes, and both hosts compile
//! against it. A rule written on one host's side only is a rule the other host's
//! author never reads as an obligation, which is how two hosts that both believe
//! they implement one component model come to differ in what they deliver. The
//! host conformance suite (`brenn-host-conformance`) is the executable half of
//! this text: a rule here that no scenario there drives is a rule only in prose.
//!
//! # The invariant
//!
//! > **There is one component model. Any component runs on any host that can
//! > satisfy its imports. Hosting eligibility is an import profile, not a
//! > component kind.**
//!
//! Every rule below is subordinate to that sentence, and it is the test a change
//! to either host has to pass. A component importing `store`/`mqtt`/`tools` is
//! backend-only; a component importing DOM capability is surface-only. Both are
//! the *same* rule reading a different import profile, not two kinds of thing.
//! Components see exactly one mechanism: **messages on named ports**.
//!
//! A behaviour one host has and the other does not is a bug unless it appears in
//! "Host-specific behaviours" below, with its reason. Silence here is not
//! "host-defined".
//!
//! # Delivery: the activation is the only shape
//!
//! A component on any hosting and any ABI sees exactly one delivery shape, the
//! **activation**: every bound input port windowed — retained context first, new
//! messages after, split by `new_from`, with a `dropped` delta — the whole thing
//! delivered by one call to the component's entry, publishes buffered during the
//! call and flushed atomically iff it returns ok.  There is no per-envelope
//! event, no drop marker, and no component-visible gap.
//!
//! The doctrine that shape encodes, because a port author must be able to read
//! it somewhere:
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
//!   capped at the binding's `push_depth`, arrives as **new**, not as context. So
//!   a message published before its consumer existed still reaches that consumer
//!   and still wakes it; a component may rely on `new` alone to catch up on
//!   attach. The symmetric cost is that a re-attach re-delivers what the
//!   component already folded, so a side-effecting fold owes itself at-most-once
//!   handling by `message_id`.
//! - **`dropped` is a counter, not a marker in the stream.** It is the delivery
//!   loss on that binding since the port's previous activation. The lost message
//!   itself is not gone: it remains visible as retained context in this or any
//!   later activation whose `retain_depth` still covers it. Recovery *is*
//!   retention — there is no gap-and-replay choreography and no terminal port
//!   failure. The carrier counts it in `u64` and the WIT world types it `u32`,
//!   so a host lowering an activation **saturates** at `u32::MAX` rather than
//!   refusing: a saturated count still says "you lost more than you can count",
//!   which is the whole of what a component does with the figure.
//! - **Err consumes.** The messages an activation was assembled for are acked
//!   when it is assembled, so returning err (or trapping) does not redeliver
//!   them; they reappear only as retained context.
//! - **Attach events are legitimately everything-is-new.** A page reload is the
//!   widest of them: cursors, rings and registrations die with the page, so
//!   everything in the first windows after a reload is new. A backend process
//!   restart is the same event on the other host, narrowed by whatever the
//!   durable store kept. A fresh attach that finds a ring already populated — the
//!   priming rule above — is the narrower one. Neither is a bug.
//! - **Pending activations coalesce.** An instance woken three times while it is
//!   running is run once afterwards, and the window's `new_from` shows what
//!   accumulated. Coalescing is the correct behaviour of a view, not a
//!   degradation path.
//! - **One instant per activation.** A host takes one clock reading per assembly
//!   and uses it for both the deferred-view boundary and the `now` the component
//!   is handed, so a component computing `now + delay` cannot park a message
//!   behind a view it was already shown.
//! - **Every mount gets one activation, guaranteed.** See below.
//!
//! # What a mount is, and what it is owed
//!
//! A **mount** is a host putting an instance into service:
//!
//! - **Surface:** the instance's registration, once a bindings document wires it.
//!   A registration made before the page's first document waits for that document
//!   ([`schedule::MountDebt::Unwired`]). A page reload is a new mount.
//! - **Backend:** the consumer task starting — at boot, and for every arriving or
//!   replaced consumer at reload converge. Positions exist before the task
//!   starts, so the debt is owed from the task's first instruction and there is
//!   no unwired state. A process restart is a new mount exactly as a page reload
//!   is.
//!
//! Every mount is owed exactly **one** activation, unconditionally. An activation
//! with nothing to deliver is otherwise never assembled; the **mount activation**
//! is the deliberate, once-per-mount exception. Its windows carry whatever
//! retained context and new messages exist — possibly nothing at all — and its
//! deferred windows ride along as always. It carries no marker: a component that
//! needs to know whether this is its first activation tracks that itself, and
//! most simply recompute from their windows.
//!
//! It exists so that a component's first output — its first state report, the
//! first tick of a deferred self-publish chain — has somewhere to come from that
//! is inside an activation, where the buffered publish seam and the deferred
//! ops live. A component cannot publish from its connect-time code, so an
//! activation that only happens when a bound channel happens to hold history is
//! not something a component can build on.
//!
//! Exactly one is delivered per mount. An instance that deregisters and registers
//! again, a consumer that is stopped and started, a restarted process: each is a
//! new mount and is owed a new one. A second bindings document mid-attachment is
//! not. The debt is settled **at assembly**, not at completion: an activation the
//! instance trapped in still happened, and the guarantee is one activation per
//! mount, not one successful one.
//!
//! [`schedule::readiness`] is that guarantee as machinery: both hosts ask it, and
//! neither keeps a gate of its own.
//!
//! # A deferred self-publish chain re-arms at mount only if it is empty
//!
//! There is no timer concept and no arming API. **A timer is a deferred
//! self-publish**: a component declares an `io` port, publishes its next tick to
//! itself with a `deliver_after` computed from the activation's own `now`, and
//! the tick arrives as an ordinary message on an ordinary input port.
//! Rescheduling and cancelling are the cancel/edit ops against the
//! [`DeferredWindow`] the activation is handed, so they ride the same flush rule:
//! an entry that errs schedules nothing.
//!
//! At every mount the component is shown its own parked messages and reconciles
//! from them. On a durable channel a tick parked before a restart survives it and
//! appears in the mount activation's deferred window; on an ephemeral or local
//! channel it does not and the window is empty. Either way the rule is the same
//! and the component never has to know which happened: **park a tick at mount iff
//! the deferred window holds none.** A chain that re-arms unconditionally runs at
//! twice its cadence after a restart; one that never re-arms is a component that
//! stops ticking after one.
//!
//! The window a host presents holds **every** entry the component parked on
//! that port that no release pass has taken — including one whose release
//! instant has already passed, carried with that past instant. "Parked" means
//! "not yet released", not "release time in the future", so an empty window
//! means no tick is standing at every instant, on either host, which is what
//! the rule above needs to be exact. A message parked before an outage whose
//! instant passed during it is therefore shown at the next mount, not hidden
//! until the host's release pass catches up. The edge that comes with it: such
//! an entry may be read but not cancelled or edited — the authority cutoff is
//! still the release instant — so a component that wants it gone waits for it
//! to arrive.
//!
//! # An instance's linear memory lives for one activation
//!
//! An instance's linear memory — statics, thread-locals, the heap, everything in
//! it — lives for **one activation**. A component MUST NOT carry state in it
//! from one activation to the next: no host promises that memory survives, and a
//! component that relies on it is silently wrong. Every host enforces it: the
//! backend builds a store and instantiates per activation, the browser page's
//! loader mints an instance inside its activation entry and drops it when the
//! entry returns, and `brenn-page-harness` instantiates per activation too — so
//! a kind's own suite, in or out of tree, catches a component that carries
//! state in memory before it is packaged.
//!
//! A component keeps state by **publishing it**: an `io` port bound
//! `push_depth = 0; retain_depth = 1` — sampled, so it never wakes its owner;
//! retained, so the newest state is always in that port's context window. The
//! component reads its state back out of that window at the top of every
//! activation and publishes the new one at the bottom. That is the same rule the
//! message bus states for state everywhere else: a retained channel *is* a state
//! variable, and there is no separate key-value store to reach for. The guest
//! SDK's `RetainedState` is the idiom in one line.
//!
//! **Host resources are owned by the mount, not by the memory.** A DOM element
//! handle on the surface, the KV store on the backend: a handle minted in one
//! activation names the same resource in the next, because the host's handle
//! tables live as long as the mount does. A handle is therefore ordinary state,
//! carried in a published state body like any other field — and a handle to a
//! resource the component has since destroyed traps on use, exactly as a stale
//! struct field would.
//!
//! # Host-specific behaviours
//!
//! The whole list. Each entry is a difference a component can observe, and each
//! has a reason that is about the host's substrate rather than about its author's
//! taste. Anything not here is one rule on both hosts.
//!
//! - **Sync activations (surface only).** A browser gesture needs its reply in
//!   the same task, while the user activation is live, so the surface can assemble
//!   and run an activation inside the event handler's own `dispatchEvent` and read
//!   a reply out of it. The backend has no such caller and mints only async
//!   activations.
//! - **The sync mount of a `dom`-granted surface instance.** An instance holding
//!   the `dom` grant is mounted synchronously by its registration: the host
//!   element is created by that registration and must be filled in the same task,
//!   so the browser never paints an empty host. Such a component sees
//!   `sync = Some(MOUNT_SYNC_PORT)` and the mount request in that port's window.
//!   **Every other mount, on either host, is the async shape** — `sync: None`, the
//!   ordinary assembly. A backend instance never holds `dom`, so every backend
//!   mount is the async one.
//! - **The ACL gate (backend only).** The backend is the trust plane and decides
//!   per port whether an instance may read a channel at all; a denied port
//!   windows empty. The surface's authority is its bindings document: a port it
//!   must not read is a port it does not bind.
//! - **Trap disposition.** The surface takes a trapped instance terminal and
//!   error-cards it; the backend quarantines the activation and carries on. One
//!   instance per activation removed the surface's *stated* reason (a poisoned
//!   memory) but not its real one: DOM effects are immediate and
//!   non-transactional, so a trapped rendering activation may leave a half-built
//!   subtree no later activation can be trusted to repair, and the error card is
//!   the page's honest state. The backend has no such irreversibility. This is
//!   the one deliberate host difference in error handling; everything else there
//!   — Err consumes, the flush rule, the side-effect gradient — is one rule.
//! - **Import profile.** `store`, `mqtt` and `tools` are backend-only; `dom` and
//!   `page-dom` are surface-only. This is the invariant working, not an exception
//!   to it: a component's hosting eligibility is exactly its import list.
//! - **`ephemeral:` bindings for backend consumers.** Not implementable today: a
//!   backend WASM consumer cannot bind an `ephemeral:` channel, because the
//!   registry forks on the address realm. A gap in the machinery rather than a
//!   rule about components, listed here so it is not read as one.

pub mod schedule;

/// One activation: every bound input port of one instance, windowed.
///
/// Every bound input port appears in **every** activation, in config (`inputs`)
/// order, whether or not it has new messages — a port with nothing new arrives
/// as a pure-context window. A component must not assume `ports.len() == 1`, and
/// must not assume a port's presence means that port is why it woke.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Activation<E> {
    /// One window per bound input port, in config order.
    pub ports: Vec<PortWindow<E>>,
    /// One deferred-window per bound output port, in config order — the
    /// component's own parked (deferred) messages on each output channel, a
    /// snapshot at drain. Separate from `ports`: a future in/out port appears in
    /// both lists, additively.
    pub deferred: Vec<DeferredWindow>,
    /// The host's wall clock at drain, epoch milliseconds UTC. Lets a guest
    /// compute an absolute future instant (e.g. for a deferred publish) without
    /// holding a clock of its own. `None` when the host exposes no UTC wall
    /// clock.
    pub now: Option<u64>,
    /// Name of the live sync port, when this is a **sync-call** activation:
    /// an ordinary activation plus a return obligation. `None` for an ordinary
    /// async one, which is every activation a message delivery causes.
    ///
    /// The named port appears in `ports` carrying exactly one envelope — the
    /// live request, `new_from == 0`, `dropped == 0` — so a component consumes
    /// it through the same window API as everything else. A sync port has no
    /// queue, no retention and no position: its window is always exactly the one
    /// request, and it appears at all only on the activation it caused. Every
    /// other bound port windows as usual, and the deferred windows ride along as
    /// usual, so the handler sees its full normal worldview.
    ///
    /// The obligation is the entry's return value: a sync-call activation may
    /// answer its caller with a reply, and an ordinary one may not.
    pub sync: Option<String>,
}

/// One output port's view onto its own parked messages: the component's
/// deferred publishes on that port's channel, ordered by `deliver_after`
/// ascending, snapshot at drain.
///
/// **Scoped to the component.** A window holds only messages this component
/// itself parked (its `wasm:<slug>` sender identity), never a peer's — the scope
/// is structural, so a shared output channel still shows each publisher only its
/// own schedule.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DeferredWindow {
    /// Logical output port name, as declared in config — never a raw channel
    /// address.
    pub port: String,
    /// This component's parked messages on the port's channel, soonest release
    /// first.
    pub entries: Vec<DeferredEntry>,
}

/// One parked message in a [`DeferredWindow`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DeferredEntry {
    /// Position within the window's `entries` list (which is release-ordered).
    /// The handle a future cancel/edit names; snapshot-relative, valid only
    /// against the window it arrived in.
    pub index: u32,
    /// The message body the component published, as handed to the deferred
    /// publish — not an envelope.
    ///
    /// A body rather than the activation's envelope type `E`: what a component
    /// gets back here is the same opaque string it handed the host, so this half
    /// of the activation carries the same shape on every hosting even where the
    /// input windows do not.
    pub payload: String,
    /// Scheduled release time, epoch milliseconds UTC.
    pub deliver_after: u64,
}

/// One input port's view onto its channel at activation time: retained context
/// followed by new messages.
///
/// **The port is a view, not a pipe.** `envelopes[..new_from]` is context —
/// messages already seen, still in the view because retention still covers them.
/// These are channel-wide most-recent messages, not a per-subscriber delivered
/// log: on a first window after (re)subscription the context may include
/// messages this component was never individually delivered. Seeing a message
/// again is not duplicate delivery; it is what "seen" means. A component needing
/// exactly-once tracks its own high-water by `message_id`.
///
/// **Attach is a delivery point.** A port whose queue has just come into
/// existence — a first or repeated registration, a binding added or rebound —
/// receives the channel's retained tail, capped at its `push_depth`, as **new**.
/// A message published before its consumer existed therefore still reaches and
/// still wakes that consumer, and `new` alone suffices to catch up on attach. The
/// cost of that symmetry is that a re-attach re-delivers what the component
/// already folded.
///
/// This is also why there is no gap vocabulary here: a message dropped from the
/// port's pending queue on overflow is still visible as context in this or any
/// later window that retention covers, so recovery is retention, not a marker.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PortWindow<E> {
    /// Logical input port name, as declared in config — never a raw channel
    /// address.
    pub port: String,
    /// Ordered oldest→newest: retained context, then new messages.
    pub envelopes: Vec<E>,
    /// Index of the first new message. `new_from == envelopes.len()` is a pure
    /// context window — nothing new on this port.
    pub new_from: u32,
    /// Messages that passed this port's position unserved since the previous
    /// activation consumed it. Nothing retires a message body: the bodies stay
    /// readable as context wherever retention covers them.
    ///
    /// Not a stored counter — the distance between the position and the oldest
    /// message the window served, so its reach is the position's reach. A
    /// durable channel persists the position, so a gap straddling a host
    /// restart is still reported after it; a non-durable channel dies with the
    /// process, so there `dropped == 0` is not proof of no-gap across one.
    /// Always 0 for a port whose `push_depth` is 0 — it holds no position and
    /// so can never be passed.
    pub dropped: u64,
}

/// One input port's window as both hosts carry it: the element is one canonical
/// `MessageEnvelope` serialized as JSON, the `envelope-json` of
/// `processor.wit`.
pub type ProcessorPortWindow = PortWindow<String>;

/// One activation as both hosts carry it — the carrier a component is handed at
/// either placement. See [`ProcessorPortWindow`] for what an element is.
pub type ProcessorActivation = Activation<String>;

impl<E> PortWindow<E> {
    /// The new messages on this port: `envelopes[new_from..]`. Empty for a
    /// pure-context window. This is the slice a component feeds to its seam;
    /// the `new_from` cast lives here so no consumer re-derives it.
    pub fn new_envelopes(&self) -> &[E] {
        &self.envelopes[self.new_from as usize..]
    }

    /// How many new messages this window carries: `envelopes.len() - new_from`.
    pub fn new_len(&self) -> u64 {
        (self.envelopes.len() as u64).saturating_sub(self.new_from as u64)
    }

    /// The newest new message, or `None` for a pure-context window.
    ///
    /// The whole fold for a **latest-wins** port — one whose state is fully
    /// described by its most recent message (a config snapshot, a theme, a
    /// layout document). On such a port message N+1 subsumes message N, so
    /// folding the older ones is work with no effect on the result, and in the
    /// failure direction it is worse than nothing: an invalid newest message
    /// leaves an older still-valid one applied, presenting stale state as
    /// current. Take the latest, and report a window that carried more than one
    /// with [`PortWindow::latest_wins_misconfiguration`].
    ///
    /// An event-stream port — where each message is its own fact — folds
    /// [`PortWindow::new_envelopes`] instead. Which one a port is, is the port
    /// author's decision and nothing here can infer it.
    pub fn latest_new(&self) -> Option<&E> {
        self.new_envelopes().last()
    }

    /// The operator-facing report for a latest-wins port handed more than one
    /// new message, or `None` when this window carries at most one.
    ///
    /// More than one new message on a latest-wins port means the binding's
    /// `push_depth` exceeds 1: coalescing to the latest is the subscription's
    /// job, and a binding that declines to do it makes every consumer redo it.
    /// The component still applies the latest and keeps working, so this is a
    /// normal error to the operator, never a panic and never an alert — the
    /// only place the fault is detectable, since latest-wins is component
    /// semantics no config layer knows.
    pub fn latest_wins_misconfiguration(&self) -> Option<String> {
        let new_len = self.new_len();
        if new_len <= 1 {
            return None;
        }
        Some(format!(
            "latest-wins port {:?} presented {} new messages; its binding's \
             push_depth should be 1 — coalescing to the latest is the \
             subscription's job, not the component's",
            self.port, new_len
        ))
    }
}

impl<E> Activation<E> {
    /// Total messages lost to push overflow across every bound port since each
    /// port's previous activation.
    pub fn total_dropped(&self) -> u64 {
        self.ports
            .iter()
            .fold(0u64, |acc, w| acc.saturating_add(w.dropped))
    }

    /// The live request's window on a sync-call activation — the [`Self::sync`]
    /// port's entry in `ports` — or `None` on an async one.
    ///
    /// The primitive under [`Self::sync_request`] and [`Self::delivered_windows`],
    /// for a component that wants the window itself rather than the request in it.
    ///
    /// Panics when `sync` names a port `ports` does not carry. The host assembles
    /// both halves together, so their disagreement is a host bug, and windowing a
    /// request that is not there is not a state to carry on from.
    pub fn sync_window(&self) -> Option<&PortWindow<E>> {
        let port = self.sync.as_deref()?;
        Some(
            self.ports
                .iter()
                .find(|window| window.port == port)
                .expect("a sync-call activation carries the window of the port it names"),
        )
    }

    /// The live request on a sync-call activation — the sync port's name and the
    /// one envelope carrying the request — or `None` on an async one.
    ///
    /// This is half of the gesture idiom; [`Self::delivered_windows`] is the
    /// other half. A component that answers gestures reads the request here and
    /// folds deliveries there, and never sees the request twice.
    ///
    /// Panics when the window carries other than exactly one new envelope. The
    /// host mints the request and windows it alone, so any other count is a host
    /// bug, and answering a gesture from the wrong request — or from none — is not
    /// a state to carry on from.
    pub fn sync_request(&self) -> Option<(&str, &E)> {
        let window = self.sync_window()?;
        let [request] = window.new_envelopes() else {
            panic!(
                "a sync-call activation's window on port {:?} carries the one live request, \
                 not {} of them",
                window.port,
                window.new_len()
            )
        };
        Some((window.port.as_str(), request))
    }

    /// Every window this activation *delivered*: its ports, minus the sync
    /// request's. The request is not a message anyone published, so it belongs in
    /// no delivery fold.
    ///
    /// The whole `ports` list on an async activation, so a component that folds
    /// through this reads the same worldview either way and cannot forget the
    /// exclusion the day it grows a gesture.
    pub fn delivered_windows(&self) -> impl Iterator<Item = &PortWindow<E>> {
        let sync = self.sync.as_deref();
        self.ports
            .iter()
            .filter(move |window| Some(window.port.as_str()) != sync)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The activation shape, pinned: field names, types, and the `new_from`
    /// split. This is the shape every component compiles against on either host,
    /// so a field added, renamed, or retyped is a deliberate edit to this test,
    /// never a silent drift.
    #[test]
    fn activation_shape_frozen() {
        let window = PortWindow {
            port: "agenda".to_string(),
            envelopes: vec!["seen-1", "seen-2", "new-1"],
            new_from: 2,
            dropped: 1,
        };
        let activation = Activation {
            ports: vec![window.clone()],
            deferred: vec![DeferredWindow {
                port: "reminders".to_string(),
                entries: vec![DeferredEntry {
                    index: 0,
                    payload: "ping".to_string(),
                    deliver_after: 1_700_000_060_000,
                }],
            }],
            now: Some(1_700_000_000_000),
            sync: None,
        };
        assert_eq!(activation.now, Some(1_700_000_000_000));
        assert_eq!(activation.sync, None);

        let DeferredWindow { port, entries } = &activation.deferred[0];
        assert_eq!(port, "reminders");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].index, 0u32);
        assert_eq!(entries[0].payload, "ping");
        assert_eq!(entries[0].deliver_after, 1_700_000_060_000u64);

        let PortWindow {
            port,
            envelopes,
            new_from,
            dropped,
        } = &activation.ports[0];
        assert_eq!(port, "agenda");
        assert_eq!(envelopes.len(), 3);
        assert_eq!(*new_from, 2u32);
        assert_eq!(*dropped, 1u64);
        assert_eq!(&envelopes[..*new_from as usize], &window.envelopes[..2]);

        let context_only = PortWindow {
            port: "clock".to_string(),
            envelopes: vec!["seen-1"],
            new_from: 1,
            dropped: 0,
        };
        assert_eq!(context_only.new_from as usize, context_only.envelopes.len());

        // Every bound port every activation, config order — the ports vec is the
        // whole bound set, not just the ports that woke the instance.
        let both = Activation {
            ports: vec![window, context_only],
            deferred: vec![],
            now: None,
            sync: None,
        };
        assert_eq!(both.ports.len(), 2);
        assert!(both.deferred.is_empty());
        assert_eq!(both.now, None);
    }

    /// The window/activation accessors: the `new_from` split, the `new_len`
    /// count (including the pure-context zero), and the whole-set `dropped` fold.
    #[test]
    fn accessors_split_count_and_fold() {
        let with_new = PortWindow {
            port: "messages".to_string(),
            envelopes: vec!["c-1", "c-2", "n-1", "n-2"],
            new_from: 2,
            dropped: 3,
        };
        assert_eq!(with_new.new_envelopes(), &["n-1", "n-2"]);
        assert_eq!(with_new.new_len(), 2);

        // The `saturating_sub` edge: `new_from == len`.
        let context_only = PortWindow {
            port: "clock".to_string(),
            envelopes: vec!["c-1"],
            new_from: 1,
            dropped: 4,
        };
        assert!(context_only.new_envelopes().is_empty());
        assert_eq!(context_only.new_len(), 0);

        // The shape a sync port's window always has, and a first delivery's.
        let all_new = PortWindow {
            port: "ack".to_string(),
            envelopes: vec!["n-1"],
            new_from: 0,
            dropped: 0,
        };
        assert_eq!(all_new.new_envelopes(), &["n-1"]);
        assert_eq!(all_new.new_len(), 1);

        let activation = Activation {
            ports: vec![with_new, context_only, all_new],
            deferred: vec![],
            now: None,
            sync: None,
        };
        assert_eq!(activation.total_dropped(), 7);
    }

    /// `sync_window` picks the named port out of `ports` — the request, not the
    /// first window and not a same-shaped delivery. A component uses it both to
    /// find the request and to skip it in its delivery loop, so picking the wrong
    /// one would fold a gesture as a publisher's message and act on a message as
    /// a gesture.
    #[test]
    fn the_sync_window_is_the_named_port_and_nothing_else() {
        fn window(port: &str, body: &'static str) -> PortWindow<&'static str> {
            PortWindow {
                port: port.to_string(),
                envelopes: vec![body],
                new_from: 0,
                dropped: 0,
            }
        }
        let mut activation = Activation {
            ports: vec![window("agenda", "snapshot"), window("ack", "dismiss")],
            deferred: vec![],
            now: None,
            sync: None,
        };
        assert!(
            activation.sync_window().is_none(),
            "an async activation has no request, however its ports are shaped"
        );

        activation.sync = Some("ack".to_string());
        let request = activation.sync_window().expect("the request is windowed");
        assert_eq!(request.port, "ack");
        assert_eq!(request.envelopes, vec!["dismiss"]);
    }

    /// The two halves of the gesture idiom against each other: the request comes
    /// out of the sync window, and the delivery fold sees every *other* window.
    /// Their disagreement is what makes a component act on its own press twice or
    /// fold it as a peer's publish.
    #[test]
    fn the_request_and_the_delivered_windows_partition_the_ports() {
        fn window(port: &str, body: &'static str) -> PortWindow<&'static str> {
            PortWindow {
                port: port.to_string(),
                envelopes: vec![body],
                new_from: 0,
                dropped: 0,
            }
        }
        let mut activation = Activation {
            ports: vec![window("agenda", "snapshot"), window("ack", "dismiss")],
            deferred: vec![],
            now: None,
            sync: None,
        };
        assert!(activation.sync_request().is_none());
        assert_eq!(
            activation
                .delivered_windows()
                .map(|w| w.port.as_str())
                .collect::<Vec<_>>(),
            vec!["agenda", "ack"],
            "an async activation delivered every one of its ports"
        );

        activation.sync = Some("ack".to_string());
        assert_eq!(activation.sync_request(), Some(("ack", &"dismiss")));
        assert_eq!(
            activation
                .delivered_windows()
                .map(|w| w.port.as_str())
                .collect::<Vec<_>>(),
            vec!["agenda"],
            "the request's window is not a delivery"
        );
    }

    /// A sync window carrying anything but the one minted request is a host that
    /// built the activation wrong. Taking the first would answer a gesture from a
    /// request the user did not make; taking none would answer from nothing.
    #[test]
    #[should_panic(expected = "carries the one live request")]
    fn a_sync_window_with_two_requests_is_a_host_bug() {
        let activation = Activation {
            ports: vec![PortWindow {
                port: "ack".to_string(),
                envelopes: vec!["dismiss", "snooze"],
                new_from: 0,
                dropped: 0,
            }],
            deferred: vec![],
            now: None,
            sync: Some("ack".to_string()),
        };
        let _ = activation.sync_request();
    }

    /// A `sync` naming a port no window carries is a host that assembled the two
    /// halves inconsistently. Reading it as "no request" would run a gesture entry
    /// with nothing to act on.
    #[test]
    #[should_panic(expected = "carries the window of the port it names")]
    fn a_sync_port_with_no_window_is_a_host_bug() {
        let activation = Activation {
            ports: vec![PortWindow {
                port: "agenda".to_string(),
                envelopes: vec!["snapshot"],
                new_from: 0,
                dropped: 0,
            }],
            deferred: vec![],
            now: None,
            sync: Some("ack".to_string()),
        };
        let _ = activation.sync_window();
    }

    /// The latest-wins fold: the newest new message and nothing else, and never a
    /// context message. Taking the last of `envelopes` instead of the last of the
    /// new slice would apply a message the component has already folded on every
    /// pure-context activation — which is every activation of an idle port.
    #[test]
    fn latest_new_takes_the_newest_new_message_only() {
        let with_new = PortWindow {
            port: "config".to_string(),
            envelopes: vec!["c-1", "n-1", "n-2"],
            new_from: 1,
            dropped: 0,
        };
        assert_eq!(with_new.latest_new(), Some(&"n-2"));

        let context_only = PortWindow {
            port: "config".to_string(),
            envelopes: vec!["c-1"],
            new_from: 1,
            dropped: 0,
        };
        assert_eq!(context_only.latest_new(), None);

        // An empty window is the same answer, without an index panic.
        let empty: PortWindow<&str> = PortWindow {
            port: "config".to_string(),
            envelopes: vec![],
            new_from: 0,
            dropped: 0,
        };
        assert_eq!(empty.latest_new(), None);
    }

    /// The misconfiguration report fires on >1 new and only on >1 new: one new
    /// message is the healthy case on a `push_depth = 1` binding, and a window of
    /// context plus one new must not be read as a burst.
    #[test]
    fn latest_wins_misconfiguration_reports_only_a_multi_new_window() {
        let one_new = PortWindow {
            port: "layout".to_string(),
            envelopes: vec!["c-1", "c-2", "n-1"],
            new_from: 2,
            dropped: 0,
        };
        assert_eq!(one_new.latest_wins_misconfiguration(), None);

        let context_only = PortWindow {
            port: "layout".to_string(),
            envelopes: vec!["c-1"],
            new_from: 1,
            dropped: 0,
        };
        assert_eq!(context_only.latest_wins_misconfiguration(), None);

        let three_new = PortWindow {
            port: "layout".to_string(),
            envelopes: vec!["c-1", "n-1", "n-2", "n-3"],
            new_from: 1,
            dropped: 0,
        };
        let report = three_new
            .latest_wins_misconfiguration()
            .expect("three new messages on a latest-wins port is a misconfiguration");
        assert!(report.contains("\"layout\""), "{report}");
        assert!(report.contains('3'), "{report}");
        assert!(report.contains("push_depth"), "{report}");
    }
}
