//! The `processor.wit` activation carrier, declared once.
//!
//! An activation is the only delivery shape a component sees: every bound input
//! port of one instance, windowed, handed to the instance's entry in one call.
//! Both hosts mint it — the wasmtime host on the backend, the kernel on the
//! surface — and a component sees the same shape under either.
//!
//! The two hosts differ in exactly one respect: what an envelope *is* to them.
//! The wasmtime host lowers windows across the WIT boundary, where an envelope
//! is its JSON text; the surface kernel hands components the canonical typed
//! carrier. That is the generic parameter `E`, and it is the only thing this
//! crate declines to decide.

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
}

impl<E> Activation<E> {
    /// Total messages lost to push overflow across every bound port since each
    /// port's previous activation.
    pub fn total_dropped(&self) -> u64 {
        self.ports
            .iter()
            .fold(0u64, |acc, w| acc.saturating_add(w.dropped))
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
        // Two context envelopes then one new: `new_from` indexes the first new
        // message, so it is also the context length.
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
        };
        assert_eq!(activation.now, Some(1_700_000_000_000));

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

        // A pure-context window: nothing new, `new_from == envelopes.len()`.
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
        };
        assert_eq!(both.ports.len(), 2);
        assert!(both.deferred.is_empty());
        assert_eq!(both.now, None);
    }

    /// The window/activation accessors: the `new_from` split, the `new_len`
    /// count (including the pure-context zero), and the whole-set `dropped` fold.
    #[test]
    fn accessors_split_count_and_fold() {
        // A window with two context envelopes, two new, and a nonzero drop.
        let with_new = PortWindow {
            port: "messages".to_string(),
            envelopes: vec!["c-1", "c-2", "n-1", "n-2"],
            new_from: 2,
            dropped: 3,
        };
        assert_eq!(with_new.new_envelopes(), &["n-1", "n-2"]);
        assert_eq!(with_new.new_len(), 2);

        // A pure-context window: `new_from == len`, so no new messages and a
        // zero count — the `saturating_sub` edge.
        let context_only = PortWindow {
            port: "clock".to_string(),
            envelopes: vec!["c-1"],
            new_from: 1,
            dropped: 4,
        };
        assert!(context_only.new_envelopes().is_empty());
        assert_eq!(context_only.new_len(), 0);

        // `total_dropped` folds `dropped` across every port, not any other field.
        let activation = Activation {
            ports: vec![with_new, context_only],
            deferred: vec![],
            now: None,
        };
        assert_eq!(activation.total_dropped(), 7);
    }
}
