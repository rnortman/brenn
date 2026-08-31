//! The `processor.wit` activation carrier, declared once.
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

        // `new_from == 0`: every envelope is new and none is context — the shape a
        // sync port's window always has, and a first delivery's.
        let all_new = PortWindow {
            port: "ack".to_string(),
            envelopes: vec!["n-1"],
            new_from: 0,
            dropped: 0,
        };
        assert_eq!(all_new.new_envelopes(), &["n-1"]);
        assert_eq!(all_new.new_len(), 1);

        // `total_dropped` folds `dropped` across every port, not any other field.
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

        // Pure context: nothing new, so nothing to apply — the `None` an idle
        // port's activation yields.
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

        // Three new: the report names the port and the count, so the operator
        // knows which binding's push_depth to fix.
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
