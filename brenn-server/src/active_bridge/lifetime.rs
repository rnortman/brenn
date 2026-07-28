//! Who is still holding this bridge open.
//!
//! A bridge can be wanted by more than one party — a browser and a bus peer —
//! and a conversation can be wanted by a peer that has no websocket and never
//! will. Each party is a door with its own policy, and keep-alive is the OR:
//! the bridge lives while *any* door holds it, and drains when the last hold
//! lets go.
//!
//! A policy is a pure decision over a [`LifetimeState`] the arbiter assembles;
//! no policy owns state of its own. Adding a third door is a policy plus a field
//! on the state, not a new branch at each of the four decision points.

use std::time::Duration;

use tokio::time::Instant;
use tracing::debug;

/// What the doors have last seen, at the moment being judged.
pub(in crate::active_bridge) struct LifetimeState {
    /// Users with at least one websocket connection attached right now.
    pub subscribers: usize,
    /// When the last websocket connection detached. `None` = none ever has,
    /// which is the shape of a bridge no browser has ever visited.
    pub last_detach: Option<Instant>,
    /// When a bus peer last drove this conversation. `None` = none ever has.
    pub last_bus_activity: Option<Instant>,
    /// The moment being judged.
    pub now: Instant,
}

/// One door's claim on a bridge's life.
pub(in crate::active_bridge) trait BridgeLifetimePolicy:
    Send + Sync
{
    /// Which door this is, for the keep-alive log line that names the holders. A
    /// drain names none — by then the holders list is empty, and which hold was
    /// the last one is only recoverable from the keep-alive lines before it.
    fn name(&self) -> &'static str;

    /// Whether this door is holding the bridge open at `state.now`.
    fn holds_open(&self, state: &LifetimeState) -> bool;

    /// How long this door's hold has left, when it is one that runs out by
    /// itself. `None` means the passage of time will not end it — something has
    /// to happen first, so there is no moment worth setting a timer for.
    fn expires_in(&self, state: &LifetimeState) -> Option<Duration>;
}

/// The browser door.
///
/// An ephemeral app's hold is exactly its attached connections. A persistent
/// app's outlives them by `idle_timeout`, so closing a tab and opening it again
/// does not pay a CC spawn.
pub(in crate::active_bridge) struct WebsocketLifetime {
    /// Grace after the last tab closes. `None` = ephemeral: the hold ends with
    /// the last connection.
    idle_timeout: Option<Duration>,
}

impl WebsocketLifetime {
    /// What is left of the post-detach grace, when there is any. `None` covers
    /// all three ways there is none: an ephemeral app, a bridge no browser has
    /// visited, and an elapsed grace.
    fn grace_left(&self, state: &LifetimeState) -> Option<Duration> {
        let timeout = self.idle_timeout?;
        let since = state.now.saturating_duration_since(state.last_detach?);
        timeout.checked_sub(since).filter(|left| !left.is_zero())
    }
}

impl BridgeLifetimePolicy for WebsocketLifetime {
    fn name(&self) -> &'static str {
        "websocket"
    }

    fn holds_open(&self, state: &LifetimeState) -> bool {
        state.subscribers > 0 || self.grace_left(state).is_some()
    }

    fn expires_in(&self, state: &LifetimeState) -> Option<Duration> {
        // An attached tab holds indefinitely: only a detach ends it, and a
        // detach re-asks on its own.
        if state.subscribers > 0 {
            return None;
        }
        self.grace_left(state)
    }
}

/// The bus door.
///
/// A driver like a voice assistant interacts in bursts with dead air between
/// them, and paying a CC spawn per burst is the cost this exists to avoid. The
/// hold runs from the last interaction, so a conversation being driven never
/// stops being held.
pub(in crate::active_bridge) struct BusLifetime {
    idle_timeout: Duration,
}

impl BusLifetime {
    fn idle_left(&self, state: &LifetimeState) -> Option<Duration> {
        let since = state
            .now
            .saturating_duration_since(state.last_bus_activity?);
        self.idle_timeout
            .checked_sub(since)
            .filter(|left| !left.is_zero())
    }
}

impl BridgeLifetimePolicy for BusLifetime {
    fn name(&self) -> &'static str {
        "bus"
    }

    fn holds_open(&self, state: &LifetimeState) -> bool {
        self.idle_left(state).is_some()
    }

    fn expires_in(&self, state: &LifetimeState) -> Option<Duration> {
        self.idle_left(state)
    }
}

/// What the doors, together, say about a bridge.
#[derive(Debug, PartialEq)]
pub(in crate::active_bridge) enum Verdict {
    /// At least one door holds the bridge. `recheck` is how long until the
    /// soonest of the current holds runs out, when any of them does — the moment
    /// worth asking again. `None` means no hold expires on its own, so a timer
    /// would fire on a decision nothing had changed.
    KeepAlive { recheck: Option<Duration> },
    /// Nothing holds it.
    Drain,
}

/// The doors a bridge has, and what each has last seen.
///
/// The state lives here rather than in the policies so that a policy stays a
/// pure function of the moment and can be judged in a test without a bridge.
pub(in crate::active_bridge) struct LifetimeArbiter {
    policies: Vec<Box<dyn BridgeLifetimePolicy>>,
    /// `std::sync::Mutex`: every touch is a read or a stamp of an `Instant`,
    /// never held across an `.await`.
    last_detach: std::sync::Mutex<Option<Instant>>,
    last_bus_activity: std::sync::Mutex<Option<Instant>>,
}

impl LifetimeArbiter {
    /// The browser door is always there — every conversation can be attached to.
    /// The bus door exists where the deployment has a bus at all; without one no
    /// peer can ever drive this conversation, so a policy for it would answer
    /// `false` forever.
    pub(in crate::active_bridge) fn new(
        websocket_idle_timeout: Option<Duration>,
        bus_idle_timeout: Option<Duration>,
    ) -> Self {
        let mut policies: Vec<Box<dyn BridgeLifetimePolicy>> = vec![Box::new(WebsocketLifetime {
            idle_timeout: websocket_idle_timeout,
        })];
        if let Some(idle_timeout) = bus_idle_timeout {
            policies.push(Box::new(BusLifetime { idle_timeout }));
        }
        Self {
            policies,
            last_detach: std::sync::Mutex::new(None),
            last_bus_activity: std::sync::Mutex::new(None),
        }
    }

    /// The last websocket connection just went away.
    pub(in crate::active_bridge) fn note_detach(&self) {
        *self.last_detach.lock().expect("last_detach lock poisoned") = Some(Instant::now());
    }

    /// A bus peer just drove this conversation.
    pub(in crate::active_bridge) fn note_bus_activity(&self) {
        *self
            .last_bus_activity
            .lock()
            .expect("last_bus_activity lock poisoned") = Some(Instant::now());
    }

    /// Ask every door, at this moment, with this many attached users.
    ///
    /// The caller supplies the subscriber count because it is holding the
    /// subscribers lock: the drain decision and the `drain_on_idle` write it
    /// leads to have to be one atomic step, or a tab that attaches between them
    /// attaches to a bridge already condemned.
    pub(in crate::active_bridge) fn verdict(&self, subscribers: usize) -> Verdict {
        let state = LifetimeState {
            subscribers,
            last_detach: *self.last_detach.lock().expect("last_detach lock poisoned"),
            last_bus_activity: *self
                .last_bus_activity
                .lock()
                .expect("last_bus_activity lock poisoned"),
            now: Instant::now(),
        };

        let mut soonest: Option<Duration> = None;
        // A hold that the passage of time cannot end outlives every timed one,
        // so no timer this pass could set would decide anything.
        let mut unbounded = false;
        let mut holders: Vec<&'static str> = Vec::new();
        for policy in &self.policies {
            if !policy.holds_open(&state) {
                continue;
            }
            holders.push(policy.name());
            match policy.expires_in(&state) {
                Some(expires_in) => {
                    soonest = Some(soonest.map_or(expires_in, |s: Duration| s.min(expires_in)));
                }
                None => unbounded = true,
            }
        }
        let recheck = if unbounded { None } else { soonest };

        if holders.is_empty() {
            debug!(
                subscribers,
                "no door holds this bridge open — it drains when CC next idles"
            );
            Verdict::Drain
        } else {
            debug!(
                subscribers,
                holders = holders.join(","),
                recheck_secs = recheck.map(|d| d.as_secs()),
                "bridge held open"
            );
            Verdict::KeepAlive { recheck }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(subscribers: usize) -> LifetimeState {
        LifetimeState {
            subscribers,
            last_detach: None,
            last_bus_activity: None,
            now: Instant::now(),
        }
    }

    /// An ephemeral app's browser hold is exactly its attached connections:
    /// one is a hold that no timer can end, zero is no hold at all.
    #[test]
    fn the_ephemeral_browser_hold_is_the_attached_connections() {
        let policy = WebsocketLifetime { idle_timeout: None };

        let attached = state(1);
        assert!(policy.holds_open(&attached));
        assert_eq!(
            policy.expires_in(&attached),
            None,
            "an attached tab is ended by detaching, not by waiting"
        );

        let empty = state(0);
        assert!(!policy.holds_open(&empty));
    }

    /// A persistent app's hold outlives the last tab by its timeout, and the
    /// remaining grace is what the arbiter would set a timer for.
    #[test]
    fn the_persistent_browser_hold_outlives_the_last_tab() {
        let policy = WebsocketLifetime {
            idle_timeout: Some(Duration::from_secs(300)),
        };
        let now = Instant::now();

        let fresh = LifetimeState {
            subscribers: 0,
            last_detach: Some(now - Duration::from_secs(60)),
            last_bus_activity: None,
            now,
        };
        assert!(policy.holds_open(&fresh));
        assert_eq!(policy.expires_in(&fresh), Some(Duration::from_secs(240)));

        let stale = LifetimeState {
            last_detach: Some(now - Duration::from_secs(301)),
            ..fresh
        };
        assert!(!policy.holds_open(&stale), "an elapsed grace is not a hold");

        let never_attached = state(0);
        assert!(
            !policy.holds_open(&never_attached),
            "a bridge no browser has visited has no grace to run"
        );
    }

    /// The bus hold runs from the last interaction and expires by waiting.
    #[test]
    fn the_bus_hold_runs_from_the_last_interaction() {
        let policy = BusLifetime {
            idle_timeout: Duration::from_secs(300),
        };
        let now = Instant::now();

        let quiet = state(0);
        assert!(
            !policy.holds_open(&quiet),
            "a conversation no peer has driven is not held by the bus"
        );

        let recent = LifetimeState {
            last_bus_activity: Some(now - Duration::from_secs(10)),
            now,
            ..state(0)
        };
        assert!(policy.holds_open(&recent));
        assert_eq!(policy.expires_in(&recent), Some(Duration::from_secs(290)));

        let elapsed = LifetimeState {
            last_bus_activity: Some(now - Duration::from_secs(300)),
            ..recent
        };
        assert!(!policy.holds_open(&elapsed));
    }

    /// Keep-alive is the OR: either door alone holds the bridge, and the
    /// verdict's recheck is the soonest expiry among the doors that do hold.
    #[test]
    fn keep_alive_is_the_or_of_the_doors() {
        let arbiter = LifetimeArbiter::new(
            Some(Duration::from_secs(300)),
            Some(Duration::from_secs(60)),
        );

        assert_eq!(
            arbiter.verdict(0),
            Verdict::Drain,
            "an untouched bridge is held by nothing"
        );

        // The bus alone.
        arbiter.note_bus_activity();
        match arbiter.verdict(0) {
            Verdict::KeepAlive {
                recheck: Some(after),
            } => assert!(
                after <= Duration::from_secs(60) && after > Duration::from_secs(55),
                "the bus hold's remaining window, got {after:?}"
            ),
            other => panic!("the bus alone must hold the bridge, got {other:?}"),
        }

        // The browser alone: an attached tab has no expiry, so nothing is worth
        // a timer even though the bus hold is still ticking.
        assert_eq!(
            arbiter.verdict(1),
            Verdict::KeepAlive { recheck: None },
            "an attached tab outlasts every timed hold"
        );

        // Both timed: the browser's post-detach grace is the longer one, so the
        // soonest expiry — the bus's — is what to re-ask at. Bracketed on both
        // sides: a recheck collapsing toward zero would satisfy an upper bound
        // alone while re-asking in a busy loop.
        arbiter.note_detach();
        match arbiter.verdict(0) {
            Verdict::KeepAlive {
                recheck: Some(after),
            } => assert!(
                after <= Duration::from_secs(60) && after > Duration::from_secs(55),
                "the soonest hold to expire decides the recheck, got {after:?}"
            ),
            other => panic!("both doors hold, got {other:?}"),
        }
    }

    /// A deployment with no bus has no bus door — bus activity on such a bridge
    /// is not something that can happen, and stamping it holds nothing.
    #[test]
    fn a_busless_deployment_has_only_the_browser_door() {
        let arbiter = LifetimeArbiter::new(Some(Duration::from_secs(300)), None);
        arbiter.note_bus_activity();
        assert_eq!(
            arbiter.verdict(0),
            Verdict::Drain,
            "without a bus door a bus stamp decides nothing"
        );
    }
}
