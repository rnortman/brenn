//! Registry of live attachment sessions, keyed by attacher.
//!
//! The key is the attacher's principal — what `AttachProfile::attacher` answers.
//! The map itself is key-opaque; naming it that way is what lets the route that
//! registers a session and the planes that fan out to it agree without a second
//! identifier travelling between them.
//!
//! Enforces the per-attacher session caps and provides per-session attribution
//! for logging. The attached-session view is also what the push router reads to
//! route wakes to live connections.
//!
//! Everything here is at channel grain. An attachment holds at most one
//! subscription per channel, so "is this session subscribed" is a channel
//! question and a delivery names no target beyond the envelope it carries —
//! whatever sits behind the channel on the attacher's side is the attacher's
//! own bookkeeping.

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::sync::{Arc, Mutex};

use brenn_attach_proto::DeferredViewEntry;
use brenn_lib::messaging::MessageEnvelope;
use chrono::{DateTime, Utc};
use tokio::sync::{Notify, mpsc};
use tracing::warn;
use uuid::Uuid;

/// Bounded depth of a session's push queue. Every producer hands work over with
/// `try_send`, so a full queue drops the copy rather than stalling the shared
/// dispatch fan-out task.
pub const PUSH_QUEUE_FRAMES: usize = 256;

/// One item handed from a producer outside the session task to a session's own
/// task, over its bounded `push_tx`.
///
/// Two variants because two independent planes reach a session from outside:
/// retained rows the session must sequence against its own cursor state, and
/// deferred-view snapshots it forwards verbatim.
#[derive(Clone)]
pub enum SessionPush {
    Live(LiveDelivery),
    DeferredView(DeferredViewPush),
}

/// One `(channel, attribution)` deferred-view snapshot bound for a session's
/// `ServerFrame::DeferredView`.
///
/// Carries no delivery state: the frame is a full replacement, so a session that
/// drops one under a full queue is corrected by the next emission rather than
/// owed a resend.
#[derive(Clone)]
pub struct DeferredViewPush {
    pub channel: String,
    /// The sub-identity whose parked set this is, or `None` for the attacher's
    /// own bare identity.
    pub attribution: Option<String>,
    pub entries: Vec<DeferredViewEntry>,
}

/// One live retained row handed from the `WakeRouter` fan-out to a subscribed
/// session's task via its bounded `push_tx`. The channel is `envelope.channel`,
/// which is the whole address of the subscription it belongs to. A session that
/// misses one — queue full, or gone between the fan-out and the send — resumes
/// past it from its own cursor, so the hand-off owes nothing and carries no
/// delivery state.
#[derive(Clone)]
pub struct LiveDelivery {
    pub envelope: Arc<MessageEnvelope>,
    /// The message's position in its channel's retention order — what the
    /// session mints the wire cursor's position from, and the key its duplicate
    /// suppression runs on.
    pub retained_seq: u64,
}

/// Session caps enforced by `try_register`. A struct (not two adjacent
/// `usize` params) so call sites cannot transpose the shared and per-account
/// caps.
#[derive(Clone, Copy)]
pub struct SessionCaps {
    /// Max attached sessions per attacher, across all accounts.
    pub per_attacher: usize,
    /// Max attached sessions per (attacher, account).
    pub per_account: usize,
}

/// Why `try_register` refused a registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegisterRejection {
    /// The attacher is at `per_attacher` capacity across all accounts.
    AttacherFull { current: usize },
    /// This account is at `per_account` capacity on this attacher.
    AccountCapExceeded { account_current: usize },
}

impl SessionCaps {
    /// Caps that never trip, for tests exercising non-capacity paths.
    #[cfg(test)]
    pub const UNCAPPED: SessionCaps = SessionCaps {
        per_attacher: usize::MAX,
        per_account: usize::MAX,
    };
}

/// Live attachment sessions, keyed by attacher — the slug of a surface, and
/// whatever names a daemon later.
///
/// Sync `Mutex`, never held across `.await` (push-window precedent): every
/// operation is a brief in-memory map mutation. Poisoning is a broken invariant
/// and `expect`s per house rules.
#[derive(Clone, Default)]
pub struct AttachRegistry {
    inner: Arc<Mutex<HashMap<String, Vec<Arc<AttachSessionHandle>>>>>,
}

/// Per-connection record for one attached session.
///
/// The push fields (`push_tx`, `active_channels`, `drain_notify`) are shared
/// between the registry (where producers write) and the session task (which
/// drains). Created by the route handler at upgrade.
#[derive(Clone)]
pub struct AttachSessionHandle {
    /// Per-connection id, for log attribution.
    pub session_id: Uuid,
    /// The authenticated account behind this attachment — the logged-in user
    /// for a browser page. Held for the per-account cap and for log
    /// attribution; the attacher's *authority* comes from its profile, never
    /// from here.
    pub account: String,
    pub client_ip: IpAddr,
    pub connected_at: DateTime<Utc>,
    /// Live rows and deferred-view snapshots to this session's task (bounded,
    /// `try_send`).
    pub push_tx: mpsc::Sender<SessionPush>,
    /// The channels this session currently holds a subscription on. Written by
    /// the session task (subscribe/unsubscribe), read by the router fan-out.
    /// Sync `Mutex`, never held across `.await` (registry discipline).
    pub active_channels: Arc<Mutex<HashSet<String>>>,
    /// Eager-wake nudge: the router notifies it so the session runs a drain pass
    /// (flushing quiet/parked rows the live path did not carry).
    pub drain_notify: Arc<Notify>,
}

impl AttachSessionHandle {
    /// Whether this session currently holds an active subscription on `channel`.
    /// Confines the `active_channels` lock scope to this method so the
    /// sync-Mutex-never-across-await discipline lives in one place rather than
    /// being re-implemented at every reader.
    pub fn is_subscribed(&self, channel: &str) -> bool {
        self.active_channels
            .lock()
            .expect("active_channels poisoned")
            .contains(channel)
    }

    /// Push one deferred-view snapshot at this session, reporting whether the
    /// queue took it. A full queue drops it: the snapshot is a full replacement,
    /// so the next emission carries the same answer and nothing is owed.
    pub fn try_push_deferred_view(&self, view: DeferredViewPush) -> bool {
        self.push_tx
            .try_send(SessionPush::DeferredView(view))
            .is_ok()
    }

    /// Minimal handle for tests that only care about `account` / capacity:
    /// fresh id, localhost IP, throwaway push channel, no subscriptions.
    /// One constructor so a new field lands in one place, not every test file.
    #[cfg(test)]
    pub fn for_test(account: &str) -> Self {
        let (push_tx, _push_rx) = mpsc::channel(PUSH_QUEUE_FRAMES);
        Self {
            session_id: Uuid::new_v4(),
            account: account.to_string(),
            client_ip: IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            connected_at: Utc::now(),
            push_tx,
            active_channels: Arc::new(Mutex::new(HashSet::new())),
            drain_notify: Arc::new(Notify::new()),
        }
    }
}

/// Unregisters its session on `Drop` — panic-safe, and correct even if the WS
/// upgrade callback never runs. Travels from the route handler into the session
/// task, which holds it for the session's lifetime.
pub struct AttachSessionGuard {
    registry: AttachRegistry,
    attacher: String,
    session_id: Uuid,
}

impl AttachRegistry {
    /// Atomic capacity check + insert. Both caps are checked before any map
    /// mutation so a rejected registration (e.g. a zero cap) never leaves a
    /// phantom empty attacher entry — pruning only runs on guard Drop, and no
    /// guard is issued here. The per-account check runs first so the more
    /// specific diagnosis wins when both caps are at their limit. The returned
    /// guard unregisters on `Drop`, which also releases the per-account slot.
    pub fn try_register(
        &self,
        attacher: &str,
        handle: AttachSessionHandle,
        caps: SessionCaps,
    ) -> Result<AttachSessionGuard, RegisterRejection> {
        let session_id = handle.session_id;
        let mut map = self.inner.lock().expect("attach_registry poisoned");
        let sessions = map.get(attacher);
        let account_current = sessions.map_or(0, |v| {
            v.iter().filter(|h| h.account == handle.account).count()
        });
        if account_current >= caps.per_account {
            return Err(RegisterRejection::AccountCapExceeded { account_current });
        }
        let current = sessions.map_or(0, Vec::len);
        if current >= caps.per_attacher {
            return Err(RegisterRejection::AttacherFull { current });
        }
        map.entry(attacher.to_string())
            .or_default()
            .push(Arc::new(handle));
        Ok(AttachSessionGuard {
            registry: self.clone(),
            attacher: attacher.to_string(),
            session_id,
        })
    }

    /// Snapshot of the sessions attached to `attacher`.
    pub fn sessions(&self, attacher: &str) -> Vec<Arc<AttachSessionHandle>> {
        let map = self.inner.lock().expect("attach_registry poisoned");
        map.get(attacher).cloned().unwrap_or_default()
    }

    /// Hand one deferred-view snapshot to every session attached to `attacher`.
    ///
    /// Every session, not just the one whose action changed the set: the parked
    /// set belongs to the sub-identity, which every attachment of the attacher
    /// shares, so a view held by only one of them would be a second answer to a
    /// question that has one.
    ///
    /// One place for the fan-out because two producers reach it — the session
    /// task's own park/cancel/edit and the release sweep, which arrives through
    /// the `WakeRouter` seam with no session of its own.
    pub fn push_deferred_view(&self, attacher: &str, view: &DeferredViewPush) {
        for handle in self.sessions(attacher) {
            if !handle.try_push_deferred_view(view.clone()) {
                warn!(
                    attacher,
                    attribution = view.attribution.as_deref().unwrap_or("<attacher>"),
                    channel = view.channel,
                    session = %handle.session_id,
                    "deferred view dropped: session push queue full; the next change to this \
                     sender's parked set carries the whole snapshot again"
                );
            }
        }
    }

    /// Count of sessions attached to `attacher`.
    pub fn count(&self, attacher: &str) -> usize {
        let map = self.inner.lock().expect("attach_registry poisoned");
        map.get(attacher).map_or(0, Vec::len)
    }
}

impl AttachSessionGuard {
    /// Atomically remove this guard's session from the registry and return the
    /// number of sessions still attached to the attacher afterward. Teardown
    /// calls this to decide "am I the last session for this attacher" atomically:
    /// reading `count()` while still registered races two concurrent closers into
    /// both observing the other and both skipping the terminal action, leaving no
    /// last-session decider. Idempotent with [`Drop`] — the drop-time removal
    /// becomes a no-op once this has run.
    pub fn unregister_returning_remaining(&self) -> usize {
        let mut map = self
            .registry
            .inner
            .lock()
            .expect("attach_registry poisoned");
        match map.get_mut(&self.attacher) {
            Some(sessions) => {
                sessions.retain(|h| h.session_id != self.session_id);
                let remaining = sessions.len();
                if sessions.is_empty() {
                    map.remove(&self.attacher);
                }
                remaining
            }
            None => 0,
        }
    }
}

impl Drop for AttachSessionGuard {
    fn drop(&mut self) {
        let _ = self.unregister_returning_remaining();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const UNCAPPED: SessionCaps = SessionCaps::UNCAPPED;

    fn handle_for(account: &str) -> AttachSessionHandle {
        AttachSessionHandle::for_test(account)
    }

    fn view(channel: &str, attribution: &str) -> DeferredViewPush {
        DeferredViewPush {
            channel: channel.to_string(),
            attribution: Some(attribution.to_string()),
            entries: Vec::new(),
        }
    }

    #[test]
    fn register_and_guard_drop_lifecycle() {
        let registry = AttachRegistry::default();
        assert_eq!(registry.count("deskbar"), 0);

        let guard = registry
            .try_register("deskbar", handle_for("alice"), UNCAPPED)
            .unwrap();
        assert_eq!(registry.count("deskbar"), 1);
        assert_eq!(registry.sessions("deskbar").len(), 1);

        drop(guard);
        assert_eq!(registry.count("deskbar"), 0);
        // Empty attacher entry is pruned, so an unknown-attacher snapshot is
        // empty.
        assert!(registry.sessions("deskbar").is_empty());
    }

    #[test]
    fn unregister_returning_remaining_reports_survivors_and_is_idempotent() {
        let registry = AttachRegistry::default();
        let g1 = registry
            .try_register("deskbar", handle_for("a"), UNCAPPED)
            .unwrap();
        let g2 = registry
            .try_register("deskbar", handle_for("b"), UNCAPPED)
            .unwrap();
        assert_eq!(registry.count("deskbar"), 2);

        // First closer removes itself and observes one survivor — not the last
        // session, so teardown would skip the terminal stamp.
        assert_eq!(g1.unregister_returning_remaining(), 1);
        assert_eq!(registry.count("deskbar"), 1);
        // Idempotent: a second call for the already-removed guard is a no-op and
        // still reports the current survivor count, never underflowing.
        assert_eq!(g1.unregister_returning_remaining(), 1);
        assert_eq!(registry.count("deskbar"), 1);

        // Last closer removes itself and observes zero — the terminal-snapshot
        // trigger.
        assert_eq!(g2.unregister_returning_remaining(), 0);
        assert_eq!(registry.count("deskbar"), 0);

        // Drop of already-unregistered guards does not re-decrement or panic.
        drop(g1);
        drop(g2);
        assert_eq!(registry.count("deskbar"), 0);
    }

    #[test]
    fn capacity_boundary() {
        let registry = AttachRegistry::default();
        // High per-account cap so the shared per-attacher cap is what trips.
        let caps = SessionCaps {
            per_attacher: 2,
            per_account: 64,
        };
        let _g1 = registry
            .try_register("deskbar", handle_for("a"), caps)
            .unwrap();
        let _g2 = registry
            .try_register("deskbar", handle_for("b"), caps)
            .unwrap();
        assert_eq!(registry.count("deskbar"), 2);

        let Err(rej) = registry.try_register("deskbar", handle_for("c"), caps) else {
            panic!("expected registration to fail at cap");
        };
        assert_eq!(rej, RegisterRejection::AttacherFull { current: 2 });
        assert_eq!(registry.count("deskbar"), 2);
    }

    #[test]
    fn per_account_boundary() {
        let registry = AttachRegistry::default();
        let caps = SessionCaps {
            per_attacher: 64,
            per_account: 2,
        };
        let _a1 = registry
            .try_register("deskbar", handle_for("alice"), caps)
            .unwrap();
        let _a2 = registry
            .try_register("deskbar", handle_for("alice"), caps)
            .unwrap();

        // Alice is at her per-account cap; the attacher is nowhere near full.
        let Err(rej) = registry.try_register("deskbar", handle_for("alice"), caps) else {
            panic!("expected alice to be refused at her per-account cap");
        };
        assert_eq!(
            rej,
            RegisterRejection::AccountCapExceeded { account_current: 2 }
        );

        // A different account is still admitted.
        let _b1 = registry
            .try_register("deskbar", handle_for("bob"), caps)
            .unwrap();
        assert_eq!(registry.count("deskbar"), 3);
    }

    #[test]
    fn rejection_precedence_account_before_attacher() {
        let registry = AttachRegistry::default();
        let caps = SessionCaps {
            per_attacher: 2,
            per_account: 2,
        };
        let _a1 = registry
            .try_register("deskbar", handle_for("alice"), caps)
            .unwrap();
        let _a2 = registry
            .try_register("deskbar", handle_for("alice"), caps)
            .unwrap();

        // Both caps are at their limit; the per-account check runs first.
        let Err(rej) = registry.try_register("deskbar", handle_for("alice"), caps) else {
            panic!("expected registration to fail");
        };
        assert_eq!(
            rej,
            RegisterRejection::AccountCapExceeded { account_current: 2 }
        );
    }

    #[test]
    fn per_account_slot_release_readmits() {
        let registry = AttachRegistry::default();
        let caps = SessionCaps {
            per_attacher: 64,
            per_account: 2,
        };
        let _a1 = registry
            .try_register("deskbar", handle_for("alice"), caps)
            .unwrap();
        let a2 = registry
            .try_register("deskbar", handle_for("alice"), caps)
            .unwrap();
        assert!(
            registry
                .try_register("deskbar", handle_for("alice"), caps)
                .is_err()
        );

        drop(a2);
        let _a3 = registry
            .try_register("deskbar", handle_for("alice"), caps)
            .expect("alice re-admitted after releasing a slot");
    }

    #[test]
    fn guard_releases_on_panic() {
        let registry = AttachRegistry::default();
        let registry_clone = registry.clone();

        let joined = std::thread::spawn(move || {
            let _guard = registry_clone
                .try_register("deskbar", handle_for("alice"), UNCAPPED)
                .unwrap();
            assert_eq!(registry_clone.count("deskbar"), 1);
            panic!("boom");
        })
        .join();
        assert!(joined.is_err(), "thread must have panicked");

        // The guard dropped during unwind, releasing the slot.
        assert_eq!(registry.count("deskbar"), 0);
    }

    #[test]
    fn zero_cap_registration_leaves_no_phantom_entry() {
        let registry = AttachRegistry::default();
        // A zero per-account cap trips first (the per-account check precedes the
        // per-attacher check), and still leaves no phantom attacher entry.
        let caps = SessionCaps {
            per_attacher: 0,
            per_account: 0,
        };
        let Err(rej) = registry.try_register("deskbar", handle_for("alice"), caps) else {
            panic!("expected registration to fail at a zero cap");
        };
        assert_eq!(
            rej,
            RegisterRejection::AccountCapExceeded { account_current: 0 }
        );
        // No empty attacher entry was created: the snapshot is empty and the
        // attacher is absent (a guard-drop would have pruned it, but none was
        // issued).
        assert!(registry.sessions("deskbar").is_empty());
        assert_eq!(registry.count("deskbar"), 0);
    }

    #[test]
    fn snapshot_isolation() {
        let registry = AttachRegistry::default();
        let _g = registry
            .try_register("deskbar", handle_for("alice"), UNCAPPED)
            .unwrap();

        let snapshot = registry.sessions("deskbar");
        assert_eq!(snapshot.len(), 1);

        // A later registration does not mutate an already-taken snapshot.
        let _g2 = registry
            .try_register("deskbar", handle_for("other"), UNCAPPED)
            .unwrap();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(registry.count("deskbar"), 2);
    }

    #[test]
    fn attachers_are_independent() {
        let registry = AttachRegistry::default();
        let _g = registry
            .try_register("deskbar", handle_for("alice"), UNCAPPED)
            .unwrap();
        assert_eq!(registry.count("deskbar"), 1);
        assert_eq!(registry.count("kitchen"), 0);
    }

    /// Subscription membership is the channel and nothing else: the attachment
    /// holds one subscription per channel, so a row on a channel it subscribed
    /// is its row however many of its own bindings sit behind it.
    #[test]
    fn subscription_membership_is_per_channel() {
        let handle = AttachSessionHandle::for_test("alice");
        handle
            .active_channels
            .lock()
            .expect("active_channels poisoned")
            .insert("brenn:home.temp".to_string());

        assert!(handle.is_subscribed("brenn:home.temp"));
        assert!(!handle.is_subscribed("brenn:home.humidity"));
    }

    #[tokio::test]
    async fn a_view_reaches_every_session_of_the_attacher() {
        let registry = AttachRegistry::default();
        let mut queues = Vec::new();
        let mut guards = Vec::new();
        for account in ["alice", "bob"] {
            let (push_tx, push_rx) = mpsc::channel(PUSH_QUEUE_FRAMES);
            let mut handle = AttachSessionHandle::for_test(account);
            handle.push_tx = push_tx;
            guards.push(
                registry
                    .try_register("deskbar", handle, UNCAPPED)
                    .expect("registered"),
            );
            queues.push(push_rx);
        }

        registry.push_deferred_view("deskbar", &view("brenn:home.cmd", "clock"));

        for queue in &mut queues {
            let SessionPush::DeferredView(pushed) = queue.try_recv().expect("view pushed") else {
                panic!("expected a deferred-view push");
            };
            assert_eq!(pushed.channel, "brenn:home.cmd");
            assert_eq!(pushed.attribution.as_deref(), Some("clock"));
        }
    }

    /// A full queue drops the snapshot rather than blocking the shared fan-out:
    /// the next emission restates the whole set, so nothing is owed.
    #[tokio::test]
    async fn a_full_queue_drops_a_view_without_blocking() {
        let registry = AttachRegistry::default();
        let (push_tx, mut push_rx) = mpsc::channel(1);
        let mut handle = AttachSessionHandle::for_test("alice");
        handle.push_tx = push_tx;
        let _guard = registry
            .try_register("deskbar", handle, UNCAPPED)
            .expect("registered");

        registry.push_deferred_view("deskbar", &view("brenn:home.cmd", "clock"));
        registry.push_deferred_view("deskbar", &view("brenn:home.cmd", "clock"));

        assert!(push_rx.try_recv().is_ok());
        assert!(
            push_rx.try_recv().is_err(),
            "the second snapshot was dropped by the full queue, not queued behind it"
        );
    }
}
