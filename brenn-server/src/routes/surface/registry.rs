//! Registry of attached surface WS sessions, keyed by surface slug.
//!
//! Enforces the per-surface session cap and provides per-session attribution
//! for logging. The attached-session view is also what a durable push router
//! reads to route wakes to live connections.

// The route handler and session task that call these land in a later
// increment; the in-crate tests already exercise them.
#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::sync::{Arc, Mutex};

use super::SubKey;
use brenn_lib::messaging::MessageEnvelope;
use chrono::{DateTime, Utc};
use tokio::sync::{Notify, mpsc};
use uuid::Uuid;

/// Bounded depth of a session's live durable-delivery queue. The router fans a
/// live row out with `try_send`, so a full queue drops the live copy (the row
/// re-parks and the session catches up on its next drain) rather than stalling
/// the shared dispatch fan-out task.
pub const DURABLE_QUEUE_FRAMES: usize = 256;

/// One live durable row handed from the `WakeRouter` fan-out to a subscribed
/// session's task via its bounded `durable_tx`. `seq` is `messaging_messages.id`
/// (the durable row id the session mints the wire cursor's high-water from). The
/// channel is `envelope.channel`; the router's
/// claim of the pending-push row *is* the mark-delivered, so the session carries
/// no `push_id`. The envelope is an `Arc` so the fan-out shares one allocation
/// across every subscribed session instead of cloning the body per session.
#[derive(Clone)]
pub struct DurableDelivery {
    pub envelope: Arc<MessageEnvelope>,
    pub seq: i64,
    /// The subscription this row is targeted at — the principal the push row
    /// named, paired with the row's channel. The session routes the resulting
    /// `Deliver` under it, so a row bound for one instance never surfaces on a
    /// sibling's ports.
    pub sub: SubKey,
}

/// Session caps enforced by `try_register`. A struct (not two adjacent
/// `usize` params) so call sites cannot transpose surface and user caps.
#[derive(Clone, Copy)]
pub struct SessionCaps {
    /// Max attached sessions per surface, across all users.
    pub per_surface: usize,
    /// Max attached sessions per (surface, username).
    pub per_user: usize,
}

/// Why `try_register` refused a registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegisterRejection {
    /// The surface is at `per_surface` capacity across all users.
    SurfaceFull { current: usize },
    /// This username is at `per_user` capacity on this surface.
    UserCapExceeded { user_current: usize },
}

/// Attached surface WS sessions, keyed by slug.
///
/// Sync `Mutex`, never held across `.await` (push-window precedent): every
/// operation is a brief in-memory map mutation. Poisoning is a broken invariant
/// and `expect`s per house rules.
#[derive(Clone, Default)]
pub struct SurfaceRegistry {
    inner: Arc<Mutex<HashMap<String, Vec<Arc<SurfaceSessionHandle>>>>>,
}

/// Per-connection record for one attached session.
///
/// The three durable-delivery fields are the live-projection handle the
/// `WakeRouter` Surface arm reaches through: it snapshots the sessions of a
/// slug, keeps those whose `durable_subs` covers the row's subscription, and
/// hands the row to `durable_tx` / nudges `drain_notify`. They are created in the WS
/// handler and shared (by `Arc`/`Sender` clone) with the session task.
#[derive(Clone)]
pub struct SurfaceSessionHandle {
    /// Per-connection id, for log attribution.
    pub session_id: Uuid,
    pub username: String,
    pub client_ip: IpAddr,
    pub connected_at: DateTime<Utc>,
    /// Live durable rows to this session's task (bounded, `try_send`).
    pub durable_tx: mpsc::Sender<DurableDelivery>,
    /// The durable subscriptions this session currently holds — `(instance,
    /// channel)`, not just channel, because the subscription belongs to the
    /// principal that bound it. Written by the session task
    /// (subscribe/unsubscribe), read by the router fan-out. Sync `Mutex`, never
    /// held across `.await` (registry discipline).
    pub durable_subs: Arc<Mutex<HashSet<SubKey>>>,
    /// Eager-wake nudge: the router notifies it so the session runs a drain pass
    /// (flushing quiet/parked rows the live path did not carry).
    pub drain_notify: Arc<Notify>,
}

impl SurfaceSessionHandle {
    /// Whether this session currently holds an active durable `Subscribe` for
    /// `sub`. Confines the `durable_subs` lock scope to this method so the
    /// sync-Mutex-never-across-await discipline lives in one place rather than
    /// being re-implemented at every reader.
    ///
    /// Keyed on the whole subscription, not the channel: a row targeted at one
    /// instance must not be delivered because a *sibling* instance on the same
    /// channel happens to be subscribed — they are separate principals with
    /// separate windows, and the row belongs to exactly one of them.
    pub fn is_subscribed(&self, sub: &SubKey) -> bool {
        self.durable_subs
            .lock()
            .expect("durable_subs poisoned")
            .contains(sub)
    }

    /// Minimal handle for tests that only care about `username` / capacity:
    /// fresh id, localhost IP, throwaway durable channel, empty subscriptions.
    /// One constructor so a new field lands in one place, not every test file.
    #[cfg(test)]
    pub fn for_test(username: &str) -> Self {
        let (durable_tx, _durable_rx) = mpsc::channel(DURABLE_QUEUE_FRAMES);
        Self {
            session_id: Uuid::new_v4(),
            username: username.to_string(),
            client_ip: IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            connected_at: Utc::now(),
            durable_tx,
            durable_subs: Arc::new(Mutex::new(HashSet::new())),
            drain_notify: Arc::new(Notify::new()),
        }
    }
}

impl SessionCaps {
    /// Caps that never trip, for tests exercising non-capacity paths.
    #[cfg(test)]
    pub const UNCAPPED: SessionCaps = SessionCaps {
        per_surface: usize::MAX,
        per_user: usize::MAX,
    };
}

/// Unregisters its session on `Drop` — panic-safe, and correct even if the WS
/// upgrade callback never runs. Travels from the route handler into the session
/// task, which holds it for the session's lifetime.
pub struct SurfaceSessionGuard {
    registry: SurfaceRegistry,
    slug: String,
    session_id: Uuid,
}

impl SurfaceRegistry {
    /// Atomic capacity check + insert. Both caps are checked before any map
    /// mutation so a rejected registration (e.g. a zero cap) never leaves a
    /// phantom empty slug entry — pruning only runs on guard Drop, and no guard
    /// is issued here. The per-user check runs first so the more specific
    /// diagnosis wins when both caps are at their limit. The returned guard
    /// unregisters on `Drop`, which also releases the per-user slot.
    pub fn try_register(
        &self,
        slug: &str,
        handle: SurfaceSessionHandle,
        caps: SessionCaps,
    ) -> Result<SurfaceSessionGuard, RegisterRejection> {
        let session_id = handle.session_id;
        let mut map = self.inner.lock().expect("surface_registry poisoned");
        let sessions = map.get(slug);
        let user_current = sessions.map_or(0, |v| {
            v.iter().filter(|h| h.username == handle.username).count()
        });
        if user_current >= caps.per_user {
            return Err(RegisterRejection::UserCapExceeded { user_current });
        }
        let current = sessions.map_or(0, Vec::len);
        if current >= caps.per_surface {
            return Err(RegisterRejection::SurfaceFull { current });
        }
        map.entry(slug.to_string())
            .or_default()
            .push(Arc::new(handle));
        Ok(SurfaceSessionGuard {
            registry: self.clone(),
            slug: slug.to_string(),
            session_id,
        })
    }

    /// Snapshot of the sessions attached to `slug`. Handles are stored behind an
    /// `Arc`, so this clones only refcounts — the router fan-out and eager-wake,
    /// which run per durable row on the shared dispatch loop, pay a pointer bump
    /// rather than a deep clone of every handle.
    pub fn sessions(&self, slug: &str) -> Vec<Arc<SurfaceSessionHandle>> {
        let map = self.inner.lock().expect("surface_registry poisoned");
        map.get(slug).cloned().unwrap_or_default()
    }

    /// Count of sessions attached to `slug`.
    pub fn count(&self, slug: &str) -> usize {
        let map = self.inner.lock().expect("surface_registry poisoned");
        map.get(slug).map_or(0, Vec::len)
    }
}

impl SurfaceSessionGuard {
    /// Atomically remove this guard's session from the registry and return the
    /// number of sessions still attached to the slug afterward. Teardown calls this
    /// to decide "am I the last session for this slug" atomically: reading `count()`
    /// while still registered races two concurrent closers into both observing the
    /// other and both skipping the terminal action, leaving no last-session decider.
    /// Idempotent with [`Drop`] — the drop-time removal becomes a no-op once this
    /// has run.
    pub fn unregister_returning_remaining(&self) -> usize {
        let mut map = self
            .registry
            .inner
            .lock()
            .expect("surface_registry poisoned");
        match map.get_mut(&self.slug) {
            Some(sessions) => {
                sessions.retain(|h| h.session_id != self.session_id);
                let remaining = sessions.len();
                if sessions.is_empty() {
                    map.remove(&self.slug);
                }
                remaining
            }
            None => 0,
        }
    }
}

impl Drop for SurfaceSessionGuard {
    fn drop(&mut self) {
        let _ = self.unregister_returning_remaining();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const UNCAPPED: SessionCaps = SessionCaps::UNCAPPED;

    fn handle_for(username: &str) -> SurfaceSessionHandle {
        SurfaceSessionHandle::for_test(username)
    }

    #[test]
    fn register_and_guard_drop_lifecycle() {
        let registry = SurfaceRegistry::default();
        assert_eq!(registry.count("deskbar"), 0);

        let guard = registry
            .try_register("deskbar", handle_for("alice"), UNCAPPED)
            .unwrap();
        assert_eq!(registry.count("deskbar"), 1);
        assert_eq!(registry.sessions("deskbar").len(), 1);

        drop(guard);
        assert_eq!(registry.count("deskbar"), 0);
        // Empty surface entry is pruned, so an unknown-slug snapshot is empty.
        assert!(registry.sessions("deskbar").is_empty());
    }

    #[test]
    fn unregister_returning_remaining_reports_survivors_and_is_idempotent() {
        let registry = SurfaceRegistry::default();
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
        let registry = SurfaceRegistry::default();
        // High per-user cap so the shared per-surface cap is what trips.
        let caps = SessionCaps {
            per_surface: 2,
            per_user: 64,
        };
        let _g1 = registry
            .try_register("deskbar", handle_for("a"), caps)
            .unwrap();
        let _g2 = registry
            .try_register("deskbar", handle_for("b"), caps)
            .unwrap();
        assert_eq!(registry.count("deskbar"), 2);

        // At cap: register fails and reports the current count.
        let Err(rej) = registry.try_register("deskbar", handle_for("c"), caps) else {
            panic!("expected registration to fail at cap");
        };
        assert_eq!(rej, RegisterRejection::SurfaceFull { current: 2 });
        assert_eq!(registry.count("deskbar"), 2);
    }

    #[test]
    fn per_user_boundary() {
        let registry = SurfaceRegistry::default();
        let caps = SessionCaps {
            per_surface: 64,
            per_user: 2,
        };
        let _a1 = registry
            .try_register("deskbar", handle_for("alice"), caps)
            .unwrap();
        let _a2 = registry
            .try_register("deskbar", handle_for("alice"), caps)
            .unwrap();

        // Alice is at her per-user cap; the surface is nowhere near full.
        let Err(rej) = registry.try_register("deskbar", handle_for("alice"), caps) else {
            panic!("expected alice to be refused at her per-user cap");
        };
        assert_eq!(rej, RegisterRejection::UserCapExceeded { user_current: 2 });

        // A different user is still admitted.
        let _b1 = registry
            .try_register("deskbar", handle_for("bob"), caps)
            .unwrap();
        assert_eq!(registry.count("deskbar"), 3);
    }

    #[test]
    fn rejection_precedence_user_before_surface() {
        let registry = SurfaceRegistry::default();
        let caps = SessionCaps {
            per_surface: 2,
            per_user: 2,
        };
        let _a1 = registry
            .try_register("deskbar", handle_for("alice"), caps)
            .unwrap();
        let _a2 = registry
            .try_register("deskbar", handle_for("alice"), caps)
            .unwrap();

        // Both caps are at their limit; the per-user check runs first.
        let Err(rej) = registry.try_register("deskbar", handle_for("alice"), caps) else {
            panic!("expected registration to fail");
        };
        assert_eq!(rej, RegisterRejection::UserCapExceeded { user_current: 2 });
    }

    #[test]
    fn per_user_slot_release_readmits() {
        let registry = SurfaceRegistry::default();
        let caps = SessionCaps {
            per_surface: 64,
            per_user: 2,
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

        // Dropping one of alice's guards frees her per-user slot.
        drop(a2);
        let _a3 = registry
            .try_register("deskbar", handle_for("alice"), caps)
            .expect("alice re-admitted after releasing a slot");
    }

    #[test]
    fn guard_releases_on_panic() {
        let registry = SurfaceRegistry::default();
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
        let registry = SurfaceRegistry::default();
        // A zero per-user cap trips first (the per-user check precedes the
        // per-surface check), and still leaves no phantom slug entry.
        let caps = SessionCaps {
            per_surface: 0,
            per_user: 0,
        };
        let Err(rej) = registry.try_register("deskbar", handle_for("alice"), caps) else {
            panic!("expected registration to fail at a zero cap");
        };
        assert_eq!(rej, RegisterRejection::UserCapExceeded { user_current: 0 });
        // No empty slug entry was created: the snapshot is empty and the slug
        // is absent (a guard-drop would have pruned it, but none was issued).
        assert!(registry.sessions("deskbar").is_empty());
        assert_eq!(registry.count("deskbar"), 0);
    }

    #[test]
    fn snapshot_isolation() {
        let registry = SurfaceRegistry::default();
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
    fn slugs_are_independent() {
        let registry = SurfaceRegistry::default();
        let _g = registry
            .try_register("deskbar", handle_for("alice"), UNCAPPED)
            .unwrap();
        assert_eq!(registry.count("deskbar"), 1);
        assert_eq!(registry.count("kitchen"), 0);
    }
}
