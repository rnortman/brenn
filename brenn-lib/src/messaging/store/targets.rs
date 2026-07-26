//! Delivery-target resolution: who a channel's just-retained message is fanned
//! out to live, and on what terms each subscriber is woken.
//!
//! One resolver serves every caller that needs the answer — the publish ladder's
//! surface fan-out, the wake pass's economics lookups, and the conversation
//! family's app→conversation resolution. It holds the participant registry: each
//! subscriber's access policy and wake economics, read as the registrations stand
//! at the moment the question is asked, never from a copy made when a message was
//! written.

use std::collections::HashMap;
use std::sync::Arc;

use indexmap::IndexMap;
use tracing::{debug, warn};

use crate::auth::user::get_user_by_username;
use crate::config::AppConfig;
use crate::conversation::{get_or_create_singleton_conversation, get_singleton_conversation_id};
use crate::messaging::{
    ParticipantId, SubscriberEntry, SubscriberEntryKind, SubscriberRegistration, Urgency,
    WakeEconomics, WakeMin,
};

/// One surface subscriber a just-retained message is fanned out to live.
///
/// Surfaces hold no cursor, so no walk over positions can name them: the
/// envelope is handed to their attached sessions at the moment it enters
/// retention, and a session that misses it resumes past its own wire cursor.
#[derive(Debug, Clone)]
pub struct SurfaceFeedTarget {
    /// The registration key, which carries the subscribing principal's instance.
    pub kind: SubscriberEntryKind,
    /// The subscribing principal as a participant identity.
    pub subscriber: ParticipantId,
    /// `true` for a push-enabled subscription, whose session can resume the
    /// suffix it missed; `false` for fold-0, which is live-or-nothing.
    pub push_enabled: bool,
}

/// The participant registry: each subscriber's access policy and wake economics,
/// plus the apps map its `App` half resolves through.
pub struct TargetResolver {
    apps: Arc<IndexMap<String, AppConfig>>,
    /// One entry per registered non-app subscriber (WASM consumer, surface, or
    /// system component), keyed by its directory [`SubscriberEntryKind`]. App
    /// subscribers are absent: their policy and economics resolve from `apps`,
    /// which also carries their non-policy configuration, so the two cannot
    /// diverge from a registry clone.
    subscribers: HashMap<SubscriberEntryKind, SubscriberRegistration>,
}

impl std::fmt::Debug for TargetResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TargetResolver")
            .field("apps", &self.apps.len())
            .field("subscribers", &self.subscribers.len())
            .finish_non_exhaustive()
    }
}

impl TargetResolver {
    pub fn new(
        apps: Arc<IndexMap<String, AppConfig>>,
        subscribers: HashMap<SubscriberEntryKind, SubscriberRegistration>,
    ) -> Self {
        Self { apps, subscribers }
    }

    /// Fold in a batch of subscriber registrations. Called once per subscriber
    /// kind at boot, while the resolver is still uniquely owned; a duplicate key
    /// across calls is a boot-wiring bug and panics.
    pub fn register(
        &mut self,
        registrations: HashMap<SubscriberEntryKind, SubscriberRegistration>,
    ) {
        for (key, reg) in registrations {
            let prev = self.subscribers.insert(key.clone(), reg);
            assert!(
                prev.is_none(),
                "target resolver: duplicate registration for {key:?} — boot wiring bug",
            );
        }
    }

    /// The registration for a non-app subscriber, if it has one.
    pub fn registration(&self, kind: &SubscriberEntryKind) -> Option<&SubscriberRegistration> {
        self.subscribers.get(kind)
    }

    /// Access-control policy for a directory subscriber of any kind.
    pub fn policy(&self, kind: &SubscriberEntryKind) -> Option<&crate::access::AppPolicy> {
        match kind {
            SubscriberEntryKind::App(slug) => self.apps.get(slug).map(|app| &app.policy),
            other => self.subscribers.get(other).map(|r| r.policy.as_ref()),
        }
    }

    /// Declared [`WakeEconomics`] for a directory subscriber, covering every
    /// kind: `App(slug)` is `UrgencyGated` iff the app exists (economics sourced
    /// from `apps`, the same split [`Self::policy`] makes); every other kind
    /// resolves through the registry. `None` for a live subscriber indicates a
    /// host wiring bug — the boot cross-check rejects it, and the wake pass
    /// panics on it rather than passing over an inline subscriber nothing else
    /// would wake.
    pub fn wake_economics(&self, kind: &SubscriberEntryKind) -> Option<WakeEconomics> {
        match kind {
            SubscriberEntryKind::App(slug) => {
                self.apps.get(slug).map(|_| WakeEconomics::UrgencyGated)
            }
            other => self.subscribers.get(other).map(|r| r.wake),
        }
    }

    /// The apps map, for callers that need an app's non-policy configuration.
    pub fn apps(&self) -> &Arc<IndexMap<String, AppConfig>> {
        &self.apps
    }

    /// The user an `App(slug)` subscriber's messages belong to: the app's single
    /// allowed user. `None` — with a `warn` naming the failed lookup — when the
    /// app, its `allowed_users` entry, or that user's row is missing; each is a
    /// host wiring or config bug rather than an ordinary outcome.
    ///
    /// `channel_address` is diagnostic context only.
    fn app_owner(
        &self,
        conn: &rusqlite::Connection,
        slug: &str,
        channel_address: &str,
    ) -> Option<i64> {
        let app = match self.apps.get(slug) {
            Some(a) => a,
            None => {
                warn!(
                    app = %slug,
                    channel = %channel_address,
                    "app subscriber not found in apps map — host wiring bug; skipping delivery"
                );
                return None;
            }
        };
        // Singleton + 1 allowed_user is enforced by config validation.
        let username = match app.allowed_users.first() {
            Some(u) => u.clone(),
            None => {
                warn!(
                    app = %slug,
                    channel = %channel_address,
                    "resolved app has no allowed_users — host wiring/config bug; \
                     skipping delivery"
                );
                return None;
            }
        };
        match get_user_by_username(conn, &username) {
            Some(u) => Some(u.id),
            None => {
                warn!(
                    app = %slug,
                    channel = %channel_address,
                    username = %username,
                    "allowed_user not found in users table — host wiring bug; skipping delivery"
                );
                None
            }
        }
    }

    /// The singleton conversation an `App(slug)` subscriber holds its delivery
    /// state under, creating it when the app has never had one.
    ///
    /// This is the lazy creator: an app wired to receive gets its conversation at
    /// the attach that gives it a position.
    pub fn ensure_app_conversation(
        &self,
        conn: &rusqlite::Connection,
        slug: &str,
        channel_address: &str,
    ) -> Option<i64> {
        let owner = self.app_owner(conn, slug, channel_address)?;
        Some(get_or_create_singleton_conversation(conn, owner, slug).id)
    }

    /// The same resolution, creating nothing: `None` when the app has no
    /// conversation yet, so a reader can ask which conversation an `App(slug)`
    /// subscriber delivers to without minting one.
    pub fn app_conversation(
        &self,
        conn: &rusqlite::Connection,
        slug: &str,
        channel_address: &str,
    ) -> Option<i64> {
        let owner = self.app_owner(conn, slug, channel_address)?;
        get_singleton_conversation_id(conn, owner, slug)
    }

    /// The surface subscribers on a channel that a just-retained message is
    /// fanned out to live, at whatever depth they subscribe.
    ///
    /// A surface holds no server-side delivery state: the client's echoed cursor
    /// is the whole of it, so both depths take the same row-less
    /// deliver-if-attached fan-out and differ only in what a *detached* session
    /// can recover afterwards — a push-enabled subscription resumes its suffix
    /// from retention, a fold-0 one gets whatever its retained window still
    /// carries at the next subscribe. Only surface subscribers take the feed: an
    /// App/Wasm/System subscriber holds a position and is served from it.
    ///
    /// Gates each target on the subscribing principal's policy as it stands at
    /// this moment — a surface whose policy no longer covers the channel is not
    /// fed, and the denial is re-decided on the next message rather than cached.
    /// The caller builds the envelope once and hands each target to the router.
    pub fn surface_feed_targets(
        &self,
        channel_address: &str,
        subscribers: &[SubscriberEntry],
    ) -> Vec<SurfaceFeedTarget> {
        let mut out = Vec::new();
        for sub in subscribers {
            let SubscriberEntryKind::Surface { slug, instance } = &sub.kind else {
                continue;
            };
            let allowed = self
                .policy(&sub.kind)
                .is_some_and(|p| p.allows_channel_access(channel_address));
            if !allowed {
                debug!(
                    subscriber = ?sub.kind,
                    channel = %channel_address,
                    "surface live feed denied — ACL not satisfied"
                );
                continue;
            }
            // The subscribing principal: a component instance's own sub-identity,
            // or the bare surface for the kernel's layout subscription.
            let subscriber = match instance {
                Some(instance) => ParticipantId::for_surface_component(slug, instance),
                None => ParticipantId::for_surface(slug),
            };
            out.push(SurfaceFeedTarget {
                kind: sub.kind.clone(),
                subscriber,
                push_enabled: sub.push_depth.is_push_enabled(),
            });
        }
        out
    }
}

/// Whether a subscriber with these wake economics is woken by a message at
/// `urgency` — the whole of the wake rule, in one place.
///
/// The wake pass asks it about the loudest message a subscriber has not seen,
/// which is the only place the question arises: nothing decides a wake at commit
/// time any more.
///
/// `Eager` subscribers wake unconditionally; `UrgencyGated` ones wake iff the
/// urgency meets their threshold. An `UrgencyGated` subscriber with no
/// threshold is a registration invariant violation, not a default.
pub fn wakes_at(wake: WakeEconomics, wake_min: Option<WakeMin>, urgency: Urgency) -> bool {
    match (wake, wake_min) {
        (WakeEconomics::Eager, _) => true,
        (WakeEconomics::UrgencyGated, Some(wm)) => wm.wakes(urgency),
        (WakeEconomics::UrgencyGated, None) => unreachable!(
            "UrgencyGated subscriber carries no wake_min — registration invariant violated"
        ),
    }
}
