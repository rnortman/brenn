//! Delivery-target resolution: who a channel's just-retained message is fanned
//! out to live, and on what terms each subscriber is woken.
//!
//! One resolver serves every caller that needs the answer — the publish ladder's
//! attacher fan-out, the wake pass's economics lookups, and the conversation
//! family's app→conversation resolution. It holds the participant registry: each
//! subscriber's access policy and wake economics, read as the registrations stand
//! at the moment the question is asked, never from a copy made when a message was
//! written.

use std::collections::HashMap;
use std::sync::Arc;

use indexmap::IndexMap;
use tracing::{debug, warn};

use brenn_db::auth::user::get_user_by_username;
use brenn_db::conversation::{get_or_create_singleton_conversation, get_singleton_conversation_id};
use brenn_lib::access::PolicyRef;
use brenn_lib::config::AppConfig;
use brenn_lib::messaging::{
    ParticipantId, SubscriberEntry, SubscriberEntryKind, SubscriberRegistration,
    TombstonedRegistry, Urgency, WakeEconomics, WakeMin,
};

/// One attach-shaped subscriber a just-retained message is fanned out to live.
///
/// Both attach-shaped kinds — a browser surface and a remote daemon — take the
/// feed on identical terms; which one a target names decides only the
/// participant identity it answers with.
#[derive(Debug, Clone)]
pub struct AttachFeedTarget {
    /// The registration key: the attacher, which is the whole grain a channel's
    /// feed is cut at.
    pub kind: SubscriberEntryKind,
    /// `true` for a push-enabled subscription, whose session can resume the
    /// suffix it missed; `false` for fold-0, which is live-or-nothing.
    pub push_enabled: bool,
}

impl AttachFeedTarget {
    /// The subscribing attacher as a participant identity — the bare
    /// `surface:<slug>` or `remote:<slug>`. Derived from `kind` rather than
    /// carried, so the two cannot disagree.
    ///
    /// # Panics
    ///
    /// If the target is keyed by any other subscriber kind. Only attach-shaped
    /// subscribers take the row-less feed, so the resolver builds no such
    /// target.
    pub fn subscriber(&self) -> ParticipantId {
        match &self.kind {
            SubscriberEntryKind::Surface(slug) => ParticipantId::for_surface(slug),
            SubscriberEntryKind::Remote(slug) => ParticipantId::for_remote(slug),
            other => panic!(
                "attach feed target keyed by {other:?} — only attach-shaped subscribers take the \
                 row-less live feed"
            ),
        }
    }
}

/// The participant registry: each subscriber's access policy and wake economics,
/// plus the apps map its `App` half resolves through.
///
/// The registry is read on the publish and wake paths and written when a
/// subscriber joins or leaves, so it sits behind an `RwLock` and every read
/// answers with owned values rather than borrows into the guard.
pub struct TargetResolver {
    apps: Arc<IndexMap<String, AppConfig>>,
    /// One entry per registered non-app subscriber (WASM consumer, surface,
    /// remote, or system component), keyed by its directory
    /// [`SubscriberEntryKind`]. App subscribers are absent: their policy and
    /// economics resolve from `apps`, which also carries their non-policy
    /// configuration, so the two cannot diverge from a registry clone.
    subscribers: TombstonedRegistry<SubscriberRegistration>,
}

impl std::fmt::Debug for TargetResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (live, retired) = self.subscribers.counts();
        f.debug_struct("TargetResolver")
            .field("apps", &self.apps.len())
            .field("subscribers", &live)
            .field("retired", &retired)
            .finish_non_exhaustive()
    }
}

impl TargetResolver {
    pub fn new(
        apps: Arc<IndexMap<String, AppConfig>>,
        subscribers: HashMap<SubscriberEntryKind, SubscriberRegistration>,
    ) -> Self {
        Self {
            apps,
            subscribers: TombstonedRegistry::with_live("target resolver", subscribers),
        }
    }

    /// Fold in a batch of subscriber registrations, clearing any tombstone the
    /// keys carried — a subscriber registered again under the same key is simply
    /// live again. Called once per subscriber kind at boot and once per
    /// subscriber that joins afterwards; registering a key that is already live
    /// is a wiring bug and panics.
    pub fn register(&self, registrations: HashMap<SubscriberEntryKind, SubscriberRegistration>) {
        self.subscribers.register_all(registrations);
    }

    /// Retire one registration: the key leaves the live map and becomes a
    /// tombstone, so a lookup racing the departure answers "gone" rather than
    /// panicking.
    ///
    /// # Panics
    ///
    /// If the key is not live. Retiring what was never registered, or retiring
    /// twice, is a wiring bug.
    pub fn retire(&self, kind: &SubscriberEntryKind) {
        self.subscribers.retire(kind);
    }

    /// Whether `kind` holds a tombstone: registered once, retired since, and not
    /// registered again.
    pub fn is_retired(&self, kind: &SubscriberEntryKind) -> bool {
        self.subscribers.is_retired(kind)
    }

    /// The registration for a non-app subscriber, if it has one — owned, because
    /// the registry's lock is released before the caller reads it. Cheap: an
    /// `Arc` clone and a `Copy` enum.
    pub fn registration(&self, kind: &SubscriberEntryKind) -> Option<SubscriberRegistration> {
        self.subscribers.get(kind).live()
    }

    /// Access-control policy for a directory subscriber of any kind.
    ///
    /// A chat conversation reads under its owning app's **harness** policy —
    /// the derived `<prefix>.app.<slug>.` authority, which is the same authority
    /// its publishes ride, so revoking it closes the conversation's read and its
    /// write together. It is deliberately not the app's authored policy: the
    /// harness and the app's LLM are separate principals. That the harness
    /// policy is per-app rather than per-conversation is also why a subscription
    /// minted at runtime needs no registration of its own — there is nothing per
    /// conversation to register.
    pub fn policy(&self, kind: &SubscriberEntryKind) -> Option<PolicyRef<'_>> {
        match kind {
            SubscriberEntryKind::App(slug) => self
                .apps
                .get(slug)
                .map(|app| PolicyRef::Borrowed(&app.policy)),
            SubscriberEntryKind::ChatConversation { app_slug, .. } => self
                .apps
                .get(app_slug)
                .map(|app| PolicyRef::Borrowed(&app.chat_harness_policy)),
            other => self
                .subscribers
                .map(other, |r| PolicyRef::Shared(r.policy.clone()))
                .live(),
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
            // Both wake the same subprocess, so both are priced the same.
            SubscriberEntryKind::App(slug)
            | SubscriberEntryKind::ChatConversation { app_slug: slug, .. } => {
                self.apps.get(slug).map(|_| WakeEconomics::UrgencyGated)
            }
            other => self.subscribers.map(other, |r| r.wake).live(),
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
    ///
    /// Provisioning the minted conversation's chat channels and announcing it on
    /// the app's roster is the caller's obligation: this is a synchronous method
    /// on the caller's connection, with no messenger to ask and nowhere to await
    /// a publish. Its one caller, `Messenger::attach_conversation`, discharges
    /// both around this call.
    ///
    /// TODO(chat-conversation-provision-chokepoint): that discharge is
    /// convention rather than structure, so a creation site added later and
    /// wired straight to this method would mint a conversation with no chat
    /// channels and no roster entry naming it.
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

    /// The attach-shaped subscribers on a channel that a just-retained message
    /// is fanned out to live, at whatever depth they subscribe.
    ///
    /// An attacher holds no server-side delivery state: the client's echoed
    /// cursor is the whole of it, so both depths take the same row-less
    /// deliver-if-attached fan-out and differ only in what a *detached* session
    /// can recover afterwards — a push-enabled subscription resumes its suffix
    /// from retention, a fold-0 one gets whatever its retained window still
    /// carries at the next subscribe. Only attach-shaped subscribers take the
    /// feed: an App/Wasm/System subscriber holds a position and is served from
    /// it.
    ///
    /// Gates each target on the subscribing principal's policy as it stands at
    /// this moment — an attacher whose policy no longer covers the channel is
    /// not fed, and the denial is re-decided on the next message rather than
    /// cached. The caller builds the envelope once and hands each target to the
    /// router.
    pub fn attach_feed_targets(
        &self,
        channel_address: &str,
        subscribers: &[SubscriberEntry],
    ) -> Vec<AttachFeedTarget> {
        let mut out = Vec::new();
        for sub in subscribers {
            if sub.kind.attach_slug().is_none() {
                continue;
            }
            let allowed = self
                .policy(&sub.kind)
                .is_some_and(|p| p.allows_channel_access(channel_address));
            if !allowed {
                debug!(
                    subscriber = ?sub.kind,
                    channel = %channel_address,
                    "attacher live feed denied — ACL not satisfied"
                );
                continue;
            }
            out.push(AttachFeedTarget {
                kind: sub.kind.clone(),
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
