//! Delivery-target resolution: who a channel's committed or released message is
//! owed to.
//!
//! One resolver serves every caller that needs the answer — the publish ladder,
//! the batch flush paths, and the durable store's own release pass. It holds the
//! participant registry (each subscriber's access policy and wake economics) and
//! the channel directory, so a target set is always resolved against the
//! registrations as they stand, never against a copy made when a message was
//! written.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::auth::user::get_user_by_username;
use crate::config::AppConfig;
use crate::conversation::get_or_create_singleton_conversation;
use crate::messaging::config::{Depth, NoiseLevel};
use crate::messaging::{
    MessagingDirectory, ParticipantId, SubscriberEntry, SubscriberEntryKind,
    SubscriberRegistration, Urgency, WakeEconomics, WakeMin,
};

use super::{DeliveryTarget, ReleaseTarget};

/// Resolved push-target metadata: one subscriber a channel's messages are owed
/// to, with the knobs its delivery record is written from.
#[derive(Debug, Clone)]
pub struct PushTarget {
    pub subscriber: ParticipantId,
    pub app_slug: String,
    pub push_depth: Depth,
    /// Noise level for this subscription (used for push-overflow handling).
    pub noise: NoiseLevel,
    /// Declared wake economics for this subscriber. `Eager` ⇒ every push row is
    /// created eager (`wake_min` ignored); `UrgencyGated` ⇒ `eager_wake` gated by
    /// `wake_min.wakes(urgency)`.
    pub wake: WakeEconomics,
    /// Wake-min threshold for this subscription. `Some` iff `wake` is
    /// `UrgencyGated` (the only case that consults it); `None` for `Eager`
    /// targets, so the delivery path cannot read a threshold for a subscriber
    /// whose economics never gate on one.
    pub wake_min: Option<WakeMin>,
}

/// The participant registry plus the channel directory: everything needed to
/// answer "who is this channel's message owed to, and on what terms".
pub struct TargetResolver {
    directory: Arc<MessagingDirectory>,
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
        directory: Arc<MessagingDirectory>,
        apps: Arc<IndexMap<String, AppConfig>>,
        subscribers: HashMap<SubscriberEntryKind, SubscriberRegistration>,
    ) -> Self {
        Self {
            directory,
            apps,
            subscribers,
        }
    }

    /// A resolver over an empty directory and an empty registry: it names no
    /// targets. For cases about a store's retention rather than the delivery
    /// records it writes.
    #[cfg(test)]
    pub(crate) fn unsubscribed() -> Self {
        Self::new(
            Arc::new(MessagingDirectory::new()),
            Arc::new(IndexMap::new()),
            HashMap::new(),
        )
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
    /// host wiring bug — the boot cross-check rejects it, and on the delivery
    /// path the ACL gate has already skipped an unresolvable subscriber.
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

    /// Resolve push targets for an outbound publish: per-subscriber, find the
    /// (singleton-app, allowed_user) → conversation_id mapping that the
    /// dispatcher will inject into. Also resolves the noise level for each
    /// subscriber from `SubscriberEntry.noise`.
    ///
    /// Accepts a caller-held `&Connection` so resolution and the subsequent
    /// insert happen under the same lock acquisition — avoiding a TOCTOU window
    /// where a channel could gain subscribers between resolution and insert.
    ///
    /// `channel_address` is the channel's stored address (`mqtt:`/`brenn:`/
    /// `webhook:`); it backs the **delivery-time ACL gate**. Every `App`/`Wasm`
    /// subscriber — regardless of how the subscription was created — is
    /// re-authorized against its current `AppPolicy` via [`Self::policy`] +
    /// `allows_channel_access`. A subscriber whose policy no longer covers the
    /// channel (ACL removed, transport grant gone, or — a wiring bug — no policy
    /// at all) is **skipped**: it is not pushed and not persisted as a pending
    /// push, with a `warn` revocation signal. The gate is uniform; there is no
    /// static/dynamic branch.
    pub fn push_targets(
        &self,
        conn: &rusqlite::Connection,
        channel_address: &str,
        subscribers: &[SubscriberEntry],
    ) -> Vec<PushTarget> {
        let mut targets = Vec::with_capacity(subscribers.len());
        for sub in subscribers {
            let push_depth = sub.push_depth;
            // depth-0 subs aren't push targets: no push row is ever created for
            // them. A fold-0 *surface* subscriber instead gets a row-less
            // deliver-if-attached context feed via [`Self::context_targets`] +
            // `WakeRouter::deliver_context`, run after this transaction commits.
            if !push_depth.is_push_enabled() {
                continue;
            }
            // Delivery-time ACL gate, uniform over App + Wasm, static + dynamic.
            // A missing policy for a live subscriber is a host wiring bug — fail
            // closed (deny) rather than panic on the delivery path.
            let allowed = self
                .policy(&sub.kind)
                .is_some_and(|p| p.allows_channel_access(channel_address));
            if !allowed {
                warn!(
                    app = %sub.kind.slug(),
                    channel = %channel_address,
                    "subscription delivery denied — ACL not satisfied"
                );
                continue;
            }
            // Declared wake economics for this subscriber, resolved per participant
            // (App from the apps map, others from the registry) — never inferred
            // from the identity prefix. `Eager` subscribers are woken on every
            // publish; `UrgencyGated` subscribers consult `wake_min`. A subscriber
            // that just passed the ACL gate always resolves here (same source), so a
            // `None` is a host-wiring invariant violation, not a routine outcome —
            // surface it and skip delivery, exactly like the missing-app/user cases
            // below. Silently defaulting to `UrgencyGated` here would re-park a live
            // `Eager` subscriber — the precise stranding this resolution exists to
            // prevent — and hide the wiring bug behind an ordinary designed-park.
            let wake = match self.wake_economics(&sub.kind) {
                Some(w) => w,
                None => {
                    warn!(
                        subscriber = ?sub.kind,
                        channel = %channel_address,
                        "subscriber passed ACL gate but has no wake-economics \
                         registration — host wiring bug; skipping delivery"
                    );
                    continue;
                }
            };
            // Only `UrgencyGated` targets ever consult a wake threshold; an
            // `Eager` target carries `None`, making "no eager delivery reads a
            // wake_min" a type-enforced invariant on the delivery path rather
            // than a convention. `SubscriberEntry.wake_min` already carries
            // `Some` iff `UrgencyGated`; forward it unchanged.
            let push_wake_min = match wake {
                WakeEconomics::UrgencyGated => sub.wake_min,
                WakeEconomics::Eager => None,
            };
            match &sub.kind {
                SubscriberEntryKind::App(slug) => {
                    // These three lookups should always succeed for a subscriber
                    // that just passed the ACL gate: its policy resolved, so the
                    // app is wired. A `None` here is a host-wiring invariant
                    // violation, NOT a deny-by-default outcome — surface it so a
                    // wiring bug after the gate is distinguishable from a
                    // successful delivery and from a normal ACL revocation.
                    let app = match self.apps.get(slug) {
                        Some(a) => a,
                        None => {
                            warn!(
                                app = %slug,
                                channel = %channel_address,
                                "subscriber passed ACL gate but app not found in apps map — \
                                 host wiring bug; skipping delivery"
                            );
                            continue;
                        }
                    };
                    let noise = sub.noise;
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
                            continue;
                        }
                    };
                    let user = match get_user_by_username(conn, &username) {
                        Some(u) => u,
                        None => {
                            warn!(
                                app = %slug,
                                channel = %channel_address,
                                username = %username,
                                "allowed_user not found in users table — host wiring bug; \
                                 skipping delivery"
                            );
                            continue;
                        }
                    };
                    let conversation = get_or_create_singleton_conversation(conn, user.id, slug);
                    targets.push(PushTarget {
                        subscriber: ParticipantId::for_conversation(conversation.id),
                        app_slug: slug.clone(),
                        push_depth,
                        noise,
                        wake,
                        wake_min: push_wake_min,
                    });
                }
                SubscriberEntryKind::Wasm(slug) => {
                    // WASM consumers do not go through the apps map /
                    // singleton-conversation.
                    targets.push(PushTarget {
                        subscriber: ParticipantId::for_wasm(slug),
                        app_slug: slug.clone(),
                        push_depth,
                        noise: sub.noise,
                        wake,
                        wake_min: push_wake_min,
                    });
                }
                SubscriberEntryKind::Surface { slug, instance } => {
                    // Surfaces reach durable dispatch via the surface:
                    // ParticipantId directly (no apps map / singleton
                    // conversation). The push window is keyed on the subscribing
                    // principal — a component instance's own sub-identity, or the
                    // bare surface for the kernel's layout subscription — so each
                    // principal's lag is tracked and bounded independently.
                    let subscriber = match instance {
                        Some(instance) => ParticipantId::for_surface_component(slug, instance),
                        None => ParticipantId::for_surface(slug),
                    };
                    targets.push(PushTarget {
                        app_slug: subscriber.as_surface_subscriber_key().to_owned(),
                        subscriber,
                        push_depth,
                        noise: sub.noise,
                        wake,
                        wake_min: push_wake_min,
                    });
                }
                SubscriberEntryKind::System(component) => {
                    // System-substrate subscribers reach durable dispatch via the
                    // system: ParticipantId directly (no apps map / singleton
                    // conversation), parked-and-woken like the Wasm arm.
                    targets.push(PushTarget {
                        subscriber: ParticipantId::for_system(component),
                        app_slug: component.clone(),
                        push_depth,
                        noise: sub.noise,
                        wake,
                        wake_min: push_wake_min,
                    });
                }
            }
        }
        targets
    }

    /// The fold-0 (depth-0) surface subscribers on a channel — the row-less
    /// context-feed targets. A depth-0 subscription creates no push row, so
    /// [`Self::push_targets`] skips it; a surface session nonetheless gets a live
    /// deliver-if-attached fan-out of durable messages while attached. Only
    /// surface subscribers take the feed: a depth-0 App/Wasm/System subscriber
    /// has no wire session to deliver to live.
    ///
    /// Runs the same delivery-time ACL gate as [`Self::push_targets`] — a
    /// subscriber whose policy no longer covers the channel is not fed. Returns
    /// the surface subscriber keys; the caller builds the envelope once and hands
    /// each to `WakeRouter::deliver_context` after commit.
    pub fn context_targets(
        &self,
        channel_address: &str,
        subscribers: &[SubscriberEntry],
    ) -> Vec<SubscriberEntryKind> {
        let mut out = Vec::new();
        for sub in subscribers {
            if sub.push_depth.is_push_enabled() {
                continue;
            }
            if !matches!(sub.kind, SubscriberEntryKind::Surface { .. }) {
                continue;
            }
            let allowed = self
                .policy(&sub.kind)
                .is_some_and(|p| p.allows_channel_access(channel_address));
            if !allowed {
                debug!(
                    subscriber = ?sub.kind,
                    channel = %channel_address,
                    "depth-0 surface context feed denied — ACL not satisfied"
                );
                continue;
            }
            out.push(sub.kind.clone());
        }
        out
    }

    /// The subscribers a commit records a delivery row for, named by the
    /// channel's uuid because the store that asks knows itself by that identity.
    ///
    /// Runs the identical gate ladder as [`Self::push_targets`], then resolves
    /// each target's wake against the one message being committed — which a
    /// commit can do and [`Self::release_targets`] cannot, a release batch
    /// carrying several urgencies.
    ///
    /// A channel absent from the directory owes its commit to nobody, on the
    /// same rule release targeting follows: a store outlives its registration
    /// only when the channel left the directory.
    pub fn commit_targets(
        &self,
        conn: &rusqlite::Connection,
        channel_uuid: Uuid,
        urgency: Urgency,
        delivery_deadline: Option<DateTime<Utc>>,
    ) -> Vec<DeliveryTarget> {
        let Some(entry) = self.directory.by_uuid(&channel_uuid) else {
            return Vec::new();
        };
        let targets = self.push_targets(conn, &entry.address, entry.subscribers.as_slice());
        delivery_targets(&targets, urgency, &entry.address, delivery_deadline)
    }

    /// The subscribers a release pass delivers a channel's due messages to,
    /// named by the channel's uuid because the store that asks knows itself by
    /// that identity.
    ///
    /// Runs the identical gate ladder as [`Self::push_targets`]: depth-0
    /// subscribers are not push targets, and every subscriber is re-authorized
    /// against its current policy, so a subscription revoked while a message was
    /// parked is not delivered to. The wake decision is *not* resolved here — a
    /// release batch carries messages of several urgencies, so the target
    /// carries its threshold and the store applies it per message.
    ///
    /// A channel absent from the directory owes its release to nobody: a store
    /// only outlives its registration when the channel left the directory, and
    /// not delivering to a subscriber set that no longer exists is what
    /// release-time targeting is for.
    pub fn release_targets(
        &self,
        conn: &rusqlite::Connection,
        channel_uuid: Uuid,
    ) -> Vec<ReleaseTarget> {
        let Some(entry) = self.directory.by_uuid(&channel_uuid) else {
            return Vec::new();
        };
        self.push_targets(conn, &entry.address, entry.subscribers.as_slice())
            .into_iter()
            .map(|tgt| ReleaseTarget {
                subscriber: tgt.subscriber,
                app_slug: tgt.app_slug,
                wake_min: match tgt.wake {
                    WakeEconomics::Eager => None,
                    WakeEconomics::UrgencyGated => Some(tgt.wake_min.expect(
                        "UrgencyGated push target carries no wake_min — \
                         push_targets invariant violated",
                    )),
                },
            })
            .collect()
    }
}

/// Whether this target's delivery claim carries an eager wake.
///
/// `Eager` subscribers (parked WASM/system consumers, attached surface sessions)
/// are always woken on publish — waking them is cheap, so urgency never gates
/// delivery. `UrgencyGated` subscribers (LLM conversations, whose wake spawns a
/// subprocess) are woken only when the message's urgency meets their `wake_min`
/// threshold; a below-threshold claim waits for the subscriber's next natural
/// wake. Gating eager subscribers on `wake_min` was the stranded-surface-push
/// bug — a below-threshold publish parked invisibly for a live, attached surface
/// session.
pub fn eager_wake_for(target: &PushTarget, urgency: Urgency, channel_address: &str) -> bool {
    let eager_wake = match (target.wake, target.wake_min) {
        (WakeEconomics::Eager, _) => true,
        (WakeEconomics::UrgencyGated, Some(wm)) => wm.wakes(urgency),
        (WakeEconomics::UrgencyGated, None) => unreachable!(
            "UrgencyGated push target carries no wake_min — push_targets invariant violated"
        ),
    };
    if !eager_wake {
        // Reachable only for `UrgencyGated` subscribers — a designed park
        // (conversation economics), not stranding, but a traced decision where
        // the stranded-surface-push failure was silent at every level.
        debug!(
            subscriber = %target.subscriber.as_str(),
            channel = %channel_address,
            ?urgency,
            wake_min = target.wake_min.map(|w| w.as_str()),
            "delivery claim created without eager wake — parked pending subscriber's next wake",
        );
    }
    eager_wake
}

/// The delivery targets a store records for one message, resolved from the
/// channel's push targets against that message's urgency and deadline.
///
/// Every entry must be push-enabled — a store cannot hold a window for a
/// depth-0 subscriber.
fn delivery_targets(
    targets: &[PushTarget],
    urgency: Urgency,
    channel_address: &str,
    delivery_deadline: Option<DateTime<Utc>>,
) -> Vec<DeliveryTarget> {
    targets
        .iter()
        .map(|target| DeliveryTarget {
            subscriber: target.subscriber.clone(),
            app_slug: target.app_slug.clone(),
            eager_wake: eager_wake_for(target, urgency, channel_address),
            delivery_deadline,
            push_depth: target.push_depth,
        })
        .collect()
}
