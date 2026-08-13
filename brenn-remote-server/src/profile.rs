//! The remote's attachment profile: a `[[remote]]` block lowered to the
//! channel-and-attribution grain the attachment session speaks.
//!
//! Where the surface profile lowers *structure* — components, ports, bindings —
//! this lowers *matchers*. A remote's channels come into being at runtime (a
//! conversation is created, a robot grows an arm), so there is no set to
//! enumerate at boot and every authority answer is a matcher fold instead of a
//! map lookup. That single difference is what the two trait members added for
//! this route exist to cover: the subscription cap a prefix grant would
//! otherwise leave unbounded, and the directory entry no boot-time fold could
//! have minted.
//!
//! Nothing here is per-connection: one profile per `[[remote]]`, shared by every
//! session attached under its slug.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use brenn_envelope::ChannelScheme;
use brenn_lib::access::{AppCapability, AppPolicy};
use brenn_lib::messaging::config::{Depth, NoiseLevel};
use brenn_lib::messaging::remote::{RemoteDepths, RemoteSubscribeAcl, ResolvedRemote};
use brenn_lib::messaging::{AttachScope, ParticipantId, SubscriberEntry, SubscriberEntryKind};
use brenn_messaging::MissingChannelPosture;

use brenn_attach_server::profile::{
    AttachProfile, DeferredTarget, PublishPosture, PublishRate, SubscriptionFacts,
};
use brenn_attach_server::registry::SessionCaps;

/// How much subscribe/unsubscribe burst a remote is admitted beyond its own
/// subscription cap.
///
/// A correct remote's worst honest burst is one full reconcile: every channel it
/// held unsubscribed and every channel the new roster names subscribed, in one
/// pass. That is twice the cap, which is what this multiplies to; churn past it
/// is throttled to the shared one-token-per-second refill.
const SUBSCRIBE_BURST_RECONCILES: u32 = 2;

/// One `[[remote]]`'s boot-resolved authority, at attachment grain.
///
/// Built once per configured remote. Every field is either the resolved config
/// verbatim or a form of it the frame path can read without allocating, because
/// these answers are consulted per inbound frame.
pub struct RemoteProfile {
    /// The remote slug: the send-budget scope, and the slug half of the one
    /// principal this profile mints.
    slug: String,
    /// `remote:<slug>` — the whole identity. A remote has no sub-identity grain:
    /// one daemon, one principal, one budget.
    attacher: ParticipantId,
    /// Durable (`brenn:`) subscribe ceilings — the matchers, and the depths a
    /// match is answered with.
    subscribe_ceilings: RemoteSubscribeAcl,
    /// Ephemeral (`ephemeral:`) subscribe ceilings.
    ephemeral_subscribe_ceilings: RemoteSubscribeAcl,
    /// The remote's resolved policy. The publish-direction answers read its
    /// ACLs, which is the same lowering the subscribe ceilings came out of, so
    /// the two directions cannot disagree about what the operator wrote.
    policy: Arc<AppPolicy>,
    /// Whether this remote's grants include the alert plane.
    alert_granted: bool,
    /// The operator-tuned per-connection publish bucket.
    publish_rate: PublishRate,
    /// Concurrent sessions admitted, per attacher and per account alike.
    max_sessions: usize,
    /// Concurrent subscriptions admitted per session.
    max_subscriptions: usize,
}

impl RemoteProfile {
    /// Lower one resolved `[[remote]]` onto the attachment grain.
    ///
    /// # Panics
    ///
    /// On a slug the participant-id guards reject. Boot's own charset check
    /// already refused those, so reaching one here is a broken boot invariant.
    pub fn build(resolved: &ResolvedRemote) -> Self {
        Self {
            slug: resolved.slug.clone(),
            attacher: ParticipantId::for_remote(&resolved.slug),
            subscribe_ceilings: resolved.subscribe_ceilings.clone(),
            ephemeral_subscribe_ceilings: resolved.ephemeral_subscribe_ceilings.clone(),
            policy: Arc::new(resolved.policy.clone()),
            alert_granted: resolved.policy.has_grant(AppCapability::SurfaceAlert),
            publish_rate: PublishRate {
                burst: resolved.publish_burst,
                per_sec: resolved.publish_per_sec,
            },
            max_sessions: resolved.max_sessions as usize,
            max_subscriptions: resolved.max_subscriptions as usize,
        }
    }

    /// The depths this remote's ACLs answer a subscribe of `channel` with, or
    /// `None` if no matcher of the address's scheme covers it.
    ///
    /// The match is exhaustive on [`ChannelScheme`], so a scheme added later
    /// fails compilation here rather than defaulting to admit. Every scheme a
    /// remote has no subscribe vocabulary for answers `None` — `local:` most
    /// pointedly, since a confined channel belongs to the host that holds it and
    /// no network principal may name one.
    fn ceiling_for(&self, channel: &str) -> Option<RemoteDepths> {
        match ChannelScheme::split(channel) {
            Some((ChannelScheme::Brenn, bare)) => self.subscribe_ceilings.ceiling_for(bare),
            Some((ChannelScheme::Ephemeral, bare)) => {
                self.ephemeral_subscribe_ceilings.ceiling_for(bare)
            }
            Some((
                ChannelScheme::Local
                | ChannelScheme::Mqtt
                | ChannelScheme::Webhook
                | ChannelScheme::PwaPush,
                _,
            ))
            | None => None,
        }
    }
}

impl AttachProfile for RemoteProfile {
    fn attacher(&self) -> &ParticipantId {
        &self.attacher
    }

    fn subscribable(&self, channel: &str) -> Option<SubscriptionFacts> {
        self.ceiling_for(channel).map(|depths| SubscriptionFacts {
            push_depth: depths.push_depth,
            retain_depth: depths.retain_depth,
        })
    }

    fn publishable(&self, attribution: Option<&str>, channel: &str) -> bool {
        // A remote publishes as itself and nothing else: any named attribution
        // is refused before the channel is even consulted.
        if attribution.is_some() {
            return false;
        }
        // Exhaustive on ChannelScheme, like `ceiling_for`: a new scheme is a
        // compile error, not a default-admit.
        match ChannelScheme::split(channel) {
            Some((ChannelScheme::Brenn, bare)) => self.policy.allows_brenn_publish(bare),
            Some((ChannelScheme::Ephemeral, bare)) => self.policy.allows_ephemeral_publish(bare),
            Some((
                ChannelScheme::Local
                | ChannelScheme::Mqtt
                | ChannelScheme::Webhook
                | ChannelScheme::PwaPush,
                _,
            ))
            | None => false,
        }
    }

    fn admit_attribution(&self, attribution: Option<&str>) -> Option<ParticipantId> {
        match attribution {
            None => Some(self.attacher.clone()),
            // No declared sub-identity set to admit from: a remote that names
            // one is naming something no operator wrote.
            Some(_) => None,
        }
    }

    fn publish_posture(&self, _channel: &str) -> PublishPosture {
        // Every channel, not just a diagnostics one. A remote's targets are
        // matcher-granted and provisioned at runtime, so a publish into a
        // conversation that was deprovisioned a moment ago is an ordinary race
        // the operator's own topology produces — a failed outcome the daemon
        // reconciles from, never a boot invariant the server may die over.
        PublishPosture::Diagnostic
    }

    fn attach_scope(&self) -> AttachScope<'_> {
        AttachScope::remote(&self.slug)
    }

    fn missing_channel_posture(&self) -> MissingChannelPosture {
        // The same call `publish_posture` makes, at the grain a whole flush is
        // decided on: a remote's targets are matcher-granted and provisioned at
        // runtime, so an entry naming one that deprovisioned mid-flush is an
        // ordinary race the daemon reconciles from.
        MissingChannelPosture::Race
    }

    fn deferred_view_targets(&self) -> &[DeferredTarget] {
        // Parked-set mirrors are cut at `(attribution, channel)`, and a remote
        // declares no attribution. Deferred publishes still park and release on
        // the server's clock; the mirror is optional visibility, not a
        // prerequisite for one.
        &[]
    }

    fn subscribe_burst(&self) -> u32 {
        self.max_subscriptions
            .try_into()
            .unwrap_or(u32::MAX)
            .saturating_mul(SUBSCRIBE_BURST_RECONCILES)
    }

    fn publish_rate(&self) -> PublishRate {
        self.publish_rate
    }

    fn alert_granted(&self) -> bool {
        self.alert_granted
    }

    fn session_caps(&self) -> SessionCaps {
        // The two grains collapse: the account behind a remote attachment *is*
        // `remote:<slug>`, so a per-account cap below the per-attacher one would
        // be the same number spelled twice, and one above it would never bind.
        SessionCaps {
            per_attacher: self.max_sessions,
            per_account: self.max_sessions,
        }
    }

    fn max_active_subscriptions(&self) -> usize {
        self.max_subscriptions
    }

    fn runtime_entry(&self, channel: &str) -> Option<SubscriberEntry> {
        let depths = self.ceiling_for(channel)?;
        Some(SubscriberEntry {
            kind: SubscriberEntryKind::Remote(self.slug.clone()),
            // The profile's ceilings, never the depths the client stated: two
            // sessions of one remote must mint the same entry, and what the
            // server holds open on a channel is the operator's sentence rather
            // than a number off the wire.
            push_depth: Depth::Bounded(depths.push_depth),
            retain_depth: Depth::Bounded(depths.retain_depth),
            // The loud half of the noise ladder is client-enacted for every
            // attach-shaped subscriber — the remote sees its own losses as the
            // `dropped` counts on its `Deliver` rows — so the backend rung only
            // has to be one the backend sink can survive.
            noise: NoiseLevel::Metered,
            // Eager economics, like every attacher-shaped kind: a remote is
            // woken by anything on a channel it holds, not by an urgency
            // threshold it never declared.
            wake_min: None,
        })
    }
}
