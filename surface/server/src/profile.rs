//! The browser surface's attachment profile: its component structure, lowered
//! to the channel-and-attribution grain the attachment session speaks.
//!
//! The operator writes components, ports, and bindings; the wire carries
//! channels and an opaque attribution. This module is the boot-time lowering
//! between the two — the per-channel subscription fold, the per-attribution
//! publishable sets, the declared sub-identity set, and the parked-view targets.
//! Nothing here is per-connection: one profile per surface, shared by every
//! session attached to it.

#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet};

use brenn_envelope::grants::AppCapability;
use brenn_lib::messaging::config::{Depth, ResolvedSurface};
use brenn_lib::messaging::{AttachScope, ComponentGrant, ParticipantId, SubscriberEntry};
use brenn_messaging::MissingChannelPosture;

use brenn_attach_server::profile::{
    AttachProfile, DeferredTarget, PublishPosture, PublishRate, SubscriptionFacts,
};
use brenn_attach_server::registry::SessionCaps;

use super::{
    MAX_SESSIONS_PER_SURFACE, MAX_SESSIONS_PER_USER_PER_SURFACE, SurfaceDescriptionRuntime,
    assert_transportable,
};

/// The surface's boot-resolved authority, at attachment grain.
///
/// Built once per surface. The maps are probed on the frame path, so they are
/// keyed by exactly what an inbound frame carries — a channel address, and an
/// attribution string — with no key construction per lookup.
pub struct SurfaceProfile {
    /// The surface slug: the send-budget scope, and the slug half of every
    /// principal this profile mints.
    slug: String,
    /// `surface:<slug>` — the bare identity, for a publish that names no
    /// attribution.
    attacher: ParticipantId,
    /// Channel → the folded facts delivery turns on. The fold is by max across
    /// every declared binding on the channel, because one channel reaches one
    /// attachment once however many components sit behind it, and the widest
    /// window any of them asked for is the one that must arrive.
    subscribable: HashMap<String, SubscriptionFacts>,
    /// Every declared component instance → the channels it may publish onto. An
    /// instance with no output bindings is present with an empty set: it is
    /// still a declared attribution, and the error channel joins every set.
    component_publishable: HashMap<String, HashSet<String>>,
    /// The channels the bare identity may publish onto: the platform telemetry
    /// pair, plus the error channel when one is configured.
    ///
    /// Deliberately disjoint from the component sets on the telemetry pair. The
    /// surface's telemetry documents are single-writer under the bare identity,
    /// and boot's single-writer sweep proves no *other* principal can reach
    /// them; this set is what keeps the surface's own components off them at
    /// runtime, so the property holds all the way down.
    kernel_publishable: HashSet<String>,
    /// The substrate error channel, when one is configured — the surface's own
    /// diagnostics path, and the one channel whose publish refusals are reported
    /// rather than fatal.
    error_channel: Option<String>,
    /// Whether this surface's operator-written policy grants the alert plane.
    /// Read at boot from the same policy the route resolved, so the flag the
    /// attachment advertises and the one every `Alert` frame is judged against
    /// are one answer.
    alert_granted: bool,
    /// The declared instances whose own grants name `alert` — the containment
    /// half of the plane, disjoint in purpose from `alert_granted`: that one is
    /// the surface's transport right toward the backend, this one says which of
    /// the components behind it may spend it. Boot refuses an instance `alert`
    /// grant on a surface without the surface-level one, so a name here implies
    /// `alert_granted`.
    component_alertable: HashSet<String>,
    /// The operator-tuned per-connection publish bucket every session of this
    /// surface starts full with.
    publish_rate: PublishRate,
    /// Parked-view seeding targets, sorted and deduped.
    deferred_targets: Vec<DeferredTarget>,
}

impl SurfaceProfile {
    /// Lower one resolved surface onto the attachment grain.
    ///
    /// The error channel is not a parameter: it is `[observability]` config that
    /// the surface map's builder holds, and it is bound afterwards with
    /// [`SurfaceProfile::bind_error_channel`] — the same shape the reserved
    /// error-report wiring already has.
    ///
    /// # Panics
    ///
    /// On a wire subscription whose resolved depths are not bounded, or on a
    /// binding address that is not a transportable surface-bindable channel.
    /// Boot proves both, so either is a broken boot invariant rather than a
    /// condition to handle.
    pub fn build(resolved: &ResolvedSurface, description: &SurfaceDescriptionRuntime) -> Self {
        let slug = resolved.slug.clone();
        let mut subscribable: HashMap<String, SubscriptionFacts> = HashMap::new();
        // `wire_subscriptions` is already the transportable half — `local:`
        // bindings are the page's own router business and never appear there —
        // so this is the whole set an attachment may name.
        for wire in &resolved.wire_subscriptions {
            let channel = &wire.subscription.channel_address;
            assert_transportable(channel);
            fold_subscription(
                &mut subscribable,
                channel,
                SubscriptionFacts {
                    push_depth: bounded_depth(
                        wire.subscription.push_depth,
                        "push_depth",
                        &slug,
                        channel,
                    ),
                    retain_depth: bounded_depth(
                        wire.subscription.retain_depth,
                        "retain_depth",
                        &slug,
                        channel,
                    ),
                },
            );
        }
        // The two boot resolvers must agree about which bindings cross the wire.
        // A transportable binding with no wire subscription reaches the page in
        // the bindings document (built from `subscriptions`) while being absent
        // from the attachment's authority, so the page subscribes what it was
        // told to and is killed for a protocol violation at runtime. Both lists
        // come off one resolver loop today, which is the argument for keeping the
        // cheap assert rather than for dropping it.
        for binding in &resolved.subscriptions {
            if brenn_envelope::is_local_channel(&binding.channel_address) {
                continue;
            }
            assert!(
                subscribable.contains_key(&binding.channel_address),
                "surface {slug:?}: transportable binding {} (instance {:?}) has no resolved wire \
                 subscription — the binding resolver and the subscription resolver disagree about \
                 which bindings cross the wire",
                binding.channel_address,
                binding.instance,
            );
        }

        // The config channel carries the surface's own bindings document, which
        // it must read to be configured at all, so the subscribe right is
        // substrate rather than operator config — injected here exactly as boot
        // injects the matching ACL. Depth 1 both ways: the document is
        // latest-wins state and the attachment wants the one current row.
        fold_subscription(
            &mut subscribable,
            &description.config_channel,
            SubscriptionFacts {
                push_depth: 1,
                retain_depth: 1,
            },
        );

        // Every declared instance is a key, including one with no outputs: it is
        // a declared attribution (the admission check reads this map), and the
        // error channel is publishable by every one of them.
        let mut component_publishable: HashMap<String, HashSet<String>> = resolved
            .components
            .iter()
            .map(|c| (c.instance.clone(), HashSet::new()))
            .collect();
        let mut deferred_targets: Vec<DeferredTarget> = Vec::new();
        for output in &resolved.outputs {
            if brenn_envelope::is_local_channel(&output.channel_address) {
                continue;
            }
            assert_transportable(&output.channel_address);
            let channels = component_publishable
                .get_mut(&output.instance)
                .unwrap_or_else(|| {
                    panic!(
                        "surface {slug:?}: output binding {}/{} names an instance absent from the \
                         resolved component set — boot resolves every binding against that set",
                        output.instance, output.port,
                    )
                });
            channels.insert(output.channel_address.clone());
            deferred_targets.push(DeferredTarget {
                channel: output.channel_address.clone(),
                attribution: Some(output.instance.clone()),
            });
        }
        // Two ports of one instance may share a channel; they share one parked
        // set and are seeded once.
        deferred_targets.sort();
        deferred_targets.dedup();

        let kernel_publishable: HashSet<String> = [
            description.geometry_channel.clone(),
            description.status_channel.clone(),
        ]
        .into_iter()
        .collect();

        SurfaceProfile {
            attacher: ParticipantId::for_surface(&slug),
            slug,
            subscribable,
            component_publishable,
            kernel_publishable,
            error_channel: None,
            alert_granted: resolved.policy.grants.has(AppCapability::SurfaceAlert),
            component_alertable: resolved
                .components
                .iter()
                .filter(|c| c.grants.contains(&ComponentGrant::Alert))
                .map(|c| c.instance.clone())
                .collect(),
            publish_rate: PublishRate {
                burst: resolved.publish_burst,
                per_sec: resolved.publish_per_sec,
            },
            deferred_targets,
        }
    }

    /// Admit the substrate error channel: every declared attribution and the
    /// bare identity may publish onto it.
    ///
    /// Many-writer by design — every surface reports onto one operator channel,
    /// and a component's report carries that component's sender — so this widens
    /// every set rather than minting a grain of its own.
    pub fn bind_error_channel(&mut self, channel: &str) {
        for channels in self.component_publishable.values_mut() {
            channels.insert(channel.to_string());
        }
        self.kernel_publishable.insert(channel.to_string());
        self.error_channel = Some(channel.to_string());
    }

    /// Whether `instance` is in this surface's boot-resolved declaration set.
    ///
    /// The kind is deliberately not consulted. It is the manifest — a load-time
    /// compatibility fact and an observability decoration — and never holds
    /// authority.
    pub fn is_declared(&self, instance: &str) -> bool {
        self.component_publishable.contains_key(instance)
    }
}

impl AttachProfile for SurfaceProfile {
    fn attacher(&self) -> &ParticipantId {
        &self.attacher
    }

    fn subscribable(&self, channel: &str) -> Option<SubscriptionFacts> {
        self.subscribable.get(channel).copied()
    }

    fn publishable(&self, attribution: Option<&str>, channel: &str) -> bool {
        match attribution {
            None => self.kernel_publishable.contains(channel),
            Some(instance) => self
                .component_publishable
                .get(instance)
                .is_some_and(|channels| channels.contains(channel)),
        }
    }

    fn admit_attribution(&self, attribution: Option<&str>) -> Option<ParticipantId> {
        match attribution {
            None => Some(self.attacher.clone()),
            // Membership first, minting second: the minting guards panic on a
            // malformed instance id, and this argument came off the wire.
            Some(instance) if self.is_declared(instance) => {
                Some(ParticipantId::for_surface_component(&self.slug, instance))
            }
            Some(_) => None,
        }
    }

    fn publish_posture(&self, channel: &str) -> PublishPosture {
        // Everything a surface publishes rides an operator allowlist that boot
        // validated, so a refusal is a broken invariant — except on the error
        // channel, which is where the shell reports its own failures. A publish
        // failure there must survive as a log line and an answer, not as a
        // process death on an attacker-sendable path.
        match &self.error_channel {
            Some(error) if error == channel => PublishPosture::Diagnostic,
            _ => PublishPosture::Invariant,
        }
    }

    fn attach_scope(&self) -> AttachScope<'_> {
        AttachScope::surface(&self.slug)
    }

    fn missing_channel_posture(&self) -> MissingChannelPosture {
        // Every channel a surface may publish is a boot-declared output that
        // boot validation proved exists and is policy-covered, so a flush
        // naming one the server cannot write is the server disagreeing with
        // itself.
        MissingChannelPosture::Invariant
    }

    fn deferred_view_targets(&self) -> &[DeferredTarget] {
        &self.deferred_targets
    }

    fn subscribe_burst(&self) -> u32 {
        // Derived — never a literal — from the boot-enforced maximum binding
        // count, so the two can never drift. The kernel's reconnect reconcile
        // sends one `Subscribe` per bound channel in a single first-connect
        // burst, so any literal below the maximum would refuse a boot-valid
        // maximum-size surface. `3×` admits that reconcile plus one full
        // detach/re-attach cycle of a maximum-size surface (MAX unsubscribes +
        // MAX subscribes); churn beyond that is throttled to one token/sec.
        3 * brenn_surface_schema::MAX_SURFACE_SUBSCRIPTION_BINDINGS as u32
    }

    fn publish_rate(&self) -> PublishRate {
        self.publish_rate
    }

    fn alert_granted(&self) -> bool {
        self.alert_granted
    }

    fn attribution_may_alert(&self, attribution: &str) -> bool {
        self.component_alertable.contains(attribution)
    }

    fn session_caps(&self) -> SessionCaps {
        // Compiled-in for every surface: the caps bound what an authenticated
        // account can pin of a shared page, and no surface has a reason to
        // differ from another yet. Per-surface config is an additive change.
        SessionCaps {
            per_attacher: MAX_SESSIONS_PER_SURFACE,
            per_account: MAX_SESSIONS_PER_USER_PER_SURFACE,
        }
    }

    fn max_active_subscriptions(&self) -> usize {
        // The map that answers `subscribable`, so the cap is exactly what this
        // surface may hold and can never drift from it. Unreachable in practice
        // — a second Subscribe on an already-active channel violates first — and
        // that is the point: a boot-enumerated attacher gets the cap for free.
        self.subscribable.len()
    }

    fn runtime_entry(&self, _channel: &str) -> Option<SubscriberEntry> {
        // A surface's directory entries are folded from its declared bindings
        // at boot, before it can attach at all.
        None
    }
}

/// Fold one binding's facts into the per-channel entry, widening both knobs.
fn fold_subscription(
    map: &mut HashMap<String, SubscriptionFacts>,
    channel: &str,
    facts: SubscriptionFacts,
) {
    map.entry(channel.to_string())
        .and_modify(|folded| {
            folded.push_depth = folded.push_depth.max(facts.push_depth);
            folded.retain_depth = folded.retain_depth.max(facts.retain_depth);
        })
        .or_insert(facts);
}

/// Read one wire subscription's resolved depth as the number boot proved it to
/// be.
///
/// # Panics
///
/// On an unbounded depth: every replay the wire serves must be bounded, and boot
/// refuses the config that would leave one otherwise, so this is a broken boot
/// invariant rather than a condition to handle.
fn bounded_depth(depth: Depth, knob: &str, slug: &str, channel: &str) -> u64 {
    let Depth::Bounded(n) = depth else {
        panic!(
            "surface {slug:?}: wire subscription {channel} resolves an unbounded {knob} — boot \
             bounds both depths of every wire subscription"
        )
    };
    n
}
