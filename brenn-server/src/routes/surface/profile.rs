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

use brenn_lib::messaging::ParticipantId;
use brenn_lib::messaging::config::{Depth, ResolvedSurface};

use crate::routes::attach::profile::{AttachProfile, DeferredTarget, SubscriptionFacts};
use crate::routes::attach::registry::SessionCaps;

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
                attribution: output.instance.clone(),
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

    fn send_budget_scope(&self) -> &str {
        &self.slug
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

    fn session_caps(&self) -> SessionCaps {
        // Compiled-in for every surface: the caps bound what an authenticated
        // account can pin of a shared page, and no surface has a reason to
        // differ from another yet. Per-surface config is an additive change.
        SessionCaps {
            per_attacher: MAX_SESSIONS_PER_SURFACE,
            per_account: MAX_SESSIONS_PER_USER_PER_SURFACE,
        }
    }
}

/// Boot-time cross-check that the two lowerings of one surface's authority — the
/// attachment-grain profile and the port-grain maps the session dispatches on —
/// describe the same thing.
///
/// Both are derived from the same resolved config, so a disagreement is a bug in
/// one of the derivations, not a condition: a channel subscribable at one grain
/// and not the other, or a bound output no attribution may publish, would show
/// up as a page that silently loses traffic. Boot is where that dies.
///
/// # Panics
///
/// On any disagreement, and on a port map naming an instance neither the
/// component set nor the reserved error-report grain accounts for.
pub fn assert_agrees_with_port_maps(runtime: &super::SurfaceRuntime) {
    let profile = &runtime.profile;
    let slug = &runtime.resolved.slug;
    assert_eq!(
        profile.attacher(),
        &runtime.participant,
        "surface {slug:?}: profile and runtime disagree about the bare identity"
    );
    assert_eq!(
        profile.send_budget_scope(),
        slug.as_str(),
        "surface {slug:?}: profile send-budget scope is not the surface slug"
    );

    // Subscribe: the profile's per-channel entry is the max fold of every
    // instance's entry on that channel, and it covers exactly those channels
    // plus the injected config channel.
    // Owned keys: this runs once per surface at boot, where the allocation buys
    // one fold rule instead of two spellings of it.
    let mut folded: HashMap<String, SubscriptionFacts> = HashMap::new();
    for (sub, facts) in &runtime.subscription_channels {
        fold_subscription(&mut folded, sub.channel.as_str(), *facts);
    }
    fold_subscription(
        &mut folded,
        runtime.description.config_channel.as_str(),
        SubscriptionFacts {
            push_depth: 1,
            retain_depth: 1,
        },
    );
    assert_eq!(
        profile.subscribable.len(),
        folded.len(),
        "surface {slug:?}: profile subscribable set and the per-instance subscription map cover \
         different channels"
    );
    for (channel, facts) in &folded {
        assert_eq!(
            profile.subscribable(channel),
            Some(*facts),
            "surface {slug:?}: profile subscribable facts for {channel} do not fold the \
             per-instance subscriptions"
        );
    }

    // Publish: every bound output port is publishable by the attribution that
    // owns it. The reserved error-report port names no component, so it is the
    // one port whose attribution is the bare identity.
    for ((instance, port), out) in &runtime.output_ports {
        if brenn_surface_contract::is_error_report_port(instance, port) {
            assert!(
                profile.publishable(None, &out.address),
                "surface {slug:?}: reserved error-report channel {} is not publishable by the \
                 bare identity",
                out.address
            );
            for declared in &runtime.resolved.components {
                assert!(
                    profile.publishable(Some(&declared.instance), &out.address),
                    "surface {slug:?}: error channel {} is not publishable by declared \
                     attribution {}",
                    out.address,
                    declared.instance
                );
            }
            continue;
        }
        assert!(
            profile.publishable(Some(instance), &out.address),
            "surface {slug:?}: bound output {instance}/{port} onto {} is not publishable by its \
             own attribution",
            out.address
        );
    }
    // The telemetry pair is the bare identity's alone: a component-attributed
    // publish onto it must be refused, which is what keeps the single-writer
    // property the boot sweep proves for every other principal true for this
    // surface's own components too.
    for channel in [
        &runtime.description.geometry_channel,
        &runtime.description.status_channel,
    ] {
        assert!(
            profile.publishable(None, channel),
            "surface {slug:?}: telemetry channel {channel} is not publishable by the bare identity"
        );
        for declared in &runtime.resolved.components {
            assert!(
                !profile.publishable(Some(&declared.instance), channel),
                "surface {slug:?}: telemetry channel {channel} is publishable by component \
                 attribution {}",
                declared.instance
            );
        }
    }

    // Attribution: every declared instance mints its own sub-identity, and
    // nothing else mints at all.
    assert_eq!(
        profile.admit_attribution(None).as_ref(),
        Some(&runtime.participant),
        "surface {slug:?}: the absent attribution does not mint the bare identity"
    );
    for declared in &runtime.resolved.components {
        assert_eq!(
            profile.admit_attribution(Some(&declared.instance)),
            Some(ParticipantId::for_surface_component(
                slug,
                &declared.instance
            )),
            "surface {slug:?}: declared instance {} does not mint its own sub-identity",
            declared.instance
        );
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
