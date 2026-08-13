use brenn_lib::access::AppPolicy;
use brenn_lib::messaging::Urgency;
use brenn_lib::messaging::config::{
    AttachSendBudget, NoiseLevel, ResolvedComponent, ResolvedSubscription, ResolvedSurface,
    ResolvedSurfaceSubscription, SurfaceBinding, SurfaceOutput,
};

/// The `[surface_description]` parameters a runtime fixture carries. Taken from
/// the config section's own defaults, so a fixture's derived telemetry channel
/// addresses read like an operator's who tuned nothing.
pub fn description_params() -> crate::SurfaceDescriptionParams {
    let config = brenn_lib::config::SurfaceDescriptionConfig::default();
    crate::SurfaceDescriptionParams {
        prefix: config.prefix,
    }
}

/// Fluent builder for `ResolvedSurface` test fixtures.
///
/// Starts from a one-component surface with no bindings, default policy, any
/// authenticated user, and the default publish token bucket (60 burst /
/// 1 per-sec). Each surface test that hand-built a full-field literal can chain
/// only the fields it cares about, so a new `ResolvedSurface` field no longer
/// forces parallel edits at every fixture site.
pub struct SurfaceFixture {
    inner: ResolvedSurface,
}

impl SurfaceFixture {
    /// A surface with the given slug and a single component kind.
    pub fn new(slug: &str, component: &str) -> Self {
        Self {
            inner: ResolvedSurface {
                slug: slug.to_string(),
                skin: "bench".to_string(),
                // The lone component doubles as the surface's chrome singleton so
                // the fixture satisfies the exactly-one-chrome invariant the build
                // path relies on.
                components: vec![ResolvedComponent {
                    instance: component.to_string(),
                    kind: component.to_string(),
                    abi: brenn_surface_schema::Abi::Dom,
                    send_budget: AttachSendBudget::default(),
                    parked_batch_depth: 8,
                    config: Default::default(),
                    chrome: true,
                }],
                subscriptions: vec![],
                wire_subscriptions: vec![],
                local_channels: vec![],
                outputs: vec![],
                policy: AppPolicy::default(),
                allowed_users: vec![],
                publish_burst: 60,
                publish_per_sec: 1,
            },
        }
    }

    /// Append a headless `processor` component instance with the given config map.
    /// Never chrome — chrome is a `dom` component by definition.
    pub fn processor(
        mut self,
        instance: &str,
        kind: &str,
        config: std::collections::BTreeMap<String, String>,
    ) -> Self {
        self.inner.components.push(ResolvedComponent {
            instance: instance.to_string(),
            kind: kind.to_string(),
            abi: brenn_surface_schema::Abi::Processor,
            send_budget: AttachSendBudget::default(),
            parked_batch_depth: 8,
            config,
            chrome: false,
        });
        self
    }

    /// Append an input binding (channel → component/port) at the stock depths: a
    /// page queue of 8, no retained context.
    pub fn subscribe(self, channel_address: &str, component: &str, port: &str) -> Self {
        self.subscribe_at_depths(channel_address, component, port, 8, 0)
    }

    /// Append an input binding at explicit depths — for tests about the depths
    /// themselves. A `push_depth` of 0 is a context feed: rows flow, no push
    /// window exists behind them. Boot rejects that on a `dom` binding, which is
    /// every binding an operator can currently declare, so it is reachable only
    /// from here.
    pub fn subscribe_at_depths(
        mut self,
        channel_address: &str,
        component: &str,
        port: &str,
        push_depth: u64,
        retain_depth: u64,
    ) -> Self {
        self.inner.subscriptions.push(SurfaceBinding {
            channel_address: channel_address.to_string(),
            instance: component.to_string(),
            port: port.to_string(),
            push_depth,
            retain_depth,
            noise: brenn_lib::messaging::config::NoiseLevel::Silent,
        });
        self
    }

    /// Restrict attach access to the given usernames (empty ⇒ any user).
    pub fn allowed_users(mut self, users: Vec<String>) -> Self {
        self.inner.allowed_users = users;
        self
    }

    /// Set the resolved access-control policy (default is `AppPolicy::default()`).
    pub fn policy(mut self, policy: AppPolicy) -> Self {
        self.inner.policy = policy;
        self
    }

    /// Append an output binding (component/port → channel).
    pub fn output(mut self, channel_address: &str, component: &str, port: &str) -> Self {
        self.inner.outputs.push(SurfaceOutput {
            channel_address: channel_address.to_string(),
            instance: component.to_string(),
            port: port.to_string(),
            default_urgency: Urgency::Normal,
            budget: brenn_budget::SinkBudget {
                fill_mt: brenn_budget::MILLITOKENS_PER_PUBLISH,
                capacity_mt: brenn_budget::MILLITOKENS_PER_PUBLISH,
            },
        });
        self
    }

    /// Set the connection's publish token bucket (default 60 burst / 1 per-sec).
    pub fn publish_rate(mut self, burst: u32, per_sec: u32) -> Self {
        self.inner.publish_burst = burst;
        self.inner.publish_per_sec = per_sec;
        self
    }

    /// Set the skin (default `"bench"`).
    #[allow(dead_code)]
    pub fn skin(mut self, skin: &str) -> Self {
        self.inner.skin = skin.to_string();
        self
    }

    /// Finish building, deriving any missing wire subscriptions
    /// ([`derive_wire_subscriptions`]).
    pub fn build(mut self) -> ResolvedSurface {
        derive_wire_subscriptions(&mut self.inner);
        self.inner
    }
}

/// Give every transportable binding that lacks one the wire subscription boot
/// would have resolved for it: both of the binding's own depths, folded by max
/// across the bindings of one `(instance, channel)` exactly as the boot resolver
/// folds them.
///
/// Boot resolves this from the config ladder; a fixture hands over an already
/// resolved surface, so the two halves are re-joined here instead. A fixture may
/// state a wire subscription itself — a `channel_uuid` the rig needs, say — but
/// its depths must be the ones its own bindings fold to, and a disagreement
/// panics: a stated pair boot could not have produced would pin a replay clamp
/// no config can reach, and the behavior under it would be untestable fiction.
/// Every rig that installs surfaces onto an `AppState` runs this.
///
/// A derived subscription's `channel_uuid` is left [`uuid::Uuid::nil`]: the
/// bindings name addresses, and only the channel entry set knows which uuid an
/// address wears. [`bind_wire_subscription_uuids`] fills them in against those
/// entries, and a nil left standing is a subscription no directory was ever built
/// from.
///
/// # Panics
///
/// If a stated wire subscription's depths differ from the max-fold of the
/// bindings of its `(instance, channel)`.
pub fn derive_wire_subscriptions(surface: &mut ResolvedSurface) {
    use brenn_lib::messaging::config::Depth;
    // Fold the bindings first, in declaration order, so a stated entry is checked
    // against the whole fold rather than against whichever binding the loop
    // reaches first — and so the derived entries land in a deterministic order.
    let mut folded: Vec<(String, String, Depth, Depth, NoiseLevel)> = Vec::new();
    for binding in &surface.subscriptions {
        if brenn_envelope::is_local_channel(&binding.channel_address) {
            continue;
        }
        let push = Depth::Bounded(binding.push_depth);
        let retain = Depth::Bounded(binding.retain_depth);
        match folded
            .iter_mut()
            .find(|(i, c, ..)| *i == binding.instance && *c == binding.channel_address)
        {
            Some((_, _, p, r, _)) => {
                *p = p.widened_by(push);
                *r = r.widened_by(retain);
            }
            None => folded.push((
                binding.instance.clone(),
                binding.channel_address.clone(),
                push,
                retain,
                binding.noise,
            )),
        }
    }

    for (instance, channel, push_depth, retain_depth, noise) in folded {
        let stated = surface
            .wire_subscriptions
            .iter()
            .find(|s| s.instance == instance && s.subscription.channel_address == channel);
        match stated {
            Some(s) => assert!(
                s.subscription.push_depth == push_depth
                    && s.subscription.retain_depth == retain_depth,
                "surface {:?}: stated wire subscription {channel} (instance {instance:?}) \
                 declares push {:?} / retain {:?}, but its bindings fold to push {push_depth:?} \
                 / retain {retain_depth:?} — boot resolves the wire depths from the bindings, \
                 so a fixture stating others pins a clamp no config produces",
                surface.slug,
                s.subscription.push_depth,
                s.subscription.retain_depth,
            ),
            None => surface
                .wire_subscriptions
                .push(ResolvedSurfaceSubscription {
                    instance,
                    subscription: ResolvedSubscription {
                        channel_uuid: uuid::Uuid::nil(),
                        channel_address: channel,
                        push_depth,
                        retain_depth,
                        noise,
                        wake_min: brenn_lib::messaging::WakeMin::Normal,
                    },
                }),
        }
    }
}

/// Join every wire subscription to the channel entry its address names, taking
/// that entry's uuid.
///
/// Boot keys a surface's subscriber entry by uuid, so a fixture that invented one
/// would carry a value boot could never produce and would be fed nothing for a
/// reason no assertion states. The rigs join here on the one thing both halves
/// genuinely share — the address — and every directory built downstream is keyed
/// on a uuid that means what it says.
///
/// A subscription whose address the rig declares no entry for keeps its nil
/// uuid: a transport-plane fixture declares no channels at all and never
/// delivers, and a nil matches no entry, so such a subscription is inert by
/// construction rather than by an accident of non-matching.
///
/// # Panics
///
/// If a fixture stated a uuid that is not the one its channel entry wears.
pub fn bind_wire_subscription_uuids(
    surface: &mut ResolvedSurface,
    entries: &[brenn_lib::messaging::ChannelEntry],
) {
    for sub in &mut surface.wire_subscriptions {
        let address = &sub.subscription.channel_address;
        let Some(entry) = entries.iter().find(|e| e.address == *address) else {
            continue;
        };
        assert!(
            sub.subscription.channel_uuid.is_nil() || sub.subscription.channel_uuid == entry.uuid,
            "surface {:?}: wire subscription on {address} (instance {:?}) states uuid {} but its \
             channel entry wears {} — the directory is keyed by uuid, so a subscription stating \
             another one is fed nothing",
            surface.slug,
            sub.instance,
            sub.subscription.channel_uuid,
            entry.uuid,
        );
        sub.subscription.channel_uuid = entry.uuid;
    }
}
