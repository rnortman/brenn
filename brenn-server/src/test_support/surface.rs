use brenn_lib::access::AppPolicy;
use brenn_lib::messaging::Urgency;
use brenn_lib::messaging::config::{
    ResolvedComponent, ResolvedSubscription, ResolvedSurface, ResolvedSurfaceSubscription,
    SurfaceBinding, SurfaceOutput, SurfaceSendBudget,
};

/// The `[surface_description]` parameters a runtime fixture carries. Taken from
/// the config section's own defaults, so a fixture's derived telemetry channel
/// addresses and heartbeat cadence read like an operator's who tuned nothing.
pub(crate) fn description_params() -> crate::routes::surface::SurfaceDescriptionParams {
    let config = brenn_lib::config::SurfaceDescriptionConfig::default();
    crate::routes::surface::SurfaceDescriptionParams {
        prefix: config.prefix,
        status_interval_secs: config.status_interval_secs,
    }
}

/// Fluent builder for `ResolvedSurface` test fixtures.
///
/// Starts from a one-component surface with no bindings, default policy, any
/// authenticated user, and the default publish token bucket (60 burst /
/// 1 per-sec). Each surface test that hand-built a full-field literal can chain
/// only the fields it cares about, so a new `ResolvedSurface` field no longer
/// forces parallel edits at every fixture site.
pub(crate) struct SurfaceFixture {
    inner: ResolvedSurface,
}

impl SurfaceFixture {
    /// A surface with the given slug and a single component kind.
    pub(crate) fn new(slug: &str, component: &str) -> Self {
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
                    abi: brenn_surface_proto::Abi::Dom,
                    send_budget: SurfaceSendBudget::default(),
                    parked_batch_depth: 8,
                    config: Default::default(),
                    chrome: true,
                }],
                subscriptions: vec![],
                durable_subscriptions: vec![],
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
    pub(crate) fn processor(
        mut self,
        instance: &str,
        kind: &str,
        config: std::collections::BTreeMap<String, String>,
    ) -> Self {
        self.inner.components.push(ResolvedComponent {
            instance: instance.to_string(),
            kind: kind.to_string(),
            abi: brenn_surface_proto::Abi::Processor,
            send_budget: SurfaceSendBudget::default(),
            parked_batch_depth: 8,
            config,
            chrome: false,
        });
        self
    }

    /// Append an input binding (channel → component/port) at the stock depths: a
    /// page queue of 8, no retained context.
    pub(crate) fn subscribe(self, channel_address: &str, component: &str, port: &str) -> Self {
        self.subscribe_at_depths(channel_address, component, port, 8, 0)
    }

    /// Append an input binding at explicit depths — for tests about the depths
    /// themselves. A `push_depth` of 0 is a context feed: rows flow, no push
    /// window exists behind them. Boot rejects that on a `dom` binding, which is
    /// every binding an operator can currently declare, so it is reachable only
    /// from here.
    pub(crate) fn subscribe_at_depths(
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
    pub(crate) fn allowed_users(mut self, users: Vec<String>) -> Self {
        self.inner.allowed_users = users;
        self
    }

    /// Set the resolved access-control policy (default is `AppPolicy::default()`).
    pub(crate) fn policy(mut self, policy: AppPolicy) -> Self {
        self.inner.policy = policy;
        self
    }

    /// Append an output binding (component/port → channel).
    pub(crate) fn output(mut self, channel_address: &str, component: &str, port: &str) -> Self {
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
    pub(crate) fn publish_rate(mut self, burst: u32, per_sec: u32) -> Self {
        self.inner.publish_burst = burst;
        self.inner.publish_per_sec = per_sec;
        self
    }

    /// Append a resolved durable (`brenn:`) input subscription owned by
    /// `instance`.
    pub(crate) fn durable_subscribe(mut self, instance: &str, sub: ResolvedSubscription) -> Self {
        self.inner
            .durable_subscriptions
            .push(ResolvedSurfaceSubscription {
                instance: instance.to_owned(),
                subscription: sub,
            });
        self
    }

    /// Set the skin (default `"bench"`).
    #[allow(dead_code)]
    pub(crate) fn skin(mut self, skin: &str) -> Self {
        self.inner.skin = skin.to_string();
        self
    }

    /// Finish building.
    pub(crate) fn build(self) -> ResolvedSurface {
        self.inner
    }
}
