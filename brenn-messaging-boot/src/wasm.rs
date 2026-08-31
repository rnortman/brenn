use std::time::Duration;

use brenn_lib::messaging::config::{
    ActivationPacing, DEFAULT_WASM_INPUT_AMPLIFICATION, DEFAULT_WASM_PUBLISH_CAPACITY,
    DEFAULT_WASM_PUBLISH_PER_ACTIVATION, Depth, NoiseLevel, ResolvedSubscription,
    ResolvedWasmConsumer, WasmConsumerConfigRaw, WasmInputPort, WasmOutputPort, WasmSinkBudget,
};
use brenn_lib::messaging::{
    ComponentGrant, ComponentHost, EntityKind, MessagingDirectory, Plane, bindable_schemes,
};
use indexmap::IndexMap;

/// Default activation-pacing burst (token-bucket capacity, in activations) when
/// `[[wasm_consumer]].activation_burst` is unset. Generous enough that legitimate
/// interactive/bursty consumers never trip the gate; only sustained pathological
/// rates hit it.
pub(crate) const DEFAULT_ACTIVATION_BURST: u32 = 60;
/// Default activation-pacing minimum period (one activation admitted per interval
/// under sustained load) when `[[wasm_consumer]].activation_min_period_ms` is
/// unset. With batched delivery this caps sustained throughput at clamp-cap rows
/// per second per port — far above any legitimate consumer today.
pub(crate) const DEFAULT_ACTIVATION_MIN_PERIOD: Duration = Duration::from_millis(1000);

use super::auto::AutoWiring;
use super::resolve_publish_millitokens;

/// Resolve `[[wasm_consumer]]` blocks against the channel directory,
/// applying the sub → channel depth/noise inheritance and validating output port
/// bindings.
///
/// Returns `Vec<ResolvedWasmConsumer>` in declaration order.
///
/// Panics on:
/// - unknown channel address in any subscription or output block
/// - duplicate subscription for the same channel within one consumer
/// - duplicate slug across two `[[wasm_consumer]]` blocks
/// - slug containing `:` or `@` (rejected by `ParticipantId::for_wasm` constructor)
/// - duplicate port name within a consumer (across inputs and outputs; an
///   io_port registers its one name once, for both of its directions, so
///   declaring that name again in either split list is the collision)
/// - empty port name or port name containing non-unreserved chars
/// - output channel is not a pub/sub scheme (`brenn:`/`ephemeral:`/`local:`) —
///   `mqtt:`/`webhook:`/`pwa_push:` egress never rides the buffered path
/// - consumer has no subscriptions but has ≥1 output (dead config)
/// - duplicate grant entries in `grants` list
/// - `outputs` non-empty but `ports` not granted (dead config)
/// - `[wasm_consumer.config]` table present but `config` not granted (dead config)
/// - `store_path` or `store_size_limit` set without `store` grant; or `store` granted without `store_path`
/// - `store_path` present but parent directory missing
/// - `activation_burst` or `activation_min_period_ms` present but zero
///
/// Identity-collision dedup: builds the set of all `wasm:<slug>`
/// identities and panics on any duplicate. Cross-kind collisions between `wasm:`
/// and `app:` are structurally impossible (prefix-disjoint namespaces).
pub(crate) fn resolve_wasm_consumers(
    raw_consumers: &[WasmConsumerConfigRaw],
    directory: &MessagingDirectory,
    global_store_size_limit: &str,
    resolved_clients: &IndexMap<String, brenn_lib::mqtt::config::MqttClientConfig>,
    auto_wiring: &AutoWiring,
) -> Vec<ResolvedWasmConsumer> {
    use brenn_lib::config::wasm::{byte_size_to_max_page_count, resolve_component_config};
    use brenn_lib::messaging::is_unreserved_char;
    use std::collections::{BTreeSet, HashSet};

    // Declared `[[mqtt_client]]` membership comes from the canonical resolved
    // client map (the same one threaded into the LLM-side `validate_mqtt_client`),
    // so this check is against the exact registry `MqttService` is populated from —
    // no second, independently-derived slug set to drift out of sync.

    // Identity-collision dedup: panic on duplicate wasm: slugs.
    let mut seen_slugs: HashSet<&str> = HashSet::new();
    for c in raw_consumers {
        assert!(
            seen_slugs.insert(c.slug.as_str()),
            "config: duplicate [[wasm_consumer]] slug {:?} — each slug must be unique \
             (bootstrap dedup)",
            c.slug,
        );
    }

    let mut result = Vec::with_capacity(raw_consumers.len());
    for consumer in raw_consumers {
        let slug = &consumer.slug;

        // --- Grant resolution ---

        // 1. Panic on duplicate grant entries; collect into BTreeSet.
        let mut grants: BTreeSet<ComponentGrant> = BTreeSet::new();
        for grant in &consumer.grants {
            assert!(
                grants.insert(*grant),
                "[[wasm_consumer]] {slug:?}: duplicate grant {:?} in grants list",
                grant,
            );
            // The placement-legality table, at the top-level end — the twin of
            // the surface's own check. Without it a word no backend host
            // implements reaches the linker mapping, which can only panic about
            // its own mechanics.
            if let Some(why) = grant.illegal_on(ComponentHost::TopLevel) {
                panic!(
                    "[[wasm_consumer]] {slug:?}: is granted {:?}, but {why}; remove it from the \
                     grants list",
                    grant.word(),
                );
            }
        }

        // 3. [wasm_consumer.config] table present but Config not granted → dead config.
        //    Run before resolve_component_config so grant error takes precedence.
        if consumer.config.is_some() && !grants.contains(&ComponentGrant::Config) {
            panic!(
                "[[wasm_consumer]] {slug:?}: [wasm_consumer.config] table is present but \
                 \"config\" is not in grants — the component cannot read its config; \
                 add \"config\" to grants or remove the config table",
            );
        }

        // 4a. store_path present but Store not granted.
        if consumer.store_path.is_some() && !grants.contains(&ComponentGrant::Store) {
            panic!(
                "[[wasm_consumer]] {slug:?}: store_path is set but \"store\" is not in grants — \
                 the component cannot access the store; add \"store\" to grants or remove store_path",
            );
        }
        // 4b. store_size_limit set but Store not granted.
        if consumer.store_size_limit.is_some() && !grants.contains(&ComponentGrant::Store) {
            panic!(
                "[[wasm_consumer]] {slug:?}: store_size_limit is set but \"store\" is not in grants — \
                 remove store_size_limit or add \"store\" to grants",
            );
        }
        // 4c. Store granted but store_path absent.
        if grants.contains(&ComponentGrant::Store) && consumer.store_path.is_none() {
            panic!(
                "[[wasm_consumer]] {slug:?}: \"store\" is in grants but store_path is not set — \
                 the store grant requires a store_path",
            );
        }

        // Validate and resolve store_path (only when Store is granted).
        let store_path: Option<std::path::PathBuf> = if let Some(ref raw_path) = consumer.store_path
        {
            let store_parent = raw_path
                .parent()
                .unwrap_or_else(|| std::path::Path::new("."));
            assert!(
                store_parent.exists(),
                "[[wasm_consumer]] {slug:?}: store_path {:?} — parent directory does not exist",
                raw_path,
            );
            let absolute = std::path::absolute(raw_path).unwrap_or_else(|e| {
                panic!(
                    "[[wasm_consumer]] {slug:?}: failed to resolve store_path {:?}: {e}",
                    raw_path,
                )
            });
            Some(absolute)
        } else {
            None
        };

        // Resolve store size limit (always compute max_page_count from the effective
        // limit; unused when store_path is None but kept non-optional on the resolved type).
        let effective_limit = consumer
            .store_size_limit
            .as_deref()
            .unwrap_or(global_store_size_limit);
        let size_field = format!("[[wasm_consumer]] {slug:?} store_size_limit");
        let max_page_count = byte_size_to_max_page_count(effective_limit, &size_field);

        // Resolve activation pacing. Both
        // knobs optional; absent ⇒ hardcoded defaults (no `[wasm]` global — the
        // per-consumer knob is the whole surface). Both must be ≥ 1
        // when present; a zero is rejected here — naming the slug — rather than
        // deferred to `TokenBucket::new`'s zero-interval panic. Fail-fast on bad
        // host-authored config per BETTER DEAD THAN WRONG.
        if let Some(burst) = consumer.activation_burst {
            assert!(
                burst >= 1,
                "[[wasm_consumer]] {slug:?}: activation_burst must be >= 1 (got {burst})",
            );
        }
        if let Some(ms) = consumer.activation_min_period_ms {
            assert!(
                ms >= 1,
                "[[wasm_consumer]] {slug:?}: activation_min_period_ms must be >= 1 (got {ms})",
            );
        }
        let activation_pacing = ActivationPacing {
            burst: consumer
                .activation_burst
                .unwrap_or(DEFAULT_ACTIVATION_BURST),
            min_period: consumer
                .activation_min_period_ms
                .map(Duration::from_millis)
                .unwrap_or(DEFAULT_ACTIVATION_MIN_PERIOD),
        };

        // Collect all port names for uniqueness check (inputs + outputs).
        let mut seen_port_names: HashSet<String> = HashSet::new();
        let mut validate_port_name = |port: &str, context: &str| {
            assert!(
                !port.is_empty(),
                "[[wasm_consumer]] {slug:?}: {context} port name must be non-empty",
            );
            assert!(
                port.chars().all(is_unreserved_char),
                "[[wasm_consumer]] {slug:?}: {context} port name {:?} must consist of \
                 RFC 3986 unreserved characters only (A-Za-z0-9._~-)",
                port,
            );
            assert!(
                port != brenn_tool_registry::bus_wiring::TOOL_RESULT_INPUT_PORT,
                "[[wasm_consumer]] {slug:?}: {context} port name {:?} is reserved for the \
                 async tool-result inbox; a consumer holding an async tool grant has this \
                 port folded in automatically, so an operator-declared port of the same name \
                 would collide",
                port,
            );
            assert!(
                seen_port_names.insert(port.to_string()),
                "[[wasm_consumer]] {slug:?}: duplicate port name {:?} (port names must be \
                 unique across inputs and outputs)",
                port,
            );
        };

        // Both halves of each io_port are channel-less and take the one address
        // the lowering pass assigned — the two directions cannot be wired apart.
        let (io_subscriptions, io_outputs) = super::auto::wasm_io_bindings(&consumer.io_ports);

        // Validate: outputs-without-inputs is dead config. An io_port carries an
        // input half, so a consumer whose only subscription is one still activates.
        assert!(
            !(consumer.subscriptions.is_empty() && io_subscriptions.is_empty())
                || consumer.outputs.is_empty(),
            "[[wasm_consumer]] {slug:?}: has output port(s) but no subscriptions — \
             a consumer with no inputs never activates; its outputs are dead config",
        );

        // Resolve input ports.
        let mut inputs = Vec::with_capacity(consumer.subscriptions.len() + io_subscriptions.len());
        let mut seen_addresses: HashSet<String> = HashSet::new();
        // Ring delivery has no runtime ACL gate — ephemeral_subscribe /
        // local_subscribe coverage must be asserted at boot (below) or the input
        // is silently dead.
        let mut ephemeral_inputs: Vec<String> = Vec::new();
        let mut local_inputs: Vec<String> = Vec::new();

        for (sub, kind) in consumer
            .subscriptions
            .iter()
            .map(|sub| (sub, "subscription"))
            .chain(io_subscriptions.iter().map(|sub| (sub, "io_port")))
        {
            validate_port_name(&sub.port, kind);

            let channel = super::bound_channel(
                &format!("[[wasm_consumer]] {slug:?}"),
                &format!("{kind} port {:?}", sub.port),
                sub.channel.as_deref(),
                auto_wiring.wasm_channel(slug, &sub.port),
            );
            let entry = directory.resolve(&channel).unwrap_or_else(|| {
                panic!(
                    "[[wasm_consumer]] {slug:?}: subscription.channel {channel:?} is not a known \
                     channel address (not a [[channel]] or [[webhook_endpoint]] declaration, \
                     nor an mqtt:<client>:<topic> address derived from a [[wasm_consumer]] or \
                     [[app.mqtt_subscription]] subscription)",
                )
            });

            // Non-durable inputs are ring-backed and carry no runtime delivery
            // ACL gate; their authorization is asserted at boot (below), keyed by
            // scheme.
            use brenn_lib::messaging::ChannelScheme;
            let in_scheme = ChannelScheme::of(&entry.address).unwrap_or_else(|| {
                panic!(
                    "[[wasm_consumer]] {slug:?}: {kind} channel {:?} carries no recognized \
                     scheme prefix",
                    entry.address,
                )
            });
            assert!(
                bindable_schemes(
                    EntityKind::Component(ComponentHost::TopLevel),
                    Plane::Subscribe
                )
                .contains(&in_scheme),
                "[[wasm_consumer]] {slug:?}: {kind} channel {:?} names a scheme a consumer \
                 does not subscribe through — a push target is an egress address a policy \
                 names, never one a port reads from",
                entry.address,
            );
            match in_scheme {
                ChannelScheme::Ephemeral => {
                    if let Some((_, bare)) = ChannelScheme::split(&entry.address) {
                        ephemeral_inputs.push(bare.to_string());
                    }
                }
                ChannelScheme::Local => {
                    if let Some((_, bare)) = ChannelScheme::split(&entry.address) {
                        local_inputs.push(bare.to_string());
                    }
                }
                _ => {}
            }
            assert!(
                seen_addresses.insert(entry.address.clone()),
                "[[wasm_consumer]] {slug:?}: duplicate subscription for channel {:?}",
                entry.address,
            );

            // The whole ladder: sub → the channel's own rung.
            let ch = &entry.resolved_channel;
            let push_depth = sub.push_depth.unwrap_or(ch.push_depth);
            let retain_depth = sub.retain_depth.unwrap_or(ch.retain_depth);
            if sub.noise.is_some() && push_depth == Depth::Bounded(0) {
                panic!(
                    "[[wasm_consumer]] {slug:?}: {kind} on channel {:?} has noise configured \
                     but push_depth = 0 (pull-only) — no push-overflow events are possible; \
                     remove the noise setting or set push_depth > 0",
                    entry.address,
                );
            }
            // Dead-port validation: push_depth=0 and retain_depth=0 — can never
            // trigger and never contributes context; dead config, fail-fast.
            if push_depth == Depth::Bounded(0) && retain_depth == Depth::Bounded(0) {
                panic!(
                    "[[wasm_consumer]] {slug:?}: {kind} on channel {:?} has \
                     push_depth = 0 AND retain_depth = 0 — this port can never trigger \
                     and never carries context (dead config); \
                     set push_depth > 0 to make it triggering, or retain_depth > 0 to make \
                     it a sampled/context-only port",
                    entry.address,
                );
            }
            let noise = sub.noise.unwrap_or(ch.noise);

            // `fatal` is the surface-only kill rung; the backend overflow path
            // has no kill wire, so any subscription resolving to `fatal` is
            // rejected at boot. The app/mqtt/webhook path rejects `fatal`
            // separately; the two sites must stay in step.
            if noise == NoiseLevel::Fatal {
                panic!(
                    "[[wasm_consumer]] {slug:?}: {kind} on channel {:?} resolves to \
                     noise = fatal, but fatal is surface-only (the backend overflow path has \
                     no kill) — set a backend-valid noise level (silent/metered/alarm)",
                    entry.address,
                );
            }

            // wake_min is meaningless on a WASM subscription: a parked WASM consumer
            // is cheap to wake, so it is always delivered eagerly (its registration
            // is `Eager`) and `wake_min` never gates its delivery. Setting it is a
            // config error at any push_depth — the honest way to say "don't push to
            // me" is push_depth = 0 (pull-only).
            if sub.wake_min.is_some() {
                panic!(
                    "[[wasm_consumer]] {slug:?}: {kind} on channel {:?} sets wake_min, \
                     but WASM consumers are always delivered eagerly — wake_min does not apply. \
                     Remove the wake_min setting; use push_depth = 0 for a pull-only subscription.",
                    entry.address,
                );
            }
            let wake_min = ch.wake_min;

            // amplification: same pattern as noise/wake_min — explicit on pull-only
            // is an error. A pull-only input produces no new envelopes, so its
            // amplification can never grant a publish token; an explicit setting is
            // meaningless. (An inherited/default amplification on a pull-only input is
            // fine — inert, like inherited noise.)
            if sub.amplification.is_some() && push_depth == Depth::Bounded(0) {
                panic!(
                    "[[wasm_consumer]] {slug:?}: {kind} on channel {:?} has \
                     amplification configured but push_depth = 0 (pull-only) — a pull-only \
                     input produces no new envelopes so amplification can never grant a \
                     publish token; remove the amplification setting or set push_depth > 0",
                    entry.address,
                );
            }

            let amplification_mt = resolve_publish_millitokens(
                sub.amplification,
                DEFAULT_WASM_INPUT_AMPLIFICATION,
                &format!(
                    "[[wasm_consumer]] {slug:?} {kind} port {:?} amplification",
                    sub.port
                ),
            );

            inputs.push(WasmInputPort {
                port: sub.port.clone(),
                sub: ResolvedSubscription {
                    channel_uuid: entry.uuid,
                    channel_address: entry.address.clone(),
                    push_depth,
                    retain_depth,
                    noise,
                    wake_min,
                },
                amplification_mt,
            });
        }

        // Dead-consumer validation: all inputs are sampled-only (push_depth=0),
        // so the consumer can never activate. Fail-fast.
        if !inputs.is_empty()
            && inputs
                .iter()
                .all(|inp| inp.sub.push_depth == Depth::Bounded(0))
        {
            panic!(
                "[[wasm_consumer]] {slug:?}: all {} input subscription(s) have push_depth = 0 \
                 (sampled/context-only) — this consumer can never activate; \
                 at least one subscription must have push_depth > 0 to trigger activations",
                inputs.len(),
            );
        }

        // Resolve output ports. The addresses of the *address-bound* ones are
        // collected separately: the per-scheme empty-publish-ACL checks below
        // speak only about them, because an auto-bound output's coverage comes
        // from an injected matcher rather than an authored ACL.
        let mut outputs = Vec::with_capacity(consumer.outputs.len() + io_outputs.len());
        let mut address_bound_outputs: Vec<String> = Vec::new();
        // An io_port's name was already registered by its input half, so
        // `is_io_port` skips the duplicate-name check that would reject the
        // second direction. `kind` is panic-message text only.
        for (out, kind, is_io_port) in consumer
            .outputs
            .iter()
            .map(|out| (out, "output", false))
            .chain(io_outputs.iter().map(|out| (out, "io_port", true)))
        {
            if !is_io_port {
                validate_port_name(&out.port, kind);
            }

            let channel = super::bound_channel(
                &format!("[[wasm_consumer]] {slug:?}"),
                &format!("{kind} port {:?}", out.port),
                out.channel.as_deref(),
                auto_wiring.wasm_channel(slug, &out.port),
            );
            let entry = directory.resolve(&channel).unwrap_or_else(|| {
                panic!(
                    "[[wasm_consumer]] {slug:?}: output.channel {channel:?} is not a known \
                     channel address",
                )
            });
            // The buffered `ports.publish` path serves pub/sub schemes only
            // (brenn:/ephemeral:/local:). Do NOT let mqtt:/webhook:/pwa_push:
            // ride this path — MQTT egress uses the separate synchronous
            // `mqtt-publish` host fn, which can surface immediate broker errors;
            // the buffered path cannot.
            let out_scheme = brenn_lib::messaging::ChannelScheme::of(&entry.address)
                .unwrap_or_else(|| {
                    panic!(
                        "[[wasm_consumer]] {slug:?}: output.channel {:?} carries no recognized \
                         scheme prefix",
                        entry.address,
                    )
                });
            assert!(
                bindable_schemes(
                    EntityKind::Component(ComponentHost::TopLevel),
                    Plane::Publish
                )
                .contains(&out_scheme),
                "[[wasm_consumer]] {slug:?}: output.channel {:?} must be a pub/sub address \
                 (brenn:/ephemeral:/local:); the buffered ports.publish path never carries \
                 mqtt:/webhook:/pwa_push: egress — MQTT egress uses the separate mqtt-publish \
                 host fn, not ports.publish",
                entry.address,
            );

            let fill_mt = resolve_publish_millitokens(
                out.publish_per_activation,
                DEFAULT_WASM_PUBLISH_PER_ACTIVATION,
                &format!(
                    "[[wasm_consumer]] {slug:?} {kind} port {:?} publish_per_activation",
                    out.port
                ),
            );
            let capacity_mt = resolve_publish_millitokens(
                out.publish_capacity,
                DEFAULT_WASM_PUBLISH_CAPACITY,
                &format!(
                    "[[wasm_consumer]] {slug:?} {kind} port {:?} publish_capacity",
                    out.port
                ),
            );

            if out.channel.is_some() {
                address_bound_outputs.push(entry.address.clone());
            }
            outputs.push(WasmOutputPort {
                port: out.port.clone(),
                channel_uuid: entry.uuid,
                channel_address: entry.address.clone(),
                default_urgency: out.urgency.unwrap_or(brenn_lib::messaging::Urgency::Normal),
                budget: WasmSinkBudget {
                    fill_mt,
                    capacity_mt,
                },
            });
        }

        // 2. outputs non-empty but Ports not granted → dead config.
        if !outputs.is_empty() && !grants.contains(&ComponentGrant::Ports) {
            panic!(
                "[[wasm_consumer]] {slug:?}: has {} output port(s) but \"ports\" is not in grants \
                 — the component cannot publish; add \"ports\" to grants or remove the output bindings",
                outputs.len(),
            );
        }

        // 2b. Address-bound output + empty publish ACL for that scheme ⇒ every
        //      publish would deny at runtime. Panic now so the operator authors an
        //      explicit ACL. One check per pub/sub scheme (brenn:/ephemeral:/local:).
        //      An *auto-bound* output (bound by a link rather than by an address
        //      on the binding) legitimately leaves the list empty: its coverage is
        //      the matcher injected from the link, and the downstream coverage
        //      asserts still verify it.
        use brenn_lib::messaging::ChannelScheme;
        let has_brenn_output = address_bound_outputs
            .iter()
            .any(|a| ChannelScheme::of(a) == Some(ChannelScheme::Brenn));
        let has_ephemeral_output = address_bound_outputs
            .iter()
            .any(|a| ChannelScheme::of(a) == Some(ChannelScheme::Ephemeral));
        if has_brenn_output && consumer.publish_acl.is_empty() {
            panic!(
                "[[wasm_consumer]] {slug:?}: has a bound brenn: output port but publish_acl is \
                 empty — under deny-by-default the component could never publish to its own bound \
                 channels (every publish would return not-permitted at runtime); add a \
                 publish_acl matcher covering each bound channel (e.g. {{ exact = \"<name>\" }}) \
                 or remove the output bindings",
            );
        }
        if has_ephemeral_output && consumer.ephemeral_publish_acl.is_empty() {
            panic!(
                "[[wasm_consumer]] {slug:?}: has a bound ephemeral: output port but \
                 ephemeral_publish_acl is empty — under deny-by-default the component could never \
                 publish to its own bound channels; add an ephemeral_publish_acl matcher covering \
                 each bound channel (e.g. {{ exact = \"<name>\" }}) or remove the output bindings",
            );
        }
        let has_local_output = address_bound_outputs
            .iter()
            .any(|a| ChannelScheme::of(a) == Some(ChannelScheme::Local));
        if has_local_output && consumer.local_publish_acl.is_empty() {
            panic!(
                "[[wasm_consumer]] {slug:?}: has a bound local: output port but \
                 local_publish_acl is empty — under deny-by-default the component could never \
                 publish to its own bound channels; add a local_publish_acl matcher covering \
                 each bound channel (e.g. {{ exact = \"<name>\" }}) or remove the output bindings",
            );
        }

        // 2c. Non-empty publish ACL but Ports not granted → dead matchers.
        //      Without the `ports` interface the ACL can never authorize anything.
        if (!consumer.publish_acl.is_empty()
            || !consumer.ephemeral_publish_acl.is_empty()
            || !consumer.local_publish_acl.is_empty())
            && !grants.contains(&ComponentGrant::Ports)
        {
            panic!(
                "[[wasm_consumer]] {slug:?}: a publish ACL has matcher(s) but \"ports\" is not in \
                 grants — without the ports grant the matchers can never authorize any publish \
                 (the ports interface is unlinked); add \"ports\" to grants or remove the ACL",
            );
        }

        // 2d. Every `mqtt_publish` ACL matcher's `client` must name a declared
        //      `[[mqtt_client]]`. The client slug in the guest's `mqtt:` address
        //      selects the session; a matcher naming an undeclared client would
        //      authorize a publish that has no session to reach — a boot-time config
        //      error, fail-fast (parallel to the LLM-side `validate_mqtt_client`).
        for matcher in &consumer.mqtt_publish_acl {
            assert!(
                resolved_clients.contains_key(matcher.client.as_str()),
                "[[wasm_consumer]] {slug:?}: mqtt_publish ACL matcher names mqtt client {:?}, \
                 but no [[mqtt_client]] with that slug is declared; declare the client or remove \
                 the matcher",
                matcher.client,
            );
        }

        // 2e. Every `mqtt_subscribe` ACL matcher's `client` must name a declared
        //      `[[mqtt_client]]`. The client slug in the subscribed `mqtt:` address
        //      selects the session; a matcher naming an undeclared client would
        //      authorize delivery from a session that has no broker connection to
        //      arrive on — a boot-time config error, fail-fast (parallel to check 2d
        //      for `mqtt_publish` and the LLM-side `validate_mqtt_client`).
        for matcher in &consumer.mqtt_subscribe_acl {
            assert!(
                resolved_clients.contains_key(matcher.client.as_str()),
                "[[wasm_consumer]] {slug:?}: mqtt_subscribe ACL matcher names mqtt client {:?}, \
                 but no [[mqtt_client]] with that slug is declared; declare the client or remove \
                 the matcher",
                matcher.client,
            );
        }

        // 2f. Non-empty `mqtt_publish` ACL but `mqtt` not granted → dead matchers
        //      (same shape as the brenn `publish_acl` + `Ports`-grant check 2c). The
        //      `build_wasm_policy` mapping derives `MqttPublish` only from the `Mqtt`
        //      grant; without it `allows_mqtt_publish` is unconditionally false, so
        //      the authored matchers can never authorize any MQTT publish. The
        //      operator wrote an `mqtt_publish` ACL expecting it to grant egress
        //      access; silently dropping it is the same runtime-only landmine
        //      fail-fast rejects. Panic now so the misconfiguration is fixed at boot,
        //      not discovered as an unexplained not-permitted after the grant is
        //      added.
        if !consumer.mqtt_publish_acl.is_empty() && !grants.contains(&ComponentGrant::Mqtt) {
            panic!(
                "[[wasm_consumer]] {slug:?}: mqtt_publish ACL has {} matcher(s) but \"mqtt\" is not \
                 in grants — without the mqtt grant the matchers can never authorize any MQTT \
                 publish (MqttPublish capability absent); add \"mqtt\" to grants or remove the \
                 mqtt_publish ACL",
                consumer.mqtt_publish_acl.len(),
            );
        }

        // Sink budgets. A sink can never emit a token if its per-activation fill is
        // 0 and no input can grant amplification tokens — dead config, fail-fast. An
        // input can grant only if it has non-zero amplification AND can produce new
        // envelopes; a pull-only input (push_depth = 0) never produces new envelopes,
        // so its amplification is inert regardless of value and must not be counted as
        // keeping the sink alive.
        let no_input_can_grant = inputs
            .iter()
            .all(|inp| inp.amplification_mt == 0 || inp.sub.push_depth == Depth::Bounded(0));

        for out in &outputs {
            assert!(
                !(out.budget.fill_mt == 0 && no_input_can_grant),
                "[[wasm_consumer]] {slug:?}: output port {:?} has publish_per_activation = 0 \
                 and every input amplification is 0 (or there are no inputs) — this sink can \
                 never publish; remove the output binding or raise publish_per_activation / an \
                 input amplification",
                out.port,
            );
        }

        // Build the MQTT egress sink budget map: one sink per distinct
        // `mqtt_publish_acl` client (default budget), overridden by
        // `[[wasm_consumer.mqtt_output]]` blocks. Per-client, not per-topic (topics
        // are guest-controlled unbounded strings; client slugs are a small
        // boot-validated operator set).
        let default_fill_mt = resolve_publish_millitokens(
            None,
            DEFAULT_WASM_PUBLISH_PER_ACTIVATION,
            "mqtt sink default publish_per_activation",
        );
        let default_capacity_mt = resolve_publish_millitokens(
            None,
            DEFAULT_WASM_PUBLISH_CAPACITY,
            "mqtt sink default publish_capacity",
        );
        let mut mqtt_sinks: std::collections::HashMap<String, WasmSinkBudget> =
            std::collections::HashMap::new();
        for matcher in &consumer.mqtt_publish_acl {
            mqtt_sinks
                .entry(matcher.client.clone())
                .or_insert(WasmSinkBudget {
                    fill_mt: default_fill_mt,
                    capacity_mt: default_capacity_mt,
                });
        }

        // Apply per-client overrides. Each must name an ACL-covered client;
        // duplicate blocks for one client are dead config.
        let mut seen_mqtt_output: HashSet<&str> = HashSet::new();
        for mo in &consumer.mqtt_outputs {
            assert!(
                mqtt_sinks.contains_key(&mo.client),
                "[[wasm_consumer]] {slug:?}: [[mqtt_output]] names client {:?} which is not \
                 covered by mqtt_publish_acl — add an mqtt_publish ACL matcher for it or remove \
                 the block",
                mo.client,
            );
            assert!(
                seen_mqtt_output.insert(mo.client.as_str()),
                "[[wasm_consumer]] {slug:?}: duplicate [[mqtt_output]] block for client {:?}",
                mo.client,
            );
            let fill_mt = resolve_publish_millitokens(
                mo.publish_per_activation,
                DEFAULT_WASM_PUBLISH_PER_ACTIVATION,
                &format!(
                    "[[wasm_consumer]] {slug:?} mqtt_output client {:?} publish_per_activation",
                    mo.client
                ),
            );
            let capacity_mt = resolve_publish_millitokens(
                mo.publish_capacity,
                DEFAULT_WASM_PUBLISH_CAPACITY,
                &format!(
                    "[[wasm_consumer]] {slug:?} mqtt_output client {:?} publish_capacity",
                    mo.client
                ),
            );
            mqtt_sinks.insert(
                mo.client.clone(),
                WasmSinkBudget {
                    fill_mt,
                    capacity_mt,
                },
            );
        }

        for (client, budget) in &mqtt_sinks {
            assert!(
                !(budget.fill_mt == 0 && no_input_can_grant),
                "[[wasm_consumer]] {slug:?}: mqtt sink for client {:?} has \
                 publish_per_activation = 0 and every input amplification is 0 (or there are no \
                 inputs) — this sink can never publish; remove the mqtt_output override or raise \
                 publish_per_activation / an input amplification",
                client,
            );
        }

        // TODO(wasm-dead-subscribe-acl-check): no check rejects a non-empty
        // subscribe/mqtt_subscribe/webhook ACL whose matchers cover none of this
        // consumer's static subscriptions. For a WASM consumer such matchers are
        // provably dead (no ComponentGrant maps to DynamicSubscribe, so nothing can ever
        // exercise them), unlike the LLM side where an ACL without a static sub
        // legitimately pre-authorizes future dynamic subs. Adding a 2g check here
        // diverges WASM from the shared subscribe_acl convention, so it wants a
        // design decision before landing.

        let config_field_name = format!("[[wasm_consumer]] {slug:?} config");
        let config = resolve_component_config(consumer.config.as_ref(), &config_field_name);

        // Build the unified AppPolicy from the resolved grants + authored
        // subscribe_acl/publish_acl channel matchers. This maps each
        // ComponentGrant onto its unified AppCapability and validates+converts the
        // channel matchers (fail-fast on a malformed matcher). It is a *separate*
        // mapping from the WIT interface each grant links at the linker. The
        // `subscribe_acl` matchers (plus the derived `MessagingSubscribe` grant)
        // ARE enforced at delivery time over `Wasm` subscribers; the broader WASM
        // enforcement surface (linker-seam capabilities, publish_acl) is not yet
        // wired here.
        let mut policy = brenn_lib::access::resolve::build_wasm_policy(
            slug,
            grants.iter().copied(),
            brenn_lib::access::raw::WasmAclsRaw {
                subscribe: &consumer.subscribe_acl,
                ephemeral_subscribe: &consumer.ephemeral_subscribe_acl,
                local_subscribe: &consumer.local_subscribe_acl,
                publish: &consumer.publish_acl,
                ephemeral_publish: &consumer.ephemeral_publish_acl,
                local_publish: &consumer.local_publish_acl,
                mqtt_publish: &consumer.mqtt_publish_acl,
                mqtt_subscribe: &consumer.mqtt_subscribe_acl,
                webhook: &consumer.webhook_acl,
            },
        );
        // Resolve the consumer's `[[wasm_consumer.tool_grant]]` tables into the
        // same `tool_grants` map an LLM app carries (one grant vocabulary, both
        // participant kinds). The registry validates these against its descriptors
        // at the component-load site, and a non-empty map derives the `Tools`
        // capability + the real tool host there.
        policy.tool_grants = brenn_lib::tools::config::resolve_tool_grants(
            &format!("wasm consumer {slug:?}"),
            &consumer.tool_grants,
        );
        // Auto-channel grants, injected before every coverage assert below so all
        // of them hold for a consumer whose links are its only ACL: the link
        // declaration is the authorization signal, the way a tool grant is for the
        // async-tool substrate.
        auto_wiring.inject_wasm_grants(slug, &mut policy);

        // Ring delivery has no runtime ACL gate, so an uncovered ephemeral:
        // subscription would be silently dead. Fail-fast.
        for bare in &ephemeral_inputs {
            assert!(
                policy.allows_ephemeral_delivery(bare),
                "[[wasm_consumer]] {slug:?}: subscription on ephemeral:{bare} is not covered by \
                 ephemeral_subscribe_acl — under deny-by-default the component could never receive \
                 from it (ring delivery is authorized only at boot); add an ephemeral_subscribe_acl \
                 matcher covering it (e.g. {{ exact = {bare:?} }}) or remove the subscription",
            );
        }
        // Same for confined local: inputs — ring delivery has no runtime gate, so
        // an uncovered subscription would be silently dead.
        for bare in &local_inputs {
            assert!(
                policy.allows_local_delivery(bare),
                "[[wasm_consumer]] {slug:?}: subscription on local:{bare} is not covered by \
                 local_subscribe_acl — under deny-by-default the component could never receive \
                 from it (ring delivery is authorized only at boot); add a local_subscribe_acl \
                 matcher covering it (e.g. {{ exact = {bare:?} }}) or remove the subscription",
            );
        }

        result.push(ResolvedWasmConsumer {
            slug: slug.clone(),
            package: consumer.package.clone(),
            spec_sha256: consumer.spec_sha256.clone(),
            grants,
            store_path,
            max_page_count,
            inputs,
            outputs,
            config,
            policy,
            activation_pacing,
            mqtt_sinks,
        });
    }
    result
}
