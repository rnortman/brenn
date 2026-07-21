use std::time::Duration;

use brenn_lib::messaging::MessagingDirectory;
use brenn_lib::messaging::config::{
    ActivationPacing, DEFAULT_WASM_INPUT_AMPLIFICATION, DEFAULT_WASM_PUBLISH_CAPACITY,
    DEFAULT_WASM_PUBLISH_PER_ACTIVATION, Depth, ResolvedSubscription, ResolvedWasmConsumer,
    WasmConsumerConfigRaw, WasmGrant, WasmInputPort, WasmOutputPort, WasmSinkBudget,
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

use super::resolve_publish_millitokens;

/// Resolve `[[wasm_consumer]]` blocks against the channel directory,
/// applying the three-level depth/noise inheritance (sub → channel → global) and
/// validating output port bindings.
///
/// Returns `Vec<ResolvedWasmConsumer>` in declaration order.
///
/// Panics on:
/// - unknown channel address in any subscription or output block
/// - duplicate subscription for the same channel within one consumer
/// - duplicate slug across two `[[wasm_consumer]]` blocks
/// - slug containing `:` or `@` (rejected by `ParticipantId::for_wasm` constructor)
/// - duplicate port name within a consumer (across inputs and outputs)
/// - empty port name or port name containing non-unreserved chars
/// - output channel is not a `brenn:` address (this-slice restriction)
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
        let mut grants: BTreeSet<WasmGrant> = BTreeSet::new();
        for grant in &consumer.grants {
            assert!(
                grants.insert(*grant),
                "[[wasm_consumer]] {slug:?}: duplicate grant {:?} in grants list",
                grant,
            );
        }

        // 3. [wasm_consumer.config] table present but Config not granted → dead config.
        //    Run before resolve_component_config so grant error takes precedence.
        if consumer.config.is_some() && !grants.contains(&WasmGrant::Config) {
            panic!(
                "[[wasm_consumer]] {slug:?}: [wasm_consumer.config] table is present but \
                 \"config\" is not in grants — the component cannot read its config; \
                 add \"config\" to grants or remove the config table",
            );
        }

        // 4a. store_path present but Store not granted.
        if consumer.store_path.is_some() && !grants.contains(&WasmGrant::Store) {
            panic!(
                "[[wasm_consumer]] {slug:?}: store_path is set but \"store\" is not in grants — \
                 the component cannot access the store; add \"store\" to grants or remove store_path",
            );
        }
        // 4b. store_size_limit set but Store not granted.
        if consumer.store_size_limit.is_some() && !grants.contains(&WasmGrant::Store) {
            panic!(
                "[[wasm_consumer]] {slug:?}: store_size_limit is set but \"store\" is not in grants — \
                 remove store_size_limit or add \"store\" to grants",
            );
        }
        // 4c. Store granted but store_path absent.
        if grants.contains(&WasmGrant::Store) && consumer.store_path.is_none() {
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
                port != crate::tool_registry::bus_wiring::TOOL_RESULT_INPUT_PORT,
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

        // Validate: outputs-without-inputs is dead config.
        assert!(
            !consumer.subscriptions.is_empty() || consumer.outputs.is_empty(),
            "[[wasm_consumer]] {slug:?}: has output port(s) but no subscriptions — \
             a consumer with no inputs never activates; its outputs are dead config",
        );

        // Resolve input ports.
        let mut inputs = Vec::with_capacity(consumer.subscriptions.len());
        let mut seen_addresses: HashSet<String> = HashSet::new();

        for sub in &consumer.subscriptions {
            validate_port_name(&sub.port, "subscription");

            let entry = directory.resolve(&sub.channel).unwrap_or_else(|| {
                panic!(
                    "[[wasm_consumer]] {slug:?}: subscription.channel {:?} is not a known \
                     channel address (not a [[channel]] or [[webhook_endpoint]] declaration, \
                     nor an mqtt:<client>:<topic> address derived from a [[wasm_consumer]] or \
                     [[app.mqtt_subscription]] subscription)",
                    sub.channel,
                )
            });
            assert!(
                seen_addresses.insert(entry.address.clone()),
                "[[wasm_consumer]] {slug:?}: duplicate subscription for channel {:?}",
                entry.address,
            );

            // Three-level inheritance: sub → channel → global.
            let ch = &entry.resolved_channel;
            let push_depth = sub.push_depth.unwrap_or(ch.push_depth);
            let retain_depth = sub.retain_depth.unwrap_or(ch.retain_depth);
            if sub.noise.is_some() && push_depth == Depth::Bounded(0) {
                panic!(
                    "[[wasm_consumer]] {slug:?}: subscription on channel {:?} has noise configured \
                     but push_depth = 0 (pull-only) — no push-overflow events are possible; \
                     remove the noise setting or set push_depth > 0",
                    entry.address,
                );
            }
            // Dead-port validation: push_depth=0 and retain_depth=0 — can never
            // trigger and never contributes context; dead config, fail-fast.
            if push_depth == Depth::Bounded(0) && retain_depth == Depth::Bounded(0) {
                panic!(
                    "[[wasm_consumer]] {slug:?}: subscription on channel {:?} has \
                     push_depth = 0 AND retain_depth = 0 — this port can never trigger \
                     and never carries context (dead config); \
                     set push_depth > 0 to make it triggering, or retain_depth > 0 to make \
                     it a sampled/context-only port",
                    entry.address,
                );
            }
            let noise = sub.noise.unwrap_or(ch.noise);

            // wake_min is meaningless on a WASM subscription: a parked WASM consumer
            // is cheap to wake, so it is always delivered eagerly (its registration
            // is `Eager`) and `wake_min` never gates its delivery. Setting it is a
            // config error at any push_depth — the honest way to say "don't push to
            // me" is push_depth = 0 (pull-only).
            if sub.wake_min.is_some() {
                panic!(
                    "[[wasm_consumer]] {slug:?}: subscription on channel {:?} sets wake_min, \
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
                    "[[wasm_consumer]] {slug:?}: subscription on channel {:?} has \
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
                    "[[wasm_consumer]] {slug:?} subscription port {:?} amplification",
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

        // Resolve output ports.
        let mut outputs = Vec::with_capacity(consumer.outputs.len());
        for out in &consumer.outputs {
            validate_port_name(&out.port, "output");

            let entry = directory.resolve(&out.channel).unwrap_or_else(|| {
                panic!(
                    "[[wasm_consumer]] {slug:?}: output.channel {:?} is not a known \
                     channel address",
                    out.channel,
                )
            });
            // The buffered `ports.publish` output path is brenn:-only — and this is
            // PERMANENT, not a temporary restriction. MQTT egress is
            // supported, but through the SEPARATE synchronous `mqtt-publish` host fn
            // (gated by the `mqtt_publish` ACL matcher), never through
            // `ports.publish` (which buffers + flushes atomically and cannot carry
            // the immediate broker error MQTT egress requires). Do NOT relax this
            // assertion to let
            // mqtt:/webhook: addresses ride the buffered path — that would route MQTT
            // traffic around the egress enforcement pipeline. webhook: outputs are
            // simply not supported at all yet.
            assert!(
                entry
                    .address
                    .starts_with(brenn_lib::messaging::BRENN_ADDRESS_PREFIX),
                "[[wasm_consumer]] {slug:?}: output.channel {:?} must be a brenn: address \
                 (the buffered ports.publish path is permanently brenn:-only; MQTT egress \
                 uses the separate mqtt-publish host fn, not ports.publish)",
                entry.address,
            );

            let fill_mt = resolve_publish_millitokens(
                out.publish_per_activation,
                DEFAULT_WASM_PUBLISH_PER_ACTIVATION,
                &format!(
                    "[[wasm_consumer]] {slug:?} output port {:?} publish_per_activation",
                    out.port
                ),
            );
            let capacity_mt = resolve_publish_millitokens(
                out.publish_capacity,
                DEFAULT_WASM_PUBLISH_CAPACITY,
                &format!(
                    "[[wasm_consumer]] {slug:?} output port {:?} publish_capacity",
                    out.port
                ),
            );

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
        if !outputs.is_empty() && !grants.contains(&WasmGrant::Ports) {
            panic!(
                "[[wasm_consumer]] {slug:?}: has {} output port(s) but \"ports\" is not in grants \
                 — the component cannot publish; add \"ports\" to grants or remove the output bindings",
                outputs.len(),
            );
        }

        // 2b. Bound output ports + empty publish_acl ⇒ refuse at resolution. The
        //      publish gate (`do_publish`) consults `allows_brenn_publish`, which is
        //      deny-all over an empty `brenn_publish` matcher list, so every guest
        //      publish to an operator-authored bound port would return NotPermitted at
        //      runtime. Panic now and force the operator to author an explicit
        //      publish_acl rather than ship a config that silently denies every publish.
        //      Narrow scope: fires ONLY with bound output ports — a Ports-granted
        //      consumer with NO bound outputs + empty publish_acl is a legitimate
        //      intermediate state and does not panic here.
        if !outputs.is_empty() && consumer.publish_acl.is_empty() {
            panic!(
                "[[wasm_consumer]] {slug:?}: has {} bound output port(s) but publish_acl is empty \
                 — under deny-by-default the component could never publish to its own bound \
                 channels (every publish would return not-permitted at runtime); add a \
                 publish_acl matcher covering each bound channel (e.g. {{ exact = \"<name>\" }}) \
                 or remove the output bindings",
                outputs.len(),
            );
        }

        // 2c. Non-empty publish_acl but Ports not granted → dead matchers. The
        //      `build_wasm_policy` mapping derives `MessagingPublish` only from the
        //      `Ports` grant; without it `allows_brenn_publish` is unconditionally
        //      false, so the authored matchers can never authorize any publish. The
        //      operator wrote a publish_acl expecting it to grant publish access;
        //      silently dropping it is the same runtime-only landmine fail-fast
        //      rejects. Panic now so the misconfiguration is fixed at boot, not
        //      discovered as an unexplained NotPermitted after outputs are added.
        if !consumer.publish_acl.is_empty() && !grants.contains(&WasmGrant::Ports) {
            panic!(
                "[[wasm_consumer]] {slug:?}: publish_acl has {} matcher(s) but \"ports\" is not in \
                 grants — without the ports grant the matchers can never authorize any publish \
                 (MessagingPublish capability absent); add \"ports\" to grants or remove publish_acl",
                consumer.publish_acl.len(),
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
        if !consumer.mqtt_publish_acl.is_empty() && !grants.contains(&WasmGrant::Mqtt) {
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
        // provably dead (no WasmGrant maps to DynamicSubscribe, so nothing can ever
        // exercise them), unlike the LLM side where an ACL without a static sub
        // legitimately pre-authorizes future dynamic subs. Adding a 2g check here
        // diverges WASM from the shared subscribe_acl convention, so it wants a
        // design decision before landing.

        let config_field_name = format!("[[wasm_consumer]] {slug:?} config");
        let config = resolve_component_config(consumer.config.as_ref(), &config_field_name);

        // Build the unified AppPolicy from the resolved grants + authored
        // subscribe_acl/publish_acl channel matchers. This maps each
        // WasmGrant onto its unified AppCapability and validates+converts the
        // channel matchers (fail-fast on a malformed matcher). It is a *separate*
        // mapping from the brenn-wasm::Capability linker conversion. The
        // `subscribe_acl` matchers (plus the derived `MessagingSubscribe` grant)
        // ARE enforced at delivery time over `Wasm` subscribers; the broader WASM
        // enforcement surface (linker-seam capabilities, publish_acl) is not yet
        // wired here.
        let mut policy = brenn_lib::access::resolve::build_wasm_policy(
            slug,
            grants.iter().copied(),
            brenn_lib::access::raw::WasmAclsRaw {
                subscribe: &consumer.subscribe_acl,
                publish: &consumer.publish_acl,
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

        result.push(ResolvedWasmConsumer {
            slug: slug.clone(),
            component_path: consumer.component_path.clone(),
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
