//! Startup composition root.

mod apps;
mod automation;
mod cleanup;
pub(crate) mod messaging;
mod mqtt;
mod obs_config;
mod pwa_push;
mod repo_sync;
mod shutdown;
mod wasm_mqtt;
mod webhook;

use std::path::PathBuf;

use brenn_lib::config::{BrennConfig, ResolvedConfig, validate_and_resolve};
use brenn_lib::db;
use brenn_lib::integration::IntegrationRegistry;
use brenn_lib::obs;
use tokio::net::TcpListener;
use tracing::info;

use crate::state::AppState;

pub async fn run_invite(config: &BrennConfig) {
    let db = db::init_db(&config.database.path);
    let conn = db.lock().await;
    let code = brenn_lib::auth::invite::create_invite_code(&conn);
    println!("{code}");
}

/// The library cannot prove the build-id invariants at compile time (the value
/// arrives as a runtime parameter), so re-assert them on entry: an empty or
/// over-long build id would overflow the WS Close-frame reason (RFC 6455
/// 123-byte limit). Panic fast rather than serve a value that would.
fn assert_build_id_valid(build_id: &str) {
    assert!(
        !build_id.is_empty() && build_id.len() <= 64,
        "build_id must be non-empty and at most 64 chars, got {build_id:?}"
    );
}

pub async fn run_server(config: BrennConfig, config_path: Option<PathBuf>, build_id: &'static str) {
    assert_build_id_valid(build_id);

    let obs_config = obs_config::build(&config, config_path.as_ref());
    obs::install_pending_panic_hook(&obs_config);
    let guard = obs::init(&obs_config);

    // Write PID file if configured (used by logrotate's postrotate to send SIGHUP).
    if let Some(ref pid_file_path) = config.server.pid_file {
        crate::pid_file::write_pid_file(pid_file_path);
    }

    // Create repo_dir and empty subdirectories so validate_and_resolve()
    // passes working_dir checks. Actual cloning happens after validation,
    // when we have ContainerSpawnConfig for container-side clones.
    apps::prepare_repo_dirs(&config);

    let integration_registry = IntegrationRegistry::new(vec![
        Box::new(brenn_pfin::PfinFactory),
        Box::new(brenn_graf::GrafFactory),
    ]);
    // Resolve XDG_RUNTIME_DIR at most once, and only when the config contains
    // at least one bare (non-containerized) app. Container-only configs pay zero
    // cost and never touch the env. The validated PathBuf is borrowed into
    // validate_and_resolve via Option<&Path> so a single resolution serves all
    // bare apps in the config.
    let runtime_dir: Option<std::path::PathBuf> = config
        .apps
        .iter()
        .any(|a| a.container.is_none())
        .then(brenn_lib::runtime_dir::resolve_validated_xdg_runtime_dir);
    let ResolvedConfig {
        apps,
        webhook_endpoints,
        mut mqtt_ingress_channels,
        mqtt_clients,
        pwa_push: resolved_pwa_push,
    } = validate_and_resolve(&config, &integration_registry, runtime_dir.as_deref());

    // Auto-clone repos into the directories created above. Runs after
    // validation so we have ContainerSpawnConfig for container-side clones
    // (SSH keys live inside the container's persistent home). Clones run
    // concurrently via join_all in auto_clone_repos.
    if config.repo_dir.is_some() {
        crate::repo_clone::auto_clone_repos(&config, &apps, &guard.alert_dispatcher).await;
    }

    apps::prepare_and_validate(&apps);

    // Synchronous before serving: apps that configure startup_hooks
    // require their data to be fresh before accepting traffic.
    apps::run_startup_hooks(&apps, &guard.alert_dispatcher).await;

    // Virtual tools files are written after the tool registry is built (below),
    // because `registry_virtual_tools` projects each app's granted registry
    // tools from their descriptors.

    // Clean up stale podman containers from previous crashes.
    // Only runs if any app uses container isolation. Uses the `brenn-managed`
    // label + `status=exited` filter so only stopped containers are touched —
    // running containers from any deployment are never affected.
    if apps.values().any(|a| a.container_spawn.is_some()) {
        cleanup::cleanup_stale_containers().await;
    }

    let db = db::init_db(&config.database.path);

    // Close any usage sessions that were open when the server last shut down.
    // This must complete before the server starts accepting requests (so that
    // new sessions are attributed correctly), but the prune is not boot-critical
    // and is spawned separately to avoid blocking startup behind the DB lock.
    {
        let conn = db.lock().await;
        let closed = brenn_lib::usage::close_open_sessions_on_startup(&conn);
        if closed > 0 {
            info!(
                count = closed,
                "closed open usage sessions from previous run"
            );
        }
    }
    // Prune usage data older than 90 days to bound disk growth.
    // Spawned so the lock is not held during startup — data older than 90 days
    // sitting around for a few extra seconds is harmless.
    {
        let db_for_prune = db.clone();
        tokio::spawn(async move {
            let prune_before = chrono::Utc::now() - chrono::Duration::days(90);
            let conn = db_for_prune.lock().await;
            brenn_lib::usage::prune_usage_before(&conn, prune_before);
        });
    }

    let pending_uploads: crate::state::PendingUploads = Default::default();
    // Build the tool registry: built-in tools + integration tools.
    let integration_tools = integration_registry.collect_tools();
    let tool_registry = crate::tools::build_tool_registry(integration_tools);

    // Propagate the drain-time repo_sync staleness cap to the library
    // constant read by `drain_pending_events`. Must be set *before* any
    // bridge spawns; no bridges exist this early in startup.
    brenn_lib::messaging::set_repo_sync_staleness_days(config.repo_sync.stale_conversation_days);

    // Validate at startup for a clear panic before any task is spawned.
    // The guard also fires inside event_cleanup_loop itself so it remains
    // enforced regardless of call site.
    brenn_lib::messaging::assert_delivered_retention_days_valid(
        config.events.delivered_retention_days,
    );

    let active_bridges = crate::active_bridge::ActiveBridges::new();

    // Spawn the repo-sync manager. Returns `None` if no sync-enabled
    // clones are configured. `AppState::repo_sync_sender` keeps a sender
    // alive so the spawned task survives for the server lifetime.
    // See `docs/designs/repo-sync.md`. The webhook index is built from the
    // manager's own clone set so the two can't disagree on which remotes
    // count as live.
    let repo_sync_result = repo_sync::start_repo_sync(
        db.clone(),
        active_bridges.clone(),
        guard.alert_dispatcher.clone(),
        &config.repos,
        &config.repo_sync,
        &apps,
    )
    .await;

    // Assert that every envelope_type='brenn' row in messaging_messages has a
    // structured sender (app:, conversation:, or wasm: prefix). Runs
    // unconditionally — messaging_messages exists in every DB (run_messaging_migrations
    // is called for all deployments), and a deployment that currently has no
    // messaging config may still carry rows from when it was enabled. Panics with
    // row detail and remediation if any pre-migration sender is found.
    {
        let conn = db.lock().await;
        brenn_lib::messaging::db::assert_senders_structured(&conn);
    }

    // Resolve server_origin once for all messaging paths. Both consumers
    // (build_messaging, build_pwa_push) must use the same value so publisher
    // identities are consistent across bus and pwa_push paths. Resolving once
    // here makes that invariant structural rather than relying on two independent
    // calls to produce the same result.
    //
    // Gated on any messaging feature being active: a deployment with no messaging
    // at all does not require `server.public_url`, so we must not call
    // `resolve_source` (which panics on absent public_url) unless messaging is
    // actually configured.
    let any_messaging = messaging::messaging_configured(
        &config,
        &webhook_endpoints,
        &mqtt_ingress_channels,
    )
        // build_pwa_push also consumes server_origin; these two terms force
        // resolution for it even when build_messaging itself early-returns.
        || apps.values().any(|a| a.messaging_enabled())
        || resolved_pwa_push.is_some();
    let messaging_server_origin: Option<std::sync::Arc<str>> = if any_messaging {
        Some(brenn_lib::messaging::resolve_source(&config.server))
    } else {
        None
    };

    // Build the first-class tool registry over the shared repo-sync state, then
    // validate every app's tool grants against it (fail-fast before serving).
    // The origin string keys tool callers' `ParticipantId` (`app:<slug>@<origin>`);
    // when messaging is off there is no public URL, so fall back to the bind
    // address — any non-empty stable identifier suffices for grant/rate-limit
    // keying.
    let tool_server_origin: std::sync::Arc<str> = messaging_server_origin
        .clone()
        .unwrap_or_else(|| std::sync::Arc::from(config.server.bind_address.to_string()));
    let tool_registry_core: std::sync::Arc<crate::tool_registry::ToolRegistry> = {
        let git_repo_pull = crate::tool_registry::GitRepoPullTool::new(
            repo_sync_result.clones.clone(),
            repo_sync_result.remote_locks.clone(),
            repo_sync_result.sender.clone(),
        );
        let registry = crate::tool_registry::ToolRegistry::new(vec![
            crate::tool_registry::RegisteredTool::Async(std::sync::Arc::new(git_repo_pull)),
        ]);
        registry.validate_config(&apps);
        std::sync::Arc::new(registry)
    };

    // Write virtual tools files for each app's noop MCP server (once at
    // startup), now that the registry can project granted registry tools.
    apps::write_virtual_tools(&apps, &tool_registry_core);

    // Messaging MVP: build the channel directory, upsert configured
    // channels, rebuild subscriptions, and build the messenger plus
    // concrete router.
    //
    // Background tasks are NOT spawned here — they run after `set_state`
    // below so a server-restart-recovery scan that finds a past-deadline /
    // past-release row already has a fully initialized router for
    // `spawn_eager_wake`. Without that ordering, those rows could be
    // released-and-orphaned during the startup race (review F1).
    let mut messaging_result = messaging::build_messaging(
        &config,
        db.clone(),
        &apps,
        active_bridges.clone(),
        guard.alert_dispatcher.clone(),
        messaging_server_origin.clone(),
        &webhook_endpoints,
        &mqtt_ingress_channels,
        &mqtt_clients,
        &tool_registry_core,
    )
    .await;

    // Boot re-activation of durable dynamic `mqtt:` subscriptions (design §3): the
    // boot merge folded them into the directory, but the ingress supervisor's
    // broker SUBSCRIBE union and the router's `IngressRoute` table (built below by
    // `start_mqtt`/`wire_mqtt_state`) are derived only from the *static*
    // `mqtt_ingress_channels`. Append a `ResolvedMqttIngressChannel` for each kept
    // dynamic `mqtt:` sub whose filter has no static channel — filling `urgency`
    // from the client's `[[mqtt_client]]` (the same per-client constant a static
    // channel on this client carries) — so its SUBSCRIBE is re-asserted on connect
    // and its deliveries route after restart. Without this a runtime-created
    // `mqtt:` subscription to a never-statically-declared filter silently stops
    // delivering after a restart.
    {
        use brenn_lib::mqtt::config::ResolvedMqttIngressChannel;
        let dynamic = std::mem::take(&mut messaging_result.dynamic_mqtt_ingress);
        for dyn_ch in dynamic {
            match mqtt_clients.get(&dyn_ch.client_slug) {
                Some(client) => {
                    mqtt_ingress_channels.push(ResolvedMqttIngressChannel {
                        channel_address: dyn_ch.channel_address,
                        channel_uuid: dyn_ch.channel_uuid,
                        client_slug: dyn_ch.client_slug,
                        topic: dyn_ch.topic,
                        qos: dyn_ch.qos,
                        urgency: client.urgency,
                    });
                }
                None => {
                    // The dynamic sub was created against a configured client, but the
                    // client was removed from `[[mqtt_client]]` config between boots —
                    // durable user state config has since overridden, not a host bug.
                    // Drop it from re-activation with a warn (its directory subscriber
                    // remains harmless; nothing will deliver to it).
                    tracing::warn!(
                        client = %dyn_ch.client_slug,
                        channel = %dyn_ch.channel_address,
                        "boot: dropping dynamic mqtt subscription whose client is no longer a \
                         configured [[mqtt_client]] — not re-activating its broker SUBSCRIBE/route"
                    );
                }
            }
        }
    }

    // Observability: log the boot-resolved surfaces and ephemeral
    // channels. Skipped when empty so a config with no `[[surface]]` /
    // `[[ephemeral_channel]]` blocks emits no new log line (upholding the
    // bit-for-bit-unchanged guarantee); the emptiness check is also the field read
    // that keeps these `pub(crate)` `MessagingResult` fields off the `dead_code`
    // lint until later consumers use them.
    if !messaging_result.surfaces.is_empty() {
        let slugs: Vec<&str> = messaging_result
            .surfaces
            .iter()
            .map(|s| s.slug.as_str())
            .collect();
        tracing::info!(
            count = messaging_result.surfaces.len(),
            surfaces = ?slugs,
            "boot: resolved [[surface]] blocks",
        );
    }
    if !messaging_result.ephemeral_channels.is_empty() {
        let names: Vec<&str> = messaging_result
            .ephemeral_channels
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        tracing::info!(
            count = messaging_result.ephemeral_channels.len(),
            channels = ?names,
            "boot: resolved [[ephemeral_channel]] blocks",
        );
    }

    // Validate `[observability] surface_error_channel` while the messaging
    // directory and the resolved surfaces are both in hand, before any session
    // can attach. No-op when the channel is unset; every failure is a boot panic
    // (operator config, fail fast).
    let messenger = messaging_result.messenger.as_ref();
    // Sweep the exact post-injection app map the publish gates consult, so the
    // single-writer validation cannot drift from enforcement. Empty when no
    // messaging is configured (the channel-set-but-no-messaging case panics on
    // the absent directory before these are read).
    let app_policies: Vec<(&str, &brenn_lib::access::AppPolicy)> = messenger
        .map(|m| m.app_policies().collect())
        .unwrap_or_default();
    crate::routes::surface::validate_surface_error_channel(
        config.observability.surface_error_channel.as_deref(),
        messenger.map(|m| &**m.directory()),
        config.messaging.max_body_bytes,
    );

    // Validate the derived self-description channel set under the same directory
    // + resolved surfaces (single-writer, brenn:, declared,
    // standing_retain_depth >= 1). Every failure is a boot panic.
    crate::routes::surface::description::validate_surface_description(
        &config.surface_description,
        &messaging_result.surfaces,
        messenger.map(|m| &**m.directory()),
        crate::routes::surface::SingleWriterPrincipals {
            app_policies: &app_policies,
            wasm_consumers: &messaging_result.wasm_consumers,
            surfaces: &messaging_result.surfaces,
            system_participants: &messaging_result.system_participants,
        },
    );

    // Publish the surface self-description documents once at boot, so any app can
    // pull them via `MessageChannelGet`. Built from the resolved surfaces, the
    // running build id, and the per-kind sidecar files under `surface_dist_dir`,
    // so the retained docs describe the running config. Skipped only when no
    // messaging is configured: there is no bus to publish onto and no surface to
    // describe.
    if let Some(messenger) = messenger {
        let prefix = &config.surface_description.prefix;
        let docs = crate::routes::surface::description::build_description_docs(
            prefix,
            build_id,
            &messaging_result.surfaces,
            &config.server.surface_dist_dir,
        );
        crate::routes::surface::description::publish_description(messenger, &docs).await;

        // Boot disconnected stamps: after the boot-published docs, write a
        // `disconnected` status snapshot (reason "server restart", the new bus
        // epoch, empty instances) per configured surface. A durable status
        // channel's retained row survives the restart; without this a dead or
        // not-yet-connected wall would read "healthy as of before the restart"
        // until a reader did timestamp math.
        let epoch = messenger.ephemeral_bus().epoch();
        crate::routes::surface::telemetry::publish_boot_disconnected_stamps(
            messenger,
            prefix,
            &messaging_result.surfaces,
            epoch,
        )
        .await;
    }

    // Build the per-surface runtime bundle map, keyed by slug. Each runtime
    // shares the one process `EphemeralBus`. Any non-empty `[[surface]]` list
    // forces messaging on (`any_messaging` above), so a `Messenger` — and thus a
    // bus — exists whenever surfaces do; the `expect` cites that gate.
    let surface_runtimes = {
        let surfaces = std::mem::take(&mut messaging_result.surfaces);
        if surfaces.is_empty() {
            std::collections::HashMap::new()
        } else {
            // Fail-fast on missing surface assets before any runtime is built: a
            // configured surface whose shell/component modules are absent from
            // surface_dist_dir is an un-serveable deploy, caught at boot rather
            // than as a broken page under auth later.
            crate::routes::surface::validate_surface_assets(
                &config.server.surface_dist_dir,
                &surfaces,
            );
            let messenger = messaging_result.messenger.as_ref().expect(
                "[[surface]] blocks configured but no Messenger: the any_messaging gate \
                 forces messaging on whenever surfaces exist, so a bus must be present",
            );
            let bus = messenger.ephemeral_bus().clone();
            // Reserved error-report port + Welcome floor, wired when an error
            // channel is configured. The floor defaults to `warn`.
            let error_report = config
                .observability
                .surface_error_channel
                .as_ref()
                .map(|addr| {
                    (
                        addr.clone(),
                        config.observability.surface_error_publish_floor,
                    )
                });
            crate::routes::surface::build_surface_runtimes(
                surfaces,
                bus,
                Some(messenger.clone()),
                config.messaging.max_body_bytes,
                error_report,
                crate::routes::surface::SurfaceDescriptionParams {
                    prefix: config.surface_description.prefix.clone(),
                    status_interval_secs: config.surface_description.status_interval_secs,
                },
            )
        }
    };

    // PWA push: construct the PwaPushService from the already-resolved config.
    // Returns `None` when no app has `pwa_push.enabled = true`.
    let pwa_push_service = pwa_push::build_pwa_push(
        &config,
        db.clone(),
        &apps,
        guard.alert_dispatcher.clone(),
        resolved_pwa_push,
        messaging_server_origin,
    );

    // Automation engine: built when a messenger is configured. Uses the
    // same deferred-state pattern as `WakeRouterImpl`.
    //
    // When no messenger is configured, the engine stays `None`; intercept
    // handlers return a "not configured" error to the LLM.
    let automation_result = automation::build_automation(
        &config,
        db.clone(),
        &apps,
        messaging_result.messenger.as_ref(),
        guard.alert_dispatcher.clone(),
    );

    // MQTT service: build a MqttService with one unified supervisor per referenced
    // `[[mqtt_client]]` (referenced by an ingress channel, an `mqtt_publish` ACL
    // matcher, or an `mqtt_subscribe` ACL matcher). Each session carries both the
    // publish and the ingress-delivery paths.
    //
    // `None` when no `[[mqtt_client]]` is declared OR no client is referenced.
    let mqtt_result = mqtt::start_mqtt(
        &config,
        &apps,
        &messaging_result.wasm_consumers,
        &mqtt_ingress_channels,
        &mqtt_clients,
    )
    .await;

    // Webhook service: build from pre-resolved endpoint table.
    //
    // `None` when no `[[webhook_endpoint]]` is declared OR no app declares any
    // `[[app.webhook_subscription]]`.
    let webhook_result = webhook::build_webhook(webhook_endpoints);

    // Replay-protection components: load each endpoint's WASM component at
    // startup, using the already-resolved (canonical) paths from the
    // WebhookService. Panics on failure (per §2.6 / AC-startup-fail-loud).
    let (replay_components, replay_locks) = {
        use std::collections::HashMap;
        use std::sync::Arc;
        let mut components = HashMap::new();
        let mut locks = HashMap::new();
        if let Some(ref svc) = webhook_result.service {
            for ep in svc.all_endpoints() {
                if let Some(ref rp) = ep.replay_protection {
                    let component = brenn_wasm::ReplayComponent::load(
                        &ep.slug,
                        &rp.component_path,
                        &rp.store_path,
                        rp.max_page_count,
                        rp.config.clone(),
                    );
                    components.insert(ep.slug.clone(), Arc::new(component));
                    locks.insert(ep.slug.clone(), Arc::new(tokio::sync::Mutex::new(())));
                    info!(
                        endpoint = %ep.slug,
                        component_path = %rp.component_path.display(),
                        store_path = %rp.store_path.display(),
                        "replay protection loaded"
                    );
                }
            }
        }
        (Arc::new(components), Arc::new(locks))
    };

    // Cross-store path uniqueness: replay stores (from webhook endpoints) and
    // consumer stores (from wasm_consumers) must not alias each other.
    // The OPEN_PATHS guard in KvStore::open catches this at load time, but with
    // a generic error. A boot-time check here provides the human-readable panic
    // (design §2.4 "explicit boot panic with a clear message").
    {
        let replay_paths: Vec<std::path::PathBuf> = webhook_result
            .service
            .as_ref()
            .map(|svc| {
                svc.all_endpoints()
                    .filter_map(|ep| {
                        ep.replay_protection
                            .as_ref()
                            .map(|rp| rp.store_path.clone())
                    })
                    .collect()
            })
            .unwrap_or_default();
        let consumer_paths: Vec<(std::path::PathBuf, String)> = messaging_result
            .wasm_consumers
            .iter()
            .filter_map(|c| c.store_path.clone().map(|p| (p, c.slug.clone())))
            .collect();
        assert_unique_store_paths(&replay_paths, &consumer_paths);
    }

    // WASM processor components: load each [[wasm_consumer]]'s component at
    // startup. Panics on failure (missing/unloadable component is a bootstrap
    // panic per requirements/Lifecycle). Also creates a per-consumer Notify and
    // registers it on the WakeRouter for the off-loop dispatch task (§2.5).
    let processing_components = {
        use std::collections::HashMap;
        use std::sync::Arc;
        let mut components: HashMap<String, Arc<brenn_wasm::ProcessorComponent>> = HashMap::new();
        for consumer in &messaging_result.wasm_consumers {
            // Build the output port map: port name → binding + per-sink budget.
            let output_ports: HashMap<String, brenn_wasm::OutputPortSpec> = consumer
                .outputs
                .iter()
                .map(|o| {
                    use brenn_lib::messaging::Urgency;
                    use brenn_wasm::ProcessorUrgency;
                    let wu = match o.default_urgency {
                        Urgency::VeryLow => ProcessorUrgency::VeryLow,
                        Urgency::Low => ProcessorUrgency::Low,
                        Urgency::Normal => ProcessorUrgency::Normal,
                        Urgency::High => ProcessorUrgency::High,
                    };
                    (
                        o.port.clone(),
                        brenn_wasm::OutputPortSpec {
                            channel_address: o.channel_address.clone(),
                            default_urgency: wu,
                            budget: brenn_wasm::SinkBudget {
                                fill_mt: o.budget.fill_mt,
                                capacity_mt: o.budget.capacity_mt,
                            },
                        },
                    )
                })
                .collect();
            // Input port → publish amplification (millitokens). Windows are built
            // from the same `inputs`, so every driven window port is present.
            let input_amplification_mt: HashMap<String, u64> = consumer
                .inputs
                .iter()
                .map(|i| (i.port.clone(), i.amplification_mt))
                .collect();
            // MQTT egress sinks: ACL-allowed client slug → per-sink budget.
            let mqtt_sinks: HashMap<String, brenn_wasm::SinkBudget> = consumer
                .mqtt_sinks
                .iter()
                .map(|(client, b)| {
                    (
                        client.clone(),
                        brenn_wasm::SinkBudget {
                            fill_mt: b.fill_mt,
                            capacity_mt: b.capacity_mt,
                        },
                    )
                })
                .collect();
            let alerter = std::sync::Arc::new(crate::wasm_dispatch::DispatcherAlerter::new(
                guard
                    .alert_dispatcher
                    .clone()
                    .with_field("wasm_slug", &consumer.slug),
                consumer.slug.clone(),
            ));
            // Map WasmGrant → brenn_wasm::Capability (exhaustive match enforces
            // sync when new variants are added to either type).
            let mut grants: std::collections::BTreeSet<brenn_wasm::Capability> = consumer
                .grants
                .iter()
                .map(|g| {
                    use brenn_lib::messaging::config::WasmGrant;
                    use brenn_wasm::Capability;
                    match g {
                        WasmGrant::Ports => Capability::Ports,
                        WasmGrant::Store => Capability::Store,
                        WasmGrant::Log => Capability::Log,
                        WasmGrant::Alert => Capability::Alert,
                        WasmGrant::Config => Capability::Config,
                        WasmGrant::Mqtt => Capability::Mqtt,
                    }
                })
                .collect();
            // Derive the `Tools` capability (grant name `"tools"`) iff the consumer
            // holds ≥1 tool grant. There is no `WasmGrant::Tools` token — the tool
            // surface is authorized by `[[wasm_consumer.tool_grant]]` tables, not a
            // grant line, and the capability presence is derived from them. The
            // `tools` WIT interface links iff this is present; which tools are
            // addressable is the per-call `tool_grants` lookup the host does.
            let has_tool_grants = !consumer.policy.tool_grants.is_empty();
            if has_tool_grants {
                grants.insert(brenn_wasm::Capability::Tools);
            }
            // Lower the resolved `brenn_publish` ACL to the brenn-lib-free output-ACL
            // predicate the WASM host calls in `do_publish`. The closure owns the
            // `brenn:`-prefix convention so `brenn-wasm` never sees a brenn-lib type nor
            // parses the address: `output_ports` holds the FULL `brenn:<name>` address,
            // while `allows_brenn_publish` takes the stripped name. A non-`brenn:`
            // address can never be in a `brenn_publish` ACL, so it denies.
            let policy = consumer.policy.clone();
            let output_acl: brenn_wasm::OutputAclFn = std::sync::Arc::new(move |addr: &str| {
                match addr.strip_prefix(brenn_lib::messaging::BRENN_ADDRESS_PREFIX) {
                    Some(name) => policy.allows_brenn_publish(name),
                    None => false,
                }
            });
            // Synchronous MQTT egress callback. Built iff the consumer holds the
            // `Mqtt` grant — the `mqtt` interface is linked iff this is `Some`, and
            // `ProcessorComponent::load` re-asserts that invariant. The constructor
            // captures the resolved policy (the same one the LLM path uses), the
            // consumer slug as the connector-namespace key, and the (optional) MQTT
            // service.
            let mqtt_publish: Option<brenn_wasm::MqttPublishFn> = if consumer
                .grants
                .contains(&brenn_lib::messaging::config::WasmGrant::Mqtt)
            {
                Some(wasm_mqtt::make_wasm_mqtt_publish_fn(
                    consumer.policy.clone(),
                    consumer.slug.clone(),
                    mqtt_result.service.clone(),
                    guard.alert_dispatcher.clone(),
                ))
            } else {
                None
            };
            // Real tool host over the shared registry, built iff the consumer
            // holds ≥1 tool grant (so `tool_host.is_some()` tracks the `Tools`
            // capability — `ProcessorComponent::load` re-asserts that invariant).
            // Validate the consumer's grants against the registry first (fail-fast,
            // before serving): the resolved wasm policy is not in the `apps` map
            // `validate_config` scanned at registry build, so it is checked here.
            let tool_host: Option<brenn_wasm::ToolHostFn> = if has_tool_grants {
                tool_registry_core.validate_grants(
                    &format!("wasm consumer {:?}", consumer.slug),
                    &consumer.policy.tool_grants,
                );
                Some(std::sync::Arc::new(
                    crate::tool_registry::WasmToolHost::new(
                        tool_registry_core.clone(),
                        consumer.policy.tool_grants.clone(),
                        consumer.slug.clone(),
                        guard.alert_dispatcher.clone(),
                    ),
                ))
            } else {
                None
            };
            let component = brenn_wasm::ProcessorComponent::load(brenn_wasm::ProcessorLoadSpec {
                component_path: &consumer.component_path,
                slug: &consumer.slug,
                output_ports,
                input_amplification_mt,
                mqtt_sinks,
                config: consumer.config.clone(),
                grants,
                store_path: consumer.store_path.as_deref(),
                max_page_count: consumer.max_page_count,
                max_payload_bytes: config.messaging.max_body_bytes,
                alerter,
                output_acl,
                mqtt_publish,
                tool_host,
            });
            let store_path_present = consumer.store_path.is_some();
            info!(
                slug = %consumer.slug,
                component_path = %consumer.component_path.display(),
                store_path_present,
                store_path = consumer.store_path.as_deref().map(|p| p.display().to_string()),
                "WASM processor component loaded"
            );
            components.insert(consumer.slug.clone(), Arc::new(component));
        }
        Arc::new(components)
    };

    // Register per-consumer Notify instances on the WakeRouter as ParkedNotify
    // delivery bindings and retain a clone for each off-loop dispatch task (design
    // §2.7). The router stores one Arc clone; the task gets another. Must happen
    // before set_state so bindings are present when the first WASM push arrives.
    let wasm_notifiers: Vec<(String, std::sync::Arc<tokio::sync::Notify>)> = {
        use crate::messaging_router::DeliveryBinding;
        use brenn_lib::messaging::SubscriberEntryKind;
        let mut notifiers = Vec::new();
        if let Some(ref router) = messaging_result.router {
            for consumer in &messaging_result.wasm_consumers {
                let notify = std::sync::Arc::new(tokio::sync::Notify::new());
                router.register_delivery_binding(
                    SubscriberEntryKind::Wasm(consumer.slug.clone()),
                    DeliveryBinding::ParkedNotify(notify.clone()),
                );
                notifiers.push((consumer.slug.clone(), notify));
            }
        }
        notifiers
    };

    // Register a ParkedNotify delivery binding for every subscribing system
    // participant (before set_state, like the wasm notifiers above, so a request
    // row found by the startup dispatcher sweep can eager-wake the drain loop),
    // retaining each `Notify` for the participant's drain task. Publish-only
    // specs (no subscriptions) are never dispatch targets and get no binding.
    let system_notifiers: Vec<(&'static str, std::sync::Arc<tokio::sync::Notify>)> = {
        let mut notifiers = Vec::new();
        if let Some(ref router) = messaging_result.router {
            for spec in &messaging_result.system_participants {
                if spec.subscriptions.is_empty() {
                    continue;
                }
                let notify = std::sync::Arc::new(tokio::sync::Notify::new());
                router.register_delivery_binding(
                    brenn_lib::messaging::SubscriberEntryKind::System(spec.component.to_string()),
                    crate::messaging_router::DeliveryBinding::ParkedNotify(notify.clone()),
                );
                notifiers.push((spec.component, notify));
            }
        }
        notifiers
    };

    // Build the tool executor's wiring off its spec-derived notifier: the
    // per-caller grant table it re-checks each dequeued request against, plus the
    // `Notify` its `SystemInbox` parks on. `Some` iff messaging is wired and at
    // least one async tool is registered (the executor's spec exists exactly
    // then — it subscribes to every `brenn:tools/<tool>` request channel).
    let tool_executor_wiring: Option<(
        std::sync::Arc<tokio::sync::Notify>,
        std::sync::Arc<crate::tool_registry::ToolCallerGrants>,
    )> = system_notifiers
        .iter()
        .find(|(component, _)| *component == crate::tool_registry::TOOL_EXECUTOR_COMPONENT)
        .map(|(_, notify)| {
            let mut caller_grants: crate::tool_registry::ToolCallerGrants =
                std::collections::HashMap::new();
            for consumer in &messaging_result.wasm_consumers {
                if consumer.policy.tool_grants.is_empty() {
                    continue;
                }
                let caller = brenn_lib::messaging::ParticipantId::for_wasm(&consumer.slug)
                    .as_str()
                    .to_owned();
                caller_grants.insert(caller, consumer.policy.tool_grants.clone());
            }
            (notify.clone(), std::sync::Arc::new(caller_grants))
        });

    // Register the remaining delivery bindings: every configured app delivers
    // inline through its conversation bridge, and every surface fans out to its
    // attached sessions. Together with the WASM/system ParkedNotify bindings
    // above, this covers every subscriber the dispatcher can target — a missing
    // binding at dispatch time is a host-wiring invariant violation and panics.
    if let Some(ref router) = messaging_result.router {
        use crate::messaging_router::DeliveryBinding;
        use brenn_lib::messaging::SubscriberEntryKind;
        for slug in apps.keys() {
            router.register_delivery_binding(
                SubscriberEntryKind::App(slug.clone()),
                DeliveryBinding::ConversationBridge,
            );
        }
        // One binding per surface principal (`ResolvedSurface::principals`),
        // because the router resolves the route by the *subscriber's*
        // registration key, and each instance is its own subscriber. Every route
        // is `SurfaceSessions` regardless of grain: the WS is transport, and one
        // session carries the whole page's principals.
        for (slug, runtime) in surface_runtimes.iter() {
            for instance in runtime.resolved.principals() {
                router.register_delivery_binding(
                    SubscriberEntryKind::Surface {
                        slug: slug.clone(),
                        instance,
                    },
                    DeliveryBinding::SurfaceSessions,
                );
            }
        }

        let messenger = messaging_result
            .messenger
            .as_ref()
            .expect("messenger is Some whenever the router is Some (both built together)");
        assert_every_subscriber_wired(messenger, router);
    }

    let state = AppState {
        build_id,
        db,
        alert_dispatcher: guard.alert_dispatcher.clone(),
        active_bridges,
        secure_cookies: config.server.secure_cookies,
        log_dir: config.logging.log_dir,
        mcp_script_path: config.claude_defaults.mcp_script_path,
        apps: apps.clone(),
        bridge_notify_tx: tokio::sync::broadcast::channel(64).0,
        pending_uploads: pending_uploads.clone(),
        static_dir: config.server.static_dir.clone(),
        surface_dist_dir: config.server.surface_dist_dir.clone(),
        cached_models: Default::default(),
        tool_registry,
        tools: tool_registry_core,
        tool_server_origin,
        wake_locks: Default::default(),
        server_shutting_down: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        repo_sync_sender: repo_sync_result.sender,
        messenger: messaging_result.messenger.clone(),
        pwa_push: pwa_push_service,
        mqtt: mqtt_result.service.clone(),
        mqtt_event_router: mqtt_result.event_router.clone(),
        webhook: webhook_result.service.clone(),
        automation_engine: automation_result.engine.clone(),
        usage_session_gap_secs: config.observability.usage.session_gap_minutes * 60,
        surfaces: std::sync::Arc::new(surface_runtimes),
        surface_registry: crate::routes::surface::registry::SurfaceRegistry::default(),
        surface_heartbeat_secs: crate::routes::surface::HEARTBEAT_SECS,
        replay_components,
        replay_locks,
        #[cfg(test)]
        test_wake_bridge: Default::default(),
    };

    // Attach the AppState to the WakeRouter, then spawn the background
    // tasks. Doing the attach first means any past-deadline /
    // past-release row that the deadline / deliver-after scanner finds
    // on its first pass already has a fully-initialized router for
    // `spawn_eager_wake`.
    if let (Some(messenger), Some(router)) = (
        messaging_result.messenger.as_ref(),
        messaging_result.router.as_ref(),
    ) {
        router.set_state(state.clone());
        // Spawn background tasks. Returned JoinHandles are intentionally
        // dropped — these tasks are process-lifetime and never joined.
        // Lifetime-task death is ACCEPTED, not supervised: any panic is logged +
        // Critical-alerted by the global panic hook (brenn-lib/src/obs/panic_hook.rs);
        // alert + manual restart is the decided mitigation. Do NOT add per-task
        // supervision. See TODO.md `task-death-supervision` (tombstone). Same applies
        // to the session/ingress/gc lifetime spawns below.
        // Spawn the unified background dispatcher task (design §2.3, §2.7).
        // Replaces the former `spawn_deadline_task` + `spawn_deliver_after_task`.
        // The R7 startup kick fires immediately after spawn so conversations holding
        // pending Immediate/deadline-expired rows are eager-woken without waiting
        // for user interaction.
        drop(brenn_lib::messaging::dispatcher::spawn_dispatcher_task(
            state.db.clone(),
            router.clone() as std::sync::Arc<dyn brenn_lib::messaging::WakeRouter>,
            messenger.dispatch_kick_notify(),
            messenger.clone(),
        ));
        // R7 startup sweep: kick the dispatcher immediately so any pending
        // Immediate/deadline-expired rows trigger eager wakes without waiting
        // for the first POLL_INTERVAL sleep.
        messenger.dispatch_kick();

        // Spawn one off-loop WASM consumer dispatch task per [[wasm_consumer]].
        // Dropped handles = process-lifetime tasks (same policy as deadline/deliver-after).
        for (slug, notify) in wasm_notifiers {
            // Slug is in wasm_notifiers iff it was in wasm_consumers (both come from
            // the same config list). A miss means the two maps diverged — a
            // host-wiring invariant violation; panic immediately per BETTER DEAD THAN WRONG.
            // processing_components is a local map (not on AppState); a miss here means
            // the wasm_notifiers and processing_components lists diverged — invariant
            // violation, panic immediately per BETTER DEAD THAN WRONG.
            let component = processing_components.get(&slug).unwrap_or_else(|| {
                panic!(
                    "wasm_dispatch bootstrap: slug {slug:?} is in wasm_notifiers \
                     but absent from processing_components — host-wiring invariant violated"
                )
            });
            let consumer = messaging_result
                .wasm_consumers
                .iter()
                .find(|c| c.slug == slug)
                .unwrap_or_else(|| {
                    panic!("wasm_dispatch bootstrap: slug {slug:?} not in wasm_consumers")
                });
            let inputs: Vec<brenn_lib::messaging::config::WasmInputPort> = consumer.inputs.clone();
            drop(crate::wasm_dispatch::spawn_wasm_consumer_task(
                crate::wasm_dispatch::WasmConsumerConfig {
                    slug: slug.clone(),
                    component: component.clone(),
                    notify,
                    messenger: messenger.clone(),
                    alert_dispatcher: state.alert_dispatcher.clone(),
                    inputs,
                    activation_pacing: consumer.activation_pacing,
                },
            ));
            info!(slug = %slug, "wasm_dispatch: consumer task spawned");
        }

        // Spawn the async tool executor drain task: the single
        // `system:tool-executor` subscriber that turns a bus tool request into an
        // execution and a result activation. Same process-lifetime, unsupervised
        // policy as the wasm dispatch tasks (dropped handle; panics are
        // panic-hook-alerted).
        if let Some((notify, caller_grants)) = tool_executor_wiring {
            drop(
                crate::tool_registry::ToolExecutor::new(
                    messenger.clone(),
                    state.tools.clone(),
                    caller_grants,
                    state.alert_dispatcher.clone(),
                    notify,
                )
                .spawn(),
            );
            info!("tool_registry: async tool executor task spawned");
        }
    }

    // MQTT: inject AppState into the event router so inbound messages can
    // call `submit_ingress`. The supervisors are already running; they won't
    // call `deliver_inbound` until they have an active connection and receive
    // a publish from the broker, which is after this point.
    let mqtt_stop_txs = if let (Some(svc), Some(router)) = (
        mqtt_result.service.as_ref(),
        mqtt_result.event_router.as_ref(),
    ) {
        mqtt::wire_mqtt_state(
            svc,
            router,
            state.clone(),
            &mqtt_ingress_channels,
            mqtt_result.stop_txs,
        )
        .await
    } else {
        // No MQTT configured — return empty vec.
        mqtt_result.stop_txs
    };

    // Webhook: inject AppState into the event router.
    if let (Some(svc), Some(router)) = (
        webhook_result.service.as_ref(),
        webhook_result.event_router.as_ref(),
    ) {
        webhook::wire_webhook_state(svc, router, state.clone()).await;
    }

    // Automation engine: inject state into the IngressRouter, run startup
    // catch-up pass, then spawn the background scheduler loop.
    if let (Some(engine), Some(ingress_router)) = (
        automation_result.engine.as_ref(),
        automation_result.ingress_router.as_ref(),
    ) {
        ingress_router.set_state(state.clone());
        // Startup consistency: rebind stale event conversations + disable orphaned jobs.
        brenn_lib::automation::startup::run_startup_consistency_checks(engine).await;
        // Startup catch-up: advance/fire missed slots per §2.10.
        brenn_lib::automation::loop_task::run_startup_catchup(engine).await;
        // Spawn the scheduler loop. JoinHandle dropped — process-lifetime task.
        drop(brenn_lib::automation::loop_task::spawn_automation_loop(
            engine.clone(),
        ));
        info!("automation engine started; scheduler loop spawned");
    }

    // Load cached model lists from DB so the picker works on first connect.
    {
        let conn = state.db.lock().await;
        let all_models = brenn_lib::db::load_all_app_models(&conn);
        drop(conn);
        if !all_models.is_empty() {
            let mut cache = state.cached_models.write().await;
            for (slug, models) in all_models {
                tracing::info!(app = %slug, count = models.len(), "loaded cached models from DB");
                cache.insert(slug, models);
            }
        }
    }

    // Spawn orphan cleanup background task.
    tokio::spawn(crate::routes::upload::orphan_cleanup_loop(
        apps,
        pending_uploads,
        state.db.clone(),
    ));

    // The session-cleanup, ingress-cleanup, and bus-GC spawns below are
    // process-lifetime tasks with intentionally-dropped JoinHandles. Death is
    // ACCEPTED, not supervised: panics are logged + Critical-alerted by the global
    // panic hook; alert + manual restart is the decided mitigation. Do NOT add
    // supervision. See TODO.md `task-death-supervision` (tombstone).
    // Spawn expired session cleanup background task.
    tokio::spawn(shutdown::session_cleanup_loop(state.db.clone()));

    // Spawn stale undelivered ingress cleanup background task.
    tokio::spawn(shutdown::ingress_cleanup_loop(
        state.db.clone(),
        config.events.delivered_retention_days,
    ));

    // Spawn bus GC loop (kind='brenn' only; non-overlapping with ingress cleanup loop).
    // TODO(unify-gc): bus GC loop spawned separately; unification deferred.
    if let Some(messenger) = messaging_result.messenger.as_ref() {
        tokio::spawn(shutdown::bus_gc_loop(
            state.db.clone(),
            messenger.directory().clone(),
            config.messaging.archive_path.clone(),
        ));
    }

    // Spawn the bridge-wedge watchdog: sweeps the live-bridge registry and
    // self-heals a bridge whose event loop died or whose session I/O is dead
    // while the bridge still believes CC is busy.
    crate::active_bridge::spawn_watchdog(
        config.watchdog.clone(),
        state.active_bridges.clone(),
        state.alert_dispatcher.clone(),
    );

    // Capture the handles `shutdown_signal` needs before `state` is consumed
    // by `build_router`. `active_bridges` and `server_shutting_down` are
    // cheap `Clone` (Arc-backed); `mqtt_stop_txs` is moved here so the
    // senders fire MQTT DISCONNECT on SIGTERM/SIGINT.
    let shutdown_handle = shutdown::ShutdownHandle {
        active_bridges: state.active_bridges.clone(),
        server_shutting_down: state.server_shutting_down.clone(),
        mqtt_stop_txs,
    };

    // Warn if resized images will exceed the upload limit. Rough JPEG upper bound: long_edge² / 4 bytes.
    let rough_max_bytes = (config.security.max_image_long_edge as usize).saturating_pow(2) / 4;
    if config.security.upload_body_limit < rough_max_bytes {
        tracing::warn!(
            upload_body_limit = config.security.upload_body_limit,
            max_image_long_edge = config.security.max_image_long_edge,
            rough_max_jpeg_bytes = rough_max_bytes,
            "upload_body_limit is smaller than the estimated maximum JPEG size for \
             max_image_long_edge; resized phone-camera uploads will likely return 413. \
             Consider increasing upload_body_limit or decreasing max_image_long_edge."
        );
    }

    let app = crate::router::build_router(
        state,
        Some(&config.security),
        config.server.trusted_proxy_hops,
        config.security.max_image_long_edge,
    );

    let listener = TcpListener::bind(config.server.bind_address)
        .await
        .unwrap_or_else(|e| panic!("failed to bind to {}: {e}", config.server.bind_address));

    info!("listening on {}", listener.local_addr().unwrap());

    use std::net::SocketAddr;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown::shutdown_signal(shutdown_handle))
    .await
    .expect("server error");

    info!("shutdown complete");

    // Clean up PID file on graceful shutdown.
    if let Some(pid_file) = &config.server.pid_file
        && let Err(e) = std::fs::remove_file(pid_file)
    {
        tracing::debug!("failed to remove PID file {}: {e}", pid_file.display());
    }

    // Guard dropped here — flushes pending log writes.
    drop(guard);
}

/// Assert that all replay store paths and consumer store paths are unique across
/// both sets.
///
/// `replay_paths`: canonical store paths from `[[webhook_endpoint]].replay_protection`.
/// `consumer_paths`: `(store_path, slug)` pairs from `[[wasm_consumer]]`.
///
/// Panics on the first duplicate with a human-readable message (design §2.4).
/// Called before `KvStore::open` so the message names the config source, not the
/// internal `OPEN_PATHS` guard error.
pub(crate) fn assert_unique_store_paths(
    replay_paths: &[std::path::PathBuf],
    consumer_paths: &[(std::path::PathBuf, String)],
) {
    use std::collections::HashMap;
    let mut seen: HashMap<&std::path::Path, String> = HashMap::new();
    for path in replay_paths {
        if let Some(prior_owner) = seen.insert(path.as_path(), "replay endpoint".to_string()) {
            panic!(
                "bootstrap: store_path {:?} is shared between two replay endpoints \
                 (also owned by {prior_owner}) — each store_path must be unique \
                 across all replay and consumer stores (design §2.4)",
                path
            );
        }
    }
    for (path, slug) in consumer_paths {
        let owner_label = format!("[[wasm_consumer]] {slug:?}");
        if let Some(prior_owner) = seen.insert(path.as_path(), owner_label) {
            panic!(
                "bootstrap: store_path {:?} is shared between [[wasm_consumer]] {slug:?} \
                 and {prior_owner} — each store_path must be unique across all \
                 replay and consumer stores (design §2.4)",
                path
            );
        }
    }
}

/// Boot cross-check: every directory subscriber must resolve to both a
/// wake-economics registration and a delivery binding. This is what makes "a
/// new subscriber kind silently inherits nothing and strands its messages"
/// unrepresentable — such an entry cannot get past boot. A missing registration
/// would fail-close ACL at delivery (silent drop); a missing binding would panic
/// at dispatch. Named boot failure beats both.
fn assert_every_subscriber_wired(
    messenger: &brenn_lib::messaging::Messenger,
    router: &crate::messaging_router::WakeRouterImpl,
) {
    for channel in messenger.directory().list() {
        for sub in &channel.subscribers {
            assert!(
                messenger.subscriber_wake_economics(&sub.kind).is_some(),
                "boot cross-check: subscriber {:?} on channel {:?} has no wake-economics \
                 registration — host wiring bug",
                sub.kind,
                channel.address,
            );
            assert!(
                router.has_delivery_binding(&sub.kind),
                "boot cross-check: subscriber {:?} on channel {:?} has no delivery binding — \
                 host wiring bug",
                sub.kind,
                channel.address,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two distinct store paths (no duplicates) must not panic.
    #[test]
    fn unique_store_paths_no_panic() {
        let tmp = tempfile::TempDir::new().unwrap();
        let replay = vec![tmp.path().join("replay.sqlite")];
        let consumer = vec![(
            tmp.path().join("consumer.sqlite"),
            "my-consumer".to_string(),
        )];
        assert_unique_store_paths(&replay, &consumer);
    }

    /// A consumer store path that aliases a replay store path must panic with a
    /// clear message (design §2.4 explicit boot panic requirement).
    #[test]
    #[should_panic(expected = "store_path")]
    fn consumer_store_path_aliasing_replay_panics() {
        let tmp = tempfile::TempDir::new().unwrap();
        let shared = tmp.path().join("shared.sqlite");
        let replay = vec![shared.clone()];
        let consumer = vec![(shared, "my-consumer".to_string())];
        assert_unique_store_paths(&replay, &consumer);
    }

    /// Two consumer store paths sharing the same path must panic.
    #[test]
    #[should_panic(expected = "store_path")]
    fn duplicate_consumer_store_paths_panic() {
        let tmp = tempfile::TempDir::new().unwrap();
        let shared = tmp.path().join("shared.sqlite");
        let consumer = vec![
            (shared.clone(), "consumer-a".to_string()),
            (shared, "consumer-b".to_string()),
        ];
        assert_unique_store_paths(&[], &consumer);
    }

    /// A valid build id (non-empty, at most 64 chars) must not panic. The
    /// 64-char boundary is inclusive.
    #[test]
    fn valid_build_id_no_panic() {
        assert_build_id_valid("test-build");
        assert_build_id_valid(&"x".repeat(64));
    }

    /// An empty build id must panic: it would produce a zero-length WS
    /// Close-frame reason and defeats the stale-client handshake.
    #[test]
    #[should_panic(expected = "build_id")]
    fn empty_build_id_panics() {
        assert_build_id_valid("");
    }

    mod boot_cross_check {
        use std::collections::HashMap;
        use std::sync::Arc;

        use brenn_lib::access::AppPolicy;
        use brenn_lib::db::init_db_memory;
        use brenn_lib::messaging::config::{Depth, MessagingGlobalConfig, NoiseLevel};
        use brenn_lib::messaging::query::NoopWakeRouter;
        use brenn_lib::messaging::testutils::{test_channel_entry, wasm_registrations};
        use brenn_lib::messaging::{
            MessagingDirectory, Messenger, SubscriberEntry, SubscriberEntryKind,
            SubscriberRegistration, WakeRouter,
        };
        use indexmap::IndexMap;

        use crate::active_bridge::ActiveBridges;
        use crate::messaging_router::{DeliveryBinding, WakeRouterImpl};

        use super::super::assert_every_subscriber_wired;

        const SLUG: &str = "my-consumer";

        fn wasm_key() -> SubscriberEntryKind {
            SubscriberEntryKind::Wasm(SLUG.to_string())
        }

        fn wasm_sub() -> SubscriberEntry {
            SubscriberEntry {
                kind: wasm_key(),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: None,
            }
        }

        /// A `Messenger` whose directory holds one channel with one WASM
        /// subscriber; `regs` controls whether that subscriber has a
        /// wake-economics registration.
        fn messenger(regs: HashMap<SubscriberEntryKind, SubscriberRegistration>) -> Arc<Messenger> {
            let entry = test_channel_entry("consumer/reqs", vec![wasm_sub()]);
            Messenger::new(
                init_db_memory(),
                Arc::new(MessagingDirectory::with_entries(vec![entry])),
                Arc::from("test"),
                Arc::new(IndexMap::new()),
                Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
                MessagingGlobalConfig::default(),
            )
            .with_subscriber_registrations(regs)
        }

        fn registered() -> HashMap<SubscriberEntryKind, SubscriberRegistration> {
            wasm_registrations(HashMap::from([(SLUG.to_string(), AppPolicy::default())]))
        }

        /// Fully wired (registration + binding) passes the cross-check.
        #[test]
        fn fully_wired_subscriber_passes() {
            let messenger = messenger(registered());
            let router = WakeRouterImpl::new(ActiveBridges::new());
            router.register_delivery_binding(
                wasm_key(),
                DeliveryBinding::ParkedNotify(Arc::new(tokio::sync::Notify::new())),
            );
            assert_every_subscriber_wired(&messenger, &router);
        }

        /// A directory subscriber with a registration but no delivery binding
        /// fails the cross-check with the named panic.
        #[test]
        #[should_panic(expected = "has no delivery binding")]
        fn missing_binding_panics() {
            let messenger = messenger(registered());
            let router = WakeRouterImpl::new(ActiveBridges::new());
            assert_every_subscriber_wired(&messenger, &router);
        }

        /// A directory subscriber with a binding but no wake-economics
        /// registration fails the cross-check with the named panic.
        #[test]
        #[should_panic(expected = "has no wake-economics")]
        fn missing_registration_panics() {
            let messenger = messenger(HashMap::new());
            let router = WakeRouterImpl::new(ActiveBridges::new());
            router.register_delivery_binding(
                wasm_key(),
                DeliveryBinding::ParkedNotify(Arc::new(tokio::sync::Notify::new())),
            );
            assert_every_subscriber_wired(&messenger, &router);
        }
    }

    /// A build id over 64 chars must panic: it would overflow the RFC 6455
    /// Close-frame reason budget.
    #[test]
    #[should_panic(expected = "build_id")]
    fn over_long_build_id_panics() {
        assert_build_id_valid(&"x".repeat(65));
    }
}
