//! Startup composition root: the wiring that assembles a running server out of
//! `brenn-server`'s router, routes, state and registries, plus the CLI surface
//! the binary parses into.
//!
//! It sits above `brenn-server` so that an edit to the wiring — or to the tests
//! that boot the wired system — does not re-run the crate below it.

mod apps;
mod automation;
mod cc_profile;
mod cleanup;
pub mod cli;
mod config_check;
mod config_diff;
mod mqtt;
mod obs_config;
mod pid_file;
mod pwa_push;
mod repo_sync;
mod shutdown;
#[cfg(test)]
mod surface_boot_harness;
#[cfg(test)]
mod surface_ws_tests;
#[cfg(test)]
mod wasm_dispatch_e2e_tests;
mod wasm_mqtt;
mod webhook;

use std::path::PathBuf;

use brenn_lib::config::{BrennConfig, ResolvedConfig, validate_and_resolve};
use brenn_lib::integration::IntegrationRegistry;
use brenn_obs as obs;
use tokio::net::TcpListener;
use tracing::info;

use brenn_server::state::AppState;

pub use config_check::run_config_check;
pub use config_diff::run_config_diff;

pub async fn run_invite(config: &BrennConfig) {
    let db = brenn_server::db::init_db(&config.database.path);
    let conn = db.lock().await;
    let code = brenn_db::auth::invite::create_invite_code(&conn);
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

/// What a server refuses to start on, before any of it is used.
///
/// The components roots are checked here rather than where a consumer resolves
/// against them, so a flag pointed at nothing is an operator error reported at
/// startup instead of one that hides until some later release happens to
/// configure a consumer. The same goes for a package name installed under two
/// roots: a broken install is refused whether or not this configuration
/// instantiates the name.
///
/// TODO(mcp-script-path-precondition): `claude_defaults.mcp_script_path` is not
/// among the artifact facts checked here, so a configuration naming a file that
/// does not exist boots and fails at the first session spawn instead.
fn assert_boot_preconditions(build_id: &str, roots: &cli::InstallRoots) {
    assert_build_id_valid(build_id);
    for root in &roots.components {
        brenn_lib::wasm_package::assert_components_root(root);
    }
    brenn_lib::wasm_package::assert_disjoint_components_roots(&roots.components);
}

/// The environment-free half of a `[[wasm_consumer]]`'s `ProcessorLoadSpec`:
/// everything derivable from the resolved consumer alone. The remaining fields
/// (alerter, store path, MQTT egress callback, tool host) depend on process-wide
/// services and stay at the call site.
pub(crate) struct ConsumerLoadParts {
    pub output_ports: std::collections::HashMap<String, brenn_wasm::OutputPortSpec>,
    pub declared_out_ports: std::collections::BTreeSet<String>,
    pub input_amplification_mt: std::collections::HashMap<String, u64>,
    pub mqtt_sinks: std::collections::HashMap<String, brenn_wasm::SinkBudget>,
    pub grants: std::collections::BTreeSet<brenn_wasm::ComponentGrant>,
    pub output_acl: brenn_wasm::OutputAclFn,
}

/// Lower a resolved consumer to the load-spec fields it fully determines.
pub(crate) fn lower_consumer_load_parts(
    consumer: &brenn_lib::messaging::config::ResolvedWasmConsumer,
) -> ConsumerLoadParts {
    use brenn_lib::messaging::ComponentGrant;
    use brenn_lib::messaging::Urgency;
    use brenn_wasm::ProcessorUrgency;
    use std::collections::{BTreeSet, HashMap};

    let output_ports: HashMap<String, brenn_wasm::OutputPortSpec> = consumer
        .outputs
        .iter()
        .map(|o| {
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
    // Windows are built from the same `inputs`, so every driven window port
    // is present.
    let input_amplification_mt: HashMap<String, u64> = consumer
        .inputs
        .iter()
        .map(|i| (i.port.clone(), i.amplification_mt))
        .collect();
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
    // `takeover` names no interface and cannot reach a top-level consumer.
    // Asserted here because a hand-built config reaches this loader without
    // passing through the config front end's refusal.
    let grants: BTreeSet<ComponentGrant> = consumer
        .grants
        .iter()
        .inspect(|g| {
            assert!(
                g.wit_import().is_some(),
                "consumer {:?}: granted `{}`, which names no WIT interface and belongs to a \
                 page — a top-level consumer has no page, and the config front end refuses \
                 the word",
                consumer.slug,
                g.word(),
            )
        })
        .copied()
        .collect();
    // The word and the statements it consents to are one configuration, refused
    // at derive in either direction. Asserted again here because a hand-built
    // config reaches this loader without passing through that refusal.
    assert_eq!(
        grants.contains(&ComponentGrant::Tools),
        !consumer.policy.tool_grants.is_empty(),
        "consumer {:?}: `tools` is granted iff the consumer names a tool — a grant with \
         no tool reaches nothing, and a tool with no grant is authority nobody gave",
        consumer.slug,
    );
    // `brenn-wasm` never sees a brenn-lib type; this closure bridges the two.
    let policy = consumer.policy.clone();
    let output_acl: brenn_wasm::OutputAclFn = std::sync::Arc::new(move |addr: &str| {
        match brenn_lib::messaging::ChannelScheme::split(addr) {
            Some((scheme, name)) => {
                brenn_lib::messaging::gates::publish_acl_allows(&policy, scheme, name)
            }
            None => false,
        }
    });

    ConsumerLoadParts {
        output_ports,
        declared_out_ports: consumer.declared_out_ports.clone(),
        input_amplification_mt,
        mqtt_sinks,
        grants,
        output_acl,
    }
}

pub async fn run_server(
    config: BrennConfig,
    config_path: Option<PathBuf>,
    install_roots: cli::InstallRoots,
    build_id: &'static str,
) {
    assert_boot_preconditions(build_id, &install_roots);
    let components_roots = install_roots.components;

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
        system_channel_tuning,
        claude_profiles,
    } = validate_and_resolve(&config, &integration_registry, runtime_dir.as_deref());

    // A token is opaque: Brenn cannot read its lifetime, so the only expiry
    // signal there is is the date the operator wrote down. One alert per
    // profile that is past it or close to it, and nothing else reads the date.
    {
        let today = chrono::Local::now().date_naive();
        for (name, warning) in brenn_lib::config::expiry_alerts(&claude_profiles, today) {
            tracing::warn!(profile = %name, "{warning}");
            guard.alert_dispatcher.alert(
                brenn_obs::alerting::AlertSeverity::Warning,
                "Claude profile token expiring".to_string(),
                warning,
            );
        }
    }

    // Auto-clone repos into the directories created above. Runs after
    // validation so we have ContainerSpawnConfig for container-side clones
    // (SSH keys live inside the container's persistent home). Clones run
    // concurrently via join_all in auto_clone_repos.
    if config.repo_sync.repo_dir.is_some() {
        brenn_git::repo_clone::auto_clone_repos(&config, &apps, &guard.alert_dispatcher).await;
    }

    apps::prepare_and_validate(&apps);

    // Synchronous before serving: apps that configure startup_hooks
    // require their data to be fresh before accepting traffic.
    apps::run_startup_hooks(&apps, &guard.alert_dispatcher).await;

    // Virtual tools files are written after the tool registry is built (below),
    // because `registry_virtual_tools` projects each app's granted registry
    // tools from their descriptors.

    if apps.values().any(|a| a.container_spawn.is_some()) {
        cleanup::cleanup_stale_containers().await;
    }

    let db = brenn_server::db::init_db(&config.database.path);

    // Close any usage sessions that were open when the server last shut down.
    // This must complete before the server starts accepting requests (so that
    // new sessions are attributed correctly), but the prune is not boot-critical
    // and is spawned separately to avoid blocking startup behind the DB lock.
    {
        let conn = db.lock().await;
        let closed = brenn_usage_db::close_open_sessions_on_startup(&conn);
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
            brenn_usage_db::prune_usage_before(&conn, prune_before);
        });
    }

    let pending_uploads: brenn_server::state::PendingUploads = Default::default();
    // Build the tool registry: built-in tools + integration tools.
    let integration_tools = integration_registry.collect_tools();
    let tool_registry = brenn_render::tools::build_tool_registry(integration_tools);

    // Propagate the drain-time repo_sync staleness cap to the library
    // constant read by `drain_pending_events`. Must be set *before* any
    // bridge spawns; no bridges exist this early in startup.
    brenn_messaging::set_repo_sync_staleness_days(config.repo_sync.stale_conversation_days);

    // Validate at startup for a clear panic before any task is spawned.
    // The guard also fires inside event_cleanup_loop itself so it remains
    // enforced regardless of call site.
    brenn_messaging::assert_delivered_retention_days_valid(config.events.delivered_retention_days);

    let active_bridges = brenn_server::active_bridge::ActiveBridges::new();

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
        brenn_messaging_store::db::assert_senders_structured(&conn);
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
    let any_messaging = brenn_messaging_boot::messaging_configured(
        &config,
        &webhook_endpoints,
        &mqtt_ingress_channels,
    )
        // build_pwa_push also consumes server_origin; these two terms force
        // resolution for it even when build_messaging itself early-returns.
        || apps.values().any(|a| a.messaging_enabled())
        || resolved_pwa_push.is_some();
    let messaging_server_origin: Option<std::sync::Arc<str>> = if any_messaging {
        Some(brenn_messaging::resolve_source(&config.server))
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
    let tool_registry_core: std::sync::Arc<brenn_tool_registry::ToolRegistry> = {
        let git_repo_pull = brenn_tool_registry::GitRepoPullTool::new(
            repo_sync_result.clones.clone(),
            repo_sync_result.remote_locks.clone(),
            repo_sync_result.sender.clone(),
        );
        let registry = brenn_tool_registry::ToolRegistry::new(vec![
            brenn_tool_registry::RegisteredTool::Async(std::sync::Arc::new(git_repo_pull)),
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
    let mut messaging_result = brenn_messaging_boot::build_messaging(
        &config,
        db.clone(),
        &apps,
        active_bridges.clone(),
        guard.alert_dispatcher.clone(),
        messaging_server_origin.clone(),
        &webhook_endpoints,
        &mqtt_ingress_channels,
        &system_channel_tuning,
        &mqtt_clients,
        &tool_registry_core,
    )
    .await;

    // Boot re-activation of durable dynamic `mqtt:` subscriptions: the
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

    // Observability: log the boot-resolved surfaces and non-durable
    // channels. Skipped when empty so a config with no `[[surface]]` /
    // non-durable `[[channel]]` blocks emits no new log line (upholding the
    // bit-for-bit-unchanged guarantee).
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
    if !messaging_result.nondurable_channels.is_empty() {
        let addresses: Vec<&str> = messaging_result
            .nondurable_channels
            .iter()
            .map(|c| c.address.as_str())
            .collect();
        tracing::info!(
            count = messaging_result.nondurable_channels.len(),
            channels = ?addresses,
            "boot: resolved non-durable [[channel]] blocks",
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
    brenn_surface_server::validate_surface_error_channel(
        config.observability.surface_error_channel.as_deref(),
        messenger.map(|m| &**m.directory()),
        config.messaging.max_body_bytes,
    );

    // Validate the derived self-description channel set under the same directory
    // + resolved surfaces (single-writer, brenn:, declared,
    // standing_retain_depth >= 1). Every failure is a boot panic.
    brenn_surface_server::description::validate_surface_description(
        &config.surface_description,
        &messaging_result.surfaces,
        messenger.map(|m| &**m.directory()),
        brenn_surface_server::SingleWriterPrincipals {
            app_policies: &app_policies,
            wasm_consumers: &messaging_result.wasm_consumers,
            surfaces: &messaging_result.surfaces,
            system_participants: &messaging_result.system_participants,
        },
    );

    // Unconditional: a broken install is refused independently of what today's
    // config touches, and the kind → root map it returns is what the router
    // serves from.
    let surface_roots = brenn_surface_server::validate_surface_assets(
        &install_roots.surface,
        &messaging_result.surfaces,
    );

    // Publish surface self-description documents so any app can pull them via
    // `MessageChannelGet`.
    if let Some(messenger) = messenger {
        let prefix = &config.surface_description.prefix;
        let docs = brenn_surface_server::description::build_description_docs(
            prefix,
            build_id,
            &messaging_result.surfaces,
            &surface_roots,
        );
        brenn_surface_server::description::publish_description(messenger, &docs).await;

        // Bindings documents: one retained document per surface on its own
        // ephemeral config channel, published here — before the server accepts
        // connections — so an attaching surface always finds a retained copy and
        // an empty replay is a server invariant failure rather than a race.
        let bindings_docs = brenn_surface_server::bindings_doc::build_bindings_documents(
            &messaging_result.surfaces,
            &brenn_surface_server::bindings_doc::BindingsDocParams {
                prefix,
                status_interval_secs: config.surface_description.status_interval_secs,
                error_report: config
                    .observability
                    .surface_error_channel
                    .as_deref()
                    .map(|addr| (addr, config.observability.surface_error_publish_floor)),
            },
        );
        brenn_surface_server::bindings_doc::publish_bindings_documents(messenger, &bindings_docs)
            .await;

        // Boot disconnected stamps: after the boot-published docs, write a
        // `disconnected` status snapshot (reason "server restart", the new
        // non-durable incarnation epoch, empty instances) per configured surface. A durable status
        // channel's retained row survives the restart; without this a dead or
        // not-yet-connected wall would read "healthy as of before the restart"
        // until a reader did timestamp math.
        let epoch = messenger.ring_epoch();
        brenn_surface_server::telemetry::publish_boot_disconnected_stamps(
            messenger,
            prefix,
            &messaging_result.surfaces,
            epoch,
        )
        .await;
    }

    // Build the per-surface runtime bundle map, keyed by slug. Any non-empty
    // `[[surface]]` list forces messaging on (`any_messaging` above), so a
    // `Messenger` exists whenever surfaces do; the `expect` cites that gate.
    let surface_runtimes = {
        let surfaces = std::mem::take(&mut messaging_result.surfaces);
        if surfaces.is_empty() {
            std::collections::HashMap::new()
        } else {
            let messenger = messaging_result.messenger.as_ref().expect(
                "[[surface]] blocks configured but no Messenger: the any_messaging gate \
                 forces messaging on whenever surfaces exist",
            );
            let error_channel = config.observability.surface_error_channel.clone();
            brenn_surface_server::build_surface_runtimes(
                surfaces,
                Some(messenger.clone()),
                config.messaging.max_body_bytes,
                error_channel,
                brenn_surface_server::SurfaceDescriptionParams {
                    prefix: config.surface_description.prefix.clone(),
                },
            )
        }
    };

    let remote_runtimes = brenn_remote_server::build_remote_runtimes(
        &messaging_result.remotes,
        messaging_result.messenger.as_ref(),
        config.messaging.max_body_bytes,
    );

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
    // WebhookService. Panics on failure — a boot that cannot load a declared
    // component must not serve.
    let (replay_components, replay_locks) = {
        use std::collections::HashMap;
        use std::sync::Arc;
        let mut components = HashMap::new();
        let mut locks = HashMap::new();
        if let Some(ref svc) = webhook_result.service {
            for ep in svc.all_endpoints() {
                if let Some(ref rp) = ep.replay_protection {
                    let component = load_verified_replay(
                        &ep.slug,
                        &components_roots,
                        &rp.component,
                        &rp.store_path,
                        rp.max_page_count,
                        rp.config.clone(),
                    );
                    components.insert(ep.slug.clone(), Arc::new(component));
                    locks.insert(ep.slug.clone(), Arc::new(tokio::sync::Mutex::new(())));
                    info!(
                        endpoint = %ep.slug,
                        component = %rp.component,
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
    // rather than that generic one.
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
    // registers it on the WakeRouter for the off-loop dispatch task.
    let processing_components = {
        use std::collections::HashMap;
        use std::sync::Arc;
        let mut components: HashMap<String, Arc<brenn_wasm::ProcessorComponent>> = HashMap::new();
        for consumer in &messaging_result.wasm_consumers {
            let ConsumerLoadParts {
                output_ports,
                declared_out_ports,
                input_amplification_mt,
                mqtt_sinks,
                grants,
                output_acl,
            } = lower_consumer_load_parts(consumer);
            let has_tool_grants = !consumer.policy.tool_grants.is_empty();
            let alerter = std::sync::Arc::new(brenn_wasm_dispatch::DispatcherAlerter::new(
                guard
                    .alert_dispatcher
                    .clone()
                    .with_field("wasm_slug", &consumer.slug),
                consumer.slug.clone(),
            ));
            // Synchronous MQTT egress callback. Built iff the consumer holds the
            // `Mqtt` grant — the `mqtt` interface is linked iff this is `Some`, and
            // `ProcessorComponent::load` re-asserts that invariant. The constructor
            // captures the resolved policy (the same one the LLM path uses), the
            // consumer slug as the connector-namespace key, and the (optional) MQTT
            // service.
            let mqtt_publish: Option<brenn_wasm::MqttPublishFn> = if consumer
                .grants
                .contains(&brenn_lib::messaging::ComponentGrant::Mqtt)
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
                Some(std::sync::Arc::new(brenn_tool_registry::WasmToolHost::new(
                    tool_registry_core.clone(),
                    consumer.policy.tool_grants.clone(),
                    consumer.slug.clone(),
                    guard.alert_dispatcher.clone(),
                )))
            } else {
                None
            };
            let (component, artifact_path) = load_verified_consumer(
                &components_roots,
                &consumer.package,
                &consumer.slug,
                &consumer.spec_sha256,
                brenn_wasm::ProcessorLoadSpec {
                    // Placeholder; the callee owns this field.
                    component_path: std::path::Path::new(""),
                    slug: &consumer.slug,
                    output_ports,
                    declared_out_ports,
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
                },
            );
            let store_path_present = consumer.store_path.is_some();
            info!(
                slug = %consumer.slug,
                package = %consumer.package,
                component_path = %artifact_path.display(),
                store_path_present,
                store_path = consumer.store_path.as_deref().map(|p| p.display().to_string()),
                "WASM processor component loaded"
            );
            components.insert(consumer.slug.clone(), Arc::new(component));
        }
        Arc::new(components)
    };

    // Register per-consumer Notify instances on the WakeRouter as ParkedNotify
    // delivery bindings and retain a clone for each off-loop dispatch task.
    // The router stores one Arc clone; the task gets another. Must happen
    // before set_state so bindings are present when the first WASM push arrives.
    let wasm_notifiers: Vec<(String, std::sync::Arc<tokio::sync::Notify>)> = {
        use brenn_lib::messaging::SubscriberEntryKind;
        use brenn_server::messaging_router::DeliveryBinding;
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
                    brenn_server::messaging_router::DeliveryBinding::ParkedNotify(notify.clone()),
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
        std::sync::Arc<brenn_tool_registry::ToolCallerGrants>,
    )> = system_notifiers
        .iter()
        .find(|(component, _)| *component == brenn_tool_registry::TOOL_EXECUTOR_COMPONENT)
        .map(|(_, notify)| {
            let mut caller_grants: brenn_tool_registry::ToolCallerGrants =
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
    // inline through its conversation bridge, and every surface and remote fans
    // out to its attached sessions. Together with the WASM/system ParkedNotify
    // bindings above, this covers every subscriber the dispatcher can target — a
    // missing binding at dispatch time is a host-wiring invariant violation and
    // panics.
    if let Some(ref router) = messaging_result.router {
        use brenn_lib::messaging::SubscriberEntryKind;
        use brenn_server::messaging_router::DeliveryBinding;
        for slug in apps.keys() {
            router.register_delivery_binding(
                SubscriberEntryKind::App(slug.clone()),
                DeliveryBinding::ConversationBridge,
            );
        }
        // One binding per surface principal, through the router's own helper —
        // the same call the surface test rigs make, so their wiring cannot drift
        // from this one.
        for runtime in surface_runtimes.values() {
            router.register_surface_delivery_routes(&runtime.resolved);
        }
        // And one per remote. Nothing subscribes on its behalf at boot — its
        // entries are minted by its own subscribes — so the binding is
        // registered from the config, which is the only enumeration of remotes
        // that exists before one attaches.
        for remote in &messaging_result.remotes {
            router.register_remote_delivery_routes(remote);
        }

        let messenger = messaging_result
            .messenger
            .as_ref()
            .expect("messenger is Some whenever the router is Some (both built together)");
        assert_every_subscriber_wired(messenger, router);
    }

    // Claude account profiles. Every profiled agent starts on the first entry of
    // its `claude_profiles`; a goal channel then moves it.
    //
    // The retained goal is learned by a *read*, not by delivery: a system
    // subscriber's position is durable, so after the first boot the retained
    // message is behind the cursor and never arrives as new. And the read
    // happens here, above the `AppState` literal, because everything below —
    // `set_state`, the dispatcher sweep, autonomous wakes — is an eager spawn
    // path that would otherwise bill a turn to the account the agent was on
    // before the last publish. A publish landing between this read and the drain
    // task's first pass is delivered as new and applied then; no spawn is
    // possible inside that window.
    let (cc_profiles, cc_profile_inbox) = {
        let app_profiles: std::collections::BTreeMap<String, brenn_lib::config::AppClaudeProfiles> =
            apps.iter()
                .filter_map(|(slug, app)| app.claude_profiles.clone().map(|p| (slug.clone(), p)))
                .collect();
        if app_profiles.is_empty() {
            (None, None)
        } else {
            let bare_profiled: Vec<String> = apps
                .iter()
                .filter(|(slug, app)| {
                    app.container_spawn.is_none() && app_profiles.contains_key(*slug)
                })
                .map(|(slug, _)| slug.clone())
                .collect();
            brenn_cc_profile::refuse_outranking_server_env(&bare_profiled);
            let goal = std::sync::Arc::new(brenn_cc_profile::ProfileGoal::new(
                claude_profiles,
                app_profiles,
                guard.alert_dispatcher.clone(),
            ));
            let mut inbox = None;
            if !goal.goal_addresses().is_empty() {
                let messenger = messaging_result.messenger.clone().expect(
                    "a claude_profile_goal names a declared durable channel, so messaging is on",
                );
                let notify = system_notifiers
                    .iter()
                    .find(|(component, _)| *component == brenn_cc_profile::CC_PROFILE_COMPONENT)
                    .map(|(_, notify)| notify.clone())
                    .expect(
                        "the cc-profile spec is pushed exactly when a goal channel is named, and \
                         every subscribing spec gets a parked-notify binding",
                    );
                inbox = Some(cc_profile::attach_and_seed(&goal, messenger, notify).await);
            }
            (Some(goal), inbox)
        }
    };

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
        surface_roots: surface_roots.clone(),
        cached_models: Default::default(),
        tool_registry,
        tools: tool_registry_core,
        tool_server_origin,
        wake_locks: Default::default(),
        spawn_backoff: Default::default(),
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
        remotes: std::sync::Arc::new(remote_runtimes),
        attach_registry: brenn_attach_server::registry::AttachRegistry::default(),
        attach_heartbeat_secs: brenn_surface_server::HEARTBEAT_SECS,
        replay_components,
        replay_locks,
        cc_profiles: cc_profiles.clone(),
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

        // Give the tool executor its position on its request channels before
        // anything can put a message into retention. Its drain loop attaches
        // too, but that runs on a spawned task; the dispatcher below releases
        // due parked messages on its first pass, and a request released before
        // the position exists would land below it and never be served.
        brenn_messaging::system::SystemInbox::new(
            brenn_tool_registry::executor::TOOL_EXECUTOR_COMPONENT,
            messenger.clone(),
            std::sync::Arc::new(tokio::sync::Notify::new()),
        )
        .attach()
        .await;

        // Spawn background tasks. Returned JoinHandles are intentionally
        // dropped — these tasks are process-lifetime and never joined.
        // Lifetime-task death is ACCEPTED, not supervised: any panic is logged +
        // Critical-alerted by the global panic hook (brenn-lib/src/obs/panic_hook.rs);
        // alert + manual restart is the decided mitigation. Do NOT add per-task
        // supervision. See TODO.md `task-death-supervision` (tombstone). Same applies
        // to the session/ingress/gc lifetime spawns below.
        drop(brenn_messaging::dispatcher::spawn_dispatcher_task(
            state.db.clone(),
            router.clone() as std::sync::Arc<dyn brenn_messaging::WakeRouter>,
            messenger.dispatch_kick_notify(),
            messenger.clone(),
        ));
        // Kick immediately so pending Immediate/deadline-expired rows trigger
        // eager wakes without waiting for the first POLL_INTERVAL sleep.
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
            let outputs: Vec<brenn_lib::messaging::config::WasmOutputPort> =
                consumer.outputs.clone();
            drop(brenn_wasm_dispatch::spawn_wasm_consumer_task(
                brenn_wasm_dispatch::WasmConsumerConfig {
                    slug: slug.clone(),
                    component: component.clone(),
                    notify,
                    messenger: messenger.clone(),
                    alert_dispatcher: state.alert_dispatcher.clone(),
                    inputs,
                    outputs,
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
                brenn_tool_registry::ToolExecutor::new(
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

        if let Some(inbox) = cc_profile_inbox {
            let goal = cc_profiles
                .clone()
                .expect("the inbox exists only when the profile goal handle does");
            cc_profile::spawn_goal_drain(inbox, goal, state.active_bridges.clone());
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
        brenn_automation::startup::run_startup_consistency_checks(engine).await;
        // Startup catch-up: advance past, and fire, the slots missed while down.
        brenn_automation::loop_task::run_startup_catchup(engine).await;
        // Spawn the scheduler loop. JoinHandle dropped — process-lifetime task.
        drop(brenn_automation::loop_task::spawn_automation_loop(
            engine.clone(),
        ));
        info!("automation engine started; scheduler loop spawned");
    }

    // Load cached model lists from DB so the picker works on first connect.
    {
        let conn = state.db.lock().await;
        let all_models = brenn_db::load_all_app_models(&conn);
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
    tokio::spawn(brenn_server::routes::upload::orphan_cleanup_loop(
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
            messenger.clone(),
            config.messaging.archive_path.clone(),
        ));
    }

    // Spawn the bridge-wedge watchdog: sweeps the live-bridge registry and
    // self-heals a bridge whose event loop died or whose session I/O is dead
    // while the bridge still believes CC is busy.
    brenn_server::active_bridge::spawn_watchdog(
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

    let app = brenn_server::router::build_router(
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

/// Resolve a replay endpoint's package, verify it, then load what it names.
///
/// The three are one function because the order is the point: a component whose
/// package does not bind it must never reach the loader, where it would be
/// instantiated on its own say-so. The resolution step is what makes that
/// structural rather than conventional — the configuration carries a package
/// name and no path, so the only way to obtain an artifact path at all is to
/// have verified the package it came out of.
fn load_verified_replay(
    slug: &str,
    components_roots: &[PathBuf],
    package: &str,
    store_path: &std::path::Path,
    max_page_count: u32,
    config: std::collections::HashMap<String, String>,
) -> brenn_wasm::ReplayComponent {
    let roots = brenn_lib::wasm_package::require_components_root(
        components_roots,
        &format!("webhook endpoint {slug:?} replay protection"),
    );
    let component_path = brenn_lib::wasm_package::verify_replay(roots, package, slug);
    brenn_wasm::ReplayComponent::load(slug, &component_path, store_path, max_page_count, config)
}

/// Resolve a consumer's package, verify it, then load what it names.
///
/// Same ordering rule as [`load_verified_replay`], and one function for the
/// same reason: the resolution is the only source of an artifact path, so a
/// component whose package does not bind it cannot reach the loader. The
/// caller assembles the dozen wired fields of the load spec but never its
/// `component_path` — whatever it staged there is discarded for the artifact
/// the package bound. The artifact's path is handed back for the caller's log.
fn load_verified_consumer(
    components_roots: &[PathBuf],
    package: &str,
    slug: &str,
    config_spec_sha256: &str,
    spec: brenn_wasm::ProcessorLoadSpec<'_>,
) -> (brenn_wasm::ProcessorComponent, std::path::PathBuf) {
    let roots = brenn_lib::wasm_package::require_components_root(
        components_roots,
        &format!("consumer {slug:?}"),
    );
    let artifact =
        brenn_lib::wasm_package::verify_consumer(roots, package, slug, config_spec_sha256);
    // The spec's borrows outlive this call; narrowing them to the artifact's
    // scope is what lets the path the verification produced be the one loaded.
    let mut spec: brenn_wasm::ProcessorLoadSpec<'_> = spec;
    spec.component_path = &artifact;
    let component = brenn_wasm::ProcessorComponent::load(spec);
    (component, artifact)
}

/// Assert that all replay store paths and consumer store paths are unique across
/// both sets.
///
/// `replay_paths`: canonical store paths from `[[webhook_endpoint]].replay_protection`.
/// `consumer_paths`: `(store_path, slug)` pairs from `[[wasm_consumer]]`.
///
/// Panics on the first duplicate with a human-readable message.
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
                 across all replay and consumer stores",
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
                 replay and consumer stores",
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
/// at dispatch. Named boot failure beats both. A third check pairs the two: a
/// parked subscriber must declare eager wake economics — its wake source
/// cannot honour urgency gating.
fn assert_every_subscriber_wired(
    messenger: &brenn_messaging::Messenger,
    router: &brenn_server::messaging_router::WakeRouterImpl,
) {
    use brenn_lib::messaging::WakeEconomics;
    use brenn_messaging::{DeliveryShape, WakeRouter};

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
            // A parked subscriber's wake source cannot honour a per-message
            // `wake_min` threshold. Every claim an Eager subscriber holds is
            // eager by construction, so the two are compatible. UrgencyGated
            // would silently wake for messages its threshold says to hold.
            if router.delivery_shape(&sub.kind) == DeliveryShape::ParkedWake {
                assert_eq!(
                    messenger.subscriber_wake_economics(&sub.kind),
                    Some(WakeEconomics::Eager),
                    "boot cross-check: parked-and-woken subscriber {:?} on channel {:?} declares \
                     urgency-gated wake economics, which its wake source cannot honour",
                    sub.kind,
                    channel.address,
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The verify-then-load pair, over a package on disk.
    ///
    /// The unit checks live in `brenn-lib`; what is pinned here is that the boot
    /// path runs them, and runs them *first*. Each case hands the loader an
    /// artifact it would reject on its own terms, so a panic naming the package
    /// is proof the verification happened before the load rather than instead of
    /// it — and a refactor that drops or reorders the call fails here instead of
    /// shipping a host that loads unbound components.
    mod verified_load {
        use std::path::{Path, PathBuf};

        struct NoopAlerter;

        impl brenn_wasm::ProcessorAlerter for NoopAlerter {
            fn alert(&self, _severity: brenn_wasm::GuestAlertSeverity, _title: &str, _body: &str) {}
        }

        /// A components root holding one package directory, with a record
        /// binding whatever `artifact_bytes` and `spec` are given here. The
        /// record is written by hand — not by the emitter — because these cases
        /// need records the emitter refuses to write.
        fn package(root: &Path, name: &str, artifact_bytes: &[u8], spec: Option<&str>) -> PathBuf {
            let dir = root.join(name);
            std::fs::create_dir_all(&dir).expect("create package dir");
            let artifact = dir.join(format!("{name}.wasm"));
            std::fs::write(&artifact, artifact_bytes).expect("write artifact");
            let artifact_sha = brenn_lib::util::sha256_hex(artifact_bytes);
            let record = match spec {
                Some(text) => {
                    std::fs::write(dir.join(format!("{name}.brenn")), text).expect("write spec");
                    format!(
                        "{{\n  \"v\": 2,\n  \"name\": \"{name}\",\n  \"world\": \
                         \"brenn:processor\",\n  \"artifact\": \"{name}.wasm\",\n  \
                         \"artifact_sha256\": \"{artifact_sha}\",\n  \"spec\": \
                         \"{name}.brenn\",\n  \"spec_sha256\": \"{}\"\n}}\n",
                        brenn_lib::util::sha256_hex(text.as_bytes()),
                    )
                }
                None => format!(
                    "{{\n  \"v\": 2,\n  \"name\": \"{name}\",\n  \"world\": \
                     \"brenn:replay\",\n  \"artifact\": \"{name}.wasm\",\n  \
                     \"artifact_sha256\": \"{artifact_sha}\"\n}}\n",
                ),
            };
            std::fs::write(dir.join("package.json"), record).expect("write record");
            artifact
        }

        fn load_spec(slug: &str) -> brenn_wasm::ProcessorLoadSpec<'_> {
            brenn_wasm::ProcessorLoadSpec {
                // Placeholder; the callee owns this field.
                component_path: Path::new(""),
                slug,
                output_ports: Default::default(),
                declared_out_ports: Default::default(),
                input_amplification_mt: Default::default(),
                mqtt_sinks: Default::default(),
                config: Default::default(),
                grants: Default::default(),
                store_path: None,
                max_page_count: 1,
                max_payload_bytes: 1024,
                alerter: std::sync::Arc::new(NoopAlerter),
                output_acl: std::sync::Arc::new(|_| true),
                mqtt_publish: None,
                tool_host: None,
            }
        }

        /// Resolve, verify, and load a consumer's package by calling the boot
        /// path itself, so these cases cover the sequence `run_server` runs
        /// rather than a copy of it.
        ///
        /// `root` is an `Option` because a host started without `--components`
        /// has no root to resolve against, and the refusal that fact earns is
        /// part of the path under test.
        fn verify_then_load(
            root: Option<&Path>,
            package: &str,
            slug: &str,
            config_spec_sha256: &str,
            fill: impl FnOnce(&mut brenn_wasm::ProcessorLoadSpec<'_>),
        ) -> brenn_wasm::ProcessorComponent {
            let mut spec = load_spec(slug);
            fill(&mut spec);
            let roots: Vec<PathBuf> = root.map(Path::to_path_buf).into_iter().collect();
            super::super::load_verified_consumer(&roots, package, slug, config_spec_sha256, spec).0
        }

        const SPEC: &str = "component Demo {\n  abi = processor;\n}\n";

        /// A real built component's bytes, by artifact basename.
        ///
        /// The refusal cases below hand the loader bytes it rejects, which is
        /// what makes them proof of ordering; the two acceptance cases need the
        /// opposite — an artifact that loads — or a false refusal on a valid
        /// package would first be seen on a deploy target.
        fn fixture_bytes(basename: &str) -> Vec<u8> {
            let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../brenn-wasm/target/components")
                .join(format!("{basename}.wasm"));
            std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
        }

        #[test]
        fn a_consumer_whose_package_binds_it_loads() {
            let dir = tempfile::tempdir().expect("tempdir");
            package(
                dir.path(),
                "demo",
                &fixture_bytes("brenn_processor_demo"),
                Some(SPEC),
            );
            // Returning at all is the assertion: verification passed and the
            // loader instantiated the artifact behind it. Both would panic.
            let component = verify_then_load(
                Some(dir.path()),
                "demo",
                "demo",
                &brenn_lib::util::sha256_hex(SPEC.as_bytes()),
                // processor-demo imports `ports`; the load is a real one, so
                // the grant it needs is the real one too.
                |spec| spec.grants = [brenn_wasm::ComponentGrant::Ports].into_iter().collect(),
            );
            drop(component);
        }

        #[test]
        fn a_replay_endpoint_whose_package_binds_it_loads() {
            let dir = tempfile::tempdir().expect("tempdir");
            package(dir.path(), "replay", &fixture_bytes("brenn_replay"), None);
            // A real page budget, unlike the refusal cases below: this load
            // reaches the KV store, which cannot lay out its schema in one
            // page.
            let component = super::super::load_verified_replay(
                "endpoint",
                &[dir.path().to_path_buf()],
                "replay",
                &dir.path().join("replay.sqlite"),
                brenn_wasm::store::DEFAULT_MAX_PAGE_COUNT,
                Default::default(),
            );
            drop(component);
        }

        #[test]
        #[should_panic(expected = "was configured against a specification that hashes to")]
        fn a_consumer_configured_against_no_specification_at_all_never_reaches_the_loader() {
            // The empty hash is what a lowering that stopped filling the field
            // would produce, and every hand-built fixture in the tree defaults
            // it to one. It must match nothing rather than match everything.
            let dir = tempfile::tempdir().expect("tempdir");
            package(dir.path(), "demo", b"not a component", Some(SPEC));
            verify_then_load(Some(dir.path()), "demo", "demo", "", |_| {});
        }

        #[test]
        #[should_panic(expected = "but its package record binds")]
        fn a_consumer_artifact_its_record_does_not_bind_never_reaches_the_loader() {
            let dir = tempfile::tempdir().expect("tempdir");
            let artifact = package(dir.path(), "demo", b"not a component", Some(SPEC));
            std::fs::write(&artifact, b"tampered").expect("tamper");
            verify_then_load(
                Some(dir.path()),
                "demo",
                "demo",
                &brenn_lib::util::sha256_hex(SPEC.as_bytes()),
                |_| {},
            );
        }

        #[test]
        #[should_panic(expected = "was configured against a specification that hashes to")]
        fn a_consumer_whose_config_spec_is_not_the_packaged_one_never_reaches_the_loader() {
            let dir = tempfile::tempdir().expect("tempdir");
            package(dir.path(), "demo", b"not a component", Some(SPEC));
            verify_then_load(
                Some(dir.path()),
                "demo",
                "demo",
                &brenn_lib::util::sha256_hex(b"component Demo {}\n"),
                |_| {},
            );
        }

        #[test]
        #[should_panic(expected = "has no readable record")]
        fn a_consumer_with_no_record_never_reaches_the_loader() {
            let dir = tempfile::tempdir().expect("tempdir");
            std::fs::create_dir(dir.path().join("demo")).expect("create package dir");
            std::fs::write(dir.path().join("demo/demo.wasm"), b"not a component")
                .expect("write artifact");
            verify_then_load(
                Some(dir.path()),
                "demo",
                "demo",
                &brenn_lib::util::sha256_hex(SPEC.as_bytes()),
                |_| {},
            );
        }

        #[test]
        #[should_panic(expected = "is not an installed package directory")]
        fn a_consumer_naming_a_package_that_is_not_installed_never_reaches_the_loader() {
            let dir = tempfile::tempdir().expect("tempdir");
            package(dir.path(), "demo", b"not a component", Some(SPEC));
            verify_then_load(
                Some(dir.path()),
                "panel",
                "demo",
                &brenn_lib::util::sha256_hex(SPEC.as_bytes()),
                |_| {},
            );
        }

        #[test]
        #[should_panic(expected = "without --components")]
        fn a_consumer_configured_on_a_host_started_without_the_flag_never_reaches_the_loader() {
            verify_then_load(
                None,
                "demo",
                "demo",
                &brenn_lib::util::sha256_hex(SPEC.as_bytes()),
                |_| {},
            );
        }

        #[test]
        #[should_panic(expected = "but its package record binds")]
        fn a_replay_artifact_its_record_does_not_bind_never_reaches_the_loader() {
            let dir = tempfile::tempdir().expect("tempdir");
            let artifact = package(dir.path(), "replay", b"not a component", None);
            std::fs::write(&artifact, b"tampered").expect("tamper");
            super::super::load_verified_replay(
                "endpoint",
                &[dir.path().to_path_buf()],
                "replay",
                &dir.path().join("replay.sqlite"),
                1,
                Default::default(),
            );
        }

        #[test]
        #[should_panic(expected = "declares world")]
        fn a_replay_endpoint_handed_a_processor_package_never_reaches_the_loader() {
            let dir = tempfile::tempdir().expect("tempdir");
            package(dir.path(), "replay", b"not a component", Some(SPEC));
            super::super::load_verified_replay(
                "endpoint",
                &[dir.path().to_path_buf()],
                "replay",
                &dir.path().join("replay.sqlite"),
                1,
                Default::default(),
            );
        }
    }

    /// A resolved consumer with the given grant words and tool names, all
    /// other fields defaulted.
    fn tool_consumer(
        grants: &[brenn_lib::messaging::ComponentGrant],
        tools: &[&str],
    ) -> brenn_lib::messaging::config::ResolvedWasmConsumer {
        let mut policy = brenn_lib::access::AppPolicy::default();
        for tool in tools {
            policy.tool_grants.insert(
                (*tool).to_string(),
                brenn_lib::tools::ResolvedToolGrant {
                    acl: Vec::new(),
                    rate_limit: None,
                },
            );
        }
        brenn_lib::messaging::config::ResolvedWasmConsumer {
            slug: "tooler".to_string(),
            package: "tooler".to_string(),
            spec_sha256: String::new(),
            declared_out_ports: std::collections::BTreeSet::new(),
            grants: grants.iter().copied().collect(),
            store_path: None,
            max_page_count: 1,
            inputs: Vec::new(),
            outputs: Vec::new(),
            config: std::collections::HashMap::new(),
            policy,
            activation_pacing: brenn_lib::messaging::config::ActivationPacing {
                burst: 1,
                min_period: std::time::Duration::from_millis(1),
            },
            mqtt_sinks: std::collections::HashMap::new(),
        }
    }

    #[test]
    fn a_granted_tools_word_with_a_named_tool_lowers_to_the_capability() {
        let parts = lower_consumer_load_parts(&tool_consumer(
            &[
                brenn_lib::messaging::ComponentGrant::Ports,
                brenn_lib::messaging::ComponentGrant::Tools,
            ],
            &["git-repo-pull"],
        ));
        assert!(parts.grants.contains(&brenn_wasm::ComponentGrant::Tools));
    }

    #[test]
    #[should_panic(expected = "granted iff the consumer names a tool")]
    fn a_tools_word_with_no_tool_statement_panics() {
        lower_consumer_load_parts(&tool_consumer(
            &[brenn_lib::messaging::ComponentGrant::Tools],
            &[],
        ));
    }

    #[test]
    #[should_panic(expected = "granted iff the consumer names a tool")]
    fn a_tool_statement_with_no_tools_word_panics() {
        lower_consumer_load_parts(&tool_consumer(
            &[brenn_lib::messaging::ComponentGrant::Ports],
            &["git-repo-pull"],
        ));
    }

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
    /// clear message.
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

    fn components_only<const N: usize>(components: [PathBuf; N]) -> cli::InstallRoots {
        cli::InstallRoots {
            components: components.into(),
            ..cli::InstallRoots::default()
        }
    }

    /// A valid build id (non-empty, at most 64 chars) must not panic. The
    /// 64-char boundary is inclusive.
    #[test]
    fn valid_build_id_no_panic() {
        assert_build_id_valid("test-build");
        assert_build_id_valid(&"x".repeat(64));
    }

    #[test]
    fn boot_preconditions_pass_on_a_real_components_root() {
        let dir = tempfile::tempdir().unwrap();
        assert_boot_preconditions("test-build", &components_only([dir.path().to_path_buf()]));
        assert_boot_preconditions("test-build", &cli::InstallRoots::default());
    }

    /// The root is checked at startup, with no consumer configured and nothing
    /// yet resolved against it.
    #[test]
    #[should_panic(expected = "which is not a directory")]
    fn boot_preconditions_refuse_a_components_root_that_is_not_a_directory() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("brenn_demo.wasm");
        std::fs::write(&file, b"not a directory").unwrap();
        assert_boot_preconditions("test-build", &components_only([file]));
    }

    /// Two releases' roots with disjoint package names boot; the same name
    /// under both is refused before any consumer resolves it.
    #[test]
    fn boot_preconditions_hold_the_components_roots_disjoint() {
        let brenn = tempfile::tempdir().unwrap();
        let bundle = tempfile::tempdir().unwrap();
        std::fs::create_dir(brenn.path().join("demo")).unwrap();
        std::fs::create_dir(bundle.path().join("relay")).unwrap();
        let roots = components_only([brenn.path().to_path_buf(), bundle.path().to_path_buf()]);
        assert_boot_preconditions("test-build", &roots);
    }

    #[test]
    #[should_panic(expected = "is installed under more than one --components root")]
    fn boot_preconditions_refuse_a_package_present_in_two_roots() {
        let brenn = tempfile::tempdir().unwrap();
        let bundle = tempfile::tempdir().unwrap();
        std::fs::create_dir(brenn.path().join("demo")).unwrap();
        std::fs::create_dir(bundle.path().join("demo")).unwrap();
        let roots = components_only([brenn.path().to_path_buf(), bundle.path().to_path_buf()]);
        assert_boot_preconditions("test-build", &roots);
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
        use brenn_lib::messaging::config::{Depth, MessagingGlobalConfig, NoiseLevel};
        use brenn_lib::messaging::{
            MessagingDirectory, SubscriberEntry, SubscriberEntryKind, SubscriberRegistration,
        };
        use brenn_messaging::query::NoopWakeRouter;
        use brenn_messaging::testutils::{
            remote_registrations, test_channel_entry, wasm_registrations,
        };
        use brenn_messaging::{Messenger, WakeRouter};
        use brenn_server::test_support::init_db_memory;
        use indexmap::IndexMap;

        use brenn_server::active_bridge::ActiveBridges;
        use brenn_server::messaging_router::{DeliveryBinding, WakeRouterImpl};

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

        /// A parked-and-woken subscriber registered as urgency-gated fails the
        /// cross-check: its wake source cannot honour a `wake_min` threshold.
        #[test]
        #[should_panic(expected = "urgency-gated wake economics")]
        fn urgency_gated_parked_subscriber_panics() {
            let regs = HashMap::from([(
                wasm_key(),
                SubscriberRegistration {
                    policy: Arc::new(AppPolicy::default()),
                    wake: brenn_lib::messaging::WakeEconomics::UrgencyGated,
                },
            )]);
            let messenger = messenger(regs);
            let router = WakeRouterImpl::new(ActiveBridges::new());
            router.register_delivery_binding(
                wasm_key(),
                DeliveryBinding::ParkedNotify(Arc::new(tokio::sync::Notify::new())),
            );
            assert_every_subscriber_wired(&messenger, &router);
        }

        const REMOTE_SLUG: &str = "pod-kitchen";

        fn remote_key() -> SubscriberEntryKind {
            SubscriberEntryKind::Remote(REMOTE_SLUG.to_string())
        }

        /// A `Messenger` whose directory holds one channel with a
        /// **runtime-minted** `Remote` subscriber — the shape a remote's
        /// subscribe leaves behind, which no boot walk over config could have
        /// produced.
        fn remote_messenger(registered: bool) -> Arc<Messenger> {
            let sub = SubscriberEntry {
                kind: remote_key(),
                push_depth: Depth::Bounded(8),
                retain_depth: Depth::Bounded(64),
                noise: NoiseLevel::Silent,
                wake_min: None,
            };
            let entry = test_channel_entry("chat.app.home.out.42", vec![sub]);
            let regs = if registered {
                remote_registrations(HashMap::from([(
                    REMOTE_SLUG.to_string(),
                    AppPolicy::default(),
                )]))
            } else {
                HashMap::new()
            };
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

        /// A runtime-minted remote entry — one a subscribe created after boot —
        /// is judged by the same registrations config put there for it: the
        /// cross-check walks the *directory*, so an entry that appears later
        /// meets the same bar.
        #[test]
        fn a_runtime_minted_remote_entry_passes_when_its_config_wiring_ran() {
            let messenger = remote_messenger(true);
            let router = WakeRouterImpl::new(ActiveBridges::new());
            router.register_delivery_binding(remote_key(), DeliveryBinding::AttachSessions);
            assert_every_subscriber_wired(&messenger, &router);
        }

        /// A dropped `register_remote_delivery_routes` call is invisible at boot —
        /// a remote's entries do not exist yet for the walk to catch — and shows
        /// up as a panic on the first delivery. This is the assertion that keeps
        /// the wiring in place.
        #[test]
        #[should_panic(expected = "has no delivery binding")]
        fn a_runtime_minted_remote_entry_without_its_binding_panics() {
            let messenger = remote_messenger(true);
            let router = WakeRouterImpl::new(ActiveBridges::new());
            assert_every_subscriber_wired(&messenger, &router);
        }

        /// And a dropped `subscriber_registrations` insert fails *closed* at the
        /// delivery ACL gate — a silent drop, the failure this check exists to
        /// turn into a named boot death.
        #[test]
        #[should_panic(expected = "has no wake-economics")]
        fn a_runtime_minted_remote_entry_without_its_registration_panics() {
            let messenger = remote_messenger(false);
            let router = WakeRouterImpl::new(ActiveBridges::new());
            router.register_delivery_binding(remote_key(), DeliveryBinding::AttachSessions);
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
