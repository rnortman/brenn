//! The pure half of the messaging lowering: a document in, a plan out.
//!
//! Everything here is a computation over [`BrennConfig`] and the values a
//! document determines. Nothing reads the host filesystem, opens a database,
//! constructs a messenger or spawns a task — [`commit_messaging`](crate::commit_messaging)
//! does all of that, from a plan this module built. Two callers depend on the
//! split: boot, which plans and then commits, and a config check on a machine
//! that is not the deployment target, which plans and throws the plan away.
//!
//! Every refusal is a panic, in the words boot prints as it dies, so a caller
//! that wants a report rather than a death catches the unwind.

use std::sync::Arc;

use brenn_lib::config::{AppConfig, BrennConfig};
use brenn_lib::messaging::config::{
    ResolvedSubscription, ResolvedSurface, ResolvedSurfaceSubscription, ResolvedWasmConsumer,
};
use brenn_lib::messaging::remote::{RemoteFacts, resolve_remote_facts_all};
use brenn_lib::messaging::{
    ChannelEntry, ChannelScheme, MessagingDirectory, SubscriberEntryKind, SubscriberRegistration,
    WakeEconomics, webhook_channel_uuid_from_slug,
};
use brenn_lib::mqtt::config::{MqttClientIdentity, ResolvedMqttIngressChannel};
use brenn_messaging as messaging;
use indexmap::IndexMap;

use crate::{
    ChannelTopology, assert_unique_channel_uuids, assert_unique_store_paths,
    build_apps_with_messaging, derive_mqtt_ingress_channels, finish_surface_policies,
    lower_channel_topology, messaging_configured, resolve_surfaces, resolve_wasm_consumers,
    validate_exact_tuning_blocks, validate_static_subscriptions_deliverable,
};

/// What a plan is computed from.
///
/// Every member is either the document itself or a value the document
/// determines. The two `Option`s are two independent facts about the caller, not
/// one mode: absent resolved apps and absent tool registry each skip exactly the
/// passes that read them, and nothing else in the planner asks where it is
/// running.
pub struct PlanInputs<'a> {
    pub config: &'a BrennConfig,
    /// The resolved app map. `None` when the caller has not run app resolution
    /// (a config check): the app-derived subscriptions, roster channels and
    /// goal-channel participant are then absent, and the description
    /// single-writer sweep is skipped.
    pub apps: Option<&'a Arc<IndexMap<String, AppConfig>>>,
    /// The non-secret half of every `[[mqtt_client]]`, which is all the
    /// derivation reads: membership, and each client's `qos`/`urgency`.
    pub mqtt_clients: &'a IndexMap<String, MqttClientIdentity>,
    /// The tool registry. `None` when the caller has not built one (a config
    /// check): the async tool substrate — request channels, result inboxes, the
    /// executor participant, the derived async grants — is then not minted, the
    /// two tool arms of the exact-tuning cross-check have nothing to check
    /// against, and per-consumer grant validation does not run.
    pub tool_registry: Option<&'a brenn_tool_registry::ToolRegistry>,
    /// Canonical store paths from `[[webhook_endpoint]].replay_protection`,
    /// against which the consumers' stores must not alias. Empty when the
    /// caller has not resolved the endpoints.
    pub replay_store_paths: &'a [std::path::PathBuf],
}

/// Everything the messaging lowering derives from a document, and nothing it
/// derives from a host.
///
/// A plan is what [`commit_messaging`](crate::commit_messaging) turns into a
/// running messaging layer: it holds the directory as assembled, the resolved
/// entities, and the tables the messenger is installed with.
pub struct MessagingPlan {
    /// The finalized directory: every channel entry with its static subscribers
    /// folded in.
    pub directory: Arc<MessagingDirectory>,
    /// The non-durable entries (`ephemeral:` and `local:`), in declaration
    /// order. These carry no DB row; the ring substrate is built from them.
    pub nondurable_channels: Vec<ChannelEntry>,
    /// Resolved `[[wasm_consumer]]` blocks, inbox ports and derived async
    /// grants applied.
    pub wasm_consumers: Vec<ResolvedWasmConsumer>,
    /// The tuning table every system-minted entry above resolved its depths
    /// from, derived from `config.channels`.
    pub system_channel_tuning: messaging::config::SystemChannelTuning,
    /// Resolved `[[surface]]` blocks, in declaration order.
    pub surfaces: Vec<ResolvedSurface>,
    /// Gates 1–5 of every `[[remote]]`. The bearer token is gate 6 and commit's.
    pub remotes: Vec<RemoteFacts>,
    /// The distinct `mqtt:<client>:<topic>` ingress channels this document
    /// mints — the set the broker SUBSCRIBE union and the router's
    /// `IngressRoute` table are built from.
    pub mqtt_ingress_channels: Vec<ResolvedMqttIngressChannel>,
    /// Code-declared system participants, one per `system:` principal this
    /// document activates.
    pub system_participants: Vec<brenn_messaging::system::SystemParticipantSpec>,
    /// One registration per non-app subscriber — consumer, surface, remote,
    /// system component — carrying its policy and wake economics.
    pub registrations: std::collections::HashMap<SubscriberEntryKind, SubscriberRegistration>,
    /// One send budget per attach principal: each surface's kernel identity and
    /// component instances, and one per remote.
    pub attach_send_budgets: Vec<(
        brenn_lib::messaging::ParticipantId,
        messaging::config::AttachSendBudget,
    )>,
    /// The global messaging block, for the messenger and the channel
    /// reconstruction commit runs.
    pub messaging_globals: messaging::config::MessagingGlobalConfig,
    /// The `[llm_chat]` block, for the messenger's chat family derivation.
    pub llm_chat: brenn_lib::config::LlmChatConfig,
    /// One entry per consumer holding a non-empty tool grant set, keyed by the
    /// caller's participant id (`wasm:<slug>`). What the executor's grant table
    /// is built from at boot and edited to on a reload.
    ///
    /// Derived here rather than at either install site because it is an
    /// authorization table over `[[wasm_consumer]]`, a block a reload converges:
    /// a second spelling of it is a second answer to who may call what.
    ///
    /// Empty after boot takes ownership of its contents. Read it before boot,
    /// or from a plan nothing has taken from.
    pub tool_caller_grants: brenn_tool_registry::CallerGrantTable,
    /// The one non-fatal finding `[observability] surface_error_channel`
    /// validation raises, or `None`. Carried rather than logged because the
    /// callers report differently — boot has a `tracing` subscriber, an offline
    /// config check has none.
    pub surface_error_advisory: Option<brenn_surface_server::SurfaceErrorAdvisory>,
    /// The resolved app map this plan's `App` subscriber entries, roster
    /// channels, goal participant and description single-writer sweep were
    /// derived from, or `None` where the planner ran without one.
    ///
    /// Not commit's operating value — commit is handed the map it installs into
    /// the messenger. This is what that map is held against
    /// ([`MessagingPlan::was_derived_from`]), so a directory computed against
    /// one policy set can never be committed beside delivery gates that consult
    /// another.
    pub(crate) planned_apps: Option<Arc<IndexMap<String, AppConfig>>>,
}

impl MessagingPlan {
    /// Whether `apps` is the very map this plan was derived from.
    ///
    /// Identity rather than equality: two equal maps are still two boot-time
    /// snapshots, and what the directory and the delivery gates have to share is
    /// one snapshot.
    pub fn was_derived_from(&self, apps: &Arc<IndexMap<String, AppConfig>>) -> bool {
        self.planned_apps
            .as_ref()
            .is_some_and(|planned| Arc::ptr_eq(planned, apps))
    }
}

/// Lower a document into a [`MessagingPlan`], or `None` when it configures no
/// messaging at all.
///
/// The `None` arm is [`messaging_configured`]'s answer, and it is the same arm
/// boot and a config check both take. Two validators run on *both* arms, before
/// the early return, because a document that routes surface error reports
/// nowhere must be refused whether or not it declared a channel to route them
/// onto.
///
/// # Panics
///
/// On every refusal the passes below make, in the words boot prints.
pub fn plan_messaging(inputs: &PlanInputs) -> Option<MessagingPlan> {
    let config = inputs.config;
    let mqtt_clients = inputs.mqtt_clients;
    let tool_registry = inputs.tool_registry;
    let no_apps: IndexMap<String, AppConfig> = IndexMap::new();
    let apps: &IndexMap<String, AppConfig> = inputs.apps.map(|a| &**a).unwrap_or(&no_apps);

    if !messaging_configured(config) {
        // Both surface validators run even with no messaging, so callers do
        // not each have to remember this arm. No directory means no channel to
        // read a frontier off, so this arm never yields an advisory.
        let advisory = brenn_surface_server::validate_surface_error_channel(
            config.observability.surface_error_channel.as_deref(),
            None,
            config.messaging.max_body_bytes,
        );
        assert!(advisory.is_none());
        brenn_surface_server::description::validate_surface_description_set(
            &config.surface_description,
            &[],
            None,
        );
        return None;
    }

    // The tuning table every mint site below resolves against, derived here
    // from the document being lowered rather than taken as an input. A
    // `[[channel]]` block addressed at a system-minted channel does not declare
    // it but sizes it, so the table is a projection of `config.channels`: a
    // caller-supplied one could describe a different document than the one
    // whose channels are being minted.
    let system_channel_tuning =
        messaging::config::build_system_channel_tuning(&config.channels, &config.messaging);

    // --- Derive webhook channel entries from the [[webhook_endpoint]] blocks ---
    //
    // Read off the raw blocks rather than off the resolved endpoint map: an
    // endpoint's channel identity, description and mount are all facts about the
    // document, and the resolved map is behind this host's secret files.
    let global_defaults = &config.messaging;
    let webhook_channel_entries: Vec<ChannelEntry> = config
        .webhook_endpoints
        .iter()
        .map(|raw| {
            let address = format!("webhook:{}", raw.slug);
            let resolved_channel = messaging::config::resolve_system_channel(
                &address,
                &system_channel_tuning,
                global_defaults,
            );
            ChannelEntry {
                uuid: webhook_channel_uuid_from_slug(&raw.slug),
                address,
                description: raw.description.clone(),
                resolved_channel,
                subscribers: vec![],
                transport_type: ChannelScheme::Webhook,
                mount: Some(brenn_lib::webhook::webhook_mount(raw)),
            }
        })
        .collect();

    let mqtt_ingress_channels = derive_mqtt_ingress_channels(config, mqtt_clients);
    let mqtt_channel_entries: Vec<ChannelEntry> = mqtt_ingress_channels
        .iter()
        .map(|channel| ChannelEntry {
            uuid: channel.channel_uuid,
            address: channel.channel_address.clone(),
            description: None,
            resolved_channel: messaging::config::resolve_system_channel(
                &channel.channel_address,
                &system_channel_tuning,
                global_defaults,
            ),
            subscribers: vec![],
            transport_type: ChannelScheme::Mqtt,
            mount: None,
        })
        .collect();

    let apps_with_messaging = build_apps_with_messaging(apps, global_defaults);

    let mut environment_entries = webhook_channel_entries;
    environment_entries.extend(mqtt_channel_entries);
    let topology = lower_channel_topology(config, environment_entries);

    // Resolve WASM consumer subscriptions before finalizing the directory:
    // the directory is built from these same entries, so the wasm_consumers
    // vec must be ready when finalize_directory_with_subscribers runs.
    let pre_directory = topology.pre_directory();
    let ChannelTopology {
        durable_entries: mut all_entries,
        nondurable_entries: nondurable_channels,
        auto_wiring,
    } = topology;
    let mut resolved_wasm_consumers = resolve_wasm_consumers(
        &config.wasm_consumers,
        &pre_directory,
        &config.wasm.store_size_limit,
        mqtt_clients,
        &auto_wiring,
    );
    // No two stores may name one file. The `OPEN_PATHS` guard in `KvStore::open`
    // catches an alias at load time with a generic error; this one names the
    // config blocks, and it runs here because both populations — the replay
    // endpoints' stores and the consumers' — are known.
    {
        let consumer_paths: Vec<(std::path::PathBuf, String)> = resolved_wasm_consumers
            .iter()
            .filter_map(|c| c.store_path.clone().map(|p| (p, c.slug.clone())))
            .collect();
        assert_unique_store_paths(inputs.replay_store_paths, &consumer_paths);
    }

    // Every input registers in the directory, whatever its channel's class: the
    // registration is where a subscription's resolved parameters (push depth,
    // retain depth, noise rung) live, and the substrate reads them for a
    // ring-backed subscriber exactly as for a durable one. What follows
    // durability is where the subscriber's *position* lives: a durable cursor
    // row, or the ring's in-memory cursor that dies with the data it names.
    let mut wasm_consumers_for_dir: Vec<(String, Vec<ResolvedSubscription>)> =
        resolved_wasm_consumers
            .iter()
            .map(|c| {
                let subs = c.inputs.iter().map(|inp| inp.sub.clone()).collect();
                (c.slug.clone(), subs)
            })
            .collect();

    // Surface resolution must precede directory finalization: every
    // transportable surface subscription folds into a
    // `SubscriberEntryKind::Surface` directory entry.
    let mut resolved_surfaces = resolve_surfaces(
        &config.surfaces,
        &pre_directory,
        global_defaults,
        &auto_wiring,
    );

    // Gates 1–5 of every `[[remote]]`, which are facts about the document. Gate
    // 6 is the bearer token, read off this host's disk further down where the
    // environment is already being touched.
    let remote_facts: Vec<RemoteFacts> = resolve_remote_facts_all(&config.remotes, global_defaults);

    finish_surface_policies(&mut resolved_surfaces, config);

    // Every surface subscription is a component instance's, keyed
    // `<slug>#<instance>` (`#` is outside the operator slug charset), so surface
    // subscribers are disjoint from app/wasm-consumer slugs by construction — no
    // bare-slug surface subscription exists to collide in the durable
    // push-window keyspace.
    let surfaces_for_dir: Vec<(String, Vec<ResolvedSurfaceSubscription>)> = resolved_surfaces
        .iter()
        .map(|s| (s.slug.clone(), s.wire_subscriptions.clone()))
        .collect();

    // --- Async tool substrate: request channels, result inboxes, derived grants ---
    //
    // One `brenn:tools/<tool>` request channel per registered async tool (the
    // executor subscribes to each as `system:tool-executor`); for each wasm
    // consumer holding ≥1 async tool grant, one `brenn:tool-results/<slug>` inbox
    // plus the derived async bus grants. The channels ride the same
    // finalize/upsert path as every other channel; the inbox subscription is
    // folded through `wasm_consumers_for_dir` into the directory like a
    // configured wasm subscription, and as a triggering `WasmInputPort` on the
    // consumer's `inputs` so a delivered result activates the consumer.
    // The System request-channel subscriber is folded in from the executor's
    // `SystemParticipantSpec` below and validated by the deliverability check
    // like every other static subscriber.
    let async_tool_names: Vec<&'static str> = tool_registry
        .map(brenn_tool_registry::ToolRegistry::async_tool_names)
        .unwrap_or_default();
    for tool in &async_tool_names {
        all_entries.push(brenn_tool_registry::bus_wiring::request_channel_entry(
            tool,
            &system_channel_tuning,
            global_defaults,
        ));
    }
    // TODO(tool-registry-unregistered-tool-sweep): once tools can be
    // dynamically (de)registered, sweep `brenn:tools/*` pending rows here for
    // tools no longer in the registry — alert and delete them at boot rather
    // than executing a request against a removed tool. Unreachable today: the
    // async tool set is fixed in code, so a pending row can only name a
    // registered tool.
    let mut tuned_inbox_slugs: std::collections::HashSet<String> = std::collections::HashSet::new();
    for consumer in resolved_wasm_consumers.iter_mut() {
        let async_tools = match tool_registry {
            Some(registry) => {
                brenn_tool_registry::bus_wiring::consumer_async_tools(registry, &consumer.policy)
            }
            None => Vec::new(),
        };
        if async_tools.is_empty() {
            continue;
        }
        brenn_tool_registry::bus_wiring::derive_async_tool_bus_grants(
            &mut consumer.policy,
            &consumer.slug,
            &async_tools,
        );
        let inbox_entry = brenn_tool_registry::bus_wiring::result_inbox_entry(
            &consumer.slug,
            &system_channel_tuning,
            global_defaults,
        );
        // The consumer's subscription follows its inbox's window, so the
        // subscriber never reaches past what the channel block sized.
        let inbox_window = inbox_entry.resolved_channel.retain_depth;
        all_entries.push(inbox_entry);
        let inbox_sub =
            brenn_tool_registry::bus_wiring::inbox_subscription(&consumer.slug, inbox_window);
        let dir_entry = wasm_consumers_for_dir
            .iter_mut()
            .find(|(slug, _)| slug == &consumer.slug)
            .expect("wasm_consumers_for_dir has an entry per resolved consumer");
        dir_entry.1.push(inbox_sub);
        consumer
            .inputs
            .push(brenn_tool_registry::bus_wiring::inbox_input_port(
                &consumer.slug,
                inbox_window,
            ));
        tuned_inbox_slugs.insert(consumer.slug.clone());
    }
    // Each consumer's tool grants against the registry: a wasm policy is not in
    // the `apps` map the registry scanned at its own construction, so this is
    // where a grant naming no registered tool, or an ACL key the tool does not
    // declare, is refused. Skipped with no registry, which is a missed refusal
    // and never a false one.
    // The same walk builds the caller grant table: presence in it means "may
    // address some tool", so a consumer with no grants has no entry. Built with
    // no registry too — nothing consumes it on that path, and making it
    // conditional would be a second rule about what a plan holds.
    let mut tool_caller_grants = brenn_tool_registry::CallerGrantTable::new();
    for consumer in &resolved_wasm_consumers {
        if consumer.policy.tool_grants.is_empty() {
            continue;
        }
        if let Some(registry) = tool_registry {
            registry.validate_grants(
                &format!("wasm consumer {:?}", consumer.slug),
                &consumer.policy.tool_grants,
            );
        }
        tool_caller_grants.insert(
            brenn_lib::messaging::ParticipantId::for_wasm(&consumer.slug)
                .as_str()
                .to_owned(),
            consumer.policy.tool_grants.clone(),
        );
    }

    // Every exact tuning block must name a system channel that exists — a typoed
    // address must not silently tune nothing. Runs here because this is the
    // first point where all three populations are known.
    let webhook_slugs: Vec<&str> = config
        .webhook_endpoints
        .iter()
        .map(|raw| raw.slug.as_str())
        .collect();
    validate_exact_tuning_blocks(
        &system_channel_tuning,
        &webhook_slugs,
        &async_tool_names,
        &tuned_inbox_slugs,
        tool_registry.is_some(),
    );
    // System participant specs: every `system:` principal is declared here and
    // everything it needs — its subscriber registration, its (subscriber)
    // directory entries, deliverability validation, and (for subscribers) a
    // parked-notify delivery binding — is derived from the one declaration.
    //   - the tool executor, present whenever any async tool is registered
    //     (subscriber; it subscribes to every `brenn:tools/<tool>` request channel);
    //   - `system:surface-help`, publish-only — granted an exact-match publish ACL
    //     on every derived boot-published help/schema/index channel;
    //   - `system:surface-config`, publish-only — granted an exact-match
    //     `ephemeral_publish` ACL on every surface's bindings channel. Separate
    //     from the help identity so each holds exactly its own family.
    //   - `system:chat-roster`, publish-only — granted an exact-match
    //     `brenn_publish` ACL on every app's roster channel. Present whenever
    //     any app is configured; the roster is the only chat address no
    //     conversation's harness may write.
    // A publish-only spec carries no subscriptions, so it gets a registry entry
    // (publish authority) but no directory subscriber entry and no delivery
    // binding — it is never a dispatch target. The surface error channel has no
    // system participant: surfaces publish reports onto it under their own
    // `surface:<slug>` identities (a boot-injected substrate grant).
    let mut system_participants: Vec<brenn_messaging::system::SystemParticipantSpec> = Vec::new();
    if !async_tool_names.is_empty() {
        system_participants.push(brenn_tool_registry::bus_wiring::tool_executor_spec(
            &async_tool_names,
        ));
    }
    let boot_published_bares = brenn_surface_server::description::boot_published_bare_channels(
        &config.surface_description.prefix,
        &resolved_surfaces,
    );
    system_participants.push(brenn_surface_server::description::surface_help_spec(
        &boot_published_bares,
    ));
    let config_bares = brenn_surface_server::description::surface_config_bare_channels(
        &config.surface_description.prefix,
        &resolved_surfaces,
    );
    system_participants.push(brenn_surface_server::description::surface_config_spec(
        &config_bares,
    ));
    // One roster channel per configured app, and the single identity that
    // writes them. Boot-declared rather than provisioned with a conversation:
    // apps are static config, and an app with no conversations still owes a
    // peer the snapshot that says so.
    let roster_bares: Vec<String> = apps
        .keys()
        .map(|slug| brenn_envelope::chat::chat_roster_bare_name(&config.llm_chat.prefix, slug))
        .collect();
    if !roster_bares.is_empty() {
        for slug in apps.keys() {
            all_entries.push(brenn_messaging::chat_roster::chat_roster_entry(
                &config.llm_chat,
                slug,
                global_defaults,
            ));
        }
        system_participants.push(
            brenn_messaging::system::SystemParticipantSpec::publish_only(
                brenn_messaging::chat_roster::CHAT_ROSTER_COMPONENT,
                brenn_lib::messaging::ChannelScheme::Brenn,
                &roster_bares,
            ),
        );
    }
    // Goal addresses are operator-declared durable channels, so they are
    // already in `all_entries` when the fold below looks for them.
    let goal_addrs: Vec<String> = {
        let mut addrs: Vec<String> = apps
            .values()
            .filter_map(|app| app.claude_profiles.as_ref())
            .filter_map(|p| p.goal.clone())
            .collect();
        addrs.sort();
        addrs.dedup();
        addrs
    };
    if !goal_addrs.is_empty() {
        system_participants.push(brenn_cc_profile::cc_profile_spec(&goal_addrs));
    }
    // Declared, not minted: the operator's two `[[channel]]` blocks are
    // already in `all_entries`, and one without the other is a refusal.
    if let Some(spec) = brenn_messaging::config_reload::config_reload_spec(&all_entries) {
        system_participants.push(spec);
    }
    brenn_messaging::system::fold_spec_subscriptions(&mut all_entries, &system_participants);

    // The non-durable channels join `all_entries` here, after the WASM and
    // surface resolution passes above read them off `pre_directory`. This is
    // the set the final subscriber-carrying directory is built from, and it is
    // built here rather than reusing `pre_directory` because the async-tool
    // channels and the system participants' folded subscriptions landed on
    // `all_entries` in between. `local:` entries are included: a backend
    // `local:` channel is a channel of this process.
    all_entries.extend(nondurable_channels.iter().cloned());

    // All entry sources (declared, webhook/mqtt-derived, auto, tool-substrate)
    // have contributed. This is the only uuid collision check; nothing
    // downstream detects one.
    assert_unique_channel_uuids(all_entries.iter());

    let directory = Arc::new(messaging::config::finalize_directory_with_subscribers(
        all_entries,
        &apps_with_messaging,
        &wasm_consumers_for_dir,
        &surfaces_for_dir,
    ));

    // The depth ceiling, checked once over the assembled directory: every
    // subscriber sits within its channel's standing_retain_depth. Runs here
    // because this is the first point where every subscriber source — config
    // blocks, folded system participants, auto synthesis, the tool substrate —
    // has contributed.
    messaging::config::validate_subscriber_depth_ceilings(&directory);

    // Boot-time fail-fast validation ("Static subscription with no
    // covering ACL matcher"): every operator-declared STATIC subscription must
    // resolve to a policy that actually authorizes delivery on its channel. A
    // static subscriber whose resolved policy lacks the transport grant + covering
    // ACL matcher can *never* receive a message — the runtime delivery gate would
    // deny-by-default on every delivery. That is a misconfiguration ("BETTER
    // DEAD THAN WRONG / fail fast on bad config"), so we refuse to start with a precise
    // diagnostic naming the offending subscription rather than booting into a
    // silently-dead subscription. The check runs against the just-finalized
    // directory (config static subscribers only — App `[[app.messaging.subscribe]]`
    // / `[[app.mqtt_subscription]]` / `[[app.webhook_subscription]]`,
    // `[[wasm_consumer.subscription]]`, and `brenn:` `[[surface.subscription]]`);
    // it runs BEFORE the dynamic-row boot merge, so dynamic durable rows (handled
    // by the non-destructive `revoked` classification — those may be re-granted
    // later) are deliberately not subject to this panic.
    validate_static_subscriptions_deliverable(
        &directory,
        apps,
        &resolved_wasm_consumers,
        &resolved_surfaces,
        &system_participants,
    );

    // Order matters: the error channel first, matching what a reporting caller
    // catches as one unwind.
    let surface_error_advisory = brenn_surface_server::validate_surface_error_channel(
        config.observability.surface_error_channel.as_deref(),
        Some(&directory),
        config.messaging.max_body_bytes,
    );
    let description_channels = brenn_surface_server::description::validate_surface_description_set(
        &config.surface_description,
        &resolved_surfaces,
        Some(&directory),
    );
    // The single-writer sweep needs the resolved app policies — the same map the
    // publish gates consult, so validation cannot drift from enforcement — and
    // so it runs only where they exist. Without them it is a missed refusal,
    // with boot authoritative.
    if inputs.apps.is_some() {
        let app_policies: Vec<(&str, &brenn_lib::access::AppPolicy)> = apps
            .iter()
            .map(|(slug, cfg)| (slug.as_str(), &cfg.policy))
            .collect();
        brenn_surface_server::description::validate_surface_description_writers(
            &description_channels,
            brenn_surface_server::SingleWriterPrincipals {
                app_policies: &app_policies,
                wasm_consumers: &resolved_wasm_consumers,
                surfaces: &resolved_surfaces,
                system_participants: &system_participants,
            },
        );
    }
    drop(description_channels);

    // The unified subscriber registry: one entry per non-app subscriber (WASM
    // consumer, surface, remote, system component), keyed by its
    // `SubscriberEntryKind`, carrying its resolved policy and declared wake
    // economics. These policies are not in the app map — they live on the
    // resolved consumer, surface, remote and spec — so `subscriber_policy` and
    // the publish-authority arms reach these subscribers through here. All are
    // cheap to wake, so each is `Eager`.
    let mut registrations: std::collections::HashMap<SubscriberEntryKind, SubscriberRegistration> =
        std::collections::HashMap::new();
    for c in &resolved_wasm_consumers {
        registrations.insert(
            SubscriberEntryKind::Wasm(c.slug.clone()),
            SubscriberRegistration {
                policy: Arc::new(c.policy.clone()),
                wake: WakeEconomics::Eager,
            },
        );
    }
    // Every declared surface is registered, not just the ones holding a
    // transportable binding today: surface target resolution fails closed on a
    // missing registration, so deriving this set from the bindings would
    // silently deny delivery the moment a binding is added. Authority is
    // per-surface, and so is the subscriber grain: the directory is cut at
    // (surface, channel), and a publish under a component attribution resolves
    // its policy here too.
    for s in &resolved_surfaces {
        registrations.insert(
            SubscriberEntryKind::Surface(s.slug.clone()),
            SubscriberRegistration {
                policy: Arc::new(s.policy.clone()),
                wake: WakeEconomics::Eager,
            },
        );
    }
    // A remote is a single principal with no finer grain — no instances, no
    // per-user split — and its wake economics are the attacher's: eager,
    // because a session waiting on a socket is not a subprocess to be spared a
    // wake. Registered here even though a remote holds no boot directory entry:
    // the entries its subscribes mint at runtime resolve their policy through
    // this map, and a runtime-minted entry with no registration would fail
    // closed at the delivery gate with nothing to point at.
    for r in &remote_facts {
        registrations.insert(
            SubscriberEntryKind::Remote(r.slug().to_string()),
            SubscriberRegistration {
                policy: Arc::new(r.policy().clone()),
                wake: WakeEconomics::Eager,
            },
        );
    }
    registrations.extend(brenn_messaging::system::registrations_from_specs(
        &system_participants,
    ));

    // One budget per attach principal. A surface contributes its own kernel
    // identity plus each component instance it declares, each with its resolved
    // parameters (a declared override, or the defaults) — both from
    // `ResolvedSurface::principal_send_budgets`, built on the same declaration
    // set the sub-identity derivation admits an instance against, so the budget
    // map and the derivation cannot disagree about which principals exist. A
    // remote contributes one, at the shared defaults: it declares no
    // sub-identities and no per-remote knob, and this backstop is the same
    // defense-in-depth bound on the same substrate whoever writes into it. A
    // publish whose principal has no bucket is a boot invariant the publish
    // gate panics on.
    let attach_send_budgets: Vec<(
        brenn_lib::messaging::ParticipantId,
        messaging::config::AttachSendBudget,
    )> = resolved_surfaces
        .iter()
        .flat_map(|s| {
            messaging::attach_principal_budgets(
                messaging::AttachScope::surface(&s.slug),
                s.principal_send_budgets().collect(),
            )
        })
        .chain(remote_facts.iter().flat_map(|r| {
            messaging::attach_principal_budgets(
                messaging::AttachScope::remote(r.slug()),
                vec![(None, messaging::config::AttachSendBudget::default())],
            )
        }))
        .collect();

    Some(MessagingPlan {
        directory,
        nondurable_channels,
        wasm_consumers: resolved_wasm_consumers,
        system_channel_tuning,
        surfaces: resolved_surfaces,
        remotes: remote_facts,
        mqtt_ingress_channels,
        system_participants,
        registrations,
        attach_send_budgets,
        messaging_globals: config.messaging.clone(),
        llm_chat: config.llm_chat.clone(),
        tool_caller_grants,
        surface_error_advisory,
        planned_apps: inputs.apps.map(Arc::clone),
    })
}

#[cfg(test)]
mod tests {
    use brenn_lib::config::BrennConfig;
    use brenn_lib::messaging::config::Depth;

    use super::{PlanInputs, plan_messaging};
    use crate::assert_unique_store_paths;
    use crate::test_fixtures::durable_channel;

    /// A document declaring the description index and one error channel at
    /// `standing`, with `surface_error_channel` pointed at it.
    fn config_with_error_channel(standing: Depth) -> BrennConfig {
        let mut config = BrennConfig::default();
        config
            .channels
            .push(durable_channel("brenn:surface.index", Depth::Unbounded));
        config
            .channels
            .push(durable_channel("brenn:surface-errors", standing));
        config.observability.surface_error_channel = Some("brenn:surface-errors".to_string());
        config
    }

    fn plan_of(config: &BrennConfig) -> super::MessagingPlan {
        plan_messaging(&PlanInputs {
            config,
            apps: None,
            mqtt_clients: &indexmap::IndexMap::new(),
            tool_registry: None,
            replay_store_paths: &[],
        })
        .expect("a document declaring channels configures messaging")
    }

    /// An error channel whose eviction frontier sits at the surface send burst
    /// is the advisory's case, and the plan is what carries it out: the
    /// validator runs in the planner because `channels` is a block a reload may
    /// change.
    #[test]
    fn a_frontier_at_the_send_burst_rides_out_on_the_plan() {
        let burst = u64::from(brenn_messaging::publish::SURFACE_SEND_BURST);
        let config = config_with_error_channel(Depth::Bounded(burst));
        let advisory = plan_of(&config)
            .surface_error_advisory
            .expect("frontier == burst must raise the retention advisory");
        assert_eq!(advisory.channel, "brenn:surface-errors");
        assert_eq!(advisory.frontier, burst);
    }

    /// The same document with the channel pinned has no frontier to advise
    /// about.
    #[test]
    fn a_pinned_error_channel_carries_no_advisory() {
        let config = config_with_error_channel(Depth::Unbounded);
        assert!(plan_of(&config).surface_error_advisory.is_none());
    }

    /// The error channel is validated against the plan's own directory, so a
    /// document that names a channel it never declares is refused here rather
    /// than at a service start.
    #[test]
    #[should_panic(expected = "does not resolve to any declared [[channel]] block")]
    fn an_undeclared_error_channel_is_refused_by_the_planner() {
        let mut config = BrennConfig::default();
        config
            .channels
            .push(durable_channel("brenn:surface.index", Depth::Unbounded));
        config.observability.surface_error_channel = Some("brenn:nowhere".to_string());
        plan_of(&config);
    }

    // ---------------------------------------------------------------------
    // The reload facility: declared by the operator, activated by the planner.
    // ---------------------------------------------------------------------

    /// A document declaring the reload pair activates `system:config-reload`,
    /// and the participant lands on the request channel as a subscriber through
    /// the same fold every other system participant goes through.
    #[test]
    fn the_declared_reload_pair_activates_the_facility() {
        use brenn_lib::messaging::SubscriberEntryKind;
        use brenn_messaging::config_reload::{
            CONFIG_RELOAD_COMPONENT, RELOAD_ADDRESS, STATUS_ADDRESS,
        };

        let mut config = BrennConfig::default();
        config
            .channels
            .push(durable_channel("brenn:surface.index", Depth::Unbounded));
        config
            .channels
            .push(durable_channel(RELOAD_ADDRESS, Depth::Bounded(4)));
        config
            .channels
            .push(durable_channel(STATUS_ADDRESS, Depth::Bounded(8)));
        let plan = plan_of(&config);

        assert!(
            plan.system_participants
                .iter()
                .any(|spec| spec.component == CONFIG_RELOAD_COMPONENT),
            "the pair is declared, so the participant exists"
        );
        let request = plan
            .directory
            .list()
            .into_iter()
            .find(|entry| entry.address == RELOAD_ADDRESS)
            .expect("the request channel is a declared entry");
        assert!(
            request.subscribers.iter().any(|sub| matches!(
                &sub.kind,
                SubscriberEntryKind::System(component) if component == CONFIG_RELOAD_COMPONENT
            )),
            "the participant reads the request channel"
        );
        let status = plan
            .directory
            .list()
            .into_iter()
            .find(|entry| entry.address == STATUS_ADDRESS)
            .expect("the status channel is a declared entry");
        assert!(status.subscribers.is_empty());
    }

    /// A document declaring neither channel has no facility and no participant
    /// — the addresses are ordinary until an operator writes them.
    #[test]
    fn a_document_without_the_pair_has_no_facility() {
        use brenn_messaging::config_reload::CONFIG_RELOAD_COMPONENT;

        let mut config = BrennConfig::default();
        config
            .channels
            .push(durable_channel("brenn:surface.index", Depth::Unbounded));
        config
            .channels
            .push(durable_channel("brenn:notes", Depth::Bounded(4)));
        assert!(
            !plan_of(&config)
                .system_participants
                .iter()
                .any(|spec| spec.component == CONFIG_RELOAD_COMPONENT)
        );
    }

    /// Half a facility is a refusal at plan time, which is a boot panic and, on
    /// the reload path, a refusal — never a process that accepts requests it
    /// cannot report on.
    #[test]
    #[should_panic(expected = "both channels or neither")]
    fn a_request_channel_alone_is_refused_by_the_planner() {
        use brenn_messaging::config_reload::RELOAD_ADDRESS;

        let mut config = BrennConfig::default();
        config
            .channels
            .push(durable_channel("brenn:surface.index", Depth::Unbounded));
        config
            .channels
            .push(durable_channel(RELOAD_ADDRESS, Depth::Bounded(4)));
        plan_of(&config);
    }

    /// The planner resolves a consumer's ports against the channel population
    /// it derived, and that population is the document's whole population —
    /// so a port on an address nothing mints is refused with one verdict,
    /// whichever inputs the planner is handed. The compiler cannot express this
    /// document (an unresolvable endpoint name is a diagnostic), which is why
    /// the shape is pinned over a raw configuration.
    #[test]
    #[should_panic(expected = "is not a known channel address")]
    fn a_port_on_an_unminted_address_is_refused_with_no_registry() {
        let mut config = BrennConfig::default();
        config
            .channels
            .push(durable_channel("brenn:surface.index", Depth::Unbounded));
        config.wasm_consumers = vec![
            brenn_lib::messaging::config::WasmConsumerConfigRaw::minimal(
                "hooked",
                "processor-demo",
                &["webhook:ghost"],
            ),
        ];
        plan_of(&config);
    }

    // ---------------------------------------------------------------------
    // The tool caller grant table: one derivation, the planner's.
    // ---------------------------------------------------------------------

    /// The table names exactly the consumers that hold grants, keyed by the
    /// participant id the executor re-checks a dequeued request under. A
    /// consumer with no grants has no entry, so presence keeps meaning "may
    /// address some tool".
    #[test]
    fn the_plan_carries_one_grant_entry_per_granted_consumer() {
        let mut granted = brenn_lib::messaging::config::WasmConsumerConfigRaw::minimal(
            "puller",
            "processor-demo",
            &["brenn:work"],
        );
        granted.subscribe_acl = vec![brenn_lib::access::raw::ChannelMatcherRaw::Exact(
            "work".to_string(),
        )];
        granted.tool_grants = vec![brenn_lib::tools::config::ToolGrantRaw {
            tool: "apull".to_string(),
            acl: vec![std::collections::BTreeMap::from([(
                "repo".to_string(),
                "brenn".to_string(),
            )])],
            rate_limit: None,
        }];
        let mut ungranted = brenn_lib::messaging::config::WasmConsumerConfigRaw::minimal(
            "quiet",
            "processor-demo",
            &["brenn:work"],
        );
        ungranted.subscribe_acl = vec![brenn_lib::access::raw::ChannelMatcherRaw::Exact(
            "work".to_string(),
        )];

        let mut config = BrennConfig::default();
        config
            .channels
            .push(durable_channel("brenn:surface.index", Depth::Unbounded));
        config
            .channels
            .push(durable_channel("brenn:work", Depth::Bounded(4)));
        config.wasm_consumers = vec![granted, ungranted];

        let plan = plan_of(&config);
        let mut callers: Vec<&String> = plan.tool_caller_grants.keys().collect();
        callers.sort();
        assert_eq!(callers, vec!["wasm:puller"]);
        let grants = &plan.tool_caller_grants["wasm:puller"];
        assert_eq!(grants.keys().collect::<Vec<_>>(), vec!["apull"]);
    }

    /// The mount is checked where the entry is minted. A block whose declared
    /// mount is outside the served namespace never reaches the HTTP layer that
    /// would have refused it, because the planner mints the entry first — and
    /// on a config check the planner is the only half that runs at all.
    #[test]
    #[should_panic(expected = "mount \"/hooks/gh\" is invalid")]
    fn a_webhook_mount_outside_the_namespace_is_refused_by_the_planner() {
        let mut config = BrennConfig::default();
        config
            .channels
            .push(durable_channel("brenn:surface.index", Depth::Unbounded));
        let mut endpoint = crate::test_fixtures::webhook_endpoint_raw("gh-events");
        endpoint.mount = Some("/hooks/gh".to_string());
        config.webhook_endpoints = vec![endpoint];
        plan_of(&config);
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
}
