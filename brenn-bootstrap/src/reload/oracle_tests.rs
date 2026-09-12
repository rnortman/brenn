//! The correctness oracle, and the cases that need a process rather than a
//! plan.
//!
//! The rule every other test here serves one clause of, stated whole: after a
//! successful reload the process is in the state a fresh boot of the new
//! document would have produced. [`the_reloaded_process_matches_a_fresh_boot`]
//! checks it literally — boot A, reload to A′, and boot A′ a second time over a
//! copy of the database taken before the reload, then compare the two
//! processes. Nothing derived from the delta is trusted: the snapshot is read
//! off the live directory, the registrations, the running registry, the ring
//! stores and the durable rows.
//!
//! The rest are the cases the earlier increments' in-memory fixture could not
//! reach: a channel under a publisher while its consumer is being retired, a
//! subscriber that only the live directory knows about, and an artifact that
//! moved under a document that did not.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use brenn_lib::messaging::config::{Depth, NoiseLevel, ResolvedSurface};
use brenn_lib::messaging::{SubscriberEntry, SubscriberEntryKind};
use brenn_messaging::Messenger;
use brenn_server::test_support::init_db_file;

use super::driver::TriggerSource;
use super::driver::tests::{
    BootFixture, Booted, CEILING, CONFIG_MOUNT, FRAGMENT_ADDRESS, FRAGMENT_CONSUMER, READER,
    SPILL_ACL_BY_ADDRESS, Tree, async_tool_registry, bearer_endpoint, boot, boot_with,
    conversation_of, document, document_covering_work, document_push_subscribing,
    document_push_subscribing_acl, document_subscribing, document_with,
    document_with_a_broker_only, document_with_a_consumer, document_with_an_mqtt_consumer,
    document_with_clients, document_with_webhooks, fragment, insert_dynamic_row,
    insert_push_dynamic_row, install_package, install_package_from, push_owner_covering_work,
    restage, seat_a_conversation, seat_position, seat_user, spill_channel, staged_module,
    subscriber_debug_lines, surface_document, surfaces_document, write_surface_kind,
};
use brenn_messaging::config_reload::Outcome;

// ── The oracle ────────────────────────────────────────────────────────────

/// Everything about a running process that a fresh boot of the same document
/// must reproduce.
///
/// Every field is read off the running system rather than off the plan that
/// produced it: a reload that told itself it had converged and had not would
/// pass a comparison of plans and fail this one.
/// `Debug` alone, deliberately: `assert_matches` is the comparison, and an
/// `assert_eq!` on two whole snapshots is the tens-of-kilobytes diagnostic it
/// exists to replace. Without the derive that assertion does not compile.
#[derive(Debug)]
pub(crate) struct Snapshot {
    /// Directory entries, one rendered line each, sorted by address.
    channels: Vec<String>,
    /// Every subscriber kind the directory names, and what the target resolver
    /// answers for it.
    registrations: Vec<String>,
    /// The consumer slugs whose tasks are running.
    running: Vec<String>,
    /// Addresses holding a ring store — the non-durable channels.
    rings: Vec<String>,
    /// The durable channel rows, and every subscriber position.
    rows: Vec<String>,
    /// The executor's per-caller tool grant table, one rendered line per
    /// caller, sorted by caller.
    grants: Vec<String>,
    /// The surface runtime table the doors read: one line per served slug,
    /// sorted by slug, carrying the resolved surface and its description
    /// channels.
    surfaces: Vec<String>,
    /// The asset roots `/surface-static` is serving from: one line per kind,
    /// sorted, with the mount, the root and both fingerprints.
    surface_roots: Vec<String>,
    /// The attach send budgets, one line per principal, sorted.
    attach_budgets: Vec<String>,
    /// The newest retained body on every channel that retains anything, one
    /// line per address, sorted. What compares the bindings documents, the
    /// description family and the disconnected stamps rather than only the
    /// addresses they sit on.
    retained: Vec<String>,
    /// The broker SUBSCRIBE set each live session holds: one line per
    /// `(client, filter)` with its qos, sorted.
    ///
    /// Read off the service rather than off the document: a filter subscribed
    /// that no document names, or named that neither process holds, is exactly
    /// the drift this field exists to see, and a plan-derived read would hide
    /// both.
    mqtt_filters: Vec<String>,
    /// The ingress router's route table, one line per route, sorted.
    ///
    /// A route's uuid is its channel's, so the line renders the address: the
    /// uuid is stable across the two processes but says nothing to a reader,
    /// and the address is what the rest of the snapshot is keyed by.
    mqtt_routes: Vec<String>,
    /// The clients holding a broker session, sorted.
    ///
    /// Read off the service rather than off the document: a reload that
    /// registered, restarted or stopped a client and left the registry in a
    /// different state than a fresh boot would build is exactly the drift
    /// this field exists to see.
    mqtt_sessions: Vec<String>,
    /// The resolved config every registered session is connected as: one line
    /// per slug, sorted.
    ///
    /// The session set beside it says which clients exist; this says what they
    /// are. A reload that registered a client on the predecessor's credential,
    /// or restarted one and carried the wrong identity into the successor,
    /// holds the same slug as a fresh boot and differs here. Secrets are
    /// rendered as digests, for the reason the endpoint field's scheme is: a
    /// snapshot is a panic message.
    mqtt_client_configs: Vec<String>,
    /// The webhook endpoint table the inbound handler resolves against: one
    /// line per served slug, sorted.
    ///
    /// Read off the running `WebhookService`, not off the resolved document:
    /// a reload that installed a subtly different entry than boot builds — an
    /// owner stamped one way on one path and another on the other, a default
    /// applied on one only — agrees with a fresh boot on the channel the
    /// endpoint mints and differs here. Secrets are rendered as digests: what
    /// must match is the bytes, and a snapshot is a panic message.
    webhook_endpoints: Vec<String>,
    /// The authority view of every agent, one line per slug, sorted.
    ///
    /// Read off the table the gates read, not off the plan: a reload that
    /// converged its subscriber entries and forgot to swap the map agrees with
    /// a fresh boot on every channel line and differs here.
    agent_authority: Vec<String>,
    /// The class-B view of every agent — what a Claude Code process is spawned
    /// with — one line per slug, sorted.
    agent_spawn: Vec<String>,
    /// The class-A view of every agent — what a route reads per request — one
    /// line per slug, sorted.
    agent_per_call: Vec<String>,
    /// Every row of `messaging_dynamic_subscriptions`, sorted.
    ///
    /// Nothing is excluded, `created_at` included: a row is minted once when
    /// the agent subscribed and is carried across a restart verbatim, so two
    /// processes over the same database must hold the same timestamps. A
    /// revoked row that a fresh boot classifies dormant is still a row on both
    /// sides; a row the reload deleted where boot keeps it is the drift this
    /// field is here to see.
    dynamic_subs: Vec<String>,
    /// The virtual-tools file each agent's `noop_mcp.py` would read at its
    /// next start: one line per slug carrying the file's whole content, sorted.
    ///
    /// Read off the filesystem rather than re-rendered from the map, because
    /// the rename is the step under test: an agent whose grant moved and whose
    /// staged rendering never landed holds the old bytes here while a fresh
    /// boot holds the new.
    virtual_tools: Vec<String>,
}

impl Snapshot {
    /// Compare against a fresh boot's, field by field, reporting only the lines
    /// the two processes disagree about.
    ///
    /// Field by field rather than one `assert_eq!` on the whole struct: a
    /// snapshot is ten lists of rendered lines, and a single differing byte in
    /// one of them prints both processes whole — tens of kilobytes in which the
    /// difference is the thing hardest to find. A convergence defect is
    /// supposed to name itself.
    fn assert_matches(&self, fresh: &Snapshot) {
        let Snapshot {
            channels,
            registrations,
            running,
            rings,
            rows,
            grants,
            surfaces,
            surface_roots,
            attach_budgets,
            retained,
            mqtt_filters,
            mqtt_routes,
            mqtt_sessions,
            mqtt_client_configs,
            webhook_endpoints,
            agent_authority,
            agent_spawn,
            agent_per_call,
            dynamic_subs,
            virtual_tools,
        } = self;
        assert_lines("channels", channels, &fresh.channels);
        assert_lines("registrations", registrations, &fresh.registrations);
        assert_lines("running", running, &fresh.running);
        assert_lines("rings", rings, &fresh.rings);
        assert_lines("rows", rows, &fresh.rows);
        assert_lines("grants", grants, &fresh.grants);
        assert_lines("surfaces", surfaces, &fresh.surfaces);
        assert_lines("surface_roots", surface_roots, &fresh.surface_roots);
        assert_lines("attach_budgets", attach_budgets, &fresh.attach_budgets);
        assert_lines("retained", retained, &fresh.retained);
        assert_lines("mqtt_filters", mqtt_filters, &fresh.mqtt_filters);
        assert_lines("mqtt_routes", mqtt_routes, &fresh.mqtt_routes);
        assert_lines("mqtt_sessions", mqtt_sessions, &fresh.mqtt_sessions);
        assert_lines(
            "mqtt_client_configs",
            mqtt_client_configs,
            &fresh.mqtt_client_configs,
        );
        assert_lines(
            "webhook_endpoints",
            webhook_endpoints,
            &fresh.webhook_endpoints,
        );
        assert_lines("agent_authority", agent_authority, &fresh.agent_authority);
        assert_lines("agent_spawn", agent_spawn, &fresh.agent_spawn);
        assert_lines("agent_per_call", agent_per_call, &fresh.agent_per_call);
        assert_lines("dynamic_subs", dynamic_subs, &fresh.dynamic_subs);
        assert_lines("virtual_tools", virtual_tools, &fresh.virtual_tools);
    }

    /// The authority view, for a transition asserting the agent moved at all.
    pub(crate) fn agent_authority(&self) -> &[String] {
        &self.agent_authority
    }

    /// The class-B view, for the transitions whose point is a respawn.
    pub(crate) fn agent_spawn(&self) -> &[String] {
        &self.agent_spawn
    }

    /// The durable dynamic rows, for the transitions that revoke, revive or
    /// prune one: a field compared while empty on both sides tests nothing.
    pub(crate) fn dynamic_subs(&self) -> &[String] {
        &self.dynamic_subs
    }

    /// The rendered tool lists, for the transition that moves one.
    pub(crate) fn virtual_tools(&self) -> &[String] {
        &self.virtual_tools
    }

    /// The broker SUBSCRIBE set, for a transition asserting it moved at all.
    pub(crate) fn mqtt_filters(&self) -> &[String] {
        &self.mqtt_filters
    }

    /// The ingress route table, for the same reason: a field compared while
    /// empty on both sides is a field testing nothing.
    pub(crate) fn mqtt_routes(&self) -> &[String] {
        &self.mqtt_routes
    }

    /// The session set, for a transition whose point is that it did *not* move.
    pub(crate) fn mqtt_sessions(&self) -> &[String] {
        &self.mqtt_sessions
    }

    /// What those sessions are connected as, for a transition whose point is
    /// that a credential or an identity field moved under an unmoved slug.
    pub(crate) fn mqtt_client_configs(&self) -> &[String] {
        &self.mqtt_client_configs
    }

    /// The served endpoint table, for a transition asserting it moved at all:
    /// a field compared while empty on both sides tests nothing.
    pub(crate) fn webhook_endpoints(&self) -> &[String] {
        &self.webhook_endpoints
    }
}

/// One field of two snapshots, compared element for element and reported as
/// the lines each holds and the other does not.
///
/// The predicate is equality on the two sorted slices, not a set difference:
/// position and multiplicity are part of what a fresh boot must reproduce, and
/// the tables that would hide a duplicate are the append-shaped ones — a route
/// added twice, a filter subscribed beside itself — which is exactly the
/// "moved and should not have" drift this comparison exists to see. The
/// difference sets are for the message only.
fn assert_lines(what: &str, reloaded: &[String], fresh: &[String]) {
    if reloaded == fresh {
        return;
    }
    let only_reloaded: Vec<&String> = reloaded.iter().filter(|l| !fresh.contains(l)).collect();
    let only_fresh: Vec<&String> = fresh.iter().filter(|l| !reloaded.contains(l)).collect();
    let difference = if only_reloaded.is_empty() && only_fresh.is_empty() {
        format!(
            "  the same lines, in a different order or a different number of times.\n  \
             after the reload: {reloaded:#?}\n  after a fresh boot: {fresh:#?}"
        )
    } else {
        format!(
            "  only after the reload: {only_reloaded:#?}\n  only after a fresh boot: \
             {only_fresh:#?}"
        )
    };
    panic!(
        "{what}: the reloaded process is not where a fresh boot of the same document would \
         have left it.\n{difference}",
    );
}

/// One directory entry as a line: its identity, its tuning, its metadata and
/// its subscribers.
///
/// `resolved_channel` and each subscriber go in through `Debug`, so a field
/// added to either joins the comparison by existing rather than by being
/// remembered here.
fn entry_line(entry: &brenn_lib::messaging::ChannelEntry) -> String {
    let subscribers = subscriber_debug_lines(entry);
    format!(
        "{} uuid={} transport={:?} mount={:?} description={:?} tuning={:?} subscribers=[{}]",
        entry.address,
        entry.uuid,
        entry.transport_type,
        entry.mount,
        entry.description,
        entry.resolved_channel,
        subscribers.join(" | "),
    )
}

/// One served endpoint as a line: what it is, what it verifies against, and
/// what its replay protection is bound to.
///
/// The scheme goes in as a digest of its `Debug` rendering rather than
/// verbatim, because that rendering carries the secret bytes and a snapshot is
/// printed by a failing assertion. A rotated secret changes the digest, which
/// is the whole of what this field has to see.
fn endpoint_line(entry: &brenn_webhook::EndpointRuntime) -> String {
    let endpoint = &entry.endpoint;
    let replay = match (endpoint.replay_protection.as_ref(), entry.replay.as_ref()) {
        (Some(rp), Some(guard)) => format!(
            "component={:?} store_path={:?} max_page_count={} config={:?} release={}",
            rp.component,
            rp.store_path,
            rp.max_page_count,
            {
                let mut config: Vec<(&String, &String)> = rp.config.iter().collect();
                config.sort();
                config
            },
            guard.verified.artifact_sha256,
        ),
        (None, None) => "none".to_string(),
        // A guard without a block, or a block without a guard, is drift in
        // itself: rendered rather than unwrapped so the comparison reports it.
        (rp, guard) => format!(
            "inconsistent block={:?} guard={:?}",
            rp.is_some(),
            guard.is_some()
        ),
    };
    format!(
        "{} mount={:?} owner={:?} ceiling={} content_type={:?} urgency={:?} scheme={} replay=[{}]",
        endpoint.slug,
        endpoint.mount,
        endpoint.owner,
        endpoint.transport_ceiling_bytes,
        endpoint.content_type,
        endpoint.urgency,
        brenn_lib::util::sha256_hex(format!("{:?}", endpoint.scheme).as_bytes()),
        replay,
    )
}

/// One registered session's resolved config as a line: the whole identity, and
/// a digest of each credential.
///
/// The identity goes in through `Debug`, so a field added to it joins the
/// comparison by existing. The two secrets do not: `MqttClientConfig`'s own
/// `Debug` renders the password as `<redacted>` and the CA as a byte count, and
/// neither says whether the bytes moved — which is the whole of what a rotation
/// transition has to see. Digested rather than rendered, because a snapshot is
/// printed by a failing assertion.
fn client_config_line(slug: &str, config: &brenn_lib::mqtt::config::MqttClientConfig) -> String {
    let digest = |bytes: &[u8]| brenn_lib::util::sha256_hex(bytes);
    format!(
        "{slug} identity={:?} password={} ca={}",
        config.identity,
        config
            .password
            .as_ref()
            .map_or_else(|| "none".to_string(), |p| digest(p.as_bytes())),
        config
            .ca_cert_pem
            .as_ref()
            .map_or_else(|| "none".to_string(), |pem| digest(pem)),
    )
}

async fn snapshot(booted: &Booted) -> Snapshot {
    let entries = booted.messenger.directory().list();

    let mut channels: Vec<String> = entries.iter().map(|entry| entry_line(entry)).collect();
    channels.sort();

    let mut kinds: Vec<SubscriberEntryKind> = Vec::new();
    for entry in &entries {
        for subscriber in &entry.subscribers {
            if !kinds.contains(&subscriber.kind) {
                kinds.push(subscriber.kind.clone());
            }
        }
    }
    // Every key the messenger holds a live registration for, beside the ones
    // the directory names: `surface-help` and `surface-config` are registered
    // publish-only, so they hold no subscriber entry on any channel and the
    // directory walk above cannot see them. Their matchers are what a reload
    // narrows as surfaces come and go, and this is the field that compares
    // them.
    for kind in booted.messenger.registered_subscriber_kinds() {
        if !kinds.contains(&kind) {
            kinds.push(kind);
        }
    }

    // The whole registration, not its `wake` alone: a registration's policy is
    // what decides whether a participant may publish where, and the two
    // surface-description participants' matchers move with every surface a
    // reload adds or retires. A narrowing the reload forgot is a difference
    // here and nowhere else.
    let mut registrations: Vec<String> = kinds
        .iter()
        .map(|kind| {
            let registration = booted.messenger.subscriber_registration(kind);
            format!("{kind:?} {registration:?}")
        })
        .collect();
    registrations.sort();

    // Each running consumer with the root its package was resolved out of: a
    // mount swap moves the root under an unchanged document, and a reload that
    // kept serving the old tree would agree with a fresh boot on every other
    // field.
    let mut running: Vec<String> = booted
        .driver
        .registry()
        .iter()
        .map(|(slug, consumer)| format!("{slug} root={}", consumer.verified.root.display()))
        .collect();
    running.sort();

    let mut rings: Vec<String> = booted
        .messenger
        .ring_stores()
        .stores()
        .iter()
        .map(|store| store.address().to_string())
        .collect();
    rings.sort();

    let mut grants: Vec<String> = booted
        .tool_caller_grants
        .as_ref()
        .map(|table| table.snapshot())
        .unwrap_or_default()
        .into_iter()
        .map(|(caller, grants)| format!("{caller} {grants:?}"))
        .collect();
    // Sorted rather than taken in map order: the outer map is a `HashMap`, so
    // its iteration order is per-instance and the oracle compares two distinct
    // processes.
    grants.sort();

    let table = booted.driver.env().surfaces.with_table(|table| {
        assert!(
            table.reconfiguring.is_empty(),
            "a process at rest holds no mid-swap mark: {:?}",
            table.reconfiguring,
        );
        let mut lines: Vec<String> = table
            .runtimes
            .iter()
            .map(|(slug, runtime)| {
                format!(
                    "{slug} resolved={:?} geometry={} status={} config={}",
                    runtime.resolved,
                    runtime.description.geometry_channel,
                    runtime.description.status_channel,
                    runtime.description.config_channel,
                )
            })
            .collect();
        // Sorted rather than taken in map order: `runtimes` is a `HashMap`,
        // and the oracle compares two distinct processes.
        lines.sort();
        lines
    });

    let served = booted.driver.env().surface_roots();
    let mut surface_roots: Vec<String> = served
        .kinds
        .iter()
        .map(|(kind, root)| format!("{kind} {root:?}"))
        .collect();
    surface_roots.sort();
    surface_roots.push(format!("kernel {:?}", served.kernel));

    let apps = booted.driver.env().apps.load();
    let mut agent_authority: Vec<String> = apps
        .values()
        .map(|app| format!("{} {:?}", app.slug, app.authority()))
        .collect();
    agent_authority.sort();
    // The two class views, rendered as `brenn-lib` spells them: their field
    // lists are one exhaustive destructuring of `AppConfig`, so a new field
    // cannot be classified in `compare.rs` and silently missed here.
    let mut agent_spawn: Vec<String> = apps
        .values()
        .map(|app| format!("{:?}", app.class_views().0))
        .collect();
    agent_spawn.sort();
    let mut agent_per_call: Vec<String> = apps
        .values()
        .map(|app| format!("{:?}", app.class_views().1))
        .collect();
    agent_per_call.sort();
    let mut virtual_tools: Vec<String> = apps
        .values()
        .map(|app| {
            let path = app.virtual_tools_path();
            match std::fs::read_to_string(&path) {
                Ok(content) => format!("{} {content}", app.slug),
                // Rendered rather than skipped: an agent whose file the process
                // never wrote is a difference from one whose file it did.
                Err(e) => format!("{} (unreadable: {e})", app.slug),
            }
        })
        .collect();
    virtual_tools.sort();

    let (mqtt_filters, mqtt_routes) = mqtt_ingress(booted, &entries).await;
    let mqtt_sessions: Vec<String> = booted.mqtt.0.client_slugs();
    let mut mqtt_client_configs: Vec<String> = booted
        .mqtt
        .0
        .baseline()
        .iter()
        .map(|(slug, config)| client_config_line(slug, config))
        .collect();
    mqtt_client_configs.sort();
    let mut webhook_endpoints: Vec<String> = booted
        .webhook
        .baseline()
        .values()
        .map(|entry| endpoint_line(entry))
        .collect();
    webhook_endpoints.sort();

    Snapshot {
        channels,
        registrations,
        running,
        rings,
        rows: durable_rows(&booted.messenger).await,
        grants,
        surfaces: table,
        surface_roots,
        attach_budgets: booted.messenger.attach_send_budget_lines(),
        retained: retained_documents(booted, &entries).await,
        mqtt_filters,
        mqtt_routes,
        mqtt_sessions,
        mqtt_client_configs,
        webhook_endpoints,
        agent_authority,
        agent_spawn,
        agent_per_call,
        dynamic_subs: dynamic_subs(&booted.messenger).await,
        virtual_tools,
    }
}

/// Every durable dynamic-subscription row, as lines.
///
/// The channel is rendered by uuid rather than by address: a row survives its
/// channel leaving the directory — that is what dormancy is — so a join would
/// drop exactly the rows a revoke case is about. The uuid is stable across the
/// two processes because the row carries it.
async fn dynamic_subs(messenger: &Messenger) -> Vec<String> {
    let conn = messenger.db().lock().await;
    let mut rows: Vec<String> = brenn_messaging_store::db::load_dynamic_subscriptions(&conn)
        .into_iter()
        .map(|row| {
            format!(
                "{} on {} push_depth={:?} retain_depth={:?} noise={:?} wake_min={:?} qos={:?} \
             created_at={}",
                row.app_slug,
                row.channel_uuid,
                row.push_depth,
                row.retain_depth,
                row.noise,
                row.wake_min,
                row.qos,
                row.created_at,
            )
        })
        .collect();
    rows.sort();
    rows
}

/// The two MQTT ingress fields: the filter set every live session holds, and
/// the router's route table.
///
/// Both are enumerated off the running subsystem — the service's registered
/// clients, then each handle's own subscription list — rather than off the
/// plan's ingress channels. A process with no MQTT runtime at all answers two
/// empty lists, which is what a document naming no client leaves behind on both
/// sides.
///
/// `sub_id` is deliberately not rendered: it is a per-session counter handed out
/// in subscription order, so the process that subscribed one filter at boot and
/// one at reload holds different numbers from the one that subscribed both at
/// boot, for the same set of filters. The filter and its qos are the whole of
/// what the broker was told.
async fn mqtt_ingress(
    booted: &Booted,
    entries: &[Arc<brenn_lib::messaging::ChannelEntry>],
) -> (Vec<String>, Vec<String>) {
    let (service, router) = &booted.mqtt;

    let mut filters = Vec::new();
    for slug in service.client_slugs() {
        let handle = service
            .get_client(&slug)
            .expect("a slug the registry just listed has a handle");
        for subscription in handle.subscriptions.read().await.iter() {
            filters.push(format!(
                "{slug} {} qos={}",
                subscription.topic_filter, subscription.qos,
            ));
        }
    }
    filters.sort();

    let mut routes: Vec<String> = router
        .route_uuids()
        .into_iter()
        .map(
            |uuid| match entries.iter().find(|entry| entry.uuid == uuid) {
                Some(entry) => entry.address.clone(),
                // A route whose channel the directory does not hold is the defect
                // this field is here to catch, so it is rendered rather than
                // skipped.
                None => format!("(no directory entry) {uuid}"),
            },
        )
        .collect();
    // Sorted rather than taken in table order: `add_route` appends, so a
    // reload's order is arrival order and a fresh boot's is plan order.
    routes.sort();

    (filters, routes)
}

/// The channel rows and the subscriber positions, as lines.
///
/// `resume_epoch` is deliberately absent: it is minted with the row, so two
/// processes that each created a row for the same channel hold different ones
/// and always will. Everything else about a row is the document's.
async fn durable_rows(messenger: &Messenger) -> Vec<String> {
    let conn = messenger.db().lock().await;
    let mut rows: Vec<String> = conn
        .prepare("SELECT address, description, transport_type FROM messaging_channels")
        .expect("the channels table is readable")
        .query_map([], |row| {
            Ok(format!(
                "channel {} description={:?} transport={}",
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .expect("the channel rows read")
        .map(|row| row.expect("a channel row"))
        .collect();

    rows.extend(
        conn.prepare(
            "SELECT c.subscriber, ch.address, c.push_depth, c.next_owed_seq \
             FROM messaging_subscriber_cursors c \
             JOIN messaging_channels ch ON ch.uuid = c.channel_uuid",
        )
        .expect("the cursor table is readable")
        .query_map([], |row| {
            Ok(format!(
                "cursor {} on {} push_depth={} next_owed_seq={}",
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })
        .expect("the cursor rows read")
        .map(|row| row.expect("a cursor row")),
    );

    rows.sort();
    rows
}

/// The newest retained body on every channel that retains anything, as lines.
///
/// Every channel that retains anything at all — `Bounded(n >= 1)` or
/// `Unbounded` — rather than the ones retaining exactly one: the surface status
/// channels retain four here, a durable channel may retain without bound, and a
/// selector keyed to one depth or one variant would silently drop a channel a
/// deployment retuned.
///
/// Bodies are compared verbatim, with three exclusions, each a value minted per
/// process at a named site and nowhere else:
///
/// - `brenn:config.status`, whose whole purpose is to differ between a reloaded
///   process and a fresh one.
/// - The generation timestamp of the description family — the `- generated:`
///   line of the index and the two help documents, and the `"ts"` member of a
///   kind schema. `build_description_docs_selected` mints one `Utc::now()` per
///   publish and threads it into every body it builds, so the reload side's is
///   its reload time and the fresh side's its boot time. Every other byte —
///   the surface and kind tables, the channel pointers, the schema and
///   dimensions, the build line — stays.
/// - The `ts` and `epoch` of a surface's `disconnected` stamp, minted from the
///   server clock and the process's own bus epoch as `resume_epoch` is.
///
/// A comparison that fails is a convergence defect and is fixed in the commit
/// step; this list is never widened to make one pass.
async fn retained_documents(
    booted: &Booted,
    entries: &[Arc<brenn_lib::messaging::ChannelEntry>],
) -> Vec<String> {
    let prefix = &booted
        .driver
        .baseline()
        .document
        .config
        .surface_description
        .prefix;
    let families = description_families(prefix, booted.driver.baseline().surfaces());
    let mut lines = Vec::new();
    for entry in entries {
        if entry.address == brenn_messaging::config_reload::STATUS_ADDRESS {
            continue;
        }
        let retains = match entry.resolved_channel.retain_depth {
            Depth::Unbounded => true,
            Depth::Bounded(n) => n >= 1,
        };
        if !retains {
            continue;
        }
        let store = booted.messenger.store_for(entry);
        let Some(newest) = store.retained_tail(Depth::Bounded(1)).await.pop() else {
            lines.push(format!("{} (nothing retained)", entry.address));
            continue;
        };
        lines.push(format!(
            "{} {}",
            entry.address,
            narrowed(families.get(&entry.address).copied(), &newest.body),
        ));
    }
    lines.sort();
    lines
}

/// Which per-process value a retained body carries, and therefore which line or
/// member the comparison elides from it.
#[derive(Clone, Copy)]
enum Family {
    /// A markdown description document — the index, a surface's help, a kind's
    /// help — carrying `- generated: <ts>`.
    Markdown,
    /// A kind schema document, carrying a `"ts"` member.
    KindSchema,
    /// A surface's `disconnected` stamp, carrying `ts` and `epoch`.
    SurfaceStatus,
}

/// Every address whose body carries a per-process value, built with the same
/// constructors the publishers derive their addresses from.
///
/// Built rather than parsed: the address grammar has one owner
/// (`brenn_surface_server::description`), and an oracle that re-derived it from
/// string surgery would be a second informal copy living in the one test meant
/// to notice when the real one moves. An address this map does not hold is
/// compared verbatim, which is the correct default for anything new.
fn description_families(prefix: &str, surfaces: &[ResolvedSurface]) -> HashMap<String, Family> {
    use brenn_surface_server::description::{
        distinct_kinds, index_channel, kind_help_channel, kind_schema_channel,
        surface_help_channel, surface_status_channel,
    };

    let mut families = HashMap::new();
    families.insert(index_channel(prefix), Family::Markdown);
    for surface in surfaces {
        families.insert(
            surface_help_channel(prefix, &surface.slug),
            Family::Markdown,
        );
        families.insert(
            surface_status_channel(prefix, &surface.slug),
            Family::SurfaceStatus,
        );
    }
    for kind in distinct_kinds(surfaces) {
        families.insert(kind_help_channel(prefix, &kind), Family::Markdown);
        families.insert(kind_schema_channel(prefix, &kind), Family::KindSchema);
    }
    families
}

/// One retained body as the comparison sees it: verbatim, unless its address is
/// one of the description family's, whose body carries a per-process value at a
/// known line.
fn narrowed(family: Option<Family>, body: &str) -> String {
    match family {
        None => body.to_string(),
        Some(Family::Markdown) => body
            .lines()
            .filter(|line| !line.starts_with("- generated: "))
            .collect::<Vec<&str>>()
            .join("\n"),
        Some(Family::KindSchema) => {
            let mut doc: serde_json::Value =
                serde_json::from_str(body).expect("a kind schema document is JSON");
            doc.as_object_mut()
                .expect("a kind schema document is a JSON object")
                .remove("ts")
                .expect("a kind schema document carries its generation timestamp");
            doc.to_string()
        }
        Some(Family::SurfaceStatus) => {
            let stamp = brenn_surface_schema::telemetry::DisconnectedStamp::parse(body)
                .expect("a server-written status body is a disconnected stamp");
            format!(
                "v={} session={:?} health={:?} reason={}",
                stamp.v, stamp.session, stamp.health, stamp.reason,
            )
        }
    }
}

/// Copy the database as it stands, consistently, to `to`.
///
/// `VACUUM INTO` rather than a file copy: the production pragmas put the
/// connection in WAL mode, where the file on disk is not the database.
async fn copy_database(db: &brenn_db::Db, to: &std::path::Path) {
    let conn = db.lock().await;
    conn.execute(
        "VACUUM INTO ?1",
        rusqlite::params![to.to_str().expect("a UTF-8 path")],
    )
    .expect("the database copies");
}

/// A consumer holding no tool grant at all, stamped into both documents.
///
/// Without it every consumer in the fixture holds a grant and the oracle
/// comparison never sees the grantless shape of the caller table.
const QUIET: &str = r#"new quiet: Plain {
    grants = [ports];
    in inbound <- sink { push_depth = 2; }
    out digest -> scratch;
}"#;

/// A: one consumer holding an async tool grant, so the process the oracle
/// compares has a non-empty executor grant table to compare.
fn document_with_a_tool_granted_consumer() -> String {
    document(&format!(
        r#"channel sink at "brenn:sink" {{
    push_depth = 1;
    retain_depth = 4;
    standing_retain_depth = 4;
}}
{}component Demo {{
    abi = processor;
    requires = [ports, tools];
    in inbound;
    in tool-results;
    out digest;
}}

component Plain {{
    abi = processor;
    requires = [ports];
    in inbound;
    out digest;
}}
{}
new sifter: Demo {{
    grants = [ports, tools];
    in inbound <- work {{ push_depth = 4; }}
    out digest -> sink;
    tool apull {{ allow {{ repo = "brenn"; }} }}
}}

{QUIET}
"#,
        brenn_lib::config::PACKAGED,
        brenn_lib::config::PACKAGED,
    ))
}

/// A′: [`document_with_a_tool_granted_consumer`] plus a channel and a second
/// consumer of the same component, granted a different repo, so the reload has
/// an added channel, an added consumer, an added caller key, and — the
/// package's spec bytes having moved with the document's packaged half — a
/// consumer that is changed rather than merely present.
fn document_with_two_consumers() -> String {
    document(&format!(
        r#"channel sink at "brenn:sink" {{
    push_depth = 1;
    retain_depth = 4;
    standing_retain_depth = 4;
}}

channel digested at "brenn:digested" {{
    push_depth = 2;
    retain_depth = 8;
    standing_retain_depth = 8;
}}
{}component Demo {{
    abi = processor;
    requires = [ports, tools];
    in inbound;
    in tool-results;
    out digest;
}}

component Plain {{
    abi = processor;
    requires = [ports];
    in inbound;
    out digest;
}}
{}
new sifter: Demo {{
    grants = [ports, tools];
    in inbound <- work {{ push_depth = 4; }}
    out digest -> sink;
    tool apull {{ allow {{ repo = "brenn"; }} }}
}}

new grinder: Demo {{
    grants = [ports, tools];
    in inbound <- sink {{ push_depth = 2; }}
    out digest -> digested;
    tool apull {{ allow {{ repo = "notes"; }} }}
}}

{QUIET}
"#,
        brenn_lib::config::PACKAGED,
        brenn_lib::config::PACKAGED,
    ))
}

/// Boot A, reload it to A', boot A' fresh over the database as it stood before
/// the reload, and compare the two processes.
///
/// The database is copied *before* the reload rather than after, so the fresh
/// boot starts from what a restart at that moment would have started from —
/// and every row the reload wrote is the reload's own claim, which is exactly
/// what is under test.
///
/// `fixture` is asked for a `BootFixture` twice, once per process, because the
/// two differ in the database they boot over and in nothing else: a fresh side
/// standing on a different rig would be comparing rigs. `arrive` writes
/// whatever the transition is — the new document, a rewritten kind tree, a
/// newly declared mount — and reloads; `worth_comparing` is the assertion that
/// the transition happened at all, since two processes that both did nothing
/// compare equal.
///
/// Where the fresh side does not reproduce a piece of state the reload edits,
/// the fixture is extended rather than the comparison narrowed.
pub(crate) async fn a_reload_matches_a_fresh_boot(
    tree: &Tree,
    fixture: impl Fn(brenn_db::Db) -> BootFixture,
    arrive: impl AsyncFnOnce(&mut Booted),
    worth_comparing: impl FnOnce(&Snapshot),
) {
    a_seeded_reload_matches_a_fresh_boot(tree, fixture, async |_| {}, arrive, worth_comparing)
        .await;
}

/// [`a_reload_matches_a_fresh_boot`] with a hook that runs on the booted
/// process **before** the database is copied.
///
/// For the transitions whose starting state is not the document alone: a user
/// row the agent's `allowed_users` names, a durable dynamic subscription the
/// agent minted at runtime. Both sides have to start from that state, and only
/// what is in the database when the copy is taken reaches the fresh side.
///
/// The seed writes rows, never the document tree: a change to the tree is the
/// transition and belongs in `arrive`.
pub(crate) async fn a_seeded_reload_matches_a_fresh_boot(
    tree: &Tree,
    fixture: impl Fn(brenn_db::Db) -> BootFixture,
    seed: impl AsyncFnOnce(&Booted),
    arrive: impl AsyncFnOnce(&mut Booted),
    worth_comparing: impl FnOnce(&Snapshot),
) {
    let store = tempfile::tempdir().expect("a directory for the databases");
    let mut booted = boot_with(
        tree,
        fixture(init_db_file(&store.path().join("running.db"))),
    )
    .await;

    seed(&booted).await;

    let restart_point = store.path().join("restart.db");
    copy_database(&booted.db, &restart_point).await;

    arrive(&mut booted).await;
    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);

    let reloaded = snapshot(&booted).await;
    worth_comparing(&reloaded);

    // The reload side's broker session is done: nothing below reads its MQTT
    // runtime, and leaving it up would have the two processes' supervisors
    // taking the same session over from each other — both boot one document, so
    // both connect under one client id — for the whole of the fresh side's
    // life. A no-op when the fixture stood no supervisor up.
    booted.stop_mqtt();

    // A fresh boot of A′ over the database as it stood before the reload:
    // what the operator would have got by restarting the service instead.
    let restarted = boot_with(tree, fixture(init_db_file(&restart_point))).await;
    let fresh = snapshot(&restarted).await;
    // And the fresh side's, explicitly: dropping `restarted` stops nothing,
    // because each supervisor holds a sender of its own, so an unsignalled
    // supervisor stays connected under the document's client id for the rest of
    // the binary. A second transition over the same broker would then be
    // contending with a session this one is finished with.
    restarted.stop_mqtt();
    reloaded.assert_matches(&fresh);
}

/// The correctness oracle over the transition the facility was built for: a
/// consumer and a channel arrive.
#[tokio::test(flavor = "multi_thread")]
async fn the_reloaded_process_matches_a_fresh_boot() {
    let components = tempfile::tempdir().expect("a components root");
    let roots = vec![components.path().to_path_buf()];

    let tree = Tree::holding(&document_with_a_tool_granted_consumer());
    install_package(components.path(), &staged_module(&tree));

    a_reload_matches_a_fresh_boot(
        &tree,
        |db| BootFixture {
            db: Some(db),
            components_roots: roots.clone(),
            tool_registry: Some(async_tool_registry()),
            ..BootFixture::default()
        },
        async |booted| {
            tree.write(&document_with_two_consumers());
            install_package(components.path(), &staged_module(&tree));
            booted.driver.reload(TriggerSource::Signal).await;
            assert_eq!(
                booted.last_status().await.delta.consumers_added,
                vec!["grinder".to_string()]
            );
        },
        // The comparison is only worth anything if there is something to
        // compare: both consumers running, and the added channel in the
        // directory.
        |reloaded| {
            assert_eq!(reloaded.running.len(), 3, "{:?}", reloaded.running);
            assert!(
                reloaded
                    .channels
                    .iter()
                    .any(|line| line.starts_with("brenn:digested ")),
                "{:?}",
                reloaded.channels
            );
            assert_eq!(
                reloaded.grants.len(),
                2,
                "the two granted consumers hold a caller key each and the grantless one holds \
                 none: {:?}",
                reloaded.grants
            );
        },
    )
    .await;
}

// ── The oracle over the surface transitions ───────────────────────────────

/// The rig every surface transition boots on: a document tree, the `panel`
/// kind's deployed assets beside it, and the fixture both processes boot under.
///
/// The asset tree is a `TempDir` the caller has to hold — dropping it takes the
/// mount out from under both processes.
fn panel_fixture(assets: &std::path::Path) -> impl Fn(brenn_db::Db) -> BootFixture + use<'_> {
    move |db| BootFixture {
        db: Some(db),
        surface_assets: Some(assets.to_path_buf()),
        ..BootFixture::default()
    }
}

/// Rewrite the document and reload.
async fn reload_onto(booted: &mut Booted, tree: &Tree, document: &str) {
    tree.write(document);
    booted.driver.reload(TriggerSource::Signal).await;
}

/// **The first surface in a document that had none.** Nothing surface-shaped
/// exists in the running process — no runtime, no matcher for a slug, no
/// budget, and an index that says `_none configured_` — and one reload has to
/// produce all of it.
#[tokio::test(flavor = "multi_thread")]
async fn a_first_surface_matches_a_fresh_boot() {
    let assets = tempfile::tempdir().expect("a surface asset tree");
    let tree = Tree::holding(&document(""));
    write_surface_kind(&tree, assets.path(), "panel", "Panel");
    tree.write(&document(""));

    a_reload_matches_a_fresh_boot(
        &tree,
        panel_fixture(assets.path()),
        async |booted| {
            reload_onto(
                booted,
                &tree,
                &surface_document("deskbar", "panel", "Panel", ""),
            )
            .await;
            assert_eq!(
                booted.last_status().await.delta.surfaces_added,
                vec!["deskbar".to_string()]
            );
        },
        |reloaded| {
            assert_eq!(reloaded.surfaces.len(), 1, "{:?}", reloaded.surfaces);
            assert!(
                reloaded
                    .retained
                    .iter()
                    .any(|line| line.contains("surface.deskbar.bindings")),
                "{:?}",
                reloaded.retained
            );
            assert!(
                reloaded
                    .attach_budgets
                    .iter()
                    .any(|line| line.contains("deskbar")),
                "the arriving surface's principals hold send budgets, or the budget field is \
                 empty on both sides and compares nothing: {:?}",
                reloaded.attach_budgets,
            );
        },
    )
    .await;
}

/// **A surface whose declaration moved.** The one value edit the rig can make
/// without moving a channel: the runtime, the registration, the budgets and the
/// documents that carry the skin are all replaced, and everything else must be
/// where a fresh boot would have left it.
#[tokio::test(flavor = "multi_thread")]
async fn a_changed_surface_matches_a_fresh_boot() {
    let assets = tempfile::tempdir().expect("a surface asset tree");
    let tree = Tree::new();
    write_surface_kind(&tree, assets.path(), "panel", "Panel");
    tree.write(&surface_document("deskbar", "panel", "Panel", ""));

    a_reload_matches_a_fresh_boot(
        &tree,
        panel_fixture(assets.path()),
        async |booted| {
            reload_onto(
                booted,
                &tree,
                &surface_document("deskbar", "panel", "Panel", "    skin = \"foundry\";\n"),
            )
            .await;
            assert_eq!(
                booted.last_status().await.delta.surfaces_changed,
                vec!["deskbar".to_string()]
            );
        },
        |reloaded| {
            assert!(
                reloaded
                    .surfaces
                    .iter()
                    .any(|line| line.contains("foundry")),
                "the served runtime carries the new skin: {:?}",
                reloaded.surfaces
            );
        },
    )
    .await;
}

/// **The only surface removed.** The emptied document: the two
/// surface-description participants must hold no matcher for the retired slug
/// or its kind, the index must be back to `_none configured_`, and the runtime
/// table must be empty — none of it enumerated, all of it compared.
#[tokio::test(flavor = "multi_thread")]
async fn removing_the_only_surface_matches_a_fresh_boot() {
    let assets = tempfile::tempdir().expect("a surface asset tree");
    let tree = Tree::new();
    write_surface_kind(&tree, assets.path(), "panel", "Panel");
    tree.write(&surface_document("deskbar", "panel", "Panel", ""));

    a_reload_matches_a_fresh_boot(
        &tree,
        panel_fixture(assets.path()),
        async |booted| {
            // The emptied document: the surface and the description stamps
            // it derived leave together, which is the one change an operator
            // makes. A stamp left behind keeps its channels declared and
            // keeps whatever they last retained, which is the arrangement the
            // design admits rather than converges.
            reload_onto(booted, &tree, &document("")).await;
            assert_eq!(
                booted.last_status().await.delta.surfaces_removed,
                vec!["deskbar".to_string()]
            );
        },
        |reloaded| {
            assert!(reloaded.surfaces.is_empty(), "{:?}", reloaded.surfaces);
            assert!(
                reloaded
                    .registrations
                    .iter()
                    .any(|line| line.starts_with("System(\"surface-help\")")),
                "the publish-only participants are in the comparison, or the narrowing this \
                 transition exists for is compared against nothing: {:?}",
                reloaded.registrations,
            );
            assert!(
                reloaded
                    .retained
                    .iter()
                    .any(|line| line.starts_with("brenn:surface.index ")
                        && line.contains("_none configured_")),
                "{:?}",
                reloaded.retained
            );
        },
    )
    .await;
}

/// **A second surface of an existing kind arrives, and one of two leaves.** The
/// kind's help document lists every instance mounting it, so both directions
/// republish a document neither surface owns — and the surviving surface's own
/// documents must be exactly what a fresh boot writes.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_surface_of_a_kind_matches_a_fresh_boot() {
    let assets = tempfile::tempdir().expect("a surface asset tree");
    let tree = Tree::new();
    write_surface_kind(&tree, assets.path(), "panel", "Panel");
    let one = surfaces_document("panel", "Panel", &[("deskbar", "")]);
    let two = surfaces_document("panel", "Panel", &[("deskbar", ""), ("ticker", "")]);
    tree.write(&one);

    a_reload_matches_a_fresh_boot(
        &tree,
        panel_fixture(assets.path()),
        async |booted| {
            reload_onto(booted, &tree, &two).await;
            assert_eq!(
                booted.last_status().await.delta.surfaces_added,
                vec!["ticker".to_string()]
            );
        },
        |reloaded| assert_eq!(reloaded.surfaces.len(), 2, "{:?}", reloaded.surfaces),
    )
    .await;

    let tree = Tree::new();
    write_surface_kind(&tree, assets.path(), "panel", "Panel");
    tree.write(&two);

    a_reload_matches_a_fresh_boot(
        &tree,
        panel_fixture(assets.path()),
        async |booted| {
            reload_onto(booted, &tree, &one).await;
            assert_eq!(
                booted.last_status().await.delta.surfaces_removed,
                vec!["ticker".to_string()]
            );
        },
        |reloaded| assert_eq!(reloaded.surfaces.len(), 1, "{:?}", reloaded.surfaces),
    )
    .await;
}

/// **A kind upgraded under its mount, with the document untouched.** The bundle
/// install a reload is supposed to make possible: new artifact bytes under an
/// unmoved specification. Nothing in the text moved, so every field a fresh
/// boot reproduces has to be reproduced by the kind closure alone — the served
/// roots, the replaced runtime, and the documents the promotion rebuilt.
#[tokio::test(flavor = "multi_thread")]
async fn an_upgraded_kind_matches_a_fresh_boot() {
    let assets = tempfile::tempdir().expect("a surface asset tree");
    let tree = Tree::new();
    write_surface_kind(&tree, assets.path(), "panel", "Panel");
    tree.write(&surface_document("deskbar", "panel", "Panel", ""));
    // The specification the document was compiled against, carried into the
    // upgraded tree unchanged: a release that moved the spec too is the stale
    // -document refusal, which is a different case.
    let spec = std::fs::read(tree.modules().join("panel.brenn")).expect("the class module");

    a_reload_matches_a_fresh_boot(
        &tree,
        panel_fixture(assets.path()),
        async |booted| {
            brenn_surface_server::test_fixtures::write_processor_tree_from_bytes(
                assets.path(),
                "panel",
                UPGRADED_ARTIFACT,
                &spec,
                Vec::new(),
                true,
                |_| {},
            );
            booted.driver.reload(TriggerSource::Signal).await;
            assert_eq!(
                booted.last_status().await.delta.surfaces_changed,
                vec!["deskbar".to_string()]
            );
        },
        |reloaded| {
            let upgraded = brenn_lib::util::sha256_hex(UPGRADED_ARTIFACT);
            assert!(
                reloaded
                    .surface_roots
                    .iter()
                    .any(|line| line.contains(&upgraded)),
                "the served roots must carry the bytes the reload was decided against: {:?}",
                reloaded.surface_roots,
            );
        },
    )
    .await;
}

const UPGRADED_ARTIFACT: &[u8] = b"the-upgraded-artifact";

/// **A mount declared since boot, holding a module and a package.** The
/// deployment story the slice exists for: the operator installs a bundle, adds
/// its `mount` line, and the document reaches vocabulary and bytes the booted
/// process could not see. The consumer's package root is in the snapshot, so
/// "running out of the new mount" is compared rather than asserted.
#[tokio::test(flavor = "multi_thread")]
async fn a_mount_added_since_boot_matches_a_fresh_boot() {
    let bundle = tempfile::tempdir().expect("a bundle tree");
    let modules = bundle.path().join("modules");
    let components = bundle.path().join("components");
    std::fs::create_dir_all(&modules).expect("the bundle's module root");
    std::fs::create_dir_all(&components).expect("the bundle's components root");

    // The two halves by hand rather than through [`Tree::write`]'s fence: the
    // fence stages the module into the tree's own module root, and the whole
    // point of this transition is that the module arrives under the mount.
    let (module, root) = brenn_lib::config::split_packaged(&document_with_a_consumer())
        .expect("the consumer fixture is fenced");

    let tree = Tree::holding(&document(""));

    a_reload_matches_a_fresh_boot(
        &tree,
        |db| BootFixture {
            db: Some(db),
            ..BootFixture::default()
        },
        async |booted| {
            std::fs::write(
                modules.join(format!("{}.brenn", brenn_lib::config::PACKAGED_MODULE)),
                &module,
            )
            .expect("the bundle's module is writable");
            install_package(&components, &module);
            std::fs::write(tree.root(), &root).expect("the document is writable");
            booted.mounts.install(
                "bundle",
                &[
                    ("modules", modules.as_path()),
                    ("components", components.as_path()),
                ],
            );
            booted.driver.reload(TriggerSource::Signal).await;
            assert_eq!(
                booted.last_status().await.delta.consumers_added,
                vec!["sifter".to_string()]
            );
        },
        |reloaded| {
            assert!(
                reloaded
                    .running
                    .iter()
                    .any(|line| line.starts_with("sifter root=")
                        && line.ends_with("/bundle/components")),
                "the consumer runs out of the arriving mount: {:?}",
                reloaded.running,
            );
        },
    )
    .await;
}

/// A channel under a continuous publisher while its only consumer is retired.
///
/// The retired-key rule is what this exercises from the outside: a publish that
/// resolved the channel before the consumer's subscriber entry left holds a
/// snapshot that still names it, and the wake it raises arrives after the
/// binding became a tombstone. The rule says that wake is dropped; the
/// alternative — the panic a never-registered key gets — would take the process
/// down for a subscriber that merely left.
#[tokio::test(flavor = "multi_thread")]
async fn publishing_across_a_consumers_retirement_never_panics() {
    let components = tempfile::tempdir().expect("a components root");
    let tree = Tree::holding(&document_with_a_consumer());
    install_package(components.path(), &staged_module(&tree));
    let mut booted = boot(&tree, vec![components.path().to_path_buf()]).await;
    seat_a_conversation(&booted.db, 1).await;

    let stop = Arc::new(AtomicBool::new(false));
    let published = Arc::new(AtomicUsize::new(0));
    let publisher = tokio::spawn({
        let messenger = booted.messenger.clone();
        let stop = stop.clone();
        let published = published.clone();
        async move {
            // Uncapped and unpaced: the reload window is what has to be
            // covered, and a publisher that fell quiet inside it would leave
            // the retirement unraced while the test still passed. The
            // fixture's send budget and the work channel's send rate are
            // sized for this (see `boot_with` and `document`).
            while !stop.load(Ordering::Relaxed) {
                let outcome = publish_work(&messenger).await;
                assert!(
                    matches!(outcome, brenn_messaging::PublishResult::Ok { .. }),
                    "a publish to a declared channel must go through: {outcome:?}"
                );
                published.fetch_add(1, Ordering::Relaxed);
                tokio::task::yield_now().await;
            }
        }
    });

    tree.write(&document(""));
    let before = published.load(Ordering::Relaxed);
    booted.driver.reload(TriggerSource::Signal).await;
    let during = published.load(Ordering::Relaxed) - before;
    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(status.delta.consumers_removed, vec!["sifter".to_string()]);

    stop.store(true, Ordering::Relaxed);
    publisher.await.expect("the publisher survived the reload");
    assert!(
        during > 0,
        "publishes have to land inside the reload window for this to be a race \
         at all; the publisher made {during} of them there"
    );

    let kind = SubscriberEntryKind::Wasm("sifter".to_string());
    assert!(booted.driver.registry().is_empty());
    assert!(booted.messenger.subscriber_registration_retired(&kind));
    assert!(booted.router.delivery_binding_retired(&kind));

    // The channel outlives its consumer, and publishing to it is still an
    // ordinary publish — a channel does not care that nobody is subscribed.
    assert!(matches!(
        publish_work(&booted.messenger).await,
        brenn_messaging::PublishResult::Ok { .. }
    ));
}

/// A retune of a channel's `send_rate` takes the buckets already drawn against
/// it with it.
#[tokio::test(flavor = "multi_thread")]
async fn a_retune_takes_the_send_rate_buckets_with_it() {
    let components = tempfile::tempdir().expect("a components root");
    let tree = Tree::holding(&document_with_a_consumer());
    install_package(components.path(), &staged_module(&tree));
    let mut booted = boot_with(
        &tree,
        BootFixture {
            components_roots: vec![components.path().to_path_buf()],
            tool_registry: Some(async_tool_registry()),
            ..BootFixture::default()
        },
    )
    .await;
    let work = booted
        .messenger
        .directory()
        .resolve("brenn:work")
        .expect("the work channel is declared")
        .uuid;

    seat_a_conversation(&booted.db, 1).await;
    assert!(matches!(
        publish_work(&booted.messenger).await,
        brenn_messaging::PublishResult::Ok { .. }
    ));
    assert!(
        booted.messenger.send_rate_bucket_channels().contains(&work),
        "the publish drew a bucket, so there is something for the reload to evict",
    );

    tree.write(&document_with_a_consumer().replace("burst = 1000000", "burst = 900000"));
    install_package(components.path(), &staged_module(&tree));
    booted.driver.reload(TriggerSource::Signal).await;
    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(status.delta.channels_changed, vec!["brenn:work"]);

    assert!(
        !booted.messenger.send_rate_bucket_channels().contains(&work),
        "the retuned entry's buckets went with the entry: {:?}",
        booted.messenger.send_rate_bucket_channels(),
    );
}

/// One publish to the work channel, as the reader app.
async fn publish_work(messenger: &Arc<Messenger>) -> brenn_messaging::PublishResult {
    messenger
        .publish(
            brenn_messaging::PublishOrigin::Conversation { id: 1 },
            READER,
            "brenn:work",
            "tick",
            brenn_messaging::Urgency::Normal,
            None,
            None,
            None,
        )
        .await
}

/// A subscriber the plan cannot see, on a channel the candidate retunes.
///
/// The first convergibility rule answers for the subscribers boot folded onto
/// an entry; the second is for the ones that arrived since — an attach-minted
/// `Surface` entry, a dynamic app subscription, a session streaming from the
/// channel. Re-creating the entry would drop them, so the reload refuses and
/// names them.
#[tokio::test(flavor = "multi_thread")]
async fn a_subscriber_only_the_live_directory_holds_refuses_the_reload() {
    let tree = Tree::holding(&document(""));
    let mut booted = boot(&tree, Vec::new()).await;
    let booted_sha = booted.driver.baseline().document.document_sha256.clone();

    let work = booted
        .messenger
        .directory()
        .resolve("brenn:work")
        .expect("the work channel is declared");
    assert!(booted.messenger.directory().add_subscriber(
        &work.uuid,
        SubscriberEntry {
            kind: SubscriberEntryKind::Surface("wall".to_string()),
            push_depth: Depth::Bounded(4),
            retain_depth: Depth::Bounded(4),
            noise: NoiseLevel::Silent,
            wake_min: None,
        },
    ));

    // The one edit is the work channel's standing depth, which makes it a
    // changed entry — and a changed entry is remove-then-add, which is what the
    // attached surface would not survive.
    tree.write(&document("").replace("standing_retain_depth = 64;", "standing_retain_depth = 32;"));
    assert!(
        booted
            .driver
            .prepare_and_report(TriggerSource::Signal)
            .await
            .is_none()
    );

    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Refused);
    assert_eq!(status.refusals.len(), 1, "{:?}", status.refusals);
    assert!(
        status.refusals[0].contains("brenn:work")
            && status.refusals[0].contains("wall")
            && status.refusals[0].ends_with(super::NEEDS_RESTART),
        "{:?}",
        status.refusals
    );
    // Refused means untouched: the surface is still subscribed and the process
    // still projects what it booted.
    assert_eq!(
        booted.driver.baseline().document.document_sha256,
        booted_sha
    );
    let work = booted
        .messenger
        .directory()
        .resolve("brenn:work")
        .expect("the work channel is still declared");
    assert!(work.subscribers.iter().any(|subscriber| {
        matches!(&subscriber.kind, SubscriberEntryKind::Surface(slug) if slug == "wall")
    }));
}

/// The artifact moves and the document does not: a bundle installed under a
/// running consumer.
///
/// Nothing in the text changed, so the raw-document comparison and the
/// resolved-value comparison both say nothing happened. What makes this a
/// change is the package record the driver re-reads off the roots at plan time
/// — without it the process would keep executing bytes the roots no longer
/// hold, which is a running system no document describes.
#[tokio::test(flavor = "multi_thread")]
async fn an_artifact_that_moved_under_an_unmoved_document_is_a_changed_consumer() {
    let components = tempfile::tempdir().expect("a components root");
    let tree = Tree::holding(&document_with_a_consumer());
    let module = staged_module(&tree);
    let original = "brenn_processor_demo.wasm";
    let replacement = "brenn_processor_dual.wasm";
    let original_sha256 = install_package_from(components.path(), &module, original);
    let mut booted = boot(&tree, vec![components.path().to_path_buf()]).await;
    let booted_sha = booted.driver.baseline().document.document_sha256.clone();

    // The bundle install: same package name, same authored spec, different
    // bytes under it.
    let replacement_sha256 = install_package_from(components.path(), &module, replacement);

    booted.driver.reload(TriggerSource::Signal).await;

    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(status.delta.consumers_changed, vec!["sifter".to_string()]);
    assert!(status.delta.consumers_added.is_empty());
    assert!(status.delta.channels_added.is_empty());
    // The document never moved, so the identity the process reports is the one
    // it booted with.
    assert_eq!(status.document_sha256.as_deref(), Some(&*booted_sha));

    // What is in service is bound to the bytes now under the root — which is
    // the whole of the claim, and the one thing a delta computed off the text
    // alone could not have got right.
    let running = booted
        .driver
        .registry()
        .get("sifter")
        .expect("the replacement is in service");
    assert_eq!(running.verified.artifact_sha256, replacement_sha256);
    assert_ne!(running.verified.artifact_sha256, original_sha256);
    // The authored spec never moved with it: a bundle upgrade is new bytes
    // under an unmoved contract.
    assert_eq!(
        running.verified.spec_sha256.as_deref(),
        Some(&*brenn_lib::util::sha256_hex(module.as_bytes())),
    );
}

// ── The oracle over the MQTT ingress transitions ──────────────────────────

/// The rig every MQTT transition here boots on, ingress and client alike: a
/// components root for the consumer the bindings feed, and the plan-only MQTT
/// runtime, which registers a session per *declared* client and spawns no
/// connection — so both sides hold the whole registry, every SUBSCRIBE defers
/// and no packet moves. The wire versions are `mqtt_broker_tests`'.
fn components_fixture(
    components: &std::path::Path,
) -> impl Fn(brenn_db::Db) -> BootFixture + use<'_> {
    move |db| BootFixture {
        db: Some(db),
        components_roots: vec![components.to_path_buf()],
        ..BootFixture::default()
    }
}

/// **The first binding on a declared broker.** The running process has a
/// session with nothing subscribed on it and no ingress route anywhere; one
/// reload has to produce the filter, the route and the channel, and land where
/// a restart onto the same document would have.
#[tokio::test(flavor = "multi_thread")]
async fn a_first_mqtt_binding_matches_a_fresh_boot() {
    const TOPIC: &str = "home/state";
    let components = tempfile::tempdir().expect("a components root");
    let tree = Tree::holding(&document_with_a_broker_only());

    a_reload_matches_a_fresh_boot(
        &tree,
        components_fixture(components.path()),
        async |booted| {
            tree.write(&document_with_an_mqtt_consumer(&[TOPIC]));
            install_package(components.path(), &staged_module(&tree));
            booted.driver.reload(TriggerSource::Signal).await;
            assert_eq!(
                booted.last_status().await.delta.mqtt_subscribed,
                vec![format!("mqtt:ha:{TOPIC}")],
            );
        },
        |reloaded| {
            assert_eq!(
                reloaded.mqtt_filters().len(),
                1,
                "{:?}",
                reloaded.mqtt_filters()
            );
            assert_eq!(
                reloaded.mqtt_routes().len(),
                1,
                "{:?}",
                reloaded.mqtt_routes()
            );
        },
    )
    .await;
}

/// **The last binding on a declared broker leaving.** The filter and the route
/// go, and the session does not — which is only a convergence if a fresh boot of
/// the broker-only document has that session too, and is what the snapshot's
/// session set is in the comparison for.
#[tokio::test(flavor = "multi_thread")]
async fn a_last_mqtt_binding_leaving_matches_a_fresh_boot() {
    const TOPIC: &str = "home/state";
    let components = tempfile::tempdir().expect("a components root");
    let tree = Tree::holding(&document_with_an_mqtt_consumer(&[TOPIC]));
    install_package(components.path(), &staged_module(&tree));

    a_reload_matches_a_fresh_boot(
        &tree,
        components_fixture(components.path()),
        async |booted| {
            tree.write(&document_with_a_broker_only());
            booted.driver.reload(TriggerSource::Signal).await;
            assert_eq!(
                booted.last_status().await.delta.mqtt_unsubscribed,
                vec![format!("mqtt:ha:{TOPIC}")],
            );
        },
        |reloaded| {
            assert!(
                reloaded.mqtt_filters().is_empty(),
                "{:?}",
                reloaded.mqtt_filters()
            );
            assert!(
                reloaded.mqtt_routes().is_empty(),
                "{:?}",
                reloaded.mqtt_routes()
            );
            assert_eq!(
                reloaded.mqtt_sessions(),
                ["ha".to_string()],
                "the session outlives its last binding on both sides, or this transition \
                 compares nothing",
            );
        },
    )
    .await;
}

// ── The oracle over the `[[mqtt_client]]` transitions ─────────────────────

/// The two client sets every transition here moves between, and the bindings
/// that go with them: one broker, or two.
fn one_client() -> [(&'static str, u16, Option<&'static std::path::Path>); 1] {
    [("ha", 8883, None)]
}

fn two_clients() -> [(&'static str, u16, Option<&'static std::path::Path>); 2] {
    [("ha", 8883, None), ("spare", 8884, None)]
}

const ONE_BINDING: [(&str, &str); 1] = [("ha", "home/state")];
const TWO_BINDINGS: [(&str, &str); 2] = [("ha", "home/state"), ("spare", "home/other")];

/// **The deploy story on the MQTT side.** A `[[mqtt_client]]` the process does
/// not hold arrives: the session, its filters, the ingress route and the
/// channel all have to be what a fresh boot of the same document builds.
#[tokio::test(flavor = "multi_thread")]
async fn a_first_mqtt_client_matches_a_fresh_boot() {
    let components = tempfile::tempdir().expect("a components root");
    let tree = Tree::holding(&document_with_clients(&one_client(), &ONE_BINDING));
    install_package(components.path(), &staged_module(&tree));

    a_reload_matches_a_fresh_boot(
        &tree,
        components_fixture(components.path()),
        async |booted| {
            tree.write(&document_with_clients(&two_clients(), &TWO_BINDINGS));
            install_package(components.path(), &staged_module(&tree));
            booted.driver.reload(TriggerSource::Signal).await;
            assert_eq!(
                booted.last_status().await.delta.mqtt_clients_added,
                vec!["spare".to_string()],
            );
            booted.stop_mqtt();
        },
        |reloaded| {
            assert_eq!(
                reloaded.mqtt_sessions(),
                ["ha".to_string(), "spare".to_string()],
                "the arriving client is registered on both sides, or this transition compares \
                 nothing",
            );
            assert_eq!(
                reloaded.mqtt_filters().len(),
                2,
                "{:?}",
                reloaded.mqtt_filters(),
            );
        },
    )
    .await;
}

/// **A client leaving.** Its session, its filters and its route go, and what a
/// fresh boot of the shorter document holds is none of them.
#[tokio::test(flavor = "multi_thread")]
async fn a_removed_mqtt_client_matches_a_fresh_boot() {
    let components = tempfile::tempdir().expect("a components root");
    let tree = Tree::holding(&document_with_clients(&two_clients(), &TWO_BINDINGS));
    install_package(components.path(), &staged_module(&tree));

    a_reload_matches_a_fresh_boot(
        &tree,
        components_fixture(components.path()),
        async |booted| {
            tree.write(&document_with_clients(&one_client(), &ONE_BINDING));
            install_package(components.path(), &staged_module(&tree));
            booted.driver.reload(TriggerSource::Signal).await;
            assert_eq!(
                booted.last_status().await.delta.mqtt_clients_removed,
                vec!["spare".to_string()],
            );
        },
        |reloaded| {
            assert_eq!(
                reloaded.mqtt_sessions(),
                ["ha".to_string()],
                "the departing client is gone on both sides, or this transition compares nothing",
            );
        },
    )
    .await;
}

/// **An identity field moving under an unmoved slug.** The client dials a
/// different broker port, so its supervisor is restarted; the registry holds
/// the same slug it always did, and what it is connected as has to be the
/// successor's — which only the config field of the snapshot can see.
#[tokio::test(flavor = "multi_thread")]
async fn a_changed_mqtt_client_matches_a_fresh_boot() {
    let components = tempfile::tempdir().expect("a components root");
    let tree = Tree::holding(&document_with_clients(&one_client(), &ONE_BINDING));
    install_package(components.path(), &staged_module(&tree));

    a_reload_matches_a_fresh_boot(
        &tree,
        components_fixture(components.path()),
        async |booted| {
            let before = snapshot(booted).await.mqtt_client_configs().to_vec();
            tree.write(&document_with_clients(&[("ha", 8885, None)], &ONE_BINDING));
            booted.driver.reload(TriggerSource::Signal).await;
            assert_eq!(
                booted.last_status().await.delta.mqtt_clients_changed,
                vec!["ha".to_string()],
            );
            assert_ne!(
                snapshot(booted).await.mqtt_client_configs(),
                before.as_slice(),
                "the edit moved the registered config, or this transition compares nothing",
            );
            booted.stop_mqtt();
        },
        |reloaded| {
            assert_eq!(
                reloaded.mqtt_sessions(),
                ["ha".to_string()],
                "a restarted client is never absent from the registry",
            );
            assert_eq!(
                reloaded.mqtt_filters().len(),
                1,
                "the successor carries the predecessor's filters: {:?}",
                reloaded.mqtt_filters(),
            );
        },
    )
    .await;
}

/// **A rotated broker password under an unmoved document.** The bytes on disk
/// are part of the resolved client, so the reload has to be connected as the
/// new ones — which is only a convergence if a fresh boot of the same unmoved
/// document is too, and is what the config field's password digest is in the
/// comparison for.
#[tokio::test(flavor = "multi_thread")]
async fn a_rotated_broker_password_matches_a_fresh_boot() {
    let components = tempfile::tempdir().expect("a components root");
    // The secret is written before the document that names it: it has to be
    // readable at both boots as well as at the reload.
    let tree = Tree::new();
    let password = tree.secret("broker.pw", "before");
    let clients = [("ha", 8883u16, Some(password.as_path()))];
    let document = document_with_clients(&clients, &ONE_BINDING);
    tree.write(&document);
    install_package(components.path(), &staged_module(&tree));

    a_reload_matches_a_fresh_boot(
        &tree,
        components_fixture(components.path()),
        async |booted| {
            let before = snapshot(booted).await.mqtt_client_configs().to_vec();
            std::fs::write(&password, "after").expect("rotate the secret");
            tree.write(&document);
            booted.driver.reload(TriggerSource::Signal).await;
            assert_eq!(
                booted.last_status().await.delta.mqtt_clients_changed,
                vec!["ha".to_string()],
            );
            assert_ne!(
                snapshot(booted).await.mqtt_client_configs(),
                before.as_slice(),
                "the rotation moved the registered config, or this transition compares nothing",
            );
            booted.stop_mqtt();
        },
        |reloaded| {
            assert_eq!(reloaded.mqtt_sessions(), ["ha".to_string()]);
        },
    )
    .await;
}

// ── The oracle over the webhook transitions ───────────────────────────────

/// The rig every webhook transition boots on: a database per process. A bearer
/// endpoint needs no mount and no components root; its secret files live under
/// the document tree, which both sides read.
fn webhook_fixture(db: brenn_db::Db) -> BootFixture {
    BootFixture {
        db: Some(db),
        ..BootFixture::default()
    }
}

/// **The deploy story.** A first endpoint arrives: the table entry, the channel
/// it mints and the owner folded onto it all have to be what a fresh boot of
/// the same document builds — including the stamped owner, which boot derives
/// from the agent's `subscribe` line and the reload derives from the
/// candidate's.
#[tokio::test(flavor = "multi_thread")]
async fn a_first_webhook_endpoint_matches_a_fresh_boot() {
    let tree = Tree::holding(&document_with_webhooks("", &[]));
    let secret = tree.secret("inbox.token", "s3cret");

    a_reload_matches_a_fresh_boot(
        &tree,
        webhook_fixture,
        async |booted| {
            tree.write(&document_with_webhooks(
                &bearer_endpoint("inbox", "/webhooks/inbox", &secret),
                &["inbox"],
            ));
            booted.driver.reload(TriggerSource::Signal).await;
            assert_eq!(
                booted.last_status().await.delta.webhook_endpoints_added,
                vec!["inbox".to_string()],
            );
        },
        |reloaded| {
            assert_eq!(
                reloaded.webhook_endpoints().len(),
                1,
                "the endpoint is served on both sides, or this transition compares nothing: \
                 {:?}",
                reloaded.webhook_endpoints(),
            );
        },
    )
    .await;
}

/// **An endpoint leaving.** The entry goes out of the table and its channel out
/// of the directory, and what a fresh boot of the shorter document holds is
/// neither.
#[tokio::test(flavor = "multi_thread")]
async fn a_removed_webhook_endpoint_matches_a_fresh_boot() {
    let tree = Tree::new();
    let secret = tree.secret("inbox.token", "s3cret");
    tree.write(&document_with_webhooks(
        &bearer_endpoint("inbox", "/webhooks/inbox", &secret),
        &["inbox"],
    ));

    a_reload_matches_a_fresh_boot(
        &tree,
        webhook_fixture,
        async |booted| {
            tree.write(&document_with_webhooks("", &[]));
            booted.driver.reload(TriggerSource::Signal).await;
            assert_eq!(
                booted.last_status().await.delta.webhook_endpoints_removed,
                vec!["inbox".to_string()],
            );
        },
        |reloaded| {
            assert!(
                reloaded.webhook_endpoints().is_empty(),
                "{:?}",
                reloaded.webhook_endpoints(),
            );
        },
    )
    .await;
}

/// **A rotated secret under an unmoved document.** The bytes on disk are the
/// entity, so the reload has to be serving the new ones — which is only a
/// convergence if a fresh boot of the same unmoved document serves them too,
/// and is what the endpoint field's scheme digest is in the comparison for.
#[tokio::test(flavor = "multi_thread")]
async fn a_rotated_webhook_secret_matches_a_fresh_boot() {
    let tree = Tree::new();
    let secret = tree.secret("inbox.token", "before");
    let document = document_with_webhooks(
        &bearer_endpoint("inbox", "/webhooks/inbox", &secret),
        &["inbox"],
    );
    tree.write(&document);

    a_reload_matches_a_fresh_boot(
        &tree,
        webhook_fixture,
        async |booted| {
            let before = snapshot(booted).await.webhook_endpoints().to_vec();
            std::fs::write(&secret, "after").expect("rotate the secret");
            tree.write(&document);
            booted.driver.reload(TriggerSource::Signal).await;
            assert_eq!(
                booted.last_status().await.delta.webhook_endpoints_changed,
                vec!["inbox".to_string()],
            );
            assert_ne!(
                snapshot(booted).await.webhook_endpoints(),
                before.as_slice(),
                "the rotation moved the served entry, or this transition compares nothing",
            );
        },
        |reloaded| {
            assert_eq!(reloaded.webhook_endpoints().len(), 1);
        },
    )
    .await;
}

// ── The oracle over the agent transitions ─────────────────────────────────

/// The rig every agent transition boots on: a database per process and
/// nothing else. An agent needs no mount, no asset tree and no broker.
fn agent_fixture(db: brenn_db::Db) -> BootFixture {
    BootFixture {
        db: Some(db),
        ..BootFixture::default()
    }
}

/// **The motivating shape.** The agent's `acl subscribe` is widened and a
/// `subscribe` line added: the entry has to appear on the live channel with
/// the candidate's depths, and the swapped table has to carry the widened
/// policy every gate reads per call.
#[tokio::test(flavor = "multi_thread")]
async fn an_acl_widened_and_a_subscription_added_matches_a_fresh_boot() {
    let tree = Tree::holding(&document_subscribing("", &[]));

    a_reload_matches_a_fresh_boot(
        &tree,
        agent_fixture,
        async |booted| {
            reload_onto(booted, &tree, &document_subscribing("", &["work"])).await;
            assert_eq!(
                booted.last_status().await.delta.subscriptions_added,
                vec![format!("{READER} brenn:work")],
            );
        },
        |reloaded| {
            assert!(
                reloaded
                    .channels
                    .iter()
                    .any(|line| line.starts_with("brenn:work ") && line.contains(READER)),
                "the agent's entry is on the channel it now reads: {:?}",
                reloaded.channels,
            );
            assert!(
                reloaded
                    .agent_authority()
                    .iter()
                    .any(|line| line.contains("brenn:work")),
                "and the swapped table carries the widened authority: {:?}",
                reloaded.agent_authority(),
            );
        },
    )
    .await;
}

/// **The agent's whole `messaging` block removed.** Its send budget and every
/// `subscribe` line go at once, so the candidate map holds `None` where the
/// booted one held a resolved config, and every static entry leaves the
/// directory.
#[tokio::test(flavor = "multi_thread")]
async fn an_agents_messaging_block_removed_matches_a_fresh_boot() {
    let tree = Tree::holding(&document_subscribing("", &["work"]));
    let unconfigured = document_subscribing("", &[]).replace("    send_budget = 1000000;\n", "");

    a_reload_matches_a_fresh_boot(
        &tree,
        agent_fixture,
        async |booted| {
            reload_onto(booted, &tree, &unconfigured).await;
            assert_eq!(
                booted.last_status().await.delta.subscriptions_removed,
                vec![format!("{READER} brenn:work")],
            );
        },
        |reloaded| {
            assert!(
                reloaded
                    .channels
                    .iter()
                    .any(|line| line.starts_with("brenn:work ") && !line.contains(READER)),
                "the entry left the channel: {:?}",
                reloaded.channels,
            );
        },
    )
    .await;
}

/// **A channel retuned under a subscribed agent.** The channel departs and
/// arrives again, and the agent is promoted by closure so that its entry comes
/// back with it.
#[tokio::test(flavor = "multi_thread")]
async fn a_retune_under_a_subscribed_agent_matches_a_fresh_boot() {
    let tree = Tree::holding(&document_subscribing("", &["work"]));
    let retuned = document_subscribing("", &["work"]).replace(
        "    standing_retain_depth = 64;",
        "    standing_retain_depth = 32;",
    );

    a_reload_matches_a_fresh_boot(
        &tree,
        agent_fixture,
        async |booted| {
            reload_onto(booted, &tree, &retuned).await;
            let status = booted.last_status().await;
            assert_eq!(
                status.delta.channels_changed,
                vec!["brenn:work".to_string()]
            );
            assert_eq!(status.delta.agents_changed, vec![READER.to_string()]);
        },
        |reloaded| {
            assert!(
                reloaded
                    .channels
                    .iter()
                    .any(|line| line.starts_with("brenn:work ") && line.contains(READER)),
                "the agent came back with the channel: {:?}",
                reloaded.channels,
            );
        },
    )
    .await;
}

/// **A grant that moves the rendered tool list.** The reload stages the
/// candidate's rendering and renames it onto the live path at the swap; a
/// fresh boot writes the same bytes at startup. An agent whose file never
/// landed holds the old list here.
#[tokio::test(flavor = "multi_thread")]
async fn a_tool_grant_added_matches_a_fresh_boot() {
    // A declared broker, so the `client` matcher that derives the MQTT publish
    // grant — and with it the tool — names a client both documents hold.
    const BROKER: &str =
        "mqtt_client ha {\n    url = \"mqtts://127.0.0.1:8883\";\n    qos = 1;\n}\n";
    let tree = Tree::holding(&document(BROKER));
    let granted = document(BROKER).replace(
        "acl publish [exact reload_requests, exact work];",
        "acl publish [exact reload_requests, exact work, client \"mqtt:ha\"];",
    );

    a_reload_matches_a_fresh_boot(
        &tree,
        agent_fixture,
        async |booted| {
            reload_onto(booted, &tree, &granted).await;
            assert_eq!(
                booted.last_status().await.delta.agents_changed,
                vec![READER.to_string()],
            );
        },
        |reloaded| {
            assert!(
                reloaded
                    .virtual_tools()
                    .iter()
                    .any(|line| line.contains("MqttSend")),
                "the renamed rendering lists the tool the grant added: {:?}",
                reloaded.virtual_tools(),
            );
        },
    )
    .await;
}

/// **A first push-enabled `subscribe` on an agent that had none.** The commit
/// mints the agent's singleton conversation, provisions that conversation's
/// chat channel family into the live directory and gives it a position — the
/// sequence boot runs for the same entry, compared against boot running it.
#[tokio::test(flavor = "multi_thread")]
async fn a_first_push_subscription_matches_a_fresh_boot() {
    let tree = Tree::holding(&document_push_subscribing("", &["alice"], &[]));

    a_seeded_reload_matches_a_fresh_boot(
        &tree,
        agent_fixture,
        // The owner's row, so both processes resolve the same owner.
        // Without it `allowed_users.first()` finds no matching user and
        // the conversation is never minted.
        async |booted| {
            seat_user(&booted.db, "alice").await;
        },
        async |booted| {
            reload_onto(
                booted,
                &tree,
                &document_push_subscribing("", &["alice"], &["work"]),
            )
            .await;
            assert!(
                conversation_of(booted, READER).await.is_some(),
                "the attach minted the agent's conversation",
            );
        },
        |reloaded| {
            assert!(
                reloaded
                    .channels
                    .iter()
                    .any(|line| line.contains(&format!("chat.app.{READER}.in."))),
                "the conversation's chat family is in the directory: {:?}",
                reloaded.channels,
            );
            assert!(
                reloaded
                    .rows
                    .iter()
                    .any(|line| line.starts_with("cursor ") && line.contains("on brenn:work")),
                "and it holds a position on the channel it now reads: {:?}",
                reloaded.rows,
            );
        },
    )
    .await;
}

/// **An ACL narrowed under a live dynamic subscription.** The entry is folded
/// out and the durable row kept — the dormant state a fresh boot's merge puts
/// the same row in.
#[tokio::test(flavor = "multi_thread")]
async fn an_acl_narrowed_under_a_dynamic_row_matches_a_fresh_boot() {
    let tree = Tree::holding(&document_covering_work(""));

    a_seeded_reload_matches_a_fresh_boot(
        &tree,
        agent_fixture,
        async |booted| {
            insert_dynamic_row(booted, "brenn:work", true).await;
        },
        async |booted| {
            reload_onto(booted, &tree, &document("")).await;
            assert_eq!(
                booted.last_status().await.delta.dynamic_revoked,
                vec![format!("{READER} brenn:work")],
            );
        },
        |reloaded| {
            assert_eq!(
                reloaded.dynamic_subs().len(),
                1,
                "the row is kept, which is what dormancy is: {:?}",
                reloaded.dynamic_subs(),
            );
            assert!(
                reloaded
                    .channels
                    .iter()
                    .any(|line| line.starts_with("brenn:work ") && !line.contains(READER)),
                "and folded out of the directory: {:?}",
                reloaded.channels,
            );
        },
    )
    .await;
}

/// **An ACL widened over a dormant row.** The row is folded back in at its own
/// depths, which is what the boot merge does with the same row.
#[tokio::test(flavor = "multi_thread")]
async fn an_acl_widened_over_a_dormant_row_matches_a_fresh_boot() {
    let tree = Tree::holding(&document(""));

    a_seeded_reload_matches_a_fresh_boot(
        &tree,
        agent_fixture,
        // Dormant: the row exists but the entry is not in the directory.
        async |booted| {
            insert_dynamic_row(booted, "brenn:work", false).await;
        },
        async |booted| {
            reload_onto(booted, &tree, &document_covering_work("")).await;
            assert_eq!(
                booted.last_status().await.delta.dynamic_revived,
                vec![format!("{READER} brenn:work")],
            );
        },
        |reloaded| {
            assert!(
                reloaded
                    .channels
                    .iter()
                    .any(|line| line.starts_with("brenn:work ") && line.contains(READER)),
                "the row is folded back onto the channel: {:?}",
                reloaded.channels,
            );
        },
    )
    .await;
}

/// **A static `subscribe` declared where the agent holds a dynamic row.**
/// Static config wins: the row is deleted and the static entry takes its
/// place.
#[tokio::test(flavor = "multi_thread")]
async fn a_static_subscription_over_a_dynamic_row_matches_a_fresh_boot() {
    let tree = Tree::holding(&document_covering_work(""));

    a_seeded_reload_matches_a_fresh_boot(
        &tree,
        agent_fixture,
        async |booted| {
            insert_dynamic_row(booted, "brenn:work", true).await;
        },
        async |booted| {
            reload_onto(booted, &tree, &document_subscribing("", &["work"])).await;
            assert_eq!(
                booted.last_status().await.delta.dynamic_pruned,
                vec![format!("{READER} brenn:work")],
            );
        },
        |reloaded| {
            assert!(
                reloaded.dynamic_subs().is_empty(),
                "the row is gone: {:?}",
                reloaded.dynamic_subs(),
            );
            assert!(
                reloaded
                    .channels
                    .iter()
                    .any(|line| line.starts_with("brenn:work ") && line.contains(READER)),
                "and the static entry took its place: {:?}",
                reloaded.channels,
            );
        },
    )
    .await;
}

/// **A dormant row on a channel the candidate removes.** The two-step
/// retirement of a channel an agent subscribed to dynamically: the ACL is
/// re-granted and the `[[channel]]` block dropped in one document, and the row
/// must be left exactly where a fresh boot leaves it — dormant, with its
/// position, and no directory entry for the address.
///
/// The position is seeded along with the row. Without it the `rows` view
/// compares an empty position set on both sides and the transition proves
/// nothing about the cursor, which is the state the carve-out is really about:
/// a fresh boot's reconcile keeps the position only because the dormant row
/// justifies it.
#[tokio::test(flavor = "multi_thread")]
async fn a_dormant_row_on_a_removed_channel_matches_a_fresh_boot() {
    let tree = Tree::holding(&document_push_subscribing_acl(
        &spill_channel(4),
        &["alice"],
        &["work"],
        &[],
        &[],
    ));

    a_seeded_reload_matches_a_fresh_boot(
        &tree,
        agent_fixture,
        async |booted| {
            seat_user(&booted.db, "alice").await;
            // The conversation both `app_conversation` calls resolve through —
            // the reload's and the fresh boot's reconcile.
            booted
                .messenger
                .attach_conversation("brenn:work", READER, Depth::Bounded(1))
                .await;
            let conversation = conversation_of(booted, READER)
                .await
                .expect("alice's conversation");
            let uuid = insert_dynamic_row(booted, "brenn:spill", false).await;
            seat_position(
                booted,
                uuid,
                &brenn_lib::messaging::ParticipantId::for_conversation(conversation),
            )
            .await;
        },
        async |booted| {
            reload_onto(
                booted,
                &tree,
                &document_push_subscribing_acl(
                    "",
                    &["alice"],
                    &["work"],
                    &[],
                    &[SPILL_ACL_BY_ADDRESS],
                ),
            )
            .await;
        },
        |reloaded| {
            assert_eq!(
                reloaded.dynamic_subs().len(),
                1,
                "the row is still there: {:?}",
                reloaded.dynamic_subs(),
            );
            assert!(
                !reloaded
                    .channels
                    .iter()
                    .any(|line| line.starts_with("brenn:spill ")),
                "and the channel it names is not in the directory: {:?}",
                reloaded.channels,
            );
            assert!(
                reloaded
                    .rows
                    .iter()
                    .any(|line| line.starts_with("cursor ") && line.contains("on brenn:spill")),
                "while the position it resumes from is: {:?}",
                reloaded.rows,
            );
        },
    )
    .await;
}

/// **An owner change on a push-subscribed singleton.** The owner is
/// `allowed_users.first()`, and every position the agent's channels hold is
/// held under that owner's singleton conversation — so moving it mints the new
/// owner's conversation with positions, and leaves the old owner's positions
/// unjustified. A fresh boot's `reconcile_subscriber_cursors` deletes those;
/// the reload has to delete them too, while leaving the old owner's
/// conversation and chat family, which their own entries justify.
#[tokio::test(flavor = "multi_thread")]
async fn an_owner_change_matches_a_fresh_boot() {
    let tree = Tree::holding(&document_push_subscribing("", &["alice"], &["work"]));

    a_seeded_reload_matches_a_fresh_boot(
        &tree,
        agent_fixture,
        // Both owners' rows, and alice's conversation with its position: the
        // starting state both processes have to walk away from.
        async |booted| {
            seat_user(&booted.db, "alice").await;
            seat_user(&booted.db, "bob").await;
            booted
                .messenger
                .attach_conversation("brenn:work", READER, Depth::Bounded(1))
                .await;
        },
        async |booted| {
            reload_onto(
                booted,
                &tree,
                &document_push_subscribing("", &["bob"], &["work"]),
            )
            .await;
            assert_eq!(
                booted.last_status().await.delta.agents_changed,
                vec![READER.to_string()],
            );
        },
        |reloaded| {
            let positions: Vec<&String> = reloaded
                .rows
                .iter()
                .filter(|line| line.starts_with("cursor ") && line.contains("on brenn:work"))
                .collect();
            assert_eq!(
                positions.len(),
                1,
                "one conversation holds the channel's position, the new owner's: {:?}",
                reloaded.rows,
            );
            assert_eq!(
                reloaded
                    .channels
                    .iter()
                    .filter(|line| line.contains(&format!("chat.app.{READER}.in.")))
                    .count(),
                2,
                "both owners' chat families are still in the directory: {:?}",
                reloaded.channels,
            );
        },
    )
    .await;
}

/// **The same with a live push-enabled dynamic row.** A fresh boot seats the
/// new owner after the dynamic merge, so it is positioned on the dynamic
/// channel too — which a reload seating it off the candidate plan's static
/// entries alone would miss.
#[tokio::test(flavor = "multi_thread")]
async fn an_owner_change_under_a_dynamic_row_matches_a_fresh_boot() {
    let tree = Tree::holding(&push_owner_covering_work(&["alice"]));

    a_seeded_reload_matches_a_fresh_boot(
        &tree,
        agent_fixture,
        async |booted| {
            seat_user(&booted.db, "alice").await;
            seat_user(&booted.db, "bob").await;
            insert_push_dynamic_row(booted, "brenn:work").await;
            booted
                .messenger
                .attach_conversation("brenn:work", READER, Depth::Bounded(1))
                .await;
        },
        async |booted| {
            reload_onto(booted, &tree, &push_owner_covering_work(&["bob"])).await;
            assert_eq!(
                booted.last_status().await.delta.agents_changed,
                vec![READER.to_string()],
            );
        },
        |reloaded| {
            assert_eq!(
                reloaded.dynamic_subs().len(),
                1,
                "the row is untouched: what moved is whose conversation holds its position: {:?}",
                reloaded.dynamic_subs(),
            );
            assert_eq!(
                reloaded
                    .rows
                    .iter()
                    .filter(|line| line.starts_with("cursor ") && line.contains("on brenn:work"))
                    .count(),
                1,
                "and the new owner holds it: {:?}",
                reloaded.rows,
            );
        },
    )
    .await;
}

/// **A class-B edit.** `model` is what a Claude Code process is spawned with,
/// so the swapped table has to carry it for the next spawn while nothing else
/// about the process moves.
#[tokio::test(flavor = "multi_thread")]
async fn a_per_process_edit_matches_a_fresh_boot() {
    let tree = Tree::holding(&document(""));
    let remodelled = document("").replace(
        "    working_dir = \".\";",
        "    working_dir = \".\";\n    model = \"opus\";",
    );

    a_reload_matches_a_fresh_boot(
        &tree,
        agent_fixture,
        async |booted| {
            reload_onto(booted, &tree, &remodelled).await;
            assert_eq!(
                booted.last_status().await.delta.agents_changed,
                vec![READER.to_string()],
            );
        },
        |reloaded| {
            assert!(
                reloaded
                    .agent_spawn()
                    .iter()
                    .any(|line| line.contains("model: \"opus\"")),
                "the swapped table carries what the next spawn reads: {:?}",
                reloaded.agent_spawn(),
            );
        },
    )
    .await;
}

/// **A class-A edit.** `icon` is read off the map per request, so the reload's
/// only work is the swap — and a reload that skipped it serves the old value
/// where a fresh boot serves the new.
#[tokio::test(flavor = "multi_thread")]
async fn a_per_call_edit_matches_a_fresh_boot() {
    let tree = Tree::holding(&document(""));
    let redecorated = document("").replace(
        "    working_dir = \".\";",
        "    working_dir = \".\";\n    icon = \"telescope\";",
    );

    a_reload_matches_a_fresh_boot(
        &tree,
        agent_fixture,
        async |booted| {
            reload_onto(booted, &tree, &redecorated).await;
            assert_eq!(
                booted.last_status().await.delta.agents_changed,
                vec![READER.to_string()],
            );
        },
        |reloaded| {
            assert!(
                reloaded
                    .agent_per_call
                    .iter()
                    .any(|line| line.contains("telescope")),
                "the swapped table carries what the route reads: {:?}",
                reloaded.agent_per_call,
            );
        },
    )
    .await;
}

// ── The oracle over a config-carrying mount ───────────────────────────────

/// The rig every fragment transition boots on: a document tree with the
/// packaged class a fragment instantiates, and the components root its package
/// is installed into. The caller holds both — dropping either takes the mount
/// out from under both processes.
fn fragment_fixture(
    components: &std::path::Path,
) -> impl Fn(brenn_db::Db) -> BootFixture + use<'_> {
    let roots = vec![components.to_path_buf()];
    move |db| BootFixture {
        db: Some(db),
        components_roots: roots.clone(),
        ..BootFixture::default()
    }
}

/// **A config-carrying mount arrives.** Nothing in the running process came
/// from a mount's own document, and one reload has to produce a channel and a
/// consumer that did — under the mount's name, bounded by the ceiling the
/// operator wrote in the same edit.
///
/// The fresh side re-derives the mounts from the same directory, so what it
/// boots is the mounts document as it stands after the transition: the oracle
/// statement holds over the mounts document and not only over the root.
#[tokio::test(flavor = "multi_thread")]
async fn a_fragment_arriving_matches_a_fresh_boot() {
    let components = tempfile::tempdir().expect("a components root");
    let tree = Tree::holding(&document_with(""));
    install_package(components.path(), &staged_module(&tree));

    a_reload_matches_a_fresh_boot(
        &tree,
        fragment_fixture(components.path()),
        async |booted| {
            restage(&tree, &components, &document_with(CEILING));
            booted
                .mounts
                .config(CONFIG_MOUNT, "automator", &fragment(4));
            booted.driver.reload(TriggerSource::Signal).await;
        },
        |reloaded| {
            assert!(
                reloaded
                    .channels
                    .iter()
                    .any(|line| line.starts_with(&format!("{FRAGMENT_ADDRESS} "))),
                "{:?}",
                reloaded.channels
            );
            assert!(
                reloaded
                    .running
                    .iter()
                    .any(|line| line.starts_with(FRAGMENT_CONSUMER)),
                "{:?}",
                reloaded.running
            );
        },
    )
    .await;
}

/// **A fragment is edited.** The mounts document does not move and the root
/// does not move; the only bytes that change are the mount author's, and the
/// process has to end up where a restart would have put it.
#[tokio::test(flavor = "multi_thread")]
async fn a_fragment_edit_matches_a_fresh_boot() {
    let components = tempfile::tempdir().expect("a components root");
    let tree = Tree::holding(&document_with(CEILING));
    install_package(components.path(), &staged_module(&tree));
    tree.mounts(&[components.path().to_path_buf()])
        .config(CONFIG_MOUNT, "automator", &fragment(4));

    a_reload_matches_a_fresh_boot(
        &tree,
        fragment_fixture(components.path()),
        async |booted| {
            booted.mounts.edit(CONFIG_MOUNT, &fragment(16));
            booted.driver.reload(TriggerSource::Signal).await;
            assert_eq!(
                booted.last_status().await.delta.channels_changed,
                vec![FRAGMENT_ADDRESS.to_string()]
            );
        },
        |reloaded| {
            assert!(
                reloaded
                    .channels
                    .iter()
                    .any(|line| line.starts_with(&format!("{FRAGMENT_ADDRESS} "))),
                "{:?}",
                reloaded.channels
            );
        },
    )
    .await;
}

/// **A config-carrying mount is withdrawn.** The operator takes the mount line
/// and its ceiling out together, and the process has to retire what the
/// fragment declared as completely as a boot without it would never have had
/// it.
#[tokio::test(flavor = "multi_thread")]
async fn a_fragment_leaving_matches_a_fresh_boot() {
    let components = tempfile::tempdir().expect("a components root");
    let tree = Tree::holding(&document_with(CEILING));
    install_package(components.path(), &staged_module(&tree));
    tree.mounts(&[components.path().to_path_buf()])
        .config(CONFIG_MOUNT, "automator", &fragment(4));

    a_reload_matches_a_fresh_boot(
        &tree,
        fragment_fixture(components.path()),
        async |booted| {
            restage(&tree, &components, &document_with(""));
            booted.mounts.uninstall(CONFIG_MOUNT);
            booted.mounts.write();
            booted.driver.reload(TriggerSource::Signal).await;
            assert_eq!(
                booted.last_status().await.delta.consumers_removed,
                vec![FRAGMENT_CONSUMER.to_string()]
            );
        },
        |reloaded| {
            assert!(
                !reloaded
                    .running
                    .iter()
                    .any(|line| line.starts_with(FRAGMENT_CONSUMER)),
                "{:?}",
                reloaded.running
            );
        },
    )
    .await;
}
