//! Level 1: everything a reload cannot converge must be equal.
//!
//! Four blocks of a document are convergible — `channels`, `links`,
//! `wasm_consumers` and `surfaces` — and this pass ignores exactly those. Every
//! other section describes an entity whose runtime tables are boot snapshots:
//! a remote's token is loaded once, an MQTT client's broker session is opened
//! once for every declared client, a webhook endpoint's route is an axum path
//! built once. Converging any of them is a later slice's work; a difference in
//! one of them here is a refusal.
//!
//! An `agent` block is compared field by field rather than whole, because its
//! fields converge in three different ways:
//!
//! - **authority** (`grants`, `acl`, `tool_grants`, `messaging`,
//!   `mqtt_subscriptions`) — every gate reads it per call, so it converges the
//!   instant the resolved map is swapped. This pass ignores these fields
//!   entirely: what decides whether the agent's authority moved is the
//!   comparison of the *resolved* `AppAuthority`, which is why a re-spelled
//!   grant or a reordered ACL is not a change.
//! - **per-call** (`name`, `icon`, `allowed_users`, `models`, …) — read from
//!   the map on each request, so the same swap converges them. A difference
//!   sets `per_call_changed`, which is the only thing that puts an agent whose
//!   authority did not move into the delta; without it the commit that
//!   performs the swap would never run.
//! - **per-process** (`model`, `mcp_servers`, `working_dir`, `approval_rules`,
//!   compaction, …) — baked into a Claude Code process at spawn. A difference
//!   sets `spawn_changed`, and the agent's live sessions are retired at their
//!   next idle moment so their successors are spawned from the new map.
//!
//! What is left is boot-shaped: folded into another subsystem's tables or side
//! effects, so converging it means converging that subsystem. Those fields are
//! refused by name, each arm carrying its reason.
//!
//! The comparison is over *loaded* configs rather than document text, so
//! defaults are applied, key order is gone, and a section rewritten into a
//! different assembly that lowers to the same value is not a difference.
//! Collections whose order the runtime ignores are sorted first, the way
//! `config-diff` sorts them, so a reordered ACL list is not a difference
//! either.

use std::collections::BTreeMap;

use brenn_lib::config::{AppConfigRaw, BrennConfig, sort_order_dead_collections};

use super::NEEDS_RESTART;

/// Which of an agent's convergible field classes moved between the two
/// documents. Neither flag set means every difference the agent has is in its
/// authority fields, which this pass does not compare.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AppFieldDiff {
    /// A field every request reads off the map differs. The swap converges it;
    /// nothing else is owed.
    pub(crate) per_call_changed: bool,
    /// A field a Claude Code process is spawned with differs. The agent's live
    /// sessions hold a stale view and are retired at idle.
    pub(crate) spawn_changed: bool,
}

impl AppFieldDiff {
    /// Whether either class moved, i.e. whether this agent belongs in the map
    /// the pass returns.
    fn moved(self) -> bool {
        self.per_call_changed || self.spawn_changed
    }
}

/// What level 1 has to say about a candidate document.
pub(crate) struct LevelOne {
    /// Every non-convergible difference, as refusal lines. Empty means level 1
    /// passed.
    pub(crate) refusals: Vec<String>,
    /// Per agent, which convergible field classes moved. Only agents with at
    /// least one flag set are present, and the map is meaningless when
    /// `refusals` is non-empty (the pass stops at no field).
    pub(crate) app_diffs: BTreeMap<String, AppFieldDiff>,
}

/// Compare the running document with a candidate.
///
/// Both sides are cloned and normalized before anything is compared: the
/// caller's baseline is the document the process is projecting and must not be
/// mutated by being asked a question about it.
pub(crate) fn non_convergible_differences(
    baseline: &BrennConfig,
    candidate: &BrennConfig,
) -> LevelOne {
    let mut a = baseline.clone();
    let mut b = candidate.clone();
    sort_order_dead_collections(&mut a);
    sort_order_dead_collections(&mut b);

    // Both sides destructured with no `..`, so a field added to `BrennConfig`
    // fails compilation here until someone classifies it as convergible or
    // not. Silently defaulting a new section to "convergible" is how a reload
    // comes to project a document it never read.
    let BrennConfig {
        server,
        database,
        logging,
        security,
        alerting,
        claude_defaults,
        claude_profiles,
        repo_sync,
        repos,
        container,
        integrations,
        apps,
        channels: _,
        messaging,
        observability,
        surface_description,
        llm_chat,
        pwa_push,
        automation,
        mqtt_clients,
        webhook_endpoints,
        events,
        wasm_consumers: _,
        surfaces: _,
        remotes,
        links: _,
        wasm,
        watchdog,
    } = &a;
    let BrennConfig {
        server: b_server,
        database: b_database,
        logging: b_logging,
        security: b_security,
        alerting: b_alerting,
        claude_defaults: b_claude_defaults,
        claude_profiles: b_claude_profiles,
        repo_sync: b_repo_sync,
        repos: b_repos,
        container: b_container,
        integrations: b_integrations,
        apps: b_apps,
        channels: _,
        messaging: b_messaging,
        observability: b_observability,
        surface_description: b_surface_description,
        llm_chat: b_llm_chat,
        pwa_push: b_pwa_push,
        automation: b_automation,
        mqtt_clients: b_mqtt_clients,
        webhook_endpoints: b_webhook_endpoints,
        events: b_events,
        wasm_consumers: _,
        surfaces: _,
        remotes: b_remotes,
        links: _,
        wasm: b_wasm,
        watchdog: b_watchdog,
    } = &b;

    let mut out = Vec::new();
    plain("server", server, b_server, &mut out);
    plain("database", database, b_database, &mut out);
    plain("logging", logging, b_logging, &mut out);
    plain("security", security, b_security, &mut out);
    plain("alerting", alerting, b_alerting, &mut out);
    plain(
        "claude_defaults",
        claude_defaults,
        b_claude_defaults,
        &mut out,
    );
    keyed_map(
        "claude_profiles",
        &by_key(claude_profiles),
        &by_key(b_claude_profiles),
        &mut out,
    );
    plain("repo_sync", repo_sync, b_repo_sync, &mut out);
    keyed_vec("repos", repos, b_repos, |r| &r.slug, &mut out);
    keyed_map(
        "container",
        &by_key(container),
        &by_key(b_container),
        &mut out,
    );
    keyed_map(
        "integrations",
        &by_key(integrations),
        &by_key(b_integrations),
        &mut out,
    );
    let app_diffs = keyed_apps(apps, b_apps, &mut out);
    plain("messaging", messaging, b_messaging, &mut out);
    plain("observability", observability, b_observability, &mut out);
    plain(
        "surface_description",
        surface_description,
        b_surface_description,
        &mut out,
    );
    plain("llm_chat", llm_chat, b_llm_chat, &mut out);
    plain("pwa_push", pwa_push, b_pwa_push, &mut out);
    plain("automation", automation, b_automation, &mut out);
    // TODO(reload-mqtt-sessions): converge this block — start a supervisor for an
    // added client, stop one for a removed client, restart one whose config
    // changed.
    keyed_vec(
        "mqtt_clients",
        mqtt_clients,
        b_mqtt_clients,
        |c| &c.slug,
        &mut out,
    );
    keyed_vec(
        "webhook_endpoints",
        webhook_endpoints,
        b_webhook_endpoints,
        |e| &e.slug,
        &mut out,
    );
    plain("events", events, b_events, &mut out);
    keyed_vec("remotes", remotes, b_remotes, |r| &r.slug, &mut out);
    plain("wasm", wasm, b_wasm, &mut out);
    plain("watchdog", watchdog, b_watchdog, &mut out);
    LevelOne {
        refusals: out,
        app_diffs,
    }
}

/// A whole section that is not a keyed collection: named, not diffed.
///
/// The refusal is the operator's cue to run `config-diff` if they want the
/// field; what a reload owes them is which section stopped it.
fn plain<T: PartialEq>(field: &str, a: &T, b: &T, out: &mut Vec<String>) {
    if a != b {
        out.push(format!("{field} differs: {NEEDS_RESTART}"));
    }
}

/// Whether one convergible field moved. The class arms of [`compare_app`] read
/// as a list of field names because of it, which is what a reader placing a new
/// `AppConfigRaw` field has to be able to do.
fn moved<T: PartialEq>(a: &T, b: &T) -> bool {
    a != b
}

/// Whether any field of a class moved.
fn moved_any(fields: &[bool]) -> bool {
    fields.iter().any(|moved| *moved)
}

/// A block array whose entries carry a unique slug: reported per key.
///
/// Order is a difference in its own right and is reported as one — the block
/// arrays are read in order by the runtime, which is why
/// `sort_order_dead_collections` deliberately leaves them alone.
fn keyed_vec<T: PartialEq>(
    field: &str,
    a: &[T],
    b: &[T],
    key: impl Fn(&T) -> &String,
    out: &mut Vec<String>,
) {
    let keys_a: Vec<&String> = a.iter().map(&key).collect();
    let keys_b: Vec<&String> = b.iter().map(&key).collect();
    let mut named = false;
    for k in &keys_a {
        if !keys_b.contains(k) {
            out.push(format!("{field}[{k}] removed: {NEEDS_RESTART}"));
            named = true;
        }
    }
    for k in &keys_b {
        if !keys_a.contains(k) {
            out.push(format!("{field}[{k}] added: {NEEDS_RESTART}"));
            named = true;
        }
    }
    for item in a {
        let k = key(item);
        if let Some(other) = b.iter().find(|o| key(o) == k)
            && item != other
        {
            out.push(format!("{field}[{k}] differs: {NEEDS_RESTART}"));
            named = true;
        }
    }
    if !named && keys_a != keys_b {
        out.push(format!("{field} is in a different order: {NEEDS_RESTART}"));
    }
}

/// The `apps` block: add, remove and reorder are refusals like any other keyed
/// section, but an agent present on both sides is compared field by field.
fn keyed_apps(
    a: &[AppConfigRaw],
    b: &[AppConfigRaw],
    out: &mut Vec<String>,
) -> BTreeMap<String, AppFieldDiff> {
    let keys_a: Vec<&String> = a.iter().map(|app| &app.slug).collect();
    let keys_b: Vec<&String> = b.iter().map(|app| &app.slug).collect();
    let mut named = false;
    // TODO(reload-agent-lifecycle): converge the agent set itself. An agent's
    // existence is wired into a delivery binding, a state directory, per-app
    // HTTP routes, integration prepare/validate, startup hooks, repo sync's
    // clone index and the roster; standing one up or tearing one down at
    // reload is a slice of its own.
    for k in &keys_a {
        if !keys_b.contains(k) {
            out.push(format!("apps[{k}] removed: {NEEDS_RESTART}"));
            named = true;
        }
    }
    for k in &keys_b {
        if !keys_a.contains(k) {
            out.push(format!("apps[{k}] added: {NEEDS_RESTART}"));
            named = true;
        }
    }
    let mut diffs = BTreeMap::new();
    for app in a {
        let Some(other) = b.iter().find(|o| o.slug == app.slug) else {
            continue;
        };
        let before = out.len();
        let diff = compare_app(&app.slug, app, other, out);
        named |= out.len() > before;
        if diff.moved() {
            diffs.insert(app.slug.clone(), diff);
        }
    }
    if !named && keys_a != keys_b {
        out.push(format!("apps is in a different order: {NEEDS_RESTART}"));
    }
    diffs
}

/// One agent, field by field.
///
/// Both sides are destructured with no `..`, so a field added to
/// `AppConfigRaw` fails compilation here until someone places it in a class.
/// A field that converges silently because nobody classified it is a process
/// projecting a document it never read.
fn compare_app(
    slug: &str,
    a: &AppConfigRaw,
    b: &AppConfigRaw,
    out: &mut Vec<String>,
) -> AppFieldDiff {
    let AppConfigRaw {
        slug: _,
        name,
        description,
        icon,
        working_dir,
        model,
        models,
        single_instance,
        singleton,
        persistent,
        idle_timeout_secs,
        compact_reminder_pct,
        compact_soft_pct,
        compact_red_pct,
        compact_hard_pct,
        compact_reminder_tokens,
        compact_soft_tokens,
        compact_red_tokens,
        compact_hard_tokens,
        compact_idle_secs,
        idle_hook_secs,
        allowed_users,
        disabled_tools,
        mcp_servers,
        multiuser,
        prefix_username,
        prefix_timestamp,
        prefix_device,
        container,
        container_working_dir,
        start_hooks,
        post_pull_hooks,
        startup_hooks,
        cc_extra_args,
        claude_profiles,
        approval_rules,
        attachment_targets,
        integrations,
        integration_config,
        mounts,
        extra_mounts,
        history_replay_limit,
        frontmatter,
        messaging: _,
        pwa_push,
        webhook_subscriptions,
        mqtt_subscriptions: _,
        grants: _,
        acl: _,
        tool_grants: _,
    } = a;
    let AppConfigRaw {
        slug: _,
        name: b_name,
        description: b_description,
        icon: b_icon,
        working_dir: b_working_dir,
        model: b_model,
        models: b_models,
        single_instance: b_single_instance,
        singleton: b_singleton,
        persistent: b_persistent,
        idle_timeout_secs: b_idle_timeout_secs,
        compact_reminder_pct: b_compact_reminder_pct,
        compact_soft_pct: b_compact_soft_pct,
        compact_red_pct: b_compact_red_pct,
        compact_hard_pct: b_compact_hard_pct,
        compact_reminder_tokens: b_compact_reminder_tokens,
        compact_soft_tokens: b_compact_soft_tokens,
        compact_red_tokens: b_compact_red_tokens,
        compact_hard_tokens: b_compact_hard_tokens,
        compact_idle_secs: b_compact_idle_secs,
        idle_hook_secs: b_idle_hook_secs,
        allowed_users: b_allowed_users,
        disabled_tools: b_disabled_tools,
        mcp_servers: b_mcp_servers,
        multiuser: b_multiuser,
        prefix_username: b_prefix_username,
        prefix_timestamp: b_prefix_timestamp,
        prefix_device: b_prefix_device,
        container: b_container,
        container_working_dir: b_container_working_dir,
        start_hooks: b_start_hooks,
        post_pull_hooks: b_post_pull_hooks,
        startup_hooks: b_startup_hooks,
        cc_extra_args: b_cc_extra_args,
        claude_profiles: b_claude_profiles,
        approval_rules: b_approval_rules,
        attachment_targets: b_attachment_targets,
        integrations: b_integrations,
        integration_config: b_integration_config,
        mounts: b_mounts,
        extra_mounts: b_extra_mounts,
        history_replay_limit: b_history_replay_limit,
        frontmatter: b_frontmatter,
        messaging: _,
        pwa_push: b_pwa_push,
        webhook_subscriptions: b_webhook_subscriptions,
        mqtt_subscriptions: _,
        grants: _,
        acl: _,
        tool_grants: _,
    } = b;

    // Authority — `grants`, `acl`, `tool_grants`, `messaging`,
    // `mqtt_subscriptions` — is bound to `_` above and compared nowhere here:
    // the delta compares its resolved form, so two spellings that resolve to
    // the same policy are not a change.

    // Class A: read off the map on each request. The swap converges them.
    // `allowed_users` additionally closes a removed user's connections and
    // retires their sessions; `start_hooks` is read per spawn of a *new*
    // conversation, so nothing a live process holds goes stale.
    let per_call_changed = moved_any(&[
        moved(name, b_name),
        moved(description, b_description),
        moved(icon, b_icon),
        moved(models, b_models),
        moved(single_instance, b_single_instance),
        moved(allowed_users, b_allowed_users),
        moved(multiuser, b_multiuser),
        moved(prefix_username, b_prefix_username),
        moved(prefix_timestamp, b_prefix_timestamp),
        moved(prefix_device, b_prefix_device),
        moved(start_hooks, b_start_hooks),
        moved(post_pull_hooks, b_post_pull_hooks),
        moved(attachment_targets, b_attachment_targets),
        moved(history_replay_limit, b_history_replay_limit),
        moved(pwa_push, b_pwa_push),
    ]);

    // Class B: baked into a Claude Code process at spawn, or copied onto its
    // bridge at construction. The swap reaches the next process; the live one
    // is retired at its next idle moment.
    let spawn_changed = moved_any(&[
        moved(working_dir, b_working_dir),
        moved(model, b_model),
        moved(singleton, b_singleton),
        moved(persistent, b_persistent),
        moved(idle_timeout_secs, b_idle_timeout_secs),
        moved(compact_reminder_pct, b_compact_reminder_pct),
        moved(compact_soft_pct, b_compact_soft_pct),
        moved(compact_red_pct, b_compact_red_pct),
        moved(compact_hard_pct, b_compact_hard_pct),
        moved(compact_reminder_tokens, b_compact_reminder_tokens),
        moved(compact_soft_tokens, b_compact_soft_tokens),
        moved(compact_red_tokens, b_compact_red_tokens),
        moved(compact_hard_tokens, b_compact_hard_tokens),
        moved(compact_idle_secs, b_compact_idle_secs),
        moved(idle_hook_secs, b_idle_hook_secs),
        moved(disabled_tools, b_disabled_tools),
        moved(mcp_servers, b_mcp_servers),
        moved(container_working_dir, b_container_working_dir),
        moved(cc_extra_args, b_cc_extra_args),
        moved(approval_rules, b_approval_rules),
        moved(extra_mounts, b_extra_mounts),
        moved(frontmatter, b_frontmatter),
    ]);

    // Class C: boot folds these into another subsystem's tables or side
    // effects, so converging one means converging that subsystem.
    let field = |name: &str| format!("apps[{slug}].{name}");
    // TODO(reload-repo-mounts): boot flattens every agent's mounts into repo
    // sync's clone index, its per-remote lock table, the pull tool's clone
    // table and the cross-agent primary-ownership check, and the sync manager
    // exists at all only if some mount asks for auto-pull.
    plain(&field("mounts"), mounts, b_mounts, out);
    // TODO(reload-integrations): moving an agent between bare and
    // containerized relocates its state directory — and with it the virtual
    // tools file and every integration manifest a boot-only `prepare` wrote
    // for the old placement.
    plain(&field("container"), container, b_container, out);
    // TODO(reload-integrations): the `Integration` contract has two boot-only
    // environment steps, `prepare` and `validate`, which scan the filesystem,
    // write manifests and shell out under a panic-on-failure contract. Running
    // them inside a reload commit turns an operator-facing refusal into a
    // process death.
    plain(&field("integrations"), integrations, b_integrations, out);
    // TODO(reload-integrations): same, by the other spelling — naming an
    // integration here enables it.
    plain(
        &field("integration_config"),
        integration_config,
        b_integration_config,
        out,
    );
    // The field's meaning is "run once at server startup"; a reload cannot
    // honour that. Running the hook executes an operator script at a moment it
    // was not written for, inside commit, under a panic-on-failure contract;
    // accepting the difference without running it diverges silently until the
    // next restart.
    plain(&field("startup_hooks"), startup_hooks, b_startup_hooks, out);
    // TODO(reload-claude-profiles): boot builds the profile goal index from
    // every agent's block and plans a `cc-profile` system participant whose
    // subscriptions are the goal addresses; converging this converges that
    // participant's subscription set and re-seeds accepted goal state.
    plain(
        &field("claude_profiles"),
        claude_profiles,
        b_claude_profiles,
        out,
    );
    // TODO(reload-webhooks): the subscriber entry is an ordinary in-place
    // edit, but endpoint *ownership* is computed from the subscribing agent
    // and stamped into the endpoint table the HTTP layer holds frozen. Moving
    // one without the other leaves the router's view and the document's apart.
    plain(
        &field("webhook_subscriptions"),
        webhook_subscriptions,
        b_webhook_subscriptions,
        out,
    );

    AppFieldDiff {
        per_call_changed,
        spawn_changed,
    }
}

/// A section that is a map: reported per key, in key order.
fn keyed_map<V: PartialEq>(
    field: &str,
    a: &BTreeMap<&str, &V>,
    b: &BTreeMap<&str, &V>,
    out: &mut Vec<String>,
) {
    for (k, v) in a {
        match b.get(k) {
            None => out.push(format!("{field}[{k}] removed: {NEEDS_RESTART}")),
            Some(other) if v != other => {
                out.push(format!("{field}[{k}] differs: {NEEDS_RESTART}"));
            }
            Some(_) => {}
        }
    }
    for k in b.keys() {
        if !a.contains_key(k) {
            out.push(format!("{field}[{k}] added: {NEEDS_RESTART}"));
        }
    }
}

/// A borrowed, key-ordered view of a map section, so hash order never reaches
/// the refusal list.
fn by_key<'a, V, M>(map: M) -> BTreeMap<&'a str, &'a V>
where
    M: IntoIterator<Item = (&'a String, &'a V)>,
{
    map.into_iter().map(|(k, v)| (k.as_str(), v)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use brenn_lib::access::raw::{AppAclRaw, ChannelMatcherRaw};
    use brenn_lib::config::AppConfigRaw;
    use brenn_lib::messaging::config::{ChannelConfigRaw, Depth, WasmConsumerConfigRaw};
    use brenn_lib::mqtt::config::MqttClientConfigRaw;

    /// An `[[app]]` block with nothing but a slug.
    fn app(slug: &str) -> AppConfigRaw {
        AppConfigRaw {
            slug: slug.to_string(),
            ..Default::default()
        }
    }

    /// The document every case starts from: one agent, nothing else.
    fn base() -> BrennConfig {
        BrennConfig {
            apps: vec![app("assistant")],
            ..Default::default()
        }
    }

    /// The refusal list alone, for the cases that are about refusals.
    fn refusals(baseline: &BrennConfig, candidate: &BrennConfig) -> Vec<String> {
        non_convergible_differences(baseline, candidate).refusals
    }

    /// The agent's field-class flags, for a candidate that passes level 1.
    /// Absent means the agent did not move in any class this pass compares.
    fn diff_of(candidate: &BrennConfig, slug: &str) -> Option<AppFieldDiff> {
        let level_one = non_convergible_differences(&base(), candidate);
        assert!(
            level_one.refusals.is_empty(),
            "expected level 1 to pass: {:?}",
            level_one.refusals
        );
        level_one.app_diffs.get(slug).copied()
    }

    /// `base()` with one edit applied to its single agent.
    fn edited(edit: impl FnOnce(&mut AppConfigRaw)) -> BrennConfig {
        let mut candidate = base();
        edit(&mut candidate.apps[0]);
        candidate
    }

    #[test]
    fn an_unedited_document_is_no_difference() {
        assert!(refusals(&base(), &base()).is_empty());
    }

    /// The three blocks a reload converges are not this pass's business, and it
    /// says nothing about them however far apart they are.
    #[test]
    fn the_convergible_blocks_are_ignored() {
        let mut candidate = base();
        candidate.channels.push(ChannelConfigRaw {
            send_rate: None,
            uuid: Some("5f1d1a9e-0000-4000-8000-0000000000c1".to_string()),
            address: Some("brenn:work".to_string()),
            address_prefix: None,
            description: Some("the work".to_string()),
            push_depth: Some(Depth::Bounded(1)),
            retain_depth: Some(Depth::Bounded(1)),
            standing_retain_depth: Some(Depth::Bounded(4)),
            noise: None,
            sink: None,
            wake_min: None,
        });
        candidate.wasm_consumers = vec![WasmConsumerConfigRaw::minimal(
            "sifter",
            "processor-demo",
            &["brenn:work"],
        )];
        assert!(refusals(&base(), &candidate).is_empty());
    }

    /// An agent's authority is compared in its resolved form, not here: a
    /// widened grant set passes level 1 and leaves both class flags clear,
    /// because nothing a live process holds and nothing a request reads off
    /// the map went stale.
    #[test]
    fn an_agents_grant_set_is_not_a_refusal_and_not_a_field_class() {
        let candidate = edited(|app| {
            app.grants = vec![brenn_envelope::grants::AppCapability::MessagingSubscribe];
        });
        assert!(refusals(&base(), &candidate).is_empty());
        assert_eq!(diff_of(&candidate, "assistant"), None);
    }

    /// The other three authority spellings, for the same reason.
    #[test]
    fn an_acl_a_tool_grant_and_a_messaging_block_are_not_field_classes() {
        let acl = edited(|app| {
            app.acl = AppAclRaw {
                brenn_subscribe: vec![ChannelMatcherRaw::Exact("brenn:work".to_string())],
                ..Default::default()
            };
        });
        assert_eq!(diff_of(&acl, "assistant"), None);

        let tool_grant = edited(|app| {
            app.tool_grants = vec![brenn_lib::tools::config::ToolGrantRaw {
                tool: "git-repo-pull".to_string(),
                acl: Vec::new(),
                rate_limit: None,
            }];
        });
        assert_eq!(diff_of(&tool_grant, "assistant"), None);

        let messaging = edited(|app| {
            app.messaging = Some(brenn_lib::messaging::config::MessagingConfigRaw {
                subscribe: Vec::new(),
                send_budget: Some(7),
            });
        });
        assert_eq!(diff_of(&messaging, "assistant"), None);
    }

    /// Class A: every request reads it off the map, so the swap is the whole
    /// convergence and no session is stale.
    #[test]
    fn a_per_call_field_moves_the_agent_without_a_respawn() {
        for candidate in [
            edited(|app| app.icon = Some("*".to_string())),
            edited(|app| app.name = Some("The Assistant".to_string())),
            edited(|app| app.description = Some("does things".to_string())),
            edited(|app| app.models = Some(vec!["opus".to_string()])),
            edited(|app| app.single_instance = true),
            edited(|app| app.allowed_users = vec!["dev".to_string()]),
            edited(|app| app.multiuser = true),
            edited(|app| app.prefix_username = Some(true)),
            edited(|app| app.prefix_timestamp = Some(true)),
            edited(|app| app.prefix_device = Some(false)),
            edited(|app| app.history_replay_limit = Some(10)),
            edited(|app| {
                app.start_hooks = Some(brenn_lib::config::StartHooksConfig {
                    host: vec!["/bin/true".to_string()],
                    container: Vec::new(),
                });
            }),
            edited(|app| {
                app.post_pull_hooks = Some(brenn_lib::config::PostPullHooksConfig {
                    host: vec!["/bin/true".to_string()],
                    container: Vec::new(),
                });
            }),
            edited(|app| {
                app.pwa_push = Some(brenn_lib::pwa_push::config::AppPwaPushBlock {
                    default_title: Some("Assistant".to_string()),
                });
            }),
            edited(|app| {
                app.attachment_targets = vec![brenn_lib::config::AttachmentTargetRaw {
                    name: "import".to_string(),
                    label: "Import".to_string(),
                    accept: vec![".ofx".to_string()],
                    multi: false,
                    handler: brenn_lib::config::AttachmentHandlerConfig::Command {
                        program: "/bin/true".to_string(),
                        args: Vec::new(),
                        file_roles: std::collections::HashMap::new(),
                        timeout_secs: 60,
                        cc_instructions: None,
                    },
                }];
            }),
        ] {
            assert_eq!(
                diff_of(&candidate, "assistant"),
                Some(AppFieldDiff {
                    per_call_changed: true,
                    spawn_changed: false,
                }),
            );
        }
    }

    /// Class B: the value is baked into a Claude Code process at spawn, so the
    /// agent's live sessions are stale and the delta has to say so.
    #[test]
    fn a_per_process_field_marks_the_agent_for_respawn() {
        for candidate in [
            edited(|app| app.model = Some("sonnet".to_string())),
            edited(|app| app.working_dir = Some(std::path::PathBuf::from("/srv/work"))),
            edited(|app| app.disabled_tools = vec!["Bash".to_string()]),
            edited(|app| app.cc_extra_args = vec!["--verbose".to_string()]),
            edited(|app| app.singleton = true),
            edited(|app| app.persistent = true),
            edited(|app| app.idle_timeout_secs = Some(60)),
            edited(|app| app.idle_hook_secs = Some(0)),
            edited(|app| app.compact_soft_pct = Some(70)),
            edited(|app| app.compact_hard_tokens = Some(100_000)),
            edited(|app| app.compact_idle_secs = Some(30)),
            edited(|app| app.container_working_dir = Some(std::path::PathBuf::from("/work"))),
            edited(|app| app.extra_mounts = vec!["/a:/b".to_string()]),
            edited(|app| {
                app.mcp_servers = std::collections::HashMap::from([(
                    "graf".to_string(),
                    brenn_lib::config::McpServerConfig {
                        command: "/usr/bin/graf".to_string(),
                        args: vec!["mcp".to_string()],
                        env: std::collections::HashMap::new(),
                    },
                )]);
            }),
            edited(|app| {
                app.approval_rules = vec![brenn_lib::config::ApprovalRuleConfig {
                    tool: "Bash".to_string(),
                    pattern: "ls *".to_string(),
                }];
            }),
            edited(|app| {
                app.frontmatter = brenn_lib::config::FrontmatterRenderConfig {
                    hide: vec!["tags".to_string()],
                    ..Default::default()
                };
            }),
        ] {
            assert_eq!(
                diff_of(&candidate, "assistant"),
                Some(AppFieldDiff {
                    per_call_changed: false,
                    spawn_changed: true,
                }),
            );
        }
    }

    /// Class C: each is folded into some other subsystem's tables at boot, and
    /// the refusal names the field so the operator knows which edit to undo.
    #[test]
    fn a_boot_shaped_field_is_refused_by_name() {
        let cases: Vec<(&str, BrennConfig)> = vec![
            (
                "mounts",
                edited(|app| {
                    app.mounts = vec![brenn_lib::config::MountConfigRaw {
                        repo: "notes".to_string(),
                        access: brenn_lib::config::AccessLevel::ReadOnly,
                        working_dir: false,
                        auto_pull: None,
                        primary: false,
                    }];
                }),
            ),
            (
                "container",
                edited(|app| app.container = Some("box".to_string())),
            ),
            (
                "integrations",
                edited(|app| app.integrations = vec!["graf".to_string()]),
            ),
            (
                "integration_config",
                edited(|app| {
                    // Typed by the field it lands in, which is what lets the
                    // value be spelled without naming the toml crate here.
                    let mut per_agent = std::collections::HashMap::new();
                    per_agent.insert(
                        "graf".to_string(),
                        "true".parse().expect("a toml scalar parses"),
                    );
                    app.integration_config = per_agent;
                }),
            ),
            (
                "startup_hooks",
                edited(|app| {
                    app.startup_hooks = Some(brenn_lib::config::StartupHooksConfig {
                        host: vec!["/bin/true".to_string()],
                        container: Vec::new(),
                    });
                }),
            ),
            (
                "claude_profiles",
                edited(|app| {
                    app.claude_profiles = Some(brenn_lib::config::AppClaudeProfiles {
                        allowed: vec!["work".to_string()],
                        goal: None,
                    });
                }),
            ),
            (
                "webhook_subscriptions",
                edited(|app| {
                    app.webhook_subscriptions =
                        vec![brenn_lib::webhook::config::AppWebhookSubscriptionRaw {
                            endpoint: "inbox".to_string(),
                            push_depth: None,
                            retain_depth: None,
                            wake_min: None,
                        }];
                }),
            ),
        ];
        for (field, candidate) in cases {
            assert_eq!(
                refusals(&base(), &candidate),
                vec![format!(
                    "apps[assistant].{field} differs: this change needs a restart"
                )],
            );
        }
    }

    /// Both classes at once, and the two flags are independent.
    #[test]
    fn a_per_call_and_a_per_process_edit_set_both_flags() {
        let candidate = edited(|app| {
            app.icon = Some("*".to_string());
            app.model = Some("sonnet".to_string());
        });
        assert_eq!(
            diff_of(&candidate, "assistant"),
            Some(AppFieldDiff {
                per_call_changed: true,
                spawn_changed: true,
            }),
        );
    }

    /// One agent moving says nothing about another.
    #[test]
    fn an_untouched_agent_is_absent_from_the_map() {
        let two = |icon: Option<&str>| BrennConfig {
            apps: vec![
                AppConfigRaw {
                    icon: icon.map(str::to_string),
                    ..app("assistant")
                },
                app("scribe"),
            ],
            ..Default::default()
        };
        let level_one = non_convergible_differences(&two(None), &two(Some("*")));
        assert!(level_one.refusals.is_empty());
        assert_eq!(
            level_one.app_diffs.keys().collect::<Vec<_>>(),
            vec!["assistant"]
        );
    }

    /// A matcher list is a set to every enforcement site, so two documents that
    /// list one in a different order are one configuration — the normalization
    /// `config-diff` applies, applied here for the same reason.
    #[test]
    fn a_reordered_acl_list_is_not_a_difference() {
        let with_acl = |first: &str, second: &str| {
            let mut config = base();
            config.apps[0].acl = AppAclRaw {
                brenn_subscribe: vec![
                    ChannelMatcherRaw::Exact(first.to_string()),
                    ChannelMatcherRaw::Exact(second.to_string()),
                ],
                ..Default::default()
            };
            config
        };
        assert!(refusals(&with_acl("alpha", "beta"), &with_acl("beta", "alpha")).is_empty());
    }

    #[test]
    fn an_added_and_a_removed_agent_are_each_named() {
        let candidate = BrennConfig {
            apps: vec![app("scribe")],
            ..Default::default()
        };
        assert_eq!(
            refusals(&base(), &candidate),
            vec![
                "apps[assistant] removed: this change needs a restart".to_string(),
                "apps[scribe] added: this change needs a restart".to_string(),
            ],
        );
    }

    /// The boundary the `mqtt:` convergence rests on: a declared client has a
    /// broker session for the life of the process, so adding one is a change
    /// only a restart can make.
    #[test]
    fn an_added_mqtt_client_is_a_restart() {
        let mut candidate = base();
        candidate.mqtt_clients = vec![MqttClientConfigRaw::minimal(
            "spare",
            "mqtts://127.0.0.1:8884",
        )];
        assert_eq!(
            refusals(&base(), &candidate),
            vec!["mqtt_clients[spare] added: this change needs a restart".to_string()],
        );
    }

    /// Block arrays are read in order by the runtime — which is why
    /// `sort_order_dead_collections` leaves them alone — so a reordering is a
    /// difference, and one worth its own words.
    #[test]
    fn a_reordered_block_array_is_a_difference_of_its_own() {
        let two = |first: &str, second: &str| BrennConfig {
            apps: vec![app(first), app(second)],
            ..Default::default()
        };
        assert_eq!(
            refusals(&two("alpha", "beta"), &two("beta", "alpha")),
            vec!["apps is in a different order: this change needs a restart".to_string()],
        );
    }

    /// A section that is not a keyed collection is named whole: the operator's
    /// next move is `config-diff`, and what a refusal owes them is which
    /// section stopped the reload.
    #[test]
    fn a_scalar_section_is_named_without_its_fields() {
        let mut candidate = base();
        candidate.server.bind_address = "127.0.0.1:3001".parse().unwrap();
        assert_eq!(
            refusals(&base(), &candidate),
            vec!["server differs: this change needs a restart".to_string()],
        );
    }

    /// Surfaces are the fourth convergible block: a difference between two
    /// documents' surface lists is level 2's to walk, not level 1's to refuse.
    #[test]
    fn a_surface_difference_is_not_a_level_1_refusal() {
        let surface = || brenn_messaging_boot::test_fixtures::minimal_surface_raw();
        let before = BrennConfig {
            surfaces: vec![surface()],
            ..base()
        };
        let mut moved = surface();
        moved.skin = Some("bench".to_string());
        let after = BrennConfig {
            surfaces: vec![moved],
            ..base()
        };
        assert_eq!(refusals(&before, &after), Vec::<String>::new());
        assert_eq!(
            refusals(
                &before,
                &BrennConfig {
                    surfaces: Vec::new(),
                    ..base()
                }
            ),
            Vec::<String>::new(),
        );
    }

    /// A section that is a map rather than a block array is named by its key
    /// too, in key order — hash order must never reach a refusal list.
    #[test]
    fn a_map_section_is_named_by_its_key() {
        let profile = |token_file: &str| {
            let mut config = base();
            config.claude_profiles.insert(
                "work".to_string(),
                brenn_lib::config::ClaudeProfileRaw {
                    token_file: std::path::PathBuf::from(token_file),
                    expires: None,
                },
            );
            config
        };
        assert_eq!(
            refusals(&profile("/keys/one"), &profile("/keys/two")),
            vec!["claude_profiles[work] differs: this change needs a restart".to_string()],
        );
    }
}
