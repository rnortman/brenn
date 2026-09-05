//! Level 1: everything a reload cannot converge must be equal.
//!
//! Three blocks of a document are convergible — `channels`, `links` and
//! `wasm_consumers` — and this pass ignores exactly those. Every other section
//! describes an entity whose runtime tables are boot snapshots: an app's policy
//! is folded into the delivery gates, a surface's bindings document is
//! published once, a remote's token is loaded once, an MQTT client's broker
//! session is opened once. Converging any of them is a later slice's work; a
//! difference in one of them here is a refusal.
//!
//! The comparison is over *loaded* configs rather than document text, so
//! defaults are applied, key order is gone, and a section rewritten into a
//! different assembly that lowers to the same value is not a difference.
//! Collections whose order the runtime ignores are sorted first, the way
//! `config-diff` sorts them, so a reordered ACL list is not a difference
//! either.

use std::collections::BTreeMap;

use brenn_lib::config::{BrennConfig, sort_order_dead_collections};

use super::NEEDS_RESTART;

/// Every non-convergible difference between the running document and a
/// candidate, as refusal lines. Empty means level 1 passed.
///
/// Both sides are cloned and normalized before anything is compared: the
/// caller's baseline is the document the process is projecting and must not be
/// mutated by being asked a question about it.
pub(crate) fn non_convergible_differences(
    baseline: &BrennConfig,
    candidate: &BrennConfig,
) -> Vec<String> {
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
        surfaces,
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
        surfaces: b_surfaces,
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
    keyed_vec("apps", apps, b_apps, |app| &app.slug, &mut out);
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
    keyed_vec("surfaces", surfaces, b_surfaces, |s| &s.slug, &mut out);
    keyed_vec("remotes", remotes, b_remotes, |r| &r.slug, &mut out);
    plain("wasm", wasm, b_wasm, &mut out);
    plain("watchdog", watchdog, b_watchdog, &mut out);
    out
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

    #[test]
    fn an_unedited_document_is_no_difference() {
        assert!(non_convergible_differences(&base(), &base()).is_empty());
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
        assert!(non_convergible_differences(&base(), &candidate).is_empty());
    }

    #[test]
    fn an_agents_grant_set_is_named_by_slug() {
        let mut candidate = base();
        candidate.apps[0].grants = vec![brenn_envelope::grants::AppCapability::MessagingSubscribe];
        assert_eq!(
            non_convergible_differences(&base(), &candidate),
            vec!["apps[assistant] differs: this change needs a restart".to_string()],
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
        assert!(
            non_convergible_differences(&with_acl("alpha", "beta"), &with_acl("beta", "alpha"))
                .is_empty()
        );
    }

    #[test]
    fn an_added_and_a_removed_agent_are_each_named() {
        let candidate = BrennConfig {
            apps: vec![app("scribe")],
            ..Default::default()
        };
        assert_eq!(
            non_convergible_differences(&base(), &candidate),
            vec![
                "apps[assistant] removed: this change needs a restart".to_string(),
                "apps[scribe] added: this change needs a restart".to_string(),
            ],
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
            non_convergible_differences(&two("alpha", "beta"), &two("beta", "alpha")),
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
            non_convergible_differences(&base(), &candidate),
            vec!["server differs: this change needs a restart".to_string()],
        );
    }

    #[test]
    fn a_surface_is_named_by_slug() {
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
        assert_eq!(
            non_convergible_differences(&before, &after),
            vec!["surfaces[deskbar] differs: this change needs a restart".to_string()],
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
            non_convergible_differences(&profile("/keys/one"), &profile("/keys/two")),
            vec!["claude_profiles[work] differs: this change needs a restart".to_string()],
        );
    }
}
