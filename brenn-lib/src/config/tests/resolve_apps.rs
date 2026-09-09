//! `resolve_apps` — the one resolver boot and reload share.
//!
//! Boot reaches it through `validate_and_resolve`; reload reaches it directly,
//! over a candidate document, with the caller-supplied client identities and
//! webhook subscription stamps.
//! What these tests hold is that the two paths are the same function: a
//! candidate resolves to the map a fresh boot of that same document would have
//! produced, and every refusal boot makes over an agent is a refusal here.

use std::collections::BTreeMap;

use indexmap::IndexMap;

use super::*;
use crate::access::raw::{AppAclRaw, ChannelMatcherRaw};
use crate::config::{ResolvedConfig, resolve_apps, validate_and_resolve};
use crate::integration::IntegrationRegistry;
use crate::messaging::config::{
    ChannelConfigRaw, Depth, MessagingConfigRaw, MessagingSubscriptionRaw,
};
use crate::mqtt::config::MqttClientIdentity;
use crate::webhook::config::ResolvedWebhookSubscription;
use brenn_envelope::grants::AppCapability;

/// Empty caller-supplied inputs: no MQTT clients, no webhook subscriptions.
fn empty_inputs() -> (
    IndexMap<String, MqttClientIdentity>,
    BTreeMap<String, Vec<ResolvedWebhookSubscription>>,
) {
    (IndexMap::new(), BTreeMap::new())
}

/// Resolve a document the way reload does: `resolve_apps` alone, over the two
/// inputs it reads rather than derives, with no `ResolvedConfig` around it.
fn candidate_apps(config: &BrennConfig) -> IndexMap<String, AppConfig> {
    let (clients, webhooks) = empty_inputs();
    resolve_apps(
        config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
        &clients,
        &webhooks,
    )
}

/// Resolve a document the way boot does.
fn booted_apps(config: &BrennConfig) -> IndexMap<String, AppConfig> {
    let ResolvedConfig { apps, .. } = validate_and_resolve(
        config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
    (*apps).clone()
}

/// A one-agent document: a singleton agent with a messaging block subscribing
/// to `brenn:ch`, the `MessagingSubscribe` grant, and a `brenn_subscribe`
/// matcher covering it.
fn document(dir: &Path, subscribe_matcher: &str) -> BrennConfig {
    BrennConfig {
        server: super::test_server_config(),
        claude_defaults: ClaudeDefaultsConfig {
            model: "sonnet".to_string(),
            ..Default::default()
        },
        channels: vec![ChannelConfigRaw {
            send_rate: None,
            uuid: Some("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32".to_string()),
            address: Some("ch".to_string()),
            address_prefix: None,
            description: None,
            push_depth: Some(Depth::Bounded(4)),
            retain_depth: Some(Depth::Bounded(4)),
            standing_retain_depth: Some(Depth::Bounded(4)),
            noise: None,
            sink: None,
            wake_min: None,
        }],
        apps: vec![AppConfigRaw {
            slug: "assistant".to_string(),
            working_dir: Some(dir.to_path_buf()),
            singleton: true,
            allowed_users: vec!["dev".to_string()],
            compact_soft_pct: Some(75),
            grants: vec![AppCapability::MessagingSubscribe],
            acl: AppAclRaw {
                brenn_subscribe: vec![ChannelMatcherRaw::Exact(subscribe_matcher.to_string())],
                ..Default::default()
            },
            messaging: Some(MessagingConfigRaw {
                subscribe: vec![MessagingSubscriptionRaw {
                    channel: "brenn:ch".to_string(),
                    push_depth: Some(Depth::Bounded(2)),
                    retain_depth: Some(Depth::Bounded(4)),
                    noise: None,
                    wake_min: None,
                }],
                send_budget: None,
            }),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// The candidate path and the boot path are one function: over the same
/// document they produce the same policies, the same messaging config and the
/// same spawn-shaped fields.
#[test]
fn candidate_equals_boot_over_the_same_document() {
    let dir = tempfile::tempdir().unwrap();
    let config = document(dir.path(), "ch");

    let booted = booted_apps(&config);
    let candidate = candidate_apps(&config);

    assert_eq!(
        booted.keys().collect::<Vec<_>>(),
        candidate.keys().collect::<Vec<_>>(),
    );
    let b = &booted["assistant"];
    let c = &candidate["assistant"];
    assert_eq!(b.policy, c.policy);
    assert_eq!(b.chat_harness_policy, c.chat_harness_policy);
    assert_eq!(format!("{:?}", b.messaging), format!("{:?}", c.messaging));
    assert_eq!(b.model, c.model);
    assert_eq!(b.working_dir, c.working_dir);
    assert_eq!(b.state_dir, c.state_dir);
    assert_eq!(b.allowed_users, c.allowed_users);
}

/// A widened ACL resolves through `resolve_apps` to exactly what a fresh boot
/// of the widened document produces — the reload's candidate is the oracle's
/// map, not a re-stamp of the booted one.
#[test]
fn widened_acl_candidate_equals_fresh_boot() {
    let dir = tempfile::tempdir().unwrap();
    let widened = document(dir.path(), "c");

    let candidate = candidate_apps(&widened);
    let fresh = booted_apps(&widened);

    assert_eq!(candidate["assistant"].policy, fresh["assistant"].policy);
    // And it differs from the narrower document's policy, so the comparison
    // above is not vacuous.
    let narrow = booted_apps(&document(dir.path(), "ch"));
    assert_ne!(candidate["assistant"].policy, narrow["assistant"].policy);
}

/// A candidate that removes the agent's whole `messaging` block resolves to
/// `None`, not to the booted block. A map that was stamped rather than
/// re-resolved would have kept it.
#[test]
fn removed_messaging_block_resolves_to_none() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = document(dir.path(), "ch");
    assert!(candidate_apps(&config)["assistant"].messaging.is_some());

    config.apps[0].messaging = None;
    let candidate = candidate_apps(&config);
    assert!(candidate["assistant"].messaging.is_none());
    assert!(candidate["assistant"].mqtt_subscriptions.is_empty());
}

/// A per-process (class-B) edit moves that field and nothing else.
#[test]
fn spawn_field_edit_moves_only_that_field() {
    let dir = tempfile::tempdir().unwrap();
    let base = candidate_apps(&document(dir.path(), "ch"));

    let mut config = document(dir.path(), "ch");
    config.apps[0].model = Some("opus".to_string());
    let edited = candidate_apps(&config);

    assert_eq!(base["assistant"].model, "sonnet");
    assert_eq!(edited["assistant"].model, "opus");
    assert_eq!(base["assistant"].policy, edited["assistant"].policy);
    assert_eq!(
        format!("{:?}", base["assistant"].messaging),
        format!("{:?}", edited["assistant"].messaging),
    );
}

/// The webhook stamps come from the caller, not from the document: the
/// agent map is stamped in one place, which is what lets boot and reload stamp
/// through one line.
#[test]
fn webhook_stamps_come_from_the_caller() {
    let dir = tempfile::tempdir().unwrap();
    let config = document(dir.path(), "ch");
    let clients: IndexMap<String, MqttClientIdentity> = IndexMap::new();
    let mut webhooks: BTreeMap<String, Vec<ResolvedWebhookSubscription>> = BTreeMap::new();
    webhooks.insert(
        "assistant".to_string(),
        vec![ResolvedWebhookSubscription {
            endpoint_slug: "ep".to_string(),
            push_depth: Depth::Bounded(1),
            retain_depth: Depth::Bounded(8),
            wake_min: crate::messaging::WakeMin::Normal,
        }],
    );
    let apps = resolve_apps(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
        &clients,
        &webhooks,
    );
    let stamped = &apps["assistant"].webhook_subscriptions;
    assert_eq!(stamped.len(), 1);
    assert_eq!(stamped[0].endpoint_slug, "ep");
}

// -----------------------------------------------------------------------
// Refusals — each in boot's own words, produced by the shared body.
// -----------------------------------------------------------------------

/// The push layer's existence is a property of the declared `[pwa_push]`
/// section, so a grant with no section is refused rather than silently arming
/// the WS dispatch handlers' `expect`.
#[test]
#[should_panic(expected = "no [pwa_push] section is")]
fn pwa_push_grant_without_section_panics() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = document(dir.path(), "ch");
    config.apps[0].grants.push(AppCapability::PwaPush);
    let _ = candidate_apps(&config);
}

/// The same refusal on the boot path: one body, one diagnostic.
#[test]
#[should_panic(expected = "no [pwa_push] section is")]
fn pwa_push_grant_without_section_panics_at_boot() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = document(dir.path(), "ch");
    config.apps[0].grants.push(AppCapability::PwaPush);
    let _ = booted_apps(&config);
}

/// A declared section with no granted app is not a refusal, and it does not
/// suppress the layer either (`resolve_pwa_push_layer` covers the layer half).
#[test]
fn pwa_push_section_without_grant_is_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let keypair = dir.path().join("vapid.json");
    let mut config = document(dir.path(), "ch");
    config.pwa_push = crate::pwa_push::config::PwaPushGlobalConfig {
        keypair_file: Some(keypair),
        subject: Some("mailto:dev@example.com".to_string()),
        ..Default::default()
    };
    let ResolvedConfig { pwa_push, .. } = validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
    assert!(
        pwa_push.is_some(),
        "a declared section has a layer whether or not any agent holds the grant",
    );
}

#[test]
#[should_panic(expected = "is not a known [[channel]] address")]
fn subscribe_to_undeclared_channel_panics() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = document(dir.path(), "ch");
    config.apps[0]
        .messaging
        .as_mut()
        .unwrap()
        .subscribe
        .push(MessagingSubscriptionRaw {
            channel: "brenn:nope".to_string(),
            push_depth: Some(Depth::Bounded(1)),
            retain_depth: Some(Depth::Bounded(1)),
            noise: None,
            wake_min: None,
        });
    let _ = candidate_apps(&config);
}

#[test]
#[should_panic(expected = "multiuser")]
fn multiuser_without_allowed_users_panics() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = document(dir.path(), "ch");
    config.apps[0].singleton = false;
    config.apps[0].compact_soft_pct = None;
    config.apps[0].multiuser = true;
    config.apps[0].allowed_users = vec![];
    config.apps[0].messaging = None;
    let _ = candidate_apps(&config);
}

#[test]
#[should_panic(expected = "singleton apps require compaction settings")]
fn singleton_without_compaction_panics() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = document(dir.path(), "ch");
    config.apps[0].compact_soft_pct = None;
    let _ = candidate_apps(&config);
}

#[test]
#[should_panic(expected = "does not exist or is not a directory")]
fn working_dir_that_is_not_a_directory_panics() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = document(dir.path(), "ch");
    config.apps[0].working_dir = Some(dir.path().join("gone"));
    let _ = candidate_apps(&config);
}

#[test]
#[should_panic(expected = "names unconfigured MQTT client")]
fn acl_naming_an_unknown_mqtt_client_panics() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = document(dir.path(), "ch");
    config.apps[0].grants.push(AppCapability::MqttPublish);
    config.apps[0]
        .acl
        .mqtt_publish
        .push(crate::access::raw::MqttClientMatcherRaw {
            client: "ha".to_string(),
        });
    // The supplied identity map is empty: no `[[mqtt_client]]` named `ha`.
    let _ = candidate_apps(&config);
}
