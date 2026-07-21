use super::*;
use crate::config::ResolvedConfig;
use crate::integration::IntegrationRegistry;

// -----------------------------------------------------------------------
// validate_and_resolve
// -----------------------------------------------------------------------

#[test]
fn validate_resolves_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        claude_defaults: ClaudeDefaultsConfig {
            model: "sonnet".to_string(),
            ..Default::default()
        },
        apps: vec![AppConfigRaw {
            slug: "pfin".to_string(),
            working_dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let ResolvedConfig { apps, .. } = validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
    assert_eq!(apps.len(), 1);
    let app = &apps["pfin"];
    assert_eq!(app.name, "pfin"); // defaults to slug
    assert_eq!(app.model, "sonnet"); // from claude_defaults
}

#[test]
fn validate_per_app_model_override() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        claude_defaults: ClaudeDefaultsConfig {
            model: "sonnet".to_string(),
            ..Default::default()
        },
        apps: vec![AppConfigRaw {
            slug: "pfin".to_string(),
            name: Some("Personal Finance".to_string()),
            working_dir: Some(dir.path().to_path_buf()),
            model: Some("opus".to_string()),
            single_instance: true,
            allowed_users: vec!["alice".to_string()],
            disabled_tools: vec!["Bash".to_string()],
            ..Default::default()
        }],
        ..Default::default()
    };
    let ResolvedConfig { apps, .. } = validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
    let app = &apps["pfin"];
    assert_eq!(app.name, "Personal Finance");
    assert_eq!(app.model, "opus"); // per-app override
    assert!(app.single_instance);
    assert_eq!(app.allowed_users, vec!["alice"]);
    assert_eq!(app.disabled_tools, vec!["Bash"]);
}

#[test]
#[should_panic(expected = "at least one [[app]] must be defined")]
fn validate_no_apps_panics() {
    let config = BrennConfig::default();
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
#[should_panic(expected = "invalid app slug")]
fn validate_invalid_slug_uppercase_panics() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "PFin".to_string(),
            working_dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        }],
        ..Default::default()
    };
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
#[should_panic(expected = "invalid app slug")]
fn validate_invalid_slug_leading_hyphen_panics() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "-pfin".to_string(),
            working_dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        }],
        ..Default::default()
    };
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
#[should_panic(expected = "invalid app slug")]
fn validate_invalid_slug_special_chars_panics() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "my_app".to_string(),
            working_dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        }],
        ..Default::default()
    };
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
#[should_panic(expected = "duplicate app slug")]
fn validate_duplicate_slugs_panics() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![
            AppConfigRaw {
                slug: "pfin".to_string(),
                working_dir: Some(dir.path().to_path_buf()),
                ..Default::default()
            },
            AppConfigRaw {
                slug: "pfin".to_string(),
                working_dir: Some(dir.path().to_path_buf()),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
}

#[test]
#[should_panic(expected = "does not exist or is not a directory")]
fn validate_nonexistent_working_dir_panics() {
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "pfin".to_string(),
            working_dir: Some(PathBuf::from("/nonexistent/path")),
            ..Default::default()
        }],
        ..Default::default()
    };
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
#[should_panic(expected = "singleton")]
fn validate_singleton_multiuser_panics() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "pa".to_string(),
            working_dir: Some(dir.path().to_path_buf()),
            singleton: true,
            compact_reminder_pct: Some(60),
            multiuser: true,
            ..Default::default()
        }],
        ..Default::default()
    };
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
#[should_panic(expected = "requires a non-empty `allowed_users` list")]
fn validate_multiuser_without_allowed_users_panics() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "collab".to_string(),
            working_dir: Some(dir.path().to_path_buf()),
            multiuser: true,
            allowed_users: vec![], // empty — must panic
            ..Default::default()
        }],
        ..Default::default()
    };
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
#[should_panic(expected = "singleton apps require compaction")]
fn validate_singleton_requires_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "pa".to_string(),
            working_dir: Some(dir.path().to_path_buf()),
            singleton: true,
            ..Default::default()
        }],
        ..Default::default()
    };
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
#[should_panic(expected = "compaction settings require")]
fn validate_compaction_without_singleton_panics() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "dev".to_string(),
            working_dir: Some(dir.path().to_path_buf()),
            compact_reminder_pct: Some(60),
            ..Default::default()
        }],
        ..Default::default()
    };
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
#[should_panic(expected = "compact_soft_pct")]
fn validate_compaction_soft_exceeds_hard_panics() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "pa".to_string(),
            working_dir: Some(dir.path().to_path_buf()),
            singleton: true,
            compact_reminder_pct: Some(50),
            compact_soft_pct: Some(90),
            compact_hard_pct: Some(80),
            ..Default::default()
        }],
        ..Default::default()
    };
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
#[should_panic(expected = "compact_soft_pct")]
fn validate_compaction_soft_exceeds_default_hard_panics() {
    // Setting soft=90 without setting hard should fail because
    // the default hard is 95 and soft must be <= red (default 80).
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "pa".to_string(),
            working_dir: Some(dir.path().to_path_buf()),
            singleton: true,
            compact_reminder_pct: Some(50),
            compact_soft_pct: Some(90),
            ..Default::default()
        }],
        ..Default::default()
    };
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
#[should_panic(expected = "compact_reminder_pct")]
fn validate_compaction_reminder_exceeds_soft_panics() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "pa".to_string(),
            working_dir: Some(dir.path().to_path_buf()),
            singleton: true,
            compact_reminder_pct: Some(80), // > soft_pct (75 default)
            ..Default::default()
        }],
        ..Default::default()
    };
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
#[should_panic(expected = "compact_red_pct")]
fn validate_compaction_red_exceeds_hard_panics() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "pa".to_string(),
            working_dir: Some(dir.path().to_path_buf()),
            singleton: true,
            compact_reminder_pct: Some(50),
            compact_soft_pct: Some(60),
            compact_red_pct: Some(96), // > hard_pct (95 default)
            ..Default::default()
        }],
        ..Default::default()
    };
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

/// Build a minimal singleton `AppConfigRaw` with the given
/// `compact_*_tokens` thresholds and a tempdir working directory.
fn raw_with_token_thresholds(
    dir: &std::path::Path,
    reminder_tokens: Option<u64>,
    soft_tokens: Option<u64>,
    red_tokens: Option<u64>,
    hard_tokens: Option<u64>,
) -> AppConfigRaw {
    AppConfigRaw {
        slug: "pa".to_string(),
        working_dir: Some(dir.to_path_buf()),
        singleton: true,
        compact_reminder_tokens: reminder_tokens,
        compact_soft_tokens: soft_tokens,
        compact_red_tokens: red_tokens,
        compact_hard_tokens: hard_tokens,
        ..Default::default()
    }
}

#[test]
#[should_panic(expected = "compact_reminder_tokens")]
fn validate_compaction_reminder_tokens_exceeds_soft_panics() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![raw_with_token_thresholds(
            dir.path(),
            Some(200_000),
            Some(150_000), // soft < reminder — invalid.
            None,
            None,
        )],
        ..Default::default()
    };
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
#[should_panic(expected = "compact_soft_tokens")]
fn validate_compaction_soft_tokens_exceeds_red_panics() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![raw_with_token_thresholds(
            dir.path(),
            None,
            Some(300_000),
            Some(200_000), // red < soft — invalid.
            None,
        )],
        ..Default::default()
    };
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
#[should_panic(expected = "compact_red_tokens")]
fn validate_compaction_red_tokens_exceeds_hard_panics() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![raw_with_token_thresholds(
            dir.path(),
            None,
            None,
            Some(400_000),
            Some(300_000), // hard < red — invalid.
        )],
        ..Default::default()
    };
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
#[should_panic(expected = "must be >= 1000")]
fn validate_compaction_reminder_tokens_below_1000_panics() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![raw_with_token_thresholds(
            dir.path(),
            Some(200), // Almost certainly meant 200_000.
            None,
            None,
            None,
        )],
        ..Default::default()
    };
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
fn validate_compaction_soft_tokens_only_succeeds() {
    // Setting only `compact_soft_tokens` (no other compaction fields)
    // should resolve to a CompactionConfig with percentage defaults
    // plus the soft_tokens populated.
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![raw_with_token_thresholds(
            dir.path(),
            None,
            Some(200_000),
            None,
            None,
        )],
        ..Default::default()
    };
    let ResolvedConfig { apps: resolved, .. } = validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
    let app = resolved.values().next().expect("one app resolved");
    let comp = app.compaction.as_ref().expect("compaction populated");
    assert_eq!(comp.reminder_pct, CompactionConfig::DEFAULT_REMINDER_PCT);
    assert_eq!(comp.soft_pct, CompactionConfig::DEFAULT_SOFT_PCT);
    assert_eq!(comp.red_pct, CompactionConfig::DEFAULT_RED_PCT);
    assert_eq!(comp.hard_pct, CompactionConfig::DEFAULT_HARD_PCT);
    assert_eq!(comp.reminder_tokens, None);
    assert_eq!(comp.soft_tokens, Some(200_000));
    assert_eq!(comp.red_tokens, None);
    assert_eq!(comp.hard_tokens, None);
}

#[test]
fn validate_compaction_mixed_pct_and_tokens_succeeds() {
    // Mixed: percentages for some stages, absolute tokens for others.
    // Each stage's threshold is independent — no cross-validation.
    let dir = tempfile::tempdir().unwrap();
    let mut raw = raw_with_token_thresholds(
        dir.path(),
        None,
        Some(200_000), // soft via tokens
        None,
        Some(500_000), // hard via tokens
    );
    raw.compact_reminder_pct = Some(50); // reminder via pct
    raw.compact_red_pct = Some(78); // red via pct
    let config = BrennConfig {
        apps: vec![raw],
        ..Default::default()
    };
    let ResolvedConfig { apps: resolved, .. } = validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
    let app = resolved.values().next().expect("one app resolved");
    let comp = app.compaction.as_ref().expect("compaction populated");
    assert_eq!(comp.reminder_pct, 50);
    assert_eq!(comp.red_pct, 78);
    assert_eq!(comp.soft_tokens, Some(200_000));
    assert_eq!(comp.hard_tokens, Some(500_000));
    assert_eq!(comp.reminder_tokens, None);
    assert_eq!(comp.red_tokens, None);
}

#[test]
fn validate_compaction_idle_default_is_270() {
    // Setting any compaction field with `compact_idle_secs` unset
    // should produce idle_duration = 270s (the prompt-cache TTL margin).
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![raw_with_token_thresholds(
            dir.path(),
            None,
            Some(200_000),
            None,
            None,
        )],
        ..Default::default()
    };
    let ResolvedConfig { apps: resolved, .. } = validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
    let app = resolved.values().next().expect("one app resolved");
    let comp = app.compaction.as_ref().expect("compaction populated");
    assert_eq!(comp.idle_duration, std::time::Duration::from_secs(270));
}

#[test]
fn user_has_access_empty_allows_all() {
    let app = AppConfig {
        slug: "pfin".to_string(),
        name: "pfin".to_string(),
        description: String::new(),
        icon: String::new(),
        working_dir: PathBuf::from("/tmp"),
        model: "sonnet".to_string(),
        single_instance: false,
        singleton: false,
        persistent: false,
        idle_timeout: None,
        compaction: None,
        idle_hook_secs: 0,
        allowed_users: vec![],
        disabled_tools: vec![],
        mcp_servers: HashMap::new(),
        multiuser: false,
        prefix_username: false,
        prefix_timestamp: false,
        prefix_device: true,
        path_mapper: PathMapper::Identity,
        container_spawn: None,
        start_hooks: StartHooksConfig::default(),
        post_pull_hooks: PostPullHooksConfig::default(),
        startup_hooks: StartupHooksConfig::default(),
        cc_extra_args: vec![],
        approval_rules: vec![],
        attachment_targets: vec![],
        integrations: HashMap::new(),
        mounts: vec![],
        history_replay_limit: 2000,
        frontmatter: FrontmatterRenderConfig::default(),
        state_dir: PathBuf::from("/tmp/.brenn/test-state"),
        messaging: None,
        pwa_push: None,
        messaging_default_send_budget: 100,
        policy: crate::access::AppPolicy::default(),
        webhook_subscriptions: vec![],
        mqtt_subscriptions: vec![],
    };
    assert!(app.user_has_access("anyone"));
}

#[test]
fn user_has_access_restricted() {
    let app = AppConfig {
        slug: "pfin".to_string(),
        name: "pfin".to_string(),
        description: String::new(),
        icon: String::new(),
        working_dir: PathBuf::from("/tmp"),
        model: "sonnet".to_string(),
        single_instance: false,
        singleton: false,
        persistent: false,
        idle_timeout: None,
        compaction: None,
        idle_hook_secs: 0,
        allowed_users: vec!["alice".to_string()],
        disabled_tools: vec![],
        mcp_servers: HashMap::new(),
        multiuser: false,
        prefix_username: false,
        prefix_timestamp: false,
        prefix_device: true,
        path_mapper: PathMapper::Identity,
        container_spawn: None,
        start_hooks: StartHooksConfig::default(),
        post_pull_hooks: PostPullHooksConfig::default(),
        startup_hooks: StartupHooksConfig::default(),
        cc_extra_args: vec![],
        approval_rules: vec![],
        attachment_targets: vec![],
        integrations: HashMap::new(),
        mounts: vec![],
        history_replay_limit: 2000,
        frontmatter: FrontmatterRenderConfig::default(),
        state_dir: PathBuf::from("/tmp/.brenn/test-state"),
        messaging: None,
        pwa_push: None,
        messaging_default_send_budget: 100,
        policy: crate::access::AppPolicy::default(),
        webhook_subscriptions: vec![],
        mqtt_subscriptions: vec![],
    };
    assert!(app.user_has_access("alice"));
    assert!(!app.user_has_access("bob"));
}

/// Build a minimal `AppConfig` for `messaging_send_budget` tests.
fn minimal_app_config_for_budget_test(
    messaging: Option<crate::messaging::config::ResolvedMessagingConfig>,
    global_default: u32,
) -> AppConfig {
    AppConfig {
        slug: "test".to_string(),
        name: "test".to_string(),
        description: String::new(),
        icon: String::new(),
        working_dir: PathBuf::from("/tmp"),
        model: "sonnet".to_string(),
        single_instance: false,
        singleton: false,
        persistent: false,
        idle_timeout: None,
        compaction: None,
        idle_hook_secs: 0,
        allowed_users: vec![],
        disabled_tools: vec![],
        mcp_servers: HashMap::new(),
        multiuser: false,
        prefix_username: false,
        prefix_timestamp: false,
        prefix_device: true,
        path_mapper: PathMapper::Identity,
        container_spawn: None,
        start_hooks: StartHooksConfig::default(),
        post_pull_hooks: PostPullHooksConfig::default(),
        startup_hooks: StartupHooksConfig::default(),
        cc_extra_args: vec![],
        approval_rules: vec![],
        attachment_targets: vec![],
        integrations: HashMap::new(),
        mounts: vec![],
        history_replay_limit: 2000,
        frontmatter: FrontmatterRenderConfig::default(),
        state_dir: PathBuf::from("/tmp/.brenn/test-state"),
        messaging,
        messaging_default_send_budget: global_default,
        policy: crate::access::AppPolicy::default(),
        pwa_push: None,
        webhook_subscriptions: vec![],
        mqtt_subscriptions: vec![],
    }
}

/// `messaging_send_budget` returns the per-app override when present
/// (design §7.7 — first tier).
#[test]
fn messaging_send_budget_uses_per_app_override() {
    let messaging = crate::messaging::config::ResolvedMessagingConfig {
        send_budget: 42,
        subscriptions: vec![],
    };
    let app = minimal_app_config_for_budget_test(Some(messaging), 100);
    assert_eq!(app.messaging_send_budget(), 42);
}

/// With no per-app messaging block, `messaging_send_budget` falls
/// back to the global default (design §7.7 — second tier).
#[test]
fn messaging_send_budget_falls_back_to_global_default() {
    let app = minimal_app_config_for_budget_test(None, 50);
    assert_eq!(app.messaging_send_budget(), 50);
}

/// `messaging_default_send_budget` is hand-set on every `AppConfig`
/// from the global `[messaging].default_send_budget`. The default
/// for `MessagingGlobalConfig` is 100, so an app constructed with
/// that value returns 100 (design §7.7 — third tier).
#[test]
fn messaging_send_budget_default_is_one_hundred() {
    let default = crate::messaging::config::MessagingGlobalConfig::default();
    let app = minimal_app_config_for_budget_test(None, default.default_send_budget);
    assert_eq!(app.messaging_send_budget(), 100);
}

/// `messaging_enabled()` is sourced from the `MessagingPublish`/
/// `MessagingSubscribe` *policy grants*, not from `[app.messaging]` section
/// presence/`enabled` (access-control Phase 0, §2.5.1/§2.7). These tests pin the
/// decoupling so a future re-coupling (re-reading the section) is caught here,
/// not only by incidental downstream coverage.
#[test]
fn messaging_enabled_reads_grant_not_section_present_block_without_grant() {
    // Raw `[app.messaging]` block present and enabled, but no messaging grant on
    // the policy → `messaging_enabled()` is `false` (the section no longer
    // authorizes).
    let messaging = crate::messaging::config::ResolvedMessagingConfig {
        send_budget: 10,
        subscriptions: vec![],
    };
    let app = minimal_app_config_for_budget_test(Some(messaging), 100);
    assert!(
        !app.policy
            .has_grant(crate::access::AppCapability::MessagingPublish)
    );
    assert!(
        !app.policy
            .has_grant(crate::access::AppCapability::MessagingSubscribe)
    );
    assert!(!app.messaging_enabled());
}

#[test]
fn messaging_enabled_reads_grant_not_section_grant_without_block() {
    // No `[app.messaging]` block at all, but a messaging grant is on the policy →
    // `messaging_enabled()` is `true` (the grant is the sole authority). Either
    // grant (publish or subscribe) suffices — both arms of the `||` are pinned
    // so an accidental `&&`, or a dropped arm, regresses a test.
    //
    // Subscribe arm.
    let mut app = minimal_app_config_for_budget_test(None, 100);
    assert!(app.messaging.is_none());
    app.policy
        .grants
        .insert(crate::access::AppCapability::MessagingSubscribe);
    assert!(app.messaging_enabled());

    // Publish arm (a distinct app holding only `MessagingPublish`).
    let mut publish_only = minimal_app_config_for_budget_test(None, 100);
    assert!(publish_only.messaging.is_none());
    publish_only
        .policy
        .grants
        .insert(crate::access::AppCapability::MessagingPublish);
    assert!(
        !publish_only
            .policy
            .has_grant(crate::access::AppCapability::MessagingSubscribe)
    );
    assert!(publish_only.messaging_enabled());
}

/// `pwa_push_enabled()` is sourced from the `PwaPush` *policy grant*, not from
/// `[app.pwa_push].enabled` section presence (access-control Phase 0,
/// §2.5.1/§2.7). These two tests directly pin the decoupling so a future
/// re-coupling (e.g. re-reading the section as a fallback) is caught by a
/// single-method test, not only by incidental downstream coverage.
#[test]
fn pwa_push_enabled_reads_grant_not_section_present_block_without_grant() {
    // Raw `[app.pwa_push]` block present and enabled, but no PwaPush grant on the
    // policy → `pwa_push_enabled()` is `false` (the section no longer authorizes).
    let mut app = minimal_app_config_for_budget_test(None, 100);
    app.pwa_push = Some(crate::pwa_push::config::AppPwaPushBlock {
        default_title: None,
    });
    assert!(!app.policy.has_grant(crate::access::AppCapability::PwaPush));
    assert!(!app.pwa_push_enabled());
}

#[test]
fn pwa_push_enabled_reads_grant_not_section_grant_without_block() {
    // No `[app.pwa_push]` block at all, but the PwaPush grant is on the policy →
    // `pwa_push_enabled()` is `true` (the grant is the sole authority).
    let mut app = minimal_app_config_for_budget_test(None, 100);
    assert!(app.pwa_push.is_none());
    app.policy
        .grants
        .insert(crate::access::AppCapability::PwaPush);
    assert!(app.pwa_push_enabled());
}

/// End-to-end through `validate_and_resolve`: an app's
/// `[[app.mqtt_subscription]]` block must be resolved and stamped onto
/// `AppConfig::mqtt_subscriptions`. This guards the wiring gap where
/// `resolve_app_mqtt_subscriptions` existed and was unit-tested but was never
/// called from the production resolution path, leaving the field `vec![]` and
/// the MQTT bridge feature silently inert (every `mqtt:` channel got an empty
/// subscriber set). The per-broker unit tests bypass `validate_and_resolve`
/// entirely, so only an end-to-end test catches this.
#[test]
fn validate_resolves_app_mqtt_subscriptions() {
    use crate::mqtt::config::AppMqttIngressSubscriptionRaw;

    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "pa".to_string(),
            working_dir: Some(dir.path().to_path_buf()),
            // Pull-only (push_depth = 0) so the consuming app need not be a
            // singleton (which would require compaction settings) — the
            // push-enabled singleton path is covered by the mqtt/config unit tests.
            mqtt_subscriptions: vec![AppMqttIngressSubscriptionRaw {
                channel: "mqtt:ha:home/+/state".to_string(),
                push_depth: Some(crate::messaging::config::Depth::Bounded(0)),
                retain_depth: None,
                noise: None,
                wake_min: None,
            }],
            ..Default::default()
        }],
        mqtt_clients: vec![ingress_test_broker("ha")],
        ..Default::default()
    };

    let ResolvedConfig {
        apps,
        mqtt_ingress_channels,
        ..
    } = validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
    let app = &apps["pa"];
    assert_eq!(
        app.mqtt_subscriptions.len(),
        1,
        "app.mqtt_subscriptions must be populated from [[app.mqtt_subscription]]"
    );
    assert_eq!(
        app.mqtt_subscriptions[0].channel_address,
        "mqtt:ha:home/+/state"
    );
    assert_eq!(app.mqtt_subscriptions[0].client_slug, "ha");
    // The distinct ingress channel set must carry the one channel.
    assert_eq!(mqtt_ingress_channels.len(), 1);
    assert_eq!(
        mqtt_ingress_channels[0].channel_address,
        "mqtt:ha:home/+/state"
    );
    assert_eq!(mqtt_ingress_channels[0].qos, 1, "qos from the broker");
}

/// WASM-consumer / app slug disjointness: a `[[wasm_consumer]]` must not share its
/// slug with an `[[app]]`. Per-owner resources are keyed by the raw, unprefixed
/// owner slug (no `wasm:`/`app:` prefix), so a collision would let one owner resolve
/// against the other's resources. `validate_and_resolve` must refuse such a config
/// at boot.
///
/// Placement note: the check lives in `validate_and_resolve` (the config-resolution
/// layer) and is tested here. `validate_and_resolve` is called exactly once, upstream
/// of `build_messaging` and every other bootstrap step (`bootstrap/mod.rs`), so there
/// is no production path by which a later layer could observe a config the
/// disjointness check did not already reject.
#[test]
#[should_panic(expected = "collides with an [[app]] slug")]
fn validate_wasm_consumer_slug_colliding_with_app_panics() {
    use crate::access::raw::MqttClientMatcherRaw;
    use crate::messaging::config::{WasmConsumerConfigRaw, WasmGrant};

    let dir = tempfile::tempdir().unwrap();
    // WASM consumer whose slug equals the app slug below ("pa").
    let colliding = WasmConsumerConfigRaw {
        slug: "pa".to_string(),
        component_path: dir.path().join("c.wasm"),
        grants: vec![WasmGrant::Mqtt],
        store_path: None,
        store_size_limit: None,
        subscriptions: vec![],
        outputs: vec![],
        subscribe_acl: vec![],
        publish_acl: vec![],
        mqtt_publish_acl: vec![MqttClientMatcherRaw {
            client: "ha".to_string(),
        }],
        mqtt_subscribe_acl: vec![],
        webhook_acl: vec![],
        config: None,
        activation_burst: None,
        activation_min_period_ms: None,
        mqtt_outputs: vec![],
        tool_grants: vec![],
    };

    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "pa".to_string(),
            working_dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        }],
        wasm_consumers: vec![colliding],
        mqtt_clients: vec![ingress_test_broker("ha")],
        ..Default::default()
    };

    validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
}

/// End-to-end: an `[[app.mqtt_subscription]]` naming a client that is not a
/// declared `[[mqtt_client]]` must panic at config-resolve (fail-fast), not be
/// silently accepted. This exercises the production call into
/// `resolve_app_mqtt_subscriptions`.
#[test]
#[should_panic(expected = "not declared in any [[mqtt_client]] block")]
fn validate_unknown_client_subscription_panics() {
    use crate::mqtt::config::AppMqttIngressSubscriptionRaw;

    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "pa".to_string(),
            working_dir: Some(dir.path().to_path_buf()),
            // Pull-only so the unknown-client check is what fails (not the
            // push-enabled singleton requirement).
            mqtt_subscriptions: vec![AppMqttIngressSubscriptionRaw {
                channel: "mqtt:nonexistent:home/+/state".to_string(),
                push_depth: Some(crate::messaging::config::Depth::Bounded(0)),
                retain_depth: None,
                noise: None,
                wake_min: None,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
}

/// Build a pull-only ingress sub raw for the given channel address.
#[cfg(test)]
fn pull_only_ingress_sub(channel: &str) -> crate::mqtt::config::AppMqttIngressSubscriptionRaw {
    crate::mqtt::config::AppMqttIngressSubscriptionRaw {
        channel: channel.to_string(),
        push_depth: Some(crate::messaging::config::Depth::Bounded(0)),
        retain_depth: None,
        noise: None,
        wake_min: None,
    }
}

/// Build a single `[[mqtt_client]]` raw with the given slug.
#[cfg(test)]
fn ingress_test_broker(slug: &str) -> crate::mqtt::config::MqttClientConfigRaw {
    crate::mqtt::config::MqttClientConfigRaw {
        slug: slug.to_string(),
        url: "mqtts://broker.example.com:8883".to_string(),
        username: None,
        password_file: None,
        ca_file: None,
        tls_version_min: "1.2".to_string(),
        keepalive_secs: None,
        inbound_payload_cap_bytes: 4 * 1024 * 1024,
        last_will: None,
        reconnect_backoff_initial_secs: 1,
        reconnect_backoff_max_secs: 60,
        qos: 1,
        urgency: crate::messaging::Urgency::Normal,
        session_expiry_secs: 0,
    }
}

/// Two apps subscribing to the SAME `(client, topic)` collapse to ONE
/// `ResolvedMqttIngressChannel` (dedup by `channel_uuid`) — one upstream
/// SUBSCRIBE, fanned out to both subscribers (design decision 9).
#[test]
fn validate_two_apps_same_channel_dedup_to_one_ingress_channel() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![
            AppConfigRaw {
                slug: "a".to_string(),
                working_dir: Some(dir_a.path().to_path_buf()),
                mqtt_subscriptions: vec![pull_only_ingress_sub("mqtt:ha:home/+/state")],
                ..Default::default()
            },
            AppConfigRaw {
                slug: "b".to_string(),
                working_dir: Some(dir_b.path().to_path_buf()),
                mqtt_subscriptions: vec![pull_only_ingress_sub("mqtt:ha:home/+/state")],
                ..Default::default()
            },
        ],
        mqtt_clients: vec![ingress_test_broker("ha")],
        ..Default::default()
    };

    let ResolvedConfig {
        apps,
        mqtt_ingress_channels,
        ..
    } = validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );

    assert_eq!(
        mqtt_ingress_channels.len(),
        1,
        "same (client, topic) across two apps must dedup to one ingress channel"
    );
    // Both apps still carry their own resolved subscription.
    assert_eq!(apps["a"].mqtt_subscriptions.len(), 1);
    assert_eq!(apps["b"].mqtt_subscriptions.len(), 1);
}

/// The same topic on two DIFFERENT clients yields TWO distinct ingress channels
/// (the client disambiguates — design decision 9).
#[test]
fn validate_same_topic_two_clients_yields_two_channels() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![
            AppConfigRaw {
                slug: "a".to_string(),
                working_dir: Some(dir_a.path().to_path_buf()),
                mqtt_subscriptions: vec![pull_only_ingress_sub("mqtt:ha:home/+/state")],
                ..Default::default()
            },
            AppConfigRaw {
                slug: "b".to_string(),
                working_dir: Some(dir_b.path().to_path_buf()),
                mqtt_subscriptions: vec![pull_only_ingress_sub("mqtt:ha2:home/+/state")],
                ..Default::default()
            },
        ],
        mqtt_clients: vec![ingress_test_broker("ha"), ingress_test_broker("ha2")],
        ..Default::default()
    };

    let ResolvedConfig {
        mqtt_ingress_channels,
        ..
    } = validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );

    assert_eq!(
        mqtt_ingress_channels.len(),
        2,
        "same topic on two distinct clients must be two distinct channels"
    );
}

// -----------------------------------------------------------------------
// Access policy resolution (access-control design §2.5.2/§2.5.3, §6.6)
// -----------------------------------------------------------------------

/// An app authored with explicit `grants` + `[app.acl.*]` resolves to an
/// `AppPolicy` whose grants and matchers match the authored config exactly (the
/// §2.5.3 explicit-build contract — no projection from legacy fields).
#[test]
fn validate_resolves_explicit_grants_and_acl_into_policy() {
    use crate::access::AppCapability;
    use crate::access::acl::{ChannelMatcher, MqttClientMatcher, MqttSubMatcher, WebhookMatcher};
    use crate::access::raw::{
        AppAclRaw, ChannelMatcherRaw, MqttClientMatcherRaw, MqttSubMatcherRaw, WebhookMatcherRaw,
    };

    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        // Both ACL-referenced MQTT clients must be configured (resolution
        // cross-checks each matcher's client against `[[mqtt_client]]`).
        mqtt_clients: vec![ingress_test_broker("home"), ingress_test_broker("office")],
        apps: vec![AppConfigRaw {
            slug: "home".to_string(),
            working_dir: Some(dir.path().to_path_buf()),
            grants: vec![
                AppCapability::DynamicSubscribe,
                AppCapability::MqttSubscribe,
                AppCapability::MqttPublish,
                AppCapability::MessagingSubscribe,
                AppCapability::Webhook,
            ],
            acl: AppAclRaw {
                mqtt_subscribe: vec![MqttSubMatcherRaw {
                    client: "home".to_string(),
                    topic_filter: "sensors/+/temp".to_string(),
                }],
                // mqtt_publish + brenn_publish authored so the integration path
                // exercises those lists too (guards against a list-swap or silent
                // drop in `resolve_access_policies`).
                mqtt_publish: vec![MqttClientMatcherRaw {
                    client: "office".to_string(),
                }],
                brenn_subscribe: vec![ChannelMatcherRaw::Prefix("alerts.".to_string())],
                brenn_publish: vec![ChannelMatcherRaw::Exact("outbox".to_string())],
                ephemeral_publish: vec![],
                webhook: vec![WebhookMatcherRaw {
                    endpoint: "github".to_string(),
                }],
            },
            ..Default::default()
        }],
        ..Default::default()
    };

    let ResolvedConfig { apps, .. } = validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );

    let policy = &apps["home"].policy;
    assert!(policy.has_grant(AppCapability::DynamicSubscribe));
    assert!(policy.has_grant(AppCapability::MqttSubscribe));
    assert!(policy.has_grant(AppCapability::MqttPublish));
    assert!(policy.has_grant(AppCapability::MessagingSubscribe));
    assert!(policy.has_grant(AppCapability::Webhook));
    assert!(!policy.has_grant(AppCapability::PwaPush));

    assert_eq!(
        policy.acls.mqtt_subscribe,
        vec![MqttSubMatcher {
            client: "home".to_string(),
            topic_filter: "sensors/+/temp".to_string(),
        }]
    );
    assert_eq!(
        policy.acls.mqtt_publish,
        vec![MqttClientMatcher {
            client: "office".to_string(),
        }]
    );
    assert_eq!(
        policy.acls.brenn_subscribe,
        vec![ChannelMatcher::Prefix("alerts.".to_string())]
    );
    assert_eq!(
        policy.acls.brenn_publish,
        vec![ChannelMatcher::Exact("outbox".to_string())]
    );
    assert_eq!(
        policy.acls.webhook,
        vec![WebhookMatcher {
            endpoint: "github".to_string(),
        }]
    );
}

/// An app with no `grants`/`acl` resolves to the default (empty, deny-everything)
/// policy — deny-by-default end to end.
#[test]
fn validate_app_without_grants_resolves_default_deny_policy() {
    use crate::access::AppCapability;

    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "home".to_string(),
            working_dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        }],
        ..Default::default()
    };

    let ResolvedConfig { apps, .. } = validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );

    let policy = &apps["home"].policy;
    assert!(!policy.has_grant(AppCapability::DynamicSubscribe));
    assert!(policy.acls.mqtt_subscribe.is_empty());
    assert!(!policy.allows_mqtt_dynamic_subscribe("home", "sensors/temp"));
}

/// A publishing app (`MessagingPublish` grant) that authors **no**
/// `brenn_publish` matcher resolves — without panicking — to a deny-all publish
/// policy: the grant is present but `allows_brenn_publish` denies every channel
/// (access-control design §3 failure mode 1, §4 config-shape clause). The
/// granted-but-no-matcher state is a legitimate intermediate (a deferred ACL),
/// so resolution emits a non-fatal warning rather than failing fast.
#[test]
fn validate_publish_grant_without_matcher_resolves_deny_all_no_panic() {
    use crate::access::AppCapability;

    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "home".to_string(),
            working_dir: Some(dir.path().to_path_buf()),
            // Holds the publish grant but authors no `brenn_publish` matcher.
            grants: vec![AppCapability::MessagingPublish],
            ..Default::default()
        }],
        ..Default::default()
    };

    let ResolvedConfig { apps, .. } = validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );

    let policy = &apps["home"].policy;
    // Layer-1 grant resolved, layer-2 list empty ⇒ deny-all publish.
    assert!(policy.has_grant(AppCapability::MessagingPublish));
    assert!(policy.acls.brenn_publish.is_empty());
    assert!(!policy.allows_brenn_publish("anything"));
    assert!(!policy.allows_brenn_publish("outbox"));
}

/// A publishing app that authors explicit `[[app.acl.brenn_publish]]` matchers
/// (the prod deployment shape, scoping each app to its own outbound channels)
/// resolves to a policy whose `allows_brenn_publish` *covers* exactly the listed
/// channels and denies the rest (access-control design §3 failure mode 1, §4
/// config-shape clause — the positive half of "the updated shape resolves").
#[test]
fn validate_publish_grant_with_matchers_resolves_covering_policy() {
    use crate::access::AppCapability;
    use crate::access::raw::{AppAclRaw, ChannelMatcherRaw};

    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "pa-alice".to_string(),
            working_dir: Some(dir.path().to_path_buf()),
            grants: vec![AppCapability::MessagingPublish],
            // Mirrors the prod shape: each PA may publish to both PA inboxes.
            acl: AppAclRaw {
                brenn_publish: vec![
                    ChannelMatcherRaw::Exact("pa-alice".to_string()),
                    ChannelMatcherRaw::Exact("pa-bob".to_string()),
                ],
                ..Default::default()
            },
            ..Default::default()
        }],
        ..Default::default()
    };

    let ResolvedConfig { apps, .. } = validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );

    let policy = &apps["pa-alice"].policy;
    assert!(policy.has_grant(AppCapability::MessagingPublish));
    // The two authored channels are covered; an unlisted one is denied.
    assert!(policy.allows_brenn_publish("pa-alice"));
    assert!(policy.allows_brenn_publish("pa-bob"));
    assert!(!policy.allows_brenn_publish("some-other-channel"));
}

/// The `MqttPublish` branch of `warn_granted_publish_no_matcher`: an app holding
/// the `mqtt_publish` grant but authoring no `mqtt_publish` matcher resolves
/// without panicking (the legitimate intermediate state — access-control design §3
/// failure mode 1), keeps the layer-1 grant, and `allows_mqtt_publish` is deny-all.
/// Mirrors `validate_publish_grant_without_matcher_resolves_deny_all_no_panic` for
/// the other publish capability so both warning branches are pinned.
#[test]
fn validate_mqtt_publish_grant_without_matcher_resolves_deny_all_no_panic() {
    use crate::access::AppCapability;

    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "home".to_string(),
            working_dir: Some(dir.path().to_path_buf()),
            // Holds the mqtt_publish grant but authors no `mqtt_publish` matcher.
            grants: vec![AppCapability::MqttPublish],
            ..Default::default()
        }],
        ..Default::default()
    };

    let ResolvedConfig { apps, .. } = validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );

    let policy = &apps["home"].policy;
    // Layer-1 grant resolved, layer-2 list empty ⇒ deny-all mqtt publish.
    assert!(policy.has_grant(AppCapability::MqttPublish));
    assert!(policy.acls.mqtt_publish.is_empty());
    assert!(!policy.allows_mqtt_publish("any-client"));
}

/// A malformed `[app.acl.mqtt_subscribe]` topic filter panics at resolution
/// (operator-authored config — fail-fast, §2.5.3 / §7.1 failure mode 4).
#[test]
#[should_panic(expected = "invalid topic filter")]
fn validate_malformed_mqtt_subscribe_filter_panics() {
    use crate::access::raw::{AppAclRaw, MqttSubMatcherRaw};

    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        // The matcher's client must be configured so the client cross-check
        // passes and the *topic-filter* validation is what fails.
        mqtt_clients: vec![ingress_test_broker("home")],
        apps: vec![AppConfigRaw {
            slug: "home".to_string(),
            working_dir: Some(dir.path().to_path_buf()),
            acl: AppAclRaw {
                mqtt_subscribe: vec![MqttSubMatcherRaw {
                    client: "home".to_string(),
                    // `#` must be terminal.
                    topic_filter: "sensors/#/extra".to_string(),
                }],
                ..Default::default()
            },
            ..Default::default()
        }],
        ..Default::default()
    };

    validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
}

/// An `[app.acl.mqtt_subscribe]` matcher naming a client with no matching
/// `[[mqtt_client]]` panics at resolution — a silent never-match (deny where the
/// operator intended allow) is turned into a fail-fast startup error (§2.5.2).
#[test]
#[should_panic(expected = "unconfigured MQTT client")]
fn validate_acl_unconfigured_mqtt_client_panics() {
    use crate::access::raw::{AppAclRaw, MqttSubMatcherRaw};

    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        // Only `home` is configured; the matcher names `typo`.
        mqtt_clients: vec![ingress_test_broker("home")],
        apps: vec![AppConfigRaw {
            slug: "home".to_string(),
            working_dir: Some(dir.path().to_path_buf()),
            acl: AppAclRaw {
                mqtt_subscribe: vec![MqttSubMatcherRaw {
                    client: "typo".to_string(),
                    topic_filter: "sensors/#".to_string(),
                }],
                ..Default::default()
            },
            ..Default::default()
        }],
        ..Default::default()
    };

    validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
}

// -----------------------------------------------------------------------
// WASM-consumer MQTT ingress-channel derivation
// -----------------------------------------------------------------------

/// Build a `[[wasm_consumer]]` raw carrying a single subscription to `channel`,
/// with empty grants/ACLs and no outputs. `validate_and_resolve` does not run the
/// per-subscription port/depth/ACL validation (that is `build_messaging`
/// territory), so the minimal shape suffices to exercise the ingress walk.
#[cfg(test)]
fn wasm_consumer_with_sub(
    slug: &str,
    component_path: std::path::PathBuf,
    channel: &str,
) -> crate::messaging::config::WasmConsumerConfigRaw {
    crate::messaging::config::WasmConsumerConfigRaw::minimal(slug, component_path, &[channel])
}

/// A `[[wasm_consumer.subscription]]` naming an `mqtt:<client>:<topic>` channel
/// that no `[[app.mqtt_subscription]]` declares must still derive a
/// `ResolvedMqttIngressChannel` — otherwise the broker would never SUBSCRIBE to
/// the filter and `directory.resolve` would later panic on an unknown channel.
/// This is the WASM half of the ingress-derivation the app path already gets.
#[test]
fn validate_resolves_wasm_consumer_mqtt_ingress_channel() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "pa".to_string(),
            working_dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        }],
        wasm_consumers: vec![wasm_consumer_with_sub(
            "consume",
            dir.path().join("c.wasm"),
            "mqtt:ha:home/+/state",
        )],
        mqtt_clients: vec![ingress_test_broker("ha")],
        ..Default::default()
    };

    let ResolvedConfig {
        mqtt_ingress_channels,
        ..
    } = validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );

    assert_eq!(
        mqtt_ingress_channels.len(),
        1,
        "a WASM-only mqtt: subscription must derive exactly one ingress channel"
    );
    assert_eq!(
        mqtt_ingress_channels[0].channel_address,
        "mqtt:ha:home/+/state"
    );
    assert_eq!(mqtt_ingress_channels[0].client_slug, "ha");
    assert_eq!(mqtt_ingress_channels[0].qos, 1, "qos from the broker");
}

/// An app and a WASM consumer subscribing to the SAME `(client, topic)` collapse
/// to ONE `ResolvedMqttIngressChannel` (dedup by `channel_uuid` across both
/// sources) — one upstream SUBSCRIBE, one route, fanned out to both subscribers.
#[test]
fn validate_app_and_wasm_same_mqtt_channel_dedup_to_one() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "pa".to_string(),
            working_dir: Some(dir.path().to_path_buf()),
            mqtt_subscriptions: vec![pull_only_ingress_sub("mqtt:ha:home/+/state")],
            ..Default::default()
        }],
        wasm_consumers: vec![wasm_consumer_with_sub(
            "consume",
            dir.path().join("c.wasm"),
            "mqtt:ha:home/+/state",
        )],
        mqtt_clients: vec![ingress_test_broker("ha")],
        ..Default::default()
    };

    let ResolvedConfig {
        mqtt_ingress_channels,
        ..
    } = validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );

    assert_eq!(
        mqtt_ingress_channels.len(),
        1,
        "an app and a WASM consumer sharing one (client, topic) must dedup to one channel"
    );
}

/// A non-`mqtt:` WASM subscription (`webhook:`/`brenn:`) contributes NO ingress
/// channel — the ingress walk selects only `mqtt:` addresses.
#[test]
fn validate_wasm_non_mqtt_subscription_derives_no_ingress_channel() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "pa".to_string(),
            working_dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        }],
        wasm_consumers: vec![wasm_consumer_with_sub(
            "consume",
            dir.path().join("c.wasm"),
            "brenn:some-channel",
        )],
        ..Default::default()
    };

    let ResolvedConfig {
        mqtt_ingress_channels,
        ..
    } = validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );

    assert!(
        mqtt_ingress_channels.is_empty(),
        "a non-mqtt WASM subscription must not derive an mqtt ingress channel"
    );
}

/// A WASM `mqtt:` subscription naming a client that no `[[mqtt_client]]` declares
/// must panic at config-resolve (fail-fast), mirroring the app path.
#[test]
#[should_panic(expected = "not declared in any [[mqtt_client]] block")]
fn validate_wasm_mqtt_subscription_unknown_client_panics() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "pa".to_string(),
            working_dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        }],
        wasm_consumers: vec![wasm_consumer_with_sub(
            "consume",
            dir.path().join("c.wasm"),
            "mqtt:nonexistent:home/+/state",
        )],
        ..Default::default()
    };

    validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
}

/// A WASM `mqtt:` subscription with a malformed topic filter (`#` not final)
/// must panic at config-resolve (fail-fast), mirroring the app path.
#[test]
#[should_panic(expected = "is invalid")]
fn validate_wasm_mqtt_subscription_malformed_filter_panics() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "pa".to_string(),
            working_dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        }],
        wasm_consumers: vec![wasm_consumer_with_sub(
            "consume",
            dir.path().join("c.wasm"),
            "mqtt:ha:home/#/state",
        )],
        mqtt_clients: vec![ingress_test_broker("ha")],
        ..Default::default()
    };

    validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
}
