use super::*;
use crate::config::ResolvedConfig;
use crate::integration::IntegrationRegistry;

// -----------------------------------------------------------------------
// Container configuration validation
// -----------------------------------------------------------------------

#[test]
fn validate_container_app_resolves_correctly() {
    let home_dir = tempfile::tempdir().unwrap();
    // Working dir under home_dir so the home_dir mapping covers it.
    let working_dir = home_dir.path().join("work");
    std::fs::create_dir(&working_dir).unwrap();

    let config = BrennConfig {
        container: HashMap::from([(
            "sandbox".to_string(),
            ContainerConfig {
                image: "brenn-cc:latest".to_string(),
                home_dir: home_dir.path().to_path_buf(),
                container_home: PathBuf::from("/home/user"),
                extra_mounts: vec![],
                extra_args: vec![],
            },
        )]),
        apps: vec![AppConfigRaw {
            slug: "pfin".to_string(),
            name: None,
            description: None,
            icon: None,
            working_dir: Some(working_dir.clone()),
            model: None,
            single_instance: false,
            singleton: false,
            persistent: false,
            idle_timeout_secs: None,
            compact_reminder_pct: None,
            compact_soft_pct: None,
            compact_red_pct: None,
            compact_hard_pct: None,
            compact_reminder_tokens: None,
            compact_soft_tokens: None,
            compact_red_tokens: None,
            compact_hard_tokens: None,
            compact_idle_secs: None,
            idle_hook_secs: None,
            allowed_users: vec![],
            disabled_tools: vec![],
            mcp_servers: HashMap::new(),
            multiuser: false,
            prefix_username: None,
            prefix_timestamp: None,
            prefix_device: None,
            container: Some("sandbox".to_string()),
            container_working_dir: Some(PathBuf::from("/home/user/work")),
            start_hooks: None,
            post_pull_hooks: None,
            startup_hooks: None,
            cc_extra_args: vec![],
            approval_rules: vec![],
            attachment_targets: vec![],
            integrations: vec![],
            integration_config: HashMap::new(),
            mounts: vec![],
            extra_mounts: vec![],
            history_replay_limit: None,
            frontmatter: FrontmatterRenderConfig::default(),
            messaging: None,
            pwa_push: None,
            webhook_subscriptions: vec![],
            mqtt_subscriptions: vec![],
            grants: vec![],
            acl: crate::access::raw::AppAclRaw::default(),
            tool_grants: vec![],
        }],
        ..Default::default()
    };
    let ResolvedConfig { apps, .. } =
        validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
    let app = apps.get("pfin").unwrap();

    // PathMapper should be Container variant.
    assert!(matches!(app.path_mapper, PathMapper::Container { .. }));
    assert_eq!(
        app.path_mapper.to_host(Path::new("/home/user/work/foo.md")),
        Some(working_dir.join("foo.md")),
    );
    assert_eq!(
        app.path_mapper
            .to_container(working_dir.join("bar.md").as_path()),
        Some(PathBuf::from("/home/user/work/bar.md")),
    );

    // ContainerSpawnConfig should be populated.
    let spawn = app.container_spawn.as_ref().unwrap();
    assert_eq!(spawn.image, "brenn-cc:latest");
    assert_eq!(spawn.home_dir, home_dir.path());
    assert_eq!(spawn.container_home, PathBuf::from("/home/user"));
    assert_eq!(
        spawn.container_working_dir,
        PathBuf::from("/home/user/work")
    );

    // state_dir for containerized apps lives under home_dir (piggybacks
    // on the home_dir → container_home bind mount) and is created.
    assert_eq!(
        app.state_dir,
        home_dir.path().join(".config").join("brenn").join("pfin"),
    );
    assert!(app.state_dir.is_dir(), "state_dir must be created");

    // PathMapper must be able to translate state_dir to a container path
    // (the home_dir catch-all mapping covers it).
    let cc_visible_state = app.path_mapper.to_container(&app.state_dir);
    assert_eq!(
        cc_visible_state,
        Some(PathBuf::from("/home/user/.config/brenn/pfin")),
        "state_dir must be translatable via home_dir mapping",
    );

    // virtual_tools_path() helper composes state_dir + filename.
    assert_eq!(
        app.virtual_tools_path(),
        app.state_dir.join("virtual-tools.json"),
    );
}

/// Bare-app state_dir resolves to `<runtime_dir>/brenn/<slug>` when a valid
/// runtime_dir is injected. The coverage of the "invalid dir → panic" and
/// "unset/empty → panic" paths now lives in `runtime_dir::tests` as pure
/// `validate_runtime_dir` / `resolve_xdg_value` calls — no env mutation needed.
#[test]
fn validate_bare_app_state_dir_uses_validated_xdg_runtime_dir() {
    let xdg_dir = super::test_runtime_dir();
    let working_dir = tempfile::tempdir().unwrap();

    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "bare-test".to_string(),
            working_dir: Some(working_dir.path().to_path_buf()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let ResolvedConfig { apps, .. } =
        validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), Some(xdg_dir));
    let app = apps.get("bare-test").unwrap();

    let expected = xdg_dir.join("brenn").join("bare-test");
    assert_eq!(app.state_dir, expected);
    assert!(
        app.state_dir.is_dir(),
        "state_dir must be created (xdg branch)"
    );

    // PathMapper is Identity for bare apps.
    assert!(matches!(app.path_mapper, PathMapper::Identity));
    assert_eq!(
        app.path_mapper.to_container(&app.state_dir),
        Some(app.state_dir.clone()),
    );

    // virtual_tools_path() helper composes state_dir + filename.
    assert_eq!(
        app.virtual_tools_path(),
        app.state_dir.join("virtual-tools.json"),
    );
}

#[test]
fn validate_app_extra_mounts_appended_after_container_extra_mounts() {
    let home_dir = tempfile::tempdir().unwrap();
    let working_dir = home_dir.path().join("work");
    std::fs::create_dir(&working_dir).unwrap();

    let config = BrennConfig {
        container: HashMap::from([(
            "sandbox".to_string(),
            ContainerConfig {
                image: "brenn-cc:latest".to_string(),
                home_dir: home_dir.path().to_path_buf(),
                container_home: PathBuf::from("/home/user"),
                extra_mounts: vec!["/host/cmount:/container/cmount:ro,z".to_string()],
                extra_args: vec![],
            },
        )]),
        apps: vec![AppConfigRaw {
            slug: "pfin".to_string(),
            name: None,
            description: None,
            icon: None,
            working_dir: Some(working_dir.clone()),
            model: None,
            single_instance: false,
            singleton: false,
            persistent: false,
            idle_timeout_secs: None,
            compact_reminder_pct: None,
            compact_soft_pct: None,
            compact_red_pct: None,
            compact_hard_pct: None,
            compact_reminder_tokens: None,
            compact_soft_tokens: None,
            compact_red_tokens: None,
            compact_hard_tokens: None,
            compact_idle_secs: None,
            idle_hook_secs: None,
            allowed_users: vec![],
            disabled_tools: vec![],
            mcp_servers: HashMap::new(),
            multiuser: false,
            prefix_username: None,
            prefix_timestamp: None,
            prefix_device: None,
            container: Some("sandbox".to_string()),
            container_working_dir: Some(PathBuf::from("/home/user/work")),
            start_hooks: None,
            post_pull_hooks: None,
            startup_hooks: None,
            cc_extra_args: vec![],
            approval_rules: vec![],
            attachment_targets: vec![],
            integrations: vec![],
            integration_config: HashMap::new(),
            mounts: vec![],
            extra_mounts: vec!["a:b:ro,z".to_string()],
            history_replay_limit: None,
            frontmatter: FrontmatterRenderConfig::default(),
            messaging: None,
            pwa_push: None,
            webhook_subscriptions: vec![],
            mqtt_subscriptions: vec![],
            grants: vec![],
            acl: crate::access::raw::AppAclRaw::default(),
            tool_grants: vec![],
        }],
        ..Default::default()
    };
    let ResolvedConfig { apps, .. } =
        validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
    let app = apps.get("pfin").unwrap();
    let spawn = app.container_spawn.as_ref().unwrap();

    // Container-level entries come first, then app-level entries.
    assert_eq!(
        spawn.extra_mounts,
        vec![
            "/host/cmount:/container/cmount:ro,z".to_string(),
            "a:b:ro,z".to_string(),
        ],
    );
}

#[test]
#[should_panic(expected = "extra_mounts")]
fn validate_bare_app_with_extra_mounts_panics() {
    let working_dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "bare-with-mounts".to_string(),
            name: None,
            description: None,
            icon: None,
            working_dir: Some(working_dir.path().to_path_buf()),
            model: None,
            single_instance: false,
            singleton: false,
            persistent: false,
            idle_timeout_secs: None,
            compact_reminder_pct: None,
            compact_soft_pct: None,
            compact_red_pct: None,
            compact_hard_pct: None,
            compact_reminder_tokens: None,
            compact_soft_tokens: None,
            compact_red_tokens: None,
            compact_hard_tokens: None,
            compact_idle_secs: None,
            idle_hook_secs: None,
            allowed_users: vec![],
            disabled_tools: vec![],
            mcp_servers: HashMap::new(),
            multiuser: false,
            prefix_username: None,
            prefix_timestamp: None,
            prefix_device: None,
            container: None,
            container_working_dir: None,
            start_hooks: None,
            post_pull_hooks: None,
            startup_hooks: None,
            cc_extra_args: vec![],
            approval_rules: vec![],
            attachment_targets: vec![],
            integrations: vec![],
            integration_config: HashMap::new(),
            mounts: vec![],
            extra_mounts: vec!["host:cont:ro".to_string()],
            history_replay_limit: None,
            frontmatter: FrontmatterRenderConfig::default(),
            messaging: None,
            pwa_push: None,
            webhook_subscriptions: vec![],
            mqtt_subscriptions: vec![],
            grants: vec![],
            acl: crate::access::raw::AppAclRaw::default(),
            tool_grants: vec![],
        }],
        ..Default::default()
    };
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
fn validate_container_home_defaults_to_home_user() {
    // Parse a ContainerConfig without container_home — should default.
    let toml_str = r#"
image = "brenn-cc:latest"
home_dir = "/tmp"
"#;
    let parsed: ContainerConfig = toml::from_str(toml_str).unwrap();
    assert_eq!(parsed.container_home, PathBuf::from("/home/user"));
}

#[test]
#[should_panic(expected = "references container")]
fn validate_nonexistent_container_panics() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "pfin".to_string(),
            name: None,
            description: None,
            icon: None,
            working_dir: Some(dir.path().to_path_buf()),
            model: None,
            single_instance: false,
            singleton: false,
            persistent: false,
            idle_timeout_secs: None,
            compact_reminder_pct: None,
            compact_soft_pct: None,
            compact_red_pct: None,
            compact_hard_pct: None,
            compact_reminder_tokens: None,
            compact_soft_tokens: None,
            compact_red_tokens: None,
            compact_hard_tokens: None,
            compact_idle_secs: None,
            idle_hook_secs: None,
            allowed_users: vec![],
            disabled_tools: vec![],
            mcp_servers: HashMap::new(),
            multiuser: false,
            prefix_username: None,
            prefix_timestamp: None,
            prefix_device: None,
            container: Some("nonexistent".to_string()),
            container_working_dir: Some(PathBuf::from("/workspace")),
            start_hooks: None,
            post_pull_hooks: None,
            startup_hooks: None,
            cc_extra_args: vec![],
            approval_rules: vec![],
            attachment_targets: vec![],
            integrations: vec![],
            integration_config: HashMap::new(),
            mounts: vec![],
            extra_mounts: vec![],
            history_replay_limit: None,
            frontmatter: FrontmatterRenderConfig::default(),
            messaging: None,
            pwa_push: None,
            webhook_subscriptions: vec![],
            mqtt_subscriptions: vec![],
            grants: vec![],
            acl: crate::access::raw::AppAclRaw::default(),
            tool_grants: vec![],
        }],
        ..Default::default()
    };
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
#[should_panic(expected = "requires `container_working_dir`")]
fn validate_container_without_working_dir_panics() {
    let dir = tempfile::tempdir().unwrap();
    let home_dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        container: HashMap::from([(
            "sandbox".to_string(),
            ContainerConfig {
                image: "brenn-cc:latest".to_string(),
                home_dir: home_dir.path().to_path_buf(),
                container_home: PathBuf::from("/home/user"),
                extra_mounts: vec![],
                extra_args: vec![],
            },
        )]),
        apps: vec![AppConfigRaw {
            slug: "pfin".to_string(),
            name: None,
            description: None,
            icon: None,
            working_dir: Some(dir.path().to_path_buf()),
            model: None,
            single_instance: false,
            singleton: false,
            persistent: false,
            idle_timeout_secs: None,
            compact_reminder_pct: None,
            compact_soft_pct: None,
            compact_red_pct: None,
            compact_hard_pct: None,
            compact_reminder_tokens: None,
            compact_soft_tokens: None,
            compact_red_tokens: None,
            compact_hard_tokens: None,
            compact_idle_secs: None,
            idle_hook_secs: None,
            allowed_users: vec![],
            disabled_tools: vec![],
            mcp_servers: HashMap::new(),
            multiuser: false,
            prefix_username: None,
            prefix_timestamp: None,
            prefix_device: None,
            container: Some("sandbox".to_string()),
            container_working_dir: None, // Missing!
            start_hooks: None,
            post_pull_hooks: None,
            startup_hooks: None,
            cc_extra_args: vec![],
            approval_rules: vec![],
            attachment_targets: vec![],
            integrations: vec![],
            integration_config: HashMap::new(),
            mounts: vec![],
            extra_mounts: vec![],
            history_replay_limit: None,
            frontmatter: FrontmatterRenderConfig::default(),
            messaging: None,
            pwa_push: None,
            webhook_subscriptions: vec![],
            mqtt_subscriptions: vec![],
            grants: vec![],
            acl: crate::access::raw::AppAclRaw::default(),
            tool_grants: vec![],
        }],
        ..Default::default()
    };
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
#[should_panic(expected = "must be an absolute path")]
fn validate_relative_container_working_dir_panics() {
    let dir = tempfile::tempdir().unwrap();
    let home_dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        container: HashMap::from([(
            "sandbox".to_string(),
            ContainerConfig {
                image: "brenn-cc:latest".to_string(),
                home_dir: home_dir.path().to_path_buf(),
                container_home: PathBuf::from("/home/user"),
                extra_mounts: vec![],
                extra_args: vec![],
            },
        )]),
        apps: vec![AppConfigRaw {
            slug: "pfin".to_string(),
            name: None,
            description: None,
            icon: None,
            working_dir: Some(dir.path().to_path_buf()),
            model: None,
            single_instance: false,
            singleton: false,
            persistent: false,
            idle_timeout_secs: None,
            compact_reminder_pct: None,
            compact_soft_pct: None,
            compact_red_pct: None,
            compact_hard_pct: None,
            compact_reminder_tokens: None,
            compact_soft_tokens: None,
            compact_red_tokens: None,
            compact_hard_tokens: None,
            compact_idle_secs: None,
            idle_hook_secs: None,
            allowed_users: vec![],
            disabled_tools: vec![],
            mcp_servers: HashMap::new(),
            multiuser: false,
            prefix_username: None,
            prefix_timestamp: None,
            prefix_device: None,
            container: Some("sandbox".to_string()),
            container_working_dir: Some(PathBuf::from("relative/path")), // Not absolute!
            start_hooks: None,
            post_pull_hooks: None,
            startup_hooks: None,
            cc_extra_args: vec![],
            approval_rules: vec![],
            attachment_targets: vec![],
            integrations: vec![],
            integration_config: HashMap::new(),
            mounts: vec![],
            extra_mounts: vec![],
            history_replay_limit: None,
            frontmatter: FrontmatterRenderConfig::default(),
            messaging: None,
            pwa_push: None,
            webhook_subscriptions: vec![],
            mqtt_subscriptions: vec![],
            grants: vec![],
            acl: crate::access::raw::AppAclRaw::default(),
            tool_grants: vec![],
        }],
        ..Default::default()
    };
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
#[should_panic(expected = "home_dir")]
fn validate_nonexistent_container_home_dir_panics() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        container: HashMap::from([(
            "sandbox".to_string(),
            ContainerConfig {
                image: "brenn-cc:latest".to_string(),
                home_dir: PathBuf::from("/nonexistent/home"),
                container_home: PathBuf::from("/home/user"),
                extra_mounts: vec![],
                extra_args: vec![],
            },
        )]),
        apps: vec![AppConfigRaw {
            slug: "pfin".to_string(),
            name: None,
            description: None,
            icon: None,
            working_dir: Some(dir.path().to_path_buf()),
            model: None,
            single_instance: false,
            singleton: false,
            persistent: false,
            idle_timeout_secs: None,
            compact_reminder_pct: None,
            compact_soft_pct: None,
            compact_red_pct: None,
            compact_hard_pct: None,
            compact_reminder_tokens: None,
            compact_soft_tokens: None,
            compact_red_tokens: None,
            compact_hard_tokens: None,
            compact_idle_secs: None,
            idle_hook_secs: None,
            allowed_users: vec![],
            disabled_tools: vec![],
            mcp_servers: HashMap::new(),
            multiuser: false,
            prefix_username: None,
            prefix_timestamp: None,
            prefix_device: None,
            container: Some("sandbox".to_string()),
            container_working_dir: Some(PathBuf::from("/workspace")),
            start_hooks: None,
            post_pull_hooks: None,
            startup_hooks: None,
            cc_extra_args: vec![],
            approval_rules: vec![],
            attachment_targets: vec![],
            integrations: vec![],
            integration_config: HashMap::new(),
            mounts: vec![],
            extra_mounts: vec![],
            history_replay_limit: None,
            frontmatter: FrontmatterRenderConfig::default(),
            messaging: None,
            pwa_push: None,
            webhook_subscriptions: vec![],
            mqtt_subscriptions: vec![],
            grants: vec![],
            acl: crate::access::raw::AppAclRaw::default(),
            tool_grants: vec![],
        }],
        ..Default::default()
    };
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
#[should_panic(expected = "start_hooks.container")]
fn validate_container_hooks_on_bare_app_panics() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "pfin".to_string(),
            name: None,
            description: None,
            icon: None,
            working_dir: Some(dir.path().to_path_buf()),
            model: None,
            single_instance: false,
            singleton: false,
            persistent: false,
            idle_timeout_secs: None,
            compact_reminder_pct: None,
            compact_soft_pct: None,
            compact_red_pct: None,
            compact_hard_pct: None,
            compact_reminder_tokens: None,
            compact_soft_tokens: None,
            compact_red_tokens: None,
            compact_hard_tokens: None,
            compact_idle_secs: None,
            idle_hook_secs: None,
            allowed_users: vec![],
            disabled_tools: vec![],
            mcp_servers: HashMap::new(),
            multiuser: false,
            prefix_username: None,
            prefix_timestamp: None,
            prefix_device: None,
            container: None,
            container_working_dir: None,
            start_hooks: Some(StartHooksConfig {
                host: vec![],
                container: vec!["./setup.sh".to_string()],
            }),
            post_pull_hooks: None,
            startup_hooks: None,
            cc_extra_args: vec![],
            approval_rules: vec![],
            attachment_targets: vec![],
            integrations: vec![],
            integration_config: HashMap::new(),
            mounts: vec![],
            extra_mounts: vec![],
            history_replay_limit: None,
            frontmatter: FrontmatterRenderConfig::default(),
            messaging: None,
            pwa_push: None,
            webhook_subscriptions: vec![],
            mqtt_subscriptions: vec![],
            grants: vec![],
            acl: crate::access::raw::AppAclRaw::default(),
            tool_grants: vec![],
        }],
        ..Default::default()
    };
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
#[should_panic(expected = "post_pull_hooks.container")]
fn validate_post_pull_container_hooks_on_bare_app_panics() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "pfin".to_string(),
            name: None,
            description: None,
            icon: None,
            working_dir: Some(dir.path().to_path_buf()),
            model: None,
            single_instance: false,
            singleton: false,
            persistent: false,
            idle_timeout_secs: None,
            compact_reminder_pct: None,
            compact_soft_pct: None,
            compact_red_pct: None,
            compact_hard_pct: None,
            compact_reminder_tokens: None,
            compact_soft_tokens: None,
            compact_red_tokens: None,
            compact_hard_tokens: None,
            compact_idle_secs: None,
            idle_hook_secs: None,
            allowed_users: vec![],
            disabled_tools: vec![],
            mcp_servers: HashMap::new(),
            multiuser: false,
            prefix_username: None,
            prefix_timestamp: None,
            prefix_device: None,
            container: None,
            container_working_dir: None,
            start_hooks: None,
            post_pull_hooks: Some(PostPullHooksConfig {
                host: vec![],
                container: vec!["pf rebuild".to_string()],
            }),
            startup_hooks: None,
            cc_extra_args: vec![],
            approval_rules: vec![],
            attachment_targets: vec![],
            integrations: vec![],
            integration_config: HashMap::new(),
            mounts: vec![],
            extra_mounts: vec![],
            history_replay_limit: None,
            frontmatter: FrontmatterRenderConfig::default(),
            messaging: None,
            pwa_push: None,
            webhook_subscriptions: vec![],
            mqtt_subscriptions: vec![],
            grants: vec![],
            acl: crate::access::raw::AppAclRaw::default(),
            tool_grants: vec![],
        }],
        ..Default::default()
    };
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
#[should_panic(expected = "startup_hooks.container")]
fn validate_startup_container_hooks_on_bare_app_panics() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "pfin".to_string(),
            name: None,
            description: None,
            icon: None,
            working_dir: Some(dir.path().to_path_buf()),
            model: None,
            single_instance: false,
            singleton: false,
            persistent: false,
            idle_timeout_secs: None,
            compact_reminder_pct: None,
            compact_soft_pct: None,
            compact_red_pct: None,
            compact_hard_pct: None,
            compact_reminder_tokens: None,
            compact_soft_tokens: None,
            compact_red_tokens: None,
            compact_hard_tokens: None,
            compact_idle_secs: None,
            idle_hook_secs: None,
            allowed_users: vec![],
            disabled_tools: vec![],
            mcp_servers: HashMap::new(),
            multiuser: false,
            prefix_username: None,
            prefix_timestamp: None,
            prefix_device: None,
            container: None,
            container_working_dir: None,
            start_hooks: None,
            post_pull_hooks: None,
            startup_hooks: Some(StartupHooksConfig {
                host: vec![],
                container: vec!["pf rebuild".to_string()],
            }),
            cc_extra_args: vec![],
            approval_rules: vec![],
            attachment_targets: vec![],
            integrations: vec![],
            integration_config: HashMap::new(),
            mounts: vec![],
            extra_mounts: vec![],
            history_replay_limit: None,
            frontmatter: FrontmatterRenderConfig::default(),
            messaging: None,
            pwa_push: None,
            webhook_subscriptions: vec![],
            mqtt_subscriptions: vec![],
            grants: vec![],
            acl: crate::access::raw::AppAclRaw::default(),
            tool_grants: vec![],
        }],
        ..Default::default()
    };
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
fn parse_start_hooks_from_toml() {
    let dir = tempfile::tempdir().unwrap();
    let toml = format!(
        r#"
[[app]]
slug = "myapp"
working_dir = "{}"

[app.start_hooks]
host = ["./setup.sh", "echo hello"]
"#,
        dir.path().display()
    );
    let config: BrennConfig = toml::from_str(&toml).unwrap();
    let hooks = config.apps[0].start_hooks.as_ref().unwrap();
    assert_eq!(hooks.host.len(), 2);
    assert!(hooks.container.is_empty());
}

#[test]
fn parse_cc_extra_args_from_toml() {
    let dir = tempfile::tempdir().unwrap();
    let toml = format!(
        r#"
[[app]]
slug = "myapp"
working_dir = "{}"
cc_extra_args = ["--max-turns", "50"]
"#,
        dir.path().display()
    );
    let config: BrennConfig = toml::from_str(&toml).unwrap();
    assert_eq!(config.apps[0].cc_extra_args, vec!["--max-turns", "50"]);
}

#[test]
fn cc_extra_args_defaults_empty() {
    let dir = tempfile::tempdir().unwrap();
    let toml = format!(
        r#"
[[app]]
slug = "myapp"
working_dir = "{}"
"#,
        dir.path().display()
    );
    let config: BrennConfig = toml::from_str(&toml).unwrap();
    assert!(config.apps[0].cc_extra_args.is_empty());
}
