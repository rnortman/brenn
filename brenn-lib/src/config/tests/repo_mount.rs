use super::*;
use crate::config::ResolvedConfig;
use crate::integration::IntegrationRegistry;

// -----------------------------------------------------------------------
// Repo / mount config validation
// -----------------------------------------------------------------------

#[test]
fn mount_config_parses_from_toml() {
    let dir = tempfile::tempdir().unwrap();
    let app_dir = dir.path().join("myapp");
    std::fs::create_dir(&app_dir).unwrap();

    let toml = format!(
        r#"
repo_dir = "{}"

[[repo]]
slug = "life"
remote = "https://example.com/life.git"

[[repo]]
slug = "tech"
remote = "https://example.com/tech.git"
auto_pull = false

[[app]]
slug = "myapp"
working_dir = "{}"

[[app.mount]]
repo = "life"

[[app.mount]]
repo = "tech"
auto_pull = true
"#,
        dir.path().display(),
        app_dir.display()
    );
    let config: BrennConfig = toml::from_str(&toml).unwrap();
    assert_eq!(config.apps[0].mounts.len(), 2);
    assert_eq!(config.apps[0].mounts[0].repo, "life");
    assert_eq!(config.apps[0].mounts[1].repo, "tech");
    assert_eq!(config.repos.len(), 2);
    assert_eq!(config.repos[0].slug, "life");
    assert!(config.repos[0].auto_pull); // default true
    assert_eq!(config.repos[1].slug, "tech");
    assert!(!config.repos[1].auto_pull);
}

#[test]
#[should_panic(expected = "duplicate repo slug")]
fn repo_duplicate_slug_panics() {
    let dir = tempfile::tempdir().unwrap();
    let app_dir = dir.path().join("myapp");
    std::fs::create_dir(&app_dir).unwrap();

    let config = BrennConfig {
        repo_dir: Some(dir.path().to_path_buf()),
        repos: vec![
            RepoDeclRaw {
                slug: "life".to_string(),
                remote: "https://example.com/life.git".to_string(),
                auto_pull: true,
            },
            RepoDeclRaw {
                slug: "life".to_string(),
                remote: "https://example.com/life2.git".to_string(),
                auto_pull: true,
            },
        ],
        apps: vec![AppConfigRaw {
            slug: "myapp".to_string(),
            name: None,
            description: None,
            icon: None,
            working_dir: Some(app_dir),
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
#[should_panic(expected = "repo slug \"all\" is reserved")]
fn repo_reserved_slug_panics() {
    let dir = tempfile::tempdir().unwrap();
    let app_dir = dir.path().join("myapp");
    std::fs::create_dir(&app_dir).unwrap();

    let config = BrennConfig {
        repo_dir: Some(dir.path().to_path_buf()),
        repos: vec![RepoDeclRaw {
            slug: "all".to_string(),
            remote: "https://example.com/all.git".to_string(),
            auto_pull: true,
        }],
        apps: vec![AppConfigRaw {
            slug: "myapp".to_string(),
            name: None,
            description: None,
            icon: None,
            working_dir: Some(app_dir),
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
fn mount_visible_path_bare() {
    let mount = ResolvedMount {
        slug: "life".to_string(),
        host_path: PathBuf::from("/host/data/life"),
        container_path: None,
        access: AccessLevel::ReadWrite,
        auto_pull: false,
        is_working_dir: false,
        primary: false,
    };
    assert_eq!(mount.visible_path(false), Path::new("/host/data/life"));
}

#[test]
fn mount_visible_path_containerized() {
    let mount = ResolvedMount {
        slug: "life".to_string(),
        host_path: PathBuf::from("/host/data/life"),
        container_path: Some(PathBuf::from("/home/user/data/life")),
        access: AccessLevel::ReadWrite,
        auto_pull: false,
        is_working_dir: false,
        primary: false,
    };
    assert_eq!(mount.visible_path(true), Path::new("/home/user/data/life"));
}

#[test]
#[should_panic(expected = "container_path required")]
fn mount_visible_path_containerized_without_container_path_panics() {
    let mount = ResolvedMount {
        slug: "life".to_string(),
        host_path: PathBuf::from("/host/data/life"),
        container_path: None,
        access: AccessLevel::ReadWrite,
        auto_pull: false,
        is_working_dir: false,
        primary: false,
    };
    let _ = mount.visible_path(true);
}

#[test]
#[should_panic(expected = "repo slug")]
fn repo_invalid_slug_format_panics() {
    let dir = tempfile::tempdir().unwrap();
    let app_dir = dir.path().join("myapp");
    std::fs::create_dir(&app_dir).unwrap();

    let config = BrennConfig {
        repo_dir: Some(dir.path().to_path_buf()),
        repos: vec![RepoDeclRaw {
            slug: "BAD SLUG".to_string(),
            remote: "https://example.com/bad.git".to_string(),
            auto_pull: true,
        }],
        apps: vec![AppConfigRaw {
            slug: "myapp".to_string(),
            name: None,
            description: None,
            icon: None,
            working_dir: Some(app_dir),
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

// -----------------------------------------------------------------------
// Mount validation: new config paths
// -----------------------------------------------------------------------

/// Helper to build a minimal BrennConfig for mount validation tests.
fn mount_test_config(
    dir: &std::path::Path,
    repos: Vec<RepoDeclRaw>,
    mounts: Vec<MountConfigRaw>,
    working_dir: Option<PathBuf>,
    container_working_dir: Option<PathBuf>,
) -> BrennConfig {
    BrennConfig {
        repo_dir: Some(dir.to_path_buf()),
        repos,
        apps: vec![AppConfigRaw {
            slug: "test".to_string(),
            name: None,
            description: None,
            icon: None,
            working_dir,
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
            container_working_dir,
            start_hooks: None,
            post_pull_hooks: None,
            startup_hooks: None,
            cc_extra_args: vec![],
            approval_rules: vec![],
            attachment_targets: vec![],
            integrations: vec![],
            integration_config: HashMap::new(),
            mounts,
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
    }
}

#[test]
#[should_panic(expected = "not defined in [[repo]]")]
fn mount_references_nonexistent_repo_panics() {
    let dir = tempfile::tempdir().unwrap();
    let app_dir = dir.path().join("app");
    std::fs::create_dir(&app_dir).unwrap();

    let config = mount_test_config(
        dir.path(),
        vec![], // no repos
        vec![MountConfigRaw {
            repo: "nonexistent".to_string(),
            access: AccessLevel::ReadWrite,
            working_dir: false,
            auto_pull: None,
            primary: false,
        }],
        Some(app_dir),
        None,
    );
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
#[should_panic(expected = "duplicate mount")]
fn mount_duplicate_repo_panics() {
    let dir = tempfile::tempdir().unwrap();
    let app_dir = dir.path().join("app");
    let repo_dir = dir.path().join("myrepo");
    std::fs::create_dir(&app_dir).unwrap();
    std::fs::create_dir(&repo_dir).unwrap();

    let config = mount_test_config(
        dir.path(),
        vec![RepoDeclRaw {
            slug: "myrepo".to_string(),
            remote: "https://example.com/r.git".to_string(),
            auto_pull: true,
        }],
        vec![
            MountConfigRaw {
                repo: "myrepo".to_string(),
                access: AccessLevel::ReadWrite,
                working_dir: false,
                auto_pull: None,
                primary: false,
            },
            MountConfigRaw {
                repo: "myrepo".to_string(),
                access: AccessLevel::ReadOnly,
                working_dir: false,
                auto_pull: None,
                primary: false,
            },
        ],
        Some(app_dir),
        None,
    );
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
#[should_panic(expected = "cannot have both")]
fn mount_working_dir_and_explicit_working_dir_panics() {
    let dir = tempfile::tempdir().unwrap();
    let app_dir = dir.path().join("app");
    let repo_dir = dir.path().join("myrepo");
    std::fs::create_dir(&app_dir).unwrap();
    std::fs::create_dir(&repo_dir).unwrap();

    let config = mount_test_config(
        dir.path(),
        vec![RepoDeclRaw {
            slug: "myrepo".to_string(),
            remote: "https://example.com/r.git".to_string(),
            auto_pull: true,
        }],
        vec![MountConfigRaw {
            repo: "myrepo".to_string(),
            access: AccessLevel::ReadWrite,
            working_dir: true,
            auto_pull: None,
            primary: false,
        }],
        Some(app_dir), // explicit working_dir conflicts with mount
        None,
    );
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
#[should_panic(expected = "must have either")]
fn mount_no_working_dir_source_panics() {
    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("myrepo");
    std::fs::create_dir(&repo_dir).unwrap();

    let config = mount_test_config(
        dir.path(),
        vec![RepoDeclRaw {
            slug: "myrepo".to_string(),
            remote: "https://example.com/r.git".to_string(),
            auto_pull: true,
        }],
        vec![MountConfigRaw {
            repo: "myrepo".to_string(),
            access: AccessLevel::ReadWrite,
            working_dir: false, // no working_dir on mount
            auto_pull: None,
            primary: false,
        }],
        None, // no explicit working_dir either
        None,
    );
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
#[should_panic(expected = "multiple mounts")]
fn mount_multiple_working_dir_panics() {
    let dir = tempfile::tempdir().unwrap();
    let repo_a = dir.path().join("repo-a");
    let repo_b = dir.path().join("repo-b");
    std::fs::create_dir(&repo_a).unwrap();
    std::fs::create_dir(&repo_b).unwrap();

    let config = mount_test_config(
        dir.path(),
        vec![
            RepoDeclRaw {
                slug: "repo-a".to_string(),
                remote: "https://example.com/a.git".to_string(),
                auto_pull: true,
            },
            RepoDeclRaw {
                slug: "repo-b".to_string(),
                remote: "https://example.com/b.git".to_string(),
                auto_pull: true,
            },
        ],
        vec![
            MountConfigRaw {
                repo: "repo-a".to_string(),
                access: AccessLevel::ReadWrite,
                working_dir: true,
                auto_pull: None,
                primary: false,
            },
            MountConfigRaw {
                repo: "repo-b".to_string(),
                access: AccessLevel::ReadWrite,
                working_dir: true,
                auto_pull: None,
                primary: false,
            },
        ],
        None,
        None,
    );
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
#[should_panic(expected = "cannot have")]
fn mount_working_dir_with_container_working_dir_panics() {
    let dir = tempfile::tempdir().unwrap();
    let home_dir = dir.path().join("home");
    let repo_dir = dir.path().join("myrepo");
    std::fs::create_dir(&home_dir).unwrap();
    std::fs::create_dir(&repo_dir).unwrap();

    let mut config = mount_test_config(
        dir.path(),
        vec![RepoDeclRaw {
            slug: "myrepo".to_string(),
            remote: "https://example.com/r.git".to_string(),
            auto_pull: true,
        }],
        vec![MountConfigRaw {
            repo: "myrepo".to_string(),
            access: AccessLevel::ReadWrite,
            working_dir: true,
            auto_pull: None,
            primary: false,
        }],
        None,
        Some(PathBuf::from("/container/work")), // conflicts with mount
    );
    config.container.insert(
        "sandbox".to_string(),
        ContainerConfig {
            image: "test:latest".to_string(),
            home_dir,
            container_home: PathBuf::from("/home/user"),
            extra_mounts: vec![],
            extra_args: vec![],
        },
    );
    config.apps[0].container = Some("sandbox".to_string());
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
fn mount_working_dir_from_mount_resolves_correctly() {
    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("myrepo");
    std::fs::create_dir(&repo_dir).unwrap();

    let config = mount_test_config(
        dir.path(),
        vec![RepoDeclRaw {
            slug: "myrepo".to_string(),
            remote: "https://example.com/r.git".to_string(),
            auto_pull: true,
        }],
        vec![MountConfigRaw {
            repo: "myrepo".to_string(),
            access: AccessLevel::ReadWrite,
            working_dir: true,
            auto_pull: None,
            primary: false,
        }],
        None, // working_dir comes from mount
        None,
    );
    let ResolvedConfig { apps, .. } = validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
    let app = apps.get("test").unwrap();
    assert_eq!(app.working_dir, repo_dir);
    assert_eq!(app.mounts.len(), 1);
    assert!(app.mounts[0].is_working_dir);
    assert_eq!(app.mounts[0].slug, "myrepo");
}

#[test]
fn containerized_mount_resolves_path_mapper_and_repo_mounts() {
    let dir = tempfile::tempdir().unwrap();
    let home_dir = dir.path().join("home");
    let repo_a = dir.path().join("repo-a");
    let repo_b = dir.path().join("repo-b");
    std::fs::create_dir(&home_dir).unwrap();
    std::fs::create_dir(&repo_a).unwrap();
    std::fs::create_dir(&repo_b).unwrap();

    let mut config = mount_test_config(
        dir.path(),
        vec![
            RepoDeclRaw {
                slug: "repo-a".to_string(),
                remote: "https://example.com/a.git".to_string(),
                auto_pull: true,
            },
            RepoDeclRaw {
                slug: "repo-b".to_string(),
                remote: "https://example.com/b.git".to_string(),
                auto_pull: true,
            },
        ],
        vec![
            MountConfigRaw {
                repo: "repo-a".to_string(),
                access: AccessLevel::ReadWrite,
                working_dir: true,
                auto_pull: None,
                primary: false,
            },
            MountConfigRaw {
                repo: "repo-b".to_string(),
                access: AccessLevel::ReadOnly,
                working_dir: false,
                auto_pull: Some(false),
                primary: false,
            },
        ],
        None,
        None,
    );
    config.container.insert(
        "sandbox".to_string(),
        ContainerConfig {
            image: "test:latest".to_string(),
            home_dir: home_dir.clone(),
            container_home: PathBuf::from("/home/user"),
            extra_mounts: vec![],
            extra_args: vec![],
        },
    );
    config.apps[0].container = Some("sandbox".to_string());

    let ResolvedConfig { apps, .. } =
        validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
    let app = apps.get("test").unwrap();

    // Working dir resolved from mount.
    assert_eq!(app.working_dir, repo_a);

    // Container spawn config.
    let spawn = app.container_spawn.as_ref().unwrap();
    assert!(spawn.working_dir_is_repo);
    assert_eq!(
        spawn.container_working_dir,
        PathBuf::from("/home/user/repos/repo-a")
    );
    assert_eq!(spawn.repo_mounts.len(), 2);
    assert!(!spawn.repo_mounts[0].read_only); // repo-a is read-write
    assert!(spawn.repo_mounts[1].read_only); // repo-b is read-only

    // PathMapper: repo-specific mappings before home mapping.
    let container_path = PathBuf::from("/home/user/repos/repo-b/some/file.txt");
    let host_path = app.path_mapper.to_host(&container_path).unwrap();
    assert_eq!(host_path, repo_b.join("some/file.txt"));

    // Home mapping still works for non-repo paths.
    let home_container = PathBuf::from("/home/user/.config/something");
    let home_host = app.path_mapper.to_host(&home_container).unwrap();
    assert_eq!(home_host, home_dir.join(".config/something"));

    // Mounts carry correct access and auto_pull.
    assert_eq!(app.mounts[0].access, AccessLevel::ReadWrite);
    assert!(app.mounts[0].auto_pull); // from repo default
    assert_eq!(app.mounts[1].access, AccessLevel::ReadOnly);
    assert!(!app.mounts[1].auto_pull); // overridden to false
}

#[test]
fn repo_sync_config_defaults_match_design() {
    // The documented defaults are 300s poll interval and 7d staleness
    // cap. Regression-lock them so a config refactor can't silently
    // change the production posture; the poller is the freshness
    // backstop for the git-webhook pipeline.
    let cfg = RepoSyncConfig::default();
    assert_eq!(cfg.poll_interval_secs, 300);
    assert_eq!(cfg.stale_conversation_days, 7);
}
