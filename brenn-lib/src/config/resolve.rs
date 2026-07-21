use std::collections::HashMap;
use std::sync::Arc;

use indexmap::IndexMap;

use crate::integration::IntegrationRegistry;

use super::*;

/// Default limit for full-fidelity history replay on WebSocket connect.
/// History beyond this is available via simplified backward pagination.
const DEFAULT_HISTORY_REPLAY_LIMIT: usize = 2000;

/// Shallow-merge two TOML values for integration config.
///
/// Both values must be tables. Per-app keys override global keys at the
/// top level — no deep merge.
pub(crate) fn shallow_merge_toml(
    global: &toml::Value,
    per_app: &toml::Value,
    app_slug: &str,
    int_name: &str,
) -> toml::Value {
    let global_table = global.as_table().unwrap_or_else(|| {
        panic!(
            "global integration config for {} must be a table, got {:?}",
            int_name, global,
        )
    });
    let per_app_table = per_app.as_table().unwrap_or_else(|| {
        panic!(
            "app {}: integration_config.{} must be a table, got {:?}",
            app_slug, int_name, per_app,
        )
    });

    let mut merged = global_table.clone();
    for (key, value) in per_app_table {
        merged.insert(key.clone(), value.clone());
    }
    toml::Value::Table(merged)
}

/// Output of [`validate_and_resolve`]: the resolved app registry plus resolved
/// subsystem configs that are threaded directly to the bootstrap layer so each
/// secret file is read exactly once during startup.
pub struct ResolvedConfig {
    /// Final app registry, keyed by slug. `Arc`-wrapped so callers that need
    /// a shared handle (e.g. `AppState`, background tasks) can clone cheaply.
    pub apps: Arc<IndexMap<String, AppConfig>>,
    /// Resolved webhook transport endpoints, keyed by slug. Empty when no
    /// `[[webhook_endpoint]]` blocks are declared.
    pub webhook_endpoints: IndexMap<String, Arc<crate::webhook::ResolvedWebhookEndpoint>>,
    /// Distinct MQTT ingress channels (the client-in-address model), deduplicated
    /// by `channel_uuid` across all apps' `[[app.mqtt_subscription]]` blocks.
    /// Empty when no app declares an mqtt subscription. Each channel is one
    /// `mqtt:<client>:<topic>` channel; the channel-entry derivation, ingress
    /// union-set, and router routing table read this in the bootstrap layer.
    pub mqtt_ingress_channels: Vec<crate::mqtt::config::ResolvedMqttIngressChannel>,
    /// Resolved `[[mqtt_client]]` map, keyed by slug, with secret files
    /// (`password_file`/`ca_file`) read exactly once during startup. Threaded to
    /// the bootstrap layer so the ingress-supervisor wiring (`start_mqtt`) looks
    /// up each client's config here instead of re-resolving (which would
    /// re-read every secret from disk a second time). Empty when no
    /// `[[mqtt_client]]` is declared.
    pub mqtt_clients: IndexMap<String, crate::mqtt::config::MqttClientConfig>,
    /// Resolved PWA push config. `None` when no app has `pwa_push.enabled = true`.
    pub pwa_push: Option<crate::pwa_push::config::ResolvedPwaPushConfig>,
}

/// Validate raw config and resolve defaults, producing the final app registry
/// plus pre-resolved subsystem configs.
///
/// `integration_registry` provides the compiled-in integration factories.
/// Each app's enabled integrations are resolved against this registry:
/// global config is merged with per-app overrides, and the factory creates
/// a configured instance.
///
/// `runtime_dir` must be `Some(&validated_path)` when the config contains at
/// least one bare (non-containerized) app, and `None` when all apps are
/// containerized. The caller is responsible for resolving and validating
/// `XDG_RUNTIME_DIR` exactly once at startup (see `crate::runtime_dir`).
///
/// # Panics
///
/// Panics if:
/// - No apps are defined
/// - Duplicate app or repo slugs
/// - Invalid slug format
/// - `repo_dir` not set when `[[repo]]` entries exist
/// - Mount references a nonexistent `[[repo]]` slug
/// - Working directory rules violated (must have exactly one source)
/// - `working_dir` does not exist or is not a directory
/// - App references a nonexistent container
/// - Container `home_dir` does not exist or is not a directory
/// - App enables an integration not in the registry
/// - Integration-contributed MCP server name collides with explicit `mcp_servers`
/// - A bare app is present but `runtime_dir` is `None` (bootstrap bug: the
///   caller failed to gate on bare-app presence before resolving XDG)
/// - `watchdog.sweep_interval_secs` is zero
pub fn validate_and_resolve(
    config: &BrennConfig,
    integration_registry: &IntegrationRegistry,
    runtime_dir: Option<&std::path::Path>,
) -> ResolvedConfig {
    assert!(
        !config.apps.is_empty(),
        "at least one [[app]] must be defined in config"
    );

    // Validate container home_dir paths upfront.
    for (name, container) in &config.container {
        assert!(
            container.home_dir.is_dir(),
            "container {:?} home_dir {:?} does not exist or is not a directory",
            name,
            container.home_dir,
        );
    }

    // Validate server.public_url format before any downstream consumer reads it.
    validate_public_url(&config.server);

    // The watchdog sweep interval is a divisor in `grace_sweeps()` and the tick
    // period; a zero is a config error, not something to silently clamp.
    assert!(
        config.watchdog.sweep_interval_secs >= 1,
        "watchdog.sweep_interval_secs must be >= 1"
    );

    // Validate server.trusted_proxy_hops before the router consumes it. Over-counting
    // hops selects an X-Forwarded-For token left of the outermost trusted proxy — back
    // into attacker-controlled territory — re-enabling the very client-IP spoof this
    // setting exists to prevent. No real self-hosted topology has 8+ trusted proxies,
    // so cap the value and fail fast on absurd configs.
    validate_trusted_proxy_hops(&config.server);

    let slug_re = regex::Regex::new(r"^[a-z0-9][a-z0-9-]*$").unwrap();

    // Validate top-level repo declarations.
    if !config.repos.is_empty() {
        assert!(
            config.repo_dir.is_some(),
            "`repo_dir` must be set when [[repo]] entries are defined",
        );
    }
    {
        let mut repo_slugs = std::collections::HashSet::new();
        for repo in &config.repos {
            assert!(
                slug_re.is_match(&repo.slug),
                "repo slug {:?} must match [a-z0-9][a-z0-9-]*",
                repo.slug,
            );
            assert!(repo.slug != "all", "repo slug \"all\" is reserved",);
            assert!(
                repo_slugs.insert(repo.slug.clone()),
                "duplicate repo slug {:?}",
                repo.slug,
            );
        }
    }

    let mut apps = IndexMap::new();

    for raw in &config.apps {
        assert!(
            slug_re.is_match(&raw.slug),
            "invalid app slug {:?}: must match [a-z0-9][a-z0-9-]*",
            raw.slug,
        );

        if let Some(ref wd) = raw.working_dir {
            assert!(
                wd.is_dir(),
                "app {:?} working_dir {:?} does not exist or is not a directory",
                raw.slug,
                wd,
            );
        }

        // Validate MCP server names don't collide with the built-in Brenn server.
        assert!(
            !raw.mcp_servers.contains_key("brenn"),
            "app {:?} has an MCP server named \"brenn\" — this name is reserved for the built-in DisplayFile server",
            raw.slug,
        );

        // Warn on unrecognized disabled_tools entries (likely typos).
        for tool in &raw.disabled_tools {
            if !CC_KNOWN_TOOLS.contains(&tool.as_str()) {
                tracing::warn!(
                    app_slug = %raw.slug,
                    tool = %tool,
                    "disabled_tools entry not in CC_KNOWN_TOOLS — possible typo or new CC tool",
                );
            }
        }

        // Validate start_hooks: container hooks require a container.
        if let Some(ref hooks) = raw.start_hooks
            && !hooks.container.is_empty()
        {
            assert!(
                raw.container.is_some(),
                "app {:?}: `start_hooks.container` requires a `container` definition",
                raw.slug,
            );
        }

        // Validate post_pull_hooks: container hooks require a container.
        if let Some(ref hooks) = raw.post_pull_hooks
            && !hooks.container.is_empty()
        {
            assert!(
                raw.container.is_some(),
                "app {:?}: `post_pull_hooks.container` requires a `container` definition",
                raw.slug,
            );
        }

        // Validate startup_hooks: container hooks require a container.
        if let Some(ref hooks) = raw.startup_hooks
            && !hooks.container.is_empty()
        {
            assert!(
                raw.container.is_some(),
                "app {:?}: `startup_hooks.container` requires a `container` definition",
                raw.slug,
            );
        }

        // Singleton and multiuser are mutually exclusive.
        assert!(
            !(raw.singleton && raw.multiuser),
            "app {:?}: `singleton` and `multiuser` cannot both be true",
            raw.slug,
        );

        // Multiuser apps must declare allowed_users. On a shared bridge (any
        // participant's conversation toggled to shared), device tools accept an
        // explicit `username` argument. Without an allowed_users list, any participant
        // could provide an arbitrary username and enumerate or mutate another user's
        // device records. Requiring non-empty allowed_users ensures the existing
        // runtime guard (username must be in allowed_users) closes this path.
        assert!(
            !raw.multiuser || !raw.allowed_users.is_empty(),
            "app {:?}: `multiuser = true` requires a non-empty `allowed_users` list \
             (device tools accept an explicit `username` arg on shared bridges; \
             without allowed_users any participant can enumerate other users' devices)",
            raw.slug,
        );

        // idle_timeout_secs without persistent is a config error.
        assert!(
            raw.idle_timeout_secs.is_none() || raw.persistent,
            "app {:?}: `idle_timeout_secs` requires `persistent = true`",
            raw.slug,
        );

        // Compaction settings require singleton (one conversation that never
        // ends = context grows without bound = needs compaction).
        let has_compact_settings = raw.compact_reminder_pct.is_some()
            || raw.compact_soft_pct.is_some()
            || raw.compact_red_pct.is_some()
            || raw.compact_hard_pct.is_some()
            || raw.compact_reminder_tokens.is_some()
            || raw.compact_soft_tokens.is_some()
            || raw.compact_red_tokens.is_some()
            || raw.compact_hard_tokens.is_some()
            || raw.compact_idle_secs.is_some();
        assert!(
            !has_compact_settings || raw.singleton,
            "app {:?}: compaction settings require `singleton = true`",
            raw.slug,
        );

        // Singleton requires compaction config. Without it, the single
        // conversation's context grows without bound and there's no way
        // to reset (no new-conversation button).
        assert!(
            !raw.singleton || has_compact_settings,
            "app {:?}: singleton apps require compaction settings \
             (at minimum, set `compact_soft_pct`)",
            raw.slug,
        );

        // Validate compaction thresholds when compaction is enabled.
        // Ordering: reminder_pct < soft_pct <= red_pct < hard_pct.
        if has_compact_settings {
            let reminder = raw
                .compact_reminder_pct
                .unwrap_or(CompactionConfig::DEFAULT_REMINDER_PCT);
            let soft = raw
                .compact_soft_pct
                .unwrap_or(CompactionConfig::DEFAULT_SOFT_PCT);
            let red = raw
                .compact_red_pct
                .unwrap_or(CompactionConfig::DEFAULT_RED_PCT);
            let hard = raw
                .compact_hard_pct
                .unwrap_or(CompactionConfig::DEFAULT_HARD_PCT);
            for (name, val) in [
                ("compact_reminder_pct", reminder),
                ("compact_soft_pct", soft),
                ("compact_red_pct", red),
                ("compact_hard_pct", hard),
            ] {
                assert!(
                    val > 0 && val <= 100,
                    "app {:?}: `{name}` ({val}) must be in 1..=100",
                    raw.slug,
                );
            }
            assert!(
                reminder < soft,
                "app {:?}: `compact_reminder_pct` ({reminder}) must be less than `compact_soft_pct` ({soft})",
                raw.slug,
            );
            assert!(
                soft <= red,
                "app {:?}: `compact_soft_pct` ({soft}) must be <= `compact_red_pct` ({red})",
                raw.slug,
            );
            assert!(
                red < hard,
                "app {:?}: `compact_red_pct` ({red}) must be less than `compact_hard_pct` ({hard})",
                raw.slug,
            );

            // Absolute token thresholds: each must be >= 1000 (anything below
            // is almost certainly a typo: `200` instead of `200000`).
            for (name, val) in [
                ("compact_reminder_tokens", raw.compact_reminder_tokens),
                ("compact_soft_tokens", raw.compact_soft_tokens),
                ("compact_red_tokens", raw.compact_red_tokens),
                ("compact_hard_tokens", raw.compact_hard_tokens),
            ] {
                if let Some(v) = val {
                    assert!(
                        v >= 1000,
                        "app {:?}: `{name}` ({v}) must be >= 1000",
                        raw.slug,
                    );
                }
            }

            // Absolute token thresholds: ordering invariant when both sides
            // are set. Same shape as the percentage ordering checks above.
            // No cross-validation between % and tokens — runtime semantics
            // ("whichever fires first") handle disagreement gracefully.
            if let (Some(reminder_t), Some(soft_t)) =
                (raw.compact_reminder_tokens, raw.compact_soft_tokens)
            {
                assert!(
                    reminder_t < soft_t,
                    "app {:?}: `compact_reminder_tokens` ({reminder_t}) must be less than `compact_soft_tokens` ({soft_t})",
                    raw.slug,
                );
            }
            if let (Some(soft_t), Some(red_t)) = (raw.compact_soft_tokens, raw.compact_red_tokens) {
                assert!(
                    soft_t <= red_t,
                    "app {:?}: `compact_soft_tokens` ({soft_t}) must be <= `compact_red_tokens` ({red_t})",
                    raw.slug,
                );
            }
            if let (Some(red_t), Some(hard_t)) = (raw.compact_red_tokens, raw.compact_hard_tokens) {
                assert!(
                    red_t < hard_t,
                    "app {:?}: `compact_red_tokens` ({red_t}) must be less than `compact_hard_tokens` ({hard_t})",
                    raw.slug,
                );
            }
        }

        // --- Resolve mounts from [[repo]] + [[app.mount]] ---
        let repo_dir = config.repo_dir.as_deref();
        let is_containerized = raw.container.is_some();

        // Look up container config early — needed for mount resolution.
        let container_cfg = raw.container.as_ref().map(|container_name| {
            config.container.get(container_name).unwrap_or_else(|| {
                panic!(
                    "app {:?} references container {:?} which is not defined in [container]",
                    raw.slug, container_name,
                )
            })
        });

        // Validate mount references and build resolved mounts.
        let mut mount_slugs = std::collections::HashSet::new();
        let mut working_dir_from_mount: Option<(std::path::PathBuf, Option<std::path::PathBuf>)> =
            None;
        let resolved_mounts: Vec<ResolvedMount> = raw
            .mounts
            .iter()
            .map(|m| {
                let repo_dir = repo_dir.unwrap_or_else(|| {
                    panic!(
                        "app {:?}: mount {:?} requires `repo_dir` to be set",
                        raw.slug, m.repo,
                    )
                });

                // Look up the top-level [[repo]] declaration.
                let repo_decl = config
                    .repos
                    .iter()
                    .find(|r| r.slug == m.repo)
                    .unwrap_or_else(|| {
                        panic!(
                            "app {:?}: mount references repo {:?} which is not defined in [[repo]]",
                            raw.slug, m.repo,
                        )
                    });

                assert!(
                    mount_slugs.insert(m.repo.clone()),
                    "app {:?}: duplicate mount for repo {:?}",
                    raw.slug,
                    m.repo,
                );

                let host_path = repo_dir.join(&m.repo);
                let container_path =
                    container_cfg.map(|c| c.container_home.join("repos").join(&m.repo));

                // Effective auto_pull: mount override → repo default.
                let auto_pull = m.auto_pull.unwrap_or(repo_decl.auto_pull);

                if m.working_dir {
                    assert!(
                        working_dir_from_mount.is_none(),
                        "app {:?}: multiple mounts have `working_dir = true`",
                        raw.slug,
                    );
                    working_dir_from_mount = Some((host_path.clone(), container_path.clone()));
                }

                ResolvedMount {
                    slug: m.repo.clone(),
                    host_path,
                    container_path,
                    access: m.access,
                    auto_pull,
                    is_working_dir: m.working_dir,
                    // Raw declaration; validate_and_resolve's post-pass may
                    // upgrade this to implicit-primary or panic on ambiguity.
                    primary: m.primary,
                }
            })
            .collect();

        // --- Resolve working directory ---
        // Exactly one of: mount with working_dir=true, or explicit working_dir on app.
        let (working_dir, container_working_dir, working_dir_is_repo) = match (
            &raw.working_dir,
            &working_dir_from_mount,
        ) {
            (Some(_), Some(_)) => panic!(
                "app {:?}: cannot have both explicit `working_dir` and a mount with `working_dir = true`",
                raw.slug,
            ),
            (None, None) => panic!(
                "app {:?}: must have either explicit `working_dir` or a mount with `working_dir = true`",
                raw.slug,
            ),
            (Some(wd), None) => {
                // Explicit working_dir — container_working_dir required if containerized.
                let cwd = if is_containerized {
                    let cwd = raw.container_working_dir.as_ref().unwrap_or_else(|| {
                            panic!(
                                "app {:?}: containerized app with explicit `working_dir` requires `container_working_dir`",
                                raw.slug,
                            )
                        });
                    assert!(
                        cwd.is_absolute(),
                        "app {:?} container_working_dir {:?} must be an absolute path",
                        raw.slug,
                        cwd,
                    );
                    Some(cwd.clone())
                } else {
                    None
                };
                (wd.clone(), cwd, false)
            }
            (None, Some((host_wd, container_wd))) => {
                // Working dir from mount — container_working_dir must be absent.
                assert!(
                    raw.container_working_dir.is_none(),
                    "app {:?}: cannot have `container_working_dir` when working dir comes from a mount",
                    raw.slug,
                );
                (host_wd.clone(), container_wd.clone(), true)
            }
        };

        // Validate working_dir exists (it might be a repo_dir/<slug> that auto-clone created).
        assert!(
            working_dir.is_dir(),
            "app {:?} working_dir {:?} does not exist or is not a directory",
            raw.slug,
            working_dir,
        );

        // --- Resolve container configuration ---
        let (path_mapper, container_spawn) = if let Some(container) = container_cfg {
            let container_working_dir = container_working_dir.unwrap_or_else(|| {
                panic!(
                    "app {:?}: containerized app must have a container working directory",
                    raw.slug,
                )
            });

            // Build path mappings: repo mounts (most-specific) first, then home dir.
            let mut mappings: Vec<PathMapping> = resolved_mounts
                .iter()
                .filter_map(|m| {
                    m.container_path.as_ref().map(|cp| PathMapping {
                        host_root: m.host_path.clone(),
                        container_root: cp.clone(),
                    })
                })
                .collect();
            // Home dir mapping (least-specific, catches everything else).
            mappings.push(PathMapping {
                host_root: container.home_dir.clone(),
                container_root: container.container_home.clone(),
            });

            let mapper = PathMapper::container(mappings);

            // Build repo bind mounts for podman.
            let repo_bind_mounts: Vec<RepoBindMount> = resolved_mounts
                .iter()
                .map(|m| RepoBindMount {
                    host_path: m.host_path.clone(),
                    container_path: m
                        .container_path
                        .clone()
                        .expect("containerized app mounts must have container_path"),
                    read_only: m.access == AccessLevel::ReadOnly,
                })
                .collect();

            // Combine container-level and app-level extra_mounts. Order doesn't
            // matter for podman, but keeping container-level first makes diffs
            // easy to read.
            let mut extra_mounts = container.extra_mounts.clone();
            extra_mounts.extend(raw.extra_mounts.iter().cloned());

            let spawn = ContainerSpawnConfig {
                image: container.image.clone(),
                home_dir: container.home_dir.clone(),
                container_home: container.container_home.clone(),
                host_working_dir: working_dir.clone(),
                container_working_dir: container_working_dir.clone(),
                working_dir_is_repo,
                repo_mounts: repo_bind_mounts,
                extra_mounts,
                extra_args: container.extra_args.clone(),
            };

            (mapper, Some(spawn))
        } else {
            assert!(
                raw.extra_mounts.is_empty(),
                "app {:?}: `extra_mounts` is only valid for containerized apps",
                raw.slug,
            );
            (PathMapper::Identity, None)
        };

        // Validate and resolve attachment targets.
        let mut target_names = std::collections::HashSet::new();
        let attachment_targets: Vec<AttachmentTarget> = raw
            .attachment_targets
            .iter()
            .map(|t| {
                assert!(
                    !t.name.is_empty(),
                    "app {:?}: attachment target name must not be empty",
                    raw.slug
                );
                assert!(
                    target_names.insert(t.name.clone()),
                    "app {:?}: duplicate attachment target {:?}",
                    raw.slug,
                    t.name
                );
                assert!(
                    t.name != "chat",
                    "app {:?}: 'chat' is a reserved attachment target name",
                    raw.slug
                );
                assert!(
                    !t.accept.is_empty(),
                    "app {:?}: attachment target {:?} must accept at least one extension",
                    raw.slug,
                    t.name
                );
                for ext in &t.accept {
                    assert!(
                        ext.starts_with('.'),
                        "app {:?}: attachment target {:?}: extension {:?} must start with '.'",
                        raw.slug,
                        t.name,
                        ext
                    );
                }
                AttachmentTarget {
                    name: t.name.clone(),
                    label: t.label.clone(),
                    accept: t.accept.clone(),
                    multi: t.multi,
                    handler: t.handler.clone(),
                }
            })
            .collect();

        // Resolve integrations: collect names from both `integrations` list and
        // `integration_config` keys, merge global + per-app config, create instances.
        let mut resolved_integrations = HashMap::new();
        let mut integration_names: Vec<String> = raw.integrations.clone();
        for name in raw.integration_config.keys() {
            if !integration_names.contains(name) {
                integration_names.push(name.clone());
            }
        }
        for int_name in &integration_names {
            let factory = integration_registry.get(int_name).unwrap_or_else(|| {
                panic!(
                    "app {:?}: integration {:?} is not registered",
                    raw.slug, int_name,
                )
            });

            // Shallow merge: start with global, override with per-app keys.
            let merged_config = match (
                config.integrations.get(int_name),
                raw.integration_config.get(int_name),
            ) {
                (None, None) => None,
                (Some(global), None) => Some(global.clone()),
                (None, Some(per_app)) => Some(per_app.clone()),
                (Some(global), Some(per_app)) => {
                    Some(shallow_merge_toml(global, per_app, &raw.slug, int_name))
                }
            };

            let instance = factory.create(merged_config.as_ref());

            // Validate: integration-contributed MCP servers don't collide
            // with explicit mcp_servers or the reserved "brenn" name.
            for (server_name, _) in instance.mcp_servers() {
                assert!(
                    server_name != "brenn",
                    "app {:?}: integration {:?} contributes MCP server \"brenn\" \
                     which is reserved for the built-in noop MCP server",
                    raw.slug,
                    int_name,
                );
                assert!(
                    !raw.mcp_servers.contains_key(&server_name),
                    "app {:?}: integration {:?} contributes MCP server {:?} which \
                     collides with an explicit [app.mcp_servers.{}] entry",
                    raw.slug,
                    int_name,
                    server_name,
                    server_name,
                );
            }

            resolved_integrations.insert(int_name.clone(), instance);
        }

        // Validate: no two integrations contribute MCP servers with the same name.
        {
            let mut seen_int_servers: HashMap<String, &str> = HashMap::new();
            for (int_name, integration) in &resolved_integrations {
                for (server_name, _) in integration.mcp_servers() {
                    if let Some(first) = seen_int_servers.get(&server_name) {
                        panic!(
                            "app {:?}: integrations {:?} and {:?} both contribute \
                             MCP server {:?}",
                            raw.slug, first, int_name, server_name,
                        );
                    }
                    seen_int_servers.insert(server_name, int_name);
                }
            }
        }

        // --- Resolve per-app state directory ---
        // Must come after container_spawn is built (we need its home_dir).
        // Containerized: piggyback on the existing home_dir mount so the state
        // dir is automatically visible inside the container via path_mapper.
        // Bare: $XDG_RUNTIME_DIR/brenn/<slug>; per-uid via XDG_RUNTIME_DIR
        //   semantics on systemd hosts; pruned at logout. The caller must have
        //   resolved XDG_RUNTIME_DIR at startup and passed it as `runtime_dir`.
        let state_dir = if let Some(ref spawn) = container_spawn {
            spawn.home_dir.join(".config").join("brenn").join(&raw.slug)
        } else {
            let xdg = runtime_dir.expect(
                "bare app present but no validated XDG_RUNTIME_DIR was provided — \
                 bootstrap must resolve XDG when any bare app is configured",
            );
            xdg.join("brenn").join(&raw.slug)
        };
        std::fs::create_dir_all(&state_dir).unwrap_or_else(|e| {
            panic!(
                "app {:?}: failed to create state_dir {}: {e}",
                raw.slug,
                state_dir.display(),
            )
        });
        tracing::info!(
            app = %raw.slug,
            dir = %state_dir.display(),
            "resolved per-app state_dir",
        );

        let resolved = AppConfig {
            slug: raw.slug.clone(),
            name: raw.name.clone().unwrap_or_else(|| raw.slug.clone()),
            description: raw.description.clone().unwrap_or_default(),
            icon: raw.icon.clone().unwrap_or_default(),
            working_dir: working_dir.clone(),
            model: raw
                .model
                .clone()
                .unwrap_or_else(|| config.claude_defaults.model.clone()),
            single_instance: raw.single_instance,
            singleton: raw.singleton,
            persistent: raw.persistent,
            idle_timeout: if raw.persistent {
                Some(std::time::Duration::from_secs(
                    raw.idle_timeout_secs.unwrap_or(1800),
                ))
            } else {
                None
            },
            compaction: if has_compact_settings {
                Some(CompactionConfig {
                    reminder_pct: raw
                        .compact_reminder_pct
                        .unwrap_or(CompactionConfig::DEFAULT_REMINDER_PCT),
                    soft_pct: raw
                        .compact_soft_pct
                        .unwrap_or(CompactionConfig::DEFAULT_SOFT_PCT),
                    red_pct: raw
                        .compact_red_pct
                        .unwrap_or(CompactionConfig::DEFAULT_RED_PCT),
                    hard_pct: raw
                        .compact_hard_pct
                        .unwrap_or(CompactionConfig::DEFAULT_HARD_PCT),
                    reminder_tokens: raw.compact_reminder_tokens,
                    soft_tokens: raw.compact_soft_tokens,
                    red_tokens: raw.compact_red_tokens,
                    hard_tokens: raw.compact_hard_tokens,
                    idle_duration: std::time::Duration::from_secs(
                        raw.compact_idle_secs
                            .unwrap_or(CompactionConfig::DEFAULT_IDLE_SECS),
                    ),
                })
            } else {
                None
            },
            idle_hook_secs: raw.idle_hook_secs.unwrap_or(270),
            allowed_users: raw.allowed_users.clone(),
            disabled_tools: raw.disabled_tools.clone(),
            mcp_servers: raw.mcp_servers.clone(),
            multiuser: raw.multiuser,
            prefix_username: raw.prefix_username.unwrap_or(raw.multiuser),
            prefix_timestamp: raw.prefix_timestamp.unwrap_or(raw.multiuser),
            prefix_device: raw.prefix_device.unwrap_or(true),
            path_mapper,
            container_spawn,
            start_hooks: raw.start_hooks.clone().unwrap_or_default(),
            post_pull_hooks: raw.post_pull_hooks.clone().unwrap_or_default(),
            startup_hooks: raw.startup_hooks.clone().unwrap_or_default(),
            cc_extra_args: raw.cc_extra_args.clone(),
            approval_rules: raw.approval_rules.clone(),
            attachment_targets,
            integrations: resolved_integrations,
            mounts: resolved_mounts,
            history_replay_limit: raw
                .history_replay_limit
                .unwrap_or(DEFAULT_HISTORY_REPLAY_LIMIT),
            frontmatter: raw.frontmatter.clone(),
            state_dir,
            // Per-app `[app.messaging]` is resolved in a follow-up pass below
            // (after the directory of channels is built). Default to None
            // here so the field stays compatible with apps that don't enable
            // messaging.
            messaging: None,
            // Stamp the global `[messaging].default_send_budget` on every
            // app so `AppConfig::messaging_send_budget()` returns the
            // operator's configured default for apps without a
            // per-app override (or any messaging block at all).
            messaging_default_send_budget: config.messaging.default_send_budget,
            // Access policy is built in the `resolve_access_policies` follow-up
            // phase (after all other phases). Default (empty, deny-everything)
            // here; populated from explicit `grants`/`[app.acl.*]` later.
            policy: crate::access::AppPolicy::default(),
            // Per-app pwa_push block: carry through as-is; validation happens
            // in `resolve_pwa_push_layer` (Phase 5 below).
            pwa_push: raw.pwa_push.clone(),
            // Webhook subscriptions resolved in Phase 7 below.
            webhook_subscriptions: vec![],
            mqtt_subscriptions: vec![],
        };

        let prev = apps.insert(raw.slug.clone(), resolved);
        assert!(prev.is_none(), "duplicate app slug {:?}", raw.slug,);
    }

    // --- Phase 2: per-clone validation (primary ownership, webhook prereqs) ---
    //
    // Primary ownership is scoped to a clone (a slug), which can be mounted
    // by multiple apps. We have to wait until all apps are resolved to see
    // every mount of a given slug and apply the rules:
    //
    //   0 RW mounts         → no primary; any `primary = true` declaration
    //                         is a config error (you can't be primary on a
    //                         clone that nobody can write). Conflicts on
    //                         RO-only clones surface via AlertDispatcher at
    //                         runtime (see docs/designs/repo-sync.md).
    //   1 RW mount          → implicit primary; we set `primary = true` on
    //                         that mount whether or not it declared so.
    //   >=2 RW mounts       → exactly one must declare `primary = true`.
    //                         Zero or >1 declarations → panic.
    //
    // Non-RW (ReadOnly) mounts are never primary.
    validate_primary_ownership(&mut apps);

    // --- Phase 4: messaging ---
    //
    // Validate channels / per-app `messaging` blocks and stash the
    // resolved messaging config on each `AppConfig`. The directory itself
    // (with subscribers populated) is rebuilt from the same source by
    // `build_messaging_directory` at server startup; the apps' resolved
    // configs carry every fact a hot path needs.
    resolve_messaging_layer(&config.channels, &config.messaging, &config.apps, &mut apps);

    // --- Phase 6: MQTT clients ---
    //
    // Resolve `[[mqtt_client]]` entries (validate URLs, load secrets). Panics on
    // any config error.
    let resolved_clients = crate::mqtt::config::resolve_clients(&config.mqtt_clients);

    // WASM-consumer / app slug disjointness is a namespace invariant that
    // directory-keyed WASM resources rely on: identities and other per-owner
    // resources are addressed by the raw, unprefixed owner slug (no `wasm:`/`app:`
    // prefix), so a shared slug would let one owner's resource resolve against the
    // other's. Refuse to start on a collision.
    for consumer in &config.wasm_consumers {
        assert!(
            !apps.contains_key(&consumer.slug),
            "config: [[wasm_consumer]] slug {:?} collides with an [[app]] slug — per-owner \
             resources are keyed by the raw owner slug with no namespace prefix, so a shared \
             slug would let one owner resolve against the other's; rename the WASM consumer or \
             the app so their slugs are disjoint",
            consumer.slug,
        );
    }

    // Resolve per-app `[[app.mqtt_subscription]]` (ingress) blocks against the
    // resolved client map, stamp them onto each
    // `AppConfig::mqtt_subscriptions`, and collect the distinct ingress channels.
    // Each subscription names its channel by full address `mqtt:<client>:<topic>`
    // (client mandatory); address parsing, client validation, topic validation,
    // and generic-param resolution all happen in `resolve_app_mqtt_subscriptions`,
    // which panics on any config error (design §2.4). The distinct ingress
    // channels (deduplicated by `channel_uuid`) are threaded to the bootstrap
    // layer via `ResolvedConfig.mqtt_ingress_channels`, where they drive `mqtt:`
    // channel derivation, the ingress union-set, and the router routing table.
    //
    // No conflict check is needed: `qos`/`urgency` are connection-level on
    // `[[mqtt_client]]`, so the same `(client, topic)` across apps always carries
    // identical delivery intent (design §2.3/§2.4).
    let mut mqtt_ingress_channels: Vec<crate::mqtt::config::ResolvedMqttIngressChannel> =
        Vec::new();
    let mut seen_channel_uuids = std::collections::HashSet::new();
    for raw in &config.apps {
        if raw.mqtt_subscriptions.is_empty() {
            continue;
        }
        let subs = crate::mqtt::config::resolve_app_mqtt_subscriptions(
            raw,
            &resolved_clients,
            &config.messaging,
        );
        for sub in &subs {
            if seen_channel_uuids.insert(sub.channel_uuid) {
                // Resolve the distinct ingress channel (carrying qos/urgency) via the
                // shared helper, exactly as the WASM-consumer walk below does — the
                // canonical sub address re-resolves to the same channel identity.
                // `resolve_app_mqtt_subscriptions` already validated the address,
                // client, and topic, so the helper's client/topic panics here are an
                // invariant re-check: a firing panic is a host bug (diverged client
                // maps), not operator config, despite its operator-shaped message.
                let owner_desc = format!("app {:?}: [[app.mqtt_subscription]]", raw.slug);
                let ch = crate::mqtt::config::resolve_mqtt_ingress_channel(
                    &sub.channel_address,
                    &resolved_clients,
                    &owner_desc,
                );
                mqtt_ingress_channels.push(ch);
            }
        }
        if let Some(app) = apps.get_mut(&raw.slug) {
            app.mqtt_subscriptions = subs;
        }
    }

    // WASM consumers contribute `mqtt:` ingress channels too. A
    // `[[wasm_consumer.subscription]]` naming an `mqtt:<client>:<topic>` channel
    // is a subscribe intent identical to an app's, and must drive the same
    // `mqtt:` channel-entry derivation, broker SUBSCRIBE union, and router route
    // — otherwise `directory.resolve` would panic on a channel no app declared.
    // Walk after the apps and dedupe into the same set by `channel_uuid`: an app
    // and a WASM consumer sharing a filter yield one channel, one SUBSCRIBE, one
    // route, with subscriber fan-out delivering to both. Non-`mqtt:`
    // subscriptions (`brenn:`/`webhook:`) are untouched by this walk. WASM sub
    // params resolve later in `resolve_wasm_consumers` against the derived
    // channel entry; here we need only the channel identity.
    for consumer in &config.wasm_consumers {
        for sub in &consumer.subscriptions {
            if !crate::mqtt::address::is_mqtt_address(&sub.channel) {
                continue;
            }
            let owner_desc = format!("[[wasm_consumer]] {:?}", consumer.slug);
            let ch = crate::mqtt::config::resolve_mqtt_ingress_channel(
                &sub.channel,
                &resolved_clients,
                &owner_desc,
            );
            if seen_channel_uuids.insert(ch.channel_uuid) {
                mqtt_ingress_channels.push(ch);
            }
        }
    }

    // --- Phase 7: webhook transport endpoints ---
    //
    // Resolve `[[webhook_endpoint]]` entries (validate slugs, load secrets,
    // pre-parse header names). Also resolves per-app `[[app.webhook_subscription]]`
    // references, enforces the singleton invariant, and stamps resolved
    // subscriptions onto each app's `AppConfig`. Panics on any config error.
    // The resolved table is threaded to the bootstrap layer via
    // `ResolvedConfig.webhook_endpoints` so each secret file is read exactly
    // once during startup.
    let webhook_endpoints = crate::webhook::config::resolve_webhook_endpoints(
        &config.webhook_endpoints,
        &config.apps,
        &config.wasm_consumers,
        &mut apps,
        &config.wasm,
        &config.messaging,
    );

    // --- Phase 8: access policies ---
    //
    // Build each LLM app's `AppPolicy` from its explicit `grants` + `[app.acl.*]`
    // config (access-control design §2.5.2/§2.5.3). Runs after the other phases so
    // it can cross-check matchers against already-resolved state — every
    // `mqtt_subscribe`/`mqtt_publish` matcher's client slug is verified against the
    // resolved MQTT client map (Phase 6, `resolved_clients`) so an ACL naming a
    // nonexistent client fails fast here rather than silently never-matching at
    // runtime. The policy itself is built *solely* from the operator's explicit
    // grants/acl — there is no legacy-signal projection (resolved OQ2). Panics on
    // any invalid matcher or duplicate grant (operator-authored config, fail-fast).
    resolve_access_policies(&config.apps, &mut apps, &resolved_clients);

    // --- Phase 9: pwa_push ---
    //
    // Validate global `[pwa_push]` block and load/generate the VAPID keypair
    // iff any app actually has the PwaPush capability. This **must** run after
    // Phase 8 (access policies): `resolve_pwa_push_layer` gates on
    // `AppConfig::pwa_push_enabled()`, which reads `policy.has_grant(PwaPush)`
    // (access-control Phase 0, §2.5.1/§2.7) — the single source of truth for
    // "this app has push capability". Running it earlier (when every app's
    // policy is still `AppPolicy::default()`) would see no grants and never load
    // the keypair. Keeping the keypair-required decision co-extensive with the
    // grant restores the invariant the WS dispatch handlers assert
    // (`pwa_push_enabled() ⟹ AppState.pwa_push.is_some()`,
    // `routes/ws/dispatch.rs`); they were decoupled when `pwa_push_enabled()`
    // moved to the policy while this layer still read `[app.pwa_push].enabled`,
    // making a browser-triggerable `expect()` reachable on a config where the
    // grant and the section disagreed.
    //
    // Panics on any config error (missing subject, missing keypair file,
    // invalid subject format). The resolved value is threaded to the bootstrap
    // layer via `ResolvedConfig.pwa_push` so the VAPID keypair file is read
    // exactly once during startup.
    //
    // Note: in test environments the call generates/loads the VAPID keypair file
    // when any app has the PwaPush grant. Tests whose apps carry no PwaPush grant
    // (the common case) skip this entirely.
    let pwa_push = crate::pwa_push::config::resolve_pwa_push_layer(&config.pwa_push, &apps);

    ResolvedConfig {
        apps: Arc::new(apps),
        webhook_endpoints,
        mqtt_ingress_channels,
        mqtt_clients: resolved_clients,
        pwa_push,
    }
}

/// Validate top-level `[[channel]]` and per-app `[app.messaging]` blocks,
/// populating `AppConfig::messaging` on each app that participates in
/// messaging. Panics on any validation failure (see
/// `crate::messaging::config` for the rules).
fn resolve_messaging_layer(
    channels: &[crate::messaging::config::ChannelConfigRaw],
    defaults: &crate::messaging::config::MessagingGlobalConfig,
    raw_apps: &[AppConfigRaw],
    apps: &mut IndexMap<String, AppConfig>,
) {
    let entries = crate::messaging::config::build_channel_entries(channels, defaults);
    let directory = crate::messaging::MessagingDirectory::with_entries(entries);

    for raw in raw_apps {
        let Some(raw_msg) = raw.messaging.as_ref() else {
            continue;
        };
        let resolved =
            crate::messaging::config::resolve_app_messaging(raw, raw_msg, defaults, &directory);
        if let Some(app) = apps.get_mut(&raw.slug) {
            app.messaging = Some(resolved);
        }
    }
}

/// Build each LLM app's resolved `AppPolicy` from its explicit `grants` +
/// `[app.acl.*]` config (access-control design §2.5.2/§2.5.3) and stamp it onto
/// the app's `AppConfig::policy`. Runs as the last resolution phase.
///
/// The policy is built solely from the operator's explicit `grants`/`acl` (no
/// legacy-signal projection — resolved OQ2). `build_app_policy` panics on a
/// duplicate grant or an invalid matcher (operator-authored config, fail-fast),
/// including an `mqtt_subscribe`/`mqtt_publish` matcher naming a client absent
/// from `resolved_clients` (the Phase 6 MQTT client map).
fn resolve_access_policies(
    raw_apps: &[AppConfigRaw],
    apps: &mut IndexMap<String, AppConfig>,
    resolved_clients: &IndexMap<String, crate::mqtt::config::MqttClientConfig>,
) {
    for raw in raw_apps {
        let Some(app) = apps.get_mut(&raw.slug) else {
            continue;
        };
        app.policy = crate::access::resolve::build_app_policy(
            &raw.slug,
            &raw.grants,
            &raw.acl,
            resolved_clients,
        );
        // Resolve tool grants into the same policy: explicit `[[app.tool_grant]]`
        // tables plus an implicit `git-repo-pull` grant derived from the app's
        // git mounts (mounted ⇒ pullable). Mounts are resolved in an earlier
        // phase, so `app.mounts` is fully populated here.
        let owner = format!("app {:?}", raw.slug);
        let mount_slugs: Vec<String> = app.mounts.iter().map(|m| m.slug.clone()).collect();
        app.policy.tool_grants =
            crate::tools::config::resolve_app_tool_grants(&owner, &raw.tool_grants, &mount_slugs);
        warn_granted_publish_no_matcher(&raw.slug, &app.policy);
    }
}

/// Resolution-time startup warning for the deny-by-default publish hole
/// (access-control design §3 failure mode 1): an app that holds a *publish*
/// grant but authored **no** corresponding publish matcher resolves to a
/// deny-all publish policy — every publish it attempts is silently denied at the
/// action site. This is a legitimate intermediate state (a deferred/empty ACL),
/// so it is a non-fatal warning, **not** a panic. Mirrors the subscribe side's
/// existing posture and the granted-but-no-matcher mitigation the design names.
fn warn_granted_publish_no_matcher(app_slug: &str, policy: &crate::access::AppPolicy) {
    use crate::access::AppCapability;

    // `grant_token` (config `grants` spelling) and `acl_table` (`[[app.acl.*]]`
    // block name) differ only for `MessagingPublish`, whose ACL is `brenn_publish`.
    // The ACL lists have different matcher types, so emptiness is precomputed here
    // (a boot-time, per-app check — cost is irrelevant).
    let checks: [(AppCapability, bool, &str, &str); 3] = [
        (
            AppCapability::MessagingPublish,
            policy.acls.brenn_publish.is_empty(),
            "messaging_publish",
            "brenn_publish",
        ),
        (
            AppCapability::MqttPublish,
            policy.acls.mqtt_publish.is_empty(),
            "mqtt_publish",
            "mqtt_publish",
        ),
        (
            AppCapability::EphemeralPublish,
            policy.acls.ephemeral_publish.is_empty(),
            "ephemeral_publish",
            "ephemeral_publish",
        ),
    ];

    for (grant, acl_empty, grant_token, acl_table) in checks {
        if policy.has_grant(grant) && acl_empty {
            tracing::warn!(
                app_slug = %app_slug,
                "app holds the {grant_token} grant but has no `{acl_table}` ACL matcher \
                 — it resolves to a deny-all publish policy; author an \
                 [[app.acl.{acl_table}]] matcher",
            );
        }
    }
}

/// Per-clone primary-ownership validation (see `docs/designs/repo-sync.md`).
/// Runs after all apps have been resolved. Panics on ambiguity. Promotes
/// the single RW mount on a one-RW clone to implicit primary in place.
fn validate_primary_ownership(apps: &mut IndexMap<String, AppConfig>) {
    // Group every mount by slug, retaining enough info to (a) validate rules
    // and (b) mutate the mount later (via (app_slug, mount_index)).
    let mut groups: HashMap<String, Vec<(String, usize)>> = HashMap::new();
    for app in apps.values() {
        for (idx, mount) in app.mounts.iter().enumerate() {
            groups
                .entry(mount.slug.clone())
                .or_default()
                .push((app.slug.clone(), idx));
        }
    }

    // Two-phase: collect promotions (and panic on any ambiguity) while
    // holding only immutable refs, then apply mutations in a second pass.
    // Promotion is the only non-panic action; `Option` encodes it directly.
    let mut to_promote: Vec<(String, usize)> = Vec::new();

    for (slug, mounts) in &groups {
        let rw: Vec<&(String, usize)> = mounts
            .iter()
            .filter(|(app_slug, idx)| {
                let app = apps.get(app_slug).expect("app must exist");
                app.mounts[*idx].access == AccessLevel::ReadWrite
            })
            .collect();

        // RO-only: no primary allowed, anywhere on this clone.
        if rw.is_empty() {
            for (app_slug, idx) in mounts {
                let app = apps.get(app_slug).expect("app must exist");
                assert!(
                    !app.mounts[*idx].primary,
                    "repo {slug:?}: app {app_slug:?} declares `primary = true` \
                     on a clone with no read-write mounts. Primary ownership \
                     is meaningless without at least one writer.",
                );
            }
            continue;
        }

        // Any `primary = true` declaration on a non-RW mount is invalid.
        for (app_slug, idx) in mounts {
            let app = apps.get(app_slug).expect("app must exist");
            let mount = &app.mounts[*idx];
            if mount.access != AccessLevel::ReadWrite && mount.primary {
                panic!(
                    "repo {slug:?}: app {app_slug:?} declares `primary = true` \
                     on a read-only mount. Only read-write mounts may be primary.",
                );
            }
        }

        // Count explicit primary declarations among the RW mounts.
        let declared: Vec<&(String, usize)> = rw
            .iter()
            .copied()
            .filter(|(app_slug, idx)| {
                apps.get(app_slug).expect("app must exist").mounts[*idx].primary
            })
            .collect();

        match (rw.len(), declared.len()) {
            (1, 0) => {
                // Single-RW-mount clone: promote to implicit-primary.
                to_promote.push((rw[0].0.clone(), rw[0].1));
            }
            (1, 1) => {} // already primary=true; fine.
            (_, 1) => {} // >=2 RW, exactly one declared; fine.
            (_, 0) => {
                let candidates: Vec<String> = rw.iter().map(|(app, _)| app.clone()).collect();
                panic!(
                    "repo {slug:?}: has {n} read-write mounts but no mount declares \
                     `primary = true`. Exactly one of these must set `primary = true`: {candidates:?}. \
                     See docs/designs/repo-sync.md for rationale.",
                    n = rw.len(),
                );
            }
            (_, _) => {
                let conflicting: Vec<String> =
                    declared.iter().map(|(app, _)| app.clone()).collect();
                panic!(
                    "repo {slug:?}: multiple mounts declare `primary = true` ({conflicting:?}). \
                     Exactly one primary per clone.",
                );
            }
        }
    }

    for (app_slug, idx) in to_promote {
        let app = apps.get_mut(&app_slug).expect("app must exist");
        app.mounts[idx].primary = true;
    }
}

/// Validate `server.public_url` at config load time.
///
/// If `public_url` is `None`, this is a no-op — consumers that require the
/// field panic independently with their own messages.
///
/// If `Some`, rejects:
/// - Raw bytes that are control characters (`b < 0x20` or `b == 0x7F`), which
///   URL parsers would silently percent-encode rather than reject.
/// - Values that are not syntactically valid URLs per the `url` crate.
///
/// # Panics
///
/// Panics with a message identifying the offending byte or parse error so the
/// operator can fix the config without reading source.
fn validate_public_url(server: &ServerConfig) {
    let url_str = match server.public_url.as_deref() {
        None => return,
        Some(s) => s,
    };

    // Check for control characters before URL parsing: url::Url would
    // percent-encode these rather than reject them.
    for (i, b) in url_str.bytes().enumerate() {
        if b < 0x20 || b == 0x7F {
            panic!(
                "config: server.public_url contains a control character at byte \
                 position {i} (value 0x{b:02X}): {:?}",
                url_str,
            );
        }
    }

    if let Err(e) = url::Url::parse(url_str) {
        panic!(
            "config: server.public_url is not a valid URL: {e}\n  value: {:?}",
            url_str,
        );
    }
}

/// Maximum accepted value for `server.trusted_proxy_hops`.
///
/// A hard cap (not itself configurable) on the trusted-proxy chain depth. No
/// real self-hosted topology places 8+ trusted proxies in front of an app, and
/// an over-count is a security regression (selects an attacker-controlled
/// `X-Forwarded-For` token), so a fat-fingered large value must be a startup
/// panic rather than a silent spoof.
const MAX_TRUSTED_PROXY_HOPS: u8 = 8;

/// Validate `server.trusted_proxy_hops` at config load time.
///
/// # Panics
///
/// Panics when the value exceeds [`MAX_TRUSTED_PROXY_HOPS`], so an absurd
/// trusted-chain depth fails fast at startup instead of re-opening client-IP
/// spoofing at runtime.
fn validate_trusted_proxy_hops(server: &ServerConfig) {
    if server.trusted_proxy_hops > MAX_TRUSTED_PROXY_HOPS {
        panic!(
            "config: server.trusted_proxy_hops = {} exceeds the maximum of {}; \
             this must equal the number of trusted proxies appending to \
             X-Forwarded-For, and no supported topology has that many",
            server.trusted_proxy_hops, MAX_TRUSTED_PROXY_HOPS,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::AppCapability;
    use crate::access::AppPolicy;
    use crate::access::acl::ChannelMatcher;
    use tracing_test::traced_test;

    #[traced_test]
    #[test]
    fn ephemeral_publish_grant_without_matcher_warns() {
        let policy = AppPolicy::with_grants(&[AppCapability::EphemeralPublish]);
        warn_granted_publish_no_matcher("demo-app", &policy);
        assert!(logs_contain(
            "app holds the ephemeral_publish grant but has no `ephemeral_publish` ACL"
        ));
        assert!(logs_contain("demo-app"));
    }

    #[traced_test]
    #[test]
    fn ephemeral_publish_grant_with_matcher_does_not_warn() {
        let mut policy = AppPolicy::with_grants(&[AppCapability::EphemeralPublish]);
        policy
            .acls
            .ephemeral_publish
            .push(ChannelMatcher::Exact("protobar-demo".to_string()));
        warn_granted_publish_no_matcher("demo-app", &policy);
        assert!(!logs_contain("ephemeral_publish grant but has no"));
    }
}
