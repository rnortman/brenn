//! Build CC subprocess spawn config + MCP server config + allowed-tools list.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use brenn_cc::protocol::outgoing::{HookMatcher, HooksConfig};
use brenn_cc::session::CcSessionConfig;
use brenn_lib::obs::alerting::AlertDispatcher;
use brenn_lib::obs::transcript::TranscriptWriter;
use tracing::info;

/// The MCP server name used in the noop MCP config.
#[cfg_attr(test, allow(dead_code))]
pub(super) const MCP_SERVER_NAME: &str = "brenn";

/// Container-side path where noop_mcp.py is bind-mounted.
pub(super) const CONTAINER_MCP_SCRIPT_PATH: &str = "/opt/brenn/noop_mcp.py";

/// Build the `CcSessionConfig` used to spawn the CC subprocess from
/// `ActiveBridge::spawn_new()`.
/// Keeps hooks, MCP config, container mounts, and allowed tools in one place.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_cc_session_config(
    app_config: &brenn_lib::config::AppConfig,
    mcp_script_path: &Path,
    model: String,
    container_name_suffix: String,
    resume_session_id: Option<String>,
    transcript: Arc<TranscriptWriter>,
    alert_dispatcher: AlertDispatcher,
    // `user_tz` becomes `GRAF_USER_TZ` in CC's environment so graf MCP
    // calls compute "today" in the user's zone. See
    // `docs/designs/graf-user-tz.md`.
    user_tz: chrono_tz::Tz,
    server_shutting_down: Arc<std::sync::atomic::AtomicBool>,
) -> CcSessionConfig {
    // Register hooks for ALL tools (matcher = None = wildcard).
    //
    // PreToolUse: intercept brenn noop tools (DisplayFile, ProposeReconciliation),
    //   pass through everything else with Continue (no permission opinion).
    // PostToolUse: intercept brenn noop tools (__NOOP__ replacement + summary),
    //   Continue for everything else. Generic tool summaries are emitted from
    //   the ToolResult handler, not here — CC skips PostToolUse on tool errors.
    //
    // Tool-use logging is decoupled from permissions. Hooks observe; permissions gate.
    let hooks = HooksConfig {
        pre_tool_use: Some(vec![HookMatcher {
            hook_callback_ids: vec!["brenn_pre_tool_0".into()],
            timeout: 30,
            matcher: None,
        }]),
        post_tool_use: Some(vec![HookMatcher {
            hook_callback_ids: vec!["brenn_post_tool_0".into()],
            timeout: 300,
            matcher: None,
        }]),
    };

    let mut integration_env_vars: Vec<(String, String)> = app_config
        .integrations
        .values()
        .flat_map(|i| i.env_vars(app_config))
        .collect();

    // `GRAF_USER_TZ` is session-level, not static-per-app — threaded
    // through `spawn_new` from the spawning WsConnection's `self.timezone`
    // (or UTC fallback for autonomous wakes). Merge it into the same
    // env slice as the static integration env vars. graf integration
    // doesn't emit this itself because Integration::env_vars() only has
    // AppConfig in scope; the live browser TZ isn't there.
    //
    // Harmless if graf isn't enabled — graf binaries won't read it, but
    // setting an unused env var on CC's process doesn't cost anything.
    //
    // Push BEFORE the duplicate-key assertion so the assertion covers this
    // key too — a future integration emitting `GRAF_USER_TZ` would be
    // caught here rather than silently overriding it.
    integration_env_vars.push(brenn_graf::graf_user_tz_env(user_tz));

    // Panic on duplicate env var keys — two integrations emitting the same
    // key would produce silent last-write-wins behaviour (both for bare-app
    // `command.envs()` and podman `-e` flags). Catch it here before a future
    // second integration silently clobbers an existing one.
    {
        let mut seen = std::collections::HashSet::new();
        for (key, _) in &integration_env_vars {
            assert!(
                seen.insert(key.as_str()),
                "BUG: duplicate integration env var key {key:?} — two integrations are \
                 emitting the same key for app {:?}; one would silently override the other",
                app_config.slug,
            );
        }
    }

    // When containerized, the host-side noop_mcp.py must be bind-mounted in
    // and the MCP config must reference the container-side path.
    // Integration env vars are injected as -e flags.
    let (mcp_script_for_config, container) = if let Some(ref spawn) = app_config.container_spawn {
        let mut spawn = spawn.clone();
        spawn.extra_mounts.push(format!(
            "{}:{}:ro,z",
            mcp_script_path.display(),
            CONTAINER_MCP_SCRIPT_PATH,
        ));
        // Add integration env vars as -e flags via extra_args.
        for (key, val) in &integration_env_vars {
            spawn.extra_args.push("-e".into());
            spawn.extra_args.push(format!("{key}={val}"));
        }
        (Path::new(CONTAINER_MCP_SCRIPT_PATH), Some(spawn))
    } else {
        (mcp_script_path, None)
    };

    // Containerized apps need the CC-visible path; bare apps use the host path.
    let virtual_tools_host = app_config.virtual_tools_path();
    let virtual_tools_for_config = if container.is_some() {
        app_config
            .path_mapper
            .to_container(&virtual_tools_host)
            .unwrap_or_else(|| {
                panic!(
                    "BUG: virtual tools path {} is not under home_dir \
                     — PathMapper construction is inconsistent",
                    virtual_tools_host.display(),
                )
            })
    } else {
        virtual_tools_host
    };

    let containerized = container.is_some();
    let mcp_config = build_mcp_config(
        mcp_script_for_config,
        &virtual_tools_for_config,
        app_config,
        containerized,
    );
    let allowed_tools = compute_allowed_tools(&app_config.disabled_tools);

    // `--add-dir` for every mount. Including the working-dir mount is
    // redundant (it's already cwd) but keeps this list mechanically equal
    // to `app_config.mounts`.
    let add_dirs: Vec<PathBuf> = app_config
        .mounts
        .iter()
        .map(|m| m.visible_path(containerized).to_path_buf())
        .collect();

    CcSessionConfig {
        model,
        cwd: app_config.working_dir.clone(),
        hooks: Some(hooks),
        mcp_config: Some(mcp_config),
        allowed_tools,
        resume_session_id,
        transcript,
        alert_dispatcher,
        container,
        app_slug: app_config.slug.clone(),
        container_name_suffix,
        add_dirs,
        cc_extra_args: app_config.cc_extra_args.clone(),
        // For bare apps, integration env vars go to the subprocess directly.
        // For containerized apps, they were already injected as podman -e flags above.
        env_vars: if app_config.container_spawn.is_none() {
            integration_env_vars
        } else {
            vec![]
        },
        // Pre-seeded with the server-level shutting_down flag so a bridge
        // spawned during the shutdown window (after shutdown_signal returned
        // but before axum finishes draining) inherits the flag and its reader
        // task suppresses the spurious EOF Critical alert on process teardown.
        shutting_down: Some(server_shutting_down),
    }
}

/// MCP-server prefix CC prepends to every noop-server tool name. The
/// virtual-tools file lists the short (unprefixed) names; a descriptor's
/// `mcp_name` is the full `mcp__brenn__<Short>` form.
const MCP_BRENN_PREFIX: &str = "mcp__brenn__";

/// Project each registry tool the app is granted into a `VirtualToolDef` (MCP
/// declaration). One declaration source for registry tools — the three-place
/// name lockstep ends for every migrated tool. The short name is the
/// descriptor's `mcp_name` with the `mcp__brenn__` prefix stripped (the noop
/// MCP server declares short names; CC re-prefixes them).
fn registry_virtual_tools(
    policy: &brenn_lib::access::AppPolicy,
    registry: &crate::tool_registry::ToolRegistry,
) -> Vec<brenn_lib::integration::VirtualToolDef> {
    let mut out = Vec::new();
    for tool_name in policy.tool_grants.keys() {
        let tool = registry.get(tool_name).unwrap_or_else(|| {
            // A grant naming a tool the registry doesn't hold is a config bug
            // `validate_config` panics on at startup. Reaching here means that
            // check was bypassed (e.g. an ordering regression) — fail loudly
            // rather than silently drop the app's granted tool, matching the
            // `strip_prefix` panic below.
            panic!("tool_grant names unregistered tool {tool_name:?} (validate_config bypassed)")
        });
        let desc = tool.descriptor();
        let short = desc
            .mcp_name
            .strip_prefix(MCP_BRENN_PREFIX)
            .unwrap_or_else(|| {
                panic!(
                    "tool {:?} mcp_name {:?} does not start with {MCP_BRENN_PREFIX:?}",
                    desc.name, desc.mcp_name,
                )
            });
        out.push(brenn_lib::integration::VirtualToolDef {
            name: short.to_string(),
            description: desc.description.to_string(),
            input_schema: desc.input_schema.clone(),
        });
    }
    out
}

/// Write the virtual tools JSON file for the noop MCP server.
///
/// Collects core virtual tools + granted registry tools + integration-
/// contributed virtual tools and writes them to `<state_dir>/virtual-tools.json`.
/// The state_dir is guaranteed to exist (created at config-resolve time).
///
/// Called once per app at startup (not per CC spawn).
pub(crate) fn write_virtual_tools_file(
    app_config: &brenn_lib::config::AppConfig,
    registry: &crate::tool_registry::ToolRegistry,
) -> PathBuf {
    use brenn_lib::integration::{VirtualToolDef, core_virtual_tools, repo_virtual_tools};

    let tools: Vec<VirtualToolDef> = core_virtual_tools(app_config)
        .into_iter()
        .chain(repo_virtual_tools(&app_config.mounts))
        .chain(registry_virtual_tools(&app_config.policy, registry))
        .chain(
            app_config
                .integrations
                .values()
                .flat_map(|i| i.virtual_tools()),
        )
        .collect();

    let path = app_config.virtual_tools_path();
    let json =
        serde_json::to_string_pretty(&tools).expect("virtual tools serialization should not fail");
    std::fs::write(&path, json)
        .unwrap_or_else(|e| panic!("failed to write virtual tools file {}: {e}", path.display()));

    info!(
        app = %app_config.slug,
        tools = tools.len(),
        path = %path.display(),
        "wrote virtual tools file for noop MCP server",
    );

    path
}

pub(super) fn build_mcp_config(
    mcp_script_path: &Path,
    virtual_tools_path: &Path,
    app_config: &brenn_lib::config::AppConfig,
    containerized: bool,
) -> serde_json::Value {
    let mcp_script = mcp_script_path.to_string_lossy();
    let tools_path = virtual_tools_path.to_string_lossy();
    // Inside containers, python3 isn't on PATH — use `uv run python` instead.
    let (command, args) = if containerized {
        (
            "uv",
            vec!["run", "python", &mcp_script, "--tools", &tools_path],
        )
    } else {
        ("python3", vec![&*mcp_script, "--tools", &tools_path])
    };
    let mut mcp_servers = serde_json::Map::new();
    mcp_servers.insert(
        MCP_SERVER_NAME.to_string(),
        serde_json::json!({
            "command": command,
            "args": args
        }),
    );

    // Explicit MCP servers + integration-contributed MCP servers.
    // Collisions are validated at startup in validate_and_resolve.
    let explicit = app_config
        .mcp_servers
        .iter()
        .map(|(n, s)| (n.clone(), s.clone()));
    let from_integrations = app_config
        .integrations
        .values()
        .flat_map(|i| i.mcp_servers());
    for (name, server) in explicit.chain(from_integrations) {
        let mut entry = serde_json::json!({
            "command": server.command,
            "args": server.args,
        });
        if !server.env.is_empty() {
            entry["env"] = serde_json::json!(server.env);
        }
        mcp_servers.insert(name, entry);
    }

    serde_json::json!({ "mcpServers": mcp_servers })
}

/// Compute the `--tools` whitelist from `disabled_tools`.
/// Returns `None` if no tools are disabled (CC uses its full default set).
pub(super) fn compute_allowed_tools(disabled_tools: &[String]) -> Option<Vec<String>> {
    if disabled_tools.is_empty() {
        None
    } else {
        let allowed: Vec<String> = brenn_lib::config::CC_KNOWN_TOOLS
            .iter()
            .filter(|t| !disabled_tools.iter().any(|d| d == *t))
            .map(|t| t.to_string())
            .collect();
        Some(allowed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_lib::config::PathMapper;
    use std::collections::HashMap;

    // -----------------------------------------------------------------------
    // build_mcp_config
    // -----------------------------------------------------------------------

    /// Minimal AppConfig for tests in this module. Callers mutate the fields
    /// they care about (mcp_servers, mounts, container_spawn, ...); everything
    /// else stays at these defaults.
    fn minimal_test_app_config() -> brenn_lib::config::AppConfig {
        brenn_lib::config::AppConfig {
            slug: "test".into(),
            name: "Test".into(),
            description: String::new(),
            icon: String::new(),
            working_dir: PathBuf::from("/tmp"),
            model: "sonnet".into(),
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
            start_hooks: brenn_lib::config::StartHooksConfig::default(),
            post_pull_hooks: brenn_lib::config::PostPullHooksConfig::default(),
            startup_hooks: brenn_lib::config::StartupHooksConfig::default(),
            cc_extra_args: vec![],
            approval_rules: vec![],
            attachment_targets: vec![],
            integrations: HashMap::new(),
            mounts: vec![],
            history_replay_limit: 2000,
            frontmatter: brenn_lib::config::FrontmatterRenderConfig::default(),
            state_dir: PathBuf::from("/tmp/.brenn/test-state"),
            messaging: None,
            messaging_default_send_budget: 100,
            policy: brenn_lib::access::AppPolicy::default(),
            pwa_push: None,
            webhook_subscriptions: vec![],
            mqtt_subscriptions: vec![],
        }
    }

    #[test]
    fn build_mcp_config_base_only() {
        let app = minimal_test_app_config();
        let config = build_mcp_config(
            Path::new("/opt/brenn/noop_mcp.py"),
            Path::new("/tmp/.brenn/virtual-tools-test.json"),
            &app,
            false,
        );
        let servers = config["mcpServers"].as_object().unwrap();
        assert_eq!(servers.len(), 1);
        assert_eq!(servers["brenn"]["command"], "python3");
        assert_eq!(servers["brenn"]["args"][0], "/opt/brenn/noop_mcp.py");
        assert_eq!(servers["brenn"]["args"][1], "--tools");
        assert_eq!(
            servers["brenn"]["args"][2],
            "/tmp/.brenn/virtual-tools-test.json"
        );
    }

    #[test]
    fn build_mcp_config_containerized() {
        let app = minimal_test_app_config();
        let config = build_mcp_config(
            Path::new("/opt/brenn/noop_mcp.py"),
            Path::new("/tmp/tools.json"),
            &app,
            true,
        );
        let servers = config["mcpServers"].as_object().unwrap();
        assert_eq!(servers["brenn"]["command"], "uv");
        assert_eq!(servers["brenn"]["args"][0], "run");
        assert_eq!(servers["brenn"]["args"][1], "python");
        assert_eq!(servers["brenn"]["args"][2], "/opt/brenn/noop_mcp.py");
        assert_eq!(servers["brenn"]["args"][3], "--tools");
        assert_eq!(servers["brenn"]["args"][4], "/tmp/tools.json");
    }

    #[test]
    fn build_mcp_config_with_custom_servers() {
        use brenn_lib::config::McpServerConfig;
        let mut app_servers = HashMap::new();
        app_servers.insert(
            "custom".to_string(),
            McpServerConfig {
                command: "node".to_string(),
                args: vec!["server.js".to_string()],
                env: HashMap::new(),
            },
        );
        app_servers.insert(
            "with-env".to_string(),
            McpServerConfig {
                command: "python3".to_string(),
                args: vec!["tool.py".to_string()],
                env: HashMap::from([("API_KEY".to_string(), "secret".to_string())]),
            },
        );

        let mut app = minimal_test_app_config();
        app.mcp_servers = app_servers;
        let config = build_mcp_config(
            Path::new("/opt/noop.py"),
            Path::new("/tmp/tools.json"),
            &app,
            false,
        );
        let servers = config["mcpServers"].as_object().unwrap();
        // Base + 2 custom = 3.
        assert_eq!(servers.len(), 3);

        // Base server present.
        assert_eq!(servers["brenn"]["command"], "python3");

        // Custom server without env — no env key.
        assert_eq!(servers["custom"]["command"], "node");
        assert_eq!(servers["custom"]["args"][0], "server.js");
        assert!(servers["custom"].get("env").is_none());

        // Custom server with env.
        assert_eq!(servers["with-env"]["env"]["API_KEY"], "secret");
    }

    // -----------------------------------------------------------------------
    // build_cc_session_config: add_dirs population from mounts
    // -----------------------------------------------------------------------

    /// Helper: build a `ResolvedMount` for tests.
    fn test_mount(
        slug: &str,
        host_path: &str,
        container_path: Option<&str>,
        is_working_dir: bool,
    ) -> brenn_lib::config::ResolvedMount {
        brenn_lib::config::ResolvedMount {
            slug: slug.into(),
            host_path: PathBuf::from(host_path),
            container_path: container_path.map(PathBuf::from),
            access: brenn_lib::config::AccessLevel::ReadWrite,
            auto_pull: false,
            is_working_dir,
            primary: false,
        }
    }

    /// Helper: `ContainerSpawnConfig` with all the uninteresting defaults.
    fn test_container(
        host_wd: &str,
        container_wd: &str,
    ) -> brenn_lib::config::ContainerSpawnConfig {
        brenn_lib::config::ContainerSpawnConfig {
            image: "brenn-cc:latest".into(),
            home_dir: PathBuf::from("/host/home/test"),
            container_home: PathBuf::from("/home/user"),
            host_working_dir: PathBuf::from(host_wd),
            container_working_dir: PathBuf::from(container_wd),
            working_dir_is_repo: true,
            repo_mounts: vec![],
            extra_mounts: vec![],
            extra_args: vec![],
        }
    }

    /// Helper: invoke `build_cc_session_config` with test-grade defaults.
    fn run_build_cc_session_config(app_config: &brenn_lib::config::AppConfig) -> CcSessionConfig {
        run_build_cc_session_config_with_tz(app_config, chrono_tz::UTC)
    }

    /// Variant that takes an explicit `user_tz` for tests that assert
    /// `GRAF_USER_TZ` wiring.
    fn run_build_cc_session_config_with_tz(
        app_config: &brenn_lib::config::AppConfig,
        user_tz: chrono_tz::Tz,
    ) -> CcSessionConfig {
        let dir = tempfile::tempdir().unwrap();
        let transcript = Arc::new(
            brenn_lib::obs::transcript::TranscriptWriter::new(dir.path(), "test.log").unwrap(),
        );
        let (alert_dispatcher, _handle) = brenn_lib::obs::alerting::noop_alert_dispatcher();
        build_cc_session_config(
            app_config,
            Path::new("/opt/brenn/noop_mcp.py"),
            "sonnet".to_string(),
            "conv-test".to_string(),
            None,
            transcript,
            alert_dispatcher,
            user_tz,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
        )
    }

    #[tokio::test]
    async fn build_cc_session_config_bare_uses_host_paths_for_add_dirs() {
        let mut app = minimal_test_app_config();
        app.mounts = vec![
            test_mount("life", "/repos/life", None, true),
            test_mount("docs", "/repos/docs", None, false),
        ];

        let cfg = run_build_cc_session_config(&app);

        // Bare app: add_dirs are host paths. Includes working-dir mount (by design).
        assert_eq!(
            cfg.add_dirs,
            vec![PathBuf::from("/repos/life"), PathBuf::from("/repos/docs")],
        );
    }

    #[tokio::test]
    async fn build_cc_session_config_containerized_uses_container_paths_for_add_dirs() {
        let mut app = minimal_test_app_config();
        app.mounts = vec![
            test_mount(
                "life",
                "/host/repos/life",
                Some("/home/user/repos/life"),
                true,
            ),
            test_mount(
                "docs",
                "/host/repos/docs",
                Some("/home/user/repos/docs"),
                false,
            ),
        ];
        app.container_spawn = Some(test_container("/host/repos/life", "/home/user/repos/life"));

        let cfg = run_build_cc_session_config(&app);

        // Containerized: add_dirs are container-side paths, not host paths.
        assert_eq!(
            cfg.add_dirs,
            vec![
                PathBuf::from("/home/user/repos/life"),
                PathBuf::from("/home/user/repos/docs"),
            ],
        );
    }

    #[tokio::test]
    async fn build_cc_session_config_no_mounts_empty_add_dirs() {
        let app = minimal_test_app_config();
        let cfg = run_build_cc_session_config(&app);
        assert!(cfg.add_dirs.is_empty());
    }

    // Note: `ResolvedMount::visible_path` has its own `#[should_panic]` test
    // in `brenn-lib` covering the "containerized app with missing
    // container_path" invariant, so we don't duplicate it here.

    /// Extract the `--tools <path>` argument passed to the noop MCP server
    /// from a rendered `CcSessionConfig`. Panics if missing — test helper.
    fn mcp_virtual_tools_path(cfg: &CcSessionConfig) -> String {
        let mcp_cfg = cfg.mcp_config.as_ref().expect("mcp_config must be set");
        let args = mcp_cfg["mcpServers"]["brenn"]["args"]
            .as_array()
            .expect("brenn MCP server args must be an array");
        // Bare: [script, --tools, path]. Container: [run, python, script, --tools, path].
        let tools_idx = args
            .iter()
            .position(|a| a.as_str() == Some("--tools"))
            .expect("args must contain --tools")
            + 1;
        args[tools_idx]
            .as_str()
            .expect("path arg must be a string")
            .to_string()
    }

    /// Bare app: the MCP config must carry the host-side state_dir path
    /// verbatim, with no PathMapper translation.
    #[tokio::test]
    async fn build_cc_session_config_bare_passes_host_virtual_tools_path_to_mcp() {
        let tmp = tempfile::tempdir().unwrap();
        let mut app = minimal_test_app_config();
        app.state_dir = tmp.path().to_path_buf();

        let cfg = run_build_cc_session_config(&app);

        assert_eq!(
            mcp_virtual_tools_path(&cfg),
            app.virtual_tools_path().display().to_string(),
        );
    }

    /// Containerized app: the MCP config must carry the container-visible
    /// path (PathMapper::to_container applied to the host state_dir),
    /// because the noop MCP server runs inside the container and only sees
    /// container paths.
    #[tokio::test]
    async fn build_cc_session_config_containerized_translates_virtual_tools_path_in_mcp() {
        let host_home = PathBuf::from("/host/home/test");
        let container_home = PathBuf::from("/home/user");
        let mut app = minimal_test_app_config();
        // state_dir must sit under home_dir so the catch-all mapping covers it.
        app.state_dir = host_home.join(".config").join("brenn").join("test");
        app.path_mapper = PathMapper::container(vec![brenn_lib::config::PathMapping {
            host_root: host_home.clone(),
            container_root: container_home.clone(),
        }]);
        app.container_spawn = Some(test_container("/host/repos/wd", "/home/user/wd"));

        let cfg = run_build_cc_session_config(&app);

        assert_eq!(
            mcp_virtual_tools_path(&cfg),
            "/home/user/.config/brenn/test/virtual-tools.json",
            "containerized virtual-tools path must be translated via PathMapper",
        );
    }

    // -----------------------------------------------------------------------
    // GRAF_USER_TZ plumbing (graf-user-tz.md)
    // -----------------------------------------------------------------------

    /// Bare-app CC spawn puts `GRAF_USER_TZ=<spawn-tz>` into
    /// `env_vars` regardless of whether graf is enabled. (Integration
    /// env vars go directly to the subprocess env on bare apps.)
    #[tokio::test]
    async fn build_cc_session_config_bare_propagates_user_tz_to_env_vars() {
        let app = minimal_test_app_config();
        let cfg = run_build_cc_session_config_with_tz(&app, chrono_tz::America::New_York);

        let pair = cfg
            .env_vars
            .iter()
            .find(|(k, _)| k == "GRAF_USER_TZ")
            .expect("GRAF_USER_TZ must be set in CC env on bare spawn");
        assert_eq!(
            pair.1, "America/New_York",
            "GRAF_USER_TZ must match the spawn-tz argument",
        );
    }

    /// Containerized CC spawn threads `GRAF_USER_TZ` through as a
    /// podman `-e KEY=VAL` flag in `extra_args`, not through
    /// `env_vars` (which is empty for containerized apps).
    #[tokio::test]
    async fn build_cc_session_config_containerized_propagates_user_tz_as_podman_flag() {
        let host_home = PathBuf::from("/host/home/test");
        let container_home = PathBuf::from("/home/user");
        let mut app = minimal_test_app_config();
        app.state_dir = host_home.join(".config").join("brenn").join("test");
        app.path_mapper = PathMapper::container(vec![brenn_lib::config::PathMapping {
            host_root: host_home.clone(),
            container_root: container_home.clone(),
        }]);
        app.container_spawn = Some(test_container("/host/repos/wd", "/home/user/wd"));

        let cfg = run_build_cc_session_config_with_tz(&app, chrono_tz::Asia::Tokyo);

        // Containerized: env_vars must be empty; the var rides on extra_args.
        assert!(
            cfg.env_vars.is_empty(),
            "containerized spawn must not use env_vars: {:?}",
            cfg.env_vars,
        );
        let container = cfg
            .container
            .as_ref()
            .expect("containerized spawn must have container config");
        let flags = &container.extra_args;
        // Find `-e GRAF_USER_TZ=Asia/Tokyo` as an adjacent pair.
        let mut found = false;
        for i in 0..flags.len().saturating_sub(1) {
            if flags[i] == "-e" && flags[i + 1] == "GRAF_USER_TZ=Asia/Tokyo" {
                found = true;
                break;
            }
        }
        assert!(
            found,
            "expected `-e GRAF_USER_TZ=Asia/Tokyo` in podman extra_args: {flags:?}",
        );
    }

    // -----------------------------------------------------------------------
    // write_virtual_tools_file
    // -----------------------------------------------------------------------

    #[test]
    fn write_virtual_tools_file_writes_to_state_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let mut app = minimal_test_app_config();
        app.state_dir = tmp.path().to_path_buf();

        let registry = crate::tool_registry::ToolRegistry::new(vec![]);
        let path = write_virtual_tools_file(&app, &registry);

        // File path is state_dir/virtual-tools.json (no per-slug suffix —
        // the directory is already per-slug).
        assert_eq!(path, tmp.path().join("virtual-tools.json"));
        assert_eq!(path, app.virtual_tools_path());
        assert!(path.is_file(), "virtual tools file must exist after write");

        // Content parses as the expected JSON shape (array of tool defs with
        // the core virtual tools present).
        let json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        let arr = json.as_array().expect("top-level JSON must be an array");
        let names: Vec<&str> = arr
            .iter()
            .filter_map(|t| t.get("name").and_then(|n| n.as_str()))
            .collect();
        assert!(
            names.contains(&"DisplayFile"),
            "core virtual tool DisplayFile must be present; got {names:?}",
        );
    }

    #[test]
    fn write_virtual_tools_file_projects_granted_registry_tool() {
        use std::collections::BTreeMap;
        use std::sync::Arc;

        use brenn_lib::tools::{AclClause, ResolvedToolGrant};

        use crate::tool_registry::{GitRepoPullTool, RegisteredTool, ToolRegistry};

        let tmp = tempfile::tempdir().unwrap();
        let mut app = minimal_test_app_config();
        app.state_dir = tmp.path().to_path_buf();
        // Grant the app git-repo-pull so `registry_virtual_tools` projects it.
        app.policy.tool_grants = BTreeMap::from([(
            "git-repo-pull".to_string(),
            ResolvedToolGrant {
                acl: vec![AclClause::new(BTreeMap::from([(
                    "repo".to_string(),
                    "brenn".to_string(),
                )]))],
                rate_limit: None,
            },
        )]);

        let git_tool = GitRepoPullTool::new(
            Arc::new(Default::default()),
            Arc::new(Default::default()),
            None,
        );
        let registry = ToolRegistry::new(vec![RegisteredTool::Async(Arc::new(git_tool))]);
        let path = write_virtual_tools_file(&app, &registry);

        let json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        let arr = json.as_array().expect("top-level JSON must be an array");
        let names: Vec<&str> = arr
            .iter()
            .filter_map(|t| t.get("name").and_then(|n| n.as_str()))
            .collect();
        assert!(
            names.contains(&"GitRepoPull"),
            "granted registry tool must project into virtual-tools.json; got {names:?}",
        );
    }

    // -----------------------------------------------------------------------
    // compute_allowed_tools
    // -----------------------------------------------------------------------

    #[test]
    fn compute_allowed_tools_empty_disabled_returns_none() {
        assert!(compute_allowed_tools(&[]).is_none());
    }

    #[test]
    fn compute_allowed_tools_subtracts_disabled() {
        let disabled = vec!["Edit".to_string(), "Write".to_string()];
        let allowed = compute_allowed_tools(&disabled).unwrap();

        // Disabled tools should not be in the result.
        assert!(!allowed.contains(&"Edit".to_string()));
        assert!(!allowed.contains(&"Write".to_string()));

        // Other known tools should be present.
        assert!(allowed.contains(&"Read".to_string()));
        assert!(allowed.contains(&"Bash".to_string()));
        assert!(allowed.contains(&"Grep".to_string()));

        // Result should be CC_KNOWN_TOOLS minus the 2 disabled.
        assert_eq!(allowed.len(), brenn_lib::config::CC_KNOWN_TOOLS.len() - 2);
    }

    #[test]
    fn compute_allowed_tools_unknown_disabled_tool_ignored() {
        // A typo in disabled_tools shouldn't cause problems — it just
        // doesn't match anything in CC_KNOWN_TOOLS.
        let disabled = vec!["NonexistentTool".to_string()];
        let allowed = compute_allowed_tools(&disabled).unwrap();
        // All known tools still present (nothing subtracted).
        assert_eq!(allowed.len(), brenn_lib::config::CC_KNOWN_TOOLS.len());
    }

    // -----------------------------------------------------------------------
    // duplicate integration env var guard
    // -----------------------------------------------------------------------

    /// Test-only integration stub that always emits a fixed env var key.
    struct ConstEnvIntegration {
        name: &'static str,
        key: &'static str,
        value: &'static str,
    }

    impl brenn_lib::integration::Integration for ConstEnvIntegration {
        fn name(&self) -> &str {
            self.name
        }

        fn env_vars(&self, _app_config: &brenn_lib::config::AppConfig) -> Vec<(String, String)> {
            vec![(self.key.to_string(), self.value.to_string())]
        }
    }

    /// Two integrations both emitting the same env key must panic before
    /// a silent last-write-wins clobber reaches the CC subprocess.
    #[tokio::test]
    #[should_panic(expected = "duplicate integration env var key")]
    async fn build_cc_session_config_panics_on_duplicate_integration_env_key() {
        use std::sync::Arc;
        let mut app = minimal_test_app_config();
        app.integrations.insert(
            "alpha".to_string(),
            Arc::new(ConstEnvIntegration {
                name: "alpha",
                key: "MY_SHARED_KEY",
                value: "from-alpha",
            }),
        );
        app.integrations.insert(
            "beta".to_string(),
            Arc::new(ConstEnvIntegration {
                name: "beta",
                key: "MY_SHARED_KEY",
                value: "from-beta",
            }),
        );
        run_build_cc_session_config(&app);
    }
}
