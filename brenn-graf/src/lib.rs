//! Graf (knowledge base + todo) integration for Brenn.
//!
//! Provides:
//! - `GrafFactory` / `GrafIntegration` for the integration system
//! - Auto-approve flags for read-only graf MCP tools
//! - Subprocess wrappers for direct-invocation todo operations
//! - `graf_config()` helper for WS handler access to typed config

pub mod subprocess;

#[cfg(test)]
pub(crate) mod test_support;

use std::any::Any;
use std::sync::Arc;

use serde::Deserialize;
use tracing::info;

use brenn_lib::app::{AppTool, AutoApproveTool};
use brenn_lib::config::AppConfig;
use brenn_lib::integration::{Integration, IntegrationFactory};

/// Integration name, used in TOML config keys and the integration registry.
const INTEGRATION_NAME: &str = "graf";

/// Config for the graf integration, deserialized from the merged
/// `[integrations.graf]` + per-app `[integration_config.graf]` TOML.
#[derive(Debug, Clone, Deserialize)]
pub struct GrafConfig {
    /// Graf binary — either a bare name (resolved via PATH) or an absolute path.
    pub command: String,
}

/// Factory for the graf integration.
pub struct GrafFactory;

impl IntegrationFactory for GrafFactory {
    fn name(&self) -> &str {
        INTEGRATION_NAME
    }

    fn create(&self, config: Option<&toml::Value>) -> Arc<dyn Integration> {
        let config: GrafConfig = config
            .expect("graf integration requires [integrations.graf] config with command")
            .clone()
            .try_into()
            .expect("invalid graf integration config");

        Arc::new(GrafIntegration { config })
    }

    fn tools(&self) -> Vec<Box<dyn AppTool>> {
        vec![
            // Read-only graf tools — auto-approve.
            Box::new(AutoApproveTool("mcp__graf__graf_todo_query")),
            Box::new(AutoApproveTool("mcp__graf__graf_lint")),
            // Idempotent derived-data rebuild — auto-approve.
            Box::new(AutoApproveTool("mcp__graf__graf_reindex")),
            // Write tools (todo_add, todo_done, todo_schedule,
            // todo_reorder, graf_fix) use default (require approval).
        ]
    }
}

/// A configured graf integration instance, bound to a specific app.
pub struct GrafIntegration {
    config: GrafConfig,
}

use graf::manifest::RepoConfig as GrafRepoConfig;

impl GrafIntegration {
    /// Construct a `GrafIntegration` with an arbitrary command string.
    /// Intended for tests that need a graf-enabled app config without a real
    /// graf installation. Pointing the command at a nonexistent path ensures
    /// `send_todo_state` hits the subprocess-failure branch (which still emits
    /// an empty `TodoState`) rather than silently no-oping.
    pub fn for_test(command: impl Into<String>) -> Self {
        Self {
            config: GrafConfig {
                command: command.into(),
            },
        }
    }

    /// Build the manifest path pair `(host_path, cc_visible_path)` for an app.
    /// Does NOT check whether any mounted repo is a graf repo — callers that
    /// care must gate on [`Self::has_graf_repo`] first.
    ///
    /// For containerized apps, the host path is under `<home_dir>/.config/graf/`
    /// and the CC-visible path is derived via `path_mapper.to_container`
    /// (the home_dir catch-all mapping guarantees translation succeeds).
    /// For bare apps, both paths are the same: `<state_dir>/graf-manifest.toml`
    /// (slug is encoded in state_dir, so no per-filename slug needed).
    fn build_manifest_paths(app_config: &AppConfig) -> (std::path::PathBuf, std::path::PathBuf) {
        if let Some(ref spawn) = app_config.container_spawn {
            let filename = format!("manifest-{}.toml", app_config.slug);
            let host = spawn.home_dir.join(".config").join("graf").join(&filename);
            let cc_visible = app_config
                .path_mapper
                .to_container(&host)
                .unwrap_or_else(|| {
                    panic!(
                        "BUG: graf manifest host path {} is not under home_dir \
                     — PathMapper construction is inconsistent",
                        host.display(),
                    )
                });
            (host, cc_visible)
        } else {
            let path = app_config.state_dir.join("graf-manifest.toml");
            (path.clone(), path)
        }
    }

    /// True iff at least one mounted repo has a `.graf/config.toml`.
    /// Cheap existence check; does not parse.
    fn has_graf_repo(app_config: &AppConfig) -> bool {
        app_config
            .mounts
            .iter()
            .any(|m| m.host_path.join(".graf").join("config.toml").exists())
    }

    /// Compute manifest paths for an app: `(host_path, cc_visible_path)`.
    /// Returns `None` if no mounted repos have `.graf/config.toml`.
    fn manifest_paths(app_config: &AppConfig) -> Option<(std::path::PathBuf, std::path::PathBuf)> {
        if !Self::has_graf_repo(app_config) {
            return None;
        }
        Some(Self::build_manifest_paths(app_config))
    }
}

impl Integration for GrafIntegration {
    fn name(&self) -> &str {
        INTEGRATION_NAME
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    // No virtual tools — graf tools are real MCP tools, not virtual.
    // No integration-contributed MCP servers — graf MCP is configured
    // explicitly in app.mcp_servers.

    fn prepare(&self, app_config: &AppConfig) {
        // Scan mounted repos for .graf/config.toml — those are graf repos.
        let mut graf_repos = Vec::new();
        for mount in &app_config.mounts {
            match GrafRepoConfig::load(&mount.host_path) {
                Ok(Some(repo_config)) => graf_repos.push((mount, repo_config)),
                Ok(None) => {} // Not a graf repo — no .graf/config.toml.
                Err(e) => panic!(
                    "failed to load graf config from {}: {e}",
                    mount.host_path.display(),
                ),
            }
        }

        if graf_repos.is_empty() {
            return;
        }

        // Reuse the shared path builder. `graf_repos` is non-empty, which is a
        // strictly stronger signal than `has_graf_repo`, so we skip the scan
        // and call `build_manifest_paths` directly.
        let (manifest_path, _) = Self::build_manifest_paths(app_config);
        let repo_paths_are_container = app_config.container_spawn.is_some();

        // Bare apps: state_dir is guaranteed to exist (created in
        // validate_and_resolve). Containerized apps: `<home_dir>/.config/graf/`
        // may not exist yet, so create it.
        if repo_paths_are_container && let Some(parent) = manifest_path.parent() {
            std::fs::create_dir_all(parent).unwrap_or_else(|e| {
                panic!("failed to create manifest dir {}: {e}", parent.display(),)
            });
        }

        // Build manifest content.
        let mut manifest = format!(
            "# Auto-generated by brenn for app {:?}\n# DO NOT EDIT — regenerated on every startup.\n\n",
            app_config.slug,
        );

        for (mount, repo_config) in &graf_repos {
            let path = if repo_paths_are_container {
                mount
                    .container_path
                    .as_ref()
                    .expect("containerized app mount must have container_path")
                    .display()
                    .to_string()
            } else {
                mount.host_path.display().to_string()
            };

            manifest.push_str("[[repo]]\n");
            manifest.push_str(&format!("slug = {:?}\n", repo_config.default_slug.as_str()));
            manifest.push_str(&format!("path = {:?}\n", path));
            manifest.push_str(&format!("id = {:?}\n", repo_config.id.as_str()));
            manifest.push('\n');
        }

        // Domains are not emitted — they're purely informational scaffolding
        // in graf today (no logic depends on them). The old manual manifests
        // didn't include them either. If needed in the future, [[domain]]
        // entries require both `slug` and `id` fields.

        std::fs::write(&manifest_path, &manifest).unwrap_or_else(|e| {
            panic!(
                "failed to write graf manifest {}: {e}",
                manifest_path.display(),
            )
        });

        info!(
            app = %app_config.slug,
            path = %manifest_path.display(),
            repos = graf_repos.len(),
            "wrote graf manifest",
        );
    }

    fn validate(&self, app_config: &AppConfig) {
        // Compute the manifest path so graf manifest check validates
        // the correct manifest (the one prepare() wrote). For containerized
        // apps, use the CC-visible (container) path since the command runs
        // inside the container.
        let manifest_env =
            Self::manifest_paths(app_config).map(|(_, cc_path)| cc_path.display().to_string());

        let (program, args) = if let Some(ref spawn) = app_config.container_spawn {
            let mut podman_args = spawn.base_podman_args();
            // Inject GRAF_MANIFEST env var into the container.
            if let Some(ref manifest) = manifest_env {
                brenn_lib::config::ContainerSpawnConfig::insert_podman_flags(
                    &mut podman_args,
                    &["-e".to_string(), format!("GRAF_MANIFEST={manifest}")],
                );
            }
            podman_args.extend([
                self.config.command.clone(),
                "manifest".into(),
                "check".into(),
                "--json".into(),
            ]);
            ("podman".to_string(), podman_args)
        } else {
            (
                self.config.command.clone(),
                vec!["manifest".into(), "check".into(), "--json".into()],
            )
        };

        info!(program, "running graf manifest check");

        let mut cmd = std::process::Command::new(&program);
        cmd.args(&args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        // For bare apps, set GRAF_MANIFEST in the process environment.
        if app_config.container_spawn.is_none()
            && let Some(ref manifest) = manifest_env
        {
            cmd.env("GRAF_MANIFEST", manifest);
        }

        let output = cmd
            .output()
            .unwrap_or_else(|e| panic!("failed to run graf manifest check ({program}): {e}"));

        assert!(
            output.status.success(),
            "graf manifest check failed (exit {}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr),
        );
    }

    fn env_vars(&self, app_config: &AppConfig) -> Vec<(String, String)> {
        graf_manifest_env(app_config).into_iter().collect()
    }
}

/// Compute the `GRAF_MANIFEST` env var for graf subprocesses.
///
/// Returns `Some(("GRAF_MANIFEST", "<path>"))` if the app has graf repos,
/// using the CC-visible (container-side) path for containerized apps.
/// Returns `None` if no graf repos are mounted.
///
/// Use this when spawning graf subprocesses (todo queries, mutations)
/// to ensure they find the auto-generated manifest.
pub fn graf_manifest_env(app: &brenn_lib::config::AppConfig) -> Option<(String, String)> {
    GrafIntegration::manifest_paths(app)
        .map(|(_, cc_path)| ("GRAF_MANIFEST".to_string(), cc_path.display().to_string()))
}

/// Compute the `GRAF_USER_TZ` env var for graf subprocesses.
///
/// graf's todo machinery uses this to compute "today" in the user's
/// local zone (completion-log headers, tentative_date advances, horizon
/// filters). The LLM can override per-call via graf's `today` MCP
/// parameter; this env var is the fallback default. See
/// `docs/designs/graf-user-tz.md`.
///
/// Type matches `graf_manifest_env` so both pairs can be collected into
/// the same `Vec<(String, String)>`.
pub fn graf_user_tz_env(tz: chrono_tz::Tz) -> (String, String) {
    ("GRAF_USER_TZ".to_string(), tz.name().to_string())
}

/// Extract `GrafConfig` from an app's integration map.
///
/// Returns `None` if the app doesn't have the graf integration enabled.
/// The WS handler calls this to get config for subprocess invocations.
pub fn graf_config(app: &brenn_lib::config::AppConfig) -> Option<&GrafConfig> {
    app.integrations
        .get(INTEGRATION_NAME)
        .and_then(|i| i.as_any().downcast_ref::<GrafIntegration>())
        .map(|g| &g.config)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use super::*;
    use crate::test_support::test_app_config;

    #[test]
    fn graf_factory_name() {
        assert_eq!(GrafFactory.name(), "graf");
    }

    #[test]
    fn graf_factory_tools_count() {
        let tools = GrafFactory.tools();
        assert_eq!(tools.len(), 3, "expected 3 tools, got {}", tools.len());
    }

    #[test]
    fn graf_tools_are_namespaced() {
        let tools = GrafFactory.tools();
        for tool in &tools {
            assert!(
                tool.name().starts_with("mcp__graf__"),
                "tool name should be MCP-namespaced: {}",
                tool.name()
            );
        }
    }

    #[test]
    fn read_only_tools_auto_approve() {
        let tools = GrafFactory.tools();
        for tool in &tools {
            assert!(
                tool.auto_approve(),
                "{} should be auto-approved",
                tool.name()
            );
        }
    }

    #[test]
    fn graf_config_deserializes() {
        let toml_val: toml::Value = toml::from_str(
            r#"
            command = "graf"
            "#,
        )
        .unwrap();
        let config: GrafConfig = toml_val.try_into().unwrap();
        assert_eq!(config.command, "graf");
    }

    #[test]
    #[should_panic(expected = "graf integration requires")]
    fn create_panics_without_config() {
        GrafFactory.create(None);
    }

    #[test]
    #[should_panic(expected = "invalid graf integration config")]
    fn create_panics_on_invalid_config() {
        // Missing required `command` field.
        let toml_val: toml::Value = toml::from_str(r#"foo = "bar""#).unwrap();
        GrafFactory.create(Some(&toml_val));
    }

    #[test]
    fn as_any_downcast_works() {
        let integration: Arc<dyn Integration> = Arc::new(GrafIntegration {
            config: GrafConfig {
                command: "graf".into(),
            },
        });
        let graf = integration
            .as_any()
            .downcast_ref::<GrafIntegration>()
            .expect("downcast to GrafIntegration should succeed");
        assert_eq!(graf.config.command, "graf");
    }

    #[test]
    fn as_any_downcast_fails_for_wrong_type() {
        let integration: Arc<dyn Integration> = Arc::new(GrafIntegration {
            config: GrafConfig {
                command: "graf".into(),
            },
        });
        assert!(
            integration.as_any().downcast_ref::<String>().is_none(),
            "downcast to wrong type should return None"
        );
    }

    /// create() doesn't validate the binary — that's validate()'s job.
    #[test]
    fn create_succeeds_without_manifest_check() {
        let toml_val: toml::Value = toml::from_str(r#"command = "/nonexistent/graf""#).unwrap();
        let integration = GrafFactory.create(Some(&toml_val));
        assert_eq!(integration.name(), "graf");
    }

    #[test]
    fn prepare_writes_manifest_with_graf_default_slug() {
        let dir = tempfile::tempdir().unwrap();
        let repo_dir = dir.path().join("my-graf-repo");
        std::fs::create_dir_all(repo_dir.join(".graf")).unwrap();

        // Write a graf repo config with a default_slug different from the brenn mount slug.
        std::fs::write(
            repo_dir.join(".graf/config.toml"),
            r#"
id = "example.com/life"
default_slug = "life"
"#,
        )
        .unwrap();

        let working_dir = dir.path().join("workdir");
        std::fs::create_dir_all(&working_dir).unwrap();

        let mount = brenn_lib::config::ResolvedMount {
            slug: "my-graf-repo".to_string(), // brenn slug != graf default_slug
            host_path: repo_dir.clone(),
            container_path: None,
            access: brenn_lib::config::AccessLevel::ReadWrite,
            auto_pull: false,
            is_working_dir: false,
            primary: false,
        };

        let app_config = test_app_config(working_dir.clone(), vec![mount], true);

        let integration = GrafIntegration {
            config: GrafConfig {
                command: "graf".into(),
            },
        };
        integration.prepare(&app_config);

        // Read the generated manifest.
        let manifest_path = app_config.state_dir.join("graf-manifest.toml");
        let manifest = std::fs::read_to_string(&manifest_path)
            .unwrap_or_else(|e| panic!("manifest not written: {e}"));

        // The slug in the manifest should be graf's default_slug ("life"),
        // NOT the brenn mount slug ("my-graf-repo").
        assert!(
            manifest.contains(r#"slug = "life""#),
            "manifest should use graf's default_slug, not brenn mount slug. Got:\n{manifest}",
        );
        assert!(
            !manifest.contains(r#"slug = "my-graf-repo""#),
            "manifest should NOT contain brenn mount slug. Got:\n{manifest}",
        );
        assert!(
            manifest.contains(r#"id = "example.com/life""#),
            "manifest should contain the graf repo id. Got:\n{manifest}",
        );
        // Path should be the host path (bare app, no container).
        assert!(
            manifest.contains(&repo_dir.display().to_string()),
            "manifest should contain the host path. Got:\n{manifest}",
        );
    }

    /// Containerized graf apps write the manifest under
    /// `<home_dir>/.config/graf/manifest-<slug>.toml` (not under state_dir).
    /// The design calls out that this path is intentionally unchanged — graf
    /// has a clean home_dir-based location for containerized apps.
    #[test]
    fn prepare_writes_manifest_under_home_dir_for_containerized_apps() {
        let tmp = tempfile::tempdir().unwrap();
        let repo_dir = tmp.path().join("life");
        std::fs::create_dir_all(repo_dir.join(".graf")).unwrap();
        std::fs::write(
            repo_dir.join(".graf/config.toml"),
            "id = \"example.com/life\"\ndefault_slug = \"life\"\n",
        )
        .unwrap();

        let host_home = tmp.path().join("host-home");
        let state_dir = host_home.join(".config").join("brenn").join("test");
        std::fs::create_dir_all(&state_dir).unwrap();

        let mount = brenn_lib::config::ResolvedMount {
            slug: "life".to_string(),
            host_path: repo_dir.clone(),
            container_path: Some(PathBuf::from("/home/user/repos/life")),
            access: brenn_lib::config::AccessLevel::ReadWrite,
            auto_pull: false,
            is_working_dir: false,
            primary: false,
        };

        let app_config = brenn_lib::config::AppConfig {
            slug: "test".into(),
            name: "test".into(),
            description: String::new(),
            icon: String::new(),
            working_dir: tmp.path().to_path_buf(),
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
            // Containerized PathMapper: home_dir → container_home catch-all.
            path_mapper: brenn_lib::config::PathMapper::container(vec![
                brenn_lib::config::PathMapping {
                    host_root: host_home.clone(),
                    container_root: PathBuf::from("/home/user"),
                },
            ]),
            container_spawn: Some(brenn_lib::config::ContainerSpawnConfig {
                image: "brenn-cc:latest".into(),
                home_dir: host_home.clone(),
                container_home: PathBuf::from("/home/user"),
                host_working_dir: tmp.path().to_path_buf(),
                container_working_dir: PathBuf::from("/home/user/work"),
                working_dir_is_repo: false,
                repo_mounts: vec![],
                extra_mounts: vec![],
                extra_args: vec![],
            }),
            start_hooks: brenn_lib::config::StartHooksConfig::default(),
            post_pull_hooks: brenn_lib::config::PostPullHooksConfig::default(),
            startup_hooks: brenn_lib::config::StartupHooksConfig::default(),
            cc_extra_args: vec![],
            approval_rules: vec![],
            attachment_targets: vec![],
            integrations: HashMap::new(),
            mounts: vec![mount],
            history_replay_limit: 2000,
            frontmatter: brenn_lib::config::FrontmatterRenderConfig::default(),
            state_dir: state_dir.clone(),
            messaging: None,
            messaging_default_send_budget: 100,
            policy: brenn_lib::access::AppPolicy::default(),
            pwa_push: None,
            webhook_subscriptions: vec![],
            mqtt_subscriptions: vec![],
        };

        let integration = GrafIntegration {
            config: GrafConfig {
                command: "graf".into(),
            },
        };
        integration.prepare(&app_config);

        // Manifest lives under home_dir, NOT under state_dir.
        let expected = host_home
            .join(".config")
            .join("graf")
            .join("manifest-test.toml");
        assert!(
            expected.is_file(),
            "containerized manifest must be at {}",
            expected.display(),
        );

        // And NOT at the state_dir bare-app location.
        let wrong = state_dir.join("graf-manifest.toml");
        assert!(
            !wrong.exists(),
            "containerized app must NOT write manifest under state_dir (found {})",
            wrong.display(),
        );

        // Sanity: manifest contains the container-side repo path (not host),
        // proving we went down the containerized branch of prepare.
        let manifest = std::fs::read_to_string(&expected).unwrap();
        assert!(
            manifest.contains("/home/user/repos/life"),
            "containerized manifest should use container paths; got:\n{manifest}",
        );
    }

    #[test]
    fn prepare_skips_non_graf_repos() {
        let dir = tempfile::tempdir().unwrap();
        let repo_dir = dir.path().join("plain-repo");
        std::fs::create_dir_all(&repo_dir).unwrap();
        // No .graf/config.toml — not a graf repo.

        let working_dir = dir.path().join("workdir");
        std::fs::create_dir_all(&working_dir).unwrap();

        let mount = brenn_lib::config::ResolvedMount {
            slug: "plain-repo".to_string(),
            host_path: repo_dir,
            container_path: None,
            access: brenn_lib::config::AccessLevel::ReadWrite,
            auto_pull: false,
            is_working_dir: false,
            primary: false,
        };

        let app_config = test_app_config(working_dir.clone(), vec![mount], true);

        let integration = GrafIntegration {
            config: GrafConfig {
                command: "graf".into(),
            },
        };
        integration.prepare(&app_config);

        // No manifest should be written.
        let manifest_path = app_config.state_dir.join("graf-manifest.toml");
        assert!(
            !manifest_path.exists(),
            "manifest should not be written for non-graf repos",
        );
    }

    #[test]
    fn graf_manifest_env_returns_path_for_graf_repos() {
        let dir = tempfile::tempdir().unwrap();
        let repo_dir = dir.path().join("life");
        std::fs::create_dir_all(repo_dir.join(".graf")).unwrap();
        std::fs::write(
            repo_dir.join(".graf/config.toml"),
            "id = \"example.com/life\"\ndefault_slug = \"life\"\n",
        )
        .unwrap();

        let working_dir = dir.path().join("workdir");
        std::fs::create_dir_all(&working_dir).unwrap();

        let mount = brenn_lib::config::ResolvedMount {
            slug: "life".to_string(),
            host_path: repo_dir,
            container_path: None,
            access: brenn_lib::config::AccessLevel::ReadWrite,
            auto_pull: false,
            is_working_dir: false,
            primary: false,
        };

        let app_config = test_app_config(working_dir, vec![mount], true);

        let env = graf_manifest_env(&app_config);
        let (key, value) = env.expect("should return env for app with graf repos");
        assert_eq!(key, "GRAF_MANIFEST");
        // Bare app: manifest path must live in state_dir (the fix for
        // working_dir pollution), not in the old working_dir/.brenn/ location.
        assert_eq!(
            value,
            app_config
                .state_dir
                .join("graf-manifest.toml")
                .display()
                .to_string(),
        );
    }

    #[test]
    fn graf_manifest_env_returns_none_without_graf_repos() {
        let dir = tempfile::tempdir().unwrap();
        let app_config = test_app_config(dir.path().to_path_buf(), vec![], true);
        assert!(
            graf_manifest_env(&app_config).is_none(),
            "should return None when no mounts",
        );
    }

    #[test]
    fn graf_user_tz_env_returns_iana_name() {
        let (k, v) = graf_user_tz_env(chrono_tz::America::New_York);
        assert_eq!(k, "GRAF_USER_TZ");
        assert_eq!(v, "America/New_York");
    }

    #[test]
    fn graf_user_tz_env_for_utc() {
        let (k, v) = graf_user_tz_env(chrono_tz::UTC);
        assert_eq!(k, "GRAF_USER_TZ");
        assert_eq!(v, "UTC");
    }

    /// validate() panics when the binary doesn't exist (bare process).
    #[test]
    #[should_panic(expected = "failed to run graf manifest check")]
    fn validate_panics_when_binary_missing() {
        // Use a tempdir so `test_app_config`'s `state_dir` creation doesn't
        // leak a directory onto a shared host path (would happen with `/tmp`).
        let dir = tempfile::tempdir().unwrap();
        let integration = GrafIntegration {
            config: GrafConfig {
                command: "/nonexistent/graf".into(),
            },
        };
        let app_config = test_app_config(dir.path().to_path_buf(), vec![], true);
        integration.validate(&app_config);
    }
}
