use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use super::alerting::AlertingConfig;
use super::app::AppConfigRaw;
use super::claude_defaults::ClaudeDefaultsConfig;
use super::container::ContainerConfig;
use super::events::EventsConfig;
use super::logging::LoggingConfig;
use super::observability::ObservabilityConfig;
use super::repo::{RepoDeclRaw, RepoSyncConfig};
use super::security::SecurityConfig;
use super::server::{DatabaseConfig, ServerConfig};
use super::surface_description::SurfaceDescriptionConfig;
use super::wasm::WasmConfig;
use super::watchdog::WatchdogConfig;

/// CC built-in tools we've vetted and expect to exist.
/// Used for three purposes:
/// 1. Config validation: warn if `disabled_tools` contains unknown entries.
/// 2. `--tools` whitelist computation: `CC_KNOWN_TOOLS - disabled_tools`.
/// 3. Runtime validation: alert if CC reports tools not in this list.
///
/// MCP tools (`mcp__*`) are NOT included — they're managed separately.
/// Audited from a live CC session's `system/init` response (CC 2.1.112,
/// 2026-04-18; includes tools surfaced by
/// `CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS=1`).
pub const CC_KNOWN_TOOLS: &[&str] = &[
    "AskUserQuestion",
    "Bash",
    "CronCreate",
    "CronDelete",
    "CronList",
    "Edit",
    "EnterPlanMode",
    "EnterWorktree",
    "ExitPlanMode",
    "ExitWorktree",
    "Glob",
    "Grep",
    "LSP",
    "Monitor",
    "NotebookEdit",
    "PushNotification",
    "Read",
    "RemoteTrigger",
    "ScheduleWakeup",
    "SendMessage",
    "Skill",
    "Task",
    "TaskCreate",
    "TaskGet",
    "TaskList",
    "TaskOutput",
    "TaskStop",
    "TaskUpdate",
    "TeamCreate",
    "TeamDelete",
    "TodoWrite",
    "ToolSearch",
    "WebFetch",
    "WebSearch",
    "Write",
];

/// Top-level Brenn configuration.
///
/// Defaults are production-hardened (absolute paths, secure cookies on, etc.).
/// Use `brenn.dev.toml` for local development.
#[derive(Debug, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct BrennConfig {
    pub server: ServerConfig,
    pub database: DatabaseConfig,
    pub logging: LoggingConfig,
    pub security: SecurityConfig,
    pub alerting: Option<AlertingConfig>,
    pub claude_defaults: ClaudeDefaultsConfig,
    /// Where Brenn stores repo clones on the host. Required if any `[[repo]]`
    /// is defined. Resolved relative to cwd like other paths.
    pub repo_dir: Option<PathBuf>,
    /// Repo-sync settings (polling interval, staleness cap, webhook URL prefix
    /// override). See `docs/designs/repo-sync.md`. Omitting the section falls
    /// back to `RepoSyncConfig::default()`.
    pub repo_sync: RepoSyncConfig,
    /// Top-level repo declarations. Each repo is cloned to `<repo_dir>/<slug>/`
    /// on startup if the directory doesn't exist.
    #[serde(default, rename = "repo")]
    pub repos: Vec<RepoDeclRaw>,
    /// Podman container definitions. Apps reference these by name.
    #[serde(default)]
    pub container: HashMap<String, ContainerConfig>,
    /// Global integration defaults. Apps enable integrations by name and can
    /// override specific config keys per-app via `integration_config`.
    #[serde(default)]
    pub integrations: HashMap<String, toml::Value>,
    /// Per-app configurations. At least one must be defined.
    /// Deserialized as a Vec from TOML `[[app]]` array, then validated and
    /// converted to a HashMap keyed by slug via `validate_and_resolve`.
    #[serde(rename = "app")]
    pub apps: Vec<AppConfigRaw>,
    /// Top-level `[[channel]]` declarations. Each entry registers a
    /// messaging channel (UUID + URL-safe address). See
    /// `crate::messaging::config::ChannelConfigRaw`.
    #[serde(default, rename = "channel")]
    pub channels: Vec<crate::messaging::config::ChannelConfigRaw>,
    /// Top-level `[[ephemeral_channel]]` declarations. Each entry registers a
    /// non-persistent (`ephemeral:`) channel (deterministic UUID + name, no DB
    /// row). See `crate::messaging::config::EphemeralChannelConfigRaw`.
    #[serde(default, rename = "ephemeral_channel")]
    pub ephemeral_channels: Vec<crate::messaging::config::EphemeralChannelConfigRaw>,
    /// Global messaging defaults (`[messaging]`). Defaults to
    /// `MessagingGlobalConfig::default()` when absent.
    #[serde(default)]
    pub messaging: crate::messaging::config::MessagingGlobalConfig,
    /// Observability settings (`[observability]`). Defaults to
    /// `ObservabilityConfig::default()` when absent.
    #[serde(default)]
    pub observability: ObservabilityConfig,
    /// Surface self-description settings (`[surface_description]`). Defaults to
    /// `SurfaceDescriptionConfig::default()` (no prefix ⇒ feature off) when absent.
    #[serde(default)]
    pub surface_description: SurfaceDescriptionConfig,
    /// Global VAPID keypair and subject for PWA push notifications.
    /// `[pwa_push]` block. Defaults to `PwaPushGlobalConfig::default()`
    /// (all-None) when absent — safe zero values; no keypair loaded unless
    /// an app has `pwa_push.enabled = true`.
    #[serde(default)]
    pub pwa_push: crate::pwa_push::config::PwaPushGlobalConfig,
    /// Automation engine config (`[automation]`). Defaults to
    /// `AutomationGlobalConfig::default()` when absent.
    #[serde(default)]
    pub automation: crate::automation::config::AutomationGlobalConfig,
    /// Top-level `[[mqtt_client]]` declarations. Each entry defines an MQTT
    /// client (the app-independent connection to a remote MQTT broker/server).
    /// Apps address it for egress via the `mqtt_publish` ACL naming the client
    /// slug, and subscribe to it via `[[app.mqtt_subscription]]` naming
    /// `mqtt:<client>:<topic>` (ingress).
    #[serde(default, rename = "mqtt_client")]
    pub mqtt_clients: Vec<crate::mqtt::config::MqttClientConfigRaw>,
    /// Top-level `[[webhook_endpoint]]` declarations. Each entry defines an
    /// inbound HTTP webhook endpoint. Apps bind to endpoints via
    /// `[[app.webhook_subscription]]`.
    #[serde(default, rename = "webhook_endpoint")]
    pub webhook_endpoints: Vec<crate::webhook::config::WebhookEndpointConfigRaw>,
    /// Events table retention settings (`[events]`). Defaults to
    /// `EventsConfig::default()` when absent (7-day delivered-row retention).
    #[serde(default)]
    pub events: EventsConfig,
    /// TOML `[[wasm_consumer]]` blocks; see [`crate::messaging::config::WasmConsumerConfigRaw`]
    /// for per-entry fields.
    #[serde(default, rename = "wasm_consumer")]
    pub wasm_consumers: Vec<crate::messaging::config::WasmConsumerConfigRaw>,
    /// Top-level `[[surface]]` blocks; see
    /// [`crate::messaging::config::SurfaceConfigRaw`] for per-entry fields.
    /// Boot-time resolution + cross-validation lives in `bootstrap::messaging`.
    #[serde(default, rename = "surface")]
    pub surfaces: Vec<crate::messaging::config::SurfaceConfigRaw>,
    /// Global WASM-host policy (`[wasm]` block). Controls defaults such as
    /// store size limits. Omitting the block is equivalent to `WasmConfig::default()`.
    #[serde(default)]
    pub wasm: WasmConfig,
    /// Bridge-wedge watchdog settings (`[watchdog]`). Defaults to
    /// `WatchdogConfig::default()` when absent (30 s sweep, 60 s wedge grace).
    #[serde(default)]
    pub watchdog: WatchdogConfig,
}

/// Load configuration from a TOML file.
///
/// If `path` is `Some`, reads that file. If `None`, looks for `brenn.toml` in the
/// current working directory. If neither exists, returns `BrennConfig::default()`.
///
/// # Panics
///
/// Panics if:
/// - `path` is `Some` and the file doesn't exist or fails to parse
/// - `path` is `None` and `brenn.toml` exists in cwd but fails to parse
/// - The file contains unrecognized keys or invalid values
pub fn load_config(path: Option<&Path>) -> BrennConfig {
    let cwd = std::env::current_dir().expect("failed to determine current directory");
    load_config_from(path, &cwd)
}

/// Load configuration, using `fallback_dir` to find `brenn.toml` when no explicit
/// path is given. Separated from `load_config` for testability (avoids
/// `set_current_dir` in tests, which is process-global and not thread-safe).
pub(crate) fn load_config_from(path: Option<&Path>, fallback_dir: &Path) -> BrennConfig {
    match path {
        Some(p) => {
            let contents = std::fs::read_to_string(p)
                .unwrap_or_else(|e| panic!("failed to read config file {}: {e}", p.display()));
            toml::from_str(&contents)
                .unwrap_or_else(|e| panic!("failed to parse config file {}: {e}", p.display()))
        }
        None => {
            let default_path = fallback_dir.join("brenn.toml");
            if default_path.exists() {
                let contents = std::fs::read_to_string(&default_path)
                    .unwrap_or_else(|e| panic!("failed to read {}: {e}", default_path.display()));
                toml::from_str(&contents)
                    .unwrap_or_else(|e| panic!("failed to parse {}: {e}", default_path.display()))
            } else {
                BrennConfig::default()
            }
        }
    }
}
