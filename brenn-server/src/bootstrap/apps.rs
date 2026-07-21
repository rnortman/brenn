//! App prepare/validate loop, startup hooks, and virtual-tools write.

use std::sync::Arc;

use brenn_lib::config::{AppConfig, BrennConfig};
use brenn_lib::obs::alerting::AlertDispatcher;
use brenn_lib::obs::alerting::AlertSeverity;
use indexmap::IndexMap;
use tracing::{info, warn};

use crate::hooks;

/// Run integration prepare and validate for all apps.
pub(crate) fn prepare_and_validate(apps: &Arc<IndexMap<String, AppConfig>>) {
    for app in apps.values() {
        for integration in app.integrations.values() {
            integration.prepare(app);
        }
    }
    for app in apps.values() {
        for integration in app.integrations.values() {
            integration.validate(app);
        }
    }
}

/// Run startup hooks for all apps that declare them. Blocks until all hooks
/// complete (or are skipped due to pull failure). Exits via panic on hook
/// failure — startup hooks failing is fatal; we refuse to start with stale data.
pub(crate) async fn run_startup_hooks(
    apps: &Arc<IndexMap<String, AppConfig>>,
    alert_dispatcher: &AlertDispatcher,
) {
    for app in apps.values() {
        if app.startup_hooks.host.is_empty() && app.startup_hooks.container.is_empty() {
            continue;
        }
        // Pull all auto_pull mounts for this app first.
        let warnings = hooks::auto_pull_mounts(&app.mounts).await;
        if !warnings.is_empty() {
            for w in &warnings {
                warn!(app = %app.slug, "startup pull warning — skipping startup_hooks: {w}");
            }
            // Alert the operator.
            alert_dispatcher.alert(
                AlertSeverity::Warning,
                format!("startup_hooks skipped for {} due to pull failure", app.slug),
                warnings.join("\n"),
            );
            continue;
        }
        // All pulls succeeded — run startup hooks.
        let result = hooks::run_startup_hooks(app).await;
        match result {
            Ok(()) => info!(app = %app.slug, "startup_hooks completed"),
            Err(e) => {
                // Startup hook failure is fatal — refuse to start with stale data.
                panic!("startup_hooks for {:?} failed: {e}", app.slug);
            }
        }
    }
}

/// Create repo_dir and empty per-repo subdirectories so `validate_and_resolve`
/// passes working_dir checks. Called before validation; actual cloning happens
/// after via `repo_clone::auto_clone_repos`.
pub(crate) fn prepare_repo_dirs(config: &BrennConfig) {
    if let Some(ref repo_dir) = config.repo_dir {
        std::fs::create_dir_all(repo_dir)
            .unwrap_or_else(|e| panic!("failed to create repo_dir {:?}: {e}", repo_dir,));
        for repo in &config.repos {
            let target = repo_dir.join(&repo.slug);
            if !target.exists() {
                std::fs::create_dir_all(&target)
                    .unwrap_or_else(|e| panic!("failed to create repo dir {:?}: {e}", target,));
            }
        }
    }
}

/// Write virtual tools files for each app's noop MCP server (once at startup).
pub(crate) fn write_virtual_tools(
    apps: &Arc<IndexMap<String, AppConfig>>,
    registry: &crate::tool_registry::ToolRegistry,
) {
    for app in apps.values() {
        crate::active_bridge::write_virtual_tools_file(app, registry);
    }
}
