//! Build the automation engine and ingress router.

use std::sync::Arc;

use brenn_lib::automation::AutomationEngine;
use brenn_lib::config::AppConfig;
use brenn_lib::config::BrennConfig;
use brenn_lib::messaging::Messenger;
use brenn_lib::obs::alerting::AlertDispatcher;
use indexmap::IndexMap;

use crate::state::IngressRouterImpl;

/// Outcome of building the automation layer.
pub(crate) struct AutomationResult {
    pub(crate) engine: Option<Arc<AutomationEngine>>,
    pub(crate) ingress_router: Option<Arc<IngressRouterImpl>>,
}

/// Build the automation engine and its ingress router.
///
/// Returns `None` values when no messenger is configured (automation
/// requires messaging; no messenger → no engine).
///
/// `set_state` on the ingress router must be called after `AppState` is
/// constructed (same deferred-state pattern as `WakeRouterImpl`).
pub(crate) fn build_automation(
    config: &BrennConfig,
    db: brenn_lib::db::Db,
    apps: &Arc<IndexMap<String, AppConfig>>,
    messenger: Option<&Arc<Messenger>>,
    alert_dispatcher: AlertDispatcher,
) -> AutomationResult {
    if let Some(m) = messenger {
        let ingress_router = Arc::new(IngressRouterImpl::new());
        let engine = AutomationEngine::new(
            db,
            m.clone(),
            apps.clone(),
            m.directory().clone(),
            ingress_router.clone() as Arc<dyn brenn_lib::automation::IngressRouter>,
            config.automation.clone(),
            alert_dispatcher,
        );
        AutomationResult {
            engine: Some(engine),
            ingress_router: Some(ingress_router),
        }
    } else {
        AutomationResult {
            engine: None,
            ingress_router: None,
        }
    }
}
