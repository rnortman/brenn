//! Build the PWA push service.

use std::sync::Arc;

use brenn_lib::config::{AppConfig, BrennConfig};
use brenn_lib::obs::alerting::AlertDispatcher;
use brenn_lib::pwa_push::PwaPushSender;
use brenn_lib::pwa_push::PwaPushService;
use brenn_lib::pwa_push::config::ResolvedPwaPushConfig;
use indexmap::IndexMap;

/// Construct the `PwaPushService` from the already-resolved config produced by
/// `validate_and_resolve`.
///
/// `server_origin` must be the value resolved once at bootstrap entry (via
/// `resolve_source`) and shared with `build_messaging` so both publish paths
/// produce consistent `app:<slug>@<server>` identities. This consistency is
/// enforced structurally by resolving `server_origin` once in `run_server` and
/// passing the same value to both builders; no runtime check verifies origin
/// consistency.
///
/// Returns `None` when `pwa_push` is `None` (no app has `pwa_push.enabled = true`).
pub(crate) fn build_pwa_push(
    config: &BrennConfig,
    db: brenn_lib::db::Db,
    apps: &Arc<IndexMap<String, AppConfig>>,
    alert_dispatcher: AlertDispatcher,
    pwa_push: Option<ResolvedPwaPushConfig>,
    server_origin: Option<Arc<str>>,
) -> Option<Arc<dyn PwaPushSender>> {
    pwa_push.map(|cfg| {
        // server_origin is always Some when pwa_push is Some (any_messaging
        // gate in bootstrap/mod.rs ensures resolve_source ran before we get here).
        let server_origin = server_origin.expect(
            "server_origin must be Some when pwa_push is configured; this is a bootstrap bug",
        );
        Arc::new(PwaPushService::new(
            db,
            cfg,
            apps.clone(),
            config.messaging.clone(),
            server_origin,
            alert_dispatcher,
        )) as Arc<dyn PwaPushSender>
    })
}
