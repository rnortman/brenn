//! Build and wire the webhook service.

use std::sync::Arc;

use brenn_lib::webhook::{ResolvedWebhookEndpoint, WebhookEventRouter, WebhookService};
use indexmap::IndexMap;
use tracing::info;

use crate::webhook_router::WebhookEventRouterImpl;

/// Outcome of building the webhook service.
pub(crate) struct WebhookResult {
    pub(crate) service: Option<Arc<WebhookService>>,
    pub(crate) event_router: Option<Arc<WebhookEventRouterImpl>>,
}

/// Build the webhook service from the pre-resolved endpoint table produced by
/// `validate_and_resolve`.
///
/// Returns `None` values when `endpoints` is empty (i.e. no `[[webhook_endpoint]]`
/// blocks declared, or no app subscribes — `resolve_webhook_endpoints` panics on
/// orphan endpoints so a non-empty table implies at least one subscriber).
///
/// `AppState` injection (`set_state` + `set_router`) must happen after
/// `AppState` construction — same deferred-state pattern as `MqttEventRouterImpl`.
pub(crate) fn build_webhook(
    endpoints: IndexMap<String, Arc<ResolvedWebhookEndpoint>>,
) -> WebhookResult {
    if endpoints.is_empty() {
        return WebhookResult {
            service: None,
            event_router: None,
        };
    }

    info!("wiring webhook service ({} endpoints)", endpoints.len());
    let svc = WebhookService::new(endpoints);
    let router = Arc::new(WebhookEventRouterImpl::new());

    WebhookResult {
        service: Some(svc),
        event_router: Some(router),
    }
}

/// Inject `AppState` into the webhook event router and service.
///
/// # Panics
///
/// Panics if called more than once (the `OnceCell` inside `WebhookEventRouterImpl`
/// does not allow re-setting), or if the state has no messenger configured when
/// webhook endpoints are present — a `[[webhook_endpoint]]` config with no
/// `[[app.channel]]` is a misconfiguration that would silently lose messages at
/// the `.expect` in `deliver_inbound`; fail fast here instead.
pub(crate) async fn wire_webhook_state(
    service: &Arc<WebhookService>,
    router: &Arc<WebhookEventRouterImpl>,
    state: crate::state::AppState,
) {
    assert!(
        state.messenger.is_some(),
        "webhook endpoint(s) configured but no messenger — \
         add at least one [[app.channel]] block or remove [[webhook_endpoint]] blocks"
    );
    router.set_state(state);
    service.set_router(router.clone() as Arc<dyn WebhookEventRouter>);
    info!("webhook service started");
}
