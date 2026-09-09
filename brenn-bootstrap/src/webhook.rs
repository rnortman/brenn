//! Build and wire the webhook service.

use std::sync::Arc;

use brenn_lib::webhook::ResolvedWebhookEndpoint;
use brenn_webhook::{EndpointRuntime, ReplayGuard, WebhookEventRouter, WebhookService};
use indexmap::IndexMap;
use tracing::info;

use brenn_server::webhook_router::WebhookEventRouterImpl;

/// Outcome of building the webhook service.
pub(crate) struct WebhookResult {
    pub(crate) service: Arc<WebhookService>,
    pub(crate) event_router: Arc<WebhookEventRouterImpl>,
}

/// Build the webhook service from the resolved endpoint table, loading each
/// replay-protected endpoint's component.
///
/// Always returns a service. An empty endpoint table leaves every path under
/// `/webhooks/` unrecognized.
///
/// Each replay component is loaded then opened, in that order (see
/// [`brenn_wasm::ReplayComponent::open_store`]). Panics on a component that
/// cannot be loaded or a store that cannot be opened — a boot that cannot serve
/// a declared endpoint must not serve.
///
/// `AppState` injection (`set_state` + `set_router`) must happen after
/// `AppState` construction — same deferred-state pattern as `MqttEventRouterImpl`.
pub(crate) fn build_webhook(
    endpoints: IndexMap<String, Arc<ResolvedWebhookEndpoint>>,
    components_roots: &[std::path::PathBuf],
) -> WebhookResult {
    info!("wiring webhook service ({} endpoints)", endpoints.len());
    let runtimes: Vec<Arc<EndpointRuntime>> = endpoints
        .into_values()
        .map(|endpoint| {
            let replay = endpoint.replay_protection.as_ref().map(|rp| {
                let (component, verified) = crate::load_verified_replay(
                    &endpoint.slug,
                    components_roots,
                    &rp.component,
                    &rp.store_path,
                    rp.max_page_count,
                    rp.config.clone(),
                );
                component.open_store();
                info!(
                    endpoint = %endpoint.slug,
                    component = %rp.component,
                    store_path = %rp.store_path.display(),
                    component_path = %verified.artifact.display(),
                    root = %verified.root.display(),
                    world = %verified.world,
                    artifact_sha256 = %verified.artifact_sha256,
                    "replay protection loaded"
                );
                ReplayGuard::new(rp.store_path.clone(), verified, Arc::new(component))
            });
            EndpointRuntime::new(endpoint, replay)
        })
        .collect();

    let svc = WebhookService::new();
    svc.install(runtimes);
    let router = Arc::new(WebhookEventRouterImpl::new());

    WebhookResult {
        service: svc,
        event_router: router,
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
    state: brenn_server::state::AppState,
) {
    assert!(
        state.messenger.is_some() || service.baseline().is_empty(),
        "webhook endpoint(s) configured but no messenger — \
         add at least one [[app.channel]] block or remove [[webhook_endpoint]] blocks"
    );
    router.set_state(state);
    service.set_router(router.clone() as Arc<dyn WebhookEventRouter>);
    info!("webhook service started");
}
