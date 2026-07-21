//! `WebhookService` and `WebhookEventRouter` trait.
//!
//! Mirrors `mqtt/service.rs`. Implemented in a follow-up increment.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, OnceLock};
use std::time::SystemTime;

use http::HeaderMap;

use crate::messaging::Urgency;
use crate::webhook::config::{ResolvedWebhookEndpoint, WebhookOwner};

// ---------------------------------------------------------------------------
// WebhookEventRouter trait
// ---------------------------------------------------------------------------

/// Delivery interface implemented by the binary crate.
///
/// Mirrors `MqttEventRouter` in `mqtt/service.rs`. The binary crate's
/// `WebhookEventRouterImpl` uses the deferred-state `OnceCell` pattern.
#[async_trait::async_trait]
pub trait WebhookEventRouter: Send + Sync + 'static {
    /// Deliver a validated inbound webhook request to the owning app's
    /// channel subscription.
    ///
    /// All HTTP transport metadata is passed through so the implementation
    /// can build a `WebhookEnvelope` carrying headers, key_id, client IP,
    /// and received-at timestamp. Returns `Err(String)` if delivery fails
    /// so the HTTP handler can return 500 to the caller instead of silently
    /// returning 204.
    #[allow(clippy::too_many_arguments)]
    async fn deliver_inbound(
        &self,
        endpoint_slug: &str,
        owner: &WebhookOwner,
        key_id: &str,
        headers: HeaderMap,
        client_ip: IpAddr,
        received_at: SystemTime,
        raw_body: String,
        urgency: Urgency,
    ) -> Result<(), String>;
}

// ---------------------------------------------------------------------------
// EndpointView
// ---------------------------------------------------------------------------

/// Lightweight view of an endpoint for `MessageChannelList` listing.
#[derive(Debug, Clone)]
pub struct EndpointView {
    pub slug: String,
    pub mount: String,
    pub description: Option<String>,
}

// ---------------------------------------------------------------------------
// WebhookService
// ---------------------------------------------------------------------------

/// Holds the resolved endpoint table and the event router.
///
/// Held on `AppState` as `Option<Arc<WebhookService>>`, mirroring
/// `Option<Arc<MqttService>>`.
pub struct WebhookService {
    pub(crate) endpoints: Arc<Vec<Arc<ResolvedWebhookEndpoint>>>,
    pub(crate) by_slug: Arc<HashMap<String, Arc<ResolvedWebhookEndpoint>>>,
    /// Set exactly once at startup via `set_router`; read on every inbound
    /// request. `OnceLock` eliminates the async lock acquire and `Option`
    /// indirection from the hot path.
    pub(crate) router: OnceLock<Arc<dyn WebhookEventRouter>>,
}

impl WebhookService {
    /// Construct from a keyed endpoint table.
    ///
    /// Accepts any iterator of `(slug, endpoint)` pairs so callers can pass an
    /// `IndexMap`, a `Vec<(String, Arc<...>)>`, or a single-element array without
    /// needing to discard and re-derive keys.
    pub fn new(
        endpoints: impl IntoIterator<Item = (String, Arc<ResolvedWebhookEndpoint>)>,
    ) -> Arc<Self> {
        let iter = endpoints.into_iter();
        let (lo, _) = iter.size_hint();
        let mut endpoints_vec: Vec<Arc<ResolvedWebhookEndpoint>> = Vec::with_capacity(lo);
        let mut by_slug: HashMap<String, Arc<ResolvedWebhookEndpoint>> = HashMap::with_capacity(lo);
        for (k, v) in iter {
            assert_eq!(
                k, v.slug,
                "WebhookService::new: key {:?} != endpoint slug {:?}",
                k, v.slug
            );
            by_slug.insert(k, Arc::clone(&v));
            endpoints_vec.push(v);
        }
        Arc::new(Self {
            endpoints: Arc::new(endpoints_vec),
            by_slug: Arc::new(by_slug),
            router: OnceLock::new(),
        })
    }

    /// Set the event router after `AppState` construction (deferred-state pattern).
    ///
    /// Panics if called more than once — the router is set exactly once at startup.
    pub fn set_router(&self, router: Arc<dyn WebhookEventRouter>) {
        self.router
            .set(router)
            .ok()
            .expect("WebhookService::set_router called more than once");
    }

    /// Retrieve the router, if set.
    pub fn router(&self) -> Option<Arc<dyn WebhookEventRouter>> {
        self.router.get().cloned()
    }

    /// Look up an endpoint by slug.
    pub fn endpoint_by_slug(&self, slug: &str) -> Option<Arc<ResolvedWebhookEndpoint>> {
        self.by_slug.get(slug).cloned()
    }

    /// Iterate all resolved endpoints. Used by the router to register per-endpoint routes.
    pub fn all_endpoints(&self) -> impl Iterator<Item = &Arc<ResolvedWebhookEndpoint>> {
        self.endpoints.iter()
    }

    /// Return endpoint views for endpoints owned by the given app slug.
    /// Used by the `MessageChannelList` post-handler extension.
    pub fn list_endpoints_for_app(&self, app_slug: &str) -> Vec<EndpointView> {
        self.endpoints
            .iter()
            .filter(|ep| ep.owner.app_slug() == Some(app_slug))
            .map(|ep| EndpointView {
                slug: ep.slug.clone(),
                mount: ep.mount.clone(),
                description: ep.description.clone(),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::webhook::signature::{HexFormat, SignatureAlgorithm, SignatureScheme};

    struct NoOpRouter;

    #[async_trait::async_trait]
    impl WebhookEventRouter for NoOpRouter {
        async fn deliver_inbound(
            &self,
            _endpoint_slug: &str,
            _owner: &WebhookOwner,
            _key_id: &str,
            _headers: HeaderMap,
            _client_ip: IpAddr,
            _received_at: SystemTime,
            _raw_body: String,
            _urgency: Urgency,
        ) -> Result<(), String> {
            Ok(())
        }
    }

    fn make_endpoint_with_owner(slug: &str, owner: WebhookOwner) -> Arc<ResolvedWebhookEndpoint> {
        Arc::new(ResolvedWebhookEndpoint {
            slug: slug.to_string(),
            mount: format!("/webhooks/{slug}"),
            description: None,
            transport_ceiling_bytes: 1024 * 1024,
            content_type: "application/json".to_string(),
            scheme: SignatureScheme::HmacRawBody {
                algorithm: SignatureAlgorithm::HmacSha256,
                header: "x-sig".parse().unwrap(),
                format: HexFormat::V1Hex,
                key_id_header: None,
                keys: {
                    let mut m = HashMap::new();
                    m.insert("k1".to_string(), b"secret".to_vec());
                    m
                },
            },
            owner,
            urgency: Urgency::Normal,
            replay_protection: None,
        })
    }

    fn make_endpoint(slug: &str) -> Arc<ResolvedWebhookEndpoint> {
        make_endpoint_with_owner(slug, WebhookOwner::App(Arc::from("test-app")))
    }

    /// Like `make_endpoint` but owned by a WASM consumer rather than an app.
    fn make_wasm_endpoint(slug: &str, consumer_slug: &str) -> Arc<ResolvedWebhookEndpoint> {
        make_endpoint_with_owner(slug, WebhookOwner::Wasm(Arc::from(consumer_slug)))
    }

    /// `list_endpoints_for_app` returns app-owned endpoints and excludes
    /// wasm-owned ones (infrastructure, not app UI).
    #[test]
    fn list_endpoints_for_app_excludes_wasm_owned() {
        let app_ep = make_endpoint("app-ep"); // owned by "test-app"
        let wasm_ep = make_wasm_endpoint("wasm-ep", "some-consumer");
        let svc = WebhookService::new(vec![
            ("app-ep".to_string(), app_ep),
            ("wasm-ep".to_string(), wasm_ep),
        ]);

        let listed = svc.list_endpoints_for_app("test-app");
        assert_eq!(listed.len(), 1, "only the app-owned endpoint is listed");
        assert_eq!(listed[0].slug, "app-ep");
    }

    #[test]
    #[should_panic(expected = "called more than once")]
    fn set_router_panics_on_double_call() {
        let service = WebhookService::new(Vec::<(String, Arc<ResolvedWebhookEndpoint>)>::new());
        let router1: Arc<dyn WebhookEventRouter> = Arc::new(NoOpRouter);
        let router2: Arc<dyn WebhookEventRouter> = Arc::new(NoOpRouter);
        service.set_router(router1);
        service.set_router(router2); // must panic
    }

    /// Constructor stores endpoint in the vec and makes it findable by slug key.
    #[test]
    fn new_with_one_endpoint_round_trips() {
        let ep = make_endpoint("my-ep");
        let svc = WebhookService::new(vec![("my-ep".to_string(), Arc::clone(&ep))]);

        assert_eq!(svc.all_endpoints().count(), 1);
        let found = svc.endpoint_by_slug("my-ep").expect("should find by key");
        assert_eq!(found.slug, "my-ep");
        assert!(svc.endpoint_by_slug("missing").is_none());
    }

    /// Constructor panics when the iterator key does not match ep.slug.
    #[test]
    #[should_panic(expected = "key \"wrong-key\" != endpoint slug \"real-slug\"")]
    fn new_panics_on_key_slug_mismatch() {
        let ep = make_endpoint("real-slug");
        WebhookService::new(vec![("wrong-key".to_string(), ep)]);
    }
}
