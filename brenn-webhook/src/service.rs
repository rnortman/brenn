//! `WebhookService` and `WebhookEventRouter` trait.

use std::collections::HashMap;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::SystemTime;

use brenn_wasm::ReplayComponent;
use http::HeaderMap;

use brenn_lib::wasm_package::Verified;
use brenn_lib::webhook::config::ResolvedWebhookEndpoint;

// ---------------------------------------------------------------------------
// WebhookEventRouter trait
// ---------------------------------------------------------------------------

/// Delivery interface implemented by the binary crate.
#[async_trait::async_trait]
pub trait WebhookEventRouter: Send + Sync + 'static {
    /// Deliver a validated inbound webhook request to the owning app's
    /// channel subscription.
    ///
    /// The endpoint is the entity the request arrived under, handed over rather
    /// than re-looked-up: owner, urgency and the scheme whose headers are
    /// masked must be the ones the request was verified against, and a reload
    /// can retire or replace the table entry while this request is still
    /// reading its body.
    ///
    /// All HTTP transport metadata is passed through so the implementation
    /// can build a `WebhookEnvelope` carrying headers, key_id, client IP,
    /// and received-at timestamp. Returns `Err(String)` if delivery fails
    /// so the HTTP handler can return 500 to the caller instead of silently
    /// returning 204.
    async fn deliver_inbound(
        &self,
        endpoint: &Arc<ResolvedWebhookEndpoint>,
        key_id: &str,
        headers: HeaderMap,
        client_ip: IpAddr,
        received_at: SystemTime,
        raw_body: String,
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
// Endpoint runtime, replay guard, table
// ---------------------------------------------------------------------------

/// One endpoint's serving state: what it is, and the replay store it checks
/// against.
pub struct EndpointRuntime {
    pub endpoint: Arc<ResolvedWebhookEndpoint>,
    /// Present iff the endpoint declares `replay_protection`.
    pub replay: Option<Arc<ReplayGuard>>,
}

impl EndpointRuntime {
    pub fn new(
        endpoint: Arc<ResolvedWebhookEndpoint>,
        replay: Option<Arc<ReplayGuard>>,
    ) -> Arc<Self> {
        Arc::new(Self { endpoint, replay })
    }

    /// This endpoint's slug — the key it is filed under in both indexes.
    pub fn slug(&self) -> &str {
        &self.endpoint.slug
    }
}

/// One replay store, and the component that holds it.
///
/// The lock is the one an inbound request takes around `check`; it also guards
/// the component slot, which is what lets a store be handed from one component
/// to the next without a request observing two holders or none. An empty slot
/// means the component that held this store has been dropped and its
/// replacement is not installed yet.
///
/// TODO(webhook-crate-wasm-dep): naming `ReplayComponent` is why this crate
/// depends on `brenn-wasm` and links wasmtime.
pub struct ReplayGuard {
    pub store_path: PathBuf,
    /// The package release the component in the slot was compiled from. A
    /// reload compares it against what the candidate's package verifies to, so
    /// a bundle that ships new bytes under an unmoved document is a change and
    /// not a component that keeps serving until the next restart.
    pub verified: Verified,
    pub slot: tokio::sync::Mutex<Option<Arc<ReplayComponent>>>,
}

impl ReplayGuard {
    /// A guard over `store_path` holding `component`, compiled from `verified`.
    pub fn new(
        store_path: PathBuf,
        verified: Verified,
        component: Arc<ReplayComponent>,
    ) -> Arc<Self> {
        Arc::new(Self {
            store_path,
            verified,
            slot: tokio::sync::Mutex::new(Some(component)),
        })
    }
}

/// The endpoints this process serves, indexed both ways a lookup arrives.
///
/// Immutable once built; a reload replaces the whole table under the service's
/// write lock, so a request that obtained an entry keeps serving against it.
pub struct WebhookTable {
    /// By mount, the path a request arrives on. Exact string match.
    by_mount: HashMap<String, Arc<EndpointRuntime>>,
    /// By slug, for `list_endpoints_for_app` and the event router's re-lookup.
    by_slug: HashMap<String, Arc<EndpointRuntime>>,
}

impl WebhookTable {
    fn empty() -> Self {
        Self {
            by_mount: HashMap::new(),
            by_slug: HashMap::new(),
        }
    }

    /// This table with `arriving` added or replacing by slug, and `retired`
    /// slugs gone.
    ///
    /// # Panics
    ///
    /// If two arriving entries share a slug or a mount, or if an arriving
    /// entry's mount is held by an entry that is neither retired nor replaced —
    /// the resolver proves both unique across the document, so either is a host
    /// bug.
    fn derive(&self, arriving: &[Arc<EndpointRuntime>], retired: &[String]) -> Self {
        let mut by_slug = self.by_slug.clone();
        for slug in retired {
            by_slug.remove(slug);
        }
        let mut arriving_slugs: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for entry in arriving {
            assert!(
                arriving_slugs.insert(entry.slug()),
                "WebhookService: two arriving entries share the slug {:?}",
                entry.slug(),
            );
            by_slug.insert(entry.slug().to_string(), Arc::clone(entry));
        }
        let mut by_mount: HashMap<String, Arc<EndpointRuntime>> =
            HashMap::with_capacity(by_slug.len());
        for entry in by_slug.values() {
            let previous = by_mount.insert(entry.endpoint.mount.clone(), Arc::clone(entry));
            assert!(
                previous.is_none(),
                "WebhookService: endpoints {:?} and {:?} both mount at {:?}",
                previous.as_ref().expect("checked above").slug(),
                entry.slug(),
                entry.endpoint.mount,
            );
        }
        Self { by_mount, by_slug }
    }
}

// ---------------------------------------------------------------------------
// WebhookService
// ---------------------------------------------------------------------------

/// Holds the endpoint table and the event router.
///
/// The table is swapped, not rebuilt: a request holds the `Arc<EndpointRuntime>`
/// it looked up for its whole life, and the next request sees whatever a reload
/// installed. The lock is held only for a lookup or a swap, never across an
/// await.
pub struct WebhookService {
    table: RwLock<Arc<WebhookTable>>,
    /// Set exactly once at startup via `set_router`.
    pub(crate) router: OnceLock<Arc<dyn WebhookEventRouter>>,
}

impl Default for WebhookService {
    fn default() -> Self {
        Self {
            table: RwLock::new(Arc::new(WebhookTable::empty())),
            router: OnceLock::new(),
        }
    }
}

impl WebhookService {
    /// An empty service. Boot installs the document's endpoints; a document
    /// with none leaves the table empty and every mount unrecognized.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// A service holding `endpoints`, none of them replay-protected.
    #[doc(hidden)]
    pub fn for_test(
        endpoints: impl IntoIterator<Item = Arc<ResolvedWebhookEndpoint>>,
    ) -> Arc<Self> {
        let svc = Self::new();
        svc.install(
            endpoints
                .into_iter()
                .map(|ep| EndpointRuntime::new(ep, None))
                .collect(),
        );
        svc
    }

    fn table(&self) -> Arc<WebhookTable> {
        Arc::clone(
            &self
                .table
                .read()
                .expect("WebhookService table lock poisoned"),
        )
    }

    /// Add or replace `arriving`, keyed by slug. Called by boot and by a
    /// reload's commit.
    pub fn install(&self, arriving: Vec<Arc<EndpointRuntime>>) {
        let mut guard = self
            .table
            .write()
            .expect("WebhookService table lock poisoned");
        *guard = Arc::new(guard.derive(&arriving, &[]));
    }

    /// Drop the named endpoints. From this instant a request to one of their
    /// mounts is an unrecognized URL.
    pub fn retire(&self, slugs: &[String]) {
        let mut guard = self
            .table
            .write()
            .expect("WebhookService table lock poisoned");
        *guard = Arc::new(guard.derive(&[], slugs));
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

    /// Look up an endpoint by the path a request arrived on.
    pub fn endpoint_by_mount(&self, mount: &str) -> Option<Arc<EndpointRuntime>> {
        self.table().by_mount.get(mount).cloned()
    }

    /// Look up an endpoint by slug.
    pub fn endpoint_by_slug(&self, slug: &str) -> Option<Arc<EndpointRuntime>> {
        self.table().by_slug.get(slug).cloned()
    }

    /// Every endpoint this process is serving right now, keyed by slug — the
    /// baseline a reload's prepare compares its candidate against, in the shape
    /// the comparison is keyed on.
    pub fn baseline(&self) -> HashMap<String, Arc<EndpointRuntime>> {
        self.table().by_slug.clone()
    }

    /// Return endpoint views for endpoints owned by the given app slug.
    /// Used by the `MessageChannelList` post-handler extension.
    pub fn list_endpoints_for_app(&self, app_slug: &str) -> Vec<EndpointView> {
        self.table()
            .by_slug
            .values()
            .filter(|entry| entry.endpoint.owner.app_slug() == Some(app_slug))
            .map(|entry| EndpointView {
                slug: entry.endpoint.slug.clone(),
                mount: entry.endpoint.mount.clone(),
                description: entry.endpoint.description.clone(),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_lib::webhook::config::WebhookOwner;
    use brenn_lib::webhook::scheme::{HexFormat, SignatureAlgorithm, SignatureScheme};

    struct NoOpRouter;

    #[async_trait::async_trait]
    impl WebhookEventRouter for NoOpRouter {
        async fn deliver_inbound(
            &self,
            _endpoint: &Arc<ResolvedWebhookEndpoint>,
            _key_id: &str,
            _headers: HeaderMap,
            _client_ip: IpAddr,
            _received_at: SystemTime,
            _raw_body: String,
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
            urgency: brenn_lib::messaging::Urgency::Normal,
            replay_protection: None,
        })
    }

    fn make_endpoint(slug: &str) -> Arc<ResolvedWebhookEndpoint> {
        make_endpoint_with_owner(slug, WebhookOwner::App(Arc::from("test-app")))
    }

    /// Like `make_endpoint` but at an arbitrary mount rather than `/webhooks/<slug>`.
    fn make_endpoint_at(slug: &str, mount: &str) -> Arc<ResolvedWebhookEndpoint> {
        let mut ep = make_endpoint_with_owner(slug, WebhookOwner::App(Arc::from("test-app")));
        Arc::get_mut(&mut ep).expect("sole reference").mount = mount.to_string();
        ep
    }

    /// Like `make_endpoint` but with an arbitrary transport ceiling.
    fn make_endpoint_with_ceiling(slug: &str, ceiling: usize) -> Arc<ResolvedWebhookEndpoint> {
        let mut ep = make_endpoint_with_owner(slug, WebhookOwner::App(Arc::from("test-app")));
        Arc::get_mut(&mut ep)
            .expect("sole reference")
            .transport_ceiling_bytes = ceiling;
        ep
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
        let svc = WebhookService::for_test(vec![app_ep, wasm_ep]);

        let listed = svc.list_endpoints_for_app("test-app");
        assert_eq!(listed.len(), 1, "only the app-owned endpoint is listed");
        assert_eq!(listed[0].slug, "app-ep");
    }

    #[test]
    #[should_panic(expected = "called more than once")]
    fn set_router_panics_on_double_call() {
        let service = WebhookService::new();
        let router1: Arc<dyn WebhookEventRouter> = Arc::new(NoOpRouter);
        let router2: Arc<dyn WebhookEventRouter> = Arc::new(NoOpRouter);
        service.set_router(router1);
        service.set_router(router2); // must panic
    }

    /// Constructor stores endpoint in the vec and makes it findable by slug key.
    #[test]
    fn new_with_one_endpoint_round_trips() {
        let ep = make_endpoint("my-ep");
        let svc = WebhookService::for_test(vec![Arc::clone(&ep)]);

        assert_eq!(svc.baseline().len(), 1);
        let found = svc.endpoint_by_slug("my-ep").expect("should find by slug");
        assert_eq!(found.endpoint.slug, "my-ep");
        assert!(svc.endpoint_by_slug("missing").is_none());
        let by_mount = svc
            .endpoint_by_mount("/webhooks/my-ep")
            .expect("should find by mount");
        assert_eq!(by_mount.endpoint.slug, "my-ep");
        assert!(svc.endpoint_by_mount("/webhooks/nope").is_none());
    }

    /// `install` panics on two live entries claiming one mount. The resolver
    /// proves mounts unique across the document, so this is a host bug.
    #[test]
    #[should_panic(expected = "both mount at")]
    fn install_panics_on_a_mount_collision() {
        let one = make_endpoint("one");
        let two = make_endpoint_at("two", &one.mount);
        WebhookService::for_test(vec![one, two]);
    }

    /// Two endpoints swapping mounts in one reload is one `install` call, so no
    /// request can see the intermediate in which both claim one mount.
    #[test]
    fn a_mount_swap_is_one_install() {
        let svc = WebhookService::for_test(vec![
            make_endpoint_at("a", "/webhooks/first"),
            make_endpoint_at("b", "/webhooks/second"),
        ]);

        svc.install(vec![
            EndpointRuntime::new(make_endpoint_at("a", "/webhooks/second"), None),
            EndpointRuntime::new(make_endpoint_at("b", "/webhooks/first"), None),
        ]);

        assert_eq!(
            svc.endpoint_by_mount("/webhooks/first")
                .expect("first is served")
                .slug(),
            "b"
        );
        assert_eq!(
            svc.endpoint_by_mount("/webhooks/second")
                .expect("second is served")
                .slug(),
            "a"
        );
    }

    /// `retire` takes an endpoint's mount out of the table; what is left stays.
    #[test]
    fn retire_removes_only_the_named_endpoints() {
        let svc = WebhookService::for_test(vec![make_endpoint("keep"), make_endpoint("drop")]);
        svc.retire(&["drop".to_string()]);
        assert!(svc.endpoint_by_slug("drop").is_none());
        assert!(svc.endpoint_by_mount("/webhooks/drop").is_none());
        assert!(svc.endpoint_by_slug("keep").is_some());
    }

    /// A handle a request already holds stays valid across a swap that replaces
    /// its endpoint; the next lookup sees the new one.
    #[test]
    fn an_installed_entry_replaces_its_predecessor_by_slug() {
        let svc = WebhookService::for_test(vec![make_endpoint("ep")]);
        let held = svc.endpoint_by_slug("ep").expect("served before the swap");

        svc.install(vec![EndpointRuntime::new(
            make_endpoint_with_ceiling("ep", 42),
            None,
        )]);

        assert_eq!(
            held.endpoint.transport_ceiling_bytes,
            1024 * 1024,
            "the held handle reads the ceiling it arrived under"
        );
        assert_eq!(
            svc.endpoint_by_slug("ep")
                .expect("served after the swap")
                .endpoint
                .transport_ceiling_bytes,
            42,
        );
    }

    /// Seam test: the resolver's output installs, so resolved endpoints and the
    /// serving table agree on slugs and mounts.
    #[test]
    fn resolver_output_builds_the_service() {
        use std::collections::HashSet;

        use brenn_lib::config::{AppConfigRaw, BrennConfig, ServerConfig, validate_and_resolve};
        use brenn_lib::integration::IntegrationRegistry;
        use brenn_lib::messaging::config::Depth;
        use brenn_lib::webhook::config::WebhookSignatureConfigRaw;
        use brenn_lib::webhook::{
            AppWebhookSubscriptionRaw, WebhookEndpointConfigRaw, WebhookKeyConfigRaw,
        };

        let dir = tempfile::tempdir().unwrap();
        let app_dir = dir.path().join("myapp");
        std::fs::create_dir(&app_dir).unwrap();
        let secret_path = dir.path().join("hmac.secret");
        std::fs::write(&secret_path, "my-super-secret").unwrap();

        let config = BrennConfig {
            server: ServerConfig {
                public_url: Some("https://brenn.example.com".to_string()),
                ..Default::default()
            },
            webhook_endpoints: vec![WebhookEndpointConfigRaw {
                slug: "test-hook".to_string(),
                mount: None,
                description: None,
                transport_ceiling_bytes: 1024 * 1024,
                content_type: "application/json".to_string(),
                signature: WebhookSignatureConfigRaw::HmacRawBody {
                    algorithm: "hmac-sha256".to_string(),
                    header: "X-Hub-Signature-256".to_string(),
                    format: "hex".to_string(),
                    key_id_header: None,
                },
                keys: vec![WebhookKeyConfigRaw {
                    key_id: "k1".to_string(),
                    secret_file: secret_path,
                }],
                tokens: vec![],
                replay_protection: None,
                urgency: None,
            }],
            apps: vec![AppConfigRaw {
                slug: "myapp".to_string(),
                working_dir: Some(app_dir),
                singleton: true,
                allowed_users: vec!["alice".to_string()],
                // singleton apps require at least compact_soft_pct.
                compact_soft_pct: Some(75),
                webhook_subscriptions: vec![AppWebhookSubscriptionRaw {
                    endpoint: "test-hook".to_string(),
                    push_depth: Some(Depth::Bounded(1)),
                    retain_depth: Some(Depth::Bounded(8)),
                    wake_min: None,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let resolved = validate_and_resolve(
            &config,
            &IntegrationRegistry::new(vec![]),
            Some(brenn_lib::runtime_dir::test_runtime_dir_once()),
        );

        let svc = WebhookService::for_test(resolved.webhook_endpoints.into_values());
        let entries = svc.baseline();
        let slugs: HashSet<&str> = entries
            .values()
            .map(|entry| entry.endpoint.slug.as_str())
            .collect();
        assert_eq!(slugs.len(), 1, "one resolved endpoint expected");
        assert!(slugs.contains("test-hook"), "service must expose test-hook");
        assert!(
            svc.endpoint_by_mount("/webhooks/test-hook").is_some(),
            "the resolved mount is the one a request arrives on"
        );
    }
}
