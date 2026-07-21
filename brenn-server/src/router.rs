//! Core application router: build_router, helmet, and companion types.

use axum::Extension;
use axum::body::Body;
use axum::extract::DefaultBodyLimit;
use axum::http::{HeaderValue, Request, StatusCode, Uri};
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::{Router, middleware as axum_mw, routing::get, routing::post};
use axum_helmet::Helmet;
use brenn_lib::config::SecurityConfig;
use brenn_lib::obs::security::{SecurityEventType, log_security_event};
use helmet_core::{
    ContentSecurityPolicy, ReferrerPolicy, StrictTransportSecurity, XContentTypeOptions,
    XFrameOptions, XXSSProtection,
};
use std::sync::LazyLock;
use tower_governor::GovernorLayer;
use tower_governor::governor::GovernorConfigBuilder;
use tracing::warn;

use crate::client_ip;
use crate::middleware;
use crate::routes::{
    app, file, login, logout, redirector, register, statics, surface, upload, webhooks, ws,
};
use crate::state::AppState;

/// Newtype wrapper for `max_image_long_edge` so it can be installed as an
/// Axum `Extension` without conflicting with other `u32` extensions.
#[derive(Clone, Copy)]
pub(crate) struct MaxImageLongEdge(pub(crate) u32);

/// Private coordination header inserted by a governor's error handler and
/// stripped by `detect_rate_limit`. Never reaches the client. Its *value* names
/// the source governor (`"global"` / `"asset"`) so `detect_rate_limit` can log
/// the right `RateLimitHit` detail.
///
/// Using a constant prevents silent breakage if the name ever changes — all
/// uses (insert, contains_key, remove, test assertions) are tied to one string.
const GOVERNOR_MARKER_HEADER: &str = "x-brenn-governor";

/// Marker *values* carried by [`GOVERNOR_MARKER_HEADER`], each naming the
/// governor that produced the 429 so `detect_rate_limit` can label the
/// `RateLimitHit` event. Constants tie the insert side (error handlers) to the
/// recognize side (`detect_rate_limit` match arms), same rationale as the header
/// name itself: a typo'd literal on either side would only surface at the first
/// real 429, as the fail-loud unrecognized-marker arm.
const GOVERNOR_MARKER_GLOBAL: &str = "global";
const GOVERNOR_MARKER_ASSET: &str = "asset";

/// Build a governor error handler that tags its 429 with [`GOVERNOR_MARKER_HEADER`]
/// carrying `marker` as the value, so `detect_rate_limit` can name the source
/// (`"global"` / `"asset"`) in its `RateLimitHit` security event. The auth
/// governor deliberately uses no marker (its 429s are covered by `AuthFailure`
/// events; see its inline error handler). `HeaderValue::from_static` rejects a
/// non-visible-ASCII value at construction — a programming error in the caller's
/// literal, not runtime input.
fn governor_error_handler(
    marker: &'static str,
) -> impl Fn(tower_governor::GovernorError) -> Response + Clone {
    move |err| {
        warn!(rate_limited = true, marker, "rate limit exceeded: {err}");
        let mut resp = err.into_response().map(axum::body::Body::new);
        resp.headers_mut()
            .insert(GOVERNOR_MARKER_HEADER, HeaderValue::from_static(marker));
        resp
    }
}

/// Build a per-client-IP `GovernorConfig` from an interval/burst pair. Every
/// governor in this router keys on the *real* client IP via
/// `ClientIpKeyExtractor`: the builder's default `PeerIpKeyExtractor` keys on
/// the proxy peer (`127.0.0.1` behind nginx), silently collapsing per-IP limits
/// into one shared bucket. Centralizing that load-bearing line here means a new
/// governor cannot copy-paste-drop it. A macro rather than a fn because the
/// finished config's middleware type is `governor::middleware::NoOpMiddleware`,
/// not nameable without taking a direct dependency on the `governor` crate.
macro_rules! per_ip_governor {
    ($interval_secs:expr, $burst:expr, $context:literal) => {
        GovernorConfigBuilder::default()
            .per_second($interval_secs)
            .burst_size($burst)
            .key_extractor(client_ip::ClientIpKeyExtractor)
            .finish()
            .expect(concat!($context, " governor config should be valid"))
    };
}

/// Overlays `Cache-Control: no-cache` on any static-serving `tower::Service`
/// (a `ServeFile` carve-out or a `ServeDir` tree). The wrapped service keeps
/// its native `Last-Modified`/`If-Modified-Since` conditional-revalidation
/// (304) behavior; `no-cache` only forces the browser to re-validate.
fn no_cache<S>(
    service: S,
) -> tower_http::set_header::SetResponseHeader<S, axum::http::HeaderValue> {
    tower::ServiceBuilder::new()
        .layer(tower_http::set_header::SetResponseHeaderLayer::overriding(
            axum::http::header::CACHE_CONTROL,
            axum::http::HeaderValue::from_static("no-cache"),
        ))
        .service(service)
}

/// The `/static/<name>` files served through the [`no_cache`] `ServeFile`
/// carve-out (streamed body, native `Last-Modified`/304, 404 on missing file)
/// rather than the `/static` `ServeDir` tree: the legacy bundle + stylesheet
/// and the surface bootstrap + stylesheet. One list so a new entry cannot
/// silently miss the `no-cache` overlay, driven by both the router builder and
/// the carve-out test. (`nav-on-message.js` is a separate pre-auth carve-out in
/// `utility_routes`, not in this protected list.)
const NO_CACHE_STATICS: &[&str] = &["main.js", "app.css", "surface.js", "surface.css"];

pub(crate) async fn health() -> &'static str {
    "ok"
}

pub(crate) async fn not_found(
    Extension(client_ip::ClientIp(ip)): Extension<client_ip::ClientIp>,
    uri: Uri,
) -> impl IntoResponse {
    // Security log only — no phone alert. This is the global 404 fallback and
    // bots/crawlers hitting unknown URLs would spam alerts and consume rate limit
    // budget. fail2ban handles the banning; phone alerts are for targeted events.
    log_security_event(SecurityEventType::UnrecognizedUrl, ip, uri.path());
    StatusCode::NOT_FOUND
}

/// Middleware that detects global rate-limit rejections and emits a
/// `SecurityEventType::RateLimitHit` security event for fail2ban.
///
/// Must run inside `resolve_client_ip` (needs `ClientIp` extension) and outside
/// both governors (needs to see the 429 response). A governor's error handler
/// inserts a [`GOVERNOR_MARKER_HEADER`] on its 429 responses, with the value
/// naming the source (`"global"` / `"asset"`); this middleware checks for that
/// marker so it only logs for governor-produced 429s, not handler-produced ones
/// (e.g. `ReplayCapHit` in `webhooks/inbound.rs` already emits its own security
/// event and must not be double-logged here). The marker is stripped before the
/// response is returned. An unrecognized marker value is a programming error
/// (a governor whose handler wasn't taught to `detect_rate_limit`): fail loud —
/// `error!` and still log `RateLimitHit` with the raw value rather than dropping
/// the event.
async fn detect_rate_limit(request: Request<Body>, next: Next) -> Response {
    let ip = request
        .extensions()
        .get::<client_ip::ClientIp>()
        .map(|client_ip::ClientIp(addr)| *addr);

    let response = next.run(request).await;

    if response.status() == StatusCode::TOO_MANY_REQUESTS
        && let Some(marker) = response.headers().get(GOVERNOR_MARKER_HEADER)
    {
        let marker_value = marker.to_str().unwrap_or("<non-ascii>").to_owned();
        let detail: std::borrow::Cow<'static, str> = match marker_value.as_str() {
            GOVERNOR_MARKER_GLOBAL => std::borrow::Cow::Borrowed("global rate limit"),
            GOVERNOR_MARKER_ASSET => std::borrow::Cow::Borrowed("asset rate limit"),
            other => {
                tracing::error!(
                    marker = other,
                    "detect_rate_limit: unrecognized governor marker value on a 429 — logging \
                     RateLimitHit with the raw value; a governor error handler is out of sync"
                );
                std::borrow::Cow::Owned(format!("unrecognized rate limit marker ({other})"))
            }
        };
        if let Some(ip) = ip {
            log_security_event(SecurityEventType::RateLimitHit, ip, &detail);
        } else {
            // Should never happen: `resolve_client_ip` is outer to this middleware and
            // always inserts `ClientIp`. If it does happen (layer reordering, test path),
            // fail loudly so the regression is visible rather than silently dropping the
            // security event. The 429 is still returned correctly; only logging is affected.
            tracing::error!(
                "detect_rate_limit: ClientIp missing from extensions on a governor 429 — \
                 RateLimitHit event suppressed; check layer ordering"
            );
        }
        // Strip the internal coordination header before returning to the client.
        let (mut parts, body) = response.into_parts();
        parts.headers.remove(GOVERNOR_MARKER_HEADER);
        return Response::from_parts(parts, body);
    }

    response
}

/// Response extension marker set by `surface_page` to opt its HTML response
/// into the wasm-relaxed CSP. `apply_relaxed_csp` swaps the strict
/// `Content-Security-Policy` for the relaxed one only when this marker is
/// present. It is an extension, not a header, so it never serializes to the
/// client, and the strict policy stays the unconditional default — a handler
/// must opt in explicitly, and only `surface_page` does. There is deliberately
/// no "respect a handler-supplied CSP header" path: that would let any future
/// handler silently weaken the policy.
#[derive(Clone)]
pub(crate) struct RelaxedWasmCsp;

/// Build the site's Content-Security-Policy. `allow_wasm` adds
/// `'wasm-unsafe-eval'` to `script-src` — required for `WebAssembly.compile`/
/// `instantiate` (every shipped engine refuses all wasm compilation under a
/// bare `script-src 'self'`). It grants no JS `eval` and creates no new
/// string→code primitive; a wasm module's reach is a subset of the JS that
/// instantiated it. Applied only to surface HTML documents.
fn content_security_policy(allow_wasm: bool) -> ContentSecurityPolicy<'static> {
    let script_src = if allow_wasm {
        vec!["'self'", "'wasm-unsafe-eval'"]
    } else {
        vec!["'self'"]
    };
    ContentSecurityPolicy::new()
        .default_src(vec!["'self'"])
        .script_src(script_src)
        // No inline styles: server-rendered markdown emits plain
        // `<pre><code>` code blocks (no syntax highlighting), and no
        // other backend-rendered HTML uses inline `style=` attributes.
        .style_src(vec!["'self'"])
        .img_src(vec!["'self'", "blob:"])
        .connect_src(vec!["'self'", "ws:", "wss:"])
        .frame_ancestors(vec!["'none'"])
}

/// The wasm-relaxed `Content-Security-Policy` header value, built once. The
/// value is compile-time constant, so the `from_str` invariant is proven at
/// first router construction (forced in `build_router`) rather than lazily on
/// the first surface page request in production.
static RELAXED_CSP_HEADER: LazyLock<HeaderValue> = LazyLock::new(|| {
    HeaderValue::from_str(&content_security_policy(true).to_string())
        .expect("relaxed CSP is valid ASCII")
});

/// Outer response middleware: when a response carries the [`RelaxedWasmCsp`]
/// marker extension, replace the strict CSP that `helmet_layer` set with the
/// wasm-relaxed one. Runs outside helmet so it sees (and overrides) the header
/// helmet already stamped; keyed on an extension so nothing leaks to the client.
async fn apply_relaxed_csp(request: Request<Body>, next: Next) -> Response {
    let response = next.run(request).await;
    if response.extensions().get::<RelaxedWasmCsp>().is_none() {
        return response;
    }
    let (mut parts, body) = response.into_parts();
    parts.headers.insert(
        axum::http::header::CONTENT_SECURITY_POLICY,
        RELAXED_CSP_HEADER.clone(),
    );
    Response::from_parts(parts, body)
}

/// Build the security headers layer.
pub(crate) fn helmet_layer() -> axum_helmet::HelmetLayer {
    Helmet::new()
        .add(content_security_policy(false))
        .add(XFrameOptions::deny())
        .add(XContentTypeOptions::nosniff())
        .add(ReferrerPolicy::no_referrer())
        .add(XXSSProtection::off()) // Disabled — deprecated, setting to 0 avoids IE's buggy XSS filter.
        .add(
            StrictTransportSecurity::new()
                .max_age(63072000)
                .include_sub_domains(),
        )
        .into_layer()
        .expect("helmet configuration should be valid")
}

/// Build the core application router from the given state.
///
/// When `security` is `Some`, rate limiting and configured body limits are applied.
/// When `None`, rate limiting is disabled and default body limits are used — for tests
/// that use `oneshot()` (which lacks real TCP connections for peer IP extraction).
///
/// `trusted_proxy_hops`: number of trusted reverse-proxy hops in front of Brenn.
/// `0` uses the TCP peer directly; `N >= 1` selects the `N`-th `X-Forwarded-For`
/// token from the right (see `client_ip::resolve_client_ip`).
///
/// `max_image_long_edge`: delivered to browsers via `<meta name="max-image-long-edge">`.
pub(crate) fn build_router(
    state: AppState,
    security: Option<&SecurityConfig>,
    trusted_proxy_hops: u8,
    max_image_long_edge: u32,
) -> Router {
    // Force the relaxed-CSP header value now, so its (constant) validity is
    // proven at boot rather than on the first surface page request.
    LazyLock::force(&RELAXED_CSP_HEADER);

    // --- Pre-auth routes (no session required) ---
    // Everything needed for login/registration, including their static assets.
    let mut auth_routes = Router::new()
        .route(
            "/auth/login",
            get(login::login_page).post(login::login_submit),
        )
        .route(
            "/auth/register",
            get(register::register_page).post(register::register_submit),
        )
        .route("/auth/static/auth.css", get(statics::auth_css));

    if let Some(sec) = security {
        auth_routes = auth_routes.layer(DefaultBodyLimit::max(sec.auth_body_limit));

        // Auth endpoints: rate limited per real client IP via
        // `ClientIpKeyExtractor`. The default `PeerIpKeyExtractor` keys on
        // `ConnectInfo<SocketAddr>`, which behind nginx is always
        // `127.0.0.1` — turning per-IP rate limits into shared global
        // limits for everyone behind the proxy.
        let auth_governor =
            per_ip_governor!(sec.auth_rate_interval_secs, sec.auth_rate_burst, "auth");
        auth_routes = auth_routes.layer(GovernorLayer::new(auth_governor).error_handler(|err| {
            // Auth rate limits are covered by AuthFailure events: every failed auth
            // attempt logs an AuthFailure, so fail2ban bans the IP before the rate
            // limiter trips. No RateLimitHit event needed here; this warn! is for
            // diagnostic post-mortem only.
            warn!(rate_limited = true, "auth rate limit exceeded: {err}");
            err.into_response().map(axum::body::Body::new)
        }));
    } else {
        // Tests: use a reasonable default body limit.
        auth_routes = auth_routes.layer(DefaultBodyLimit::max(4096));
    }

    // Utility routes (no auth, global rate limit only).
    let utility_routes = Router::new()
        .route("/health", get(health))
        .route("/favicon.ico", get(statics::favicon))
        .route("/sw.js", get(statics::service_worker))
        // PWA manifest + manifest-referenced icons. Browsers fetch
        // these anonymously (per Fetch spec, manifest fetches default
        // to crossorigin="anonymous"), so they must not sit behind the
        // session-auth gate. See `statics::manifest` for full rationale.
        // matchit (used by axum 0.8) gives static routes higher priority
        // than wildcards, so these win over the `/static/{*rest}`
        // ServeDir nest below; the carve-out tests in `#[cfg(test)] mod`
        // lock that precedence in.
        .route("/static/manifest.webmanifest", get(statics::manifest))
        .route("/static/icon-192.png", get(statics::icon_192))
        .route("/static/icon-512.png", get(statics::icon_512))
        // nav-on-message.js is loaded on /auth/login and /auth/register (pre-auth
        // pages). Browsers fetch it unauthenticated, so it must live outside
        // require_auth. Cache-Control: no-cache — same rationale as main.js.
        .route_service(
            "/static/nav-on-message.js",
            no_cache(tower_http::services::ServeFile::new(
                state.static_dir.join("nav-on-message.js"),
            )),
        );

    // Per-endpoint inbound webhook routes. Registered only when a WebhookService
    // is configured. Each endpoint gets its own literal mount path (e.g.
    // `/webhooks/phonebuddy`) with a per-endpoint `DefaultBodyLimit` and an
    // `Extension(EndpointSlug(...))` so the shared handler knows which endpoint
    // is being addressed.
    let utility_routes = if let Some(ref webhook_svc) = state.webhook {
        let endpoints: Vec<_> = webhook_svc.all_endpoints().cloned().collect();
        endpoints.iter().fold(utility_routes, |router, ep| {
            let slug = ep.slug.clone();
            let ceiling = ep.transport_ceiling_bytes;
            router.route(
                &ep.mount,
                post(webhooks::inbound::receive).layer(
                    tower::ServiceBuilder::new()
                        .layer(axum::Extension(webhooks::inbound::EndpointSlug(slug)))
                        .layer(DefaultBodyLimit::max(ceiling)),
                ),
            )
        })
    } else {
        utility_routes
    };

    // --- Protected routes (auth required) ---
    // Everything behind auth, including app static assets (JS/CSS).
    let protected_routes = Router::new()
        .route("/", get(app::landing_page))
        .route("/app/{slug}", get(app::app_page))
        .route(
            "/app/{slug}/c/{conversation_id}",
            get(app::app_page_with_conversation),
        )
        .route("/app/{slug}/file/{*path}", get(file::file_view))
        .route(
            "/app/{slug}/mount/{mount_slug}/file/{*path}",
            get(file::mount_file_view),
        )
        .route(
            "/app/{slug}/upload",
            post(upload::upload).layer(
                tower::ServiceBuilder::new()
                    // log_upload_413 observes 413 responses returned from the handler when
                    // the multipart extractor enforces RouteBodyLimit. The 413 originates
                    // from the handler's map_multipart_err helper, not from the layer itself.
                    .layer(axum_mw::from_fn(middleware::log_upload_413))
                    .layer(DefaultBodyLimit::max(
                        security
                            .map(|s| s.upload_body_limit)
                            .unwrap_or(20 * 1024 * 1024),
                    )),
            ),
        )
        .route(
            "/app/{slug}/attachment/{upload_id}/{filename}",
            get(upload::serve_attachment),
        )
        .route("/app/{slug}/ws", get(ws::ws_handler))
        .route("/surface/{slug}", get(surface::page::surface_page))
        .route("/surface/{slug}/ws", get(surface::surface_ws_handler))
        .route("/r/{nonce}", get(redirector::redirect))
        .route("/logout", post(logout::logout))
        .layer(Extension(MaxImageLongEdge(max_image_long_edge)))
        .layer(axum_mw::from_fn_with_state(
            state.clone(),
            middleware::require_auth,
        ));

    // --- Asset routes (auth-gated static assets, own generous governor) ---
    // The auth-gated static-asset tree carries a ~7x request multiplier per
    // surface load, so a synchronized fleet reload would drain the strict global
    // governor and feed fail2ban. These routes get their own generous per-IP
    // bucket instead (see `SecurityConfig::asset_rate_burst`). The surface page
    // HTML and WS upgrade stay under the global governor (they are 1 token each
    // and the abuse-interesting handlers).
    //
    // Cache-busting carve-outs (`NO_CACHE_STATICS`): serve the JS bundle,
    // stylesheet, and surface bootstrap + its stylesheet via tower_http
    // `ServeFile` (streamed body, native `Last-Modified`/`If-Modified-Since` 304
    // support, 404 on missing file) overlaid with `Cache-Control: no-cache` so
    // mobile browsers re-validate on every soft `location.reload()`. The page
    // HTML also bumps `?v=BUILD_ID` on these URLs as belt-and-braces against
    // caches that ignore `no-cache` (the wasm kernel reads the build id from the
    // page meta, so `no-cache` re-validation is the coherence guard for a caller
    // that ignores the query bump). The `nest_service` below still serves
    // anything else under `/static/` (e.g. file-view-copy.js); these explicit
    // routes win via matchit's static-over-wildcard priority.
    let asset_routes = NO_CACHE_STATICS
        .iter()
        .fold(Router::<AppState>::new(), |router, name| {
            router.route_service(
                &format!("/static/{name}"),
                no_cache(tower_http::services::ServeFile::new(
                    state.static_dir.join(name),
                )),
            )
        });
    let asset_routes = asset_routes
        .nest_service(
            "/static",
            tower_http::services::ServeDir::new(&state.static_dir),
        )
        // Surface wasm/JS asset tree. Distinct `/surface-static/` prefix (not
        // `/surface/…`) so no surface slug can ever collide with the asset
        // tree. `no-cache` across the whole tree: `_bg.wasm` fetches resolve
        // relative to the importing module and drop any `?v` query, so
        // conditional revalidation is their only cache-coherence guard.
        // `ServeDir` serves `.wasm` as `application/wasm`, which
        // `WebAssembly.instantiateStreaming` requires.
        .nest_service(
            "/surface-static",
            no_cache(tower_http::services::ServeDir::new(&state.surface_dist_dir)),
        )
        // require_auth INNER, asset governor OUTER (applied last below): an
        // unauthenticated asset request still spends an asset token before the
        // 303, so a 303-flood stays metered and 429-able.
        .layer(axum_mw::from_fn_with_state(
            state.clone(),
            middleware::require_auth,
        ));
    let asset_routes = if let Some(sec) = security {
        let asset_governor =
            per_ip_governor!(sec.asset_rate_interval_secs, sec.asset_rate_burst, "asset");
        asset_routes.layer(
            GovernorLayer::new(asset_governor)
                .error_handler(governor_error_handler(GOVERNOR_MARKER_ASSET)),
        )
    } else {
        // Tests: no governor, matching the global test behavior.
        asset_routes
    };

    // Fallback is on the top-level router so it fires for ALL unmatched URLs,
    // regardless of auth state. This ensures unauthenticated URL probing
    // generates fail2ban signal (not just authenticated 404s).
    //
    // The global governor wraps the `governed` sub-router (auth + utility +
    // protected + fallback). The asset governor is already on `asset_routes`.
    // Merging composes them into one route tree; each merged sub-router keeps
    // its own layers, so the two governors stay independent per route class.
    let mut governed = Router::new()
        .merge(auth_routes)
        .merge(utility_routes)
        .merge(protected_routes)
        .fallback(not_found);
    if let Some(sec) = security {
        // Global: rate limited per real client IP via `ClientIpKeyExtractor`.
        let global_governor = per_ip_governor!(
            sec.global_rate_interval_secs,
            sec.global_rate_burst,
            "global"
        );
        governed = governed.layer(
            GovernorLayer::new(global_governor)
                .error_handler(governor_error_handler(GOVERNOR_MARKER_GLOBAL)),
        );
    }

    // Layer ordering (axum semantics: the *last* `.layer(...)` call is
    // OUTERMOST, i.e. runs first on every incoming request):
    //
    //   request flow → Extension(TrustedProxyHops)
    //                → resolve_client_ip   (inserts ClientIp extension)
    //                → detect_rate_limit   (logs RateLimitHit on either governor's 429)
    //                → body_limit
    //                → apply_relaxed_csp / helmet / catch_panic
    //                → [governed | asset_routes], each with its own governor
    //                → handler
    //
    // Load-bearing:
    //   - `ClientIpKeyExtractor` (both governors) reads `ClientIp`, so
    //     `resolve_client_ip` MUST run before either governor sees the request.
    //   - `detect_rate_limit` MUST be outer of both governors (to see their 429
    //     + marker) and inner of `resolve_client_ip` (to read the IP).
    //   - `DefaultBodyLimit` sits outer of both governors; that is inert — it
    //     only sets the limit extension that body extractors consult, consuming
    //     nothing at admission.
    //   - `catch_panic`/`helmet`/`apply_relaxed_csp` sit OUTER of both governors,
    //     so governor 429s carry helmet security headers. Intentional.
    let mut router = Router::new()
        .merge(governed)
        .merge(asset_routes)
        .with_state(state)
        // Innermost first — last `.layer(...)` call is outermost.
        // CatchPanicLayer converts handler panics (e.g. WASM trap propagated
        // via resume_unwind) into 500 responses with a tracing::error! log.
        // Layered inside helmet; outside the route handlers so it catches all
        // handler panics. Note: no TraceLayer in this router, so panic logs are
        // not correlated with a request-id span.
        .layer(tower_http::catch_panic::CatchPanicLayer::new())
        .layer(helmet_layer())
        // Outer of helmet: swaps in the wasm-relaxed CSP for responses that
        // opted in via the `RelaxedWasmCsp` extension (surface HTML only).
        .layer(axum_mw::from_fn(apply_relaxed_csp));

    if let Some(sec) = security {
        // Use Axum's DefaultBodyLimit (not tower-http's RequestBodyLimitLayer)
        // so that per-route overrides (e.g., 20MB on the upload endpoint) work.
        // RequestBodyLimitLayer is a tower layer that fires before Axum's
        // extractors and ignores route-level DefaultBodyLimit overrides. Applied
        // at top level so asset routes stay body-limited too.
        router = router.layer(DefaultBodyLimit::max(sec.global_body_limit));
    } else {
        // Tests: use a reasonable default body limit.
        router = router.layer(DefaultBodyLimit::max(1024 * 1024));
    }

    // One rate-limit security logger for both governors. Runs inside
    // `resolve_client_ip` (ClientIp already in extensions) and outside both
    // governors (sees the 429 + marker header on the response path); it is
    // marker-based, so it composes over the global and asset governors and is an
    // inert passthrough when no governor exists (tests).
    router = router.layer(axum_mw::from_fn(detect_rate_limit));

    router
        .layer(axum_mw::from_fn(client_ip::resolve_client_ip))
        .layer(Extension(client_ip::TrustedProxyHops(trusted_proxy_hops)))
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;

    use axum::body::Body;
    use axum::extract::connect_info::MockConnectInfo;
    use axum::http::{Request, StatusCode};
    use brenn_lib::auth::invite::create_invite_code;
    use brenn_lib::auth::password::hash_password;
    use brenn_lib::auth::user::create_user;
    use brenn_lib::config::SecurityConfig;
    use brenn_lib::db;
    use indexmap::IndexMap;
    use tower::ServiceExt;

    use crate::test_support::app_config::default_test_app_config;
    use crate::test_support::http::{
        auth_login_status, body_string, extract_session_token, fetch_landing_page, get_set_cookie,
        multipart_body, setup_authenticated_user, xff_get_status,
    };
    use crate::test_support::state::{
        landing_page_router_two_apps, test_app, test_app_for_access_control, test_app_security,
        test_app_with_apps, test_app_with_static_dir, test_app_with_surface_dist_dir, test_state,
    };

    use super::*;

    // -----------------------------------------------------------------------
    // Health check
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn health_returns_ok() {
        let (app, _db) = test_app();
        let response = app
            .oneshot(Request::get("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_string(response.into_body()).await, "ok");
    }

    /// The redirector route must be behind `require_auth`.
    /// Without a session cookie, the middleware 303s to `/auth/login`.
    /// A 303 (not 404) proves the route is registered behind the auth
    /// middleware — not absent from the router entirely.
    #[tokio::test]
    async fn redirector_requires_auth() {
        let (app, _db) = test_app();
        let response = app
            .oneshot(
                Request::get("/r/550e8400-e29b-41d4-a716-446655440000?to=/app/graf/c/42")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        let location = response
            .headers()
            .get("location")
            .expect("303 must carry Location header")
            .to_str()
            .unwrap();
        assert_eq!(location, "/auth/login");
    }

    /// Regression guard: the success-path 303 survives the full middleware stack
    /// (including `helmet_layer`). The unit tests in `redirector.rs` use a
    /// minimal router that bypasses `helmet_layer`; this test covers wiring.
    #[tokio::test]
    async fn redirector_authenticated_303s_to_target() {
        let (app, db) = test_app();
        let (session_token, _csrf) = setup_authenticated_user(&db).await;
        let response = app
            .oneshot(
                Request::get("/r/550e8400-e29b-41d4-a716-446655440000?to=/app/graf/c/42")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        let location = response
            .headers()
            .get("location")
            .expect("303 must carry Location header")
            .to_str()
            .unwrap();
        assert_eq!(location, "/app/graf/c/42");
        assert!(
            body_string(response.into_body()).await.is_empty(),
            "303 body must be empty"
        );
    }

    /// The fixed `/webhooks/git` route is retired; a POST to it now falls
    /// through to the global 404 handler (which also emits a fail2ban-lane
    /// security event). Forges must be repointed at the per-forge endpoints.
    #[tokio::test]
    async fn retired_webhooks_git_route_returns_404() {
        let (app, _db) = test_app();
        let response = app
            .oneshot(
                Request::post("/webhooks/git")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    /// Router that accepts every delivery. Lets a fully-verified inbound
    /// webhook reach 204 without standing up the real messaging fan-out.
    struct AlwaysOkRouter;

    #[async_trait::async_trait]
    impl brenn_lib::webhook::service::WebhookEventRouter for AlwaysOkRouter {
        async fn deliver_inbound(
            &self,
            _endpoint_slug: &str,
            _owner: &brenn_lib::webhook::config::WebhookOwner,
            _key_id: &str,
            _headers: axum::http::HeaderMap,
            _client_ip: std::net::IpAddr,
            _received_at: std::time::SystemTime,
            _raw_body: String,
            _urgency: brenn_lib::messaging::Urgency,
        ) -> Result<(), String> {
            Ok(())
        }
    }

    /// An operator-declared `[[webhook_endpoint]]` may mount at `/webhooks/git`:
    /// it registers in `build_router` with no route collision, and its POST
    /// resolves and serves through the generic inbound handler — a verified
    /// request reaches 204.
    #[tokio::test]
    async fn user_endpoint_mounted_at_webhooks_git_resolves_and_serves() {
        use brenn_lib::webhook::config::{ResolvedWebhookEndpoint, WebhookOwner};
        use brenn_lib::webhook::service::WebhookService;
        use brenn_lib::webhook::signature::{
            HexFormat, SignatureAlgorithm, SignatureScheme, hmac_sha256_hex,
        };

        const SECRET: &[u8] = b"collision-acceptance-secret";
        const MOUNT: &str = "/webhooks/git";
        let mut keys = std::collections::HashMap::new();
        keys.insert("forgejo".to_string(), SECRET.to_vec());
        let endpoint = Arc::new(ResolvedWebhookEndpoint {
            slug: "git-forgejo".to_string(),
            mount: MOUNT.to_string(),
            description: None,
            transport_ceiling_bytes: 65536,
            content_type: "application/json".to_string(),
            scheme: SignatureScheme::HmacRawBody {
                algorithm: SignatureAlgorithm::HmacSha256,
                header: "x-gitea-signature".parse().unwrap(),
                format: HexFormat::Hex,
                key_id_header: None,
                keys,
            },
            owner: WebhookOwner::Wasm(Arc::from("git-forge-parser")),
            urgency: brenn_lib::messaging::Urgency::Normal,
            replay_protection: None,
        });
        let svc = WebhookService::new(vec![("git-forgejo".to_string(), endpoint)]);
        svc.set_router(Arc::new(AlwaysOkRouter));

        let db = db::init_db_memory();
        let mut state = test_state(&db);
        state.webhook = Some(svc);
        // build_router registers the endpoint's mount in the dynamic per-endpoint
        // loop; a surviving fixed `/webhooks/git` route would panic here on the
        // duplicate path. Reaching past this call is itself part of the proof.
        let app = build_router(state, None, 0, 2576)
            .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))));

        let body = br#"{"repository":{"ssh_url":"ssh://git@f/x.git"}}"#.to_vec();
        let signature = hmac_sha256_hex(SECRET, &body);
        let response = app
            .oneshot(
                Request::post(MOUNT)
                    .header("content-type", "application/json")
                    .header("x-gitea-signature", signature)
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::NO_CONTENT,
            "user endpoint at /webhooks/git must resolve and serve (collision retired)"
        );
    }

    // --- Security headers ---

    #[tokio::test]
    async fn security_headers_present_on_all_responses() {
        let (app, _db) = test_app();
        let response = app
            .oneshot(Request::get("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();

        let headers = response.headers();
        let csp = headers
            .get("content-security-policy")
            .expect("missing CSP header")
            .to_str()
            .unwrap();
        // style-src must be locked to 'self' with no 'unsafe-inline': the whole
        // point of dropping inkjet highlighting was to let us tighten this.
        // A future regression that re-adds inline styles must not pass tests.
        assert!(
            csp.contains("style-src 'self'"),
            "CSP must pin style-src to 'self', got: {csp}"
        );
        assert!(
            !csp.contains("unsafe-inline"),
            "CSP must not contain 'unsafe-inline', got: {csp}"
        );
        // The wasm relaxation is scoped to surface HTML documents only; a
        // non-surface route must keep the strict policy byte-for-byte.
        assert!(
            !csp.contains("wasm-unsafe-eval"),
            "non-surface CSP must not contain 'wasm-unsafe-eval', got: {csp}"
        );
        assert!(
            headers.contains_key("x-frame-options"),
            "missing X-Frame-Options"
        );
        assert!(
            headers.contains_key("x-content-type-options"),
            "missing X-Content-Type-Options"
        );
        assert!(
            headers.contains_key("referrer-policy"),
            "missing Referrer-Policy"
        );
        assert!(
            headers.contains_key("strict-transport-security"),
            "missing HSTS"
        );
        assert!(
            headers.contains_key("x-xss-protection"),
            "missing X-XSS-Protection"
        );

        // Verify specific values.
        assert_eq!(
            headers.get("x-frame-options").unwrap().to_str().unwrap(),
            "DENY"
        );
        assert_eq!(
            headers
                .get("x-content-type-options")
                .unwrap()
                .to_str()
                .unwrap(),
            "nosniff"
        );
        assert_eq!(
            headers.get("x-xss-protection").unwrap().to_str().unwrap(),
            "0"
        );
    }

    // --- Login page ---

    #[tokio::test]
    async fn login_page_renders() {
        let (app, _db) = test_app();
        let response = app
            .oneshot(Request::get("/auth/login").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_string(response.into_body()).await;
        assert!(body.contains("<form"), "should contain a form");
        assert!(
            body.contains("action=\"/auth/login\""),
            "form should POST to /login"
        );
    }

    #[tokio::test]
    async fn login_page_shows_error_with_query_param() {
        let (app, _db) = test_app();
        let response = app
            .oneshot(
                Request::get("/auth/login?error=1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = body_string(response.into_body()).await;
        assert!(
            body.contains("Invalid username or password"),
            "should show error message"
        );
    }

    // --- Login authentication ---

    #[tokio::test]
    async fn login_success_sets_session_cookie() {
        let (app, db) = test_app();

        // Create a user directly in the DB.
        {
            let conn = db.lock().await;
            let hash = hash_password(b"correct-password1");
            create_user(&conn, "alice", &hash);
        }

        let response = app
            .oneshot(
                Request::post("/auth/login")
                    .header("content-type", "application/x-www-form-urlencoded")
                    .body(Body::from("username=alice&password=correct-password1"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            response
                .headers()
                .get("location")
                .unwrap()
                .to_str()
                .unwrap(),
            "/"
        );
        let cookie = get_set_cookie(&response).expect("should set session cookie");
        assert!(cookie.starts_with("brenn_session="));
        assert!(cookie.contains("HttpOnly"));
        assert!(cookie.contains("SameSite=Lax"));
    }

    #[tokio::test]
    async fn login_failure_redirects_with_error() {
        let (app, db) = test_app();

        {
            let conn = db.lock().await;
            let hash = hash_password(b"correct-password1");
            create_user(&conn, "alice", &hash);
        }

        let response = app
            .oneshot(
                Request::post("/auth/login")
                    .header("content-type", "application/x-www-form-urlencoded")
                    .body(Body::from("username=alice&password=wrong-password11"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            response
                .headers()
                .get("location")
                .unwrap()
                .to_str()
                .unwrap(),
            "/auth/login?error=1"
        );
    }

    #[tokio::test]
    async fn login_nonexistent_user_redirects_with_error() {
        let (app, _db) = test_app();

        let response = app
            .oneshot(
                Request::post("/auth/login")
                    .header("content-type", "application/x-www-form-urlencoded")
                    .body(Body::from("username=nobody&password=doesnt-matter1"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            response
                .headers()
                .get("location")
                .unwrap()
                .to_str()
                .unwrap(),
            "/auth/login?error=1"
        );
    }

    // --- Registration ---

    #[tokio::test]
    async fn register_page_404_without_invite_codes() {
        let (app, _db) = test_app();
        let response = app
            .oneshot(Request::get("/auth/register").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn register_page_renders_with_invite_code() {
        let (app, db) = test_app();

        {
            let conn = db.lock().await;
            create_invite_code(&conn);
        }

        let response = app
            .oneshot(Request::get("/auth/register").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_string(response.into_body()).await;
        assert!(body.contains("action=\"/auth/register\""));
    }

    #[tokio::test]
    async fn register_success_creates_user_and_logs_in() {
        let (app, db) = test_app();

        let code = {
            let conn = db.lock().await;
            create_invite_code(&conn)
        };

        let body = format!("username=newuser&password=strong-password-12&invite_code={code}");

        let response = app
            .oneshot(
                Request::post("/auth/register")
                    .header("content-type", "application/x-www-form-urlencoded")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            response
                .headers()
                .get("location")
                .unwrap()
                .to_str()
                .unwrap(),
            "/"
        );
        let cookie = get_set_cookie(&response).expect("should set session cookie");
        assert!(cookie.starts_with("brenn_session="));
    }

    #[tokio::test]
    async fn register_invalid_invite_code_rejected() {
        let (app, db) = test_app();

        // Need at least one invite code to not get 404 on GET, but use a wrong one for POST.
        {
            let conn = db.lock().await;
            create_invite_code(&conn);
        }

        let response = app
            .oneshot(
                Request::post("/auth/register")
                    .header("content-type", "application/x-www-form-urlencoded")
                    .body(Body::from(
                        "username=newuser&password=strong-password-12&invite_code=bogus",
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            response
                .headers()
                .get("location")
                .unwrap()
                .to_str()
                .unwrap(),
            "/auth/register?error=invite"
        );
    }

    #[tokio::test]
    async fn register_short_password_rejected() {
        let (app, db) = test_app();

        let code = {
            let conn = db.lock().await;
            create_invite_code(&conn)
        };

        let body = format!("username=newuser&password=short&invite_code={code}");

        let response = app
            .oneshot(
                Request::post("/auth/register")
                    .header("content-type", "application/x-www-form-urlencoded")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            response
                .headers()
                .get("location")
                .unwrap()
                .to_str()
                .unwrap(),
            "/auth/register?error=password"
        );
    }

    // --- Auth middleware ---

    #[tokio::test]
    async fn unauthenticated_request_redirects_to_login() {
        // Use a real protected route — `/` requires auth.
        let (app, _db) = test_app();
        let response = app
            .oneshot(Request::get("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            response
                .headers()
                .get("location")
                .unwrap()
                .to_str()
                .unwrap(),
            "/auth/login"
        );
    }

    #[tokio::test]
    async fn invalid_session_cookie_redirects_to_login() {
        // Use a real protected route — `/` requires auth.
        let (app, _db) = test_app();
        let response = app
            .oneshot(
                Request::get("/")
                    .header("cookie", "brenn_session=bogus-token-value")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            response
                .headers()
                .get("location")
                .unwrap()
                .to_str()
                .unwrap(),
            "/auth/login"
        );
    }

    #[tokio::test]
    async fn unrecognized_url_returns_404() {
        // Unmatched URLs get 404 regardless of auth state — the fallback
        // is at the top level, before auth middleware.
        let (app, _db) = test_app();
        let response = app
            .oneshot(
                Request::get("/some-bogus-page")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // --- Landing page ---

    #[tokio::test]
    async fn landing_page_redirects_for_single_app() {
        let (app, db) = test_app();
        let (session_token, _csrf_token) = setup_authenticated_user(&db).await;

        let response = app
            .oneshot(
                Request::get("/")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            response
                .headers()
                .get("location")
                .unwrap()
                .to_str()
                .unwrap(),
            "/app/test"
        );
    }

    #[tokio::test]
    async fn landing_page_lists_apps_for_multi_app() {
        let mut apps = IndexMap::new();
        apps.insert(
            "alpha".to_string(),
            default_test_app_config("alpha", "Alpha App"),
        );
        apps.insert(
            "beta".to_string(),
            default_test_app_config("beta", "Beta App"),
        );
        let (app, db) = test_app_with_apps(Arc::new(apps));
        let (session_token, _csrf_token) = setup_authenticated_user(&db).await;
        let response = app
            .oneshot(
                Request::get("/")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = body_string(response.into_body()).await;
        assert!(body.contains("Alpha App"), "should list alpha app");
        assert!(body.contains("Beta App"), "should list beta app");
        assert!(body.contains("/app/alpha"), "should have link to alpha app");
        assert!(body.contains("/app/beta"), "should have link to beta app");
    }

    // --- App page ---

    #[tokio::test]
    async fn app_page_renders_for_authenticated_user() {
        let (app, db) = test_app();
        let (session_token, csrf_token) = setup_authenticated_user(&db).await;

        let response = app
            .oneshot(
                Request::get("/app/test")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = body_string(response.into_body()).await;
        // username is now delivered via Welcome WS message, not static HTML
        assert!(
            body.contains(&format!("content=\"{csrf_token}\"")),
            "should contain CSRF token in meta tag"
        );
        assert!(
            body.contains("name=\"app-slug\" content=\"test\""),
            "should have app-slug meta tag"
        );
        assert!(
            body.contains("<title>Brenn — Test App</title>"),
            "should have app name in title"
        );
        assert!(
            body.contains("name=\"initial-conversation-id\" content=\"\""),
            "app_page should have empty initial-conversation-id meta tag"
        );
    }

    #[tokio::test]
    async fn app_page_with_conversation_renders_with_id() {
        let (app, db) = test_app();
        let (session_token, _) = setup_authenticated_user(&db).await;

        let response = app
            .oneshot(
                Request::get("/app/test/c/42")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = body_string(response.into_body()).await;
        assert!(
            body.contains("name=\"initial-conversation-id\" content=\"42\""),
            "should have conversation ID in meta tag"
        );
        assert!(
            body.contains("name=\"app-slug\" content=\"test\""),
            "should have app-slug meta tag"
        );
    }

    #[tokio::test]
    async fn app_page_conversation_non_integer_returns_400() {
        let (app, db) = test_app();
        let (session_token, _) = setup_authenticated_user(&db).await;

        let response = app
            .oneshot(
                Request::get("/app/test/c/notanumber")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn app_page_unrecognized_subpath_returns_404() {
        let (app, db) = test_app();
        let (session_token, _) = setup_authenticated_user(&db).await;

        let response = app
            .oneshot(
                Request::get("/app/test/garbage")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn app_page_unknown_slug_returns_404() {
        let (app, db) = test_app();
        let (session_token, _) = setup_authenticated_user(&db).await;

        let response = app
            .oneshot(
                Request::get("/app/nonexistent")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn app_page_access_denied_returns_403() {
        let (app, db) = test_app_for_access_control("restricted", vec!["otheruser".to_string()]);
        let (session_token, _) = setup_authenticated_user(&db).await;
        let response = app
            .oneshot(
                Request::get("/app/restricted")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn app_page_redirects_unauthenticated() {
        let (app, _db) = test_app();
        let response = app
            .oneshot(Request::get("/app/test").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            response
                .headers()
                .get("location")
                .unwrap()
                .to_str()
                .unwrap(),
            "/auth/login"
        );
    }

    #[tokio::test]
    async fn logout_from_app_page_works() {
        // Simulates the real browser flow: load app page, extract CSRF, POST logout.
        let db = db::init_db_memory();
        let mock_addr = MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999)));
        let (session_token, _) = setup_authenticated_user(&db).await;

        // Step 1: Load app page and extract CSRF token from HTML.
        let state = test_state(&db);
        let app = build_router(state, None, 0, 2576).layer(mock_addr);
        let response = app
            .oneshot(
                Request::get("/app/test")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = body_string(response.into_body()).await;
        // Extract CSRF token from: <meta name="csrf-token" content="<token>">
        let csrf_token = body
            .split("name=\"csrf-token\" content=\"")
            .nth(1)
            .unwrap()
            .split('"')
            .next()
            .unwrap();

        // Step 2: POST logout with extracted CSRF token.
        let state = test_state(&db);
        let app = build_router(state, None, 0, 2576).layer(mock_addr);
        let response = app
            .oneshot(
                Request::post("/logout")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .header("content-type", "application/x-www-form-urlencoded")
                    .body(Body::from(format!("csrf_token={csrf_token}")))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            response
                .headers()
                .get("location")
                .unwrap()
                .to_str()
                .unwrap(),
            "/auth/login"
        );
        let cookie = get_set_cookie(&response).unwrap();
        assert!(cookie.contains("Max-Age=0"), "cookie should be cleared");
    }

    // --- Logout ---

    #[tokio::test]
    async fn logout_with_valid_csrf_destroys_session() {
        let (app, db) = test_app();

        let (session_token, csrf_token) = setup_authenticated_user(&db).await;

        let body = format!("csrf_token={csrf_token}");

        let response = app
            .oneshot(
                Request::post("/logout")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .header("content-type", "application/x-www-form-urlencoded")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            response
                .headers()
                .get("location")
                .unwrap()
                .to_str()
                .unwrap(),
            "/auth/login"
        );

        // Cookie should be cleared.
        let cookie = get_set_cookie(&response).unwrap();
        assert!(cookie.contains("Max-Age=0"), "cookie should be cleared");

        // Session should be gone from DB.
        let conn = db.lock().await;
        let session = brenn_lib::auth::session::validate_session(&conn, &session_token);
        assert!(session.is_none(), "session should be deleted from DB");
    }

    #[tokio::test]
    async fn logout_with_wrong_csrf_returns_403() {
        let (app, db) = test_app();

        let (session_token, _csrf_token) = setup_authenticated_user(&db).await;

        let response = app
            .oneshot(
                Request::post("/logout")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .header("content-type", "application/x-www-form-urlencoded")
                    .body(Body::from("csrf_token=wrong-csrf-token"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    // --- Static assets ---

    #[tokio::test]
    async fn auth_css_served() {
        let (app, _db) = test_app();
        let response = app
            .oneshot(
                Request::get("/auth/static/auth.css")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .unwrap()
                .to_str()
                .unwrap(),
            "text/css"
        );
    }

    // PWA manifest + icons: served pre-auth.

    #[tokio::test]
    async fn manifest_served_without_auth() {
        let (app, _db) = test_app();
        let response = app
            .oneshot(
                Request::get("/static/manifest.webmanifest")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response.headers().get("location").is_none(),
            "manifest must not redirect to /auth/login"
        );
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .unwrap()
                .to_str()
                .unwrap(),
            "application/manifest+json"
        );
        let body = body_string(response.into_body()).await;
        assert!(!body.is_empty(), "manifest body should not be empty");
        assert!(
            body.contains("\"name\""),
            "manifest should look like JSON: {body}"
        );
    }

    #[tokio::test]
    async fn icon_192_served_without_auth() {
        let (app, _db) = test_app();
        let response = app
            .oneshot(
                Request::get("/static/icon-192.png")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get("location").is_none());
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .unwrap()
                .to_str()
                .unwrap(),
            "image/png"
        );
    }

    #[tokio::test]
    async fn icon_512_served_without_auth() {
        let (app, _db) = test_app();
        let response = app
            .oneshot(
                Request::get("/static/icon-512.png")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get("location").is_none());
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .unwrap()
                .to_str()
                .unwrap(),
            "image/png"
        );
    }

    #[tokio::test]
    async fn manifest_cache_control_present() {
        // Lock in the moderate-freshness Cache-Control. Without it,
        // background-tab manifest revalidations would re-hit the server
        // on every wake.
        let (app, _db) = test_app();
        let response = app
            .oneshot(
                Request::get("/static/manifest.webmanifest")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let cc = response
            .headers()
            .get("cache-control")
            .expect("manifest should set Cache-Control")
            .to_str()
            .unwrap();
        assert!(
            cc.contains("max-age="),
            "Cache-Control should set max-age, got: {cc}"
        );
    }

    #[tokio::test]
    async fn static_main_js_still_requires_auth() {
        // Locks in the matchit static-over-wildcard precedence: only the
        // three carve-outs above are exposed pre-auth. Anything else in
        // /static/ (like main.js) still hits `require_auth` and 303s.
        let (app, _db) = test_app();
        let response = app
            .oneshot(Request::get("/static/main.js").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            response
                .headers()
                .get("location")
                .unwrap()
                .to_str()
                .unwrap(),
            "/auth/login"
        );
    }

    // Cache-busting for /static/main.js + /static/app.css.

    #[tokio::test]
    async fn app_shell_references_main_js_with_build_id() {
        let (app, db) = test_app();
        let (session_token, _csrf) = setup_authenticated_user(&db).await;
        let response = app
            .oneshot(
                Request::get("/app/test")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_string(response.into_body()).await;
        let needle = format!(
            "src=\"/static/main.js?v={}\"",
            crate::test_support::TEST_BUILD_ID
        );
        assert!(
            body.contains(&needle),
            "expected {needle:?} in app shell, got: {body}"
        );
    }

    #[tokio::test]
    async fn app_shell_references_app_css_with_build_id() {
        let (app, db) = test_app();
        let (session_token, _csrf) = setup_authenticated_user(&db).await;
        let response = app
            .oneshot(
                Request::get("/app/test")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_string(response.into_body()).await;
        let needle = format!(
            "href=\"/static/app.css?v={}\"",
            crate::test_support::TEST_BUILD_ID
        );
        assert!(
            body.contains(&needle),
            "expected {needle:?} in app shell, got: {body}"
        );
    }

    #[tokio::test]
    async fn app_shell_has_no_store_cache_header() {
        let (app, db) = test_app();
        let (session_token, _csrf) = setup_authenticated_user(&db).await;
        let response = app
            .oneshot(
                Request::get("/app/test")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let cc = response
            .headers()
            .get("cache-control")
            .expect("app shell should set Cache-Control")
            .to_str()
            .unwrap();
        assert_eq!(
            cc, "no-store",
            "app shell HTML must be `no-store`, got: {cc}"
        );
    }

    /// Shared assertions for the `serve_no_cache_file` routes: 200, the
    /// `Cache-Control: no-cache` contract they all share, the expected
    /// content-type, and `Last-Modified` (required by the `If-Modified-Since`
    /// / 304 conditional-GET cycle the design promises). `session` is `None`
    /// for pre-auth routes served outside `require_auth`.
    async fn assert_no_cache_static(
        app: Router,
        path: &str,
        expected_content_type: &str,
        session: Option<&str>,
    ) {
        let mut req = Request::get(path);
        if let Some(token) = session {
            req = req.header("cookie", format!("brenn_session={token}"));
        }
        let response = app.oneshot(req.body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "{path} should return 200"
        );
        assert_eq!(
            response
                .headers()
                .get("cache-control")
                .unwrap_or_else(|| panic!("{path} should set Cache-Control"))
                .to_str()
                .unwrap(),
            "no-cache",
            "{path} must be no-cache"
        );
        // ServeFile guesses the content-type via mime_guess; for `.js` it
        // returns "text/javascript" (the spec-current value per RFC 9239).
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .unwrap()
                .to_str()
                .unwrap(),
            expected_content_type,
            "{path} content-type"
        );
        assert!(
            response.headers().contains_key("last-modified"),
            "{path} ServeFile should emit Last-Modified for conditional GETs"
        );
    }

    /// `/surface-static/*` serves the surface asset tree with `no-cache`, the
    /// correct `application/wasm` MIME (load-bearing for
    /// `WebAssembly.instantiateStreaming`), 404s missing files, and sits behind
    /// `require_auth` like the rest of `protected_routes`.
    #[tokio::test]
    async fn surface_static_serves_assets_no_cache_and_authed() {
        let (app, db, _tmp) = test_app_with_surface_dist_dir(&[
            ("brenn_surface_kernel.js", b"export {};"),
            ("brenn_surface_kernel_bg.wasm", b"\0asm\x01\0\0\0"),
        ]);
        let (session_token, _) = setup_authenticated_user(&db).await;

        // (a) real JS file: 200 + no-cache.
        assert_no_cache_static(
            app.clone(),
            "/surface-static/brenn_surface_kernel.js",
            "text/javascript",
            Some(&session_token),
        )
        .await;

        // (b) `.wasm` served as application/wasm.
        assert_no_cache_static(
            app.clone(),
            "/surface-static/brenn_surface_kernel_bg.wasm",
            "application/wasm",
            Some(&session_token),
        )
        .await;

        // (c) missing file → 404.
        let missing = app
            .clone()
            .oneshot(
                Request::get("/surface-static/nope.js")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);

        // (d) no session cookie → require_auth redirects (303), proving the
        //     route lives inside protected_routes.
        let unauthed = app
            .oneshot(
                Request::get("/surface-static/brenn_surface_kernel.js")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauthed.status(), StatusCode::SEE_OTHER);
    }

    /// Every `NO_CACHE_STATICS` carve-out (main.js/app.css/surface.js/surface.css)
    /// serves 200 + `no-cache` + `Last-Modified` with its extension-derived
    /// content-type, behind `require_auth`. Loops the same list the router folds
    /// over, so a newly-added entry is covered without a new test — and a fifth
    /// entry that silently missed the fold would fail here.
    #[tokio::test]
    async fn no_cache_statics_all_serve_no_cache_header() {
        for &name in super::NO_CACHE_STATICS {
            let content_type = if name.ends_with(".css") {
                "text/css"
            } else {
                "text/javascript"
            };
            let (app, db, _tmp) = test_app_with_static_dir(&[(name, b"x")]);
            let (session_token, _) = setup_authenticated_user(&db).await;
            assert_no_cache_static(
                app,
                &format!("/static/{name}"),
                content_type,
                Some(&session_token),
            )
            .await;
        }
    }

    #[tokio::test]
    async fn static_nav_on_message_js_has_no_cache_header() {
        // nav-on-message.js lives in `utility_routes`, outside `require_auth`:
        // it's fetched by the pre-auth login/register pages. No session cookie.
        let (app, _db, _tmp) = test_app_with_static_dir(&[("nav-on-message.js", b"export {};")]);
        assert_no_cache_static(app, "/static/nav-on-message.js", "text/javascript", None).await;
    }

    /// `/static/main.js` returns 304 Not Modified when the client sends
    /// `If-Modified-Since` matching `Last-Modified`. Locks in the
    /// "browser gets a 304 most of the time" contract from the design
    /// (`design.md:147-148`).
    #[tokio::test]
    async fn static_main_js_returns_304_on_if_modified_since() {
        let (app, db, _tmp) = test_app_with_static_dir(&[("main.js", b"export {};")]);
        let (session_token, _) = setup_authenticated_user(&db).await;

        // First request: capture Last-Modified.
        let resp1 = app
            .clone()
            .oneshot(
                Request::get("/static/main.js")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp1.status(), StatusCode::OK);
        let last_modified = resp1
            .headers()
            .get("last-modified")
            .expect("Last-Modified header missing")
            .clone();

        // Second request: conditional GET with If-Modified-Since.
        let resp2 = app
            .oneshot(
                Request::get("/static/main.js")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .header("if-modified-since", &last_modified)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp2.status(), StatusCode::NOT_MODIFIED);
    }

    /// `/static/main.js` returns 404 when the file is missing — proves we
    /// no longer regress the prior `ServeDir` behavior to a hand-rolled
    /// 500. (Pre-fix the handler did `tokio::fs::read` and 500'd on
    /// `Err`; `ServeFile` 404s.)
    #[tokio::test]
    async fn static_main_js_missing_returns_404() {
        // Empty static_dir: no main.js on disk.
        let (app, db, _tmp) = test_app_with_static_dir(&[]);
        let (session_token, _) = setup_authenticated_user(&db).await;
        let response = app
            .oneshot(
                Request::get("/static/main.js")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // Rate-limiter keys on resolved client IP, not proxy peer.

    #[tokio::test]
    async fn rate_limit_keyed_per_real_client_ip_not_proxy_peer() {
        // Tight bucket: burst=2, refill every 600s. After 2 requests
        // from `1.2.3.4` the bucket is empty for ~10 minutes; the 3rd
        // from `.4` should be 429. A request from `5.6.7.8` should
        // still succeed (200 in this case — the login page renders).
        let sec = SecurityConfig {
            auth_rate_burst: 2,
            auth_rate_interval_secs: 600,
            ..Default::default()
        };
        let (app, _db) = test_app_security(&sec);

        // Drain `.4`'s bucket.
        let s1 = auth_login_status(&app, "1.2.3.4").await;
        assert_eq!(s1, StatusCode::OK, "first .4 request should succeed");
        let s2 = auth_login_status(&app, "1.2.3.4").await;
        assert_eq!(s2, StatusCode::OK, "second .4 request should succeed");

        // Third .4 request: bucket empty, expect 429.
        let s3 = auth_login_status(&app, "1.2.3.4").await;
        assert_eq!(
            s3,
            StatusCode::TOO_MANY_REQUESTS,
            "third .4 request should be rate-limited (bucket empty)"
        );

        // Fresh IP: should NOT be rate-limited by `.4`'s drain. With
        // the default PeerIpKeyExtractor the proxy peer would be
        // `127.0.0.1` for both and this would 429 too.
        let s_other = auth_login_status(&app, "5.6.7.8").await;
        assert_eq!(
            s_other,
            StatusCode::OK,
            "fresh client IP must not inherit another IP's rate-limit drain"
        );
    }

    /// Global rate-limit rejections must emit a `RateLimitHit` security event.
    ///
    /// Uses a tight global governor (burst=2) so the third request triggers a 429.
    /// Asserts that:
    /// 1. The response is 429.
    /// 2. The `x-brenn-governor` marker header is NOT present on the client response
    ///    (the `detect_rate_limit` middleware must strip it).
    /// 3. A `security_event=true` + `rate_limit_hit` tracing event was emitted with
    ///    the correct client IP — verifies the full chain from XFF header to security log.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn global_rate_limit_emits_security_event() {
        let sec = SecurityConfig {
            global_rate_burst: 2,
            global_rate_interval_secs: 600,
            ..Default::default()
        };
        let (app, _db) = test_app_security(&sec);

        // Drain the bucket for this IP.
        let s1 = global_health_status(&app, "1.2.3.4").await;
        assert_eq!(s1, StatusCode::OK, "first request should succeed");
        let s2 = global_health_status(&app, "1.2.3.4").await;
        assert_eq!(s2, StatusCode::OK, "second request should succeed");

        // Third request: bucket empty, expect 429.
        // Can't use global_health_status here — we need the full response for
        // status, header, and body assertions.
        let req = Request::get("/health")
            .header("x-forwarded-for", "1.2.3.4")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "third request must be rate-limited"
        );

        // The marker header must be stripped before the response reaches the client.
        assert!(
            response.headers().get("x-brenn-governor").is_none(),
            "x-brenn-governor marker header must be stripped from client response"
        );

        // Security event must have been emitted with the correct IP.
        assert!(
            logs_contain("security_event=true"),
            "global rate limit must emit security_event=true"
        );
        assert!(
            logs_contain("rate_limit_hit"),
            "global rate limit must emit event_type=rate_limit_hit"
        );
        assert!(
            logs_contain("1.2.3.4"),
            "security event must log the client IP resolved from XFF header"
        );
    }

    /// A handler-produced 429 (without the `x-brenn-governor` marker) must NOT
    /// emit a `RateLimitHit` security event.
    ///
    /// This exercises the marker-header discrimination in `detect_rate_limit`:
    /// the middleware should only log for governor-produced 429s, not arbitrary
    /// handler 429s (e.g. `ReplayCapHit` in `webhooks/inbound.rs`).
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn handler_429_without_marker_does_not_emit_rate_limit_event() {
        use std::convert::Infallible;
        use tower::ServiceExt;
        use tower::service_fn;

        // Inner service returns a plain 429 with no GOVERNOR_MARKER_HEADER.
        let inner = service_fn(|_req: Request<Body>| async {
            Ok::<_, Infallible>(
                Response::builder()
                    .status(StatusCode::TOO_MANY_REQUESTS)
                    .body(Body::empty())
                    .unwrap(),
            )
        });
        let svc = tower::ServiceBuilder::new()
            .layer(axum_mw::from_fn(detect_rate_limit))
            .service(inner);

        // Feed a request with ClientIp already set (simulates resolve_client_ip having run).
        let req = Request::get("/")
            .extension(client_ip::ClientIp("1.2.3.4".parse().unwrap()))
            .body(Body::empty())
            .unwrap();
        let resp = svc.oneshot(req).await.unwrap();

        assert_eq!(
            resp.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "plain 429 must pass through unchanged"
        );
        assert!(
            !logs_contain("security_event=true"),
            "handler-produced 429 without marker must not emit RateLimitHit"
        );
        assert!(
            !logs_contain("rate_limit_hit"),
            "handler-produced 429 without marker must not emit rate_limit_hit"
        );
    }

    /// When `ClientIp` is absent from extensions on a governor 429,
    /// `detect_rate_limit` must still strip the marker, return the 429, and
    /// emit a `tracing::error!` (not a security event — there is no IP to log).
    ///
    /// This exercises the defensive `None` branch and confirms it doesn't panic.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn governor_429_without_client_ip_strips_marker_and_logs_error() {
        use std::convert::Infallible;
        use tower::ServiceExt;
        use tower::service_fn;

        // Inner service returns a governor-style 429 (recognized marker) but the
        // request has no ClientIp extension — simulates wrong layer ordering.
        let inner = service_fn(|_req: Request<Body>| async {
            Ok::<_, Infallible>(
                Response::builder()
                    .status(StatusCode::TOO_MANY_REQUESTS)
                    .header(GOVERNOR_MARKER_HEADER, GOVERNOR_MARKER_GLOBAL)
                    .body(Body::empty())
                    .unwrap(),
            )
        });
        let svc = tower::ServiceBuilder::new()
            .layer(axum_mw::from_fn(detect_rate_limit))
            .service(inner);

        // No ClientIp extension on the request.
        let req = Request::get("/").body(Body::empty()).unwrap();
        let resp = svc.oneshot(req).await.unwrap();

        assert_eq!(
            resp.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "429 must pass through"
        );
        assert!(
            resp.headers().get(GOVERNOR_MARKER_HEADER).is_none(),
            "marker must be stripped even when ClientIp is absent"
        );
        assert!(
            !logs_contain("security_event=true"),
            "no security event when IP is unknown"
        );
        assert!(
            logs_contain("RateLimitHit event suppressed"),
            "missing ClientIp must produce a tracing::error!"
        );
    }

    /// Helper: send a GET to `/health` from `xff_ip` and return the response status.
    async fn global_health_status(app: &Router, xff_ip: &str) -> StatusCode {
        xff_get_status(app, "/health", xff_ip).await
    }

    /// Router with security + one asset (`probe.js`) served from a tempdir under
    /// both `/static` and `/surface-static`, plus an authenticated session, so
    /// the asset governor and the authenticated asset path can both be driven.
    /// Returns `(app, db, session_token, tempdir)`; the db and tempdir must
    /// outlive the app (auth lookups and file serving depend on them).
    async fn asset_governor_app(
        sec: &SecurityConfig,
    ) -> (Router, db::Db, String, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("probe.js"), b"export {};").unwrap();
        let db = db::init_db_memory();
        let mut state = test_state(&db);
        state.static_dir = tmp.path().to_path_buf();
        state.surface_dist_dir = tmp.path().to_path_buf();
        let app = build_router(state, Some(sec), 1, 2576)
            .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))));
        let (session_token, _) = setup_authenticated_user(&db).await;
        (app, db, session_token, tmp)
    }

    /// Helper: authenticated GET for an asset path from `xff_ip`, returning status.
    async fn authed_asset_status(
        app: &Router,
        path: &str,
        token: &str,
        xff_ip: &str,
    ) -> StatusCode {
        let req = Request::get(path)
            .header("cookie", format!("brenn_session={token}"))
            .header("x-forwarded-for", xff_ip)
            .body(Body::empty())
            .unwrap();
        app.clone().oneshot(req).await.unwrap().status()
    }

    /// Asset routes draw on their own bucket, not the global one: with a tight
    /// global governor, many more asset requests than the global burst all serve,
    /// and the global budget is still fully intact afterwards.
    #[tokio::test]
    async fn asset_routes_do_not_consume_global_budget() {
        let sec = SecurityConfig {
            global_rate_burst: 2,
            global_rate_interval_secs: 600,
            asset_rate_burst: 2000,
            asset_rate_interval_secs: 600,
            ..Default::default()
        };
        let (app, _db, token, _tmp) = asset_governor_app(&sec).await;

        // Far more asset requests than the global burst (2) all succeed.
        for _ in 0..5 {
            assert_eq!(
                authed_asset_status(&app, "/surface-static/probe.js", &token, "1.2.3.4").await,
                StatusCode::OK,
                "asset requests must serve from the asset bucket, not the global one"
            );
        }

        // The global bucket is untouched: exactly two non-asset requests succeed,
        // the third 429s.
        assert_eq!(global_health_status(&app, "1.2.3.4").await, StatusCode::OK);
        assert_eq!(global_health_status(&app, "1.2.3.4").await, StatusCode::OK);
        assert_eq!(
            global_health_status(&app, "1.2.3.4").await,
            StatusCode::TOO_MANY_REQUESTS,
            "global budget (burst 2) must be intact after the asset requests"
        );
    }

    /// The reverse isolation: draining a tight asset governor 429s asset routes
    /// while non-asset routes keep serving from the independent global bucket.
    #[tokio::test]
    async fn tight_asset_governor_does_not_throttle_non_asset_routes() {
        let sec = SecurityConfig {
            asset_rate_burst: 2,
            asset_rate_interval_secs: 600,
            global_rate_burst: 100,
            global_rate_interval_secs: 600,
            ..Default::default()
        };
        let (app, _db, token, _tmp) = asset_governor_app(&sec).await;

        // Drain the asset bucket (burst 2).
        assert_eq!(
            authed_asset_status(&app, "/surface-static/probe.js", &token, "1.2.3.4").await,
            StatusCode::OK
        );
        assert_eq!(
            authed_asset_status(&app, "/surface-static/probe.js", &token, "1.2.3.4").await,
            StatusCode::OK
        );
        assert_eq!(
            authed_asset_status(&app, "/surface-static/probe.js", &token, "1.2.3.4").await,
            StatusCode::TOO_MANY_REQUESTS,
            "third asset request drains the asset bucket"
        );

        // Non-asset routes keep serving — the global bucket is independent.
        assert_eq!(
            global_health_status(&app, "1.2.3.4").await,
            StatusCode::OK,
            "a drained asset bucket must not throttle non-asset routes"
        );
    }

    /// An asset-governor 429 emits `RateLimitHit` with the `asset rate limit`
    /// detail (distinct from the global governor's), and the marker header is
    /// stripped from the client response.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn asset_governor_429_emits_asset_rate_limit_event() {
        let sec = SecurityConfig {
            asset_rate_burst: 1,
            asset_rate_interval_secs: 600,
            ..Default::default()
        };
        let (app, _db, token, _tmp) = asset_governor_app(&sec).await;

        // First asset request drains the single token.
        assert_eq!(
            authed_asset_status(&app, "/surface-static/probe.js", &token, "9.9.9.9").await,
            StatusCode::OK
        );

        // Second: 429 with a stripped marker and an asset-detail RateLimitHit.
        let req = Request::get("/surface-static/probe.js")
            .header("cookie", format!("brenn_session={token}"))
            .header("x-forwarded-for", "9.9.9.9")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(
            resp.headers().get(GOVERNOR_MARKER_HEADER).is_none(),
            "asset governor marker must be stripped from the client response"
        );
        assert!(
            logs_contain("rate_limit_hit"),
            "asset 429 must emit a RateLimitHit security event"
        );
        assert!(
            logs_contain("asset rate limit"),
            "RateLimitHit detail must name the asset governor"
        );
        assert!(
            logs_contain("9.9.9.9"),
            "event must log the client IP resolved from XFF"
        );
    }

    /// The fail-loud arm: a 429 carrying an unrecognized `GOVERNOR_MARKER_HEADER`
    /// value must `error!`, still log `RateLimitHit` with the raw value (not drop
    /// it), and still strip the marker.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn unrecognized_governor_marker_fails_loud_and_still_logs() {
        use std::convert::Infallible;
        use tower::ServiceExt;
        use tower::service_fn;

        let inner = service_fn(|_req: Request<Body>| async {
            Ok::<_, Infallible>(
                Response::builder()
                    .status(StatusCode::TOO_MANY_REQUESTS)
                    .header(GOVERNOR_MARKER_HEADER, "bogus")
                    .body(Body::empty())
                    .unwrap(),
            )
        });
        let svc = tower::ServiceBuilder::new()
            .layer(axum_mw::from_fn(detect_rate_limit))
            .service(inner);

        let req = Request::get("/")
            .extension(client_ip::ClientIp("1.2.3.4".parse().unwrap()))
            .body(Body::empty())
            .unwrap();
        let resp = svc.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(
            resp.headers().get(GOVERNOR_MARKER_HEADER).is_none(),
            "unrecognized marker must still be stripped"
        );
        assert!(
            logs_contain("unrecognized governor marker value"),
            "an unrecognized marker must fail loud with an error!"
        );
        assert!(
            logs_contain("rate_limit_hit"),
            "the RateLimitHit event must still be logged, not dropped"
        );
        assert!(
            logs_contain("bogus"),
            "the RateLimitHit detail must carry the raw marker value"
        );
    }

    #[tokio::test]
    async fn landing_page_references_app_css_with_build_id() {
        let (app, _db, session_token) = landing_page_router_two_apps().await;
        let response = fetch_landing_page(app, &session_token).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_string(response.into_body()).await;
        let needle = format!(
            "href=\"/static/app.css?v={}\"",
            crate::test_support::TEST_BUILD_ID
        );
        assert!(
            body.contains(&needle),
            "expected {needle:?} in landing page, got: {body}"
        );
    }

    #[tokio::test]
    async fn landing_page_has_no_store_cache_header() {
        // Same no-store contract as the app shell, so a stale landing
        // page can't pin a stale `?v=` after a deploy.
        let (app, _db, session_token) = landing_page_router_two_apps().await;
        let response = fetch_landing_page(app, &session_token).await;
        assert_eq!(response.status(), StatusCode::OK);
        let cc = response
            .headers()
            .get("cache-control")
            .expect("landing page should set Cache-Control")
            .to_str()
            .unwrap();
        assert_eq!(cc, "no-store");
    }

    // --- Full flow ---

    #[tokio::test]
    async fn full_flow_register_login_logout() {
        // This test exercises the full user lifecycle but needs separate app instances
        // for each request since oneshot consumes the service.
        let db = db::init_db_memory();
        let mock_addr = MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999)));

        // Step 1: Create invite code.
        let code = {
            let conn = db.lock().await;
            create_invite_code(&conn)
        };

        // Step 2: Register.
        let state = test_state(&db);
        let app = build_router(state, None, 0, 2576).layer(mock_addr);
        let response = app
            .oneshot(
                Request::post("/auth/register")
                    .header("content-type", "application/x-www-form-urlencoded")
                    .body(Body::from(format!(
                        "username=alice&password=strong-password-12&invite_code={code}"
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        let reg_cookie = get_set_cookie(&response).expect("registration should set cookie");
        let reg_token = extract_session_token(&reg_cookie).to_string();

        // Step 3: Access the app page — should render (not redirect).
        let state = test_state(&db);
        let app = build_router(state, None, 0, 2576).layer(mock_addr);
        let response = app
            .oneshot(
                Request::get("/app/test")
                    .header("cookie", format!("brenn_session={reg_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "authenticated user should see app page"
        );
        let body = body_string(response.into_body()).await;
        // username is now delivered via Welcome WS message, not static HTML

        // Step 4: Extract CSRF token from app page meta tag for logout.
        let csrf_token = body
            .split("name=\"csrf-token\" content=\"")
            .nth(1)
            .unwrap()
            .split('"')
            .next()
            .unwrap()
            .to_string();

        // Step 5: Logout.
        let state = test_state(&db);
        let app = build_router(state, None, 0, 2576).layer(mock_addr);
        let response = app
            .oneshot(
                Request::post("/logout")
                    .header("cookie", format!("brenn_session={reg_token}"))
                    .header("content-type", "application/x-www-form-urlencoded")
                    .body(Body::from(format!("csrf_token={csrf_token}")))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SEE_OTHER);

        // Step 6: Try to access app page with the old cookie — should redirect.
        let state = test_state(&db);
        let app = build_router(state, None, 0, 2576).layer(mock_addr);
        let response = app
            .oneshot(
                Request::get("/")
                    .header("cookie", format!("brenn_session={reg_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::SEE_OTHER,
            "logged-out user should be redirected"
        );
        assert_eq!(
            response
                .headers()
                .get("location")
                .unwrap()
                .to_str()
                .unwrap(),
            "/auth/login"
        );
    }

    // --- max-image-long-edge meta tag ---

    /// The app shell must include `<meta name="max-image-long-edge" content="...">` with
    /// the value passed to `build_router`. This is the only channel the browser uses to
    /// read the resize cap.
    #[tokio::test]
    async fn app_page_includes_max_image_long_edge_meta_tag_default() {
        let (app, db) = test_app();
        let (session_token, _) = setup_authenticated_user(&db).await;
        let response = app
            .oneshot(
                Request::get("/app/test")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_string(response.into_body()).await;
        assert!(
            body.contains(r#"name="max-image-long-edge" content="2576""#),
            "app shell must contain max-image-long-edge meta tag with default value 2576; got: {body}"
        );
    }

    /// When `build_router` is called with a non-default `max_image_long_edge`, the meta
    /// tag must reflect the configured value.
    #[tokio::test]
    async fn app_page_includes_max_image_long_edge_meta_tag_configured() {
        let db = db::init_db_memory();
        let state = test_state(&db);
        let mock_addr = MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999)));
        // Build router with a non-default cap of 1024.
        let app = build_router(state, None, 0, 1024).layer(mock_addr);
        let (session_token, _) = setup_authenticated_user(&db).await;
        let response = app
            .oneshot(
                Request::get("/app/test")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_string(response.into_body()).await;
        assert!(
            body.contains(r#"name="max-image-long-edge" content="1024""#),
            "app shell must reflect configured max_image_long_edge=1024; got: {body}"
        );
    }

    // --- Upload route body limit wiring ---

    /// With a configured `SecurityConfig.upload_body_limit` set to a small value,
    /// a body that exceeds the limit must return 413. Asserts that `upload_body_limit`
    /// is wired into the route-level body limit rather than using the hardcoded literal.
    #[tokio::test]
    async fn upload_route_body_limit_from_security_config() {
        let dir = tempfile::tempdir().unwrap();
        let mut apps = IndexMap::new();
        let mut cfg = default_test_app_config("test", "Test App");
        cfg.working_dir = dir.path().to_path_buf();
        apps.insert("test".to_string(), cfg);
        let db = db::init_db_memory();
        let state = crate::test_support::state::test_state_with_apps(&db, Arc::new(apps));
        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        // Set a small body limit: 4 KiB for the upload route.
        let sec = SecurityConfig {
            upload_body_limit: 4096,
            ..Default::default()
        };

        let mock_addr = MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999)));

        // Straddle the limit by exactly 1 byte. The multipart envelope for
        // "big.bin" is 161 bytes (boundary + headers + end-boundary), so
        // content = 3936 → total body = 4097 = limit + 1.
        let oversized_content = vec![0u8; 3936];
        let (content_type, body_over) = multipart_body("big.bin", &oversized_content);
        let app_over = build_router(state.clone(), Some(&sec), 0, 2576).layer(mock_addr);
        let resp_over = app_over
            .oneshot(
                Request::post("/app/test/upload")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .header("content-type", content_type)
                    .body(Body::from(body_over))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp_over.status(),
            StatusCode::PAYLOAD_TOO_LARGE,
            "body exceeding upload_body_limit must return 413"
        );

        // Body exactly at the limit must be accepted. The multipart envelope
        // for "big.bin" is 161 bytes, so content = 3935 → total body = 4096 = limit.
        let small_content = vec![0u8; 3935];
        let (content_type2, body_small) = multipart_body("big.bin", &small_content);
        let mock_addr2 = MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999)));
        let app_small = build_router(state, Some(&sec), 0, 2576).layer(mock_addr2);
        let resp_small = app_small
            .oneshot(
                Request::post("/app/test/upload")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .header("content-type", content_type2)
                    .body(Body::from(body_small))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp_small.status(),
            StatusCode::OK,
            "body at upload_body_limit must be accepted"
        );
    }
}
