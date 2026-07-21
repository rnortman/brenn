//! Surface HTML page handler: `GET /surface/{slug}`.
//!
//! Serves the backend-rendered page a browser tab loads to run a surface: the
//! two metas the kernel reads (`surface-slug`, `brenn-build-id`), the component
//! module manifest the TS bootstrap consumes, the kernel stylesheet, the kernel
//! DOM root, and the bootstrap script. Pre-render checks (unknown slug → 404,
//! denied user → 403) mirror the WS handler so the same fail2ban signal flows
//! from both surface entry points.

use axum::Extension;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Response;
use brenn_lib::auth::session::Session;
use brenn_surface_contract::{
    KERNEL_ARTIFACT, SURFACE_ROOT_ID, module_artifact, processor_module_path,
};
use brenn_surface_proto::Abi;
use serde::Serialize;

use super::authorize_surface;
use crate::client_ip::ClientIp;
use crate::router::RelaxedWasmCsp;
use crate::routes::app::page_html;
use crate::routes::html_escape;
use crate::state::AppState;

/// The component-module manifest embedded in the page as JSON.
///
/// Produced by the backend, consumed only by the TS bootstrap; never a WS frame
/// and never touched by ts-rs. Two fields: `kernel` is the fixed kernel artifact
/// URL, `components` maps each configured component `kind` to its module URL by
/// the frozen naming convention (`brenn_<kind _>.js`). All URLs carry `?v=` set
/// to the serving build id.
#[derive(Serialize)]
struct SurfaceManifest {
    kernel: String,
    components: Vec<ManifestComponent>,
}

#[derive(Serialize)]
struct ManifestComponent {
    instance: String,
    kind: String,
    abi: Abi,
    module: String,
}

/// The module URL for one declared instance, by ABI.
///
/// `Dom` carries an `instance=` query: distinct resolved URLs are distinct module
/// records to the browser, which is what forces one ES-module evaluation — and so
/// one wasm-bindgen singleton and one linear memory — per instance, while the
/// HTTP and bytecode caches still share the kind's bytes.
///
/// `Processor` carries no such query, and must not: the jco `--instantiation`
/// module instantiates nothing at evaluation, so one evaluation per kind is
/// correct and per-instance isolation comes from calling its `instantiate` once
/// per instance. Forcing extra evaluations would only duplicate glue.
///
/// The reserved ABIs never reach here — `resolve_abi` panics on them at boot.
fn module_url(entry: &brenn_surface_proto::ComponentEntry, build_id: &str) -> String {
    match entry.abi {
        Abi::Dom => format!(
            "/surface-static/{}?v={build_id}&instance={}",
            module_artifact(&entry.kind),
            entry.instance
        ),
        Abi::Processor => format!(
            "/surface-static/{}?v={build_id}",
            processor_module_path(&entry.kind)
        ),
        Abi::DomTs | Abi::Html => panic!(
            "surface page: instance {:?} declares reserved abi {:?} — boot validation rejects \
             these before a page can be served",
            entry.instance,
            entry.abi.as_str()
        ),
    }
}

/// GET /surface/{slug} — the surface page a browser tab loads.
///
/// Auth middleware has already validated the session and injected `Session` /
/// `ClientIp`. Checks run access-first: an unknown slug 404s (probe signal), a
/// denied user 403s, both as security events feeding fail2ban exactly like the
/// WS handler.
pub async fn surface_page(
    Path(slug): Path<String>,
    Extension(session): Extension<Session>,
    Extension(ClientIp(ip)): Extension<ClientIp>,
    State(state): State<AppState>,
) -> Result<Response, StatusCode> {
    // 1-2. Surface must exist and the user must pass its access check.
    let runtime = authorize_surface(&state, &slug, &session.user.username, ip, false)?;

    // 3. Render. The body is process-constant per slug (BUILD_ID and bindings
    //    are fixed at boot), so this rebuild is redundant work on a cold path.
    //    Component kinds are boot-validated to `^[a-z0-9][a-z0-9-]*$`
    //    (no `<`), and `<` is additionally escaped to `<` below so the
    //    JSON-encoded manifest cannot break out of its `<script>` container
    //    regardless of future field additions; the metas are HTML-escaped.
    let build_id = state.build_id;
    // One entry per declared instance: whatever the ABI, a trap poisons one
    // instance and never its siblings of the same kind. How that isolation is
    // bought differs by ABI — see `module_url` — so the entry carries the `abi`
    // the bootstrap loader branches on.
    let manifest_components: Vec<ManifestComponent> = runtime
        .bindings
        .components
        .iter()
        .map(|entry| ManifestComponent {
            instance: entry.instance.clone(),
            kind: entry.kind.clone(),
            abi: entry.abi,
            module: module_url(entry, build_id),
        })
        .collect();
    let manifest = SurfaceManifest {
        kernel: format!("/surface-static/{KERNEL_ARTIFACT}?v={build_id}"),
        components: manifest_components,
    };
    // `serde_json` escapes `"`/`\` but not `<`; escaping `<` to its JSON unicode
    // escape keeps the value identical while preventing a `</script>` breakout.
    let manifest_json = serde_json::to_string(&manifest)
        .expect("surface manifest serializes to JSON")
        .replace('<', "\\u003c");

    // Skin stylesheet: the page emits the base scaffolding stylesheet plus the
    // skin's CSS pack, both build-ID-stamped. `data-skin` is stamped twice from
    // one value: on `<body>` (the token scope — skin custom properties are
    // produced here so they cascade down to `body`'s own `var(--surface-bg, …)`
    // chrome fill and to every component) and on `#surface-root` (the dressing
    // scope — the skin's structural selectors). `<body>` also carries
    // `data-theme="dark"`, the initial value of the runtime theme axis a
    // theme-driving component may later rewrite. Boot validated the skin against
    // the registry, so a miss here is a broken boot invariant.
    let skin = &runtime.resolved.skin;
    let skin_path = super::skin_stylesheet_path(skin).unwrap_or_else(|| {
        panic!(
            "surface {slug}: configured skin {skin:?} absent from the skin registry — boot \
             validation guarantees its presence"
        )
    });
    let skin_escaped = html_escape(skin);

    let slug_escaped = html_escape(&slug);
    let build_id_escaped = html_escape(build_id);

    let mut response = page_html(format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
    <meta name="surface-slug" content="{slug_escaped}">
    <meta name="brenn-build-id" content="{build_id_escaped}">
    <title>Brenn — {slug_escaped}</title>
    <script type="application/json" id="brenn-surface-manifest">{manifest_json}</script>
    <link rel="stylesheet" href="/static/surface.css?v={build_id}">
    <link rel="stylesheet" href="/static/{skin_path}?v={build_id}">
</head>
<body data-skin="{skin_escaped}" data-theme="dark">
    <div id="{SURFACE_ROOT_ID}" data-skin="{skin_escaped}"></div>
    <script type="module" src="/static/surface.js?v={build_id}"></script>
</body>
</html>"#
    ));
    // Opt this document into the wasm-relaxed CSP; the kernel and component
    // modules are wasm and cannot compile under the strict `script-src 'self'`.
    response.extensions_mut().insert(RelaxedWasmCsp);
    Ok(response)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::test_support::TEST_BUILD_ID;
    use axum::body::Body;
    use axum::extract::connect_info::MockConnectInfo;
    use axum::http::{Request, StatusCode};
    use brenn_lib::db;
    use brenn_lib::messaging::config::ResolvedSurface;
    use brenn_surface_contract::SURFACE_ROOT_ID;
    use std::net::SocketAddr;
    use tower::ServiceExt;

    use super::super::build_surface_runtimes;
    use super::super::test_fixtures::{TEST_MAX_BODY_BYTES, fixture_bus};
    use crate::router::build_router;
    use crate::test_support::http::{body_string, setup_authenticated_user};
    use crate::test_support::state::test_state;
    use crate::test_support::surface::SurfaceFixture;

    /// A `deskbar` surface with an `echo-stub` component, the given access list
    /// (empty ⇒ any authenticated user).
    fn deskbar(allowed_users: Vec<String>) -> ResolvedSurface {
        SurfaceFixture::new("deskbar", "echo-stub")
            .subscribe("ephemeral:dev-stub", "echo-stub", "messages")
            .allowed_users(allowed_users)
            .build()
    }

    /// Build a router with the given surface installed and a seeded, logged-in
    /// user. Returns `(router, db, session_token)`.
    async fn surface_router(resolved: ResolvedSurface) -> (axum::Router, db::Db, String) {
        let db = db::init_db_memory();
        let mut state = test_state(&db);
        let bus = fixture_bus(vec![]);
        state.surfaces = Arc::new(build_surface_runtimes(
            vec![resolved],
            bus,
            None,
            TEST_MAX_BODY_BYTES,
            None,
            crate::test_support::surface::description_params(),
        ));
        let router = build_router(state, None, 0, 2576)
            .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))));
        let (token, _) = setup_authenticated_user(&db).await;
        (router, db, token)
    }

    #[tokio::test]
    async fn unknown_slug_404s() {
        let (router, _db, token) = surface_router(deskbar(vec![])).await;
        let response = router
            .oneshot(
                Request::get("/surface/nope")
                    .header("cookie", format!("brenn_session={token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn denied_user_403s() {
        // `setup_authenticated_user` seeds `testuser`; restrict to someone else.
        let (router, _db, token) = surface_router(deskbar(vec!["someone-else".to_string()])).await;
        let response = router
            .oneshot(
                Request::get("/surface/deskbar")
                    .header("cookie", format!("brenn_session={token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn allowed_user_gets_page_with_metas_and_manifest() {
        let (router, _db, token) = surface_router(deskbar(vec![])).await;
        let response = router
            .oneshot(
                Request::get("/surface/deskbar")
                    .header("cookie", format!("brenn_session={token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // `no-store` per the shared page_html contract.
        assert_eq!(
            response
                .headers()
                .get("cache-control")
                .unwrap()
                .to_str()
                .unwrap(),
            "no-store"
        );

        let body = body_string(response.into_body()).await;

        assert!(
            body.contains(r#"<meta name="surface-slug" content="deskbar">"#),
            "missing surface-slug meta: {body}"
        );
        assert!(
            body.contains(&format!(
                r#"<meta name="brenn-build-id" content="{TEST_BUILD_ID}">"#
            )),
            "missing brenn-build-id meta: {body}"
        );
        // Manifest: kernel + the echo-stub module by the naming convention,
        // build-ID-stamped, with the per-instance `?instance=` specifier that
        // forces a distinct module record (one evaluation, one linear memory).
        assert!(
            body.contains(&format!(
                r#""kernel":"/surface-static/brenn_surface_kernel.js?v={TEST_BUILD_ID}""#
            )),
            "missing manifest kernel URL: {body}"
        );
        assert!(
            body.contains(&format!(
                r#""module":"/surface-static/brenn_echo_stub.js?v={TEST_BUILD_ID}&instance=echo-stub""#
            )),
            "missing manifest echo-stub module URL: {body}"
        );
        assert!(
            body.contains(r#""instance":"echo-stub""#),
            "missing manifest component instance: {body}"
        );
        assert!(
            body.contains(r#""kind":"echo-stub""#),
            "missing manifest component kind: {body}"
        );
        // The loader branches on `abi`, so it must be on the entry.
        assert!(
            body.contains(r#""abi":"dom""#),
            "missing manifest component abi: {body}"
        );
        // Stylesheet + bootstrap, build-ID-stamped.
        assert!(
            body.contains(&format!(r#"href="/static/surface.css?v={TEST_BUILD_ID}""#)),
            "missing surface.css link: {body}"
        );
        // Skin stylesheet (default bench) + data-skin on the root.
        assert!(
            body.contains(&format!(
                r#"href="/static/skins/bench.css?v={TEST_BUILD_ID}""#
            )),
            "missing bench skin link: {body}"
        );
        assert!(
            body.contains(&format!(r#"src="/static/surface.js?v={TEST_BUILD_ID}""#)),
            "missing surface.js bootstrap: {body}"
        );
        assert!(
            body.contains(&format!(
                r#"<div id="{SURFACE_ROOT_ID}" data-skin="bench"></div>"#
            )),
            "missing surface-root div with data-skin: {body}"
        );
        // `<body>` carries the token-scope skin stamp plus the initial theme axis.
        assert!(
            body.contains(r#"<body data-skin="bench" data-theme="dark">"#),
            "missing body data-skin/data-theme stamps: {body}"
        );
    }

    /// A surface configured with the `foundry` skin links the foundry stylesheet
    /// and stamps `data-skin="foundry"` — proving the skin selection reaches the
    /// page.
    #[tokio::test]
    async fn foundry_skin_page_links_foundry_stylesheet() {
        let resolved = SurfaceFixture::new("barpixel", "echo-stub")
            .skin("foundry")
            .subscribe("ephemeral:dev-stub", "echo-stub", "messages")
            .build();
        let (router, _db, token) = surface_router(resolved).await;
        let response = router
            .oneshot(
                Request::get("/surface/barpixel")
                    .header("cookie", format!("brenn_session={token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_string(response.into_body()).await;
        assert!(
            body.contains(&format!(
                r#"href="/static/skins/foundry.css?v={TEST_BUILD_ID}""#
            )),
            "missing foundry skin link: {body}"
        );
        assert!(
            !body.contains("skins/bench.css"),
            "foundry page must not link the bench skin: {body}"
        );
        assert!(
            body.contains(&format!(
                r#"<div id="{SURFACE_ROOT_ID}" data-skin="foundry"></div>"#
            )),
            "missing data-skin=foundry on root: {body}"
        );
        assert!(
            body.contains(r#"<body data-skin="foundry" data-theme="dark">"#),
            "missing body data-skin=foundry stamp: {body}"
        );
    }

    /// A processor instance's manifest entry names the transpiled tree's entry
    /// module and carries **no** `instance=` query.
    ///
    /// The absence is the load-bearing half: the jco `--instantiation` module
    /// instantiates nothing at evaluation, so per-instance isolation comes from
    /// calling its `instantiate` once per instance, not from minting a module
    /// record per instance the way `dom` must. An `instance=` query here would buy
    /// nothing and duplicate the glue.
    #[tokio::test]
    async fn processor_instance_gets_the_transpiled_tree_url_without_an_instance_query() {
        let resolved = SurfaceFixture::new("deskbar", "echo-stub")
            .processor("counter-a", "counter", Default::default())
            .subscribe("ephemeral:dev-stub", "echo-stub", "messages")
            .build();
        let (router, _db, token) = surface_router(resolved).await;
        let response = router
            .oneshot(
                Request::get("/surface/deskbar")
                    .header("cookie", format!("brenn_session={token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_string(response.into_body()).await;

        assert!(
            body.contains(&format!(
                r#""abi":"processor","module":"/surface-static/processor/counter/counter.js?v={TEST_BUILD_ID}""#
            )),
            "missing processor module URL: {body}"
        );
        // The dom sibling keeps its per-instance specifier, so the branch is real
        // and not a blanket change.
        assert!(
            body.contains(&format!(
                r#""module":"/surface-static/brenn_echo_stub.js?v={TEST_BUILD_ID}&instance=echo-stub""#
            )),
            "the dom sibling keeps its instance-scoped specifier: {body}"
        );
    }

    #[tokio::test]
    async fn surface_page_csp_is_wasm_relaxed() {
        let (router, _db, token) = surface_router(deskbar(vec![])).await;
        let response = router
            .oneshot(
                Request::get("/surface/deskbar")
                    .header("cookie", format!("brenn_session={token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let headers = response.headers();
        let csp = headers
            .get("content-security-policy")
            .expect("missing CSP header")
            .to_str()
            .unwrap();
        assert!(
            csp.contains("script-src 'self' 'wasm-unsafe-eval'"),
            "surface CSP must relax script-src for wasm, got: {csp}"
        );
        // The bare `'unsafe-eval'` string→code primitive must never appear;
        // only the `'wasm-unsafe-eval'` compile-only keyword is permitted.
        assert!(
            !csp.replace("'wasm-unsafe-eval'", "")
                .contains("unsafe-eval"),
            "surface CSP must not contain bare 'unsafe-eval', got: {csp}"
        );
        assert!(
            !csp.contains("unsafe-inline"),
            "surface CSP must not contain 'unsafe-inline', got: {csp}"
        );
        // Other helmet headers are still present on the relaxed response.
        assert!(
            headers.contains_key("x-frame-options"),
            "missing X-Frame-Options on surface page"
        );
        assert!(
            headers.contains_key("x-content-type-options"),
            "missing X-Content-Type-Options on surface page"
        );
        assert!(
            headers.contains_key("strict-transport-security"),
            "missing HSTS on surface page"
        );
    }

    #[tokio::test]
    async fn surface_static_asset_keeps_strict_csp() {
        // The marker is set only by the page handler, so the asset tree keeps
        // the strict policy. A missing file 404s but still carries the header.
        let (router, _db, token) = surface_router(deskbar(vec![])).await;
        let response = router
            .oneshot(
                Request::get("/surface-static/brenn_surface_kernel.js")
                    .header("cookie", format!("brenn_session={token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let csp = response
            .headers()
            .get("content-security-policy")
            .expect("missing CSP header")
            .to_str()
            .unwrap();
        assert!(
            !csp.contains("wasm-unsafe-eval"),
            "asset response must keep the strict CSP, got: {csp}"
        );
    }
}
