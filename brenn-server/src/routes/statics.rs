use axum::extract::State;
use axum::http::StatusCode;
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE};
use axum::response::IntoResponse;
use tracing::error;

use crate::state::AppState;

/// CSS for auth pages (login, register). Compiled into the binary.
/// Small, stable file — no reason to serve from disk.
const AUTH_CSS: &str = include_str!("../../../frontend/auth/auth.css");

/// Favicon compiled into the binary. 16x16 solid #e94560 (Brenn accent color).
const FAVICON: &[u8] = include_bytes!("favicon.ico");

/// PWA manifest compiled into the binary. The manifest is fetched anonymously
/// by the browser (per Fetch spec, manifest fetches default to
/// `crossorigin="anonymous"`), so it must not sit behind the session-auth
/// gate — otherwise every browser gets a 303 to `/auth/login` and tries to
/// parse the login HTML as JSON. Content is stable across users (app name,
/// theme cosmetics, brand icon URLs, hardcoded share-target action) so
/// exposing it pre-auth leaks no secrets.
///
/// `include_str!` rebuilds the binary automatically when the source manifest
/// changes; `make build` runs both frontend and backend so dist/ and the
/// in-binary copy stay in sync.
const MANIFEST: &str = include_str!("../../../frontend/src/manifest.webmanifest");

/// PWA 192px icon. Compiled in for the same reason as `MANIFEST` (browser
/// fetches it anonymously via the manifest's `icons` array). Small brand
/// artwork; baking it in keeps the auth-bypass carve-out minimal.
const ICON_192: &[u8] = include_bytes!("../../../frontend/src/icon-192.png");

/// PWA 512px icon. Same rationale as `ICON_192`.
const ICON_512: &[u8] = include_bytes!("../../../frontend/src/icon-512.png");

/// `Cache-Control` for manifest + icons: revalidate after a day, treat as
/// fresh for a week if the server is unreachable. Manifest changes only on
/// deploys; this trades a small revalidation noise floor for keeping
/// background-tab manifest re-fetches off the auth path.
const MANIFEST_CACHE_CONTROL: &str = "public, max-age=86400, stale-while-revalidate=604800";

/// GET /auth/static/auth.css — serve the compiled-in auth stylesheet.
pub async fn auth_css() -> impl IntoResponse {
    ([(CONTENT_TYPE, "text/css")], AUTH_CSS)
}

/// GET /favicon.ico — serve the compiled-in favicon.
pub async fn favicon() -> impl IntoResponse {
    ([(CONTENT_TYPE, "image/x-icon")], FAVICON)
}

/// Helper: serve a compiled-in constant body with a fixed `Content-Type`
/// and the manifest cache-control. Used by the three PWA-shell carve-out
/// handlers below; they differ only in body and content-type.
fn serve_constant(body: &'static [u8], content_type: &'static str) -> impl IntoResponse {
    (
        [
            (CONTENT_TYPE, content_type),
            (CACHE_CONTROL, MANIFEST_CACHE_CONTROL),
        ],
        body,
    )
}

/// GET /static/manifest.webmanifest — serve the PWA manifest without auth.
/// See `MANIFEST` doc-comment for why this lives outside `require_auth`.
pub async fn manifest() -> impl IntoResponse {
    serve_constant(MANIFEST.as_bytes(), "application/manifest+json")
}

/// GET /static/icon-192.png — serve the 192px PWA icon without auth.
pub async fn icon_192() -> impl IntoResponse {
    serve_constant(ICON_192, "image/png")
}

/// GET /static/icon-512.png — serve the 512px PWA icon without auth.
pub async fn icon_512() -> impl IntoResponse {
    serve_constant(ICON_512, "image/png")
}

/// GET /sw.js — serve the service worker from static_dir.
///
/// Served at `/sw.js` (not `/static/sw.js`) so the SW scope defaults to `/`,
/// allowing it to intercept `/share-target` POSTs from the Web Share Target API.
/// Cache-Control: no-cache — browsers already do SW update checks; aggressive
/// caching would delay updates.
pub async fn service_worker(State(state): State<AppState>) -> impl IntoResponse {
    let path = state.static_dir.join("sw.js");
    match tokio::fs::read(&path).await {
        Ok(contents) => (
            StatusCode::OK,
            [
                (CONTENT_TYPE, "application/javascript"),
                (CACHE_CONTROL, "no-cache"),
            ],
            contents,
        )
            .into_response(),
        Err(e) => {
            error!("failed to read service worker at {}: {e}", path.display());
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
