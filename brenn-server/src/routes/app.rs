//! App page and landing page handlers.

use axum::Extension;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE};
use axum::response::{IntoResponse, Redirect, Response};
use brenn_lib::auth::session::Session;
use brenn_lib::obs::security::{SecurityEventType, log_and_alert_security_event};

use super::html_escape;
use crate::client_ip::ClientIp;
use crate::router::MaxImageLongEdge;
use crate::state::AppState;

/// Build a fully-formed HTML response for a per-user page: text/html plus
/// `Cache-Control: no-store`. Browsers without explicit cache headers fall
/// back to heuristic caching, which is unreliable across mobile vendors;
/// `no-store` keeps stale shells (with stale `?v=BUILD_ID` references)
/// from sitting in the disk cache after a deploy.
pub(crate) fn page_html(body: String) -> Response {
    (
        [
            (CONTENT_TYPE, "text/html; charset=utf-8"),
            (CACHE_CONTROL, "no-store"),
        ],
        body,
    )
        .into_response()
}

/// Theme color for meta tags and PWA manifest. #1a1a2e (Brenn dark background).
const THEME_COLOR: &str = "#1a1a2e";

/// GET / — landing page. Single app: redirect. Multiple apps: app selector.
pub async fn landing_page(
    Extension(session): Extension<Session>,
    State(state): State<AppState>,
) -> Response {
    let username = &session.user.username;

    // Collect apps this user has access to.
    let accessible: Vec<_> = state
        .apps
        .values()
        .filter(|app| app.user_has_access(username))
        .collect();

    // Single app (total, not just accessible): redirect directly.
    // This matches the common case of a single-app deployment.
    if state.apps.len() == 1
        && let Some(app) = accessible.first()
    {
        return Redirect::to(&format!("/app/{}", app.slug)).into_response();
    }

    // Multiple apps or no access: render the app selector page.
    let csrf_token = html_escape(&session.csrf_token);
    let username_escaped = html_escape(username);
    let build_id = state.build_id;

    let theme_color = THEME_COLOR;
    let app_links: String = if accessible.is_empty() {
        "<p>You don't have access to any apps.</p>".to_string()
    } else {
        accessible
            .iter()
            .map(|app| {
                let slug = html_escape(&app.slug);
                let name = html_escape(&app.name);
                let icon = html_escape(&app.icon);
                let desc = html_escape(&app.description);

                let icon_html = if icon.is_empty() {
                    String::new()
                } else {
                    format!(r#"<span class="app-icon">{icon}</span>"#)
                };

                let desc_html = if desc.is_empty() {
                    String::new()
                } else {
                    format!(r#"<span class="app-desc">{desc}</span>"#)
                };

                format!(
                    r#"<li><a href="/app/{slug}">{icon_html}<span class="app-info"><span class="app-name">{name}</span>{desc_html}</span></a></li>"#,
                )
            })
            .collect::<Vec<_>>()
            .join("\n            ")
    };

    page_html(format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
    <meta name="theme-color" content="{theme_color}">
    <title>Brenn</title>
    <link rel="stylesheet" href="/static/app.css?v={build_id}">
    <link rel="manifest" href="/static/manifest.webmanifest">
</head>
<body>
    <header class="app-header">
        <a href="/" class="brand">brenn</a>
        <span class="spacer"></span>
        <span class="user">{username_escaped}</span>
        <form method="post" action="/logout" class="logout-form">
            <input type="hidden" name="csrf_token" value="{csrf_token}">
            <button type="submit" class="logout-btn">Log out</button>
        </form>
    </header>
    <main class="app-selector">
        <div class="app-selector__content">
            <h1>Choose an app to get started</h1>
            <ul>
                {app_links}
            </ul>
        </div>
    </main>
    <script>if('serviceWorker' in navigator)navigator.serviceWorker.register('/sw.js')</script>
    <script type="module" src="/static/nav-on-message.js?v={build_id}"></script>
</body>
</html>"#
    ))
}

/// GET /app/{slug} — serve the app shell for a specific app (no specific conversation).
pub async fn app_page(
    Path(slug): Path<String>,
    Extension(session): Extension<Session>,
    Extension(ClientIp(ip)): Extension<ClientIp>,
    Extension(MaxImageLongEdge(max_image_long_edge)): Extension<MaxImageLongEdge>,
    State(state): State<AppState>,
) -> Result<Response, StatusCode> {
    render_app_shell(&slug, None, &session, ip, max_image_long_edge, &state)
}

/// GET /app/{slug}/c/{conversation_id} — serve the app shell targeting a specific conversation.
/// The conversation ID is not validated here — the frontend sends SwitchConversation over WS,
/// and the existing handler does full ownership validation. This just passes the ID through
/// as a meta tag so the frontend knows which conversation to request.
pub async fn app_page_with_conversation(
    Path((slug, conversation_id)): Path<(String, i64)>,
    Extension(session): Extension<Session>,
    Extension(ClientIp(ip)): Extension<ClientIp>,
    Extension(MaxImageLongEdge(max_image_long_edge)): Extension<MaxImageLongEdge>,
    State(state): State<AppState>,
) -> Result<Response, StatusCode> {
    render_app_shell(
        &slug,
        Some(conversation_id),
        &session,
        ip,
        max_image_long_edge,
        &state,
    )
}

/// Render the app shell HTML. Shared by `app_page` and `app_page_with_conversation`.
fn render_app_shell(
    slug: &str,
    initial_conversation_id: Option<i64>,
    session: &Session,
    client_ip: std::net::IpAddr,
    max_image_long_edge: u32,
    state: &AppState,
) -> Result<Response, StatusCode> {
    let app = match state.apps.get(slug) {
        Some(app) => app,
        None => {
            log_and_alert_security_event(
                &state.alert_dispatcher,
                SecurityEventType::UnrecognizedUrl,
                client_ip,
                &format!("/app/{slug}"),
            );
            return Err(StatusCode::NOT_FOUND);
        }
    };

    if !app.user_has_access(&session.user.username) {
        log_and_alert_security_event(
            &state.alert_dispatcher,
            SecurityEventType::AuthFailure,
            client_ip,
            &format!(
                "user {} denied access to app {}",
                session.user.username, slug
            ),
        );
        return Err(StatusCode::FORBIDDEN);
    }

    let csrf_token = html_escape(&session.csrf_token);
    let app_slug = html_escape(slug);
    let app_name = html_escape(&app.name);
    let build_id = state.build_id;
    let theme_color = THEME_COLOR;
    let initial_conv_id = match initial_conversation_id {
        Some(id) => id.to_string(),
        None => String::new(),
    };

    Ok(page_html(format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
    <meta name="theme-color" content="{theme_color}">
    <meta name="csrf-token" content="{csrf_token}">
    <meta name="app-slug" content="{app_slug}">
    <meta name="initial-conversation-id" content="{initial_conv_id}">
    <meta name="max-image-long-edge" content="{max_image_long_edge}">
    <title>Brenn — {app_name}</title>
    <link rel="stylesheet" href="/static/app.css?v={build_id}">
    <link rel="manifest" href="/static/manifest.webmanifest">
</head>
<body>
    <header class="app-header">
        <a href="/" class="brand">brenn</a>
        <span class="spacer"></span>
    </header>
    <brenn-app></brenn-app>
    <script type="module" src="/static/main.js?v={build_id}"></script>
    <script>if('serviceWorker' in navigator)navigator.serviceWorker.register('/sw.js')</script>
</body>
</html>"#
    )))
}
