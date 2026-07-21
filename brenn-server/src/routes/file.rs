//! File view route: serve rendered or raw files from the app repo.

use std::net::IpAddr;
use std::path::Path;

use axum::Extension;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use brenn_lib::auth::session::Session;
use brenn_lib::obs::security::{SecurityEventType, log_and_alert_security_event};
use serde::Deserialize;
use tracing::warn;

use super::html_escape;
use crate::client_ip::ClientIp;
use crate::state::AppState;

use crate::artifact::encode_url_path;

/// Maximum file size for display (1 MB).
const MAX_FILE_SIZE: u64 = 1024 * 1024;

/// Size of the prefix checked for binary detection (null bytes).
const BINARY_CHECK_SIZE: usize = 8192;

#[derive(Deserialize)]
pub struct FileViewParams {
    #[serde(default)]
    raw: Option<String>,
}

impl FileViewParams {
    fn is_raw(&self) -> bool {
        self.raw.is_some()
    }
}

/// GET /app/{slug}/file/*path — view a file from the app's working directory.
pub async fn file_view(
    AxumPath((slug, path)): AxumPath<(String, String)>,
    Query(params): Query<FileViewParams>,
    Extension(session): Extension<Session>,
    Extension(ClientIp(ip)): Extension<ClientIp>,
    State(state): State<AppState>,
) -> Result<Response, StatusCode> {
    // Look up app config.
    let app = match state.apps.get(&slug) {
        Some(app) => app,
        None => {
            log_and_alert_security_event(
                &state.alert_dispatcher,
                SecurityEventType::UnrecognizedUrl,
                ip,
                &format!("/app/{slug}/file/{path}"),
            );
            return Err(StatusCode::NOT_FOUND);
        }
    };

    // Verify user has access.
    if !app.user_has_access(&session.user.username) {
        log_and_alert_security_event(
            &state.alert_dispatcher,
            SecurityEventType::AuthFailure,
            ip,
            &format!(
                "user {} denied file access to app {}",
                session.user.username, slug
            ),
        );
        return Err(StatusCode::FORBIDDEN);
    }

    render_file_response(
        &app.working_dir,
        &format!("/app/{slug}/file/"),
        &path,
        &params,
        &state,
        ip,
        &app.name,
        &app.frontmatter,
    )
    .await
}

/// GET /app/{slug}/mount/{mount_slug}/file/*path — view a file from one of
/// the app's declared `[[app.mount]]` repos.
pub async fn mount_file_view(
    AxumPath((slug, mount_slug, path)): AxumPath<(String, String, String)>,
    Query(params): Query<FileViewParams>,
    Extension(session): Extension<Session>,
    Extension(ClientIp(ip)): Extension<ClientIp>,
    State(state): State<AppState>,
) -> Result<Response, StatusCode> {
    // Look up app config.
    let app = match state.apps.get(&slug) {
        Some(app) => app,
        None => {
            log_and_alert_security_event(
                &state.alert_dispatcher,
                SecurityEventType::UnrecognizedUrl,
                ip,
                &format!("/app/{slug}/mount/{mount_slug}/file/{path}"),
            );
            return Err(StatusCode::NOT_FOUND);
        }
    };

    // Verify user has access.
    if !app.user_has_access(&session.user.username) {
        log_and_alert_security_event(
            &state.alert_dispatcher,
            SecurityEventType::AuthFailure,
            ip,
            &format!(
                "user {} denied file access to app {}",
                session.user.username, slug
            ),
        );
        return Err(StatusCode::FORBIDDEN);
    }

    // Look up the mount by slug among the app's resolved mounts. Any access
    // level qualifies; is_working_dir is allowed but redundant — the
    // canonical form for working-dir files is the `/file/` route.
    let mount = match app.mounts.iter().find(|m| m.slug == mount_slug) {
        Some(m) => m,
        None => {
            log_and_alert_security_event(
                &state.alert_dispatcher,
                SecurityEventType::UnrecognizedUrl,
                ip,
                &format!("/app/{slug}/mount/{mount_slug}/file/{path}"),
            );
            return Err(StatusCode::NOT_FOUND);
        }
    };

    render_file_response(
        &mount.host_path,
        &format!("/app/{slug}/mount/{mount_slug}/file/"),
        &path,
        &params,
        &state,
        ip,
        &app.name,
        &app.frontmatter,
    )
    .await
}

/// Shared rendering pipeline for working-dir and mount routes. `root` is the
/// host-side directory `path` is resolved against (canonicalised + a
/// `starts_with` containment check). `url_prefix` is the route prefix used
/// for self-links inside the rendered HTML; callers must end it with `/`.
/// `app_name` is unescaped; this helper HTML-escapes it exactly once.
/// `frontmatter_cfg` controls how a YAML frontmatter block at the top of
/// markdown files is rendered ahead of the body.
#[allow(clippy::too_many_arguments)]
async fn render_file_response(
    root: &Path,
    url_prefix: &str,
    path: &str,
    params: &FileViewParams,
    state: &AppState,
    ip: IpAddr,
    app_name: &str,
    frontmatter_cfg: &brenn_lib::config::FrontmatterRenderConfig,
) -> Result<Response, StatusCode> {
    debug_assert!(
        url_prefix.ends_with('/'),
        "url_prefix must end with '/'; got {url_prefix:?}",
    );
    // Resolve and validate path.
    let resolved = root.join(path);
    let canonical = match resolved.canonicalize() {
        Ok(p) => p,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(StatusCode::NOT_FOUND);
        }
        Err(e) => {
            warn!(path = %path, error = %e, "file_view: canonicalize failed");
            return Err(StatusCode::NOT_FOUND);
        }
    };

    let canonical_root = match root.canonicalize() {
        Ok(p) => p,
        Err(e) => {
            warn!(root = %root.display(), error = %e, "file_view: root canonicalize failed");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    // Containment check.
    if !canonical.starts_with(&canonical_root) {
        log_and_alert_security_event(
            &state.alert_dispatcher,
            SecurityEventType::MalformedMessage,
            ip,
            &format!("file_view path traversal attempt: {path}"),
        );
        return Err(StatusCode::NOT_FOUND);
    }

    // Must be a regular file.
    let metadata = match tokio::fs::metadata(&canonical).await {
        Ok(m) => m,
        Err(_) => return Err(StatusCode::NOT_FOUND),
    };
    if !metadata.is_file() {
        return Err(StatusCode::NOT_FOUND);
    }

    // Size check.
    if metadata.len() > MAX_FILE_SIZE {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }

    // Read file.
    let content_bytes = match tokio::fs::read(&canonical).await {
        Ok(c) => c,
        Err(e) => {
            warn!(path = %path, error = %e, "file_view: read failed");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    // Binary detection: check for null bytes in the first 8KB.
    let check_len = content_bytes.len().min(BINARY_CHECK_SIZE);
    let is_binary = content_bytes[..check_len].contains(&0);

    // Raw mode: return plain text.
    if params.is_raw() {
        if is_binary {
            return Ok((
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/octet-stream")],
                content_bytes,
            )
                .into_response());
        }
        return Ok((
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            content_bytes,
        )
            .into_response());
    }

    // Rendered mode: build HTML page.
    let display_path = html_escape(path);
    let escaped_app_name = html_escape(app_name);

    // Percent-encode path segments for use in URLs within the page.
    let url_encoded_path = encode_url_path(path);

    let rendered_content = if is_binary {
        format!(
            r#"<p class="binary-notice">Binary file &mdash; cannot display.</p>
<p><a href="{prefix}{url_path}?raw=1" download>Download raw file</a></p>"#,
            prefix = html_escape(url_prefix),
            url_path = html_escape(&url_encoded_path),
        )
    } else {
        let content_str = String::from_utf8_lossy(&content_bytes);
        let extension = Path::new(path)
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("");
        if extension == "md" {
            crate::frontmatter::render_markdown_with_frontmatter(&content_str, frontmatter_cfg)
        } else {
            format!("<pre><code>{}</code></pre>", html_escape(&content_str))
        }
    };

    let frontmatter_css = crate::frontmatter::FRONTMATTER_CSS;
    let build_id = state.build_id;
    let html = format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{display_path} &mdash; {escaped_app_name}</title>
    <link rel="stylesheet" href="/static/app.css">
    <style>
        html {{ height: auto; }}
        body {{ background: #0f0f1e; color: #d0d0d8; margin: 0; height: auto; overflow: auto; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }}
        .file-view-header {{ display: flex; align-items: center; justify-content: space-between; padding: 0.5rem 1rem; border-bottom: 1px solid #2a2a40; background: #161628; position: sticky; top: 0; z-index: 1; }}
        .file-path {{ font-family: "JetBrains Mono", "Fira Code", monospace; font-size: 0.85rem; color: #a0a0b8; }}
        .file-view-actions {{ display: flex; gap: 0.5rem; align-items: center; }}
        .file-view-actions a, .file-view-actions button {{ padding: 0.25rem 0.75rem; border: 1px solid #3a3a50; border-radius: 3px; background: none; color: #a0a0b8; font-size: 0.8rem; cursor: pointer; text-decoration: none; }}
        .file-view-actions a:hover, .file-view-actions button:hover {{ background: #2a2a40; color: #d0d0d8; }}
        .file-view-content {{ padding: 1rem; max-width: 48rem; line-height: 1.6; }}
        .binary-notice {{ color: #a0a0b8; font-style: italic; }}
{frontmatter_css}
    </style>
</head>
<body>
    <header class="file-view-header">
        <span class="file-path">{display_path}</span>
        <div class="file-view-actions">
            <a href="?raw=1">Raw</a>
            <button id="copy-btn">Copy</button>
        </div>
    </header>
    <main class="file-view-content md-content">
        {rendered_content}
    </main>
    <script src="/static/file-view-copy.js"></script>
    <script type="module" src="/static/nav-on-message.js?v={build_id}"></script>
</body>
</html>"#
    );

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        html,
    )
        .into_response())
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;

    use axum::body::Body;
    use axum::extract::connect_info::MockConnectInfo;
    use axum::http::{Request, StatusCode};
    use brenn_lib::db;
    use indexmap::IndexMap;
    use tower::ServiceExt;

    use crate::router::build_router;
    use crate::test_support::app_config::default_test_app_config;
    use crate::test_support::http::{body_string, setup_authenticated_user};
    use crate::test_support::state::{
        test_app, test_app_with_mount, test_app_with_working_dir, test_state_with_apps,
    };

    // --- File view route ---

    #[tokio::test]
    async fn file_view_unknown_slug_returns_404() {
        let (app, db) = test_app();
        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        let response = app
            .oneshot(
                Request::get("/app/nonexistent/file/test.md")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn file_view_unauthenticated_redirects() {
        let (app, _db) = test_app();

        let response = app
            .oneshot(
                Request::get("/app/test/file/test.md")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        // Auth middleware redirects to login.
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
    }

    #[tokio::test]
    async fn file_view_path_traversal_returns_404() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("safe.md"), "# Safe").unwrap();
        let (app, db) = test_app_with_working_dir(dir.path().to_path_buf());
        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        let response = app
            .oneshot(
                Request::get("/app/test/file/../../etc/passwd")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn file_view_renders_markdown() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("test.md"), "# Hello World").unwrap();
        let (app, db) = test_app_with_working_dir(dir.path().to_path_buf());
        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        let response = app
            .oneshot(
                Request::get("/app/test/file/test.md")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_string(response.into_body()).await;
        assert!(
            body.contains("<h1>Hello World</h1>"),
            "should render markdown: {body}"
        );
        assert!(body.contains("test.md"), "should show file path: {body}");
    }

    #[tokio::test]
    async fn file_view_renders_frontmatter_block() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("task.md"),
            "---\nstatus: in_progress\npriority: 2\n---\n# Buy tires\n",
        )
        .unwrap();
        let (app, db) = test_app_with_working_dir(dir.path().to_path_buf());
        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        let response = app
            .oneshot(
                Request::get("/app/test/file/task.md")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_string(response.into_body()).await;
        assert!(
            body.contains("class=\"fm-block\""),
            "should render frontmatter: {body}"
        );
        assert!(
            body.contains("<dt>status</dt>"),
            "should render status key: {body}"
        );
        assert!(
            body.contains("<h1>Buy tires</h1>"),
            "should still render body: {body}"
        );
    }

    #[tokio::test]
    async fn file_view_raw_returns_plain_text() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("test.md"), "# Raw Content").unwrap();
        let (app, db) = test_app_with_working_dir(dir.path().to_path_buf());
        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        let response = app
            .oneshot(
                Request::get("/app/test/file/test.md?raw=1")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "text/plain; charset=utf-8"
        );
        let body = body_string(response.into_body()).await;
        assert_eq!(body, "# Raw Content");
    }

    #[tokio::test]
    async fn file_view_nonexistent_file_returns_404() {
        let dir = tempfile::tempdir().unwrap();
        let (app, db) = test_app_with_working_dir(dir.path().to_path_buf());
        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        let response = app
            .oneshot(
                Request::get("/app/test/file/does_not_exist.md")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn file_view_non_markdown_renders_pre() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("code.rs"), "fn main() {}").unwrap();
        let (app, db) = test_app_with_working_dir(dir.path().to_path_buf());
        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        let response = app
            .oneshot(
                Request::get("/app/test/file/code.rs")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_string(response.into_body()).await;
        assert!(
            body.contains("<pre><code>"),
            "non-md should be in pre/code: {body}"
        );
        assert!(
            body.contains("fn main()"),
            "should contain file content: {body}"
        );
    }

    #[tokio::test]
    async fn file_view_access_denied_returns_403() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("test.md"), "# Test").unwrap();

        // Create an app with restricted access.
        let db = db::init_db_memory();
        let mut apps = IndexMap::new();
        let mut cfg = default_test_app_config("restricted", "Restricted");
        cfg.working_dir = dir.path().to_path_buf();
        cfg.allowed_users = vec!["otheruser".to_string()];
        apps.insert("restricted".to_string(), cfg);
        let state = test_state_with_apps(&db, Arc::new(apps));
        let app = build_router(state, None, 0, 2576)
            .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))));

        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        let response = app
            .oneshot(
                Request::get("/app/restricted/file/test.md")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn file_view_subdirectory_path() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("docs/sub")).unwrap();
        std::fs::write(dir.path().join("docs/sub/nested.md"), "# Nested").unwrap();
        let (app, db) = test_app_with_working_dir(dir.path().to_path_buf());
        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        let response = app
            .oneshot(
                Request::get("/app/test/file/docs/sub/nested.md")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_string(response.into_body()).await;
        assert!(
            body.contains("<h1>Nested</h1>"),
            "should render nested file: {body}"
        );
    }

    #[tokio::test]
    async fn file_view_binary_file_shows_notice() {
        let dir = tempfile::tempdir().unwrap();
        // Write a file with null bytes (binary).
        let mut content = b"PNG\x00\x00\x00binary data".to_vec();
        content.resize(100, 0);
        std::fs::write(dir.path().join("image.png"), &content).unwrap();
        let (app, db) = test_app_with_working_dir(dir.path().to_path_buf());
        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        let response = app
            .oneshot(
                Request::get("/app/test/file/image.png")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_string(response.into_body()).await;
        assert!(
            body.contains("Binary file"),
            "should show binary notice: {body}"
        );
        assert!(
            body.contains("Download raw file"),
            "should have download link: {body}"
        );
    }

    #[tokio::test]
    async fn file_view_binary_raw_returns_octet_stream() {
        let dir = tempfile::tempdir().unwrap();
        let content = b"\x00\x01\x02binary";
        std::fs::write(dir.path().join("data.bin"), content).unwrap();
        let (app, db) = test_app_with_working_dir(dir.path().to_path_buf());
        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        let response = app
            .oneshot(
                Request::get("/app/test/file/data.bin?raw=1")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "application/octet-stream"
        );
    }

    #[tokio::test]
    async fn file_view_directory_returns_404() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("subdir")).unwrap();
        let (app, db) = test_app_with_working_dir(dir.path().to_path_buf());
        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        let response = app
            .oneshot(
                Request::get("/app/test/file/subdir")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn file_view_too_large_returns_413() {
        let dir = tempfile::tempdir().unwrap();
        // Write a file just over 1MB.
        let content = "x".repeat(1024 * 1024 + 1);
        std::fs::write(dir.path().join("big.txt"), content).unwrap();
        let (app, db) = test_app_with_working_dir(dir.path().to_path_buf());
        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        let response = app
            .oneshot(
                Request::get("/app/test/file/big.txt")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    // --- Mount file view route ---

    #[tokio::test]
    async fn mount_file_view_renders_markdown() {
        let wd = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        std::fs::write(mount.path().join("doc.md"), "# Mount Doc").unwrap();
        let (app, db) = test_app_with_mount(
            "test",
            wd.path().to_path_buf(),
            "life",
            mount.path().to_path_buf(),
            vec![],
        );
        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        let response = app
            .oneshot(
                Request::get("/app/test/mount/life/file/doc.md")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_string(response.into_body()).await;
        assert!(
            body.contains("<h1>Mount Doc</h1>"),
            "should render mount file: {body}"
        );
    }

    #[tokio::test]
    async fn mount_file_view_raw_returns_plain_text() {
        let wd = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        std::fs::write(mount.path().join("doc.md"), "# Raw").unwrap();
        let (app, db) = test_app_with_mount(
            "test",
            wd.path().to_path_buf(),
            "life",
            mount.path().to_path_buf(),
            vec![],
        );
        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        let response = app
            .oneshot(
                Request::get("/app/test/mount/life/file/doc.md?raw=1")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "text/plain; charset=utf-8"
        );
        let body = body_string(response.into_body()).await;
        assert_eq!(body, "# Raw");
    }

    #[tokio::test]
    async fn mount_file_view_unknown_mount_returns_404() {
        let wd = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        std::fs::write(mount.path().join("doc.md"), "# Doc").unwrap();
        let (app, db) = test_app_with_mount(
            "test",
            wd.path().to_path_buf(),
            "life",
            mount.path().to_path_buf(),
            vec![],
        );
        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        let response = app
            .oneshot(
                Request::get("/app/test/mount/nonexistent/file/doc.md")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn mount_file_view_path_traversal_returns_404() {
        let wd = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        std::fs::write(mount.path().join("safe.md"), "# Safe").unwrap();
        let (app, db) = test_app_with_mount(
            "test",
            wd.path().to_path_buf(),
            "life",
            mount.path().to_path_buf(),
            vec![],
        );
        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        let response = app
            .oneshot(
                Request::get("/app/test/mount/life/file/../../etc/passwd")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn mount_file_view_nonexistent_file_returns_404() {
        let wd = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        let (app, db) = test_app_with_mount(
            "test",
            wd.path().to_path_buf(),
            "life",
            mount.path().to_path_buf(),
            vec![],
        );
        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        let response = app
            .oneshot(
                Request::get("/app/test/mount/life/file/missing.md")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn mount_file_view_access_denied_returns_403() {
        let wd = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        std::fs::write(mount.path().join("doc.md"), "# Doc").unwrap();

        // Restrict the app to a different user than the one logging in.
        let (app, db) = test_app_with_mount(
            "restricted",
            wd.path().to_path_buf(),
            "life",
            mount.path().to_path_buf(),
            vec!["otheruser".to_string()],
        );
        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        let response = app
            .oneshot(
                Request::get("/app/restricted/mount/life/file/doc.md")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn mount_file_view_subdirectory_path() {
        let wd = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(mount.path().join("kb/finance")).unwrap();
        std::fs::write(mount.path().join("kb/finance/tips.md"), "# Tips").unwrap();
        let (app, db) = test_app_with_mount(
            "test",
            wd.path().to_path_buf(),
            "life",
            mount.path().to_path_buf(),
            vec![],
        );
        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        let response = app
            .oneshot(
                Request::get("/app/test/mount/life/file/kb/finance/tips.md")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_string(response.into_body()).await;
        assert!(body.contains("<h1>Tips</h1>"), "body: {body}");
    }

    #[tokio::test]
    async fn mount_file_view_binary_file_download_link_uses_mount_url() {
        // Load-bearing test for the UrlRoot threading: the binary-notice
        // page's download anchor must use the mount URL form, not the
        // working-dir form. Regressing the helper's `url_prefix()` would
        // silently break this one place.
        let wd = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        let mut content = b"PNG\x00\x00\x00binary data".to_vec();
        content.resize(100, 0);
        std::fs::write(mount.path().join("image.png"), &content).unwrap();
        let (app, db) = test_app_with_mount(
            "test",
            wd.path().to_path_buf(),
            "life",
            mount.path().to_path_buf(),
            vec![],
        );
        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        let response = app
            .oneshot(
                Request::get("/app/test/mount/life/file/image.png")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_string(response.into_body()).await;
        assert!(body.contains("Binary file"), "body: {body}");

        // Scrape the download anchor href and assert the full URL, not
        // just presence. This is the one place url_prefix threading can
        // silently regress.
        let expected_href =
            r#"<a href="/app/test/mount/life/file/image.png?raw=1" download>Download raw file</a>"#;
        assert!(
            body.contains(expected_href),
            "expected download anchor {expected_href:?}, body: {body}"
        );
    }
}
