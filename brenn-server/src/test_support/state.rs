use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use axum::extract::connect_info::MockConnectInfo;
use brenn_lib::config::{AppConfig, SecurityConfig};
use brenn_obs::alerting::make_capturing_alerter;
use indexmap::IndexMap;

use crate::router::build_router;
use crate::state::AppState;

use super::app_config::default_test_app_config;

/// Build a default test `AppState` from a shared in-memory database.
/// All optional services are `None`; apps defaults to the single "test" app.
pub fn test_state(db: &brenn_db::Db) -> AppState {
    AppState::for_test(db.clone(), None)
}

/// Build a test app with an in-memory database and mock ConnectInfo.
/// Rate limiting is disabled — it requires real TCP connections for peer IP extraction.
pub fn test_app() -> (Router, brenn_db::Db) {
    let db = crate::test_support::init_db_memory();
    let state = test_state(&db);
    let app = build_router(state, None, 0, 2576)
        .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))));
    (app, db)
}

/// Build an AppState with a custom apps map (for access-control tests).
pub fn test_state_with_apps(db: &brenn_db::Db, apps: Arc<IndexMap<String, AppConfig>>) -> AppState {
    let mut state = test_state(db);
    state.apps = apps;
    state
}

/// Build a router from an already-constructed apps map. Returns `(router, db)`.
/// Use when the test needs to control app config directly (e.g. custom names or
/// multi-app landing pages).
pub fn test_app_with_apps(apps: Arc<IndexMap<String, AppConfig>>) -> (Router, brenn_db::Db) {
    let db = crate::test_support::init_db_memory();
    let state = test_state_with_apps(&db, apps);
    let app = build_router(state, None, 0, 2576)
        .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))));
    (app, db)
}

/// Build a router for access-control tests: one app at `slug` with the given
/// `allowed_users` list. Returns `(router, db)`.
pub fn test_app_for_access_control(
    slug: &str,
    allowed_users: Vec<String>,
) -> (Router, brenn_db::Db) {
    let db = crate::test_support::init_db_memory();
    let mut cfg = default_test_app_config(slug, &format!("{slug} App"));
    cfg.allowed_users = allowed_users;
    let mut apps = IndexMap::new();
    apps.insert(slug.to_string(), cfg);
    let state = test_state_with_apps(&db, Arc::new(apps));
    let app = build_router(state, None, 0, 2576)
        .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))));
    (app, db)
}

/// Build an `AppState` identical to `test_state` but with a
/// `CapturingAlerter` so the handshake tests can assert on dispatched alerts.
///
/// Returns `(state, captured, handle)`. Callers that need to assert on
/// `captured` must drop all `AlertDispatcher` clones and `.await` the handle
/// before locking the vec to ensure the drainer task has flushed.
#[allow(clippy::type_complexity)]
pub fn test_state_with_capturing_alerter(
    db: &brenn_db::Db,
) -> (
    AppState,
    Arc<std::sync::Mutex<Vec<(String, String)>>>,
    tokio::task::JoinHandle<()>,
) {
    let (alert_dispatcher, captured, handle) = make_capturing_alerter();
    let mut state = test_state(db);
    state.alert_dispatcher = alert_dispatcher;
    (state, captured, handle)
}

/// Build a router with security (rate limiting) + one trusted proxy hop, so the
/// rate-limit integration tests can drive client IPs via `X-Forwarded-For`.
/// Uses a per-test SecurityConfig with very tight buckets so the integration
/// test is fast. Returns (router, db).
pub fn test_app_security(sec: &SecurityConfig) -> (Router, brenn_db::Db) {
    let db = crate::test_support::init_db_memory();
    let state = test_state(&db);
    let app = build_router(state, Some(sec), 1, 2576)
        .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))));
    (app, db)
}

/// Build a test app with a tempdir-backed static_dir containing
/// the named files. Used by tests that exercise `/static/main.js` /
/// `/static/app.css` so the underlying `ServeFile` finds something
/// (or 404s on an empty list). Returns (router, db, tempdir); the
/// tempdir must outlive the router or files vanish on drop.
pub fn test_app_with_static_dir(
    files: &[(&str, &[u8])],
) -> (Router, brenn_db::Db, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    for (name, contents) in files {
        std::fs::write(tmp.path().join(name), contents).unwrap();
    }
    let db = crate::test_support::init_db_memory();
    let mut state = test_state(&db);
    state.static_dir = tmp.path().to_path_buf();
    let app = build_router(state, None, 0, 2576)
        .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))));
    (app, db, tmp)
}

/// Build a test app with a tempdir-backed surface kernel root containing the
/// named files, for tests exercising the `/surface-static` asset tree. A name
/// may carry directories (`processor/<kind>/…`), and every kind so named is
/// mapped to the same root, which is the single-root arrangement. Returns
/// (router, db, tempdir); the tempdir must outlive the router or files vanish
/// on drop.
pub fn test_app_with_surface_roots(
    files: &[(&str, &[u8])],
) -> (Router, brenn_db::Db, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let mut kinds = std::collections::BTreeMap::new();
    for (name, contents) in files {
        let path = tmp.path().join(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&path, contents).unwrap();
        if let Some(kind) = name
            .strip_prefix("processor/")
            .and_then(|rest| rest.split('/').next())
        {
            kinds.insert(kind.to_string(), tmp.path().to_path_buf());
        }
    }
    let db = crate::test_support::init_db_memory();
    let mut state = test_state(&db);
    state.surface_roots = brenn_surface_server::SurfaceRoots {
        kernel: Some(tmp.path().to_path_buf()),
        kinds,
    };
    let app = build_router(state, None, 0, 2576)
        .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))));
    (app, db, tmp)
}

/// Create a test app whose working_dir is a real temp directory with files.
pub fn test_app_with_working_dir(working_dir: PathBuf) -> (Router, brenn_db::Db) {
    let db = crate::test_support::init_db_memory();
    let mut cfg = default_test_app_config("test", "Test App");
    cfg.working_dir = working_dir;
    let mut apps = IndexMap::new();
    apps.insert("test".to_string(), cfg);
    let state = test_state_with_apps(&db, Arc::new(apps));
    let app = build_router(state, None, 0, 2576)
        .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))));
    (app, db)
}

/// Create a test app with one named mount at the given host path.
/// `app_slug` and `allowed_users` let callers vary the two things that
/// distinguish the happy-path and access-denied tests.
pub fn test_app_with_mount(
    app_slug: &str,
    working_dir: PathBuf,
    mount_slug: &str,
    mount_host_path: PathBuf,
    allowed_users: Vec<String>,
) -> (Router, brenn_db::Db) {
    let db = crate::test_support::init_db_memory();
    let mut cfg = default_test_app_config(app_slug, "Test App");
    cfg.working_dir = working_dir;
    cfg.allowed_users = allowed_users;
    cfg.mounts = vec![brenn_lib::config::ResolvedMount {
        slug: mount_slug.to_string(),
        host_path: mount_host_path,
        container_path: None,
        access: brenn_lib::config::AccessLevel::ReadWrite,
        auto_pull: false,
        is_working_dir: false,
        primary: false,
    }];
    let mut apps = IndexMap::new();
    apps.insert(app_slug.to_string(), cfg);
    let state = test_state_with_apps(&db, Arc::new(apps));
    let app = build_router(state, None, 0, 2576)
        .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))));
    (app, db)
}

/// Build an `AppState` with a single app and an optional pre-seeded user.
/// Used by `mqtt_router` tests and other modules that need a user+app pair
/// without constructing a full HTTP router.
///
/// Returns `(state, db, user_id)` where `user_id` is `Some` if
/// `allowed_users` is non-empty (the first entry is created in the DB).
///
/// The state enforces exactly one app being present, matching the assumption
/// of conversation-cache tests that index on a single slug.
pub fn test_state_with_user_and_app(
    app_slug: &str,
    allowed_users: Vec<String>,
) -> (AppState, brenn_db::Db, Option<i64>) {
    use brenn_db::auth::user::create_user;
    let db = crate::test_support::init_db_memory();
    let user_id = allowed_users.first().map(|username| {
        let conn = db.try_lock().expect("db lock");
        create_user(&conn, username, "hash")
    });
    let mut cfg = default_test_app_config(app_slug, app_slug);
    cfg.allowed_users = allowed_users;
    let mut apps = IndexMap::new();
    apps.insert(app_slug.to_string(), cfg);
    let mut state = test_state(&db);
    state.apps = Arc::new(apps);
    assert_eq!(
        state.apps.len(),
        1,
        "test_state_with_user_and_app: expected exactly 1 app"
    );
    (state, db, user_id)
}

/// Build a router targeting the multi-app landing page (two apps so
/// `/` renders the selector instead of redirecting to the single
/// app). Returns (router, db, session_token).
pub async fn landing_page_router_two_apps() -> (Router, brenn_db::Db, String) {
    use super::app_config::test_apps_multi;
    use super::http::setup_authenticated_user;
    let db = crate::test_support::init_db_memory();
    let mut state = test_state(&db);
    state.apps = test_apps_multi(&["alpha", "beta"]);
    let app = build_router(state, None, 0, 2576)
        .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))));
    let (session_token, _) = setup_authenticated_user(&db).await;
    (app, db, session_token)
}
