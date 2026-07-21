use super::*;
use crate::db::init_db_memory;
use crate::messaging::config::{MessagingGlobalConfig, ResolvedMessagingConfig};
use crate::pwa_push::config::{AppPwaPushBlock, ResolvedPwaPushConfig};
use crate::pwa_push::vapid::load_or_generate;
use base64ct::{Base64UrlUnpadded, Encoding as _};
use chrono::Utc;
use indexmap::IndexMap;
use std::sync::Arc;
use std::time::Duration;

mod device;
mod fanout;
mod gate;
mod get_target;
mod list_targets;
mod persistence;

pub(super) fn make_pwa_push_config() -> ResolvedPwaPushConfig {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("vapid.json");
    let vapid = load_or_generate(&path);
    ResolvedPwaPushConfig {
        vapid,
        subject: "mailto:test@example.com".to_string(),
        endpoint_policy: crate::pwa_push::endpoint_validator::EndpointPolicy::new(vec![], false),
    }
}

pub(super) fn make_app_config(
    slug: &str,
    pwa_push_enabled: bool,
    allowed_users: Vec<String>,
) -> AppConfig {
    let dir = tempfile::tempdir().unwrap();
    AppConfig {
        slug: slug.to_string(),
        name: slug.to_string(),
        description: String::new(),
        icon: String::new(),
        working_dir: dir.path().to_path_buf(),
        model: String::new(),
        single_instance: false,
        singleton: false,
        persistent: false,
        idle_timeout: None,
        compaction: None,
        idle_hook_secs: 0,
        allowed_users,
        disabled_tools: vec![],
        mcp_servers: Default::default(),
        multiuser: false,
        prefix_username: false,
        prefix_timestamp: false,
        prefix_device: true,
        path_mapper: crate::config::PathMapper::Identity,
        container_spawn: None,
        start_hooks: Default::default(),
        post_pull_hooks: Default::default(),
        startup_hooks: Default::default(),
        cc_extra_args: vec![],
        approval_rules: vec![],
        attachment_targets: vec![],
        integrations: Default::default(),
        mounts: vec![],
        history_replay_limit: 100,
        frontmatter: Default::default(),
        state_dir: dir.path().to_path_buf(),
        messaging: Some(ResolvedMessagingConfig {
            send_budget: 100,
            subscriptions: vec![],
        }),
        messaging_default_send_budget: 100,
        // Grant PwaPush exactly when this fixture wants push enabled, so
        // pwa_push_enabled() reflects the intended state.
        policy: {
            let mut p = crate::access::AppPolicy::default();
            if pwa_push_enabled {
                p.grants.insert(crate::access::AppCapability::PwaPush);
            }
            p
        },
        pwa_push: if pwa_push_enabled {
            Some(AppPwaPushBlock {
                default_title: Some("Test App".to_string()),
            })
        } else {
            None
        },
        webhook_subscriptions: vec![],
        mqtt_subscriptions: vec![],
    }
}

pub(super) async fn make_db_with_users() -> crate::db::Db {
    let db = init_db_memory();
    {
        let conn = db.lock().await;
        let now = crate::db::format_ts_for_db(Utc::now());
        conn.execute_batch(&format!(
            "
                INSERT INTO users (id, username, password_hash, created_at)
                    VALUES (1, 'alice', 'hash', '{now}');
                INSERT INTO users (id, username, password_hash, created_at)
                    VALUES (2, 'bob', 'hash', '{now}');
                INSERT INTO devices (id, token, guessed_slug, last_seen_at, created_at)
                    VALUES (1, 'tok1', 'laptop', '{now}', '{now}');
                INSERT INTO devices (id, token, guessed_slug, last_seen_at, created_at)
                    VALUES (2, 'tok2', 'phone', '{now}', '{now}');
                INSERT INTO device_users (device_id, user_id, first_seen_at, last_seen_at)
                    VALUES (1, 1, '{now}', '{now}');
                INSERT INTO device_users (device_id, user_id, first_seen_at, last_seen_at)
                    VALUES (2, 1, '{now}', '{now}');
                -- Conversation for budget tracking
                INSERT INTO conversations (id, user_id, status, app_slug, created_at, updated_at)
                    VALUES (1, 1, 'active', 'graf', '2024-01-01', '2024-01-01');
                "
        ))
        .expect("test setup");
    }
    db
}

pub(super) fn make_service_with_apps(
    db: crate::db::Db,
    apps: IndexMap<String, AppConfig>,
) -> Arc<PwaPushService> {
    Arc::new(PwaPushService::new(
        db,
        make_pwa_push_config(),
        Arc::new(apps),
        MessagingGlobalConfig {
            default_send_budget: 100,
            max_body_bytes: 4096,
            ..MessagingGlobalConfig::default()
        },
        Arc::from("https://brenn.test"),
        crate::obs::alerting::noop_alert_dispatcher().0,
    ))
}

pub(super) fn make_service(db: crate::db::Db) -> Arc<PwaPushService> {
    let mut apps = IndexMap::new();
    apps.insert("graf".to_string(), make_app_config("graf", true, vec![]));
    apps.insert("other".to_string(), make_app_config("other", false, vec![]));
    make_service_with_apps(db, apps)
}

/// Generate a random valid P-256 public key (SEC1 uncompressed, 65 bytes)
/// encoded as base64url. Used by fanout tests that need `deliver_to_subscription`
/// to get past the key-parse step.
pub(super) fn valid_p256dh() -> String {
    use web_push_native::p256::SecretKey;
    // Use a fixed seed so tests are deterministic.
    let sk = SecretKey::from_slice(&[
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e,
        0x1f, 0x20,
    ])
    .expect("fixed scalar must produce a valid P-256 key");
    let pk_bytes = sk.public_key().to_sec1_bytes();
    Base64UrlUnpadded::encode_string(&pk_bytes)
}

pub(super) fn valid_auth() -> String {
    Base64UrlUnpadded::encode_string(&[0x01u8; 16])
}

// -----------------------------------------------------------------------
// Shared HTTP mock (used by fanout.rs and the device-targeted tests below).
// -----------------------------------------------------------------------

use std::collections::HashMap as StdHashMap;

/// What a `MockHttpPoster` endpoint should do when hit.
pub(super) enum MockResponse {
    /// Return an HTTP response with the given status after `delay`.
    Ok { status: u16, delay: Duration },
    /// Sleep `delay` then return a `reqwest::Error` — exercises the
    /// `Err(reqwest_error)` → `DeliveryOutcome::Failed` branch in
    /// `deliver_to_subscription`.
    NetworkError { delay: Duration },
    /// Never resolve — exercises the publish-wide cap.
    Hang,
    /// Panic inside the task body — exercises `JoinError::is_panic`.
    Panic,
}

/// Test-only `HttpPoster` that returns configurable responses per endpoint.
pub(super) struct MockHttpPoster {
    /// Keyed by endpoint URL prefix (just enough to distinguish).
    responses: StdHashMap<String, MockResponse>,
    /// Every request passed to `execute`, in the order `execute` observed them.
    /// A `std` mutex — the lock is taken and dropped synchronously inside
    /// `execute`, never held across an await, so it cannot deadlock the runtime.
    captured: std::sync::Mutex<Vec<reqwest::Request>>,
}

impl MockHttpPoster {
    pub(super) fn new(responses: StdHashMap<String, MockResponse>) -> Arc<Self> {
        Arc::new(Self {
            responses,
            captured: std::sync::Mutex::new(Vec::new()),
        })
    }

    /// Drain and return every request seen by `execute` so far.
    ///
    /// Order reflects `execute` call order. With concurrent multi-subscription
    /// fanout this order is nondeterministic; callers asserting on ordering must
    /// pin themselves to a single subscription (or sort/match by URL).
    pub(super) fn take_captured(&self) -> Vec<reqwest::Request> {
        std::mem::take(
            &mut self
                .captured
                .lock()
                .expect("captured mutex is never held across a panic"),
        )
    }
}

#[async_trait::async_trait]
impl HttpPoster for MockHttpPoster {
    async fn execute(&self, req: reqwest::Request) -> reqwest::Result<reqwest::Response> {
        let url = req.url().to_string();
        // Record the outbound request before dispatching. The trait takes the
        // request by value and no response arm below uses it past this URL
        // lookup, so capturing by move here needs no clone. The lock is released
        // before any await (and before the `Panic` arm), so it never poisons.
        self.captured
            .lock()
            .expect("captured mutex is never held across a panic")
            .push(req);
        // Find the matching config (exact URL match).
        let mock = self
            .responses
            .get(&url)
            .unwrap_or_else(|| panic!("MockHttpPoster: unexpected URL {url}"));

        match mock {
            MockResponse::Ok { status, delay } => {
                tokio::time::sleep(*delay).await;
                let resp = http::Response::builder()
                    .status(*status)
                    .body(bytes::Bytes::new())
                    .expect("build mock response");
                Ok(reqwest::Response::from(resp))
            }
            MockResponse::NetworkError { delay } => {
                tokio::time::sleep(*delay).await;
                // Produce a reqwest::Error without any I/O by attempting to
                // build a request for an invalid URL. `reqwest::Client::get`
                // with a syntactically-invalid URI surfaces an error at
                // `.send()` time without opening a socket.
                Err(reqwest::Client::new()
                    .get("not-a-valid-url://\x00")
                    .send()
                    .await
                    .expect_err("invalid URL must fail without I/O"))
            }
            MockResponse::Hang => {
                // Await forever — only an outer cancellation can stop this.
                std::future::pending::<()>().await;
                unreachable!()
            }
            MockResponse::Panic => {
                panic!("mock task panic");
            }
        }
    }
}

// All callers of this helper
// pass a `MockHttpPoster`, so the redirect policy is irrelevant. If a future
// test passes a real `ReqwestPoster` here, construct it with
// `redirect::Policy::none()` to mirror the production semantics set by
// `PwaPushService::new` — otherwise redirect-following would silently diverge
// from the production client.
pub(super) fn make_service_with_poster(
    db: crate::db::Db,
    poster: Arc<dyn HttpPoster>,
) -> Arc<PwaPushService> {
    let mut apps = IndexMap::new();
    apps.insert("graf".to_string(), make_app_config("graf", true, vec![]));
    Arc::new(PwaPushService::new_with_poster(
        db,
        make_pwa_push_config(),
        Arc::new(apps),
        MessagingGlobalConfig {
            default_send_budget: 100,
            max_body_bytes: 4096,
            ..MessagingGlobalConfig::default()
        },
        Arc::from("https://brenn.test"),
        crate::obs::alerting::noop_alert_dispatcher().0,
        poster,
    ))
}
