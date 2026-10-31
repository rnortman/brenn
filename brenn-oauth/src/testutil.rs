//! Test fixtures: a provider pointed at a stub authorization server on
//! `127.0.0.1`, and the service over a scratch vault.

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use brenn_lib::config::{
    ApiKeyProviderConfig, OAuthProviderConfig, OAuthProviderRaw, ResolvedOAuth, SecretString,
};

use crate::OAuthService;

/// A provider named `name` whose token and userinfo endpoints live under
/// `base` (`http://127.0.0.1:<port>`).
pub fn provider_config(name: &str, base: &str) -> (String, OAuthProviderConfig) {
    let config = OAuthProviderConfig {
        raw: OAuthProviderRaw {
            app_name: "Brenn Test".to_string(),
            ..brenn_lib::config::test_oauth_provider_raw(base)
        },
        client_id: "client-id".to_string(),
        client_secret: SecretString::new("client-secret".to_string()),
        redirect_uri: format!("https://brenn.example.com/oauth/{name}/callback"),
        api_origins: vec![{
            let url = url::Url::parse(base).unwrap();
            (brenn_lib::config::origin_host(&url), url)
        }],
    };
    (name.to_string(), config)
}

/// The key every [`key_provider_config`] provider sends.
pub const KEY: &str = "K3Y-deployment-secret";

/// A key provider named `name` whose one API origin is `base`, sending
/// [`KEY`] in the header `test_api_key_provider_raw` names. Tests needing
/// another header or limit override the fields with struct update.
pub fn key_provider_config(name: &str, base: &str) -> (String, ApiKeyProviderConfig) {
    let url = url::Url::parse(base).unwrap();
    let raw = brenn_lib::config::test_api_key_provider_raw(base);
    let header = reqwest::header::HeaderName::from_bytes(raw.header.as_bytes()).unwrap();
    let config = ApiKeyProviderConfig {
        raw,
        key: SecretString::new(KEY.to_string()),
        header,
        api_origins: vec![(brenn_lib::config::origin_host(&url), url)],
    };
    (name.to_string(), config)
}

/// The service over `providers` and a fresh 0700 vault, its alerts dropped.
pub fn service(
    providers: Vec<(String, OAuthProviderConfig)>,
) -> (Arc<OAuthService>, tempfile::TempDir) {
    let (alerts, _drainer) = brenn_obs::alerting::noop_alert_dispatcher();
    service_with_alerts(providers, alerts)
}

/// The service over `providers` and a fresh 0700 vault, alerting to `alerts`.
pub fn service_with_alerts(
    providers: Vec<(String, OAuthProviderConfig)>,
    alerts: brenn_obs::alerting::AlertDispatcher,
) -> (Arc<OAuthService>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let providers = providers.into_iter().collect();
    let svc = OAuthService::new(
        ResolvedOAuth {
            vault_dir: dir.path().to_path_buf(),
            providers,
        },
        alerts,
    );
    (Arc::new(svc), dir)
}

/// What the stub answers and what it saw.
#[derive(Default)]
pub struct StubState {
    pub token_status: Option<u16>,
    pub token_body: Option<serde_json::Value>,
    pub userinfo_body: Option<serde_json::Value>,
    /// Every form posted to `/token`, decoded.
    pub token_forms: Vec<BTreeMap<String, String>>,
    /// Every `Authorization` header sent to `/userinfo`.
    pub userinfo_auth: Vec<String>,
    pub revoke_status: Option<u16>,
    pub revoke_body: Option<serde_json::Value>,
    /// Every form posted to `/revoke`, decoded.
    pub revoke_forms: Vec<BTreeMap<String, String>>,
    /// Statuses for successive API requests (any other path) that
    /// `api_failures` does not answer; 200 once exhausted.
    pub api_statuses: std::collections::VecDeque<u16>,
    /// Status and body for every API request to an exact path, query
    /// excluded; ahead of `api_statuses` and `api_bodies`.
    pub api_failures: BTreeMap<String, (u16, Vec<u8>)>,
    /// Extra headers on every API response.
    pub api_headers: Vec<(String, String)>,
    /// The body of every API response whose path has no entry in
    /// `api_bodies`.
    pub api_body: Vec<u8>,
    /// API response bodies by exact request path, query excluded.
    pub api_bodies: BTreeMap<String, Vec<u8>>,
    /// Every API request, as received.
    pub api_seen: Vec<ApiSeen>,
}

/// One API request the stub received.
#[derive(Clone, Debug)]
pub struct ApiSeen {
    pub method: String,
    /// The path and query exactly as sent.
    pub path_and_query: String,
    pub headers: HeaderMap,
    pub body: Vec<u8>,
}

pub type Stub = Arc<Mutex<StubState>>;

/// Start the stub on an ephemeral port; returns its base URL and its state.
pub async fn start_stub() -> (String, Stub) {
    let stub: Stub = Arc::default();
    let app = Router::new()
        .route("/token", post(token))
        .route("/userinfo", get(userinfo))
        .route("/revoke", post(revoke))
        .fallback(api)
        .with_state(stub.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), stub)
}

async fn token(State(stub): State<Stub>, body: String) -> impl IntoResponse {
    let mut s = stub.lock().unwrap();
    s.token_forms.push(
        url::form_urlencoded::parse(body.as_bytes())
            .into_owned()
            .collect(),
    );
    let status = StatusCode::from_u16(s.token_status.unwrap_or(200)).unwrap();
    let body = s.token_body.clone().unwrap_or_else(|| {
        serde_json::json!({
            "access_token": "AT-1",
            "refresh_token": "RT-1",
            "expires_in": 3599,
            "scope": "openid mail.read",
            "token_type": "Bearer",
        })
    });
    (status, axum::Json(body))
}

async fn userinfo(State(stub): State<Stub>, headers: HeaderMap) -> impl IntoResponse {
    let mut s = stub.lock().unwrap();
    s.userinfo_auth.push(
        headers
            .get("authorization")
            .map(|v| v.to_str().unwrap().to_string())
            .unwrap_or_default(),
    );
    axum::Json(
        s.userinfo_body
            .clone()
            .unwrap_or_else(|| serde_json::json!({"sub": "1", "email": "alice@example.com"})),
    )
}

async fn revoke(State(stub): State<Stub>, body: String) -> impl IntoResponse {
    let mut s = stub.lock().unwrap();
    s.revoke_forms.push(
        url::form_urlencoded::parse(body.as_bytes())
            .into_owned()
            .collect(),
    );
    let status = StatusCode::from_u16(s.revoke_status.unwrap_or(200)).unwrap();
    let body = s
        .revoke_body
        .clone()
        .unwrap_or_else(|| serde_json::json!({}));
    (status, axum::Json(body))
}

async fn api(
    State(stub): State<Stub>,
    method: axum::http::Method,
    uri: axum::http::Uri,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let mut s = stub.lock().unwrap();
    s.api_seen.push(ApiSeen {
        method: method.to_string(),
        path_and_query: uri
            .path_and_query()
            .map(ToString::to_string)
            .unwrap_or_default(),
        headers,
        body: body.to_vec(),
    });
    let failure = s.api_failures.get(uri.path()).cloned();
    let status = match &failure {
        Some((status, _)) => *status,
        None => s.api_statuses.pop_front().unwrap_or(200),
    };
    let status = StatusCode::from_u16(status).unwrap();
    let mut response_headers = HeaderMap::new();
    for (name, value) in &s.api_headers {
        response_headers.append(
            axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
            value.parse().unwrap(),
        );
    }
    let body = match failure {
        Some((_, body)) => body,
        None => s.api_bodies.get(uri.path()).unwrap_or(&s.api_body).clone(),
    };
    (status, response_headers, body)
}

/// The Brenn user the fixtures act for.
pub fn alice() -> brenn_tool_registry::ActingUser {
    brenn_tool_registry::ActingUser {
        id: 1,
        username: "alice".to_string(),
    }
}

/// A conversation caller's context: `user` acting, `acl` the grant.
pub fn tool_ctx(
    user: Option<brenn_tool_registry::ActingUser>,
    acl: Vec<brenn_lib::tools::AclClause>,
) -> brenn_tool_registry::ToolCtx {
    brenn_tool_registry::ToolCtx {
        caller: brenn_lib::messaging::ParticipantId::for_app("pa", "test-origin"),
        grant: brenn_lib::tools::ResolvedToolGrant {
            acl,
            rate_limit: None,
        },
        acting_conversation_id: Some(7),
        acting_user: user,
        deadline: None,
    }
}

/// An ACL clause constraining `provider` alone.
pub fn provider_clause(provider: &str) -> brenn_lib::tools::AclClause {
    brenn_lib::tools::AclClause::new(BTreeMap::from([(
        "provider".to_string(),
        provider.to_string(),
    )]))
}

/// A credential for `account` at `provider`, its access token valid an hour.
pub fn credential(provider: &str, account: &str, refresh: &str) -> crate::vault::Credential {
    let now = chrono::Utc::now();
    crate::vault::Credential::new(
        provider,
        account,
        refresh.to_string(),
        "access".to_string(),
        now + chrono::Duration::hours(1),
        vec!["openid".to_string(), "mail.read".to_string()],
        now,
    )
}
