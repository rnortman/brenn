//! `GET /oauth/{provider}/callback` — the provider's redirect back after the
//! user answered its consent screen.
//!
//! The route sits behind `require_auth`, so the handler always holds the
//! session of the browser that carried the redirect. A browser with no session
//! is sent through the login page and returned here by `next`.
//!
//! The query string is composed by the authorization server, not by Brenn:
//! only the values Brenn minted (`state`) or relies on (`code`, `error`) are
//! held to a shape, and a malformed one is a schema violation. Every other key
//! is the provider's vocabulary and is ignored without an event.

use axum::Extension;
use axum::extract::{Path, RawQuery, State};
use axum::http::{StatusCode, Uri, header};
use axum::response::{Html, IntoResponse, Response};
use brenn_db::auth::session::Session;
use brenn_oauth::CallbackOutcome;
use brenn_oauth::flow::parse_callback_query;
use brenn_obs::security::{SecurityEventType, log_and_alert_security_event};
use tracing::{debug, info, warn};

use crate::client_ip::ClientIp;
use crate::routes::html_escape;
use crate::state::AppState;

/// Complete the connect flow the redirect's `state` names.
pub async fn callback(
    Extension(client_ip): Extension<ClientIp>,
    Extension(session): Extension<Session>,
    State(state): State<AppState>,
    Path(provider): Path<String>,
    RawQuery(raw_query): RawQuery,
    uri: Uri,
) -> Response {
    let ip = client_ip.0;
    let Some(service) = state
        .oauth
        .as_ref()
        .filter(|service| service.provider(&provider).is_some())
    else {
        return crate::router::not_found(Extension(client_ip), uri)
            .await
            .into_response();
    };

    let query = match parse_callback_query(raw_query.as_deref()) {
        Ok(query) => query,
        Err(violation) => {
            log_and_alert_security_event(
                &state.alert_dispatcher,
                SecurityEventType::SchemaViolation,
                ip,
                &format!("oauth callback for {provider}: {}", violation.0),
            );
            return (StatusCode::BAD_REQUEST, "invalid query").into_response();
        }
    };
    if !query.ignored.is_empty() {
        debug!(provider = %provider, keys = ?query.ignored, "oauth callback: ignored provider parameters");
    }

    let app_name = service
        .provider(&provider)
        .expect("checked above")
        .raw
        .app_name
        .clone();
    let user = &session.user;
    match service.complete_callback(&provider, &query, user.id).await {
        CallbackOutcome::Connected { account } => {
            info!(provider = %provider, user = %user.username, account = %account, "oauth callback: connected");
            page(
                StatusCode::OK,
                "Connected",
                &format!(
                    "Connected <strong>{}</strong> to {} for Brenn user <strong>{}</strong>. \
                     Go back to your assistant.",
                    html_escape(&account),
                    html_escape(&app_name),
                    html_escape(&user.username),
                ),
            )
        }
        CallbackOutcome::Expired => {
            warn!(provider = %provider, user = %user.username, "oauth callback: no pending flow for this state");
            page(
                StatusCode::OK,
                "Link expired",
                "This link has expired or was already used. Ask your assistant to start again.",
            )
        }
        CallbackOutcome::WrongUser { flow_user } => {
            log_and_alert_security_event(
                &state.alert_dispatcher,
                SecurityEventType::AuthFailure,
                ip,
                &format!(
                    "oauth callback for {provider}: flow started by {flow_user} carried by the session of {}",
                    user.username
                ),
            );
            page(
                StatusCode::FORBIDDEN,
                "Wrong account",
                "This link was started by another Brenn user. Nothing was connected.",
            )
        }
        CallbackOutcome::ProviderMismatch { flow_provider } => {
            log_and_alert_security_event(
                &state.alert_dispatcher,
                SecurityEventType::SchemaViolation,
                ip,
                &format!(
                    "oauth callback for {provider}: the state belongs to a {flow_provider} flow"
                ),
            );
            (StatusCode::BAD_REQUEST, "invalid query").into_response()
        }
        CallbackOutcome::Declined { error, description } => {
            info!(provider = %provider, user = %user.username, error = %error, "oauth callback: the provider reports access was not granted");
            let description = description
                .map(|d| format!(" ({})", html_escape(&d)))
                .unwrap_or_default();
            page(
                StatusCode::OK,
                "Nothing was connected",
                &format!(
                    "Nothing was connected. {} answered <code>{}</code>{description}.",
                    html_escape(&provider),
                    html_escape(&error),
                ),
            )
        }
        CallbackOutcome::NoRefreshToken => page(
            StatusCode::OK,
            "Nothing was connected",
            &format!(
                "{} did not issue a long-lived credential, so nothing was connected. \
                 Remove {} from the apps that can access your account (for Google: \
                 myaccount.google.com/permissions), then ask your assistant to start again.",
                html_escape(&provider),
                html_escape(&app_name),
            ),
        ),
        CallbackOutcome::ProviderUnavailable { detail } => page(
            StatusCode::OK,
            "Try again",
            &format!(
                "{} did not answer ({}). Ask your assistant to start again.",
                html_escape(&provider),
                html_escape(&detail),
            ),
        ),
        CallbackOutcome::ProviderContract { .. } => page(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Something went wrong",
            &format!(
                "{} answered in a way Brenn does not understand, so nothing was connected. \
                     The operator has been told.",
                html_escape(&provider),
            ),
        ),
        CallbackOutcome::ClientRefused { .. } => page(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Something went wrong",
            &format!(
                "{} refused Brenn's own credentials, so nothing was connected. The operator \
                 has been told; trying again will not help until they fix it.",
                html_escape(&provider),
            ),
        ),
    }
}

/// A static page: no script, never cached. `body_html` is already escaped.
fn page(status: StatusCode, title: &str, body_html: &str) -> Response {
    let html = format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Brenn — {title}</title>
    <link rel="stylesheet" href="/auth/static/auth.css">
</head>
<body>
    <main>
        <h1>{title}</h1>
        <p>{body_html}</p>
    </main>
</body>
</html>"#,
        title = html_escape(title),
    );
    (status, [(header::CACHE_CONTROL, "no-store")], Html(html)).into_response()
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    use axum::Router;
    use axum::body::Body;
    use axum::extract::connect_info::MockConnectInfo;
    use axum::http::{Request, StatusCode};
    use brenn_oauth::OAuthService;
    use brenn_oauth::flow::{PendingFlow, mint_state};
    use brenn_oauth::testutil::{Stub, provider_config, service_with_alerts, start_stub};
    use brenn_tool_registry::ActingUser;
    use tower::ServiceExt;

    use crate::router::build_router;
    use crate::test_support::http::{TEST_USERNAME, body_string, setup_authenticated_user};
    use crate::test_support::state::test_state_with_capturing_alerter;

    struct Fixture {
        app: Router,
        cookie: String,
        user_id: i64,
        svc: Arc<OAuthService>,
        stub: Stub,
        captured: Arc<Mutex<Vec<(String, String)>>>,
        alerts: tokio::task::JoinHandle<()>,
        _vault: tempfile::TempDir,
    }

    impl Fixture {
        async fn new() -> Self {
            let db = crate::test_support::init_db_memory();
            let (session, _csrf) = setup_authenticated_user(&db).await;
            let user_id =
                brenn_db::auth::user::get_user_credentials(&*db.lock().await, TEST_USERNAME)
                    .expect("test user exists")
                    .0;
            let (base, stub) = start_stub().await;
            let (mut state, captured, alerts) = test_state_with_capturing_alerter(&db);
            let (svc, vault) = service_with_alerts(
                vec![
                    provider_config("google", &base),
                    provider_config("other", &base),
                ],
                state.alert_dispatcher.clone(),
            );
            state.oauth = Some(svc.clone());
            let app = build_router(state, None, 0, 2576)
                .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))));
            Self {
                app,
                cookie: format!("brenn_session={session}"),
                user_id,
                svc,
                stub,
                captured,
                alerts,
                _vault: vault,
            }
        }

        /// Start a `google` flow for `user_id` and return its state.
        fn start(&self, user_id: i64, username: &str) -> String {
            let state = mint_state();
            self.svc.flows().insert(
                state.clone(),
                PendingFlow {
                    provider: "google".to_string(),
                    user: ActingUser {
                        id: user_id,
                        username: username.to_string(),
                    },
                    conversation_id: Some(1),
                    verifier: "the-verifier".to_string(),
                    scopes: vec!["openid".to_string()],
                    created_at: Instant::now(),
                },
            );
            state
        }

        fn start_own(&self) -> String {
            self.start(self.user_id, TEST_USERNAME)
        }

        async fn get(&self, uri: &str) -> (StatusCode, axum::http::HeaderMap, String) {
            let response = self
                .app
                .clone()
                .oneshot(
                    Request::get(uri)
                        .header("cookie", &self.cookie)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let status = response.status();
            let headers = response.headers().clone();
            (status, headers, body_string(response.into_body()).await)
        }

        async fn stored(&self, account: &str) -> bool {
            let vault = self.svc.vault();
            vault
                .read(&vault.credential_path("google", TEST_USERNAME, account))
                .await
                .is_some()
        }

        /// Every alert raised, once the router and the service (and with them
        /// every dispatcher clone) are gone.
        async fn alerts(self) -> Vec<(String, String)> {
            drop(self.app);
            drop(self.svc);
            self.alerts.await.unwrap();
            self.captured.lock().unwrap().clone()
        }
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn a_callback_with_a_code_stores_the_credential_and_ignores_provider_keys() {
        let f = Fixture::new().await;
        let state = f.start_own();
        let (status, headers, body) = f
            .get(&format!(
                "/oauth/google/callback?state={state}&code=the-code&scope=openid&iss=https%3A%2F%2Fissuer&hd=example.com&made_up=1"
            ))
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(headers["cache-control"], "no-store");
        assert!(
            body.contains(&format!(
                "Connected <strong>alice@example.com</strong> to Brenn Test for Brenn user \
                 <strong>{TEST_USERNAME}</strong>."
            )),
            "{body}"
        );
        assert!(!body.contains("<script"));
        assert!(f.stored("alice@example.com").await);
        assert_eq!(f.stub.lock().unwrap().token_forms[0]["code"], "the-code");
        assert!(!logs_contain("schema_violation"));
        assert!(f.alerts().await.is_empty());
    }

    #[tokio::test]
    async fn the_account_is_escaped_on_the_page() {
        let f = Fixture::new().await;
        f.stub.lock().unwrap().userinfo_body =
            Some(serde_json::json!({"email": "<b>x</b>@example.com"}));
        let state = f.start_own();
        let (status, _, body) = f
            .get(&format!("/oauth/google/callback?state={state}&code=c"))
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("&lt;b&gt;x&lt;/b&gt;@example.com"), "{body}");
        assert!(!body.contains("<b>x"));
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn a_malformed_query_is_a_400_and_a_schema_violation() {
        let f = Fixture::new().await;
        let state = f.start_own();
        for query in [
            "code=c".to_string(),
            format!("state={state}&state={state}&code=c"),
            "state=NOT-HEX&code=c".to_string(),
            format!("state={state}&code=a&code=b"),
        ] {
            let (status, _, _) = f.get(&format!("/oauth/google/callback?{query}")).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{query}");
        }
        assert!(logs_contain("schema_violation"));
        assert!(
            f.svc.flows().take(&state).is_some(),
            "a refused query does not consume the flow"
        );
        assert!(f.stub.lock().unwrap().token_forms.is_empty());
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn an_unknown_or_used_state_is_the_expired_page_without_an_event() {
        let f = Fixture::new().await;
        let (status, _, body) = f
            .get(&format!(
                "/oauth/google/callback?state={}&code=c",
                mint_state()
            ))
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("expired or was already used"), "{body}");
        assert!(!logs_contain("security_event"));

        let state = f.start_own();
        let uri = format!("/oauth/google/callback?state={state}&code=c");
        assert!(f.get(&uri).await.2.contains("Connected"));
        assert!(f.get(&uri).await.2.contains("expired or was already used"));
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn another_users_flow_is_a_403_and_an_auth_failure() {
        let f = Fixture::new().await;
        let state = f.start(f.user_id + 1, "bob");
        let (status, _, body) = f
            .get(&format!("/oauth/google/callback?state={state}&code=c"))
            .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert!(body.contains("another Brenn user"));
        assert!(logs_contain("auth_failure"));
        assert!(f.svc.flows().take(&state).is_none(), "the flow is gone");
        assert!(f.stub.lock().unwrap().token_forms.is_empty());
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn another_providers_path_is_a_400_and_a_schema_violation() {
        let f = Fixture::new().await;
        let state = f.start_own();
        let (status, _, _) = f
            .get(&format!("/oauth/other/callback?state={state}&code=c"))
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(logs_contain("schema_violation"));
        assert!(!f.stored("alice@example.com").await);
    }

    #[tokio::test]
    async fn a_declined_consent_connects_nothing() {
        let f = Fixture::new().await;
        let state = f.start_own();
        let (status, _, body) = f
            .get(&format!(
                "/oauth/google/callback?state={state}&error=access_denied&error_description=%3Cno%3E&error_uri=https%3A%2F%2Fx"
            ))
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("Nothing was connected"));
        assert!(
            body.contains("<code>access_denied</code> (&lt;no&gt;)"),
            "{body}"
        );
        assert!(!f.stored("alice@example.com").await);
    }

    #[tokio::test]
    async fn no_refresh_token_names_the_fix_and_stores_nothing() {
        let f = Fixture::new().await;
        f.stub.lock().unwrap().token_body =
            Some(serde_json::json!({"access_token": "AT", "expires_in": 60}));
        let state = f.start_own();
        let (status, _, body) = f
            .get(&format!("/oauth/google/callback?state={state}&code=c"))
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            body.contains("did not issue a long-lived credential"),
            "{body}"
        );
        assert!(body.contains("myaccount.google.com/permissions"));
        assert!(!f.stored("alice@example.com").await);
    }

    #[tokio::test]
    async fn a_token_endpoint_refusal_says_try_again() {
        let f = Fixture::new().await;
        f.stub.lock().unwrap().token_status = Some(400);
        let state = f.start_own();
        let (status, _, body) = f
            .get(&format!("/oauth/google/callback?state={state}&code=c"))
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("did not answer (HTTP 400)"), "{body}");
        assert!(f.alerts().await.is_empty());
    }

    #[tokio::test]
    async fn userinfo_without_the_account_is_a_500_and_an_alert() {
        let f = Fixture::new().await;
        f.stub.lock().unwrap().userinfo_body = Some(serde_json::json!({"sub": "1"}));
        let state = f.start_own();
        let (status, _, body) = f
            .get(&format!("/oauth/google/callback?state={state}&code=c"))
            .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(body.contains("The operator has been told"));
        let alerts = f.alerts().await;
        assert!(
            alerts
                .iter()
                .any(|(title, _)| title.contains("google broke the protocol")),
            "{alerts:?}"
        );
    }

    #[tokio::test]
    async fn an_out_of_range_expires_in_is_a_500_and_an_alert() {
        let f = Fixture::new().await;
        f.stub.lock().unwrap().token_body = Some(serde_json::json!({
            "access_token": "AT", "refresh_token": "RT", "expires_in": u64::MAX,
        }));
        let state = f.start_own();
        let (status, _, body) = f
            .get(&format!("/oauth/google/callback?state={state}&code=c"))
            .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(body.contains("The operator has been told"), "{body}");
        assert!(!f.stored("alice@example.com").await);
        let alerts = f.alerts().await;
        assert_eq!(alerts.len(), 1, "{alerts:?}");
        assert!(
            alerts[0].0.contains("google broke the protocol"),
            "{alerts:?}"
        );
        assert!(alerts[0].1.contains("out of range"), "{alerts:?}");
    }

    #[tokio::test]
    async fn a_refused_client_is_a_500_and_an_alert() {
        let f = Fixture::new().await;
        {
            let mut s = f.stub.lock().unwrap();
            s.token_status = Some(401);
            s.token_body = Some(serde_json::json!({"error": "invalid_client"}));
        }
        let state = f.start_own();
        let (status, _, body) = f
            .get(&format!("/oauth/google/callback?state={state}&code=c"))
            .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(body.contains("refused Brenn's own credentials"), "{body}");
        assert!(!f.stored("alice@example.com").await);
        let alerts = f.alerts().await;
        assert_eq!(alerts.len(), 1, "{alerts:?}");
        assert_eq!(
            alerts[0].0,
            "OAuth provider google refused Brenn's client credentials"
        );
        assert!(
            alerts[0].1.starts_with("token exchange: HTTP 401"),
            "{alerts:?}"
        );
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn an_unknown_provider_is_an_unrecognized_url() {
        let f = Fixture::new().await;
        let (status, _, _) = f
            .get(&format!(
                "/oauth/nope/callback?state={}&code=c",
                mint_state()
            ))
            .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(logs_contain("unrecognized_url"));
    }

    #[tokio::test]
    async fn without_oauth_every_callback_is_a_404() {
        let db = crate::test_support::init_db_memory();
        let (session, _csrf) = setup_authenticated_user(&db).await;
        let app = build_router(crate::test_support::state::test_state(&db), None, 0, 2576)
            .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))));
        let response = app
            .oneshot(
                Request::get(format!("/oauth/google/callback?state={}", mint_state()))
                    .header("cookie", format!("brenn_session={session}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
