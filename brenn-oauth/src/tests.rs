//! `OAuthService::complete_callback` against the stub authorization server.

use std::time::Instant;

use brenn_tool_registry::ActingUser;

use crate::CallbackOutcome;
use crate::flow::{CallbackQuery, PendingFlow, mint_state};
use crate::testutil::{Stub, provider_config, service, start_stub};
use crate::{OAuthService, vault::Credential};

struct Fixture {
    svc: std::sync::Arc<OAuthService>,
    stub: Stub,
    _dir: tempfile::TempDir,
}

async fn fixture() -> Fixture {
    let (base, stub) = start_stub().await;
    let (svc, dir) = service(vec![
        provider_config("google", &base),
        provider_config("other", &base),
    ]);
    Fixture {
        svc,
        stub,
        _dir: dir,
    }
}

fn alice() -> ActingUser {
    ActingUser {
        id: 1,
        username: "alice".to_string(),
    }
}

/// Start a flow for alice at `google` and return its state.
fn start(svc: &OAuthService) -> String {
    let state = mint_state();
    svc.flows().insert(
        state.clone(),
        PendingFlow {
            provider: "google".to_string(),
            user: alice(),
            conversation_id: Some(3),
            verifier: "the-verifier".to_string(),
            scopes: vec!["openid".to_string(), "mail.read".to_string()],
            created_at: Instant::now(),
        },
    );
    state
}

fn code_query(state: &str) -> CallbackQuery {
    CallbackQuery {
        state: state.to_string(),
        code: Some("the-code".to_string()),
        error: None,
        error_description: None,
        ignored: vec![],
    }
}

async fn stored(svc: &OAuthService) -> Option<Credential> {
    let vault = svc.vault();
    vault
        .read(&vault.credential_path("google", "alice", "alice@example.com"))
        .await
}

#[tokio::test]
async fn success_exchanges_the_code_and_stores_the_credential() {
    let f = fixture().await;
    let state = start(&f.svc);
    let outcome = f
        .svc
        .complete_callback("google", &code_query(&state), 1)
        .await;
    assert_eq!(
        outcome,
        CallbackOutcome::Connected {
            account: "alice@example.com".to_string()
        }
    );

    let cred = stored(&f.svc).await.expect("credential written");
    assert_eq!(cred.refresh_token, "RT-1");
    assert_eq!(cred.access_token, "AT-1");
    assert_eq!(cred.scopes, vec!["openid", "mail.read"]);
    assert!(cred.expires_at > cred.obtained_at);

    {
        let s = f.stub.lock().unwrap();
        let form = &s.token_forms[0];
        assert_eq!(form["grant_type"], "authorization_code");
        assert_eq!(form["code"], "the-code");
        assert_eq!(form["code_verifier"], "the-verifier");
        assert_eq!(form["client_id"], "client-id");
        assert_eq!(form["client_secret"], "client-secret");
        assert_eq!(
            form["redirect_uri"],
            "https://brenn.example.com/oauth/google/callback"
        );
        assert_eq!(s.userinfo_auth, vec!["Bearer AT-1"]);
    }

    assert_eq!(
        f.svc
            .complete_callback("google", &code_query(&state), 1)
            .await,
        CallbackOutcome::Expired,
        "a flow is single use"
    );
}

#[tokio::test]
async fn absent_scope_means_the_requested_set() {
    let f = fixture().await;
    f.stub.lock().unwrap().token_body = Some(serde_json::json!({
        "access_token": "AT", "refresh_token": "RT", "expires_in": 60,
    }));
    let state = start(&f.svc);
    f.svc
        .complete_callback("google", &code_query(&state), 1)
        .await;
    assert_eq!(
        stored(&f.svc).await.unwrap().scopes,
        vec!["openid", "mail.read"]
    );
}

#[tokio::test]
async fn unknown_state_is_expired_and_calls_nothing() {
    let f = fixture().await;
    let outcome = f
        .svc
        .complete_callback("google", &code_query(&mint_state()), 1)
        .await;
    assert_eq!(outcome, CallbackOutcome::Expired);
    assert!(f.stub.lock().unwrap().token_forms.is_empty());
}

#[tokio::test]
async fn wrong_user_deletes_the_flow() {
    let f = fixture().await;
    let state = start(&f.svc);
    assert_eq!(
        f.svc
            .complete_callback("google", &code_query(&state), 2)
            .await,
        CallbackOutcome::WrongUser {
            flow_user: "alice".to_string()
        }
    );
    assert_eq!(
        f.svc
            .complete_callback("google", &code_query(&state), 1)
            .await,
        CallbackOutcome::Expired
    );
    assert!(f.stub.lock().unwrap().token_forms.is_empty());
}

#[tokio::test]
async fn another_providers_path_is_a_mismatch() {
    let f = fixture().await;
    let state = start(&f.svc);
    assert_eq!(
        f.svc
            .complete_callback("other", &code_query(&state), 1)
            .await,
        CallbackOutcome::ProviderMismatch {
            flow_provider: "google".to_string()
        }
    );
    assert!(stored(&f.svc).await.is_none());
}

#[tokio::test]
async fn provider_error_stores_nothing() {
    let f = fixture().await;
    let state = start(&f.svc);
    let query = CallbackQuery {
        code: None,
        error: Some("access_denied".to_string()),
        error_description: Some("no".to_string()),
        ..code_query(&state)
    };
    assert_eq!(
        f.svc.complete_callback("google", &query, 1).await,
        CallbackOutcome::Declined {
            error: "access_denied".to_string(),
            description: Some("no".to_string())
        }
    );
    assert!(stored(&f.svc).await.is_none());
    assert!(f.stub.lock().unwrap().token_forms.is_empty());
}

#[tokio::test]
async fn neither_code_nor_error_is_a_contract_violation() {
    let f = fixture().await;
    let state = start(&f.svc);
    let query = CallbackQuery {
        code: None,
        ..code_query(&state)
    };
    assert!(matches!(
        f.svc.complete_callback("google", &query, 1).await,
        CallbackOutcome::ProviderContract { .. }
    ));
}

#[tokio::test]
async fn no_refresh_token_stores_nothing() {
    let f = fixture().await;
    f.stub.lock().unwrap().token_body =
        Some(serde_json::json!({"access_token": "AT", "expires_in": 60}));
    let state = start(&f.svc);
    assert_eq!(
        f.svc
            .complete_callback("google", &code_query(&state), 1)
            .await,
        CallbackOutcome::NoRefreshToken
    );
    assert!(stored(&f.svc).await.is_none());
    assert!(f.stub.lock().unwrap().userinfo_auth.is_empty());
}

#[tokio::test]
async fn token_endpoint_refusal_is_unavailable() {
    let f = fixture().await;
    {
        let mut s = f.stub.lock().unwrap();
        s.token_status = Some(400);
        s.token_body = Some(serde_json::json!({"error": "invalid_grant"}));
    }
    let state = start(&f.svc);
    assert_eq!(
        f.svc
            .complete_callback("google", &code_query(&state), 1)
            .await,
        CallbackOutcome::ProviderUnavailable {
            detail: "HTTP 400".to_string()
        }
    );
    assert!(stored(&f.svc).await.is_none());
}

#[tokio::test]
async fn userinfo_without_the_account_field_is_a_contract_violation() {
    let f = fixture().await;
    for body in [
        serde_json::json!({"sub": "1"}),
        serde_json::json!({"email": ""}),
        serde_json::json!({"email": 7}),
    ] {
        f.stub.lock().unwrap().userinfo_body = Some(body.clone());
        let state = start(&f.svc);
        assert!(
            matches!(
                f.svc
                    .complete_callback("google", &code_query(&state), 1)
                    .await,
                CallbackOutcome::ProviderContract { .. }
            ),
            "{body}"
        );
    }
    assert!(stored(&f.svc).await.is_none());
}

#[tokio::test]
async fn unreachable_provider_is_unavailable() {
    // Port 9 (discard) on loopback: nothing listens, the connect is refused.
    let (svc, _dir) = service(vec![provider_config("google", "http://127.0.0.1:9")]);
    let state = start(&svc);
    let outcome = svc
        .complete_callback("google", &code_query(&state), 1)
        .await;
    let CallbackOutcome::ProviderUnavailable { detail } = outcome else {
        panic!("expected ProviderUnavailable, got {outcome:?}");
    };
    assert!(detail.contains("Connection refused"), "{detail}");
    assert!(!detail.contains("http://127.0.0.1:9"), "{detail}");
}

#[tokio::test]
async fn a_reconnect_replaces_the_stored_credential() {
    let f = fixture().await;
    let state = start(&f.svc);
    f.svc
        .complete_callback("google", &code_query(&state), 1)
        .await;
    f.stub.lock().unwrap().token_body = Some(serde_json::json!({
        "access_token": "AT-2", "refresh_token": "RT-2", "expires_in": 60,
    }));
    let state = start(&f.svc);
    f.svc
        .complete_callback("google", &code_query(&state), 1)
        .await;
    assert_eq!(stored(&f.svc).await.unwrap().refresh_token, "RT-2");
}

#[tokio::test]
async fn a_refused_client_on_exchange_is_client_refused() {
    let f = fixture().await;
    {
        let mut s = f.stub.lock().unwrap();
        s.token_status = Some(401);
        s.token_body = Some(serde_json::json!({"error": "invalid_client"}));
    }
    let state = start(&f.svc);
    assert_eq!(
        f.svc
            .complete_callback("google", &code_query(&state), 1)
            .await,
        CallbackOutcome::ClientRefused {
            detail: "HTTP 401".to_string()
        }
    );
    assert!(stored(&f.svc).await.is_none());
    assert!(f.stub.lock().unwrap().userinfo_auth.is_empty());
}

#[test]
fn expiry_adds_the_lifetime_and_refuses_what_overflows() {
    use chrono::{TimeDelta, Utc};

    use crate::expiry;

    let now = Utc::now();
    assert_eq!(expiry(now, None), Some(now), "no lifetime: already expired");
    assert_eq!(expiry(now, Some(0)), Some(now));
    assert_eq!(
        expiry(now, Some(3599)),
        Some(now + TimeDelta::seconds(3599))
    );
    assert_eq!(expiry(now, Some(u64::MAX)), None, "not an i64");
    assert_eq!(
        expiry(now, Some(i64::MAX as u64)),
        None,
        "not a chrono duration"
    );
    assert_eq!(
        expiry(now, Some(i64::MAX as u64 / 1000)),
        None,
        "a duration, but past the last representable instant"
    );
}
