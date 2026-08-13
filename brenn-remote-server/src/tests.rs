//! The credential path, pinned in the crate that owns it: what counts as a
//! well-formed `Bearer` header, and what every refusal answers and emits.
//!
//! These drive `authenticate_remote` directly — no socket, no spawned server,
//! no route — so the posture holds on this crate's own target rather than only
//! on an upstream integration rig.

use std::net::Ipv4Addr;

use axum::http::HeaderValue;
use brenn_lib::messaging::config::MessagingGlobalConfig;
use brenn_lib::messaging::remote::{RemoteConfigRaw, resolve_remotes};
use brenn_obs::alerting::make_capturing_alerter;

use super::*;

const SLUG: &str = "pod-kitchen";
const TOKEN: &str = "s3cret-token";
const TEST_MAX_BODY_BYTES: usize = 64 * 1024;

const BLOCK: &str = r#"
slug = "pod-kitchen"
token_file = "TOKEN_FILE"
grants = ["subscribe", "publish"]
subscribe_acl = [ { prefix = "chat.app.home.out.", push_depth = 8, retain_depth = 64 } ]
publish_acl   = [ { prefix = "chat.app.home.in." } ]
"#;

/// A 0600 token file holding [`TOKEN`], so resolution runs the real
/// mode-checked load rather than a stubbed credential.
fn write_token() -> tempfile::NamedTempFile {
    use std::io::Write as _;
    let mut f = tempfile::NamedTempFile::new().unwrap();
    writeln!(f, "{TOKEN}").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(f.path(), std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    f
}

/// The runtime map holding one remote under [`SLUG`]. The token file is
/// returned alongside so the caller holds it open for the test's length.
fn runtimes() -> (HashMap<String, Arc<RemoteRuntime>>, tempfile::NamedTempFile) {
    let token = write_token();
    let toml = BLOCK.replace("TOKEN_FILE", &token.path().display().to_string());
    let raw: RemoteConfigRaw = toml::from_str(&toml).expect("[[remote]] block must parse");
    let resolved = resolve_remotes(&[raw], &MessagingGlobalConfig::default());
    let messenger = brenn_messaging::testutils::empty_directory_messenger("remote-auth-tests");
    (
        build_remote_runtimes(&resolved, Some(&messenger), TEST_MAX_BODY_BYTES),
        token,
    )
}

fn headers(authorization: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, HeaderValue::from_str(authorization).unwrap());
    headers
}

/// A header value that is a valid HTTP field value but not UTF-8, which is what
/// `to_str` refuses.
fn opaque_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_bytes(b"Bearer \xff\xfe").unwrap(),
    );
    headers
}

fn ip() -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7))
}

/// Refuse against `runtimes` and return the alert bodies the refusal emitted.
async fn refuse(
    remotes: &HashMap<String, Arc<RemoteRuntime>>,
    slug: &str,
    headers: &HeaderMap,
) -> Vec<String> {
    let (dispatcher, captured, handle) = make_capturing_alerter();
    match authenticate_remote(remotes, &dispatcher, slug, headers, ip()) {
        Err(status) => assert_eq!(status, StatusCode::UNAUTHORIZED),
        Ok(runtime) => panic!("expected a refusal for slug {slug:?}, got {}", runtime.slug),
    }
    drop(dispatcher);
    handle.await.unwrap();
    let captured = captured.lock().unwrap();
    captured.iter().map(|(_, body)| body.clone()).collect()
}

/// **A credential is `Bearer <something>`, and nothing else.** Every other
/// shape — absent, unreadable, another scheme, an empty credential — answers
/// `None`, so the caller has one miss to handle and a prober one answer to see.
#[test]
fn bearer_credential_admits_only_a_well_formed_bearer_header() {
    assert_eq!(bearer_credential(&HeaderMap::new()), None, "absent header");
    assert_eq!(
        bearer_credential(&opaque_headers()),
        None,
        "non-UTF-8 value"
    );
    assert_eq!(
        bearer_credential(&headers("Basic aGk6dGhlcmU=")),
        None,
        "another scheme"
    );
    assert_eq!(bearer_credential(&headers(TOKEN)), None, "no scheme at all");
    assert_eq!(
        bearer_credential(&headers("Bearer ")),
        None,
        "empty after the scheme"
    );
    assert_eq!(
        bearer_credential(&headers("Bearer    ")),
        None,
        "whitespace-only credential"
    );
    assert_eq!(
        bearer_credential(&headers(&format!("Bearer {TOKEN}"))),
        Some(TOKEN),
        "a well-formed credential is returned verbatim"
    );
    assert_eq!(
        bearer_credential(&headers(&format!("bearer {TOKEN}"))),
        Some(TOKEN),
        "the scheme is case-insensitive, per RFC 7235"
    );
}

/// **The right token on the right slug is the only success.** It resolves to
/// that remote's runtime, which is what carries the authority the session runs
/// under.
#[tokio::test]
async fn the_configured_token_resolves_its_own_runtime() {
    let (remotes, _token) = runtimes();
    let (dispatcher, _captured, _handle) = make_capturing_alerter();
    let runtime = authenticate_remote(
        &remotes,
        &dispatcher,
        SLUG,
        &headers(&format!("Bearer {TOKEN}")),
        ip(),
    )
    .expect("the configured token is admitted");
    assert_eq!(runtime.slug, SLUG);
    assert_eq!(
        runtime.registry_key,
        AttachScope::remote(SLUG).registry_key().into_owned()
    );
}

/// **Every refusal is the same refusal.** Unknown slug, wrong token, and a
/// missing header answer one `401` and emit exactly one `AuthFailure` each, so
/// fail2ban sees uniform signal and a prober learns nothing about which slugs
/// exist.
#[tokio::test]
async fn every_refusal_answers_401_and_emits_one_auth_failure() {
    let (remotes, _token) = runtimes();

    for (name, slug, header) in [
        (
            "unknown slug with a valid-looking token",
            "no-such-remote",
            Some(format!("Bearer {TOKEN}")),
        ),
        (
            "wrong token",
            SLUG,
            Some("Bearer not-the-token".to_string()),
        ),
        ("missing header", SLUG, None),
        (
            "malformed header",
            SLUG,
            Some("Basic aGk6dGhlcmU=".to_string()),
        ),
    ] {
        let header_map = header.map_or_else(HeaderMap::new, |value| headers(&value));
        let bodies = refuse(&remotes, slug, &header_map).await;
        assert_eq!(bodies.len(), 1, "{name} must emit exactly one alert");
        assert!(
            bodies[0].contains(&ip().to_string()),
            "{name} must carry the client IP, got {:?}",
            bodies[0]
        );
    }
}

/// **A slug is attacker-authored text.** It reaches the security event only
/// through the shared sanitizer, so a control character in the URL cannot break
/// the log line it lands in.
#[tokio::test]
async fn an_unknown_slug_is_sanitized_in_the_security_event() {
    let (remotes, _token) = runtimes();
    let bodies = refuse(&remotes, "evil\nslug", &headers(&format!("Bearer {TOKEN}"))).await;
    assert_eq!(bodies.len(), 1);
    assert!(
        bodies[0].contains("evil\\nslug"),
        "the newline must be escaped, got {:?}",
        bodies[0]
    );
    assert!(
        !bodies[0].contains("evil\nslug"),
        "the raw slug must not reach the alert body, got {:?}",
        bodies[0]
    );
}
