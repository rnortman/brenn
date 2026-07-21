//! Client IP extraction, with reverse proxy support.
//!
//! The client identity is derived from the configured trusted-proxy-hop count
//! `N` (`server.trusted_proxy_hops`):
//!
//! - `N == 0`: no trusted proxy. The TCP peer address from `ConnectInfo` is used
//!   directly and `X-Forwarded-For` is ignored entirely.
//! - `N >= 1`: trust the `N` rightmost `X-Forwarded-For` tokens as written by
//!   trusted proxies and take the client identity as the `N`-th token counted
//!   from the right — the address the outermost trusted proxy observed as its
//!   peer. Everything to the left of that token is client-supplied and untrusted.
//!
//! Why rightmost-from-the-right rather than leftmost: nginx's
//! `proxy_add_x_forwarded_for` *appends* its immediate peer to whatever the
//! client sent. The leftmost token is fully attacker-chosen; only the tokens a
//! trusted proxy wrote (the rightmost ones) are trustworthy. Keying rate limits
//! and security attribution on the leftmost token let a client spoof its
//! identity — see security finding H2111.
//!
//! No fallback: when `N >= 1` and `X-Forwarded-For` is absent, too short, or the
//! selected token does not parse, the request is **rejected** (HTTP 400) and a
//! `ForwardedHeaderInvalid` security event is logged on the TCP peer. Falling
//! back to the peer would re-open the spoof via a proxy-bypass request.
//!
//! This is implemented as an Axum middleware that inserts `ClientIp` into
//! request extensions, so individual handlers just extract `Extension(ClientIp(ip))`.

use std::net::{IpAddr, SocketAddr};

use axum::Extension;
use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use brenn_lib::obs::security::{SecurityEventType, log_security_event};
use tower_governor::errors::GovernorError;
use tower_governor::key_extractor::KeyExtractor;

/// The resolved client IP address, inserted into request extensions by
/// [`resolve_client_ip`] middleware.
#[derive(Debug, Clone, Copy)]
pub struct ClientIp(pub IpAddr);

/// Build the client IP resolution middleware for the given trusted-proxy-hop count.
///
/// Must be applied after `ConnectInfo` is available (i.e., after
/// `Router::into_make_service_with_connect_info` or `MockConnectInfo`).
///
/// When `hops == 0`: uses the TCP peer address directly; `X-Forwarded-For` is
/// ignored.
///
/// When `hops >= 1`: selects the `hops`-th `X-Forwarded-For` token counted from
/// the right (see [`extract_forwarded_ip`]). If no valid forwarded identity
/// exists (header absent, too short, or the selected token unparseable), the
/// request is **rejected** with HTTP 400 and a `ForwardedHeaderInvalid` security
/// event is logged on the TCP peer — no fallback to the peer as a trusted
/// identity, because that is precisely the proxy-bypass spoof this guards against.
///
/// This reject path runs **outside** (before) both rate-limit governors, so it
/// is not throttled in-process by them. The bound is fail2ban: each reject emits
/// a `security_event` line keyed on the peer, so after `maxretry` hits the jail
/// bans the peer at the firewall. Keying an in-process throttle here would require
/// keying on the untrusted peer — a different key space, no security gain. This is
/// the same eventual-bound model every other pre-auth security signal relies on.
pub async fn resolve_client_ip(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Extension(TrustedProxyHops(hops)): Extension<TrustedProxyHops>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let ip = if hops == 0 {
        addr.ip()
    } else {
        match extract_forwarded_ip(&request, hops) {
            Ok(ip) => ip,
            Err(reject) => {
                // The operator asserted a trusted proxy always sits in front and
                // appends >= `hops` entries. That invariant is violated for this
                // request: either the proxy is misconfigured or the request reached
                // Brenn's port bypassing the proxy (peer = attacker). Reject loud;
                // never substitute the untrusted peer as the trusted identity.
                //
                // The detail string is tagged per cause so on-call triage can tell
                // a misconfigured proxy (Absent / TooShort — operational fix) from a
                // crafted/adversarial request (NonUtf8 / Unparseable token).
                log_security_event(
                    SecurityEventType::ForwardedHeaderInvalid,
                    addr.ip(),
                    reject.detail(),
                );
                return StatusCode::BAD_REQUEST.into_response();
            }
        }
    };

    request.extensions_mut().insert(ClientIp(ip));
    next.run(request).await
}

/// Carrier for the trusted-proxy-hop count (`server.trusted_proxy_hops`),
/// inserted into request extensions by an `Extension` layer so the middleware
/// can read it.
#[derive(Debug, Clone, Copy)]
pub struct TrustedProxyHops(pub u8);

/// Why a forwarded-identity selection was rejected.
///
/// The variants exist so the reject path can log a cause-specific `detail`
/// string: on-call triage needs to distinguish a *misconfigured proxy*
/// (`Absent` / `TooShort` — operational remediation) from a *crafted /
/// adversarial request* (`NonUtf8` / `Unparseable` — an attacker probing the
/// trust boundary). The security response (HTTP 400 + `ForwardedHeaderInvalid`
/// event → fail2ban ban) is identical for every cause; only the detail differs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ForwardedReject {
    /// No `X-Forwarded-For` header present, or it held no usable token (an
    /// empty / whitespace-only value, or a trailing comma writing an empty
    /// rightmost token). Almost always a proxy that did not append.
    Absent,
    /// The header was present but held fewer than `hops` tokens — the chain is
    /// shorter than the configured trusted-proxy depth (proxy misconfigured, or
    /// a request that bypassed one or more proxies).
    TooShort,
    /// The selected token was non-empty but did not parse as an IP address —
    /// a trusted proxy should never write garbage, so this is an adversarial
    /// signal (or a serious proxy bug).
    Unparseable,
    /// The header value was not valid UTF-8, so it could not be parsed at all.
    /// Behind a real proxy this is effectively unreachable and indicates a
    /// crafted request.
    NonUtf8,
}

impl ForwardedReject {
    /// Cause-specific detail string for the `ForwardedHeaderInvalid` security
    /// event (free-text log/alert body — no downstream parser keys on it).
    fn detail(self) -> &'static str {
        match self {
            Self::Absent => "X-Forwarded-For absent or empty for configured trusted_proxy_hops",
            Self::TooShort => "X-Forwarded-For has fewer tokens than configured trusted_proxy_hops",
            Self::Unparseable => "X-Forwarded-For selected token does not parse as an IP address",
            Self::NonUtf8 => "X-Forwarded-For header value is not valid UTF-8",
        }
    }
}

/// Select the client IP from `X-Forwarded-For` given a trusted-proxy-hop count.
///
/// `X-Forwarded-For` is an append chain: `client, proxy1, proxy2, ..., peer`,
/// where each trusted proxy appends the address it saw as its immediate peer
/// (the rightmost token is the one the nearest trusted proxy wrote). With `hops`
/// trusted proxies, the client identity is the `hops`-th token counted from the
/// right — i.e. index `len - hops` from the left — the address the *outermost*
/// trusted proxy observed as its peer. Everything further left is
/// client-supplied and untrusted.
///
/// Returns `Err(ForwardedReject)` — tagged by cause — if the header is absent /
/// empty, non-UTF-8, has fewer than `hops` tokens, or the selected token does
/// not parse as an IP address. `hops` is assumed `>= 1` (callers handle the
/// no-proxy case separately).
fn extract_forwarded_ip(request: &Request<Body>, hops: u8) -> Result<IpAddr, ForwardedReject> {
    let header = request
        .headers()
        .get("x-forwarded-for")
        .ok_or(ForwardedReject::Absent)?
        .to_str()
        .map_err(|_| ForwardedReject::NonUtf8)?;

    // Select the `hops`-th token from the right without allocating: walk
    // `hops` tokens back from the end. `split(',')` yields a
    // `DoubleEndedIterator`, so each `next_back()` consumes one token from the
    // right. If fewer than `hops` tokens exist, a `next_back()` returns `None`
    // and we short-circuit as `TooShort`.
    let mut it = header.split(',');
    for _ in 1..hops {
        it.next_back().ok_or(ForwardedReject::TooShort)?;
    }
    // The first (rightmost) token always exists for a present header — a bare
    // empty header value yields one empty token, classified as `Absent` below.
    let selected = it.next_back().ok_or(ForwardedReject::TooShort)?.trim();

    // The selected token may be empty (e.g. an empty/whitespace-only XFF value,
    // or a trailing comma writes an empty rightmost token). An empty token is
    // not a valid identity; treat it as `Absent` (the proxy effectively wrote
    // nothing) rather than falling through to `parse`.
    if selected.is_empty() {
        return Err(ForwardedReject::Absent);
    }

    // Some proxies include port (e.g., "1.2.3.4:5678"), strip it.
    // For IPv6, brackets would be present: "[::1]:5678"
    let ip_str = if let Some(bracketed) = selected.strip_prefix('[') {
        // IPv6 with brackets: "[::1]:port" or "[::1]"
        bracketed
            .split(']')
            .next()
            .ok_or(ForwardedReject::Unparseable)?
    } else if selected.contains('.') && selected.contains(':') {
        // IPv4 with port: "1.2.3.4:5678"
        selected
            .rsplit_once(':')
            .ok_or(ForwardedReject::Unparseable)?
            .0
    } else {
        // Plain IPv4 or IPv6 without port
        selected
    };

    ip_str.parse().map_err(|_| ForwardedReject::Unparseable)
}

/// `tower_governor` [`KeyExtractor`] that keys per real client IP via the
/// `ClientIp` request extension inserted by [`resolve_client_ip`].
///
/// The default `PeerIpKeyExtractor` reads `ConnectInfo<SocketAddr>` and
/// returns the TCP peer address — behind nginx, that's `127.0.0.1` for
/// every request, so per-IP rate limits collapse into shared global limits
/// for everyone behind the proxy. Wiring this extractor into the governor
/// keys on the already-resolved real client IP instead.
///
/// `resolve_client_ip` is registered as global middleware (see
/// `build_router`) so the `ClientIp` extension is present on every request
/// the governor sees in production. If it's missing — meaning some future
/// refactor inverted the layer order — `extract` returns
/// `UnableToExtractKey` and `tower_governor` translates that into HTTP
/// 500. A loud `tracing::error!` on the same path gives operators an
/// unmistakable signal so the order can be fixed.
///
/// `name()` / `key_name()` are gated behind tower_governor's `tracing`
/// feature, which we don't enable.
#[derive(Debug, Clone, Copy)]
pub struct ClientIpKeyExtractor;

impl KeyExtractor for ClientIpKeyExtractor {
    type Key = IpAddr;

    fn extract<T>(&self, req: &axum::http::Request<T>) -> Result<Self::Key, GovernorError> {
        match req.extensions().get::<ClientIp>() {
            Some(ClientIp(ip)) => Ok(*ip),
            None => {
                tracing::error!(
                    "ClientIp extension missing — resolve_client_ip middleware \
                     not in front of governor; rate-limit keying is broken"
                );
                Err(GovernorError::UnableToExtractKey)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use axum::Router;
    use axum::body::Body;
    use axum::extract::connect_info::MockConnectInfo;
    use axum::http::Request;
    use axum::middleware as axum_mw;
    use axum::routing::get;
    use tower::ServiceExt;

    use super::*;

    // --- Unit tests for extract_forwarded_ip (selector) ---

    fn make_request(xff: Option<&str>) -> Request<Body> {
        let mut builder = Request::builder().uri("/test");
        if let Some(val) = xff {
            builder = builder.header("x-forwarded-for", val);
        }
        builder.body(Body::empty()).unwrap()
    }

    #[test]
    fn hops_1_single_token() {
        let req = make_request(Some("203.0.113.50"));
        assert_eq!(
            extract_forwarded_ip(&req, 1),
            Ok("203.0.113.50".parse().unwrap())
        );
    }

    /// Spoof-defeat case: with one trusted proxy, the rightmost token (the one
    /// nginx appended) is selected, not the attacker-chosen leftmost token.
    #[test]
    fn hops_1_multiple_takes_rightmost() {
        let req = make_request(Some("1.2.3.4, 203.0.113.50"));
        assert_eq!(
            extract_forwarded_ip(&req, 1),
            Ok("203.0.113.50".parse().unwrap())
        );
    }

    #[test]
    fn hops_2_takes_second_from_right() {
        let req = make_request(Some("1.2.3.4, 5.6.7.8, 9.10.11.12"));
        assert_eq!(
            extract_forwarded_ip(&req, 2),
            Ok("5.6.7.8".parse().unwrap())
        );
    }

    #[test]
    fn hops_1_absent_or_empty_classified() {
        // Absent header → Absent.
        let req = make_request(None);
        assert_eq!(extract_forwarded_ip(&req, 1), Err(ForwardedReject::Absent));
        // Empty header value: single empty token → Absent (proxy wrote nothing).
        let req = make_request(Some(""));
        assert_eq!(extract_forwarded_ip(&req, 1), Err(ForwardedReject::Absent));
    }

    #[test]
    fn hops_2_only_one_token_too_short() {
        let req = make_request(Some("9.9.9.9"));
        assert_eq!(
            extract_forwarded_ip(&req, 2),
            Err(ForwardedReject::TooShort)
        );
    }

    #[test]
    fn ipv4_with_port() {
        // Port stripping applies to the selected (rightmost) token.
        let req = make_request(Some("203.0.113.50:8080"));
        assert_eq!(
            extract_forwarded_ip(&req, 1),
            Ok("203.0.113.50".parse().unwrap())
        );
    }

    #[test]
    fn ipv6_plain() {
        let req = make_request(Some("2001:db8::1"));
        assert_eq!(
            extract_forwarded_ip(&req, 1),
            Ok("2001:db8::1".parse().unwrap())
        );
    }

    #[test]
    fn ipv6_with_brackets_and_port() {
        let req = make_request(Some("[2001:db8::1]:8080"));
        assert_eq!(
            extract_forwarded_ip(&req, 1),
            Ok("2001:db8::1".parse().unwrap())
        );
    }

    #[test]
    fn selected_token_garbage_unparseable() {
        let req = make_request(Some("not-an-ip"));
        assert_eq!(
            extract_forwarded_ip(&req, 1),
            Err(ForwardedReject::Unparseable)
        );
    }

    #[test]
    fn whitespace_trimmed() {
        // The selected (rightmost) token is trimmed before parsing.
        let req = make_request(Some("10.0.0.1,  203.0.113.50  "));
        assert_eq!(
            extract_forwarded_ip(&req, 1),
            Ok("203.0.113.50".parse().unwrap())
        );
    }

    /// Each reject cause maps to a distinct, non-empty detail string. The reject
    /// path logs this verbatim, so a future merge of two causes (regressing the
    /// triage-fidelity fix) would collide here.
    #[test]
    fn reject_detail_strings_are_distinct() {
        let details = [
            ForwardedReject::Absent.detail(),
            ForwardedReject::TooShort.detail(),
            ForwardedReject::Unparseable.detail(),
            ForwardedReject::NonUtf8.detail(),
        ];
        for d in details {
            assert!(!d.is_empty());
        }
        for (i, a) in details.iter().enumerate() {
            for b in &details[i + 1..] {
                assert_ne!(a, b, "reject detail strings must be distinct");
            }
        }
    }

    // --- Integration tests for the middleware ---

    /// Test handler that echoes back the resolved ClientIp.
    async fn echo_ip(Extension(ClientIp(ip)): Extension<ClientIp>) -> String {
        ip.to_string()
    }

    /// Build a minimal router with the client IP middleware for testing.
    fn test_router(hops: u8) -> Router {
        Router::new()
            .route("/ip", get(echo_ip))
            .layer(axum_mw::from_fn(resolve_client_ip))
            .layer(Extension(TrustedProxyHops(hops)))
            .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))))
    }

    /// Send a request and return the full response (status + body), so reject
    /// (400) tests can assert on status.
    async fn get_response(router: Router, xff: Option<&str>) -> Response {
        let mut builder = Request::builder().uri("/ip");
        if let Some(val) = xff {
            builder = builder.header("x-forwarded-for", val);
        }
        let req = builder.body(Body::empty()).unwrap();
        router.oneshot(req).await.unwrap()
    }

    async fn get_ip(router: Router, xff: Option<&str>) -> String {
        let resp = get_response(router, xff).await;
        crate::test_support::http::body_string(resp.into_body()).await
    }

    #[tokio::test]
    async fn hops_0_ignores_xff() {
        let ip = get_ip(test_router(0), Some("203.0.113.50")).await;
        assert_eq!(ip, "127.0.0.1");
    }

    #[tokio::test]
    async fn hops_0_missing_uses_peer() {
        let ip = get_ip(test_router(0), None).await;
        assert_eq!(ip, "127.0.0.1");
    }

    #[tokio::test]
    async fn hops_1_uses_rightmost() {
        let ip = get_ip(test_router(1), Some("1.2.3.4, 198.51.100.1")).await;
        assert_eq!(ip, "198.51.100.1");
    }

    /// Explicit regression test for H2111: an attacker-supplied leftmost token
    /// must not become the resolved identity. nginx appends the real peer as the
    /// rightmost token; that is what we select.
    #[tokio::test]
    async fn hops_1_attacker_spoof_defeated() {
        // Attacker sends a forged victim IP; the appended (rightmost) token wins.
        let ip = get_ip(test_router(1), Some("9.9.9.9, 198.51.100.1")).await;
        assert_eq!(ip, "198.51.100.1");
        assert_ne!(ip, "9.9.9.9");
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn hops_1_missing_header_rejected() {
        let resp = get_response(test_router(1), None).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert!(
            logs_contain("security_event=true"),
            "missing XFF under trust must emit a security event"
        );
        assert!(
            logs_contain("forwarded_header_invalid"),
            "missing XFF must emit event_type=forwarded_header_invalid"
        );
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn hops_1_garbage_rejected() {
        let resp = get_response(test_router(1), Some("not-an-ip")).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert!(
            logs_contain("security_event=true"),
            "garbage selected token must emit security_event=true"
        );
        assert!(
            logs_contain("forwarded_header_invalid"),
            "garbage selected token must emit a ForwardedHeaderInvalid event"
        );
    }

    /// Middleware-level reject path for `hops == 2` with a too-short header.
    /// The selector unit tests prove `extract_forwarded_ip` returns `None`;
    /// this proves the middleware wires a hop count > 1 through to the 400 +
    /// security-event reject path (catches an off-by-one / constant-folding
    /// regression that the `hops_1_*` tests would not).
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn hops_2_short_header_rejected() {
        let resp = get_response(test_router(2), Some("9.9.9.9")).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert!(
            logs_contain("security_event=true"),
            "too-short XFF under hops=2 must emit security_event=true"
        );
        assert!(
            logs_contain("forwarded_header_invalid"),
            "too-short XFF under hops=2 must emit event_type=forwarded_header_invalid"
        );
    }

    // --- Tests for ClientIpKeyExtractor ---

    #[test]
    fn client_ip_key_extractor_uses_extension() {
        let mut req = Request::builder().uri("/test").body(Body::empty()).unwrap();
        let expected: IpAddr = "1.2.3.4".parse().unwrap();
        req.extensions_mut().insert(ClientIp(expected));
        let got = ClientIpKeyExtractor.extract(&req).unwrap();
        assert_eq!(got, expected);
    }

    #[test]
    fn client_ip_key_extractor_missing_extension_errors() {
        let req = Request::builder().uri("/test").body(Body::empty()).unwrap();
        match ClientIpKeyExtractor.extract(&req) {
            Err(GovernorError::UnableToExtractKey) => {}
            other => panic!("expected UnableToExtractKey, got {other:?}"),
        }
    }
}
