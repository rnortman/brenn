//! `one-click-unsubscribe`: send the RFC 8058 one-click unsubscribe request to
//! a public `https` URL the caller names, with no credential, and answer its
//! status.
//!
//! The tool guarantees the shape of the request: one `POST` of the fixed body
//! `List-Unsubscribe=One-Click` to a public address, no redirect followed, the
//! response body unread. Where the URL came from is the caller's business; a
//! deployer who grants the tool trusts the grantee with that choice.

use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use brenn_lib::special_address::{is_special_purpose, is_special_use_name};
use brenn_lib::tools::AclClause;
use brenn_obs::alerting::AlertDispatcher;
use brenn_obs::security::{RefusedDestination, signal_refused_destination};
use brenn_tool_registry::{
    AclDenied, AsyncTool, HeldAttachments, Idempotency, ToolClass, ToolCtx, ToolDescriptor,
    ToolError, ToolOutput, UserBinding,
};
use serde::Deserialize;
use tracing::info;
use url::{Host, Url};

use crate::http::CONNECT_TIMEOUT;

const NAME: &str = "one-click-unsubscribe";

/// Longest URL accepted, in bytes.
pub const URL_MAX_BYTES: usize = 2048;

/// Total time a call may take, connection included.
const TOTAL_TIMEOUT: Duration = Duration::from_secs(30);

/// The request body RFC 8058 section 3.2 fixes.
const BODY: &str = "List-Unsubscribe=One-Click";

const USER_AGENT: &str = concat!(
    "brenn/",
    env!("CARGO_PKG_VERSION"),
    " one-click-unsubscribe"
);

/// Resolves a host name to the socket addresses the request may dial.
type Resolve = Arc<
    dyn Fn(String) -> Pin<Box<dyn Future<Output = std::io::Result<Vec<SocketAddr>>> + Send>>
        + Send
        + Sync,
>;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UnsubscribeArgs {
    url: String,
}

/// The `one-click-unsubscribe` tool.
pub struct OneClickUnsubscribeTool {
    descriptor: ToolDescriptor,
    alerts: AlertDispatcher,
    resolve: Resolve,
    special: fn(IpAddr) -> bool,
    extra_root: Option<reqwest::Certificate>,
}

impl OneClickUnsubscribeTool {
    /// The tool, raising its refused-destination alerts through `alerts`.
    pub fn new(alerts: AlertDispatcher) -> Self {
        Self::with_network(
            alerts,
            Arc::new(|host: String| {
                Box::pin(async move {
                    Ok(tokio::net::lookup_host((host.as_str(), 443))
                        .await?
                        .collect())
                })
            }),
            is_special_purpose,
            None,
        )
    }

    fn with_network(
        alerts: AlertDispatcher,
        resolve: Resolve,
        special: fn(IpAddr) -> bool,
        extra_root: Option<reqwest::Certificate>,
    ) -> Self {
        Self {
            descriptor: ToolDescriptor {
                name: NAME,
                mcp_name: "mcp__brenn__OneClickUnsubscribe",
                description: "Send the RFC 8058 one-click unsubscribe request — a POST with \
                              the body `List-Unsubscribe=One-Click` and no credential — to an \
                              `https` URL on a public host, as a message's `List-Unsubscribe` \
                              header names it. No redirect is followed and the response body \
                              is not read. Answers `{\"status\": <HTTP status>, \"host\": \
                              <host>}` for any response, or `{\"error\": \"unreachable\", \
                              \"host\": <host>}` when no response came (resolution, \
                              connection, TLS, or 30 seconds passed); `host` is the URL's host \
                              as this tool parsed it, the one the request went to. A URL \
                              whose host is not a public address is refused `denied`, and \
                              nothing is sent. Callable by a WASM component only; any other \
                              caller is refused `denied`.",
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "url": {
                            "type": "string",
                            "maxLength": URL_MAX_BYTES,
                            "description": "The absolute `https` URL, as the header wrote it: \
                                            no user info, no port other than 443.",
                        },
                    },
                    "required": ["url"],
                    "additionalProperties": false,
                }),
                class: ToolClass::Async { max_concurrency: 4 },
                acl_keys: &[],
                idempotency: Idempotency::Natural,
                auto_approve: false,
                user_binding: UserBinding::None,
                accepts_attachments: false,
            },
            alerts,
            resolve,
            special,
            extra_root,
        }
    }

    /// Refuse the destination: signal it, then answer `Denied`.
    fn refuse(&self, ctx: &ToolCtx, host: &str, reason: RefusedDestination) -> ToolError {
        signal_refused_destination(&self.alerts, ctx.caller.as_str(), NAME, host, reason);
        let why = match reason {
            RefusedDestination::Literal => "is an address in a special-purpose range",
            RefusedDestination::ReservedName => "is a name that never denotes a public host",
            RefusedDestination::NoPublicAddress => "resolves only to non-public addresses",
        };
        ToolError::Denied(format!("{NAME}: host {host:?} {why}; nothing was sent"))
    }

    /// Where the request may go: `Ok(None)` dials an IP literal as written,
    /// `Ok(Some(addrs))` dials a name at those addresses only. `host_str` is
    /// the URL's host as written, which the signal and the refusal name.
    async fn destination(
        &self,
        ctx: &ToolCtx,
        url: &Url,
        host_str: &str,
    ) -> Result<Option<Vec<SocketAddr>>, Undialled> {
        match url.host() {
            Some(Host::Ipv4(v4)) if (self.special)(IpAddr::V4(v4)) => Err(Undialled::Refused(
                self.refuse(ctx, host_str, RefusedDestination::Literal),
            )),
            Some(Host::Ipv6(v6)) if (self.special)(IpAddr::V6(v6)) => Err(Undialled::Refused(
                self.refuse(ctx, host_str, RefusedDestination::Literal),
            )),
            Some(Host::Ipv4(_) | Host::Ipv6(_)) => Ok(None),
            Some(Host::Domain(name)) => {
                if is_special_use_name(name) {
                    return Err(Undialled::Refused(self.refuse(
                        ctx,
                        host_str,
                        RefusedDestination::ReservedName,
                    )));
                }
                let resolved = match (self.resolve)(name.to_string()).await {
                    Ok(addrs) if !addrs.is_empty() => addrs,
                    Ok(_) => return Err(Undialled::Unresolved("resolution: no addresses".into())),
                    Err(e) => {
                        return Err(Undialled::Unresolved(format!("resolution: {:?}", e.kind())));
                    }
                };
                let public: Vec<SocketAddr> = resolved
                    .into_iter()
                    .filter(|addr| !(self.special)(addr.ip()))
                    .collect();
                if public.is_empty() {
                    return Err(Undialled::Refused(self.refuse(
                        ctx,
                        host_str,
                        RefusedDestination::NoPublicAddress,
                    )));
                }
                Ok(Some(public))
            }
            None => unreachable!("the URL's host was checked present"),
        }
    }
}

/// Why a URL with an acceptable shape is not dialled.
enum Undialled {
    /// The destination is not public; the refusal has been signalled.
    Refused(ToolError),
    /// The name did not resolve, for the reason given.
    Unresolved(String),
}

/// The parsed URL, or why it is refused.
fn parse_url(raw: &str) -> Result<Url, ToolError> {
    let refuse = |why: &str| ToolError::InvalidArgs(format!("{NAME}: url {why}"));
    if raw.len() > URL_MAX_BYTES {
        return Err(refuse(&format!(
            "is {} bytes, more than {URL_MAX_BYTES}",
            raw.len()
        )));
    }
    let url = Url::parse(raw).map_err(|e| refuse(&format!("is not an absolute URL: {e}")))?;
    if url.scheme() != "https" {
        return Err(refuse("must use the https scheme"));
    }
    if url.host().is_none() {
        return Err(refuse("has no host"));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(refuse("must not carry user info"));
    }
    if url.port().is_some() {
        return Err(refuse("must not name a port other than 443"));
    }
    Ok(url)
}

#[async_trait]
impl AsyncTool for OneClickUnsubscribeTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn check_acl(&self, _args: &serde_json::Value, _acl: &[AclClause]) -> Result<(), AclDenied> {
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &ToolCtx,
        args: serde_json::Value,
        _attachments: HeldAttachments,
    ) -> Result<ToolOutput, ToolError> {
        // The tool reaches any public host, so only a component, whose grant
        // is its authorization, may call it.
        if !ctx.caller.is_wasm() {
            return Err(ToolError::Denied(format!(
                "{NAME}: callable by a component only (caller {})",
                ctx.caller.as_str()
            )));
        }
        let user = ctx.require_acting_user(NAME)?.clone();
        let args: UnsubscribeArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArgs(format!("{NAME} args: {e}")))?;
        let url = parse_url(&args.url)?;
        let host = url
            .host_str()
            .expect("the URL's host was checked present")
            .to_string();
        let dial = match self.destination(ctx, &url, &host).await {
            Ok(dial) => dial,
            Err(Undialled::Refused(refused)) => return Err(refused),
            Err(Undialled::Unresolved(cause)) => {
                return Ok(no_response(ctx, &user.username, &host, &cause));
            }
        };

        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(TOTAL_TIMEOUT);
        if let Some(addrs) = &dial {
            builder = builder.resolve_to_addrs(&host, addrs);
        }
        if let Some(root) = &self.extra_root {
            builder = builder.tls_certs_only([root.clone()]);
        }
        let client = builder
            .build()
            .expect("failed to build the one-click-unsubscribe HTTP client");
        let sent = client
            .post(url)
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/x-www-form-urlencoded",
            )
            .header(reqwest::header::USER_AGENT, USER_AGENT)
            .body(BODY)
            .send()
            .await;
        match sent {
            Ok(response) => {
                let status = response.status().as_u16();
                info!(
                    caller = %ctx.caller.as_str(),
                    user = %user.username,
                    host = %host,
                    status,
                    "one-click unsubscribe sent"
                );
                Ok(serde_json::json!({ "status": status, "host": host }).into())
            }
            Err(e) if e.is_builder() => Err(ToolError::Internal(format!(
                "{NAME}: the checked URL did not build a request: {}",
                e.without_url()
            ))),
            Err(e) => Ok(no_response(ctx, &user.username, &host, &transport_cause(e))),
        }
    }
}

/// Why `e` brought no response, for the log: timeout, connection or another
/// failure, then the error and its sources. The URL is dropped first, since
/// its path and query identify the recipient.
fn transport_cause(e: reqwest::Error) -> String {
    let kind = if e.is_timeout() {
        "timeout"
    } else if e.is_connect() {
        "connect"
    } else {
        "request"
    };
    let e = e.without_url();
    let mut cause = format!("{kind}: {e}");
    let mut source = std::error::Error::source(&e);
    while let Some(inner) = source {
        cause.push_str(": ");
        cause.push_str(&inner.to_string());
        source = inner.source();
    }
    cause
}

/// The answer for a request that got no response; `cause` is logged, not
/// answered.
fn no_response(ctx: &ToolCtx, user: &str, host: &str, cause: &str) -> ToolOutput {
    info!(
        caller = %ctx.caller.as_str(),
        user = %user,
        host = %host,
        status = "unreachable",
        cause = %cause,
        "one-click unsubscribe got no response"
    );
    serde_json::json!({ "error": "unreachable", "host": host }).into()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::Ipv4Addr;
    use std::sync::Mutex;

    use brenn_obs::alerting::{AlertSeverity, make_capturing_alerter_with_severity};
    use brenn_tool_registry::testutil::ExecuteValue;
    use rcgen::{
        BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer,
        KeyPair, KeyUsagePurpose, SanType,
    };
    use rustls_pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;
    use crate::testutil::{alice, tool_ctx};

    const PUBLIC_NAME: &str = "lists.example.com";
    const LISTENER: Ipv4Addr = Ipv4Addr::LOCALHOST;
    const TRIPWIRE: Ipv4Addr = Ipv4Addr::new(127, 0, 0, 2);

    /// Only the listener's address counts as public, so a loopback listener
    /// stands for a public host and every other address is special.
    fn only_listener_is_public(addr: IpAddr) -> bool {
        addr != IpAddr::V4(LISTENER)
    }

    struct Tls {
        root: reqwest::Certificate,
        acceptor: tokio_rustls::TlsAcceptor,
    }

    fn tls() -> Tls {
        let not_after = rcgen::date_time_ymd(2125, 1, 1);
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.not_after = not_after;
        ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
        ca_params
            .distinguished_name
            .push(DnType::CommonName, "unsubscribe-test-ca");
        let ca_key = KeyPair::generate().unwrap();
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();
        let issuer = Issuer::from_params(&ca_params, &ca_key);

        let mut leaf = CertificateParams::new(Vec::<String>::new()).unwrap();
        leaf.not_after = not_after;
        leaf.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        leaf.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        leaf.subject_alt_names = vec![SanType::DnsName(PUBLIC_NAME.try_into().unwrap())];
        let leaf_key = KeyPair::generate().unwrap();
        let leaf_cert = leaf.signed_by(&leaf_key, &issuer).unwrap();

        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let config = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![CertificateDer::from(leaf_cert.der().to_vec())],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(leaf_key.serialize_der())),
            )
            .unwrap();
        Tls {
            root: reqwest::Certificate::from_der(ca_cert.der()).unwrap(),
            acceptor: tokio_rustls::TlsAcceptor::from(Arc::new(config)),
        }
    }

    /// One request as the listener received it.
    #[derive(Debug, Clone)]
    struct Received {
        head: String,
        body: String,
    }

    impl Received {
        fn header(&self, name: &str) -> Option<String> {
            self.head.lines().skip(1).find_map(|line| {
                let (key, value) = line.split_once(':')?;
                key.eq_ignore_ascii_case(name)
                    .then(|| value.trim().to_string())
            })
        }
    }

    /// A TLS listener on `ip` that answers every request with `response` and
    /// records what it received; also counts connections.
    struct Listener {
        port: u16,
        received: Arc<Mutex<Vec<Received>>>,
        connections: Arc<Mutex<usize>>,
    }

    async fn listen(ip: Ipv4Addr, acceptor: tokio_rustls::TlsAcceptor, response: &str) -> Listener {
        let socket = TcpListener::bind((ip, 0)).await.unwrap();
        let port = socket.local_addr().unwrap().port();
        let received = Arc::new(Mutex::new(Vec::new()));
        let connections = Arc::new(Mutex::new(0));
        let response = response.to_string();
        let (rec, conns) = (received.clone(), connections.clone());
        tokio::spawn(async move {
            loop {
                let (tcp, _) = socket.accept().await.unwrap();
                *conns.lock().unwrap() += 1;
                let Ok(mut stream) = acceptor.accept(tcp).await else {
                    continue;
                };
                let mut bytes = Vec::new();
                let mut buf = [0u8; 4096];
                let (head, body_start) = loop {
                    let n = stream.read(&mut buf).await.unwrap();
                    assert!(n > 0, "connection closed before the request head ended");
                    bytes.extend_from_slice(&buf[..n]);
                    if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        break (String::from_utf8(bytes[..end].to_vec()).unwrap(), end + 4);
                    }
                };
                let length: usize = head
                    .lines()
                    .find_map(|l| {
                        let (k, v) = l.split_once(':')?;
                        k.eq_ignore_ascii_case("content-length")
                            .then(|| v.trim().parse().unwrap())
                    })
                    .unwrap_or(0);
                while bytes.len() < body_start + length {
                    let n = stream.read(&mut buf).await.unwrap();
                    assert!(n > 0, "connection closed inside the body");
                    bytes.extend_from_slice(&buf[..n]);
                }
                let body =
                    String::from_utf8(bytes[body_start..body_start + length].to_vec()).unwrap();
                rec.lock().unwrap().push(Received { head, body });
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.shutdown().await.unwrap();
            }
        });
        Listener {
            port,
            received,
            connections,
        }
    }

    fn resolver(answers: BTreeMap<&'static str, Vec<SocketAddr>>) -> Resolve {
        let answers = Arc::new(answers);
        Arc::new(move |host: String| {
            let answer = answers.get(host.as_str()).cloned();
            Box::pin(async move {
                answer.ok_or_else(|| std::io::Error::other(format!("no such name {host}")))
            })
        })
    }

    type Alerts = Arc<Mutex<Vec<(AlertSeverity, String, String)>>>;

    fn tool_with(
        resolve: Resolve,
        root: Option<reqwest::Certificate>,
    ) -> (
        OneClickUnsubscribeTool,
        Alerts,
        AlertDispatcher,
        tokio::task::JoinHandle<()>,
    ) {
        let (dispatcher, captured, handle) = make_capturing_alerter_with_severity();
        let tool = OneClickUnsubscribeTool::with_network(
            dispatcher.clone(),
            resolve,
            only_listener_is_public,
            root,
        );
        (tool, captured, dispatcher, handle)
    }

    /// Drop every dispatcher clone and drain the alerts.
    async fn drained(
        tool: OneClickUnsubscribeTool,
        dispatcher: AlertDispatcher,
        handle: tokio::task::JoinHandle<()>,
        captured: &Alerts,
    ) -> Vec<(AlertSeverity, String, String)> {
        drop(tool);
        drop(dispatcher);
        handle.await.unwrap();
        captured.lock().unwrap().clone()
    }

    /// A component's stamped call: `alice` acting, no conversation.
    fn ctx() -> ToolCtx {
        ToolCtx {
            caller: brenn_lib::messaging::ParticipantId::for_wasm("mail-write"),
            acting_conversation_id: None,
            ..tool_ctx(Some(alice()), vec![])
        }
    }

    async fn call(
        tool: &OneClickUnsubscribeTool,
        url: &str,
    ) -> Result<serde_json::Value, ToolError> {
        tool.execute_value(&ctx(), json!({ "url": url })).await
    }

    const OK_200: &str = "HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok";

    #[tokio::test]
    async fn descriptor_holds_the_contract() {
        let (dispatcher, _handle) = brenn_obs::alerting::noop_alert_dispatcher();
        let tool = OneClickUnsubscribeTool::new(dispatcher);
        let d = tool.descriptor();
        assert_eq!(d.name, "one-click-unsubscribe");
        assert_eq!(d.mcp_name, "mcp__brenn__OneClickUnsubscribe");
        assert!(matches!(d.class, ToolClass::Async { max_concurrency: 4 }));
        assert!(d.acl_keys.is_empty());
        assert_eq!(d.user_binding, UserBinding::None);
        assert_eq!(d.idempotency, Idempotency::Natural);
        assert!(!d.accepts_attachments);
        // The tool reaches any public host, so brenn does not approve an
        // agent's call to it on its own.
        assert!(!d.auto_approve);
        assert_eq!(
            brenn_envelope::tool_schema::check_input_schema(&d.input_schema),
            Ok(())
        );
    }

    #[tokio::test]
    async fn malformed_urls_and_args_are_invalid_and_alert_nothing() {
        let (tool, captured, dispatcher, handle) = tool_with(resolver(BTreeMap::new()), None);
        let long = format!("https://{PUBLIC_NAME}/{}", "a".repeat(URL_MAX_BYTES));
        let long = &long[..URL_MAX_BYTES + 1];
        for url in [
            "not a url",
            "http://lists.example.com/u",
            "https://bob:pw@lists.example.com/u",
            "https://bob@lists.example.com/u",
            "https://lists.example.com:8443/u",
            long,
        ] {
            let err = call(&tool, url).await.unwrap_err();
            assert!(matches!(err, ToolError::InvalidArgs(_)), "{url}: {err:?}");
        }
        let err = tool
            .execute_value(
                &ctx(),
                json!({ "url": "https://lists.example.com/u", "x": 1 }),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::InvalidArgs(_)), "{err:?}");
        assert!(
            drained(tool, dispatcher, handle, &captured)
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn a_url_of_exactly_the_limit_passes_the_shape_check() {
        let url = format!("https://{PUBLIC_NAME}/{}", "a".repeat(URL_MAX_BYTES));
        assert!(parse_url(&url[..URL_MAX_BYTES]).is_ok());
    }

    #[tokio::test]
    async fn no_acting_user_is_denied() {
        let (tool, captured, dispatcher, handle) = tool_with(resolver(BTreeMap::new()), None);
        let err = tool
            .execute_value(
                &ToolCtx {
                    acting_user: None,
                    ..ctx()
                },
                json!({ "url": "https://lists.example.com/u" }),
            )
            .await
            .unwrap_err();
        let ToolError::Denied(detail) = err else {
            panic!("expected Denied, got {err:?}");
        };
        assert!(!detail.contains("component only"), "{detail}");
        assert!(
            drained(tool, dispatcher, handle, &captured)
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn a_caller_that_is_not_a_component_is_denied_and_nothing_is_sent() {
        let tls = tls();
        let server = listen(LISTENER, tls.acceptor, OK_200).await;
        let (tool, captured, dispatcher, handle) = tool_with(
            resolver(BTreeMap::from([(
                PUBLIC_NAME,
                vec![SocketAddr::from((LISTENER, server.port))],
            )])),
            Some(tls.root),
        );
        let err = tool
            .execute_value(
                &tool_ctx(Some(alice()), vec![]),
                json!({ "url": format!("https://{PUBLIC_NAME}/u/abc") }),
            )
            .await
            .unwrap_err();
        let ToolError::Denied(detail) = err else {
            panic!("expected Denied, got {err:?}");
        };
        assert_eq!(
            detail,
            "one-click-unsubscribe: callable by a component only (caller app:pa@test-origin)"
        );
        assert!(
            drained(tool, dispatcher, handle, &captured)
                .await
                .is_empty()
        );
        assert!(server.received.lock().unwrap().is_empty());
        assert_eq!(*server.connections.lock().unwrap(), 0);
    }

    fn assert_one_refusal_alert(alerts: &[(AlertSeverity, String, String)], host: &str) {
        assert_eq!(alerts.len(), 1, "{alerts:?}");
        let (severity, title, body) = &alerts[0];
        assert!(matches!(severity, AlertSeverity::Warning), "{severity:?}");
        assert_eq!(title, "Security: ssrf_attempt");
        assert!(body.contains("wasm:mail-write"), "{body}");
        assert!(body.contains(&format!("host={host} ")), "{body}");
        assert!(!body.contains("secret-token"), "{body}");
        assert!(!body.contains("/u/"), "{body}");
    }

    #[tokio::test]
    async fn a_private_literal_is_denied_with_one_alert() {
        let (tool, captured, dispatcher, handle) = tool_with(resolver(BTreeMap::new()), None);
        let err = call(&tool, "https://10.1.2.3/u/secret-token")
            .await
            .unwrap_err();
        let ToolError::Denied(detail) = err else {
            panic!("expected Denied, got {err:?}");
        };
        assert!(detail.contains("10.1.2.3"), "{detail}");
        assert!(!detail.contains("secret-token"), "{detail}");
        let alerts = drained(tool, dispatcher, handle, &captured).await;
        assert_one_refusal_alert(&alerts, "10.1.2.3");
    }

    #[tokio::test]
    async fn reserved_names_are_denied_with_one_alert_each() {
        for host in [
            "localhost",
            "printer.local",
            "app.localhost",
            "intranet",
            "localhost.",
            "nas.home.arpa",
            "db.internal",
        ] {
            let (tool, captured, dispatcher, handle) = tool_with(resolver(BTreeMap::new()), None);
            let err = call(&tool, &format!("https://{host}/u/secret-token"))
                .await
                .unwrap_err();
            assert!(matches!(err, ToolError::Denied(_)), "{host}: {err:?}");
            let alerts = drained(tool, dispatcher, handle, &captured).await;
            assert_one_refusal_alert(&alerts, host);
        }
    }

    #[tokio::test]
    async fn a_name_with_only_private_addresses_is_denied_and_dials_nothing() {
        let tls = tls();
        let tripwire = listen(TRIPWIRE, tls.acceptor.clone(), OK_200).await;
        let (tool, captured, dispatcher, handle) = tool_with(
            resolver(BTreeMap::from([(
                PUBLIC_NAME,
                vec![SocketAddr::from((TRIPWIRE, tripwire.port))],
            )])),
            Some(tls.root),
        );
        let err = call(&tool, &format!("https://{PUBLIC_NAME}/u/secret-token"))
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::Denied(_)), "{err:?}");
        let alerts = drained(tool, dispatcher, handle, &captured).await;
        assert_one_refusal_alert(&alerts, PUBLIC_NAME);
        assert_eq!(*tripwire.connections.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn the_request_is_the_fixed_post_with_no_credential() {
        let tls = tls();
        let server = listen(LISTENER, tls.acceptor, OK_200).await;
        let (tool, captured, dispatcher, handle) = tool_with(
            resolver(BTreeMap::from([(
                PUBLIC_NAME,
                vec![SocketAddr::from((LISTENER, server.port))],
            )])),
            Some(tls.root),
        );
        let answer = call(
            &tool,
            &format!("https://{PUBLIC_NAME}/u/abc?list=7&t=x%2Fy"),
        )
        .await
        .unwrap();
        assert_eq!(answer, json!({ "status": 200, "host": PUBLIC_NAME }));
        let received = server.received.lock().unwrap().clone();
        assert_eq!(received.len(), 1);
        let request = &received[0];
        assert!(
            request
                .head
                .starts_with("POST /u/abc?list=7&t=x%2Fy HTTP/1.1\r\n"),
            "{}",
            request.head
        );
        assert_eq!(
            request.header("content-type").as_deref(),
            Some("application/x-www-form-urlencoded")
        );
        assert_eq!(request.body, "List-Unsubscribe=One-Click");
        assert_eq!(request.header("authorization"), None);
        assert_eq!(request.header("cookie"), None);
        assert!(
            request
                .header("user-agent")
                .is_some_and(|ua| ua.starts_with("brenn/")),
            "{}",
            request.head
        );
        assert!(
            drained(tool, dispatcher, handle, &captured)
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn every_status_is_answered_as_itself_and_no_redirect_is_followed() {
        for (response, status) in [
            (
                "HTTP/1.1 302 Found\r\nlocation: https://lists.example.com/elsewhere\r\n\
                 content-length: 0\r\nconnection: close\r\n\r\n",
                302,
            ),
            (
                "HTTP/1.1 500 Internal Server Error\r\ncontent-length: 0\r\n\
                 connection: close\r\n\r\n",
                500,
            ),
        ] {
            let tls = tls();
            let server = listen(LISTENER, tls.acceptor, response).await;
            let (tool, captured, dispatcher, handle) = tool_with(
                resolver(BTreeMap::from([(
                    PUBLIC_NAME,
                    vec![SocketAddr::from((LISTENER, server.port))],
                )])),
                Some(tls.root),
            );
            let answer = call(&tool, &format!("https://{PUBLIC_NAME}/u/abc"))
                .await
                .unwrap();
            assert_eq!(answer, json!({ "status": status, "host": PUBLIC_NAME }));
            let received = server.received.lock().unwrap().clone();
            assert_eq!(received.len(), 1, "only the first request: {received:?}");
            assert!(
                drained(tool, dispatcher, handle, &captured)
                    .await
                    .is_empty()
            );
        }
    }

    #[tokio::test]
    async fn a_mixed_name_dials_only_its_public_address_and_alerts_nothing() {
        let tls = tls();
        let tripwire = listen(TRIPWIRE, tls.acceptor.clone(), OK_200).await;
        let server = listen(LISTENER, tls.acceptor, OK_200).await;
        let (tool, captured, dispatcher, handle) = tool_with(
            resolver(BTreeMap::from([(
                PUBLIC_NAME,
                vec![
                    SocketAddr::from((TRIPWIRE, tripwire.port)),
                    SocketAddr::from((LISTENER, server.port)),
                ],
            )])),
            Some(tls.root),
        );
        let answer = call(&tool, &format!("https://{PUBLIC_NAME}/u/abc"))
            .await
            .unwrap();
        assert_eq!(answer, json!({ "status": 200, "host": PUBLIC_NAME }));
        assert_eq!(server.received.lock().unwrap().len(), 1);
        assert_eq!(*tripwire.connections.lock().unwrap(), 0);
        assert!(
            drained(tool, dispatcher, handle, &captured)
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn the_answer_names_the_host_the_request_went_to() {
        let tls = tls();
        let server = listen(LISTENER, tls.acceptor, OK_200).await;
        let (tool, captured, dispatcher, handle) = tool_with(
            resolver(BTreeMap::from([(
                PUBLIC_NAME,
                vec![SocketAddr::from((LISTENER, server.port))],
            )])),
            Some(tls.root),
        );
        let answer = call(&tool, &format!("https://{PUBLIC_NAME}\\@other.example/u"))
            .await
            .unwrap();
        assert_eq!(answer, json!({ "status": 200, "host": PUBLIC_NAME }));
        let received = server.received.lock().unwrap().clone();
        assert!(
            received[0]
                .head
                .starts_with("POST /@other.example/u HTTP/1.1\r\n"),
            "{}",
            received[0].head
        );
        assert!(
            drained(tool, dispatcher, handle, &captured)
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn a_closed_port_is_unreachable() {
        let tls = tls();
        let closed = {
            let socket = TcpListener::bind((LISTENER, 0)).await.unwrap();
            socket.local_addr().unwrap().port()
        };
        let (tool, captured, dispatcher, handle) = tool_with(
            resolver(BTreeMap::from([(
                PUBLIC_NAME,
                vec![SocketAddr::from((LISTENER, closed))],
            )])),
            Some(tls.root),
        );
        let answer = call(&tool, &format!("https://{PUBLIC_NAME}/u/abc"))
            .await
            .unwrap();
        assert_eq!(
            answer,
            json!({ "error": "unreachable", "host": PUBLIC_NAME })
        );
        assert!(logs_contain("cause=connect"));
        assert!(!logs_contain("/u/abc"));
        assert!(
            drained(tool, dispatcher, handle, &captured)
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn a_name_that_does_not_resolve_is_unreachable_and_alerts_nothing() {
        let (tool, captured, dispatcher, handle) = tool_with(resolver(BTreeMap::new()), None);
        let answer = call(&tool, "https://gone.example.com/u/abc").await.unwrap();
        assert_eq!(
            answer,
            json!({ "error": "unreachable", "host": "gone.example.com" })
        );
        assert!(logs_contain("cause=resolution: Other"));
        assert!(
            drained(tool, dispatcher, handle, &captured)
                .await
                .is_empty()
        );
    }

    fn registry() -> brenn_tool_registry::ToolRegistry {
        let (dispatcher, _handle) = brenn_obs::alerting::noop_alert_dispatcher();
        brenn_tool_registry::ToolRegistry::new(vec![crate::tools::one_click_unsubscribe(
            dispatcher,
        )])
    }

    fn grant(acl: Vec<AclClause>) -> BTreeMap<String, brenn_lib::tools::ResolvedToolGrant> {
        BTreeMap::from([(
            NAME.to_string(),
            brenn_lib::tools::ResolvedToolGrant {
                acl,
                rate_limit: None,
            },
        )])
    }

    #[tokio::test]
    async fn a_wasm_consumer_may_hold_the_grant_without_an_allow_clause() {
        registry().validate_grants(
            brenn_tool_registry::GrantOwner::WasmConsumer { slug: "mail-write" },
            &grant(vec![]),
        );
    }

    #[tokio::test]
    #[should_panic(expected = "unknown ACL key")]
    async fn a_grant_with_an_allow_clause_is_refused() {
        registry().validate_grants(
            brenn_tool_registry::GrantOwner::WasmConsumer { slug: "mail-write" },
            &grant(vec![AclClause::new(BTreeMap::from([(
                "host".to_string(),
                "lists.example.com".to_string(),
            )]))]),
        );
    }
}
