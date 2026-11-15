//! The dial: TCP, TLS for `wss://`, and the websocket handshake, run here so a
//! failure keeps its kind.
//!
//! The client crate's own connector takes TLS from a `tokio-tungstenite`
//! feature, which inside this workspace would change every member's build, and
//! reports each failure as text, so a rejected credential reads like a network
//! fault. This one opens the stream itself, wraps it in rustls with the
//! `aws_lc_rs` provider passed to the config builder, runs the handshake with
//! `client_async`, and hands the socket to the client crate's
//! [`NativeConnection`]. Each failed attempt is recorded as a [`DialFailure`]
//! in a cell the supervisor reads.

use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};

use brenn_attach_client::{
    NativeConnection, TransportConnector, TransportError, insert_bearer_token,
    refuse_cleartext_bearer,
};
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, ServerName};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tokio_tungstenite::tungstenite;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;

use crate::DialFailure;

/// Where the latest attempt's failure is left for the supervisor. Cleared at
/// the start of each attempt, so an attempt the connect deadline abandoned
/// leaves it empty.
pub(crate) type DialCell = Arc<Mutex<Option<DialFailure>>>;

/// Which certificates a `wss://` dial trusts.
#[derive(Clone)]
pub enum Trust {
    /// Exactly these, and nothing else.
    Roots(Vec<CertificateDer<'static>>),
    /// The platform's trust store, read at the first `wss://` dial.
    Platform,
}

/// The stream a websocket runs over: plain TCP to loopback, or TLS.
pub enum Stream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

/// Dials with a bearer token and records why an attempt failed.
// TODO(attach-blocking-second-loop-and-dial): a second dial beside the client
// crate's `NativeConnector`; a fix to one does not reach the other.
pub struct BlockingConnector {
    token: String,
    trust: Trust,
    /// Built at the first `wss://` dial that succeeds in building it.
    tls: Option<Arc<rustls::ClientConfig>>,
    failure: DialCell,
}

impl BlockingConnector {
    pub(crate) fn new(token: String, trust: Trust, failure: DialCell) -> Self {
        Self {
            token,
            trust,
            tls: None,
            failure,
        }
    }

    async fn dial(&mut self, url: &str) -> Result<NativeConnection<Stream>, DialFailure> {
        let handshake = |detail: String| DialFailure::Handshake { detail };
        let request = upgrade_request(url, &self.token).map_err(handshake)?;
        let uri = request.uri();
        let tls = uri.scheme_str() == Some("wss");
        let host = uri
            .host()
            .ok_or_else(|| handshake("the URL names no host".to_string()))?
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_string();
        let port = uri.port_u16().unwrap_or(if tls { 443 } else { 80 });

        let tcp = TcpStream::connect((host.as_str(), port))
            .await
            .map_err(|err| DialFailure::Io {
                detail: err.to_string(),
            })?;
        let stream = if tls {
            let config = self.tls_config()?;
            let name = ServerName::try_from(host).map_err(|err| DialFailure::Tls {
                detail: err.to_string(),
            })?;
            let stream = TlsConnector::from(config)
                .connect(name, tcp)
                .await
                .map_err(|err| DialFailure::Tls {
                    detail: err.to_string(),
                })?;
            Stream::Tls(Box::new(stream))
        } else {
            Stream::Plain(tcp)
        };
        match tokio_tungstenite::client_async(request, stream).await {
            Ok((ws, _response)) => Ok(NativeConnection::from_stream(ws)),
            Err(tungstenite::Error::Http(response)) => Err(DialFailure::UpgradeRejected {
                status: response.status().as_u16(),
            }),
            Err(err) => Err(handshake(err.to_string())),
        }
    }

    fn tls_config(&mut self) -> Result<Arc<rustls::ClientConfig>, DialFailure> {
        if let Some(config) = &self.tls {
            return Ok(config.clone());
        }
        let tls = |detail: String| DialFailure::Tls { detail };
        let mut roots = RootCertStore::empty();
        match &self.trust {
            Trust::Roots(certs) => {
                for cert in certs {
                    roots
                        .add(cert.clone())
                        .map_err(|err| tls(format!("an authority in ca_pem: {err}")))?;
                }
            }
            Trust::Platform => {
                let found = rustls_native_certs::load_native_certs();
                roots.add_parsable_certificates(found.certs);
                if roots.is_empty() {
                    let mut detail = "the platform trust store yielded no certificate".to_string();
                    if !found.errors.is_empty() {
                        let errors: Vec<String> =
                            found.errors.iter().map(ToString::to_string).collect();
                        detail = format!("{detail}: {}", errors.join("; "));
                    }
                    return Err(tls(detail));
                }
            }
        }
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let config = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|err| tls(err.to_string()))?
            .with_root_certificates(roots)
            .with_no_client_auth();
        let config = Arc::new(config);
        self.tls = Some(config.clone());
        Ok(config)
    }
}

impl TransportConnector for BlockingConnector {
    type Conn = NativeConnection<Stream>;

    async fn connect(&mut self, url: &str) -> Result<Self::Conn, TransportError> {
        set_failure(&self.failure, None);
        self.dial(url).await.map_err(|failure| {
            let error = TransportError::new(failure.to_string());
            set_failure(&self.failure, Some(failure));
            error
        })
    }
}

/// The websocket upgrade request for `url`, carrying `token`; `Err` says why
/// there is none: a URL that does not parse or is not `ws://` or `wss://`,
/// cleartext to a host that is not loopback, or a token that is not a header
/// value or is empty.
pub(crate) fn upgrade_request(url: &str, token: &str) -> Result<Request, String> {
    let mut request = url
        .into_client_request()
        .map_err(|err| format!("the URL: {err}"))?;
    if !matches!(request.uri().scheme_str(), Some("ws" | "wss")) {
        return Err("the URL must be ws:// or wss://".to_string());
    }
    refuse_cleartext_bearer(request.uri()).map_err(|err| err.to_string())?;
    insert_bearer_token(request.headers_mut(), token)
        .map_err(|_| "the token is not a valid header value".to_string())?;
    if token.is_empty() {
        return Err("the token is empty".to_string());
    }
    Ok(request)
}

pub(crate) fn set_failure(cell: &DialCell, failure: Option<DialFailure>) {
    *cell.lock().unwrap_or_else(PoisonError::into_inner) = failure;
}

pub(crate) fn take_failure(cell: &DialCell) -> Option<DialFailure> {
    cell.lock().unwrap_or_else(PoisonError::into_inner).take()
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Stream::Tls(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Stream::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            Stream::Tls(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Stream::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Stream::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}
