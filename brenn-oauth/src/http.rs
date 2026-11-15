//! The one outbound HTTP client and the authorization-server calls made
//! through it.
//!
//! The client never follows a redirect: a credential sent to one origin must
//! never be replayed to the origin a `Location` names.

use std::sync::Arc;
use std::time::Duration;

use brenn_lib::config::OAuthProviderConfig;
use brenn_messaging_store::blob::{BlobError, BlobHold, BlobStore, BlobWriter};

use crate::blocking::blocking;

/// Connect timeout for every call.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Total timeout for an authorization-server call (exchange, refresh,
/// userinfo, revoke).
pub const AUTH_SERVER_TIMEOUT: Duration = Duration::from_secs(30);

/// Total timeout for a credentialed API request, body included, whatever the
/// size of a body streamed from or into a blob.
// TODO(api-request-transfer-time-bound): a blob body of `max_blob_bytes` must
// cross the wire inside this one bound.
pub const API_TIMEOUT: Duration = Duration::from_secs(60);

/// Most bytes of an error body carried into a log line.
const LOGGED_BODY_LIMIT: usize = 2048;

/// Fields an authorization server's JSON may carry that must never be logged.
const TOKEN_FIELDS: &[&str] = &["access_token", "refresh_token", "id_token"];

/// Why an authorization-server call produced nothing usable.
#[derive(Debug, PartialEq, Eq)]
pub enum CallFailure {
    /// No HTTP response: connect failure, timeout, reset.
    Transport(String),
    /// A non-2xx response. `body` is redacted and bounded, fit for a log.
    Status { status: u16, body: String },
    /// The token endpoint refused Brenn's client credentials (a 401, or
    /// `invalid_client` / `unauthorized_client`): the operator's client
    /// configuration, not the user's credential, is at fault. `body` as in
    /// [`CallFailure::Status`].
    ClientRefused { status: u16, body: String },
    /// A 2xx response whose body breaks the protocol's contract.
    Contract(String),
}

/// A successful token response, as far as Brenn reads it.
#[derive(Debug)]
pub struct TokenResponse {
    pub access_token: String,
    pub refresh_token: Option<String>,
    /// Seconds until `access_token` expires, when the server says.
    pub expires_in: Option<u64>,
    /// The granted scopes, when the server lists them (RFC 6749 §5.1: absent
    /// means exactly the requested set).
    pub scope: Option<Vec<String>>,
}

/// The client for authorization-server and API calls. Clones share one
/// connection pool.
#[derive(Clone)]
pub struct OAuthHttp {
    client: reqwest::Client,
}

impl Default for OAuthHttp {
    fn default() -> Self {
        Self::new()
    }
}

impl OAuthHttp {
    /// # Panics
    ///
    /// The TLS backend cannot initialise.
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .expect("failed to build the OAuth HTTP client");
        Self { client }
    }

    /// Exchange an authorization code for tokens (RFC 6749 §4.1.3 with the
    /// RFC 7636 verifier).
    pub async fn exchange_code(
        &self,
        provider: &OAuthProviderConfig,
        code: &str,
        verifier: &str,
    ) -> Result<TokenResponse, CallFailure> {
        let form = [
            ("grant_type", "authorization_code"),
            ("code", code),
            ("redirect_uri", &provider.redirect_uri),
            ("client_id", &provider.client_id),
            ("client_secret", provider.client_secret.expose()),
            ("code_verifier", verifier),
        ];
        let request = self
            .client
            .post(&provider.raw.token_url)
            .form(&form)
            .timeout(AUTH_SERVER_TIMEOUT);
        let (status, bytes) = send_raw(request).await?;
        if !status.is_success() {
            return Err(token_endpoint_failure(status, &bytes));
        }
        parse_token_response(&require_success_json(status, &bytes)?)
    }

    /// Obtain a fresh access token with the stored refresh token (RFC 6749
    /// section 6). A `400 invalid_grant` is [`Refreshed::Invalid`]: the provider
    /// no longer honours the user's credential. A refusal of the client itself
    /// is [`CallFailure::ClientRefused`].
    pub async fn refresh(
        &self,
        provider: &OAuthProviderConfig,
        refresh_token: &str,
    ) -> Result<Refreshed, CallFailure> {
        let form = [
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
            ("client_id", &provider.client_id),
            ("client_secret", provider.client_secret.expose()),
        ];
        let request = self
            .client
            .post(&provider.raw.token_url)
            .form(&form)
            .timeout(AUTH_SERVER_TIMEOUT);
        let (status, bytes) = send_raw(request).await?;
        if status.is_success() {
            return parse_token_response(&require_success_json(status, &bytes)?)
                .map(Refreshed::Tokens);
        }
        match token_endpoint_failure(status, &bytes) {
            CallFailure::Status { status: 400, body }
                if oauth_error(&bytes) == Some("invalid_grant") =>
            {
                Ok(Refreshed::Invalid {
                    detail: format!("HTTP 400: {body}"),
                })
            }
            failure => Err(failure),
        }
    }

    /// Send a credentialed API request: `request` with `auth` placed on it,
    /// never following a redirect. Answers once the status and headers have
    /// arrived; the body is read by [`Answered::read`] or
    /// [`Answered::read_into`].
    pub async fn send(&self, request: ApiCall, auth: Auth<'_>) -> Result<Answered, String> {
        let builder = self.credentialed(request, auth);
        let response = builder.send().await.map_err(|e| transport_text(&e))?;
        Ok(Answered {
            status: response.status().as_u16(),
            headers: response.headers().clone(),
            content_length: response.content_length(),
            response,
        })
    }

    /// `request` as a builder with `auth` placed on it: a bearer or key header
    /// marked sensitive.
    fn credentialed(&self, request: ApiCall, auth: Auth<'_>) -> reqwest::RequestBuilder {
        let ApiCall {
            method,
            url,
            mut headers,
            body,
        } = request;
        let mut bearer = None;
        match auth {
            Auth::Bearer(token) => bearer = Some(token),
            Auth::Header { name, value } => {
                let mut value = reqwest::header::HeaderValue::from_str(value)
                    .expect("a key's bytes are checked at boot to be a valid header value");
                value.set_sensitive(true);
                headers.insert(name.clone(), value);
            }
        }
        let mut builder = self
            .client
            .request(method, url)
            .headers(headers)
            .timeout(API_TIMEOUT);
        if let Some(token) = bearer {
            builder = builder.bearer_auth(token);
        }
        match body {
            None => {}
            Some(RequestBody::Bytes(bytes)) => builder = builder.body(bytes),
            Some(RequestBody::Blob(hold)) => {
                builder = builder.body(reqwest::Body::wrap(BlobBody::new(hold)));
            }
        }
        builder
    }

    /// Fetch the userinfo document with `access_token` as the bearer.
    pub async fn userinfo(
        &self,
        provider: &OAuthProviderConfig,
        access_token: &str,
    ) -> Result<serde_json::Value, CallFailure> {
        let request = self
            .client
            .get(&provider.raw.userinfo_url)
            .bearer_auth(access_token)
            .timeout(AUTH_SERVER_TIMEOUT);
        send_json(request).await
    }

    /// Revoke `token` at the provider's revocation endpoint (RFC 7009 §2.1,
    /// form `token=`). `revoke_url` is the provider's configured endpoint.
    pub async fn revoke(&self, revoke_url: &str, token: &str) -> Result<Revoked, CallFailure> {
        let request = self
            .client
            .post(revoke_url)
            .form(&[("token", token)])
            .timeout(AUTH_SERVER_TIMEOUT);
        let (status, bytes) = send_raw(request).await?;
        if status.is_success() {
            return Ok(Revoked::Revoked);
        }
        if status == reqwest::StatusCode::BAD_REQUEST
            && oauth_error(&bytes) == Some("invalid_token")
        {
            return Ok(Revoked::AlreadyInvalid);
        }
        Err(status_failure(status, &bytes))
    }
}

/// What a refresh came to, when the provider answered as the protocol says.
#[derive(Debug)]
pub enum Refreshed {
    Tokens(TokenResponse),
    /// The provider refused the refresh token (`400 invalid_grant`).
    /// `detail` is the status and the redacted body.
    Invalid {
        detail: String,
    },
}

/// Where [`OAuthHttp::api`] places a request's credential.
#[derive(Clone, Copy)]
pub enum Auth<'a> {
    /// A user's access token, as `Authorization: Bearer`.
    Bearer(&'a str),
    /// A deployment key as the whole value of the header `name`.
    Header {
        name: &'a reqwest::header::HeaderName,
        value: &'a str,
    },
}

/// One credentialed request, fully built: the URL from validated segments,
/// headers without credentials.
pub struct ApiCall {
    pub method: reqwest::Method,
    pub url: url::Url,
    pub headers: reqwest::header::HeaderMap,
    pub body: Option<RequestBody>,
}

/// A request body: bytes in memory, or a sealed blob streamed from the store
/// in pieces of [`BLOB_CHUNK_BYTES`].
#[derive(Clone)]
pub enum RequestBody {
    Bytes(Vec<u8>),
    Blob(Arc<BlobHold>),
}

/// The piece a blob is read or written in while it streams to or from the
/// wire: one piece per direction is in memory at a time.
pub const BLOB_CHUNK_BYTES: usize = 256 * 1024;

/// A blob as an HTTP request body: each piece read on the blocking pool, the
/// exact remaining size as the size hint, so the transport sends it as the
/// `Content-Length`.
struct BlobBody {
    hold: Arc<BlobHold>,
    offset: u64,
    reading: Option<tokio::task::JoinHandle<Result<Vec<u8>, BlobError>>>,
}

impl BlobBody {
    fn new(hold: Arc<BlobHold>) -> Self {
        Self {
            hold,
            offset: 0,
            reading: None,
        }
    }

    fn size(&self) -> u64 {
        self.hold.reference().size
    }
}

impl http_body::Body for BlobBody {
    type Data = bytes::Bytes;
    type Error = BlobError;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        use std::task::Poll;
        let size = self.size();
        if self.reading.is_none() {
            if self.offset == size {
                return Poll::Ready(None);
            }
            let len = usize::try_from(size - self.offset)
                .unwrap_or(usize::MAX)
                .min(BLOB_CHUNK_BYTES);
            let hold = Arc::clone(&self.hold);
            let offset = self.offset;
            self.reading = Some(tokio::task::spawn_blocking(move || hold.read(offset, len)));
        }
        let reading = self.reading.as_mut().expect("set above");
        let joined = std::task::ready!(std::pin::Pin::new(reading).poll(cx));
        self.reading = None;
        Poll::Ready(Some(crate::blocking::joined(joined).map(|piece| {
            self.offset += piece.len() as u64;
            http_body::Frame::data(bytes::Bytes::from(piece))
        })))
    }

    fn is_end_stream(&self) -> bool {
        self.reading.is_none() && self.offset == self.size()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        http_body::SizeHint::with_exact(self.size() - self.offset)
    }
}

/// An API response whose status and headers have arrived and whose body has
/// not been read.
pub struct Answered {
    pub status: u16,
    pub headers: reqwest::header::HeaderMap,
    /// The `Content-Length` the response declared, when it did.
    pub content_length: Option<u64>,
    response: reqwest::Response,
}

/// What [`Answered::read_into`] made of a body.
#[derive(Debug)]
pub enum IntoBlob {
    /// The whole body, sealed.
    Sealed(BlobHold),
    /// The body passes the store's largest blob.
    TooLarge,
    /// The body passes what the store's budget has left.
    BudgetExhausted,
}

impl Answered {
    /// The body, reading at most `limit` bytes of it.
    pub async fn read(mut self, limit: usize) -> Result<ApiResponse, String> {
        let mut body = Vec::new();
        let mut truncated = false;
        while let Some(chunk) = self
            .response
            .chunk()
            .await
            .map_err(|e| transport_text(&e))?
        {
            let room = limit - body.len();
            if chunk.len() > room {
                body.extend_from_slice(&chunk[..room]);
                truncated = true;
                break;
            }
            body.extend_from_slice(&chunk);
        }
        Ok(ApiResponse {
            status: self.status,
            headers: self.headers,
            body,
            truncated,
            content_length: self.content_length,
        })
    }

    /// The body written into a new blob of `content_type` as it arrives,
    /// gathered into pieces of [`BLOB_CHUNK_BYTES`], each written on the
    /// blocking pool. A declared length is reserved before the
    /// first byte is read, so a body over a bound is refused before any of it
    /// lands. On every outcome but [`IntoBlob::Sealed`] the unsealed blob is
    /// dropped, which deletes it, and the connection is closed undrained.
    ///
    /// # Panics
    ///
    /// `content_type` is not one the store accepts.
    pub async fn read_into(
        mut self,
        store: &Arc<BlobStore>,
        content_type: &str,
    ) -> Result<IntoBlob, String> {
        let declared = self.content_length;
        let created = {
            let store = Arc::clone(store);
            let content_type = content_type.to_string();
            blocking(move || {
                let mut writer = store.create(&content_type)?;
                if let Some(size) = declared {
                    writer.reserve(size)?;
                }
                Ok::<_, BlobError>(writer)
            })
            .await
        };
        let mut writer = match created {
            Ok(writer) => writer,
            Err(BlobError::InvalidContentType(e)) => {
                panic!("BUG: read_into was handed a content type the store refuses: {e}")
            }
            Err(e) => return refused(e),
        };
        let mut offset: u64 = 0;
        let mut piece: Vec<u8> = Vec::with_capacity(BLOB_CHUNK_BYTES);
        loop {
            let chunk = match self.response.chunk().await {
                Ok(Some(chunk)) => chunk,
                Ok(None) => break,
                Err(e) => {
                    discard(writer).await;
                    return Err(transport_text(&e));
                }
            };
            let mut rest = &chunk[..];
            while !rest.is_empty() {
                let take = rest.len().min(BLOB_CHUNK_BYTES - piece.len());
                piece.extend_from_slice(&rest[..take]);
                rest = &rest[take..];
                if piece.len() < BLOB_CHUNK_BYTES {
                    continue;
                }
                let (returned, emptied, written) = write_piece(writer, piece, offset).await;
                (writer, piece) = (returned, emptied);
                match written {
                    Ok(len) => offset += len,
                    Err(e) => {
                        discard(writer).await;
                        return refused(e);
                    }
                }
            }
        }
        if !piece.is_empty() {
            let (returned, _, written) = write_piece(writer, piece, offset).await;
            writer = returned;
            if let Err(e) = written {
                discard(writer).await;
                return refused(e);
            }
        }
        match writer.seal() {
            Ok(hold) => Ok(IntoBlob::Sealed(hold)),
            Err(refused) => {
                let gap = refused.gap.clone();
                discard(refused.writer).await;
                Err(format!(
                    "the response body ended at byte {} of the {} it declared",
                    gap.start,
                    declared.unwrap_or(gap.end)
                ))
            }
        }
    }
}

/// Write `piece` at `offset` on the blocking pool. The writer comes back, with
/// the piece emptied for reuse and the length written or the store's refusal.
async fn write_piece(
    mut writer: BlobWriter,
    mut piece: Vec<u8>,
    offset: u64,
) -> (BlobWriter, Vec<u8>, Result<u64, BlobError>) {
    blocking(move || {
        let written = writer.write(offset, &piece).map(|()| piece.len() as u64);
        piece.clear();
        (writer, piece, written)
    })
    .await
}

/// Drop an unsealed writer on the blocking pool, where its file is unlinked.
async fn discard(writer: BlobWriter) {
    blocking(move || drop(writer)).await;
}

/// A store refusal or failure while a body was written into a blob.
fn refused(e: BlobError) -> Result<IntoBlob, String> {
    match e {
        BlobError::TooLarge { .. } => Ok(IntoBlob::TooLarge),
        BlobError::BudgetExhausted { .. } => Ok(IntoBlob::BudgetExhausted),
        BlobError::Io(e) => Err(format!("writing the response body into a blob: {e}")),
        e @ (BlobError::InvalidContentType(_)
        | BlobError::TooFragmented
        | BlobError::NotFound
        | BlobError::OutOfRange { .. }) => {
            panic!("BUG: a front-to-back blob write refused: {e}")
        }
    }
}

/// An API response, its body read up to the caller's limit.
pub struct ApiResponse {
    pub status: u16,
    pub headers: reqwest::header::HeaderMap,
    pub body: Vec<u8>,
    /// More body followed the bytes kept.
    pub truncated: bool,
    /// The `Content-Length` the response declared, when it did.
    pub content_length: Option<u64>,
}

/// What a revocation came to, when the credential is dead either way.
#[derive(Debug, PartialEq, Eq)]
pub enum Revoked {
    /// The provider accepted the revocation.
    Revoked,
    /// The provider answered `400 invalid_token`: it already considered the
    /// token dead.
    AlreadyInvalid,
}

/// The `error` field of an OAuth error body (RFC 6749 §5.2), when it is one.
fn oauth_error(bytes: &[u8]) -> Option<&str> {
    #[derive(serde::Deserialize)]
    struct ErrorBody<'a> {
        #[serde(borrow)]
        error: &'a str,
    }
    serde_json::from_slice::<ErrorBody<'_>>(bytes)
        .ok()
        .map(|b| b.error)
}

/// Send, require 2xx, and parse the body as JSON.
async fn send_json(request: reqwest::RequestBuilder) -> Result<serde_json::Value, CallFailure> {
    let (status, bytes) = send_raw(request).await?;
    require_success_json(status, &bytes)
}

/// Send and read the whole body, whatever the status.
async fn send_raw(
    request: reqwest::RequestBuilder,
) -> Result<(reqwest::StatusCode, Vec<u8>), CallFailure> {
    let response = request
        .send()
        .await
        .map_err(|e| CallFailure::Transport(transport_text(&e)))?;
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .map_err(|e| CallFailure::Transport(transport_text(&e)))?;
    Ok((status, bytes.into()))
}

/// A 2xx response's body as JSON; any other status is a [`status_failure`].
fn require_success_json(
    status: reqwest::StatusCode,
    bytes: &[u8],
) -> Result<serde_json::Value, CallFailure> {
    if !status.is_success() {
        return Err(status_failure(status, bytes));
    }
    serde_json::from_slice(bytes)
        .map_err(|e| CallFailure::Contract(format!("response is not JSON: {e}")))
}

/// A response the call does not accept, its body redacted for the log.
fn status_failure(status: reqwest::StatusCode, bytes: &[u8]) -> CallFailure {
    CallFailure::Status {
        status: status.as_u16(),
        body: redacted_body(bytes),
    }
}

/// A token endpoint's non-2xx answer. RFC 6749 section 5.2 reserves 401 for a
/// failed client authentication; `invalid_client` and `unauthorized_client`
/// name the client whatever the status.
fn token_endpoint_failure(status: reqwest::StatusCode, bytes: &[u8]) -> CallFailure {
    let client_refused = status == reqwest::StatusCode::UNAUTHORIZED
        || matches!(
            oauth_error(bytes),
            Some("invalid_client" | "unauthorized_client")
        );
    if client_refused {
        CallFailure::ClientRefused {
            status: status.as_u16(),
            body: redacted_body(bytes),
        }
    } else {
        status_failure(status, bytes)
    }
}

/// A transport error's text and its causes (DNS, connection, TLS), joined by
/// `": "`, with its URL, whose query can carry data, replaced in each.
fn transport_text(e: &reqwest::Error) -> String {
    let chain: Vec<String> =
        std::iter::successors(Some(e as &dyn std::error::Error), |s| s.source())
            .map(ToString::to_string)
            .collect();
    let mut text = chain.join(": ");
    if let Some(url) = e.url() {
        text = text.replace(url.as_str(), "<url>");
    }
    if e.is_timeout() {
        format!("timed out: {text}")
    } else {
        text
    }
}

fn parse_token_response(body: &serde_json::Value) -> Result<TokenResponse, CallFailure> {
    let field = |name: &str| -> Result<Option<String>, CallFailure> {
        match body.get(name) {
            None | Some(serde_json::Value::Null) => Ok(None),
            Some(serde_json::Value::String(s)) => Ok(Some(s.clone())),
            Some(_) => Err(CallFailure::Contract(format!(
                "token response `{name}` is not a string"
            ))),
        }
    };
    let access_token = field("access_token")?
        .filter(|t| !t.is_empty())
        .ok_or_else(|| CallFailure::Contract("token response has no access_token".to_string()))?;
    let expires_in = match body.get("expires_in") {
        None | Some(serde_json::Value::Null) => None,
        Some(v) => Some(v.as_u64().ok_or_else(|| {
            CallFailure::Contract("token response `expires_in` is not a whole number".to_string())
        })?),
    };
    Ok(TokenResponse {
        access_token,
        refresh_token: field("refresh_token")?.filter(|t| !t.is_empty()),
        expires_in,
        scope: field("scope")?.map(|s| s.split_whitespace().map(str::to_string).collect()),
    })
}

/// An error body fit for a log: token fields replaced when it is a JSON
/// object, and cut to [`LOGGED_BODY_LIMIT`] bytes of body plus the truncation
/// marker.
pub fn redacted_body(bytes: &[u8]) -> String {
    let text = match serde_json::from_slice::<serde_json::Value>(bytes) {
        Ok(serde_json::Value::Object(mut map)) => {
            for field in TOKEN_FIELDS {
                if let Some(v) = map.get_mut(*field) {
                    *v = serde_json::Value::String("[redacted]".to_string());
                }
            }
            serde_json::Value::Object(map).to_string()
        }
        _ => String::from_utf8_lossy(bytes).into_owned(),
    };
    brenn_lib::util::truncate_with_marker(&text, LOGGED_BODY_LIMIT)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(url: &str) -> ApiCall {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("accept", "application/json".parse().unwrap());
        ApiCall {
            method: reqwest::Method::GET,
            url: url::Url::parse(url).unwrap(),
            headers,
            body: None,
        }
    }

    fn store(dir: &std::path::Path, max_blob_bytes: u64, budget_bytes: u64) -> Arc<BlobStore> {
        BlobStore::for_tests(
            dir,
            brenn_messaging_store::blob::BlobLimits {
                max_blob_bytes,
                budget_bytes,
            },
        )
    }

    fn sealed(store: &Arc<BlobStore>, bytes: &[u8]) -> Arc<BlobHold> {
        let mut writer = store.create("application/octet-stream").unwrap();
        writer.write(0, bytes).unwrap();
        Arc::new(writer.seal().unwrap())
    }

    /// Every frame of `body`, concatenated, and the size hints seen before
    /// each frame.
    async fn drain(mut body: BlobBody) -> (Vec<u8>, Vec<u64>) {
        use http_body_util::BodyExt;
        let mut out = Vec::new();
        let mut hints = Vec::new();
        loop {
            hints.push(http_body::Body::size_hint(&body).exact().unwrap());
            match body.frame().await {
                None => break,
                Some(frame) => out.extend_from_slice(&frame.unwrap().into_data().unwrap()),
            }
        }
        assert!(http_body::Body::is_end_stream(&body));
        (out, hints)
    }

    #[tokio::test]
    async fn a_blob_body_streams_in_pieces_with_an_exact_size() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(dir.path(), 1 << 30, 1 << 30);
        let bytes: Vec<u8> = (0..BLOB_CHUNK_BYTES * 2 + 7).map(|i| i as u8).collect();
        let (out, hints) = drain(BlobBody::new(sealed(&store, &bytes))).await;
        assert_eq!(out, bytes);
        let total = bytes.len() as u64;
        let chunk = BLOB_CHUNK_BYTES as u64;
        assert_eq!(hints, [total, total - chunk, 7, 0]);

        let (out, hints) = drain(BlobBody::new(sealed(&store, b""))).await;
        assert!(out.is_empty());
        assert_eq!(hints, [0]);
    }

    #[test]
    fn each_credential_placement() {
        let http = OAuthHttp::new();
        let name = reqwest::header::HeaderName::from_static("x-goog-api-key");
        let request = http
            .credentialed(
                call("https://api.example.com/v1/things?a=1"),
                Auth::Header {
                    name: &name,
                    value: "K3Y",
                },
            )
            .build()
            .unwrap();
        let value = &request.headers()["x-goog-api-key"];
        assert_eq!(value, "K3Y");
        assert!(value.is_sensitive());
        assert!(!request.headers().contains_key("authorization"));
        assert_eq!(request.headers()["accept"], "application/json");
        assert_eq!(
            request.url().as_str(),
            "https://api.example.com/v1/things?a=1"
        );

        let request = http
            .credentialed(call("https://api.example.com/v1"), Auth::Bearer("AT"))
            .build()
            .unwrap();
        let value = &request.headers()["authorization"];
        assert_eq!(value, "Bearer AT");
        assert!(value.is_sensitive());
    }

    #[test]
    fn token_response_reads_the_fields_brenn_uses() {
        let t = parse_token_response(&serde_json::json!({
            "access_token": "a", "refresh_token": "r", "expires_in": 3599,
            "scope": "openid  mail", "token_type": "Bearer", "id_token": "x",
        }))
        .unwrap();
        assert_eq!(t.access_token, "a");
        assert_eq!(t.refresh_token.as_deref(), Some("r"));
        assert_eq!(t.expires_in, Some(3599));
        assert_eq!(
            t.scope,
            Some(vec!["openid".to_string(), "mail".to_string()])
        );

        let t = parse_token_response(&serde_json::json!({"access_token": "a"})).unwrap();
        assert_eq!(t.refresh_token, None);
        assert_eq!(t.expires_in, None);
        assert_eq!(t.scope, None);
    }

    #[test]
    fn token_response_contract_violations() {
        for body in [
            serde_json::json!({}),
            serde_json::json!({"access_token": ""}),
            serde_json::json!({"access_token": 5}),
            serde_json::json!({"access_token": "a", "expires_in": "soon"}),
            serde_json::json!({"access_token": "a", "refresh_token": true}),
        ] {
            assert!(
                matches!(parse_token_response(&body), Err(CallFailure::Contract(_))),
                "{body} must be refused"
            );
        }
    }

    #[test]
    fn oauth_error_reads_only_an_error_object() {
        assert_eq!(
            oauth_error(br#"{"error":"invalid_token","error_description":"x"}"#),
            Some("invalid_token")
        );
        assert_eq!(oauth_error(br#"{"error":5}"#), None);
        assert_eq!(oauth_error(b"invalid_token"), None);
        assert_eq!(oauth_error(b""), None);
    }

    #[test]
    fn a_token_endpoint_refusal_of_the_client_is_told_apart() {
        let refused = |status: u16, body: &[u8]| {
            token_endpoint_failure(reqwest::StatusCode::from_u16(status).unwrap(), body)
        };
        for (status, body) in [
            (401, br#"{"error":"invalid_client"}"#.as_slice()),
            (401, br#"{"error":"deleted_client"}"#.as_slice()),
            (401, b"".as_slice()),
            (400, br#"{"error":"invalid_client"}"#.as_slice()),
            (400, br#"{"error":"unauthorized_client"}"#.as_slice()),
        ] {
            assert!(
                matches!(refused(status, body), CallFailure::ClientRefused { status: s, .. } if s == status),
                "{status} {}",
                String::from_utf8_lossy(body)
            );
        }
        for (status, body) in [
            (400, br#"{"error":"invalid_grant"}"#.as_slice()),
            (400, br#"{"error":"invalid_request"}"#.as_slice()),
            (503, b"".as_slice()),
        ] {
            assert!(
                matches!(refused(status, body), CallFailure::Status { status: s, .. } if s == status),
                "{status} {}",
                String::from_utf8_lossy(body)
            );
        }
    }

    #[test]
    fn redaction_replaces_token_fields_and_bounds_length() {
        let body = br#"{"error":"invalid_grant","access_token":"AT","refresh_token":"RT","id_token":"IT"}"#;
        let text = redacted_body(body);
        assert!(text.contains("invalid_grant"));
        for secret in ["AT", "RT", "IT"] {
            assert!(!text.contains(&format!("\"{secret}\"")), "{text}");
        }
        let long = "é".repeat(LOGGED_BODY_LIMIT);
        let text = redacted_body(long.as_bytes());
        let (kept, marker) = text.split_once('…').unwrap();
        assert!(kept.len() <= LOGGED_BODY_LIMIT, "{}", kept.len());
        assert!(
            marker.contains(&format!("[truncated, {} bytes total]", long.len())),
            "{marker}"
        );
    }
}
