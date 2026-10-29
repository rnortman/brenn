//! The one outbound HTTP client and the authorization-server calls made
//! through it.
//!
//! The client never follows a redirect: a credential sent to one origin must
//! never be replayed to the origin a `Location` names.

use std::time::Duration;

use brenn_lib::config::OAuthProviderConfig;

/// Connect timeout for every call.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Total timeout for an authorization-server call (exchange, refresh,
/// userinfo, revoke).
pub const AUTH_SERVER_TIMEOUT: Duration = Duration::from_secs(30);

/// Total timeout for a credentialed API request, body included.
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
    /// never following a redirect, reading at most `limit` body bytes.
    pub async fn api(
        &self,
        request: ApiCall,
        auth: Auth<'_>,
        limit: usize,
    ) -> Result<ApiResponse, String> {
        let builder = self.credentialed(request, auth);
        let mut response = builder.send().await.map_err(|e| transport_text(&e))?;
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let content_length = response.content_length();
        let mut body = Vec::new();
        let mut truncated = false;
        while let Some(chunk) = response.chunk().await.map_err(|e| transport_text(&e))? {
            let room = limit - body.len();
            if chunk.len() > room {
                body.extend_from_slice(&chunk[..room]);
                truncated = true;
                break;
            }
            body.extend_from_slice(&chunk);
        }
        Ok(ApiResponse {
            status,
            headers,
            body,
            truncated,
            content_length,
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
        if let Some(body) = body {
            builder = builder.body(body);
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
    pub body: Option<Vec<u8>>,
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
