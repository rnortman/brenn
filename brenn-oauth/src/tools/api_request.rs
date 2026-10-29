//! `api-request`: an HTTP request to a provider's API with a user's stored
//! credential attached by Brenn.
//!
//! The caller names the request; the credential is never an argument. A
//! conversation caller acts with its owner's credential; a caller with no
//! acting user (a WASM component) with the one user its own grant names, and
//! only to read.
//!
//! The ACL is matched here, not by [`AclClause::matches`]: `path` is a segment
//! template (see [`crate::path_template`]) and every other key exact or `*`.
//! `check_acl` sees only the arguments, so it defers `user` (and an omitted
//! `account`); `execute` matches the full attribute set once the credential is
//! resolved.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use base64ct::{Base64, Encoding};
use brenn_lib::config::OAuthProviderConfig;
use brenn_lib::tools::AclClause;
use brenn_tool_registry::{
    AclDenied, AsyncTool, ENVELOPE_MARGIN, Idempotency, MAX_ASYNC_RESULT_BYTES, ToolClass, ToolCtx,
    ToolDescriptor, ToolError, UserBinding,
};
use chrono::Utc;
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use serde::Deserialize;
use tracing::{error, info, warn};

use crate::http::{ApiCall, ApiResponse, CallFailure, Refreshed};
use crate::path_template::{
    build_url, decode_path, display_path, template_admits, validate_template,
};
use crate::{CallSite, OAuthService};

const NAME: &str = "api-request";

const METHODS: &[&str] = &["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"];

/// Methods a caller with a lent credential may use.
const READ_METHODS: &[&str] = &["GET", "HEAD"];

/// Request headers the caller may not set: the credential and the transport
/// are Brenn's.
const FORBIDDEN_HEADERS: &[&str] = &[
    "authorization",
    "host",
    "content-length",
    "transfer-encoding",
    "cookie",
    "connection",
];

/// An access token expiring sooner than this is refreshed before use.
const REFRESH_MARGIN: chrono::TimeDelta = chrono::TimeDelta::seconds(60);

/// The most bytes the serialized result may take, leaving room for the
/// executor's envelope.
const RESULT_BUDGET: usize = MAX_ASYNC_RESULT_BYTES - ENVELOPE_MARGIN;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ApiArgs {
    provider: String,
    #[serde(default)]
    account: Option<String>,
    method: String,
    host: String,
    path: String,
    #[serde(default)]
    query: BTreeMap<String, QueryValue>,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(default)]
    body: Option<String>,
    #[serde(default)]
    body_base64: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum QueryValue {
    One(String),
    /// The key repeated once per value.
    Many(Vec<String>),
}

/// A provider `api-request` can attach a credential for, by kind.
enum ResolvedProvider<'a> {
    OAuth(&'a OAuthProviderConfig),
}

impl ResolvedProvider<'_> {
    /// Each API origin's `host[:port]` and the origin.
    fn api_origins(&self) -> &[(String, url::Url)] {
        match self {
            ResolvedProvider::OAuth(config) => &config.api_origins,
        }
    }

    fn max_response_bytes(&self) -> usize {
        match self {
            ResolvedProvider::OAuth(config) => config.raw.max_response_bytes,
        }
    }
}

/// The ACL attributes of a call. `None` is a deferred attribute: a clause
/// constraining it is not refused for it.
struct Attrs<'a> {
    provider: &'a str,
    user: Option<&'a str>,
    account: Option<&'a str>,
    method: &'a str,
    host: &'a str,
    segments: &'a [String],
}

/// Whether `clause` admits `attrs`: every constrained key is deferred or
/// matches — `path` as a segment template, every other key exactly or `*`.
fn clause_admits(clause: &AclClause, attrs: &Attrs<'_>) -> bool {
    let exact =
        |allowed: &str, value: Option<&str>| value.is_none_or(|v| allowed == "*" || allowed == v);
    clause.iter().all(|(key, allowed)| match key {
        "provider" => exact(allowed, Some(attrs.provider)),
        "user" => exact(allowed, attrs.user),
        "account" => exact(allowed, attrs.account),
        "method" => exact(allowed, Some(attrs.method)),
        "host" => exact(allowed, Some(attrs.host)),
        "path" => template_admits(allowed, attrs.segments),
        other => panic!(
            "BUG: {NAME} clause constrains {other:?}, which grant validation admits only for \
             declared keys"
        ),
    })
}

fn acl_admits(acl: &[AclClause], attrs: &Attrs<'_>) -> bool {
    acl.iter().any(|clause| clause_admits(clause, attrs))
}

/// The user a caller with no acting user borrows: the one non-`*` `user`
/// value every clause admitting `provider` names.
fn lent_user(acl: &[AclClause], provider: &str) -> Result<String, ToolError> {
    let users: BTreeSet<&str> = acl
        .iter()
        .filter(|clause| {
            clause
                .iter()
                .all(|(key, allowed)| key != "provider" || allowed == "*" || allowed == provider)
        })
        .map(|clause| {
            clause
                .iter()
                .find(|(key, _)| *key == "user")
                .map_or("*", |(_, user)| user)
        })
        .collect();
    match users.into_iter().collect::<Vec<_>>().as_slice() {
        [user] if *user != "*" => Ok((*user).to_string()),
        users => Err(ToolError::Denied(format!(
            "a caller with no acting user borrows the one user its {NAME} clauses for provider \
             {provider:?} name, and every such clause must name it; they name {users:?} \
             (\"*\" is a clause naming none)"
        ))),
    }
}

/// A call checked against everything but the credential.
struct Prepared {
    provider: String,
    account: Option<String>,
    method: String,
    host: String,
    origin: url::Url,
    segments: Vec<String>,
    query: Vec<(String, String)>,
    headers: HeaderMap,
    body: Option<Vec<u8>>,
}

impl Prepared {
    fn attrs<'a>(&'a self, user: Option<&'a str>, account: Option<&'a str>) -> Attrs<'a> {
        Attrs {
            provider: &self.provider,
            user,
            account,
            method: &self.method,
            host: &self.host,
            segments: &self.segments,
        }
    }

    fn line(&self) -> String {
        request_line(&self.provider, &self.method, &self.host, &self.segments)
    }
}

/// The request line as logs and refusals name it.
fn request_line(provider: &str, method: &str, host: &str, segments: &[String]) -> String {
    format!("{provider} {method} {host}{}", display_path(segments))
}

fn prepare(provider: &ResolvedProvider<'_>, args: ApiArgs) -> Result<Prepared, ToolError> {
    let invalid = |detail: String| ToolError::InvalidArgs(format!("{NAME}: {detail}"));
    if !METHODS.contains(&args.method.as_str()) {
        return Err(invalid(format!(
            "method {:?} is not one of {METHODS:?}",
            args.method
        )));
    }
    let (_, origin) = provider
        .api_origins()
        .iter()
        .find(|(host, _)| *host == args.host)
        .ok_or_else(|| {
            let hosts: Vec<&str> = provider
                .api_origins()
                .iter()
                .map(|(host, _)| host.as_str())
                .collect();
            invalid(format!(
                "host {:?} is not one of provider {:?}'s API hosts {hosts:?}",
                args.host, args.provider
            ))
        })?
        .clone();
    let segments = decode_path(&args.path).map_err(invalid)?;
    let query = args
        .query
        .into_iter()
        .flat_map(|(key, value)| match value {
            QueryValue::One(v) => vec![(key, v)],
            QueryValue::Many(vs) => vs.into_iter().map(|v| (key.clone(), v)).collect(),
        })
        .collect();
    let mut headers = HeaderMap::new();
    for (name, value) in &args.headers {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| invalid(format!("{name:?} is not a header name")))?;
        if FORBIDDEN_HEADERS.contains(&name.as_str()) {
            return Err(invalid(format!(
                "header {:?} is Brenn's to set, not the caller's",
                name.as_str()
            )));
        }
        let value = HeaderValue::from_str(value)
            .map_err(|_| invalid(format!("header {:?} has an invalid value", name.as_str())))?;
        headers.append(name, value);
    }
    let body = match (args.body, args.body_base64) {
        (Some(_), Some(_)) => {
            return Err(invalid(
                "`body` and `body_base64` are mutually exclusive".to_string(),
            ));
        }
        (Some(text), None) => Some(text.into_bytes()),
        (None, Some(encoded)) => Some(
            Base64::decode_vec(&encoded)
                .map_err(|_| invalid("`body_base64` is not padded standard base64".to_string()))?,
        ),
        (None, None) => None,
    };
    if body.is_some() && !headers.contains_key(CONTENT_TYPE) {
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    }
    Ok(Prepared {
        provider: args.provider,
        account: args.account,
        method: args.method,
        host: args.host,
        origin,
        segments,
        query,
        headers,
        body,
    })
}

/// A usable access token, or the provider's refusal of the credential.
enum Token {
    Ready {
        access: String,
        /// Obtained after the token a previous attempt was refused with, or
        /// refreshed in this call: a 401 on it is the provider's verdict.
        fresh: bool,
        refreshed: bool,
    },
    Invalid(String),
}

/// The `credential_invalid` result: the user's state, not a tool failure.
fn credential_invalid(provider: &str, account: Option<&str>, detail: &str) -> serde_json::Value {
    serde_json::json!({
        "error": "credential_invalid",
        "provider": provider,
        "account": account,
        "detail": detail,
    })
}

/// A response body as the result carries it.
enum Body {
    Text(String),
    Bytes(Vec<u8>),
}

/// The text of `bytes` when they are UTF-8. A body cut at the read limit may
/// end inside a character; that partial character is dropped.
fn body_of(mut bytes: Vec<u8>, cut: bool) -> Body {
    match std::str::from_utf8(&bytes) {
        Ok(_) => {}
        Err(e) if cut && e.error_len().is_none() => bytes.truncate(e.valid_up_to()),
        Err(_) => return Body::Bytes(bytes),
    }
    Body::Text(String::from_utf8(bytes).expect("checked UTF-8 above"))
}

/// The bytes `c` takes inside a serialized JSON string.
fn escaped_len(c: char) -> usize {
    match c {
        '"' | '\\' | '\n' | '\r' | '\t' | '\u{8}' | '\u{c}' => 2,
        c if u32::from(c) < 0x20 => 6,
        c => c.len_utf8(),
    }
}

/// The response as the tool's result, shortened until it serializes within
/// [`RESULT_BUDGET`].
fn bounded_result(response: ApiResponse) -> Result<serde_json::Value, ToolError> {
    let mut headers = serde_json::Map::new();
    for name in response.headers.keys() {
        let values: Vec<String> = response
            .headers
            .get_all(name)
            .iter()
            .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
            .collect();
        headers.insert(
            name.as_str().to_string(),
            serde_json::Value::String(values.join(", ")),
        );
    }
    let bytes = match (response.content_length, response.truncated) {
        (Some(length), _) => serde_json::json!(length),
        (None, false) => serde_json::json!(response.body.len()),
        (None, true) => serde_json::Value::Null,
    };
    let mut truncated = response.truncated;
    let mut body = body_of(response.body, truncated);
    loop {
        let mut value = serde_json::json!({
            "status": response.status,
            "headers": headers,
            "bytes": bytes,
            "truncated": truncated,
        });
        match &body {
            Body::Text(text) => value["body"] = serde_json::json!(text),
            Body::Bytes(raw) => {
                value["body_base64"] = serde_json::json!(Base64::encode_string(raw))
            }
        }
        let size = serde_json::to_vec(&value)
            .expect("a JSON value serializes")
            .len();
        if size <= RESULT_BUDGET {
            return Ok(value);
        }
        // The room left for the body string, quotes included, given the size
        // of everything else.
        let body_size = match &body {
            Body::Text(text) => serde_json::to_vec(text).expect("a string serializes").len(),
            Body::Bytes(raw) => Base64::encoded_len(raw) + 2,
        };
        let Some(room) = (RESULT_BUDGET + body_size)
            .checked_sub(size)
            .and_then(|room| room.checked_sub(2))
        else {
            return Err(ToolError::Internal(format!(
                "{NAME}: the response's status and headers alone exceed the {RESULT_BUDGET}-byte \
                 result budget"
            )));
        };
        // Each pass keeps strictly less, so the loop ends.
        match &mut body {
            Body::Text(text) => {
                let mut keep = 0;
                let mut used = 0;
                for (at, c) in text.char_indices() {
                    used += escaped_len(c);
                    if used > room {
                        break;
                    }
                    keep = at + c.len_utf8();
                }
                let keep = keep.min(text.floor_char_boundary(text.len().saturating_sub(1)));
                text.truncate(keep);
            }
            Body::Bytes(raw) => {
                let keep = (room / 4 * 3).min(raw.len().saturating_sub(1) / 3 * 3);
                raw.truncate(keep);
            }
        }
        truncated = true;
    }
}

/// The `api-request` tool.
pub struct ApiRequestTool {
    descriptor: ToolDescriptor,
    service: Arc<OAuthService>,
}

impl ApiRequestTool {
    pub fn new(service: Arc<OAuthService>) -> Self {
        Self {
            descriptor: ToolDescriptor {
                name: NAME,
                mcp_name: "mcp__brenn__ApiRequest",
                description: "Make an HTTP request to a provider's API as the user, with the \
                              user's connected credential attached by Brenn (never by you). \
                              Name the provider, the API host and the path; for Google, Gmail \
                              is host `gmail.googleapis.com` under `/gmail/v1/users/me/...` and \
                              Calendar is `www.googleapis.com` under `/calendar/v3/...`. The \
                              response's status, headers and body come back as they are, \
                              non-2xx included; redirects are not followed. Results are \
                              capped: when `truncated` is true, page (`maxResults`, \
                              `pageToken`, `format=metadata`) rather than retry. A result with \
                              `error: \"credential_invalid\"` means the user must connect \
                              again: run `oauth-connect` and relay its steps; after the user \
                              says done, `oauth-accounts` confirms. With several accounts \
                              connected for a provider, name `account`.",
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "provider": {
                            "type": "string",
                            "description": "The provider's configured name, e.g. \"google\".",
                        },
                        "account": {
                            "type": "string",
                            "description": "The account, as `oauth-accounts` lists it. \
                                            Omitted: the user's only account there.",
                        },
                        "method": {
                            "type": "string",
                            "enum": METHODS,
                        },
                        "host": {
                            "type": "string",
                            "description": "host[:port], e.g. \"gmail.googleapis.com\".",
                        },
                        "path": {
                            "type": "string",
                            "description": "Absolute path, percent-encoded within each \
                                            segment, e.g. \"/gmail/v1/users/me/messages\".",
                        },
                        "query": {
                            "type": "object",
                            "additionalProperties": {
                                "oneOf": [
                                    { "type": "string" },
                                    { "type": "array", "items": { "type": "string" } },
                                ],
                            },
                            "description": "Query parameters, unencoded; an array repeats \
                                            the key.",
                        },
                        "headers": {
                            "type": "object",
                            "additionalProperties": { "type": "string" },
                            "description": "Extra request headers. Not Authorization, Host, \
                                            Cookie or transport headers.",
                        },
                        "body": {
                            "type": "string",
                            "description": "Text body; Content-Type defaults to \
                                            application/json.",
                        },
                        "body_base64": {
                            "type": "string",
                            "description": "Binary body, standard padded base64. Not with \
                                            `body`.",
                        },
                    },
                    "required": ["provider", "method", "host", "path"],
                    "additionalProperties": false,
                }),
                class: ToolClass::Async { max_concurrency: 8 },
                acl_keys: &["provider", "user", "account", "method", "host", "path"],
                idempotency: Idempotency::Natural,
                auto_approve: true,
                user_binding: UserBinding::ActingOrLentUser,
            },
            service,
        }
    }

    fn resolve(&self, provider: &str) -> Result<ResolvedProvider<'_>, ToolError> {
        self.service
            .require_provider(provider)
            .map(ResolvedProvider::OAuth)
    }

    /// The account to use when the caller named none: the user's only one.
    async fn sole_account(&self, provider: &str, user: &str) -> Result<Option<String>, ToolError> {
        let accounts = self.service.vault().accounts(provider, user).await;
        match accounts.as_slice() {
            [] => Ok(None),
            [account] => Ok(Some(account.clone())),
            several => Err(ToolError::Denied(format!(
                "several {provider:?} accounts are connected; name one as `account`: {several:?}"
            ))),
        }
    }

    /// A refresh that produced nothing usable, logged once, as the tool's
    /// error.
    fn refresh_failed(
        &self,
        provider: &str,
        user: &str,
        account: &str,
        failure: CallFailure,
    ) -> ToolError {
        let site = CallSite {
            provider,
            call: "refresh",
            user: Some(user),
            account: Some(account),
        };
        let detail = self.service.report_failure(site, failure).into_detail();
        ToolError::Internal(format!(
            "{NAME}: refreshing the access token failed ({detail})"
        ))
    }

    /// The access token for one credential, refreshed under the credential's
    /// lock when it expires within [`REFRESH_MARGIN`] or is `rejected`.
    async fn token(
        &self,
        config: &OAuthProviderConfig,
        provider: &str,
        user: &str,
        account: &str,
        rejected: Option<&str>,
    ) -> Result<Token, ToolError> {
        let vault = self.service.vault();
        let guard = vault.lock(provider, user, account).await;
        let Some(mut credential) = vault.read(guard.path()).await else {
            return Ok(Token::Invalid(
                "no credential is stored for this account".to_string(),
            ));
        };
        let now = Utc::now();
        let expiring = credential.expires_at - now < REFRESH_MARGIN;
        let refused = rejected.is_some_and(|r| r == credential.access_token);
        if !expiring && !refused {
            return Ok(Token::Ready {
                access: credential.access_token,
                fresh: rejected.is_some(),
                refreshed: false,
            });
        }
        let outcome = match self
            .service
            .http()
            .refresh(config, &credential.refresh_token)
            .await
        {
            Ok(Refreshed::Tokens(tokens)) => tokens,
            Ok(Refreshed::Invalid { detail }) => {
                warn!(provider, user, account, detail = %detail, "api-request: refresh refused; credential invalid");
                return Ok(Token::Invalid(format!(
                    "the provider refused the stored credential ({detail})"
                )));
            }
            Err(failure) => return Err(self.refresh_failed(provider, user, account, failure)),
        };
        let Some(expires_at) = crate::expiry(now, outcome.expires_in) else {
            return Err(self.refresh_failed(
                provider,
                user,
                account,
                CallFailure::Contract(format!(
                    "expires_in {} is out of range",
                    outcome.expires_in.unwrap_or_default()
                )),
            ));
        };
        credential.access_token = outcome.access_token;
        credential.expires_at = expires_at;
        if let Some(refresh_token) = outcome.refresh_token {
            credential.refresh_token = refresh_token;
        }
        if let Some(scopes) = outcome.scope {
            credential.scopes = scopes;
        }
        vault.write(&guard, &credential).await;
        info!(
            provider,
            user, account, "api-request: access token refreshed"
        );
        Ok(Token::Ready {
            access: credential.access_token,
            fresh: true,
            refreshed: true,
        })
    }
}

#[async_trait]
impl AsyncTool for ApiRequestTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn validate_clause(&self, clause: &AclClause) -> Result<(), String> {
        match clause.iter().find(|(key, _)| *key == "path") {
            Some((_, template)) => validate_template(template),
            None => Ok(()),
        }
    }

    fn check_acl(&self, args: &serde_json::Value, acl: &[AclClause]) -> Result<(), AclDenied> {
        // Unparseable args and unusable paths are not an ACL question;
        // `execute` refuses them.
        let Ok(args) = serde_json::from_value::<ApiArgs>(args.clone()) else {
            return Ok(());
        };
        let Ok(segments) = decode_path(&args.path) else {
            return Ok(());
        };
        let attrs = Attrs {
            provider: &args.provider,
            user: None,
            account: args.account.as_deref(),
            method: &args.method,
            host: &args.host,
            segments: &segments,
        };
        if acl_admits(acl, &attrs) {
            Ok(())
        } else {
            Err(AclDenied {
                resource: request_line(&args.provider, &args.method, &args.host, &segments),
            })
        }
    }

    async fn execute(
        &self,
        ctx: &ToolCtx,
        args: serde_json::Value,
    ) -> Result<serde_json::Value, ToolError> {
        let started = Instant::now();
        let args: ApiArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArgs(format!("{NAME} args: {e}")))?;
        let provider = self.resolve(&args.provider)?;
        let request = prepare(&provider, args)?;
        let (user, lent) = match &ctx.acting_user {
            Some(acting) => (acting.username.clone(), false),
            None => (lent_user(&ctx.grant.acl, &request.provider)?, true),
        };
        if lent && !READ_METHODS.contains(&request.method.as_str()) {
            return Err(ToolError::Denied(format!(
                "{}: a caller with no acting user may only read ({READ_METHODS:?}) with a lent \
                 credential",
                request.line()
            )));
        }
        let account = match &request.account {
            Some(account) => account.clone(),
            None => match self.sole_account(&request.provider, &user).await? {
                Some(account) => account,
                None => {
                    info!(provider = %request.provider, user = %user, "api-request: no account connected");
                    return Ok(credential_invalid(
                        &request.provider,
                        None,
                        "no account is connected for this provider",
                    ));
                }
            },
        };
        if !acl_admits(&ctx.grant.acl, &request.attrs(Some(&user), Some(&account))) {
            return Err(ToolError::Denied(format!(
                "{} as user {user:?}, account {account:?}",
                request.line()
            )));
        }
        let ResolvedProvider::OAuth(config) = provider;

        let mut rejected: Option<String> = None;
        let mut refreshed = false;
        let response = loop {
            let token = self
                .token(
                    config,
                    &request.provider,
                    &user,
                    &account,
                    rejected.as_deref(),
                )
                .await?;
            let (access, fresh) = match token {
                Token::Ready {
                    access,
                    fresh,
                    refreshed: now,
                } => {
                    refreshed |= now;
                    (access, fresh)
                }
                Token::Invalid(detail) => {
                    info!(provider = %request.provider, user = %user, account = %account, refreshed, "api-request: credential invalid");
                    return Ok(credential_invalid(
                        &request.provider,
                        Some(&account),
                        &detail,
                    ));
                }
            };
            let call = ApiCall {
                method: reqwest::Method::from_bytes(request.method.as_bytes())
                    .expect("METHODS are valid methods"),
                url: build_url(&request.origin, &request.segments, &request.query),
                headers: request.headers.clone(),
                body: request.body.clone(),
            };
            let response = self
                .service
                .http()
                .api(call, &access, provider.max_response_bytes())
                .await
                .map_err(|text| {
                    error!(provider = %request.provider, user = %user, account = %account, error = %text, "api-request: transport failure");
                    ToolError::Internal(format!("{NAME}: {text}"))
                })?;
            if response.status != 401 {
                break response;
            }
            if fresh {
                info!(provider = %request.provider, user = %user, account = %account, refreshed, "api-request: 401 on a fresh token; credential invalid");
                return Ok(credential_invalid(
                    &request.provider,
                    Some(&account),
                    "the provider refused a freshly obtained access token (HTTP 401)",
                ));
            }
            // A 401 on a stored token that had not yet expired: refresh once
            // and resend.
            rejected = Some(access);
        };
        info!(
            provider = %request.provider,
            user = %user,
            account = %account,
            method = %request.method,
            host = %request.host,
            path = %display_path(&request.segments),
            status = response.status,
            bytes = response.content_length.unwrap_or(response.body.len() as u64),
            truncated = response.truncated,
            elapsed_ms = started.elapsed().as_millis() as u64,
            refreshed,
            "api-request"
        );
        bounded_result(response)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use brenn_tool_registry::ActingUser;

    use super::*;
    use crate::testutil::{
        Stub, alice, credential, provider_clause, provider_config, service, service_with_alerts,
        start_stub, tool_ctx,
    };
    use crate::vault::Credential;

    fn clause(pairs: &[(&str, &str)]) -> AclClause {
        AclClause::new(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    fn bob() -> ActingUser {
        ActingUser {
            id: 2,
            username: "bob".to_string(),
        }
    }

    struct Fixture {
        tool: ApiRequestTool,
        svc: Arc<OAuthService>,
        stub: Stub,
        /// `host[:port]` of the stub, as the `host` argument names it.
        host: String,
        _dir: tempfile::TempDir,
    }

    async fn fixture_with(max_response_bytes: usize) -> Fixture {
        let (alerts, _drainer) = brenn_obs::alerting::noop_alert_dispatcher();
        fixture_alerting(max_response_bytes, alerts).await
    }

    async fn fixture_alerting(
        max_response_bytes: usize,
        alerts: brenn_obs::alerting::AlertDispatcher,
    ) -> Fixture {
        let (base, stub) = start_stub().await;
        let (name, mut google) = provider_config("google", &base);
        google.raw.max_response_bytes = max_response_bytes;
        let (svc, dir) = service_with_alerts(vec![(name, google)], alerts);
        svc.vault()
            .store("alice", &credential("google", "alice@example.com", "RT-a"))
            .await;
        Fixture {
            tool: ApiRequestTool::new(svc.clone()),
            svc,
            stub,
            host: base.trim_start_matches("http://").to_string(),
            _dir: dir,
        }
    }

    async fn fixture() -> Fixture {
        fixture_with(brenn_lib::config::DEFAULT_MAX_RESPONSE_BYTES).await
    }

    impl Fixture {
        fn args(&self, method: &str, path: &str) -> serde_json::Value {
            serde_json::json!({
                "provider": "google", "method": method, "host": self.host, "path": path,
            })
        }

        async fn call(
            &self,
            user: Option<ActingUser>,
            acl: Vec<AclClause>,
            args: serde_json::Value,
        ) -> Result<serde_json::Value, ToolError> {
            self.tool.execute(&tool_ctx(user, acl), args).await
        }

        async fn get(&self, path: &str) -> Result<serde_json::Value, ToolError> {
            self.call(
                Some(alice()),
                vec![provider_clause("google")],
                self.args("GET", path),
            )
            .await
        }

        fn seen(&self) -> Vec<crate::testutil::ApiSeen> {
            self.stub.lock().unwrap().api_seen.clone()
        }

        async fn stored(&self, user: &str, account: &str) -> Option<Credential> {
            let vault = self.svc.vault();
            vault
                .read(&vault.credential_path("google", user, account))
                .await
        }

        /// Make alice's stored access token expire in `seconds`.
        async fn expire_in(&self, seconds: i64) {
            let mut c = self.stored("alice", "alice@example.com").await.unwrap();
            c.expires_at = Utc::now() + chrono::TimeDelta::seconds(seconds);
            self.svc.vault().store("alice", &c).await;
        }
    }

    #[tokio::test]
    async fn sends_with_the_bearer_and_returns_the_response() {
        let f = fixture().await;
        {
            let mut s = f.stub.lock().unwrap();
            s.api_body = br#"{"messages":[]}"#.to_vec();
            s.api_headers = vec![("x-thing".to_string(), "1".to_string())];
        }
        let mut args = f.args("GET", "/gmail/v1/users/me/messages");
        args["query"] = serde_json::json!({"maxResults": "5", "labelIds": ["INBOX", "UNREAD"]});
        args["headers"] = serde_json::json!({"Accept": "application/json"});
        let out = f
            .call(Some(alice()), vec![provider_clause("google")], args)
            .await
            .unwrap();
        assert_eq!(out["status"], 200);
        assert_eq!(out["body"], r#"{"messages":[]}"#);
        assert_eq!(out["bytes"], 15);
        assert_eq!(out["truncated"], false);
        assert_eq!(out["headers"]["x-thing"], "1");
        let seen = f.seen();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].method, "GET");
        assert_eq!(
            seen[0].path_and_query,
            "/gmail/v1/users/me/messages?labelIds=INBOX&labelIds=UNREAD&maxResults=5"
        );
        assert_eq!(seen[0].headers["authorization"], "Bearer access");
        assert_eq!(seen[0].headers["accept"], "application/json");
        assert!(f.stub.lock().unwrap().token_forms.is_empty(), "no refresh");
    }

    #[tokio::test]
    async fn a_body_defaults_to_json_and_base64_is_decoded() {
        let f = fixture().await;
        let mut args = f.args("POST", "/drafts");
        args["body"] = serde_json::json!(r#"{"raw":"x"}"#);
        f.call(Some(alice()), vec![provider_clause("google")], args)
            .await
            .unwrap();
        let mut args = f.args("PUT", "/blob");
        args["body_base64"] = serde_json::json!(Base64::encode_string(&[0xff, 0, 1]));
        args["headers"] = serde_json::json!({"Content-Type": "application/octet-stream"});
        f.call(Some(alice()), vec![provider_clause("google")], args)
            .await
            .unwrap();
        let seen = f.seen();
        assert_eq!(seen[0].headers["content-type"], "application/json");
        assert_eq!(seen[0].body, br#"{"raw":"x"}"#);
        assert_eq!(seen[1].method, "PUT");
        assert_eq!(seen[1].headers["content-type"], "application/octet-stream");
        assert_eq!(seen[1].body, [0xff, 0, 1]);
    }

    #[tokio::test]
    async fn invalid_args_are_refused_before_sending() {
        let f = fixture().await;
        let with = |key: &str, value: serde_json::Value| {
            let mut args = f.args("GET", "/a");
            args[key] = value;
            args
        };
        let mut both = f.args("POST", "/a");
        both["body"] = serde_json::json!("x");
        both["body_base64"] = serde_json::json!("eA==");
        let cases = [
            with("host", serde_json::json!("evil.example.com")),
            with("method", serde_json::json!("get")),
            with("method", serde_json::json!("TRACE")),
            with("provider", serde_json::json!("nope")),
            with("path", serde_json::json!("/a/%2e%2e/b")),
            with("path", serde_json::json!("/a%2Fb")),
            with("path", serde_json::json!("/a/")),
            with("headers", serde_json::json!({"Authorization": "Bearer x"})),
            with("headers", serde_json::json!({"cookie": "a=b"})),
            with("headers", serde_json::json!({"Host": "x"})),
            with(
                "headers",
                serde_json::json!({"Transfer-Encoding": "chunked"}),
            ),
            with("headers", serde_json::json!({"bad name": "x"})),
            with("body_base64", serde_json::json!("not base64!")),
            with("extra", serde_json::json!(1)),
            both,
        ];
        for args in cases {
            let out = f
                .call(Some(alice()), vec![provider_clause("*")], args.clone())
                .await;
            assert!(
                matches!(out, Err(ToolError::InvalidArgs(_))),
                "{args}: {out:?}"
            );
        }
        assert!(f.seen().is_empty());
    }

    #[tokio::test]
    async fn the_path_sent_is_the_segments_that_matched() {
        let f = fixture().await;
        let acl = vec![
            clause(&[
                ("provider", "google"),
                ("path", "/calendar/v3/calendars/*/events"),
            ]),
            clause(&[
                ("provider", "google"),
                ("path", "/calendar/v3/calendars/*/events/*"),
            ]),
        ];
        for path in [
            "/calendar/v3/calendars/en.usa%23holiday%40group.v.calendar.google.com/events",
            "/calendar/v3/calendars/x%3F/events/y",
            "/calendar/v3/calendars/a b/events/c:d",
        ] {
            f.call(Some(alice()), acl.clone(), f.args("GET", path))
                .await
                .unwrap();
        }
        let seen: Vec<String> = f.seen().into_iter().map(|s| s.path_and_query).collect();
        assert_eq!(
            seen,
            [
                "/calendar/v3/calendars/en.usa%23holiday@group.v.calendar.google.com/events",
                "/calendar/v3/calendars/x%3F/events/y",
                "/calendar/v3/calendars/a%20b/events/c:d",
            ]
        );
        let out = f
            .call(
                Some(alice()),
                acl,
                f.args("GET", "/calendar/v3/calendars/x/clear"),
            )
            .await;
        assert!(matches!(out, Err(ToolError::Denied(_))), "{out:?}");
    }

    #[tokio::test]
    async fn validate_clause_checks_path_templates() {
        let (svc, _dir) = service(vec![provider_config("google", "http://127.0.0.1:9")]);
        let tool = ApiRequestTool::new(svc);
        assert_eq!(tool.validate_clause(&clause(&[("path", "/a/*/b")])), Ok(()));
        assert_eq!(tool.validate_clause(&clause(&[("path", "*")])), Ok(()));
        assert_eq!(tool.validate_clause(&clause(&[("method", "GET")])), Ok(()));
        for bad in ["a/b", "/a//b", "/a/"] {
            assert!(
                tool.validate_clause(&clause(&[("path", bad)])).is_err(),
                "{bad}"
            );
        }
    }

    #[tokio::test]
    async fn check_acl_defers_user_and_an_omitted_account() {
        let f = fixture().await;
        let args = f.args("GET", "/gmail/v1/users/me/messages");
        let named = clause(&[
            ("provider", "google"),
            ("user", "alice"),
            ("account", "alice@example.com"),
            ("method", "GET"),
        ]);
        assert_eq!(f.tool.check_acl(&args, &[named]), Ok(()));
        let post_only = clause(&[
            ("provider", "google"),
            ("user", "alice"),
            ("method", "POST"),
        ]);
        assert!(f.tool.check_acl(&args, &[post_only]).is_err());
        let mut with_account = args.clone();
        with_account["account"] = serde_json::json!("other@example.com");
        let only_alice = clause(&[("provider", "google"), ("account", "alice@example.com")]);
        assert!(f.tool.check_acl(&with_account, &[only_alice]).is_err());
        // The DSL primitive keeps its exact match for a `path` key.
        let template = clause(&[("path", "/gmail/*")]);
        let attrs = BTreeMap::from([("path".to_string(), "/gmail/v1".to_string())]);
        assert!(!template.matches(&attrs));
    }

    #[tokio::test]
    async fn the_authoritative_check_sees_user_and_account() {
        let f = fixture().await;
        // An omitted account resolves to alice's only one, which the clause names.
        let acl = vec![clause(&[
            ("provider", "google"),
            ("account", "alice@example.com"),
        ])];
        f.call(Some(alice()), acl, f.args("GET", "/a"))
            .await
            .unwrap();
        // The resolved account is not the one the clause names.
        let acl = vec![clause(&[
            ("provider", "google"),
            ("account", "work@example.com"),
        ])];
        let out = f.call(Some(alice()), acl, f.args("GET", "/a")).await;
        assert!(matches!(out, Err(ToolError::Denied(_))), "{out:?}");
        // A clause naming another user does not admit alice.
        let acl = vec![clause(&[("provider", "google"), ("user", "bob")])];
        let out = f.call(Some(alice()), acl, f.args("GET", "/a")).await;
        assert!(matches!(out, Err(ToolError::Denied(_))), "{out:?}");
        assert_eq!(f.seen().len(), 1);
    }

    #[tokio::test]
    async fn each_conversation_caller_uses_its_owners_credential() {
        let f = fixture().await;
        let mut bobs = credential("google", "alice@example.com", "RT-b");
        bobs.access_token = "bobs-access".to_string();
        f.svc.vault().store("bob", &bobs).await;
        f.get("/a").await.unwrap();
        f.call(
            Some(bob()),
            vec![provider_clause("google")],
            f.args("GET", "/a"),
        )
        .await
        .unwrap();
        let auth: Vec<String> = f
            .seen()
            .iter()
            .map(|s| s.headers["authorization"].to_str().unwrap().to_string())
            .collect();
        assert_eq!(auth, ["Bearer access", "Bearer bobs-access"]);
    }

    #[tokio::test]
    async fn a_lent_caller_uses_the_user_its_clause_names() {
        let f = fixture().await;
        let mut bobs = credential("google", "alice@example.com", "RT-b");
        bobs.access_token = "bobs-access".to_string();
        f.svc.vault().store("bob", &bobs).await;
        let lent = vec![clause(&[
            ("provider", "google"),
            ("user", "bob"),
            ("account", "alice@example.com"),
            ("method", "GET"),
        ])];
        let mut args = f.args("GET", "/a");
        args["account"] = serde_json::json!("alice@example.com");
        f.call(None, lent.clone(), args.clone()).await.unwrap();
        assert_eq!(f.seen()[0].headers["authorization"], "Bearer bobs-access");

        // Writes are refused to a lent credential.
        let mut post = args.clone();
        post["method"] = serde_json::json!("POST");
        let out = f
            .call(
                None,
                vec![clause(&[("provider", "google"), ("user", "bob")])],
                post,
            )
            .await;
        assert!(matches!(out, Err(ToolError::Denied(_))), "{out:?}");

        // No user named, a wildcard user, or two users: nobody to lend.
        for acl in [
            vec![provider_clause("google")],
            vec![clause(&[("provider", "google"), ("user", "*")])],
            vec![
                clause(&[("provider", "google"), ("user", "bob")]),
                clause(&[("provider", "google"), ("user", "alice")]),
            ],
            vec![
                clause(&[("provider", "google"), ("user", "bob")]),
                provider_clause("*"),
            ],
        ] {
            let out = f.call(None, acl.clone(), args.clone()).await;
            assert!(matches!(out, Err(ToolError::Denied(_))), "{acl:?}: {out:?}");
        }
        // A clause for another provider does not count.
        let acl = vec![
            clause(&[("provider", "google"), ("user", "bob")]),
            clause(&[("provider", "other"), ("user", "alice")]),
        ];
        f.call(None, acl, args).await.unwrap();
        assert_eq!(f.seen().len(), 2);
    }

    #[tokio::test]
    async fn account_resolution() {
        let f = fixture().await;
        let mut args = f.args("GET", "/a");
        args["account"] = serde_json::json!("nobody@example.com");
        let out = f
            .call(Some(alice()), vec![provider_clause("google")], args)
            .await
            .unwrap();
        assert_eq!(out["error"], "credential_invalid");
        assert_eq!(out["account"], "nobody@example.com");

        let out = f
            .call(
                Some(bob()),
                vec![provider_clause("google")],
                f.args("GET", "/a"),
            )
            .await
            .unwrap();
        assert_eq!(out["error"], "credential_invalid");
        assert_eq!(out["account"], serde_json::Value::Null);

        f.svc
            .vault()
            .store("alice", &credential("google", "work@example.com", "RT-w"))
            .await;
        let out = f.get("/a").await;
        let Err(ToolError::Denied(detail)) = out else {
            panic!("expected Denied, got {out:?}");
        };
        assert!(
            detail.contains("alice@example.com") && detail.contains("work@example.com"),
            "{detail}"
        );
        assert!(f.seen().is_empty());
    }

    #[tokio::test]
    async fn an_expiring_token_is_refreshed_and_stored() {
        let f = fixture().await;
        f.expire_in(30).await;
        f.stub.lock().unwrap().token_body = Some(serde_json::json!({
            "access_token": "AT-2", "refresh_token": "RT-2", "expires_in": 3600,
        }));
        f.get("/a").await.unwrap();
        let forms = f.stub.lock().unwrap().token_forms.clone();
        assert_eq!(forms.len(), 1);
        assert_eq!(forms[0]["grant_type"], "refresh_token");
        assert_eq!(forms[0]["refresh_token"], "RT-a");
        assert_eq!(forms[0]["client_secret"], "client-secret");
        assert_eq!(f.seen()[0].headers["authorization"], "Bearer AT-2");
        let stored = f.stored("alice", "alice@example.com").await.unwrap();
        assert_eq!(stored.access_token, "AT-2");
        assert_eq!(stored.refresh_token, "RT-2");
        assert_eq!(stored.scopes, ["openid", "mail.read"], "no scope: kept");
        assert!(stored.expires_at > Utc::now() + chrono::TimeDelta::minutes(59));
    }

    #[tokio::test]
    async fn concurrent_expiring_calls_refresh_once() {
        let f = fixture().await;
        f.expire_in(0).await;
        let (a, b) = tokio::join!(f.get("/a"), f.get("/b"));
        a.unwrap();
        b.unwrap();
        assert_eq!(f.stub.lock().unwrap().token_forms.len(), 1);
        assert_eq!(f.seen().len(), 2);
    }

    #[tokio::test]
    async fn a_refused_refresh_is_credential_invalid() {
        let f = fixture().await;
        f.expire_in(0).await;
        {
            let mut s = f.stub.lock().unwrap();
            s.token_status = Some(400);
            s.token_body = Some(serde_json::json!({"error": "invalid_grant"}));
        }
        let out = f.get("/a").await.unwrap();
        assert_eq!(out["error"], "credential_invalid");
        assert_eq!(out["account"], "alice@example.com");
        assert!(f.seen().is_empty());
        assert!(
            f.stored("alice", "alice@example.com").await.is_some(),
            "kept"
        );
        let f = fixture().await;
        f.expire_in(0).await;
        f.stub.lock().unwrap().token_status = Some(503);
        let out = f.get("/a").await;
        assert!(matches!(out, Err(ToolError::Internal(_))), "{out:?}");
    }

    /// A refresh answered outside the protocol alerts the operator, as the
    /// connect callback's does; an unavailable provider does not.
    #[tokio::test]
    async fn a_refresh_contract_breach_alerts_the_operator() {
        let (alerts, captured, drainer) = brenn_obs::alerting::make_capturing_alerter();
        let f = fixture_alerting(brenn_lib::config::DEFAULT_MAX_RESPONSE_BYTES, alerts).await;
        f.expire_in(0).await;
        f.stub.lock().unwrap().token_status = Some(503);
        assert!(matches!(f.get("/a").await, Err(ToolError::Internal(_))));
        {
            let mut s = f.stub.lock().unwrap();
            s.token_status = None;
            s.token_body = Some(serde_json::json!({"access_token": 5}));
        }
        let out = f.get("/a").await;
        let Err(ToolError::Internal(detail)) = out else {
            panic!("expected Internal, got {out:?}");
        };
        assert!(detail.contains("access_token"), "{detail}");
        assert!(f.seen().is_empty());
        drop(f);
        drainer.await.unwrap();
        let alerts = captured.lock().unwrap().clone();
        assert_eq!(alerts.len(), 1, "{alerts:?}");
        assert_eq!(alerts[0].0, "OAuth provider google broke the protocol");
        assert!(alerts[0].1.starts_with("refresh: "), "{alerts:?}");
    }

    /// An `account` too long to be a file name names no credential: the
    /// answer is `credential_invalid`, not a vault panic.
    #[tokio::test]
    async fn an_account_too_long_for_a_file_name_is_not_connected() {
        let f = fixture().await;
        let mut args = f.args("GET", "/a");
        args["account"] = serde_json::Value::String("+".repeat(100));
        let out = f
            .call(Some(alice()), vec![provider_clause("google")], args)
            .await
            .unwrap();
        assert_eq!(out["error"], "credential_invalid");
        assert!(f.seen().is_empty());
        assert!(f.stub.lock().unwrap().token_forms.is_empty());
    }

    /// A refused client is the operator's to fix, not the user's: the call is
    /// `Internal`, never `credential_invalid`, the credential is kept, and the
    /// operator is alerted once however many calls hit it.
    #[tokio::test]
    async fn a_refused_client_on_refresh_alerts_the_operator_once() {
        let (alerts, captured, drainer) = brenn_obs::alerting::make_capturing_alerter();
        let f = fixture_alerting(brenn_lib::config::DEFAULT_MAX_RESPONSE_BYTES, alerts).await;
        f.expire_in(0).await;
        for (status, body) in [
            (401, serde_json::json!({"error": "invalid_client"})),
            (401, serde_json::json!({"error": "deleted_client"})),
            (400, serde_json::json!({"error": "unauthorized_client"})),
        ] {
            {
                let mut s = f.stub.lock().unwrap();
                s.token_status = Some(status);
                s.token_body = Some(body.clone());
            }
            let out = f.get("/a").await;
            let Err(ToolError::Internal(detail)) = out else {
                panic!("{status} {body}: expected Internal, got {out:?}");
            };
            assert!(detail.contains("client credentials"), "{detail}");
            assert!(detail.contains(&format!("HTTP {status}")), "{detail}");
        }
        assert!(f.seen().is_empty());
        assert_eq!(
            f.stored("alice", "alice@example.com")
                .await
                .unwrap()
                .refresh_token,
            "RT-a",
            "kept"
        );
        drop(f);
        drainer.await.unwrap();
        let alerts = captured.lock().unwrap().clone();
        assert_eq!(alerts.len(), 1, "{alerts:?}");
        assert_eq!(
            alerts[0].0,
            "OAuth provider google refused Brenn's client credentials"
        );
        assert!(alerts[0].1.starts_with("refresh: HTTP 401"), "{alerts:?}");
    }

    /// An `expires_in` too large to add to the clock is a protocol breach: the
    /// stored credential is left as it was and the operator is alerted.
    #[tokio::test]
    async fn an_out_of_range_expires_in_on_refresh_alerts_and_stores_nothing() {
        let (alerts, captured, drainer) = brenn_obs::alerting::make_capturing_alerter();
        let f = fixture_alerting(brenn_lib::config::DEFAULT_MAX_RESPONSE_BYTES, alerts).await;
        f.expire_in(0).await;
        let before = f.stored("alice", "alice@example.com").await.unwrap();
        f.stub.lock().unwrap().token_body = Some(serde_json::json!({
            "access_token": "AT-2", "refresh_token": "RT-2", "expires_in": u64::MAX,
        }));
        let out = f.get("/a").await;
        let Err(ToolError::Internal(detail)) = out else {
            panic!("expected Internal, got {out:?}");
        };
        assert!(detail.contains("out of range"), "{detail}");
        assert!(f.seen().is_empty());
        assert!(
            f.stored("alice", "alice@example.com").await.unwrap() == before,
            "the stored credential is unchanged"
        );
        drop(f);
        drainer.await.unwrap();
        let alerts = captured.lock().unwrap().clone();
        assert_eq!(alerts.len(), 1, "{alerts:?}");
        assert_eq!(alerts[0].0, "OAuth provider google broke the protocol");
        assert!(alerts[0].1.starts_with("refresh: "), "{alerts:?}");
    }

    #[tokio::test]
    async fn a_401_refreshes_once_then_is_credential_invalid() {
        let f = fixture().await;
        f.stub.lock().unwrap().api_statuses = [401].into();
        let out = f.get("/a").await.unwrap();
        assert_eq!(out["status"], 200, "resent with a fresh token");
        let auth: Vec<String> = f
            .seen()
            .iter()
            .map(|s| s.headers["authorization"].to_str().unwrap().to_string())
            .collect();
        assert_eq!(auth, ["Bearer access", "Bearer AT-1"]);

        f.stub.lock().unwrap().api_statuses = [401].into();
        let out = f.get("/a").await.unwrap();
        assert_eq!(out["status"], 200, "AT-1 refused: refreshed again");

        f.stub.lock().unwrap().api_statuses = [401, 401].into();
        let out = f.get("/a").await.unwrap();
        assert_eq!(out["error"], "credential_invalid");
        assert_eq!(f.stub.lock().unwrap().token_forms.len(), 3);
    }

    #[tokio::test]
    async fn a_redirect_is_returned_not_followed() {
        let f = fixture().await;
        {
            let mut s = f.stub.lock().unwrap();
            s.api_statuses = [302].into();
            s.api_headers = vec![("location".to_string(), "http://127.0.0.1:9/x".to_string())];
        }
        let out = f.get("/a").await.unwrap();
        assert_eq!(out["status"], 302);
        assert_eq!(out["headers"]["location"], "http://127.0.0.1:9/x");
        assert_eq!(f.seen().len(), 1);
    }

    #[tokio::test]
    async fn the_body_is_read_up_to_the_provider_limit() {
        let f = fixture_with(9).await;
        f.stub.lock().unwrap().api_body = "abcdefghé-and-more".as_bytes().to_vec();
        let out = f.get("/a").await.unwrap();
        // The limit falls inside `é`; the partial character is dropped.
        assert_eq!(out["body"], "abcdefgh");
        assert_eq!(out["truncated"], true);
        assert_eq!(out["bytes"], 19);
    }

    #[tokio::test]
    async fn the_result_is_shortened_to_the_budget() {
        let ceiling = brenn_lib::config::MAX_RESPONSE_BYTES_CEILING;
        let f = fixture_with(ceiling).await;
        f.stub.lock().unwrap().api_body = "\"\\".repeat(ceiling / 2).into_bytes();
        let out = f.get("/a").await.unwrap();
        let size = serde_json::to_vec(&out).unwrap().len();
        assert!(size <= RESULT_BUDGET, "{size}");
        assert!(
            size > RESULT_BUDGET - 64,
            "not cut further than needed: {size}"
        );
        assert_eq!(out["truncated"], true);
        assert_eq!(out["bytes"], ceiling);

        let binary: Vec<u8> = (0..ceiling).map(|i| (i % 251) as u8 | 0x80).collect();
        f.stub.lock().unwrap().api_body = binary.clone();
        let out = f.get("/a").await.unwrap();
        assert!(serde_json::to_vec(&out).unwrap().len() <= RESULT_BUDGET);
        assert_eq!(out["truncated"], true);
        let decoded = Base64::decode_vec(out["body_base64"].as_str().unwrap()).unwrap();
        assert!(decoded.len() > RESULT_BUDGET / 2, "{}", decoded.len());
        assert_eq!(decoded[..], binary[..decoded.len()]);
    }

    #[tokio::test]
    async fn descriptor() {
        let f = fixture().await;
        let d = f.tool.descriptor();
        assert_eq!(d.user_binding, UserBinding::ActingOrLentUser);
        assert_eq!(
            d.acl_keys,
            &["provider", "user", "account", "method", "host", "path"]
        );
        assert!(d.auto_approve);
        assert!(matches!(d.class, ToolClass::Async { max_concurrency: 8 }));
        assert_eq!(d.idempotency, Idempotency::Natural);
    }
}
