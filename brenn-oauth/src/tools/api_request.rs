//! `api-request`: an HTTP request to a provider's API with a credential
//! attached by Brenn — a user's stored OAuth credential, or for an
//! `api_key_provider` either the deployment's key or a user's.
//!
//! The caller names the request; the credential is never an argument. On an
//! OAuth provider or a key provider with per-user accounts a caller with an
//! acting user acts with that user's credential: a conversation's owner, or
//! the user of the provided-tool call a component's call serves, stamped by
//! the executor. A caller with no acting user (a WASM component calling on its
//! own account) borrows the one user its grant names. The call's `account`
//! picks among the user's accounts. On a key provider holding the deployment's
//! key every caller sends the same key.
//!
//! A caller with no acting user only reads, on either kind: its call can be
//! re-run from its channel window. A stamped component call is its user's
//! call, the method bounded by its clause like any other: a re-run of it
//! cannot act, because the pending call it serves is gone once completed,
//! expired or lost to a restart, and the executor refuses a serving id that is
//! not pending.
//!
//! The ACL is matched here, not by [`AclClause::matches`]: `path` is a segment
//! template (see [`crate::path_template`]) and every other key exact or `*`.
//! `check_acl` sees only the arguments, so on an OAuth or per-user key
//! provider it defers `user` (and an omitted `account`); `execute` matches the
//! full attribute set once the credential is resolved. A deployment-key call
//! has no `user` or `account`: a clause constraining either never admits it.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use base64ct::{Base64, Encoding};
use brenn_lib::config::{
    API_REQUEST_TOOL, AccountKeys, AccountLookup, ApiKeyProviderConfig, ApiKeyTable, ApiKeys,
    OAuthProviderConfig, TRANSPORT_HEADERS,
};
use brenn_lib::tools::AclClause;
use brenn_tool_registry::{
    AclDenied, AsyncTool, ENVELOPE_MARGIN, Idempotency, MAX_ASYNC_RESULT_BYTES, ToolClass, ToolCtx,
    ToolDescriptor, ToolError, UserBinding,
};
use chrono::Utc;
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use serde::Deserialize;
use tracing::{error, info, warn};

use crate::http::{ApiCall, ApiResponse, Auth, CallFailure, OAuthHttp, Refreshed};
use crate::path_template::{
    ANY_PATH, RequestPath, build_url, decode_path, display_path, template_admits, validate_template,
};
use crate::{CallSite, OAuthService};

const NAME: &str = API_REQUEST_TOOL;

const METHODS: &[&str] = &[
    "GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "PROPFIND", "REPORT",
];

/// Methods a caller with no acting user may use. `PROPFIND` and `REPORT` read
/// too and are deliberately absent, left to the `consumer-own-credentials`
/// TODO entry.
const READ_METHODS: &[&str] = &["GET", "HEAD"];

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
    OAuth {
        service: &'a OAuthService,
        config: &'a OAuthProviderConfig,
    },
    Key(&'a ApiKeyProviderConfig),
}

impl ResolvedProvider<'_> {
    /// Each API origin's `host[:port]` and the origin.
    fn api_origins(&self) -> &[(String, url::Url)] {
        match self {
            ResolvedProvider::OAuth { config, .. } => &config.api_origins,
            ResolvedProvider::Key(config) => &config.api_origins,
        }
    }

    /// The header Brenn sends the key in, for a key provider.
    fn key_header(&self) -> Option<&HeaderName> {
        match self {
            ResolvedProvider::OAuth { .. } => None,
            ResolvedProvider::Key(config) => Some(&config.header),
        }
    }

    /// Whether this is a key provider holding the deployment's one key.
    fn is_deployment_key(&self) -> bool {
        matches!(self, ResolvedProvider::Key(config) if is_deployment_key(config))
    }
}

fn is_deployment_key(config: &ApiKeyProviderConfig) -> bool {
    matches!(config.keys, ApiKeys::Deployment(_))
}

/// One ACL attribute of a call.
#[derive(Clone, Copy)]
enum Attr<'a> {
    /// Not known yet: any constraint on it admits.
    Deferred,
    /// The call has none: no constraint on it admits, `*` included.
    Absent,
    /// Matched by `*` or this value.
    Is(&'a str),
}

impl<'a> From<Option<&'a str>> for Attr<'a> {
    fn from(value: Option<&'a str>) -> Self {
        value.map_or(Attr::Deferred, Attr::Is)
    }
}

/// The ACL attributes of a call.
struct Attrs<'a> {
    provider: &'a str,
    user: Attr<'a>,
    account: Attr<'a>,
    method: &'a str,
    host: &'a str,
    path: &'a RequestPath,
}

/// Whether `clause` admits `attrs`: every constrained key is deferred or
/// matches — `path` as a segment template, every other key exactly or `*`. A
/// clause naming no `path` is matched as `path = "*"`, which admits no path
/// ending in `/`.
fn clause_admits(clause: &AclClause, attrs: &Attrs<'_>) -> bool {
    let path_template = clause
        .iter()
        .find(|(key, _)| *key == "path")
        .map_or(ANY_PATH, |(_, template)| template);
    if !template_admits(path_template, attrs.path) {
        return false;
    }
    let exact = |allowed: &str, value: Attr<'_>| match value {
        Attr::Deferred => true,
        Attr::Absent => false,
        Attr::Is(v) => allowed == "*" || allowed == v,
    };
    clause.iter().all(|(key, allowed)| match key {
        "provider" => exact(allowed, Attr::Is(attrs.provider)),
        "user" => exact(allowed, attrs.user),
        "account" => exact(allowed, attrs.account),
        "method" => exact(allowed, Attr::Is(attrs.method)),
        "host" => exact(allowed, Attr::Is(attrs.host)),
        "path" => true,
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

/// The provider named `name`, of either kind, from `oauth` and the key
/// snapshot `keys`, or the refusal naming every configured provider with its
/// kind.
fn resolve<'a>(
    oauth: Option<&'a OAuthService>,
    keys: &'a BTreeMap<String, ApiKeyProviderConfig>,
    name: &str,
) -> Result<ResolvedProvider<'a>, ToolError> {
    if let Some(config) = keys.get(name) {
        return Ok(ResolvedProvider::Key(config));
    }
    if let Some(service) = oauth
        && let Some(config) = service.provider(name)
    {
        return Ok(ResolvedProvider::OAuth { service, config });
    }
    let mut configured: Vec<String> = oauth
        .iter()
        .flat_map(|service| service.provider_names())
        .map(|name| format!("{name} (oauth_provider)"))
        .chain(keys.keys().map(|name| format!("{name} (api_key_provider)")))
        .collect();
    configured.sort();
    Err(ToolError::InvalidArgs(format!(
        "unknown provider {name:?}; configured: {configured:?}"
    )))
}

/// A call checked against everything but the credential.
struct Prepared {
    provider: String,
    account: Option<String>,
    method: String,
    host: String,
    origin: url::Url,
    path: RequestPath,
    query: Vec<(String, String)>,
    headers: HeaderMap,
    body: Option<Vec<u8>>,
}

impl Prepared {
    fn attrs<'a>(&'a self, user: Attr<'a>, account: Attr<'a>) -> Attrs<'a> {
        Attrs {
            provider: &self.provider,
            user,
            account,
            method: &self.method,
            host: &self.host,
            path: &self.path,
        }
    }

    fn line(&self) -> String {
        request_line(&self.provider, &self.method, &self.host, &self.path)
    }

    /// The user whose credential a per-user call uses: the acting user, or
    /// the one the grant lends a caller with none.
    fn credential_user(&self, ctx: &ToolCtx) -> Result<String, ToolError> {
        match &ctx.acting_user {
            Some(acting) => Ok(acting.username.clone()),
            None => lent_user(&ctx.grant.acl, &self.provider),
        }
    }

    /// Hold the call, its credential resolved to `user`'s `account`, to the
    /// grant's clauses.
    fn admit(&self, ctx: &ToolCtx, user: &str, account: &str) -> Result<(), ToolError> {
        if acl_admits(
            &ctx.grant.acl,
            &self.attrs(Attr::Is(user), Attr::Is(account)),
        ) {
            Ok(())
        } else {
            Err(ToolError::Denied(format!(
                "{} as user {user:?}, account {account:?}",
                self.line()
            )))
        }
    }

    /// The request as sent, before its credential is placed.
    fn call(&self) -> ApiCall {
        ApiCall {
            method: reqwest::Method::from_bytes(self.method.as_bytes())
                .expect("METHODS are valid methods"),
            url: build_url(&self.origin, &self.path, &self.query),
            headers: self.headers.clone(),
            body: self.body.clone(),
        }
    }

    /// A request that got no answer, logged with its scrubbed text, as the
    /// tool's error. `account` is `-` on a deployment-key provider.
    fn transport_failed(&self, user: &str, account: &str, text: &str) -> ToolError {
        error!(
            provider = %self.provider,
            user = %user,
            account = %account,
            error = %text,
            "api-request: transport failure"
        );
        ToolError::Internal(format!("{NAME}: {text}"))
    }

    /// The completion line of an answered request. `account` is `-` on a
    /// deployment-key provider, `refreshed` set on an OAuth provider only; the
    /// query is never logged.
    fn log_sent(
        &self,
        user: &str,
        account: &str,
        refreshed: Option<bool>,
        response: &ApiResponse,
        started: Instant,
    ) {
        info!(
            provider = %self.provider,
            user = %user,
            account = %account,
            method = %self.method,
            host = %self.host,
            path = %display_path(&self.path),
            status = response.status,
            bytes = response.content_length.unwrap_or(response.body.len() as u64),
            truncated = response.truncated,
            elapsed_ms = started.elapsed().as_millis() as u64,
            refreshed,
            "api-request"
        );
    }
}

/// The request line as logs and refusals name it.
fn request_line(provider: &str, method: &str, host: &str, path: &RequestPath) -> String {
    format!("{provider} {method} {host}{}", display_path(path))
}

fn prepare(provider: &ResolvedProvider<'_>, args: ApiArgs) -> Result<Prepared, ToolError> {
    let invalid = |detail: String| ToolError::InvalidArgs(format!("{NAME}: {detail}"));
    let key_header = provider.key_header();
    if provider.is_deployment_key() && args.account.is_some() {
        return Err(invalid(format!(
            "`account` applies to OAuth providers and key providers with `account` blocks; \
             {:?} holds one deployment key",
            args.provider
        )));
    }
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
    let path = decode_path(&args.path).map_err(invalid)?;
    let reserved = |what: &str, name: &str| {
        invalid(format!(
            "{what} {name:?} is Brenn's to set, not the caller's"
        ))
    };
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
        if name == reqwest::header::AUTHORIZATION
            || TRANSPORT_HEADERS.contains(&name.as_str())
            || key_header == Some(&name)
        {
            return Err(reserved("header", name.as_str()));
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
        path,
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

/// The `api-request` tool, over the OAuth providers of `oauth` and the key
/// providers `keys` holds when a call reads it.
pub struct ApiRequestTool {
    descriptor: ToolDescriptor,
    oauth: Option<Arc<OAuthService>>,
    keys: ApiKeyTable,
    /// Sends every API request: a clone of the service's client when there is
    /// a service, so requests and refreshes share one pool.
    http: OAuthHttp,
}

impl ApiRequestTool {
    /// # Panics
    ///
    /// A provider name is declared as both kinds in the table as it stands.
    pub fn new(oauth: Option<Arc<OAuthService>>, keys: ApiKeyTable) -> Self {
        if let Some(service) = &oauth {
            let initial = keys.load();
            for name in service.provider_names() {
                assert!(
                    !initial.contains_key(name),
                    "BUG: provider {name:?} is both an oauth_provider and an api_key_provider; \
                     config lowering refuses that"
                );
            }
        }
        let http = oauth
            .as_ref()
            .map_or_else(OAuthHttp::new, |service| service.http().clone());
        Self {
            descriptor: ToolDescriptor {
                name: NAME,
                mcp_name: "mcp__brenn__ApiRequest",
                description: "Make an HTTP request to a provider's API with a credential Brenn \
                              attaches (never you): the user's connected account for an OAuth \
                              provider, or for an API-key provider the deployment's key or \
                              the user's own. Name \
                              the provider, the API host and the path; the operator's \
                              instructions say which providers and hosts exist. The response's \
                              status, headers and body come back as they are, non-2xx \
                              included; redirects are not followed. Results are capped: when \
                              `truncated` is true, page or ask for fewer fields rather than \
                              retry. OAuth providers only: a result with `error: \
                              \"credential_invalid\"` means the user must connect again: run \
                              `oauth-connect` and relay its steps; after the user says done, \
                              `oauth-accounts` confirms. With several accounts for a provider, \
                              name `account`; `account` is refused on an API-key provider \
                              holding one deployment key.",
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "provider": {
                            "type": "string",
                            "description": "The provider's configured name, e.g. \"google\" \
                                            or \"google-maps\".",
                        },
                        "account": {
                            "type": "string",
                            "description": "For an OAuth provider, the account as \
                                            `oauth-accounts` lists it; for a key provider with \
                                            per-user accounts, the account's configured name. \
                                            Omitted: the user's only account, or the one the \
                                            operator marked default. Refused on a key provider \
                                            holding one deployment key.",
                        },
                        "method": {
                            "type": "string",
                            "enum": METHODS,
                        },
                        "host": {
                            "type": "string",
                            "description": "host[:port], e.g. \"api.example.com\".",
                        },
                        "path": {
                            "type": "string",
                            "description": "Absolute path, percent-encoded within each \
                                            segment, e.g. \"/v1/things\"; a trailing `/` \
                                            is sent as written.",
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
                                            Cookie, transport headers, or the provider's key \
                                            header.",
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
            oauth,
            keys,
            http,
        }
    }

    /// The account to use when the caller named none: the user's only one.
    async fn sole_account(
        service: &OAuthService,
        provider: &str,
        user: &str,
    ) -> Result<Option<String>, ToolError> {
        let accounts = service.vault().accounts(provider, user).await;
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
        service: &OAuthService,
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
        let detail = service.report_failure(site, failure).into_detail();
        ToolError::Internal(format!(
            "{NAME}: refreshing the access token failed ({detail})"
        ))
    }

    /// The access token for one credential, refreshed under the credential's
    /// lock when it expires within [`REFRESH_MARGIN`] or is `rejected`.
    async fn token(
        service: &OAuthService,
        config: &OAuthProviderConfig,
        provider: &str,
        user: &str,
        account: &str,
        rejected: Option<&str>,
    ) -> Result<Token, ToolError> {
        let vault = service.vault();
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
        let outcome = match service
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
            Err(failure) => {
                return Err(Self::refresh_failed(
                    service, provider, user, account, failure,
                ));
            }
        };
        let Some(expires_at) = crate::expiry(now, outcome.expires_in) else {
            return Err(Self::refresh_failed(
                service,
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

    /// The path template only: the clause rules that read the key providers
    /// are a document rule, checked where the document is resolved.
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
        let Ok(path) = decode_path(&args.path) else {
            return Ok(());
        };
        // A pre-screen: `execute` re-admits the call against its own snapshot.
        let deployment_key = self
            .keys
            .load()
            .get(&args.provider)
            .is_some_and(is_deployment_key);
        let (user, account) = if deployment_key {
            (Attr::Absent, Attr::Absent)
        } else {
            (Attr::Deferred, Attr::from(args.account.as_deref()))
        };
        let attrs = Attrs {
            provider: &args.provider,
            user,
            account,
            method: &args.method,
            host: &args.host,
            path: &path,
        };
        if acl_admits(acl, &attrs) {
            Ok(())
        } else {
            Err(AclDenied {
                resource: request_line(&args.provider, &args.method, &args.host, &path),
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
        // One snapshot for the whole call: the request completes with the key
        // it resolved here, whatever a reload stores meanwhile.
        let keys = self.keys.load();
        let provider = resolve(self.oauth.as_deref(), &keys, &args.provider)?;
        let request = prepare(&provider, args)?;
        // A caller with no acting user can be replayed from its channel
        // window, so it only reads, whatever the credential. A stamped
        // component call is its user's and is bounded by its clause alone.
        // TODO(consumer-own-credentials): no configured way lets a consumer
        // with no acting user write, with any credential.
        if ctx.acting_user.is_none() && !READ_METHODS.contains(&request.method.as_str()) {
            return Err(ToolError::Denied(format!(
                "{}: a caller with no acting user may only read ({READ_METHODS:?})",
                request.line()
            )));
        }
        match provider {
            ResolvedProvider::Key(config) => match &config.keys {
                ApiKeys::Deployment(key) => {
                    self.execute_key(ctx, config, key.expose(), request, started)
                        .await
                }
                ApiKeys::Accounts(keys) => {
                    self.execute_key_accounts(ctx, config, keys, request, started)
                        .await
                }
            },
            ResolvedProvider::OAuth { service, config } => {
                self.execute_oauth(ctx, service, config, request, started)
                    .await
            }
        }
    }
}

impl ApiRequestTool {
    /// One request with the deployment's key: sent once, its answer returned
    /// whatever the status.
    async fn execute_key(
        &self,
        ctx: &ToolCtx,
        config: &ApiKeyProviderConfig,
        key: &str,
        request: Prepared,
        started: Instant,
    ) -> Result<serde_json::Value, ToolError> {
        if !acl_admits(&ctx.grant.acl, &request.attrs(Attr::Absent, Attr::Absent)) {
            return Err(ToolError::Denied(format!(
                "{} (a deployment-key call has no user or account)",
                request.line()
            )));
        }
        let user = ctx
            .acting_user
            .as_ref()
            .map_or("-", |acting| acting.username.as_str());
        self.send_key(config, key, &request, user, "-", started)
            .await
    }

    /// One request with the acting (or lent) user's key for the account the
    /// call names, or their default or only one: sent once, its answer
    /// returned whatever the status.
    async fn execute_key_accounts(
        &self,
        ctx: &ToolCtx,
        config: &ApiKeyProviderConfig,
        keys: &AccountKeys,
        request: Prepared,
        started: Instant,
    ) -> Result<serde_json::Value, ToolError> {
        let user = request.credential_user(ctx)?;
        let provider = &request.provider;
        // TODO(per-agent-account-bound): every account of the acting user is
        // reachable here; no bound from the agent's grant of the provided tool
        // reaches the nested call to narrow it.
        let (account, key) = match keys.lookup(&user, request.account.as_deref()) {
            AccountLookup::Found { account, key } => (account, key.expose()),
            AccountLookup::NoUser => {
                return Err(ToolError::Denied(format!(
                    "{}: api_key_provider {provider} holds no account for user {user}; the \
                     operator adds an `account` block naming the user and reloads Brenn",
                    request.line()
                )));
            }
            AccountLookup::NoSuchAccount { available } => {
                return Err(ToolError::Denied(format!(
                    "{}: {provider} has no account {:?} for user {user}; theirs: {available:?}",
                    request.line(),
                    request.account.as_deref().unwrap_or_default(),
                )));
            }
            AccountLookup::Ambiguous { available } => {
                return Err(ToolError::Denied(format!(
                    "{}: user {user} has several {provider} accounts and none is the default; \
                     name one as `account`: {available:?}",
                    request.line()
                )));
            }
        };
        request.admit(ctx, &user, account)?;
        self.send_key(config, key, &request, &user, account, started)
            .await
    }

    /// Send `request` once with `key` in the provider's header, and log it
    /// under `user` and `account` (`-` where there is none).
    async fn send_key(
        &self,
        config: &ApiKeyProviderConfig,
        key: &str,
        request: &Prepared,
        user: &str,
        account: &str,
        started: Instant,
    ) -> Result<serde_json::Value, ToolError> {
        let auth = Auth::Header {
            name: &config.header,
            value: key,
        };
        let response = self
            .http
            .api(request.call(), auth, config.raw.max_response_bytes)
            .await
            .map_err(|text| request.transport_failed(user, account, &text))?;
        request.log_sent(user, account, None, &response, started);
        bounded_result(response)
    }

    /// One request with a user's OAuth credential, refreshed and resent once
    /// on a 401 to a token that was not fresh.
    async fn execute_oauth(
        &self,
        ctx: &ToolCtx,
        service: &OAuthService,
        config: &OAuthProviderConfig,
        request: Prepared,
        started: Instant,
    ) -> Result<serde_json::Value, ToolError> {
        let user = request.credential_user(ctx)?;
        let account = match &request.account {
            Some(account) => account.clone(),
            None => match Self::sole_account(service, &request.provider, &user).await? {
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
        request.admit(ctx, &user, &account)?;

        let mut rejected: Option<String> = None;
        let mut refreshed = false;
        let response = loop {
            let token = Self::token(
                service,
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
            let response = self
                .http
                .api(
                    request.call(),
                    Auth::Bearer(&access),
                    config.raw.max_response_bytes,
                )
                .await
                .map_err(|text| request.transport_failed(&user, &account, &text))?;
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
        request.log_sent(&user, &account, Some(refreshed), &response, started);
        bounded_result(response)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use brenn_tool_registry::ActingUser;

    use super::*;
    use crate::testutil::{
        KEY, Stub, account_key, alice, credential, key_accounts_provider_config,
        key_provider_config, provider_clause, provider_config, service, service_with_alerts,
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

    fn table(keys: BTreeMap<String, ApiKeyProviderConfig>) -> ApiKeyTable {
        ApiKeyTable::new(Arc::new(keys))
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
            tool: ApiRequestTool::new(Some(svc.clone()), ApiKeyTable::empty()),
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
            with("method", serde_json::json!("propfind")),
            with("method", serde_json::json!("MKCOL")),
            with("provider", serde_json::json!("nope")),
            with("path", serde_json::json!("/a/%2e%2e/b")),
            with("path", serde_json::json!("/a%2Fb")),
            with("path", serde_json::json!("/a//")),
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
        let tool = ApiRequestTool::new(Some(svc), ApiKeyTable::empty());
        assert_eq!(tool.validate_clause(&clause(&[("path", "/a/*/b")])), Ok(()));
        assert_eq!(tool.validate_clause(&clause(&[("path", "*")])), Ok(()));
        assert_eq!(tool.validate_clause(&clause(&[("path", "/a/")])), Ok(()));
        assert_eq!(tool.validate_clause(&clause(&[("method", "GET")])), Ok(()));
        for bad in ["a/b", "/a//b", "/a//"] {
            assert!(
                tool.validate_clause(&clause(&[("path", bad)])).is_err(),
                "{bad}"
            );
        }
    }

    /// A path ending in `/` is sent as written, and only a clause whose `path`
    /// template ends in `/` admits it.
    #[tokio::test]
    async fn a_slashed_path_is_admitted_by_a_slashed_template_alone() {
        let f = fixture().await;
        let home = "/dav/calendars/user/alice@example.com/";
        let slashed = clause(&[("provider", "google"), ("path", "/dav/calendars/user/*/")]);
        f.call(
            Some(alice()),
            vec![slashed.clone()],
            f.args("PROPFIND", home),
        )
        .await
        .unwrap();
        assert_eq!(
            f.tool
                .check_acl(&f.args("PROPFIND", home), std::slice::from_ref(&slashed)),
            Ok(())
        );
        let seen = f.seen();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].method, "PROPFIND");
        assert_eq!(seen[0].path_and_query, home);

        for acl in [
            clause(&[("provider", "google"), ("path", "*")]),
            provider_clause("google"),
            clause(&[("provider", "google"), ("path", "/dav/calendars/user/*")]),
        ] {
            assert!(
                f.tool
                    .check_acl(&f.args("PROPFIND", home), std::slice::from_ref(&acl))
                    .is_err(),
                "{acl:?}"
            );
            let detail = denied(
                f.call(Some(alice()), vec![acl.clone()], f.args("PROPFIND", home))
                    .await,
            );
            assert!(detail.contains(home), "{detail}");
        }
        let out = f
            .call(
                Some(alice()),
                vec![slashed],
                f.args("PROPFIND", "/dav/calendars/user/alice@example.com"),
            )
            .await;
        assert!(matches!(out, Err(ToolError::Denied(_))), "{out:?}");
        assert_eq!(f.seen().len(), 1);
    }

    /// The root path `/` is sent as `/`, under a clause whose `path` is `/`.
    #[tokio::test]
    async fn the_root_is_admitted_by_the_root_template_alone() {
        let f = fixture().await;
        let out = f
            .call(
                Some(alice()),
                vec![provider_clause("google")],
                f.args("GET", "/"),
            )
            .await;
        assert!(matches!(out, Err(ToolError::Denied(_))), "{out:?}");
        f.call(
            Some(alice()),
            vec![clause(&[("provider", "google"), ("path", "/")])],
            f.args("GET", "/"),
        )
        .await
        .unwrap();
        let seen = f.seen();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path_and_query, "/");
    }

    /// `PROPFIND` and `REPORT` carry the caller's headers and XML body, and a
    /// `207` comes back as it was answered.
    #[tokio::test]
    async fn propfind_and_report_are_sent_and_answered() {
        let f = fixture().await;
        let multistatus = "<?xml version=\"1.0\"?>\r\n<d:multistatus xmlns:d=\"DAV:\">\r\n\
                           </d:multistatus>\r\n";
        {
            let mut s = f.stub.lock().unwrap();
            s.api_statuses = [207, 207].into();
            s.api_body = multistatus.as_bytes().to_vec();
            s.api_headers = vec![
                ("etag".to_string(), "\"e1\"".to_string()),
                (
                    "content-type".to_string(),
                    "application/xml; charset=utf-8".to_string(),
                ),
            ];
        }
        let acl = vec![clause(&[("provider", "google"), ("path", "/dav/*/")])];
        let request = "<d:propfind xmlns:d=\"DAV:\"><d:prop><d:getetag/></d:prop></d:propfind>";
        for (method, depth) in [("PROPFIND", "1"), ("REPORT", "0")] {
            let mut args = f.args(method, "/dav/calendar/");
            args["headers"] = serde_json::json!({
                "Depth": depth,
                "Content-Type": "application/xml; charset=utf-8",
            });
            args["body"] = serde_json::json!(request);
            let out = f.call(Some(alice()), acl.clone(), args).await.unwrap();
            assert_eq!(out["status"], 207, "{method}");
            assert_eq!(out["body"], multistatus, "{method}");
            assert_eq!(out["headers"]["etag"], "\"e1\"", "{method}");
            assert_eq!(
                out["headers"]["content-type"], "application/xml; charset=utf-8",
                "{method}"
            );
            assert_eq!(out["truncated"], false, "{method}");
        }
        let seen = f.seen();
        assert_eq!(seen.len(), 2);
        for (seen, (method, depth)) in seen.iter().zip([("PROPFIND", "1"), ("REPORT", "0")]) {
            assert_eq!(seen.method, method);
            assert_eq!(seen.path_and_query, "/dav/calendar/");
            assert_eq!(seen.headers["depth"], depth);
            assert_eq!(
                seen.headers["content-type"],
                "application/xml; charset=utf-8"
            );
            assert_eq!(seen.body, request.as_bytes());
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
        assert_eq!(
            d.input_schema["properties"]["method"]["enum"],
            serde_json::json!(METHODS)
        );
    }

    /// A tool over the OAuth provider `google` and the key provider `maps`,
    /// both pointed at one stub.
    struct KeyFixture {
        tool: ApiRequestTool,
        stub: Stub,
        host: String,
        _dir: tempfile::TempDir,
    }

    async fn key_fixture() -> KeyFixture {
        let (base, stub) = start_stub().await;
        let (svc, dir) = service(vec![provider_config("google", &base)]);
        let keys = BTreeMap::from([key_provider_config("maps", &base)]);
        KeyFixture {
            tool: ApiRequestTool::new(Some(svc), table(keys)),
            stub,
            host: base.trim_start_matches("http://").to_string(),
            _dir: dir,
        }
    }

    impl KeyFixture {
        fn args(&self, method: &str, path: &str) -> serde_json::Value {
            serde_json::json!({
                "provider": "maps", "method": method, "host": self.host, "path": path,
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

        fn seen(&self) -> Vec<crate::testutil::ApiSeen> {
            self.stub.lock().unwrap().api_seen.clone()
        }
    }

    #[tokio::test]
    async fn the_key_is_sent_in_its_header() {
        let f = key_fixture().await;
        f.stub.lock().unwrap().api_body = br#"{"ok":true}"#.to_vec();
        let mut args = f.args("GET", "/v1/things");
        // Only the configured name is reserved: `key` is any parameter here.
        args["query"] = serde_json::json!({"a": "1", "key": "mine"});
        let out = f
            .call(Some(alice()), vec![provider_clause("maps")], args)
            .await
            .unwrap();
        assert_eq!(out["status"], 200);
        assert_eq!(out["body"], r#"{"ok":true}"#);
        assert!(!out.to_string().contains(KEY), "{out}");
        let seen = f.seen();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path_and_query, "/v1/things?a=1&key=mine");
        assert_eq!(seen[0].headers["x-api-key"], KEY);
        assert!(!seen[0].headers.contains_key("authorization"));
    }

    /// A table stored while a request is in flight reaches the next call, not
    /// the one already resolved.
    #[tokio::test]
    async fn a_call_in_flight_completes_with_the_key_it_started_with() {
        let (base, stub) = start_stub().await;
        let keys = table(BTreeMap::from([key_provider_config("maps", &base)]));
        let tool = Arc::new(ApiRequestTool::new(None, keys.clone()));
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        stub.lock().unwrap().api_gate = Some(gate.clone());
        let args = serde_json::json!({
            "provider": "maps", "method": "GET",
            "host": base.trim_start_matches("http://"), "path": "/v1/things",
        });
        let ctx = || tool_ctx(Some(alice()), vec![provider_clause("maps")]);

        let in_flight = tokio::spawn({
            let (tool, args, ctx) = (tool.clone(), args.clone(), ctx());
            async move { tool.execute(&ctx, args).await }
        });
        while stub.lock().unwrap().api_seen.is_empty() {
            tokio::task::yield_now().await;
        }
        let (name, mut rotated) = key_provider_config("maps", &base);
        rotated.keys = ApiKeys::Deployment(brenn_lib::config::SecretString::new(
            "K3Y-rotated".to_string(),
        ));
        keys.store(Arc::new(BTreeMap::from([(name, rotated)])));
        gate.add_permits(1);
        let out = in_flight.await.unwrap().unwrap();
        assert_eq!(out["status"], 200);

        stub.lock().unwrap().api_gate = None;
        tool.execute(&ctx(), args).await.unwrap();
        let sent: Vec<String> = stub
            .lock()
            .unwrap()
            .api_seen
            .iter()
            .map(|seen| seen.headers["x-api-key"].to_str().unwrap().to_string())
            .collect();
        assert_eq!(sent, [KEY, "K3Y-rotated"]);
    }

    #[tokio::test]
    async fn the_key_header_and_account_are_refused_to_the_caller() {
        let f = key_fixture().await;
        let with = |key: &str, value: serde_json::Value| {
            let mut args = f.args("GET", "/v1/things");
            args[key] = value;
            args
        };
        for args in [
            with("headers", serde_json::json!({"X-API-KEY": "theirs"})),
            with("headers", serde_json::json!({"x-api-key": "theirs"})),
            with("headers", serde_json::json!({"Authorization": "Bearer x"})),
            with("headers", serde_json::json!({"Cookie": "a=b"})),
            with("account", serde_json::json!("alice@example.com")),
        ] {
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
    async fn clauses_admit_a_key_call_on_method_host_and_path_only() {
        let f = key_fixture().await;
        let host = f.host.clone();
        let admitting = [
            vec![provider_clause("maps")],
            vec![provider_clause("*")],
            vec![clause(&[
                ("provider", "maps"),
                ("method", "GET"),
                ("host", &host),
                ("path", "/v1/*"),
            ])],
        ];
        for acl in admitting {
            assert_eq!(f.tool.check_acl(&f.args("GET", "/v1/things"), &acl), Ok(()));
            f.call(Some(alice()), acl.clone(), f.args("GET", "/v1/things"))
                .await
                .unwrap_or_else(|e| panic!("{acl:?}: {e:?}"));
        }
        let denying = [
            vec![clause(&[("provider", "maps"), ("method", "POST")])],
            vec![clause(&[
                ("provider", "maps"),
                ("host", "other.example.com"),
            ])],
            vec![clause(&[("provider", "maps"), ("path", "/v2/*")])],
            vec![provider_clause("google")],
            vec![clause(&[("provider", "*"), ("user", "alice")])],
            vec![clause(&[("provider", "*"), ("user", "*")])],
            vec![clause(&[("provider", "*"), ("account", "*")])],
        ];
        for acl in denying {
            assert!(
                f.tool
                    .check_acl(&f.args("GET", "/v1/things"), &acl)
                    .is_err(),
                "{acl:?}"
            );
            let out = f
                .call(Some(alice()), acl.clone(), f.args("GET", "/v1/things"))
                .await;
            assert!(matches!(out, Err(ToolError::Denied(_))), "{acl:?}: {out:?}");
        }
        assert_eq!(f.seen().len(), 3);
    }

    /// A tool over the OAuth provider `google`, the deployment-key provider
    /// `maps` and the per-user key provider `mail`
    /// ([`key_accounts_provider_config`]), all pointed at one stub.
    async fn accounts_fixture() -> KeyFixture {
        let (base, stub) = start_stub().await;
        let (svc, dir) = service(vec![provider_config("google", &base)]);
        let keys = BTreeMap::from([
            key_provider_config("maps", &base),
            key_accounts_provider_config("mail", &base),
        ]);
        KeyFixture {
            tool: ApiRequestTool::new(Some(svc), table(keys)),
            stub,
            host: base.trim_start_matches("http://").to_string(),
            _dir: dir,
        }
    }

    fn user(id: i64, username: &str) -> ActingUser {
        ActingUser {
            id,
            username: username.to_string(),
        }
    }

    impl KeyFixture {
        fn mail_args(&self, method: &str, account: Option<&str>) -> serde_json::Value {
            let mut args = serde_json::json!({
                "provider": "mail", "method": method, "host": self.host, "path": "/v1/mail",
            });
            if let Some(account) = account {
                args["account"] = serde_json::json!(account);
            }
            args
        }

        /// The key header of the last request the stub saw.
        fn last_key(&self) -> String {
            let seen = self.seen();
            let last = seen.last().expect("a request was sent");
            last.headers["x-api-key"].to_str().unwrap().to_string()
        }
    }

    fn denied(out: Result<serde_json::Value, ToolError>) -> String {
        match out {
            Err(ToolError::Denied(detail)) => detail,
            other => panic!("expected Denied, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_per_user_key_is_chosen_by_the_acting_user_and_account() {
        let f = accounts_fixture().await;
        let acl = vec![provider_clause("mail")];
        for (who, account, sent) in [
            (alice(), None, "alice"),
            (alice(), Some("alice-work"), "alice-work"),
            (alice(), Some("alice"), "alice"),
            (bob(), None, "bob"),
            (user(4, "dan"), Some("dan-home"), "dan-home"),
        ] {
            let out = f
                .call(Some(who.clone()), acl.clone(), f.mail_args("GET", account))
                .await
                .unwrap_or_else(|e| panic!("{} {account:?}: {e:?}", who.username));
            assert_eq!(out["status"], 200);
            assert_eq!(
                f.last_key(),
                account_key(sent),
                "{} {account:?}",
                who.username
            );
            assert!(!out.to_string().contains("K3Y"), "{out}");
        }
        assert_eq!(f.seen().len(), 5);
    }

    #[tokio::test]
    async fn a_per_user_key_call_without_a_usable_account_is_refused_unsent() {
        let f = accounts_fixture().await;
        let acl = vec![provider_clause("mail")];
        let detail = denied(
            f.call(Some(alice()), acl.clone(), f.mail_args("GET", Some("bob")))
                .await,
        );
        assert!(
            detail.contains(r#"mail has no account "bob" for user alice"#)
                && detail.contains(r#"theirs: ["alice", "alice-work"]"#),
            "{detail}"
        );
        let detail = denied(
            f.call(
                Some(user(3, "carol")),
                acl.clone(),
                f.mail_args("GET", None),
            )
            .await,
        );
        assert!(
            detail.contains("api_key_provider mail holds no account for user carol")
                && detail.contains("the operator adds an `account` block"),
            "{detail}"
        );
        let detail = denied(
            f.call(Some(user(4, "dan")), acl.clone(), f.mail_args("GET", None))
                .await,
        );
        assert!(
            detail.contains("user dan has several mail accounts and none is the default")
                && detail.contains(r#"["dan-home", "dan-work"]"#),
            "{detail}"
        );
        assert!(!detail.contains("K3Y"), "{detail}");
        assert!(f.seen().is_empty());
    }

    #[tokio::test]
    async fn user_and_account_clauses_bound_a_per_user_key_call() {
        let f = accounts_fixture().await;
        let args = f.mail_args("GET", None);
        let work = f.mail_args("GET", Some("alice-work"));
        // `check_acl` defers the user and an omitted account.
        let alice_only = vec![clause(&[("provider", "mail"), ("user", "alice")])];
        assert_eq!(f.tool.check_acl(&args, &alice_only), Ok(()));
        let home_only = vec![clause(&[("provider", "mail"), ("account", "alice")])];
        assert_eq!(f.tool.check_acl(&args, &home_only), Ok(()));
        assert!(f.tool.check_acl(&work, &home_only).is_err());

        f.call(Some(alice()), alice_only.clone(), args.clone())
            .await
            .unwrap();
        let detail = denied(f.call(Some(bob()), alice_only, args.clone()).await);
        assert!(
            detail.contains(r#"as user "bob", account "bob""#),
            "{detail}"
        );
        f.call(Some(alice()), home_only.clone(), args.clone())
            .await
            .unwrap();
        // The omitted account resolves to the default, then meets the clause.
        let work_only = vec![clause(&[("provider", "mail"), ("account", "alice-work")])];
        let detail = denied(f.call(Some(alice()), work_only.clone(), args).await);
        assert!(
            detail.contains(r#"as user "alice", account "alice""#),
            "{detail}"
        );
        f.call(Some(alice()), work_only, work).await.unwrap();
        assert_eq!(f.seen().len(), 3);
    }

    #[tokio::test]
    async fn a_lent_caller_uses_its_clauses_user_and_only_reads() {
        let f = accounts_fixture().await;
        let lent = vec![clause(&[("provider", "mail"), ("user", "bob")])];
        f.call(None, lent.clone(), f.mail_args("GET", None))
            .await
            .unwrap();
        assert_eq!(f.last_key(), account_key("bob"));
        for method in ["POST", "PROPFIND", "REPORT"] {
            let detail = denied(f.call(None, lent.clone(), f.mail_args(method, None)).await);
            assert!(detail.contains("may only read"), "{method}: {detail}");
        }
        let detail = denied(
            f.call(
                None,
                vec![clause(&[("provider", "mail"), ("user", "*")])],
                f.mail_args("GET", None),
            )
            .await,
        );
        assert!(detail.contains("borrows the one user"), "{detail}");
        assert_eq!(f.seen().len(), 1);
    }

    #[tokio::test]
    async fn a_stamped_component_call_uses_its_users_key() {
        let f = accounts_fixture().await;
        let mut post = f.mail_args("POST", Some("alice-work"));
        post["body"] = serde_json::json!("{}");
        let out = f
            .tool
            .execute(
                &stamped_component_ctx(vec![clause(&[
                    ("provider", "mail"),
                    ("user", "*"),
                    ("method", "POST"),
                ])]),
                post,
            )
            .await
            .unwrap();
        assert_eq!(out["status"], 200);
        assert_eq!(f.last_key(), account_key("alice-work"));
    }

    #[tokio::test]
    async fn account_on_a_deployment_key_provider_names_both_forms() {
        let f = accounts_fixture().await;
        let mut args = f.args("GET", "/v1/things");
        args["account"] = serde_json::json!("alice");
        let out = f
            .call(Some(alice()), vec![provider_clause("*")], args)
            .await;
        let Err(ToolError::InvalidArgs(detail)) = out else {
            panic!("expected InvalidArgs, got {out:?}");
        };
        assert!(
            detail.contains("key providers with `account` blocks")
                && detail.contains(r#""maps" holds one deployment key"#),
            "{detail}"
        );
        assert!(f.seen().is_empty());
    }

    #[tokio::test]
    async fn the_key_providers_own_response_limit_applies() {
        let (base, stub) = start_stub().await;
        let (name, mut config) = key_provider_config("maps", &base);
        config.raw.max_response_bytes = 9;
        let tool = ApiRequestTool::new(None, table(BTreeMap::from([(name, config)])));
        stub.lock().unwrap().api_body = b"abcdefghijklmnop".to_vec();
        let args = serde_json::json!({
            "provider": "maps", "method": "GET",
            "host": base.trim_start_matches("http://"), "path": "/v1/things",
        });
        let out = tool
            .execute(
                &tool_ctx(Some(alice()), vec![provider_clause("maps")]),
                args,
            )
            .await
            .unwrap();
        assert_eq!(out["body"], "abcdefghi");
        assert_eq!(out["truncated"], true);
        assert_eq!(out["bytes"], 16);
    }

    #[tokio::test]
    async fn an_answer_quoting_the_key_is_returned_unchanged() {
        let f = key_fixture().await;
        let echo = format!("{{\"error\":\"bad key\",\"x-api-key\":\"{KEY}\"}}");
        {
            let mut s = f.stub.lock().unwrap();
            s.api_statuses = [400].into();
            s.api_body = echo.clone().into_bytes();
        }
        let mut args = f.args("GET", "/v1/things");
        args["headers"] = serde_json::json!({"Accept-Encoding": "identity", "Range": "bytes=0-"});
        let out = f
            .call(Some(alice()), vec![provider_clause("maps")], args)
            .await
            .unwrap();
        assert_eq!(out["status"], 400);
        assert_eq!(out["body"], echo);
        assert_eq!(f.seen().len(), 1);
    }

    #[tokio::test]
    async fn a_caller_with_no_acting_user_only_reads_with_a_key() {
        let f = key_fixture().await;
        let out = f
            .call(
                None,
                vec![provider_clause("maps")],
                f.args("GET", "/v1/things"),
            )
            .await
            .unwrap();
        assert_eq!(out["status"], 200);
        let mut post = f.args("POST", "/v1/things:search");
        post["body"] = serde_json::json!("{}");
        let out = f.call(None, vec![provider_clause("maps")], post).await;
        let Err(ToolError::Denied(detail)) = out else {
            panic!("expected Denied, got {out:?}");
        };
        assert!(detail.contains("may only read"), "{detail}");
        for method in ["PROPFIND", "REPORT"] {
            let detail = denied(
                f.call(
                    None,
                    vec![provider_clause("maps")],
                    f.args(method, "/v1/things"),
                )
                .await,
            );
            assert!(detail.contains("may only read"), "{method}: {detail}");
        }
        assert_eq!(f.seen().len(), 1);
    }

    /// A component's call the executor stamped with the user of the
    /// provided-tool call it serves.
    fn stamped_component_ctx(acl: Vec<AclClause>) -> ToolCtx {
        ToolCtx {
            caller: brenn_lib::messaging::ParticipantId::for_wasm("x"),
            acting_conversation_id: None,
            ..tool_ctx(Some(alice()), acl)
        }
    }

    /// A stamped component call is its user's call: it writes within its
    /// clause, with the key on a key provider and with that user's own
    /// credential on OAuth, under a `user = "*"` clause that lends nobody.
    #[tokio::test]
    async fn a_stamped_component_call_writes_as_its_user() {
        let key = key_fixture().await;
        let mut post = key.args("POST", "/v1/things:search");
        post["body"] = serde_json::json!("{}");
        let out = key
            .tool
            .execute(
                &stamped_component_ctx(vec![clause(&[("provider", "maps"), ("method", "POST")])]),
                post,
            )
            .await
            .unwrap();
        assert_eq!(out["status"], 200);
        let seen = key.seen();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].method, "POST");
        assert_eq!(seen[0].headers["x-api-key"], KEY);

        let f = fixture().await;
        let mut bobs = credential("google", "alice@example.com", "RT-b");
        bobs.access_token = "bobs-access".to_string();
        f.svc.vault().store("bob", &bobs).await;
        let mut draft = f.args("POST", "/gmail/v1/users/me/drafts");
        draft["body"] = serde_json::json!(r#"{"raw":"x"}"#);
        let out = f
            .tool
            .execute(
                &stamped_component_ctx(vec![clause(&[
                    ("provider", "google"),
                    ("user", "*"),
                    ("method", "POST"),
                ])]),
                draft,
            )
            .await
            .unwrap();
        assert_eq!(out["status"], 200);
        let seen = f.seen();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].method, "POST");
        assert_eq!(seen[0].headers["authorization"], "Bearer access");

        // The clause still bounds the method.
        let out = f
            .tool
            .execute(
                &stamped_component_ctx(vec![clause(&[
                    ("provider", "google"),
                    ("user", "*"),
                    ("method", "GET"),
                ])]),
                f.args("DELETE", "/gmail/v1/users/me/drafts/d"),
            )
            .await;
        assert!(matches!(out, Err(ToolError::Denied(_))), "{out:?}");
        assert_eq!(f.seen().len(), 1);
    }

    /// A stamped component call sends `REPORT` under a clause naming it or
    /// naming no method, and not under one naming another method.
    #[tokio::test]
    async fn a_stamped_component_call_reports_within_its_clause() {
        let f = key_fixture().await;
        for acl in [
            clause(&[("provider", "maps"), ("method", "REPORT")]),
            provider_clause("maps"),
        ] {
            let out = f
                .tool
                .execute(
                    &stamped_component_ctx(vec![acl.clone()]),
                    f.args("REPORT", "/dav/calendar"),
                )
                .await
                .unwrap_or_else(|e| panic!("{acl:?}: {e:?}"));
            assert_eq!(out["status"], 200);
        }
        let out = f
            .tool
            .execute(
                &stamped_component_ctx(vec![clause(&[("provider", "maps"), ("method", "GET")])]),
                f.args("REPORT", "/dav/calendar"),
            )
            .await;
        assert!(matches!(out, Err(ToolError::Denied(_))), "{out:?}");
        let methods: Vec<String> = f.seen().into_iter().map(|seen| seen.method).collect();
        assert_eq!(methods, ["REPORT", "REPORT"]);
    }

    #[tokio::test]
    async fn a_refused_key_is_returned_once_as_the_providers_answer() {
        let f = key_fixture().await;
        for status in [401, 403] {
            f.stub.lock().unwrap().api_statuses = [status].into();
            let out = f
                .call(
                    Some(alice()),
                    vec![provider_clause("maps")],
                    f.args("GET", "/v1/things"),
                )
                .await
                .unwrap();
            assert_eq!(out["status"], status);
            assert!(out.get("error").is_none(), "{out}");
        }
        assert_eq!(f.seen().len(), 2);
        assert!(f.stub.lock().unwrap().token_forms.is_empty());
    }

    #[tokio::test]
    async fn an_unknown_provider_names_both_kinds() {
        let f = key_fixture().await;
        let mut args = f.args("GET", "/v1/things");
        args["provider"] = serde_json::json!("nope");
        let out = f
            .call(Some(alice()), vec![provider_clause("*")], args)
            .await;
        let Err(ToolError::InvalidArgs(detail)) = out else {
            panic!("expected InvalidArgs, got {out:?}");
        };
        assert!(
            detail.contains("google (oauth_provider)")
                && detail.contains("maps (api_key_provider)"),
            "{detail}"
        );
    }

    #[tokio::test]
    #[should_panic(expected = "both an oauth_provider and an api_key_provider")]
    async fn one_name_as_both_kinds_is_a_bug() {
        let base = "http://127.0.0.1:9";
        let (svc, _dir) = service(vec![provider_config("maps", base)]);
        let keys = BTreeMap::from([key_provider_config("maps", base)]);
        ApiRequestTool::new(Some(svc), table(keys));
    }

    /// The ` ```json ` blocks of the provider document at `relative` (from the
    /// repository root), each parsed as `api-request`'s argument object for
    /// `provider`, with every path one `api-request` decodes, no header Brenn
    /// reserves for itself (`Authorization`, transport headers, and the API-key
    /// provider's `key_header`, lowercase), and every `body` JSON.
    fn provider_examples(relative: &str, provider: &str, key_header: Option<&str>) -> Vec<ApiArgs> {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join(relative);
        let doc = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("{} must be readable: {error}", path.display()));
        let mut examples = Vec::new();
        let mut open: Option<String> = None;
        for line in doc.lines() {
            match (line.trim_start().starts_with("```"), open.take()) {
                (true, None) if line.trim_start() == "```json" => open = Some(String::new()),
                (true, Some(block)) => examples.push(block),
                (false, Some(mut block)) => {
                    block.push_str(line);
                    block.push('\n');
                    open = Some(block);
                }
                _ => {}
            }
        }
        assert!(open.is_none(), "a `json` block is never closed");
        examples
            .iter()
            .map(|example| {
                let args: ApiArgs = serde_json::from_str(example).unwrap_or_else(|error| {
                    panic!("not an api-request argument ({error}):\n{example}")
                });
                assert_eq!(args.provider, provider, "{example}");
                if let Err(error) = decode_path(&args.path) {
                    panic!("{error}:\n{example}");
                }
                for name in args.headers.keys() {
                    let name = name.to_ascii_lowercase();
                    assert!(
                        name != "authorization"
                            && !TRANSPORT_HEADERS.contains(&name.as_str())
                            && key_header != Some(name.as_str()),
                        "header {name:?} is Brenn's to set:\n{example}"
                    );
                }
                if let Some(body) = &args.body {
                    serde_json::from_str::<serde_json::Value>(body)
                        .unwrap_or_else(|error| panic!("body is not JSON ({error}):\n{example}"));
                }
                args
            })
            .collect()
    }

    /// Every `json` example in `docs/providers/google-maps.md` is an argument
    /// object `api-request` accepts, and every `body` it carries is JSON.
    #[test]
    fn the_google_maps_provider_examples_are_api_request_arguments() {
        let examples = provider_examples(
            "docs/providers/google-maps.md",
            "google-maps",
            Some("x-goog-api-key"),
        );
        assert_eq!(
            examples.len(),
            7,
            "one example per endpoint the document covers"
        );
    }

    /// Every `json` example in `docs/providers/google.md` is an argument
    /// object `api-request` accepts for the `google` OAuth provider, and none
    /// names `account` — Brenn uses the calling user's grant.
    #[test]
    fn the_google_provider_examples_are_api_request_arguments() {
        let examples = provider_examples("docs/providers/google.md", "google", None);
        assert_eq!(
            examples.len(),
            11,
            "one example per endpoint the document covers"
        );
        for args in &examples {
            assert!(args.account.is_none(), "{args:?}");
        }
    }
}

/// `brenn-provider-kit`'s timing and call budget are derived from the numbers
/// below; a change to either side breaks these assertions, which is where the
/// kit's constants are re-derived.
#[cfg(test)]
mod provider_kit_pins {
    use brenn_provider_kit::{CALL_BUDGET, HOST_DEADLINE_MS, NESTED_WORST_CASE_MS};
    use brenn_tool_registry::{MAX_FAST_CALLS_PER_ACTIVATION, PROVIDED_TOOL_CEILING};

    use crate::http::{API_TIMEOUT, AUTH_SERVER_TIMEOUT};

    #[test]
    fn the_kit_deadline_is_the_hosts_provided_tool_ceiling() {
        assert_eq!(
            u128::from(HOST_DEADLINE_MS),
            PROVIDED_TOOL_CEILING.as_millis()
        );
    }

    /// `execute_oauth`'s worst case: refresh, send, and on a 401 to a stored
    /// token one more refresh and one resend.
    #[test]
    fn the_kit_nested_worst_case_is_refresh_send_refresh_resend() {
        assert_eq!(
            u128::from(NESTED_WORST_CASE_MS),
            2 * AUTH_SERVER_TIMEOUT.as_millis() + 2 * API_TIMEOUT.as_millis()
        );
    }

    /// The kit spends at most half the host's per-activation call cap, which
    /// the registry mirrors from `brenn-wasm`.
    #[test]
    fn the_kit_call_budget_is_at_most_half_the_hosts_call_cap() {
        const { assert!(2 * CALL_BUDGET <= MAX_FAST_CALLS_PER_ACTIVATION) };
    }
}
