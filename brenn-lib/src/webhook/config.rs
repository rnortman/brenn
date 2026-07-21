//! Webhook config types: raw (TOML deserialized) and resolved forms.
//!
//! Wired into `BrennConfig` via:
//! - top-level `[[webhook_endpoint]]` arrays → `Vec<WebhookEndpointConfigRaw>`
//! - per-app `[[app.webhook_subscription]]` → `Vec<AppWebhookSubscriptionRaw>`
//!
//! Validation and resolution in `resolve_webhook_endpoints` and
//! `resolve_app_webhook_subscriptions`.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use http::HeaderName;
use indexmap::IndexMap;
use serde::Deserialize;

use crate::config::wasm::{WasmConfig, byte_size_to_max_page_count, resolve_component_config};
use crate::config::{AppConfig, AppConfigRaw, load_secret_file};
use crate::messaging::{Urgency, WakeMin};
use crate::webhook::is_valid_key_id;
use crate::webhook::signature::{HexFormat, SignatureAlgorithm, SignatureScheme};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DEFAULT_TRANSPORT_CEILING: usize = 1024 * 1024; // 1 MiB
const DEFAULT_CONTENT_TYPE: &str = "application/json";

fn default_transport_ceiling() -> usize {
    DEFAULT_TRANSPORT_CEILING
}

fn default_content_type() -> String {
    DEFAULT_CONTENT_TYPE.to_string()
}

fn default_hmac_algorithm() -> String {
    "hmac-sha256".to_string()
}

// ---------------------------------------------------------------------------
// Raw config types (TOML deserialized)
// ---------------------------------------------------------------------------

/// Top-level `[[webhook_endpoint]]` block.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebhookEndpointConfigRaw {
    /// URL-safe identifier; charset `[A-Za-z0-9._~-]+`, globally unique.
    pub slug: String,
    /// HTTP mount path. Defaults to `/webhooks/<slug>` when omitted; globally unique.
    ///
    /// Must start with `/webhooks/` and include a non-empty path segment after that prefix.
    pub mount: Option<String>,
    /// Human-readable description. Optional.
    pub description: Option<String>,
    /// Transport-level body size ceiling (bytes). Default 1 MiB.
    #[serde(default = "default_transport_ceiling")]
    pub transport_ceiling_bytes: usize,
    /// Expected `Content-Type` header (media-type only, no params). Default
    /// `"application/json"`.
    #[serde(default = "default_content_type")]
    pub content_type: String,
    /// Signature scheme configuration (tagged enum keyed on `scheme`).
    pub signature: WebhookSignatureConfigRaw,
    /// HMAC key entries. Required for HMAC schemes; must be empty for `bearer-token`.
    #[serde(default, rename = "key")]
    pub keys: Vec<WebhookKeyConfigRaw>,
    /// Bearer token entries. Required for `bearer-token`; must be empty for HMAC schemes.
    #[serde(default, rename = "token")]
    pub tokens: Vec<WebhookTokenConfigRaw>,
    /// Optional replay-protection binding. When present, inbound requests are
    /// checked against the WASM replay component before being delivered.
    pub replay_protection: Option<ReplayProtectionConfigRaw>,
    /// Per-message urgency intent for traffic entering via this endpoint (sender side).
    /// Default `Normal` when omitted. Note: the old per-app `wake_kind` field was *required*
    /// (operators made an explicit wake/park decision per endpoint). Omitting `urgency` here
    /// therefore silently assigns `Normal`, which eagerly wakes default-policy subscribers —
    /// operators migrating from `wake_kind = "none"` must set `urgency = "low"` on the
    /// `[[webhook_endpoint]]` block (or set `wake_min = "high"` on the subscription) to
    /// preserve parked behaviour.
    pub urgency: Option<Urgency>,
}

/// Tagged signature scheme config. `deny_unknown_fields` means a field
/// belonging to the wrong variant is a hard parse error.
#[derive(Debug, Deserialize)]
#[serde(tag = "scheme", rename_all = "kebab-case", deny_unknown_fields)]
pub enum WebhookSignatureConfigRaw {
    /// HMAC-SHA256 over raw body. Phonebuddy, GitHub/Forgejo, generic.
    HmacRawBody {
        #[serde(default = "default_hmac_algorithm")]
        algorithm: String,
        header: String,
        format: String,
        key_id_header: Option<String>,
    },
    /// HMAC-SHA256 over `<template>` filled with `{t}` and `{body}`. Slack.
    HmacTimestampedBody {
        #[serde(default = "default_hmac_algorithm")]
        algorithm: String,
        sig_header: String,
        sig_format: String,
        timestamp_header: String,
        template: String,
        max_skew_secs: u64,
        key_id_header: Option<String>,
    },
    /// Stripe's combined `t=...,v1=...` header. HMAC over `<t>.<body>`.
    HmacStripe {
        #[serde(default = "default_hmac_algorithm")]
        algorithm: String,
        header: String,
        max_skew_secs: u64,
        key_id_header: Option<String>,
    },
    /// Constant-time bearer-header compare. Google push, Mailgun.
    BearerToken {
        header: String,
        token_id_header: Option<String>,
    },
}

/// `[[webhook_endpoint.key]]` entry (HMAC variants only).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebhookKeyConfigRaw {
    /// Opaque key identifier; charset `[A-Za-z0-9._-]{1,64}`.
    pub key_id: String,
    /// Path to file containing the HMAC secret (trimmed of trailing whitespace).
    pub secret_file: PathBuf,
}

/// `[[webhook_endpoint.token]]` entry (`bearer-token` variant only).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebhookTokenConfigRaw {
    /// Opaque token identifier; charset `[A-Za-z0-9._-]{1,64}`.
    pub token_id: String,
    /// Path to file containing the expected bearer string.
    pub secret_file: PathBuf,
}

/// `[webhook_endpoint.replay_protection]` sub-table (optional).
///
/// When present, inbound requests for this endpoint are checked against the
/// named WASM replay component before being delivered. Both fields required
/// when the sub-table is present.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayProtectionConfigRaw {
    /// Path to the WASM replay component artifact (must exist at startup).
    pub component_path: PathBuf,
    /// Path to the SQLite store file. Created by the host if absent; must not
    /// be shared with another endpoint.
    pub store_path: PathBuf,
    /// Per-store size cap override (e.g. `"128MiB"`). When absent the global
    /// `[wasm].store_size_limit` default applies. Human-readable binary
    /// byte-size string; parsed and validated at load time.
    #[serde(default)]
    pub store_size_limit: Option<String>,
    /// Operator-supplied config map for this replay component
    /// (`[webhook_endpoint.replay_protection.config]`). Values must be strings,
    /// integers, or booleans; floats, datetimes, arrays, and nested tables are
    /// rejected at load time. `None` when the sub-table is absent.
    #[serde(default)]
    pub config: Option<toml::Table>,
}

/// Per-app `[[app.webhook_subscription]]` block.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppWebhookSubscriptionRaw {
    /// References `[[webhook_endpoint]].slug`.
    pub endpoint: String,
    /// Per-subscription wake policy (subscriber side). Absent ⇒ inherit from channel's
    /// resolved `wake_min` (global default `Normal`).
    pub wake_min: Option<WakeMin>,
}

// ---------------------------------------------------------------------------
// Resolved config types
// ---------------------------------------------------------------------------

/// Resolved replay-protection binding for one endpoint.
///
/// `component_path` and `store_path` are canonicalized (symlinks resolved,
/// `./` and `..` segments collapsed). The canonical `store_path` is guaranteed
/// unique across all endpoints — any duplicate triggers a startup panic.
#[derive(Debug, Clone)]
pub struct ResolvedReplayProtection {
    /// Absolute canonical path to the WASM component artifact.
    pub component_path: PathBuf,
    /// Absolute canonical path to the SQLite store file.
    pub store_path: PathBuf,
    /// Host-enforced SQLite `PRAGMA max_page_count` value for this store.
    /// Derived from the per-store `store_size_limit` override if present,
    /// else from `[wasm].store_size_limit` global default. `ceil(bytes /
    /// PAGE_SIZE)` so the enforced cap is always `>= `configured bytes.
    pub max_page_count: u32,
    /// Operator-supplied config map for this replay component (from
    /// `[webhook_endpoint.replay_protection.config]`), plus host-injected
    /// `brenn.*` keys (e.g. `brenn.max-skew-secs` for timestamped schemes).
    /// Empty map when no config table is present and no keys are injected.
    pub config: std::collections::HashMap<String, String>,
}

/// The participant that owns an inbound webhook endpoint.
///
/// Ownership is the runtime existence anchor: an endpoint must resolve to a
/// live participant at delivery time or the request is a 500 (config-invariant
/// violation). An `App` owner is a singleton `[[app]]` with a matching
/// `[[app.webhook_subscription]]`; a `Wasm` owner is a `[[wasm_consumer]]`
/// whose sole `webhook:<slug>` subscription designates it.
#[derive(Debug, Clone)]
pub enum WebhookOwner {
    /// App slug of the singleton app subscribing to this endpoint.
    App(Arc<str>),
    /// `[[wasm_consumer]]` slug subscribing to this endpoint's `webhook:` channel.
    Wasm(Arc<str>),
}

impl WebhookOwner {
    /// The owner's slug, regardless of kind.
    pub fn slug(&self) -> &str {
        match self {
            WebhookOwner::App(s) | WebhookOwner::Wasm(s) => s.as_ref(),
        }
    }

    /// The app slug iff this endpoint is app-owned, else `None`.
    pub fn app_slug(&self) -> Option<&str> {
        match self {
            WebhookOwner::App(s) => Some(s.as_ref()),
            WebhookOwner::Wasm(_) => None,
        }
    }
}

impl std::fmt::Display for WebhookOwner {
    /// Kind-qualified rendering (`app:<slug>` / `wasm:<slug>`) for logs and
    /// error messages — app and wasm-consumer slugs are separate namespaces, so
    /// the bare slug alone is ambiguous to an operator.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WebhookOwner::App(s) => write!(f, "app:{s}"),
            WebhookOwner::Wasm(s) => write!(f, "wasm:{s}"),
        }
    }
}

/// Resolved per-endpoint config, ready for the hot path. All headers are
/// pre-parsed to `HeaderName`; all secrets are pre-loaded from disk.
#[derive(Debug)]
pub struct ResolvedWebhookEndpoint {
    pub slug: String,
    pub mount: String,
    pub description: Option<String>,
    pub transport_ceiling_bytes: usize,
    /// Lowercased, params-stripped content-type (e.g. `"application/json"`).
    pub content_type: String,
    pub scheme: SignatureScheme,
    /// The participant (app or WASM consumer) that owns this endpoint.
    pub owner: WebhookOwner,
    /// Ingress urgency intent assigned to this endpoint (sender side). Default `Normal`.
    pub urgency: Urgency,
    /// Replay-protection binding. `None` means replay protection is disabled
    /// for this endpoint; requests bypass the component check entirely.
    pub replay_protection: Option<ResolvedReplayProtection>,
}

/// Resolved per-app webhook subscription.
#[derive(Debug, Clone)]
pub struct ResolvedWebhookSubscription {
    pub endpoint_slug: String,
    /// Per-subscription wake policy (subscriber side). Resolved from
    /// sub → channel → global default.
    pub wake_min: WakeMin,
}

// ---------------------------------------------------------------------------
// Resolution helpers
// ---------------------------------------------------------------------------

/// Validate and canonicalize a `[webhook_endpoint.replay_protection]` sub-table.
///
/// - `component_path` must exist as a regular file; panics if absent.
/// - `store_path` parent directory must exist; the file itself is created if
///   absent (KvStore::open calls `Connection::open` which creates it).
///   The file is touched (created if needed) then canonicalized so the
///   process-global guard in `brenn_wasm::KvStore` sees uniform paths
///   regardless of symlinks or `./foo.sqlite` vs `foo.sqlite` spelling.
///
/// Returns `None` when `raw` is `None`.
///
/// # Panics
///
/// Panics when `component_path` is missing/unreadable or when `store_path`'s
/// parent directory does not exist, or when the effective size limit string is
/// unparseable or below the floor.
///
/// Pure validator: no write side-effects. The store file is created (touched)
/// lazily by `KvStore::open` at first use.
fn resolve_replay_protection(
    raw: Option<&ReplayProtectionConfigRaw>,
    endpoint_slug: &str,
    global_store_size_limit: &str,
) -> Option<ResolvedReplayProtection> {
    let raw = raw?;

    // component_path must be a regular file.
    let component_meta = std::fs::metadata(&raw.component_path).unwrap_or_else(|e| {
        panic!(
            "[[webhook_endpoint]] {:?}: replay_protection.component_path {:?} \
             cannot be accessed: {e}",
            endpoint_slug, raw.component_path,
        )
    });
    assert!(
        component_meta.is_file(),
        "[[webhook_endpoint]] {:?}: replay_protection.component_path {:?} is not a regular file",
        endpoint_slug,
        raw.component_path,
    );
    let component_path = std::fs::canonicalize(&raw.component_path).unwrap_or_else(|e| {
        panic!(
            "[[webhook_endpoint]] {:?}: failed to canonicalize component_path {:?}: {e}",
            endpoint_slug, raw.component_path,
        )
    });

    // store_path parent dir must exist; file itself is auto-created by KvStore::open.
    let store_parent = raw.store_path.parent().unwrap_or_else(|| Path::new("."));
    assert!(
        store_parent.exists(),
        "[[webhook_endpoint]] {:?}: replay_protection.store_path {:?} — \
         parent directory does not exist",
        endpoint_slug,
        raw.store_path,
    );
    // `std::path::absolute` joins relative paths against cwd and normalises lone
    // `.` components, covering the common relative-vs-absolute alias case.
    // It does NOT resolve symlinks. Operators must not alias store_path values
    // via symlinks; see `KvStore::open`'s OPEN_PATHS guard comment.
    let store_path = std::path::absolute(&raw.store_path).unwrap_or_else(|e| {
        panic!(
            "[[webhook_endpoint]] {:?}: failed to resolve store_path {:?}: {e}",
            endpoint_slug, raw.store_path,
        )
    });

    // Effective size limit: per-store override takes priority over global default.
    let effective_limit = raw
        .store_size_limit
        .as_deref()
        .unwrap_or(global_store_size_limit);
    let field_name = format!(
        "[[webhook_endpoint]] {:?} replay_protection.store_size_limit",
        endpoint_slug,
    );
    let max_page_count = byte_size_to_max_page_count(effective_limit, &field_name);

    let field_name = format!("[[webhook_endpoint]] {endpoint_slug:?} replay_protection.config",);
    let config = resolve_component_config(raw.config.as_ref(), &field_name);

    Some(ResolvedReplayProtection {
        component_path,
        store_path,
        max_page_count,
        config,
    })
}

/// Parse and validate an `algorithm` string (only `"hmac-sha256"` in MVP).
fn resolve_algorithm(algorithm: &str, endpoint_slug: &str) -> SignatureAlgorithm {
    assert!(
        algorithm == "hmac-sha256",
        "[[webhook_endpoint]] {:?}: unsupported algorithm {:?}; only \"hmac-sha256\" is supported in MVP",
        endpoint_slug,
        algorithm,
    );
    SignatureAlgorithm::HmacSha256
}

/// Parse and validate a hex format string.
fn resolve_hex_format(format: &str, endpoint_slug: &str, field: &str) -> HexFormat {
    match format {
        "hex" => HexFormat::Hex,
        "v0-hex" => HexFormat::V0Hex,
        "v1-hex" => HexFormat::V1Hex,
        "sha256-hex" => HexFormat::Sha256Hex,
        other => panic!(
            "[[webhook_endpoint]] {:?}: {field} {:?} is unrecognised; \
             must be one of \"hex\", \"v0-hex\", \"v1-hex\", \"sha256-hex\"",
            endpoint_slug, other,
        ),
    }
}

/// Parse a header name, panicking on invalid input.
fn resolve_header(name: &str, endpoint_slug: &str, field: &str) -> HeaderName {
    name.parse::<HeaderName>().unwrap_or_else(|_| {
        panic!(
            "[[webhook_endpoint]] {:?}: {field} {:?} is not a valid HTTP header name",
            endpoint_slug, name,
        )
    })
}

/// Parse an optional header name, panicking on invalid input.
fn resolve_opt_header(name: Option<&str>, endpoint_slug: &str, field: &str) -> Option<HeaderName> {
    name.map(|n| resolve_header(n, endpoint_slug, field))
}

/// Load all HMAC key entries for an endpoint. Validates key_id charset and
/// uniqueness, and loads secrets via `load_secret_file`.
fn resolve_keys(raw_keys: &[WebhookKeyConfigRaw], endpoint_slug: &str) -> HashMap<String, Vec<u8>> {
    let mut keys = HashMap::new();
    for raw_key in raw_keys {
        assert!(
            is_valid_key_id(&raw_key.key_id),
            "[[webhook_endpoint]] {:?}: key_id {:?} is invalid; must match [A-Za-z0-9._-]{{1,64}}",
            endpoint_slug,
            raw_key.key_id,
        );
        let label = format!(
            "[[webhook_endpoint]] {:?} key {:?} secret_file",
            endpoint_slug, raw_key.key_id,
        );
        let secret = load_secret_file(&label, &raw_key.secret_file);
        let prev = keys.insert(raw_key.key_id.clone(), secret.into_bytes());
        assert!(
            prev.is_none(),
            "[[webhook_endpoint]] {:?}: duplicate key_id {:?}",
            endpoint_slug,
            raw_key.key_id,
        );
    }
    assert!(
        !keys.is_empty(),
        "[[webhook_endpoint]] {:?}: HMAC scheme requires at least one [[webhook_endpoint.key]] entry",
        endpoint_slug,
    );
    keys
}

/// Load all bearer token entries for an endpoint. Validates token_id charset,
/// uniqueness, and loads token bytes via `load_secret_file`.
fn resolve_tokens(
    raw_tokens: &[WebhookTokenConfigRaw],
    endpoint_slug: &str,
) -> HashMap<String, Vec<u8>> {
    let mut tokens = HashMap::new();
    for raw_token in raw_tokens {
        assert!(
            is_valid_key_id(&raw_token.token_id),
            "[[webhook_endpoint]] {:?}: token_id {:?} is invalid; must match [A-Za-z0-9._-]{{1,64}}",
            endpoint_slug,
            raw_token.token_id,
        );
        let label = format!(
            "[[webhook_endpoint]] {:?} token {:?} secret_file",
            endpoint_slug, raw_token.token_id,
        );
        let secret = load_secret_file(&label, &raw_token.secret_file);
        let prev = tokens.insert(raw_token.token_id.clone(), secret.into_bytes());
        assert!(
            prev.is_none(),
            "[[webhook_endpoint]] {:?}: duplicate token_id {:?}",
            endpoint_slug,
            raw_token.token_id,
        );
    }
    assert!(
        !tokens.is_empty(),
        "[[webhook_endpoint]] {:?}: bearer-token scheme requires at least one [[webhook_endpoint.token]] entry",
        endpoint_slug,
    );
    tokens
}

/// Parse the `HmacTimestampedBody` template string. Validates it contains
/// `{t}` and `{body}` exactly once each, with no other `{...}` placeholders,
/// and splits it into `(prefix, mid, suffix, t_before_body)`.
fn resolve_template(template: &str, endpoint_slug: &str) -> (String, String, String, bool) {
    // Check for any `{...}` placeholders.
    let mut placeholders = Vec::new();
    let mut i = 0usize;
    while i < template.len() {
        if template.as_bytes()[i] == b'{' {
            let start = i;
            i += 1;
            while i < template.len() && template.as_bytes()[i] != b'}' {
                i += 1;
            }
            if i < template.len() {
                let placeholder = &template[start..=i];
                placeholders.push(placeholder.to_string());
                i += 1;
            } else {
                i += 1;
            }
        } else {
            i += 1;
        }
    }

    let t_count = placeholders.iter().filter(|p| p.as_str() == "{t}").count();
    let body_count = placeholders
        .iter()
        .filter(|p| p.as_str() == "{body}")
        .count();
    let other_count = placeholders
        .iter()
        .filter(|p| p.as_str() != "{t}" && p.as_str() != "{body}")
        .count();

    assert!(
        t_count == 1,
        "[[webhook_endpoint]] {:?}: template {:?} must contain {{t}} exactly once (found {})",
        endpoint_slug,
        template,
        t_count,
    );
    assert!(
        body_count == 1,
        "[[webhook_endpoint]] {:?}: template {:?} must contain {{body}} exactly once (found {})",
        endpoint_slug,
        template,
        body_count,
    );
    assert!(
        other_count == 0,
        "[[webhook_endpoint]] {:?}: template {:?} contains unrecognised placeholder(s); \
         only {{t}} and {{body}} are allowed",
        endpoint_slug,
        template,
    );

    let t_pos = template.find("{t}").unwrap();
    let body_pos = template.find("{body}").unwrap();
    let t_before_body = t_pos < body_pos;

    if t_before_body {
        // prefix || {t} || mid || {body} || suffix
        let prefix = template[..t_pos].to_string();
        let after_t = t_pos + "{t}".len();
        let mid = template[after_t..body_pos].to_string();
        let after_body = body_pos + "{body}".len();
        let suffix = template[after_body..].to_string();
        (prefix, mid, suffix, true)
    } else {
        // prefix || {body} || mid || {t} || suffix
        let prefix = template[..body_pos].to_string();
        let after_body = body_pos + "{body}".len();
        let mid = template[after_body..t_pos].to_string();
        let after_t = t_pos + "{t}".len();
        let suffix = template[after_t..].to_string();
        (prefix, mid, suffix, false)
    }
}

/// Resolve one `WebhookEndpointConfigRaw` into a `SignatureScheme`, validating
/// all per-scheme invariants and loading secrets.
fn resolve_signature_scheme(raw: &WebhookEndpointConfigRaw) -> SignatureScheme {
    let slug = &raw.slug;
    match &raw.signature {
        WebhookSignatureConfigRaw::HmacRawBody {
            algorithm,
            header,
            format,
            key_id_header,
        } => {
            assert!(
                raw.tokens.is_empty(),
                "[[webhook_endpoint]] {:?}: HMAC scheme must not have [[webhook_endpoint.token]] entries; \
                 use [[webhook_endpoint.key]] instead",
                slug,
            );
            let algorithm = resolve_algorithm(algorithm, slug);
            let header = resolve_header(header, slug, "signature.header");
            let format = resolve_hex_format(format, slug, "signature.format");
            let key_id_header =
                resolve_opt_header(key_id_header.as_deref(), slug, "signature.key_id_header");
            let keys = resolve_keys(&raw.keys, slug);
            if keys.len() > 1 {
                assert!(
                    key_id_header.is_some(),
                    "[[webhook_endpoint]] {:?}: multiple [[webhook_endpoint.key]] entries require \
                     signature.key_id_header to be configured",
                    slug,
                );
            }
            SignatureScheme::HmacRawBody {
                algorithm,
                header,
                format,
                key_id_header,
                keys,
            }
        }
        WebhookSignatureConfigRaw::HmacTimestampedBody {
            algorithm,
            sig_header,
            sig_format,
            timestamp_header,
            template,
            max_skew_secs,
            key_id_header,
        } => {
            assert!(
                raw.tokens.is_empty(),
                "[[webhook_endpoint]] {:?}: HMAC scheme must not have [[webhook_endpoint.token]] entries",
                slug,
            );
            assert!(
                *max_skew_secs > 0,
                "[[webhook_endpoint]] {:?}: signature.max_skew_secs must be > 0",
                slug,
            );
            let algorithm = resolve_algorithm(algorithm, slug);
            let sig_header = resolve_header(sig_header, slug, "signature.sig_header");
            let sig_format = resolve_hex_format(sig_format, slug, "signature.sig_format");
            let timestamp_header =
                resolve_header(timestamp_header, slug, "signature.timestamp_header");
            let key_id_header =
                resolve_opt_header(key_id_header.as_deref(), slug, "signature.key_id_header");
            let (template_prefix, template_mid, template_suffix, t_before_body) =
                resolve_template(template, slug);
            let keys = resolve_keys(&raw.keys, slug);
            if keys.len() > 1 {
                assert!(
                    key_id_header.is_some(),
                    "[[webhook_endpoint]] {:?}: multiple [[webhook_endpoint.key]] entries require \
                     signature.key_id_header to be configured",
                    slug,
                );
            }
            SignatureScheme::HmacTimestampedBody {
                algorithm,
                sig_header,
                sig_format,
                timestamp_header,
                template_prefix,
                template_mid,
                template_suffix,
                t_before_body,
                max_skew_secs: *max_skew_secs,
                key_id_header,
                keys,
            }
        }
        WebhookSignatureConfigRaw::HmacStripe {
            algorithm,
            header,
            max_skew_secs,
            key_id_header,
        } => {
            assert!(
                raw.tokens.is_empty(),
                "[[webhook_endpoint]] {:?}: HMAC scheme must not have [[webhook_endpoint.token]] entries",
                slug,
            );
            assert!(
                *max_skew_secs > 0,
                "[[webhook_endpoint]] {:?}: signature.max_skew_secs must be > 0",
                slug,
            );
            let algorithm = resolve_algorithm(algorithm, slug);
            let header = resolve_header(header, slug, "signature.header");
            let key_id_header =
                resolve_opt_header(key_id_header.as_deref(), slug, "signature.key_id_header");
            let keys = resolve_keys(&raw.keys, slug);
            if keys.len() > 1 {
                assert!(
                    key_id_header.is_some(),
                    "[[webhook_endpoint]] {:?}: multiple [[webhook_endpoint.key]] entries require \
                     signature.key_id_header to be configured",
                    slug,
                );
            }
            SignatureScheme::HmacStripe {
                algorithm,
                header,
                max_skew_secs: *max_skew_secs,
                key_id_header,
                keys,
            }
        }
        WebhookSignatureConfigRaw::BearerToken {
            header,
            token_id_header,
        } => {
            assert!(
                raw.keys.is_empty(),
                "[[webhook_endpoint]] {:?}: bearer-token scheme must not have [[webhook_endpoint.key]] entries; \
                 use [[webhook_endpoint.token]] instead",
                slug,
            );
            let header = resolve_header(header, slug, "signature.header");
            let token_id_header = resolve_opt_header(
                token_id_header.as_deref(),
                slug,
                "signature.token_id_header",
            );
            let tokens = resolve_tokens(&raw.tokens, slug);
            if tokens.len() > 1 {
                assert!(
                    token_id_header.is_some(),
                    "[[webhook_endpoint]] {:?}: multiple [[webhook_endpoint.token]] entries require \
                     signature.token_id_header to be configured",
                    slug,
                );
            }
            SignatureScheme::BearerToken {
                header,
                token_id_header,
                tokens,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Public resolution functions
// ---------------------------------------------------------------------------

/// Required prefix for all webhook endpoint mount paths.
///
/// Webhook mounts must start with this prefix and include a non-empty path
/// segment after it (e.g. `/webhooks/myendpoint`). Bare `/webhooks` and
/// `/webhooks/` are rejected.
const WEBHOOK_MOUNT_PREFIX: &str = "/webhooks/";

/// Resolve replay protection for `raw` and assert that its store_path is not
/// already in `store_path_set`. On success the store_path is inserted into the
/// set.
///
/// `max_skew_secs`: when `Some(n)`, injects `"brenn.max-skew-secs" → n.to_string()`
/// into the resolved config map. Pass `Some` for `HmacTimestampedBody` and
/// `HmacStripe` schemes; `None` for schemes without a timestamp skew window.
fn resolve_and_check_replay_protection(
    raw: Option<&ReplayProtectionConfigRaw>,
    slug: &str,
    store_path_set: &mut HashSet<PathBuf>,
    global_store_size_limit: &str,
    max_skew_secs: Option<u64>,
) -> Option<ResolvedReplayProtection> {
    let mut rp = resolve_replay_protection(raw, slug, global_store_size_limit)?;
    assert!(
        store_path_set.insert(rp.store_path.clone()),
        "[[webhook_endpoint]] {:?}: replay_protection.store_path {:?} is already \
         used by another endpoint; each endpoint must have its own store file",
        slug,
        rp.store_path,
    );
    if let Some(skew) = max_skew_secs {
        rp.config
            .insert("brenn.max-skew-secs".to_string(), skew.to_string());
    }
    Some(rp)
}

/// Validate and resolve all `[[webhook_endpoint]]` raw entries, producing
/// a map of endpoint slug → `Arc<ResolvedWebhookEndpoint>`.
///
/// Also validates cross-app binding constraints (one endpoint → one owning
/// app; every endpoint must be bound; singleton invariant) against `apps`.
///
/// `wasm_config` supplies the global WASM-host defaults (e.g.
/// `store_size_limit`) used when a per-store override is absent.
///
/// # Panics
///
/// Panics on any config error — see design §2.4 rules 1–9.
pub fn resolve_webhook_endpoints(
    raw_endpoints: &[WebhookEndpointConfigRaw],
    raw_apps: &[AppConfigRaw],
    raw_wasm_consumers: &[crate::messaging::config::WasmConsumerConfigRaw],
    apps: &mut IndexMap<String, AppConfig>,
    wasm_config: &WasmConfig,
    global_messaging: &crate::messaging::config::MessagingGlobalConfig,
) -> IndexMap<String, Arc<ResolvedWebhookEndpoint>> {
    use crate::messaging::WEBHOOK_ADDRESS_PREFIX;
    use crate::messaging::is_unreserved_char;

    if raw_endpoints.is_empty() && raw_apps.iter().all(|a| a.webhook_subscriptions.is_empty()) {
        return IndexMap::new();
    }

    // Build a map: endpoint_slug → app_slug for each subscription.
    // Also enforce singleton invariant on subscribing apps.
    let mut endpoint_to_app: HashMap<String, String> = HashMap::new();
    for raw_app in raw_apps {
        if raw_app.webhook_subscriptions.is_empty() {
            continue;
        }
        // Singleton invariant.
        assert!(
            raw_app.singleton && raw_app.allowed_users.len() == 1,
            "app {:?}: webhook_subscription requires singleton = true and exactly one \
             allowed_users entry (MVP restriction matching messaging and MQTT)",
            raw_app.slug,
        );
        for sub in &raw_app.webhook_subscriptions {
            let prev = endpoint_to_app.insert(sub.endpoint.clone(), raw_app.slug.clone());
            assert!(
                prev.is_none(),
                "endpoint {:?} is subscribed to by both app {:?} and app {:?}; \
                 each endpoint must have exactly one owning app",
                sub.endpoint,
                prev.unwrap(),
                raw_app.slug,
            );
        }
    }

    // Build a map: endpoint_slug → distinct wasm-consumer slugs whose
    // `[[wasm_consumer.subscription]]` names `webhook:<endpoint_slug>`. These are
    // the fallback owners when no app subscribes (Rule 9). Distinct-slug dedup so a
    // consumer with two subscriptions to the same webhook channel counts once.
    let mut endpoint_to_wasm: HashMap<String, Vec<String>> = HashMap::new();
    for consumer in raw_wasm_consumers {
        for sub in &consumer.subscriptions {
            if let Some(ep_slug) = sub.channel.strip_prefix(WEBHOOK_ADDRESS_PREFIX) {
                let entry = endpoint_to_wasm.entry(ep_slug.to_string()).or_default();
                if !entry.contains(&consumer.slug) {
                    entry.push(consumer.slug.clone());
                }
            }
        }
    }

    // Resolve each endpoint.
    let mut result: IndexMap<String, Arc<ResolvedWebhookEndpoint>> = IndexMap::new();
    let mut mount_set: HashSet<String> = HashSet::new();
    // Duplicate-store-path guard: canonical store_path must be unique across endpoints.
    let mut store_path_set: HashSet<PathBuf> = HashSet::new();

    for raw in raw_endpoints {
        let slug = &raw.slug;

        // Rule 1: slug charset.
        assert!(
            !slug.is_empty() && slug.chars().all(is_unreserved_char),
            "[[webhook_endpoint]] slug {:?} is invalid; must match [A-Za-z0-9._~-]+",
            slug,
        );

        // Rule 3: slug uniqueness.
        assert!(
            !result.contains_key(slug.as_str()),
            "[[webhook_endpoint]]: duplicate slug {:?}",
            slug,
        );

        // Mount path: default or explicit.
        let mount = raw
            .mount
            .clone()
            .unwrap_or_else(|| format!("{WEBHOOK_MOUNT_PREFIX}{slug}"));

        // Namespace check: mount must start with WEBHOOK_MOUNT_PREFIX and have a non-empty tail.
        let tail = mount.strip_prefix(WEBHOOK_MOUNT_PREFIX).unwrap_or("");
        assert!(
            !tail.is_empty(),
            "[[webhook_endpoint]] {slug:?}: mount {mount:?} is invalid — \
             must start with {WEBHOOK_MOUNT_PREFIX:?} and include a non-empty path segment \
             after it (suggestion: use \"{WEBHOOK_MOUNT_PREFIX}{slug}\")",
        );

        // Rule 4: mount uniqueness.
        assert!(
            mount_set.insert(mount.clone()),
            "[[webhook_endpoint]] {:?}: mount {:?} is already used by another endpoint",
            slug,
            mount,
        );

        // Rule 9: every endpoint must resolve to exactly one owner. An app
        // subscription wins; otherwise a sole WASM consumer subscribing to the
        // `webhook:<slug>` channel owns it. No app + ≥2 wasm subscribers is
        // ambiguous; no subscriber of either kind is an orphan.
        let owner = match endpoint_to_app.get(slug.as_str()) {
            Some(app_slug) => WebhookOwner::App(Arc::from(app_slug.as_str())),
            None => {
                let wasm_owners = endpoint_to_wasm
                    .get(slug.as_str())
                    .map(Vec::as_slice)
                    .unwrap_or(&[]);
                match wasm_owners {
                    [only] => WebhookOwner::Wasm(Arc::from(only.as_str())),
                    [] => panic!(
                        "[[webhook_endpoint]] {:?}: no app has a [[app.webhook_subscription]] \
                         and no [[wasm_consumer]] has a webhook:{} subscription referencing this \
                         endpoint; orphan endpoints are not permitted",
                        slug, slug,
                    ),
                    many => panic!(
                        "[[webhook_endpoint]] {:?}: ambiguous ownership; multiple WASM subscribers \
                         {:?} require an owning app or a single designated consumer",
                        slug, many,
                    ),
                }
            }
        };

        // Endpoint-level urgency intent (sender side). Default Normal per §2.7 mapping.
        let urgency = raw.urgency.unwrap_or(Urgency::Normal);

        // App-owned endpoints stamp a resolved subscription (with a resolved
        // wake_min) onto the owning app. WASM-owned endpoints carry no app-side
        // stamping — the consumer's own `[[wasm_consumer.subscription]]` block
        // carries its inherit ladder, resolved where wasm subscriptions resolve.
        let app_stamp = owner.app_slug().map(|app_slug| {
            // Look up the app's wake_min for this endpoint (subscriber side).
            // Absent ⇒ inherit from the global default_wake_min (same three-level ladder as
            // all other subscription types: sub → channel → global). Webhook channels have no
            // per-channel wake_min override, so the global default is the direct fallback.
            let sub_wake_min = raw_apps.iter().find(|a| a.slug == app_slug).and_then(|a| {
                a.webhook_subscriptions
                    .iter()
                    .find(|s| &s.endpoint == slug)
                    .and_then(|s| s.wake_min)
            });
            // Webhook subscriptions are always push-enabled (enforcement at higher level);
            // fall back to global default_wake_min so operators setting that field get
            // consistent behaviour across all subscription types.
            let wake_min = sub_wake_min.unwrap_or(global_messaging.default_wake_min);
            (app_slug.to_string(), wake_min)
        });

        // Rule 2 + per-scheme validation (also loads secrets).
        let scheme = resolve_signature_scheme(raw);

        // Normalize content_type: strip params, lowercase.
        let content_type = raw
            .content_type
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .to_lowercase();

        // Extract max_skew_secs from schemes that carry a timestamp window.
        // Injected into replay protection config as "brenn.max-skew-secs".
        let max_skew_secs = match &scheme {
            SignatureScheme::HmacTimestampedBody { max_skew_secs, .. } => Some(*max_skew_secs),
            SignatureScheme::HmacStripe { max_skew_secs, .. } => Some(*max_skew_secs),
            SignatureScheme::HmacRawBody { .. } | SignatureScheme::BearerToken { .. } => None,
        };

        // Replay protection: validate paths, canonicalize, check for dup store.
        let replay_protection = resolve_and_check_replay_protection(
            raw.replay_protection.as_ref(),
            slug,
            &mut store_path_set,
            &wasm_config.store_size_limit,
            max_skew_secs,
        );

        let endpoint = Arc::new(ResolvedWebhookEndpoint {
            slug: slug.clone(),
            mount,
            description: raw.description.clone(),
            transport_ceiling_bytes: raw.transport_ceiling_bytes,
            content_type,
            scheme,
            owner,
            urgency,
            replay_protection,
        });

        result.insert(slug.clone(), endpoint.clone());

        // Stamp resolved subscriptions onto the owning AppConfig (app-owned only).
        if let Some((app_slug, wake_min)) = app_stamp
            && let Some(app) = apps.get_mut(&app_slug)
        {
            app.webhook_subscriptions.push(ResolvedWebhookSubscription {
                endpoint_slug: slug.clone(),
                wake_min,
            });
        }
    }

    // Rule 7/9 (inverse): every subscription references a declared endpoint.
    for raw_app in raw_apps {
        for sub in &raw_app.webhook_subscriptions {
            assert!(
                result.contains_key(&sub.endpoint),
                "app {:?}: [[app.webhook_subscription]] references endpoint {:?} \
                 which is not declared in any [[webhook_endpoint]] block",
                raw_app.slug,
                sub.endpoint,
            );
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::*;

    // Helper: create a temp file with given contents, return path.
    fn secret_file(contents: &[u8]) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(contents).unwrap();
        f
    }

    fn raw_key(key_id: &str, secret_file: &tempfile::NamedTempFile) -> WebhookKeyConfigRaw {
        WebhookKeyConfigRaw {
            key_id: key_id.to_string(),
            secret_file: secret_file.path().to_owned(),
        }
    }

    fn raw_token(token_id: &str, secret_file: &tempfile::NamedTempFile) -> WebhookTokenConfigRaw {
        WebhookTokenConfigRaw {
            token_id: token_id.to_string(),
            secret_file: secret_file.path().to_owned(),
        }
    }

    fn minimal_app_raw(slug: &str, singleton: bool, allowed_users: Vec<String>) -> AppConfigRaw {
        AppConfigRaw {
            slug: slug.to_string(),
            singleton,
            allowed_users,
            ..Default::default()
        }
    }

    fn app_raw_with_sub(slug: &str, endpoint: &str) -> AppConfigRaw {
        AppConfigRaw {
            slug: slug.to_string(),
            singleton: true,
            allowed_users: vec!["alice".to_string()],
            webhook_subscriptions: vec![AppWebhookSubscriptionRaw {
                endpoint: endpoint.to_string(),
                wake_min: None,
            }],
            ..Default::default()
        }
    }

    fn raw_hmac_endpoint(slug: &str, keys: Vec<WebhookKeyConfigRaw>) -> WebhookEndpointConfigRaw {
        WebhookEndpointConfigRaw {
            slug: slug.to_string(),
            mount: None,
            description: None,
            transport_ceiling_bytes: DEFAULT_TRANSPORT_CEILING,
            content_type: DEFAULT_CONTENT_TYPE.to_string(),
            signature: WebhookSignatureConfigRaw::HmacRawBody {
                algorithm: "hmac-sha256".to_string(),
                header: "x-test-signature".to_string(),
                format: "hex".to_string(),
                key_id_header: None,
            },
            keys,
            tokens: vec![],
            replay_protection: None,
            urgency: None,
        }
    }

    // A test-only stand-in for AppConfig that we don't actually need to fully
    // populate — we only test config.rs which only calls `resolve_webhook_endpoints`.
    // That function modifies `app.webhook_subscriptions`, so we need real AppConfig
    // instances. We use a helper in the brenn-lib integration test style.
    //
    // Rather than constructing a full AppConfig (which requires a real working_dir etc.),
    // the tests for resolve_webhook_endpoints are structured to test the raw-side
    // resolution logic (panics) without needing to construct full AppConfig instances.
    // We pass an empty `apps` IndexMap and verify the resolved endpoint fields directly.

    // Helper to call resolve with real (empty) apps map and default WasmConfig.
    fn resolve(
        endpoints: &[WebhookEndpointConfigRaw],
        app_raws: &[AppConfigRaw],
    ) -> IndexMap<String, Arc<ResolvedWebhookEndpoint>> {
        resolve_with_wasm(endpoints, app_raws, &[])
    }

    // Helper to call resolve with WASM consumers participating in ownership.
    fn resolve_with_wasm(
        endpoints: &[WebhookEndpointConfigRaw],
        app_raws: &[AppConfigRaw],
        wasm_consumers: &[crate::messaging::config::WasmConsumerConfigRaw],
    ) -> IndexMap<String, Arc<ResolvedWebhookEndpoint>> {
        let mut apps: IndexMap<String, AppConfig> = IndexMap::new();
        resolve_webhook_endpoints(
            endpoints,
            app_raws,
            wasm_consumers,
            &mut apps,
            &WasmConfig::default(),
            &crate::messaging::config::MessagingGlobalConfig::default(),
        )
    }

    /// Minimal `[[wasm_consumer]]` raw with the given `webhook:<endpoint>`
    /// subscriptions (one per endpoint slug). Only the fields ownership
    /// resolution reads are populated; everything else is defaulted/empty.
    fn wasm_consumer_with_webhook_subs(
        slug: &str,
        endpoint_slugs: &[&str],
    ) -> crate::messaging::config::WasmConsumerConfigRaw {
        let channels: Vec<String> = endpoint_slugs
            .iter()
            .map(|ep| format!("webhook:{ep}"))
            .collect();
        let channel_refs: Vec<&str> = channels.iter().map(String::as_str).collect();
        crate::messaging::config::WasmConsumerConfigRaw::minimal(
            slug,
            std::path::PathBuf::from("/dev/null"),
            &channel_refs,
        )
    }

    // --- Address tests ---

    #[test]
    fn default_mount_is_applied() {
        let secret = secret_file(b"mysecret");
        let ep = raw_hmac_endpoint("myendpoint", vec![raw_key("k1", &secret)]);
        let app = app_raw_with_sub("myapp", "myendpoint");
        let result = resolve(&[ep], &[app]);
        assert_eq!(result["myendpoint"].mount, "/webhooks/myendpoint");
    }

    #[test]
    fn explicit_mount_is_used() {
        let secret = secret_file(b"mysecret");
        let mut ep = raw_hmac_endpoint("myendpoint", vec![raw_key("k1", &secret)]);
        ep.mount = Some("/webhooks/custom".to_string());
        let app = app_raw_with_sub("myapp", "myendpoint");
        let result = resolve(&[ep], &[app]);
        assert_eq!(result["myendpoint"].mount, "/webhooks/custom");
    }

    #[test]
    fn owning_app_slug_stamped() {
        let secret = secret_file(b"mysecret");
        let ep = raw_hmac_endpoint("ep", vec![raw_key("k1", &secret)]);
        let app = app_raw_with_sub("myapp", "ep");
        let result = resolve(&[ep], &[app]);
        assert_eq!(result["ep"].owner.app_slug(), Some("myapp"));
        assert_eq!(result["ep"].urgency, Urgency::Normal);
    }

    #[test]
    fn content_type_lowercased_and_params_stripped() {
        let secret = secret_file(b"mysecret");
        let mut ep = raw_hmac_endpoint("ep", vec![raw_key("k1", &secret)]);
        ep.content_type = "Application/JSON; charset=utf-8".to_string();
        let app = app_raw_with_sub("myapp", "ep");
        let result = resolve(&[ep], &[app]);
        assert_eq!(result["ep"].content_type, "application/json");
    }

    // --- Panic tests: cross-cutting ---

    #[test]
    #[should_panic(expected = "singleton")]
    fn non_singleton_app_panics() {
        let secret = secret_file(b"mysecret");
        let ep = raw_hmac_endpoint("ep", vec![raw_key("k1", &secret)]);
        let mut app = app_raw_with_sub("myapp", "ep");
        app.singleton = false;
        app.allowed_users = vec!["alice".to_string()];
        resolve(&[ep], &[app]);
    }

    #[test]
    #[should_panic(expected = "singleton")]
    fn multi_user_app_panics() {
        let secret = secret_file(b"mysecret");
        let ep = raw_hmac_endpoint("ep", vec![raw_key("k1", &secret)]);
        let mut app = app_raw_with_sub("myapp", "ep");
        app.singleton = true;
        app.allowed_users = vec!["alice".to_string(), "bob".to_string()];
        resolve(&[ep], &[app]);
    }

    #[test]
    #[should_panic(expected = "orphan endpoints are not permitted")]
    fn orphan_endpoint_panics() {
        let secret = secret_file(b"mysecret");
        let ep = raw_hmac_endpoint("ep", vec![raw_key("k1", &secret)]);
        // No app subscribes to "ep".
        let app = minimal_app_raw("myapp", true, vec!["alice".to_string()]);
        resolve(&[ep], &[app]);
    }

    // --- Rule 9: WASM ownership ---

    #[test]
    fn wasm_consumer_owns_endpoint_when_no_app_subscribes() {
        let secret = secret_file(b"mysecret");
        let ep = raw_hmac_endpoint("ep", vec![raw_key("k1", &secret)]);
        // No app subscribes; a sole WASM consumer subscribes to webhook:ep.
        let consumer = wasm_consumer_with_webhook_subs("myconsumer", &["ep"]);
        let result = resolve_with_wasm(&[ep], &[], &[consumer]);
        match &result["ep"].owner {
            WebhookOwner::Wasm(slug) => assert_eq!(slug.as_ref(), "myconsumer"),
            other => panic!("expected Wasm owner, got {other:?}"),
        }
        assert_eq!(result["ep"].owner.app_slug(), None);
    }

    #[test]
    fn app_owns_endpoint_even_when_wasm_also_subscribes() {
        // An app subscription wins over a WASM subscriber (fan-out is allowed;
        // ownership stays with the app).
        let secret = secret_file(b"mysecret");
        let ep = raw_hmac_endpoint("ep", vec![raw_key("k1", &secret)]);
        let app = app_raw_with_sub("myapp", "ep");
        let consumer = wasm_consumer_with_webhook_subs("myconsumer", &["ep"]);
        let result = resolve_with_wasm(&[ep], &[app], &[consumer]);
        assert_eq!(result["ep"].owner.app_slug(), Some("myapp"));
    }

    #[test]
    #[should_panic(expected = "ambiguous ownership")]
    fn two_wasm_subscribers_no_app_panics() {
        let secret = secret_file(b"mysecret");
        let ep = raw_hmac_endpoint("ep", vec![raw_key("k1", &secret)]);
        let c1 = wasm_consumer_with_webhook_subs("consumer-a", &["ep"]);
        let c2 = wasm_consumer_with_webhook_subs("consumer-b", &["ep"]);
        resolve_with_wasm(&[ep], &[], &[c1, c2]);
    }

    #[test]
    #[should_panic(expected = "orphan endpoints are not permitted")]
    fn endpoint_with_no_app_or_wasm_subscriber_panics() {
        let secret = secret_file(b"mysecret");
        let ep = raw_hmac_endpoint("ep", vec![raw_key("k1", &secret)]);
        // A WASM consumer exists but subscribes to a different endpoint.
        let consumer = wasm_consumer_with_webhook_subs("myconsumer", &["other"]);
        resolve_with_wasm(&[ep], &[], &[consumer]);
    }

    #[test]
    fn wasm_owner_dedupes_repeated_subscriptions() {
        // A consumer subscribing to webhook:ep twice counts once, so it is a
        // sole owner (not ambiguous).
        let secret = secret_file(b"mysecret");
        let ep = raw_hmac_endpoint("ep", vec![raw_key("k1", &secret)]);
        let consumer = wasm_consumer_with_webhook_subs("myconsumer", &["ep", "ep"]);
        let result = resolve_with_wasm(&[ep], &[], &[consumer]);
        assert_eq!(result["ep"].owner.slug(), "myconsumer");
    }

    #[test]
    #[should_panic(expected = "duplicate slug")]
    fn duplicate_slug_panics() {
        let secret = secret_file(b"mysecret");
        let ep1 = raw_hmac_endpoint("ep", vec![raw_key("k1", &secret)]);
        let ep2 = raw_hmac_endpoint("ep", vec![raw_key("k1", &secret)]);
        let app = app_raw_with_sub("myapp", "ep");
        resolve(&[ep1, ep2], &[app]);
    }

    #[test]
    #[should_panic(expected = "already used by another endpoint")]
    fn duplicate_mount_panics() {
        let s1 = secret_file(b"secret1");
        let s2 = secret_file(b"secret2");
        let mut ep1 = raw_hmac_endpoint("ep1", vec![raw_key("k1", &s1)]);
        ep1.mount = Some("/webhooks/shared".to_string());
        let mut ep2 = raw_hmac_endpoint("ep2", vec![raw_key("k1", &s2)]);
        ep2.mount = Some("/webhooks/shared".to_string());
        let app1 = app_raw_with_sub("app1", "ep1");
        let app2 = app_raw_with_sub("app2", "ep2");
        resolve(&[ep1, ep2], &[app1, app2]);
    }

    /// Namespace check fires *before* mount-uniqueness: two endpoints sharing the
    /// same invalid mount must panic with "must start with", not "already used".
    #[test]
    #[should_panic(expected = "must start with")]
    fn namespace_check_precedes_uniqueness_check() {
        let s1 = secret_file(b"secret1");
        let s2 = secret_file(b"secret2");
        let mut ep1 = raw_hmac_endpoint("ep1", vec![raw_key("k1", &s1)]);
        ep1.mount = Some("/foo".to_string()); // invalid prefix, shared mount
        let mut ep2 = raw_hmac_endpoint("ep2", vec![raw_key("k1", &s2)]);
        ep2.mount = Some("/foo".to_string()); // same invalid mount
        let app1 = app_raw_with_sub("app1", "ep1");
        let app2 = app_raw_with_sub("app2", "ep2");
        resolve(&[ep1, ep2], &[app1, app2]);
    }

    #[test]
    #[should_panic(expected = "must start with")]
    fn mount_not_under_webhooks_panics() {
        let secret = secret_file(b"mysecret");
        let mut ep = raw_hmac_endpoint("ep", vec![raw_key("k1", &secret)]);
        ep.mount = Some("/foo".to_string());
        let app = app_raw_with_sub("myapp", "ep");
        resolve(&[ep], &[app]);
    }

    #[test]
    #[should_panic(expected = "must start with")]
    fn mount_under_hooks_legacy_panics() {
        let secret = secret_file(b"mysecret");
        let mut ep = raw_hmac_endpoint("ep", vec![raw_key("k1", &secret)]);
        ep.mount = Some("/hooks/ep".to_string());
        let app = app_raw_with_sub("myapp", "ep");
        resolve(&[ep], &[app]);
    }

    #[test]
    #[should_panic(expected = "must start with")]
    fn mount_webhook_singular_panics() {
        let secret = secret_file(b"mysecret");
        let mut ep = raw_hmac_endpoint("ep", vec![raw_key("k1", &secret)]);
        ep.mount = Some("/webhook/x".to_string());
        let app = app_raw_with_sub("myapp", "ep");
        resolve(&[ep], &[app]);
    }

    #[test]
    #[should_panic(expected = "must start with")]
    fn mount_bare_webhooks_namespace_panics_no_slash() {
        let secret = secret_file(b"mysecret");
        let mut ep = raw_hmac_endpoint("ep", vec![raw_key("k1", &secret)]);
        ep.mount = Some("/webhooks".to_string());
        let app = app_raw_with_sub("myapp", "ep");
        resolve(&[ep], &[app]);
    }

    #[test]
    #[should_panic(expected = "must start with")]
    fn mount_bare_webhooks_namespace_panics_trailing_slash() {
        let secret = secret_file(b"mysecret");
        let mut ep = raw_hmac_endpoint("ep", vec![raw_key("k1", &secret)]);
        ep.mount = Some("/webhooks/".to_string());
        let app = app_raw_with_sub("myapp", "ep");
        resolve(&[ep], &[app]);
    }

    #[test]
    fn mount_under_webhooks_accepted() {
        let secret = secret_file(b"mysecret");
        let mut ep = raw_hmac_endpoint("ep", vec![raw_key("k1", &secret)]);
        ep.mount = Some("/webhooks/anything".to_string());
        let app = app_raw_with_sub("myapp", "ep");
        let result = resolve(&[ep], &[app]);
        assert_eq!(result["ep"].mount, "/webhooks/anything");
    }

    #[test]
    #[should_panic(expected = "exactly one owning app")]
    fn two_apps_same_endpoint_panics() {
        let s1 = secret_file(b"secret1");
        let ep = raw_hmac_endpoint("ep", vec![raw_key("k1", &s1)]);
        let app1 = app_raw_with_sub("app1", "ep");
        let app2 = app_raw_with_sub("app2", "ep");
        resolve(&[ep], &[app1, app2]);
    }

    #[test]
    #[should_panic(expected = "not declared")]
    fn subscription_to_undeclared_endpoint_panics() {
        let app = app_raw_with_sub("myapp", "nonexistent");
        resolve(&[], &[app]);
    }

    #[test]
    fn one_app_multiple_subscriptions_ok() {
        let s1 = secret_file(b"secret1");
        let s2 = secret_file(b"secret2");
        let ep1 = raw_hmac_endpoint("ep1", vec![raw_key("k1", &s1)]);
        let ep2 = raw_hmac_endpoint("ep2", vec![raw_key("k1", &s2)]);
        let app = AppConfigRaw {
            slug: "myapp".to_string(),
            singleton: true,
            allowed_users: vec!["alice".to_string()],
            webhook_subscriptions: vec![
                AppWebhookSubscriptionRaw {
                    endpoint: "ep1".to_string(),
                    wake_min: None,
                },
                AppWebhookSubscriptionRaw {
                    endpoint: "ep2".to_string(),
                    wake_min: None,
                },
            ],
            ..Default::default()
        };
        let result = resolve(&[ep1, ep2], &[app]);
        assert_eq!(result.len(), 2);
        assert!(result.contains_key("ep1"));
        assert!(result.contains_key("ep2"));
    }

    // --- HmacRawBody scheme tests ---

    #[test]
    fn hmac_raw_body_phonebuddy_shape_resolves() {
        let secret = secret_file(b"supersecret");
        let ep = WebhookEndpointConfigRaw {
            slug: "phonebuddy".to_string(),
            mount: Some("/webhooks/phonebuddy/v1/ingest".to_string()),
            description: Some("PhoneBuddy ingest".to_string()),
            transport_ceiling_bytes: DEFAULT_TRANSPORT_CEILING,
            content_type: DEFAULT_CONTENT_TYPE.to_string(),
            signature: WebhookSignatureConfigRaw::HmacRawBody {
                algorithm: "hmac-sha256".to_string(),
                header: "x-phonebuddy-signature".to_string(),
                format: "v1-hex".to_string(),
                key_id_header: Some("x-phonebuddy-key-id".to_string()),
            },
            keys: vec![raw_key("primary", &secret)],
            tokens: vec![],
            replay_protection: None,
            urgency: None,
        };
        let app = app_raw_with_sub("phonebuddy", "phonebuddy");
        let result = resolve(&[ep], &[app]);
        let ep = &result["phonebuddy"];
        assert_eq!(ep.mount, "/webhooks/phonebuddy/v1/ingest");
        assert_eq!(ep.description.as_deref(), Some("PhoneBuddy ingest"));
        let SignatureScheme::HmacRawBody {
            format: HexFormat::V1Hex,
            key_id_header: Some(_),
            ..
        } = &ep.scheme
        else {
            panic!("expected HmacRawBody with V1Hex format and key_id_header");
        };
    }

    #[test]
    #[should_panic(expected = "[[webhook_endpoint.token]]")]
    fn hmac_with_tokens_panics() {
        let secret = secret_file(b"mysecret");
        let token_secret = secret_file(b"tokenvalue");
        let ep = WebhookEndpointConfigRaw {
            slug: "ep".to_string(),
            mount: None,
            description: None,
            transport_ceiling_bytes: DEFAULT_TRANSPORT_CEILING,
            content_type: DEFAULT_CONTENT_TYPE.to_string(),
            signature: WebhookSignatureConfigRaw::HmacRawBody {
                algorithm: "hmac-sha256".to_string(),
                header: "x-sig".to_string(),
                format: "hex".to_string(),
                key_id_header: None,
            },
            keys: vec![raw_key("k1", &secret)],
            tokens: vec![raw_token("t1", &token_secret)],
            replay_protection: None,
            urgency: None,
        };
        let app = app_raw_with_sub("myapp", "ep");
        resolve(&[ep], &[app]);
    }

    #[test]
    #[should_panic(expected = "key_id_header")]
    fn hmac_multi_key_without_key_id_header_panics() {
        let s1 = secret_file(b"secret1");
        let s2 = secret_file(b"secret2");
        let ep = WebhookEndpointConfigRaw {
            slug: "ep".to_string(),
            mount: None,
            description: None,
            transport_ceiling_bytes: DEFAULT_TRANSPORT_CEILING,
            content_type: DEFAULT_CONTENT_TYPE.to_string(),
            signature: WebhookSignatureConfigRaw::HmacRawBody {
                algorithm: "hmac-sha256".to_string(),
                header: "x-sig".to_string(),
                format: "hex".to_string(),
                key_id_header: None, // absent but we have 2 keys
            },
            keys: vec![raw_key("k1", &s1), raw_key("k2", &s2)],
            tokens: vec![],
            replay_protection: None,
            urgency: None,
        };
        let app = app_raw_with_sub("myapp", "ep");
        resolve(&[ep], &[app]);
    }

    #[test]
    #[should_panic(expected = "unsupported algorithm")]
    fn unknown_algorithm_panics() {
        let secret = secret_file(b"mysecret");
        let ep = WebhookEndpointConfigRaw {
            slug: "ep".to_string(),
            mount: None,
            description: None,
            transport_ceiling_bytes: DEFAULT_TRANSPORT_CEILING,
            content_type: DEFAULT_CONTENT_TYPE.to_string(),
            signature: WebhookSignatureConfigRaw::HmacRawBody {
                algorithm: "hmac-sha512".to_string(), // not supported
                header: "x-sig".to_string(),
                format: "hex".to_string(),
                key_id_header: None,
            },
            keys: vec![raw_key("k1", &secret)],
            tokens: vec![],
            replay_protection: None,
            urgency: None,
        };
        let app = app_raw_with_sub("myapp", "ep");
        resolve(&[ep], &[app]);
    }

    #[test]
    #[should_panic(expected = "unrecognised")]
    fn unknown_format_panics() {
        let secret = secret_file(b"mysecret");
        let ep = WebhookEndpointConfigRaw {
            slug: "ep".to_string(),
            mount: None,
            description: None,
            transport_ceiling_bytes: DEFAULT_TRANSPORT_CEILING,
            content_type: DEFAULT_CONTENT_TYPE.to_string(),
            signature: WebhookSignatureConfigRaw::HmacRawBody {
                algorithm: "hmac-sha256".to_string(),
                header: "x-sig".to_string(),
                format: "base64".to_string(), // not supported
                key_id_header: None,
            },
            keys: vec![raw_key("k1", &secret)],
            tokens: vec![],
            replay_protection: None,
            urgency: None,
        };
        let app = app_raw_with_sub("myapp", "ep");
        resolve(&[ep], &[app]);
    }

    // --- HmacTimestampedBody scheme tests ---

    #[test]
    fn hmac_timestamped_body_slack_shape_resolves() {
        let secret = secret_file(b"slack-secret");
        let ep = WebhookEndpointConfigRaw {
            slug: "slack".to_string(),
            mount: None,
            description: None,
            transport_ceiling_bytes: DEFAULT_TRANSPORT_CEILING,
            content_type: DEFAULT_CONTENT_TYPE.to_string(),
            signature: WebhookSignatureConfigRaw::HmacTimestampedBody {
                algorithm: "hmac-sha256".to_string(),
                sig_header: "x-slack-signature".to_string(),
                sig_format: "v0-hex".to_string(),
                timestamp_header: "x-slack-request-timestamp".to_string(),
                template: "v0:{t}:{body}".to_string(),
                max_skew_secs: 300,
                key_id_header: None,
            },
            keys: vec![raw_key("main", &secret)],
            tokens: vec![],
            replay_protection: None,
            urgency: None,
        };
        let app = app_raw_with_sub("myapp", "slack");
        let result = resolve(&[ep], &[app]);
        let ep = &result["slack"];
        match &ep.scheme {
            SignatureScheme::HmacTimestampedBody {
                t_before_body,
                template_prefix,
                template_mid,
                template_suffix,
                max_skew_secs,
                ..
            } => {
                assert!(
                    *t_before_body,
                    "t should come before body in v0:{{t}}:{{body}}"
                );
                assert_eq!(template_prefix, "v0:");
                assert_eq!(template_mid, ":");
                assert_eq!(template_suffix, "");
                assert_eq!(*max_skew_secs, 300);
            }
            _ => panic!("expected HmacTimestampedBody"),
        }
    }

    #[test]
    fn hmac_timestamped_body_before_t_resolves() {
        let secret = secret_file(b"secret");
        let ep = WebhookEndpointConfigRaw {
            slug: "ep".to_string(),
            mount: None,
            description: None,
            transport_ceiling_bytes: DEFAULT_TRANSPORT_CEILING,
            content_type: DEFAULT_CONTENT_TYPE.to_string(),
            signature: WebhookSignatureConfigRaw::HmacTimestampedBody {
                algorithm: "hmac-sha256".to_string(),
                sig_header: "x-sig".to_string(),
                sig_format: "hex".to_string(),
                timestamp_header: "x-ts".to_string(),
                template: "prefix:{body}:{t}:suffix".to_string(),
                max_skew_secs: 60,
                key_id_header: None,
            },
            keys: vec![raw_key("k", &secret)],
            tokens: vec![],
            replay_protection: None,
            urgency: None,
        };
        let app = app_raw_with_sub("myapp", "ep");
        let result = resolve(&[ep], &[app]);
        match &result["ep"].scheme {
            SignatureScheme::HmacTimestampedBody {
                t_before_body,
                template_prefix,
                template_mid,
                template_suffix,
                ..
            } => {
                assert!(!t_before_body, "body should come before t");
                assert_eq!(template_prefix, "prefix:");
                assert_eq!(template_mid, ":");
                assert_eq!(template_suffix, ":suffix");
            }
            _ => panic!("expected HmacTimestampedBody"),
        }
    }

    #[test]
    #[should_panic(expected = "max_skew_secs must be > 0")]
    fn hmac_timestamped_zero_skew_panics() {
        let secret = secret_file(b"secret");
        let ep = WebhookEndpointConfigRaw {
            slug: "ep".to_string(),
            mount: None,
            description: None,
            transport_ceiling_bytes: DEFAULT_TRANSPORT_CEILING,
            content_type: DEFAULT_CONTENT_TYPE.to_string(),
            signature: WebhookSignatureConfigRaw::HmacTimestampedBody {
                algorithm: "hmac-sha256".to_string(),
                sig_header: "x-sig".to_string(),
                sig_format: "hex".to_string(),
                timestamp_header: "x-ts".to_string(),
                template: "v0:{t}:{body}".to_string(),
                max_skew_secs: 0,
                key_id_header: None,
            },
            keys: vec![raw_key("k", &secret)],
            tokens: vec![],
            replay_protection: None,
            urgency: None,
        };
        let app = app_raw_with_sub("myapp", "ep");
        resolve(&[ep], &[app]);
    }

    #[test]
    #[should_panic(expected = "must contain {t} exactly once")]
    fn template_missing_t_panics() {
        let secret = secret_file(b"secret");
        let ep = WebhookEndpointConfigRaw {
            slug: "ep".to_string(),
            mount: None,
            description: None,
            transport_ceiling_bytes: DEFAULT_TRANSPORT_CEILING,
            content_type: DEFAULT_CONTENT_TYPE.to_string(),
            signature: WebhookSignatureConfigRaw::HmacTimestampedBody {
                algorithm: "hmac-sha256".to_string(),
                sig_header: "x-sig".to_string(),
                sig_format: "hex".to_string(),
                timestamp_header: "x-ts".to_string(),
                template: "prefix:{body}:suffix".to_string(), // no {t}
                max_skew_secs: 60,
                key_id_header: None,
            },
            keys: vec![raw_key("k", &secret)],
            tokens: vec![],
            replay_protection: None,
            urgency: None,
        };
        let app = app_raw_with_sub("myapp", "ep");
        resolve(&[ep], &[app]);
    }

    #[test]
    #[should_panic(expected = "must contain {body} exactly once")]
    fn template_missing_body_panics() {
        let secret = secret_file(b"secret");
        let ep = WebhookEndpointConfigRaw {
            slug: "ep".to_string(),
            mount: None,
            description: None,
            transport_ceiling_bytes: DEFAULT_TRANSPORT_CEILING,
            content_type: DEFAULT_CONTENT_TYPE.to_string(),
            signature: WebhookSignatureConfigRaw::HmacTimestampedBody {
                algorithm: "hmac-sha256".to_string(),
                sig_header: "x-sig".to_string(),
                sig_format: "hex".to_string(),
                timestamp_header: "x-ts".to_string(),
                template: "prefix:{t}:suffix".to_string(), // no {body}
                max_skew_secs: 60,
                key_id_header: None,
            },
            keys: vec![raw_key("k", &secret)],
            tokens: vec![],
            replay_protection: None,
            urgency: None,
        };
        let app = app_raw_with_sub("myapp", "ep");
        resolve(&[ep], &[app]);
    }

    #[test]
    #[should_panic(expected = "unrecognised placeholder")]
    fn template_unknown_placeholder_panics() {
        let secret = secret_file(b"secret");
        let ep = WebhookEndpointConfigRaw {
            slug: "ep".to_string(),
            mount: None,
            description: None,
            transport_ceiling_bytes: DEFAULT_TRANSPORT_CEILING,
            content_type: DEFAULT_CONTENT_TYPE.to_string(),
            signature: WebhookSignatureConfigRaw::HmacTimestampedBody {
                algorithm: "hmac-sha256".to_string(),
                sig_header: "x-sig".to_string(),
                sig_format: "hex".to_string(),
                timestamp_header: "x-ts".to_string(),
                template: "{t}:{body}:{extra}".to_string(),
                max_skew_secs: 60,
                key_id_header: None,
            },
            keys: vec![raw_key("k", &secret)],
            tokens: vec![],
            replay_protection: None,
            urgency: None,
        };
        let app = app_raw_with_sub("myapp", "ep");
        resolve(&[ep], &[app]);
    }

    #[test]
    #[should_panic(expected = "[[webhook_endpoint.token]]")]
    fn hmac_timestamped_with_tokens_panics() {
        let secret = secret_file(b"secret");
        let token_secret = secret_file(b"tokenvalue");
        let ep = WebhookEndpointConfigRaw {
            slug: "ep".to_string(),
            mount: None,
            description: None,
            transport_ceiling_bytes: DEFAULT_TRANSPORT_CEILING,
            content_type: DEFAULT_CONTENT_TYPE.to_string(),
            signature: WebhookSignatureConfigRaw::HmacTimestampedBody {
                algorithm: "hmac-sha256".to_string(),
                sig_header: "x-sig".to_string(),
                sig_format: "hex".to_string(),
                timestamp_header: "x-ts".to_string(),
                template: "v0:{t}:{body}".to_string(),
                max_skew_secs: 300,
                key_id_header: None,
            },
            keys: vec![raw_key("k", &secret)],
            tokens: vec![raw_token("t1", &token_secret)],
            replay_protection: None,
            urgency: None,
        };
        let app = app_raw_with_sub("myapp", "ep");
        resolve(&[ep], &[app]);
    }

    // --- HmacStripe scheme tests ---

    #[test]
    fn hmac_stripe_resolves() {
        let secret = secret_file(b"stripe-secret");
        let ep = WebhookEndpointConfigRaw {
            slug: "stripe".to_string(),
            mount: None,
            description: None,
            transport_ceiling_bytes: DEFAULT_TRANSPORT_CEILING,
            content_type: DEFAULT_CONTENT_TYPE.to_string(),
            signature: WebhookSignatureConfigRaw::HmacStripe {
                algorithm: "hmac-sha256".to_string(),
                header: "stripe-signature".to_string(),
                max_skew_secs: 300,
                key_id_header: None,
            },
            keys: vec![raw_key("main", &secret)],
            tokens: vec![],
            replay_protection: None,
            urgency: None,
        };
        let app = app_raw_with_sub("myapp", "stripe");
        let result = resolve(&[ep], &[app]);
        assert!(matches!(
            result["stripe"].scheme,
            SignatureScheme::HmacStripe {
                max_skew_secs: 300,
                ..
            }
        ));
    }

    #[test]
    #[should_panic(expected = "max_skew_secs must be > 0")]
    fn hmac_stripe_zero_skew_panics() {
        let secret = secret_file(b"secret");
        let ep = WebhookEndpointConfigRaw {
            slug: "ep".to_string(),
            mount: None,
            description: None,
            transport_ceiling_bytes: DEFAULT_TRANSPORT_CEILING,
            content_type: DEFAULT_CONTENT_TYPE.to_string(),
            signature: WebhookSignatureConfigRaw::HmacStripe {
                algorithm: "hmac-sha256".to_string(),
                header: "stripe-signature".to_string(),
                max_skew_secs: 0,
                key_id_header: None,
            },
            keys: vec![raw_key("k", &secret)],
            tokens: vec![],
            replay_protection: None,
            urgency: None,
        };
        let app = app_raw_with_sub("myapp", "ep");
        resolve(&[ep], &[app]);
    }

    #[test]
    #[should_panic(expected = "[[webhook_endpoint.token]]")]
    fn hmac_stripe_with_tokens_panics() {
        let secret = secret_file(b"secret");
        let token_secret = secret_file(b"tokenvalue");
        let ep = WebhookEndpointConfigRaw {
            slug: "ep".to_string(),
            mount: None,
            description: None,
            transport_ceiling_bytes: DEFAULT_TRANSPORT_CEILING,
            content_type: DEFAULT_CONTENT_TYPE.to_string(),
            signature: WebhookSignatureConfigRaw::HmacStripe {
                algorithm: "hmac-sha256".to_string(),
                header: "stripe-signature".to_string(),
                max_skew_secs: 300,
                key_id_header: None,
            },
            keys: vec![raw_key("k", &secret)],
            tokens: vec![raw_token("t1", &token_secret)],
            replay_protection: None,
            urgency: None,
        };
        let app = app_raw_with_sub("myapp", "ep");
        resolve(&[ep], &[app]);
    }

    // --- BearerToken scheme tests ---

    #[test]
    fn bearer_token_google_push_shape_resolves() {
        let token_secret = secret_file(b"my-google-token");
        let ep = WebhookEndpointConfigRaw {
            slug: "google".to_string(),
            mount: None,
            description: None,
            transport_ceiling_bytes: DEFAULT_TRANSPORT_CEILING,
            content_type: DEFAULT_CONTENT_TYPE.to_string(),
            signature: WebhookSignatureConfigRaw::BearerToken {
                header: "x-goog-channel-token".to_string(),
                token_id_header: None,
            },
            keys: vec![],
            tokens: vec![raw_token("main", &token_secret)],
            replay_protection: None,
            urgency: None,
        };
        let app = app_raw_with_sub("myapp", "google");
        let result = resolve(&[ep], &[app]);
        assert!(matches!(
            result["google"].scheme,
            SignatureScheme::BearerToken {
                token_id_header: None,
                ..
            }
        ));
    }

    #[test]
    #[should_panic(expected = "[[webhook_endpoint.key]]")]
    fn bearer_with_keys_panics() {
        let secret = secret_file(b"hmac-secret");
        let token_secret = secret_file(b"bearer-token");
        let ep = WebhookEndpointConfigRaw {
            slug: "ep".to_string(),
            mount: None,
            description: None,
            transport_ceiling_bytes: DEFAULT_TRANSPORT_CEILING,
            content_type: DEFAULT_CONTENT_TYPE.to_string(),
            signature: WebhookSignatureConfigRaw::BearerToken {
                header: "authorization".to_string(),
                token_id_header: None,
            },
            keys: vec![raw_key("k1", &secret)],
            tokens: vec![raw_token("t1", &token_secret)],
            replay_protection: None,
            urgency: None,
        };
        let app = app_raw_with_sub("myapp", "ep");
        resolve(&[ep], &[app]);
    }

    #[test]
    #[should_panic(expected = "token_id_header")]
    fn bearer_multi_token_without_token_id_header_panics() {
        let t1 = secret_file(b"token1");
        let t2 = secret_file(b"token2");
        let ep = WebhookEndpointConfigRaw {
            slug: "ep".to_string(),
            mount: None,
            description: None,
            transport_ceiling_bytes: DEFAULT_TRANSPORT_CEILING,
            content_type: DEFAULT_CONTENT_TYPE.to_string(),
            signature: WebhookSignatureConfigRaw::BearerToken {
                header: "x-token".to_string(),
                token_id_header: None, // absent but 2 tokens
            },
            keys: vec![],
            tokens: vec![raw_token("t1", &t1), raw_token("t2", &t2)],
            replay_protection: None,
            urgency: None,
        };
        let app = app_raw_with_sub("myapp", "ep");
        resolve(&[ep], &[app]);
    }

    // --- Slug/key_id charset tests ---

    #[test]
    #[should_panic(expected = "slug")]
    fn invalid_endpoint_slug_panics() {
        let secret = secret_file(b"secret");
        let ep = WebhookEndpointConfigRaw {
            slug: "bad slug!".to_string(),
            mount: None,
            description: None,
            transport_ceiling_bytes: DEFAULT_TRANSPORT_CEILING,
            content_type: DEFAULT_CONTENT_TYPE.to_string(),
            signature: WebhookSignatureConfigRaw::HmacRawBody {
                algorithm: "hmac-sha256".to_string(),
                header: "x-sig".to_string(),
                format: "hex".to_string(),
                key_id_header: None,
            },
            keys: vec![raw_key("k", &secret)],
            tokens: vec![],
            replay_protection: None,
            urgency: None,
        };
        let app = app_raw_with_sub("myapp", "bad slug!");
        resolve(&[ep], &[app]);
    }

    // --- Replay protection: dup-store-path panic tests ---

    /// Build a `WebhookEndpointConfigRaw` with replay_protection pointing to
    /// `component_path` (a real file) and `store_path` (a real path).
    fn raw_hmac_endpoint_with_replay(
        slug: &str,
        keys: Vec<WebhookKeyConfigRaw>,
        component_path: std::path::PathBuf,
        store_path: std::path::PathBuf,
    ) -> WebhookEndpointConfigRaw {
        WebhookEndpointConfigRaw {
            slug: slug.to_string(),
            mount: None,
            description: None,
            transport_ceiling_bytes: DEFAULT_TRANSPORT_CEILING,
            content_type: DEFAULT_CONTENT_TYPE.to_string(),
            signature: WebhookSignatureConfigRaw::HmacRawBody {
                algorithm: "hmac-sha256".to_string(),
                header: "x-test-signature".to_string(),
                format: "hex".to_string(),
                key_id_header: None,
            },
            keys,
            tokens: vec![],
            replay_protection: Some(ReplayProtectionConfigRaw {
                component_path,
                store_path,
                store_size_limit: None,
                config: None,
            }),
            urgency: None,
        }
    }

    #[test]
    fn resolve_replay_protection_does_not_create_store_file() {
        // resolve_replay_protection must be a pure validator: it must NOT create the
        // store file. File creation is KvStore::open's responsibility. A regression
        // that re-adds a touch() here would silently create SQLite files during config
        // validation, which runs before the WASM runtime is ready.
        let component_file = tempfile::NamedTempFile::new().unwrap();
        let temp_dir = tempfile::TempDir::new().unwrap();
        let store_path = temp_dir.path().join("should-not-be-created.sqlite");
        assert!(
            !store_path.exists(),
            "precondition: store file must not exist before resolve"
        );

        let s = secret_file(b"secret");
        let ep = raw_hmac_endpoint_with_replay(
            "ep",
            vec![raw_key("k1", &s)],
            component_file.path().to_owned(),
            store_path.clone(),
        );
        let app = app_raw_with_sub("myapp", "ep");
        resolve(&[ep], &[app]);

        assert!(
            !store_path.exists(),
            "resolve must not create the store file; got: {store_path:?}"
        );
    }

    #[test]
    #[should_panic(expected = "already used by another endpoint")]
    fn resolve_webhook_endpoints_dup_store_path_panics() {
        // Two endpoints sharing the same canonical store_path must panic.
        let s1 = secret_file(b"secret1");
        let s2 = secret_file(b"secret2");
        // Use a single NamedTempFile as both the component artifact (real file) and store.
        // We need a real file for component_path; reuse a dummy temp file.
        let component_file = tempfile::NamedTempFile::new().unwrap();
        let store_file = tempfile::NamedTempFile::new().unwrap();
        let ep1 = raw_hmac_endpoint_with_replay(
            "ep1",
            vec![raw_key("k1", &s1)],
            component_file.path().to_owned(),
            store_file.path().to_owned(),
        );
        let ep2 = raw_hmac_endpoint_with_replay(
            "ep2",
            vec![raw_key("k1", &s2)],
            component_file.path().to_owned(),
            store_file.path().to_owned(), // same store path → must panic
        );
        let app1 = app_raw_with_sub("app1", "ep1");
        let app2 = app_raw_with_sub("app2", "ep2");
        resolve(&[ep1, ep2], &[app1, app2]);
    }

    // --- AC-5: per-store override takes priority over global default ---

    /// Helper: call resolve_webhook_endpoints with a custom WasmConfig.
    fn resolve_with_wasm_config(
        endpoints: &[WebhookEndpointConfigRaw],
        app_raws: &[AppConfigRaw],
        wasm_config: &WasmConfig,
    ) -> IndexMap<String, Arc<ResolvedWebhookEndpoint>> {
        let mut apps: IndexMap<String, AppConfig> = IndexMap::new();
        resolve_webhook_endpoints(
            endpoints,
            app_raws,
            &[],
            &mut apps,
            wasm_config,
            &crate::messaging::config::MessagingGlobalConfig::default(),
        )
    }

    #[test]
    fn store_size_limit_per_store_override_takes_priority() {
        // AC-5: when per-store store_size_limit is set, it overrides the global default.
        // 128 MiB override vs 64 MiB global → resolved max_page_count must come from 128 MiB.
        use crate::config::wasm::byte_size_to_max_page_count;
        let s = secret_file(b"secret");
        let component_file = tempfile::NamedTempFile::new().unwrap();
        let store_dir = tempfile::TempDir::new().unwrap();
        let store_path = store_dir.path().join("store.sqlite");
        let mut ep = raw_hmac_endpoint_with_replay(
            "ep",
            vec![raw_key("k", &s)],
            component_file.path().to_owned(),
            store_path,
        );
        // Set per-store override to 128 MiB.
        if let Some(ref mut rp) = ep.replay_protection {
            rp.store_size_limit = Some("128MiB".to_string());
        }
        let app = app_raw_with_sub("myapp", "ep");
        let wasm = WasmConfig {
            store_size_limit: "64MiB".to_string(),
        };
        let result = resolve_with_wasm_config(&[ep], &[app], &wasm);
        let rp = result["ep"].replay_protection.as_ref().unwrap();
        let expected = byte_size_to_max_page_count("128MiB", "test");
        assert_eq!(
            rp.max_page_count, expected,
            "per-store override (128MiB) must win over global default (64MiB)"
        );
    }

    #[test]
    fn store_size_limit_global_default_applies_when_no_override() {
        // AC-5: when no per-store override, global default is used.
        use crate::config::wasm::byte_size_to_max_page_count;
        let s = secret_file(b"secret");
        let component_file = tempfile::NamedTempFile::new().unwrap();
        let store_dir = tempfile::TempDir::new().unwrap();
        let store_path = store_dir.path().join("store.sqlite");
        // No per-store override (store_size_limit: None).
        let ep = raw_hmac_endpoint_with_replay(
            "ep",
            vec![raw_key("k", &s)],
            component_file.path().to_owned(),
            store_path,
        );
        let app = app_raw_with_sub("myapp", "ep");
        let wasm = WasmConfig {
            store_size_limit: "256MiB".to_string(),
        };
        let result = resolve_with_wasm_config(&[ep], &[app], &wasm);
        let rp = result["ep"].replay_protection.as_ref().unwrap();
        let expected = byte_size_to_max_page_count("256MiB", "test");
        assert_eq!(
            rp.max_page_count, expected,
            "global default (256MiB) must apply when no per-store override"
        );
    }

    // --- AC-6: bad store_size_limit → fatal panic naming the field ---

    #[test]
    #[should_panic(expected = "ep")]
    fn store_size_limit_bad_per_store_value_panics_with_slug() {
        // AC-6: unparseable per-store value → fatal panic; message must contain slug.
        let s = secret_file(b"secret");
        let component_file = tempfile::NamedTempFile::new().unwrap();
        let store_dir = tempfile::TempDir::new().unwrap();
        let store_path = store_dir.path().join("store.sqlite");
        let mut ep = raw_hmac_endpoint_with_replay(
            "ep",
            vec![raw_key("k", &s)],
            component_file.path().to_owned(),
            store_path,
        );
        if let Some(ref mut rp) = ep.replay_protection {
            rp.store_size_limit = Some("notasize".to_string());
        }
        let app = app_raw_with_sub("myapp", "ep");
        resolve(&[ep], &[app]);
    }

    #[test]
    #[should_panic(expected = "store_size_limit")]
    fn store_size_limit_bad_global_value_panics_with_field_name() {
        // AC-6: unparseable global default → fatal panic; message must contain field identifier.
        // "99KB" uses decimal-SI (rejected); "99MiB" would pass. Expect panic containing the
        // string "store_size_limit" (the config key name embedded in the field_name argument).
        let s = secret_file(b"secret");
        let component_file = tempfile::NamedTempFile::new().unwrap();
        let store_dir = tempfile::TempDir::new().unwrap();
        let store_path = store_dir.path().join("store.sqlite");
        let ep = raw_hmac_endpoint_with_replay(
            "ep",
            vec![raw_key("k", &s)],
            component_file.path().to_owned(),
            store_path,
        );
        let app = app_raw_with_sub("myapp", "ep");
        let wasm = WasmConfig {
            store_size_limit: "99KB".to_string(), // decimal SI, rejected
        };
        resolve_with_wasm_config(&[ep], &[app], &wasm);
    }

    // --- Host injection: brenn.max-skew-secs ---

    /// Helper: build a `WebhookEndpointConfigRaw` with `HmacTimestampedBody` scheme
    /// and replay protection. `secret` must outlive this function's result.
    fn timestamped_endpoint_with_replay(
        slug: &str,
        max_skew_secs: u64,
        component_path: std::path::PathBuf,
        store_path: std::path::PathBuf,
        replay_config: Option<toml::Table>,
        secret: &tempfile::NamedTempFile,
    ) -> WebhookEndpointConfigRaw {
        WebhookEndpointConfigRaw {
            slug: slug.to_string(),
            mount: None,
            description: None,
            transport_ceiling_bytes: DEFAULT_TRANSPORT_CEILING,
            content_type: DEFAULT_CONTENT_TYPE.to_string(),
            signature: WebhookSignatureConfigRaw::HmacTimestampedBody {
                algorithm: "hmac-sha256".to_string(),
                sig_header: "x-sig".to_string(),
                sig_format: "hex".to_string(),
                timestamp_header: "x-ts".to_string(),
                template: "v0:{t}:{body}".to_string(),
                max_skew_secs,
                key_id_header: None,
            },
            keys: vec![WebhookKeyConfigRaw {
                key_id: "k1".to_string(),
                secret_file: secret.path().to_owned(),
            }],
            tokens: vec![],
            replay_protection: Some(ReplayProtectionConfigRaw {
                component_path,
                store_path,
                store_size_limit: None,
                config: replay_config,
            }),
            urgency: None,
        }
    }

    /// `HmacTimestampedBody` endpoint → `brenn.max-skew-secs` injected into replay config.
    #[test]
    fn hmac_timestamped_injects_max_skew_secs() {
        let secret = secret_file(b"test-secret");
        let component_file = tempfile::NamedTempFile::new().unwrap();
        let store_dir = tempfile::TempDir::new().unwrap();
        let store_path = store_dir.path().join("store.sqlite");

        let ep = timestamped_endpoint_with_replay(
            "ep",
            300,
            component_file.path().to_owned(),
            store_path,
            None,
            &secret,
        );
        let app = app_raw_with_sub("myapp", "ep");
        let result = resolve(&[ep], &[app]);

        let rp = result["ep"].replay_protection.as_ref().unwrap();
        assert_eq!(
            rp.config.get("brenn.max-skew-secs").map(String::as_str),
            Some("300"),
            "brenn.max-skew-secs must be injected for HmacTimestampedBody"
        );
    }

    /// `HmacRawBody` endpoint → no `brenn.max-skew-secs` injected.
    #[test]
    fn hmac_raw_body_does_not_inject_max_skew_secs() {
        let s = secret_file(b"secret");
        let component_file = tempfile::NamedTempFile::new().unwrap();
        let store_dir = tempfile::TempDir::new().unwrap();
        let store_path = store_dir.path().join("store.sqlite");

        let ep = raw_hmac_endpoint_with_replay(
            "ep",
            vec![raw_key("k", &s)],
            component_file.path().to_owned(),
            store_path,
        );
        let app = app_raw_with_sub("myapp", "ep");
        let result = resolve(&[ep], &[app]);

        let rp = result["ep"].replay_protection.as_ref().unwrap();
        assert!(
            !rp.config.contains_key("brenn.max-skew-secs"),
            "brenn.max-skew-secs must NOT be injected for HmacRawBody"
        );
    }

    /// Operator-supplied keys coexist with the injected `brenn.max-skew-secs`.
    #[test]
    fn operator_keys_coexist_with_injected_skew() {
        let secret = secret_file(b"test-secret");
        let component_file = tempfile::NamedTempFile::new().unwrap();
        let store_dir = tempfile::TempDir::new().unwrap();
        let store_path = store_dir.path().join("store.sqlite");

        let mut operator_config = toml::Table::new();
        operator_config.insert("window-multiplier".to_string(), toml::Value::Integer(2));

        let ep = timestamped_endpoint_with_replay(
            "ep",
            60,
            component_file.path().to_owned(),
            store_path,
            Some(operator_config),
            &secret,
        );
        let app = app_raw_with_sub("myapp", "ep");
        let result = resolve(&[ep], &[app]);

        let rp = result["ep"].replay_protection.as_ref().unwrap();
        assert_eq!(
            rp.config.get("brenn.max-skew-secs").map(String::as_str),
            Some("60"),
        );
        assert_eq!(
            rp.config.get("window-multiplier").map(String::as_str),
            Some("2"),
        );
        assert_eq!(rp.config.len(), 2);
    }

    /// `HmacStripe` endpoint → `brenn.max-skew-secs` injected into replay config.
    #[test]
    fn hmac_stripe_injects_max_skew_secs() {
        let secret = secret_file(b"stripe-secret");
        let component_file = tempfile::NamedTempFile::new().unwrap();
        let store_dir = tempfile::TempDir::new().unwrap();
        let store_path = store_dir.path().join("store.sqlite");

        let ep = WebhookEndpointConfigRaw {
            slug: "ep".to_string(),
            mount: None,
            description: None,
            transport_ceiling_bytes: DEFAULT_TRANSPORT_CEILING,
            content_type: DEFAULT_CONTENT_TYPE.to_string(),
            signature: WebhookSignatureConfigRaw::HmacStripe {
                algorithm: "hmac-sha256".to_string(),
                header: "stripe-signature".to_string(),
                max_skew_secs: 300,
                key_id_header: None,
            },
            keys: vec![raw_key("main", &secret)],
            tokens: vec![],
            replay_protection: Some(ReplayProtectionConfigRaw {
                component_path: component_file.path().to_owned(),
                store_path,
                store_size_limit: None,
                config: None,
            }),
            urgency: None,
        };
        let app = app_raw_with_sub("myapp", "ep");
        let result = resolve(&[ep], &[app]);

        let rp = result["ep"].replay_protection.as_ref().unwrap();
        assert_eq!(
            rp.config.get("brenn.max-skew-secs").map(String::as_str),
            Some("300"),
            "brenn.max-skew-secs must be injected for HmacStripe"
        );
    }

    /// `BearerToken` endpoint → no `brenn.max-skew-secs` injected.
    #[test]
    fn bearer_token_does_not_inject_max_skew_secs() {
        let token_secret = secret_file(b"bearer-token");
        let component_file = tempfile::NamedTempFile::new().unwrap();
        let store_dir = tempfile::TempDir::new().unwrap();
        let store_path = store_dir.path().join("store.sqlite");

        let ep = WebhookEndpointConfigRaw {
            slug: "ep".to_string(),
            mount: None,
            description: None,
            transport_ceiling_bytes: DEFAULT_TRANSPORT_CEILING,
            content_type: DEFAULT_CONTENT_TYPE.to_string(),
            signature: WebhookSignatureConfigRaw::BearerToken {
                header: "authorization".to_string(),
                token_id_header: None,
            },
            keys: vec![],
            tokens: vec![raw_token("t1", &token_secret)],
            replay_protection: Some(ReplayProtectionConfigRaw {
                component_path: component_file.path().to_owned(),
                store_path,
                store_size_limit: None,
                config: None,
            }),
            urgency: None,
        };
        let app = app_raw_with_sub("myapp", "ep");
        let result = resolve(&[ep], &[app]);

        let rp = result["ep"].replay_protection.as_ref().unwrap();
        assert!(
            !rp.config.contains_key("brenn.max-skew-secs"),
            "brenn.max-skew-secs must NOT be injected for BearerToken"
        );
    }

    /// Operator sets `brenn.*` key in `replay_protection.config` → boot panic.
    #[test]
    #[should_panic(expected = "reserved prefix")]
    fn operator_brenn_prefix_in_replay_config_panics() {
        let secret = secret_file(b"test-secret");
        let component_file = tempfile::NamedTempFile::new().unwrap();
        let store_dir = tempfile::TempDir::new().unwrap();
        let store_path = store_dir.path().join("store.sqlite");

        let mut operator_config = toml::Table::new();
        operator_config.insert(
            "brenn.my-key".to_string(),
            toml::Value::String("value".to_string()),
        );

        let ep = timestamped_endpoint_with_replay(
            "ep",
            300,
            component_file.path().to_owned(),
            store_path,
            Some(operator_config),
            &secret,
        );
        let app = app_raw_with_sub("myapp", "ep");
        resolve(&[ep], &[app]);
    }

    // -----------------------------------------------------------------------
    // test-6: removed `wake_kind` on [[app.webhook_subscription]] is rejected
    // -----------------------------------------------------------------------

    /// Deserializing `AppWebhookSubscriptionRaw` with the removed `wake_kind` field
    /// (pre-urgency-redesign config key) must fail due to `deny_unknown_fields`.
    /// Guards the fail-fast boot behaviour described in design §2.7:
    /// operators upgrading with a config still containing `wake_kind` get an
    /// explicit parse error rather than silent field-ignore.
    #[test]
    fn app_webhook_subscription_wake_kind_rejected_by_deny_unknown_fields() {
        let toml_str = r#"endpoint = "my-endpoint"
wake_kind = "immediate"
"#;
        let result = toml::from_str::<AppWebhookSubscriptionRaw>(toml_str);
        assert!(
            result.is_err(),
            "AppWebhookSubscriptionRaw with wake_kind must fail to deserialize (deny_unknown_fields)"
        );
        let err_str = result.unwrap_err().to_string();
        assert!(
            err_str.contains("wake_kind"),
            "error message must mention 'wake_kind': {err_str}"
        );
    }

    /// Valid `AppWebhookSubscriptionRaw` (no `wake_kind`, optional `wake_min`) deserializes OK.
    #[test]
    fn app_webhook_subscription_valid_deserializes() {
        let toml_str = r#"endpoint = "my-endpoint"
wake_min = "low"
"#;
        let result = toml::from_str::<AppWebhookSubscriptionRaw>(toml_str);
        assert!(
            result.is_ok(),
            "valid AppWebhookSubscriptionRaw must deserialize: {:?}",
            result
        );
        let raw = result.unwrap();
        assert_eq!(raw.endpoint, "my-endpoint");
        use crate::messaging::WakeMin;
        assert_eq!(raw.wake_min, Some(WakeMin::Low));
    }
}
