//! Resolved signature-scheme types carried on `ResolvedWebhookEndpoint`.
//!
//! These are data: the header names, key/token tables and scheme parameters
//! that config resolution produces.

use std::collections::HashMap;

use http::HeaderName;

/// HMAC algorithm. Only `HmacSha256` in MVP.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignatureAlgorithm {
    HmacSha256,
}

/// Hex-encoding flavour used by HMAC variants.
///
/// Note: the prefix (e.g. `"v1="`) is matched case-sensitively; the hex body
/// is accepted in any case by `hex::decode` but providers are expected to emit
/// lowercase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HexFormat {
    /// Raw hex, 64 chars (Gitea `X-Gitea-Signature`).
    Hex,
    /// `v0=<hex>` (Slack `X-Slack-Signature`).
    V0Hex,
    /// `v1=<hex>` (phonebuddy `X-PhoneBuddy-Signature`).
    V1Hex,
    /// `sha256=<hex>` (GitHub `X-Hub-Signature-256`).
    Sha256Hex,
}

// ---------------------------------------------------------------------------
// Resolved per-endpoint signature scheme
// ---------------------------------------------------------------------------

/// Resolved per-endpoint signature scheme. Each variant is self-contained:
/// it carries the header names, the key/token table, and any scheme-specific
/// parameters (skew window, template parts).
///
/// Populated at config-resolve time from `WebhookSignatureConfigRaw`.
/// The hot path reads this directly — no re-parsing of header names or
/// secret files on the request path.
#[derive(Debug)]
pub enum SignatureScheme {
    /// HMAC-SHA256 over raw body. Phonebuddy, GitHub/Forgejo, generic.
    HmacRawBody {
        algorithm: SignatureAlgorithm,
        header: HeaderName,
        format: HexFormat,
        key_id_header: Option<HeaderName>,
        /// key_id → secret bytes
        keys: HashMap<String, Vec<u8>>,
    },
    /// HMAC-SHA256 over `<template>` filled with `{t}` from a separate
    /// timestamp header and `{body}` from raw body. Covers Slack.
    HmacTimestampedBody {
        algorithm: SignatureAlgorithm,
        sig_header: HeaderName,
        sig_format: HexFormat,
        timestamp_header: HeaderName,
        /// Template split at config-resolve time into parts around `{t}` and
        /// `{body}`. The hot path concatenates these without further parsing.
        template_prefix: String,
        template_mid: String,
        template_suffix: String,
        /// True when `{t}` appears before `{body}` in the template.
        t_before_body: bool,
        max_skew_secs: u64,
        key_id_header: Option<HeaderName>,
        /// key_id → secret bytes
        keys: HashMap<String, Vec<u8>>,
    },
    /// Stripe's combined `t=...,v1=...` header. HMAC over `<t>.<body>`.
    HmacStripe {
        algorithm: SignatureAlgorithm,
        header: HeaderName,
        max_skew_secs: u64,
        key_id_header: Option<HeaderName>,
        /// key_id → secret bytes
        keys: HashMap<String, Vec<u8>>,
    },
    /// No HMAC; constant-time compare of a header value against a configured
    /// secret. Google push, Mailgun bearer.
    BearerToken {
        header: HeaderName,
        token_id_header: Option<HeaderName>,
        /// token_id → expected bearer bytes
        tokens: HashMap<String, Vec<u8>>,
    },
}

impl SignatureScheme {
    /// Return the header name(s) whose values are credential secrets for this
    /// scheme. These are the headers whose values must be masked when building
    /// the `WebhookEnvelope`.
    ///
    /// Specifically:
    /// - HMAC variants: the signature header (`header` / `sig_header`) carries
    ///   the HMAC digest — exposing it allows offline brute-force attacks against
    ///   the signing key.
    /// - `BearerToken`: the bearer header carries the raw secret directly.
    ///
    /// `key_id_header`, `token_id_header`, and `timestamp_header` are **not**
    /// returned here; they are identifiers or public timestamps, not secrets.
    ///
    /// For `HmacStripe`, the combined `t=…,v1=…` header carries both a public
    /// timestamp (`t=`) and the HMAC signature (`v1=`). The whole header value is
    /// considered credential-bearing so the HMAC digest is never exposed, even
    /// though the timestamp portion is not secret.
    pub fn credential_header_names(&self) -> &[HeaderName] {
        match self {
            SignatureScheme::HmacRawBody { header, .. } => std::slice::from_ref(header),
            SignatureScheme::HmacTimestampedBody { sig_header, .. } => {
                std::slice::from_ref(sig_header)
            }
            SignatureScheme::HmacStripe { header, .. } => std::slice::from_ref(header),
            SignatureScheme::BearerToken { header, .. } => std::slice::from_ref(header),
        }
    }
}
