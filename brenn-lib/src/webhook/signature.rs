//! Webhook signature verification types and the `verify_request` free function.
//!
//! This module is the only cryptographic surface on the webhook transport.
//! It provides:
//! - The `SignatureScheme` enum (resolved form, carried on
//!   `ResolvedWebhookEndpoint`). Populated at config-resolve time.
//! - Supporting type enums (`SignatureAlgorithm`, `HexFormat`).
//! - `WebhookRejection` — failure modes from `verify_request`.
//! - `VerifiedRequest` — success payload from `verify_request`.
//! - `verify_request` — the one function that performs signature/bearer
//!   verification for all four `SignatureScheme` variants.

use std::collections::HashMap;
use std::time::SystemTime;

use bytes::Bytes;
use hmac::{Hmac, KeyInit, Mac};
use http::{HeaderMap, HeaderName, HeaderValue};
use sha2::Sha256;
use subtle::ConstantTimeEq;

use crate::webhook::config::ResolvedWebhookEndpoint;
use crate::webhook::is_valid_key_id;

// ---------------------------------------------------------------------------
// Supporting enums
// ---------------------------------------------------------------------------

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
    /// the `WebhookEnvelope` (design §2.2).
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
    /// though the timestamp portion is not secret. The validator (WASM replay
    /// component) receives the raw value before masking (design §2.2).
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

// ---------------------------------------------------------------------------
// Rejection and success types
// ---------------------------------------------------------------------------

/// Failure modes from `verify_request`.
#[derive(Debug, PartialEq, Eq)]
pub enum WebhookRejection {
    /// Content-Type doesn't match endpoint config. → 415, fail2ban.
    WrongContentType,
    /// Body is not valid UTF-8. → 400, fail2ban.
    BodyNotUtf8,
    /// Signature header absent, empty, or malformed per the configured format.
    /// Also used by `HmacStripe` for malformed `t=...,v1=...` parse, and by
    /// `HmacTimestampedBody` for a missing/malformed timestamp header.
    /// → 401, fail2ban.
    MissingOrMalformedSignatureHeader,
    /// `key_id_header` (or `token_id_header`) is configured but the request
    /// lacks it, or the value fails the key-id charset. → 401, fail2ban.
    MissingOrMalformedKeyIdHeader,
    /// key_id (or token_id) header parses cleanly but is not in the endpoint's
    /// key/token table. → 401, fail2ban. Timing-parity'd against `HmacMismatch`.
    UnknownKeyId,
    /// HMAC compute didn't match the provided signature (HMAC variants) OR
    /// bearer token didn't match (BearerToken variant). → 401, fail2ban.
    HmacMismatch,
    /// Timestamp is outside the configured `max_skew_secs` window
    /// (`HmacTimestampedBody` / `HmacStripe` only). → 401, fail2ban.
    TimestampOutOfWindow,
}

/// Success payload from `verify_request`.
#[derive(Debug)]
pub struct VerifiedRequest {
    /// The matched `key_id` for HMAC variants, or `token_id` for `BearerToken`.
    /// Single-key/single-token endpoints with no id-header configured yield
    /// the lone configured id.
    pub key_id: String,
    /// Raw body as UTF-8 string. Same bytes that were signed.
    pub body: String,
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Dummy key used for timing-parity when an unknown key_id/token_id is supplied.
/// 32 zero bytes — always fails the constant-time compare.
static DUMMY_KEY: [u8; 32] = [0u8; 32];

/// Compute HMAC-SHA256 over `data` using `key`. Returns 32 output bytes.
pub fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

/// Compute HMAC-SHA256 over multiple data slices fed in sequence, using `key`.
/// Equivalent to `hmac_sha256(key, &parts.concat())` but without allocating a
/// temporary buffer. Returns 32 output bytes.
pub fn hmac_sha256_parts(key: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC can take key of any size");
    for part in parts {
        mac.update(part);
    }
    mac.finalize().into_bytes().into()
}

/// Compute HMAC-SHA256 over `data` using `key` and return the result as a
/// lowercase hex string. Convenience wrapper used by test helpers in
/// `git.rs` and `inbound.rs` so they don't duplicate the hex-encode step.
pub fn hmac_sha256_hex(key: &[u8], data: &[u8]) -> String {
    hex::encode(hmac_sha256(key, data))
}

/// Verify a multi-part HMAC-SHA256 signature in constant time.
///
/// Returns `true` iff `sig` is exactly 32 bytes and matches
/// `HMAC-SHA256(key, parts[0] || parts[1] || … || parts[N-1])`.
///
/// The length check is deliberately non-constant-time (length is not secret).
/// The 32-byte comparison is constant-time via `subtle`. This is the single
/// constant-time HMAC gate for the whole module; all HMAC arms in
/// `verify_request` and `hmac_sha256_verify` route through here.
pub(crate) fn hmac_sha256_parts_verify(key: &[u8], parts: &[&[u8]], sig: &[u8]) -> bool {
    if sig.len() != 32 {
        return false;
    }
    let expected = hmac_sha256_parts(key, parts);
    expected.as_slice().ct_eq(sig).into()
}

/// Verify a raw HMAC-SHA256 signature in constant time.
///
/// Returns `true` iff `sig` is exactly 32 bytes and matches `HMAC-SHA256(key,
/// data)`. The length short-circuit is deliberately non-constant-time (length
/// is not secret) but short-circuits before any MAC computation; the 32-byte
/// comparison is constant-time via `subtle`.
///
/// Thin wrapper over `hmac_sha256_parts_verify(key, &[data], sig)`.
pub fn hmac_sha256_verify(key: &[u8], data: &[u8], sig: &[u8]) -> bool {
    hmac_sha256_parts_verify(key, &[data], sig)
}

/// Constant-time equality check for two byte slices.
///
/// Returns `true` iff both slices have the same length and identical contents.
/// Uses `subtle::ConstantTimeEq` to prevent timing oracles. Length inequality
/// is not secret in the webhook context (secret lengths are operator-controlled
/// config values), but the comparison is constant-time on equal-length inputs.
pub fn ct_eq_bytes(a: &[u8], b: &[u8]) -> bool {
    a.ct_eq(b).into()
}

/// Parse a hex-encoded HMAC signature from a header value per the given
/// `HexFormat`. Returns the raw 32 decoded bytes, or `None` on failure.
fn parse_sig_header(hv: &HeaderValue, format: HexFormat) -> Option<[u8; 32]> {
    let s = hv.to_str().ok()?;
    let hex_str = match format {
        HexFormat::Hex => s,
        HexFormat::V0Hex => s.strip_prefix("v0=")?,
        HexFormat::V1Hex => s.strip_prefix("v1=")?,
        HexFormat::Sha256Hex => s.strip_prefix("sha256=")?,
    };
    let bytes = hex::decode(hex_str).ok()?;
    if bytes.len() != 32 {
        return None;
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Some(arr)
}

/// Extract unix seconds from the current `SystemTime`. Panics on pre-epoch
/// times, which cannot occur in normal operation.
fn unix_secs_now(now: SystemTime) -> i64 {
    now.duration_since(std::time::UNIX_EPOCH)
        .expect("SystemTime before UNIX_EPOCH")
        .as_secs()
        .try_into()
        .expect("unix timestamp overflows i64")
}

/// Check content-type: extract the media-type token before the first `;`,
/// trim, lowercase, ASCII-case-insensitive compare.
fn check_content_type(
    content_type_header: Option<&HeaderValue>,
    expected: &str,
) -> Result<(), WebhookRejection> {
    let Some(hv) = content_type_header else {
        return Err(WebhookRejection::WrongContentType);
    };
    let s = hv
        .to_str()
        .map_err(|_| WebhookRejection::WrongContentType)?;
    let media_type = s
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    if media_type != expected {
        return Err(WebhookRejection::WrongContentType);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Key resolution with timing-parity
// ---------------------------------------------------------------------------

/// Result of resolving a key_id from the key table.
struct KeyResolution<'a> {
    effective_key: &'a [u8],
    resolved_id: String,
    unknown: bool,
}

/// Resolve the signing key from the key table. When `key_id_header` is
/// configured, reads it from `headers` and looks up in `keys`. When absent
/// from the table, substitutes `DUMMY_KEY` and sets `unknown = true`.
///
/// When `key_id_header` is `None` (single-key endpoint), returns the one
/// configured key unconditionally with `unknown = false`.
fn resolve_key<'a>(
    key_id_header: &Option<HeaderName>,
    keys: &'a HashMap<String, Vec<u8>>,
    headers: &HeaderMap,
) -> Result<KeyResolution<'a>, WebhookRejection> {
    match key_id_header {
        None => {
            // Single-key endpoint: exactly one entry guaranteed at config-resolve time.
            let (id, key) = keys.iter().next().expect(
                "invariant violated: single-key endpoint constructed with empty keys map; \
                 this is a bug in config resolution (resolve_signature_scheme must have \
                 rejected an empty keys list)",
            );
            Ok(KeyResolution {
                effective_key: key.as_slice(),
                resolved_id: id.clone(),
                unknown: false,
            })
        }
        Some(hdr) => {
            let raw_id = headers
                .get(hdr)
                .and_then(|v| v.to_str().ok())
                .ok_or(WebhookRejection::MissingOrMalformedKeyIdHeader)?;
            if !is_valid_key_id(raw_id) {
                return Err(WebhookRejection::MissingOrMalformedKeyIdHeader);
            }
            match keys.get(raw_id) {
                Some(key) => Ok(KeyResolution {
                    effective_key: key.as_slice(),
                    resolved_id: raw_id.to_string(),
                    unknown: false,
                }),
                None => Ok(KeyResolution {
                    effective_key: &DUMMY_KEY,
                    resolved_id: raw_id.to_string(),
                    unknown: true,
                }),
            }
        }
    }
}

/// Resolve a bearer token from the token table. Same timing-parity discipline
/// as `resolve_key`.
fn resolve_token<'a>(
    token_id_header: &Option<HeaderName>,
    tokens: &'a HashMap<String, Vec<u8>>,
    headers: &HeaderMap,
) -> Result<KeyResolution<'a>, WebhookRejection> {
    match token_id_header {
        None => {
            let (id, tok) = tokens
                .iter()
                .next()
                .expect("single-token endpoint has no tokens");
            Ok(KeyResolution {
                effective_key: tok.as_slice(),
                resolved_id: id.clone(),
                unknown: false,
            })
        }
        Some(hdr) => {
            let raw_id = headers
                .get(hdr)
                .and_then(|v| v.to_str().ok())
                .ok_or(WebhookRejection::MissingOrMalformedKeyIdHeader)?;
            if !is_valid_key_id(raw_id) {
                return Err(WebhookRejection::MissingOrMalformedKeyIdHeader);
            }
            match tokens.get(raw_id) {
                Some(tok) => Ok(KeyResolution {
                    effective_key: tok.as_slice(),
                    resolved_id: raw_id.to_string(),
                    unknown: false,
                }),
                None => Ok(KeyResolution {
                    effective_key: &DUMMY_KEY,
                    resolved_id: raw_id.to_string(),
                    unknown: true,
                }),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// verify_request
// ---------------------------------------------------------------------------

/// Perform the full host-side validation pipeline for an inbound webhook request.
///
/// Steps:
///
/// 1. Content-Type check (415 on mismatch).
/// 2. UTF-8 check on body (400 on failure).
/// 3. Scheme-dispatched auth (401 variants; all failure modes in `WebhookRejection`).
///
/// On success returns a `VerifiedRequest` with the matched trust-anchor
/// identifier and the raw body as UTF-8.
///
/// `now` is injected for testability; production callers pass `SystemTime::now()`.
pub fn verify_request(
    endpoint: &ResolvedWebhookEndpoint,
    content_type: Option<&HeaderValue>,
    headers: &HeaderMap,
    body: Bytes,
    now: SystemTime,
) -> Result<VerifiedRequest, WebhookRejection> {
    // Step 1: Content-Type check.
    check_content_type(content_type, &endpoint.content_type)?;

    // Step 2: UTF-8 check.
    let body_str = std::str::from_utf8(&body).map_err(|_| WebhookRejection::BodyNotUtf8)?;

    match &endpoint.scheme {
        // ------------------------------------------------------------------
        // HmacRawBody
        // ------------------------------------------------------------------
        SignatureScheme::HmacRawBody {
            algorithm: _,
            header,
            format,
            key_id_header,
            keys,
        } => {
            // Step 3: parse signature header.
            let sig = headers
                .get(header)
                .and_then(|hv| parse_sig_header(hv, *format))
                .ok_or(WebhookRejection::MissingOrMalformedSignatureHeader)?;

            // Step 4: resolve key (timing-parity).
            let kr = resolve_key(key_id_header, keys, headers)?;

            // Step 5: compute HMAC and compare via the shared constant-time gate.
            let ct_equal = hmac_sha256_verify(kr.effective_key, body.as_ref(), &sig);

            // Step 7: decide.
            if kr.unknown {
                Err(WebhookRejection::UnknownKeyId)
            } else if !ct_equal {
                Err(WebhookRejection::HmacMismatch)
            } else {
                Ok(VerifiedRequest {
                    key_id: kr.resolved_id,
                    body: body_str.to_string(),
                })
            }
        }

        // ------------------------------------------------------------------
        // HmacTimestampedBody
        // ------------------------------------------------------------------
        SignatureScheme::HmacTimestampedBody {
            algorithm: _,
            sig_header,
            sig_format,
            timestamp_header,
            template_prefix,
            template_mid,
            template_suffix,
            t_before_body,
            max_skew_secs,
            key_id_header,
            keys,
        } => {
            // Step 3: read timestamp header.
            let t_str = headers
                .get(timestamp_header)
                .and_then(|hv| hv.to_str().ok())
                .ok_or(WebhookRejection::MissingOrMalformedSignatureHeader)?;
            let t: i64 = t_str
                .parse()
                .map_err(|_| WebhookRejection::MissingOrMalformedSignatureHeader)?;

            // Step 4: skew check (cheap, done before HMAC; no timing channel —
            // timestamp is plaintext).
            // Use `checked_sub` to guard against attacker-supplied `t = i64::MIN`
            // which would overflow wrapping subtraction and panic in debug builds.
            // Overflow → treat as out-of-window (not as a valid timestamp).
            let now_secs = unix_secs_now(now);
            let skew = now_secs
                .checked_sub(t)
                .map(i64::unsigned_abs)
                .unwrap_or(u64::MAX);
            if skew > *max_skew_secs {
                return Err(WebhookRejection::TimestampOutOfWindow);
            }

            // Step 5: parse signature header.
            let sig = headers
                .get(sig_header)
                .and_then(|hv| parse_sig_header(hv, *sig_format))
                .ok_or(WebhookRejection::MissingOrMalformedSignatureHeader)?;

            // Step 6: resolve key (timing-parity).
            let kr = resolve_key(key_id_header, keys, headers)?;

            // Step 7: build canonical parts array. Layout: prefix || (t_str or body)
            // || mid || (body or t_str) || suffix. No temporary Vec allocation.
            let parts: [&[u8]; 5] = if *t_before_body {
                [
                    template_prefix.as_bytes(),
                    t_str.as_bytes(),
                    template_mid.as_bytes(),
                    body.as_ref(),
                    template_suffix.as_bytes(),
                ]
            } else {
                [
                    template_prefix.as_bytes(),
                    body.as_ref(),
                    template_mid.as_bytes(),
                    t_str.as_bytes(),
                    template_suffix.as_bytes(),
                ]
            };

            // Step 8: compute HMAC and compare via the shared constant-time gate.
            let ct_equal = hmac_sha256_parts_verify(kr.effective_key, &parts, &sig);

            // Step 9: decide.
            if kr.unknown {
                Err(WebhookRejection::UnknownKeyId)
            } else if !ct_equal {
                Err(WebhookRejection::HmacMismatch)
            } else {
                Ok(VerifiedRequest {
                    key_id: kr.resolved_id,
                    body: body_str.to_string(),
                })
            }
        }

        // ------------------------------------------------------------------
        // HmacStripe
        // ------------------------------------------------------------------
        SignatureScheme::HmacStripe {
            algorithm: _,
            header,
            max_skew_secs,
            key_id_header,
            keys,
        } => {
            // Step 3: parse the combined header into (t, [v1, v1, ...]).
            let raw_header = headers
                .get(header)
                .and_then(|hv| hv.to_str().ok())
                .ok_or(WebhookRejection::MissingOrMalformedSignatureHeader)?;

            let (t_raw, t_secs, v1_sigs) = parse_stripe_header(raw_header)?;

            // Step 4: skew check.
            // Use `checked_sub` to guard against attacker-supplied `t = i64::MIN`
            // which would overflow wrapping subtraction and panic in debug builds.
            // Overflow → treat as out-of-window.
            let now_secs = unix_secs_now(now);
            let skew = now_secs
                .checked_sub(t_secs)
                .map(i64::unsigned_abs)
                .unwrap_or(u64::MAX);
            if skew > *max_skew_secs {
                return Err(WebhookRejection::TimestampOutOfWindow);
            }

            // Step 5: resolve key (timing-parity).
            let kr = resolve_key(key_id_header, keys, headers)?;

            // Step 6: build canonical parts = t_raw || "." || body.
            // Use `t_raw` (the literal header bytes) rather than re-serialising
            // `t_secs` so we match Stripe's spec even if the sender emits a
            // non-canonical decimal form (e.g. `+1700000000`).
            let parts: [&[u8]; 3] = [t_raw.as_bytes(), b".", body.as_ref()];

            // Step 7: short-circuit over v1 candidates through the shared
            // constant-time gate. Each call performs one HMAC; loop exits on
            // first match. Timing reveals which candidate index matched (the
            // signer chooses the order), which is acceptable per requirements.
            let mut ct_equal = false;
            for v1 in &v1_sigs {
                if hmac_sha256_parts_verify(kr.effective_key, &parts, v1) {
                    ct_equal = true;
                    break;
                }
            }

            // Step 8: decide.
            if kr.unknown {
                Err(WebhookRejection::UnknownKeyId)
            } else if !ct_equal {
                Err(WebhookRejection::HmacMismatch)
            } else {
                Ok(VerifiedRequest {
                    key_id: kr.resolved_id,
                    body: body_str.to_string(),
                })
            }
        }

        // ------------------------------------------------------------------
        // BearerToken
        // ------------------------------------------------------------------
        SignatureScheme::BearerToken {
            header,
            token_id_header,
            tokens,
        } => {
            // Step 3: read bearer header.
            let supplied = headers
                .get(header)
                .map(|hv| hv.as_bytes())
                .filter(|b| !b.is_empty())
                .ok_or(WebhookRejection::MissingOrMalformedSignatureHeader)?;

            // Step 4: resolve token (timing-parity).
            let kr = resolve_token(token_id_header, tokens, headers)?;

            // Step 5: constant-time compare. `subtle::ConstantTimeEq` on `[u8]`
            // yields Choice(0) immediately on length mismatch — that's the
            // length-prefix timing channel acknowledged in design §3. Acceptable
            // for ≥32-byte random tokens.
            let ct_equal = bool::from(supplied.ct_eq(kr.effective_key));

            // Step 6: decide.
            if kr.unknown {
                Err(WebhookRejection::UnknownKeyId)
            } else if !ct_equal {
                Err(WebhookRejection::HmacMismatch)
            } else {
                Ok(VerifiedRequest {
                    key_id: kr.resolved_id,
                    body: body_str.to_string(),
                })
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Stripe header parser
// ---------------------------------------------------------------------------

/// Parse Stripe's `Stripe-Signature` header value.
///
/// Format: comma-separated `key=value` pairs; e.g.
/// `t=1492774577,v1=5257a869e7ecebeda32affa62cdca3fa51cad7e77a05bd539ba74dd4efd15da`
///
/// Returns `(t_raw: &str, t_secs: i64, v1_sigs: Vec<[u8; 32]>)`.
///
/// `t_raw` is the literal bytes of the `t=` value as it appears in the header.
/// Stripe's canonical string is `<t_raw>.<body>` — the spec signs the literal
/// header value, not a re-serialised integer. Using the raw string avoids
/// divergence if the sender emits a non-canonical decimal form (e.g. `+1700…`).
///
/// Errors map to `MissingOrMalformedSignatureHeader`.
fn parse_stripe_header(raw: &str) -> Result<(&str, i64, Vec<[u8; 32]>), WebhookRejection> {
    let mut t_raw: Option<&str> = None;
    let mut t_secs: Option<i64> = None;
    let mut v1_sigs: Vec<[u8; 32]> = Vec::new();

    for part in raw.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (key, value) = part
            .split_once('=')
            .ok_or(WebhookRejection::MissingOrMalformedSignatureHeader)?;
        match key {
            "t" => {
                let secs: i64 = value
                    .parse()
                    .map_err(|_| WebhookRejection::MissingOrMalformedSignatureHeader)?;
                t_raw = Some(value);
                t_secs = Some(secs);
            }
            "v1" => {
                let bytes = hex::decode(value)
                    .map_err(|_| WebhookRejection::MissingOrMalformedSignatureHeader)?;
                if bytes.len() != 32 {
                    return Err(WebhookRejection::MissingOrMalformedSignatureHeader);
                }
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&bytes);
                v1_sigs.push(arr);
            }
            // Other keys (e.g. `v0`) are silently skipped per Stripe's spec.
            _ => {}
        }
    }

    let t_raw = t_raw.ok_or(WebhookRejection::MissingOrMalformedSignatureHeader)?;
    let t_secs = t_secs.ok_or(WebhookRejection::MissingOrMalformedSignatureHeader)?;
    if v1_sigs.is_empty() {
        return Err(WebhookRejection::MissingOrMalformedSignatureHeader);
    }

    Ok((t_raw, t_secs, v1_sigs))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use http::HeaderMap;

    use super::*;
    use crate::messaging::Urgency;
    use crate::webhook::config::ResolvedWebhookEndpoint;

    // -----------------------------------------------------------------------
    // Test helpers
    // -----------------------------------------------------------------------

    fn make_hmac_raw_body_endpoint(
        key_id: &str,
        secret: &[u8],
        format: HexFormat,
        key_id_header: Option<&str>,
    ) -> ResolvedWebhookEndpoint {
        let mut keys = HashMap::new();
        keys.insert(key_id.to_string(), secret.to_vec());
        ResolvedWebhookEndpoint {
            slug: "test".to_string(),
            mount: "/webhooks/test".to_string(),
            description: None,
            transport_ceiling_bytes: 1024 * 1024,
            content_type: "application/json".to_string(),
            scheme: SignatureScheme::HmacRawBody {
                algorithm: SignatureAlgorithm::HmacSha256,
                header: "x-sig".parse().unwrap(),
                format,
                key_id_header: key_id_header.map(|s| s.parse().unwrap()),
                keys,
            },
            owner: crate::webhook::config::WebhookOwner::App("pa-test".into()),
            urgency: Urgency::Normal,
            replay_protection: None,
        }
    }

    fn make_bearer_endpoint(
        token_id: &str,
        token: &[u8],
        token_id_header: Option<&str>,
    ) -> ResolvedWebhookEndpoint {
        let mut tokens = HashMap::new();
        tokens.insert(token_id.to_string(), token.to_vec());
        ResolvedWebhookEndpoint {
            slug: "test".to_string(),
            mount: "/webhooks/test".to_string(),
            description: None,
            transport_ceiling_bytes: 1024 * 1024,
            content_type: "application/json".to_string(),
            scheme: SignatureScheme::BearerToken {
                header: "x-token".parse().unwrap(),
                token_id_header: token_id_header.map(|s| s.parse().unwrap()),
                tokens,
            },
            owner: crate::webhook::config::WebhookOwner::App("pa-test".into()),
            urgency: Urgency::Normal,
            replay_protection: None,
        }
    }

    fn make_hmac_timestamped_endpoint(
        key_id: &str,
        secret: &[u8],
        template: &str,
        max_skew_secs: u64,
    ) -> ResolvedWebhookEndpoint {
        // Parse the template into (prefix, mid, suffix, t_before_body).
        // This mirrors what config.rs does at resolve time.
        let (t_before_body, template_prefix, template_mid, template_suffix) =
            parse_template_for_test(template);
        let mut keys = HashMap::new();
        keys.insert(key_id.to_string(), secret.to_vec());
        ResolvedWebhookEndpoint {
            slug: "test".to_string(),
            mount: "/webhooks/test".to_string(),
            description: None,
            transport_ceiling_bytes: 1024 * 1024,
            content_type: "application/json".to_string(),
            scheme: SignatureScheme::HmacTimestampedBody {
                algorithm: SignatureAlgorithm::HmacSha256,
                sig_header: "x-sig".parse().unwrap(),
                sig_format: HexFormat::V0Hex,
                timestamp_header: "x-timestamp".parse().unwrap(),
                template_prefix,
                template_mid,
                template_suffix,
                t_before_body,
                max_skew_secs,
                key_id_header: None,
                keys,
            },
            owner: crate::webhook::config::WebhookOwner::App("pa-test".into()),
            urgency: Urgency::Normal,
            replay_protection: None,
        }
    }

    fn parse_template_for_test(template: &str) -> (bool, String, String, String) {
        // Determine order of {t} and {body}.
        // "{t}" is 3 bytes; "{body}" is 6 bytes.
        let t_pos = template.find("{t}").unwrap();
        let body_pos = template.find("{body}").unwrap();
        let t_before_body = t_pos < body_pos;
        if t_before_body {
            let prefix = template[..t_pos].to_string();
            let mid = template[t_pos + 3..body_pos].to_string();
            let suffix = template[body_pos + 6..].to_string();
            (true, prefix, mid, suffix)
        } else {
            let prefix = template[..body_pos].to_string();
            let mid = template[body_pos + 6..t_pos].to_string();
            let suffix = template[t_pos + 3..].to_string();
            (false, prefix, mid, suffix)
        }
    }

    fn make_stripe_endpoint(
        key_id: &str,
        secret: &[u8],
        max_skew_secs: u64,
    ) -> ResolvedWebhookEndpoint {
        let mut keys = HashMap::new();
        keys.insert(key_id.to_string(), secret.to_vec());
        ResolvedWebhookEndpoint {
            slug: "test".to_string(),
            mount: "/webhooks/test".to_string(),
            description: None,
            transport_ceiling_bytes: 1024 * 1024,
            content_type: "application/json".to_string(),
            scheme: SignatureScheme::HmacStripe {
                algorithm: SignatureAlgorithm::HmacSha256,
                header: "stripe-signature".parse().unwrap(),
                max_skew_secs,
                key_id_header: None,
                keys,
            },
            owner: crate::webhook::config::WebhookOwner::App("pa-test".into()),
            urgency: Urgency::Normal,
            replay_protection: None,
        }
    }

    fn at_unix(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    fn make_sig(format: HexFormat, mac: &[u8; 32]) -> String {
        let hex_str = hex::encode(mac);
        match format {
            HexFormat::Hex => hex_str,
            HexFormat::V0Hex => format!("v0={}", hex_str),
            HexFormat::V1Hex => format!("v1={}", hex_str),
            HexFormat::Sha256Hex => format!("sha256={}", hex_str),
        }
    }

    // -----------------------------------------------------------------------
    // Content-type rejection tests
    // -----------------------------------------------------------------------

    #[test]
    fn wrong_content_type_rejected() {
        let ep = make_hmac_raw_body_endpoint("k1", b"secret", HexFormat::Hex, None);
        let ct: HeaderValue = "text/plain".parse().unwrap();
        let headers = HeaderMap::new();
        let body = Bytes::from(b"{}".as_ref());
        let result = verify_request(&ep, Some(&ct), &headers, body, at_unix(1000));
        assert_eq!(result.unwrap_err(), WebhookRejection::WrongContentType);
    }

    #[test]
    fn missing_content_type_rejected() {
        let ep = make_hmac_raw_body_endpoint("k1", b"secret", HexFormat::Hex, None);
        let headers = HeaderMap::new();
        let body = Bytes::from(b"{}".as_ref());
        let result = verify_request(&ep, None, &headers, body, at_unix(1000));
        assert_eq!(result.unwrap_err(), WebhookRejection::WrongContentType);
    }

    #[test]
    fn content_type_with_params_accepted() {
        // "application/json; charset=utf-8" should strip to "application/json".
        let ep = make_hmac_raw_body_endpoint("k1", b"secret", HexFormat::Hex, None);
        let ct: HeaderValue = "application/json; charset=utf-8".parse().unwrap();
        let body = Bytes::from(b"{}".as_ref());
        let mac = hmac_sha256(b"secret", &body);
        let mut headers = HeaderMap::new();
        headers.insert("x-sig", make_sig(HexFormat::Hex, &mac).parse().unwrap());
        let result = verify_request(&ep, Some(&ct), &headers, body, at_unix(1000));
        assert!(result.is_ok(), "{:?}", result.unwrap_err());
    }

    // -----------------------------------------------------------------------
    // UTF-8 rejection tests
    // -----------------------------------------------------------------------

    #[test]
    fn non_utf8_body_rejected() {
        let ep = make_hmac_raw_body_endpoint("k1", b"secret", HexFormat::Hex, None);
        let ct: HeaderValue = "application/json".parse().unwrap();
        let body = Bytes::from(vec![0xff, 0xfe]);
        let mac = hmac_sha256(b"secret", &body);
        let mut headers = HeaderMap::new();
        headers.insert("x-sig", make_sig(HexFormat::Hex, &mac).parse().unwrap());
        let result = verify_request(&ep, Some(&ct), &headers, body, at_unix(1000));
        assert_eq!(result.unwrap_err(), WebhookRejection::BodyNotUtf8);
    }

    // -----------------------------------------------------------------------
    // HmacRawBody tests
    // -----------------------------------------------------------------------

    #[test]
    fn hmac_raw_body_happy_path_hex() {
        let ep = make_hmac_raw_body_endpoint("k1", b"mysecret", HexFormat::Hex, None);
        let ct: HeaderValue = "application/json".parse().unwrap();
        let body = Bytes::from("{\"hello\":\"world\"}");
        let mac = hmac_sha256(b"mysecret", &body);
        let mut headers = HeaderMap::new();
        headers.insert("x-sig", make_sig(HexFormat::Hex, &mac).parse().unwrap());
        let result = verify_request(&ep, Some(&ct), &headers, body.clone(), at_unix(1000));
        let vr = result.unwrap();
        assert_eq!(vr.key_id, "k1");
        assert_eq!(vr.body, "{\"hello\":\"world\"}");
    }

    #[test]
    fn hmac_raw_body_happy_path_v1_hex() {
        let ep =
            make_hmac_raw_body_endpoint("primary", b"phonebuddy-secret", HexFormat::V1Hex, None);
        let ct: HeaderValue = "application/json".parse().unwrap();
        let body = Bytes::from("{\"kind\":\"ping\"}");
        let mac = hmac_sha256(b"phonebuddy-secret", &body);
        let mut headers = HeaderMap::new();
        headers.insert("x-sig", make_sig(HexFormat::V1Hex, &mac).parse().unwrap());
        let result = verify_request(&ep, Some(&ct), &headers, body, at_unix(1000));
        assert!(result.is_ok());
    }

    #[test]
    fn hmac_raw_body_sha256hex_format() {
        let ep = make_hmac_raw_body_endpoint("gh", b"github-secret", HexFormat::Sha256Hex, None);
        let ct: HeaderValue = "application/json".parse().unwrap();
        let body = Bytes::from("{\"action\":\"push\"}");
        let mac = hmac_sha256(b"github-secret", &body);
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-sig",
            make_sig(HexFormat::Sha256Hex, &mac).parse().unwrap(),
        );
        let result = verify_request(&ep, Some(&ct), &headers, body, at_unix(1000));
        assert!(result.is_ok());
    }

    #[test]
    fn hmac_raw_body_missing_sig_header() {
        let ep = make_hmac_raw_body_endpoint("k1", b"secret", HexFormat::Hex, None);
        let ct: HeaderValue = "application/json".parse().unwrap();
        let body = Bytes::from("{}");
        let headers = HeaderMap::new();
        let result = verify_request(&ep, Some(&ct), &headers, body, at_unix(1000));
        assert_eq!(
            result.unwrap_err(),
            WebhookRejection::MissingOrMalformedSignatureHeader
        );
    }

    #[test]
    fn hmac_raw_body_malformed_sig_header_wrong_prefix() {
        let ep = make_hmac_raw_body_endpoint("k1", b"secret", HexFormat::V1Hex, None);
        let ct: HeaderValue = "application/json".parse().unwrap();
        let body = Bytes::from("{}");
        let mac = hmac_sha256(b"secret", &body);
        let mut headers = HeaderMap::new();
        // Send "sha256=..." when "v1=..." is expected — prefix mismatch.
        headers.insert(
            "x-sig",
            make_sig(HexFormat::Sha256Hex, &mac).parse().unwrap(),
        );
        let result = verify_request(&ep, Some(&ct), &headers, body, at_unix(1000));
        assert_eq!(
            result.unwrap_err(),
            WebhookRejection::MissingOrMalformedSignatureHeader
        );
    }

    #[test]
    fn hmac_raw_body_hmac_mismatch() {
        let ep = make_hmac_raw_body_endpoint("k1", b"correct-secret", HexFormat::Hex, None);
        let ct: HeaderValue = "application/json".parse().unwrap();
        let body = Bytes::from("{}");
        // Sign with the wrong key.
        let mac = hmac_sha256(b"wrong-secret", &body);
        let mut headers = HeaderMap::new();
        headers.insert("x-sig", make_sig(HexFormat::Hex, &mac).parse().unwrap());
        let result = verify_request(&ep, Some(&ct), &headers, body, at_unix(1000));
        assert_eq!(result.unwrap_err(), WebhookRejection::HmacMismatch);
    }

    #[test]
    fn hmac_raw_body_unknown_key_id() {
        // Endpoint has a key_id_header configured; request supplies an unknown id.
        let ep = make_hmac_raw_body_endpoint(
            "primary",
            b"primary-secret",
            HexFormat::Hex,
            Some("x-key-id"),
        );
        let ct: HeaderValue = "application/json".parse().unwrap();
        let body = Bytes::from("{}");
        // Sign with correct secret so we know it's the unknown-id path, not HMAC.
        let mac = hmac_sha256(b"primary-secret", &body);
        let mut headers = HeaderMap::new();
        headers.insert("x-sig", make_sig(HexFormat::Hex, &mac).parse().unwrap());
        headers.insert("x-key-id", "not-a-real-key".parse().unwrap());
        let result = verify_request(&ep, Some(&ct), &headers, body, at_unix(1000));
        assert_eq!(result.unwrap_err(), WebhookRejection::UnknownKeyId);
    }

    #[test]
    fn hmac_raw_body_missing_key_id_header() {
        // Endpoint requires key_id_header; request omits it.
        let ep = make_hmac_raw_body_endpoint(
            "primary",
            b"primary-secret",
            HexFormat::Hex,
            Some("x-key-id"),
        );
        let ct: HeaderValue = "application/json".parse().unwrap();
        let body = Bytes::from("{}");
        let mac = hmac_sha256(b"primary-secret", &body);
        let mut headers = HeaderMap::new();
        headers.insert("x-sig", make_sig(HexFormat::Hex, &mac).parse().unwrap());
        // No x-key-id header.
        let result = verify_request(&ep, Some(&ct), &headers, body, at_unix(1000));
        assert_eq!(
            result.unwrap_err(),
            WebhookRejection::MissingOrMalformedKeyIdHeader
        );
    }

    #[test]
    fn hmac_raw_body_with_key_id_header_happy() {
        // Endpoint with key_id_header; request provides valid id.
        let ep = make_hmac_raw_body_endpoint(
            "primary",
            b"primary-secret",
            HexFormat::Hex,
            Some("x-key-id"),
        );
        let ct: HeaderValue = "application/json".parse().unwrap();
        let body = Bytes::from("{\"hello\":\"world\"}");
        let mac = hmac_sha256(b"primary-secret", &body);
        let mut headers = HeaderMap::new();
        headers.insert("x-sig", make_sig(HexFormat::Hex, &mac).parse().unwrap());
        headers.insert("x-key-id", "primary".parse().unwrap());
        let result = verify_request(&ep, Some(&ct), &headers, body, at_unix(1000));
        let vr = result.unwrap();
        assert_eq!(vr.key_id, "primary");
    }

    /// A key_id value of exactly 65 characters (one over the 64-char limit)
    /// is rejected as `MissingOrMalformedKeyIdHeader`, not `UnknownKeyId`.
    /// This distinction matters for fail2ban classification: an oversized key_id
    /// is a malformed header (structural probe), not an unknown-key attempt.
    #[test]
    fn oversized_key_id_returns_malformed_header_not_unknown_key() {
        let ep = make_hmac_raw_body_endpoint(
            "primary",
            b"primary-secret",
            HexFormat::Hex,
            Some("x-key-id"),
        );
        let ct: HeaderValue = "application/json".parse().unwrap();
        let body = Bytes::from("{}");
        let mac = hmac_sha256(b"primary-secret", &body);
        let mut headers = HeaderMap::new();
        headers.insert("x-sig", make_sig(HexFormat::Hex, &mac).parse().unwrap());
        // 65-character key_id — one over the allowed maximum.
        let oversized_key_id: String = "a".repeat(65);
        headers.insert("x-key-id", oversized_key_id.parse().unwrap());
        let result = verify_request(&ep, Some(&ct), &headers, body, at_unix(1000));
        assert_eq!(
            result.unwrap_err(),
            WebhookRejection::MissingOrMalformedKeyIdHeader,
            "65-char key_id must be rejected as malformed, not as unknown key"
        );
    }

    // -----------------------------------------------------------------------
    // HmacTimestampedBody tests
    // -----------------------------------------------------------------------

    #[test]
    fn hmac_timestamped_body_happy_path() {
        // Slack-style: "v0:{t}:{body}"
        let ep = make_hmac_timestamped_endpoint("k1", b"slack-secret", "v0:{t}:{body}", 300);
        let ct: HeaderValue = "application/json".parse().unwrap();
        let t: i64 = 1000;
        let body = Bytes::from("{\"event\":\"message\"}");
        // canonical: "v0:1000:{body}"
        let canonical = format!("v0:{}:", t)
            .into_bytes()
            .into_iter()
            .chain(body.iter().copied())
            .collect::<Vec<u8>>();
        let mac = hmac_sha256(b"slack-secret", &canonical);
        let mut headers = HeaderMap::new();
        headers.insert("x-sig", make_sig(HexFormat::V0Hex, &mac).parse().unwrap());
        headers.insert("x-timestamp", t.to_string().parse().unwrap());
        let result = verify_request(&ep, Some(&ct), &headers, body.clone(), at_unix(1000));
        let vr = result.unwrap();
        assert_eq!(vr.key_id, "k1");
        assert_eq!(vr.body, "{\"event\":\"message\"}");
    }

    #[test]
    fn hmac_timestamped_body_timestamp_out_of_window() {
        let ep = make_hmac_timestamped_endpoint("k1", b"slack-secret", "v0:{t}:{body}", 300);
        let ct: HeaderValue = "application/json".parse().unwrap();
        let body = Bytes::from("{}");
        let t: i64 = 1000;
        // 400 seconds ago — outside the 300-second skew window.
        let now = at_unix(1400);
        let canonical = format!("v0:{}:", t)
            .into_bytes()
            .into_iter()
            .chain(body.iter().copied())
            .collect::<Vec<u8>>();
        let mac = hmac_sha256(b"slack-secret", &canonical);
        let mut headers = HeaderMap::new();
        headers.insert("x-sig", make_sig(HexFormat::V0Hex, &mac).parse().unwrap());
        headers.insert("x-timestamp", t.to_string().parse().unwrap());
        let result = verify_request(&ep, Some(&ct), &headers, body, now);
        assert_eq!(result.unwrap_err(), WebhookRejection::TimestampOutOfWindow);
    }

    #[test]
    fn hmac_timestamped_body_missing_timestamp_header() {
        let ep = make_hmac_timestamped_endpoint("k1", b"slack-secret", "v0:{t}:{body}", 300);
        let ct: HeaderValue = "application/json".parse().unwrap();
        let body = Bytes::from("{}");
        let mac = hmac_sha256(b"slack-secret", b"v0:0:{}");
        let mut headers = HeaderMap::new();
        headers.insert("x-sig", make_sig(HexFormat::V0Hex, &mac).parse().unwrap());
        // No x-timestamp header.
        let result = verify_request(&ep, Some(&ct), &headers, body, at_unix(1000));
        assert_eq!(
            result.unwrap_err(),
            WebhookRejection::MissingOrMalformedSignatureHeader
        );
    }

    #[test]
    fn hmac_timestamped_body_hmac_mismatch() {
        let ep = make_hmac_timestamped_endpoint("k1", b"slack-secret", "v0:{t}:{body}", 300);
        let ct: HeaderValue = "application/json".parse().unwrap();
        let body = Bytes::from("{}");
        let t: i64 = 1000;
        let mac = hmac_sha256(b"wrong-secret", b"v0:1000:{}");
        let mut headers = HeaderMap::new();
        headers.insert("x-sig", make_sig(HexFormat::V0Hex, &mac).parse().unwrap());
        headers.insert("x-timestamp", t.to_string().parse().unwrap());
        let result = verify_request(&ep, Some(&ct), &headers, body, at_unix(1000));
        assert_eq!(result.unwrap_err(), WebhookRejection::HmacMismatch);
    }

    /// Body-before-t ordering (`{body}:{t}` template): canonical bytes are
    /// assembled as `prefix || body || mid || t_str || suffix`. This test
    /// exercises the `t_before_body = false` branch in `verify_request` that
    /// was previously untested at the verify layer.
    #[test]
    fn hmac_timestamped_body_before_t_happy_path() {
        // Template: "prefix:{body}:{t}:suffix"
        // Canonical bytes = b"prefix:" + body + b":" + t_str + b":suffix"
        let template = "prefix:{body}:{t}:suffix";
        let ep = make_hmac_timestamped_endpoint("k1", b"secret", template, 300);
        let ct: HeaderValue = "application/json".parse().unwrap();
        let body_bytes = b"{\"hello\":1}";
        let t: i64 = 1000;
        let canonical = format!(
            "prefix:{}:{}:suffix",
            std::str::from_utf8(body_bytes).unwrap(),
            t
        );
        let mac = hmac_sha256(b"secret", canonical.as_bytes());
        let mut headers = HeaderMap::new();
        headers.insert("x-sig", make_sig(HexFormat::V0Hex, &mac).parse().unwrap());
        headers.insert("x-timestamp", t.to_string().parse().unwrap());
        let body = Bytes::from(body_bytes.as_ref());
        let result = verify_request(&ep, Some(&ct), &headers, body, at_unix(t as u64));
        let vr = result.expect("body-before-t verify_request should succeed");
        assert_eq!(vr.key_id, "k1");
        assert_eq!(vr.body, std::str::from_utf8(body_bytes).unwrap());
    }

    // -----------------------------------------------------------------------
    // HmacStripe tests
    // -----------------------------------------------------------------------

    fn stripe_header(t: i64, mac: &[u8; 32]) -> String {
        format!("t={},v1={}", t, hex::encode(mac))
    }

    #[test]
    fn hmac_stripe_happy_path() {
        let ep = make_stripe_endpoint("stripe-key", b"stripe-secret", 300);
        let ct: HeaderValue = "application/json".parse().unwrap();
        let t: i64 = 1000;
        let body = Bytes::from("{\"type\":\"payment_intent.created\"}");
        let canonical = format!("{}.", t)
            .into_bytes()
            .into_iter()
            .chain(body.iter().copied())
            .collect::<Vec<u8>>();
        let mac = hmac_sha256(b"stripe-secret", &canonical);
        let mut headers = HeaderMap::new();
        headers.insert("stripe-signature", stripe_header(t, &mac).parse().unwrap());
        let result = verify_request(&ep, Some(&ct), &headers, body.clone(), at_unix(1000));
        let vr = result.unwrap();
        assert_eq!(vr.key_id, "stripe-key");
        assert_eq!(vr.body, "{\"type\":\"payment_intent.created\"}");
    }

    #[test]
    fn hmac_stripe_multiple_v1_any_match() {
        // Stripe sends multiple v1= values for rotation; one correct match suffices.
        let ep = make_stripe_endpoint("stripe-key", b"stripe-secret", 300);
        let ct: HeaderValue = "application/json".parse().unwrap();
        let t: i64 = 1000;
        let body = Bytes::from("{}");
        let canonical = format!("{}.", t)
            .into_bytes()
            .into_iter()
            .chain(body.iter().copied())
            .collect::<Vec<u8>>();
        let mac_correct = hmac_sha256(b"stripe-secret", &canonical);
        let mac_other = hmac_sha256(b"old-secret", &canonical);
        let hdr = format!(
            "t={},v1={},v1={}",
            t,
            hex::encode(mac_other),
            hex::encode(mac_correct)
        );
        let mut headers = HeaderMap::new();
        headers.insert("stripe-signature", hdr.parse().unwrap());
        let result = verify_request(&ep, Some(&ct), &headers, body, at_unix(1000));
        let vr = result.expect("multiple v1= with one correct candidate should succeed");
        assert_eq!(vr.key_id, "stripe-key");
        assert_eq!(vr.body, "{}");
    }

    #[test]
    fn hmac_stripe_timestamp_out_of_window() {
        let ep = make_stripe_endpoint("stripe-key", b"stripe-secret", 300);
        let ct: HeaderValue = "application/json".parse().unwrap();
        let t: i64 = 1000;
        let body = Bytes::from("{}");
        let canonical = format!("{}.", t)
            .into_bytes()
            .into_iter()
            .chain(body.iter().copied())
            .collect::<Vec<u8>>();
        let mac = hmac_sha256(b"stripe-secret", &canonical);
        let mut headers = HeaderMap::new();
        headers.insert("stripe-signature", stripe_header(t, &mac).parse().unwrap());
        let result = verify_request(&ep, Some(&ct), &headers, body, at_unix(1400));
        assert_eq!(result.unwrap_err(), WebhookRejection::TimestampOutOfWindow);
    }

    #[test]
    fn hmac_stripe_malformed_header_no_t() {
        let ep = make_stripe_endpoint("stripe-key", b"stripe-secret", 300);
        let ct: HeaderValue = "application/json".parse().unwrap();
        let body = Bytes::from("{}");
        let mac = hmac_sha256(b"stripe-secret", b"1000.{}");
        let mut headers = HeaderMap::new();
        // No "t=" entry.
        headers.insert(
            "stripe-signature",
            format!("v1={}", hex::encode(mac)).parse().unwrap(),
        );
        let result = verify_request(&ep, Some(&ct), &headers, body, at_unix(1000));
        assert_eq!(
            result.unwrap_err(),
            WebhookRejection::MissingOrMalformedSignatureHeader
        );
    }

    #[test]
    fn hmac_stripe_hmac_mismatch() {
        let ep = make_stripe_endpoint("stripe-key", b"stripe-secret", 300);
        let ct: HeaderValue = "application/json".parse().unwrap();
        let t: i64 = 1000;
        let body = Bytes::from("{}");
        let mac = hmac_sha256(b"wrong-secret", b"1000.{}");
        let mut headers = HeaderMap::new();
        headers.insert("stripe-signature", stripe_header(t, &mac).parse().unwrap());
        let result = verify_request(&ep, Some(&ct), &headers, body, at_unix(1000));
        assert_eq!(result.unwrap_err(), WebhookRejection::HmacMismatch);
    }

    // -----------------------------------------------------------------------
    // BearerToken tests
    // -----------------------------------------------------------------------

    #[test]
    fn bearer_happy_path() {
        let ep = make_bearer_endpoint("goog", b"my-super-secret-token", None);
        let ct: HeaderValue = "application/json".parse().unwrap();
        let body = Bytes::from("{}");
        let mut headers = HeaderMap::new();
        headers.insert("x-token", "my-super-secret-token".parse().unwrap());
        let result = verify_request(&ep, Some(&ct), &headers, body, at_unix(1000));
        let vr = result.unwrap();
        assert_eq!(vr.key_id, "goog");
    }

    #[test]
    fn bearer_mismatch() {
        let ep = make_bearer_endpoint("goog", b"my-super-secret-token", None);
        let ct: HeaderValue = "application/json".parse().unwrap();
        let body = Bytes::from("{}");
        let mut headers = HeaderMap::new();
        headers.insert("x-token", "wrong-token".parse().unwrap());
        let result = verify_request(&ep, Some(&ct), &headers, body, at_unix(1000));
        assert_eq!(result.unwrap_err(), WebhookRejection::HmacMismatch);
    }

    #[test]
    fn bearer_missing_header() {
        let ep = make_bearer_endpoint("goog", b"my-super-secret-token", None);
        let ct: HeaderValue = "application/json".parse().unwrap();
        let body = Bytes::from("{}");
        let headers = HeaderMap::new();
        let result = verify_request(&ep, Some(&ct), &headers, body, at_unix(1000));
        assert_eq!(
            result.unwrap_err(),
            WebhookRejection::MissingOrMalformedSignatureHeader
        );
    }

    #[test]
    fn bearer_unknown_token_id() {
        let ep = make_bearer_endpoint("primary", b"my-secret-token", Some("x-token-id"));
        let ct: HeaderValue = "application/json".parse().unwrap();
        let body = Bytes::from("{}");
        let mut headers = HeaderMap::new();
        headers.insert("x-token", "my-secret-token".parse().unwrap());
        headers.insert("x-token-id", "not-a-real-id".parse().unwrap());
        let result = verify_request(&ep, Some(&ct), &headers, body, at_unix(1000));
        assert_eq!(result.unwrap_err(), WebhookRejection::UnknownKeyId);
    }

    #[test]
    fn bearer_with_token_id_header_happy() {
        let ep = make_bearer_endpoint("primary", b"my-secret-token", Some("x-token-id"));
        let ct: HeaderValue = "application/json".parse().unwrap();
        let body = Bytes::from("{}");
        let mut headers = HeaderMap::new();
        headers.insert("x-token", "my-secret-token".parse().unwrap());
        headers.insert("x-token-id", "primary".parse().unwrap());
        let result = verify_request(&ep, Some(&ct), &headers, body, at_unix(1000));
        let vr = result.unwrap();
        assert_eq!(vr.key_id, "primary");
    }

    // -----------------------------------------------------------------------
    // Stripe header parser tests
    // -----------------------------------------------------------------------

    #[test]
    fn parse_stripe_header_basic() {
        let mac = hmac_sha256(b"key", b"data");
        let raw = format!("t=1234567890,v1={}", hex::encode(mac));
        let (t_raw, t_secs, sigs) = parse_stripe_header(&raw).unwrap();
        assert_eq!(t_raw, "1234567890");
        assert_eq!(t_secs, 1234567890);
        assert_eq!(sigs.len(), 1);
        assert_eq!(&sigs[0], &mac);
    }

    #[test]
    fn parse_stripe_header_multiple_v1() {
        let mac1 = hmac_sha256(b"key1", b"data");
        let mac2 = hmac_sha256(b"key2", b"data");
        let raw = format!("t=9999,v1={},v1={}", hex::encode(mac1), hex::encode(mac2));
        let (t_raw, t_secs, sigs) = parse_stripe_header(&raw).unwrap();
        assert_eq!(t_raw, "9999");
        assert_eq!(t_secs, 9999);
        assert_eq!(sigs.len(), 2);
    }

    #[test]
    fn parse_stripe_header_missing_t() {
        let mac = hmac_sha256(b"key", b"data");
        let raw = format!("v1={}", hex::encode(mac));
        let err = parse_stripe_header(&raw).unwrap_err();
        assert_eq!(err, WebhookRejection::MissingOrMalformedSignatureHeader);
    }

    #[test]
    fn parse_stripe_header_missing_v1() {
        let raw = "t=1000".to_string();
        let err = parse_stripe_header(&raw).unwrap_err();
        assert_eq!(err, WebhookRejection::MissingOrMalformedSignatureHeader);
    }

    // -----------------------------------------------------------------------
    // hmac_sha256_parts equivalence test (test-3)
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // hmac_sha256_hex and hmac_sha256_verify unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn hmac_sha256_hex_produces_lowercase_hex_of_hmac() {
        let key = b"test-key";
        let data = b"test-data";
        let raw = hmac_sha256(key, data);
        let expected_hex = hex::encode(raw);
        // Must match hex::encode of the raw digest, and must be lowercase.
        assert_eq!(hmac_sha256_hex(key, data), expected_hex);
        // Spot-check: the hex string is 64 lowercase hex chars.
        let h = hmac_sha256_hex(key, data);
        assert_eq!(h.len(), 64);
        assert!(
            h.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase())
        );
    }

    #[test]
    fn hmac_sha256_verify_correct_returns_true() {
        let key = b"verify-key";
        let data = b"verify-data";
        let sig = hmac_sha256(key, data);
        assert!(hmac_sha256_verify(key, data, &sig));
    }

    #[test]
    fn hmac_sha256_verify_wrong_key_returns_false() {
        let key = b"verify-key";
        let wrong_key = b"wrong-key!!";
        let data = b"verify-data";
        let sig = hmac_sha256(key, data);
        assert!(!hmac_sha256_verify(wrong_key, data, &sig));
    }

    #[test]
    fn hmac_sha256_verify_wrong_length_returns_false() {
        let key = b"verify-key";
        let data = b"verify-data";
        // A sig shorter than 32 bytes should be rejected without computing the MAC.
        let short_sig = &hmac_sha256(key, data)[..16];
        assert!(!hmac_sha256_verify(key, data, short_sig));
        // A sig longer than 32 bytes should also be rejected.
        let long_sig = [hmac_sha256(key, data).as_slice(), b"\x00"].concat();
        assert!(!hmac_sha256_verify(key, data, &long_sig));
    }

    #[test]
    fn hmac_sha256_parts_equivalent_to_concat() {
        // Verify that feeding parts one at a time gives the same digest as
        // concatenating them first. Non-trivial slices chosen to exercise
        // boundary alignment in the HMAC update path.
        let key = b"test-key-for-parts-equivalence";
        let a: &[u8] = b"prefix-data/";
        let b: &[u8] = b"middle-chunk";
        let c: &[u8] = b":suffix";
        let parts_result = hmac_sha256_parts(key, &[a, b, c]);
        let concat: Vec<u8> = [a, b, c].concat();
        let concat_result = hmac_sha256(key, &concat);
        assert_eq!(
            parts_result, concat_result,
            "hmac_sha256_parts must be equivalent to hmac_sha256 on concatenated input"
        );
    }

    /// `hmac_sha256_parts_verify(key, &[data], sig)` must agree with
    /// `hmac_sha256_verify(key, data, sig)` for both matching and non-matching
    /// signatures. Pins the forwarding invariant introduced by the consolidation
    /// refactor.
    #[test]
    fn hmac_sha256_parts_verify_single_part_equivalent() {
        let key = b"gate-key";
        let data = b"gate-data";
        let sig_match = hmac_sha256(key, data);
        let sig_wrong = hmac_sha256(b"other-key", data);

        // Matching sig: both helpers return true.
        assert!(
            hmac_sha256_parts_verify(key, &[data], &sig_match),
            "parts_verify(single part) must return true on match"
        );
        assert!(
            hmac_sha256_verify(key, data, &sig_match),
            "hmac_sha256_verify must return true on match"
        );

        // Non-matching sig: both helpers return false.
        assert!(
            !hmac_sha256_parts_verify(key, &[data], &sig_wrong),
            "parts_verify(single part) must return false on mismatch"
        );
        assert!(
            !hmac_sha256_verify(key, data, &sig_wrong),
            "hmac_sha256_verify must return false on mismatch"
        );
    }

    /// `hmac_sha256_parts_verify` with multiple parts must match the
    /// equivalent concatenation, reject a wrong key, and enforce the 32-byte
    /// length guard directly at the primitive level.
    #[test]
    fn hmac_sha256_parts_verify_multi_part() {
        let key = b"multi-key";
        let a: &[u8] = b"part-a";
        let b_part: &[u8] = b"part-b";
        let c: &[u8] = b"part-c";

        // Match: parts_verify against the correct multi-part MAC.
        let correct_sig = hmac_sha256_parts(key, &[a, b_part, c]);
        assert!(
            hmac_sha256_parts_verify(key, &[a, b_part, c], &correct_sig),
            "must return true on multi-part match"
        );

        // Mismatch: wrong key.
        let wrong_sig = hmac_sha256_parts(b"wrong-key", &[a, b_part, c]);
        assert!(
            !hmac_sha256_parts_verify(key, &[a, b_part, c], &wrong_sig),
            "must return false with wrong key"
        );

        // Length guard: sig shorter than 32 bytes.
        let short_sig = &correct_sig[..16];
        assert!(
            !hmac_sha256_parts_verify(key, &[a, b_part, c], short_sig),
            "must return false for sig shorter than 32 bytes"
        );

        // Length guard: sig longer than 32 bytes.
        let mut long_sig = correct_sig.to_vec();
        long_sig.push(0x00);
        assert!(
            !hmac_sha256_parts_verify(key, &[a, b_part, c], &long_sig),
            "must return false for sig longer than 32 bytes"
        );
    }

    // -----------------------------------------------------------------------
    // credential_header_names tests
    // -----------------------------------------------------------------------

    #[test]
    fn credential_header_names_hmac_raw_body() {
        // Returns the signature header; key_id_header is not a credential.
        let ep = make_hmac_raw_body_endpoint("k1", b"secret", HexFormat::Hex, Some("x-key-id"));
        let names = ep.scheme.credential_header_names();
        assert_eq!(names.len(), 1);
        assert_eq!(names[0].as_str(), "x-sig");
    }

    #[test]
    fn credential_header_names_hmac_timestamped_body() {
        // Returns sig_header; timestamp_header is not a credential.
        let ep = make_hmac_timestamped_endpoint("k1", b"secret", "v0:{t}:{body}", 300);
        let names = ep.scheme.credential_header_names();
        assert_eq!(names.len(), 1);
        assert_eq!(names[0].as_str(), "x-sig");
    }

    #[test]
    fn credential_header_names_hmac_stripe() {
        // Returns the combined `t=…,v1=…` header (whole value is credential-bearing).
        let ep = make_stripe_endpoint("stripe-key", b"stripe-secret", 300);
        let names = ep.scheme.credential_header_names();
        assert_eq!(names.len(), 1);
        assert_eq!(names[0].as_str(), "stripe-signature");
    }

    #[test]
    fn credential_header_names_bearer_token() {
        // Returns the bearer header; token_id_header is not a credential.
        let ep = make_bearer_endpoint("t1", b"mysecret", Some("x-token-id"));
        let names = ep.scheme.credential_header_names();
        assert_eq!(names.len(), 1);
        assert_eq!(names[0].as_str(), "x-token");
    }
}
