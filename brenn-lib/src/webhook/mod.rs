//! Webhook transport for Brenn.
//!
//! Adds `webhook:` as a fourth peer transport alongside `brenn:` / `mqtt:` /
//! `pwa_push:`. External senders POST cryptographically authenticated requests
//! to per-endpoint HTTP routes; Brenn verifies the signature/bearer token and
//! publishes the raw body onto the messaging substrate.
//!
//! # Module layout
//!
//! - `address`   — `WebhookAddress` + `parse_webhook_address`.
//! - `error`     — `WebhookError` enum.
//! - `config`    — raw + resolved config types; `resolve_webhook_endpoints`.
//! - `signature` — `SignatureScheme` enum, `WebhookRejection`, `VerifiedRequest`,
//!   and the `verify_request` free function.
//! - `service`   — `WebhookService`, `WebhookEventRouter` trait, `EndpointView`.

// ---------------------------------------------------------------------------
// Shared charset validation
// ---------------------------------------------------------------------------

/// Validate that a key_id, token_id, or endpoint slug matches
/// `^[A-Za-z0-9._-]{1,64}$`.
///
/// Used by both `config` (at config-resolve time) and `signature` (at
/// request time when reading caller-supplied key_id headers). Single source of
/// truth so charset changes stay in sync.
pub fn is_valid_key_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-')
}

pub mod address;
pub mod config;
pub mod error;
pub mod service;
pub mod signature;

pub use address::{WEBHOOK_PREFIX, WebhookAddress, parse_webhook_address};
pub use config::{
    AppWebhookSubscriptionRaw, ResolvedWebhookEndpoint, ResolvedWebhookSubscription,
    WebhookEndpointConfigRaw, WebhookKeyConfigRaw, WebhookOwner, WebhookTokenConfigRaw,
    resolve_webhook_endpoints,
};
pub use error::WebhookError;
pub use service::{EndpointView, WebhookEventRouter, WebhookService};
pub use signature::{
    HexFormat, SignatureAlgorithm, SignatureScheme, VerifiedRequest, WebhookRejection,
};
