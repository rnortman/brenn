//! Webhook configuration for Brenn.
//!
//! `webhook:` is a peer transport alongside `brenn:` / `mqtt:` / `pwa_push:`.
//! This module holds the raw config blocks, the resolved endpoint table
//! `BrennConfig`/`AppConfig` carry, and the signature schemes resolution
//! produces.
//!
//! # Module layout
//!
//! - `config` — raw + resolved config types; `resolve_webhook_endpoints`.
//! - `scheme` — `SignatureScheme` and its supporting enums, resolved from
//!   config.

// ---------------------------------------------------------------------------
// Shared charset validation
// ---------------------------------------------------------------------------

/// Validate that a key_id, token_id, or endpoint slug matches
/// `^[A-Za-z0-9._-]{1,64}$`.
///
/// Single source of truth for the key_id charset; callers at config-resolve
/// time and at request time must both use this.
pub fn is_valid_key_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-')
}

pub mod config;
pub mod scheme;

pub use config::{
    AppWebhookSubscriptionRaw, ResolvedWebhookEndpoint, ResolvedWebhookSubscription,
    WebhookEndpointConfigRaw, WebhookKeyConfigRaw, WebhookOwner, WebhookTokenConfigRaw,
    resolve_webhook_endpoints,
};
pub use scheme::{HexFormat, SignatureAlgorithm, SignatureScheme};
