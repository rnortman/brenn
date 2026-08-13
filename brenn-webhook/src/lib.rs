//! Webhook transport runtime for Brenn.
//!
//! `webhook:` is the fourth peer transport alongside `brenn:` / `mqtt:` /
//! `pwa_push:`. External senders POST cryptographically authenticated requests
//! to per-endpoint HTTP routes; this crate verifies the signature or bearer
//! token and hands the raw body to the router the binary crate implements,
//! which publishes it onto the messaging substrate.
//!
//! # Module layout
//!
//! - `address`   — `WebhookAddress` + `parse_webhook_address`.
//! - `error`     — `WebhookError` enum.
//! - `signature` — `WebhookRejection`, `VerifiedRequest`, and the
//!   `verify_request` free function.
//! - `service`   — `WebhookService`, `WebhookEventRouter` trait, `EndpointView`.
//!
//! The configuration half — the raw and resolved config types, the
//! `SignatureScheme` those resolve into, and the `is_valid_key_id` charset —
//! stays in `brenn_lib::webhook`.

pub mod address;
pub mod error;
pub mod service;
pub mod signature;

pub use address::{WEBHOOK_PREFIX, WebhookAddress, parse_webhook_address};
pub use error::WebhookError;
pub use service::{EndpointView, WebhookEventRouter, WebhookService};
pub use signature::{VerifiedRequest, WebhookRejection, verify_request};
