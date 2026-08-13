//! PWA push delivery for Brenn.
//!
//! `pwa_push:` is a peer transport alongside `brenn:` / `mqtt:` / `webhook:`.
//! This crate holds everything on the delivery path: endpoint validation, the
//! subscription DAOs, address parsing, the service-worker payload, and the
//! publish service that encrypts and posts to the push services.
//!
//! # Module layout
//!
//! - `endpoint_validator` — SSRF-safe endpoint URL validation.
//! - `db`                 — subscribe- and publish-side subscription DAOs.
//! - `targets`            — `pwa_push:` address parsing.
//! - `payload`            — the JSON payload the service worker receives.
//! - `publish`            — `PwaPushService`, the fan-out and delivery pipeline.
//!
//! The configuration half — the raw and resolved config blocks, the
//! `EndpointPolicy`, and the VAPID keypair — stays in `brenn_lib::pwa_push`.

pub mod db;
pub mod endpoint_validator;
pub mod payload;
pub mod publish;
pub mod targets;

#[cfg(test)]
pub(crate) mod test_helpers;

pub use publish::{PwaPushSender, PwaPushService};

/// The first 16 chars of an endpoint URL, for log preview.
pub fn endpoint_preview(endpoint: &str) -> String {
    endpoint.chars().take(16).collect()
}
