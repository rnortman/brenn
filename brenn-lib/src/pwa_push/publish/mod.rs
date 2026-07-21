//! pwa_push publish service: public types, the `PwaPushSender` trait, and the
//! `PwaPushService` that implements it.
//!
//! The public API surface (`GetTargetResult`, `PushSendResult`,
//! `PushTargetEntry`, `MAX_TTL_SECONDS`, the `Urgency` re-export, plus the
//! `PwaPushSender` trait and `PwaPushService` struct) and the constructors live
//! here. The per-concern method bodies live in submodules:
//!   - `service`: the `list_targets` / `get_target` query methods + accessors.
//!   - `send`: the `send()` publish pipeline (design §2.7.3).
//!   - `delivery`: single-subscription delivery + fan-out collection.
//!   - `http_poster`: the `HttpPoster` trait + `ReqwestPoster`.

use std::sync::Arc;

use uuid::Uuid;

use crate::config::AppConfig;
use crate::db::Db;
use crate::messaging::MessagingGlobalConfig;
use crate::obs::alerting::AlertDispatcher;
use crate::pwa_push::config::ResolvedPwaPushConfig;
use crate::pwa_push::endpoint_validator::EndpointPolicy;
use crate::pwa_push::targets::PwaPushAddress;
use indexmap::IndexMap;

mod delivery;
mod http_poster;
mod send;
mod service;

pub(crate) use http_poster::{HttpPoster, ReqwestPoster};

/// TTL ceiling for RFC 8030 (28 days in seconds).
pub const MAX_TTL_SECONDS: u32 = 28 * 24 * 3600;

/// Outcome of `PwaPushService::get_target`.
#[derive(Debug)]
pub enum GetTargetResult {
    /// Target found; contains the resolved entry.
    Found(PushTargetEntry),
    /// App slug unknown, subscription absent, or user no longer top user on device.
    NotFound,
    /// User does not appear in the app's `allowed_users`.
    Forbidden,
    /// App does not hold the `PwaPush` grant (`pwa_push_enabled()` is false).
    Disabled,
}

/// Outcome of `PwaPushService::send`.
#[derive(Debug)]
pub enum PushSendResult {
    /// Send succeeded (possibly with 0 delivered endpoints).
    Ok {
        message_uuid: Uuid,
        address: String,
        /// Number of subscriptions that received a 201 response.
        delivered: u32,
        /// Number of subscriptions that returned 410 or 404 (deleted).
        gone: u32,
        /// Number of subscriptions that failed for other reasons.
        failed: u32,
        /// Number of subscriptions skipped because the subscriber is no
        /// longer the most-recently-seen user on their device.
        failed_stale_user: u32,
        /// Number of subscriptions whose endpoint failed SSRF validation
        /// at delivery time (row deleted; security event fired).
        failed_invalid_endpoint: u32,
        /// Budget units remaining after this call.
        remaining_budget: u32,
    },
    /// App does not hold the `PwaPush` grant (`pwa_push_enabled()` is false),
    /// or the app slug is unknown (routing/auth bug — the LLM supplied an app
    /// slug not in `self.apps`). Symmetric with
    /// `messaging::publish::PublishResult::MissingSender`.
    MissingSender,
    /// Body exceeds `messaging.max_body_bytes`.
    BodyTooLarge { len: usize, max: usize },
    /// Budget exhausted.
    BudgetExhausted,
    /// Target address was well-formed but the user is not in the app's `allowed_users`.
    Forbidden { address: String },
    /// Target address could not be parsed as a valid `pwa_push:` address.
    MalformedAddress(String),
}

/// Web push urgency level (RFC 8030 §5.3).
///
/// Re-exported from [`crate::messaging::Urgency`]; the canonical definition
/// lives there. `pwa_push` egress is a pass-through of the shared type —
/// no translation needed since both use the same RFC 8030 string values.
pub use crate::messaging::Urgency;

/// A single push target entry returned by `PwaPushService::list_targets`.
#[derive(Debug, serde::Serialize)]
pub struct PushTargetEntry {
    /// Canonical `pwa_push:` address for use with `PushSend`.
    pub address: String,
    /// Username of the target user.
    pub user: String,
    /// Device slug (`assigned_slug` if set, else `guessed_slug`). `None` for
    /// the fan-out `pwa_push:<u>` entry.
    pub device: Option<String>,
    /// ISO 8601 timestamp: `max(device.last_seen_at, subscription.last_used_at)`.
    pub last_seen_at: String,
}

/// Trait abstraction over the methods called through `AppState.pwa_push` and
/// `bridge.pwa_push_service()`.
///
/// `PwaPushService` implements this trait. Tests inject `MockPwaPushSender` to
/// capture arguments without making real HTTP calls. The inherent methods NOT on
/// this trait (`new`, `new_with_poster`) are constructors and are never called
/// through the trait object.
#[async_trait::async_trait]
pub trait PwaPushSender: Send + Sync {
    #[allow(clippy::too_many_arguments)]
    async fn send(
        self: Arc<Self>,
        sender_conversation_id: i64,
        sender_app_slug: &str,
        address: &str,
        body: &str,
        title: Option<&str>,
        ttl_seconds: u32,
        urgency: Urgency,
        topic: Option<&str>,
        tag: Option<&str>,
        data: Option<serde_json::Map<String, serde_json::Value>>,
    ) -> PushSendResult;

    async fn get_target(&self, app_slug: &str, parsed_addr: &PwaPushAddress) -> GetTargetResult;

    async fn list_targets(&self, app_slug: &str) -> Vec<PushTargetEntry>;

    /// Return the VAPID public key in base64url form for `PushVapidKeyRequest`.
    fn public_key_b64url(&self) -> &str;

    /// Return a reference to the endpoint validation policy.
    fn endpoint_policy(&self) -> &EndpointPolicy;
}

/// The pwa_push delivery service. Held on `AppState` as
/// `Option<Arc<dyn PwaPushSender>>`. `None` when no app holds the `PwaPush` grant.
pub struct PwaPushService {
    db: Db,
    config: ResolvedPwaPushConfig,
    apps: Arc<IndexMap<String, AppConfig>>,
    defaults: MessagingGlobalConfig,
    /// Server origin used to derive publisher identity (`app:<slug>@<server>`).
    /// Same value as the one fed to `messaging::resolve_source`.
    server_origin: Arc<str>,
    /// Shared HTTP poster for all outbound push requests.
    http_client: Arc<dyn HttpPoster>,
    /// Alert dispatcher for security events fired during delivery.
    alert_dispatcher: AlertDispatcher,
}

impl PwaPushService {
    pub fn new(
        db: Db,
        config: ResolvedPwaPushConfig,
        apps: Arc<IndexMap<String, AppConfig>>,
        defaults: MessagingGlobalConfig,
        server_origin: Arc<str>,
        alert_dispatcher: AlertDispatcher,
    ) -> Self {
        // Disable redirect following: RFC 8030 push services do not redirect on
        // POST; any redirect indicates misconfiguration or SSRF amplification
        // attempt. Reject 3xx as `Failed` (security-7).
        let http_client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("failed to build HTTP client for pwa_push");
        Self {
            db,
            config,
            apps,
            defaults,
            server_origin,
            http_client: Arc::new(ReqwestPoster {
                client: http_client,
            }),
            alert_dispatcher,
        }
    }

    /// Constructor accepting a custom `HttpPoster` — used by tests to inject
    /// controllable delays and failure modes.
    #[cfg(test)]
    pub(super) fn new_with_poster(
        db: Db,
        config: ResolvedPwaPushConfig,
        apps: Arc<IndexMap<String, AppConfig>>,
        defaults: MessagingGlobalConfig,
        server_origin: Arc<str>,
        alert_dispatcher: AlertDispatcher,
        poster: Arc<dyn HttpPoster>,
    ) -> Self {
        Self {
            db,
            config,
            apps,
            defaults,
            server_origin,
            http_client: poster,
            alert_dispatcher,
        }
    }
}

#[async_trait::async_trait]
impl PwaPushSender for PwaPushService {
    /// List push targets visible to the given app slug.
    async fn list_targets(&self, app_slug: &str) -> Vec<PushTargetEntry> {
        self.list_targets_impl(app_slug).await
    }

    /// Look up a single push target by parsed address.
    async fn get_target(&self, app_slug: &str, parsed_addr: &PwaPushAddress) -> GetTargetResult {
        self.get_target_impl(app_slug, parsed_addr).await
    }

    /// Execute a `PushSend` tool call.
    ///
    /// `sender_conversation_id` identifies the CC conversation that originated
    /// the call (used for budget bookkeeping). `sender_app_slug` identifies
    /// the app making the call (for gate checks and sender derivation).
    ///
    /// Receives `self: Arc<Self>` so the fanout spawn closures can hold an
    /// `Arc<PwaPushService>` clone without unsafe aliasing.
    #[allow(clippy::too_many_arguments)]
    async fn send(
        self: Arc<Self>,
        sender_conversation_id: i64,
        sender_app_slug: &str,
        address: &str,
        body: &str,
        title: Option<&str>,
        ttl_seconds: u32,
        urgency: Urgency,
        topic: Option<&str>,
        tag: Option<&str>,
        data: Option<serde_json::Map<String, serde_json::Value>>,
    ) -> PushSendResult {
        self.send_impl(
            sender_conversation_id,
            sender_app_slug,
            address,
            body,
            title,
            ttl_seconds,
            urgency,
            topic,
            tag,
            data,
        )
        .await
    }

    fn public_key_b64url(&self) -> &str {
        self.public_key_b64url_impl()
    }

    fn endpoint_policy(&self) -> &EndpointPolicy {
        self.endpoint_policy_impl()
    }
}

#[cfg(test)]
mod tests;
