//! `WebhookEventRouterImpl` — inbound webhook delivery adapter.
//!
//! Implements `brenn_lib::webhook::WebhookEventRouter` against `AppState` + `Db`.
//! Uses the same deferred-state pattern as `MqttEventRouterImpl`:
//! the `AppState` is not yet constructed when the webhook service is created, so
//! we stash a `OnceCell<RouterState>` and call `set_state` after `AppState`
//! construction, before any webhook request can arrive.
//!
//! Delivery path (design §2.5):
//! 1. Build the `WebhookEnvelope` from the threaded HTTP metadata, masking
//!    credential-bearing header values using `SignatureScheme::credential_header_names`.
//! 2. Resolve the `webhook:<slug>` channel from the `MessagingDirectory`.
//! 3. Call `Messenger::publish_transport_ingress` — durably enqueues + dispatches
//!    to channel subscribers via the standard bus publish path.

use std::net::IpAddr;
use std::time::SystemTime;

use axum::http::HeaderMap;
use brenn_lib::messaging::{SubscriberEntryKind, Urgency, WEBHOOK_ADDRESS_PREFIX, WebhookEnvelope};
use brenn_lib::webhook::config::WebhookOwner;
use brenn_lib::webhook::service::WebhookEventRouter;
use brenn_lib::webhook::signature::SignatureScheme;
use tokio::sync::OnceCell;

use crate::state::AppState;

/// State bundle stored in `WebhookEventRouterImpl`'s inner `OnceCell`.
struct RouterState {
    app_state: AppState,
}

/// Concrete `WebhookEventRouter` impl. Closes over `AppState` (via `OnceCell`).
pub struct WebhookEventRouterImpl {
    inner: OnceCell<RouterState>,
}

impl WebhookEventRouterImpl {
    pub fn new() -> Self {
        Self {
            inner: OnceCell::new(),
        }
    }

    /// Fill in the `AppState`.
    /// Must be called before any webhook request can arrive.
    pub fn set_state(&self, state: AppState) {
        self.inner
            .set(RouterState { app_state: state })
            .map_err(|_| ())
            .expect("WebhookEventRouterImpl state already set");
    }
}

/// Build a `WebhookEnvelope` from the threaded HTTP metadata, applying
/// scheme-driven credential-header masking (design §2.2).
///
/// All headers from the `HeaderMap` are captured as ordered `(name, value)`
/// pairs (lowercased names, as axum exposes them). Header values for the
/// scheme's credential-bearing header names are replaced with `"[redacted]"`.
/// Non-UTF-8 header values are skipped (they cannot appear in valid UTF-8
/// webhook payloads; the admission path already guards for replay-protected
/// endpoints; any non-UTF-8 value on a non-replay endpoint is treated as
/// non-existent rather than panicking here).
///
/// `received_at` is converted from `SystemTime` to `DateTime<Utc>`. A
/// `SystemTime` before UNIX_EPOCH panics — that is a host clock misconfiguration,
/// not a transient error.
fn build_webhook_envelope(
    endpoint_slug: &str,
    key_id: &str,
    headers: HeaderMap,
    client_ip: IpAddr,
    received_at: SystemTime,
    raw_body: String,
    scheme: &SignatureScheme,
) -> WebhookEnvelope {
    use chrono::{DateTime, Utc};

    let credential_names = scheme.credential_header_names();

    let mut skipped_non_utf8_headers: Vec<&str> = Vec::new();
    let headers_vec: Vec<(String, String)> = headers
        .iter()
        .filter_map(|(name, value)| {
            let name_str = name.as_str().to_owned();
            let value_str = match value.to_str() {
                Ok(s) => s.to_owned(),
                Err(_) => {
                    // Non-UTF-8 header value: skip but record for logging.
                    skipped_non_utf8_headers.push(name.as_str());
                    return None;
                }
            };
            // Mask credential-bearing header values.
            let masked = if credential_names
                .iter()
                .any(|cred| cred.as_str() == name.as_str())
            {
                "[redacted]".to_owned()
            } else {
                value_str
            };
            Some((name_str, masked))
        })
        .collect();

    if !skipped_non_utf8_headers.is_empty() {
        tracing::warn!(
            endpoint = %endpoint_slug,
            skipped_count = skipped_non_utf8_headers.len(),
            skipped_header_names = ?skipped_non_utf8_headers,
            "build_webhook_envelope: skipped non-UTF-8 header value(s); \
             these headers are absent from the stored envelope"
        );
    }

    let duration_since_epoch = received_at
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_else(|_| {
            panic!(
                "SystemTime before UNIX_EPOCH at envelope construction for \
                 endpoint={endpoint_slug} — clock misconfigured"
            )
        });
    let received_at_chrono = DateTime::<Utc>::from(std::time::UNIX_EPOCH + duration_since_epoch);

    WebhookEnvelope {
        headers: headers_vec,
        key_id: key_id.to_owned(),
        client_ip: client_ip.to_string(),
        received_at: received_at_chrono,
        body: raw_body,
        endpoint_slug: endpoint_slug.to_owned(),
    }
}

#[async_trait::async_trait]
impl WebhookEventRouter for WebhookEventRouterImpl {
    async fn deliver_inbound(
        &self,
        endpoint_slug: &str,
        owner: &WebhookOwner,
        key_id: &str,
        headers: HeaderMap,
        client_ip: IpAddr,
        received_at: SystemTime,
        raw_body: String,
        urgency: Urgency,
    ) -> Result<(), String> {
        // Returns Err only for invariant-violation cases (startup race, unknown owner,
        // missing messenger/channel) that require the HTTP layer to emit 5xx so the
        // sender can retry rather than treating the drop as success.
        //
        // Use `get()` rather than `expect()` to distinguish a startup race from
        // a configuration bug.
        let router_state = match self.inner.get() {
            Some(s) => s,
            None => {
                tracing::error!(
                    endpoint = endpoint_slug,
                    owner = %owner,
                    "webhook_router: AppState not yet initialized. \
                     This is a startup race; if it persists after the process is up, it is a bug."
                );
                return Err("AppState not yet initialized".to_string());
            }
        };
        let state = &router_state.app_state;

        // Guard (app owners): the owning app must exist in the apps map
        // (config-invariant). WASM owners have no app entry — their existence is
        // checked against the messaging directory after channel resolution below.
        if let WebhookOwner::App(app_slug) = owner
            && state.apps.get(app_slug.as_ref()).is_none()
        {
            let msg = format!(
                "webhook_router: unknown owning app '{app_slug}' for endpoint \
                 '{endpoint_slug}' — config-invariant violation; returning 500 to caller"
            );
            tracing::error!(endpoint = endpoint_slug, owner = %owner, "{msg}");
            return Err(msg);
        }

        // Resolve the endpoint to get its SignatureScheme for credential masking.
        // The endpoint must exist in the webhook service since the HTTP handler already
        // looked it up — panic if it's missing now (startup invariant violation).
        let endpoint_arc = state
            .webhook
            .as_ref()
            .unwrap_or_else(|| {
                panic!(
                    "webhook_router: WebhookService not present in AppState for endpoint \
                     '{endpoint_slug}' — startup invariant violated"
                )
            })
            .endpoint_by_slug(endpoint_slug)
            .unwrap_or_else(|| {
                panic!(
                    "webhook_router: endpoint '{endpoint_slug}' missing from WebhookService index \
                     at deliver_inbound time — routing invariant violated"
                )
            });

        // Build the WebhookEnvelope with credential-header masking (design §2.2).
        let envelope = build_webhook_envelope(
            endpoint_slug,
            key_id,
            headers,
            client_ip,
            received_at,
            raw_body,
            &endpoint_arc.scheme,
        );
        let envelope_json =
            serde_json::to_string(&envelope).expect("WebhookEnvelope serialization is infallible");

        // Resolve the webhook: channel from the messaging directory.
        // The channel must exist — it was derived from this endpoint at startup and upserted.
        // An unresolvable channel is a config-invariant violation: panic (fail-fast, §2.5).
        let channel_address = format!("{WEBHOOK_ADDRESS_PREFIX}{endpoint_slug}");
        let messenger = state.messenger.as_ref().unwrap_or_else(|| {
            panic!(
                "webhook_router: Messenger not present in AppState for endpoint \
                     '{endpoint_slug}' — startup invariant violated (messenger required when \
                     webhook endpoints exist)"
            )
        });
        let channel = messenger
            .directory()
            .resolve(&channel_address)
            .unwrap_or_else(|| {
                panic!(
                    "webhook_router: webhook channel '{channel_address}' not found in directory \
                     for endpoint '{endpoint_slug}' — channel must be derived at startup"
                )
            });

        // Guard (WASM owners): the resolved channel must carry a matching
        // `Wasm(<owner slug>)` subscriber. `AppState` holds no wasm-consumer map,
        // so the directory — populated at boot from the same config the owner was
        // derived from — is the authoritative runtime set. A miss is a
        // config-invariant violation (500, sender retries).
        if let WebhookOwner::Wasm(wasm_slug) = owner {
            let present = channel.subscribers.iter().any(|s| {
                matches!(&s.kind, SubscriberEntryKind::Wasm(slug) if slug == wasm_slug.as_ref())
            });
            if !present {
                let msg = format!(
                    "webhook_router: WASM owner '{wasm_slug}' for endpoint '{endpoint_slug}' \
                     is not a subscriber on channel '{channel_address}' — config-invariant \
                     violation; returning 500 to caller"
                );
                tracing::error!(endpoint = endpoint_slug, owner = %owner, "{msg}");
                return Err(msg);
            }
        }

        // source == "webhook:<slug>", sender == key_id (envelope identifier).
        let source = channel_address.clone();
        let sender = key_id;

        // Publish via the real bus publish path. This durably enqueues the message +
        // pending-push rows and dispatches to all channel subscribers (design §2.5).
        // `204` is returned by the HTTP handler only after this returns (durable enqueue
        // has completed). Never 204 if enqueue failed — the handler maps Err→500.
        messenger
            .publish_transport_ingress(channel, &source, sender, &envelope_json, urgency)
            .await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;
    use std::sync::Arc;
    use std::time::SystemTime;

    use axum::http::HeaderMap;
    use brenn_lib::messaging::{
        ChannelEntry, ChannelScheme, MessagingDirectory, SubscriberEntry, SubscriberEntryKind,
        Urgency, WEBHOOK_ADDRESS_PREFIX, WakeMin,
        config::{Depth, MessagingGlobalConfig, NoiseLevel, ResolvedChannel, Sink},
        db::upsert_channels,
        webhook_channel_uuid_from_slug,
    };
    use brenn_lib::webhook::config::ResolvedWebhookEndpoint;
    use brenn_lib::webhook::service::WebhookService;
    use brenn_lib::webhook::signature::{HexFormat, SignatureAlgorithm, SignatureScheme};
    use indexmap::IndexMap;

    use super::*;
    use crate::test_support::state::test_state_with_user_and_app;

    /// Build a `WebhookService` with a `HmacRawBody` endpoint that has the
    /// given `sig_header` as its credential header.
    fn test_webhook_svc_with_scheme(
        endpoint_slug: &str,
        app_slug: &str,
        scheme: SignatureScheme,
    ) -> Arc<WebhookService> {
        let endpoint = Arc::new(ResolvedWebhookEndpoint {
            slug: endpoint_slug.to_string(),
            mount: format!("/webhooks/{endpoint_slug}"),
            description: None,
            transport_ceiling_bytes: 1024 * 1024,
            content_type: "application/json".to_string(),
            scheme,
            owner: WebhookOwner::App(Arc::from(app_slug)),
            urgency: brenn_lib::messaging::Urgency::Normal,
            replay_protection: None,
        });
        WebhookService::new(vec![(endpoint_slug.to_string(), endpoint)])
    }

    /// Build a minimal `WebhookService` with a no-credential `HmacRawBody`
    /// endpoint for the given slug.
    fn test_webhook_svc(endpoint_slug: &str, app_slug: &str) -> Arc<WebhookService> {
        let mut keys = std::collections::HashMap::new();
        keys.insert("k1".to_string(), b"secret".to_vec());
        test_webhook_svc_with_scheme(
            endpoint_slug,
            app_slug,
            SignatureScheme::HmacRawBody {
                algorithm: SignatureAlgorithm::HmacSha256,
                header: "x-sig".parse().unwrap(),
                format: HexFormat::V1Hex,
                key_id_header: None,
                keys,
            },
        )
    }

    /// Build a `Messenger` with one `webhook:` channel derived from `endpoint_slug`.
    /// The channel has no subscribers (zero push targets — used for envelope-level tests).
    fn messenger_with_webhook_channel(
        db: brenn_lib::db::Db,
        endpoint_slug: &str,
    ) -> Arc<brenn_lib::messaging::Messenger> {
        let channel_uuid = webhook_channel_uuid_from_slug(endpoint_slug);
        let address = format!("{WEBHOOK_ADDRESS_PREFIX}{endpoint_slug}");
        let entry = ChannelEntry {
            uuid: channel_uuid,
            address: address.clone(),
            description: None,
            resolved_channel: ResolvedChannel {
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers: vec![],
            transport_type: ChannelScheme::Webhook,
            mount: Some(format!("/webhooks/{endpoint_slug}")),
        };
        {
            let conn = db.try_lock().expect("db lock for channel upsert");
            upsert_channels(&conn, std::slice::from_ref(&entry));
        }
        let directory = Arc::new(MessagingDirectory::with_entries(vec![entry]));
        brenn_lib::messaging::Messenger::new(
            db,
            directory,
            Arc::from("webhook-test"),
            Arc::new(IndexMap::new()),
            Arc::new(brenn_lib::messaging::query::NoopWakeRouter)
                as Arc<dyn brenn_lib::messaging::WakeRouter>,
            MessagingGlobalConfig::default(),
        )
    }

    /// Build a `Messenger` with one `webhook:` channel and one subscriber conversation.
    fn messenger_with_webhook_channel_and_subscriber(
        db: brenn_lib::db::Db,
        endpoint_slug: &str,
        app_slug: &str,
        allowed_users: Vec<String>,
    ) -> Arc<brenn_lib::messaging::Messenger> {
        use brenn_lib::messaging::config::{ResolvedMessagingConfig, ResolvedSubscription};

        let channel_uuid = webhook_channel_uuid_from_slug(endpoint_slug);
        let address = format!("{WEBHOOK_ADDRESS_PREFIX}{endpoint_slug}");
        let entry = ChannelEntry {
            uuid: channel_uuid,
            address: address.clone(),
            description: None,
            resolved_channel: ResolvedChannel {
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers: vec![SubscriberEntry {
                kind: SubscriberEntryKind::App(app_slug.to_string()),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: Some(WakeMin::Normal),
            }],
            transport_type: ChannelScheme::Webhook,
            mount: Some(format!("/webhooks/{endpoint_slug}")),
        };
        {
            let conn = db.try_lock().expect("db lock for channel upsert");
            upsert_channels(&conn, std::slice::from_ref(&entry));
        }
        let directory = Arc::new(MessagingDirectory::with_entries(vec![entry]));

        // Build apps map so resolve_push_targets can find the subscriber.
        let mut app_cfg =
            crate::test_support::app_config::default_test_app_config(app_slug, app_slug);
        app_cfg.allowed_users = allowed_users;
        // Delivery-time ACL gate (design §2.2 Point A): cover the webhook channel.
        app_cfg.policy =
            crate::test_support::app_config::delivery_policy_for_addresses([address.as_str()]);
        app_cfg.messaging = Some(ResolvedMessagingConfig {
            send_budget: 100,
            subscriptions: vec![ResolvedSubscription {
                channel_uuid,
                channel_address: address.clone(),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: WakeMin::Normal,
            }],
        });
        let mut apps_raw: IndexMap<String, brenn_lib::config::AppConfig> = IndexMap::new();
        apps_raw.insert(app_slug.to_string(), app_cfg);
        brenn_lib::messaging::Messenger::new(
            db,
            directory,
            Arc::from("webhook-test"),
            Arc::new(apps_raw),
            Arc::new(brenn_lib::messaging::query::NoopWakeRouter)
                as Arc<dyn brenn_lib::messaging::WakeRouter>,
            MessagingGlobalConfig::default(),
        )
    }

    /// Build a `Messenger` with one `webhook:` channel whose sole subscriber is a
    /// WASM consumer (`wasm:<wasm_slug>`), plus a `wasm_policies` entry for it.
    /// `covering` builds the policy from a `webhook_acl` naming the endpoint (which
    /// derives the grant and a covering matcher — the exact prod block shape), so the
    /// delivery gate admits it; `false` leaves `webhook_acl` empty so the gate denies
    /// at runtime (the operator-forgot-the-ACL case, exercised post-boot).
    fn messenger_with_webhook_channel_and_wasm_subscriber(
        db: brenn_lib::db::Db,
        endpoint_slug: &str,
        wasm_slug: &str,
        covering: bool,
    ) -> Arc<brenn_lib::messaging::Messenger> {
        let channel_uuid = webhook_channel_uuid_from_slug(endpoint_slug);
        let address = format!("{WEBHOOK_ADDRESS_PREFIX}{endpoint_slug}");
        let entry = crate::test_support::wasm::wasm_subscriber_channel_entry(
            channel_uuid,
            &address,
            ChannelScheme::Webhook,
            Some(format!("/webhooks/{endpoint_slug}")),
            wasm_slug,
        );

        // Derive the WASM consumer's delivery policy through the real
        // `build_wasm_policy` path so this exercises the identical grant/ACL math
        // that boot resolution runs for the prod `[[wasm_consumer]]` block.
        let webhook_acl: Vec<brenn_lib::access::raw::WebhookMatcherRaw> = if covering {
            vec![brenn_lib::access::raw::WebhookMatcherRaw {
                endpoint: endpoint_slug.to_string(),
            }]
        } else {
            vec![]
        };
        let policy = brenn_lib::access::resolve::build_wasm_policy(
            wasm_slug,
            [],
            brenn_lib::access::raw::WasmAclsRaw {
                webhook: &webhook_acl,
                ..Default::default()
            },
        );
        crate::test_support::wasm::messenger_with_wasm_policy(
            db,
            vec![entry],
            "webhook-test",
            wasm_slug,
            policy,
        )
    }

    /// End-to-end webhook receive for a WASM consumer: a real inbound webhook
    /// delivered through the router reaches the `wasm:` subscriber's push window
    /// when its policy carries a covering `webhook_acl`.
    #[tokio::test]
    async fn deliver_inbound_reaches_wasm_subscriber_when_covered() {
        let (mut state, db, _) = test_state_with_user_and_app("myapp", vec!["alice".to_string()]);
        state.messenger = Some(messenger_with_webhook_channel_and_wasm_subscriber(
            db.clone(),
            "push-alice",
            "consume-demo",
            true,
        ));
        state.webhook = Some(test_webhook_svc("push-alice", "myapp"));
        let router = WebhookEventRouterImpl::new();
        router.set_state(state);

        router
            .deliver_inbound(
                "push-alice",
                &WebhookOwner::App(Arc::from("myapp")),
                "primary",
                test_headers(),
                test_ip(),
                SystemTime::now(),
                "hello".to_string(),
                Urgency::Normal,
            )
            .await
            .expect("delivery should succeed");

        // The WASM consumer's push row is keyed on the `wasm:<slug>` participant,
        // and it carries the webhook envelope (envelope_type='webhook').
        let (push_count, envelope_type) =
            crate::test_support::wasm::wasm_push_rows(&db, "consume-demo").await;
        assert_eq!(
            push_count, 1,
            "the covered WASM consumer must receive exactly one push row"
        );
        assert_eq!(
            envelope_type.as_deref(),
            Some("webhook"),
            "the WASM consumer's push row must carry the webhook envelope"
        );
    }

    /// End-to-end webhook receive denial: with no covering `webhook_acl`, the
    /// delivery-time ACL gate denies the WASM consumer — no push row lands and a
    /// denial warn is emitted. Deny-by-default at the runtime gate (the boot half
    /// of the fail-fast posture panics; this is the post-boot runtime half).
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn deliver_inbound_denies_uncovered_wasm_subscriber() {
        let (mut state, db, _) = test_state_with_user_and_app("myapp", vec!["alice".to_string()]);
        state.messenger = Some(messenger_with_webhook_channel_and_wasm_subscriber(
            db.clone(),
            "push-alice",
            "consume-demo",
            false,
        ));
        state.webhook = Some(test_webhook_svc("push-alice", "myapp"));
        let router = WebhookEventRouterImpl::new();
        router.set_state(state);

        router
            .deliver_inbound(
                "push-alice",
                &WebhookOwner::App(Arc::from("myapp")),
                "primary",
                test_headers(),
                test_ip(),
                SystemTime::now(),
                "hello".to_string(),
                Urgency::Normal,
            )
            .await
            .expect("delivery itself succeeds; the subscriber is denied, not the ingress");

        let (push_count, _) = crate::test_support::wasm::wasm_push_rows(&db, "consume-demo").await;
        assert_eq!(
            push_count, 0,
            "an uncovered WASM consumer must receive no push row"
        );
        assert!(
            logs_contain("subscription delivery denied"),
            "the delivery gate must emit a denial warn for the uncovered subscriber"
        );
    }

    fn test_headers() -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("content-type", "application/json".parse().unwrap());
        h.insert("x-test-header", "test-value".parse().unwrap());
        h
    }

    fn test_ip() -> IpAddr {
        "127.0.0.1".parse().unwrap()
    }

    /// Unknown `owning_app_slug` returns `Err` (config-invariant violation).
    /// The HTTP handler returns 500 so the sender can retry.
    #[tokio::test]
    async fn unknown_app_slug_returns_err() {
        let (state, _db, _) = test_state_with_user_and_app("myapp", vec!["alice".to_string()]);
        let router = WebhookEventRouterImpl::new();
        router.set_state(state);

        let result = router
            .deliver_inbound(
                "ep",
                &WebhookOwner::App(Arc::from("no_such_app")),
                "k",
                test_headers(),
                test_ip(),
                SystemTime::now(),
                "x".to_string(),
                Urgency::Normal,
            )
            .await;

        assert!(
            result.is_err(),
            "unknown owning_app_slug must return Err so the HTTP handler returns 500"
        );
    }

    /// A `WebhookOwner::Wasm` owner whose slug matches a `Wasm(...)` subscriber
    /// on the resolved channel delivers successfully (the wasm-ownership guard
    /// passes) and the WASM consumer receives its push row.
    #[tokio::test]
    async fn wasm_owner_present_delivers() {
        let (mut state, db, _) = test_state_with_user_and_app("myapp", vec!["alice".to_string()]);
        state.messenger = Some(messenger_with_webhook_channel_and_wasm_subscriber(
            db.clone(),
            "push-alice",
            "consume-demo",
            true,
        ));
        state.webhook = Some(test_webhook_svc("push-alice", "myapp"));
        let router = WebhookEventRouterImpl::new();
        router.set_state(state);

        router
            .deliver_inbound(
                "push-alice",
                &WebhookOwner::Wasm(Arc::from("consume-demo")),
                "primary",
                test_headers(),
                test_ip(),
                SystemTime::now(),
                "hello".to_string(),
                Urgency::Normal,
            )
            .await
            .expect("delivery should succeed for a present wasm owner");

        let (push_count, _) = crate::test_support::wasm::wasm_push_rows(&db, "consume-demo").await;
        assert_eq!(
            push_count, 1,
            "the owning WASM consumer must receive exactly one push row"
        );
    }

    /// A `WebhookOwner::Wasm` owner whose slug is absent from the channel's
    /// subscriber set returns `Err` (config-invariant violation → 500). The
    /// channel here carries `consume-demo`, but the owner claims `ghost`.
    #[tokio::test]
    async fn wasm_owner_missing_from_subscribers_returns_err() {
        let (mut state, db, _) = test_state_with_user_and_app("myapp", vec!["alice".to_string()]);
        state.messenger = Some(messenger_with_webhook_channel_and_wasm_subscriber(
            db.clone(),
            "push-alice",
            "consume-demo",
            true,
        ));
        state.webhook = Some(test_webhook_svc("push-alice", "myapp"));
        let router = WebhookEventRouterImpl::new();
        router.set_state(state);

        let result = router
            .deliver_inbound(
                "push-alice",
                &WebhookOwner::Wasm(Arc::from("ghost")),
                "primary",
                test_headers(),
                test_ip(),
                SystemTime::now(),
                "hello".to_string(),
                Urgency::Normal,
            )
            .await;

        let err = result
            .expect_err("a wasm owner absent from the channel subscribers must return Err (500)");
        assert!(
            err.contains("is not a subscriber on channel"),
            "the Err must be the wasm-owner-guard rejection, not an unrelated \
             delivery failure; got: {err}"
        );
    }

    /// Delivery stores a `WebhookEnvelope` JSON body with `envelope_type='webhook'`
    /// and a non-NULL `channel_uuid` for the `webhook:<slug>` channel. Verifies
    /// that headers, key_id, client_ip, body, and endpoint_slug round-trip correctly.
    #[tokio::test]
    async fn deliver_inbound_stores_webhook_envelope_json() {
        let (mut state, db, _) = test_state_with_user_and_app("myapp", vec!["alice".to_string()]);
        state.messenger = Some(messenger_with_webhook_channel(db.clone(), "ep-test"));
        state.webhook = Some(test_webhook_svc("ep-test", "myapp"));
        let router = WebhookEventRouterImpl::new();
        router.set_state(state);

        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());
        headers.insert("x-custom-header", "custom-value".parse().unwrap());

        router
            .deliver_inbound(
                "ep-test",
                &WebhookOwner::App(Arc::from("myapp")),
                "primary",
                headers,
                "192.168.1.1".parse().unwrap(),
                SystemTime::now(),
                "raw-body-content".to_string(),
                Urgency::Normal,
            )
            .await
            .expect("delivery should succeed");

        // Fetch the inserted message from the unified store.
        let (envelope_type, body, channel_uuid_is_not_null): (String, String, bool) = {
            let conn = db.lock().await;
            conn.query_row(
                "SELECT envelope_type, body, channel_uuid IS NOT NULL \
                 FROM messaging_messages ORDER BY id DESC LIMIT 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get::<_, bool>(2)?)),
            )
            .expect("message must have been inserted")
        };

        assert_eq!(
            envelope_type, "webhook",
            "must be stored as envelope_type='webhook'"
        );
        assert!(
            channel_uuid_is_not_null,
            "webhook message must have a non-NULL channel_uuid"
        );

        let envelope: brenn_lib::messaging::WebhookEnvelope =
            serde_json::from_str(&body).expect("body must be a valid WebhookEnvelope JSON");

        assert_eq!(envelope.endpoint_slug, "ep-test");
        assert_eq!(envelope.key_id, "primary");
        assert_eq!(envelope.client_ip, "192.168.1.1");
        assert_eq!(envelope.body, "raw-body-content");
        let header_names: Vec<&str> = envelope.headers.iter().map(|(n, _)| n.as_str()).collect();
        assert!(
            header_names.contains(&"content-type"),
            "content-type header must be in envelope"
        );
        assert!(
            header_names.contains(&"x-custom-header"),
            "x-custom-header must be in envelope"
        );
    }

    /// Inbound webhook delivery inserts a pending-push row for the subscribing
    /// conversation (end-to-end channel publish path).
    #[tokio::test]
    async fn deliver_inbound_enqueues_pending_push_for_subscriber() {
        let (mut state, db, _user_id) =
            test_state_with_user_and_app("myapp", vec!["alice".to_string()]);
        state.messenger = Some(messenger_with_webhook_channel_and_subscriber(
            db.clone(),
            "ep-test",
            "myapp",
            vec!["alice".to_string()],
        ));
        state.webhook = Some(test_webhook_svc("ep-test", "myapp"));
        let router = WebhookEventRouterImpl::new();
        router.set_state(state);

        router
            .deliver_inbound(
                "ep-test",
                &WebhookOwner::App(Arc::from("myapp")),
                "primary",
                test_headers(),
                test_ip(),
                SystemTime::now(),
                "hello".to_string(),
                Urgency::Normal,
            )
            .await
            .expect("delivery should succeed");

        // Verify a pending-push row exists for the webhook channel message.
        let push_count: i64 = {
            let conn = db.lock().await;
            conn.query_row(
                "SELECT COUNT(*) FROM messaging_pending_pushes pp \
                 JOIN messaging_messages m ON pp.message_id = m.id \
                 WHERE m.envelope_type = 'webhook'",
                [],
                |r| r.get(0),
            )
            .expect("query must succeed")
        };
        assert_eq!(
            push_count, 1,
            "exactly one pending-push row for the subscriber"
        );
    }

    /// Credential-header redaction: the scheme-named credential header value
    /// is masked to "[redacted]" in the stored envelope; other headers survive verbatim.
    #[tokio::test]
    async fn credential_header_value_is_masked_in_envelope() {
        // Build a state with an HmacRawBody endpoint that has x-sig as credential header
        // and x-key-id as identifier header.
        let (mut state, db, _) = test_state_with_user_and_app("myapp", vec!["alice".to_string()]);
        state.messenger = Some(messenger_with_webhook_channel(db.clone(), "ep-test"));

        let mut keys = std::collections::HashMap::new();
        keys.insert("k1".to_string(), b"secret".to_vec());
        let endpoint = Arc::new(ResolvedWebhookEndpoint {
            slug: "ep-test".to_string(),
            mount: "/webhooks/ep-test".to_string(),
            description: None,
            transport_ceiling_bytes: 1024 * 1024,
            content_type: "application/json".to_string(),
            scheme: SignatureScheme::HmacRawBody {
                algorithm: SignatureAlgorithm::HmacSha256,
                header: "x-sig".parse().unwrap(),
                format: HexFormat::V1Hex,
                key_id_header: Some("x-key-id".parse().unwrap()),
                keys,
            },
            owner: WebhookOwner::App(Arc::from("myapp")),
            urgency: brenn_lib::messaging::Urgency::Normal,
            replay_protection: None,
        });
        state.webhook = Some(brenn_lib::webhook::service::WebhookService::new(vec![(
            "ep-test".to_string(),
            endpoint,
        )]));

        let router = WebhookEventRouterImpl::new();
        router.set_state(state);

        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());
        headers.insert("x-sig", "v1=abcdef1234".parse().unwrap()); // credential header
        headers.insert("x-key-id", "k1".parse().unwrap()); // identifier, NOT credential

        router
            .deliver_inbound(
                "ep-test",
                &WebhookOwner::App(Arc::from("myapp")),
                "k1",
                headers,
                "127.0.0.1".parse().unwrap(),
                SystemTime::now(),
                "{}".to_string(),
                Urgency::Normal,
            )
            .await
            .expect("delivery should succeed");

        let body: String = {
            let conn = db.lock().await;
            conn.query_row(
                "SELECT body FROM messaging_messages WHERE envelope_type='webhook' ORDER BY id DESC LIMIT 1",
                [],
                |r| r.get(0),
            )
            .expect("webhook message must have been inserted")
        };

        let envelope: brenn_lib::messaging::WebhookEnvelope =
            serde_json::from_str(&body).expect("must be valid WebhookEnvelope JSON");

        // x-sig is the credential header — its value must be masked.
        let sig_entry = envelope
            .headers
            .iter()
            .find(|(n, _)| n == "x-sig")
            .expect("x-sig header must be present in envelope");
        assert_eq!(
            sig_entry.1, "[redacted]",
            "credential header value must be masked"
        );

        // x-key-id is the identifier header — must survive verbatim.
        let kid_entry = envelope
            .headers
            .iter()
            .find(|(n, _)| n == "x-key-id")
            .expect("x-key-id header must be present in envelope");
        assert_eq!(
            kid_entry.1, "k1",
            "identifier header value must NOT be masked"
        );

        // key_id field itself is not masked.
        assert_eq!(envelope.key_id, "k1");
    }

    /// `publish_transport_ingress` bypasses the send-budget gate: even if the
    /// subscribing app has `send_budget = 0`, a webhook delivery still enqueues
    /// the message. Pins the invariant that host-originated ingress is never
    /// blocked by a CC-facing budget limit.
    #[tokio::test]
    async fn publish_transport_ingress_bypasses_send_budget() {
        let (mut state, db, _) = test_state_with_user_and_app("myapp", vec!["alice".to_string()]);

        // Build a messenger where the subscribing app has send_budget=0.
        let channel_uuid = webhook_channel_uuid_from_slug("ep-budget");
        let address = format!("{WEBHOOK_ADDRESS_PREFIX}ep-budget");
        let entry = ChannelEntry {
            uuid: channel_uuid,
            address: address.clone(),
            description: None,
            resolved_channel: ResolvedChannel {
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers: vec![SubscriberEntry {
                kind: SubscriberEntryKind::App("myapp".to_string()),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: Some(WakeMin::Normal),
            }],
            transport_type: ChannelScheme::Webhook,
            mount: Some("/webhooks/ep-budget".to_string()),
        };
        {
            let conn = db.try_lock().expect("db lock for channel upsert");
            upsert_channels(&conn, std::slice::from_ref(&entry));
        }
        let directory = Arc::new(MessagingDirectory::with_entries(vec![entry]));

        use brenn_lib::messaging::config::{ResolvedMessagingConfig, ResolvedSubscription};
        let mut app_cfg =
            crate::test_support::app_config::default_test_app_config("myapp", "myapp");
        app_cfg.allowed_users = vec!["alice".to_string()];
        // Delivery-time ACL gate (design §2.2 Point A): cover the webhook channel.
        app_cfg.policy =
            crate::test_support::app_config::delivery_policy_for_addresses([address.as_str()]);
        app_cfg.messaging = Some(ResolvedMessagingConfig {
            send_budget: 0, // zero budget — must not block host-originated ingress
            subscriptions: vec![ResolvedSubscription {
                channel_uuid,
                channel_address: address.clone(),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: WakeMin::Normal,
            }],
        });
        let mut apps_raw: IndexMap<String, brenn_lib::config::AppConfig> = IndexMap::new();
        apps_raw.insert("myapp".to_string(), app_cfg);
        let messenger = brenn_lib::messaging::Messenger::new(
            db.clone(),
            directory,
            Arc::from("webhook-test"),
            Arc::new(apps_raw),
            Arc::new(brenn_lib::messaging::query::NoopWakeRouter)
                as Arc<dyn brenn_lib::messaging::WakeRouter>,
            MessagingGlobalConfig::default(),
        );
        state.messenger = Some(messenger);
        state.webhook = Some(test_webhook_svc("ep-budget", "myapp"));

        let router = WebhookEventRouterImpl::new();
        router.set_state(state);

        router
            .deliver_inbound(
                "ep-budget",
                &WebhookOwner::App(Arc::from("myapp")),
                "k1",
                test_headers(),
                test_ip(),
                SystemTime::now(),
                "body".to_string(),
                Urgency::Normal,
            )
            .await
            .expect("delivery must succeed even with send_budget=0");

        // Verify the message was inserted despite zero budget.
        let count: i64 = {
            let conn = db.lock().await;
            conn.query_row(
                "SELECT COUNT(*) FROM messaging_messages WHERE envelope_type='webhook'",
                [],
                |r| r.get(0),
            )
            .expect("query must succeed")
        };
        assert_eq!(count, 1, "message must be enqueued even when send_budget=0");
    }

    /// BearerToken scheme: the bearer header value is masked; a non-credential
    /// header survives verbatim. Distinct from the HmacRawBody test above —
    /// `BearerToken::credential_header_names()` has its own implementation.
    #[tokio::test]
    async fn bearer_token_credential_header_masked_in_envelope() {
        let (mut state, db, _) = test_state_with_user_and_app("myapp", vec!["alice".to_string()]);
        state.messenger = Some(messenger_with_webhook_channel(db.clone(), "ep-bearer"));

        let mut tokens = std::collections::HashMap::new();
        tokens.insert("t1".to_string(), b"supersecret".to_vec());
        let endpoint = Arc::new(ResolvedWebhookEndpoint {
            slug: "ep-bearer".to_string(),
            mount: "/webhooks/ep-bearer".to_string(),
            description: None,
            transport_ceiling_bytes: 1024 * 1024,
            content_type: "application/json".to_string(),
            scheme: SignatureScheme::BearerToken {
                header: "authorization".parse().unwrap(),
                token_id_header: Some("x-token-id".parse().unwrap()),
                tokens,
            },
            owner: WebhookOwner::App(Arc::from("myapp")),
            urgency: brenn_lib::messaging::Urgency::Normal,
            replay_protection: None,
        });
        state.webhook = Some(brenn_lib::webhook::service::WebhookService::new(vec![(
            "ep-bearer".to_string(),
            endpoint,
        )]));

        let router = WebhookEventRouterImpl::new();
        router.set_state(state);

        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());
        headers.insert("authorization", "supersecret".parse().unwrap()); // credential header
        headers.insert("x-token-id", "t1".parse().unwrap()); // identifier, NOT credential

        router
            .deliver_inbound(
                "ep-bearer",
                &WebhookOwner::App(Arc::from("myapp")),
                "t1",
                headers,
                "127.0.0.1".parse().unwrap(),
                SystemTime::now(),
                "{}".to_string(),
                Urgency::Normal,
            )
            .await
            .expect("delivery should succeed");

        let body: String = {
            let conn = db.lock().await;
            conn.query_row(
                "SELECT body FROM messaging_messages \
                 WHERE envelope_type='webhook' ORDER BY id DESC LIMIT 1",
                [],
                |r| r.get(0),
            )
            .expect("webhook message must have been inserted")
        };

        let envelope: brenn_lib::messaging::WebhookEnvelope =
            serde_json::from_str(&body).expect("must be valid WebhookEnvelope JSON");

        // `authorization` is the credential header — its value must be masked.
        let auth_entry = envelope
            .headers
            .iter()
            .find(|(n, _)| n == "authorization")
            .expect("authorization header must be present in envelope");
        assert_eq!(
            auth_entry.1, "[redacted]",
            "bearer token value must be masked in envelope"
        );

        // `x-token-id` is the identifier header — must survive verbatim.
        let tid_entry = envelope
            .headers
            .iter()
            .find(|(n, _)| n == "x-token-id")
            .expect("x-token-id header must be present in envelope");
        assert_eq!(tid_entry.1, "t1", "identifier header must NOT be masked");
    }
}
