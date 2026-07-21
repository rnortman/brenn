//! `PwaPushService::send()` — the pwa_push publish pipeline.
//!
//! Corresponds to design §2.7.3. Called from the `mcp__brenn__PushSend`
//! PostToolUse intercept in `brenn/src/pwa_push_intercept.rs` (via the
//! `PwaPushSender::send` trait method, which delegates here).
//!
//! Steps in brief:
//!   1. App gate.
//!   2. Body cap.
//!   3. Sender derivation.
//!   4. Resolve targets (ACL + subscription lookup).
//!   5. Plaintext size precheck (3993-byte cap, worst-case user_id width).
//!   6. Budget decrement.
//!   7. Persist message row (ensure_pwa_channel + insert_message_with_pushes).
//!   8. Release DB lock.
//!   9. Encrypt + POST per subscription.
//!  10. Record outcomes (201/410/404/other), update/delete rows.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;

use crate::auth::user::get_user_by_username_nocase;
use crate::messaging::db::{
    BudgetDecrement, decrement_send_budget, insert_message_with_pushes, utc_to_ns,
};
use crate::messaging::{ParticipantId, Urgency as MessagingUrgency};
use crate::pwa_push::db::{
    DeviceSubscriptionLookup, SubscriptionRow, delete_subscription_by_id, ensure_pwa_channel,
    list_subscriptions_current_user_only, list_subscriptions_for_user, lookup_device_subscription,
    touch_subscription,
};
use crate::pwa_push::payload::PushPayload;
use crate::pwa_push::targets::{PwaPushAddress, parse_pwa_push_address};

use super::delivery::{DeliveryOutcome, collect_with_publish_cap, publish_wide_cap};
use super::{MAX_TTL_SECONDS, PushSendResult, PwaPushService, Urgency};

impl PwaPushService {
    /// Execute a `PushSend` tool call.
    ///
    /// `sender_conversation_id` identifies the CC conversation that originated
    /// the call (used for budget bookkeeping). `sender_app_slug` identifies
    /// the app making the call (for gate checks and sender derivation).
    ///
    /// Receives `self: Arc<Self>` so the fanout spawn closures can hold an
    /// `Arc<PwaPushService>` clone without unsafe aliasing.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::pwa_push) async fn send_impl(
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
        // --- Step 1: App gate ---
        let app_config = match self.apps.get(sender_app_slug) {
            Some(a) => a,
            None => {
                tracing::warn!(app = sender_app_slug, "PushSend from unknown app");
                return PushSendResult::MissingSender;
            }
        };
        if !app_config.pwa_push_enabled() {
            return PushSendResult::MissingSender;
        }

        // --- Step 2: Body cap ---
        let max_body = self.defaults.max_body_bytes;
        if body.len() > max_body {
            return PushSendResult::BodyTooLarge {
                len: body.len(),
                max: max_body,
            };
        }

        // --- Step 3: Sender derivation ---
        // Host-derived from app slug + server origin. Informational only for
        // pwa_push, but must be the same structured identity as the bus path
        // (AC2) so that edit/cancel authz works across both publish paths.
        let sender = ParticipantId::for_app(sender_app_slug, &self.server_origin)
            .as_str()
            .to_owned();

        // --- Step 4: Parse and resolve target address ---
        let parsed_addr = match parse_pwa_push_address(address) {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!(address, error = %e, "PushSend: malformed pwa_push address");
                return PushSendResult::MalformedAddress(address.to_string());
            }
        };

        // Canonicalize the address and resolve the user_id in a single DB lock:
        // look up the stored `users` row via a case-insensitive username match and
        // rebuild the address from the DB's canonical casing. This prevents
        // `pwa_push:Alice` and `pwa_push:alice` from interning two different
        // `messaging_channels` rows for the same user.
        //
        // For the `User` fan-out variant we also capture the `user_id` here so the
        // subscription-resolve block below reuses it without a second `users` query.
        //
        // Unknown-user fallback: lowercase the username component to prevent
        // case-fragmentation in `messaging_channels` even when no user row exists.
        // Subscription lookups will return an empty vec (the address has no push
        // target), but the persisted channel address is at least deterministic.
        let (canonical_addr, canonical_user_id): (PwaPushAddress, Option<i64>) = {
            let conn = self.db.lock().await;
            match get_user_by_username_nocase(&conn, parsed_addr.user()) {
                Some(user_row) => {
                    let canonical = match &parsed_addr {
                        PwaPushAddress::User { .. } => PwaPushAddress::User {
                            user: user_row.username.clone(),
                        },
                        PwaPushAddress::Device { device, .. } => PwaPushAddress::Device {
                            user: user_row.username.clone(),
                            device: device.clone(),
                        },
                    };
                    let uid = match &canonical {
                        PwaPushAddress::User { .. } => Some(user_row.id),
                        PwaPushAddress::Device { .. } => None, // resolved via subscription lookup
                    };
                    (canonical, uid)
                }
                None => {
                    // No matching user row; lowercase the username for a deterministic
                    // channel address that won't fragment on case-variant sends.
                    let lower = parsed_addr.user().to_ascii_lowercase();
                    let fallback = match &parsed_addr {
                        PwaPushAddress::User { .. } => PwaPushAddress::User { user: lower },
                        PwaPushAddress::Device { device, .. } => PwaPushAddress::Device {
                            user: lower,
                            device: device.clone(),
                        },
                    };
                    (fallback, None)
                }
            }
        };

        // ACL check: run against the canonical username (DB-stored casing) so the
        // same identity is used for authorization and for all downstream operations
        // (subscription lookup, channel interning, audit log).
        let canonical_username = canonical_addr.user();
        if !app_config.user_has_access(canonical_username) {
            tracing::warn!(
                address,
                username = canonical_username,
                "PushSend: user not in app allowed_users"
            );
            return PushSendResult::Forbidden {
                address: address.to_string(),
            };
        }

        // User fan-out pre-fetch: list all subscriptions for this user so that the
        // current-user filter in the second match can count stale vs passing.
        // Device path skips this entirely — `lookup_device_subscription` below does
        // the existence + current-user check in a single query.
        // Unknown user (canonical_user_id == None) → None; treated as "no
        // subscriptions" to avoid leaking user existence.
        let user_subscriptions: Option<Vec<SubscriptionRow>> = match &canonical_addr {
            PwaPushAddress::User { .. } => {
                let conn = self.db.lock().await;
                Some(match canonical_user_id {
                    Some(id) => list_subscriptions_for_user(&conn, id),
                    None => vec![],
                })
            }
            PwaPushAddress::Device { .. } => None,
        };

        // --- Step 5: Plaintext size precheck ---
        // Hoisted before the current-user filter so that oversized bodies are
        // rejected (without consuming budget) regardless of subscription count.
        // An oversized body to a user with zero subscriptions must not silently
        // persist via finish_with_zero_subscriptions (errhandling-1 fix).
        // Build the payload template with worst-case user_id (i64::MAX = 19 digits).
        let notification_title = title
            .map(|t| t.to_string())
            .or_else(|| {
                app_config
                    .pwa_push
                    .as_ref()
                    .and_then(|p| p.default_title.clone())
            })
            .unwrap_or_else(|| app_config.name.clone());

        let payload_template = PushPayload {
            title: notification_title.clone(),
            body: body.to_string(),
            icon: None,
            badge: None,
            tag: tag.map(|s| s.to_string()),
            data: data.clone(),
            user_id: i64::MAX, // worst-case width for size check
        };

        let payload_bytes_template =
            serde_json::to_vec(&payload_template).expect("PushPayload serialization is infallible");
        const PLAINTEXT_CAP: usize = 3993;
        if payload_bytes_template.len() > PLAINTEXT_CAP {
            return PushSendResult::BodyTooLarge {
                len: payload_bytes_template.len(),
                max: PLAINTEXT_CAP,
            };
        }

        // For fan-out we need to know which subs pass the current-user check
        // so we can count failed_stale_user vs stale subs. For device-specific
        // lookups, lookup_device_subscription resolves existence and current-user
        // status in a single query. For user fan-out, we re-filter.
        let (passing, failed_stale_user_count) = match &canonical_addr {
            PwaPushAddress::Device { user, device } => {
                let conn = self.db.lock().await;
                match lookup_device_subscription(&conn, user, device) {
                    DeviceSubscriptionLookup::Current(row) => (vec![row], 0u32),
                    DeviceSubscriptionLookup::Stale => {
                        tracing::debug!(
                            user,
                            device,
                            "PushSend: device-targeted subscription exists but user is stale"
                        );
                        (vec![], 1u32)
                    }
                    DeviceSubscriptionLookup::NotFound => (vec![], 0u32),
                }
            }
            PwaPushAddress::User { user } => {
                // Filter against current-user list. We need to determine which
                // subs from the pre-fetched list also appear in the current-user list.
                // If there are no subscriptions, skip the DB lookup — the empty
                // passing vec falls through steps 6 and 7 naturally.
                let subscriptions =
                    user_subscriptions.expect("User arm: user_subscriptions must be Some");
                if subscriptions.is_empty() {
                    (vec![], 0u32)
                } else {
                    let user_id = subscriptions[0].user_id;
                    let current: Vec<SubscriptionRow> = {
                        let conn = self.db.lock().await;
                        list_subscriptions_current_user_only(&conn, user_id)
                    };
                    let current_ids: std::collections::HashSet<i64> =
                        current.iter().map(|s| s.id).collect();
                    let total = subscriptions.len() as u32;
                    let passing: Vec<SubscriptionRow> = subscriptions
                        .into_iter()
                        .filter(|s| current_ids.contains(&s.id))
                        .collect();
                    let stale = total - passing.len() as u32;
                    if stale > 0 {
                        tracing::debug!(user, stale, "PushSend: skipping stale-user subscriptions");
                    }
                    (passing, stale)
                }
            }
        };

        // --- Step 6: Budget decrement ---
        let remaining_budget = {
            let conn = self.db.lock().await;
            let send_budget = app_config.messaging_send_budget();
            let dec = decrement_send_budget(&conn, sender_conversation_id, send_budget);
            match dec {
                BudgetDecrement::Ok { remaining } => remaining,
                BudgetDecrement::Exhausted => return PushSendResult::BudgetExhausted,
            }
        };

        // --- Step 7: Persist message row ---
        let message_uuid = {
            let conn = self.db.lock().await;
            let channel_uuid = ensure_pwa_channel(&conn, &canonical_addr);
            let publish_ts_ns = utc_to_ns(Utc::now());
            let inserted = insert_message_with_pushes(
                &conn,
                channel_uuid,
                // source: use the server's own identifier (same as brenn: path)
                sender_app_slug,
                &sender,
                body,
                MessagingUrgency::Low, // pwa_push messages don't wake bus subscribers
                crate::messaging::ChannelScheme::Brenn,
                None, // no reply_to
                None, // no delivery_deadline
                None, // no deliver_after
                publish_ts_ns,
                &[], // no messaging_pending_pushes rows for pwa_push
            );
            inserted.uuid
        };
        // --- Step 8: DB lock released (dropped above) ---

        // --- Step 9: Concurrent fanout + POST ---
        let ttl = ttl_seconds.min(MAX_TTL_SECONDS);

        // Build endpoint-preview map before fanout; keeps task return tuples
        // narrow and avoids cloning endpoint strings into every task frame.
        let endpoint_previews: HashMap<i64, String> = passing
            .iter()
            .map(|s| (s.id, crate::pwa_push::endpoint_preview(&s.endpoint)))
            .collect();

        // Wrap shared immutable publish-scope values in Arc once; each spawn
        // clones the Arc (one atomic increment) rather than deep-copying strings
        // or JSON data N times across N subscriptions.
        let title_arc: Arc<str> = notification_title.as_str().into();
        let body_arc: Arc<str> = body.into();
        let tag_arc: Option<Arc<str>> = tag.map(Arc::from);
        let topic_arc: Option<Arc<str>> = topic.map(Arc::from);
        let data_arc: Arc<Option<serde_json::Map<String, serde_json::Value>>> = Arc::new(data);

        // Spawn one task per surviving subscription; collect with a
        // publish-wide outer timeout.
        //
        // `passing` is moved into the loop (not borrowed) so each SubscriptionRow
        // is moved into its task frame — zero per-sub string copies.
        let mut join_set: tokio::task::JoinSet<(i64, DeliveryOutcome)> =
            tokio::task::JoinSet::new();
        let mut id_to_sub: HashMap<tokio::task::Id, i64> = HashMap::new();

        for sub in passing {
            let svc = Arc::clone(&self);
            let title = Arc::clone(&title_arc);
            let body_arc = Arc::clone(&body_arc);
            let tag_arc = tag_arc.clone();
            let topic_arc = topic_arc.clone();
            let data_arc = Arc::clone(&data_arc);
            let sub_id = sub.id;

            let handle = join_set.spawn(async move {
                let outcome = svc
                    .deliver_to_subscription(
                        &sub,
                        &title,
                        &body_arc,
                        tag_arc.as_deref(),
                        &data_arc,
                        ttl,
                        urgency,
                        topic_arc.as_deref(),
                    )
                    .await;
                (sub_id, outcome)
            });
            id_to_sub.insert(handle.id(), sub_id);
        }

        let outcomes =
            collect_with_publish_cap(&mut join_set, &id_to_sub, message_uuid, publish_wide_cap())
                .await;

        // --- Step 10: Batch outcome write (single DB-lock acquisition) ---
        //
        // Collect Failed entries for logging *after* COMMIT so warn-log emissions
        // do not extend the SQLite write-lock window under tracing-subscriber
        // backpressure (efficiency-5).
        let mut delivered = 0u32;
        let mut gone = 0u32;
        let mut failed = 0u32;
        let mut failed_invalid_endpoint = 0u32;
        let mut failed_log_entries: Vec<(i64, String)> = Vec::new();

        {
            let conn = self.db.lock().await;
            conn.execute("BEGIN", []).expect("BEGIN");
            for (sub_id, outcome) in &outcomes {
                match outcome {
                    DeliveryOutcome::Delivered => {
                        delivered += 1;
                        touch_subscription(&conn, *sub_id);
                    }
                    DeliveryOutcome::Gone => {
                        gone += 1;
                        delete_subscription_by_id(&conn, *sub_id);
                    }
                    DeliveryOutcome::Failed(reason) => {
                        failed += 1;
                        failed_log_entries.push((*sub_id, reason.clone()));
                    }
                    DeliveryOutcome::InvalidEndpoint(_reason) => {
                        // Security event already fired inside deliver_to_subscription.
                        // Delete the row so this invalid endpoint stops being retried.
                        failed_invalid_endpoint += 1;
                        delete_subscription_by_id(&conn, *sub_id);
                    }
                }
            }
            conn.execute("COMMIT", []).expect("COMMIT");
            // conn guard dropped here, releasing the SQLite write lock before logging.
        }

        // Emit Failed warn-logs after COMMIT; keeps the write-lock window to pure SQL.
        for (sub_id, reason) in &failed_log_entries {
            tracing::warn!(
                subscription_id = *sub_id,
                endpoint = %endpoint_previews[sub_id],
                reason = %reason,
                "PushSend: delivery failed; subscription retained"
            );
        }

        PushSendResult::Ok {
            message_uuid,
            // Echo the caller's parsed address (not the DB-canonical form) to avoid
            // leaking whether a username exists under a different casing via the response.
            // The canonical form is used internally for channel interning and subscription
            // resolution; it must not be reflected to the sender.
            address: parsed_addr.to_canonical_string(),
            delivered,
            gone,
            failed,
            failed_stale_user: failed_stale_user_count,
            failed_invalid_endpoint,
            remaining_budget,
        }
    }
}
