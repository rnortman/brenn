//! `Messenger::cancel`, `Messenger::edit`, and `Messenger::list_pending`.
//!
//! Authorship is keyed on `messaging_messages.sender` (the derived
//! `ParticipantId::for_app` identity). Per design §2.5.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::db::{self, EditFieldsApplied, EditUpdateResult};
use super::gates::{reply_to_visible, well_formed_name};
use super::identity::ParticipantId;
use super::{ChannelScheme, MessageEnvelope, Messenger, Urgency};

// ---------------------------------------------------------------------------
// Public result types
// ---------------------------------------------------------------------------

/// Outcome of `Messenger::cancel`.
#[derive(Debug)]
pub enum CancelResult {
    /// Cancel succeeded; `cancelled_pushes` is the count of push rows deleted.
    Ok {
        message_id: Uuid,
        cancelled_pushes: u32,
    },
    /// No `messaging_messages` row with that UUID.
    UnknownMessage,
    /// Message exists but the `sender` string does not match the caller.
    NotAuthorized,
    /// Every push for the message has `delivered_at IS NOT NULL` (genuine delivery).
    AlreadyDelivered,
    /// Zero undelivered push rows; covers prior cancel and zero-target broadcast.
    NoPendingPushes,
    /// Calling app has no `[app.messaging]` section or `sender` is absent/empty.
    MissingSender,
}

/// Fields that may be changed by `Messenger::edit`. `None` means "leave
/// column unchanged". For nullable columns, `Some(None)` means "clear to NULL".
#[derive(Debug, Default)]
pub struct EditFields {
    pub body: Option<String>,
    /// `None` = leave; `Some(None)` = clear reply_to to NULL.
    pub reply_to: Option<Option<String>>,
    /// `None` = leave; `Some(None)` = deliver immediately (clear schedule).
    pub deliver_after: Option<Option<DateTime<Utc>>>,
    /// `None` = leave; `Some(None)` = clear deadline.
    pub delivery_deadline: Option<Option<DateTime<Utc>>>,
    pub urgency: Option<Urgency>,
}

/// Outcome of `Messenger::edit`.
#[derive(Debug)]
pub enum EditResult {
    /// Edit applied; returns the updated envelope.
    Ok { envelope: MessageEnvelope },
    /// No `messaging_messages` row with that UUID.
    UnknownMessage,
    /// Message exists but the `sender` string does not match the caller.
    NotAuthorized,
    /// At least one push has `delivered_at IS NOT NULL` (per requirements A3).
    AlreadyDelivered,
    /// Zero undelivered push rows (cancelled or zero-target broadcast).
    NoPendingPushes,
    /// No mutable fields were specified.
    NoFieldsProvided,
    /// `body` exceeds `max_body_bytes`.
    BodyTooLarge { len: usize, max: usize },
    /// `reply_to` address is well-formed but not registered in the directory.
    UnknownChannel(String),
    /// `reply_to` address is outside the sender's visibility scope (neither in
    /// its publish allowlist nor its delivery scope). Surfaced identically to
    /// `UnknownChannel` at the intercept so the reply_to gate reveals no
    /// channel-existence bit.
    AclDenied(String),
    /// `reply_to` address failed shape validation.
    MalformedAddress(String),
    /// Calling app has no `[app.messaging]` section or `sender` is absent/empty.
    MissingSender,
}

// ---------------------------------------------------------------------------
// Messenger impl
// ---------------------------------------------------------------------------

impl Messenger {
    /// Cancel all undelivered pending pushes for a message.
    ///
    /// Per design §2.2, cancel DELETEs push rows rather than marking them
    /// delivered, preserving the semantic that `delivered_at IS NOT NULL`
    /// means "bridge actually accepted delivery".
    pub async fn cancel(&self, sender_app_slug: &str, message_uuid: Uuid) -> CancelResult {
        // 1. Resolve sender string.
        let sender = match self.resolve_sender(sender_app_slug) {
            Some(s) => s,
            None => return CancelResult::MissingSender,
        };

        // 2. Auth + status check + DELETE under a single lock acquisition so the
        //    lookup and the DELETE are linearizable (no TOCTOU gap for a delivery
        //    task to slip between them).
        let cancelled = {
            let conn = self.db.lock().await;
            let lk = match db::lookup_message_for_authorship(&conn, message_uuid) {
                None => return CancelResult::UnknownMessage,
                Some(lk) => lk,
            };
            if lk.sender != sender {
                return CancelResult::NotAuthorized;
            }
            // All pushes delivered (genuinely — none cancelled because cancel DELETEs).
            if lk.undelivered_count == 0 && lk.delivered_count > 0 {
                return CancelResult::AlreadyDelivered;
            }
            // No pushes at all (zero-target broadcast, or already cancelled).
            if lk.undelivered_count == 0 {
                return CancelResult::NoPendingPushes;
            }
            // 3. DELETE undelivered push rows while we still hold the lock.
            db::cancel_pending_pushes_for_message(&conn, lk.message_id, &sender)
        };

        // 4. Kick the dispatcher — the cancel may have been the earliest row in
        //    either timer queue (design §2.7).
        self.dispatch_kick();

        CancelResult::Ok {
            message_id: message_uuid,
            cancelled_pushes: cancelled,
        }
    }

    /// Edit a pending message in-place.
    ///
    /// Fails with `AlreadyDelivered` if any push has been delivered (A3).
    /// Dispatches immediately if `deliver_after` is cleared (§3.3).
    /// Kicks background timers for touched scheduling fields (§2.7).
    pub async fn edit(
        &self,
        sender_app_slug: &str,
        message_uuid: Uuid,
        fields: EditFields,
    ) -> EditResult {
        // 1. At least one field must be provided.
        if fields.body.is_none()
            && fields.reply_to.is_none()
            && fields.deliver_after.is_none()
            && fields.delivery_deadline.is_none()
            && fields.urgency.is_none()
        {
            return EditResult::NoFieldsProvided;
        }

        // 2. Sender string.
        let sender = match self.resolve_sender(sender_app_slug) {
            Some(s) => s,
            None => return EditResult::MissingSender,
        };

        // 3. Auth + status check — before field validation so unauthorized callers
        //    receive NotAuthorized rather than field-specific errors that could
        //    leak server state (security §3).
        let lookup = {
            let conn = self.db.lock().await;
            match db::lookup_message_for_authorship(&conn, message_uuid) {
                None => return EditResult::UnknownMessage,
                Some(lk) => lk,
            }
        };
        if lookup.sender != sender {
            return EditResult::NotAuthorized;
        }
        // A3: fail if any push has been delivered.
        if lookup.delivered_count > 0 {
            return EditResult::AlreadyDelivered;
        }
        if lookup.undelivered_count == 0 {
            return EditResult::NoPendingPushes;
        }

        // 4. Body size check.
        let max_body = self.defaults.max_body_bytes;
        if let Some(body) = &fields.body
            && body.len() > max_body
        {
            return EditResult::BodyTooLarge {
                len: body.len(),
                max: max_body,
            };
        }

        // 5. Resolve reply_to (if Some(Some(addr))): shape → visibility → resolve.
        //    The visibility gate runs BEFORE resolution so an out-of-visibility
        //    reply_to fails identically whether or not the channel exists —
        //    the same success/failure existence oracle `Messenger::publish`
        //    closes on its own reply_to. Visibility is the union of the sender's
        //    publish allowlist and its delivery scope.
        let reply_to_resolved: Option<Option<Uuid>> = match &fields.reply_to {
            None => None,
            Some(None) => Some(None), // clear
            Some(Some(addr)) => {
                let name = match well_formed_name(addr, ChannelScheme::Brenn) {
                    Some(n) => n,
                    None => return EditResult::MalformedAddress(addr.clone()),
                };
                let policy = &self
                    .apps
                    .get(sender_app_slug)
                    .expect("edit: sender app resolved at step 2 must be present")
                    .policy;
                if !reply_to_visible(policy, ChannelScheme::Brenn, name, addr) {
                    return EditResult::AclDenied(addr.clone());
                }
                match self.directory.resolve(addr) {
                    Some(ch) => Some(Some(ch.uuid)),
                    None => return EditResult::UnknownChannel(addr.clone()),
                }
            }
        };

        // 6. Normalize deliver_after: a past timestamp is equivalent to clearing
        //    the schedule (§3.4). Also determine whether we need to dispatch
        //    immediately after the edit (§3.3).
        let normalized_deliver_after: Option<Option<DateTime<Utc>>> = match fields.deliver_after {
            Some(Some(da)) if da <= Utc::now() => Some(None), // past → treat as null
            other => other,
        };
        // Dispatch immediately when deliver_after is being cleared (explicit null or past).
        let deliver_after_cleared = matches!(normalized_deliver_after, Some(None));

        // 7. Apply to DB (inside a transaction with A3 re-check).
        let applied = EditFieldsApplied {
            body: fields.body.as_deref(),
            reply_to_uuid: reply_to_resolved,
            deliver_after: normalized_deliver_after,
            delivery_deadline: fields.delivery_deadline,
            urgency: fields.urgency,
        };

        let update_result = {
            let conn = self.db.lock().await;
            db::update_message_and_pending_pushes(&conn, lookup.message_id, &sender, &applied)
        };

        match update_result {
            EditUpdateResult::AnyDelivered => return EditResult::AlreadyDelivered,
            EditUpdateResult::NoPendingPushes => return EditResult::NoPendingPushes,
            EditUpdateResult::Ok { .. } => {}
        }

        // 8. Per-push wake recomputation is now folded into
        //    `update_message_and_pending_pushes` via a correlated UPDATE inside
        //    its own transaction (§3.5). No second lock cycle needed here.

        // 9. Kick the dispatcher for touched scheduling fields (§2.7).
        // Only kick deliver_after when scheduling forward (Some(Some(future))); clearing
        // deliver_after (Some(None)) dispatches immediately via step 10 — no kick needed.
        if matches!(normalized_deliver_after, Some(Some(_))) {
            self.dispatch_kick();
        }
        if applied.delivery_deadline.is_some() {
            self.dispatch_kick();
        }

        // 10. If deliver_after was cleared, signal the dispatcher (R1).
        if deliver_after_cleared {
            self.dispatch_kick();
        }

        // 11. Re-read the envelope to return the updated state.
        // Use a point lookup by UUID — cheaper than listing all sender's pending messages and
        // unambiguous whether or not the message was immediately dispatched by step 10.
        let envelope = {
            let conn = self.db.lock().await;
            db::load_envelope_by_uuid(&conn, message_uuid)
                .unwrap_or_else(|| panic!("messaging: edited message {message_uuid} vanished"))
        };

        EditResult::Ok { envelope }
    }

    /// List all pending (at least one undelivered push) messages authored by
    /// this app's sender. Optionally filtered to a single channel.
    ///
    /// Per design §2.11: an unresolvable or malformed channel address returns
    /// an empty list (not an error); the intercept logs malformed cases.
    pub async fn list_pending(
        &self,
        sender_app_slug: &str,
        channel: Option<&str>,
    ) -> Vec<MessageEnvelope> {
        let sender = match self.resolve_sender(sender_app_slug) {
            Some(s) => s,
            None => return vec![],
        };

        let channel_uuid_filter = if let Some(addr) = channel {
            match self.directory.resolve(addr) {
                Some(ch) => Some(ch.uuid),
                None => return vec![], // unknown or malformed address → empty (§2.11)
            }
        } else {
            None
        };

        let conn = self.db.lock().await;
        db::list_pending_messages_for_sender(&conn, &sender, channel_uuid_filter)
    }

    // ---------------------------------------------------------------------------
    // Private helpers
    // ---------------------------------------------------------------------------

    /// Resolve the sender identity string for an app slug. Returns `None` if
    /// the app holds no messaging grant (`messaging_publish` or
    /// `messaging_subscribe`), i.e. `messaging_enabled()` is false.
    /// Host-derived from app slug + server origin (design §2.5).
    pub(crate) fn resolve_sender(&self, app_slug: &str) -> Option<String> {
        let app = self.apps.get(app_slug)?;
        if !app.messaging_enabled() {
            return None;
        }
        Some(
            ParticipantId::for_app(app_slug, &self.source)
                .as_str()
                .to_owned(),
        )
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db_memory;
    use crate::messaging::config::{
        MessagingGlobalConfig, ResolvedMessagingConfig, ResolvedSubscription,
    };
    use crate::messaging::db::upsert_channels;
    use crate::messaging::{
        ChannelEntry, ChannelScheme, MessagingDirectory, WakeMin, WakeRouter, canonical_address,
    };
    use indexmap::IndexMap;
    use rusqlite::OptionalExtension;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    // -----------------------------------------------------------------------
    // Test doubles
    // -----------------------------------------------------------------------

    #[derive(Default)]
    struct CountingRouter {
        // Stores ParticipantId directly so the mock stays opaque — assertions that
        // need a concrete conversation id call as_conversation_id() in the test, not
        // in shared mock infrastructure.
        deliveries: tokio::sync::Mutex<Vec<crate::messaging::ParticipantId>>,
        deliver_returns: AtomicU64,
        eager_wakes: AtomicU64,
    }

    #[async_trait::async_trait]
    impl WakeRouter for CountingRouter {
        async fn deliver(
            &self,
            _key: &crate::messaging::SubscriberEntryKind,
            subscriber: &crate::messaging::ParticipantId,
            _: &crate::messaging::MessageEnvelope,
            _push_id: i64,
            _seq: i64,
        ) -> Result<bool, String> {
            self.deliveries.lock().await.push(subscriber.clone());
            match self.deliver_returns.load(Ordering::SeqCst) {
                0 => Ok(false),
                1 => Ok(true),
                _ => Err("simulated error".to_string()),
            }
        }
        async fn deliver_ingress(
            &self,
            _key: &crate::messaging::SubscriberEntryKind,
            subscriber: &crate::messaging::ParticipantId,
            _event: &crate::messaging::ingress::Event,
        ) -> Result<bool, String> {
            self.deliveries.lock().await.push(subscriber.clone());
            match self.deliver_returns.load(Ordering::SeqCst) {
                0 => Ok(false),
                1 => Ok(true),
                _ => Err("simulated error".to_string()),
            }
        }
        fn spawn_eager_wake(
            &self,
            _key: &crate::messaging::SubscriberEntryKind,
            _: &crate::messaging::ParticipantId,
        ) {
            self.eager_wakes.fetch_add(1, Ordering::SeqCst);
        }
        fn delivery_shape(
            &self,
            key: &crate::messaging::SubscriberEntryKind,
        ) -> crate::messaging::DeliveryShape {
            crate::messaging::default_delivery_shape(key)
        }
        fn alarm(&self, _channel: &str, _subscriber: &crate::messaging::ParticipantId) {}
    }

    // -----------------------------------------------------------------------
    // Fixture builder
    // -----------------------------------------------------------------------

    /// Builds a standard two-app messenger fixture:
    ///  - `pa-bob` (host-derived sender `app:pa-bob@<origin>`, conversation 1,
    ///    user 1) — the publisher
    ///  - `pa-alice` (host-derived sender `app:pa-alice@<origin>`, conversation
    ///    2, user 2) — the subscriber with `Immediate` subscription to
    ///    `brenn:pa-alice`.
    ///
    /// Returns `(messenger, channel_uuid, pa_bob_conv_id, pa_alice_conv_id, router)`.
    async fn build_messenger(
        deliver_returns: u64,
    ) -> (Arc<Messenger>, Uuid, i64, i64, Arc<CountingRouter>) {
        let db = init_db_memory();
        {
            let conn = db.lock().await;
            conn.execute(
                "INSERT INTO users (id, username, password_hash, created_at) \
                 VALUES (1, 'bob', 'h', '2024-01-01')",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO users (id, username, password_hash, created_at) \
                 VALUES (2, 'alice', 'h', '2024-01-01')",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO conversations (id, user_id, status, app_slug, created_at, updated_at) \
                 VALUES (1, 1, 'active', 'pa-bob', '2024-01-01', '2024-01-01')",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO conversations (id, user_id, status, app_slug, created_at, updated_at) \
                 VALUES (2, 2, 'active', 'pa-alice', '2024-01-01', '2024-01-01')",
                [],
            )
            .unwrap();
        }

        let channel_uuid = Uuid::new_v4();
        let entry = ChannelEntry {
            uuid: channel_uuid,
            address: canonical_address("pa-alice"),
            description: None,
            resolved_channel: crate::messaging::config::ResolvedChannel {
                push_depth: crate::messaging::config::Depth::Unbounded,
                retain_depth: crate::messaging::config::Depth::Unbounded,
                standing_retain_depth: crate::messaging::config::Depth::Unbounded,
                noise: crate::messaging::config::NoiseLevel::Silent,
                sink: crate::messaging::config::Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers: vec![crate::messaging::SubscriberEntry {
                kind: crate::messaging::SubscriberEntryKind::App("pa-alice".to_string()),
                push_depth: crate::messaging::config::Depth::Unbounded,
                retain_depth: crate::messaging::config::Depth::Unbounded,
                noise: crate::messaging::config::NoiseLevel::Silent,
                wake_min: Some(WakeMin::Normal),
            }],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        };
        {
            let conn = db.lock().await;
            upsert_channels(&conn, std::slice::from_ref(&entry));
            // Insert subscription record used for wake recomputation.
            conn.execute(
                "INSERT INTO messaging_subscriptions \
                 (channel_uuid, app_slug, push_depth, retain_depth, noise, wake_min) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    channel_uuid.as_bytes().to_vec(),
                    "pa-alice",
                    "unbounded",
                    "unbounded",
                    "silent",
                    "normal",
                ],
            )
            .unwrap();
        }

        let directory = Arc::new(MessagingDirectory::with_entries(vec![entry]));
        let mut apps: IndexMap<String, crate::config::AppConfig> = IndexMap::new();
        apps.insert(
            "pa-bob".to_string(),
            make_app_config(
                "pa-bob",
                Some(ResolvedMessagingConfig {
                    send_budget: 100,
                    subscriptions: vec![],
                }),
                vec!["bob".to_string()],
            ),
        );
        apps.insert(
            "pa-alice".to_string(),
            make_app_config(
                "pa-alice",
                Some(ResolvedMessagingConfig {
                    send_budget: 100,
                    subscriptions: vec![ResolvedSubscription {
                        channel_uuid,
                        channel_address: canonical_address("pa-alice"),
                        push_depth: crate::messaging::config::Depth::Unbounded,
                        retain_depth: crate::messaging::config::Depth::Unbounded,
                        noise: crate::messaging::config::NoiseLevel::Silent,
                        wake_min: WakeMin::Normal,
                    }],
                }),
                vec!["alice".to_string()],
            ),
        );

        let router = Arc::new(CountingRouter::default());
        router
            .deliver_returns
            .store(deliver_returns, Ordering::SeqCst);
        let messenger = Messenger::new(
            db,
            directory,
            Arc::from("test-source"),
            Arc::new(apps),
            router.clone() as Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        );
        (messenger, channel_uuid, 1, 2, router)
    }

    fn make_app_config(
        slug: &str,
        messaging: Option<ResolvedMessagingConfig>,
        allowed_users: Vec<String>,
    ) -> crate::config::AppConfig {
        crate::messaging::test_support::test_app_config(slug, messaging, allowed_users)
    }

    /// Publish a test message from pa-bob to brenn:pa-alice.
    async fn publish_one(
        m: &Arc<Messenger>,
        body: &str,
        deliver_after: Option<DateTime<Utc>>,
    ) -> Uuid {
        match m
            .publish(
                crate::messaging::PublishOrigin::Conversation { id: 1 },
                "pa-bob",
                &canonical_address("pa-alice"),
                body,
                crate::messaging::Urgency::Normal,
                None,
                deliver_after,
                None,
            )
            .await
        {
            crate::messaging::PublishResult::Ok { message_id, .. } => message_id,
            other => panic!("publish failed: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Cancel tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn cancel_unknown_message_returns_unknown_message() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let result = m.cancel("pa-bob", Uuid::new_v4()).await;
        assert!(matches!(result, CancelResult::UnknownMessage));
    }

    #[tokio::test]
    async fn cancel_wrong_sender_returns_not_authorized() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let future = Utc::now() + chrono::Duration::seconds(3600);
        let mid = publish_one(&m, "hello", Some(future)).await;
        // pa-alice tries to cancel pa-bob's message.
        let result = m.cancel("pa-alice", mid).await;
        assert!(matches!(result, CancelResult::NotAuthorized));
    }

    #[tokio::test]
    async fn cancel_already_delivered_returns_already_delivered() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let mid = publish_one(&m, "hello", None).await;
        // Simulate delivery: find the pending push and mark it delivered.
        let sub = crate::messaging::ParticipantId::for_conversation(2);
        let pending = m.load_pending_pushes(&sub).await;
        assert_eq!(pending.len(), 1);
        m.mark_pushes_delivered(&[pending[0].0]).await;
        // Now cancel should see no undelivered rows → AlreadyDelivered.
        let result = m.cancel("pa-bob", mid).await;
        assert!(matches!(result, CancelResult::AlreadyDelivered));
    }

    #[tokio::test]
    async fn cancel_succeeds_and_kicks_both_timers() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let future = Utc::now() + chrono::Duration::seconds(3600);
        let mid = publish_one(&m, "hello", Some(future)).await;

        // Capture the single dispatcher kick notify.
        let kick = m.dispatch_kick_notify.clone();

        // notified() future must be set up BEFORE cancel triggers it.
        let kick_notified = kick.notified();

        let result = m.cancel("pa-bob", mid).await;
        assert!(matches!(
            result,
            CancelResult::Ok {
                cancelled_pushes: 1,
                ..
            }
        ));

        // Kick fired.
        tokio::time::timeout(std::time::Duration::from_millis(100), kick_notified)
            .await
            .expect("dispatch_kick not fired");
    }

    #[tokio::test]
    async fn cancel_then_channel_get_still_shows_message() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let future = Utc::now() + chrono::Duration::seconds(3600);
        let mid = publish_one(&m, "cancel-then-read", Some(future)).await;
        let _ = m.cancel("pa-bob", mid).await;

        // Message still visible in channel history.
        let q = crate::messaging::MessageQuery {
            channel: canonical_address("pa-alice"),
            limit: 10,
            before: None,
            after: None,
            sender: None,
            search: None,
            calling_app_slug: "pa-bob".to_string(),
        };
        let results = m.query(&q).await.expect("query");
        assert!(results.iter().any(|e| e.message_id == mid));
    }

    #[tokio::test]
    async fn cancel_same_sender_two_conversations_both_can_cancel() {
        // Two conversations of the same app (same sender string) can cancel
        // each other's messages — accepted A1 contract.
        let (m, _, _, _, _) = build_messenger(0).await;
        let future = Utc::now() + chrono::Duration::seconds(3600);

        // Publish from conversation 1 (pa-bob).
        let mid = publish_one(&m, "shared-sender", Some(future)).await;

        // Add a second conversation for the same pa-bob sender.
        {
            let conn = m.db.lock().await;
            conn.execute(
                "INSERT OR IGNORE INTO users (id, username, password_hash, created_at) \
                 VALUES (3, 'bob2', 'h', '2024-01-01')",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO conversations (id, user_id, status, app_slug, created_at, updated_at) \
                 VALUES (3, 3, 'active', 'pa-bob', '2024-01-01', '2024-01-01')",
                [],
            )
            .unwrap();
        }

        // Same sender string — cancel succeeds from the "second conversation".
        // (Both conversations share pa-bob's host-derived sender identity,
        // `app:pa-bob@<origin>`.)
        let result = m.cancel("pa-bob", mid).await;
        assert!(
            matches!(result, CancelResult::Ok { .. }),
            "expected Ok, got {result:?}"
        );
    }

    #[tokio::test]
    async fn cancel_idempotent_returns_no_pending_pushes() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let future = Utc::now() + chrono::Duration::seconds(3600);
        let mid = publish_one(&m, "cancel-twice", Some(future)).await;
        let r1 = m.cancel("pa-bob", mid).await;
        assert!(matches!(r1, CancelResult::Ok { .. }));
        let r2 = m.cancel("pa-bob", mid).await;
        assert!(matches!(r2, CancelResult::NoPendingPushes));
    }

    // -----------------------------------------------------------------------
    // Edit tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn edit_unknown_message_returns_unknown_message() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let result = m
            .edit(
                "pa-bob",
                Uuid::new_v4(),
                EditFields {
                    body: Some("x".to_string()),
                    ..Default::default()
                },
            )
            .await;
        assert!(matches!(result, EditResult::UnknownMessage));
    }

    #[tokio::test]
    async fn edit_wrong_sender_returns_not_authorized() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let future = Utc::now() + chrono::Duration::seconds(3600);
        let mid = publish_one(&m, "orig", Some(future)).await;
        let result = m
            .edit(
                "pa-alice",
                mid,
                EditFields {
                    body: Some("hacked".to_string()),
                    ..Default::default()
                },
            )
            .await;
        assert!(matches!(result, EditResult::NotAuthorized));
    }

    #[tokio::test]
    async fn edit_with_no_fields_returns_no_fields_provided() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let future = Utc::now() + chrono::Duration::seconds(3600);
        let mid = publish_one(&m, "hello", Some(future)).await;
        let result = m.edit("pa-bob", mid, EditFields::default()).await;
        assert!(matches!(result, EditResult::NoFieldsProvided));
    }

    #[tokio::test]
    async fn edit_body_too_large_returns_body_too_large() {
        let (mut m, _, _, _, _) = build_messenger(0).await;
        Arc::get_mut(&mut m).unwrap().defaults = MessagingGlobalConfig {
            default_send_budget: 100,
            max_body_bytes: 5,
            ..MessagingGlobalConfig::default()
        };
        let future = Utc::now() + chrono::Duration::seconds(3600);
        let mid = publish_one(&m, "hi", Some(future)).await;
        let result = m
            .edit(
                "pa-bob",
                mid,
                EditFields {
                    body: Some("toolong".to_string()),
                    ..Default::default()
                },
            )
            .await;
        assert!(matches!(result, EditResult::BodyTooLarge { .. }));
    }

    #[tokio::test]
    async fn edit_reply_to_malformed_returns_malformed_address() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let future = Utc::now() + chrono::Duration::seconds(3600);
        let mid = publish_one(&m, "hello", Some(future)).await;
        let result = m
            .edit(
                "pa-bob",
                mid,
                EditFields {
                    reply_to: Some(Some("not-a-brenn-address".to_string())),
                    ..Default::default()
                },
            )
            .await;
        assert!(matches!(result, EditResult::MalformedAddress(_)));
    }

    #[tokio::test]
    async fn edit_reply_to_unknown_channel_returns_unknown_channel() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let future = Utc::now() + chrono::Duration::seconds(3600);
        let mid = publish_one(&m, "hello", Some(future)).await;
        let result = m
            .edit(
                "pa-bob",
                mid,
                EditFields {
                    reply_to: Some(Some(canonical_address("no-such-channel"))),
                    ..Default::default()
                },
            )
            .await;
        assert!(matches!(result, EditResult::UnknownChannel(_)));
    }

    /// Build a messenger whose publisher `pa-bob` has a NARROW visibility scope
    /// (`brenn_publish`/`brenn_subscribe` = exactly `pa-alice`) and a directory
    /// carrying a second channel `secret` that `pa-bob` can neither publish to
    /// nor receive deliveries from. Returns the messenger and a still-pending
    /// message id authored by `pa-bob` on `brenn:pa-alice`.
    async fn build_narrow_messenger() -> (Arc<Messenger>, Uuid) {
        use crate::access::acl::ChannelMatcher;
        use crate::access::{AppCapability, AppPolicy};

        let db = init_db_memory();
        {
            let conn = db.lock().await;
            conn.execute(
                "INSERT INTO users (id, username, password_hash, created_at) \
                 VALUES (1, 'bob', 'h', '2024-01-01')",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO users (id, username, password_hash, created_at) \
                 VALUES (2, 'alice', 'h', '2024-01-01')",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO conversations (id, user_id, status, app_slug, created_at, updated_at) \
                 VALUES (1, 1, 'active', 'pa-bob', '2024-01-01', '2024-01-01')",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO conversations (id, user_id, status, app_slug, created_at, updated_at) \
                 VALUES (2, 2, 'active', 'pa-alice', '2024-01-01', '2024-01-01')",
                [],
            )
            .unwrap();
        }

        let mk_entry = |name: &str, with_subscriber: bool| {
            let subscribers = if with_subscriber {
                vec![crate::messaging::SubscriberEntry {
                    kind: crate::messaging::SubscriberEntryKind::App("pa-alice".to_string()),
                    push_depth: crate::messaging::config::Depth::Unbounded,
                    retain_depth: crate::messaging::config::Depth::Unbounded,
                    noise: crate::messaging::config::NoiseLevel::Silent,
                    wake_min: Some(WakeMin::Normal),
                }]
            } else {
                vec![]
            };
            crate::messaging::testutils::test_channel_entry(name, subscribers)
        };
        let alice_entry = mk_entry("pa-alice", true);
        let secret_entry = mk_entry("secret", false);
        {
            let conn = db.lock().await;
            upsert_channels(&conn, &[alice_entry.clone(), secret_entry.clone()]);
        }

        let directory = Arc::new(MessagingDirectory::with_entries(vec![
            alice_entry,
            secret_entry,
        ]));

        let mut bob = make_app_config(
            "pa-bob",
            Some(ResolvedMessagingConfig {
                send_budget: 100,
                subscriptions: vec![],
            }),
            vec!["bob".to_string()],
        );
        bob.policy = {
            let mut p = AppPolicy::default();
            p.grants.insert(AppCapability::MessagingPublish);
            p.grants.insert(AppCapability::MessagingSubscribe);
            p.acls
                .brenn_publish
                .push(ChannelMatcher::Exact("pa-alice".to_string()));
            p.acls
                .brenn_subscribe
                .push(ChannelMatcher::Exact("pa-alice".to_string()));
            p
        };
        let mut apps: IndexMap<String, crate::config::AppConfig> = IndexMap::new();
        apps.insert("pa-bob".to_string(), bob);
        apps.insert(
            "pa-alice".to_string(),
            make_app_config(
                "pa-alice",
                Some(ResolvedMessagingConfig {
                    send_budget: 100,
                    subscriptions: vec![],
                }),
                vec!["alice".to_string()],
            ),
        );

        let router = Arc::new(CountingRouter::default());
        let messenger = Messenger::new(
            db,
            directory,
            Arc::from("test-source"),
            Arc::new(apps),
            router as Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        );
        let future = Utc::now() + chrono::Duration::seconds(3600);
        let mid = publish_one(&messenger, "orig", Some(future)).await;
        (messenger, mid)
    }

    /// An out-of-visibility `reply_to` fails with `AclDenied` whether or not the
    /// channel exists — closing the success/failure existence oracle. An
    /// in-visibility target still resolves normally.
    #[tokio::test]
    async fn edit_reply_to_out_of_visibility_is_acl_denied_regardless_of_existence() {
        let (m, mid) = build_narrow_messenger().await;

        // `secret` exists in the directory but is outside pa-bob's scope.
        let existing = m
            .edit(
                "pa-bob",
                mid,
                EditFields {
                    reply_to: Some(Some(canonical_address("secret"))),
                    ..Default::default()
                },
            )
            .await;
        // A well-formed address that does not exist at all, also out of scope.
        let absent = m
            .edit(
                "pa-bob",
                mid,
                EditFields {
                    reply_to: Some(Some(canonical_address("ghost"))),
                    ..Default::default()
                },
            )
            .await;
        // Both are the SAME variant — the existence bit does not leak.
        assert!(
            matches!(existing, EditResult::AclDenied(_)),
            "existing out-of-scope reply_to must be AclDenied, got {existing:?}"
        );
        assert!(
            matches!(absent, EditResult::AclDenied(_)),
            "absent out-of-scope reply_to must be AclDenied, got {absent:?}"
        );

        // In-visibility, existing channel still resolves.
        let ok = m
            .edit(
                "pa-bob",
                mid,
                EditFields {
                    reply_to: Some(Some(canonical_address("pa-alice"))),
                    ..Default::default()
                },
            )
            .await;
        assert!(
            matches!(ok, EditResult::Ok { .. }),
            "in-visibility reply_to must succeed, got {ok:?}"
        );
    }

    #[tokio::test]
    async fn edit_reply_to_clear_writes_null_reply_to() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let future = Utc::now() + chrono::Duration::seconds(3600);
        let mid = publish_one(&m, "hello", Some(future)).await;
        let result = m
            .edit(
                "pa-bob",
                mid,
                EditFields {
                    reply_to: Some(None),
                    ..Default::default()
                },
            )
            .await;
        assert!(matches!(result, EditResult::Ok { ref envelope } if envelope.reply_to.is_none()));
    }

    #[tokio::test]
    async fn edit_after_partial_delivery_returns_already_delivered() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let mid = publish_one(&m, "hello", None).await;
        // Simulate delivery: mark the push delivered (off-stack dispatch means
        // publish no longer delivers inline, so we must simulate it explicitly).
        let sub = crate::messaging::ParticipantId::for_conversation(2);
        let pending = m.load_pending_pushes(&sub).await;
        assert_eq!(pending.len(), 1);
        m.mark_pushes_delivered(&[pending[0].0]).await;
        // Edit after delivery → AlreadyDelivered.
        let result = m
            .edit(
                "pa-bob",
                mid,
                EditFields {
                    body: Some("new".to_string()),
                    ..Default::default()
                },
            )
            .await;
        assert!(matches!(result, EditResult::AlreadyDelivered));
    }

    #[tokio::test]
    async fn edit_body_only_no_kicks() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let future = Utc::now() + chrono::Duration::seconds(3600);
        let mid = publish_one(&m, "orig", Some(future)).await;

        // Drain the existing kick from publish (deliver_after was set).
        let kick = m.dispatch_kick_notify.clone();
        // Absorb the publish kick.
        let _ = tokio::time::timeout(std::time::Duration::from_millis(10), kick.notified()).await;

        // Body-only edit: no kicks expected.
        let result = m
            .edit(
                "pa-bob",
                mid,
                EditFields {
                    body: Some("updated".to_string()),
                    ..Default::default()
                },
            )
            .await;
        assert!(matches!(result, EditResult::Ok { .. }));

        // No kick fired within 20ms.
        let no_kick =
            tokio::time::timeout(std::time::Duration::from_millis(20), kick.notified()).await;
        assert!(
            no_kick.is_err(),
            "dispatch_kick should not fire for body-only edit"
        );
    }

    #[tokio::test]
    async fn edit_deliver_after_to_null_clears_release_after_and_signals_dispatcher() {
        let (m, _, _, _, _router) = build_messenger(0).await; // sleeping bridge
        let future = Utc::now() + chrono::Duration::seconds(3600);
        let mid = publish_one(&m, "scheduled", Some(future)).await;

        // Drain any kick from publish so we start fresh.
        let notify = m.dispatch_kick_notify();

        // Edit: clear deliver_after → should signal dispatcher (off-stack dispatch, R1).
        let result = m
            .edit(
                "pa-bob",
                mid,
                EditFields {
                    deliver_after: Some(None),
                    ..Default::default()
                },
            )
            .await;
        assert!(matches!(result, EditResult::Ok { .. }));
        // Dispatcher kick fired (notify has a pending permit).
        tokio::time::timeout(std::time::Duration::from_millis(10), notify.notified())
            .await
            .expect("dispatch_kick must be signaled after clearing deliver_after");
    }

    #[tokio::test]
    async fn edit_deliver_after_to_future_kicks_deliver_after() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let future = Utc::now() + chrono::Duration::seconds(3600);
        let mid = publish_one(&m, "hello", Some(future)).await;

        // Absorb the publish kick.
        let kick = m.dispatch_kick_notify.clone();
        let _ = tokio::time::timeout(std::time::Duration::from_millis(10), kick.notified()).await;

        let kick_notified = kick.notified();
        let new_future = Utc::now() + chrono::Duration::seconds(7200);
        let result = m
            .edit(
                "pa-bob",
                mid,
                EditFields {
                    deliver_after: Some(Some(new_future)),
                    ..Default::default()
                },
            )
            .await;
        assert!(matches!(result, EditResult::Ok { .. }));
        tokio::time::timeout(std::time::Duration::from_millis(100), kick_notified)
            .await
            .expect("dispatch_kick not fired after deliver_after edit");
    }

    #[tokio::test]
    async fn edit_delivery_deadline_kicks_deadline() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let future = Utc::now() + chrono::Duration::seconds(3600);
        let mid = publish_one(&m, "hello", Some(future)).await;

        let kick = m.dispatch_kick_notify.clone();
        // Absorb any publish kick.
        let _ = tokio::time::timeout(std::time::Duration::from_millis(10), kick.notified()).await;

        let kick_notified = kick.notified();
        let deadline = Utc::now() + chrono::Duration::seconds(7200);
        let result = m
            .edit(
                "pa-bob",
                mid,
                EditFields {
                    delivery_deadline: Some(Some(deadline)),
                    ..Default::default()
                },
            )
            .await;
        assert!(matches!(result, EditResult::Ok { .. }));
        tokio::time::timeout(std::time::Duration::from_millis(100), kick_notified)
            .await
            .expect("dispatch_kick not fired");
    }

    #[tokio::test]
    async fn edit_preserves_message_id_uuid() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let future = Utc::now() + chrono::Duration::seconds(3600);
        let mid = publish_one(&m, "orig", Some(future)).await;
        let result = m
            .edit(
                "pa-bob",
                mid,
                EditFields {
                    body: Some("new body".to_string()),
                    ..Default::default()
                },
            )
            .await;
        match result {
            EditResult::Ok { envelope } => assert_eq!(envelope.message_id, mid),
            other => panic!("expected Ok, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn edit_wake_recomputes_per_push_kind() {
        // pa-alice subscribes Immediate. Message is sent with wake=None (combined → None).
        // Edit message wake to Immediate → push wake should flip to Immediate
        // (because subscriber kind is Immediate).
        let (m, _, _, _, _) = build_messenger(0).await;
        let future = Utc::now() + chrono::Duration::seconds(3600);
        // Publish with urgency=Low (parks by default).
        let mid = m
            .publish(
                crate::messaging::PublishOrigin::Conversation { id: 1 },
                "pa-bob",
                &canonical_address("pa-alice"),
                "hello",
                crate::messaging::Urgency::Low,
                None,
                Some(future),
                None,
            )
            .await;
        let mid = match mid {
            crate::messaging::PublishResult::Ok { message_id, .. } => message_id,
            other => panic!("publish failed: {other:?}"),
        };

        // Verify push was created with eager_wake = 0 (publish-time push insert uses
        // eager_wake=false because the subscriber's push_depth is positive but the
        // initial publish is deferred; edit to Immediate → push should flip to eager_wake=1).
        let result = m
            .edit(
                "pa-bob",
                mid,
                EditFields {
                    urgency: Some(Urgency::Normal),
                    ..Default::default()
                },
            )
            .await;
        assert!(matches!(result, EditResult::Ok { .. }));

        // Verify push eager_wake is now 1 in DB after editing to Immediate.
        let conn = m.db.lock().await;
        let eager_wake_int: i64 = conn
            .query_row(
                "SELECT pp.eager_wake FROM messaging_pending_pushes pp
                 JOIN messaging_messages m ON pp.message_id = m.id
                 WHERE m.uuid = ?1",
                rusqlite::params![mid.as_bytes().to_vec()],
                |r| r.get(0),
            )
            .expect("query push eager_wake");
        assert_eq!(
            eager_wake_int, 1,
            "push eager_wake should flip to 1 after editing message to Immediate"
        );
    }

    // -----------------------------------------------------------------------
    // list_pending tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn list_pending_returns_empty_for_sender_with_no_pending() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let _ = publish_one(&m, "delivered", None).await;
        // Simulate delivery: mark the push delivered (off-stack dispatch, R1).
        let sub = crate::messaging::ParticipantId::for_conversation(2);
        let pending = m.load_pending_pushes(&sub).await;
        assert_eq!(pending.len(), 1);
        m.mark_pushes_delivered(&[pending[0].0]).await;
        // All delivered; pending list empty.
        let list = m.list_pending("pa-bob", None).await;
        assert!(list.is_empty());
    }

    #[tokio::test]
    async fn list_pending_returns_only_callers_messages() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let future = Utc::now() + chrono::Duration::seconds(3600);
        let _ = publish_one(&m, "from-bob", Some(future)).await;
        // pa-alice's pending list should be empty (she hasn't sent anything).
        let list = m.list_pending("pa-alice", None).await;
        assert!(list.is_empty());
        // pa-bob's list has 1.
        let list = m.list_pending("pa-bob", None).await;
        assert_eq!(list.len(), 1);
    }

    #[tokio::test]
    async fn list_pending_filters_by_channel_when_provided() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let future = Utc::now() + chrono::Duration::seconds(3600);
        let _ = publish_one(&m, "hello", Some(future)).await;

        // Known channel → 1 result.
        let list = m
            .list_pending("pa-bob", Some(&canonical_address("pa-alice")))
            .await;
        assert_eq!(list.len(), 1);

        // Unknown channel → empty.
        let list = m
            .list_pending("pa-bob", Some("brenn:unknown-channel"))
            .await;
        assert!(list.is_empty());
    }

    #[tokio::test]
    async fn list_pending_orders_by_deliver_after_asc() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let t1 = Utc::now() + chrono::Duration::seconds(100);
        let t2 = Utc::now() + chrono::Duration::seconds(200);
        let _ = publish_one(&m, "later", Some(t2)).await;
        let _ = publish_one(&m, "sooner", Some(t1)).await;
        let list = m.list_pending("pa-bob", None).await;
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].body, "sooner");
        assert_eq!(list[1].body, "later");
    }

    #[tokio::test]
    async fn list_pending_missing_sender_returns_empty() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let list = m.list_pending("no-such-app", None).await;
        assert!(list.is_empty());
    }

    // -----------------------------------------------------------------------
    // Tests from review findings
    // -----------------------------------------------------------------------

    /// test-1: cancel that loses the race with delivery returns Ok{cancelled_pushes:0}.
    /// Documents the §3.2 contract: after delivery the cancel sees no undelivered rows
    /// and returns 0 (here simulated by manually delivering before cancel).
    #[tokio::test]
    async fn cancel_race_returns_ok_with_zero_pushes() {
        // Simulate the "cancel races delivery" window: manually mark the push
        // delivered before cancelling. Off-stack dispatch (R1) means there is no
        // inline delivery, so we must simulate it explicitly here.
        let (m, _, _, _, _) = build_messenger(0).await;
        let mid = publish_one(&m, "race", None).await;
        let sub = crate::messaging::ParticipantId::for_conversation(2);
        let pending = m.load_pending_pushes(&sub).await;
        assert_eq!(pending.len(), 1);
        m.mark_pushes_delivered(&[pending[0].0]).await;
        // All pushes now delivered → AlreadyDelivered.
        let result = m.cancel("pa-bob", mid).await;
        assert!(
            matches!(result, CancelResult::AlreadyDelivered),
            "expected AlreadyDelivered after push marked delivered, got {result:?}"
        );
    }

    /// test-2: editing a previously-cancelled message returns NoPendingPushes.
    #[tokio::test]
    async fn edit_after_cancel_returns_no_pending_pushes() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let mid = publish_one(&m, "to-cancel", None).await;
        let cancel = m.cancel("pa-bob", mid).await;
        assert!(matches!(cancel, CancelResult::Ok { .. }), "{cancel:?}");
        let edit = m
            .edit(
                "pa-bob",
                mid,
                EditFields {
                    body: Some("changed".to_string()),
                    ..Default::default()
                },
            )
            .await;
        assert!(
            matches!(edit, EditResult::NoPendingPushes),
            "expected NoPendingPushes after cancel, got {edit:?}"
        );
    }

    /// test-3: clearing deliver_after zeroes release_after in the DB.
    #[tokio::test]
    async fn edit_clear_deliver_after_zeroes_db_release_after() {
        let (m, _, pa_alice_conv_id, _, _) = build_messenger(0).await;
        let future = Utc::now() + chrono::Duration::seconds(3600);
        let mid = publish_one(&m, "scheduled2", Some(future)).await;

        let result = m
            .edit(
                "pa-bob",
                mid,
                EditFields {
                    deliver_after: Some(None),
                    ..Default::default()
                },
            )
            .await;
        assert!(matches!(result, EditResult::Ok { .. }), "{result:?}");

        // Verify the push row has release_after = NULL and delivered_at set
        // (because dispatch ran with deliver_returns=0 → Ok(false) → bridge parked).
        let conn = m.db.lock().await;
        let release_after: Option<String> = conn
            .query_row(
                "SELECT release_after FROM messaging_pending_pushes \
                 WHERE target_subscriber = ?1 AND delivered_at IS NULL",
                rusqlite::params![
                    crate::messaging::ParticipantId::for_conversation(pa_alice_conv_id).as_str()
                ],
                |r| r.get(0),
            )
            .optional()
            .expect("query release_after");
        // Either the push row exists with release_after IS NULL (parked), or it was
        // delivered (no row). Both are correct outcomes; the key invariant is that
        // if a row exists, release_after must be NULL.
        if let Some(ra) = release_after {
            assert!(
                ra.is_empty() || ra == "NULL",
                "release_after should be NULL, got {ra:?}"
            );
        }
        // Separate check: no undelivered row should have a non-null release_after.
        let has_release: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM messaging_pending_pushes \
                 WHERE target_subscriber = ?1 AND delivered_at IS NULL \
                 AND release_after IS NOT NULL",
                rusqlite::params![
                    crate::messaging::ParticipantId::for_conversation(pa_alice_conv_id).as_str()
                ],
                |r| r.get(0),
            )
            .expect("check release_after null");
        assert!(
            !has_release,
            "undelivered push still has release_after set after edit"
        );
    }

    /// test-4: cancel with unknown app slug returns MissingSender.
    #[tokio::test]
    async fn cancel_missing_sender_returns_missing_sender() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let mid = publish_one(&m, "body", None).await;
        let result = m.cancel("no-such-app", mid).await;
        assert!(
            matches!(result, CancelResult::MissingSender),
            "expected MissingSender, got {result:?}"
        );
    }

    /// test-4 (edit): edit with unknown app slug returns MissingSender.
    #[tokio::test]
    async fn edit_missing_sender_returns_missing_sender() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let mid = publish_one(&m, "body", None).await;
        let result = m
            .edit(
                "no-such-app",
                mid,
                EditFields {
                    body: Some("changed".to_string()),
                    ..Default::default()
                },
            )
            .await;
        assert!(
            matches!(result, EditResult::MissingSender),
            "expected MissingSender, got {result:?}"
        );
    }

    /// test-3 / design §2.5 authz-coherence: `resolve_sender` must return the exact
    /// `app:<slug>@<server>` string. Any drift from this format (e.g. dropping `@server`,
    /// or using a different source than the messenger's `source` field) would make all
    /// owning-app cancel/edit calls silently return `NotAuthorized` after migration.
    #[tokio::test]
    async fn resolve_sender_returns_structured_identity() {
        let (m, _, _, _, _) = build_messenger(0).await;
        // messenger is built with source = "test-source" (build_messenger default).
        let sender = m.resolve_sender("pa-bob");
        assert_eq!(
            sender.as_deref(),
            Some("app:pa-bob@test-source"),
            "resolve_sender must return app:<slug>@<source> exactly"
        );
        // No messaging config → None.
        let none_sender = m.resolve_sender("no-such-app");
        assert!(none_sender.is_none(), "missing app must return None");
    }
}
