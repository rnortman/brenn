//! `Messenger::cancel`, `Messenger::edit`, and `Messenger::list_pending`.
//!
//! Authorship is keyed on `messaging_messages.sender` (the derived
//! `ParticipantId::for_app` identity). Per design §2.5.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::db::{self, EditFieldsApplied};
use super::gates::{reply_to_visible, well_formed_name};
use super::identity::ParticipantId;
use super::{ChannelScheme, MessageEnvelope, Messenger, Urgency};

// ---------------------------------------------------------------------------
// Public result types
// ---------------------------------------------------------------------------

/// Outcome of `Messenger::cancel`.
#[derive(Debug)]
pub enum CancelResult {
    /// The parked message was withdrawn.
    Ok { message_id: Uuid },
    /// No `messaging_messages` row with that UUID.
    UnknownMessage,
    /// Message exists but the `sender` string does not match the caller.
    NotAuthorized,
    /// The message has entered retention and is past recall: every subscriber
    /// reads it from its own position, and the server keeps no per-subscriber
    /// record of the delivery to revoke.
    AlreadyDelivered,
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
    /// The message has entered retention and is past editing, on the same rule
    /// [`CancelResult::AlreadyDelivered`] states.
    AlreadyDelivered,
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
    /// Withdraw a message the sender has not yet let into retention.
    ///
    /// A parked message is the only thing there is to cancel: once a message
    /// enters retention every subscriber reads it from its own position, and no
    /// server-side record of who has read it exists to revoke.
    ///
    /// TODO(ring-deferred-recall): this reaches durable parked messages only. A
    /// deferred publish to a non-durable channel parks in that channel's ring
    /// and returns a message id to its publisher, but there is no
    /// `messaging_messages` row to look up, so it answers `UnknownMessage`.
    /// `edit` and `list_pending` have the same scope.
    pub async fn cancel(&self, sender_app_slug: &str, message_uuid: Uuid) -> CancelResult {
        // 1. Resolve sender string.
        let sender = match self.resolve_sender(sender_app_slug) {
            Some(s) => s,
            None => return CancelResult::MissingSender,
        };

        // 2. Auth + status check + DELETE under a single lock acquisition so the
        //    lookup and the DELETE are linearizable (no TOCTOU gap for a delivery
        //    task to slip between them).
        {
            let conn = self.db.lock().await;
            let lk = match db::lookup_message_for_authorship(&conn, message_uuid) {
                None => return CancelResult::UnknownMessage,
                Some(lk) => lk,
            };
            if lk.sender != sender {
                return CancelResult::NotAuthorized;
            }
            if !lk.parked {
                return CancelResult::AlreadyDelivered;
            }
            // Withdrawing the row is the whole of the cancel: its `deliver_after`
            // is the only thing keeping it out of retention, so a row left behind
            // would enter retention at its release time and deliver exactly what
            // the sender cancelled.
            //
            // The lookup that reported the message parked and this withdrawal
            // share one lock acquisition, and every release pass takes the same
            // lock, so the row cannot have moved between them.
            assert!(
                db::withdraw_parked_message(&conn, lk.message_id, &sender),
                "messaging: parked message {message_uuid} vanished between the lookup and the \
                 withdrawal under one lock acquisition"
            );
        }

        // 4. Kick the dispatcher — the cancel may have been the earliest parked
        //    message in the release timer.
        self.dispatch_kick();

        CancelResult::Ok {
            message_id: message_uuid,
        }
    }

    /// Edit a still-parked message in-place.
    ///
    /// Fails with `AlreadyDelivered` once the message has entered retention.
    /// Releases immediately if `deliver_after` is cleared.
    /// Kicks background timers for touched scheduling fields.
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
        // A parked message is the only editable one: a retained message is past
        // recall, on the same rule cancel applies.
        if !lookup.parked {
            return EditResult::AlreadyDelivered;
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

        // 6. Normalize deliver_after: a past timestamp is the same request as
        //    clearing the schedule. Also determine whether we need to release
        //    immediately after the edit.
        let normalized_deliver_after: Option<Option<DateTime<Utc>>> = match fields.deliver_after {
            Some(Some(da)) if da <= Utc::now() => Some(None), // past → treat as null
            other => other,
        };
        // Dispatch immediately when deliver_after is being cleared (explicit null or past).
        let deliver_after_cleared = matches!(normalized_deliver_after, Some(None));
        // Unscheduling a parked message means releasing it now, not blanking its
        // column: only the release path assigns a retention position. Writing NULL
        // here would strand the row outside retention forever — invisible to
        // replay, history, the pending list, and cancel alike. Leaving it due
        // instead hands it to the next release pass, which the kick below wakes at
        // once.
        let applied_deliver_after = if deliver_after_cleared {
            Some(Some(Utc::now()))
        } else {
            normalized_deliver_after
        };

        // 7. Apply to DB (inside a transaction with A3 re-check).
        let applied = EditFieldsApplied {
            body: fields.body.as_deref(),
            reply_to_uuid: reply_to_resolved,
            deliver_after: applied_deliver_after,
            delivery_deadline: fields.delivery_deadline,
            urgency: fields.urgency,
        };

        {
            let conn = self.db.lock().await;
            db::update_parked_message(&conn, lookup.message_id, &sender, &applied);
        }

        // 8. Nothing per-subscriber to recompute: the wake decision is made at
        //    wake time, from the message row this edit just rewrote.

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

    /// List the still-parked messages authored by this app's sender. Optionally
    /// filtered to a single channel.
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
            _envelope: &std::sync::Arc<crate::messaging::MessageEnvelope>,
            _retained_seq: i64,
        ) -> Result<bool, String> {
            self.deliveries
                .lock()
                .await
                .push(ParticipantId::for_surface(_key.slug()));
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
        fn alarm(
            &self,
            _channel: &str,
            _subscriber: &crate::messaging::ParticipantId,
            _count: u64,
        ) {
        }
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
                send_rate: Default::default(),
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

    /// An unparked publish is in retention, where every subscriber reads it from
    /// its own position — nothing to withdraw and no per-subscriber record to
    /// revoke.
    #[tokio::test]
    async fn cancel_a_published_message_returns_already_delivered() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let mid = publish_one(&m, "hello", None).await;
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
        assert!(
            matches!(result, CancelResult::Ok { .. }),
            "expected Ok, got {result:?}"
        );

        // Kick fired.
        tokio::time::timeout(std::time::Duration::from_millis(100), kick_notified)
            .await
            .expect("dispatch_kick not fired");
    }

    /// A refused cancel changes nothing: the message it named is in retention and
    /// stays queryable.
    #[tokio::test]
    async fn a_refused_cancel_leaves_the_message_in_history() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let mid = publish_one(&m, "cancel-then-read", None).await;
        assert!(matches!(
            m.cancel("pa-bob", mid).await,
            CancelResult::AlreadyDelivered
        ));

        let results = m.query(&history_query()).await.expect("query");
        assert!(results.iter().any(|e| e.message_id == mid));
    }

    /// A parked message holds no retention position, so no read observes it —
    /// history included — until a release pass moves it into the window.
    #[tokio::test]
    async fn parked_message_is_absent_from_channel_history_until_release() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let future = Utc::now() + chrono::Duration::seconds(3600);
        let mid = publish_one(&m, "parked-then-read", Some(future)).await;

        let parked = m.query(&history_query()).await.expect("query");
        assert!(
            !parked.iter().any(|e| e.message_id == mid),
            "a parked message must not appear in channel history"
        );

        m.store_for_address(&canonical_address("pa-alice"))
            .release_due(future + chrono::Duration::seconds(1))
            .await;

        let released = m.query(&history_query()).await.expect("query");
        assert!(
            released.iter().any(|e| e.message_id == mid),
            "the message joins history when it releases"
        );
    }

    fn history_query() -> crate::messaging::MessageQuery {
        crate::messaging::MessageQuery {
            channel: canonical_address("pa-alice"),
            limit: 10,
            before: None,
            after: None,
            sender: None,
            search: None,
            calling_app_slug: "pa-bob".to_string(),
        }
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

    /// Cancelling a parked message withdraws the row, so a second cancel has
    /// nothing left to name.
    #[tokio::test]
    async fn cancel_twice_is_not_found_the_second_time() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let future = Utc::now() + chrono::Duration::seconds(3600);
        let parked = publish_one(&m, "cancel-twice", Some(future)).await;
        assert!(matches!(
            m.cancel("pa-bob", parked).await,
            CancelResult::Ok { .. }
        ));
        assert!(matches!(
            m.cancel("pa-bob", parked).await,
            CancelResult::UnknownMessage
        ));
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

    /// An unparked publish is already in retention, where each subscriber reads it
    /// from its own position — past editing.
    #[tokio::test]
    async fn edit_a_published_message_returns_already_delivered() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let mid = publish_one(&m, "hello", None).await;
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
    async fn edit_clearing_deliver_after_signals_the_dispatcher() {
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

    // -----------------------------------------------------------------------
    // list_pending tests
    // -----------------------------------------------------------------------

    /// An unparked publish is in retention from the moment it commits, so it is
    /// never pending.
    #[tokio::test]
    async fn list_pending_returns_empty_for_sender_with_no_parked_messages() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let _ = publish_one(&m, "published", None).await;
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

    /// The message row's `(deliver_after, retained_seq)` pair, or `None` if the
    /// row is gone.
    async fn schedule_and_position(
        m: &Messenger,
        message_uuid: Uuid,
    ) -> Option<(Option<String>, Option<i64>)> {
        let conn = m.db.lock().await;
        conn.query_row(
            "SELECT deliver_after, retained_seq FROM messaging_messages WHERE uuid = ?1",
            rusqlite::params![message_uuid.as_bytes().to_vec()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .expect("query message schedule")
    }

    /// test-3: clearing `deliver_after` on a parked message releases it — the
    /// operation the edit tool advertises as "send it now".
    ///
    /// A parked message holds no retention position; only the release path assigns
    /// one. Blanking the column instead would leave the row outside retention with
    /// no schedule to bring it back: invisible to replay, history, the pending
    /// list, and cancel alike, and readable by nobody.
    #[tokio::test]
    async fn edit_clearing_deliver_after_releases_a_parked_message() {
        let (m, _, _, _, _) = build_messenger(0).await;
        let future = Utc::now() + chrono::Duration::seconds(3600);
        let mid = publish_one(&m, "scheduled2", Some(future)).await;
        assert!(
            schedule_and_position(&m, mid)
                .await
                .expect("the parked message row")
                .1
                .is_none(),
            "a parked message holds no retention position"
        );

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

        // The edit leaves the message due; the release pass the kick wakes is
        // what moves it into retention.
        m.release_due_messages(Utc::now()).await;

        let (deliver_after, retained_seq) = schedule_and_position(&m, mid)
            .await
            .expect("the unscheduled message survives the edit");
        assert!(
            deliver_after.is_none(),
            "a released message carries no schedule, got {deliver_after:?}"
        );
        assert!(
            retained_seq.is_some(),
            "release assigns the retention position every read keys on"
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
