//! Conversations as cursor subscribers.
//!
//! An `App(slug)` directory subscriber delivers to the app's singleton
//! conversation, and that conversation holds a position on every channel it
//! subscribes to — the same delivery state a WASM port or a system participant
//! holds. This module is that family's substrate side: resolving which channels
//! a conversation subscribes to, creating its positions, reading its window, and
//! advancing it once the bridge has rendered what the window served.
//!
//! The ack point is after the render (at-least-once): a crash between the read
//! and the advance re-serves the same suffix.

use std::sync::Arc;

use tracing::debug;

use super::config::NoiseLevel;
use super::store::{MessageSeq, priming_for_kind};
use super::{
    ChannelEntry, MessageEnvelope, Messenger, ParticipantId, SubscriberEntry, SubscriberEntryKind,
};

/// What a conversation's channels are holding for it, and the advances that
/// pass over it.
///
/// The two halves are separate because the caller renders and sends between
/// them: an advance that ran before a failed send would lose the batch.
#[derive(Debug, Default)]
pub struct ConversationDelivery {
    /// Every new message across the conversation's channels, in publish order.
    pub messages: Vec<MessageEnvelope>,
    /// One span per channel that served something, applied by
    /// [`Messenger::advance_conversation`] after the batch lands.
    spans: Vec<ConversationAdvance>,
}

/// One channel's advance: where this conversation's position moves to, and the
/// noise rung its losses are enacted at.
#[derive(Debug)]
struct ConversationAdvance {
    channel_address: String,
    through: MessageSeq,
    seen_floor: MessageSeq,
    noise: NoiseLevel,
}

impl ConversationDelivery {
    /// Nothing was owed on any channel.
    ///
    /// Keyed on the spans, not the messages: a batch consisting entirely of the
    /// conversation's own utterances is filtered down to no messages while its
    /// positions are still owed an advance. A caller that read emptiness off the
    /// message list would leave those positions in place and be handed the same
    /// batch on every subsequent wake, forever.
    pub fn is_empty(&self) -> bool {
        self.spans.is_empty()
    }
}

impl Messenger {
    /// Every channel carrying an `App` subscriber that resolves to
    /// `conversation_id`, paired with that subscriber entry.
    ///
    /// The directory is the authority on what an app subscribes to, and the
    /// resolver is the authority on which conversation an `App` subscriber
    /// delivers to — the same resolution the commit path performs, run
    /// backwards. Resolve-only: asking what a conversation subscribes to never
    /// mints a conversation.
    ///
    /// The conversation row names its own app, so only that app's subscribers
    /// are candidates and the forward resolution (app → owner → singleton
    /// conversation) answers identically on each of its channels: it runs once,
    /// not once per channel. The row alone is not the answer — an app can hold
    /// more than one conversation and only the singleton is a delivery target —
    /// so the resolution still decides, it just decides once.
    ///
    /// Carries the delivery-time ACL gate: a subscriber whose policy no longer
    /// covers the channel is not served from it. Its position stays where it is,
    /// so a restored ACL resumes rather than re-primes.
    async fn conversation_subscriptions(
        &self,
        conversation_id: i64,
    ) -> Vec<(Arc<ChannelEntry>, SubscriberEntry)> {
        let conn = self.db.lock().await;
        let Some(conversation) = crate::conversation::get_conversation_opt(&conn, conversation_id)
        else {
            return Vec::new();
        };
        let participant = ParticipantId::for_conversation(conversation_id);
        let mut resolves_here: Option<bool> = None;
        let mut found = Vec::new();
        for entry in self.directory.list() {
            for sub in &entry.subscribers {
                let SubscriberEntryKind::App(slug) = &sub.kind else {
                    continue;
                };
                if *slug != conversation.app_slug {
                    continue;
                }
                let mine = *resolves_here.get_or_insert_with(|| {
                    self.targets.app_conversation(&conn, slug, &entry.address)
                        == Some(conversation_id)
                });
                if !mine {
                    continue;
                }
                if !self.channel_access_allowed(&sub.kind, &entry.address) {
                    self.warn_acl_denied(&entry, &participant);
                    continue;
                }
                found.push((Arc::clone(&entry), sub.clone()));
            }
        }
        found
    }

    /// Create a position for every push-enabled `App` subscriber in the
    /// directory, on the channel it subscribes to.
    ///
    /// Run at boot, and again whenever an app gains a subscription: a
    /// push-enabled subscriber with no position is served nothing, and `Head`
    /// priming means the position must exist before the publish it is meant to
    /// catch, not after.
    ///
    /// The conversation itself is minted here when the app has never had one —
    /// an app wired to receive is a delivery target whether or not anything has
    /// been published to it yet.
    pub async fn attach_conversation_subscribers(&self) {
        for entry in self.directory.list() {
            for sub in &entry.subscribers {
                let SubscriberEntryKind::App(slug) = &sub.kind else {
                    continue;
                };
                if !sub.push_depth.is_push_enabled() {
                    continue;
                }
                self.attach_conversation(&entry.address, slug, sub.push_depth)
                    .await;
            }
        }
    }

    /// Create one app's position on one channel, minting its conversation if it
    /// has none. A no-op beyond a depth retune when the position is already
    /// there, so a re-run at boot or a re-subscribe keeps what the conversation
    /// has seen.
    pub async fn attach_conversation(
        &self,
        channel_address: &str,
        app_slug: &str,
        push_depth: super::config::Depth,
    ) {
        let conversation = {
            let conn = self.db.lock().await;
            self.targets
                .ensure_app_conversation(&conn, app_slug, channel_address)
        };
        let Some(conversation) = conversation else {
            // `ensure_app_conversation` has already named the missing piece.
            return;
        };
        self.attach_subscriber(
            channel_address,
            app_slug,
            &ParticipantId::for_conversation(conversation),
            push_depth,
            priming_for_kind(&SubscriberEntryKind::App(app_slug.to_string())),
        )
        .await;
    }

    /// Tear down one app's position on one channel — the inverse of
    /// [`Messenger::attach_conversation`], for an app that unsubscribes.
    /// Resolve-only: an app that never had a conversation holds no position.
    pub async fn detach_conversation(&self, channel_address: &str, app_slug: &str) {
        let conversation = {
            let conn = self.db.lock().await;
            self.targets
                .app_conversation(&conn, app_slug, channel_address)
        };
        if let Some(conversation) = conversation {
            self.detach_subscriber(
                channel_address,
                &ParticipantId::for_conversation(conversation),
            )
            .await;
        }
    }

    /// What every channel this conversation subscribes to is holding for it:
    /// the new entries of each channel's window, in publish order across
    /// channels, plus the advance that passes over them.
    ///
    /// A pure read — the conversation's positions do not move until
    /// [`Messenger::advance_conversation`] runs, so a render or a send that
    /// fails leaves the batch owed.
    ///
    /// A subscription whose position is gone by the time its window is read is
    /// skipped: an unsubscribe can land between the enumeration above and the
    /// read below, and a subscriber that left is owed nothing.
    ///
    /// **A conversation is never handed a message it itself sent.** Envelopes
    /// whose sender is this conversation are dropped from the batch: its own
    /// utterances are already in its context, so repeating them back is zero
    /// information at the price of a real turn. It also breaks the cycle an
    /// operator-authored subscription on a conversation's own record would
    /// otherwise arm, where a delivery is injected as a system message, the
    /// system message is republished to the record, and the record delivers it
    /// again — a loop with no decision in it anywhere.
    ///
    /// The spans are computed over everything the windows held, filtered or not,
    /// so the positions advance past a self-echo rather than being re-served it
    /// on every wake.
    pub async fn conversation_delivery(&self, conversation_id: i64) -> ConversationDelivery {
        let subscriber = ParticipantId::for_conversation(conversation_id);
        let mut delivery = ConversationDelivery::default();
        for (entry, sub) in self.conversation_subscriptions(conversation_id).await {
            if !sub.push_depth.is_push_enabled() {
                // Pull-only: visibility without delivery. It holds no position,
                // and `MessageChannelGet` is how it reads.
                continue;
            }
            let window = self
                .store_for(&entry)
                .window(&subscriber, sub.push_depth, sub.retain_depth)
                .await;
            let Some(window) = window else {
                debug!(
                    channel = %entry.address,
                    subscriber = %subscriber.as_str(),
                    "conversation delivery skips a subscription with no position"
                );
                continue;
            };
            if window.new_entries().is_empty() {
                continue;
            }
            delivery.messages.extend(
                window
                    .new_entries()
                    .iter()
                    .filter(|(_, env)| env.sender != subscriber.as_str())
                    .map(|(_, env)| MessageEnvelope::clone(env)),
            );
            if let Some((through, seen_floor)) = window.advance_span() {
                delivery.spans.push(ConversationAdvance {
                    channel_address: entry.address.clone(),
                    through,
                    seen_floor,
                    noise: sub.noise,
                });
            }
        }
        delivery.messages.sort_by_key(|env| env.publish_ts);
        delivery
    }

    /// Move the conversation's positions past what `delivery` served, enacting
    /// the noise for whatever its windows skipped.
    ///
    /// Called after the batch has been rendered and accepted — the drain's
    /// at-least-once ack point.
    ///
    /// A span whose position is gone by the time it is advanced is skipped: an
    /// unsubscribe can land between the read that produced the span and this
    /// call, and there is then no position to move and nothing to report.
    pub async fn advance_conversation(&self, conversation_id: i64, delivery: ConversationDelivery) {
        let subscriber = ParticipantId::for_conversation(conversation_id);
        for span in delivery.spans {
            let advanced = self
                .advance_subscriber(
                    &span.channel_address,
                    &subscriber,
                    span.through,
                    span.seen_floor,
                    span.noise,
                )
                .await;
            if advanced.is_none() {
                debug!(
                    channel = %span.channel_address,
                    subscriber = %subscriber.as_str(),
                    "conversation advance skips a span whose position is gone"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use indexmap::IndexMap;
    use uuid::Uuid;

    use crate::config::AppConfig;
    use crate::db::init_db_memory;
    use crate::messaging::config::{
        Depth, MessagingGlobalConfig, NoiseLevel, ResolvedChannel, Sink,
    };
    use crate::messaging::db::{insert_message, upsert_channels, utc_to_ns};
    use crate::messaging::query::NoopWakeRouter;
    use crate::messaging::test_support::{brenn_delivery_policy, test_app_config};
    use crate::messaging::{
        ChannelEntry, ChannelScheme, MessagingDirectory, Messenger, ParticipantId, SubscriberEntry,
        SubscriberEntryKind, Urgency, WakeMin, WakeRouter, canonical_address,
    };

    const APP: &str = "chatapp";
    const USER: &str = "u";

    fn channel(name: &str, push_depth: Depth) -> ChannelEntry {
        transport_channel(&canonical_address(name), ChannelScheme::Brenn, push_depth)
    }

    /// The same subscriber shape on a channel of any scheme — what the delivery
    /// gate dispatches on is the address, so a test of that dispatch needs to
    /// name one of each.
    fn transport_channel(
        address: &str,
        transport_type: ChannelScheme,
        push_depth: Depth,
    ) -> ChannelEntry {
        ChannelEntry {
            uuid: Uuid::new_v4(),
            address: address.to_string(),
            description: None,
            resolved_channel: ResolvedChannel {
                send_rate: Default::default(),
                push_depth,
                retain_depth: Depth::Bounded(10),
                standing_retain_depth: Depth::Bounded(10),
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers: vec![SubscriberEntry {
                kind: SubscriberEntryKind::App(APP.to_string()),
                push_depth,
                retain_depth: Depth::Bounded(0),
                noise: NoiseLevel::Silent,
                wake_min: Some(WakeMin::Normal),
            }],
            transport_type,
            mount: None,
        }
    }

    /// A messenger over `entries`, with `chatapp` wired as a singleton app whose
    /// one allowed user exists — the shape an `App` subscriber resolves to a
    /// conversation through. The conversation itself is not created here: the
    /// attach mints it, which is half of what these rows are about.
    async fn messenger(entries: Vec<ChannelEntry>) -> Arc<Messenger> {
        messenger_with_policy(
            entries,
            brenn_delivery_policy(crate::access::acl::ChannelMatcher::Prefix(String::new())),
        )
        .await
    }

    /// The same, with the app's policy chosen by the caller — the delivery gate
    /// reads it, so a case about the gate has to be able to say what it holds.
    async fn messenger_with_policy(
        entries: Vec<ChannelEntry>,
        policy: crate::access::AppPolicy,
    ) -> Arc<Messenger> {
        let db = init_db_memory();
        {
            let conn = db.lock().await;
            crate::auth::user::create_user(&conn, USER, "$argon2id$fake");
            upsert_channels(&conn, &entries);
        }
        let mut app: AppConfig = test_app_config(APP, None, vec![USER.to_string()]);
        app.singleton = true;
        app.policy = policy;
        let mut apps: IndexMap<String, AppConfig> = IndexMap::new();
        apps.insert(APP.to_string(), app);
        Messenger::new(
            db,
            Arc::new(MessagingDirectory::with_entries(entries)),
            Arc::from("test-source"),
            Arc::new(apps),
            Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        )
    }

    async fn publish(m: &Messenger, channel_uuid: Uuid, body: &str) {
        publish_from(m, channel_uuid, "someone", body).await;
    }

    /// [`publish`] with the sender named, for the cases that turn on who spoke.
    async fn publish_from(m: &Messenger, channel_uuid: Uuid, sender: &str, body: &str) {
        let conn = m.db.lock().await;
        insert_message(
            &conn,
            channel_uuid,
            "test-source",
            sender,
            body,
            Urgency::Normal,
            ChannelScheme::Brenn,
            None,
            None,
            None,
            None,
            utc_to_ns(chrono::Utc::now()),
        );
    }

    /// The conversation the app's subscriber resolves to, after an attach has
    /// minted it.
    async fn conversation_of(m: &Messenger) -> i64 {
        let conn = m.db.lock().await;
        let user = crate::auth::user::get_user_by_username(&conn, USER).expect("user exists");
        crate::conversation::get_singleton_conversation_id(&conn, user.id, APP)
            .expect("attach minted the app's conversation")
    }

    /// Attach mints the conversation and positions it at head: what was
    /// published before the app had a position is not served, what follows is.
    #[tokio::test]
    async fn attach_primes_the_conversation_at_head() {
        let ch = channel("chat", Depth::Bounded(5));
        let uuid = ch.uuid;
        let m = messenger(vec![ch]).await;
        publish(&m, uuid, "before").await;

        m.attach_conversation_subscribers().await;
        let conversation = conversation_of(&m).await;
        assert!(
            m.conversation_delivery(conversation).await.is_empty(),
            "a message published before the position existed is not owed to it"
        );

        publish(&m, uuid, "after").await;
        let delivery = m.conversation_delivery(conversation).await;
        assert_eq!(
            delivery
                .messages
                .iter()
                .map(|e| e.body.as_str())
                .collect::<Vec<_>>(),
            vec!["after"]
        );
    }

    /// The read is pure and the advance is the ack point: the same batch is
    /// served again until the advance runs, and never again after.
    #[tokio::test]
    async fn the_advance_is_the_ack_point() {
        let ch = channel("chat", Depth::Bounded(5));
        let uuid = ch.uuid;
        let m = messenger(vec![ch]).await;
        m.attach_conversation_subscribers().await;
        let conversation = conversation_of(&m).await;
        publish(&m, uuid, "one").await;

        let first = m.conversation_delivery(conversation).await;
        assert_eq!(first.messages.len(), 1);
        let again = m.conversation_delivery(conversation).await;
        assert_eq!(
            again.messages.len(),
            1,
            "a read that was never advanced over re-serves the same suffix"
        );

        m.advance_conversation(conversation, again).await;
        assert!(
            m.conversation_delivery(conversation).await.is_empty(),
            "the advance moved the position past the batch"
        );
    }

    /// A conversation subscribed to a channel it also publishes on is served
    /// everyone else's messages and none of its own.
    #[tokio::test]
    async fn a_conversation_is_not_served_its_own_utterances() {
        let ch = channel("chat", Depth::Bounded(5));
        let uuid = ch.uuid;
        let m = messenger(vec![ch]).await;
        m.attach_conversation_subscribers().await;
        let conversation = conversation_of(&m).await;
        let me = ParticipantId::for_conversation(conversation);

        publish_from(&m, uuid, "someone", "from a peer").await;
        publish_from(&m, uuid, me.as_str(), "my own echo").await;
        publish_from(&m, uuid, "app:other@host", "from an app").await;

        let delivery = m.conversation_delivery(conversation).await;
        assert_eq!(
            delivery
                .messages
                .iter()
                .map(|e| e.body.as_str())
                .collect::<Vec<_>>(),
            vec!["from a peer", "from an app"],
            "only the conversation's own sender is filtered"
        );
    }

    /// The livelock pin: a batch that filters down to nothing still owes an
    /// advance, and taking it leaves nothing owed. A caller that read emptiness
    /// off the message list would be handed this batch on every wake forever.
    #[tokio::test]
    async fn an_all_self_batch_advances_without_serving_anything() {
        let ch = channel("chat", Depth::Bounded(5));
        let uuid = ch.uuid;
        let m = messenger(vec![ch]).await;
        m.attach_conversation_subscribers().await;
        let conversation = conversation_of(&m).await;
        let me = ParticipantId::for_conversation(conversation);

        publish_from(&m, uuid, me.as_str(), "one").await;
        publish_from(&m, uuid, me.as_str(), "two").await;

        let delivery = m.conversation_delivery(conversation).await;
        assert!(delivery.messages.is_empty(), "nothing to say");
        assert!(
            !delivery.is_empty(),
            "the positions are still owed an advance"
        );

        m.advance_conversation(conversation, delivery).await;
        assert!(
            m.conversation_delivery(conversation).await.is_empty(),
            "the advance retired the batch; a later wake finds nothing owed"
        );

        // And the channel is live again for a real message.
        publish_from(&m, uuid, "someone", "after").await;
        let delivery = m.conversation_delivery(conversation).await;
        assert_eq!(
            delivery
                .messages
                .iter()
                .map(|e| e.body.as_str())
                .collect::<Vec<_>>(),
            vec!["after"]
        );
    }

    /// One delivery spans every channel the conversation subscribes to, in
    /// publish order, and advances each of them.
    #[tokio::test]
    async fn one_delivery_spans_every_subscribed_channel() {
        let ch_a = channel("chat-a", Depth::Bounded(5));
        let ch_b = channel("chat-b", Depth::Bounded(5));
        let (uuid_a, uuid_b) = (ch_a.uuid, ch_b.uuid);
        let m = messenger(vec![ch_a, ch_b]).await;
        m.attach_conversation_subscribers().await;
        let conversation = conversation_of(&m).await;

        publish(&m, uuid_b, "first").await;
        publish(&m, uuid_a, "second").await;
        publish(&m, uuid_b, "third").await;

        let delivery = m.conversation_delivery(conversation).await;
        assert_eq!(
            delivery
                .messages
                .iter()
                .map(|e| e.body.as_str())
                .collect::<Vec<_>>(),
            vec!["first", "second", "third"],
            "channels are read one after another but the batch is in publish order"
        );
        m.advance_conversation(conversation, delivery).await;
        assert!(
            m.conversation_delivery(conversation).await.is_empty(),
            "both channels advanced"
        );
    }

    /// A detach landing between the subscription enumeration and the window read
    /// leaves one subscription with no position. The drain skips it — it is owed
    /// nothing — and the conversation's other channels still deliver.
    #[tokio::test]
    async fn a_subscription_detached_mid_read_is_skipped_and_the_rest_deliver() {
        let ch_a = channel("chat-a", Depth::Bounded(5));
        let ch_b = channel("chat-b", Depth::Bounded(5));
        let (uuid_a, uuid_b) = (ch_a.uuid, ch_b.uuid);
        let addr_a = ch_a.address.clone();
        let m = messenger(vec![ch_a, ch_b]).await;
        m.attach_conversation_subscribers().await;
        let conversation = conversation_of(&m).await;
        publish(&m, uuid_a, "gone").await;
        publish(&m, uuid_b, "kept").await;

        // The directory still lists the subscription; only its position is gone.
        m.detach_conversation(&addr_a, APP).await;

        let delivery = m.conversation_delivery(conversation).await;
        assert_eq!(
            delivery
                .messages
                .iter()
                .map(|e| e.body.as_str())
                .collect::<Vec<_>>(),
            vec!["kept"],
            "the departed subscription is skipped, the rest is served"
        );
        m.advance_conversation(conversation, delivery).await;
        assert!(
            m.conversation_delivery(conversation).await.is_empty(),
            "the served channel advanced and the departed one still serves nothing"
        );
    }

    /// A detach landing between the read and the ack: the span the read produced
    /// no longer has a position to move. Two legal operations interleaving, so
    /// the advance completes as a no-op rather than killing the process.
    #[tokio::test]
    async fn an_advance_over_a_detached_subscription_is_a_no_op() {
        let mut ch = channel("chat", Depth::Bounded(1));
        ch.subscribers[0].noise = NoiseLevel::Metered;
        let (uuid, address) = (ch.uuid, ch.address.clone());
        let m = messenger(vec![ch]).await;
        m.attach_conversation_subscribers().await;
        let conversation = conversation_of(&m).await;
        for body in ["one", "two", "three"] {
            publish(&m, uuid, body).await;
        }

        let delivery = m.conversation_delivery(conversation).await;
        assert_eq!(delivery.spans.len(), 1, "the read produced a span to ack");

        m.detach_conversation(&address, APP).await;
        m.advance_conversation(conversation, delivery).await;

        assert_eq!(
            m.drop_counter(&address, &ParticipantId::for_conversation(conversation)),
            0,
            "a refused advance charges the departed subscriber nothing"
        );
    }

    /// A backlog deeper than the subscription's push depth is served newest-first
    /// and the remainder is charged at the subscription's rung. This is the
    /// substrate's answer to a conversation that was away: the newest state, and
    /// a visible, attributed loss for the rest — never the oldest, never silence.
    #[tokio::test]
    async fn an_over_deep_backlog_serves_the_newest_and_charges_the_rest() {
        let mut ch = channel("chat", Depth::Bounded(1));
        ch.subscribers[0].noise = NoiseLevel::Metered;
        let (uuid, address) = (ch.uuid, ch.address.clone());
        let m = messenger(vec![ch]).await;
        m.attach_conversation_subscribers().await;
        let conversation = conversation_of(&m).await;

        for body in ["one", "two", "three"] {
            publish(&m, uuid, body).await;
        }
        let delivery = m.conversation_delivery(conversation).await;
        assert_eq!(
            delivery
                .messages
                .iter()
                .map(|e| e.body.as_str())
                .collect::<Vec<_>>(),
            vec!["three"],
            "the clamp hands over the newest, not the oldest"
        );

        m.advance_conversation(conversation, delivery).await;
        assert_eq!(
            m.drop_counter(&address, &ParticipantId::for_conversation(conversation)),
            2,
            "the two the window skipped are metered at the subscription's rung"
        );
    }

    /// A pull-only subscription holds no position and is never delivered to —
    /// visibility is `MessageChannelGet`'s job, not the drain's.
    #[tokio::test]
    async fn a_pull_only_subscriber_is_never_delivered_to() {
        let ch = channel("chat", Depth::Bounded(0));
        let uuid = ch.uuid;
        let m = messenger(vec![ch]).await;
        m.attach_conversation_subscribers().await;
        // No attach happened, so no conversation was minted either.
        let conversation = {
            let conn = m.db.lock().await;
            let user = crate::auth::user::get_user_by_username(&conn, USER).expect("user exists");
            assert!(
                crate::conversation::get_singleton_conversation_id(&conn, user.id, APP).is_none(),
                "a pull-only subscriber is not a delivery target and mints nothing"
            );
            crate::conversation::get_or_create_singleton_conversation(&conn, user.id, APP).id
        };
        publish(&m, uuid, "unread").await;
        assert!(
            m.conversation_delivery(conversation).await.is_empty(),
            "a pull-only subscription delivers nothing"
        );
    }

    /// The same store and directory, under an app whose policy covers nothing —
    /// the operator revoking the app's access to the channel, with every position
    /// left exactly where it was.
    fn revoked_messenger(m: &Messenger) -> Arc<Messenger> {
        let mut app: AppConfig = test_app_config(APP, None, vec![USER.to_string()]);
        app.singleton = true;
        app.policy = crate::access::AppPolicy::default();
        let mut apps: IndexMap<String, AppConfig> = IndexMap::new();
        apps.insert(APP.to_string(), app);
        Messenger::new(
            m.db.clone(),
            Arc::clone(m.directory()),
            Arc::from("test-source"),
            Arc::new(apps),
            Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        )
    }

    /// A revoked ACL stops delivery from that channel — the same delivery-time
    /// gate the commit path applies, now applied where the read happens.
    #[tokio::test]
    async fn a_revoked_acl_stops_delivery() {
        let ch = channel("chat", Depth::Bounded(5));
        let uuid = ch.uuid;
        let m = messenger(vec![ch]).await;
        m.attach_conversation_subscribers().await;
        let conversation = conversation_of(&m).await;
        publish(&m, uuid, "allowed-then-revoked").await;
        assert_eq!(
            m.conversation_delivery(conversation).await.messages.len(),
            1
        );

        let revoked = revoked_messenger(&m);
        assert!(
            revoked.conversation_delivery(conversation).await.is_empty(),
            "a subscriber whose policy no longer covers the channel is not served from it"
        );
    }

    /// The gate is two lookups composed — the app has a policy, and that policy
    /// covers this address — and the second one dispatches on the address's
    /// scheme: `mqtt:` against the app's mqtt matchers, `webhook:` against its
    /// webhook ones. One covered and one uncovered channel of each scheme pin
    /// that dispatch at the delivery point, where routing an address to the wrong
    /// per-transport check either leaks a channel the operator did not grant or
    /// silently starves one they did.
    #[tokio::test]
    async fn the_delivery_gate_checks_each_transport_against_its_own_acl() {
        use crate::access::acl::{MqttSubMatcher, WebhookMatcher};
        use crate::access::{AppCapability, AppPolicy};

        let covered_mqtt = transport_channel(
            "mqtt:home:sensors/kitchen/temp",
            ChannelScheme::Mqtt,
            Depth::Bounded(5),
        );
        let uncovered_mqtt = transport_channel(
            "mqtt:office:sensors/kitchen/temp",
            ChannelScheme::Mqtt,
            Depth::Bounded(5),
        );
        let covered_hook =
            transport_channel("webhook:github", ChannelScheme::Webhook, Depth::Bounded(5));
        let uncovered_hook =
            transport_channel("webhook:gitlab", ChannelScheme::Webhook, Depth::Bounded(5));
        let uuids = [
            covered_mqtt.uuid,
            uncovered_mqtt.uuid,
            covered_hook.uuid,
            uncovered_hook.uuid,
        ];

        let mut policy = AppPolicy::default();
        policy.grants.insert(AppCapability::MqttSubscribe);
        policy.grants.insert(AppCapability::Webhook);
        policy.acls.mqtt_subscribe.push(MqttSubMatcher {
            client: "home".to_string(),
            topic_filter: "sensors/+/temp".to_string(),
        });
        policy.acls.webhook.push(WebhookMatcher {
            endpoint: "github".to_string(),
        });

        let m = messenger_with_policy(
            vec![covered_mqtt, uncovered_mqtt, covered_hook, uncovered_hook],
            policy,
        )
        .await;
        m.attach_conversation_subscribers().await;
        let conversation = conversation_of(&m).await;

        // Every channel is subscribed and positioned; only the ACL differs.
        publish(&m, uuids[0], "mqtt-covered").await;
        publish(&m, uuids[1], "mqtt-uncovered").await;
        publish(&m, uuids[2], "webhook-covered").await;
        publish(&m, uuids[3], "webhook-uncovered").await;

        let delivery = m.conversation_delivery(conversation).await;
        assert_eq!(
            delivery
                .messages
                .iter()
                .map(|e| e.body.as_str())
                .collect::<Vec<_>>(),
            vec!["mqtt-covered", "webhook-covered"],
            "each address is decided by its own transport's matchers"
        );
    }

    /// The other half of the same rule: the denial skips the channel and moves
    /// nothing, so restoring the ACL serves everything published in between,
    /// bounded by retention. Authorization is a read-time fact — the position
    /// moves only by its owner's advance.
    ///
    /// This is the half a regression destroys invisibly: a denial that skipped
    /// *and* advanced passes `a_revoked_acl_stops_delivery` unchanged and eats
    /// the backlog silently.
    #[tokio::test]
    async fn a_restored_acl_serves_the_backlog_the_denial_held() {
        let ch = channel("chat", Depth::Bounded(5));
        let uuid = ch.uuid;
        let m = messenger(vec![ch]).await;
        m.attach_conversation_subscribers().await;
        let conversation = conversation_of(&m).await;
        publish(&m, uuid, "before-revocation").await;

        let revoked = revoked_messenger(&m);
        publish(&m, uuid, "during-revocation").await;
        assert!(
            revoked.conversation_delivery(conversation).await.is_empty(),
            "nothing is served while the policy does not cover the channel"
        );

        // `m` is the same app with its coverage back: both messages are still
        // ahead of the position the denial declined to move.
        let delivery = m.conversation_delivery(conversation).await;
        assert_eq!(
            delivery
                .messages
                .iter()
                .map(|e| e.body.as_str())
                .collect::<Vec<_>>(),
            vec!["before-revocation", "during-revocation"],
            "a restored ACL is a window over the history the subscriber missed"
        );
    }
}
