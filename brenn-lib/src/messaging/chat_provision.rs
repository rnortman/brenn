//! Making a conversation's chat channels exist, and unmaking them.
//!
//! Every other channel family in the process is declared: `[[channel]]` rows,
//! webhook endpoints, surface descriptions. A conversation's chat family cannot
//! be, because the conversation id that terminates each name is minted at
//! runtime. So this is the durable-channel creation path that is not boot: it
//! upserts the `.in`/`.out` rows, registers the `.stream`/`.wake` rings, and
//! folds all four into the directory.
//!
//! **The channels outlive the bridge, deliberately.** A peer wakes a dormant
//! conversation by publishing a command, and a publish to a name the directory
//! does not hold is refused — nothing is created on publish. So the family has
//! to be there while the conversation sleeps, which means provisioning happens
//! at conversation creation and at boot for every existing conversation,
//! never lazily at first use.
//!
//! Provisioning is server-initiated only: the sites that call it are the ones
//! that create a conversation. No publish reaches it, so it does not open the
//! mint-a-channel-to-widen-my-budget hole that the per-`(sender, channel)`
//! send-rate gate warns dynamic creation would.

use rusqlite::Connection;
use uuid::Uuid;

use crate::config::{ChatLeaf, LlmChatConfig, chat_address};
use crate::messaging::config::{Depth, ResolvedChannel, SendRate};
use crate::messaging::db::upsert_channels;
use crate::messaging::{
    ChannelEntry, ChannelScheme, Messenger, SubscriberEntry, SubscriberEntryKind, WakeMin,
    chat_channel_uuid_from_address, nondurable_channel_uuid,
};

/// Send rate on the record channel. Well above any turn's event traffic — a
/// completed message, a handful of status transitions, telemetry — while still
/// bounding a conversation stuck in a publish loop.
const OUT_SEND_RATE: SendRate = SendRate {
    burst: 2_000,
    refill_interval_secs: 1,
    refill: 200,
};

/// Send rate on the token stream. Effectively unlimited: a batch of five to ten
/// tokens is one publish, so a fast turn publishes tens per second for as long
/// as it generates, and throttling it would corrupt the stream rather than
/// protect anything — the ring's own depth is what bounds memory.
const STREAM_SEND_RATE: SendRate = SendRate {
    burst: 100_000,
    refill_interval_secs: 1,
    refill: 10_000,
};

/// Ring depth on the token stream. Sized as a turn: enough batches that a
/// consumer briefly behind still renders continuously, and overflow past it
/// drops the oldest partials, which the durable completed message supersedes
/// anyway.
const STREAM_RETAIN_DEPTH: Depth = Depth::Bounded(512);

/// Ring depth on the pre-warm channel. The body is ignored and the message's
/// existence is the whole signal, so retaining a window of them buys nothing;
/// a handful is enough to keep a burst of wake-word fires from evicting each
/// other before the pass that reads them.
const WAKE_RETAIN_DEPTH: Depth = Depth::Bounded(16);

/// What a chat subscription pulls as context: nothing. The drain wants the
/// commands it has not executed, and a command it already ran is not context for
/// the next one — it is done.
const SUBSCRIPTION_RETAIN_DEPTH: Depth = Depth::Bounded(0);

/// The channel entry for one leaf of one conversation.
///
/// The per-leaf parameters are the whole point of the function: the record
/// channel's retained window *is* the conversation history, the command
/// channel's `wake_min` is what decides whether a command wakes a dormant
/// conversation or parks until something else does, and the two ephemeral
/// leaves are sized for loss rather than for record.
///
/// The two peer-facing leaves also carry the conversation's own subscription,
/// which is what puts them in front of the wake pass: a publish on either makes
/// the conversation's position deliverable, and the pass turns that into a
/// notify for a live bridge or a spawn for a sleeping one. Without the entry the
/// pass skips the channel outright and a command reaches a dormant conversation
/// never.
fn chat_channel_entry(
    chat: &LlmChatConfig,
    app_slug: &str,
    leaf: ChatLeaf,
    conversation_id: i64,
    defaults: &crate::messaging::config::MessagingGlobalConfig,
) -> ChannelEntry {
    let address = chat_address(&chat.prefix, app_slug, leaf, conversation_id);
    let scheme = leaf.scheme();
    let window = Depth::Bounded(u64::from(chat.retained_window));

    let (send_rate, retain_depth, wake_min, push_depth, noise) = match leaf {
        // Peer-authored commands: the default rate is the backstop against a
        // peer driving a conversation harder than a human ever would. The push
        // depth is pinned rather than defaulted — it bounds how many unseen
        // commands one drain can see, and every message here is a distinct
        // command, so coalescing them the way a signal channel coalesces
        // telemetry would execute the newest and discard the rest. The window
        // is the pin: retention holds no more than that, so a drain that sees a
        // window's worth sees everything there is, and a depth spelled
        // `Unbounded` instead would pin the channel against reaping forever.
        ChatLeaf::In => (
            defaults.default_send_rate,
            window,
            chat.wake_min,
            window,
            defaults.default_noise,
        ),
        // TODO(chat-history-on-demand): this window is the entire history a bus
        // peer can reach, so it has to be sized for the deepest read anyone
        // wants rather than for the traffic.
        ChatLeaf::Out => (
            OUT_SEND_RATE,
            window,
            defaults.default_wake_min,
            defaults.default_push_depth,
            defaults.default_noise,
        ),
        ChatLeaf::Stream => (
            STREAM_SEND_RATE,
            STREAM_RETAIN_DEPTH,
            defaults.default_wake_min,
            defaults.default_push_depth,
            defaults.default_noise,
        ),
        // Any message at all must wake: the point of the channel is paying the
        // spawn cost before there is input to justify it. Depth 1 because a
        // pre-warm carries no content — ten of them and one of them ask for the
        // same single thing, so coalescing them all into one activation is the
        // whole intent rather than a loss. Which is also why the noise rung is
        // pinned silent instead of inherited: on this one channel a passed-over
        // message is the design working, and an operator who turned the global
        // rung up wants to hear about real loss, not about this.
        ChatLeaf::Wake => (
            defaults.default_send_rate,
            WAKE_RETAIN_DEPTH,
            WakeMin::VeryLow,
            Depth::Bounded(1),
            crate::messaging::config::NoiseLevel::Silent,
        ),
        ChatLeaf::Approvals => panic!(
            "chat provisioning: the approvals leaf is reserved and unbuilt; \
             provisioning it would fix its parameters before the flow that uses it exists",
        ),
    };

    let uuid = match scheme {
        ChannelScheme::Brenn => chat_channel_uuid_from_address(&address),
        other => nondurable_channel_uuid(
            other,
            address
                .strip_prefix(other.prefix())
                .expect("chat_address stamps the leaf's own scheme prefix"),
        ),
    };

    // The conversation subscribes to what peers publish to it, and to nothing
    // else: it is the publisher on `.out` and `.stream`, and subscribing to its
    // own output would only wake it with its own voice.
    let subscribers = match leaf {
        ChatLeaf::In | ChatLeaf::Wake => vec![SubscriberEntry {
            kind: SubscriberEntryKind::ChatConversation {
                app_slug: app_slug.to_string(),
                conversation_id,
            },
            push_depth,
            retain_depth: SUBSCRIPTION_RETAIN_DEPTH,
            noise,
            // Waking this subscriber spawns a Claude Code subprocess, so the
            // threshold is what decides between spawning now and waiting for
            // whatever wakes the conversation next. It is the channel's own —
            // one number per leaf, the one an operator set.
            wake_min: Some(wake_min),
        }],
        ChatLeaf::Out | ChatLeaf::Stream | ChatLeaf::Approvals => Vec::new(),
    };

    ChannelEntry {
        uuid,
        address,
        description: None,
        resolved_channel: ResolvedChannel {
            send_rate,
            push_depth,
            retain_depth,
            standing_retain_depth: retain_depth,
            noise,
            sink: defaults.default_sink,
            wake_min,
        },
        subscribers,
        transport_type: scheme,
        mount: None,
    }
}

/// The leaves provisioned today. `Approvals` is reserved: its name is fixed so
/// it cannot collide later, but nothing publishes or subscribes to it, and a
/// channel with no traffic is a row to reap rather than a feature.
const PROVISIONED_LEAVES: [ChatLeaf; 4] = [
    ChatLeaf::In,
    ChatLeaf::Out,
    ChatLeaf::Stream,
    ChatLeaf::Wake,
];

impl Messenger {
    /// Give a conversation its chat channels, idempotently.
    ///
    /// Called wherever a conversation is created, and by
    /// [`Messenger::backfill_conversation_chat_channels`] at boot for the ones
    /// that already existed. Repeat calls are free: the call returns without
    /// touching the database. That matters beyond
    /// tidiness — the already-provisioned case is the common one (every
    /// automation fire makes the call), and a conversation re-provisioned while
    /// a peer is mid-turn must not lose its retained record or reset a
    /// subscriber's position.
    ///
    /// Returns the channel entries the conversation now has, in
    /// [`PROVISIONED_LEAVES`] order, whether this call created them or found
    /// them.
    ///
    /// The caller supplies the connection because conversation creation already
    /// holds one; taking it here would deadlock against the caller's guard.
    pub fn provision_conversation_chat_channels(
        &self,
        conn: &Connection,
        app_slug: &str,
        conversation_id: i64,
    ) -> Vec<ChannelEntry> {
        let entries: Vec<ChannelEntry> = PROVISIONED_LEAVES
            .iter()
            .map(|leaf| {
                chat_channel_entry(
                    &self.llm_chat,
                    app_slug,
                    *leaf,
                    conversation_id,
                    &self.defaults,
                )
            })
            .collect();

        // The rows, the rings, and the directory entries are made in one pass,
        // so a directory that answers for every leaf means all three are already
        // there. Asking it is four in-memory lookups; the alternative is a write
        // statement per durable leaf on the connection everything else shares.
        if entries
            .iter()
            .all(|e| self.directory.by_uuid(&e.uuid).is_some())
        {
            return entries;
        }

        let durable: Vec<ChannelEntry> = entries
            .iter()
            .filter(|e| e.capabilities().durable)
            .cloned()
            .collect();
        upsert_channels(conn, &durable);
        ensure_command_cursor(conn, &entries, &self.llm_chat, app_slug, conversation_id);

        for entry in &entries {
            if !entry.capabilities().durable {
                self.ring_stores.register(entry);
                ensure_ring_cursor(&self.ring_stores, entry, app_slug, conversation_id);
            }
            if self.directory.by_uuid(&entry.uuid).is_none() {
                self.directory.add_channel(entry.clone());
            }
        }
        entries
    }

    /// Take a conversation's chat channels away, and everything that lived in
    /// them.
    ///
    /// Conversation deletion owns this: the channel names carry the conversation
    /// id, so once the conversation is gone nothing can ever address them again,
    /// and leaving them behind leaks a directory entry, a retained window, and a
    /// row per subscriber cursor per conversation ever deleted.
    ///
    /// TODO(chat-deletion-teardown): no conversation-deletion path exists yet,
    /// so nothing calls this; the one that gets built has to.
    ///
    /// The durable half is atomic — cursors, pending pushes, and messages all
    /// reference the channel row, so a partial teardown leaves dangling
    /// references. A caller that already has a transaction open on this
    /// connection gets its statements inside that one; a caller that has none
    /// gets a transaction of its own.
    ///
    /// Returns the number of channels removed.
    pub fn deprovision_conversation_chat_channels(
        &self,
        conn: &Connection,
        app_slug: &str,
        conversation_id: i64,
    ) -> usize {
        let entries: Vec<ChannelEntry> = PROVISIONED_LEAVES
            .iter()
            .map(|leaf| {
                chat_channel_entry(
                    &self.llm_chat,
                    app_slug,
                    *leaf,
                    conversation_id,
                    &self.defaults,
                )
            })
            .collect();

        let durable: Vec<Uuid> = entries
            .iter()
            .filter(|e| e.capabilities().durable)
            .map(|e| e.uuid)
            .collect();
        delete_durable_channels(conn, &durable);

        let mut removed = 0;
        for entry in &entries {
            if !entry.capabilities().durable {
                self.ring_stores.deregister(&entry.uuid);
            }
            if self.directory.remove_channel(&entry.uuid) {
                removed += 1;
            }
        }
        removed
    }

    /// Provision every existing conversation's chat channels at boot.
    ///
    /// The durable rows survive restart but the directory and the rings do not,
    /// so a conversation created before this boot has no reachable channels
    /// until something re-registers them. Rather
    /// than special-casing "old" conversations, this walks them all: the call is
    /// idempotent, and one already registered this boot costs a lookup.
    ///
    /// Conversations belonging to an app that is no longer in config are
    /// skipped. Their channels would be unreachable anyway — the authority to
    /// publish or subscribe on a chat subtree is derived from the owning app's
    /// policy, and an app with no config has no policy.
    ///
    /// Returns the number of conversations provisioned.
    pub fn backfill_conversation_chat_channels(&self, conn: &Connection) -> usize {
        let rows: Vec<(i64, String)> = {
            let mut stmt = conn
                .prepare("SELECT id, app_slug FROM conversations ORDER BY id")
                .expect("chat backfill: prepare conversation scan");
            stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .expect("chat backfill: query conversations")
                .map(|r| r.expect("chat backfill: decode conversation row"))
                .collect()
        };

        let mut provisioned = 0;
        for (id, app_slug) in rows {
            if !self.apps.contains_key(&app_slug) {
                continue;
            }
            self.provision_conversation_chat_channels(conn, &app_slug, id);
            provisioned += 1;
        }
        if provisioned > 0 {
            tracing::info!(
                conversations = provisioned,
                "chat channels provisioned at boot"
            );
        }
        provisioned
    }
}

/// Give the conversation its position on its own command channel, at the head
/// of that channel as it stands.
///
/// The position is created with the channel rather than at the adapter's first
/// attach, and that is the difference between a dormant conversation's commands
/// waiting and vanishing: a position primed at head is owed only what is
/// published after it exists, so one created at first spawn classifies
/// everything published while the conversation slept as pre-history. Created
/// against the empty channel, every command the conversation will ever be sent
/// is owed to it. Idempotent — an existing position keeps its place.
fn ensure_command_cursor(
    conn: &Connection,
    entries: &[ChannelEntry],
    chat: &LlmChatConfig,
    app_slug: &str,
    conversation_id: i64,
) {
    let address = chat_address(&chat.prefix, app_slug, ChatLeaf::In, conversation_id);
    let entry = entries
        .iter()
        .find(|e| e.address == address)
        .expect("chat provisioning: the command leaf is provisioned");
    let head = crate::messaging::db::channel_last_retained_seq(conn, entry.uuid) + 1;
    crate::messaging::db::ensure_subscriber_cursor(
        conn,
        entry.uuid,
        &crate::messaging::ParticipantId::for_conversation(conversation_id),
        app_slug,
        entry.resolved_channel.push_depth,
        head,
    );
}

/// Give the conversation its position on a ring-backed leaf it subscribes to.
///
/// A ring's cursors live in memory, so they are made with the ring rather than
/// restored with it: every boot re-registers the ring and re-attaches here. The
/// durable half of the same job is [`ensure_command_cursor`], and the split is
/// only about where the position is kept.
///
/// `Head` for the same reason the command cursor takes it: a pre-warm published
/// before this process started is not a request to spawn now.
fn ensure_ring_cursor(
    rings: &crate::messaging::store::RingStores,
    entry: &ChannelEntry,
    app_slug: &str,
    conversation_id: i64,
) {
    debug_assert!(
        entry.subscribers.len() <= 1,
        "chat provisioning: {} carries {} subscribers — a chat leaf has the conversation's \
         subscription or none",
        entry.address,
        entry.subscribers.len()
    );
    let Some(subscriber) = entry.subscribers.first() else {
        return;
    };
    let depth = crate::messaging::store::depth_bound(subscriber.push_depth);
    let ring = rings.get(&entry.uuid).unwrap_or_else(|| {
        panic!(
            "chat provisioning: ring for {} was just registered and must be there",
            entry.address
        )
    });
    ring.attach(
        &crate::messaging::ParticipantId::for_conversation(conversation_id),
        app_slug,
        depth,
        crate::messaging::store::Priming::Head,
    );
}

/// Delete durable channels and everything that references them, in one
/// transaction.
///
/// Order is by foreign key: the rows pointing at the channel go before the
/// channel row. Cursors cascade on delete, but they are removed explicitly
/// anyway — the cascade is a schema property that a later migration could
/// change, and this must not quietly start leaking if it does.
fn delete_durable_channels(conn: &Connection, channel_uuids: &[Uuid]) {
    if channel_uuids.is_empty() {
        return;
    }
    // A caller deleting the conversation itself has its own transaction open on
    // this connection, and SQLite has no nested `BEGIN` — so the statements run
    // inside that transaction, which is the atomicity the caller wanted. Only a
    // transactionless caller opens one here, and its guard rolls the deletes
    // back if a statement panics part-way: an unwinding task does not end the
    // process, and an abandoned open transaction would poison whatever ran on
    // this connection next.
    if conn.is_autocommit() {
        let tx =
            rusqlite::Transaction::new_unchecked(conn, rusqlite::TransactionBehavior::Immediate)
                .expect("chat teardown: begin transaction");
        delete_channel_rows(&tx, channel_uuids);
        tx.commit().expect("chat teardown: commit transaction");
    } else {
        delete_channel_rows(conn, channel_uuids);
    }
}

/// The teardown statements themselves, ordered by foreign key.
fn delete_channel_rows(conn: &Connection, channel_uuids: &[Uuid]) {
    for uuid in channel_uuids {
        let bytes = uuid.as_bytes().to_vec();
        for sql in [
            "DELETE FROM messaging_pending_pushes WHERE message_id IN \
             (SELECT id FROM messaging_messages WHERE channel_uuid = ?1)",
            "DELETE FROM messaging_subscriber_cursors WHERE channel_uuid = ?1",
            "DELETE FROM messaging_dynamic_subscriptions WHERE channel_uuid = ?1",
            "DELETE FROM messaging_messages WHERE channel_uuid = ?1",
            "DELETE FROM messaging_channels WHERE uuid = ?1",
        ] {
            conn.execute(sql, rusqlite::params![bytes])
                .unwrap_or_else(|e| panic!("chat teardown: {sql} failed for channel {uuid}: {e}"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use indexmap::IndexMap;

    use crate::config::{AppConfig, chat_bare_name};
    use crate::db::init_db_memory;
    use crate::messaging::config::MessagingGlobalConfig;
    use crate::messaging::store::RingStores;
    use crate::messaging::{
        MessageEnvelope, MessagingDirectory, ParticipantId, SubscriberEntryKind, Urgency,
        WakeRouter,
    };

    const APP: &str = "pa-bob";

    /// Records what the wake pass asked for rather than doing it. The pass's
    /// output *is* the ask — whether a sleeping conversation gets a subprocess
    /// bought for it — and the subprocess itself lives a crate away.
    #[derive(Debug, Default)]
    struct RecordingRouter {
        wakes: std::sync::Mutex<Vec<(SubscriberEntryKind, String)>>,
    }

    impl RecordingRouter {
        fn wakes(&self) -> Vec<(SubscriberEntryKind, String)> {
            self.wakes.lock().expect("wakes lock poisoned").clone()
        }
    }

    #[async_trait::async_trait]
    impl WakeRouter for RecordingRouter {
        async fn deliver(
            &self,
            _key: &SubscriberEntryKind,
            _envelope: &Arc<MessageEnvelope>,
            _retained_seq: i64,
        ) -> Result<bool, String> {
            Ok(false)
        }
        async fn deliver_ingress(
            &self,
            _key: &SubscriberEntryKind,
            _subscriber: &ParticipantId,
            _event: &crate::messaging::ingress::Event,
        ) -> Result<bool, String> {
            Ok(false)
        }
        fn spawn_eager_wake(&self, key: &SubscriberEntryKind, subscriber: &ParticipantId) {
            self.wakes
                .lock()
                .expect("wakes lock poisoned")
                .push((key.clone(), subscriber.as_str().to_string()));
        }
        fn delivery_shape(&self, key: &SubscriberEntryKind) -> crate::messaging::DeliveryShape {
            crate::messaging::default_delivery_shape(key)
        }
        fn alarm(&self, _channel: &str, _subscriber: &ParticipantId, _count: u64) {}
    }

    /// A messenger with one LLM app, no declared channels, and the default chat
    /// config — the shape a deployment has before any conversation exists.
    async fn messenger() -> (Arc<Messenger>, crate::db::Db) {
        let (messenger, db, _) = messenger_watching_wakes().await;
        (messenger, db)
    }

    /// The same messenger, with the router it was built on handed back so a test
    /// can read what the wake pass asked for.
    async fn messenger_watching_wakes() -> (Arc<Messenger>, crate::db::Db, Arc<RecordingRouter>) {
        let db = init_db_memory();
        {
            let conn = db.lock().await;
            conn.execute(
                "INSERT INTO users (id, username, password_hash, created_at) \
                 VALUES (1, 'bob', 'h', '2024-01-01')",
                [],
            )
            .unwrap();
        }

        let mut apps: IndexMap<String, AppConfig> = IndexMap::new();
        let mut app = crate::messaging::test_support::test_app_config(
            APP,
            Some(crate::messaging::config::ResolvedMessagingConfig {
                send_budget: 100,
                subscriptions: vec![],
            }),
            vec!["bob".to_string()],
        );
        app.policy = crate::access::AppPolicy::default();
        LlmChatConfig::default().grant_app_chat_tree(APP, &mut app.policy);
        apps.insert(APP.to_string(), app);

        let router = Arc::new(RecordingRouter::default());
        let messenger = Messenger::new(
            db.clone(),
            Arc::new(MessagingDirectory::with_entries(vec![])),
            Arc::from("test-source"),
            Arc::new(apps),
            router.clone() as Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        )
        .with_ring_stores(Arc::new(RingStores::empty()))
        .with_llm_chat(LlmChatConfig::default());
        (messenger, db, router)
    }

    /// Insert a conversation row directly; provisioning is what the tests drive.
    fn insert_conversation(conn: &Connection, id: i64, app_slug: &str) {
        conn.execute(
            "INSERT INTO conversations (id, user_id, status, app_slug, created_at, updated_at) \
             VALUES (?1, 1, 'active', ?2, '2024-01-01', '2024-01-01')",
            rusqlite::params![id, app_slug],
        )
        .unwrap();
    }

    /// The four leaves land, with the right scheme and the right store class on
    /// each, and every one is resolvable by the name the grammar mints.
    #[tokio::test]
    async fn provisioning_makes_all_four_leaves_resolvable() {
        let (m, db) = messenger().await;
        let conn = db.lock().await;
        insert_conversation(&conn, 7, APP);

        let entries = m.provision_conversation_chat_channels(&conn, APP, 7);
        assert_eq!(entries.len(), 4);

        for leaf in PROVISIONED_LEAVES {
            let address = chat_address("chat", APP, leaf, 7);
            let entry = m
                .directory
                .resolve(&address)
                .unwrap_or_else(|| panic!("{address} must be in the directory"));
            assert_eq!(entry.transport_type, leaf.scheme());
            assert_eq!(
                m.ring_stores.get(&entry.uuid).is_some(),
                !entry.capabilities().durable,
                "{address}: a ring exists iff the channel is non-durable",
            );
        }
        assert_eq!(m.ring_stores.len(), 2, "stream and wake, not in and out");

        // The reserved leaf is not minted — its parameters stay undecided.
        assert!(
            m.directory
                .resolve(&chat_address("chat", APP, ChatLeaf::Approvals, 7))
                .is_none()
        );
    }

    /// The durable half is a row, so it survives the process; the row's UUID is
    /// the one the address derives, which is what makes a later boot find it
    /// rather than mint a second channel with the same name.
    #[tokio::test]
    async fn the_durable_leaves_get_rows_keyed_by_their_address() {
        let (m, db) = messenger().await;
        let conn = db.lock().await;
        insert_conversation(&conn, 7, APP);
        m.provision_conversation_chat_channels(&conn, APP, 7);

        for leaf in [ChatLeaf::In, ChatLeaf::Out] {
            let address = chat_address("chat", APP, leaf, 7);
            let stored: Vec<u8> = conn
                .query_row(
                    "SELECT uuid FROM messaging_channels WHERE address = ?1",
                    rusqlite::params![&address],
                    |r| r.get(0),
                )
                .unwrap_or_else(|e| panic!("{address} must have a row: {e}"));
            assert_eq!(
                Uuid::from_slice(&stored).unwrap(),
                chat_channel_uuid_from_address(&address),
            );
        }
        let ephemeral_rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM messaging_channels WHERE address LIKE 'ephemeral:%'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(ephemeral_rows, 0, "a ring-backed channel has no row");
    }

    /// The record channel carries the configured window and the command channel
    /// carries the configured wake threshold — the two parameters an operator
    /// sets and everything downstream reads.
    #[tokio::test]
    async fn the_configured_window_and_threshold_reach_the_channels() {
        let (m, db) = messenger().await;
        let conn = db.lock().await;
        insert_conversation(&conn, 7, APP);
        m.provision_conversation_chat_channels(&conn, APP, 7);

        let out = m
            .directory
            .resolve(&chat_address("chat", APP, ChatLeaf::Out, 7))
            .expect("provisioned");
        assert_eq!(
            out.resolved_channel.retain_depth,
            Depth::Bounded(u64::from(LlmChatConfig::default().retained_window)),
        );

        let inbound = m
            .directory
            .resolve(&chat_address("chat", APP, ChatLeaf::In, 7))
            .expect("provisioned");
        assert_eq!(
            inbound.resolved_channel.wake_min,
            LlmChatConfig::default().wake_min
        );

        let wake = m
            .directory
            .resolve(&chat_address("chat", APP, ChatLeaf::Wake, 7))
            .expect("provisioned");
        assert_eq!(
            wake.resolved_channel.wake_min,
            WakeMin::VeryLow,
            "any pre-warm message must wake a dormant conversation"
        );
    }

    /// Re-provisioning is what boot does to a conversation that is already
    /// there, so it must not disturb the record or the ring.
    #[tokio::test]
    async fn re_provisioning_preserves_the_record_and_the_ring() {
        let (m, db) = messenger().await;
        let out = chat_address("chat", APP, ChatLeaf::Out, 7);
        let stream = chat_address("chat", APP, ChatLeaf::Stream, 7);
        {
            let conn = db.lock().await;
            insert_conversation(&conn, 7, APP);
            m.provision_conversation_chat_channels(&conn, APP, 7);
        }

        for addr in [&out, &stream] {
            let result = m
                .publish_from_conversation(7, APP, addr, "body", Urgency::Normal)
                .await;
            assert!(
                matches!(result, crate::messaging::publish::PublishResult::Ok { .. }),
                "{addr}: {result:?}"
            );
        }
        let ring_before = m
            .ring_stores
            .get_by_address(&stream)
            .expect("stream is ring-backed");

        {
            let conn = db.lock().await;
            m.provision_conversation_chat_channels(&conn, APP, 7);
            let messages: i64 = conn
                .query_row("SELECT COUNT(*) FROM messaging_messages", [], |r| r.get(0))
                .unwrap();
            assert_eq!(messages, 1, "the record survives re-provisioning");
        }
        assert!(
            Arc::ptr_eq(
                &ring_before,
                &m.ring_stores.get_by_address(&stream).expect("still there"),
            ),
            "the stream ring survives re-provisioning",
        );
        assert_eq!(m.directory.list().len(), 4, "no channel is duplicated");
    }

    /// The already-provisioned case is the common one — every automation fire
    /// makes the call — so it must not reach the database at all.
    #[tokio::test]
    async fn re_provisioning_does_not_write() {
        let (m, db) = messenger().await;
        let conn = db.lock().await;
        insert_conversation(&conn, 7, APP);

        m.provision_conversation_chat_channels(&conn, APP, 7);
        let writes = conn.total_changes();
        assert!(writes > 0, "the first call writes the durable rows");

        let entries = m.provision_conversation_chat_channels(&conn, APP, 7);
        assert_eq!(conn.total_changes(), writes, "a repeat call writes nothing");
        assert_eq!(entries.len(), 4, "and still answers with the whole family");
    }

    /// Every row-count teardown asserts must be non-zero before the call, or the
    /// assertion holds whether or not the statement that clears it exists.
    fn table_counts(conn: &Connection) -> (i64, i64, i64) {
        let count = |sql: &str| conn.query_row(sql, [], |r| r.get::<_, i64>(0)).unwrap();
        (
            count("SELECT COUNT(*) FROM messaging_channels"),
            count("SELECT COUNT(*) FROM messaging_messages"),
            count("SELECT COUNT(*) FROM messaging_subscriber_cursors"),
        )
    }

    /// Teardown removes the names, the rows, and the ring. What it must leave is
    /// a directory that no longer answers for a conversation that is gone.
    #[tokio::test]
    async fn teardown_removes_the_channels_and_their_contents() {
        let (m, db) = messenger().await;
        {
            let conn = db.lock().await;
            insert_conversation(&conn, 7, APP);
            m.provision_conversation_chat_channels(&conn, APP, 7);
        }
        // A second cursor beside the one provisioning makes, so the cursor
        // assertion below is about the delete rather than about an empty table.
        m.attach_subscriber(
            &chat_address("chat", APP, ChatLeaf::Out, 7),
            APP,
            &ParticipantId::for_app(APP, "test-source"),
            Depth::Unbounded,
            crate::messaging::store::Priming::Head,
        )
        .await;
        let result = m
            .publish_from_conversation(
                7,
                APP,
                &chat_address("chat", APP, ChatLeaf::Out, 7),
                "body",
                Urgency::Normal,
            )
            .await;
        assert!(matches!(
            result,
            crate::messaging::publish::PublishResult::Ok { .. }
        ));
        {
            let conn = db.lock().await;
            let (channels, messages, cursors) = table_counts(&conn);
            assert_eq!(channels, 2, "the two durable leaves have rows");
            assert_eq!(messages, 1, "the record holds the published message");
            assert_eq!(
                cursors, 2,
                "the command cursor and the attached subscriber both hold positions"
            );
        }

        let removed = {
            let conn = db.lock().await;
            let removed = m.deprovision_conversation_chat_channels(&conn, APP, 7);
            assert_eq!(table_counts(&conn), (0, 0, 0));
            removed
        };
        assert_eq!(removed, 4);
        assert!(m.ring_stores.is_empty());
        assert!(m.directory.list().is_empty());

        // Nothing answers for the conversation any more.
        let result = m
            .publish_from_conversation(
                7,
                APP,
                &chat_address("chat", APP, ChatLeaf::Out, 7),
                "body",
                Urgency::Normal,
            )
            .await;
        assert!(
            matches!(
                result,
                crate::messaging::publish::PublishResult::UnknownChannel(_)
            ),
            "{result:?}"
        );
    }

    /// Teardown is scoped to the one conversation. A sibling conversation of the
    /// same app keeps everything.
    #[tokio::test]
    async fn teardown_leaves_sibling_conversations_alone() {
        let (m, db) = messenger().await;
        let conn = db.lock().await;
        insert_conversation(&conn, 7, APP);
        insert_conversation(&conn, 8, APP);
        m.provision_conversation_chat_channels(&conn, APP, 7);
        m.provision_conversation_chat_channels(&conn, APP, 8);
        assert_eq!(m.directory.list().len(), 8);

        m.deprovision_conversation_chat_channels(&conn, APP, 7);

        assert_eq!(m.directory.list().len(), 4);
        assert_eq!(m.ring_stores.len(), 2);
        for leaf in PROVISIONED_LEAVES {
            assert!(
                m.directory
                    .resolve(&chat_address("chat", APP, leaf, 8))
                    .is_some(),
                "conversation 8 keeps its {leaf:?} channel",
            );
        }
    }

    /// Boot walks every conversation, including ones from earlier boots, and
    /// skips the ones whose app is no longer configured — those have
    /// no policy, so their channels would be unreachable.
    #[tokio::test]
    async fn the_boot_backfill_covers_configured_apps_only() {
        let (m, db) = messenger().await;
        let conn = db.lock().await;
        insert_conversation(&conn, 1, APP);
        insert_conversation(&conn, 2, APP);
        insert_conversation(&conn, 3, "app-that-was-removed");

        assert_eq!(m.backfill_conversation_chat_channels(&conn), 2);
        assert_eq!(m.directory.list().len(), 8);
        assert!(
            m.directory
                .resolve(&chat_address(
                    "chat",
                    "app-that-was-removed",
                    ChatLeaf::In,
                    3
                ))
                .is_none()
        );

        // A second boot over the same database changes nothing.
        assert_eq!(m.backfill_conversation_chat_channels(&conn), 2);
        assert_eq!(m.directory.list().len(), 8);
    }

    /// A command published while the conversation has never had a bridge is
    /// owed to it, because the position exists from provisioning rather than
    /// from the adapter's first attach.
    #[tokio::test]
    async fn a_command_published_to_a_dormant_conversation_is_owed() {
        let (m, db) = messenger().await;
        let address = chat_address("chat", APP, ChatLeaf::In, 7);
        {
            let conn = db.lock().await;
            insert_conversation(&conn, 7, APP);
            m.provision_conversation_chat_channels(&conn, APP, 7);
        }

        let result = m
            .publish_from_conversation(7, APP, &address, "command", Urgency::Normal)
            .await;
        assert!(
            matches!(result, crate::messaging::publish::PublishResult::Ok { .. }),
            "{result:?}"
        );

        // No attach in between: this is what an adapter starting for the first
        // time would see.
        let window = m
            .store_for_address(&address)
            .window(
                &ParticipantId::for_conversation(7),
                Depth::Unbounded,
                Depth::Bounded(0),
            )
            .await
            .expect("provisioning gave the conversation its command position");
        assert_eq!(
            window.new_entries().len(),
            1,
            "a command published while dormant is unseen, not pre-history",
        );
    }

    /// The command channel's push depth is what bounds one drain pass, and every
    /// message on it is a distinct command — so it is pinned to the whole
    /// retained window, not inherited from an operator's global coalescing
    /// default. The window and not `Unbounded`: retention holds no more than the
    /// window anyway, and an unbounded depth would pin the channel against
    /// reaping.
    #[tokio::test]
    async fn the_command_channel_keeps_its_push_depth_when_the_global_default_drops() {
        let (m, db) = messenger().await;
        let defaults = MessagingGlobalConfig {
            default_push_depth: Depth::Bounded(1),
            ..MessagingGlobalConfig::default()
        };
        let window = Depth::Bounded(u64::from(LlmChatConfig::default().retained_window));
        let entry = chat_channel_entry(&LlmChatConfig::default(), APP, ChatLeaf::In, 7, &defaults);
        assert_eq!(
            entry.resolved_channel.push_depth, window,
            "commands must not coalesce",
        );
        assert_eq!(
            entry.reap_frontier(),
            Some(u64::from(LlmChatConfig::default().retained_window)),
            "and the channel is still reapable at its window",
        );

        // And that is the depth the channel actually carries.
        let conn = db.lock().await;
        insert_conversation(&conn, 7, APP);
        m.provision_conversation_chat_channels(&conn, APP, 7);
        let provisioned = m
            .directory
            .resolve(&chat_address("chat", APP, ChatLeaf::In, 7))
            .expect("provisioned");
        assert_eq!(provisioned.resolved_channel.push_depth, window);
        assert_eq!(
            provisioned.subscribers[0].push_depth, window,
            "and the subscription reads the same window the channel promises",
        );
    }

    /// The conversation subscribes to what peers send it and to nothing it says
    /// itself. The subscription is what puts a leaf in front of the wake pass, so
    /// this is also the statement of which publishes can wake a conversation.
    #[tokio::test]
    async fn only_the_peer_facing_leaves_carry_the_conversations_subscription() {
        let (m, db) = messenger().await;
        let conn = db.lock().await;
        insert_conversation(&conn, 7, APP);
        m.provision_conversation_chat_channels(&conn, APP, 7);

        let expected = SubscriberEntryKind::ChatConversation {
            app_slug: APP.to_string(),
            conversation_id: 7,
        };
        // `.in` serves the whole retained window because every message on it is a
        // distinct command; `.wake` serves one because ten wake-word fires all
        // ask for the same single thing. Neither is the global coalescing
        // default.
        let depths = [
            (
                ChatLeaf::In,
                Depth::Bounded(u64::from(LlmChatConfig::default().retained_window)),
            ),
            (ChatLeaf::Wake, Depth::Bounded(1)),
        ];
        for (leaf, push_depth) in depths {
            let entry = m
                .directory
                .resolve(&chat_address("chat", APP, leaf, 7))
                .expect("provisioned");
            assert_eq!(entry.subscribers.len(), 1, "{leaf:?}");
            let sub = &entry.subscribers[0];
            assert_eq!(sub.kind, expected, "{leaf:?}");
            assert_eq!(
                sub.push_depth, push_depth,
                "{leaf:?}: the coalescing this leaf's traffic asks for",
            );
            assert_eq!(
                sub.wake_min,
                Some(entry.resolved_channel.wake_min),
                "{leaf:?}: the subscription is woken by the threshold the channel declares",
            );
        }

        for leaf in [ChatLeaf::Out, ChatLeaf::Stream] {
            let entry = m
                .directory
                .resolve(&chat_address("chat", APP, leaf, 7))
                .expect("provisioned");
            assert!(
                entry.subscribers.is_empty(),
                "{leaf:?}: a conversation must not wake itself with its own voice",
            );
        }

        let wake = m
            .directory
            .resolve(&chat_address("chat", APP, ChatLeaf::Wake, 7))
            .expect("provisioned");
        assert_eq!(
            wake.subscribers[0].noise,
            crate::messaging::config::NoiseLevel::Silent,
            "coalescing pre-warms is the design, not loss to report",
        );
    }

    /// The point of the whole registration: a command published to a conversation
    /// with no bridge makes the wake pass buy it one.
    #[tokio::test]
    async fn a_command_to_a_dormant_conversation_buys_it_a_bridge() {
        let (m, db, router) = messenger_watching_wakes().await;
        {
            let conn = db.lock().await;
            insert_conversation(&conn, 7, APP);
            m.provision_conversation_chat_channels(&conn, APP, 7);
        }
        let address = chat_address("chat", APP, ChatLeaf::In, 7);
        let result = m
            .publish_from_conversation(7, APP, &address, "a command", Urgency::Normal)
            .await;
        assert!(
            matches!(result, crate::messaging::publish::PublishResult::Ok { .. }),
            "{result:?}"
        );

        m.wake_owed_subscribers(chrono::Utc::now()).await;
        assert_eq!(
            router.wakes(),
            vec![(
                SubscriberEntryKind::ChatConversation {
                    app_slug: APP.to_string(),
                    conversation_id: 7,
                },
                "conversation:7".to_string(),
            )],
            "the conversation's own subscription is what the pass woke",
        );
    }

    /// Below the threshold the command still lands and is still owed — it just
    /// does not buy a subprocess. That is the sender's choice, spelled as
    /// urgency, and the whole of the queue-until-active behavior.
    #[tokio::test]
    async fn a_quiet_command_waits_instead_of_spawning() {
        let (m, db, router) = messenger_watching_wakes().await;
        {
            let conn = db.lock().await;
            insert_conversation(&conn, 7, APP);
            m.provision_conversation_chat_channels(&conn, APP, 7);
        }
        let address = chat_address("chat", APP, ChatLeaf::In, 7);
        m.publish_from_conversation(7, APP, &address, "a quiet command", Urgency::Low)
            .await;

        m.wake_owed_subscribers(chrono::Utc::now()).await;
        assert!(router.wakes().is_empty(), "below wake_min buys no spawn");

        // Still owed, so the next activation serves it.
        let window = m
            .store_for_address(&address)
            .window(
                &ParticipantId::for_conversation(7),
                Depth::Bounded(10),
                Depth::Bounded(0),
            )
            .await
            .expect("the command position exists");
        assert_eq!(window.new_entries().len(), 1, "waiting, not dropped");
    }

    /// The pre-warm channel exists to pay the spawn cost before there is
    /// anything to say, so the quietest possible message on it still wakes.
    #[tokio::test]
    async fn the_quietest_pre_warm_still_buys_a_bridge() {
        let (m, db, router) = messenger_watching_wakes().await;
        {
            let conn = db.lock().await;
            insert_conversation(&conn, 7, APP);
            m.provision_conversation_chat_channels(&conn, APP, 7);
        }
        let address = chat_address("chat", APP, ChatLeaf::Wake, 7);
        let result = m
            .publish_from_conversation(7, APP, &address, "", Urgency::VeryLow)
            .await;
        assert!(
            matches!(result, crate::messaging::publish::PublishResult::Ok { .. }),
            "{result:?}"
        );

        m.wake_owed_subscribers(chrono::Utc::now()).await;
        assert_eq!(router.wakes().len(), 1, "any pre-warm wakes");
    }

    /// Boot judges every cursor row against the directory and deletes the ones no
    /// live subscription names. The conversation's command position is named by
    /// its own subscription — without one, boot would delete the very row a
    /// dormant conversation's commands were published against.
    #[tokio::test]
    async fn the_boot_reconcile_keeps_the_command_position() {
        let (m, db) = messenger().await;
        let address = chat_address("chat", APP, ChatLeaf::In, 7);
        {
            let conn = db.lock().await;
            insert_conversation(&conn, 7, APP);
            m.provision_conversation_chat_channels(&conn, APP, 7);
        }
        m.publish_from_conversation(7, APP, &address, "a command", Urgency::Normal)
            .await;

        let report = m.reconcile_subscriber_cursors(&[]).await;
        assert_eq!(
            report.orphans_removed, 0,
            "the command position is not an orphan",
        );

        let window = m
            .store_for_address(&address)
            .window(
                &ParticipantId::for_conversation(7),
                Depth::Bounded(10),
                Depth::Bounded(0),
            )
            .await
            .expect("the position survived the reconcile");
        assert_eq!(
            window.new_entries().len(),
            1,
            "and it still owes what was published while the conversation slept",
        );
    }

    /// Conversation deletion will own teardown, and it deletes in a transaction
    /// of its own. SQLite has no nested `BEGIN`, so the teardown must join that
    /// transaction rather than try to open a second one.
    #[tokio::test]
    async fn teardown_joins_a_transaction_the_caller_already_opened() {
        let (m, db) = messenger().await;
        let conn = db.lock().await;
        insert_conversation(&conn, 7, APP);
        m.provision_conversation_chat_channels(&conn, APP, 7);

        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let removed = m.deprovision_conversation_chat_channels(&conn, APP, 7);
        conn.execute_batch("COMMIT").unwrap();

        assert_eq!(removed, 4);
        assert_eq!(table_counts(&conn), (0, 0, 0));
    }

    /// The chat seam and the ambience-injection seam coexist without touching.
    /// A conversation's own command channel must never reach
    /// `conversation_delivery`, which is what the injection renderer drains: a
    /// command is to be *executed*, not narrated to CC as a system message, and
    /// the record replayed into the conversation it describes is the same
    /// mistake in the other direction.
    ///
    /// Both walks filter on `SubscriberEntryKind::App`, and provisioning happens
    /// not to author an `App` subscriber on a chat leaf — so without this the
    /// separation is carried by a doc comment and two `else { continue }` lines.
    #[tokio::test]
    async fn the_chat_tree_is_invisible_to_the_ambience_drain() {
        let (m, db) = messenger().await;
        {
            let conn = db.lock().await;
            insert_conversation(&conn, 7, APP);
            m.provision_conversation_chat_channels(&conn, APP, 7);
        }
        for leaf in [ChatLeaf::In, ChatLeaf::Out] {
            m.publish_from_conversation(
                7,
                APP,
                &chat_address("chat", APP, leaf, 7),
                "a command, and a record entry",
                Urgency::Normal,
            )
            .await;
        }

        let delivery = m.conversation_delivery(7).await;
        assert!(
            delivery.messages.is_empty(),
            "the conversation's own chat traffic is not ambience to render into it: {:?}",
            delivery.messages,
        );

        // And the attach walk authors no position on a chat leaf either, so a
        // later boot cannot start one draining.
        let before = cursor_count(&*db.lock().await);
        m.attach_conversation_subscribers().await;
        assert_eq!(
            cursor_count(&*db.lock().await),
            before,
            "the app-subscriber attach walk steps over the chat tree",
        );
    }

    /// Every cursor row on any channel.
    fn cursor_count(conn: &Connection) -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM messaging_subscriber_cursors",
            [],
            |r| r.get(0),
        )
        .unwrap()
    }

    /// A chat subscription belongs to one conversation, and the registration
    /// lookup matches on that id rather than on the app the cursor was written
    /// under. Sibling conversations of one app share an app slug and nothing
    /// else, so a slug-only match would resolve conversation 8's stray position
    /// on conversation 7's command channel to 7's registration — and the wake
    /// pass would then spawn 7 for traffic owed to 8.
    #[tokio::test]
    async fn a_siblings_cursor_on_this_command_channel_wakes_nobody() {
        let (m, db, router) = messenger_watching_wakes().await;
        {
            let conn = db.lock().await;
            insert_conversation(&conn, 7, APP);
            insert_conversation(&conn, 8, APP);
            m.provision_conversation_chat_channels(&conn, APP, 7);
            m.provision_conversation_chat_channels(&conn, APP, 8);
        }

        // A stray position for the sibling on conversation 7's command channel —
        // the same call the adapter makes for its own.
        let address = chat_address("chat", APP, ChatLeaf::In, 7);
        m.attach_subscriber(
            &address,
            APP,
            &ParticipantId::for_conversation(8),
            Depth::Bounded(10),
            crate::messaging::store::Priming::Head,
        )
        .await;
        m.publish_from_conversation(7, APP, &address, "a command", Urgency::Normal)
            .await;

        m.wake_owed_subscribers(chrono::Utc::now()).await;
        assert_eq!(
            router.wakes(),
            vec![(
                SubscriberEntryKind::ChatConversation {
                    app_slug: APP.to_string(),
                    conversation_id: 7,
                },
                "conversation:7".to_string(),
            )],
            "only the channel's own conversation is woken by what lands on it",
        );
    }

    /// Two apps' conversations are distinct channels even at the same id — the
    /// slug is a segment of the name, not decoration.
    #[tokio::test]
    async fn the_app_slug_separates_conversations_sharing_an_id() {
        let (m, db) = messenger().await;
        let conn = db.lock().await;
        insert_conversation(&conn, 7, APP);
        m.provision_conversation_chat_channels(&conn, APP, 7);
        m.provision_conversation_chat_channels(&conn, "pa-alice", 7);

        assert_eq!(m.directory.list().len(), 8);
        assert_ne!(
            chat_bare_name("chat", APP, ChatLeaf::In, 7),
            chat_bare_name("chat", "pa-alice", ChatLeaf::In, 7),
        );
    }
}
