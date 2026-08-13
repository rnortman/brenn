//! The per-app conversation roster: the state channel that tells a bus peer
//! which conversations exist.
//!
//! Grants over the chat tree are expressible at fleet grain — a prefix matcher
//! covers every conversation of an app, present and future — but `Subscribe` is
//! exact-channel, and a conversation id is minted at runtime. Without a roster
//! a peer holding a fleet grant has no way to learn what to subscribe to; with
//! one it reconciles.
//!
//! It is a **state channel**, per the bus's own pairing rule: each snapshot is
//! the whole current set and subsumes the one before it, so a peer that was
//! down reconciles against the newest instead of replaying every arrival and
//! departure it missed. Two consequences follow. The window is shallow — history
//! here has no readers. And the body is a pure function of the conversation
//! table, so two boots on unchanged state publish identical bytes and a peer can
//! compare rather than re-derive.
//!
//! One writer, always: the reserved `system:chat-roster` identity, which no
//! config can mint. The app's own harness policy deliberately stops at the
//! conversation leaves (`LlmChatConfig::harness_policy`), so nothing else can
//! reach the address.

use brenn_envelope::chat::{ChatRoster, RosterConversation, chat_roster_address, encode};
use rusqlite::Connection;

use crate::config::{Depth, MessagingGlobalConfig, ResolvedChannel};
use crate::{
    ChannelEntry, ChannelScheme, Messenger, PublishResult, Urgency, chat_channel_uuid_from_address,
};
use brenn_lib::config::LlmChatConfig;

/// System-participant component name of the roster writer; its identity is
/// `system:chat-roster`.
pub const CHAT_ROSTER_COMPONENT: &str = "chat-roster";

/// Standing window on a roster channel, in snapshots.
///
/// A state channel owes a reader exactly one message — the current one — so the
/// depth is about resume rather than history: a peer whose cursor is a few
/// snapshots behind resumes without a reported drop, and one further behind than
/// that reads the newest snapshot, which is the whole truth anyway. Deep enough
/// that ordinary churn does not manufacture drop counts, shallow enough that the
/// channel never becomes a log of conversation lifecycle events.
const ROSTER_RETAIN_DEPTH: Depth = Depth::Bounded(8);

/// The roster channel entry for one app.
///
/// Boot-declared rather than provisioned per conversation: apps are static
/// config, so an app's roster exists from boot and outlives every conversation
/// it lists — including the state of having none. No subscriber is pre-set; a
/// peer's subscription is minted by its own attach.
pub fn chat_roster_entry(
    chat: &LlmChatConfig,
    app_slug: &str,
    defaults: &MessagingGlobalConfig,
) -> ChannelEntry {
    let address = chat_roster_address(&chat.prefix, app_slug);
    ChannelEntry {
        uuid: chat_channel_uuid_from_address(&address),
        address,
        description: None,
        resolved_channel: ResolvedChannel {
            send_rate: defaults.default_send_rate,
            push_depth: ROSTER_RETAIN_DEPTH,
            retain_depth: ROSTER_RETAIN_DEPTH,
            standing_retain_depth: ROSTER_RETAIN_DEPTH,
            noise: defaults.default_noise,
            sink: defaults.default_sink,
            wake_min: defaults.default_wake_min,
        },
        subscribers: Vec::new(),
        transport_type: ChannelScheme::Brenn,
        mount: None,
    }
}

/// Read one app's conversation ids, ascending.
///
/// Ascending by id makes the body a function of the set alone: the same
/// conversations produce the same bytes whatever order the rows were written in.
fn conversation_ids(conn: &Connection, app_slug: &str) -> Vec<i64> {
    let mut stmt = conn
        .prepare("SELECT id FROM conversations WHERE app_slug = ?1 ORDER BY id")
        .expect("chat roster: prepare conversation scan");
    stmt.query_map([app_slug], |row| row.get(0))
        .expect("chat roster: query conversations")
        .map(|r| r.expect("chat roster: decode conversation row"))
        .collect()
}

impl Messenger {
    /// The app's roster snapshot body, as it stands in `conn`.
    ///
    /// Pure: it reads the conversation table and nothing else — no clock, no
    /// counter, no ordering that depends on when a row was written.
    pub fn chat_roster_body(&self, conn: &Connection, app_slug: &str) -> String {
        encode(&ChatRoster {
            conversations: conversation_ids(conn, app_slug)
                .into_iter()
                .map(|id| RosterConversation { id })
                .collect(),
        })
    }

    /// Publish the app's roster snapshot, unless it would say what the last one
    /// said.
    ///
    /// Returns the publish outcome, or `None` when nothing was published: an
    /// unchanged snapshot, an app that is not in config, or a messenger whose
    /// directory holds no roster channel for the app. The last case is a
    /// messenger built without the boot channel set — every configured app gets
    /// its roster entry at boot, so on a running server the address always
    /// resolves.
    ///
    /// **Deduplicated against the last published body**, because the callers are
    /// the provisioning sites, and provisioning is idempotent and called far more
    /// often than a conversation is created — every automation fire makes the
    /// call. Republishing an identical snapshot would wake every subscribed peer
    /// to tell it nothing.
    ///
    /// **One publisher at a time, from the table read through the publish.** A
    /// state channel's newest message is its whole truth, so two publishers whose
    /// reads and publishes interleave could leave an older set retained above a
    /// newer one — and the dedupe would then hold the newer body and suppress the
    /// correction until the conversation set changed again. Serializing the whole
    /// body is what rules that out; the call rate is boot plus provisioning, so
    /// one lock per messenger costs nothing.
    ///
    /// Takes the database lock itself, so a caller must not be holding it.
    pub async fn publish_chat_roster(&self, app_slug: &str) -> Option<PublishResult> {
        if !self.apps.contains_key(app_slug) {
            return None;
        }
        let address = chat_roster_address(&self.llm_chat.prefix, app_slug);
        self.directory.resolve(&address)?;

        let mut published = self.chat_roster_published.lock().await;

        let body = {
            let conn = self.db.lock().await;
            self.chat_roster_body(&conn, app_slug)
        };
        if published.get(app_slug) == Some(&body) {
            return None;
        }

        let result = self
            .publish_from_system(
                CHAT_ROSTER_COMPONENT,
                &address,
                &body,
                Urgency::Normal,
                None,
            )
            .await;
        if matches!(result, PublishResult::Ok { .. }) {
            // Claimed only once it is out there, which the serialization above
            // makes safe: no other publisher read the table in the meantime, so
            // this body is still the newest one.
            published.insert(app_slug.to_string(), body);
        }
        Some(result)
    }

    /// [`Messenger::publish_chat_roster`] for the runtime hooks: a refusal is
    /// logged rather than returned.
    ///
    /// The channel is boot-provisioned and the writer's policy is code-built, so
    /// there is no operator mistake behind a failure here and no caller that
    /// could act on one — a conversation was still created, and the peers that
    /// care learn about it on the next snapshot.
    pub async fn republish_chat_roster(&self, app_slug: &str) {
        if let Some(result) = self.publish_chat_roster(app_slug).await
            && !matches!(result, PublishResult::Ok { .. })
        {
            tracing::error!(
                app = %app_slug,
                ?result,
                "chat roster republish refused; the app's conversation list on the bus is stale \
                 until the next change"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use indexmap::IndexMap;

    use super::*;

    use crate::db::init_db_memory;
    use crate::query::NoopWakeRouter;
    use crate::store::RingStores;
    use crate::system::{SystemParticipantSpec, registrations_from_specs};
    use crate::{MessagingDirectory, WakeRouter};

    const APP: &str = "alice";
    const OTHER: &str = "bob";

    /// A messenger over the two configured apps, holding whichever channels the
    /// caller passes and the roster writer's own registration.
    ///
    /// `with_roster_channels` is what separates a booted server from a fixture
    /// that never declared one: the publish path resolves the address through
    /// the directory, so an empty one is the "nothing to publish onto" case.
    async fn messenger(with_roster_channels: bool) -> Arc<Messenger> {
        let db = init_db_memory();
        let chat = LlmChatConfig::default();
        let defaults = MessagingGlobalConfig::default();

        let mut apps: IndexMap<String, brenn_lib::config::AppConfig> = IndexMap::new();
        for slug in [APP, OTHER] {
            let mut app = crate::test_support::test_app_config(slug, None, vec![]);
            app.chat_harness_policy = chat.harness_policy(slug);
            apps.insert(slug.to_string(), app);
        }

        let entries: Vec<ChannelEntry> = if with_roster_channels {
            [APP, OTHER]
                .iter()
                .map(|slug| chat_roster_entry(&chat, slug, &defaults))
                .collect()
        } else {
            Vec::new()
        };
        let spec = SystemParticipantSpec::publish_only(
            CHAT_ROSTER_COMPONENT,
            ChannelScheme::Brenn,
            &entries
                .iter()
                .map(|e| {
                    e.address
                        .strip_prefix(ChannelScheme::Brenn.prefix())
                        .expect("a roster address is a brenn: address")
                        .to_string()
                })
                .collect::<Vec<_>>(),
        );

        {
            let conn = db.lock().await;
            conn.execute(
                "INSERT INTO users (id, username, password_hash, created_at) \
                 VALUES (1, 'bob', 'h', '2025-01-01')",
                [],
            )
            .expect("seed the conversation owner");
            crate::db::upsert_channels(&conn, &entries);
        }
        Messenger::new(
            db,
            Arc::new(MessagingDirectory::with_entries(entries)),
            Arc::from("test-source"),
            Arc::new(apps),
            Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
            defaults,
        )
        .with_ring_stores(Arc::new(RingStores::empty()))
        .with_subscriber_registrations(registrations_from_specs(&[spec]))
        .with_llm_chat(chat)
    }

    /// Seed a conversation row. The roster reads ids and app slugs; the rest of
    /// the row is there to satisfy the schema.
    async fn seed_conversation(m: &Messenger, id: i64, app_slug: &str) {
        let conn = m.db.lock().await;
        conn.execute(
            "INSERT INTO conversations (id, user_id, status, app_slug, created_at, updated_at) \
             VALUES (?1, 1, 'active', ?2, '2025-01-01', '2025-01-01')",
            rusqlite::params![id, app_slug],
        )
        .expect("seed conversation");
    }

    #[tokio::test]
    async fn the_body_is_a_function_of_the_conversation_set() {
        let m = messenger(false).await;

        {
            let conn = m.db.lock().await;
            assert_eq!(
                m.chat_roster_body(&conn, APP),
                r#"{"v":1,"conversations":[]}"#,
                "an app with no conversations has an empty roster, not no roster"
            );
        }

        // Inserted out of order: the body is ordered by id, not by arrival.
        seed_conversation(&m, 42, APP).await;
        seed_conversation(&m, 7, APP).await;
        seed_conversation(&m, 9, OTHER).await;

        let conn = m.db.lock().await;
        let body = m.chat_roster_body(&conn, APP);
        assert_eq!(body, r#"{"v":1,"conversations":[{"id":7},{"id":42}]}"#);
        assert_eq!(
            body,
            m.chat_roster_body(&conn, APP),
            "two reads of unchanged state produce identical bytes"
        );
        assert_eq!(
            m.chat_roster_body(&conn, OTHER),
            r#"{"v":1,"conversations":[{"id":9}]}"#,
            "an app's roster stops at its own conversations"
        );
    }

    #[tokio::test]
    async fn a_snapshot_publishes_once_per_change() {
        let m = messenger(true).await;
        seed_conversation(&m, 7, APP).await;

        assert!(
            matches!(
                m.publish_chat_roster(APP).await,
                Some(PublishResult::Ok { .. })
            ),
            "the first snapshot is news"
        );
        assert!(
            m.publish_chat_roster(APP).await.is_none(),
            "an unchanged snapshot says nothing and is not published"
        );

        seed_conversation(&m, 8, APP).await;
        assert!(
            matches!(
                m.publish_chat_roster(APP).await,
                Some(PublishResult::Ok { .. })
            ),
            "a new conversation changes the snapshot"
        );

        // The dedupe is per app: another app's first snapshot is still news.
        assert!(matches!(
            m.publish_chat_roster(OTHER).await,
            Some(PublishResult::Ok { .. })
        ),);
    }

    #[tokio::test]
    async fn the_published_body_is_the_snapshot() {
        let m = messenger(true).await;
        seed_conversation(&m, 7, APP).await;
        m.publish_chat_roster(APP).await;

        let conn = m.db.lock().await;
        let (sender, body): (String, String) = conn
            .query_row(
                "SELECT sender, body FROM messaging_messages ORDER BY id DESC LIMIT 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .expect("one snapshot was published");
        assert_eq!(body, r#"{"v":1,"conversations":[{"id":7}]}"#);
        assert_eq!(
            sender, "system:chat-roster",
            "the roster has one writer, and it is not any app"
        );
    }

    /// Concurrent publishers must not leave an older snapshot retained above a
    /// newer one.
    ///
    /// Each publisher reads the table and then publishes, and a state channel's
    /// truth is its newest message, so unserialized publishers can land in the
    /// opposite order to their reads — and the dedupe then holds the newer body
    /// and suppresses the correction until the set changes again. Every task
    /// here inserts before it announces, so the last publisher to run has seen
    /// every insert and the channel must end holding exactly the table.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_publishers_leave_the_newest_snapshot_last() {
        let m = messenger(true).await;

        let tasks: Vec<_> = (1..=8)
            .map(|id| {
                let m = m.clone();
                tokio::spawn(async move {
                    seed_conversation(&m, id, APP).await;
                    m.publish_chat_roster(APP).await;
                })
            })
            .collect();
        for task in tasks {
            task.await.expect("no publisher panicked");
        }

        let conn = m.db.lock().await;
        let newest: String = conn
            .query_row(
                "SELECT body FROM messaging_messages ORDER BY id DESC LIMIT 1",
                [],
                |r| r.get(0),
            )
            .expect("at least one snapshot was published");
        assert_eq!(
            newest,
            m.chat_roster_body(&conn, APP),
            "the newest retained snapshot is the current conversation set"
        );
    }

    #[tokio::test]
    async fn nothing_is_published_without_a_channel_or_an_app() {
        let m = messenger(false).await;

        assert!(
            m.publish_chat_roster(APP).await.is_none(),
            "no roster channel in the directory, nothing to publish onto"
        );
        assert!(
            messenger(true)
                .await
                .publish_chat_roster("nobody")
                .await
                .is_none(),
            "an app that is not in config has no roster"
        );
    }

    #[test]
    fn the_entry_is_a_shallow_durable_state_channel() {
        let entry = chat_roster_entry(
            &LlmChatConfig::default(),
            APP,
            &MessagingGlobalConfig::default(),
        );

        assert_eq!(entry.address, "brenn:chat.app.alice.roster");
        assert!(entry.capabilities().durable);
        assert_eq!(entry.resolved_channel.retain_depth, ROSTER_RETAIN_DEPTH);
        assert_eq!(
            entry.resolved_channel.standing_retain_depth, ROSTER_RETAIN_DEPTH,
            "a peer may not hold a deeper window than the channel keeps"
        );
        assert!(
            entry.subscribers.is_empty(),
            "roster subscriptions are minted by the attacher, never declared"
        );
    }
}
