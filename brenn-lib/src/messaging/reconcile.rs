//! Boot reconciliation of the durable cursor rows against the world they stand
//! in.
//!
//! A cursor row is the one piece of per-subscriber delivery state that outlives
//! the process, and the two things it stands on — the registration that
//! justifies it and the retention it indexes into — can both change while the
//! process is down. Nothing at runtime can produce either state, so both are
//! answered once, at boot, against the assembled directory:
//!
//! - a row whose registration is gone wakes nobody (every wake pass resolves
//!   against the live directory) but is reported against by every eviction pass
//!   forever — noise attributed to a ghost. It is deleted. A registration the
//!   config merely no longer stands behind is not gone: a durable dynamic row
//!   the boot merge held back — for a revoked ACL, a tightened standing depth,
//!   or a channel whose declaration was removed — is registration state the
//!   operator may restore, so its position is justified and kept.
//! - a row standing above everything its channel ever held is the internal
//!   analogue of the wire cursor's resume-ahead gap: an operator restored a
//!   database under a position that outlived it. It is escalated and reset to
//!   head. Not a panic — an operator restore is an external event, not a code
//!   bug, and refusing to boot would strand every other subscriber over one
//!   subscriber's uncountable loss.
//!
//! Ring cursors need none of this: they are minted at attach and die with the
//! process, so neither state can survive into a boot.

use std::collections::{HashMap, HashSet};

use uuid::Uuid;

use super::config::DormantSubscription;
use super::db;
use super::{Messenger, ParticipantId, SubscriberEntryKind};

/// What one [`Messenger::reconcile_subscriber_cursors`] pass found.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CursorReconcile {
    /// Rows deleted for having no live registration behind them.
    pub orphans_removed: usize,
    /// Rows whose position stood above the channel's head and was reset to it.
    pub positions_reset: usize,
}

impl CursorReconcile {
    /// Whether the pass changed nothing — the ordinary boot.
    pub fn is_clean(&self) -> bool {
        self.orphans_removed == 0 && self.positions_reset == 0
    }
}

impl Messenger {
    /// Reconcile every durable cursor row against the live directory and its
    /// channel's head, per the module doc.
    ///
    /// Runs once at boot, after the directory is assembled (static, WASM,
    /// surface, system, and the durable dynamic rows folded back in) and before
    /// the boot attaches: what it judges is the state a *previous* boot left, and
    /// a row this boot's attaches are about to create or reposition is not that.
    ///
    /// `dormant` carries every durable dynamic row the boot merge held back.
    /// Those rows are deliberately kept in their table and deliberately kept out
    /// of the directory, so the directory alone would read them as ghosts and
    /// this pass would delete the very positions a restored ACL is supposed to
    /// resume from — turning a revoke/restore across a restart into silent,
    /// unattributed loss. The address travels on each entry because one dormant
    /// class has no directory entry to read it off: a row whose channel exists
    /// but whose `[[channel]]` block is gone.
    pub async fn reconcile_subscriber_cursors(
        &self,
        dormant: &[DormantSubscription],
    ) -> CursorReconcile {
        let conn = self.db.lock().await;
        let rows = db::all_subscriber_cursors(&conn);
        if rows.is_empty() {
            return CursorReconcile::default();
        }
        // The directory is the authority on who subscribes, so the justified set
        // is read off it rather than off any registration table: a subscriber
        // reaches its position through the same directory entry a wake pass and a
        // window read reach it through, and a row no such entry names is
        // reachable by nothing.
        let mut justified: HashMap<Uuid, HashSet<ParticipantId>> = HashMap::new();
        for entry in self.directory.list() {
            for sub in &entry.subscribers {
                if !sub.push_depth.is_push_enabled() {
                    // Sampled: visibility without delivery, and so no position.
                    // A row for one is residue of a demotion, which is an orphan
                    // by the same rule.
                    continue;
                }
                let participant = match &sub.kind {
                    // Resolve-only. An app whose singleton conversation does not
                    // exist has never been a delivery target, so it justifies no
                    // row; minting one here would justify whatever row it found.
                    SubscriberEntryKind::App(slug) => self
                        .targets
                        .app_conversation(&conn, slug, &entry.address)
                        .map(ParticipantId::for_conversation),
                    SubscriberEntryKind::Wasm(slug) => Some(ParticipantId::for_wasm(slug)),
                    SubscriberEntryKind::System(component) => {
                        Some(ParticipantId::for_system(component))
                    }
                    // A surface holds no server-side position at all: the cursor
                    // it echoes at subscribe is its whole delivery state. Any row
                    // under a surface identity is an orphan.
                    SubscriberEntryKind::Surface(_) => None,
                    // The subscription names its conversation outright, so
                    // nothing has to be resolved to know whose row is whose. The
                    // row is the one this boot's provisioning just re-created,
                    // and it is what a dormant conversation's commands were
                    // published against.
                    SubscriberEntryKind::ChatConversation {
                        conversation_id, ..
                    } => Some(ParticipantId::for_conversation(*conversation_id)),
                };
                if let Some(participant) = participant {
                    justified.entry(entry.uuid).or_default().insert(participant);
                }
            }
        }
        // A dormant dynamic row justifies its conversation's position by the same
        // rule its live sibling does, resolve-only: an app that never had a
        // conversation on the channel has never been a delivery target there.
        // The address comes from the caller rather than from the directory: a row
        // whose channel is undeclared has no directory entry at all, and looking
        // one up would skip exactly the position a redeclared channel resumes
        // from. The row's own `channel_uuid` is the key either way.
        //
        // TODO(dormant-missing-app-cursor): `app_conversation` resolves through
        // the apps map, so a dormant row whose `[[app]]` block is the thing that
        // went missing resolves to nothing and loses its position here — the one
        // dormant class whose promise is only half-kept, and the class the merge
        // mints whenever an app has no resolvable policy.
        for row in dormant {
            if let Some(conversation) =
                self.targets
                    .app_conversation(&conn, &row.app_slug, &row.channel_address)
            {
                justified
                    .entry(row.channel_uuid)
                    .or_default()
                    .insert(ParticipantId::for_conversation(conversation));
            }
        }

        let mut report = CursorReconcile::default();
        for (channel_uuid, row, head) in rows {
            let address = self
                .directory
                .by_uuid(&channel_uuid)
                .map(|entry| entry.address.clone())
                .unwrap_or_else(|| channel_uuid.to_string());
            let live = justified
                .get(&channel_uuid)
                .is_some_and(|subs| subs.contains(&row.subscriber));
            if !live {
                tracing::warn!(
                    channel = %address,
                    subscriber = %row.subscriber.as_str(),
                    "messaging: deleting orphaned cursor — no live registration names this \
                     subscriber on this channel"
                );
                db::delete_subscriber_cursor(&conn, channel_uuid, &row.subscriber);
                report.orphans_removed += 1;
                continue;
            }
            // `head + 1` is where a position primed at head sits, so it is the
            // highest legitimate value: everything above it names a sequence the
            // channel never issued.
            if row.next_owed_seq > head + 1 {
                tracing::error!(
                    channel = %address,
                    subscriber = %row.subscriber.as_str(),
                    position = row.next_owed_seq,
                    head,
                    "messaging: cursor stands above everything the channel ever held — the \
                     database was restored under a position that outlived it; resetting to head. \
                     Whatever the subscriber missed is uncountable."
                );
                self.router.position_ahead_of_retention(
                    &address,
                    &row.subscriber,
                    row.next_owed_seq.max(0).unsigned_abs(),
                    head.max(0).unsigned_abs(),
                );
                db::set_subscriber_cursor_position(&conn, channel_uuid, &row.subscriber, head + 1);
                report.positions_reset += 1;
            }
        }
        report
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use indexmap::IndexMap;

    use super::*;
    use crate::config::AppConfig;
    use crate::db::init_db_memory;
    use crate::messaging::config::{
        Depth, MessagingGlobalConfig, NoiseLevel, ResolvedChannel, Sink,
    };
    use crate::messaging::db::{ensure_subscriber_cursor, load_subscriber_cursor, upsert_channels};
    use crate::messaging::query::NoopWakeRouter;
    use crate::messaging::test_support::test_app_config;
    use crate::messaging::{
        ChannelEntry, ChannelScheme, MessagingDirectory, SubscriberEntry, WakeMin, WakeRouter,
    };

    const APP: &str = "chatapp";
    /// A second app on the same user, so a case can tell "this registration
    /// justifies this position" from "some registration justifies everything".
    const PEER_APP: &str = "peerapp";
    const USER: &str = "owner";

    /// Records the escalations the reconcile fires, which is the half of the
    /// seq-regression rule the database cannot show.
    #[derive(Default)]
    struct RecordingRouter {
        ahead: Mutex<Vec<(String, String, u64, u64)>>,
    }

    #[async_trait::async_trait]
    impl WakeRouter for RecordingRouter {
        async fn deliver(
            &self,
            _key: &SubscriberEntryKind,
            _envelope: &std::sync::Arc<crate::messaging::MessageEnvelope>,
            _retained_seq: i64,
        ) -> Result<bool, String> {
            Ok(false)
        }

        async fn deliver_ingress(
            &self,
            _key: &SubscriberEntryKind,
            _: &ParticipantId,
            _event: &crate::messaging::ingress::Event,
        ) -> Result<bool, String> {
            Ok(false)
        }

        fn spawn_eager_wake(&self, _key: &SubscriberEntryKind, _: &ParticipantId) {}

        fn delivery_shape(&self, key: &SubscriberEntryKind) -> crate::messaging::DeliveryShape {
            crate::messaging::default_delivery_shape(key)
        }

        fn alarm(&self, _channel: &str, _subscriber: &ParticipantId, _count: u64) {}

        fn position_ahead_of_retention(
            &self,
            channel: &str,
            subscriber: &ParticipantId,
            position: u64,
            head: u64,
        ) {
            self.ahead.lock().expect("lock").push((
                channel.to_string(),
                subscriber.as_str().to_string(),
                position,
                head,
            ));
        }
    }

    fn channel(name: &str, subscribers: Vec<SubscriberEntry>) -> ChannelEntry {
        ChannelEntry {
            uuid: Uuid::new_v4(),
            address: crate::messaging::canonical_address(name),
            description: None,
            resolved_channel: ResolvedChannel {
                send_rate: Default::default(),
                push_depth: Depth::Bounded(5),
                retain_depth: Depth::Bounded(10),
                standing_retain_depth: Depth::Bounded(10),
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers,
            transport_type: ChannelScheme::Brenn,
            mount: None,
        }
    }

    fn wasm_subscriber(slug: &str) -> SubscriberEntry {
        SubscriberEntry {
            kind: SubscriberEntryKind::Wasm(slug.to_string()),
            push_depth: Depth::Bounded(5),
            retain_depth: Depth::Bounded(0),
            noise: NoiseLevel::Silent,
            wake_min: None,
        }
    }

    async fn messenger(
        entries: Vec<ChannelEntry>,
        router: std::sync::Arc<dyn WakeRouter>,
    ) -> std::sync::Arc<Messenger> {
        let db = init_db_memory();
        {
            let conn = db.lock().await;
            crate::auth::user::create_user(&conn, USER, "$argon2id$fake");
        }
        messenger_over(db, entries, router).await
    }

    /// A messenger over an existing database, so a case can boot a second one on
    /// the state the first left — the only way to reach a directory that no
    /// longer holds a channel whose rows are still there.
    async fn messenger_over(
        db: crate::db::Db,
        entries: Vec<ChannelEntry>,
        router: std::sync::Arc<dyn WakeRouter>,
    ) -> std::sync::Arc<Messenger> {
        {
            let conn = db.lock().await;
            upsert_channels(&conn, &entries);
        }
        let mut app: AppConfig = test_app_config(APP, None, vec![USER.to_string()]);
        app.singleton = true;
        let mut peer: AppConfig = test_app_config(PEER_APP, None, vec![USER.to_string()]);
        peer.singleton = true;
        let mut apps: IndexMap<String, AppConfig> = IndexMap::new();
        apps.insert(APP.to_string(), app);
        apps.insert(PEER_APP.to_string(), peer);
        Messenger::new(
            db,
            std::sync::Arc::new(MessagingDirectory::with_entries(entries)),
            std::sync::Arc::from("test-source"),
            std::sync::Arc::new(apps),
            router,
            MessagingGlobalConfig::default(),
        )
    }

    /// Write a cursor row directly, so a case can plant the state a previous boot
    /// would have left without needing the registration that would justify it.
    async fn plant_cursor(m: &Messenger, channel_uuid: Uuid, subscriber: &ParticipantId, at: i64) {
        let conn = m.db.lock().await;
        ensure_subscriber_cursor(
            &conn,
            channel_uuid,
            subscriber,
            "whatever",
            Depth::Bounded(5),
            at,
        );
    }

    async fn cursor_at(
        m: &Messenger,
        channel_uuid: Uuid,
        subscriber: &ParticipantId,
    ) -> Option<i64> {
        let conn = m.db.lock().await;
        load_subscriber_cursor(&conn, channel_uuid, subscriber).map(|row| row.next_owed_seq)
    }

    /// A row the directory still names survives; one it does not is deleted. The
    /// orphan wakes nothing either way — what it costs is an eviction report on
    /// every pass, forever, attributed to a subscriber that does not exist.
    #[tokio::test]
    async fn a_cursor_with_no_live_registration_is_deleted_and_a_registered_one_survives() {
        let ch = channel("chat", vec![wasm_subscriber("live")]);
        let uuid = ch.uuid;
        let m = messenger(vec![ch], std::sync::Arc::new(NoopWakeRouter)).await;
        let live = ParticipantId::for_wasm("live");
        let gone = ParticipantId::for_wasm("gone");
        plant_cursor(&m, uuid, &live, 1).await;
        plant_cursor(&m, uuid, &gone, 1).await;

        let report = m.reconcile_subscriber_cursors(&[]).await;

        assert_eq!(report.orphans_removed, 1);
        assert_eq!(report.positions_reset, 0);
        assert_eq!(
            cursor_at(&m, uuid, &live).await,
            Some(1),
            "the registered subscriber keeps its position untouched"
        );
        assert_eq!(cursor_at(&m, uuid, &gone).await, None);
    }

    /// A surface holds no server-side position by design, so a row under a
    /// surface identity is an orphan even though the directory names the
    /// subscriber.
    #[tokio::test]
    async fn a_surface_cursor_is_an_orphan_whatever_the_directory_says() {
        let ch = channel(
            "chat",
            vec![SubscriberEntry {
                kind: SubscriberEntryKind::Surface("dash".to_string()),
                push_depth: Depth::Bounded(5),
                retain_depth: Depth::Bounded(0),
                noise: NoiseLevel::Silent,
                wake_min: None,
            }],
        );
        let uuid = ch.uuid;
        let m = messenger(vec![ch], std::sync::Arc::new(NoopWakeRouter)).await;
        let surface = ParticipantId::for_surface_component("dash", "one");
        plant_cursor(&m, uuid, &surface, 1).await;

        assert_eq!(m.reconcile_subscriber_cursors(&[]).await.orphans_removed, 1);
        assert_eq!(cursor_at(&m, uuid, &surface).await, None);
    }

    /// A sampled subscriber is never delivered to and so holds no position; a row
    /// left by a demotion is an orphan by the same rule.
    #[tokio::test]
    async fn a_sampled_subscribers_leftover_row_is_an_orphan() {
        let mut sub = wasm_subscriber("watcher");
        sub.push_depth = Depth::Bounded(0);
        let ch = channel("chat", vec![sub]);
        let uuid = ch.uuid;
        let m = messenger(vec![ch], std::sync::Arc::new(NoopWakeRouter)).await;
        let watcher = ParticipantId::for_wasm("watcher");
        plant_cursor(&m, uuid, &watcher, 1).await;

        assert_eq!(m.reconcile_subscriber_cursors(&[]).await.orphans_removed, 1);
        assert_eq!(cursor_at(&m, uuid, &watcher).await, None);
    }

    /// A position above `head + 1` names a sequence the channel never issued —
    /// a database restored under a cursor that outlived it. It is escalated and
    /// reset to head, not panicked on: the cause is external, and refusing to
    /// boot would strand every other subscriber over one subscriber's
    /// uncountable loss.
    #[tokio::test]
    async fn a_position_above_the_channel_head_is_escalated_and_reset() {
        let ch = channel("chat", vec![wasm_subscriber("ahead")]);
        let uuid = ch.uuid;
        let address = ch.address.clone();
        let router = std::sync::Arc::new(RecordingRouter::default());
        let m = messenger(vec![ch], router.clone()).await;
        let ahead = ParticipantId::for_wasm("ahead");
        // The channel never issued a sequence: its head is 0, so anything above
        // 1 is ahead of everything it ever held.
        plant_cursor(&m, uuid, &ahead, 42).await;

        let report = m.reconcile_subscriber_cursors(&[]).await;

        assert_eq!(report.positions_reset, 1);
        assert_eq!(report.orphans_removed, 0);
        assert_eq!(
            cursor_at(&m, uuid, &ahead).await,
            Some(1),
            "reset to head + 1, where a head-primed position sits"
        );
        assert_eq!(
            *router.ahead.lock().expect("lock"),
            vec![(address, ahead.as_str().to_string(), 42, 0)],
            "the operator hears about it once, naming both figures"
        );
    }

    /// The same rule on a channel that has actually issued sequences, where the
    /// boundary, the reset target and the head-primed position are three
    /// different numbers rather than all collapsing onto 1: a position five past
    /// a head of four resets to five, and the escalation names the real figures.
    /// The sibling sitting *at* head is still owed the newest message and is left
    /// exactly where it is.
    #[tokio::test]
    async fn a_position_above_a_used_channels_head_resets_to_that_head() {
        let ch = channel(
            "chat",
            vec![wasm_subscriber("ahead"), wasm_subscriber("lagging")],
        );
        let entry = ch.clone();
        let uuid = ch.uuid;
        let address = ch.address.clone();
        let router = std::sync::Arc::new(RecordingRouter::default());
        let m = messenger(vec![ch], router.clone()).await;
        for i in 0..4 {
            crate::messaging::testutils::insert_bus_message(
                &m,
                &entry,
                &format!("m{i}"),
                ChannelScheme::Brenn,
            )
            .await;
        }

        let ahead = ParticipantId::for_wasm("ahead");
        let lagging = ParticipantId::for_wasm("lagging");
        plant_cursor(&m, uuid, &ahead, 9).await;
        plant_cursor(&m, uuid, &lagging, 4).await;

        let report = m.reconcile_subscriber_cursors(&[]).await;

        assert_eq!(report.positions_reset, 1);
        assert_eq!(report.orphans_removed, 0);
        assert_eq!(
            cursor_at(&m, uuid, &ahead).await,
            Some(5),
            "the reset target is this channel's head + 1, not a hardcoded 1"
        );
        assert_eq!(
            cursor_at(&m, uuid, &lagging).await,
            Some(4),
            "a position inside the channel's history is a backlog, not a regression"
        );
        assert_eq!(
            *router.ahead.lock().expect("lock"),
            vec![(address, ahead.as_str().to_string(), 9, 4)],
            "the escalation carries the position and the head it stood above"
        );
    }

    /// The boundary on a used channel: `head + 1` is where a caught-up
    /// subscriber sits, so it is the highest position that is not a regression.
    #[tokio::test]
    async fn a_position_at_a_used_channels_boundary_is_not_a_regression() {
        let ch = channel("chat", vec![wasm_subscriber("caught-up")]);
        let entry = ch.clone();
        let uuid = ch.uuid;
        let router = std::sync::Arc::new(RecordingRouter::default());
        let m = messenger(vec![ch], router.clone()).await;
        for i in 0..3 {
            crate::messaging::testutils::insert_bus_message(
                &m,
                &entry,
                &format!("m{i}"),
                ChannelScheme::Brenn,
            )
            .await;
        }
        let sub = ParticipantId::for_wasm("caught-up");
        plant_cursor(&m, uuid, &sub, 4).await;

        assert!(m.reconcile_subscriber_cursors(&[]).await.is_clean());
        assert_eq!(cursor_at(&m, uuid, &sub).await, Some(4));
        assert!(router.ahead.lock().expect("lock").is_empty());
    }

    /// A dynamic subscription the boot merge held back for a revoked ACL keeps
    /// its registration row on purpose, so its conversation's position is
    /// justified and survives the boot. Deleting it would re-prime the
    /// subscriber at head on the restore boot, skipping everything published
    /// while it was denied — the silent loss the revoke-without-advance rule
    /// exists to prevent. A row with no registration at all is still an orphan.
    #[tokio::test]
    async fn a_dormant_dynamic_registration_justifies_its_conversations_position() {
        // The merge does not fold a revoked row, so the channel carries no `App`
        // subscriber — the state this pass would otherwise read as a ghost.
        let ch = channel("chat", vec![]);
        let uuid = ch.uuid;
        let address = ch.address.clone();
        let m = messenger(vec![ch], std::sync::Arc::new(NoopWakeRouter)).await;
        m.attach_conversation(&address, APP, Depth::Bounded(5))
            .await;
        let conversation = {
            let conn = m.db.lock().await;
            m.targets
                .app_conversation(&conn, APP, &address)
                .expect("the attach minted it")
        };
        let position = ParticipantId::for_conversation(conversation);
        // Left behind by the denied period: a position that has not moved.
        let dormant = vec![DormantSubscription {
            channel_uuid: uuid,
            app_slug: APP.to_string(),
            channel_address: address.clone(),
        }];

        assert!(m.reconcile_subscriber_cursors(&dormant).await.is_clean());
        assert!(
            cursor_at(&m, uuid, &position).await.is_some(),
            "the restore boot must find the position the denial left standing"
        );

        // The same row once the subscription itself is gone — nothing names it.
        assert_eq!(m.reconcile_subscriber_cursors(&[]).await.orphans_removed, 1);
        assert_eq!(cursor_at(&m, uuid, &position).await, None);
    }

    /// A dormant row whose channel is undeclared has no directory entry at all —
    /// its `[[channel]]` block was removed or commented out while the channel's
    /// own row survived. The pass reads the channel off the registration rather
    /// than looking the uuid up in a directory that cannot answer, so the
    /// position a redeclaring boot resumes from survives.
    ///
    /// Two apps hold positions on the channel and only one is registered dormant,
    /// which is what makes the pass discriminating: it keys on the registration's
    /// own `(channel, app)` identity, not on any dormant entry existing.
    #[tokio::test]
    async fn a_dormant_row_on_an_undeclared_channel_keeps_its_position() {
        let ch = channel("chat", vec![]);
        let uuid = ch.uuid;
        let address = ch.address.clone();
        let db = init_db_memory();
        {
            let conn = db.lock().await;
            crate::auth::user::create_user(&conn, USER, "$argon2id$fake");
        }
        let declared =
            messenger_over(db.clone(), vec![ch], std::sync::Arc::new(NoopWakeRouter)).await;
        let mut positions = Vec::new();
        for slug in [APP, PEER_APP] {
            declared
                .attach_conversation(&address, slug, Depth::Bounded(5))
                .await;
            let conn = declared.db.lock().await;
            positions.push(ParticipantId::for_conversation(
                declared
                    .targets
                    .app_conversation(&conn, slug, &address)
                    .expect("the attach minted it"),
            ));
        }
        let (position, peer_position) = (positions[0].clone(), positions[1].clone());

        // The next boot, with the block gone: the channel is in no directory, so
        // only the merge's skip report can name it.
        let undeclared = messenger_over(db, Vec::new(), std::sync::Arc::new(NoopWakeRouter)).await;
        let dormant = vec![DormantSubscription {
            channel_uuid: uuid,
            app_slug: APP.to_string(),
            channel_address: address,
        }];
        assert_eq!(
            undeclared
                .reconcile_subscriber_cursors(&dormant)
                .await
                .orphans_removed,
            1,
            "the peer holds no dormant registration, so its position is an orphan"
        );
        assert!(
            cursor_at(&undeclared, uuid, &position).await.is_some(),
            "the position a redeclared channel resumes from must survive"
        );
        assert_eq!(
            cursor_at(&undeclared, uuid, &peer_position).await,
            None,
            "one app's dormant registration must not justify another's position"
        );

        // Without the dormant registration the same row is an orphan — the
        // address alone justifies nothing.
        assert_eq!(
            undeclared
                .reconcile_subscriber_cursors(&[])
                .await
                .orphans_removed,
            1
        );
    }

    /// The boundary is not a regression: a position at `head + 1` is exactly
    /// where a caught-up (or head-primed) subscriber sits.
    #[tokio::test]
    async fn a_caught_up_position_is_not_a_regression() {
        let ch = channel("chat", vec![wasm_subscriber("caught-up")]);
        let uuid = ch.uuid;
        let router = std::sync::Arc::new(RecordingRouter::default());
        let m = messenger(vec![ch], router.clone()).await;
        let sub = ParticipantId::for_wasm("caught-up");
        plant_cursor(&m, uuid, &sub, 1).await;

        assert!(m.reconcile_subscriber_cursors(&[]).await.is_clean());
        assert_eq!(cursor_at(&m, uuid, &sub).await, Some(1));
        assert!(router.ahead.lock().expect("lock").is_empty());
    }

    /// An `App` subscriber's row is justified through the conversation it
    /// delivers to, resolved without minting one: an app that has never had a
    /// conversation justifies no row.
    #[tokio::test]
    async fn an_app_subscribers_row_is_justified_through_its_conversation() {
        let ch = channel(
            "chat",
            vec![SubscriberEntry {
                kind: SubscriberEntryKind::App(APP.to_string()),
                push_depth: Depth::Bounded(5),
                retain_depth: Depth::Bounded(0),
                noise: NoiseLevel::Silent,
                wake_min: Some(WakeMin::Normal),
            }],
        );
        let uuid = ch.uuid;
        let address = ch.address.clone();
        let m = messenger(vec![ch], std::sync::Arc::new(NoopWakeRouter)).await;
        // Before the attach the app holds no conversation, so a row under one is
        // justified by nothing.
        let stale = ParticipantId::for_conversation(999);
        plant_cursor(&m, uuid, &stale, 1).await;
        assert_eq!(m.reconcile_subscriber_cursors(&[]).await.orphans_removed, 1);

        // The attach mints the conversation and its position; the next boot's
        // reconcile leaves both alone.
        m.attach_conversation(&address, APP, Depth::Bounded(5))
            .await;
        let conversation = {
            let conn = m.db.lock().await;
            m.targets
                .app_conversation(&conn, APP, &address)
                .expect("the attach minted it")
        };
        let position = ParticipantId::for_conversation(conversation);
        assert!(m.reconcile_subscriber_cursors(&[]).await.is_clean());
        assert!(cursor_at(&m, uuid, &position).await.is_some());
    }

    /// Nothing planted, nothing to do — the ordinary boot pays one read.
    #[tokio::test]
    async fn an_empty_table_reconciles_clean() {
        let ch = channel("chat", vec![wasm_subscriber("live")]);
        let m = messenger(vec![ch], std::sync::Arc::new(NoopWakeRouter)).await;
        assert!(m.reconcile_subscriber_cursors(&[]).await.is_clean());
    }

    /// Where a fresh attach lands is not this pass's business, but the fixture's
    /// planted rows must agree with what an attach would produce, so the two are
    /// pinned together: an attach on an empty channel lands at 1, the value the
    /// regression boundary uses.
    #[tokio::test]
    async fn a_fresh_attach_on_an_empty_channel_sits_at_the_boundary() {
        let ch = channel("chat", vec![wasm_subscriber("fresh")]);
        let uuid = ch.uuid;
        let address = ch.address.clone();
        let m = messenger(vec![ch], std::sync::Arc::new(NoopWakeRouter)).await;
        let fresh = ParticipantId::for_wasm("fresh");
        m.attach_subscriber(&address, "fresh", &fresh, Depth::Bounded(5))
            .await;
        assert_eq!(cursor_at(&m, uuid, &fresh).await, Some(1));
        assert!(m.reconcile_subscriber_cursors(&[]).await.is_clean());
    }
}
