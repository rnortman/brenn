//! Acting on conversation epochs.
//!
//! A singleton agent's current conversation must carry the epoch its channel
//! last said. One that does not is superseded: a new row takes over as the
//! agent's current conversation, inheriting its bus positions, and the old
//! session retires at its next idle moment. The old row, its chat family and
//! its roster entry stay.

use std::collections::HashMap;
use std::sync::Arc;

use brenn_conversation_epoch::EpochGoal;
use tracing::{debug, info, warn};

use crate::active_bridge::ActiveBridges;

/// Supersedes an epoch agent's conversation when its epoch is stale.
pub struct EpochReconciler {
    db: brenn_db::Db,
    messenger: Arc<brenn_messaging::Messenger>,
    active_bridges: ActiveBridges,
    /// Read for each agent's owner at the moment of the reconcile.
    apps: brenn_lib::config::AppTable,
    epochs: Arc<EpochGoal>,
    /// One async mutex per agent, minted on first use, never removed
    /// (bounded by the agent count). Two triggers for one agent run one
    /// after the other, never interleaved.
    slug_locks: tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl EpochReconciler {
    pub fn new(
        db: brenn_db::Db,
        messenger: Arc<brenn_messaging::Messenger>,
        active_bridges: ActiveBridges,
        apps: brenn_lib::config::AppTable,
        epochs: Arc<EpochGoal>,
    ) -> Self {
        Self {
            db,
            messenger,
            active_bridges,
            apps,
            epochs,
            slug_locks: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    /// The epoch handle this reconciler acts on.
    pub fn epochs(&self) -> &Arc<EpochGoal> {
        &self.epochs
    }

    /// Bring `app_slug`'s owner conversation in line with the agent's current
    /// epoch.
    ///
    /// Idempotent, and serialized per agent: a second call for the same agent
    /// waits for the first to finish. Must not be called with the db lock held.
    /// Resolving the owner and the current conversation takes it once, and
    /// minting the successor with its chat family takes it once more; the
    /// position walk and the retirements take it themselves.
    pub async fn reconcile(&self, app_slug: &str) {
        let lock = {
            let mut locks = self.slug_locks.lock().await;
            locks
                .entry(app_slug.to_string())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        let _serial = lock.lock().await;

        let Some(latest) = self.epochs.current(app_slug) else {
            return;
        };
        // The slug set is frozen for the process, so a miss is a host bug.
        let username = self
            .apps
            .get(app_slug)
            .unwrap_or_else(|| {
                panic!(
                    "BUG: conversation epoch reconcile for {app_slug:?}, which the agent table does not hold"
                )
            })
            .allowed_users
            .first()
            .cloned()
            .unwrap_or_else(|| {
                panic!(
                    "BUG: conversation epoch agent {app_slug:?} has no allowed user; config lowering refuses an epoch agent without exactly one"
                )
            });

        let (owner, current) = {
            let conn = self.db.lock().await;
            let Some(user) = brenn_db::auth::user::get_user_by_username(&conn, &username) else {
                drop(conn);
                warn!(
                    app = %app_slug,
                    username = %username,
                    "conversation epoch: owner user row missing; nothing to reconcile"
                );
                return;
            };
            (
                user.id,
                brenn_db::conversation::get_singleton_conversation(&conn, user.id, app_slug),
            )
        };
        // No conversation yet: the first attach mints one, and that one is
        // reconciled at its first idle moment.
        let Some(current) = current else {
            return;
        };

        if current.epoch.as_deref() == Some(latest.as_str()) {
            self.retire_strays(app_slug, owner, current.id).await;
            return;
        }

        // The row and its chat family come into existence together, so no
        // spawn can find the successor without its channels.
        let new = {
            let conn = self.db.lock().await;
            let new =
                brenn_db::conversation::create_singleton_successor(&conn, owner, app_slug, &latest);
            self.messenger
                .provision_conversation_chat_channels(&conn, app_slug, new);
            new
        };
        {
            // A batch the old bridge has read and not yet advanced past would
            // otherwise be inherited and answered a second time.
            let old_bridge = self.active_bridges.get(current.id).await;
            let _delivering = match &old_bridge {
                Some(bridge) => Some(bridge.lock_bus_delivery().await),
                None => None,
            };
            self.messenger
                .supersede_conversation_positions(app_slug, current.id, new)
                .await;
        }
        self.retire_strays(app_slug, owner, new).await;
        self.messenger.dispatch_kick();
        info!(
            app = %app_slug,
            old = current.id,
            new,
            epoch = %latest,
            "conversation superseded by a new epoch"
        );
    }

    /// Condemn every live session of `owner` on `app_slug` other than `keep`,
    /// and kill the ones idle now.
    ///
    /// `keep` is excluded because a wake can spawn and register the current
    /// conversation's own bridge while a reconcile runs. Only the owner's
    /// sessions are touched. Config holds an epoch agent to one allowed user,
    /// so a session of anyone else can only be a former owner's, left from
    /// before a reload moved `allowed_users`, and this epoch never decided its
    /// conversation.
    async fn retire_strays(&self, app_slug: &str, owner: i64, keep: i64) {
        for bridge in self.active_bridges.get_for_app(app_slug).await {
            if bridge.user_id != owner || bridge.conversation_id == keep {
                continue;
            }
            bridge.mark_superseded();
            let retired = bridge.retire_if_superseded_and_idle().await;
            debug!(
                conversation_id = bridge.conversation_id,
                retired, "conversation epoch: superseded session condemned"
            );
        }
    }
}

/// A reconciler over `db` and `registry` whose epoch handle binds `app_slug`
/// to the epoch address `brenn:<slug>.epoch`, over a messenger with no
/// channels.
#[cfg(test)]
pub(crate) fn test_reconciler(
    db: brenn_db::Db,
    registry: ActiveBridges,
    apps: brenn_lib::config::AppTable,
    app_slug: &str,
) -> Arc<EpochReconciler> {
    use std::collections::BTreeMap;

    use brenn_lib::messaging::{MessagingDirectory, MessagingGlobalConfig};
    use brenn_messaging::query::NoopWakeRouter;
    use brenn_messaging_store::store::RingStores;

    let messenger = brenn_messaging::Messenger::new(
        db.clone(),
        Arc::new(MessagingDirectory::with_entries(vec![])),
        Arc::from("test"),
        Arc::new(indexmap::IndexMap::new()),
        Arc::new(NoopWakeRouter) as Arc<dyn brenn_messaging::WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_ring_stores(Arc::new(RingStores::empty()));
    let epochs = Arc::new(EpochGoal::new(
        BTreeMap::from([(app_slug.to_string(), format!("brenn:{app_slug}.epoch"))]),
        brenn_obs::alerting::noop_alert_dispatcher().0,
    ));
    Arc::new(EpochReconciler::new(db, messenger, registry, apps, epochs))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use brenn_conversation_epoch::EpochGoal;
    use brenn_db::Db;
    use brenn_db::conversation::{Conversation, ConversationStatus};
    use brenn_lib::config::LlmChatConfig;
    use brenn_lib::messaging::config::{Depth, NoiseLevel, ResolvedChannel, Sink};
    use brenn_lib::messaging::{
        ChannelEntry, ChannelScheme, MessagingDirectory, MessagingGlobalConfig, ParticipantId,
        Urgency,
    };
    use brenn_messaging::Messenger;
    use brenn_messaging::query::NoopWakeRouter;
    use brenn_messaging_store::store::RingStores;

    use super::EpochReconciler;
    use crate::active_bridge::{ActiveBridge, ActiveBridges, test_bridge_for_reload};

    const APP: &str = "cogsworth";
    const OWNER: &str = "bob";
    const EPOCH: &str = "brenn:cogsworth.epoch";
    const WORK: &str = "brenn:work";
    const WORK_UUID: uuid::Uuid = uuid::Uuid::from_bytes([
        0x00, 0x00, 0x00, 0x00, 0xe9, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x01,
    ]);

    struct Fixture {
        db: Db,
        messenger: Arc<Messenger>,
        registry: ActiveBridges,
        reconciler: Arc<EpochReconciler>,
        owner: i64,
        old: i64,
        _alerts: tokio::task::JoinHandle<()>,
    }

    /// The work channel, subscribed by the agent at push and retain depth 5.
    fn work_entry() -> ChannelEntry {
        ChannelEntry {
            uuid: WORK_UUID,
            address: WORK.to_string(),
            description: None,
            resolved_channel: ResolvedChannel {
                send_rate: Default::default(),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: brenn_lib::messaging::WakeMin::Normal,
            },
            subscribers: vec![brenn_lib::messaging::SubscriberEntry {
                kind: brenn_lib::messaging::SubscriberEntryKind::App(APP.to_string()),
                push_depth: Depth::Bounded(5),
                retain_depth: Depth::Bounded(5),
                noise: NoiseLevel::Silent,
                wake_min: Some(brenn_lib::messaging::WakeMin::Normal),
            }],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        }
    }

    /// A singleton epoch agent owned by `bob`, reading `brenn:work`, with its
    /// current conversation seated on the channel and its chat family
    /// provisioned.
    async fn fixture() -> Fixture {
        let db = crate::test_support::init_db_memory();
        let (owner, old) = {
            let conn = db.lock().await;
            let owner = brenn_db::auth::user::create_user(&conn, OWNER, "$argon2id$fake");
            let old = brenn_db::conversation::create_conversation(&conn, owner, APP, false);
            (owner, old)
        };

        let mut app = brenn_lib::config::test_app_config(APP);
        app.singleton = true;
        app.allowed_users = vec![OWNER.to_string()];
        app.conversation_epoch = Some(EPOCH.to_string());
        app.policy_mut()
            .grants
            .insert(brenn_envelope::grants::AppCapability::MessagingSubscribe);
        app.policy_mut()
            .acls
            .brenn_subscribe
            .push(brenn_lib::access::acl::ChannelMatcher::Prefix(
                "work".to_string(),
            ));
        app.chat_harness_policy = Arc::new(LlmChatConfig::default().harness_policy(APP));
        let mut apps = indexmap::IndexMap::new();
        apps.insert(APP.to_string(), app);

        let roster = brenn_messaging::chat_roster::chat_roster_entry(
            &LlmChatConfig::default(),
            APP,
            &MessagingGlobalConfig::default(),
        );
        let roster_bare = roster
            .address
            .strip_prefix(ChannelScheme::Brenn.prefix())
            .expect("a roster address is a brenn: address")
            .to_string();
        let entries = vec![work_entry(), roster];
        let messenger = Messenger::new(
            db.clone(),
            Arc::new(MessagingDirectory::with_entries(entries.clone())),
            Arc::from("test-source"),
            Arc::new(apps),
            Arc::new(NoopWakeRouter) as Arc<dyn brenn_messaging::WakeRouter>,
            MessagingGlobalConfig::default(),
        )
        .with_ring_stores(Arc::new(RingStores::empty()))
        .with_subscriber_registrations(brenn_messaging::system::registrations_from_specs(&[
            brenn_messaging::system::SystemParticipantSpec::publish_only(
                brenn_messaging::chat_roster::CHAT_ROSTER_COMPONENT,
                ChannelScheme::Brenn,
                &[roster_bare],
            ),
        ]));
        {
            let conn = db.lock().await;
            brenn_messaging_store::db::upsert_channels(&conn, &entries);
            messenger.provision_conversation_chat_channels(&conn, APP, old);
        }
        messenger.attach_conversation_subscribers().await;

        let registry = ActiveBridges::new();
        let (alerts, alerts_handle) = brenn_obs::alerting::noop_alert_dispatcher();
        let epochs = Arc::new(EpochGoal::new(
            BTreeMap::from([(APP.to_string(), EPOCH.to_string())]),
            alerts,
        ));
        let reconciler = Arc::new(EpochReconciler::new(
            db.clone(),
            messenger.clone(),
            registry.clone(),
            messenger.app_table(),
            epochs,
        ));
        Fixture {
            db,
            messenger,
            registry,
            reconciler,
            owner,
            old,
            _alerts: alerts_handle,
        }
    }

    /// One message on the work channel, from a peer.
    async fn publish_work(db: &Db, body: &str) {
        let conn = db.lock().await;
        brenn_messaging_store::db::insert_message(
            &conn,
            WORK_UUID,
            "test-source",
            "app:peer",
            body,
            Urgency::Normal,
            ChannelScheme::Brenn,
            None,
            None,
            None,
            None,
            brenn_messaging_store::db::utc_to_ns(chrono::Utc::now()),
        );
    }

    /// Deliver what `conv` is owed and advance past it.
    async fn serve(m: &Messenger, conv: i64) {
        let delivery = m.conversation_delivery(conv).await;
        m.advance_conversation(conv, delivery).await;
    }

    /// The bodies `conv` is owed.
    async fn owed(m: &Messenger, conv: i64) -> Vec<String> {
        m.conversation_delivery(conv)
            .await
            .messages
            .iter()
            .map(|env| env.body.clone())
            .collect()
    }

    async fn singleton(db: &Db, owner: i64) -> Conversation {
        let conn = db.lock().await;
        brenn_db::conversation::get_singleton_conversation(&conn, owner, APP)
            .expect("the agent has a conversation")
    }

    async fn app_rows(db: &Db) -> i64 {
        let conn = db.lock().await;
        conn.query_row(
            "SELECT COUNT(*) FROM conversations WHERE app_slug = ?1",
            rusqlite::params![APP],
            |r| r.get(0),
        )
        .unwrap()
    }

    async fn status(db: &Db, conv: i64) -> ConversationStatus {
        let conn = db.lock().await;
        brenn_db::conversation::get_conversation(&conn, conv).status
    }

    async fn bridge_on(f: &Fixture, user: i64, conv: i64) -> Arc<ActiveBridge> {
        let bridge = test_bridge_for_reload(
            f.db.clone(),
            user,
            conv,
            APP,
            f.messenger.app_table(),
            f.registry.clone(),
        );
        f.registry.insert(conv, bridge.clone()).await;
        bridge
    }

    fn apply(f: &Fixture, epoch: &str) {
        f.reconciler.epochs().apply(EPOCH, epoch);
    }

    #[tokio::test]
    async fn no_epoch_reconciles_nothing() {
        let f = fixture().await;
        f.reconciler.reconcile(APP).await;
        assert_eq!(app_rows(&f.db).await, 1);
        assert_eq!(singleton(&f.db, f.owner).await.id, f.old);
    }

    #[tokio::test]
    async fn a_stale_conversation_is_superseded_and_what_it_saw_stays_seen() {
        let f = fixture().await;
        publish_work(&f.db, "a").await;
        publish_work(&f.db, "b").await;
        serve(&f.messenger, f.old).await;
        apply(&f, "1");
        f.reconciler.reconcile(APP).await;

        let new = singleton(&f.db, f.owner).await;
        assert_ne!(new.id, f.old, "the singleton is a new conversation");
        assert_eq!(new.epoch.as_deref(), Some("1"));
        assert_eq!(app_rows(&f.db).await, 2);
        assert!(
            owed(&f.messenger, new.id).await.is_empty(),
            "what the old conversation saw is not owed to its successor"
        );
        {
            let conn = f.db.lock().await;
            assert!(
                brenn_messaging_store::db::load_subscriber_cursor(
                    &conn,
                    WORK_UUID,
                    &ParticipantId::for_conversation(f.old),
                )
                .is_none(),
                "the old conversation's position is gone"
            );
        }
        assert_eq!(
            status(&f.db, f.old).await,
            ConversationStatus::Active,
            "no bridge was killed, so the old row is untouched"
        );

        let roster_uuid = f
            .messenger
            .directory()
            .resolve(
                &brenn_messaging::chat_roster::chat_roster_entry(
                    &LlmChatConfig::default(),
                    APP,
                    &MessagingGlobalConfig::default(),
                )
                .address,
            )
            .expect("the roster channel is declared")
            .uuid;
        let body: String = {
            let conn = f.db.lock().await;
            conn.query_row(
                "SELECT body FROM messaging_messages WHERE channel_uuid = ?1 ORDER BY id DESC LIMIT 1",
                rusqlite::params![roster_uuid.as_bytes().as_slice()],
                |r| r.get(0),
            )
            .unwrap()
        };
        assert_eq!(
            body,
            format!(
                "{{\"v\":1,\"conversations\":[{{\"id\":{}}},{{\"id\":{}}}]}}",
                f.old, new.id
            )
        );
    }

    #[tokio::test]
    async fn what_landed_after_the_old_position_is_owed_to_the_successor() {
        let f = fixture().await;
        publish_work(&f.db, "a").await;
        serve(&f.messenger, f.old).await;
        publish_work(&f.db, "b").await;
        apply(&f, "1");
        f.reconciler.reconcile(APP).await;
        let new = singleton(&f.db, f.owner).await;
        assert_eq!(owed(&f.messenger, new.id).await, vec!["b".to_string()]);
    }

    #[tokio::test]
    async fn an_idle_bridge_on_the_stale_conversation_is_retired() {
        let f = fixture().await;
        let bridge = bridge_on(&f, f.owner, f.old).await;
        apply(&f, "1");
        f.reconciler.reconcile(APP).await;
        assert!(f.registry.get(f.old).await.is_none(), "the bridge is gone");
        assert_eq!(status(&f.db, f.old).await, ConversationStatus::Completed);
        assert!(bridge.is_superseded_killing());
    }

    #[tokio::test]
    async fn a_busy_bridge_is_condemned_and_left_to_its_turn() {
        let f = fixture().await;
        let bridge = bridge_on(&f, f.owner, f.old).await;
        bridge.set_cc_idle_for_test(false);
        apply(&f, "1");
        f.reconciler.reconcile(APP).await;
        assert!(f.registry.get(f.old).await.is_some(), "still registered");
        assert!(bridge.is_superseded());
        assert_ne!(singleton(&f.db, f.owner).await.id, f.old);
    }

    #[tokio::test]
    async fn a_stray_bridge_is_retired_when_the_epoch_is_already_current() {
        let f = fixture().await;
        apply(&f, "1");
        f.reconciler.reconcile(APP).await;
        // The bridge carries no reconciler, so registration does not condemn it.
        let bridge = bridge_on(&f, f.owner, f.old).await;
        assert!(!bridge.is_superseded());
        f.reconciler.reconcile(APP).await;
        assert!(
            f.registry.get(f.old).await.is_none(),
            "the stray is retired"
        );
        assert_eq!(app_rows(&f.db).await, 2);
    }

    #[tokio::test]
    async fn reconciling_twice_mints_one_successor() {
        let f = fixture().await;
        apply(&f, "1");
        f.reconciler.reconcile(APP).await;
        f.reconciler.reconcile(APP).await;
        assert_eq!(app_rows(&f.db).await, 2);
    }

    #[tokio::test]
    async fn a_newer_epoch_supersedes_the_successor() {
        let f = fixture().await;
        apply(&f, "1");
        f.reconciler.reconcile(APP).await;
        apply(&f, "2");
        f.reconciler.reconcile(APP).await;
        assert_eq!(app_rows(&f.db).await, 3);
        assert_eq!(singleton(&f.db, f.owner).await.epoch.as_deref(), Some("2"));
    }

    /// A session of a user other than the configured owner (a former owner's) is not condemned.
    #[tokio::test]
    async fn a_former_owners_session_is_left_alone() {
        let f = fixture().await;
        let (carol, theirs) = {
            let conn = f.db.lock().await;
            let carol = brenn_db::auth::user::create_user(&conn, "carol", "$argon2id$fake");
            let theirs = brenn_db::conversation::create_conversation(&conn, carol, APP, false);
            (carol, theirs)
        };
        let bridge = bridge_on(&f, carol, theirs).await;
        apply(&f, "1");
        f.reconciler.reconcile(APP).await;
        assert!(f.registry.get(theirs).await.is_some(), "still registered");
        assert!(!bridge.is_superseded());
    }
}
