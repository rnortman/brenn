//! Boot wiring for conversation epochs: [`boot`], which boot runs above the
//! `AppState` literal (the read that learns each agent's retained epoch, then
//! one reconcile per agent), and the drain task that applies every publish
//! after it and reconciles each agent it changed.

use std::collections::BTreeSet;
use std::sync::Arc;

use brenn_conversation_epoch::{CONVERSATION_EPOCH_COMPONENT, EpochGoal};
use brenn_messaging::Messenger;
use brenn_messaging::system::SystemInbox;
use brenn_server::conversation_epoch::EpochReconciler;
use tokio::sync::Notify;
use tracing::info;

/// Boot's conversation-epoch step, run above the `AppState` literal: seed
/// every epoch agent's epoch from what its channel retains, then reconcile
/// each one, so an epoch published while the server was down supersedes the
/// stale conversation before any spawn path exists. Messaging boot has
/// already attached positions, so a successor inherits them.
///
/// `None` when no agent names an epoch channel. Otherwise the reconciler,
/// and the inbox the seed read through, which [`spawn_epoch_drain`] takes
/// once the spawn paths exist.
///
/// # Panics
///
/// When an agent names an epoch channel and `messenger` is `None`, or no
/// parked-notify binding exists for the participant: both are host wiring
/// bugs.
pub(crate) async fn boot(
    app_table: &brenn_lib::config::AppTable,
    messenger: Option<Arc<Messenger>>,
    system_notifiers: &[(&'static str, Arc<Notify>)],
    db: &brenn_db::Db,
    active_bridges: &brenn_server::active_bridge::ActiveBridges,
    alerts: brenn_obs::alerting::AlertDispatcher,
) -> Option<(Arc<EpochReconciler>, SystemInbox)> {
    let epoch_apps: std::collections::BTreeMap<String, String> = app_table
        .load()
        .iter()
        .filter_map(|(slug, app)| {
            app.conversation_epoch
                .clone()
                .map(|addr| (slug.clone(), addr))
        })
        .collect();
    if epoch_apps.is_empty() {
        return None;
    }
    let slugs: Vec<String> = epoch_apps.keys().cloned().collect();
    let epochs = Arc::new(EpochGoal::new(epoch_apps, alerts));
    let messenger = messenger
        .expect("a conversation_epoch names a declared durable channel, so messaging is on");
    let notify = system_notifiers
        .iter()
        .find(|(component, _)| *component == CONVERSATION_EPOCH_COMPONENT)
        .map(|(_, notify)| notify.clone())
        .expect(
            "the conversation-epoch spec is pushed exactly when an epoch channel is named, and \
             every subscribing spec gets a parked-notify binding",
        );
    let inbox = attach_and_seed(&epochs, messenger.clone(), notify).await;

    let reconciler = Arc::new(EpochReconciler::new(
        db.clone(),
        messenger,
        active_bridges.clone(),
        app_table.clone(),
        epochs,
    ));
    for slug in &slugs {
        reconciler.reconcile(slug).await;
    }
    Some((reconciler, inbox))
}

/// Attach the `system:conversation-epoch` participant and seed `epochs` from
/// what the epoch channels already retain, by the read
/// [`crate::state_channel::attach_and_seed`] describes. An empty channel leaves
/// the agent with no epoch.
pub(crate) async fn attach_and_seed(
    epochs: &EpochGoal,
    messenger: Arc<Messenger>,
    notify: Arc<Notify>,
) -> SystemInbox {
    crate::state_channel::attach_and_seed(
        CONVERSATION_EPOCH_COMPONENT,
        messenger,
        notify,
        |address, body| {
            epochs.apply(address, body);
        },
    )
    .await
}

/// Spawn the conversation-epoch drain loop: it applies later publishes — the
/// retained epochs were read by [`attach_and_seed`] — and reconciles each agent
/// they changed.
///
/// A batch is applied whole before anything is reconciled, so an epoch already
/// overtaken within the same batch mints no intermediate conversation.
///
/// Same process-lifetime, unsupervised policy as the other boot drain tasks
/// (dropped handle; panics are panic-hook-alerted).
pub(crate) fn spawn_epoch_drain(inbox: SystemInbox, reconciler: Arc<EpochReconciler>) {
    drop(tokio::spawn(async move {
        inbox
            .run(move |batch| {
                let reconciler = reconciler.clone();
                async move {
                    let mut changed = BTreeSet::new();
                    for (_, envelope) in batch {
                        changed
                            .extend(reconciler.epochs().apply(&envelope.channel, &envelope.body));
                    }
                    for slug in changed {
                        reconciler.reconcile(&slug).await;
                    }
                }
            })
            .await;
    }));
    info!("conversation_epoch: epoch drain task spawned");
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use brenn_conversation_epoch::conversation_epoch_spec;
    use brenn_lib::messaging::config::MessagingGlobalConfig;
    use brenn_lib::messaging::{ChannelEntry, ChannelScheme, MessagingDirectory};
    use brenn_messaging::query::NoopWakeRouter;
    use brenn_messaging::system::{
        SystemParticipantSpec, fold_spec_subscriptions, registrations_from_specs,
    };
    use brenn_messaging::testutils::test_channel_entry;
    use brenn_messaging::{Urgency, WakeRouter};
    use brenn_messaging_store::db::{init_db_memory, upsert_channels};
    use indexmap::IndexMap;

    use super::*;

    const EPOCH: &str = "cogsworth.epoch";
    /// The publisher standing in for a policy component.
    const PUBLISHER: &str = "test-epoch-publisher";

    fn epoch_address() -> String {
        format!("brenn:{EPOCH}")
    }

    /// A messenger over one durable epoch channel, with the conversation-epoch
    /// participant's subscription folded in exactly as boot folds it and its
    /// real code-built policy registered — so a matcher that would strand every
    /// epoch at the delivery gate strands them here too.
    fn messenger() -> Arc<Messenger> {
        let specs = vec![
            conversation_epoch_spec(&[epoch_address()]),
            SystemParticipantSpec::publish_only(
                PUBLISHER,
                ChannelScheme::Brenn,
                &[EPOCH.to_string()],
            ),
        ];
        let mut entries: Vec<ChannelEntry> = vec![test_channel_entry(EPOCH, vec![])];
        fold_spec_subscriptions(&mut entries, &specs[..1]);
        let db = init_db_memory();
        {
            let conn = db
                .try_lock()
                .expect("fresh in-memory db is uniquely owned here");
            upsert_channels(&conn, &entries);
        }
        Messenger::new(
            db,
            Arc::new(MessagingDirectory::with_entries(entries)),
            Arc::from("test"),
            Arc::new(IndexMap::new()),
            Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        )
        .with_subscriber_registrations(registrations_from_specs(&specs))
    }

    async fn publish(messenger: &Messenger, body: &str) {
        let outcome = messenger
            .publish_from_system(PUBLISHER, &epoch_address(), body, Urgency::Normal, None)
            .await;
        assert!(
            matches!(outcome, brenn_messaging::PublishResult::Ok { .. }),
            "the publisher spec grants exactly this channel, so the publish must land: \
             {outcome:?}",
        );
    }

    /// A reconciler over `messenger`'s db for one singleton epoch agent,
    /// `cogsworth`, whose only allowed user is `owner`.
    fn reconciler(
        messenger: &Arc<Messenger>,
        epochs: Arc<EpochGoal>,
        owner: &str,
    ) -> Arc<EpochReconciler> {
        Arc::new(EpochReconciler::new(
            messenger.db().clone(),
            messenger.clone(),
            brenn_server::active_bridge::ActiveBridges::new(),
            epoch_table(owner),
            epochs,
        ))
    }

    /// An app table holding one singleton epoch agent, `cogsworth`, whose only
    /// allowed user is `owner`.
    fn epoch_table(owner: &str) -> brenn_lib::config::AppTable {
        let mut app = brenn_lib::config::test_app_config("cogsworth");
        app.singleton = true;
        app.conversation_epoch = Some(epoch_address());
        app.allowed_users = vec![owner.to_string()];
        let mut map = IndexMap::new();
        map.insert("cogsworth".to_string(), app);
        brenn_lib::config::AppTable::new(Arc::new(map))
    }

    fn handle(alerts: brenn_obs::alerting::AlertDispatcher) -> Arc<EpochGoal> {
        Arc::new(EpochGoal::new(
            BTreeMap::from([("cogsworth".into(), epoch_address())]),
            alerts,
        ))
    }

    /// The seeding property boot depends on: an epoch published before this
    /// process existed is the handle's value before any spawn path, with no
    /// drain task anywhere.
    #[tokio::test]
    async fn an_epoch_retained_before_boot_is_the_seeded_value() {
        let (alerts, _alerts_task) = brenn_obs::alerting::noop_alert_dispatcher();
        let messenger = messenger();
        publish(&messenger, "3").await;

        let epochs = handle(alerts);
        let _inbox = attach_and_seed(&epochs, messenger.clone(), Arc::new(Notify::new())).await;

        assert_eq!(epochs.current("cogsworth").as_deref(), Some("3"));
    }

    #[tokio::test]
    async fn the_newest_retained_epoch_wins_and_is_trimmed() {
        let (alerts, _alerts_task) = brenn_obs::alerting::noop_alert_dispatcher();
        let messenger = messenger();
        publish(&messenger, "3").await;
        publish(&messenger, " 4\n").await;

        let epochs = handle(alerts);
        let _inbox = attach_and_seed(&epochs, messenger.clone(), Arc::new(Notify::new())).await;

        assert_eq!(epochs.current("cogsworth").as_deref(), Some("4"));
    }

    /// The first-boot case: nothing to read, so the agent has no epoch.
    #[tokio::test]
    async fn an_empty_epoch_channel_seeds_no_epoch() {
        let (alerts, _alerts_task) = brenn_obs::alerting::noop_alert_dispatcher();
        let messenger = messenger();

        let epochs = handle(alerts);
        let _inbox = attach_and_seed(&epochs, messenger.clone(), Arc::new(Notify::new())).await;

        assert_eq!(epochs.current("cogsworth"), None);
    }

    #[tokio::test]
    async fn a_refused_retained_epoch_seeds_no_epoch() {
        let (alerts, _alerts_task) = brenn_obs::alerting::noop_alert_dispatcher();
        let messenger = messenger();
        publish(&messenger, &"a".repeat(65)).await;

        let epochs = handle(alerts);
        let _inbox = attach_and_seed(&epochs, messenger.clone(), Arc::new(Notify::new())).await;

        assert_eq!(epochs.current("cogsworth"), None);
    }

    /// What the seeding read deliberately does *not* cover: a publish landing
    /// after boot reaches the handle through the drain task, over the position
    /// the read attached.
    #[tokio::test]
    async fn a_later_publish_is_applied_by_the_drain_task() {
        let (alerts, _alerts_task) = brenn_obs::alerting::noop_alert_dispatcher();
        let messenger = messenger();
        let epochs = handle(alerts);
        let notify = Arc::new(Notify::new());
        let inbox = attach_and_seed(&epochs, messenger.clone(), notify.clone()).await;
        assert_eq!(epochs.current("cogsworth"), None);

        spawn_epoch_drain(inbox, reconciler(&messenger, epochs.clone(), "bob"));
        publish(&messenger, "5").await;
        notify.notify_one();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while epochs.current("cogsworth").as_deref() != Some("5") {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the drain task did not apply the published epoch within the timeout",
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// A publish after boot supersedes the owner's current conversation
    /// through the drain.
    #[tokio::test]
    async fn a_later_publish_supersedes_the_current_conversation_through_the_drain() {
        let (alerts, _alerts_task) = brenn_obs::alerting::noop_alert_dispatcher();
        let messenger = messenger();
        let (bob, old) = {
            let conn = messenger.db().lock().await;
            let bob = brenn_db::auth::user::create_user(&conn, "bob", "$argon2id$fake");
            let old = brenn_db::conversation::create_conversation(&conn, bob, "cogsworth", false);
            (bob, old)
        };
        let epochs = handle(alerts);
        let notify = Arc::new(Notify::new());
        let inbox = attach_and_seed(&epochs, messenger.clone(), notify.clone()).await;

        spawn_epoch_drain(inbox, reconciler(&messenger, epochs, "bob"));
        publish(&messenger, "5").await;
        notify.notify_one();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let current = {
                let conn = messenger.db().lock().await;
                brenn_db::conversation::get_singleton_conversation(&conn, bob, "cogsworth")
            };
            if let Some(current) = current
                && current.epoch.as_deref() == Some("5")
                && current.id != old
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the drain did not supersede the conversation within the timeout",
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Boot's epoch step against an epoch published while the server was down:
    /// the seed learns it and the reconcile supersedes the stale conversation,
    /// keeping the old row, without spawning anything.
    #[tokio::test]
    async fn boot_supersedes_a_conversation_whose_epoch_moved_while_the_server_was_down() {
        let (alerts, _alerts_task) = brenn_obs::alerting::noop_alert_dispatcher();
        let messenger = messenger();
        let (bob, old) = {
            let conn = messenger.db().lock().await;
            let bob = brenn_db::auth::user::create_user(&conn, "bob", "$argon2id$fake");
            let old =
                brenn_db::conversation::create_singleton_successor(&conn, bob, "cogsworth", "1");
            (bob, old)
        };
        publish(&messenger, "2").await;

        let bridges = brenn_server::active_bridge::ActiveBridges::new();
        let notifiers = vec![(CONVERSATION_EPOCH_COMPONENT, Arc::new(Notify::new()))];
        let (reconciler, _inbox) = boot(
            &epoch_table("bob"),
            Some(messenger.clone()),
            &notifiers,
            messenger.db(),
            &bridges,
            alerts,
        )
        .await
        .expect("an epoch agent boots its epoch step");

        assert_eq!(
            reconciler.epochs().current("cogsworth").as_deref(),
            Some("2")
        );
        let conn = messenger.db().lock().await;
        let singleton = brenn_db::conversation::get_singleton_conversation(&conn, bob, "cogsworth")
            .expect("the agent has a singleton");
        assert_ne!(singleton.id, old);
        assert_eq!(singleton.epoch.as_deref(), Some("2"));
        assert_eq!(
            brenn_db::conversation::get_conversation(&conn, old)
                .epoch
                .as_deref(),
            Some("1"),
            "the old row is kept",
        );
        assert_eq!(
            brenn_db::conversation::list_conversations(&conn, bob, "cogsworth").len(),
            2
        );
        drop(conn);
        assert!(
            bridges.get_for_app("cogsworth").await.is_empty(),
            "boot's epoch step spawns no session",
        );
    }

    /// No epoch agent: the step is `None` and never reaches for messaging.
    #[tokio::test]
    async fn boot_without_an_epoch_agent_is_none() {
        let (alerts, _alerts_task) = brenn_obs::alerting::noop_alert_dispatcher();
        let mut app = brenn_lib::config::test_app_config("cogsworth");
        app.singleton = true;
        app.conversation_epoch = None;
        let mut map = IndexMap::new();
        map.insert("cogsworth".to_string(), app);
        let table = brenn_lib::config::AppTable::new(Arc::new(map));

        assert!(
            boot(
                &table,
                None,
                &[],
                &init_db_memory(),
                &brenn_server::active_bridge::ActiveBridges::new(),
                alerts,
            )
            .await
            .is_none()
        );
    }
}
