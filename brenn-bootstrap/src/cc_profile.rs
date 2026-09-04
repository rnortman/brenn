//! Boot wiring for Claude account profiles.
//!
//! Two pieces, in the order boot runs them: the synchronous read that learns
//! the retained goals before any spawn path exists, and the drain task that
//! applies every publish after it.

use std::sync::Arc;

use brenn_cc_profile::{CC_PROFILE_COMPONENT, ProfileGoal};
use brenn_messaging::Messenger;
use brenn_messaging::system::SystemInbox;
use brenn_server::active_bridge::ActiveBridges;
use tokio::sync::Notify;
use tracing::info;

/// Attach the `system:cc-profile` participant and seed `goal` from what the
/// goal channels already retain.
///
/// The retained goal is learned by a *read*, not by delivery: a system
/// subscriber's position is durable, so after the first boot the retained
/// message is behind the cursor and never arrives as new. The returned inbox is
/// the one that did the read, so the drain task built on it resumes from the
/// position attached here.
pub(crate) async fn attach_and_seed(
    goal: &ProfileGoal,
    messenger: Arc<Messenger>,
    notify: Arc<Notify>,
) -> SystemInbox {
    let inbox = SystemInbox::new(CC_PROFILE_COMPONENT, messenger, notify);
    inbox.attach().await;
    for (address, window) in inbox.snapshot().await {
        // Newest last, new or context alike: the channel carries state, so only
        // the latest message means anything. An empty channel leaves the
        // first-allowed seed in place.
        if let Some((_, envelope)) = window.entries.last() {
            goal.apply(&address, &envelope.body);
        }
    }
    inbox
}

/// Spawn the cc-profile drain loop: later publishes only — the retained goals
/// were read by [`attach_and_seed`], before any spawn path existed.
///
/// Same process-lifetime, unsupervised policy as the other boot drain tasks
/// (dropped handle; panics are panic-hook-alerted).
pub(crate) fn spawn_goal_drain(inbox: SystemInbox, goal: Arc<ProfileGoal>, bridges: ActiveBridges) {
    drop(tokio::spawn(async move {
        inbox
            .run(move |batch| {
                let goal = goal.clone();
                let bridges = bridges.clone();
                async move {
                    for (_, envelope) in batch {
                        let changed = goal.apply(&envelope.channel, &envelope.body);
                        if changed.is_empty() {
                            continue;
                        }
                        // A live conversation of a changed app is now on the
                        // wrong account, and swaps at its next idle moment.
                        bridges.reconsider_profiles(&changed).await;
                    }
                }
            })
            .await;
    }));
    info!("cc_profile: goal drain task spawned");
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use brenn_cc_profile::cc_profile_spec;
    use brenn_lib::config::{AppClaudeProfiles, ClaudeProfile, SecretString};
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

    const GOAL: &str = "cc-profile.pa";
    /// The publisher standing in for a policy component.
    const PUBLISHER: &str = "test-goal-publisher";

    fn goal_address() -> String {
        format!("brenn:{GOAL}")
    }

    fn profiles() -> BTreeMap<String, ClaudeProfile> {
        ["main", "spare"]
            .into_iter()
            .map(|name| {
                (
                    name.to_string(),
                    ClaudeProfile {
                        token: SecretString::new(format!("token-{name}")),
                        expires: None,
                    },
                )
            })
            .collect()
    }

    /// One app allowed both profiles and bound to the goal channel.
    fn apps() -> BTreeMap<String, AppClaudeProfiles> {
        BTreeMap::from([(
            "pa".to_string(),
            AppClaudeProfiles {
                allowed: vec!["main".to_string(), "spare".to_string()],
                goal: Some(goal_address()),
            },
        )])
    }

    /// A messenger over one durable goal channel, with the cc-profile
    /// participant's subscription folded in exactly as boot folds it and its
    /// real code-built policy registered — so a matcher that would strand every
    /// goal at the delivery gate strands them here too.
    fn messenger() -> Arc<Messenger> {
        let specs = vec![
            cc_profile_spec(&[goal_address()]),
            SystemParticipantSpec::publish_only(
                PUBLISHER,
                ChannelScheme::Brenn,
                &[GOAL.to_string()],
            ),
        ];
        let mut entries: Vec<ChannelEntry> = vec![test_channel_entry(GOAL, vec![])];
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
            .publish_from_system(PUBLISHER, &goal_address(), body, Urgency::Normal, None)
            .await;
        assert!(
            matches!(outcome, brenn_messaging::PublishResult::Ok { .. }),
            "the publisher spec grants exactly this channel, so the publish must land: \
             {outcome:?}",
        );
    }

    fn handle(alerts: brenn_obs::alerting::AlertDispatcher) -> Arc<ProfileGoal> {
        Arc::new(ProfileGoal::new(profiles(), apps(), alerts))
    }

    /// The seeding property boot depends on: a goal published before this
    /// process existed is the handle's value by the time `AppState` is built,
    /// with no drain task anywhere.
    #[tokio::test]
    async fn a_goal_retained_before_boot_is_the_seeded_value() {
        let (alerts, _alerts_task) = brenn_obs::alerting::noop_alert_dispatcher();
        let messenger = messenger();
        publish(&messenger, "spare").await;

        let goal = handle(alerts);
        let _inbox = attach_and_seed(&goal, messenger.clone(), Arc::new(Notify::new())).await;

        assert_eq!(goal.current("pa").as_deref(), Some("spare"));
    }

    /// Trailing whitespace is the publisher's, not the operator's: the whole
    /// doctype is a trimmed name.
    #[tokio::test]
    async fn the_newest_retained_message_wins_and_is_trimmed() {
        let (alerts, _alerts_task) = brenn_obs::alerting::noop_alert_dispatcher();
        let messenger = messenger();
        publish(&messenger, "spare").await;
        publish(&messenger, "  main\n").await;

        let goal = handle(alerts);
        let _inbox = attach_and_seed(&goal, messenger.clone(), Arc::new(Notify::new())).await;

        assert_eq!(goal.current("pa").as_deref(), Some("main"));
    }

    /// An empty channel is the first-boot case: nothing to read, so the app
    /// keeps the first entry of its `claude_profiles`.
    #[tokio::test]
    async fn an_empty_goal_channel_leaves_the_first_allowed_seed() {
        let (alerts, _alerts_task) = brenn_obs::alerting::noop_alert_dispatcher();
        let messenger = messenger();

        let goal = handle(alerts);
        let _inbox = attach_and_seed(&goal, messenger.clone(), Arc::new(Notify::new())).await;

        assert_eq!(goal.current("pa").as_deref(), Some("main"));
    }

    /// A retained name the agent may not run under — the operator removed it
    /// from the list and restarted. Rejected for that agent; the seed stands.
    #[tokio::test]
    async fn a_rejected_retained_goal_leaves_the_previous_value() {
        let (alerts, _alerts_task) = brenn_obs::alerting::noop_alert_dispatcher();
        let messenger = messenger();
        publish(&messenger, "legacy").await;

        let goal = handle(alerts);
        let _inbox = attach_and_seed(&goal, messenger.clone(), Arc::new(Notify::new())).await;

        assert_eq!(goal.current("pa").as_deref(), Some("main"));
    }

    /// What the seeding read deliberately does *not* cover: a publish landing
    /// after boot reaches the handle through the drain task, over the position
    /// the read attached.
    #[tokio::test]
    async fn a_later_publish_is_applied_by_the_drain_task() {
        let (alerts, _alerts_task) = brenn_obs::alerting::noop_alert_dispatcher();
        let messenger = messenger();
        let goal = handle(alerts);
        let notify = Arc::new(Notify::new());
        let inbox = attach_and_seed(&goal, messenger.clone(), notify.clone()).await;
        assert_eq!(goal.current("pa").as_deref(), Some("main"));

        spawn_goal_drain(inbox, goal.clone(), ActiveBridges::new());
        publish(&messenger, "spare").await;
        notify.notify_one();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while goal.current("pa").as_deref() != Some("spare") {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the drain task did not apply the published goal within the timeout",
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
