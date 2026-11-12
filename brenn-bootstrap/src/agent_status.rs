//! Boot wiring for agent status: the reporter, its ledger seeded from the
//! retained accounts body, and the drain task that keeps the participant's
//! position on the accounts channel moving.

use std::collections::BTreeMap;
use std::sync::Arc;

use brenn_envelope::agent::AGENT_STATUS_SENDER;
use brenn_lib::config::ClaudeProfile;
use brenn_messaging::Messenger;
use brenn_messaging::agent_status::{ACCOUNTS_ADDRESS, AGENT_STATUS_COMPONENT};
use brenn_messaging::system::SystemInbox;
use brenn_server::agent_status::{AgentStatusReporter, MessengerSink, Publishing};
use tokio::sync::Notify;
use tracing::info;

/// Start the reporter over every configured profile.
///
/// When the agent-status channels are declared — the planner then pushed the
/// participant, and boot gave it a parked-notify binding, `notify` — the
/// participant is attached, the ledger is seeded from what the accounts channel
/// retains by the read [`crate::state_channel::attach_and_seed`] describes, and
/// the reporter publishes. The returned inbox is the one that did the read.
/// Without the channels the reporter tracks and publishes nothing.
///
/// Runs above the `AppState` literal: every spawn path is below it, and a turn
/// charged before the seed would be lost when the seed replaced the record.
pub(crate) async fn boot(
    profiles: &BTreeMap<String, ClaudeProfile>,
    messenger: Option<Arc<Messenger>>,
    notify: Option<Arc<Notify>>,
) -> (AgentStatusReporter, Option<SystemInbox>) {
    let Some(notify) = notify else {
        return (AgentStatusReporter::start(profiles, None), None);
    };
    let messenger = messenger.expect(
        "the agent-status channels are declared channels, so messaging is on when they are",
    );
    let mut retained_accounts = None;
    let inbox = crate::state_channel::attach_and_seed(
        AGENT_STATUS_COMPONENT,
        messenger.clone(),
        notify,
        |address, envelope| {
            assert_eq!(
                address, ACCOUNTS_ADDRESS,
                "system:{AGENT_STATUS_COMPONENT} subscribes to the accounts channel alone"
            );
            assert_ledger_writer(&envelope.sender);
            retained_accounts = Some(envelope.body.clone());
        },
    )
    .await;
    let reporter = AgentStatusReporter::start(
        profiles,
        Some(Publishing {
            sink: Arc::new(MessengerSink(messenger)),
            retained_accounts,
        }),
    );
    info!("agent_status: reporter publishing on the agent-status channels");
    (reporter, Some(inbox))
}

/// The accounts channel is the ledger's storage, written by the reporter alone.
///
/// # Panics
///
/// If `sender` is anyone else: a principal the deployment granted publish on
/// the accounts channel wrote the ledger, and seeding from it would adopt
/// whatever spend and flags it chose. The operator removes the grant and
/// clears the channel.
fn assert_ledger_writer(sender: &str) {
    assert_eq!(
        sender, AGENT_STATUS_SENDER,
        "the latest message on {ACCOUNTS_ADDRESS} was published by `{sender}`, not by \
         {AGENT_STATUS_SENDER}; remove that principal's publish grant on the channel and clear \
         it to start every profile at zero"
    );
}

/// Spawn the drain loop over the accounts channel. The ledger was read once by
/// [`boot`]; every later message there is the reporter's own publish, or one
/// the next snapshot overwrites, so the loop only moves the position.
///
/// Same process-lifetime, unsupervised policy as the other boot drain tasks
/// (dropped handle; panics are panic-hook-alerted).
pub(crate) fn spawn_accounts_drain(inbox: SystemInbox) {
    drop(tokio::spawn(async move {
        inbox.run(|_batch| async {}).await;
    }));
    info!("agent_status: accounts drain task spawned");
}

#[cfg(test)]
mod tests {
    use brenn_envelope::agent::{AccountFlag, bucket};
    use brenn_envelope::agent::{AgentAccounts, decode};
    use brenn_lib::config::{ClaudeCredential, SecretString};
    use brenn_lib::messaging::config::{Depth, MessagingGlobalConfig};
    use brenn_lib::messaging::{ChannelEntry, ChannelScheme, MessagingDirectory};
    use brenn_messaging::agent_status::{ENDED_ADDRESS, SESSIONS_ADDRESS, agent_status_spec};
    use brenn_messaging::query::NoopWakeRouter;
    use brenn_messaging::system::{fold_spec_subscriptions, registrations_from_specs};
    use brenn_messaging::testutils::test_channel_entry;
    use brenn_messaging::{PublishResult, Urgency, WakeRouter};
    use brenn_messaging_store::db::{init_db_memory, upsert_channels};
    use indexmap::IndexMap;

    use super::*;

    /// One subscription profile, `personal`.
    fn profiles() -> BTreeMap<String, ClaudeProfile> {
        BTreeMap::from([(
            "personal".to_string(),
            ClaudeProfile {
                credential: ClaudeCredential::OauthToken(SecretString::new("tok".to_string())),
                expires: None,
            },
        )])
    }

    /// The durable accounts and ended channels, with the participant's
    /// subscription folded in and its real code-built policy registered. The
    /// ephemeral sessions channel is only looked up by the spec here.
    fn messenger() -> Arc<Messenger> {
        let entry = |address: &'static str| {
            let (scheme, name) = ChannelScheme::split(address).expect("a schemed address");
            let mut entry = test_channel_entry(name, vec![]);
            entry.address = address.to_string();
            entry.transport_type = scheme;
            entry.resolved_channel.retain_depth = Depth::Bounded(1);
            entry.resolved_channel.standing_retain_depth = Depth::Bounded(1);
            entry
        };
        let declared = [
            entry(SESSIONS_ADDRESS),
            entry(ENDED_ADDRESS),
            entry(ACCOUNTS_ADDRESS),
        ];
        let specs = vec![agent_status_spec(&declared).expect("all three are declared")];
        let mut entries: Vec<ChannelEntry> = declared[1..].to_vec();
        fold_spec_subscriptions(&mut entries, &specs);
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

    async fn retained_accounts(messenger: &Arc<Messenger>) -> AgentAccounts {
        let inbox = SystemInbox::new(
            AGENT_STATUS_COMPONENT,
            messenger.clone(),
            Arc::new(Notify::new()),
        );
        let snapshot = inbox.snapshot().await;
        let (_, window) = snapshot
            .iter()
            .find(|(address, _)| address == ACCOUNTS_ADDRESS)
            .expect("the participant reads the accounts channel");
        let (_, envelope) = window.entries.last().expect("a retained accounts body");
        decode(&envelope.body).expect("an accounts body")
    }

    /// The boot read and the boot publish, through a real durable channel: the
    /// first boot publishes a record with no flags; a flag published before the
    /// next boot seeds its ledger, which publishes it back.
    #[tokio::test]
    async fn the_ledger_round_trips_through_the_accounts_channel() {
        let messenger = messenger();
        let (reporter, inbox) = boot(
            &profiles(),
            Some(messenger.clone()),
            Some(Arc::new(Notify::new())),
        )
        .await;
        assert!(inbox.is_some());
        reporter.flush().await;
        let first = retained_accounts(&messenger).await;
        assert!(first.accounts[0].exhausted_buckets.is_empty());

        let mut spent = first.clone();
        // An instant no boot publishes at, so the next boot's publish is
        // told apart from this one.
        spent.at_ms = 1;
        let flag = AccountFlag {
            at_ms: 1,
            session: Some("7".to_string()),
            message: None,
            // 2100-01-01: far enough out that no boot in the test lifts it.
            until_ms: Some(4_102_444_800_000),
            kind: Some("five_hour".to_string()),
        };
        spent.accounts[0]
            .exhausted_buckets
            .insert(bucket::FIVE_HOUR.to_string(), flag.clone());
        let outcome = messenger
            .publish_from_system(
                AGENT_STATUS_COMPONENT,
                ACCOUNTS_ADDRESS,
                &brenn_envelope::agent::encode(&spent),
                Urgency::Normal,
                None,
            )
            .await;
        assert!(matches!(outcome, PublishResult::Ok { .. }), "{outcome:?}");

        let (reporter, _inbox) = boot(
            &profiles(),
            Some(messenger.clone()),
            Some(Arc::new(Notify::new())),
        )
        .await;
        reporter.flush().await;
        let latest = retained_accounts(&messenger).await;
        assert_ne!(
            latest.at_ms, 1,
            "the second boot publishes its seeded ledger"
        );
        assert_eq!(
            latest.accounts[0].exhausted_buckets.get(bucket::FIVE_HOUR),
            Some(&flag)
        );
    }

    /// The participant's policy refuses a channel outside its three.
    #[tokio::test]
    async fn the_participant_cannot_publish_elsewhere() {
        let messenger = messenger();
        let outcome = messenger
            .publish_from_system(
                AGENT_STATUS_COMPONENT,
                "brenn:elsewhere",
                "{}",
                Urgency::Normal,
                None,
            )
            .await;
        assert!(!matches!(outcome, PublishResult::Ok { .. }), "{outcome:?}");
    }

    /// A ledger body some other principal published is not seeded from.
    #[test]
    #[should_panic(expected = "remove that principal's publish grant")]
    fn a_ledger_written_by_another_principal_panics() {
        assert_ledger_writer("app:assistant");
    }

    /// Without the channels the reporter publishes nothing and attaches nothing.
    #[tokio::test]
    async fn without_the_channels_there_is_no_inbox() {
        let (reporter, inbox) = boot(&profiles(), None, None).await;
        assert!(inbox.is_none());
        reporter.session_ended(1, true);
    }
}
