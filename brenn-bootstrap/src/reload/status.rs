//! The bootstrap side of the outcome document: turning a prepared reload's
//! delta into the shape the status body reports it in.
//!
//! The body itself — its fields, its version and the publish that carries it —
//! is `brenn_messaging::config_reload`, beside the address it goes to, because
//! it is a bus contract and its readers are not this crate. What is here is the
//! one piece that is: `PlanDelta` is the prepare phase's own vocabulary.

use brenn_messaging::Messenger;
use brenn_messaging::config_reload::{
    CONFIG_RELOAD_COMPONENT, ReloadStatus, StatusDelta, publish_status,
};
use brenn_messaging::system::{SystemInbox, SystemParticipantSpec};
use tracing::info;

use super::delta::PlanDelta;

/// Give the reload facility its position on the request channel and publish the
/// outcome boot has: this process now projects this document.
///
/// A no-op unless the document declared the pair, which is what puts the
/// participant in the plan — the facility is off in a deployment that never
/// asked for it, and publishing to a channel nobody declared would panic.
///
/// The attach comes first and the publish immediately after: the position has
/// to exist before anything can put a request into retention, or a request
/// released below it would never be served; and the retained body has to name
/// this process's document from the moment there is a process to ask about.
pub(crate) async fn attach_and_publish_booted(
    participants: &[SystemParticipantSpec],
    messenger: &std::sync::Arc<Messenger>,
    document_sha256: &str,
    root: Option<String>,
) {
    if !participants
        .iter()
        .any(|spec| spec.component == CONFIG_RELOAD_COMPONENT)
    {
        return;
    }
    SystemInbox::attach_for(CONFIG_RELOAD_COMPONENT, messenger).await;
    publish_status(
        messenger,
        &ReloadStatus::booted(document_sha256.to_string(), root),
    )
    .await;
    info!(
        document_sha256 = %document_sha256,
        "reload facility declared; booted status published"
    );
}

impl From<&PlanDelta> for StatusDelta {
    fn from(delta: &PlanDelta) -> Self {
        Self {
            consumers_added: delta.consumers_added.clone(),
            consumers_removed: delta.consumers_removed.clone(),
            consumers_changed: delta.consumers_changed.clone(),
            channels_added: addresses(&delta.channels_added),
            channels_removed: addresses(&delta.channels_removed),
            channels_changed: delta
                .channels_changed
                .iter()
                .map(|change| change.new.address.clone())
                .collect(),
            channels_described: addresses(&delta.channels_described),
        }
    }
}

fn addresses(entries: &[std::sync::Arc<brenn_lib::messaging::ChannelEntry>]) -> Vec<String> {
    entries.iter().map(|entry| entry.address.clone()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use brenn_messaging::{PublishResult, Urgency};

    #[test]
    fn a_plan_delta_projects_to_addresses_and_slugs() {
        use brenn_lib::messaging::ChannelEntry;
        use brenn_lib::messaging::test_support::test_channel_entry;
        use std::sync::Arc;

        fn entry(address: &str) -> Arc<ChannelEntry> {
            let mut entry = test_channel_entry(address, Vec::new());
            entry.address = address.to_string();
            Arc::new(entry)
        }

        let changed = entry("brenn:moved");
        let plan_delta = PlanDelta {
            channels_added: vec![entry("brenn:new")],
            channels_removed: vec![entry("brenn:gone")],
            channels_changed: vec![super::super::delta::ChannelChange {
                old: Arc::clone(&changed),
                new: Arc::clone(&changed),
            }],
            channels_described: vec![entry("brenn:notes")],
            consumers_added: vec!["watcher".to_string()],
            consumers_removed: vec!["old".to_string()],
            consumers_changed: vec!["rewired".to_string()],
        };
        let status: StatusDelta = (&plan_delta).into();
        assert_eq!(status.channels_added, vec!["brenn:new".to_string()]);
        assert_eq!(status.channels_removed, vec!["brenn:gone".to_string()]);
        // A changed entry is reported at the address it will have, which is the
        // candidate's — the side the process is converging to.
        assert_eq!(status.channels_changed, vec!["brenn:moved".to_string()]);
        assert_eq!(status.channels_described, vec!["brenn:notes".to_string()]);
        assert_eq!(status.consumers_added, vec!["watcher".to_string()]);
        assert_eq!(status.consumers_removed, vec!["old".to_string()]);
        assert_eq!(status.consumers_changed, vec!["rewired".to_string()]);
    }

    /// The facility's own identity publishes onto the operator's status
    /// channel, and nowhere else. The reachability half is the risk: the ACL is
    /// exact-scoped and code-built, so a bare-name mismatch would leave every
    /// outcome `AclDenied` with nothing but a panic at the first boot to say so.
    #[tokio::test]
    async fn the_facility_publishes_its_outcome_and_reaches_nothing_else() {
        use brenn_lib::config::BrennConfig;
        use brenn_lib::messaging::config::{ChannelConfigRaw, Depth};
        use brenn_messaging::config_reload::{RELOAD_ADDRESS, STATUS_ADDRESS};
        use brenn_messaging::query::MessageQuery;
        use brenn_messaging_boot::test_fixtures::durable_channel;
        use brenn_obs::alerting::AlertDispatcher;
        use brenn_server::test_support::init_db_memory;
        use std::sync::Arc;

        fn channel(address: &str) -> ChannelConfigRaw {
            ChannelConfigRaw {
                retain_depth: Some(Depth::Bounded(4)),
                ..durable_channel(address, Depth::Bounded(4))
            }
        }

        let mut config = BrennConfig::default();
        config.channels.push(channel("brenn:surface.index"));
        config.channels.push(channel(RELOAD_ADDRESS));
        config.channels.push(channel(STATUS_ADDRESS));
        // A reader holding channel access, so the retained body can be pulled
        // back out through the ordinary read gate.
        let mut reader =
            brenn_server::test_support::app_config::minimal_app_config("some-reader", None, vec![]);
        reader.policy =
            brenn_lib::access::test_fixtures::delivery_policy_for_addresses([STATUS_ADDRESS]);
        let mut apps_map: indexmap::IndexMap<String, brenn_lib::config::AppConfig> =
            indexmap::IndexMap::new();
        apps_map.insert("some-reader".to_string(), reader);
        let apps = Arc::new(apps_map);
        let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();

        let result = brenn_messaging_boot::test_fixtures::boot_messaging_with(
            &config,
            init_db_memory(),
            &apps,
            alert_dispatcher,
            "brenn://test",
        )
        .await;
        let messenger = result.messenger.as_ref().expect("messaging must be up");

        let status = ReloadStatus::booted("abc123".to_string(), None);
        publish_status(messenger, &status).await;

        let envelopes = messenger
            .query(&MessageQuery {
                channel: STATUS_ADDRESS.to_string(),
                limit: 10,
                before: None,
                after: None,
                sender: None,
                search: None,
                calling_app_slug: "some-reader".to_string(),
            })
            .await
            .expect("the status channel is declared and readable");
        assert_eq!(envelopes.len(), 1);
        assert_eq!(envelopes[0].sender, "system:config-reload");
        let read: ReloadStatus =
            serde_json::from_str(&envelopes[0].body).expect("the retained body is the schema");
        assert_eq!(read, status);

        // The facility may report; it may not manufacture the request it
        // reports on.
        let denied = messenger
            .publish_from_system(
                CONFIG_RELOAD_COMPONENT,
                RELOAD_ADDRESS,
                "{}",
                Urgency::Normal,
                None,
            )
            .await;
        assert!(
            matches!(denied, PublishResult::AclDenied(..)),
            "the reload identity's publish ACL covers the status channel alone; got {denied:?}"
        );
    }

    /// Boot's own half: with the pair declared the participant is in the plan,
    /// so the inbox is attached and the retained body names the document boot
    /// loaded. Without it nothing is attached and nothing is published — a
    /// publish there would panic on an undeclared channel, which would be a
    /// hard boot failure for every deployment that never asked for the
    /// facility.
    #[tokio::test]
    async fn boot_publishes_the_booted_outcome_only_where_the_facility_is_declared() {
        use brenn_lib::config::BrennConfig;
        use brenn_lib::messaging::config::{ChannelConfigRaw, Depth};
        use brenn_messaging::config_reload::{Outcome, RELOAD_ADDRESS, STATUS_ADDRESS};
        use brenn_messaging::query::MessageQuery;
        use brenn_messaging_boot::test_fixtures::durable_channel;
        use brenn_obs::alerting::AlertDispatcher;
        use brenn_server::test_support::init_db_memory;
        use std::sync::Arc;

        fn channel(address: &str) -> ChannelConfigRaw {
            ChannelConfigRaw {
                retain_depth: Some(Depth::Bounded(4)),
                ..durable_channel(address, Depth::Bounded(4))
            }
        }

        async fn boot_and_read(declare_the_pair: bool) -> Vec<String> {
            let mut config = BrennConfig::default();
            config.channels.push(channel("brenn:surface.index"));
            config.channels.push(channel("brenn:work"));
            if declare_the_pair {
                config.channels.push(channel(RELOAD_ADDRESS));
                config.channels.push(channel(STATUS_ADDRESS));
            }
            let mut reader = brenn_server::test_support::app_config::minimal_app_config(
                "some-reader",
                None,
                vec![],
            );
            reader.policy = brenn_lib::access::test_fixtures::delivery_policy_for_addresses([
                STATUS_ADDRESS,
                "brenn:work",
            ]);
            let mut apps_map: indexmap::IndexMap<String, brenn_lib::config::AppConfig> =
                indexmap::IndexMap::new();
            apps_map.insert("some-reader".to_string(), reader);
            let apps = Arc::new(apps_map);
            let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();

            let result = brenn_messaging_boot::test_fixtures::boot_messaging_with(
                &config,
                init_db_memory(),
                &apps,
                alert_dispatcher,
                "brenn://test",
            )
            .await;
            let messenger = result.messenger.clone().expect("messaging must be up");

            attach_and_publish_booted(
                &result.system_participants,
                &messenger,
                "d0cd0c",
                Some("/etc/brenn/main.brenn".to_string()),
            )
            .await;

            if !declare_the_pair {
                // Nothing to read: the channel does not exist. That the call
                // above returned at all is the assertion.
                return Vec::new();
            }
            messenger
                .query(&MessageQuery {
                    channel: STATUS_ADDRESS.to_string(),
                    limit: 10,
                    before: None,
                    after: None,
                    sender: None,
                    search: None,
                    calling_app_slug: "some-reader".to_string(),
                })
                .await
                .expect("the status channel is declared and readable")
                .into_iter()
                .map(|envelope| envelope.body)
                .collect()
        }

        let bodies = boot_and_read(true).await;
        assert_eq!(bodies.len(), 1);
        let read: ReloadStatus =
            serde_json::from_str(&bodies[0]).expect("the retained body is the schema");
        assert_eq!(read.outcome, Outcome::Booted);
        assert_eq!(read.generation, 0);
        assert_eq!(read.running_document_sha256, "d0cd0c");
        assert_eq!(read.document_sha256.as_deref(), Some("d0cd0c"));
        assert_eq!(read.root.as_deref(), Some("/etc/brenn/main.brenn"));

        assert!(boot_and_read(false).await.is_empty());
    }
}
