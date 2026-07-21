//! System participants: bus principals that only Brenn's own code can mint,
//! operating under code-built policies.
//!
//! A `system:` identity attests naming authority — no config file can produce
//! one — not what executes behind it. This module is the substrate for
//! authoring them: [`SystemParticipantSpec`] declares a participant (its
//! component name, policy, and static subscriptions) so bootstrap derives its
//! registry entry, directory subscriber entries, deliverability validation,
//! and delivery binding from one declaration; [`SystemInbox`] is the shared
//! park/wake drain loop for the subscribing ones.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Notify;

use crate::access::acl::ChannelMatcher;
use crate::access::{AppCapability, AppPolicy};

use super::config::{Depth, NoiseLevel};
use super::ingress::IngressOrBus;
use super::{
    ChannelEntry, MessageEnvelope, Messenger, ParticipantId, SubscriberEntry, SubscriberEntryKind,
    SubscriberRegistration, WakeEconomics,
};

/// A system participant: a bus principal that only Brenn's own code can
/// mint, operating under a code-built policy. What executes behind it is
/// not part of the identity's meaning.
///
/// Bootstrap collects every spec and derives, in one place: the subscriber
/// registration ([`registrations_from_specs`]), the directory subscriber
/// entries for each subscription ([`fold_spec_subscriptions`]), inclusion in
/// the boot deliverability validator, and — for specs with subscriptions — a
/// parked-notify delivery binding whose `Notify` is handed to the
/// participant's drain task.
pub struct SystemParticipantSpec {
    /// Component name; the participant's identity is `system:<component>`.
    pub component: &'static str,
    /// Code-built policy; no config can produce one.
    pub policy: AppPolicy,
    /// Static subscriptions (canonical channel addresses). Empty for
    /// publish-only participants. Non-empty ⇒ the participant is a
    /// subscriber and must be given a drain task (see [`SystemInbox`]).
    pub subscriptions: Vec<String>,
}

impl SystemParticipantSpec {
    /// A publish-only system participant granted exactly `MessagingPublish` plus
    /// one exact-match `brenn_publish` ACL per bare (scheme-stripped) channel
    /// name, and no subscriptions. The substrate shape for a code-built boot
    /// publisher (the surface self-description publisher): code-built, a fixed set
    /// of channels, no consumer side. An empty slice yields publish authority with
    /// no channel it may write — a degenerate spec the caller is expected to avoid.
    pub fn publish_only(component: &'static str, bare_channels: &[String]) -> Self {
        let mut policy = AppPolicy::default();
        policy.grants.insert(AppCapability::MessagingPublish);
        for bare in bare_channels {
            policy
                .acls
                .brenn_publish
                .push(ChannelMatcher::Exact(bare.clone()));
        }
        Self {
            component,
            policy,
            subscriptions: vec![],
        }
    }
}

/// Derive the subscriber-registry entries for a set of system participant
/// specs: `System(component)` → `{ policy, wake: Eager }`. System
/// participants are cheap to wake (a parked task on a `Notify`), so eager
/// delivery is never urgency-gated.
///
/// # Panics
///
/// Panics on a duplicate component name (host wiring bug — each system
/// participant is declared exactly once).
pub fn registrations_from_specs(
    specs: &[SystemParticipantSpec],
) -> HashMap<SubscriberEntryKind, SubscriberRegistration> {
    let mut map = HashMap::new();
    for spec in specs {
        let prev = map.insert(
            SubscriberEntryKind::System(spec.component.to_string()),
            SubscriberRegistration {
                policy: Arc::new(spec.policy.clone()),
                wake: WakeEconomics::Eager,
            },
        );
        assert!(
            prev.is_none(),
            "system participant component {:?} declared twice — host wiring bug",
            spec.component,
        );
    }
    map
}

/// Fold each spec's static subscriptions into the channel entries as
/// `System(component)` directory subscribers, ahead of directory
/// finalization — so system subscriptions flow through the same
/// deliverability validation and dispatch as every other subscriber.
///
/// Depths are `Unbounded` and noise `Silent` deliberately: a bounded push
/// depth would let overflow silently retire substrate messages (e.g. tool
/// requests) under load — the silent-drop class the substrate must never
/// have.
///
/// # Panics
///
/// Panics when a subscription address matches no channel entry, or when a spec
/// would install a second `System(component)` subscriber on the same channel (a
/// repeated address in one spec's `subscriptions`) — both host wiring bugs (the
/// same bootstrap builds both the entries and the specs). A duplicate entry
/// would emit a second push row per publish and drive the handler to execute the
/// message twice: the silent-double-delivery class the substrate must never
/// have, so it fails boot like the sibling duplicate-wiring checks.
pub fn fold_spec_subscriptions(entries: &mut [ChannelEntry], specs: &[SystemParticipantSpec]) {
    for spec in specs {
        for address in &spec.subscriptions {
            let entry = entries
                .iter_mut()
                .find(|e| &e.address == address)
                .unwrap_or_else(|| {
                    panic!(
                        "system participant {:?} subscribes to {address:?} but no such channel \
                         entry exists — host wiring bug",
                        spec.component,
                    )
                });
            assert!(
                !entry.subscribers.iter().any(|s| matches!(
                    &s.kind,
                    SubscriberEntryKind::System(c) if c == spec.component
                )),
                "system participant {:?} already subscribes to {address:?} — duplicate \
                 subscription would double-deliver; host wiring bug",
                spec.component,
            );
            entry.subscribers.push(SubscriberEntry {
                kind: SubscriberEntryKind::System(spec.component.to_string()),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: None,
            });
        }
    }
}

/// The shared drain loop for a subscribing system participant: a startup
/// sweep, then `Notify`-driven passes. Each pass dequeues the participant's
/// pending pushes with **ack-at-dequeue** (every loaded row is marked
/// delivered before the handler runs — at-most-once; the handler must
/// tolerate loss on crash) and hands the batch to the handler.
pub struct SystemInbox {
    component: &'static str,
    messenger: Arc<Messenger>,
    notify: Arc<Notify>,
}

impl SystemInbox {
    /// Build the inbox for `system:<component>`. `notify` is the same handle
    /// registered as the participant's parked-notify delivery binding, so a
    /// publish targeting the participant nudges the drain loop.
    pub fn new(component: &'static str, messenger: Arc<Messenger>, notify: Arc<Notify>) -> Self {
        Self {
            component,
            messenger,
            notify,
        }
    }

    /// Load the participant's pending pushes and ack the whole batch before
    /// returning it (ack-at-dequeue, at-most-once). Empty when nothing is
    /// pending.
    ///
    /// # Panics
    ///
    /// Panics on an ingress row: ingress rows are conversation-targeted, so
    /// one on a system subscriber is a host-wiring invariant violation.
    pub async fn dequeue_batch(&self) -> Vec<(i64, MessageEnvelope)> {
        let subscriber = ParticipantId::for_system(self.component);
        let rows = self.messenger.load_pending_pushes(&subscriber).await;
        if rows.is_empty() {
            return Vec::new();
        }
        let push_ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
        self.messenger.mark_pushes_delivered(&push_ids).await;
        rows.into_iter()
            .map(|(push_id, iob)| match iob {
                IngressOrBus::Bus(env) => (push_id, env),
                IngressOrBus::Ingress(ev) => panic!(
                    "system inbox {:?}: ingress row on a system subscriber — host-wiring \
                     invariant violated; push_id={push_id} source={:?}",
                    self.component, ev.source,
                ),
            })
            .collect()
    }

    /// Park/wake drain loop: a startup sweep (rows a prior crash left
    /// pending are picked up before the first wake), then `Notify`-driven
    /// passes. Each non-empty batch is handed to `handler` and awaited
    /// before the next pass, so a batch is fully processed before the loop
    /// advances. Never returns.
    ///
    /// The handler is a plain `FnMut -> Future` (not `AsyncFnMut`) so callers
    /// can spawn the loop: an `AsyncFnMut`'s lending future cannot carry the
    /// `Send` bound `tokio::spawn` needs.
    pub async fn run<F, Fut>(self, mut handler: F)
    where
        F: FnMut(Vec<(i64, MessageEnvelope)>) -> Fut,
        Fut: Future<Output = ()>,
    {
        loop {
            let batch = self.dequeue_batch().await;
            if !batch.is_empty() {
                handler(batch).await;
            }
            self.notify.notified().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use chrono::Utc;
    use indexmap::IndexMap;
    use serde_json::json;

    use crate::access::acl::{AclSet, ChannelMatcher};
    use crate::access::{AppCapability, GrantSet};
    use crate::db::init_db_memory;
    use crate::messaging::config::MessagingGlobalConfig;
    use crate::messaging::db::{
        PendingPushInsert, insert_message_with_pushes, upsert_channels, utc_to_ns,
    };
    use crate::messaging::query::NoopWakeRouter;
    use crate::messaging::testutils::test_channel_entry;
    use crate::messaging::{ChannelScheme, MessagingDirectory, Urgency, WakeRouter};

    use super::*;

    const COMPONENT: &str = "test-inbox";

    fn subscribe_policy(prefix: &str) -> AppPolicy {
        let mut grants = GrantSet::default();
        grants.insert(AppCapability::MessagingSubscribe);
        let mut acls = AclSet::default();
        acls.brenn_subscribe
            .push(ChannelMatcher::Prefix(prefix.to_string()));
        AppPolicy {
            grants,
            acls,
            tool_grants: BTreeMap::new(),
        }
    }

    fn spec(component: &'static str, subscriptions: Vec<String>) -> SystemParticipantSpec {
        SystemParticipantSpec {
            component,
            policy: subscribe_policy("inbox/"),
            subscriptions,
        }
    }

    fn inbox_sub() -> SubscriberEntry {
        SubscriberEntry {
            kind: SubscriberEntryKind::System(COMPONENT.to_string()),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: None,
        }
    }

    struct Harness {
        messenger: Arc<Messenger>,
        channel_uuid: uuid::Uuid,
    }

    async fn harness() -> Harness {
        let db = init_db_memory();
        let channel = test_channel_entry("inbox/reqs", vec![inbox_sub()]);
        let channel_uuid = channel.uuid;
        {
            let conn = db.lock().await;
            upsert_channels(&conn, std::slice::from_ref(&channel));
        }
        let directory = Arc::new(MessagingDirectory::with_entries(vec![channel]));
        let messenger = Messenger::new(
            db,
            directory,
            Arc::from("test"),
            Arc::new(IndexMap::new()),
            Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        )
        .with_subscriber_registrations(registrations_from_specs(&[spec(COMPONENT, vec![])]));
        Harness {
            messenger,
            channel_uuid,
        }
    }

    async fn insert_row(h: &Harness, body: &str) -> i64 {
        let conn = h.messenger.db().lock().await;
        let push = PendingPushInsert {
            target_subscriber: ParticipantId::for_system(COMPONENT),
            target_app_slug: COMPONENT.to_string(),
            eager_wake: true,
            release_after: None,
            delivery_deadline: None,
        };
        let msg = insert_message_with_pushes(
            &conn,
            h.channel_uuid,
            "test",
            "wasm:someone",
            body,
            Urgency::Normal,
            ChannelScheme::Brenn,
            None,
            None,
            None,
            utc_to_ns(Utc::now()),
            &[push],
        );
        msg.push_ids[0]
    }

    #[tokio::test]
    async fn dequeue_batch_returns_rows_and_acks_at_dequeue() {
        let h = harness().await;
        insert_row(&h, &json!({ "n": 1 }).to_string()).await;
        insert_row(&h, &json!({ "n": 2 }).to_string()).await;
        let inbox = SystemInbox::new(COMPONENT, h.messenger.clone(), Arc::new(Notify::new()));

        let batch = inbox.dequeue_batch().await;
        assert_eq!(batch.len(), 2, "both pending rows dequeued");

        // Ack-at-dequeue: a second pass sees nothing, even though no handler ran.
        let again = inbox.dequeue_batch().await;
        assert!(again.is_empty(), "rows are acked at dequeue: {again:?}");
    }

    #[tokio::test]
    async fn run_sweeps_at_startup_then_drains_on_notify() {
        let h = harness().await;
        // A row pending before the loop starts: the startup sweep must pick it up.
        insert_row(&h, &json!({ "phase": "sweep" }).to_string()).await;

        let notify = Arc::new(Notify::new());
        let inbox = SystemInbox::new(COMPONENT, h.messenger.clone(), notify.clone());
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            inbox
                .run(move |batch| {
                    let tx = tx.clone();
                    async move {
                        for (_, env) in batch {
                            tx.send(env.body).unwrap();
                        }
                    }
                })
                .await;
        });

        let sweep = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("startup sweep delivers within the timeout")
            .expect("handler sender alive");
        assert_eq!(sweep, json!({ "phase": "sweep" }).to_string());

        // A row published while the loop is parked: a notify drains it.
        insert_row(&h, &json!({ "phase": "wake" }).to_string()).await;
        notify.notify_one();
        let woken = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("notify-driven pass delivers within the timeout")
            .expect("handler sender alive");
        assert_eq!(woken, json!({ "phase": "wake" }).to_string());

        task.abort();
    }

    #[test]
    fn registrations_from_specs_maps_component_to_eager_registration() {
        let regs = registrations_from_specs(&[spec("alpha", vec![])]);
        let reg = regs
            .get(&SubscriberEntryKind::System("alpha".to_string()))
            .expect("registration present");
        assert_eq!(reg.wake, WakeEconomics::Eager);
        assert!(reg.policy.allows_channel_access("brenn:inbox/reqs"));
    }

    #[test]
    #[should_panic(expected = "declared twice")]
    fn registrations_from_specs_panics_on_duplicate_component() {
        registrations_from_specs(&[spec("alpha", vec![]), spec("alpha", vec![])]);
    }

    #[test]
    fn fold_spec_subscriptions_appends_system_subscriber_entries() {
        let mut entries = vec![test_channel_entry("inbox/reqs", vec![])];
        fold_spec_subscriptions(
            &mut entries,
            &[spec(COMPONENT, vec!["brenn:inbox/reqs".to_string()])],
        );
        assert!(matches!(
            entries[0].subscribers.as_slice(),
            [SubscriberEntry {
                kind: SubscriberEntryKind::System(c),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                ..
            }] if c == COMPONENT
        ));
    }

    #[test]
    #[should_panic(expected = "double-deliver")]
    fn fold_spec_subscriptions_panics_on_duplicate_address() {
        let mut entries = vec![test_channel_entry("inbox/reqs", vec![])];
        fold_spec_subscriptions(
            &mut entries,
            &[spec(
                COMPONENT,
                vec![
                    "brenn:inbox/reqs".to_string(),
                    "brenn:inbox/reqs".to_string(),
                ],
            )],
        );
    }

    #[test]
    #[should_panic(expected = "no such channel entry")]
    fn fold_spec_subscriptions_panics_on_unknown_channel() {
        let mut entries = vec![test_channel_entry("inbox/reqs", vec![])];
        fold_spec_subscriptions(
            &mut entries,
            &[spec(COMPONENT, vec!["brenn:inbox/ghost".to_string()])],
        );
    }
}
