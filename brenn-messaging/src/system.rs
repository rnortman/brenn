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

use brenn_envelope::grants::AppCapability;
use brenn_lib::access::AppPolicy;
use brenn_lib::access::acl::ChannelMatcher;

use super::config::NoiseLevel;
use super::store::MessageSeq;
use super::{
    ChannelEntry, ChannelScheme, MessageEnvelope, Messenger, ParticipantId, SubscriberEntry,
    SubscriberEntryKind, SubscriberRegistration, WakeEconomics,
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
    /// A publish-only system participant granted exactly `scheme`'s publish
    /// capability plus one exact-match ACL per bare (scheme-stripped) channel
    /// name in `scheme`'s own matcher family, and no subscriptions. The
    /// substrate shape for a code-built boot publisher: code-built, a fixed set
    /// of channels, no consumer side. An empty slice yields publish authority
    /// with no channel it may write — a degenerate spec the caller is expected
    /// to avoid.
    ///
    /// One scheme per participant: the publish gate dispatches the ACL family by
    /// the channel's scheme, so a participant granted in the wrong family holds
    /// authority the gate never reads.
    ///
    /// # Panics
    ///
    /// On an egress scheme (`mqtt:`, `webhook:`, `pwa_push:`). Those are reached
    /// through their own adapters with their own client/endpoint-shaped ACLs,
    /// not as a bare channel set, so a spec naming one is a host wiring bug.
    pub fn publish_only(
        component: &'static str,
        scheme: ChannelScheme,
        bare_channels: &[String],
    ) -> Self {
        let mut policy = AppPolicy::default();
        let (capability, family) = match scheme {
            ChannelScheme::Brenn => (
                AppCapability::MessagingPublish,
                &mut policy.acls.brenn_publish,
            ),
            ChannelScheme::Ephemeral => (
                AppCapability::EphemeralPublish,
                &mut policy.acls.ephemeral_publish,
            ),
            ChannelScheme::Local => (AppCapability::LocalPublish, &mut policy.acls.local_publish),
            ChannelScheme::Mqtt | ChannelScheme::Webhook | ChannelScheme::PwaPush => panic!(
                "system participant {component:?} declared publish-only on scheme {} — egress \
                 schemes are reached through their own adapters, never as a bare channel set; \
                 host wiring bug",
                scheme.as_str(),
            ),
        };
        for bare in bare_channels {
            family.push(ChannelMatcher::Exact(bare.clone()));
        }
        policy.grants.insert(capability);
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
/// Both depths are the channel's own `retain_depth` and noise is `Silent`
/// deliberately: a drain that sees a window's worth sees everything there is,
/// so the participant is owed exactly what the channel retains. Sizing the
/// window is the operator's decision on the channel block; a subscriber that
/// reached past it would pin the channel against reaping and quietly move the
/// retention decision out of the operator's hands.
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
///
/// Also panics when the channel's `retain_depth` is zero, naming the channel:
/// the participant would take a `push_depth` of zero, which creates no position,
/// and the first window read would blame the host for a wiring bug that is
/// really a channel sized to hold nothing.
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
            let window = entry.resolved_channel.retain_depth;
            assert!(
                window.is_push_enabled(),
                "system participant {:?} subscribes to {address:?}, whose retain_depth is \
                 {window:?} — a participant follows its channel's window, and a window of \
                 zero would leave it with no position at all; size the channel's \
                 retain_depth to at least one",
                spec.component,
            );
            entry.subscribers.push(SubscriberEntry {
                kind: SubscriberEntryKind::System(spec.component.to_string()),
                push_depth: window,
                retain_depth: window,
                noise: NoiseLevel::Silent,
                wake_min: None,
            });
        }
    }
}

/// The shared drain loop for a subscribing system participant: an attach, a
/// startup sweep, then `Notify`-driven passes. Each pass reads the
/// participant's window on every channel it subscribes to and advances its
/// position **before** the handler runs — at-most-once; the handler must
/// tolerate loss on crash.
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

    /// The participant's own identity on the bus.
    fn subscriber(&self) -> ParticipantId {
        ParticipantId::for_system(self.component)
    }

    /// Every channel carrying this participant's directory subscriber entry,
    /// paired with that entry. The directory is the authority on what the
    /// participant subscribes to.
    fn subscriptions(&self) -> Vec<(Arc<ChannelEntry>, SubscriberEntry)> {
        let kind = SubscriberEntryKind::System(self.component.to_string());
        let mut found = Vec::new();
        for entry in self.messenger.directory().list() {
            if let Some(sub) = entry.subscribers.iter().find(|s| s.kind == kind) {
                found.push((Arc::clone(&entry), sub.clone()));
            }
        }
        found
    }

    /// Create the participant's position on every channel it subscribes to.
    ///
    /// A position coming into existence is primed behind the channel's retained
    /// tail, so a genuinely fresh participant drains what is retained rather
    /// than silently skipping it. A position already there — seeded by the
    /// migration or left by a previous boot — keeps its place, so a restart
    /// resumes rather than re-drains.
    ///
    /// The guarantee is bounded, and deliberately so: a message is retained for
    /// the channel's window, sized for the outage the operator intends to
    /// survive, not forever. So a fresh position re-runs at most a window's
    /// worth of already-handled messages, and anything dropped past the window
    /// is counted, not silent. Handlers must tolerate that repeat — for the tool
    /// executor it is the `Idempotency::Natural` contract every async tool
    /// declares.
    ///
    /// Runs before the first window read; a push-enabled window read without a
    /// position panics.
    pub async fn attach(&self) {
        let subscriber = self.subscriber();
        for (entry, sub) in self.subscriptions() {
            self.messenger
                .attach_subscriber(&entry.address, self.component, &subscriber, sub.push_depth)
                .await;
        }
    }

    /// Read the participant's window on every subscribed channel and advance
    /// its position past what the window served, before returning the batch —
    /// ack-at-dequeue, at-most-once. Empty when every channel is caught up.
    ///
    /// The batch is ordered by publish time across channels, so a handler that
    /// groups by channel sees each channel's messages in publish order.
    ///
    /// Carries the delivery-time ACL gate: a channel this participant's policy
    /// no longer covers is skipped without being read and without being advanced
    /// over, so a restored policy serves the accumulated backlog rather than
    /// silently stepping past it.
    pub async fn dequeue_batch(&self) -> Vec<(MessageSeq, MessageEnvelope)> {
        let subscriber = self.subscriber();
        let mut batch: Vec<(MessageSeq, MessageEnvelope)> = Vec::new();
        for (entry, sub) in self.subscriptions() {
            if !self
                .messenger
                .channel_access_allowed(&sub.kind, &entry.address)
            {
                self.messenger.warn_acl_denied(&entry, &subscriber);
                continue;
            }
            // A system participant's subscriptions are static: attached at the
            // top of its loop and torn down by nothing, so a missing position
            // is a wiring bug rather than a departure to tolerate.
            let window = self
                .messenger
                .store_for(&entry)
                .window(&subscriber, sub.push_depth, sub.retain_depth)
                .await
                .unwrap_or_else(|| {
                    panic!(
                        "messaging: no position for system subscriber {} on {} — a push-enabled \
                         read over a queue that was never created",
                        subscriber.as_str(),
                        entry.address
                    )
                });
            if window.new_entries().is_empty() {
                continue;
            }
            let new: Vec<(MessageSeq, MessageEnvelope)> = window
                .new_entries()
                .iter()
                .map(|(seq, env)| (*seq, MessageEnvelope::clone(env)))
                .collect();
            // Advance first: the handler runs against a position already past
            // the batch, so a crash mid-handler loses it rather than repeating it.
            if let Some((through, seen_floor)) = window.advance_span() {
                self.messenger
                    .advance_subscriber(&entry.address, &subscriber, through, seen_floor, sub.noise)
                    .await
                    .unwrap_or_else(|| {
                        panic!(
                            "messaging: no position for system subscriber {} on {} to advance",
                            subscriber.as_str(),
                            entry.address
                        )
                    });
            }
            batch.extend(new);
        }
        batch.sort_by_key(|(_, env)| env.publish_ts);
        batch
    }

    /// Park/wake drain loop: an attach, a startup sweep (whatever a prior
    /// crash left unseen is picked up before the first wake), then
    /// `Notify`-driven passes. Each non-empty batch is handed to `handler` and
    /// awaited before the next pass, so a batch is fully processed before the
    /// loop advances. Never returns.
    ///
    /// The handler is a plain `FnMut -> Future` (not `AsyncFnMut`) so callers
    /// can spawn the loop: an `AsyncFnMut`'s lending future cannot carry the
    /// `Send` bound `tokio::spawn` needs.
    pub async fn run<F, Fut>(self, mut handler: F)
    where
        F: FnMut(Vec<(MessageSeq, MessageEnvelope)>) -> Fut,
        Fut: Future<Output = ()>,
    {
        self.attach().await;
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

    use crate::config::{Depth, MessagingGlobalConfig};
    use crate::db::init_db_memory;
    use crate::db::{insert_message, upsert_channels, utc_to_ns};
    use crate::query::NoopWakeRouter;
    use crate::testutils::test_channel_entry;
    use crate::{ChannelScheme, MessagingDirectory, Urgency, WakeRouter};
    use brenn_envelope::grants::AppCapability;
    use brenn_lib::access::GrantSet;
    use brenn_lib::access::acl::{AclSet, ChannelMatcher};

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

    /// [`spec`] with the subscribe ACL narrowed to `prefix` — for the
    /// delivery-time ACL gate, which needs a policy that covers one of the
    /// harness's two channels and not the other.
    fn spec_covering(component: &'static str, prefix: &str) -> SystemParticipantSpec {
        SystemParticipantSpec {
            component,
            policy: subscribe_policy(prefix),
            subscriptions: vec![],
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
        reqs_uuid: uuid::Uuid,
        alt_uuid: uuid::Uuid,
    }

    /// Two channels, both carrying the participant's subscriber entry, so the
    /// per-channel window walk and the cross-channel batch order are both
    /// exercisable.
    async fn harness() -> Harness {
        harness_with(inbox_sub()).await
    }

    /// Build a harness from pre-assembled channel entries and a participant
    /// spec. `entries[0]` becomes `reqs_uuid`, `entries[1]` becomes `alt_uuid`.
    async fn harness_over(
        entries: Vec<ChannelEntry>,
        participant: SystemParticipantSpec,
    ) -> Harness {
        let db = init_db_memory();
        let reqs_uuid = entries[0].uuid;
        let alt_uuid = entries[1].uuid;
        {
            let conn = db.lock().await;
            upsert_channels(&conn, &entries);
        }
        let messenger = Messenger::new(
            db,
            Arc::new(MessagingDirectory::with_entries(entries)),
            Arc::from("test"),
            Arc::new(IndexMap::new()),
            Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        )
        .with_subscriber_registrations(registrations_from_specs(&[participant]));
        Harness {
            messenger,
            reqs_uuid,
            alt_uuid,
        }
    }

    /// [`harness`] with the participant's subscription spelled out, for the
    /// cases that turn the depth or the noise rung away from the defaults every
    /// production system subscription uses.
    async fn harness_with(sub: SubscriberEntry) -> Harness {
        let entries = vec![
            test_channel_entry("inbox/reqs", vec![sub.clone()]),
            test_channel_entry("inbox/alt", vec![sub]),
        ];
        harness_over(entries, spec(COMPONENT, vec![])).await
    }

    /// Like [`harness`] but with a bounded channel window. Folds subscriber
    /// entries from the spec rather than hand-writing them, keeping the test
    /// honest about what production builds.
    async fn harness_windowed(window: u64) -> Harness {
        let mut entries = vec![
            test_channel_entry("inbox/reqs", vec![]),
            test_channel_entry("inbox/alt", vec![]),
        ];
        for entry in &mut entries {
            entry.resolved_channel.push_depth = Depth::Bounded(window);
            entry.resolved_channel.retain_depth = Depth::Bounded(window);
            entry.resolved_channel.standing_retain_depth = Depth::Bounded(window);
        }
        let participant = spec(
            COMPONENT,
            entries.iter().map(|e| e.address.clone()).collect(),
        );
        fold_spec_subscriptions(&mut entries, std::slice::from_ref(&participant));
        harness_over(entries, participant).await
    }

    fn inbox(h: &Harness) -> SystemInbox {
        SystemInbox::new(COMPONENT, h.messenger.clone(), Arc::new(Notify::new()))
    }

    /// The same channels and the same database under a policy covering only
    /// `prefix` — an operator revoking or restoring an ACL, with every position
    /// left exactly where it was. Only the registration differs.
    fn repolicy(h: &Harness, prefix: &str) -> Harness {
        let entries: Vec<ChannelEntry> = h
            .messenger
            .directory()
            .list()
            .iter()
            .map(|entry| ChannelEntry::clone(entry))
            .collect();
        let messenger = Messenger::new(
            h.messenger.db().clone(),
            Arc::new(MessagingDirectory::with_entries(entries)),
            Arc::from("test"),
            Arc::new(IndexMap::new()),
            Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        )
        .with_subscriber_registrations(registrations_from_specs(&[spec_covering(
            COMPONENT, prefix,
        )]));
        Harness {
            messenger,
            reqs_uuid: h.reqs_uuid,
            alt_uuid: h.alt_uuid,
        }
    }

    async fn insert_on(h: &Harness, channel_uuid: uuid::Uuid, body: &str) {
        let conn = h.messenger.db().lock().await;
        insert_message(
            &conn,
            channel_uuid,
            "test",
            "wasm:someone",
            body,
            Urgency::Normal,
            ChannelScheme::Brenn,
            None,
            None,
            None,
            None,
            utc_to_ns(Utc::now()),
        );
    }

    async fn insert_row(h: &Harness, body: &str) {
        insert_on(h, h.reqs_uuid, body).await
    }

    fn bodies(batch: &[(MessageSeq, MessageEnvelope)]) -> Vec<String> {
        batch.iter().map(|(_, env)| env.body.clone()).collect()
    }

    /// A system participant's positions are static — attached at the top of its
    /// loop and torn down by nothing — so a push-enabled read that finds none is
    /// a wiring bug. The store reports the absence; this caller judges it fatal.
    ///
    /// The expected text names the *read* site specifically. The advance site
    /// carries its own panic, and no single-threaded case can reach it: once the
    /// window answered `Some`, the position exists. That one is held by review,
    /// not by this test.
    #[tokio::test]
    #[should_panic(expected = "a push-enabled read over a queue that was never created")]
    async fn dequeue_without_a_position_panics() {
        let h = harness().await;
        insert_row(&h, "unattached").await;
        inbox(&h).dequeue_batch().await;
    }

    #[tokio::test]
    async fn dequeue_batch_returns_rows_and_acks_at_dequeue() {
        let h = harness().await;
        let inbox = inbox(&h);
        inbox.attach().await;
        insert_row(&h, &json!({ "n": 1 }).to_string()).await;
        insert_row(&h, &json!({ "n": 2 }).to_string()).await;

        let batch = inbox.dequeue_batch().await;
        assert_eq!(batch.len(), 2, "both unseen messages dequeued");

        // Ack-at-dequeue: the position moved past the batch before it was
        // returned, so a second pass sees nothing even though no handler ran.
        let again = inbox.dequeue_batch().await;
        assert!(
            again.is_empty(),
            "the batch was advanced past at dequeue: {again:?}"
        );
    }

    /// A subscription narrower than the channel's window: the window serves the
    /// newest `push_depth`, and the seqs it skipped are charged at the
    /// subscription's own rung — not the channel's, and not lost.
    #[tokio::test]
    async fn a_bounded_inbox_takes_the_newest_and_charges_what_it_skipped() {
        let h = harness_with(SubscriberEntry {
            push_depth: Depth::Bounded(1),
            retain_depth: Depth::Bounded(0),
            noise: NoiseLevel::Metered,
            ..inbox_sub()
        })
        .await;
        let inbox = inbox(&h);
        inbox.attach().await;
        for n in 1..=3 {
            insert_row(&h, &json!({ "n": n }).to_string()).await;
        }

        assert_eq!(
            bodies(&inbox.dequeue_batch().await),
            vec![json!({ "n": 3 }).to_string()],
            "the clamp serves the newest, not the oldest",
        );
        let address = h
            .messenger
            .directory()
            .by_uuid(&h.reqs_uuid)
            .expect("channel in directory")
            .address
            .clone();
        assert_eq!(
            h.messenger
                .drop_counter(&address, &ParticipantId::for_system(COMPONENT)),
            2,
            "the two the clamp skipped are metered against this participant",
        );
    }

    #[tokio::test]
    async fn a_fresh_position_drains_what_was_retained_before_it_existed() {
        let h = harness().await;
        insert_row(&h, &json!({ "phase": "before-attach" }).to_string()).await;
        let inbox = inbox(&h);
        inbox.attach().await;
        insert_row(&h, &json!({ "phase": "after-attach" }).to_string()).await;

        assert_eq!(
            bodies(&inbox.dequeue_batch().await),
            vec![
                json!({ "phase": "before-attach" }).to_string(),
                json!({ "phase": "after-attach" }).to_string(),
            ],
            "a fresh position is owed the whole retained window",
        );
    }

    /// Safety of the drain on a long-lived channel: the repeat is bounded by
    /// the channel's window, not by its history.
    #[tokio::test]
    async fn a_fresh_position_primes_at_the_window_floor_not_the_history_floor() {
        let h = harness_windowed(2).await;
        for n in 1..=5 {
            insert_row(&h, &json!({ "n": n }).to_string()).await;
        }
        let inbox = inbox(&h);
        inbox.attach().await;

        assert_eq!(
            bodies(&inbox.dequeue_batch().await),
            vec![json!({ "n": 4 }).to_string(), json!({ "n": 5 }).to_string()],
            "the drain is bounded by the channel's window, not by its history",
        );
    }

    /// Complement: rows past the window actually leave the database, so
    /// history cannot grow into a replay a later fresh position would face.
    #[tokio::test]
    async fn the_gc_pass_retires_what_the_window_no_longer_covers() {
        let h = harness_windowed(2).await;
        for n in 1..=5 {
            insert_row(&h, &json!({ "n": n }).to_string()).await;
        }
        let inbox = inbox(&h);
        inbox.attach().await;
        assert_eq!(inbox.dequeue_batch().await.len(), 2, "the window's worth");

        let entry = h
            .messenger
            .directory()
            .by_uuid(&h.reqs_uuid)
            .expect("channel in directory");
        let frontier = entry
            .reap_frontier()
            .expect("a bounded standing depth gives the reaper a frontier");
        assert_eq!(frontier, 2, "the frontier is the channel's standing number");

        let eviction = {
            let conn = h.messenger.db().lock().await;
            crate::db::bus_gc_evict_channel(
                &conn,
                entry.uuid,
                &entry.address,
                entry.transport_type,
                frontier,
                entry.resolved_channel.sink,
                None,
            )
        };
        assert_eq!(
            eviction.messages_evicted, 3,
            "the three past the frontier are gone; the window's two remain",
        );
    }

    #[tokio::test]
    async fn re_attach_keeps_the_position_a_previous_boot_left() {
        let h = harness().await;
        let inbox = inbox(&h);
        inbox.attach().await;
        insert_row(&h, &json!({ "n": 1 }).to_string()).await;
        assert_eq!(inbox.dequeue_batch().await.len(), 1);
        insert_row(&h, &json!({ "n": 2 }).to_string()).await;

        // A restart re-attaches; the position it finds is the one the previous
        // boot left, so the unseen message is served and the seen one is not.
        inbox.attach().await;
        assert_eq!(
            bodies(&inbox.dequeue_batch().await),
            vec![json!({ "n": 2 }).to_string()],
            "re-attach resumes rather than re-priming",
        );
    }

    #[tokio::test]
    async fn dequeue_batch_spans_every_subscribed_channel_in_publish_order() {
        let h = harness().await;
        let inbox = inbox(&h);
        inbox.attach().await;
        insert_on(&h, h.alt_uuid, "alt-first").await;
        insert_on(&h, h.reqs_uuid, "reqs-second").await;
        insert_on(&h, h.alt_uuid, "alt-third").await;

        assert_eq!(
            bodies(&inbox.dequeue_batch().await),
            vec!["alt-first", "reqs-second", "alt-third"],
            "one batch over both channels, ordered by publish time",
        );
    }

    /// The delivery-time ACL gate, both halves. A channel the participant's
    /// policy no longer covers is skipped without being read — and, because the
    /// skip never advances the position, a restored policy serves the backlog
    /// that accumulated under the revocation instead of stepping past it.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn a_denied_channel_is_skipped_without_advancing() {
        let h = harness().await;
        inbox(&h).attach().await;
        insert_on(&h, h.reqs_uuid, "covered").await;
        insert_on(&h, h.alt_uuid, "denied").await;

        let revoked = repolicy(&h, "inbox/reqs");
        assert_eq!(
            bodies(&inbox(&revoked).dequeue_batch().await),
            vec!["covered"],
            "only the channel the narrowed policy still covers may be served",
        );
        assert!(
            logs_contain("subscription delivery denied"),
            "the gate must name the denied subscription",
        );

        let restored = repolicy(&h, "inbox/");
        assert_eq!(
            bodies(&inbox(&restored).dequeue_batch().await),
            vec!["denied"],
            "the denied channel's position never moved, so its backlog survives the revocation",
        );
    }

    #[tokio::test]
    async fn a_drained_inbox_stops_being_deliverable() {
        let h = harness().await;
        let inbox = inbox(&h);
        inbox.attach().await;
        insert_row(&h, &json!({ "n": 1 }).to_string()).await;

        let entry = h
            .messenger
            .directory()
            .by_uuid(&h.reqs_uuid)
            .expect("channel in directory");
        let store = h.messenger.store_for(&entry);
        let subscriber = ParticipantId::for_system(COMPONENT);
        let owed = store.deliverable_subscribers().await;
        assert_eq!(
            owed.iter().map(|o| &o.subscriber).collect::<Vec<_>>(),
            vec![&subscriber],
            "an unseen message makes the participant deliverable — the wake walk's read",
        );

        assert_eq!(inbox.dequeue_batch().await.len(), 1);
        assert!(
            store.deliverable_subscribers().await.is_empty(),
            "a drained inbox is caught up, so the wake walk has nothing to wake",
        );
    }

    #[tokio::test]
    async fn run_sweeps_at_startup_then_drains_on_notify() {
        let h = harness().await;
        // Attach before the row lands so the startup sweep picks it up as the
        // position's first unseen message rather than as part of its priming.
        inbox(&h).attach().await;
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

    /// **One scheme per participant, all the way down.** A spec must grant its
    /// scheme's capability and put its matchers in that scheme's ACL family —
    /// and leave the other two empty, or the participant holds authority nothing
    /// reads and the misplacement is invisible until the first publish.
    #[test]
    fn publish_only_grants_exactly_its_schemes_capability_and_family() {
        let bares = vec!["alpha".to_string(), "beta".to_string()];
        let expected: Vec<ChannelMatcher> = bares
            .iter()
            .map(|b| ChannelMatcher::Exact(b.clone()))
            .collect();
        for (scheme, capability, family) in [
            (
                ChannelScheme::Brenn,
                AppCapability::MessagingPublish,
                "brenn_publish",
            ),
            (
                ChannelScheme::Ephemeral,
                AppCapability::EphemeralPublish,
                "ephemeral_publish",
            ),
            (
                ChannelScheme::Local,
                AppCapability::LocalPublish,
                "local_publish",
            ),
        ] {
            let spec = SystemParticipantSpec::publish_only("pub", scheme, &bares);
            assert!(spec.subscriptions.is_empty(), "{scheme:?}: publish-only");
            for other in [
                AppCapability::MessagingPublish,
                AppCapability::EphemeralPublish,
                AppCapability::LocalPublish,
            ] {
                assert_eq!(
                    spec.policy.grants.has(other),
                    other == capability,
                    "{scheme:?}: grant {other:?} does not match the scheme's capability",
                );
            }
            let acls = &spec.policy.acls;
            for (name, matchers) in [
                ("brenn_publish", &acls.brenn_publish),
                ("ephemeral_publish", &acls.ephemeral_publish),
                ("local_publish", &acls.local_publish),
            ] {
                if name == family {
                    assert_eq!(matchers, &expected, "{scheme:?}: {name} holds the matchers");
                } else {
                    assert!(
                        matchers.is_empty(),
                        "{scheme:?}: {name} must hold nothing, got {matchers:?}",
                    );
                }
            }
        }
    }

    #[test]
    #[should_panic(expected = "egress schemes are reached through their own adapters")]
    fn publish_only_panics_on_mqtt() {
        SystemParticipantSpec::publish_only("pub", ChannelScheme::Mqtt, &[]);
    }

    #[test]
    #[should_panic(expected = "egress schemes are reached through their own adapters")]
    fn publish_only_panics_on_webhook() {
        SystemParticipantSpec::publish_only("pub", ChannelScheme::Webhook, &[]);
    }

    #[test]
    #[should_panic(expected = "egress schemes are reached through their own adapters")]
    fn publish_only_panics_on_pwa_push() {
        SystemParticipantSpec::publish_only("pub", ChannelScheme::PwaPush, &[]);
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
        entries[0].resolved_channel.retain_depth = Depth::Bounded(16);
        fold_spec_subscriptions(
            &mut entries,
            &[spec(COMPONENT, vec!["brenn:inbox/reqs".to_string()])],
        );
        assert!(matches!(
            entries[0].subscribers.as_slice(),
            [SubscriberEntry {
                kind: SubscriberEntryKind::System(c),
                push_depth: Depth::Bounded(16),
                retain_depth: Depth::Bounded(16),
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
