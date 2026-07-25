//! Live attach: reading a transportable non-durable channel as a stream.
//!
//! A `RingStore` holds retention and a broadcast fan-out; this module is the
//! consumer half of that fan-out — the receiver a wire transport drives, plus
//! the two [`Messenger`] entry points that resolve a channel address to it.
//!
//! Attach is atomic: every message is either entirely in the replay the attach
//! returns or entirely on the receiver it hands out — no loss, no duplication
//! at the boundary. Loss beyond that is detectable via `(epoch, seq)`: a
//! per-boot epoch and a dense per-channel seq. Delivery is best-effort: a
//! publish with no attached consumer still enters retention, so a later fresh
//! attach sees it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use tokio::sync::broadcast;
use tracing::{debug, warn};
use uuid::Uuid;

use brenn_envelope::{ChannelScheme, MessageEnvelope, Urgency};
use brenn_queue::{ReplayDecision, Resume};

/// Why a replay carries a gap (a discontinuity the consumer must be told about).
pub use brenn_queue::GapReason;

use crate::access::AppPolicy;
use crate::messaging::gates::{check_body_size, publish_acl_allows};
use crate::messaging::store::RingStore;
use crate::messaging::store::ring::{Appended, RetainedMessage};
use crate::messaging::{ChannelEntry, Messenger, ParticipantId};

/// A single message on a live stream plus its per-channel sequence number.
pub type EphemeralDelivery = RetainedMessage;

/// A consumer's resume position: the epoch and last seq it has already seen on
/// a channel.
pub type EphemeralResume = Resume<Uuid>;

/// The replay decision for an attach, alongside the replayed messages.
pub type Replay = ReplayDecision;

/// Per-`(channel address, participant)` counters for the live stream, owned by
/// the [`Messenger`] and shared with every receiver it hands out.
///
/// Both counts are consumer-side facts the store cannot see: a lag is measured
/// against the receiver's own position in the fan-out, and a delivery-time
/// denial happens after the message has already entered retention.
#[derive(Debug, Default)]
pub struct LiveCounters {
    /// Overflow-dropped message count, summed from fan-out lag events.
    dropped: Mutex<HashMap<(String, String), u64>>,
    /// Delivery-time ACL denials. Expected to stay zero (policies are
    /// boot-static); nonzero signals a wiring bug.
    delivery_denied: Mutex<HashMap<(String, String), u64>>,
}

impl LiveCounters {
    /// Increment a `(channel, participant)`-keyed counter by `n`.
    fn bump(map: &Mutex<HashMap<(String, String), u64>>, channel: &str, participant: &str, n: u64) {
        *map.lock()
            .expect("messaging live: counter lock poisoned")
            .entry((channel.to_owned(), participant.to_owned()))
            .or_insert(0) += n;
    }

    /// Read a `(channel, participant)`-keyed counter (0 if absent).
    fn get(map: &Mutex<HashMap<(String, String), u64>>, channel: &str, participant: &str) -> u64 {
        *map.lock()
            .expect("messaging live: counter lock poisoned")
            .get(&(channel.to_owned(), participant.to_owned()))
            .unwrap_or(&0)
    }
}

/// Why a live attach was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EphemeralSubscribeError {
    /// Address names no channel in the directory.
    UnknownChannel(String),
    /// The channel exists but carries no live stream: a durable channel's
    /// consumers read it through the dispatcher, and a confined (`local:`)
    /// channel never crosses the process boundary at all.
    NotLiveAttachable(String),
    /// Consumer holds no covering grant + ACL matcher for the channel.
    AclDenied(String),
}

/// The result of a successful attach: the replay set (oldest-first, seq
/// ascending), the replay decision, and the live receiver.
pub struct EphemeralSubscription {
    pub replay: Vec<Arc<EphemeralDelivery>>,
    pub decision: Replay,
    pub receiver: EphemeralReceiver,
}

/// An event from the live stream after attach.
#[derive(Debug, Clone)]
pub enum EphemeralEvent {
    /// A message delivered on the channel.
    Delivery(Arc<EphemeralDelivery>),
    /// `n` messages were lost to fan-out overflow (this consumer lagged).
    Dropped(u64),
}

/// The live half of an attach. Wraps the fan-out receiver plus the context
/// needed for delivery-time ACL re-checks and per-`(channel, participant)`
/// counters. Dropping it detaches the consumer (receiver drop is the whole
/// mechanism).
pub struct EphemeralReceiver {
    rx: broadcast::Receiver<Arc<EphemeralDelivery>>,
    subscriber: ParticipantId,
    policy: Arc<AppPolicy>,
    /// Full scheme-prefixed channel address, for the ACL re-check, the counter
    /// keys, and logs.
    address: String,
    counters: Arc<LiveCounters>,
}

impl EphemeralReceiver {
    /// Block until the next event, or `None` when every sender is gone (process
    /// shutdown). Delivery-time ACL denials and lag notifications are handled
    /// internally: a denied delivery is skipped (loop continues), a lag
    /// surfaces as `Dropped(n)`.
    pub async fn recv(&mut self) -> Option<EphemeralEvent> {
        loop {
            match self.rx.recv().await {
                Ok(delivery) => {
                    // Belt-and-suspenders re-check, mirroring durable
                    // Enforcement point A: on deny, warn + count + skip, never
                    // panic. Policies are boot-static, so this cannot fire
                    // differently than the attach check — it is symmetry, not
                    // revocation.
                    if !self.policy.allows_channel_access(&self.address) {
                        warn!(
                            channel = %self.address,
                            subscriber = self.subscriber.as_str(),
                            "live delivery denied — ACL not satisfied"
                        );
                        LiveCounters::bump(
                            &self.counters.delivery_denied,
                            &self.address,
                            self.subscriber.as_str(),
                            1,
                        );
                        continue;
                    }
                    return Some(EphemeralEvent::Delivery(delivery));
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    LiveCounters::bump(
                        &self.counters.dropped,
                        &self.address,
                        self.subscriber.as_str(),
                        n,
                    );
                    warn!(
                        channel = %self.address,
                        subscriber = self.subscriber.as_str(),
                        dropped = n,
                        "live consumer lagged — messages dropped"
                    );
                    return Some(EphemeralEvent::Dropped(n));
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }
}

impl Drop for EphemeralReceiver {
    fn drop(&mut self) {
        debug!(
            channel = %self.address,
            subscriber = self.subscriber.as_str(),
            "live detach"
        );
    }
}

impl Messenger {
    /// The incarnation every non-durable channel of this process carries. A
    /// resume cursor bearing a different epoch is a guaranteed gap, which is how
    /// restart loss becomes visible on all of them at once.
    pub fn ring_epoch(&self) -> Uuid {
        self.ring_stores().epoch()
    }

    /// Overflow-dropped message count for a `(channel address, participant)`
    /// pair on the live stream.
    pub fn live_drop_count(&self, channel_address: &str, participant: &str) -> u64 {
        LiveCounters::get(&self.live_counters.dropped, channel_address, participant)
    }

    /// Delivery-time ACL-denial count for a `(channel address, participant)`
    /// pair on the live stream.
    pub fn live_delivery_denied_count(&self, channel_address: &str, participant: &str) -> u64 {
        LiveCounters::get(
            &self.live_counters.delivery_denied,
            channel_address,
            participant,
        )
    }

    /// The ring store behind a channel that carries a live stream.
    ///
    /// # Panics
    ///
    /// If the entry has no ring. The caller has already established the channel
    /// is non-durable, so a miss means the directory and the store registry
    /// disagree about which channels exist.
    fn live_store(&self, entry: &ChannelEntry) -> Arc<RingStore> {
        self.ring_stores()
            .get(&entry.uuid)
            .unwrap_or_else(|| {
                panic!(
                    "messaging live: non-durable channel {:?} is in the directory but has no \
                     retention store — the directory and the store registry disagree",
                    entry.address
                )
            })
            .clone()
    }

    /// Resolve a channel that carries a live stream, or say why it does not.
    ///
    /// The capability check is the whole class decision and it is made here,
    /// once: a durable channel's consumers read it through the dispatcher, and a
    /// confined channel issues no handle a serializer could reach.
    fn live_channel(
        &self,
        channel_address: &str,
    ) -> Result<Arc<ChannelEntry>, EphemeralSubscribeError> {
        let entry = self
            .directory
            .resolve(channel_address)
            .ok_or_else(|| EphemeralSubscribeError::UnknownChannel(channel_address.to_string()))?;
        let caps = entry.capabilities();
        if caps.durable || !caps.transportable {
            return Err(EphemeralSubscribeError::NotLiveAttachable(
                channel_address.to_string(),
            ));
        }
        Ok(entry)
    }

    /// Attach a consumer to a channel's live stream. Returns the retained-window
    /// replay (computed per the resume rules), the replay decision, and a
    /// receiver for messages that enter retention after the attach.
    ///
    /// The consumer arrives already resolved (identity + policy). The ACL is
    /// checked once here via `allows_channel_access`; the receiver re-checks it
    /// on each live delivery (belt-and-suspenders symmetry with the durable
    /// path).
    ///
    /// Denial observability: the denial arms are silent — no counter, no log.
    /// A caller passing an attacker-influenceable address must bring its own
    /// denial observability, mirroring the publish ladder's.
    pub fn attach_live(
        &self,
        subscriber: ParticipantId,
        policy: Arc<AppPolicy>,
        channel_address: &str,
        resume: Option<EphemeralResume>,
    ) -> Result<EphemeralSubscription, EphemeralSubscribeError> {
        let entry = self.live_channel(channel_address)?;

        // Delivery-time ACL (grant + covering matcher, deny-by-default) against
        // the full stored address, matching the durable delivery gate.
        if !policy.allows_channel_access(&entry.address) {
            return Err(EphemeralSubscribeError::AclDenied(
                channel_address.to_string(),
            ));
        }

        let attach = self.live_store(&entry).subscribe_live(resume);

        if let Replay::Gap(GapReason::ResumeAhead) = attach.decision {
            // Matching epoch but a seq this epoch never assigned: impossible for
            // an honest consumer. The substrate only warns (fail closed, never
            // panic); the distinguishable reason lets the transport above it
            // escalate this as a protocol violation.
            warn!(
                channel = %entry.address,
                subscriber = subscriber.as_str(),
                "live attach: resume seq ahead of assigned range"
            );
        }

        debug!(
            channel = %entry.address,
            subscriber = subscriber.as_str(),
            decision = ?attach.decision,
            replay_len = attach.replay.len(),
            "live attach"
        );

        Ok(EphemeralSubscription {
            replay: attach.replay,
            decision: attach.decision,
            receiver: EphemeralReceiver {
                rx: attach.receiver,
                subscriber,
                policy,
                address: entry.address.clone(),
                counters: Arc::clone(&self.live_counters),
            },
        })
    }

    /// Apply one entry of an activation flush that is **already paid for**.
    ///
    /// Runs the validate-only gates and commits into retention exactly as the
    /// publish ladder does, with two differences:
    ///
    /// - **The per-sender rate gate is never consulted.** This is the backend's
    ///   own tiering, not a bypass: a flush is metered per call at buffer time by
    ///   the host that mints the activation, and its wire rate is metered by the
    ///   caller's one all-or-nothing draw against the per-instance backstop
    ///   before any entry is applied. The wall-clock per-sender bucket meters
    ///   *ad-hoc* sends — the single-`Publish` path still routes through the
    ///   ladder and its gate. Nothing downstream of an admitted flush's buffer is
    ///   permitted to refuse, because refusing would lose entries of a batch the
    ///   client was already answered `Ok`.
    /// - **`publish_ts` is the caller's**, not minted here: the caller stamps the
    ///   whole batch in call order in one pass before splitting it by class, so
    ///   call order is visible across the class boundary at ns precision.
    ///
    /// The gates **panic rather than return**: every client-reachable failure was
    /// already answered as a violation by the caller's per-entry resolve against
    /// its boot-resolved output map, so a failure here means the caller's map
    /// disagrees with the directory or the principal's policy — publishing anyway
    /// would route traffic no operator authorized.
    ///
    /// Caller invariant, as for the ladder: `sender` and `policy` MUST both be
    /// derived from the same config-resolved principal, never from client input.
    ///
    /// Returns the append's outcome, whose `overflow` names every attached
    /// subscriber whose owed messages this entry evicted. The caller must route
    /// it to the noise ladder: the eviction charge is booked as reported the
    /// moment it happens, so a discarded event is a drop no later take will ever
    /// surface.
    #[must_use]
    pub fn publish_prepaid(
        &self,
        sender: &ParticipantId,
        policy: &AppPolicy,
        channel_address: &str,
        body: &str,
        urgency: Urgency,
        publish_ts: DateTime<Utc>,
    ) -> Appended {
        let entry = self.live_channel(channel_address).unwrap_or_else(|err| {
            panic!(
                "publish_prepaid: bound output {channel_address:?} is not a live-attachable \
                 channel ({err:?}) — boot validation proves every bound output resolves, so this \
                 is a broken boot invariant"
            )
        });
        let (scheme, name) = ChannelScheme::split(&entry.address)
            .expect("a directory entry's address always carries its scheme");
        assert!(
            publish_acl_allows(policy, scheme, name),
            "publish_prepaid: sender {sender:?} has no publish ACL covering bound output \
             {channel_address:?} — boot validation proves every bound output is policy-covered, so \
             this is a broken boot invariant",
            sender = sender.as_str(),
        );
        if let Err(e) = check_body_size(body, self.defaults.max_body_bytes) {
            panic!(
                "publish_prepaid: bound output {channel_address:?} carries a {len}-byte body over \
                 the {max}-byte cap — the caller rejects an over-cap entry as a violation before \
                 drawing, so the two caps disagree",
                len = e.len,
                max = e.max,
            );
        }

        self.live_store(&entry).append(MessageEnvelope {
            message_id: Uuid::new_v4(),
            source: self.source().into(),
            channel: entry.address.clone(),
            sender: sender.as_str().into(),
            publish_ts,
            body: body.to_string(),
            reply_to: None,
            delivery_deadline: None,
            deliver_after: None,
            urgency,
            envelope_type: scheme,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::access::AppCapability;
    use crate::access::acl::ChannelMatcher;
    use crate::messaging::query::NoopWakeRouter;
    use crate::messaging::store::ring::RING_FAN_OUT_CAPACITY;
    use crate::messaging::store::{OverflowEvent, Priming, RingStores};
    use crate::messaging::testutils::{ephemeral_channel_entry, local_channel_entry};
    use crate::messaging::{MessagingDirectory, MessagingGlobalConfig, WakeRouter};

    const CHANNEL: &str = "ephemeral:protobar";
    const NAME: &str = "protobar";

    fn pid(slug: &str) -> ParticipantId {
        ParticipantId::for_app(slug, "test-source")
    }

    /// A `Messenger` over `entries` alone: no durable channels, one ring store
    /// apiece, a noop router.
    fn messenger_with(entries: &[ChannelEntry], fan_out_capacity: u32) -> Arc<Messenger> {
        let stores = Arc::new(RingStores::build_with_fan_out_capacity(
            entries,
            fan_out_capacity,
        ));
        Messenger::new(
            crate::db::init_db_memory(),
            Arc::new(MessagingDirectory::with_entries(entries.to_vec())),
            Arc::from("test-source"),
            Arc::new(indexmap::IndexMap::new()),
            Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        )
        .with_ring_stores(stores)
    }

    fn subscriber_policy(channel: &str) -> Arc<AppPolicy> {
        let mut p = AppPolicy::with_grants(&[AppCapability::EphemeralSubscribe]);
        p.acls.ephemeral_subscribe = vec![ChannelMatcher::Exact(channel.to_string())];
        Arc::new(p)
    }

    fn publisher_policy(channel: &str) -> AppPolicy {
        let mut p = AppPolicy::with_grants(&[AppCapability::EphemeralPublish]);
        p.acls.ephemeral_publish = vec![ChannelMatcher::Exact(channel.to_string())];
        p
    }

    /// Commit `n` messages onto the fixture channel, bypassing the gates.
    fn commit_n(messenger: &Messenger, n: usize) {
        let store = messenger
            .ring_stores()
            .get_by_address(CHANNEL)
            .expect("fixture channel")
            .clone();
        for _ in 0..n {
            store.append(MessageEnvelope {
                message_id: Uuid::new_v4(),
                source: "test-source".into(),
                channel: CHANNEL.into(),
                sender: "app:pub@test-source".into(),
                publish_ts: Utc::now(),
                body: "x".to_string(),
                reply_to: None,
                delivery_deadline: None,
                deliver_after: None,
                urgency: Urgency::Normal,
                envelope_type: ChannelScheme::Ephemeral,
            });
        }
    }

    #[tokio::test]
    async fn attach_rejects_unknown_unattachable_and_denied() {
        let messenger = messenger_with(
            &[
                ephemeral_channel_entry(NAME, 8),
                local_channel_entry("confined", 8),
            ],
            RING_FAN_OUT_CAPACITY,
        );

        assert_eq!(
            messenger
                .attach_live(pid("s"), subscriber_policy(NAME), NAME, None)
                .err(),
            Some(EphemeralSubscribeError::UnknownChannel(NAME.to_string())),
        );
        // A confined channel exists but issues no live handle.
        assert_eq!(
            messenger
                .attach_live(
                    pid("s"),
                    subscriber_policy("confined"),
                    "local:confined",
                    None
                )
                .err(),
            Some(EphemeralSubscribeError::NotLiveAttachable(
                "local:confined".to_string()
            )),
        );
        // Grant but no matcher → deny-by-default.
        let no_matcher = Arc::new(AppPolicy::with_grants(&[AppCapability::EphemeralSubscribe]));
        assert_eq!(
            messenger
                .attach_live(pid("s"), no_matcher, CHANNEL, None)
                .err(),
            Some(EphemeralSubscribeError::AclDenied(CHANNEL.to_string())),
        );
        // Matcher but no grant → deny-by-default.
        let mut p = AppPolicy::with_grants(&[]);
        p.acls.ephemeral_subscribe = vec![ChannelMatcher::Exact(NAME.to_string())];
        assert_eq!(
            messenger
                .attach_live(pid("s"), Arc::new(p), CHANNEL, None)
                .err(),
            Some(EphemeralSubscribeError::AclDenied(CHANNEL.to_string())),
        );
    }

    /// The attach hands over the store's replay decision and window untouched,
    /// and the live half carries everything after it.
    #[tokio::test]
    async fn attach_replays_the_window_then_streams_live() {
        let messenger = messenger_with(&[ephemeral_channel_entry(NAME, 8)], RING_FAN_OUT_CAPACITY);
        commit_n(&messenger, 3);

        let resume = EphemeralResume {
            epoch: messenger.ring_epoch(),
            seq: 1,
        };
        let sub = messenger
            .attach_live(pid("s"), subscriber_policy(NAME), CHANNEL, Some(resume))
            .expect("attach");
        assert_eq!(sub.decision, Replay::Exact);
        assert_eq!(
            sub.replay.iter().map(|d| d.seq).collect::<Vec<_>>(),
            vec![2, 3]
        );

        let mut receiver = sub.receiver;
        commit_n(&messenger, 1);
        match receiver.recv().await {
            Some(EphemeralEvent::Delivery(d)) => {
                assert_eq!(d.seq, 4);
                assert_eq!(d.envelope.channel, CHANNEL);
            }
            other => panic!("expected the live delivery, got {other:?}"),
        }
    }

    /// A resume this epoch never assigned is answered, not hidden: the decision
    /// reaches the caller so the transport above can escalate it.
    #[tokio::test]
    async fn attach_reports_a_resume_ahead_of_the_assigned_range() {
        let messenger = messenger_with(&[ephemeral_channel_entry(NAME, 8)], RING_FAN_OUT_CAPACITY);
        commit_n(&messenger, 2);

        let resume = EphemeralResume {
            epoch: messenger.ring_epoch(),
            seq: 99,
        };
        let sub = messenger
            .attach_live(pid("s"), subscriber_policy(NAME), CHANNEL, Some(resume))
            .expect("attach");
        assert_eq!(sub.decision, Replay::Gap(GapReason::ResumeAhead));
        assert_eq!(sub.replay.len(), 2);
    }

    /// A lagging consumer's loss is counted against it by name and surfaced as
    /// an exact drop count, then delivery resumes.
    #[tokio::test]
    async fn a_lagging_consumer_is_counted_and_told() {
        const CAPACITY: u32 = 4;
        const OVERSHOOT: u64 = 3;
        let flood = CAPACITY as usize + OVERSHOOT as usize;
        let messenger = messenger_with(&[ephemeral_channel_entry(NAME, 0)], CAPACITY);

        let subscriber = pid("s");
        let mut receiver = messenger
            .attach_live(subscriber.clone(), subscriber_policy(NAME), CHANNEL, None)
            .expect("attach")
            .receiver;

        commit_n(&messenger, flood);

        match receiver.recv().await {
            Some(EphemeralEvent::Dropped(n)) => assert_eq!(n, OVERSHOOT),
            other => panic!("expected Dropped({OVERSHOOT}), got {other:?}"),
        }
        assert_eq!(
            messenger.live_drop_count(CHANNEL, subscriber.as_str()),
            OVERSHOOT
        );

        let mut resumed = Vec::new();
        for _ in 0..CAPACITY {
            match receiver.recv().await {
                Some(EphemeralEvent::Delivery(d)) => resumed.push(d.seq),
                other => panic!("expected Delivery, got {other:?}"),
            }
        }
        assert_eq!(resumed, (OVERSHOOT + 1..=flood as u64).collect::<Vec<_>>());
    }

    /// The delivery-time re-check is exercised by constructing a receiver whose
    /// policy denies — boot-static policies cannot reach this branch through
    /// `attach_live`.
    #[tokio::test]
    async fn delivery_time_acl_deny_skips_and_counts() {
        let counters = Arc::new(LiveCounters::default());
        let (tx, rx) = broadcast::channel(4);
        let subscriber = pid("s");
        let mut receiver = EphemeralReceiver {
            rx,
            subscriber: subscriber.clone(),
            policy: Arc::new(AppPolicy::with_grants(&[])),
            address: CHANNEL.to_string(),
            counters: Arc::clone(&counters),
        };

        tx.send(Arc::new(RetainedMessage {
            seq: 1,
            envelope: Arc::new(MessageEnvelope {
                message_id: Uuid::new_v4(),
                source: "test-source".into(),
                channel: CHANNEL.into(),
                sender: "app:pub@test-source".into(),
                publish_ts: Utc::now(),
                body: "x".to_string(),
                reply_to: None,
                delivery_deadline: None,
                deliver_after: None,
                urgency: Urgency::Normal,
                envelope_type: ChannelScheme::Ephemeral,
            }),
        }))
        .expect("send");
        drop(tx); // close after the one message

        // The denied delivery is skipped; the loop then sees Closed → None.
        assert!(receiver.recv().await.is_none());
        assert_eq!(
            LiveCounters::get(&counters.delivery_denied, CHANNEL, subscriber.as_str()),
            1
        );
    }

    /// A prepaid publish enters retention and hands its overflow back to the
    /// caller. The ring charges an eviction as *reported* the moment it
    /// overwrites an unread position, so a caller that drops this outcome drops
    /// the only report there will ever be.
    #[tokio::test]
    async fn publish_prepaid_commits_and_reports_its_overflow() {
        let messenger = messenger_with(&[ephemeral_channel_entry(NAME, 2)], RING_FAN_OUT_CAPACITY);
        let sender = pid("pub");
        let policy = publisher_policy(NAME);
        let absent = ParticipantId::for_wasm("absent");
        messenger
            .ring_stores()
            .get_by_address(CHANNEL)
            .expect("fixture channel")
            .attach(&absent, 4, Priming::Head);

        let publish = |body: &str| {
            messenger.publish_prepaid(&sender, &policy, CHANNEL, body, Urgency::Normal, Utc::now())
        };

        let first = publish("a");
        assert_eq!(first.retained.seq, 1);
        assert_eq!(first.retained.envelope.channel, CHANNEL);
        assert!(first.overflow.is_empty());
        assert!(publish("b").overflow.is_empty());

        assert_eq!(
            publish("c").overflow,
            vec![OverflowEvent {
                subscriber: absent.clone(),
                dropped: 1,
                app_slug: None,
            }]
        );
    }

    #[tokio::test]
    #[should_panic(expected = "not a live-attachable channel")]
    async fn publish_prepaid_panics_on_a_channel_that_is_not_live_attachable() {
        let messenger = messenger_with(&[ephemeral_channel_entry(NAME, 2)], RING_FAN_OUT_CAPACITY);
        let _ = messenger.publish_prepaid(
            &pid("pub"),
            &publisher_policy(NAME),
            "ephemeral:nope",
            "x",
            Urgency::Normal,
            Utc::now(),
        );
    }

    #[tokio::test]
    #[should_panic(expected = "no publish ACL covering bound output")]
    async fn publish_prepaid_panics_on_an_uncovered_sender() {
        let messenger = messenger_with(&[ephemeral_channel_entry(NAME, 2)], RING_FAN_OUT_CAPACITY);
        let _ = messenger.publish_prepaid(
            &pid("pub"),
            &AppPolicy::with_grants(&[AppCapability::EphemeralPublish]),
            CHANNEL,
            "x",
            Urgency::Normal,
            Utc::now(),
        );
    }
}
