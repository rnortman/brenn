//! Non-durable channel entry points: resolving a transportable ring-backed
//! channel, and committing or parking an already-paid-for activation entry on
//! one.
//!
//! Messages are read back from retention by resume position, through
//! [`crate::messaging::store::RetentionStore`], so loss is detectable via
//! `(epoch, seq)`: a per-boot epoch and a dense per-channel seq. Delivery is
//! best-effort: a publish with no consumer attached still enters retention, so
//! a later resume sees it.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use brenn_envelope::{ChannelScheme, MessageEnvelope, Urgency};
use brenn_queue::{QuotaExceeded, ReplayDecision};

/// Why a replay carries a gap (a discontinuity the consumer must be told about).
pub use brenn_queue::GapReason;

use crate::access::AppPolicy;
use crate::messaging::gates::{check_body_size, publish_acl_allows};
use crate::messaging::store::RingStore;
use crate::messaging::store::SurfaceFeedTarget;
use crate::messaging::store::ring::Appended;
use crate::messaging::{ChannelEntry, Messenger, ParticipantId};

/// The decision a replay reached, alongside the replayed messages.
pub type Replay = ReplayDecision;

/// One entry of an activation flush the caller has already paid for: what to
/// publish, at which urgency and stamp. Where it goes is a
/// [`PrepaidDestination`], resolved separately.
///
/// A struct rather than a parameter list because the two entry points that take
/// it — [`Messenger::publish_prepaid`] and [`Messenger::park_prepaid`] — must
/// take *exactly* the same admitted entry.
pub struct PrepaidEntry<'a> {
    pub body: &'a str,
    pub urgency: Urgency,
    /// Assigned by the caller in call order across the whole flush, before it was
    /// split by class, so call order stays visible across the class boundary.
    pub publish_ts: DateTime<Utc>,
}

/// Where an activation flush's entries land: the resolved channel, the ring that
/// takes its messages, and the surface subscribers a committed entry is fanned
/// out to.
///
/// Split out of the entry so a flush resolves its destination **once per
/// address** rather than once per entry: the directory and the channel's
/// subscriber list are fixed for the flush's life, and the dominant case is a
/// flush fanning one port. Whether a session is *attached* is still asked per
/// entry, at fan-out, since a session can detach mid-flush.
///
/// Constructing one runs the whole class and authorization decision
/// ([`Messenger::resolve_prepaid`]), so holding one is the proof that entries
/// may be committed onto that channel under that sender.
pub struct PrepaidDestination {
    sender: ParticipantId,
    scheme: ChannelScheme,
    entry: Arc<ChannelEntry>,
    store: Arc<RingStore>,
    feed_targets: Vec<SurfaceFeedTarget>,
}

impl Messenger {
    /// The incarnation every non-durable channel of this process carries. A
    /// resume cursor bearing a different epoch is a guaranteed gap, which is how
    /// restart loss becomes visible on all of them at once.
    pub fn ring_epoch(&self) -> Uuid {
        self.ring_stores().epoch()
    }

    /// Resolve and gate the destination an already-paid-for activation flush's
    /// entries commit onto.
    ///
    /// The capability check is the whole class decision and it is made here,
    /// once: a durable channel's retention is the database's, and a confined
    /// channel never crosses the process boundary at all. The publish ACL is
    /// checked here too, so a destination cannot be held without one.
    ///
    /// # Panics
    ///
    /// If the address names no channel, names one that is not a transportable
    /// ring, or one the sender holds no publish ACL for. Every client-reachable
    /// failure was already answered as a violation by the caller's per-entry
    /// resolve against its boot-resolved output map, so a failure here means
    /// that map disagrees with the directory or the principal's policy —
    /// publishing anyway would route traffic no operator authorized.
    ///
    /// Caller invariant, as for the ladder: `sender` and `policy` MUST both be
    /// derived from the same config-resolved principal, never from client input.
    pub fn resolve_prepaid(
        &self,
        sender: &ParticipantId,
        policy: &AppPolicy,
        channel_address: &str,
    ) -> PrepaidDestination {
        let entry = self.directory.resolve(channel_address).unwrap_or_else(|| {
            panic!(
                "prepaid publish: bound output {channel_address:?} is not in the directory — boot \
                 validation proves every bound output resolves, so this is a broken boot invariant"
            )
        });
        let caps = entry.capabilities();
        assert!(
            !caps.durable && caps.transportable,
            "prepaid publish: bound output {channel_address:?} is not a transportable ring channel \
             — the caller splits its flush by class before reaching here, so this is a broken boot \
             invariant"
        );
        let (scheme, name) = ChannelScheme::split(&entry.address)
            .expect("a directory entry's address always carries its scheme");
        assert!(
            publish_acl_allows(policy, scheme, name),
            "prepaid publish: sender {sender:?} has no publish ACL covering bound output \
             {channel_address:?} — boot validation proves every bound output is policy-covered, so \
             this is a broken boot invariant",
            sender = sender.as_str(),
        );
        let store = self.ring_store(&entry.uuid);
        let feed_targets =
            self.resolve_surface_feed_targets(&entry.address, entry.subscribers.as_slice());
        PrepaidDestination {
            sender: sender.clone(),
            scheme,
            entry,
            store,
            feed_targets,
        }
    }

    /// Apply one entry of an activation flush that is **already paid for**,
    /// committing it into retention.
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
    /// The body-size gate **panics rather than returns**: the caller rejects an
    /// over-cap entry as a violation before drawing against its budget, so a
    /// breach here means the transport and bus caps disagree. The class and ACL
    /// gates were run once, at [`resolve_prepaid`].
    ///
    /// The committed entry is fanned out to the channel's attached surface
    /// subscriptions before this returns: a surface holds no position for
    /// anything to walk, so the commit is its whole delivery trigger.
    ///
    /// Returns the append's outcome, whose `overflow` names every attached
    /// subscriber whose owed messages this entry evicted. The caller must route
    /// it to the noise ladder: the eviction charge is booked as reported the
    /// moment it happens, so a discarded event is a drop no later take will ever
    /// surface.
    ///
    /// [`resolve_prepaid`]: Messenger::resolve_prepaid
    #[must_use]
    pub async fn publish_prepaid(
        &self,
        dest: &PrepaidDestination,
        entry: PrepaidEntry<'_>,
    ) -> Appended {
        let envelope = self.prepaid_envelope(dest, entry);
        let appended = dest.store.append(envelope);
        // Resolution is the destination's, memoized across the flush; attachment
        // is asked now, because a session can detach mid-flush.
        if !dest.feed_targets.is_empty()
            && self
                .router
                .any_surface_session_subscribed(&dest.entry.address, &dest.feed_targets)
        {
            self.fan_out_surface_feed(
                &dest.feed_targets,
                Arc::clone(&appended.retained.envelope),
                i64::try_from(appended.retained.seq)
                    .expect("messaging: retention position out of range"),
            )
            .await;
        }
        appended
    }

    /// Park one entry of an already-paid-for activation flush until `release_at`,
    /// instead of committing it into retention.
    ///
    /// The gates and the caller invariants are [`publish_prepaid`]'s, whole — this
    /// is the same admitted entry taking the other branch of the same decision.
    /// Only where it lands differs: the message goes to the channel's deferred
    /// set, where no retention read can observe it until the release sweep moves
    /// it, and the release time lives in that set rather than on the envelope.
    ///
    /// The cap is the store's own, checked there: a channel holds at most as much
    /// parked future as it holds retained past. Exhaustion is [`QuotaExceeded`]
    /// rather than a drop of the oldest schedule, and it is the caller's to report
    /// — a flush has no error channel back to the guest, so the only honest
    /// treatment is a logged, counted dropped schedule.
    ///
    /// [`publish_prepaid`]: Messenger::publish_prepaid
    pub fn park_prepaid(
        &self,
        dest: &PrepaidDestination,
        entry: PrepaidEntry<'_>,
        release_at: DateTime<Utc>,
    ) -> Result<Uuid, QuotaExceeded> {
        let envelope = self.prepaid_envelope(dest, entry);
        dest.store.park(envelope, release_at)
    }

    /// The per-entry gate every prepaid entry passes, and the envelope it mints.
    /// Shared by the immediate and the parking entry points so one admitted
    /// entry cannot be admitted by two slightly different rules.
    ///
    /// The mint stamps `deliver_after: None` on both paths: a parked message's
    /// release time belongs to the channel's deferred set, and the envelope a
    /// consumer eventually reads is the one that was minted here.
    fn prepaid_envelope(
        &self,
        dest: &PrepaidDestination,
        entry: PrepaidEntry<'_>,
    ) -> MessageEnvelope {
        let PrepaidEntry {
            body,
            urgency,
            publish_ts,
        } = entry;
        if let Err(e) = check_body_size(body, self.defaults.max_body_bytes) {
            panic!(
                "prepaid publish: bound output {addr:?} carries a {len}-byte body over the \
                 {max}-byte cap — the caller rejects an over-cap entry as a violation before \
                 drawing, so the two caps disagree",
                addr = dest.entry.address,
                len = e.len,
                max = e.max,
            );
        }

        MessageEnvelope {
            message_id: Uuid::new_v4(),
            source: self.source().into(),
            channel: dest.entry.address.clone(),
            sender: dest.sender.as_str().into(),
            publish_ts,
            body: body.to_string(),
            reply_to: None,
            delivery_deadline: None,
            deliver_after: None,
            impetus: None,
            urgency,
            envelope_type: dest.scheme,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::access::AppCapability;
    use crate::access::acl::ChannelMatcher;
    use crate::messaging::query::NoopWakeRouter;
    use crate::messaging::store::{OverflowEvent, RingStores};
    use crate::messaging::testutils::{ephemeral_channel_entry, local_channel_entry};
    use crate::messaging::{MessagingDirectory, MessagingGlobalConfig, WakeRouter};

    const CHANNEL: &str = "ephemeral:protobar";
    const NAME: &str = "protobar";

    fn pid(slug: &str) -> ParticipantId {
        ParticipantId::for_app(slug, "test-source")
    }

    /// A `Messenger` over `entries` alone: no durable channels, one ring store
    /// apiece, a noop router.
    fn messenger_with(entries: &[ChannelEntry]) -> Arc<Messenger> {
        let stores = Arc::new(RingStores::build(entries));
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

    fn publisher_policy(channel: &str) -> AppPolicy {
        let mut p = AppPolicy::with_grants(&[AppCapability::EphemeralPublish]);
        p.acls.ephemeral_publish = vec![ChannelMatcher::Exact(channel.to_string())];
        p
    }

    /// A confined channel is in the directory and holds retention, but nothing
    /// here may commit onto it: the class decision is made once, at resolution,
    /// and a caller that reaches it has a broken boot invariant rather than a
    /// tolerable no-op.
    #[test]
    #[should_panic(expected = "not a transportable ring channel")]
    fn a_confined_channel_is_not_a_prepaid_destination() {
        let messenger = messenger_with(&[
            ephemeral_channel_entry(NAME, 8),
            local_channel_entry("confined", 8),
        ]);
        let mut policy = AppPolicy::with_grants(&[AppCapability::EphemeralPublish]);
        policy.acls.ephemeral_publish = vec![ChannelMatcher::Exact("confined".to_string())];
        let _ = messenger.resolve_prepaid(&pid("pub"), &policy, "local:confined");
    }

    /// A prepaid publish enters retention and hands its overflow back to the
    /// caller. The ring charges an eviction as *reported* the moment it
    /// overwrites an unread position, so a caller that drops this outcome drops
    /// the only report there will ever be.
    #[tokio::test]
    async fn publish_prepaid_commits_and_reports_its_overflow() {
        let messenger = messenger_with(&[ephemeral_channel_entry(NAME, 2)]);
        let sender = pid("pub");
        let policy = publisher_policy(NAME);
        let absent = ParticipantId::for_wasm("absent");
        messenger
            .ring_stores()
            .get_by_address(CHANNEL)
            .expect("fixture channel")
            .attach(&absent, "absent", 4);

        let dest = messenger.resolve_prepaid(&sender, &policy, CHANNEL);
        async fn prepaid(messenger: &Messenger, dest: &PrepaidDestination, body: &str) -> Appended {
            messenger
                .publish_prepaid(
                    dest,
                    PrepaidEntry {
                        body,
                        urgency: Urgency::Normal,
                        publish_ts: Utc::now(),
                    },
                )
                .await
        }
        let publish = async |body: &str| prepaid(&messenger, &dest, body).await;

        let first = publish("a").await;
        assert_eq!(first.retained.seq, 1);
        assert_eq!(first.retained.envelope.channel, CHANNEL);
        assert!(first.overflow.is_empty());
        assert!(publish("b").await.overflow.is_empty());

        assert_eq!(
            publish("c").await.overflow,
            vec![OverflowEvent {
                subscriber: absent.clone(),
                dropped: 1,
                app_slug: Some("absent".to_string()),
            }]
        );
    }

    #[tokio::test]
    async fn park_prepaid_holds_the_entry_out_of_retention_until_it_is_due() {
        let messenger = messenger_with(&[ephemeral_channel_entry(NAME, 4)]);
        let store = messenger
            .ring_stores()
            .get_by_address(CHANNEL)
            .expect("fixture channel");
        let now = Utc::now();
        let later = now + chrono::Duration::minutes(10);

        let dest = messenger.resolve_prepaid(&pid("pub"), &publisher_policy(NAME), CHANNEL);
        messenger
            .park_prepaid(
                &dest,
                PrepaidEntry {
                    body: "scheduled",
                    urgency: Urgency::Normal,
                    publish_ts: now,
                },
                later,
            )
            .expect("the deferred set has room");

        // The deferred set carries epoch-ms, so the deadline comes back rounded to
        // the millisecond the schedule named.
        assert_eq!(
            store.next_release(),
            DateTime::from_timestamp_millis(later.timestamp_millis())
        );
        assert!(
            store.retained_tail(u64::MAX).is_empty(),
            "a parked message is not retained"
        );

        let released = store.release_due(later);
        assert_eq!(
            released
                .messages
                .iter()
                .map(|r| r.envelope.body.as_str())
                .collect::<Vec<_>>(),
            vec!["scheduled"],
            "the release moves it onto the channel"
        );
    }

    /// The cap is the channel's `retain_depth`, and exhaustion refuses the *new*
    /// schedule rather than dropping an older one: silently cancelling scheduled
    /// work is worse than declining to schedule more.
    #[tokio::test]
    async fn park_prepaid_refuses_at_the_channels_deferred_cap() {
        let messenger = messenger_with(&[ephemeral_channel_entry(NAME, 1)]);
        let later = Utc::now() + chrono::Duration::minutes(10);
        let dest = messenger.resolve_prepaid(&pid("pub"), &publisher_policy(NAME), CHANNEL);
        let park = |body: &str| {
            messenger.park_prepaid(
                &dest,
                PrepaidEntry {
                    body,
                    urgency: Urgency::Normal,
                    publish_ts: Utc::now(),
                },
                later,
            )
        };

        assert!(park("first").is_ok());
        assert_eq!(park("second"), Err(QuotaExceeded { cap: 1 }));

        let store = messenger
            .ring_stores()
            .get_by_address(CHANNEL)
            .expect("fixture channel");
        assert_eq!(
            store
                .release_due(later)
                .messages
                .iter()
                .map(|r| r.envelope.body.as_str())
                .collect::<Vec<_>>(),
            vec!["first"],
            "the refusal left the schedule that was already there"
        );
    }

    #[test]
    #[should_panic(expected = "is not in the directory")]
    fn resolve_prepaid_panics_on_a_channel_that_is_not_in_the_directory() {
        let messenger = messenger_with(&[ephemeral_channel_entry(NAME, 2)]);
        let _ = messenger.resolve_prepaid(&pid("pub"), &publisher_policy(NAME), "ephemeral:nope");
    }

    #[test]
    #[should_panic(expected = "no publish ACL covering bound output")]
    fn resolve_prepaid_panics_on_an_uncovered_sender() {
        let messenger = messenger_with(&[ephemeral_channel_entry(NAME, 2)]);
        let _ = messenger.resolve_prepaid(
            &pid("pub"),
            &AppPolicy::with_grants(&[AppCapability::EphemeralPublish]),
            CHANNEL,
        );
    }
}
