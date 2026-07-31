//! The wire subscription plane of an attachment: which channels this attacher
//! is subscribed to, at what depths, and where each one's stream stands.
//!
//! Sans-I/O and channel-keyed. [`Subscriptions`] owns exactly what the *wire*
//! says about a channel — the refcount of local subscribers holding it open, the
//! `Subscribe`/`Unsubscribe` handshake state, the opaque resume cursor, and the
//! per-span continuity check — and nothing about what the attacher does with the
//! messages. What it keeps of a channel (retention windows, activation) is the
//! embedder's store, above this layer.
//!
//! One subscription per channel per attachment: the wire delivers each message
//! once and fan-out to whatever the attacher binds to the channel is the
//! attacher's own business. That is the whole de-instancing — N local
//! subscribers on one channel are one wire subscription, refcounted here.
//!
//! Cursors are server-minted, opaque, and client-held: this layer stores the
//! latest one a channel accepted and echoes it verbatim on the next subscribe.
//! It never interprets one, and it never persists anything — a cursor's lifetime
//! is exactly "at least one local subscriber attached".

use std::collections::BTreeMap;

use brenn_attach_proto::{ClientFrame, Cursor, GapInfo, SubscribeOutcome};
use brenn_envelope::is_local_channel;

/// The two independent knobs a subscription is defined by.
///
/// Stated on the wire because they are core bus vocabulary, not a server-side
/// implementation detail: `push_depth` is what wakes the attacher, `retain_depth`
/// is what it may see. The server clamps both to whatever its own configuration
/// resolved for the channel, so an attacher that overstates them sees no
/// difference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubscriptionDepths {
    pub push_depth: u64,
    pub retain_depth: u64,
}

/// Whether a channel's subscription carries a resume claim across a reconnect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumePolicy {
    /// Ordinary: hold the latest accepted cursor and present it on the next
    /// subscribe, so an in-window transport blip is lossless.
    Resume,
    /// Never present a resume claim — every subscribe on this channel is a fresh
    /// attach that receives the retained window again.
    ///
    /// For a channel carrying retained state the attacher must re-apply on every
    /// attachment: a resumed cursor would be answered with zero deliveries on a
    /// same-epoch reconnect, and the state would never be re-applied. The
    /// attacher pays one redelivery per connect for that guarantee.
    Cursorless,
}

/// Where one channel's subscription stands on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WireState {
    /// No `Subscribe` is outstanding for this channel.
    Unsubscribed,
    /// A `Subscribe` was sent; awaiting its `SubscribeResult`.
    Pending,
    /// `SubscribeResult::Ok` arrived; the subscription is live.
    Active,
}

/// What a `SubscribeResult` settled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribeAck {
    /// Frames the embedder must send — the deferred `Unsubscribe` of a channel
    /// whose last local subscriber detached while the `Subscribe` was in flight.
    pub frames: Vec<ClientFrame>,
    /// Whether the subscription is live now. False means the deferred
    /// `Unsubscribe` above just closed it.
    pub live: bool,
    /// How many retained messages the server is about to replay. Informational
    /// here: what an empty replay *means* is the embedder's policy, and for a
    /// cursorless state channel it is a broken peer invariant.
    pub replay_count: u32,
    /// Present when replay could not cover the resume claim. Reported, never
    /// interpreted: this layer's answer to a gap is the resubscribe it already
    /// performed, and what staleness costs is the application's question.
    pub gap: Option<GapInfo>,
}

/// What to do with the envelope of an accepted `Deliver`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliverDisposition {
    /// Route it. The subscription advanced its span and took the cursor.
    ///
    /// `dropped` is the server's count of messages this channel lost since the
    /// previous delivery — the channel's window rolled past this attacher's
    /// position. It is charged once per channel, and every local subscriber on
    /// the channel missed exactly those messages.
    Accept { dropped: u64 },
    /// Discard it: a delivery from a span this attacher has already left, in
    /// flight when its `Unsubscribe` crossed the wire. It advances nothing — not
    /// the span, not the cursor — because a discarded delivery must not resume
    /// past a retained message the next fresh attach is owed.
    ///
    /// `first` marks the first discard of the current post-`Active` window, so a
    /// diagnostic can be raised once per span rather than once per straggler.
    Discard { first: bool },
}

/// Per-channel wire bookkeeping.
struct ChannelSubscription {
    /// How many local subscribers hold this channel open. N on one channel is
    /// one wire subscription.
    refcount: u32,
    depths: SubscriptionDepths,
    resume_policy: ResumePolicy,
    wire: WireState,
    /// The opaque cursor of the last delivery accepted while `Active`, presented
    /// as `Subscribe.resume` on the next subscribe. Discarded the moment the
    /// refcount reaches zero: no local subscriber is owed replay, so a later
    /// fresh attach takes the retained tail rather than resuming past it.
    /// Survives a detach — the subscribers stay attached across a transport blip.
    token: Option<Cursor>,
    /// The largest `seq` accepted on the current span. The server assigns `seq`
    /// strictly increasing per span, so a delivery that does not exceed this is a
    /// peer bug. Reset at each subscribe: a span starts at its `SubscribeResult`
    /// with the counter back at 1.
    span_hw: Option<u64>,
    /// Whether this channel reached `Active` at some point on the current
    /// attachment. A delivery while this is set but the channel is not currently
    /// `Active` is a tolerated straggler; a delivery while it is unset is
    /// inexplicable — the peer's writer orders the `SubscribeResult` ahead of any
    /// replay — and is fatal.
    has_been_active: bool,
    /// Whether a straggler has already been reported in the current post-`Active`
    /// window. Caps the diagnostic at one per span: stragglers are peer-paced, so
    /// nothing may ride an unbounded diagnostic channel on them.
    straggler_reported: bool,
}

impl ChannelSubscription {
    /// Move to `Pending` for a fresh `Subscribe`, reset the span, and answer with
    /// the resume claim to put on it. Class-blind: the peer decides what a cursor
    /// means, including a stale one.
    fn prepare_subscribe(&mut self) -> Option<Cursor> {
        self.wire = WireState::Pending;
        self.span_hw = None;
        match self.resume_policy {
            ResumePolicy::Resume => self.token.clone(),
            ResumePolicy::Cursorless => None,
        }
    }

    fn subscribe_frame(&mut self, channel: &str) -> ClientFrame {
        let resume = self.prepare_subscribe();
        ClientFrame::Subscribe {
            channel: channel.to_string(),
            push_depth: self.depths.push_depth,
            retain_depth: self.depths.retain_depth,
            resume,
        }
    }
}

fn unsubscribe_frame(channel: &str) -> ClientFrame {
    ClientFrame::Unsubscribe {
        channel: channel.to_string(),
    }
}

/// The attachment's wire subscriptions, keyed by channel address.
///
/// The embedder drives it: [`acquire`](Subscriptions::acquire) and
/// [`release`](Subscriptions::release) as its local subscribers come and go,
/// [`on_attached`](Subscriptions::on_attached) and
/// [`on_detached`](Subscriptions::on_detached) as the connection under it comes
/// and goes, and the two intake methods as the peer answers. Frames come back to
/// be sent; a `Result::Err` is a peer contract the layer cannot reconcile, which
/// the embedder turns into a fatal on its connection.
#[derive(Default)]
pub struct Subscriptions {
    channels: BTreeMap<String, ChannelSubscription>,
    /// Whether an attachment is live. Off it there is no wire to carry a
    /// `Subscribe`, so acquisitions are recorded and subscribed at the next
    /// attach.
    live: bool,
}

impl Subscriptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Take a reference on `channel`, subscribing it if this is the first.
    ///
    /// `depths` is the channel's whole subscription, not one local subscriber's
    /// share: the wire has one subscription per channel, so an embedder holding
    /// several local subscribers on one channel folds their depths itself — it is
    /// the layer with the binding table — and states the fold identically at every
    /// acquisition of a channel it already holds. Two different statements for one
    /// channel mean the embedder holds two ideas about a single wire subscription,
    /// which is not representable; it panics rather than silently honouring one of
    /// them. So does a `local:` address (a confined channel never crosses the
    /// wire), a changed [`ResumePolicy`], and depths of `0/0` (a subscription that
    /// neither wakes nor shows anything, which the peer refuses as a violation).
    ///
    /// A channel whose last subscriber released it holds no wire subscription —
    /// its entry survives only to tell stragglers from inexplicable deliveries —
    /// so the first acquisition after that states the subscription afresh and may
    /// state it differently.
    pub fn acquire(
        &mut self,
        channel: &str,
        depths: SubscriptionDepths,
        resume_policy: ResumePolicy,
    ) -> Vec<ClientFrame> {
        assert!(
            !is_local_channel(channel),
            "attach client: {channel:?} is confined and never crosses the wire"
        );
        assert!(
            depths.push_depth > 0 || depths.retain_depth > 0,
            "attach client: subscription on {channel:?} states no depth on either knob"
        );
        let live = self.live;
        let entry = self
            .channels
            .entry(channel.to_string())
            .or_insert(ChannelSubscription {
                refcount: 0,
                depths,
                resume_policy,
                wire: WireState::Unsubscribed,
                token: None,
                span_hw: None,
                has_been_active: false,
                straggler_reported: false,
            });
        // Nothing holds this channel and nothing is open on it: what the entry
        // carries is straggler tolerance, not a subscription, so this acquisition
        // is the one that says what to subscribe.
        if entry.refcount == 0 && entry.wire == WireState::Unsubscribed {
            entry.depths = depths;
            entry.resume_policy = resume_policy;
        }
        assert!(
            entry.depths == depths && entry.resume_policy == resume_policy,
            "attach client: {channel:?} re-acquired with a different subscription \
             ({depths:?}, {resume_policy:?} against {:?}, {:?})",
            entry.depths,
            entry.resume_policy
        );
        entry.refcount = entry.refcount.saturating_add(1);
        match entry.wire {
            WireState::Unsubscribed if live => vec![entry.subscribe_frame(channel)],
            WireState::Unsubscribed | WireState::Pending | WireState::Active => Vec::new(),
        }
    }

    /// Release one reference, unsubscribing `channel` when the last goes.
    ///
    /// Releasing a channel with no reference is an embedder bug and panics: a
    /// detach without a matching attach means the caller's own bookkeeping is
    /// wrong, and carrying on would leave a subscription nobody can ever close.
    pub fn release(&mut self, channel: &str) -> Vec<ClientFrame> {
        let entry = self.channels.get_mut(channel).unwrap_or_else(|| {
            panic!("attach client: release of unsubscribed channel {channel:?}")
        });
        entry.refcount = entry
            .refcount
            .checked_sub(1)
            .unwrap_or_else(|| panic!("attach client: refcount underflow on {channel:?}"));
        if entry.refcount > 0 {
            return Vec::new();
        }
        // Nobody is owed replay any more, so the cursor goes: a later fresh
        // attach takes the retained tail rather than resuming past it.
        entry.token = None;
        let frames = match entry.wire {
            WireState::Active => {
                entry.wire = WireState::Unsubscribed;
                vec![unsubscribe_frame(channel)]
            }
            // Pending: the `Unsubscribe` waits for the `SubscribeResult` — the
            // peer will not accept one for a subscription it has not yet
            // acknowledged. Unsubscribed: nothing was ever open.
            WireState::Pending | WireState::Unsubscribed => Vec::new(),
        };
        self.forget_if_spent(channel);
        frames
    }

    /// The attachment came up: subscribe every channel that still has a local
    /// subscriber, presenting each one's retained cursor.
    ///
    /// A channel at refcount zero is left alone — it holds no cursor and no
    /// subscriber, so no `Subscribe` is ever emitted for it.
    pub fn on_attached(&mut self) -> Vec<ClientFrame> {
        self.live = true;
        let mut frames = Vec::new();
        for (channel, entry) in self.channels.iter_mut() {
            if entry.refcount > 0 && entry.wire == WireState::Unsubscribed {
                frames.push(entry.subscribe_frame(channel));
            }
        }
        frames
    }

    /// The attachment went away. Every wire subscription died with it, so each
    /// channel drops back to `Unsubscribed` and its span state clears — a span
    /// cannot outlive the connection that opened it. Cursors survive: their
    /// owners are still attached, and echoing one is what makes a transport blip
    /// lossless.
    pub fn on_detached(&mut self) {
        self.live = false;
        self.channels.retain(|_, entry| entry.refcount > 0);
        for entry in self.channels.values_mut() {
            entry.wire = WireState::Unsubscribed;
            entry.span_hw = None;
            entry.has_been_active = false;
            entry.straggler_reported = false;
        }
    }

    /// Intake one `SubscribeResult`.
    ///
    /// It must answer a `Pending` channel: the peer's writer orders the result
    /// ahead of any replay, so a result for a channel this attacher is not
    /// waiting on is unreconcilable.
    pub fn on_subscribe_result(
        &mut self,
        channel: &str,
        outcome: SubscribeOutcome,
        replay_count: u32,
        gap: Option<GapInfo>,
    ) -> Result<SubscribeAck, String> {
        let entry = match self.channels.get_mut(channel) {
            Some(entry) if entry.wire == WireState::Pending => entry,
            _ => {
                return Err(format!(
                    "SubscribeResult for a channel not pending: {channel}"
                ));
            }
        };
        let SubscribeOutcome::Ok = outcome;
        // The channel has now been acknowledged on this attachment, even if the
        // next line immediately closes it again: that is what makes a delivery
        // crossing the `Unsubscribe` a straggler rather than an inexplicable one.
        entry.has_been_active = true;
        entry.straggler_reported = false;
        let (frames, live) = if entry.refcount == 0 {
            entry.wire = WireState::Unsubscribed;
            (vec![unsubscribe_frame(channel)], false)
        } else {
            entry.wire = WireState::Active;
            (Vec::new(), true)
        };
        // No pruning here even when the deferred `Unsubscribe` just closed it:
        // the channel has been active, so a straggler is still possible and its
        // entry is what tells one from an inexplicable delivery.
        Ok(SubscribeAck {
            frames,
            live,
            replay_count,
            gap,
        })
    }

    /// Intake one `Deliver`'s wire half, answering what to do with its envelope.
    ///
    /// Fatal, rather than tolerated, for a delivery on a channel this attachment
    /// never had open, and for a `seq` that does not exceed the span's
    /// high-water. Both say the peer is not keeping the contract this attacher
    /// takes everything else on faith from.
    pub fn on_deliver(
        &mut self,
        channel: &str,
        seq: u64,
        cursor: Cursor,
        dropped: u64,
    ) -> Result<DeliverDisposition, String> {
        let entry = match self.channels.get_mut(channel) {
            Some(entry) if entry.has_been_active => entry,
            _ => {
                return Err(format!(
                    "Deliver on a channel never active on this attachment: {channel}"
                ));
            }
        };
        if entry.wire != WireState::Active {
            let first = !entry.straggler_reported;
            entry.straggler_reported = true;
            return Ok(DeliverDisposition::Discard { first });
        }
        if let Some(hw) = entry.span_hw
            && seq <= hw
        {
            return Err(format!(
                "Deliver seq regression on {channel}: {seq} not greater than {hw}"
            ));
        }
        entry.span_hw = Some(seq);
        if entry.resume_policy == ResumePolicy::Resume {
            entry.token = Some(cursor);
        }
        Ok(DeliverDisposition::Accept { dropped })
    }

    /// Whether `channel` has a live wire subscription right now.
    pub fn is_active(&self, channel: &str) -> bool {
        self.channels
            .get(channel)
            .is_some_and(|entry| entry.wire == WireState::Active)
    }

    /// How many local subscribers hold `channel` open.
    pub fn refcount(&self, channel: &str) -> u32 {
        self.channels.get(channel).map_or(0, |entry| entry.refcount)
    }

    /// Every channel with at least one local subscriber, in address order.
    pub fn held_channels(&self) -> Vec<&str> {
        self.channels
            .iter()
            .filter(|(_, entry)| entry.refcount > 0)
            .map(|(channel, _)| channel.as_str())
            .collect()
    }

    /// Drop a channel's entry once it can no longer answer for anything: no
    /// subscriber, nothing open, and no span whose stragglers are still owed
    /// tolerance. An entry that *has* been active is kept until the attachment
    /// ends, which is when its stragglers stop being possible.
    fn forget_if_spent(&mut self, channel: &str) {
        let spent = self.channels.get(channel).is_some_and(|entry| {
            entry.refcount == 0 && entry.wire == WireState::Unsubscribed && !entry.has_been_active
        });
        if spent {
            self.channels.remove(channel);
        }
    }
}

#[cfg(test)]
mod tests;
