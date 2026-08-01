//! The surface's two-phase connect: what an attachment gives it, and what it
//! still has to fetch before it can do anything with it.
//!
//! An attachment hands the surface only transport facts — who it is on this
//! connection, how big a body may be, whether it may alert. Its *wiring* — the
//! components to mount, the channels to bind, where its telemetry goes — is
//! retained state on a channel, so the surface is not configured when the
//! attachment comes up. It is configured one round trip later, when the retained
//! bindings document arrives.
//!
//! That is the whole two-phase shape:
//!
//! 1. **Transport up.** [`SurfaceConnect::on_attached`] records the attachment's
//!    facts and takes a reference on the config channel, which subscribes it
//!    immediately. Nothing else is subscribed: the set the surface *may*
//!    subscribe is what the document about to arrive says it is, and asking for
//!    a channel the peer's current configuration no longer admits is a protocol
//!    violation.
//! 2. **Configured.** The document arrives as an ordinary delivery,
//!    [`SurfaceConnect::on_config_deliver`] applies it, and the caller
//!    reconciles its stores, registrations and subscriptions against the wiring
//!    now in force.
//!
//! Phase 2 runs again on every reconnect, which is what the config
//! subscription's cursorless policy buys: a resumed cursor would be answered
//! with zero deliveries on a same-epoch reconnect and the wiring would never be
//! re-applied. The price is one redelivered document per attachment, and the
//! byte comparison below is what keeps an unchanged one from costing anything
//! more.
//!
//! Every refusal here is fatal to the attachment rather than something to retry.
//! Both ends of a live surface are built together and the peer resolves this
//! document from configuration it validated at boot, so an empty replay or a
//! document this kernel cannot apply is a broken peer, and reconnecting into the
//! identical answer is the carry-on this project does not do.

#[cfg(test)]
mod tests;

use brenn_attach_client::conn::AttachmentFacts;
use brenn_attach_client::subs::{ResumePolicy, SubscribeAck, SubscriptionDepths, Subscriptions};
use brenn_attach_proto::ClientFrame;

use crate::bindings::{AppliedBindings, channel_is_transportable};

/// What the surface asks for on its config channel.
///
/// One message deep on both knobs: the channel carries exactly one retained
/// document, the surface wants to be woken when it changes, and a window deeper
/// than the state it holds would only offer superseded copies of it.
const CONFIG_DEPTHS: SubscriptionDepths = SubscriptionDepths {
    push_depth: 1,
    retain_depth: 1,
};

/// How far the surface has got with the attachment it currently has.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// No attachment. Any wiring from a previous one is still held — it is what
    /// the next document is compared against — but nothing may be sent.
    Detached,
    /// Phase 1 done: the attachment is live and its config subscription is open
    /// or opening. The surface has transport facts and no wiring it may act on.
    AwaitingConfig,
    /// Phase 2 done: a bindings document has arrived on this attachment and the
    /// wiring it describes is in force.
    Configured,
}

/// What applying a bindings document settled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigApplied {
    /// Whether this is the first document of the current attachment — phase 2
    /// proper, after which the caller reconciles and the surface is connected.
    ///
    /// False for a second document mid-attachment. A healthy peer never sends
    /// one: the document is published once per boot, and a republish implies a
    /// restart, which severed this socket. It is handled rather than refused
    /// because it is well-formed input, and the conservative answer to wiring
    /// that changed under a live page is the same one a reconnect gets.
    pub first_of_attachment: bool,
    /// Whether the wiring differs from what was in force before it.
    ///
    /// False for the first document a page ever applies: there was no previous
    /// wiring for it to differ from. Otherwise it is byte inequality of the two
    /// retained bodies, which is the caller's cue to reload the page — the
    /// components it mounted and the ports it attached were built against wiring
    /// that no longer describes this surface.
    pub wiring_changed: bool,
}

/// The surface's half of the connect sequence: the config channel's custody, the
/// attachment facts, and the wiring in force.
///
/// Holds no stores, no registrations and no components. What it answers is *when*
/// the surface may act and *on what*; acting on it is the layers above.
pub struct SurfaceConnect {
    /// The address the bindings document is retained on, from the page's boot
    /// identity. Constant for the life of the page.
    config_channel: String,
    phase: Phase,
    /// The current attachment's transport contract; `None` while detached.
    facts: Option<AttachmentFacts>,
    /// The wiring in force. Survives a detach — it is what the next attachment's
    /// document is compared against, and comparing is the only way to know
    /// whether the page must reload.
    bindings: Option<AppliedBindings>,
}

impl SurfaceConnect {
    /// Build the connect state for a page whose bindings document is retained on
    /// `config_channel`.
    ///
    /// # Panics
    ///
    /// If the address is empty or does not cross the wire. It comes from the
    /// page's boot identity, rendered by the same server that publishes the
    /// document, so anything else there is a broken boot rather than a
    /// configuration the page could work around.
    pub fn new(config_channel: String) -> Self {
        assert!(
            !config_channel.is_empty(),
            "surface client: the page declares no config channel"
        );
        assert!(
            channel_is_transportable(&config_channel),
            "surface client: config channel {config_channel:?} does not cross the wire"
        );
        Self {
            config_channel,
            phase: Phase::Detached,
            facts: None,
            bindings: None,
        }
    }

    /// The address the bindings document is retained on.
    pub fn config_channel(&self) -> &str {
        &self.config_channel
    }

    /// Whether `channel` is the config channel — the one delivery the surface
    /// routes into [`on_config_deliver`](SurfaceConnect::on_config_deliver)
    /// rather than to a bound port.
    pub fn is_config_channel(&self, channel: &str) -> bool {
        channel == self.config_channel
    }

    /// How far the surface has got with its current attachment.
    pub fn phase(&self) -> Phase {
        self.phase
    }

    /// The current attachment's transport contract, or `None` while detached.
    pub fn facts(&self) -> Option<&AttachmentFacts> {
        self.facts.as_ref()
    }

    /// The wiring in force, or `None` before the first document of this page.
    ///
    /// Outlives the attachment it arrived on: a detached page still knows what
    /// it mounted.
    pub fn bindings(&self) -> Option<&AppliedBindings> {
        self.bindings.as_ref()
    }

    /// The wiring the *current* attachment put in force, or `None` until its own
    /// document has arrived.
    ///
    /// The question every wire-bound composition asks, and it is not the same one
    /// [`bindings`](Self::bindings) answers. Between phase 1 and phase 2 of a
    /// reconnect the page is live and still holding the previous attachment's
    /// wiring, and the peer on the other end of the new socket judges a channel
    /// address and an attribution against its own current configuration — so a
    /// frame composed from the old wiring is answered with a protocol close and a
    /// fail2ban strike charged to a legitimate user. Page-local work asks
    /// `bindings` and keeps running; anything bound for the socket asks this.
    pub fn configured_bindings(&self) -> Option<&AppliedBindings> {
        match self.phase {
            Phase::Configured => self.bindings.as_ref(),
            Phase::Detached | Phase::AwaitingConfig => None,
        }
    }

    /// Phase 1: the attachment is live.
    ///
    /// Records its facts and takes the config channel's reference, which the
    /// now-live subscription plane turns into the one `Subscribe` this phase
    /// sends. The caller sends the frames and waits; nothing else about the
    /// surface may move until the document lands.
    ///
    /// # Panics
    ///
    /// If an attachment is already recorded. Two live attachments under one page
    /// is not a state the surface has — the connection hands over exactly one at
    /// a time — so reaching it means the caller's own bookkeeping is wrong.
    pub fn on_attached(
        &mut self,
        facts: AttachmentFacts,
        subs: &mut Subscriptions,
    ) -> Vec<ClientFrame> {
        assert_eq!(
            self.phase,
            Phase::Detached,
            "surface client: attached while an attachment is already live"
        );
        self.facts = Some(facts);
        self.phase = Phase::AwaitingConfig;
        // Live first, so the acquisition below emits its own `Subscribe`; the
        // application channels stay unsubscribed until the document says which
        // of them this surface still has.
        subs.go_live();
        subs.acquire(
            &self.config_channel,
            CONFIG_DEPTHS,
            // Cursorless: a resumed cursor answers `UpToDate` with no deliveries
            // on a same-epoch reconnect, and phase 2 would never run again.
            ResumePolicy::Cursorless,
        )
    }

    /// The attachment went away.
    ///
    /// Drops the config channel's reference — the subscription died with the
    /// connection and the next attachment states it afresh — and tears down the
    /// subscription plane's per-connection half. The wiring stays: it is what
    /// the page is still running on, and what the next document is compared
    /// against.
    ///
    /// Tolerates a detach with no attachment behind it: a connection that dropped
    /// while negotiating reports one, and the surface never got a phase 1.
    pub fn on_detached(&mut self, subs: &mut Subscriptions) {
        // The plane's teardown first, so the release below sees a channel with
        // no open subscription and emits no `Unsubscribe` into a dead socket.
        subs.on_detached();
        if self.phase != Phase::Detached {
            subs.release(&self.config_channel);
        }
        self.phase = Phase::Detached;
        self.facts = None;
    }

    /// Intake the config channel's own `SubscribeResult`.
    ///
    /// `Err` names a broken peer invariant for the caller to go fatal on. An
    /// empty replay is one: the peer publishes the document before it accepts a
    /// single connection and this subscription carries no resume claim, so there
    /// is no race and no cursor that could have skipped past it — the document
    /// is simply not there. So is a gap, which answers a resume claim this
    /// subscription never makes.
    pub fn on_config_ack(&self, ack: &SubscribeAck) -> Result<(), String> {
        if let Some(gap) = &ack.gap {
            return Err(format!(
                "the surface config channel answered a resume claim it was never given: {gap:?}"
            ));
        }
        if ack.replay_count == 0 {
            return Err(format!(
                "the surface config channel {} retains no bindings document",
                self.config_channel
            ));
        }
        Ok(())
    }

    /// Phase 2: apply a bindings document delivered on the config channel.
    ///
    /// `Err` names what makes the body unusable, for the caller to go fatal on.
    /// There is no partial application and nothing to fall back on: the previous
    /// wiring described a surface this page may no longer be, and a page that
    /// cannot read its own wiring has nothing to render.
    ///
    /// # Panics
    ///
    /// If no attachment is live. The document arrives as a delivery on a
    /// subscription that only exists while attached, so reaching this detached
    /// means the caller routed a frame from a connection it had already torn
    /// down.
    pub fn on_config_deliver(&mut self, body: &str) -> Result<ConfigApplied, String> {
        assert_ne!(
            self.phase,
            Phase::Detached,
            "surface client: config document delivered with no attachment"
        );
        let applied = AppliedBindings::apply(body)
            .map_err(|e| format!("the surface bindings document is unusable: {e}"))?;
        let wiring_changed = self
            .bindings
            .as_ref()
            .is_some_and(|prior| !prior.same_wiring_as(&applied));
        let first_of_attachment = self.phase == Phase::AwaitingConfig;
        self.bindings = Some(applied);
        self.phase = Phase::Configured;
        Ok(ConfigApplied {
            first_of_attachment,
            wiring_changed,
        })
    }
}
