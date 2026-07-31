//! The surface's rules for its own confined planes.
//!
//! The [`PlanePolicy`] trait asks three questions of a confined publish: may
//! this writer publish here at all, is this body acceptable as written, and what
//! has just become visible. This module is the surface's answer to all three.
//!
//! Everything it knows comes from two places. The reserved `local:brenn/*` table
//! is contract, fixed at compile time: it says which planes exist and which of
//! them only the kernel writes. The rest — who chrome is, which component
//! instances exist — is the surface's applied wiring, so the policy is
//! re-pointed at each new bindings document and answers nothing before the
//! first one.
//!
//! Two planes have rules beyond admission:
//!
//! - **takeover**: the payload names the instance a request, denial or release
//!   is *from*, and chrome trusts it. The policy overwrites that field with the
//!   authenticated publisher, so a component cannot forge a sibling's takeover.
//! - **overlay-state**: only chrome may report which component holds the
//!   fullscreen overlay, the body must parse, and a named holder must be a
//!   component this surface declares. The kernel remembers the answer, because
//!   holdership is the one confined fact its own telemetry reports.

#[cfg(test)]
mod tests;

use std::collections::BTreeSet;

use brenn_attach_client::router::{GuardedBody, Origin, PlanePolicy};
use brenn_envelope::MessageEnvelope;
use brenn_surface_schema::{
    LOCAL_OVERLAY_STATE_CHANNEL, LOCAL_TAKEOVER_CHANNEL, OverlayReport, OverlayStateBody,
    TakeoverBody, reserved_local_channel,
};

use crate::bindings::AppliedBindings;

/// The surface's confined-plane policy, and the plane state the kernel keeps.
#[derive(Debug, Default)]
pub struct SurfacePlanes {
    /// The identity questions the guards ask, from the current wiring. `None`
    /// until the first bindings document is applied — before that the surface
    /// has no components, so no component-authored publish can exist.
    wiring: Option<PlaneWiring>,
    /// The overlay chrome last reported holding, recorded where the report
    /// reached its readers. Page-local like the stores: a reload starts at
    /// `None`, which is the truthful answer for a page whose chrome has not
    /// spoken yet.
    overlay: Option<OverlayReport>,
}

/// What the guards need out of a bindings document, extracted once per document
/// rather than re-scanned per publish.
#[derive(Debug)]
struct PlaneWiring {
    chrome_instance: String,
    instances: BTreeSet<String>,
}

impl SurfacePlanes {
    pub fn new() -> Self {
        Self::default()
    }

    /// Re-point the policy at a surface's wiring.
    ///
    /// A recorded overlay whose holder the new wiring no longer declares is
    /// dropped: the kernel cannot stand behind a holder for a component this
    /// surface no longer has, and chrome republishes on its next transition. A
    /// holder the new wiring still declares survives — a reconnect is exactly
    /// when a live wedge most needs reporting.
    pub fn apply(&mut self, bindings: &AppliedBindings) {
        let wiring = PlaneWiring {
            chrome_instance: bindings.chrome_instance().to_string(),
            instances: bindings
                .components()
                .iter()
                .map(|c| c.instance.clone())
                .collect(),
        };
        if let Some(overlay) = &self.overlay
            && !wiring.instances.contains(&overlay.holder)
        {
            self.overlay = None;
        }
        self.wiring = Some(wiring);
    }

    /// The overlay chrome holds, or `None` when it holds none.
    pub fn overlay(&self) -> Option<&OverlayReport> {
        self.overlay.as_ref()
    }

    /// The applied wiring.
    ///
    /// # Panics
    ///
    /// If no bindings document has been applied. Every caller is reached from a
    /// publish on a plane, and a plane is publishable only once the wiring that
    /// declares its writers exists.
    fn wiring(&self) -> &PlaneWiring {
        self.wiring
            .as_ref()
            .expect("surface client: a confined publish implies applied bindings")
    }

    /// Judge a publish on the overlay-state plane, without recording anything.
    ///
    /// Three refusals, all of them "the kernel would otherwise report something
    /// it cannot stand behind": a publisher that is not this surface's chrome (a
    /// component cannot speak for chrome's overlay — chrome is unique per
    /// surface, so any other publisher is an operator wiring a lie); a body that
    /// does not parse; and a `holder` naming an instance the surface does not
    /// declare, which is not a principal this surface can attribute anything to.
    fn refuse_overlay_state(&self, instance: &str, body: &str) -> Option<String> {
        let wiring = self.wiring();
        if wiring.chrome_instance != instance {
            return Some("only the surface's chrome instance may publish it".to_string());
        }
        let parsed: OverlayStateBody = match serde_json::from_str(body) {
            Ok(parsed) => parsed,
            Err(err) => return Some(format!("unparseable body: {err}")),
        };
        match &parsed.holder {
            Some(holder) if !wiring.instances.contains(holder) => {
                Some(format!("holder {holder:?} is not a declared instance"))
            }
            _ => None,
        }
    }
}

impl PlanePolicy for SurfacePlanes {
    /// Which writer a confined channel admits.
    ///
    /// The kernel writes exactly the reserved planes it owns; a component writes
    /// everything else confined — its own operator-declared channels, and the
    /// reserved planes that are not kernel-only. An address in the reserved
    /// namespace that the table does not define is undefined vocabulary and
    /// admits nobody.
    fn admits(&self, channel: &str, origin: Origin<'_>) -> bool {
        let kernel_only = reserved_local_channel(channel).is_some_and(|r| r.kernel_publish_only);
        match origin {
            Origin::Attacher => kernel_only,
            Origin::Sub(_) => !kernel_only && !is_undefined_reserved(channel),
        }
    }

    /// The reserved planes' rules for one body about to become a message.
    ///
    /// # Panics
    ///
    /// If the kernel itself reaches either guarded plane. Both are component
    /// planes — [`Self::admits`] refuses the kernel on them — and the kernel has
    /// neither a takeover identity to stamp nor an overlay to report, so a
    /// kernel-minted body on either would be the kernel inventing telemetry
    /// about a component.
    fn guard(&self, channel: &str, origin: Origin<'_>, body: String) -> GuardedBody {
        let instance = match (channel, origin) {
            (LOCAL_OVERLAY_STATE_CHANNEL | LOCAL_TAKEOVER_CHANNEL, Origin::Sub(instance)) => {
                instance
            }
            (LOCAL_OVERLAY_STATE_CHANNEL | LOCAL_TAKEOVER_CHANNEL, Origin::Attacher) => {
                panic!("surface client: the kernel does not publish on {channel}")
            }
            _ => return GuardedBody::Carry(body),
        };
        if channel == LOCAL_OVERLAY_STATE_CHANNEL {
            return match self.refuse_overlay_state(instance, &body) {
                Some(reason) => GuardedBody::Refused(reason),
                None => GuardedBody::Carry(body),
            };
        }
        GuardedBody::Carry(inject_takeover_instance(body, instance))
    }

    /// Record who holds the overlay.
    ///
    /// Called where the message became observable rather than where it was
    /// minted, so a schedule that was parked and then cancelled — one no reader
    /// ever saw and never will — leaves no trace in what the kernel reports.
    fn observe(&mut self, envelope: &MessageEnvelope) {
        if envelope.channel != LOCAL_OVERLAY_STATE_CHANNEL {
            return;
        }
        let parsed: OverlayStateBody = serde_json::from_str(&envelope.body)
            .expect("surface client: a body on the overlay plane parsed when it was accepted");
        // A holder the surface stopped declaring between the guard and here — a
        // parked report released after a wiring change — is skipped rather than
        // recorded: the kernel would otherwise report a component it no longer
        // has.
        if let Some(holder) = &parsed.holder
            && !self.wiring().instances.contains(holder)
        {
            return;
        }
        self.overlay = parsed.holder.map(|holder| OverlayReport {
            holder,
            since: envelope.publish_ts,
        });
    }
}

/// Whether `channel` sits in the reserved namespace without naming a plane the
/// contract defines. Reserved names are unreachable through operator config, so
/// such an address is vocabulary nobody has written rules for.
fn is_undefined_reserved(channel: &str) -> bool {
    brenn_surface_schema::is_reserved_local_namespace(channel)
        && reserved_local_channel(channel).is_none()
}

/// Overwrite the `instance` field of a takeover body with the authenticated
/// publisher. A body that does not parse as a [`TakeoverBody`] is passed through
/// unchanged — any receiver must reject an unparseable body, so a malformed
/// spoof attempt gains nothing from bypassing the stamp.
// TODO(takeover-parser-symmetry-guard): the anti-spoof guarantee rests on the
// router and chrome sharing the exact same parse strictness for `TakeoverBody`
// (both reject the same malformed bodies). Nothing structural enforces that
// cross-crate symmetry; if chrome's parser is ever loosened, an unstamped body
// the router passed through could be accepted, reopening instance forgery.
// Close the passthrough at the trust boundary, or pin the symmetry structurally.
fn inject_takeover_instance(body: String, instance: &str) -> String {
    match serde_json::from_str::<TakeoverBody>(&body) {
        Ok(mut parsed) => {
            parsed.instance = instance.to_string();
            serde_json::to_string(&parsed).expect("a TakeoverBody serializes to JSON")
        }
        Err(_) => body,
    }
}
