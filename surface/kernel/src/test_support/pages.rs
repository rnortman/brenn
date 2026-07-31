//! The attachment every fixture page comes up under, and the assemblers that get a
//! page from its first instant to a document in force.
//!
//! [`AttachmentFacts`] is a wire-contract type the protocol rounds are the most
//! likely thing in the tree to reshape, and the register-then-track-then-apply
//! sequence's steps must move together — a registered instance with no scheduler
//! state panics on its next assembly. Both live here so a suite that varies a knob
//! says so in a `..`-override instead of restating the shape.

use brenn_attach_client::Millis;
use brenn_attach_client::conn::AttachmentFacts;
use brenn_attach_proto::SubscribeOutcome;
use brenn_surface_schema::bindings::BindingsDocument;
use uuid::Uuid;

use crate::page::SurfacePage;

/// The principal every fixture page attaches as: the `bar` surface of the shared
/// cast.
pub(crate) const PRINCIPAL: &str = "surface:bar";

/// The attachment's own id, which the surface's documents are self-attributed with.
pub(crate) const SESSION_ID: &str = "s-1";

/// The publish-body cap every fixture attachment states.
pub(crate) const BODY_CAP: u64 = 4_096;

/// The attachment a fixture page comes up under: the `bar` surface, one session, a
/// body cap generous enough for any fixture body, and no alert grant.
///
/// A suite that varies one of them overrides it —
/// `AttachmentFacts { alert_granted: true, ..pages::facts() }` — so a field added to
/// the contract lands here alone.
pub(crate) fn facts() -> AttachmentFacts {
    AttachmentFacts {
        version: 1,
        participant_id: PRINCIPAL.to_string(),
        session_id: SESSION_ID.to_string(),
        heartbeat_secs: 20,
        max_body_bytes: BODY_CAP,
        max_frame_bytes: 65_536,
        alert_granted: false,
    }
}

/// A page through phase 1: attached under `facts`, with the config channel's own
/// subscribe acknowledged so a document may be delivered on it.
pub(crate) fn attached_page(
    config_channel: &str,
    epoch: Uuid,
    facts: AttachmentFacts,
) -> SurfacePage {
    let mut page = SurfacePage::new(config_channel.to_string(), epoch);
    page.on_attached(facts);
    page.subs
        .on_subscribe_result(config_channel, SubscribeOutcome::Ok, 1, None)
        .expect("the config channel is pending");
    page
}

/// Register and track each of `instances`.
///
/// The two move together deliberately: registration is what gives an instance
/// positions and subscriptions, and the scheduler state is what an assembly of it
/// requires — a fixture holding one without the other is a page no pass can drive.
pub(crate) fn mount(page: &mut SurfacePage, instances: &[&str]) {
    for instance in instances {
        page.registrations
            .register(instance, None, &mut page.stores, &mut page.subs);
        page.schedules.track(instance);
    }
}

/// A configured page: phase 1, `instances` mounted, and `doc` in force.
pub(crate) fn configured_page(
    config_channel: &str,
    epoch: Uuid,
    facts: AttachmentFacts,
    instances: &[&str],
    doc: &BindingsDocument,
    now: Millis,
) -> SurfacePage {
    let mut page = attached_page(config_channel, epoch, facts);
    mount(&mut page, instances);
    page.apply_config(&doc.to_body(), now)
        .expect("the fixture document applies");
    page
}
