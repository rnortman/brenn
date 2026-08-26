//! The two policy passes boot runs over its resolved surfaces: the substrate
//! grant every surface gets for its own geometry/status channels, and the
//! coverage assertion that no declared output can publish into a channel its
//! policy does not authorize.
//!
//! They live beside the surface routes rather than with the wiring that calls
//! them, because they read the surface module's derived channel names and the
//! fixtures here mimic boot with them.

use brenn_lib::messaging::ChannelScheme;
use brenn_lib::messaging::config::ResolvedSurface;

/// Assert every surface output binding is covered by the surface's publish
/// policy: the binding's channel must be authorized by a transport grant plus a
/// covering publish ACL matcher, else the output can never publish — dead
/// config, fail fast.
///
/// Runs as a post-pass **after** [`inject_surface_error_grant`], so a
/// `[[surface.output]]` bound to the configured `surface_error_channel` is
/// covered by the injected substrate grant (the many-writer shape the operator
/// opted into) rather than panicking on a grant that is injected moments later.
pub fn assert_output_bindings_covered(surfaces: &[ResolvedSurface]) {
    for surface in surfaces {
        for out in &surface.outputs {
            let covered = match ChannelScheme::split(&out.channel_address) {
                Some((ChannelScheme::Ephemeral, name)) => {
                    surface.policy.allows_ephemeral_publish(name)
                }
                Some((ChannelScheme::Brenn, name)) => surface.policy.allows_brenn_publish(name),
                // A `local:` output publishes into the page's own router and
                // never onto the bus, so there is no publish for a bus ACL to
                // authorize — requiring coverage would demand the operator grant
                // a right the server does not mediate. Its declaration is policed
                // by `resolve_surfaces` (reserved-channel rules incl. the
                // takeover grant and the kernel-publish-only planes).
                Some((ChannelScheme::Local, _)) => true,
                // resolve_surfaces' validate_binding already enforced the
                // brenn:/ephemeral:/local: scheme on every output.
                Some((
                    ChannelScheme::Mqtt | ChannelScheme::Webhook | ChannelScheme::PwaPush,
                    _,
                ))
                | None => {
                    unreachable!("output channel scheme validated by validate_binding")
                }
            };
            assert!(
                covered,
                "config: [[surface]] {:?}: output binds channel {:?} but the surface's \
                 access policy does not authorize publishing there (missing transport grant \
                 and/or a covering publish ACL matcher) — dead config",
                surface.slug, out.channel_address,
            );
        }
    }
}

/// Inject the surface self-description telemetry grant onto every resolved
/// surface's policy: each surface may publish its own geometry and status
/// documents onto its two derived runtime channels under its own
/// `surface:<slug>` identity. Applied immediately after
/// [`resolve_surfaces`], alongside the error-report grant, so each policy is
/// complete everywhere it is read (the runtimes, the subscriber registry, and the
/// single-writer sweep, which excludes exactly this owning-surface coverage).
///
/// Like error reporting, geometry/status is a substrate right every surface
/// has, not a per-`[[surface]]` operator grant — a forgotten surface would
/// otherwise vanish from telemetry with no error. Each surface receives an
/// exact `brenn_publish` matcher for its own two channels only; the sweep proves
/// no *other* principal can write them. `prefix` roots the derived bare names.
pub fn inject_surface_geometry_status_grants(surfaces: &mut [ResolvedSurface], prefix: &str) {
    use super::description::{surface_geometry_bare, surface_status_bare};
    use brenn_envelope::grants::AppCapability;
    use brenn_lib::access::acl::ChannelMatcher;
    for surface in surfaces {
        let geometry = surface_geometry_bare(prefix, &surface.slug);
        let status = surface_status_bare(prefix, &surface.slug);
        surface
            .policy
            .grants
            .insert(AppCapability::MessagingPublish);
        surface
            .policy
            .acls
            .brenn_publish
            .push(ChannelMatcher::Exact(geometry));
        surface
            .policy
            .acls
            .brenn_publish
            .push(ChannelMatcher::Exact(status));
    }
}
