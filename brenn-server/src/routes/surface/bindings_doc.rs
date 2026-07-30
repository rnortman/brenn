//! The per-surface bindings document: build at boot, publish once, before the
//! server accepts a connection.
//!
//! A surface's wiring — which components to mount, what they bind, where the
//! kernel's own telemetry and error reports go — is boot-resolved state, and
//! state on this bus is a retained channel. So the document is built from the
//! same [`ResolvedSurface`] list every other boot consumer reads and published
//! onto the surface's config channel under the reserved single-writer
//! `system:surface-config` identity. A surface replays it on every attach.
//!
//! The body is a pure function of resolved config (the schema crate's
//! determinism rule): two boots on unchanged config produce identical bytes, so
//! a reconnecting surface can compare what it is handed against what it is
//! running and reload only on a real difference.

use brenn_lib::messaging::config::ResolvedSurface;
use brenn_lib::messaging::{Messenger, PublishResult, Urgency};
use brenn_surface_schema::bindings::{
    BINDINGS_DOCUMENT_VERSION, BindingsDocument, PlatformSection,
};
use brenn_surface_schema::{LogLevel, SurfaceBindings};

use super::description::{
    SURFACE_CONFIG_COMPONENT, surface_config_channel, surface_geometry_channel,
    surface_status_channel,
};
use super::lower_surface_bindings;

/// The parameters the document carries that are not per-surface config: the
/// derived-channel namespace, the status cadence, and the substrate
/// error-reporting wiring (channel + floor, `None` when no error channel is
/// configured — the kernel then keeps its console copy only).
pub struct BindingsDocParams<'a> {
    /// Bare-name namespace rooting every derived channel address.
    pub prefix: &'a str,
    /// Status document cadence, seconds.
    pub status_interval_secs: u32,
    /// `(channel address, publish floor)` from `[observability]`, or `None`.
    pub error_report: Option<(&'a str, LogLevel)>,
}

/// Build one surface's bindings document from its resolved config.
///
/// Every field is read off `resolved` or off boot-wide parameters; nothing here
/// reads a clock, a connection, or a random source, which is what makes the body
/// byte-stable across boots on unchanged config.
///
/// # Panics
///
/// If the surface declares no chrome component. Surface resolution enforces
/// exactly one per surface, so a miss is a broken boot invariant.
pub fn build_bindings_document(
    resolved: &ResolvedSurface,
    params: &BindingsDocParams<'_>,
) -> BindingsDocument {
    let (error_channel, error_report_floor) = match params.error_report {
        Some((channel, floor)) => (Some(channel.to_string()), Some(floor)),
        None => (None, None),
    };
    let SurfaceBindings {
        components,
        subscriptions,
        outputs,
        local_channels,
        chrome_instance,
    } = lower_surface_bindings(resolved);
    BindingsDocument {
        v: BINDINGS_DOCUMENT_VERSION,
        components,
        subscriptions,
        outputs,
        local_channels,
        chrome_instance,
        platform: PlatformSection {
            geometry_channel: surface_geometry_channel(params.prefix, &resolved.slug),
            status_channel: surface_status_channel(params.prefix, &resolved.slug),
            status_interval_secs: params.status_interval_secs,
            error_channel,
            error_report_floor,
            takeover_granted: resolved
                .policy
                .grants
                .has(brenn_lib::access::AppCapability::SurfaceTakeover),
        },
    }
}

/// Build every surface's bindings document as `(config channel address, body)`
/// pairs, in surface declaration order — the publish loop's input.
///
/// # Panics
///
/// If a built document fails the schema's own validation. The builder is the
/// only writer these documents ever have, so a document it cannot validate means
/// the resolver and the schema disagree about what wiring is representable —
/// worse to publish than to refuse.
pub fn build_bindings_documents(
    surfaces: &[ResolvedSurface],
    params: &BindingsDocParams<'_>,
) -> Vec<(String, String)> {
    surfaces
        .iter()
        .map(|resolved| {
            let doc = build_bindings_document(resolved, params);
            doc.validate().unwrap_or_else(|err| {
                panic!(
                    "boot: built bindings document for surface {:?} does not validate ({err}) — \
                     the surface resolver and the document schema disagree about representable \
                     wiring. Refusing to start.",
                    resolved.slug,
                )
            });
            (
                surface_config_channel(params.prefix, &resolved.slug),
                doc.to_body(),
            )
        })
        .collect()
}

/// Publish every bindings document under the `system:surface-config` identity,
/// once at boot, before the server begins accepting connections.
///
/// # Panics
///
/// A publish that does not return `Ok` panics rather than starting with surfaces
/// that can never boot. `BodyTooLarge` is the operator-reachable arm — a surface
/// with many components and large config maps against an operator-set
/// `max_body_bytes` — so it gets a config-flavored message; every other arm is a
/// host bug made unreachable by the code-built policy and the boot-validated
/// channels.
pub async fn publish_bindings_documents(messenger: &Messenger, docs: &[(String, String)]) {
    for (address, body) in docs {
        let result = messenger
            .publish_from_system(
                SURFACE_CONFIG_COMPONENT,
                address,
                body,
                Urgency::Normal,
                None,
            )
            .await;
        match result {
            PublishResult::Ok { .. } => {}
            PublishResult::BodyTooLarge { len, max } => panic!(
                "boot: bindings-document publish to {address:?} rejected — the document is {len} \
                 bytes but [messaging] max_body_bytes is {max}. A surface cannot boot without it; \
                 raise max_body_bytes above {len} (or shrink the surface's component config maps). \
                 Refusing to start (fail-fast on invalid config)."
            ),
            other => panic!(
                "boot: bindings-document publish to {address:?} did not succeed ({other:?}) — the \
                 reserved system publisher's policy and the boot-validated channels make this \
                 unreachable, so a failure is a host bug. Refusing to start."
            ),
        }
    }
}

#[cfg(test)]
mod tests;
