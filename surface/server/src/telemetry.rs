//! The surface telemetry the *server* writes: the `disconnected` stamp.
//!
//! Live geometry and status documents are the page's own — the kernel observes
//! the viewport and owns the mount table, and publishes both documents like any
//! other message. `disconnected` is the one flavour a page cannot self-report,
//! so the server writes it: once per surface at boot, and once more when the
//! surface's last attachment ends. The document shapes are
//! [`brenn_surface_schema::telemetry`](brenn_surface_schema::telemetry), shared
//! with the kernel; every document on a given channel is latest-wins on a
//! retained-depth-bounded channel.

use brenn_lib::messaging::Urgency;
use brenn_lib::messaging::config::ResolvedSurface;
use brenn_messaging::{Messenger, PublishResult};
use brenn_surface_schema::telemetry::DisconnectedStamp;
use uuid::Uuid;

use super::SurfaceRuntime;
use super::description::surface_status_channel;

/// Build a server-written `disconnected` stamp: the terminal snapshot when the
/// last session for a slug closes, and the boot stamp. `session` is the closing
/// session for a terminal snapshot and `None` for a boot stamp.
///
/// # Panics
///
/// If the composed stamp fails the schema's own rules — an empty `reason` or an
/// empty `session`. Every caller passes a literal reason and a minted session
/// id, so a failure is a broken caller in this build rather than bad input; the
/// alternative is publishing a retained stamp every reader refuses, which is how
/// "is this surface down?" would silently stop having an answer.
fn disconnected_body(session: Option<&str>, epoch: Uuid, reason: &str) -> String {
    let stamp = DisconnectedStamp::new(
        session.map(str::to_string),
        chrono::Utc::now(),
        epoch,
        reason.to_string(),
    );
    stamp
        .validate()
        .expect("disconnected stamp composed from server-held facts");
    stamp.to_body()
}

/// What an oversized stamp body means at the site that publishes it. Every
/// other refusal is a broken invariant at both sites; this is the one outcome
/// the two flavours judge differently.
#[derive(Debug, Clone, Copy)]
enum Oversize {
    /// A body the configured cap refuses is a config error found while the
    /// server is still starting, so it joins every other refusal in the panic.
    Fatal,
    /// The socket is already closing and the stamp is one latest-wins document,
    /// so a late-discovered config error is logged and the stamp dropped rather
    /// than taken as grounds to end the process.
    DropAndLog,
}

/// Publish one `disconnected` stamp to `channel` through the platform path
/// (send-budget exempt), composing the body from server-held facts.
///
/// # Panics
///
/// On any outcome but `Ok`, except an oversized body under
/// [`Oversize::DropAndLog`]: the status channel is boot-declared,
/// single-writer, and covered by the surface's injected geometry/status grant,
/// and the platform path is send-budget exempt, so a refusal is a broken boot
/// invariant rather than anything a running server reaches.
async fn publish_stamp(
    messenger: &Messenger,
    slug: &str,
    channel: &str,
    session: Option<&str>,
    epoch: Uuid,
    reason: &str,
    oversize: Oversize,
) {
    let body = disconnected_body(session, epoch, reason);
    match messenger
        .publish_from_surface_platform(slug, channel, &body, Urgency::Normal)
        .await
    {
        PublishResult::Ok { .. } => {}
        PublishResult::BodyTooLarge { len, max } if matches!(oversize, Oversize::DropAndLog) => {
            tracing::error!(
                surface = %slug,
                channel = %channel,
                len,
                max,
                "surface disconnected stamp rejected as oversized — the server-built body \
                 exceeds max_body_bytes; dropping this stamp"
            );
        }
        other => panic!(
            "surface {slug}: {reason:?} disconnected stamp publish to {channel} did not succeed \
             ({other:?}) — the status channel is boot-declared, single-writer, and covered by the \
             surface's injected geometry/status grant, and the platform path is send-budget \
             exempt, so any failure is a broken boot invariant"
        ),
    }
}

/// Publish a boot `disconnected` stamp (`reason: "server restart"`, the new bus
/// `epoch`) to every configured surface's status channel, once
/// at boot after the boot-published documents. A durable status channel's
/// retained row survives a restart; without this stamp a dead or not-yet-connected
/// wall would read "healthy as of before the restart" until a reader did timestamp
/// math.
///
/// # Panics
///
/// On any non-`Ok` outcome, oversized bodies included, rather than starting with
/// a stale retained value; see [`publish_stamp`].
pub async fn publish_boot_disconnected_stamps(
    messenger: &Messenger,
    prefix: &str,
    surfaces: &[ResolvedSurface],
    epoch: Uuid,
) {
    for surface in surfaces {
        let channel = surface_status_channel(prefix, &surface.slug);
        publish_stamp(
            messenger,
            &surface.slug,
            &channel,
            None,
            epoch,
            "server restart",
            Oversize::Fatal,
        )
        .await;
    }
}

/// Publish the surface's terminal `disconnected` stamp.
///
/// Written by the server rather than the departing attacher, because a page that
/// is gone cannot report that it is: the retained status document itself says the
/// surface is down, with no timestamp math on the reader's side. The route calls
/// this only when the attachment that just ended was the surface's last one —
/// decided atomically by the session's own unregistration — so a departing device
/// never overwrites a live sibling's health.
///
/// A new attachment that registers and publishes its first status between that
/// unregistration and this publish (a reload closing the old socket as the new
/// page connects) can be transiently overwritten; the new attachment's next
/// status tick corrects it within one `status_interval_secs`, which is the same
/// staleness bound the retained-status model already relies on.
///
/// # Panics
///
/// On any outcome but `Ok` and an oversized body; see [`publish_stamp`].
pub async fn publish_terminal_disconnected_stamp(runtime: &SurfaceRuntime, session_id: Uuid) {
    let messenger = runtime.messenger();
    let session = session_id.simple().to_string();
    publish_stamp(
        messenger,
        &runtime.resolved.slug,
        &runtime.description.status_channel,
        Some(&session),
        messenger.ring_epoch(),
        "session closed",
        Oversize::DropAndLog,
    )
    .await;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use brenn_messaging::testutils::ephemeral_channel_entry;
    use brenn_surface_schema::telemetry::Health;

    use crate::test_fixtures::{
        EPH_NAME, brenn_channel_entry, declare_channels, deskbar_loop, fixture_messenger,
    };

    /// A `Messenger` whose body cap is `max_body_bytes` and whose directory
    /// carries the `deskbar` fixture's derived status channel, with the substrate
    /// telemetry grant injected as boot injects it. Everything the stamp path
    /// reads, and nothing else — the channel is declared and authorized, so the
    /// only thing left to refuse the publish is the cap.
    async fn stamp_messenger(
        db: &brenn_db::Db,
        max_body_bytes: usize,
    ) -> (Arc<Messenger>, String, uuid::Uuid) {
        let params = crate::fixtures_config::description_params();
        let mut surfaces = vec![deskbar_loop(vec![])];
        crate::boot_policy::inject_surface_geometry_status_grants(&mut surfaces, &params.prefix);
        let status_uuid = uuid::Uuid::new_v4();
        let entries = vec![
            ephemeral_channel_entry(EPH_NAME, 4),
            brenn_channel_entry(
                &crate::description::surface_status_bare(&params.prefix, "deskbar"),
                status_uuid,
            ),
        ];
        crate::fixtures_config::derive_wire_subscriptions(&mut surfaces[0]);
        crate::fixtures_config::bind_wire_subscription_uuids(&mut surfaces[0], &entries);
        let stores = declare_channels(db, &entries).await;
        // Wake routing is the composition root's concern a crate above; the stamp
        // path publishes and never wakes anyone.
        let router = Arc::new(brenn_messaging::query::NoopWakeRouter)
            as Arc<dyn brenn_messaging::WakeRouter>;
        let messenger =
            fixture_messenger(db, &entries, &surfaces[0], stores, router, max_body_bytes);
        (
            messenger,
            surface_status_channel(&params.prefix, "deskbar"),
            status_uuid,
        )
    }

    /// How many rows the status channel persisted.
    async fn rows_on(db: &brenn_db::Db, channel_uuid: uuid::Uuid) -> i64 {
        let conn = db.lock().await;
        conn.query_row(
            "SELECT COUNT(*) FROM messaging_messages WHERE channel_uuid = ?1",
            rusqlite::params![channel_uuid.as_bytes().to_vec()],
            |row| row.get(0),
        )
        .unwrap()
    }

    /// A cap no composed stamp can fit under: the body carries a session id, a
    /// timestamp, an epoch uuid, and a reason.
    const CAP_BELOW_ANY_STAMP: usize = 16;

    /// **The terminal flavour drops an oversized stamp.** The socket is already
    /// closing, so a late-discovered cap misconfiguration costs one latest-wins
    /// document rather than the process. The row count is what says the publish
    /// was refused rather than quietly fitting under the cap.
    #[tokio::test]
    async fn an_oversized_terminal_stamp_is_dropped_rather_than_fatal() {
        let db = brenn_messaging_store::db::init_db_memory();
        let (messenger, channel, status_uuid) = stamp_messenger(&db, CAP_BELOW_ANY_STAMP).await;
        publish_stamp(
            &messenger,
            "deskbar",
            &channel,
            Some("sess"),
            uuid::Uuid::nil(),
            "session closed",
            Oversize::DropAndLog,
        )
        .await;
        assert_eq!(
            rows_on(&db, status_uuid).await,
            0,
            "the oversized stamp was dropped, so nothing landed"
        );
    }

    /// The same publish under a cap the body fits: the drop arm is not the only
    /// thing this rig can reach, so the test above is measuring the cap rather
    /// than a rig that could never publish at all.
    #[tokio::test]
    async fn a_terminal_stamp_that_fits_the_cap_lands() {
        let db = brenn_messaging_store::db::init_db_memory();
        let (messenger, channel, status_uuid) = stamp_messenger(&db, 65_536).await;
        publish_stamp(
            &messenger,
            "deskbar",
            &channel,
            Some("sess"),
            uuid::Uuid::nil(),
            "session closed",
            Oversize::DropAndLog,
        )
        .await;
        assert_eq!(rows_on(&db, status_uuid).await, 1);
    }

    /// **The boot flavour dies on the same input.** A cap that refuses the stamp
    /// is a config error found while starting, and starting anyway would leave
    /// every reader on a stale retained value from before the restart.
    #[tokio::test]
    #[should_panic(expected = "did not succeed")]
    async fn an_oversized_boot_stamp_refuses_to_start() {
        let db = brenn_messaging_store::db::init_db_memory();
        let (messenger, channel, _status_uuid) = stamp_messenger(&db, CAP_BELOW_ANY_STAMP).await;
        publish_stamp(
            &messenger,
            "deskbar",
            &channel,
            None,
            uuid::Uuid::nil(),
            "server restart",
            Oversize::Fatal,
        )
        .await;
    }

    /// Both stamp flavours: the boot stamp names no session, the terminal one
    /// names the session that closed. Each carries the bus epoch a reader
    /// compares against a live document's.
    #[test]
    fn disconnected_body_covers_both_stamp_flavours() {
        let boot = DisconnectedStamp::parse(&disconnected_body(
            None,
            uuid::Uuid::nil(),
            "server restart",
        ))
        .expect("the boot stamp is valid");
        assert_eq!(boot.session, None);
        assert_eq!(boot.health, Health::Disconnected);
        assert_eq!(boot.reason, "server restart");
        assert_eq!(boot.epoch, uuid::Uuid::nil());

        let terminal = DisconnectedStamp::parse(&disconnected_body(
            Some("sess"),
            uuid::Uuid::nil(),
            "session closed",
        ))
        .expect("the terminal stamp is valid");
        assert_eq!(terminal.session.as_deref(), Some("sess"));
        assert_eq!(terminal.reason, "session closed");
    }

    /// A stamp with no reason is one every reader refuses, so the composer
    /// refuses to publish it instead of leaving the channel's latest-wins row
    /// unreadable.
    #[test]
    #[should_panic(expected = "disconnected stamp composed from server-held facts")]
    fn a_reasonless_stamp_does_not_compose() {
        disconnected_body(Some("sess"), uuid::Uuid::nil(), "");
    }
}
