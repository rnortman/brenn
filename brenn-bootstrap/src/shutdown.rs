//! Graceful shutdown signal and session cleanup background task.

use tracing::info;

/// Shared interval for hourly background cleanup loops.
const HOURLY_INTERVAL_SECS: u64 = 3600;

/// Stagger offset for the bus GC loop (45 min after session_cleanup_loop,
/// 15 min before ingress_cleanup_loop), so the three hourly loops never
/// grab the SQLite mutex simultaneously.
const BUS_GC_STAGGER_SECS: u64 = 45 * 60;

use brenn_server::active_bridge::ActiveBridges;
use brenn_server::mqtt_connection::MqttConnectionHub;

/// Handles [`serve_until_shutdown`] acts on at the shutdown signal and after
/// the server returns. All three are cheap `Clone` (Arc-backed). `mqtt` is the
/// live client registry: every session in it, and every one a reload has let go
/// of and is still stopping, is signalled at the signal and joined before the
/// function returns, so each egress reader has reported what it had in flight
/// and each supervisor has sent its DISCONNECT before the process exits. A
/// client a reload adds after the signal is born stopped. `mqtt_connection`,
/// `None` when messaging is off, writes `stopped` for each client it still
/// holds open once every session is joined.
pub(crate) struct ShutdownHandle {
    pub(crate) active_bridges: ActiveBridges,
    pub(crate) server_shutting_down: std::sync::Arc<std::sync::atomic::AtomicBool>,
    pub(crate) mqtt: std::sync::Arc<brenn_mqtt::MqttService>,
    pub(crate) mqtt_connection: Option<std::sync::Arc<MqttConnectionHub>>,
}

/// Resolve at the first SIGINT or SIGTERM.
pub(crate) async fn os_shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to install SIGTERM handler");

    tokio::select! {
        result = ctrl_c => {
            result.expect("failed to listen for SIGINT");
            info!("received SIGINT");
        }
        _ = sigterm.recv() => { info!("received SIGTERM"); }
    }
}

/// Serve `app` on `listener` until `signal` resolves, then shut down: mark the
/// process shutting down, signal every MQTT session to stop, let `axum` drain
/// its in-flight requests, and join every MQTT session and write `stopped` for
/// each client the connection hub still holds open before returning.
///
/// The sessions' stops start at the signal and run beside the HTTP drain, so
/// the whole takes the longer of the two, not their sum.
pub(crate) async fn serve_until_shutdown(
    listener: tokio::net::TcpListener,
    app: axum::Router,
    handle: ShutdownHandle,
    signal: impl std::future::Future<Output = ()> + Send + 'static,
) {
    let mqtt = handle.mqtt.clone();
    let mqtt_connection = handle.mqtt_connection.clone();
    // TODO(surface-small-row-delivery-latency): small binary rows round-trip a
    // surface page's websocket about 20 ms slower than megabyte rows; the
    // listener's sockets keep Nagle's algorithm on.
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        signal.await;
        begin_shutdown(handle).await;
    })
    .await
    .expect("server error");

    // TODO(shutdown-does-not-quiesce-reload): the reload driver is not stopped
    // first, so any bus write a reload's commit is still making when this
    // returns is cut off by process exit.
    join_mqtt(&mqtt, mqtt_connection.as_deref()).await;
}

/// Join every MQTT session, then publish each open client's `stopped`
/// connection state.
///
/// `join_all` returns only once every egress reader has exited, after its
/// last report, so nothing is handed after a `stopped` is written; a
/// `stopped` is what tells the next boot this process ended cleanly.
///
/// # Panics
///
/// If `mqtt.stop_all()` has not been called.
pub(crate) async fn join_mqtt(
    mqtt: &brenn_mqtt::MqttService,
    connection: Option<&MqttConnectionHub>,
) {
    let joined = mqtt.join_all().await;
    if joined > 0 {
        info!(count = joined, "joined MQTT sessions");
    }
    if let Some(connection) = connection {
        connection.finish_all().await;
    }
}

/// What the shutdown signal sets off before `axum`'s graceful shutdown begins.
async fn begin_shutdown(handle: ShutdownHandle) {
    // Mark the process as shutting down BEFORE letting axum begin its
    // graceful shutdown. Two effects:
    //   1. Flip the global flag so each bridge's `SessionEvent::Died` arm
    //      skips its Warning alert (see `cc_event_loop` in
    //      `active_bridge.rs`).
    //   2. Walk every live bridge and call `mark_shutting_down()` on its
    //      `CcSession` so the reader task suppresses its Critical EOF alert
    //      (see `spawn_stdout_reader` in `brenn-cc`).
    // Order matters: global flag first, so any Died event that races our
    // per-session walk already sees the flag.
    handle
        .server_shutting_down
        .store(true, std::sync::atomic::Ordering::SeqCst);
    handle
        .active_bridges
        .mark_all_sessions_shutting_down()
        .await;

    // Start every MQTT session's stop: each egress reader waits, up to its
    // client's `stop_wait_secs`, for the outcomes of what it has in flight,
    // and each supervisor disconnects once its reader has exited.
    // `serve_until_shutdown` joins them after the HTTP drain.
    let signalled = handle.mqtt.stop_all();
    if signalled > 0 {
        info!(count = signalled, "signalled MQTT sessions to stop");
    }

    info!("marked active bridges shutting down, yielding to graceful shutdown");
}

/// Background task: clean up expired sessions once per hour.
pub(crate) async fn session_cleanup_loop(db: brenn_db::Db) {
    use brenn_db::auth::session::cleanup_expired_sessions;

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(HOURLY_INTERVAL_SECS)).await;
        let conn = db.lock().await;
        let removed = cleanup_expired_sessions(&conn);
        if removed > 0 {
            tracing::info!(count = removed, "cleaned up expired sessions");
        }
    }
}

/// Background task: mark stale undelivered repo-sync ingress rows as delivered
/// once per hour, so abandoned conversations don't accumulate orphaned ingress
/// rows that `split_stale_repo_sync` (drain-time only) would never see. Also
/// deletes delivered ingress rows older than `retention_days` from the unified
/// `messaging_pending_pushes` / `messaging_messages` store.
///
/// Offset by 30 minutes relative to `session_cleanup_loop` so both loops
/// don't grab the DB lock simultaneously.
///
/// The `retention_days` parameter is read from `[events].delivered_retention_days`
/// in the operator config.
pub(crate) async fn ingress_cleanup_loop(db: brenn_db::Db, retention_days: u64) {
    use brenn_messaging::{assert_delivered_retention_days_valid, repo_sync_staleness_days};
    use brenn_messaging_store::db as msg_db;

    // Guard against operator values that would overflow i64 arithmetic when
    // computing the cutoff. Fires at spawn time (before any sleep) so a bad
    // config panics immediately with a clear message. Guard lives here so it
    // travels with the value regardless of where the loop is spawned from.
    assert_delivered_retention_days_valid(retention_days);

    // Stagger startup so this loop fires at a different phase than session_cleanup_loop.
    tokio::time::sleep(std::time::Duration::from_secs(HOURLY_INTERVAL_SECS / 2)).await;

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(HOURLY_INTERVAL_SECS)).await;
        let conn = db.lock().await;
        let staleness_days = repo_sync_staleness_days();
        let marked = msg_db::mark_stale_undelivered_ingress_repo_sync(&conn, staleness_days);
        if marked > 0 {
            tracing::info!(
                count = marked,
                staleness_days,
                "marked stale undelivered repo-sync ingress rows as delivered"
            );
        }
        let cutoff = chrono::Utc::now() - chrono::Duration::days(retention_days as i64);
        let (pushes_deleted, messages_deleted) =
            msg_db::delete_delivered_ingress_pushes_before(&conn, cutoff);
        if pushes_deleted > 0 || messages_deleted > 0 {
            tracing::info!(
                pushes_deleted,
                messages_deleted,
                retention_days,
                "deleted delivered ingress rows older than retention window"
            );
        }
    }
}

// TODO(blob-budget-rule-sized-by-window): the blob-pool sizing rule assumes a
// row's blobs go when the row leaves its channel's window; a durable row and
// its blobs go at this loop's next pass.
/// Background task: GC bus (channel-associated) messages past the channel reap
/// frontier.
///
/// Runs once per hour. Staggered by 45 minutes relative to
/// `session_cleanup_loop` so the three hourly loops never contend the single
/// SQLite mutex simultaneously. The stagger is a performance optimization —
/// correctness does not depend on it.
///
/// **Two-reaper non-overlap invariant:** `bus_gc_evict_channel` carries an
/// `envelope_type != 'ingress'` predicate, which matches all channel-associated
/// rows (`brenn`, `webhook`, future transports). `ingress_cleanup_loop` touches
/// only `channel_uuid IS NULL` (`envelope_type = 'ingress'`) rows. The two passes
/// are non-overlapping because `ingress` rows never have a `channel_uuid`, so
/// they are excluded by the channel-uuid join on the GC path.
///
/// For each channel in the directory:
/// 1. Compute the reap frontier via [`brenn_lib::messaging::ChannelEntry::reap_frontier`].
///    If `None`, skip.
/// 2. For bounded frontiers: evict bodies past the frontier via
///    `bus_gc_evict_channel` (handles both drop and archive sinks).
/// 3. Enact the noise ladder over the eviction's report: retention outran a
///    subscriber's position, and this pass is the durable class's eviction
///    accounting point, the sibling of the ring's displacing append. The
///    report lands up to one GC interval after the loss — durable eviction is
///    itself periodic, and the ladder is minutes-grade.
///
/// After every channel, one [`brenn_messaging::Messenger::reap_blobs`] deletes
/// the blobs the pass's evictions left unheld.
pub(crate) async fn bus_gc_loop(
    db: brenn_db::Db,
    messenger: std::sync::Arc<brenn_messaging::Messenger>,
    archive_path: Option<std::path::PathBuf>,
) {
    // Stagger startup to avoid lock contention with the other two hourly loops.
    tokio::time::sleep(std::time::Duration::from_secs(BUS_GC_STAGGER_SECS)).await;

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(HOURLY_INTERVAL_SECS)).await;

        let pass = bus_gc_pass(&db, &messenger, archive_path.as_deref()).await;
        if pass.messages_evicted > 0 || pass.reaped.blobs > 0 {
            tracing::info!(
                messages_evicted = pass.messages_evicted,
                channels_processed = pass.channels_processed,
                blobs_reaped = pass.reaped.blobs,
                blob_bytes_reaped = pass.reaped.bytes,
                "bus GC pass complete"
            );
        }
    }
}

/// What one [`bus_gc_pass`] did.
#[derive(Debug)]
pub(crate) struct BusGcPass {
    pub(crate) messages_evicted: usize,
    pub(crate) channels_processed: usize,
    pub(crate) reaped: brenn_messaging_store::blob::ReapReport,
}

/// One pass of [`bus_gc_loop`]: evict every durable channel past its frontier,
/// enact the noise ladder over each eviction, then reap the blobs nothing
/// holds any longer.
pub(crate) async fn bus_gc_pass(
    db: &brenn_db::Db,
    messenger: &brenn_messaging::Messenger,
    archive_path: Option<&std::path::Path>,
) -> BusGcPass {
    use brenn_messaging_store::db as msg_db;

    // Durable channels only: what this pass evicts is database rows, and a
    // non-durable channel keeps its bounded window in memory, where the ring
    // itself is the bound.
    let channels = messenger.directory().list_durable();
    let mut messages_evicted: usize = 0;

    for entry in &channels {
        let Some(frontier) = entry.reap_frontier() else {
            continue; // channel is pinned (Unbounded depth present)
        };

        let eviction = {
            let conn = db.lock().await;
            msg_db::bus_gc_evict_channel(
                &conn,
                entry.uuid,
                &entry.address,
                entry.transport_type,
                frontier,
                entry.resolved_channel.sink,
                archive_path,
            )
        };
        messages_evicted += eviction.messages_evicted;
        // Enacted outside the connection hold: the ladder alarms through
        // the router, which is no business of the eviction transaction.
        messenger.enact_overflow_for_channel(&entry.address, &eviction.overflow);
    }

    // Runs after every eviction so blobs held only by evicted rows go in this
    // pass, and outside the connection hold, which the reap takes itself.
    let reaped = messenger.reap_blobs().await;

    BusGcPass {
        messages_evicted,
        channels_processed: channels.len(),
        reaped,
    }
}
