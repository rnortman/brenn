//! Graceful shutdown signal and session cleanup background task.

use tracing::info;

/// Shared interval for hourly background cleanup loops.
const HOURLY_INTERVAL_SECS: u64 = 3600;

/// Stagger offset for the bus GC loop (45 min after session_cleanup_loop,
/// 15 min before ingress_cleanup_loop), so the three hourly loops never
/// grab the SQLite mutex simultaneously.
const BUS_GC_STAGGER_SECS: u64 = 45 * 60;

use crate::active_bridge::ActiveBridges;

/// Handles passed into `shutdown_signal` so it can mark live CC sessions
/// shutting-down before yielding control back to `axum::serve`'s graceful
/// shutdown. Both `active_bridges` and `server_shutting_down` are cheap
/// `Clone` (Arc-backed). `mqtt_stop_txs` is `Vec<watch::Sender<bool>>`
/// moved here so the senders are signalled on SIGTERM/SIGINT before process
/// exit, causing each supervisor to send MQTT DISCONNECT.
pub(crate) struct ShutdownHandle {
    pub(crate) active_bridges: ActiveBridges,
    pub(crate) server_shutting_down: std::sync::Arc<std::sync::atomic::AtomicBool>,
    pub(crate) mqtt_stop_txs: Vec<tokio::sync::watch::Sender<bool>>,
}

pub(crate) async fn shutdown_signal(handle: ShutdownHandle) {
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

    // Signal all MQTT supervisors to disconnect cleanly. Each supervisor will
    // send MQTT DISCONNECT and exit its event loop on the next poll cycle.
    for tx in &handle.mqtt_stop_txs {
        let _ = tx.send(true);
    }
    if !handle.mqtt_stop_txs.is_empty() {
        info!(
            count = handle.mqtt_stop_txs.len(),
            "signalled MQTT supervisors to disconnect"
        );
    }

    info!("marked active bridges shutting down, yielding to graceful shutdown");
}

/// Background task: clean up expired sessions once per hour.
pub(crate) async fn session_cleanup_loop(db: brenn_lib::db::Db) {
    use brenn_lib::auth::session::cleanup_expired_sessions;

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
/// in the operator config — the key is preserved as-is (§2.7 of the delivery
/// unification design).
pub(crate) async fn ingress_cleanup_loop(db: brenn_lib::db::Db, retention_days: u64) {
    use brenn_lib::messaging::db as msg_db;
    use brenn_lib::messaging::{assert_delivered_retention_days_valid, repo_sync_staleness_days};

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

/// Background task: GC bus (channel-associated) messages past the channel reap
/// frontier and retire stale push-claim rows for bounded subscribers.
///
/// Runs once per hour. Staggered by 45 minutes relative to
/// `session_cleanup_loop` so the three hourly loops never contend the single
/// SQLite mutex simultaneously. The stagger is a performance optimization —
/// correctness does not depend on it.
///
/// **Two-reaper non-overlap invariant:** `bus_gc_evict_channel` /
/// `bus_gc_retire_pushes` carry an `envelope_type != 'ingress'` predicate,
/// which matches all channel-associated rows (`brenn`, `webhook`, future
/// transports). `ingress_cleanup_loop` touches only `channel_uuid IS NULL`
/// (`envelope_type = 'ingress'`) rows. The two passes are non-overlapping
/// because `ingress` rows never have a `channel_uuid`, so they are excluded
/// by the channel-uuid join on the GC path.
///
/// For each channel in the directory:
/// 1. Compute the reap frontier via [`brenn_lib::messaging::ChannelEntry::reap_frontier`].
///    If `None`, skip.
/// 2. For bounded frontiers: evict bodies past the frontier via
///    `bus_gc_evict_channel` (handles both drop and archive sinks).
/// 3. Backstop push-claim retirement: retire push rows past `push_depth` for
///    each bounded-`push_depth` subscriber via `bus_gc_retire_pushes`.
pub(crate) async fn bus_gc_loop(
    db: brenn_lib::db::Db,
    directory: std::sync::Arc<brenn_lib::messaging::MessagingDirectory>,
    archive_path: Option<std::path::PathBuf>,
) {
    use brenn_lib::messaging::db as msg_db;

    // Stagger startup to avoid lock contention with the other two hourly loops.
    tokio::time::sleep(std::time::Duration::from_secs(BUS_GC_STAGGER_SECS)).await;

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(HOURLY_INTERVAL_SECS)).await;

        let channels = directory.list();
        let mut total_messages_evicted: usize = 0;
        let mut total_pushes_retired: usize = 0;

        for entry in &channels {
            let Some(frontier) = entry.reap_frontier() else {
                continue; // channel is pinned (Unbounded depth present)
            };

            // Evict bodies past the frontier.
            let (msgs, pushes) = {
                let conn = db.lock().await;
                msg_db::bus_gc_evict_channel(
                    &conn,
                    entry.uuid,
                    &entry.address,
                    entry.transport_type,
                    frontier,
                    entry.resolved_channel.sink,
                    archive_path.as_deref(),
                )
            };
            total_messages_evicted += msgs;
            total_pushes_retired += pushes;

            // Backstop push-claim retirement for each bounded-push_depth subscriber.
            for sub in &entry.subscribers {
                use brenn_lib::messaging::config::Depth;
                if let Depth::Bounded(n) = sub.push_depth
                    && n > 0
                {
                    let conn = db.lock().await;
                    let retired =
                        msg_db::bus_gc_retire_pushes(&conn, entry.uuid, sub.kind.slug(), n);
                    total_pushes_retired += retired;
                }
            }
        }

        if total_messages_evicted > 0 || total_pushes_retired > 0 {
            tracing::info!(
                messages_evicted = total_messages_evicted,
                push_rows_retired = total_pushes_retired,
                channels_processed = channels.len(),
                "bus GC pass complete"
            );
        }
    }
}
