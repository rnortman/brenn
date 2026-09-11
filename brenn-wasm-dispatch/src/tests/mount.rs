//! The mount activation: the one every consumer is owed when it is put into
//! service.
//!
//! A backend mount is the consumer task starting — at boot, at a reload that
//! arrives or replaces the consumer, and after a process restart. Its positions
//! exist before the task does, so the debt is `Owed` from the task's first
//! instruction and `Settled` for every step after.
//!
//! These tests count activations off the `processor-multiport` fixture's output
//! channel: it publishes exactly one summary per activation, whatever its
//! windows hold, so an activation over empty ports is as visible as one carrying
//! a batch.

use super::*;

use std::time::Duration;

// The deadline a positive assertion waits under and the quiet period a negative
// one buys confidence with. Both are the host-conformance suite's, which polls
// the same question on the same runner: one pair of knobs, retuned in one place.
use brenn_host_conformance::{QUIET_PERIOD, WAIT_TIMEOUT as SETTLE_DEADLINE};
use brenn_lib::messaging::config::Depth;
use brenn_obs::alerting::{AlertSeverity, make_capturing_alerter_with_severity};

/// How many summaries the fixture has published — one per activation.
async fn activation_count(messenger: &brenn_messaging::Messenger, out_addr: &str) -> i64 {
    let conn = messenger.db().lock().await;
    conn.query_row(
        "SELECT COUNT(*) FROM messaging_messages m \
         JOIN messaging_channels c ON c.uuid = m.channel_uuid \
         WHERE c.address = ?1",
        rusqlite::params![out_addr],
        |row| row.get(0),
    )
    .expect("count the fixture's summaries")
}

/// This subscriber's quarantine rows.
async fn failure_row_count(
    messenger: &brenn_messaging::Messenger,
    subscriber: &ParticipantId,
) -> i64 {
    let conn = messenger.db().lock().await;
    conn.query_row(
        "SELECT COUNT(*) FROM messaging_wasm_consume_failures WHERE subscriber = ?1",
        rusqlite::params![subscriber.as_str()],
        |row| row.get(0),
    )
    .expect("count this subscriber's failure rows")
}

/// One captured alert: severity, title, body.
type CapturedAlert = (AlertSeverity, String, String);

/// The alerts raised so far, polled: alert dispatch is asynchronous, so the
/// capture lags the drain step that raised it.
async fn await_alerts(
    captured: &std::sync::Arc<std::sync::Mutex<Vec<CapturedAlert>>>,
) -> Vec<CapturedAlert> {
    let start = std::time::Instant::now();
    loop {
        let seen = captured.lock().unwrap().clone();
        if !seen.is_empty() || start.elapsed() >= SETTLE_DEADLINE {
            return seen;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Poll until the fixture has published `want` summaries, or give up.
async fn await_activations(
    messenger: &brenn_messaging::Messenger,
    out_addr: &str,
    want: i64,
) -> i64 {
    let start = std::time::Instant::now();
    loop {
        let seen = activation_count(messenger, out_addr).await;
        if seen >= want || start.elapsed() >= SETTLE_DEADLINE {
            return seen;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// The bare guarantee: a consumer started over a channel that has never carried
/// anything still runs, exactly once.
#[tokio::test]
async fn a_consumer_task_activates_once_over_an_empty_channel() {
    let slug = "mount-empty";
    let (messenger, _in_entries, out_entry, _out_sub, _wasm_sub, cfg, _alerts, _db) =
        build_multiport_setup_with_depths(
            slug,
            &[("mount-empty-ch", Depth::Bounded(4), Depth::Bounded(4))],
        )
        .await;

    let handle = spawn_wasm_consumer_task(cfg);
    assert_eq!(
        await_activations(&messenger, &out_entry.address, 1).await,
        1,
        "the mount activation is owed unconditionally"
    );

    tokio::time::sleep(QUIET_PERIOD).await;
    assert_eq!(
        activation_count(&messenger, &out_entry.address).await,
        1,
        "the debt is settled at the mount; no second empty activation follows"
    );

    let summary: serde_json::Value = read_latest(&messenger, &out_entry.address)
        .await
        .expect("the mount activation published its summary");
    let ports = summary.as_array().expect("the summary is a port array");
    assert_eq!(ports.len(), 1, "every bound port is present in the mount");
    assert_eq!(ports[0]["len"], 0, "an empty channel windows nothing");
    assert_eq!(ports[0]["new_from"], 0);

    handle.stop_and_join().await;
}

/// A process restart is a new mount, exactly as a page reload is on the surface:
/// the debt is per mount, not per component, and stopping and starting the task
/// over the same positions incurs it again.
#[tokio::test]
async fn a_restart_is_a_new_mount() {
    let slug = "mount-restart";
    let (messenger, _in_entries, out_entry, _out_sub, _wasm_sub, cfg, _alerts, _db) =
        build_multiport_setup_with_depths(
            slug,
            &[("mount-restart-ch", Depth::Bounded(4), Depth::Bounded(4))],
        )
        .await;

    // A second config over the same component, messenger and ports: what a
    // restart gives a consumer, since the config is moved into its task.
    let second = cfg.clone();
    let handle = spawn_wasm_consumer_task(cfg);
    assert_eq!(
        await_activations(&messenger, &out_entry.address, 1).await,
        1
    );
    handle.stop_and_join().await;

    let handle = spawn_wasm_consumer_task(second);
    assert_eq!(
        await_activations(&messenger, &out_entry.address, 2).await,
        2,
        "the restarted task is a fresh mount and is owed its own activation"
    );
    handle.stop_and_join().await;
}

/// A consumer whose only input is sampled (`push_depth = 0`).
///
/// A sampled port holds no position, so no publish onto its channel can ever
/// wake its owner. The mount activation is the whole of such a consumer's
/// scheduling — it is where a self-tick chain is armed — and traffic on the
/// sampled channel changes nothing.
#[tokio::test]
async fn a_sampled_only_consumer_activates_at_mount_and_not_on_traffic() {
    let slug = "mount-sampled";
    let (messenger, in_entries, out_entry, _out_sub, _wasm_sub, cfg, _alerts, _db) =
        build_multiport_setup_with_depths(
            slug,
            &[("mount-sampled-ch", Depth::Bounded(0), Depth::Bounded(4))],
        )
        .await;
    let notify = Arc::clone(&cfg.notify);

    let handle = spawn_wasm_consumer_task(cfg);
    assert_eq!(
        await_activations(&messenger, &out_entry.address, 1).await,
        1,
        "a sampled-only consumer is still owed its mount activation"
    );

    testutils::insert_bus_message(&messenger, &in_entries[0], "ignored", ChannelScheme::Brenn)
        .await;
    notify.notify_one();
    tokio::time::sleep(QUIET_PERIOD).await;
    assert_eq!(
        activation_count(&messenger, &out_entry.address).await,
        1,
        "a sampled port never activates its owner"
    );

    handle.stop_and_join().await;
}

/// A mount activation carries what its ports hold, which on a sampled port is
/// context. The window is served and the position is not moved, because a
/// sampled port has none.
#[tokio::test]
async fn a_mount_serves_a_sampled_port_its_context() {
    let slug = "mount-context";
    let (messenger, in_entries, out_entry, _out_sub, wasm_sub, cfg, _alerts, _db) =
        build_multiport_setup_with_depths(
            slug,
            &[("mount-context-ch", Depth::Bounded(0), Depth::Bounded(4))],
        )
        .await;

    testutils::insert_bus_message(&messenger, &in_entries[0], "retained", ChannelScheme::Brenn)
        .await;

    drain_step(&cfg, &wasm_sub, MountDebt::Owed).await;

    let summary: serde_json::Value = read_latest(&messenger, &out_entry.address)
        .await
        .expect("the mount activation published its summary");
    let ports = summary.as_array().expect("the summary is a port array");
    assert_eq!(ports[0]["len"], 1, "the retained message is in the window");
    assert_eq!(
        ports[0]["new_from"], 1,
        "all of it is context: a sampled port is never delivered to"
    );
}

/// A mount activation that fails having carried nothing new: the alert fires and
/// the log names it, and no `messaging_wasm_consume_failures` row is written.
///
/// The failure table says which messages a component choked on. Nothing was
/// consumed here, so there is no message to quarantine and nothing to name — a
/// row would be an entry about no message at all, and re-delivery has nothing to
/// re-deliver.
#[tokio::test]
async fn an_empty_mount_that_errs_alerts_and_writes_no_failure_rows() {
    let slug = "mount-err";
    let (messenger, in_entries, _out_entry, _out_sub, wasm_sub, cfg, _alerts, _db) =
        build_multiport_setup_with_depths(
            slug,
            &[("mount-err-ch", Depth::Bounded(0), Depth::Bounded(4))],
        )
        .await;

    // On a sampled port this is context at every read, so the activation that
    // errs on it is one that consumed nothing.
    testutils::insert_bus_message(
        &messenger,
        &in_entries[0],
        "__err_on_context__",
        ChannelScheme::Brenn,
    )
    .await;

    let (alert_dispatcher, captured, _cap_handle) = make_capturing_alerter_with_severity();
    let cfg = WasmConsumerConfig {
        alert_dispatcher,
        ..cfg.clone()
    };

    drain_step(&cfg, &wasm_sub, MountDebt::Owed).await;

    assert_eq!(
        failure_row_count(&messenger, &wasm_sub).await,
        0,
        "an activation that consumed nothing quarantines nothing"
    );

    let alerts = await_alerts(&captured).await;
    assert_eq!(
        alerts.len(),
        1,
        "the failure is still surfaced, got {alerts:?}"
    );
    assert!(matches!(alerts[0].0, AlertSeverity::Warning));
    assert!(
        alerts[0].1.contains("mount"),
        "the alert is the whole record of a mount that died with nothing to \
         quarantine, so it has to say which kind of activation died: {:?}",
        alerts[0].1,
    );
}

/// The other half of the pair: an ordinary delivery that errs names itself a
/// delivery, and does write its quarantine rows.
#[tokio::test]
async fn a_delivery_that_errs_names_itself_a_delivery_and_quarantines() {
    let slug = "delivery-err";
    let (messenger, in_entries, _out_entry, _out_sub, wasm_sub, cfg, _alerts, _db) =
        build_multiport_setup_with_depths(
            slug,
            &[("delivery-err-ch", Depth::Bounded(4), Depth::Bounded(4))],
        )
        .await;

    testutils::insert_bus_message(&messenger, &in_entries[0], "__err__", ChannelScheme::Brenn)
        .await;

    let (alert_dispatcher, captured, _cap_handle) = make_capturing_alerter_with_severity();
    let cfg = WasmConsumerConfig {
        alert_dispatcher,
        ..cfg.clone()
    };

    drain_step(&cfg, &wasm_sub, MountDebt::Settled).await;

    assert_eq!(
        failure_row_count(&messenger, &wasm_sub).await,
        1,
        "a delivery consumed the message it choked on, and says so"
    );

    let alerts = await_alerts(&captured).await;
    assert_eq!(alerts.len(), 1, "the failure is surfaced, got {alerts:?}");
    assert!(
        alerts[0].1.contains("delivery"),
        "an ordinary delivery failure names itself one: {:?}",
        alerts[0].1,
    );
}

/// The trap arm of the empty-mount disposition, which the err arm above cannot
/// speak for: it is a different branch, with its own quarantine call and its own
/// alert title.
#[tokio::test]
async fn an_empty_mount_that_traps_alerts_and_writes_no_failure_rows() {
    let slug = "mount-trap";
    let (messenger, in_entries, _out_entry, _out_sub, wasm_sub, cfg, _alerts, _db) =
        build_multiport_setup_with_depths(
            slug,
            &[("mount-trap-ch", Depth::Bounded(0), Depth::Bounded(4))],
        )
        .await;

    // Sampled, so this body is context at every read and the activation that
    // traps on it consumed nothing.
    testutils::insert_bus_message(
        &messenger,
        &in_entries[0],
        "__trap_on_context__",
        ChannelScheme::Brenn,
    )
    .await;

    let (alert_dispatcher, captured, _cap_handle) = make_capturing_alerter_with_severity();
    let cfg = WasmConsumerConfig {
        alert_dispatcher,
        ..cfg.clone()
    };

    drain_step(&cfg, &wasm_sub, MountDebt::Owed).await;

    assert_eq!(
        failure_row_count(&messenger, &wasm_sub).await,
        0,
        "a trapped activation that consumed nothing quarantines nothing"
    );

    let alerts = await_alerts(&captured).await;
    assert_eq!(
        alerts.len(),
        1,
        "the trap is still surfaced, got {alerts:?}"
    );
    assert!(matches!(alerts[0].0, AlertSeverity::Warning));
    assert!(
        alerts[0].1.contains("mount"),
        "the trap alert names the kind of activation that died: {:?}",
        alerts[0].1,
    );
}
