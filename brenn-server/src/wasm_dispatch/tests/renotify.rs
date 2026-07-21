//! Clamp self-renotify family.
//!
//! When a port's pending backlog exceeds this activation's cap, `drain_step`
//! stores a notify permit so the loop drains the leftover without waiting for
//! an external wake. These tests pin: (a) the permit is stored exactly when the
//! backlog is clamped and not otherwise, and (b) the full consumer task drains
//! a clamped backlog to empty with no external wakes, including under a guest
//! that traps every activation (termination via ack-before-guest).

use super::*;

use std::time::Duration;

// `wait_pending_empty` is shared from the test module root (`tests/mod.rs`).

// ── Permit stored when the activation clamps ──────────────────────────────

/// A `push_depth = Bounded(1)` port with 3 pending rows clamps to 1 new row.
/// After `drain_step`, a notify permit must be waiting (leftover rows to drain).
#[tokio::test]
async fn drain_step_stores_notify_permit_on_clamp() {
    let slug = "renotify-clamp";
    let (messenger, channel, wasm_sub) = testutils::build_wasm_messenger(
        slug,
        "renotify-clamp-ch",
        Depth::Bounded(1),
        Depth::Bounded(0),
    )
    .await;

    for i in 0..3 {
        testutils::insert_wasm_push(
            &messenger,
            &channel,
            &wasm_sub,
            &format!("row-{i}"),
            ChannelScheme::Brenn,
        )
        .await;
    }

    let (cfg, _handle, _db) = build_cfg(
        slug,
        Arc::clone(&messenger),
        &channel,
        Depth::Bounded(1),
        Depth::Bounded(0),
    );
    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;

    // A permit stored by `notify_one` makes the next `notified()` resolve at once.
    let permit = tokio::time::timeout(Duration::from_millis(100), cfg.notify.notified()).await;
    assert!(
        permit.is_ok(),
        "clamped activation must store a self-renotify permit"
    );
}

// ── No permit when the backlog is not clamped ─────────────────────────────

/// A `push_depth = Bounded(2)` port with exactly 2 pending rows is NOT clamped
/// (`clamped_leftover == 0`). No permit must be stored — this pins the exact
/// signal against a cap-equality heuristic that would fire a spurious wake.
#[tokio::test]
async fn drain_step_stores_no_permit_when_backlog_equals_cap() {
    let slug = "renotify-noclamp";
    let (messenger, channel, wasm_sub) = testutils::build_wasm_messenger(
        slug,
        "renotify-noclamp-ch",
        Depth::Bounded(2),
        Depth::Bounded(0),
    )
    .await;

    for i in 0..2 {
        testutils::insert_wasm_push(
            &messenger,
            &channel,
            &wasm_sub,
            &format!("row-{i}"),
            ChannelScheme::Brenn,
        )
        .await;
    }

    let (cfg, _handle, _db) = build_cfg(
        slug,
        Arc::clone(&messenger),
        &channel,
        Depth::Bounded(2),
        Depth::Bounded(0),
    );
    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;

    // Backlog exactly equals the cap → no leftover → no permit → this times out.
    let permit = tokio::time::timeout(Duration::from_millis(100), cfg.notify.notified()).await;
    assert!(
        permit.is_err(),
        "an unclamped activation must not store a self-renotify permit"
    );
}

// ── End-to-end: clamped backlog drains with no external wakes ──────────────

/// `push_depth = Bounded(2)` with 5 pending rows (→ 3 activations). Spawn the
/// consumer task and send NO wake. The startup sweep plus the self-renotify
/// chain must empty the pending set on its own — the property the fix delivers:
/// leftover rows no longer wait for the next publish or a restart.
#[tokio::test]
async fn consumer_drains_clamped_backlog_without_external_wake() {
    let slug = "renotify-e2e";
    let (messenger, channel, wasm_sub) = testutils::build_wasm_messenger(
        slug,
        "renotify-e2e-ch",
        Depth::Bounded(2),
        Depth::Bounded(0),
    )
    .await;

    for i in 0..5 {
        testutils::insert_wasm_push(
            &messenger,
            &channel,
            &wasm_sub,
            &format!("row-{i}"),
            ChannelScheme::Brenn,
        )
        .await;
    }

    let (cfg, _handle, _db) = build_cfg(
        slug,
        Arc::clone(&messenger),
        &channel,
        Depth::Bounded(2),
        Depth::Bounded(0),
    );
    // Spawn the real consumer task. It runs the startup sweep then loops on
    // `notified()`. No external wake is ever sent; only self-renotify drives it.
    let _task = spawn_wasm_consumer_task(cfg);

    assert!(
        wait_pending_empty(&messenger, &wasm_sub, Duration::from_secs(5)).await,
        "self-renotify chain must empty the pending set with no external wake"
    );
}

// ── End-to-end termination under a trapping guest ─────────────────────────

/// A clamped backlog whose every activation traps must still drain to empty in
/// `ceil(N / cap)` activations and must not busy-spin. Rows are acked before the
/// guest runs, so each activation permanently removes its batch even on a trap.
/// With N=6 and cap=2 the scan counter settles at exactly 3 and stops advancing.
#[tokio::test]
async fn consumer_drains_clamped_trapping_backlog_and_terminates() {
    let slug = "renotify-trap";
    let (messenger, channel, wasm_sub) = testutils::build_wasm_messenger(
        slug,
        "renotify-trap-ch",
        Depth::Bounded(2),
        Depth::Bounded(0),
    )
    .await;

    // 6 sentinel rows: the demo guest traps on body == "__trap__" every activation.
    for _ in 0..6 {
        testutils::insert_wasm_push(
            &messenger,
            &channel,
            &wasm_sub,
            "__trap__",
            ChannelScheme::Brenn,
        )
        .await;
    }

    let (cfg, _handle, _db) = build_cfg(
        slug,
        Arc::clone(&messenger),
        &channel,
        Depth::Bounded(2),
        Depth::Bounded(0),
    );
    let _task = spawn_wasm_consumer_task(cfg);

    assert!(
        wait_pending_empty(&messenger, &wasm_sub, Duration::from_secs(5)).await,
        "trapping guest still drains the clamped backlog (ack-before-guest)"
    );

    // Termination: the scan counter must settle (no busy-spin). ceil(6/2) = 3
    // activations empty the backlog; nothing renotifies after the last one.
    let settled = messenger.pending_bus_pushes_scan_count();
    tokio::time::sleep(Duration::from_millis(150)).await;
    let after = messenger.pending_bus_pushes_scan_count();
    assert_eq!(
        settled, after,
        "consumer must not busy-spin after the backlog drains"
    );
    assert_eq!(
        after, 3,
        "exactly ceil(N/cap) = 3 activations, not one scan per leftover row"
    );
}
