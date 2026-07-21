//! Single-scan / ordering / channel-visit / empty-skip family (AC 3/6/7).

use super::*;

// ── Single scan per drain step regardless of K (AC 7) ─────────────────────

/// With K subscribed channels each holding a pending row, a single drain_step
/// must perform exactly ONE pending-bus scan (not one per channel) and deliver
/// every row.
#[tokio::test]
async fn single_scan_per_drain_step_regardless_of_k() {
    let slug = "single-scan-k";
    let (messenger, channels, wasm_sub, cfg, _handle, _db) =
        build_multi_channel_setup(slug, &["scan-ch-a", "scan-ch-b", "scan-ch-c", "scan-ch-d"])
            .await;

    // Insert one pending row on each channel.
    for channel in &channels {
        testutils::insert_wasm_push(&messenger, channel, &wasm_sub, "row", ChannelScheme::Brenn)
            .await;
    }

    // Snapshot the counter immediately before the drain step.
    let before = messenger.pending_bus_pushes_scan_count();
    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;
    let after = messenger.pending_bus_pushes_scan_count();

    assert_eq!(
        after - before,
        1,
        "exactly one scan per drain step regardless of K={} channels; counter delta={}",
        channels.len(),
        after - before,
    );

    // All rows must be delivered (correctness check).
    let rows = messenger.load_pending_pushes(&wasm_sub).await;
    assert!(rows.is_empty(), "all rows must be delivered after drain");
}

// ── Order preservation within a channel (AC 3) ───────────────────────────

/// Insert N messages on each of K channels in interleaved publish_ts_ns order
/// (row for ch-a, row for ch-b, row for ch-c, row for ch-a, …). Partition and
/// assert that each channel's partition is in ascending push_id order (= scan
/// order = publish_ts_ns ASC). This directly catches a bug where partitioning
/// reverses or scrambles per-channel order.
///
/// Also asserts: drain delivers all rows, scan counter advances by exactly 1.
#[tokio::test]
async fn order_preserving_partition_delivers_all_rows() {
    let slug = "order-partition";
    let (messenger, channels, wasm_sub, cfg, _handle, _db) =
        build_multi_channel_setup(slug, &["ord-ch-a", "ord-ch-b", "ord-ch-c"]).await;

    // Insert 3 rows per channel in interleaved order. Record insertion order
    // per channel (push_id, channel_address) so we can assert partition ordering.
    let mut expected_order: std::collections::HashMap<String, Vec<i64>> =
        std::collections::HashMap::new();
    for i in 0..3usize {
        for channel in &channels {
            let (push_id, _) = testutils::insert_wasm_push(
                &messenger,
                channel,
                &wasm_sub,
                &format!("msg-{i}"),
                ChannelScheme::Brenn,
            )
            .await;
            expected_order
                .entry(channel.address.clone())
                .or_default()
                .push(push_id);
        }
    }

    // Before drain: assert load_activation_snapshot assembles per-channel new_rows
    // in ascending push_id order (= scan order = publish_ts_ns ASC, id ASC).
    // This directly pins AC 3 "within a channel" without relying on the demo guest.
    let pre_scan_before = messenger.pending_bus_pushes_scan_count();
    let pre_snapshots = messenger
        .load_activation_snapshot(&wasm_sub, &cfg.inputs)
        .await
        .expect("expected Some — all channels have pending rows");
    let pre_scan_after = messenger.pending_bus_pushes_scan_count();
    assert_eq!(
        pre_scan_after - pre_scan_before,
        1,
        "pre-drain load_activation_snapshot must use exactly one scan"
    );
    for snap in &pre_snapshots {
        let got: Vec<i64> = snap.new_rows.iter().map(|(id, _)| *id).collect();
        let expected = expected_order
            .get(&snap.channel_address)
            .cloned()
            .unwrap_or_default();
        assert_eq!(
            got, expected,
            "channel {} new_rows must be in insertion (scan) order; got={got:?} expected={expected:?}",
            snap.channel_address
        );
    }

    let before = messenger.pending_bus_pushes_scan_count();
    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;
    let after = messenger.pending_bus_pushes_scan_count();

    // +1 from the pre-drain load_activation_snapshot call above + 1 from drain = 2 total.
    assert_eq!(after - before, 1, "drain itself must use exactly one scan");

    let rows = messenger.load_pending_pushes(&wasm_sub).await;
    assert!(
        rows.is_empty(),
        "all rows across channels must be delivered"
    );
}

// ── All subscribed channels with pending rows are visited (AC 3 across) ───

/// All three subscribed channels have pending rows; drain_step must deliver
/// all of them in ONE activation (AC 3 across-channel: every triggering port
/// appears in the activation; none is silently skipped).
///
/// Under multi-port, all 3 channels' rows arrive in one activation window.
/// The demo guest iterates all ports, processes new envelopes, and returns Ok.
/// Across-channel order within the activation follows cfg.inputs order.
#[tokio::test]
async fn all_channels_with_pending_rows_are_visited() {
    let slug = "visit-order";
    let (messenger, channels, wasm_sub, cfg, _handle, _db) =
        build_multi_channel_setup(slug, &["vis-ch-a", "vis-ch-b", "vis-ch-c"]).await;

    for channel in &channels {
        testutils::insert_wasm_push(&messenger, channel, &wasm_sub, "body", ChannelScheme::Brenn)
            .await;
    }

    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;

    let rows = messenger.load_pending_pushes(&wasm_sub).await;
    assert!(rows.is_empty(), "all channels visited and rows delivered");
}

// ── Empty partition skip (AC 6) ──────────────────────────────────────────

/// With K channels but only one having pending rows, drain_step must deliver
/// that channel's rows in one activation. The other channels appear as
/// empty context windows in the activation (no new rows, not skipped entirely).
/// Counter advances by exactly 1 (single scan).
#[tokio::test]
async fn empty_channels_skipped_no_scan_per_empty_channel() {
    let slug = "empty-skip";
    let (messenger, channels, wasm_sub, cfg, _handle, _db) =
        build_multi_channel_setup(slug, &["empty-ch-a", "empty-ch-b", "empty-ch-c"]).await;

    // Insert a row only on the first channel.
    testutils::insert_wasm_push(
        &messenger,
        &channels[0],
        &wasm_sub,
        "only-row",
        ChannelScheme::Brenn,
    )
    .await;

    let before = messenger.pending_bus_pushes_scan_count();
    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;
    let after = messenger.pending_bus_pushes_scan_count();

    // Exactly one scan, regardless of empty channels.
    assert_eq!(after - before, 1, "single scan even with empty channels");

    // The row is delivered; empty channels leave no spurious failure records.
    let rows = messenger.load_pending_pushes(&wasm_sub).await;
    assert!(rows.is_empty(), "pending row must be delivered");
}
