//! `drain_step_with_limits`: one activation under test-chosen fuel or memory.
//!
//! Counted off the `processor-multiport` fixture's output channel, which gets
//! one summary per activation that completes; a trapped activation publishes
//! nothing.

use super::*;

use brenn_lib::messaging::config::Depth;
use brenn_wasm::PROCESSOR_MAX_MEMORY_BYTES;

/// The production epoch deadline, in ticks: 300 ticks of 100 ms.
const EPOCH_TICKS: u64 = 300;

/// How many summaries the fixture has published.
async fn completed(messenger: &brenn_messaging::Messenger, out_addr: &str) -> i64 {
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

/// The same mount activation completes on production limits and under generous
/// overrides, and traps under a fuel or a memory override too small for it.
#[tokio::test]
async fn an_override_replaces_the_activations_fuel_or_memory() {
    let (messenger, _in_entries, out_entry, _out_sub, wasm_sub, cfg, _alerts, _db) =
        build_multiport_setup_with_depths(
            "limits",
            &[("limits-ch", Depth::Bounded(4), Depth::Bounded(4))],
        )
        .await;
    let out = out_entry.address.as_str();

    drain_step(&cfg, &wasm_sub, MountDebt::Owed).await;
    assert_eq!(completed(&messenger, out).await, 1, "production limits");

    let starved_fuel = ActivationLimits::Fuel {
        fuel: 1,
        epoch_ticks: EPOCH_TICKS,
    };
    drain_step_with_limits(&cfg, &wasm_sub, MountDebt::Owed, starved_fuel).await;
    assert_eq!(
        completed(&messenger, out).await,
        1,
        "one unit of fuel traps the activation"
    );

    drain_step_with_limits(
        &cfg,
        &wasm_sub,
        MountDebt::Owed,
        ActivationLimits::Memory(1),
    )
    .await;
    assert_eq!(
        completed(&messenger, out).await,
        1,
        "a one-byte memory limit traps the activation"
    );

    let ample_fuel = ActivationLimits::Fuel {
        fuel: 1_000_000_000,
        epoch_ticks: EPOCH_TICKS,
    };
    drain_step_with_limits(&cfg, &wasm_sub, MountDebt::Owed, ample_fuel).await;
    assert_eq!(completed(&messenger, out).await, 2, "ample fuel completes");

    let ample_memory = ActivationLimits::Memory(PROCESSOR_MAX_MEMORY_BYTES);
    drain_step_with_limits(&cfg, &wasm_sub, MountDebt::Owed, ample_memory).await;
    assert_eq!(
        completed(&messenger, out).await,
        3,
        "the production memory limit, given as an override, completes"
    );
}

/// A share of the production grant: the whole of it completes, a share too
/// small to instantiate traps, and the whole of it then completes again.
#[tokio::test]
async fn a_fuel_share_scales_the_production_grant() {
    let (messenger, _in_entries, out_entry, _out_sub, wasm_sub, cfg, _alerts, _db) =
        build_multiport_setup_with_depths(
            "share",
            &[("share-ch", Depth::Bounded(4), Depth::Bounded(4))],
        )
        .await;
    let out = out_entry.address.as_str();

    let whole = ActivationLimits::FuelShare {
        numerator: 1,
        denominator: 1,
    };
    drain_step_with_limits(&cfg, &wasm_sub, MountDebt::Owed, whole).await;
    assert_eq!(
        completed(&messenger, out).await,
        1,
        "the whole production grant completes"
    );

    let sliver = ActivationLimits::FuelShare {
        numerator: 1,
        denominator: 1_000_000_000,
    };
    drain_step_with_limits(&cfg, &wasm_sub, MountDebt::Owed, sliver).await;
    assert_eq!(
        completed(&messenger, out).await,
        1,
        "a billionth of the production grant traps the activation"
    );

    drain_step_with_limits(&cfg, &wasm_sub, MountDebt::Owed, whole).await;
    assert_eq!(
        completed(&messenger, out).await,
        2,
        "the whole grant completes what the sliver trapped on"
    );
}
