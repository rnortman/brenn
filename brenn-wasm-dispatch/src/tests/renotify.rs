//! Clamped-activation family.
//!
//! A port whose owed backlog exceeds the activation's push cap is served the
//! newest messages and the rest are reported as drops — nothing is held back for
//! a later drain. These tests pin that one activation clears the whole backlog,
//! that no follow-up wake is stored for it, and that the same holds under a guest
//! that traps.

use super::*;

use std::time::Duration;

// ── A clamped activation leaves nothing behind ────────────────────────────

/// A `push_depth = Bounded(1)` port with 3 owed rows serves the newest one and
/// reports the other two. Nothing is left owed, so no follow-up wake is stored:
/// there is no backlog left to chain over.
#[tokio::test]
async fn a_clamped_activation_clears_the_whole_backlog() {
    let slug = "renotify-clamp";
    let (messenger, channel, wasm_sub) = testutils::build_wasm_messenger(
        slug,
        "renotify-clamp-ch",
        Depth::Bounded(1),
        Depth::Bounded(0),
    )
    .await;

    for i in 0..3 {
        testutils::insert_bus_message(
            &messenger,
            &channel,
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
    drain_step(&cfg, &wasm_sub).await;

    assert!(
        brenn_messaging::testutils::owed_everywhere(&messenger, &wasm_sub)
            .await
            .is_empty(),
        "one activation passes the whole backlog: what did not fit was reported, not held"
    );
    let permit = tokio::time::timeout(Duration::from_millis(100), cfg.notify.notified()).await;
    assert!(
        permit.is_err(),
        "nothing is owed after the activation, so no follow-up wake is stored"
    );
}

/// The same under a guest that traps: the position advances before the guest
/// runs, so a trapping activation still leaves nothing owed — the state a
/// re-wake would find, which is what keeps a trapping guest from being re-fed
/// its own batch.
#[tokio::test]
async fn a_trapping_clamped_activation_leaves_nothing_owed() {
    let slug = "renotify-trap";
    let (messenger, channel, wasm_sub) = testutils::build_wasm_messenger(
        slug,
        "renotify-trap-ch",
        Depth::Bounded(2),
        Depth::Bounded(0),
    )
    .await;

    for i in 0..6 {
        // The demo guest traps on the `__trap__` sentinel body.
        testutils::insert_bus_message(
            &messenger,
            &channel,
            &format!("__trap__{i}"),
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
    drain_step(&cfg, &wasm_sub).await;

    assert!(
        brenn_messaging::testutils::owed_everywhere(&messenger, &wasm_sub)
            .await
            .is_empty(),
        "a trapped batch is passed, not redelivered"
    );
}
