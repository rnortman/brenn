//! `dispatch_row` Err / Ok(false) / Ok(true) arm tests.
//!
//! These lock the eager-wake-on-failed-send behaviour (an eager row whose bridge
//! died mid-send fires `spawn_eager_wake` instead of parking silently) and the
//! wake gate. Each constructs a `PendingPushRow` via the single-family
//! `fake_ingress_row` helper and calls `dispatcher::dispatch_row` against a
//! `CountingRouter` configured to return the relevant arm.
//!
//! Production items (`DispatchOutcome`, `Messenger`) are reached via
//! `use super::super::*;` (directly from `publish/mod.rs`); the cross-family
//! shared `CountingRouter` fixture is declared `pub(super)` in `tests/mod.rs`
//! and pulled in by the named `use super::{…};` below. `fake_ingress_row` is
//! used only by this family, so it lives here rather than in the harness.

use super::super::*;
use super::CountingRouter;
use crate::messaging::dispatcher;
use crate::messaging::{ParticipantId, Urgency, WakeRouter};
use chrono::Utc;
use std::sync::Arc;
use std::sync::atomic::Ordering;

// -----------------------------------------------------------------------
// The Err arm: an eager row whose bridge died mid-send must wake rather than
// park silently. Without these tests, a regression that re-broke it would pass
// every other test in the suite. We construct a `PendingPushRow` directly and
// call `dispatch_row` against a `CountingRouter` configured to return `Err`,
// asserting both the outcome (Parked) and that the eager wake fired iff the row
// asks for one.
// -----------------------------------------------------------------------

/// One channel-less ingress row — the only kind `dispatch_row` still sees, since
/// what a subscriber is owed on a channel is its cursor position.
fn fake_ingress_row(push_id: i64, urgency: Urgency) -> crate::messaging::db::PendingPushRow {
    // eager_wake mirrors what the ingress insert computes: eager at Normal and up.
    let eager_wake = urgency >= Urgency::Normal;
    crate::messaging::db::PendingPushRow {
        push_id,
        message_id: push_id,
        event: crate::messaging::ingress::Event {
            id: push_id,
            conversation_id: 99,
            source: "repo_sync".into(),
            summary: "summary".into(),
            payload: "payload".into(),
            created_at: Utc::now(),
        },
        target_subscriber: ParticipantId::for_conversation(99),
        target_app_slug: "test-app".to_string(),
        eager_wake,
    }
}

#[tokio::test]
async fn dispatch_row_eager_err_fires_eager_wake() {
    let router = Arc::new(CountingRouter::default());
    // Simulate bridge-died-mid-send: the send returns Err.
    router.deliver_returns.store(2, Ordering::SeqCst);
    let row = fake_ingress_row(7, Urgency::Normal);
    let outcome = dispatcher::dispatch_row(router.as_ref() as &dyn WakeRouter, &row, false).await;
    assert_eq!(outcome, DispatchOutcome::Parked { woke: true });
    assert_eq!(
        router.eager_wakes.load(Ordering::SeqCst),
        1,
        "an eager row must eager-wake on Err"
    );
}

#[tokio::test]
async fn dispatch_row_quiet_err_does_not_eager_wake() {
    let router = Arc::new(CountingRouter::default());
    router.deliver_returns.store(2, Ordering::SeqCst);
    let row = fake_ingress_row(8, Urgency::Low);
    let outcome = dispatcher::dispatch_row(router.as_ref() as &dyn WakeRouter, &row, false).await;
    assert_eq!(outcome, DispatchOutcome::Parked { woke: false });
    assert_eq!(
        router.eager_wakes.load(Ordering::SeqCst),
        0,
        "a row that asks for no wake must not eager-wake",
    );
}

/// Sanity check the `Ok(false)` arm against the same fixture so the
/// Err vs. Ok(false) eager-wake parity is locked.
#[tokio::test]
async fn dispatch_row_eager_ok_false_fires_eager_wake() {
    let router = Arc::new(CountingRouter::default());
    // deliver_returns = 0 → Ok(false).
    let row = fake_ingress_row(9, Urgency::Normal);
    let outcome = dispatcher::dispatch_row(router.as_ref() as &dyn WakeRouter, &row, false).await;
    assert_eq!(outcome, DispatchOutcome::Parked { woke: true });
    assert_eq!(router.eager_wakes.load(Ordering::SeqCst), 1);
}

/// `Ok(true)` returns `Delivered(push_id)`.
#[tokio::test]
async fn dispatch_row_ok_true_returns_delivered() {
    let router = Arc::new(CountingRouter::default());
    router.deliver_returns.store(1, Ordering::SeqCst);
    let row = fake_ingress_row(10, Urgency::Normal);
    let outcome = dispatcher::dispatch_row(router.as_ref() as &dyn WakeRouter, &row, false).await;
    assert_eq!(outcome, DispatchOutcome::Delivered(10));
    assert_eq!(router.eager_wakes.load(Ordering::SeqCst), 0);
}

/// A parked-shaped (`wasm:`) target: the row's own eager wake is the whole of the
/// dispatch — the inline delivery path is never called for it.
#[tokio::test]
async fn dispatch_row_parked_target_wakes_without_delivering() {
    let router = Arc::new(CountingRouter::default());
    let mut row = fake_ingress_row(11, Urgency::Normal);
    row.target_subscriber = ParticipantId::for_wasm("test-slug");
    let outcome = dispatcher::dispatch_row(router.as_ref() as &dyn WakeRouter, &row, false).await;
    assert_eq!(outcome, DispatchOutcome::Parked { woke: true });
    assert_eq!(
        router.eager_wakes.load(Ordering::SeqCst),
        1,
        "a store-less ingress row must fire exactly one eager wake",
    );
}

/// A parked-shaped target on a row that asks for no wake: parks and fires
/// nothing. Locks the quiet branch of the parked gate — a regression that
/// accidentally calls `spawn_eager_wake` for it would fail here.
#[tokio::test]
async fn dispatch_row_parked_target_quiet_row_wakes_nobody() {
    let router = Arc::new(CountingRouter::default());
    let mut row = fake_ingress_row(12, Urgency::Low);
    row.target_subscriber = ParticipantId::for_wasm("test-slug");
    let outcome = dispatcher::dispatch_row(router.as_ref() as &dyn WakeRouter, &row, false).await;
    assert_eq!(outcome, DispatchOutcome::Parked { woke: false });
    assert_eq!(
        router.eager_wakes.load(Ordering::SeqCst),
        0,
        "a quiet row on a parked target must NOT fire an eager wake",
    );
}

// -----------------------------------------------------------------------
// Wake gate: `wake_gated=true` — the subscriber's own urgency economics say
// this row buys no subprocess — suppresses the eager wake but never the
// delivery attempt.
// -----------------------------------------------------------------------

/// Eager row + `wake_gated=true` + no active bridge (Ok(false)): the gate
/// suppresses the eager wake — `Parked { woke: false }` and `eager_wakes == 0`.
#[tokio::test]
async fn dispatch_row_wake_gated_suppresses_eager_wake() {
    let router = Arc::new(CountingRouter::default());
    // deliver_returns = 0 → Ok(false) (no active bridge).
    let row = fake_ingress_row(30, Urgency::Normal);
    let outcome = dispatcher::dispatch_row(router.as_ref() as &dyn WakeRouter, &row, true).await;
    assert_eq!(outcome, DispatchOutcome::Parked { woke: false });
    assert_eq!(
        router.eager_wakes.load(Ordering::SeqCst),
        0,
        "wake_gated must suppress the eager wake on the Ok(false) branch",
    );
}

/// Eager row on a parked-shaped target + `wake_gated=true`: the parked gate
/// honours `wake_gated` too — no eager wake fires.
#[tokio::test]
async fn dispatch_row_wake_gated_suppresses_a_parked_targets_wake() {
    let router = Arc::new(CountingRouter::default());
    let mut row = fake_ingress_row(31, Urgency::Normal);
    row.target_subscriber = ParticipantId::for_wasm("test-slug");
    let outcome = dispatcher::dispatch_row(router.as_ref() as &dyn WakeRouter, &row, true).await;
    assert_eq!(outcome, DispatchOutcome::Parked { woke: false });
    assert_eq!(
        router.eager_wakes.load(Ordering::SeqCst),
        0,
        "wake_gated must suppress the eager wake on the parked-gate branch",
    );
}

/// The gate is never a delivery gate: a gated eager row still reaches the bridge,
/// and a live one is delivered.
#[tokio::test]
async fn dispatch_row_wake_gated_still_delivers() {
    let router = Arc::new(CountingRouter::default());
    router.deliver_returns.store(1, Ordering::SeqCst);
    let row = fake_ingress_row(32, Urgency::Normal);
    let outcome = dispatcher::dispatch_row(router.as_ref() as &dyn WakeRouter, &row, true).await;
    assert_eq!(outcome, DispatchOutcome::Delivered(32));
    assert_eq!(
        router.eager_wakes.load(Ordering::SeqCst),
        0,
        "a delivered row needs no wake",
    );
}
