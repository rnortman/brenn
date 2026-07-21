//! F26: `dispatch_row` Err / Ok(false) / Ok(true) / deadline-override arm tests
//! (design §"Tests: fanned into `publish/tests/` by family":
//! `publish/tests/dispatch_row.rs`).
//!
//! These lock the F3 fix (Immediate row + bridge-died-mid-send →
//! `spawn_eager_wake` instead of silent park) and the R6 deadline override.
//! Each constructs a `PendingPushRow` via the single-family `fake_pending_row`
//! helper and calls `dispatcher::dispatch_row` against a `CountingRouter`
//! configured to return the relevant arm.
//!
//! Production items (`DispatchOutcome`, `Messenger`) are reached via
//! `use super::super::*;` (directly from `publish/mod.rs`); the cross-family
//! shared `CountingRouter` fixture is declared `pub(super)` in `tests/mod.rs`
//! and pulled in by the named `use super::{…};` below. `fake_pending_row` is
//! used only by this family, so per design §"Tests: fanned…" it lives here
//! rather than in the harness.

use super::super::*;
use super::CountingRouter;
use crate::messaging::dispatcher;
use crate::messaging::{ParticipantId, Urgency, WakeRouter, canonical_address};
use chrono::Utc;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use uuid::Uuid;

// -----------------------------------------------------------------------
// F26: dispatch_row Err arm (F3 fix)
//
// The Err arm is the F3 fix (Immediate row + bridge-died-mid-send →
// spawn_eager_wake instead of silent park). Without these tests, a
// regression that re-broke F3 would pass every other test in the
// suite. We construct a `PendingPushRow` directly and call
// `dispatch_row` against a `CountingRouter` configured to return
// `Err`, asserting both the outcome (Parked) and that the eager
// wake fired iff `wake_kind == Immediate`.
// -----------------------------------------------------------------------

fn fake_pending_row(push_id: i64, urgency: Urgency) -> crate::messaging::db::PendingPushRow {
    // eager_wake mirrors what insert_pushes would compute with default wake_min=Normal.
    let eager_wake = urgency >= Urgency::Normal;
    crate::messaging::db::PendingPushRow {
        push_id,
        message_id: push_id,
        payload: crate::messaging::IngressOrBus::Bus(crate::messaging::MessageEnvelope {
            message_id: Uuid::new_v4(),
            source: "src".into(),
            channel: canonical_address("test"),
            sender: "sender".into(),
            publish_ts: Utc::now(),
            body: "body".into(),
            reply_to: None,
            delivery_deadline: None,
            deliver_after: None,
            urgency,
            envelope_type: crate::messaging::ChannelScheme::Brenn,
        }),
        target_subscriber: ParticipantId::for_conversation(99),
        target_app_slug: "test-app".to_string(),
        eager_wake,
    }
}

#[tokio::test]
async fn dispatch_row_immediate_err_fires_eager_wake() {
    let router = Arc::new(CountingRouter::default());
    // Simulate bridge-died-mid-send: deliver returns Err.
    router.deliver_returns.store(2, Ordering::SeqCst);
    let row = fake_pending_row(7, Urgency::Normal);
    let outcome =
        dispatcher::dispatch_row(router.as_ref() as &dyn WakeRouter, &row, false, false).await;
    assert_eq!(outcome, DispatchOutcome::Parked { woke: true });
    assert_eq!(
        router.eager_wakes.load(Ordering::SeqCst),
        1,
        "Immediate-wake row must eager-wake on Err (F3 contract)"
    );
}

#[tokio::test]
async fn dispatch_row_none_wake_err_does_not_eager_wake() {
    let router = Arc::new(CountingRouter::default());
    router.deliver_returns.store(2, Ordering::SeqCst);
    let row = fake_pending_row(8, Urgency::Low);
    let outcome =
        dispatcher::dispatch_row(router.as_ref() as &dyn WakeRouter, &row, false, false).await;
    assert_eq!(outcome, DispatchOutcome::Parked { woke: false });
    assert_eq!(
        router.eager_wakes.load(Ordering::SeqCst),
        0,
        "None-wake rows must not eager-wake — only Immediate gets the F3 fix",
    );
}

/// Sanity check the `Ok(false)` arm against the same fixture so the
/// Err vs. Ok(false) eager-wake parity is locked.
#[tokio::test]
async fn dispatch_row_immediate_ok_false_fires_eager_wake() {
    let router = Arc::new(CountingRouter::default());
    // deliver_returns = 0 → Ok(false).
    let row = fake_pending_row(9, Urgency::Normal);
    let outcome =
        dispatcher::dispatch_row(router.as_ref() as &dyn WakeRouter, &row, false, false).await;
    assert_eq!(outcome, DispatchOutcome::Parked { woke: true });
    assert_eq!(router.eager_wakes.load(Ordering::SeqCst), 1);
}

/// `Ok(true)` returns `Delivered(push_id)`.
#[tokio::test]
async fn dispatch_row_ok_true_returns_delivered() {
    let router = Arc::new(CountingRouter::default());
    router.deliver_returns.store(1, Ordering::SeqCst);
    let row = fake_pending_row(10, Urgency::Normal);
    let outcome =
        dispatcher::dispatch_row(router.as_ref() as &dyn WakeRouter, &row, false, false).await;
    assert_eq!(outcome, DispatchOutcome::Delivered(10));
    assert_eq!(router.eager_wakes.load(Ordering::SeqCst), 0);
}

/// Wasm + `Immediate` wake: parks and fires one eager wake.
/// `CountingRouter.deliver_returns` defaults to 0 (Ok(false)/no bridge); if
/// `deliver` were called the outcome would still be `Parked`, but the
/// eager-wake count would come from the Ok(false) arm, not the Wasm gate.
/// We distinguish by checking that `eager_wakes == 1` before any deliver path
/// could run (the Wasm gate returns early).
#[tokio::test]
async fn dispatch_row_wasm_immediate_parks_and_eager_wakes() {
    let router = Arc::new(CountingRouter::default());
    let mut row = fake_pending_row(11, Urgency::Normal);
    row.target_subscriber = ParticipantId::for_wasm("test-slug");
    let outcome =
        dispatcher::dispatch_row(router.as_ref() as &dyn WakeRouter, &row, false, false).await;
    assert_eq!(outcome, DispatchOutcome::Parked { woke: true });
    assert_eq!(
        router.eager_wakes.load(Ordering::SeqCst),
        1,
        "Immediate-wake Wasm row must fire exactly one eager wake",
    );
}

/// Wasm + `None` wake: parks and does NOT fire an eager wake. Locks the
/// `None`-wake branch of the Wasm gate — a regression that accidentally
/// calls `spawn_eager_wake` for `None`-wake rows would fail here.
#[tokio::test]
async fn dispatch_row_wasm_none_parks_no_eager_wake() {
    let router = Arc::new(CountingRouter::default());
    let mut row = fake_pending_row(12, Urgency::Low);
    row.target_subscriber = ParticipantId::for_wasm("test-slug");
    let outcome =
        dispatcher::dispatch_row(router.as_ref() as &dyn WakeRouter, &row, false, false).await;
    assert_eq!(outcome, DispatchOutcome::Parked { woke: false });
    assert_eq!(
        router.eager_wakes.load(Ordering::SeqCst),
        0,
        "None-wake Wasm row must NOT fire an eager wake",
    );
}

/// `None`-wake row + `deadline_expired=true`: the deadline override must trigger
/// an unconditional eager wake even though `wake_kind == None` (R6 deadline override,
/// design §2.4). Without this test a regression removing `|| deadline_expired` from
/// the dispatch_row Ok(false)/Err branches for None-wake rows would pass all other tests.
#[tokio::test]
async fn dispatch_row_none_wake_deadline_expired_unconditional_wake() {
    let router = Arc::new(CountingRouter::default());
    // deliver_returns = 0 → Ok(false) (no active bridge).
    let row = fake_pending_row(20, Urgency::Low);
    let outcome =
        dispatcher::dispatch_row(router.as_ref() as &dyn WakeRouter, &row, true, false).await;
    assert_eq!(outcome, DispatchOutcome::Parked { woke: true });
    assert_eq!(
        router.eager_wakes.load(Ordering::SeqCst),
        1,
        "None-wake row with deadline_expired=true must unconditionally eager-wake (R6 deadline override)"
    );
}

/// `None`-wake row + `deadline_expired=true` + Err from deliver: same unconditional
/// wake on the Err arm (symmetry with the Ok(false) arm above).
#[tokio::test]
async fn dispatch_row_none_wake_deadline_expired_unconditional_wake_on_err() {
    let router = Arc::new(CountingRouter::default());
    router.deliver_returns.store(2, Ordering::SeqCst); // Err arm
    let row = fake_pending_row(21, Urgency::Low);
    let outcome =
        dispatcher::dispatch_row(router.as_ref() as &dyn WakeRouter, &row, true, false).await;
    assert_eq!(outcome, DispatchOutcome::Parked { woke: true });
    assert_eq!(
        router.eager_wakes.load(Ordering::SeqCst),
        1,
        "None-wake + deadline_expired + Err must still eager-wake unconditionally"
    );
}

// -----------------------------------------------------------------------
// Wake gate: `wake_gated=true` suppresses the eager wake but never the
// delivery attempt; `deadline_expired` still overrides the gate.
// -----------------------------------------------------------------------

/// Eager row + `wake_gated=true` + no active bridge (Ok(false)): the gate
/// suppresses the eager wake — `Parked { woke: false }` and `eager_wakes == 0`.
#[tokio::test]
async fn dispatch_row_wake_gated_suppresses_eager_wake() {
    let router = Arc::new(CountingRouter::default());
    // deliver_returns = 0 → Ok(false) (no active bridge).
    let row = fake_pending_row(30, Urgency::Normal);
    let outcome =
        dispatcher::dispatch_row(router.as_ref() as &dyn WakeRouter, &row, false, true).await;
    assert_eq!(outcome, DispatchOutcome::Parked { woke: false });
    assert_eq!(
        router.eager_wakes.load(Ordering::SeqCst),
        0,
        "wake_gated must suppress the eager wake on the Ok(false) branch",
    );
}

/// Eager Wasm row + `wake_gated=true`: the Wasm gate honours `wake_gated` too —
/// no eager wake fires. Covers the wasm-gate branch of the gate.
#[tokio::test]
async fn dispatch_row_wake_gated_suppresses_wasm_wake() {
    let router = Arc::new(CountingRouter::default());
    let mut row = fake_pending_row(31, Urgency::Normal);
    row.target_subscriber = ParticipantId::for_wasm("test-slug");
    let outcome =
        dispatcher::dispatch_row(router.as_ref() as &dyn WakeRouter, &row, false, true).await;
    assert_eq!(outcome, DispatchOutcome::Parked { woke: false });
    assert_eq!(
        router.eager_wakes.load(Ordering::SeqCst),
        0,
        "wake_gated must suppress the eager wake on the Wasm gate branch",
    );
}

/// Eager row + `wake_gated=true` + `deadline_expired=true`: the deadline override
/// beats the gate — the wake fires (`Parked { woke: true }`, `eager_wakes == 1`).
#[tokio::test]
async fn dispatch_row_wake_gated_deadline_beats_gate() {
    let router = Arc::new(CountingRouter::default());
    // deliver_returns = 0 → Ok(false).
    let row = fake_pending_row(32, Urgency::Normal);
    let outcome =
        dispatcher::dispatch_row(router.as_ref() as &dyn WakeRouter, &row, true, true).await;
    assert_eq!(outcome, DispatchOutcome::Parked { woke: true });
    assert_eq!(
        router.eager_wakes.load(Ordering::SeqCst),
        1,
        "deadline_expired must force the eager wake even when wake_gated is true",
    );
}
