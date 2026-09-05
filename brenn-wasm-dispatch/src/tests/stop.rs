//! The consumer loop's one orderly exit.
//!
//! The loop is otherwise process-lifetime, so these tests are about the stop
//! arm alone: that a signal ends the task, that the step already in flight is
//! finished first, that a dropped sender says the same thing as a signal, and
//! that a task which died on its own is not reported as an orderly stop.

use super::*;

use std::time::Duration;

use brenn_lib::messaging::config::{ActivationPacing, Depth};
use brenn_obs::alerting::{AlertSeverity, make_capturing_alerter_with_severity};

/// A stopped task must resolve promptly. Generous enough not to flake on a
/// loaded runner, short enough that a loop with no exit fails the test rather
/// than hanging the suite.
const JOIN_DEADLINE: Duration = Duration::from_secs(5);

/// Await a handle's join, failing rather than hanging if the loop never leaves.
async fn join_within(join: tokio::task::JoinHandle<()>) {
    tokio::time::timeout(JOIN_DEADLINE, join)
        .await
        .expect("the consumer loop must leave on its stop signal")
        .expect("the consumer task must not panic");
}

/// The signal, arriving while the loop is parked on its `Notify`: nothing is in
/// flight, so the task leaves without doing another step.
#[tokio::test]
async fn a_stop_while_parked_ends_the_task() {
    let slug = "stop-parked";
    let (messenger, channel, wasm_sub) = testutils::build_wasm_messenger(
        slug,
        "stop-parked-ch",
        Depth::Bounded(4),
        Depth::Bounded(0),
    )
    .await;
    let (cfg, _alerts, _db) = build_cfg(
        slug,
        Arc::clone(&messenger),
        &channel,
        Depth::Bounded(4),
        Depth::Bounded(0),
    );
    let handle = spawn_wasm_consumer_task(cfg);
    // The startup sweep has nothing to do here; waiting for the subscriber to
    // be owed nothing is what says the loop reached its wait.
    assert!(wait_pending_empty(&messenger, &wasm_sub, JOIN_DEADLINE).await);

    assert!(handle.stop.send(true).is_ok());
    join_within(handle.join).await;
}

/// The signal, arriving before the startup sweep can have finished: the sweep
/// runs before the loop and so before the arm exists at all, and its rows are
/// consumed whatever the signal says.
///
/// This pins the sweep's own imperviousness. The loop's step is
/// `a_stop_during_a_drain_step_lets_the_step_finish` below, which is where the
/// arm is.
#[tokio::test]
async fn a_stop_before_the_startup_sweep_does_not_preempt_it() {
    let slug = "stop-mid-step";
    let (messenger, channel, wasm_sub) = testutils::build_wasm_messenger(
        slug,
        "stop-mid-step-ch",
        Depth::Bounded(4),
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
    let (cfg, _alerts, _db) = build_cfg(
        slug,
        Arc::clone(&messenger),
        &channel,
        Depth::Bounded(4),
        Depth::Bounded(0),
    );
    let handle = spawn_wasm_consumer_task(cfg);
    // No await between the spawn and the signal, so the sweep is at best
    // partway through it.
    assert!(handle.stop.send(true).is_ok());
    join_within(handle.join).await;

    assert!(
        brenn_messaging::testutils::owed_everywhere(&messenger, &wasm_sub)
            .await
            .is_empty(),
        "the startup sweep must run to completion whatever the stop signal says"
    );
}

/// A dropped sender is the same statement as a signal: nobody is holding this
/// consumer in service any more.
#[tokio::test]
async fn a_dropped_stop_sender_ends_the_task() {
    let slug = "stop-dropped";
    let (messenger, channel, wasm_sub) = testutils::build_wasm_messenger(
        slug,
        "stop-dropped-ch",
        Depth::Bounded(4),
        Depth::Bounded(0),
    )
    .await;
    let (cfg, _alerts, _db) = build_cfg(
        slug,
        Arc::clone(&messenger),
        &channel,
        Depth::Bounded(4),
        Depth::Bounded(0),
    );
    let handle = spawn_wasm_consumer_task(cfg);
    assert!(wait_pending_empty(&messenger, &wasm_sub, JOIN_DEADLINE).await);

    let ConsumerHandle { stop, join } = handle;
    drop(stop);
    join_within(join).await;
}

/// The loop still serves wakes: a stop that never comes leaves the consumer
/// working, which is what makes the tests above about the arm rather than about
/// a loop that exits on anything.
#[tokio::test]
async fn an_unstopped_loop_keeps_draining() {
    let slug = "stop-unstopped";
    let (messenger, channel, wasm_sub) = testutils::build_wasm_messenger(
        slug,
        "stop-unstopped-ch",
        Depth::Bounded(4),
        Depth::Bounded(0),
    )
    .await;
    let (cfg, _alerts, _db) = build_cfg(
        slug,
        Arc::clone(&messenger),
        &channel,
        Depth::Bounded(4),
        Depth::Bounded(0),
    );
    let notify = Arc::clone(&cfg.notify);
    let handle = spawn_wasm_consumer_task(cfg);
    assert!(wait_pending_empty(&messenger, &wasm_sub, JOIN_DEADLINE).await);

    testutils::insert_bus_message(&messenger, &channel, "after-sweep", ChannelScheme::Brenn).await;
    notify.notify_one();
    assert!(
        wait_pending_empty(&messenger, &wasm_sub, JOIN_DEADLINE).await,
        "a running loop must drain a row published after its startup sweep"
    );
    handle.stop_and_join().await;
}

/// The signal, arriving while a *loop* step is underway: the arm is on the wait
/// and not around the step, so the rows that step was assembled over are
/// consumed and their publishes go out before the task leaves.
///
/// The step is held open through the pacer rather than through the guest: a
/// burst of one is spent on the startup sweep, so the wake's `admit` sleeps a
/// whole `min_period`, and the throttle alert is the observation that the wake
/// has been taken up and the step is committed. Sending the signal before that
/// observation would race the two `select!` arms, which is a different
/// question — with both ready either may be chosen.
#[tokio::test]
async fn a_stop_during_a_drain_step_lets_the_step_finish() {
    let slug = "stop-loop-step";
    let (messenger, channel, wasm_sub) = testutils::build_wasm_messenger(
        slug,
        "stop-loop-step-ch",
        Depth::Bounded(4),
        Depth::Bounded(0),
    )
    .await;
    // One row for the sweep, so waiting for the subscriber to be owed nothing
    // says the sweep is over and its token is spent.
    testutils::insert_bus_message(&messenger, &channel, "sweep-row", ChannelScheme::Brenn).await;

    let (mut cfg, _alerts, _db) = build_cfg(
        slug,
        Arc::clone(&messenger),
        &channel,
        Depth::Bounded(4),
        Depth::Bounded(0),
    );
    let (dispatcher, captured, _drainer) = make_capturing_alerter_with_severity();
    cfg.alert_dispatcher = dispatcher;
    cfg.activation_pacing = ActivationPacing {
        burst: 1,
        min_period: STEP_WINDOW,
    };
    let notify = Arc::clone(&cfg.notify);
    let handle = spawn_wasm_consumer_task(cfg);
    assert!(wait_pending_empty(&messenger, &wasm_sub, JOIN_DEADLINE).await);

    for i in 0..3 {
        testutils::insert_bus_message(
            &messenger,
            &channel,
            &format!("row-{i}"),
            ChannelScheme::Brenn,
        )
        .await;
    }
    notify.notify_one();
    assert!(
        wait_for_throttle_alert(&captured).await,
        "the wake's activation must be held at the pacing gate for this test to \
         signal inside the step"
    );

    assert!(handle.stop.send(true).is_ok());
    join_within(handle.join).await;

    assert!(
        brenn_messaging::testutils::owed_everywhere(&messenger, &wasm_sub)
            .await
            .is_empty(),
        "the step in flight when the stop arrived must have finished its rows"
    );
}

/// How long the paced step is held open. Long enough that the signal below
/// lands inside it on a loaded runner, short enough not to stretch the suite.
const STEP_WINDOW: Duration = Duration::from_secs(2);

/// What a capturing alerter collects: severity, title, body per alert.
type CapturedAlerts = Arc<std::sync::Mutex<Vec<(AlertSeverity, String, String)>>>;

/// Poll until the pacer's throttle alert has been dispatched, or the deadline
/// passes. The alert travels through the dispatcher's drainer task, so the
/// capture is observed rather than read once.
async fn wait_for_throttle_alert(captured: &CapturedAlerts) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < JOIN_DEADLINE {
        if !captured
            .lock()
            .expect("the capture is not poisoned")
            .is_empty()
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    false
}

/// A task that dies *under* the stop is not an orderly stop: the stopper is the
/// first holder positioned to see it, so the panic is re-raised there rather
/// than reported as a consumer cleanly out of service. The task here cannot
/// have run before the join is awaited — this runtime is single-threaded — so
/// its panic is one the stop provoked.
#[tokio::test]
#[should_panic(expected = "guest died")]
async fn a_panicked_task_re_raises_its_panic_at_the_stopper() {
    let (stop, _stop_rx) = tokio::sync::watch::channel(false);
    let handle = ConsumerHandle {
        stop,
        join: tokio::spawn(async { panic!("guest died") }),
    };
    handle.stop_and_join().await;
}

/// A task that ended without running to completion and without panicking —
/// cancelled from outside — is the same class of surprise, and named as one.
#[tokio::test]
#[should_panic(expected = "consumer task ended abnormally")]
async fn an_aborted_task_is_not_an_orderly_stop() {
    let (stop, _stop_rx) = tokio::sync::watch::channel(false);
    let join = tokio::spawn(async { std::future::pending::<()>().await });
    join.abort();
    let handle = ConsumerHandle { stop, join };
    handle.stop_and_join().await;
}

/// A task that had already died before anything asked it to stop is treated as
/// stopped.
///
/// A dead consumer task is an accepted state (`TODO.md`
/// `task-death-supervision`: the panic hook alerts, an operator restarts), and
/// the operator's natural response is to edit the document and reload — remove
/// the consumer, or point it at a fixed bundle. Re-raising that old panic at
/// the stopper would unwind the reload's commit walk *after* it had taken the
/// consumer's directory entries away, which is the half-applied state the whole
/// phase split exists to prevent.
#[tokio::test]
async fn a_task_that_died_before_the_stop_is_treated_as_stopped() {
    let (stop, _stop_rx) = tokio::sync::watch::channel(false);
    let join = tokio::spawn(async { panic!("the guest took the host down an hour ago") });
    while !join.is_finished() {
        tokio::task::yield_now().await;
    }
    // Returns rather than unwinding: that is the whole assertion.
    ConsumerHandle { stop, join }.stop_and_join().await;
}
