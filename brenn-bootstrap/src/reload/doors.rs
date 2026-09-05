//! The two ways a reload is asked for, and the one task that answers.
//!
//! A reload has to be serialized — the driver holds the baseline, the consumer
//! registry and the generation counter, and two of them walking the directory
//! at once is exactly the half-applied state the phase split exists to prevent.
//! So neither door reloads anything. Each one enqueues a [`TriggerSource`] on a
//! bounded channel, and one task takes them off it strictly one at a time.
//!
//! - **The bus door** is the facility's own [`SystemInbox`] loop. One batch is
//!   one request, whatever the batch holds: the body is not parsed, and
//!   publishing anything to the request channel *is* the request. The handler
//!   only enqueues, so the inbox loop is never blocked by the reload it asked
//!   for and its position advances at dequeue as it does for any participant.
//! - **The signal door** is `SIGUSR1`, and its listener is installed whether or
//!   not the facility is declared, because the default disposition of `SIGUSR1`
//!   is to terminate the process. Not `SIGHUP`: that is already the
//!   log-rotation signal, and a rotation job must not apply a document the
//!   operator staged for a later window.
//!
//! Requests coalesce, which is the point rather than a degradation: the
//! question a trigger asks is "converge to what is on disk now", and five of
//! them arriving *while a reload runs* have one answer between them, so they
//! collapse into one further reload of whatever is on disk when that one
//! starts. A trigger that arrives while nothing is running is a reload of its
//! own — two asked for back to back are two, the second of which typically
//! reports `unchanged`. A full queue drops the trigger rather than blocking the
//! door: the reload it would have asked for is already pending.

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tracing::{debug, info, warn};

use brenn_messaging::Messenger;
use brenn_messaging::config_reload::CONFIG_RELOAD_COMPONENT;
use brenn_messaging::system::SystemInbox;

use super::driver::{ReloadDriver, TriggerSource};

/// How many triggers may be waiting when a reload is already running.
///
/// Small on purpose. A trigger is a signal, not a work item: the queue exists
/// so that a request arriving *during* a reload still produces a further one,
/// not so that requests accumulate. Anything past this depth is asking for a
/// reload the trigger ahead of it is already going to perform.
pub(crate) const TRIGGER_QUEUE_DEPTH: usize = 8;

/// Aborts the process if the reload driver task unwinds.
///
/// The rule is that the driver task never survives a panic. Every panic it can
/// reach is a host bug — prepare turns the panics a *document* can provoke into
/// refusals, and commit is past everything that could decline — and a panicking
/// task unwinds that task alone. What that leaves is a process whose reload
/// facility is silently dead: the directory, registry and bindings wherever the
/// walk stopped if it panicked mid-commit, or an intact process converging to
/// nothing forever if it panicked anywhere else, with a retained status that may
/// now name a document the process does not project. Both are a process
/// tolerating a known bug in one of its subsystems, which this backend does not
/// do. So the unwind becomes process death: systemd restarts, and the next boot
/// reads the document on disk and publishes its hash.
///
/// The panic hook has already run by the time this drops — it logs and alerts
/// synchronously — so the operator's evidence is out before the process goes.
/// Only an unwind: a future dropped during runtime teardown is a process on its
/// way out already, and `SIGTERM` mid-reload is the design's own accepted case.
/// The `catch_unwind` classifiers inside prepare are unaffected: they unwind
/// frames beneath this guard, not its own.
struct AbortOnUnwind;

impl Drop for AbortOnUnwind {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            return;
        }
        tracing::error!(
            "the reload driver task panicked; aborting rather than serving a process whose \
             reload facility is dead or whose retained status is false"
        );
        std::process::abort();
    }
}

/// A handle either door uses to ask for a reload.
#[derive(Clone)]
pub(crate) struct ReloadRequests {
    tx: mpsc::Sender<TriggerSource>,
}

impl ReloadRequests {
    /// Ask for a reload, without waiting for one.
    ///
    /// Never blocks and never fails a caller: a full queue means a reload is
    /// pending that will read the same disk this trigger wanted read. A closed
    /// queue means the driver task has ended, which happens only at process
    /// teardown — a panicking driver takes the process with it rather than
    /// leaving a closed queue behind — so the arm below is a defensive log.
    pub(crate) fn ask(&self, source: TriggerSource) {
        match self.tx.try_send(source) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => debug!(
                trigger = ?source,
                "reload requested while {TRIGGER_QUEUE_DEPTH} requests are already queued; \
                 coalesced into those"
            ),
            Err(TrySendError::Closed(_)) => warn!(
                trigger = ?source,
                "reload requested but the reload driver task has ended; this process is on its \
                 way down"
            ),
        }
    }
}

/// Start the one task that runs reloads, and hand back the way to ask it for
/// one.
///
/// Process-lifetime, and not one of the tasks whose death is tolerated: it runs
/// under [`AbortOnUnwind`], so a panic anywhere in it takes the process down for
/// systemd to restart rather than leaving a facility that converges nothing.
pub(crate) fn spawn_driver(driver: ReloadDriver) -> ReloadRequests {
    let (requests, rx) = trigger_channel();
    spawn_driver_on(driver, rx);
    requests
}

/// The trigger channel on its own: a way to ask, and the receiving half a
/// driver is run over.
///
/// Split from [`spawn_driver`] so that triggers can be enqueued before the
/// driver starts taking them — which is how the coalescing arm is reached
/// without racing a running reload.
pub(crate) fn trigger_channel() -> (ReloadRequests, mpsc::Receiver<TriggerSource>) {
    let (tx, rx) = mpsc::channel(TRIGGER_QUEUE_DEPTH);
    (ReloadRequests { tx }, rx)
}

/// Run one driver over an existing trigger queue.
pub(crate) fn spawn_driver_on(mut driver: ReloadDriver, mut rx: mpsc::Receiver<TriggerSource>) {
    drop(tokio::spawn(async move {
        // Named, never `let _`, which would drop it at once and cover nothing.
        let _abort_on_unwind = AbortOnUnwind;
        // The one trigger carried over from the reload that just ran, standing
        // for every request that arrived while it did.
        let mut pending: Option<TriggerSource> = None;
        loop {
            let source = match pending.take() {
                Some(source) => source,
                // Ends only when every door's handle is dropped, which is
                // process teardown: the doors hold theirs for the life of the
                // process.
                None => match rx.recv().await {
                    Some(source) => source,
                    None => break,
                },
            };
            driver.reload(source).await;
            // Whatever arrived during that reload asked to converge to the disk
            // as it stands now, and one further reload answers all of it.
            let mut coalesced = 0;
            while let Ok(next) = rx.try_recv() {
                if pending.is_none() {
                    pending = Some(next);
                } else {
                    coalesced += 1;
                }
            }
            if coalesced > 0 {
                info!(
                    coalesced,
                    "reload requests that arrived during a reload coalesced into one"
                );
            }
        }
        info!("reload driver: every door is closed; the driver task is done");
    }));
}

/// Open the bus door: the facility's participant drains its request channel and
/// turns each batch into one trigger.
///
/// The position on the request channel must already exist (boot attaches it
/// before anything can publish).
pub(crate) fn spawn_bus_door(
    messenger: &Arc<Messenger>,
    notify: Arc<tokio::sync::Notify>,
    requests: ReloadRequests,
) {
    let inbox = SystemInbox::new(CONFIG_RELOAD_COMPONENT, Arc::clone(messenger), notify);
    drop(tokio::spawn(inbox.run(move |batch| {
        let requests = requests.clone();
        async move {
            debug!(
                requests = batch.len(),
                "reload requested on the bus; the batch is one request"
            );
            requests.ask(TriggerSource::Bus);
        }
    })));
    info!("reload facility: the bus door is open");
}

/// Change `SIGUSR1`'s disposition, before anything slow runs.
///
/// Separate from the door, and called at the very top of boot, because the
/// default disposition of `SIGUSR1` is to *terminate the process*. The door
/// itself cannot open until the document is compiled, the database migrated and
/// every component cranelift-compiled — and the workflow the operator is told
/// to follow is `restart`, then `reload`. A `reload` issued while the previous
/// `restart` is still booting would land on a process with no handler and kill
/// it outright, on a window that grows with the number of installed components.
/// Installing the stream here makes the signal harmless from the first
/// millisecond; anything raised before the door opens is delivered to it when
/// it does, which is one reload of whatever is on disk by then.
pub(crate) fn install_sigusr1() -> tokio::signal::unix::Signal {
    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1())
        .expect("failed to install SIGUSR1 handler")
}

/// Open the signal door over an already-installed stream.
///
/// Opened unconditionally, and that is the whole reason this takes an
/// `Option`: a deployment that never declared the facility still has the
/// stream installed by [`install_sigusr1`], and something has to drain it. With
/// no facility the signal is logged and ignored.
pub(crate) fn spawn_signal_door(
    mut usr1: tokio::signal::unix::Signal,
    requests: Option<ReloadRequests>,
) {
    drop(tokio::spawn(async move {
        while usr1.recv().await.is_some() {
            match &requests {
                Some(requests) => {
                    info!("received SIGUSR1: converging to the document on disk");
                    requests.ask(TriggerSource::Signal);
                }
                None => info!(
                    "received SIGUSR1 but the reload facility is not declared; ignoring. Declare \
                     brenn:config.reload and brenn:config.status to turn it on"
                ),
            }
        }
    }));
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    /// A request handle with no driver behind it.
    fn requests() -> (ReloadRequests, mpsc::Receiver<TriggerSource>) {
        trigger_channel()
    }

    /// Serializes the tests that raise `SIGUSR1`: the signal is process-wide,
    /// so every stream installed anywhere in this binary sees every raise, and
    /// two of these running at once would each count the other's.
    static RAISING: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn raise_usr1() {
        assert_eq!(
            unsafe { libc::kill(std::process::id() as libc::pid_t, libc::SIGUSR1) },
            0,
            "raising SIGUSR1 on this process"
        );
    }

    /// The trigger the door enqueued, or `None` after a generous wait.
    async fn next_trigger(rx: &mut mpsc::Receiver<TriggerSource>) -> Option<TriggerSource> {
        tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap_or(None)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sigusr1_asks_for_a_reload_and_does_not_end_the_process() {
        let _raising = RAISING.lock().await;
        let (requests, mut rx) = requests();
        spawn_signal_door(install_sigusr1(), Some(requests));
        raise_usr1();
        assert_eq!(next_trigger(&mut rx).await, Some(TriggerSource::Signal));

        raise_usr1();
        assert_eq!(next_trigger(&mut rx).await, Some(TriggerSource::Signal));
    }

    /// The arm whose absence is fatal rather than merely wrong: a deployment
    /// that declared no reload facility still has the stream installed, and the
    /// signal is logged and ignored instead of terminating the process.
    ///
    /// Surviving the raise *is* the assertion — a stream that was never
    /// installed would have killed this test binary. The armed door beside it
    /// is the proof that the signal really was delivered, rather than the test
    /// having asserted nothing about a signal that never arrived.
    #[tokio::test(flavor = "multi_thread")]
    async fn sigusr1_without_a_facility_is_ignored_rather_than_fatal() {
        let _raising = RAISING.lock().await;
        spawn_signal_door(install_sigusr1(), None);
        let (requests, mut rx) = requests();
        spawn_signal_door(install_sigusr1(), Some(requests));

        raise_usr1();

        assert_eq!(next_trigger(&mut rx).await, Some(TriggerSource::Signal));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_full_queue_drops_the_trigger_rather_than_blocking_the_door() {
        let (requests, mut rx) = requests();
        for _ in 0..TRIGGER_QUEUE_DEPTH {
            requests.ask(TriggerSource::Bus);
        }
        // Must return even though the queue is full — non-blocking is the
        // contract the doors rely on.
        requests.ask(TriggerSource::Signal);

        let mut queued = Vec::new();
        while let Ok(trigger) = rx.try_recv() {
            queued.push(trigger);
        }
        assert_eq!(queued.len(), TRIGGER_QUEUE_DEPTH);
        assert!(
            queued.iter().all(|source| *source == TriggerSource::Bus),
            "the dropped trigger is the one that arrived last: {queued:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_closed_driver_does_not_fail_the_door() {
        let (requests, rx) = requests();
        drop(rx);
        requests.ask(TriggerSource::Bus);
    }
}
