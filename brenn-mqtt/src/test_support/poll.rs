//! The deadline poll every wait in this module sits on.
//!
//! One loop rather than one per waiter: the interval and the
//! evaluate-once-past-the-deadline ordering are a property of how these tests
//! wait, and a suite's flakiness must not depend on which helper a case
//! happened to call.

use std::future::Future;

/// How long between probes in every poll built on [`poll_until`].
pub const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(25);

/// Call `probe` every [`POLL_INTERVAL`] until it answers `Some`, and return that
/// answer.
///
/// Each pass probes first and checks the deadline second, so a state reached
/// during the last sleep is still seen — the grace is one probe, and it exists
/// only where the deadline elapses inside a sleep. At `timeout_secs = 0` the
/// deadline is already past when the loop starts, so the first probe is the
/// only one and there is no grace beyond it. `diagnostic` is awaited only on
/// the failing path — a caller composes its own message and whatever it can read
/// about why nothing happened into the string it returns.
///
/// # Panics
///
/// With `diagnostic()` when `probe` has not answered `Some` by `timeout_secs`.
pub async fn poll_until<T, P, PFut, D, DFut>(timeout_secs: u64, mut probe: P, diagnostic: D) -> T
where
    P: FnMut() -> PFut,
    PFut: Future<Output = Option<T>>,
    D: Fn() -> DFut,
    DFut: Future<Output = String>,
{
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
    loop {
        if let Some(reached) = probe().await {
            return reached;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{}",
            diagnostic().await
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::poll_until;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn a_probe_answering_late_returns_and_the_diagnostic_is_never_built() {
        let probes = AtomicUsize::new(0);
        let diagnostics = AtomicUsize::new(0);
        let reached = poll_until(
            5,
            || async {
                let call = probes.fetch_add(1, Ordering::SeqCst) + 1;
                (call == 3).then_some(call)
            },
            || async {
                diagnostics.fetch_add(1, Ordering::SeqCst);
                "the probe never answered".to_string()
            },
        )
        .await;
        assert_eq!(reached, 3, "the answer is the probe's, not a retry count");
        assert_eq!(probes.load(Ordering::SeqCst), 3);
        assert_eq!(
            diagnostics.load(Ordering::SeqCst),
            0,
            "the diagnostic is awaited on the failing path only",
        );
    }

    #[tokio::test]
    async fn the_probe_is_asked_before_the_deadline_is_checked() {
        // The deadline is already past when the loop starts, so a probe that
        // answers on its first call is the whole test of the ordering: the
        // reversed loop would time out here.
        let reached = poll_until(0, || async { Some(7) }, || async { "unused".to_string() }).await;
        assert_eq!(reached, 7);
    }

    #[tokio::test]
    #[should_panic(expected = "nothing ever happened (health: unknown)")]
    async fn a_probe_that_never_answers_panics_with_the_diagnostic() {
        poll_until(
            0,
            || async { None::<()> },
            || async { "nothing ever happened (health: unknown)".to_string() },
        )
        .await;
    }
}
