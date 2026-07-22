// Parallelism control for `xtask check`. Reads the BRENN_CHECK_JOBS knob and runs
// a set of independent lane closures across a bounded worker pool.
//
// BRENN_CHECK_JOBS semantics on THIS (xtask) side — a concurrency count bounding
// the lane pool below:
//   unset      → available parallelism (workstation default)
//   "0" | "1"  → fully serial
//   "N"        → up to N concurrent lanes
// A present-but-non-numeric value is a hard error.
//
// The Makefile check-common step reads the same variable but treats it as a
// BINARY overlap switch (any N>1 overlaps the non-cargo steps as a group; the
// magnitude does not bound that side). Both consumers reject a non-numeric value.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

/// Resolved concurrency limit from BRENN_CHECK_JOBS. Always >= 1.
pub fn check_jobs() -> usize {
    match std::env::var("BRENN_CHECK_JOBS") {
        Ok(v) => parse_jobs(Some(&v)),
        // Non-unicode or absent → treat as unset (default parallel). A garbage
        // env value is not a correctness hazard; the default is the safe choice.
        Err(_) => parse_jobs(None),
    }
}

/// Parse the raw knob value into a concurrency limit (>= 1). Pure, for testing.
fn parse_jobs(val: Option<&str>) -> usize {
    match val {
        // Absent or empty → default. Empty matches the Makefile's `-n` unset test,
        // which treats an empty value as "no knob set".
        None => default_jobs(),
        Some("") => default_jobs(),
        Some(v) => {
            // Digits-only, no trim: exactly the grammar the Makefile validates
            // (`^[0-9]+$`), so both consumers accept and reject the same strings
            // (a leading `+` or surrounding whitespace is rejected by both).
            if !v.bytes().all(|b| b.is_ascii_digit()) {
                panic!("BRENN_CHECK_JOBS must be a non-negative integer (0/1 = serial), got {v:?}");
            }
            match v.parse::<usize>() {
                Ok(0) | Ok(1) => 1,
                Ok(n) => n,
                Err(_) => panic!(
                    "BRENN_CHECK_JOBS must be a non-negative integer (0/1 = serial), got {v:?}"
                ),
            }
        }
    }
}

fn default_jobs() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// A named check lane: an attribution string plus its work closure.
pub type NamedTask = (&'static str, Box<dyn FnOnce() -> bool + Send>);

/// Extract a human-readable message from a caught panic payload. Handles the two
/// common payload types (`&'static str` from `panic!("lit")`, `String` from a
/// formatted message); anything else is opaque.
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

/// If any lane panicked, print an attributed summary and re-panic so the process
/// dies (exit 101). Each original panic already hit the default panic hook at its
/// site (full message + backtrace on stderr); this restores the lane attribution
/// that a bare scoped-thread join dropped as `"a scoped thread panicked"`.
fn report_panics(panics: &[(&'static str, String)]) {
    if panics.is_empty() {
        return;
    }
    let mut summary = format!("xtask check: {} lane(s) panicked:", panics.len());
    for (name, msg) in panics {
        summary.push_str(&format!("\n  [{name}] {msg}"));
    }
    panic!("{summary}");
}

/// Run every task to completion across up to `jobs` concurrent workers and return
/// true iff all returned true. No early abort: every task runs even if one fails
/// (aggregate reporting, matching the serial check pipeline). With `jobs <= 1` (or
/// a single task) the tasks run inline in the given order — no threads spawned, so
/// output stays deterministic for the serial/CI case.
///
/// Every task runs under `catch_unwind` in both paths; a panicking lane is recorded
/// with its name, does not starve the queue, and after all lanes finish `run_jobs`
/// re-panics with an aggregated, attributed summary (BETTER DEAD THAN WRONG: the
/// process still dies, but the death names the lane). `AssertUnwindSafe` is sound:
/// each closure is moved in and consumed once, and the only cross-task state is
/// behind an atomic or short mutex-guarded pushes, so a caught panic leaves no torn
/// state observable by later tasks.
///
/// This relies on unwinding: a `panic = "abort"` profile would bypass `catch_unwind`.
/// Do not set `panic = "abort"` for the profile xtask runs under.
pub fn run_jobs(jobs: usize, tasks: Vec<NamedTask>) -> bool {
    let n = tasks.len();
    if jobs <= 1 || n <= 1 {
        let mut all_ok = true;
        let mut panics: Vec<(&'static str, String)> = Vec::new();
        for (name, task) in tasks {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(task)) {
                Ok(ok) => all_ok = all_ok && ok,
                Err(payload) => {
                    all_ok = false;
                    panics.push((name, panic_message(payload.as_ref())));
                }
            }
        }
        report_panics(&panics);
        return all_ok;
    }

    let queue: Mutex<VecDeque<NamedTask>> = Mutex::new(tasks.into_iter().collect());
    let all_ok = AtomicBool::new(true);
    let panics: Mutex<Vec<(&'static str, String)>> = Mutex::new(Vec::new());
    let workers = jobs.min(n);

    std::thread::scope(|s| {
        for _ in 0..workers {
            s.spawn(|| {
                loop {
                    let task = {
                        let mut q = queue.lock().unwrap_or_else(|e| {
                            panic!("xtask check: job queue mutex poisoned: {e}")
                        });
                        q.pop_front()
                    };
                    match task {
                        Some((name, t)) => {
                            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(t)) {
                                Ok(true) => {}
                                Ok(false) => all_ok.store(false, Ordering::Relaxed),
                                Err(payload) => {
                                    all_ok.store(false, Ordering::Relaxed);
                                    panics
                                        .lock()
                                        .unwrap_or_else(|e| {
                                            panic!("xtask check: panic list mutex poisoned: {e}")
                                        })
                                        .push((name, panic_message(payload.as_ref())));
                                }
                            }
                        }
                        None => break,
                    }
                }
            });
        }
    });

    let panics = panics
        .into_inner()
        .unwrap_or_else(|e| panic!("xtask check: panic list mutex poisoned: {e}"));
    report_panics(&panics);
    all_ok.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn parse_jobs_unset_is_at_least_one() {
        assert!(parse_jobs(None) >= 1);
    }

    #[test]
    fn parse_jobs_zero_and_one_are_serial() {
        assert_eq!(parse_jobs(Some("0")), 1);
        assert_eq!(parse_jobs(Some("1")), 1);
    }

    #[test]
    fn parse_jobs_reads_n() {
        assert_eq!(parse_jobs(Some("4")), 4);
    }

    #[test]
    #[should_panic(expected = "must be a non-negative integer")]
    fn parse_jobs_rejects_whitespace() {
        // Digits-only grammar, matching the Makefile's `^[0-9]+$` grep.
        parse_jobs(Some("  3  "));
    }

    #[test]
    #[should_panic(expected = "must be a non-negative integer")]
    fn parse_jobs_rejects_leading_plus() {
        parse_jobs(Some("+3"));
    }

    #[test]
    fn parse_jobs_empty_is_default() {
        assert!(parse_jobs(Some("")) >= 1);
    }

    #[test]
    #[should_panic(expected = "must be a non-negative integer")]
    fn parse_jobs_rejects_garbage() {
        parse_jobs(Some("abc"));
    }

    /// Build `results.len()` named tasks that each bump `count` and return the given
    /// boolean. `count` is an Arc so the boxed (`'static`) closures can own a clone.
    fn counting_tasks(count: &Arc<AtomicUsize>, results: &[bool]) -> Vec<NamedTask> {
        results
            .iter()
            .map(|&r| {
                let c = Arc::clone(count);
                (
                    "counter",
                    Box::new(move || {
                        c.fetch_add(1, Ordering::Relaxed);
                        r
                    }) as Box<dyn FnOnce() -> bool + Send>,
                )
            })
            .collect()
    }

    /// Serial path (jobs=1): every task runs and the aggregate is true when all pass.
    #[test]
    fn run_jobs_serial_runs_all_true() {
        let count = Arc::new(AtomicUsize::new(0));
        let tasks = counting_tasks(&count, &[true, true, true]);
        assert!(run_jobs(1, tasks));
        assert_eq!(count.load(Ordering::Relaxed), 3);
    }

    /// A single failing task makes the aggregate false, but all tasks still run.
    #[test]
    fn run_jobs_parallel_aggregates_failure_and_runs_all() {
        let count = Arc::new(AtomicUsize::new(0));
        let tasks = counting_tasks(&count, &[true, false, true, true]);
        assert!(
            !run_jobs(4, tasks),
            "one false task must fail the aggregate"
        );
        assert_eq!(count.load(Ordering::Relaxed), 4, "all tasks must run");
    }

    /// More workers than tasks still runs each task exactly once.
    #[test]
    fn run_jobs_more_workers_than_tasks() {
        let count = Arc::new(AtomicUsize::new(0));
        let tasks = counting_tasks(&count, &[true, true]);
        assert!(run_jobs(8, tasks));
        assert_eq!(count.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn run_jobs_empty_is_true() {
        let tasks: Vec<NamedTask> = Vec::new();
        assert!(run_jobs(4, tasks));
    }

    /// Parallel path: one of four lanes panics. The aggregate re-panic names the lane
    /// and carries the original message, and the other three lanes all ran (the
    /// panicking lane does not starve the queue).
    #[test]
    fn run_jobs_parallel_panic_is_attributed_and_others_run() {
        let count = Arc::new(AtomicUsize::new(0));
        let (c0, c1, c2, c3) = (
            Arc::clone(&count),
            Arc::clone(&count),
            Arc::clone(&count),
            Arc::clone(&count),
        );
        let tasks: Vec<NamedTask> = vec![
            (
                "lane-a",
                Box::new(move || {
                    c0.fetch_add(1, Ordering::Relaxed);
                    true
                }),
            ),
            (
                "lane-b",
                Box::new(move || {
                    c1.fetch_add(1, Ordering::Relaxed);
                    panic!("boom")
                }),
            ),
            (
                "lane-c",
                Box::new(move || {
                    c2.fetch_add(1, Ordering::Relaxed);
                    true
                }),
            ),
            (
                "lane-d",
                Box::new(move || {
                    c3.fetch_add(1, Ordering::Relaxed);
                    true
                }),
            ),
        ];
        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_jobs(4, tasks)))
            .expect_err("run_jobs must re-panic when a lane panics");
        let msg = payload
            .downcast_ref::<String>()
            .expect("aggregate panic carries a String");
        assert!(
            msg.contains("lane-b"),
            "summary names the panicking lane: {msg}"
        );
        assert!(
            msg.contains("boom"),
            "summary carries the original message: {msg}"
        );
        assert_eq!(
            count.load(Ordering::Relaxed),
            4,
            "all four lanes ran despite the panic"
        );
    }

    /// Fewer workers than tasks, with every worker forced through a panic: each worker
    /// must catch a panic and keep pulling from the queue, or the trailing ok lanes never
    /// run. Guards the "does not starve the queue" property against a regression that
    /// `break`s the worker loop on a caught panic — the equal-workers-and-tasks test above
    /// cannot catch that, since a surviving sibling worker would drain the queue. Two
    /// panics sit at the front so both workers catch one before any ok is poppable.
    #[test]
    fn run_jobs_parallel_panic_does_not_starve_queue() {
        let count = Arc::new(AtomicUsize::new(0));
        let (c0, c1, c2, c3) = (
            Arc::clone(&count),
            Arc::clone(&count),
            Arc::clone(&count),
            Arc::clone(&count),
        );
        let tasks: Vec<NamedTask> = vec![
            (
                "panic-1",
                Box::new(move || {
                    c0.fetch_add(1, Ordering::Relaxed);
                    panic!("boom-1")
                }),
            ),
            (
                "panic-2",
                Box::new(move || {
                    c1.fetch_add(1, Ordering::Relaxed);
                    panic!("boom-2")
                }),
            ),
            (
                "ok-1",
                Box::new(move || {
                    c2.fetch_add(1, Ordering::Relaxed);
                    true
                }),
            ),
            (
                "ok-2",
                Box::new(move || {
                    c3.fetch_add(1, Ordering::Relaxed);
                    true
                }),
            ),
        ];
        // jobs=2, n=4 → parallel path, workers = 2: each worker services multiple tasks.
        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_jobs(2, tasks)))
            .expect_err("run_jobs must re-panic when lanes panic");
        let msg = payload
            .downcast_ref::<String>()
            .expect("aggregate panic carries a String");
        assert!(
            msg.contains("panic-1") && msg.contains("panic-2"),
            "both panicking lanes attributed: {msg}"
        );
        assert_eq!(
            count.load(Ordering::Relaxed),
            4,
            "both ok lanes ran after the workers caught their panics (queue not starved)"
        );
    }

    /// Serial path: the first lane panics; later lanes still run, and the harness
    /// re-panics at the end with attribution.
    #[test]
    fn run_jobs_serial_panic_runs_remaining_then_attributes() {
        let count = Arc::new(AtomicUsize::new(0));
        let (c0, c1, c2) = (Arc::clone(&count), Arc::clone(&count), Arc::clone(&count));
        let tasks: Vec<NamedTask> = vec![
            (
                "first",
                Box::new(move || {
                    c0.fetch_add(1, Ordering::Relaxed);
                    panic!("first boom")
                }),
            ),
            (
                "second",
                Box::new(move || {
                    c1.fetch_add(1, Ordering::Relaxed);
                    true
                }),
            ),
            (
                "third",
                Box::new(move || {
                    c2.fetch_add(1, Ordering::Relaxed);
                    true
                }),
            ),
        ];
        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_jobs(1, tasks)))
            .expect_err("serial run_jobs must re-panic at the end");
        let msg = payload
            .downcast_ref::<String>()
            .expect("aggregate panic carries a String");
        assert!(
            msg.contains("first"),
            "attributes the panicking lane: {msg}"
        );
        assert_eq!(
            count.load(Ordering::Relaxed),
            3,
            "later lanes still ran after an early panic"
        );
    }

    /// Two panicking lanes are both listed in one summary.
    #[test]
    fn run_jobs_reports_multiple_panics() {
        let tasks: Vec<NamedTask> = vec![
            ("lane-x", Box::new(|| panic!("x-boom"))),
            ("ok", Box::new(|| true)),
            ("lane-y", Box::new(|| panic!("y-boom"))),
        ];
        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_jobs(4, tasks)))
            .expect_err("run_jobs must re-panic");
        let msg = payload
            .downcast_ref::<String>()
            .expect("aggregate panic carries a String");
        assert!(
            msg.contains("lane-x") && msg.contains("lane-y"),
            "both panicking lanes listed: {msg}"
        );
        assert!(
            msg.contains("2 lane(s) panicked"),
            "count reflects both: {msg}"
        );
    }

    /// A non-string panic payload is reported with placeholder text; the lane is
    /// still named.
    #[test]
    fn run_jobs_non_string_panic_payload() {
        // Single task → serial path; wrap to catch the aggregate re-panic.
        let tasks: Vec<NamedTask> = vec![("numeric", Box::new(|| std::panic::panic_any(42u32)))];
        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_jobs(1, tasks)))
            .expect_err("run_jobs must re-panic");
        let msg = payload
            .downcast_ref::<String>()
            .expect("aggregate panic carries a String");
        assert!(
            msg.contains("<non-string panic payload>"),
            "opaque payload noted: {msg}"
        );
        assert!(msg.contains("numeric"), "lane still named: {msg}");
    }
}
