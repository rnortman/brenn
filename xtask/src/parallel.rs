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

/// Run every task to completion across up to `jobs` concurrent workers and return
/// true iff all returned true. No early abort: every task runs even if one fails
/// (aggregate reporting, matching the serial check pipeline). With `jobs <= 1` (or
/// a single task) the tasks run inline in the given order — no threads spawned, so
/// output stays deterministic for the serial/CI case.
pub fn run_jobs(jobs: usize, tasks: Vec<Box<dyn FnOnce() -> bool + Send>>) -> bool {
    let n = tasks.len();
    if jobs <= 1 || n <= 1 {
        // fold (not any/all) so every task is invoked; no short-circuit.
        return tasks.into_iter().fold(true, |acc, t| {
            let ok = t();
            acc && ok
        });
    }

    let queue: Mutex<VecDeque<Box<dyn FnOnce() -> bool + Send>>> =
        Mutex::new(tasks.into_iter().collect());
    let all_ok = AtomicBool::new(true);
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
                        Some(t) => {
                            if !t() {
                                all_ok.store(false, Ordering::Relaxed);
                            }
                        }
                        None => break,
                    }
                }
            });
        }
    });

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

    /// Build `results.len()` tasks that each bump `count` and return the given
    /// boolean. `count` is an Arc so the boxed (`'static`) closures can own a clone.
    fn counting_tasks(
        count: &Arc<AtomicUsize>,
        results: &[bool],
    ) -> Vec<Box<dyn FnOnce() -> bool + Send>> {
        results
            .iter()
            .map(|&r| {
                let c = Arc::clone(count);
                Box::new(move || {
                    c.fetch_add(1, Ordering::Relaxed);
                    r
                }) as Box<dyn FnOnce() -> bool + Send>
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
        let tasks: Vec<Box<dyn FnOnce() -> bool + Send>> = Vec::new();
        assert!(run_jobs(4, tasks));
    }
}
