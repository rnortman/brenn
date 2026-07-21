// `xtask deny`: run cargo-deny over every Rust unit in the repo.
// Fetches the advisory DB once, then checks all units in parallel with fetching
// disabled. Aggregates failures across all units, then exits non-zero if any fail.

use crate::discover::discover_units;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

/// Run cargo-deny over all units discovered from `repo_root`. Returns true on success (all pass).
pub fn run_deny(repo_root: &Path) -> bool {
    assert_cargo_deny_available();

    let deny_config = repo_root.join("deny.toml");
    // Validate the path is UTF-8 once up front (fail fast, consistent with the upfront
    // assert_cargo_deny_available() probe).
    let deny_config_str = deny_config
        .to_str()
        .expect("deny.toml path contains non-UTF-8 bytes");
    let units = discover_units(repo_root);

    // Fetch the shared advisory DB once. Every parallel check below runs with
    // --disable-fetch, so they read one consistent snapshot without racing git fetches.
    if !prefetch_advisory_db(repo_root, deny_config_str) {
        return false;
    }

    println!("xtask deny: checking {} units in parallel...", units.len());

    // Spawn one check per unit concurrently; each captures its own output. As each
    // thread joins (in unit/discovery order) its header and output are printed, so a
    // completed prefix streams progressively even while slower units are still running.
    let outputs = run_parallel(
        &units,
        |unit| {
            Command::new("cargo")
                .args([
                    "deny",
                    "check",
                    "--disable-fetch",
                    "--config",
                    deny_config_str,
                ])
                .current_dir(&unit.dir)
                .output()
                .unwrap_or_else(|e| {
                    panic!(
                        "xtask deny: failed to spawn `cargo deny check` in {:?}: {e}",
                        unit.dir
                    )
                })
        },
        |unit, output| {
            println!(
                "xtask deny: [{kind}] {dir}",
                kind = unit.kind.as_str(),
                dir = unit.dir.display()
            );
            dump_child_output(output);
        },
    );

    // A non-zero exit code is a genuine cargo-deny policy violation. Termination by
    // signal (no exit code) is abnormal — typically the OS reaping a child under
    // memory pressure from the parallel fan-out — and is reported separately so it is
    // not mistaken for a policy finding (its captured output is usually empty).
    let mut violations: Vec<PathBuf> = Vec::new();
    let mut abnormal: Vec<PathBuf> = Vec::new();
    for (unit, output) in units.iter().zip(&outputs) {
        if output.status.success() {
            continue;
        }
        if output.status.code().is_some() {
            violations.push(unit.dir.clone());
        } else {
            abnormal.push(unit.dir.clone());
        }
    }

    if violations.is_empty() && abnormal.is_empty() {
        println!("xtask deny: all units passed.");
        return true;
    }

    if !violations.is_empty() {
        eprintln!("\nxtask deny: FAILED — the following units had cargo-deny violations:");
        for f in &violations {
            eprintln!("  {f:?}");
        }
    }
    if !abnormal.is_empty() {
        eprintln!(
            "\nxtask deny: FAILED — the following units terminated abnormally \
             (killed by signal, not a cargo-deny violation — likely resource pressure \
             from the parallel fan-out):"
        );
        for f in &abnormal {
            eprintln!("  {f:?}");
        }
    }
    eprintln!("\n{} unit(s) failed.", violations.len() + abnormal.len());
    false
}

/// Write a child process's captured stdout then stderr to the parent's streams as
/// raw bytes. cargo-deny may emit non-UTF-8; never decode it.
fn dump_child_output(output: &Output) {
    io::stdout()
        .write_all(&output.stdout)
        .expect("write to stdout failed");
    io::stderr()
        .write_all(&output.stderr)
        .expect("write to stderr failed");
}

/// Fetch the advisory database once from `repo_root`. Returns true on success.
/// A non-zero exit fails the whole deny run (no fallback to a stale DB) per fail-fast.
fn prefetch_advisory_db(repo_root: &Path, deny_config_str: &str) -> bool {
    let output = Command::new("cargo")
        .args(["deny", "fetch", "db", "--config", deny_config_str])
        .current_dir(repo_root)
        .output()
        .unwrap_or_else(|e| panic!("xtask deny: failed to spawn `cargo deny fetch db`: {e}"));

    if !output.status.success() {
        dump_child_output(&output);
        eprintln!("xtask deny: FAILED — could not fetch advisory database");
        return false;
    }
    true
}

/// Run `f` over every item concurrently (one scoped thread each) and collect the
/// results in input order. Handles are joined in input order; as each completes,
/// `on_result` is invoked with that item and its result (still inside the scope, so
/// slower workers are still running) — this lets callers stream a completed prefix
/// progressively. A worker panic is re-raised on join, so nothing is silently
/// dropped. N is small and fixed here, so no bounded pool is used.
fn run_parallel<U, T, F, G>(items: &[U], f: F, mut on_result: G) -> Vec<T>
where
    U: Sync,
    T: Send,
    F: Fn(&U) -> T + Sync,
    G: FnMut(&U, &T),
{
    std::thread::scope(|scope| {
        let f = &f;
        let handles: Vec<_> = items
            .iter()
            .map(|item| scope.spawn(move || f(item)))
            .collect();
        items
            .iter()
            .zip(handles)
            .map(|(item, h)| {
                let value = h
                    .join()
                    .unwrap_or_else(|payload| std::panic::resume_unwind(payload));
                on_result(item, &value);
                value
            })
            .collect()
    })
}

/// Assert that cargo-deny is available. Panics with an actionable remediation message if absent.
/// Probes once (the binary is the same for every unit, unlike clippy whose toolchain can differ).
/// Mirrors `assert_clippy_available` in lint.rs.
fn assert_cargo_deny_available() {
    let output = Command::new("cargo")
        .args(["deny", "--version"])
        .output()
        .unwrap_or_else(|e| {
            // Spawning `cargo` itself failed — cargo not on PATH or OS error.
            // Do NOT emit the cargo-deny install instruction here; that is the wrong fix.
            panic!(
                "xtask deny: failed to spawn `cargo` to probe cargo-deny availability: {e}\n\
                 Ensure `cargo` is on PATH."
            )
        });

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!(
            "cargo-deny not found; install with: cargo install --locked cargo-deny\n\
             cargo stderr: {stderr}"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn run_parallel_preserves_input_order_and_runs_all() {
        let items = vec![0usize, 1, 2, 3, 4];
        // Every worker must reach the barrier before any may proceed. This proves real
        // concurrency deterministically (no timing margins): a serial implementation
        // could never release the barrier and would deadlock here rather than pass.
        let barrier = Barrier::new(items.len());
        let ran = AtomicUsize::new(0);

        let results = run_parallel(
            &items,
            |&i| {
                barrier.wait();
                ran.fetch_add(1, Ordering::SeqCst);
                i * 10
            },
            |_, _| {},
        );

        // Results are returned in input order regardless of completion order.
        assert_eq!(results, vec![0, 10, 20, 30, 40]);
        // Every item ran exactly once.
        assert_eq!(ran.load(Ordering::SeqCst), items.len());
    }
}
