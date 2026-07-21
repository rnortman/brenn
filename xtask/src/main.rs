// xtask: policy runner for brenn. Subcommands: lint, guard, check-wit, check, deny, test.
// Invoked via `cargo run -p xtask -- <subcommand>` or `cargo xtask <subcommand>`.
// See design §2.

mod check_wit;
mod deny;
mod discover;
mod guard;
mod lint;
mod parallel;
mod policy;
mod removal_guard;
mod test_run;

fn main() {
    let mut args = std::env::args().skip(1);
    let subcommand = args.next().unwrap_or_else(|| {
        eprintln!("Usage: cargo xtask <subcommand> [args]");
        eprintln!("Subcommands: lint [<path>] | guard | check-wit | check | deny | test");
        std::process::exit(2);
    });

    // Resolve repo root from CARGO_MANIFEST_DIR (set by cargo when running the xtask binary).
    // xtask/ is in the repo root, so repo root is its parent.
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir
        .parent()
        .unwrap_or_else(|| panic!("xtask: CARGO_MANIFEST_DIR {:?} has no parent", manifest_dir))
        .to_path_buf();

    let ok = match subcommand.as_str() {
        "lint" => {
            let path_arg = args.next();
            match path_arg {
                None => lint::lint_all(&repo_root),
                Some(p) => lint::lint_one(&repo_root, std::path::Path::new(&p)),
            }
        }
        "guard" => guard::run_guard(&repo_root) & removal_guard::run_removal_guard(&repo_root),
        "check-wit" => check_wit::run_check_wit(&repo_root),
        "check" => {
            // guard, lint, and check-wit run across a bounded worker pool
            // (BRENN_CHECK_JOBS; 0/1 = fully serial in this order). Each lane runs to
            // completion — no early abort — so all failures are reported.
            //
            // Lanes are grouped by shared mutable resource so concurrent lanes never
            // collide:
            //   - guard: pure discovery, allowlist, and tracked-source reads
            //     (no cargo, no writes).
            //   - root clippy: uses the root `target/` dir only.
            //   - wasm clippy then check-wit: both touch WASM_COMPONENTS_TARGET and the
            //     component `bindings.rs` files (check-wit regenerates them), so they
            //     share one serial lane.
            let jobs = parallel::check_jobs();
            let lanes: Vec<Box<dyn FnOnce() -> bool + Send>> = vec![
                {
                    let r = repo_root.clone();
                    Box::new(move || {
                        let units_ok = guard::run_guard(&r);
                        let removal_ok = removal_guard::run_removal_guard(&r);
                        units_ok && removal_ok
                    })
                },
                {
                    let r = repo_root.clone();
                    Box::new(move || lint::lint_root(&r))
                },
                {
                    let r = repo_root.clone();
                    Box::new(move || {
                        let wasm_ok = lint::lint_wasm(&r);
                        let wit_ok = check_wit::run_check_wit(&r);
                        wasm_ok && wit_ok
                    })
                },
            ];
            parallel::run_jobs(jobs, lanes)
        }
        "deny" => deny::run_deny(&repo_root),
        "test" => test_run::run_test(&repo_root),
        other => {
            eprintln!("xtask: unknown subcommand {other:?}");
            eprintln!("Subcommands: lint [<path>] | guard | check-wit | check | deny | test");
            std::process::exit(2);
        }
    };

    if !ok {
        std::process::exit(1);
    }
}
