// xtask: policy runner for brenn. Subcommands: lint, guard, policy-parity, check-wit,
// check, deny, test.
// Invoked via `cargo run -p xtask -- <subcommand>` or `cargo xtask <subcommand>`.

mod build_id_guard;
mod check_wit;
mod deny;
mod discover;
mod file_set;
mod git_spawn_guard;
mod guard;
mod help_guard;
mod lint;
mod parallel;
mod policy;
mod policy_parity;
mod removal_guard;
mod sync_guard;
mod test_run;
mod world_sig;

use std::path::{Path, PathBuf};

const GUARD_USAGE: &str = "guard [--root <dir>] [--manifest <file>]";
const PARITY_USAGE: &str = "policy-parity [--root <dir>] --manifest <file>";

/// Each guard runs to completion so one failure does not hide the rest.
fn run_guards(root: &Path, files: &[PathBuf]) -> bool {
    let units_ok = guard::run_guard(root, files);
    let removal_ok = removal_guard::run_removal_guard(root, files);
    let spawn_ok = git_spawn_guard::run_git_spawn_guard(root, files);
    let help_ok = help_guard::run_help_guard(root, files);
    let build_id_ok = build_id_guard::run_build_id_guard(root, files);
    let sync_ok = sync_guard::run_sync_guard(root, files);
    units_ok && removal_ok && spawn_ok && help_ok && build_id_ok && sync_ok
}

/// `--manifest` names a listing of repo-root-relative paths; without it the set
/// is the tracked tree. Never both, and never a guess: an unreadable manifest
/// is a failure, not a reason to fall back to git.
struct GuardArgs {
    root: PathBuf,
    manifest: Option<PathBuf>,
}

fn parse_guard_args(
    args: &mut impl Iterator<Item = String>,
    default_root: &Path,
    usage: &str,
) -> GuardArgs {
    let mut parsed = GuardArgs {
        root: default_root.to_path_buf(),
        manifest: None,
    };
    while let Some(flag) = args.next() {
        let value = args.next().unwrap_or_else(|| {
            eprintln!("xtask: {flag} needs a value");
            eprintln!("Usage: cargo xtask {usage}");
            std::process::exit(2);
        });
        match flag.as_str() {
            "--root" => parsed.root = PathBuf::from(value),
            "--manifest" => parsed.manifest = Some(PathBuf::from(value)),
            other => {
                eprintln!("xtask: unknown option {other:?}");
                eprintln!("Usage: cargo xtask {usage}");
                std::process::exit(2);
            }
        }
    }
    parsed
}

fn main() {
    let mut args = std::env::args().skip(1);
    let subcommand = args.next().unwrap_or_else(|| {
        eprintln!("Usage: cargo xtask <subcommand> [args]");
        eprintln!("Subcommands: lint [<path>] | {GUARD_USAGE} | {PARITY_USAGE} | check-wit | check | deny | test");
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
        "guard" => {
            let parsed = parse_guard_args(&mut args, &repo_root, GUARD_USAGE);
            let files = match &parsed.manifest {
                Some(manifest) => file_set::from_manifest(manifest),
                None => file_set::from_git(&parsed.root),
            };
            run_guards(&parsed.root, &files)
        }
        // Outside the sandbox by necessity: it needs both the manifest the
        // build produced and the git listing the build cannot see.
        "policy-parity" => {
            let parsed = parse_guard_args(&mut args, &repo_root, PARITY_USAGE);
            let manifest = parsed.manifest.unwrap_or_else(|| {
                eprintln!("xtask: policy-parity needs the manifest to compare against");
                eprintln!("Usage: cargo xtask {PARITY_USAGE}");
                std::process::exit(2);
            });
            policy_parity::run_policy_parity(&parsed.root, &manifest)
        }
        "check-wit" => check_wit::run_check_wit(&repo_root),
        "check" => {
            // guard, lint, and check-wit run across a bounded worker pool
            // (BRENN_CHECK_JOBS; 0/1 = fully serial in this order). Each lane runs to
            // completion — no early abort — so all failures are reported; run_jobs
            // re-panics with lane attribution if any lane panics.
            //
            // Invariant: no check lane — and no `make check` step running concurrently
            // with `xtask check` — writes anywhere under the repo working tree. The only
            // writes are cargo target dirs (excluded from discovery walks by component
            // name) and out-of-repo scratch. Under that invariant every lane is
            // tree-read-only, so all four run fully concurrently without colliding:
            //   - guard: discovery, allowlist, and tracked-source reads (no cargo).
            //   - lint-root: root clippy; uses the root `target/` dir only.
            //   - lint-wasm: wasm clippy; shares WASM_COMPONENTS_TARGET (cargo serializes
            //     via its build-dir lock) and reads the committed `bindings.rs` files.
            //   - check-wit: reads the final artifacts and the committed `bindings.rs`,
            //     regenerating into out-of-repo scratch for the drift compare — it
            //     touches no tracked file.
            let jobs = parallel::check_jobs();
            let lanes: Vec<parallel::NamedTask> = vec![
                ("guard", {
                    let r = repo_root.clone();
                    Box::new(move || {
                        let files = file_set::from_git(&r);
                        run_guards(&r, &files)
                    })
                }),
                ("lint-root", {
                    let r = repo_root.clone();
                    Box::new(move || lint::lint_root(&r))
                }),
                ("lint-wasm", {
                    let r = repo_root.clone();
                    Box::new(move || lint::lint_wasm(&r))
                }),
                ("check-wit", {
                    let r = repo_root.clone();
                    Box::new(move || check_wit::run_check_wit(&r))
                }),
            ];
            parallel::run_jobs(jobs, lanes)
        }
        "deny" => deny::run_deny(&repo_root),
        "test" => test_run::run_test(&repo_root),
        other => {
            eprintln!("xtask: unknown subcommand {other:?}");
            eprintln!(
                "Subcommands: lint [<path>] | {GUARD_USAGE} | {PARITY_USAGE} | check-wit | check | deny | test"
            );
            std::process::exit(2);
        }
    };

    if !ok {
        std::process::exit(1);
    }
}
