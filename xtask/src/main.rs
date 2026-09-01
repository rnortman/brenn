// xtask: policy runner for brenn. Subcommands: guard, policy-parity, check-wit, deny.
// Invoked via `bazel run //xtask -- <subcommand>`; the guards and the WIT gates also
// run as Bazel test targets.

mod build_id_guard;
mod check_wit;
mod deny;
mod discover;
mod file_set;
mod generated_bindings_guard;
mod git_spawn_guard;
mod help_guard;
mod policy_parity;
mod removal_guard;
mod sync_guard;
mod test_target_guard;
mod testutils_deps_guard;
mod workspace_guard;
mod world_sig;

use std::path::{Path, PathBuf};

const GUARD_USAGE: &str = "guard [--root <dir>] [--manifest <file>]";
const PARITY_USAGE: &str = "policy-parity [--root <dir>] --manifest <file>";
const CHECK_WIT_USAGE: &str = "check-wit [--root <dir>]";
const DENY_USAGE: &str = "deny [--root <dir>]";
const SUBCOMMANDS: &str = "guard | policy-parity | check-wit | deny";

/// Each guard runs to completion so one failure does not hide the rest.
fn run_guards(root: &Path, files: &[PathBuf]) -> bool {
    let removal_ok = removal_guard::run_removal_guard(root, files);
    let spawn_ok = git_spawn_guard::run_git_spawn_guard(root, files);
    let help_ok = help_guard::run_help_guard(root, files);
    let build_id_ok = build_id_guard::run_build_id_guard(root, files);
    let sync_ok = sync_guard::run_sync_guard(root, files);
    let workspace_ok = workspace_guard::run_workspace_guard(root, files);
    let test_target_ok = test_target_guard::run_test_target_guard(root, files);
    let testutils_deps_ok = testutils_deps_guard::run_testutils_deps_guard(root, files);
    let bindings_ok = generated_bindings_guard::run_generated_bindings_guard(files);
    removal_ok
        && spawn_ok
        && help_ok
        && build_id_ok
        && sync_ok
        && workspace_ok
        && test_target_ok
        && testutils_deps_ok
        && bindings_ok
}

/// `--manifest` names a listing of repo-root-relative paths; without it the set
/// is the tracked tree. Never both, and never a guess: an unreadable manifest
/// is a failure, not a reason to fall back to git.
struct GuardArgs {
    root: PathBuf,
    manifest: Option<PathBuf>,
}

/// Whether the subcommand being parsed reads `parsed.manifest`. A subcommand
/// that does not must reject the flag: accepting an option and then ignoring it
/// runs clean while doing something other than what the invocation asked for.
#[derive(Clone, Copy, PartialEq)]
enum Manifest {
    Accepted,
    Rejected,
}

fn parse_guard_args(
    args: &mut impl Iterator<Item = String>,
    default_root: &Path,
    usage: &str,
    manifest_policy: Manifest,
) -> GuardArgs {
    let mut parsed = GuardArgs {
        root: default_root.to_path_buf(),
        manifest: None,
    };
    while let Some(flag) = args.next() {
        let value = args.next().unwrap_or_else(|| {
            eprintln!("xtask: {flag} needs a value");
            eprintln!("Usage: xtask {usage}");
            std::process::exit(2);
        });
        match flag.as_str() {
            "--root" => parsed.root = PathBuf::from(value),
            "--manifest" if manifest_policy == Manifest::Accepted => {
                parsed.manifest = Some(PathBuf::from(value))
            }
            other => {
                eprintln!("xtask: unknown option {other:?}");
                eprintln!("Usage: xtask {usage}");
                std::process::exit(2);
            }
        }
    }
    parsed
}

fn main() {
    let mut args = std::env::args().skip(1);
    let subcommand = args.next().unwrap_or_else(|| {
        eprintln!("Usage: xtask <subcommand> [args]");
        eprintln!("Subcommands: {SUBCOMMANDS}");
        std::process::exit(2);
    });

    // The default root, for an invocation that states none: the parent of the
    // crate dir the binary was compiled from. Under `bazel run` the binary
    // starts in its runfiles tree rather than the workspace, so every caller
    // that needs the real tree passes `--root`.
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir
        .parent()
        .unwrap_or_else(|| panic!("xtask: CARGO_MANIFEST_DIR {:?} has no parent", manifest_dir))
        .to_path_buf();

    let ok = match subcommand.as_str() {
        "guard" => {
            let parsed = parse_guard_args(&mut args, &repo_root, GUARD_USAGE, Manifest::Accepted);
            let files = match &parsed.manifest {
                Some(manifest) => file_set::from_manifest(manifest),
                None => file_set::from_git(&parsed.root),
            };
            run_guards(&parsed.root, &files)
        }
        // Outside the sandbox by necessity: it needs both the manifest the
        // build produced and the git listing the build cannot see.
        "policy-parity" => {
            let parsed = parse_guard_args(&mut args, &repo_root, PARITY_USAGE, Manifest::Accepted);
            let manifest = parsed.manifest.unwrap_or_else(|| {
                eprintln!("xtask: policy-parity needs the manifest to compare against");
                eprintln!("Usage: xtask {PARITY_USAGE}");
                std::process::exit(2);
            });
            policy_parity::run_policy_parity(&parsed.root, &manifest)
        }
        "check-wit" => {
            let parsed =
                parse_guard_args(&mut args, &repo_root, CHECK_WIT_USAGE, Manifest::Rejected);
            check_wit::run_check_wit(&parsed.root)
        }
        "deny" => {
            let parsed = parse_guard_args(&mut args, &repo_root, DENY_USAGE, Manifest::Rejected);
            deny::run_deny(&parsed.root)
        }
        other => {
            eprintln!("xtask: unknown subcommand {other:?}");
            eprintln!("Subcommands: {SUBCOMMANDS}");
            std::process::exit(2);
        }
    };

    if !ok {
        std::process::exit(1);
    }
}
