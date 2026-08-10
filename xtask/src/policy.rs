// Policy: per-Kind lint commands. THE single source of -D warnings for every kind.
// See design §4.1, §2.3.

use crate::discover::Kind;
use std::path::Path;

/// The shared WASM components target dir (matches Makefile WASM_COMPONENTS_TARGET).
pub fn wasm_components_target(repo_root: &Path) -> std::path::PathBuf {
    // Mirrors: WASM_COMPONENTS_TARGET ?= $(abspath brenn-wasm/components/target)
    // Honour an env override (CI may set WASM_COMPONENTS_TARGET).
    if let Ok(v) = std::env::var("WASM_COMPONENTS_TARGET") {
        return std::path::PathBuf::from(v);
    }
    repo_root
        .join("brenn-wasm")
        .join("components")
        .join("target")
}

/// Returns the `cargo (component) clippy` command for the given kind.
///
/// The return value is (program, args) — the program to exec and its argument list.
/// CARGO_TARGET_DIR override for WASM kinds is applied by the caller (lint.rs) using
/// the `wasm_components_target` helper.
///
/// This function is THE single source of the -D warnings policy. Every kind's clippy
/// invocation is here and nowhere else. Adding a new kind adds exactly one arm.
pub fn lint_command_for(kind: &Kind) -> (&'static str, Vec<&'static str>) {
    match kind {
        Kind::RootWorkspace => (
            "cargo",
            vec!["clippy", "--all-targets", "--", "-D", "warnings"],
        ),
        Kind::WasmWorkspace => {
            // No `cargo component clippy` exists; WIT-binding staleness is
            // checked separately by `xtask check-wit`.
            (
                "cargo",
                vec![
                    "clippy",
                    "--workspace",
                    "--target",
                    "wasm32-unknown-unknown",
                    "--",
                    "-D",
                    "warnings",
                ],
            )
        }
    }
}

/// Assert that every Kind's lint command contains "clippy" and "-D warnings".
/// This is a structural invariant test — no kind is exempt.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::discover::Kind;

    #[test]
    fn all_kinds_have_dwarnings() {
        for kind in &[Kind::RootWorkspace, Kind::WasmWorkspace] {
            let (prog, args) = lint_command_for(kind);
            let full: Vec<&str> = std::iter::once(prog).chain(args.iter().copied()).collect();
            let cmd_str = full.join(" ");
            assert!(
                cmd_str.contains("clippy"),
                "lint_command_for({kind:?}) does not contain 'clippy': {cmd_str}"
            );
            assert!(
                cmd_str.contains("-D warnings"),
                "lint_command_for({kind:?}) does not contain '-D warnings': {cmd_str}"
            );
        }
    }
}
