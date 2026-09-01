//! Shared test helpers for `brenn-graf` tests.

use std::path::PathBuf;

/// Build a minimal `AppConfig` for tests.
///
/// `mounts` — pass `vec![]` when no mount is needed; supply mounts for
/// integration tests that exercise per-repo dispatch.
///
/// `create_state_dir` — pass `true` when the test code reads from or writes
/// to the state directory (e.g. lib.rs integration tests); `false` when
/// `run_graf_raw` / subprocess tests never touch it and the side-effect
/// would be noise.
pub(crate) fn test_app_config(
    working_dir: PathBuf,
    mounts: Vec<brenn_lib::config::ResolvedMount>,
    create_state_dir: bool,
) -> brenn_lib::config::AppConfig {
    let state_dir = working_dir.join(".brenn-state");
    if create_state_dir {
        std::fs::create_dir_all(&state_dir).unwrap();
    }
    brenn_lib::config::AppConfig {
        working_dir,
        model: "sonnet".into(),
        mounts,
        state_dir,
        ..brenn_lib::config::test_app_config("test")
    }
}
