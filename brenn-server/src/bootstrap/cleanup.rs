//! Stale podman container cleanup on startup.

use tracing::info;
use tracing::warn;

/// Build the `--filter` args used by `cleanup_stale_containers`.
///
/// Returns `["label=brenn-managed", "status=exited"]`. Both filters are
/// applied together so cleanup only touches stopped containers that brenn
/// itself spawned — never running containers from any deployment.
pub(crate) fn cleanup_filter_args() -> [&'static str; 2] {
    ["label=brenn-managed", "status=exited"]
}

/// Remove any stale podman containers from previous crashes of this
/// brenn instance.
///
/// When `kill_on_drop(true)` sends SIGKILL to a `podman run` process, the
/// `--rm` flag may not get a chance to fire, leaving stopped containers
/// behind. This runs on startup to clean them up.
///
/// Only stopped containers carrying `brenn-managed=true` are touched — running
/// containers from any deployment are never affected.
pub(crate) async fn cleanup_stale_containers() {
    use tokio::process::Command;

    // List all stopped containers carrying the brenn-managed label.
    // If podman isn't available or fails, panic — containerized apps can't function
    // without a working podman installation.
    let filters = cleanup_filter_args();
    let output = Command::new("podman")
        .args([
            "ps",
            "-a",
            "--filter",
            filters[0],
            "--filter",
            filters[1],
            "--format",
            "{{.Names}}",
        ])
        .output()
        .await
        .unwrap_or_else(|e| {
            panic!(
                "failed to run `podman ps` — is podman installed? Container apps require it: {e}"
            )
        });

    assert!(
        output.status.success(),
        "podman ps failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );

    let names: Vec<&str> = std::str::from_utf8(&output.stdout)
        .expect("podman ps output is not valid UTF-8")
        .lines()
        .filter(|l| !l.is_empty())
        .collect();

    if names.is_empty() {
        return;
    }

    info!(count = names.len(), "removing stale podman containers");
    // Batch all names into a single `podman rm -f name1 name2 ...` call to
    // eliminate N-1 fork/exec overhead at startup. `podman rm` accepts
    // multiple names and is idempotent with `-f`.
    let result = Command::new("podman")
        .arg("rm")
        .arg("-f")
        .args(&names)
        .output()
        .await;
    match result {
        Ok(o) if o.status.success() => {
            info!(count = names.len(), "removed stale containers");
        }
        Ok(o) => {
            warn!(
                count = names.len(),
                stderr = %String::from_utf8_lossy(&o.stderr).trim(),
                "failed to remove some stale containers"
            );
        }
        Err(e) => {
            warn!(count = names.len(), error = %e, "failed to remove stale containers");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Cleanup filter must use `brenn-managed` label + `status=exited`, not the old
    /// per-instance `brenn-instance=<id>` shape. The `status=exited` filter is the
    /// critical safety property: running containers from any deployment are never touched.
    ///
    /// Pairing note: spawned containers carry `--label brenn-managed=true`
    /// (`brenn-cc/src/session/mod.rs`). The cleanup filter uses `label=brenn-managed`
    /// (key-only form), which matches any value of that label key per podman semantics.
    /// This is intentional — the filter matches on label presence, not value.
    #[test]
    fn cleanup_filter_uses_brenn_managed_and_status_exited() {
        let args = cleanup_filter_args();
        assert_eq!(args[0], "label=brenn-managed");
        assert_eq!(args[1], "status=exited");
        // Regression guard: must never revert to a name-prefix filter, which
        // would risk matching containers from other deployments.
        assert!(
            !args[0].starts_with("name="),
            "cleanup filter must not be a name filter — must be label-based"
        );
    }
}
