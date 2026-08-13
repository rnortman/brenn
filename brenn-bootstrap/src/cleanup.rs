//! Stale podman container cleanup on startup.

use tracing::info;
use tracing::warn;

/// Build the `--filter` args used by `cleanup_stale_containers`.
///
/// Returns `["label=brenn-managed"]` — label only, so the sweep sees running
/// containers as well as stopped ones. Rootless podman state is per-user and
/// container names are per-conversation, so **one brenn instance per Unix user**
/// is a standing constraint of the deployment model; every brenn-managed
/// container visible at startup therefore belongs to a previous life of this
/// instance and is stale.
pub(crate) fn cleanup_filter_args() -> [&'static str; 1] {
    ["label=brenn-managed"]
}

/// Remove any stale podman containers left by a previous life of this brenn
/// instance.
///
/// A host SIGKILL or an OOM leaves no chance to run session teardown, so both
/// stopped containers (whose `--rm` never fired) and running ones (whose PID 1
/// never saw stdin close) can survive. This runs at startup, before anything is
/// spawned, and removes them all.
///
/// Every removal is logged at `warn!`: debris after a crash is expected, but an
/// operator should still see that it happened.
pub(crate) async fn cleanup_stale_containers() {
    use tokio::process::Command;

    // List every container carrying the brenn-managed label, running or not.
    // If podman isn't available or fails, panic — containerized apps can't function
    // without a working podman installation.
    let filters = cleanup_filter_args();
    let output = Command::new("podman")
        .args(["ps", "-a", "--filter", filters[0], "--format", "{{.Names}}"])
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

    warn!(
        count = names.len(),
        containers = %names.join(" "),
        "removing stale podman containers left by a previous run"
    );
    // Batch into one call to eliminate N-1 fork/exec overhead at startup.
    let result = Command::new("podman")
        .args(brenn_lib::config::container_rm_args(&names))
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

    /// Cleanup filter must be the `brenn-managed` label alone — not a
    /// per-instance `brenn-instance=<id>` shape, and not paired with a status
    /// filter.
    ///
    /// A `status=` filter would put running orphans back out of reach, which is
    /// exactly the case a crash leaves behind. Sweeping running containers is
    /// safe only because one brenn instance per Unix user is a standing
    /// constraint (rootless podman state is per-user, and container names are
    /// per-conversation, so two instances would collide on names anyway).
    ///
    /// Pairing note: spawned containers carry `--label brenn-managed=true`
    /// (`brenn-cc/src/session/mod.rs`). The cleanup filter uses `label=brenn-managed`
    /// (key-only form), which matches any value of that label key per podman semantics.
    /// This is intentional — the filter matches on label presence, not value.
    #[test]
    fn cleanup_filter_is_brenn_managed_label_only() {
        let args = cleanup_filter_args();
        assert_eq!(args, ["label=brenn-managed"]);
        // Regression guard: must never revert to a name-prefix filter, which
        // would risk matching containers from other deployments.
        assert!(
            !args[0].starts_with("name="),
            "cleanup filter must not be a name filter — must be label-based"
        );
        // Regression guard: a status filter must not reappear — running orphans
        // are precisely what this sweep exists to remove.
        assert!(
            !args.iter().any(|a| a.starts_with("status=")),
            "cleanup filter must not constrain container status"
        );
    }
}
