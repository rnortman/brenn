//! Periodic poll loop.
//!
//! One task, wakes every `poll_interval`, fans out a `SyncTrigger::Poll`
//! for every unique remote. Per-remote Mutex (inside the reactor) handles
//! the actual serialization; the poller only cares about cadence.

use std::time::Duration;

use tokio::sync::mpsc;

use super::SyncTrigger;

/// Run the poll loop until the trigger channel closes.
pub async fn poll_loop(remotes: Vec<String>, interval: Duration, tx: mpsc::Sender<SyncTrigger>) {
    // Skip the first tick — cold-start triggers (fired from `start()`)
    // already covered this moment. Sleep first, then fire every tick.
    // This avoids a double-poll right after boot.
    loop {
        tokio::time::sleep(interval).await;
        for remote in &remotes {
            // try_send not blocking — if the channel is full (very unlikely
            // given capacity == 16 * num_remotes) we drop and the next tick
            // catches up. The design's debounce contract permits coalescing.
            if let Err(e) = tx.try_send(SyncTrigger::Poll {
                remote: remote.clone(),
            }) {
                match e {
                    mpsc::error::TrySendError::Full(_) => {
                        tracing::warn!(remote = %remote, "poll trigger dropped — channel full");
                    }
                    mpsc::error::TrySendError::Closed(_) => {
                        tracing::info!("poll loop exiting: channel closed");
                        return;
                    }
                }
            }
        }
    }
}
