//! The vocabulary of keeping a managed clone fresh: what a clone is, and the
//! channel that asks for a sync cycle over it.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::warn;

/// Why a sync cycle should run.
///
/// Both variants converge on the same reaction pipeline; they differ only in
/// whether they bypass debounce (Push) or go through it (Poll).
#[derive(Debug, Clone)]
pub enum SyncTrigger {
    /// Periodic poll tick for one remote. Debounced.
    Poll { remote: String },
    /// An agent just mutated a clone via an MCP git tool (`GitRepoPull` or
    /// `GitRepoCommitAndPush`). Bypasses debounce. `acting_conversation_id`
    /// is suppressed from the notification fan-out so the invoking bridge
    /// doesn't get a `repo_sync:pulled` for its own change.
    Push {
        remote: String,
        acting_conversation_id: Option<i64>,
    },
}

/// The sending half of the trigger channel. Dropping it does not kill the
/// manager task, whose receiver lives inside the task.
#[derive(Clone)]
pub struct SyncTriggerSender {
    tx: mpsc::Sender<SyncTrigger>,
    /// Clone slug → remote URL. Built once at startup from the config.
    slug_to_remote: Arc<HashMap<String, String>>,
}

impl SyncTriggerSender {
    /// Build a sender over an existing trigger channel.
    pub fn new(
        tx: mpsc::Sender<SyncTrigger>,
        slug_to_remote: Arc<HashMap<String, String>>,
    ) -> Self {
        Self { tx, slug_to_remote }
    }

    /// Try to send a trigger. Non-blocking; a full channel drops the
    /// trigger with a warn — triggers are coalescable and polling will
    /// catch up on the next tick. Returns `true` on success, `false` if
    /// the channel was full and the trigger was dropped.
    pub fn try_send(&self, trigger: SyncTrigger) -> bool {
        match self.tx.try_send(trigger) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(t)) => {
                warn!(?t, "repo_sync trigger channel full — dropping");
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                panic!(
                    "repo_sync trigger channel closed — repo_sync manager task died; \
                     process cannot continue safely"
                );
            }
        }
    }

    /// Emit a `SyncTrigger::Push` for the clone identified by `slug`.
    /// Unknown slug is warned and dropped — it would indicate a
    /// config/code mismatch.
    pub fn push_for_slug(&self, slug: &str, acting_conversation_id: Option<i64>) {
        let Some(remote) = self.slug_to_remote.get(slug) else {
            warn!(
                slug = %slug,
                "repo_sync: push_for_slug on unknown slug — no trigger emitted"
            );
            return;
        };
        // Discard intentional: Full case is already logged by try_send as a
        // warn. Push triggers are coalescable; the poll loop catches up.
        let _delivered = self.try_send(SyncTrigger::Push {
            remote: remote.clone(),
            acting_conversation_id,
        });
    }
}

/// Per-clone static info, built once from the resolved config.
#[derive(Debug, Clone)]
pub struct CloneInfo {
    pub slug: String,
    pub host_path: PathBuf,
    pub remote: String,
    /// `true` if *any* mount of this clone has `auto_pull = true`. A clone
    /// that is not sync-enabled is never polled.
    pub sync_enabled: bool,
    /// Apps that mount this clone (any access, any auto_pull).
    /// Every active conversation of an app in this set is a consumer
    /// for notification purposes.
    pub consumer_apps: HashSet<String>,
    /// Apps whose mount of this clone is the declared primary (the
    /// primary-pool). Conflict notifications go only to consumers in
    /// conversations of these apps.
    ///
    /// Empty set → RO-only clone; conflicts route to `AlertDispatcher`
    /// instead of LLM events.
    pub primary_apps: HashSet<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_for_slug_emits_trigger_for_known_slug() {
        let (tx, mut rx) = mpsc::channel::<SyncTrigger>(4);
        let slug_to_remote = Arc::new(HashMap::from([(
            "src-x".to_string(),
            "ssh://example/x.git".to_string(),
        )]));
        let sender = SyncTriggerSender::new(tx, slug_to_remote);
        sender.push_for_slug("src-x", Some(42));
        match rx.try_recv() {
            Ok(SyncTrigger::Push {
                remote,
                acting_conversation_id,
            }) => {
                assert_eq!(remote, "ssh://example/x.git");
                assert_eq!(acting_conversation_id, Some(42));
            }
            other => panic!("expected Push, got {other:?}"),
        }
    }

    #[test]
    fn push_for_slug_drops_unknown_slug_without_panicking() {
        let (tx, mut rx) = mpsc::channel::<SyncTrigger>(4);
        let slug_to_remote = Arc::new(HashMap::new());
        let sender = SyncTriggerSender::new(tx, slug_to_remote);
        sender.push_for_slug("unknown", Some(1));
        assert!(rx.try_recv().is_err(), "unknown slug should emit nothing");
    }

    #[test]
    fn try_send_returns_false_when_channel_full() {
        let (tx, _rx) = mpsc::channel::<SyncTrigger>(1);
        let slug_to_remote = Arc::new(HashMap::new());
        let sender = SyncTriggerSender::new(tx.clone(), slug_to_remote);
        tx.try_send(SyncTrigger::Poll {
            remote: "ssh://example/x.git".to_string(),
        })
        .expect("first send into empty channel must succeed");
        let delivered = sender.try_send(SyncTrigger::Poll {
            remote: "ssh://example/x.git".to_string(),
        });
        assert!(!delivered, "try_send must return false when channel full");
    }

    #[test]
    fn push_for_slug_returns_normally_when_channel_full() {
        let (tx, mut rx) = mpsc::channel::<SyncTrigger>(1);
        let slug_to_remote = Arc::new(HashMap::from([(
            "src-x".to_string(),
            "ssh://example/x.git".to_string(),
        )]));
        let sender = SyncTriggerSender::new(tx.clone(), slug_to_remote);
        tx.try_send(SyncTrigger::Poll {
            remote: "ssh://example/x.git".to_string(),
        })
        .expect("pre-fill must succeed");
        sender.push_for_slug("src-x", None);
        let _ = rx.try_recv().expect("pre-filled item must be present");
        assert!(
            rx.try_recv().is_err(),
            "channel must contain no extra item after dropped push"
        );
    }
}
