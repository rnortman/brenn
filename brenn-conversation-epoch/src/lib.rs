//! Conversation epochs at runtime.
//!
//! A singleton agent that names a `conversation_epoch` channel has a current
//! conversation only while that conversation was minted under the channel's
//! latest message. This crate holds what that latest message said, per agent.
//!
//! Nothing here decides anything. [`EpochGoal::apply`] takes what a publisher
//! said, checks that it is an epoch at all, and remembers it; the host reads
//! it back through [`EpochGoal::current`]. Deciding *when* a conversation is
//! over lives in a policy component on the other side of the channel.

use std::collections::BTreeMap;
use std::sync::RwLock;

use brenn_messaging::system::SystemParticipantSpec;
use brenn_obs::alerting::{AlertDispatcher, AlertSeverity};
use tracing::{info, warn};

/// Component name of the system participant that subscribes to every epoch
/// channel. Its bus identity is `system:conversation-epoch`.
pub const CONVERSATION_EPOCH_COMPONENT: &str = "conversation-epoch";

/// Longest accepted epoch, in bytes after trimming. An epoch is an opaque
/// identifier stored on every conversation row it mints, not a document.
pub const MAX_EPOCH_BYTES: usize = 64;

/// Why an epoch body was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EpochError {
    /// The body was empty, or nothing but whitespace.
    Empty,
    /// The trimmed body was this many bytes, more than [`MAX_EPOCH_BYTES`].
    TooLong(usize),
}

impl std::fmt::Display for EpochError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EpochError::Empty => f.write_str("empty epoch body"),
            EpochError::TooLong(n) => {
                write!(
                    f,
                    "epoch is {n} bytes; at most {MAX_EPOCH_BYTES} are accepted"
                )
            }
        }
    }
}

impl EpochError {
    /// A stable short tag for alert dedup, so one bad publisher pages once per
    /// `(agent, reason)` rather than once per publish.
    fn tag(&self) -> &'static str {
        match self {
            EpochError::Empty => "empty",
            EpochError::TooLong(_) => "too-long",
        }
    }
}

/// Read an epoch channel body as an epoch.
///
/// The whole doctype is the epoch: trimmed text, no JSON. That is the entire
/// contract an out-of-tree publisher has to meet.
pub fn accept(body: &str) -> Result<String, EpochError> {
    let epoch = body.trim();
    if epoch.is_empty() {
        return Err(EpochError::Empty);
    }
    if epoch.len() > MAX_EPOCH_BYTES {
        return Err(EpochError::TooLong(epoch.len()));
    }
    Ok(epoch.to_string())
}

/// The live epoch state: the epoch each agent's current conversation must
/// carry. Empty until an epoch is read off a channel — an agent whose
/// channel has never carried one has no epoch, and nothing is superseded
/// for it.
pub struct EpochGoal {
    /// Canonical epoch address → the agents bound to it. Several agents may
    /// share one channel and are then reset together.
    channel_apps: BTreeMap<String, Vec<String>>,
    /// App slug → the epoch last accepted for it.
    current: RwLock<BTreeMap<String, String>>,
    /// Where a refused body is reported.
    alerts: AlertDispatcher,
}

impl EpochGoal {
    /// Build the handle from each agent's epoch channel: `apps` maps app slug
    /// to canonical epoch address.
    pub fn new(apps: BTreeMap<String, String>, alerts: AlertDispatcher) -> Self {
        let mut channel_apps: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (slug, addr) in apps {
            channel_apps.entry(addr).or_default().push(slug);
        }
        Self {
            channel_apps,
            current: RwLock::new(BTreeMap::new()),
            alerts,
        }
    }

    /// The epoch `app_slug`'s current conversation must carry. `None` for an
    /// agent that names no epoch channel, or whose channel has carried none.
    pub fn current(&self, app_slug: &str) -> Option<String> {
        self.current
            .read()
            .expect("conversation-epoch: current epochs lock poisoned")
            .get(app_slug)
            .cloned()
    }

    /// Apply one epoch message to every agent bound to `addr`, returning the
    /// slugs whose epoch actually changed.
    ///
    /// A refusal alerts once per process per `(agent, reason)` and leaves the
    /// previous epoch standing.
    pub fn apply(&self, addr: &str, body: &str) -> Vec<String> {
        let Some(slugs) = self.channel_apps.get(addr) else {
            // The participant subscribes to exactly the declared epoch
            // channels, so a body from anywhere else is a wiring bug rather
            // than a publisher's mistake.
            panic!(
                "BUG: conversation-epoch received a message on {addr:?}, which no agent named as \
                 its epoch channel"
            )
        };
        let mut changed = Vec::new();
        match accept(body) {
            Ok(epoch) => {
                for slug in slugs {
                    let mut current = self
                        .current
                        .write()
                        .expect("conversation-epoch: current epochs lock poisoned");
                    let prev = current.insert(slug.clone(), epoch.clone());
                    if prev.as_deref() != Some(epoch.as_str()) {
                        info!(app = %slug, epoch = %epoch, channel = %addr, "conversation epoch changed");
                        changed.push(slug.clone());
                    }
                }
            }
            Err(err) => {
                for slug in slugs {
                    warn!(app = %slug, channel = %addr, "conversation epoch rejected: {err}");
                    self.alerts.alert_once_per_process(
                        AlertSeverity::Warning,
                        "Conversation epoch rejected".to_string(),
                        &format!("conversation-epoch:{slug}:{}", err.tag()),
                        format!(
                            "A message on {addr} was refused as a conversation epoch for agent \
                             {slug}: {err}. The agent keeps its current conversation. Whoever \
                             publishes this channel is sending something that is not an epoch."
                        ),
                    );
                }
            }
        }
        changed
    }
}

/// The `system:conversation-epoch` participant: subscribe-only, on exactly the
/// declared epoch channels.
///
/// # Panics
///
/// On an epoch address that is not `brenn:` (see
/// [`SystemParticipantSpec::subscribe_only_durable`]).
pub fn conversation_epoch_spec(epoch_addrs: &[String]) -> SystemParticipantSpec {
    SystemParticipantSpec::subscribe_only_durable(CONVERSATION_EPOCH_COMPONENT, epoch_addrs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    fn epoch_handle(apps: &[(&str, &str)]) -> (EpochGoal, tokio::task::JoinHandle<()>) {
        let (alerts, handle) = brenn_obs::alerting::noop_alert_dispatcher();
        let apps = apps
            .iter()
            .map(|(slug, addr)| (slug.to_string(), addr.to_string()))
            .collect();
        (EpochGoal::new(apps, alerts), handle)
    }

    #[test]
    fn accept_trims_surrounding_whitespace() {
        assert_eq!(accept("  7 \n"), Ok("7".into()));
    }

    #[test]
    fn accept_refuses_an_empty_body() {
        assert_eq!(accept(""), Err(EpochError::Empty));
        assert_eq!(accept(" \t\n"), Err(EpochError::Empty));
    }

    #[test]
    fn accept_takes_exactly_the_maximum_and_refuses_one_more() {
        let max = "a".repeat(MAX_EPOCH_BYTES);
        let over = "a".repeat(MAX_EPOCH_BYTES + 1);
        assert_eq!(accept(&max), Ok(max.clone()));
        assert_eq!(accept(&over), Err(EpochError::TooLong(65)));
        assert_eq!(accept(&format!("  {over}  ")), Err(EpochError::TooLong(65)));
        // Length is measured after trimming.
        assert_eq!(accept(&format!("  {max}  ")), Ok(max));
    }

    #[tokio::test]
    async fn an_agent_has_no_epoch_until_one_is_applied() {
        let (epochs, _h) = epoch_handle(&[("cogsworth", "brenn:cogsworth.epoch")]);
        assert_eq!(epochs.current("cogsworth"), None);
    }

    #[tokio::test]
    async fn apply_moves_every_agent_on_the_channel() {
        let addr = "brenn:shared.epoch";
        let (epochs, _h) = epoch_handle(&[
            ("lumiere", addr),
            ("cogsworth", addr),
            ("mrs-potts", "brenn:other.epoch"),
        ]);
        assert_eq!(epochs.apply(addr, "1"), names(&["cogsworth", "lumiere"]));
        assert_eq!(epochs.current("cogsworth").as_deref(), Some("1"));
        assert_eq!(epochs.current("lumiere").as_deref(), Some("1"));
        assert_eq!(epochs.current("mrs-potts"), None);
    }

    #[tokio::test]
    async fn apply_reports_nothing_when_the_epoch_is_unchanged() {
        let addr = "brenn:cogsworth.epoch";
        let (epochs, _h) = epoch_handle(&[("cogsworth", addr)]);
        assert_eq!(epochs.apply(addr, "1"), names(&["cogsworth"]));
        assert!(epochs.apply(addr, " 1 ").is_empty());
        assert_eq!(epochs.apply(addr, "2"), names(&["cogsworth"]));
    }

    #[tokio::test]
    async fn a_refused_body_leaves_the_previous_epoch_standing() {
        let addr = "brenn:cogsworth.epoch";
        let (epochs, _h) = epoch_handle(&[("cogsworth", addr)]);
        epochs.apply(addr, "1");
        assert!(epochs.apply(addr, "").is_empty());
        assert!(epochs.apply(addr, &"a".repeat(65)).is_empty());
        assert_eq!(epochs.current("cogsworth").as_deref(), Some("1"));
    }

    #[tokio::test]
    #[should_panic(expected = "which no agent named as its epoch channel")]
    async fn apply_on_an_unbound_address_is_a_wiring_bug() {
        let (epochs, _h) = epoch_handle(&[("cogsworth", "brenn:cogsworth.epoch")]);
        epochs.apply("brenn:nobody.epoch", "1");
    }

    #[test]
    fn spec_matchers_are_bare_names() {
        let spec =
            conversation_epoch_spec(&names(&["brenn:cogsworth.epoch", "brenn:lumiere.epoch"]));
        assert_eq!(spec.component, CONVERSATION_EPOCH_COMPONENT);
        assert_eq!(
            spec.subscriptions,
            names(&["brenn:cogsworth.epoch", "brenn:lumiere.epoch"])
        );
        assert!(spec.policy.allows_channel_access("brenn:cogsworth.epoch"));
        assert!(!spec.policy.allows_channel_access("brenn:cogsworth.other"));
        assert!(!spec.policy.allows_channel_access("brenn:some-other-thing"));
    }

    #[test]
    #[should_panic(expected = "is not a durable `brenn:` address")]
    fn spec_refuses_a_non_durable_epoch_address() {
        conversation_epoch_spec(&names(&["ephemeral:cogsworth.epoch"]));
    }

    /// The operator-facing half of "reject and log". The dedup key carries both
    /// the agent and the reason: without the agent a second agent on the channel
    /// would be silently invisible, and without the reason a publisher flipping
    /// between two kinds of bad body would page once and never again.
    #[tokio::test]
    async fn a_refused_epoch_pages_once_per_agent_and_reason() {
        let addr = "brenn:shared.epoch";
        let (alerts, captured, drainer) =
            brenn_obs::alerting::make_capturing_alerter_with_severity();
        let apps = BTreeMap::from([
            ("cogsworth".to_string(), addr.to_string()),
            ("lumiere".to_string(), addr.to_string()),
        ]);
        let epochs = EpochGoal::new(apps, alerts);

        epochs.apply(addr, "");
        // Same agents, same reason: already paged.
        epochs.apply(addr, "");
        // A different reason, for both agents.
        epochs.apply(addr, &"a".repeat(65));

        drop(epochs);
        drainer.await.expect("alert drainer panicked");
        let fired = captured.lock().expect("captured alerts lock").clone();
        assert!(
            fired
                .iter()
                .all(|(severity, _, _)| matches!(severity, AlertSeverity::Warning)),
            "a publisher's mistake is contained, not fatal: {fired:?}"
        );
        let bodies: Vec<&str> = fired.iter().map(|(_, _, body)| body.as_str()).collect();
        assert_eq!(bodies.len(), 4, "one page per (agent, reason): {bodies:?}");
        for agent in ["agent cogsworth", "agent lumiere"] {
            assert_eq!(
                bodies.iter().filter(|b| b.contains(agent)).count(),
                2,
                "every page names its agent: {bodies:?}"
            );
        }
        assert_eq!(
            bodies
                .iter()
                .filter(|b| b.contains("empty epoch body"))
                .count(),
            2,
            "both agents refuse the empty body, and each pages for itself: {bodies:?}"
        );
    }
}
