//! The reload facility's bus identity: the two channels an operator declares to
//! turn it on, and the `system:config-reload` participant that reads one and
//! writes the other.
//!
//! The pair is **declared, not minted**. Nothing here adds a channel to a
//! document: the operator writes both `[[channel]]` blocks, sizes them, and
//! grants access to them like any other channel, and their presence is what
//! activates the facility. A principal that may ask for a reload is one holding
//! `publish` on the request channel — written by the deployer, and by nobody
//! else.
//!
//! The addresses are fixed rather than configurable because the agent that asks
//! for a reload has to know them without reading config, and there is one
//! process and one document for them to name.
//!
//! Two channels with two roles, per the bus's own pairing rule:
//!
//! - `brenn:config.reload` is a **signal**: request N+1 subsumes N, so the
//!   window is short and pending activations coalesce. The body is not read —
//!   publishing anything is the request.
//! - `brenn:config.status` is **state**: it retains the newest outcome, so a
//!   reader learns which document the process is projecting without having been
//!   subscribed when the reload ran.
//!
//! Both, or neither: a document declaring one alone is refused, because a
//! request nobody can answer and an answer nobody asked for are each a facility
//! that looks present and is not.
//!
//! The outcome body is here too, beside the address it is published to. It is
//! an **additive contract**: an LLM reads these bodies, so fields are added and
//! never renamed or removed, and `v` moves only on a reshape that breaks a
//! reader. A reader learns the schema by naming this type rather than by
//! transcribing a struct it cannot see.

use brenn_envelope::grants::AppCapability;
use brenn_lib::access::AppPolicy;
use brenn_lib::access::acl::ChannelMatcher;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::system::SystemParticipantSpec;
use crate::{ChannelEntry, Messenger, PublishResult, Urgency};

/// System-participant component name; the identity is `system:config-reload`.
pub const CONFIG_RELOAD_COMPONENT: &str = "config-reload";

/// The request channel: publishing anything here asks the process to converge
/// to the document on disk.
pub const RELOAD_ADDRESS: &str = "brenn:config.reload";

/// The retained outcome channel.
pub const STATUS_ADDRESS: &str = "brenn:config.status";

/// The scheme-stripped name, which is the grain the delivery and publish gates
/// match ACLs at.
///
/// Derived rather than restated: a bare name that drifted from its address
/// would leave the code-built ACL scoped to a channel nobody publishes to, and
/// every outcome would come back `AclDenied`.
///
/// # Panics
///
/// If the address is not a `brenn:` one. Both are constants in this module.
fn bare(address: &'static str) -> &'static str {
    address
        .strip_prefix("brenn:")
        .unwrap_or_else(|| panic!("{address:?} is not a brenn: address"))
}

/// The `system:config-reload` participant, or `None` when the document does not
/// declare the facility.
///
/// It holds exactly the subscribe authority for the request channel and the
/// publish authority for the status channel, in the one ACL family both
/// addresses live in. Nothing else in the process holds either.
///
/// # Panics
///
/// If exactly one of the two channels is declared, naming the missing one; and
/// if the status channel is declared with a standing window of zero, which
/// would leave the retained outcome unreadable by the reader the channel exists
/// for.
pub fn config_reload_spec(entries: &[ChannelEntry]) -> Option<SystemParticipantSpec> {
    let request = entries.iter().find(|e| e.address == RELOAD_ADDRESS);
    let status = entries.iter().find(|e| e.address == STATUS_ADDRESS);
    let status = match (request, status) {
        (None, None) => return None,
        (Some(_), None) => panic!(
            "[[channel]] {RELOAD_ADDRESS:?} is declared but {STATUS_ADDRESS:?} is not — the \
             reload facility is both channels or neither: a request whose outcome nothing \
             reports is a reload nobody can check"
        ),
        (None, Some(_)) => panic!(
            "[[channel]] {STATUS_ADDRESS:?} is declared but {RELOAD_ADDRESS:?} is not — the \
             reload facility is both channels or neither: an outcome channel with no request \
             channel reports on a facility that is off"
        ),
        (Some(_), Some(status)) => status,
    };
    let standing = status.resolved_channel.standing_retain_depth;
    assert!(
        standing.is_push_enabled(),
        "[[channel]] {STATUS_ADDRESS:?} has a standing_retain_depth of {standing:?} — the status \
         channel is state, and a channel that retains nothing cannot tell a reader which \
         document this process is projecting; size it to at least one",
    );

    let mut policy = AppPolicy::default();
    policy.grants.insert(AppCapability::MessagingSubscribe);
    policy.grants.insert(AppCapability::MessagingPublish);
    policy
        .acls
        .brenn_subscribe
        .push(ChannelMatcher::Exact(bare(RELOAD_ADDRESS).to_string()));
    policy
        .acls
        .brenn_publish
        .push(ChannelMatcher::Exact(bare(STATUS_ADDRESS).to_string()));
    Some(SystemParticipantSpec {
        component: CONFIG_RELOAD_COMPONENT,
        policy,
        subscriptions: vec![RELOAD_ADDRESS.to_string()],
    })
}

/// Schema version. Bumped only on a reshape a reader cannot absorb.
pub const STATUS_VERSION: u32 = 1;

/// What happened.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    /// The process started on this document. Always `generation` 0.
    Booted,
    /// The process converged to a new document.
    Applied,
    /// The document on disk compiled to the projection already running: the
    /// bytes moved and nothing the process does depends on the difference.
    Unchanged,
    /// The document was not applied, and the running state was not touched.
    Refused,
}

/// Which door the outcome came through.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Trigger {
    /// Startup — not a reload at all, but the same question answered.
    Boot,
    /// A message on the request channel.
    Bus,
    /// `SIGUSR1`.
    Signal,
}

/// What moved, named. Lists rather than counts: the reader that cares is asking
/// whether *its* automation installed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusDelta {
    pub consumers_added: Vec<String>,
    pub consumers_removed: Vec<String>,
    pub consumers_changed: Vec<String>,
    pub channels_added: Vec<String>,
    pub channels_removed: Vec<String>,
    pub channels_changed: Vec<String>,
    /// Entries whose description text was updated in place. Nothing about them
    /// routes, sizes or authorizes, which is why they are listed apart from
    /// `channels_changed`.
    pub channels_described: Vec<String>,
}

/// One outcome, as published.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReloadStatus {
    /// Schema version.
    pub v: u32,
    pub outcome: Outcome,
    pub trigger: Trigger,
    /// Applied reloads since boot. Boot itself publishes 0, so the retained
    /// body always describes *this* process; an `unchanged` outcome does not
    /// move it, because nothing about the running state moved.
    pub generation: u64,
    /// When this outcome was reached, RFC 3339 in UTC.
    pub at: String,
    /// The candidate document's identity, or `null` where there is no candidate
    /// to name — a refusal that did not get as far as compiling one.
    pub document_sha256: Option<String>,
    /// The root document's path, or `null` when the process was started without
    /// one.
    pub root: Option<String>,
    /// The identity of the document this process is projecting, which is what a
    /// reader compares against a hash of the tree on disk.
    pub running_document_sha256: String,
    pub delta: StatusDelta,
    /// Why the document was not applied, one line per reason. Empty unless the
    /// outcome is `refused`.
    ///
    /// Two grammars share this list and they mean opposite remedies: a line
    /// ending in "this change needs a restart" says the document is good and
    /// the process cannot walk to it, while a compile diagnostic or one of
    /// boot's environment asserts says the document or the host is wrong and a
    /// restart makes it worse. A reader today tells them apart by their text.
    // TODO(reload-refusal-kinds): carry the remedy as a field rather than in
    // the prose, so a reader does not substring-match it.
    pub refusals: Vec<String>,
}

impl ReloadStatus {
    /// The outcome boot publishes: this process now projects this document, and
    /// nothing has been asked of it yet.
    pub fn booted(document_sha256: String, root: Option<String>) -> Self {
        Self {
            v: STATUS_VERSION,
            outcome: Outcome::Booted,
            trigger: Trigger::Boot,
            generation: 0,
            at: now(),
            document_sha256: Some(document_sha256.clone()),
            root,
            running_document_sha256: document_sha256,
            delta: StatusDelta::default(),
            refusals: Vec::new(),
        }
    }

    /// The JSON body, as published.
    ///
    /// # Panics
    ///
    /// Never in practice: every field is a string, a number or a list of
    /// strings, so serialization cannot fail on shape.
    pub fn body(&self) -> String {
        serde_json::to_string(self).expect("reload status is plain data and always serializes")
    }
}

/// The current instant as an RFC 3339 string, UTC, truncated to seconds.
pub fn now() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// The length one refusal line is cut to when the body does not fit.
///
/// Two thousand characters is more of a compile diagnostic or an environment
/// assert than anyone reads off a channel, and the journal has the whole of it.
const REFUSAL_LINE_MAX_CHARS: usize = 2_000;

/// `text`, cut to `max` characters on a character boundary, with an ellipsis
/// where it was cut.
fn cut(text: &str, max: usize) -> String {
    match text.char_indices().nth(max) {
        None => text.to_string(),
        Some((at, _)) => format!("{}…", &text[..at]),
    }
}

/// The most a refusal alert body may carry.
///
/// The phone backend is the binding constraint: ntfy's own message limit is
/// 4 KiB and an oversized message is rejected outright, which would lose the
/// alert entirely — and the refusals worth alerting on are exactly the long
/// ones, a compile report with excerpts or a level-1 diff across a large
/// deployment. Comfortably under, because the backend counts bytes and this
/// counts characters.
const ALERT_BODY_MAX_CHARS: usize = 3_000;

/// One refusal line as an alert carries it. Short enough that a handful fit.
const ALERT_LINE_MAX_CHARS: usize = 400;

/// `refusals` as an alert body that a phone backend will accept.
///
/// Refusal text is principal-controlled and unbounded: the document's author
/// and the principal that may ask for a reload are the same LLM in the
/// deployment this facility exists for. So the body is cut to fit and says how
/// much it dropped; the journal line and the retained status body carry the
/// rest.
pub fn refusal_alert_body(refusals: &[String]) -> String {
    let mut body = String::new();
    let mut shown = 0;
    for line in refusals {
        let line = cut(line, ALERT_LINE_MAX_CHARS);
        if body.len() + line.len() + 1 > ALERT_BODY_MAX_CHARS {
            break;
        }
        if shown > 0 {
            body.push('\n');
        }
        body.push_str(&line);
        shown += 1;
    }
    if shown < refusals.len() {
        let tail = format!(
            "\n… {} more refusal line(s), and the untruncated text of these, in the journal and \
             on {STATUS_ADDRESS}",
            refusals.len() - shown,
        );
        body.push_str(&tail);
    }
    body
}

/// The same outcome with its refusal list cut down to fit `max_body_bytes`, or
/// `None` when it already fits or carries no refusal to cut.
///
/// An outcome with no refusals is answered `None` whatever its size: there is
/// nothing to cut. An oversized body with no refusals is the publisher's panic
/// to report.
///
/// `refusals` is the only list that can be oversized *here*. The delta's lists
/// grow with the document too, but an `applied` body is built and measured in
/// the reload's prepare phase and refused if it does not fit, so by the time an
/// outcome carrying a delta reaches this function it has already been proved
/// publishable. Refusals cannot be handled that way: the document's author and
/// the principal that may ask for a reload are the same LLM in the deployment
/// this facility exists for, so "the diagnostics are enormous" is a thing that
/// principal can arrange — and a body that cannot be published is not a reason
/// to take the process down over a document nothing applied.
fn fitted(status: &ReloadStatus, max_body_bytes: usize) -> Option<ReloadStatus> {
    if status.refusals.is_empty() || status.body().len() <= max_body_bytes {
        return None;
    }
    let full = status.refusals.len();
    let mut cut_lines: Vec<String> = status
        .refusals
        .iter()
        .map(|line| cut(line, REFUSAL_LINE_MAX_CHARS))
        .collect();
    let mut trimmed = status.clone();
    loop {
        trimmed.refusals = cut_lines.clone();
        if cut_lines.len() < full {
            trimmed.refusals.push(format!(
                "… {} more refusal line(s), and the untruncated text of these, in the journal",
                full - cut_lines.len(),
            ));
        }
        if trimmed.body().len() <= max_body_bytes || cut_lines.is_empty() {
            return Some(trimmed);
        }
        cut_lines.pop();
    }
}

/// Publish one outcome under the reload facility's own identity.
///
/// A body that does not fit the channel's `max_body_bytes` is republished with
/// its refusal list cut down (see [`fitted`]); the whole of it is in the
/// journal either way.
///
/// # Panics
///
/// On any publish outcome other than `Ok`, with one exception: a `refused`
/// outcome that is rate-limited is logged and dropped. An oversized body is a
/// misconfiguration for a `refused` outcome, whose refusal list a principal can
/// grow at will past what cutting it down can save, and a host bug for every
/// other, whose body prepare has already measured against this same limit; the
/// two say so separately. The status channel is
/// operator-declared and validated at plan time, and the publishing policy is
/// code-built to reach exactly it, so a failure here means either a host wiring
/// bug or a channel the operator sized such that the facility cannot report —
/// and a reload facility whose outcomes go nowhere is worse than one that is
/// off, because an operator reading a stale retained body would believe the
/// process projects a document it does not. A refusal is the exception because
/// it moved nothing: the retained body still names the document this process
/// projects, which is the question that body answers, and refusals are the one
/// outcome a principal can produce back-to-back at will.
pub async fn publish_status(messenger: &Messenger, status: &ReloadStatus) {
    let fitted = fitted(status, messenger.max_body_bytes());
    if let Some(ref cut) = fitted {
        warn!(
            refusals = status.refusals.len(),
            published = cut.refusals.len(),
            reason = %status.refusals.join("; "),
            "reload status body exceeds max_body_bytes; publishing a cut-down refusal list"
        );
    }
    let status = fitted.as_ref().unwrap_or(status);
    let body = status.body();
    match messenger
        .publish_from_system(
            CONFIG_RELOAD_COMPONENT,
            STATUS_ADDRESS,
            &body,
            Urgency::Normal,
            None,
        )
        .await
    {
        PublishResult::Ok { .. } => {}
        PublishResult::RateLimited if status.outcome == Outcome::Refused => {
            // Nothing moved, so the retained body still answers the question it
            // exists to answer: which document this process is projecting.
            warn!(
                reason = %status.refusals.join("; "),
                "reload refusal not published: {STATUS_ADDRESS} is rate-limited"
            );
        }
        PublishResult::RateLimited => panic!(
            "reload status publish to {STATUS_ADDRESS:?} was rate-limited — the outcome channel's \
             send_rate is below the rate this process reports outcomes at; raise it. Refusing to \
             carry on with an outcome nobody can read."
        ),
        PublishResult::BodyTooLarge { len, max } if status.outcome == Outcome::Refused => panic!(
            "reload status publish to {STATUS_ADDRESS:?} rejected — the refusal body is {len} \
             bytes but [messaging] max_body_bytes is {max}, and cutting the refusal list down did \
             not bring it under. Raise max_body_bytes above {len}."
        ),
        PublishResult::BodyTooLarge { len, max } => panic!(
            "reload status publish to {STATUS_ADDRESS:?} rejected — a {:?} body of {len} bytes \
             against a [messaging] max_body_bytes of {max}. This is a host bug: an outcome \
             carrying a delta is built and measured against this same limit in the reload's \
             prepare phase and refused there if it does not fit, and every other outcome carries \
             an empty delta. The delta names {} added, {} removed, {} changed and {} described \
             channels and {} added, {} removed and {} changed consumers.",
            status.outcome,
            status.delta.channels_added.len(),
            status.delta.channels_removed.len(),
            status.delta.channels_changed.len(),
            status.delta.channels_described.len(),
            status.delta.consumers_added.len(),
            status.delta.consumers_removed.len(),
            status.delta.consumers_changed.len(),
        ),
        other => panic!(
            "reload status publish to {STATUS_ADDRESS:?} did not succeed ({other:?}) — the \
             facility's code-built policy reaches exactly this channel and the channel is \
             validated at plan time, so a failure is a host bug."
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use brenn_lib::messaging::config::Depth;
    use brenn_lib::messaging::test_support::test_channel_entry;

    fn entry(address: &'static str, standing: Depth) -> ChannelEntry {
        let mut entry = test_channel_entry(bare(address), Vec::new());
        entry.resolved_channel.standing_retain_depth = standing;
        entry
    }

    fn pair() -> Vec<ChannelEntry> {
        vec![
            entry(RELOAD_ADDRESS, Depth::Bounded(4)),
            entry(STATUS_ADDRESS, Depth::Bounded(8)),
        ]
    }

    fn refused_with(refusals: Vec<String>) -> ReloadStatus {
        ReloadStatus {
            outcome: Outcome::Refused,
            trigger: Trigger::Bus,
            refusals,
            ..ReloadStatus::booted("abc".to_string(), Some("/etc/brenn/main.brenn".to_string()))
        }
    }

    /// A body that fits is published as it stands: the cut is for the case that
    /// does not, and nothing else pays for it.
    #[test]
    fn a_body_within_the_limit_is_not_cut() {
        assert!(fitted(&refused_with(vec!["one line".to_string()]), 65_536).is_none());
    }

    /// The principal that writes the document is the principal that asks for
    /// the reload, so the refusal list is content it controls. A body over the
    /// limit is cut to fit rather than published — which would panic — or
    /// dropped.
    #[test]
    fn an_oversized_refusal_list_is_cut_to_fit() {
        let status = refused_with(
            (0..500)
                .map(|i| format!("line {i}: {}", "x".repeat(500)))
                .collect(),
        );
        assert!(status.body().len() > 65_536, "the fixture must not fit");

        let cut = fitted(&status, 65_536).expect("a body over the limit is cut");
        assert!(cut.body().len() <= 65_536, "{}", cut.body().len());
        assert!(cut.refusals.len() < status.refusals.len());
        assert!(
            cut.refusals
                .last()
                .expect("a cut list keeps a tail line")
                .contains("more refusal line(s)"),
            "{:?}",
            cut.refusals.last()
        );
        // Everything but the refusals survives: the identity half of the body
        // is what a reader compares against the tree on disk.
        assert_eq!(cut.running_document_sha256, status.running_document_sha256);
        assert_eq!(cut.root, status.root);
    }

    /// An outcome with no refusals is answered `None` whatever its size: there
    /// is nothing to cut. An oversized body with no refusals is the publisher's
    /// panic.
    #[test]
    fn an_outcome_with_no_refusals_is_never_cut() {
        let status = ReloadStatus {
            outcome: Outcome::Applied,
            delta: StatusDelta {
                channels_added: (0..500)
                    .map(|i| format!("brenn:channel-{i}-{}", "x".repeat(500)))
                    .collect(),
                ..StatusDelta::default()
            },
            ..ReloadStatus::booted("abc".to_string(), Some("/etc/brenn/main.brenn".to_string()))
        };
        assert!(status.body().len() > 65_536, "the fixture must not fit");
        assert!(fitted(&status, 65_536).is_none());
    }

    /// One enormous line is cut in itself, not just dropped from the list: a
    /// compile report is one line and dropping it would leave a refusal with
    /// nothing said about it.
    #[test]
    fn a_single_enormous_line_is_truncated_rather_than_dropped() {
        let status = refused_with(vec!["d".repeat(200_000)]);
        let cut = fitted(&status, 65_536).expect("a body over the limit is cut");
        assert_eq!(cut.refusals.len(), 1);
        assert!(cut.refusals[0].ends_with('…'));
        assert!(cut.body().len() <= 65_536);
    }

    #[test]
    fn a_document_declaring_neither_channel_has_no_participant() {
        assert!(config_reload_spec(&[entry("brenn:other", Depth::Bounded(1))]).is_none());
    }

    #[test]
    fn the_declared_pair_yields_the_participant() {
        let spec = config_reload_spec(&pair()).expect("both channels declared");
        assert_eq!(spec.component, CONFIG_RELOAD_COMPONENT);
        assert_eq!(spec.subscriptions, vec![RELOAD_ADDRESS.to_string()]);
        assert!(spec.policy.grants.has(AppCapability::MessagingSubscribe));
        assert!(spec.policy.grants.has(AppCapability::MessagingPublish));
        assert_eq!(
            spec.policy.acls.brenn_subscribe,
            vec![ChannelMatcher::Exact("config.reload".to_string())]
        );
        assert_eq!(
            spec.policy.acls.brenn_publish,
            vec![ChannelMatcher::Exact("config.status".to_string())]
        );
    }

    #[test]
    fn the_participant_may_not_publish_the_request_channel_or_read_the_status_channel() {
        let spec = config_reload_spec(&pair()).expect("both channels declared");
        // The two authorities are one direction each: the identity that answers
        // requests cannot manufacture one, and the identity that reports
        // outcomes is not a reader of its own reports.
        assert!(
            !spec
                .policy
                .acls
                .brenn_publish
                .contains(&ChannelMatcher::Exact("config.reload".to_string()))
        );
        assert!(
            !spec
                .policy
                .acls
                .brenn_subscribe
                .contains(&ChannelMatcher::Exact("config.status".to_string()))
        );
    }

    #[test]
    #[should_panic(expected = "brenn:config.status\" is not")]
    fn a_request_channel_without_a_status_channel_is_refused() {
        drop(config_reload_spec(&[entry(
            RELOAD_ADDRESS,
            Depth::Bounded(4),
        )]));
    }

    #[test]
    #[should_panic(expected = "brenn:config.reload\" is not")]
    fn a_status_channel_without_a_request_channel_is_refused() {
        drop(config_reload_spec(&[entry(
            STATUS_ADDRESS,
            Depth::Bounded(8),
        )]));
    }

    #[test]
    #[should_panic(expected = "standing_retain_depth")]
    fn a_status_channel_that_retains_nothing_is_refused() {
        drop(config_reload_spec(&[
            entry(RELOAD_ADDRESS, Depth::Bounded(4)),
            entry(STATUS_ADDRESS, Depth::Bounded(0)),
        ]));
    }

    fn booted() -> ReloadStatus {
        ReloadStatus::booted("abc123".to_string(), Some("/etc/brenn.brenn".to_string()))
    }

    #[test]
    fn a_booted_outcome_names_one_document_twice() {
        let status = booted();
        assert_eq!(status.outcome, Outcome::Booted);
        assert_eq!(status.trigger, Trigger::Boot);
        assert_eq!(status.generation, 0);
        // Nothing has been asked of the process yet, so the candidate and the
        // running document are one document.
        assert_eq!(status.document_sha256.as_deref(), Some("abc123"));
        assert_eq!(status.running_document_sha256, "abc123");
        assert!(status.delta == StatusDelta::default());
        assert!(status.refusals.is_empty());
    }

    #[test]
    fn a_status_round_trips_through_its_body() {
        let status = ReloadStatus {
            v: STATUS_VERSION,
            outcome: Outcome::Refused,
            trigger: Trigger::Signal,
            generation: 7,
            at: "2026-09-04T12:00:00Z".to_string(),
            document_sha256: None,
            root: None,
            running_document_sha256: "deadbeef".to_string(),
            delta: StatusDelta {
                consumers_added: vec!["watcher".to_string()],
                channels_described: vec!["brenn:notes".to_string()],
                ..Default::default()
            },
            refusals: vec!["apps[assistant] differs: this change needs a restart".to_string()],
        };
        let parsed: ReloadStatus =
            serde_json::from_str(&status.body()).expect("the body is this schema");
        assert_eq!(parsed, status);
    }

    #[test]
    fn the_body_spells_the_field_names_a_reader_was_promised() {
        let value: serde_json::Value =
            serde_json::from_str(&booted().body()).expect("valid json body");
        let object = value.as_object().expect("an object");
        // Renaming or dropping one of these breaks every reader; the contract
        // is additive, so this list may grow and never shrink.
        for field in [
            "v",
            "outcome",
            "trigger",
            "generation",
            "at",
            "document_sha256",
            "root",
            "running_document_sha256",
            "delta",
            "refusals",
        ] {
            assert!(object.contains_key(field), "missing field {field}");
        }
        assert_eq!(value["v"], 1);
        assert_eq!(value["outcome"], "booted");
        assert_eq!(value["trigger"], "boot");
        for field in [
            "consumers_added",
            "consumers_removed",
            "consumers_changed",
            "channels_added",
            "channels_removed",
            "channels_changed",
            "channels_described",
        ] {
            assert!(
                value["delta"].get(field).is_some_and(|v| v.is_array()),
                "missing delta field {field}"
            );
        }
    }

    #[test]
    fn an_outcome_timestamp_is_utc_to_the_second() {
        let at = booted().at;
        assert!(at.ends_with('Z'), "{at} is not UTC-stamped");
        chrono::DateTime::parse_from_rfc3339(&at).expect("an RFC 3339 instant");
    }
    /// The alert body is bounded, because the phone backend rejects an
    /// oversized message outright — and the refusals worth alerting on are
    /// exactly the long ones.
    #[test]
    fn a_refusal_alert_body_is_cut_to_what_a_phone_backend_accepts() {
        let refusals: Vec<String> = (0..50)
            .map(|n| format!("{n}: {}", "a very long diagnostic line ".repeat(100)))
            .collect();
        let body = refusal_alert_body(&refusals);
        assert!(
            body.len() <= ALERT_BODY_MAX_CHARS,
            "an alert body of {} bytes would be rejected by the backend",
            body.len(),
        );
        assert!(
            body.starts_with("0: a very long diagnostic line"),
            "the first refusal is the one an operator most needs: {body:.80}"
        );
        assert!(
            body.contains("more refusal line(s)"),
            "a body that dropped lines says so: {body:.200}"
        );
    }

    /// A list that fits is carried whole, line for line.
    #[test]
    fn a_short_refusal_alert_body_is_the_refusals_themselves() {
        let refusals = vec!["apples differ".to_string(), "pears differ".to_string()];
        assert_eq!(refusal_alert_body(&refusals), "apples differ\npears differ");
    }
}
