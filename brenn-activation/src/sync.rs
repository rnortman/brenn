//! The sync-call activation: the shape, the vocabulary, and the request envelope.
//!
//! A **sync-call activation** is an ordinary activation plus a return
//! obligation. The request arrives as the one envelope in a named port's window,
//! the reply is the entry's return value, and every other bound port and every
//! deferred window rides along exactly as in the async shape. Nothing about it is
//! a property of one host: what differs between hosts is only *who can raise
//! one*, which the crate docs' host-specific list states.
//!
//! Everything a host needs to answer a caller lives here, because both hosts
//! answer the same questions. A refusal spelled one way on one host and another
//! way on the other is a component model with two dialects, which is what this
//! crate exists to refuse.

use brenn_envelope::{ChannelScheme, MessageEnvelope, Urgency};
use chrono::{DateTime, Utc};
use uuid::Uuid;

/// The address family a sync-call request envelope carries — the port name
/// appended to this prefix.
///
/// **Never routed and never bindable.** The envelope exists only inside the
/// activation's sync window: it never enters a ring, never passes a router and
/// never crosses the wire. These addresses are therefore deliberately absent
/// from the surface schema's table of *routable* reserved local channels, and
/// absence is the enforcement — boot rejects a `local:brenn/*` binding the table
/// does not name, so a component can neither bind nor write one.
pub const SYNC_CHANNEL_PREFIX: &str = "local:brenn/sync/";

/// The channel a sync-call activation's request envelope carries for `port` —
/// [`SYNC_CHANNEL_PREFIX`] plus the port name.
///
/// Sync-ness is the activation's own `sync` field, never a property of the
/// envelope: the envelope's `envelope_type` is `local`, which is truthful on
/// every axis that field answers (host-local, never on the wire, no durable row).
#[must_use]
pub fn sync_channel(port: &str) -> String {
    format!("{SYNC_CHANNEL_PREFIX}{port}")
}

/// Mint the envelope a sync-call activation's request rides in on.
///
/// One minting for both hosts, so a component reading its request sees the same
/// bytes wherever it runs. `instance` is the target's own name as its
/// specification and the document spell it — a backend consumer's slug, a page
/// instance's id — and not either host's participant vocabulary, because a
/// request is never published and a name that differed per host is a difference
/// a component could read straight off `sender`. `source` and `sender` are both
/// that target: the host mints this on the callee's behalf and hands it straight
/// back to that same callee, so there is no other honest attribution and no peer
/// identity in play. No deadline and no deferral: the request is delivered by the activation
/// it causes, and the activation is assembled around it.
///
/// `message_id` and `publish_ts` come from the edge that reads entropy and
/// clocks; this layer reads neither.
#[must_use]
pub fn sync_request(
    instance: &str,
    port: &str,
    body: String,
    message_id: Uuid,
    publish_ts: DateTime<Utc>,
) -> MessageEnvelope {
    MessageEnvelope {
        message_id,
        source: instance.to_string(),
        channel: sync_channel(port),
        sender: instance.to_string(),
        publish_ts,
        body,
        reply_to: None,
        delivery_deadline: None,
        deliver_after: None,
        impetus: None,
        urgency: Urgency::Normal,
        envelope_type: ChannelScheme::Local,
    }
}

/// Whether a body is small enough to cross a host's boundary.
///
/// One cap for both directions of a sync call, because both are bodies the
/// deployment already bounds: a reply is held to the cap a publish is, so a
/// callee cannot hand a caller more than it could have published, and a request
/// is held to it too, so a caller cannot hand a guest an envelope its own host
/// would have refused to publish.
///
/// The dispositions differ and belong to the callers of this function. An
/// oversize reply is a trap of the callee on both hosts — it is the callee that
/// wrote it, and truncating it or answering an error in its place would hand
/// the caller something the callee never said. An oversize request is a caller
/// bug the host refuses to assemble.
#[must_use]
pub fn within_body_cap(reply: &str, cap: usize) -> bool {
    reply.len() <= cap
}

/// How one sync-call request finished, in the vocabulary both hosts answer in:
/// the reply on ok, the callee's own account on err, and which refusal it was
/// when nothing ran.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncAnswer {
    /// The entry returned ok, with the reply it answered with (or `None` if it
    /// answered without one). Its buffer flushed before this answer was handed
    /// back.
    Ok(Option<String>),
    /// The entry returned err, carrying its own sanitized account. The buffer was
    /// discarded and a failure counted; the callee keeps running.
    Err(String),
    /// The callee trapped without having answered, or the assembly's own
    /// loud-rung verdict killed it before the entry could run. Both are one fact
    /// for the caller — stop. The buffer flushed nothing.
    Trap,
    /// Nothing was assembled and no entry ran. Always a bug in the caller or the
    /// host; the refusal says whose.
    Refused(SyncRefusal),
}

/// Why a sync-call request was not admitted.
///
/// Every one of them is a bug — in the caller, or in the host that let the
/// request through — never a configured outcome and never a state a conforming
/// deployment reaches. The caller is told, and faults on it.
///
/// Two of the four are unreachable on the backend, for the same kind of reason
/// `Fatal` noise is: that host has no terminal instance state and no unwired
/// mount to be in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncRefusal {
    /// The target is already in flight in the caller's own chain. On the page
    /// this also covers the page-wide case: one JS thread means a genuine user
    /// gesture can never arrive re-entrant, so only an event a component
    /// dispatched programmatically during an activation can.
    ReEntrant,
    /// The target holds no activation entry: never registered, deregistered,
    /// stopping, or still inside the connect-time code that runs *before* its
    /// registration completes.
    Unregistered,
    /// The target is terminal. It has no next activation of any flavor. Surface
    /// only — the backend quarantines an activation rather than an instance.
    Failed,
    /// No wiring is in force, so there is nothing to window against. Surface
    /// only — a backend consumer's wiring exists before its task starts.
    Unwired,
    /// The target holds `dom` and its mount has not run. A `dom` instance's
    /// mount is the call that fills its host element, and nothing else may run
    /// against an element its author has not filled yet, so a peer's call
    /// arriving first is refused rather than served. Surface only — a backend
    /// consumer's mount is its task's first statement, so a request can never
    /// overtake it.
    Unmounted,
}

impl SyncRefusal {
    /// The operator-facing sentence for this refusal — the breadcrumb a host
    /// leaves next to the answer the caller gets.
    #[must_use]
    pub fn describe(&self, instance: &str, port: &str) -> String {
        match self {
            Self::ReEntrant => format!(
                "refused sync activation of {instance} on port {port}: an activation is already \
                 in flight, so this request came from inside an entry"
            ),
            Self::Unregistered => format!(
                "refused sync activation of {instance} on port {port}: the instance holds no \
                 activation entry"
            ),
            Self::Failed => format!(
                "refused sync activation of {instance} on port {port}: the instance is terminal"
            ),
            Self::Unwired => format!(
                "refused sync activation of {instance} on port {port}: no bindings document is in \
                 force"
            ),
            Self::Unmounted => format!(
                "refused sync activation of {instance} on port {port}: the instance renders and \
                 its mount has not run yet"
            ),
        }
    }
}

/// What a host tells its guest about a [`SyncAnswer`] that is not a reply.
///
/// The two words a caller may act on, and the whole of what a component learns
/// about a call it did not get an answer to. Each host lifts its own
/// `call-error` type from these; the classification itself is here, beside the
/// type it classifies, because both hosts speak one `SyncAnswer` and a call the
/// caller cannot distinguish must not be two different errors depending on where
/// the caller was placed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallDisposition {
    /// The peer ran and did not answer ok — it erred or it trapped. Its buffer
    /// flushed nothing. The peer's own account of the failure is deliberately
    /// not carried: it is the peer's, the caller cannot act on it, and handing it
    /// over would make one component's error text another's input.
    Failed,
    /// The peer did not run at all. Which refusal it was is deliberately not
    /// carried either: `Unregistered`, `Failed`, `Unwired` and `Unmounted` are
    /// states a deployment or a mount pass moves through, and a component reading
    /// them apart would be reading host topology out of an error code.
    Refused,
}

/// The peer's reply, or the disposition of an answer that carried none.
///
/// # Panics
///
/// On [`SyncRefusal::ReEntrant`]. The document is the only source of call wiring
/// and it refused cycles at compile time, so a target that finds itself in its
/// caller's chain means that check was bypassed — the compiler and the host
/// disagree about the graph, which is not a state to answer a component from.
pub fn call_disposition(
    answer: SyncAnswer,
    caller: &str,
    port: &str,
) -> Result<Option<String>, CallDisposition> {
    match answer {
        SyncAnswer::Ok(reply) => Ok(reply),
        SyncAnswer::Err(_) | SyncAnswer::Trap => Err(CallDisposition::Failed),
        SyncAnswer::Refused(SyncRefusal::ReEntrant) => panic!(
            "call by {caller} on port {port} was refused re-entrant — the document's acyclicity \
             check was bypassed"
        ),
        SyncAnswer::Refused(_) => Err(CallDisposition::Refused),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stamp() -> (Uuid, DateTime<Utc>) {
        (
            Uuid::parse_str("7d5c2c6e-1f4a-4c7e-9c1a-2b3d4e5f6071").expect("a literal uuid parses"),
            DateTime::parse_from_rfc3339("2026-09-12T00:00:00Z")
                .expect("a literal timestamp parses")
                .with_timezone(&Utc),
        )
    }

    #[test]
    fn a_sync_channel_is_the_prefix_and_the_port() {
        assert_eq!(SYNC_CHANNEL_PREFIX, "local:brenn/sync/");
        assert_eq!(sync_channel("resolve"), "local:brenn/sync/resolve");
    }

    /// The shape is frozen: both hosts mint this, and a caller reading the
    /// request on one host must read the same bytes on the other.
    #[test]
    fn the_request_envelope_is_local_self_attributed_and_never_parked() {
        let (message_id, publish_ts) = stamp();
        let request = sync_request(
            "kiosk-menu",
            "resolve",
            "{\"q\":\"pier\"}".to_string(),
            message_id,
            publish_ts,
        );
        assert_eq!(request.message_id, message_id);
        assert_eq!(request.publish_ts, publish_ts);
        assert_eq!(request.channel, "local:brenn/sync/resolve");
        assert_eq!(request.source, "kiosk-menu");
        assert_eq!(request.sender, "kiosk-menu");
        assert_eq!(request.body, "{\"q\":\"pier\"}");
        assert_eq!(request.envelope_type, ChannelScheme::Local);
        assert_eq!(request.urgency, Urgency::Normal);
        assert!(request.reply_to.is_none());
        assert!(request.delivery_deadline.is_none());
        assert!(request.deliver_after.is_none());
        assert!(request.impetus.is_none());
    }

    #[test]
    fn a_body_is_within_the_cap_up_to_and_including_the_cap() {
        assert!(within_body_cap("", 4));
        assert!(within_body_cap("abc", 4));
        assert!(within_body_cap("abcd", 4));
        assert!(!within_body_cap("abcde", 4));
    }

    /// Every refusal names the instance, the port and which one it was: the
    /// breadcrumb is the only place an operator sees a bug that never reached a
    /// component.
    #[test]
    fn every_refusal_describes_itself_with_its_subject() {
        for refusal in [
            SyncRefusal::ReEntrant,
            SyncRefusal::Unregistered,
            SyncRefusal::Failed,
            SyncRefusal::Unwired,
            SyncRefusal::Unmounted,
        ] {
            let described = refusal.describe("geo", "resolve");
            assert!(described.contains("geo"), "{refusal:?}: {described}");
            assert!(described.contains("resolve"), "{refusal:?}: {described}");
        }
    }

    #[test]
    fn a_peers_reply_is_the_disposition() {
        assert_eq!(
            call_disposition(SyncAnswer::Ok(Some("42".to_string())), "menu", "lookup"),
            Ok(Some("42".to_string()))
        );
        assert_eq!(
            call_disposition(SyncAnswer::Ok(None), "menu", "lookup"),
            Ok(None)
        );
    }

    #[test]
    fn a_peer_that_ran_and_did_not_answer_is_failed_and_one_that_did_not_run_is_refused() {
        for answer in [SyncAnswer::Err("boom".to_string()), SyncAnswer::Trap] {
            assert_eq!(
                call_disposition(answer, "menu", "lookup"),
                Err(CallDisposition::Failed)
            );
        }
        for refusal in [
            SyncRefusal::Unregistered,
            SyncRefusal::Failed,
            SyncRefusal::Unwired,
            SyncRefusal::Unmounted,
        ] {
            assert_eq!(
                call_disposition(SyncAnswer::Refused(refusal), "menu", "lookup"),
                Err(CallDisposition::Refused),
                "{refusal:?}"
            );
        }
    }

    #[test]
    #[should_panic(expected = "acyclicity check was bypassed")]
    fn a_re_entrant_refusal_is_a_host_bug() {
        let _ = call_disposition(
            SyncAnswer::Refused(SyncRefusal::ReEntrant),
            "menu",
            "lookup",
        );
    }
}
