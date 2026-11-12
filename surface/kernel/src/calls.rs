//! What a component's synchronous call to a peer is answered with.
//!
//! The page's half of the `calls` facility splits in two. The *seam* — the
//! wasm-bindgen export, the loader's import shim, the in-flight stack a nested
//! activation pushes onto — is browser-only and lives in [`crate::entry`] and
//! [`crate::sync_door`]. The *decisions* are here: which admission a call must
//! pass before anyone is asked, and which `call-error` a
//! [`SyncAnswer`] is. Both are host-independent facts about the facility rather
//! than about the browser, and both are testable natively, which is the whole
//! reason they are not written inline at the seam.
//!
//! The backend host makes the same two decisions in `brenn-wasm`'s `do_call`,
//! in the same order. What a non-reply *means* is neither host's to decide —
//! that is [`brenn_activation::sync::call_disposition`], over the one
//! `SyncAnswer` both hosts speak — and what is written twice is only each host's
//! lift of two words into its own error type, which each pins with its own test
//! against [`brenn_surface_contract::call_error_str`]'s vocabulary.

use brenn_activation::sync::{CallDisposition, SyncAnswer, call_disposition};
use brenn_envelope::MessageBody;

use crate::contract::CallError;

/// The `call-error` a [`SyncAnswer`] is, or the peer's reply.
///
/// Which of the two dispositions a non-reply is belongs to
/// [`brenn_activation::sync::call_disposition`], beside the answer type both
/// hosts speak; what is decided here is only how this host spells the two words
/// in its own error type. A peer state a component cannot distinguish must not
/// be two different `call-error`s depending on where the component was placed,
/// and that is exactly what a second copy of the classification would allow.
///
/// # Panics
///
/// On [`brenn_activation::sync::SyncRefusal::ReEntrant`], through the shared
/// classification: the
/// document is the only source of call wiring and it refused cycles at compile
/// time, so a target that finds itself in its caller's chain means that check
/// was bypassed.
///
/// The reply is the callee's body as it built it, typed or not and text or
/// not; what a caller of the string contract does with a body it cannot read is
/// [`v0_1_reply`]'s.
pub fn call_answer(
    caller: &str,
    port: &str,
    answer: SyncAnswer,
) -> Result<Option<MessageBody>, CallError> {
    match call_disposition(answer, caller, port) {
        Ok(reply) => Ok(reply),
        Err(CallDisposition::Failed) => Err(CallError::Failed),
        Err(CallDisposition::Refused) => Err(CallError::Refused),
        Err(CallDisposition::TextOnlyPeer) => Err(CallError::TextOnlyPeer),
    }
}

/// A peer's reply as a `brenn:processor@0.1.0` caller reads it: a text reply
/// as its text, its content type dropped as on a channel, and a non-text reply
/// as the trap it is for that caller — the callee already ran and flushed.
pub fn v0_1_reply(caller: &str, port: &str, reply: &MessageBody) -> Result<String, String> {
    match reply.as_text() {
        Some(text) => Ok(text.to_owned()),
        None => Err(format!(
            "surface client: call port {port} of {caller} was answered with a non-text reply \
             ({}); the calling component is built against brenn:processor@0.1.0, which carries \
             only text. The peer ran and its buffer was flushed",
            reply.summary()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::call_error_str;
    use brenn_activation::sync::SyncRefusal;

    #[test]
    fn a_peers_reply_is_the_answer() {
        assert_eq!(
            call_answer(
                "menu",
                "lookup",
                SyncAnswer::Ok(Some(MessageBody::text("42")))
            ),
            Ok(Some(MessageBody::text("42")))
        );
        let bytes = MessageBody::from_parts(Some("application/octet-stream"), &[0xff])
            .expect("a byte body builds");
        assert_eq!(
            call_answer("menu", "lookup", SyncAnswer::Ok(Some(bytes.clone()))),
            Ok(Some(bytes))
        );
        assert_eq!(
            call_answer("menu", "lookup", SyncAnswer::Ok(None)),
            Ok(None)
        );
    }

    #[test]
    fn a_peer_that_did_not_answer_ok_is_failed() {
        assert_eq!(
            call_answer("menu", "lookup", SyncAnswer::Err("boom".to_string())),
            Err(CallError::Failed)
        );
        assert_eq!(
            call_answer("menu", "lookup", SyncAnswer::Trap),
            Err(CallError::Failed)
        );
    }

    #[test]
    fn every_refusal_that_means_the_peer_did_not_run_is_refused() {
        for refusal in [
            SyncRefusal::Unregistered,
            SyncRefusal::Failed,
            SyncRefusal::Unwired,
            SyncRefusal::Unmounted,
        ] {
            assert_eq!(
                call_answer("menu", "lookup", SyncAnswer::Refused(refusal)),
                Err(CallError::Refused),
            );
        }
    }

    #[test]
    fn a_non_text_request_to_a_text_only_peer_is_text_only_peer() {
        assert_eq!(
            call_answer(
                "menu",
                "lookup",
                SyncAnswer::Refused(SyncRefusal::TextOnlyTarget)
            ),
            Err(CallError::TextOnlyPeer)
        );
    }

    #[test]
    fn a_string_contract_caller_reads_text_and_traps_on_bytes() {
        assert_eq!(
            v0_1_reply("menu", "lookup", &MessageBody::text("42")),
            Ok("42".to_string())
        );
        let typed = MessageBody::from_parts(Some("text/csv"), b"a,b").expect("typed text builds");
        assert_eq!(
            v0_1_reply("menu", "lookup", &typed),
            Ok("a,b".to_string()),
            "typed text loses its type"
        );
        let bytes =
            MessageBody::from_parts(Some("image/jpeg"), &[0xff, 0xd8]).expect("a byte body builds");
        let trap = v0_1_reply("menu", "lookup", &bytes).expect_err("bytes trap the caller");
        assert!(trap.contains("port lookup of menu") && trap.contains("non-text reply"));
    }

    #[test]
    #[should_panic(expected = "acyclicity check was bypassed")]
    fn a_re_entrant_refusal_is_a_kernel_bug() {
        let _ = call_answer(
            "menu",
            "lookup",
            SyncAnswer::Refused(SyncRefusal::ReEntrant),
        );
    }

    #[test]
    fn the_answered_words_are_the_wit_vocabulary() {
        // The seam hands these straight to the loader's shim, which throws them
        // as the variant tag the guest lifts. Pinned here as well as in the
        // contract so a new mapping arm cannot reach a component un-spelled.
        assert_eq!(call_error_str(&CallError::Failed), "failed");
        assert_eq!(call_error_str(&CallError::Refused), "refused");
    }
}
