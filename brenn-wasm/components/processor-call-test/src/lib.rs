// Peer-call fixture for the `brenn:processor` world: the one component that
// imports `brenn:processor/calls`.
//
// It is both halves of a call, chosen per activation by the body it is handed,
// so one kind wired to itself makes a chain of any depth:
//
//   "call:<rest>" — call the peer wired to the `ask` port with `<rest>` and
//                   answer `via:<the peer's reply>`. A nested `call:` in
//                   `<rest>` makes the peer do the same to its own peer, which
//                   is how a three-deep chain is spelled with one fixture.
//   anything else — answer `answer:<body>`.
//
// Whatever it answers is also published to the `out` port first, buffered like
// any other publish. That ordering is the point: a callee's ok flushes its
// buffer before its caller is handed the reply, so a test reading the output
// channel sees the innermost callee's row before its caller's.
//
// Only a sync-call activation is answered. On an async one the same work
// happens and the reply is published and not returned, because answering an
// activation that asked nothing is a trap.

use brenn_guest::{Activation, Error, Processor, calls, publish};

/// The declared `call` port every call here goes out through.
const ASK: &str = "ask";
/// The declared output port the answer is also published to.
const OUT: &str = "out";
/// Body prefix selecting the caller role.
const CALL: &str = "call:";

struct ProcessorCallTest;

impl Processor for ProcessorCallTest {
    fn receive(activation: Activation) -> Result<Option<String>, Error> {
        // The first new envelope on any window — the sync request when this is a
        // sync-call activation, the delivered message when it is not.
        let Some(envelope) = activation
            .port_windows()
            .filter_map(|w| w.new_envelopes().next())
            .next()
        else {
            return Ok(None);
        };
        let body = envelope?.body;

        let answer = match body.strip_prefix(CALL) {
            Some(rest) => match calls::call(ASK, rest)? {
                Some(reply) => format!("via:{reply}"),
                None => "via:none".to_string(),
            },
            None => format!("answer:{body}"),
        };

        publish(OUT, &answer)?;
        if activation.sync().is_some() {
            Ok(Some(answer))
        } else {
            Ok(None)
        }
    }
}

brenn_guest::export_processor!(ProcessorCallTest);
