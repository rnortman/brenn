// Demo WASM component for the `brenn:processor` world.
//
// For each new envelope (activation.ports[*].envelopes[new_from..]):
//   - Parses the JSON; returns Err(MalformedEnvelope) on parse failure.
//   - Asserts the `channel` field is non-empty; returns Err(ProcessingFailed)
//     if not.
//   - If the body is exactly the sentinel `"__trap__"`, traps unconditionally
//     so the always-trap acceptance criterion is exercisable.
//   - If `envelope_type == Webhook`: parses `body` as a WebhookEnvelope,
//     then publishes the inner `body` on port "out". If the inner body is the
//     sentinel `"__defer__"`, schedules a deferred self-publish on "out" one
//     minute out (via the host-stamped activation `now`) instead of an immediate
//     publish. If the inner body is the sentinel `"__viewcount__"`, reads the
//     deferred view for output port "out" and publishes a summary of it
//     (`view=<n> first=<payload> at=<deliver_after>`) immediately — exercising
//     the output-port deferred view. If the inner body is `"__cancel__"`, cancels
//     the first parked message on "out" by its view index (defer-cancel). If it is
//     `"__reschedule__"`, edits the first parked message's release one hour further
//     out (defer-edit). A publish error returns Err(ProcessingFailed) with the
//     diagnostic.
//
// Does not import `store` (import-GC strips it; host links it regardless —
// exercising the subset-instantiation property).
//
// Returns Ok on success (all new entries processed).

mod spec;

use crate::spec::port::OUT;
use brenn_guest::{
    Activation, Error, MessageEnvelopeExt, Processor, defer_cancel, defer_edit, publish,
    publish_deferred,
};

struct ProcessorDemo;

impl Processor for ProcessorDemo {
    fn receive(activation: Activation) -> Result<(), Error> {
        for window in activation.port_windows() {
            for env in window.new_envelopes() {
                let env = env?;

                if env.channel.is_empty() {
                    return Err(Error::failed("envelope missing non-empty 'channel' field"));
                }

                if env.body == "__trap__" {
                    unreachable!("processor-demo: deliberate trap on sentinel body __trap__");
                }

                if env.envelope_type == brenn_guest::ChannelScheme::Webhook {
                    let webhook = env.webhook_body()?;
                    // Backend activations always carry a host-stamped `now`;
                    // its absence traps.
                    if webhook.body == "__defer__" {
                        let now = activation
                            .now()
                            .expect("backend activation carries a host-stamped now");
                        publish_deferred(OUT, "deferred-payload", now + 60_000)?;
                    } else if webhook.body == "__viewcount__" {
                        let view = activation.deferred_for(OUT).ok_or_else(|| {
                            Error::failed("output port 'out' has no deferred window")
                        })?;
                        let summary = match view.entries().first() {
                            Some(e) => format!(
                                "view={} first={} at={}",
                                view.entries().len(),
                                e.payload(),
                                e.deliver_after()
                            ),
                            None => format!("view={}", view.entries().len()),
                        };
                        publish(OUT, &summary)?;
                    } else if webhook.body == "__cancel__" {
                        let view = activation.deferred_for(OUT).ok_or_else(|| {
                            Error::failed("output port 'out' has no deferred window")
                        })?;
                        if let Some(first) = view.entries().first() {
                            defer_cancel(OUT, first.index())?;
                        }
                    } else if webhook.body == "__reschedule__" {
                        let now = activation
                            .now()
                            .expect("backend activation carries a host-stamped now");
                        let view = activation.deferred_for(OUT).ok_or_else(|| {
                            Error::failed("output port 'out' has no deferred window")
                        })?;
                        if let Some(first) = view.entries().first() {
                            defer_edit(OUT, first.index(), None, Some(now + 3_600_000))?;
                        }
                    } else {
                        publish(OUT, &webhook.body)?;
                    }
                }
            }
        }

        Ok(())
    }
}

brenn_guest::export_processor!(ProcessorDemo);
