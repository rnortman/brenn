// The transplant fixture: one artifact, two hostings, one transcript.
//
// Every observable this fixture produces is a function of the activation it was
// handed plus its own config map, so a host that implements the contract
// correctly produces byte-identical transcripts under wasmtime and in a
// browser. Nothing here is host-aware.
//
// Per activation, in order:
//   1. Read config keys "greeting" (present) and "absent" (missing).
//   2. Log one info line.
//   3. Publish a summary of every port window, every output-port deferred
//      window, and the activation's `now` to "out".
//   4. Publish one marker per new envelope body to "out".
//   5. Run the deferral markers below, in window order.
//   6. Honour the err/trap sentinels below.
//
// Markers, matched against a new envelope's body. A body starting with "__" is
// always a marker; an unrecognized or unparseable one is an error, never a
// silently ignored message.
//   "__err__"                     — return Err after everything above has been
//                                   buffered. A conforming host discards the
//                                   whole buffer: the transcript shows an err
//                                   activation with nothing flushed.
//   "__trap__"                    — trap after the same buffering. Same
//                                   discard, plus the instance is terminal.
//   "__park__:<delay_ms>"         — publish-deferred "parked:<delay_ms>" at
//                                   `now + delay_ms`.
//   "__cancel__:<index>"          — defer-cancel that index of "out"'s window.
//   "__edit__:<index>:<delay_ms>" — defer-edit both halves: body
//                                   "edited@<index>", release `now + delay_ms`.
//   "__retime__:<index>:<delay>"  — defer-edit the release half only, leaving
//                                   the body alone.
//
// The three timing markers need a clock and read it from `now`; an activation
// that carries none fails them rather than inventing a time. That is the
// portability claim under test: a guest computes absolute release instants from
// what the host hands it, identically on either host.
//
// Deliberately stateless across activations. The two hostings genuinely differ
// here — the wasmtime host builds a fresh store per invocation, a browser
// instance's linear memory lives as long as the instance — and neither the
// contract nor `processor.wit` promises either behaviour. A fixture that
// carried a counter would pin that divergence into the transcript and fail for
// a reason the contract never claimed.

mod spec;

use crate::spec::{config, log, port::OUT};
use brenn_guest::{
    Activation, Error, Processor, defer_cancel, defer_edit, publish, publish_deferred,
};

#[derive(serde::Serialize)]
struct PortSummary<'a> {
    port: &'a str,
    /// `message_id` of every envelope in the window, context first — the
    /// window's identity, independent of body encoding.
    ids: Vec<String>,
    new_from: usize,
    dropped: u32,
}

/// One parked message as the summary reports it — the whole of what a
/// `deferred-entry` carries, so a host that loses or reorders a field shows it.
#[derive(serde::Serialize)]
struct DeferredEntrySummary<'a> {
    index: u32,
    payload: &'a str,
    deliver_after: u64,
}

#[derive(serde::Serialize)]
struct DeferredSummary<'a> {
    port: &'a str,
    entries: Vec<DeferredEntrySummary<'a>>,
}

#[derive(serde::Serialize)]
struct ActivationSummary<'a> {
    ports: Vec<PortSummary<'a>>,
    /// One window per bound output port, in config order — this component's own
    /// parked messages, release-ordered.
    deferred: Vec<DeferredSummary<'a>>,
    /// The host's wall clock at drain, epoch milliseconds UTC; `null` on a host
    /// that exposes none.
    now: Option<u64>,
    /// Present key, then a deliberately absent one — `null` distinguishes
    /// "no such key" from "empty value" in the transcript.
    greeting: Option<String>,
    absent: Option<String>,
}

/// What one marker body asks the fixture to do after its publishes are buffered.
enum Marker {
    Err,
    Trap,
    Park { delay_ms: u64 },
    Cancel { index: u32 },
    Edit { index: u32, delay_ms: u64 },
    Retime { index: u32, delay_ms: u64 },
}

/// Parse a new envelope's body. `Ok(None)` is an ordinary message; a body
/// starting with `__` is a marker and an unrecognized one is an error.
fn parse_marker(body: &str) -> Result<Option<Marker>, Error> {
    if !body.starts_with("__") {
        return Ok(None);
    }
    let mut parts = body.split(':');
    let name = parts.next().unwrap_or_default();
    let args: Vec<&str> = parts.collect();
    let arg = |i: usize| -> Result<u64, Error> {
        args.get(i)
            .ok_or_else(|| Error::failed(format!("marker {body}: missing argument {i}")))?
            .parse::<u64>()
            .map_err(|e| Error::failed(format!("marker {body}: argument {i}: {e}")))
    };
    let marker = match name {
        "__err__" => Marker::Err,
        "__trap__" => Marker::Trap,
        "__park__" => Marker::Park { delay_ms: arg(0)? },
        "__cancel__" => Marker::Cancel {
            index: arg(0)? as u32,
        },
        "__edit__" => Marker::Edit {
            index: arg(0)? as u32,
            delay_ms: arg(1)?,
        },
        "__retime__" => Marker::Retime {
            index: arg(0)? as u32,
            delay_ms: arg(1)?,
        },
        _ => return Err(Error::failed(format!("marker {body}: unknown"))),
    };
    Ok(Some(marker))
}

struct ProcessorTransplant;

impl Processor for ProcessorTransplant {
    fn receive(activation: Activation) -> Result<(), Error> {
        let windows: Vec<_> = activation.port_windows().collect();

        let mut ports = Vec::with_capacity(windows.len());
        let mut markers: Vec<String> = Vec::new();
        let mut actions: Vec<Marker> = Vec::new();
        let mut sentinel: Option<Marker> = None;

        for window in &windows {
            let mut ids = Vec::new();
            for result in window.context_envelopes() {
                ids.push(result?.message_id.to_string());
            }
            let new_from = ids.len();
            for result in window.new_envelopes() {
                let env = result?;
                ids.push(env.message_id.to_string());
                match parse_marker(&env.body)? {
                    None => markers.push(format!("{}:{}", window.port(), env.body)),
                    Some(m @ (Marker::Err | Marker::Trap)) => sentinel = Some(m),
                    Some(m) => actions.push(m),
                }
            }
            ports.push(PortSummary {
                port: window.port(),
                ids,
                new_from,
                dropped: window.dropped(),
            });
        }

        let deferred = activation
            .deferred_windows()
            .map(|window| DeferredSummary {
                port: window.port(),
                entries: window
                    .entries()
                    .iter()
                    .map(|entry| DeferredEntrySummary {
                        index: entry.index(),
                        payload: entry.payload(),
                        deliver_after: entry.deliver_after(),
                    })
                    .collect(),
            })
            .collect();

        let summary = ActivationSummary {
            ports,
            deferred,
            now: activation.now(),
            greeting: config::get("greeting"),
            absent: config::get("absent"),
        };

        log::info("transplant activation");

        publish(
            OUT,
            &serde_json::to_string(&summary)
                .map_err(|e| Error::failed(format!("serialize summary: {e}")))?,
        )?;
        for marker in &markers {
            publish(OUT, marker)?;
        }

        // A release instant is `now` plus the marker's delay: the host owns the
        // clock, the guest owns the offset.
        let release_at = |delay_ms: u64| -> Result<u64, Error> {
            activation
                .now()
                .map(|now| now + delay_ms)
                .ok_or_else(|| Error::failed("timing marker on an activation with no `now`"))
        };
        for action in &actions {
            match action {
                Marker::Park { delay_ms } => {
                    publish_deferred(OUT, &format!("parked:{delay_ms}"), release_at(*delay_ms)?)?
                }
                Marker::Cancel { index } => defer_cancel(OUT, *index)?,
                Marker::Edit { index, delay_ms } => defer_edit(
                    OUT,
                    *index,
                    Some(&format!("edited@{index}")),
                    Some(release_at(*delay_ms)?),
                )?,
                Marker::Retime { index, delay_ms } => {
                    defer_edit(OUT, *index, None, Some(release_at(*delay_ms)?))?
                }
                Marker::Err | Marker::Trap => unreachable!("err/trap are sentinels, not actions"),
            }
        }

        match sentinel {
            Some(Marker::Err) => Err(Error::failed("transplant: deliberate err sentinel")),
            Some(Marker::Trap) => unreachable!("transplant: deliberate trap sentinel"),
            _ => Ok(()),
        }
    }
}

brenn_guest::export_processor!(ProcessorTransplant);
