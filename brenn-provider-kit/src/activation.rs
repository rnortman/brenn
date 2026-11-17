//! A provider's activation loop: serving `provide`d tools by nested
//! `api-request` calls, at most [`CALL_BUDGET`] of them per activation and at
//! most [`BUILD_BUDGET`] with a body the component builds, with the requests
//! past either budget kept in a backlog that later activations drain as
//! results free capacity.
//!
//! The loop names the host through [`Host`] and the component through
//! [`Logic`], so it runs on the host under test against an in-memory store.
//! One activation, in order:
//!
//! 1. **Sweep.** In-flight rows past [`INFLIGHT_TTL_MS`] are deleted; backlog
//!    entries that have waited in the backlog past [`BACKLOG_MAX_WAIT_MS`] are
//!    deleted and answered `internal`.
//! 2. **Results.** Each result claims its request's in-flight row and
//!    advances it: the request completes, fails, or issues or queues its next
//!    step. A request more than [`HOST_DEADLINE_MS`] older than the activation
//!    that delivered it is answered `internal` instead and not advanced.
//! 3. **Backlog.** Entries are issued oldest first; the drain stops at the
//!    first entry a budget leaves it unable to issue.
//! 4. **New requests.** Each is planned, then issued or queued; once the
//!    drain has left an entry behind, every one is queued.
//! 5. **Guard.** A backlog with no call issued this activation and no live
//!    in-flight row has nothing to wake the instance and drain it: every entry
//!    is answered `internal` now.
//!
//! At the end of every activation that returns `Ok`, a non-empty backlog
//! implies an in-flight row issued within [`NESTED_WORST_CASE_MS`], whose
//! result activates the instance again.
//!
//! Each step — planning a request, advancing it on an answer, building a
//! call's body, issuing a backlog entry — runs with a [`Step`] over the host's
//! [`Blobs`], and the
//! loop calls [`Blobs::release`] once the step's outcome is carried out. A
//! completion carries what the step attached; a failure carries nothing; a
//! step that attaches and then issues a call is the component's fault.

use std::borrow::Cow;

use serde_json::{Map, Value};

use crate::answer::{NestedError, not_issued, result_summary};
use crate::backlog::{Queued, parse_key};
use crate::blob::{Attached, Blob, Blobs, OpenError, Step};
use crate::call::{ApiCall, BUILT_BODY_ATTACHMENT, Body, Call, ErrorKind, Failure, Next};
use crate::row::{InFlight, Tool, claim, issued_at_ms, row_expired};

/// The host's ceiling on a provided-tool call (`PROVIDED_TOOL_CEILING`). The
/// host counts it from when it takes the call, before the call waits for the
/// tool's permit, and a nested provided call may inherit an earlier deadline
/// from the call it serves; so the host may answer a request well before this
/// long after the provider receives it.
pub const HOST_DEADLINE_MS: u64 = 240_000;

/// `api-request`'s execution worst case once a call is admitted: a token
/// refresh, the send, one more refresh and one resend on a 401 (2 ×
/// `AUTH_SERVER_TIMEOUT` + 2 × `API_TIMEOUT` in `brenn-oauth`), the case the
/// host's deadline is itself sized above. It excludes the executor's
/// admission wait before the call starts (rate limit and concurrency), which
/// is unbounded and which the host's own ceiling leaves out the same way.
pub const NESTED_WORST_CASE_MS: u64 = 180_000;

/// An in-flight row older than this is garbage: the host has answered its
/// request (`HOST_DEADLINE_MS` after receipt, and receipt is no later than
/// issue), and an admitted call's answer is due within
/// `NESTED_WORST_CASE_MS` of admission. An answer later than this — possible
/// only after the executor's unbounded admission wait — finds no row and is
/// skipped as stale.
pub const INFLIGHT_TTL_MS: u64 = HOST_DEADLINE_MS + NESTED_WORST_CASE_MS;

/// A backlog entry that has waited longer than this is answered `internal`
/// rather than issued: none of the calls ahead of it has answered in that
/// time, which is an outage of the API or its authorization server. For a
/// first step the wait is the request's age, so a call issued at the cut is
/// still answered, at `api-request`'s worst case, by the host's deadline; a
/// later step's wait counts from its earlier step's answer, as the same step
/// issued without queueing has no age check either.
pub const BACKLOG_MAX_WAIT_MS: u64 = HOST_DEADLINE_MS - NESTED_WORST_CASE_MS;

/// The most nested calls the loop issues in one activation: half the host's
/// 64-call cap, so the host's `call-count-exceeded` is never the kit's and a
/// component's own calls outside the kit have the other half. The host holds
/// its call cap to half its per-activation publish budget, which nested calls
/// share with port publishes, so the budget is within both.
pub const CALL_BUDGET: usize = 32;

/// The most bodies the loop builds ([`Logic::body`]) in one activation, a
/// failed build included: a build can fail having spent its whole cost. A
/// call whose body is built past it is queued, as a call past
/// [`CALL_BUDGET`] is. An activation's least fuel is the host's floor
/// (`PROCESSOR_FUEL_MINIMUM`, 50M), which covers its first five envelopes;
/// every further envelope adds its own 10M. With at most four builds in an
/// activation, a component whose other steps cost S and whose build costs B
/// is safe where S + 4B fits the floor. The drain builds out of the backlog
/// in activations that carry no envelope for it, so the floor is what such
/// a build runs on.
pub const BUILD_BUDGET: usize = 4;

/// The store namespace of in-flight rows, keyed by call id.
pub const INFLIGHT_NS: &str = "inflight";

/// The store namespace of backlog entries, keyed by [`Queued::key`].
pub const BACKLOG_NS: &str = "backlog";

/// The tool every nested call reaches.
pub const NESTED_TOOL: &str = "api-request";

/// A namespace's rows as `(key, value)` pairs.
pub type Rows = Vec<(Vec<u8>, Vec<u8>)>;

/// What the loop needs of the host: one store transaction, the `provides`
/// calls, the log, and the `blobs` interface its steps use. Every method acts
/// inside the activation it serves.
pub trait Host: Blobs {
    type Error;

    /// A structural fault of the loop's own (a request for an unprovided tool,
    /// a row that does not parse, a row belonging to another request).
    fn fault(detail: String) -> Self::Error;

    fn get(&self, ns: &str, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    fn put(&self, ns: &str, key: &[u8], value: &[u8]) -> Result<(), Self::Error>;

    fn delete(&self, ns: &str, key: &[u8]) -> Result<(), Self::Error>;

    /// Every row of `ns`, ascending by key.
    fn scan(&self, ns: &str) -> Result<Rows, Self::Error>;

    /// Queue the nested `api-request` call `call` serving request `id`,
    /// correlated by `call_id`, carrying `body` — the attachment named in the
    /// call's `body_attachment` and its blob — when it has one; `Err` is the
    /// host's refusal text (the call was not queued).
    fn call(
        &self,
        id: &str,
        call: &ApiCall,
        call_id: &str,
        body: Option<(&str, &Blob)>,
    ) -> Result<(), String>;

    /// Complete `id` with `value` and `attachments`; a request the host no
    /// longer holds is dropped, not an error.
    fn complete(
        &self,
        id: &str,
        value: &Value,
        attachments: &[(String, Blob)],
    ) -> Result<(), Self::Error>;

    /// Answer `id` with an error; a request the host no longer holds is
    /// dropped, not an error.
    fn complete_err(&self, id: &str, kind: ErrorKind, detail: &str) -> Result<(), Self::Error>;

    fn log(&self, level: Level, line: String);
}

/// A log line's level.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Level {
    Debug,
    Info,
    Warn,
}

/// What the loop needs of the component: its tool enum, its settings, the
/// two step functions, and the builder of a body the component builds.
pub trait Logic {
    type Tool: Tool;
    type Settings;

    /// A new request's first call, or the reason its arguments are refused.
    /// `step` reaches the request's attachments; it cannot attach.
    fn plan(
        tool: Self::Tool,
        args: &Value,
        step: &Step<'_>,
        settings: &Self::Settings,
    ) -> Result<Call, String>;

    /// What a request does once its outstanding call answered `outcome`.
    /// `step` reaches the request's and the answer's attachments; what it
    /// attaches goes with [`Next::Complete`].
    fn advance(
        state: &InFlight<Self::Tool>,
        outcome: Result<Value, NestedError>,
        step: &mut Step<'_>,
        settings: &Self::Settings,
    ) -> Next;

    /// The body of `call`, whose call returned [`Body::Built`], built when
    /// the call is issued: in the activation that planned or advanced it, or
    /// in a later one that drains it from the backlog. `call` is the call as
    /// a backlog row holds it — the tool, the caller's arguments, `derived`,
    /// the call with its step, and the request's attachment headers. `step`
    /// reaches the request's attachments and builds blobs; it carries no
    /// answer and cannot attach. A value an earlier answer taught reaches the
    /// body through `derived`. The returned blob's content type is what
    /// `api-request` sends when the call sets no `Content-Type` header.
    ///
    /// At most [`BUILD_BUDGET`] bodies are built per activation, a failed
    /// build included, so `plan` refuses everything this could refuse on the
    /// arguments, and a build fails only on what it meets at issue time: the
    /// blob pool, the activation's bound on bytes written, which every build
    /// and blob write of the activation shares, a request no longer pending.
    /// A failure answers the request and no call is attempted, so a body the
    /// write bound refuses fails its request even where a later activation
    /// would have built it.
    ///
    /// # Panics
    /// By default, naming the fault: a `Logic` that returns `Body::Built`
    /// defines this.
    // TODO(built-body-from-attachment-unfueled-after-arrival): fuel a body
    // built from a request's attachment outside the activation the request
    // arrived in, or refuse one over a declared bound when it is planned.
    fn body(
        call: &Queued<Self::Tool>,
        _step: &Step<'_>,
        _settings: &Self::Settings,
    ) -> Result<Blob, Failure> {
        panic!(
            "{} step {} for {} returns `Body::Built`, and its `Logic` defines no `body`",
            call.tool.name(),
            call.call.step,
            call.id
        )
    }
}

/// A new request on the instance's request inbox.
#[derive(Clone, Debug, PartialEq)]
pub struct Request {
    pub id: String,
    pub tool: String,
    pub args: Value,
    pub attachments: Vec<Attached>,
}

/// A nested call's result on the instance's result inbox.
#[derive(Clone, Debug, PartialEq)]
pub struct Delivered {
    pub call_id: String,
    /// The pending id the call served, if any.
    pub serving: Option<String>,
    pub outcome: Result<Value, NestedError>,
    /// The result's attachments; empty on an error outcome.
    pub attachments: Vec<Attached>,
}

/// One activation's input: the host's clock, and the new messages.
#[derive(Clone, Debug, PartialEq)]
pub struct Input {
    pub now_ms: u64,
    pub requests: Vec<Request>,
    pub results: Vec<Delivered>,
}

/// Serve one activation. `Err` is a structural fault or a host failure; the
/// caller fails the activation with it, and the host's transaction discards
/// every row this call wrote.
pub fn serve<H: Host, L: Logic>(
    host: &H,
    settings: &L::Settings,
    input: Input,
) -> Result<(), H::Error> {
    let mut run = Run::<H, L> {
        host,
        settings,
        now_ms: input.now_ms,
        attempted: 0,
        built: 0,
        issued: 0,
        queued: false,
        left_behind: false,
        last_refusal: None,
        last_build_failure: None,
    };
    run.sweep()?;
    for result in input.results {
        run.answer(result)?;
    }
    run.drain()?;
    for request in input.requests {
        run.serve(request)?;
    }
    if (run.queued || run.left_behind) && run.issued == 0 && !run.live_row()? {
        run.refuse_backlog()?;
    }
    Ok(())
}

/// One activation's state.
struct Run<'a, H: Host, L: Logic> {
    host: &'a H,
    settings: &'a L::Settings,
    now_ms: u64,
    /// Calls attempted this activation, refused ones included: the host's
    /// per-activation counter counts both.
    attempted: usize,
    /// Bodies built this activation, failed builds included.
    built: usize,
    /// Calls the host accepted this activation.
    issued: usize,
    /// Whether this activation wrote a backlog entry.
    queued: bool,
    /// Whether the drain stopped at an entry it could not issue.
    left_behind: bool,
    /// The host's text for the latest refused call.
    last_refusal: Option<String>,
    /// The detail of the latest failed build.
    last_build_failure: Option<String>,
}

impl<H: Host, L: Logic> Run<'_, H, L> {
    /// Delete every in-flight row older than the TTL, reading its age alone so
    /// a row another build wrote still expires, and answer every backlog entry
    /// that has waited in the backlog past [`BACKLOG_MAX_WAIT_MS`]. An entry's
    /// wait starts no earlier than its request's arrival, which its key spells
    /// and keys ascend by, so the backlog is read only up to the first entry
    /// whose request arrived within the wait. An entry before that point that
    /// does not parse — another build wrote it — is answered and deleted on its
    /// key alone, as an in-flight row expires on its age alone.
    // TODO(kit-sweep-decodes-expired-backlog-rows-unbudgeted): read a row's
    // times without decoding the whole row, and bound the entries answered
    // per activation.
    fn sweep(&mut self) -> Result<(), H::Error> {
        for (key, bytes) in self.host.scan(INFLIGHT_NS)? {
            if row_expired(&bytes, self.now_ms).map_err(H::fault)? {
                self.host.log(
                    Level::Debug,
                    format!("swept in-flight {}", String::from_utf8_lossy(&key)),
                );
                self.host.delete(INFLIGHT_NS, &key)?;
            }
        }
        for (key, bytes) in self.host.scan(BACKLOG_NS)? {
            let (received_at_ms, id) = parse_key(&key).map_err(H::fault)?;
            if self.now_ms.saturating_sub(received_at_ms) <= BACKLOG_MAX_WAIT_MS {
                break;
            }
            let detail = match checked_row::<L::Tool>(&key, &bytes) {
                Ok(entry) => {
                    let waited = self.now_ms.saturating_sub(entry.queued_at_ms);
                    if waited <= BACKLOG_MAX_WAIT_MS {
                        continue;
                    }
                    self.host.log(
                        Level::Warn,
                        format!(
                            "{} step {} for {id} answered after {waited} ms in the backlog",
                            entry.tool.name(),
                            entry.call.step,
                        ),
                    );
                    format!(
                        "this request waited {} s in the provider's backlog behind calls \
                         still outstanding and can no longer be answered before its deadline",
                        waited / 1000
                    )
                }
                Err(reason) => {
                    self.host.log(
                        Level::Warn,
                        format!("backlog entry for {id} answered unread: {reason}"),
                    );
                    "this request's backlog entry was written by another build of this \
                     provider and cannot be issued by this one"
                        .to_string()
                }
            };
            self.host.delete(BACKLOG_NS, &key)?;
            self.host.complete_err(id, ErrorKind::Internal, &detail)?;
        }
        Ok(())
    }

    /// Answer one result: claim its request's row and advance the request.
    /// A request more than [`HOST_DEADLINE_MS`] older than the activation that
    /// delivered it is answered `internal` and not advanced. The host's own
    /// deadline is not visible here and may fall earlier (see
    /// [`HOST_DEADLINE_MS`]), so an answer arriving between that deadline and
    /// this cutoff is still advanced.
    fn answer(&mut self, result: Delivered) -> Result<(), H::Error> {
        let Some(id) = result.serving else {
            self.host.log(
                Level::Debug,
                format!("ignored result {} serving no request", result.call_id),
            );
            return Ok(());
        };
        let row = self.host.get(INFLIGHT_NS, result.call_id.as_bytes())?;
        let Some(state) =
            claim::<L::Tool>(&id, &result.call_id, row.as_deref()).map_err(H::fault)?
        else {
            self.host.log(
                Level::Debug,
                format!(
                    "result for {} with no in-flight state: duplicate or stale",
                    result.call_id
                ),
            );
            return Ok(());
        };
        self.host.delete(INFLIGHT_NS, result.call_id.as_bytes())?;
        // TODO(correctness-max-concurrency-one-does-not-bound-attachment-decodes-per-activation):
        // cut on the host's actual deadline for this request, which the
        // provides contract does not carry.
        let age = self.now_ms.saturating_sub(state.received_at_ms);
        if age > HOST_DEADLINE_MS {
            self.host.log(
                Level::Info,
                format!(
                    "{} step {} result for {id} arrived {age} ms after the request: \
                     past the host's deadline, not advanced",
                    state.tool.name(),
                    state.step,
                ),
            );
            return self.host.complete_err(
                &id,
                ErrorKind::Internal,
                &format!(
                    "step {} of this request was answered {} s after the request arrived, \
                     past the {} s a provided tool is given; the request was not advanced",
                    state.step,
                    age / 1000,
                    HOST_DEADLINE_MS / 1000
                ),
            );
        }
        self.host.log(
            Level::Info,
            format!(
                "{} step {} result {}",
                state.tool.name(),
                state.step,
                result_summary(&result.outcome)
            ),
        );
        let mut step = Step::answering(
            self.host,
            &id,
            &result.call_id,
            state.attachments.clone(),
            result.attachments,
        );
        let next = L::advance(&state, result.outcome, &mut step, self.settings);
        let attached = step.take_attached();
        drop(step);
        let carried = match next {
            Next::Complete(value) => self.host.complete(&id, &value, &attached),
            Next::Fail(failure) => self.host.complete_err(&id, failure.kind, &failure.detail),
            Next::Call { .. } if !attached.is_empty() => Err(H::fault(format!(
                "{} step {} attached a blob to its completion and then issued a call",
                state.tool.name(),
                state.step
            ))),
            Next::Call { call, derived } => self.issue_or_queue(Queued {
                id: state.id,
                tool: state.tool,
                args: state.args,
                derived,
                call,
                received_at_ms: state.received_at_ms,
                queued_at_ms: self.now_ms,
                attachments: state.attachments,
            }),
        };
        self.host.release();
        carried
    }

    /// Issue backlog entries oldest first, stopping at the first a budget
    /// leaves this activation unable to issue: an entry is never skipped to
    /// reach a later one. Once the call budget is spent the drain stops
    /// without decoding the next row; until then each entry is decoded before
    /// deciding, since whether the build budget holds it depends on its body.
    fn drain(&mut self) -> Result<(), H::Error> {
        for (key, bytes) in self.host.scan(BACKLOG_NS)? {
            if self.attempted >= CALL_BUDGET {
                self.host.log(
                    Level::Debug,
                    format!(
                        "left the backlog from {} on: {CALL_BUDGET} calls attempted this \
                         activation",
                        String::from_utf8_lossy(&key)
                    ),
                );
                self.left_behind = true;
                return Ok(());
            }
            let entry = self.queued_row(&key, &bytes)?;
            if let Some(reason) = self.held(&entry) {
                self.host.log(
                    Level::Debug,
                    format!(
                        "left {} step {} for {} in the backlog: {reason}",
                        entry.tool.name(),
                        entry.call.step,
                        entry.id
                    ),
                );
                self.left_behind = true;
                return Ok(());
            }
            self.host.delete(BACKLOG_NS, &key)?;
            let issued = self.issue(entry);
            self.host.release();
            issued?;
        }
        Ok(())
    }

    /// Why `entry`'s call cannot be issued this activation, or `None` when it
    /// can.
    fn held(&self, entry: &Queued<L::Tool>) -> Option<String> {
        if self.left_behind {
            Some("the backlog holds an older entry this activation cannot issue".to_string())
        } else if self.attempted >= CALL_BUDGET {
            Some(format!("{CALL_BUDGET} calls attempted this activation"))
        } else if entry.call.api.body == Some(Body::Built) && self.built >= BUILD_BUDGET {
            Some(format!("{BUILD_BUDGET} bodies built this activation"))
        } else {
            None
        }
    }

    /// Serve one new request: plan its first call and issue or queue it.
    fn serve(&mut self, request: Request) -> Result<(), H::Error> {
        let Some(tool) = L::Tool::from_name(&request.tool) else {
            return Err(H::fault(format!(
                "request for tool `{}`, which this component does not provide",
                request.tool
            )));
        };
        let step = Step::planning(self.host, &request.id, request.attachments.clone());
        let planned = L::plan(tool, &request.args, &step, self.settings);
        drop(step);
        let carried = match planned {
            Err(reason) => self
                .host
                .complete_err(&request.id, ErrorKind::InvalidArgs, &reason),
            Ok(call) => self.issue_or_queue(Queued {
                id: request.id,
                tool,
                args: request.args,
                derived: Map::new(),
                call,
                received_at_ms: self.now_ms,
                queued_at_ms: self.now_ms,
                attachments: request.attachments,
            }),
        };
        self.host.release();
        carried
    }

    /// Issue `entry`'s call if the budgets allow, or keep it in the backlog.
    fn issue_or_queue(&mut self, entry: Queued<L::Tool>) -> Result<(), H::Error> {
        let Some(reason) = self.held(&entry) else {
            return self.issue(entry);
        };
        self.host
            .put(BACKLOG_NS, entry.key().as_bytes(), &entry.encode())?;
        self.queued = true;
        self.host.log(
            Level::Debug,
            format!(
                "queued {} step {} for {}: {reason}",
                entry.tool.name(),
                entry.call.step,
                entry.id
            ),
        );
        Ok(())
    }

    /// Issue `entry`'s call and record its in-flight row, or answer the
    /// request with the host's refusal when the call cannot be queued. A
    /// refused attempt counts against the call budget; a body attachment that
    /// cannot be opened, or a body that cannot be built, answers the request
    /// without an attempt. A built body goes as the call's attachment
    /// [`BUILT_BODY_ATTACHMENT`], named in its `body_attachment`.
    fn issue(&mut self, entry: Queued<L::Tool>) -> Result<(), H::Error> {
        let (api, body) = match &entry.call.api.body {
            Some(Body::Attachment(name)) => match self.open_body(&entry, name)? {
                Ok(blob) => (Cow::Borrowed(&entry.call.api), Some((name.as_str(), blob))),
                Err(failure) => {
                    return self
                        .host
                        .complete_err(&entry.id, failure.kind, &failure.detail);
                }
            },
            Some(Body::Built) => match self.build(&entry) {
                Ok(blob) => (
                    Cow::Owned(ApiCall {
                        body: Some(Body::Attachment(BUILT_BODY_ATTACHMENT.to_string())),
                        ..entry.call.api.clone()
                    }),
                    Some((BUILT_BODY_ATTACHMENT, blob)),
                ),
                Err(failure) => {
                    return self
                        .host
                        .complete_err(&entry.id, failure.kind, &failure.detail);
                }
            },
            _ => (Cow::Borrowed(&entry.call.api), None),
        };
        self.attempted += 1;
        let state = entry.in_flight(self.now_ms);
        let call_id = state.call_id();
        let api = api.as_ref();
        let body = body.as_ref().map(|(name, blob)| (*name, blob));
        match self.host.call(&entry.id, api, &call_id, body) {
            Ok(()) => {
                self.issued += 1;
                self.host.log(
                    Level::Info,
                    format!(
                        "{} step {} {} {}{}",
                        state.tool.name(),
                        state.step,
                        api.method,
                        api.host,
                        api.path
                    ),
                );
                self.host
                    .put(INFLIGHT_NS, call_id.as_bytes(), &state.encode())
            }
            Err(refusal) => {
                let detail = not_issued(&refusal);
                self.last_refusal = Some(refusal);
                self.host
                    .complete_err(&entry.id, ErrorKind::Internal, &detail)
            }
        }
    }

    /// `entry`'s body, built by the component; a failed build counts against
    /// [`BUILD_BUDGET`] as a built one does.
    // TODO(correctness-built-bodies-share-activation-write-cap-and-fail-instead-of-queue):
    // queue a build the activation's remaining write bound cannot hold
    // instead of answering it, without requeueing a body no activation holds.
    fn build(&mut self, entry: &Queued<L::Tool>) -> Result<Blob, Failure> {
        self.built += 1;
        let step = Step::planning(self.host, &entry.id, entry.attachments.clone());
        let built = L::body(entry, &step, self.settings);
        drop(step);
        if let Err(failure) = &built {
            self.host.log(
                Level::Info,
                format!(
                    "{} step {} for {}: body not built: {}",
                    entry.tool.name(),
                    entry.call.step,
                    entry.id,
                    failure.detail
                ),
            );
            self.last_build_failure = Some(failure.detail.clone());
        }
        built
    }

    /// The request's attachment `name`, which `entry`'s call forwards as its
    /// body, or the request's failure when it cannot be opened. A name the
    /// request does not carry is the component's fault.
    fn open_body(
        &self,
        entry: &Queued<L::Tool>,
        name: &str,
    ) -> Result<Result<Blob, Failure>, H::Error> {
        if !entry
            .attachments
            .iter()
            .any(|attached| attached.name == name)
        {
            return Err(H::fault(format!(
                "{} step {} for {} sends attachment `{name}` as its body, which the request \
                 does not carry",
                entry.tool.name(),
                entry.call.step,
                entry.id
            )));
        }
        Ok(match self.host.open_request(&entry.id, name) {
            Ok(blob) => Ok(blob),
            Err(OpenError::NotPending) => {
                self.host.log(
                    Level::Info,
                    format!(
                        "{} step {} for {}: the request is no longer pending; attachment \
                         `{name}` not sent",
                        entry.tool.name(),
                        entry.call.step,
                        entry.id
                    ),
                );
                Err(crate::call::internal(format!(
                    "the request is no longer pending; its attachment `{name}` was not sent"
                )))
            }
            Err(OpenError::NotAttached) => {
                return Err(H::fault(format!(
                    "the host finds no attachment `{name}` on request {}, whose message lists it",
                    entry.id
                )));
            }
            Err(other) => Err(other.into()),
        })
    }

    /// Whether some in-flight row was issued within the execution worst case
    /// of an admitted call. An older row's result may never come (it was
    /// consumed by a failed activation, lost to a restart or to an undersized
    /// result window), so it cannot be relied on to wake the instance.
    fn live_row(&self) -> Result<bool, H::Error> {
        for (_, bytes) in self.host.scan(INFLIGHT_NS)? {
            let issued = issued_at_ms(&bytes).map_err(H::fault)?;
            if self.now_ms.saturating_sub(issued) <= NESTED_WORST_CASE_MS {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Answer every backlog entry now: nothing outstanding will wake the
    /// instance to drain it. The detail names the host's refusal when a call
    /// was refused, and otherwise the failed builds.
    fn refuse_backlog(&mut self) -> Result<(), H::Error> {
        let detail = match (&self.last_refusal, &self.last_build_failure) {
            (Some(refusal), _) => format!(
                "api-request refused every call this activation could issue and none is \
                 outstanding; this request was not attempted. {}",
                not_issued(refusal)
            ),
            (None, Some(failure)) => format!(
                "this provider could not build the body of any call this activation could \
                 issue and none is outstanding; this request was not attempted. The last \
                 build failed: {failure}"
            ),
            (None, None) => panic!(
                "a backlog is left with no call issued only after a refused call or a failed build"
            ),
        };
        for (key, bytes) in self.host.scan(BACKLOG_NS)? {
            let entry = self.queued_row(&key, &bytes)?;
            self.host.delete(BACKLOG_NS, &key)?;
            self.host.log(
                Level::Warn,
                format!(
                    "{} step {} for {} answered: no call issued and none outstanding",
                    entry.tool.name(),
                    entry.call.step,
                    entry.id
                ),
            );
            self.host
                .complete_err(&entry.id, ErrorKind::Internal, &detail)?;
        }
        Ok(())
    }

    /// A backlog entry, parsed and checked against its key.
    fn queued_row(&self, key: &[u8], bytes: &[u8]) -> Result<Queued<L::Tool>, H::Error> {
        checked_row(key, bytes).map_err(H::fault)
    }
}

/// A backlog entry, parsed and checked against its key, or why it is not one.
fn checked_row<T: Tool>(key: &[u8], bytes: &[u8]) -> Result<Queued<T>, String> {
    let entry = Queued::<T>::decode(bytes)?;
    if entry.key().as_bytes() != key {
        return Err(format!(
            "backlog row under {} is {}'s",
            String::from_utf8_lossy(key),
            entry.key()
        ));
    }
    Ok(entry)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use serde_json::json;

    use super::*;
    use crate::call::{Query, internal};
    use crate::fixture::Fixture;
    use crate::testutil::{Answered, Completion, MemoryHost, NestedCall};

    /// A two-step-or-more fixture provider: a request's first call reads
    /// `/v1/<tool>/<q>`; an answer carrying `"more": true` advances it one step.
    struct Steps;

    fn call(step: u8, tool: Fixture, q: &str) -> Call {
        Call {
            step,
            api: ApiCall {
                provider: "example".to_string(),
                account: None,
                method: "GET".to_string(),
                host: "api.example.com".to_string(),
                path: format!("/v1/{}/{q}", tool.name()),
                query: Query::new(),
                headers: BTreeMap::new(),
                body: None,
                response_attachment: None,
            },
        }
    }

    impl Logic for Steps {
        type Tool = Fixture;
        type Settings = ();

        fn plan(tool: Fixture, args: &Value, _: &Step<'_>, _: &()) -> Result<Call, String> {
            let q = args
                .get("q")
                .and_then(Value::as_str)
                .ok_or("`q` is required")?;
            Ok(call(1, tool, q))
        }

        fn advance(
            state: &InFlight<Fixture>,
            outcome: Result<Value, NestedError>,
            _: &mut Step<'_>,
            _: &(),
        ) -> Next {
            match outcome {
                Ok(value) if value.get("more") == Some(&json!(true)) => Next::Call {
                    call: call(state.step + 1, state.tool, "more"),
                    derived: Map::from_iter([("from".to_string(), json!(state.step))]),
                },
                Ok(_) => Next::Complete(json!({ "tool": state.tool.name(), "step": state.step })),
                Err(error) => Next::Fail(internal(error.detail)),
            }
        }
    }

    fn id(n: usize) -> String {
        format!("r{n:03}")
    }

    fn requests(range: std::ops::Range<usize>) -> Vec<Request> {
        range
            .map(|n| Request {
                id: id(n),
                tool: "fixture-lookup".to_string(),
                args: json!({ "q": format!("q{n}") }),
                attachments: Vec::new(),
            })
            .collect()
    }

    fn result(call: &NestedCall, more: bool) -> Delivered {
        Delivered {
            call_id: call.call_id.clone(),
            serving: Some(call.id.clone()),
            outcome: Ok(json!({ "status": 200, "more": more })),
            attachments: Vec::new(),
        }
    }

    fn input(now_ms: u64, requests: Vec<Request>, results: Vec<Delivered>) -> Input {
        Input {
            now_ms,
            requests,
            results,
        }
    }

    fn run(host: &MemoryHost, input: Input) -> Result<(), String> {
        host.activate::<Steps>(&(), input)
    }

    fn ids(calls: &[NestedCall]) -> Vec<&str> {
        calls.iter().map(|call| call.id.as_str()).collect()
    }

    fn backlog_ids(host: &MemoryHost) -> Vec<String> {
        host.rows(BACKLOG_NS)
            .iter()
            .map(|(_, bytes)| Queued::<Fixture>::decode(bytes).unwrap().id)
            .collect()
    }

    fn errors(completions: &[Completion]) -> Vec<(&str, ErrorKind, &str)> {
        completions
            .iter()
            .map(|completion| match &completion.answer {
                Answered::Error(kind, detail) => (completion.id.as_str(), *kind, detail.as_str()),
                Answered::Value(value) => panic!("{} completed with {value}", completion.id),
            })
            .collect()
    }

    /// The deferral invariant: a non-empty backlog has a live in-flight row
    /// behind it.
    fn holds_invariant(host: &MemoryHost, now_ms: u64) -> bool {
        host.rows(BACKLOG_NS).is_empty()
            || host.rows(INFLIGHT_NS).iter().any(|(_, bytes)| {
                now_ms - InFlight::<Fixture>::decode(bytes).unwrap().issued_at_ms
                    <= NESTED_WORST_CASE_MS
            })
    }

    #[test]
    fn the_constants_derive_from_the_host_deadline_and_the_worst_case() {
        assert_eq!(INFLIGHT_TTL_MS, 420_000);
        assert_eq!(BACKLOG_MAX_WAIT_MS, 60_000);
    }

    #[test]
    fn requests_past_the_budget_queue_and_drain_as_results_arrive() {
        let host = MemoryHost::new();
        run(&host, input(1_000, requests(0..80), vec![])).unwrap();
        let first = host.take_calls();
        assert_eq!(first.len(), CALL_BUDGET);
        assert_eq!(ids(&first), (0..32).map(id).collect::<Vec<_>>());
        assert_eq!(host.rows(INFLIGHT_NS).len(), 32);
        assert_eq!(backlog_ids(&host), (32..80).map(id).collect::<Vec<_>>());
        assert!(host.take_completions().is_empty());
        assert!(holds_invariant(&host, 1_000));

        // Three complete, two advance: both next steps and thirty queued
        // requests spend the budget, eighteen stay queued.
        let results = (0..5).map(|n| result(&first[n], n >= 3)).collect();
        run(&host, input(2_000, vec![], results)).unwrap();
        let second = host.take_calls();
        assert_eq!(second.len(), CALL_BUDGET);
        assert_eq!(ids(&second[..2]), [id(3), id(4)]);
        assert_eq!(second[0].call_id, format!("{}/2", id(3)));
        assert_eq!(ids(&second[2..]), (32..62).map(id).collect::<Vec<_>>());
        assert_eq!(backlog_ids(&host), (62..80).map(id).collect::<Vec<_>>());
        let completed: Vec<_> = host.take_completions().into_iter().map(|c| c.id).collect();
        assert_eq!(completed, [id(0), id(1), id(2)]);
        assert!(holds_invariant(&host, 2_000));

        // Twenty more complete; the backlog empties.
        let results = first[5..25]
            .iter()
            .map(|call| result(call, false))
            .collect();
        run(&host, input(3_000, vec![], results)).unwrap();
        let third = host.take_calls();
        assert_eq!(ids(&third), (62..80).map(id).collect::<Vec<_>>());
        assert!(host.rows(BACKLOG_NS).is_empty());
        let completed: BTreeSet<String> = host
            .take_completions()
            .into_iter()
            .map(|c| c.id)
            .chain([id(0), id(1), id(2)])
            .collect();
        assert_eq!(completed.len(), 23);
        let outstanding: Vec<String> = host
            .rows(INFLIGHT_NS)
            .iter()
            .map(|(_, bytes)| InFlight::<Fixture>::decode(bytes).unwrap().id)
            .collect();
        assert_eq!(outstanding.len(), 57);
        for n in 0..80 {
            let calls = outstanding.iter().filter(|o| **o == id(n)).count();
            assert_eq!(
                usize::from(completed.contains(&id(n))) + calls,
                1,
                "{} is answered or has one call outstanding",
                id(n)
            );
        }
    }

    #[test]
    fn a_results_next_step_past_the_budget_queues_at_the_front() {
        let host = MemoryHost::new();
        run(&host, input(0, requests(0..32), vec![])).unwrap();
        run(&host, input(1, requests(32..64), vec![])).unwrap();
        let outstanding = host.take_calls();
        run(&host, input(2, requests(64..104), vec![])).unwrap();
        assert_eq!(backlog_ids(&host), (96..104).map(id).collect::<Vec<_>>());
        host.take_calls();

        let results = outstanding[..33].iter().map(|c| result(c, true)).collect();
        run(&host, input(3, vec![], results)).unwrap();
        assert_eq!(host.take_calls().len(), CALL_BUDGET);
        let mut expected = vec![id(32)];
        expected.extend((96..104).map(id));
        assert_eq!(backlog_ids(&host), expected);
        let front = Queued::<Fixture>::decode(&host.rows(BACKLOG_NS)[0].1).unwrap();
        assert_eq!(
            (front.call.step, front.received_at_ms, front.queued_at_ms),
            (2, 1, 3)
        );
        assert_eq!(
            front.derived,
            Map::from_iter([("from".to_string(), json!(1))])
        );

        run(
            &host,
            input(4, vec![], vec![result(&outstanding[40], false)]),
        )
        .unwrap();
        let drained = host.take_calls();
        assert_eq!(drained[0].call_id, format!("{}/2", id(32)));
        assert_eq!(drained.len(), 9);
        let row = host
            .get(INFLIGHT_NS, drained[0].call_id.as_bytes())
            .unwrap()
            .unwrap();
        let row = InFlight::<Fixture>::decode(&row).unwrap();
        assert_eq!((row.received_at_ms, row.issued_at_ms), (1, 4));
    }

    #[test]
    fn a_queued_request_is_issued_up_to_its_wait_and_answered_past_it() {
        for (now, issued) in [
            (BACKLOG_MAX_WAIT_MS, true),
            (BACKLOG_MAX_WAIT_MS + 1, false),
        ] {
            let host = MemoryHost::new();
            run(&host, input(0, requests(0..33), vec![])).unwrap();
            host.take_calls();
            run(&host, input(now, vec![], vec![])).unwrap();
            assert!(host.rows(BACKLOG_NS).is_empty());
            if issued {
                assert_eq!(ids(&host.take_calls()), [id(32)]);
                assert!(host.take_completions().is_empty());
            } else {
                assert!(host.take_calls().is_empty());
                assert_eq!(
                    errors(&host.take_completions()),
                    [(
                        id(32).as_str(),
                        ErrorKind::Internal,
                        "this request waited 60 s in the provider's backlog behind calls still \
                         outstanding and can no longer be answered before its deadline"
                    )]
                );
            }
        }
    }

    /// A backlog entry for request `n`'s step `step`, as the loop writes it.
    fn entry(n: usize, step: u8, received_at_ms: u64, queued_at_ms: u64) -> Queued<Fixture> {
        Queued {
            id: id(n),
            tool: Fixture::Lookup,
            args: json!({ "q": format!("q{n}") }),
            derived: Map::new(),
            call: call(step, Fixture::Lookup, "more"),
            received_at_ms,
            queued_at_ms,
            attachments: Vec::new(),
        }
    }

    fn insert_entry(host: &MemoryHost, entry: &Queued<Fixture>) {
        host.insert(BACKLOG_NS, entry.key().as_bytes(), &entry.encode());
    }

    const WAITED: &str = "s in the provider's backlog behind calls still outstanding and can no \
                          longer be answered before its deadline";

    #[test]
    fn the_sweep_answers_every_entry_past_its_wait_and_no_other() {
        let now = 100_000;
        let host = MemoryHost::new();
        // A later step of a request that arrived 90 s ago, queued 10 s ago;
        // two first steps that have waited 80 s and 70 s; one that has waited
        // 50 s; one that has just arrived.
        for queued in [
            entry(0, 2, 10_000, 90_000),
            entry(1, 1, 20_000, 20_000),
            entry(2, 1, 30_000, 30_000),
            entry(3, 1, 50_000, 50_000),
            entry(4, 1, 99_000, 99_000),
        ] {
            insert_entry(&host, &queued);
        }
        run(&host, input(now, vec![], vec![])).unwrap();
        assert_eq!(
            errors(&host.take_completions()),
            [
                (
                    id(1).as_str(),
                    ErrorKind::Internal,
                    format!("this request waited 80 {WAITED}").as_str()
                ),
                (
                    id(2).as_str(),
                    ErrorKind::Internal,
                    format!("this request waited 70 {WAITED}").as_str()
                ),
            ]
        );
        let calls = host.take_calls();
        assert_eq!(ids(&calls), [id(0), id(3), id(4)]);
        assert_eq!(calls[0].call_id, format!("{}/2", id(0)));
        assert!(host.rows(BACKLOG_NS).is_empty());
    }

    #[test]
    fn a_later_step_queued_late_in_its_request_goes_first_next_activation() {
        let host = MemoryHost::new();
        run(&host, input(0, requests(0..32), vec![])).unwrap();
        let outstanding = host.take_calls();
        run(&host, input(1, requests(32..64), vec![])).unwrap();
        let later = host.take_calls();
        // At 65 s, 33 results advance: the 33rd's next step is queued behind
        // the budget, its request older than the backlog's wait.
        let results = outstanding
            .iter()
            .chain(&later[..1])
            .map(|c| result(c, true))
            .collect();
        run(&host, input(65_000, vec![], results)).unwrap();
        assert_eq!(host.take_calls().len(), CALL_BUDGET);
        assert_eq!(backlog_ids(&host), [id(32)]);

        run(&host, input(65_200, vec![], vec![])).unwrap();
        assert!(host.take_completions().is_empty());
        assert_eq!(
            host.take_calls()
                .iter()
                .map(|c| c.call_id.as_str())
                .collect::<Vec<_>>(),
            [format!("{}/2", id(32))]
        );
        assert!(host.rows(BACKLOG_NS).is_empty());
    }

    #[test]
    fn a_backlog_entry_another_build_wrote_is_cleared_once_its_wait_has_passed() {
        let mut wire: Value = serde_json::from_slice(&entry(7, 1, 1_000, 1_000).encode()).unwrap();
        wire["tool"] = json!("fixture-moved-away");
        let key = entry(7, 1, 1_000, 1_000).key();
        let host = MemoryHost::new();
        host.insert(
            BACKLOG_NS,
            key.as_bytes(),
            &serde_json::to_vec(&wire).unwrap(),
        );

        let error = run(&host, input(1_000 + BACKLOG_MAX_WAIT_MS, vec![], vec![])).unwrap_err();
        assert!(error.starts_with("backlog row does not parse: "), "{error}");
        assert_eq!(host.rows(BACKLOG_NS).len(), 1);

        run(&host, input(1_001 + BACKLOG_MAX_WAIT_MS, vec![], vec![])).unwrap();
        assert!(host.rows(BACKLOG_NS).is_empty());
        assert!(host.take_calls().is_empty());
        assert_eq!(
            errors(&host.take_completions()),
            [(
                id(7).as_str(),
                ErrorKind::Internal,
                "this request's backlog entry was written by another build of this provider \
                 and cannot be issued by this one"
            )]
        );
        let log = host.take_log();
        assert!(
            log.iter().any(|(level, line)| *level == Level::Warn
                && line.starts_with(&format!(
                    "backlog entry for {} answered unread: backlog row does not parse: ",
                    id(7)
                ))),
            "{log:?}"
        );
    }

    #[test]
    fn a_backlog_key_the_loop_does_not_write_is_a_fault() {
        let host = MemoryHost::new();
        host.insert(BACKLOG_NS, b"r000", b"{}");
        let error = run(&host, input(1, vec![], vec![])).unwrap_err();
        assert_eq!(error, "backlog key r000 is not `<20-digit arrival>/<id>`");
    }

    #[test]
    fn an_in_flight_row_expires_only_after_the_ttl() {
        let host = MemoryHost::new();
        run(&host, input(0, requests(0..1), vec![])).unwrap();
        run(&host, input(INFLIGHT_TTL_MS, vec![], vec![])).unwrap();
        assert_eq!(host.rows(INFLIGHT_NS).len(), 1);
        run(&host, input(INFLIGHT_TTL_MS + 1, vec![], vec![])).unwrap();
        assert!(host.rows(INFLIGHT_NS).is_empty());
        assert!(
            host.take_log()
                .contains(&(Level::Debug, format!("swept in-flight {}/1", id(0))))
        );
    }

    const REFUSAL: &str = "call-async api-request: denied: no clause admits GET";

    #[test]
    fn with_every_call_refused_the_guard_answers_the_backlog_now() {
        let host = MemoryHost::new();
        host.refuse_all(REFUSAL);
        run(&host, input(0, requests(0..33), vec![])).unwrap();
        assert!(host.take_calls().is_empty());
        assert!(host.rows(BACKLOG_NS).is_empty());
        let completions = host.take_completions();
        let answered = errors(&completions);
        assert_eq!(answered.len(), 33);
        for (n, (answered_id, kind, detail)) in answered.iter().enumerate().take(32) {
            assert_eq!((*answered_id, *kind), (id(n).as_str(), ErrorKind::Internal));
            assert_eq!(*detail, not_issued(REFUSAL));
        }
        assert_eq!(answered[32].0, id(32));
        assert_eq!(
            answered[32].2,
            format!(
                "api-request refused every call this activation could issue and none is \
                 outstanding; this request was not attempted. {}",
                not_issued(REFUSAL)
            )
        );
    }

    #[test]
    fn a_refused_attempt_counts_against_the_budget() {
        let host = MemoryHost::new();
        host.refuse_next(31, REFUSAL);
        run(&host, input(0, requests(0..41), vec![])).unwrap();
        assert_eq!(ids(&host.take_calls()), [id(31)]);
        assert_eq!(errors(&host.take_completions()).len(), 31);
        assert_eq!(backlog_ids(&host), (32..41).map(id).collect::<Vec<_>>());
        assert!(holds_invariant(&host, 0));
    }

    #[test]
    fn a_row_past_its_liveness_does_not_hold_the_guard_open() {
        for (age, live) in [
            (NESTED_WORST_CASE_MS, true),
            (NESTED_WORST_CASE_MS + 1, false),
        ] {
            let host = MemoryHost::new();
            run(&host, input(0, requests(0..1), vec![])).unwrap();
            host.take_calls();
            host.refuse_all(REFUSAL);
            run(&host, input(age, requests(1..34), vec![])).unwrap();
            let answered = errors(&host.take_completions()).len();
            if live {
                assert_eq!(backlog_ids(&host), [id(33)]);
                assert_eq!(answered, 32);
            } else {
                assert!(host.rows(BACKLOG_NS).is_empty());
                assert_eq!(answered, 33);
            }
            assert!(holds_invariant(&host, age));
        }
    }

    #[test]
    fn a_failed_activation_leaves_its_rows_and_the_backlog_as_they_were() {
        let host = MemoryHost::new();
        run(&host, input(0, requests(0..33), vec![])).unwrap();
        let calls = host.take_calls();
        let inflight = host.rows(INFLIGHT_NS);
        let backlog = host.rows(BACKLOG_NS);
        host.fail_completes(true);
        let error = run(&host, input(1_000, vec![], vec![result(&calls[0], false)])).unwrap_err();
        assert_eq!(error, format!("complete {}: host failure", id(0)));
        assert_eq!(host.rows(INFLIGHT_NS), inflight);
        assert_eq!(host.rows(BACKLOG_NS), backlog);
        assert!(host.take_calls().is_empty());

        // The consumed result's row is now a phantom; the backlog drains on
        // the next activation the instance gets.
        host.fail_completes(false);
        run(&host, input(2_000, vec![], vec![])).unwrap();
        assert_eq!(ids(&host.take_calls()), [id(32)]);
        assert!(host.rows(BACKLOG_NS).is_empty());
    }

    #[test]
    fn a_request_whose_arguments_are_refused_is_answered_invalid_args() {
        let host = MemoryHost::new();
        let request = Request {
            id: id(0),
            tool: "fixture-search".to_string(),
            args: json!({}),
            attachments: Vec::new(),
        };
        run(&host, input(0, vec![request], vec![])).unwrap();
        assert!(host.take_calls().is_empty());
        assert_eq!(
            errors(&host.take_completions()),
            [(id(0).as_str(), ErrorKind::InvalidArgs, "`q` is required")]
        );
    }

    #[test]
    fn a_request_for_an_unprovided_tool_is_a_fault() {
        let host = MemoryHost::new();
        let request = Request {
            id: id(0),
            tool: "no-such-tool".to_string(),
            args: json!({ "q": "x" }),
            attachments: Vec::new(),
        };
        assert_eq!(
            run(&host, input(0, vec![request], vec![])),
            Err("request for tool `no-such-tool`, which this component does not provide".into())
        );
    }

    #[test]
    fn a_result_for_another_requests_row_is_a_fault() {
        let host = MemoryHost::new();
        run(&host, input(0, requests(0..1), vec![])).unwrap();
        let mut foreign = result(&host.take_calls()[0], false);
        foreign.serving = Some(id(9));
        let error = run(&host, input(1, vec![], vec![foreign])).unwrap_err();
        assert!(
            error.ends_with("but its in-flight row is r000's"),
            "{error}"
        );
    }

    #[test]
    fn a_corrupt_row_is_a_fault() {
        let host = MemoryHost::new();
        host.insert(INFLIGHT_NS, b"r000/1", br#"{"issued_at_ms":0}"#);
        let delivered = Delivered {
            call_id: "r000/1".to_string(),
            serving: Some(id(0)),
            outcome: Ok(json!({})),
            attachments: Vec::new(),
        };
        let error = run(&host, input(1, vec![], vec![delivered])).unwrap_err();
        assert!(
            error.starts_with("in-flight row does not parse: "),
            "{error}"
        );

        let host = MemoryHost::new();
        host.insert(BACKLOG_NS, b"00000000000000000000/r000", b"{}");
        let error = run(&host, input(1, vec![], vec![])).unwrap_err();
        assert!(error.starts_with("backlog row does not parse: "), "{error}");

        let host = MemoryHost::new();
        run(&host, input(0, requests(0..33), vec![])).unwrap();
        let (_, bytes) = host.rows(BACKLOG_NS).remove(0);
        host.insert(BACKLOG_NS, b"00000000000000000000/r999", &bytes);
        let error = run(&host, input(1, vec![], vec![])).unwrap_err();
        assert_eq!(
            error,
            "backlog row under 00000000000000000000/r999 is 00000000000000000000/r032's"
        );
    }

    #[test]
    fn a_result_serving_nothing_or_finding_no_row_is_skipped() {
        let host = MemoryHost::new();
        let results = vec![
            Delivered {
                call_id: "x/1".to_string(),
                serving: None,
                outcome: Ok(json!({})),
                attachments: Vec::new(),
            },
            Delivered {
                call_id: "r000/1".to_string(),
                serving: Some(id(0)),
                outcome: Ok(json!({})),
                attachments: Vec::new(),
            },
        ];
        run(&host, input(0, vec![], results)).unwrap();
        assert!(host.take_completions().is_empty());
        assert_eq!(
            host.take_log(),
            [
                (
                    Level::Debug,
                    "ignored result x/1 serving no request".to_string()
                ),
                (
                    Level::Debug,
                    "result for r000/1 with no in-flight state: duplicate or stale".to_string()
                ),
            ]
        );
    }

    #[test]
    fn a_nested_error_fails_the_request() {
        let host = MemoryHost::new();
        run(&host, input(0, requests(0..1), vec![])).unwrap();
        let call = &host.take_calls()[0];
        let failed = Delivered {
            call_id: call.call_id.clone(),
            serving: Some(call.id.clone()),
            outcome: Err(NestedError {
                kind: "internal".to_string(),
                detail: "timed out".to_string(),
            }),
            attachments: Vec::new(),
        };
        run(&host, input(1, vec![], vec![failed])).unwrap();
        assert_eq!(
            errors(&host.take_completions()),
            [(id(0).as_str(), ErrorKind::Internal, "timed out")]
        );
        assert!(host.rows(INFLIGHT_NS).is_empty());
    }

    #[test]
    fn an_answer_past_the_host_deadline_is_not_advanced() {
        let host = MemoryHost::new();
        run(&host, input(0, requests(0..1), vec![])).unwrap();
        run(&host, input(1, requests(1..2), vec![])).unwrap();
        let calls = host.take_calls();
        let answers = calls.iter().map(|call| result(call, true)).collect();
        run(&host, input(HOST_DEADLINE_MS + 1, vec![], answers)).unwrap();
        assert_eq!(
            errors(&host.take_completions()),
            [(
                id(0).as_str(),
                ErrorKind::Internal,
                "step 1 of this request was answered 240 s after the request arrived, past \
                 the 240 s a provided tool is given; the request was not advanced"
            )]
        );
        let next = host.take_calls();
        assert_eq!(ids(&next), [id(1).as_str()]);
        assert_eq!(next[0].call_id, format!("{}/2", id(1)));
        let rows = host.rows(INFLIGHT_NS);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, next[0].call_id.as_bytes());
    }

    /// One scripted activation: when, how many new requests, how many of the
    /// oldest outstanding calls answer, every how-many-th of those advances
    /// (0: none), how many calls the host refuses, and whether completions
    /// fail the activation.
    struct Script {
        now_ms: u64,
        new: usize,
        answer: usize,
        advance_every: usize,
        refuse: usize,
        fail: bool,
    }

    #[test]
    fn the_backlog_always_has_a_live_call_behind_it() {
        #[rustfmt::skip]
        let script = [
            Script { now_ms: 0, new: 40, answer: 0, advance_every: 0, refuse: 0, fail: false },
            Script { now_ms: 1_000, new: 10, answer: 5, advance_every: 2, refuse: 0, fail: false },
            Script { now_ms: 2_000, new: 0, answer: 10, advance_every: 3, refuse: 4, fail: false },
            Script { now_ms: 3_000, new: 50, answer: 3, advance_every: 0, refuse: 0, fail: false },
            Script { now_ms: 4_000, new: 0, answer: 6, advance_every: 2, refuse: 0, fail: true },
            Script { now_ms: 5_000, new: 5, answer: 20, advance_every: 2, refuse: 0, fail: false },
            Script { now_ms: 6_000, new: 0, answer: 0, advance_every: 0, refuse: 0, fail: false },
            Script { now_ms: 7_000, new: 30, answer: 30, advance_every: 4, refuse: 10, fail: false },
            Script { now_ms: 30_000, new: 45, answer: 0, advance_every: 0, refuse: 32, fail: false },
            Script { now_ms: 211_000, new: 40, answer: 0, advance_every: 0, refuse: 64, fail: false },
            Script { now_ms: 212_000, new: 10, answer: 10, advance_every: 2, refuse: 0, fail: false },
            Script { now_ms: 213_000, new: 0, answer: 200, advance_every: 0, refuse: 0, fail: false },
        ];
        let host = MemoryHost::new();
        let mut outstanding: Vec<NestedCall> = Vec::new();
        let mut answered: BTreeMap<String, usize> = BTreeMap::new();
        let mut next_id = 0;
        let mut guard_fired = false;
        for step in script {
            let new = requests(next_id..next_id + step.new);
            next_id += step.new;
            let take = step.answer.min(outstanding.len());
            let results = outstanding
                .drain(..take)
                .enumerate()
                .map(|(k, call)| {
                    result(&call, step.advance_every > 0 && k % step.advance_every == 0)
                })
                .collect();
            host.accept_all();
            host.refuse_next(step.refuse, REFUSAL);
            host.fail_completes(step.fail);
            let outcome = run(&host, input(step.now_ms, new, results));
            assert_eq!(outcome.is_err(), step.fail, "at {}", step.now_ms);
            outstanding.extend(host.take_calls());
            for completion in host.take_completions() {
                if let Answered::Error(_, detail) = &completion.answer {
                    guard_fired |= detail.starts_with("api-request refused every call");
                }
                *answered.entry(completion.id).or_default() += 1;
            }
            if outcome.is_ok() {
                assert!(holds_invariant(&host, step.now_ms), "at {}", step.now_ms);
            }
        }
        assert!(guard_fired, "the script reaches the guard");
        assert!(host.rows(BACKLOG_NS).is_empty());
        let held: Vec<String> = host
            .rows(INFLIGHT_NS)
            .iter()
            .map(|(_, bytes)| InFlight::<Fixture>::decode(bytes).unwrap().id)
            .collect();
        for n in 0..next_id {
            let held = held.iter().filter(|h| **h == id(n)).count();
            let answered = answered.get(&id(n)).copied().unwrap_or(0);
            assert_eq!(
                (answered, held),
                if answered == 1 { (1, 0) } else { (0, 1) },
                "{} is answered once or held by one row",
                id(n)
            );
        }
    }

    /// A provider that forwards its request's `file` as a `PUT` body, or
    /// copies a `GET`'s attachment answer piece by piece into its completion.
    struct Files;

    impl Logic for Files {
        type Tool = Fixture;
        type Settings = ();

        fn plan(tool: Fixture, args: &Value, _: &Step<'_>, _: &()) -> Result<Call, String> {
            let mut call = call(1, tool, "f");
            if args.get("put").is_some() {
                call.api.method = "PUT".to_string();
                call.api.body = Some(Body::Attachment("file".to_string()));
            } else {
                call.api = call.api.answer_as_attachment();
            }
            Ok(call)
        }

        fn advance(
            state: &InFlight<Fixture>,
            outcome: Result<Value, NestedError>,
            step: &mut Step<'_>,
            _: &(),
        ) -> Next {
            let copied = (|| {
                let answer = crate::answer::read_api_blob(outcome, &state.target, "Example")?;
                let crate::answer::BlobAnswer::Blob { attachment, .. } = answer else {
                    return Ok(None);
                };
                let blob = step.open_answer(&attachment)?;
                let mut copy = step.append("text/plain")?;
                for piece in step.pieces(&blob) {
                    copy.write(&piece?)?;
                }
                Ok::<_, Failure>(Some(copy.finish()?))
            })();
            match copied {
                Ok(Some(blob)) => {
                    step.attach("copy", blob);
                    Next::Complete(json!({ "copied": true }))
                }
                Ok(None) => Next::Complete(json!({ "copied": false })),
                Err(failure) => Next::Fail(failure),
            }
        }
    }

    #[test]
    fn attachments_go_out_as_a_body_and_come_back_into_a_completion() {
        let host = MemoryHost::new();
        let bytes: Vec<u8> = (0..5_000u32).map(|n| n as u8).collect();
        let file = host.seed_request(&id(0), "file", "application/pdf", bytes.clone());
        let put = Request {
            id: id(0),
            tool: "fixture-lookup".to_string(),
            args: json!({ "put": true }),
            attachments: vec![file.clone()],
        };
        host.activate::<Files>(&(), input(0, vec![put.clone()], vec![]))
            .unwrap();
        let calls = host.take_calls();
        assert_eq!(calls[0].body, Some(("file".to_string(), bytes.clone())));
        let row = InFlight::<Fixture>::decode(&host.rows(INFLIGHT_NS)[0].1).unwrap();
        assert_eq!(row.attachments, [file]);

        host.requests_not_pending(true);
        let late = Request { id: id(1), ..put };
        host.activate::<Files>(&(), input(1, vec![late], vec![]))
            .unwrap();
        assert!(host.take_calls().is_empty());
        assert_eq!(host.take_completions(), [not_sent(1)]);
        assert_eq!(host.rows(INFLIGHT_NS).len(), 1);
        assert!(host.rows(BACKLOG_NS).is_empty());
        assert!(
            host.take_log()
                .iter()
                .any(|(level, line)| *level == Level::Info
                    && line
                        .contains("the request is no longer pending; attachment `file` not sent"))
        );
        host.requests_not_pending(false);

        host.set_blob_limits(Some(crate::blob::BlobLimits {
            max_read_bytes: 1024,
            max_write_bytes: 1024,
        }));
        host.activate::<Files>(&(), input(2, requests(2..3), vec![]))
            .unwrap();
        let get = host.take_calls().remove(0);
        assert_eq!(get.call.response_attachment.as_deref(), Some("body"));
        let answer = host.seed_result(&get.call_id, "body", "text/plain", bytes.clone());
        let delivered = Delivered {
            call_id: get.call_id.clone(),
            serving: Some(get.id.clone()),
            outcome: crate::testutil::blob_answer("body", 5_000),
            attachments: vec![answer],
        };
        host.activate::<Files>(&(), input(3, vec![], vec![delivered]))
            .unwrap();
        let completion = host.take_completions().remove(0);
        assert_eq!(
            completion.answer,
            Answered::Value(json!({ "copied": true }))
        );
        assert_eq!(completion.attachments.len(), 1);
        assert_eq!(completion.attachments[0].name, "copy");
        assert_eq!(completion.attachments[0].bytes, bytes);
        assert_eq!(host.peak_handles(), 2);
    }

    /// The kit's failure for request `n`, whose attachment `file` it could
    /// not open because the request is no longer pending.
    fn not_sent(n: usize) -> Completion {
        Completion {
            id: id(n),
            answer: Answered::Error(
                ErrorKind::Internal,
                "the request is no longer pending; its attachment `file` was not sent".to_string(),
            ),
            attachments: Vec::new(),
        }
    }

    fn put(n: usize, file: &crate::blob::Attached) -> Request {
        Request {
            id: id(n),
            tool: "fixture-lookup".to_string(),
            args: json!({ "put": true }),
            attachments: vec![file.clone()],
        }
    }

    #[test]
    fn a_body_attachment_queued_past_the_budget_is_issued_next_activation_with_its_blob() {
        let host = MemoryHost::new();
        let mut puts = Vec::new();
        let mut files = Vec::new();
        for n in 0..=CALL_BUDGET {
            let bytes = format!("file {n}").into_bytes();
            let file = host.seed_request(&id(n), "file", "text/plain", bytes.clone());
            puts.push(put(n, &file));
            files.push((file, bytes));
        }
        host.activate::<Files>(&(), input(0, puts, vec![])).unwrap();
        let first = host.take_calls();
        assert_eq!(first.len(), CALL_BUDGET);
        for (n, call) in first.iter().enumerate() {
            assert_eq!(call.body, Some(("file".to_string(), files[n].1.clone())));
        }
        let backlog = host.rows(BACKLOG_NS);
        assert_eq!(backlog.len(), 1);
        let queued = Queued::<Fixture>::decode(&backlog[0].1).unwrap();
        assert_eq!(queued.attachments, [files[CALL_BUDGET].0.clone()]);

        host.activate::<Files>(&(), input(1, vec![], vec![]))
            .unwrap();
        let second = host.take_calls();
        assert_eq!(ids(&second), [id(CALL_BUDGET)]);
        assert_eq!(
            second[0].body,
            Some(("file".to_string(), files[CALL_BUDGET].1.clone()))
        );
        assert_eq!(second[0].call.body, Some(Body::Attachment("file".into())));
        let row = host
            .rows(INFLIGHT_NS)
            .iter()
            .map(|(_, bytes)| InFlight::<Fixture>::decode(bytes).unwrap())
            .find(|row| row.id == id(CALL_BUDGET))
            .unwrap();
        assert_eq!(row.attachments, [files[CALL_BUDGET].0.clone()]);
        assert!(host.rows(BACKLOG_NS).is_empty());
    }

    #[test]
    fn a_body_attachment_of_a_request_no_longer_pending_spends_no_budget() {
        let host = MemoryHost::new();
        let file = host.seed_request(&id(0), "file", "text/plain", b"x".to_vec());
        host.requests_not_pending(true);
        let mut new = vec![put(0, &file)];
        new.extend(requests(1..CALL_BUDGET + 1));
        host.activate::<Files>(&(), input(0, new, vec![])).unwrap();
        assert_eq!(
            ids(&host.take_calls()),
            (1..=CALL_BUDGET).map(id).collect::<Vec<_>>()
        );
        assert!(host.rows(BACKLOG_NS).is_empty());
        assert_eq!(host.take_completions(), [not_sent(0)]);
        assert!(
            host.rows(INFLIGHT_NS)
                .iter()
                .all(|(_, bytes)| InFlight::<Fixture>::decode(bytes).unwrap().id != id(0))
        );
        assert!(
            host.take_log()
                .iter()
                .any(|(level, line)| *level == Level::Info
                    && line
                        .contains("the request is no longer pending; attachment `file` not sent"))
        );
    }

    #[test]
    fn a_body_naming_an_attachment_the_request_does_not_carry_is_a_fault() {
        let host = MemoryHost::new();
        let bare = Request {
            id: id(0),
            tool: "fixture-lookup".to_string(),
            args: json!({ "put": true }),
            attachments: Vec::new(),
        };
        let fault = host
            .activate::<Files>(&(), input(0, vec![bare], vec![]))
            .unwrap_err();
        assert_eq!(
            fault,
            format!(
                "fixture-lookup step 1 for {} sends attachment `file` as its body, which the \
                 request does not carry",
                id(0)
            )
        );
        assert!(host.take_calls().is_empty());
        assert!(host.rows(INFLIGHT_NS).is_empty());
    }

    #[test]
    fn a_blob_larger_than_a_read_or_a_write_is_copied_by_range() {
        let host = MemoryHost::new();
        host.set_blob_limits(Some(crate::blob::BlobLimits {
            max_read_bytes: 1024,
            max_write_bytes: 1024,
        }));
        host.activate::<Files>(&(), input(0, requests(0..1), vec![]))
            .unwrap();
        let get = host.take_calls().remove(0);
        let bytes: Vec<u8> = (0..1u32 << 20).map(|n| (n % 251) as u8).collect();
        let answer = host.seed_result(&get.call_id, "body", "text/plain", bytes.clone());
        let delivered = Delivered {
            call_id: get.call_id.clone(),
            serving: Some(get.id.clone()),
            outcome: crate::testutil::blob_answer("body", bytes.len() as u64),
            attachments: vec![answer],
        };
        host.activate::<Files>(&(), input(1, vec![], vec![delivered]))
            .unwrap();
        let completion = host.take_completions().remove(0);
        assert_eq!(
            completion.answer,
            Answered::Value(json!({ "copied": true }))
        );
        assert_eq!(completion.attachments[0].bytes, bytes);
        assert_eq!(completion.attachments[0].content_type, "text/plain");
        assert_eq!(host.peak_handles(), 2);
    }

    /// A provider whose first call's path counts the request's attachments,
    /// and whose answer builds a blob of `size` bytes (default 1), attaches it,
    /// and then completes, fails (`"then": "fail"`) or calls again
    /// (`"then": "call"`).
    struct Attacher;

    impl Logic for Attacher {
        type Tool = Fixture;
        type Settings = ();

        fn plan(tool: Fixture, _: &Value, step: &Step<'_>, _: &()) -> Result<Call, String> {
            Ok(call(
                1,
                tool,
                &format!("n{}", step.request_attachments().len()),
            ))
        }

        fn advance(
            state: &InFlight<Fixture>,
            _: Result<Value, NestedError>,
            step: &mut Step<'_>,
            _: &(),
        ) -> Next {
            let size = state.args.get("size").and_then(Value::as_u64).unwrap_or(1) as usize;
            let built = (|| -> Result<Blob, Failure> {
                let mut blob = step.append("text/plain")?;
                blob.write(&vec![7; size])?;
                blob.finish()
            })();
            let blob = match built {
                Ok(blob) => blob,
                Err(failure) => return Next::Fail(failure),
            };
            step.attach("built", blob);
            match state.args.get("then").and_then(Value::as_str) {
                Some("fail") => Next::Fail(internal("refused after attaching")),
                Some("call") => Next::Call {
                    call: call(state.step + 1, state.tool, "more"),
                    derived: Map::new(),
                },
                _ => Next::Complete(json!({ "built": size })),
            }
        }
    }

    fn attacher(n: usize, args: Value) -> Request {
        Request {
            id: id(n),
            tool: "fixture-lookup".to_string(),
            args,
            attachments: Vec::new(),
        }
    }

    /// Plan `request` and deliver its call's answer, in two activations;
    /// the second activation's outcome.
    fn answer_attacher(host: &MemoryHost, request: Request) -> Result<(), String> {
        host.activate::<Attacher>(&(), input(0, vec![request], vec![]))
            .unwrap();
        let call = host.take_calls().remove(0);
        host.activate::<Attacher>(&(), input(1, vec![], vec![result(&call, false)]))
    }

    #[test]
    fn a_requests_attachments_reach_its_plan() {
        let host = MemoryHost::new();
        let file = host.seed_request(&id(0), "file", "text/plain", b"x".to_vec());
        let more = host.seed_request(&id(0), "more", "text/plain", b"y".to_vec());
        let mut request = attacher(0, json!({}));
        request.attachments = vec![file.clone(), more.clone()];
        host.activate::<Attacher>(&(), input(0, vec![request], vec![]))
            .unwrap();
        assert_eq!(
            host.take_calls()[0].call.path,
            "/v1/fixture-lookup/n2".to_string()
        );
        let row = InFlight::<Fixture>::decode(&host.rows(INFLIGHT_NS)[0].1).unwrap();
        assert_eq!(row.attachments, [file, more]);
    }

    #[test]
    fn what_a_step_attaches_goes_with_its_completion_only() {
        let host = MemoryHost::new();
        answer_attacher(&host, attacher(0, json!({}))).unwrap();
        let completion = host.take_completions().remove(0);
        assert_eq!(completion.answer, Answered::Value(json!({ "built": 1 })));
        assert_eq!(completion.attachments.len(), 1);
        assert_eq!(completion.attachments[0].name, "built");
        assert_eq!(completion.attachments[0].bytes, [7]);

        answer_attacher(&host, attacher(1, json!({ "then": "fail" }))).unwrap();
        let completion = host.take_completions().remove(0);
        assert_eq!(
            completion.answer,
            Answered::Error(ErrorKind::Internal, "refused after attaching".to_string())
        );
        assert!(completion.attachments.is_empty());

        let fault = answer_attacher(&host, attacher(2, json!({ "then": "call" }))).unwrap_err();
        assert_eq!(
            fault,
            "fixture-lookup step 1 attached a blob to its completion and then issued a call"
        );
        assert!(host.take_calls().is_empty());
        assert!(host.take_completions().is_empty());
    }

    #[test]
    fn each_steps_handles_are_released_before_the_next_step() {
        let host = MemoryHost::new();
        let new = (0..3).map(|n| attacher(n, json!({}))).collect();
        host.activate::<Attacher>(&(), input(0, new, vec![]))
            .unwrap();
        let results = host
            .take_calls()
            .iter()
            .map(|call| result(call, false))
            .collect();
        host.activate::<Attacher>(&(), input(1, vec![], results))
            .unwrap();
        assert_eq!(host.take_completions().len(), 3);
        assert_eq!(host.peak_handles(), 1);
    }

    #[test]
    fn a_write_past_the_activations_budget_is_refused_quota() {
        let host = MemoryHost::new();
        host.set_write_budget(1_000);
        answer_attacher(&host, attacher(0, json!({ "size": 1_001 }))).unwrap();
        let completion = host.take_completions().remove(0);
        assert_eq!(
            completion.answer,
            Answered::Error(
                ErrorKind::Internal,
                crate::Failure::from(crate::blob::WriteError::Quota).detail
            )
        );
        assert!(completion.attachments.is_empty());

        answer_attacher(&host, attacher(1, json!({ "size": 1_000 }))).unwrap();
        let completion = host.take_completions().remove(0);
        assert_eq!(completion.attachments[0].bytes.len(), 1_000);
    }

    /// A provider whose calls carry a body it builds: `"build"` plans a
    /// built `PUT`; `"two_step"` plans a plain first step whose answer's
    /// `learned` goes into `derived` and whose second step is built. The
    /// body is `derived`'s `learned`, then `text` (default `x`) `times` times
    /// (default 1), then the request's attachment `file` when it carries
    /// one; `"fail"` refuses to build it. Any other request is a plain `GET`.
    struct Builder;

    const BUILT_TYPE: &str = "application/x-built";

    fn built_put(step: u8, tool: Fixture) -> Call {
        let mut call = call(step, tool, "built");
        call.api.method = "PUT".to_string();
        call.api.body = Some(Body::Built);
        call
    }

    impl Logic for Builder {
        type Tool = Fixture;
        type Settings = ();

        fn plan(tool: Fixture, args: &Value, _: &Step<'_>, _: &()) -> Result<Call, String> {
            Ok(if args.get("build").is_some() {
                built_put(1, tool)
            } else {
                call(1, tool, "plain")
            })
        }

        fn advance(
            state: &InFlight<Fixture>,
            outcome: Result<Value, NestedError>,
            _: &mut Step<'_>,
            _: &(),
        ) -> Next {
            match outcome {
                Ok(value) if state.step == 1 && state.args.get("two_step").is_some() => {
                    Next::Call {
                        call: built_put(2, state.tool),
                        derived: Map::from_iter([(
                            "learned".to_string(),
                            value["learned"].clone(),
                        )]),
                    }
                }
                Ok(_) => Next::Complete(json!({ "step": state.step })),
                Err(error) => Next::Fail(internal(error.detail)),
            }
        }

        fn body(call: &Queued<Fixture>, step: &Step<'_>, _: &()) -> Result<Blob, Failure> {
            if call.args.get("fail").is_some() {
                return Err(internal("refused to build"));
            }
            let mut blob = step.append(BUILT_TYPE)?;
            if let Some(learned) = call.derived.get("learned").and_then(Value::as_str) {
                blob.write(learned.as_bytes())?;
            }
            let text = call.args.get("text").and_then(Value::as_str).unwrap_or("x");
            let times = call.args.get("times").and_then(Value::as_u64).unwrap_or(1);
            for _ in 0..times {
                blob.write(text.as_bytes())?;
            }
            if step.request_attachments().iter().any(|a| a.name == "file") {
                let file = step.open_request("file")?;
                for piece in step.pieces(&file) {
                    blob.write(&piece?)?;
                }
            }
            blob.finish()
        }
    }

    /// Request `n` for a built body, with `args` beside `"build"`.
    fn built(n: usize, args: Value) -> Request {
        let mut args = args;
        args["build"] = json!(true);
        Request {
            id: id(n),
            tool: "fixture-lookup".to_string(),
            args,
            attachments: Vec::new(),
        }
    }

    fn run_builder(host: &MemoryHost, input: Input) -> Result<(), String> {
        host.activate::<Builder>(&(), input)
    }

    fn built_body(bytes: &str) -> Option<(String, Vec<u8>)> {
        Some((BUILT_BODY_ATTACHMENT.to_string(), bytes.as_bytes().to_vec()))
    }

    fn logged(host: &MemoryHost, level: Level, line: &str) -> bool {
        host.take_log()
            .iter()
            .any(|(at, text)| *at == level && text == line)
    }

    #[test]
    fn a_built_body_goes_out_as_the_calls_attachment() {
        let host = MemoryHost::new();
        let request = built(0, json!({ "text": "ab", "times": 3 }));
        run_builder(&host, input(0, vec![request], vec![])).unwrap();
        let call = host.take_calls().remove(0);
        assert_eq!(call.body, built_body("ababab"));
        assert_eq!(call.body_content_type.as_deref(), Some(BUILT_TYPE));
        assert_eq!(
            call.call.body,
            Some(Body::Attachment(BUILT_BODY_ATTACHMENT.to_string()))
        );
        let sent = serde_json::to_value(&call.call).unwrap();
        assert_eq!(sent["body_attachment"], json!(BUILT_BODY_ATTACHMENT));
        assert!(sent.get("body_built").is_none(), "{sent}");
        let row = InFlight::<Fixture>::decode(&host.rows(INFLIGHT_NS)[0].1).unwrap();
        assert_eq!(row.target, built_put(1, Fixture::Lookup).api.target());
        assert!(host.rows(BACKLOG_NS).is_empty());
        assert_eq!(host.peak_handles(), 1);
    }

    #[test]
    fn a_body_built_after_an_earlier_step_sees_its_derived() {
        let host = MemoryHost::new();
        let request = Request {
            id: id(0),
            tool: "fixture-lookup".to_string(),
            args: json!({ "two_step": true }),
            attachments: Vec::new(),
        };
        run_builder(&host, input(0, vec![request], vec![])).unwrap();
        let first = host.take_calls().remove(0);
        assert_eq!(first.body, None);
        let answered = Delivered {
            outcome: Ok(json!({ "status": 200, "learned": "learned-" })),
            ..result(&first, false)
        };
        run_builder(&host, input(1, vec![], vec![answered])).unwrap();
        let second = host.take_calls().remove(0);
        assert_eq!(second.call_id, format!("{}/2", id(0)));
        assert_eq!(second.body, built_body("learned-x"));
    }

    #[test]
    fn built_bodies_past_the_build_budget_queue_and_are_built_next_activation() {
        let host = MemoryHost::new();
        let mut new: Vec<Request> = (0..=BUILD_BUDGET)
            .map(|n| built(n, json!({ "text": id(n) })))
            .collect();
        new.extend(requests(BUILD_BUDGET + 1..BUILD_BUDGET + 2));
        run_builder(&host, input(0, new, vec![])).unwrap();
        let first = host.take_calls();
        let mut expected: Vec<String> = (0..BUILD_BUDGET).map(id).collect();
        expected.push(id(BUILD_BUDGET + 1));
        assert_eq!(ids(&first), expected);
        for (n, call) in first[..BUILD_BUDGET].iter().enumerate() {
            assert_eq!(call.body, built_body(&id(n)));
        }
        assert_eq!(first[BUILD_BUDGET].body, None);
        assert_eq!(backlog_ids(&host), [id(BUILD_BUDGET)]);
        let row = &host.rows(BACKLOG_NS)[0].1;
        let wire: Value = serde_json::from_slice(row).unwrap();
        assert_eq!(wire["call"]["api"]["body_built"], json!(true));
        for field in ["body", "body_base64", "body_attachment"] {
            assert!(wire["call"]["api"].get(field).is_none(), "{wire}");
        }
        assert!(logged(
            &host,
            Level::Debug,
            &format!(
                "queued fixture-lookup step 1 for {}: {BUILD_BUDGET} bodies built this activation",
                id(BUILD_BUDGET)
            )
        ));
        assert!(holds_invariant(&host, 0));

        run_builder(&host, input(1, vec![], vec![])).unwrap();
        let second = host.take_calls();
        assert_eq!(ids(&second), [id(BUILD_BUDGET)]);
        assert_eq!(second[0].body, built_body(&id(BUILD_BUDGET)));
        assert!(host.rows(BACKLOG_NS).is_empty());
    }

    #[test]
    fn a_built_call_past_the_call_budget_is_built_and_issued_next_activation() {
        let host = MemoryHost::new();
        let mut new = requests(0..CALL_BUDGET);
        new.push(built(CALL_BUDGET, json!({ "text": "late" })));
        run_builder(&host, input(0, new, vec![])).unwrap();
        let first = host.take_calls();
        assert_eq!(first.len(), CALL_BUDGET);
        assert!(first.iter().all(|call| call.body.is_none()));
        assert_eq!(backlog_ids(&host), [id(CALL_BUDGET)]);

        run_builder(&host, input(1, vec![], vec![])).unwrap();
        let second = host.take_calls();
        assert_eq!(ids(&second), [id(CALL_BUDGET)]);
        assert_eq!(second[0].body, built_body("late"));
        assert!(host.rows(BACKLOG_NS).is_empty());
    }

    /// Requests `0..=BUILD_BUDGET`, each building `pre-` and its seeded
    /// attachment `file`, `file <n>`.
    fn built_with_files(host: &MemoryHost) -> Vec<Request> {
        (0..=BUILD_BUDGET)
            .map(|n| {
                let file = host.seed_request(
                    &id(n),
                    "file",
                    "text/plain",
                    format!("file {n}").into_bytes(),
                );
                Request {
                    attachments: vec![file],
                    ..built(n, json!({ "text": "pre-" }))
                }
            })
            .collect()
    }

    #[test]
    fn a_body_built_from_a_requests_attachment_is_built_when_its_call_is_issued() {
        let host = MemoryHost::new();
        run_builder(&host, input(0, built_with_files(&host), vec![])).unwrap();
        let first = host.take_calls();
        for (n, call) in first.iter().enumerate() {
            assert_eq!(call.body, built_body(&format!("pre-file {n}")));
        }
        assert_eq!(first.len(), BUILD_BUDGET);
        let queued = Queued::<Fixture>::decode(&host.rows(BACKLOG_NS)[0].1).unwrap();
        assert_eq!(queued.attachments[0].name, "file");

        run_builder(&host, input(1, vec![], vec![])).unwrap();
        let second = host.take_calls();
        assert_eq!(ids(&second), [id(BUILD_BUDGET)]);
        assert_eq!(
            second[0].body,
            built_body(&format!("pre-file {BUILD_BUDGET}"))
        );
    }

    #[test]
    fn a_built_body_of_a_request_no_longer_pending_spends_no_call() {
        let host = MemoryHost::new();
        run_builder(&host, input(0, built_with_files(&host), vec![])).unwrap();
        host.take_calls();
        host.requests_not_pending(true);
        let after = BUILD_BUDGET + 1;
        run_builder(
            &host,
            input(1, requests(after..after + CALL_BUDGET), vec![]),
        )
        .unwrap();
        assert_eq!(
            ids(&host.take_calls()),
            (after..after + CALL_BUDGET).map(id).collect::<Vec<_>>()
        );
        assert_eq!(
            errors(&host.take_completions()),
            [(
                id(BUILD_BUDGET).as_str(),
                ErrorKind::Internal,
                "blob open: the request is no longer pending"
            )]
        );
        assert!(host.rows(BACKLOG_NS).is_empty());
    }

    #[test]
    fn the_drain_past_the_call_budget_decodes_no_row() {
        let host = MemoryHost::new();
        run(&host, input(1_000, requests(0..CALL_BUDGET), vec![])).unwrap();
        let first = host.take_calls();
        // Not a backlog row: decoding it would fail the activation.
        let key = format!("{:020}/{}", 500, id(99));
        host.insert(BACKLOG_NS, key.as_bytes(), b"not a backlog row");
        host.take_log();

        let results = first.iter().map(|call| result(call, true)).collect();
        run(&host, input(2_000, vec![], results)).unwrap();
        assert_eq!(host.take_calls().len(), CALL_BUDGET);
        let line = format!(
            "left the backlog from {key} on: {CALL_BUDGET} calls attempted this activation"
        );
        let log = host.take_log();
        assert!(log.contains(&(Level::Debug, line)), "{log:?}");
        assert_eq!(host.rows(BACKLOG_NS).len(), 1);
    }

    #[test]
    fn the_drain_stops_at_a_built_entry_the_build_budget_holds() {
        let host = MemoryHost::new();
        let two_step = (0..BUILD_BUDGET)
            .map(|n| Request {
                id: id(n),
                tool: "fixture-lookup".to_string(),
                args: json!({ "two_step": true }),
                attachments: Vec::new(),
            })
            .collect();
        run_builder(&host, input(0, two_step, vec![])).unwrap();
        let outstanding = host.take_calls();
        let mut held = entry(10, 1, 0, 0);
        held.args = json!({ "build": true });
        held.call = built_put(1, Fixture::Lookup);
        insert_entry(&host, &held);
        insert_entry(&host, &entry(11, 1, 0, 0));

        let results = outstanding.iter().map(|c| result(c, false)).collect();
        run_builder(&host, input(1, requests(12..13), results)).unwrap();
        let second = host.take_calls();
        assert_eq!(ids(&second), (0..BUILD_BUDGET).map(id).collect::<Vec<_>>());
        assert!(second.iter().all(|call| call.body == built_body("x")));
        assert_eq!(backlog_ids(&host), [id(10), id(11), id(12)]);
        let log = host.take_log();
        for line in [
            format!(
                "left fixture-lookup step 1 for {} in the backlog: {BUILD_BUDGET} bodies built \
                 this activation",
                id(10)
            ),
            format!(
                "queued fixture-lookup step 1 for {}: the backlog holds an older entry this \
                 activation cannot issue",
                id(12)
            ),
        ] {
            assert!(log.contains(&(Level::Debug, line)), "{log:?}");
        }
        assert!(holds_invariant(&host, 1));

        run_builder(&host, input(2, vec![], vec![])).unwrap();
        let third = host.take_calls();
        assert_eq!(ids(&third), [id(10), id(11), id(12)]);
        assert_eq!(third[0].body, built_body("x"));
        assert!(host.rows(BACKLOG_NS).is_empty());
    }

    #[test]
    fn a_failed_build_answers_its_request_and_spends_the_build_budget() {
        let host = MemoryHost::new();
        let mut new = vec![built(0, json!({ "fail": true }))];
        new.extend((1..=BUILD_BUDGET).map(|n| built(n, json!({}))));
        run_builder(&host, input(0, new, vec![])).unwrap();
        assert_eq!(
            ids(&host.take_calls()),
            (1..BUILD_BUDGET).map(id).collect::<Vec<_>>()
        );
        assert_eq!(
            errors(&host.take_completions()),
            [(id(0).as_str(), ErrorKind::Internal, "refused to build")]
        );
        assert_eq!(backlog_ids(&host), [id(BUILD_BUDGET)]);
        assert!(logged(
            &host,
            Level::Info,
            &format!(
                "fixture-lookup step 1 for {}: body not built: refused to build",
                id(0)
            )
        ));
        assert!(
            host.rows(INFLIGHT_NS)
                .iter()
                .all(|(_, bytes)| InFlight::<Fixture>::decode(bytes).unwrap().id != id(0))
        );
    }

    const NOT_BUILT: &str = "this provider could not build the body of any call this activation \
                             could issue and none is outstanding; this request was not attempted. \
                             The last build failed: refused to build";

    #[test]
    fn with_every_build_failed_and_nothing_outstanding_the_guard_answers_the_backlog() {
        let failing = |range: std::ops::Range<usize>| -> Vec<Request> {
            range.map(|n| built(n, json!({ "fail": true }))).collect()
        };
        let host = MemoryHost::new();
        run_builder(&host, input(0, failing(0..BUILD_BUDGET + 2), vec![])).unwrap();
        assert!(host.take_calls().is_empty());
        assert!(host.rows(BACKLOG_NS).is_empty());
        let completions = host.take_completions();
        let answered = errors(&completions);
        let mut expected: Vec<(String, &str)> = (0..BUILD_BUDGET)
            .map(|n| (id(n), "refused to build"))
            .collect();
        expected.extend((BUILD_BUDGET..BUILD_BUDGET + 2).map(|n| (id(n), NOT_BUILT)));
        assert_eq!(
            answered
                .iter()
                .map(|(answered_id, kind, detail)| {
                    assert_eq!(*kind, ErrorKind::Internal);
                    (answered_id.to_string(), *detail)
                })
                .collect::<Vec<_>>(),
            expected
        );

        let host = MemoryHost::new();
        run_builder(&host, input(0, requests(0..1), vec![])).unwrap();
        host.take_calls();
        run_builder(&host, input(1, failing(1..BUILD_BUDGET + 3), vec![])).unwrap();
        assert!(host.take_calls().is_empty());
        assert_eq!(errors(&host.take_completions()).len(), BUILD_BUDGET);
        assert_eq!(
            backlog_ids(&host),
            [id(BUILD_BUDGET + 1), id(BUILD_BUDGET + 2)]
        );
        assert!(holds_invariant(&host, 1));
    }

    #[test]
    fn a_refused_built_call_releases_its_body_and_the_guard_names_the_refusal() {
        let host = MemoryHost::new();
        host.refuse_all(REFUSAL);
        let mut new = vec![built(0, json!({ "fail": true }))];
        new.extend((1..=BUILD_BUDGET).map(|n| built(n, json!({}))));
        run_builder(&host, input(0, new, vec![])).unwrap();
        assert!(host.take_calls().is_empty());
        assert!(host.rows(BACKLOG_NS).is_empty());
        assert!(host.rows(INFLIGHT_NS).is_empty());
        assert_eq!(host.peak_handles(), 1);
        let refused = not_issued(REFUSAL);
        let guard = format!(
            "api-request refused every call this activation could issue and none is \
             outstanding; this request was not attempted. {refused}"
        );
        let mut expected = vec![(id(0), "refused to build".to_string())];
        expected.extend((1..BUILD_BUDGET).map(|n| (id(n), refused.clone())));
        expected.push((id(BUILD_BUDGET), guard));
        assert_eq!(
            errors(&host.take_completions())
                .iter()
                .map(|(answered_id, kind, detail)| {
                    assert_eq!(*kind, ErrorKind::Internal);
                    (answered_id.to_string(), detail.to_string())
                })
                .collect::<Vec<_>>(),
            expected
        );
    }

    #[test]
    fn a_built_body_served_no_blobs_fails_naming_the_entry_point() {
        let host = MemoryHost::new();
        host.set_blob_limits(None);
        run_builder(&host, input(0, vec![built(0, json!({}))], vec![])).unwrap();
        assert!(host.take_calls().is_empty());
        let completion = host.take_completions().remove(0);
        let Answered::Error(ErrorKind::Internal, detail) = completion.answer else {
            panic!("completed with {:?}", completion.answer);
        };
        assert!(detail.contains("`serve_with_blobs`"), "{detail}");
    }

    /// A provider that returns a built body and defines no `body`.
    struct NoBody;

    impl Logic for NoBody {
        type Tool = Fixture;
        type Settings = ();

        fn plan(tool: Fixture, _: &Value, _: &Step<'_>, _: &()) -> Result<Call, String> {
            Ok(built_put(1, tool))
        }

        fn advance(
            _: &InFlight<Fixture>,
            _: Result<Value, NestedError>,
            _: &mut Step<'_>,
            _: &(),
        ) -> Next {
            Next::Fail(internal("never answered"))
        }
    }

    #[test]
    #[should_panic(
        expected = "fixture-lookup step 1 for r000 returns `Body::Built`, and its `Logic` defines no `body`"
    )]
    fn a_built_body_with_no_body_defined_panics_naming_the_fault() {
        let host = MemoryHost::new();
        host.activate::<NoBody>(&(), input(0, requests(0..1), vec![]))
            .unwrap();
    }

    #[test]
    fn a_step_served_no_blobs_fails_naming_the_entry_point() {
        let host = MemoryHost::new();
        host.set_blob_limits(None);
        answer_attacher(&host, attacher(0, json!({}))).unwrap();
        let completion = host.take_completions().remove(0);
        let Answered::Error(ErrorKind::Internal, detail) = completion.answer else {
            panic!("completed with {:?}", completion.answer);
        };
        assert!(detail.contains("`serve_with_blobs`"), "{detail}");
    }
}
