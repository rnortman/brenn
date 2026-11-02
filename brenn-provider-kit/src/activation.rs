//! A provider's activation loop: serving `provide`d tools by nested
//! `api-request` calls, at most [`CALL_BUDGET`] of them per activation, with
//! the requests past the budget kept in a backlog that later activations
//! drain as results free capacity.
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
//!    step.
//! 3. **Backlog.** Entries are issued oldest first until the budget is spent.
//! 4. **New requests.** Each is planned, then issued or queued.
//! 5. **Guard.** A backlog with no call issued this activation and no live
//!    in-flight row has nothing to wake the instance and drain it: every entry
//!    is answered `internal` now.
//!
//! At the end of every activation that returns `Ok`, a non-empty backlog
//! implies an in-flight row issued within [`NESTED_WORST_CASE_MS`], whose
//! result activates the instance again.

use serde_json::{Map, Value};

use crate::answer::{NestedError, not_issued, result_summary};
use crate::backlog::{Queued, parse_key};
use crate::call::{ApiCall, Call, ErrorKind, Next};
use crate::row::{InFlight, Tool, claim, issued_at_ms, row_expired};

/// The host answers a provided-tool call at this age (`PROVIDED_TOOL_CEILING`).
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

/// The store namespace of in-flight rows, keyed by call id.
pub const INFLIGHT_NS: &str = "inflight";

/// The store namespace of backlog entries, keyed by [`Queued::key`].
pub const BACKLOG_NS: &str = "backlog";

/// The tool every nested call reaches.
pub const NESTED_TOOL: &str = "api-request";

/// A namespace's rows as `(key, value)` pairs.
pub type Rows = Vec<(Vec<u8>, Vec<u8>)>;

/// What the loop needs of the host: one store transaction, the `provides`
/// calls and the log. Every method acts inside the activation it serves.
pub trait Host {
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
    /// correlated by `call_id`; `Err` is the host's refusal text (the call was
    /// not queued).
    fn call(&self, id: &str, call: &ApiCall, call_id: &str) -> Result<(), String>;

    /// Complete `id`; a request the host no longer holds is dropped, not an
    /// error.
    fn complete(&self, id: &str, value: &Value) -> Result<(), Self::Error>;

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

/// What the loop needs of the component: its tool enum, its settings, and
/// the two step functions.
pub trait Logic {
    type Tool: Tool;
    type Settings;

    /// A new request's first call, or the reason its arguments are refused.
    fn plan(tool: Self::Tool, args: &Value, settings: &Self::Settings) -> Result<Call, String>;

    /// What a request does once its outstanding call answered `outcome`.
    fn advance(
        state: &InFlight<Self::Tool>,
        outcome: Result<Value, NestedError>,
        settings: &Self::Settings,
    ) -> Next;
}

/// A new request on the instance's request inbox.
#[derive(Clone, Debug, PartialEq)]
pub struct Request {
    pub id: String,
    pub tool: String,
    pub args: Value,
}

/// A nested call's result on the instance's result inbox.
#[derive(Clone, Debug, PartialEq)]
pub struct Delivered {
    pub call_id: String,
    /// The pending id the call served, if any.
    pub serving: Option<String>,
    pub outcome: Result<Value, NestedError>,
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
        issued: 0,
        queued: false,
        last_refusal: None,
    };
    run.sweep()?;
    for result in input.results {
        run.answer(result)?;
    }
    let left_behind = run.drain()?;
    for request in input.requests {
        run.serve(request)?;
    }
    if (run.queued || left_behind) && run.issued == 0 && !run.live_row()? {
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
    /// Calls the host accepted this activation.
    issued: usize,
    /// Whether this activation wrote a backlog entry.
    queued: bool,
    /// The host's text for the latest refused call.
    last_refusal: Option<String>,
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
        self.host.log(
            Level::Info,
            format!(
                "{} step {} result {}",
                state.tool.name(),
                state.step,
                result_summary(&result.outcome)
            ),
        );
        match L::advance(&state, result.outcome, self.settings) {
            Next::Complete(value) => self.host.complete(&id, &value),
            Next::Fail(failure) => self.host.complete_err(&id, failure.kind, &failure.detail),
            Next::Call { call, derived } => self.issue_or_queue(Queued {
                id: state.id,
                tool: state.tool,
                args: state.args,
                derived,
                call,
                received_at_ms: state.received_at_ms,
                queued_at_ms: self.now_ms,
            }),
        }
    }

    /// Issue backlog entries oldest first until the budget is spent. Whether
    /// entries are left behind.
    fn drain(&mut self) -> Result<bool, H::Error> {
        for (key, bytes) in self.host.scan(BACKLOG_NS)? {
            if self.attempted >= CALL_BUDGET {
                return Ok(true);
            }
            let entry = self.queued_row(&key, &bytes)?;
            self.host.delete(BACKLOG_NS, &key)?;
            self.issue(entry)?;
        }
        Ok(false)
    }

    /// Serve one new request: plan its first call and issue or queue it.
    fn serve(&mut self, request: Request) -> Result<(), H::Error> {
        let Some(tool) = L::Tool::from_name(&request.tool) else {
            return Err(H::fault(format!(
                "request for tool `{}`, which this component does not provide",
                request.tool
            )));
        };
        match L::plan(tool, &request.args, self.settings) {
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
            }),
        }
    }

    /// Issue `entry`'s call if the budget allows, or keep it in the backlog.
    fn issue_or_queue(&mut self, entry: Queued<L::Tool>) -> Result<(), H::Error> {
        if self.attempted < CALL_BUDGET {
            return self.issue(entry);
        }
        self.host
            .put(BACKLOG_NS, entry.key().as_bytes(), &entry.encode())?;
        self.queued = true;
        self.host.log(
            Level::Debug,
            format!(
                "queued {} step {} for {}: {CALL_BUDGET} calls attempted this activation",
                entry.tool.name(),
                entry.call.step,
                entry.id
            ),
        );
        Ok(())
    }

    /// Issue `entry`'s call and record its in-flight row, or answer the
    /// request with the host's refusal when the call cannot be queued. A
    /// refused attempt counts against the budget.
    fn issue(&mut self, entry: Queued<L::Tool>) -> Result<(), H::Error> {
        self.attempted += 1;
        let state = entry.in_flight(self.now_ms);
        let call_id = state.call_id();
        let api = &entry.call.api;
        match self.host.call(&entry.id, api, &call_id) {
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
    /// instance to drain it.
    fn refuse_backlog(&mut self) -> Result<(), H::Error> {
        let refusal = self
            .last_refusal
            .as_deref()
            .expect("a backlog is left only once the budget is spent, and none was issued");
        let detail = format!(
            "api-request refused every call this activation could issue and none is \
             outstanding; this request was not attempted. {}",
            not_issued(refusal)
        );
        for (key, bytes) in self.host.scan(BACKLOG_NS)? {
            let entry = self.queued_row(&key, &bytes)?;
            self.host.delete(BACKLOG_NS, &key)?;
            self.host.log(
                Level::Warn,
                format!(
                    "{} step {} for {} answered: every call refused and none outstanding",
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
            },
        }
    }

    impl Logic for Steps {
        type Tool = Fixture;
        type Settings = ();

        fn plan(tool: Fixture, args: &Value, _: &()) -> Result<Call, String> {
            let q = args
                .get("q")
                .and_then(Value::as_str)
                .ok_or("`q` is required")?;
            Ok(call(1, tool, q))
        }

        fn advance(state: &InFlight<Fixture>, outcome: Result<Value, NestedError>, _: &()) -> Next {
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
            })
            .collect()
    }

    fn result(call: &NestedCall, more: bool) -> Delivered {
        Delivered {
            call_id: call.call_id.clone(),
            serving: Some(call.id.clone()),
            outcome: Ok(json!({ "status": 200, "more": more })),
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
            },
            Delivered {
                call_id: "r000/1".to_string(),
                serving: Some(id(0)),
                outcome: Ok(json!({})),
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
        };
        run(&host, input(1, vec![], vec![failed])).unwrap();
        assert_eq!(
            errors(&host.take_completions()),
            [(id(0).as_str(), ErrorKind::Internal, "timed out")]
        );
        assert!(host.rows(INFLIGHT_NS).is_empty());
    }

    /// One scripted activation: when, how many new requests, how many of the
    /// oldest outstanding calls answer, every how-many-th of those advances
    /// (0: none), how many calls the host refuses, and whether completions
    /// fail the activation.
    struct Step {
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
            Step { now_ms: 0, new: 40, answer: 0, advance_every: 0, refuse: 0, fail: false },
            Step { now_ms: 1_000, new: 10, answer: 5, advance_every: 2, refuse: 0, fail: false },
            Step { now_ms: 2_000, new: 0, answer: 10, advance_every: 3, refuse: 4, fail: false },
            Step { now_ms: 3_000, new: 50, answer: 3, advance_every: 0, refuse: 0, fail: false },
            Step { now_ms: 4_000, new: 0, answer: 6, advance_every: 2, refuse: 0, fail: true },
            Step { now_ms: 5_000, new: 5, answer: 20, advance_every: 2, refuse: 0, fail: false },
            Step { now_ms: 6_000, new: 0, answer: 0, advance_every: 0, refuse: 0, fail: false },
            Step { now_ms: 7_000, new: 30, answer: 30, advance_every: 4, refuse: 10, fail: false },
            Step { now_ms: 30_000, new: 45, answer: 0, advance_every: 0, refuse: 32, fail: false },
            Step { now_ms: 211_000, new: 40, answer: 0, advance_every: 0, refuse: 64, fail: false },
            Step { now_ms: 212_000, new: 10, answer: 10, advance_every: 2, refuse: 0, fail: false },
            Step { now_ms: 213_000, new: 0, answer: 200, advance_every: 0, refuse: 0, fail: false },
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
}
