//! The `google-calendar-write` component, end to end, on the rig in
//! [`super::google_workspace_rig`].

use std::sync::Arc;

use brenn_messaging::testutils::owed_everywhere;
use brenn_tool_registry::{HeldAttachments, ToolError, ToolOutput};
use brenn_wasm_dispatch::ActivationLimits;
use serde_json::{Value, json};
use tokio::task::JoinHandle;

use super::google_workspace_rig::{
    self as workspace, ACCOUNT, BEARER, CALENDAR_WRITE, Clauses, FirstRead, GUIDE_BUS_CAP,
    RigOptions, google_query,
};
use super::kit_body_fuel_measure::{least_under, measure_all};
use super::kit_file_e2e::BUILD_BUDGET;
use super::provider_rig::{MARGIN, fuel, margin_of};
use super::stub_rig::{Pairs, StubRig};

const PRIMARY_EVENTS: &str = "/calendar/v3/calendars/primary/events";

/// The instance's `api-request` clauses: delete's read first, then the three
/// writes.
const CLAUSES: Clauses<'static> = &[
    ("GET", "/calendar/v3/calendars/*/events/*"),
    ("POST", "/calendar/v3/calendars/*/events"),
    ("PATCH", "/calendar/v3/calendars/*/events/*"),
    ("DELETE", "/calendar/v3/calendars/*/events/*"),
];

/// The rig with every clause and alice's one account connected.
async fn rig() -> StubRig {
    workspace::rig(&CALENDAR_WRITE, CLAUSES).await
}

/// The credential cases' first call: `CalendarUpdate`'s `PATCH`.
fn first_reads() -> [FirstRead; 1] {
    [FirstRead {
        tool: "CalendarUpdate",
        slug: "calendar-update",
        args: json!({ "id": "e1", "summary": "Standup" }),
        method: "PATCH",
        path: "/calendar/v3/calendars/primary/events/e1",
        body: json!({ "id": "e1", "summary": "Standup" }).to_string(),
    }]
}

/// With no account connected, `api-request`'s `credential_invalid` value is
/// the tool's answer unchanged, and the component makes no further call.
#[tokio::test]
async fn no_connected_account_is_relayed_as_credential_invalid() {
    for read in first_reads() {
        workspace::assert_no_connected_account(&CALENDAR_WRITE, CLAUSES, read).await;
    }
}

/// With two accounts connected, a call naming none is `denied` with
/// `api-request`'s list of them, and a call naming one sends that account's
/// bearer.
#[tokio::test]
async fn several_accounts_need_one_named() {
    for read in first_reads() {
        workspace::assert_several_accounts_need_one_named(&CALENDAR_WRITE, CLAUSES, read).await;
    }
}

/// A component calling a tool on its own account has no user, and an
/// instance whose clauses say `user = "*"` lends none: the caller's result is
/// `denied` carrying `api-request`'s explanation, and nothing reaches the API.
#[tokio::test]
async fn a_userless_caller_is_denied_with_the_reason() {
    for read in first_reads() {
        workspace::assert_a_userless_caller_is_denied_with_the_reason(
            &CALENDAR_WRITE,
            CLAUSES,
            read,
        )
        .await;
    }
}

/// The query an event write sends: whom Google notifies, and the write's
/// mask, so the answer is the event as `CalendarEvent` reads it, without its
/// description.
fn event_write_query(notify: &str) -> Pairs {
    google_query(&[
        ("sendUpdates", notify),
        ("maxAttendees", "50"),
        (
            "fields",
            "id,status,summary,location,start,end,\
             organizer(email,displayName,self),\
             attendees(email,displayName,responseStatus,optional,self),\
             attendeesOmitted,recurrence,recurringEventId,hangoutLink,\
             conferenceData/entryPoints(entryPointType,uri)",
        ),
    ])
}

/// The one body the stub received, the event the component built, as JSON;
/// it was sent as `application/json`.
fn the_body(r: &StubRig) -> serde_json::Value {
    let bodies = r.bodies();
    assert_eq!(bodies.len(), 1, "exactly one request carried a body");
    let stub = r.stub.lock().unwrap();
    let sent = stub
        .api_seen
        .iter()
        .find(|seen| !seen.body.is_empty())
        .unwrap();
    assert_eq!(sent.headers["content-type"], "application/json");
    serde_json::from_slice(&bodies[0]).unwrap()
}

/// `CalendarCreate` creates the event with one `POST` carrying alice's
/// bearer, nobody notified by default, and a body of only the fields given;
/// the answer is Google's event, compacted as `CalendarEvent` answers one.
#[tokio::test]
async fn calendar_create_posts_one_insert_with_send_updates_none() {
    let r = rig().await;
    r.answer(
        PRIMARY_EVENTS,
        serde_json::to_string_pretty(&json!({
            "id": "new0123456789",
            "status": "confirmed",
            "summary": "Lunch",
            "start": { "dateTime": "2026-11-02T12:00:00-05:00", "timeZone": "America/New_York" },
            "end": { "dateTime": "2026-11-02T13:00:00-05:00", "timeZone": "America/New_York" },
            "attendees": [{ "email": "bob@example.com", "responseStatus": "needsAction" }],
        }))
        .unwrap(),
    );
    let call = r.call_for_alice(
        "CalendarCreate",
        json!({
            "summary": "Lunch",
            "start": "2026-11-02T12:00:00-05:00",
            "end": "2026-11-02T13:00:00-05:00",
            "time_zone": "America/New_York",
            "attendees": ["bob@example.com"],
        }),
    );
    let id = r.requested_id().await;
    r.run(1).await;

    assert_eq!(
        call.await.unwrap().expect("the component answered"),
        json!({
            "id": "new0123456789",
            "summary": "Lunch",
            "start": { "date_time": "2026-11-02T12:00:00-05:00", "time_zone": "America/New_York" },
            "end": { "date_time": "2026-11-02T13:00:00-05:00", "time_zone": "America/New_York" },
            "status": "confirmed",
            "attendees": [{ "email": "bob@example.com", "response": "needsAction" }],
            "attendees_total": 1,
            "attendee_responses": {
                "accepted": 0,
                "declined": 0,
                "tentative": 0,
                "needs_action": 1,
            },
        })
    );
    assert_eq!(
        r.seen(),
        [(
            "POST".to_string(),
            PRIMARY_EVENTS.to_string(),
            event_write_query("none")
        )]
    );
    assert_eq!(r.bearers(), [format!("Bearer {BEARER}")]);
    assert_eq!(
        the_body(&r),
        json!({
            "summary": "Lunch",
            "start": { "dateTime": "2026-11-02T12:00:00-05:00", "timeZone": "America/New_York" },
            "end": { "dateTime": "2026-11-02T13:00:00-05:00", "timeZone": "America/New_York" },
            "attendees": [{ "email": "bob@example.com" }],
        })
    );
    r.assert_finished(&id);
}

/// `CalendarUpdate` sends one `PATCH` of the event on the named calendar
/// whose body holds only the fields named, a given time complete with the
/// other kind's key `null`, and answers the event as Google holds it,
/// without a description even where Google's answer carries one.
#[tokio::test]
async fn calendar_update_patches_only_the_named_fields() {
    let path = "/calendar/v3/calendars/team@example.com/events/e1";
    let r = rig().await;
    r.answer(
        path,
        serde_json::to_string_pretty(&json!({
            "id": "e1",
            "status": "confirmed",
            "summary": "Offsite",
            "description": "Bring a laptop",
            "start": { "date": "2026-11-05" },
            "end": { "date": "2026-11-07" },
        }))
        .unwrap(),
    );
    let call = r.call_for_alice(
        "CalendarUpdate",
        json!({
            "id": "e1",
            "calendar": "team@example.com",
            "start": "2026-11-05",
            "end": "2026-11-07",
            "notify": "all",
        }),
    );
    let id = r.requested_id().await;
    r.run(1).await;

    assert_eq!(
        call.await.unwrap().expect("the component answered"),
        json!({
            "id": "e1",
            "summary": "Offsite",
            "start": { "date": "2026-11-05" },
            "end": { "date": "2026-11-07" },
            "status": "confirmed",
            "attendees": [],
            "attendees_total": 0,
            "attendee_responses": {
                "accepted": 0,
                "declined": 0,
                "tentative": 0,
                "needs_action": 0,
            },
        })
    );
    assert_eq!(
        r.seen(),
        [(
            "PATCH".to_string(),
            path.to_string(),
            event_write_query("all")
        )]
    );
    assert_eq!(
        the_body(&r),
        json!({
            "start": { "date": "2026-11-05", "dateTime": null },
            "end": { "date": "2026-11-07", "dateTime": null },
        })
    );
    r.assert_finished(&id);
}

/// `CalendarDelete` reads the event, then deletes it with one `DELETE`
/// carrying no body and nobody notified by default; Google's 204 with no
/// body is the deletion done, and the answer names what was read.
#[tokio::test]
async fn calendar_delete_reads_then_deletes_and_a_204_is_success() {
    let path = format!("{PRIMARY_EVENTS}/e1");
    let r = rig().await;
    r.answer(
        &path,
        json!({
            "id": "e1",
            "status": "confirmed",
            "summary": "Dentist",
            "start": { "dateTime": "2026-11-03T09:00:00-05:00" },
        })
        .to_string(),
    );
    let call = r.call_for_alice("CalendarDelete", json!({ "id": "e1" }));
    r.respond_to("DELETE", &path, 204, Vec::new());
    let id = r.requested_id().await;
    r.run(2).await;

    assert_eq!(
        call.await.unwrap(),
        Ok(json!({
            "deleted": true,
            "id": "e1",
            "summary": "Dentist",
            "start": { "date_time": "2026-11-03T09:00:00-05:00" },
        }))
    );
    assert_eq!(
        r.seen(),
        [
            (
                "GET".to_string(),
                path.clone(),
                google_query(&[(
                    "fields",
                    "id,summary,start,recurrence,recurringEventId,status"
                )])
            ),
            (
                "DELETE".to_string(),
                path.clone(),
                google_query(&[("sendUpdates", "none")])
            ),
        ]
    );
    assert!(r.bodies().is_empty(), "neither call carried a body");
    assert_eq!(
        r.bearers(),
        [format!("Bearer {BEARER}"), format!("Bearer {BEARER}")]
    );
    r.assert_finished(&id);
}

/// A recurring series is refused at the read without `series: true`: one
/// `GET`, no `DELETE`, and the refusal names the event as a series.
#[tokio::test]
async fn calendar_delete_refuses_a_series_without_the_flag() {
    let path = format!("{PRIMARY_EVENTS}/weekly1");
    let r = rig().await;
    r.answer(
        &path,
        json!({
            "id": "weekly1",
            "status": "confirmed",
            "summary": "Team sync",
            "start": { "dateTime": "2026-11-02T10:00:00-05:00", "timeZone": "America/New_York" },
            "recurrence": ["RRULE:FREQ=WEEKLY;BYDAY=MO"],
        })
        .to_string(),
    );
    let call = r.call_for_alice("CalendarDelete", json!({ "id": "weekly1" }));
    let id = r.requested_id().await;
    r.run(1).await;

    assert_eq!(
        call.await.unwrap(),
        Err(ToolError::InvalidArgs(
            "`weekly1` is a recurring series: pass `series: true` to delete every occurrence, \
             or delete one occurrence by its own `id` from `CalendarEvents`"
                .to_string()
        ))
    );
    let seen = r.seen();
    assert_eq!(seen.len(), 1, "{seen:?}");
    assert_eq!(
        (seen[0].0.as_str(), seen[0].1.as_str()),
        ("GET", path.as_str())
    );
    r.owed(&r.executor_sub(), 0).await;
    r.assert_finished(&id);
}

/// How many ended requests' rows, or late answers, the measured activations
/// carry.
const LATE_ANSWERS: usize = 5;
/// The most characters `CalendarCreate` and `CalendarUpdate` admit in
/// `description`.
const DESCRIPTION_MAX_CHARS: usize = 8_000;
/// The description bounds measured.
const RUNGS: [usize; 2] = [4_000, 8_000];
/// The calendar every widest write names: four-byte characters.
fn widest_calendar() -> String {
    four_byte(0, 320)
}
/// The event every widest update and delete names.
fn widest_id() -> String {
    "e".repeat(1024)
}

/// `n` characters of four UTF-8 bytes, the first naming `i` so that strings
/// built for different `i` differ.
fn four_byte(i: usize, n: usize) -> String {
    let first = char::from_u32(0x1_0000 + u32::try_from(i).unwrap()).unwrap();
    std::iter::once(first)
        .chain(std::iter::repeat_n('\u{1d11e}', n - 1))
        .collect()
}

/// `chars` characters of `unit`, after one `a` when `unit` is whitespace,
/// which alone would be refused as blank.
fn description(unit: char, chars: usize) -> String {
    if unit.is_whitespace() {
        std::iter::once('a')
            .chain(std::iter::repeat_n(unit, chars - 1))
            .collect()
    } else {
        unit.to_string().repeat(chars)
    }
}

/// `CalendarUpdate`'s arguments with every field at the bound the component
/// admits, each at its widest serialized form, and `description` the
/// [`description`] of `chars` characters of `unit`, or none for `None`. `account` names alice's
/// connected account, the one value that reaches Google.
fn widest_update(content: Option<(char, usize)>) -> Value {
    let widest = |n: usize| "\u{1}".repeat(n);
    let mut args = json!({
        "id": widest_id(),
        "calendar": widest_calendar(),
        "summary": widest(1000),
        "start": widest(64),
        "end": widest(64),
        "location": widest(1000),
        "attendees": (0..50).map(|i| four_byte(i, 40)).collect::<Vec<_>>(),
        "time_zone": widest(64),
        "recurrence": (0..5)
            .map(|i| format!("{i}{}", "\"".repeat(499)))
            .collect::<Vec<_>>(),
        "notify": "externalOnly",
        "account": ACCOUNT,
    });
    if let Some((unit, chars)) = content {
        args["description"] = json!(description(unit, chars));
    }
    args
}

/// `CalendarCreate`'s counterpart of [`widest_update`]: the same event, on
/// the same calendar, without an id.
fn widest_create(unit: char, chars: usize) -> Value {
    let mut args = widest_update(Some((unit, chars)));
    args.as_object_mut().unwrap().remove("id");
    args
}

/// `CalendarDelete`'s arguments naming the widest update's event, a
/// recurring series.
fn widest_delete() -> Value {
    json!({
        "id": widest_id(),
        "calendar": widest_calendar(),
        "series": true,
        "account": ACCOUNT,
    })
}

/// Google's answer to every widest write, the delete's read included: the
/// event with every field written, in the shape the write's mask gives it,
/// so without its description.
fn echo() -> Value {
    let args = widest_update(None);
    let when = |key: &str| json!({ "dateTime": args[key], "timeZone": args["time_zone"] });
    json!({
        "id": args["id"],
        "status": "confirmed",
        "summary": args["summary"],
        "location": args["location"],
        "start": when("start"),
        "end": when("end"),
        "attendees": args["attendees"]
            .as_array()
            .unwrap()
            .iter()
            .map(|email| json!({ "email": email, "responseStatus": "needsAction" }))
            .collect::<Vec<_>>(),
        "recurrence": args["recurrence"],
    })
}

/// A rig at the bound: alice's one account connected, the bus cap at
/// [`GUIDE_BUS_CAP`], and the stub answering every request with [`echo`].
async fn bound_rig() -> StubRig {
    let r = workspace::rig_built(
        &CALENDAR_WRITE,
        CLAUSES,
        RigOptions {
            max_body_bytes: Some(GUIDE_BUS_CAP),
            ..RigOptions::default()
        },
    )
    .await;
    r.stub.lock().unwrap().api_body = echo().to_string().into_bytes();
    r
}

/// The activations measured. Each tool's `max_concurrency` is 1, so the
/// request inbox is three deep and the window is one request of each tool.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    /// The window new: a widest create, a widest update and a delete, handed
    /// to one activation; two events built and three calls issued.
    WindowNew,
    /// The window settling: those three requests' answers, all three
    /// pending, handed to one activation; two settle and the delete's read
    /// issues its `DELETE`.
    WindowSettles,
    /// [`LATE_ANSWERS`] late answers to the widest update, each request
    /// ended.
    Late,
    /// The window new over the rows of [`LATE_ANSWERS`] ended widest updates
    /// whose answers have not come.
    OverEndedRows,
    /// The window new, handed beside [`LATE_ANSWERS`] late answers to ended
    /// widest updates.
    BesideLateAnswers,
    /// One small create, new over the rows of [`LATE_ANSWERS`] ended widest
    /// updates whose answers have not come: its bytes bring nothing above the
    /// grant's floor, so the rows meet the least room for them.
    SmallOverEndedRows,
}

const SHAPES: [Shape; 6] = [
    Shape::WindowNew,
    Shape::WindowSettles,
    Shape::Late,
    Shape::OverEndedRows,
    Shape::BesideLateAnswers,
    Shape::SmallOverEndedRows,
];

fn small_create() -> Value {
    json!({
        "summary": "Lunch",
        "start": "2026-11-02T12:00:00-05:00",
        "end": "2026-11-02T13:00:00-05:00",
        "account": ACCOUNT,
    })
}

/// A rig brought to a measured activation, the calls it serves, and the
/// executor steps the stub's gate holds.
struct Staged {
    r: StubRig,
    calls: Vec<JoinHandle<Result<ToolOutput, ToolError>>>,
    held: Vec<JoinHandle<()>>,
}

/// Call the window's three requests on `r`, their descriptions `chars` of
/// `unit`, and wait until the provider is owed them beside `already` other
/// envelopes.
async fn call_window(
    r: &StubRig,
    unit: char,
    chars: usize,
    already: usize,
) -> Vec<JoinHandle<Result<ToolOutput, ToolError>>> {
    let calls = vec![
        r.call_for_alice_with(
            "CalendarCreate",
            widest_create(unit, chars),
            HeldAttachments::none(),
        ),
        r.call_for_alice_with(
            "CalendarUpdate",
            widest_update(Some((unit, chars))),
            HeldAttachments::none(),
        ),
        r.call_for_alice_with("CalendarDelete", widest_delete(), HeldAttachments::none()),
    ];
    r.owed(&r.provider_sub, already + 3).await;
    calls
}

/// Stage [`LATE_ANSWERS`] widest updates on a [`bound_rig`], their
/// descriptions `chars` of `unit`, each ended with its patch held at the
/// stub's gate. The stub holds every request until it takes a permit. Each
/// update is called and its request served, which issues its patch; the
/// patch is then made a late answer, which also frees the tool's one permit
/// for the next update. The rig, the gate, and the held executor steps.
async fn ended_updates(
    unit: char,
    chars: usize,
) -> (StubRig, Arc<tokio::sync::Semaphore>, Vec<JoinHandle<()>>) {
    let r = bound_rig().await;
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    r.stub.lock().unwrap().api_gate = Some(gate.clone());
    let args = widest_update(Some((unit, chars)));
    let mut steps = Vec::new();
    for n in 1..=LATE_ANSWERS {
        let call = r.call_for_alice_with("CalendarUpdate", args.clone(), HeldAttachments::none());
        r.requested_id().await;
        r.serve().await;
        steps.push(r.end_held(call, n).await);
    }
    assert_eq!(r.inflight_keys().len(), LATE_ANSWERS);
    (r, gate, steps)
}

/// Let the ended updates' patches be answered and assert each answer is
/// owed to the provider as Google's echo, with every row still in flight.
async fn answer_ended(r: &StubRig, gate: &tokio::sync::Semaphore, steps: Vec<JoinHandle<()>>) {
    for result in r.answer_late(gate, steps).await {
        let ok = &result["outcome"]["ok"];
        assert_eq!(ok["status"], 200, "a late answer is the stub's echo");
    }
}

/// Bring a rig to the activation `shape` names, descriptions `chars` of
/// `unit`.
async fn stage(shape: Shape, unit: char, chars: usize) -> Staged {
    match shape {
        Shape::WindowNew => {
            let r = bound_rig().await;
            let calls = call_window(&r, unit, chars, 0).await;
            Staged {
                r,
                calls,
                held: Vec::new(),
            }
        }
        Shape::WindowSettles => {
            let r = bound_rig().await;
            let calls = call_window(&r, unit, chars, 0).await;
            r.serve().await;
            assert_eq!(r.inflight_keys().len(), 3, "the window was issued");
            r.run_nested_calls(3).await;
            Staged {
                r,
                calls,
                held: Vec::new(),
            }
        }
        Shape::Late => {
            let (r, gate, steps) = ended_updates(unit, chars).await;
            answer_ended(&r, &gate, steps).await;
            Staged {
                r,
                calls: Vec::new(),
                held: Vec::new(),
            }
        }
        Shape::OverEndedRows => {
            let (r, _, held) = ended_updates(unit, chars).await;
            let calls = call_window(&r, unit, chars, 0).await;
            Staged { r, calls, held }
        }
        Shape::BesideLateAnswers => {
            let (r, gate, steps) = ended_updates(unit, chars).await;
            answer_ended(&r, &gate, steps).await;
            let calls = call_window(&r, unit, chars, LATE_ANSWERS).await;
            Staged {
                r,
                calls,
                held: Vec::new(),
            }
        }
        Shape::SmallOverEndedRows => {
            let (r, _, held) = ended_updates(unit, chars).await;
            let calls = vec![r.call_for_alice_with(
                "CalendarCreate",
                small_create(),
                HeldAttachments::none(),
            )];
            r.owed(&r.provider_sub, 1).await;
            Staged { r, calls, held }
        }
    }
}

/// How many calls the executor is owed.
async fn issued(r: &StubRig) -> usize {
    owed_everywhere(&r.messenger, &r.executor_sub()).await.len()
}

/// Whether the activation `shape` names completed on `r`: what it issues is
/// owed to the executor and the rows it leaves are in the store. A failed
/// activation's store writes roll back, leaving the rows it was handed, and
/// the requests new in it are failed without a call.
async fn completed(shape: Shape, r: &StubRig) -> bool {
    let rows = r.inflight_keys().len();
    match shape {
        Shape::WindowNew => issued(r).await == 3 && rows == 3,
        Shape::WindowSettles => issued(r).await == 1 && rows == 1,
        Shape::Late => r.settled_late(LATE_ANSWERS),
        Shape::OverEndedRows => issued(r).await == 3 && rows == LATE_ANSWERS + 3,
        Shape::BesideLateAnswers => issued(r).await == 3 && rows == 3,
        Shape::SmallOverEndedRows => issued(r).await == 1 && rows == LATE_ANSWERS + 1,
    }
}

/// Run the activation `shape` names, descriptions `chars` of `unit`, under
/// `limits` given its production grant, or the production limits when
/// `None`: whether it completed, the production grant and the envelopes it
/// was handed.
async fn run_shape(
    shape: Shape,
    unit: char,
    chars: usize,
    limits: impl Fn(u64) -> Option<ActivationLimits>,
) -> (bool, u64, usize) {
    let Staged { r, calls, held } = stage(shape, unit, chars).await;
    let envelopes = r
        .owed_windows()
        .await
        .iter()
        .map(|port| port.entries.len())
        .sum();
    let grant = r.production_grant().await;
    r.serve_under(limits(grant)).await;
    let done = completed(shape, &r).await;
    for call in calls {
        call.abort();
    }
    for step in held {
        step.abort();
    }
    (done, grant, envelopes)
}

/// The `description` contents measured: a control character, `"`, `\` and
/// line feeds, each escaped in the request's JSON and again in the row, a
/// four-byte character, and an ASCII letter.
const DESCRIPTIONS: [char; 6] = ['\u{1}', '"', '\\', '\n', '\u{1d11e}', 'a'];

/// The measured cells run this many at a time.
const MEASURED_AT_ONCE: usize = 6;

/// The description content each gated activation is held at: the costliest
/// share of its grant at the bound, by `measure_window_and_late_answers`.
const COSTLIEST_W: char = '\n';
const COSTLIEST_S: char = '\n';
const COSTLIEST_L: char = '\u{1}';
const COSTLIEST_R: char = '\n';
const COSTLIEST_M: char = '\u{1}';

/// One activation's figures: its production grant, the envelopes it was
/// handed, and the least grant it completes on, `None` when the production
/// grant does not carry it.
#[derive(Clone, Copy, Debug)]
struct Figures {
    grant: u64,
    envelopes: usize,
    least: Option<u64>,
}

impl Figures {
    /// The least grant's share of the production grant.
    fn share(&self) -> Option<f64> {
        self.least.map(|least| least as f64 / self.grant as f64)
    }

    /// The fuel left under the margin: [`MARGIN`]'s share of the production
    /// grant less the least grant.
    fn headroom(&self) -> Option<i64> {
        self.least
            .map(|least| margin_of(self.grant) as i64 - least as i64)
    }
}

/// `shape` measured at a description of `chars` characters of `unit`.
async fn measure_shape(shape: Shape, unit: char, chars: usize) -> Figures {
    let (done, grant, envelopes) = run_shape(shape, unit, chars, |_| None).await;
    let least = if done {
        Some(
            least_under(grant, |g| async move {
                run_shape(shape, unit, chars, |_| fuel(g)).await.0
            })
            .await,
        )
    } else {
        None
    };
    let figures = Figures {
        grant,
        envelopes,
        least,
    };
    println!("{shape:?}, {chars} of {unit:?}: {figures:?}");
    figures
}

/// The least grant and share of the production grant each measured
/// activation completes on, for each description content at each rung; the
/// fuel one ended request's row adds, and how many more such rows the margin
/// absorbs over the window, beside the late answers and over one small
/// create; and the byte length of the widest update's
/// request with no description, as the registry publishes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "a measurement: run by hand"]
async fn measure_window_and_late_answers() {
    {
        let r = bound_rig().await;
        let _call = r.call_for_alice_with(
            "CalendarUpdate",
            widest_update(None),
            HeldAttachments::none(),
        );
        r.requested_id().await;
        let ports = r.owed_windows().await;
        let request = ports
            .iter()
            .flat_map(|port| &port.entries)
            .map(|(_, envelope)| envelope.body.len())
            .sum::<usize>();
        println!("the widest update's request with no description: {request} bytes");
    }
    let mut cells = Vec::new();
    for chars in RUNGS {
        for unit in DESCRIPTIONS {
            for shape in SHAPES {
                cells.push((shape, unit, chars));
            }
        }
    }
    let results = measure_all(cells.clone(), MEASURED_AT_ONCE, |(shape, unit, chars)| {
        measure_shape(shape, unit, chars)
    })
    .await;
    let find = |shape: Shape, unit: char, chars: usize| {
        cells
            .iter()
            .zip(&results)
            .find(|((s, u, c), _)| (*s, *u, *c) == (shape, unit, chars))
            .map(|(_, figures)| *figures)
            .unwrap()
    };
    for ((shape, unit, chars), figures) in cells.iter().zip(&results) {
        match figures.least {
            Some(least) => println!(
                "{shape:?}, {chars} of {unit:?}: {} envelopes, completes on {least} of a \
                 production grant of {}, {:.4} of it",
                figures.envelopes,
                figures.grant,
                figures.share().unwrap()
            ),
            None => println!(
                "{shape:?}, {chars} of {unit:?}: {} envelopes, does not complete on its \
                 production grant of {}",
                figures.envelopes, figures.grant
            ),
        }
    }
    for chars in RUNGS {
        for unit in DESCRIPTIONS {
            let window = find(Shape::WindowNew, unit, chars);
            let over = find(Shape::OverEndedRows, unit, chars);
            let beside = find(Shape::BesideLateAnswers, unit, chars);
            let small = find(Shape::SmallOverEndedRows, unit, chars);
            let (Some(window_least), Some(over_least)) = (window.least, over.least) else {
                continue;
            };
            let row = over_least.saturating_sub(window_least) / LATE_ANSWERS as u64;
            let spare = |figures: Figures| match (figures.headroom(), row) {
                (Some(headroom), row) if row > 0 => format!("{}", headroom.max(0) as u64 / row),
                _ => "-".to_string(),
            };
            println!(
                "{chars} of {unit:?}: an ended row adds {row}; rows to spare over ended rows {}, \
                 beside late answers {}, small create over ended rows {}",
                spare(over),
                spare(beside),
                spare(small)
            );
        }
    }
}

/// Run the activation `shape` names at the bound, descriptions of `unit`,
/// under the margin, and assert it completed; the staged rig.
async fn inside_the_margin(shape: Shape, unit: char) -> Staged {
    let staged = stage(shape, unit, DESCRIPTION_MAX_CHARS).await;
    staged.r.serve_under(Some(MARGIN)).await;
    assert!(
        completed(shape, &staged.r).await,
        "{shape:?} completed inside the margin"
    );
    staged
}

/// Carry the window's requests on from the measured activation: run each
/// nested call issued, and the activation its answer brings, until none is
/// issued; then assert every request was answered with Google's event, or
/// the delete's answer.
async fn answer_window(r: &StubRig, calls: Vec<JoinHandle<Result<ToolOutput, ToolError>>>) {
    loop {
        let owed = issued(r).await;
        if owed == 0 {
            break;
        }
        r.owed(&r.executor_sub(), owed).await;
        r.executor.drain_step().await;
        r.serve().await;
    }
    for call in calls {
        let answer = call.await.unwrap().expect("the request was answered");
        assert_eq!(answer.value["id"], json!(widest_id()));
    }
    assert!(r.inflight_keys().is_empty(), "no row is left");
}

/// The window new — a widest create, a widest update and a delete handed to
/// one activation, two events built at the bound — completes inside the
/// margin. Measured by `measure_window_and_late_answers`, with line feeds,
/// the costliest content, it completes on about 13.4M of a 73.3M production
/// grant, 0.183 of it. The request inbox is three deep: a fourth request
/// waits for a tool's permit. Four builds of the largest event the stub
/// received fit the activation's blob write bound. A failure here is
/// answered by measuring again and correcting this figure or the
/// component's bounds, never by raising the share.
#[tokio::test(flavor = "multi_thread")]
async fn the_window_new_builds_inside_the_margin() {
    let Staged { r, calls, .. } = stage(Shape::WindowNew, COSTLIEST_W, DESCRIPTION_MAX_CHARS).await;
    let fourth = r.call_for_alice_with(
        "CalendarCreate",
        widest_create(COSTLIEST_W, DESCRIPTION_MAX_CHARS),
        HeldAttachments::none(),
    );
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(
        owed_everywhere(&r.messenger, &r.provider_sub).await.len(),
        3,
        "the request inbox is three deep"
    );
    r.serve_under(Some(MARGIN)).await;
    assert!(
        completed(Shape::WindowNew, &r).await,
        "the window was built inside the margin"
    );
    answer_window(&r, calls).await;
    let largest = r.bodies().iter().map(Vec::len).max().unwrap();
    assert!(
        (largest * BUILD_BUDGET) as u64
            <= brenn_wasm::PROCESSOR_MAX_BLOB_BYTES_WRITTEN_PER_ACTIVATION,
        "{largest} bytes"
    );
    fourth.abort();
}

/// The window settling — its three requests' answers, all pending, in one
/// activation — completes inside the margin: the create and the update
/// settle, and the delete's read issues its `DELETE`. Measured by
/// `measure_window_and_late_answers`, with line feeds, the costliest content,
/// it completes on about 23.8M of a 112.5M production grant, 0.211 of it. A
/// failure here is
/// answered by measuring again and correcting this figure or the
/// component's bounds, never by raising the share.
#[tokio::test(flavor = "multi_thread")]
async fn the_window_settles_inside_the_margin() {
    let Staged { r, calls, .. } = inside_the_margin(Shape::WindowSettles, COSTLIEST_S).await;
    answer_window(&r, calls).await;
}

/// Five late answers to the widest `CalendarUpdate` at the bound, each
/// answered with Google's event, are settled in one activation under the
/// margin: the kit drops each answer unread and deletes its row, leaving no
/// row. What the activation still pays for is the sweep's pass over the five
/// rows and the answers' own parse. Measured by
/// `measure_window_and_late_answers`, with U+0001, the costliest content, it
/// completes on about 17.6M of a 128.6M production grant, 0.137 of it. A
/// failure here is answered by
/// measuring again and correcting this figure or the component's bounds,
/// never by raising the share.
#[tokio::test(flavor = "multi_thread")]
async fn five_late_answers_to_the_widest_update_settle_inside_the_margin() {
    inside_the_margin(Shape::Late, COSTLIEST_L).await;
}

/// The window new over the rows of five ended widest updates, their answers
/// not come, completes inside the margin: five in-flight rows before it,
/// eight after it, three calls issued. Five is the count measured, not a
/// ceiling: nothing bounds how many ended requests' rows a store holds
/// until their answers come or 420 s pass. Measured by
/// `measure_window_and_late_answers`, with line feeds, the costliest content,
/// it completes on about 19.5M of a 73.3M production grant, 0.266 of it, the
/// costliest activation measured; one ended row adds about 1.22M, and the
/// margin absorbs 24 more here, where the window's bytes raise the grant;
/// over one small create on the grant's floor it absorbs 22 more of line
/// feeds and 15 of U+0001, the least room measured. The three new
/// requests are answered once their calls are. A failure here is answered by
/// measuring again and correcting this figure or the component's bounds,
/// never by raising the share.
#[tokio::test(flavor = "multi_thread")]
async fn the_window_new_over_five_ended_rows_builds_inside_the_margin() {
    let Staged { r, calls, held } = inside_the_margin(Shape::OverEndedRows, COSTLIEST_R).await;
    let gate = r.stub.lock().unwrap().api_gate.take().unwrap();
    gate.add_permits(LATE_ANSWERS);
    for step in held {
        step.await.expect("the executor's drain step");
    }
    answer_window(&r, calls).await;
}

/// The window new handed beside five late answers to ended widest updates
/// completes inside the margin: five results owed and three requests new
/// before it, three in-flight rows after it. Five is the count measured,
/// not a ceiling. Measured by `measure_window_and_late_answers`, with U+0001,
/// the costliest content, it completes on about 32.9M of a 168.3M production
/// grant, 0.196 of it; one ended row adds about 1.66M, and the margin absorbs
/// 49 more here, where the window's bytes and the late answers' raise the
/// grant. The three new requests are answered once their
/// calls are. A failure here is answered by measuring again and correcting
/// this figure or the component's bounds, never by raising the share.
#[tokio::test(flavor = "multi_thread")]
async fn the_window_new_beside_five_late_answers_builds_inside_the_margin() {
    let Staged { r, calls, .. } = inside_the_margin(Shape::BesideLateAnswers, COSTLIEST_M).await;
    r.stub.lock().unwrap().api_gate = None;
    answer_window(&r, calls).await;
}

/// The event the widest write `args` describes, as the component sends it:
/// each field given, `start` and `end` with the zone, an update's times
/// complete.
fn sent_event(args: &Value, update: bool) -> Value {
    let when = |key: &str| {
        let mut time = json!({ "dateTime": args[key], "timeZone": args["time_zone"] });
        if update {
            time["date"] = Value::Null;
        }
        time
    };
    json!({
        "summary": args["summary"],
        "description": args["description"],
        "location": args["location"],
        "start": when("start"),
        "end": when("end"),
        "attendees": args["attendees"]
            .as_array()
            .unwrap()
            .iter()
            .map(|email| json!({ "email": email }))
            .collect::<Vec<_>>(),
        "recurrence": args["recurrence"],
    })
}

/// The widest write `tool` of `args` is sent whole: one request, its body
/// the event the arguments describe, the description whole, and the caller
/// answered with Google's event.
async fn assert_sent_whole(tool: &str, args: Value, update: bool) {
    let r = bound_rig().await;
    let answer = r
        .call_running(tool, args.clone(), HeldAttachments::none(), 1)
        .await;
    let answer = answer.expect("the component answered");
    assert_eq!(answer.value["id"], json!(widest_id()));
    assert_eq!(answer.value.get("description"), None);
    assert_eq!(r.seen().len(), 1, "one request");
    assert_eq!(the_body(&r), sent_event(&args, update));
}

/// A create at the description bound, every other field at its widest,
/// sends the event whole.
#[tokio::test(flavor = "multi_thread")]
async fn a_create_at_the_description_bound_sends_the_event_whole() {
    assert_sent_whole(
        "CalendarCreate",
        widest_create('\u{1}', DESCRIPTION_MAX_CHARS),
        false,
    )
    .await;
}

/// An update at the description bound, every other field at its widest,
/// sends the event whole.
#[tokio::test(flavor = "multi_thread")]
async fn an_update_at_the_description_bound_sends_the_event_whole() {
    assert_sent_whole(
        "CalendarUpdate",
        widest_update(Some(('\u{1}', DESCRIPTION_MAX_CHARS))),
        true,
    )
    .await;
}

/// A description one character over the bound is refused before any call.
#[tokio::test(flavor = "multi_thread")]
async fn a_description_over_the_bound_is_refused_before_any_call() {
    let r = rig().await;
    let answer = r
        .call_running(
            "CalendarCreate",
            json!({
                "summary": "Lunch",
                "start": "2026-11-02T12:00:00-05:00",
                "end": "2026-11-02T13:00:00-05:00",
                "description": "a".repeat(DESCRIPTION_MAX_CHARS + 1),
            }),
            HeldAttachments::none(),
            0,
        )
        .await;
    assert_eq!(
        answer.map(|output| output.value),
        Err(ToolError::InvalidArgs(format!(
            "`description` is longer than {DESCRIPTION_MAX_CHARS} characters"
        )))
    );
    assert!(r.seen().is_empty(), "nothing reached Google");
}
