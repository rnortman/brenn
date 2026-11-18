//! The `google-calendar-write` component, end to end, on the rig in
//! [`super::google_workspace_rig`].

use std::sync::Arc;
use std::time::Duration;

use brenn_tool_registry::{HeldAttachments, ToolError};
use brenn_wasm_dispatch::ActivationLimits;
use serde_json::{Value, json};

use super::google_workspace_rig::{
    self as workspace, ACCOUNT, BEARER, CALENDAR_WRITE, Clauses, FirstRead, google_query,
};
use super::kit_body_fuel_measure::{least, narrow};
use super::kit_file_e2e::drop_caller;
use super::provider_rig::{MARGIN, fuel};
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

/// The query an event write sends: whom Google notifies, and the events.get
/// mask so the answer is the event as `CalendarEvent` reads it.
fn event_write_query(notify: &str) -> Pairs {
    google_query(&[
        ("sendUpdates", notify),
        ("maxAttendees", "50"),
        (
            "fields",
            "id,status,summary,description,location,start,end,\
             organizer(email,displayName,self),\
             attendees(email,displayName,responseStatus,optional,self),\
             attendeesOmitted,recurrence,recurringEventId,hangoutLink,\
             conferenceData/entryPoints(entryPointType,uri)",
        ),
    ])
}

/// The one body the stub received, as JSON.
fn the_body(r: &StubRig) -> serde_json::Value {
    let bodies = r.bodies();
    assert_eq!(bodies.len(), 1, "exactly one request carried a body");
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
/// other kind's key `null`, and answers the event as Google holds it.
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
            "description": "Bring a laptop",
            "description_chars": 14,
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

/// How many late answers the measured activation settles.
const LATE_ANSWERS: usize = 5;
/// The most characters `CalendarUpdate` admits in `description`.
const DESCRIPTION_CHARS: usize = 2_000;

/// `n` characters of four UTF-8 bytes, the first naming `i` so that strings
/// built for different `i` differ.
fn four_byte(i: usize, n: usize) -> String {
    let first = char::from_u32(0x1_0000 + u32::try_from(i).unwrap()).unwrap();
    std::iter::once(first)
        .chain(std::iter::repeat_n('\u{1d11e}', n - 1))
        .collect()
}

/// `CalendarUpdate`'s arguments with every field at the bound the component
/// admits, each at its widest serialized form, and `description` the
/// [`DESCRIPTION_CHARS`] characters of `unit`. `account` names alice's
/// connected account, the one value that reaches Google.
fn widest_update(unit: char) -> Value {
    let widest = |n: usize| "\u{1}".repeat(n);
    json!({
        "id": "e".repeat(1024),
        "calendar": four_byte(0, 320),
        "summary": widest(1000),
        "start": widest(64),
        "end": widest(64),
        "description": unit.to_string().repeat(DESCRIPTION_CHARS),
        "location": widest(1000),
        "attendees": (0..50).map(|i| four_byte(i, 40)).collect::<Vec<_>>(),
        "time_zone": widest(64),
        "recurrence": (0..5)
            .map(|i| format!("{i}{}", "\"".repeat(499)))
            .collect::<Vec<_>>(),
        "notify": "externalOnly",
        "account": ACCOUNT,
    })
}

/// Google's answer to the patch `args` asks for: the event with every field
/// written, in the shape the events.get mask gives it.
fn echo(args: &Value) -> Value {
    let when = |key: &str| json!({ "dateTime": args[key], "timeZone": args["time_zone"] });
    json!({
        "id": args["id"],
        "status": "confirmed",
        "summary": args["summary"],
        "description": args["description"],
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

/// Wait until the stub has received `n` API requests.
async fn stub_saw(r: &StubRig, n: usize) {
    for _ in 0..3000 {
        if r.stub.lock().unwrap().api_seen.len() >= n {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the stub never received {n} requests");
}

/// Bring the provider to the activation that settles [`LATE_ANSWERS`] late
/// answers to the widest `CalendarUpdate` whose `description` is of `unit`,
/// and assert that activation is the one meant: five in-flight rows, and
/// five results owed, each Google's echo with the description whole.
///
/// The stub holds every request until it takes a permit. Each write is
/// called, its request served, which issues its patch, and its nested call
/// started on a task of its own; once the stub has the patch, the call is
/// past the executor's check that its request is pending, and the caller is
/// dropped, which ends the request and frees the tool's one permit for the
/// next write. Only then are the five patches answered, so no answer reaches
/// the provider before the last is issued.
async fn late_answers(unit: char) -> StubRig {
    let r = rig().await;
    let args = widest_update(unit);
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    {
        let mut stub = r.stub.lock().unwrap();
        stub.api_gate = Some(gate.clone());
        stub.api_body = echo(&args).to_string().into_bytes();
    }
    let mut steps = Vec::new();
    for n in 1..=LATE_ANSWERS {
        let call = r.call_for_alice_with("CalendarUpdate", args.clone(), HeldAttachments::none());
        r.requested_id().await;
        r.serve().await;
        steps.push(r.start_nested().await);
        stub_saw(&r, n).await;
        drop_caller(call).await;
    }
    gate.add_permits(LATE_ANSWERS);
    for step in steps {
        step.await.expect("the executor's drain step");
    }

    let results = r.owed(&r.provider_sub, LATE_ANSWERS).await;
    assert_eq!(
        r.inflight_keys().len(),
        LATE_ANSWERS,
        "five writes in flight"
    );
    for result in &results {
        let ok = &result["outcome"]["ok"];
        assert_eq!(ok["status"], 200, "a late answer is the stub's echo");
        let event: Value =
            serde_json::from_str(ok["body"].as_str().expect("a text body")).expect("JSON");
        assert_eq!(
            event["description"], args["description"],
            "the echo's description is whole"
        );
    }
    r
}

/// Whether the late answers' settle, of a description of `unit`, completes
/// under `limits`, or the production limits when `None`.
async fn settles(unit: char, limits: Option<ActivationLimits>) -> (bool, u64) {
    let r = late_answers(unit).await;
    r.serve_under(limits).await;
    let left = r.inflight_keys().len();
    assert!(left == 0 || left == LATE_ANSWERS, "{left} rows left");
    (left == 0, 0)
}

/// The parts of the production grant a share is counted in: one part of a
/// grant near 140M is about 34,000 fuel.
const SHARE_PARTS: u64 = 4096;

/// The `description` contents measured: a control character, `"` and `\`,
/// each escaped in the echo's JSON and again in the answer carrying it, and
/// four-byte characters.
const DESCRIPTIONS: [char; 4] = ['\u{1}', '"', '\\', '\u{1d11e}'];

/// The least grant the late answers' settle completes on, for each
/// description content, and the least share of the production grant it
/// completes on, from which the production grant for that activation follows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement: run by hand"]
async fn measure_late_answers() {
    for unit in DESCRIPTIONS {
        assert!(settles(unit, None).await.0, "the production grant settles");
        let (least, _) = least(|grant| settles(unit, fuel(grant))).await;
        let parts = narrow(0, SHARE_PARTS, 1, &|numerator| {
            settles(
                unit,
                Some(ActivationLimits::FuelShare {
                    numerator,
                    denominator: SHARE_PARTS,
                }),
            )
        })
        .await;
        println!(
            "late answers, description of {unit:?}: completes on {least}, \
             {parts}/{SHARE_PARTS} of the production grant ({:.3} of it), \
             which is about {}",
            parts as f64 / SHARE_PARTS as f64,
            least * SHARE_PARTS / parts
        );
    }
}

/// Five late answers to the widest `CalendarUpdate`, its description of
/// U+0001, the costliest of [`DESCRIPTIONS`], each answered with Google's
/// echo, are settled in one activation under the margin: the kit decodes
/// each in-flight row, reads the echo, and completes a request the host no
/// longer holds, leaving no row. Measured by `measure_late_answers`, it
/// completes on about 55.1M of a 146.5M production grant. A failure here is
/// answered by measuring again and correcting this figure or the
/// component's bounds, never by raising the share.
#[tokio::test(flavor = "multi_thread")]
async fn five_late_answers_to_the_widest_update_settle_inside_the_margin() {
    let r = late_answers(DESCRIPTIONS[0]).await;
    r.serve_under(Some(MARGIN)).await;
    assert!(
        r.inflight_keys().is_empty(),
        "the five late answers were settled"
    );
}
