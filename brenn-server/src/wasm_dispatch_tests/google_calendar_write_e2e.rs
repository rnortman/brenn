//! The `google-calendar-write` component, end to end, on the rig in
//! [`super::google_workspace_rig`].

use brenn_tool_registry::ToolError;
use serde_json::json;

use super::google_workspace_rig::{
    self as workspace, BEARER, CALENDAR_WRITE, Clauses, FirstRead, google_query,
};
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
