//! The `google-calendar-read` component, end to end, on the rig in
//! [`super::google_workspace_rig`].

use brenn_tool_registry::ToolError;
use serde_json::json;

use super::google_workspace_rig::{
    self as workspace, ACCOUNT, BEARER, BOTH_WAYS, CALENDAR_READ, Clauses, FirstRead, OVERSIZED,
    Unreadable, assert_unreadable, google_query, insufficient_scope_body, scope_denied,
};
use super::stub_rig::{Pairs, StubRig, assert_could_not_be_issued};

const CALENDAR_LIST: &str = "/calendar/v3/users/me/calendarList";
const FREE_BUSY: &str = "/calendar/v3/freeBusy";

/// The instance's `api-request` clauses: one per endpoint the tools call.
const CLAUSES: Clauses<'static> = &[
    ("GET", CALENDAR_LIST),
    ("GET", "/calendar/v3/calendars/*/events"),
    ("GET", "/calendar/v3/calendars/*/events/*"),
    ("POST", FREE_BUSY),
];

/// The rig with every clause and alice's one account connected.
async fn rig() -> StubRig {
    workspace::rig(&CALENDAR_READ, CLAUSES).await
}

/// The rig with `clauses` and alice's one account connected.
async fn rig_with(clauses: Clauses<'_>) -> StubRig {
    workspace::rig_with(&CALENDAR_READ, clauses, &[(ACCOUNT, BEARER)], None).await
}

/// The rig for `way`, with every clause and alice's one account connected.
async fn unreadable_rig(way: Unreadable) -> StubRig {
    workspace::unreadable_rig(&CALENDAR_READ, CLAUSES, way).await
}

/// The credential cases' first calls: `CalendarList`'s bare `GET` and
/// `CalendarFreeBusy`'s `POST`, so each method resolves the caller's account.
fn first_reads() -> [FirstRead; 2] {
    [
        FirstRead {
            tool: "CalendarList",
            slug: "calendar-list",
            args: json!({}),
            method: "GET",
            path: CALENDAR_LIST,
            body: json!({ "items": [] }).to_string(),
        },
        FirstRead {
            tool: "CalendarFreeBusy",
            slug: "calendar-free-busy",
            args: json!({ "from": FROM, "to": TO }),
            method: "POST",
            path: FREE_BUSY,
            body: json!({ "calendars": { "primary": { "busy": [] } } }).to_string(),
        },
    ]
}

/// With no account connected, `api-request`'s `credential_invalid` value is
/// the tool's answer unchanged, and the component makes no further call.
#[tokio::test]
async fn no_connected_account_is_relayed_as_credential_invalid() {
    for read in first_reads() {
        workspace::assert_no_connected_account(&CALENDAR_READ, CLAUSES, read).await;
    }
}

/// With two accounts connected, a call naming none is `denied` with
/// `api-request`'s list of them, and a call naming one sends that account's
/// bearer.
#[tokio::test]
async fn several_accounts_need_one_named() {
    for read in first_reads() {
        workspace::assert_several_accounts_need_one_named(&CALENDAR_READ, CLAUSES, read).await;
    }
}

/// A component calling a tool on its own account has no user, and an
/// instance whose clauses say `user = "*"` lends none: the caller's result is
/// `denied` carrying `api-request`'s explanation, and nothing reaches the API.
#[tokio::test]
async fn a_userless_caller_is_denied_with_the_reason() {
    for read in first_reads() {
        workspace::assert_a_userless_caller_is_denied_with_the_reason(
            &CALENDAR_READ,
            CLAUSES,
            read,
        )
        .await;
    }
}

/// calendarList.list's query as `CalendarList` sends it, with `extra`.
fn calendar_list_query(extra: &[(&str, &str)]) -> Pairs {
    google_query(
        &[
            &[
                ("maxResults", "100"),
                (
                    "fields",
                    "items(id,summary,primary,accessRole,timeZone),nextPageToken",
                ),
            ],
            extra,
        ]
        .concat(),
    )
}

/// `CalendarList` reads one page of alice's calendar list on the Calendar
/// host with her bearer, and answers it compactly in Google's order with the
/// page token relayed; a second call passes the token back.
#[tokio::test]
async fn calendar_list_reads_the_users_calendars() {
    let r = rig().await;
    r.answer(
        CALENDAR_LIST,
        serde_json::to_string_pretty(&json!({
            "items": [
                {
                    "id": ACCOUNT,
                    "summary": ACCOUNT,
                    "timeZone": "America/New_York",
                    "accessRole": "owner",
                    "primary": true,
                },
                {
                    "id": "en.usa#holiday@group.v.calendar.google.com",
                    "summary": "Holidays in United States",
                    "timeZone": "America/New_York",
                    "accessRole": "reader",
                },
            ],
            "nextPageToken": "page-2",
        }))
        .unwrap(),
    );
    let call = r.call_for_alice("CalendarList", json!({}));
    let id = r.requested_id().await;
    r.run(1).await;

    assert_eq!(
        call.await.unwrap().expect("the component answered"),
        json!({
            "calendars": [
                {
                    "id": ACCOUNT,
                    "summary": ACCOUNT,
                    "primary": true,
                    "access_role": "owner",
                    "time_zone": "America/New_York",
                },
                {
                    "id": "en.usa#holiday@group.v.calendar.google.com",
                    "summary": "Holidays in United States",
                    "access_role": "reader",
                    "time_zone": "America/New_York",
                },
            ],
            "next_page": "page-2",
        })
    );
    assert_eq!(r.bearers(), [format!("Bearer {BEARER}")]);
    r.assert_finished(&id);

    let call = r.call_for_alice("CalendarList", json!({ "page": "page-2" }));
    let id = r.requested_id().await;
    r.run(1).await;
    call.await.unwrap().expect("the component answered");
    assert_eq!(
        r.seen(),
        [
            (
                "GET".to_string(),
                CALENDAR_LIST.to_string(),
                calendar_list_query(&[]),
            ),
            (
                "GET".to_string(),
                CALENDAR_LIST.to_string(),
                calendar_list_query(&[("pageToken", "page-2")]),
            ),
        ]
    );
    r.assert_finished(&id);
}

/// Google's 403 on a Calendar tool is `denied` with the sentence naming the
/// Calendar read scope, not Gmail's.
#[tokio::test]
async fn a_calendar_403_is_denied_with_the_calendar_scope_hint() {
    let r = rig().await;
    r.respond(CALENDAR_LIST, 403, insufficient_scope_body());
    let call = r.call_for_alice("CalendarList", json!({}));
    let id = r.requested_id().await;
    r.run(1).await;

    assert_eq!(call.await.unwrap(), scope_denied("Calendar read"));
    r.assert_finished(&id);
}

/// A calendar id with `#` and `@`, as Google's holiday calendars have.
const HOLIDAYS: &str = "en.usa#holiday@group.v.calendar.google.com";
/// [`HOLIDAYS`]' events path as it goes on the wire: `#` percent-encoded,
/// `@` as is.
const HOLIDAY_EVENTS: &str =
    "/calendar/v3/calendars/en.usa%23holiday@group.v.calendar.google.com/events";

/// The window every `CalendarEvents` call here asks for.
const FROM: &str = "2026-11-02T00:00:00-05:00";
const TO: &str = "2026-11-09T00:00:00-05:00";

/// events.list's query as `CalendarEvents` sends it for [`FROM`]..[`TO`],
/// with `extra`.
fn events_query(extra: &[(&str, &str)]) -> Pairs {
    google_query(
        &[
            &[
                ("timeMin", FROM),
                ("timeMax", TO),
                ("singleEvents", "true"),
                ("orderBy", "startTime"),
                ("maxResults", "50"),
                ("maxAttendees", "1"),
                (
                    "fields",
                    "items(id,summary,start,end,location,status,organizer/email,\
                     attendees(self,responseStatus)),nextPageToken,timeZone",
                ),
            ],
            extra,
        ]
        .concat(),
    )
}

/// `CalendarEvents` reads one window of a calendar named by an id holding `#`,
/// which goes on the wire percent-encoded, with alice's bearer, and answers
/// each event compactly with her own response.
#[tokio::test]
async fn calendar_events_reads_a_window_of_a_calendar_by_id() {
    let r = rig().await;
    r.answer(
        HOLIDAY_EVENTS,
        serde_json::to_string_pretty(&json!({
            "timeZone": "America/New_York",
            "nextPageToken": "page-2",
            "items": [
                {
                    "id": "allday0123456789",
                    "status": "confirmed",
                    "summary": "Veterans Day",
                    "organizer": { "email": HOLIDAYS },
                    "start": { "date": "2026-11-11" },
                    "end": { "date": "2026-11-12" },
                },
                {
                    "id": "weekly0123456789_20261104T140000Z",
                    "status": "confirmed",
                    "summary": "Weekly sync",
                    "organizer": { "email": "bob@example.com" },
                    "start": { "dateTime": "2026-11-04T14:00:00Z" },
                    "end": { "dateTime": "2026-11-04T14:30:00Z" },
                    "attendees": [{ "self": true, "responseStatus": "declined" }],
                },
            ],
        }))
        .unwrap(),
    );
    let call = r.call_for_alice(
        "CalendarEvents",
        json!({ "calendar": HOLIDAYS, "from": FROM, "to": TO, "q": "sync" }),
    );
    let id = r.requested_id().await;
    r.run(1).await;

    assert_eq!(
        call.await.unwrap().expect("the component answered"),
        json!({
            "events": [
                {
                    "id": "allday0123456789",
                    "summary": "Veterans Day",
                    "start": { "date": "2026-11-11" },
                    "end": { "date": "2026-11-12" },
                    "status": "confirmed",
                    "organizer": HOLIDAYS,
                },
                {
                    "id": "weekly0123456789_20261104T140000Z",
                    "summary": "Weekly sync",
                    "start": { "date_time": "2026-11-04T14:00:00Z" },
                    "end": { "date_time": "2026-11-04T14:30:00Z" },
                    "status": "confirmed",
                    "organizer": "bob@example.com",
                    "self_response": "declined",
                },
            ],
            "time_zone": "America/New_York",
            "next_page": "page-2",
        })
    );
    assert_eq!(
        r.seen(),
        [(
            "GET".to_string(),
            HOLIDAY_EVENTS.to_string(),
            events_query(&[("q", "sync")]),
        )]
    );
    assert_eq!(r.bearers(), [format!("Bearer {BEARER}")]);
    r.assert_finished(&id);
}

/// A window whose events cannot be read whole is `internal`, naming why and
/// what to narrow; nothing is read again.
#[tokio::test]
async fn a_window_too_large_to_read_whole_is_refused_with_what_to_narrow() {
    for way in BOTH_WAYS {
        let r = unreadable_rig(way).await;
        let wide = json!({ "items": [{
            "id": "e1",
            "summary": "s".repeat(OVERSIZED),
            "start": { "date": "2026-11-03" },
            "end": { "date": "2026-11-04" },
        }] })
        .to_string();
        r.answer("/calendar/v3/calendars/primary/events", wide.clone());
        let call = r.call_for_alice("CalendarEvents", json!({ "from": FROM, "to": TO }));
        let id = r.requested_id().await;
        r.run(1).await;

        let Err(ToolError::Internal(detail)) = call.await.unwrap() else {
            panic!("{way:?}: the window was not refused internal");
        };
        let why = detail
            .strip_suffix("; ask for fewer events with `max`, or a shorter `from`/`to` window")
            .unwrap_or_else(|| panic!("{detail}"));
        assert_unreadable(way, why, wide.len());
        assert_eq!(r.seen().len(), 1, "{way:?}");
        r.owed(&r.executor_sub(), 0).await;
        r.assert_finished(&id);
    }
}

/// Google's 400 for a time it cannot read is the caller's: `invalid_args`
/// with Google's message.
#[tokio::test]
async fn a_google_400_on_a_window_is_invalid_args() {
    let r = rig().await;
    r.respond(
        "/calendar/v3/calendars/primary/events",
        400,
        json!({ "error": { "code": 400, "message": "Bad Request", "status": "INVALID_ARGUMENT" } })
            .to_string()
            .into_bytes(),
    );
    let call = r.call_for_alice("CalendarEvents", json!({ "from": "monday", "to": TO }));
    let id = r.requested_id().await;
    r.run(1).await;

    assert_eq!(
        call.await.unwrap(),
        Err(ToolError::InvalidArgs(
            "Google INVALID_ARGUMENT (HTTP 400): Bad Request".to_string()
        ))
    );
    r.assert_finished(&id);
}

/// [`HOLIDAYS`]' one event's path as it goes on the wire.
const HOLIDAY_EVENT: &str =
    "/calendar/v3/calendars/en.usa%23holiday@group.v.calendar.google.com/events/veterans0123456789";

/// events.get's query as `CalendarEvent` sends it.
fn event_query() -> Pairs {
    google_query(&[
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

/// `CalendarEvent` reads one event of a calendar named by an id holding `#`,
/// with alice's bearer, and answers it with its attendees and their responses
/// counted.
#[tokio::test]
async fn calendar_event_reads_one_event_with_its_attendees() {
    let r = rig().await;
    r.answer(
        HOLIDAY_EVENT,
        serde_json::to_string_pretty(&json!({
            "id": "veterans0123456789",
            "status": "confirmed",
            "summary": "Veterans Day parade",
            "description": "<b>Bring</b> a chair",
            "start": { "date": "2026-11-11" },
            "end": { "date": "2026-11-12" },
            "organizer": { "email": HOLIDAYS, "displayName": "Holidays" },
            "attendees": [
                { "email": ACCOUNT, "self": true, "responseStatus": "accepted" },
                { "email": "bob@example.com", "responseStatus": "declined", "optional": true },
            ],
        }))
        .unwrap(),
    );
    let call = r.call_for_alice(
        "CalendarEvent",
        json!({ "id": "veterans0123456789", "calendar": HOLIDAYS }),
    );
    let id = r.requested_id().await;
    r.run(1).await;

    assert_eq!(
        call.await.unwrap().expect("the component answered"),
        json!({
            "id": "veterans0123456789",
            "summary": "Veterans Day parade",
            "description": "<b>Bring</b> a chair",
            "description_chars": 20,
            "start": { "date": "2026-11-11" },
            "end": { "date": "2026-11-12" },
            "status": "confirmed",
            "organizer": { "email": HOLIDAYS, "name": "Holidays" },
            "self_response": "accepted",
            "attendees": [
                { "email": ACCOUNT, "response": "accepted" },
                { "email": "bob@example.com", "response": "declined", "optional": true },
            ],
            "attendees_total": 2,
            "attendee_responses": {
                "accepted": 1,
                "declined": 1,
                "tentative": 0,
                "needs_action": 0,
            },
        })
    );
    assert_eq!(
        r.seen(),
        [("GET".to_string(), HOLIDAY_EVENT.to_string(), event_query())]
    );
    assert_eq!(r.bearers(), [format!("Bearer {BEARER}")]);
    r.assert_finished(&id);
}

/// An event that cannot be read whole is `internal`, naming why and nothing
/// to narrow; nothing is read again.
#[tokio::test]
async fn an_event_too_large_to_read_whole_is_refused_with_the_reason_alone() {
    for way in BOTH_WAYS {
        let r = unreadable_rig(way).await;
        let wide = json!({
            "id": "e1",
            "description": "d".repeat(OVERSIZED),
            "start": { "date": "2026-11-03" },
            "end": { "date": "2026-11-04" },
        })
        .to_string();
        r.answer("/calendar/v3/calendars/primary/events/e1", wide.clone());
        let call = r.call_for_alice("CalendarEvent", json!({ "id": "e1" }));
        let id = r.requested_id().await;
        r.run(1).await;

        let Err(ToolError::Internal(detail)) = call.await.unwrap() else {
            panic!("{way:?}: the event was not refused internal");
        };
        assert_unreadable(way, &detail, wide.len());
        assert_eq!(r.seen().len(), 1, "{way:?}");
        r.owed(&r.executor_sub(), 0).await;
        r.assert_finished(&id);
    }
}

/// Google's 404 for an event not on the calendar asked is the caller's:
/// `invalid_args` with Google's message.
#[tokio::test]
async fn a_google_404_on_an_event_is_invalid_args() {
    let r = rig().await;
    r.respond(
        "/calendar/v3/calendars/primary/events/e1",
        404,
        json!({ "error": { "code": 404, "message": "Not Found", "status": "NOT_FOUND" } })
            .to_string()
            .into_bytes(),
    );
    let call = r.call_for_alice("CalendarEvent", json!({ "id": "e1" }));
    let id = r.requested_id().await;
    r.run(1).await;

    assert_eq!(
        call.await.unwrap(),
        Err(ToolError::InvalidArgs(
            "Google NOT_FOUND (HTTP 404): Not Found".to_string()
        ))
    );
    r.assert_finished(&id);
}

/// `CalendarFreeBusy` asks for two calendars, one named by an id holding `#`,
/// in one `POST` carrying alice's bearer and the window as a JSON body, and
/// answers each calendar's busy intervals or Google's reasons.
#[tokio::test]
async fn calendar_free_busy_posts_one_query_and_answers_each_calendar() {
    let r = rig().await;
    r.answer(
        FREE_BUSY,
        serde_json::to_string_pretty(&json!({
            "kind": "calendar#freeBusy",
            "timeMin": "2026-11-02T05:00:00.000Z",
            "timeMax": "2026-11-09T05:00:00.000Z",
            "calendars": {
                "primary": { "busy": [
                    { "start": "2026-11-03T14:00:00Z", "end": "2026-11-03T15:00:00Z" },
                ] },
                HOLIDAYS: {
                    "errors": [{ "domain": "global", "reason": "notFound" }],
                    "busy": [],
                },
            },
        }))
        .unwrap(),
    );
    let call = r.call_for_alice(
        "CalendarFreeBusy",
        json!({ "from": FROM, "to": TO, "calendars": ["primary", HOLIDAYS] }),
    );
    let id = r.requested_id().await;
    r.run(1).await;

    assert_eq!(
        call.await.unwrap().expect("the component answered"),
        json!({
            "from": "2026-11-02T05:00:00.000Z",
            "to": "2026-11-09T05:00:00.000Z",
            "calendars": {
                "primary": { "busy": [
                    { "start": "2026-11-03T14:00:00Z", "end": "2026-11-03T15:00:00Z" },
                ] },
                HOLIDAYS: { "errors": ["notFound"] },
            },
        })
    );
    assert_eq!(
        r.seen(),
        [("POST".to_string(), FREE_BUSY.to_string(), google_query(&[]))]
    );
    let bodies = r.bodies();
    assert_eq!(bodies.len(), 1);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&bodies[0]).unwrap(),
        json!({
            "timeMin": FROM,
            "timeMax": TO,
            "items": [{ "id": "primary" }, { "id": HOLIDAYS }],
            "calendarExpansionMax": 2,
        })
    );
    assert_eq!(r.bearers(), [format!("Bearer {BEARER}")]);
    r.assert_finished(&id);
}

/// Without `calendars`, the query asks for `primary` alone.
#[tokio::test]
async fn calendar_free_busy_asks_for_primary_by_default() {
    let r = rig().await;
    r.answer(
        FREE_BUSY,
        json!({ "calendars": { "primary": { "busy": [] } } }).to_string(),
    );
    let call = r.call_for_alice("CalendarFreeBusy", json!({ "from": FROM, "to": TO }));
    let id = r.requested_id().await;
    r.run(1).await;

    assert_eq!(
        call.await.unwrap(),
        Ok(json!({ "calendars": { "primary": { "busy": [] } } }))
    );
    let body: serde_json::Value = serde_json::from_slice(&r.bodies()[0]).unwrap();
    assert_eq!(body["items"], json!([{ "id": "primary" }]));
    r.assert_finished(&id);
}

/// A free/busy answer that cannot be read whole is `internal`, naming why and
/// what to narrow; nothing is asked again.
#[tokio::test]
async fn a_free_busy_window_too_large_to_read_whole_is_refused_with_what_to_narrow() {
    for way in BOTH_WAYS {
        let r = unreadable_rig(way).await;
        let interval = json!({ "start": "2026-11-03T14:00:00Z", "end": "2026-11-03T15:00:00Z" });
        let busy = vec![interval; OVERSIZED / 60];
        let wide = json!({ "calendars": { "primary": { "busy": busy } } }).to_string();
        assert!(wide.len() > OVERSIZED - 1024, "{}", wide.len());
        r.answer(FREE_BUSY, wide.clone());
        let call = r.call_for_alice("CalendarFreeBusy", json!({ "from": FROM, "to": TO }));
        let id = r.requested_id().await;
        r.run(1).await;

        let Err(ToolError::Internal(detail)) = call.await.unwrap() else {
            panic!("{way:?}: the window was not refused internal");
        };
        let why = detail
            .strip_suffix("; ask for a shorter `from`/`to` window, or fewer `calendars`")
            .unwrap_or_else(|| panic!("{detail}"));
        assert_unreadable(way, why, wide.len());
        assert_eq!(r.seen().len(), 1, "{way:?}");
        r.owed(&r.executor_sub(), 0).await;
        r.assert_finished(&id);
    }
}

/// A `POST` the instance's grant does not admit is refused when it is
/// queued, even though every `GET` clause stands: the request is answered
/// `internal` with Brenn's text and the clause guidance.
#[tokio::test]
async fn a_free_busy_post_without_its_clause_could_not_be_issued() {
    let clauses: Vec<(&str, &str)> = CLAUSES
        .iter()
        .copied()
        .filter(|&(method, _)| method != "POST")
        .collect();
    let r = rig_with(&clauses).await;
    let call = r.call_for_alice("CalendarFreeBusy", json!({ "from": FROM, "to": TO }));
    let id = r.requested_id().await;
    r.serve().await;

    assert_could_not_be_issued(call.await.unwrap());
    r.owed(&r.executor_sub(), 0).await;
    assert!(r.seen().is_empty(), "a refused call reaches no API");
    r.assert_finished(&id);
}
