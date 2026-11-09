//! The wrapper's decisions, in `std` plus `serde_json`: validating
//! `CalendarList`'s, `CalendarEvents`', `CalendarEvent`'s and
//! `CalendarFreeBusy`'s arguments, building their Calendar calls as
//! `api-request` arguments, reading `api-request`'s answer, and compacting
//! Google's JSON into the tool's result. Nothing here names the guest SDK, so
//! the whole module runs on the host under test. Tools are the generated
//! `Provided` enum, whose SDK-facing parts are wasm32-only.

// The host test's crate root is this file, so it declares the generated
// module itself; in the component, `lib.rs` does.
#[cfg(test)]
mod spec;

use brenn_google_workspace::args::{QUERY_MAX_CHARS, account, id_list, max_chars};
use brenn_google_workspace::calendar::{
    ACCOUNT_MAX_BYTES, ATTENDEES_MAX, Compact, EVENT_GET_FIELDS, LISTING_TEXT_MAX_BYTES,
    PRIMARY_CALENDAR, SHORT_MAX_BYTES, calendar_id, calendar_id_rule, compact_event, event_id,
    event_path, events_path, is_calendar_id, is_event_id, is_listed_calendar_id, self_response,
    time,
};
use brenn_google_workspace::call::{get_call, json_call};
use brenn_google_workspace::scope::{Scope, google_error, no_body_refused};
use brenn_google_workspace::settings::Settings;
use brenn_google_workspace::text::{
    PAGE_MAX_CHARS, bounded_quote, fit_text, next_page, plain, shape,
};
use brenn_provider_kit::{
    Answer, ApiCall, Args, COMPACT_RESULT_CEILING, Call, Failure, Logic, NestedError, Next, Query,
    QueryValue, Tool, Unreadable, bounded, capped, cut, google, integer, internal, no_such_step,
    object, query, read_api_result, stored,
};
use serde_json::{Value, json};

use crate::spec::Provided;

/// The path Calendar lists the user's calendars at.
const CALENDAR_LIST_PATH: &str = "/calendar/v3/users/me/calendarList";
/// calendarList.list's mask: what a listing line carries, and the page token.
const CALENDAR_LIST_FIELDS: &str = "items(id,summary,primary,accessRole,timeZone),nextPageToken";
/// calendarList.list's page size: the most entries whose worst-case answer
/// stays under `COMPACT_RESULT_CEILING` (an entry serializes to at most
/// 1 297 bytes; 100 of them and a page token to about 138 000).
const CALENDAR_LIST_PAGE: usize = 100;

/// events.list's mask: what an events line carries, the page token and the
/// calendar's time zone. Of the attendees only the entry Google marks `self`
/// is read: the calendar read's own, the user's on `primary`.
const EVENTS_LIST_FIELDS: &str = "items(id,summary,start,end,location,status,organizer/email,\
                                  attendees(self,responseStatus)),nextPageToken,timeZone";
/// events.list's `maxAttendees`: past it Google returns only the calendar
/// read's own attendee entry, which is the one an events line answers.
const EVENTS_LIST_ATTENDEES: &str = "1";
/// `CalendarEvents`' `max`: the range and the default. A line serializes to
/// at most about 3 900 bytes, so 50 lines and the page's envelope stay under
/// `COMPACT_RESULT_CEILING` (about 203 000); 70 would not.
const EVENTS_MAX_MIN: u64 = 1;
const EVENTS_MAX_MAX: u64 = 50;
const EVENTS_MAX_DEFAULT: u64 = 50;
/// What the caller can narrow when a window's events are unreadable.
const EVENTS_HINT: &str = "ask for fewer events with `max`, or a shorter `from`/`to` window";

/// The path Calendar answers free/busy queries at.
const FREEBUSY_PATH: &str = "/calendar/v3/freeBusy";
/// The most calendars one free/busy query may name: Google's limit.
const FREEBUSY_CALENDARS_MAX: usize = 50;
/// The most UTF-8 bytes the `calendars` ids may hold together. The query's
/// body carries every id uncut and travels as one JSON string inside the
/// nested call's args, which the host caps at 64 KiB; with this budget, the
/// window, `account` at its cap and the call's envelope, the widest admitted
/// call serializes to about 52 000 bytes. An id can hold no character JSON
/// escapes (`calendar_id_shape` refuses `"`, `\` and controls), so its bytes
/// are what it adds; the window and `account` can, so they count at their
/// escaped width: 7 bytes a character for the window, escaped in the body
/// and again in the args, and 6 for `account`, escaped once.
const FREEBUSY_IDS_MAX_BYTES: usize = 48_000;
/// What the caller can narrow when a free/busy answer is unreadable or past
/// the result ceiling.
const FREEBUSY_HINT: &str = "ask for a shorter `from`/`to` window, or fewer `calendars`";
/// The most calendar ids a free/busy refusal quotes from each side of a
/// mismatch between the calendars asked and those Google answered.
const QUOTED_IDS_MAX: usize = 5;

/// The scope `tool`'s step `step` needs. A step the tool does not have is
/// never asked for: `advance` maps an error only for a step it issued.
fn scope(tool: Provided, step: u8) -> Scope {
    match (tool, step) {
        (
            Provided::CalendarList
            | Provided::CalendarEvents
            | Provided::CalendarEvent
            | Provided::CalendarFreeBusy,
            _,
        ) => Scope::CalendarRead,
    }
}

impl Tool for Provided {
    fn name(self) -> &'static str {
        Provided::name(self)
    }

    fn from_name(name: &str) -> Option<Provided> {
        Provided::from_name(name)
    }
}

/// A request's in-flight row, naming one of this component's tools.
pub type InFlight = brenn_provider_kit::InFlight<Provided>;

/// This component's step functions, for the kit's activation loop.
pub struct CalendarReads;

impl Logic for CalendarReads {
    type Tool = Provided;
    type Settings = Settings;

    fn plan(tool: Provided, args: &Value, settings: &Settings) -> Result<Call, String> {
        plan(tool, args, settings)
    }

    fn advance(state: &InFlight, outcome: Result<Value, NestedError>, _: &Settings) -> Next {
        advance(state, outcome)
    }
}

/// The `calendars` argument: 1 to `FREEBUSY_CALENDARS_MAX` distinct calendar
/// ids, each as `calendar_id` bounds it, together at most
/// `FREEBUSY_IDS_MAX_BYTES`; absent is `primary` alone. Google answers one
/// entry per distinct id, so a repeated id is refused.
fn calendars(args: &Args) -> Result<Vec<String>, String> {
    let Some(out) = id_list(
        args,
        "calendars",
        (1, FREEBUSY_CALENDARS_MAX),
        "calendar ids",
        is_calendar_id,
        calendar_id_rule,
    )?
    else {
        return Ok(vec![PRIMARY_CALENDAR.to_string()]);
    };
    if out.iter().map(String::len).sum::<usize>() > FREEBUSY_IDS_MAX_BYTES {
        return Err(format!(
            "`calendars` together exceed {FREEBUSY_IDS_MAX_BYTES} bytes (UTF-8); ask for fewer"
        ));
    }
    Ok(out)
}

/// Validate a request's arguments and build its first nested call. An error is
/// the request's `invalid_args` detail, naming the argument.
pub fn plan(tool: Provided, args: &Value, settings: &Settings) -> Result<Call, String> {
    match tool {
        Provided::CalendarList => {
            let list = CalendarListArgs::parse(args)?;
            Ok(Call {
                step: 1,
                api: calendar_list_call(&list, settings),
            })
        }
        Provided::CalendarEvents => {
            let events = EventsArgs::parse(args)?;
            Ok(Call {
                step: 1,
                api: events_list_call(&events, settings),
            })
        }
        Provided::CalendarEvent => {
            let event = EventArgs::parse(args)?;
            Ok(Call {
                step: 1,
                api: event_get_call(&event, settings),
            })
        }
        Provided::CalendarFreeBusy => {
            let free_busy = FreeBusyArgs::parse(args)?;
            Ok(Call {
                step: 1,
                api: free_busy_call(&free_busy, settings),
            })
        }
    }
}

/// `CalendarList`'s arguments.
struct CalendarListArgs {
    page: Option<String>,
    account: Option<String>,
}

impl CalendarListArgs {
    /// Every argument the tool takes.
    const KEYS: &[&str] = &["page", "account"];

    fn parse(args: &Value) -> Result<CalendarListArgs, String> {
        let map = object(args, Self::KEYS)?;
        Ok(CalendarListArgs {
            page: capped(map, "page", PAGE_MAX_CHARS)?.map(str::to_string),
            account: account(map)?,
        })
    }
}

/// calendarList.list, one page of at most `CALENDAR_LIST_PAGE` entries.
fn calendar_list_call(args: &CalendarListArgs, settings: &Settings) -> ApiCall {
    let mut query = query([
        ("maxResults", CALENDAR_LIST_PAGE.to_string()),
        ("fields", CALENDAR_LIST_FIELDS.to_string()),
    ]);
    if let Some(page) = &args.page {
        query.insert("pageToken".to_string(), QueryValue::One(page.clone()));
    }
    get_call(
        settings,
        args.account.as_deref(),
        CALENDAR_LIST_PATH.to_string(),
        query,
    )
}

/// `CalendarEvents`' arguments.
struct EventsArgs {
    calendar: String,
    from: String,
    to: String,
    q: Option<String>,
    max: usize,
    page: Option<String>,
    account: Option<String>,
}

impl EventsArgs {
    /// Every argument the tool takes.
    const KEYS: &[&str] = &["calendar", "from", "to", "q", "max", "page", "account"];

    fn parse(args: &Value) -> Result<EventsArgs, String> {
        let map = object(args, Self::KEYS)?;
        let max =
            integer(map, "max", EVENTS_MAX_MIN, EVENTS_MAX_MAX)?.unwrap_or(EVENTS_MAX_DEFAULT);
        Ok(EventsArgs {
            calendar: calendar_id(map, "calendar")?.unwrap_or_else(|| PRIMARY_CALENDAR.to_string()),
            from: time(map, "from")?.ok_or("`from` is required")?,
            to: time(map, "to")?.ok_or("`to` is required")?,
            q: capped(map, "q", QUERY_MAX_CHARS)?.map(str::to_string),
            max: usize::try_from(max).expect("max is at most 50"),
            page: capped(map, "page", PAGE_MAX_CHARS)?.map(str::to_string),
            account: account(map)?,
        })
    }
}

/// events.list of the window `from`..`to`, recurring events expanded into
/// their instances in start order, one page of at most `max`.
fn events_list_call(args: &EventsArgs, settings: &Settings) -> ApiCall {
    let mut query = query([
        ("timeMin", args.from.clone()),
        ("timeMax", args.to.clone()),
        ("singleEvents", "true".to_string()),
        ("orderBy", "startTime".to_string()),
        ("maxResults", args.max.to_string()),
        ("maxAttendees", EVENTS_LIST_ATTENDEES.to_string()),
        ("fields", EVENTS_LIST_FIELDS.to_string()),
    ]);
    if let Some(q) = &args.q {
        query.insert("q".to_string(), QueryValue::One(q.clone()));
    }
    if let Some(page) = &args.page {
        query.insert("pageToken".to_string(), QueryValue::One(page.clone()));
    }
    get_call(
        settings,
        args.account.as_deref(),
        events_path(&args.calendar),
        query,
    )
}

/// `CalendarEvent`'s arguments.
struct EventArgs {
    id: String,
    calendar: String,
    max_chars: usize,
    account: Option<String>,
}

impl EventArgs {
    /// Every argument the tool takes.
    const KEYS: &[&str] = &["id", "calendar", "max_chars", "account"];

    fn parse(args: &Value) -> Result<EventArgs, String> {
        let map = object(args, Self::KEYS)?;
        Ok(EventArgs {
            id: event_id(map, "id")?.ok_or("`id` is required")?,
            calendar: calendar_id(map, "calendar")?.unwrap_or_else(|| PRIMARY_CALENDAR.to_string()),
            max_chars: max_chars(map)?,
            account: account(map)?,
        })
    }
}

/// events.get of one event, at most `ATTENDEES_MAX` attendees listed.
fn event_get_call(args: &EventArgs, settings: &Settings) -> ApiCall {
    get_call(
        settings,
        args.account.as_deref(),
        event_path(&args.calendar, &args.id),
        query([
            ("maxAttendees", ATTENDEES_MAX.to_string()),
            ("fields", EVENT_GET_FIELDS.to_string()),
        ]),
    )
}

/// `CalendarFreeBusy`'s arguments.
struct FreeBusyArgs {
    from: String,
    to: String,
    calendars: Vec<String>,
    account: Option<String>,
}

impl FreeBusyArgs {
    /// Every argument the tool takes.
    const KEYS: &[&str] = &["from", "to", "calendars", "account"];

    fn parse(args: &Value) -> Result<FreeBusyArgs, String> {
        let map = object(args, Self::KEYS)?;
        Ok(FreeBusyArgs {
            from: time(map, "from")?.ok_or("`from` is required")?,
            to: time(map, "to")?.ok_or("`to` is required")?,
            calendars: calendars(map)?,
            account: account(map)?,
        })
    }
}

/// freebusy.query for the window `from`..`to` over `calendars`. The POST
/// changes nothing; `api-request` admits it only for a stamped call.
fn free_busy_call(args: &FreeBusyArgs, settings: &Settings) -> ApiCall {
    let items: Vec<Value> = args
        .calendars
        .iter()
        .map(|id| json!({ "id": id }))
        .collect();
    json_call(
        settings,
        "POST",
        args.account.as_deref(),
        FREEBUSY_PATH.to_string(),
        Query::new(),
        json!({
            "timeMin": args.from,
            "timeMax": args.to,
            "items": items,
            "calendarExpansionMax": args.calendars.len(),
        }),
    )
}

/// What the request does with its nested call's answer: complete or fail.
/// Every answer completed here is held to the result ceiling; a relayed
/// credential answer is `api-request`'s own and is not.
pub fn advance(state: &InFlight, outcome: Result<Value, NestedError>) -> Next {
    let body = match read_api_result(outcome, &state.target, google::API_NAME) {
        Ok(Answer::Body(body)) => body,
        Ok(Answer::NoBody { status }) => return settle(state.tool, no_body(state, status)),
        Ok(Answer::Relay(value)) => return Next::Complete(value),
        Ok(Answer::Unreadable(why)) => return unreadable(state, why),
        Ok(Answer::Error { status, body }) => {
            return Next::Fail(google_error(status, &body, scope(state.tool, state.step)));
        }
        Ok(Answer::Bytes { status, .. }) => {
            return Next::Fail(internal(format!(
                "{} returned a non-text body ({status})",
                google::API_NAME
            )));
        }
        Err(failure) => return Next::Fail(failure),
    };
    settle(
        state.tool,
        match (state.tool, state.step) {
            (Provided::CalendarList, 1) => compact_calendar_list(&body).map(Next::Complete),
            (Provided::CalendarEvents, 1) => stored(state, EventsArgs::parse)
                .and_then(|args| compact_events(&body, args.max))
                .map(Next::Complete),
            (Provided::CalendarEvent, 1) => stored(state, EventArgs::parse)
                .and_then(|args| compact_event(&body, "events.get", Some(&args.id), args.max_chars))
                .map(Next::Complete),
            (Provided::CalendarFreeBusy, 1) => stored(state, FreeBusyArgs::parse)
                .and_then(|args| compact_free_busy(&body, &args))
                .map(Next::Complete),
            (tool, step) => Err(no_such_step(tool, step)),
        },
    )
}

/// A nested read that was cut or refused, decided by the step's shape: a
/// window's events or free/busy fail with why and what to narrow; every other
/// step fails with why.
fn unreadable(state: &InFlight, why: Unreadable) -> Next {
    settle(
        state.tool,
        match (state.tool, state.step) {
            (Provided::CalendarList | Provided::CalendarEvent, 1) => {
                Err(why.failure(google::API_NAME, None))
            }
            (Provided::CalendarEvents, 1) => Err(why.failure(google::API_NAME, Some(EVENTS_HINT))),
            (Provided::CalendarFreeBusy, 1) => {
                Err(why.failure(google::API_NAME, Some(FREEBUSY_HINT)))
            }
            (tool, step) => Err(no_such_step(tool, step)),
        },
    )
}

/// A 2xx with no body, which no step here accepts: refused, naming the API
/// whose answer it expected, as that step's `shape` failure names it.
fn no_body(state: &InFlight, status: u64) -> Result<Next, Failure> {
    let (api, answers) = match (state.tool, state.step) {
        (Provided::CalendarList, 1) => ("calendarList.list", "JSON"),
        (Provided::CalendarEvents, 1) => ("events.list", "JSON"),
        (Provided::CalendarEvent, 1) => ("events.get", "JSON"),
        (Provided::CalendarFreeBusy, 1) => ("freebusy.query", "JSON"),
        (tool, step) => return Err(no_such_step(tool, step)),
    };
    Err(no_body_refused(status, api, answers))
}

/// A step's decision as the request's next move, a completed answer held to
/// the result ceiling — after `CalendarEvent`'s description is cut to fit it;
/// a `CalendarFreeBusy` answer past it is refused with what to narrow.
fn settle(tool: Provided, decided: Result<Next, Failure>) -> Next {
    match decided {
        Ok(Next::Complete(value)) => {
            let (value, hint) = match tool {
                Provided::CalendarEvent => {
                    (fit_text(value, "description", COMPACT_RESULT_CEILING), None)
                }
                Provided::CalendarFreeBusy => (value, Some(FREEBUSY_HINT)),
                Provided::CalendarList | Provided::CalendarEvents => (value, None),
            };
            bounded(value, COMPACT_RESULT_CEILING, hint)
        }
        Ok(next) => next,
        Err(failure) => Next::Fail(failure),
    }
}

/// calendarList.list as `{ "calendars": [...], "next_page"? }` in Google's
/// order: each entry's `id`, `summary`, `primary` (only when true),
/// `access_role` and `time_zone`. Google omits `items` when there are none,
/// which is an empty list; more entries than the page size asked for is
/// Google's fault.
fn compact_calendar_list(body: &Value) -> Result<Value, Failure> {
    let api = "calendarList.list";
    let mut page = Compact::of(body, api)?;
    let items = page.array("items")?;
    if items.len() > CALENDAR_LIST_PAGE {
        return Err(internal(format!(
            "Google {api} answered {} items for maxResults {CALENDAR_LIST_PAGE}",
            items.len()
        )));
    }
    let calendars = items
        .iter()
        .map(|item| calendar_entry(item, api))
        .collect::<Result<Vec<_>, _>>()?;
    page.out
        .insert("calendars".to_string(), Value::Array(calendars));
    next_page(&mut page.out, page.item, api)?;
    Ok(page.done())
}

/// One calendar of the user's list.
fn calendar_entry(item: &Value, api: &str) -> Result<Value, Failure> {
    let mut entry = Compact::of(item, api)?;
    entry.id("id", "id", is_listed_calendar_id)?;
    entry.text("summary", "summary", LISTING_TEXT_MAX_BYTES)?;
    entry.when_true("primary", "primary")?;
    entry.short("accessRole", "access_role")?;
    entry.short("timeZone", "time_zone")?;
    Ok(entry.done())
}

/// events.list as `{ "events": [...], "time_zone"?, "next_page"? }` in
/// Google's order (start time). Google omits `items` when there are none,
/// which is an empty list; more events than `max` is Google's fault.
fn compact_events(body: &Value, max: usize) -> Result<Value, Failure> {
    let api = "events.list";
    let mut page = Compact::of(body, api)?;
    let items = page.array("items")?;
    if items.len() > max {
        return Err(internal(format!(
            "Google {api} answered {} events for maxResults {max}",
            items.len()
        )));
    }
    let events = items
        .iter()
        .map(|item| event_line(item, api))
        .collect::<Result<Vec<_>, _>>()?;
    page.out.insert("events".to_string(), Value::Array(events));
    page.short("timeZone", "time_zone")?;
    next_page(&mut page.out, page.item, api)?;
    Ok(page.done())
}

/// One event of a window: `id`, `summary`, `start`, `end`, `location`,
/// `status`, `organizer` (the organizer's email) and `self_response` (the
/// calendar read's own `responseStatus`, when that calendar is an attendee).
fn event_line(item: &Value, api: &str) -> Result<Value, Failure> {
    let mut line = Compact::of(item, api)?;
    line.id("id", "id", is_event_id)?;
    line.text("summary", "summary", LISTING_TEXT_MAX_BYTES)?;
    line.when("start")?;
    line.when("end")?;
    line.text("location", "location", LISTING_TEXT_MAX_BYTES)?;
    line.short("status", "status")?;
    if let Some(organizer) = line.object("organizer")?
        && let Some(email) = organizer.string("email")?
    {
        line.put_text("organizer", email, ACCOUNT_MAX_BYTES);
    }
    if let Some(response) = self_response(&line)? {
        line.put_short("self_response", response);
    }
    Ok(line.done())
}

/// freebusy.query as `{ "from"?, "to"?, "calendars": { <id>: { "busy"?,
/// "errors"? } } }`: Google's echo of the window, and per calendar asked its
/// busy intervals (`start`, `end`, as Google gave them) or the reasons Google
/// could not answer for it. The empty `busy` Google sends beside reasons is
/// left out, since it would read as free time. The calendars must be exactly
/// those asked, and the refusal otherwise names the ids on each side of the
/// difference; one with neither `busy` nor reasons is Google's fault. The
/// calendars are keyed by the ids as asked, uncut: the keys are at most
/// `FREEBUSY_IDS_MAX_BYTES` bytes together, and the result ceiling's refusal
/// in `settle` is what bounds the whole answer.
fn compact_free_busy(body: &Value, args: &FreeBusyArgs) -> Result<Value, Failure> {
    let api = "freebusy.query";
    let mut answer = Compact::of(body, api)?;
    answer.short("timeMin", "from")?;
    answer.short("timeMax", "to")?;
    let Some(Value::Object(google)) = answer.item.get("calendars") else {
        return Err(shape(api));
    };
    let unasked: Vec<&String> = google
        .keys()
        .filter(|id| !args.calendars.contains(*id))
        .collect();
    let unanswered: Vec<&String> = args
        .calendars
        .iter()
        .filter(|id| !google.contains_key(*id))
        .collect();
    if !(unasked.is_empty() && unanswered.is_empty()) {
        return Err(internal(format!(
            "unexpected Google {api} response shape: calendars answered but not asked: {}; \
             calendars asked but not answered: {}",
            quoted_ids(&unasked),
            quoted_ids(&unanswered)
        )));
    }
    let mut calendars = Args::new();
    for (id, calendar) in google {
        let calendar = Compact::of(calendar, api)?;
        let busy = calendar
            .array("busy")?
            .iter()
            .map(|interval| busy_interval(interval, api))
            .collect::<Result<Vec<_>, _>>()?;
        let errors = calendar
            .array("errors")?
            .iter()
            .map(|error| match Compact::of(error, api)?.string("reason")? {
                Some(reason) => Ok(json!(cut(&plain(reason), SHORT_MAX_BYTES).0)),
                None => Err(shape(api)),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut out = Args::new();
        if errors.is_empty() {
            if !calendar.item.contains_key("busy") {
                return Err(shape(api));
            }
            out.insert("busy".to_string(), Value::Array(busy));
        } else {
            if !busy.is_empty() {
                out.insert("busy".to_string(), Value::Array(busy));
            }
            out.insert("errors".to_string(), Value::Array(errors));
        }
        calendars.insert(id.clone(), Value::Object(out));
    }
    answer
        .out
        .insert("calendars".to_string(), Value::Object(calendars));
    Ok(answer.done())
}

/// Calendar ids for a refusal's detail: the first `QUOTED_IDS_MAX`, each
/// through `bounded_quote`, then how many more; `none` when empty.
fn quoted_ids(ids: &[&String]) -> String {
    if ids.is_empty() {
        return "none".to_string();
    }
    let mut out = ids
        .iter()
        .take(QUOTED_IDS_MAX)
        .map(|id| format!("\"{}\"", bounded_quote(id)))
        .collect::<Vec<_>>()
        .join(", ");
    if ids.len() > QUOTED_IDS_MAX {
        out.push_str(&format!(" and {} more", ids.len() - QUOTED_IDS_MAX));
    }
    out
}

/// One busy interval: Google's `start` and `end`, both required.
fn busy_interval(interval: &Value, api: &str) -> Result<Value, Failure> {
    let mut out = Compact::of(interval, api)?;
    out.short("start", "start")?;
    out.short("end", "end")?;
    if !(out.out.contains_key("start") && out.out.contains_key("end")) {
        return Err(shape(api));
    }
    Ok(out.done())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use brenn_google_workspace::args::{
        ACCOUNT_MAX_CHARS, HOST_ARGS_CAP_BYTES, MAX_CHARS_MAX, MAX_CHARS_MIN,
    };
    use brenn_google_workspace::calendar::{
        CALENDAR_ID_MAX_BYTES, CALENDAR_ID_MAX_CHARS, EVENT_ID_MAX_CHARS, EVENT_TEXT_MAX_BYTES,
        RECURRENCE_MAX, TIME_MAX_CHARS, calendar_id_pattern,
    };
    use brenn_google_workspace::scope::scope_hint;
    use brenn_google_workspace::testutil::{
        EVENT, EVENT_ID, admits_characters, assert_read_failures_are_bare, calendar_id_probes,
        completed, cut_read, description, escaped_widest, event_with, four_byte, lookup,
        no_body_parts, wide,
    };
    use brenn_google_workspace::text::PAGE_MAX_BYTES;
    use brenn_provider_kit::testutil::{failure, google, http, nested, text_body};
    use brenn_provider_kit::{
        Bound, ErrorKind, Probes, ToolBounds, check, hold_probes, hold_required, input_schema,
        probes, serialized_len,
    };
    use serde_json::{Map, json};

    use super::*;

    const BASE: &[(&str, &str)] = &[
        ("provider", "google"),
        ("calendar_host", "calendar.example.com"),
    ];

    fn settings() -> Settings {
        Settings::from_lookup(lookup(BASE), "calendar_host").unwrap()
    }

    /// `advance` on the row.
    fn answered(state: &InFlight, outcome: Result<Value, NestedError>) -> Next {
        advance(state, outcome)
    }

    /// The guide quotes, in its answer summary and in the assistant's
    /// instructions, the two description sentences that say whose response
    /// `self_response` and `organizer.self` carry; each quote must stay the
    /// spec's words.
    #[test]
    fn the_google_workspace_guide_quotes_the_calendar_descriptions() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let read = |relative: &str| {
            let path = root.join(relative);
            std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("{} must be readable: {error}", path.display()))
        };
        let spec = read("config/specs/google-calendar-read.brenn");
        // From the opening words to the delimiter, the delimiter kept when
        // `keep` is set.
        let quoted = |text: &str, opening: &str, delimiter: &str, keep: bool| -> String {
            let start = text
                .find(opening)
                .unwrap_or_else(|| panic!("no {opening:?} in the description"));
            let end = start
                + text[start..]
                    .find(delimiter)
                    .unwrap_or_else(|| panic!("no {delimiter:?} after {opening:?}"));
            let end = if keep { end + 1 } else { end };
            text[start..end].to_string()
        };
        let collapse = |text: &str| text.split_whitespace().collect::<Vec<_>>().join(" ");
        let guide = collapse(&read("docs/google-workspace.md"));
        for sentence in [
            quoted(
                &description(&spec, "calendar-events"),
                "the answer to the invitation of the calendar read",
                ")",
                false,
            ),
            quoted(
                &description(&spec, "calendar-event"),
                "The calendar read is the user on",
                ". ",
                true,
            ),
        ] {
            assert_eq!(
                guide.matches(&collapse(&sentence)).count(),
                2,
                "docs/google-workspace.md must quote {sentence:?} in section 5 and section 6"
            );
        }
    }

    #[test]
    fn a_google_403_on_a_calendar_tool_names_the_calendar_scope() {
        let body = google(
            403,
            "PERMISSION_DENIED",
            "Request had insufficient authentication scopes.",
        );
        assert_eq!(
            google_error(403, &body, Scope::CalendarRead),
            failure(
                ErrorKind::Denied,
                "Google PERMISSION_DENIED (HTTP 403): Request had insufficient authentication \
                 scopes. If Google names insufficient authentication scopes, the account was \
                 connected without the Calendar read scope: run oauth-connect with that scope \
                 set and relay its steps."
            )
        );
        let state = calendar_list_state(json!({}));
        assert_eq!(
            answered(&state, http(403, &body)),
            Next::Fail(failure(
                ErrorKind::Denied,
                &format!(
                    "Google PERMISSION_DENIED (HTTP 403): Request had insufficient \
                     authentication scopes. {}",
                    scope_hint(Scope::CalendarRead)
                )
            ))
        );
    }

    fn calendar_list(args: Value) -> Result<ApiCall, String> {
        plan(Provided::CalendarList, &args, &settings()).map(|call| {
            assert_eq!(call.step, 1);
            call.api
        })
    }

    fn calendar_list_query(extra: &[(&str, &str)]) -> Query {
        let mut query = query([
            ("maxResults", "100".to_string()),
            (
                "fields",
                "items(id,summary,primary,accessRole,timeZone),nextPageToken".to_string(),
            ),
            ("prettyPrint", "false".to_string()),
        ]);
        for (key, value) in extra {
            query.insert(key.to_string(), QueryValue::One(value.to_string()));
        }
        query
    }

    #[test]
    fn calendar_list_reads_one_page_on_the_calendar_host() {
        assert_eq!(
            calendar_list(json!({})).unwrap(),
            ApiCall {
                provider: "google".to_string(),
                account: None,
                method: "GET".to_string(),
                host: "calendar.example.com".to_string(),
                path: "/calendar/v3/users/me/calendarList".to_string(),
                query: calendar_list_query(&[]),
                headers: BTreeMap::new(),
                body: None,
            }
        );
        let call =
            calendar_list(json!({ "page": "page-2", "account": "alice@example.com" })).unwrap();
        assert_eq!(call.account.as_deref(), Some("alice@example.com"));
        assert_eq!(call.query, calendar_list_query(&[("pageToken", "page-2")]));
    }

    #[test]
    fn calendar_list_refuses_bad_arguments() {
        assert_eq!(
            calendar_list(json!({ "calendar": "primary" })).unwrap_err(),
            "unknown argument `calendar`; this tool takes `page`, `account`"
        );
        assert_eq!(
            calendar_list(json!({ "page": "" })).unwrap_err(),
            "`page` is empty"
        );
        assert_eq!(
            calendar_list(json!({ "page": "x".repeat(PAGE_MAX_CHARS + 1) })).unwrap_err(),
            "`page` is longer than 4096 characters"
        );
        assert_eq!(
            calendar_list(json!({ "page": 2 })).unwrap_err(),
            "`page` must be a string"
        );
        assert_eq!(
            calendar_list(json!({ "account": "x".repeat(321) })).unwrap_err(),
            "`account` is longer than 320 characters"
        );
    }

    const CALENDAR_LIST: &str = include_str!("fixtures/calendar-list.json");

    fn calendar_list_state(args: Value) -> InFlight {
        let call = plan(Provided::CalendarList, &args, &settings()).unwrap();
        InFlight::new(
            &"a".repeat(32),
            Provided::CalendarList,
            &args,
            &call,
            Map::new(),
            1_000,
            1_000,
        )
    }

    fn list_calendars(body: &str) -> Next {
        answered(&calendar_list_state(json!({})), http(200, body))
    }

    #[test]
    fn calendar_list_compacts_in_googles_order() {
        assert_eq!(
            list_calendars(CALENDAR_LIST),
            Next::Complete(json!({
                "calendars": [
                    {
                        "id": "alice@example.com",
                        "summary": "alice@example.com",
                        "primary": true,
                        "access_role": "owner",
                        "time_zone": "America/New_York",
                    },
                    {
                        "id": "c_0123456789abcdef@group.calendar.google.com",
                        "summary": "Family \"shared\"",
                        "access_role": "writer",
                        "time_zone": "America/Chicago",
                    },
                    {
                        "id": "en.usa#holiday@group.v.calendar.google.com",
                        "summary": "Holidays in United States",
                        "access_role": "reader",
                        "time_zone": "America/New_York",
                    },
                    {
                        "id": "bob@example.com",
                        "access_role": "freeBusyReader",
                        "time_zone": "Europe/London",
                    },
                ],
                "next_page": "page-2",
            }))
        );
        assert_eq!(
            list_calendars("{}"),
            Next::Complete(json!({ "calendars": [] }))
        );
        assert_eq!(
            list_calendars(r#"{ "items": [{ "id": "primary", "primary": false }] }"#),
            Next::Complete(json!({ "calendars": [{ "id": "primary" }] }))
        );
    }

    #[test]
    fn a_calendar_summary_is_plain_and_cut_flagged_and_vocabulary_cut_unflagged() {
        let body = json!({ "items": [{
            "id": "alice@example.com",
            "summary": format!("a\u{1}{}", "é".repeat(200)),
            "accessRole": "r".repeat(SHORT_MAX_BYTES + 10),
            "timeZone": "America/New_York",
        }] });
        let answer = completed(list_calendars(&body.to_string()));
        let entry = &answer["calendars"][0];
        let summary = entry["summary"].as_str().unwrap();
        assert!(summary.starts_with("a é"), "{summary}");
        assert!(summary.ends_with('…'), "{summary}");
        assert!(summary.len() <= LISTING_TEXT_MAX_BYTES + '…'.len_utf8());
        assert_eq!(entry["fields_truncated"], json!(["summary"]));
        assert_eq!(
            entry["access_role"],
            json!(format!("{}…", "r".repeat(SHORT_MAX_BYTES)))
        );
    }

    #[test]
    fn calendar_list_refuses_an_unexpected_shape() {
        let shape = || {
            Next::Fail(failure(
                ErrorKind::Internal,
                "unexpected Google calendarList.list response shape",
            ))
        };
        for body in [
            json!([]),
            json!({ "items": {} }),
            json!({ "items": [1] }),
            json!({ "items": [{}] }),
            json!({ "items": [{ "id": 7 }] }),
            json!({ "items": [{ "id": "a/b" }] }),
            json!({ "items": [{ "id": "x".repeat(CALENDAR_ID_MAX_BYTES + 1) }] }),
            json!({ "items": [{ "id": "primary", "summary": 1 }] }),
            json!({ "items": [{ "id": "primary", "primary": "yes" }] }),
            json!({ "items": [{ "id": "primary", "timeZone": null }] }),
            json!({ "items": [], "nextPageToken": 3 }),
            json!({ "items": [], "nextPageToken": "" }),
            json!({ "items": [], "nextPageToken": " " }),
            json!({ "items": [], "nextPageToken": "x".repeat(PAGE_MAX_BYTES + 1) }),
        ] {
            assert_eq!(list_calendars(&body.to_string()), shape(), "{body}");
        }
    }

    #[test]
    fn calendar_list_refuses_more_entries_than_it_asked_for() {
        let items: Vec<Value> = (0..=CALENDAR_LIST_PAGE)
            .map(|i| json!({ "id": format!("c{i}@example.com") }))
            .collect();
        assert_eq!(
            list_calendars(&json!({ "items": items }).to_string()),
            Next::Fail(failure(
                ErrorKind::Internal,
                "Google calendarList.list answered 101 items for maxResults 100"
            ))
        );
    }

    #[test]
    fn calendar_list_relays_googles_errors() {
        let state = calendar_list_state(json!({ "page": "stale" }));
        assert_eq!(
            answered(
                &state,
                http(400, &google(400, "INVALID_ARGUMENT", "Invalid page token"))
            ),
            Next::Fail(failure(
                ErrorKind::InvalidArgs,
                "Google INVALID_ARGUMENT (HTTP 400): Invalid page token"
            ))
        );
    }

    #[test]
    fn the_widest_calendar_list_page_fits_the_ceiling() {
        // Every string at its bound and as wide as it serializes: a summary
        // of quotes past its cut, a full-length id, vocabulary past its cut.
        let items: Vec<Value> = (0..CALENDAR_LIST_PAGE)
            .map(|i| {
                let id = format!("{i:03}{}", "a".repeat(CALENDAR_ID_MAX_BYTES - 3));
                json!({
                    "id": id,
                    "summary": "\"".repeat(LISTING_TEXT_MAX_BYTES + 1),
                    "primary": true,
                    "accessRole": "\"".repeat(SHORT_MAX_BYTES + 1),
                    "timeZone": "\"".repeat(SHORT_MAX_BYTES + 1),
                })
            })
            .collect();
        let body = json!({ "items": items, "nextPageToken": "\"".repeat(PAGE_MAX_BYTES) });
        let answer = completed(list_calendars(&body.to_string()));
        assert_eq!(
            answer["calendars"].as_array().unwrap().len(),
            CALENDAR_LIST_PAGE
        );
        assert_eq!(
            answer["calendars"][0]["fields_truncated"],
            json!(["summary"])
        );
        let size = serialized_len(&answer);
        assert!(size < 140_000, "{size}");
        assert!(size < COMPACT_RESULT_CEILING);
    }

    #[test]
    fn a_calendar_id_argument_is_refused_before_any_call() {
        let refusal = "`calendar` must be a calendar id of 1 to 320 characters with no `/`, `\\`, `%`, \
                       `\"`, space or control character";
        for bad in [
            json!("a/b"),
            json!("a%40b"),
            json!("a b"),
            json!(""),
            json!("x".repeat(CALENDAR_ID_MAX_CHARS + 1)),
            json!(7),
        ] {
            assert_eq!(
                events(json!({ "calendar": bad, "from": FROM, "to": TO })).unwrap_err(),
                refusal,
                "{bad}"
            );
        }
    }

    const FROM: &str = "2026-11-02T00:00:00-05:00";
    const TO: &str = "2026-11-09T00:00:00-05:00";

    fn events(args: Value) -> Result<ApiCall, String> {
        plan(Provided::CalendarEvents, &args, &settings()).map(|call| {
            assert_eq!(call.step, 1);
            call.api
        })
    }

    fn events_query(max: &str, extra: &[(&str, &str)]) -> Query {
        let mut query = query([
            ("timeMin", FROM.to_string()),
            ("timeMax", TO.to_string()),
            ("singleEvents", "true".to_string()),
            ("orderBy", "startTime".to_string()),
            ("maxResults", max.to_string()),
            ("maxAttendees", "1".to_string()),
            (
                "fields",
                "items(id,summary,start,end,location,status,organizer/email,\
                 attendees(self,responseStatus)),nextPageToken,timeZone"
                    .to_string(),
            ),
            ("prettyPrint", "false".to_string()),
        ]);
        for (key, value) in extra {
            query.insert(key.to_string(), QueryValue::One(value.to_string()));
        }
        query
    }

    #[test]
    fn events_reads_one_window_of_primary_on_the_calendar_host() {
        assert_eq!(
            events(json!({ "from": FROM, "to": TO })).unwrap(),
            ApiCall {
                provider: "google".to_string(),
                account: None,
                method: "GET".to_string(),
                host: "calendar.example.com".to_string(),
                path: "/calendar/v3/calendars/primary/events".to_string(),
                query: events_query("50", &[]),
                headers: BTreeMap::new(),
                body: None,
            }
        );
        let call = events(json!({
            "calendar": "en.usa#holiday@group.v.calendar.google.com",
            "from": FROM,
            "to": TO,
            "q": "lunch & co",
            "max": 7,
            "page": "page-2",
            "account": "alice@example.com",
        }))
        .unwrap();
        assert_eq!(
            call.path,
            "/calendar/v3/calendars/en.usa#holiday@group.v.calendar.google.com/events"
        );
        assert_eq!(call.account.as_deref(), Some("alice@example.com"));
        assert_eq!(
            call.query,
            events_query("7", &[("q", "lunch & co"), ("pageToken", "page-2")])
        );
    }

    #[test]
    fn events_refuse_bad_arguments() {
        for (args, refusal) in [
            (json!({ "to": TO }), "`from` is required"),
            (json!({ "from": FROM }), "`to` is required"),
            (json!({ "from": " ", "to": TO }), "`from` is empty"),
            (
                json!({ "from": FROM, "to": "x".repeat(TIME_MAX_CHARS + 1) }),
                "`to` is longer than 64 characters",
            ),
            (
                json!({ "from": FROM, "to": TO, "max": 0 }),
                "`max` must be an integer from 1 to 50",
            ),
            (
                json!({ "from": FROM, "to": TO, "max": 51 }),
                "`max` must be an integer from 1 to 50",
            ),
            (json!({ "from": FROM, "to": TO, "q": "" }), "`q` is empty"),
            (
                json!({ "from": FROM, "to": TO, "page": "x".repeat(PAGE_MAX_CHARS + 1) }),
                "`page` is longer than 4096 characters",
            ),
            (
                json!({ "from": FROM, "to": TO, "id": "e1" }),
                "unknown argument `id`; this tool takes `calendar`, `from`, `to`, `q`, `max`, \
                 `page`, `account`",
            ),
        ] {
            assert_eq!(events(args.clone()).unwrap_err(), refusal, "{args}");
        }
    }

    const EVENTS_LIST: &str = include_str!("fixtures/events-list.json");

    fn events_state(args: Value) -> InFlight {
        let call = plan(Provided::CalendarEvents, &args, &settings()).unwrap();
        InFlight::new(
            &"a".repeat(32),
            Provided::CalendarEvents,
            &args,
            &call,
            Map::new(),
            1_000,
            1_000,
        )
    }

    fn list_events(body: &str) -> Next {
        answered(
            &events_state(json!({ "from": FROM, "to": TO })),
            http(200, body),
        )
    }

    #[test]
    fn events_compact_in_googles_order_with_the_users_own_response() {
        assert_eq!(
            list_events(EVENTS_LIST),
            Next::Complete(json!({
                "events": [
                    {
                        "id": "0a1b2c3d4e5f6g7h8i9j",
                        "summary": "Planning",
                        "start": {
                            "date_time": "2026-11-02T09:00:00-05:00",
                            "time_zone": "America/New_York",
                        },
                        "end": {
                            "date_time": "2026-11-02T10:00:00-05:00",
                            "time_zone": "America/New_York",
                        },
                        "location": "Room 4 \"north\"",
                        "status": "confirmed",
                        "organizer": "alice@example.com",
                        "self_response": "accepted",
                    },
                    {
                        "id": "allday0123456789",
                        "summary": "Holiday",
                        "start": { "date": "2026-11-03" },
                        "end": { "date": "2026-11-04" },
                        "status": "confirmed",
                        "organizer": "en.usa#holiday@group.v.calendar.google.com",
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
                    {
                        "id": "offsite0123456789",
                        "summary": "Offsite",
                        "start": {
                            "date_time": "2026-11-05T09:00:00-08:00",
                            "time_zone": "America/Los_Angeles",
                        },
                        "end": {
                            "date_time": "2026-11-05T17:00:00-08:00",
                            "time_zone": "America/Los_Angeles",
                        },
                        "status": "tentative",
                        "organizer": "bob@example.com",
                    },
                ],
                "time_zone": "America/New_York",
                "next_page": "page-2",
            }))
        );
        assert_eq!(list_events("{}"), Next::Complete(json!({ "events": [] })));
        assert_eq!(
            list_events(r#"{ "items": [{ "id": "e1", "attendees": [{ "self": true }] }] }"#),
            Next::Complete(json!({ "events": [{ "id": "e1" }] }))
        );
    }

    #[test]
    fn an_events_line_cuts_and_flags_its_text_and_organizer() {
        let organizer = format!("{}@example.com", "o".repeat(ACCOUNT_MAX_BYTES));
        let body = json!({ "items": [{
            "id": "e1",
            "summary": format!("a\u{1}{}", "é".repeat(200)),
            "location": "Room 4",
            "status": "s".repeat(SHORT_MAX_BYTES + 10),
            "organizer": { "email": organizer },
            "attendees": [{ "self": true, "responseStatus": "r".repeat(SHORT_MAX_BYTES + 1) }],
        }] });
        let answer = completed(list_events(&body.to_string()));
        let line = &answer["events"][0];
        let summary = line["summary"].as_str().unwrap();
        assert!(summary.starts_with("a é"), "{summary}");
        assert!(summary.len() <= LISTING_TEXT_MAX_BYTES + '…'.len_utf8());
        assert!(line["organizer"].as_str().unwrap().ends_with('…'));
        assert_eq!(line["fields_truncated"], json!(["organizer", "summary"]));
        assert_eq!(
            line["status"],
            json!(format!("{}…", "s".repeat(SHORT_MAX_BYTES)))
        );
        assert_eq!(
            line["self_response"],
            json!(format!("{}…", "r".repeat(SHORT_MAX_BYTES)))
        );
    }

    #[test]
    fn an_events_lines_organizer_is_cut_in_bytes() {
        // 160 two-byte characters and a 4-byte domain: 164 characters, 324
        // bytes. Under the argument's 320-character bound, over the answer's
        // 320-byte one.
        let organizer = format!("{}@x.y", "é".repeat(160));
        assert!(organizer.chars().count() <= ACCOUNT_MAX_CHARS);
        let body = json!({ "items": [{ "id": "e1", "organizer": { "email": organizer } }] });
        let answer = completed(list_events(&body.to_string()));
        let line = &answer["events"][0];
        let cut_email = line["organizer"].as_str().unwrap();
        assert_eq!(cut_email, format!("{}…", "é".repeat(ACCOUNT_MAX_BYTES / 2)));
        assert_eq!(line["fields_truncated"], json!(["organizer"]));
    }

    #[test]
    fn a_listed_calendar_id_is_bounded_in_bytes() {
        let id = wide(CALENDAR_ID_MAX_BYTES / 2 + 1);
        assert!(is_calendar_id(&id), "an argument of 161 characters");
        assert!(!is_listed_calendar_id(&id), "an answer of 322 bytes");
        assert!(is_listed_calendar_id(&wide(CALENDAR_ID_MAX_BYTES / 2)));
        for bad in ["", ".", "..", "a/b", "a b"] {
            assert!(!is_listed_calendar_id(bad), "{bad:?}");
        }
        let body = json!({ "items": [{ "id": id }] });
        assert_eq!(
            list_calendars(&body.to_string()),
            Next::Fail(failure(
                ErrorKind::Internal,
                "unexpected Google calendarList.list response shape"
            ))
        );
    }

    #[test]
    fn events_refuse_an_unexpected_shape() {
        let shape = || {
            Next::Fail(failure(
                ErrorKind::Internal,
                "unexpected Google events.list response shape",
            ))
        };
        let event = |extra: Value| {
            let mut item = json!({ "id": "e1" });
            item.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            json!({ "items": [item] })
        };
        for body in [
            json!([]),
            json!({ "items": {} }),
            json!({ "items": [1] }),
            json!({ "items": [{}] }),
            json!({ "items": [{ "id": "e.1" }] }),
            json!({ "items": [{ "id": "e".repeat(EVENT_ID_MAX_CHARS + 1) }] }),
            event(json!({ "summary": 1 })),
            event(json!({ "start": "2026-11-02" })),
            event(json!({ "start": {} })),
            event(json!({ "start": { "timeZone": "UTC" } })),
            event(json!({ "start": { "date": "2026-11-02", "dateTime": "2026-11-02T00:00:00Z" } })),
            event(json!({ "end": { "date": 20261102 } })),
            event(json!({ "organizer": "bob@example.com" })),
            event(json!({ "organizer": { "email": 1 } })),
            event(json!({ "attendees": {} })),
            event(json!({ "attendees": ["alice@example.com"] })),
            event(json!({ "attendees": [{ "self": "yes" }] })),
            event(json!({ "attendees": [{ "self": true, "responseStatus": 1 }] })),
            event(json!({ "attendees": [{ "self": true }, { "self": true }] })),
            json!({ "items": [], "timeZone": 5 }),
            json!({ "items": [], "nextPageToken": "" }),
        ] {
            assert_eq!(list_events(&body.to_string()), shape(), "{body}");
        }
    }

    #[test]
    fn events_refuse_more_events_than_they_asked_for() {
        let items: Vec<Value> = (0..=7).map(|i| json!({ "id": format!("e{i}") })).collect();
        let state = events_state(json!({ "from": FROM, "to": TO, "max": 7 }));
        assert_eq!(
            answered(&state, http(200, &json!({ "items": items }).to_string())),
            Next::Fail(failure(
                ErrorKind::Internal,
                "Google events.list answered 8 events for maxResults 7"
            ))
        );
    }

    #[test]
    fn events_relay_googles_errors() {
        let state = events_state(json!({ "from": "monday", "to": TO }));
        assert_eq!(
            answered(
                &state,
                http(400, &google(400, "INVALID_ARGUMENT", "Bad Request"))
            ),
            Next::Fail(failure(
                ErrorKind::InvalidArgs,
                "Google INVALID_ARGUMENT (HTTP 400): Bad Request"
            ))
        );
    }

    #[test]
    fn the_widest_events_page_fits_the_ceiling() {
        // Every string at its bound and as wide as it serializes.
        let quotes = |n: usize| "\"".repeat(n);
        let when = || json!({ "dateTime": quotes(SHORT_MAX_BYTES + 1), "timeZone": quotes(SHORT_MAX_BYTES + 1) });
        let items: Vec<Value> = (0..EVENTS_MAX_MAX)
            .map(|i| {
                json!({
                    "id": format!("{i:02}{}", "a".repeat(EVENT_ID_MAX_CHARS - 2)),
                    "summary": quotes(LISTING_TEXT_MAX_BYTES + 1),
                    "location": quotes(LISTING_TEXT_MAX_BYTES + 1),
                    "status": quotes(SHORT_MAX_BYTES + 1),
                    "start": when(),
                    "end": when(),
                    "organizer": { "email": quotes(ACCOUNT_MAX_BYTES + 1) },
                    "attendees": [{ "self": true, "responseStatus": quotes(SHORT_MAX_BYTES + 1) }],
                })
            })
            .collect();
        let body = json!({
            "items": items,
            "timeZone": quotes(SHORT_MAX_BYTES + 1),
            "nextPageToken": quotes(PAGE_MAX_BYTES),
        });
        let answer = completed(list_events(&body.to_string()));
        assert_eq!(
            answer["events"].as_array().unwrap().len(),
            usize::try_from(EVENTS_MAX_MAX).unwrap()
        );
        assert_eq!(
            answer["events"][0]["fields_truncated"],
            json!(["location", "organizer", "summary"])
        );
        let size = serialized_len(&answer);
        assert!(size < 210_000, "{size}");
        assert!(size < COMPACT_RESULT_CEILING);
    }

    fn event(args: Value) -> Result<ApiCall, String> {
        plan(Provided::CalendarEvent, &args, &settings()).map(|call| {
            assert_eq!(call.step, 1);
            call.api
        })
    }

    fn event_query() -> Query {
        query([
            ("maxAttendees", "50".to_string()),
            (
                "fields",
                "id,status,summary,description,location,start,end,\
                 organizer(email,displayName,self),\
                 attendees(email,displayName,responseStatus,optional,self),\
                 attendeesOmitted,recurrence,recurringEventId,hangoutLink,\
                 conferenceData/entryPoints(entryPointType,uri)"
                    .to_string(),
            ),
            ("prettyPrint", "false".to_string()),
        ])
    }

    #[test]
    fn event_reads_one_event_of_primary_on_the_calendar_host() {
        assert_eq!(
            event(json!({ "id": "e1" })).unwrap(),
            ApiCall {
                provider: "google".to_string(),
                account: None,
                method: "GET".to_string(),
                host: "calendar.example.com".to_string(),
                path: "/calendar/v3/calendars/primary/events/e1".to_string(),
                query: event_query(),
                headers: BTreeMap::new(),
                body: None,
            }
        );
        let call = event(json!({
            "id": "weekly0123456789_20261104T140000Z",
            "calendar": "en.usa#holiday@group.v.calendar.google.com",
            "max_chars": 200,
            "account": "alice@example.com",
        }))
        .unwrap();
        assert_eq!(
            call.path,
            "/calendar/v3/calendars/en.usa#holiday@group.v.calendar.google.com/events/\
             weekly0123456789_20261104T140000Z"
        );
        assert_eq!(call.account.as_deref(), Some("alice@example.com"));
        assert_eq!(call.query, event_query());
    }

    #[test]
    fn event_refuses_bad_arguments() {
        let bad_id = "`id` must be 1 to 1024 letters, digits, `_` or `-`";
        for (args, refusal) in [
            (json!({}), "`id` is required"),
            (json!({ "id": "" }), bad_id),
            (json!({ "id": "e.1" }), bad_id),
            (json!({ "id": "e/1" }), bad_id),
            (json!({ "id": "e".repeat(EVENT_ID_MAX_CHARS + 1) }), bad_id),
            (json!({ "id": 1 }), bad_id),
            (
                json!({ "id": "e1", "calendar": "a/b" }),
                "`calendar` must be a calendar id of 1 to 320 characters with no `/`, `\\`, `%`, \
                 `\"`, space or control character",
            ),
            (
                json!({ "id": "e1", "max_chars": 199 }),
                "`max_chars` must be an integer from 200 to 60000",
            ),
            (
                json!({ "id": "e1", "max_chars": 60_001 }),
                "`max_chars` must be an integer from 200 to 60000",
            ),
            (
                json!({ "id": "e1", "from": "x" }),
                "unknown argument `from`; this tool takes `id`, `calendar`, `max_chars`, `account`",
            ),
        ] {
            assert_eq!(event(args.clone()).unwrap_err(), refusal, "{args}");
        }
    }

    fn event_state(args: Value) -> InFlight {
        let call = plan(Provided::CalendarEvent, &args, &settings()).unwrap();
        InFlight::new(
            &"a".repeat(32),
            Provided::CalendarEvent,
            &args,
            &call,
            Map::new(),
            1_000,
            1_000,
        )
    }

    fn read_event(body: &str) -> Next {
        answered(&event_state(json!({ "id": EVENT_ID })), http(200, body))
    }

    #[test]
    fn event_compacts_with_its_attendees_and_their_responses() {
        let description = "<p>Agenda:</p>\n<ul><li>roadmap</li><li>hiring &amp; budget</li></ul>";
        assert_eq!(
            read_event(EVENT),
            Next::Complete(json!({
                "id": EVENT_ID,
                "summary": "Weekly sync",
                "description": description,
                "description_chars": description.chars().count(),
                "start": {
                    "date_time": "2026-11-04T09:00:00-05:00",
                    "time_zone": "America/New_York",
                },
                "end": {
                    "date_time": "2026-11-04T09:30:00-05:00",
                    "time_zone": "America/New_York",
                },
                "location": "Room 4 \"north\"",
                "status": "confirmed",
                "organizer": { "email": "bob@example.com", "name": "Bob Example" },
                "self_response": "tentative",
                "attendees": [
                    { "email": "bob@example.com", "name": "Bob Example", "response": "accepted" },
                    { "email": "alice@example.com", "response": "tentative" },
                    {
                        "email": "carol@example.com",
                        "name": "Carol",
                        "response": "declined",
                        "optional": true,
                    },
                    { "email": "dave@example.com", "response": "needsAction" },
                ],
                "attendees_total": 4,
                "attendee_responses": {
                    "accepted": 1,
                    "declined": 1,
                    "tentative": 1,
                    "needs_action": 1,
                },
                "recurring_event_id": "weekly0123456789",
                "conference": "https://meet.example.com/abc-defg-hij",
            }))
        );
    }

    #[test]
    fn an_event_with_attendees_left_out_answers_the_users_own_entry_and_no_counts() {
        let answer = completed(read_event(&event_with(json!({
            "attendees": [{ "email": "alice@example.com", "self": true, "responseStatus": "accepted" }],
            "attendeesOmitted": true,
        }))));
        assert_eq!(
            answer["attendees"],
            json!([{ "email": "alice@example.com", "response": "accepted" }])
        );
        assert_eq!(answer["self_response"], json!("accepted"));
        assert_eq!(answer["attendees_omitted"], json!(true));
        assert!(answer.get("attendees_total").is_none(), "{answer}");
        assert!(answer.get("attendee_responses").is_none(), "{answer}");

        // Left out with no entry of the user's: nothing listed, nothing counted.
        let answer = completed(read_event(&event_with(
            json!({ "attendees": null, "attendeesOmitted": true }),
        )));
        assert_eq!(answer["attendees"], json!([]));
        assert_eq!(answer["attendees_omitted"], json!(true));
        assert!(answer.get("self_response").is_none(), "{answer}");

        // No guests at all: an empty list, counted.
        let answer = completed(read_event(&event_with(json!({ "attendees": null }))));
        assert_eq!(answer["attendees"], json!([]));
        assert_eq!(answer["attendees_total"], json!(0));
        assert_eq!(
            answer["attendee_responses"],
            json!({ "accepted": 0, "declined": 0, "tentative": 0, "needs_action": 0 })
        );
    }

    #[test]
    fn an_event_answers_its_recurrence_and_conference_and_all_day_times() {
        let rules: Vec<String> = (0..=RECURRENCE_MAX)
            .map(|i| format!("EXDATE:202611{i:02}"))
            .collect();
        let answer = completed(read_event(&event_with(json!({
            "start": { "date": "2026-11-04" },
            "end": { "date": "2026-11-05" },
            "recurrence": rules,
            "recurringEventId": null,
            "hangoutLink": null,
            "conferenceData": { "entryPoints": [
                { "entryPointType": "phone", "uri": "tel:+1-555-0100" },
                { "entryPointType": "video", "uri": "https://video.example.com/j/1" },
                { "entryPointType": "video", "uri": "https://video.example.com/j/2" },
            ] },
        }))));
        assert_eq!(answer["start"], json!({ "date": "2026-11-04" }));
        assert_eq!(answer["end"], json!({ "date": "2026-11-05" }));
        assert_eq!(answer["recurrence"], json!(rules[..RECURRENCE_MAX]));
        assert_eq!(answer["recurrence_truncated"], json!(true));
        assert!(answer.get("recurring_event_id").is_none(), "{answer}");
        assert_eq!(answer["conference"], json!("https://video.example.com/j/1"));

        let answer = completed(read_event(&event_with(json!({
            "recurrence": ["RRULE:FREQ=WEEKLY"],
            "hangoutLink": null,
            "conferenceData": { "entryPoints": [{ "entryPointType": "phone", "uri": "tel:1" }] },
        }))));
        assert_eq!(answer["recurrence"], json!(["RRULE:FREQ=WEEKLY"]));
        assert!(answer.get("recurrence_truncated").is_none(), "{answer}");
        assert!(answer.get("conference").is_none(), "{answer}");

        // A Meet link wins over a video entry point of the conference data.
        let answer = completed(read_event(&event_with(json!({
            "hangoutLink": "https://meet.example.com/abc",
            "conferenceData": { "entryPoints": [
                { "entryPointType": "video", "uri": "https://video.example.com/j/1" },
            ] },
        }))));
        assert_eq!(answer["conference"], json!("https://meet.example.com/abc"));
    }

    #[test]
    fn an_event_cuts_and_flags_its_text_and_emails() {
        let long = format!("a\u{1}{}", "é".repeat(EVENT_TEXT_MAX_BYTES));
        let email = format!("{}@example.com", "o".repeat(ACCOUNT_MAX_BYTES));
        let answer = completed(read_event(&event_with(json!({
            "summary": long,
            "location": long,
            "status": "s".repeat(SHORT_MAX_BYTES + 1),
            "organizer": { "email": email, "displayName": long, "self": true },
            "attendees": [{
                "email": email,
                "displayName": long,
                "self": true,
                "responseStatus": "accepted",
            }],
            "recurrence": [long, "RRULE:FREQ=DAILY"],
            "hangoutLink": long,
        }))));
        let summary = answer["summary"].as_str().unwrap();
        assert!(summary.starts_with("a é"), "{summary}");
        assert!(summary.len() <= EVENT_TEXT_MAX_BYTES + '…'.len_utf8());
        let location = answer["location"].as_str().unwrap();
        assert!(location.ends_with('…'), "{location}");
        assert!(location.len() <= EVENT_TEXT_MAX_BYTES + '…'.len_utf8());
        assert!(location.len() > LISTING_TEXT_MAX_BYTES + '…'.len_utf8());
        assert_eq!(
            answer["fields_truncated"],
            json!(["conference", "location", "recurrence", "summary"])
        );
        assert_eq!(answer["recurrence"][1], json!("RRULE:FREQ=DAILY"));
        assert_eq!(
            answer["status"],
            json!(format!("{}…", "s".repeat(SHORT_MAX_BYTES)))
        );
        let organizer = &answer["organizer"];
        assert_eq!(organizer["self"], json!(true));
        assert_eq!(organizer["fields_truncated"], json!(["email", "name"]));
        assert!(organizer["email"].as_str().unwrap().ends_with('…'));
        let attendee = &answer["attendees"][0];
        assert_eq!(attendee["fields_truncated"], json!(["email", "name"]));
        let name = attendee["name"].as_str().unwrap();
        assert!(name.len() <= LISTING_TEXT_MAX_BYTES + '…'.len_utf8());
        assert!(attendee.get("self").is_none(), "{attendee}");
    }

    #[test]
    fn an_event_description_is_cut_at_max_chars_and_not_decoded() {
        let description = format!("a&amp;\u{1}{}", "é".repeat(300));
        let body = event_with(json!({ "description": description }));
        let state = event_state(json!({ "id": EVENT_ID, "max_chars": 200 }));
        let answer = completed(answered(&state, http(200, &body)));
        assert_eq!(
            answer["description"],
            json!(format!("a&amp; {}", "é".repeat(193)))
        );
        assert_eq!(answer["description_truncated"], json!(true));
        assert_eq!(answer["description_chars"], json!(307));

        let answer = completed(read_event(&event_with(json!({ "description": null }))));
        assert!(answer.get("description").is_none(), "{answer}");
        assert!(answer.get("description_chars").is_none(), "{answer}");
    }

    #[test]
    fn an_event_refuses_an_unexpected_shape() {
        let shape = || {
            Next::Fail(failure(
                ErrorKind::Internal,
                "unexpected Google events.get response shape",
            ))
        };
        let attendee = |n: usize| {
            (0..n)
                .map(|i| json!({ "email": format!("a{i}@example.com"), "responseStatus": "accepted" }))
                .collect::<Vec<_>>()
        };
        for body in [
            "[]".to_string(),
            event_with(json!({ "id": "e1" })),
            event_with(json!({ "id": null })),
            event_with(json!({ "recurringEventId": "weekly.1" })),
            event_with(json!({ "recurringEventId": "w".repeat(EVENT_ID_MAX_CHARS + 1) })),
            event_with(json!({ "description": 1 })),
            event_with(json!({ "start": { "timeZone": "UTC" } })),
            event_with(json!({ "organizer": "bob@example.com" })),
            event_with(json!({ "organizer": { "self": "yes" } })),
            event_with(json!({ "attendees": attendee(ATTENDEES_MAX + 1) })),
            event_with(
                json!({ "attendees": [{ "email": "a@example.com", "responseStatus": "maybe" }] }),
            ),
            event_with(json!({ "attendees": [{ "email": "a@example.com" }] })),
            event_with(json!({ "attendees": [{ "email": 1, "responseStatus": "accepted" }] })),
            event_with(
                json!({ "attendees": [{ "optional": "no", "responseStatus": "accepted" }] }),
            ),
            event_with(json!({ "attendeesOmitted": "true" })),
            event_with(json!({ "recurrence": "RRULE:FREQ=DAILY" })),
            event_with(json!({ "recurrence": [1] })),
            event_with(json!({ "hangoutLink": 1 })),
            event_with(json!({ "hangoutLink": null, "conferenceData": [] })),
            event_with(
                json!({ "hangoutLink": null, "conferenceData": { "entryPoints": [{ "entryPointType": "video", "uri": 1 }] } }),
            ),
        ] {
            assert_eq!(read_event(&body), shape(), "{body}");
        }
        // Exactly the bound is Google's own list, and an unknown response is
        // not counted against when the list is not being counted.
        let answer = completed(read_event(&event_with(
            json!({ "attendees": attendee(ATTENDEES_MAX) }),
        )));
        assert_eq!(answer["attendees_total"], json!(ATTENDEES_MAX));
        let answer = completed(read_event(&event_with(json!({
            "attendees": [{ "email": "a@example.com", "self": true, "responseStatus": "maybe" }],
            "attendeesOmitted": true,
        }))));
        assert_eq!(answer["self_response"], json!("maybe"));
    }

    #[test]
    fn event_relays_googles_errors_with_the_calendar_hint() {
        let state = event_state(json!({ "id": "e1" }));
        assert_eq!(
            answered(&state, http(404, &google(404, "NOT_FOUND", "Not Found"))),
            Next::Fail(failure(
                ErrorKind::InvalidArgs,
                "Google NOT_FOUND (HTTP 404): Not Found"
            ))
        );
        let Next::Fail(Failure { kind, detail }) = answered(
            &state,
            http(
                403,
                &google(403, "PERMISSION_DENIED", "Insufficient Permission"),
            ),
        ) else {
            panic!("a 403 did not fail");
        };
        assert_eq!(kind, ErrorKind::Denied);
        assert!(
            detail.ends_with(&scope_hint(Scope::CalendarRead)),
            "{detail}"
        );
    }

    #[test]
    fn the_widest_event_fits_the_ceiling_with_its_description_fitted() {
        // Every string at its bound and as wide as it serializes.
        let quotes = |n: usize| "\"".repeat(n);
        let id = "a".repeat(EVENT_ID_MAX_CHARS);
        let when = || json!({ "dateTime": quotes(SHORT_MAX_BYTES + 1), "timeZone": quotes(SHORT_MAX_BYTES + 1) });
        let attendees: Vec<Value> = (0..ATTENDEES_MAX)
            .map(|_| {
                json!({
                    "email": quotes(ACCOUNT_MAX_BYTES + 1),
                    "displayName": quotes(LISTING_TEXT_MAX_BYTES + 1),
                    "responseStatus": "needsAction",
                    "optional": true,
                })
            })
            .collect();
        let body = json!({
            "id": id,
            "summary": quotes(EVENT_TEXT_MAX_BYTES + 1),
            // Four bytes a character, the widest a character serializes
            // without escaping, so `max_chars` of them pass the ceiling.
            "description": "😀".repeat(MAX_CHARS_MAX as usize + 1),
            "location": quotes(EVENT_TEXT_MAX_BYTES + 1),
            "status": quotes(SHORT_MAX_BYTES + 1),
            "start": when(),
            "end": when(),
            "organizer": {
                "email": quotes(ACCOUNT_MAX_BYTES + 1),
                "displayName": quotes(EVENT_TEXT_MAX_BYTES + 1),
                "self": true,
            },
            "attendees": attendees,
            "recurrence": vec![quotes(EVENT_TEXT_MAX_BYTES + 1); RECURRENCE_MAX + 1],
            "recurringEventId": id,
            "hangoutLink": quotes(EVENT_TEXT_MAX_BYTES + 1),
        });
        let state = event_state(json!({ "id": id, "max_chars": MAX_CHARS_MAX }));
        let answer = completed(answered(&state, http(200, &body.to_string())));
        let mut without = answer.clone();
        without.as_object_mut().unwrap().remove("description");
        let rest = serialized_len(&without);
        assert!(rest < 126_000, "{rest}");
        // The description is fitted into what is left, never emptied.
        let kept = answer["description"].as_str().unwrap().chars().count();
        assert!(kept > 30_000 && kept < MAX_CHARS_MAX as usize, "{kept}");
        assert_eq!(answer["description_truncated"], json!(true));
        assert_eq!(answer["description_chars"], json!(MAX_CHARS_MAX + 1));
        let size = serialized_len(&answer);
        assert!(
            size <= COMPACT_RESULT_CEILING && size > COMPACT_RESULT_CEILING - 4,
            "{size}"
        );
    }

    fn free_busy(args: Value) -> Result<ApiCall, String> {
        plan(Provided::CalendarFreeBusy, &args, &settings()).map(|call| {
            assert_eq!(call.step, 1);
            call.api
        })
    }

    /// The JSON body a free/busy call carries.
    fn free_busy_body(call: &ApiCall) -> Value {
        serde_json::from_str(text_body(call)).unwrap()
    }

    #[test]
    fn free_busy_posts_one_query_for_primary_on_the_calendar_host() {
        let call = free_busy(json!({ "from": FROM, "to": TO })).unwrap();
        assert_eq!(
            call,
            ApiCall {
                provider: "google".to_string(),
                account: None,
                method: "POST".to_string(),
                host: "calendar.example.com".to_string(),
                path: "/calendar/v3/freeBusy".to_string(),
                query: query([("prettyPrint", "false".to_string())]),
                headers: BTreeMap::new(),
                body: call.body.clone(),
            }
        );
        assert_eq!(
            free_busy_body(&call),
            json!({
                "timeMin": FROM,
                "timeMax": TO,
                "items": [{ "id": "primary" }],
                "calendarExpansionMax": 1,
            })
        );
        assert_eq!(
            serde_json::to_value(&call).unwrap()["body"],
            json!(text_body(&call))
        );

        let holidays = "en.usa#holiday@group.v.calendar.google.com";
        let call = free_busy(json!({
            "from": FROM,
            "to": TO,
            "calendars": ["primary", holidays],
            "account": "alice@example.com",
        }))
        .unwrap();
        assert_eq!(call.account.as_deref(), Some("alice@example.com"));
        assert_eq!(
            free_busy_body(&call),
            json!({
                "timeMin": FROM,
                "timeMax": TO,
                "items": [{ "id": "primary" }, { "id": holidays }],
                "calendarExpansionMax": 2,
            })
        );
        // A GET carries no body, and `api-request` is not sent one.
        let get = event(json!({ "id": "e1" })).unwrap();
        assert!(serde_json::to_value(&get).unwrap().get("body").is_none());
    }

    #[test]
    fn free_busy_refuses_bad_arguments() {
        let window = |extra: Value| {
            let mut args = json!({ "from": FROM, "to": TO });
            args.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            args
        };
        let count = "`calendars` must be an array of 1 to 50 calendar ids";
        let bad_id = "each of `calendars` must be a calendar id of 1 to 320 characters with no `/`, \
                      `\\`, `%`, `\"`, space or control character";
        for (args, refusal) in [
            (json!({ "to": TO }), "`from` is required".to_string()),
            (json!({ "from": FROM }), "`to` is required".to_string()),
            (
                json!({ "from": " ", "to": TO }),
                "`from` is empty".to_string(),
            ),
            (
                json!({ "from": FROM, "to": "t".repeat(TIME_MAX_CHARS + 1) }),
                "`to` is longer than 64 characters".to_string(),
            ),
            (window(json!({ "calendars": [] })), count.to_string()),
            (
                window(json!({ "calendars": vec!["c"; FREEBUSY_CALENDARS_MAX + 1] })),
                count.to_string(),
            ),
            (window(json!({ "calendars": "primary" })), count.to_string()),
            (window(json!({ "calendars": ["a/b"] })), bad_id.to_string()),
            (window(json!({ "calendars": ["a b"] })), bad_id.to_string()),
            (
                window(json!({ "calendars": ["c".repeat(CALENDAR_ID_MAX_CHARS + 1)] })),
                bad_id.to_string(),
            ),
            (window(json!({ "calendars": [1] })), bad_id.to_string()),
            (
                window(json!({ "calendars": ["primary", "bob@example.com", "primary"] })),
                "`calendars` names \"primary\" twice".to_string(),
            ),
            (
                window(json!({ "max": 5 })),
                "unknown argument `max`; this tool takes `from`, `to`, `calendars`, `account`"
                    .to_string(),
            ),
        ] {
            assert_eq!(free_busy(args.clone()).unwrap_err(), refusal, "{args}");
        }
        let fifty: Vec<String> = (0..FREEBUSY_CALENDARS_MAX)
            .map(|i| format!("c{i}"))
            .collect();
        let call = free_busy(window(json!({ "calendars": fifty }))).unwrap();
        assert_eq!(free_busy_body(&call)["calendarExpansionMax"], json!(50));
    }

    #[test]
    fn the_widest_free_busy_call_fits_the_args_cap() {
        let with = |calendars: Vec<String>| {
            free_busy(json!({
                "from": escaped_widest(TIME_MAX_CHARS),
                "to": escaped_widest(TIME_MAX_CHARS),
                "calendars": calendars,
                "account": escaped_widest(ACCOUNT_MAX_CHARS),
            }))
        };
        let ids = |chars: usize| -> Vec<String> {
            (0..FREEBUSY_CALENDARS_MAX)
                .map(|i| four_byte(i, chars))
                .collect()
        };
        let refusal = "`calendars` together exceed 48000 bytes (UTF-8); ask for fewer";
        assert_eq!(
            with(ids(CALENDAR_ID_MAX_CHARS)).unwrap_err(),
            refusal,
            "fifty maximal ids of four-byte characters"
        );

        let at_budget = ids(FREEBUSY_IDS_MAX_BYTES / FREEBUSY_CALENDARS_MAX / 4);
        assert_eq!(
            at_budget.iter().map(String::len).sum::<usize>(),
            FREEBUSY_IDS_MAX_BYTES
        );
        let call = with(at_budget.clone()).unwrap();
        let args = serde_json::to_vec(&call).unwrap();
        assert!(
            args.len() <= HOST_ARGS_CAP_BYTES,
            "{} bytes of args",
            args.len()
        );

        let mut over = at_budget;
        over[0].push('x');
        assert_eq!(with(over).unwrap_err(), refusal, "one byte over");
    }

    const FREE_BUSY: &str = include_str!("fixtures/free-busy.json");
    const FREE_BUSY_CALENDARS: [&str; 3] = [
        "primary",
        "en.usa#holiday@group.v.calendar.google.com",
        "bob@example.com",
    ];

    fn free_busy_state(args: Value) -> InFlight {
        let call = plan(Provided::CalendarFreeBusy, &args, &settings()).unwrap();
        InFlight::new(
            &"a".repeat(32),
            Provided::CalendarFreeBusy,
            &args,
            &call,
            Map::new(),
            1_000,
            1_000,
        )
    }

    /// The fixture's three calendars asked, answered `body`.
    fn read_free_busy(body: &str) -> Next {
        let state = free_busy_state(json!({
            "from": FROM,
            "to": TO,
            "calendars": FREE_BUSY_CALENDARS,
        }));
        answered(&state, http(200, body))
    }

    /// The fixture with its `calendars` entry `id` replaced by `entry`, or
    /// removed when `entry` is null.
    fn free_busy_with(id: &str, entry: Value) -> String {
        let mut body: Value = serde_json::from_str(FREE_BUSY).unwrap();
        let calendars = body["calendars"].as_object_mut().unwrap();
        if entry.is_null() {
            calendars.remove(id);
        } else {
            calendars.insert(id.to_string(), entry);
        }
        body.to_string()
    }

    #[test]
    fn free_busy_answers_each_calendar_asked() {
        assert_eq!(
            completed(read_free_busy(FREE_BUSY)),
            json!({
                "from": "2026-11-03T05:00:00.000Z",
                "to": "2026-11-04T05:00:00.000Z",
                "calendars": {
                    "primary": { "busy": [
                        { "start": "2026-11-03T14:00:00Z", "end": "2026-11-03T15:00:00Z" },
                        { "start": "2026-11-03T18:30:00Z", "end": "2026-11-03T19:00:00Z" },
                    ] },
                    // Free all window: an empty list, not an absent one.
                    "en.usa#holiday@group.v.calendar.google.com": { "busy": [] },
                    // Unknown, not free: Google's empty `busy` is left out.
                    "bob@example.com": { "errors": ["notFound"] },
                },
            })
        );
        // Busy intervals beside an error are kept.
        let partial = free_busy_with(
            "bob@example.com",
            json!({
                "errors": [{ "domain": "global", "reason": "tooManyCalendarsRequested" }],
                "busy": [{ "start": "2026-11-03T16:00:00Z", "end": "2026-11-03T17:00:00Z" }],
            }),
        );
        assert_eq!(
            completed(read_free_busy(&partial))["calendars"]["bob@example.com"],
            json!({
                "busy": [{ "start": "2026-11-03T16:00:00Z", "end": "2026-11-03T17:00:00Z" }],
                "errors": ["tooManyCalendarsRequested"],
            })
        );
    }

    #[test]
    fn free_busy_vocabulary_is_plain_and_cut_unflagged() {
        let long = format!("2026-11-03T14:00:00Z\u{7}{}", "9".repeat(SHORT_MAX_BYTES));
        let body = free_busy_with(
            "primary",
            json!({ "busy": [{ "start": long, "end": "2026-11-03T15:00:00Z" }] }),
        );
        let answer = completed(read_free_busy(&body));
        let start = answer["calendars"]["primary"]["busy"][0]["start"]
            .as_str()
            .unwrap();
        // Cut at the bound, `…` after it.
        assert_eq!(start.len(), SHORT_MAX_BYTES + '…'.len_utf8());
        assert!(
            start.starts_with("2026-11-03T14:00:00Z ") && start.ends_with('…'),
            "{start}"
        );
        assert!(
            answer["calendars"]["primary"]["busy"][0]
                .get("fields_truncated")
                .is_none()
        );
        let body = free_busy_with(
            "bob@example.com",
            json!({ "errors": [{ "domain": "global", "reason": "r".repeat(SHORT_MAX_BYTES + 1) }] }),
        );
        let answer = completed(read_free_busy(&body));
        assert_eq!(
            answer["calendars"]["bob@example.com"]["errors"][0]
                .as_str()
                .unwrap()
                .len(),
            SHORT_MAX_BYTES + '…'.len_utf8()
        );
    }

    #[test]
    fn free_busy_refuses_an_unexpected_shape() {
        let mut no_calendars: Value = serde_json::from_str(FREE_BUSY).unwrap();
        no_calendars.as_object_mut().unwrap().remove("calendars");
        for body in [
            "[]".to_string(),
            no_calendars.to_string(),
            json!({ "calendars": [] }).to_string(),
            // A calendar that says nothing, or is not of Google's shape.
            free_busy_with("primary", json!({})),
            free_busy_with("primary", json!([])),
            free_busy_with("primary", json!({ "busy": {} })),
            free_busy_with(
                "primary",
                json!({ "busy": [{ "start": "2026-11-03T14:00:00Z" }] }),
            ),
            free_busy_with("primary", json!({ "busy": [{ "start": 1, "end": 2 }] })),
            free_busy_with("primary", json!({ "busy": ["2026-11-03T14:00:00Z"] })),
            free_busy_with("primary", json!({ "errors": [{ "domain": "global" }] })),
            free_busy_with("primary", json!({ "errors": "notFound" })),
            free_busy_with("primary", json!({ "errors": [], "busy": "x" })),
            json!({ "timeMin": 1, "calendars": {} }).to_string(),
        ] {
            assert_eq!(
                read_free_busy(&body),
                Next::Fail(failure(
                    ErrorKind::Internal,
                    "unexpected Google freebusy.query response shape"
                )),
                "{body}"
            );
        }
    }

    #[test]
    fn free_busy_names_the_calendars_asked_and_answered_apart() {
        let shape = "unexpected Google freebusy.query response shape";
        let mut stranger: Value = serde_json::from_str(FREE_BUSY).unwrap();
        stranger["calendars"]["carol@example.com"] = json!({ "busy": [] });
        let mut members: Value = serde_json::from_str(FREE_BUSY).unwrap();
        let calendars = members["calendars"].as_object_mut().unwrap();
        calendars.remove("bob@example.com");
        for n in 0..7 {
            calendars.insert(format!("member{n}@example.com"), json!({ "busy": [] }));
        }
        for (body, detail) in [
            (
                free_busy_with("primary", Value::Null),
                format!(
                    "{shape}: calendars answered but not asked: none; calendars asked but not \
                     answered: \"primary\""
                ),
            ),
            (
                stranger.to_string(),
                format!(
                    "{shape}: calendars answered but not asked: \"carol@example.com\"; calendars \
                     asked but not answered: none"
                ),
            ),
            (
                members.to_string(),
                format!(
                    "{shape}: calendars answered but not asked: \"member0@example.com\", \
                     \"member1@example.com\", \"member2@example.com\", \"member3@example.com\", \
                     \"member4@example.com\" and 2 more; calendars asked but not answered: \
                     \"bob@example.com\""
                ),
            ),
        ] {
            assert_eq!(
                read_free_busy(&body),
                Next::Fail(failure(ErrorKind::Internal, &detail)),
                "{body}"
            );
        }
    }

    #[test]
    fn free_busy_relays_googles_errors_with_the_calendar_hint() {
        let state = free_busy_state(json!({ "from": "monday", "to": TO }));
        assert_eq!(
            answered(
                &state,
                http(400, &google(400, "INVALID_ARGUMENT", "Bad Request"))
            ),
            Next::Fail(failure(
                ErrorKind::InvalidArgs,
                "Google INVALID_ARGUMENT (HTTP 400): Bad Request"
            ))
        );
        let Next::Fail(Failure { kind, detail }) = answered(
            &state,
            http(
                403,
                &google(403, "PERMISSION_DENIED", "Insufficient Permission"),
            ),
        ) else {
            panic!("a 403 did not fail");
        };
        assert_eq!(kind, ErrorKind::Denied);
        assert!(
            detail.ends_with(&scope_hint(Scope::CalendarRead)),
            "{detail}"
        );
    }

    /// A free/busy body whose answer, once compacted, serializes to exactly
    /// `size` bytes: `primary` busy with as many intervals as fit, the first
    /// ones' `end` padded (within `SHORT_MAX_BYTES`) to land on it.
    fn free_busy_answering(size: usize) -> String {
        let interval = || json!({ "start": "2026-11-03T14:00:00Z", "end": "2026-11-03T15:00:00Z" });
        let body = |busy: &[Value]| json!({ "timeMin": "f", "timeMax": "t", "calendars": { "primary": { "busy": busy } } });
        let compacted = |busy: &[Value]| {
            serialized_len(
                &json!({ "from": "f", "to": "t", "calendars": { "primary": { "busy": busy } } }),
            )
        };
        // Each interval past the first adds itself and a comma.
        let step = serialized_len(&interval()) + 1;
        let mut busy = vec![interval()];
        let mut len = compacted(&busy);
        while len + serialized_len(&interval()) < size {
            busy.push(interval());
            len += step;
        }
        let mut deficit = size - compacted(&busy);
        for item in &mut busy {
            let end = item["end"].as_str().unwrap().to_string();
            let pad = deficit.min(SHORT_MAX_BYTES - end.len());
            item["end"] = json!(format!("{end}{}", "0".repeat(pad)));
            deficit -= pad;
        }
        assert_eq!(deficit, 0);
        body(&busy).to_string()
    }

    #[test]
    fn free_busy_past_the_ceiling_is_refused_with_what_to_narrow() {
        let state = free_busy_state(json!({ "from": FROM, "to": TO }));
        for size in [COMPACT_RESULT_CEILING - 1, COMPACT_RESULT_CEILING] {
            let answer = completed(answered(&state, http(200, &free_busy_answering(size))));
            assert_eq!(serialized_len(&answer), size);
        }
        let over = COMPACT_RESULT_CEILING + 1;
        assert_eq!(
            answered(&state, http(200, &free_busy_answering(over))),
            Next::Fail(failure(
                ErrorKind::Internal,
                &format!(
                    "compact result of {over} bytes exceeds the wrapper's ceiling; ask for \
                     a shorter `from`/`to` window, or fewer `calendars`"
                )
            ))
        );
    }

    #[test]
    fn a_row_names_every_provided_tool_by_its_registry_name() {
        let state = calendar_list_state(json!({}));
        for tool in Provided::ALL {
            let row = InFlight {
                tool,
                ..state.clone()
            };
            let wire: Value = serde_json::from_slice(&row.encode()).unwrap();
            assert_eq!(wire["tool"], json!(tool.name()));
            assert_eq!(InFlight::decode(&row.encode()).unwrap().tool, tool);
        }
        let mut wire: Value = serde_json::from_slice(&state.encode()).unwrap();
        wire["tool"] = json!("CalendarList");
        let error = InFlight::decode(&serde_json::to_vec(&wire).unwrap()).unwrap_err();
        assert!(
            error.contains("no provided tool is named `CalendarList`"),
            "{error}"
        );
    }

    /// A row of each tool at its one step, as `plan` issues it.
    fn first_reads() -> [InFlight; 4] {
        let reads = [
            calendar_list_state(json!({})),
            events_state(json!({ "from": FROM, "to": TO })),
            event_state(json!({ "id": "e1" })),
            free_busy_state(json!({ "from": FROM, "to": TO })),
        ];
        for tool in Provided::ALL {
            assert!(
                reads.iter().any(|state| state.tool == tool),
                "{} has no state here",
                tool.name()
            );
        }
        reads
    }

    #[test]
    fn every_step_names_its_scope() {
        for state in first_reads() {
            assert_eq!(state.step, 1);
            assert_eq!(
                scope(state.tool, state.step),
                Scope::CalendarRead,
                "{} step 1",
                state.tool.name()
            );
        }
    }

    #[test]
    fn argument_caps_count_characters_not_bytes() {
        admits_characters(calendar_list, json!({}), "account", ACCOUNT_MAX_CHARS);
        admits_characters(calendar_list, json!({}), "page", PAGE_MAX_CHARS);
        admits_characters(events, json!({ "to": TO }), "from", TIME_MAX_CHARS);
        admits_characters(free_busy, json!({ "from": FROM }), "to", TIME_MAX_CHARS);
        admits_characters(
            events,
            json!({ "from": FROM, "to": TO }),
            "q",
            QUERY_MAX_CHARS,
        );
        let calendar = |id: String| events(json!({ "calendar": id, "from": FROM, "to": TO }));
        assert!(calendar(wide(CALENDAR_ID_MAX_CHARS)).is_ok());
        assert!(calendar(wide(CALENDAR_ID_MAX_CHARS + 1)).is_err());
        let calendars =
            |id: String| free_busy(json!({ "from": FROM, "to": TO, "calendars": [id] }));
        assert!(calendars(wide(CALENDAR_ID_MAX_CHARS)).is_ok());
        assert!(calendars(wide(CALENDAR_ID_MAX_CHARS + 1)).is_err());
    }

    /// Every argument `tool` takes, with the bound its parser holds it to,
    /// and the arguments it requires: the parser's own constants and key
    /// lists, as `every_tools_schema_states_what_its_parser_enforces` holds
    /// the spec to them. One parser rule is outside what a schema states,
    /// leaving the schema wider than the parser, and is in the description:
    /// `calendars`' ids together hold at most `FREEBUSY_IDS_MAX_BYTES`, as
    /// `every_description_states_its_parsers_budgets` holds. The per-value
    /// probes are this component's, in `workspace_probes`.
    fn bounds(tool: Provided) -> ToolBounds<'static> {
        let (keys, required): (&'static [&'static str], &'static [&'static str]) = match tool {
            Provided::CalendarList => (CalendarListArgs::KEYS, &[]),
            Provided::CalendarEvents => (EventsArgs::KEYS, &["from", "to"]),
            Provided::CalendarEvent => (EventArgs::KEYS, &["id"]),
            Provided::CalendarFreeBusy => (FreeBusyArgs::KEYS, &["from", "to"]),
        };
        let text = |max_chars| Bound::Text {
            max_chars,
            format: None,
        };
        let integer = |min, max| Bound::Integer { min, max };
        let property = |key: &'static str| {
            let bound = match (tool, key) {
                (_, "account") => text(ACCOUNT_MAX_CHARS),
                (_, "q") => text(QUERY_MAX_CHARS),
                (_, "page") => text(PAGE_MAX_CHARS),
                (_, "from" | "to") => text(TIME_MAX_CHARS),
                (_, "calendar") => Bound::Pattern(calendar_id_pattern()),
                (_, "calendars") => Bound::Array {
                    min: 1,
                    max: FREEBUSY_CALENDARS_MAX,
                    unique: true,
                    item: Box::new(Bound::Pattern(calendar_id_pattern())),
                },
                (_, "max_chars") => integer(MAX_CHARS_MIN, MAX_CHARS_MAX),
                (Provided::CalendarEvent, "id") => Bound::Token {
                    max_chars: EVENT_ID_MAX_CHARS,
                },
                (Provided::CalendarEvents, "max") => integer(EVENTS_MAX_MIN, EVENTS_MAX_MAX),
                _ => panic!("{tool:?} takes `{key}`, which no bound here states"),
            };
            (key, bound)
        };
        ToolBounds {
            properties: keys.iter().copied().map(property).collect(),
            required,
        }
    }

    /// Arguments `tool` admits, every required one present, into which a
    /// probe of one argument is set.
    fn admitted_args(tool: Provided) -> Value {
        match tool {
            Provided::CalendarList => json!({}),
            Provided::CalendarEvent => json!({ "id": "e1" }),
            Provided::CalendarEvents | Provided::CalendarFreeBusy => {
                json!({ "from": FROM, "to": TO })
            }
        }
    }

    /// The probes of the arguments whose bounds are this component's, or
    /// whose parser holds a rule past the kit's bound: the calendar id, alone
    /// as `calendar` and as `calendars`' items. `None` where the kit's probes
    /// for the bound hold.
    fn workspace_probes(key: &str) -> Option<Probes> {
        let ids = |n: usize| json!((0..n).map(|i| format!("c{i}")).collect::<Vec<_>>());
        Some(match key {
            "calendar" => calendar_id_probes(),
            "calendars" => Probes {
                admitted: vec![ids(1), ids(FREEBUSY_CALENDARS_MAX)],
                refused: vec![
                    ids(0),
                    ids(FREEBUSY_CALENDARS_MAX + 1),
                    json!(["c0", "c0"]),
                    json!([""]),
                    json!(["a/b"]),
                    json!([1]),
                    json!("primary"),
                    // The byte budget the schema cannot state.
                    json!(
                        (0..FREEBUSY_CALENDARS_MAX)
                            .map(|i| four_byte(i, CALENDAR_ID_MAX_CHARS))
                            .collect::<Vec<_>>()
                    ),
                ],
            },
            _ => return None,
        })
    }

    /// The component's specification, as the package ships it.
    fn spec_text() -> String {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../config/specs/google-calendar-read.brenn");
        std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("{} must be readable: {error}", path.display()))
    }

    /// Each tool's `input_schema` states exactly the bounds its parser
    /// enforces, and the parser admits and refuses every bound's probes,
    /// naming the argument it refuses: the schema and the parser are held to
    /// the same table from both sides.
    #[test]
    fn every_tools_schema_states_what_its_parser_enforces() {
        let spec = spec_text();
        for tool in Provided::ALL {
            let name = tool.name();
            let bounds = bounds(tool);
            if let Err(error) = check(&input_schema(&spec, name), &bounds) {
                panic!("`{name}`'s input_schema: {error}");
            }
            let plan_with = |args: &Value| plan(tool, args, &settings()).map(drop);
            let base = admitted_args(tool);
            for key in bounds.required {
                hold_required(name, key, &base, plan_with);
            }
            for (key, bound) in &bounds.properties {
                let probes = workspace_probes(key)
                    .or_else(|| probes(bound))
                    .unwrap_or_else(|| panic!("`{name}`'s `{key}` has no probes"));
                hold_probes(name, key, &probes, &base, plan_with);
            }
        }
    }

    /// The parser budget no schema can state: the tool's description gives
    /// the number its parser's constant holds.
    #[test]
    fn every_description_states_its_parsers_budgets() {
        let budget =
            format!("the ids together may hold at most {FREEBUSY_IDS_MAX_BYTES} bytes of UTF-8");
        assert!(
            description(&spec_text(), Provided::CalendarFreeBusy.name()).contains(&budget),
            "`{}`'s description must say {budget:?}",
            Provided::CalendarFreeBusy.name()
        );
    }

    #[test]
    fn every_step_answers_an_unreadable_read_by_its_shape() {
        let refusal = "result exceeds [messaging] max_body_bytes";
        let outcomes = || {
            [
                (
                    cut_read(json!(300000)),
                    Unreadable::Cut("300000 bytes".to_string()),
                ),
                (
                    nested("internal", refusal),
                    Unreadable::Refused(refusal.to_string()),
                ),
            ]
        };
        let name = |state: &InFlight| format!("{} step {}", state.tool.name(), state.step);

        // Neither one of many nor with a narrower form: the rendering alone.
        for state in [
            calendar_list_state(json!({})),
            event_state(json!({ "id": "e1" })),
        ] {
            for (outcome, why) in outcomes() {
                assert_eq!(
                    answered(&state, outcome),
                    Next::Fail(why.failure(google::API_NAME, None)),
                    "{}",
                    name(&state)
                );
            }
        }

        // A window the caller can narrow: the rendering and what to narrow.
        for (state, hint, wording) in [
            (
                events_state(json!({ "from": FROM, "to": TO })),
                EVENTS_HINT,
                "; ask for fewer events with `max`, or a shorter `from`/`to` window",
            ),
            (
                free_busy_state(json!({ "from": FROM, "to": TO })),
                FREEBUSY_HINT,
                "; ask for a shorter `from`/`to` window, or fewer `calendars`",
            ),
        ] {
            for (outcome, why) in outcomes() {
                let answer = answered(&state, outcome);
                assert_eq!(
                    answer,
                    Next::Fail(why.failure(google::API_NAME, Some(hint))),
                    "{}",
                    name(&state)
                );
                let Next::Fail(Failure { detail, .. }) = answer else {
                    panic!("{answer:?}");
                };
                assert!(detail.ends_with(wording), "{detail}");
            }
        }

        // A step the tool does not have is refused as such.
        for (outcome, _) in outcomes() {
            let mut state = calendar_list_state(json!({}));
            state.step = 2;
            assert_eq!(
                answered(&state, outcome),
                Next::Fail(no_such_step(Provided::CalendarList, 2))
            );
        }
    }

    #[test]
    fn an_empty_answer_is_internal_at_every_step() {
        for state in first_reads() {
            for status in [200, 204] {
                let Next::Fail(Failure { kind, detail }) = answered(&state, http(status, ""))
                else {
                    panic!("{} step 1 accepted no body", state.tool.name());
                };
                assert_eq!(kind, ErrorKind::Internal);
                assert_eq!(
                    no_body_parts(&detail, status).map(|(_, answers)| answers),
                    Some("JSON"),
                    "{detail}"
                );
            }
        }
    }

    #[test]
    fn a_step_names_one_api_for_no_body_and_for_a_bad_shape() {
        let detail = |next: Next| match next {
            Next::Fail(Failure { detail, .. }) => detail,
            other => panic!("{other:?}"),
        };
        for state in first_reads() {
            let name = format!("{} step {}", state.tool.name(), state.step);
            let empty = detail(answered(&state, http(204, "")));
            let api = no_body_parts(&empty, 204)
                .map(|(api, _)| api)
                .unwrap_or_else(|| panic!("{name}: {empty}"));
            let bad = detail(answered(&state, http(200, "42")));
            assert!(
                bad.starts_with(&format!("unexpected Google {api} response shape")),
                "{name}: {bad}"
            );
        }
    }

    #[test]
    fn every_tools_first_read_has_an_unreadable_answer() {
        // `unreadable` runs only on a cut or refused read, so a tool missing
        // its arm there would pass every other test and be answered as a
        // step it does not have.
        for tool in Provided::ALL {
            let mut state = calendar_list_state(json!({}));
            state.tool = tool;
            for outcome in [
                cut_read(json!(300000)),
                nested("internal", "result exceeds [messaging] max_body_bytes"),
            ] {
                assert_ne!(
                    answered(&state, outcome),
                    Next::Fail(no_such_step(tool, 1)),
                    "{}",
                    tool.name()
                );
            }
        }
    }

    #[test]
    fn every_reads_failure_is_bare_and_names_the_read_scope() {
        // No step here writes, so every step's failure is the bare one: a
        // failure Google may have acted under carries no hint.
        assert_read_failures_are_bare(&first_reads(), Scope::CalendarRead, answered);
    }
}
