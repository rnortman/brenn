//! The wrapper's decisions, in `std` plus `serde_json`: validating
//! `CalendarCreate`'s, `CalendarUpdate`'s and `CalendarDelete`'s arguments,
//! building their Calendar calls as `api-request` arguments, and reading
//! `api-request`'s answer. Nothing here names the guest SDK, so the whole
//! module runs on the host under test. Tools are the generated `Provided`
//! enum, whose SDK-facing parts are wasm32-only.

// The host test's crate root is this file, so it declares the generated
// module itself; in the component, `lib.rs` does.
#[cfg(test)]
mod spec;

use brenn_google_workspace::args::{account, id_list, is_printable_ascii};
use brenn_google_workspace::calendar::{
    ATTENDEES_MAX, Compact, EVENT_GET_FIELDS, EVENT_TEXT_MAX_BYTES, PRIMARY_CALENDAR, calendar_id,
    calendar_id_rule, compact_event, event_id, event_path, events_path, is_calendar_id,
    is_event_id, time, written_event_max_chars,
};
use brenn_google_workspace::call::{delete_call, get_call, json_call};
use brenn_google_workspace::scope::{Scope, google_error, no_body_refused, with_write_hint};
use brenn_google_workspace::settings::Settings;
use brenn_google_workspace::text::{fit_text, shape};
use brenn_provider_kit::{
    Answer, ApiCall, Args, COMPACT_RESULT_CEILING, Call, ErrorKind, Failure, Logic, NestedError,
    Next, Query, Tool, Unreadable, bounded, capped, choice, copy, flag, google, internal,
    no_such_step, object, query, read_api_result, stored,
};
use serde_json::{Value, json};

use crate::spec::Provided;

/// The most characters an event write's `summary` or `location` may hold. A
/// write's arguments travel as one JSON body inside the nested call's args,
/// which the host caps at 64 KiB: with every argument at its cap, each at its
/// escaped width — `summary` and `location` at 7 bytes a character (escaped in
/// the body and again in the args), `description` at 7, `recurrence` at 4
/// (printable ASCII whose `"` and `\` escape twice), `attendees` at
/// `ATTENDEES_MAX_BYTES` plus their `{"email":…}` wrapping, `start`, `end`
/// and `time_zone` at 7 with `time_zone` written into both times, `account` at
/// 6, the calendar id and an update's event id in the path at their bytes —
/// the widest admitted create serializes to about 52 500 bytes and the widest
/// update to about 53 600.
const EVENT_FIELD_MAX_CHARS: usize = 1000;
/// The most characters an event write's `description` may hold; see
/// `EVENT_FIELD_MAX_CHARS` for the call's budget.
const EVENT_DESCRIPTION_MAX_CHARS: usize = 2000;
/// The most characters an event write's `time_zone` may hold: an IANA name,
/// passed to Google as given.
const EVENT_TIME_ZONE_MAX_CHARS: usize = 64;
/// The most UTF-8 bytes an event write's `attendees` may hold together. A
/// calendar id holds nothing JSON escapes (`calendar_id_shape` refuses `"`,
/// `\` and controls), so its bytes are what it adds to the call; see
/// `EVENT_FIELD_MAX_CHARS` for the call's budget.
const ATTENDEES_MAX_BYTES: usize = 8_000;
/// The most lines an event write's `recurrence` may hold.
const RECURRENCE_WRITE_MAX: usize = 5;
/// The most characters one `recurrence` line may hold, each printable ASCII.
const RECURRENCE_LINE_MAX_CHARS: usize = 500;
/// Google's `sendUpdates` words, in the order the schema states them: who
/// Google emails about a write.
const NOTIFY: [&str; 3] = ["none", "all", "externalOnly"];
/// Whom a write notifies when the caller names nobody: nobody.
const NOTIFY_DEFAULT: &str = "none";
/// What the caller is told when an events.insert may have been acted on and
/// its answer was lost or unreadable.
const CREATE_UNKNOWN_HINT: &str = "the event may have been created before its answer was lost: \
                                   look for it with CalendarEvents in its window before asking \
                                   again, or the user has two";
/// What the caller is told when an events.patch may have been acted on and
/// its answer was lost or unreadable.
const UPDATE_UNKNOWN_HINT: &str = "the change may already have been applied before its answer \
                                   was lost: CalendarEvent shows the event as it is, and asking \
                                   again with the same arguments is safe";
/// `CalendarDelete`'s read of the event it deletes.
const DELETE_READ_STEP: u8 = 1;
/// `CalendarDelete`'s `DELETE`.
const DELETE_STEP: u8 = 2;
/// The fields the delete's read asks for: what the answer names, and what
/// the series rule and the already-cancelled refusal read.
const DELETE_READ_FIELDS: &str = "id,summary,start,recurrence,recurringEventId,status";
/// What the caller is told when an events.delete may have been acted on and
/// its answer was lost or unreadable.
const DELETE_UNKNOWN_HINT: &str = "the event may already be deleted before its answer was lost: \
                                   asking again is safe, since a deleted event is refused as \
                                   already cancelled";

/// The scope `tool`'s step `step` needs. A step the tool does not have is
/// never asked for: `advance` maps an error only for a step it issued.
fn scope(tool: Provided, step: u8) -> Scope {
    match (tool, step) {
        // The delete's read is under the write set too: `calendar.events`
        // reads, and an account connected for reading alone fails the
        // `DELETE`, so the hint names the set that makes the whole tool work.
        (Provided::CalendarCreate | Provided::CalendarUpdate | Provided::CalendarDelete, _) => {
            Scope::CalendarWrite
        }
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
pub struct CalendarWrites;

impl Logic for CalendarWrites {
    type Tool = Provided;
    type Settings = Settings;

    fn plan(tool: Provided, args: &Value, settings: &Settings) -> Result<Call, String> {
        plan(tool, args, settings)
    }

    fn advance(state: &InFlight, outcome: Result<Value, NestedError>, settings: &Settings) -> Next {
        advance(state, outcome, settings)
    }
}

/// Validate a request's arguments and build its first nested call. An error is
/// the request's `invalid_args` detail, naming the argument.
pub fn plan(tool: Provided, args: &Value, settings: &Settings) -> Result<Call, String> {
    match tool {
        Provided::CalendarCreate => {
            let create = EventWriteArgs::parse_create(args)?;
            Ok(Call {
                step: 1,
                api: event_insert_call(&create, settings),
            })
        }
        Provided::CalendarUpdate => {
            let update = EventWriteArgs::parse_update(args)?;
            Ok(Call {
                step: 1,
                api: event_patch_call(&update, settings),
            })
        }
        Provided::CalendarDelete => {
            let delete = DeleteArgs::parse(args)?;
            Ok(Call {
                step: DELETE_READ_STEP,
                api: delete_read_call(&delete, settings),
            })
        }
    }
}

/// Which event write a `EventWriteArgs` is parsed for: a create requires
/// `summary`, `start` and `end`; an update requires `id`, admits an empty
/// `attendees`, and moves `start` and `end` together.
#[derive(Clone, Copy, PartialEq, Eq)]
enum EventWrite {
    Create,
    Update,
}

/// `CalendarCreate`'s and `CalendarUpdate`'s arguments. Every value is passed
/// to Google as given; the parser decides only which of Google's two time
/// shapes `start` and `end` are sent as.
struct EventWriteArgs {
    calendar: String,
    /// The event an update changes; `None` on a create.
    id: Option<String>,
    summary: Option<String>,
    start: Option<String>,
    end: Option<String>,
    description: Option<String>,
    location: Option<String>,
    attendees: Option<Vec<String>>,
    time_zone: Option<String>,
    recurrence: Option<Vec<String>>,
    notify: &'static str,
    account: Option<String>,
}

impl EventWriteArgs {
    /// Every argument `CalendarCreate` takes.
    const CREATE_KEYS: &[&str] = &[
        "calendar",
        "summary",
        "start",
        "end",
        "description",
        "location",
        "attendees",
        "time_zone",
        "recurrence",
        "notify",
        "account",
    ];
    /// Every argument `CalendarUpdate` takes.
    const UPDATE_KEYS: &[&str] = &[
        "id",
        "calendar",
        "summary",
        "start",
        "end",
        "description",
        "location",
        "attendees",
        "time_zone",
        "recurrence",
        "notify",
        "account",
    ];

    fn parse_create(args: &Value) -> Result<EventWriteArgs, String> {
        Self::parse(args, EventWrite::Create)
    }

    fn parse_update(args: &Value) -> Result<EventWriteArgs, String> {
        Self::parse(args, EventWrite::Update)
    }

    fn parse(args: &Value, write: EventWrite) -> Result<EventWriteArgs, String> {
        let keys = match write {
            EventWrite::Create => Self::CREATE_KEYS,
            EventWrite::Update => Self::UPDATE_KEYS,
        };
        let map = object(args, keys)?;
        let id = match write {
            EventWrite::Create => None,
            EventWrite::Update => Some(event_id(map, "id")?.ok_or("`id` is required")?),
        };
        let text = |key: &str, max: usize| -> Result<Option<String>, String> {
            Ok(capped(map, key, max)?.map(str::to_string))
        };
        let summary = text("summary", EVENT_FIELD_MAX_CHARS)?;
        let start = time(map, "start")?;
        let end = time(map, "end")?;
        if write == EventWrite::Create {
            for (key, value) in [("summary", &summary), ("start", &start), ("end", &end)] {
                if value.is_none() {
                    return Err(format!("`{key}` is required"));
                }
            }
        }
        let min_attendees = match write {
            EventWrite::Create => 1,
            EventWrite::Update => 0,
        };
        let attendees = id_list(
            map,
            "attendees",
            (min_attendees, ATTENDEES_MAX),
            "email addresses",
            is_calendar_id,
            calendar_id_rule,
        )?;
        if attendees.iter().flatten().map(String::len).sum::<usize>() > ATTENDEES_MAX_BYTES {
            return Err(format!(
                "`attendees` together exceed {ATTENDEES_MAX_BYTES} bytes (UTF-8); name fewer"
            ));
        }
        let recurrence = id_list(
            map,
            "recurrence",
            (1, RECURRENCE_WRITE_MAX),
            "RRULE, RDATE or EXDATE lines",
            is_recurrence_line,
            || format!("1 to {RECURRENCE_LINE_MAX_CHARS} printable ASCII characters"),
        )?;
        let parsed = EventWriteArgs {
            calendar: calendar_id(map, "calendar")?.unwrap_or_else(|| PRIMARY_CALENDAR.to_string()),
            id,
            summary,
            start,
            end,
            description: text("description", EVENT_DESCRIPTION_MAX_CHARS)?,
            location: text("location", EVENT_FIELD_MAX_CHARS)?,
            attendees,
            time_zone: text("time_zone", EVENT_TIME_ZONE_MAX_CHARS)?,
            recurrence,
            notify: choice(map, "notify", &NOTIFY, |notify| notify)?.unwrap_or(NOTIFY_DEFAULT),
            account: account(map)?,
        };
        parsed.cross_field(write)?;
        Ok(parsed)
    }

    /// The rules between arguments: `start` and `end` of one shape; on an
    /// update, both or neither, and `time_zone` only with them; a recurring
    /// timed event names its `time_zone`; an update names a field to change.
    fn cross_field(&self, write: EventWrite) -> Result<(), String> {
        if write == EventWrite::Update
            && (self.start.is_some() != self.end.is_some()
                || (self.time_zone.is_some() && self.start.is_none()))
        {
            return Err(
                "`start` and `end` must be given together, and `time_zone` only with \
                        them"
                    .to_string(),
            );
        }
        if let (Some(start), Some(end)) = (&self.start, &self.end)
            && is_date_shape(start) != is_date_shape(end)
        {
            return Err(
                "`start` and `end` must both be `YYYY-MM-DD` dates or both be times".to_string(),
            );
        }
        if self.recurrence.is_some()
            && self
                .start
                .as_deref()
                .is_some_and(|start| !is_date_shape(start))
            && self.time_zone.is_none()
        {
            return Err("a recurring timed event needs `time_zone`".to_string());
        }
        if write == EventWrite::Update
            && self.summary.is_none()
            && self.start.is_none()
            && self.description.is_none()
            && self.location.is_none()
            && self.attendees.is_none()
            && self.recurrence.is_none()
        {
            return Err("`CalendarUpdate` changes nothing: name a field".to_string());
        }
        Ok(())
    }
}

/// Whether `value` is one `recurrence` line: 1 to `RECURRENCE_LINE_MAX_CHARS`
/// printable ASCII characters.
fn is_recurrence_line(value: &str) -> bool {
    is_printable_ascii(value, RECURRENCE_LINE_MAX_CHARS)
}

/// Whether `value` has the shape of an all-day date, `dddd-dd-dd`: ten bytes,
/// digits but for a hyphen at the fifth and eighth. Nothing is interpreted; a
/// value of this shape Google does not take as a date is Google's to refuse.
fn is_date_shape(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 10
        && bytes.iter().enumerate().all(|(at, b)| match at {
            4 | 7 => *b == b'-',
            _ => b.is_ascii_digit(),
        })
}

/// One event time as Google takes it: `{"date"}` for a date-shaped value,
/// `{"dateTime"}` for any other, with `timeZone` when given. A `complete`
/// time also sends the other shape's key as `null`, so a patch replaces the
/// stored time whole whichever shape it held.
fn event_time(value: &str, time_zone: Option<&str>, complete: bool) -> Value {
    let (given, other) = if is_date_shape(value) {
        ("date", "dateTime")
    } else {
        ("dateTime", "date")
    };
    let mut time = Args::new();
    time.insert(given.to_string(), json!(value));
    if complete {
        time.insert(other.to_string(), Value::Null);
    }
    if let Some(zone) = time_zone {
        time.insert("timeZone".to_string(), json!(zone));
    }
    Value::Object(time)
}

/// An event write's body: each field the caller named, and no other. No
/// top-level key is ever `null`; times are `complete` on an update.
fn event_body(args: &EventWriteArgs, complete: bool) -> Value {
    let mut body = Args::new();
    for (key, value) in [
        ("summary", &args.summary),
        ("description", &args.description),
        ("location", &args.location),
    ] {
        if let Some(value) = value {
            body.insert(key.to_string(), json!(value));
        }
    }
    for (key, value) in [("start", &args.start), ("end", &args.end)] {
        if let Some(value) = value {
            body.insert(
                key.to_string(),
                event_time(value, args.time_zone.as_deref(), complete),
            );
        }
    }
    if let Some(attendees) = &args.attendees {
        let attendees: Vec<Value> = attendees
            .iter()
            .map(|email| json!({ "email": email }))
            .collect();
        body.insert("attendees".to_string(), Value::Array(attendees));
    }
    if let Some(recurrence) = &args.recurrence {
        body.insert("recurrence".to_string(), json!(recurrence));
    }
    Value::Object(body)
}

/// An event write's query: whom Google notifies, always sent, and the
/// events.get mask, so the answer is the event `compact_event` reads.
fn event_write_query(args: &EventWriteArgs) -> Query {
    query([
        ("sendUpdates", args.notify.to_string()),
        ("maxAttendees", ATTENDEES_MAX.to_string()),
        ("fields", EVENT_GET_FIELDS.to_string()),
    ])
}

/// events.insert of the arguments' event on their calendar.
fn event_insert_call(args: &EventWriteArgs, settings: &Settings) -> ApiCall {
    json_call(
        settings,
        "POST",
        args.account.as_deref(),
        events_path(&args.calendar),
        event_write_query(args),
        event_body(args, false),
    )
}

/// events.patch of the arguments' event: only the fields named change.
fn event_patch_call(args: &EventWriteArgs, settings: &Settings) -> ApiCall {
    let id = args.id.as_deref().expect("an update's parse requires `id`");
    json_call(
        settings,
        "PATCH",
        args.account.as_deref(),
        event_path(&args.calendar, id),
        event_write_query(args),
        event_body(args, true),
    )
}

/// `CalendarDelete`'s arguments.
struct DeleteArgs {
    id: String,
    calendar: String,
    /// Whether the caller asked to delete a recurring series whole.
    series: bool,
    notify: &'static str,
    account: Option<String>,
}

impl DeleteArgs {
    /// Every argument the tool takes.
    const KEYS: &[&str] = &["id", "calendar", "series", "notify", "account"];

    fn parse(args: &Value) -> Result<DeleteArgs, String> {
        let map = object(args, Self::KEYS)?;
        Ok(DeleteArgs {
            id: event_id(map, "id")?.ok_or("`id` is required")?,
            calendar: calendar_id(map, "calendar")?.unwrap_or_else(|| PRIMARY_CALENDAR.to_string()),
            series: flag(map, "series")?,
            notify: choice(map, "notify", &NOTIFY, |notify| notify)?.unwrap_or(NOTIFY_DEFAULT),
            account: account(map)?,
        })
    }

    fn path(&self) -> String {
        event_path(&self.calendar, &self.id)
    }
}

/// events.get of the event a delete names: what the answer reports and what
/// the series rule reads, nothing more.
fn delete_read_call(args: &DeleteArgs, settings: &Settings) -> ApiCall {
    get_call(
        settings,
        args.account.as_deref(),
        args.path(),
        query([("fields", DELETE_READ_FIELDS.to_string())]),
    )
}

/// events.delete of the event, whom Google notifies always sent.
fn event_delete_call(args: &DeleteArgs, settings: &Settings) -> ApiCall {
    delete_call(
        settings,
        args.account.as_deref(),
        args.path(),
        query([("sendUpdates", args.notify.to_string())]),
    )
}

/// The delete's read: the event's `DELETE` next, carrying in `derived` the
/// `summary` and `start` the answer names and `series: true` for a series
/// master. An event already cancelled is refused; a series master is deleted
/// only with `series`, and `series` is refused for any other event. An event
/// whose id is not the one asked for is Google's fault.
fn delete_checked(state: &InFlight, body: &Value, settings: &Settings) -> Result<Next, Failure> {
    let api = "events.get";
    let args = stored(state, DeleteArgs::parse)?;
    let mut event = Compact::of(body, api)?;
    if event.string("id")? != Some(args.id.as_str()) {
        return Err(shape(api));
    }
    let status = event.string("status")?;
    let master = !event.array("recurrence")?.is_empty();
    let occurrence_of = match event.string("recurringEventId")? {
        None => None,
        Some(master_id) if is_event_id(master_id) => Some(master_id),
        Some(_) => return Err(shape(api)),
    };
    event.text("summary", "summary", EVENT_TEXT_MAX_BYTES)?;
    event.when("start")?;
    let id = &args.id;
    let refusal = if status == Some("cancelled") {
        Some(format!(
            "`{id}` is already cancelled (deleted); nothing to delete"
        ))
    } else if master && !args.series {
        Some(format!(
            "`{id}` is a recurring series: pass `series: true` to delete every occurrence, or \
             delete one occurrence by its own `id` from `CalendarEvents`"
        ))
    } else if args.series && !master {
        Some(match occurrence_of {
            Some(master_id) => format!(
                "`{id}` is one occurrence of the series `{master_id}`: delete the series by that \
                 id with `series: true`, or this occurrence without `series`"
            ),
            None => format!("`{id}` is not a recurring series: delete it without `series`"),
        })
    } else {
        None
    };
    if let Some(detail) = refusal {
        return Err(Failure {
            kind: ErrorKind::InvalidArgs,
            detail,
        });
    }
    let Value::Object(mut derived) = event.done() else {
        unreachable!("a compacted event is an object");
    };
    if master {
        derived.insert("series".to_string(), json!(true));
    }
    Ok(Next::Call {
        call: Call {
            step: DELETE_STEP,
            api: event_delete_call(&args, settings),
        },
        derived,
    })
}

/// An events.delete Google accepted, whatever 2xx it answered with:
/// `deleted: true`, the `id`, and from the read before it the event's
/// `summary`, `start` and `series: true` for a series master.
fn delete_accepted(state: &InFlight) -> Result<Next, Failure> {
    let args = stored(state, DeleteArgs::parse).map_err(|failure| at_write(state, failure))?;
    let mut out = Args::new();
    out.insert("deleted".to_string(), json!(true));
    out.insert("id".to_string(), json!(args.id));
    for key in ["summary", "start", "series", "fields_truncated"] {
        copy(&mut out, key, state.derived.get(key));
    }
    Ok(Next::Complete(Value::Object(out)))
}

/// What the request does with its nested call's answer: complete, fail, or
/// issue its next step. Every answer completed here is held to the result
/// ceiling; a relayed credential answer is `api-request`'s own and is not.
pub fn advance(state: &InFlight, outcome: Result<Value, NestedError>, settings: &Settings) -> Next {
    let body = match read_api_result(outcome, &state.target, google::API_NAME) {
        Ok(Answer::Body(body)) => body,
        Ok(Answer::NoBody { status }) => return settle(state.tool, no_body(state, status)),
        Ok(Answer::Relay(value)) => return Next::Complete(value),
        Ok(Answer::Unreadable(why)) => return unreadable(state, why),
        Ok(Answer::Error { status, body }) => {
            return Next::Fail(at_write(
                state,
                google_error(status, &body, scope(state.tool, state.step)),
            ));
        }
        Ok(Answer::Bytes { status, .. }) => {
            let failure = internal(format!(
                "{} returned a non-text body ({status})",
                google::API_NAME
            ));
            return Next::Fail(at_write(state, failure));
        }
        Err(failure) => return Next::Fail(at_write(state, failure)),
    };
    settle(
        state.tool,
        match (state.tool, state.step) {
            (Provided::CalendarCreate, 1) => {
                compact_event(&body, "events.insert", None, written_event_max_chars())
                    .map(Next::Complete)
                    .map_err(|failure| at_write(state, failure))
            }
            (Provided::CalendarUpdate, 1) => stored(state, EventWriteArgs::parse_update)
                .and_then(|args| {
                    compact_event(
                        &body,
                        "events.patch",
                        args.id.as_deref(),
                        written_event_max_chars(),
                    )
                })
                .map(Next::Complete)
                .map_err(|failure| at_write(state, failure)),
            (Provided::CalendarDelete, DELETE_READ_STEP) => delete_checked(state, &body, settings),
            // Google's acceptance of a delete, whatever its body.
            (Provided::CalendarDelete, DELETE_STEP) => delete_accepted(state),
            (tool, step) => Err(no_such_step(tool, step)),
        },
    )
}

/// A nested read that was cut or refused, decided by the step's shape: a
/// write step fails with why and its hint; a delete's read of its event fails
/// with why.
fn unreadable(state: &InFlight, why: Unreadable) -> Next {
    if let Some(hint) = write_hint(state.tool, state.step) {
        return settle(state.tool, Err(why.failure(google::API_NAME, Some(hint))));
    }
    settle(
        state.tool,
        match (state.tool, state.step) {
            (Provided::CalendarDelete, DELETE_READ_STEP) => {
                Err(why.failure(google::API_NAME, None))
            }
            (tool, step) => Err(no_such_step(tool, step)),
        },
    )
}

/// The hint a write step's failure carries when Google may have acted: what
/// the caller can look for, or that asking again is safe. `None` for a read.
/// Every write step has its row here and nowhere else.
fn write_hint(tool: Provided, step: u8) -> Option<&'static str> {
    match (tool, step) {
        (Provided::CalendarCreate, 1) => Some(CREATE_UNKNOWN_HINT),
        (Provided::CalendarUpdate, 1) => Some(UPDATE_UNKNOWN_HINT),
        (Provided::CalendarDelete, DELETE_STEP) => Some(DELETE_UNKNOWN_HINT),
        _ => None,
    }
}

fn at_write(state: &InFlight, failure: Failure) -> Failure {
    with_write_hint(write_hint(state.tool, state.step), failure)
}

/// A 2xx with no body, decided by the step: an event's delete is accepted;
/// every other step refuses it, naming the API whose answer it expected, as
/// that step's `shape` failure names it, with a write step's hint.
fn no_body(state: &InFlight, status: u64) -> Result<Next, Failure> {
    let (api, answers) = match (state.tool, state.step) {
        (Provided::CalendarDelete, DELETE_STEP) => return delete_accepted(state),
        (Provided::CalendarCreate, 1) => ("events.insert", "the event"),
        (Provided::CalendarUpdate, 1) => ("events.patch", "the event"),
        (Provided::CalendarDelete, DELETE_READ_STEP) => ("events.get", "JSON"),
        (tool, step) => return Err(no_such_step(tool, step)),
    };
    Err(at_write(state, no_body_refused(status, api, answers)))
}

/// A step's decision as the request's next move, a completed answer held to
/// the result ceiling — after a written event's description is cut to fit it.
fn settle(tool: Provided, decided: Result<Next, Failure>) -> Next {
    match decided {
        Ok(Next::Complete(value)) => {
            let (value, hint) = match tool {
                Provided::CalendarCreate | Provided::CalendarUpdate => {
                    (fit_text(value, "description", COMPACT_RESULT_CEILING), None)
                }
                Provided::CalendarDelete => (value, None),
            };
            bounded(value, COMPACT_RESULT_CEILING, hint)
        }
        Ok(next) => next,
        Err(failure) => Next::Fail(failure),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use brenn_google_workspace::args::{
        ACCOUNT_MAX_CHARS, HOST_ARGS_CAP_BYTES, MAX_CHARS_DEFAULT, printable_ascii_pattern,
    };
    use brenn_google_workspace::calendar::{
        CALENDAR_ID_MAX_CHARS, EVENT_ID_MAX_CHARS, TIME_MAX_CHARS, calendar_id_pattern,
    };
    use brenn_google_workspace::scope::scope_hint;
    use brenn_google_workspace::testutil::{
        EVENT, EVENT_ID, admits_characters, calendar_id_probes, completed, cut_read, description,
        escaped_widest, event_with, follow, four_byte, lookup, no_body_parts,
    };
    use brenn_provider_kit::testutil::{failure, google, http, nested, text_body};
    use brenn_provider_kit::{
        Bound, Probes, QueryValue, ToolBounds, check, cut, hold_probes, hold_required,
        input_schema, internal, probes,
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

    /// `advance` on the row, under the test settings.
    fn answered(state: &InFlight, outcome: Result<Value, NestedError>) -> Next {
        advance(state, outcome, &settings())
    }

    /// `body` compacted as the event `EVENT_ID`, its description cut at
    /// `written_event_max_chars`, which the shared crate's tests hold to a
    /// `CalendarEvent` read's default.
    fn read_event(body: &str) -> Next {
        let body: Value = serde_json::from_str(body).unwrap();
        Next::Complete(
            compact_event(
                &body,
                "events.get",
                Some(EVENT_ID),
                written_event_max_chars(),
            )
            .unwrap(),
        )
    }

    /// An event's start and end, timed, in one zone.
    const START: &str = "2026-11-02T09:00:00-05:00";
    const END: &str = "2026-11-02T09:30:00-05:00";
    const ZONE: &str = "America/New_York";

    /// `extra` set over `base`.
    fn over(mut base: Value, extra: Value) -> Value {
        base.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        base
    }

    /// A create's arguments: a timed lunch in `ZONE`, with `extra` set over
    /// them.
    fn create_args(extra: Value) -> Value {
        over(
            json!({ "summary": "Lunch", "start": START, "end": END, "time_zone": ZONE }),
            extra,
        )
    }

    /// An update's arguments: event `e1` moved to `START`..`END` in `ZONE`,
    /// with `extra` set over them.
    fn update_args(extra: Value) -> Value {
        over(
            json!({ "id": "e1", "start": START, "end": END, "time_zone": ZONE }),
            extra,
        )
    }

    fn create(args: Value) -> Result<ApiCall, String> {
        plan(Provided::CalendarCreate, &args, &settings()).map(|call| {
            assert_eq!(call.step, 1);
            call.api
        })
    }

    fn update(args: Value) -> Result<ApiCall, String> {
        plan(Provided::CalendarUpdate, &args, &settings()).map(|call| {
            assert_eq!(call.step, 1);
            call.api
        })
    }

    fn write_state(tool: Provided, args: Value) -> InFlight {
        let call = plan(tool, &args, &settings()).unwrap();
        InFlight::new(
            &"a".repeat(32),
            tool,
            &args,
            &call,
            Map::new(),
            1_000,
            1_000,
        )
    }

    fn create_state(args: Value) -> InFlight {
        write_state(Provided::CalendarCreate, args)
    }

    fn update_state(args: Value) -> InFlight {
        write_state(Provided::CalendarUpdate, args)
    }

    /// A call's JSON body.
    fn json_body(call: &ApiCall) -> Value {
        serde_json::from_str(text_body(call)).unwrap()
    }

    /// The paths of every `null` in `value`.
    fn nulls(value: &Value, at: &str) -> Vec<String> {
        match value {
            Value::Null => vec![at.to_string()],
            Value::Object(map) => map
                .iter()
                .flat_map(|(key, value)| nulls(value, &format!("{at}.{key}")))
                .collect(),
            Value::Array(items) => items
                .iter()
                .enumerate()
                .flat_map(|(i, value)| nulls(value, &format!("{at}[{i}]")))
                .collect(),
            _ => Vec::new(),
        }
    }

    /// An event write's query: `notify`'s word, then the read's mask.
    fn event_write_query_with(notify: &str) -> Query {
        query([
            ("sendUpdates", notify.to_string()),
            ("maxAttendees", "50".to_string()),
            ("fields", EVENT_GET_FIELDS.to_string()),
            ("prettyPrint", "false".to_string()),
        ])
    }

    #[test]
    fn a_create_posts_only_the_fields_given_with_send_updates() {
        let call = create(create_args(json!({
            "calendar": "team@group.calendar.example.com",
            "description": "Agenda",
            "location": "Room 4",
            "attendees": ["bob@example.com", "carol@example.com"],
            "recurrence": ["RRULE:FREQ=WEEKLY;BYDAY=MO"],
            "notify": "externalOnly",
            "account": "alice@example.com",
        })))
        .unwrap();
        assert_eq!(
            call,
            ApiCall {
                provider: "google".to_string(),
                account: Some("alice@example.com".to_string()),
                method: "POST".to_string(),
                host: "calendar.example.com".to_string(),
                path: "/calendar/v3/calendars/team@group.calendar.example.com/events".to_string(),
                query: event_write_query_with("externalOnly"),
                headers: BTreeMap::new(),
                body: call.body.clone(),
            }
        );
        assert_eq!(
            json_body(&call),
            json!({
                "summary": "Lunch",
                "description": "Agenda",
                "location": "Room 4",
                "start": { "dateTime": START, "timeZone": ZONE },
                "end": { "dateTime": END, "timeZone": ZONE },
                "attendees": [{ "email": "bob@example.com" }, { "email": "carol@example.com" }],
                "recurrence": ["RRULE:FREQ=WEEKLY;BYDAY=MO"],
            })
        );
        // The fewest arguments: only those fields, on `primary`, and
        // `sendUpdates=none` sent although nobody was named.
        let call = create(json!({ "summary": "Lunch", "start": START, "end": END })).unwrap();
        assert_eq!(call.path, "/calendar/v3/calendars/primary/events");
        assert_eq!(call.query, event_write_query_with("none"));
        let body = json_body(&call);
        assert_eq!(
            body,
            json!({
                "summary": "Lunch",
                "start": { "dateTime": START },
                "end": { "dateTime": END },
            })
        );
        assert_eq!(nulls(&body, "body"), Vec::<String>::new());
        for notify in NOTIFY {
            let call = create(create_args(json!({ "notify": notify }))).unwrap();
            assert_eq!(call.query, event_write_query_with(notify));
        }
    }

    #[test]
    fn an_update_patches_only_the_fields_named() {
        // Every subset of the fields an update changes, `start` and `end`
        // (with `time_zone`) moving as one.
        let fields: [(&[&str], Value); 6] = [
            (&["summary"], json!({ "summary": "Lunch" })),
            (&["description"], json!({ "description": "Agenda" })),
            (&["location"], json!({ "location": "Room 4" })),
            (&["attendees"], json!({ "attendees": [] })),
            (
                &["recurrence"],
                json!({ "recurrence": ["RRULE:FREQ=DAILY"] }),
            ),
            (
                &["start", "end"],
                json!({ "start": START, "end": END, "time_zone": ZONE }),
            ),
        ];
        for subset in 1_u32..(1 << fields.len()) {
            let mut args = json!({ "id": "e1" });
            let mut named = BTreeSet::new();
            for (at, (keys, given)) in fields.iter().enumerate() {
                if subset & (1 << at) != 0 {
                    args = over(args, given.clone());
                    named.extend(keys.iter().copied());
                }
            }
            let call = update(args.clone()).unwrap_or_else(|error| panic!("{args}: {error}"));
            assert_eq!(call.method, "PATCH");
            assert_eq!(call.path, "/calendar/v3/calendars/primary/events/e1");
            assert_eq!(call.query, event_write_query_with("none"));
            let body = json_body(&call);
            let keys: BTreeSet<&str> = body
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect();
            assert_eq!(keys, named, "{args}");
            // The only nulls are the unused kind inside a given time.
            let mut expected_nulls = Vec::new();
            if named.contains("start") {
                expected_nulls = vec!["body.end.date".to_string(), "body.start.date".to_string()];
            }
            let mut found = nulls(&body, "body");
            found.sort();
            assert_eq!(found, expected_nulls, "{args}");
        }
        assert_eq!(
            json_body(&update(update_args(json!({}))).unwrap()),
            json!({
                "start": { "dateTime": START, "date": null, "timeZone": ZONE },
                "end": { "dateTime": END, "date": null, "timeZone": ZONE },
            })
        );
    }

    #[test]
    fn a_date_shaped_time_is_sent_as_date() {
        for (value, date) in [
            ("2026-11-02", true),
            ("2026-13-45", true),
            ("0000-00-00", true),
            ("2026-11-2", false),
            ("2026/11/02", false),
            ("2026-11-02 ", false),
            ("20261-1-02", false),
            ("２０２６-11-02", false),
            (START, false),
            ("2026-11-02T09:00:00Z", false),
        ] {
            assert_eq!(is_date_shape(value), date, "{value:?}");
        }
        let all_day = json!({ "start": "2026-11-02", "end": "2026-11-03" });
        // A create sends the date alone, `time_zone` beside it when given.
        let body = json_body(&create(over(json!({ "summary": "Trip" }), all_day.clone())).unwrap());
        assert_eq!(body["start"], json!({ "date": "2026-11-02" }));
        assert_eq!(body["end"], json!({ "date": "2026-11-03" }));
        let body = json_body(&create(create_args(all_day.clone())).unwrap());
        assert_eq!(
            body["start"],
            json!({ "date": "2026-11-02", "timeZone": ZONE })
        );
        // An update clears the other kind, so an event moves between them.
        let body = json_body(&update(over(json!({ "id": "e1" }), all_day)).unwrap());
        assert_eq!(
            body["start"],
            json!({ "date": "2026-11-02", "dateTime": null })
        );
        assert_eq!(
            body["end"],
            json!({ "date": "2026-11-03", "dateTime": null })
        );
        let body = json_body(&update(json!({ "id": "e1", "start": START, "end": END })).unwrap());
        assert_eq!(body["start"], json!({ "dateTime": START, "date": null }));
    }

    #[test]
    fn event_times_are_both_dates_or_both_times() {
        let refusal = "`start` and `end` must both be `YYYY-MM-DD` dates or both be times";
        for (start, end) in [("2026-11-02", END), (START, "2026-11-03")] {
            let times = json!({ "start": start, "end": end });
            assert_eq!(create(create_args(times.clone())).unwrap_err(), refusal);
            assert_eq!(update(update_args(times)).unwrap_err(), refusal);
        }
    }

    #[test]
    fn an_update_moves_start_and_end_together() {
        let refusal = "`start` and `end` must be given together, and `time_zone` only with them";
        for args in [
            json!({ "id": "e1", "start": START }),
            json!({ "id": "e1", "end": END }),
            json!({ "id": "e1", "time_zone": ZONE }),
            json!({ "id": "e1", "start": START, "time_zone": ZONE }),
            json!({ "id": "e1", "end": END, "time_zone": ZONE, "summary": "Lunch" }),
            json!({ "id": "e1", "time_zone": ZONE, "summary": "Lunch" }),
        ] {
            assert_eq!(update(args.clone()).unwrap_err(), refusal, "{args}");
        }
        assert!(update(json!({ "id": "e1", "start": START, "end": END })).is_ok());
    }

    #[test]
    fn a_recurring_timed_event_needs_a_time_zone() {
        let refusal = "a recurring timed event needs `time_zone`";
        let weekly = json!({ "recurrence": ["RRULE:FREQ=WEEKLY"] });
        let zoneless = |base: Value| {
            let mut args = over(base, weekly.clone());
            args.as_object_mut().unwrap().remove("time_zone");
            args
        };
        assert_eq!(
            create(zoneless(create_args(json!({})))).unwrap_err(),
            refusal
        );
        assert_eq!(
            update(zoneless(update_args(json!({})))).unwrap_err(),
            refusal
        );
        assert!(create(create_args(weekly.clone())).is_ok());
        assert!(update(update_args(weekly.clone())).is_ok());
        // An all-day series needs no zone, and an update's rules alone leave
        // the stored time to Google.
        let all_day = json!({ "start": "2026-11-02", "end": "2026-11-03" });
        assert!(create(zoneless(create_args(all_day))).is_ok());
        assert!(update(over(json!({ "id": "e1" }), weekly)).is_ok());
    }

    #[test]
    fn an_update_names_a_field() {
        let refusal = "`CalendarUpdate` changes nothing: name a field";
        for args in [
            json!({ "id": "e1" }),
            json!({
                "id": "e1",
                "calendar": "primary",
                "notify": "all",
                "account": "alice@example.com",
            }),
        ] {
            assert_eq!(update(args.clone()).unwrap_err(), refusal, "{args}");
        }
        // An empty list is a change: every attendee removed.
        let call = update(json!({ "id": "e1", "attendees": [] })).unwrap();
        assert_eq!(json_body(&call), json!({ "attendees": [] }));
    }

    #[test]
    fn event_writes_refuse_bad_arguments() {
        for (args, refusal) in [
            (
                json!({ "start": START, "end": END }),
                "`summary` is required",
            ),
            (
                json!({ "summary": "Lunch", "end": END }),
                "`start` is required",
            ),
            (
                create_args(json!({ "notify": "everyone" })),
                "`notify` must be one of `none`, `all`, `externalOnly`",
            ),
            (
                create_args(json!({ "notify": true })),
                "`notify` must be one of `none`, `all`, `externalOnly`",
            ),
            (
                create_args(json!({ "attendees": [] })),
                "`attendees` must be an array of 1 to 50 email addresses",
            ),
            (
                create_args(json!({ "attendees": ["bob@example.com", "bob@example.com"] })),
                "`attendees` names \"bob@example.com\" twice",
            ),
            (
                create_args(json!({
                    "attendees": (0..ATTENDEES_MAX)
                        .map(|i| four_byte(i, CALENDAR_ID_MAX_CHARS / 2))
                        .collect::<Vec<_>>(),
                })),
                "`attendees` together exceed 8000 bytes (UTF-8); name fewer",
            ),
            (
                create_args(json!({ "recurrence": ["RRULE:FREQ=DAILY\nEXDATE:x"] })),
                "each of `recurrence` must be 1 to 500 printable ASCII characters",
            ),
            (
                create_args(json!({ "recurrence": [] })),
                "`recurrence` must be an array of 1 to 5 RRULE, RDATE or EXDATE lines",
            ),
            (
                create_args(json!({ "reminders": { "useDefault": false } })),
                "unknown argument `reminders`; this tool takes `calendar`, `summary`, `start`, \
                 `end`, `description`, `location`, `attendees`, `time_zone`, `recurrence`, \
                 `notify`, `account`",
            ),
            (
                create_args(json!({ "id": "e1" })),
                "unknown argument `id`; this tool takes `calendar`, `summary`, `start`, `end`, \
                 `description`, `location`, `attendees`, `time_zone`, `recurrence`, `notify`, \
                 `account`",
            ),
        ] {
            assert_eq!(create(args.clone()).unwrap_err(), refusal, "{args}");
        }
        for (args, refusal) in [
            (json!({ "summary": "Lunch" }), "`id` is required"),
            (
                update_args(json!({ "id": "e/1" })),
                "`id` must be 1 to 1024 letters, digits, `_` or `-`",
            ),
            (
                update_args(json!({ "attendees": ["a b"] })),
                "each of `attendees` must be a calendar id of 1 to 320 characters with no `/`, \
                 `\\`, `%`, `\"`, space or control character",
            ),
            (
                update_args(json!({ "conferenceData": {} })),
                "unknown argument `conferenceData`; this tool takes `id`, `calendar`, `summary`, \
                 `start`, `end`, `description`, `location`, `attendees`, `time_zone`, \
                 `recurrence`, `notify`, `account`",
            ),
        ] {
            assert_eq!(update(args.clone()).unwrap_err(), refusal, "{args}");
        }
    }

    #[test]
    fn a_create_answers_the_event_with_its_new_id() {
        let state = create_state(create_args(json!({})));
        assert_eq!(answered(&state, http(200, EVENT)), read_event(EVENT));
        // A new id is required and held to an event id's shape; a failure
        // to read it carries the hint, since the event may exist.
        let hinted = Next::Fail(internal(format!(
            "unexpected Google events.insert response shape; {CREATE_UNKNOWN_HINT}"
        )));
        for body in [
            event_with(json!({ "id": null })),
            event_with(json!({ "id": "not/an/id" })),
            event_with(json!({ "start": { "date": "2026-11-04", "dateTime": START } })),
            "42".to_string(),
        ] {
            assert_eq!(answered(&state, http(200, &body)), hinted, "{body}");
        }
    }

    #[test]
    fn an_update_answers_the_event_as_google_holds_it() {
        let state = update_state(update_args(json!({ "id": EVENT_ID })));
        assert_eq!(answered(&state, http(200, EVENT)), read_event(EVENT));
        // Another event's answer is Google's fault, named for the patch.
        let state = update_state(update_args(json!({})));
        assert_eq!(
            answered(&state, http(200, EVENT)),
            Next::Fail(internal(format!(
                "unexpected Google events.patch response shape; {UPDATE_UNKNOWN_HINT}"
            )))
        );
    }

    #[test]
    fn a_written_events_description_is_cut_as_an_events_read_cuts_it() {
        let whole = usize::try_from(MAX_CHARS_DEFAULT).unwrap() + 1;
        let body = event_with(json!({ "description": "d".repeat(whole) }));
        let read = read_event(&body);
        let answer = completed(read.clone());
        assert_eq!(answer["description"], json!("d".repeat(whole - 1)));
        assert_eq!(answer["description_truncated"], json!(true));
        assert_eq!(answer["description_chars"], json!(whole));
        for state in [
            create_state(create_args(json!({}))),
            update_state(update_args(json!({ "id": EVENT_ID }))),
        ] {
            assert_eq!(
                answered(&state, http(200, &body)),
                read,
                "{}",
                state.tool.name()
            );
        }
    }

    #[test]
    fn an_update_refuses_an_event_holding_both_kinds() {
        // Should Google keep both kinds after a patch that cleared one, the
        // read's both-or-neither rule still refuses the answer.
        let state = update_state(update_args(json!({ "id": EVENT_ID })));
        for key in ["start", "end"] {
            let body = event_with(json!({
                key: { "date": "2026-11-04", "dateTime": START, "timeZone": ZONE },
            }));
            assert_eq!(
                answered(&state, http(200, &body)),
                Next::Fail(internal(format!(
                    "unexpected Google events.patch response shape; {UPDATE_UNKNOWN_HINT}"
                )))
            );
        }
    }

    #[test]
    fn an_event_writes_refusal_carries_googles_message_and_a_403_the_write_scope() {
        for (state, hint) in [
            (create_state(create_args(json!({}))), CREATE_UNKNOWN_HINT),
            (update_state(update_args(json!({}))), UPDATE_UNKNOWN_HINT),
        ] {
            let name = state.tool.name();
            // Google's refusal changed nothing: its message, no hint.
            let refused = google(
                400,
                "INVALID_ARGUMENT",
                "The specified time range is empty.",
            );
            let Next::Fail(failed) = answered(&state, http(400, &refused)) else {
                panic!("{name} fails");
            };
            assert_eq!(failed.kind, ErrorKind::InvalidArgs, "{name}");
            assert!(
                failed.detail.contains("The specified time range is empty."),
                "{name}: {failed:?}"
            );
            assert!(!failed.detail.contains(hint), "{name}: {failed:?}");
            // A 403 names the scope set that makes every Calendar write work.
            let denied = google(
                403,
                "PERMISSION_DENIED",
                "Request had insufficient authentication scopes.",
            );
            assert_eq!(
                answered(&state, http(403, &denied)),
                Next::Fail(failure(
                    ErrorKind::Denied,
                    &format!(
                        "Google PERMISSION_DENIED (HTTP 403): Request had insufficient \
                         authentication scopes. {}",
                        scope_hint(Scope::CalendarWrite)
                    )
                )),
                "{name}"
            );
        }
        assert!(scope_hint(Scope::CalendarWrite).contains("the Calendar events scope"));
    }

    /// The widest admitted event write of `tool`: every text at its cap at
    /// its escaped width, the most attendees at their budget, the most and
    /// longest recurrence lines of `"`, and the calendar id at four bytes a
    /// character.
    fn widest_event_write(tool: Provided) -> Value {
        let mut args = json!({
            "calendar": four_byte(0, CALENDAR_ID_MAX_CHARS),
            "summary": escaped_widest(EVENT_FIELD_MAX_CHARS),
            "start": escaped_widest(TIME_MAX_CHARS),
            "end": escaped_widest(TIME_MAX_CHARS),
            "description": escaped_widest(EVENT_DESCRIPTION_MAX_CHARS),
            "location": escaped_widest(EVENT_FIELD_MAX_CHARS),
            "attendees": (0..ATTENDEES_MAX)
                .map(|i| four_byte(i, ATTENDEES_MAX_BYTES / ATTENDEES_MAX / 4))
                .collect::<Vec<_>>(),
            "time_zone": escaped_widest(EVENT_TIME_ZONE_MAX_CHARS),
            "recurrence": (0..RECURRENCE_WRITE_MAX)
                .map(|i| format!("{i}{}", "\"".repeat(RECURRENCE_LINE_MAX_CHARS - 1)))
                .collect::<Vec<_>>(),
            "notify": "externalOnly",
            "account": escaped_widest(ACCOUNT_MAX_CHARS),
        });
        if tool == Provided::CalendarUpdate {
            args["id"] = json!("e".repeat(EVENT_ID_MAX_CHARS));
        }
        args
    }

    #[test]
    fn the_widest_create_call_fits_the_args_cap() {
        let args = widest_event_write(Provided::CalendarCreate);
        let attendees: usize = args["attendees"]
            .as_array()
            .unwrap()
            .iter()
            .map(|id| id.as_str().unwrap().len())
            .sum();
        assert_eq!(attendees, ATTENDEES_MAX_BYTES);
        let call = serde_json::to_vec(&create(args).unwrap()).unwrap();
        assert!(
            call.len() <= HOST_ARGS_CAP_BYTES,
            "{} bytes of args",
            call.len()
        );
    }

    #[test]
    fn the_widest_update_call_fits_the_args_cap() {
        let call =
            serde_json::to_vec(&update(widest_event_write(Provided::CalendarUpdate)).unwrap())
                .unwrap();
        assert!(
            call.len() <= HOST_ARGS_CAP_BYTES,
            "{} bytes of args",
            call.len()
        );
    }

    fn delete(args: Value) -> Result<ApiCall, String> {
        plan(Provided::CalendarDelete, &args, &settings()).map(|call| {
            assert_eq!(call.step, DELETE_READ_STEP);
            call.api
        })
    }

    fn delete_state(args: Value) -> InFlight {
        write_state(Provided::CalendarDelete, args)
    }

    /// The delete's read of event `e1`, a timed standup, with `extra` set
    /// over Google's answer.
    fn delete_read(extra: Value) -> String {
        over(
            json!({
                "id": "e1",
                "status": "confirmed",
                "summary": "Standup",
                "start": { "dateTime": START, "timeZone": ZONE },
            }),
            extra,
        )
        .to_string()
    }

    /// The delete's row at its `DELETE`, after `read` was answered to
    /// `args`, and that `DELETE`'s call.
    fn delete_after_read(args: Value, read: &str) -> (InFlight, Call) {
        let state = delete_state(args);
        follow(&state, answered(&state, http(200, read)))
    }

    #[test]
    fn a_delete_reads_then_deletes() {
        let args = json!({
            "id": "e1",
            "calendar": "team@example.com",
            "notify": "all",
            "account": "alice@example.com",
        });
        assert_eq!(
            delete(args.clone()).unwrap(),
            ApiCall {
                provider: "google".to_string(),
                account: Some("alice@example.com".to_string()),
                method: "GET".to_string(),
                host: "calendar.example.com".to_string(),
                path: "/calendar/v3/calendars/team@example.com/events/e1".to_string(),
                query: query([
                    ("fields", DELETE_READ_FIELDS.to_string()),
                    ("prettyPrint", "false".to_string()),
                ]),
                headers: BTreeMap::new(),
                body: None,
            }
        );
        let (row, call) = delete_after_read(args, &delete_read(json!({})));
        assert_eq!(call.step, DELETE_STEP);
        assert_eq!(
            call.api,
            ApiCall {
                provider: "google".to_string(),
                account: Some("alice@example.com".to_string()),
                method: "DELETE".to_string(),
                host: "calendar.example.com".to_string(),
                path: "/calendar/v3/calendars/team@example.com/events/e1".to_string(),
                query: query([
                    ("sendUpdates", "all".to_string()),
                    ("prettyPrint", "false".to_string()),
                ]),
                headers: BTreeMap::new(),
                body: None,
            }
        );
        assert_eq!(
            answered(&row, http(204, "")),
            Next::Complete(json!({
                "deleted": true,
                "id": "e1",
                "summary": "Standup",
                "start": { "date_time": START, "time_zone": ZONE },
            }))
        );
        // Whom Google notifies is always sent: nobody by default.
        let (_, call) = delete_after_read(json!({ "id": "e1" }), &delete_read(json!({})));
        assert_eq!(
            call.api.query.get("sendUpdates"),
            Some(&QueryValue::One("none".to_string()))
        );
        assert_eq!(call.api.path, "/calendar/v3/calendars/primary/events/e1");
    }

    #[test]
    fn delete_refuses_bad_arguments() {
        for (args, refusal) in [
            (json!({}), "`id` is required"),
            (
                json!({ "id": "e/1" }),
                "`id` must be 1 to 1024 letters, digits, `_` or `-`",
            ),
            (
                json!({ "id": "e1", "series": "yes" }),
                "`series` must be true or false",
            ),
            (
                json!({ "id": "e1", "notify": "everyone" }),
                "`notify` must be one of `none`, `all`, `externalOnly`",
            ),
            (
                json!({ "id": "e1", "calendar": "a/b" }),
                "`calendar` must be a calendar id of 1 to 320 characters with no `/`, `\\`, \
                 `%`, `\"`, space or control character",
            ),
            (
                json!({ "id": "e1", "move_to": "team@example.com" }),
                "unknown argument `move_to`; this tool takes `id`, `calendar`, `series`, \
                 `notify`, `account`",
            ),
        ] {
            assert_eq!(delete(args.clone()).unwrap_err(), refusal, "{args}");
        }
    }

    #[test]
    fn a_delete_applies_the_series_rule_before_deleting() {
        let master = json!({ "recurrence": ["RRULE:FREQ=WEEKLY;BYDAY=MO"] });
        let occurrence = json!({ "id": "e1", "recurringEventId": "s1" });
        let refused = |detail: &str| Next::Fail(failure(ErrorKind::InvalidArgs, detail));
        // Refused at the read: nothing is deleted.
        for (args, read, answer) in [
            (
                json!({ "id": "e1" }),
                master.clone(),
                refused(
                    "`e1` is a recurring series: pass `series: true` to delete every \
                     occurrence, or delete one occurrence by its own `id` from \
                     `CalendarEvents`",
                ),
            ),
            (
                json!({ "id": "e1", "series": true }),
                occurrence.clone(),
                refused(
                    "`e1` is one occurrence of the series `s1`: delete the series by that id \
                     with `series: true`, or this occurrence without `series`",
                ),
            ),
            (
                json!({ "id": "e1", "series": true }),
                json!({}),
                refused("`e1` is not a recurring series: delete it without `series`"),
            ),
            (
                json!({ "id": "e1" }),
                json!({ "status": "cancelled" }),
                refused("`e1` is already cancelled (deleted); nothing to delete"),
            ),
            // A cancelled series is refused as cancelled, whatever `series` says.
            (
                json!({ "id": "e1", "series": true }),
                over(master.clone(), json!({ "status": "cancelled" })),
                refused("`e1` is already cancelled (deleted); nothing to delete"),
            ),
        ] {
            let state = delete_state(args.clone());
            assert_eq!(
                answered(&state, http(200, &delete_read(read.clone()))),
                answer,
                "{args} {read}"
            );
        }
        // Deleted: a master with `series`, an occurrence without it.
        let (row, call) =
            delete_after_read(json!({ "id": "e1", "series": true }), &delete_read(master));
        assert_eq!(call.api.method, "DELETE");
        assert_eq!(
            answered(&row, http(204, "")),
            Next::Complete(json!({
                "deleted": true,
                "id": "e1",
                "summary": "Standup",
                "start": { "date_time": START, "time_zone": ZONE },
                "series": true,
            }))
        );
        let (row, call) = delete_after_read(json!({ "id": "e1" }), &delete_read(occurrence));
        assert_eq!(call.api.method, "DELETE");
        assert_eq!(
            answered(&row, http(204, "")),
            Next::Complete(json!({
                "deleted": true,
                "id": "e1",
                "summary": "Standup",
                "start": { "date_time": START, "time_zone": ZONE },
            }))
        );
    }

    #[test]
    fn a_delete_reads_what_google_answers_strictly() {
        let state = delete_state(json!({ "id": "e1" }));
        let bad = Next::Fail(internal(
            "unexpected Google events.get response shape".to_string(),
        ));
        for body in [
            delete_read(json!({ "id": "e2" })),
            delete_read(json!({ "id": null })),
            delete_read(json!({ "recurrence": "RRULE:FREQ=DAILY" })),
            delete_read(json!({ "recurringEventId": "s/1" })),
            delete_read(json!({ "status": 1 })),
            delete_read(json!({ "start": { "date": "2026-11-02", "dateTime": START } })),
            "42".to_string(),
        ] {
            assert_eq!(answered(&state, http(200, &body)), bad, "{body}");
        }
        // An all-day event, no summary: the answer names what Google gave.
        let (row, _) = delete_after_read(
            json!({ "id": "e1" }),
            &json!({ "id": "e1", "start": { "date": "2026-11-02" } }).to_string(),
        );
        assert_eq!(
            answered(&row, http(204, "")),
            Next::Complete(json!({
                "deleted": true,
                "id": "e1",
                "start": { "date": "2026-11-02" },
            }))
        );
        // A summary past the answer's bound is cut, and says so.
        let long = "s".repeat(EVENT_TEXT_MAX_BYTES + 1);
        let (row, _) = delete_after_read(
            json!({ "id": "e1" }),
            &delete_read(json!({ "summary": &long })),
        );
        assert_eq!(
            answered(&row, http(204, "")),
            Next::Complete(json!({
                "deleted": true,
                "id": "e1",
                "summary": cut(&long, EVENT_TEXT_MAX_BYTES).0,
                "start": { "date_time": START, "time_zone": ZONE },
                "fields_truncated": ["summary"],
            }))
        );
    }

    #[test]
    fn a_delete_is_accepted_on_any_2xx() {
        let (row, _) = delete_after_read(json!({ "id": "e1" }), &delete_read(json!({})));
        let accepted = json!({
            "deleted": true,
            "id": "e1",
            "summary": "Standup",
            "start": { "date_time": START, "time_zone": ZONE },
        });
        for outcome in [
            http(204, ""),
            http(200, ""),
            http(200, "{}"),
            http(200, r#"{"unexpected":[1]}"#),
            http(200, "42"),
        ] {
            assert_eq!(answered(&row, outcome), Next::Complete(accepted.clone()));
        }
    }

    #[test]
    fn a_deletes_410_carries_its_hint_and_its_reads_failures_none() {
        let row = write_step_state(Provided::CalendarDelete, DELETE_STEP);
        // A 410 is an event deleted between the read and the `DELETE`.
        let Next::Fail(failed) = answered(
            &row,
            http(410, &google(410, "GONE", "Resource has been deleted")),
        ) else {
            panic!("the delete fails");
        };
        assert_eq!(failed.kind, ErrorKind::Internal);
        assert!(
            failed.detail.ends_with(&format!("; {DELETE_UNKNOWN_HINT}")),
            "{failed:?}"
        );
        // The read deletes nothing: its failures carry no hint.
        let read = delete_state(json!({ "id": "e1" }));
        for outcome in [
            http(503, "{}"),
            http(404, &google(404, "NOT_FOUND", "Not Found")),
            cut_read(json!(300000)),
            nested("rate_limited", "slow down"),
        ] {
            let Next::Fail(failed) = answered(&read, outcome) else {
                panic!("the read fails");
            };
            assert!(!failed.detail.contains(DELETE_UNKNOWN_HINT), "{failed:?}");
        }
        assert_eq!(
            answered(&read, http(404, &google(404, "NOT_FOUND", "Not Found"))),
            Next::Fail(failure(
                ErrorKind::InvalidArgs,
                "Google NOT_FOUND (HTTP 404): Not Found"
            ))
        );
        // A 403 at either step names the scope set the whole tool needs.
        let denied = google(
            403,
            "PERMISSION_DENIED",
            "Request had insufficient authentication scopes.",
        );
        for state in [&read, &row] {
            assert_eq!(
                answered(state, http(403, &denied)),
                Next::Fail(failure(
                    ErrorKind::Denied,
                    &format!(
                        "Google PERMISSION_DENIED (HTTP 403): Request had insufficient \
                         authentication scopes. {}",
                        scope_hint(Scope::CalendarWrite)
                    )
                )),
                "step {}",
                state.step
            );
        }
    }

    #[test]
    fn a_row_names_every_provided_tool_by_its_registry_name() {
        let state = create_state(create_args(json!({})));
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
        wire["tool"] = json!("CalendarCreate");
        let error = InFlight::decode(&serde_json::to_vec(&wire).unwrap()).unwrap_err();
        assert!(
            error.contains("no provided tool is named `CalendarCreate`"),
            "{error}"
        );
    }

    /// Every step `tool` issues a call at.
    fn steps(tool: Provided) -> Vec<u8> {
        match tool {
            Provided::CalendarCreate | Provided::CalendarUpdate => vec![1],
            Provided::CalendarDelete => vec![DELETE_READ_STEP, DELETE_STEP],
        }
    }

    /// Every step that completes on any 2xx, an empty one included, and so
    /// names no API it expected: `a_delete_is_accepted_on_any_2xx` holds the
    /// event's delete.
    const ACCEPTS_EVERY_2XX: [(Provided, u8); 1] = [(Provided::CalendarDelete, DELETE_STEP)];

    #[test]
    fn every_step_names_its_scope() {
        for tool in Provided::ALL {
            for step in steps(tool) {
                assert_eq!(
                    scope(tool, step),
                    Scope::CalendarWrite,
                    "{} step {step}",
                    tool.name()
                );
            }
        }
    }

    #[test]
    fn argument_caps_count_characters_not_bytes() {
        admits_characters(create, create_args(json!({})), "account", ACCOUNT_MAX_CHARS);
    }

    /// Every argument `tool` takes, with the bound its parser holds it to,
    /// and the arguments it requires: the parser's own constants and key
    /// lists, as `every_tools_schema_states_what_its_parser_enforces` holds
    /// the spec to them. Five parser rules are outside what a schema states,
    /// each leaving the schema wider than the parser and each in the
    /// description: an event write's `attendees` hold at most
    /// `ATTENDEES_MAX_BYTES`, as `every_description_states_its_parsers_budgets`
    /// holds; and four are cross-field — an event write's `start` and `end`
    /// are both dates or both times, an update gives them together and
    /// `time_zone` only with them, a recurring timed event names its
    /// `time_zone`, and an update names a field to change, as
    /// `event_times_are_both_dates_or_both_times`,
    /// `an_update_moves_start_and_end_together`,
    /// `a_recurring_timed_event_needs_a_time_zone` and
    /// `an_update_names_a_field` hold. The per-value probes are this
    /// component's, in `workspace_probes`.
    fn bounds(tool: Provided) -> ToolBounds<'static> {
        let (keys, required): (&'static [&'static str], &'static [&'static str]) = match tool {
            Provided::CalendarCreate => (EventWriteArgs::CREATE_KEYS, &["summary", "start", "end"]),
            Provided::CalendarUpdate => (EventWriteArgs::UPDATE_KEYS, &["id"]),
            Provided::CalendarDelete => (DeleteArgs::KEYS, &["id"]),
        };
        let text = |max_chars| Bound::Text {
            max_chars,
            format: None,
        };
        let property = |key: &'static str| {
            let bound = match (tool, key) {
                (_, "account") => text(ACCOUNT_MAX_CHARS),
                (_, "start" | "end") => text(TIME_MAX_CHARS),
                (_, "summary" | "location") => text(EVENT_FIELD_MAX_CHARS),
                (_, "description") => text(EVENT_DESCRIPTION_MAX_CHARS),
                (_, "time_zone") => text(EVENT_TIME_ZONE_MAX_CHARS),
                (_, "attendees") => Bound::Array {
                    min: usize::from(tool == Provided::CalendarCreate),
                    max: ATTENDEES_MAX,
                    unique: true,
                    item: Box::new(Bound::Pattern(calendar_id_pattern())),
                },
                (_, "recurrence") => Bound::Array {
                    min: 1,
                    max: RECURRENCE_WRITE_MAX,
                    unique: true,
                    item: Box::new(Bound::Pattern(printable_ascii_pattern(
                        RECURRENCE_LINE_MAX_CHARS,
                    ))),
                },
                (_, "notify") => Bound::Enum(NOTIFY.to_vec()),
                (_, "calendar") => Bound::Pattern(calendar_id_pattern()),
                (_, "series") => Bound::Bool,
                (Provided::CalendarUpdate | Provided::CalendarDelete, "id") => Bound::Token {
                    max_chars: EVENT_ID_MAX_CHARS,
                },
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
            Provided::CalendarDelete => json!({ "id": "e1" }),
            Provided::CalendarCreate => create_args(json!({})),
            Provided::CalendarUpdate => update_args(json!({})),
        }
    }

    /// The probes of the arguments whose bounds are this component's, or
    /// whose parser holds a rule past the kit's bound: the calendar id, and
    /// an event write's attendees and recurrence lines. `None` where the
    /// kit's probes for the bound hold.
    fn workspace_probes(tool: Provided, key: &str) -> Option<Probes> {
        Some(match (tool, key) {
            (_, "calendar") => calendar_id_probes(),
            (_, "attendees") => {
                let min = usize::from(tool == Provided::CalendarCreate);
                let emails = |n: usize| {
                    json!(
                        (0..n)
                            .map(|i| format!("a{i}@example.com"))
                            .collect::<Vec<_>>()
                    )
                };
                // Ids of four-byte characters, `n` of them at `len`.
                let wide_ids = |n: usize, len: usize| {
                    json!((0..n).map(|i| four_byte(i, len)).collect::<Vec<_>>())
                };
                let at_budget = ATTENDEES_MAX_BYTES / (4 * CALENDAR_ID_MAX_CHARS);
                let mut probes = Probes {
                    admitted: vec![
                        emails(1),
                        emails(ATTENDEES_MAX),
                        // The budget's bytes, at the most attendees.
                        wide_ids(at_budget, CALENDAR_ID_MAX_CHARS),
                    ],
                    refused: vec![
                        emails(ATTENDEES_MAX + 1),
                        json!(["a@example.com", "a@example.com"]),
                        json!([""]),
                        json!(["a b@example.com"]),
                        json!(["a/b"]),
                        json!([1]),
                        json!("a@example.com"),
                        // The byte budget the schema cannot state.
                        wide_ids(ATTENDEES_MAX, CALENDAR_ID_MAX_CHARS / 2),
                    ],
                };
                if min == 0 {
                    probes.admitted.push(json!([]));
                } else {
                    probes.refused.push(json!([]));
                }
                probes
            }
            (_, "recurrence") => {
                let line = |n: usize| "R".repeat(n);
                Probes {
                    admitted: vec![
                        json!(["RRULE:FREQ=WEEKLY;BYDAY=MO"]),
                        json!(
                            (0..RECURRENCE_WRITE_MAX)
                                .map(|i| format!("{i}{}", line(RECURRENCE_LINE_MAX_CHARS - 1)))
                                .collect::<Vec<_>>()
                        ),
                        json!(["\"\\~ "]),
                    ],
                    refused: vec![
                        json!([]),
                        json!(
                            (0..=RECURRENCE_WRITE_MAX)
                                .map(|i| format!("RRULE:COUNT={i}"))
                                .collect::<Vec<_>>()
                        ),
                        json!(["RRULE:FREQ=DAILY", "RRULE:FREQ=DAILY"]),
                        json!([line(RECURRENCE_LINE_MAX_CHARS + 1)]),
                        json!([""]),
                        json!(["RRULE:FREQ=DAILY\r\nX"]),
                        json!(["é"]),
                        json!([1]),
                        json!("RRULE:FREQ=DAILY"),
                    ],
                }
            }
            _ => return None,
        })
    }

    /// The component's specification, as the package ships it.
    fn spec_text() -> String {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../config/specs/google-calendar-write.brenn");
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
                let probes = workspace_probes(tool, key)
                    .or_else(|| probes(bound))
                    .unwrap_or_else(|| panic!("`{name}`'s `{key}` has no probes"));
                hold_probes(name, key, &probes, &base, plan_with);
            }
        }
    }

    /// What an event write's description says of `attendees`, which may
    /// hold `min` of them or more.
    fn attendees_budget(min: usize) -> String {
        format!(
            "`attendees` is a list of {min} to {ATTENDEES_MAX} distinct email addresses; \
             together they may hold at most {ATTENDEES_MAX_BYTES} bytes of UTF-8, and more is \
             refused"
        )
    }

    /// What an event write's description says of each `recurrence` line.
    fn recurrence_bound() -> String {
        format!(
            "`recurrence` is 1 to {RECURRENCE_WRITE_MAX} RRULE, RDATE or EXDATE lines as Google \
             writes them (`RRULE:FREQ=WEEKLY;BYDAY=MO`), each 1 to {RECURRENCE_LINE_MAX_CHARS} \
             printable ASCII characters"
        )
    }

    /// The parser budgets no schema can state: each tool's description gives
    /// the number its parser's constant holds.
    #[test]
    fn every_description_states_its_parsers_budgets() {
        let spec = spec_text();
        for (tool, budget) in [
            (Provided::CalendarCreate, attendees_budget(1)),
            (Provided::CalendarUpdate, attendees_budget(0)),
            (Provided::CalendarCreate, recurrence_bound()),
            (Provided::CalendarUpdate, recurrence_bound()),
        ] {
            assert!(
                description(&spec, tool.name()).contains(&budget),
                "`{}`'s description must say {budget:?}",
                tool.name()
            );
        }
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
        let at = |mut state: InFlight, step: u8| {
            state.step = step;
            state
        };
        let name = |state: &InFlight| format!("{} step {}", state.tool.name(), state.step);

        // A delete's read of its event: the rendering alone.
        let state = delete_state(json!({ "id": "e1" }));
        for (outcome, why) in outcomes() {
            assert_eq!(
                answered(&state, outcome),
                Next::Fail(why.failure(google::API_NAME, None)),
                "{}",
                name(&state)
            );
        }

        // A write: the rendering and its hint.
        for (state, hint, wording) in [
            (
                write_step_state(Provided::CalendarCreate, 1),
                CREATE_UNKNOWN_HINT,
                "; the event may have been created before its answer was lost: look for it with \
                 CalendarEvents in its window before asking again, or the user has two",
            ),
            (
                write_step_state(Provided::CalendarUpdate, 1),
                UPDATE_UNKNOWN_HINT,
                "; the change may already have been applied before its answer was lost: \
                 CalendarEvent shows the event as it is, and asking again with the same \
                 arguments is safe",
            ),
            (
                write_step_state(Provided::CalendarDelete, DELETE_STEP),
                DELETE_UNKNOWN_HINT,
                "; the event may already be deleted before its answer was lost: asking again is \
                 safe, since a deleted event is refused as already cancelled",
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
            assert_eq!(
                answered(&at(create_state(create_args(json!({}))), 2), outcome),
                Next::Fail(no_such_step(Provided::CalendarCreate, 2))
            );
        }
    }

    /// What `tool`'s step `step` expects Google to answer, as a no-body
    /// refusal names it.
    fn no_body_answers(tool: Provided, step: u8) -> &'static str {
        match (tool, step) {
            (Provided::CalendarCreate | Provided::CalendarUpdate, 1) => "the event",
            _ => "JSON",
        }
    }

    #[test]
    fn an_empty_answer_is_internal_at_every_step() {
        for tool in Provided::ALL {
            for step in steps(tool) {
                if ACCEPTS_EVERY_2XX.contains(&(tool, step)) {
                    continue;
                }
                let mut state = create_state(create_args(json!({})));
                state.tool = tool;
                state.step = step;
                for status in [200, 204] {
                    let Next::Fail(Failure { kind, detail }) = answered(&state, http(status, ""))
                    else {
                        panic!("{} step {step} accepted no body", tool.name());
                    };
                    assert_eq!(kind, ErrorKind::Internal);
                    let answers = no_body_answers(tool, step);
                    let expected = match write_hint(tool, step) {
                        Some(hint) => format!("{answers}; {hint}"),
                        None => answers.to_string(),
                    };
                    assert_eq!(
                        no_body_parts(&detail, status).map(|(_, rest)| rest),
                        Some(expected.as_str()),
                        "{detail}"
                    );
                }
            }
        }
    }

    #[test]
    fn a_step_names_one_api_for_no_body_and_for_a_bad_shape() {
        let bases = [
            create_state(create_args(json!({}))),
            update_state(update_args(json!({}))),
            delete_state(json!({ "id": "e1" })),
        ];
        for tool in Provided::ALL {
            assert!(
                bases.iter().any(|state| state.tool == tool),
                "{} has no state here",
                tool.name()
            );
        }
        let detail = |next: Next| match next {
            Next::Fail(Failure { detail, .. }) => detail,
            other => panic!("{other:?}"),
        };
        for base in &bases {
            for step in steps(base.tool) {
                if ACCEPTS_EVERY_2XX.contains(&(base.tool, step)) {
                    continue;
                }
                let mut state = base.clone();
                state.step = step;
                let name = format!("{} step {step}", state.tool.name());
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
    }

    #[test]
    fn every_tools_first_read_has_an_unreadable_answer() {
        // `unreadable` runs only on a cut or refused read, so a tool missing
        // its arm there would pass every other test and be answered as a
        // step it does not have.
        for tool in Provided::ALL {
            let mut state = create_state(create_args(json!({})));
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

    /// The row of `tool` at its write step `step`, as `write_hint` names it.
    fn write_step_state(tool: Provided, step: u8) -> InFlight {
        let state = match (tool, step) {
            (Provided::CalendarCreate, 1) => create_state(create_args(json!({}))),
            (Provided::CalendarUpdate, 1) => update_state(update_args(json!({}))),
            (Provided::CalendarDelete, DELETE_STEP) => {
                delete_after_read(json!({ "id": "e1" }), &delete_read(json!({}))).0
            }
            _ => panic!("no row for {} step {step}", tool.name()),
        };
        assert_eq!((state.tool, state.step), (tool, step));
        state
    }

    #[test]
    fn every_write_steps_failure_carries_its_hint_only_when_google_may_have_acted() {
        let mut write_steps = 0;
        for tool in Provided::ALL {
            for step in steps(tool) {
                let Some(hint) = write_hint(tool, step) else {
                    continue;
                };
                write_steps += 1;
                let state = write_step_state(tool, step);
                let at = format!("{} step {step}", tool.name());
                let hinted = |detail: &str| Next::Fail(internal(format!("{detail}; {hint}")));
                for (outcome, detail) in [
                    (
                        nested("invalid_args", "bad wrapper"),
                        "api-request rejected the wrapper's request: bad wrapper",
                    ),
                    (
                        nested("rate_limited", "slow down"),
                        "rate_limited: slow down",
                    ),
                    (
                        http(200, "<html>"),
                        "Google returned HTTP 200 with a body that is not JSON",
                    ),
                    (
                        http(302, ""),
                        "Google answered 302 with a redirect, which Brenn does not follow",
                    ),
                ] {
                    assert_eq!(answered(&state, outcome), hinted(detail), "{at}");
                }
                for outcome in [
                    http(500, "{}"),
                    http(503, "{}"),
                    http(429, "{}"),
                    cut_read(json!(300000)),
                ] {
                    let Next::Fail(failed) = answered(&state, outcome) else {
                        panic!("{at} fails");
                    };
                    assert_eq!(failed.kind, ErrorKind::Internal, "{at}");
                    assert!(
                        failed.detail.ends_with(&format!("; {hint}")),
                        "{at}: {failed:?}"
                    );
                }
                // Google's refusal, or `api-request`'s, changed nothing: its
                // kind and message, no hint.
                let denied = format!(
                    "Google PERMISSION_DENIED (HTTP 403): Insufficient Permission. {}",
                    scope_hint(scope(tool, step))
                );
                for (outcome, kind, detail) in [
                    (
                        http(400, &google(400, "INVALID_ARGUMENT", "Invalid value")),
                        ErrorKind::InvalidArgs,
                        "Google INVALID_ARGUMENT (HTTP 400): Invalid value",
                    ),
                    (
                        http(
                            403,
                            &google(403, "PERMISSION_DENIED", "Insufficient Permission"),
                        ),
                        ErrorKind::Denied,
                        denied.as_str(),
                    ),
                    (
                        http(404, &google(404, "NOT_FOUND", "Not Found")),
                        ErrorKind::InvalidArgs,
                        "Google NOT_FOUND (HTTP 404): Not Found",
                    ),
                ] {
                    assert_eq!(
                        answered(&state, outcome),
                        Next::Fail(failure(kind, detail)),
                        "{at}"
                    );
                }
                let Next::Fail(refused) = answered(&state, nested("denied", "no clause admits it"))
                else {
                    panic!("{at} fails");
                };
                assert_eq!(refused.kind, ErrorKind::Denied, "{at}");
                assert!(
                    refused
                        .detail
                        .starts_with("api-request refused the nested ")
                        && refused.detail.ends_with(": no clause admits it"),
                    "{at}: {refused:?}"
                );
            }
        }
        assert!(write_steps > 0);
        // A read step's failure carries no hint.
        assert_eq!(
            answered(
                &delete_state(json!({ "id": "e1" })),
                nested("invalid_args", "bad wrapper")
            ),
            Next::Fail(internal(
                "api-request rejected the wrapper's request: bad wrapper"
            ))
        );
    }
}
