//! What every Calendar component shares: paths, id validators, bounds, and
//! the compaction of one event as events.get, insert and patch answer it.

use brenn_provider_kit::{Args, Failure, capped, cut};
use serde_json::{Value, json};

use crate::args::{MAX_CHARS_DEFAULT, id_arg, is_token};
use crate::text::{plain, put_cut_text, shape};

/// The most UTF-8 bytes of an email address copied into an event answer (the
/// organizer's and each attendee's). The same number as `ACCOUNT_MAX_CHARS`,
/// counted in bytes because it sizes the answer against the byte-counted
/// result ceiling.
pub const ACCOUNT_MAX_BYTES: usize = 320;

/// The path Calendar lists a calendar's events below: `{CALENDARS_PATH}/{id}/events`.
pub const CALENDARS_PATH: &str = "/calendar/v3/calendars";

/// A calendar's events collection. The calendar id goes into the path as it
/// is; `api-request` percent-encodes it.
pub fn events_path(calendar: &str) -> String {
    format!("{CALENDARS_PATH}/{calendar}/events")
}

/// One event of a calendar, by `events_path`'s rule.
pub fn event_path(calendar: &str, id: &str) -> String {
    format!("{}/{id}", events_path(calendar))
}
/// The calendar a tool reads when the caller names none.
pub const PRIMARY_CALENDAR: &str = "primary";

/// events.get's mask: one event's own text, its times, organizer, attendees,
/// recurrence and conference link.
pub const EVENT_GET_FIELDS: &str = "id,status,summary,description,location,start,end,\
                                organizer(email,displayName,self),\
                                attendees(email,displayName,responseStatus,optional,self),\
                                attendeesOmitted,recurrence,recurringEventId,hangoutLink,\
                                conferenceData/entryPoints(entryPointType,uri)";
/// events.get's `maxAttendees`: past it Google returns only the calendar
/// read's own attendee entry and marks the event `attendeesOmitted`. It bounds the read
/// and the answer alike, and is the most attendees an event write may name.
pub const ATTENDEES_MAX: usize = 50;
/// The most UTF-8 bytes of one event's text field (`summary`, `location`, the
/// organizer's name, a recurrence line, the conference link) copied into an
/// answer.
pub const EVENT_TEXT_MAX_BYTES: usize = 1000;
/// The most recurrence lines an event answers; past it `recurrence_truncated`.
pub const RECURRENCE_MAX: usize = 20;
/// The attendee responses Google writes, and the names they are counted under.
pub const RESPONSES: &[(&str, &str)] = &[
    ("accepted", "accepted"),
    ("declined", "declined"),
    ("tentative", "tentative"),
    ("needsAction", "needs_action"),
];

/// The most characters a calendar id argument may hold.
pub const CALENDAR_ID_MAX_CHARS: usize = 320;
/// The most bytes of a calendar id Google answers in `calendarList.list`.
/// Counted in bytes because it sizes the listing against the result ceiling;
/// no more than `CALENDAR_ID_MAX_CHARS`, as asserted below, so a listed id is
/// one `calendar` admits back.
pub const CALENDAR_ID_MAX_BYTES: usize = 320;
const _: () = assert!(CALENDAR_ID_MAX_BYTES <= CALENDAR_ID_MAX_CHARS);
/// The most characters an event id may hold: Google's base32hex ids are at
/// most 1 024 characters, an instance's suffix included. Being ASCII, an id's
/// characters are its bytes.
pub const EVENT_ID_MAX_CHARS: usize = 1024;
/// The most characters a time argument may hold. Times are passed to Google
/// as given; Google refuses one that is not RFC 3339 with an offset.
pub const TIME_MAX_CHARS: usize = 64;
/// The most UTF-8 bytes of text copied into one line of a listing.
pub const LISTING_TEXT_MAX_BYTES: usize = 300;
/// The most UTF-8 bytes of a value from Google's own vocabulary (a role, a
/// time zone, a timestamp) copied into an answer. No honest value reaches
/// it; it exists so an answer's size is bounded, and a cut is not flagged.
pub const SHORT_MAX_BYTES: usize = 64;

/// Whether `id` has a calendar id's shape, whatever its length: one path
/// segment `api-request` takes, so not `.` or `..`, with no `/`, `\`, `%` or
/// `"`, and no control or whitespace character. `#`, `@`, `?`, `.` and `+` are
/// content.
pub fn calendar_id_shape(id: &str) -> bool {
    id != "."
        && id != ".."
        && !id
            .chars()
            .any(|c| matches!(c, '/' | '\\' | '%' | '"') || c.is_control() || c.is_whitespace())
}

/// Whether `id` is a calendar id argument: 1 to `CALENDAR_ID_MAX_CHARS`
/// characters of a calendar id's shape. Arguments are counted in characters,
/// as the tool's `input_schema` states them; an answer's ids are held to
/// `is_listed_calendar_id`.
pub fn is_calendar_id(id: &str) -> bool {
    (1..=CALENDAR_ID_MAX_CHARS).contains(&id.chars().count()) && calendar_id_shape(id)
}

/// Whether `id` is a calendar id Google may answer in `calendarList.list`: 1
/// to `CALENDAR_ID_MAX_BYTES` bytes of a calendar id's shape. Counted in bytes
/// so a listing's size stays bounded against the result ceiling.
pub fn is_listed_calendar_id(id: &str) -> bool {
    (1..=CALENDAR_ID_MAX_BYTES).contains(&id.len()) && calendar_id_shape(id)
}

/// What `is_calendar_id` admits, as a refusal states it.
pub fn calendar_id_rule() -> String {
    format!(
        "a calendar id of 1 to {CALENDAR_ID_MAX_CHARS} characters with no `/`, `\\`, `%`, \
         `\"`, space or control character"
    )
}

/// What `is_calendar_id` admits, as the `pattern` an `input_schema` states it.
/// `\x00-\x1f` with `\x7f-\x9f` is `char::is_control`, and ECMA-262's `\s`
/// is Unicode whitespace less U+0085 (a control character, refused anyway)
/// plus U+FEFF. Two differences remain, both leaving the schema narrower than
/// the parser: an id holding U+FEFF is refused, and, the quantifier counting
/// UTF-16 units without the `u` flag, an id of characters outside the Basic
/// Multilingual Plane is admitted only up to half the length. Tests compare
/// this text with the spec's and never run it as a regex, so these claims
/// about what it matches are checked by reading, not by a test.
pub fn calendar_id_pattern() -> String {
    format!(r#"^(?!\.\.?$)[^\s/\\%"\x00-\x1f\x7f-\x9f]{{1,{CALENDAR_ID_MAX_CHARS}}}$"#)
}

/// A calendar id argument, refused here rather than by `api-request` so a bad
/// id is the caller's to fix.
pub fn calendar_id(args: &Args, key: &str) -> Result<Option<String>, String> {
    id_arg(args, key, is_calendar_id, calendar_id_rule)
}

/// Whether `id` is an event id as Google writes them: 1 to
/// `EVENT_ID_MAX_CHARS` letters, digits, `_` and `-`.
pub fn is_event_id(id: &str) -> bool {
    is_token(id, EVENT_ID_MAX_CHARS)
}

/// An event id argument: one path segment of letters, digits, `_` and `-`.
pub fn event_id(args: &Args, key: &str) -> Result<Option<String>, String> {
    id_arg(args, key, is_event_id, || {
        format!("1 to {EVENT_ID_MAX_CHARS} letters, digits, `_` or `-`")
    })
}

/// A time argument, passed to Google as given.
pub fn time(args: &Args, key: &str) -> Result<Option<String>, String> {
    Ok(capped(args, key, TIME_MAX_CHARS)?.map(str::to_string))
}

/// One compacted object of a Calendar answer: the fields copied so far, and
/// the names of those cut at a bound that is flagged.
pub struct Compact<'a> {
    pub item: &'a Args,
    pub api: &'a str,
    pub out: Args,
    pub truncated: Vec<String>,
}

impl<'a> Compact<'a> {
    pub fn of(item: &'a Value, api: &'a str) -> Result<Compact<'a>, Failure> {
        let Value::Object(item) = item else {
            return Err(shape(api));
        };
        Ok(Compact {
            item,
            api,
            out: Args::new(),
            truncated: Vec::new(),
        })
    }

    /// Google's string `key`, when present; any other value is Google's
    /// fault.
    pub fn string(&self, key: &str) -> Result<Option<&'a str>, Failure> {
        match self.item.get(key) {
            None => Ok(None),
            Some(Value::String(value)) => Ok(Some(value)),
            Some(_) => Err(shape(self.api)),
        }
    }

    /// Google's id `key` under `name`, required and held to `valid`: an id is
    /// a handle the caller passes back, so it is never cut.
    pub fn id(&mut self, key: &str, name: &str, valid: fn(&str) -> bool) -> Result<(), Failure> {
        match self.string(key)? {
            Some(id) if valid(id) => {
                self.out.insert(name.to_string(), json!(id));
                Ok(())
            }
            _ => Err(shape(self.api)),
        }
    }

    /// Google's text `key` under `name`, through `plain` and cut to `bound`
    /// UTF-8 bytes; a cut is listed in `fields_truncated`.
    pub fn text(&mut self, key: &str, name: &str, bound: usize) -> Result<(), Failure> {
        if let Some(value) = self.string(key)? {
            self.put_text(name, value, bound);
        }
        Ok(())
    }

    /// `value` under `name`, through `plain` and cut to `bound` UTF-8 bytes;
    /// a cut is listed in `fields_truncated`.
    pub fn put_text(&mut self, name: &str, value: &str, bound: usize) {
        let (value, cut) = cut(&plain(value), bound);
        if cut {
            self.truncated.push(name.to_string());
        }
        self.out.insert(name.to_string(), json!(value));
    }

    /// Google's object `key`, when present; any other value is Google's
    /// fault.
    pub fn object(&self, key: &str) -> Result<Option<Compact<'a>>, Failure> {
        match self.item.get(key) {
            None => Ok(None),
            Some(value) => Compact::of(value, self.api).map(Some),
        }
    }

    /// Google's list `key`; Google omits a list with nothing in it, so
    /// absent is empty. Any other value is Google's fault.
    pub fn array(&self, key: &str) -> Result<&'a [Value], Failure> {
        match self.item.get(key) {
            None => Ok(&[]),
            Some(Value::Array(items)) => Ok(items),
            Some(_) => Err(shape(self.api)),
        }
    }

    /// Google's vocabulary `key` under `name`; see `put_short`.
    pub fn short(&mut self, key: &str, name: &str) -> Result<(), Failure> {
        if let Some(value) = self.string(key)? {
            self.put_short(name, value);
        }
        Ok(())
    }

    /// Vocabulary `value` under `name`, through `plain` and cut to
    /// `SHORT_MAX_BYTES` unflagged.
    pub fn put_short(&mut self, name: &str, value: &str) {
        let value = cut(&plain(value), SHORT_MAX_BYTES).0;
        self.out.insert(name.to_string(), json!(value));
    }

    /// Google's flag `key`: absent is `false`; a non-bool is Google's fault.
    pub fn flag(&self, key: &str) -> Result<bool, Failure> {
        match self.item.get(key) {
            None => Ok(false),
            Some(Value::Bool(flag)) => Ok(*flag),
            Some(_) => Err(shape(self.api)),
        }
    }

    /// Google's flag `key` under `name`, only when it is `true`.
    pub fn when_true(&mut self, key: &str, name: &str) -> Result<(), Failure> {
        if self.flag(key)? {
            self.out.insert(name.to_string(), json!(true));
        }
        Ok(())
    }

    /// Google's event time `key` under the same name, as Google gave it:
    /// `{ "date_time", "time_zone"? }` for a timed event, `{ "date" }` for an
    /// all-day one. One with both or neither is Google's fault.
    pub fn when(&mut self, key: &str) -> Result<(), Failure> {
        let Some(mut time) = self.object(key)? else {
            return Ok(());
        };
        time.short("dateTime", "date_time")?;
        time.short("timeZone", "time_zone")?;
        time.short("date", "date")?;
        if time.out.contains_key("date_time") == time.out.contains_key("date") {
            return Err(shape(self.api));
        }
        self.out.insert(key.to_string(), time.done());
        Ok(())
    }

    /// The object, with `fields_truncated` sorted when a field was cut.
    pub fn done(mut self) -> Value {
        if !self.truncated.is_empty() {
            self.truncated.sort();
            self.out
                .insert("fields_truncated".to_string(), json!(self.truncated));
        }
        Value::Object(self.out)
    }
}

/// The `responseStatus` of the attendee entry marked `self`, when Google
/// returned one; every other entry is ignored. Two `self` entries are
/// Google's fault.
pub fn self_response<'a>(event: &Compact<'a>) -> Result<Option<&'a str>, Failure> {
    let mut found = None;
    for attendee in event.array("attendees")? {
        let attendee = Compact::of(attendee, event.api)?;
        if !attendee.flag("self")? {
            continue;
        }
        if found.is_some() {
            return Err(shape(event.api));
        }
        found = Some(attendee.string("responseStatus")?);
    }
    Ok(found.flatten())
}

/// The `max_chars` a written event's answer cuts `description` at: a write
/// takes no `max_chars`, so `CalendarEvent`'s default.
pub fn written_event_max_chars() -> usize {
    usize::try_from(MAX_CHARS_DEFAULT).expect("MAX_CHARS_DEFAULT fits usize")
}

/// One event as events.get, insert or patch answers it: `id`, `summary`, `description` (cut at
/// `max_chars` characters, `description_chars` its whole length), `start`,
/// `end`, `location`, `status`, `organizer`, `self_response`, `attendees`
/// with their counts or `attendees_omitted`, `recurrence`,
/// `recurring_event_id` and `conference`. An event whose id is not the one
/// asked for (`expected`, when the caller knows it) is Google's fault, and
/// every shape failure names `api`, the method that answered.
pub fn compact_event(
    body: &Value,
    api: &str,
    expected: Option<&str>,
    max_chars: usize,
) -> Result<Value, Failure> {
    let mut event = Compact::of(body, api)?;
    if let Some(expected) = expected
        && event.string("id")? != Some(expected)
    {
        return Err(shape(api));
    }
    event.id("id", "id", is_event_id)?;
    event.text("summary", "summary", EVENT_TEXT_MAX_BYTES)?;
    if let Some(description) = event.string("description")? {
        put_cut_text(&mut event.out, "description", description, max_chars);
    }
    event.when("start")?;
    event.when("end")?;
    event.text("location", "location", EVENT_TEXT_MAX_BYTES)?;
    event.short("status", "status")?;
    if let Some(mut organizer) = event.object("organizer")? {
        organizer.text("email", "email", ACCOUNT_MAX_BYTES)?;
        organizer.text("displayName", "name", EVENT_TEXT_MAX_BYTES)?;
        organizer.when_true("self", "self")?;
        event.out.insert("organizer".to_string(), organizer.done());
    }
    if let Some(response) = self_response(&event)? {
        event.put_short("self_response", response);
    }
    attendees(&mut event)?;
    recurrence(&mut event)?;
    if event.item.contains_key("recurringEventId") {
        event.id("recurringEventId", "recurring_event_id", is_event_id)?;
    }
    if let Some(link) = conference(&event)? {
        event.put_text("conference", link, EVENT_TEXT_MAX_BYTES);
    }
    Ok(event.done())
}

/// The event's `attendees` as Google sent them, each `email`, `name`,
/// `response` and `optional` (only when true). With the whole list, its
/// length as `attendees_total` and its responses counted in
/// `attendee_responses`; when Google left attendees out, `attendees_omitted`
/// instead. More than `ATTENDEES_MAX` entries, or a response outside Google's
/// four in a list being counted, is Google's fault.
pub fn attendees(event: &mut Compact<'_>) -> Result<(), Failure> {
    let api = event.api;
    let attendees = event.array("attendees")?;
    if attendees.len() > ATTENDEES_MAX {
        return Err(shape(api));
    }
    let omitted = event.flag("attendeesOmitted")?;
    let mut counts = [0_usize; RESPONSES.len()];
    let mut entries = Vec::with_capacity(attendees.len());
    for attendee in attendees {
        let mut entry = Compact::of(attendee, api)?;
        entry.text("email", "email", ACCOUNT_MAX_BYTES)?;
        entry.text("displayName", "name", LISTING_TEXT_MAX_BYTES)?;
        let response = entry.string("responseStatus")?;
        if let Some(response) = response {
            entry.put_short("response", response);
        }
        entry.when_true("optional", "optional")?;
        if !omitted {
            let counted = RESPONSES
                .iter()
                .position(|(google, _)| Some(*google) == response)
                .ok_or_else(|| shape(api))?;
            counts[counted] += 1;
        }
        entries.push(entry.done());
    }
    if omitted {
        event
            .out
            .insert("attendees_omitted".to_string(), json!(true));
    } else {
        event
            .out
            .insert("attendees_total".to_string(), json!(entries.len()));
        let responses: Args = RESPONSES
            .iter()
            .zip(counts)
            .map(|((_, name), count)| (name.to_string(), json!(count)))
            .collect();
        event
            .out
            .insert("attendee_responses".to_string(), Value::Object(responses));
    }
    event
        .out
        .insert("attendees".to_string(), Value::Array(entries));
    Ok(())
}

/// The event's first `RECURRENCE_MAX` recurrence lines, each through `plain`
/// and cut to `EVENT_TEXT_MAX_BYTES` (a cut lists `recurrence` in
/// `fields_truncated`), and `recurrence_truncated` past them.
pub fn recurrence(event: &mut Compact<'_>) -> Result<(), Failure> {
    let lines = event.array("recurrence")?;
    if lines.is_empty() {
        return Ok(());
    }
    let mut cut_any = false;
    let mut kept = Vec::with_capacity(lines.len().min(RECURRENCE_MAX));
    for line in lines.iter().take(RECURRENCE_MAX) {
        let Value::String(line) = line else {
            return Err(shape(event.api));
        };
        let (line, was_cut) = cut(&plain(line), EVENT_TEXT_MAX_BYTES);
        cut_any |= was_cut;
        kept.push(line);
    }
    if cut_any {
        event.truncated.push("recurrence".to_string());
    }
    event.out.insert("recurrence".to_string(), json!(kept));
    if lines.len() > RECURRENCE_MAX {
        event
            .out
            .insert("recurrence_truncated".to_string(), json!(true));
    }
    Ok(())
}

/// The event's conference link: its `hangoutLink`, else the `uri` of the
/// first `video` entry point of its `conferenceData`.
pub fn conference<'a>(event: &Compact<'a>) -> Result<Option<&'a str>, Failure> {
    if let Some(link) = event.string("hangoutLink")? {
        return Ok(Some(link));
    }
    let Some(data) = event.object("conferenceData")? else {
        return Ok(None);
    };
    for point in data.array("entryPoints")? {
        let point = Compact::of(point, event.api)?;
        if point.string("entryPointType")? == Some("video") {
            return point.string("uri");
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_calendar_id_is_one_segment_api_request_admits() {
        for ok in [
            "primary",
            "alice@example.com",
            "en.usa#holiday@group.v.calendar.google.com",
            "c_0123?x+y@group.calendar.google.com",
            &"a".repeat(CALENDAR_ID_MAX_CHARS),
        ] {
            assert!(is_calendar_id(ok), "{ok:?}");
        }
        for bad in [
            "",
            ".",
            "..",
            "a/b",
            "a\\b",
            "a%40b",
            "a\"b",
            "a b",
            "a\tb",
            "a\u{0}b",
            "a\u{a0}b",
            &"a".repeat(CALENDAR_ID_MAX_CHARS + 1),
        ] {
            assert!(!is_calendar_id(bad), "{bad:?}");
        }
    }

    #[test]
    fn an_event_id_is_googles_base32hex_with_an_instance_suffix() {
        for ok in [
            "0a1b2c3d4e5f",
            "weekly0123456789_20261104T140000Z",
            &"a".repeat(EVENT_ID_MAX_CHARS),
        ] {
            assert!(is_event_id(ok), "{ok:?}");
        }
        for bad in ["", "a b", "a/b", "a.b", &"a".repeat(EVENT_ID_MAX_CHARS + 1)] {
            assert!(!is_event_id(bad), "{bad:?}");
        }
    }

    #[test]
    fn a_written_events_description_bound_is_an_event_reads_default() {
        assert_eq!(
            written_event_max_chars(),
            crate::args::max_chars(&Args::new()).unwrap()
        );
    }
}
