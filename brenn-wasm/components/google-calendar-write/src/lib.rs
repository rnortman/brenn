//! Google Calendar write (brenn:processor world): `CalendarCreate`,
//! `CalendarUpdate` and `CalendarDelete`.
//!
//! Each activation reads the instance's `config` and hands the activation to
//! `brenn-provider-kit`'s loop. At most `CALL_BUDGET` nested calls go out per
//! activation.
//!
//! Serving a request validates its arguments and plans its first nested
//! `api-request` for the caller's user — an event's `POST` or `PATCH`, or a
//! delete's read of the event; answering a result either completes the
//! request from `api-request`'s answer or plans the delete's `DELETE`, so a
//! request has at most one nested call outstanding. The loop is served with
//! the SDK's `blobs`: the body of an event's `POST` or `PATCH` is the event
//! the component builds from the request's arguments, which keeps it out of
//! the nested call's arguments and their cap. A request's own problem —
//! bad arguments, Brenn's refusal, Google's error — is answered with
//! `complete_err`, and a missing credential is relayed as `api-request`
//! answered it; only a structural fault (a bad `config`, a store failure, a
//! malformed envelope) fails the activation. Log lines name tool, step,
//! method, host, path, status, size and truncation — never a body, a header or
//! the query.

mod logic;
mod spec;

use brenn_google_workspace::settings::Settings;
use brenn_guest::{Activation, Body, Error, Processor};

use crate::spec::config;

struct GoogleCalendarWrite;

impl Processor for GoogleCalendarWrite {
    fn receive(activation: Activation) -> Result<Option<Body>, Error> {
        let settings =
            Settings::from_lookup(config::get, "calendar_host").map_err(Error::failed)?;
        brenn_provider_kit_guest::serve_with_blobs::<logic::CalendarWrites>(
            &activation,
            &settings,
        )?;
        Ok(None)
    }
}

brenn_guest::export_processor!(GoogleCalendarWrite);
