//! Google Calendar read (brenn:processor world): `CalendarList`,
//! `CalendarEvents`, `CalendarEvent` and `CalendarFreeBusy`.
//!
//! Each activation reads the instance's `config` and hands the activation to
//! `brenn-provider-kit`'s loop. At most `CALL_BUDGET` nested calls go out per
//! activation.
//!
//! Serving a request validates its arguments and plans its one nested
//! `api-request` for the caller's user; answering its result completes the
//! request from `api-request`'s answer. A request's own problem — bad
//! arguments, Brenn's refusal, Google's error — is answered with
//! `complete_err`, and a missing credential is relayed as `api-request`
//! answered it; only a structural fault (a bad `config`, a store failure, a
//! malformed envelope) fails the activation. Log lines name tool, step,
//! method, host, path, status, size and truncation — never a body, a header or
//! the query.

mod logic;
mod spec;

use brenn_google_workspace::settings::Settings;
use brenn_guest::{Activation, Error, Processor};

use crate::spec::config;

struct GoogleCalendarRead;

impl Processor for GoogleCalendarRead {
    fn receive(activation: Activation) -> Result<Option<String>, Error> {
        let settings =
            Settings::from_lookup(config::get, "calendar_host").map_err(Error::failed)?;
        brenn_provider_kit_guest::serve::<logic::CalendarReads>(&activation, &settings)?;
        Ok(None)
    }
}

brenn_guest::export_processor!(GoogleCalendarRead);
