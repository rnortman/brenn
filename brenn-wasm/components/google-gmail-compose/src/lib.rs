//! Google Gmail compose (brenn:processor world): `GmailDraft`.
//!
//! Each activation reads the instance's `config` and hands the activation to
//! `brenn-provider-kit`'s loop. At most `CALL_BUDGET` nested calls go out per
//! activation.
//!
//! Serving a request validates its arguments and plans its first nested
//! `api-request` for the caller's user — a reply's read of its original, or
//! the draft's `POST`; answering a result either completes the request from
//! `api-request`'s answer or plans the `POST`, so a request has at most one
//! nested call outstanding. A request's own problem — bad arguments, Brenn's
//! refusal, Google's error — is answered with `complete_err`, and a missing
//! credential is relayed as `api-request` answered it; only a structural fault
//! (a bad `config`, a store failure, a malformed envelope) fails the
//! activation. Log lines name tool, step, method, host, path, status, size and
//! truncation — never a body, a header or the query.

mod logic;
mod spec;

use brenn_google_workspace::settings::Settings;
use brenn_guest::{Activation, Error, Processor};

use crate::spec::config;

struct GoogleGmailCompose;

impl Processor for GoogleGmailCompose {
    fn receive(activation: Activation) -> Result<Option<String>, Error> {
        let settings = Settings::from_lookup(config::get, "gmail_host").map_err(Error::failed)?;
        brenn_provider_kit_guest::serve::<logic::GmailCompose>(&activation, &settings)?;
        Ok(None)
    }
}

brenn_guest::export_processor!(GoogleGmailCompose);
