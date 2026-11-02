//! Google Workspace tools (brenn:processor world).
//!
//! Each activation reads the instance's `config` and hands the activation to
//! `brenn-provider-kit`'s loop, which runs it in one store transaction: it
//! sweeps rows no result can still arrive for, answers every new result,
//! issues queued requests, serves every new request, and commits. At most
//! `CALL_BUDGET` nested calls go out per activation; requests past it wait in
//! the store's backlog for a later activation.
//!
//! Serving a request validates its arguments and plans its first nested
//! `api-request` for the caller's user; answering a result either completes
//! the request from `api-request`'s answer or plans its next step, so a
//! request has at most one nested call outstanding. A request's own problem —
//! bad arguments, Brenn's refusal, Google's error — is answered with
//! `complete_err`, and a missing credential is relayed as `api-request`
//! answered it; only a structural fault (a bad `config`, a store failure, a
//! malformed envelope) fails the activation. Log lines name tool, step,
//! method, host, path, status, size and truncation — never a body, a header or
//! the query, which carries the user's search text.

mod logic;
mod spec;

use brenn_guest::{Activation, Error, Processor};

use crate::logic::Settings;
use crate::spec::config;

struct GoogleWorkspaceTools;

impl Processor for GoogleWorkspaceTools {
    fn receive(activation: Activation) -> Result<Option<String>, Error> {
        let settings = Settings::from_lookup(config::get).map_err(Error::failed)?;
        brenn_provider_kit_guest::serve::<logic::Workspace>(&activation, &settings)?;
        Ok(None)
    }
}

brenn_guest::export_processor!(GoogleWorkspaceTools);
