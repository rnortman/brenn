//! A provider component's core: the parts a WASM component that serves
//! `provide`d tools by issuing nested `api-request` calls needs. Everything
//! but [`google`] is provider-neutral: the text the model sees names the API
//! by the display name the component passes.
//!
//! - [`activation`]: an activation loop over a [`Host`] trait, issuing at
//!   most [`CALL_BUDGET`] nested calls per activation and keeping the rest in
//!   a backlog that later activations drain.
//! - [`backlog`]: the backlog row a queued request keeps.
//! - [`row`]: the in-flight row a request keeps in the component's store
//!   between the activation that issues a nested call and the one that
//!   receives its answer, generic over the component's [`Tool`] enum.
//! - [`call`]: the request's error answer, the nested call's arguments and
//!   what a step decides next.
//! - [`args`]: reading a tool's JSON arguments.
//! - [`answer`]: reading `api-request`'s answer envelope, bounding what is
//!   copied out of it, and the result ceiling.
//! - [`google`]: Google's display name and the detail of its standard error
//!   shape, shared by the components that call Google APIs.
//! - [`settings`]: reading an instance's `config` values.
//! - `schema` (behind the `testutils` feature): a host test's check that each
//!   tool's `input_schema` states exactly the bounds its argument parser
//!   enforces, the argument values that probe the parser against each
//!   bound, and the assertions that hold the parser to them. It panics by
//!   design: it serves host tests, never a component step.
//! - `testutil` (behind the `testutils` feature): fixtures for a step's tests
//!   — `api-request` answers, nested-call errors, Google error bodies and
//!   error answers — and `MemoryHost`, an in-memory [`Host`] for a loop's
//!   tests.
//!
//! The crate is `std` plus `serde`/`serde_json` and names no guest SDK, so all
//! of it runs on the host under test; `brenn-provider-kit-guest` is where the
//! guest SDK is named, and serves the loop through it. Each component keeps
//! its own tools, settings keys and status-to-kind mapping.
//!
//! The kit is a build-time contract for out-of-tree provider components, cut
//! at the consumer's pin as `docs/component-packages.md`, "Contract evolution",
//! states. The host tree and the guest workspace share its sources (the guest
//! workspace reaches them through `brenn-wasm/components/host-crates/`).

pub mod activation;
pub mod answer;
pub mod args;
pub mod backlog;
pub mod call;
pub mod google;
pub mod row;
#[cfg(feature = "testutils")]
pub mod schema;
pub mod settings;
#[cfg(any(test, feature = "testutils"))]
pub mod testutil;

pub use activation::{
    BACKLOG_MAX_WAIT_MS, CALL_BUDGET, Delivered, HOST_DEADLINE_MS, Host, INFLIGHT_TTL_MS, Input,
    Level, Logic, NESTED_WORST_CASE_MS, Request, Rows, serve,
};
pub use answer::{
    Answer, COMPACT_RESULT_CEILING, NestedError, Unreadable, bounded, copy, cut, read_api_result,
    serialized_len,
};
pub use args::{Args, capped, choice, flag, integer, object};
pub use backlog::Queued;
pub use call::{
    ApiCall, Call, ErrorKind, Failure, Next, Query, QueryValue, Target, internal, query,
};
pub use row::{InFlight, Tool, no_such_step, stored};
#[cfg(feature = "testutils")]
pub use schema::{
    Bound, Probes, ToolBounds, check, hold_probes, hold_required, input_schema, probes,
};
pub use settings::{host, required};

/// A two-tool enum standing in for a component's generated `Provided`.
#[cfg(test)]
pub(crate) mod fixture {
    use crate::Tool;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Fixture {
        Lookup,
        Search,
    }

    impl Fixture {
        pub const ALL: [Fixture; 2] = [Fixture::Lookup, Fixture::Search];
    }

    impl Tool for Fixture {
        fn name(self) -> &'static str {
            match self {
                Fixture::Lookup => "fixture-lookup",
                Fixture::Search => "fixture-search",
            }
        }

        fn from_name(name: &str) -> Option<Fixture> {
            Fixture::ALL.into_iter().find(|tool| tool.name() == name)
        }
    }
}
