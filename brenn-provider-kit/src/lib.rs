//! A provider component's core: the parts a WASM component that serves
//! `provide`d tools by issuing nested `api-request` calls needs. The row, the
//! call, the argument helpers and the settings readers are provider-neutral;
//! the answer reading is shaped to Google APIs' answers and names Google in
//! the text the model sees.
//!
//! - [`row`]: the in-flight row a request keeps in the component's store
//!   between the activation that issues a nested call and the one that
//!   receives its answer, generic over the component's [`Tool`] enum.
//! - [`call`]: the request's error answer, the nested call's arguments and
//!   what a step decides next.
//! - [`args`]: reading a tool's JSON arguments.
//! - [`answer`]: reading `api-request`'s answer envelope from a Google API,
//!   bounding what is copied out of it, and the result ceiling.
//! - [`settings`]: reading an instance's `config` values.
//! - `schema` (behind the `testutils` feature): a host test's check that each
//!   tool's `input_schema` states exactly the bounds its argument parser
//!   enforces, the argument values that probe the parser against each
//!   bound, and the assertions that hold the parser to them. It panics by
//!   design: it serves host tests, never a component step.
//! - `testutil` (behind the `testutils` feature): fixtures for a step's tests
//!   — `api-request` answers, nested-call errors, Google error bodies and
//!   error answers.
//!
//! The crate is `std` plus `serde`/`serde_json` and names no guest SDK, so all
//! of it runs on the host under test. Each component keeps its own activation
//! loop, which is written against the guest SDK, and its own tools, settings
//! keys and status-to-kind mapping.
//!
//! This is an in-tree crate shared by the host tree and the guest workspace
//! (which reaches it through `brenn-wasm/components/host-crates/`). It is not
//! a guest-SDK surface and not a contract for out-of-tree components: its API
//! changes with its in-tree callers, all of them updated at once.

pub mod answer;
pub mod args;
pub mod call;
pub mod row;
#[cfg(feature = "testutils")]
pub mod schema;
pub mod settings;
#[cfg(any(test, feature = "testutils"))]
pub mod testutil;

pub use answer::{
    Answer, COMPACT_RESULT_CEILING, GOOGLE_MESSAGE_MAX_BYTES, GOOGLE_STATUS_MAX_BYTES, NestedError,
    UNREADABLE_KEY, Unreadable, bounded, copy, cut, google_error_detail, not_issued,
    read_api_result, result_summary, serialized_len,
};
pub use args::{Args, capped, choice, flag, integer, object};
pub use call::{
    ApiCall, Call, ErrorKind, Failure, Next, Query, QueryValue, Target, internal, query,
};
pub use row::{INFLIGHT_TTL_MS, InFlight, Tool, call_id, claim, no_such_step, row_expired, stored};
#[cfg(feature = "testutils")]
pub use schema::{
    Bound, NON_BLANK, Probes, ToolBounds, check, hold_probes, hold_required, input_schema, probes,
    token_pattern,
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
