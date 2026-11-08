//! The wrapper's decisions, in `std` plus `serde_json`: validating
//! `GmailModifyLabels`' arguments, building its Gmail call as `api-request`
//! arguments, and reading `api-request`'s answer. Nothing here names the
//! guest SDK, so the whole module runs on the host under test. The tool is
//! the generated `Provided` enum, whose SDK-facing parts are wasm32-only.

// The host test's crate root is this file, so it declares the generated
// module itself; in the component, `lib.rs` does.
#[cfg(test)]
mod spec;

use brenn_google_workspace::args::{account, id_list};
use brenn_google_workspace::call::json_call;
use brenn_google_workspace::gmail::{gmail_id_rule, is_gmail_id};
use brenn_google_workspace::scope::{Scope, google_error, with_write_hint};
use brenn_google_workspace::settings::Settings;
use brenn_provider_kit::{
    Answer, ApiCall, Args, COMPACT_RESULT_CEILING, Call, Failure, Logic, NestedError, Next, Query,
    Tool, Unreadable, bounded, google, no_such_step, object, read_api_result, stored,
};
use serde_json::{Value, json};

use crate::spec::Provided;

/// The path Gmail changes many messages' labels at in one call.
const BATCH_MODIFY_PATH: &str = "/gmail/v1/users/me/messages/batchModify";
/// The most message ids one batchModify takes: Google's limit.
const MODIFY_IDS_MAX: usize = 1000;
/// The most characters `GmailModifyLabels`' message ids may hold together
/// (ids are ASCII, so characters are bytes). They travel in the POST body
/// inside the nested call's args, which the host caps at 64 KiB: with `add`
/// and `remove` at `MODIFY_LABELS_MAX` ids of `ID_MAX_CHARS` and `account` at
/// its escaped width, the widest admitted call serializes to about 61 000
/// bytes.
const MODIFY_IDS_MAX_CHARS: usize = 48_000;
/// The most label ids `add` or `remove` may name.
const MODIFY_LABELS_MAX: usize = 20;
/// The system label ids `add` refuses. Applying `TRASH` or `SPAM` moves mail
/// to a folder Gmail empties after 30 days, and `SPAM` also trains the spam
/// filter; these tools never trash mail or report it. Every other label Gmail
/// lets a caller apply (`INBOX`, `UNREAD`, `STARRED`, `IMPORTANT`, the
/// `CATEGORY_` tabs, the user's `Label_` ids) is undone by removing it, and
/// `remove` may name these two, which restores mail. Compared ignoring ASCII
/// case: no label id an account has is either name in another case.
const MODIFY_ADD_REFUSED: [&str; 2] = ["TRASH", "SPAM"];
/// What the caller is told when a batchModify may have been acted on and its
/// answer was lost or unreadable.
const MODIFY_UNKNOWN_HINT: &str =
    "the labels may already have been changed; asking again with the same arguments is safe";

/// The scope `tool`'s step `step` needs. A step the tool does not have is
/// never asked for: `advance` maps an error only for a step it issued.
fn scope(tool: Provided, step: u8) -> Scope {
    match (tool, step) {
        (Provided::GmailModifyLabels, _) => Scope::GmailOrganize,
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
pub struct GmailOrganize;

impl Logic for GmailOrganize {
    type Tool = Provided;
    type Settings = Settings;

    fn plan(tool: Provided, args: &Value, settings: &Settings) -> Result<Call, String> {
        plan(tool, args, settings)
    }

    fn advance(state: &InFlight, outcome: Result<Value, NestedError>, _: &Settings) -> Next {
        advance(state, outcome)
    }
}

/// A list of Gmail ids: 1 to `max` distinct ids, each one path segment of
/// letters, digits, `_` and `-`.
fn gmail_ids(
    args: &Args,
    key: &str,
    max: usize,
    what: &str,
) -> Result<Option<Vec<String>>, String> {
    id_list(args, key, (1, max), what, is_gmail_id, gmail_id_rule)
}

/// Validate a request's arguments and build its first nested call. An error is
/// the request's `invalid_args` detail, naming the argument.
pub fn plan(tool: Provided, args: &Value, settings: &Settings) -> Result<Call, String> {
    match tool {
        Provided::GmailModifyLabels => {
            let modify = ModifyLabelsArgs::parse(args)?;
            Ok(Call {
                step: 1,
                api: batch_modify_call(&modify, settings),
            })
        }
    }
}

/// `GmailModifyLabels`' arguments: the messages, and the label ids to add
/// and to remove, at least one of the two given and no id in both.
struct ModifyLabelsArgs {
    ids: Vec<String>,
    add: Option<Vec<String>>,
    remove: Option<Vec<String>>,
    account: Option<String>,
}

impl ModifyLabelsArgs {
    /// Every argument the tool takes.
    const KEYS: &[&str] = &["ids", "add", "remove", "account"];

    fn parse(args: &Value) -> Result<ModifyLabelsArgs, String> {
        let map = object(args, Self::KEYS)?;
        let ids =
            gmail_ids(map, "ids", MODIFY_IDS_MAX, "message ids")?.ok_or("`ids` is required")?;
        if ids.iter().map(String::len).sum::<usize>() > MODIFY_IDS_MAX_CHARS {
            return Err(format!(
                "`ids` together exceed {MODIFY_IDS_MAX_CHARS} characters; ask for fewer"
            ));
        }
        let add = gmail_ids(map, "add", MODIFY_LABELS_MAX, "label ids")?;
        if let Some(refused) = add.iter().flatten().find(|id| {
            MODIFY_ADD_REFUSED
                .iter()
                .any(|label| id.eq_ignore_ascii_case(label))
        }) {
            return Err(format!(
                "`add` may not name `{refused}`: these tools never move mail to Trash or Spam"
            ));
        }
        let remove = gmail_ids(map, "remove", MODIFY_LABELS_MAX, "label ids")?;
        match (&add, &remove) {
            (None, None) => return Err("`add` or `remove` must name a label".to_string()),
            (Some(add), Some(remove)) => {
                if let Some(both) = add.iter().find(|id| remove.contains(id)) {
                    return Err(format!("`add` and `remove` both name `{both}`"));
                }
            }
            _ => {}
        }
        Ok(ModifyLabelsArgs {
            ids,
            add,
            remove,
            account: account(map)?,
        })
    }
}

/// messages.batchModify of the arguments' messages: one `POST`, each label
/// list only when given. Google applies it whole or refuses it.
fn batch_modify_call(args: &ModifyLabelsArgs, settings: &Settings) -> ApiCall {
    let mut body = json!({ "ids": args.ids });
    if let Some(add) = &args.add {
        body["addLabelIds"] = json!(add);
    }
    if let Some(remove) = &args.remove {
        body["removeLabelIds"] = json!(remove);
    }
    json_call(
        settings,
        "POST",
        args.account.as_deref(),
        BATCH_MODIFY_PATH.to_string(),
        Query::new(),
        body,
    )
}

/// A batchModify Google accepted, whatever 2xx it answered with: how many
/// messages, and the label ids added and removed, as asked.
fn modify_labels_accepted(state: &InFlight) -> Result<Next, Failure> {
    let args =
        stored(state, ModifyLabelsArgs::parse).map_err(|failure| at_write(state, failure))?;
    let mut out = Args::new();
    out.insert("accepted".to_string(), json!(true));
    out.insert("ids".to_string(), json!(args.ids.len()));
    if let Some(add) = args.add {
        out.insert("add".to_string(), json!(add));
    }
    if let Some(remove) = args.remove {
        out.insert("remove".to_string(), json!(remove));
    }
    Ok(Next::Complete(Value::Object(out)))
}

/// What the request does with its nested call's answer: complete or fail.
/// Every answer completed here is held to the result ceiling; a relayed
/// credential answer is `api-request`'s own and is not.
pub fn advance(state: &InFlight, outcome: Result<Value, NestedError>) -> Next {
    match read_api_result(outcome, &state.target, google::API_NAME) {
        // Google's acceptance of a label change, whatever its body.
        Ok(Answer::Body(_)) => {}
        Ok(Answer::NoBody { .. }) => return settle(state.tool, no_body(state)),
        Ok(Answer::Relay(value)) => return Next::Complete(value),
        Ok(Answer::Unreadable(why)) => return unreadable(state, why),
        Ok(Answer::Error { status, body }) => {
            return Next::Fail(at_write(
                state,
                google_error(status, &body, scope(state.tool, state.step)),
            ));
        }
        Err(failure) => return Next::Fail(at_write(state, failure)),
    }
    settle(
        state.tool,
        match (state.tool, state.step) {
            (Provided::GmailModifyLabels, 1) => modify_labels_accepted(state),
            (tool, step) => Err(no_such_step(tool, step)),
        },
    )
}

/// A nested answer that was cut or refused: a write step fails with why and
/// its hint; a step the tool does not have is refused as such.
fn unreadable(state: &InFlight, why: Unreadable) -> Next {
    if let Some(hint) = write_hint(state.tool, state.step) {
        return settle(state.tool, Err(why.failure(google::API_NAME, Some(hint))));
    }
    settle(state.tool, Err(no_such_step(state.tool, state.step)))
}

/// The hint a write step's failure carries when Google may have acted: what
/// the caller can look for, or that asking again is safe. `None` for a read.
/// Every write step has its row here and nowhere else.
fn write_hint(tool: Provided, step: u8) -> Option<&'static str> {
    match (tool, step) {
        (Provided::GmailModifyLabels, 1) => Some(MODIFY_UNKNOWN_HINT),
        _ => None,
    }
}

fn at_write(state: &InFlight, failure: Failure) -> Failure {
    with_write_hint(write_hint(state.tool, state.step), failure)
}

/// A 2xx with no body, decided by the step: a label change is accepted; a
/// step the tool does not have is refused as such.
fn no_body(state: &InFlight) -> Result<Next, Failure> {
    match (state.tool, state.step) {
        (Provided::GmailModifyLabels, 1) => modify_labels_accepted(state),
        (tool, step) => Err(no_such_step(tool, step)),
    }
}

/// A step's decision as the request's next move, a completed answer held to
/// the result ceiling.
fn settle(tool: Provided, decided: Result<Next, Failure>) -> Next {
    match decided {
        Ok(Next::Complete(value)) => {
            let (value, hint) = match tool {
                Provided::GmailModifyLabels => (value, None),
            };
            bounded(value, COMPACT_RESULT_CEILING, hint)
        }
        Ok(next) => next,
        Err(failure) => Next::Fail(failure),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use brenn_google_workspace::args::{ACCOUNT_MAX_CHARS, HOST_ARGS_CAP_BYTES};
    use brenn_google_workspace::gmail::ID_MAX_CHARS;
    use brenn_google_workspace::scope::scope_hint;
    use brenn_google_workspace::testutil::{
        admits_characters, cut_read, description, escaped_widest, lookup,
    };
    use brenn_provider_kit::testutil::{failure, google, http, nested};
    use brenn_provider_kit::{
        Bound, ErrorKind, Probes, ToolBounds, check, hold_probes, hold_required, input_schema,
        internal, probes, query,
    };
    use serde_json::{Map, json};

    use super::*;

    const BASE: &[(&str, &str)] = &[("provider", "google"), ("gmail_host", "gmail.example.com")];

    fn settings() -> Settings {
        Settings::from_lookup(lookup(BASE), "gmail_host").unwrap()
    }

    /// `advance` on the row.
    fn answered(state: &InFlight, outcome: Result<Value, NestedError>) -> Next {
        advance(state, outcome)
    }

    /// A label change's arguments: `m1` gains `Label_7`, with `extra` set
    /// over them.
    fn modify_args(extra: Value) -> Value {
        let mut args = json!({ "ids": ["m1"], "add": ["Label_7"] });
        args.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        args
    }

    fn modify(args: Value) -> Result<ApiCall, String> {
        plan(Provided::GmailModifyLabels, &args, &settings()).map(|call| {
            assert_eq!(call.step, 1);
            call.api
        })
    }

    fn modify_state(args: Value) -> InFlight {
        let call = plan(Provided::GmailModifyLabels, &args, &settings()).unwrap();
        InFlight::new(
            &"a".repeat(32),
            Provided::GmailModifyLabels,
            &args,
            &call,
            Map::new(),
            1_000,
            1_000,
        )
    }

    #[test]
    fn a_row_names_every_provided_tool_by_its_registry_name() {
        let state = modify_state(modify_args(json!({})));
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
        wire["tool"] = json!("GmailModifyLabels");
        let error = InFlight::decode(&serde_json::to_vec(&wire).unwrap()).unwrap_err();
        assert!(
            error.contains("no provided tool is named `GmailModifyLabels`"),
            "{error}"
        );
    }

    /// Every step `tool` issues a call at.
    fn steps(tool: Provided) -> Vec<u8> {
        match tool {
            Provided::GmailModifyLabels => vec![1],
        }
    }

    #[test]
    fn every_step_names_its_scope() {
        for tool in Provided::ALL {
            for step in steps(tool) {
                assert_eq!(
                    scope(tool, step),
                    Scope::GmailOrganize,
                    "{} step {step}",
                    tool.name()
                );
            }
        }
    }

    /// `n` distinct ids of exactly `len` ASCII characters: `prefix`, then the
    /// id's number zero-padded to fill the rest.
    fn padded_ids(prefix: &str, n: usize, len: usize) -> Vec<String> {
        (0..n)
            .map(|i| format!("{prefix}{i:0>width$}", width = len - prefix.len()))
            .collect()
    }

    #[test]
    fn argument_caps_count_characters_not_bytes() {
        admits_characters(modify, modify_args(json!({})), "account", ACCOUNT_MAX_CHARS);
    }

    /// Every argument `tool` takes, with the bound its parser holds it to,
    /// and the arguments it requires: the parser's own constants and key
    /// lists, as `every_tools_schema_states_what_its_parser_enforces` holds
    /// the spec to them. Three parser rules are outside what a schema
    /// states, each leaving the schema wider than the parser and each in the
    /// description: a label change's `ids` hold at most
    /// `MODIFY_IDS_MAX_CHARS` together, as
    /// `every_description_states_its_parsers_budgets` holds; its `add` names
    /// none of `MODIFY_ADD_REFUSED`; and, cross-field, it names a label in
    /// `add` or `remove` and none in both, as
    /// `modify_labels_refuses_bad_arguments` holds. The per-value probes are
    /// this component's, in `workspace_probes`.
    fn bounds(tool: Provided) -> ToolBounds<'static> {
        let (keys, required): (&'static [&'static str], &'static [&'static str]) = match tool {
            Provided::GmailModifyLabels => (ModifyLabelsArgs::KEYS, &["ids"]),
        };
        let text = |max_chars| Bound::Text {
            max_chars,
            format: None,
        };
        let gmail_ids = |max| Bound::Array {
            min: 1,
            max,
            unique: true,
            item: Box::new(Bound::Token {
                max_chars: ID_MAX_CHARS,
            }),
        };
        let property = |key: &'static str| {
            let bound = match (tool, key) {
                (_, "account") => text(ACCOUNT_MAX_CHARS),
                (Provided::GmailModifyLabels, "ids") => gmail_ids(MODIFY_IDS_MAX),
                (Provided::GmailModifyLabels, "add" | "remove") => gmail_ids(MODIFY_LABELS_MAX),
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
            Provided::GmailModifyLabels => modify_args(json!({})),
        }
    }

    /// The probes of the arguments whose parser holds a rule past the kit's
    /// bound: a label change's id lists. `None` where the kit's probes for
    /// the bound hold.
    fn workspace_probes(tool: Provided, key: &str) -> Option<Probes> {
        let gmail_ids = |prefix: &str, n: usize, len: usize| json!(padded_ids(prefix, n, len));
        Some(match (tool, key) {
            (Provided::GmailModifyLabels, "ids") => Probes {
                admitted: vec![
                    json!(["m1"]),
                    gmail_ids("m", MODIFY_IDS_MAX, 8),
                    json!([&"m".repeat(ID_MAX_CHARS)]),
                    // The ids at their budget together.
                    gmail_ids("m", MODIFY_IDS_MAX_CHARS / ID_MAX_CHARS, ID_MAX_CHARS),
                ],
                refused: [vec![
                    json!([]),
                    gmail_ids("m", MODIFY_IDS_MAX + 1, 8),
                    json!(["m1", "m1"]),
                    json!([""]),
                    json!([&"m".repeat(ID_MAX_CHARS + 1)]),
                    json!(["a/b"]),
                    json!(["a b"]),
                    json!([1]),
                    json!("m1"),
                    // The budget the schema cannot state.
                    gmail_ids("m", MODIFY_IDS_MAX_CHARS / ID_MAX_CHARS + 1, ID_MAX_CHARS),
                ]]
                .concat(),
            },
            (Provided::GmailModifyLabels, "add" | "remove") => {
                let mut probes = Probes {
                    admitted: vec![
                        json!(["STARRED"]),
                        gmail_ids("l", MODIFY_LABELS_MAX, ID_MAX_CHARS),
                    ],
                    refused: vec![
                        json!([]),
                        gmail_ids("l", MODIFY_LABELS_MAX + 1, 8),
                        json!(["STARRED", "STARRED"]),
                        json!([""]),
                        json!(["My label"]),
                        json!([1]),
                        json!("STARRED"),
                    ],
                };
                // The labels `add` refuses and the schema cannot state;
                // `remove` restores mail from them.
                let trash_and_spam = [
                    json!(["TRASH"]),
                    json!(["SPAM"]),
                    json!(["Label_8", "trash"]),
                ];
                if key == "add" {
                    probes.refused.extend(trash_and_spam);
                } else {
                    probes.admitted.extend(trash_and_spam);
                }
                probes
            }
            _ => return None,
        })
    }

    /// The component's specification, as the package ships it.
    fn spec_text() -> String {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../config/specs/google-gmail-organize.brenn");
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

    /// The parser budgets no schema can state: each tool's description gives
    /// the number its parser's constant holds.
    #[test]
    fn every_description_states_its_parsers_budgets() {
        let spec = spec_text();
        let tool = Provided::GmailModifyLabels;
        let budget = format!(
            "the ids together may hold at most {MODIFY_IDS_MAX_CHARS} characters, and more is \
             refused"
        );
        assert!(
            description(&spec, tool.name()).contains(&budget),
            "`{}`'s description must say {budget:?}",
            tool.name()
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
        let at = |mut state: InFlight, step: u8| {
            state.step = step;
            state
        };
        let name = |state: &InFlight| format!("{} step {}", state.tool.name(), state.step);

        let state = modify_state(modify_args(json!({})));
        let wording = "; the labels may already have been changed; asking again with the same \
                       arguments is safe";
        for (outcome, why) in outcomes() {
            let answer = answered(&state, outcome);
            assert_eq!(
                answer,
                Next::Fail(why.failure(google::API_NAME, Some(MODIFY_UNKNOWN_HINT))),
                "{}",
                name(&state)
            );
            let Next::Fail(Failure { detail, .. }) = answer else {
                panic!("{answer:?}");
            };
            assert!(detail.ends_with(wording), "{detail}");
        }

        for (outcome, _) in outcomes() {
            assert_eq!(
                answered(&at(modify_state(modify_args(json!({}))), 2), outcome),
                Next::Fail(no_such_step(Provided::GmailModifyLabels, 2))
            );
        }
    }

    #[test]
    fn every_tools_first_read_has_an_unreadable_answer() {
        // `unreadable` runs only on a cut or refused read, so a tool missing
        // its arm there would pass every other test and be answered as a
        // step it does not have.
        for tool in Provided::ALL {
            let mut state = modify_state(modify_args(json!({})));
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
    fn a_label_change_posts_one_batch_modify_on_the_gmail_host() {
        let call = modify(json!({
            "ids": ["m1", "m2"],
            "add": ["Label_7", "STARRED"],
            "remove": ["INBOX"],
            "account": "alice@example.com",
        }))
        .unwrap();
        assert_eq!(
            call,
            ApiCall {
                provider: "google".to_string(),
                account: Some("alice@example.com".to_string()),
                method: "POST".to_string(),
                host: "gmail.example.com".to_string(),
                path: "/gmail/v1/users/me/messages/batchModify".to_string(),
                query: query([("prettyPrint", "false".to_string())]),
                headers: BTreeMap::new(),
                body: call.body.clone(),
            }
        );
        let body: Value = serde_json::from_str(call.body.as_deref().unwrap()).unwrap();
        assert_eq!(
            body,
            json!({
                "ids": ["m1", "m2"],
                "addLabelIds": ["Label_7", "STARRED"],
                "removeLabelIds": ["INBOX"],
            })
        );
        // Each label list only when given.
        let body = |args: Value| -> Value {
            serde_json::from_str(modify(args).unwrap().body.as_deref().unwrap()).unwrap()
        };
        assert_eq!(
            body(json!({ "ids": ["m1"], "add": ["Label_7"] })),
            json!({ "ids": ["m1"], "addLabelIds": ["Label_7"] })
        );
        assert_eq!(
            body(json!({ "ids": ["m1"], "remove": ["UNREAD"] })),
            json!({ "ids": ["m1"], "removeLabelIds": ["UNREAD"] })
        );
    }

    #[test]
    fn modify_labels_refuses_bad_arguments() {
        for (args, refusal) in [
            (json!({ "add": ["Label_7"] }), "`ids` is required"),
            (
                json!({ "ids": ["m1"] }),
                "`add` or `remove` must name a label",
            ),
            (
                json!({ "ids": ["m1"], "add": ["UNREAD", "Label_7"], "remove": ["Label_7"] }),
                "`add` and `remove` both name `Label_7`",
            ),
            (
                json!({ "ids": [], "add": ["Label_7"] }),
                "`ids` must be an array of 1 to 1000 message ids",
            ),
            (
                json!({ "ids": ["m1", "m1"], "add": ["Label_7"] }),
                "`ids` names \"m1\" twice",
            ),
            (
                json!({ "ids": ["m/1"], "add": ["Label_7"] }),
                "each of `ids` must be 1 to 128 letters, digits, `_` or `-`",
            ),
            (
                json!({ "ids": ["m1"], "add": ["Receipts 2026"] }),
                "each of `add` must be 1 to 128 letters, digits, `_` or `-`",
            ),
            (
                json!({ "ids": ["m1"], "remove": "INBOX" }),
                "`remove` must be an array of 1 to 20 label ids",
            ),
            (
                json!({ "ids": ["m1"], "add": ["Label_7", "TRASH"] }),
                "`add` may not name `TRASH`: these tools never move mail to Trash or Spam",
            ),
            (
                json!({ "ids": ["m1"], "add": ["Spam"], "remove": ["INBOX"] }),
                "`add` may not name `Spam`: these tools never move mail to Trash or Spam",
            ),
            (
                json!({ "ids": ["m1"], "add": ["Label_7"], "labels": ["INBOX"] }),
                "unknown argument `labels`; this tool takes `ids`, `add`, `remove`, `account`",
            ),
        ] {
            assert_eq!(modify(args.clone()).unwrap_err(), refusal, "{args}");
        }
        assert!(modify(json!({ "ids": ["m1"], "remove": ["TRASH", "SPAM"] })).is_ok());
        let ids = |n: usize| padded_ids("m", n, ID_MAX_CHARS);
        let at_budget = ids(MODIFY_IDS_MAX_CHARS / ID_MAX_CHARS);
        assert_eq!(
            at_budget.iter().map(String::len).sum::<usize>(),
            MODIFY_IDS_MAX_CHARS
        );
        assert!(modify(modify_args(json!({ "ids": at_budget }))).is_ok());
        assert_eq!(
            modify(modify_args(
                json!({ "ids": ids(MODIFY_IDS_MAX_CHARS / ID_MAX_CHARS + 1) })
            ))
            .unwrap_err(),
            "`ids` together exceed 48000 characters; ask for fewer"
        );
    }

    #[test]
    fn a_label_change_is_accepted_on_any_2xx() {
        let accepted =
            json!({ "accepted": true, "ids": 2, "add": ["Label_7"], "remove": ["INBOX"] });
        let state = modify_state(json!({
            "ids": ["m1", "m2"],
            "add": ["Label_7"],
            "remove": ["INBOX"],
        }));
        for outcome in [
            http(204, ""),
            http(200, ""),
            http(200, "{}"),
            http(200, r#"{"unexpected":[1]}"#),
            http(200, "42"),
        ] {
            assert_eq!(answered(&state, outcome), Next::Complete(accepted.clone()));
        }
        // Only the label lists asked for are answered.
        assert_eq!(
            answered(
                &modify_state(json!({ "ids": ["m1"], "remove": ["UNREAD"] })),
                http(204, "")
            ),
            Next::Complete(json!({ "accepted": true, "ids": 1, "remove": ["UNREAD"] }))
        );
    }

    #[test]
    fn a_modify_labels_403_names_the_organize_scope() {
        let body = google(
            403,
            "PERMISSION_DENIED",
            "Request had insufficient authentication scopes.",
        );
        assert_eq!(
            answered(&modify_state(modify_args(json!({}))), http(403, &body)),
            Next::Fail(failure(
                ErrorKind::Denied,
                "Google PERMISSION_DENIED (HTTP 403): Request had insufficient authentication \
                 scopes. If Google names insufficient authentication scopes, the account was \
                 connected without the Gmail organize scope: run oauth-connect with that scope \
                 set and relay its steps."
            ))
        );
    }

    #[test]
    fn the_widest_modify_labels_call_fits_the_args_cap() {
        // The most ids, and the longest, each at the ids' budget, beside
        // label lists of the most and longest ids and `account` at its
        // escaped width.
        let labels = |prefix: &str| padded_ids(prefix, MODIFY_LABELS_MAX, ID_MAX_CHARS);
        let with = |ids: Vec<String>| {
            modify(json!({
                "ids": ids,
                "add": labels("a"),
                "remove": labels("r"),
                "account": escaped_widest(ACCOUNT_MAX_CHARS),
            }))
        };
        for (n, len) in [
            (MODIFY_IDS_MAX, MODIFY_IDS_MAX_CHARS / MODIFY_IDS_MAX),
            (MODIFY_IDS_MAX_CHARS / ID_MAX_CHARS, ID_MAX_CHARS),
        ] {
            let ids = padded_ids("m", n, len);
            assert_eq!(
                ids.iter().map(String::len).sum::<usize>(),
                MODIFY_IDS_MAX_CHARS
            );
            let args = serde_json::to_vec(&with(ids).unwrap()).unwrap();
            assert!(
                args.len() <= HOST_ARGS_CAP_BYTES,
                "{n} ids: {} bytes of args",
                args.len()
            );
        }
        assert_eq!(
            with(padded_ids(
                "m",
                MODIFY_IDS_MAX,
                MODIFY_IDS_MAX_CHARS / MODIFY_IDS_MAX + 1
            ))
            .unwrap_err(),
            "`ids` together exceed 48000 characters; ask for fewer"
        );
    }

    /// The row of `tool` at its write step `step`, as `write_hint` names it.
    fn write_step_state(tool: Provided, step: u8) -> InFlight {
        let state = match (tool, step) {
            (Provided::GmailModifyLabels, 1) => modify_state(modify_args(json!({}))),
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
    }
}
