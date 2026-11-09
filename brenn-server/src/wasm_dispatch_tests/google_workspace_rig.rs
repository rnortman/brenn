//! What the five Google Workspace components' end-to-end suites share.
//!
//! Each compiled component serves its provided tools through the one
//! registry: an app conversation acting for alice calls a tool, the request
//! lands on the component's `brenn:tool-requests/<slug>` inbox, the component
//! issues its nested `api-request`s through a real `WasmProvidesHost`, the
//! executor runs each against the OAuth test stub with the credential alice
//! connected, and the result comes back on the component's
//! `brenn:tool-results/<slug>` inbox. The one host the component's `config`
//! names is the stub, which answers by request path. The component and the
//! executor are stepped by hand; the conversation's call runs on a spawned
//! task.

use super::*;

use std::collections::HashMap;

use brenn_lib::config::{PACKAGED, config_from_dsl};
use brenn_lib::messaging::config::ProvidedToolDecl;
use brenn_messaging::MountDebt;
use brenn_oauth::testutil;
use brenn_tool_registry::ToolError;
use serde_json::json;

use super::provider_rig::CallerSetup;
use super::stub_rig::{self, Pairs, StubProvider, StubRig, pairs};

/// One Workspace component under test: its artifact, its packaged
/// specification, its class, the instance's slug, and the one host key its
/// `config` names.
pub(super) struct GoogleClass {
    pub wasm: &'static str,
    pub spec: &'static str,
    pub class: &'static str,
    pub slug: &'static str,
    pub host_key: &'static str,
}

pub(super) const GMAIL_READ: GoogleClass = GoogleClass {
    wasm: concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../brenn-wasm/target/components/brenn_google_gmail_read.wasm"
    ),
    spec: concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../config/specs/google-gmail-read.brenn"
    ),
    class: "GoogleGmailRead",
    slug: "gmail-read",
    host_key: "gmail_host",
};

pub(super) const GMAIL_COMPOSE: GoogleClass = GoogleClass {
    wasm: concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../brenn-wasm/target/components/brenn_google_gmail_compose.wasm"
    ),
    spec: concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../config/specs/google-gmail-compose.brenn"
    ),
    class: "GoogleGmailCompose",
    slug: "gmail-compose",
    host_key: "gmail_host",
};

pub(super) const GMAIL_ORGANIZE: GoogleClass = GoogleClass {
    wasm: concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../brenn-wasm/target/components/brenn_google_gmail_organize.wasm"
    ),
    spec: concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../config/specs/google-gmail-organize.brenn"
    ),
    class: "GoogleGmailOrganize",
    slug: "gmail-organize",
    host_key: "gmail_host",
};

pub(super) const CALENDAR_READ: GoogleClass = GoogleClass {
    wasm: concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../brenn-wasm/target/components/brenn_google_calendar_read.wasm"
    ),
    spec: concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../config/specs/google-calendar-read.brenn"
    ),
    class: "GoogleCalendarRead",
    slug: "calendar-read",
    host_key: "calendar_host",
};

pub(super) const CALENDAR_WRITE: GoogleClass = GoogleClass {
    wasm: concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../brenn-wasm/target/components/brenn_google_calendar_write.wasm"
    ),
    spec: concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../config/specs/google-calendar-write.brenn"
    ),
    class: "GoogleCalendarWrite",
    slug: "calendar-write",
    host_key: "calendar_host",
};

/// The component that calls a provided tool on its own account.
pub(super) const CALLER: &str = "agenda";
/// The OAuth provider the instance's `config` names.
pub(super) const OAUTH_PROVIDER: &str = "google";

/// Alice's connected account, and the bearer its credential holds.
pub(super) const ACCOUNT: &str = "alice@example.com";
pub(super) const BEARER: &str = "AT-alice";
/// A second account alice connected, and its bearer.
pub(super) const WORK_ACCOUNT: &str = "alice.work@example.com";
pub(super) const WORK_BEARER: &str = "AT-alice-work";

/// An instance's `api-request` clauses, as method and exact path, each on the
/// stub's host for any user.
pub(super) type Clauses<'a> = &'a [(&'a str, &'a str)];

/// `class`'s `provide` statements, lowered from its packaged specification
/// under one instance of the class.
fn provided(class: &GoogleClass) -> Vec<ProvidedToolDecl> {
    let GoogleClass {
        spec,
        class: name,
        slug,
        ..
    } = class;
    let text = std::fs::read_to_string(spec).unwrap_or_else(|e| panic!("read {spec}: {e}"));
    let config = config_from_dsl(&format!(
        r#"{PACKAGED}{text}{PACKAGED}
new {slug}: {name} {{
    grants = [log, store, config, tools, provides];
    store_path = "/var/lib/brenn/google-{slug}.db";
    tool api-request {{}}
}}
"#
    ));
    config
        .wasm_consumers
        .into_iter()
        .find(|consumer| consumer.slug == *slug)
        .expect("the instance lowers")
        .provides
}

/// The accounts alice has connected: the account and its access token.
pub(super) type Connected<'a> = &'a [(&'a str, &'a str)];

/// The rig for `class` with `clauses` and alice's one account connected.
pub(super) async fn rig(class: &GoogleClass, clauses: Clauses<'_>) -> StubRig {
    rig_with(class, clauses, &[(ACCOUNT, BEARER)], None).await
}

/// Stand up the rig for `class`: the provider's `api-request` grant holds
/// `clauses`, alice has connected `connected`, and `caller` is the calling
/// component, if any.
pub(super) async fn rig_with(
    class: &GoogleClass,
    clauses: Clauses<'_>,
    connected: Connected<'_>,
    caller: Option<CallerSetup<'_>>,
) -> StubRig {
    rig_configured(class, clauses, connected, caller, |_| {}).await
}

/// The rig for `class` with `clauses` and alice's one account connected,
/// `api-request` reading at most `max_response_bytes` of each of Google's
/// answers.
pub(super) async fn rig_reading_at_most(
    class: &GoogleClass,
    clauses: Clauses<'_>,
    max_response_bytes: usize,
) -> StubRig {
    rig_configured(class, clauses, &[(ACCOUNT, BEARER)], None, |provider| {
        provider.raw.max_response_bytes = max_response_bytes;
    })
    .await
}

/// [`rig_with`], with `configure` applied to the OAuth provider's
/// configuration before the service is built.
async fn rig_configured(
    class: &GoogleClass,
    clauses: Clauses<'_>,
    connected: Connected<'_>,
    caller: Option<CallerSetup<'_>>,
    configure: impl FnOnce(&mut brenn_lib::config::OAuthProviderConfig),
) -> StubRig {
    let started = stub_rig::start().await;
    let host = started.host.clone();
    let (name, mut provider) = testutil::provider_config(OAUTH_PROVIDER, &started.base);
    configure(&mut provider);
    let (service, vault) = testutil::service(vec![(name, provider)]);
    for (account, bearer) in connected {
        let mut credential = testutil::credential(OAUTH_PROVIDER, account, "RT-alice");
        credential.access_token = bearer.to_string();
        service.vault().store("alice", &credential).await;
    }
    StubRig::build(
        started,
        StubProvider {
            slug: class.slug,
            wasm: class.wasm,
            decls: provided(class),
            native: brenn_oauth::tools::registered(
                Some(&service),
                brenn_lib::config::ApiKeyTable::empty(),
            ),
            clauses: clauses
                .iter()
                .map(|&(method, path)| {
                    vec![
                        ("provider", OAUTH_PROVIDER),
                        ("user", "*"),
                        ("method", method),
                        ("path", path),
                    ]
                })
                .collect(),
            config: HashMap::from([
                ("provider".to_string(), OAUTH_PROVIDER.to_string()),
                (class.host_key.to_string(), host),
            ]),
            caller,
            vault,
        },
    )
    .await
}

/// The query pairs a Google call sends: `expected` plus the
/// `prettyPrint=false` every call carries.
pub(super) fn google_query(expected: &[(&str, &str)]) -> Pairs {
    pairs(&[expected, &[("prettyPrint", "false")]].concat())
}

/// Google's 403 for a token without the scope the call needs.
pub(super) fn insufficient_scope_body() -> Vec<u8> {
    json!({
        "error": {
            "code": 403,
            "message": "Request had insufficient authentication scopes.",
            "status": "PERMISSION_DENIED",
            "details": [{ "reason": "ACCESS_TOKEN_SCOPE_INSUFFICIENT" }],
        },
    })
    .to_string()
    .into_bytes()
}

/// The tool's `denied` for Google's 403 over [`insufficient_scope_body`] on a
/// call under the scope `scope` names ("Gmail read", "Calendar read", …).
pub(super) fn scope_denied(scope: &str) -> Result<serde_json::Value, ToolError> {
    Err(ToolError::Denied(format!(
        "Google PERMISSION_DENIED (HTTP 403): Request had insufficient authentication scopes. \
         If Google names insufficient authentication scopes, the account was connected without \
         the {scope} scope: run oauth-connect with that scope set and relay its steps."
    )))
}

/// How a nested read is made unreadable end to end.
#[derive(Clone, Copy, Debug)]
pub(super) enum Unreadable {
    /// `api-request` reads at most 4 096 bytes, so an [`OVERSIZED`] body is
    /// cut at the read limit.
    Cut,
    /// The rig's bus cap is the 64 KiB default and the read limit the
    /// 256 KiB default, so an [`OVERSIZED`] body is read whole and its result
    /// refused by the executor on `max_body_bytes`.
    Refused,
}

pub(super) const BOTH_WAYS: [Unreadable; 2] = [Unreadable::Cut, Unreadable::Refused];

/// A Google body size past both the cut rig's read limit and the default bus
/// cap, and under the default read limit.
pub(super) const OVERSIZED: usize = 100 * 1024;

/// The rig for `class` and `way`, with `clauses` and alice's one account
/// connected.
pub(super) async fn unreadable_rig(
    class: &GoogleClass,
    clauses: Clauses<'_>,
    way: Unreadable,
) -> StubRig {
    match way {
        Unreadable::Cut => rig_reading_at_most(class, clauses, 4096).await,
        Unreadable::Refused => rig(class, clauses).await,
    }
}

/// `reason` is the wrapper's sentence for a read of a `len`-byte body made
/// unreadable `way`.
pub(super) fn assert_unreadable(way: Unreadable, reason: &str, len: usize) {
    match way {
        Unreadable::Cut => assert_eq!(
            reason,
            format!(
                "Google's answer ({len} bytes) exceeds api-request's read limit for this \
                 provider (max_response_bytes)"
            )
        ),
        Unreadable::Refused => {
            let (size, cap) = reason
                .strip_prefix("the host did not deliver api-request's answer: result of ")
                .and_then(|rest| rest.split_once(" bytes exceeds [messaging] max_body_bytes ("))
                .and_then(|(size, cap)| Some((size, cap.strip_suffix(')')?)))
                .unwrap_or_else(|| panic!("{reason}"));
            let size: usize = size.parse().unwrap_or_else(|_| panic!("{reason}"));
            let cap: usize = cap.parse().unwrap_or_else(|_| panic!("{reason}"));
            assert!(size >= len && size > cap, "{reason}");
        }
    }
}

/// A tool's first call: the tool's name and slug, the arguments it is called
/// with, the method and path the call issues, and a body that answers it.
pub(super) struct FirstRead {
    pub tool: &'static str,
    pub slug: &'static str,
    pub args: serde_json::Value,
    pub method: &'static str,
    pub path: &'static str,
    pub body: String,
}

/// `args` with `account` set to `account`.
pub(super) fn naming(args: &serde_json::Value, account: &str) -> serde_json::Value {
    let mut args = args.clone();
    args["account"] = json!(account);
    args
}

/// With no account connected, `api-request`'s `credential_invalid` value is
/// `read`'s tool's answer unchanged, and the component makes no further call.
pub(super) async fn assert_no_connected_account(
    class: &GoogleClass,
    clauses: Clauses<'_>,
    read: FirstRead,
) {
    let FirstRead { tool, args, .. } = read;
    let r = rig_with(class, clauses, &[], None).await;
    let call = r.call_for_alice(tool, args);
    let id = r.requested_id().await;
    r.run(1).await;

    assert_eq!(
        call.await.unwrap(),
        Ok(json!({
            "error": "credential_invalid",
            "provider": OAUTH_PROVIDER,
            "account": null,
            "detail": "no account is connected for this provider",
        })),
        "{tool}"
    );
    r.owed(&r.executor_sub(), 0).await;
    assert!(
        r.seen().is_empty(),
        "{tool}: no API was called without a credential"
    );
    r.assert_finished(&id);
}

/// With two accounts connected, a call of `read`'s tool naming none is
/// `denied` with `api-request`'s list of them, and a call naming one sends
/// that account's bearer.
pub(super) async fn assert_several_accounts_need_one_named(
    class: &GoogleClass,
    clauses: Clauses<'_>,
    read: FirstRead,
) {
    let FirstRead {
        tool,
        args,
        method,
        path,
        body,
        ..
    } = read;
    let r = rig_with(
        class,
        clauses,
        &[(ACCOUNT, BEARER), (WORK_ACCOUNT, WORK_BEARER)],
        None,
    )
    .await;
    r.answer(path, body);

    let call = r.call_for_alice(tool, args.clone());
    let id = r.requested_id().await;
    r.run(1).await;
    let answer = call.await.unwrap();
    let Err(ToolError::Denied(detail)) = answer else {
        panic!("{tool}: expected a denied answer, got {answer:?}");
    };
    assert!(
        detail.starts_with(&format!(
            "api-request refused the nested {method} {}{path}: several \"google\" accounts \
             are connected; name one as `account`: ",
            r.host
        )) && detail.contains(ACCOUNT)
            && detail.contains(WORK_ACCOUNT),
        "{tool}: {detail}"
    );
    assert!(r.seen().is_empty(), "{tool}: a refused call reaches no API");
    r.assert_finished(&id);

    let call = r.call_for_alice(tool, naming(&args, WORK_ACCOUNT));
    let id = r.requested_id().await;
    r.run(1).await;
    call.await
        .unwrap()
        .unwrap_or_else(|e| panic!("{tool}: the named account answered: {e:?}"));
    assert_eq!(r.bearers(), [format!("Bearer {WORK_BEARER}")], "{tool}");
    r.assert_finished(&id);
}

/// A component calling `read`'s tool on its own account has no user, and an
/// instance whose clauses say `user = "*"` lends none: the caller's result is
/// `denied` carrying `api-request`'s explanation, and nothing reaches the API.
pub(super) async fn assert_a_userless_caller_is_denied_with_the_reason(
    class: &GoogleClass,
    clauses: Clauses<'_>,
    read: FirstRead,
) {
    let FirstRead {
        slug,
        args,
        method,
        path,
        ..
    } = read;
    let r = rig_with(
        class,
        clauses,
        &[(ACCOUNT, BEARER)],
        Some(CallerSetup {
            slug: CALLER,
            tool: slug,
            args,
        }),
    )
    .await;
    let caller = r.caller();
    testutils::insert_bus_message(&r.messenger, &caller.trigger, "go", ChannelScheme::Brenn).await;
    drain_step(&caller.component, &caller.sub, MountDebt::Settled).await;

    // The executor hands the call to the provider, which issues its
    // first call.
    r.executor.drain_step().await;
    let id = r.requested_id().await;
    r.serve().await;
    let nested = r.owed(&r.executor_sub(), 1).await;
    assert_eq!(nested[0]["serving"], id.as_str(), "{slug}");

    r.executor.drain_step().await;
    assert!(r.seen().is_empty(), "{slug}: a refused call reaches no API");
    r.owed(&r.provider_sub, 1).await;
    r.serve().await;

    let results = r.owed(&caller.sub, 1).await;
    assert_eq!(results[0]["tool"], slug);
    let error = &results[0]["outcome"]["err"];
    assert_eq!(error["kind"], "denied", "{}", results[0]);
    let detail = error["detail"].as_str().expect("a detail");
    // A userless read has no user to borrow; a userless `POST` is refused
    // first, as not a read.
    let reason = match method {
        "GET" => "a caller with no acting user borrows the one user",
        _ => "a caller with no acting user may only read",
    };
    assert!(
        detail.starts_with(&format!(
            "api-request refused the nested {method} {}{path}: ",
            r.host
        )) && detail.contains(reason),
        "{slug}: {detail}"
    );
    r.assert_finished(&id);
}
