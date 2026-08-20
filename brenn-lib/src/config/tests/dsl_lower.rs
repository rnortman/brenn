//! Lowering a derived `.brenn` document to a [`BrennConfig`].
//!
//! The shape of every test here is the same equivalence claim: a `.brenn`
//! document and the TOML that says the same thing must parse to the same
//! `BrennConfig`. That is what makes the transcription checkable rather than a
//! second specification — the assertion runs against what the runtime will
//! actually see, and everything downstream of `load_config` is provenance-blind.

use std::path::Path;

use brenn_dsl::diag::Diagnostic;

use crate::config::BrennConfig;
use crate::config::dsl_lower::lower;

/// Compile a `.brenn` document from a tempdir and lower it.
///
/// `compile` takes a root file path — the root file's directory is the module
/// root — so a document under test is written out rather than passed as text.
fn lowered(document: &str) -> Result<BrennConfig, Vec<Diagnostic>> {
    let dir = tempfile::tempdir().expect("a tempdir");
    let root = dir.path().join("main.brenn");
    std::fs::write(&root, document).expect("write the root module");
    let output = brenn_dsl::compile(&root)
        .unwrap_or_else(|errors| panic!("the document must compile:\n{}", render(&errors)));
    assert!(
        output.warnings.is_empty(),
        "the document must compile clean:\n{}",
        render(&output.warnings)
    );
    lower(output.config)
}

/// Every diagnostic, one per line, for a panic message.
fn render(diagnostics: &[Diagnostic]) -> String {
    diagnostics
        .iter()
        .map(Diagnostic::render)
        .collect::<Vec<_>>()
        .join("\n")
}

/// The document lowers, and equals the TOML that says the same thing.
fn assert_equivalent(document: &str, toml: &str) {
    let from_dsl = lowered(document)
        .unwrap_or_else(|errors| panic!("the document must lower:\n{}", render(&errors)));
    let from_toml: BrennConfig = toml::from_str(toml).expect("the TOML twin must parse");
    assert_eq!(from_dsl, from_toml);
}

/// The TOML twin of a refused document is refused too, by serde, and for the
/// same reason: `because` is a substring serde's own error must contain.
///
/// What locks a hand-carried table to serde as the source of truth: the DSL side
/// and the TOML side must agree about *what* is wrong. Asserting only that the
/// twin is refused would pass on any unrelated defect in the fixture, which is
/// exactly the drift this helper exists to catch.
fn assert_toml_refused(toml: &str, because: &str) {
    let refused = toml::from_str::<BrennConfig>(toml);
    let error = refused.expect_err("the TOML twin must be refused too");
    let text = error.to_string();
    assert!(
        text.contains(because),
        "serde must refuse the twin over `{because}`, and refused it over: {text}"
    );
}

/// The one diagnostic the document produces.
fn refusal(document: &str) -> Diagnostic {
    let mut errors = lowered(document).expect_err("the document must be refused");
    assert_eq!(errors.len(), 1, "one refusal:\n{}", render(&errors));
    errors.pop().expect("one refusal")
}

// ---------------------------------------------------------------------------
// Channels and tunings
// ---------------------------------------------------------------------------

/// A durable channel carries a configured identity, and the DSL derives it from
/// the address rather than making the operator mint one. The literal below is
/// that derivation's answer, pinned here so a seed change in `brenn-dsl` breaks
/// a test outside `brenn-dsl` too — the row name of every persisted channel a
/// lowered config created depends on it.
#[test]
fn a_durable_channel_lowers_with_its_derived_uuid() {
    assert_equivalent(
        r#"
channel alerts at "brenn:alice-alerts" {
    description = "Where alice's alerts land.";
    push_depth = 8;
    retain_depth = 128;
    standing_retain_depth = unbounded;
    noise = metered;
    sink = archive;
    wake_min = low;
    send_rate = { burst = 4, refill_interval_secs = 60, refill = 2 };
}
"#,
        r#"
[[channel]]
uuid = "85a5cf7e-6874-5766-9d69-712784754a1f"
address = "brenn:alice-alerts"
description = "Where alice's alerts land."
push_depth = 8
retain_depth = 128
standing_retain_depth = "unbounded"
noise = "metered"
sink = "archive"
wake_min = "low"
send_rate = { burst = 4, refill_interval_secs = 60, refill = 2 }
"#,
    );
}

/// A non-durable channel carries no configured identity — the runtime derives
/// one from its address — and states only the two depths it has.
#[test]
fn an_ephemeral_channel_lowers_without_a_uuid() {
    assert_equivalent(
        r#"
channel presence at "ephemeral:alice-desk.presence" {
    push_depth = 4;
    retain_depth = 16;
}
"#,
        r#"
[[channel]]
address = "ephemeral:alice-desk.presence"
push_depth = 4
retain_depth = 16
"#,
    );
}

/// The minimal body: every optional attr omitted on the DSL side, every
/// defaulted key omitted on the TOML side. This is the defaults-parity lock —
/// an omitted key must get exactly what serde would have given it.
#[test]
fn a_local_channel_with_a_minimal_body_matches_serde_defaults() {
    assert_equivalent(
        r#"
channel scratch at "local:alice-scratch" {
    push_depth = 1;
    retain_depth = 1;
}
"#,
        r#"
[[channel]]
address = "local:alice-scratch"
push_depth = 1
retain_depth = 1
"#,
    );
}

/// A tuning block keyed by a whole address tunes the one channel the system
/// mints at it; it is not a declaration, so it carries no uuid.
#[test]
fn a_tuning_at_an_address_lowers_to_an_address_keyed_entry() {
    assert_equivalent(
        r#"
channel at "brenn:tool-results/alice" {
    push_depth = 2;
    retain_depth = 32;
    standing_retain_depth = 32;
}
"#,
        r#"
[[channel]]
address = "brenn:tool-results/alice"
push_depth = 2
retain_depth = 32
standing_retain_depth = 32
"#,
    );
}

/// A tuning keyed by a prefix covers a whole family of dynamically named
/// channels, and lands in `address_prefix` rather than `address`.
#[test]
fn a_tuning_at_a_prefix_lowers_to_a_prefix_keyed_entry() {
    assert_equivalent(
        r#"
channel at prefix "brenn:tool-results/" {
    push_depth = 2;
    retain_depth = 32;
    standing_retain_depth = 32;
}
"#,
        r#"
[[channel]]
address_prefix = "brenn:tool-results/"
push_depth = 2
retain_depth = 32
standing_retain_depth = 32
"#,
    );
}

// ---------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------

/// A word that spells no variant of the target enum is refused at the word
/// itself, with the legal spellings named. The spellings come from the enum's
/// own `Deserialize`, so there is no second table here to drift.
#[test]
fn a_word_spelling_no_noise_level_is_refused_at_the_word() {
    let refusal = refusal(
        r#"
channel alerts at "brenn:alice-alerts" {
    push_depth = 1;
    retain_depth = 1;
    standing_retain_depth = 1;
    noise = loud;
}
"#,
    );
    let message = refusal.render();
    assert!(message.contains("`noise`"), "{message}");
    assert!(message.contains("silent"), "{message}");
    assert!(message.contains("metered"), "{message}");
    assert!(
        message.contains("main.brenn:6:13:"),
        "the span is the offending word's own: {message}"
    );
}

/// A depth is a non-negative count or the word `unbounded`; a negative count is
/// neither, and the refusal says so where it was written.
#[test]
fn a_negative_depth_is_refused() {
    let refusal = refusal(
        r#"
channel presence at "ephemeral:alice-desk.presence" {
    push_depth = -1;
    retain_depth = 1;
}
"#,
    );
    let message = refusal.render();
    assert!(message.contains("`push_depth`"), "{message}");
    assert!(message.contains("unbounded"), "{message}");
}

/// A word other than `unbounded` in a depth position is refused the same way a
/// negative count is.
#[test]
fn a_word_other_than_unbounded_is_refused_in_a_depth_position() {
    let refusal = refusal(
        r#"
channel presence at "ephemeral:alice-desk.presence" {
    push_depth = infinite;
    retain_depth = 1;
}
"#,
    );
    let message = refusal.render();
    assert!(message.contains("`push_depth`"), "{message}");
    assert!(message.contains("`infinite`"), "{message}");
}

/// `send_rate` is a table with no vocabulary behind it, so its keys are matched
/// by hand — and a stray key is refused at its own token, which is the typo
/// protection `deny_unknown_fields` gives the TOML path, relocated.
#[test]
fn a_stray_send_rate_key_is_refused_with_the_legal_set() {
    let refusal = refusal(
        r#"
channel presence at "ephemeral:alice-desk.presence" {
    push_depth = 1;
    retain_depth = 1;
    send_rate = { burst = 4, refil = 2 };
}
"#,
    );
    let message = refusal.render();
    assert!(message.contains("`refil`"), "{message}");
    assert!(message.contains("`refill`"), "{message}");
    assert!(message.contains("`burst`"), "{message}");
}

/// A `send_rate` that states only some of its keys gets serde's own defaults
/// for the rest, from the same `SendRate::default()` its `#[serde(default)]`
/// reads.
#[test]
fn a_partial_send_rate_table_keeps_the_defaults_for_the_rest() {
    assert_equivalent(
        r#"
channel presence at "ephemeral:alice-desk.presence" {
    push_depth = 1;
    retain_depth = 1;
    send_rate = { burst = 4 };
}
"#,
        r#"
[[channel]]
address = "ephemeral:alice-desk.presence"
push_depth = 1
retain_depth = 1
send_rate = { burst = 4 }
"#,
    );
}

/// Every refusal in a document is reported, not just the first: lowering
/// accumulates and fails at the end.
#[test]
fn every_bad_value_in_a_document_is_reported() {
    let errors = lowered(
        r#"
channel presence at "ephemeral:alice-desk.presence" {
    push_depth = -1;
    retain_depth = -2;
    noise = loud;
}
"#,
    )
    .expect_err("the document must be refused");
    // The identity of the three refusals is the substance; a count alone would
    // pass on one refusal reported three times.
    let mut keys: Vec<&str> = errors
        .iter()
        .map(|error| match &error.message {
            message if message.starts_with("`push_depth`") => "push_depth",
            message if message.starts_with("`retain_depth`") => "retain_depth",
            message if message.starts_with("`noise`") => "noise",
            message => panic!("unexpected refusal: {message}"),
        })
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        ["noise", "push_depth", "retain_depth"],
        "{}",
        render(&errors)
    );
}

/// A path that is not a `.brenn` file is not this module's concern, but a
/// document that compiles to nothing lowers to the default config — the empty
/// case has to be the identity, or every equivalence test above is measuring
/// the wrong thing.
#[test]
fn an_empty_document_lowers_to_the_default_config() {
    let dir = tempfile::tempdir().expect("a tempdir");
    let root = dir.path().join("main.brenn");
    std::fs::write(&root, "").expect("write the root module");
    let output = brenn_dsl::compile(Path::new(&root)).expect("an empty document compiles");
    assert_eq!(
        lower(output.config).expect("an empty document lowers"),
        BrennConfig::default()
    );
}

// ---------------------------------------------------------------------------
// Configuration sections
// ---------------------------------------------------------------------------

/// Every scalar section, every key stated, each value chosen away from its
/// default so a lowering line that dropped its value could not pass.
#[test]
fn every_section_lowers_with_every_key_stated() {
    assert_equivalent(
        r#"
server {
    bind_address = "127.0.0.1:3100";
    static_dir = "/opt/brenn/frontend/dist";
    surface_dist_dir = "/opt/brenn/surface/dist";
    secure_cookies = false;
    trusted_proxy_hops = 1;
    pid_file = "/run/brenn/brenn.pid";
    public_url = "https://brenn.example.com";
}

database { path = "/var/lib/brenn/alice.db"; }

logging {
    log_dir = "/var/log/brenn";
    console_level = info;
    file_level = trace;
}

security {
    auth_rate_interval_secs = 7;
    auth_rate_burst = 11;
    global_rate_interval_secs = 2;
    global_rate_burst = 101;
    asset_rate_interval_secs = 3;
    asset_rate_burst = 2001;
    auth_body_limit = 4097;
    global_body_limit = 1048577;
    upload_body_limit = 26214401;
    max_image_long_edge = 2577;
}

alerting {
    max_alerts = 11;
    window_secs = 3601;
    ntfy { url = "https://ntfy.example.com/alice-alerts"; }
    mail {
        to = "alice@example.com";
        subject_label = "Alice's Brenn";
    }
}

claude_defaults {
    mcp_script_path = "/opt/brenn/alice_mcp.py";
    model = "opus";
}

repo_sync {
    repo_dir = "/home/alice/repos";
    poll_interval_secs = 301;
    stale_conversation_days = 8;
}

messaging {
    default_send_budget = 101;
    max_body_bytes = 65537;
    default_noise = metered;
    default_sink = archive;
    default_wake_min = high;
    default_send_rate = { burst = 5, refill_interval_secs = 61, refill = 3 };
    archive_path = "/var/lib/brenn/archive";
}

observability {
    surface_error_channel = "brenn:alice-surface-errors";
    surface_error_publish_floor = error;
    usage { session_gap_minutes = 45; }
}

surface_description {
    prefix = "alice-surface";
    status_interval_secs = 61;
}

llm_chat {
    prefix = "alice-chat";
    retained_window = 1001;
    wake_min = low;
    idle_timeout_secs = 301;
}

pwa_push {
    keypair_file = "/var/lib/brenn/vapid.json";
    subject = "mailto:alice@example.com";
    endpoint_host_allowlist = ["push.example.com", "push.example.org"];
    endpoint_host_allowlist_enforce = false;
}

automation {
    max_fires_per_hour_per_job = 61;
    max_error_reports_per_hour_per_job = 4;
    consecutive_failures_to_disable = 6;
    max_jobs_per_app = 51;
}

events { delivered_retention_days = 8; }

wasm { store_size_limit = "128MiB"; }

watchdog {
    sweep_interval_secs = 31;
    wedge_grace_secs = 61;
}

container cc {
    image = "brenn-cc:latest";
    home_dir = "/home/alice/container-home";
    container_home = "/home/bob";
    extra_mounts = ["/srv/shared:/srv/shared"];
    extra_args = ["--pids-limit", "512"];
}
"#,
        r#"
[server]
bind_address = "127.0.0.1:3100"
static_dir = "/opt/brenn/frontend/dist"
surface_dist_dir = "/opt/brenn/surface/dist"
secure_cookies = false
trusted_proxy_hops = 1
pid_file = "/run/brenn/brenn.pid"
public_url = "https://brenn.example.com"

[database]
path = "/var/lib/brenn/alice.db"

[logging]
log_dir = "/var/log/brenn"
console_level = "info"
file_level = "trace"

[security]
auth_rate_interval_secs = 7
auth_rate_burst = 11
global_rate_interval_secs = 2
global_rate_burst = 101
asset_rate_interval_secs = 3
asset_rate_burst = 2001
auth_body_limit = 4097
global_body_limit = 1048577
upload_body_limit = 26214401
max_image_long_edge = 2577

[alerting]
max_alerts = 11
window_secs = 3601
ntfy = { url = "https://ntfy.example.com/alice-alerts" }
mail = { to = "alice@example.com", subject_label = "Alice's Brenn" }

[claude_defaults]
mcp_script_path = "/opt/brenn/alice_mcp.py"
model = "opus"

[repo_sync]
repo_dir = "/home/alice/repos"
poll_interval_secs = 301
stale_conversation_days = 8

[messaging]
default_send_budget = 101
max_body_bytes = 65537
default_noise = "metered"
default_sink = "archive"
default_wake_min = "high"
default_send_rate = { burst = 5, refill_interval_secs = 61, refill = 3 }
archive_path = "/var/lib/brenn/archive"

[observability]
surface_error_channel = "brenn:alice-surface-errors"
surface_error_publish_floor = "error"
usage = { session_gap_minutes = 45 }

[surface_description]
prefix = "alice-surface"
status_interval_secs = 61

[llm_chat]
prefix = "alice-chat"
retained_window = 1001
wake_min = "low"
idle_timeout_secs = 301

[pwa_push]
keypair_file = "/var/lib/brenn/vapid.json"
subject = "mailto:alice@example.com"
endpoint_host_allowlist = ["push.example.com", "push.example.org"]
endpoint_host_allowlist_enforce = false

[automation]
max_fires_per_hour_per_job = 61
max_error_reports_per_hour_per_job = 4
consecutive_failures_to_disable = 6
max_jobs_per_app = 51

[events]
delivered_retention_days = 8

[wasm]
store_size_limit = "128MiB"

[watchdog]
sweep_interval_secs = 31
wedge_grace_secs = 61

[container.cc]
image = "brenn-cc:latest"
home_dir = "/home/alice/container-home"
container_home = "/home/bob"
extra_mounts = ["/srv/shared:/srv/shared"]
extra_args = ["--pids-limit", "512"]
"#,
    );
}

/// The minimal body of every section that has one: only the keys the target
/// requires. What the omitted keys get must be what serde gives them, which is
/// what pins lowering to the default functions rather than to restated
/// literals.
#[test]
fn minimal_sections_take_the_defaults_serde_would_give() {
    assert_equivalent(
        r#"
server { public_url = "https://brenn.example.com"; }
alerting {
    max_alerts = 5;
    window_secs = 60;
    mail { to = "alice@example.com"; }
}
observability { surface_error_channel = "brenn:alice-surface-errors"; }
pwa_push { subject = "mailto:alice@example.com"; }
container cc {
    image = "brenn-cc:latest";
    home_dir = "/home/alice/container-home";
}
"#,
        r#"
[server]
public_url = "https://brenn.example.com"

[alerting]
max_alerts = 5
window_secs = 60
mail = { to = "alice@example.com" }

[observability]
surface_error_channel = "brenn:alice-surface-errors"

[pwa_push]
subject = "mailto:alice@example.com"

[container.cc]
image = "brenn-cc:latest"
home_dir = "/home/alice/container-home"
"#,
    );
}

/// A minimal section per configuration kindword: the required keys and nothing
/// else.
///
/// Lowering dispatches on the kindword string; an unhandled kindword panics on
/// a document the front end called valid. This table
/// is the tripwire for that: the test below asserts it covers every kindword the
/// language admits, and lowers all of them, so a kindword added to the
/// vocabulary is a red test rather than a boot panic.
const MINIMAL_SECTIONS: [(&str, &str); 17] = [
    (
        "server",
        r#"server { public_url = "https://brenn.example.com"; }"#,
    ),
    ("database", "database { }"),
    ("logging", "logging { }"),
    ("security", "security { }"),
    ("alerting", "alerting { max_alerts = 5; window_secs = 60; }"),
    ("claude_defaults", "claude_defaults { }"),
    ("repo_sync", "repo_sync { }"),
    ("messaging", "messaging { }"),
    ("observability", "observability { }"),
    ("surface_description", "surface_description { }"),
    ("llm_chat", "llm_chat { }"),
    ("pwa_push", "pwa_push { }"),
    ("automation", "automation { }"),
    ("events", "events { }"),
    ("wasm", "wasm { }"),
    ("watchdog", "watchdog { }"),
    (
        "container",
        r#"container cc { image = "brenn-cc:latest"; home_dir = "/home/alice/container-home"; }"#,
    ),
];

/// Every kindword the language admits reaches a lowering arm.
#[test]
fn every_configuration_section_kindword_lowers() {
    let mut covered: Vec<&str> = MINIMAL_SECTIONS
        .iter()
        .map(|(kindword, _)| *kindword)
        .collect();
    covered.sort_unstable();
    let mut admitted: Vec<&str> = brenn_dsl::model::CONFIG_BLOCK_KINDWORDS.to_vec();
    admitted.sort_unstable();
    assert_eq!(
        covered, admitted,
        "every configuration section kindword needs a row above, and a lowering arm"
    );

    let document: String = MINIMAL_SECTIONS
        .iter()
        .map(|(_, section)| format!("{section}\n"))
        .collect();
    lowered(&document)
        .unwrap_or_else(|errors| panic!("every section must lower:\n{}", render(&errors)));
}

/// A minimal document per sub-block kindword, in the parent that admits it.
///
/// Sub-blocks are looked up by name, so a kindword the language admits and no
/// arm reads lowers to nothing at all. Each row below is a document that must
/// lower, which is what makes the tables below coverage rather than a second
/// copy of the constants.
const MINIMAL_ALERTING_BLOCKS: [(&str, &str); 2] = [
    (
        "ntfy",
        r#"alerting { max_alerts = 5; window_secs = 60;
               ntfy { url = "https://ntfy.example.com/alice"; } }"#,
    ),
    (
        "mail",
        r#"alerting { max_alerts = 5; window_secs = 60;
               mail { to = "alice@example.com"; } }"#,
    ),
];

const MINIMAL_OBSERVABILITY_BLOCKS: [(&str, &str); 1] = [(
    "usage",
    "observability { usage { session_gap_minutes = 30; } }",
)];

const MINIMAL_AGENT_BLOCKS: [(&str, &str); 3] = [
    (
        "start_hooks",
        r#"agent A() { start_hooks { host = ["git fetch"]; } }
           new alice: A();"#,
    ),
    (
        "post_pull_hooks",
        r#"agent A() { post_pull_hooks { host = ["cargo build"]; } }
           new alice: A();"#,
    ),
    (
        "startup_hooks",
        r#"agent A() { startup_hooks { host = ["pf migrate"]; } }
           new alice: A();"#,
    ),
];

/// Every sub-block kindword the language admits reaches a lowering arm, and the
/// arm reads the block rather than dropping it.
#[test]
fn every_sub_block_kindword_lowers() {
    for (admitted, table) in [
        (
            brenn_dsl::model::ALERTING_BLOCK_KINDWORDS,
            MINIMAL_ALERTING_BLOCKS.as_slice(),
        ),
        (
            brenn_dsl::model::OBSERVABILITY_BLOCK_KINDWORDS,
            MINIMAL_OBSERVABILITY_BLOCKS.as_slice(),
        ),
        (
            brenn_dsl::model::AGENT_BLOCK_KINDWORDS,
            MINIMAL_AGENT_BLOCKS.as_slice(),
        ),
    ] {
        let mut covered: Vec<&str> = table.iter().map(|(kindword, _)| *kindword).collect();
        covered.sort_unstable();
        let mut admitted: Vec<&str> = admitted.to_vec();
        admitted.sort_unstable();
        assert_eq!(
            covered, admitted,
            "every sub-block kindword needs a row above, and a lowering arm"
        );
        for (kindword, document) in table {
            lowered(document)
                .unwrap_or_else(|errors| panic!("`{kindword}` must lower:\n{}", render(&errors)));
        }
    }
}

// Lowering's multiplicity and "no sub-blocks" refusals are unreachable through
// the pipeline — resolution refuses them at check time, for section sub-blocks,
// webhook blocks and an agent's hook blocks alike. They stay as defense in
// depth; the resolve-side refusals are tested in brenn-dsl.

/// A token in a section reaches lowering as text, and goes through the same
/// enum `Deserialize` a channel's token does — so the refusal names the legal
/// spellings from serde's own tables.
#[test]
fn a_bad_section_token_names_the_legal_spellings() {
    let diagnostic = refusal("messaging { default_noise = loud; }");
    assert!(
        diagnostic.message.starts_with("`default_noise`:"),
        "{}",
        diagnostic.render()
    );
    assert!(
        diagnostic.message.contains("silent"),
        "the legal spellings are named: {}",
        diagnostic.message
    );
    assert_eq!(diagnostic.span.text_str(), Some("loud"));
}

/// A log level goes through the config's own `deserialize_with`, so the DSL
/// path refuses exactly what the TOML path refuses.
#[test]
fn a_bad_log_level_is_refused_by_the_configs_own_parser() {
    let diagnostic = refusal("logging { console_level = chatty; }");
    assert!(
        diagnostic.message.contains("invalid log level"),
        "{}",
        diagnostic.render()
    );
    assert_eq!(diagnostic.span.text_str(), Some("chatty"));
}

/// A value of the wrong shape is refused at the value, saying what it found.
#[test]
fn a_string_where_a_count_belongs_is_refused() {
    let diagnostic = refusal(r#"security { auth_rate_burst = "eleven"; }"#);
    assert_eq!(
        diagnostic.message,
        "`auth_rate_burst`: expected an integer, got a string"
    );
}

/// A count too large for the field it targets is refused rather than
/// truncated: the narrowing is the target type's own range.
#[test]
fn a_count_out_of_the_targets_range_is_refused() {
    let diagnostic =
        refusal("server { public_url = \"https://brenn.example.com\"; trusted_proxy_hops = 300; }");
    assert_eq!(
        diagnostic.message,
        "`trusted_proxy_hops`: 300 is out of range for this key"
    );
}

// ---------------------------------------------------------------------------
// Repos and mqtt clients
// ---------------------------------------------------------------------------

/// A repo is a top-level declaration whose handle is its wire slug, and an
/// agent reaches one through a `mount` statement whose tail is the mount's own
/// body.
#[test]
fn repos_and_the_mounts_that_reach_them_lower() {
    assert_equivalent(
        r#"
repo life {
    remote = "forgejo@example.com:alice/life.git";
    auto_pull = true;
}

repo notes {
    remote = "forgejo@example.com:alice/notes.git";
    auto_pull = false;
}

agent Assistant() {
    name = "Assistant";

    mount life { working_dir = true; primary = true; }
    mount notes { access = read-only; auto_pull = false; }
}

new alice: Assistant();
"#,
        r#"
[[repo]]
slug = "life"
remote = "forgejo@example.com:alice/life.git"
auto_pull = true

[[repo]]
slug = "notes"
remote = "forgejo@example.com:alice/notes.git"
auto_pull = false

[[app]]
slug = "alice"
name = "Assistant"

[[app.mount]]
repo = "life"
working_dir = true
primary = true

[[app.mount]]
repo = "notes"
access = "read-only"
auto_pull = false
"#,
    );
}

/// The minimal repo body: only the remote, with `auto_pull` taking the same
/// default function serde calls.
#[test]
fn a_minimal_repo_takes_its_default_auto_pull() {
    assert_equivalent(
        r#"
repo life {
    remote = "forgejo@example.com:alice/life.git";
}
"#,
        r#"
[[repo]]
slug = "life"
remote = "forgejo@example.com:alice/life.git"
"#,
    );
}

/// Everything an mqtt client body can say, each value chosen away from its
/// default so a dropped key shows up as a difference.
#[test]
fn an_mqtt_client_lowers_every_key_it_states() {
    assert_equivalent(
        r#"
mqtt_client broker {
    url = "mqtts://broker.example.com:8883";
    username = "alice";
    password_file = "/home/alice/.secrets/broker.password";
    ca_file = "/home/alice/.secrets/broker-ca.pem";
    tls_version_min = "1.3";
    keepalive_secs = 30;
    inbound_payload_cap_bytes = 262144;
    reconnect_backoff_initial_secs = 2;
    reconnect_backoff_max_secs = 120;
    session_expiry_secs = 300;
    qos = 2;
    urgency = high;
}
"#,
        r#"
[[mqtt_client]]
slug = "broker"
url = "mqtts://broker.example.com:8883"
username = "alice"
password_file = "/home/alice/.secrets/broker.password"
ca_file = "/home/alice/.secrets/broker-ca.pem"
tls_version_min = "1.3"
keepalive_secs = 30
inbound_payload_cap_bytes = 262144
reconnect_backoff_initial_secs = 2
reconnect_backoff_max_secs = 120
session_expiry_secs = 300
qos = 2
urgency = "high"
"#,
    );
}

/// The minimal mqtt client body: the broker url alone. Every other field takes
/// the `#[serde(default = "fn")]` function serde itself would call, which is
/// what this row locks.
#[test]
fn a_minimal_mqtt_client_takes_every_default() {
    assert_equivalent(
        r#"
mqtt_client broker {
    url = "mqtts://broker.example.com:8883";
}
"#,
        r#"
[[mqtt_client]]
slug = "broker"
url = "mqtts://broker.example.com:8883"
"#,
    );
}

// ---------------------------------------------------------------------------
// Agents
// ---------------------------------------------------------------------------

/// Every scalar attr an agent body can state, each value away from its default.
#[test]
fn an_agent_lowers_every_scalar_attr_it_states() {
    assert_equivalent(
        r#"
agent Assistant() {
    slug = "alice-pa";
    name = "Personal Assistant";
    description = "The desk assistant.";
    icon = "assistant";
    working_dir = "/home/alice/work";
    model = "sonnet";
    single_instance = true;
    singleton = true;
    persistent = true;
    multiuser = true;
    idle_timeout_secs = 900;
    idle_hook_secs = 60;
    compact_reminder_pct = 60;
    compact_soft_pct = 70;
    compact_red_pct = 85;
    compact_hard_pct = 95;
    compact_reminder_tokens = 120000;
    compact_soft_tokens = 140000;
    compact_red_tokens = 170000;
    compact_hard_tokens = 190000;
    compact_idle_secs = 1800;
    history_replay_limit = 50;
    allowed_users = ["alice", "bob"];
    disabled_tools = ["WebSearch"];
    cc_extra_args = ["--verbose"];
    integrations = ["calendar"];
    extra_mounts = ["/home/alice/notes"];
    prefix_username = true;
    prefix_timestamp = false;
    prefix_device = true;
    container = "sandbox";
    container_working_dir = "/work";
}

new alice: Assistant();
"#,
        r#"
[[app]]
slug = "alice-pa"
name = "Personal Assistant"
description = "The desk assistant."
icon = "assistant"
working_dir = "/home/alice/work"
model = "sonnet"
single_instance = true
singleton = true
persistent = true
multiuser = true
idle_timeout_secs = 900
idle_hook_secs = 60
compact_reminder_pct = 60
compact_soft_pct = 70
compact_red_pct = 85
compact_hard_pct = 95
compact_reminder_tokens = 120000
compact_soft_tokens = 140000
compact_red_tokens = 170000
compact_hard_tokens = 190000
compact_idle_secs = 1800
history_replay_limit = 50
allowed_users = ["alice", "bob"]
disabled_tools = ["WebSearch"]
cc_extra_args = ["--verbose"]
integrations = ["calendar"]
extra_mounts = ["/home/alice/notes"]
prefix_username = true
prefix_timestamp = false
prefix_device = true
container = "sandbox"
container_working_dir = "/work"
"#,
    );
}

/// The minimal agent body: nothing but the wire slug the handle supplies. Every
/// list is the empty one serde's `default` gives, and `messaging` is absent
/// because an agent with no subscriptions and no budget has nothing to put in
/// it.
#[test]
fn a_minimal_agent_states_only_its_slug() {
    assert_equivalent(
        r#"
agent Assistant() {
}

new alice: Assistant();
"#,
        r#"
[[app]]
slug = "alice"
"#,
    );
}

/// Hooks are named sub-blocks, one per point in the agent's life; mcp servers
/// arrive either as a reference to a top-level definition or as a body defined
/// inside the agent.
#[test]
fn an_agent_lowers_its_hook_blocks_and_both_mcp_forms() {
    assert_equivalent(
        r#"
mcp_server graf {
    command = "graf";
    args = ["mcp"];
    env = { GRAF_ROOT = "/home/alice/kb" };
}

agent Assistant() {
    mcp_server graf;
    mcp_server pfin {
        command = "pf";
        args = ["mcp", "--quiet"];
    }

    start_hooks {
        host = ["git fetch"];
        container = ["pf rebuild"];
    }
    post_pull_hooks {
        host = ["cargo build"];
    }
    startup_hooks {
        container = ["pf migrate"];
    }
}

new alice: Assistant();
"#,
        r#"
[[app]]
slug = "alice"

[app.mcp_servers.graf]
command = "graf"
args = ["mcp"]
env = { GRAF_ROOT = "/home/alice/kb" }

[app.mcp_servers.pfin]
command = "pf"
args = ["mcp", "--quiet"]

[app.start_hooks]
host = ["git fetch"]
container = ["pf rebuild"]

[app.post_pull_hooks]
host = ["cargo build"]

[app.startup_hooks]
container = ["pf migrate"]
"#,
    );
}

/// The three subscription families ride one statement form, dispatched on the
/// scheme of the address it names, and `send_budget` nests into the app's
/// `messaging` table. Each subscription also derives the ACL entry that
/// authorizes it, which is why the TOML twin spells them.
#[test]
fn an_agent_lowers_all_three_subscription_families_and_its_send_budget() {
    assert_equivalent(
        r#"
channel cmd at "brenn:alice.cmd" {
    push_depth = 8;
    retain_depth = 32;
    standing_retain_depth = 64;
}

channel presence at "ephemeral:alice.presence" { push_depth = 4; retain_depth = 8; }

mqtt_client broker {
    url = "mqtts://broker.example.com:8883";
}

webhook push_alice {
    slug = "push-alice";
    mount = "/webhooks/push-alice";

    signature {
        scheme = bearer-token;
        header = "authorization";
    }

    token phone { secret_file = "/home/alice/.secrets/push-alice.token"; }
}

agent Assistant() {
    grants = [subscribe];
    send_budget = 40;

    subscribe cmd { push_depth = 1000; retain_depth = 2000; noise = metered; wake_min = low; }
    subscribe presence { push_depth = 4; retain_depth = 8; }
    subscribe "webhook:push-alice" { push_depth = 10; retain_depth = 20; wake_min = normal; }
    subscribe "mqtt:broker:alice/lamp" { push_depth = 2; retain_depth = 4; noise = alarm; }
}

new alice: Assistant();
"#,
        r#"
[[channel]]
uuid = "c88e5596-574b-53d1-9b55-6e612b8f3d49"
address = "brenn:alice.cmd"
push_depth = 8
retain_depth = 32
standing_retain_depth = 64

[[channel]]
address = "ephemeral:alice.presence"
push_depth = 4
retain_depth = 8

[[mqtt_client]]
slug = "broker"
url = "mqtts://broker.example.com:8883"

[[app]]
slug = "alice"
grants = ["messaging_subscribe", "ephemeral_subscribe", "mqtt_subscribe", "webhook"]

[app.messaging]
send_budget = 40

[[app.messaging.subscribe]]
channel = "brenn:alice.cmd"
push_depth = 1000
retain_depth = 2000
noise = "metered"
wake_min = "low"

[[app.messaging.subscribe]]
channel = "ephemeral:alice.presence"
push_depth = 4
retain_depth = 8

[[app.webhook_subscription]]
endpoint = "push-alice"
push_depth = 10
retain_depth = 20
wake_min = "normal"

[[app.mqtt_subscription]]
channel = "mqtt:broker:alice/lamp"
push_depth = 2
retain_depth = 4
noise = "alarm"

[app.acl]
brenn_subscribe = [{ exact = "alice.cmd" }]
ephemeral_subscribe = [{ exact = "alice.presence" }]
mqtt_subscribe = [{ client = "broker", topic_filter = "alice/lamp" }]
webhook = [{ endpoint = "push-alice" }]

[[webhook_endpoint]]
slug = "push-alice"
mount = "/webhooks/push-alice"

[webhook_endpoint.signature]
scheme = "bearer-token"
header = "authorization"

[[webhook_endpoint.token]]
token_id = "phone"
secret_file = "/home/alice/.secrets/push-alice.token"
"#,
    );
}

/// The `[app.acl]` block has three provenances that all land in the same
/// lists: an `acl` statement in the agent's own body, the entry a subscription
/// derives, and a `grant` statement written about the agent from outside. The
/// patterns arrive scheme-stripped, which is how the raw config spells them.
#[test]
fn an_agent_lowers_acl_entries_from_every_provenance() {
    assert_equivalent(
        r#"
channel notes at "brenn:shared.notes" {
    push_depth = 4;
    retain_depth = 8;
    standing_retain_depth = 16;
}

agent Assistant() {
    grants = [subscribe, publish];

    acl subscribe [prefix "brenn:alice.", exact "brenn:alice-errors"];
    acl publish [prefix "ephemeral:alice."];
}

new alice: Assistant();

grant alice publish exact notes;
"#,
        r#"
[[channel]]
uuid = "46cec031-27ab-5416-b9ac-a72c8eb8a0d9"
address = "brenn:shared.notes"
push_depth = 4
retain_depth = 8
standing_retain_depth = 16

[[app]]
slug = "alice"
grants = ["messaging_subscribe", "messaging_publish", "ephemeral_publish"]

[app.acl]
brenn_subscribe = [{ prefix = "alice." }, { exact = "alice-errors" }]
ephemeral_publish = [{ prefix = "alice." }]
brenn_publish = [{ exact = "shared.notes" }]
"#,
    );
}

// ---------------------------------------------------------------------------
// Refusals in the open positions the entity families read
// ---------------------------------------------------------------------------

/// A key the union vocabulary admits and the family the address selects has no
/// field for. The front end cannot know which family a `subscribe` statement
/// is — the address decides that, and resolution is where an address is
/// known — so this one is lowering's refusal, at the value's own token.
#[test]
fn a_noise_policy_on_a_webhook_subscription_is_refused() {
    let error = refusal(
        r#"
webhook push_alice {
    slug = "push-alice";
    mount = "/webhooks/push-alice";

    signature {
        scheme = bearer-token;
        header = "authorization";
    }

    token phone { secret_file = "/home/alice/.secrets/push-alice.token"; }
}

agent Assistant() {
    grants = [subscribe];
    subscribe "webhook:push-alice" { push_depth = 4; noise = metered; }
}

new alice: Assistant();
"#,
    );
    assert_eq!(
        error.message,
        "`noise` is not a key of a subscription to `webhook:push-alice`; expected \
         `push_depth`, `retain_depth` or `wake_min`"
    );
    assert_eq!(
        error.line_col(),
        Some((16, 62)),
        "the span is the refused key's own token: {}",
        error.render()
    );
}

/// A key this family does not read earns one diagnostic, not two: the refusal
/// is the whole answer, and lowering the same token again would add a spelling
/// complaint implying the key would be fine once respelled.
#[test]
fn a_refused_family_key_is_not_also_value_checked() {
    let error = refusal(
        r#"
webhook push_alice {
    slug = "push-alice";
    mount = "/webhooks/push-alice";

    signature {
        scheme = bearer-token;
        header = "authorization";
    }

    token phone { secret_file = "/home/alice/.secrets/push-alice.token"; }
}

agent Assistant() {
    grants = [subscribe];
    subscribe "webhook:push-alice" { push_depth = 4; noise = loud; }
}

new alice: Assistant();
"#,
    );
    assert!(
        error.message.starts_with("`noise` is not a key of"),
        "{}",
        error.render()
    );
}

/// A tail value in the wrong shape is refused where it was written, with the
/// spelling the target admits named.
#[test]
fn a_bad_token_in_a_subscription_tail_names_the_legal_spellings() {
    let error = refusal(
        r#"
channel cmd at "brenn:alice.cmd" {
    push_depth = 8;
    retain_depth = 32;
    standing_retain_depth = 64;
}

agent Assistant() {
    grants = [subscribe];
    subscribe cmd { noise = loud; }
}

new alice: Assistant();
"#,
    );
    assert!(
        error.message.starts_with("`noise`: unknown variant `loud`"),
        "{}",
        error.message
    );
}

/// A depth in a tail takes a count or the word `unbounded`, and nothing else —
/// the same two spellings a depth in a channel body takes.
#[test]
fn a_negative_depth_in_a_subscription_tail_is_refused() {
    let error = refusal(
        r#"
channel cmd at "brenn:alice.cmd" {
    push_depth = 8;
    retain_depth = 32;
    standing_retain_depth = 64;
}

agent Assistant() {
    grants = [subscribe];
    subscribe cmd { push_depth = -1; }
}

new alice: Assistant();
"#,
    );
    assert_eq!(
        error.message,
        "`push_depth`: expected a non-negative integer or the word `unbounded`, got -1"
    );
}

/// Two mcp servers under one key would be one server with two definitions, so
/// the second is refused and the first is cited.
#[test]
fn a_duplicate_mcp_key_in_one_agent_is_refused_at_both_sites() {
    let error = refusal(
        r#"
mcp_server graf {
    command = "graf";
}

agent Assistant() {
    mcp_server graf;
    mcp_server graf;
}

new alice: Assistant();
"#,
    );
    assert_eq!(
        error.message,
        "agent `alice` states `graf` once, and this is the second"
    );
    assert_eq!(error.related.len(), 1);
}

// ---------------------------------------------------------------------------
// Channels and tunings in one document
// ---------------------------------------------------------------------------

/// Declarations and tunings share one `channels` vec, and lowering emits every
/// declaration before every tuning whatever order the document wrote them in.
/// The mixed document is also the only place the "a declaration carries a uuid,
/// a tuning never does" rule meets its own counterexample.
#[test]
fn a_document_mixing_a_tuning_and_a_declaration_emits_declarations_first() {
    assert_equivalent(
        r#"
channel at prefix "brenn:tool-results/" {
    push_depth = 2;
    retain_depth = 32;
    standing_retain_depth = 32;
}

channel alerts at "brenn:alice-alerts" {
    push_depth = 8;
    retain_depth = 128;
    standing_retain_depth = 256;
}
"#,
        r#"
[[channel]]
uuid = "85a5cf7e-6874-5766-9d69-712784754a1f"
address = "brenn:alice-alerts"
push_depth = 8
retain_depth = 128
standing_retain_depth = 256

[[channel]]
address_prefix = "brenn:tool-results/"
push_depth = 2
retain_depth = 32
standing_retain_depth = 32
"#,
    );
}

// ---------------------------------------------------------------------------
// The `[app.messaging]` presence rule
// ---------------------------------------------------------------------------

/// A budget with no subscriptions still needs the block — it is the only place
/// the budget can go.
#[test]
fn an_agent_with_only_a_send_budget_still_gets_a_messaging_block() {
    assert_equivalent(
        r#"
agent Assistant() {
    send_budget = 40;
}

new alice: Assistant();
"#,
        r#"
[[app]]
slug = "alice"

[app.messaging]
send_budget = 40
"#,
    );
}

/// And subscriptions with no budget: the block is present with the subscribe
/// list and no budget, so the app inherits the global one.
#[test]
fn an_agent_with_only_subscriptions_gets_a_messaging_block_without_a_budget() {
    assert_equivalent(
        r#"
channel cmd at "brenn:alice.cmd" {
    push_depth = 8;
    retain_depth = 32;
    standing_retain_depth = 64;
}

agent Assistant() {
    grants = [subscribe];
    subscribe cmd { push_depth = 4; retain_depth = 8; }
}

new alice: Assistant();
"#,
        r#"
[[channel]]
uuid = "c88e5596-574b-53d1-9b55-6e612b8f3d49"
address = "brenn:alice.cmd"
push_depth = 8
retain_depth = 32
standing_retain_depth = 64

[[app]]
slug = "alice"
grants = ["messaging_subscribe"]

[[app.messaging.subscribe]]
channel = "brenn:alice.cmd"
push_depth = 4
retain_depth = 8

[app.acl]
brenn_subscribe = [{ exact = "alice.cmd" }]
"#,
    );
}

/// An unbounded window in a tail is the bare word, exactly as it is in a
/// channel body: a tail is a vocabulary position, so there is one spelling of
/// the token per language.
#[test]
fn an_unbounded_depth_in_a_subscription_tail_lowers() {
    assert_equivalent(
        r#"
channel cmd at "brenn:alice.cmd" {
    push_depth = 8;
    retain_depth = 32;
    standing_retain_depth = 64;
}

agent Assistant() {
    grants = [subscribe];
    subscribe cmd { push_depth = 4; retain_depth = unbounded; }
}

new alice: Assistant();
"#,
        r#"
[[channel]]
uuid = "c88e5596-574b-53d1-9b55-6e612b8f3d49"
address = "brenn:alice.cmd"
push_depth = 8
retain_depth = 32
standing_retain_depth = 64

[[app]]
slug = "alice"
grants = ["messaging_subscribe"]

[[app.messaging.subscribe]]
channel = "brenn:alice.cmd"
push_depth = 4
retain_depth = "unbounded"

[app.acl]
brenn_subscribe = [{ exact = "alice.cmd" }]
"#,
    );
}

// ---------------------------------------------------------------------------
// Refusals in the value layer
// ---------------------------------------------------------------------------

/// A matcher is a legal thing to write in an `acl` statement, so one reaching
/// a value position is user error with its own wording rather than a shape
/// mismatch or a broken tree. A consumer body is one of the positions that
/// carries a value with no vocabulary over it, so a matcher reaches lowering
/// there.
#[test]
fn a_matcher_in_a_value_position_is_refused_as_one() {
    let error = refusal(
        r#"
channel cmd at "brenn:alice.cmd" {
    push_depth = 8;
    retain_depth = 32;
    standing_retain_depth = 64;
}

component Router {
    abi = processor;
    component_path = "/lib/brenn_router.wasm";
    in inbound;
}

new router: Router {
    grants = [];
    store_path = exact "alice.";

    in inbound <- cmd;
}
"#,
    );
    assert_eq!(error.message, "`store_path`: a matcher is not a value here");
    assert_eq!(
        error.line_col(),
        Some((16, 18)),
        "the span is the matcher's own token: {}",
        error.render()
    );
}

/// A list of strings refuses the element that is not a string, at that element
/// — not the whole list at the attr.
#[test]
fn a_non_string_element_of_a_string_list_is_refused_at_the_element() {
    let error = refusal(
        r#"
agent Assistant() {
    allowed_users = ["alice", 3];
}

new alice: Assistant();
"#,
    );
    assert_eq!(
        error.message,
        "`allowed_users`: expected a string, got an integer"
    );
}

/// Two bad elements report twice: a list is walked whole, like every other
/// position here.
#[test]
fn two_bad_elements_of_a_string_list_are_both_reported() {
    let errors = lowered(
        r#"
agent Assistant() {
    allowed_users = ["alice", 3, true];
}

new alice: Assistant();
"#,
    )
    .expect_err("the document must be refused");
    assert_eq!(errors.len(), 2, "{}", render(&errors));
    for error in &errors {
        assert!(
            error
                .message
                .starts_with("`allowed_users`: expected a string"),
            "{}",
            error.render()
        );
    }
}

/// A single string where a list belongs is refused at the attr, with the list
/// named.
#[test]
fn a_bare_string_where_a_string_list_belongs_is_refused() {
    let error = refusal(
        r#"
agent Assistant() {
    disabled_tools = "WebSearch";
}

new alice: Assistant();
"#,
    );
    assert_eq!(
        error.message,
        "`disabled_tools`: expected a list of strings, got a string"
    );
}

/// An mcp server's `env` is a table of strings, and the entry whose value is
/// not one is refused at that value.
#[test]
fn a_non_string_env_entry_is_refused_at_the_entry() {
    let error = refusal(
        r#"
agent Assistant() {
    mcp_server graf {
        command = "graf";
        env = { GRAF_ROOT = 3 };
    }
}

new alice: Assistant();
"#,
    );
    assert_eq!(error.message, "`env`: expected a string, got an integer");
}

/// A table where a table of strings belongs — the whole-value refusal of the
/// same reader.
#[test]
fn a_bare_string_where_an_env_table_belongs_is_refused() {
    let error = refusal(
        r#"
agent Assistant() {
    mcp_server graf {
        command = "graf";
        env = "GRAF_ROOT";
    }
}

new alice: Assistant();
"#,
    );
    assert_eq!(
        error.message,
        "`env`: expected a table of strings, got a string"
    );
}

// ---------------------------------------------------------------------------
// A lowered config through the boot gates
// ---------------------------------------------------------------------------

/// Equality against a TOML twin says the two representations agree; it does not
/// say the runtime accepts either. This runs a lowered config through the two
/// gates a boot puts it through — the channel directory and the whole-config
/// resolver — and compares the resolved channel entries against those the TOML
/// twin produces while spelling its durable address bare, which is the form
/// today's TOML corpus uses and lowering never emits.
#[test]
fn a_lowered_config_resolves_the_same_entries_a_bare_address_toml_does() {
    let dir = tempfile::tempdir().expect("a tempdir");
    let repo_dir = dir.path().join("repos");
    let clone = repo_dir.join("life");
    std::fs::create_dir_all(&clone).expect("the repo clone directory");
    let runtime_dir = dir.path().join("run");
    std::fs::create_dir_all(&runtime_dir).expect("the runtime directory");
    let repos = repo_dir.display();

    let from_dsl = lowered(&format!(
        r#"
server {{ public_url = "https://brenn.example.com"; }}
repo_sync {{ repo_dir = "{repos}"; }}

repo life {{ remote = "forgejo@example.com:alice/life.git"; }}

channel cmd at "brenn:alice.cmd" {{
    push_depth = 8;
    retain_depth = 32;
    standing_retain_depth = 64;
}}

channel presence at "ephemeral:alice.presence" {{ push_depth = 4; retain_depth = 8; }}

agent Assistant() {{
    grants = [subscribe];
    mount life {{ working_dir = true; }}
    // Pull-only: a pushed subscription would need `singleton = true`, which
    // the resolver checks and which is beside this test's point.
    subscribe cmd {{ push_depth = 0; retain_depth = 8; }}
}}

new alice: Assistant();
"#
    ))
    .unwrap_or_else(|errors| panic!("the document must lower:\n{}", render(&errors)));

    let from_toml: BrennConfig = toml::from_str(&format!(
        r#"
[server]
public_url = "https://brenn.example.com"

[repo_sync]
repo_dir = "{repos}"

[[repo]]
slug = "life"
remote = "forgejo@example.com:alice/life.git"

[[channel]]
uuid = "c88e5596-574b-53d1-9b55-6e612b8f3d49"
address = "alice.cmd"
push_depth = 8
retain_depth = 32
standing_retain_depth = 64

[[channel]]
address = "ephemeral:alice.presence"
push_depth = 4
retain_depth = 8

[[app]]
slug = "alice"
grants = ["messaging_subscribe"]

[[app.mount]]
repo = "life"
working_dir = true

[[app.messaging.subscribe]]
channel = "brenn:alice.cmd"
push_depth = 0
retain_depth = 8

[app.acl]
brenn_subscribe = [{{ exact = "alice.cmd" }}]
"#
    ))
    .expect("the TOML twin must parse");

    // The runtime canonicalizes a bare address to `brenn:`, so the two configs
    // — one qualified on emit, one bare as written — must resolve to the same
    // entries. `ChannelEntry` has no `PartialEq`; its `Debug` covers every
    // resolved field.
    let dsl_entries =
        crate::messaging::config::build_channel_entries(&from_dsl.channels, &from_dsl.messaging);
    let toml_entries =
        crate::messaging::config::build_channel_entries(&from_toml.channels, &from_toml.messaging);
    assert_eq!(format!("{dsl_entries:#?}"), format!("{toml_entries:#?}"));

    // And the whole config passes the resolver every boot runs.
    let resolved = crate::config::validate_and_resolve(
        &from_dsl,
        &crate::integration::IntegrationRegistry::new(vec![]),
        Some(&runtime_dir),
    );
    let app = resolved
        .apps
        .get("alice")
        .expect("the lowered app resolves");
    assert_eq!(app.working_dir, clone);
}

// ---------------------------------------------------------------------------
// WASM consumers
// ---------------------------------------------------------------------------

/// A consumer stating every key its body admits, every binding direction, and
/// entries on all nine ACL families a consumer has: the whole transcription
/// surface of `[[wasm_consumer]]` in one row.
///
/// Nine families means nine adjacent, identical-shaped lowering lines, which is
/// the shape where a copy-paste crosses two permission families in silence —
/// the highest-consequence mistake this module can make. So every one of them
/// carries an entry here, checked against the TOML twin, and the two families
/// of each plane carry *different* patterns: identical ones would be blind to
/// exactly the crossing, since a transposed pair would still compare equal.
#[test]
fn a_consumer_lowers_with_every_key_binding_and_acl_family() {
    assert_equivalent(
        r#"
mqtt_client broker {
    url = "mqtts://broker.example.com:8883";
}

channel alerts at "brenn:alice-alerts" {
    push_depth = 8;
    retain_depth = 128;
    standing_retain_depth = 128;
}

channel digests at "brenn:alice-digests" {
    push_depth = 2;
    retain_depth = 32;
    standing_retain_depth = 32;
}

channel presence at "ephemeral:alice-desk.presence" {
    push_depth = 4;
    retain_depth = 16;
}

channel status at "ephemeral:alice-desk.status" {
    push_depth = 2;
    retain_depth = 8;
}

webhook push_alice {
    slug = "push-alice";
    mount = "/webhooks/push-alice";

    signature {
        scheme = bearer-token;
        header = "authorization";
    }

    token phone { secret_file = "/home/alice/.secrets/push-alice.token"; }
}

component Router {
    abi = processor;
    component_path = "/lib/brenn_router.wasm";
    in inbound;
    in feed;
    in status;
    in hook;
    out outbound;
    out digest;
    io acks;
    io tick;
}

new router: Router {
    slug = "router";
    grants = [ports, store, log, config, mqtt];
    store_path = "/state/router.db";
    store_size_limit = "64MiB";
    activation_burst = 4;
    activation_min_period_ms = 250;
    config = { mode = "fanout", depth = 3, verbose = true };

    acl subscribe [
        exact alerts,
        exact presence,
        prefix "local:router.in.",
        exact "local:router.acks",
        topic_filter "mqtt:broker:alice/#",
        endpoint "webhook:push-alice"
    ];
    acl publish [
        exact digests,
        exact status,
        prefix "local:router.out.",
        exact "local:router.acks",
        client "mqtt:broker"
    ];

    in inbound <- alerts {
        push_depth = 4;
        retain_depth = unbounded;
        noise = metered;
        wake_min = low;
        amplification = 0.5;
    }
    in feed <- "local:router.in.feed" { push_depth = 2; retain_depth = 4; }
    in status <- presence { push_depth = 2; retain_depth = 4; }
    in hook <- "webhook:push-alice" { push_depth = 1; retain_depth = 2; }
    out digest -> digests { urgency = low; }
    out outbound -> status {
        urgency = high;
        publish_per_activation = 2;
        publish_capacity = 3.5;
    }
    io acks <-> "local:router.acks" { push_depth = 1; retain_depth = 2; }
    io tick {
        push_depth = 1;
        retain_depth = 2;
        noise = alarm;
        amplification = 1;
        urgency = low;
        publish_per_activation = 1;
        publish_capacity = 1;
    }
}
"#,
        r#"
[[mqtt_client]]
slug = "broker"
url = "mqtts://broker.example.com:8883"

[[channel]]
uuid = "85a5cf7e-6874-5766-9d69-712784754a1f"
address = "brenn:alice-alerts"
push_depth = 8
retain_depth = 128
standing_retain_depth = 128

[[channel]]
uuid = "8b2e83fc-6121-55ef-a665-7bea3fb6a9a6"
address = "brenn:alice-digests"
push_depth = 2
retain_depth = 32
standing_retain_depth = 32

[[channel]]
address = "ephemeral:alice-desk.presence"
push_depth = 4
retain_depth = 16

[[channel]]
address = "ephemeral:alice-desk.status"
push_depth = 2
retain_depth = 8

[[webhook_endpoint]]
slug = "push-alice"
mount = "/webhooks/push-alice"
signature = { scheme = "bearer-token", header = "authorization" }
token = [{ token_id = "phone", secret_file = "/home/alice/.secrets/push-alice.token" }]

[[wasm_consumer]]
slug = "router"
component_path = "/lib/brenn_router.wasm"
grants = ["ports", "store", "log", "config", "mqtt"]
store_path = "/state/router.db"
store_size_limit = "64MiB"
activation_burst = 4
activation_min_period_ms = 250
subscribe_acl = [{ exact = "alice-alerts" }]
publish_acl = [{ exact = "alice-digests" }]
ephemeral_subscribe_acl = [{ exact = "alice-desk.presence" }]
ephemeral_publish_acl = [{ exact = "alice-desk.status" }]
local_subscribe_acl = [{ prefix = "router.in." }, { exact = "router.acks" }]
local_publish_acl = [{ prefix = "router.out." }, { exact = "router.acks" }]
mqtt_subscribe_acl = [{ client = "broker", topic_filter = "alice/#" }]
mqtt_publish_acl = [{ client = "broker" }]
webhook_acl = [{ endpoint = "push-alice" }]

[wasm_consumer.config]
mode = "fanout"
depth = 3
verbose = true

[[wasm_consumer.subscription]]
port = "inbound"
channel = "brenn:alice-alerts"
push_depth = 4
retain_depth = "unbounded"
noise = "metered"
wake_min = "low"
amplification = 0.5

[[wasm_consumer.subscription]]
port = "feed"
channel = "local:router.in.feed"
push_depth = 2
retain_depth = 4

[[wasm_consumer.subscription]]
port = "status"
channel = "ephemeral:alice-desk.presence"
push_depth = 2
retain_depth = 4

[[wasm_consumer.subscription]]
port = "hook"
channel = "webhook:push-alice"
push_depth = 1
retain_depth = 2

[[wasm_consumer.output]]
port = "digest"
channel = "brenn:alice-digests"
urgency = "low"

[[wasm_consumer.output]]
port = "outbound"
channel = "ephemeral:alice-desk.status"
urgency = "high"
publish_per_activation = 2.0
publish_capacity = 3.5

[[wasm_consumer.io_port]]
port = "acks"
channel = "local:router.acks"
push_depth = 1
retain_depth = 2

[[wasm_consumer.io_port]]
port = "tick"
push_depth = 1
retain_depth = 2
noise = "alarm"
amplification = 1.0
urgency = "low"
publish_per_activation = 1.0
publish_capacity = 1.0
"#,
    );
}

/// The minimal consumer body: a class, a grant and one input. Every optional
/// key is omitted on the DSL side and every defaulted key on the TOML side, so
/// this is the defaults-parity lock for `[[wasm_consumer]]` — and with no `acl`
/// statement the input's own authority is what derivation reads off the
/// binding.
#[test]
fn a_minimal_consumer_lowers_with_serdes_defaults() {
    assert_equivalent(
        r#"
channel utterance at "ephemeral:alice-pod.utterance" {
    push_depth = 4;
    retain_depth = 16;
}

component Logger {
    abi = processor;
    component_path = "/lib/brenn_logger.wasm";
    in heard;
}

new logger: Logger {
    grants = [log];

    in heard <- utterance;
}
"#,
        r#"
[[channel]]
address = "ephemeral:alice-pod.utterance"
push_depth = 4
retain_depth = 16

[[wasm_consumer]]
slug = "logger"
component_path = "/lib/brenn_logger.wasm"
grants = ["log"]
ephemeral_subscribe_acl = [{ exact = "alice-pod.utterance" }]

[[wasm_consumer.subscription]]
port = "heard"
channel = "ephemeral:alice-pod.utterance"
"#,
    );
}

/// The operator config map is one of the two positions where a raw field
/// literally stores TOML, so it is the one place a value tree is the target.
///
/// The entries are strings, integers and booleans because that is what the
/// runtime accepts here: `resolve_component_config` is a flat surface and
/// panics at boot on a float, an array or a nested table, whichever front end
/// wrote it. So this row pins the transcription *and* a config that boots.
#[test]
fn a_consumers_config_map_carries_typed_scalars() {
    assert_equivalent(
        r#"
component Sink {
    abi = processor;
    component_path = "/lib/brenn_sink.wasm";
    io tick;
}

new sink: Sink {
    grants = [config];
    config = {
        mode = "fast",
        window_secs = 30,
        strict = true,
    };

    io tick { push_depth = 1; retain_depth = 2; }
}
"#,
        r#"
[[wasm_consumer]]
slug = "sink"
component_path = "/lib/brenn_sink.wasm"
grants = ["config"]

[wasm_consumer.config]
mode = "fast"
window_secs = 30
strict = true

[[wasm_consumer.io_port]]
port = "tick"
push_depth = 1
retain_depth = 2
"#,
    );
}

/// Two consumers in one document, distinguishable on the axis lowering zips:
/// each pairs a resolved instance with its derived authority by position, so a
/// mis-pairing would hand one consumer the other's grants and ACLs. That is a
/// silent privilege transfer rather than a parse error, and a single-entity row
/// cannot see it.
#[test]
fn two_consumers_keep_their_own_grants_and_acls() {
    assert_equivalent(
        r#"
channel alerts at "brenn:alice-alerts" {
    push_depth = 8;
    retain_depth = 128;
    standing_retain_depth = 128;
}

channel presence at "ephemeral:alice-desk.presence" {
    push_depth = 4;
    retain_depth = 16;
}

component Router {
    abi = processor;
    component_path = "/lib/brenn_router.wasm";
    in inbound;
}

component Sink {
    abi = processor;
    component_path = "/lib/brenn_sink.wasm";
    in feed;
}

new router: Router {
    grants = [log];

    in inbound <- alerts { push_depth = 4; retain_depth = 8; }
}

new sink: Sink {
    grants = [store, config];
    store_path = "/state/sink.db";

    in feed <- presence { push_depth = 2; retain_depth = 4; }
}
"#,
        r#"
[[channel]]
uuid = "85a5cf7e-6874-5766-9d69-712784754a1f"
address = "brenn:alice-alerts"
push_depth = 8
retain_depth = 128
standing_retain_depth = 128

[[channel]]
address = "ephemeral:alice-desk.presence"
push_depth = 4
retain_depth = 16

[[wasm_consumer]]
slug = "router"
component_path = "/lib/brenn_router.wasm"
grants = ["log"]
subscribe_acl = [{ exact = "alice-alerts" }]

[[wasm_consumer.subscription]]
port = "inbound"
channel = "brenn:alice-alerts"
push_depth = 4
retain_depth = 8

[[wasm_consumer]]
slug = "sink"
component_path = "/lib/brenn_sink.wasm"
grants = ["store", "config"]
store_path = "/state/sink.db"
ephemeral_subscribe_acl = [{ exact = "alice-desk.presence" }]

[[wasm_consumer.subscription]]
port = "feed"
channel = "ephemeral:alice-desk.presence"
push_depth = 2
retain_depth = 4
"#,
    );
}

/// The operator config map is a `toml::Value` position, and the transcription
/// recurses: a float stays a float, a list stays an array in order, and an
/// inner table stays a table.
///
/// The two front ends must agree on the `toml::Value` they produce for the same
/// map — that is the claim `rval_to_toml`'s recursion makes.
#[test]
fn a_consumers_config_map_transcribes_floats_lists_and_nested_tables() {
    assert_equivalent(
        r#"
component Sink {
    abi = processor;
    component_path = "/lib/brenn_sink.wasm";
    io tick;
}

new sink: Sink {
    grants = [config];
    config = {
        rate = 1.5,
        tags = ["fast", "quiet"],
        limits = { max = 3, spill = false },
    };

    io tick { push_depth = 1; retain_depth = 2; }
}
"#,
        r#"
[[wasm_consumer]]
slug = "sink"
component_path = "/lib/brenn_sink.wasm"
grants = ["config"]

[wasm_consumer.config]
rate = 1.5
tags = ["fast", "quiet"]
limits = { max = 3, spill = false }

[[wasm_consumer.io_port]]
port = "tick"
push_depth = 1
retain_depth = 2
"#,
    );
}

/// A refusal inside a nested config value cites the inner token, not the map.
#[test]
fn a_matcher_nested_in_a_config_list_is_refused_at_the_inner_token() {
    let refusal = refusal(
        r#"
component Sink {
    abi = processor;
    component_path = "/lib/brenn_sink.wasm";
    io tick;
}

new sink: Sink {
    grants = [config];
    config = { tags = ["fast", exact "alice."] };

    io tick { push_depth = 1; retain_depth = 2; }
}
"#,
    );
    assert_eq!(refusal.message, "`config`: a matcher is not a value here");
    assert_eq!(
        refusal.span.text_str(),
        Some("exact \"alice.\""),
        "the span is the inner element's own, not the map's"
    );
}

/// A budget knob is a number, and a whole one written without a point is the
/// same number: what is refused is a value that is no number at all.
#[test]
fn a_non_number_in_a_budget_position_is_refused() {
    let refusal = refusal(
        r#"
component Sink {
    abi = processor;
    component_path = "/lib/brenn_sink.wasm";
    io tick;
}

new sink: Sink {
    grants = [log];

    io tick { push_depth = 1; retain_depth = 2; amplification = "fast"; }
}
"#,
    );
    assert_eq!(
        refusal.message,
        "`amplification`: expected a number, got a string"
    );
}

/// A matcher is a legal thing to write in an open position, so a matcher where
/// the config map expects a value is user error rather than a broken tree —
/// refused at the matcher's own token.
#[test]
fn a_matcher_in_a_consumers_config_map_is_refused() {
    let refusal = refusal(
        r#"
component Sink {
    abi = processor;
    component_path = "/lib/brenn_sink.wasm";
    io tick;
}

new sink: Sink {
    grants = [config];
    config = { mode = exact "alice." };

    io tick { push_depth = 1; retain_depth = 2; }
}
"#,
    );
    assert_eq!(refusal.message, "`config`: a matcher is not a value here");
}

// ---------------------------------------------------------------------------
// Surfaces
// ---------------------------------------------------------------------------

/// A surface stating every attr its vocabulary admits, two component instances
/// between them stating every body key and every binding direction, and entries
/// on all four ACL families a surface has: the whole transcription surface of
/// `[[surface]]` in one row.
///
/// The binding tables are flat under the surface and carry the instance name,
/// which is the raw config's shape rather than the document's nesting.
#[test]
fn a_surface_lowers_with_every_key_component_and_acl_family() {
    assert_equivalent(
        r#"
channel alerts at "brenn:alice-alerts" {
    push_depth = 8;
    retain_depth = 128;
    standing_retain_depth = 128;
}

channel presence at "ephemeral:alice-desk.presence" {
    push_depth = 4;
    retain_depth = 16;
}

component Panel {
    abi = dom;
    in messages;
    out outbound;
    io acks;
    io tick;
}

component Chrome {
    abi = dom;
    in state;
}

surface alice_desk {
    slug = "alice-desk";
    grants = [subscribe, publish, alert, takeover];
    skin = "bench";
    allowed_users = ["alice", "bob"];
    publish_burst = 32;
    publish_per_sec = 4;

    acl subscribe [exact alerts, prefix "ephemeral:alice-desk."];
    acl publish [prefix "brenn:alice-desk.", exact presence];

    new panel: Panel {
        send_burst = 16;
        send_refill_secs = 30;
        parked_batch_depth = unbounded;
        config = { mode = "compact", layout = "wide" };

        in messages <- alerts {
            push_depth = 4;
            retain_depth = 8;
            noise = metered;
            wake_min = low;
        }
        out outbound -> presence {
            urgency = high;
            publish_per_activation = 2;
            publish_capacity = 3.5;
        }
        io acks <-> "local:panel/acks" { push_depth = 1; retain_depth = 2; }
        io tick {
            push_depth = 1;
            retain_depth = 2;
            noise = alarm;
            urgency = low;
            publish_per_activation = 1;
            publish_capacity = 1;
        }
    }

    new chrome: Chrome {
        chrome = true;

        in state <- presence { push_depth = 1; }
    }
}
"#,
        r#"
[[channel]]
uuid = "85a5cf7e-6874-5766-9d69-712784754a1f"
address = "brenn:alice-alerts"
push_depth = 8
retain_depth = 128
standing_retain_depth = 128

[[channel]]
address = "ephemeral:alice-desk.presence"
push_depth = 4
retain_depth = 16

[[surface]]
slug = "alice-desk"
grants = ["subscribe", "ephemeral_subscribe", "publish", "ephemeral_publish", "alert", "takeover"]
skin = "bench"
allowed_users = ["alice", "bob"]
publish_burst = 32
publish_per_sec = 4
subscribe_acl = [{ exact = "alice-alerts" }]
publish_acl = [{ prefix = "alice-desk." }]
ephemeral_subscribe_acl = [{ prefix = "alice-desk." }]
ephemeral_publish_acl = [{ exact = "alice-desk.presence" }]

[[surface.component]]
kind = "panel"
instance = "panel"
abi = "dom"
send_burst = 16
send_refill_secs = 30
parked_batch_depth = "unbounded"

[surface.component.config]
mode = "compact"
layout = "wide"

[[surface.component]]
kind = "chrome"
instance = "chrome"
abi = "dom"
chrome = true

[[surface.subscription]]
channel = "brenn:alice-alerts"
instance = "panel"
port = "messages"
push_depth = 4
retain_depth = 8
noise = "metered"
wake_min = "low"

[[surface.subscription]]
channel = "ephemeral:alice-desk.presence"
instance = "chrome"
port = "state"
push_depth = 1

[[surface.output]]
instance = "panel"
port = "outbound"
channel = "ephemeral:alice-desk.presence"
urgency = "high"
publish_per_activation = 2.0
publish_capacity = 3.5

[[surface.io_port]]
instance = "panel"
port = "acks"
channel = "local:panel/acks"
push_depth = 1
retain_depth = 2

[[surface.io_port]]
instance = "panel"
port = "tick"
push_depth = 1
retain_depth = 2
noise = "alarm"
urgency = "low"
publish_per_activation = 1.0
publish_capacity = 1.0
"#,
    );
}

/// The minimal surface: a grant, one instance and one input. Every optional
/// attr is omitted on the DSL side and every defaulted key on the TOML side, so
/// this is the defaults-parity lock for `[[surface]]` — the wire slug falls back
/// to the handle, the instance name to the `new` handle, and `chrome` to false.
#[test]
fn a_minimal_surface_lowers_with_serdes_defaults() {
    assert_equivalent(
        r#"
channel utterance at "ephemeral:alice-pod.utterance" {
    push_depth = 4;
    retain_depth = 16;
}

component Widget {
    abi = dom;
    in heard;
}

surface alice_pod {
    grants = [subscribe];

    new widget: Widget {
        in heard <- utterance { push_depth = 2; }
    }
}
"#,
        r#"
[[channel]]
address = "ephemeral:alice-pod.utterance"
push_depth = 4
retain_depth = 16

[[surface]]
slug = "alice_pod"
grants = ["ephemeral_subscribe"]
ephemeral_subscribe_acl = [{ exact = "alice-pod.utterance" }]

[[surface.component]]
kind = "widget"
instance = "widget"
abi = "dom"

[[surface.subscription]]
channel = "ephemeral:alice-pod.utterance"
instance = "widget"
port = "heard"
push_depth = 2
"#,
    );
}

/// Two surfaces in one document, each with its own component class: lowering
/// zips a resolved surface with both its derived authority and its wire-kind
/// list, so a mis-pairing would render one surface with the other's component
/// kinds and subscribe with the other's ACLs. A one-surface row cannot see it.
#[test]
fn two_surfaces_keep_their_own_component_kinds_and_acls() {
    assert_equivalent(
        r#"
channel alerts at "brenn:alice-alerts" {
    push_depth = 8;
    retain_depth = 128;
    standing_retain_depth = 128;
}

channel presence at "ephemeral:alice-desk.presence" {
    push_depth = 4;
    retain_depth = 16;
}

component Panel {
    abi = dom;
    in messages;
}

component Board {
    abi = dom;
    in feed;
}

surface alice_desk {
    grants = [subscribe];
    skin = "bench";

    new panel: Panel {
        in messages <- alerts { push_depth = 4; }
    }
}

surface bob_desk {
    grants = [subscribe];
    skin = "lab";

    new board: Board {
        in feed <- presence { push_depth = 2; }
    }
}
"#,
        r#"
[[channel]]
uuid = "85a5cf7e-6874-5766-9d69-712784754a1f"
address = "brenn:alice-alerts"
push_depth = 8
retain_depth = 128
standing_retain_depth = 128

[[channel]]
address = "ephemeral:alice-desk.presence"
push_depth = 4
retain_depth = 16

[[surface]]
slug = "alice_desk"
grants = ["subscribe"]
skin = "bench"
subscribe_acl = [{ exact = "alice-alerts" }]

[[surface.component]]
kind = "panel"
instance = "panel"
abi = "dom"

[[surface.subscription]]
channel = "brenn:alice-alerts"
instance = "panel"
port = "messages"
push_depth = 4

[[surface]]
slug = "bob_desk"
grants = ["ephemeral_subscribe"]
skin = "lab"
ephemeral_subscribe_acl = [{ exact = "alice-desk.presence" }]

[[surface.component]]
kind = "board"
instance = "board"
abi = "dom"

[[surface.subscription]]
channel = "ephemeral:alice-desk.presence"
instance = "board"
port = "feed"
push_depth = 2
"#,
    );
}

/// A binding tail's vocabulary is the union across the families the statement
/// can lower into, so the key a surface port has no field for — `amplification`,
/// a consumer's throughput knob — is refused at lowering, at its own entry,
/// naming the port and the keys that direction reads.
#[test]
fn amplification_on_a_surface_in_binding_is_refused() {
    let refusal = refusal(
        r#"
channel utterance at "ephemeral:alice-pod.utterance" {
    push_depth = 4;
    retain_depth = 16;
}

component Widget {
    abi = dom;
    in heard;
}

surface alice_pod {
    grants = [subscribe];

    new widget: Widget {
        in heard <- utterance { push_depth = 2; amplification = 0.5; }
    }
}
"#,
    );
    assert_eq!(
        refusal.message,
        "`amplification` is not a key of the `heard` port of instance `widget` of surface \
         `alice_pod`; expected `push_depth`, `retain_depth`, `noise` or `wake_min`"
    );
    assert_eq!(
        refusal.line_col(),
        Some((16, 65)),
        "the span is the refused key's own value: {}",
        refusal.render()
    );
}

/// The `io` twin of the refusal above: an io tail unions both directions, and
/// the surface families still hold no `amplification` field.
#[test]
fn amplification_on_a_surface_io_binding_is_refused() {
    let refusal = refusal(
        r#"
channel utterance at "ephemeral:alice-pod.utterance" {
    push_depth = 4;
    retain_depth = 16;
}

component Widget {
    abi = dom;
    in heard;
    io tick;
}

surface alice_pod {
    grants = [subscribe];

    new widget: Widget {
        in heard <- utterance { push_depth = 2; }
        io tick { push_depth = 1; retain_depth = 2; amplification = 0.5; }
    }
}
"#,
    );
    assert_eq!(
        refusal.message,
        "`amplification` is not a key of the `tick` port of instance `widget` of surface \
         `alice_pod`; expected `push_depth`, `retain_depth`, `noise`, `urgency`, \
         `publish_per_activation` or `publish_capacity`"
    );
    assert_eq!(
        refusal.line_col(),
        Some((18, 69)),
        "the span is the refused key's own value: {}",
        refusal.render()
    );
}

/// The surface twin of the one-token-one-diagnostic rule: a refused
/// `amplification` is not also read as a number.
#[test]
fn a_refused_surface_binding_key_is_not_also_value_checked() {
    let refusal = refusal(
        r#"
channel utterance at "ephemeral:alice-pod.utterance" {
    push_depth = 4;
    retain_depth = 16;
}

component Widget {
    abi = dom;
    in heard;
}

surface alice_pod {
    grants = [subscribe];

    new widget: Widget {
        in heard <- utterance { push_depth = 2; amplification = "half"; }
    }
}
"#,
    );
    assert!(
        refusal
            .message
            .starts_with("`amplification` is not a key of"),
        "{}",
        refusal.render()
    );
}

/// A component body's values are typed at lowering, and a refusal cites the
/// offending token rather than the instance.
#[test]
fn a_bad_value_in_a_surface_component_body_is_refused() {
    let refusal = refusal(
        r#"
channel utterance at "ephemeral:alice-pod.utterance" {
    push_depth = 4;
    retain_depth = 16;
}

component Widget {
    abi = dom;
    in heard;
}

surface alice_pod {
    grants = [subscribe];

    new widget: Widget {
        parked_batch_depth = -1;

        in heard <- utterance { push_depth = 2; }
    }
}
"#,
    );
    assert_eq!(
        refusal.message,
        "`parked_batch_depth`: expected a non-negative integer or the word `unbounded`, got -1"
    );
}

/// A surface component's `config` is a map of strings, not the `toml::Table` a
/// consumer's is, so a non-string value is refused at that value.
#[test]
fn a_non_string_in_a_surface_components_config_is_refused() {
    let refusal = refusal(
        r#"
channel utterance at "ephemeral:alice-pod.utterance" {
    push_depth = 4;
    retain_depth = 16;
}

component Widget {
    abi = dom;
    in heard;
}

surface alice_pod {
    grants = [subscribe];

    new widget: Widget {
        config = { depth = 3 };

        in heard <- utterance { push_depth = 2; }
    }
}
"#,
    );
    assert_eq!(
        refusal.message,
        "`config`: expected a string, got an integer"
    );
}

// ---------------------------------------------------------------------------
// Remotes
// ---------------------------------------------------------------------------

/// A remote stating every attr, with ceilings on both subscribe planes and
/// matchers on both publish planes.
#[test]
fn a_remote_lowers_with_ceilings_on_both_subscribe_planes() {
    assert_equivalent(
        r#"
remote bob_pod {
    token_file = "/home/alice/.secrets/bob-pod.token";
    grants = [subscribe, publish, alert];
    publish_burst = 16;
    publish_per_sec = 2;
    max_sessions = 4;
    max_subscriptions = 64;

    acl subscribe [
        exact "brenn:alice.cmd" { push_depth = 0, retain_depth = 32 },
        prefix "ephemeral:alice." { push_depth = 8, retain_depth = 1 }
    ];
    acl publish [prefix "brenn:alice.in.", exact "ephemeral:bob.presence"];
}
"#,
        r#"
[[remote]]
slug = "bob_pod"
token_file = "/home/alice/.secrets/bob-pod.token"
grants = ["subscribe", "ephemeral_subscribe", "publish", "ephemeral_publish", "alert"]
publish_burst = 16
publish_per_sec = 2
max_sessions = 4
max_subscriptions = 64
subscribe_acl = [{ exact = "alice.cmd", push_depth = 0, retain_depth = 32 }]
ephemeral_subscribe_acl = [{ prefix = "alice.", push_depth = 8, retain_depth = 1 }]
publish_acl = [{ prefix = "alice.in." }]
ephemeral_publish_acl = [{ exact = "bob.presence" }]
"#,
    );
}

/// The minimal remote: a token file, one grant and the entry that grant is
/// about. Every optional attr is omitted on the DSL side and every defaulted
/// key on the TOML side, so this is the defaults-parity lock for `[[remote]]`.
#[test]
fn a_minimal_remote_lowers_with_serdes_defaults() {
    assert_equivalent(
        r#"
remote bob_pod {
    token_file = "/home/alice/.secrets/bob-pod.token";
    grants = [subscribe];

    acl subscribe [exact "brenn:alice.cmd" { push_depth = 1, retain_depth = 8 }];
}
"#,
        r#"
[[remote]]
slug = "bob_pod"
token_file = "/home/alice/.secrets/bob-pod.token"
grants = ["subscribe"]
subscribe_acl = [{ exact = "alice.cmd", push_depth = 1, retain_depth = 8 }]
"#,
    );
}

/// Two remotes in one document, with different ceilings on different planes:
/// lowering zips a resolved remote with its derived authority by position, and
/// a mis-pairing here hands one peer the other's subscribe ceilings. A
/// one-remote row cannot see it.
#[test]
fn two_remotes_keep_their_own_ceilings() {
    assert_equivalent(
        r#"
remote bob_pod {
    token_file = "/home/alice/.secrets/bob-pod.token";
    grants = [subscribe];
    max_sessions = 4;

    acl subscribe [exact "brenn:alice.cmd" { push_depth = 1, retain_depth = 8 }];
}

remote charlie_pod {
    token_file = "/home/alice/.secrets/charlie-pod.token";
    grants = [subscribe, publish];
    max_sessions = 2;

    acl subscribe [prefix "ephemeral:alice." { push_depth = 8, retain_depth = 1 }];
    acl publish [exact "brenn:alice.in.charlie"];
}
"#,
        r#"
[[remote]]
slug = "bob_pod"
token_file = "/home/alice/.secrets/bob-pod.token"
grants = ["subscribe"]
max_sessions = 4
subscribe_acl = [{ exact = "alice.cmd", push_depth = 1, retain_depth = 8 }]

[[remote]]
slug = "charlie_pod"
token_file = "/home/alice/.secrets/charlie-pod.token"
grants = ["ephemeral_subscribe", "publish"]
max_sessions = 2
ephemeral_subscribe_acl = [{ prefix = "alice.", push_depth = 8, retain_depth = 1 }]
publish_acl = [{ exact = "alice.in.charlie" }]
"#,
    );
}

/// A remote attr is typed at lowering like any other, and a count out of the
/// target's range is refused at the token that wrote it.
#[test]
fn a_remote_count_out_of_range_is_refused() {
    let refusal = refusal(
        r#"
remote bob_pod {
    token_file = "/home/alice/.secrets/bob-pod.token";
    grants = [subscribe];
    max_sessions = 5000000000;

    acl subscribe [exact "brenn:alice.cmd" { push_depth = 1, retain_depth = 8 }];
}
"#,
    );
    assert_eq!(
        refusal.message,
        "`max_sessions`: 5000000000 is out of range for this key"
    );
}

// ---------------------------------------------------------------------------
// Webhook endpoints
// ---------------------------------------------------------------------------

/// A webhook stating every attr, an HMAC signature scheme, two key entries and
/// a replay-protection binding with a nested config map.
#[test]
fn a_webhook_endpoint_lowers_with_every_block() {
    assert_equivalent(
        r#"
webhook alice_inbox {
    slug = "alice-inbox";
    mount = "/webhooks/alice-inbox";
    description = "Pushes from alice's phone.";
    transport_ceiling_bytes = 65536;
    content_type = "application/json";
    urgency = high;

    signature {
        scheme = hmac-raw-body;
        algorithm = "hmac-sha512";
        header = "x-signature";
        format = "hex-lower";
        key_id_header = "x-key-id";
    }

    key primary { secret_file = "/home/alice/.secrets/inbox-primary.key"; }
    key rotated { secret_file = "/home/alice/.secrets/inbox-rotated.key"; }

    replay_protection {
        component_path = "/opt/brenn/lib/replay.wasm";
        store_path = "/var/lib/brenn/alice-inbox-replay.sqlite";
        store_size_limit = "128MiB";
        config = { window_secs = 300, strict = true };
    }
}
"#,
        r#"
[[webhook_endpoint]]
slug = "alice-inbox"
mount = "/webhooks/alice-inbox"
description = "Pushes from alice's phone."
transport_ceiling_bytes = 65536
content_type = "application/json"
urgency = "high"

[webhook_endpoint.signature]
scheme = "hmac-raw-body"
algorithm = "hmac-sha512"
header = "x-signature"
format = "hex-lower"
key_id_header = "x-key-id"

[[webhook_endpoint.key]]
key_id = "primary"
secret_file = "/home/alice/.secrets/inbox-primary.key"

[[webhook_endpoint.key]]
key_id = "rotated"
secret_file = "/home/alice/.secrets/inbox-rotated.key"

[webhook_endpoint.replay_protection]
component_path = "/opt/brenn/lib/replay.wasm"
store_path = "/var/lib/brenn/alice-inbox-replay.sqlite"
store_size_limit = "128MiB"
config = { window_secs = 300, strict = true }
"#,
    );
}

/// The minimal webhook: a bearer scheme and the one token it checks against.
/// Every optional attr is omitted on the DSL side and every defaulted key on
/// the TOML side, so this is the defaults-parity lock for
/// `[[webhook_endpoint]]` — the wire slug falls back to the handle, and
/// `transport_ceiling_bytes`, `content_type` and `algorithm` to serde's own
/// default functions.
#[test]
fn a_minimal_webhook_endpoint_lowers_with_serdes_defaults() {
    assert_equivalent(
        r#"
webhook alice_inbox {
    signature {
        scheme = bearer-token;
        header = "authorization";
    }

    token phone { secret_file = "/home/alice/.secrets/inbox-phone.token"; }
}
"#,
        r#"
[[webhook_endpoint]]
slug = "alice_inbox"

[webhook_endpoint.signature]
scheme = "bearer-token"
header = "authorization"

[[webhook_endpoint.token]]
token_id = "phone"
secret_file = "/home/alice/.secrets/inbox-phone.token"
"#,
    );
}

/// The timestamped-body scheme, whose fields are the widest of the four: the
/// signature parity row for that variant.
#[test]
fn the_timestamped_body_signature_scheme_lowers() {
    assert_equivalent(
        r#"
webhook alice_inbox {
    signature {
        scheme = hmac-timestamped-body;
        sig_header = "x-signature";
        sig_format = "v0=hex-lower";
        timestamp_header = "x-request-timestamp";
        template = "v0:{t}:{body}";
        max_skew_secs = 300;
    }

    key primary { secret_file = "/home/alice/.secrets/inbox-primary.key"; }
}
"#,
        r#"
[[webhook_endpoint]]
slug = "alice_inbox"

[webhook_endpoint.signature]
scheme = "hmac-timestamped-body"
sig_header = "x-signature"
sig_format = "v0=hex-lower"
timestamp_header = "x-request-timestamp"
template = "v0:{t}:{body}"
max_skew_secs = 300

[[webhook_endpoint.key]]
key_id = "primary"
secret_file = "/home/alice/.secrets/inbox-primary.key"
"#,
    );
}

/// The combined-header scheme: the signature parity row for that variant.
#[test]
fn the_stripe_signature_scheme_lowers() {
    assert_equivalent(
        r#"
webhook alice_inbox {
    signature {
        scheme = hmac-stripe;
        header = "stripe-signature";
        max_skew_secs = 300;
    }

    key primary { secret_file = "/home/alice/.secrets/inbox-primary.key"; }
}
"#,
        r#"
[[webhook_endpoint]]
slug = "alice_inbox"

[webhook_endpoint.signature]
scheme = "hmac-stripe"
header = "stripe-signature"
max_skew_secs = 300

[[webhook_endpoint.key]]
key_id = "primary"
secret_file = "/home/alice/.secrets/inbox-primary.key"
"#,
    );
}

/// The `signature` vocabulary is the union of every scheme's fields, so an attr
/// belonging to another variant is refused at lowering — the same answer
/// `deny_unknown_fields` gives the TOML path, at the key that was written and
/// naming the fields the chosen scheme reads.
#[test]
fn a_signature_attr_the_chosen_scheme_has_no_field_for_is_refused() {
    let refusal = refusal(
        r#"
webhook alice_inbox {
    signature {
        scheme = bearer-token;
        header = "authorization";
        max_skew_secs = 300;
    }

    token phone { secret_file = "/home/alice/.secrets/inbox-phone.token"; }
}
"#,
    );
    assert_eq!(
        refusal.message,
        "`max_skew_secs` is not a key of `signature` of webhook `alice_inbox`; expected \
         `scheme`, `header` or `token_id_header`"
    );
    assert_toml_refused(
        r#"
[[webhook_endpoint]]
slug = "alice_inbox"

[webhook_endpoint.signature]
scheme = "bearer-token"
header = "authorization"
max_skew_secs = 300

[[webhook_endpoint.token]]
token_id = "phone"
secret_file = "/home/alice/.secrets/inbox-phone.token"
"#,
        "max_skew_secs",
    );
}

/// A field the chosen scheme requires and the block does not state is refused
/// at the block, which is the finest position a missing key has.
#[test]
fn a_signature_missing_a_required_field_is_refused() {
    let refusal = refusal(
        r#"
webhook alice_inbox {
    signature {
        scheme = hmac-raw-body;
        header = "x-signature";
    }

    key primary { secret_file = "/home/alice/.secrets/inbox-primary.key"; }
}
"#,
    );
    assert_eq!(
        refusal.message,
        "`signature` of webhook `alice_inbox` states no `format`, which it requires"
    );
    assert_toml_refused(
        r#"
[[webhook_endpoint]]
slug = "alice_inbox"

[webhook_endpoint.signature]
scheme = "hmac-raw-body"
header = "x-signature"

[[webhook_endpoint.key]]
key_id = "primary"
secret_file = "/home/alice/.secrets/inbox-primary.key"
"#,
        "format",
    );
}

/// The scheme word is matched against the raw enum's own tags, and a word that
/// is not one of them is refused naming the four that are.
#[test]
fn an_unknown_signature_scheme_is_refused() {
    let refusal = refusal(
        r#"
webhook alice_inbox {
    signature {
        scheme = hmac-blake3;
        header = "x-signature";
    }
}
"#,
    );
    assert_eq!(
        refusal.message,
        "`hmac-blake3` is not a signature scheme; expected `hmac-raw-body`, \
         `hmac-timestamped-body`, `hmac-stripe` or `bearer-token`"
    );
    assert_eq!(
        refusal.span.text_str(),
        Some("hmac-blake3"),
        "the span is the scheme word's own, not the block's"
    );
    assert_toml_refused(
        r#"
[[webhook_endpoint]]
slug = "alice_inbox"

[webhook_endpoint.signature]
scheme = "hmac-blake3"
header = "x-signature"
"#,
        "unknown variant",
    );
}

/// A webhook with no signature block is refused rather than defaulted: which
/// scheme guards an endpoint is the reason for declaring one.
#[test]
fn a_webhook_endpoint_with_no_signature_block_is_refused() {
    let refusal = refusal(
        r#"
webhook alice_inbox {
    mount = "/webhooks/alice-inbox";
}
"#,
    );
    assert_eq!(
        refusal.message,
        "webhook `alice_inbox` states no `signature` block: which scheme guards an endpoint \
         has no default"
    );
}
