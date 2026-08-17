//! Entity attr vocabularies: what each body admits, which of its fields are
//! token contexts, and what a key outside the vocabulary costs.

use brenn_dsl::model::{ChannelDef, File, IntOrWord, Value};
use brenn_dsl::parse_str;

mod support;

use support::corpus_file;

fn entities() -> File {
    corpus_file("entities.brenn")
}

/// The diagnostic from parsing `src` as a standalone file.  Vocabulary
/// checking runs during the parse, so a refused key is a parse failure.
fn refusal(src: &str) -> brenn_dsl::diag::Diagnostic {
    parse_str(src, "t.brenn").expect_err("the body is outside the vocabulary")
}

// ── the vocabularies ─────────────────────────────────────────────────────────

/// A channel body is where the token contexts cluster: three depth fields that
/// take a count or a word, and three enum-valued fields that take a word.
#[test]
fn a_channel_body_types_its_depths_and_its_words() {
    let file = entities();
    let ChannelDef::Decl(declared) = file.channels().next().expect("a channel") else {
        panic!("the channel names a handle");
    };
    let tuning = &declared.body.as_ref().expect("a body").attrs;

    assert!(matches!(
        tuning.push_depth.as_ref().expect("a push depth").value,
        IntOrWord::Int(_)
    ));
    let IntOrWord::Word(standing) = &tuning
        .standing_retain_depth
        .as_ref()
        .expect("a standing depth")
        .value
    else {
        panic!("`unbounded` is a word, not a count");
    };
    assert_eq!(standing.as_str(), "unbounded");

    assert_eq!(
        tuning.noise.as_ref().expect("noise").value.as_str(),
        "metered"
    );
    assert_eq!(
        tuning.sink.as_ref().expect("sink").value.as_str(),
        "archive"
    );
    assert_eq!(
        tuning.wake_min.as_ref().expect("wake_min").value.as_str(),
        "low"
    );
    assert!(tuning.description.is_some());
    assert!(matches!(
        tuning
            .send_rate
            .as_ref()
            .expect("a send rate")
            .value
            .value(),
        Value::Table(_)
    ));
}

#[test]
fn a_surface_body_types_its_grants_as_words() {
    let file = entities();
    let surface = file.surfaces().next().expect("a surface");
    let spellings: Vec<&str> = surface
        .attrs
        .grants
        .value
        .words
        .iter()
        .map(|word| word.as_str())
        .collect();
    assert_eq!(spellings, ["subscribe", "publish", "takeover"]);
    assert_eq!(surface.acls.len(), 1, "an ACL is a statement, not an attr");
}

/// The widest vocabulary in the language. That every key the fixture writes
/// reaches a field of its own is the fixture parse itself: the vocabulary
/// denies unknown fields, so a key with no field fails the load in `entities()`.
/// What the config struct declares and what this vocabulary spells are tied by
/// nothing mechanical — that is `TODO(dsl-vocabulary-config-parity)`, not this
/// test.
#[test]
fn an_agent_body_admits_the_whole_scalar_vocabulary() {
    let file = entities();
    let agent = file.agents().next().expect("an agent class");
    let attrs = &agent.attrs;

    assert_eq!(attrs.grants.as_ref().expect("grants").value.words.len(), 2);
    assert!(matches!(
        attrs
            .allowed_users
            .as_ref()
            .expect("allowed users")
            .value
            .value(),
        Value::List(_)
    ));
    assert!(matches!(
        attrs
            .single_instance
            .as_ref()
            .expect("a single-instance flag")
            .value
            .value(),
        Value::Bool(_)
    ));
}

#[test]
fn a_remote_body_carries_its_quotas_and_its_grants() {
    let file = entities();
    let remote = file.remotes().next().expect("a remote");
    assert!(matches!(
        remote.attrs.token_file.value.value(),
        Value::Str(_)
    ));
    assert_eq!(remote.attrs.grants.value.words.len(), 2);
    assert!(matches!(
        remote
            .attrs
            .max_subscriptions
            .as_ref()
            .expect("a subscription quota")
            .value
            .value(),
        Value::Int(_)
    ));
    assert_eq!(remote.acls.len(), 1);
}

#[test]
fn a_webhook_body_types_its_urgency_as_a_word() {
    let file = entities();
    let endpoint = file.webhooks().next().expect("a webhook");
    assert_eq!(
        endpoint
            .attrs
            .urgency
            .as_ref()
            .expect("urgency")
            .value
            .as_str(),
        "high"
    );
    assert_eq!(endpoint.blocks.len(), 1, "the signature is a sub-block");
}

/// The three `keyword name { … }` declarations share a struct and differ only
/// in the vocabulary it is parameterized by.
#[test]
fn the_named_declarations_each_get_their_own_vocabulary() {
    let file = entities();

    let repo = file.repos().next().expect("a repo");
    assert!(repo.body.attrs.auto_pull.is_some());

    let client = file.mqtt_clients().next().expect("an mqtt client");
    assert_eq!(
        client
            .body
            .attrs
            .urgency
            .as_ref()
            .expect("urgency")
            .value
            .as_str(),
        "normal",
        "urgency is a token context"
    );
    assert!(client.body.attrs.tls_version_min.is_some());
    assert!(client.body.attrs.session_expiry_secs.is_some());

    let server = file.mcp_servers().next().expect("an mcp server");
    assert!(server.body.attrs.args.is_some());
    assert!(server.body.attrs.env.is_some());
}

// ── the refusals ─────────────────────────────────────────────────────────────

/// A key the vocabulary does not have is refused where it was written, with the
/// legal set named.
#[test]
fn an_unknown_key_is_refused_at_the_key_naming_the_legal_set() {
    let error = refusal("surface s {\n    nope = 1;\n}\n");
    assert!(error.message.contains("nope"), "{}", error.message);
    assert!(
        error.message.contains("publish_burst"),
        "the message names the legal set: {}",
        error.message
    );
    assert_eq!(error.line_col(), Some((2, 5)));
}

/// What a statement of the body carries is not also an attr: a surface's slug is
/// its declaration name, so writing `slug` is outside the vocabulary.
#[test]
fn a_key_a_statement_carries_is_not_an_attr() {
    let error = refusal("surface s {\n    slug = \"s\";\n}\n");
    assert!(error.message.contains("slug"), "{}", error.message);

    let error = refusal("agent A {\n    mcp_servers = [];\n}\n");
    assert!(error.message.contains("mcp_servers"), "{}", error.message);
}

/// A config field with a nested-table shape has no attr spelling yet, and says
/// so rather than accepting a value nothing will read.
#[test]
fn a_config_field_with_no_attr_spelling_is_an_unknown_key() {
    let error = refusal("agent A {\n    approval_rules = [];\n}\n");
    assert!(
        error.message.contains("approval_rules"),
        "{}",
        error.message
    );
}

#[test]
fn a_missing_required_key_is_refused() {
    for (src, key) in [
        ("repo life {\n}\n", "remote"),
        ("mqtt_client broker {\n}\n", "url"),
        ("mcp_server graf {\n}\n", "command"),
        // Transport rights are stated, not defaulted: deny-by-default is only
        // deny-by-default if omitting the key is a refusal.
        ("surface s {\n}\n", "grants"),
        ("remote r {\n    grants = [subscribe];\n}\n", "token_file"),
        (
            "remote r {\n    token_file = \"/home/alice/.secrets/r.token\";\n}\n",
            "grants",
        ),
    ] {
        let error = refusal(src);
        assert!(
            error.message.contains(key),
            "the message names the missing key {key}: {}",
            error.message
        );
    }
}

/// The other half of `req`: a body that writes only its required keys is legal,
/// so an `opt` key mis-declared `req` fails here rather than in a user's
/// document. The fixtures write every key, so nothing else exercises this.
#[test]
fn a_body_writing_only_its_required_keys_is_accepted() {
    for src in [
        "channel c at \"brenn:c\" {\n}\n",
        "agent A {\n}\n",
        "surface s {\n    grants = [subscribe];\n}\n",
        "remote r {\n    token_file = \"/home/alice/.secrets/r.token\";\n    grants = [subscribe];\n}\n",
        "webhook w {\n}\n",
        "repo life {\n    remote = \"forgejo@example.com:alice/life.git\";\n}\n",
        "mqtt_client broker {\n    url = \"mqtts://broker.example.com:8883\";\n}\n",
        "mcp_server graf {\n    command = \"graf\";\n}\n",
    ] {
        parse_str(src, "t.brenn").unwrap_or_else(|error| panic!("{src}: {error}"));
    }
}

/// A token context refuses a quoted spelling: the word is the point, and a
/// string there is a different value the resolver would have to guess about.
#[test]
fn a_token_context_refuses_a_string() {
    let error = refusal("channel c at \"brenn:c\" {\n    noise = \"metered\";\n}\n");
    assert!(
        error.message.contains("expected a bare word"),
        "{}",
        error.message
    );
}

/// `grants` is a list of words in every vocabulary that has one, so a quoted
/// spelling is refused whether it is the list or one of its elements.
#[test]
fn a_word_list_context_refuses_a_string_and_a_quoted_element() {
    let error = refusal("surface s {\n    grants = \"publish\";\n}\n");
    assert!(
        error.message.contains("expected a list of bare words"),
        "{}",
        error.message
    );

    let error = refusal("agent A {\n    grants = [\"publish\"];\n}\n");
    assert!(
        error.message.contains("element 1"),
        "the message names the element: {}",
        error.message
    );
}
