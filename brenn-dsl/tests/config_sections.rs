//! The top-level configuration sections: what each kindword selects, what its
//! vocabulary admits, and what it refuses.
//!
//! These are the sections that carry the server's own settings rather than an
//! entity; no keyword leads them.

use std::mem::Discriminant;

use brenn_dsl::model::{
    ALERTING_BLOCK_KINDWORDS, AlertingBlock, CONFIG_BLOCK_KINDWORDS, ConfigBlock, File,
    OBSERVABILITY_BLOCK_KINDWORDS, ObservabilityBlock, SectionNode, Value, alerting_block,
    config_block, observability_block, section_kindword,
};
use brenn_dsl::parse_str;

mod support;

use support::corpus_file;

fn config() -> File {
    corpus_file("config.brenn")
}

/// The one section of `file` whose kindword is `kindword`.
fn section<'a>(file: &'a File, kindword: &str) -> &'a SectionNode {
    file.sections()
        .find(|node| section_kindword(node).0 == kindword)
        .unwrap_or_else(|| panic!("the corpus writes a `{kindword}` section"))
}

/// The one section of `file` named `kindword`, typed.
fn typed(file: &File, kindword: &str) -> ConfigBlock {
    config_block(section(file, kindword)).unwrap_or_else(|error| panic!("{error}"))
}

/// The lone section of a document written inline, typed.
fn typed_str(src: &str) -> Result<ConfigBlock, brenn_dsl::diag::Diagnostic> {
    let file = parse_str(src, "t.brenn").expect("a parse");
    let node = file.sections().next().expect("one section");
    config_block(node)
}

// ── the dispatch ─────────────────────────────────────────────────────────────

/// Every kindword the dispatch admits is written by the corpus, types, and
/// selects a target no other kindword selects.
///
/// The dispatch table is hand-maintained, and a section held as a CST subtree is
/// never deserialized until something calls `config_block` on it — so a kindword
/// no fixture writes is a whole vocabulary that has never been run. Driving the
/// loop off the const is what makes adding one cost a fixture section.
#[test]
fn every_config_kindword_is_written_by_the_corpus_and_selects_its_own_target() {
    let file = config();
    let mut selected: Vec<(&str, Discriminant<ConfigBlock>)> = Vec::new();

    for kindword in CONFIG_BLOCK_KINDWORDS {
        let node = file
            .sections()
            .find(|node| section_kindword(node).0 == *kindword)
            .unwrap_or_else(|| panic!("no corpus section writes the `{kindword}` config block"));
        let block = config_block(node).unwrap_or_else(|error| panic!("{kindword}: {error}"));

        let variant = std::mem::discriminant(&block);
        if let Some((first, _)) = selected.iter().find(|(_, seen)| *seen == variant) {
            panic!("`{kindword}` selects the same target as `{first}`");
        }
        selected.push((kindword, variant));
    }
}

/// The `server` section, read back: the one required key and the doc comment a
/// section carries.
#[test]
fn a_config_section_carries_its_required_key_and_its_doc() {
    let file = config();
    let ConfigBlock::Server(server) = typed(&file, "server") else {
        panic!("the server section");
    };
    assert!(matches!(
        server.attrs.public_url.value.value(),
        Value::Str(_)
    ));
    let doc = server.doc.as_ref().expect("the section carries a doc");
    assert_eq!(doc.lines.len(), 1);
}

/// A section that writes none of its optional keys is legal: every config
/// section exists to override a few fields of a struct that defaults the rest,
/// so an `opt` key mis-declared `req` breaks documents the fixtures never write.
#[test]
fn a_config_section_writing_no_optional_key_is_accepted() {
    for src in [
        "database { }\n",
        "logging { }\n",
        "security { }\n",
        "claude_defaults { }\n",
        "repo_sync { }\n",
        "messaging { }\n",
        "observability { }\n",
        "surface_description { }\n",
        "llm_chat { }\n",
        "pwa_push { }\n",
        "automation { }\n",
        "events { }\n",
        "wasm { }\n",
        "watchdog { }\n",
    ] {
        typed_str(src).unwrap_or_else(|error| panic!("{src}: {error}"));
    }
}

/// The sections whose fields are token contexts: a level, a noise policy, a
/// sink, a wake threshold and a publish floor are all bare words.
#[test]
fn the_policy_naming_fields_are_token_contexts() {
    let file = config();

    let ConfigBlock::Logging(logging) = typed(&file, "logging") else {
        panic!("the logging section");
    };
    assert_eq!(
        logging
            .attrs
            .console_level
            .as_ref()
            .expect("a console level")
            .value
            .as_str(),
        "info"
    );
    assert_eq!(
        logging
            .attrs
            .file_level
            .as_ref()
            .expect("a file level")
            .value
            .as_str(),
        "debug"
    );

    let ConfigBlock::Messaging(messaging) = typed(&file, "messaging") else {
        panic!("the messaging section");
    };
    for (field, spelling) in [
        (&messaging.attrs.default_noise, "silent"),
        (&messaging.attrs.default_sink, "drop"),
        (&messaging.attrs.default_wake_min, "normal"),
    ] {
        assert_eq!(
            field.as_ref().expect("a policy word").value.as_str(),
            spelling
        );
    }
    assert!(matches!(
        messaging
            .attrs
            .default_send_rate
            .as_ref()
            .expect("a send rate")
            .value
            .value(),
        Value::Table(_)
    ));

    // A key the document omits is absent, not defaulted: what a missing key
    // means is lowering's, and the model records only what was written.
    let ConfigBlock::Messaging(bare) = typed_str("messaging { }\n").expect("a bare section") else {
        panic!("the messaging section");
    };
    assert!(bare.attrs.default_send_rate.is_none());

    let ConfigBlock::LlmChat(chat) = typed(&file, "llm_chat") else {
        panic!("the llm_chat section");
    };
    assert_eq!(
        chat.attrs
            .wake_min
            .as_ref()
            .expect("a wake threshold")
            .value
            .as_str(),
        "normal"
    );
}

/// A container definition carries its name, and `image` and `home_dir` are
/// required.
#[test]
fn a_container_block_carries_its_name_and_requires_its_image() {
    let file = config();
    let ConfigBlock::Container(container) = typed(&file, "container") else {
        panic!("the container section");
    };
    assert_eq!(
        container.name.as_ref().expect("the block is named").value(),
        "cc"
    );
    assert!(matches!(container.attrs.image.value.value(), Value::Str(_)));
    assert!(container.attrs.extra_args.is_some());

    let error = typed_str("container cc { home_dir = \"/home/alice\"; }\n")
        .expect_err("an image is required");
    assert!(error.message.contains("image"), "{}", error.message);
}

/// A block whose identity is its name is refused without one: the grammar makes
/// every section's name optional, and only the kindword knows which ones need
/// one.
#[test]
fn a_container_block_without_a_name_is_refused() {
    let error = typed_str(
        "container {\n    image = \"brenn-cc:latest\";\n    home_dir = \"/home/alice\";\n}\n",
    )
    .expect_err("a container is named");
    assert_eq!(
        error.message,
        "a `container` block is named: `container <name> { … }`"
    );
    assert_eq!(error.line_col(), Some((1, 1)));
}

#[test]
fn an_integration_block_without_a_name_is_refused() {
    let error = typed_str("integration {\n    command = \"graf\";\n}\n")
        .expect_err("an integration is named");
    assert_eq!(
        error.message,
        "a `integration` block is named: `integration <name> { … }`"
    );
    assert_eq!(error.line_col(), Some((1, 1)));
}

/// The other direction: a name written on a block that has nothing to do with
/// one would be silently dropped.
#[test]
fn a_name_on_a_nameless_config_section_is_refused_at_the_name() {
    let error = typed_str("database alpha {\n    path = \"/var/lib/brenn/brenn.db\";\n}\n")
        .expect_err("a database section takes no name");
    assert_eq!(error.message, "a `database` block takes no name");
    assert_eq!(error.line_col(), Some((1, 10)));
}

/// `public_url` has a config default like every other server key, and is
/// required here anyway: without it the server does not start, and this layer
/// can say so at the block.
#[test]
fn a_server_section_without_its_public_url_is_refused() {
    let error = typed_str("server {\n    bind_address = \"127.0.0.1:3000\";\n}\n")
        .expect_err("a public url is required");
    assert!(error.message.contains("public_url"), "{}", error.message);
}

// ── nesting ──────────────────────────────────────────────────────────────────

/// An `alerting` section's backends are held sub-blocks, typed by their own
/// dispatch.
#[test]
fn an_alerting_section_holds_its_backends_as_sub_blocks() {
    let file = config();
    let ConfigBlock::Alerting(alerting) = typed(&file, "alerting") else {
        panic!("the alerting section");
    };
    assert!(matches!(
        alerting.attrs.max_alerts.value.value(),
        Value::Int(_)
    ));
    assert_eq!(alerting.subs.len(), 2);

    let AlertingBlock::Ntfy(ntfy) = alerting_block(&alerting.subs[0]).expect("the ntfy backend")
    else {
        panic!("the first sub-block is ntfy");
    };
    assert!(matches!(ntfy.attrs.url.value.value(), Value::Str(_)));

    let AlertingBlock::Mail(mail) = alerting_block(&alerting.subs[1]).expect("the mail backend")
    else {
        panic!("the second sub-block is mail");
    };
    assert!(mail.attrs.subject_label.is_some());

    // The dispatch is per context, so the same held node is a legal block in one
    // and an unknown kindword in another.
    let error =
        observability_block(&alerting.subs[0]).expect_err("`ntfy` is not an observability block");
    assert!(error.message.contains("usage"), "{}", error.message);
}

/// The nested dispatches get the same gate as the top-level one: a sub-block
/// kindword no fixture writes is a vocabulary nothing deserializes.
#[test]
fn every_nested_kindword_is_written_by_the_corpus() {
    let file = config();

    let ConfigBlock::Alerting(alerting) = typed(&file, "alerting") else {
        panic!("the alerting section");
    };
    for kindword in ALERTING_BLOCK_KINDWORDS {
        let node = alerting
            .subs
            .iter()
            .find(|node| section_kindword(node).0 == *kindword)
            .unwrap_or_else(|| panic!("no corpus `alerting` section writes `{kindword}`"));
        alerting_block(node).unwrap_or_else(|error| panic!("{kindword}: {error}"));
    }

    let ConfigBlock::Observability(observability) = typed(&file, "observability") else {
        panic!("the observability section");
    };
    for kindword in OBSERVABILITY_BLOCK_KINDWORDS {
        let node = observability
            .subs
            .iter()
            .find(|node| section_kindword(node).0 == *kindword)
            .unwrap_or_else(|| panic!("no corpus `observability` section writes `{kindword}`"));
        observability_block(node).unwrap_or_else(|error| panic!("{kindword}: {error}"));
    }
}

#[test]
fn an_observability_section_holds_its_usage_sub_block() {
    let file = config();
    let ConfigBlock::Observability(observability) = typed(&file, "observability") else {
        panic!("the observability section");
    };
    assert_eq!(
        observability
            .attrs
            .surface_error_publish_floor
            .as_ref()
            .expect("a floor")
            .value
            .as_str(),
        "warn"
    );
    assert_eq!(observability.subs.len(), 1);

    let ObservabilityBlock::Usage(usage) =
        observability_block(&observability.subs[0]).expect("the usage sub-block");
    assert!(usage.attrs.session_gap_minutes.is_some());
}

// ── the refusals ─────────────────────────────────────────────────────────────

#[test]
fn an_unknown_top_level_kindword_is_refused_at_its_span_naming_the_legal_set() {
    let file = parse_str("telemetry {\n    a = 1;\n}\n", "unknown-section.brenn")
        .expect("the grammar admits any kindword");
    let node = file.sections().next().expect("one section");
    let error = config_block(node).expect_err("`telemetry` is not a config section");

    assert!(error.message.contains("`telemetry`"), "{}", error.message);
    assert!(
        error.message.contains("observability"),
        "the message names the legal set: {}",
        error.message
    );
    assert_eq!(error.file, "unknown-section.brenn");
    assert_eq!(error.line_col(), Some((1, 1)));
}

#[test]
fn an_unknown_key_in_a_config_section_is_refused_at_the_key() {
    let error = typed_str("server {\n    bind_address = \"0.0.0.0:3000\";\n    port = 3000;\n}\n")
        .expect_err("`port` is not a server key");
    assert!(error.message.contains("port"), "{}", error.message);
    assert!(
        error.message.contains("bind_address"),
        "the message names the legal set: {}",
        error.message
    );
    assert_eq!(error.line_col(), Some((3, 5)));
}

/// A config field whose shape is a nested table has no attr spelling, so
/// writing it as one is an unknown key rather than a silently accepted value.
#[test]
fn a_nested_table_config_field_has_no_attr_spelling() {
    let error = typed_str("server {\n    integrations = { graf = 1 };\n}\n")
        .expect_err("`integrations` is not a server key");
    assert!(error.message.contains("integrations"), "{}", error.message);
}

/// The level fields are token contexts, so a quoted spelling is refused.
#[test]
fn a_quoted_log_level_is_refused() {
    let error =
        typed_str("logging {\n    console_level = \"info\";\n}\n").expect_err("a level is a word");
    assert!(error.message.contains("bare word"), "{}", error.message);
    assert_eq!(error.line_col(), Some((2, 21)));
}

#[test]
fn an_alerting_section_without_its_rate_limit_is_refused() {
    let error =
        typed_str("alerting {\n    max_alerts = 10;\n}\n").expect_err("`window_secs` is required");
    assert!(error.message.contains("window_secs"), "{}", error.message);
}
