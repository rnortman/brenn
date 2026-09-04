//! Typed sections and token contexts: what the kindword selects, what the
//! projections accept, and what each refusal says and where.

use fltk_serde_core::Spanned;

use brenn_dsl::model::{
    AGENT_BLOCK_KINDWORDS, AgentBlock, AgentClass, AttachmentBlock, File, INSTANCE_BLOCK_KINDWORDS,
    IntOrWord, Item, SectionNode, TOOL_BLOCK_KINDWORDS, Value, WEBHOOK_BLOCK_KINDWORDS,
    WebhookBlock, WebhookDef, Word, WordList, agent_block, attachment_block, instance_block,
    section_kindword, tool_block, webhook_block,
};
use brenn_dsl::parse_str;

mod support;

use support::corpus_file;

fn sections() -> File {
    corpus_file("sections.brenn")
}

fn webhook<'a>(file: &'a File, name: &str) -> &'a WebhookDef {
    file.webhooks()
        .find(|webhook| webhook.name.value() == name)
        .unwrap_or_else(|| panic!("the corpus declares a webhook named {name}"))
}

fn agent<'a>(file: &'a File, name: &str) -> &'a AgentClass {
    file.agents()
        .find(|agent| agent.name.value() == name)
        .unwrap_or_else(|| panic!("the corpus declares an agent class named {name}"))
}

/// The value of the sole `const` declaration in `src`.
fn only_const_value(src: &str) -> Spanned<Value> {
    let file = parse_str(src, "t.brenn").expect("a parse");
    let Item::ConstDef(constant) = file
        .items
        .into_iter()
        .next()
        .expect("one item")
        .into_value()
    else {
        panic!("a constant");
    };
    constant.value
}

// ── the two-phase dispatch ───────────────────────────────────────────────────

/// Every kindword the HMAC endpoint writes selects its own target, and each
/// body deserializes with its own vocabulary.
#[test]
fn a_webhook_body_types_each_sub_block_by_its_kindword() {
    let file = sections();
    let endpoint = webhook(&file, "push_alice");
    assert_eq!(endpoint.blocks.len(), 3);

    let typed: Vec<_> = endpoint
        .blocks
        .iter()
        .map(|block| webhook_block(block).unwrap_or_else(|error| panic!("{error}")))
        .collect();

    let WebhookBlock::Signature(signature) = &typed[0] else {
        panic!("the first block is the signature");
    };
    assert_eq!(signature.kindword.value(), "signature");
    assert_eq!(
        signature.attrs.scheme.value.as_str(),
        "hmac-timestamped-body"
    );
    assert!(signature.attrs.max_skew_secs.is_some());
    // A field belonging to another scheme's variant is simply absent; which
    // fields the named scheme requires is lowering's check.
    assert!(signature.attrs.token_id_header.is_none());

    let WebhookBlock::Key(key) = &typed[1] else {
        panic!("the second block is the key");
    };
    assert_eq!(
        key.name.as_ref().expect("the key is named").value(),
        "primary"
    );
    assert!(matches!(
        key.attrs.secret_file.value.value(),
        Value::Fstr(_)
    ));
    let doc = key.doc.as_ref().expect("the key carries a doc comment");
    assert_eq!(doc.lines.len(), 1);

    let WebhookBlock::ReplayProtection(replay) = &typed[2] else {
        panic!("the third block is the replay guard");
    };
    assert!(replay.attrs.store_size_limit.is_some());
    assert!(replay.attrs.config.is_none());
}

/// The bearer endpoint writes the other two of the four legal kindwords.
#[test]
fn the_bearer_endpoint_writes_a_token_where_the_hmac_one_writes_a_key() {
    let file = sections();
    let endpoint = webhook(&file, "push_bob");
    let typed: Vec<_> = endpoint
        .blocks
        .iter()
        .map(|block| webhook_block(block).unwrap_or_else(|error| panic!("{error}")))
        .collect();

    let WebhookBlock::Signature(signature) = &typed[0] else {
        panic!("the first block is the signature");
    };
    assert_eq!(signature.attrs.scheme.value.as_str(), "bearer-token");
    assert!(signature.attrs.sig_header.is_none());

    let WebhookBlock::Token(token) = &typed[1] else {
        panic!("the second block is the token");
    };
    assert_eq!(
        token.name.as_ref().expect("the token is named").value(),
        "primary"
    );
}

/// The three hook kindwords share one vocabulary, so what distinguishes them is
/// the variant the dispatch selects.
#[test]
fn an_agent_body_types_each_of_its_hook_blocks() {
    let file = sections();
    let class = agent(&file, "PersonalAssistant");
    assert_eq!(class.blocks.len(), 7);

    let typed: Vec<_> = class
        .blocks
        .iter()
        .map(|block| agent_block(block).unwrap_or_else(|error| panic!("{error}")))
        .collect();

    let AgentBlock::StartHooks(start) = &typed[0] else {
        panic!("the first block is start_hooks");
    };
    assert_eq!(start.kindword.value(), "start_hooks");
    assert!(start.name.is_none());
    assert!(matches!(
        start
            .attrs
            .host
            .as_ref()
            .expect("host scripts")
            .value
            .value(),
        Value::List(_)
    ));
    assert!(start.attrs.container.is_some());

    let AgentBlock::PostPullHooks(post_pull) = &typed[1] else {
        panic!("the second block is post_pull_hooks");
    };
    assert!(post_pull.attrs.host.is_some());
    assert!(post_pull.attrs.container.is_none());

    let AgentBlock::StartupHooks(startup) = &typed[2] else {
        panic!("the third block is startup_hooks");
    };
    assert!(startup.attrs.host.is_none());
    assert!(startup.attrs.container.is_some());
}

/// An `attachment_target` is named, its `handler` is the block it holds, and the
/// handler's `type` is a token context — the word as written, not a reference.
#[test]
fn an_attachment_target_carries_its_handler_block() {
    let file = sections();
    let class = agent(&file, "PersonalAssistant");
    let AgentBlock::AttachmentTarget(target) =
        agent_block(&class.blocks[3]).unwrap_or_else(|error| panic!("{error}"))
    else {
        panic!("the fourth block is the attachment target");
    };
    assert_eq!(
        target.name.as_ref().expect("the target is named").value(),
        "import"
    );
    assert!(target.attrs.name.is_none());
    assert!(target.attrs.multi.is_some());
    assert!(target.doc.is_some());

    let AttachmentBlock::Handler(handler) =
        attachment_block(&target.subs[0]).unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(handler.attrs.r#type.value.as_str(), "command");
    assert!(handler.name.is_none());
    assert!(handler.attrs.file_roles.is_some());
}

/// An `integration_config` block is named and its body is open: every key it
/// wrote is carried, whatever the key is, because the config field is a value
/// tree with no vocabulary to check against.
#[test]
fn an_integration_config_block_carries_an_open_body() {
    let file = sections();
    let class = agent(&file, "PersonalAssistant");

    let AgentBlock::IntegrationConfig(ledger) =
        agent_block(&class.blocks[4]).unwrap_or_else(|error| panic!("{error}"))
    else {
        panic!("the fifth block is the ledger integration config");
    };
    assert_eq!(
        ledger.name.as_ref().expect("the block is named").value(),
        "ledger"
    );
    assert!(ledger.doc.is_some());
    assert!(ledger.subs.is_empty());
    let entries = ledger.attrs.clone().entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].0, "env");
    assert!(matches!(entries[0].1.value(), Value::Table(_)));

    let AgentBlock::IntegrationConfig(calendar) =
        agent_block(&class.blocks[5]).unwrap_or_else(|error| panic!("{error}"))
    else {
        panic!("the sixth block is the calendar integration config");
    };
    assert_eq!(
        calendar.name.as_ref().expect("the block is named").value(),
        "calendar"
    );
    let keys: Vec<String> = calendar
        .attrs
        .clone()
        .entries()
        .into_iter()
        .map(|(key, _)| key)
        .collect();
    assert_eq!(keys, ["env", "timeout_secs"]);
}

/// An `integration_config` block states an integration name: the map key has no
/// default, so the unnamed spelling is refused at the kindword.
#[test]
fn an_unnamed_integration_config_block_is_refused() {
    let file = parse_str(
        "agent A() {\n    integration_config {\n        env = { X = \"1\" };\n    }\n}\n",
        "unnamed-integration-config.brenn",
    )
    .expect("a parse");
    let class = file.agents().next().expect("the class");
    let error = agent_block(&class.blocks[0]).expect_err("the block states no name");
    assert!(
        error.message.contains("integration_config"),
        "{}",
        error.message
    );
}

/// The union vocabulary refuses a key no handler type has, at the key.
#[test]
fn an_unknown_handler_key_is_refused_at_the_key() {
    let file = parse_str(
        "agent A() {\n    attachment_target import {\n        label = \"Import\";\n                 accept = [\".ofx\"];\n        handler {\n            type = command;\n                     shell = true;\n        }\n    }\n}\n",
        "handler-key.brenn",
    )
    .expect("a parse");
    let class = file.agents().next().expect("the class");
    let AgentBlock::AttachmentTarget(target) =
        agent_block(&class.blocks[0]).expect("the target types")
    else {
        panic!("an attachment target");
    };
    let error = attachment_block(&target.subs[0]).expect_err("`shell` is not a handler key");
    assert!(error.message.contains("shell"), "{}", error.message);
    assert!(
        error.message.contains("program"),
        "the message names the legal set: {}",
        error.message
    );
}

/// A handler with no `type` word is refused: which handler kind this is has no
/// default.
#[test]
fn a_handler_without_a_type_is_refused() {
    let file = parse_str(
        "agent A() {\n    attachment_target import {\n        label = \"Import\";\n                 accept = [\".ofx\"];\n        handler { program = \"pf\"; }\n    }\n}\n",
        "handler-type.brenn",
    )
    .expect("a parse");
    let class = file.agents().next().expect("the class");
    let AgentBlock::AttachmentTarget(target) =
        agent_block(&class.blocks[0]).expect("the target types")
    else {
        panic!("an attachment target");
    };
    let error = attachment_block(&target.subs[0]).expect_err("`type` is required");
    assert!(error.message.contains("type"), "{}", error.message);
}

/// An `attachment_target` written without a name defines something nothing can
/// address, so the arity check refuses it.
#[test]
fn an_unnamed_attachment_target_is_refused() {
    let file = parse_str(
        "agent A() {\n    attachment_target { label = \"Import\"; accept = [\".ofx\"]; }\n}\n",
        "unnamed-target.brenn",
    )
    .expect("a parse");
    let class = file.agents().next().expect("the class");
    let error = agent_block(&class.blocks[0]).expect_err("a target is named");
    assert!(
        error.message.contains("attachment_target <name>"),
        "{}",
        error.message
    );
}

/// Every kindword either dispatch admits is written by the corpus and types.
///
/// The tables are hand-maintained, and a sub-block is held as a CST subtree
/// until something types it — so a kindword no fixture writes has never had
/// its vocabulary deserialized, whatever its field names say.
#[test]
fn every_sub_block_kindword_is_written_by_the_corpus() {
    let file = sections();

    let webhook_blocks: Vec<&SectionNode> = file
        .webhooks()
        .flat_map(|endpoint| endpoint.blocks.iter())
        .collect();
    for kindword in WEBHOOK_BLOCK_KINDWORDS {
        let node = webhook_blocks
            .iter()
            .find(|node| section_kindword(node).0 == *kindword)
            .unwrap_or_else(|| panic!("no corpus webhook writes a `{kindword}` block"));
        webhook_block(node).unwrap_or_else(|error| panic!("{kindword}: {error}"));
    }

    let agent_blocks: Vec<&SectionNode> = file
        .agents()
        .flat_map(|class| class.blocks.iter())
        .collect();
    for kindword in AGENT_BLOCK_KINDWORDS {
        let node = agent_blocks
            .iter()
            .find(|node| section_kindword(node).0 == *kindword)
            .unwrap_or_else(|| panic!("no corpus agent writes a `{kindword}` block"));
        agent_block(node).unwrap_or_else(|error| panic!("{kindword}: {error}"));
    }

    let instance_blocks: Vec<&SectionNode> = file
        .instantiations()
        .filter_map(|inst| inst.body.as_ref())
        .flat_map(|body| body.value().blocks.iter())
        .collect();
    for kindword in INSTANCE_BLOCK_KINDWORDS {
        let node = instance_blocks
            .iter()
            .find(|node| section_kindword(node).0 == *kindword)
            .unwrap_or_else(|| panic!("no corpus instance writes a `{kindword}` block"));
        instance_block(node).unwrap_or_else(|error| panic!("{kindword}: {error}"));
    }

    // A `tool` block is the one place a typed section nests a second level, so
    // its own vocabulary is reached through the block that holds it.
    let tool_blocks: Vec<SectionNode> = instance_blocks
        .iter()
        .flat_map(|node| {
            let block = instance_block(node).expect("a legal kindword");
            let (_, subs) = block.parts();
            subs.to_vec()
        })
        .collect();
    for kindword in TOOL_BLOCK_KINDWORDS {
        let node = tool_blocks
            .iter()
            .find(|node| section_kindword(node).0 == *kindword)
            .unwrap_or_else(|| panic!("no corpus `tool` block writes a `{kindword}` block"));
        tool_block(node).unwrap_or_else(|error| panic!("{kindword}: {error}"));
    }
}

/// A held node is a handle, not a walked tree: typing it twice costs a second
/// walk and yields the same value both times.
#[test]
fn a_held_section_can_be_re_entered_more_than_once() {
    let file = sections();
    let endpoint = webhook(&file, "push_bob");
    let first = webhook_block(&endpoint.blocks[1]).expect("the token block");
    let second = webhook_block(&endpoint.blocks[1]).expect("the token block again");
    assert_eq!(first, second);
}

// ── the refusals ─────────────────────────────────────────────────────────────

#[test]
fn an_unknown_kindword_is_refused_at_its_own_span_naming_the_legal_set() {
    let file = parse_str(
        "webhook w {\n    bogus { a = 1; }\n}\n",
        "unknown-kindword.brenn",
    )
    .expect("the grammar admits any kindword");
    let endpoint = webhook(&file, "w");
    let error = webhook_block(&endpoint.blocks[0]).expect_err("`bogus` is not a webhook block");

    assert!(error.message.contains("`bogus`"), "{}", error.message);
    assert!(
        error.message.contains("replay_protection"),
        "the message names the legal set: {}",
        error.message
    );
    assert_eq!(error.file, "unknown-kindword.brenn");
    assert_eq!(error.line_col(), Some((2, 5)));
}

#[test]
fn an_agent_block_refusal_names_the_agent_vocabulary() {
    let file = parse_str("agent A {\n    stop_hooks { host = []; }\n}\n", "t.brenn")
        .expect("the grammar admits any kindword");
    let class = agent(&file, "A");
    let error = agent_block(&class.blocks[0]).expect_err("`stop_hooks` is not an agent block");
    assert!(error.message.contains("start_hooks"), "{}", error.message);
}

#[test]
fn an_unknown_key_inside_a_typed_section_is_refused_at_the_key() {
    let file = parse_str(
        "webhook w {\n    signature {\n        scheme = bearer-token;\n        nope = 1;\n    }\n}\n",
        "unknown-key.brenn",
    )
    .expect("the grammar knows no field names");
    let endpoint = webhook(&file, "w");
    let error = webhook_block(&endpoint.blocks[0]).expect_err("`nope` is not a signature field");

    assert!(error.message.contains("nope"), "{}", error.message);
    assert!(
        error.message.contains("sig_header"),
        "the message names the legal set: {}",
        error.message
    );
    assert_eq!(error.file, "unknown-key.brenn");
    assert_eq!(error.line_col(), Some((4, 9)));
}

#[test]
fn a_missing_required_key_inside_a_typed_section_is_refused() {
    let file = parse_str(
        "webhook w {\n    signature { header = \"authorization\"; }\n}\n",
        "t.brenn",
    )
    .expect("a parse");
    let endpoint = webhook(&file, "w");
    let error = webhook_block(&endpoint.blocks[0]).expect_err("`scheme` is required");
    assert!(error.message.contains("scheme"), "{}", error.message);
}

/// A credential block's name is the id lowering keys it by, so one written
/// without a name would define a secret nothing can select.
#[test]
fn a_key_block_without_a_name_is_refused() {
    let file = parse_str(
        "webhook w {\n    key { secret_file = \"/home/alice/.secrets/push.key\"; }\n}\n",
        "unnamed-key.brenn",
    )
    .expect("a parse");
    let endpoint = webhook(&file, "w");
    let error = webhook_block(&endpoint.blocks[0]).expect_err("a key is named");
    assert_eq!(error.message, "a `key` block is named: `key <name> { … }`");
    assert_eq!(error.line_col(), Some((2, 5)));
}

/// The other direction: a `signature` block has one per endpoint and no id, so a
/// name on it would be dropped.
#[test]
fn a_name_on_a_nameless_webhook_block_is_refused_at_the_name() {
    let file = parse_str(
        "webhook w {\n    signature alpha { scheme = bearer-token; }\n}\n",
        "named-signature.brenn",
    )
    .expect("a parse");
    let endpoint = webhook(&file, "w");
    let error = webhook_block(&endpoint.blocks[0]).expect_err("a signature takes no name");
    assert_eq!(error.message, "a `signature` block takes no name");
    assert_eq!(error.line_col(), Some((2, 15)));
}

/// `scheme` is a token context, so a quoted spelling is refused — the reverse
/// of the raw config's shape, where it is a string.
#[test]
fn a_token_context_refuses_a_quoted_spelling_through_the_bridge() {
    let file = parse_str(
        "webhook w {\n    signature { scheme = \"bearer-token\"; }\n}\n",
        "quoted-scheme.brenn",
    )
    .expect("a parse");
    let endpoint = webhook(&file, "w");
    let error = webhook_block(&endpoint.blocks[0]).expect_err("a scheme is a word");

    assert!(error.message.contains("bare word"), "{}", error.message);
    assert_eq!(error.line_col(), Some((2, 26)));
}

// ── the projections ──────────────────────────────────────────────────────────

#[test]
fn a_word_is_a_single_segment_reference_and_nothing_else() {
    let value = only_const_value("const a = container;\n");
    let word = Word::from_value(&value).expect("a bare word");
    assert_eq!(word.as_str(), "container");
    assert_eq!(word.name.span().text_str(), Some("container"));
}

#[test]
fn a_word_refuses_every_other_value_shape_at_the_value() {
    for (src, found) in [
        ("const a = \"container\";\n", "a string"),
        ("const a = f\"{x}\";\n", "an f-string"),
        ("const a = \"\"\"container\"\"\";\n", "a raw string"),
        ("const a = 1;\n", "an integer"),
        ("const a = 1.5;\n", "a float"),
        ("const a = true;\n", "a boolean"),
        ("const a = [x];\n", "a list"),
        ("const a = { x = 1 };\n", "an inline table"),
        ("const a = exact \"brenn:alice\";\n", "a matcher"),
    ] {
        let value = only_const_value(src);
        let error = Word::from_value(&value).expect_err(src);
        assert_eq!(
            error.message,
            format!("expected a bare word, found {found}"),
            "{src}"
        );
        assert_eq!(error.line_col(), Some((1, 11)), "{src}");
    }
}

#[test]
fn a_word_refuses_a_qualified_reference() {
    let value = only_const_value("const a = alice.desk;\n");
    let error = Word::from_value(&value).expect_err("a word has one segment");
    assert!(error.message.contains("qualified"), "{}", error.message);
}

#[test]
fn a_word_list_projects_every_element_and_reports_the_first_that_is_not_a_word() {
    let value = only_const_value("const a = [subscribe, publish, takeover];\n");
    let list = WordList::from_value(&value).expect("three words");
    let spellings: Vec<_> = list.words.iter().map(Word::as_str).collect();
    assert_eq!(spellings, ["subscribe", "publish", "takeover"]);

    let value = only_const_value("const a = [subscribe, \"publish\"];\n");
    let error = WordList::from_value(&value).expect_err("the second element is a string");
    assert_eq!(
        error.message,
        "element 2: expected a bare word, found a string"
    );
    assert_eq!(error.line_col(), Some((1, 23)));

    let value = only_const_value("const a = subscribe;\n");
    let error = WordList::from_value(&value).expect_err("a list is required");
    assert_eq!(
        error.message,
        "expected a list of bare words, found a reference"
    );
}

#[test]
fn an_int_or_word_takes_both_arms_and_refuses_the_rest() {
    let value = only_const_value("const a = 64;\n");
    let IntOrWord::Int(count) = IntOrWord::from_value(&value).expect("an integer") else {
        panic!("the integer arm");
    };
    assert_eq!(*count.value(), 64);

    // A parse-time projection: the reservation on this spelling is a check at
    // a constant's declaration, and nothing here resolves.
    let value = only_const_value("const a = unbounded;\n");
    assert!(
        IntOrWord::from_value(&value)
            .expect("a reference")
            .is_unbounded(),
        "the reference arm, holding the one word a depth spells"
    );

    let value = only_const_value("const a = depths.geometry;\n");
    let IntOrWord::Name { path, .. } =
        IntOrWord::from_value(&value).expect("a qualified reference")
    else {
        panic!("the reference arm");
    };
    assert_eq!(path.spelling(), "depths.geometry");

    let value = only_const_value("const a = \"64\";\n");
    let error = IntOrWord::from_value(&value).expect_err("a quoted count is not a count");
    assert_eq!(
        error.message,
        "expected a count, the word `unbounded`, or a name that resolves to a count, found a \
         string"
    );
}
