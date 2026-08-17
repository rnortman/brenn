//! Statement-layer suite: every declaration form, what the model makes of it,
//! and the refusals the grammar owns.

use brenn_dsl::model::{
    AgentClass, Binding, ChanRef, ChannelDef, File, IntOrWord, Item, McpServerStmt, PortDir, Value,
    WebhookDef, agent_block, webhook_block,
};
use brenn_dsl::parse_str;

mod support;

use support::corpus_file;

fn statements() -> File {
    corpus_file("statements.brenn")
}

/// Every top-level form in the corpus reaches the model as its own variant.
#[test]
fn the_statement_corpus_covers_every_top_level_form() {
    let file = statements();
    let mut seen = Vec::new();
    for item in &file.items {
        seen.push(match item.value() {
            Item::ConstDef(_) => "const",
            Item::UuidPins(_) => "uuid_pins",
            Item::Component(_) => "component",
            Item::Agent(_) => "agent",
            Item::Assembly(_) => "assembly",
            Item::Channel(_) => "channel",
            Item::Surface(_) => "surface",
            Item::Inst(_) => "new",
            Item::Remote(_) => "remote",
            Item::Webhook(_) => "webhook",
            Item::Repo(_) => "repo",
            Item::MqttClient(_) => "mqtt_client",
            Item::McpServer(_) => "mcp_server",
            Item::Acl(_) => "acl",
            Item::Grant(_) => "grant",
            Item::Section(_) => "section",
        });
    }
    for form in [
        "uuid_pins",
        "component",
        "agent",
        "assembly",
        "channel",
        "new",
        "remote",
        "webhook",
        "repo",
        "mqtt_client",
        "mcp_server",
        "acl",
        "grant",
    ] {
        assert!(seen.contains(&form), "the corpus is missing a {form}");
    }
}

/// The two channel roles are two alternatives, and `prefix` is recorded.
#[test]
fn a_channel_declares_or_tunes() {
    let file = statements();
    let channels: Vec<_> = file.channels().collect();
    assert_eq!(channels.len(), 2);

    let ChannelDef::Decl(declared) = &channels[0] else {
        panic!("the first channel names a handle");
    };
    assert_eq!(declared.handle.value(), "utterance");
    assert!(!declared.addr.is_prefix);
    let tuning = &declared.body.as_ref().expect("a body").attrs;
    assert!(tuning.description.is_some());
    assert!(matches!(
        tuning.push_depth.as_ref().expect("a push depth").value,
        IntOrWord::Int(_)
    ));
    assert!(tuning.retain_depth.is_some());
    assert!(tuning.noise.is_none());

    let ChannelDef::Tuning(tuned) = &channels[1] else {
        panic!("the second channel has no handle");
    };
    assert!(tuned.addr.is_prefix, "`prefix` was written");
}

#[test]
fn uuid_pins_reach_the_model_in_source_order() {
    let file = statements();
    let pins = file.uuid_pins().next().expect("the corpus pins two uuids");
    assert_eq!(pins.pins.len(), 2);
    assert_eq!(pins.pins[0].addr.parts.len(), 1);
}

/// A port's direction is the alternative, and its doctype rides along inert.
#[test]
fn a_port_declaration_carries_a_direction_and_an_optional_doctype() {
    let file = statements();
    let class = file
        .components()
        .find(|class| class.name.value() == "EchoStub")
        .expect("EchoStub is declared");

    let directions: Vec<_> = class
        .ports
        .iter()
        .map(|port| (port.name.value().as_str(), port.dir.value()))
        .collect();
    assert_eq!(
        directions,
        vec![
            ("inbound", &PortDir::Into),
            ("outbound", &PortDir::Outof),
            ("acks", &PortDir::Both),
        ]
    );
    assert!(class.ports[0].doctype.is_some(), "the doctype is parsed");
    assert!(class.ports[1].doctype.is_none());
}

/// A class body's statements and its attrs are separate fields; nothing a
/// statement carries shows up in the attr map.
#[test]
fn an_agent_body_separates_its_statements_from_its_attrs() {
    let file = statements();
    let agent = file.agents().next().expect("the corpus declares an agent");

    assert_eq!(agent.name.value(), "PersonalAssistant");
    let params = agent.params.as_ref().expect("four parameters");
    assert_eq!(params.params.len(), 4);
    assert_eq!(params.params[0].ty.value(), "String");
    assert!(params.params[0].default.is_none());
    assert!(
        params.params[3].default.is_some(),
        "the last parameter has a default"
    );

    assert_eq!(agent.mounts.len(), 2);
    assert_eq!(agent.subs.len(), 2);
    assert_eq!(agent.acls.len(), 1);
    assert_eq!(agent.blocks.len(), 1, "start_hooks is a generic section");
    assert_eq!(
        agent
            .attrs
            .grants
            .as_ref()
            .expect("granted capabilities")
            .value
            .words
            .len(),
        2
    );
    assert!(agent.attrs.name.is_some());
}

/// The bare form references, the braced form defines.
#[test]
fn an_mcp_server_statement_is_a_reference_or_an_inline_definition() {
    let file = statements();
    let agent = file.agents().next().expect("the corpus declares an agent");

    let McpServerStmt::Ref(referenced) = &agent.mcps[0] else {
        panic!("the bare form is a reference");
    };
    assert_eq!(referenced.value(), "graf");
    let McpServerStmt::Inline(defined) = &agent.mcps[1] else {
        panic!("the braced form is a definition");
    };
    assert_eq!(defined.name.value(), "pfin");
    assert!(defined.body.attrs.args.is_some());
    assert!(defined.body.attrs.env.is_none());
}

/// A mount is a reference with an optional per-use tail, never a definition.
#[test]
fn a_mount_carries_an_optional_tail() {
    let file = statements();
    let agent = file.agents().next().expect("the corpus declares an agent");

    assert_eq!(agent.mounts[0].repo.head.value(), "ws");
    assert_eq!(
        agent.mounts[0].tail.as_ref().expect("a tail").attrs.len(),
        1
    );
    assert!(agent.mounts[1].tail.is_none());
    assert!(agent.mounts[1].semi, "the bare form ends in `;`");
}

/// A subscription takes a handle or a literal address, and `f"…"` is the
/// address form even though `f` is a legal identifier.
#[test]
fn a_subscription_takes_a_handle_or_an_address() {
    let file = statements();
    let agent = file.agents().next().expect("the corpus declares an agent");

    assert!(matches!(agent.subs[0].chan, ChanRef::Handle(_)));
    assert!(
        matches!(agent.subs[1].chan, ChanRef::Addr(_)),
        "an f-string address is not the identifier `f`"
    );
}

/// An assembly's body is the top-level vocabulary again, nested.
#[test]
fn an_assembly_stamps_the_top_level_vocabulary() {
    let file = statements();
    let assembly = file
        .assemblies()
        .next()
        .expect("the corpus declares an assembly");

    assert_eq!(assembly.params.params.len(), 2);
    let surface = assembly
        .surfaces()
        .next()
        .expect("the assembly stamps a surface");
    assert_eq!(surface.insts.len(), 2);
    assert_eq!(surface.acls.len(), 1);
}

/// Direction is the variant; the free io form connects nothing.
#[test]
fn bindings_carry_their_direction_as_the_variant() {
    let file = statements();
    let assembly = file
        .assemblies()
        .next()
        .expect("the corpus declares an assembly");
    let surface = assembly
        .surfaces()
        .next()
        .expect("the assembly stamps a surface");

    let body = surface.insts[0].body.as_ref().expect("a body");
    let Binding::Into(inbound) = &body.bindings[0] else {
        panic!("the first binding points in");
    };
    assert_eq!(inbound.port.value(), "messages");
    assert!(inbound.tail.is_none());

    let Binding::Both(free) = &body.bindings[1] else {
        panic!("the second binding is an io port");
    };
    assert!(free.target.is_none(), "a free io port connects nothing");
    assert_eq!(free.tail.as_ref().expect("a tail").attrs.len(), 2);

    let echo = surface.insts[1].body.as_ref().expect("a body");
    assert!(matches!(&echo.bindings[0], Binding::Both(io) if io.target.is_some()));
    assert!(matches!(&echo.bindings[1], Binding::Outof(_)));
}

/// Arguments and a body are alternatives in practice: one names a
/// parameterized class, the other configures an instance.
#[test]
fn an_instantiation_takes_arguments_or_a_body() {
    let file = statements();
    let instances: Vec<_> = file.instantiations().collect();
    assert_eq!(instances.len(), 3);

    let parameterized = &instances[0];
    assert_eq!(parameterized.handle.value(), "alice_pa");
    assert_eq!(parameterized.cls.head.value(), "PersonalAssistant");
    assert_eq!(
        parameterized.args.as_ref().expect("arguments").args.len(),
        3
    );
    assert!(parameterized.body.is_none());
    assert!(parameterized.semi);

    let configured = &instances[2];
    assert!(configured.args.is_none());
    let body = configured.body.as_ref().expect("a body");
    assert_eq!(body.bindings.len(), 2);
    assert!(
        body.attrs.get("slug").is_some(),
        "an instance body's attrs stay a map until the class is known"
    );
    assert!(!configured.semi, "a block-ended statement takes no `;`");
}

/// The plane is a word of the statement, the matchers are its scope.
#[test]
fn acl_and_grant_statements_name_a_plane_and_a_scope() {
    let file = statements();
    let acl = file.acls().next().expect("a top-level acl");
    assert_eq!(acl.plane.value(), "subscribe");
    assert_eq!(acl.matchers.items.len(), 1);
    assert_eq!(acl.matchers.items[0].kind.value(), "prefix");

    let grant = file.grants().next().expect("a top-level grant");
    assert_eq!(grant.principal.head.value(), "alice_pa");
    assert_eq!(grant.plane.value(), "publish");
    assert_eq!(grant.m.kind.value(), "exact");
}

/// A webhook's sub-blocks ride the generic section rule: three of them, held
/// un-walked until their kindword is read.
#[test]
fn a_webhook_holds_its_sub_blocks_as_sections() {
    let file = statements();
    let webhook = file
        .webhooks()
        .next()
        .expect("the corpus declares a webhook");
    assert_eq!(webhook.blocks.len(), 3);
    assert!(webhook.attrs.mount.is_some());
    assert!(webhook.attrs.description.is_some());
    assert!(webhook.attrs.urgency.is_none());
}

/// Class names refuse consecutive uppercase at the definition site: the
/// terminal matches only the prefix, and the declaration fails on the residue.
///
/// At top level the failure lands in the generic `section` fallback rather than
/// in a syntax error — `component HTTPProxy { … }` is a section whose kindword
/// is `component`, which the resolver refuses. Where no fallback exists, as in a
/// parameter type, the refusal is the parse error itself.
#[test]
fn a_class_name_refuses_consecutive_uppercase() {
    let refused = parse_str("component HTTPProxy {\n}\n", "t.brenn")
        .expect("the section fallback catches it");
    assert!(
        matches!(refused.items[0].value(), Item::Section(_)),
        "the declaration did not match; spell it HttpProxy"
    );

    for accepted in [
        "component HttpProxy {\n}\n",
        "component A {\n}\n",
        "component HttpA {\n}\n",
    ] {
        let file = parse_str(accepted, "t.brenn").expect("a parse");
        assert!(
            matches!(file.items[0].value(), Item::Component(_)),
            "{accepted:?} is a component declaration"
        );
    }

    parse_str("assembly Deskbar(slug: HTTPProxy) {\n}\n", "t.brenn")
        .expect_err("a parameter type has no fallback to fall into");
    parse_str("assembly Deskbar(slug: String) {\n}\n", "t.brenn").expect("the CamelCase spelling");
}

/// A path's segments admit no whitespace. That is the rule's own shape, and it
/// is what lets a statement require whitespace after a path.
#[test]
fn a_path_takes_no_whitespace_around_its_separators() {
    parse_str("const a = alice.desk;\n", "t.brenn").expect("a dotted path");
    parse_str("const a = alice . desk;\n", "t.brenn").expect_err("no spaces inside a path");
}

/// The value vocabulary is one rule, so a matcher in an `acl` list and a
/// matcher in a value position are the same shape.
#[test]
fn an_acl_matcher_may_carry_an_attribute_tail() {
    let file = parse_str(
        "acl subscribe [prefix \"brenn:alice.\" { push_depth = 4 }];\n",
        "t.brenn",
    )
    .expect("a matcher with a tail");
    let Item::Acl(acl) = file.items[0].value() else {
        panic!("an acl statement");
    };
    let tail = acl.matchers.items[0].tail.as_ref().expect("a tail");
    assert!(matches!(
        tail.entries.get("push_depth").map(|value| value.value()),
        Some(Value::Int(_))
    ));
}

/// A duplicate key in an entity body is a positioned error, not a syntax error,
/// and it cites the entry that already claimed the key.
#[test]
fn a_duplicate_key_in_an_entity_body_is_refused() {
    let error = parse_str(
        "component Alice {\n  abi = dom;\n  abi = processor;\n}\n",
        "t.brenn",
    )
    .expect_err("the second `abi` has no home");
    assert!(
        error.message.contains("duplicate"),
        "the refusal names what it is: {error}"
    );
    assert_eq!(error.line_col(), Some((3, 3)));

    let [(note, first)] = &error.related[..] else {
        panic!("one related location: the first `abi`");
    };
    assert!(note.contains("previously defined"), "{note}");
    let position = first.line_col_inner().expect("the first entry is located");
    assert_eq!((position.line + 1, position.col + 1), (2, 3));
}

/// Round trip: the canonical form of the corpus deserializes to the same model
/// the source did. Spans differ and do not take part in equality.
///
/// A declaration holding a `Raw` subtree cannot be compared whole — a held CST
/// node compares by its entire subtree, comments and layout included, and those
/// are exactly what formatting moves. Rather than drop those declarations, which
/// would aim the test away from the two richest forms in the language, each is
/// compared field by field: everything but the held nodes directly, and the held
/// nodes through the span-free typed form their kindword selects.
#[test]
fn formatting_preserves_the_statement_model() {
    fn without_held_nodes(file: &File) -> Vec<&Item> {
        file.items
            .iter()
            .map(|item| item.value())
            .filter(|item| !matches!(item, Item::Section(_) | Item::Agent(_) | Item::Webhook(_)))
            .collect()
    }

    fn agents_match(source: &AgentClass, canonical: &AgentClass) {
        assert_eq!(source.doc, canonical.doc);
        assert_eq!(source.name, canonical.name);
        assert_eq!(source.params, canonical.params);
        assert_eq!(source.attrs, canonical.attrs);
        assert_eq!(source.mounts, canonical.mounts);
        assert_eq!(source.mcps, canonical.mcps);
        assert_eq!(source.subs, canonical.subs);
        assert_eq!(source.acls, canonical.acls);
        assert_eq!(source.blocks.len(), canonical.blocks.len());
        for (source, canonical) in source.blocks.iter().zip(&canonical.blocks) {
            assert_eq!(
                agent_block(source).expect("a legal kindword"),
                agent_block(canonical).expect("a legal kindword"),
            );
        }
    }

    fn webhooks_match(source: &WebhookDef, canonical: &WebhookDef) {
        assert_eq!(source.doc, canonical.doc);
        assert_eq!(source.name, canonical.name);
        assert_eq!(source.attrs, canonical.attrs);
        assert_eq!(source.blocks.len(), canonical.blocks.len());
        for (source, canonical) in source.blocks.iter().zip(&canonical.blocks) {
            assert_eq!(
                webhook_block(source).expect("a legal kindword"),
                webhook_block(canonical).expect("a legal kindword"),
            );
        }
    }

    let source = statements();
    let canonical = corpus_file("statements.canonical.brenn");
    assert_eq!(source.items.len(), canonical.items.len());
    assert_eq!(without_held_nodes(&source), without_held_nodes(&canonical));

    assert_eq!(source.agents().count(), canonical.agents().count());
    for (source, canonical) in source.agents().zip(canonical.agents()) {
        agents_match(source, canonical);
    }
    assert_eq!(source.webhooks().count(), canonical.webhooks().count());
    for (source, canonical) in source.webhooks().zip(canonical.webhooks()) {
        webhooks_match(source, canonical);
    }
}
