//! The load pass: following `use` from a root file across a tree of modules.
//!
//! These run against real fixture trees rather than in-memory files, because
//! what is under test is the mapping from a module path to a file on disk and
//! what happens when that file is not there.

mod support;

use std::path::PathBuf;

use brenn_dsl::compile;
use brenn_dsl::diag::Diagnostic;

/// One fixture tree's root file.
fn root(tree: &str) -> PathBuf {
    support::corpus_dir()
        .join("trees")
        .join(tree)
        .join("main.brenn")
}

/// Compile a fixture tree, expecting it to fail, and return the diagnostics.
fn errors(tree: &str) -> Vec<Diagnostic> {
    match compile(&root(tree)) {
        Ok(_) => panic!("`{tree}` was expected not to compile"),
        Err(errors) => errors,
    }
}

/// The one message a failing tree is expected to produce.
fn one_error(tree: &str) -> String {
    let errors = errors(tree);
    assert_eq!(errors.len(), 1, "{:?}", messages(&errors));
    errors[0].message.clone()
}

fn messages(errors: &[Diagnostic]) -> Vec<&str> {
    errors.iter().map(|error| error.message.as_str()).collect()
}

#[test]
fn a_root_reaches_a_nested_module_and_a_flat_one() {
    let output = compile(&root("ok")).unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
    // The nested module's own declaration reached the resolved config, and the
    // constant it interpolated came from the flat module the root imported
    // whole: loading and indexing a module is not the same as emitting it.
    let addresses: Vec<&str> = output
        .resolved
        .channels
        .iter()
        .map(|channel| channel.address.value().as_str())
        .collect();
    // The module's own channel, then the one the root's instantiation stamped
    // out of the module's assembly.
    assert_eq!(
        addresses,
        ["brenn:bench.status", "brenn:alice-desk.in.p1.messages"]
    );
    assert_eq!(output.resolved.channels[0].handle.dotted(), "bench_status");
    assert_eq!(
        output.resolved.channels[1].handle.dotted(),
        "alice_desk.messages"
    );
    // The whole emitted shape, so that a change in what emission carries out of
    // a module is visible here and not only for channels.
    let repos: Vec<String> = output
        .resolved
        .repos
        .iter()
        .map(|repo| repo.handle.dotted())
        .collect();
    assert_eq!(repos, ["notes"]);
    let config = &output.resolved;
    assert!(config.tunings.is_empty());
    assert!(config.uuid_pins.is_empty());
    assert!(config.surfaces.is_empty());
    assert!(config.consumers.is_empty());
    assert!(config.agents.is_empty());
    assert!(config.remotes.is_empty());
    assert!(config.webhooks.is_empty());
    assert!(config.mqtt_clients.is_empty());
    assert!(config.mcp_servers.is_empty());
    assert!(config.grants.is_empty());
    assert_eq!(config.channels.len(), 2);
}

#[test]
fn the_root_file_cannot_be_loaded_a_second_time_as_a_named_module() {
    assert_eq!(
        one_error("reimport"),
        "`main` is already loaded as <root>: one file is one module"
    );
}

#[test]
fn a_missing_module_names_the_file_it_looked_for() {
    assert_eq!(
        one_error("missing"),
        "no module `wiring::deskbar`: expected `wiring/deskbar.brenn`"
    );
}

#[test]
fn an_import_cycle_names_its_members() {
    assert_eq!(
        one_error("cycle"),
        "import cycle: alpha -> beta -> alpha",
        "the chain starts where the cycle does, not at the root"
    );
}

#[test]
fn two_globs_bringing_in_one_name_collide_at_the_second() {
    let errors = errors("collide");
    assert_eq!(
        messages(&errors),
        ["importing `skin` collides with another import"]
    );
    assert_eq!(errors[0].related.len(), 1, "the first import is cited too");
}

#[test]
fn a_missing_module_is_positioned_at_the_use_that_named_it() {
    let errors = errors("missing");
    assert_eq!(errors[0].line_col(), Some((1, 5)));
    assert!(errors[0].file.ends_with("main.brenn"), "{}", errors[0].file);
}

// ── stamped entities across a `use` boundary ─────────────────────────────────
//
// These go through the I/O-free core, because what is under test is the
// cross-file keying, not the loader.

/// The class definitions both arrangements share.
const CLASSES: &str = "\
agent Assistant(name: String) {
    slug = name;
}

assembly Pod(slug: String) {
    channel messages at f\"brenn:{slug}.in.messages\";
    new pa: Assistant(name = slug);
}

assembly Watch(peer: Agent, feed: Channel) {
    surface board { grants = [subscribe]; }
    grant peer subscribe exact feed;
}
";

const PRODUCER: &str = "new alice: Pod(slug = \"alice\");\n";
const CONSUMER: &str = "new watch: Watch(peer = alice.pa, feed = alice.messages);\n";

#[test]
fn an_instantiation_reaches_an_imported_module_s_stamped_entities() {
    // The producer is written in the module, the consumer in the root: the
    // stamp is recorded under the module and reached through the name the root
    // imported.
    let config = support::resolved_tree(&[
        ("", &format!("use wiring::*;\n{CONSUMER}")),
        ("wiring", &format!("{CLASSES}{PRODUCER}")),
    ]);
    assert_eq!(
        config
            .grants
            .iter()
            .map(|grant| grant.principal.dotted())
            .collect::<Vec<_>>(),
        ["alice.pa"]
    );
    assert_eq!(
        config
            .agents
            .iter()
            .map(|agent| agent.handle.dotted())
            .collect::<Vec<_>>(),
        ["alice.pa"]
    );
}

#[test]
fn an_instantiation_in_one_module_waits_for_one_in_another() {
    // The consumer's module imports the producer's, so the deferral and the
    // stamp lookup both cross a `use` boundary.
    let config = support::resolved_tree(&[
        ("", "use wiring::*;\nuse pods::*;\n"),
        ("wiring", &format!("use pods::*;\n{CLASSES}{CONSUMER}")),
        ("pods", &format!("use wiring::Pod;\n{PRODUCER}")),
    ]);
    assert_eq!(
        config
            .grants
            .iter()
            .map(|grant| grant.principal.dotted())
            .collect::<Vec<_>>(),
        ["alice.pa"]
    );
}
