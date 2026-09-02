//! The load pass: following `use` from a root file across a tree of modules.
//!
//! These run against real fixture trees rather than in-memory files, because
//! what is under test is the mapping from a module path to a file on disk and
//! what happens when that file is not there.

mod support;

use std::path::PathBuf;

use brenn_dsl::diag::Diagnostic;
use brenn_dsl::{DocumentInputs, compile};

/// One fixture tree's root file.
fn root(tree: &str) -> PathBuf {
    support::corpus_dir()
        .join("trees")
        .join(tree)
        .join("main.brenn")
}

/// A fixture tree's packaged-module root: the `modules/` directory beside its
/// root file.
///
/// It sits inside the tree only because a fixture has to keep its files
/// somewhere. Nothing about resolution reads it as part of the tree — that is
/// what `pkg-two-keys` is for.
fn modules(tree: &str) -> PathBuf {
    support::corpus_dir()
        .join("trees")
        .join(tree)
        .join("modules")
}

/// A fixture tree's root file with the module roots to compile it against.
fn inputs(tree: &str, module_roots: &[PathBuf]) -> DocumentInputs {
    DocumentInputs {
        root: root(tree),
        module_roots: module_roots.to_vec(),
    }
}

/// Compile a fixture tree, expecting it to fail, and return the diagnostics.
fn errors(tree: &str) -> Vec<Diagnostic> {
    errors_with(tree, &[])
}

/// Compile a fixture tree against module roots, expecting it to fail.
fn errors_with(tree: &str, module_roots: &[PathBuf]) -> Vec<Diagnostic> {
    match compile(&inputs(tree, module_roots)) {
        Ok(_) => panic!("`{tree}` was expected not to compile"),
        Err(errors) => errors,
    }
}

/// The one message a failing tree is expected to produce.
fn one_error(tree: &str) -> String {
    one_error_with(tree, &[])
}

/// The one message a failing tree compiled against module roots produces.
fn one_error_with(tree: &str, module_roots: &[PathBuf]) -> String {
    let errors = errors_with(tree, module_roots);
    assert_eq!(errors.len(), 1, "{:?}", messages(&errors));
    errors[0].message.clone()
}

fn messages(errors: &[Diagnostic]) -> Vec<&str> {
    errors.iter().map(|error| error.message.as_str()).collect()
}

#[test]
fn a_root_reaches_a_nested_module_and_a_flat_one() {
    let output =
        compile(&inputs("ok", &[])).unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
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

// ── packaged-module imports ──────────────────────────────────────────────────
//
// `use @<name>::…` resolves against a module root the caller supplies rather
// than against the root file's directory, so these need real trees for the same
// reason the ones above do: what is under test is which directory a name reads
// from.

#[test]
fn a_root_reaches_two_packaged_modules_and_one_reaches_the_other() {
    let output = compile(&inputs("pkg-ok", &[modules("pkg-ok")]))
        .unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
    // The glob brought in the module's assembly, which the root stamped, and
    // the module's own `@` import resolved against the same root.
    let addresses: Vec<&str> = output
        .resolved
        .channels
        .iter()
        .map(|channel| channel.address.value().as_str())
        .collect();
    assert_eq!(addresses, ["brenn:alice-desk.in.p1.messages"]);
    assert_eq!(
        output.resolved.channels[0].handle.dotted(),
        "alice_desk.messages"
    );
    // A packaged module declares and stamps nothing itself: the only entity in
    // the config is the one the root's `new` asked for.
    assert!(output.resolved.consumers.is_empty());
}

#[test]
fn a_missing_packaged_module_names_the_file_it_looked_for() {
    let expected = modules("pkg-missing").join("absent.brenn");
    assert_eq!(
        one_error_with("pkg-missing", &[modules("pkg-missing")]),
        format!(
            "no packaged module `absent`: expected `{}`",
            expected.display()
        ),
    );
}

#[test]
fn a_packaged_import_with_no_module_root_names_the_flag() {
    // The fixture has two packaged imports; one diagnostic is correct because
    // the missing root is per-invocation, not per-import.
    assert_eq!(
        one_error("pkg-no-root"),
        "this document imports packaged modules; pass `--modules <dir>`",
    );
}

#[test]
fn a_module_root_that_is_not_a_directory_is_refused_whatever_the_document_imports() {
    // The document here has no packaged import at all: an operator typo must
    // not pass silently just because nothing happened to reach for it.
    let typo = support::corpus_dir().join("trees").join("no-such-modules");
    let errors = errors_with("ok", std::slice::from_ref(&typo));
    assert_eq!(errors.len(), 1, "{:?}", messages(&errors));
    let prefix = format!("--modules {}: not a readable directory: ", typo.display());
    assert!(
        errors[0].message.starts_with(&prefix),
        "{}",
        errors[0].message
    );
}

/// A module root that exists and cannot be listed is the shape a wrongly
/// permissioned `MODULES_DIR` takes on a host. Refusing it here is what keeps
/// its imports from being reported as absent files.
#[cfg(unix)]
#[test]
fn a_module_root_that_cannot_be_listed_is_refused_as_the_root_and_not_as_absent_modules() {
    use std::os::unix::fs::PermissionsExt;

    let scratch = std::env::var("TEST_TMPDIR").map_or_else(|_| std::env::temp_dir(), PathBuf::from);
    let modules = scratch.join(format!("unlistable-module-root-{}", std::process::id()));
    std::fs::create_dir(&modules).unwrap();
    std::fs::write(modules.join("widget.brenn"), "const skin = \"bench\";\n").unwrap();
    std::fs::set_permissions(&modules, std::fs::Permissions::from_mode(0o000)).unwrap();
    // Root reads a mode-000 directory anyway, and then there is nothing to
    // observe; the assertion is worth nothing rather than wrong.
    if std::fs::read_dir(&modules).is_ok() {
        return;
    }
    let errors = errors_with("ok", std::slice::from_ref(&modules));
    let prefix = format!(
        "--modules {}: not a readable directory: ",
        modules.display()
    );
    assert!(
        errors[0].message.starts_with(&prefix),
        "{}",
        errors[0].message
    );
    std::fs::set_permissions(&modules, std::fs::Permissions::from_mode(0o700)).unwrap();
}

#[test]
fn a_packaged_import_is_one_level_deep() {
    assert_eq!(
        one_error_with("pkg-deep", &[modules("pkg-deep")]),
        "a packaged module is one level: `use @<module>::<Item>;`",
    );
}

#[test]
fn one_file_reached_as_a_tree_module_and_a_packaged_one_is_still_one_module() {
    assert_eq!(
        one_error_with("pkg-two-keys", &[modules("pkg-two-keys")]),
        "`@widget` is already loaded as modules::widget: one file is one module",
    );
}

#[test]
fn a_cycle_between_packaged_modules_names_its_members() {
    assert_eq!(
        one_error_with("pkg-cycle", &[modules("pkg-cycle")]),
        "import cycle: @alpha -> @beta -> @alpha",
    );
}

#[test]
fn a_packaged_module_that_imports_the_tree_and_stamps_is_refused_for_each() {
    // The tree import is refused rather than followed: the module it names does
    // not exist in this tree, so a loader that walked it would stack a fourth
    // diagnostic — an absence — in front of the refusal that is the real
    // answer. Exactly three messages is what proves it did not.
    let errors = errors_with("pkg-discipline", &[modules("pkg-discipline")]);
    assert_eq!(
        messages(&errors),
        [
            "a packaged module imports only packaged modules: `use @<module>::<Item>;`",
            "a packaged module declares vocabulary — component classes, assemblies, constants \
             — and instantiates nothing",
            "a packaged module declares vocabulary — component classes, assemblies, constants \
             — and instantiates nothing",
        ],
    );
    for error in &errors {
        assert!(
            error.file.ends_with("widget.brenn"),
            "the refusal names the packaged module: {}",
            error.file
        );
    }
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

// ── more than one module root ────────────────────────────────────────────────
//
// Each installed release has a module root of its own, so a host names several.
// A module is under exactly one of them: the roots are declared inputs, and the
// same name under two is a broken install whether or not the document imports
// it.

/// A fixture tree's second-release module root: a subdirectory beside its root
/// file, named for the release.
fn release_root(tree: &str, release: &str) -> PathBuf {
    support::corpus_dir().join("trees").join(tree).join(release)
}

#[test]
fn a_packaged_import_resolves_in_whichever_root_holds_it() {
    let output = compile(&inputs(
        "pkg-two-roots",
        &[
            release_root("pkg-two-roots", "a"),
            release_root("pkg-two-roots", "b"),
        ],
    ))
    .unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
    // `widget` came from `a/` and the `base` it imports from `b/`: a packaged
    // module's own `@` import is resolved against the whole list, not against
    // the root it was found under.
    assert_eq!(
        output.resolved.channels[0].handle.dotted(),
        "alice_desk.messages"
    );
}

#[test]
fn a_module_under_two_roots_is_refused_naming_both() {
    let a = release_root("pkg-dup", "a");
    let b = release_root("pkg-dup", "b");
    assert_eq!(
        one_error_with("pkg-dup", &[a.clone(), b.clone()]),
        format!(
            "packaged module `widget` is installed under more than one --modules root: {}, {}. \
             It ships with exactly one release; two copies mean a stale install or two bundles \
             claiming one name. Remove or rename one",
            a.display(),
            b.display()
        ),
    );
}

#[test]
fn a_duplicate_nothing_imports_is_still_refused() {
    // The `ok` tree imports no packaged module at all. The roots are declared
    // inputs, and a broken install is refused independently of what today's
    // document happens to touch.
    let errors = errors_with(
        "ok",
        &[release_root("pkg-dup", "a"), release_root("pkg-dup", "b")],
    );
    assert_eq!(errors.len(), 1, "{:?}", messages(&errors));
    assert!(
        errors[0].message.starts_with(
            "packaged module `widget` is installed under more than one --modules root"
        ),
        "{}",
        errors[0].message
    );
}

#[test]
fn the_same_root_named_twice_is_refused_as_one_directory() {
    // `a` and `a/` are the same directory, and canonicalization is what says
    // so; the refusal names the directory rather than every module in it.
    let modules = modules("pkg-ok");
    let mut trailing = modules.clone().into_os_string();
    trailing.push("/");
    let errors = errors_with(
        "pkg-ok",
        &[modules.clone(), PathBuf::from(trailing.clone())],
    );
    assert_eq!(errors.len(), 1, "{:?}", messages(&errors));
    assert_eq!(
        errors[0].message,
        format!(
            "--modules {} and --modules {} name the same directory: every --modules root is a \
             distinct release's",
            modules.display(),
            PathBuf::from(trailing).display()
        ),
    );
}

#[test]
fn a_missing_packaged_module_names_every_root_it_was_looked_for_under() {
    let first = modules("pkg-missing");
    let second = release_root("pkg-two-roots", "b");
    assert_eq!(
        one_error_with("pkg-missing", &[first.clone(), second.clone()]),
        format!(
            "no packaged module `absent`: expected `{}` or `{}`",
            first.join("absent.brenn").display(),
            second.join("absent.brenn").display()
        ),
    );
}

#[test]
fn a_directory_named_like_a_module_is_not_a_duplicate_of_the_module() {
    // Import resolution requires a file; a stray `widget.brenn/` directory
    // under a second root is not a duplicate and cannot shadow the real module.
    let scratch = std::env::var("TEST_TMPDIR").map_or_else(|_| std::env::temp_dir(), PathBuf::from);
    let decoy = scratch.join(format!("decoy-module-root-{}", std::process::id()));
    std::fs::create_dir_all(decoy.join("widget.brenn")).unwrap();
    compile(&inputs("pkg-ok", &[modules("pkg-ok"), decoy.clone()]))
        .unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
    // Ordering must not matter: the module resolves even when the decoy
    // directory precedes the real root in the list.
    compile(&inputs("pkg-ok", &[decoy, modules("pkg-ok")]))
        .unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
}

#[test]
fn an_unreadable_root_is_refused_beside_readable_ones() {
    // One bad entry in the list is reported as itself; the good root is not
    // blamed for it.
    let typo = support::corpus_dir().join("trees").join("no-such-modules");
    let errors = errors_with("pkg-ok", &[modules("pkg-ok"), typo.clone()]);
    assert_eq!(errors.len(), 1, "{:?}", messages(&errors));
    let prefix = format!("--modules {}: not a readable directory: ", typo.display());
    assert!(
        errors[0].message.starts_with(&prefix),
        "{}",
        errors[0].message
    );
}
