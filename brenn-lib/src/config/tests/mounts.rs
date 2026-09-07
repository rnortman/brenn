//! The mounts document: what it declares, what the host refuses, and what the
//! three root lists derive to.
//!
//! Every fixture builds a real tree under a tempdir, because everything this
//! module does past the compile is a filesystem question — a `VERSION` file, a
//! tree that is present or absent, a symlink two declarations point through.

use std::path::{Path, PathBuf};

use crate::config::{MountStatus, MountTree, compile_mounts, load_mounts, try_load_mounts};

/// A mount directory holding `VERSION` and the named trees.
fn install(root: &Path, name: &str, version: &str, trees: &[MountTree]) -> PathBuf {
    let path = root.join(name);
    std::fs::create_dir_all(&path).unwrap();
    std::fs::write(path.join("VERSION"), format!("{version}\n")).unwrap();
    for tree in trees {
        std::fs::create_dir_all(path.join(tree.dir_name())).unwrap();
    }
    path
}

/// A mounts document written into `dir`, named `mounts.brenn`.
fn document(dir: &Path, body: &str) -> PathBuf {
    let path = dir.join("mounts.brenn");
    std::fs::write(&path, body).unwrap();
    path
}

/// One `mount name { path = "…"; }` line.
fn line(name: &str, path: &Path) -> String {
    format!("mount {name} {{ path = \"{}\"; }}\n", path.display())
}

// ── the happy path ───────────────────────────────────────────────────────────

/// The whole of what boot asks: the declarations resolve, each is verified, and
/// the three root lists hold `<mount>/<tree>` for the trees each mount offers,
/// in declaration order.
#[test]
fn the_roots_derive_from_the_trees_each_mount_offers() {
    let dir = tempfile::tempdir().unwrap();
    let release = install(
        dir.path(),
        "release",
        "0.20.0",
        &[
            MountTree::Components,
            MountTree::Surface,
            MountTree::Modules,
        ],
    );
    let bundle = install(
        dir.path(),
        "bundle",
        "1.4.2",
        &[MountTree::Components, MountTree::Modules],
    );
    let file = document(
        dir.path(),
        &format!("{}{}", line("brenn", &release), line("demo", &bundle)),
    );

    let loaded = try_load_mounts(Some(&file)).expect("both mounts are installed");

    let names: Vec<&str> = loaded
        .config
        .mounts
        .iter()
        .map(|mount| mount.name.as_str())
        .collect();
    assert_eq!(names, ["brenn", "demo"]);

    let versions: Vec<&str> = loaded
        .config
        .mounts
        .iter()
        .map(|mount| mount.version.as_str())
        .collect();
    assert_eq!(versions, ["0.20.0", "1.4.2"], "the VERSION line, trimmed");

    assert_eq!(
        loaded.config.mounts[1].trees,
        [MountTree::Components, MountTree::Modules],
        "a mount offers only the trees it ships"
    );

    let roots = &loaded.roots;
    assert_eq!(
        *roots.components_roots,
        [release.join("components"), bundle.join("components")]
    );
    assert_eq!(
        *roots.module_roots,
        [release.join("modules"), bundle.join("modules")]
    );
    assert_eq!(
        *roots.surface_roots,
        [release.join("surface")],
        "the bundle ships no surface tree, so it contributes no surface root"
    );
    assert_eq!(
        roots
            .components_roots
            .source()
            .locate(&release.join("components")),
        format!("mount `brenn` ({})", release.join("components").display()),
        "a root list carries which mount offered each root, for the refusals"
    );

    assert_eq!(
        loaded.path.as_deref(),
        Some(file.as_path()),
        "what was loaded knows where it came from, so a re-read cannot be aimed elsewhere"
    );
}

/// No `--mounts` is zero mounts, not a default document: a dev server with no
/// components, no surfaces and no packaged vocabulary.
#[test]
fn no_mounts_file_is_no_mounts() {
    let loaded = load_mounts(None);
    assert!(loaded.config.mounts.is_empty());
    assert_eq!(loaded.roots, Default::default());
}

/// The empty roots are still *the mounts'* roots. A host that derived them from
/// `RootList`'s own default would carry the workstation flag as their source,
/// and a document with a packaged import would then be refused with "pass
/// `--modules <dir>`" — a flag `serve` rejects.
#[test]
fn a_host_with_no_mounts_is_refused_in_the_mounts_words() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("site.brenn");
    std::fs::write(
        &root,
        "use @sifter::*;
",
    )
    .unwrap();

    let roots = load_mounts(None).roots;
    let errors = brenn_dsl::compile(&brenn_dsl::DocumentInputs {
        root,
        module_roots: roots.module_roots,
        role: brenn_dsl::DocumentRole::Deployment,
    })
    .expect_err("a packaged import with no root does not compile");
    assert!(
        errors.iter().any(|error| error.message
            == "this document imports packaged modules, but no declared mount offers a \
                `modules/` tree"),
        "{:?}",
        errors.iter().map(|e| &e.message).collect::<Vec<_>>(),
    );
}

/// The mount path is canonicalized on read, because a bundle's is a symlink to
/// a versioned sibling that the installer swaps under a running host.
#[test]
fn a_mount_path_is_canonicalized() {
    let dir = tempfile::tempdir().unwrap();
    let versioned = install(dir.path(), "demo.v1.4.2", "1.4.2", &[MountTree::Modules]);
    let link = dir.path().join("demo");
    std::os::unix::fs::symlink(&versioned, &link).unwrap();
    let file = document(dir.path(), &line("demo", &link));

    let loaded = try_load_mounts(Some(&file)).expect("the symlink resolves to the tree");
    let mount = &loaded.config.mounts[0];
    assert_eq!(
        mount.path,
        versioned.canonicalize().unwrap(),
        "every scan reads the tree the symlink points at"
    );
    assert_eq!(*loaded.roots.module_roots, [mount.path.join("modules")]);
}

// ── document-level refusals ──────────────────────────────────────────────────

/// A relative path: the host's working directory is not the operator's, so
/// there is no honest way to read one.
#[test]
fn a_relative_path_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let file = document(dir.path(), "mount demo { path = \"bundles/demo\"; }\n");

    let report = compile_mounts(&file).expect_err("a relative path is refused");
    assert!(
        report.contains("not an absolute path"),
        "the refusal says what is wrong: {report}"
    );
    assert!(report.contains("demo"), "and which mount: {report}");
}

/// A tab in a path. The `brenn mounts` listing is one line per mount with three
/// tab-separated fields, so a path holding a tab or a newline is a line an
/// installer parses as two mounts or as extra fields.
#[test]
fn a_control_character_in_a_path_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let file = document(dir.path(), "mount demo { path = \"/srv/a\tb\"; }\n");

    let report = compile_mounts(&file).expect_err("a tab in a path is refused");
    assert!(
        report.contains("control character"),
        "the refusal says what is wrong: {report}"
    );
    assert!(report.contains("demo"), "and which mount: {report}");
}

/// Two declarations of one directory, and one declaration inside another.
/// Both are checked before anything is read, so an installer sees them against
/// a mount that does not exist yet.
#[test]
fn nested_and_duplicate_declared_paths_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let outer = dir.path().join("brenn");
    let inner = outer.join("bundles/demo");

    let file = document(
        dir.path(),
        &format!("{}{}", line("brenn", &outer), line("demo", &inner)),
    );
    let report = compile_mounts(&file).expect_err("a nested mount is refused");
    assert!(
        report.contains("is inside mount"),
        "the refusal names the arrangement: {report}"
    );

    let file = document(
        dir.path(),
        &format!("{}{}", line("brenn", &outer), line("also", &outer)),
    );
    let report = compile_mounts(&file).expect_err("one directory is one mount");
    assert!(
        report.contains("the same declared path"),
        "the refusal names the collision: {report}"
    );
}

/// A mounts document is a `.brenn` document, and the extension dispatch says so
/// rather than handing the text to a parser that will not understand it.
#[test]
fn a_mounts_file_is_a_brenn_document() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("mounts.toml");
    std::fs::write(&file, "").unwrap();
    let report = compile_mounts(&file).expect_err("only `.brenn` is a mounts document");
    assert!(report.contains("unrecognized extension"), "{report}");
}

// ── on-disk refusals ─────────────────────────────────────────────────────────

/// A declared mount whose path does not exist. The window between adding the
/// line and running the deploy is the operator's own, and a reload attempted
/// inside it is refused naming the mount rather than silently running without
/// it.
#[test]
fn a_missing_mount_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let file = document(dir.path(), &line("demo", &dir.path().join("nothing-here")));

    let report = try_load_mounts(Some(&file)).expect_err("a declared mount must be installed");
    assert!(report.contains("demo"), "{report}");
    assert!(report.contains("cannot be resolved"), "{report}");
}

/// No `VERSION`, or an empty one: the install is incomplete, or the path is not
/// a mount at all.
#[test]
fn a_mount_without_a_version_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("demo");
    std::fs::create_dir_all(path.join("modules")).unwrap();
    let file = document(dir.path(), &line("demo", &path));

    let report = try_load_mounts(Some(&file)).expect_err("a mount says which revision it is");
    assert!(report.contains("no readable VERSION"), "{report}");

    std::fs::write(path.join("VERSION"), "\n").unwrap();
    let report = try_load_mounts(Some(&file)).expect_err("an empty VERSION says nothing");
    assert!(report.contains("is empty"), "{report}");
}

/// A directory holding none of the three trees is a flag pointed one directory
/// off — the case the rule exists for.
#[test]
fn a_mount_offering_no_tree_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = install(dir.path(), "demo", "1.0.0", &[]);
    let file = document(dir.path(), &line("demo", &path));

    let report = try_load_mounts(Some(&file)).expect_err("a mount offers the host something");
    assert!(report.contains("holds none of"), "{report}");
}

/// Two declarations that canonicalize to one tree. The declared paths are
/// distinct, so only the second pass can see it.
#[test]
fn two_mounts_of_one_tree_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let real = install(dir.path(), "demo.v1", "1.0.0", &[MountTree::Modules]);
    let link = dir.path().join("demo");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    let file = document(
        dir.path(),
        &format!("{}{}", line("demo", &real), line("demo-alias", &link)),
    );

    let report = try_load_mounts(Some(&file)).expect_err("one tree is one mount");
    assert!(report.contains("the same canonical path"), "{report}");
}

/// Every fault is reported, not just the first: an operator who mistyped two
/// paths learns both from one reload.
#[test]
fn every_on_disk_fault_is_reported() {
    let dir = tempfile::tempdir().unwrap();
    let file = document(
        dir.path(),
        &format!(
            "{}{}",
            line("one", &dir.path().join("absent-one")),
            line("two", &dir.path().join("absent-two")),
        ),
    );

    let report = try_load_mounts(Some(&file)).expect_err("neither mount is installed");
    assert!(report.contains("one"), "{report}");
    assert!(report.contains("two"), "{report}");
}

// ── the listing ──────────────────────────────────────────────────────────────

/// What the `mounts` subcommand and the installer read: a per-mount status,
/// with `missing` a legal answer. The document itself is valid, so nothing
/// here refuses — the installer is about to create the missing tree.
#[test]
fn a_listing_reports_each_mount_separately() {
    let dir = tempfile::tempdir().unwrap();
    let good = install(dir.path(), "brenn", "0.20.0", &[MountTree::Surface]);
    let broken = dir.path().join("broken");
    std::fs::create_dir_all(broken.join("modules")).unwrap();
    let absent = dir.path().join("not-yet");

    let file = document(
        dir.path(),
        &format!(
            "{}{}{}",
            line("brenn", &good),
            line("broken", &broken),
            line("not-yet", &absent),
        ),
    );

    let compiled = compile_mounts(&file).expect("the document itself is valid");
    let statuses = compiled.statuses();
    let names: Vec<&str> = statuses
        .iter()
        .map(|(decl, _)| decl.name.as_str())
        .collect();
    assert_eq!(names, ["brenn", "broken", "not-yet"]);

    assert_eq!(
        statuses[0].1,
        MountStatus::Ok {
            version: "0.20.0".to_string(),
            trees: vec![MountTree::Surface],
        }
    );
    let MountStatus::Fault(message) = &statuses[1].1 else {
        panic!("a tree with no VERSION is a fault, not a status");
    };
    assert!(message.contains("VERSION"), "{message}");
    assert_eq!(statuses[2].1, MountStatus::Missing);
}
