use super::*;

// -----------------------------------------------------------------------
// load_config()
// -----------------------------------------------------------------------

#[test]
fn load_config_explicit_path() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("main.brenn");
    std::fs::write(&path, DOCUMENT).unwrap();
    let config = load_config_from(Some(&path), &[], dir.path()).config;
    assert_eq!(
        config.server.bind_address,
        "127.0.0.1:4000".parse().unwrap()
    );
    assert!(!config.server.secure_cookies);
}

// A named-but-absent config is its own operator error, so the panic names the
// path it could not read and the reason it could not — not a bare "compile
// failed" that a syntax error would produce just as well.
#[test]
#[should_panic(
    expected = "failed to compile config file /nonexistent/brenn.brenn:\n/nonexistent/brenn.brenn: No such file or directory"
)]
fn load_config_explicit_path_missing_panics() {
    load_config_from(
        Some(Path::new("/nonexistent/brenn.brenn")),
        &[],
        Path::new("/tmp"),
    );
}

#[test]
fn load_config_no_path_no_file_returns_defaults() {
    // Empty temp directory — no config file present.
    let dir = tempfile::tempdir().unwrap();
    let config = load_config_from(None, &[], dir.path()).config;
    // Should get production defaults.
    assert!(config.server.secure_cookies);
    assert_eq!(
        config.server.bind_address,
        SocketAddr::from(([0, 0, 0, 0], 3000))
    );
}

// -----------------------------------------------------------------------
// Extension dispatch: `.brenn`, and nothing else
// -----------------------------------------------------------------------

const DOCUMENT: &str = r#"
server {
    public_url = "https://brenn.example.com";
    bind_address = "127.0.0.1:4000";
    secure_cookies = false;
}

channel alerts at "brenn:alice-alerts" {
    push_depth = 8;
    retain_depth = 128;
    standing_retain_depth = 16;
}
"#;

#[test]
#[should_panic(expected = "failed to compile config file")]
fn load_config_brenn_path_that_does_not_compile_panics() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("main.brenn");
    std::fs::write(&path, "server { bind_address = ").unwrap();
    load_config_from(Some(&path), &[], dir.path());
}

#[test]
#[should_panic(expected = "failed to lower config file")]
fn load_config_brenn_path_that_does_not_lower_panics() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("main.brenn");
    // Compiles — `secure_cookies` takes any value at the front end — and is
    // refused when lowering asks for a boolean.
    std::fs::write(
        &path,
        r#"server { public_url = "https://brenn.example.com"; secure_cookies = 3; }"#,
    )
    .unwrap();
    load_config_from(Some(&path), &[], dir.path());
}

/// The boot panic shows every refusal, not just the first.
#[test]
fn load_config_brenn_lower_panic_reports_every_refusal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("main.brenn");
    // Two independent value-typing refusals in one section.
    std::fs::write(&path, "server { public_url = 3; secure_cookies = 3; }").unwrap();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        load_config_from(Some(&path), &[], dir.path())
    }))
    .expect_err("the document must not lower");
    let message = panic
        .downcast_ref::<String>()
        .expect("the panic message")
        .clone();
    assert!(message.contains("public_url"), "{message}");
    assert!(message.contains("secure_cookies"), "{message}");
}

#[test]
#[should_panic(expected = "unrecognized extension")]
fn load_config_unrecognized_extension_panics() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("brenn.yaml");
    std::fs::write(&path, "server: {}\n").unwrap();
    load_config_from(Some(&path), &[], dir.path());
}

#[test]
#[should_panic(expected = "unrecognized extension")]
fn load_config_toml_path_panics() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("brenn.toml");
    std::fs::write(
        &path,
        "[server]\npublic_url = \"https://brenn.example.com\"\n",
    )
    .unwrap();
    load_config_from(Some(&path), &[], dir.path());
}

#[test]
#[should_panic(expected = "unrecognized extension")]
fn load_config_extensionless_path_panics() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("brennconfig");
    std::fs::write(&path, "[server]\n").unwrap();
    load_config_from(Some(&path), &[], dir.path());
}

// -----------------------------------------------------------------------
// The no-`--config` fallback probe
// -----------------------------------------------------------------------

#[test]
fn load_config_finds_brenn_brenn_in_fallback_dir() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("brenn.brenn"), DOCUMENT).unwrap();
    let config = load_config_from(None, &[], dir.path()).config;
    assert_eq!(
        config.server.bind_address,
        "127.0.0.1:4000".parse().unwrap()
    );
}

/// A `.brenn` fallback gets the full pipeline's diagnostics, not a parse error
/// about an unexpected character: the probe hands the file to the same
/// `check_config` the explicit path uses.
#[test]
#[should_panic(expected = "failed to lower config file")]
fn load_config_invalid_brenn_brenn_in_fallback_dir_panics() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("brenn.brenn"),
        "channel alerts at \"brenn:alice-alerts\" {\n  push_depth = 8;\n  retain_depth = 128;\n  \
         standing_retain_depth = 16;\n  noise = deafening;\n}\n",
    )
    .unwrap();
    load_config_from(None, &[], dir.path());
}

/// A name that can neither be confirmed present nor confirmed absent is not
/// read as absent: a symlink loop named `brenn.brenn` would otherwise boot on
/// defaults, with no word said about the config that is sitting right there.
#[test]
#[should_panic(expected = "cannot be determined")]
fn load_config_unstattable_fallback_name_panics() {
    let dir = tempfile::tempdir().unwrap();
    let loop_path = dir.path().join("brenn.brenn");
    std::os::unix::fs::symlink(&loop_path, &loop_path).unwrap();
    load_config_from(None, &[], dir.path());
}

// -----------------------------------------------------------------------
// Document identity
// -----------------------------------------------------------------------

/// A load reports which text it loaded, and the hash is over that report.
#[test]
fn load_config_reports_the_document_it_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("main.brenn");
    std::fs::write(&path, DOCUMENT).unwrap();
    let document = load_config_from(Some(&path), &[], dir.path());
    // One file, named by its place inside the document rather than by the
    // temporary directory it happens to sit in.
    assert_eq!(document.file_places(), "main.brenn");
    assert_eq!(
        document.document_sha256,
        brenn_dsl::document_sha256(&document.files)
    );
    assert_eq!(
        document.files[0].source_sha256,
        brenn_dsl::source_sha256(DOCUMENT)
    );
}

/// A boot with no document at all still answers what it is projecting.
#[test]
fn load_config_defaults_carry_an_empty_document() {
    let dir = tempfile::tempdir().unwrap();
    let document = load_config_from(None, &[], dir.path());
    assert!(document.files.is_empty());
    assert_eq!(document.document_sha256, brenn_dsl::document_sha256(&[]));
}

/// A document of more than one file reports every file, and its identity is
/// over all of them.
///
/// The single-file case above cannot tell a hash over the whole tree from a
/// hash over the root alone, and the whole tree is what the check tool's
/// `document_sha256=` line and the boot log claim to name.
#[test]
fn load_config_reports_every_file_of_a_multi_file_document() {
    let dir = tempfile::tempdir().unwrap();
    let inputs = stage_fixture(dir.path(), "main.brenn", FENCED_DOCUMENT);
    let document = load_config_from(Some(&inputs.root), &inputs.module_roots, dir.path());
    assert_eq!(
        document.file_places(),
        format!("main.brenn @{PACKAGED_MODULE}.brenn")
    );
    assert_eq!(
        document.document_sha256,
        brenn_dsl::document_sha256(&document.files)
    );
    // The packaged module is part of the identity, so a hash of the root alone
    // is a different hash.
    assert_ne!(
        document.document_sha256,
        brenn_dsl::document_sha256(&document.files[..1])
    );
}

/// The two-file shape of [`DOCUMENT`]: the same server block, with a module of
/// its own to reach.
const FENCED_DOCUMENT: &str = concat!(
    r#"
server {
    public_url = "https://brenn.example.com";
    bind_address = "127.0.0.1:4000";
    secure_cookies = false;
}
"#,
    brenn_dsl::packaged_fence!(),
    "const skin = \"bench\";\n",
    brenn_dsl::packaged_fence!(),
);

// -----------------------------------------------------------------------
// What the document says it was read from
// -----------------------------------------------------------------------

/// The inputs a load reports are the ones it actually read, so that whatever
/// re-reads the document later — the reload facility — reads the same tree, and
/// whatever reports where the process's document lives names the same root.
#[test]
fn an_explicit_load_reports_the_root_and_module_roots_it_read() {
    let dir = tempfile::tempdir().unwrap();
    let inputs = stage_fixture(dir.path(), "main.brenn", FENCED_DOCUMENT);
    let document = load_config_from(Some(&inputs.root), &inputs.module_roots, dir.path());
    let reported = document
        .inputs
        .expect("a load from a tree reports its tree");
    assert_eq!(reported.root, inputs.root);
    assert_eq!(reported.module_roots, inputs.module_roots);
}

/// The `--config`-less boot that finds its document by probing: the root it
/// reports is the file it read, not the flag it did not get. An operator
/// reading the reload status during an incident is deciding whether the process
/// is projecting the file they edited, and `null` there answers nothing.
#[test]
fn a_fallback_load_reports_the_file_it_probed_for() {
    let dir = tempfile::tempdir().unwrap();
    let probed = dir.path().join("brenn.brenn");
    std::fs::write(&probed, DOCUMENT).unwrap();
    let document = load_config_from(None, &[], dir.path());
    let reported = document
        .inputs
        .expect("a load that found a document reports where");
    assert_eq!(reported.root, probed);
    assert_eq!(
        document.config.server.bind_address,
        "127.0.0.1:4000".parse().unwrap(),
        "and it is that file that was loaded",
    );
}

/// The document-less boot has no tree to name and says so, rather than naming
/// a path nothing was read from.
#[test]
fn a_document_less_boot_reports_no_inputs() {
    let dir = tempfile::tempdir().unwrap();
    let document = load_config_from(None, &[], dir.path());
    assert!(document.inputs.is_none());
    assert!(document.files.is_empty());
}
