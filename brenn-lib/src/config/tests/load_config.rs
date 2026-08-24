use super::*;

// -----------------------------------------------------------------------
// load_config()
// -----------------------------------------------------------------------

#[test]
fn load_config_explicit_path() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("main.brenn");
    std::fs::write(&path, DOCUMENT).unwrap();
    let config = load_config_from(Some(&path), dir.path());
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
        Path::new("/tmp"),
    );
}

#[test]
fn load_config_no_path_no_file_returns_defaults() {
    // Empty temp directory — no config file present.
    let dir = tempfile::tempdir().unwrap();
    let config = load_config_from(None, dir.path());
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
    load_config_from(Some(&path), dir.path());
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
    load_config_from(Some(&path), dir.path());
}

/// The boot panic shows every refusal, not just the first.
#[test]
fn load_config_brenn_lower_panic_reports_every_refusal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("main.brenn");
    // Two independent value-typing refusals in one section.
    std::fs::write(&path, "server { public_url = 3; secure_cookies = 3; }").unwrap();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        load_config_from(Some(&path), dir.path())
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
    load_config_from(Some(&path), dir.path());
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
    load_config_from(Some(&path), dir.path());
}

#[test]
#[should_panic(expected = "unrecognized extension")]
fn load_config_extensionless_path_panics() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("brennconfig");
    std::fs::write(&path, "[server]\n").unwrap();
    load_config_from(Some(&path), dir.path());
}

// -----------------------------------------------------------------------
// The no-`--config` fallback probe
// -----------------------------------------------------------------------

#[test]
fn load_config_finds_brenn_brenn_in_fallback_dir() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("brenn.brenn"), DOCUMENT).unwrap();
    let config = load_config_from(None, dir.path());
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
    load_config_from(None, dir.path());
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
    load_config_from(None, dir.path());
}
