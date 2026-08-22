use super::*;

// -----------------------------------------------------------------------
// load_config()
// -----------------------------------------------------------------------

#[test]
fn load_config_explicit_path() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.toml");
    std::fs::write(
        &path,
        r#"
[server]
public_url = "https://brenn.example.com"
bind_address = "127.0.0.1:4000"
secure_cookies = false
"#,
    )
    .unwrap();
    let config = load_config_from(Some(&path), dir.path());
    assert_eq!(
        config.server.bind_address,
        "127.0.0.1:4000".parse().unwrap()
    );
    assert!(!config.server.secure_cookies);
}

#[test]
#[should_panic(expected = "failed to read config file")]
fn load_config_explicit_path_missing_panics() {
    load_config_from(
        Some(Path::new("/nonexistent/brenn.toml")),
        Path::new("/tmp"),
    );
}

#[test]
#[should_panic(expected = "failed to parse config file")]
fn load_config_explicit_path_invalid_toml_panics() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.toml");
    std::fs::write(&path, "this is not valid toml [[[").unwrap();
    load_config_from(Some(&path), dir.path());
}

#[test]
fn load_config_no_path_no_file_returns_defaults() {
    // Empty temp directory — no brenn.toml present.
    let dir = tempfile::tempdir().unwrap();
    let config = load_config_from(None, dir.path());
    // Should get production defaults.
    assert!(config.server.secure_cookies);
    assert_eq!(
        config.server.bind_address,
        SocketAddr::from(([0, 0, 0, 0], 3000))
    );
}

#[test]
fn load_config_finds_brenn_toml_in_fallback_dir() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("brenn.toml"),
        r#"
[server]
bind_address = "10.0.0.1:5555"
"#,
    )
    .unwrap();
    let config = load_config_from(None, dir.path());
    assert_eq!(config.server.bind_address, "10.0.0.1:5555".parse().unwrap());
}

#[test]
#[should_panic(expected = "failed to parse")]
fn load_config_invalid_brenn_toml_in_fallback_dir_panics() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("brenn.toml"), "garbage [[[").unwrap();
    load_config_from(None, dir.path());
}

// -----------------------------------------------------------------------
// Extension dispatch: `.toml` vs `.brenn`
// -----------------------------------------------------------------------

/// A `.brenn` document; `TWIN` is its TOML equivalent.
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

const TWIN: &str = r#"
[server]
public_url = "https://brenn.example.com"
bind_address = "127.0.0.1:4000"
secure_cookies = false

[[channel]]
uuid = "85a5cf7e-6874-5766-9d69-712784754a1f"
address = "brenn:alice-alerts"
push_depth = 8
retain_depth = 128
standing_retain_depth = 16
"#;

#[test]
fn load_config_brenn_path_equals_its_toml_twin() {
    let dir = tempfile::tempdir().unwrap();
    let document = dir.path().join("main.brenn");
    std::fs::write(&document, DOCUMENT).unwrap();
    let twin = dir.path().join("twin.toml");
    std::fs::write(&twin, TWIN).unwrap();

    let from_dsl = load_config_from(Some(&document), dir.path());
    let from_toml = load_config_from(Some(&twin), dir.path());
    assert_eq!(from_dsl, from_toml);
    assert_eq!(
        from_dsl.server.bind_address,
        "127.0.0.1:4000".parse().unwrap()
    );
}

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
fn load_config_extensionless_path_panics() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("brennconfig");
    std::fs::write(&path, "[server]\n").unwrap();
    load_config_from(Some(&path), dir.path());
}

// -----------------------------------------------------------------------
// The no-`--config` fallback probes both names
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

/// Two configs that could disagree, and no way to tell from the outside which
/// one the server read. Neither is read.
#[test]
#[should_panic(expected = "holds more than one config file")]
fn load_config_both_fallback_names_panics() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("brenn.brenn"), DOCUMENT).unwrap();
    std::fs::write(dir.path().join("brenn.toml"), TWIN).unwrap();
    load_config_from(None, dir.path());
}

/// A name that can neither be confirmed present nor confirmed absent is not
/// read as absent: a symlink loop named `brenn.brenn` beside a real
/// `brenn.toml` would otherwise boot the TOML file with no word said, which is
/// the silent precedence the both-present panic exists to forbid.
#[test]
#[should_panic(expected = "cannot be determined")]
fn load_config_unstattable_fallback_name_panics() {
    let dir = tempfile::tempdir().unwrap();
    let loop_path = dir.path().join("brenn.brenn");
    std::os::unix::fs::symlink(&loop_path, &loop_path).unwrap();
    std::fs::write(dir.path().join("brenn.toml"), TWIN).unwrap();
    load_config_from(None, dir.path());
}
