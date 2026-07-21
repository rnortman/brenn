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
