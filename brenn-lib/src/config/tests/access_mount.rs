use serde::Deserialize;
use serde::de::value::StrDeserializer;

use super::*;

// -----------------------------------------------------------------------
// AccessLevel's token spellings
// -----------------------------------------------------------------------

/// `AccessLevel` is deserialized from a bare word by the lowering pass, so its
/// serde spellings are the spellings a `.brenn` document uses.
fn access_level(word: &str) -> Result<AccessLevel, serde::de::value::Error> {
    AccessLevel::deserialize(StrDeserializer::<serde::de::value::Error>::new(word))
}

#[test]
fn access_level_parses_kebab_case() {
    assert_eq!(access_level("read-write").unwrap(), AccessLevel::ReadWrite);
    assert_eq!(access_level("read-only").unwrap(), AccessLevel::ReadOnly);
}

#[test]
fn access_level_rejects_non_kebab_case() {
    assert!(access_level("readwrite").is_err());
    assert!(access_level("ReadWrite").is_err());
}

// -----------------------------------------------------------------------
// A mount's defaults, as loaded
// -----------------------------------------------------------------------

#[test]
fn mount_config_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let app_dir = dir.path().join("app");
    std::fs::create_dir(&app_dir).unwrap();

    let config = config_from_dsl(&format!(
        r#"
repo_sync {{ repo_dir = "{}"; }}

repo myrepo {{ remote = "https://example.com/r.git"; }}

agent Assistant() {{
    working_dir = "{}";

    mount myrepo {{}}
}}

new myapp: Assistant();
"#,
        dir.path().display(),
        app_dir.display(),
    ));
    let mount = &config.apps[0].mounts[0];
    assert_eq!(mount.access, AccessLevel::ReadWrite); // default
    assert!(!mount.working_dir); // default false
    assert!(mount.auto_pull.is_none()); // default None
}

#[test]
fn mount_config_access_read_only() {
    let dir = tempfile::tempdir().unwrap();
    let app_dir = dir.path().join("app");
    std::fs::create_dir(&app_dir).unwrap();

    let config = config_from_dsl(&format!(
        r#"
repo_sync {{ repo_dir = "{}"; }}

repo docs {{ remote = "https://example.com/docs.git"; }}

agent Assistant() {{
    working_dir = "{}";

    mount docs {{
        access = read-only;
        working_dir = false;
        auto_pull = false;
    }}
}}

new myapp: Assistant();
"#,
        dir.path().display(),
        app_dir.display(),
    ));
    let mount = &config.apps[0].mounts[0];
    assert_eq!(mount.access, AccessLevel::ReadOnly);
    assert!(!mount.working_dir);
    assert_eq!(mount.auto_pull, Some(false));
}
