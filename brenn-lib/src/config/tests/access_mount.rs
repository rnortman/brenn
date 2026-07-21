use super::*;
use serde::Deserialize;

// -----------------------------------------------------------------------
// TOML parsing: AccessLevel and MountConfigRaw
// -----------------------------------------------------------------------

#[test]
fn access_level_parses_kebab_case() {
    #[derive(Deserialize)]
    struct Wrapper {
        access: AccessLevel,
    }
    let rw: Wrapper = toml::from_str(r#"access = "read-write""#).unwrap();
    assert_eq!(rw.access, AccessLevel::ReadWrite);

    let ro: Wrapper = toml::from_str(r#"access = "read-only""#).unwrap();
    assert_eq!(ro.access, AccessLevel::ReadOnly);
}

#[test]
fn access_level_rejects_non_kebab_case() {
    #[derive(Deserialize)]
    struct Wrapper {
        #[allow(dead_code)]
        access: AccessLevel,
    }
    assert!(toml::from_str::<Wrapper>(r#"access = "readwrite""#).is_err());
    assert!(toml::from_str::<Wrapper>(r#"access = "ReadWrite""#).is_err());
}

#[test]
fn mount_config_defaults_from_toml() {
    let dir = tempfile::tempdir().unwrap();
    let app_dir = dir.path().join("app");
    std::fs::create_dir(&app_dir).unwrap();

    let toml = format!(
        r#"
repo_dir = "{}"

[[repo]]
slug = "myrepo"
remote = "https://example.com/r.git"

[[app]]
slug = "myapp"
working_dir = "{}"

[[app.mount]]
repo = "myrepo"
"#,
        dir.path().display(),
        app_dir.display(),
    );
    let config: BrennConfig = toml::from_str(&toml).unwrap();
    let mount = &config.apps[0].mounts[0];
    assert_eq!(mount.access, AccessLevel::ReadWrite); // default
    assert!(!mount.working_dir); // default false
    assert!(mount.auto_pull.is_none()); // default None
}

#[test]
fn mount_config_access_read_only_from_toml() {
    let dir = tempfile::tempdir().unwrap();
    let app_dir = dir.path().join("app");
    std::fs::create_dir(&app_dir).unwrap();

    let toml = format!(
        r#"
repo_dir = "{}"

[[repo]]
slug = "docs"
remote = "https://example.com/docs.git"

[[app]]
slug = "myapp"
working_dir = "{}"

[[app.mount]]
repo = "docs"
access = "read-only"
working_dir = false
auto_pull = false
"#,
        dir.path().display(),
        app_dir.display(),
    );
    let config: BrennConfig = toml::from_str(&toml).unwrap();
    let mount = &config.apps[0].mounts[0];
    assert_eq!(mount.access, AccessLevel::ReadOnly);
    assert!(!mount.working_dir);
    assert_eq!(mount.auto_pull, Some(false));
}
