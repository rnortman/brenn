use super::*;
use crate::config::ResolvedConfig;
use crate::integration::IntegrationRegistry;

// -----------------------------------------------------------------------
// Primary-owner validation (repo-sync design)
//
// Tests use `two_app_mount_config` to wire up two apps mounting the
// same repo, since the interesting case (>=2 RW mounts on one clone)
// needs multiple apps.
// -----------------------------------------------------------------------

/// Build a BrennConfig with two apps, each mounting the given repo.
/// Every app has a concrete working dir under `dir`.
fn two_app_mount_config(
    dir: &std::path::Path,
    repos: Vec<RepoDeclRaw>,
    app_a_mounts: Vec<MountConfigRaw>,
    app_b_mounts: Vec<MountConfigRaw>,
) -> BrennConfig {
    let app_a_dir = dir.join("app-a");
    let app_b_dir = dir.join("app-b");
    std::fs::create_dir_all(&app_a_dir).unwrap();
    std::fs::create_dir_all(&app_b_dir).unwrap();
    let make_app = |slug: &str, wd: PathBuf, mounts: Vec<MountConfigRaw>| AppConfigRaw {
        slug: slug.to_string(),
        working_dir: Some(wd),
        mounts,
        ..Default::default()
    };
    BrennConfig {
        server: super::test_server_config(),
        repo_sync: repo_sync_at(dir),
        repos,
        apps: vec![
            make_app("app-a", app_a_dir, app_a_mounts),
            make_app("app-b", app_b_dir, app_b_mounts),
        ],
        ..Default::default()
    }
}

#[test]
fn primary_implicit_on_single_rw_mount() {
    // One RW mount + one RO mount → the RW is implicit-primary.
    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("shared");
    std::fs::create_dir(&repo_dir).unwrap();
    let config = two_app_mount_config(
        dir.path(),
        vec![RepoDeclRaw {
            slug: "shared".to_string(),
            remote: "https://example.com/s.git".to_string(),
            auto_pull: true,
        }],
        vec![MountConfigRaw {
            repo: "shared".to_string(),
            access: AccessLevel::ReadWrite,
            working_dir: false,
            auto_pull: None,
            primary: false,
        }],
        vec![MountConfigRaw {
            repo: "shared".to_string(),
            access: AccessLevel::ReadOnly,
            working_dir: false,
            auto_pull: None,
            primary: false,
        }],
    );
    let ResolvedConfig { apps, .. } = validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
    let rw = apps.get("app-a").unwrap().mounts[0].clone();
    let ro = apps.get("app-b").unwrap().mounts[0].clone();
    assert!(rw.primary, "single RW mount must be promoted to primary");
    assert!(!ro.primary, "RO mount must never be primary");
}

#[test]
fn primary_explicit_redundant_ok_on_single_rw() {
    // Declaring primary=true on the single RW mount is allowed (redundant).
    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("shared");
    std::fs::create_dir(&repo_dir).unwrap();
    let config = two_app_mount_config(
        dir.path(),
        vec![RepoDeclRaw {
            slug: "shared".to_string(),
            remote: "https://example.com/s.git".to_string(),
            auto_pull: true,
        }],
        vec![MountConfigRaw {
            repo: "shared".to_string(),
            access: AccessLevel::ReadWrite,
            working_dir: false,
            auto_pull: None,
            primary: true,
        }],
        vec![MountConfigRaw {
            repo: "shared".to_string(),
            access: AccessLevel::ReadOnly,
            working_dir: false,
            auto_pull: None,
            primary: false,
        }],
    );
    let ResolvedConfig { apps, .. } = validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
    assert!(apps.get("app-a").unwrap().mounts[0].primary);
}

#[test]
fn primary_explicit_on_multi_rw_ok() {
    // Two RW mounts: app-a declares primary, app-b does not. Accepted.
    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("shared");
    std::fs::create_dir(&repo_dir).unwrap();
    let config = two_app_mount_config(
        dir.path(),
        vec![RepoDeclRaw {
            slug: "shared".to_string(),
            remote: "https://example.com/s.git".to_string(),
            auto_pull: true,
        }],
        vec![MountConfigRaw {
            repo: "shared".to_string(),
            access: AccessLevel::ReadWrite,
            working_dir: false,
            auto_pull: None,
            primary: true,
        }],
        vec![MountConfigRaw {
            repo: "shared".to_string(),
            access: AccessLevel::ReadWrite,
            working_dir: false,
            auto_pull: None,
            primary: false,
        }],
    );
    let ResolvedConfig { apps, .. } = validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
    assert!(apps.get("app-a").unwrap().mounts[0].primary);
    assert!(!apps.get("app-b").unwrap().mounts[0].primary);
}

#[test]
#[should_panic(expected = "no mount declares `primary = true`")]
fn primary_undeclared_on_multi_rw_panics() {
    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("shared");
    std::fs::create_dir(&repo_dir).unwrap();
    let config = two_app_mount_config(
        dir.path(),
        vec![RepoDeclRaw {
            slug: "shared".to_string(),
            remote: "https://example.com/s.git".to_string(),
            auto_pull: true,
        }],
        vec![MountConfigRaw {
            repo: "shared".to_string(),
            access: AccessLevel::ReadWrite,
            working_dir: false,
            auto_pull: None,
            primary: false,
        }],
        vec![MountConfigRaw {
            repo: "shared".to_string(),
            access: AccessLevel::ReadWrite,
            working_dir: false,
            auto_pull: None,
            primary: false,
        }],
    );
    validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
}

#[test]
#[should_panic(expected = "Exactly one primary per clone")]
fn primary_double_declared_panics() {
    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("shared");
    std::fs::create_dir(&repo_dir).unwrap();
    let config = two_app_mount_config(
        dir.path(),
        vec![RepoDeclRaw {
            slug: "shared".to_string(),
            remote: "https://example.com/s.git".to_string(),
            auto_pull: true,
        }],
        vec![MountConfigRaw {
            repo: "shared".to_string(),
            access: AccessLevel::ReadWrite,
            working_dir: false,
            auto_pull: None,
            primary: true,
        }],
        vec![MountConfigRaw {
            repo: "shared".to_string(),
            access: AccessLevel::ReadWrite,
            working_dir: false,
            auto_pull: None,
            primary: true,
        }],
    );
    validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
}

#[test]
#[should_panic(expected = "read-only mount")]
fn primary_on_ro_mount_panics() {
    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("shared");
    std::fs::create_dir(&repo_dir).unwrap();
    let config = two_app_mount_config(
        dir.path(),
        vec![RepoDeclRaw {
            slug: "shared".to_string(),
            remote: "https://example.com/s.git".to_string(),
            auto_pull: true,
        }],
        vec![MountConfigRaw {
            repo: "shared".to_string(),
            access: AccessLevel::ReadWrite,
            working_dir: false,
            auto_pull: None,
            primary: false,
        }],
        vec![MountConfigRaw {
            repo: "shared".to_string(),
            access: AccessLevel::ReadOnly,
            working_dir: false,
            auto_pull: None,
            primary: true, // error: RO can't be primary
        }],
    );
    validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
}

#[test]
#[should_panic(expected = "clone with no read-write mounts")]
fn primary_on_ro_only_clone_panics() {
    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("shared");
    std::fs::create_dir(&repo_dir).unwrap();
    let config = two_app_mount_config(
        dir.path(),
        vec![RepoDeclRaw {
            slug: "shared".to_string(),
            remote: "https://example.com/s.git".to_string(),
            auto_pull: true,
        }],
        vec![MountConfigRaw {
            repo: "shared".to_string(),
            access: AccessLevel::ReadOnly,
            working_dir: false,
            auto_pull: None,
            primary: true, // error: no RW mounts on this clone
        }],
        vec![MountConfigRaw {
            repo: "shared".to_string(),
            access: AccessLevel::ReadOnly,
            working_dir: false,
            auto_pull: None,
            primary: false,
        }],
    );
    validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
}

#[test]
fn ro_only_clone_no_primary_no_panic() {
    // Two RO mounts, no primary declarations anywhere — valid.
    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("shared");
    std::fs::create_dir(&repo_dir).unwrap();
    let config = two_app_mount_config(
        dir.path(),
        vec![RepoDeclRaw {
            slug: "shared".to_string(),
            remote: "https://example.com/s.git".to_string(),
            auto_pull: true,
        }],
        vec![MountConfigRaw {
            repo: "shared".to_string(),
            access: AccessLevel::ReadOnly,
            working_dir: false,
            auto_pull: None,
            primary: false,
        }],
        vec![MountConfigRaw {
            repo: "shared".to_string(),
            access: AccessLevel::ReadOnly,
            working_dir: false,
            auto_pull: None,
            primary: false,
        }],
    );
    let ResolvedConfig { apps, .. } = validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
    assert!(!apps.get("app-a").unwrap().mounts[0].primary);
    assert!(!apps.get("app-b").unwrap().mounts[0].primary);
}
