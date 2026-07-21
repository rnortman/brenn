use super::*;
use crate::config::ResolvedConfig;
use crate::integration::IntegrationRegistry;

// -----------------------------------------------------------------------
// Attachment target validation
// -----------------------------------------------------------------------

/// Helper to build a minimal valid AppConfigRaw with attachment_targets.
fn app_raw_with_targets(dir: &std::path::Path, targets: Vec<AttachmentTargetRaw>) -> AppConfigRaw {
    AppConfigRaw {
        slug: "test".to_string(),
        working_dir: Some(dir.to_path_buf()),
        attachment_targets: targets,
        ..Default::default()
    }
}

fn make_import_target(name: &str) -> AttachmentTargetRaw {
    AttachmentTargetRaw {
        name: name.to_string(),
        label: "Test target".to_string(),
        accept: vec![".ofx".to_string()],
        multi: false,
        handler: AttachmentHandlerConfig::Command {
            program: "echo".to_string(),
            args: vec!["{ofx}".to_string()],
            file_roles: HashMap::from([("ofx".to_string(), vec![".ofx".to_string()])]),
            timeout_secs: 60,
            cc_instructions: None,
        },
    }
}

#[test]
fn attachment_target_valid_config() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![app_raw_with_targets(
            dir.path(),
            vec![make_import_target("import")],
        )],
        ..Default::default()
    };
    let ResolvedConfig { apps, .. } = validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
    let app = apps.get("test").unwrap();
    assert_eq!(app.attachment_targets.len(), 1);
    assert_eq!(app.attachment_targets[0].name, "import");
    assert_eq!(app.attachment_targets[0].accept, vec![".ofx"]);
}

#[test]
#[should_panic(expected = "duplicate attachment target")]
fn attachment_target_duplicate_name_panics() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![app_raw_with_targets(
            dir.path(),
            vec![make_import_target("import"), make_import_target("import")],
        )],
        ..Default::default()
    };
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
#[should_panic(expected = "'chat' is a reserved")]
fn attachment_target_reserved_chat_name_panics() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![app_raw_with_targets(
            dir.path(),
            vec![make_import_target("chat")],
        )],
        ..Default::default()
    };
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
#[should_panic(expected = "must not be empty")]
fn attachment_target_empty_name_panics() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![app_raw_with_targets(
            dir.path(),
            vec![make_import_target("")],
        )],
        ..Default::default()
    };
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
#[should_panic(expected = "must accept at least one extension")]
fn attachment_target_empty_accept_panics() {
    let dir = tempfile::tempdir().unwrap();
    let mut target = make_import_target("import");
    target.accept = vec![];
    let config = BrennConfig {
        apps: vec![app_raw_with_targets(dir.path(), vec![target])],
        ..Default::default()
    };
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

#[test]
#[should_panic(expected = "must start with '.'")]
fn attachment_target_extension_without_dot_panics() {
    let dir = tempfile::tempdir().unwrap();
    let mut target = make_import_target("import");
    target.accept = vec!["ofx".to_string()];
    let config = BrennConfig {
        apps: vec![app_raw_with_targets(dir.path(), vec![target])],
        ..Default::default()
    };
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}
