//! Attachment target handler execution.
//!
//! When a file upload targets an app-defined attachment target (e.g. "import"),
//! this module handles running the configured command handler: matching uploaded
//! files to roles, substituting args, executing the subprocess, and building
//! the CC context message.

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use brenn_lib::config::{
    AttachmentHandlerConfig, AttachmentTarget, ContainerSpawnConfig, PathMapper,
};
use tracing::warn;

use crate::routes::upload::WrittenFile;

/// Result of running a target handler.
pub struct HandlerResult {
    pub success: bool,
    pub exit_code: Option<i32>,
    pub summary: String,
    pub detail: Option<String>,
    /// The actual command + args that were executed.
    pub executed_command: Vec<String>,
}

/// Build the final arg list from a template with file-role substitution.
///
/// Each arg is checked for `{role}` placeholders. When a role is filled,
/// the placeholder is replaced with the file path. When a role is unfilled:
/// - The arg containing the placeholder is dropped.
/// - If the preceding arg is `--{role}` (flag name matches role name), it is
///   also dropped (flag + value pair). E.g., `--csv {csv}` → both dropped.
///   Standalone flags like `--json` are never incorrectly dropped.
pub fn build_command_args(args: &[String], role_paths: &HashMap<String, String>) -> Vec<String> {
    let mut final_args: Vec<String> = Vec::new();
    let mut skip_next = false;

    for (i, arg) in args.iter().enumerate() {
        if skip_next {
            skip_next = false;
            continue;
        }

        if let Some(role) = extract_role_placeholder(arg) {
            // This arg IS a placeholder.
            if let Some(path) = role_paths.get(&role) {
                final_args.push(arg.replace(&format!("{{{role}}}"), path));
            }
            // If unfilled, just don't push it. The preceding --flag was already
            // handled by the lookahead in the previous iteration (if it matched).
        } else if i + 1 < args.len() {
            // Lookahead: check if the NEXT arg is a placeholder for an unfilled role,
            // AND this arg is the flag for that role (e.g., --csv for {csv}).
            // Only drop both when the flag name matches the role name.
            if let Some(role) = extract_role_placeholder(&args[i + 1])
                && !role_paths.contains_key(&role)
                && *arg == format!("--{role}")
            {
                skip_next = true;
                continue;
            }
            final_args.push(arg.clone());
        } else {
            final_args.push(arg.clone());
        }
    }

    final_args
}

/// Run the command handler for a target, given the uploaded files.
///
/// When `container_spawn` is `Some`, the command is executed inside the
/// container via `podman run` (same pattern as `run_container_hook` in
/// `hooks.rs`). Uploaded file paths are translated from host paths to
/// container-visible paths via `path_mapper`.
pub async fn run_command_handler(
    target: &AttachmentTarget,
    files: &[WrittenFile],
    working_dir: &Path,
    container_spawn: Option<&ContainerSpawnConfig>,
    path_mapper: &PathMapper,
) -> HandlerResult {
    let AttachmentHandlerConfig::Command {
        program,
        args,
        file_roles,
        timeout_secs,
        ..
    } = &target.handler;

    // Match each file to a role based on extension.
    let mut role_paths: HashMap<String, String> = HashMap::new();
    for file in files {
        let ext = file
            .filename
            .rfind('.')
            .map(|i| file.filename[i..].to_lowercase())
            .unwrap_or_default();

        for (role, extensions) in file_roles {
            if extensions.iter().any(|e| e.to_lowercase() == ext) {
                if role_paths.contains_key(role) {
                    return HandlerResult {
                        success: false,
                        exit_code: None,
                        summary: format!(
                            "Multiple files matched role {role:?}. \
                             Only one file per role is supported."
                        ),
                        detail: None,
                        executed_command: vec![],
                    };
                }
                role_paths.insert(role.clone(), file.path.to_string_lossy().to_string());
                break;
            }
        }
    }

    // For container execution, translate host file paths to container-visible paths.
    // Uploaded files are under working_dir/attachments/ which is inside the bind mount.
    let role_paths = if container_spawn.is_some() {
        role_paths
            .into_iter()
            .map(|(role, host_path)| {
                let container_path = path_mapper
                    .to_container(Path::new(&host_path))
                    .expect("uploaded file path must be within mapped root");
                (role, container_path.to_string_lossy().to_string())
            })
            .collect()
    } else {
        role_paths
    };

    let final_args = build_command_args(args, &role_paths);

    // Build and execute the command, with or without container wrapping.
    let (result, executed_command) = if let Some(container) = container_spawn {
        let mut podman_args = container.base_podman_args();
        podman_args.push(program.clone());
        podman_args.extend(final_args.iter().cloned());

        let mut cmd = vec!["podman".to_string()];
        cmd.extend(podman_args.iter().cloned());

        let spawn_result = tokio::process::Command::new("podman")
            .args(&podman_args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn();

        (spawn_result, cmd)
    } else {
        let mut cmd = vec![program.clone()];
        cmd.extend(final_args.iter().cloned());

        let spawn_result = tokio::process::Command::new(program)
            .args(&final_args)
            .current_dir(working_dir)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn();

        (spawn_result, cmd)
    };

    let child = match result {
        Ok(child) => child,
        Err(e) => {
            let msg = format!("Failed to start command {program:?}: {e}");
            warn!("{msg}");
            return HandlerResult {
                success: false,
                exit_code: None,
                summary: msg,
                detail: None,
                executed_command,
            };
        }
    };

    // Wait with timeout. wait_with_output() takes ownership of child.
    // On timeout, the future is cancelled and child is dropped (which kills it).
    let timeout = Duration::from_secs(*timeout_secs);
    let output = match tokio::time::timeout(timeout, child.wait_with_output()).await {
        Ok(Ok(output)) => output,
        Ok(Err(e)) => {
            let msg = format!("Command {program:?} failed: {e}");
            warn!("{msg}");
            return HandlerResult {
                success: false,
                exit_code: None,
                summary: msg,
                detail: None,
                executed_command,
            };
        }
        Err(_) => {
            let msg = format!("Command {program:?} timed out after {timeout_secs}s");
            warn!("{msg}");
            return HandlerResult {
                success: false,
                exit_code: None,
                summary: msg,
                detail: None,
                executed_command,
            };
        }
    };

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let exit_code = output.status.code();
    let success = output.status.success();

    // Build summary: for successful commands, use stderr (pfin writes human-readable
    // output there). For failures, include both.
    let summary = if success {
        if stderr.is_empty() {
            "Command completed successfully.".to_string()
        } else {
            stderr.trim().to_string()
        }
    } else {
        let code = exit_code.unwrap_or(-1);
        let mut s = format!("Command failed (exit code {code}).");
        if !stderr.is_empty() {
            s.push('\n');
            s.push_str(stderr.trim());
        }
        s
    };

    let detail = format!("stdout:\n{stdout}\nstderr:\n{stderr}");

    HandlerResult {
        success,
        exit_code,
        summary,
        detail: Some(detail),
        executed_command,
    }
}

/// Build the CC context message from handler results.
pub fn build_cc_context(
    target: &AttachmentTarget,
    files: &[WrittenFile],
    result: &HandlerResult,
) -> String {
    let AttachmentHandlerConfig::Command {
        cc_instructions, ..
    } = &target.handler;

    let mut ctx = String::new();

    // Prepend cc_instructions if present.
    if let Some(instructions) = cc_instructions {
        ctx.push_str(instructions);
        ctx.push_str("\n\n");
    }

    ctx.push_str(&format!("[Target handler: {}]\n", target.name));

    // Files uploaded.
    let filenames: Vec<&str> = files.iter().map(|f| f.filename.as_str()).collect();
    ctx.push_str(&format!("Files uploaded: {}\n", filenames.join(", ")));

    // Actual command that was executed.
    if !result.executed_command.is_empty() {
        let cmd_str: Vec<&str> = result.executed_command.iter().map(|s| s.as_str()).collect();
        ctx.push_str(&format!("Command executed: {}\n", cmd_str.join(" ")));
    }

    // Exit code.
    match result.exit_code {
        Some(code) => ctx.push_str(&format!("Exit code: {code}\n")),
        None => ctx.push_str("Exit code: (unavailable)\n"),
    }

    if let Some(detail) = &result.detail {
        ctx.push_str(detail);
    }

    ctx
}

/// Extract a role name from a placeholder like `{ofx}` or `{csv}`.
/// Returns None if the arg doesn't contain a `{...}` placeholder with a
/// non-empty role name, or contains multiple placeholders.
fn extract_role_placeholder(arg: &str) -> Option<String> {
    let start = arg.find('{')?;
    let end = arg.find('}')?;
    if start + 1 < end && arg.matches('{').count() == 1 && arg.matches('}').count() == 1 {
        Some(arg[start + 1..end].to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_lib::config::PathMapping;
    use std::path::PathBuf;
    use uuid::Uuid;

    // -----------------------------------------------------------------------
    // extract_role_placeholder
    // -----------------------------------------------------------------------

    #[test]
    fn test_extract_role_placeholder() {
        assert_eq!(extract_role_placeholder("{ofx}"), Some("ofx".to_string()));
        assert_eq!(extract_role_placeholder("{csv}"), Some("csv".to_string()));
        assert_eq!(extract_role_placeholder("--json"), None);
        assert_eq!(extract_role_placeholder("import"), None);
        // Empty placeholder is not a valid role.
        assert_eq!(extract_role_placeholder("{}"), None);
        // Multiple placeholders.
        assert_eq!(extract_role_placeholder("{a}{b}"), None);
    }

    // -----------------------------------------------------------------------
    // build_command_args
    // -----------------------------------------------------------------------

    #[test]
    fn test_build_args_ofx_only() {
        let args = strs(&["import", "--json", "{ofx}", "--csv", "{csv}"]);
        let role_paths = roles(&[("ofx", "/path/to/file.ofx")]);

        let result = build_command_args(&args, &role_paths);
        assert_eq!(result, vec!["import", "--json", "/path/to/file.ofx"]);
    }

    #[test]
    fn test_build_args_ofx_and_csv() {
        let args = strs(&["import", "--json", "{ofx}", "--csv", "{csv}"]);
        let role_paths = roles(&[("ofx", "/path/to/file.ofx"), ("csv", "/path/to/file.csv")]);

        let result = build_command_args(&args, &role_paths);
        assert_eq!(
            result,
            vec![
                "import",
                "--json",
                "/path/to/file.ofx",
                "--csv",
                "/path/to/file.csv"
            ]
        );
    }

    #[test]
    fn test_build_args_no_roles_filled() {
        let args = strs(&["import", "--json", "{ofx}", "--csv", "{csv}"]);
        let role_paths = HashMap::new();

        // --json is standalone (not --ofx), so it's preserved.
        // --csv matches the {csv} role name, so both are dropped.
        // {ofx} is unfilled and has no matching --ofx flag, so just the placeholder is dropped.
        let result = build_command_args(&args, &role_paths);
        assert_eq!(result, vec!["import", "--json"]);
    }

    #[test]
    fn test_build_args_unrelated_flags_preserved() {
        let args = strs(&["cmd", "--verbose", "--missing", "{missing}"]);
        let role_paths = HashMap::new();

        let result = build_command_args(&args, &role_paths);
        assert_eq!(result, vec!["cmd", "--verbose"]);
    }

    #[test]
    fn test_build_args_standalone_flag_before_positional_placeholder() {
        let args = strs(&["cmd", "--json", "{ofx}"]);
        let role_paths = HashMap::new();

        let result = build_command_args(&args, &role_paths);
        assert_eq!(result, vec!["cmd", "--json"]);
    }

    #[test]
    fn test_build_args_all_positional() {
        let args = strs(&["cmd", "{a}", "{b}"]);
        let role_paths = roles(&[("a", "/path/a")]);

        let result = build_command_args(&args, &role_paths);
        assert_eq!(result, vec!["cmd", "/path/a"]);
    }

    // -----------------------------------------------------------------------
    // build_cc_context
    // -----------------------------------------------------------------------

    fn make_target(cc_instructions: Option<&str>) -> AttachmentTarget {
        AttachmentTarget {
            name: "import".to_string(),
            label: "Import bank export".to_string(),
            accept: vec![".ofx".to_string()],
            multi: false,
            handler: AttachmentHandlerConfig::Command {
                program: "pf".to_string(),
                args: vec![],
                file_roles: HashMap::new(),
                timeout_secs: 60,
                cc_instructions: cc_instructions.map(|s| s.to_string()),
            },
        }
    }

    fn make_written_file(filename: &str) -> WrittenFile {
        WrittenFile {
            upload_id: Uuid::new_v4(),
            filename: filename.to_string(),
            disk_filename: format!("uuid_{filename}"),
            media_type: "application/octet-stream".to_string(),
            size: 100,
            path: PathBuf::from(format!("/tmp/attachments/uuid_{filename}")),
        }
    }

    #[test]
    fn test_cc_context_basic() {
        let target = make_target(None);
        let files = vec![make_written_file("data.ofx")];
        let result = HandlerResult {
            success: true,
            exit_code: Some(0),
            summary: "OK".to_string(),
            detail: Some("stdout:\n{}\nstderr:\n".to_string()),
            executed_command: vec![
                "pf".to_string(),
                "import".to_string(),
                "/tmp/data.ofx".to_string(),
            ],
        };

        let ctx = build_cc_context(&target, &files, &result);
        assert!(ctx.contains("[Target handler: import]"));
        assert!(ctx.contains("Files uploaded: data.ofx"));
        assert!(ctx.contains("Command executed: pf import /tmp/data.ofx"));
        assert!(ctx.contains("Exit code: 0"));
        // No cc_instructions, so no preamble.
        assert!(ctx.starts_with("[Target handler:"));
    }

    #[test]
    fn test_cc_context_with_instructions() {
        let target = make_target(Some("Review pending transactions."));
        let files = vec![make_written_file("data.ofx")];
        let result = HandlerResult {
            success: true,
            exit_code: Some(0),
            summary: "OK".to_string(),
            detail: None,
            executed_command: vec!["pf".to_string()],
        };

        let ctx = build_cc_context(&target, &files, &result);
        assert!(ctx.starts_with("Review pending transactions.\n\n[Target handler:"));
    }

    #[test]
    fn test_cc_context_failure() {
        let target = make_target(None);
        let files = vec![make_written_file("data.ofx")];
        let result = HandlerResult {
            success: false,
            exit_code: Some(1),
            summary: "Failed".to_string(),
            detail: Some("stdout:\n\nstderr:\nerror msg\n".to_string()),
            executed_command: vec!["pf".to_string()],
        };

        let ctx = build_cc_context(&target, &files, &result);
        assert!(ctx.contains("Exit code: 1"));
        assert!(ctx.contains("error msg"));
    }

    #[test]
    fn test_cc_context_no_exit_code() {
        let target = make_target(None);
        let files = vec![];
        let result = HandlerResult {
            success: false,
            exit_code: None,
            summary: "Timeout".to_string(),
            detail: None,
            executed_command: vec![],
        };

        let ctx = build_cc_context(&target, &files, &result);
        assert!(ctx.contains("Exit code: (unavailable)"));
    }

    // -----------------------------------------------------------------------
    // run_command_handler (integration — runs real subprocesses)
    // -----------------------------------------------------------------------

    fn make_command_target(
        program: &str,
        args: &[&str],
        file_roles: &[(&str, &[&str])],
    ) -> AttachmentTarget {
        AttachmentTarget {
            name: "test".to_string(),
            label: "Test".to_string(),
            accept: vec![".txt".to_string()],
            multi: false,
            handler: AttachmentHandlerConfig::Command {
                program: program.to_string(),
                args: args.iter().map(|s| s.to_string()).collect(),
                file_roles: file_roles
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.iter().map(|s| s.to_string()).collect()))
                    .collect(),
                timeout_secs: 5,
                cc_instructions: None,
            },
        }
    }

    #[tokio::test]
    async fn test_handler_success() {
        let dir = tempfile::tempdir().unwrap();
        let target = make_command_target("echo", &["hello"], &[]);
        let result =
            run_command_handler(&target, &[], dir.path(), None, &PathMapper::Identity).await;

        assert!(result.success);
        assert_eq!(result.exit_code, Some(0));
        assert!(result.detail.unwrap().contains("hello"));
        assert_eq!(result.executed_command, vec!["echo", "hello"]);
    }

    #[tokio::test]
    async fn test_handler_failure() {
        let dir = tempfile::tempdir().unwrap();
        let target = make_command_target("false", &[], &[]);
        let result =
            run_command_handler(&target, &[], dir.path(), None, &PathMapper::Identity).await;

        assert!(!result.success);
        assert_eq!(result.exit_code, Some(1));
        assert!(result.summary.contains("exit code 1"));
    }

    #[tokio::test]
    async fn test_handler_program_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let target = make_command_target("nonexistent_program_xyz", &[], &[]);
        let result =
            run_command_handler(&target, &[], dir.path(), None, &PathMapper::Identity).await;

        assert!(!result.success);
        assert!(result.summary.contains("Failed to start"));
    }

    #[tokio::test]
    async fn test_handler_timeout() {
        let dir = tempfile::tempdir().unwrap();
        // sleep 60 should be killed by the 5-second timeout.
        // Use a target with 1-second timeout for faster test.
        let mut target = make_command_target("sleep", &["60"], &[]);
        let AttachmentHandlerConfig::Command {
            ref mut timeout_secs,
            ..
        } = target.handler;
        *timeout_secs = 1;

        let result =
            run_command_handler(&target, &[], dir.path(), None, &PathMapper::Identity).await;

        assert!(!result.success);
        assert!(result.summary.contains("timed out"));
    }

    #[tokio::test]
    async fn test_handler_file_role_matching() {
        let dir = tempfile::tempdir().unwrap();

        // Create a real file so the path exists.
        let file_path = dir.path().join("test.ofx");
        std::fs::write(&file_path, "ofx content").unwrap();

        let target = make_command_target("cat", &["{ofx}"], &[("ofx", &[".ofx"])]);
        let files = vec![WrittenFile {
            upload_id: Uuid::new_v4(),
            filename: "test.ofx".to_string(),
            disk_filename: "uuid_test.ofx".to_string(),
            media_type: "text/plain".to_string(),
            size: 11,
            path: file_path,
        }];

        let result =
            run_command_handler(&target, &files, dir.path(), None, &PathMapper::Identity).await;
        assert!(result.success);
        assert!(result.detail.unwrap().contains("ofx content"));
    }

    #[tokio::test]
    async fn test_handler_duplicate_role_error() {
        let dir = tempfile::tempdir().unwrap();
        let target = make_command_target("echo", &["{ofx}"], &[("ofx", &[".ofx", ".qfx"])]);

        // Two files both matching the "ofx" role.
        let files = vec![make_written_file("a.ofx"), make_written_file("b.qfx")];

        let result =
            run_command_handler(&target, &files, dir.path(), None, &PathMapper::Identity).await;
        assert!(!result.success);
        assert!(result.summary.contains("Multiple files matched role"));
    }

    #[tokio::test]
    async fn test_handler_stderr_in_summary() {
        let dir = tempfile::tempdir().unwrap();
        // sh -c 'echo error >&2' writes to stderr and exits 0.
        let target = make_command_target("sh", &["-c", "echo error >&2"], &[]);

        let result =
            run_command_handler(&target, &[], dir.path(), None, &PathMapper::Identity).await;
        assert!(result.success);
        assert_eq!(result.summary, "error");
    }

    // -----------------------------------------------------------------------
    // container execution
    // -----------------------------------------------------------------------

    /// Build the same container + mapper fixtures used by container tests.
    fn make_container_fixtures() -> (PathBuf, PathBuf, ContainerSpawnConfig, PathMapper) {
        let host_root = PathBuf::from("/home/user/src/pfin");
        let container_root = PathBuf::from("/workspace/pfin");

        let container = ContainerSpawnConfig {
            image: "test-image:latest".to_string(),
            home_dir: PathBuf::from("/home/user"),
            container_home: PathBuf::from("/home/user"),
            host_working_dir: host_root.clone(),
            container_working_dir: container_root.clone(),
            working_dir_is_repo: false,
            repo_mounts: vec![],
            extra_mounts: vec![],
            extra_args: vec![],
        };

        let mapper = PathMapper::container(vec![PathMapping {
            host_root: host_root.clone(),
            container_root: container_root.clone(),
        }]);

        (host_root, container_root, container, mapper)
    }

    #[tokio::test]
    async fn test_handler_container_translates_file_paths() {
        // Verify that when a container is configured, file paths in command
        // args are translated from host paths to container paths, and the
        // podman arg structure is correct.
        //
        // We can't run an actual podman command in unit tests, so the command
        // will fail to spawn. But executed_command is populated before spawn,
        // so we can verify the full arg structure from the error result.
        let (host_root, _container_root, container, mapper) = make_container_fixtures();

        let target = make_command_target("pf", &["import", "{ofx}"], &[("ofx", &[".ofx"])]);
        let files = vec![WrittenFile {
            upload_id: Uuid::new_v4(),
            filename: "test.ofx".to_string(),
            disk_filename: "uuid_test.ofx".to_string(),
            media_type: "application/octet-stream".to_string(),
            size: 100,
            path: PathBuf::from("/home/user/src/pfin/attachments/uuid_test.ofx"),
        }];

        let result =
            run_command_handler(&target, &files, &host_root, Some(&container), &mapper).await;

        // Verify full arg structure: podman <base_args...> image program arg...
        // base_podman_args ends with the image name; our command follows.
        let cmd = &result.executed_command;
        assert_eq!(cmd[0], "podman");
        assert_eq!(cmd[1], "run");
        assert_eq!(cmd[2], "--rm");

        // Image should appear before our command.
        let image_pos = cmd.iter().position(|a| a == "test-image:latest").unwrap();
        assert_eq!(cmd[image_pos + 1], "pf", "program should follow image");
        assert_eq!(
            cmd[image_pos + 2],
            "import",
            "first arg should follow program"
        );
        // The file path arg should be the container path, not the host path.
        assert_eq!(
            cmd[image_pos + 3],
            "/workspace/pfin/attachments/uuid_test.ofx",
            "file path should be translated to container path"
        );

        // Host path must not appear anywhere in the args.
        assert!(
            !cmd.iter()
                .any(|a| a.contains("/home/user/src/pfin/attachments/")),
            "host path should not appear in container args: {cmd:?}"
        );
    }

    #[tokio::test]
    async fn test_handler_container_no_file_roles() {
        // Container execution with no file roles — just verify the command
        // structure is correct (program + args appended after image).
        let (host_root, _container_root, container, mapper) = make_container_fixtures();

        let target = make_command_target("pf", &["status", "--json"], &[]);

        let result = run_command_handler(&target, &[], &host_root, Some(&container), &mapper).await;

        let cmd = &result.executed_command;
        let image_pos = cmd.iter().position(|a| a == "test-image:latest").unwrap();
        assert_eq!(&cmd[image_pos + 1..], &["pf", "status", "--json"]);
    }

    // -----------------------------------------------------------------------
    // helpers
    // -----------------------------------------------------------------------

    fn strs(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    fn roles(v: &[(&str, &str)]) -> HashMap<String, String> {
        v.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }
}
