//! Shared subprocess utilities for integration crates.
//!
//! Provides `run_in_app_env()` — a container-aware command builder used by
//! `brenn-graf` and `brenn-pfin` to spawn CLI tools in the correct environment
//! (bare process on dev, podman on containerized deployments).
//!
//! Also provides `drain_stream()` — shared output-draining helper used by
//! `brenn-graf/src/subprocess.rs` and `brenn/src/git_subprocess.rs`.

use std::collections::HashMap;
use std::io;
use std::path::Path;

use tokio::io::AsyncReadExt;

use crate::config::ContainerSpawnConfig;

/// Bundled subprocess execution context for integration-spawned one-shot processes.
///
/// Groups the four params that every pfin exec function receives: `command`/`env`
/// (pfin-config-level) and `working_dir`/`container_spawn` (app-level). Constructed
/// once per call site from `PfinConfig` + `ActiveBridge` fields; passed by reference
/// to all exec functions, eliminating repeated four-loose-param threading.
///
/// Borrows all four values — callers own the backing data (config struct and bridge
/// fields). Closures that must be `'static` (e.g., `buffer_unordered`) still clone
/// the values before entering the closure; this type simplifies *function signatures*,
/// not per-future ownership.
pub struct SubprocessExecContext<'a> {
    pub command: &'a str,
    pub env: &'a HashMap<String, String>,
    pub working_dir: &'a Path,
    pub container_spawn: Option<&'a ContainerSpawnConfig>,
}

/// Reads up to `cap + 1` bytes from `handle`. Returns exactly `cap + 1`
/// bytes if the stream exceeds the cap, enabling `buf.len() > cap` to detect
/// overflow. The `+ 1` sentinel is an implementation detail — callers pass
/// the actual byte cap, not `cap + 1`.
pub async fn drain_stream(
    handle: impl tokio::io::AsyncRead + Unpin,
    cap: usize,
) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    handle
        .take(
            (cap as u64)
                .checked_add(1)
                .expect("drain_stream cap overflows u64"),
        )
        .read_to_end(&mut buf)
        .await?;
    Ok(buf)
}

/// Build a `tokio::process::Command` that runs in the app's environment.
///
/// For bare-process apps (`container_spawn` is None), spawns the command
/// directly with `current_dir` set to `working_dir` and env vars applied
/// via `.envs()`.
///
/// For containerized apps (`container_spawn` is Some), wraps the command
/// in `podman run ...` using the container's mounts, home dir, and working
/// directory. The `working_dir` parameter is unused in this case — the
/// container's working directory is set by `base_podman_args()` (the `-w`
/// flag). Env vars are injected as `-e KEY=VAL` podman flags.
///
/// `extra_podman_flags` (e.g., `["-i"]` for stdin passthrough) are inserted
/// before the image name; ignored for bare-process apps.
///
/// Returns a configured but **unspawned** `Command`. Callers set up stdio
/// and spawn it themselves.
pub fn run_in_app_env(
    command: &str,
    args: &[&str],
    working_dir: &Path,
    container_spawn: Option<&ContainerSpawnConfig>,
    env: &[(&str, &str)],
    extra_podman_flags: &[&str],
) -> tokio::process::Command {
    if let Some(spawn) = container_spawn {
        let mut podman_args = spawn.base_podman_args();

        // Env vars become -e KEY=VAL flags, inserted before the image name.
        if !env.is_empty() {
            let env_flags: Vec<String> = env
                .iter()
                .flat_map(|(k, v)| ["-e".to_string(), format!("{k}={v}")])
                .collect();
            ContainerSpawnConfig::insert_podman_flags(&mut podman_args, &env_flags);
        }

        // Extra podman flags (e.g., -i for stdin passthrough).
        if !extra_podman_flags.is_empty() {
            let flags: Vec<String> = extra_podman_flags.iter().map(|s| s.to_string()).collect();
            ContainerSpawnConfig::insert_podman_flags(&mut podman_args, &flags);
        }

        // Append the actual command + args after the image name.
        podman_args.push(command.to_string());
        podman_args.extend(args.iter().map(|s| s.to_string()));

        let mut cmd = tokio::process::Command::new("podman");
        cmd.args(&podman_args);
        cmd
    } else {
        let mut cmd = tokio::process::Command::new(command);
        cmd.args(args);
        cmd.current_dir(working_dir);
        cmd.envs(env.iter().copied());
        cmd
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use tokio::io::BufReader;

    use super::*;

    /// At-cap: stream has exactly `cap` bytes — result is `cap` bytes, no overflow.
    #[tokio::test]
    async fn drain_stream_at_cap_returns_cap_bytes() {
        let cap: usize = 8;
        let data = vec![0u8; cap];
        let reader = BufReader::new(Cursor::new(data));
        let result = drain_stream(reader, cap).await.unwrap();
        assert_eq!(
            result.len(),
            cap,
            "at-cap: expected {cap} bytes, got {}",
            result.len()
        );
        // len == cap: no overflow
        assert!(result.len() <= cap, "at-cap result should not exceed cap");
    }

    /// Over-cap: stream has `cap + 5` bytes — result is `cap + 1` bytes, signalling overflow.
    #[tokio::test]
    async fn drain_stream_over_cap_returns_cap_plus_one_bytes() {
        let cap: usize = 8;
        let data = vec![0u8; cap + 5];
        let reader = BufReader::new(Cursor::new(data));
        let result = drain_stream(reader, cap).await.unwrap();
        assert_eq!(
            result.len(),
            cap + 1,
            "over-cap: expected {} bytes (sentinel), got {}",
            cap + 1,
            result.len()
        );
        // len > cap: overflow detected by caller
        assert!(
            result.len() > cap,
            "over-cap result must exceed cap so caller can detect overflow"
        );
    }

    /// Bare-process path: command is the program, args are passed through.
    #[test]
    fn bare_process_builds_direct_command() {
        let cmd = run_in_app_env(
            "graf",
            &["todo", "--json"],
            Path::new("/tmp"),
            None,
            &[],
            &[],
        );
        let cmd_std = cmd.as_std();
        assert_eq!(cmd_std.get_program(), "graf");
        let args: Vec<&std::ffi::OsStr> = cmd_std.get_args().collect();
        assert_eq!(args, vec!["todo", "--json"]);
        assert_eq!(cmd_std.get_current_dir(), Some(Path::new("/tmp")));
    }

    /// Bare-process path with env vars.
    #[test]
    fn bare_process_with_env() {
        let cmd = run_in_app_env(
            "pf",
            &["--json", "reconcile"],
            Path::new("/data"),
            None,
            &[("PFIN_DB", "/data/pfin.db")],
            &[],
        );
        let cmd_std = cmd.as_std();
        assert_eq!(cmd_std.get_program(), "pf");
        let envs: Vec<_> = cmd_std.get_envs().collect();
        assert!(
            envs.iter()
                .any(|(k, v)| *k == "PFIN_DB" && v == &Some(std::ffi::OsStr::new("/data/pfin.db"))),
            "expected PFIN_DB env var, got {envs:?}"
        );
    }

    /// Extra podman flags are ignored for bare-process commands.
    #[test]
    fn bare_process_ignores_extra_podman_flags() {
        let cmd = run_in_app_env(
            "graf",
            &["todo", "--json"],
            Path::new("/tmp"),
            None,
            &[],
            &["-i"],
        );
        let cmd_std = cmd.as_std();
        assert_eq!(cmd_std.get_program(), "graf");
        let args: Vec<&std::ffi::OsStr> = cmd_std.get_args().collect();
        // -i should NOT appear in args for bare-process.
        assert_eq!(args, vec!["todo", "--json"]);
    }

    /// Containerized path: command is podman, with the app command appended
    /// after the image name.
    #[test]
    fn containerized_builds_podman_command() {
        let spawn = ContainerSpawnConfig {
            image: "brenn-cc:latest".to_string(),
            home_dir: "/host/home".into(),
            container_home: "/home/user".into(),
            host_working_dir: "/host/work".into(),
            container_working_dir: "/container/work".into(),
            working_dir_is_repo: false,
            repo_mounts: vec![],
            extra_mounts: vec![],
            extra_args: vec![],
        };
        let cmd = run_in_app_env(
            "graf",
            &["todo", "--json"],
            Path::new("/ignored"),
            Some(&spawn),
            &[],
            &[],
        );
        let cmd_std = cmd.as_std();
        assert_eq!(cmd_std.get_program(), "podman");
        let args: Vec<String> = cmd_std
            .get_args()
            .map(|a| a.to_string_lossy().to_string())
            .collect();
        // Image should be present, followed by the command + args.
        let image_pos = args.iter().position(|a| a == "brenn-cc:latest").unwrap();
        assert_eq!(args[image_pos + 1], "graf");
        assert_eq!(args[image_pos + 2], "todo");
        assert_eq!(args[image_pos + 3], "--json");
    }

    /// Containerized path with env vars — they become -e flags before the image.
    #[test]
    fn containerized_with_env() {
        let spawn = ContainerSpawnConfig {
            image: "brenn-cc:latest".to_string(),
            home_dir: "/host/home".into(),
            container_home: "/home/user".into(),
            host_working_dir: "/host/work".into(),
            container_working_dir: "/container/work".into(),
            working_dir_is_repo: false,
            repo_mounts: vec![],
            extra_mounts: vec![],
            extra_args: vec![],
        };
        let cmd = run_in_app_env(
            "pf",
            &["--json", "show"],
            Path::new("/ignored"),
            Some(&spawn),
            &[("PFIN_DB", "/data/pfin.db")],
            &[],
        );
        let args: Vec<String> = cmd
            .as_std()
            .get_args()
            .map(|a| a.to_string_lossy().to_string())
            .collect();
        let image_pos = args.iter().position(|a| a == "brenn-cc:latest").unwrap();
        // -e PFIN_DB=/data/pfin.db must appear before the image.
        let e_pos = args
            .iter()
            .position(|a| a == "PFIN_DB=/data/pfin.db")
            .unwrap();
        assert!(e_pos < image_pos, "-e env flag should be before image name");
        assert_eq!(args[e_pos - 1], "-e");
    }

    /// Containerized path with extra podman flags.
    #[test]
    fn containerized_with_extra_flags() {
        let spawn = ContainerSpawnConfig {
            image: "brenn-cc:latest".to_string(),
            home_dir: "/host/home".into(),
            container_home: "/home/user".into(),
            host_working_dir: "/host/work".into(),
            container_working_dir: "/container/work".into(),
            working_dir_is_repo: false,
            repo_mounts: vec![],
            extra_mounts: vec![],
            extra_args: vec![],
        };
        let cmd = run_in_app_env(
            "pf",
            &["--json", "reconcile"],
            Path::new("/ignored"),
            Some(&spawn),
            &[],
            &["-i"],
        );
        let args: Vec<String> = cmd
            .as_std()
            .get_args()
            .map(|a| a.to_string_lossy().to_string())
            .collect();
        let image_pos = args.iter().position(|a| a == "brenn-cc:latest").unwrap();
        // -i must appear before the image name.
        let i_pos = args.iter().position(|a| a == "-i").unwrap();
        assert!(i_pos < image_pos, "-i should be before image name");
    }

    /// Containerized path does NOT set current_dir — the container's -w flag
    /// (from base_podman_args) handles the working directory.
    #[test]
    fn containerized_does_not_set_current_dir() {
        let spawn = ContainerSpawnConfig {
            image: "brenn-cc:latest".to_string(),
            home_dir: "/host/home".into(),
            container_home: "/home/user".into(),
            host_working_dir: "/host/work".into(),
            container_working_dir: "/container/work".into(),
            working_dir_is_repo: false,
            repo_mounts: vec![],
            extra_mounts: vec![],
            extra_args: vec![],
        };
        let cmd = run_in_app_env(
            "graf",
            &["todo", "--json"],
            Path::new("/should/be/ignored"),
            Some(&spawn),
            &[],
            &[],
        );
        // The podman process itself should not have current_dir set —
        // the container's working directory is set via the -w flag.
        assert!(
            cmd.as_std().get_current_dir().is_none(),
            "containerized command should not set current_dir on the podman process"
        );
    }

    /// Containerized path with both env vars AND extra flags — both should
    /// appear before the image name, and the command after it.
    #[test]
    fn containerized_with_env_and_extra_flags() {
        let spawn = ContainerSpawnConfig {
            image: "brenn-cc:latest".to_string(),
            home_dir: "/host/home".into(),
            container_home: "/home/user".into(),
            host_working_dir: "/host/work".into(),
            container_working_dir: "/container/work".into(),
            working_dir_is_repo: false,
            repo_mounts: vec![],
            extra_mounts: vec![],
            extra_args: vec![],
        };
        let cmd = run_in_app_env(
            "pf",
            &["--json", "reconcile", "--user", "alice"],
            Path::new("/ignored"),
            Some(&spawn),
            &[("PFIN_DB", "/data/pfin.db")],
            &["-i"],
        );
        let args: Vec<String> = cmd
            .as_std()
            .get_args()
            .map(|a| a.to_string_lossy().to_string())
            .collect();
        let image_pos = args.iter().position(|a| a == "brenn-cc:latest").unwrap();

        // -e flag and -i flag both before image.
        let e_pos = args
            .iter()
            .position(|a| a == "PFIN_DB=/data/pfin.db")
            .unwrap();
        let i_pos = args.iter().position(|a| a == "-i").unwrap();
        assert!(e_pos < image_pos, "-e env flag should be before image");
        assert!(i_pos < image_pos, "-i flag should be before image");

        // Command and args after image.
        assert_eq!(args[image_pos + 1], "pf");
        assert_eq!(args[image_pos + 2], "--json");
        assert_eq!(args[image_pos + 3], "reconcile");
        assert_eq!(args[image_pos + 4], "--user");
        assert_eq!(args[image_pos + 5], "alice");
    }
}
