use std::path::PathBuf;

use serde::Deserialize;

/// Podman container definition, referenced by apps via `container = "<name>"`.
///
/// Multiple apps can share a container definition (at different working dirs).
/// Each CC session gets its own ephemeral container instance (`--rm`).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContainerConfig {
    /// Podman image name/tag (e.g. "brenn-cc:latest").
    pub image: String,
    /// Host path bind-mounted as the container's home directory.
    /// This is where `.claude/`, `.ssh/`, etc. live persistently.
    pub home_dir: PathBuf,
    /// Container-side home directory path. Default: `/home/user`.
    /// Set via `-e HOME=` to ensure CC finds credentials regardless of image user config.
    #[serde(default = "default_container_home")]
    pub container_home: PathBuf,
    /// Additional bind mounts as `["host:container", ...]`.
    /// These are opaque to Brenn's path translation — CC-reported absolute paths
    /// within extra_mounts will not be resolvable by the host for artifact/file serving.
    #[serde(default)]
    pub extra_mounts: Vec<String>,
    /// Extra `podman run` arguments for things we haven't anticipated.
    #[serde(default)]
    pub extra_args: Vec<String>,
}

fn default_container_home() -> PathBuf {
    PathBuf::from("/home/user")
}

/// Resolved container spawn configuration for a containerized app.
/// Passed through to `CcSessionConfig` when spawning.
#[derive(Debug, Clone)]
pub struct ContainerSpawnConfig {
    /// Podman image name/tag.
    pub image: String,
    /// Host path for the persistent home directory.
    pub home_dir: PathBuf,
    /// Container-side home directory path (set as HOME env var).
    pub container_home: PathBuf,
    /// Host-side working directory (bind-mounted into container).
    pub host_working_dir: PathBuf,
    /// Container-side working directory (CC's cwd).
    pub container_working_dir: PathBuf,
    /// Whether the working directory comes from a repo mount.
    /// When true, the working dir `-v` mount is skipped (the repo mount covers it).
    pub working_dir_is_repo: bool,
    /// Repo bind mounts generated from app.mount declarations.
    pub repo_mounts: Vec<RepoBindMount>,
    /// Additional bind mounts (opaque `host:container` strings).
    pub extra_mounts: Vec<String>,
    /// Extra podman run arguments.
    pub extra_args: Vec<String>,
}

/// A bind mount for a repo in a container.
#[derive(Debug, Clone)]
pub struct RepoBindMount {
    pub host_path: PathBuf,
    pub container_path: PathBuf,
    pub read_only: bool,
}

impl ContainerSpawnConfig {
    /// Build the common `podman run` arguments shared by all container invocations
    /// (CC spawn, container hooks, auto_pull). Returns args from `run` through the
    /// image name — callers append the command to execute.
    ///
    /// Includes: `run --rm --network=host -e HOME=... -v home -v work`
    /// `extra_mounts extra_args -w workdir image`.
    ///
    /// Does NOT include caller-specific flags like `-i`, `--name`, or extra env
    /// vars — callers add those via [`Self::insert_podman_flags`].
    pub fn base_podman_args(&self) -> Vec<String> {
        let mut args: Vec<String> = vec![
            "run".into(),
            "--rm".into(),
            "--network=host".into(),
            "-e".into(),
            format!("HOME={}", self.container_home.display()),
            "-v".into(),
            format!(
                "{}:{}:z",
                self.home_dir.display(),
                self.container_home.display(),
            ),
        ];

        // Working dir mount — only when working dir is NOT a repo (repo mount covers it).
        if !self.working_dir_is_repo {
            args.push("-v".into());
            args.push(format!(
                "{}:{}:z",
                self.host_working_dir.display(),
                self.container_working_dir.display(),
            ));
        }

        // Repo bind mounts.
        for repo_mount in &self.repo_mounts {
            args.push("-v".into());
            let ro_suffix = if repo_mount.read_only { ":ro,z" } else { ":z" };
            args.push(format!(
                "{}:{}{}",
                repo_mount.host_path.display(),
                repo_mount.container_path.display(),
                ro_suffix,
            ));
        }

        // Extra mounts (opaque host:container strings, unchanged).
        for mount in &self.extra_mounts {
            args.push("-v".into());
            args.push(mount.clone());
        }

        // Extra podman args (escape hatch for --userns, resource limits, etc.).
        // These are container-level settings that apply to all invocations.
        args.extend(self.extra_args.iter().cloned());

        args.push("-w".into());
        args.push(self.container_working_dir.display().to_string());

        // Image is always the last element — callers append their command after this.
        args.push(self.image.clone());

        args
    }

    /// Insert additional podman flags before the image name (the last element
    /// of a `base_podman_args()` result). Use this for caller-specific flags
    /// like `-i`, `--name`, or `-e KEY=VAL`.
    pub fn insert_podman_flags(args: &mut Vec<String>, flags: &[String]) {
        let image_pos = args.len() - 1;
        for (i, flag) in flags.iter().enumerate() {
            args.insert(image_pos + i, flag.clone());
        }
    }
}
