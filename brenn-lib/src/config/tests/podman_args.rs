use super::*;

// -----------------------------------------------------------------------
// base_podman_args: repo mounts and working_dir_is_repo
// -----------------------------------------------------------------------

#[test]
fn base_podman_args_includes_repo_mounts() {
    let spawn = ContainerSpawnConfig {
        image: "test:latest".to_string(),
        home_dir: PathBuf::from("/host/home"),
        container_home: PathBuf::from("/home/user"),
        host_working_dir: PathBuf::from("/repos/life"),
        container_working_dir: PathBuf::from("/home/user/repos/life"),
        working_dir_is_repo: true,
        repo_mounts: vec![
            RepoBindMount {
                host_path: PathBuf::from("/repos/life"),
                container_path: PathBuf::from("/home/user/repos/life"),
                read_only: false,
            },
            RepoBindMount {
                host_path: PathBuf::from("/repos/docs"),
                container_path: PathBuf::from("/home/user/repos/docs"),
                read_only: true,
            },
        ],
        extra_mounts: vec![],
        extra_args: vec![],
    };

    let args = spawn.base_podman_args();
    let args_str = args.join(" ");

    // Read-write repo mount should have :z (not :ro).
    assert!(
        args_str.contains("/repos/life:/home/user/repos/life:z"),
        "expected rw repo mount in args: {args_str}",
    );

    // Read-only repo mount should have :ro,z.
    assert!(
        args_str.contains("/repos/docs:/home/user/repos/docs:ro,z"),
        "expected ro repo mount in args: {args_str}",
    );
}

#[test]
fn base_podman_args_skips_working_dir_mount_when_repo() {
    let spawn = ContainerSpawnConfig {
        image: "test:latest".to_string(),
        home_dir: PathBuf::from("/host/home"),
        container_home: PathBuf::from("/home/user"),
        host_working_dir: PathBuf::from("/repos/life"),
        container_working_dir: PathBuf::from("/home/user/repos/life"),
        working_dir_is_repo: true,
        repo_mounts: vec![RepoBindMount {
            host_path: PathBuf::from("/repos/life"),
            container_path: PathBuf::from("/home/user/repos/life"),
            read_only: false,
        }],
        extra_mounts: vec![],
        extra_args: vec![],
    };

    let args = spawn.base_podman_args();

    // Count -v flags — should be: home mount + 1 repo mount = 2.
    // No separate working dir mount (working_dir_is_repo = true).
    let v_count = args.iter().filter(|a| *a == "-v").count();
    assert_eq!(
        v_count, 2,
        "expected 2 -v flags (home + repo), got {v_count}. args: {args:?}",
    );

    // But -w should still point to the container working dir.
    let w_pos = args.iter().position(|a| a == "-w").unwrap();
    assert_eq!(args[w_pos + 1], "/home/user/repos/life");
}

#[test]
fn base_podman_args_includes_working_dir_mount_when_not_repo() {
    let spawn = ContainerSpawnConfig {
        image: "test:latest".to_string(),
        home_dir: PathBuf::from("/host/home"),
        container_home: PathBuf::from("/home/user"),
        host_working_dir: PathBuf::from("/host/work"),
        container_working_dir: PathBuf::from("/container/work"),
        working_dir_is_repo: false,
        repo_mounts: vec![],
        extra_mounts: vec![],
        extra_args: vec![],
    };

    let args = spawn.base_podman_args();

    // Should have: home mount + working dir mount = 2 -v flags.
    let v_count = args.iter().filter(|a| *a == "-v").count();
    assert_eq!(
        v_count, 2,
        "expected 2 -v flags (home + workdir), got {v_count}. args: {args:?}",
    );

    let args_str = args.join(" ");
    assert!(
        args_str.contains("/host/work:/container/work:z"),
        "expected working dir mount when not a repo: {args_str}",
    );
}
