use super::*;

// -----------------------------------------------------------------------
// PathMapper unit tests
// -----------------------------------------------------------------------

#[test]
fn path_mapper_identity_passes_through() {
    let mapper = PathMapper::Identity;
    let path = Path::new("/some/path/file.md");
    assert_eq!(mapper.to_host(path), Some(path.to_owned()));
    assert_eq!(mapper.to_container(path), Some(path.to_owned()));
}

#[test]
fn path_mapper_container_translates() {
    let mapper = PathMapper::container(vec![PathMapping {
        host_root: PathBuf::from("/home/user/src/pfin"),
        container_root: PathBuf::from("/workspace/pfin"),
    }]);

    // to_host
    assert_eq!(
        mapper.to_host(Path::new("/workspace/pfin/docs/README.md")),
        Some(PathBuf::from("/home/user/src/pfin/docs/README.md")),
    );

    // to_container
    assert_eq!(
        mapper.to_container(Path::new("/home/user/src/pfin/docs/README.md")),
        Some(PathBuf::from("/workspace/pfin/docs/README.md")),
    );
}

#[test]
fn path_mapper_container_rejects_outside_paths() {
    let mapper = PathMapper::container(vec![PathMapping {
        host_root: PathBuf::from("/home/user/src/pfin"),
        container_root: PathBuf::from("/workspace/pfin"),
    }]);

    // Path outside container root
    assert_eq!(mapper.to_host(Path::new("/other/path/file.md")), None);

    // Path outside host root
    assert_eq!(mapper.to_container(Path::new("/other/path/file.md")), None,);
}

// -----------------------------------------------------------------------
// PathMapper multi-root tests
// -----------------------------------------------------------------------

#[test]
fn path_mapper_multi_root_to_host_first_match_wins() {
    let mapper = PathMapper::container(vec![
        PathMapping {
            host_root: PathBuf::from("/repos/life"),
            container_root: PathBuf::from("/home/user/repos/life"),
        },
        PathMapping {
            host_root: PathBuf::from("/host/home"),
            container_root: PathBuf::from("/home/user"),
        },
    ]);

    // A path under the repo mount should match the repo mapping (first),
    // not the home mapping (which also matches /home/user/*).
    assert_eq!(
        mapper.to_host(Path::new("/home/user/repos/life/doc.md")),
        Some(PathBuf::from("/repos/life/doc.md")),
    );

    // A path under home but not under any repo should match the home mapping.
    assert_eq!(
        mapper.to_host(Path::new("/home/user/.config/something")),
        Some(PathBuf::from("/host/home/.config/something")),
    );
}

#[test]
fn path_mapper_multi_root_to_container_first_match_wins() {
    let mapper = PathMapper::container(vec![
        PathMapping {
            host_root: PathBuf::from("/repos/life"),
            container_root: PathBuf::from("/home/user/repos/life"),
        },
        PathMapping {
            host_root: PathBuf::from("/host/home"),
            container_root: PathBuf::from("/home/user"),
        },
    ]);

    // Host repo path should map to container repo path.
    assert_eq!(
        mapper.to_container(Path::new("/repos/life/doc.md")),
        Some(PathBuf::from("/home/user/repos/life/doc.md")),
    );

    // Host home path should map to container home path.
    assert_eq!(
        mapper.to_container(Path::new("/host/home/.ssh/config")),
        Some(PathBuf::from("/home/user/.ssh/config")),
    );
}

#[test]
#[should_panic(expected = "ordered most-specific first")]
fn path_mapper_container_panics_on_wrong_container_order() {
    // Most-specific container_root must come first; reversed order must panic.
    PathMapper::container(vec![
        PathMapping {
            host_root: PathBuf::from("/host/home"),
            container_root: PathBuf::from("/home/user"),
        },
        PathMapping {
            host_root: PathBuf::from("/repos/life"),
            container_root: PathBuf::from("/home/user/repos/life"),
        },
    ]);
}

#[test]
#[should_panic(expected = "ordered most-specific first")]
fn path_mapper_container_panics_on_wrong_host_order() {
    // Most-specific host_root must also come first; reversed host order must panic.
    PathMapper::container(vec![
        PathMapping {
            host_root: PathBuf::from("/host/home"),
            container_root: PathBuf::from("/home/user/repos/life"),
        },
        PathMapping {
            host_root: PathBuf::from("/repos/life/subdir"),
            container_root: PathBuf::from("/home/user"),
        },
    ]);
}

#[test]
fn path_mapper_multi_root_rejects_outside_all_mappings() {
    let mapper = PathMapper::container(vec![
        PathMapping {
            host_root: PathBuf::from("/repos/life"),
            container_root: PathBuf::from("/home/user/repos/life"),
        },
        PathMapping {
            host_root: PathBuf::from("/host/home"),
            container_root: PathBuf::from("/home/user"),
        },
    ]);

    assert_eq!(mapper.to_host(Path::new("/completely/unrelated")), None);
    assert_eq!(
        mapper.to_container(Path::new("/completely/unrelated")),
        None
    );
}
