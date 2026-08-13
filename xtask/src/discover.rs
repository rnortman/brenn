// The wasm32 guest workspace's members, and which generator owns each one's WIT
// bindings. The world-equivalence gate dispatches on that provenance.

use std::path::{Path, PathBuf};

/// The wasm32 guest workspace, repo-root-relative. Stated rather than found by
/// a tree walk: it is the only standalone workspace in the tree, and the crates
/// it lists are the only ones that produce a component artifact.
pub const WASM_WORKSPACE: &str = "brenn-wasm/components";

/// Which generator owns a wasm crate's WIT bindings — the per-crate WIT
/// provenance the check-wit gates dispatch on.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Family {
    /// `bindings.rs` generated from the pinned wit-bindgen CLI; names its WIT
    /// file in `[package.metadata.component]`.
    Raw,
    /// Bindings come from the brenn-guest SDK's `generate!` invocation.
    Guest,
    /// The brenn-guest SDK itself. Builds no component artifact.
    Sdk,
}

/// A member crate of the wasm workspace.
#[derive(Debug, Clone)]
pub struct WasmCrate {
    /// Absolute path to the crate directory.
    pub dir: PathBuf,
    pub family: Family,
}

/// Every member crate of the wasm workspace, with its family.
///
/// The member list comes from the workspace manifest, so a crate that exists on
/// disk but is not a member is invisible here — and invisible to the build,
/// which resolves the same manifest.
pub fn wasm_crates(repo_root: &Path) -> Vec<WasmCrate> {
    let workspace_dir = repo_root.join(WASM_WORKSPACE);
    let manifest = read_manifest(&workspace_dir.join("Cargo.toml"));
    let members = collect_workspace_members(&manifest);
    assert!(
        !members.is_empty(),
        "discover: {WASM_WORKSPACE}/Cargo.toml lists no [workspace] members — \
         a gate iterating an empty set passes exactly as a healthy one does."
    );
    members
        .into_iter()
        .map(|member| {
            let dir = workspace_dir.join(member);
            let manifest = read_manifest(&dir.join("Cargo.toml"));
            WasmCrate {
                family: classify_family(&manifest, &dir),
                dir,
            }
        })
        .collect()
}

/// Classify one wasm workspace member into a `Family`. Panics on unclassifiable.
fn classify_family(manifest: &toml::Value, crate_dir: &Path) -> Family {
    let package = manifest.get("package");
    let pkg_name = package.and_then(|p| p.get("name")).and_then(|n| n.as_str());
    if pkg_name == Some("brenn-guest") {
        return Family::Sdk;
    }
    if package
        .and_then(|p| p.get("metadata"))
        .and_then(|m| m.get("component"))
        .is_some()
    {
        return Family::Raw;
    }
    let has_guest_dep = manifest
        .get("dependencies")
        .and_then(|d| d.as_table())
        .is_some_and(|deps| deps.contains_key("brenn-guest"));
    if has_guest_dep {
        return Family::Guest;
    }
    panic!(
        "discover: unclassifiable wasm workspace member at {crate_dir:?}.\n\
         Does not match any known family:\n\
         - Sdk (package.name == \"brenn-guest\"): no\n\
         - Raw ([package.metadata.component]): no\n\
         - Guest (brenn-guest in [dependencies]): no\n\
         Add a classification rule in xtask/src/discover.rs or re-examine this crate's structure."
    );
}

/// Read and parse a Cargo.toml. Panics on unreadable or malformed input.
fn read_manifest(path: &Path) -> toml::Value {
    let content =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("Failed to read {path:?}: {e}"));
    toml::from_str(&content).unwrap_or_else(|e| panic!("Failed to parse {path:?}: {e}"))
}

/// The member paths a workspace manifest lists, relative to the workspace root.
fn collect_workspace_members(manifest: &toml::Value) -> Vec<PathBuf> {
    manifest
        .get("workspace")
        .and_then(|w| w.get("members"))
        .and_then(|m| m.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str())
                .map(PathBuf::from)
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The repo root, from CARGO_MANIFEST_DIR (xtask/).
    ///
    /// Under a build system that hands the variable over workspace-relative,
    /// xtask/'s parent is the empty path; the tree is then rooted at the
    /// working directory the test starts in.
    fn repo_root() -> PathBuf {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        match manifest_dir.parent() {
            Some(parent) if parent.as_os_str().is_empty() => PathBuf::from("."),
            Some(parent) => parent.to_path_buf(),
            None => panic!("xtask/ has no parent"),
        }
    }

    /// Every wasm workspace member, by family. The family is the sole dispatch
    /// key of which WIT world a component's artifact is compared against, so a
    /// member that silently changed family would be compared to the wrong
    /// world, green.
    #[test]
    fn known_wasm_crate_families() {
        let repo_root = repo_root();
        let components = repo_root.join(WASM_WORKSPACE);

        let mut observed: Vec<(String, Family)> = wasm_crates(&repo_root)
            .into_iter()
            .map(|c| {
                let rel = c
                    .dir
                    .strip_prefix(&components)
                    .unwrap_or_else(|_| panic!("{:?} is not under {components:?}", c.dir));
                (rel.to_string_lossy().into_owned(), c.family)
            })
            .collect();
        observed.sort();

        let mut expected: Vec<(String, Family)> = [
            ("guest", Family::Sdk),
            ("processor-exhaust", Family::Raw),
            ("processor-mem-exhaust", Family::Raw),
            ("processor-mqtt-test", Family::Raw),
            ("processor-tool-test", Family::Raw),
            ("replay", Family::Raw),
            ("replay-fault-test", Family::Raw),
            ("replay-generic", Family::Raw),
            ("git-forge-parser", Family::Guest),
            ("git-sync-consumer", Family::Guest),
            ("processor-config", Family::Guest),
            ("processor-demo", Family::Guest),
            ("processor-dual", Family::Guest),
            ("processor-log", Family::Guest),
            ("processor-multiport", Family::Guest),
            ("processor-store-rt", Family::Guest),
            ("processor-transplant", Family::Guest),
        ]
        .into_iter()
        .map(|(name, family)| (name.to_string(), family))
        .collect();
        expected.sort();

        assert_eq!(observed, expected);
    }

    fn manifest(text: &str) -> toml::Value {
        toml::from_str(text).expect("fixture manifest parses")
    }

    #[test]
    fn each_family_arm_is_recognized() {
        let dir = Path::new("brenn-wasm/components/x");
        assert_eq!(
            classify_family(&manifest("[package]\nname = \"brenn-guest\"\n"), dir),
            Family::Sdk
        );
        assert_eq!(
            classify_family(
                &manifest(
                    "[package]\nname = \"replay\"\n\n[package.metadata.component]\n\
                     target = { path = \"../../wit/replay.wit\" }\n"
                ),
                dir
            ),
            Family::Raw
        );
        assert_eq!(
            classify_family(
                &manifest(
                    "[package]\nname = \"demo\"\n\n[dependencies]\n\
                     brenn-guest = { path = \"../guest\" }\n"
                ),
                dir
            ),
            Family::Guest
        );
    }

    /// The SDK arm wins over the raw arm: `brenn-guest` itself would otherwise
    /// have to be kept free of component metadata forever.
    #[test]
    fn the_sdk_arm_is_checked_before_the_raw_arm() {
        assert_eq!(
            classify_family(
                &manifest(
                    "[package]\nname = \"brenn-guest\"\n\n[package.metadata.component]\n\
                     target = { path = \"x.wit\" }\n"
                ),
                Path::new("brenn-wasm/components/guest")
            ),
            Family::Sdk
        );
    }

    #[test]
    #[should_panic(expected = "unclassifiable wasm workspace member")]
    fn a_member_matching_no_family_panics() {
        classify_family(
            &manifest("[package]\nname = \"stray\"\n\n[dependencies]\nserde = \"1\"\n"),
            Path::new("brenn-wasm/components/stray"),
        );
    }

    /// A memberless workspace manifest is a discovery walk gone stale, not an
    /// empty tree: every gate downstream would iterate nothing and pass.
    #[test]
    #[should_panic(expected = "lists no [workspace] members")]
    fn a_memberless_workspace_panics() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join(WASM_WORKSPACE);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("Cargo.toml"),
            "[workspace]\nmembers = []\nresolver = \"3\"\n",
        )
        .unwrap();

        wasm_crates(tmp.path());
    }
}
