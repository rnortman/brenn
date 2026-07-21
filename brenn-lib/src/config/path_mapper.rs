use std::path::{Path, PathBuf};

/// Maps between host filesystem paths and CC-visible paths.
/// For bare-process apps, this is an identity mapping.
#[derive(Debug, Clone)]
pub enum PathMapper {
    /// No container — host paths and CC paths are identical.
    Identity,
    /// Containerized — multiple mappings checked in order (most-specific first).
    /// Each mapping translates between a host path prefix and a container path prefix.
    Container {
        /// Checked in order; first match wins.
        /// More-specific prefixes come first (repo mounts before home dir).
        /// Private: construct via `PathMapper::container()` which enforces the
        /// ordering invariant. Direct struct-literal construction is forbidden
        /// so the invariant cannot be bypassed.
        mappings: Vec<PathMapping>,
    },
}

impl PathMapper {
    /// Construct a `Container` variant, asserting that `mappings` is ordered
    /// from most-specific to least-specific (descending component count for both
    /// `container_root` and `host_root`). A violated ordering would silently
    /// misroute paths in `to_host` / `to_container` (first-match-wins semantics).
    ///
    /// The check runs in all build profiles because a misordered mapping in
    /// release would silently misroute host↔container translations, including
    /// those used by `ExportUsage` sandbox enforcement.
    pub fn container(mappings: Vec<PathMapping>) -> Self {
        assert!(
            mappings.windows(2).all(|w| {
                w[0].container_root.components().count() >= w[1].container_root.components().count()
                    && w[0].host_root.components().count() >= w[1].host_root.components().count()
            }),
            "PathMapper::Container mappings must be ordered most-specific first \
             (descending component count for both container_root and host_root); \
             got: {mappings:?}",
        );
        Self::Container { mappings }
    }
}

/// A single host ↔ container path prefix mapping.
#[derive(Debug, Clone)]
pub struct PathMapping {
    pub host_root: PathBuf,
    pub container_root: PathBuf,
}

impl PathMapper {
    /// Translate a CC-reported path to a host path.
    /// Returns None if the path is outside all mapped roots.
    pub fn to_host(&self, cc_path: &Path) -> Option<PathBuf> {
        match self {
            Self::Identity => Some(cc_path.to_owned()),
            Self::Container { mappings } => {
                for mapping in mappings {
                    if let Ok(relative) = cc_path.strip_prefix(&mapping.container_root) {
                        return Some(mapping.host_root.join(relative));
                    }
                }
                None
            }
        }
    }

    /// Translate a host path to a CC-visible path.
    /// Returns None if the path is outside all mapped roots.
    pub fn to_container(&self, host_path: &Path) -> Option<PathBuf> {
        match self {
            Self::Identity => Some(host_path.to_owned()),
            Self::Container { mappings } => {
                for mapping in mappings {
                    if let Ok(relative) = host_path.strip_prefix(&mapping.host_root) {
                        return Some(mapping.container_root.join(relative));
                    }
                }
                None
            }
        }
    }
}
