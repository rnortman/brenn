//! Artifact viewer: read and render markdown files from the CC working directory
//! and any of the app's declared repo mounts.
//!
//! Security boundary: all file access is validated against an explicit list of
//! allowed roots — the conversation's `cwd` plus any non-working-dir
//! `[[app.mount]]` repos. Symlinks pointing outside any allowed root are
//! rejected. Currently only CC-originated and browser-`ReopenArtifact` paths
//! reach this code; the latter routes `PathTraversal` errors through
//! `log_and_alert_security_event` for fail2ban.

use std::fmt;
use std::path::{Path, PathBuf};

use tracing::warn;

/// Maximum file size for artifact display (1 MB).
const MAX_FILE_SIZE: u64 = 1024 * 1024;

/// A repo mount permitted as a source for artifact display.
///
/// Conceptually a slimmed-down `brenn_lib::config::ResolvedMount`; we keep a
/// dedicated type so `artifact.rs`'s public API is self-contained and tests
/// don't have to construct a full `ResolvedMount`.
#[derive(Debug, Clone)]
pub struct MountRoot {
    /// Host-side path to the mount root.
    pub host_path: PathBuf,
    /// Mount slug, used as the leading display-path segment for files in
    /// this mount (e.g. `graf-life` → display `graf-life/kb/foo.md`).
    pub slug: String,
}

impl From<&brenn_lib::config::ResolvedMount> for MountRoot {
    fn from(m: &brenn_lib::config::ResolvedMount) -> Self {
        Self {
            host_path: m.host_path.clone(),
            slug: m.slug.clone(),
        }
    }
}

/// Build the `MountRoot` list for an app's resolved mounts, filtering out
/// the working-dir mount (already covered by `cwd`). Use this in every place
/// that constructs roots for `validate_artifact_path` /
/// `read_artifact_content` / `resolve_display_path`, so the working-dir
/// filter can never be forgotten.
pub fn mount_roots_for(mounts: &[brenn_lib::config::ResolvedMount]) -> Vec<MountRoot> {
    mounts
        .iter()
        .filter(|m| !m.is_working_dir)
        .map(MountRoot::from)
        .collect()
}

/// A successfully validated artifact path.
#[derive(Debug, Clone)]
pub struct ValidatedArtifact {
    /// Canonical absolute host path.
    pub canonical_path: PathBuf,
    /// Display path: bare relative path under `cwd`, or `<slug>/<rel>` under
    /// a mount. This is the string that's persisted in the DB and broadcast
    /// to the browser.
    pub display_path: String,
}

/// Errors from artifact path validation and file reading.
#[derive(Debug)]
pub enum ArtifactError {
    /// Path escapes all allowed roots. Logged as security event by callers
    /// where the path is browser-originated.
    /// `file_path` deliberately excluded from Display (don't leak attempted path to user).
    PathTraversal {
        #[allow(dead_code)]
        file_path: String,
    },
    /// Not a .md file.
    NotMarkdown { file_path: String },
    /// File not found.
    NotFound { file_path: String },
    /// No cwd set for this conversation. Emitted by callers (handlers) before
    /// they construct a `cwd: &Path` to pass to validation; not produced by
    /// `validate_artifact_path` itself.
    NoCwd,
    /// File too large.
    TooLarge { file_path: String, size_bytes: u64 },
    /// IO error reading the file.
    ReadError { file_path: String, detail: String },
}

impl fmt::Display for ArtifactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PathTraversal { file_path: _ } => {
                // Don't include the path in user-facing message — it may be a
                // malicious traversal attempt. The path is logged via tracing::warn
                // in validate_artifact_path.
                write!(f, "Access denied: file is outside the working directory.")
            }
            Self::NotMarkdown { file_path } => {
                write!(
                    f,
                    "Only .md files can be viewed. Got: {file_path}. Try: /view path/to/file.md"
                )
            }
            Self::NotFound { file_path } => {
                write!(
                    f,
                    "File not found: {file_path}. Check the file path and try again."
                )
            }
            Self::NoCwd => {
                write!(
                    f,
                    "No working directory set for this conversation. Start a conversation with Claude first."
                )
            }
            Self::TooLarge {
                file_path,
                size_bytes,
            } => {
                write!(
                    f,
                    "File is too large to display: {file_path} ({size_bytes} bytes, max 1MB)."
                )
            }
            Self::ReadError { file_path, detail } => {
                write!(f, "Could not read {file_path}: {detail}")
            }
        }
    }
}

/// Match a canonical path against a list of (slug, canonical_root) pairs.
/// Returns the matching (display_prefix, relative_path) on first hit. The
/// cwd entry has `display_prefix = None` (no slug prefix); mounts pass the
/// slug as `Some(...)`.
fn match_canonical_to_roots<'a>(
    canonical: &'a Path,
    cwd_canonical: &'a Path,
    mount_canonicals: &'a [(String, PathBuf)],
) -> Option<(Option<&'a str>, &'a Path)> {
    if let Ok(rel) = canonical.strip_prefix(cwd_canonical) {
        return Some((None, rel));
    }
    for (slug, mc) in mount_canonicals {
        if let Ok(rel) = canonical.strip_prefix(mc) {
            return Some((Some(slug.as_str()), rel));
        }
    }
    None
}

/// Build display_path from (display_prefix, relative_path).
fn build_display_path(prefix: Option<&str>, rel: &Path) -> String {
    let rel_str = rel.to_string_lossy();
    match prefix {
        None => rel_str.into_owned(),
        Some(slug) if rel_str.is_empty() => slug.to_string(),
        Some(slug) => format!("{slug}/{rel_str}"),
    }
}

/// Canonicalise every mount in one pass. Mounts that fail to canonicalise are
/// dropped (a missing/broken mount root just means files there aren't
/// reachable for this conversation; not a fatal error, but worth logging).
fn canonicalise_mounts(mounts: &[MountRoot]) -> Vec<(String, PathBuf)> {
    mounts
        .iter()
        .filter_map(|m| {
            m.host_path
                .canonicalize()
                .map_err(|e| warn!(slug = %m.slug, error = %e, "mount canonicalize failed — mount unreachable"))
                .ok()
                .map(|c| (m.slug.clone(), c))
        })
        .collect()
}

/// Validate that `file_path` is a `.md` file under one of the allowed roots
/// (cwd or any mount in `mounts`). Returns the canonical path plus the
/// display-path string to persist/show to the user.
///
/// Resolution rules:
///
/// - **Absolute** `file_path`: canonicalise; first matching root in
///   `[cwd] ++ mounts.host_paths` wins (cwd → unprefixed display path; mount
///   → `<slug>/<rest>`).
/// - **Relative** `file_path`: try cwd first (`cwd.join(file_path)`); only
///   if cwd doesn't have it AND the first segment matches a mount slug,
///   strip the slug and resolve the remainder against that mount's
///   `host_path`. This makes "relative paths resolve to cwd" literally true
///   for any path cwd can satisfy; the slug-prefix is a fallback that
///   activates only when cwd can't.
///
/// Slug-prefix collision precedence: cwd wins. A cwd directory named after
/// a mount slug shadows the mount in the relative-path branch.
pub fn validate_artifact_path(
    file_path: &str,
    cwd: &Path,
    mounts: &[MountRoot],
) -> Result<ValidatedArtifact, ArtifactError> {
    let path = Path::new(file_path);

    // Canonicalise cwd up-front. Any IO error here is fatal because cwd is
    // configured server-side; if it's broken, nothing about artifact serving
    // will work.
    let cwd_canonical = cwd.canonicalize().map_err(|e| ArtifactError::ReadError {
        file_path: file_path.to_string(),
        detail: format!("could not resolve working directory: {e}"),
    })?;

    let mount_canonicals = canonicalise_mounts(mounts);

    let validated = if path.is_absolute() {
        validate_absolute(path, file_path, &cwd_canonical, &mount_canonicals)?
    } else {
        validate_relative(
            path,
            file_path,
            cwd,
            &cwd_canonical,
            mounts,
            &mount_canonicals,
        )?
    };

    // Extension check (after root validation so we don't leak existence
    // information about wrong-extension files outside our roots).
    let ext = validated
        .canonical_path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");
    if ext != "md" {
        return Err(ArtifactError::NotMarkdown {
            file_path: file_path.to_string(),
        });
    }

    Ok(validated)
}

fn validate_absolute(
    path: &Path,
    file_path_str: &str,
    cwd_canonical: &Path,
    mount_canonicals: &[(String, PathBuf)],
) -> Result<ValidatedArtifact, ArtifactError> {
    // Canonicalise the absolute input once.
    let canonical = path.canonicalize().map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            ArtifactError::NotFound {
                file_path: file_path_str.to_string(),
            }
        } else {
            ArtifactError::ReadError {
                file_path: file_path_str.to_string(),
                detail: e.to_string(),
            }
        }
    })?;

    match match_canonical_to_roots(&canonical, cwd_canonical, mount_canonicals) {
        Some((prefix, rel)) => Ok(ValidatedArtifact {
            display_path: build_display_path(prefix, rel),
            canonical_path: canonical,
        }),
        None => {
            warn!(
                requested = file_path_str,
                resolved = %canonical.display(),
                cwd = %cwd_canonical.display(),
                mount_count = mount_canonicals.len(),
                "artifact path traversal attempt (absolute)"
            );
            Err(ArtifactError::PathTraversal {
                file_path: file_path_str.to_string(),
            })
        }
    }
}

fn validate_relative(
    path: &Path,
    file_path_str: &str,
    cwd: &Path,
    cwd_canonical: &Path,
    mounts: &[MountRoot],
    mount_canonicals: &[(String, PathBuf)],
) -> Result<ValidatedArtifact, ArtifactError> {
    // First try: resolve against cwd. The candidate may follow symlinks into
    // a mount, so we accept the result if it lands under any allowed root —
    // not just cwd. This makes "relative paths resolve to cwd" literally
    // true at the source-of-resolution level, while still allowing legit
    // symlinks-into-mount (and the rare `../mount/foo.md` form) to display.
    // The security invariant is "canonical lands under SOME allowed root",
    // not "the typed path was syntactically cwd-relative".
    let cwd_candidate = cwd.join(path);
    match cwd_candidate.canonicalize() {
        Ok(canonical) => {
            if let Some((prefix, rel)) =
                match_canonical_to_roots(&canonical, cwd_canonical, mount_canonicals)
            {
                return Ok(ValidatedArtifact {
                    display_path: build_display_path(prefix, rel),
                    canonical_path: canonical,
                });
            }
            // Cwd had a file at that name but it canonicalises outside all
            // allowed roots (e.g. symlink to /etc/passwd).
            warn!(
                requested = file_path_str,
                resolved = %canonical.display(),
                cwd = %cwd_canonical.display(),
                "artifact path traversal attempt (relative, cwd-join escaped)"
            );
            return Err(ArtifactError::PathTraversal {
                file_path: file_path_str.to_string(),
            });
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Fall through to slug-prefix fallback.
        }
        Err(e) => {
            return Err(ArtifactError::ReadError {
                file_path: file_path_str.to_string(),
                detail: e.to_string(),
            });
        }
    }

    // Slug-prefix fallback: if first segment matches a mount slug, resolve
    // the remainder against that mount.
    if let Some((first, rest)) = split_first_segment(path)
        && let Some(mount) = mounts.iter().find(|m| m.slug == first)
        && let Some((_, mount_canonical)) = mount_canonicals.iter().find(|(s, _)| s == &mount.slug)
    {
        let mount_candidate = mount.host_path.join(&rest);
        match mount_candidate.canonicalize() {
            Ok(canonical) => {
                if canonical.starts_with(mount_canonical) {
                    let rel = canonical
                        .strip_prefix(mount_canonical)
                        .expect("starts_with verified");
                    return Ok(ValidatedArtifact {
                        display_path: build_display_path(Some(&mount.slug), rel),
                        canonical_path: canonical,
                    });
                }
                warn!(
                    requested = file_path_str,
                    slug = %mount.slug,
                    resolved = %canonical.display(),
                    "artifact path traversal attempt (relative slug-prefixed)"
                );
                return Err(ArtifactError::PathTraversal {
                    file_path: file_path_str.to_string(),
                });
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Fall through to NotFound below.
            }
            Err(e) => {
                return Err(ArtifactError::ReadError {
                    file_path: file_path_str.to_string(),
                    detail: e.to_string(),
                });
            }
        }
    }

    Err(ArtifactError::NotFound {
        file_path: file_path_str.to_string(),
    })
}

/// Split a path into its first component and the rest. Returns `None` for
/// empty paths or paths whose first component isn't a normal segment (e.g.
/// `.` or `..`).
fn split_first_segment(path: &Path) -> Option<(&str, PathBuf)> {
    let mut components = path.components();
    let first = components.next()?;
    let first_str = match first {
        std::path::Component::Normal(s) => s.to_str()?,
        _ => return None,
    };
    let rest: PathBuf = components.as_path().to_path_buf();
    Some((first_str, rest))
}

/// Map a stored display path back to its canonical host path.
///
/// Mirrors the resolution algorithm in [`validate_artifact_path`] for
/// already-known display paths. Returns `None` for any kind of miss: file
/// gone, mount removed from config, root no longer canonicalisable, etc.
/// These cases are indistinguishable to the caller by design — `None` here
/// just means "no stable URL available", never a security signal.
///
/// Resolve a stored display path to its canonical host path.
///
/// Mirrors [`validate_artifact_path`]'s resolution algorithm: cwd-first,
/// then slug-prefix lookup by name. Returns `None` for any kind of miss
/// (file gone, mount removed, root uncanonicalisable) — by design, these
/// cases are indistinguishable to the caller.
///
/// Tests-only helper. Production code computes `stable_url` via
/// [`compute_stable_url`], which calls the same underlying classifier.
#[cfg(test)]
pub(crate) fn resolve_display_path(
    display_path: &str,
    cwd: &Path,
    mounts: &[MountRoot],
) -> Option<PathBuf> {
    classify_display_path(display_path, cwd, mounts).map(|r| r.canonical)
}

/// Which root a resolved display path belongs to.
enum ResolvedRoot<'a> {
    /// File is under the conversation's cwd.
    Cwd,
    /// File is under a named mount. Carries the canonicalised `host_path`
    /// so callers don't have to re-canonicalise.
    Mount {
        mount: &'a MountRoot,
        canonical_host: PathBuf,
    },
}

struct ResolvedDisplayPath<'a> {
    root: ResolvedRoot<'a>,
    canonical: PathBuf,
}

/// Resolves a display path to its canonical host path and which root it
/// belongs to. Single source of truth for the cwd-first, slug-by-name
/// algorithm shared with [`validate_artifact_path`].
fn classify_display_path<'a>(
    display_path: &str,
    cwd: &Path,
    mounts: &'a [MountRoot],
) -> Option<ResolvedDisplayPath<'a>> {
    let path = Path::new(display_path);
    let cwd_canonical = cwd.canonicalize().ok()?;

    // Cwd-first (matches validate_relative).
    let cwd_candidate = cwd.join(path);
    if let Ok(canonical) = cwd_candidate.canonicalize()
        && canonical.starts_with(&cwd_canonical)
    {
        return Some(ResolvedDisplayPath {
            root: ResolvedRoot::Cwd,
            canonical,
        });
    }

    // Slug-prefix fallback: lookup by slug name.
    let (first, rest) = split_first_segment(path)?;
    let mount = mounts.iter().find(|m| m.slug == first)?;
    let canonical_host = mount.host_path.canonicalize().ok()?;
    let canonical = mount.host_path.join(&rest).canonicalize().ok()?;
    if canonical.starts_with(&canonical_host) {
        Some(ResolvedDisplayPath {
            root: ResolvedRoot::Mount {
                mount,
                canonical_host,
            },
            canonical,
        })
    } else {
        None
    }
}

/// Read a markdown file and return its raw content (without rendering).
///
/// Returns `(display_path, raw_content)` where `display_path` is computed
/// per [`validate_artifact_path`]. Used by the snapshot storage flow which
/// needs raw content for DB storage and renders separately.
pub async fn read_artifact_content(
    file_path: &str,
    cwd: &Path,
    mounts: &[MountRoot],
) -> Result<(String, String), ArtifactError> {
    let validated = validate_artifact_path(file_path, cwd, mounts)?;

    // Check file size before reading.
    let metadata = tokio::fs::metadata(&validated.canonical_path)
        .await
        .map_err(|e| ArtifactError::ReadError {
            file_path: file_path.to_string(),
            detail: e.to_string(),
        })?;

    if metadata.len() > MAX_FILE_SIZE {
        return Err(ArtifactError::TooLarge {
            file_path: file_path.to_string(),
            size_bytes: metadata.len(),
        });
    }

    // Read the file.
    let content = tokio::fs::read_to_string(&validated.canonical_path)
        .await
        .map_err(|e| ArtifactError::ReadError {
            file_path: file_path.to_string(),
            detail: e.to_string(),
        })?;

    Ok((validated.display_path, content))
}

/// Characters that need percent-encoding in a URL path segment.
/// Based on RFC 3986 — encodes everything except unreserved + common sub-delimiters.
const PATH_SEGMENT_ENCODE: &percent_encoding::AsciiSet = &percent_encoding::CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'<')
    .add(b'>')
    .add(b'`')
    .add(b'?')
    .add(b'{')
    .add(b'}')
    .add(b'%');

/// Percent-encode a file path for use in a URL, encoding each component separately.
pub fn encode_url_path(path: &str) -> String {
    path.split('/')
        .map(|segment| {
            percent_encoding::utf8_percent_encode(segment, PATH_SEGMENT_ENCODE).to_string()
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// Compute the stable file URL for a file identified by its stored display path.
///
/// Takes the same inputs as `resolve_display_path` plus the app's `working_dir`
/// and `slug`, and goes through the same `classify_display_path` helper, so
/// the two functions cannot disagree on which root (cwd or which mount) a
/// display path belongs to. Cwd-first precedence is preserved.
///
/// Returns:
/// - `Some("/app/{slug}/file/{rel}")` when the display path resolves under
///   `cwd` AND the canonical path is inside `working_dir` (the normal case
///   where `cwd == working_dir`; also handles `cwd ⊂ working_dir`).
/// - `Some("/app/{slug}/mount/{mount_slug}/file/{rel}")` when the display
///   path resolves via a slug prefix into one of `mount_roots`.
/// - `None` when the display path can't be resolved (file gone, mount
///   removed from config, cwd uncanonicalisable), or when the cwd-resolved
///   canonical path doesn't fall under `working_dir` at all.
pub fn compute_stable_url(
    display_path: &str,
    cwd: &Path,
    working_dir: &Path,
    mount_roots: &[MountRoot],
    slug: &str,
) -> Option<String> {
    let resolved = classify_display_path(display_path, cwd, mount_roots)?;
    match resolved.root {
        ResolvedRoot::Cwd => {
            let canonical_working_dir = working_dir.canonicalize().ok()?;
            let relative = resolved
                .canonical
                .strip_prefix(&canonical_working_dir)
                .ok()?;
            let encoded = encode_url_path(relative.to_str()?);
            Some(format!("/app/{slug}/file/{encoded}"))
        }
        ResolvedRoot::Mount {
            mount,
            canonical_host,
        } => {
            let relative = resolved.canonical.strip_prefix(&canonical_host).ok()?;
            let encoded = encode_url_path(relative.to_str()?);
            let mount_slug = &mount.slug;
            Some(format!("/app/{slug}/mount/{mount_slug}/file/{encoded}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn no_mounts() -> Vec<MountRoot> {
        vec![]
    }

    #[test]
    fn validate_relative_path_in_cwd() {
        let dir = tempfile::tempdir().unwrap();
        let md_path = dir.path().join("test.md");
        std::fs::write(&md_path, "# Hello").unwrap();

        let result = validate_artifact_path("test.md", dir.path(), &no_mounts());
        let v = result.expect("should succeed");
        assert_eq!(v.canonical_path, md_path.canonicalize().unwrap());
        assert_eq!(v.display_path, "test.md");
    }

    #[test]
    fn validate_absolute_path_in_cwd() {
        let dir = tempfile::tempdir().unwrap();
        let md_path = dir.path().join("test.md");
        std::fs::write(&md_path, "# Hello").unwrap();

        let result = validate_artifact_path(md_path.to_str().unwrap(), dir.path(), &no_mounts());
        let v = result.expect("should succeed");
        assert_eq!(v.display_path, "test.md");
    }

    #[test]
    fn validate_subdirectory_path() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("docs");
        std::fs::create_dir(&sub).unwrap();
        let md_path = sub.join("plan.md");
        std::fs::write(&md_path, "# Plan").unwrap();

        let result = validate_artifact_path("docs/plan.md", dir.path(), &no_mounts());
        let v = result.expect("should succeed");
        assert_eq!(v.display_path, "docs/plan.md");
    }

    #[test]
    fn reject_path_traversal() {
        let dir = tempfile::tempdir().unwrap();
        let md_path = dir.path().join("test.md");
        std::fs::write(&md_path, "# Hello").unwrap();

        let result = validate_artifact_path("../../../etc/passwd", dir.path(), &no_mounts());
        assert!(
            matches!(
                result,
                Err(ArtifactError::PathTraversal { .. })
                    | Err(ArtifactError::NotFound { .. })
                    | Err(ArtifactError::NotMarkdown { .. })
            ),
            "should reject traversal: {result:?}"
        );
    }

    #[test]
    fn reject_non_markdown_extension() {
        let dir = tempfile::tempdir().unwrap();
        let txt_path = dir.path().join("notes.txt");
        std::fs::write(&txt_path, "hello").unwrap();

        let result = validate_artifact_path("notes.txt", dir.path(), &no_mounts());
        assert!(
            matches!(result, Err(ArtifactError::NotMarkdown { .. })),
            "should reject non-.md: {result:?}"
        );
    }

    #[test]
    fn reject_file_not_found() {
        let dir = tempfile::tempdir().unwrap();

        let result = validate_artifact_path("nonexistent.md", dir.path(), &no_mounts());
        assert!(
            matches!(result, Err(ArtifactError::NotFound { .. })),
            "should report not found: {result:?}"
        );
    }

    #[test]
    fn reject_symlink_outside_cwd() {
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let outside_file = outside.path().join("secret.md");
        std::fs::write(&outside_file, "# Secret").unwrap();

        // Create symlink inside cwd pointing outside.
        let link_path = dir.path().join("link.md");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&outside_file, &link_path).unwrap();
        #[cfg(not(unix))]
        {
            // Skip on non-unix — symlinks work differently on Windows.
            return;
        }

        let result = validate_artifact_path("link.md", dir.path(), &no_mounts());
        assert!(
            matches!(result, Err(ArtifactError::PathTraversal { .. })),
            "should reject symlink outside cwd: {result:?}"
        );
    }

    // --- Mount support ---

    #[test]
    fn validate_absolute_path_in_mount_succeeds_with_slug_prefix() {
        let cwd = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        let sub = mount.path().join("kb").join("finance");
        std::fs::create_dir_all(&sub).unwrap();
        let md_path = sub.join("tips.md");
        std::fs::write(&md_path, "# Tips").unwrap();

        let mounts = vec![MountRoot {
            host_path: mount.path().to_path_buf(),
            slug: "graf-life".into(),
        }];

        let v = validate_artifact_path(md_path.to_str().unwrap(), cwd.path(), &mounts)
            .expect("should succeed");
        assert_eq!(v.display_path, "graf-life/kb/finance/tips.md");
        assert_eq!(v.canonical_path, md_path.canonicalize().unwrap());
    }

    #[test]
    fn validate_relative_slug_prefixed_path_in_mount() {
        let cwd = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        let md_path = mount.path().join("foo.md");
        std::fs::write(&md_path, "# Foo").unwrap();

        let mounts = vec![MountRoot {
            host_path: mount.path().to_path_buf(),
            slug: "graf-life".into(),
        }];

        let v = validate_artifact_path("graf-life/foo.md", cwd.path(), &mounts)
            .expect("should succeed");
        assert_eq!(v.display_path, "graf-life/foo.md");
        assert_eq!(v.canonical_path, md_path.canonicalize().unwrap());
    }

    #[test]
    fn cwd_first_precedence_when_slug_collides_with_cwd_dir() {
        // Both cwd/<slug>/foo.md and mount/foo.md exist. Path "<slug>/foo.md"
        // must resolve to the cwd file.
        let cwd = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        let cwd_sub = cwd.path().join("graf-life");
        std::fs::create_dir(&cwd_sub).unwrap();
        let cwd_file = cwd_sub.join("foo.md");
        std::fs::write(&cwd_file, "# Cwd version").unwrap();
        let mount_file = mount.path().join("foo.md");
        std::fs::write(&mount_file, "# Mount version").unwrap();

        let mounts = vec![MountRoot {
            host_path: mount.path().to_path_buf(),
            slug: "graf-life".into(),
        }];

        let v = validate_artifact_path("graf-life/foo.md", cwd.path(), &mounts)
            .expect("should succeed");
        assert_eq!(v.display_path, "graf-life/foo.md");
        assert_eq!(v.canonical_path, cwd_file.canonicalize().unwrap());
    }

    #[test]
    fn absolute_path_outside_all_roots_rejects() {
        let cwd = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let outside_file = outside.path().join("secret.md");
        std::fs::write(&outside_file, "# Secret").unwrap();

        let mounts = vec![MountRoot {
            host_path: mount.path().to_path_buf(),
            slug: "graf-life".into(),
        }];

        let result = validate_artifact_path(outside_file.to_str().unwrap(), cwd.path(), &mounts);
        assert!(
            matches!(result, Err(ArtifactError::PathTraversal { .. })),
            "should reject absolute path outside all roots: {result:?}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn symlink_inside_cwd_pointing_into_mount_succeeds() {
        // A symlink in cwd pointing to a mount file should resolve through
        // the mount root and display with slug prefix.
        let cwd = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        let mount_file = mount.path().join("real.md");
        std::fs::write(&mount_file, "# Real").unwrap();

        let link = cwd.path().join("link.md");
        std::os::unix::fs::symlink(&mount_file, &link).unwrap();

        let mounts = vec![MountRoot {
            host_path: mount.path().to_path_buf(),
            slug: "graf-life".into(),
        }];

        let v = validate_artifact_path("link.md", cwd.path(), &mounts)
            .expect("symlink-cwd→mount should succeed via mount root");
        assert_eq!(v.canonical_path, mount_file.canonicalize().unwrap());
        assert_eq!(v.display_path, "graf-life/real.md");
    }

    #[test]
    fn multiple_mounts_first_match_wins() {
        // Two mounts; absolute paths land in each. Verify display path uses
        // the slug of the matching mount (not just the first).
        let cwd = tempfile::tempdir().unwrap();
        let mount_a = tempfile::tempdir().unwrap();
        let mount_b = tempfile::tempdir().unwrap();
        let file_a = mount_a.path().join("a.md");
        let file_b = mount_b.path().join("b.md");
        std::fs::write(&file_a, "# A").unwrap();
        std::fs::write(&file_b, "# B").unwrap();

        let mounts = vec![
            MountRoot {
                host_path: mount_a.path().to_path_buf(),
                slug: "alpha".into(),
            },
            MountRoot {
                host_path: mount_b.path().to_path_buf(),
                slug: "beta".into(),
            },
        ];

        let v_a = validate_artifact_path(file_a.to_str().unwrap(), cwd.path(), &mounts).unwrap();
        assert_eq!(v_a.display_path, "alpha/a.md");

        let v_b = validate_artifact_path(file_b.to_str().unwrap(), cwd.path(), &mounts).unwrap();
        assert_eq!(v_b.display_path, "beta/b.md");
    }

    #[test]
    fn relative_dotdot_into_mount_resolves_via_canonicalisation() {
        // A relative path with ".." that canonicalises into a mount should
        // succeed and produce a slug-prefixed display path. This is the
        // behaviour the code-reviewer flagged: the security invariant is
        // "canonical lands under SOME allowed root", not "input was
        // syntactically cwd-relative".
        let parent = tempfile::tempdir().unwrap();
        let cwd = parent.path().join("cwd");
        std::fs::create_dir(&cwd).unwrap();
        let mount = parent.path().join("mount-root");
        std::fs::create_dir(&mount).unwrap();
        let mount_file = mount.join("foo.md");
        std::fs::write(&mount_file, "# Foo").unwrap();

        let mounts = vec![MountRoot {
            host_path: mount.clone(),
            slug: "graf-life".into(),
        }];

        let v = validate_artifact_path("../mount-root/foo.md", &cwd, &mounts).unwrap();
        assert_eq!(v.display_path, "graf-life/foo.md");
        assert_eq!(v.canonical_path, mount_file.canonicalize().unwrap());
    }

    #[test]
    fn relative_dotdot_outside_all_roots_rejects() {
        // ".." into a directory that is neither cwd nor any mount must reject
        // even if the file exists.
        let parent = tempfile::tempdir().unwrap();
        let cwd = parent.path().join("cwd");
        std::fs::create_dir(&cwd).unwrap();
        let outside = parent.path().join("not-a-mount");
        std::fs::create_dir(&outside).unwrap();
        let outside_file = outside.join("secret.md");
        std::fs::write(&outside_file, "# Secret").unwrap();

        let result = validate_artifact_path("../not-a-mount/secret.md", &cwd, &no_mounts());
        assert!(
            matches!(result, Err(ArtifactError::PathTraversal { .. })),
            "should reject .. escaping all roots: {result:?}"
        );
    }

    #[test]
    fn broken_mount_is_skipped_with_warning() {
        // A mount whose host_path doesn't exist (e.g. cleaned up at runtime)
        // is dropped with a warning; it should not poison validation for files
        // in other mounts or cwd.
        let cwd = tempfile::tempdir().unwrap();
        let cwd_file = cwd.path().join("foo.md");
        std::fs::write(&cwd_file, "# Foo").unwrap();

        let mounts = vec![MountRoot {
            host_path: PathBuf::from("/this/path/does/not/exist/anywhere"),
            slug: "ghost".into(),
        }];

        // Cwd resolution still works.
        let v = validate_artifact_path("foo.md", cwd.path(), &mounts).unwrap();
        assert_eq!(v.display_path, "foo.md");

        // Slug-prefixed path against the broken mount returns NotFound, not
        // a panic or a leaked IO error.
        let result = validate_artifact_path("ghost/anything.md", cwd.path(), &mounts);
        assert!(
            matches!(result, Err(ArtifactError::NotFound { .. })),
            "broken mount should return NotFound for slug-prefixed lookup: {result:?}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn symlink_inside_mount_pointing_outside_rejects() {
        let cwd = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let outside_file = outside.path().join("secret.md");
        std::fs::write(&outside_file, "# Secret").unwrap();

        let link = mount.path().join("link.md");
        std::os::unix::fs::symlink(&outside_file, &link).unwrap();

        let mounts = vec![MountRoot {
            host_path: mount.path().to_path_buf(),
            slug: "graf-life".into(),
        }];

        // Try via the slug-prefixed relative form.
        let result = validate_artifact_path("graf-life/link.md", cwd.path(), &mounts);
        assert!(
            matches!(result, Err(ArtifactError::PathTraversal { .. })),
            "should reject symlink inside mount pointing outside: {result:?}"
        );
    }

    #[tokio::test]
    async fn read_artifact_content_success() {
        let dir = tempfile::tempdir().unwrap();
        let md_path = dir.path().join("test.md");
        std::fs::write(&md_path, "# Hello\n\nWorld").unwrap();

        let result = read_artifact_content("test.md", dir.path(), &no_mounts()).await;
        let (display_path, raw) = result.expect("should succeed");
        assert_eq!(display_path, "test.md");
        assert_eq!(raw, "# Hello\n\nWorld");
    }

    #[tokio::test]
    async fn read_artifact_content_too_large() {
        let dir = tempfile::tempdir().unwrap();
        let md_path = dir.path().join("big.md");
        // Write a file just over 1MB.
        let content = "x".repeat(MAX_FILE_SIZE as usize + 1);
        std::fs::write(&md_path, content).unwrap();

        let result = read_artifact_content("big.md", dir.path(), &no_mounts()).await;
        assert!(
            matches!(result, Err(ArtifactError::TooLarge { .. })),
            "should reject large file: {result:?}"
        );
    }

    #[test]
    fn error_display_messages() {
        // Verify each error variant produces a useful message.
        let errors = vec![
            ArtifactError::PathTraversal {
                file_path: "../secret".into(),
            },
            ArtifactError::NotMarkdown {
                file_path: "file.txt".into(),
            },
            ArtifactError::NotFound {
                file_path: "missing.md".into(),
            },
            ArtifactError::NoCwd,
            ArtifactError::TooLarge {
                file_path: "big.md".into(),
                size_bytes: 2_000_000,
            },
            ArtifactError::ReadError {
                file_path: "bad.md".into(),
                detail: "permission denied".into(),
            },
        ];
        for err in &errors {
            let msg = err.to_string();
            assert!(!msg.is_empty(), "error message should not be empty");
        }
    }

    // --- resolve_display_path ---

    #[test]
    fn resolve_display_path_cwd_file() {
        let cwd = tempfile::tempdir().unwrap();
        let f = cwd.path().join("foo.md");
        std::fs::write(&f, "x").unwrap();

        let resolved = resolve_display_path("foo.md", cwd.path(), &[]);
        assert_eq!(resolved, Some(f.canonicalize().unwrap()));
    }

    #[test]
    fn resolve_display_path_mount_file() {
        let cwd = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        let f = mount.path().join("foo.md");
        std::fs::write(&f, "x").unwrap();

        let mounts = vec![MountRoot {
            host_path: mount.path().to_path_buf(),
            slug: "graf-life".into(),
        }];

        let resolved = resolve_display_path("graf-life/foo.md", cwd.path(), &mounts);
        assert_eq!(resolved, Some(f.canonicalize().unwrap()));
    }

    #[test]
    fn resolve_display_path_round_trips_cwd() {
        let cwd = tempfile::tempdir().unwrap();
        let f = cwd.path().join("docs").join("plan.md");
        std::fs::create_dir(cwd.path().join("docs")).unwrap();
        std::fs::write(&f, "x").unwrap();

        let v = validate_artifact_path("docs/plan.md", cwd.path(), &[]).unwrap();
        let resolved = resolve_display_path(&v.display_path, cwd.path(), &[]);
        assert_eq!(resolved, Some(v.canonical_path));
    }

    #[test]
    fn resolve_display_path_round_trips_mount() {
        let cwd = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        let f = mount.path().join("kb").join("foo.md");
        std::fs::create_dir(mount.path().join("kb")).unwrap();
        std::fs::write(&f, "x").unwrap();

        let mounts = vec![MountRoot {
            host_path: mount.path().to_path_buf(),
            slug: "graf-life".into(),
        }];

        let v = validate_artifact_path(f.to_str().unwrap(), cwd.path(), &mounts).unwrap();
        let resolved = resolve_display_path(&v.display_path, cwd.path(), &mounts);
        assert_eq!(resolved, Some(v.canonical_path));
    }

    #[test]
    fn resolve_display_path_returns_none_when_mount_no_longer_present() {
        let cwd = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        let f = mount.path().join("foo.md");
        std::fs::write(&f, "x").unwrap();

        // Display path persisted while a mount with slug "old" was present;
        // mount has since been removed from config.
        let resolved = resolve_display_path("old/foo.md", cwd.path(), &[]);
        assert_eq!(resolved, None);
    }

    #[test]
    fn resolve_display_path_returns_none_when_file_gone() {
        let cwd = tempfile::tempdir().unwrap();
        let resolved = resolve_display_path("never-existed.md", cwd.path(), &[]);
        assert_eq!(resolved, None);
    }

    // --- encode_url_path ---

    #[test]
    fn encode_url_path_plain_path_unchanged() {
        assert_eq!(encode_url_path("docs/plan.md"), "docs/plan.md");
    }

    #[test]
    fn encode_url_path_encodes_spaces() {
        assert_eq!(
            encode_url_path("my docs/my file.md"),
            "my%20docs/my%20file.md"
        );
    }

    #[test]
    fn encode_url_path_encodes_hash_and_question_mark() {
        assert_eq!(encode_url_path("file#1.md"), "file%231.md");
        assert_eq!(encode_url_path("file?v=2.md"), "file%3Fv=2.md");
    }

    #[test]
    fn encode_url_path_preserves_slashes() {
        assert_eq!(encode_url_path("a/b/c/d.txt"), "a/b/c/d.txt");
    }

    #[test]
    fn encode_url_path_encodes_percent() {
        assert_eq!(encode_url_path("100%.md"), "100%25.md");
    }

    // --- compute_stable_url ---

    #[test]
    fn compute_stable_url_file_in_working_dir() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("docs")).unwrap();
        std::fs::write(dir.path().join("docs").join("plan.md"), "# Plan").unwrap();

        let url = compute_stable_url("docs/plan.md", dir.path(), dir.path(), &[], "myapp");
        assert_eq!(url, Some("/app/myapp/file/docs/plan.md".to_string()));
    }

    #[test]
    fn compute_stable_url_unresolvable_display_path_returns_none() {
        let cwd = tempfile::tempdir().unwrap();

        // Display path doesn't exist under cwd and no mount prefix matches.
        let url = compute_stable_url("gone.md", cwd.path(), cwd.path(), &[], "myapp");
        assert_eq!(url, None);
    }

    #[test]
    fn compute_stable_url_cwd_file_outside_working_dir_returns_none() {
        // `cwd` resolves the file, but the file canonicalises outside
        // `working_dir` — no URL route serves it.
        let cwd = tempfile::tempdir().unwrap();
        let working = tempfile::tempdir().unwrap();
        std::fs::write(cwd.path().join("note.md"), "x").unwrap();

        let url = compute_stable_url("note.md", cwd.path(), working.path(), &[], "app");
        assert_eq!(url, None);
    }

    #[test]
    fn compute_stable_url_encodes_special_chars() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("my file.md"), "# Test").unwrap();

        let url = compute_stable_url("my file.md", dir.path(), dir.path(), &[], "app");
        assert_eq!(url, Some("/app/app/file/my%20file.md".to_string()));
    }

    #[test]
    fn compute_stable_url_nonexistent_working_dir_returns_none() {
        let file = tempfile::tempdir().unwrap();
        std::fs::write(file.path().join("test.md"), "x").unwrap();

        let url = compute_stable_url(
            "test.md",
            file.path(),
            Path::new("/nonexistent/dir"),
            &[],
            "app",
        );
        assert_eq!(url, None);
    }

    #[test]
    fn compute_stable_url_file_in_mount() {
        let cwd = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        std::fs::create_dir(mount.path().join("kb")).unwrap();
        std::fs::write(mount.path().join("kb").join("tips.md"), "# Tips").unwrap();

        let mounts = vec![MountRoot {
            host_path: mount.path().to_path_buf(),
            slug: "life".into(),
        }];
        let url = compute_stable_url("life/kb/tips.md", cwd.path(), cwd.path(), &mounts, "pa");
        assert_eq!(url, Some("/app/pa/mount/life/file/kb/tips.md".to_string()));
    }

    #[test]
    fn compute_stable_url_mount_encodes_special_chars() {
        let cwd = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        std::fs::write(mount.path().join("my file#1.md"), "x").unwrap();

        let mounts = vec![MountRoot {
            host_path: mount.path().to_path_buf(),
            slug: "tech".into(),
        }];
        let url = compute_stable_url("tech/my file#1.md", cwd.path(), cwd.path(), &mounts, "pa");
        assert_eq!(
            url,
            Some("/app/pa/mount/tech/file/my%20file%231.md".to_string())
        );
    }

    #[test]
    fn compute_stable_url_cwd_dir_shadows_mount_slug() {
        // When a cwd subdirectory shares a name with a mount slug, cwd wins
        // (cwd-first precedence, matching `resolve_display_path`). The
        // resulting URL is the `/file/` form, not `/mount/.../file/`.
        let cwd = tempfile::tempdir().unwrap();
        std::fs::create_dir(cwd.path().join("life")).unwrap();
        std::fs::write(cwd.path().join("life").join("foo.md"), "cwd content").unwrap();

        // Also create a different file in a mount with the same slug.
        let mount = tempfile::tempdir().unwrap();
        std::fs::write(mount.path().join("foo.md"), "mount content").unwrap();

        let mounts = vec![MountRoot {
            host_path: mount.path().to_path_buf(),
            slug: "life".into(),
        }];
        // Display path "life/foo.md" matches both; cwd wins.
        let url = compute_stable_url("life/foo.md", cwd.path(), cwd.path(), &mounts, "app");
        assert_eq!(url, Some("/app/app/file/life/foo.md".to_string()));
    }

    #[test]
    fn compute_stable_url_working_dir_mount_uses_file_form() {
        // Regression pin for `mount_roots_for`'s `is_working_dir` filter:
        // a working-dir mount must be filtered out, so working-dir files
        // get the `/file/` URL form instead of `/mount/<wd-slug>/file/`.
        use brenn_lib::config::{AccessLevel, ResolvedMount};

        let working = tempfile::tempdir().unwrap();
        std::fs::write(working.path().join("readme.md"), "x").unwrap();

        let wd_mount = ResolvedMount {
            slug: "ws-test".into(),
            host_path: working.path().to_path_buf(),
            container_path: None,
            access: AccessLevel::ReadWrite,
            auto_pull: false,
            is_working_dir: true,
            primary: false,
        };
        let mount_roots = mount_roots_for(std::slice::from_ref(&wd_mount));
        assert!(
            mount_roots.is_empty(),
            "mount_roots_for must filter is_working_dir=true mounts"
        );

        let url = compute_stable_url(
            "readme.md",
            working.path(),
            working.path(),
            &mount_roots,
            "app",
        );
        assert_eq!(url, Some("/app/app/file/readme.md".to_string()));
    }

    #[test]
    fn resolve_and_stable_url_agree_on_cwd_file() {
        // Paired cwd-case: `resolve_display_path` lands on a canonical path
        // inside cwd; `compute_stable_url` emits the `/file/` URL form with
        // the same relative path. Both go through `classify_display_path`'s
        // cwd-first branch, so they can't disagree.
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("docs")).unwrap();
        std::fs::write(dir.path().join("docs").join("plan.md"), "x").unwrap();

        let canonical = resolve_display_path("docs/plan.md", dir.path(), &[]).unwrap();
        assert_eq!(
            canonical,
            dir.path().join("docs/plan.md").canonicalize().unwrap()
        );

        let url = compute_stable_url("docs/plan.md", dir.path(), dir.path(), &[], "app");
        assert_eq!(url, Some("/app/app/file/docs/plan.md".to_string()));
    }

    #[test]
    fn resolve_and_stable_url_agree_on_mount_selection() {
        // Both functions go through the same `classify_display_path` helper,
        // so for any display path where `resolve_display_path` returns a path
        // under mount slug S, `compute_stable_url` must emit a URL under
        // `/mount/S/`. This test pins that invariant across multiple mounts.
        let cwd = tempfile::tempdir().unwrap();
        let mount_a = tempfile::tempdir().unwrap();
        let mount_b = tempfile::tempdir().unwrap();
        std::fs::write(mount_a.path().join("x.md"), "x").unwrap();
        std::fs::write(mount_b.path().join("y.md"), "y").unwrap();

        let mounts = vec![
            MountRoot {
                host_path: mount_a.path().to_path_buf(),
                slug: "alpha".into(),
            },
            MountRoot {
                host_path: mount_b.path().to_path_buf(),
                slug: "beta".into(),
            },
        ];

        // alpha/x.md: resolver → mount A canonical; URL → /mount/alpha/.
        let a_canonical = resolve_display_path("alpha/x.md", cwd.path(), &mounts).unwrap();
        assert!(a_canonical.starts_with(mount_a.path().canonicalize().unwrap()));
        let a_url = compute_stable_url("alpha/x.md", cwd.path(), cwd.path(), &mounts, "app");
        assert_eq!(a_url, Some("/app/app/mount/alpha/file/x.md".to_string()));

        // beta/y.md: resolver → mount B canonical; URL → /mount/beta/.
        let b_canonical = resolve_display_path("beta/y.md", cwd.path(), &mounts).unwrap();
        assert!(b_canonical.starts_with(mount_b.path().canonicalize().unwrap()));
        let b_url = compute_stable_url("beta/y.md", cwd.path(), cwd.path(), &mounts, "app");
        assert_eq!(b_url, Some("/app/app/mount/beta/file/y.md".to_string()));
    }

    #[test]
    fn compute_stable_url_aliased_mount_hosts_still_agree() {
        // Pathological config: two mounts with different slugs whose host
        // paths resolve to the same canonical tree (one mounted via symlink).
        // Because both functions look mounts up by the display-path's slug
        // name — not by list-order or canonical prefix — the chosen mount
        // matches the slug in the display path. `alpha/f.md` → mount alpha;
        // `beta/f.md` → mount beta. Even though they share on-disk content.
        let cwd = tempfile::tempdir().unwrap();
        let real = tempfile::tempdir().unwrap();
        let alias_parent = tempfile::tempdir().unwrap();
        let alias = alias_parent.path().join("alias");
        std::os::unix::fs::symlink(real.path(), &alias).unwrap();
        std::fs::write(real.path().join("f.md"), "x").unwrap();

        let mounts = vec![
            MountRoot {
                host_path: real.path().to_path_buf(),
                slug: "alpha".into(),
            },
            MountRoot {
                host_path: alias.clone(),
                slug: "beta".into(),
            },
        ];

        let url_a = compute_stable_url("alpha/f.md", cwd.path(), cwd.path(), &mounts, "app");
        assert_eq!(url_a, Some("/app/app/mount/alpha/file/f.md".to_string()));
        let url_b = compute_stable_url("beta/f.md", cwd.path(), cwd.path(), &mounts, "app");
        assert_eq!(url_b, Some("/app/app/mount/beta/file/f.md".to_string()));
    }
}
