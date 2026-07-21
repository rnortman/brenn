//! Resolution of the gitleaks config a scan runs against.
//!
//! Every scan uses the repo's shared `.gitleaks.toml`. An optional site-local
//! overlay adds rules on top; when one is discovered the layering is
//! synthesized per invocation into a temp config that extends *this* repo's
//! public file, so one shared overlay works across repos.

use std::path::{Path, PathBuf};

/// Env var naming an overlay file. Setting it declares the overlay required.
pub const OVERLAY_ENV: &str = "BRENN_SCRUB_DENYLIST";
/// Gitignored repo-root overlay location.
pub const OVERLAY_FILENAME: &str = ".gitleaks.local.toml";
/// Tracked public rule file, one per repo.
pub const PUBLIC_FILENAME: &str = ".gitleaks.toml";

/// Presence of a discovery candidate. A dangling symlink is distinct from
/// absence: "never had it" is lenient, "have it but broken" is fatal.
#[derive(Debug, PartialEq, Eq)]
pub enum Candidate {
    Absent,
    Present,
    Dangling,
}

pub fn candidate_state(path: &Path) -> Candidate {
    match std::fs::symlink_metadata(path) {
        Err(_) => Candidate::Absent,
        Ok(_) if path.exists() => Candidate::Present,
        Ok(_) => Candidate::Dangling,
    }
}

/// First hit wins: env var, then repo-root overlay, then nothing.
///
/// `env_value` is passed in rather than read here so the order is testable
/// without mutating process env.
pub fn discover_overlay(repo_root: &Path, env_value: Option<&str>) -> Option<PathBuf> {
    if let Some(raw) = env_value {
        let path = PathBuf::from(raw);
        match candidate_state(&path) {
            Candidate::Present => return Some(path),
            Candidate::Dangling => {
                panic!(
                    "{OVERLAY_ENV} points at a dangling symlink: {}",
                    path.display()
                )
            }
            Candidate::Absent => {
                panic!(
                    "{OVERLAY_ENV} is set but its target does not exist: {}",
                    path.display()
                )
            }
        }
    }

    let local = repo_root.join(OVERLAY_FILENAME);
    match candidate_state(&local) {
        Candidate::Present => Some(local),
        Candidate::Dangling => {
            panic!(
                "{OVERLAY_FILENAME} is a dangling symlink: {}",
                local.display()
            )
        }
        Candidate::Absent => None,
    }
}

/// Build the synthesized layering config text: an `[extend]` at the current
/// repo's public file plus the overlay's rules verbatim.
///
/// The overlay must be rules-only; a stray `[extend]` would pin it to one
/// repo's public config and silently break cross-repo reuse.
pub fn synthesize(public: &Path, overlay_text: &str, overlay_path: &Path) -> String {
    let parsed: toml::Table = overlay_text
        .parse()
        .unwrap_or_else(|e| panic!("overlay {} is not valid TOML: {e}", overlay_path.display()));
    assert!(
        !parsed.contains_key("extend"),
        "overlay {} contains an [extend] key; overlays must be rules-only \
         (layering is synthesized per invocation)",
        overlay_path.display()
    );

    let quoted = toml::Value::String(public.to_string_lossy().into_owned()).to_string();
    format!(
        "title = \"brenn-scrub synthesized layering\"\n\n[extend]\npath = {quoted}\n\n{overlay_text}"
    )
}

/// A config ready to hand to gitleaks, plus what went into it.
pub struct Resolved {
    pub config_path: PathBuf,
    pub public: PathBuf,
    pub overlay: Option<PathBuf>,
    /// Keeps the synthesized temp file alive for the scan's duration.
    _synth: Option<tempfile::TempPath>,
}

impl Resolved {
    /// One-line summary of what was actually loaded, for gating-mode output.
    pub fn summary(&self) -> String {
        match &self.overlay {
            Some(o) => format!(
                "config: {} + overlay {}",
                self.public.display(),
                o.display()
            ),
            None => format!("config: {} (no overlay)", self.public.display()),
        }
    }
}

pub fn resolve(repo_root: &Path, env_value: Option<&str>) -> Resolved {
    let public = repo_root.join(PUBLIC_FILENAME);
    assert!(
        public.exists(),
        "{} is missing; refusing to scan (gitleaks would silently fall back to its default config)",
        public.display()
    );

    let Some(overlay) = discover_overlay(repo_root, env_value) else {
        return Resolved {
            config_path: public.clone(),
            public,
            overlay: None,
            _synth: None,
        };
    };

    let text = std::fs::read_to_string(&overlay)
        .unwrap_or_else(|e| panic!("cannot read overlay {}: {e}", overlay.display()));
    let synthesized = synthesize(&public, &text, &overlay);

    let file = tempfile::Builder::new()
        .prefix("brenn-scrub-config")
        .suffix(".toml")
        .tempfile()
        .expect("cannot create temp config");
    std::fs::write(file.path(), synthesized).expect("cannot write temp config");
    let temp = file.into_temp_path();

    Resolved {
        config_path: temp.to_path_buf(),
        public,
        overlay: Some(overlay),
        _synth: Some(temp),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo_with_public() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(PUBLIC_FILENAME), "title = \"pub\"\n").unwrap();
        dir
    }

    #[test]
    fn env_override_wins_over_repo_local() {
        let dir = repo_with_public();
        let local = dir.path().join(OVERLAY_FILENAME);
        std::fs::write(&local, "").unwrap();
        let env_target = dir.path().join("elsewhere.toml");
        std::fs::write(&env_target, "").unwrap();

        let found = discover_overlay(dir.path(), Some(env_target.to_str().unwrap()));
        assert_eq!(found, Some(env_target));
    }

    #[test]
    #[should_panic(expected = "is set but its target does not exist")]
    fn env_override_missing_target_panics() {
        let dir = repo_with_public();
        discover_overlay(dir.path(), Some("/nonexistent/denylist.toml"));
    }

    #[test]
    fn repo_local_overlay_found_when_no_env() {
        let dir = repo_with_public();
        let local = dir.path().join(OVERLAY_FILENAME);
        std::fs::write(&local, "").unwrap();
        assert_eq!(discover_overlay(dir.path(), None), Some(local));
    }

    #[test]
    fn absence_of_both_is_silent_none() {
        let dir = repo_with_public();
        assert_eq!(discover_overlay(dir.path(), None), None);
    }

    #[test]
    #[should_panic(expected = "dangling symlink")]
    fn dangling_repo_local_symlink_panics() {
        let dir = repo_with_public();
        let local = dir.path().join(OVERLAY_FILENAME);
        std::os::unix::fs::symlink(dir.path().join("gone.toml"), &local).unwrap();
        discover_overlay(dir.path(), None);
    }

    #[test]
    #[should_panic(expected = "dangling symlink")]
    fn dangling_env_target_panics() {
        let dir = repo_with_public();
        let link = dir.path().join("link.toml");
        std::os::unix::fs::symlink(dir.path().join("gone.toml"), &link).unwrap();
        discover_overlay(dir.path(), Some(link.to_str().unwrap()));
    }

    #[test]
    fn synthesized_config_extends_public_and_carries_overlay_rules() {
        let overlay = "[[rules]]\nid = \"local-rule-a\"\nregex = '''zz'''\n";
        let out = synthesize(
            Path::new("/repo/.gitleaks.toml"),
            overlay,
            Path::new("/o.toml"),
        );
        assert!(out.contains("[extend]"));
        assert!(out.contains("\"/repo/.gitleaks.toml\""));
        assert!(out.contains("id = \"local-rule-a\""));
        // Parses as TOML with both the extend and the rule intact.
        let parsed: toml::Table = out.parse().unwrap();
        assert!(parsed.contains_key("extend"));
        assert_eq!(parsed["rules"].as_array().unwrap().len(), 1);
    }

    #[test]
    #[should_panic(expected = "must be rules-only")]
    fn stray_extend_in_overlay_panics() {
        synthesize(
            Path::new("/repo/.gitleaks.toml"),
            "[extend]\nuseDefault = true\n",
            Path::new("/o.toml"),
        );
    }

    #[test]
    #[should_panic(expected = "not valid TOML")]
    fn unparseable_overlay_panics() {
        synthesize(
            Path::new("/repo/.gitleaks.toml"),
            "this is not toml {{{",
            Path::new("/o.toml"),
        );
    }

    #[test]
    fn resolve_without_overlay_uses_public_directly() {
        let dir = repo_with_public();
        let r = resolve(dir.path(), None);
        assert_eq!(r.config_path, dir.path().join(PUBLIC_FILENAME));
        assert!(r.overlay.is_none());
        assert!(r.summary().contains("no overlay"));
    }

    #[test]
    fn resolve_with_overlay_synthesizes_temp_config() {
        let dir = repo_with_public();
        let local = dir.path().join(OVERLAY_FILENAME);
        std::fs::write(&local, "[[rules]]\nid = \"x\"\nregex = '''z'''\n").unwrap();

        let r = resolve(dir.path(), None);
        assert_ne!(r.config_path, r.public);
        let text = std::fs::read_to_string(&r.config_path).unwrap();
        assert!(text.contains("id = \"x\""));
        assert!(text.contains(dir.path().join(PUBLIC_FILENAME).to_str().unwrap()));
        assert!(r.summary().contains("overlay"));
    }

    #[test]
    #[should_panic(expected = "refusing to scan")]
    fn missing_public_config_panics() {
        let dir = tempfile::tempdir().unwrap();
        resolve(dir.path(), None);
    }
}
