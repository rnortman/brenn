//! Help-sidecar guard: every in-tree surface `help.md` is generated, not typed.
//!
//! A component's `help.md` is copied verbatim into the surface asset dir and
//! published to a retained channel for LLM conversations to read. A hand-written
//! one is a second, unlinked copy of facts that live in code, and nothing fails
//! when the two diverge. The per-crate drift tests hold the generated ones to
//! their generator, but they only exist for crates that have one: a new component
//! shipping a typed sidecar would sail through, because the build discovers
//! sidecars by glob.
//!
//! This guard closes that hole from the other side. It discovers the same sidecar
//! set the build does and requires each to open with the generated-file header, so
//! a component whose sidecar is not generated fails the gate instead of shipping.
//!
//! In-tree only. Out-of-tree components land their sidecars in the asset dir by
//! whatever means they choose; nothing in the server inspects the header, and this
//! guard never sees them.

use std::path::{Path, PathBuf};

/// The prefix a generated sidecar's first line carries. The full header text
/// lives in `brenn-surface-contract`; matching the stable prefix here keeps the
/// policy runner free of a dependency on the surface crates.
const HEADER_PREFIX: &str = "<!-- AUTO-GENERATED";

/// Where component crates live, relative to the repo root.
const COMPONENTS_DIR: &str = "surface/components";

/// The in-tree default chrome, which lives beside `surface/components/` but ships
/// a sidecar exactly like a component does.
const CHROME_DIR: &str = "surface/chrome";

/// A discovered sidecar: repo-root-relative path plus its first line.
type Sidecar = (PathBuf, String);

fn violations_from(observed: &[Sidecar]) -> Vec<String> {
    let mut found = Vec::new();
    if observed.is_empty() {
        found.push(format!(
            "no in-tree help sidecars found under {COMPONENTS_DIR}/*/help.md or \
             {CHROME_DIR}/help.md. Either every component stopped shipping one or this \
             guard's discovery no longer matches the build's — a gate that inspects \
             nothing passes vacuously, so this is a failure."
        ));
    }
    for (rel, first_line) in observed {
        if !first_line.starts_with(HEADER_PREFIX) {
            found.push(format!(
                "{}: first line is not the generated-sidecar header. In-tree help \
                 sidecars are written by their crate's `src/help.rs` \
                 (`help_markdown()`), gated by a `help_sidecar_matches_generator` test \
                 calling `brenn_surface_test_fixtures::enforce_help_sidecar`, and \
                 rewritten by `make regen-surface-help`. Move the prose into a \
                 generator and interpolate every fact that has an identifier in code, \
                 rather than retyping it here.",
                rel.display()
            ));
        }
    }
    found
}

/// Component crate dirs plus chrome, repo-root-relative. A dir without a
/// `Cargo.toml` is not a crate and is not a component.
fn sidecar_dirs(files: &[PathBuf]) -> Vec<PathBuf> {
    let components = Path::new(COMPONENTS_DIR);
    let mut dirs: Vec<PathBuf> = files
        .iter()
        .filter(|rel| rel.file_name().is_some_and(|n| n == "Cargo.toml"))
        .filter_map(|rel| rel.parent())
        // Immediate children of the components dir only: a crate nested deeper
        // is a component's own subcrate, not a component.
        .filter(|dir| dir.parent() == Some(components))
        .map(Path::to_path_buf)
        .collect();
    dirs.sort();
    dirs.push(PathBuf::from(CHROME_DIR));
    dirs
}

/// Every discovered sidecar with its first line. A dir with no `help.md` ships no
/// sidecar and is not this guard's business; an unreadable one is.
fn collect_sidecars(root: &Path, files: &[PathBuf]) -> Vec<Sidecar> {
    let present: std::collections::HashSet<&PathBuf> = files.iter().collect();
    sidecar_dirs(files)
        .into_iter()
        .map(|dir| dir.join("help.md"))
        .filter(|rel| present.contains(rel))
        .map(|rel| {
            let path = root.join(&rel);
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("help guard: cannot read {}: {e}", path.display()));
            (rel, text.lines().next().unwrap_or("").to_string())
        })
        .collect()
}

fn violations(root: &Path, files: &[PathBuf]) -> Vec<String> {
    violations_from(&collect_sidecars(root, files))
}

/// True if every in-tree surface sidecar opens with the generated-file header.
pub fn run_help_guard(root: &Path, files: &[PathBuf]) -> bool {
    let found = violations(root, files);
    if found.is_empty() {
        return true;
    }
    eprintln!("help guard: in-tree surface help sidecars must be generated:");
    for line in &found {
        eprintln!("  {line}");
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEADER: &str = "<!-- AUTO-GENERATED from this component's src/help.rs. Do not edit. -->";

    fn obs(pairs: &[(&str, &str)]) -> Vec<Sidecar> {
        pairs
            .iter()
            .map(|(p, line)| (PathBuf::from(*p), (*line).to_string()))
            .collect()
    }

    #[test]
    fn headered_sidecars_yield_no_violations() {
        let out = violations_from(&obs(&[
            ("surface/chrome/help.md", HEADER),
            ("surface/components/meeting/help.md", HEADER),
        ]));
        assert!(out.is_empty(), "{out:?}");
    }

    #[test]
    fn an_unheadered_sidecar_fails() {
        let out = violations_from(&obs(&[
            ("surface/chrome/help.md", HEADER),
            ("surface/components/gauge/help.md", "# gauge"),
        ]));
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].starts_with("surface/components/gauge/help.md:"));
        assert!(out[0].contains("regen-surface-help"), "{}", out[0]);
    }

    /// An empty file has no first line, so it cannot carry the header.
    #[test]
    fn an_empty_sidecar_fails() {
        let out = violations_from(&obs(&[("surface/components/gauge/help.md", "")]));
        assert_eq!(out.len(), 1, "{out:?}");
    }

    /// A guard that found nothing must fail: that is what a discovery walk gone
    /// stale looks like, and it is indistinguishable from a clean pass otherwise.
    #[test]
    fn an_empty_sidecar_set_fails() {
        let out = violations_from(&[]);
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].contains("vacuously"), "{}", out[0]);
    }

    /// The prefix is matched, not the whole header: the full text lives in the
    /// contract crate and may gain wording without breaking the gate.
    #[test]
    fn the_prefix_is_what_is_matched() {
        let out = violations_from(&obs(&[(
            "surface/chrome/help.md",
            "<!-- AUTO-GENERATED, somewhat differently worded -->",
        )]));
        assert!(out.is_empty(), "{out:?}");
    }

    /// The collector half: every component crate dir plus chrome, a dir that is
    /// not a crate excluded, and a crate that ships no sidecar simply absent.
    #[test]
    fn the_collector_finds_every_component_sidecar_plus_chrome() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path();
        let mut files: Vec<PathBuf> = Vec::new();
        for (dir, names) in [
            ("surface/components/alpha", vec!["Cargo.toml", "help.md"]),
            ("surface/components/beta", vec!["Cargo.toml", "help.md"]),
            // A crate with no sidecar: nothing is published for it, so there is
            // nothing to gate.
            ("surface/components/gamma", vec!["Cargo.toml"]),
            // Not a crate — scratch, a stray dir — even carrying a help.md.
            ("surface/components/notacrate", vec!["help.md"]),
            // A crate nested below a component is not itself a component.
            (
                "surface/components/alpha/inner",
                vec!["Cargo.toml", "help.md"],
            ),
            ("surface/chrome", vec!["Cargo.toml", "help.md"]),
        ] {
            std::fs::create_dir_all(root.join(dir)).unwrap();
            for name in names {
                let body = if name == "help.md" {
                    format!("{HEADER}\nbody\n")
                } else {
                    String::new()
                };
                std::fs::write(root.join(dir).join(name), body).unwrap();
                files.push(PathBuf::from(dir).join(name));
            }
        }
        files.sort();

        let observed = collect_sidecars(root, &files);
        assert_eq!(
            observed.iter().map(|(p, _)| p.clone()).collect::<Vec<_>>(),
            vec![
                PathBuf::from("surface/components/alpha/help.md"),
                PathBuf::from("surface/components/beta/help.md"),
                PathBuf::from("surface/chrome/help.md"),
            ]
        );
        assert!(observed.iter().all(|(_, line)| line == HEADER));
        assert!(violations_from(&observed).is_empty());
    }

    /// The build's sidecar set and this guard's are two hand-maintained lists, and
    /// the test above only proves the walk matches its own rules. Pin the two
    /// together: every dir the Makefile discovers sidecars from must fall under a
    /// root this guard walks, so a new sidecar-bearing tier fails here instead of
    /// shipping a hand-written `help.md` past a clean gate.
    #[test]
    fn the_makefile_discovers_no_sidecar_dir_outside_the_guards_roots() {
        let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("xtask/ sits in the repo root");
        let makefile = std::fs::read_to_string(repo_root.join("Makefile"))
            .expect("the repo root has a Makefile");
        let definition = makefile_variable(&makefile, "SURFACE_COMPONENT_DIRS");
        let dirs = path_tokens(&definition);
        assert!(
            !dirs.is_empty(),
            "no path extracted from `SURFACE_COMPONENT_DIRS := {definition}` — the pin \
             below would compare nothing"
        );

        let components_root = format!("{COMPONENTS_DIR}/");
        for dir in &dirs {
            assert!(
                dir.starts_with(&components_root) || dir == CHROME_DIR,
                "the build discovers component sidecars from `{dir}`, which this guard's \
                 roots ({COMPONENTS_DIR}/*, {CHROME_DIR}) do not cover — add the new root \
                 to `sidecar_dirs`"
            );
        }
        // Both roots must be represented, or the pin is comparing against a list
        // that has itself lost one.
        assert!(
            dirs.iter().any(|dir| dir.starts_with(&components_root)),
            "{dirs:?}"
        );
        assert!(dirs.iter().any(|dir| dir == CHROME_DIR), "{dirs:?}");
    }

    /// The right-hand side of a `NAME :=` / `NAME =` Makefile assignment, with
    /// backslash continuations folded into one line.
    fn makefile_variable(makefile: &str, name: &str) -> String {
        let forms = [
            format!("{name} :="),
            format!("{name}:="),
            format!("{name} ="),
            format!("{name}="),
        ];
        let mut lines = makefile.lines();
        while let Some(line) = lines.next() {
            let Some(rest) = forms
                .iter()
                .find_map(|form| line.strip_prefix(form.as_str()))
            else {
                continue;
            };
            let mut value = rest.trim().to_string();
            while let Some(head) = value.strip_suffix('\\') {
                let next = lines
                    .next()
                    .unwrap_or_else(|| panic!("{name} ends on a line continuation"));
                value = format!("{} {}", head.trim_end(), next.trim());
            }
            return value;
        }
        panic!("the Makefile has no {name} assignment")
    }

    /// The directory paths a Makefile expression names: words containing a `/`,
    /// with make's own syntax (`$`, parens, commas) split away and its patterns
    /// (`%`) dropped. A trailing `/Cargo.toml` is stripped, since the build globs
    /// manifests to name their dirs.
    fn path_tokens(expr: &str) -> Vec<String> {
        expr.split(|c: char| {
            c.is_whitespace() || matches!(c, '(' | ')' | ',' | '$' | '{' | '}' | ':')
        })
        .filter(|token| token.contains('/') && !token.contains('%'))
        .map(|token| {
            token
                .strip_suffix("/Cargo.toml")
                .unwrap_or(token)
                .to_string()
        })
        .filter(|token| !token.is_empty())
        .collect()
    }

    #[test]
    fn the_makefile_extractors_read_make_syntax() {
        let makefile = "OTHER := x\nDIRS := $(patsubst %/,%,$(dir $(wildcard a/b/*/Cargo.toml))) \\\n    c/d\nNEXT := y\n";
        let definition = makefile_variable(makefile, "DIRS");
        assert_eq!(path_tokens(&definition), vec!["a/b/*", "c/d"]);
    }
}
