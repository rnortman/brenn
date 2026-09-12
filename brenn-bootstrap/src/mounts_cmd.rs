//! `brenn mounts --mounts <FILE>`: what the operator declared, and what each
//! declaration looks like on disk.
//!
//! This is how an installer reads the mounts document without parsing the DSL
//! in shell. It is deliberately weaker than a boot: the document-level checks
//! decide the exit status, and the on-disk state is *reported* per mount rather
//! than refused. An installer needs both halves — it refuses when the mount it
//! is about to fill is undeclared (a document fact), and it refuses when some
//! *other* mount is not installed (a per-mount fact, because the reload this
//! install ends in would be refused), and only the first of those is an error
//! in the document.

use std::path::Path;

use brenn_lib::config::{MountDecl, MountStatus, compile_mounts};

/// Compile the mounts document and print one line per mount. Returns whether
/// the document itself is a mounts document, which the binary turns into its
/// exit status.
pub fn run_mounts(path: &Path) -> bool {
    let document = match compile_mounts(path) {
        Ok(document) => document,
        Err(report) => {
            eprintln!("{report}");
            return false;
        }
    };
    for (declared, status) in document.statuses() {
        println!("{}", line(declared, &status));
    }
    true
}

/// One mount's line of the listing: four tab-separated fields, always four.
///
/// The fourth is empty for a mount under no principal, and the tab before it is
/// still printed — the annex's `read_mounts` splits on tabs into four names, and
/// a line with three fields would fold the status into the wrong variable for
/// every mount that declares no ceiling.
fn line(declared: &MountDecl, status: &MountStatus) -> String {
    format!(
        "{}\t{}\t{}\t{}",
        declared.name,
        declared.path.display(),
        render_status(status),
        render_under(declared.under.as_ref().map(|under| under.value().as_str())),
    )
}

/// One status as the single field an installer reads with `cut -f3`.
///
/// Colon-separated so the discriminant is a prefix test: a shell asks
/// `case "$status" in ok:*)`, and the version and the tree list ride along for
/// the operator without a second invocation. A fault message is last because it
/// is the only field that can hold anything.
fn render_status(status: &MountStatus) -> String {
    match status {
        MountStatus::Ok { version, trees } => {
            let trees: Vec<&str> = trees.iter().map(|tree| tree.dir_name()).collect();
            format!("ok:{version}:{}", trees.join(","))
        }
        MountStatus::Missing => "missing".to_string(),
        MountStatus::Fault(message) => format!("fault:{}", one_line(message)),
    }
}

/// The ceiling field: `under:<p>` when the mount declares one, empty when it
/// does not.
///
/// A fourth field rather than a fifth colon-separated piece of the third,
/// because the ceiling is a document fact and the third field is what the mount
/// looks like on disk. An installer reading `cut -f3` reads what it always did.
fn render_under(under: Option<&str>) -> String {
    match under {
        Some(under) => format!("under:{under}"),
        None => String::new(),
    }
}

/// A diagnostic on one line: the listing's grammar is line-per-mount, so a
/// message that wraps would read as another mount.
fn one_line(message: &str) -> String {
    message.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use brenn_lib::config::MountTree;

    use super::*;

    fn tree_status(trees: &[MountTree]) -> String {
        render_status(&MountStatus::Ok {
            version: "0.20.0".to_string(),
            trees: trees.to_vec(),
        })
    }

    #[test]
    fn an_installed_mount_reports_its_version_and_its_trees() {
        assert_eq!(
            tree_status(&[MountTree::Components, MountTree::Modules]),
            "ok:0.20.0:components,modules"
        );
        assert_eq!(tree_status(&[MountTree::Surface]), "ok:0.20.0:surface");
        assert_eq!(tree_status(&[MountTree::Config]), "ok:0.20.0:config");
    }

    #[test]
    fn a_declaration_with_nothing_behind_it_reports_missing() {
        assert_eq!(render_status(&MountStatus::Missing), "missing");
    }

    /// A fault's message is folded onto one line: the listing's grammar is one
    /// line per mount, and the on-disk diagnostics are written as prose that
    /// the source wraps.
    #[test]
    fn a_fault_is_one_line_whatever_the_message_looked_like() {
        let status = MountStatus::Fault(
            "mount `x`: /srv/x holds none of `components/`,\n  `surface/`, `modules/`".to_string(),
        );
        assert_eq!(
            render_status(&status),
            "fault:mount `x`: /srv/x holds none of `components/`, `surface/`, `modules/`"
        );
    }

    /// The document decides the exit status; a declaration nothing has been
    /// installed under does not. That is what lets an installer read the
    /// declaration of the mount it is about to create.
    #[test]
    fn a_missing_mount_still_lists() {
        let dir = tempfile::tempdir().unwrap();
        let installed = dir.path().join("brenn");
        std::fs::create_dir_all(installed.join("modules")).unwrap();
        std::fs::write(installed.join("VERSION"), "0.20.0\n").unwrap();
        let document = dir.path().join("mounts.brenn");
        std::fs::write(
            &document,
            format!(
                "mount brenn {{ path = \"{}\"; }}\nmount later {{ path = \"{}\"; }}\n",
                installed.display(),
                dir.path().join("later").display(),
            ),
        )
        .unwrap();
        assert!(run_mounts(&document));

        let compiled = compile_mounts(&document).unwrap();
        let statuses = compiled.statuses();
        assert_eq!(statuses.len(), 2);
        assert_eq!(render_status(&statuses[0].1), "ok:0.20.0:modules");
        assert_eq!(render_status(&statuses[1].1), "missing");
    }

    /// The ceiling rides in a fourth field, so an installer reading the first
    /// three reads what it always did.
    #[test]
    fn a_ceiling_is_a_field_of_its_own() {
        assert_eq!(
            render_under(Some("assistant-automations")),
            "under:assistant-automations"
        );
        assert_eq!(render_under(None), "");
    }

    /// The line the binary prints, not the fields in isolation: every line
    /// splits to exactly four tab-separated pieces, with the fourth empty for a
    /// mount under no principal. The annex's `read_mounts` reads four names off
    /// each line, so dropping the trailing tab as a tidy-up would fold the
    /// status field into the wrong variable on every host.
    #[test]
    fn every_line_of_the_listing_has_four_fields() {
        let dir = tempfile::tempdir().unwrap();
        let installed = dir.path().join("brenn");
        std::fs::create_dir_all(installed.join("modules")).unwrap();
        std::fs::write(installed.join("VERSION"), "0.20.0\n").unwrap();
        let document = dir.path().join("mounts.brenn");
        std::fs::write(
            &document,
            format!(
                "mount brenn {{ path = \"{}\"; }}\n\
                 mount automations under assistant-automations {{ path = \"{}\"; }}\n",
                installed.display(),
                dir.path().join("automations").display(),
            ),
        )
        .unwrap();
        let compiled = compile_mounts(&document).unwrap();
        let lines: Vec<String> = compiled
            .statuses()
            .iter()
            .map(|(declared, status)| line(declared, status))
            .collect();
        let fields: Vec<Vec<&str>> = lines
            .iter()
            .map(|line| line.split('\t').collect())
            .collect();
        assert_eq!(fields[0].len(), 4, "{}", lines[0]);
        assert_eq!(fields[1].len(), 4, "{}", lines[1]);
        assert_eq!(fields[0][0], "brenn");
        assert_eq!(fields[0][3], "", "a mount under no one has an empty field");
        assert_eq!(fields[1][0], "automations");
        assert_eq!(fields[1][3], "under:assistant-automations");
    }

    /// A document that is not a mounts document is the one thing that fails.
    #[test]
    fn a_relative_path_is_a_refusal() {
        let dir = tempfile::tempdir().unwrap();
        let document = dir.path().join("mounts.brenn");
        std::fs::write(&document, "mount brenn { path = \"release\"; }\n").unwrap();
        assert!(!run_mounts(&document));
    }
}
