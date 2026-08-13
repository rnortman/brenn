//! The drift gate shared by every in-tree surface component's help-sidecar test.
//!
//! An in-tree `help.md` is generated from its crate's `src/help.rs`. The Bazel
//! build ships the generator's output directly into the surface asset dir; the
//! committed file is the fixture this gate holds to that same generator, and a
//! parity test holds the shipped bytes to the committed ones. What keeps the two
//! in step is one unit test per component crate calling [`enforce_help_sidecar`]:
//! an edit to either side fails that test until the file is regenerated.

use std::path::PathBuf;

/// The env var that flips the gate from compare to rewrite.
const REGEN_VAR: &str = "BRENN_REGEN_HELP";

/// How a stale sidecar is refreshed. The generator's output is a build artifact,
/// so the procedure is to build it and copy it over the committed file rather
/// than to run a single verb.
const REGEN_PROCEDURE: &str = "to refresh it, build the component package's \
     `<kind>_help` target and copy the generated `brenn_<kind>.help.md` over \
     the package's `help.md`";

/// Assert `<manifest_dir>/help.md` is byte-identical to `generated`, or — with
/// `BRENN_REGEN_HELP=1` in the environment — rewrite the file with `generated`
/// instead.
///
/// `generated` must begin with [`brenn_surface_contract::HELP_SIDECAR_HEADER`];
/// a generator that drops the header is a bug and panics here on either path.
/// The comparison is byte-exact with no normalization: the generator's output,
/// trailing newline included, is the canonical form of the file. A missing or
/// unreadable `help.md` is a failure, never a skip.
///
/// Panics carry the sidecar path and the regeneration procedure, so a failing
/// component test is self-remediating.
pub fn enforce_help_sidecar(manifest_dir: &str, generated: &str) {
    assert!(
        generated.starts_with(brenn_surface_contract::HELP_SIDECAR_HEADER),
        "generated help text for {manifest_dir} does not start with the \
         auto-generated header; the generator in src/help.rs must emit \
         brenn_surface_contract::HELP_SIDECAR_HEADER first"
    );

    let path: PathBuf = [manifest_dir, "help.md"].iter().collect();

    if regen_requested() {
        std::fs::write(&path, generated)
            .unwrap_or_else(|e| panic!("writing {}: {e}", path.display()));
        println!("regenerated {}", path.display());
        return;
    }

    let committed = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("reading {}: {e} — {REGEN_PROCEDURE}", path.display()));

    assert!(
        committed == generated,
        "{} is out of date: it is generated from this crate's src/help.rs, and \
         the committed bytes no longer match the generator's output. \
         {REGEN_PROCEDURE}, then commit the result.",
        path.display()
    );
}

/// Whether the environment asks for a rewrite. The var has exactly two accepted
/// states — unset (compare) and `1` (rewrite); anything else panics rather than
/// quietly comparing, because a run that looks like a regen but only compared
/// reports success on a tree it never rewrote.
fn regen_requested() -> bool {
    match std::env::var_os(REGEN_VAR) {
        None => false,
        Some(value) if value == "1" => true,
        Some(value) => panic!(
            "{REGEN_VAR}={value:?} is not an accepted value: leave it unset to \
             compare against the committed sidecar, or set it to exactly `1` to \
             rewrite it"
        ),
    }
}

/// The bodies of `doc`'s fenced ```` ```json ```` blocks, in order, each line
/// newline-terminated.
///
/// The help-sidecar tests feed the doc's own examples back through the parsers
/// that must accept them, so they need the examples as the doc ships them.
/// Returning every block — not just the first — is the point: a doc that grows a
/// second example must not leave it silently unchecked. An unterminated fence is
/// a malformed doc and panics.
pub fn json_blocks(doc: &str) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut current: Option<String> = None;
    for line in doc.lines() {
        match (&mut current, line) {
            (None, "```json") => current = Some(String::new()),
            (Some(_), "```") => blocks.push(current.take().expect("inside a block")),
            (Some(body), line) => {
                body.push_str(line);
                body.push('\n');
            }
            (None, _) => {}
        }
    }
    assert!(current.is_none(), "unterminated fenced block");
    blocks
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_surface_contract::HELP_SIDECAR_HEADER;

    /// A generated body with the mandatory header.
    fn generated(body: &str) -> String {
        format!("{HELP_SIDECAR_HEADER}{body}")
    }

    /// A scratch component dir holding `help.md` with `contents`.
    fn dir_with_sidecar(contents: &str) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("help.md"), contents).unwrap();
        dir
    }

    /// The gate is read-only unless BRENN_REGEN_HELP is set, and the tests here
    /// exercise both paths, so they must not race on the process-wide env. Every
    /// test goes through [`with_regen_state`], which takes this lock.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Run `f` with `BRENN_REGEN_HELP` in `state` (`None` = unset), restoring
    /// whatever it held before.
    ///
    /// Every test here pins the state rather than inheriting it: a value exported
    /// in the invoking shell would otherwise silently flip the compare-path tests
    /// onto the write path, where they fail for a reason that has nothing to do
    /// with the behavior they assert.
    fn with_regen_state(state: Option<&str>, f: impl FnOnce()) {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let previous = std::env::var_os(REGEN_VAR);
        // SAFETY: the lock serializes every env mutation and read in this
        // module, and no other thread in this test binary touches the var.
        unsafe {
            match state {
                Some(value) => std::env::set_var(REGEN_VAR, value),
                None => std::env::remove_var(REGEN_VAR),
            }
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
        unsafe {
            match &previous {
                Some(value) => std::env::set_var(REGEN_VAR, value),
                None => std::env::remove_var(REGEN_VAR),
            }
        };
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    }

    /// Run `f` on the compare path, whatever the ambient environment holds.
    fn comparing(f: impl FnOnce()) {
        with_regen_state(None, f);
    }

    fn with_regen_var(value: &str, f: impl FnOnce()) {
        with_regen_state(Some(value), f);
    }

    fn with_regen(f: impl FnOnce()) {
        with_regen_var("1", f);
    }

    #[test]
    fn matching_sidecar_passes() {
        comparing(|| {
            let text = generated("# demo\n");
            let dir = dir_with_sidecar(&text);
            enforce_help_sidecar(dir.path().to_str().unwrap(), &text);
        });
    }

    #[test]
    fn mismatched_sidecar_names_the_regen_procedure() {
        comparing(|| {
            let dir = dir_with_sidecar(&generated("# stale\n"));
            let manifest = dir.path().to_str().unwrap().to_string();
            let err = std::panic::catch_unwind(|| {
                enforce_help_sidecar(&manifest, &generated("# fresh\n"));
            })
            .expect_err("a mismatched sidecar must fail the gate");
            let message = panic_message(err.as_ref());
            assert!(message.contains("_help` target"), "{message}");
            assert!(message.contains("help.md"), "{message}");
        });
    }

    #[test]
    fn trailing_newline_difference_is_a_mismatch() {
        comparing(|| {
            let dir = dir_with_sidecar(&generated("# demo"));
            let manifest = dir.path().to_str().unwrap().to_string();
            std::panic::catch_unwind(|| {
                enforce_help_sidecar(&manifest, &generated("# demo\n"));
            })
            .expect_err("the comparison is byte-exact, newline included");
        });
    }

    #[test]
    fn missing_sidecar_fails() {
        comparing(|| {
            let dir = tempfile::tempdir().unwrap();
            let manifest = dir.path().to_str().unwrap().to_string();
            let err = std::panic::catch_unwind(|| {
                enforce_help_sidecar(&manifest, &generated("# demo\n"));
            })
            .expect_err("a missing sidecar must fail, not skip");
            assert!(panic_message(err.as_ref()).contains("help.md"));
        });
    }

    #[test]
    fn headerless_generator_output_fails() {
        comparing(|| {
            let dir = dir_with_sidecar("# demo\n");
            let manifest = dir.path().to_str().unwrap().to_string();
            let err = std::panic::catch_unwind(|| {
                enforce_help_sidecar(&manifest, "# demo\n");
            })
            .expect_err("generator output without the header must fail");
            assert!(panic_message(err.as_ref()).contains("HELP_SIDECAR_HEADER"));
        });
    }

    #[test]
    fn regen_writes_the_exact_bytes() {
        with_regen(|| {
            let text = generated("# fresh\n");
            let dir = dir_with_sidecar(&generated("# stale\n"));
            enforce_help_sidecar(dir.path().to_str().unwrap(), &text);
            let written = std::fs::read_to_string(dir.path().join("help.md")).unwrap();
            assert_eq!(written, text);
        });
    }

    #[test]
    fn regen_creates_a_missing_sidecar() {
        with_regen(|| {
            let text = generated("# fresh\n");
            let dir = tempfile::tempdir().unwrap();
            enforce_help_sidecar(dir.path().to_str().unwrap(), &text);
            assert_eq!(
                std::fs::read_to_string(dir.path().join("help.md")).unwrap(),
                text
            );
        });
    }

    #[test]
    fn regen_still_rejects_a_headerless_generator() {
        with_regen(|| {
            let dir = dir_with_sidecar("# demo\n");
            let manifest = dir.path().to_str().unwrap().to_string();
            std::panic::catch_unwind(|| enforce_help_sidecar(&manifest, "# demo\n"))
                .expect_err("the header assert holds on the write path too");
            assert_eq!(
                std::fs::read_to_string(dir.path().join("help.md")).unwrap(),
                "# demo\n",
                "a rejected generator must not have rewritten the file"
            );
        });
    }

    /// A truthy-looking value is not silently "compare": the caller believed it
    /// asked for a rewrite, so the gate refuses rather than reporting success on
    /// a tree it never touched.
    #[test]
    fn an_unrecognized_regen_value_fails() {
        for value in ["true", "yes", "1 ", "0"] {
            with_regen_var(value, || {
                let dir = dir_with_sidecar(&generated("# demo\n"));
                let manifest = dir.path().to_str().unwrap().to_string();
                let err = std::panic::catch_unwind(|| {
                    enforce_help_sidecar(&manifest, &generated("# demo\n"));
                })
                .expect_err("an unrecognized regen value must fail");
                let message = panic_message(err.as_ref());
                assert!(message.contains(REGEN_VAR), "{message}");
            });
        }
    }

    #[test]
    fn json_blocks_returns_every_block_in_order() {
        let doc = "intro\n\n```json\n{\"a\":1}\n```\n\nmiddle\n\n```json\n{\"b\":2}\n```\n";
        assert_eq!(json_blocks(doc), vec!["{\"a\":1}\n", "{\"b\":2}\n"]);
    }

    #[test]
    fn json_blocks_ignores_prose_and_other_fences() {
        let doc = "text\n\n```\nnot json\n```\n\n```json\n{}\n```\n";
        assert_eq!(json_blocks(doc), vec!["{}\n"]);
        assert!(json_blocks("no fences at all\n").is_empty());
    }

    #[test]
    fn an_unterminated_json_fence_fails() {
        std::panic::catch_unwind(|| json_blocks("```json\n{\"a\":1}\n"))
            .expect_err("an unterminated fence is a malformed doc");
    }

    fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
        payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_string()))
            .expect("assert! panics carry a string payload")
    }
}
