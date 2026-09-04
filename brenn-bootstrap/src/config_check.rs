//! `brenn config-check <file>`: would the server load this config file?
//!
//! Two layers. First the file goes through the same path `--config` boots from —
//! parse, resolve, derive, lower — with diagnostics rendered instead of a panic
//! ([`brenn_lib::config::check_config`]). Then the lowered configuration goes
//! through the messaging resolution passes that read nothing but the
//! configuration itself ([`resolve_messaging_offline`]), so the per-instance
//! surface gates — grant/binding coherence, chrome placement — decide the
//! verdict here rather than only at a service start.
//!
//! Environment facts remain out of scope: the passes that stat a path, read a
//! secret, or touch the DB are excluded by name on
//! [`resolve_messaging_offline`]. A workstation must be able to check a config
//! destined for another host. What the check does read is exactly its declared
//! inputs: the root document, and the module roots its packaged imports resolve
//! against.

use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};

use brenn_lib::config::{BrennConfig, DocumentInputs, check_config};
use brenn_lib::panic_util::{CONFIG_REFUSAL, catch_quietly, panic_message};
use brenn_messaging_boot::resolve_messaging_offline;

/// Check one config file, print the verdict. Returns whether it would load.
///
/// Strictly stronger than [`check_config`]: a document that compiles and lowers
/// clean can still be refused here, by a messaging gate that reads only the
/// configuration.
pub fn run_config_check(file: &Path, module_roots: &[PathBuf]) -> bool {
    let inputs = DocumentInputs {
        root: file.to_path_buf(),
        module_roots: module_roots.to_vec(),
    };
    let config = match check_config(&inputs) {
        Ok(config) => config,
        Err(report) => {
            eprintln!("{report}");
            return false;
        }
    };
    match offline_messaging_outcome(&config) {
        Ok(advisories) => {
            // Advice, not a verdict: the document is a config either way, and
            // this tool is the last place before a deploy where an operator
            // reads anything the passes have to say.
            for advisory in &advisories {
                eprintln!("{}: warning: {advisory}", file.display());
            }
            println!("{}: ok", file.display());
            true
        }
        Err(message) => {
            // Neutral framing on purpose: nothing has been started or stopped
            // here, and the refusals these passes raise are no longer only about
            // messaging resolution — a missing self-description stamp comes
            // through this arm too. The message names its own lane.
            eprintln!("{}: refused:\n{message}", file.display());
            false
        }
    }
}

/// Run the offline messaging resolution, returning its advisories, or the
/// refusal text if it refused.
///
/// The resolution asserts panic by design — the config is operator-authored and
/// the server's answer to a bad one is to die — but the check tool reports. So
/// the unwind is caught here, deliberately and narrowly: those asserts are the
/// single source of the refusal text, and re-stating them as `Result`s would
/// fork every message.
///
/// The default panic hook would print its own `thread '…' panicked at …` line
/// before the report, leaving the gate's output carrying two competing refusal
/// texts, so the catch goes through
/// [`catch_quietly`](brenn_lib::panic_util::catch_quietly), which silences this
/// thread's hook for exactly this call.
///
/// # Panics
///
/// On a payload [`refusal_text`] does not read as a refusal — a host bug rather
/// than a config verdict.
fn offline_messaging_outcome(config: &BrennConfig) -> Result<Vec<String>, String> {
    catch_quietly(AssertUnwindSafe(|| resolve_messaging_offline(config))).map_err(refusal_text)
}

/// How every refusal in the offline messaging passes starts.
///
/// A refusal and a bug both arrive here as text — `unwrap()` on `None`, an index
/// out of bounds and an integer overflow all carry a `String` payload, exactly
/// as a formatted `assert!` message does — so the payload's type cannot tell
/// them apart. The text can: every assert in those passes names the config it
/// is refusing, and it does so in one of these spellings — `config: …` for the
/// resolvers, and one per `AttachOwner` `Display` arm for the attach-policy
/// lowering, which prefixes its asserts with the principal it is lowering for.
/// A new owner arm is spelled here too, or its refusals read as host bugs.
///
/// The resolvers' spelling is [`CONFIG_REFUSAL`], shared with the validators
/// that build their messages from it, so the classifier and its producers
/// cannot drift by a retyped literal.
const REFUSAL_PREFIXES: [&str; 3] = [CONFIG_REFUSAL, "surface \"", "remote \""];

/// Read a caught payload as a refusal, or re-panic because it is a bug.
///
/// The narrow reading is the point. Rendering any panic as a config refusal
/// would send an operator hunting through a file that is fine for a defect that
/// lives in the resolvers, with the location and backtrace — the one thing a
/// bug report needs — suppressed by the very catch that made the report
/// readable. The re-panic happens after [`catch_quietly`] has returned, so it
/// reports through the real hook.
///
/// # Panics
///
/// On a payload carrying no text, and on text that carries no refusal prefix.
/// A refusal spelled some third way lands here too, and re-panicking on it is
/// the safe direction: the operator still reads the message, with a backtrace
/// naming the assert that produced it.
fn refusal_text(payload: Box<dyn Any + Send>) -> String {
    let Some(message) = panic_message(&*payload) else {
        panic!(
            "config-check: messaging resolution panicked with a payload of type id {:?}, which \
             carries no message — every refusal here is an assert message, so this is a bug",
            (*payload).type_id(),
        );
    };
    assert!(
        REFUSAL_PREFIXES
            .iter()
            .any(|prefix| message.starts_with(prefix)),
        "config-check: messaging resolution panicked with a message that is not a config \
         refusal, so this is a bug in the resolvers rather than a verdict on the file: \
         {message}",
    );
    message.to_string()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use brenn_dsl::fixture_text::processor_header;
    use brenn_dsl::processor_needs;
    use brenn_lib::messaging::config::Depth;
    // The counterpart constant, read only here: these tests are what hold it
    // disjoint from `REFUSAL_PREFIXES`.
    use brenn_lib::panic_util::HOST_DEFECT;

    use super::*;

    /// Exercises both `run_config_check` (the boolean) and `check_config` (the
    /// text), asserting the one implication that survives their layering:
    /// `run_config_check` runs the offline messaging pass on top of
    /// `check_config`, so it is strictly stronger — a `check_config` refusal is
    /// always a `run_config_check` refusal, but not the reverse.
    ///
    /// The boot-gate fixtures below therefore drive `run_config_check` directly:
    /// the text returned here is `check_config`'s alone, so a document refused
    /// only by the offline pass comes back `(false, "")` and any assertion on
    /// the gate's wording would be asserting against the empty string. Use
    /// `boot_gate_refusal` for those.
    fn check(name: &str, contents: &str) -> (bool, String) {
        let dir = tempfile::tempdir().unwrap();
        let inputs = brenn_lib::config::stage_fixture(dir.path(), name, contents);
        let ok = run_config_check(&inputs.root, &inputs.module_roots);
        let config = check_config(&inputs);
        assert!(
            config.is_ok() || !ok,
            "the report refused the document but the verdict passed it",
        );
        (ok, config.err().unwrap_or_default())
    }

    /// Every document that activates messaging owes boot the surface index, so
    /// a fixture about anything else declares it by hand. Spliced into each
    /// such fixture rather than retyped in it.
    const SURFACE_INDEX_DECL: &str = r#"
channel surface_index at "brenn:surface.index" {
    push_depth = 1;
    retain_depth = 1;
    standing_retain_depth = 1;
}
"#;

    /// A refusal's own text, rendered through. Every spelling the resolution
    /// passes use is a refusal, and both payload shapes carry text: a formatted
    /// `assert!` message arrives as a `String`, a bare literal as a
    /// `&'static str`.
    #[test]
    fn a_refusal_message_renders_through() {
        assert_eq!(
            refusal_text(Box::new("config: [[surface]] \"panel\": no".to_string())),
            "config: [[surface]] \"panel\": no",
        );
        assert_eq!(
            refusal_text(Box::new("surface \"panel\": duplicate AttachGrant")),
            "surface \"panel\": duplicate AttachGrant",
        );
        assert_eq!(
            refusal_text(Box::new(
                "remote \"pod\": publish_acl prefix matcher is empty (would match every channel)"
                    .to_string(),
            )),
            "remote \"pod\": publish_acl prefix matcher is empty (would match every channel)",
        );
    }

    /// A runtime panic inside the resolvers — an `unwrap()` on `None`, an index
    /// out of bounds — carries a `String` payload exactly as a refusal does. It
    /// must not be rendered as one: reported as a config refusal it sends the
    /// operator hunting through a file that is fine, with no location and no
    /// backtrace, which is the one thing the bug report needs.
    #[test]
    #[should_panic(expected = "is not a config refusal")]
    fn a_runtime_panic_is_a_bug_rather_than_a_verdict() {
        let _ = refusal_text(Box::new(
            "index out of bounds: the len is 3 but the index is 3".to_string(),
        ));
    }

    /// The deliberate host-bug spelling is the one shape that must never be read
    /// as a verdict: an assert that fires only when the host is wrong reaches
    /// the operator as a bug report with its backtrace, not as a claim that
    /// their file needs fixing. `resolve_derived_bare`'s malformed-address
    /// panic runs on the offline path and spells itself this way.
    #[test]
    #[should_panic(expected = "is not a config refusal")]
    fn a_host_defect_spelling_is_a_bug_rather_than_a_verdict() {
        let _ = refusal_text(Box::new(format!(
            "{HOST_DEFECT}derived surface-description channel is not a well-formed address"
        )));
    }

    /// The same property stated once, mechanically, over the classifier's whole
    /// array: the two constants live in `brenn-lib` and the array lives here,
    /// so either can be edited without the other's tests noticing. A fourth
    /// prefix that happens to admit the host-defect spelling fails here.
    #[test]
    fn no_refusal_prefix_admits_the_host_defect_spelling() {
        assert!(
            !REFUSAL_PREFIXES
                .iter()
                .any(|prefix| HOST_DEFECT.starts_with(prefix) || prefix.starts_with(HOST_DEFECT)),
            "{HOST_DEFECT:?} must not be classified as a config refusal by {REFUSAL_PREFIXES:?}",
        );
    }

    /// The other bug shape: a payload carrying no text at all. Nothing in the
    /// resolution passes produces one.
    #[test]
    #[should_panic(expected = "carries no message")]
    fn a_textless_payload_is_a_bug() {
        let _ = refusal_text(Box::new(42u64));
    }

    /// `ok` means "this file is a config", not "this config will boot here":
    /// `validate_and_resolve` is deliberately not run, so environment facts —
    /// here a container whose home directory does not exist on this machine —
    /// do not decide the verdict. A workstation must be able to check a config
    /// destined for another host.
    #[test]
    fn a_document_boot_would_refuse_for_an_environment_fact_still_passes() {
        let (ok, report) = check(
            "main.brenn",
            r#"
container alice {
    image = "example.com/cc:latest";
    home_dir = "/nonexistent/alice";
}
"#,
        );
        assert!(ok, "{report}");
    }

    #[test]
    fn a_valid_brenn_document_passes() {
        let (ok, report) = check(
            "main.brenn",
            &[
                SURFACE_INDEX_DECL,
                r#"
channel alerts at "brenn:alice-alerts" {
    push_depth = 8;
    retain_depth = 128;
    standing_retain_depth = 128;
}
"#,
            ]
            .concat(),
        );
        assert!(ok, "{report}");
    }

    /// A document that parses, resolves and derives, and is refused only at
    /// lowering: `noise` is a key of the subscribe tail's union vocabulary and a
    /// field no `webhook:` subscription has.
    #[test]
    fn a_lowering_only_refusal_fails_the_check() {
        let (ok, report) = check(
            "main.brenn",
            r#"
webhook push_alice {
    mount = "/webhooks/push-alice";

    signature {
        scheme = bearer-token;
        header = "authorization";
    }

    token phone { secret_file = "/home/alice/.secrets/push-alice.token"; }
}

agent Assistant() {
    grants = [subscribe];
    subscribe "webhook:push_alice" { push_depth = 4; noise = metered; }
}

new alice: Assistant();
"#,
        );
        assert!(!ok);
        // The stage matters: the document itself is well-formed, and a check
        // that stopped at compile would report this file as fine.
        assert!(report.contains("failed to lower"), "{report}");
        assert!(report.contains("noise"), "{report}");
    }

    /// The packaged module a checked document reaches for.
    const PACKAGED_SINK: &str = concat!(
        "component Sink {\n",
        "    ",
        processor_needs!("ports"),
        "\n",
        "    in messages;\n",
        "    out events;\n",
        "}\n",
    );

    /// A document reaching for a packaged module, and the module it reaches for.
    ///
    /// The module root is a sibling directory of the document, so the two are
    /// related only by the flag: nothing about the document's own location
    /// finds it.
    fn packaged_document() -> (tempfile::TempDir, PathBuf, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let modules = dir.path().join("modules");
        std::fs::create_dir(&modules).unwrap();
        std::fs::write(modules.join("sink.brenn"), PACKAGED_SINK).unwrap();
        let file = dir.path().join("main.brenn");
        std::fs::write(
            &file,
            [
                "use @sink::Sink;\n",
                SURFACE_INDEX_DECL,
                r#"
channel feed at "brenn:alice.feed" {
    push_depth = 4;
    retain_depth = 16;
    standing_retain_depth = 64;
}

channel replies at "brenn:alice.replies" {
    push_depth = 4;
    retain_depth = 16;
    standing_retain_depth = 64;
}

new alice_sink: Sink {
    grants = [ports];
    in messages <- feed;
    out events -> replies;
}
"#,
            ]
            .concat(),
        )
        .unwrap();
        (dir, file, modules)
    }

    /// The check reads no facts beyond its declared inputs, and the module root
    /// is one of them: the same document checks against whichever module tree
    /// the caller names, and is refused when it names none.
    #[test]
    fn a_document_importing_packaged_modules_checks_against_the_module_root() {
        let (_dir, file, modules) = packaged_document();
        assert!(run_config_check(&file, &[modules]));
    }

    #[test]
    fn the_same_document_without_a_module_root_is_refused_naming_the_flag() {
        let (_dir, file, _modules) = packaged_document();
        assert!(!run_config_check(&file, &[]));
        let report =
            check_config(&DocumentInputs::bare(file)).expect_err("the document must be refused");
        assert!(report.contains("pass `--modules <dir>`"), "{report}");
    }

    /// A second release's module root beside the first: the document's imports
    /// resolve in whichever holds them.
    fn second_release(dir: &Path, name: &str, module: &str) -> PathBuf {
        let root = dir.join(name);
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("bundle.brenn"), module).unwrap();
        root
    }

    const BUNDLE_MODULE: &str = r#"
component Relay {
    abi = processor;
    requires = [ports];
    in messages;
    out events;
}
"#;

    #[test]
    fn a_document_importing_from_two_roots_checks_against_both() {
        let (dir, _file, modules) = packaged_document();
        let bundle = second_release(dir.path(), "bundle", BUNDLE_MODULE);
        let file = dir.path().join("two.brenn");
        std::fs::write(
            &file,
            [
                "use @sink::Sink;\nuse @bundle::Relay;\n",
                SURFACE_INDEX_DECL,
                r#"
channel feed at "brenn:alice.feed" {
    push_depth = 4;
    retain_depth = 16;
    standing_retain_depth = 64;
}

channel replies at "brenn:alice.replies" {
    push_depth = 4;
    retain_depth = 16;
    standing_retain_depth = 64;
}

channel relayed at "brenn:alice.relayed" {
    push_depth = 4;
    retain_depth = 16;
    standing_retain_depth = 64;
}

new alice_sink: Sink {
    grants = [ports];
    in messages <- feed;
    out events -> replies;
}

new relay: Relay {
    grants = [ports];
    in messages <- replies;
    out events -> relayed;
}
"#,
            ]
            .concat(),
        )
        .unwrap();
        assert!(run_config_check(&file, &[modules.clone(), bundle.clone()]));
        // Either root alone leaves exactly the other's import unresolved.
        for (root, missing) in [(&modules, "bundle"), (&bundle, "sink")] {
            let inputs = DocumentInputs {
                root: file.clone(),
                module_roots: vec![root.clone()],
            };
            assert!(!run_config_check(&inputs.root, &inputs.module_roots));
            let report = check_config(&inputs).expect_err("one import must be unresolved");
            assert!(
                report.contains(&format!("no packaged module `{missing}`")),
                "{report}"
            );
        }
    }

    /// A second root holding a byte-identical copy of the first's module: the
    /// shape of the same release installed twice.
    fn duplicate_release(dir: &Path, modules: &Path) -> PathBuf {
        let copy = dir.join("copy");
        std::fs::create_dir(&copy).unwrap();
        std::fs::copy(modules.join("sink.brenn"), copy.join("sink.brenn")).unwrap();
        copy
    }

    #[test]
    fn a_module_installed_under_two_roots_is_refused_naming_both() {
        let (dir, file, modules) = packaged_document();
        let copy = duplicate_release(dir.path(), &modules);
        let inputs = DocumentInputs {
            root: file,
            module_roots: vec![modules.clone(), copy.clone()],
        };
        assert!(!run_config_check(&inputs.root, &inputs.module_roots));
        let report = check_config(&inputs).expect_err("the duplicate must be refused");
        assert!(
            report.contains("packaged module `sink` is installed under more than one"),
            "{report}"
        );
        assert!(
            report.contains(&modules.display().to_string())
                && report.contains(&copy.display().to_string()),
            "{report}"
        );
    }

    #[test]
    fn a_duplicate_the_document_never_imports_is_still_refused() {
        let (dir, _file, modules) = packaged_document();
        let copy = duplicate_release(dir.path(), &modules);
        let plain = dir.path().join("plain.brenn");
        std::fs::write(&plain, "const host = \"example.com\";\n").unwrap();
        assert!(run_config_check(&plain, std::slice::from_ref(&modules)));
        let inputs = DocumentInputs {
            root: plain,
            module_roots: vec![modules, copy],
        };
        assert!(!run_config_check(&inputs.root, &inputs.module_roots));
        let report = check_config(&inputs).expect_err("the duplicate must be refused");
        assert!(
            report.contains("packaged module `sink` is installed under more than one"),
            "{report}"
        );
    }

    /// The check tool reports; only boot panics.
    #[test]
    fn an_unrecognized_extension_fails_without_panicking() {
        let (ok, report) = check("brenn.conf", "nope = 1\n");
        assert!(!ok);
        assert!(report.contains("unrecognized extension"), "{report}");
    }

    /// A document that compiles and lowers clean, and is refused by a messaging
    /// gate that reads only the configuration. Returns the gate's own text.
    ///
    /// Both layers are pinned: `check_config` accepts the document (so the
    /// refusal is genuinely the offline pass's and not the front end's), and
    /// `run_config_check` refuses it (so the pass is actually wired into the
    /// verdict).
    fn boot_gate_refusal(contents: &str) -> String {
        gate_refusal(contents, &[])
    }

    /// Check a document against the shipped `config/specs` module root and
    /// return the offline pass's own refusal.
    fn shipped_module_refusal(contents: &str) -> String {
        gate_refusal(contents, &[repo_root().join("config/specs")])
    }

    /// The body of both: write `contents` to a temporary root, check it against
    /// `module_roots`, and return the offline pass's refusal text.
    fn gate_refusal(contents: &str, module_roots: &[PathBuf]) -> String {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let file = dir.path().join("main.brenn");
        std::fs::write(&file, contents).expect("the document is writable");
        let inputs = DocumentInputs {
            root: file.clone(),
            module_roots: module_roots.to_vec(),
        };
        let config = check_config(&inputs)
            .unwrap_or_else(|report| panic!("the front end must accept this document: {report}"));
        let message = offline_messaging_outcome(&config)
            .expect_err("a messaging gate must refuse this configuration");
        assert!(
            !run_config_check(&file, module_roots),
            "the offline pass refused it but the verdict passed it",
        );
        message
    }

    /// The channel, and the two classes every surface fixture below places. The
    /// widget requires exactly what its instance grants: an interface word
    /// cannot be optional, so the class states what the case gives it.
    fn preamble(widget_requires: &str) -> String {
        format!(
            r#"
channel feed at "brenn:alice.feed" {{
    push_depth = 8;
    retain_depth = 64;
    standing_retain_depth = 64;
}}

component Widget {{
    {}
    optional = [takeover];
    in messages;
    optional out takeover;
}}

component Shell {{
    {}
}}
"#,
            processor_header(widget_requires),
            processor_header("dom, page-dom"),
        )
    }

    /// Every surface needs exactly one chrome; these fixtures are about other
    /// gates, so theirs binds nothing and is granted only the page authority a
    /// chrome designation requires.
    const CHROME: &str = r#"    new shell: Shell {
        grants = [dom, page-dom];
        chrome = true;
    }
"#;

    #[test]
    fn a_takeover_binding_without_the_instance_grant_is_refused() {
        let message = boot_gate_refusal(&format!(
            r#"{preamble}
surface panel {{
    grants = [subscribe];
    new w: Widget {{
        grants = [ports, log];
        in messages <- feed {{ push_depth = 4; }}
        out takeover -> "local:brenn/takeover";
    }}
{CHROME}}}
"#,
            preamble = preamble("ports, log")
        ));
        assert!(message.contains("takeover"), "{message}");
        assert!(
            message.contains("\"w\"") || message.contains("`w`"),
            "{message}"
        );
    }

    #[test]
    fn an_instance_alert_grant_on_an_alertless_surface_is_refused() {
        let message = boot_gate_refusal(&format!(
            r#"{preamble}
surface panel {{
    grants = [subscribe];
    new w: Widget {{
        grants = [log, alert];
        in messages <- feed {{ push_depth = 4; }}
    }}
{CHROME}}}
"#,
            preamble = preamble("log, alert")
        ));
        assert!(message.contains("is granted \"alert\""), "{message}");
        assert!(
            message.contains("the surface has no `alert` grant"),
            "{message}"
        );
        assert!(message.contains("\"w\""), "{message}");
    }

    /// A `config` map with no `config` grant to read it. The per-instance `acl`
    /// containment gate is not yet wired (`TODO(surface-instance-acl-bound)`);
    /// this tests the dead-config direction of the gate that does run.
    #[test]
    fn a_config_map_without_the_config_grant_is_refused() {
        let message = boot_gate_refusal(&format!(
            r#"{preamble}
surface panel {{
    grants = [subscribe];
    new w: Widget {{
        grants = [log];
        config = {{ mode = "fanout" }};
        in messages <- feed {{ push_depth = 4; }}
    }}
{CHROME}}}
"#,
            preamble = preamble("log")
        ));
        assert!(message.contains("declares a `config` map"), "{message}");
        assert!(message.contains("\"w\""), "{message}");
    }

    /// The channel-depth asserts reach the verdict too: `build_channel_entries`
    /// runs in the offline pass, so a `[[channel]]` block whose `retain_depth`
    /// is above its own `standing_retain_depth` is refused here.
    #[test]
    fn a_retain_depth_above_standing_is_refused() {
        let message = boot_gate_refusal(
            r#"
channel feed at "brenn:alice.feed" {
    push_depth = 8;
    retain_depth = 128;
    standing_retain_depth = 16;
}
"#,
        );
        assert!(
            message.contains("exceeds standing_retain_depth"),
            "{message}"
        );
    }

    /// The error channel every surface is granted must be a `brenn:` address.
    /// The output-coverage assert has no config-check fixture: a DSL-authored
    /// surface always covers its own outputs, and the assert is pinned in
    /// `brenn-messaging-boot`'s surface tests for the raw shapes boot handles.
    #[test]
    fn a_misschemed_surface_error_channel_is_refused() {
        let message = boot_gate_refusal(
            r#"observability {
    surface_error_channel = "ephemeral:alice.errors";
}
"#,
        );
        assert!(message.contains("surface_error_channel"), "{message}");
        assert!(
            message.contains("must be a well-formed brenn: address"),
            "{message}"
        );
    }

    /// The subset-certifier direction, for the exclusions that are whole config
    /// families rather than one block: a webhook secret, an mqtt client's
    /// password and CA, and a consumer's component artifact are all environment
    /// facts this machine does not hold, and none of them may decide the
    /// verdict. The offline pass also sees fewer channels than boot does — no
    /// `webhook:` or `mqtt:` directory entries — which can only make it more
    /// permissive, never stricter.
    #[test]
    fn a_config_full_of_environment_coupled_blocks_still_passes() {
        let (ok, report) = check(
            "main.brenn",
            &[
                SURFACE_INDEX_DECL,
                r#"
channel feed at "brenn:alice.feed" {
    push_depth = 8;
    retain_depth = 64;
    standing_retain_depth = 64;
}

webhook push_alice {
    mount = "/webhooks/push-alice";
    signature {
        scheme = bearer-token;
        header = "authorization";
    }
    token phone { secret_file = "/nonexistent/alice/push-alice.token"; }
}

mqtt_client broker {
    url = "mqtts://broker.example.com:8883";
    username = "alice";
    password_file = "/nonexistent/alice/broker.password";
    ca_file = "/nonexistent/alice/broker-ca.pem";
}

// ── packaged ──
component Sink {
    "#,
                processor_needs!(""),
                r#"
    in inbound;
}
// ── packaged ──

new sink: Sink {
    slug = "sink";
    grants = [];

    in inbound <- feed { push_depth = 4; }
}
"#,
            ]
            .concat(),
        );
        assert!(ok, "{report}");
    }

    /// A `[[remote]]` names a token file this machine does not have, and the
    /// check still passes: the token load is the one `[[remote]]` step the
    /// offline pass excludes, because it reads the deployment host's disk. A
    /// workstation checking a config destined for another host must not fail on
    /// the other host's secrets.
    #[test]
    fn a_remotes_unreadable_token_file_does_not_decide_the_verdict() {
        let (ok, report) = check(
            "main.brenn",
            &[
                SURFACE_INDEX_DECL,
                r#"
channel out at "brenn:alice.out" {
    push_depth = 8;
    retain_depth = 64;
    standing_retain_depth = 64;
}

remote pod {
    token_file = "/nonexistent/alice/remote-pod.token";
    grants = [publish];
    acl publish [prefix "brenn:alice."];
}
"#,
            ]
            .concat(),
        );
        assert!(ok, "{report}");
    }

    /// Every `[[remote]]` gate above the token load does decide the verdict:
    /// a remote admitted to no session at all is dead config, and the refusal
    /// reaches an operator before the deploy rather than from the service dying
    /// after it.
    #[test]
    fn a_remotes_dead_session_ceiling_is_refused() {
        let message = boot_gate_refusal(
            r#"
channel out at "brenn:alice.out" {
    push_depth = 8;
    retain_depth = 64;
    standing_retain_depth = 64;
}

remote pod {
    token_file = "/nonexistent/alice/remote-pod.token";
    max_sessions = 0;
    grants = [publish];
    acl publish [prefix "brenn:alice."];
}
"#,
        );
        assert!(message.contains("max_sessions must be >= 1"), "{message}");
        assert!(message.contains("\"pod\""), "{message}");
    }

    /// The shipped configs pass the strengthened check. Without the offline
    /// messaging pass, a config that lowers clean but panics the service at boot
    /// is reported `ok`.
    fn shipped_config(filename: &str) {
        let root = repo_root();
        let specs = root.join("config/specs");
        assert!(
            run_config_check(&root.join(filename), std::slice::from_ref(&specs)),
            "{filename} must pass config-check"
        );
        // The outcome, not the mechanism: a shipped root that stamps the
        // description module bare must also come back with nothing to advise, so
        // an error-lane retention default that stopped clearing the surface send
        // burst — from either side — is a red test here rather than a warning on
        // every dev boot.
        let config = check_config(&DocumentInputs::with_modules(root.join(filename), specs))
            .unwrap_or_else(|report| panic!("{filename} must compile: {report}"));
        let advisories = offline_messaging_outcome(&config)
            .unwrap_or_else(|message| panic!("{filename} must pass the offline pass: {message}"));
        assert!(
            advisories.is_empty(),
            "{filename} must have nothing to advise: {advisories:?}",
        );
    }

    #[test]
    fn brenn_dev_brenn_passes_the_strengthened_check() {
        shipped_config("brenn.dev.brenn");
    }

    #[test]
    fn brenn_e2e_brenn_passes_the_strengthened_check() {
        shipped_config("brenn.e2e.brenn");
    }

    /// Each component's `spec_sha256` must match `config/specs/<kind>.brenn`.
    ///
    /// No automated suite boots the real asset tree, so a spec that drifted
    /// would pass every gate and refuse to start on the deploy host. This
    /// checks the hermetic half: config against specification sources, no
    /// built artifacts.
    fn shipped_config_binds_to_its_packaged_specs(filename: &str) {
        let root = repo_root();
        let inputs = DocumentInputs::with_modules(root.join(filename), root.join("config/specs"));
        let config = match check_config(&inputs) {
            Ok(config) => config,
            Err(report) => panic!("{filename} must compile: {report}"),
        };
        let mut checked = 0;
        for surface in &config.surfaces {
            for component in &surface.components {
                let spec = root.join(format!("config/specs/{}.brenn", component.kind));
                let bytes = std::fs::read(&spec).unwrap_or_else(|e| {
                    panic!(
                        "{filename} mounts kind {} but {} is unreadable: {e}",
                        component.kind,
                        spec.display(),
                    )
                });
                assert_eq!(
                    component.spec_sha256,
                    brenn_lib::util::sha256_hex(&bytes),
                    "{filename} declares kind {} in a file other than {}; the build packages \
                     that file and boot binds this hash to it",
                    component.kind,
                    spec.display(),
                );
                checked += 1;
            }
        }
        assert!(checked > 0, "{filename} mounts no surface components");
    }

    #[test]
    fn brenn_dev_brenn_binds_to_its_packaged_specs() {
        shipped_config_binds_to_its_packaged_specs("brenn.dev.brenn");
    }

    #[test]
    fn brenn_e2e_brenn_binds_to_its_packaged_specs() {
        shipped_config_binds_to_its_packaged_specs("brenn.e2e.brenn");
    }

    fn repo_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
    }

    fn compiled_against_shipped_modules(contents: &str) -> BrennConfig {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let root = dir.path().join("stamped.brenn");
        std::fs::write(&root, contents).expect("the document is writable");
        let inputs = DocumentInputs {
            root,
            module_roots: vec![repo_root().join("config/specs")],
        };
        match check_config(&inputs) {
            Ok(config) => config,
            Err(report) => panic!("the document must compile: {report}"),
        }
    }

    fn depths(
        config: &BrennConfig,
        address: &str,
    ) -> (Option<Depth>, Option<Depth>, Option<Depth>) {
        let channel = config
            .channels
            .iter()
            .find(|c| c.address.as_deref() == Some(address))
            .unwrap_or_else(|| {
                let declared: Vec<&str> = config
                    .channels
                    .iter()
                    .filter_map(|c| c.address.as_deref())
                    .collect();
                panic!("no channel at {address}; the document declares {declared:?}")
            });
        (
            channel.push_depth,
            channel.retain_depth,
            channel.standing_retain_depth,
        )
    }

    /// Every stamp of `surface-description` in either repository is bare, so
    /// every gate over one compiles the defaults — which are the literals the
    /// parameters replaced. A parameter bound to the wrong attr, or to the wrong
    /// channel of the pair, changes nothing any of those gates observes, and its
    /// first reader is prod's stamp, where the symptom is a silently mis-sized
    /// retention window rather than a refusal. So the module is stamped here with
    /// a distinct number per position: a transposition cannot pass.
    #[test]
    fn the_shipped_description_module_binds_each_depth_parameter_to_its_own_position() {
        let config = compiled_against_shipped_modules(
            "use @surface-description::*;\n\
             \n\
             new commons: SurfaceCommons(errors_retain = 7, errors_standing = 9);\n\
             new desc: SurfaceDescription(slug = \"s\", geometry_retain = 11, status_retain = 240);\n",
        );
        assert_eq!(
            depths(&config, "brenn:surface-errors"),
            (
                Some(Depth::Bounded(1)),
                Some(Depth::Bounded(7)),
                Some(Depth::Bounded(9)),
            ),
        );
        assert_eq!(
            depths(&config, "brenn:surface.surface.s.geometry"),
            (
                Some(Depth::Bounded(1)),
                Some(Depth::Bounded(11)),
                Some(Depth::Bounded(11)),
            ),
        );
        assert_eq!(
            depths(&config, "brenn:surface.surface.s.status"),
            (
                Some(Depth::Bounded(1)),
                Some(Depth::Bounded(240)),
                Some(Depth::Bounded(240)),
            ),
        );
    }

    /// The front end's own detector for a forgotten `SurfaceCommons` stamp, in a
    /// deployment that pins its durable uuids: a pin whose address no channel
    /// declares is refused, and the two commons addresses are pinned here. This
    /// refuses before the offline pass runs, which is why the report names both
    /// addresses rather than the one `validate_surface_description_set` derives.
    #[test]
    fn removing_the_surface_commons_stamp_from_a_shipped_root_is_refused() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        std::fs::create_dir(dir.path().join("config")).expect("the tree is writable");
        std::fs::copy(
            repo_root().join("config/bar.brenn"),
            dir.path().join("config/bar.brenn"),
        )
        .expect("the imported module is readable");

        let stamp = "new surface_commons: SurfaceCommons;";
        let shipped =
            std::fs::read_to_string(repo_root().join("brenn.dev.brenn")).expect("a shipped root");
        assert!(
            shipped.contains(stamp),
            "brenn.dev.brenn no longer stamps SurfaceCommons; this case removes that line",
        );
        let root = dir.path().join("brenn.dev.brenn");
        std::fs::write(&root, shipped.replace(stamp, "")).expect("the document is writable");

        let inputs = DocumentInputs {
            root,
            module_roots: vec![repo_root().join("config/specs")],
        };
        let report = check_config(&inputs).expect_err("the pins name channels nothing declares");
        assert!(
            report.contains("brenn:surface-errors") && report.contains("brenn:surface.index"),
            "the refusal must name both commons addresses: {report}",
        );
    }

    /// The other half: the defaults the four in-repository documents take by
    /// stamping the module bare.
    #[test]
    fn a_bare_stamp_of_the_description_module_takes_the_shipped_defaults() {
        let config = compiled_against_shipped_modules(
            "use @surface-description::*;\n\
             \n\
             new commons: SurfaceCommons;\n\
             new desc: SurfaceDescription(slug = \"s\");\n",
        );
        assert_eq!(
            depths(&config, "brenn:surface-errors"),
            (
                Some(Depth::Bounded(1)),
                Some(Depth::Bounded(100)),
                Some(Depth::Bounded(1024)),
            ),
        );
        let (_, _, standing) = depths(&config, "brenn:surface-errors");
        let burst = u64::from(brenn_messaging::publish::SURFACE_SEND_BURST);
        assert!(
            matches!(standing, Some(Depth::Bounded(n)) if n > burst),
            "the shipped standing default must clear the send burst it is sized \
             against, or a bare stamp warns at every boot: {standing:?} vs {burst}",
        );
        assert_eq!(
            depths(&config, "brenn:surface.surface.s.geometry"),
            (
                Some(Depth::Bounded(1)),
                Some(Depth::Bounded(1)),
                Some(Depth::Bounded(1)),
            ),
        );
        assert_eq!(
            depths(&config, "brenn:surface.surface.s.status"),
            (
                Some(Depth::Bounded(1)),
                Some(Depth::Bounded(1)),
                Some(Depth::Bounded(1)),
            ),
        );
    }

    /// A root declaring one surface — two kinds, `widget` and `shell` — against
    /// the shipped description module. `preface` carries the case's top-level
    /// blocks and `stamps` the self-description stamps it chooses to write; what
    /// each case leaves out is the point.
    fn described_root(preface: &str, stamps: &str) -> String {
        format!(
            r#"use @surface-description::*;

surface_description {{
    prefix = "surface";
    status_interval_secs = 60;
}}
{preface}
{stamps}
{preamble}
surface panel {{
    grants = [subscribe];
    new w: Widget {{
        grants = [log];
        in messages <- feed {{ push_depth = 4; }}
    }}
{CHROME}}}
"#,
            preamble = preamble("log"),
        )
    }

    /// Both stamps of the surface's two kinds, which every case below wants.
    const KIND_STAMPS: &str = "new widget_desc: KindDescription(kind = \"widget\");\n\
                               new shell_desc: KindDescription(kind = \"shell\");";

    /// The accepting direction of the four refusal cases below: `described_root`
    /// with every stamp written is accepted. Without it a defect in the shared
    /// helper — a kind declared but never placed, a grant that stopped matching,
    /// a stamp name resolving to nothing — would leave all four refusing for a
    /// reason other than the one each is about, with every assertion still
    /// passing.
    #[test]
    fn a_fully_stamped_described_root_is_accepted() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let file = dir.path().join("main.brenn");
        std::fs::write(
            &file,
            described_root(
                "",
                &format!(
                    "new commons: SurfaceCommons;\n\
                     new panel_desc: SurfaceDescription(slug = \"panel\");\n{KIND_STAMPS}"
                ),
            ),
        )
        .expect("the document is writable");
        assert!(
            run_config_check(&file, &[repo_root().join("config/specs")]),
            "the stamps each case below omits are the whole of what it owes",
        );
    }

    /// The order the two validators run in is boot's, and a caught unwind is one
    /// message: a document missing the error lane's channel *and* the whole
    /// description set is refused for the error lane, as it is at boot.
    #[test]
    fn the_error_lane_refusal_comes_before_the_description_set() {
        let message = shipped_module_refusal(&described_root(
            r#"
observability {
    surface_error_channel = "brenn:surface-errors";
}
"#,
            &format!("new panel_desc: SurfaceDescription(slug = \"panel\");\n{KIND_STAMPS}"),
        ));
        assert!(message.contains("brenn:surface-errors"), "{message}");
        assert!(!message.contains("brenn:surface.index"), "{message}");
    }

    /// A deployment that pins no uuids and forgets the commons stamp: refused by
    /// the set validator, offline, naming the index it never declared.
    #[test]
    fn a_missing_commons_stamp_is_refused_naming_the_index() {
        let message = shipped_module_refusal(&described_root(
            "",
            &format!("new panel_desc: SurfaceDescription(slug = \"panel\");\n{KIND_STAMPS}"),
        ));
        assert!(message.contains("brenn:surface.index"), "{message}");
        assert!(!message.contains("surface.surface.panel"), "{message}");
    }

    /// A declared surface with no `SurfaceDescription` stamp: the refusal names
    /// all four of that slug's addresses at once, which is what an operator who
    /// forgot one stamp needs.
    #[test]
    fn a_missing_surface_description_stamp_is_refused_naming_the_slugs_channels() {
        let message = shipped_module_refusal(&described_root(
            "",
            &format!("new commons: SurfaceCommons;\n{KIND_STAMPS}"),
        ));
        for address in [
            "brenn:surface.surface.panel.help",
            "brenn:surface.surface.panel.geometry",
            "brenn:surface.surface.panel.status",
            "ephemeral:surface.surface.panel.bindings",
        ] {
            assert!(
                message.contains(address),
                "{address} missing from {message}"
            );
        }
    }

    /// The per-kind half: a placed kind with no `KindDescription` stamp is
    /// refused naming its help and schema documents.
    #[test]
    fn a_missing_kind_description_stamp_is_refused_naming_the_kinds_channels() {
        let message = shipped_module_refusal(&described_root(
            "",
            "new commons: SurfaceCommons;\n\
             new panel_desc: SurfaceDescription(slug = \"panel\");\n\
             new shell_desc: KindDescription(kind = \"shell\");",
        ));
        assert!(
            message.contains("brenn:surface.kind.widget.help"),
            "{message}"
        );
        assert!(
            message.contains("brenn:surface.kind.widget.schema"),
            "{message}"
        );
        assert!(!message.contains("brenn:surface.kind.shell."), "{message}");
    }

    /// The `None` arm: a document that activates no messaging is handed no
    /// directory, so the set validator has nothing to require. Without this the
    /// offline pass would refuse a const-only document for lacking an index
    /// boot never asks it for.
    #[test]
    fn a_document_with_no_messaging_owes_no_description_channels() {
        let (ok, report) = check("main.brenn", "const host = \"example.com\";\n");
        assert!(ok, "{report}");
    }

    /// A single `[[channel]]` activates messaging, so boot builds a directory
    /// and requires `brenn:surface.index` — surface or no surface. This pass
    /// refuses that document before the installer stops the service.
    #[test]
    fn a_channel_only_document_owes_the_surface_index() {
        let message = boot_gate_refusal(
            r#"
channel alerts at "brenn:alice-alerts" {
    push_depth = 8;
    retain_depth = 128;
    standing_retain_depth = 128;
}
"#,
        );
        assert!(message.contains("brenn:surface.index"), "{message}");
    }

    /// The advisory path. The error lane's eviction frontier sits at the surface
    /// send burst, which is advice and not a refusal — so the document passes,
    /// and the advice comes back to be printed. Nothing on this path has a
    /// `tracing` subscriber, so a warning logged inside the validator would be
    /// dropped and the operator running the gate before a deploy would read
    /// nothing.
    #[test]
    fn an_eviction_frontier_at_the_send_burst_is_advice_the_check_carries_back() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let file = dir.path().join("main.brenn");
        std::fs::write(
            &file,
            [
                SURFACE_INDEX_DECL,
                r#"
observability {
    surface_error_channel = "brenn:surface-errors";
}

channel surface_errors at "brenn:surface-errors" {
    push_depth = 8;
    retain_depth = 64;
    standing_retain_depth = 256;
}
"#,
            ]
            .concat(),
        )
        .expect("the document is writable");
        let config = check_config(&DocumentInputs::bare(&file))
            .unwrap_or_else(|report| panic!("the front end must accept this document: {report}"));
        let advisories = offline_messaging_outcome(&config).expect("the document must pass");
        assert_eq!(advisories.len(), 1, "{advisories:?}");
        assert!(
            advisories[0].contains("eviction frontier"),
            "{}",
            advisories[0],
        );
        assert!(run_config_check(&file, &[]), "advice is not a refusal");
    }

    /// The error-lane validator's `[messaging]` refusal, which rides along
    /// offline because the function is one unit: a `max_body_bytes` under the
    /// worst-case report body is a refusal here rather than a re-panicked host
    /// bug, which is what the `config: ` spelling buys.
    #[test]
    fn a_max_body_bytes_below_the_error_report_floor_is_a_refusal() {
        let message = shipped_module_refusal(
            r#"use @surface-description::*;

observability {
    surface_error_channel = "brenn:surface-errors";
}

messaging {
    max_body_bytes = 1024;
}

new commons: SurfaceCommons;
"#,
        );
        assert!(message.starts_with("config: [messaging]"), "{message}");
        assert!(
            message.contains("worst-case surface error report body"),
            "{message}"
        );
    }
}
