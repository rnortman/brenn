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
//! inputs: the root document, and the module root its packaged imports resolve
//! against.

use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::path::Path;

use brenn_lib::config::{BrennConfig, check_config};
use brenn_lib::panic_util::{catch_quietly, panic_message};
use brenn_messaging_boot::resolve_messaging_offline;

/// Check one config file, print the verdict. Returns whether it would load.
///
/// Strictly stronger than [`check_config`]: a document that compiles and lowers
/// clean can still be refused here, by a messaging gate that reads only the
/// configuration.
pub fn run_config_check(file: &Path, module_root: Option<&Path>) -> bool {
    let config = match check_config(file, module_root) {
        Ok(config) => config,
        Err(report) => {
            eprintln!("{report}");
            return false;
        }
    };
    match offline_messaging_refusal(&config) {
        None => {
            println!("{}: ok", file.display());
            true
        }
        Some(message) => {
            eprintln!(
                "failed to resolve messaging in config file {}:\n{message}",
                file.display(),
            );
            false
        }
    }
}

/// Run the offline messaging resolution, returning the refusal text if it
/// refused.
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
fn offline_messaging_refusal(config: &BrennConfig) -> Option<String> {
    catch_quietly(AssertUnwindSafe(|| resolve_messaging_offline(config)))
        .err()
        .map(refusal_text)
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
const REFUSAL_PREFIXES: [&str; 3] = ["config: ", "surface \"", "remote \""];

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

    use brenn_dsl::{dom_any, processor_any};

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
        let (file, module_root) = brenn_lib::config::stage_fixture(dir.path(), name, contents);
        let module_root = module_root.as_deref();
        let ok = run_config_check(&file, module_root);
        let config = check_config(&file, module_root);
        assert!(
            config.is_ok() || !ok,
            "the report refused the document but the verdict passed it",
        );
        (ok, config.err().unwrap_or_default())
    }

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
            r#"
channel alerts at "brenn:alice-alerts" {
    push_depth = 8;
    retain_depth = 128;
    standing_retain_depth = 128;
}
"#,
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
        processor_any!(),
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
            r#"use @sink::Sink;

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
        assert!(run_config_check(&file, Some(&modules)));
    }

    #[test]
    fn the_same_document_without_a_module_root_is_refused_naming_the_flag() {
        let (_dir, file, _modules) = packaged_document();
        assert!(!run_config_check(&file, None));
        let report = check_config(&file, None).expect_err("the document must be refused");
        assert!(report.contains("pass `--modules <dir>`"), "{report}");
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
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("main.brenn");
        std::fs::write(&file, contents).unwrap();
        let config = check_config(&file, None)
            .unwrap_or_else(|report| panic!("the front end must accept this document: {report}"));
        let message = offline_messaging_refusal(&config)
            .expect("a messaging gate must refuse this configuration");
        assert!(
            !run_config_check(&file, None),
            "the offline pass refused it but the verdict passed it",
        );
        message
    }

    const PREAMBLE: &str = concat!(
        r#"
channel feed at "brenn:alice.feed" {
    push_depth = 8;
    retain_depth = 64;
    standing_retain_depth = 64;
}

component Widget {
    "#,
        dom_any!(),
        r#"
    in messages;
    optional out takeover;
}

component Shell {
    "#,
        dom_any!(),
        r#"
}
"#
    );

    /// Every surface needs exactly one chrome; these fixtures are about other
    /// gates, so theirs binds nothing and is granted nothing.
    const CHROME: &str = r#"    new shell: Shell {
        grants = [];
        chrome = true;
    }
"#;

    #[test]
    fn a_takeover_binding_without_the_instance_grant_is_refused() {
        let message = boot_gate_refusal(&format!(
            r#"{PREAMBLE}
surface panel {{
    grants = [subscribe];
    new w: Widget {{
        grants = [ports, log];
        in messages <- feed {{ push_depth = 4; }}
        out takeover -> "local:brenn/takeover";
    }}
{CHROME}}}
"#
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
            r#"{PREAMBLE}
surface panel {{
    grants = [subscribe];
    new w: Widget {{
        grants = [log, alert];
        in messages <- feed {{ push_depth = 4; }}
    }}
{CHROME}}}
"#
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
            r#"{PREAMBLE}
surface panel {{
    grants = [subscribe];
    new w: Widget {{
        grants = [log];
        config = {{ mode = "fanout" }};
        in messages <- feed {{ push_depth = 4; }}
    }}
{CHROME}}}
"#
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
            concat!(
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
                processor_any!(),
                r#"
    in inbound;
}
// ── packaged ──

new sink: Sink {
    slug = "sink";
    grants = [];

    in inbound <- feed { push_depth = 4; }
}
"#
            ),
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
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        assert!(
            run_config_check(&root.join(filename), Some(&root.join("config/specs"))),
            "{filename} must pass config-check"
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
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let config = match check_config(&root.join(filename), Some(&root.join("config/specs"))) {
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
}
