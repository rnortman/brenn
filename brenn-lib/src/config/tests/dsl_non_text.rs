//! Lowering `non_text`: on a channel declaration, on each consumer binding
//! tail and the link endpoints those bindings present, and its refusal on
//! every surface binding.

use brenn_dsl::{processor_needs, surface_any};

use crate::config::{config_from_dsl, sole_refusal};
use crate::messaging::config::LinkHostRaw;

/// A channel says whether it may carry non-text bodies; saying nothing is
/// text-only.
#[test]
fn a_channel_declaration_lowers_its_non_text() {
    let config = config_from_dsl(
        r#"
channel frames at "ephemeral:alice-cam.frames" {
    push_depth = 1;
    retain_depth = 4;
    non_text = true;
}

channel notes at "ephemeral:alice-cam.notes" {
    push_depth = 1;
    retain_depth = 4;
}

channel captions at "ephemeral:alice-cam.captions" {
    push_depth = 1;
    retain_depth = 4;
    non_text = false;
}
"#,
    );
    let non_text: Vec<(Option<&str>, bool)> = config
        .channels
        .iter()
        .map(|channel| (channel.address.as_deref(), channel.non_text))
        .collect();
    assert_eq!(
        non_text,
        vec![
            (Some("ephemeral:alice-cam.frames"), true),
            (Some("ephemeral:alice-cam.notes"), false),
            (Some("ephemeral:alice-cam.captions"), false),
        ]
    );
}

#[test]
fn a_non_text_that_is_not_a_boolean_is_refused() {
    let refusal = sole_refusal(
        r#"
channel frames at "ephemeral:alice-cam.frames" {
    push_depth = 1;
    retain_depth = 4;
    non_text = "yes";
}
"#,
    );
    assert!(
        refusal.message.contains("non_text"),
        "the refusal names the key: {}",
        refusal.message
    );
}

/// Each consumer tail carries `non_text` onto the raw record its direction
/// lowers to: an `in`, an `out`, an `io` bound to a declared channel (a
/// subscription and an output sharing a name), a free `io` and an `io` on a
/// literal address (each an io port). A tail that says nothing is `false`.
#[test]
fn every_consumer_tail_lowers_its_non_text() {
    let document = format!(
        concat!(
            r#"
channel frames at "ephemeral:alice-cam.frames" {{
    push_depth = 1;
    retain_depth = 4;
    non_text = true;
}}

channel notes at "ephemeral:alice-cam.notes" {{
    push_depth = 1;
    retain_depth = 4;
}}

// ── packaged ──
component Camera {{
    {}
    in frames_in;
    in notes_in;
    out frames_out;
    out notes_out;
    io shared;
    io state;
    io scratch;
    io spool;
}}
// ── packaged ──

new camera: Camera {{
    grants = [ports];

    in frames_in <- frames {{ push_depth = 1; retain_depth = 2; non_text = true; }}
    in notes_in <- notes {{ push_depth = 1; retain_depth = 2; }}
    out frames_out -> frames {{ non_text = true; }}
    out notes_out -> notes;
    io shared <-> frames {{ push_depth = 1; retain_depth = 2; non_text = true; }}
    io state {{ push_depth = 0; retain_depth = 1; non_text = true; }}
    io scratch {{ push_depth = 1; retain_depth = 2; }}
    io spool <-> "local:alice-cam.spool" {{ push_depth = 1; retain_depth = 2; non_text = true; }}
}}
"#
        ),
        processor_needs!("ports"),
    );
    let config = config_from_dsl(&document);
    let consumer = &config.wasm_consumers[0];
    let subscriptions: Vec<(&str, bool)> = consumer
        .subscriptions
        .iter()
        .map(|sub| (sub.port.as_str(), sub.non_text))
        .collect();
    assert_eq!(
        subscriptions,
        vec![("frames_in", true), ("notes_in", false), ("shared", true)]
    );
    let outputs: Vec<(&str, bool)> = consumer
        .outputs
        .iter()
        .map(|out| (out.port.as_str(), out.non_text))
        .collect();
    assert_eq!(
        outputs,
        vec![("frames_out", true), ("notes_out", false), ("shared", true)]
    );
    let io_ports: Vec<(&str, Option<&str>, bool)> = consumer
        .io_ports
        .iter()
        .map(|io| (io.port.as_str(), io.channel.as_deref(), io.non_text))
        .collect();
    assert_eq!(
        io_ports,
        vec![
            ("state", None, true),
            ("scratch", None, false),
            ("spool", Some("local:alice-cam.spool"), true),
        ]
    );
}

/// A link endpoint carries what its binding's tail said, so the boot fold can
/// make the link's channel one that may carry non-text bodies.
#[test]
fn a_link_endpoint_carries_its_bindings_non_text() {
    let document = format!(
        concat!(
            "// ── packaged ──\n",
            "component Camera {{\n{}\n    out frames;\n}}\n",
            "component Recorder {{\n{}\n    in frames;\n}}\n",
            "// ── packaged ──\n",
            "link relay;\n",
            "new camera: Camera {{\n",
            "    grants = [ports];\n",
            "    out frames -> relay {{ non_text = true; }}\n",
            "}}\n",
            "new recorder: Recorder {{\n",
            "    grants = [];\n",
            "    in frames <- relay {{ push_depth = 1; retain_depth = 4; }}\n",
            "}}\n",
        ),
        processor_needs!("ports"),
        processor_needs!(""),
    );
    let config = config_from_dsl(&document);
    let endpoints: Vec<(String, bool)> = config.links[0]
        .endpoints
        .iter()
        .map(|endpoint| {
            let LinkHostRaw::Wasm { slug } = &endpoint.host else {
                panic!("every endpoint here is a backend port");
            };
            (format!("{slug}/{}", endpoint.port), endpoint.non_text)
        })
        .collect();
    assert_eq!(
        endpoints,
        vec![
            ("camera/frames".to_string(), true),
            ("recorder/frames".to_string(), false),
        ]
    );
}

/// The refusal every surface binding draws for `non_text`, whatever its
/// direction.
fn surface_refusal(port: &str) -> String {
    format!(
        "the `{port}` port of instance `widget` of surface `alice_pod` states `non_text`: a \
         surface port carries only text — the surface host serves the string component \
         contract and the page's wire carries text frames — so it can be bound to no channel \
         that may carry non-text bodies"
    )
}

#[test]
fn non_text_on_a_surface_in_binding_is_refused() {
    let refusal = sole_refusal(concat!(
        r#"
channel utterance at "ephemeral:alice-pod.utterance" {
    push_depth = 4;
    retain_depth = 16;
}

component Widget {
    "#,
        surface_any!(),
        r#"
    in heard;
}

surface alice_pod {
    grants = [subscribe];

    new widget: Widget {
        chrome = true;
        grants = [];
        in heard <- utterance { push_depth = 2; non_text = true; }
    }
}
"#
    ));
    assert_eq!(refusal.message, surface_refusal("heard"));
    assert_eq!(
        refusal.line_col(),
        Some((18, 60)),
        "the span is the refused key's own value: {}",
        refusal.render()
    );
}

#[test]
fn non_text_on_a_surface_out_binding_is_refused() {
    let refusal = sole_refusal(concat!(
        r#"
channel utterance at "ephemeral:alice-pod.utterance" {
    push_depth = 4;
    retain_depth = 16;
}

component Widget {
    "#,
        processor_needs!("ports"),
        r#"
    out said;
}

surface alice_pod {
    grants = [publish];

    new widget: Widget {
        chrome = true;
        grants = [ports];
        out said -> utterance { non_text = true; }
    }
}
"#
    ));
    assert_eq!(refusal.message, surface_refusal("said"));
}

/// A surface `io` with no channel is page-local: the tail is the only place
/// such a channel could be declared, so it is refused there too.
#[test]
fn non_text_on_a_surface_io_binding_is_refused() {
    let refusal = sole_refusal(concat!(
        r#"
component Widget {
    "#,
        processor_needs!("ports"),
        r#"
    io state;
}

surface alice_pod {
    grants = [];

    new widget: Widget {
        chrome = true;
        grants = [ports];
        io state { push_depth = 0; retain_depth = 1; non_text = true; }
    }
}
"#
    ));
    assert_eq!(refusal.message, surface_refusal("state"));
}
