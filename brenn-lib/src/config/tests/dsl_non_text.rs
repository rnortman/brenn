//! Lowering `non_text`: on a channel declaration, on each consumer binding
//! tail and the link endpoints those bindings present, and on surface binding
//! tails — carried on every address but a reserved plane, where it is refused.

use brenn_dsl::{processor_needs, surface_any};

use crate::config::{config_from_dsl, sole_refusal};
use crate::messaging::config::{LinkHostRaw, SurfaceConfigRaw};

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

/// Each surface tail carries `non_text` where the port's channel is
/// page-local: an `in` and an `out` on a literal `local:` address, a free `io`,
/// an `io` on a handle to a `local:` declaration (both halves of the pair),
/// and a link-bound port, whose endpoint carries it too. A tail that says
/// nothing is `false`.
#[test]
fn every_surface_tail_carries_its_non_text_where_the_channel_is_page_local() {
    let document = format!(
        concat!(
            r#"
channel scratch at "local:alice-pod.scratch" {{
    push_depth = 1;
    retain_depth = 4;
}}

// ── packaged ──
component Widget {{
    {}
    in heard;
    in quiet;
    out said;
    io state;
    io shared;
    out relayed;
}}
component Gauge {{
    {}
    in relayed;
}}
// ── packaged ──

link relay;

surface alice_pod {{
    grants = [];

    new widget: Widget {{
        chrome = true;
        grants = [ports];
        in heard <- "local:alice-pod.frames" {{ push_depth = 1; non_text = true; }}
        in quiet <- "local:alice-pod.notes" {{ push_depth = 1; }}
        out said -> "local:alice-pod.frames" {{ non_text = true; }}
        io state {{ push_depth = 0; retain_depth = 1; non_text = true; }}
        io shared <-> scratch {{ push_depth = 1; retain_depth = 2; non_text = true; }}
        out relayed -> relay {{ non_text = true; }}
    }}
    new gauge: Gauge {{
        chrome = false;
        grants = [];
        in relayed <- relay {{ push_depth = 1; retain_depth = 1; }}
    }}
}}
"#
        ),
        processor_needs!("ports"),
        processor_needs!(""),
    );
    let config = config_from_dsl(&document);
    let surface: &SurfaceConfigRaw = &config.surfaces[0];
    let subscriptions: Vec<(&str, &str, bool)> = surface
        .subscriptions
        .iter()
        .map(|sub| (sub.instance.as_str(), sub.port.as_str(), sub.non_text))
        .collect();
    assert_eq!(
        subscriptions,
        vec![
            ("widget", "heard", true),
            ("widget", "quiet", false),
            ("widget", "shared", true),
            ("gauge", "relayed", false),
        ]
    );
    let outputs: Vec<(&str, bool)> = surface
        .outputs
        .iter()
        .map(|out| (out.port.as_str(), out.non_text))
        .collect();
    assert_eq!(
        outputs,
        vec![("said", true), ("shared", true), ("relayed", true)]
    );
    let io_ports: Vec<(&str, bool)> = surface
        .io_ports
        .iter()
        .map(|io| (io.port.as_str(), io.non_text))
        .collect();
    assert_eq!(io_ports, vec![("state", true)]);
    let endpoints: Vec<(String, bool)> = config.links[0]
        .endpoints
        .iter()
        .map(|endpoint| {
            let LinkHostRaw::Surface { instance, .. } = &endpoint.host else {
                panic!("every endpoint here is a surface port");
            };
            (format!("{instance}/{}", endpoint.port), endpoint.non_text)
        })
        .collect();
    assert_eq!(
        endpoints,
        vec![
            ("widget/relayed".to_string(), true),
            ("gauge/relayed".to_string(), false),
        ]
    );
}

/// Lowering sees an address and not a declaration, so a surface port's
/// `non_text` on a transportable address — an `in`, an `out` and an `io` on a
/// handle, each to an `ephemeral:` or `brenn:` channel — is carried to the
/// planner, which judges it against the channel's declaration.
#[test]
fn every_surface_tail_carries_its_non_text_on_a_transportable_address() {
    let document = format!(
        concat!(
            r#"
channel utterance at "ephemeral:alice-pod.utterance" {{
    push_depth = 4;
    retain_depth = 16;
    non_text = true;
}}

channel said at "brenn:alice-pod.said" {{
    push_depth = 4;
    retain_depth = 16;
    standing_retain_depth = 16;
}}

channel state at "ephemeral:alice-pod.state" {{
    push_depth = 1;
    retain_depth = 1;
    non_text = true;
}}

// ── packaged ──
component Widget {{
    {}
    in heard;
    out said;
    io state;
}}
// ── packaged ──

surface alice_pod {{
    grants = [subscribe, publish];

    new widget: Widget {{
        chrome = true;
        grants = [ports];
        in heard <- utterance {{ push_depth = 2; non_text = true; }}
        out said -> said {{ non_text = true; }}
        io state <-> state {{ push_depth = 1; retain_depth = 1; non_text = true; }}
    }}
}}
"#
        ),
        processor_needs!("ports"),
    );
    let config = config_from_dsl(&document);
    let surface: &SurfaceConfigRaw = &config.surfaces[0];
    let subscriptions: Vec<(&str, Option<&str>, bool)> = surface
        .subscriptions
        .iter()
        .map(|sub| (sub.port.as_str(), sub.channel.as_deref(), sub.non_text))
        .collect();
    assert_eq!(
        subscriptions,
        vec![
            ("heard", Some("ephemeral:alice-pod.utterance"), true),
            ("state", Some("ephemeral:alice-pod.state"), true),
        ]
    );
    let outputs: Vec<(&str, Option<&str>, bool)> = surface
        .outputs
        .iter()
        .map(|out| (out.port.as_str(), out.channel.as_deref(), out.non_text))
        .collect();
    assert_eq!(
        outputs,
        vec![
            ("said", Some("brenn:alice-pod.said"), true),
            ("state", Some("ephemeral:alice-pod.state"), true),
        ]
    );
}

#[test]
fn non_text_on_a_reserved_plane_is_refused() {
    let refusal = sole_refusal(concat!(
        r#"
component Widget {
    "#,
        surface_any!(),
        r#"
    in theme;
}

surface alice_pod {
    grants = [];

    new widget: Widget {
        chrome = true;
        grants = [];
        in theme <- "local:brenn/theme" { push_depth = 1; non_text = true; }
    }
}
"#
    ));
    assert_eq!(
        refusal.message,
        "the `theme` port of instance `widget` of surface `alice_pod` states `non_text` on the \
         reserved plane local:brenn/theme: the kernel and chrome read every `local:brenn/` \
         plane as text"
    );
}
