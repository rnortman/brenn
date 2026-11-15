//! Lowering `fetch_bytes`: carried from a surface `in` binding onto its raw
//! subscription, unstated on a surface `io` port, and refused on a consumer's
//! `in` binding, which has no field for it.

use brenn_dsl::processor_needs;

use crate::config::{config_from_dsl, sole_refusal};

/// A surface `in` binding carries the bound it states and `None` when it
/// states none; an `io` port bound to a channel has no key, so its
/// subscription half carries `None` too. Lowering judges nothing about the
/// value beyond its type: the grant, the channel's class and the cap are
/// boot's.
#[test]
fn a_surface_in_binding_carries_its_fetch_bytes() {
    let document = format!(
        concat!(
            r#"
channel frames at "ephemeral:alice-cam.frames" {{
    push_depth = 1;
    retain_depth = 4;
}}

channel notes at "ephemeral:alice-cam.notes" {{
    push_depth = 1;
    retain_depth = 4;
}}

channel state at "ephemeral:alice-cam.state" {{
    push_depth = 1;
    retain_depth = 1;
}}

// ── packaged ──
component Viewer {{
    {}
    in frames;
    in notes;
    io state;
}}
// ── packaged ──

surface alice_cam {{
    grants = [subscribe, publish];

    new viewer: Viewer {{
        chrome = true;
        grants = [ports, blobs];
        in frames <- frames {{ push_depth = 1; fetch_bytes = 4194304; }}
        in notes <- notes {{ push_depth = 1; }}
        io state <-> state {{ push_depth = 1; retain_depth = 1; }}
    }}
}}
"#
        ),
        processor_needs!("ports, blobs"),
    );
    let config = config_from_dsl(&document);
    let fetch: Vec<(&str, Option<u64>)> = config.surfaces[0]
        .subscriptions
        .iter()
        .map(|sub| (sub.port.as_str(), sub.fetch_bytes))
        .collect();
    assert_eq!(
        fetch,
        vec![
            ("frames", Some(4_194_304)),
            ("notes", None),
            ("state", None)
        ]
    );
}

/// A bound is a byte count: a negative one is out of range for the key.
#[test]
fn a_negative_fetch_bytes_is_refused() {
    let refusal = sole_refusal(&format!(
        concat!(
            r#"
channel frames at "ephemeral:alice-cam.frames" {{
    push_depth = 1;
    retain_depth = 4;
}}

// ── packaged ──
component Viewer {{
    {}
    in frames;
    out echo;
}}
// ── packaged ──

surface alice_cam {{
    grants = [subscribe, publish];

    new viewer: Viewer {{
        chrome = true;
        grants = [ports, blobs];
        in frames <- frames {{ push_depth = 1; fetch_bytes = -1; }}
        out echo -> frames;
    }}
}}
"#
        ),
        processor_needs!("ports, blobs"),
    ));
    assert_eq!(
        refusal.message,
        "`fetch_bytes`: -1 is out of range for this key"
    );
}

/// A backend component opens an attachment when it reads it; nothing is
/// fetched ahead of its activation, so a consumer's `in` binding has no
/// `fetch_bytes` and the key is refused at its own value, naming the keys the
/// binding reads — and only once: the refused value is not also type-checked.
#[test]
fn fetch_bytes_on_a_consumer_in_binding_is_refused() {
    let refusal = sole_refusal(&format!(
        concat!(
            r#"
channel frames at "ephemeral:alice-cam.frames" {{
    push_depth = 1;
    retain_depth = 4;
}}

// ── packaged ──
component Recorder {{
    {}
    in frames;
    out echo;
}}
// ── packaged ──

new recorder: Recorder {{
    grants = [ports, blobs];
    in frames <- frames {{ push_depth = 1; fetch_bytes = "lots"; }}
    out echo -> frames;
}}
"#
        ),
        processor_needs!("ports, blobs"),
    ));
    assert!(
        refusal
            .message
            .starts_with("`fetch_bytes` is not a key of the `frames` port of consumer `"),
        "{}",
        refusal.render()
    );
    assert!(
        refusal.message.ends_with(
            "; expected `push_depth`, `retain_depth`, `noise`, `wake_min`, `amplification` or \
             `non_text`"
        ),
        "{}",
        refusal.render()
    );
}
