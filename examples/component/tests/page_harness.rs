//! The recording page host, reached from another Bazel module.
//!
//! This component is headless — it holds `ports` and `log` and no `dom` — so
//! there is no view to drive here. What the test proves is the thing an author
//! finds out first and most expensively otherwise: that the harness crate
//! builds, resolves its WIT world, and instantiates a component from a
//! repository that is not brenn's.

use std::path::PathBuf;

use brenn_envelope::grants::ComponentGrant;
use brenn_page_harness::{Harness, Page};

/// An out-of-tree consumer's artifact path comes from the build, not a tree
/// walk, because the crate sits wherever its own repository puts it.
fn artifact() -> PathBuf {
    PathBuf::from(std::env::var("EXAMPLE_CASER_WASM").expect("the build names the artifact"))
}

#[test]
fn the_example_component_instantiates_against_the_page_host() {
    let harness = Harness::new(
        &artifact(),
        Page::new(),
        &[ComponentGrant::Ports, ComponentGrant::Log],
    );
    // Instantiation is the first assertion: an artifact whose imports the linked
    // profile does not cover panics inside `new`.
    drop(harness);
}

/// One delivery driven all the way through, from another module. The half of
/// the API a consumer's own tests are written against — the activation
/// constructors, `call`, the transcript and the publish record — is reached
/// here rather than left to the first out-of-tree author: an item that is
/// private, or a type a consumer cannot name, builds green in brenn and fails
/// only across this boundary.
#[test]
fn a_delivery_driven_from_another_module_publishes_and_logs() {
    let mut harness = Harness::new(
        &artifact(),
        Page::new(),
        &[ComponentGrant::Ports, ComponentGrant::Log],
    );
    harness.call(brenn_page_harness::delivery_on(
        "text",
        &[],
        &["hello world"],
        0,
    ));

    let published = harness.page().published_on("cased");
    assert_eq!(published.len(), 1, "{published:?}");
    assert!(published[0].contains(r#""kebab":"hello-world""#), "{published:?}");

    let transcript = harness.transcript();
    assert!(
        transcript.iter().any(|call| call.starts_with("ports.publish(cased,")),
        "{transcript:?}",
    );
}
