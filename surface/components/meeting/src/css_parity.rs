//! The panel's styling seam, pinned across the two languages that spell it.
//!
//! The glue stamps one marker attribute on this instance's host element and
//! every meeting rule in the base stylesheet and both skins descends from it.
//! The two sides are independent literals in independent files, and a
//! disagreement fails silently: the selectors match nothing and an escalated
//! meeting takeover renders unstyled, with every gate green.
//!
//! So the glue and the stylesheets are both data of this test, and the needle
//! is read out of the glue rather than restated here.

/// The value of a `const <name>: &str = "…";` in `source`.
///
/// The glue is `cfg(target_arch = "wasm32")` and absent from the host build
/// that runs this test, so its constants are read out of its text instead —
/// the mechanism the contract crate already uses against the guest SDK.
fn glue_marker(source: &str, name: &str) -> String {
    let declaration = format!("const {name}: &str = \"");
    let from = source
        .find(&declaration)
        .unwrap_or_else(|| panic!("the glue declares no `{declaration}…`"))
        + declaration.len();
    let rest = &source[from..];
    let end = rest
        .find('"')
        .unwrap_or_else(|| panic!("`{name}`'s declaration does not close"));
    rest[..end].to_string()
}

const GLUE: &str = include_str!("component.rs");

const SURFACE_CSS: &str = "frontend/src/surface.css";
const FOUNDRY_SKIN: &str = "frontend/skins/foundry.css";
const BENCH_SKIN: &str = "frontend/skins/bench.css";

/// The three stylesheets that dress it, read from the test's own data tree.
fn stylesheets() -> Vec<(&'static str, String)> {
    [SURFACE_CSS, FOUNDRY_SKIN, BENCH_SKIN]
        .into_iter()
        .map(|path| {
            let text = std::fs::read_to_string(path)
                .unwrap_or_else(|err| panic!("{path} is not in this test's data: {err}"));
            (path, text)
        })
        .collect()
}

#[test]
fn every_stylesheet_descends_from_the_marker_the_panel_stamps() {
    let marker = glue_marker(GLUE, "ROOT_MARKER");
    let selector = format!("[{marker}]");
    for (path, css) in stylesheets() {
        assert!(
            css.contains(&selector),
            "{path} selects nothing on `{marker}`, which is what the panel \
             stamps on its host element; every meeting rule there is dead"
        );
    }
}
