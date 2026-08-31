//! The banner's styling seam, pinned across the two languages that spell it.
//!
//! Chrome stamps its connection banner with marker attributes and the
//! stylesheets dress it by selecting on them. The two are independent
//! literals in independent files, and a disagreement fails silently: the
//! selector matches nothing, the banner renders unstyled — full width and
//! fixed over the top of a kiosk on the fatal path, the one moment the page
//! most needs to be legible — and every gate stays green.
//!
//! So the glue and the stylesheets are both data of these tests. The needles
//! are read out of the glue rather than restated here, because a copy of the
//! attribute name in this file would be a third literal to drift.

/// The value of a `const <name>: &str = "…";` in `source`.
///
/// The glue is `cfg(target_arch = "wasm32")` and absent from the host build
/// that runs these tests, so its constants are unreachable as items and are
/// read out of its text instead — the mechanism the contract crate already
/// uses against the guest SDK.
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
fn every_stylesheet_dresses_the_banner_by_the_marker_chrome_stamps() {
    for name in ["BANNER_MARKER", "BANNER_STATE_ATTR"] {
        let marker = glue_marker(GLUE, name);
        let selector = format!("[{marker}");
        for (path, css) in stylesheets() {
            assert!(
                css.contains(&selector),
                "{path} selects nothing on `{marker}`, which is what chrome \
                 stamps as `{name}`; the banner would render unstyled"
            );
        }
    }
}

#[test]
fn the_banner_is_reached_by_attribute_and_never_by_id() {
    // The marker replaced an id hook: an id is not on the `dom` capability's
    // attribute allow-list, so a stylesheet that went back to one would select
    // an element nothing can name.
    for (path, css) in stylesheets() {
        assert!(
            !css.contains("#brenn-surface-banner"),
            "{path} still selects the banner by id"
        );
    }
}
