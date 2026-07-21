//! Shared rendering envelope helpers for batch tools.
//!
//! Both `BatchReconcile` and `BatchAssign` share an identical outer structure
//! for their table and swipe renderers: empty-guard, config-block, custom-
//! element open/close. The per-row/per-card body is the only diverging part.
//! These helpers eliminate the duplication.

use std::fmt::Write as _;

use brenn_lib::util::json_for_script_tag;

use crate::batch::EnrichedBatchItem;

/// Shared envelope skeleton: open custom element, emit config script, optional
/// header, call `inner` to write the body, close the tag. Returns `None` when
/// `items` is empty so callers can fall back to generic display without a
/// separate empty-check.
fn build_envelope(
    tag: &str,
    items: &[EnrichedBatchItem],
    render_header: Option<&str>,
    inner: impl FnOnce(&mut String),
) -> Option<String> {
    if items.is_empty() {
        return None;
    }

    let config = serde_json::json!({ "count": items.len() });
    let config_json = json_for_script_tag(&config);

    let mut html = String::with_capacity(4096);
    html.push('<');
    html.push_str(tag);
    html.push_str(">\n");
    writeln!(
        html,
        "  <script type=\"application/json\">{config_json}</script>"
    )
    .expect("write to String is infallible");

    if let Some(header) = render_header {
        html.push_str(header);
    }

    inner(&mut html);

    html.push_str("</");
    html.push_str(tag);
    html.push('>');

    Some(html)
}

/// Render a custom-element table wrapper with a config script tag and per-row
/// body. Returns `None` when `items` is empty (caller falls back to generic
/// display).
///
/// Public counterpart to `render_swipe_envelope`: together they form the
/// stable crate API for batch rendering. Both delegate to the private
/// `build_envelope` skeleton; the distinction between the two is the closure
/// shape — `FnOnce` for a single table body vs `Fn` per-item for swipe cards.
///
/// `tag` — the custom element tag name (e.g. `"brenn-pfin-batch-table"`).
/// `items` — enriched batch items.
/// `render_header` — optional header HTML inserted before `<table>`.
/// `render_body` — closure that appends `<thead>` and `<tbody>` rows to the
///   provided `String`; called only when items is non-empty.
pub fn render_table_envelope(
    tag: &str,
    items: &[EnrichedBatchItem],
    render_header: Option<&str>,
    render_body: impl FnOnce(&mut String),
) -> Option<String> {
    build_envelope(tag, items, render_header, render_body)
}

/// Render a custom-element swipe wrapper with a config script tag and per-card
/// body. Returns `None` when `items` is empty.
///
/// `tag` — the custom element tag name (e.g. `"brenn-pfin-batch-swipe"`).
/// `items` — enriched batch items.
/// `render_header` — optional header HTML inserted before the cards.
/// `render_card` — closure called per item to append a card element to the
///   provided `String`.
pub fn render_swipe_envelope(
    tag: &str,
    items: &[EnrichedBatchItem],
    render_header: Option<&str>,
    render_card: impl Fn(&EnrichedBatchItem, &mut String),
) -> Option<String> {
    build_envelope(tag, items, render_header, |html| {
        for item in items {
            render_card(item, html);
        }
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn make_item() -> EnrichedBatchItem {
        EnrichedBatchItem {
            original_index: 0,
            item: json!({}),
            pending_import: json!({}),
        }
    }

    // --- render_table_envelope ---

    #[test]
    fn table_envelope_empty_returns_none() {
        assert!(render_table_envelope("brenn-test-table", &[], None, |_| ()).is_none());
    }

    #[test]
    fn table_envelope_tag_flows_through() {
        let items = vec![make_item()];
        let html = render_table_envelope("brenn-test-table", &items, None, |_| ())
            .expect("non-empty items must produce Some");
        assert!(html.starts_with("<brenn-test-table>"), "html: {html}");
        assert!(html.ends_with("</brenn-test-table>"), "html: {html}");
    }

    #[test]
    fn table_envelope_no_header_when_none() {
        let items = vec![make_item()];
        let html = render_table_envelope("brenn-test-table", &items, None, |_| ()).unwrap();
        // Verify only the open-tag, config script, and close-tag are present
        // (the body closure appends nothing). No stray header markup.
        let script_count = html.matches("<script type=\"application/json\">").count();
        assert_eq!(
            script_count, 1,
            "expected exactly one script tag; html: {html}"
        );
        assert!(
            !html.contains("<header"),
            "unexpected <header in html: {html}"
        );
    }

    #[test]
    fn table_envelope_header_appears_when_some() {
        let items = vec![make_item()];
        let header = "<p class=\"hdr\">hello</p>";
        let html = render_table_envelope("brenn-test-table", &items, Some(header), |_| ()).unwrap();
        assert!(html.contains(header), "html: {html}");
    }

    // --- render_swipe_envelope ---

    #[test]
    fn swipe_envelope_empty_returns_none() {
        assert!(render_swipe_envelope("brenn-test-swipe", &[], None, |_, _| ()).is_none());
    }

    #[test]
    fn swipe_envelope_tag_flows_through() {
        let items = vec![make_item()];
        let html = render_swipe_envelope("brenn-test-swipe", &items, None, |_, _| ())
            .expect("non-empty items must produce Some");
        assert!(html.starts_with("<brenn-test-swipe>"), "html: {html}");
        assert!(html.ends_with("</brenn-test-swipe>"), "html: {html}");
    }

    #[test]
    fn swipe_envelope_no_header_when_none() {
        let items = vec![make_item()];
        let html = render_swipe_envelope("brenn-test-swipe", &items, None, |_, _| ()).unwrap();
        assert!(
            !html.contains("<header"),
            "unexpected <header in html: {html}"
        );
    }

    #[test]
    fn swipe_envelope_header_appears_when_some() {
        let items = vec![make_item()];
        let header = "<p class=\"hdr\">world</p>";
        let html =
            render_swipe_envelope("brenn-test-swipe", &items, Some(header), |_, _| ()).unwrap();
        assert!(html.contains(header), "html: {html}");
    }

    // --- build_envelope contract: count field and per-item body ---

    /// config JSON must contain `"count":N` equal to the number of items.
    #[test]
    fn envelope_config_json_contains_count() {
        let items = vec![make_item(), make_item(), make_item()];
        let html = render_table_envelope("brenn-test-table", &items, None, |_| ())
            .expect("non-empty items must produce Some");
        // The script tag holds the JSON config block.
        let count_fragment = format!("\"count\":{}", items.len());
        assert!(
            html.contains(&count_fragment),
            "config JSON must contain {count_fragment:?}; html: {html}"
        );
    }

    /// render_swipe_envelope must invoke the render_card closure once per item
    /// and append the results in order.
    #[test]
    fn swipe_envelope_calls_render_card_per_item_in_order() {
        let items = vec![
            EnrichedBatchItem {
                original_index: 0,
                item: json!({}),
                pending_import: json!({}),
            },
            EnrichedBatchItem {
                original_index: 1,
                item: json!({}),
                pending_import: json!({}),
            },
            EnrichedBatchItem {
                original_index: 2,
                item: json!({}),
                pending_import: json!({}),
            },
        ];
        let html = render_swipe_envelope("brenn-test-swipe", &items, None, |item, buf| {
            buf.push_str(&format!("<card-{}>", item.original_index));
        })
        .expect("non-empty items must produce Some");

        // Each marker must appear exactly once, in index order.
        for i in 0..items.len() {
            let marker = format!("<card-{i}>");
            assert!(
                html.contains(&marker),
                "marker {marker:?} missing from html: {html}"
            );
        }
        // Verify ordering: card-0 before card-1 before card-2.
        let pos0 = html.find("<card-0>").expect("card-0 must be present");
        let pos1 = html.find("<card-1>").expect("card-1 must be present");
        let pos2 = html.find("<card-2>").expect("card-2 must be present");
        assert!(pos0 < pos1, "card-0 must appear before card-1");
        assert!(pos1 < pos2, "card-1 must appear before card-2");
    }
}
