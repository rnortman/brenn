//! The surface layout document: the latest-wins JSON doc a layout-channel
//! writer publishes to pick one of the fixed layouts and place a configured
//! component instance in each panel slot.
//!
//! Lives in `brenn-surface-proto` so the shell, the server, and any publisher
//! share exactly one schema and one validator. The document is untrusted input
//! (its writer is an LLM that will sometimes emit a bad doc): parsing is strict
//! (`deny_unknown_fields`) and [`LayoutDoc::validate`] is total — every
//! rejection class is a typed [`LayoutError`], never a panic. The shell rejects
//! a bad doc and keeps its last-good layout; nothing here decides that policy,
//! it only classifies.

use serde::{Deserialize, Serialize};

/// A layout document: which layout to show and which instance fills each slot.
///
/// `#[serde(deny_unknown_fields)]` so a stray top-level key is a parse error.
/// Extra *panel slots* are not caught here (the `panels` map accepts any key);
/// they are caught by [`LayoutDoc::validate`] against the kind's slot set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LayoutDoc {
    /// Schema version. Must be `1`; any other value is [`LayoutError::BadVersion`]
    /// (the forward-compat rejection point).
    pub v: u32,
    /// Which fixed layout to render.
    pub kind: LayoutKind,
    /// Slot id (`"a"`/`"b"`/`"c"`) → the panel that fills it. Must be exactly the
    /// slot set the `kind` defines (see [`LayoutKind::slots`]).
    pub panels: std::collections::BTreeMap<String, Panel>,
    /// Split fraction of the first region, for the layouts that split unevenly
    /// (`columns-2` slot `a`, `main-side` main column). Valid `[0.15, 0.85]`.
    /// Absent → the skin's CSS default. Present on `single`/`columns-3` is a
    /// rejection ([`LayoutError::RatioNotAllowed`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ratio: Option<f64>,
}

/// One panel: the instance that fills a slot, plus an optional operator label.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Panel {
    /// The configured component instance placed in this slot.
    pub instance: String,
    /// Optional label rendered by the chrome as `textContent` (never markup).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

/// The fixed layout vocabulary. Wire strings are kebab-case, spelled explicitly
/// (serde's `kebab-case` rule would render `Columns2` as `columns2`, not the
/// `columns-2` the schema requires).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LayoutKind {
    /// One panel filling the surface. Slot `a`.
    #[serde(rename = "single")]
    Single,
    /// Two side-by-side columns. Slots `a`, `b`. `ratio` splits them.
    #[serde(rename = "columns-2")]
    Columns2,
    /// Three side-by-side columns. Slots `a`, `b`, `c`.
    #[serde(rename = "columns-3")]
    Columns3,
    /// A main column (`a`) beside a side column of two stacked panels (`b`, `c`)
    /// — one split each way. `ratio` splits the main column from the side.
    #[serde(rename = "main-side")]
    MainSide,
}

impl LayoutKind {
    /// The exact slot ids this layout defines, in render order. A valid doc's
    /// `panels` keys must equal this set.
    pub fn slots(self) -> &'static [&'static str] {
        match self {
            LayoutKind::Single => &["a"],
            LayoutKind::Columns2 => &["a", "b"],
            LayoutKind::Columns3 => &["a", "b", "c"],
            LayoutKind::MainSide => &["a", "b", "c"],
        }
    }

    /// The wire string for this kind — the inverse of the serde rename, for
    /// error messages and any non-serde formatting.
    pub fn as_wire_str(self) -> &'static str {
        match self {
            LayoutKind::Single => "single",
            LayoutKind::Columns2 => "columns-2",
            LayoutKind::Columns3 => "columns-3",
            LayoutKind::MainSide => "main-side",
        }
    }

    /// Whether this layout splits its first region unevenly, i.e. whether a
    /// `ratio` is meaningful. Only `columns-2` and `main-side` do.
    pub fn accepts_ratio(self) -> bool {
        matches!(self, LayoutKind::Columns2 | LayoutKind::MainSide)
    }
}

/// Every layout kind, in render order. The single source of truth for "which
/// kinds exist" — any exhaustive iteration keys off this.
pub const ALL_KINDS: [LayoutKind; 4] = [
    LayoutKind::Single,
    LayoutKind::Columns2,
    LayoutKind::Columns3,
    LayoutKind::MainSide,
];

/// The inclusive `ratio` bounds. Outside this range the split degenerates into a
/// near-invisible sliver; clamp at the schema instead of rendering it.
pub const RATIO_MIN: f64 = 0.15;
pub const RATIO_MAX: f64 = 0.85;

/// Why a [`LayoutDoc`] was rejected. Every variant carries enough to name the
/// reason in a `warn` log; the shell surfaces one of these and keeps last-good.
#[derive(Debug, Clone, PartialEq)]
pub enum LayoutError {
    /// `v` was not `1`.
    BadVersion { got: u32 },
    /// The `panels` slot set did not equal the kind's slot set.
    WrongSlots {
        kind: LayoutKind,
        expected: &'static [&'static str],
        got: Vec<String>,
    },
    /// A panel named an instance that is not configured on this surface.
    UnknownInstance { slot: String, instance: String },
    /// Two panels named the same instance.
    DuplicateInstance { instance: String },
    /// `ratio` was outside `[RATIO_MIN, RATIO_MAX]`.
    RatioOutOfRange { got: f64 },
    /// `ratio` was present on a layout that does not split unevenly.
    RatioNotAllowed { kind: LayoutKind },
}

impl std::fmt::Display for LayoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LayoutError::BadVersion { got } => {
                write!(f, "unsupported layout version {got} (expected 1)")
            }
            LayoutError::WrongSlots {
                kind,
                expected,
                got,
            } => write!(
                f,
                "layout {} requires slots {:?}, got {:?}",
                kind.as_wire_str(),
                expected,
                got
            ),
            LayoutError::UnknownInstance { slot, instance } => {
                write!(f, "slot {slot} names unknown instance {instance:?}")
            }
            LayoutError::DuplicateInstance { instance } => {
                write!(f, "instance {instance:?} placed in more than one panel")
            }
            LayoutError::RatioOutOfRange { got } => {
                write!(f, "ratio {got} out of range [{RATIO_MIN}, {RATIO_MAX}]")
            }
            LayoutError::RatioNotAllowed { kind } => {
                write!(f, "ratio not allowed on layout {}", kind.as_wire_str())
            }
        }
    }
}

impl std::error::Error for LayoutError {}

impl LayoutDoc {
    /// Validate the document against the surface's configured instances.
    /// Returns the first rejection reason, or `Ok(())` when the doc is fully
    /// applicable. Total and side-effect-free — safe on untrusted input.
    ///
    /// `is_configured_instance` answers whether an instance id is one this
    /// surface actually mounts (the shell and server each know their own set).
    pub fn validate(
        &self,
        is_configured_instance: impl Fn(&str) -> bool,
    ) -> Result<(), LayoutError> {
        if self.v != 1 {
            return Err(LayoutError::BadVersion { got: self.v });
        }

        let expected = self.kind.slots();
        let mut got: Vec<String> = self.panels.keys().cloned().collect();
        got.sort();
        // `expected` is already sorted for every kind; compare as sets by
        // comparing the sorted key lists.
        if got.len() != expected.len() || got.iter().zip(expected).any(|(g, e)| g != e) {
            return Err(LayoutError::WrongSlots {
                kind: self.kind,
                expected,
                got,
            });
        }

        match self.ratio {
            Some(_) if !self.kind.accepts_ratio() => {
                return Err(LayoutError::RatioNotAllowed { kind: self.kind });
            }
            Some(r) if !(RATIO_MIN..=RATIO_MAX).contains(&r) => {
                return Err(LayoutError::RatioOutOfRange { got: r });
            }
            _ => {}
        }

        // Instance existence + distinctness. Iterate slots in the kind's order
        // so the first offending slot reported is stable.
        let mut seen: Vec<&str> = Vec::with_capacity(expected.len());
        for slot in expected {
            // The slot is guaranteed present: the set check above proved
            // `panels` keys equal `expected`.
            let panel = &self.panels[*slot];
            if !is_configured_instance(&panel.instance) {
                return Err(LayoutError::UnknownInstance {
                    slot: (*slot).to_string(),
                    instance: panel.instance.clone(),
                });
            }
            if seen.contains(&panel.instance.as_str()) {
                return Err(LayoutError::DuplicateInstance {
                    instance: panel.instance.clone(),
                });
            }
            seen.push(&panel.instance);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    /// The three-instance surface used across the accept/reject cases.
    fn configured() -> impl Fn(&str) -> bool {
        |inst: &str| matches!(inst, "p1" | "p2" | "p3")
    }

    fn parse(v: Value) -> Result<LayoutDoc, serde_json::Error> {
        serde_json::from_value(v)
    }

    // ── Accept cases, one per kind ────────────────────────────────────────

    #[test]
    fn single_accepts() {
        let doc = parse(json!({
            "v": 1, "kind": "single",
            "panels": { "a": { "instance": "p1" } }
        }))
        .unwrap();
        assert_eq!(doc.kind, LayoutKind::Single);
        doc.validate(configured()).unwrap();
    }

    #[test]
    fn columns_2_accepts_with_labels() {
        let doc = parse(json!({
            "v": 1, "kind": "columns-2",
            "panels": {
                "a": { "instance": "p1", "label": "LEFT" },
                "b": { "instance": "p2" }
            }
        }))
        .unwrap();
        assert_eq!(doc.panels["a"].label.as_deref(), Some("LEFT"));
        assert_eq!(doc.panels["b"].label, None);
        doc.validate(configured()).unwrap();
    }

    #[test]
    fn columns_3_accepts() {
        let doc = parse(json!({
            "v": 1, "kind": "columns-3",
            "panels": {
                "a": { "instance": "p1" },
                "b": { "instance": "p2" },
                "c": { "instance": "p3" }
            }
        }))
        .unwrap();
        doc.validate(configured()).unwrap();
    }

    #[test]
    fn main_side_accepts_with_ratio() {
        let doc = parse(json!({
            "v": 1, "kind": "main-side", "ratio": 0.6,
            "panels": {
                "a": { "instance": "p1" },
                "b": { "instance": "p2" },
                "c": { "instance": "p3" }
            }
        }))
        .unwrap();
        assert_eq!(doc.ratio, Some(0.6));
        doc.validate(configured()).unwrap();
    }

    // ── Version ───────────────────────────────────────────────────────────

    #[test]
    fn bad_version_rejected() {
        let doc = parse(json!({
            "v": 2, "kind": "single",
            "panels": { "a": { "instance": "p1" } }
        }))
        .unwrap();
        assert_eq!(
            doc.validate(configured()),
            Err(LayoutError::BadVersion { got: 2 })
        );
    }

    // ── Slot-set mismatches ───────────────────────────────────────────────

    #[test]
    fn missing_slot_rejected() {
        let doc = parse(json!({
            "v": 1, "kind": "columns-2",
            "panels": { "a": { "instance": "p1" } }
        }))
        .unwrap();
        assert!(matches!(
            doc.validate(configured()),
            Err(LayoutError::WrongSlots { .. })
        ));
    }

    #[test]
    fn extra_slot_rejected() {
        let doc = parse(json!({
            "v": 1, "kind": "single",
            "panels": {
                "a": { "instance": "p1" },
                "b": { "instance": "p2" }
            }
        }))
        .unwrap();
        assert!(matches!(
            doc.validate(configured()),
            Err(LayoutError::WrongSlots { .. })
        ));
    }

    #[test]
    fn wrong_slot_name_rejected() {
        let doc = parse(json!({
            "v": 1, "kind": "single",
            "panels": { "z": { "instance": "p1" } }
        }))
        .unwrap();
        assert!(matches!(
            doc.validate(configured()),
            Err(LayoutError::WrongSlots { .. })
        ));
    }

    // ── Instances ─────────────────────────────────────────────────────────

    #[test]
    fn unknown_instance_rejected() {
        let doc = parse(json!({
            "v": 1, "kind": "single",
            "panels": { "a": { "instance": "nope" } }
        }))
        .unwrap();
        assert_eq!(
            doc.validate(configured()),
            Err(LayoutError::UnknownInstance {
                slot: "a".to_string(),
                instance: "nope".to_string()
            })
        );
    }

    #[test]
    fn duplicate_instance_rejected() {
        let doc = parse(json!({
            "v": 1, "kind": "columns-2",
            "panels": {
                "a": { "instance": "p1" },
                "b": { "instance": "p1" }
            }
        }))
        .unwrap();
        assert_eq!(
            doc.validate(configured()),
            Err(LayoutError::DuplicateInstance {
                instance: "p1".to_string()
            })
        );
    }

    // ── Ratio ─────────────────────────────────────────────────────────────

    #[test]
    fn ratio_boundaries_accepted() {
        for r in [RATIO_MIN, RATIO_MAX] {
            let doc = parse(json!({
                "v": 1, "kind": "columns-2", "ratio": r,
                "panels": {
                    "a": { "instance": "p1" },
                    "b": { "instance": "p2" }
                }
            }))
            .unwrap();
            doc.validate(configured()).unwrap();
        }
    }

    #[test]
    fn ratio_below_min_rejected() {
        let doc = parse(json!({
            "v": 1, "kind": "columns-2", "ratio": 0.1,
            "panels": {
                "a": { "instance": "p1" },
                "b": { "instance": "p2" }
            }
        }))
        .unwrap();
        assert_eq!(
            doc.validate(configured()),
            Err(LayoutError::RatioOutOfRange { got: 0.1 })
        );
    }

    #[test]
    fn ratio_above_max_rejected() {
        let doc = parse(json!({
            "v": 1, "kind": "columns-2", "ratio": 0.9,
            "panels": {
                "a": { "instance": "p1" },
                "b": { "instance": "p2" }
            }
        }))
        .unwrap();
        assert_eq!(
            doc.validate(configured()),
            Err(LayoutError::RatioOutOfRange { got: 0.9 })
        );
    }

    #[test]
    fn ratio_on_single_rejected() {
        let doc = parse(json!({
            "v": 1, "kind": "single", "ratio": 0.5,
            "panels": { "a": { "instance": "p1" } }
        }))
        .unwrap();
        assert_eq!(
            doc.validate(configured()),
            Err(LayoutError::RatioNotAllowed {
                kind: LayoutKind::Single
            })
        );
    }

    #[test]
    fn ratio_on_columns_3_rejected() {
        let doc = parse(json!({
            "v": 1, "kind": "columns-3", "ratio": 0.5,
            "panels": {
                "a": { "instance": "p1" },
                "b": { "instance": "p2" },
                "c": { "instance": "p3" }
            }
        }))
        .unwrap();
        assert_eq!(
            doc.validate(configured()),
            Err(LayoutError::RatioNotAllowed {
                kind: LayoutKind::Columns3
            })
        );
    }

    // ── Strict parsing ────────────────────────────────────────────────────

    #[test]
    fn unknown_top_level_field_fails_to_parse() {
        let err = parse(json!({
            "v": 1, "kind": "single",
            "panels": { "a": { "instance": "p1" } },
            "bogus": true
        }));
        assert!(err.is_err());
    }

    #[test]
    fn unknown_panel_field_fails_to_parse() {
        let err = parse(json!({
            "v": 1, "kind": "single",
            "panels": { "a": { "instance": "p1", "bogus": 1 } }
        }));
        assert!(err.is_err());
    }

    #[test]
    fn unknown_kind_fails_to_parse() {
        let err = parse(json!({
            "v": 1, "kind": "columns-4",
            "panels": { "a": { "instance": "p1" } }
        }));
        assert!(err.is_err());
    }

    #[test]
    fn missing_instance_fails_to_parse() {
        let err = parse(json!({
            "v": 1, "kind": "single",
            "panels": { "a": { "label": "x" } }
        }));
        assert!(err.is_err());
    }

    // ── Serde round-trip / golden ─────────────────────────────────────────

    #[test]
    fn golden_serialize_and_roundtrip() {
        let doc = LayoutDoc {
            v: 1,
            kind: LayoutKind::MainSide,
            panels: [
                (
                    "a".to_string(),
                    Panel {
                        instance: "p1".to_string(),
                        label: Some("NEXT EVENT".to_string()),
                    },
                ),
                (
                    "b".to_string(),
                    Panel {
                        instance: "p2".to_string(),
                        label: None,
                    },
                ),
                (
                    "c".to_string(),
                    Panel {
                        instance: "p3".to_string(),
                        label: Some("GRAF · TOP 3".to_string()),
                    },
                ),
            ]
            .into_iter()
            .collect(),
            ratio: Some(0.6),
        };
        let v = serde_json::to_value(&doc).unwrap();
        assert_eq!(v["v"], json!(1));
        assert_eq!(v["kind"], json!("main-side"));
        assert_eq!(v["ratio"], json!(0.6));
        assert_eq!(
            v["panels"]["a"],
            json!({ "instance": "p1", "label": "NEXT EVENT" })
        );
        // `label: None` is omitted, not serialized as null.
        assert_eq!(v["panels"]["b"], json!({ "instance": "p2" }));
        let back: LayoutDoc = serde_json::from_value(v).unwrap();
        assert_eq!(back, doc);
    }

    #[test]
    fn kind_wire_strings_pinned() {
        assert_eq!(LayoutKind::Single.as_wire_str(), "single");
        assert_eq!(LayoutKind::Columns2.as_wire_str(), "columns-2");
        assert_eq!(LayoutKind::Columns3.as_wire_str(), "columns-3");
        assert_eq!(LayoutKind::MainSide.as_wire_str(), "main-side");
        // The wire strings match serde exactly.
        for kind in [
            LayoutKind::Single,
            LayoutKind::Columns2,
            LayoutKind::Columns3,
            LayoutKind::MainSide,
        ] {
            assert_eq!(
                serde_json::to_value(kind).unwrap(),
                json!(kind.as_wire_str())
            );
        }
    }
}
