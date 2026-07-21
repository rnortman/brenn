use super::super::*;

/// A representative `DebugViewportSnapshot` payload round-trips through
/// `WsClientMessage` serde without error. Tests that `Option` fields absent
/// in JSON become `None` and that required scalars are present.
#[test]
fn debug_viewport_snapshot_round_trip_with_partial_fields() {
    // Minimal valid payload: required scalars present, all Options absent.
    // screen_avail_height and window_outer_height are required non-Option fields.
    let json = r#"{
        "type": "DebugViewportSnapshot",
        "snapshot": {
            "inner_width": 390.0,
            "inner_height": 844.0,
            "document_element_client_width": 390.0,
            "document_element_client_height": 844.0,
            "document_element_scroll_height": 844.0,
            "scroll_x": 0.0,
            "scroll_y": 0.0,
            "device_pixel_ratio": 3.0,
            "screen_width": 390.0,
            "screen_height": 844.0,
            "display_mode_standalone": true,
            "max_width_768": true,
            "screen_avail_height": 844.0,
            "window_outer_height": 844.0,
            "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0)",
            "visibility_state": "visible",
            "client_timestamp": "2026-06-06T12:00:00.000Z",
            "build_id": "abc123"
        }
    }"#;
    let msg: WsClientMessage = serde_json::from_str(json).unwrap();
    match msg {
        WsClientMessage::DebugViewportSnapshot { snapshot } => {
            assert!((snapshot.inner_width - 390.0).abs() < f64::EPSILON);
            assert!((snapshot.inner_height - 844.0).abs() < f64::EPSILON);
            assert!(snapshot.display_mode_standalone);
            assert!(snapshot.max_width_768);
            assert!(snapshot.visual_viewport.is_none());
            assert!(snapshot.input.is_none());
            assert!(snapshot.scrolling_element_scroll_top.is_none());
            assert!(snapshot.screen_orientation_type.is_none());
            assert!(snapshot.input_bottom_below_visual_fold.is_none());
            assert!(snapshot.input_bottom_below_layout.is_none());
            assert!(snapshot.html_height.is_none());
            assert!(snapshot.safe_area_inset_bottom.is_none());
            assert!(snapshot.ua_brands.is_none());
            assert!(snapshot.ua_mobile.is_none());
            assert!(snapshot.active_element_tag.is_none());
            assert_eq!(
                snapshot.user_agent,
                "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0)"
            );
            assert_eq!(snapshot.build_id, "abc123");
        }
        _ => panic!("wrong variant"),
    }
}

/// Full `DebugViewportSnapshot` payload with all populated optional fields
/// round-trips correctly, including derived booleans and visual viewport.
#[test]
fn debug_viewport_snapshot_round_trip_with_full_fields() {
    let snap = DebugViewportSnapshotData {
        inner_width: 390.0,
        inner_height: 660.0,
        document_element_client_width: 390.0,
        document_element_client_height: 660.0,
        document_element_scroll_height: 660.0,
        scroll_x: 0.0,
        scroll_y: 0.0,
        scrolling_element_scroll_top: Some(0.0),
        scrolling_element_scroll_left: Some(0.0),
        device_pixel_ratio: 3.0,
        screen_width: 390.0,
        screen_height: 844.0,
        screen_orientation_type: Some("portrait-primary".into()),
        display_mode_standalone: true,
        max_width_768: true,
        visual_viewport: Some(VisualViewportData {
            width: 390.0,
            height: 660.0,
            offset_top: 0.0,
            offset_left: 0.0,
            page_top: 0.0,
            page_left: 0.0,
            scale: 1.0,
        }),
        input: Some(RectData {
            top: 610.0,
            left: 0.0,
            right: 390.0,
            bottom: 660.0,
            width: 390.0,
            height: 50.0,
        }),
        input_bar: None,
        app_main: None,
        pane_layout: None,
        message_list: None,
        attachment_strip: None,
        chip_bar: None,
        presence_bar: None,
        steal_bar: None,
        status_bar: None,
        body: None,
        document_element: None,
        message_list_scroll_top: Some(100.0),
        message_list_scroll_height: Some(800.0),
        message_list_client_height: Some(560.0),
        input_bottom_below_visual_fold: Some(false),
        input_bottom_below_layout: Some(false),
        html_height: Some("100%".into()),
        body_height: Some("100%".into()),
        body_overflow: Some("hidden".into()),
        input_bar_position: Some("sticky".into()),
        input_bar_flex_shrink: Some("0".into()),
        app_main_min_height: Some("0px".into()),
        pane_layout_min_height: Some("0px".into()),
        pane_layout_height: Some("660px".into()),
        message_list_min_height: Some("0px".into()),
        message_list_height: Some("560px".into()),
        mobile_slot_content_min_height: Some("0px".into()),
        app_main_height: Some("660px".into()),
        app_topbar: Some(RectData {
            top: 0.0,
            left: 0.0,
            right: 390.0,
            bottom: 56.76,
            width: 390.0,
            height: 56.76,
        }),
        app_header: Some(RectData {
            top: 0.0,
            left: 0.0,
            right: 390.0,
            bottom: 34.99,
            width: 390.0,
            height: 34.99,
        }),
        app_layout: Some(RectData {
            top: 34.99,
            left: 0.0,
            right: 390.0,
            bottom: 660.0,
            width: 390.0,
            height: 625.01,
        }),
        document_element_offset_height: Some(660.0),
        safe_area_inset_top: Some("47px".into()),
        safe_area_inset_right: Some("0px".into()),
        safe_area_inset_bottom: Some("34px".into()),
        safe_area_inset_left: Some("0px".into()),
        probe_100vh_px: Some(660.0),
        probe_100svh_px: Some(660.0),
        probe_100lvh_px: Some(844.0),
        probe_100dvh_px: Some(889.524),
        probe_exception_units: None,
        screen_avail_height: 844.0,
        window_outer_height: 844.0,
        user_agent: "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0)".into(),
        ua_brands: Some(vec!["Chromium".into(), "Google Chrome".into()]),
        ua_mobile: Some(true),
        active_element_tag: Some("TEXTAREA".into()),
        active_element_id: Some("input".into()),
        visibility_state: "visible".into(),
        client_timestamp: "2026-06-06T12:00:00.000Z".into(),
        build_id: "abc123".into(),
    };
    let msg = WsClientMessage::DebugViewportSnapshot {
        snapshot: Box::new(snap.clone()),
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert!(json.contains("\"type\":\"DebugViewportSnapshot\""));
    assert!(json.contains("visual_viewport"));
    assert!(json.contains("portrait-primary"));
    assert!(json.contains("\"input_bottom_below_visual_fold\":false"));
    assert!(json.contains("\"input_bottom_below_layout\":false"));
    assert!(json.contains("\"ua_mobile\":true"));

    let parsed: WsClientMessage = serde_json::from_str(&json).unwrap();
    match parsed {
        WsClientMessage::DebugViewportSnapshot { snapshot: back } => {
            assert!((back.inner_height - 660.0).abs() < f64::EPSILON);
            assert_eq!(
                back.screen_orientation_type.as_deref(),
                Some("portrait-primary")
            );
            let vv = back.visual_viewport.unwrap();
            assert!((vv.height - 660.0).abs() < f64::EPSILON);
            let inp = back.input.unwrap();
            assert!((inp.bottom - 660.0).abs() < f64::EPSILON);
            assert_eq!(back.input_bottom_below_visual_fold, Some(false));
            assert_eq!(back.input_bottom_below_layout, Some(false));
            assert_eq!(
                back.ua_brands.as_deref(),
                Some(&["Chromium".to_string(), "Google Chrome".to_string()][..])
            );
            assert_eq!(back.ua_mobile, Some(true));
            assert_eq!(back.active_element_tag.as_deref(), Some("TEXTAREA"));
            assert_eq!(back.safe_area_inset_bottom.as_deref(), Some("34px"));
            assert_eq!(back.pane_layout_min_height.as_deref(), Some("0px"));
            assert_eq!(back.pane_layout_height.as_deref(), Some("660px"));
            assert_eq!(back.message_list_min_height.as_deref(), Some("0px"));
            assert_eq!(back.message_list_height.as_deref(), Some("560px"));
            assert_eq!(back.mobile_slot_content_min_height.as_deref(), Some("0px"));
            assert_eq!(back.app_main_height.as_deref(), Some("660px"));
            let topbar = back.app_topbar.unwrap();
            assert!((topbar.height - 56.76).abs() < 0.01);
            let app_header = back.app_header.unwrap();
            assert!((app_header.height - 34.99).abs() < 0.01);
            let layout = back.app_layout.unwrap();
            assert!((layout.top - 34.99).abs() < 0.01);
            assert_eq!(back.document_element_offset_height, Some(660.0));
            assert!((back.probe_100vh_px.unwrap() - 660.0).abs() < f64::EPSILON);
            assert!((back.probe_100svh_px.unwrap() - 660.0).abs() < f64::EPSILON);
            assert!((back.probe_100lvh_px.unwrap() - 844.0).abs() < f64::EPSILON);
            assert!((back.probe_100dvh_px.unwrap() - 889.524).abs() < 0.001);
            assert!((back.screen_avail_height - 844.0).abs() < f64::EPSILON);
            assert!((back.window_outer_height - 844.0).abs() < f64::EPSILON);
        }
        _ => panic!("wrong variant after round-trip"),
    }
}

/// `DebugViewportSnapshot` partial payload (minimal required fields only) still
/// deserializes correctly — all optional fields absent maps to `None`.
/// Covers all optional fields added in the H2′ instrumentation pass (mid-chain
/// and header-band), plus spot-checks of pre-existing optional fields.
#[test]
fn debug_viewport_snapshot_round_trip_minimal_absent_optional_fields() {
    // Minimal JSON — all optional fields absent; they must all deserialize as None.
    // screen_avail_height and window_outer_height are required (non-Option) f64 fields.
    let json = r#"{
        "type": "DebugViewportSnapshot",
        "snapshot": {
            "inner_width": 390.0,
            "inner_height": 844.0,
            "document_element_client_width": 390.0,
            "document_element_client_height": 844.0,
            "document_element_scroll_height": 844.0,
            "scroll_x": 0.0,
            "scroll_y": 0.0,
            "device_pixel_ratio": 3.0,
            "screen_width": 390.0,
            "screen_height": 844.0,
            "display_mode_standalone": true,
            "max_width_768": true,
            "screen_avail_height": 844.0,
            "window_outer_height": 844.0,
            "user_agent": "Mozilla/5.0",
            "visibility_state": "visible",
            "client_timestamp": "2026-06-09T00:00:00.000Z",
            "build_id": "abc"
        }
    }"#;
    let msg: WsClientMessage = serde_json::from_str(json).unwrap();
    match msg {
        WsClientMessage::DebugViewportSnapshot { snapshot } => {
            // Pre-existing optional fields (spot-check).
            assert!(snapshot.visual_viewport.is_none());
            assert!(snapshot.input.is_none());
            assert!(snapshot.app_main_min_height.is_none());
            // H2′ mid-chain and header-band fields.
            assert!(snapshot.pane_layout_min_height.is_none());
            assert!(snapshot.pane_layout_height.is_none());
            assert!(snapshot.message_list_min_height.is_none());
            assert!(snapshot.message_list_height.is_none());
            assert!(snapshot.mobile_slot_content_min_height.is_none());
            assert!(snapshot.app_main_height.is_none());
            assert!(snapshot.app_topbar.is_none());
            assert!(snapshot.app_header.is_none());
            assert!(snapshot.app_layout.is_none());
            assert!(snapshot.document_element_offset_height.is_none());
            // Viewport-unit probe and window-bounds fields.
            assert!(snapshot.probe_100vh_px.is_none());
            assert!(snapshot.probe_100svh_px.is_none());
            assert!(snapshot.probe_100lvh_px.is_none());
            assert!(snapshot.probe_100dvh_px.is_none());
            // screen_avail_height and window_outer_height are required non-Option fields.
            assert!((snapshot.screen_avail_height - 844.0).abs() < f64::EPSILON);
            assert!((snapshot.window_outer_height - 844.0).abs() < f64::EPSILON);
        }
        _ => panic!("wrong variant"),
    }
}

/// `SystemMessageCategory::DebugSnapshot` round-trips as the string "DebugSnapshot".
#[test]
fn system_message_category_debug_snapshot_round_trip() {
    let cat = SystemMessageCategory::DebugSnapshot;
    let json = serde_json::to_string(&cat).unwrap();
    assert_eq!(json, r#""DebugSnapshot""#, "unexpected wire value: {json}");
    let parsed: SystemMessageCategory = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, cat);
}
