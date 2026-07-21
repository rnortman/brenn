use super::super::*;

#[test]
fn ts_rs_export() {
    // ts-rs convention: export types via test.
    // The #[ts(export)] attribute handles this, but we can also call it explicitly
    // to verify it works and see the output path.
    let cfg = ts_rs::Config::default();
    WsClientMessage::export(&cfg).expect("WsClientMessage export failed");
    WsServerMessage::export(&cfg).expect("WsServerMessage export failed");
    CcState::export(&cfg).expect("CcState export failed");
    PermissionDecision::export(&cfg).expect("PermissionDecision export failed");
    ToolResponseDecision::export(&cfg).expect("ToolResponseDecision export failed");
    ConversationSummary::export(&cfg).expect("ConversationSummary export failed");
    ConversationListStatus::export(&cfg).expect("ConversationListStatus export failed");
    UserSettings::export(&cfg).expect("UserSettings export failed");
    SnapshotMetadata::export(&cfg).expect("SnapshotMetadata export failed");
    ArtifactFileInfo::export(&cfg).expect("ArtifactFileInfo export failed");
    ArtifactVersionInfo::export(&cfg).expect("ArtifactVersionInfo export failed");
    ModelInfo::export(&cfg).expect("ModelInfo export failed");
    PresenceUser::export(&cfg).expect("PresenceUser export failed");
    ViewportClass::export(&cfg).expect("ViewportClass export failed");
    PaneLayout::export(&cfg).expect("PaneLayout export failed");
    AttachmentRef::export(&cfg).expect("AttachmentRef export failed");
    AttachmentMeta::export(&cfg).expect("AttachmentMeta export failed");
    PushClickTraceEvent::export(&cfg).expect("PushClickTraceEvent export failed");
    TraceClient::export(&cfg).expect("TraceClient export failed");
    DroppedClient::export(&cfg).expect("DroppedClient export failed");
    PushClickTerminalBranch::export(&cfg).expect("PushClickTerminalBranch export failed");
    RuleScope::export(&cfg).expect("RuleScope export failed");
    SelectedTask::export(&cfg).expect("SelectedTask export failed");
    TodoItem::export(&cfg).expect("TodoItem export failed");
    TodoLintError::export(&cfg).expect("TodoLintError export failed");
    // PermissionModeValue: NOT exported. It keeps its hand-written serde
    // strategy (from/into String) so the backend retains the raw unknown
    // string for alerts. The ts-rs type override on the `mode` field in
    // WsServerMessage inlines the string union directly.
    SwToAppMessage::export(&cfg).expect("SwToAppMessage export failed");
    // Debug snapshot types.
    DebugViewportSnapshotData::export(&cfg).expect("DebugViewportSnapshotData export failed");
    RectData::export(&cfg).expect("RectData export failed");
    VisualViewportData::export(&cfg).expect("VisualViewportData export failed");
    SystemMessageCategory::export(&cfg).expect("SystemMessageCategory export failed");
}
