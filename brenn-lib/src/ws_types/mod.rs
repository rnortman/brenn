//! WebSocket message types for the browser ↔ backend protocol.
//!
//! These types are the contract between Rust and TypeScript. TypeScript
//! definitions are generated via `ts-rs` (see the export test at the bottom).

use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// Browser → Backend message.
///
/// Protocol convention: no `#[serde(other)]` on TS-exported enums.
/// Backend-internal catch-all enums must not derive `TS`.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(tag = "type")]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub enum WsClientMessage {
    /// User sends a chat message to start/continue a conversation.
    /// `attachments` references files previously uploaded via POST /app/{slug}/upload.
    /// `model` optionally overrides the model for this message (and going forward).
    SendMessage {
        text: String,
        #[serde(default)]
        attachments: Vec<AttachmentRef>,
        /// Model alias to use (e.g. "sonnet", "opus", "haiku", "default").
        /// If None, uses the app's configured default model.
        #[serde(default)]
        model: Option<String>,
        /// Tasks selected in the todo list to inject as context alongside the message.
        #[serde(default)]
        selected_tasks: Vec<SelectedTask>,
    },
    /// User responds to a CC permission request (synchronous — CC is blocking).
    PermissionResponse {
        request_id: String,
        decision: PermissionDecision,
    },
    /// User responds to an interactive tool card (asynchronous — CC already continued).
    ToolCardResponse {
        request_id: String,
        decision: ToolResponseDecision,
    },
    /// Frontend reporting an error back to the backend for logging/alerting.
    ClientError { message: String },
    /// Fine-grained trace event from the service worker's push-click handler.
    /// Carries one `PushClickTraceEvent` variant per checkpoint.
    PushClickTrace {
        #[ts(type = "number")]
        user_id: i64,
        event: PushClickTraceEvent,
    },
    /// Request the list of conversations.
    ListConversations,
    /// Switch to a different conversation (load its history, attach to its bridge).
    SwitchConversation {
        #[ts(type = "number")]
        conversation_id: i64,
    },
    /// Start a new conversation.
    NewConversation,
    /// Reconnect to a conversation after WS disconnect.
    /// `last_seq` is `None` for "send everything" (full replace).
    /// `Some(n)` reserved for future incremental catch-up optimization.
    Reconnect {
        #[ts(type = "number")]
        conversation_id: i64,
        #[ts(type = "number | null")]
        last_seq: Option<i64>,
    },
    /// Re-open a previously displayed artifact file.
    /// If `message_id` is set, load the stored snapshot from DB (no disk read).
    /// If absent, re-read from disk (current content). Path is validated against cwd.
    ReopenArtifact {
        file_path: String,
        #[ts(type = "number | null")]
        message_id: Option<i64>,
    },
    /// Load a specific artifact snapshot by its message id.
    /// Used by the version selector in the file picker (Chunk 3).
    LoadArtifactSnapshot {
        #[ts(type = "number")]
        message_id: i64,
    },
    /// Force-steal the current app's active session (single_instance enforcement).
    /// No fields — the app slug is fixed on this WS connection.
    /// Handler logic is Phase D; currently responds with an Error placeholder.
    StealApp,
    /// Report the browser's IANA timezone (e.g. "America/New_York", "Asia/Tokyo").
    /// Sent on connect. Used for message timestamp attribution.
    SetTimezone { timezone: String },
    /// Report browser device info (sent right after SetTimezone on every connect).
    /// Empty strings mean "no value available"; the backend retains previously
    /// stored values when a field is empty.
    SetDeviceInfo {
        user_agent: String,
        platform: String,
        #[ts(type = "number")]
        screen_width: u32,
        #[ts(type = "number")]
        screen_height: u32,
    },
    /// Request CC to stop its current turn (interrupt).
    /// Sends the interrupt control message; CC finishes gracefully with a result.
    StopRequest,
    /// Report the browser's viewport class. Sent on connect (first message) and
    /// on change (e.g., device rotation, window resize crossing the threshold).
    /// The backend uses this to select the appropriate pane layout.
    SetViewportClass { viewport_class: ViewportClass },
    /// Owner toggles a conversation's shared/private state (multiuser apps only).
    SetConversationPrivacy {
        #[ts(type = "number")]
        conversation_id: i64,
        shared: bool,
    },
    /// User clicked the "compact" button in the context usage indicator.
    /// Backend asks the LLM to persist state and call RequestCompaction.
    RequestCompaction,
    /// Run a target handler (e.g. import) on previously uploaded files.
    /// Files must have been uploaded via POST /upload with a target field;
    /// upload_ids reference those pending uploads.
    RunTarget {
        /// Target name (e.g. "import").
        target: String,
        /// Upload IDs from the Phase 1 HTTP upload.
        upload_ids: Vec<String>,
    },

    /// Request a fresh todo list from the graf integration.
    TodoRefresh,
    /// Mark a task as done.
    TodoDone {
        path: String,
        /// Repo slug (from `TodoItem.repo`). Required for multi-repo manifests.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        repo: Option<String>,
        /// Browser-local today (authoritative — the backend refuses to
        /// guess via server wall-clock because backend and browser can
        /// disagree on timezone). Required: the frontend always sends
        /// `localTodayStr()` here; serde rejects the message at
        /// deserialize if the field is absent.
        #[ts(type = "string")]
        completion_date: chrono::NaiveDate,
    },
    /// Set a task's tentative date.
    TodoSchedule {
        path: String,
        /// Repo slug (from `TodoItem.repo`). Required for multi-repo manifests.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        repo: Option<String>,
        /// ISO 8601 date (YYYY-MM-DD).
        #[ts(type = "string")]
        date: chrono::NaiveDate,
    },
    /// Reorder a task relative to its neighbors.
    /// At least one of `after`/`before` must be `Some`.
    TodoReorder {
        path: String,
        /// Repo slug (from `TodoItem.repo`). Required for multi-repo manifests.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        repo: Option<String>,
        /// Place after this neighbor.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        after: Option<TodoAnchor>,
        /// Place before this neighbor.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        before: Option<TodoAnchor>,
    },
    /// Request a page of older (simplified) history.
    /// Backend responds with `HistoryPage`. Only valid when the server
    /// indicated older history is available via `HistoryComplete.oldest_loaded_seq`.
    LoadMoreHistory {
        #[ts(type = "number")]
        before_seq: i64,
    },

    // ── PWA Push (§2.6.1) ──────────────────────────────────────────────────
    /// Request the server's VAPID public key for `PushManager.subscribe()`.
    /// Only valid when the active app has `pwa_push.enabled = true`.
    PushVapidKeyRequest,

    /// Register or replace a push subscription for this `(device, user)` pair.
    /// `p256dh` and `auth` are base64url-encoded (URL_SAFE_NO_PAD).
    /// `p256dh`: 65-byte uncompressed P-256 public key (87 base64url chars).
    /// `auth`: 16-byte auth secret (~22 base64url chars).
    PushSubscribe {
        endpoint: String,
        p256dh: String,
        auth: String,
    },

    /// Delete the push subscription for this `(device, user)` pair.
    PushUnsubscribe,

    /// User-initiated viewport/layout geometry snapshot for diagnosing the
    /// intermittent Chrome-Android-PWA off-screen-input bug. Delivered via
    /// the system-message dual-delivery path (persisted + broadcast as a
    /// neutral collapsed card, sent to CC as a `<brenn-debug-snapshot>`-tagged
    /// host message). NOT a security/fail2ban anomaly — a legitimate, expected
    /// debug action. No `record_usage` / `EventType` on purpose: this is a
    /// diagnostic, not a usage-billable user action.
    DebugViewportSnapshot {
        snapshot: Box<DebugViewportSnapshotData>,
    },
}

/// Reusable bounding-client-rect for an element. All fields in CSS pixels.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub struct RectData {
    pub top: f64,
    pub left: f64,
    pub right: f64,
    pub bottom: f64,
    pub width: f64,
    pub height: f64,
}

/// `window.visualViewport` snapshot. Absent when the API is not supported.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub struct VisualViewportData {
    pub width: f64,
    pub height: f64,
    pub offset_top: f64,
    pub offset_left: f64,
    pub page_top: f64,
    pub page_left: f64,
    pub scale: f64,
}

/// Single-snapshot geometry payload for `WsClientMessage::DebugViewportSnapshot`.
///
/// All values are read-only observations from the browser at capture time.
/// Missing elements or absent APIs are represented as `None`/`null`, not as
/// errors — a partial snapshot is still a valid, useful snapshot (AC2).
#[derive(Debug, Clone, Default, Serialize, Deserialize, TS)]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub struct DebugViewportSnapshotData {
    // ── Viewport / window scalars ────────────────────────────────────────────
    pub inner_width: f64,
    pub inner_height: f64,
    pub document_element_client_width: f64,
    pub document_element_client_height: f64,
    pub document_element_scroll_height: f64,
    pub scroll_x: f64,
    pub scroll_y: f64,
    pub scrolling_element_scroll_top: Option<f64>,
    pub scrolling_element_scroll_left: Option<f64>,
    pub device_pixel_ratio: f64,
    pub screen_width: f64,
    pub screen_height: f64,
    pub screen_orientation_type: Option<String>,
    pub display_mode_standalone: bool,
    pub max_width_768: bool,

    // ── Visual viewport ──────────────────────────────────────────────────────
    pub visual_viewport: Option<VisualViewportData>,

    // ── Element bounding-client-rects ────────────────────────────────────────
    /// `#input` (the chat textarea).
    pub input: Option<RectData>,
    /// `brenn-input-bar` host element.
    pub input_bar: Option<RectData>,
    /// `.app-main`.
    pub app_main: Option<RectData>,
    /// `brenn-pane-layout`.
    pub pane_layout: Option<RectData>,
    /// `brenn-message-list` (the message-list scroll container).
    pub message_list: Option<RectData>,
    /// `.attachment-strip` (optional, may be absent from DOM).
    pub attachment_strip: Option<RectData>,
    /// `.chip-bar` (optional, may be absent from DOM).
    pub chip_bar: Option<RectData>,
    /// `.presence-bar` (optional, may be absent from DOM).
    pub presence_bar: Option<RectData>,
    /// `.steal-bar` (optional, conditionally rendered when another session can
    /// be stolen; `showStealButton` in app.ts). Absence of this field in
    /// the snapshot indicates the bar was not present at capture time, which
    /// must otherwise be inferred from app state as a manual side-channel check.
    pub steal_bar: Option<RectData>,
    /// `brenn-status-bar`.
    pub status_bar: Option<RectData>,
    /// `document.body`.
    pub body: Option<RectData>,
    /// `document.documentElement`.
    pub document_element: Option<RectData>,

    // ── Message-list scroll metrics ──────────────────────────────────────────
    pub message_list_scroll_top: Option<f64>,
    pub message_list_scroll_height: Option<f64>,
    pub message_list_client_height: Option<f64>,

    // ── Derived booleans (AC6 — the binding observable contract) ────────────
    /// `input.bottom > visualViewport.offsetTop + visualViewport.height`.
    /// `None` when the input rect or visual viewport is absent.
    pub input_bottom_below_visual_fold: Option<bool>,
    /// `input.bottom > window.innerHeight`.
    /// `None` when the input rect is absent.
    pub input_bottom_below_layout: Option<bool>,

    // ── Computed styles ──────────────────────────────────────────────────────
    pub html_height: Option<String>,
    pub body_height: Option<String>,
    pub body_overflow: Option<String>,
    pub input_bar_position: Option<String>,
    pub input_bar_flex_shrink: Option<String>,
    /// `getComputedStyle('.app-main', 'min-height')`.
    pub app_main_min_height: Option<String>,
    /// `getComputedStyle('.app-main', 'height')` — complement to `app_main_min_height`.
    pub app_main_height: Option<String>,

    // ── Computed styles of flex chain below .app-main ────────────────────────
    /// `getComputedStyle(brenn-pane-layout, 'min-height')` — resolves `:host` rule.
    pub pane_layout_min_height: Option<String>,
    /// `getComputedStyle(brenn-pane-layout, 'height')` — distinct from rect height.
    pub pane_layout_height: Option<String>,
    /// `getComputedStyle(brenn-message-list, 'min-height')` — `:host` value.
    pub message_list_min_height: Option<String>,
    /// `getComputedStyle(brenn-message-list, 'height')` — catches flex-basis mismatch.
    pub message_list_height: Option<String>,
    /// `getComputedStyle('.mobile-slot-content', 'min-height')` — SinglePane wrapper.
    pub mobile_slot_content_min_height: Option<String>,

    // ── Bounding rects of internal header elements ───────────────────────────
    /// `.app-topbar` bounding rect — first child inside `.app-main`.
    pub app_topbar: Option<RectData>,
    /// Outer `.app-header` bounding rect — child of `<body>` (`app.rs:222`).
    /// Together with `app_topbar` forms the double-header check.
    pub app_header: Option<RectData>,
    /// `.app-layout` bounding rect — the `flex:1` row between `brenn-app` and `.app-main`.
    pub app_layout: Option<RectData>,

    // ── Root scalar ──────────────────────────────────────────────────────────
    /// `documentElement.offsetHeight` — disambiguates border-box vs content-box at root.
    pub document_element_offset_height: Option<f64>,

    // ── Safe-area insets (probe via throwaway element) ───────────────────────
    pub safe_area_inset_top: Option<String>,
    pub safe_area_inset_right: Option<String>,
    pub safe_area_inset_bottom: Option<String>,
    pub safe_area_inset_left: Option<String>,

    // ── Viewport-unit probes ─────────────────────────────────────────────────
    /// `getBoundingClientRect().height` of a transient 100vh-sized probe element.
    /// Documents the `vh` unit's resolved value independent of which unit `html` uses.
    pub probe_100vh_px: Option<f64>,
    /// Same probe for `100svh` (small viewport height).
    pub probe_100svh_px: Option<f64>,
    /// Same probe for `100lvh` (large viewport height).
    pub probe_100lvh_px: Option<f64>,
    /// Same probe for `100dvh` (dynamic viewport height).
    /// On the affected device this resolves to ~889.5px vs innerHeight=833px;
    /// the discrepancy is the root cause of the offscreen-input bug.
    pub probe_100dvh_px: Option<f64>,
    /// CSS unit strings (e.g. `"100dvh"`) for which the probe threw an exception
    /// rather than resolving normally. Distinct from `null` probe fields, which
    /// mean the unit was not supported (caught silently via `CSS.supports`).
    /// `None` means no exceptions occurred (most common case).
    pub probe_exception_units: Option<Vec<String>>,

    // ── Window-bounds scalars ────────────────────────────────────────────────
    /// `screen.availHeight` — visible screen height excluding taskbars (CSS px).
    /// These are always present in browser JS environments; not Option.
    pub screen_avail_height: f64,
    /// `window.outerHeight` — total outer window height (CSS px).
    /// These are always present in browser JS environments; not Option.
    pub window_outer_height: f64,

    // ── Environment ──────────────────────────────────────────────────────────
    pub user_agent: String,
    pub ua_brands: Option<Vec<String>>,
    pub ua_mobile: Option<bool>,
    pub active_element_tag: Option<String>,
    pub active_element_id: Option<String>,
    pub visibility_state: String,
    /// ISO 8601 timestamp from the browser at capture time.
    pub client_timestamp: String,
    /// Build id from `BUILD_ID` (`build-info.ts`).
    pub build_id: String,
}

/// A reference to a neighbor task used as an anchor for reorder operations.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub struct TodoAnchor {
    pub path: String,
    /// Repo slug. Required for multi-repo manifests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
}

/// CC's reported permission mode. `Auto` is the expected value (matching
/// Brenn's `--permission-mode auto` spawn flag). Any other string from CC
/// lands in `Other`, which the backend alerts on; the raw string is
/// preserved so alert details are meaningful.
///
/// Serde treats the wire as a bare string via `#[serde(from = "String",
/// into = "String")]`. Round-trips byte-identically for known values.
/// The `#[ts(type)]` override on the `mode` field in `WsServerMessage`
/// inlines the string union directly; no separate generated TS file exists
/// for this type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(from = "String", into = "String")]
pub enum PermissionModeValue {
    Auto,
    Other(String),
}

impl From<String> for PermissionModeValue {
    fn from(s: String) -> Self {
        match s.as_str() {
            "auto" => Self::Auto,
            _ => Self::Other(s),
        }
    }
}

impl From<PermissionModeValue> for String {
    fn from(v: PermissionModeValue) -> String {
        match v {
            PermissionModeValue::Auto => "auto".into(),
            // Serialize unknown modes as "other" so the wire type matches the
            // closed TS union ("auto" | "other" | null). The raw string is
            // retained in the Other(s) payload for backend logging/alerting
            // only — it never reaches the wire.
            PermissionModeValue::Other(_) => "other".into(),
        }
    }
}

impl std::fmt::Display for PermissionModeValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s: String = self.clone().into();
        f.write_str(&s)
    }
}

/// Structured error code from graf's todo mutation output. Known variants
/// correspond to the codes emitted by `graf::todo::DoneError::code()`.
/// Unknown codes deserialize to `Other` (serialized as `"other"`).
/// Display delegates to serde serialization for use in log macros.
/// Backend-internal only — not exported to TypeScript.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TodoErrorCode {
    StaleAnchor,
    SkipPastUnnecessary,
    SkipPastOnSlipRule,
    SkipPastOnNonRecurring,
    AlreadyDoneOrNotDueYet,
    ReplacingOnRecurring,
    AlreadyTerminal,
    Invalid,
    Infra,
    #[serde(other)]
    Other,
}

impl std::fmt::Display for TodoErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::StaleAnchor => "stale_anchor",
            Self::SkipPastUnnecessary => "skip_past_unnecessary",
            Self::SkipPastOnSlipRule => "skip_past_on_slip_rule",
            Self::SkipPastOnNonRecurring => "skip_past_on_non_recurring",
            Self::AlreadyDoneOrNotDueYet => "already_done_or_not_due_yet",
            Self::ReplacingOnRecurring => "replacing_on_recurring",
            Self::AlreadyTerminal => "already_terminal",
            Self::Invalid => "invalid",
            Self::Infra => "infra",
            Self::Other => "other",
        })
    }
}

/// Service worker → app page `postMessage` channel.
///
/// Every message the SW sends to an `/app/*` client is typed here. The SW
/// emits `satisfies SwToAppMessage` literals; receivers pattern-match on
/// `data.type`. `#[serde(tag = "type")]` makes ts-rs emit a discriminated
/// union directly — no boundary adapter needed on this channel.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(tag = "type")]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub enum SwToAppMessage {
    /// SW → app: navigate to `url` (derived from a push notification click).
    /// `url` is a same-origin `/app/*` path; the receiver validates both
    /// invariants before calling `window.location.assign`.
    NavigateTo { url: String },
    /// SW → app: the push subscription has changed (Firefox subscription
    /// rotation). The receiver re-subscribes from scratch via `_handleEnablePush()`;
    /// forwarding the SW's already-issued subscription would be redundant.
    /// If the receiver ever short-circuits to `setApplicationServerKey`, this
    /// variant needs a `new_subscription` field.
    PushSubscriptionChanged,
    /// SW → app: per-checkpoint trace event from `handleNotificationClick`.
    PushClickTrace {
        #[ts(type = "number | null")]
        user_id: Option<i64>,
        event: PushClickTraceEvent,
    },
}

/// Backend → Browser message.
///
/// Protocol convention: no `#[serde(other)]` on TS-exported enums.
/// Backend-internal catch-all enums must not derive `TS`.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(tag = "type")]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub enum WsServerMessage {
    /// Streaming token fragment from CC (text content).
    StreamToken { token: String },
    /// Streaming token fragment from CC (thinking/reasoning content).
    ThinkingToken { token: String },
    /// Complete assistant message (final, replaces accumulated stream tokens).
    /// `content` is rendered HTML from the server-side markdown pipeline.
    /// `seq` is the DB sequence number, present during history replay for
    /// incremental reconnect tracking. Omitted for live messages.
    AssistantMessage {
        content: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[ts(type = "number | null")]
        seq: Option<i64>,
    },
    /// CC requests permission to use a tool (synchronous — CC blocks waiting).
    PermissionRequest {
        request_id: String,
        /// Tool name, kept for logging/debugging. The frontend does not use this
        /// for display or dispatch — all display data is embedded in `formatted_display`.
        tool_name: String,
        #[ts(type = "Record<string, unknown>")]
        tool_input: serde_json::Value,
        /// Pre-rendered HTML display of the tool input, formatted by the backend.
        /// Contains an interactive component (`<brenn-tool-approve>` or a custom
        /// element) that handles user interaction and dispatches events.
        formatted_display: String,
    },
    /// A pending permission request was cancelled by CC.
    PermissionCancelled { request_id: String },
    /// A pending permission request was resolved by a user (in any tab).
    PermissionResolved {
        request_id: String,
        decision: PermissionDecision,
    },
    /// An interactive tool card for user action (asynchronous — CC already continued).
    ToolCardRequest {
        request_id: String,
        /// Tool name, kept for logging/debugging.
        tool_name: String,
        #[ts(type = "Record<string, unknown>")]
        tool_input: serde_json::Value,
        /// Pre-rendered HTML display of the tool card, formatted by the backend.
        formatted_display: String,
    },
    /// An interactive tool card was resolved by a user (in any tab).
    ToolCardResolved {
        request_id: String,
        decision: ToolResponseDecision,
    },
    /// CC session state machine transition.
    Status { state: CcState },
    /// Actionable error with human-readable description (e.g. CC died, spawn failed).
    /// Typically followed by a Status message with the resulting state.
    Error { message: String },
    /// Full list of conversations for the sidebar.
    ConversationList {
        conversations: Vec<ConversationSummary>,
    },
    /// Signals which conversation is now active in this tab.
    /// `None` means no active conversation (empty state).
    ///
    /// `is_owner` and `shared` are meaningful only when `conversation_id` is `Some`.
    /// When `conversation_id` is `None`, they default to `true` and `false` respectively
    /// (vacuously — no conversation, no restriction).
    ConversationSwitched {
        #[ts(type = "number | null")]
        conversation_id: Option<i64>,
        state: CcState,
        /// Whether the receiving user owns this conversation.
        is_owner: bool,
        /// Whether this conversation is shared (visible to all app users).
        shared: bool,
        /// When true, the frontend must clear and reload even if the conversation_id
        /// hasn't changed. Used for recovery after broadcast lag or mpsc buffer overflow.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        reload: bool,
    },
    /// Signals the end of history replay. Frontend should scroll to bottom.
    /// `oldest_loaded_seq` is the cursor for backward pagination: when `Some`,
    /// older history is available via `LoadMoreHistory`. When `None`, the full
    /// history was sent.
    HistoryComplete {
        /// Seq of the oldest message in the initial replay batch.
        /// `None` when the full history was replayed (no pagination needed).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[ts(type = "number | null")]
        oldest_loaded_seq: Option<i64>,
    },
    /// Echo of a chat-input message — text typed by a human user into the
    /// chat input field of some Brenn session. Carries the sender's identity,
    /// attachments, and any selected-task context.
    ///
    /// System-origin messages use `SystemMessageBroadcast` instead. The
    /// variant tag is the discriminator; invalid combinations (e.g. system
    /// content in a `UserMessageEcho`) are unrepresentable by construction.
    UserMessageEcho {
        text: String,
        /// Username of the sender (resolved from user_id).
        username: String,
        /// ISO 8601 timestamp with timezone offset in the sender's local time.
        timestamp: String,
        /// Metadata for attached files (empty vec if none).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        attachments: Vec<AttachmentMeta>,
        /// Selected tasks from the todo list, sent as context with this message.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        selected_tasks: Vec<SelectedTask>,
        /// DB sequence number for incremental reconnect. Omitted for live echoes.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[ts(type = "number | null")]
        seq: Option<i64>,
    },
    /// Broadcast of a Brenn-generated system message. The visible payload is
    /// the pre-rendered HTML card; the LLM-facing text never reaches the browser.
    /// The category drives per-category CSS exceptions.
    SystemMessageBroadcast {
        /// Pre-rendered HTML — the entire `<details class="brenn-system ...">` block.
        rendered_html: String,
        /// Category tag, used by the frontend to apply per-category CSS classes.
        category: SystemMessageCategory,
        /// ISO 8601 timestamp; kept for ordering / debugging.
        timestamp: String,
        /// DB sequence number for incremental reconnect. Omitted for live broadcasts.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[ts(type = "number | null")]
        seq: Option<i64>,
    },
    /// Rendered markdown file content for the artifact viewer.
    ArtifactContent {
        /// Display name (relative to cwd).
        file_path: String,
        /// Pre-rendered markdown HTML from the server-side pipeline.
        rendered_html: String,
        /// Raw file source (the markdown that was rendered). Sent so the
        /// frontend can offer a Copy button without a round-trip. Always
        /// present — matches exactly what `rendered_html` was rendered from,
        /// including for historical snapshots whose on-disk file may differ.
        raw_content: String,
        /// Snapshot metadata when loaded from DB storage. None for live disk reads.
        snapshot: Option<SnapshotMetadata>,
        /// DB sequence number for incremental reconnect. Omitted for live artifact views.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[ts(type = "number | null")]
        seq: Option<i64>,
    },
    /// Read-only summary of a completed tool use, rendered inline in the chat history.
    /// Backend renders tool-aware HTML; frontend mechanism is generic.
    ToolUseSummary {
        tool_name: String,
        /// Pre-rendered HTML summary (compact, read-only).
        rendered_summary: String,
        /// Pre-rendered HTML detail (expanded view: approval info, input, result).
        /// None for app-specific tools that provide their own `format_summary`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail_html: Option<String>,
        /// DB sequence number for incremental reconnect. Omitted for live summaries.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[ts(type = "number | null")]
        seq: Option<i64>,
    },
    /// Index of all artifact files and their versions for a conversation.
    /// Sent after history replay, after live DisplayFile, and on conversation clear.
    ArtifactIndex { files: Vec<ArtifactFileInfo> },
    /// This tab's CC session was stolen by another user/tab (single_instance enforcement).
    SessionStolen { message: String },
    /// App is busy — another session is active (single_instance enforcement).
    /// Frontend should offer the user the option to steal.
    AppBusy { message: String },
    /// Sent once immediately after WS upgrade. Gives the frontend its identity
    /// and app-level flags needed for rendering decisions.
    Welcome {
        /// Current user's username.
        username: String,
        /// Numeric user id (`users.id` rowid). Used by the service worker's
        /// `signed_in_user_ids` IndexedDB set (§2.6.3) to guard push delivery.
        #[ts(type = "number")]
        user_id: i64,
        /// Whether this app has multiuser mode enabled.
        multiuser: bool,
        /// Whether this app is singleton (one conversation per user, no conversation list).
        singleton: bool,
        /// Available models for the model picker. Empty on first connect if no
        /// CC subprocess has been spawned yet; updated via `ModelsAvailable`.
        #[serde(default)]
        available_models: Vec<ModelInfo>,
        /// The app's configured default model (e.g. "sonnet").
        default_model: String,
        /// App-defined attachment targets (e.g. "Import bank export").
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        attachment_targets: Vec<TargetInfo>,
        /// Whether the active app has `pwa_push.enabled = true`. Frontend uses
        /// this to decide whether to render the "Enable push" affordance.
        #[serde(default)]
        pwa_push_enabled: bool,
    },
    /// Pushes the available model list to all connected clients.
    /// Sent after the first CC subprocess spawns and reports its models.
    ModelsAvailable { available_models: Vec<ModelInfo> },
    /// Current list of users present in a conversation. Sent as a full snapshot
    /// (not deltas) on attach and whenever presence changes.
    PresenceUpdate {
        #[ts(type = "number")]
        conversation_id: i64,
        users: Vec<PresenceUser>,
    },
    /// Tells the frontend which pane layout to use. Sent in the Welcome sequence
    /// (with default) and in response to SetViewportClass.
    SetLayout { layout: PaneLayout },
    /// An "Always Allow" rule creation failed (invalid pattern).
    /// The approval request remains pending — the dialog stays open.
    ApprovalRuleError { request_id: String, error: String },
    /// Conversation privacy changed. Broadcast to all subscribers on the bridge.
    /// Non-owner frontends self-eject when `shared` becomes `false`.
    PrivacyChanged {
        #[ts(type = "number")]
        conversation_id: i64,
        shared: bool,
    },
    /// Result from an attachment target handler (e.g. a command that ran on uploaded files).
    TargetResult {
        /// Target name (e.g. "import").
        target: String,
        /// Whether the handler succeeded (exit code 0 for commands).
        success: bool,
        /// Human-readable plain text summary for display.
        summary: String,
        /// Full command output for debugging (stdout + stderr).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
        /// Original filenames that were processed.
        files: Vec<String>,
        /// DB sequence number. Present during history replay, absent for live messages.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[ts(type = "number | null")]
        seq: Option<i64>,
    },
    /// CC's reported permission mode from the init frame. Sent once per
    /// CC spawn so the UI can display a pill next to the context-usage
    /// indicator. Expected value is "auto" (matches Brenn's spawn flag).
    /// If `mode != "auto"`, the backend has already warned and alerted;
    /// the frontend is responsible only for rendering the pill with an
    /// appropriate warning style.
    PermissionMode {
        /// The mode CC reported (e.g. `Auto`, or `Other("default")`), or None
        /// if CC omitted the field in the init frame. The wire representation
        /// is a bare string (e.g. `"auto"` or `"other"`). `PermissionModeValue`
        /// keeps its hand-written serde strategy so the backend retains the raw
        /// unknown string for alerts; the ts-rs override inlines the string
        /// union here rather than generating a separate TS file.
        // SYNC: status-bar.ts `PermissionModeValue` type alias must list the same
        // string literals as this override. Adding a known mode here requires a
        // matching update there; `tsc` will catch mismatches on rebuild.
        #[ts(type = "\"auto\" | \"other\" | null")]
        mode: Option<PermissionModeValue>,
    },
    /// Current context usage from a `/context` query. Sent to browsers
    /// after each internal `/context` check so the UI can display context
    /// fullness and color-code thresholds.
    ContextUsage {
        /// Context usage as a percentage (0-100).
        usage_pct: u8,
        /// Current tokens in context.
        #[ts(type = "number")]
        current_tokens: u64,
        /// Maximum tokens for the model's context window.
        #[ts(type = "number")]
        max_tokens: u64,
        /// Threshold for yellow (warning) indicator — `reminder_pct` from config.
        reminder_pct: u8,
        /// Threshold for red (danger) indicator — `red_pct` from config.
        red_pct: u8,
        /// Absolute reminder-stage token threshold (yellow indicator).
        /// `None` if not configured. When set, the reminder stage fires at
        /// this absolute token count regardless of the percentage threshold.
        #[ts(type = "number | null")]
        reminder_tokens: Option<u64>,
        /// Absolute red-stage token threshold (red indicator).
        /// `None` if not configured. When set, the red stage fires at this
        /// absolute token count regardless of the percentage threshold.
        #[ts(type = "number | null")]
        red_tokens: Option<u64>,
    },

    /// Full todo list state. Sent on connect (for apps with graf) and
    /// after mutations.
    TodoState {
        tasks: Vec<TodoItem>,
        lint_errors: Vec<TodoLintError>,
        /// Sharing domains from the graf manifest. `None` when no manifest
        /// is active (single-repo mode).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        domains: Option<Vec<String>>,
        /// "Today" in the connection's timezone — the server's authoritative
        /// answer to the browser's sectioning question (Overdue / Today /
        /// Tomorrow / Weekday / Future). The frontend was computing this
        /// from `new Date()` in browser-local time, which disagreed with
        /// graf's date math when the browser TZ and graf's resolved zone
        /// differed. See `docs/designs/graf-user-tz.md`.
        #[ts(type = "string")]
        today: chrono::NaiveDate,
    },
    /// A page of simplified older history, sent in response to `LoadMoreHistory`.
    /// Messages are in chronological order (oldest first). The frontend prepends
    /// them to the chat with scroll position preservation.
    HistoryPage {
        messages: Vec<HistoryPageMessage>,
        has_more: bool,
    },
    /// Response to a `TodoDone` request. Carries the full done-result payload
    /// including done-specific fields (completion_date, terminal, etc.).
    TodoDoneResult {
        /// Which task was acted on.
        path: String,
        /// Repo slug the task belongs to. `None` when the app has a single
        /// repo (path alone identifies the task). Used by the frontend to
        /// build the exact `todoKey(repo, path)` match — avoids the suffix
        /// scan that misfires when two repos have tasks at the same path.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        repo: Option<String>,
        success: bool,
        /// Human-readable error message on failure.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
        /// The date recorded as `on_date`/`end_date` (non-recurring or
        /// anchored-exhaustion completion).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[ts(type = "string | null")]
        completion_date: Option<chrono::NaiveDate>,
        /// True when this advance was the final rrule occurrence (or
        /// the task was non-recurring). Absent when the recurrence
        /// continues.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        terminal: Option<bool>,
        /// Next `check_in_date` on recurring non-terminal completions.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[ts(type = "string | null")]
        next_check_in_date: Option<chrono::NaiveDate>,
        /// Next `due_date` on recurring non-terminal completions whose
        /// schedule is anchored by `due_date`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[ts(type = "string | null")]
        next_due_date: Option<chrono::NaiveDate>,
        /// Slip idempotent no-op flag: task already had a log entry for
        /// the requested `completion_date`. No file write occurred.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        already_done: Option<bool>,
        /// The existing log entry that triggered the slip no-op.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        existing_entry: Option<CompletionLogEntry>,
        /// Whether a user-supplied comment was discarded (slip no-op path only).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        comment_discarded: Option<bool>,
    },

    /// Response to a non-done todo mutation (schedule, reorder, snooze).
    /// Carries only the four shared fields — done-specific fields are not
    /// present, making illegal states (e.g. terminal on a schedule result)
    /// unrepresentable.
    TodoMutationResult {
        /// Which task was acted on.
        path: String,
        /// Repo slug the task belongs to. `None` when the app has a single
        /// repo (path alone identifies the task).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        repo: Option<String>,
        success: bool,
        /// Human-readable error message on failure.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },

    /// Per-turn cost telemetry. Broadcast after each turn completion, after
    /// the corresponding `ContextUsage`. Frontend renders next to the context
    /// pill. Field names are snake_case on the wire (no rename_all on
    /// WsServerMessage); TypeScript receives them as `last_turn_usd`, etc.
    CostUsage {
        /// Cost of the most recent turn (cumulative current minus cumulative
        /// previous). On the first turn of a fresh session this equals
        /// `since_last_compaction_usd`. On a turn immediately after compaction,
        /// this is the cost of the compaction operation itself (near zero).
        last_turn_usd: f64,
        /// Cumulative session cost since the last `/compact`, taken straight
        /// from CC's `result.total_cost_usd` (CC resets at compaction).
        since_last_compaction_usd: f64,
        /// Process-wide sum of `last_turn_usd` samples across every conversation
        /// served by this brenn instance in the last 24 wall-clock hours.
        last_24h_usd: f64,
    },

    // ── PWA Push (§2.6.1) ──────────────────────────────────────────────────
    /// Server VAPID public key for `PushManager.subscribe({ applicationServerKey })`.
    /// Sent in response to `PushVapidKeyRequest`.
    /// `public_key_b64url`: base64url-encoded uncompressed P-256 public key (87 chars).
    PushVapidKey { public_key_b64url: String },

    /// Current push subscription state for this `(device, user)` pair.
    /// Sent after `PushSubscribe`, `PushUnsubscribe`, and on WS connect when
    /// a subscription record exists.
    PushEnabled { enabled: bool },
}

impl WsServerMessage {
    /// Construct a `TodoMutationResult` from the four shared fields.
    ///
    /// Centralises construction so adding a field to the variant requires
    /// only one edit here, not at every call site.
    pub fn todo_mutation_result(
        path: &str,
        repo: Option<&str>,
        success: bool,
        error: Option<String>,
    ) -> Self {
        Self::TodoMutationResult {
            path: path.to_string(),
            repo: repo.map(str::to_string),
            success,
            error,
        }
    }

    /// Construct a `TodoDoneResult` failure shell.
    ///
    /// Centralises failure construction so adding a field to `TodoDoneResult`
    /// requires only one edit here, parallel to `todo_mutation_result`.
    /// All done-specific fields (`completion_date`, `terminal`, etc.) are `None`
    /// — they are only meaningful on success and are absent on failure.
    pub fn todo_done_failure(path: &str, repo: Option<&str>, error: String) -> Self {
        Self::TodoDoneResult {
            path: path.to_string(),
            repo: repo.map(str::to_string),
            success: false,
            error: Some(error),
            completion_date: None,
            terminal: None,
            next_check_in_date: None,
            next_due_date: None,
            already_done: None,
            existing_entry: None,
            comment_discarded: None,
        }
    }

    /// Construct a `TodoDoneResult` success shell.
    ///
    /// Centralises success construction so adding a field to `TodoDoneResult`
    /// requires only one edit here, parallel to `todo_done_failure`.
    #[allow(clippy::too_many_arguments)]
    pub fn todo_done_success(
        path: &str,
        repo: Option<&str>,
        completion_date: Option<chrono::NaiveDate>,
        terminal: Option<bool>,
        next_check_in_date: Option<chrono::NaiveDate>,
        next_due_date: Option<chrono::NaiveDate>,
        already_done: Option<bool>,
        existing_entry: Option<CompletionLogEntry>,
        comment_discarded: Option<bool>,
    ) -> Self {
        Self::TodoDoneResult {
            path: path.to_string(),
            repo: repo.map(str::to_string),
            success: true,
            error: None,
            completion_date,
            terminal,
            next_check_in_date,
            next_due_date,
            already_done,
            existing_entry,
            comment_discarded,
        }
    }
}

/// One entry in a task's completion log. Mirrors the completion-log entry
/// shape in graf's frontmatter schema.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub struct CompletionLogEntry {
    #[ts(type = "string")]
    pub completed: chrono::NaiveDate,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(type = "string | null")]
    pub occurrence: Option<chrono::NaiveDate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// A simplified message for backward history pagination.
///
/// Contains only user text and assistant text — no tool summaries, artifacts,
/// or other side-effect-bearing message types. Pre-rendered as HTML by the
/// backend so the frontend can simply set innerHTML.
///
/// The `rendered_html` field carries different shapes depending on `category`:
/// - `category` is `Some(_)`: `rendered_html` is the full
///   `<details class="brenn-system ...">` block, identical to what
///   `SystemMessageBroadcast.rendered_html` carries on the live path.
/// - `category` is `None`: `rendered_html` is chat-bubble inner HTML
///   (user-escaped text or assistant markdown).
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub struct HistoryPageMessage {
    /// DB sequence number for cursor-based pagination.
    #[ts(type = "number")]
    pub seq: i64,
    /// "user" or "assistant". For system-origin rows `category` is the
    /// dispositive signal; this field retains "user" to match the DB row
    /// type but the frontend dispatches on `category` first.
    pub role: String,
    /// Pre-rendered HTML content. Shape depends on `category` — see struct doc.
    pub rendered_html: String,
    /// ISO 8601 timestamp.
    pub timestamp: String,
    /// Username of the sender (present for chat user messages, absent for
    /// assistant and system rows).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// Present iff this row is a system-origin row (post-1e8bb906). When
    /// present, `rendered_html` is the full `<details class="brenn-system …">`
    /// block. When absent, `rendered_html` is chat-bubble inner HTML.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category: Option<SystemMessageCategory>,
    /// Attachments on this message. Non-empty only for chat-input user messages.
    /// Absent from JSON when empty (matches `UserMessageEcho` convention).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<AttachmentMeta>,
}

/// A single task from the graf todo system.
///
/// Mirrors graf's `TodoItem` JSON serialization. Fields are passed through
/// to the frontend for display — Brenn doesn't interpret status/effort values.
///
/// `effective_date` is non-optional by contract: graf's `todo_query`
/// partitions rows with null `effective_date` into `lint_errors` rather
/// than `tasks`, so any `TodoItem` that reaches brenn has a concrete
/// date. This is the fail-fast site for that invariant — a null value
/// from graf fails `serde_json::from_str` at the subprocess boundary
/// (see `brenn-graf/src/subprocess.rs::query_todos`).
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub struct TodoItem {
    pub path: String,
    pub tldr: String,
    /// Repo slug (e.g. "life", "eng"). Present when a manifest is active.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    /// Sharing domain (e.g. "example.org/personal"). Present when a manifest
    /// is active and the repo declares a domain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[ts(type = "string")]
    pub effective_date: chrono::NaiveDate,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(type = "string | null")]
    pub tentative_date: Option<chrono::NaiveDate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(type = "string | null")]
    pub check_in_date: Option<chrono::NaiveDate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(type = "string | null")]
    pub due_date: Option<chrono::NaiveDate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(type = "number | null")]
    pub sort_order: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(type = "number | null")]
    pub priority: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rrule: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<String>,
}

/// A lint error from the graf todo system.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, TS)]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub struct TodoLintError {
    pub path: String,
    pub message: String,
    /// Repo slug. Present when a manifest is active.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
}

/// Attachment target metadata sent to the frontend in the Welcome message.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub struct TargetInfo {
    /// Target slug (e.g. "import").
    pub name: String,
    /// Human-readable label for UI (e.g. "Import bank export").
    pub label: String,
    /// Accepted file extensions (e.g. [".ofx", ".csv"]).
    pub accept: Vec<String>,
    /// Whether multiple files can be uploaded at once.
    pub multi: bool,
}

/// A model available for selection, sourced from CC's init response.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub struct ModelInfo {
    /// The value to pass to CC (e.g. "default", "sonnet", "haiku").
    pub value: String,
    /// Human-readable name (e.g. "Sonnet", "Haiku").
    pub display_name: String,
    /// Short description (e.g. "Sonnet 4.6 · Best for everyday tasks").
    pub description: String,
}

/// A user present in a conversation (for presence indicators).
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub struct PresenceUser {
    pub username: String,
}

/// Category of a system-generated message. Used to apply category-specific
/// CSS classes in the frontend and to drive the collapsed-card rendering path.
/// Carried by `SystemMessageBroadcast` (never by `UserMessageEcho`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub enum SystemMessageCategory {
    /// Received intra-Brenn messages (category 1).
    MessagesReceived,
    /// Event-queue drain (repo_sync / cron / discord) (category 2).
    EventDrain,
    /// Compaction soft reminder nudge (category 3).
    CompactionReminder,
    /// Compaction hard trigger persist message (category 4).
    CompactionHardTrigger,
    /// Compaction soft-trigger idle timer fired (category 5).
    CompactionIdlePrompt,
    /// Idle hooks fired (dirty-repo reminder) (category 6).
    IdleHook,
    /// User-initiated compaction request (category 7).
    CompactionUserRequest,
    /// UI tool error reported back to the LLM (category 8). Renders with
    /// a red border and expanded by default.
    UiError,
    /// Unassigned-device-slug reminder to the LLM (category 9).
    DeviceSlugReminder,
    /// Graf subprocess query failure surfaced to UI and LLM (category 10).
    /// Renders with a red border and expanded by default (same visual treatment
    /// as `UiError` but semantically distinct — graf subprocess failures are not
    /// user-attempted UI tool calls).
    GrafError,
    /// CC compaction attempt failed (category 11). Collapsed card in the chat
    /// thread so the user is aware without being disrupted.
    CompactionFailed,
    /// User-triggered viewport/layout geometry snapshot (category 12). Neutral
    /// collapsed card — not an error, not expanded. The LLM receives the full
    /// blob via the `<brenn-debug-snapshot>` envelope.
    DebugSnapshot,
}

/// Viewport class reported by the browser. The backend uses this to select
/// the appropriate pane layout. Not pixel dimensions — just a classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub enum ViewportClass {
    /// Single-column layout. Phone portrait, small tablet portrait.
    Compact,
    /// Multi-column layout. Desktop, tablet landscape, wide windows.
    Wide,
}

/// Pane layout directive from backend to frontend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "type")]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub enum PaneLayout {
    /// Single slot — everything navigates within it (mobile).
    SinglePane,
    /// Two side-by-side slots — chat left, files right (desktop).
    TwoColumn,
}

/// Reference to a previously uploaded file, sent in `SendMessage`.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub struct AttachmentRef {
    /// UUID from the upload endpoint. Parsed as UUID at the handling boundary
    /// (not during deserialization) so we can produce specific fail2ban signals.
    pub upload_id: String,
}

/// A selected task reference, sent alongside a user message for context injection.
///
/// The `ref` field uses graf's `slug:path` format (e.g. `"life:todo/buy-groceries.md"`)
/// or a bare path for single-repo manifests.
///
/// Intentionally contains no tldr / summary text: the ref is the authoritative
/// identifier, and anything beyond that (task title, status) is looked up by
/// CC via graf MCP. Keeping the wire payload minimal avoids a class of
/// self-rejection bugs where a backend-emitted tldr (length-uncapped in
/// `TodoItem`) would exceed a cap enforced only on `SelectedTask`.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub struct SelectedTask {
    /// "repo:path" or bare "path" — matches graf's slug:path convention.
    #[serde(rename = "ref")]
    #[ts(rename = "ref")]
    pub task_ref: String,
}

/// Attachment metadata sent in `UserMessageEcho` and history replay.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub struct AttachmentMeta {
    pub upload_id: String,
    pub filename: String,
    pub media_type: String,
    #[ts(type = "number")]
    pub size: u64,
}

/// Transient state of the CC interaction, for UI rendering.
///
/// Distinct from `ConversationStatus` (the DB model) which tracks the permanent
/// record (Active/Completed/Error). This tracks the live session state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub enum CcState {
    /// No CC running, ready for input.
    Idle,
    /// CC subprocess is spawning but not yet ready.
    Connecting,
    /// CC is processing.
    Thinking,
    /// Waiting for user to approve tool use.
    AwaitingApproval,
    /// CC is compacting its context.
    Compacting,
    /// Something went wrong.
    Error,
}

/// Decision for a CC permission request (synchronous): "may this tool run?"
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(tag = "decision")]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub enum PermissionDecision {
    /// Allow the tool use, optionally with modified input (e.g., AskUserQuestion answers).
    Allow {
        #[serde(skip_serializing_if = "Option::is_none")]
        #[ts(type = "Record<string, unknown> | null")]
        updated_input: Option<serde_json::Value>,
    },
    /// Deny the tool use with an optional reason.
    Deny { reason: Option<String> },
    /// Allow this request AND create rules for future matching requests.
    AlwaysAllow {
        /// Regex patterns to match against tool input (one rule created per pattern).
        /// For simple commands this has one element; for compound commands one per sub-command.
        patterns: Vec<String>,
        /// Where to store the rules.
        scope: RuleScope,
        /// The tool name these rules apply to (echoed from PermissionRequest.tool_name).
        tool_name: String,
    },
}

/// Decision for an async interactive tool card (PostToolUse): "here's what the user chose."
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(tag = "decision")]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub enum ToolResponseDecision {
    /// Allow/accept with optional selection data (e.g., selected proposal index).
    Allow {
        #[serde(skip_serializing_if = "Option::is_none")]
        #[ts(type = "Record<string, unknown> | null")]
        updated_input: Option<serde_json::Value>,
    },
    /// Deny/reject with an optional reason.
    Deny { reason: Option<String> },
}

/// Per-checkpoint trace event from the service worker's `handleNotificationClick`.
/// Each variant carries the data relevant to that checkpoint.
/// Tagged enum; tag is "type" to match the existing WsClientMessage style.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(tag = "type")]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub enum PushClickTraceEvent {
    /// Entry: parsed `event.notification.data`.
    HandlerEntry {
        #[ts(type = "number | null")]
        target_user_id: Option<i64>,
        target_path: String,
        redirector_url: String,
        payload_keys: Vec<String>,
    },
    /// `matchAll(...)` result — all window clients.
    MatchAllResult { clients: Vec<TraceClient> },
    /// After same-origin filter — which clients were kept and which dropped.
    BrennClientsFilter {
        kept: Vec<String>,
        dropped_with_reason: Vec<DroppedClient>,
    },
    /// T1: a client was chosen for focus; NavigateTo posted.
    T1Chosen {
        client_id: String,
        target_path: String,
        /// Whether `focus()` was rejected by the platform. When `true`,
        /// the NavigateTo message was still posted — the platform may
        /// surface the window via notification-click semantics.
        focus_rejected: bool,
    },
    /// T1: no same-origin Brenn clients; falling through to openWindow.
    T1Skipped { reason: String },
    /// T2/T3 path: `openWindow(url)` was called.
    OpenWindowCalled { url: String },
    /// T2/T3 path: result of `openWindow` (null → failed).
    OpenWindowResult { opened_url: Option<String> },
    /// Fenix (Firefox-Android) detected: T1 focus-existing cascade skipped;
    /// proceeding directly to T2/T3 openWindow via redirector.
    FenixCascadeSkipped,
    /// Final branch label.
    Terminal { branch: PushClickTerminalBranch },
}

/// Metadata snapshot of a window client at trace time.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub struct TraceClient {
    pub id: String,
    pub url: String,
    pub focused: bool,
    pub visibility_state: String,
    #[serde(rename = "type")]
    pub client_type: String,
}

/// A same-origin client excluded from the Brenn-client filter with an explanatory reason string.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub struct DroppedClient {
    pub id: String,
    pub url: String,
    pub reason: String,
}

/// Terminal branch label for the final `Terminal` trace event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub enum PushClickTerminalBranch {
    T1Posted,
    T2T3Opened,
    OpenWindowRejected,
    NoEligibleTarget,
}

/// Scope for an "Always Allow" approval rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub enum RuleScope {
    /// Rule applies to this conversation only (persists across restarts).
    Conversation,
    /// Rule applies to all conversations for this app.
    Permanent,
}

/// Summary of a conversation for the sidebar list.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub struct ConversationSummary {
    #[ts(type = "number")]
    pub id: i64,
    pub title: Option<String>,
    pub status: ConversationListStatus,
    pub model: Option<String>,
    pub updated_at: String,
    #[ts(type = "number")]
    pub message_count: i64,
    /// Whether this conversation is shared (multiuser).
    pub shared: bool,
    /// Username of the conversation owner, if it's someone else's shared conversation.
    pub owner: Option<String>,
}

/// User preferences for the frontend UI.
///
/// Not a WebSocket message type — lives here for ts-rs export convenience.
/// Rust uses snake_case; serde/ts-rs rename to camelCase for the JS side.
/// Frontend stores in localStorage for now; backend persistence is future work.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, export_to = "../../frontend/src/generated/")]
#[ts(rename_all = "camelCase")]
pub struct UserSettings {
    /// Whether pressing Enter sends the message (true) or inserts a newline (false).
    /// Shift+Enter always inserts newline; Ctrl/Cmd+Enter always sends.
    pub enter_sends: bool,
}

impl Default for UserSettings {
    fn default() -> Self {
        Self { enter_sends: true }
    }
}

/// Metadata about a stored artifact snapshot.
///
/// Included in `ArtifactContent` when the content comes from the DB
/// (DisplayFile storage, history replay, or explicit snapshot load).
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub struct SnapshotMetadata {
    /// The `id` of the `"artifact"` message in the messages table.
    #[ts(type = "number")]
    pub message_id: i64,
    /// 1-indexed version number for this file_path within the conversation.
    pub version: i32,
    /// Total number of versions for this file_path in the conversation.
    pub total_versions: i32,
    /// Sequence number in the conversation message stream.
    #[ts(type = "number")]
    pub seq: i64,
    /// URL to the stable file view route, if the file is within the app's working_dir.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stable_url: Option<String>,
}

/// Summary of a file in the artifact index (for the file picker).
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub struct ArtifactFileInfo {
    pub file_path: String,
    pub versions: Vec<ArtifactVersionInfo>,
}

/// A single version of an artifact file.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub struct ArtifactVersionInfo {
    #[ts(type = "number")]
    pub message_id: i64,
    pub version: i32,
    #[ts(type = "number")]
    pub seq: i64,
}

/// Conversation status as exposed to the frontend.
/// Mirrors ConversationStatus from the DB layer but is a separate type
/// (wire protocol type, not internal domain type).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub enum ConversationListStatus {
    Active,
    Completed,
    Error,
}

#[cfg(test)]
mod tests;
