use brenn_envelope::{SURFACE_SUB_IDENTITY_SEP, surface_sub_identity};

/// Opaque participant identity. One namespace, two roles (publisher attribution
/// and subscriber queue ownership). Format `kind:<id>[@<server>]` is
/// resolver/human convention — the substrate NEVER parses it; it is an opaque
/// key only.
///
/// Constructors:
/// - `for_conversation` — subscriber-side; produces `conversation:<id>`.
/// - `for_app` — publisher-side; produces `app:<slug>@<server>`.
/// - `for_wasm` — WASM processing-component subscriber; produces `wasm:<slug>`.
/// - `for_surface` — browser surface; produces `surface:<slug>`.
/// - `for_surface_component` — one declared component instance; produces
///   `surface:<slug>#<instance>`.
///
/// The substrate keys all of them on `as_str()` and performs no structural
/// parsing.
///
/// # The surface sub-identity
///
/// A surface has two identity grains, and which one applies is decided by *who
/// acted*, not by which channel was touched:
///
/// - `surface:<slug>` — the platform grain. The page's kernel itself: its
///   telemetry (geometry, status, boot/terminal stamps), its heartbeats, and
///   its reports about itself. It holds no durable subscription of its own —
///   every surface subscription is a component instance's.
/// - `surface:<slug>#<instance>` — the component grain. One declared component
///   instance acting on its own behalf: its publishes, its send budget, and its
///   own subscriptions (one per bound channel, each with its own push window and
///   resume cursor).
///
/// The WebSocket is transport, not a principal: one page carries every one of
/// its instances' subscriptions, exactly as one server carries every
/// `[[app]]`'s.
///
/// The principal is the *instance*, the exact analog of a backend `[[app]]`
/// slug: an `[[app]]` block is an instance (its slug names one, its ACLs name
/// exact channels), and all LLM apps are one kind. Twelve `agenda` instances
/// showing twelve people's agendas bind twelve different channels and are
/// twelve principals with twelve budgets. The kind is the manifest — a
/// load-time compatibility check (the instance's grants must satisfy the kind's
/// requirements) — and never holds authority.
///
/// The sub-identity is always *derived* by the server from the `instance` its
/// own boot-resolved declaration set admits — never claimed by the client,
/// which asserts no identity on the wire at all. It exists so budget scoping
/// and attribution land on the component that acted rather than on its
/// neighbours.
///
/// `#` ([`SURFACE_SUB_IDENTITY_SEP`]) is outside the slug and instance charsets
/// (instances wear the kind charset, `^[a-z0-9][a-z0-9-]*$`), so the form is
/// unambiguous and `surface_component` can recover the two halves by splitting.
/// The composition itself lives in [`surface_sub_identity`], shared with the
/// surface's page-local router — the other party that derives this form — so the
/// two spellings cannot drift.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ParticipantId(String);

/// Discriminated subscriber kind, recovered from a `ParticipantId` via
/// [`ParticipantId::kind`]. Used by `WakeRouter` to dispatch each subscriber
/// to the correct delivery arm without inline `starts_with` checks at each
/// site. The match is exhaustive: a future kind is a compile error, not a
/// silent fall-through.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscriberKind {
    /// A Claude Code conversation subscriber (`conversation:<i64>`).
    Conversation(i64),
    /// A WASM processing-component subscriber (`wasm:<slug>`).
    Wasm(String),
    /// A browser surface subscriber. Two grains, exactly as [`ParticipantId`]
    /// has two: `instance: Some(_)` is a component instance
    /// (`surface:<slug>#<instance>`) — the principal that owns a
    /// `[[surface.subscription]]` binding, one subscription per (channel,
    /// instance), the analog of a backend `[[app]]`. `instance: None` is the
    /// surface's own kernel (`surface:<slug>`), the platform grain for
    /// kernel-originated traffic; it holds no durable subscription of its own.
    /// No surface subscription frame or key carries the None grain; the grain
    /// exists here only because the bare `surface:<slug>` identity is a live
    /// *publisher* participant (kernel telemetry and self-reports).
    Surface {
        slug: String,
        instance: Option<String>,
    },
    /// An in-process system-substrate subscriber (`system:<component>`); the
    /// component name identifies the substrate service (e.g. `tool-executor`).
    /// Parked-and-woken exactly like a `Wasm` subscriber — never delivered
    /// inline on the shared dispatch loop.
    System(String),
}

impl ParticipantId {
    /// The LLM (Claude conversation) as participant. Subscriber-side
    /// constructor; also the only `conversation:` producer (no publisher emits
    /// this in this slice).
    pub fn for_conversation(conversation_id: i64) -> Self {
        Self(format!("conversation:{conversation_id}"))
    }

    /// Coarse app attribution. The ONLY publisher-side constructor in this
    /// slice. Host-derived from app slug (+ server origin); never from tool
    /// input. `server` is the existing origin string — NOT a new `Server`
    /// newtype.
    ///
    /// Panics at construction if `app_slug` is empty, contains `@` or `:`, or
    /// `server` is empty (AC6 — no malformed identity reaches a publish).
    pub fn for_app(app_slug: &str, server: &str) -> Self {
        assert!(
            !app_slug.is_empty(),
            "ParticipantId::for_app: app_slug must not be empty"
        );
        assert!(
            !app_slug.contains('@'),
            "ParticipantId::for_app: app_slug must not contain '@': {app_slug:?}"
        );
        assert!(
            !app_slug.contains(':'),
            "ParticipantId::for_app: app_slug must not contain ':': {app_slug:?}"
        );
        assert!(
            !server.is_empty(),
            "ParticipantId::for_app: server must not be empty (app_slug={app_slug:?})"
        );
        Self(format!("app:{app_slug}@{server}"))
    }

    /// Borrow the opaque string for storage / queue keying. The substrate
    /// uses only this — it never inspects structure.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Reconstruct from a stored string (the only path that materializes a
    /// `ParticipantId` from the DB). No validation here — see
    /// `as_conversation_id` for the fail-fast recovery boundary.
    pub fn from_stored(s: String) -> Self {
        Self(s)
    }

    /// WASM processing-component subscriber. Host-assigned, never claimed.
    /// Produces `wasm:<slug>`. The slug is a clean config join key — it must
    /// not be empty, contain `:`, or contain `@` (mirrors `for_app` guards;
    /// no `@server` segment because a processing component is local-only).
    ///
    /// Panics on an empty slug or a slug containing `:` or `@`.
    pub fn for_wasm(slug: &str) -> Self {
        assert!(
            !slug.is_empty(),
            "ParticipantId::for_wasm: slug must not be empty"
        );
        assert!(
            !slug.contains(':'),
            "ParticipantId::for_wasm: slug must not contain ':': {slug:?}"
        );
        assert!(
            !slug.contains('@'),
            "ParticipantId::for_wasm: slug must not contain '@': {slug:?}"
        );
        Self(format!("wasm:{slug}"))
    }

    /// Browser surface participant — the platform grain. Host-assigned, never
    /// claimed. Produces `surface:<slug>`: the identity a surface subscribes
    /// under, and the publisher identity for its kernel's own traffic (telemetry
    /// and its self-reports). A publish *by a component* uses
    /// [`ParticipantId::for_surface_component`] instead.
    ///
    /// The slug is a clean config join key — it must not be empty, contain `:`,
    /// or contain `@` (mirrors `for_wasm` guards), and additionally must not
    /// contain `#`, which separates the sub-identity's instance half.
    ///
    /// Panics on an empty slug or a slug containing `:`, `@`, or `#`.
    pub fn for_surface(slug: &str) -> Self {
        assert!(
            !slug.is_empty(),
            "ParticipantId::for_surface: slug must not be empty"
        );
        assert!(
            !slug.contains(':'),
            "ParticipantId::for_surface: slug must not contain ':': {slug:?}"
        );
        assert!(
            !slug.contains('@'),
            "ParticipantId::for_surface: slug must not contain '@': {slug:?}"
        );
        // `#` separates the sub-identity's instance half; a slug carrying one
        // would make `surface_component` ambiguous.
        assert!(
            !slug.contains('#'),
            "ParticipantId::for_surface: slug must not contain '#' (reserved for \
             per-component sub-identities): {slug:?}"
        );
        Self(format!("surface:{slug}"))
    }

    /// Per-component surface sub-identity: `surface:<slug>#<instance>`.
    /// The principal a declared component instance acts as — both as a publisher
    /// (its sender and send budget) and as a subscriber (its own durable
    /// subscription, push window, and resume cursor). The analog of a backend
    /// `[[app]]` slug: one instance, one principal, one of everything.
    ///
    /// Host-derived, never claimed: the server takes `instance` from the
    /// client's frame and admits it only if its own boot-resolved declaration
    /// set holds it. A client cannot spell an identity here — it supplies an
    /// instance id that is either declared or a protocol violation.
    ///
    /// `slug` wears the `for_surface` guards; `instance` wears the same ones for
    /// the same reason (a `:`/`@`/`#` in either half would make the form
    /// ambiguous to split or collide with another instance's namespace).
    ///
    /// Panics on an empty slug or instance, or on `:`/`@`/`#` in either.
    pub fn for_surface_component(slug: &str, instance: &str) -> Self {
        // Reuse the slug guards verbatim rather than restating them: the slug
        // half of a sub-identity is the same slug, under the same rules.
        let base = Self::for_surface(slug);
        assert!(
            !instance.is_empty(),
            "ParticipantId::for_surface_component: instance must not be empty (slug={slug:?})"
        );
        assert!(
            !instance.contains(':'),
            "ParticipantId::for_surface_component: instance must not contain ':': {instance:?}"
        );
        assert!(
            !instance.contains('@'),
            "ParticipantId::for_surface_component: instance must not contain '@': {instance:?}"
        );
        assert!(
            !instance.contains('#'),
            "ParticipantId::for_surface_component: instance must not contain '#': {instance:?}"
        );
        Self(surface_sub_identity(&base.0, instance))
    }

    /// In-process system-substrate participant. Host-assigned, never claimed.
    /// Produces `system:<component>`; the component name identifies the
    /// substrate service (`tool-executor`). Both a publisher (results flow
    /// through the gated publish core as this principal) and a subscriber (the
    /// service parks and wakes on its channels). The component name is a clean
    /// join key — it must not be empty, contain `:`, or contain `@` (mirrors
    /// `for_wasm` guards; no `@server` segment — a system service is local).
    ///
    /// Panics on an empty component or one containing `:` or `@`.
    pub fn for_system(component: &str) -> Self {
        assert!(
            !component.is_empty(),
            "ParticipantId::for_system: component must not be empty"
        );
        assert!(
            !component.contains(':'),
            "ParticipantId::for_system: component must not contain ':': {component:?}"
        );
        assert!(
            !component.contains('@'),
            "ParticipantId::for_system: component must not contain '@': {component:?}"
        );
        Self(format!("system:{component}"))
    }

    /// Recover the system component this identity denotes. PANICS if the
    /// identity is not a `system:<component>` — an unrecognized kind at a
    /// recovery point is a structural host-wiring bug (BETTER DEAD THAN WRONG; mirrors
    /// `as_wasm_slug`).
    pub fn as_system_component(&self) -> &str {
        self.0.strip_prefix("system:").unwrap_or_else(|| {
            panic!(
                "ParticipantId::as_system_component: not a system identity: {:?}",
                self.0
            )
        })
    }

    /// Recover the surface slug this identity denotes — the slug half for a
    /// sub-identity, so `surface:kitchen` and `surface:kitchen#agenda-alice`
    /// both answer `"kitchen"`. That is the point: a caller asking "which
    /// surface?" wants the surface, and a sub-identity leaking its `#instance`
    /// tail into a slug-keyed lookup would silently miss. Callers who need the
    /// finer grain ask [`ParticipantId::surface_component`].
    ///
    /// PANICS if the identity is not a `surface:` one — an unrecognized kind at
    /// a recovery point is a structural host-wiring bug (BETTER DEAD THAN WRONG;
    /// mirrors `as_wasm_slug`).
    pub fn as_surface_slug(&self) -> &str {
        let rest = self.0.strip_prefix("surface:").unwrap_or_else(|| {
            panic!(
                "ParticipantId::as_surface_slug: not a surface identity: {:?}",
                self.0
            )
        });
        match rest.split_once(SURFACE_SUB_IDENTITY_SEP) {
            Some((slug, _instance)) => slug,
            None => rest,
        }
    }

    /// The subscriber key this surface identity stores in
    /// `messaging_subscriptions.app_slug` / `messaging_pending_pushes
    /// .target_app_slug`: everything after the `surface:` prefix, so
    /// `surface:kitchen` keys `"kitchen"` and `surface:kitchen#agenda-alice`
    /// keys `"kitchen#agenda-alice"`.
    ///
    /// The two grains therefore occupy disjoint keyspaces without a second
    /// column: `#` is outside the operator slug charset, so a component's key
    /// can never collide with its own surface's platform key, with a sibling
    /// instance's, or with an app/wasm slug. The platform grain shares the
    /// bare-slug namespace with apps and wasm consumers but holds no durable
    /// subscription, so no bare-slug surface subscriber row exists to collide.
    ///
    /// PANICS if the identity is not a `surface:` one, matching
    /// `as_surface_slug`.
    pub fn as_surface_subscriber_key(&self) -> &str {
        self.0.strip_prefix("surface:").unwrap_or_else(|| {
            panic!(
                "ParticipantId::as_surface_subscriber_key: not a surface identity: {:?}",
                self.0
            )
        })
    }

    /// Recover the component instance of a `surface:<slug>#<instance>`
    /// sub-identity, or `None` for the bare `surface:<slug>` platform identity.
    /// The executable definition of "is this publish attributed to a component
    /// or to the kernel?", so no caller re-derives it with its own
    /// `contains('#')`.
    ///
    /// PANICS if the identity is not a `surface:` one, matching
    /// `as_surface_slug`: asking a non-surface identity for its component is a
    /// wiring bug, not a `None`.
    pub fn surface_component(&self) -> Option<&str> {
        let rest = self.0.strip_prefix("surface:").unwrap_or_else(|| {
            panic!(
                "ParticipantId::surface_component: not a surface identity: {:?}",
                self.0
            )
        });
        rest.split_once(SURFACE_SUB_IDENTITY_SEP)
            .map(|(_slug, instance)| instance)
    }

    /// Recover the WASM component slug this identity denotes. PANICS if the
    /// identity is not a `wasm:<slug>` — an unrecognized kind at a recovery
    /// point is a structural host-wiring bug (BETTER DEAD THAN WRONG; mirrors
    /// `as_conversation_id`).
    pub fn as_wasm_slug(&self) -> &str {
        self.0.strip_prefix("wasm:").unwrap_or_else(|| {
            panic!(
                "ParticipantId::as_wasm_slug: not a wasm identity: {:?}",
                self.0
            )
        })
    }

    /// Returns `true` if `s` is already a structured `ParticipantId` (i.e. uses the
    /// `app:`, `conversation:`, or `wasm:` scheme). Used by the startup
    /// sender-invariant check to detect any pre-migration rows without
    /// hard-coding the prefix string at the call site.
    ///
    /// This is the single source of truth for "is this a structured identity?" — all
    /// code that needs to distinguish legacy from structured values must use this
    /// predicate rather than a bare `starts_with("app:")` check, so that adding new
    /// kinds only requires updating this function.
    pub fn is_structured(s: &str) -> bool {
        s.starts_with("app:")
            || s.starts_with("conversation:")
            || s.starts_with("wasm:")
            || s.starts_with("surface:")
            || s.starts_with("system:")
    }

    /// Classify this identity into a typed [`SubscriberKind`]. PANICS on an
    /// unrecognized prefix — an unknown kind stored in the DB is a host-wiring
    /// invariant violation (BETTER DEAD THAN WRONG). The exhaustive match ensures that
    /// a future kind that is added to [`SubscriberKind`] without a corresponding
    /// arm here becomes a compile error, not a silent fall-through.
    pub fn kind(&self) -> SubscriberKind {
        if let Some(rest) = self.0.strip_prefix("conversation:") {
            let id = rest.parse::<i64>().unwrap_or_else(|_| {
                panic!(
                    "ParticipantId::kind: malformed conversation identity (non-integer id): {:?}",
                    self.0
                )
            });
            SubscriberKind::Conversation(id)
        } else if let Some(slug) = self.0.strip_prefix("wasm:") {
            SubscriberKind::Wasm(slug.to_owned())
        } else if self.0.starts_with("surface:") {
            // Both grains are subscriber keys. A component instance owns its own
            // subscription, push window, and cursor — twelve instances of one
            // kind are twelve principals, exactly like twelve `[[app]]` blocks.
            // The bare form is the kernel's, and subscribes only what no
            // component owns (the layout channel).
            SubscriberKind::Surface {
                slug: self.as_surface_slug().to_owned(),
                instance: self.surface_component().map(str::to_owned),
            }
        } else if let Some(component) = self.0.strip_prefix("system:") {
            SubscriberKind::System(component.to_owned())
        } else if self.0.starts_with("app:") {
            panic!(
                "ParticipantId::kind: app: identities are publisher-side only and \
                 cannot be used as subscriber keys: {:?}",
                self.0
            )
        } else {
            panic!(
                "ParticipantId::kind: unrecognized identity kind: {:?}",
                self.0
            )
        }
    }

    /// Recover the concrete conversation id this identity denotes. PANICS if
    /// the identity is not a `conversation:<i64>` — an unrecognized kind at a
    /// recovery point is "unexpected" (CLAUDE.md robustness). Since this
    /// method is only valid on `conversation:` identities, any other shape is
    /// a structural bug.
    pub fn as_conversation_id(&self) -> i64 {
        self.0
            .strip_prefix("conversation:")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or_else(|| {
                panic!(
                    "ParticipantId::as_conversation_id: not a conversation identity: {:?}",
                    self.0
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- conversation: kind (carried over from subscriber.rs, renamed) ---

    #[test]
    fn round_trip_for_conversation() {
        for id in [0_i64, 1, 42, i64::MAX, i64::MIN] {
            let pid = ParticipantId::for_conversation(id);
            assert_eq!(pid.as_conversation_id(), id);
        }
    }

    #[test]
    fn as_str_format_conversation() {
        let pid = ParticipantId::for_conversation(42);
        assert_eq!(pid.as_str(), "conversation:42");
    }

    #[test]
    fn from_stored_round_trip() {
        let pid = ParticipantId::for_conversation(99);
        let stored = pid.as_str().to_string();
        let recovered = ParticipantId::from_stored(stored);
        assert_eq!(recovered.as_conversation_id(), 99);
    }

    #[test]
    #[should_panic(expected = "not a conversation identity")]
    fn as_conversation_id_panics_on_app_kind() {
        ParticipantId::from_stored("app:foo@https://example.com".to_string()).as_conversation_id();
    }

    #[test]
    #[should_panic(expected = "not a conversation identity")]
    fn as_conversation_id_panics_on_non_integer() {
        ParticipantId::from_stored("conversation:x".to_string()).as_conversation_id();
    }

    #[test]
    #[should_panic(expected = "not a conversation identity")]
    fn as_conversation_id_panics_on_empty() {
        ParticipantId::from_stored(String::new()).as_conversation_id();
    }

    // --- app: kind (new in this slice) ---

    #[test]
    fn for_app_produces_expected_format() {
        let pid = ParticipantId::for_app("my-app", "https://brenn.example");
        assert_eq!(pid.as_str(), "app:my-app@https://brenn.example");
    }

    #[test]
    fn for_app_various_slugs_and_origins() {
        let pid = ParticipantId::for_app("bob-pa", "https://brenn.example");
        assert_eq!(pid.as_str(), "app:bob-pa@https://brenn.example");

        let pid2 = ParticipantId::for_app("a1", "http://localhost:3000");
        assert_eq!(pid2.as_str(), "app:a1@http://localhost:3000");
    }

    #[test]
    #[should_panic(expected = "app_slug must not be empty")]
    fn for_app_panics_on_empty_slug() {
        ParticipantId::for_app("", "https://example.com");
    }

    #[test]
    #[should_panic(expected = "app_slug must not contain '@'")]
    fn for_app_panics_on_at_in_slug() {
        ParticipantId::for_app("bad@slug", "https://example.com");
    }

    #[test]
    #[should_panic(expected = "app_slug must not contain ':'")]
    fn for_app_panics_on_colon_in_slug() {
        ParticipantId::for_app("bad:slug", "https://example.com");
    }

    #[test]
    #[should_panic(expected = "server must not be empty")]
    fn for_app_panics_on_empty_server() {
        ParticipantId::for_app("my-app", "");
    }

    #[test]
    fn for_app_identity_not_conversation_parseable() {
        // as_conversation_id must panic on an app: identity
        let result = std::panic::catch_unwind(|| {
            ParticipantId::for_app("my-app", "https://example.com").as_conversation_id()
        });
        assert!(result.is_err(), "expected panic on app: identity");
    }

    // --- wasm: kind ---

    #[test]
    fn for_wasm_round_trip() {
        let pid = ParticipantId::for_wasm("isolation-demo");
        assert_eq!(pid.as_str(), "wasm:isolation-demo");
        assert_eq!(pid.as_wasm_slug(), "isolation-demo");
    }

    #[test]
    fn for_wasm_various_slugs() {
        for slug in ["a", "my-component", "foo-bar-baz", "x1"] {
            let pid = ParticipantId::for_wasm(slug);
            assert_eq!(pid.as_str(), format!("wasm:{slug}"));
            assert_eq!(pid.as_wasm_slug(), slug);
        }
    }

    #[test]
    #[should_panic(expected = "slug must not be empty")]
    fn for_wasm_panics_on_empty_slug() {
        ParticipantId::for_wasm("");
    }

    #[test]
    #[should_panic(expected = "slug must not contain ':'")]
    fn for_wasm_panics_on_colon_in_slug() {
        ParticipantId::for_wasm("bad:slug");
    }

    #[test]
    #[should_panic(expected = "slug must not contain '@'")]
    fn for_wasm_panics_on_at_in_slug() {
        ParticipantId::for_wasm("bad@slug");
    }

    #[test]
    #[should_panic(expected = "not a wasm identity")]
    fn as_wasm_slug_panics_on_conversation_kind() {
        ParticipantId::for_conversation(42).as_wasm_slug();
    }

    #[test]
    #[should_panic(expected = "not a wasm identity")]
    fn as_wasm_slug_panics_on_app_kind() {
        ParticipantId::for_app("my-app", "https://example.com").as_wasm_slug();
    }

    #[test]
    #[should_panic(expected = "not a conversation identity")]
    fn as_conversation_id_panics_on_wasm_kind() {
        ParticipantId::for_wasm("my-component").as_conversation_id();
    }

    // --- surface: kind ---

    #[test]
    fn for_surface_round_trip() {
        let pid = ParticipantId::for_surface("deskbar");
        assert_eq!(pid.as_str(), "surface:deskbar");
        assert_eq!(pid.as_surface_slug(), "deskbar");
    }

    #[test]
    fn for_surface_various_slugs() {
        for slug in ["a", "deskbar", "kitchen-panel", "x1"] {
            let pid = ParticipantId::for_surface(slug);
            assert_eq!(pid.as_str(), format!("surface:{slug}"));
            assert_eq!(pid.as_surface_slug(), slug);
        }
    }

    // --- surface sub-identity (`surface:<slug>#<instance>`) ---

    #[test]
    fn for_surface_component_format() {
        let pid = ParticipantId::for_surface_component("kitchen", "agenda-alice");
        assert_eq!(pid.as_str(), "surface:kitchen#agenda-alice");
    }

    /// The two halves survive the round trip, and the slug half is recoverable
    /// without the `#instance` tail — the property every slug-keyed lookup
    /// relies on.
    #[test]
    fn surface_component_round_trips_both_halves() {
        for (slug, instance) in [
            ("kitchen", "agenda-alice"),
            ("a", "x"),
            ("bar-2", "clock-1"),
        ] {
            let pid = ParticipantId::for_surface_component(slug, instance);
            assert_eq!(pid.as_surface_slug(), slug);
            assert_eq!(pid.surface_component(), Some(instance));
        }
    }

    /// Identity is per *instance*: two instances of one kind are two
    /// principals. Nothing in the identity form mentions the kind, so sibling
    /// instances can never collide into a shared bucket.
    #[test]
    fn sibling_instances_of_one_kind_are_distinct_principals() {
        let a = ParticipantId::for_surface_component("kitchen", "agenda-alice");
        let b = ParticipantId::for_surface_component("kitchen", "agenda-bob");
        assert_ne!(a, b);
        assert_ne!(a.as_str(), b.as_str());
        // Both still answer their surface — the slug-keyed lookups are unmoved.
        assert_eq!(a.as_surface_slug(), "kitchen");
        assert_eq!(b.as_surface_slug(), "kitchen");
    }

    /// The platform grain answers `None` — this is how a caller tells a kernel
    /// publish from a component publish.
    #[test]
    fn bare_surface_identity_has_no_component() {
        assert_eq!(
            ParticipantId::for_surface("kitchen").surface_component(),
            None
        );
    }

    /// A sub-identity and its bare surface are distinct principals: the whole
    /// point of the grain (a runaway component must not draw its surface's
    /// budget), so pin that they never compare or hash equal.
    #[test]
    fn sub_identity_is_a_distinct_principal_from_its_surface() {
        let bare = ParticipantId::for_surface("kitchen");
        let sub = ParticipantId::for_surface_component("kitchen", "graf-todos");
        assert_ne!(bare, sub);
        assert_ne!(bare.as_str(), sub.as_str());
    }

    /// Two instances on one surface are distinct principals — blast-radius
    /// scoping is per instance, so a shared bucket would defeat the feature.
    #[test]
    fn sibling_instances_are_distinct_principals() {
        let a = ParticipantId::for_surface_component("kitchen", "graf-todos");
        let b = ParticipantId::for_surface_component("kitchen", "protobar");
        assert_ne!(a, b);
    }

    /// The slug half wears the `for_surface` guards — `for_surface_component`
    /// delegates rather than restating them, so this pins the delegation.
    #[test]
    #[should_panic(expected = "for_surface: slug must not contain ':'")]
    fn for_surface_component_panics_on_colon_in_slug() {
        ParticipantId::for_surface_component("bad:slug", "graf-todos");
    }

    #[test]
    #[should_panic(expected = "instance must not be empty")]
    fn for_surface_component_panics_on_empty_instance() {
        ParticipantId::for_surface_component("kitchen", "");
    }

    /// A `#` in the instance would let one instance spell another instance's
    /// namespace, making the split ambiguous.
    #[test]
    #[should_panic(expected = "instance must not contain '#'")]
    fn for_surface_component_panics_on_hash_in_instance() {
        ParticipantId::for_surface_component("kitchen", "bad#instance");
    }

    #[test]
    #[should_panic(expected = "instance must not contain ':'")]
    fn for_surface_component_panics_on_colon_in_instance() {
        ParticipantId::for_surface_component("kitchen", "bad:instance");
    }

    #[test]
    #[should_panic(expected = "instance must not contain '@'")]
    fn for_surface_component_panics_on_at_in_instance() {
        ParticipantId::for_surface_component("kitchen", "bad@instance");
    }

    /// A sub-identity classifies as its own subscriber: the instance is the
    /// principal, so it subscribes, and `kind()` must carry the instance half
    /// through rather than folding it away.
    #[test]
    fn kind_carries_the_instance_of_a_sub_identity() {
        assert_eq!(
            ParticipantId::for_surface_component("kitchen", "agenda-alice").kind(),
            SubscriberKind::Surface {
                slug: "kitchen".to_string(),
                instance: Some("agenda-alice".to_string()),
            }
        );
    }

    /// Sibling instances classify as *different* subscribers. The behavioural
    /// inverse of the old kind-grain fold: if identity ever collapses back to
    /// the surface, twelve instances share one subscription and this fails.
    #[test]
    fn sibling_instances_are_distinct_subscriber_kinds() {
        assert_ne!(
            ParticipantId::for_surface_component("kitchen", "agenda-alice").kind(),
            ParticipantId::for_surface_component("kitchen", "agenda-bob").kind(),
        );
    }

    /// The bare surface identity remains a subscriber key — it is the kernel's
    /// grain, which subscribes the layout channel.
    #[test]
    fn kind_accepts_bare_surface_identity() {
        assert_eq!(
            ParticipantId::for_surface("kitchen").kind(),
            SubscriberKind::Surface {
                slug: "kitchen".to_string(),
                instance: None,
            }
        );
    }

    /// The subscriber key is the storage encoding for `app_slug` /
    /// `target_app_slug`: the two grains must land in disjoint keyspaces, and
    /// the component grain must never collide with a sibling's.
    #[test]
    fn surface_subscriber_keys_separate_the_grains() {
        assert_eq!(
            ParticipantId::for_surface("kitchen").as_surface_subscriber_key(),
            "kitchen"
        );
        assert_eq!(
            ParticipantId::for_surface_component("kitchen", "agenda-alice")
                .as_surface_subscriber_key(),
            "kitchen#agenda-alice"
        );
        assert_ne!(
            ParticipantId::for_surface_component("kitchen", "agenda-alice")
                .as_surface_subscriber_key(),
            ParticipantId::for_surface_component("kitchen", "agenda-bob")
                .as_surface_subscriber_key(),
        );
    }

    #[test]
    #[should_panic(expected = "not a surface identity")]
    fn surface_component_panics_on_non_surface_identity() {
        ParticipantId::for_wasm("mode-clock").surface_component();
    }

    #[test]
    #[should_panic(expected = "slug must not be empty")]
    fn for_surface_panics_on_empty_slug() {
        ParticipantId::for_surface("");
    }

    #[test]
    #[should_panic(expected = "slug must not contain ':'")]
    fn for_surface_panics_on_colon_in_slug() {
        ParticipantId::for_surface("bad:slug");
    }

    #[test]
    #[should_panic(expected = "slug must not contain '@'")]
    fn for_surface_panics_on_at_in_slug() {
        ParticipantId::for_surface("bad@slug");
    }

    #[test]
    #[should_panic(expected = "slug must not contain '#'")]
    fn for_surface_panics_on_hash_in_slug() {
        ParticipantId::for_surface("deskbar#protobar");
    }

    #[test]
    #[should_panic(expected = "not a surface identity")]
    fn as_surface_slug_panics_on_conversation_kind() {
        ParticipantId::for_conversation(42).as_surface_slug();
    }

    #[test]
    #[should_panic(expected = "not a surface identity")]
    fn as_surface_slug_panics_on_wasm_kind() {
        ParticipantId::for_wasm("my-component").as_surface_slug();
    }

    // --- system: kind ---

    #[test]
    fn for_system_round_trip() {
        let pid = ParticipantId::for_system("tool-executor");
        assert_eq!(pid.as_str(), "system:tool-executor");
        assert_eq!(pid.as_system_component(), "tool-executor");
    }

    #[test]
    fn for_system_various_components() {
        for c in ["a", "tool-executor", "x1"] {
            let pid = ParticipantId::for_system(c);
            assert_eq!(pid.as_str(), format!("system:{c}"));
            assert_eq!(pid.as_system_component(), c);
        }
    }

    #[test]
    #[should_panic(expected = "component must not be empty")]
    fn for_system_panics_on_empty_component() {
        ParticipantId::for_system("");
    }

    #[test]
    #[should_panic(expected = "component must not contain ':'")]
    fn for_system_panics_on_colon() {
        ParticipantId::for_system("bad:component");
    }

    #[test]
    #[should_panic(expected = "component must not contain '@'")]
    fn for_system_panics_on_at() {
        ParticipantId::for_system("bad@component");
    }

    #[test]
    #[should_panic(expected = "not a system identity")]
    fn as_system_component_panics_on_wasm_kind() {
        ParticipantId::for_wasm("my-component").as_system_component();
    }

    #[test]
    #[should_panic(expected = "not a conversation identity")]
    fn as_conversation_id_panics_on_system_kind() {
        ParticipantId::for_system("tool-executor").as_conversation_id();
    }

    #[test]
    fn kind_system() {
        let pid = ParticipantId::for_system("tool-executor");
        assert_eq!(
            pid.kind(),
            SubscriberKind::System("tool-executor".to_owned())
        );
    }

    #[test]
    fn is_structured_recognizes_system() {
        assert!(ParticipantId::is_structured("system:tool-executor"));
    }

    // --- SubscriberKind / kind() ---

    #[test]
    fn kind_conversation() {
        let pid = ParticipantId::for_conversation(42);
        assert_eq!(pid.kind(), SubscriberKind::Conversation(42));
    }

    #[test]
    fn kind_wasm() {
        let pid = ParticipantId::for_wasm("my-component");
        assert_eq!(pid.kind(), SubscriberKind::Wasm("my-component".to_owned()));
    }

    #[test]
    fn kind_surface() {
        let pid = ParticipantId::for_surface("deskbar");
        assert_eq!(
            pid.kind(),
            SubscriberKind::Surface {
                slug: "deskbar".to_owned(),
                instance: None,
            }
        );
    }

    #[test]
    #[should_panic(expected = "app: identities are publisher-side only")]
    fn kind_panics_on_app_kind() {
        // app: is a publisher kind, not a subscriber kind — kind() panics on it.
        ParticipantId::for_app("my-app", "https://example.com").kind();
    }

    #[test]
    #[should_panic(expected = "unrecognized identity kind")]
    fn kind_panics_on_legacy_string() {
        ParticipantId::from_stored("some-legacy-sender".to_string()).kind();
    }

    // --- is_structured predicate ---

    #[test]
    fn is_structured_recognizes_known_kinds() {
        assert!(ParticipantId::is_structured(
            "app:my-app@https://example.com"
        ));
        assert!(ParticipantId::is_structured("conversation:42"));
        assert!(ParticipantId::is_structured("wasm:my-component"));
        assert!(ParticipantId::is_structured("surface:deskbar"));
        assert!(!ParticipantId::is_structured("My App"));
        assert!(!ParticipantId::is_structured(""));
        assert!(!ParticipantId::is_structured("orphaned-custom-sender"));
    }

    // --- unified type: both roles are the same type ---

    #[test]
    fn both_roles_same_type() {
        // Compile-level verification: both constructors produce `ParticipantId`.
        // There is no runtime behavior to assert here beyond "this compiles without a
        // cast" — if either constructor were renamed to a different type the test would
        // fail to compile. The `as_str()` calls exercise the shared interface.
        let publisher: ParticipantId = ParticipantId::for_app("my-app", "https://example.com");
        let subscriber: ParticipantId = ParticipantId::for_conversation(42);
        assert_eq!(publisher.as_str(), "app:my-app@https://example.com");
        assert_eq!(subscriber.as_str(), "conversation:42");
    }
}
