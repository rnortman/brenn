//! Channel address protocol: the canonical `<scheme>:<name>` forms, the
//! deterministic channel UUIDs derived from them, and the auto-channel
//! namespace.
//!
//! Every UUID here is a pinned UUIDv5 derivation: the publish side and the
//! subscription side must land on the same value from the same address, and a
//! changed namespace seed would orphan persisted channel rows.

use uuid::Uuid;

use super::{BRENN_ADDRESS_PREFIX, ChannelScheme};

// ---------------------------------------------------------------------------
// Address protocol
// ---------------------------------------------------------------------------

/// Derive a deterministic UUIDv5 for a `webhook:` channel from the endpoint slug.
///
/// Both the publish side and the subscription side must call this function with
/// the same slug to arrive at the same UUID. The namespace is fixed and
/// documented; changing it would invalidate persisted channel UUIDs.
///
/// Namespace: UUIDv5(DNS-namespace, `"brenn.webhook-channel"`) =
/// `658063f4-9afb-5209-b411-249fb15498fc` (pre-computed once; constant across
/// all deployments so restarts and multi-process setups agree).
pub fn webhook_channel_uuid_from_slug(slug: &str) -> Uuid {
    // Two-level derivation keeps the per-slug UUID space isolated:
    // namespace = UUIDv5(DNS-namespace, "brenn.webhook-channel")
    // channel UUID = UUIDv5(namespace, slug)
    let ns = Uuid::new_v5(&Uuid::NAMESPACE_DNS, b"brenn.webhook-channel");
    Uuid::new_v5(&ns, slug.as_bytes())
}

/// Derive a deterministic UUIDv5 for an `mqtt:` channel from its full resolved
/// address `mqtt:<client>:<topic>`.
///
/// The channel identity *is* the resolved address: both the publish side (the
/// router) and the subscription side (app subscription resolution) must call
/// this function with the same canonical `mqtt:<client>:<topic>` string to
/// arrive at the same UUID. Always derive the address via the shared formatter
/// (`MqttAddress::format`) — never re-concatenate ad hoc — so both sides agree.
/// The namespace is fixed and documented; changing it would invalidate
/// persisted channel UUIDs.
///
/// The namespace seed (`"brenn.mqtt-channel"`) is deliberately distinct from the
/// webhook seed (`"brenn.webhook-channel"`) so the MQTT and webhook address
/// spaces cannot collide: the same string yields a different UUID under each
/// transport.
pub fn mqtt_channel_uuid_from_address(address: &str) -> Uuid {
    // Two-level derivation keeps the per-address UUID space isolated:
    // namespace = UUIDv5(DNS-namespace, "brenn.mqtt-channel")
    // channel UUID = UUIDv5(namespace, address)
    let ns = Uuid::new_v5(&Uuid::NAMESPACE_DNS, b"brenn.mqtt-channel");
    Uuid::new_v5(&ns, address.as_bytes())
}

/// Derive a deterministic UUIDv5 for an `ephemeral:` channel from its bare name.
///
/// Ephemeral channels have no DB row, but a stable UUID keeps their identity
/// uniform with the durable/webhook/MQTT channel spaces. Deterministic across
/// calls, processes, and restarts so every derivation agrees on the same name.
///
/// The namespace seed (`"brenn.ephemeral-channel"`) is deliberately
/// distinct from the webhook and MQTT seeds so the same string yields a
/// different UUID under each transport — the ephemeral, webhook, and MQTT
/// address spaces cannot collide.
pub fn ephemeral_channel_uuid_from_name(name: &str) -> Uuid {
    // Two-level derivation keeps the per-name UUID space isolated:
    // namespace = UUIDv5(DNS-namespace, "brenn.ephemeral-channel")
    // channel UUID = UUIDv5(namespace, name)
    let ns = Uuid::new_v5(&Uuid::NAMESPACE_DNS, b"brenn.ephemeral-channel");
    Uuid::new_v5(&ns, name.as_bytes())
}

/// Derive a deterministic UUIDv5 for a `local:` channel from its bare name.
///
/// Own namespace seed, so `local:foo` and `ephemeral:foo` are distinct
/// identities — they are distinct channels (a `local:` channel never leaves the
/// process) and must never collide in the directory.
pub fn local_channel_uuid_from_name(name: &str) -> Uuid {
    let ns = Uuid::new_v5(&Uuid::NAMESPACE_DNS, b"brenn.local-channel");
    Uuid::new_v5(&ns, name.as_bytes())
}

/// Deterministic UUID for a non-durable channel, dispatching on its scheme.
///
/// # Panics
///
/// On a durable or non-pub/sub scheme — those channels carry an operator or
/// transport-derived UUID instead.
pub fn nondurable_channel_uuid(scheme: ChannelScheme, name: &str) -> Uuid {
    match scheme {
        ChannelScheme::Ephemeral => ephemeral_channel_uuid_from_name(name),
        ChannelScheme::Local => local_channel_uuid_from_name(name),
        other => panic!("nondurable_channel_uuid called with durable scheme {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Auto channels
// ---------------------------------------------------------------------------

/// Channel-name segment owned by the auto-channel machinery: an anonymous auto
/// channel's bare name is `auto.<cid>`. Operators may not declare, reference, or
/// write ACL matchers reaching into it — an anonymous channel is reachable only
/// through the `[[connection]]` / `io_port` declarations that created it.
pub const AUTO_CHANNEL_SEGMENT: &str = "auto";

/// Does `name` (a scheme-stripped channel name) fall in a namespace reserved by
/// the host — the tool substrate's (`tools`, `tool-results`) or the
/// auto-channel machinery's (`auto`)?
///
/// True for an exact segment match or a leading segment followed by a `.`/`/`
/// boundary, so a sibling name like `autobahn` is not falsely reserved. This is
/// the gate for every operator-authored channel name: `[[channel]]` addresses,
/// named auto channels, and surface-declared `local:` names.
///
/// Deliberately separate from [`crate::tools::is_reserved_channel`], which keys
/// the System-principal publish-time well-formedness exemption and must not
/// widen: an `auto.<cid>` name is charset-clean and needs no exemption.
pub fn is_reserved_channel_name(name: &str) -> bool {
    crate::tools::is_reserved_channel(name) || is_auto_channel_name(name)
}

/// Does `name` (a scheme-stripped channel name) fall in the `auto` namespace?
///
/// Same boundary rule as the tool namespaces: an exact segment match, or the
/// segment followed by `.`/`/`. Used to reject every operator-written *reference*
/// to the namespace — a binding address, an ACL matcher — not just declarations.
/// The cid is deterministic, so without that a hand-computed `auto.<cid>` would
/// attach a third party to an "anonymous" channel with no connection to show for
/// it.
pub fn is_auto_channel_name(name: &str) -> bool {
    name == AUTO_CHANNEL_SEGMENT
        || name
            .strip_prefix(AUTO_CHANNEL_SEGMENT)
            .is_some_and(|rest| rest.starts_with('.') || rest.starts_with('/'))
}

/// Derive an auto channel's connection id from its canonical endpoint refs.
///
/// `endpoint_refs` is the connection's endpoint set; the caller need not sort it
/// — this function sorts a copy internally, so the cid depends on the *set* and
/// not on declaration order. It does not deduplicate. Deterministic across
/// boots, which keeps anonymous addresses stable in logs, tests, and directory
/// listings even though the rings behind them die with the process.
///
/// The namespace seed (`"brenn.auto-channel"`) is distinct from every transport
/// seed, so an auto address space cannot collide with any other.
pub fn auto_channel_cid(endpoint_refs: &[String]) -> Uuid {
    let mut sorted: Vec<&str> = endpoint_refs.iter().map(String::as_str).collect();
    sorted.sort_unstable();
    let key = sorted.join("\n");
    let ns = Uuid::new_v5(&Uuid::NAMESPACE_DNS, b"brenn.auto-channel");
    Uuid::new_v5(&ns, key.as_bytes())
}

/// The bare channel name for an anonymous auto channel with connection id `cid`:
/// `auto.<cid>`. Hyphenated-lowercase hex and `.` are both in the unreserved
/// charset, so the name passes ordinary channel-name validation everywhere.
pub fn auto_channel_name(cid: Uuid) -> String {
    format!("{AUTO_CHANNEL_SEGMENT}.{cid}")
}

/// Derive the DB-row identity of a *durable named* auto channel from its bare
/// name.
///
/// Durable auto channels have no operator-written `uuid` by default, so their
/// identity is derived — which means renaming one re-keys its DB row (fresh
/// `resume_epoch`, old row kept orphaned). An operator who needs
/// rename-stability writes the `uuid` field on the `[[connection]]` instead.
///
/// The namespace seed (`"brenn.auto-channel-durable"`) is distinct from
/// [`auto_channel_cid`]'s, so a name and an endpoint-set key can never derive
/// the same identity.
pub fn durable_auto_channel_uuid(bare_name: &str) -> Uuid {
    let ns = Uuid::new_v5(&Uuid::NAMESPACE_DNS, b"brenn.auto-channel-durable");
    Uuid::new_v5(&ns, bare_name.as_bytes())
}

/// Derive a deterministic UUIDv5 for a tool-substrate channel from its full
/// canonical address (`brenn:tools/<tool>` or `brenn:tool-results/<slug>`).
///
/// The tool request channels and result inboxes are created programmatically at
/// bootstrap (not from `[[channel]]` config), so they need a stable identity that
/// is the same across restarts — durable pending-push rows on a request channel
/// must match the same channel UUID after a restart.
///
/// The namespace seed (`"brenn.tool-channel"`) is deliberately distinct from the
/// webhook, MQTT, and ephemeral seeds so the tool address space cannot collide
/// with any other transport's.
pub fn tool_channel_uuid_from_address(address: &str) -> Uuid {
    // Two-level derivation keeps the per-address UUID space isolated:
    // namespace = UUIDv5(DNS-namespace, "brenn.tool-channel")
    // channel UUID = UUIDv5(namespace, address)
    let ns = Uuid::new_v5(&Uuid::NAMESPACE_DNS, b"brenn.tool-channel");
    Uuid::new_v5(&ns, address.as_bytes())
}

/// Derive a deterministic UUIDv5 for a durable chat channel from its full
/// canonical address (`brenn:<prefix>.app.<slug>.<leaf>.<id>`).
///
/// A conversation's command and record channels are minted at conversation
/// creation, not authored in config, so they need an identity that survives
/// restart: the retained record and every subscriber cursor key off the channel
/// UUID, and a fresh UUID each boot would orphan both. Deriving it from the
/// address makes provisioning idempotent by construction — the same
/// conversation always resolves to the same channel.
///
/// The namespace seed (`"brenn.chat-channel"`) is deliberately distinct from
/// the webhook, MQTT, ephemeral, and tool seeds, so the chat address space
/// cannot collide with any other.
pub fn chat_channel_uuid_from_address(address: &str) -> Uuid {
    let ns = Uuid::new_v5(&Uuid::NAMESPACE_DNS, b"brenn.chat-channel");
    Uuid::new_v5(&ns, address.as_bytes())
}

/// Returns `true` if `c` is in the RFC 3986 unreserved character set
/// (`A-Za-z0-9._~-`). Single source of truth for channel-name and
/// push-address charset validation; used by both `messaging` and
/// `pwa_push::targets`.
pub fn is_unreserved_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '~' | '-')
}

/// Build a canonical channel address from a bare name. The name must already
/// be validated; this only adds the `brenn:` prefix.
pub fn canonical_address(name: &str) -> String {
    format!("{}{}", BRENN_ADDRESS_PREFIX, name)
}

/// Qualify an address that names no scheme with `brenn:`, leaving a
/// scheme-qualified one alone.
///
/// A bare address means `brenn:` everywhere the operator can write one, and
/// minted addresses are always qualified, so this is the one spelling every
/// comparison against them has to reach first.
pub fn canonicalize_channel_address(address: &str) -> String {
    match ChannelScheme::of(address) {
        Some(_) => address.to_string(),
        None => canonical_address(address),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_namespace_holds_at_segment_boundaries() {
        assert!(is_auto_channel_name("auto"));
        assert!(is_auto_channel_name("auto.0f1e"));
        assert!(is_auto_channel_name("auto/nested"));
        // Siblings that merely share a prefix are ordinary names.
        assert!(!is_auto_channel_name("autobahn"));
        assert!(!is_auto_channel_name("auto-pilot"));
        assert!(!is_auto_channel_name("my.auto"));
        assert!(!is_auto_channel_name(""));
    }

    #[test]
    fn reserved_channel_names_span_tool_and_auto_namespaces() {
        assert!(is_reserved_channel_name("tools"));
        assert!(is_reserved_channel_name("tool-results/probe"));
        assert!(is_reserved_channel_name("auto.abc"));
        assert!(!is_reserved_channel_name("alerts.high"));
        // The tool-scoped predicate must not widen: it keys the publish-time
        // well-formedness exemption, which auto channels do not need.
        assert!(!crate::tools::is_reserved_channel("auto.abc"));
    }

    #[test]
    fn auto_cid_depends_on_the_endpoint_set_not_its_order() {
        let a = auto_channel_cid(&[
            "wasm:etl/out".to_string(),
            "wasm:indexer/in".to_string(),
            "surface:bench#monitor/tap".to_string(),
        ]);
        let b = auto_channel_cid(&[
            "surface:bench#monitor/tap".to_string(),
            "wasm:indexer/in".to_string(),
            "wasm:etl/out".to_string(),
        ]);
        assert_eq!(a, b);
    }

    #[test]
    fn auto_cid_changes_when_an_endpoint_joins() {
        let two = auto_channel_cid(&["wasm:etl/out".to_string(), "wasm:indexer/in".to_string()]);
        let three = auto_channel_cid(&[
            "wasm:etl/out".to_string(),
            "wasm:indexer/in".to_string(),
            "wasm:audit/in".to_string(),
        ]);
        assert_ne!(two, three);
    }

    #[test]
    fn auto_channel_name_is_charset_clean() {
        let cid = auto_channel_cid(&["wasm:etl/timer".to_string()]);
        let name = auto_channel_name(cid);
        assert!(name.starts_with("auto."));
        assert!(name.chars().all(is_unreserved_char));
        assert!(is_auto_channel_name(&name));
    }

    #[test]
    fn auto_uuid_seeds_are_distinct_from_each_other_and_from_transports() {
        // The same string under the two auto seeds and the transport seeds must
        // never coincide, or an endpoint-set key and a durable name could claim
        // one identity.
        let s = "etl.batches";
        let cid = auto_channel_cid(&[s.to_string()]);
        let durable = durable_auto_channel_uuid(s);
        assert_ne!(cid, durable);
        assert_ne!(durable, ephemeral_channel_uuid_from_name(s));
        assert_ne!(durable, local_channel_uuid_from_name(s));
        assert_ne!(durable, tool_channel_uuid_from_address(s));
        assert_ne!(durable, chat_channel_uuid_from_address(s));
        // Deterministic across calls.
        assert_eq!(durable, durable_auto_channel_uuid(s));
    }

    #[test]
    fn durable_auto_channel_uuid_is_pinned_to_its_value() {
        // This uuid is the primary key of a persisted channel row. Changing the
        // seed string or the input normalization silently re-keys every deployed
        // durable auto channel on the next boot — the old row is orphaned and
        // every schedule parked on it stops firing. No other test in the suite
        // would catch drift, because every other assertion derives its expected
        // uuid from the same function. If this assertion fails the derivation
        // changed: fix the derivation, not this test.
        assert_eq!(
            durable_auto_channel_uuid("etl.batches"),
            Uuid::parse_str("8cff7546-d545-5114-8d13-732c3ffb110e").unwrap(),
        );
    }

    #[test]
    fn is_unreserved_char_accepts_rfc3986_unreserved_set() {
        // All ASCII alphanumerics must be accepted.
        for c in ('A'..='Z').chain('a'..='z').chain('0'..='9') {
            assert!(is_unreserved_char(c), "expected true for {c:?}");
        }
        // The four non-alphanumeric RFC 3986 unreserved chars.
        for c in ['.', '_', '~', '-'] {
            assert!(is_unreserved_char(c), "expected true for {c:?}");
        }
        // Reserved / special chars must be rejected.
        for c in ['@', '!', ' ', '/', '?', '#', '%', '+', ':'] {
            assert!(!is_unreserved_char(c), "expected false for {c:?}");
        }
    }

    /// The reserved `local:brenn/*` control-channel names are unreachable to
    /// operator config *by construction*: every reserved name contains `/`,
    /// which the unreserved charset rejects, so no declared channel name can
    /// ever collide with one. The same reservation-by-construction the `tools/`
    /// namespace relies on.
    ///
    /// This pins the property the reservation rests on rather than the names
    /// themselves: if `is_unreserved_char` ever admits `/`, the reserved
    /// namespace silently stops being reserved and this fails.
    #[test]
    fn reserved_local_control_channel_names_are_unreachable_to_operator_config() {
        for name in [
            "brenn/theme",
            "brenn/takeover",
            "brenn/link-state",
            "brenn/surface-state",
            "brenn/toast",
        ] {
            assert!(
                !name.chars().all(is_unreserved_char),
                "local:{name} is expressible in the operator charset — it is not reserved"
            );
        }
    }

    /// `webhook_channel_uuid_from_slug` is deterministic (same slug → same UUID
    /// across calls, processes, and restarts) and unique per slug.
    #[test]
    fn webhook_channel_uuid_from_slug_is_deterministic() {
        let u1 = webhook_channel_uuid_from_slug("my-endpoint");
        let u2 = webhook_channel_uuid_from_slug("my-endpoint");
        assert_eq!(u1, u2, "same slug must produce same UUID");

        let other = webhook_channel_uuid_from_slug("other-endpoint");
        assert_ne!(u1, other, "different slugs must produce different UUIDs");

        // The UUID must be v5 (version bits 0101).
        assert_eq!(u1.get_version(), Some(uuid::Version::Sha1));
    }

    /// `mqtt_channel_uuid_from_address` is deterministic (same address → same
    /// UUID across calls, processes, and restarts), unique per address, and
    /// lives in a distinct namespace from `webhook_channel_uuid_from_slug` so
    /// the MQTT and webhook address spaces cannot collide. The full
    /// `mqtt:<client>:<topic>` address is hashed, so distinct clients and
    /// distinct topics (including `:`-vs-`/` differences) yield distinct UUIDs.
    #[test]
    fn mqtt_channel_uuid_from_address_is_deterministic_and_distinct() {
        let u1 = mqtt_channel_uuid_from_address("mqtt:c1:home/+/state");
        let u2 = mqtt_channel_uuid_from_address("mqtt:c1:home/+/state");
        assert_eq!(u1, u2, "same address must produce same UUID");

        // Distinct clients, same topic → distinct UUIDs.
        let c2 = mqtt_channel_uuid_from_address("mqtt:c2:home/+/state");
        assert_ne!(u1, c2, "different clients must produce different UUIDs");

        // Distinct topics on the same client → distinct UUIDs.
        let t2 = mqtt_channel_uuid_from_address("mqtt:c1:home/+/other");
        assert_ne!(u1, t2, "different topics must produce different UUIDs");

        // `:`-vs-`/` topic difference must hash distinctly (the full address is
        // hashed verbatim, not decomposed).
        assert_ne!(
            mqtt_channel_uuid_from_address("mqtt:c:a/b"),
            mqtt_channel_uuid_from_address("mqtt:c:a:b"),
            "topics differing only in `:`-vs-`/` must produce different UUIDs"
        );

        // Same string under the two transports must NOT collide (distinct seed).
        let s = "phonebuddy";
        assert_ne!(
            mqtt_channel_uuid_from_address(s),
            webhook_channel_uuid_from_slug(s),
            "mqtt and webhook namespaces must not collide for the same string"
        );

        // The UUID must be v5 (version bits 0101).
        assert_eq!(u1.get_version(), Some(uuid::Version::Sha1));
    }

    /// `tool_channel_uuid_from_address` is deterministic (same address → same
    /// UUID across restarts, so durable request rows match), unique per address,
    /// and lives in a distinct namespace from the other transports so a tool
    /// channel can never collide with a webhook/mqtt/ephemeral channel of the same
    /// name.
    #[test]
    fn tool_channel_uuid_from_address_is_deterministic_and_distinct() {
        let u1 = tool_channel_uuid_from_address("brenn:tools/git-repo-pull");
        assert_eq!(
            u1,
            tool_channel_uuid_from_address("brenn:tools/git-repo-pull")
        );
        assert_ne!(u1, tool_channel_uuid_from_address("brenn:tools/other"));
        // Request channel vs result inbox for the same handle are distinct.
        assert_ne!(
            tool_channel_uuid_from_address("brenn:tools/sync"),
            tool_channel_uuid_from_address("brenn:tool-results/sync"),
        );
        // Distinct namespace seed: the same string does not collide with webhook.
        assert_ne!(
            tool_channel_uuid_from_address("phonebuddy"),
            webhook_channel_uuid_from_slug("phonebuddy"),
        );
        assert_eq!(u1.get_version(), Some(uuid::Version::Sha1));
    }

    #[test]
    fn tool_channel_uuid_from_address_is_pinned_to_its_value() {
        // This uuid is the primary key of a persisted channel row for every tool
        // channel in production. Changing the seed string or the input
        // normalization silently re-keys all of them on the next boot: a fresh
        // row, the old one orphaned with its history, and every parked
        // `deliver_after` schedule on it stops firing. The determinism test
        // above cannot catch that — both sides of its assertions derive from the
        // function under test. If this assertion fails the derivation changed:
        // fix the derivation, not this test.
        assert_eq!(
            tool_channel_uuid_from_address("brenn:tools/git-repo-pull"),
            uuid::Uuid::parse_str("3f460148-040b-598e-8812-3440728ddb2e").unwrap(),
        );
    }

    /// `webhook_channel_uuid_from_slug` produces a fixed, documented value for a
    /// known slug so we can detect any accidental change to the derivation logic.
    ///
    /// **Do NOT change this test if the UUID changes.** If the UUID changes, it
    /// means the derivation logic changed, which would break persisted rows across
    /// restarts. Fix the derivation logic, not this test.
    #[test]
    fn webhook_channel_uuid_from_slug_stable_known_value() {
        // Pre-computed once; must never change. If this assertion fails the
        // derivation logic changed and persisted channel UUIDs across all
        // deployments would be invalidated. Fix the derivation logic, not this test.
        let u = webhook_channel_uuid_from_slug("phonebuddy");
        assert_eq!(
            u.to_string(),
            "3ea885fd-3cc5-5c04-b3c6-36f23b0e978c",
            "webhook_channel_uuid_from_slug(\"phonebuddy\") must be stable"
        );
        // Also verify it is a v5 UUID.
        assert_eq!(u.get_version(), Some(uuid::Version::Sha1));
    }

    /// `ephemeral_channel_uuid_from_name` is deterministic (same name → same
    /// UUID across calls, processes, and restarts), unique per name, and lives in
    /// a distinct namespace from the webhook and MQTT derivations so the same
    /// string cannot collide across transports.
    #[test]
    fn ephemeral_channel_uuid_from_name_is_deterministic_and_distinct() {
        let u1 = ephemeral_channel_uuid_from_name("protobar-demo");
        let u2 = ephemeral_channel_uuid_from_name("protobar-demo");
        assert_eq!(u1, u2, "same name must produce same UUID");

        let other = ephemeral_channel_uuid_from_name("other-channel");
        assert_ne!(u1, other, "different names must produce different UUIDs");

        // Same string under the three transports must NOT collide (distinct seeds).
        let s = "phonebuddy";
        assert_ne!(
            ephemeral_channel_uuid_from_name(s),
            webhook_channel_uuid_from_slug(s),
            "ephemeral and webhook namespaces must not collide for the same string"
        );
        assert_ne!(
            ephemeral_channel_uuid_from_name(s),
            mqtt_channel_uuid_from_address(s),
            "ephemeral and mqtt namespaces must not collide for the same string"
        );

        // The UUID must be v5 (version bits 0101).
        assert_eq!(u1.get_version(), Some(uuid::Version::Sha1));
    }

    /// `ephemeral_channel_uuid_from_name` produces a fixed, documented value for a
    /// known name so we can detect any accidental change to the derivation logic.
    ///
    /// **Do NOT change this test if the UUID changes.** A change means the
    /// derivation logic changed; fix the derivation logic, not this test.
    #[test]
    fn ephemeral_channel_uuid_from_name_stable_known_value() {
        let u = ephemeral_channel_uuid_from_name("phonebuddy");
        assert_eq!(
            u.to_string(),
            "bcb7d898-d580-51b8-9eec-c7d93d26911d",
            "ephemeral_channel_uuid_from_name(\"phonebuddy\") must be stable"
        );
        assert_eq!(u.get_version(), Some(uuid::Version::Sha1));
    }
}
