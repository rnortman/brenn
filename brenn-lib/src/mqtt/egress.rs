//! Shared MQTT-egress enforcement orchestration (design §2.1).
//!
//! `try_handle_mqtt_tool` in the binary crate (the LLM `MqttSend` intercept) was
//! historically the *only* caller of `MqttService::publish`, and it hand-sequenced
//! the capability/ACL/session-lookup/budget gate chain inline. With WASM components
//! gaining MQTT egress (design deliverable 2), a second call site would re-hand-
//! sequence the same chain — a place for two *security* gates to drift. This module
//! owns that orchestration once so "both paths enforce identically" is a structural
//! property, not a code-review discipline.
//!
//! What lives here: the authorization + rate-limit + publish *sequencing* and the
//! per-failure-mode error mapping. What stays at each call site (design §2.2): JSON
//! field extraction, cross-protocol address rejection, body decoding, QoS parsing,
//! `parse_topic_name` (the caller passes an already-parsed, already-wildcard-
//! validated `MqttAddress`), and all response-type mapping. The shared function
//! takes typed, pre-validated inputs and returns a typed [`MqttEgressError`] each
//! caller maps to its own response format.

use crate::access::AppPolicy;
use crate::db::Db;
use crate::messaging::db::{BudgetDecrement, decrement_send_budget};
use crate::mqtt::address::MqttAddress;
use crate::mqtt::error::MqttError;
use crate::mqtt::service::MqttService;
use crate::mqtt::state::PubackOutcome;

/// How the send budget is gated, which differs by caller kind (design §2.1,
/// requirements §"The budget differs by caller kind").
pub enum SendBudget<'a> {
    /// LLM caller: a per-conversation budget row. The function acquires the DB
    /// lock for *just* the synchronous decrement and drops it **before** the
    /// broker-publish await — it never holds the connection guard across `.await`.
    ///
    /// This matters for two reasons: (1) `rusqlite::Connection` is `!Sync`, so a
    /// guard held across `.await` would make the calling future `!Send`, and the
    /// sole LLM call site runs inside a `tokio::spawn`ed task (the CC event loop)
    /// that requires `Send`; (2) holding the single DB mutex across a (possibly
    /// slow, QoS-2) broker round-trip would serialize all DB access for that
    /// duration. The variant therefore takes the `Db` handle (not a borrowed live
    /// connection), and the design's "decrement under the lock, before publish"
    /// invariant is preserved by scoping the lock to the decrement alone.
    Conversation {
        /// The shared DB handle. The function locks it only for the decrement.
        db: &'a Db,
        /// The conversation whose budget row is decremented.
        conversation_id: i64,
        /// Budget the row is initialized to on first decrement.
        default_budget: u32,
    },
    /// WASM caller: no DB budget gate here. The per-activation publish quota is
    /// enforced at the WASM host-fn boundary *before* this function is called
    /// (design §2.5), so this function performs no budget step for it.
    None,
}

/// Distinguishable MQTT-egress failure modes (design §2.1, requirements
/// §"Error types"). Each caller maps these to its own user-facing response; this
/// enum carries no fixed end-user wording (the LLM intercept's strings differ from
/// what a WASM `publish-error` variant carries), so it deliberately does not
/// implement a user-facing `Display`.
#[derive(Debug)]
pub enum MqttEgressError {
    /// Grant absent OR client not in the `mqtt_publish` ACL. A single variant for
    /// enforcement; a caller wanting to distinguish "no grant at all" from "grant
    /// held but client unlisted" (for diagnostics / message selection) does its own
    /// cheap `has_grant(MqttPublish)` check (design §2.1, §2.3).
    AclDenied {
        /// The denied target client slug.
        client: String,
    },
    /// The conversation budget hit zero (only reachable for
    /// [`SendBudget::Conversation`]).
    BudgetExhausted,
    /// Disconnect / submit failure / ack-channel drop from the broker layer.
    Broker(MqttError),
    /// The broker returned a PUBACK/PUBCOMP reason code other than success.
    BrokerRejected {
        /// The broker-supplied rejection reason.
        reason: String,
    },
}

/// Run the shared MQTT-egress enforcement chain and, if every gate passes,
/// publish straight to the broker (design §2.1).
///
/// Order is fixed and load-bearing (the "validate before budget" invariant,
/// requirements §"Gate ordering invariant"):
///
/// 1. **ACL** — `policy.allows_mqtt_publish(client)` composes the layer-1
///    `MqttPublish` grant check and the layer-2 per-client matcher, so no separate
///    grant gate is needed here. A deny returns [`MqttEgressError::AclDenied`]
///    *before* any session lookup or budget decrement.
/// 2. **Session lookup** — `svc.get_client(client)`. A miss after an ACL pass is
///    a broken boot invariant (every matcher client is validated and a session
///    registered for it at boot; the registry is immutable after startup), so it
///    **panics** rather than returning an error, per project posture.
/// 3. **Budget** — for [`SendBudget::Conversation`], decrement under the supplied
///    connection; exhausted → [`MqttEgressError::BudgetExhausted`]. For
///    [`SendBudget::None`] this step is skipped entirely (WASM's quota is enforced
///    upstream, design §2.5).
/// 4. **Publish** — `svc.publish_on_handle(...)`, mapping the broker outcome to
///    `Ok(())` / [`MqttEgressError::BrokerRejected`] / [`MqttEgressError::Broker`].
///
/// Preconditions (design §2.2): `addr` is an already-parsed, already-wildcard-
/// validated [`MqttAddress`] (the caller owns `parse_topic_name`); `payload` and
/// `content_type` are already decoded. ACL-deny logging belongs to the caller,
/// which has the caller-identifying context (design §3.3) — this function does not
/// log.
#[allow(clippy::too_many_arguments)]
pub async fn enforce_and_publish(
    svc: &MqttService,
    policy: &AppPolicy,
    addr: &MqttAddress,
    payload: Vec<u8>,
    content_type: Option<String>,
    qos: u8,
    retain: bool,
    budget: SendBudget<'_>,
) -> Result<(), MqttEgressError> {
    // 1. ACL (layer-1 grant + layer-2 per-client matcher). Deny-by-default: an
    //    empty matcher list denies all clients (`.any` over empty = false).
    if !policy.allows_mqtt_publish(&addr.client) {
        return Err(MqttEgressError::AclDenied {
            client: addr.client.clone(),
        });
    }

    // 2. Session lookup — replaces connector resolution. The client slug selects
    //    the session; a miss after an ACL pass is a broken boot invariant (every
    //    matcher client is validated and registered at boot, registry immutable
    //    after startup), so panic per project posture.
    let handle = svc.get_client(&addr.client).unwrap_or_else(|| {
        panic!(
            "mqtt egress: ACL authorized a publish to client {:?} but no session is registered \
             — every matcher client is validated and a session registered at boot; the client \
             registry is immutable after startup (broken invariant)",
            addr.client
        )
    });

    // 3. Budget — last gate before the publish. Skipped entirely for the WASM
    //    caller (its per-activation quota is enforced upstream, design §2.5). The
    //    DB lock is held only for the synchronous decrement and dropped before the
    //    publish await below, so the calling future stays `Send` and the single DB
    //    mutex is never held across the broker round-trip (see `SendBudget` doc).
    if let SendBudget::Conversation {
        db,
        conversation_id,
        default_budget,
    } = budget
    {
        let exhausted = {
            let conn = db.lock().await;
            matches!(
                decrement_send_budget(&conn, conversation_id, default_budget),
                BudgetDecrement::Exhausted
            )
        };
        if exhausted {
            return Err(MqttEgressError::BudgetExhausted);
        }
    }

    // 4. Publish straight to the broker (synchronous; QoS ≥ 1 awaits the ack). No
    //    store-and-forward — the broker outcome propagates inline (requirements
    //    §"Original request").
    match svc
        .publish_on_handle(
            &handle,
            addr.topic.clone(),
            payload,
            content_type,
            qos,
            retain,
        )
        .await
    {
        Ok(PubackOutcome::Success) => Ok(()),
        Ok(PubackOutcome::BrokerRejected { reason }) => {
            Err(MqttEgressError::BrokerRejected { reason })
        }
        Err(e) => Err(MqttEgressError::Broker(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::acl::MqttClientMatcher;
    use crate::access::{AppCapability, AppPolicy};
    use crate::db::init_db_memory;
    use crate::mqtt::state::MqttClientHandle;
    use crate::test_utils::ensure_user_and_conv;

    /// A policy granting `MqttPublish` with a `mqtt_publish` matcher for each of
    /// `clients`.
    fn policy_allowing(clients: &[&str]) -> AppPolicy {
        let mut p = AppPolicy::with_grants(&[AppCapability::MqttPublish]);
        for c in clients {
            p.acls.mqtt_publish.push(MqttClientMatcher {
                client: (*c).to_string(),
            });
        }
        p
    }

    /// An `MqttService` holding a single (disconnected) session for `client`. The
    /// session has no live `AsyncClient`, so any publish that reaches the broker
    /// layer fails with `NotConnected` — exactly what the "reached the broker"
    /// tests assert against.
    async fn service_with_client(client: &str) -> std::sync::Arc<MqttService> {
        let svc = MqttService::new();
        let (tx, _rx) = tokio::sync::watch::channel(false);
        let config = std::sync::Arc::new(crate::mqtt::test_support::test_client_config(client));
        let handle = MqttClientHandle::new(config, vec![], tx);
        svc.add_client(handle).await;
        svc
    }

    fn addr(client: &str, topic: &str) -> MqttAddress {
        MqttAddress {
            client: client.to_string(),
            topic: topic.to_string(),
        }
    }

    /// ACL deny short-circuits: returns `AclDenied`, never looks up a session, and
    /// (with a conversation budget) never decrements the budget row. The row
    /// staying absent proves no budget unit was consumed.
    #[tokio::test]
    async fn acl_deny_returns_acl_denied_and_skips_resolution_and_budget() {
        // Policy allows `home`; service has a session for `home`. The publish
        // targets `office`, denied by the ACL.
        let svc = service_with_client("home").await;
        let policy = policy_allowing(&["home"]);
        let db = init_db_memory();

        let result = enforce_and_publish(
            &svc,
            &policy,
            &addr("office", "cmd/light"),
            b"on".to_vec(),
            None,
            1,
            false,
            SendBudget::Conversation {
                db: &db,
                conversation_id: 1,
                default_budget: 5,
            },
        )
        .await;

        assert!(
            matches!(result, Err(MqttEgressError::AclDenied { ref client }) if client == "office"),
            "expected AclDenied for office, got {result:?}"
        );
        // No budget row was created — the decrement was never reached.
        assert_eq!(
            crate::messaging::db::read_send_budget(&db.lock().await as &rusqlite::Connection, 1),
            None,
            "ACL deny must not touch the budget row"
        );
    }

    /// Deny-by-default: grant held but an empty matcher list denies all clients.
    #[tokio::test]
    async fn deny_by_default_empty_matcher_list_denies() {
        let svc = service_with_client("home").await;
        let policy = policy_allowing(&[]); // grant present, no matchers
        let db = init_db_memory();

        let result = enforce_and_publish(
            &svc,
            &policy,
            &addr("home", "cmd/light"),
            b"on".to_vec(),
            None,
            1,
            false,
            SendBudget::Conversation {
                db: &db,
                conversation_id: 1,
                default_budget: 5,
            },
        )
        .await;

        assert!(
            matches!(result, Err(MqttEgressError::AclDenied { .. })),
            "grant + empty matcher list must deny, got {result:?}"
        );
    }

    /// Grant + ACL pass but no registered session for the client is a broken boot
    /// invariant (boot validates + registers a session for every matcher client;
    /// the registry is immutable after startup), so it panics rather than errors.
    #[tokio::test]
    #[should_panic(expected = "no session is registered")]
    async fn session_miss_after_acl_pass_panics() {
        // ACL allows `home`, but the service has a session only for `other`.
        let svc = service_with_client("other").await;
        let policy = policy_allowing(&["home"]);
        let db = init_db_memory();

        let _ = enforce_and_publish(
            &svc,
            &policy,
            &addr("home", "cmd/light"),
            b"on".to_vec(),
            None,
            1,
            false,
            SendBudget::Conversation {
                db: &db,
                conversation_id: 1,
                default_budget: 5,
            },
        )
        .await;
    }

    /// Budget exhausted ⇒ `BudgetExhausted`, and the broker is never reached. (If
    /// the broker had been reached on a disconnected session the error would be
    /// `Broker`, not `BudgetExhausted`.)
    #[tokio::test]
    async fn budget_exhausted_returns_budget_exhausted_without_publish() {
        let svc = service_with_client("home").await;
        let policy = policy_allowing(&["home"]);
        let db = init_db_memory();
        {
            let conn = db.lock().await;
            ensure_user_and_conv(&conn, 1);
            // Pre-exhaust the budget: default 1, decrement once to reach 0.
            let pre = decrement_send_budget(&conn, 1, 1);
            assert!(
                matches!(pre, BudgetDecrement::Ok { remaining: 0 }),
                "pre-exhaust setup failed: {pre:?}"
            );
        }

        let result = enforce_and_publish(
            &svc,
            &policy,
            &addr("home", "cmd/light"),
            b"on".to_vec(),
            None,
            1,
            false,
            SendBudget::Conversation {
                db: &db,
                conversation_id: 1,
                default_budget: 1,
            },
        )
        .await;

        assert!(
            matches!(result, Err(MqttEgressError::BudgetExhausted)),
            "expected BudgetExhausted, got {result:?}"
        );
    }

    /// Fully-authorized conversation call with a registered (disconnected) session
    /// reaches the broker layer and surfaces `Broker(...)` — proving ACL, session
    /// lookup, and the budget all passed before the publish attempt.
    #[tokio::test]
    async fn authorized_call_reaches_broker_layer() {
        let svc = service_with_client("home").await;
        let policy = policy_allowing(&["home"]);
        let db = init_db_memory();
        {
            let conn = db.lock().await;
            ensure_user_and_conv(&conn, 1);
        }

        let result = enforce_and_publish(
            &svc,
            &policy,
            &addr("home", "cmd/light"),
            b"on".to_vec(),
            None,
            1,
            false,
            SendBudget::Conversation {
                db: &db,
                conversation_id: 1,
                default_budget: 5,
            },
        )
        .await;

        assert!(
            matches!(result, Err(MqttEgressError::Broker(_))),
            "an authorized publish to a disconnected session must reach the broker \
             layer (Broker), got {result:?}"
        );
        // The budget was decremented exactly once (5 → 4): the publish step ran.
        assert_eq!(
            crate::messaging::db::read_send_budget(&db.lock().await as &rusqlite::Connection, 1),
            Some(4),
            "an authorized call must decrement the conversation budget once"
        );
    }

    /// `SendBudget::None` (the WASM path) skips the budget entirely but still
    /// enforces ACL + session lookup: a fully-authorized call reaches the broker
    /// layer with no DB connection involved.
    #[tokio::test]
    async fn none_budget_skips_budget_but_enforces_acl_and_session() {
        let svc = service_with_client("home").await;
        let policy = policy_allowing(&["home"]);

        // A DB the function must NOT touch under `SendBudget::None`: the variant
        // carries no `db` handle, so accidental budget access is structurally
        // impossible — this asserts the behavioral counterpart (no row created)
        // to `authorized_call_reaches_broker_layer`'s "row decremented under
        // Conversation".
        let db = init_db_memory();
        {
            let conn = db.lock().await;
            ensure_user_and_conv(&conn, 1);
        }

        // Authorized → reaches the broker layer.
        let ok = enforce_and_publish(
            &svc,
            &policy,
            &addr("home", "cmd/light"),
            b"on".to_vec(),
            None,
            1,
            false,
            SendBudget::None,
        )
        .await;
        assert!(
            matches!(ok, Err(MqttEgressError::Broker(_))),
            "None-budget authorized call must reach the broker layer, got {ok:?}"
        );
        // No budget row was created/decremented for the `None` path.
        assert_eq!(
            crate::messaging::db::read_send_budget(&db.lock().await as &rusqlite::Connection, 1),
            None,
            "SendBudget::None must never touch any conversation budget row"
        );

        // ACL still enforced for an unlisted client.
        let denied = enforce_and_publish(
            &svc,
            &policy,
            &addr("office", "cmd/light"),
            b"on".to_vec(),
            None,
            1,
            false,
            SendBudget::None,
        )
        .await;
        assert!(
            matches!(denied, Err(MqttEgressError::AclDenied { .. })),
            "None-budget must still enforce the ACL, got {denied:?}"
        );
    }
}
