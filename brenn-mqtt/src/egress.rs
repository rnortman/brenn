//! Shared MQTT-egress enforcement orchestration.
//!
//! Both the LLM `MqttSend` intercept and the WASM host-fn publish path must
//! enforce the same capability/ACL/budget gate chain. This module owns that
//! sequencing once so "both paths enforce identically" is a structural property,
//! not a code-review discipline.
//!
//! The chain's order is fixed and load-bearing — "validate before budget": ACL,
//! session lookup, budget, publish. It is exposed in two halves so a caller can
//! check what only the session's configuration knows between them:
//! [`authorize`] runs the ACL and the session lookup and returns an
//! [`Authorized`], whose [`Authorized::publish`] runs the budget and the
//! publish. [`Authorized`] has no other constructor, so a publish without the
//! first two steps cannot be expressed, and the budget step is inside the
//! publish half. [`enforce_and_publish`] runs both halves for a caller with
//! nothing to check between them.
//!
//! What lives here: the authorization + rate-limit + publish *sequencing* and the
//! per-failure-mode error mapping. What stays at each call site: JSON
//! field extraction, cross-protocol address rejection, body decoding, QoS parsing,
//! `parse_topic_name` (the caller passes an already-parsed, already-wildcard-
//! validated `MqttAddress`), and all response-type mapping. The shared function
//! takes typed, pre-validated inputs and returns a typed [`MqttEgressError`] each
//! caller maps to its own response format.

use std::sync::Arc;

use crate::service::{MqttService, PublishProps};
use crate::state::{MqttClientHandle, PubackOutcome};
use brenn_db::Db;
use brenn_lib::access::AppPolicy;
use brenn_lib::mqtt::address::MqttAddress;
use brenn_lib::mqtt::config::MqttClientIdentity;
use brenn_lib::mqtt::error::MqttError;
use brenn_messaging_store::db::{BudgetDecrement, decrement_send_budget};

/// How the send budget is gated, which differs by caller kind.
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
    /// connection), and the "decrement under the lock, before publish" invariant
    /// is preserved by scoping the lock to the decrement alone.
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
    /// so this function performs no budget step for it.
    None,
}

/// Distinguishable MQTT-egress failure modes. Each caller maps these to its own
/// user-facing response; this enum carries no fixed end-user wording (the LLM
/// intercept's strings differ from
/// what a WASM `publish-error` variant carries), so it deliberately does not
/// implement a user-facing `Display`.
#[derive(Debug)]
pub enum MqttEgressError {
    /// Grant absent OR no `mqtt_publish` matcher admits the topic on the client.
    /// A single variant for enforcement; a caller wanting to distinguish "no grant
    /// at all" from "grant held but topic unlisted" (for diagnostics / message
    /// selection) does its own cheap `has_grant(MqttPublish)` check.
    AclDenied {
        /// The denied target client slug.
        client: String,
        /// The denied concrete topic (guest- or model-supplied; sanitize before
        /// logging or echoing).
        topic: String,
    },
    /// The conversation budget hit zero (only reachable for
    /// [`SendBudget::Conversation`]).
    BudgetExhausted,
    /// Disconnect / submit failure / ack-channel drop from the broker layer,
    /// or `MqttError::TimedOut` when the session did not complete the publish
    /// within `PUBLISH_WAIT`; a timed-out message may still be delivered.
    Broker(MqttError),
    /// The broker returned a PUBACK/PUBCOMP reason code other than success.
    BrokerRejected {
        /// The broker-supplied rejection reason.
        reason: String,
    },
    /// The ACL passed but the registry holds no session for the client.
    ///
    /// Reachable for a caller whose authority snapshot predates a reload that
    /// stopped the client: the snapshot still lists the client and the registry
    /// no longer holds it. A runtime outcome for such a caller, and a broken
    /// invariant for one whose policy and registry reads are under one table
    /// snapshot — the caller decides which it is.
    ClientNotRegistered {
        /// The slug with no registered session.
        client: String,
    },
    /// The encoded publish exceeds the Maximum Packet Size the broker
    /// advertised for the session's current connection. Nothing was sent; the
    /// budget step, if any, has run.
    PacketTooLarge {
        /// The size of the PUBLISH packet as it would be encoded.
        packet_bytes: usize,
        /// The broker's advertised maximum.
        broker_max: u32,
    },
}

/// Steps 1-2 of the chain passed for one address: the ACL allowed the client
/// and the registry holds its session. Made only by [`authorize`], so a
/// publish that skipped either step cannot be expressed.
pub struct Authorized {
    handle: Arc<MqttClientHandle>,
    topic: String,
}

impl Authorized {
    /// The configuration of the session this publish will use.
    pub fn client(&self) -> &MqttClientIdentity {
        &self.handle.config.identity
    }

    /// Steps 3-4: budget, then publish.
    ///
    /// 3. **Budget** — for [`SendBudget::Conversation`], decrement under the
    ///    database lock; exhausted → [`MqttEgressError::BudgetExhausted`]. For
    ///    [`SendBudget::None`] this step is skipped entirely (WASM's quota is
    ///    enforced upstream).
    /// 4. **Publish** — `svc.publish_on_handle(...)`, mapping the broker
    ///    outcome to `Ok(())` / [`MqttEgressError::BrokerRejected`] /
    ///    [`MqttEgressError::PacketTooLarge`] / [`MqttEgressError::Broker`].
    ///
    /// `payload` and `props.content_type` are already decoded; `props` passes
    /// through to the broker unchanged.
    pub async fn publish(
        self,
        svc: &MqttService,
        payload: Vec<u8>,
        props: PublishProps,
        budget: SendBudget<'_>,
    ) -> Result<(), MqttEgressError> {
        // 3. Budget — last gate before the publish. The DB lock is held only for
        //    the synchronous decrement and dropped before the publish await
        //    below, so the calling future stays `Send` and the single DB mutex
        //    is never held across the broker round-trip (see `SendBudget` doc).
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

        // 4. Publish straight to the broker (synchronous; QoS ≥ 1 awaits the
        //    ack). No store-and-forward — the broker outcome propagates inline.
        match svc
            .publish_on_handle(&self.handle, self.topic, payload, props)
            .await
        {
            Ok(PubackOutcome::Success) => Ok(()),
            Ok(PubackOutcome::BrokerRejected { reason }) => {
                Err(MqttEgressError::BrokerRejected { reason })
            }
            Err(MqttError::PacketTooLarge {
                packet_bytes,
                broker_max,
                ..
            }) => Err(MqttEgressError::PacketTooLarge {
                packet_bytes,
                broker_max,
            }),
            Err(e) => Err(MqttEgressError::Broker(e)),
        }
    }
}

/// Steps 1-2 of the chain: ACL, then session lookup.
///
/// 1. **ACL** — `policy.allows_mqtt_publish(client, topic)` composes the
///    layer-1 `MqttPublish` grant check and the layer-2 `(client, topic_filter)`
///    matchers, so no separate grant gate is needed here. A deny returns
///    [`MqttEgressError::AclDenied`] *before* any session lookup.
/// 2. **Session lookup** — `svc.get_client(client)`. A miss after an ACL pass
///    returns [`MqttEgressError::ClientNotRegistered`]: the registry shrinks at
///    a reload that stops a client, and a caller holding an older authority
///    snapshot can still name it. Whether that is a runtime outcome or a broken
///    invariant depends on how the caller obtained its policy, so the decision
///    is the caller's.
///
/// `addr` is an already-parsed, already-wildcard-validated [`MqttAddress`]
/// (the caller owns `parse_topic_name`). ACL-deny logging belongs to the
/// caller, which has the caller-identifying context — this function does not
/// log.
pub fn authorize(
    svc: &MqttService,
    policy: &AppPolicy,
    addr: &MqttAddress,
) -> Result<Authorized, MqttEgressError> {
    // Deny-by-default: an empty matcher list denies every topic (`.any` over
    // empty = false).
    if !policy.allows_mqtt_publish(&addr.client, &addr.topic) {
        return Err(MqttEgressError::AclDenied {
            client: addr.client.clone(),
            topic: addr.topic.clone(),
        });
    }

    // A reload's commit stops a client's session after it swaps the authority
    // that named it, so a caller whose ACL snapshot was taken before that swap
    // can pass the gate above and find no session here.
    let Some(handle) = svc.get_client(&addr.client) else {
        return Err(MqttEgressError::ClientNotRegistered {
            client: addr.client.clone(),
        });
    };

    Ok(Authorized {
        handle,
        topic: addr.topic.clone(),
    })
}

/// Run the whole chain — [`authorize`], then [`Authorized::publish`] — for a
/// caller with nothing to check between the two halves.
pub async fn enforce_and_publish(
    svc: &MqttService,
    policy: &AppPolicy,
    addr: &MqttAddress,
    payload: Vec<u8>,
    props: PublishProps,
    budget: SendBudget<'_>,
) -> Result<(), MqttEgressError> {
    authorize(svc, policy, addr)?
        .publish(svc, payload, props, budget)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::MqttClientHandle;
    use brenn_envelope::grants::AppCapability;
    use brenn_lib::access::AppPolicy;
    use brenn_lib::access::acl::MqttPubMatcher;
    use brenn_messaging_store::db::init_db_memory;
    use brenn_messaging_store::testutils::ensure_user_and_conv;

    /// A policy granting `MqttPublish` with a `mqtt_publish` matcher of filter
    /// `cmd/#` for each of `clients`.
    fn policy_allowing(clients: &[&str]) -> AppPolicy {
        let mut p = AppPolicy::with_grants(&[AppCapability::MqttPublish]);
        for c in clients {
            p.acls.mqtt_publish.push(MqttPubMatcher {
                client: (*c).to_string(),
                topic_filter: "cmd/#".to_string(),
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
        let config = std::sync::Arc::new(brenn_lib::mqtt::test_support::test_client_config(client));
        let handle = MqttClientHandle::new(config, vec![], tx);
        svc.add_client(handle);
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
            PublishProps {
                qos: 1,
                ..Default::default()
            },
            SendBudget::Conversation {
                db: &db,
                conversation_id: 1,
                default_budget: 5,
            },
        )
        .await;

        assert!(
            matches!(
                result,
                Err(MqttEgressError::AclDenied { ref client, ref topic })
                    if client == "office" && topic == "cmd/light"
            ),
            "expected AclDenied for office, got {result:?}"
        );
        // No budget row was created — the decrement was never reached.
        assert_eq!(
            brenn_messaging_store::db::read_send_budget(
                &db.lock().await as &rusqlite::Connection,
                1
            ),
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
            PublishProps {
                qos: 1,
                ..Default::default()
            },
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

    /// Grant + ACL pass but no registered session for the client is reported,
    /// not panicked: the registry shrinks at a reload that stops a client, and
    /// an authority snapshot taken before that commit still names it. The
    /// caller decides whether that is a runtime outcome — it is, for an
    /// in-flight tool call — or an invariant break.
    #[tokio::test]
    async fn session_miss_after_acl_pass_is_reported() {
        // ACL allows `home`, but the service has a session only for `other`.
        let svc = service_with_client("other").await;
        let policy = policy_allowing(&["home"]);
        let db = init_db_memory();

        let result = enforce_and_publish(
            &svc,
            &policy,
            &addr("home", "cmd/light"),
            b"on".to_vec(),
            PublishProps {
                qos: 1,
                ..Default::default()
            },
            SendBudget::Conversation {
                db: &db,
                conversation_id: 1,
                default_budget: 5,
            },
        )
        .await;

        assert!(
            matches!(
                result,
                Err(MqttEgressError::ClientNotRegistered { ref client }) if client == "home"
            ),
            "got {result:?}",
        );
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
            PublishProps {
                qos: 1,
                ..Default::default()
            },
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
            PublishProps {
                qos: 1,
                ..Default::default()
            },
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
            brenn_messaging_store::db::read_send_budget(
                &db.lock().await as &rusqlite::Connection,
                1
            ),
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
            PublishProps {
                qos: 1,
                ..Default::default()
            },
            SendBudget::None,
        )
        .await;
        assert!(
            matches!(ok, Err(MqttEgressError::Broker(_))),
            "None-budget authorized call must reach the broker layer, got {ok:?}"
        );
        // No budget row was created/decremented for the `None` path.
        assert_eq!(
            brenn_messaging_store::db::read_send_budget(
                &db.lock().await as &rusqlite::Connection,
                1
            ),
            None,
            "SendBudget::None must never touch any conversation budget row"
        );

        // ACL still enforced for an unlisted client.
        let denied = enforce_and_publish(
            &svc,
            &policy,
            &addr("office", "cmd/light"),
            b"on".to_vec(),
            PublishProps {
                qos: 1,
                ..Default::default()
            },
            SendBudget::None,
        )
        .await;
        assert!(
            matches!(denied, Err(MqttEgressError::AclDenied { .. })),
            "None-budget must still enforce the ACL, got {denied:?}"
        );
    }

    /// `authorize` runs steps 1-2 alone: it denies an unlisted client, misses
    /// an unregistered one, and for an allowed, registered client hands back
    /// the session's configuration without touching any budget.
    #[tokio::test]
    async fn authorize_runs_acl_then_session_lookup() {
        let svc = service_with_client("home").await;
        let policy = policy_allowing(&["home", "gone"]);

        assert!(matches!(
            authorize(&svc, &policy, &addr("office", "cmd/light")),
            Err(MqttEgressError::AclDenied { ref client, .. }) if client == "office"
        ));
        // A listed client, but a topic outside its filter, is denied before the
        // session lookup and names the topic.
        assert!(matches!(
            authorize(&svc, &policy, &addr("gone", "status/light")),
            Err(MqttEgressError::AclDenied { ref client, ref topic })
                if client == "gone" && topic == "status/light"
        ));
        assert!(matches!(
            authorize(&svc, &policy, &addr("gone", "cmd/light")),
            Err(MqttEgressError::ClientNotRegistered { ref client }) if client == "gone"
        ));
        let authorized = match authorize(&svc, &policy, &addr("home", "cmd/light")) {
            Ok(a) => a,
            Err(e) => panic!("expected Authorized, got {e:?}"),
        };
        assert_eq!(authorized.client().slug, "home");
    }

    /// `Authorized::publish` runs steps 3-4: the budget is spent, then the
    /// broker layer is reached.
    #[tokio::test]
    async fn authorized_publish_spends_the_budget_and_reaches_the_broker() {
        let svc = service_with_client("home").await;
        let policy = policy_allowing(&["home"]);
        let db = init_db_memory();
        {
            let conn = db.lock().await;
            ensure_user_and_conv(&conn, 1);
        }

        let authorized = match authorize(&svc, &policy, &addr("home", "cmd/light")) {
            Ok(a) => a,
            Err(e) => panic!("expected Authorized, got {e:?}"),
        };
        let result = authorized
            .publish(
                &svc,
                b"on".to_vec(),
                PublishProps {
                    qos: 1,
                    ..Default::default()
                },
                SendBudget::Conversation {
                    db: &db,
                    conversation_id: 1,
                    default_budget: 5,
                },
            )
            .await;

        assert!(
            matches!(result, Err(MqttEgressError::Broker(_))),
            "got {result:?}"
        );
        assert_eq!(
            brenn_messaging_store::db::read_send_budget(
                &db.lock().await as &rusqlite::Connection,
                1
            ),
            Some(4),
        );
    }

    /// A publish over the broker's advertised maximum is its own variant
    /// carrying both numbers, not a `Broker` error, and it comes after the
    /// budget step.
    #[tokio::test]
    async fn packet_over_the_broker_maximum_maps_to_packet_too_large() {
        let svc = service_with_client("home").await;
        let handle = svc.get_client("home").expect("registered above");
        let _eventloop = crate::test_support::install_unpolled_client(&handle, Some(16)).await;
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
            vec![0u8; 64],
            PublishProps {
                qos: 1,
                ..Default::default()
            },
            SendBudget::Conversation {
                db: &db,
                conversation_id: 1,
                default_budget: 5,
            },
        )
        .await;

        assert!(
            matches!(
                result,
                Err(MqttEgressError::PacketTooLarge {
                    packet_bytes: 80,
                    broker_max: 16,
                })
            ),
            "got {result:?}"
        );
        assert_eq!(
            brenn_messaging_store::db::read_send_budget(
                &db.lock().await as &rusqlite::Connection,
                1
            ),
            Some(4),
            "the broker's limit is checked in the publish step, after the budget",
        );
    }
}
