//! The sync-call cause: the shape it assembles, when it answers, and who it
//! answers when nobody served it.
//!
//! What the suite is watching for is that a request is *an ordinary activation
//! plus a return obligation* and not a second kind of dispatch. So every
//! assertion here is about the ordinary half — every bound input windowed, the
//! positions advanced, the buffer flushed — with the answer read afterwards.

use super::*;

/// The bodies published to the output channel so far, oldest first.
async fn out_bodies(messenger: &brenn_messaging::Messenger, out_address: &str) -> Vec<String> {
    let conn = messenger.db().lock().await;
    let mut stmt = conn
        .prepare(
            "SELECT m.body \
             FROM messaging_messages m \
             JOIN messaging_channels c ON c.uuid = m.channel_uuid \
             WHERE c.address = ?1 \
             ORDER BY m.publish_ts_ns ASC",
        )
        .expect("the output query prepares");
    let rows = stmt
        .query_map(rusqlite::params![out_address], |row| {
            row.get::<_, String>(0)
        })
        .expect("the output query runs");
    rows.map(|r| r.expect("a published body reads back"))
        .collect()
}

/// The quarantine rows this subscriber has accumulated, as
/// `(outcome, last_message_id)` pairs.
async fn failure_rows(
    messenger: &brenn_messaging::Messenger,
    subscriber: &ParticipantId,
) -> Vec<(String, String)> {
    let conn = messenger.db().lock().await;
    let mut stmt = conn
        .prepare(
            "SELECT outcome, last_message_id FROM messaging_wasm_consume_failures \
             WHERE subscriber = ?1 ORDER BY last_message_id ASC",
        )
        .expect("the failure query prepares");
    let rows = stmt
        .query_map(rusqlite::params![subscriber.as_str()], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .expect("the failure query runs");
    rows.map(|r| r.expect("a failure row reads back")).collect()
}

/// Send one request into a config's own loop-less drain and read the answer.
async fn call(
    cfg: &WasmConsumerConfig,
    subscriber: &ParticipantId,
    port: &str,
    body: &str,
) -> SyncAnswer {
    let (reply, answer) = tokio::sync::oneshot::channel();
    drain_step_sync(
        cfg,
        subscriber,
        SyncRequest {
            port: port.to_string(),
            body: body.to_string(),
            chain: vec![],
            reply,
        },
    )
    .await;
    answer.await.expect("the drain answers its caller")
}

/// A sync call is one ordinary activation plus a reply: every bound input is
/// windowed and advanced over, the request rides in a window of its own, and
/// the callee's buffer is on the bus before the answer comes back.
///
/// The fixture answers with the same per-port summary it would have published,
/// which is how the windows the host assembled are read back through the guest
/// rather than from the host's own bookkeeping.
#[tokio::test]
async fn a_sync_call_is_one_ordinary_activation_plus_a_reply() {
    let slug = "sync-shape";
    let (messenger, in_entries, out_entry, out_sub, wasm_sub, mut cfg, _alert_handle, _store_db) =
        build_multiport_setup(slug, &["sync-shape-a", "sync-shape-b"]).await;
    cfg.sync_ports = ["ask".to_string()].into_iter().collect();

    testutils::insert_bus_message(&messenger, &in_entries[0], "body-a", ChannelScheme::Brenn).await;
    testutils::insert_bus_message(&messenger, &in_entries[1], "body-b", ChannelScheme::Brenn).await;

    let answer = call(&cfg, &wasm_sub, "ask", "__reply__").await;

    let SyncAnswer::Ok(Some(reply)) = answer else {
        panic!("the callee answered ok with a reply, got {answer:?}");
    };
    let summary: serde_json::Value =
        serde_json::from_str(&reply).expect("the reply is the fixture's summary");
    let ports: Vec<&str> = summary
        .as_array()
        .expect("the summary is an array")
        .iter()
        .map(|entry| entry["port"].as_str().expect("each entry names its port"))
        .collect();
    assert_eq!(
        ports,
        vec!["in0", "in1", "ask"],
        "every bound input is windowed, and the request rides last"
    );
    assert_eq!(
        summary[2]["len"], 1,
        "the request's window carries exactly the one request"
    );
    assert_eq!(summary[2]["new_from"], 0, "all of it is new");
    assert_eq!(summary[2]["dropped"], 0, "and nothing was dropped");

    assert!(
        brenn_messaging::testutils::owed_everywhere(&messenger, &wasm_sub)
            .await
            .is_empty(),
        "a sync call consumes queued input: every input position advanced"
    );
    assert_eq!(
        out_bodies(&messenger, &out_entry.address).await,
        vec!["buffered-before-reply".to_string()],
        "the buffer flushed before the answer was handed back"
    );

    // And no async activation follows for input this one already drained.
    drain_step(&cfg, &wasm_sub, MountDebt::Settled).await;
    assert_eq!(
        brenn_messaging::testutils::owed_everywhere(&messenger, &out_sub)
            .await
            .len(),
        1,
        "the consumed input causes no second activation"
    );
}

/// A sync call over ports that hold nothing is still an activation: the caller
/// is blocked, so there is nothing to elide. The `Delivery`-only invariant — a
/// wake carried something new — does not bind this cause.
#[tokio::test]
async fn a_sync_call_over_empty_ports_still_activates() {
    let slug = "sync-empty";
    let (messenger, _in_entries, out_entry, _out_sub, wasm_sub, mut cfg, _alert_handle, _store_db) =
        build_multiport_setup(slug, &["sync-empty-a"]).await;
    cfg.sync_ports = ["ask".to_string()].into_iter().collect();

    let answer = call(&cfg, &wasm_sub, "ask", "__reply__").await;
    assert!(
        matches!(answer, SyncAnswer::Ok(Some(_))),
        "an empty-window sync call runs and answers, got {answer:?}"
    );
    assert_eq!(
        out_bodies(&messenger, &out_entry.address).await.len(),
        1,
        "and it flushed like any other ok"
    );
}

/// A callee that errs answers `Err` with its own sanitized account, and flushes
/// nothing — the same disposition an async activation gets, plus the answer.
#[tokio::test]
async fn a_callee_that_errs_answers_err_and_flushes_nothing() {
    let slug = "sync-err";
    let (messenger, _in_entries, out_entry, _out_sub, wasm_sub, mut cfg, _alert_handle, _store_db) =
        build_multiport_setup(slug, &["sync-err-a"]).await;
    cfg.sync_ports = ["ask".to_string()].into_iter().collect();

    let answer = call(&cfg, &wasm_sub, "ask", "__err__").await;
    let SyncAnswer::Err(diagnostic) = answer else {
        panic!("an erring callee answers Err, got {answer:?}");
    };
    assert!(
        diagnostic.contains("__err__"),
        "the callee's own account reaches the caller: {diagnostic}"
    );
    assert!(
        out_bodies(&messenger, &out_entry.address).await.is_empty(),
        "an err flushes nothing"
    );
}

/// A callee that traps answers `Trap`. The caller is told to stop; the consumer
/// itself lives on, which is this host's recorded disposition.
#[tokio::test]
async fn a_callee_that_traps_answers_trap() {
    let slug = "sync-trap";
    let (messenger, _in_entries, out_entry, _out_sub, wasm_sub, mut cfg, _alert_handle, _store_db) =
        build_multiport_setup(slug, &["sync-trap-a"]).await;
    cfg.sync_ports = ["ask".to_string()].into_iter().collect();

    let answer = call(&cfg, &wasm_sub, "ask", "__trap__").await;
    assert!(
        matches!(answer, SyncAnswer::Trap),
        "a trapping callee answers Trap, got {answer:?}"
    );
    assert!(
        out_bodies(&messenger, &out_entry.address).await.is_empty(),
        "a trap flushes nothing"
    );
}

/// A sync-call activation that trapped over messages it consumed leaves the
/// operator the same account an async one does: a quarantine row naming what it
/// choked on. The widened skip condition must not have taken the record with
/// it.
#[tokio::test]
async fn a_trapping_sync_call_that_consumed_input_records_its_failure() {
    let slug = "sync-trap-record";
    let (messenger, in_entries, _out_entry, _out_sub, wasm_sub, mut cfg, _alert_handle, _store_db) =
        build_multiport_setup(slug, &["sync-trap-record-a"]).await;
    cfg.sync_ports = ["ask".to_string()].into_iter().collect();

    testutils::insert_bus_message(&messenger, &in_entries[0], "eaten", ChannelScheme::Brenn).await;

    let answer = call(&cfg, &wasm_sub, "ask", "__trap__").await;
    assert!(matches!(answer, SyncAnswer::Trap), "{answer:?}");

    let rows = failure_rows(&messenger, &wasm_sub).await;
    assert_eq!(
        rows.len(),
        1,
        "the messages the trapped activation consumed are named in a row: {rows:?}"
    );
    assert_eq!(rows[0].0, "trap");
}

/// And a sync-call activation that carried nothing new writes no row: the rows
/// exist to say which messages a component choked on, and this one consumed
/// none. The deliberate skip, pinned rather than incidental.
#[tokio::test]
async fn a_trapping_sync_call_over_empty_ports_records_nothing() {
    let slug = "sync-trap-empty";
    let (messenger, _in, _out_entry, _out_sub, wasm_sub, mut cfg, _alert_handle, _store_db) =
        build_multiport_setup(slug, &["sync-trap-empty-a"]).await;
    cfg.sync_ports = ["ask".to_string()].into_iter().collect();

    let answer = call(&cfg, &wasm_sub, "ask", "__trap__").await;
    assert!(matches!(answer, SyncAnswer::Trap), "{answer:?}");

    assert!(
        failure_rows(&messenger, &wasm_sub).await.is_empty(),
        "an activation that consumed nothing has no message to quarantine"
    );
}

/// A request naming a port the specification does not declare `sync` is a
/// caller that bypassed the static checks, and this host will not carry on from
/// a disagreement about the wiring.
#[tokio::test]
#[should_panic(expected = "does not declare sync")]
async fn an_undeclared_sync_port_is_a_host_panic() {
    let slug = "sync-undeclared";
    let (_messenger, _in, _out_entry, _out_sub, wasm_sub, mut cfg, _alert_handle, _store_db) =
        build_multiport_setup(slug, &["sync-undeclared-a"]).await;
    cfg.sync_ports = ["ask".to_string()].into_iter().collect();

    let _ = call(&cfg, &wasm_sub, "shout", "__reply__").await;
}

/// A request arriving with its own target already in its chain is a cycle the
/// document was supposed to refuse. Same rule, same fail-fast.
#[tokio::test]
#[should_panic(expected = "call chain")]
async fn a_target_in_its_own_chain_is_a_host_panic() {
    let slug = "sync-cycle";
    let (_messenger, _in, _out_entry, _out_sub, wasm_sub, mut cfg, _alert_handle, _store_db) =
        build_multiport_setup(slug, &["sync-cycle-a"]).await;
    cfg.sync_ports = ["ask".to_string()].into_iter().collect();

    let (reply, _answer) = tokio::sync::oneshot::channel();
    drain_step_sync(
        &cfg,
        &wasm_sub,
        SyncRequest {
            port: "ask".to_string(),
            body: "__reply__".to_string(),
            chain: vec!["someone-else".to_string(), slug.to_string()],
            reply,
        },
    )
    .await;
}

/// A request body over the deployment's cap is the third caller bug this host
/// will not assemble: the body becomes an envelope in the guest's window, and
/// no envelope reaches a guest over the cap its deployment declared.
#[tokio::test]
#[should_panic(expected = "over the deployment's")]
async fn an_oversize_request_body_is_a_host_panic() {
    let slug = "sync-oversize";
    let (_messenger, _in, _out_entry, _out_sub, wasm_sub, mut cfg, _alert_handle, _store_db) =
        build_multiport_setup(slug, &["sync-oversize-a"]).await;
    cfg.sync_ports = ["ask".to_string()].into_iter().collect();

    let cap = cfg.component.max_payload_bytes();
    let _ = call(&cfg, &wasm_sub, "ask", &"x".repeat(cap + 1)).await;
}

/// Every request a stopping consumer will never serve is answered on its way
/// out. A caller blocked on a consumer that left service is owed the fact, not
/// a wait that ends when its own process does.
#[tokio::test]
async fn requests_pending_at_stop_are_refused_unregistered() {
    let (tx, mut rx) = sync_request_channel();
    let mut answers = Vec::new();
    for port in ["ask", "ask"] {
        let (reply, answer) = tokio::sync::oneshot::channel();
        tx.send(SyncRequest {
            port: port.to_string(),
            body: "hello".to_string(),
            chain: vec![],
            reply,
        })
        .await
        .expect("the receiver is still open");
        answers.push(answer);
    }

    refuse_pending_sync_requests("stopping", &mut rx);

    for answer in answers {
        assert_eq!(
            answer.await.expect("a pending request is answered"),
            SyncAnswer::Refused(SyncRefusal::Unregistered),
        );
    }
    // And the channel is closed, so a request racing the stop fails at the send.
    let (reply, _answer) = tokio::sync::oneshot::channel();
    assert!(
        tx.send(SyncRequest {
            port: "ask".to_string(),
            body: "late".to_string(),
            chain: vec![],
            reply,
        })
        .await
        .is_err(),
        "a closed request channel refuses the send"
    );
}

/// The same refusal, driven through the stop arm of the real task rather than
/// the helper it calls.
///
/// The queue is filled and the task stopped at once, so whichever requests the
/// loop had not served when the stop landed take the stop path. Every caller is
/// answered: a served one with its activation's answer, an unserved one with
/// the refusal. What this rules out is the stop arm returning without draining
/// the queue, which would drop the request's `oneshot` sender and leave every
/// blocked caller with a channel error instead of the fact.
#[tokio::test]
async fn a_stopping_task_refuses_the_requests_it_will_not_serve() {
    let slug = "sync-stop-arm";
    let (_messenger, _in, _out_entry, _out_sub, _wasm_sub, mut cfg, _alert_handle, _store_db) =
        build_multiport_setup(slug, &["sync-stop-arm-a"]).await;
    cfg.sync_ports = ["ask".to_string()].into_iter().collect();

    let (sync_tx, sync_rx) = sync_request_channel();
    let handle = spawn_wasm_consumer_task(cfg, sync_rx);

    let mut answers = Vec::new();
    for _ in 0..SYNC_REQUEST_QUEUE_DEPTH {
        let (reply, answer) = tokio::sync::oneshot::channel();
        sync_tx
            .send(SyncRequest {
                port: "ask".to_string(),
                body: "__reply__".to_string(),
                chain: vec![],
                reply,
            })
            .await
            .expect("the task holds the receiver");
        answers.push(answer);
    }
    handle.stop_and_join().await;

    let mut refused = 0;
    for answer in answers {
        match answer.await {
            Ok(SyncAnswer::Refused(SyncRefusal::Unregistered)) => refused += 1,
            Ok(SyncAnswer::Ok(_)) => {}
            Ok(other) => panic!("a request is either served or refused, got {other:?}"),
            Err(_) => {
                panic!("the stopping task dropped a caller's reply channel instead of answering it")
            }
        }
    }
    assert!(
        refused > 0,
        "the loop cannot have served a full queue between the sends and the stop"
    );
}

/// A request handed to a running consumer task is served between two drain
/// steps, and the answer carries the whole activation with it.
#[tokio::test]
async fn the_consumer_task_serves_a_request_from_its_own_loop() {
    let slug = "sync-loop";
    let (messenger, in_entries, out_entry, _out_sub, _wasm_sub, mut cfg, _alert_handle, _store_db) =
        build_multiport_setup(slug, &["sync-loop-a"]).await;
    cfg.sync_ports = ["ask".to_string()].into_iter().collect();

    testutils::insert_bus_message(&messenger, &in_entries[0], "queued", ChannelScheme::Brenn).await;

    let (sync_tx, sync_rx) = sync_request_channel();
    let handle = spawn_wasm_consumer_task(cfg, sync_rx);

    let (reply, answer) = tokio::sync::oneshot::channel();
    sync_tx
        .send(SyncRequest {
            port: "ask".to_string(),
            body: "__reply__".to_string(),
            chain: vec![],
            reply,
        })
        .await
        .expect("the task holds the receiver");
    let answer = answer.await.expect("the task answers");
    assert!(
        matches!(answer, SyncAnswer::Ok(Some(_))),
        "the task served the request, got {answer:?}"
    );
    // Whatever the mount activation published, the answer's own flush is on the
    // bus by the time the caller has the answer.
    assert!(
        out_bodies(&messenger, &out_entry.address)
            .await
            .contains(&"buffered-before-reply".to_string()),
        "the flush precedes the answer"
    );

    handle.stop_and_join().await;
}

/// A request queued before the consumer's task starts is served **after** the
/// mount.
///
/// A caller who arrives before the task exists still waits through the mount,
/// and the kind of a consumer's first activation is never a race between its
/// task's start and a caller.
#[tokio::test]
async fn a_request_queued_before_the_task_starts_is_served_after_the_mount() {
    let slug = "sync-prologue";
    let (messenger, _in_entries, out_entry, _out_sub, _wasm_sub, mut cfg, _alert_handle, _store_db) =
        build_multiport_setup(slug, &["sync-prologue-a"]).await;
    cfg.sync_ports = ["ask".to_string()].into_iter().collect();

    // Queued into the channel while nothing is reading it: the task does not
    // exist yet, so this request is waiting from its first instruction.
    let (sync_tx, sync_rx) = sync_request_channel();
    let (reply, answer) = tokio::sync::oneshot::channel();
    sync_tx
        .send(SyncRequest {
            port: "ask".to_string(),
            body: "__reply__".to_string(),
            chain: vec![],
            reply,
        })
        .await
        .expect("the channel holds a request before its reader exists");

    let handle = spawn_wasm_consumer_task(cfg, sync_rx);
    let answer = answer.await.expect("the task answers");
    let SyncAnswer::Ok(Some(reply)) = answer else {
        panic!("the queued request was served and answered, got {answer:?}");
    };
    assert!(
        reply.contains("\"port\":\"ask\""),
        "the answer came from the request's own activation: {reply}"
    );

    // The fixture publishes one summary per ordinary activation and
    // `buffered-before-reply` on the one it answers, so the two are told apart
    // by what reached the channel and in which order.
    let bodies = out_bodies(&messenger, &out_entry.address).await;
    assert_eq!(
        bodies.len(),
        2,
        "the mount activation and the request's, and nothing else: {bodies:?}"
    );
    assert!(
        !bodies[0].contains("\"port\":\"ask\""),
        "the first activation is the mount — no request was windowed in it: {}",
        bodies[0],
    );
    assert_eq!(
        bodies[1], "buffered-before-reply",
        "and the request's activation is the second"
    );

    handle.stop_and_join().await;
}
