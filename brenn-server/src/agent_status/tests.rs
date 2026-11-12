use std::sync::Mutex;

use brenn_envelope::agent::{AgentAccounts, bucket, decode};
use brenn_lib::config::{ClaudeCredential, KeyProfileSettings, SecretString};

use super::*;

/// Every publish, in order, as `(address, body)`.
#[derive(Default)]
pub(crate) struct Recording(Mutex<Vec<(&'static str, String)>>);

impl StatusSink for Recording {
    fn publish(&self, address: &'static str, body: String) -> BoxFuture<'_, ()> {
        self.0.lock().expect("recording lock").push((address, body));
        Box::pin(async {})
    }
}

impl Recording {
    /// Take everything published so far.
    pub(crate) fn take(&self) -> Vec<(&'static str, String)> {
        std::mem::take(&mut *self.0.lock().expect("recording lock"))
    }
}

/// A subscription profile `personal` and an API-key profile `work-key`.
pub(crate) fn profiles() -> BTreeMap<String, ClaudeProfile> {
    BTreeMap::from([
        (
            "personal".to_string(),
            ClaudeProfile {
                credential: ClaudeCredential::OauthToken(SecretString::new("tok".to_string())),
                expires: None,
            },
        ),
        (
            "work-key".to_string(),
            ClaudeProfile {
                credential: ClaudeCredential::ApiKey {
                    secret: SecretString::new("sk".to_string()),
                    settings: KeyProfileSettings {
                        budget_usd: Some(50.0),
                        reset_tz: chrono_tz::UTC,
                        exhausted_markers: vec!["credit balance is too low".to_string()],
                    },
                },
                expires: None,
            },
        ),
    ])
}

/// A reporter publishing into a recording, with the boot publish already
/// taken off it.
async fn reporter() -> (AgentStatusReporter, Arc<Recording>) {
    reporter_seeded(None).await
}

async fn reporter_seeded(retained: Option<String>) -> (AgentStatusReporter, Arc<Recording>) {
    let sink = Arc::new(Recording::default());
    let reporter = AgentStatusReporter::start(
        &profiles(),
        Some(Publishing {
            sink: sink.clone(),
            retained_accounts: retained,
        }),
    );
    reporter.flush().await;
    let boot = sink.take();
    assert_eq!(boot.len(), 1, "boot publishes the accounts snapshot alone");
    assert_eq!(boot[0].0, ACCOUNTS_ADDRESS);
    (reporter, sink)
}

pub(crate) fn sessions_of(body: &str) -> Vec<AgentSession> {
    decode::<AgentSessions>(body)
        .expect("a sessions body")
        .sessions
}

pub(crate) fn accounts_of(body: &str) -> Vec<AgentAccount> {
    decode::<AgentAccounts>(body)
        .expect("an accounts body")
        .accounts
}

fn start(reporter: &AgentStatusReporter, id: i64, profile: Option<&str>) {
    reporter.session_initialized(
        id,
        "assistant",
        &format!("cc-{id}"),
        "claude-opus-4",
        "/srv/work",
        profile.map(str::to_string),
    );
}

fn ok_turn(cost: f64) -> TurnOutcome {
    TurnOutcome {
        cost,
        is_error: false,
        refusal: None,
    }
}

#[tokio::test]
async fn boot_publishes_accounts_even_when_identical_to_what_was_retained() {
    let first = Arc::new(Recording::default());
    let reporter = AgentStatusReporter::start(
        &profiles(),
        Some(Publishing {
            sink: first.clone(),
            retained_accounts: None,
        }),
    );
    reporter.flush().await;
    let retained = first.take().remove(0).1;

    let (_reporter, sink) = reporter_seeded(Some(retained.clone())).await;
    assert!(sink.take().is_empty());
    let accounts = accounts_of(&retained);
    assert_eq!(
        accounts
            .iter()
            .map(|a| a.account.as_str())
            .collect::<Vec<_>>(),
        ["personal", "work-key"]
    );
}

#[tokio::test]
async fn the_seed_carries_spend_across_a_restart() {
    let (reporter, sink) = reporter().await;
    start(&reporter, 1, Some("work-key"));
    reporter.turn_completed(1, Some("work-key".to_string()), ok_turn(2.5), None);
    reporter.flush().await;
    let retained = sink
        .take()
        .into_iter()
        .rev()
        .find(|(address, _)| *address == ACCOUNTS_ADDRESS)
        .expect("the turn published accounts")
        .1;

    let sink = Arc::new(Recording::default());
    let reporter = AgentStatusReporter::start(
        &profiles(),
        Some(Publishing {
            sink: sink.clone(),
            retained_accounts: Some(retained),
        }),
    );
    reporter.flush().await;
    let boot = sink.take();
    let key = accounts_of(&boot[0].1)
        .into_iter()
        .find(|a| a.account == "work-key")
        .expect("the key's record");
    assert_eq!(key.usd, 2.5);
    assert_eq!(key.turns, Some(1));
}

#[tokio::test]
async fn a_session_starts_changes_state_and_charges_a_turn() {
    let (reporter, sink) = reporter().await;
    let before = now_ms();
    start(&reporter, 7, Some("personal"));
    reporter.session_state(7, CcState::Thinking);
    reporter.turn_completed(
        7,
        Some("personal".to_string()),
        ok_turn(0.25),
        Some("claude-fable-1".to_string()),
    );
    reporter.flush().await;
    let published = sink.take();
    assert!(
        published.iter().all(|(a, _)| *a == SESSIONS_ADDRESS),
        "a subscription turn does not touch the ledger: {published:?}"
    );
    let snapshots: Vec<Vec<AgentSession>> = published.iter().map(|(_, b)| sessions_of(b)).collect();
    assert_eq!(snapshots.len(), 3);
    let first = &snapshots[0][0];
    assert_eq!(first.conversation_id, 7);
    assert_eq!(first.app, "assistant");
    assert_eq!(first.state, CcState::Idle);
    assert!(first.started_at_ms >= before);
    assert_eq!(first.cc_session_id.as_deref(), Some("cc-7"));
    assert_eq!(first.account.as_deref(), Some("personal"));
    assert_eq!(first.account_kind, Some(AccountKind::Subscription));
    assert_eq!(first.cwd.as_deref(), Some("/srv/work"));
    assert_eq!(snapshots[1][0].state, CcState::Thinking);
    assert_eq!(snapshots[2][0].cost_usd, 0.25);
    assert_eq!(snapshots[2][0].model.as_deref(), Some("claude-fable-1"));
}

#[tokio::test]
async fn an_unchanged_snapshot_is_not_republished() {
    let (reporter, sink) = reporter().await;
    start(&reporter, 7, None);
    reporter.session_state(7, CcState::Idle);
    reporter.session_state(7, CcState::Thinking);
    reporter.session_state(7, CcState::Thinking);
    reporter.flush().await;
    let states: Vec<CcState> = sink
        .take()
        .iter()
        .map(|(_, b)| sessions_of(b)[0].state)
        .collect();
    assert_eq!(states, [CcState::Idle, CcState::Thinking]);
}

#[tokio::test]
async fn a_state_before_initialized_is_the_session_s_first_state() {
    let (reporter, sink) = reporter().await;
    reporter.session_state(7, CcState::Thinking);
    reporter.flush().await;
    assert!(sink.take().is_empty(), "no session is running yet");
    start(&reporter, 7, None);
    reporter.flush().await;
    let published = sink.take();
    assert_eq!(sessions_of(&published[0].1)[0].state, CcState::Thinking);
    assert_eq!(sessions_of(&published[0].1)[0].account, None);
    assert_eq!(sessions_of(&published[0].1)[0].account_kind, None);
}

#[tokio::test]
async fn a_key_turn_charges_the_ledger_and_a_refusal_flags_it() {
    let (reporter, sink) = reporter().await;
    start(&reporter, 7, Some("work-key"));
    reporter.turn_completed(7, Some("work-key".to_string()), ok_turn(1.5), None);
    reporter.turn_completed(
        7,
        Some("work-key".to_string()),
        TurnOutcome {
            cost: 0.0,
            is_error: true,
            refusal: Some(AccountFlag {
                at_ms: 1,
                session: Some("7".to_string()),
                message: Some("Credit balance is too low".to_string()),
                until_ms: None,
                kind: None,
            }),
        },
        None,
    );
    reporter.flush().await;
    let accounts: Vec<AgentAccount> = sink
        .take()
        .iter()
        .filter(|(a, _)| *a == ACCOUNTS_ADDRESS)
        .map(|(_, b)| {
            accounts_of(b)
                .into_iter()
                .find(|a| a.account == "work-key")
                .expect("the key's record")
        })
        .collect();
    assert_eq!(accounts.len(), 2);
    assert_eq!(accounts[0].usd, 1.5);
    assert_eq!(accounts[0].turns, Some(1));
    assert_eq!(accounts[0].exhausted, None);
    assert_eq!(accounts[1].turns, Some(2));
    assert_eq!(
        accounts[1]
            .exhausted
            .as_ref()
            .and_then(|f| f.message.as_deref()),
        Some("Credit balance is too low")
    );
}

#[tokio::test]
async fn a_bucket_refusal_flags_the_subscription() {
    let (reporter, sink) = reporter().await;
    let until = now_ms() + 3_600_000;
    reporter.bucket_refused(
        "personal".to_string(),
        bucket::FIVE_HOUR,
        AccountFlag {
            at_ms: 1,
            session: Some("7".to_string()),
            message: None,
            until_ms: Some(until),
            kind: Some("five_hour".to_string()),
        },
    );
    reporter.flush().await;
    let published = sink.take();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].0, ACCOUNTS_ADDRESS);
    let personal = accounts_of(&published[0].1)
        .into_iter()
        .find(|a| a.account == "personal")
        .expect("the subscription's record");
    assert_eq!(
        personal.exhausted_buckets[bucket::FIVE_HOUR].until_ms,
        Some(until)
    );
}

#[tokio::test]
async fn a_swap_updates_the_account_in_place() {
    let (reporter, sink) = reporter().await;
    start(&reporter, 7, Some("personal"));
    reporter.turn_completed(7, Some("personal".to_string()), ok_turn(0.5), None);
    reporter.flush().await;
    let before = sessions_of(&sink.take().last().expect("a snapshot").1)[0].clone();

    reporter.session_initialized(
        7,
        "assistant",
        "cc-7b",
        "claude-sonnet-4",
        "/srv/work",
        Some("work-key".to_string()),
    );
    reporter.flush().await;
    let published = sink.take();
    assert_eq!(published.len(), 1, "a swap publishes no ended fact");
    let after = &sessions_of(&published[0].1)[0];
    assert_eq!(after.account.as_deref(), Some("work-key"));
    assert_eq!(after.account_kind, Some(AccountKind::ApiKey));
    assert_eq!(after.cc_session_id.as_deref(), Some("cc-7b"));
    assert_eq!(after.model.as_deref(), Some("claude-sonnet-4"));
    assert_eq!(after.started_at_ms, before.started_at_ms);
    assert_eq!(after.cost_usd, 0.5);
}

#[tokio::test]
async fn an_end_publishes_state_before_fact() {
    let (reporter, sink) = reporter().await;
    start(&reporter, 7, None);
    start(&reporter, 9, None);
    reporter.turn_completed(7, None, ok_turn(0.75), None);
    reporter.session_ended(7, true);
    reporter.flush().await;
    let published = sink.take();
    let tail = &published[published.len() - 2..];
    assert_eq!(tail[0].0, SESSIONS_ADDRESS);
    assert_eq!(
        sessions_of(&tail[0].1)
            .iter()
            .map(|s| s.conversation_id)
            .collect::<Vec<_>>(),
        [9]
    );
    assert_eq!(tail[1].0, ENDED_ADDRESS);
    let ended: AgentSessionEnded = decode(&tail[1].1).expect("an ended body");
    assert_eq!(ended.session.conversation_id, 7);
    assert_eq!(ended.session.cost_usd, 0.75);
    assert!(ended.ok);
    assert_eq!(
        ended.duration_ms,
        ended.ended_at_ms - ended.session.started_at_ms
    );
}

#[tokio::test]
async fn an_unexpected_end_is_not_ok_and_a_second_end_is_a_no_op() {
    let (reporter, sink) = reporter().await;
    start(&reporter, 7, None);
    reporter.session_ended(7, false);
    reporter.session_ended(7, true);
    reporter.flush().await;
    let ended: Vec<AgentSessionEnded> = sink
        .take()
        .iter()
        .filter(|(a, _)| *a == ENDED_ADDRESS)
        .map(|(_, b)| decode(b).expect("an ended body"))
        .collect();
    assert_eq!(ended.len(), 1);
    assert!(!ended[0].ok, "the first call's `ok` stands");
}

#[tokio::test]
async fn an_end_with_no_session_publishes_nothing() {
    let (reporter, sink) = reporter().await;
    reporter.session_ended(7, false);
    reporter.flush().await;
    assert!(sink.take().is_empty());
}

#[tokio::test]
async fn a_stop_then_a_wake_runs_the_same_id_again() {
    let (reporter, sink) = reporter().await;
    start(&reporter, 7, None);
    reporter.turn_completed(7, None, ok_turn(1.0), None);
    reporter.session_ended(7, true);
    start(&reporter, 7, None);
    reporter.flush().await;
    let published = sink.take();
    let addresses: Vec<&str> = published.iter().map(|(a, _)| *a).collect();
    assert_eq!(
        addresses,
        [
            SESSIONS_ADDRESS,
            SESSIONS_ADDRESS,
            SESSIONS_ADDRESS,
            ENDED_ADDRESS,
            SESSIONS_ADDRESS
        ]
    );
    let again = &sessions_of(&published[4].1)[0];
    assert_eq!(again.conversation_id, 7);
    assert_eq!(again.cost_usd, 0.0, "cost is per run");
}

fn ended_of(published: &[(&'static str, String)]) -> Vec<AgentSessionEnded> {
    published
        .iter()
        .filter(|(a, _)| *a == ENDED_ADDRESS)
        .map(|(_, b)| decode(b).expect("an ended body"))
        .collect()
}

/// The wedge watchdog's path: the old bridge's run is ended by the reset, a
/// successor bridge starts the conversation again, and only then does the old
/// event loop report its end and a stale state. Neither touches the successor.
#[tokio::test]
async fn a_replaced_bridge_s_late_end_and_state_leave_the_successor_running() {
    let (reporter, sink) = reporter().await;
    let old = reporter.for_bridge();
    let new = reporter.for_bridge();
    start(&old, 7, None);
    old.session_ended(7, false);
    start(&new, 7, None);
    reporter.flush().await;
    sink.take();
    old.session_state(7, CcState::Thinking);
    old.session_ended(7, true);
    reporter.flush().await;
    let published = sink.take();
    assert!(published.is_empty(), "{published:?}");

    new.session_state(7, CcState::Thinking);
    new.session_ended(7, true);
    reporter.flush().await;
    let ended = ended_of(&sink.take());
    assert_eq!(ended.len(), 1);
    assert_eq!(ended[0].session.state, CcState::Thinking);
}

/// A successor that starts before the run it replaces has reported its end:
/// the old run leaves the snapshot at once, keeps its own cost, and still gets
/// its ended fact.
#[tokio::test]
async fn a_successor_started_first_supersedes_the_old_run() {
    let (reporter, sink) = reporter().await;
    let old = reporter.for_bridge();
    let new = reporter.for_bridge();
    start(&old, 7, None);
    new.session_initialized(7, "assistant", "cc-new", "claude-opus-4", "/srv/work", None);
    old.turn_completed(7, None, ok_turn(0.3), None);
    reporter.flush().await;
    let published = sink.take();
    let running = sessions_of(&published.last().expect("a sessions snapshot").1);
    assert_eq!(running.len(), 1);
    assert_eq!(running[0].cc_session_id.as_deref(), Some("cc-new"));
    assert_eq!(running[0].cost_usd, 0.0, "the old run's cost is its own");

    old.session_ended(7, true);
    reporter.flush().await;
    let published = sink.take();
    assert_eq!(
        published.iter().map(|(a, _)| *a).collect::<Vec<_>>(),
        [ENDED_ADDRESS],
        "the snapshot already lacks the old run"
    );
    let ended = &ended_of(&published)[0];
    assert_eq!(ended.session.cc_session_id.as_deref(), Some("cc-7"));
    assert_eq!(ended.session.cost_usd, 0.3);
    assert!(ended.ok);
}

#[tokio::test]
async fn without_publishing_nothing_is_published_and_the_ledger_still_runs() {
    let reporter = AgentStatusReporter::start(&profiles(), None);
    start(&reporter, 7, Some("work-key"));
    reporter.turn_completed(7, Some("work-key".to_string()), ok_turn(1.0), None);
    reporter.session_ended(7, true);
    reporter.flush().await;
    assert_eq!(reporter.profile("work-key").kind, AccountKind::ApiKey);
    assert_eq!(
        reporter.profile("work-key").exhausted_markers,
        ["credit balance is too low"]
    );
    assert!(reporter.profile("personal").exhausted_markers.is_empty());
}

#[test]
#[should_panic(expected = "is not configured")]
fn an_unknown_profile_panics_at_the_call() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    let _guard = runtime.enter();
    let reporter = AgentStatusReporter::start(&profiles(), None);
    start(&reporter, 7, Some("nobody"));
}
