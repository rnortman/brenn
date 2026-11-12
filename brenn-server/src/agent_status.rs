//! Agent status: what Claude Code sessions are running, and what each Claude
//! account has spent and been refused.
//!
//! [`AgentStatusReporter`] is a cheap handle on one reporter task. Every entry
//! point stamps the wall clock and sends one event on an unbounded channel; it
//! never awaits and never blocks, so the synchronous broadcast choke point can
//! call it. The task owns the live sessions and the [`AccountLedger`], applies
//! events in arrival order, and — when the agent-status channels are declared —
//! publishes each changed snapshot, awaiting the publish before taking the next
//! event. That is what orders the snapshots on the bus; there is no mutex.
//!
//! The channel is unbounded because a reporter call must neither stall nor drop
//! a bridge event; its depth is bounded in practice by the rate of Claude Code
//! events. The task runs under [`abort_on_panic`]: a panic in it — a refused
//! publish, a ledger invariant — ends the process rather than leaving every
//! bridge to panic at its next report. A send that fails means the task is gone,
//! and panics.
//!
//! Sessions are keyed by conversation id, which a later bridge for the same
//! conversation shares. So each bridge reports through its own handle
//! ([`AgentStatusReporter::for_bridge`]), whose run id the task records on the
//! session the bridge started: a state, cost or end from any other bridge's run
//! never touches it. A profile swap keeps the bridge, and so the run.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use brenn_cc_profile::{AccountLedger, TurnOutcome};
use brenn_envelope::agent::{
    AccountFlag, AccountKind, AgentAccount, AgentSession, AgentSessionEnded, AgentSessions, encode,
};
use brenn_envelope::chat::CcState;
use brenn_lib::config::ClaudeProfile;
use brenn_lib::panic_util::abort_on_panic;
use brenn_messaging::agent_status::{
    ACCOUNTS_ADDRESS, AGENT_STATUS_COMPONENT, ENDED_ADDRESS, SESSIONS_ADDRESS,
};
use futures::future::BoxFuture;
use tokio::sync::mpsc;

/// Where the reporter's snapshots and facts go.
pub trait StatusSink: Send + Sync {
    /// Publish `body` on `address`, completing once it is on the channel.
    fn publish(&self, address: &'static str, body: String) -> BoxFuture<'_, ()>;
}

/// The bus, as `system:agent-status`.
pub struct MessengerSink(pub Arc<brenn_messaging::Messenger>);

impl StatusSink for MessengerSink {
    /// # Panics
    ///
    /// On any outcome but `Ok`. A system publish draws on no send budget, but
    /// it does pass the per-(sender, channel) send-rate gate. Boot refuses a
    /// channel whose resolved rate is below `AGENT_STATUS_MIN_SEND_RATE`; what
    /// remains is a host whose sessions change state faster than the declared
    /// rate above that floor, refused here. That, like every other
    /// refusal — an undeclared channel, a policy that does not cover it, a body
    /// over the cap — leaves a channel whose retained body is false, or an
    /// accounts ledger that was not stored.
    fn publish(&self, address: &'static str, body: String) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let outcome = self
                .0
                .publish_from_system(
                    AGENT_STATUS_COMPONENT,
                    address,
                    &body,
                    brenn_messaging::Urgency::Normal,
                    None,
                )
                .await;
            match outcome {
                brenn_messaging::PublishResult::Ok { .. } => {}
                brenn_messaging::PublishResult::RateLimited => panic!(
                    "system:{AGENT_STATUS_COMPONENT} publish to {address:?} was rate-limited — \
                     the channel's send_rate is below the rate this host's Claude Code sessions \
                     change state; raise it. Refusing to carry on with a false retained status."
                ),
                other => panic!(
                    "system:{AGENT_STATUS_COMPONENT} could not publish on {address}: {other:?}"
                ),
            }
        })
    }
}

/// What the bridge needs to know about a profile to classify a turn, read off
/// the profile's credential at start.
#[derive(Debug, Clone, PartialEq)]
pub struct ProfileFacts {
    pub kind: AccountKind,
    /// The markers whose presence in an error `result` is a key's refusal;
    /// empty for a subscription.
    pub exhausted_markers: Vec<String>,
}

/// Where the reporter publishes, and the ledger it starts from.
pub struct Publishing {
    pub sink: Arc<dyn StatusSink>,
    /// The retained message on the accounts channel, if any.
    pub retained_accounts: Option<String>,
}

/// One thing that happened to a conversation or an account.
enum Event {
    Initialized {
        conversation_id: i64,
        run: u64,
        app: String,
        cc_session_id: String,
        model: String,
        cwd: String,
        profile: Option<String>,
        at_ms: u64,
    },
    State {
        conversation_id: i64,
        run: u64,
        state: CcState,
    },
    Turn {
        conversation_id: i64,
        run: u64,
        profile: Option<String>,
        outcome: TurnOutcome,
        model: Option<String>,
        at_ms: u64,
    },
    BucketRefused {
        profile: String,
        bucket: &'static str,
        flag: AccountFlag,
        at_ms: u64,
    },
    Ended {
        conversation_id: i64,
        run: u64,
        ok: bool,
        at_ms: u64,
    },
    /// Answered once every earlier event is applied and published.
    Flush(tokio::sync::oneshot::Sender<()>),
}

/// The handle on the reporter task. Clones share the task and the run.
#[derive(Clone)]
pub struct AgentStatusReporter {
    tx: mpsc::UnboundedSender<Event>,
    profiles: Arc<BTreeMap<String, ProfileFacts>>,
    /// The run every session event sent through this handle belongs to.
    run: u64,
}

/// The next run id. Process-wide, so two handles never share one by accident.
static NEXT_RUN: AtomicU64 = AtomicU64::new(1);

fn next_run() -> u64 {
    NEXT_RUN.fetch_add(1, Ordering::Relaxed)
}

impl std::fmt::Debug for AgentStatusReporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgentStatusReporter")
            .field("profiles", &self.profiles)
            .field("run", &self.run)
            .finish_non_exhaustive()
    }
}

/// Wall clock, epoch milliseconds: the unit of every instant in an agent-status
/// body.
pub(crate) fn now_ms() -> u64 {
    u64::try_from(chrono::Utc::now().timestamp_millis())
        .expect("the wall clock reads after the epoch")
}

impl AgentStatusReporter {
    /// Build the ledger over `profiles`, seed it from the retained accounts
    /// body when publishing, and spawn the task. With `publishing`, the task's
    /// first act is to publish the seeded accounts snapshot, whatever was
    /// retained: the profile set or the period may have changed under it.
    /// Nothing is published to the sessions channel until a session starts.
    ///
    /// # Panics
    ///
    /// If the retained accounts body does not decode (see
    /// [`AccountLedger::seed`]). Must be called inside a tokio runtime.
    pub fn start(
        profiles: &BTreeMap<String, ClaudeProfile>,
        publishing: Option<Publishing>,
    ) -> Self {
        let now = now_ms();
        let mut ledger = AccountLedger::new(profiles, now);
        let sink = publishing.map(|p| {
            if let Some(body) = &p.retained_accounts {
                ledger.seed(body, now);
            }
            p.sink
        });
        let facts: BTreeMap<String, ProfileFacts> = profiles
            .iter()
            .map(|(name, profile)| {
                let facts = ProfileFacts {
                    kind: profile.credential.kind(),
                    exhausted_markers: profile
                        .credential
                        .key_settings()
                        .map(|key| key.exhausted_markers.clone())
                        .unwrap_or_default(),
                };
                (name.clone(), facts)
            })
            .collect();
        let profiles = Arc::new(facts);
        let (tx, rx) = mpsc::unbounded_channel();
        let task = Task {
            rx,
            profiles: profiles.clone(),
            ledger,
            sessions: BTreeMap::new(),
            superseded: HashMap::new(),
            pending_state: HashMap::new(),
            sink,
            last_sessions: None,
            last_accounts: None,
        };
        drop(tokio::spawn(abort_on_panic(
            "the agent-status reporter task panicked; aborting rather than serving bridges that \
             panic at their next report",
            task.run(),
        )));
        Self {
            tx,
            profiles,
            run: next_run(),
        }
    }

    /// A handle on the same task for one bridge, with a run of its own. A
    /// session that bridge starts is ended, changed and costed only through
    /// this handle or its clones.
    pub fn for_bridge(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            profiles: self.profiles.clone(),
            run: next_run(),
        }
    }

    /// The facts of a configured profile.
    ///
    /// # Panics
    ///
    /// If `name` is not configured: a bridge only runs under a configured one.
    pub fn profile(&self, name: &str) -> &ProfileFacts {
        self.profiles
            .get(name)
            .unwrap_or_else(|| panic!("claude_profile `{name}` is not configured"))
    }

    fn send(&self, event: Event) {
        assert!(
            self.tx.send(event).is_ok(),
            "the agent-status reporter task is gone"
        );
    }

    /// A Claude Code process for `conversation_id` reported `initialized`.
    /// Starts a session, or — for a profile swap's replacement — updates the
    /// tracked one in place.
    pub fn session_initialized(
        &self,
        conversation_id: i64,
        app: &str,
        cc_session_id: &str,
        model: &str,
        cwd: &str,
        profile: Option<String>,
    ) {
        if let Some(name) = &profile {
            self.profile(name);
        }
        self.send(Event::Initialized {
            conversation_id,
            run: self.run,
            app: app.to_string(),
            cc_session_id: cc_session_id.to_string(),
            model: model.to_string(),
            cwd: cwd.to_string(),
            profile,
            at_ms: now_ms(),
        });
    }

    /// The conversation's Claude Code state changed.
    pub fn session_state(&self, conversation_id: i64, state: CcState) {
        self.send(Event::State {
            conversation_id,
            run: self.run,
            state,
        });
    }

    /// A `result` frame that counts as a turn: its cost goes to the session,
    /// and under an API-key profile the ledger's `turn` runs.
    pub fn turn_completed(
        &self,
        conversation_id: i64,
        profile: Option<String>,
        outcome: TurnOutcome,
        model: Option<String>,
    ) {
        if let Some(name) = &profile {
            self.profile(name);
        }
        self.send(Event::Turn {
            conversation_id,
            run: self.run,
            profile,
            outcome,
            model,
            at_ms: now_ms(),
        });
    }

    /// A subscription profile was refused in `bucket`.
    pub fn bucket_refused(&self, profile: String, bucket: &'static str, flag: AccountFlag) {
        self.send(Event::BucketRefused {
            profile,
            bucket,
            flag,
            at_ms: now_ms(),
        });
    }

    /// The conversation's Claude Code process stopped. `ok` is whether it
    /// stopped on purpose. A no-op unless this handle's run of the
    /// conversation is tracked: a second end for one death, or an end from a
    /// bridge a successor has already replaced, changes nothing.
    pub fn session_ended(&self, conversation_id: i64, ok: bool) {
        self.send(Event::Ended {
            conversation_id,
            run: self.run,
            ok,
            at_ms: now_ms(),
        });
    }

    /// Wait until every event sent before this call — and, from a fresh
    /// handle, the boot publish of the accounts snapshot — is applied and
    /// published.
    pub async fn flush(&self) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.send(Event::Flush(tx));
        rx.await
            .expect("the agent-status reporter task answers a flush");
    }
}

/// A session and the run that started it.
struct Tracked {
    run: u64,
    session: AgentSession,
}

/// The reporter task's state.
struct Task {
    rx: mpsc::UnboundedReceiver<Event>,
    profiles: Arc<BTreeMap<String, ProfileFacts>>,
    ledger: AccountLedger,
    /// Every conversation with a running Claude Code process.
    sessions: BTreeMap<i64, Tracked>,
    /// Runs a newer run of the same conversation replaced before they
    /// reported their end, by `(conversation_id, run)`. Out of the sessions
    /// snapshot already; held for the ended fact their end publishes.
    superseded: HashMap<(i64, u64), AgentSession>,
    /// The latest state of a run that has not reported `initialized` yet, by
    /// `(conversation_id, run)`.
    pending_state: HashMap<(i64, u64), CcState>,
    sink: Option<Arc<dyn StatusSink>>,
    last_sessions: Option<Vec<AgentSession>>,
    last_accounts: Option<Vec<AgentAccount>>,
}

impl Task {
    async fn run(mut self) {
        if self.sink.is_some() {
            self.publish_accounts(now_ms(), true).await;
        }
        while let Some(event) = self.rx.recv().await {
            self.apply(event).await;
        }
    }

    async fn apply(&mut self, event: Event) {
        match event {
            Event::Initialized {
                conversation_id,
                run,
                app,
                cc_session_id,
                model,
                cwd,
                profile,
                at_ms,
            } => {
                let account_kind = profile.as_ref().map(|name| self.kind(name));
                match self.sessions.get_mut(&conversation_id) {
                    // A swap's replacement process: same bridge, same run.
                    Some(tracked) if tracked.run == run => {
                        let session = &mut tracked.session;
                        session.account = profile;
                        session.account_kind = account_kind;
                        session.cc_session_id = Some(cc_session_id);
                        session.model = Some(model);
                        session.cwd = Some(cwd);
                    }
                    found => {
                        let state = self
                            .pending_state
                            .remove(&(conversation_id, run))
                            .unwrap_or(CcState::Idle);
                        let tracked = Tracked {
                            run,
                            session: AgentSession {
                                conversation_id,
                                app,
                                state,
                                started_at_ms: at_ms,
                                cc_session_id: Some(cc_session_id),
                                model: Some(model),
                                account: profile,
                                account_kind,
                                cwd: Some(cwd),
                                cost_usd: 0.0,
                            },
                        };
                        // A successor bridge started before the run it
                        // replaced reported its end: that run leaves the
                        // snapshot now and keeps its record for its end.
                        if let Some(replaced) = found {
                            let replaced = std::mem::replace(replaced, tracked);
                            self.superseded
                                .insert((conversation_id, replaced.run), replaced.session);
                        } else {
                            self.sessions.insert(conversation_id, tracked);
                        }
                    }
                }
                self.publish_sessions(at_ms).await;
            }
            Event::State {
                conversation_id,
                run,
                state,
            } => match self.session_of(conversation_id, run) {
                Some(session) => {
                    session.state = state;
                    self.publish_sessions(now_ms()).await;
                }
                None => {
                    self.pending_state.insert((conversation_id, run), state);
                }
            },
            Event::Turn {
                conversation_id,
                run,
                profile,
                outcome,
                model,
                at_ms,
            } => {
                if let Some(session) = self.session_of(conversation_id, run) {
                    session.cost_usd += outcome.cost;
                    if model.is_some() {
                        session.model = model;
                    }
                }
                // The spend is the account's whichever run incurred it.
                if let Some(name) = profile
                    && self.kind(&name) == AccountKind::ApiKey
                {
                    self.ledger.turn(&name, outcome, at_ms);
                    self.publish_accounts(at_ms, false).await;
                }
                self.publish_sessions(at_ms).await;
            }
            Event::BucketRefused {
                profile,
                bucket,
                flag,
                at_ms,
            } => {
                self.ledger.flag_bucket(&profile, bucket, flag, at_ms);
                self.publish_accounts(at_ms, false).await;
            }
            Event::Ended {
                conversation_id,
                run,
                ok,
                at_ms,
            } => {
                self.pending_state.remove(&(conversation_id, run));
                let session = match self.sessions.get(&conversation_id) {
                    Some(tracked) if tracked.run == run => {
                        let tracked = self
                            .sessions
                            .remove(&conversation_id)
                            .expect("the tracked session was just read");
                        // State before fact: once the fact is on `agent.ended`,
                        // the retained snapshot no longer lists the run. A
                        // reader of both channels may still hold the fact
                        // beside an older snapshot, so it must match the two
                        // on `conversation_id` and `started_at_ms`.
                        self.publish_sessions(at_ms).await;
                        tracked.session
                    }
                    // Out of the snapshot since its successor started.
                    _ => match self.superseded.remove(&(conversation_id, run)) {
                        Some(session) => session,
                        None => return,
                    },
                };
                if let Some(sink) = &self.sink {
                    let ended = AgentSessionEnded {
                        duration_ms: at_ms.saturating_sub(session.started_at_ms),
                        session,
                        ended_at_ms: at_ms,
                        ok,
                    };
                    sink.publish(ENDED_ADDRESS, encode(&ended)).await;
                }
            }
            // A caller that stopped waiting is owed nothing.
            Event::Flush(tx) => tx.send(()).unwrap_or(()),
        }
    }

    /// The session `run` started for `conversation_id`, whether it is the
    /// tracked one or one a successor replaced.
    fn session_of(&mut self, conversation_id: i64, run: u64) -> Option<&mut AgentSession> {
        match self.sessions.get_mut(&conversation_id) {
            Some(tracked) if tracked.run == run => Some(&mut tracked.session),
            _ => self.superseded.get_mut(&(conversation_id, run)),
        }
    }

    fn kind(&self, profile: &str) -> AccountKind {
        self.profiles
            .get(profile)
            .unwrap_or_else(|| panic!("claude_profile `{profile}` is not configured"))
            .kind
    }

    /// Publish the sessions snapshot unless it equals the last one published.
    async fn publish_sessions(&mut self, at_ms: u64) {
        let Some(sink) = &self.sink else {
            return;
        };
        let sessions: Vec<AgentSession> = self
            .sessions
            .values()
            .map(|tracked| tracked.session.clone())
            .collect();
        if self.last_sessions.as_ref() == Some(&sessions) {
            return;
        }
        let body = encode(&AgentSessions {
            at_ms,
            sessions: sessions.clone(),
        });
        sink.publish(SESSIONS_ADDRESS, body).await;
        self.last_sessions = Some(sessions);
    }

    /// Publish the accounts snapshot unless it equals the last one published
    /// and `force` is false.
    async fn publish_accounts(&mut self, at_ms: u64, force: bool) {
        let Some(sink) = &self.sink else {
            return;
        };
        let snapshot = self.ledger.snapshot(at_ms);
        if !force && self.last_accounts.as_ref() == Some(&snapshot.accounts) {
            return;
        }
        sink.publish(ACCOUNTS_ADDRESS, encode(&snapshot)).await;
        self.last_accounts = Some(snapshot.accounts);
    }
}

#[cfg(test)]
pub(crate) mod tests;
