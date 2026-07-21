//! Automation engine — general trigger->action framework.
//!
//! This iteration instantiates one trigger (cron-time) and one action
//! (send-message). Future variants plug into the same data model and
//! execution loop without redesign.
//!
//! Module layout:
//! - `config`    — `[automation]` config section.
//! - `db`        — schema DDL + DAO helpers.
//! - `fire`      — per-fire logic (auth + budget + send + bookkeeping).
//! - `job`       — job/trigger/action types, MCP tool-name constants.
//! - `loop_task` — background scheduler loop (poll + kick).
//! - `startup`   — startup-time consistency checks (rebind + orphan-disable).

pub mod config;
pub mod db;
pub mod error_payload;
pub mod fire;
pub mod job;
pub mod loop_task;
pub mod startup;
#[cfg(test)]
pub(super) mod test_support;

use std::sync::Arc;

use chrono::Utc;
use indexmap::IndexMap;
use tokio::sync::Notify;
use uuid::Uuid;

use crate::config::AppConfig;
use crate::db::Db;
use crate::messaging::{MessagingDirectory, Messenger};
use crate::obs::alerting::AlertDispatcher;
use crate::obs::security::DenialKind;

pub use config::AutomationGlobalConfig;
pub use db::run_automation_migrations;
pub use job::{
    Action, CreateJob, CronTrigger, EditJob, JobSnapshot, JobView, MCP_AUTO_CREATE_TOOL,
    MCP_AUTO_DELETE_TOOL, MCP_AUTO_EDIT_TOOL, MCP_AUTO_LIST_TOOL, SendMessageAction, Trigger,
};

// ---------------------------------------------------------------------------
// IngressRouter trait
// ---------------------------------------------------------------------------

/// Abstraction over `Messenger::submit_ingress`. Implemented in the binary crate.
///
/// This trait exists so `brenn-lib` can submit ingress events without
/// depending on binary-crate types. The binary crate wires this up via a
/// thin newtype around `Arc<AppState>`.
#[async_trait::async_trait]
pub trait IngressRouter: Send + Sync + 'static {
    /// Durably record an ingress event for `conversation_id` and deliver
    /// immediately if a bridge is attached, else queue for next wake.
    async fn submit_ingress(
        &self,
        conversation_id: i64,
        app_slug: &str,
        source: &str,
        summary: &str,
        payload: &str,
        urgency: crate::messaging::Urgency,
    );
}

// ---------------------------------------------------------------------------
// Result enums (public API)
// ---------------------------------------------------------------------------

/// Result of `AutomationEngine::create`.
#[derive(Debug)]
pub enum CreateResult {
    Ok {
        /// Opaque job identifier (UUID string).
        id: String,
        /// Precomputed next fire time (RFC 3339 / ISO 8601 UTC).
        next_fire_at: chrono::DateTime<Utc>,
    },
    /// Caller's app has no `[app.messaging]` block with a sender.
    MissingSender,
    /// Caller's app `allowed_users` is empty — cannot determine owner user.
    OwnerHasNoUser,
    /// `name` field is empty or exceeds 128 bytes.
    InvalidName(String),
    /// Cron trigger is invalid (bad expression, bad timezone, never matches).
    InvalidCron(job::CronValidationError),
    /// Action `to`/`reply_to` address is malformed, outside the owner's publish
    /// (or, for `reply_to`, publish∪delivery) scope, or resolves to no channel.
    /// `kind` (`MalformedAddress` / `AclDenied` / `UnknownChannel`) selects the
    /// security-event kind; the LLM-visible message is unified so it discloses no
    /// channel-existence bit.
    InvalidAddress { addr: String, kind: DenialKind },
    /// Action `body` exceeds `max_body_bytes`.
    BodyTooLarge { len: usize, max: usize },
    /// `delivery_deadline_secs` is outside `[1, 2_592_000]`.
    InvalidDeadline(u32),
    /// App has reached the per-app job cap (`max_jobs_per_app`). Includes
    /// disabled jobs — delete a job to free a slot.
    TooManyJobs { count: u32, max: u32 },
}

/// Result of `AutomationEngine::edit`.
#[derive(Debug)]
pub enum EditResult {
    Ok {
        next_fire_at: chrono::DateTime<Utc>,
    },
    /// No job with the given id visible to the caller.
    NotFound,
    /// Caller's app is not the job's owner.
    Forbidden {
        reason: String,
    },
    /// Same field errors as `CreateResult`.
    InvalidName(String),
    InvalidCron(job::CronValidationError),
    InvalidAddress {
        addr: String,
        kind: DenialKind,
    },
    BodyTooLarge {
        len: usize,
        max: usize,
    },
    InvalidDeadline(u32),
    /// Owner lost the `MessagingPublish` grant since the job was authored.
    Unauthorized(String),
}

/// Result of `AutomationEngine::delete`.
#[derive(Debug)]
pub enum DeleteResult {
    Ok,
    NotFound,
    Forbidden { reason: String },
}

/// Result of `AutomationEngine::list`.
#[derive(Debug)]
pub enum ListResult {
    Ok(Vec<JobView>),
}

// ---------------------------------------------------------------------------
// AutomationEngine
// ---------------------------------------------------------------------------

/// The automation engine. Held on `AppState` as `Arc<AutomationEngine>`.
///
/// Constructed once at startup; background loop spawned separately (see
/// `loop_task` — future increment).
pub struct AutomationEngine {
    pub(crate) db: Db,
    pub(crate) messenger: Arc<Messenger>,
    pub(crate) apps: Arc<IndexMap<String, AppConfig>>,
    pub(crate) directory: Arc<MessagingDirectory>,
    /// Used by fire/error_route to submit ingress error reports.
    pub(crate) ingress_router: Arc<dyn IngressRouter>,
    /// Configuration defaults: rate limits, failure thresholds.
    pub(crate) defaults: AutomationGlobalConfig,
    /// Kick the scheduler loop when a create/edit/delete changes what is due.
    pub(crate) kick: Arc<Notify>,
    pub(crate) alerts: AlertDispatcher,
}

impl AutomationEngine {
    /// Construct an `AutomationEngine`. The caller owns the `Arc` for sharing
    /// with the background loop task.
    pub fn new(
        db: Db,
        messenger: Arc<Messenger>,
        apps: Arc<IndexMap<String, AppConfig>>,
        directory: Arc<MessagingDirectory>,
        ingress_router: Arc<dyn IngressRouter>,
        defaults: AutomationGlobalConfig,
        alerts: AlertDispatcher,
    ) -> Arc<Self> {
        Arc::new(Self {
            db,
            messenger,
            apps,
            directory,
            ingress_router,
            defaults,
            kick: Arc::new(Notify::new()),
            alerts,
        })
    }

    pub fn kick(&self) -> Arc<Notify> {
        self.kick.clone()
    }

    // -----------------------------------------------------------------------
    // MCP tool handlers
    // -----------------------------------------------------------------------

    /// Create a new automation job on behalf of `caller_app_slug`.
    pub async fn create(&self, caller_app_slug: &str, req: CreateJob) -> CreateResult {
        let now = Utc::now();

        // 1. Caller app config + publish-grant check. The publish/subscribe
        // split (design §2.5 site 3): create gates on the `MessagingPublish`
        // grant specifically, NOT `messaging_enabled()`'s `OR` — a
        // `messaging_subscribe`-only app cannot author a SendMessage job it
        // could never fire (Seam A/B both gate on the publish grant). The
        // per-channel `brenn_publish` ACL scope is checked in step 5
        // (`validate_action`), and re-runs authoritatively at fire time (Seam B,
        // §2.3).
        let app_config = match self.apps.get(caller_app_slug) {
            Some(c) => c,
            // Unknown slug: no app entry — publish disabled by definition.
            None => return CreateResult::MissingSender,
        };
        if !app_config
            .policy
            .has_grant(crate::access::AppCapability::MessagingPublish)
        {
            return CreateResult::MissingSender;
        }

        // 2. Owner user check.
        if app_config.allowed_users.is_empty() {
            return CreateResult::OwnerHasNoUser;
        }

        // 3. Name validation.
        if let Err(e) = validate_name(&req.name) {
            return CreateResult::InvalidName(e);
        }

        // 4. Trigger validation.
        let (cron_expr, tz) = match &req.trigger {
            Trigger::Cron(ct) => match job::validate_cron_trigger(ct, now) {
                Ok(v) => v,
                Err(e) => return CreateResult::InvalidCron(e),
            },
        };
        let next_fire_at = match &req.trigger {
            Trigger::Cron(ct) => {
                // Use compute_next (not raw find_next_occurrence) so DST
                // spring-forward gap-snap correction is applied consistently
                // with the fire loop (correctness-1).
                match job::compute_next(ct, now) {
                    Some(dt) => dt,
                    None => {
                        return CreateResult::InvalidCron(job::CronValidationError::NeverMatches);
                    }
                }
            }
        };
        // Suppress unused-variable warning; cron_expr and tz were used only
        // for validation above.
        let _ = (cron_expr, tz);

        // 5. Action validation. The owner's `MessagingPublish` grant was
        //    confirmed at step 1, so the per-address ACL scope check here reduces
        //    to the layer-2 matcher: `to` must be covered by the owner's
        //    `brenn_publish` allowlist and `reply_to` by its publish∪delivery
        //    visibility scope, checked BEFORE resolution so an out-of-scope
        //    address discloses no channel-existence bit. The same per-channel ACL
        //    re-runs authoritatively at fire time (Seam B).
        let max_body_bytes = self.messenger.defaults.max_body_bytes;
        if let Err(e) = validate_action(
            &req.action,
            &app_config.policy,
            &self.directory,
            max_body_bytes,
        ) {
            return e.into_create();
        }

        // 6. Count check + INSERT — both under the same lock to prevent TOCTOU
        // races where two concurrent creates both see count < max and both insert.
        let id = Uuid::new_v4();
        let trigger_kind = req.trigger.kind_str();
        let trigger_payload =
            serde_json::to_string(&req.trigger).expect("Trigger serialization never fails");
        let action_kind = req.action.kind_str();
        let action_payload =
            serde_json::to_string(&req.action).expect("Action serialization never fails");

        let conn = self.db.lock().await;
        let count = db::count_jobs_for_app(&conn, caller_app_slug);
        if count >= self.defaults.max_jobs_per_app {
            let max = self.defaults.max_jobs_per_app;
            tracing::warn!(
                owner_app_slug = %caller_app_slug,
                count,
                max,
                "job cap reached: create rejected"
            );
            return CreateResult::TooManyJobs { count, max };
        }
        db::insert_job(
            &conn,
            id,
            caller_app_slug,
            &req.name,
            trigger_kind,
            &trigger_payload,
            action_kind,
            &action_payload,
            req.enabled,
            now,
            next_fire_at,
        );
        drop(conn);

        // 7. Kick the loop.
        self.kick.notify_one();

        CreateResult::Ok {
            id: id.to_string(),
            next_fire_at,
        }
    }

    /// Edit an existing job. `caller_app_slug` must be the job's owner.
    pub async fn edit(&self, caller_app_slug: &str, req: EditJob) -> EditResult {
        let now = Utc::now();

        // Parse the UUID.
        let id_uuid = match req.id.parse::<Uuid>() {
            Ok(u) => u,
            Err(_) => return EditResult::NotFound,
        };

        // Acquire the lock briefly to load the job snapshot, then release it
        // before any non-DB work (alerting, validation) to avoid holding the
        // SQLite lock across an unrelated mutex inside AlertDispatcher
        // (correctness-7).
        let existing = {
            let conn = self.db.lock().await;
            match db::get_job(&conn, id_uuid) {
                Some(j) => j,
                None => return EditResult::NotFound,
            }
        };

        // Ownership check.
        if existing.owner_app_slug != caller_app_slug {
            tracing::warn!(
                caller_app = %caller_app_slug,
                owner_app = %existing.owner_app_slug,
                job_id = %id_uuid,
                "automation cross-app attempt (edit)"
            );
            self.alerts.alert_once_per_process(
                crate::obs::alerting::AlertSeverity::Warning,
                "Claude-Code anomaly: automation cross-app attempt".to_string(),
                &format!(
                    "automation_cross_app:{}:{}",
                    caller_app_slug, existing.owner_app_slug
                ),
                format!(
                    "caller_app={} attempted to edit job {} owned by {}",
                    caller_app_slug, id_uuid, existing.owner_app_slug
                ),
            );
            return EditResult::Forbidden {
                reason: "caller is not the job owner".to_string(),
            };
        }

        // Caller app config + messaging sender (owner might have lost sender).
        let app_config = match self.apps.get(caller_app_slug) {
            Some(c) => c,
            None => {
                return EditResult::Forbidden {
                    reason: "app no longer exists".to_string(),
                };
            }
        };
        if app_config.allowed_users.is_empty() {
            return EditResult::Forbidden {
                reason: "app has no allowed_users".to_string(),
            };
        }

        // Compute new trigger, action from the edit request (fall back to existing).
        let new_trigger = match &req.trigger {
            Some(t) => t.clone(),
            None => existing.trigger.clone(),
        };
        let new_action = match &req.action {
            Some(a) => a.clone(),
            None => existing.action.clone(),
        };
        let new_name = req.name.as_deref().unwrap_or(&existing.name);
        let new_enabled = req.enabled.unwrap_or(existing.enabled);

        // Validate name if changed.
        if req.name.is_some()
            && let Err(e) = validate_name(new_name)
        {
            return EditResult::InvalidName(e);
        }

        // Validate trigger if changed; recompute next_fire_at only when the
        // trigger actually changed (correctness-8: a name-only or enabled-only
        // edit must not silently reset the schedule).
        let next_fire_at = if req.trigger.is_some() {
            // Validate new trigger.
            match &new_trigger {
                Trigger::Cron(ct) => {
                    if let Err(e) = job::validate_cron_trigger(ct, now) {
                        return EditResult::InvalidCron(e);
                    }
                }
            }
            // Editing never retroactively fires missed slots — compute from
            // max(last_fired_at, now).  Use compute_next (not raw
            // find_next_occurrence) so DST spring-forward gap correction is
            // applied (correctness-1).
            let anchor = match existing.last_fired_at {
                Some(lf) => lf.max(now),
                None => now,
            };
            match &new_trigger {
                Trigger::Cron(ct) => match job::compute_next(ct, anchor) {
                    Some(dt) => dt,
                    None => {
                        return EditResult::InvalidCron(job::CronValidationError::NeverMatches);
                    }
                },
            }
        } else {
            // Trigger unchanged — preserve the existing next_fire_at so a
            // cosmetic or enabled-toggle edit doesn't silently cancel a
            // scheduled fire.
            existing.next_fire_at
        };

        // Publish grant pre-check, before per-address validation: a grant-absent
        // owner gets a distinct `Unauthorized` (grant state reveals only the
        // app's own policy, no namespace bit) rather than folding into the
        // unified address error below. Runs unconditionally — a name- or
        // enabled-only edit on an owner that lost the grant is still rejected.
        // The same grant re-runs authoritatively at fire time (Seam B).
        if !app_config
            .policy
            .has_grant(crate::access::AppCapability::MessagingPublish)
        {
            return EditResult::Unauthorized(format!(
                "app {caller_app_slug:?} holds no messaging_publish grant"
            ));
        }

        // Validate action if changed: shape → ACL scope → resolve per address,
        // with the grant already confirmed above so the scope check reduces to
        // the layer-2 matcher (`to`) / publish∪delivery visibility (`reply_to`).
        let max_body_bytes = self.messenger.defaults.max_body_bytes;
        if req.action.is_some()
            && let Err(e) = validate_action(
                &new_action,
                &app_config.policy,
                &self.directory,
                max_body_bytes,
            )
        {
            return e.into_edit();
        }

        // Apply in a single transaction.
        let trigger_payload =
            serde_json::to_string(&new_trigger).expect("Trigger serialization never fails");
        let action_payload =
            serde_json::to_string(&new_action).expect("Action serialization never fails");

        // When re-enabling a previously-disabled job, reset consecutive_failures
        // so the job gets a fresh chance (correctness-4).
        let reset_failure_counter = new_enabled && !existing.enabled;

        // Re-acquire the lock for the write (released after get_job to avoid
        // holding it across alerting — correctness-7).
        let conn = self.db.lock().await;
        let updated = db::update_job(
            &conn,
            id_uuid,
            new_name,
            &trigger_payload,
            &action_payload,
            new_enabled,
            now,
            next_fire_at,
            reset_failure_counter,
        );
        drop(conn);

        if !updated {
            // Job was deleted between the ownership check and this write.
            return EditResult::NotFound;
        }

        self.kick.notify_one();

        EditResult::Ok { next_fire_at }
    }

    /// Delete a job. `caller_app_slug` must be the job's owner.
    pub async fn delete(&self, caller_app_slug: &str, job_id: &str) -> DeleteResult {
        let id_uuid = match job_id.parse::<Uuid>() {
            Ok(u) => u,
            Err(_) => return DeleteResult::NotFound,
        };

        // Brief lock to load the snapshot; release before alerting to avoid
        // holding the SQLite lock across AlertDispatcher's internal mutex
        // (correctness-7).
        let existing = {
            let conn = self.db.lock().await;
            match db::get_job(&conn, id_uuid) {
                Some(j) => j,
                None => return DeleteResult::NotFound,
            }
        };

        if existing.owner_app_slug != caller_app_slug {
            tracing::warn!(
                caller_app = %caller_app_slug,
                owner_app = %existing.owner_app_slug,
                job_id = %id_uuid,
                "automation cross-app attempt (delete)"
            );
            self.alerts.alert_once_per_process(
                crate::obs::alerting::AlertSeverity::Warning,
                "Claude-Code anomaly: automation cross-app attempt".to_string(),
                &format!(
                    "automation_cross_app:{}:{}",
                    caller_app_slug, existing.owner_app_slug
                ),
                format!(
                    "caller_app={} attempted to delete job {} owned by {}",
                    caller_app_slug, id_uuid, existing.owner_app_slug
                ),
            );
            return DeleteResult::Forbidden {
                reason: "caller is not the job owner".to_string(),
            };
        }

        {
            let conn = self.db.lock().await;
            db::delete_job(&conn, id_uuid);
        }

        self.kick.notify_one();

        DeleteResult::Ok
    }

    /// List all jobs owned by `caller_app_slug`. Optional `enabled_only` filter.
    pub async fn list(&self, caller_app_slug: &str, enabled_only: bool) -> ListResult {
        let conn = self.db.lock().await;
        let jobs = db::list_jobs_by_owner(&conn, caller_app_slug, enabled_only);
        ListResult::Ok(jobs)
    }

    /// Return the earliest `next_fire_at` among all enabled jobs, if any.
    pub async fn earliest_enabled_next_fire(&self) -> Option<chrono::DateTime<Utc>> {
        let conn = self.db.lock().await;
        db::earliest_enabled_next_fire(&conn)
    }

    /// Load all enabled jobs whose `next_fire_at <= now`, oldest first.
    pub async fn get_due_jobs(&self) -> Vec<JobSnapshot> {
        let conn = self.db.lock().await;
        db::get_due_jobs(&conn, Utc::now())
    }
}

// ---------------------------------------------------------------------------
// Validation helpers (shared by create + edit paths)
// ---------------------------------------------------------------------------

fn validate_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("name must not be empty".to_string());
    }
    if name.len() > 128 {
        return Err(format!("name exceeds 128 bytes (got {})", name.len()));
    }
    Ok(())
}

/// Errors from `validate_action`.
enum ActionValidationError {
    InvalidAddress { addr: String, kind: DenialKind },
    BodyTooLarge { len: usize, max: usize },
    InvalidDeadline(u32),
}

impl ActionValidationError {
    fn into_create(self) -> CreateResult {
        match self {
            Self::InvalidAddress { addr, kind } => CreateResult::InvalidAddress { addr, kind },
            Self::BodyTooLarge { len, max } => CreateResult::BodyTooLarge { len, max },
            Self::InvalidDeadline(v) => CreateResult::InvalidDeadline(v),
        }
    }

    fn into_edit(self) -> EditResult {
        match self {
            Self::InvalidAddress { addr, kind } => EditResult::InvalidAddress { addr, kind },
            Self::BodyTooLarge { len, max } => EditResult::BodyTooLarge { len, max },
            Self::InvalidDeadline(v) => EditResult::InvalidDeadline(v),
        }
    }
}

/// Validate a `brenn:` publish target: shape → `brenn_publish` ACL scope →
/// directory resolution, in that order. The scope check runs BEFORE resolution
/// so an address outside the owner's allowlist fails identically whether or not
/// the channel exists, closing the create/edit-time existence oracle. `policy`
/// is the owner's; the grant is assumed already confirmed by the caller, so the
/// scope check reduces to the layer-2 matcher.
fn validate_publish_target(
    addr: &str,
    policy: &crate::access::AppPolicy,
    directory: &MessagingDirectory,
) -> Result<(), ActionValidationError> {
    let name = match crate::messaging::gates::well_formed_name(
        addr,
        crate::messaging::ChannelScheme::Brenn,
    ) {
        Some(n) => n,
        None => {
            return Err(ActionValidationError::InvalidAddress {
                addr: addr.to_string(),
                kind: DenialKind::MalformedAddress,
            });
        }
    };
    if !crate::messaging::gates::publish_acl_allows(
        policy,
        crate::messaging::ChannelScheme::Brenn,
        name,
    ) {
        return Err(ActionValidationError::InvalidAddress {
            addr: addr.to_string(),
            kind: DenialKind::AclDenied,
        });
    }
    if directory.resolve(addr).is_none() {
        return Err(ActionValidationError::InvalidAddress {
            addr: addr.to_string(),
            kind: DenialKind::UnknownChannel,
        });
    }
    Ok(())
}

/// Validate a `reply_to` address: shape → visibility scope → resolution. Same
/// order and oracle-closing rationale as `validate_publish_target`, but the
/// scope is the union of the owner's publish allowlist and its delivery scope
/// (a reply target is a channel the sender may name or may legitimately receive
/// deliveries on) — matching the `Messenger::publish` reply_to gate.
fn validate_reply_to(
    addr: &str,
    policy: &crate::access::AppPolicy,
    directory: &MessagingDirectory,
) -> Result<(), ActionValidationError> {
    let name = match crate::messaging::gates::well_formed_name(
        addr,
        crate::messaging::ChannelScheme::Brenn,
    ) {
        Some(n) => n,
        None => {
            return Err(ActionValidationError::InvalidAddress {
                addr: addr.to_string(),
                kind: DenialKind::MalformedAddress,
            });
        }
    };
    let visible = crate::messaging::gates::reply_to_visible(
        policy,
        crate::messaging::ChannelScheme::Brenn,
        name,
        addr,
    );
    if !visible {
        return Err(ActionValidationError::InvalidAddress {
            addr: addr.to_string(),
            kind: DenialKind::AclDenied,
        });
    }
    if directory.resolve(addr).is_none() {
        return Err(ActionValidationError::InvalidAddress {
            addr: addr.to_string(),
            kind: DenialKind::UnknownChannel,
        });
    }
    Ok(())
}

fn validate_action(
    action: &Action,
    policy: &crate::access::AppPolicy,
    directory: &MessagingDirectory,
    max_body_bytes: usize,
) -> Result<(), ActionValidationError> {
    match action {
        Action::SendMessage(sma) => {
            validate_publish_target(&sma.to, policy, directory)?;
            if let Some(rt) = &sma.reply_to {
                validate_reply_to(rt, policy, directory)?;
            }
            // Body size.
            if sma.body.len() > max_body_bytes {
                return Err(ActionValidationError::BodyTooLarge {
                    len: sma.body.len(),
                    max: max_body_bytes,
                });
            }
            // Delivery deadline bounds.
            if let Some(d) = sma.delivery_deadline_secs
                && !(1..=2_592_000).contains(&d)
            {
                return Err(ActionValidationError::InvalidDeadline(d));
            }
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// AutomationEngine create/edit/delete validation tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use uuid::Uuid;

    use super::*;
    use crate::automation::config::AutomationGlobalConfig;
    use crate::automation::job::{Action, CronTrigger, SendMessageAction, Trigger};
    use crate::automation::test_support::{FakeIngressRouter, FakeWakeRouter, make_engine_full};
    use crate::db::init_db_memory;
    use crate::messaging::{
        ChannelEntry, ChannelScheme, MessagingDirectory, MessagingGlobalConfig, Messenger,
        SubscriberEntry, SubscriberEntryKind, WakeMin, canonical_address,
        config::{Depth, NoiseLevel, ResolvedChannel, Sink},
    };
    use crate::obs::alerting::AlertDispatcher;

    fn make_engine_with_directory(directory: MessagingDirectory) -> Arc<AutomationEngine> {
        make_engine_full(
            init_db_memory(),
            directory,
            FakeIngressRouter::new(),
            Arc::new(FakeWakeRouter),
            AlertDispatcher::noop().0,
            AutomationGlobalConfig::default(),
            true,
        )
    }

    fn make_engine_with_config(
        directory: MessagingDirectory,
        cfg: AutomationGlobalConfig,
    ) -> Arc<AutomationEngine> {
        make_engine_full(
            init_db_memory(),
            directory,
            FakeIngressRouter::new(),
            Arc::new(FakeWakeRouter),
            AlertDispatcher::noop().0,
            cfg,
            true,
        )
    }

    /// Build a `MessagingDirectory` with a single channel `"test"` subscribed
    /// by `"test-app"` (the standard fixture for cap tests).
    fn test_channel_directory() -> MessagingDirectory {
        let channel_entry = ChannelEntry {
            uuid: Uuid::new_v4(),
            address: canonical_address("test"),
            description: None,
            resolved_channel: ResolvedChannel {
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers: vec![SubscriberEntry {
                kind: SubscriberEntryKind::App("test-app".to_string()),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: Some(WakeMin::Normal),
            }],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        };
        MessagingDirectory::with_entries(vec![channel_entry])
    }

    fn make_engine_with_no_messaging() -> Arc<AutomationEngine> {
        // App with no messaging sender — triggers MissingSender / Unauthorized.
        // Uses a custom slug and messaging=None, so cannot use default_app_cfg directly.
        let db = init_db_memory();
        let mut app_cfg = crate::automation::test_support::default_app_cfg("no-sender-app", true);
        app_cfg.messaging = None; // no sender → MissingSender / Unauthorized
        // Clear the messaging grants so this app is genuinely not a messaging
        // sender (messaging_enabled() returns false).
        app_cfg.policy = crate::access::AppPolicy::default();
        let mut apps = indexmap::IndexMap::new();
        apps.insert("no-sender-app".to_string(), app_cfg);
        let apps = Arc::new(apps);
        let directory_arc = Arc::new(MessagingDirectory::new());
        let messenger = Messenger::new(
            db.clone(),
            directory_arc.clone(),
            Arc::from("brenn://test"),
            apps.clone(),
            Arc::new(FakeWakeRouter),
            MessagingGlobalConfig::default(),
        );
        let (alerts, _) = AlertDispatcher::noop();
        AutomationEngine::new(
            db,
            messenger,
            apps,
            directory_arc,
            FakeIngressRouter::new(),
            AutomationGlobalConfig::default(),
            alerts,
        )
    }

    /// Build an engine whose `"test-app"` holds a *subscribe-only* policy:
    /// `messaging_enabled()` is still `true` (it grants `MessagingSubscribe`),
    /// but the publish-specific `MessagingPublish` grant is absent. Pins the
    /// publish/subscribe split (design §2.5 sites 3-4) at the create/edit gates.
    fn make_subscribe_only_engine() -> Arc<AutomationEngine> {
        let mut app_cfg = crate::automation::test_support::default_app_cfg("test-app", true);
        // Replace the default sender policy (which grants MessagingPublish) with
        // a subscribe-only one. `messaging_enabled()` reads the grant set, so it
        // is still true; only the publish grant is missing.
        app_cfg.policy = crate::access::AppPolicy::with_grants(&[
            crate::access::AppCapability::MessagingSubscribe,
        ]);
        let mut apps = indexmap::IndexMap::new();
        apps.insert("test-app".to_string(), app_cfg);
        // Reuse the canonical engine-wiring helper rather than re-inlining the
        // Messenger + AutomationEngine construction; the caller-supplied
        // `directory` is unused here (these create/edit-gate tests do not exercise
        // delivery), so `make_engine_with_apps`'s default (empty) directory
        // suffices.
        crate::automation::test_support::make_engine_with_apps(init_db_memory(), Arc::new(apps))
    }

    fn minimal_create_job(to: &str) -> CreateJob {
        CreateJob {
            name: "test-job".to_string(),
            trigger: Trigger::Cron(CronTrigger {
                expr: "*/5 * * * *".to_string(),
                tz: "UTC".to_string(),
                persistent: false,
            }),
            action: Action::SendMessage(SendMessageAction {
                to: to.to_string(),
                body: "hello".to_string(),
                urgency: crate::messaging::Urgency::Low,
                reply_to: None,
                delivery_deadline_secs: None,
            }),
            enabled: true,
        }
    }

    /// `validate_create_job`: unknown channel address → `InvalidAddress`.
    #[tokio::test]
    async fn validate_address_unknown_channel_rejected() {
        // Empty directory — no channels known.
        let engine = make_engine_with_directory(MessagingDirectory::with_entries(vec![]));
        let req = minimal_create_job("brenn:unknown-channel");
        let result = engine.create("test-app", req).await;
        assert!(
            matches!(result, CreateResult::InvalidAddress { .. }),
            "unknown channel must return InvalidAddress, got {result:?}"
        );
    }

    /// `validate_create_job`: name longer than 128 bytes → `InvalidName`.
    #[tokio::test]
    async fn validate_name_oversize_rejected() {
        let channel_uuid = Uuid::new_v4();
        let channel_entry = ChannelEntry {
            uuid: channel_uuid,
            address: canonical_address("test"),
            description: None,
            resolved_channel: ResolvedChannel {
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers: vec![SubscriberEntry {
                kind: SubscriberEntryKind::App("test-app".to_string()),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: Some(WakeMin::Normal),
            }],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        };
        let engine =
            make_engine_with_directory(MessagingDirectory::with_entries(vec![channel_entry]));

        let mut req = minimal_create_job("brenn:test");
        req.name = "a".repeat(129); // 129 bytes > 128 limit
        let result = engine.create("test-app", req).await;
        assert!(
            matches!(result, CreateResult::InvalidName(_)),
            "oversize name must return InvalidName, got {result:?}"
        );
    }

    /// `validate_create_job`: app has no messaging sender → `MissingSender`
    /// (owner authz failure at the sender-existence check).
    #[tokio::test]
    async fn validate_owner_authz_failure_rejected_at_create() {
        let engine = make_engine_with_no_messaging();
        let req = minimal_create_job("brenn:any");
        let result = engine.create("no-sender-app", req).await;
        assert!(
            matches!(result, CreateResult::MissingSender),
            "app with no messaging sender must return MissingSender, got {result:?}"
        );
    }

    /// Publish/subscribe split (design §2.5 sites 3-4): a `messaging_subscribe`-only
    /// app cannot **create** a SendMessage automation job. `messaging_enabled()` is
    /// still `true` (subscribe grant present), but the create gate now requires the
    /// `MessagingPublish` grant specifically, so it returns `MissingSender`.
    #[tokio::test]
    async fn create_denied_for_subscribe_only_app() {
        let engine = make_subscribe_only_engine();
        // Sanity: the app IS messaging-enabled (participation), only the publish
        // grant is missing — this is the split, not a blanket disable.
        assert!(
            engine.apps.get("test-app").unwrap().messaging_enabled(),
            "subscribe-only app must still read as messaging_enabled() (participation)"
        );
        let req = minimal_create_job("brenn:test");
        let result = engine.create("test-app", req).await;
        assert!(
            matches!(result, CreateResult::MissingSender),
            "subscribe-only app must not create a SendMessage job (split), got {result:?}"
        );
    }

    /// Publish/subscribe split (design §2.5 site 4): a `messaging_subscribe`-only
    /// app cannot **edit** a SendMessage automation job — `check_publish_auth` now
    /// gates on the `MessagingPublish` grant, so `edit` returns `Unauthorized`.
    /// The job is inserted directly (a subscribe-only app could never create one),
    /// modelling a policy tightened after the job was authored under an older grant.
    #[tokio::test]
    async fn edit_denied_for_subscribe_only_app() {
        let engine = make_subscribe_only_engine();
        // Insert a pre-existing job owned by "test-app" directly into the DB.
        let job_uuid = Uuid::new_v4();
        let trigger = Trigger::Cron(CronTrigger {
            expr: "*/5 * * * *".to_string(),
            tz: "UTC".to_string(),
            persistent: false,
        });
        let action = Action::SendMessage(SendMessageAction {
            to: "brenn:test".to_string(),
            body: "hello".to_string(),
            urgency: crate::messaging::Urgency::Low,
            reply_to: None,
            delivery_deadline_secs: None,
        });
        let now = chrono::Utc::now();
        {
            let conn = engine.db.lock().await;
            db::insert_job(
                &conn,
                job_uuid,
                "test-app",
                "preexisting-job",
                trigger.kind_str(),
                &serde_json::to_string(&trigger).unwrap(),
                action.kind_str(),
                &serde_json::to_string(&action).unwrap(),
                true,
                now,
                now,
            );
        }

        let result = engine
            .edit(
                "test-app",
                crate::automation::job::EditJob {
                    id: job_uuid.to_string(),
                    name: Some("renamed".to_string()),
                    trigger: None,
                    action: None,
                    enabled: None,
                },
            )
            .await;
        assert!(
            matches!(result, EditResult::Unauthorized(_)),
            "subscribe-only app must not edit a SendMessage job (split), got {result:?}"
        );
    }

    /// Create exactly `max_jobs_per_app` jobs → all succeed. Attempt one more
    /// → `TooManyJobs` with the correct `count` and `max`.
    #[tokio::test]
    async fn create_rejects_when_job_limit_reached() {
        let cfg = AutomationGlobalConfig {
            max_jobs_per_app: 3,
            ..AutomationGlobalConfig::default()
        };
        let engine = make_engine_with_config(test_channel_directory(), cfg);

        // Create exactly 3 jobs — each must succeed.
        for i in 0..3u32 {
            let mut req = minimal_create_job("brenn:test");
            req.name = format!("job-{i}");
            let result = engine.create("test-app", req).await;
            assert!(
                matches!(result, CreateResult::Ok { .. }),
                "job {i}: expected Ok, got {result:?}"
            );
        }

        // Fourth create must fail with TooManyJobs.
        let mut req = minimal_create_job("brenn:test");
        req.name = "job-over-limit".to_string();
        let result = engine.create("test-app", req).await;
        assert!(
            matches!(result, CreateResult::TooManyJobs { count: 3, max: 3 }),
            "expected TooManyJobs {{ count: 3, max: 3 }}, got {result:?}"
        );
    }

    /// Disabled jobs count toward the cap: fill to max-1, disable one, fill to
    /// max (succeeds), then attempt one more (fails).
    #[tokio::test]
    async fn create_counts_disabled_jobs_toward_limit() {
        let cfg = AutomationGlobalConfig {
            max_jobs_per_app: 3,
            ..AutomationGlobalConfig::default()
        };
        let engine = make_engine_with_config(test_channel_directory(), cfg);

        // Create 2 jobs (max-1).
        let mut first_id = String::new();
        for i in 0..2u32 {
            let mut req = minimal_create_job("brenn:test");
            req.name = format!("job-{i}");
            let result = engine.create("test-app", req).await;
            match result {
                CreateResult::Ok { id, .. } => {
                    if i == 0 {
                        first_id = id;
                    }
                }
                other => panic!("job {i}: expected Ok, got {other:?}"),
            }
        }

        // Disable first job via edit — it still counts toward cap.
        let edit_result = engine
            .edit(
                "test-app",
                crate::automation::job::EditJob {
                    id: first_id.clone(),
                    name: None,
                    trigger: None,
                    action: None,
                    enabled: Some(false),
                },
            )
            .await;
        assert!(
            matches!(edit_result, EditResult::Ok { .. }),
            "expected Ok from edit (disable), got {edit_result:?}"
        );

        // Create a 3rd job (count = 3 = max) — should succeed despite disabled job.
        let mut req = minimal_create_job("brenn:test");
        req.name = "job-fill".to_string();
        let result = engine.create("test-app", req).await;
        assert!(
            matches!(result, CreateResult::Ok { .. }),
            "3rd create (filling to max) must succeed, got {result:?}"
        );

        // Now at max. Another create must fail.
        let mut req = minimal_create_job("brenn:test");
        req.name = "job-over-limit".to_string();
        let result = engine.create("test-app", req).await;
        assert!(
            matches!(result, CreateResult::TooManyJobs { count: 3, max: 3 }),
            "expected TooManyJobs after reaching cap with disabled job counted, got {result:?}"
        );
    }

    /// Deleting a job at the cap frees a slot: fill to max, delete one, assert
    /// next create returns `CreateResult::Ok`.
    #[tokio::test]
    async fn create_after_delete_frees_slot() {
        let cfg = AutomationGlobalConfig {
            max_jobs_per_app: 2,
            ..AutomationGlobalConfig::default()
        };
        let engine = make_engine_with_config(test_channel_directory(), cfg);

        // Fill to cap.
        let mut first_id = String::new();
        for i in 0..2u32 {
            let mut req = minimal_create_job("brenn:test");
            req.name = format!("job-{i}");
            let result = engine.create("test-app", req).await;
            match result {
                CreateResult::Ok { id, .. } => {
                    if i == 0 {
                        first_id = id;
                    }
                }
                other => panic!("job {i}: expected Ok, got {other:?}"),
            }
        }

        // Confirm cap is hit.
        let over = {
            let mut req = minimal_create_job("brenn:test");
            req.name = "over-limit".to_string();
            engine.create("test-app", req).await
        };
        assert!(
            matches!(over, CreateResult::TooManyJobs { count: 2, max: 2 }),
            "expected TooManyJobs at cap, got {over:?}"
        );

        // Delete one job — row is removed, count drops.
        let del = engine.delete("test-app", &first_id).await;
        assert!(
            matches!(del, DeleteResult::Ok),
            "expected Ok from delete, got {del:?}"
        );

        // Slot is now free; next create must succeed.
        let mut req = minimal_create_job("brenn:test");
        req.name = "after-delete".to_string();
        let result = engine.create("test-app", req).await;
        assert!(
            matches!(result, CreateResult::Ok { .. }),
            "create after delete must succeed, got {result:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Create/edit publish-ACL scope gate + create-time existence oracle
    // -----------------------------------------------------------------------

    /// Build a directory with the named `brenn:` channels (no subscribers — the
    /// create/edit gate tests never resolve push targets).
    fn brenn_directory(names: &[&str]) -> MessagingDirectory {
        let entries = names
            .iter()
            .map(|n| crate::messaging::testutils::test_channel_entry(n, vec![]))
            .collect();
        MessagingDirectory::with_entries(entries)
    }

    /// Engine whose `"test-app"` holds `policy`, over `directory`. Subscriptions
    /// are empty (gate tests never fire), so any `policy`/`directory` pair is
    /// valid without the `resolve_push_targets` invariant applying.
    fn make_engine_with_policy(
        policy: crate::access::AppPolicy,
        directory: MessagingDirectory,
    ) -> Arc<AutomationEngine> {
        let db = init_db_memory();
        let mut app_cfg = crate::automation::test_support::default_app_cfg("test-app", true);
        app_cfg.policy = policy;
        let mut apps = indexmap::IndexMap::new();
        apps.insert("test-app".to_string(), app_cfg);
        let apps = Arc::new(apps);
        let directory = Arc::new(directory);
        let messenger = Messenger::new(
            db.clone(),
            directory.clone(),
            Arc::from("brenn://test"),
            apps.clone(),
            Arc::new(FakeWakeRouter),
            MessagingGlobalConfig::default(),
        );
        let (alerts, _) = AlertDispatcher::noop();
        AutomationEngine::new(
            db,
            messenger,
            apps,
            directory,
            FakeIngressRouter::new(),
            AutomationGlobalConfig::default(),
            alerts,
        )
    }

    /// `MessagingPublish` grant + a `brenn_publish` matcher for exactly
    /// `"allowed"` — every other channel is out of publish scope.
    fn restricted_publish_policy() -> crate::access::AppPolicy {
        let mut p = crate::access::AppPolicy::with_grants(&[
            crate::access::AppCapability::MessagingPublish,
        ]);
        p.acls
            .brenn_publish
            .push(crate::access::acl::ChannelMatcher::Exact(
                "allowed".to_string(),
            ));
        p
    }

    fn create_invalid_kind(r: &CreateResult) -> Option<DenialKind> {
        match r {
            CreateResult::InvalidAddress { kind, .. } => Some(*kind),
            _ => None,
        }
    }

    fn edit_invalid_kind(r: &EditResult) -> Option<DenialKind> {
        match r {
            EditResult::InvalidAddress { kind, .. } => Some(*kind),
            _ => None,
        }
    }

    fn create_job_with_reply_to(to: &str, reply_to: &str) -> CreateJob {
        let mut j = minimal_create_job(to);
        let Action::SendMessage(sma) = &mut j.action;
        sma.reply_to = Some(reply_to.to_string());
        j
    }

    /// Insert a valid job (owned by `"test-app"`, `to="brenn:allowed"`) directly,
    /// bypassing the create gate, so an edit can be exercised against it.
    async fn insert_allowed_job(engine: &AutomationEngine) -> Uuid {
        let job_uuid = Uuid::new_v4();
        let trigger = Trigger::Cron(CronTrigger {
            expr: "*/5 * * * *".to_string(),
            tz: "UTC".to_string(),
            persistent: false,
        });
        let action = Action::SendMessage(SendMessageAction {
            to: "brenn:allowed".to_string(),
            body: "hi".to_string(),
            urgency: crate::messaging::Urgency::Low,
            reply_to: None,
            delivery_deadline_secs: None,
        });
        let now = chrono::Utc::now();
        let conn = engine.db.lock().await;
        db::insert_job(
            &conn,
            job_uuid,
            "test-app",
            "seed-job",
            trigger.kind_str(),
            &serde_json::to_string(&trigger).unwrap(),
            action.kind_str(),
            &serde_json::to_string(&action).unwrap(),
            true,
            now,
            now,
        );
        job_uuid
    }

    /// Vector-3 oracle: an out-of-ACL `to` returns `acl_denied` at create whether
    /// or not the channel exists — the ACL gate runs before resolution, so the
    /// unified LLM-visible reject (built from the address alone) is byte-identical
    /// and leaks no channel-existence bit.
    #[tokio::test]
    async fn create_to_out_of_acl_is_acl_denied_regardless_of_existence() {
        let exists = make_engine_with_policy(
            restricted_publish_policy(),
            brenn_directory(&["allowed", "secret"]),
        );
        let r_exists = exists
            .create("test-app", minimal_create_job("brenn:secret"))
            .await;
        let absent =
            make_engine_with_policy(restricted_publish_policy(), brenn_directory(&["allowed"]));
        let r_absent = absent
            .create("test-app", minimal_create_job("brenn:secret"))
            .await;
        assert_eq!(
            create_invalid_kind(&r_exists),
            Some(DenialKind::AclDenied),
            "{r_exists:?}"
        );
        assert_eq!(
            create_invalid_kind(&r_absent),
            Some(DenialKind::AclDenied),
            "{r_absent:?}"
        );
    }

    /// An in-ACL `to` still distinguishes existence (Ok vs `unknown_channel`) —
    /// legitimate: an app may probe channels inside its own publish allowlist.
    #[tokio::test]
    async fn create_to_in_acl_resolves_within_own_scope() {
        let ok =
            make_engine_with_policy(restricted_publish_policy(), brenn_directory(&["allowed"]));
        assert!(matches!(
            ok.create("test-app", minimal_create_job("brenn:allowed"))
                .await,
            CreateResult::Ok { .. }
        ));
        let missing = make_engine_with_policy(restricted_publish_policy(), brenn_directory(&[]));
        let r = missing
            .create("test-app", minimal_create_job("brenn:allowed"))
            .await;
        assert_eq!(
            create_invalid_kind(&r),
            Some(DenialKind::UnknownChannel),
            "{r:?}"
        );
    }

    /// Vector-3 oracle for `reply_to`: an out-of-visibility `reply_to` returns
    /// `acl_denied` whether or not the channel exists. `to` is in scope so the
    /// reply_to gate is reached.
    #[tokio::test]
    async fn create_reply_to_out_of_visibility_is_acl_denied_regardless_of_existence() {
        let exists = make_engine_with_policy(
            restricted_publish_policy(),
            brenn_directory(&["allowed", "secret"]),
        );
        let r_exists = exists
            .create(
                "test-app",
                create_job_with_reply_to("brenn:allowed", "brenn:secret"),
            )
            .await;
        let absent =
            make_engine_with_policy(restricted_publish_policy(), brenn_directory(&["allowed"]));
        let r_absent = absent
            .create(
                "test-app",
                create_job_with_reply_to("brenn:allowed", "brenn:secret"),
            )
            .await;
        assert_eq!(
            create_invalid_kind(&r_exists),
            Some(DenialKind::AclDenied),
            "{r_exists:?}"
        );
        assert_eq!(
            create_invalid_kind(&r_absent),
            Some(DenialKind::AclDenied),
            "{r_absent:?}"
        );
    }

    /// A `reply_to` reachable through the delivery scope (subscribe grant +
    /// matcher) is in visibility even though it is not a publish target — the
    /// union arm of the visibility check.
    #[tokio::test]
    async fn create_reply_to_within_delivery_scope_succeeds() {
        let mut policy = restricted_publish_policy();
        policy
            .grants
            .insert(crate::access::AppCapability::MessagingSubscribe);
        policy
            .acls
            .brenn_subscribe
            .push(crate::access::acl::ChannelMatcher::Exact(
                "replies".to_string(),
            ));
        let engine = make_engine_with_policy(policy, brenn_directory(&["allowed", "replies"]));
        let r = engine
            .create(
                "test-app",
                create_job_with_reply_to("brenn:allowed", "brenn:replies"),
            )
            .await;
        assert!(
            matches!(r, CreateResult::Ok { .. }),
            "reply_to in delivery scope must succeed, got {r:?}"
        );
    }

    /// Edit path, vector-3 oracle: editing a job's action `to` to an out-of-ACL
    /// address returns `acl_denied` regardless of the channel's existence.
    #[tokio::test]
    async fn edit_to_out_of_acl_is_acl_denied_regardless_of_existence() {
        let new_action = |to: &str| {
            Some(Action::SendMessage(SendMessageAction {
                to: to.to_string(),
                body: "hi".to_string(),
                urgency: crate::messaging::Urgency::Low,
                reply_to: None,
                delivery_deadline_secs: None,
            }))
        };

        let exists = make_engine_with_policy(
            restricted_publish_policy(),
            brenn_directory(&["allowed", "secret"]),
        );
        let id_exists = insert_allowed_job(&exists).await;
        let r_exists = exists
            .edit(
                "test-app",
                EditJob {
                    id: id_exists.to_string(),
                    name: None,
                    trigger: None,
                    action: new_action("brenn:secret"),
                    enabled: None,
                },
            )
            .await;

        let absent =
            make_engine_with_policy(restricted_publish_policy(), brenn_directory(&["allowed"]));
        let id_absent = insert_allowed_job(&absent).await;
        let r_absent = absent
            .edit(
                "test-app",
                EditJob {
                    id: id_absent.to_string(),
                    name: None,
                    trigger: None,
                    action: new_action("brenn:secret"),
                    enabled: None,
                },
            )
            .await;

        assert_eq!(
            edit_invalid_kind(&r_exists),
            Some(DenialKind::AclDenied),
            "{r_exists:?}"
        );
        assert_eq!(
            edit_invalid_kind(&r_absent),
            Some(DenialKind::AclDenied),
            "{r_absent:?}"
        );
    }

    /// A malformed `to` is tagged `malformed_address` (distinct signal kind) — it
    /// still renders through the same unified LLM-visible reject.
    #[tokio::test]
    async fn create_malformed_to_is_malformed_kind() {
        let engine =
            make_engine_with_policy(restricted_publish_policy(), brenn_directory(&["allowed"]));
        let r = engine
            .create("test-app", minimal_create_job("not-a-brenn-address"))
            .await;
        assert_eq!(
            create_invalid_kind(&r),
            Some(DenialKind::MalformedAddress),
            "{r:?}"
        );
    }
}
