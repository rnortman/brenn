//! `Messenger::publish` and the dispatch path that fires after commit.
//!
//! See `docs/designs/messaging-mvp.md` §7 for the full sequence.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::token_bucket::TokenBucketOutcome;

/// Burst capacity of one surface principal's send budget: publishes admitted
/// before its rate limit engages. The budget is process-lifetime and keyed by
/// principal, so a client looping connect → burst → disconnect does not refresh
/// it.
///
/// Equal to `brenn_budget::MAX_PUBLISHES_PER_ACTIVATION`, so a full bucket
/// admits exactly one maximal conforming activation flush. That constraint — not
/// the number — is the contract: this bucket is a backstop drawn in
/// whole-publish units against a flush's entries, and a backstop sized below the
/// flush it backstops would refuse truthful traffic. Boot asserts it (see
/// `resolve_send_budget` in the server's surface bootstrap). Sustained
/// throughput is governed by [`SURFACE_SEND_REFILL`], which is the knob that
/// means "rate".
pub const SURFACE_SEND_BURST: u32 = 256;

/// The default's half of the sizing invariant, at compile time.
///
/// Boot asserts every *resolved* burst, which covers this one too — but the
/// default is the value every surface gets without stating anything, including
/// the kernel grain, which has no override knob to state. A default that
/// violates the invariant should not compile, let alone reach a boot.
const _: () = assert!(
    SURFACE_SEND_BURST as usize >= brenn_budget::MAX_PUBLISHES_PER_ACTIVATION,
    "SURFACE_SEND_BURST must cover a maximal conforming activation flush \
     (MAX_PUBLISHES_PER_ACTIVATION)"
);

/// One durable-send token refilled per this interval, per surface principal
/// (steady-state 4/min) — far above any legitimate sustained rate while
/// bounding an attacker.
///
/// The surface's bare identity (no `[[surface.component]]` override) runs at
/// this rate. An operator sizing `status_interval_secs` is therefore sizing
/// against this refill; a cadence faster than it outruns the budget once the
/// burst is spent.
pub const SURFACE_SEND_REFILL: Duration = Duration::from_secs(15);

use super::db::{
    self, BudgetDecrement, InsertedMessage, decrement_send_budget, insert_ingress_message,
    insert_message_in_tx, refund_send_budget,
};
use super::gates::{
    check_body_size, publish_acl_allows, reply_to_visible, resolve_publish_sender, well_formed_name,
};
use super::store::SurfaceFeedTarget;
use super::{
    ChannelEntry, ChannelScheme, Impetus, Messenger, ParticipantId, PrepaidDestination,
    PrepaidEntry, SubscriberEntryKind, Urgency, store,
};
use crate::access::AppCapability;
use brenn_common::{MAX_LOGGED_UNTRUSTED_BYTES, sanitize_untrusted_str};

use crate::obs::security::{DenialKind, SecurityEventType, log_component_security_event};

/// Per-channel memoized resolution for a batch flush: the channel entry and its
/// live surface-feed targets.
type ResolvedChannelTargets = (Arc<super::ChannelEntry>, Vec<SurfaceFeedTarget>);

/// Outcome of a publish, on any pub/sub scheme. Maps directly to the success /
/// failure JSON returned to CC by the `MessageSend` PostToolUse intercept.
///
/// `MalformedAddress` covers shape errors on either `to` or `reply_to` (missing
/// or unrecognized scheme prefix, disallowed characters). `UnknownChannel`
/// covers well-formed addresses that don't resolve to a registered channel, for
/// either `to` or `reply_to`.
#[derive(Debug)]
pub enum PublishResult {
    Ok {
        message_id: Uuid,
        address: String,
        /// `Some` for a `Conversation` origin (remaining per-conversation send
        /// budget after this publish); `None` for a `System` origin, which has
        /// no send budget.
        remaining_budget: Option<u32>,
    },
    /// Budget exhausted; no message was inserted.
    BudgetExhausted,
    /// Channel address didn't resolve to a registered channel. Carries
    /// the address that failed (`to` or `reply_to`).
    UnknownChannel(String),
    /// Address didn't pass shape validation: missing `brenn:` prefix,
    /// disallowed characters, or otherwise malformed. Carries the
    /// offending string.
    MalformedAddress(String),
    /// Sender app holds no `MessagingPublish` grant (publish/subscribe split):
    /// the publish path gates on `MessagingPublish` specifically, not the
    /// participation `OR`. A `messaging_subscribe`-only app is `MissingSender`
    /// here. This is a layer-1 (grant) absence — distinct from `AclDenied`,
    /// which is a layer-2 (ACL scope) denial with the grant held.
    MissingSender,
    /// Sender app holds `MessagingPublish` but the target `brenn:` channel is not
    /// covered by any `brenn_publish` ACL matcher (layer-2 deny).
    /// Distinct from `MissingSender` (layer-1 grant absence) so the LLM-facing
    /// error and automation-outcome class can name the *allowlist*, not the
    /// grant. Carries the offending address (`brenn:<channel>`). Budget is not
    /// consumed.
    AclDenied(String),
    /// Body length > `max_body_bytes`. Budget is not consumed.
    BodyTooLarge { len: usize, max: usize },
    /// The per-`(sender, channel)` send-rate gate refused this publish. No
    /// message was published and no budget was consumed.
    RateLimited,
    /// A deferred (`deliver_after`) publish was refused because the channel's
    /// deferred set is at its channel-wide cap (`retain_depth`). Refusing new
    /// work rather than silently cancelling already-scheduled work; no message
    /// was parked and no budget was consumed.
    DeferredQuotaExceeded { cap: u64 },
    /// The publish carried `impetus` from a principal whose policy lacks
    /// `MintImpetus`. Refused whole — never stripped and accepted: a publish
    /// claiming authority it does not hold is a wrong thing, not a field to
    /// quietly drop. Nothing was stored, no rate token and no budget were
    /// consumed.
    ImpetusUnauthorized,
}

impl PublishResult {
    /// Kind tag for every denial that warrants an intercept-level security
    /// signal, and the key of the per-`(sender, kind)` denied-publish counter.
    /// A caller that signals denials derives the log `kind` field from here.
    ///
    /// `Ok`, `BudgetExhausted`, `RateLimited`, and `DeferredQuotaExceeded`
    /// return `None`: the limit arms are normal operational conditions with
    /// their own counters and LLM-facing recovery paths, not policy denials.
    pub fn signal_kind(&self) -> Option<DenialKind> {
        match self {
            Self::MalformedAddress(_) => Some(DenialKind::MalformedAddress),
            Self::UnknownChannel(_) => Some(DenialKind::UnknownChannel),
            Self::MissingSender => Some(DenialKind::MissingSender),
            Self::AclDenied(_) => Some(DenialKind::AclDenied),
            Self::BodyTooLarge { .. } => Some(DenialKind::BodyTooLarge),
            Self::ImpetusUnauthorized => Some(DenialKind::ImpetusUnauthorized),
            Self::Ok { .. }
            | Self::BudgetExhausted
            | Self::RateLimited
            | Self::DeferredQuotaExceeded { .. } => None,
        }
    }

    /// The echoed target address an address-bearing denial arm carries.
    /// `MissingSender` and `BodyTooLarge` carry none; a caller substitutes the
    /// original publish target.
    pub fn denied_address(&self) -> Option<&str> {
        match self {
            Self::MalformedAddress(addr) | Self::UnknownChannel(addr) | Self::AclDenied(addr) => {
                Some(addr)
            }
            _ => None,
        }
    }
}

/// One draw against a surface principal's send budget.
///
/// The fields the bucket needs (`slug`/`component`/`tokens`) travel with the
/// ones only its transition warns need (`principal`/`channel`), because a warn
/// with no principal on it is unactionable — the bucket is per-principal and the
/// operator's next question is always "which one".
pub struct SurfaceSendDraw<'a> {
    pub slug: &'a str,
    /// The identity grain: `Some(instance)` draws that instance's bucket, `None`
    /// the surface's own kernel bucket. Server-derived, never client-claimed.
    pub component: Option<&'a str>,
    /// The stamped principal string, for the warns only.
    pub principal: &'a str,
    /// The target address, when the draw has exactly one. A batch spans channels
    /// and passes `None` rather than naming an arbitrary member.
    pub channel: Option<&'a str>,
    /// Tokens this draw consumes, all or nothing: admission is sufficiency, so a
    /// draw the balance does not cover whole is refused and costs nothing — see
    /// [`crate::token_bucket`]. Boot's sizing invariant is what keeps a maximal
    /// conforming flush from ever being wider than the burst.
    pub tokens: u32,
}

/// Verdict of a [`SurfaceSendDraw`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SurfaceSendVerdict {
    /// Tokens were drawn; the caller proceeds.
    Admitted,
    /// The budget refused the draw. The caller answers its client a rate limit —
    /// never a violation and never a kill.
    Denied,
}

/// The principal a surface publish is stamped with: the sub-identity's when the
/// caller names one, the surface's own bare identity otherwise.
///
/// The two-grain key every surface-side gate shares — budget bucket, stored
/// sender, parked-set ownership — so a flush and a report by the same principal
/// are the same principal everywhere.
fn surface_principal(slug: &str, component: Option<&str>) -> ParticipantId {
    match component {
        Some(component) => ParticipantId::for_surface_component(slug, component),
        None => ParticipantId::for_surface(slug),
    }
}

/// Identifies the publisher for the send-budget gate.
#[derive(Debug, Clone, Copy)]
pub enum PublishOrigin {
    /// LLM/automation publish attributed to a conversation; the per-conversation
    /// send budget applies (decrement_send_budget).
    Conversation { id: i64 },
    /// Every publisher the per-conversation send budget does not price: the
    /// in-process system publishers, surfaces, and a conversation publishing its
    /// own chat record. No send budget — flood protection is the per-channel
    /// send rate plus whatever bounds the caller applies. `BudgetExhausted` is
    /// unrepresentable for this origin.
    System,
}

/// Sender-authority source for the shared `publish_core` gate sequence.
///
/// Selects where layer-1 (existence + `MessagingPublish` grant) resolves, which
/// principal the stored message carries, and — for a `Conversation` origin —
/// which per-conversation send budget applies. Every downstream gate (layer-2
/// ACL, body cap, reply_to resolution, insert, dispatch) is identical across
/// arms; only this resolution differs, so the app and surface publish entries
/// share one gate order rather than duplicating it.
#[derive(Clone, Copy)]
enum PublishPrincipal<'a> {
    /// App/automation publisher: layer-1 resolves against `self.apps`; principal
    /// is `for_app(slug)`; the `Conversation`-origin budget is the app's
    /// configured send budget.
    App { slug: &'a str },
    /// Surface (browser WASM) publisher: layer-1 resolves against the unified
    /// subscriber registry (surfaces are not in `self.apps`). Always paired with
    /// a `System` origin, so it reads no per-conversation budget; its durable
    /// flood bound is the surface send budget consulted in `publish_core`
    /// (`surface_send_budgets`), keyed by principal so it is reconnect-resistant.
    /// The per-connection publish bucket at the session gates first.
    ///
    /// `component` picks the identity grain, and is the **server's** answer, never
    /// the client's: `Some(instance)` — an instance the boot-resolved declaration
    /// set admits — stamps the sub-identity `surface:<slug>#<instance>` and draws
    /// that instance's bucket; `None` stamps the bare `surface:<slug>` kernel
    /// identity and draws the surface's own bucket. Layer-1 and layer-2 read the
    /// surface's policy in both cases: a component's grants *are* its
    /// config-declared bindings, which boot validation already proved are covered
    /// by the surface's ACLs, so there is no separate per-component policy to
    /// consult.
    ///
    /// `platform` distinguishes application publishes (`false` — bound content
    /// outputs and error reports, which draw the send budget) from
    /// platform-originated telemetry (`true` — server-constructed geometry/status
    /// documents and the boot/terminal status stamps, which skip *only* the
    /// send-budget step). A `true` publish passes every other gate — shape,
    /// directory, grant, ACL, body cap — unchanged; the exemption exists because a
    /// heartbeat-forever cadence would drain the budget by design and starve the
    /// surface's own error reports, the silent telemetry death the feature exists
    /// to eliminate. The bodies are server-constructed and size-bounded and the
    /// cadence is bounded elsewhere (shell debounce, the per-connection publish
    /// bucket, the fixed status interval, once-per-boot/close stamps).
    ///
    /// The exemption tracks *what is published*, not which grain published it:
    /// telemetry is exempt, and a kernel error report — bare identity, `platform:
    /// false` — is not, because its cadence is driven by whatever went wrong
    /// rather than by a server-side timer.
    Surface {
        slug: &'a str,
        component: Option<&'a str>,
        platform: bool,
    },
    /// System-substrate publisher (e.g. the tool executor delivering results):
    /// layer-1 resolves against `self.system_policies` (system components are
    /// not in `self.apps`); principal is `for_system(component)`. Always paired
    /// with a `System` origin — no send budget; the substrate self-limits at
    /// its admission point.
    System { component: &'a str },
    /// A conversation speaking for itself on its own chat channels — the record
    /// it writes and the token batches it streams. `app_slug` selects the owning
    /// app's **harness** policy (`AppConfig::chat_harness_policy`, derived and
    /// never authored); layer-1 and layer-2 both resolve against it. The app's
    /// authored policy is not consulted and does not widen or narrow this arm:
    /// the harness is a separate principal from the app's LLM.
    ///
    /// The stored principal is `for_conversation(id)`, distinct from the app's
    /// own `app:<slug>@<server>`, so the record shows which of the two spoke.
    /// Bus identity and authority are separate concepts here: the envelope says
    /// the conversation spoke, the gates read the harness policy.
    ///
    /// Always paired with a `System` origin: the per-conversation send budget
    /// prices an LLM's *tool* publishes, and chat output volume is not that —
    /// it is governed by the chat channels' own `send_rate`, which this arm
    /// draws like every other principal.
    Conversation { id: i64, app_slug: &'a str },
}

impl Messenger {
    /// Publish a message on behalf of a CC subprocess, on any pub/sub scheme.
    ///
    /// The `origin` and `sender_app_slug` identify the publisher (used for
    /// budget bookkeeping and sender-config lookup). A `Conversation` origin
    /// consumes the per-conversation send budget; a `System` origin has no
    /// budget. The message body, channel address, and option fields come from
    /// the LLM tool call.
    ///
    /// Thin wrapper over `publish_core` with the app-sender authority source;
    /// `publish_from_surface` is the sibling entry for surface publishers.
    #[allow(clippy::too_many_arguments)]
    pub async fn publish(
        &self,
        origin: PublishOrigin,
        sender_app_slug: &str,
        addr: &str,
        body: &str,
        urgency: super::Urgency,
        reply_to: Option<&str>,
        deliver_after: Option<DateTime<Utc>>,
        delivery_deadline: Option<DateTime<Utc>>,
    ) -> PublishResult {
        assert!(
            !matches!(origin, PublishOrigin::System),
            "Messenger::publish called with PublishOrigin::System — a system publish must go \
             through publish_from_system under a code-built policy, not the App arm. The \
             reserved-app pattern is deleted; this guards against its silent return."
        );
        self.publish_core(
            origin,
            PublishPrincipal::App {
                slug: sender_app_slug,
            },
            addr,
            body,
            urgency,
            reply_to,
            deliver_after,
            delivery_deadline,
            None,
        )
        .await
    }

    /// Publish a durable (`brenn:`) message on behalf of a surface (browser
    /// WASM) component.
    ///
    /// Runs the identical gate sequence as `publish` (`publish_core`) — the same
    /// address-shape, directory, `MessagingPublish` grant, `brenn_publish` ACL,
    /// and body-cap gates — differing only in the layer-1 authority source (the
    /// unified subscriber registry, keyed by boot-resolved surface slug) and the
    /// stored principal. `System` origin: no per-conversation send budget, but the
    /// durable surface send budget (`surface_send_budgets`) bounds it in
    /// `publish_core`, so `BudgetExhausted` is a client-reachable outcome here.
    /// Urgency is `Normal` in v1 (the surface `Publish` frame carries none). No
    /// `reply_to`/`delivery_deadline` — not exposed to surfaces. No
    /// `deliver_after` either: a surface's deferred publish is always a *buffered*
    /// component publish, and buffered publishes flush as batches, so surface
    /// deferral rides [`Messenger::publish_batch_from_surface`] and never this
    /// single-frame path.
    ///
    /// `component` is the identity grain, and both halves are backend-validated,
    /// never client-trusted fields: `Some(instance)` stamps
    /// `surface:<slug>#<instance>` and draws that instance's budget — the caller
    /// admitted `instance` against its own declaration set before naming it here;
    /// `None` stamps the bare `surface:<slug>` and draws the surface's own budget,
    /// for a publish the kernel itself made with no component subject (its own
    /// error reports).
    ///
    /// Because every reachable channel is an operator allowlist
    /// (`[[surface.output]]` binding + covering `publish_acl`, both boot-validated),
    /// `MissingSender`/`AclDenied`/`UnknownChannel`/`MalformedAddress` here are
    /// broken boot invariants — the session caller panics on them (see
    /// `handle_publish`); `Ok`/`BodyTooLarge`/`BudgetExhausted` are the
    /// client-reachable outcomes.
    pub async fn publish_from_surface(
        &self,
        slug: &str,
        component: Option<&str>,
        addr: &str,
        body: &str,
        urgency: super::Urgency,
    ) -> PublishResult {
        self.publish_core(
            PublishOrigin::System,
            PublishPrincipal::Surface {
                slug,
                component,
                platform: false,
            },
            addr,
            body,
            urgency,
            None,
            None,
            None,
            None,
        )
        .await
    }

    /// Publish a durable (`brenn:`) platform-telemetry document on behalf of a
    /// surface: the server-constructed geometry/status snapshots and the
    /// boot/terminal status stamps.
    ///
    /// Identical to [`publish_from_surface`] except that it is **exempt from the
    /// per-surface send budget** — every other gate (address shape, directory,
    /// `MessagingPublish` grant, `brenn_publish` ACL, body cap) applies unchanged.
    /// The exemption keeps a heartbeat-forever cadence from draining the budget
    /// that carries the surface's own error reports; it is safe because the bodies
    /// are server-constructed and size-bounded and the cadence is bounded
    /// elsewhere. With the exemption `BudgetExhausted` is unreachable here, so the
    /// caller panics on it as a broken invariant.
    ///
    /// Always the bare `surface:<slug>` kernel identity: the kernel is the party
    /// that observes the viewport and owns the mount/pump state these documents
    /// report, so there is no component whose behalf it could be acting on.
    pub async fn publish_from_surface_platform(
        &self,
        slug: &str,
        addr: &str,
        body: &str,
        urgency: super::Urgency,
    ) -> PublishResult {
        self.publish_core(
            PublishOrigin::System,
            PublishPrincipal::Surface {
                slug,
                component: None,
                platform: true,
            },
            addr,
            body,
            urgency,
            None,
            None,
            None,
            // Server-constructed telemetry: no user gesture behind it.
            None,
        )
        .await
    }

    /// Publish a durable (`brenn:`) message on behalf of an in-process
    /// system-substrate component (e.g. the tool executor publishing a result
    /// to a caller's `brenn:tool-results/<slug>` inbox).
    ///
    /// Runs the identical `publish_core` gate sequence as `publish` — same
    /// address-shape, directory, `MessagingPublish` grant, `brenn_publish` ACL,
    /// and body-cap gates — differing only in the layer-1 authority source
    /// (`system_policies`, keyed by component name) and the stored principal
    /// (`for_system(component)`). `System` origin: no send budget. There is no
    /// ACL bypass — a system component publishes only where its code-built
    /// policy authorizes, exactly like every other principal.
    pub async fn publish_from_system(
        &self,
        component: &str,
        addr: &str,
        body: &str,
        urgency: super::Urgency,
        reply_to: Option<&str>,
    ) -> PublishResult {
        self.publish_core(
            PublishOrigin::System,
            PublishPrincipal::System { component },
            addr,
            body,
            urgency,
            reply_to,
            None,
            None,
            // In-process substrate output; nothing here descends from a gesture.
            None,
        )
        .await
    }

    /// Publish on behalf of a conversation, on any pub/sub scheme — writing its
    /// conversation record (`brenn:`) and token stream (`ephemeral:`).
    ///
    /// No bypass: runs the identical gate sequence as every other entry
    /// (`publish_core`), reaching only what the owning app's derived harness
    /// policy authorizes — its own chat subtree and nothing else.
    ///
    /// `app_slug` selects the owning app's harness policy
    /// (`AppConfig::chat_harness_policy`); the app's authored policy plays no
    /// part. `conversation_id` is stamped as the sender (`conversation:<id>`).
    ///
    /// `System` origin: no per-conversation send budget, so `BudgetExhausted` is
    /// unreachable here. No `reply_to`/`deliver_after`/`delivery_deadline` — a
    /// conversation's own output is neither a reply nor scheduled.
    pub async fn publish_from_conversation(
        &self,
        conversation_id: i64,
        app_slug: &str,
        addr: &str,
        body: &str,
        urgency: super::Urgency,
    ) -> PublishResult {
        self.publish_core(
            PublishOrigin::System,
            PublishPrincipal::Conversation {
                id: conversation_id,
                app_slug,
            },
            addr,
            body,
            urgency,
            None,
            None,
            None,
            // The conversation's own record and stream. Impetus does not
            // propagate outward through the machinery's republish legs: an
            // attended turn's record must not re-arm every observer downstream.
            None,
        )
        .await
    }

    /// Draw against one surface principal's send budget — the defense-in-depth
    /// backstop on everything a surface republishes into the server's substrate.
    ///
    /// Keyed by principal, not connection: a component's retry loop drains its own
    /// instance's bucket and leaves its siblings and the kernel's own reports
    /// able to publish, and a reconnecting session inherits the drained bucket
    /// rather than refreshing it. The bucket's admission rule is sufficiency
    /// (`crate::token_bucket`): a draw the balance does not cover whole is
    /// refused and deducts nothing, and the balance never goes negative.
    ///
    /// `unit` names what the draw is buying, for the transition warns ("durable
    /// publishes", "activation batches") — the operator reads them to tell an
    /// erroring component from a flooding one.
    ///
    /// Panics if the principal has no bucket: boot installs one per surface and
    /// one per declared instance, so a miss is a broken boot invariant, and
    /// admitting an unbudgeted principal would be a silent hole in exactly the
    /// backstop this is.
    fn draw_surface_send_budget(
        &self,
        draw: SurfaceSendDraw<'_>,
        unit: &str,
    ) -> SurfaceSendVerdict {
        let SurfaceSendDraw {
            slug,
            component,
            principal,
            channel,
            tokens,
        } = draw;
        // Owned key: the map is keyed by principal grain, and probing it without
        // allocating would mean a parallel borrowed-key type for a lookup that
        // happens once per publish.
        let key = (slug.to_string(), component.map(str::to_string));
        let bucket = self.surface_send_budgets.get(&key).unwrap_or_else(|| {
            panic!(
                "draw_surface_send_budget: surface principal {principal:?} has no send budget — \
                 boot installs one per surface and one per declared component instance, so a miss \
                 is a broken boot invariant"
            )
        });
        match bucket
            .lock()
            .expect("surface send budget mutex poisoned")
            .try_consume_n(tokens)
        {
            TokenBucketOutcome::Granted => SurfaceSendVerdict::Admitted,
            TokenBucketOutcome::GrantedAfterSuppression { suppressed } => {
                warn!(
                    surface = %slug,
                    principal = %principal,
                    channel = channel.unwrap_or("<batch>"),
                    suppressed,
                    "surface send budget recovered; {unit} were suppressed"
                );
                SurfaceSendVerdict::Admitted
            }
            TokenBucketOutcome::Denied { first } => {
                if first {
                    warn!(
                        surface = %slug,
                        principal = %principal,
                        channel = channel.unwrap_or("<batch>"),
                        tokens,
                        "surface send budget exhausted; suppressing {unit}"
                    );
                }
                SurfaceSendVerdict::Denied
            }
        }
    }

    /// Draw `tokens` against a surface principal's send budget as one
    /// all-or-nothing unit — the entry point for an activation flush, which is
    /// admitted or refused whole because the batch is atomic.
    ///
    /// The caller draws once for the whole batch and then applies its entries
    /// through [`Messenger::publish_batch_from_surface`], which does not draw
    /// again. That split is deliberate: a per-entry draw could admit a prefix of
    /// an atomic flush and refuse the rest, which is the one thing the batch
    /// contract forbids.
    ///
    /// `component` is the sub-identity whose activation produced the flush, or
    /// `None` for the surface's own kernel grain.
    pub fn draw_surface_send_budget_for_batch(
        &self,
        slug: &str,
        component: Option<&str>,
        tokens: u32,
    ) -> SurfaceSendVerdict {
        let principal = surface_principal(slug, component);
        self.draw_surface_send_budget(
            SurfaceSendDraw {
                slug,
                component,
                principal: principal.as_str(),
                channel: None,
                tokens,
            },
            "activation batches",
        )
    }

    /// The one publish gate sequence, behind `publish`, `publish_from_surface`,
    /// and `publish_from_system`, for every pub/sub scheme.
    ///
    /// The `principal` selects the layer-1 authority source (app vs surface vs
    /// system); every other gate is identical across arms. Where behavior
    /// differs by class it is driven by the resolved channel's
    /// [`ChannelCapabilities`](brenn_envelope::ChannelCapabilities), never by
    /// matching on the scheme at a call site: `durable` picks the commit (DB
    /// rows vs the in-memory ring) and gates the surface send budget;
    /// `transportable` decides whether a commit also fans out to attached wire
    /// receivers. Every option field a publish can carry is carried on both
    /// classes.
    ///
    /// Records the per-`(sender, kind)` denied-publish counter for every denial
    /// arm that carries a [`DenialKind`], so the counter cannot drift from the
    /// outcome. It does not log denials: it lacks the boundary context to tell
    /// an attack from a server bug, and the same arms are "impossible, panic"
    /// for the surface caller. A caller passing an attacker-influenceable
    /// address owns boundary-appropriate security signaling.
    #[allow(clippy::too_many_arguments)]
    async fn publish_core(
        &self,
        origin: PublishOrigin,
        principal: PublishPrincipal<'_>,
        addr: &str,
        body: &str,
        urgency: super::Urgency,
        reply_to: Option<&str>,
        deliver_after: Option<DateTime<Utc>>,
        delivery_deadline: Option<DateTime<Utc>>,
        impetus: Option<Impetus>,
    ) -> PublishResult {
        let sender = self.principal_identity(principal);
        let result = self
            .publish_gated(
                origin,
                principal,
                &sender,
                addr,
                body,
                urgency,
                reply_to,
                deliver_after,
                delivery_deadline,
                impetus,
            )
            .await;
        self.record_publish_denial(&sender, &result);
        result
    }

    /// The stored principal string for a publish, derivable before any policy
    /// lookup so a denial as early as the address gate is still attributable.
    fn principal_identity(&self, principal: PublishPrincipal<'_>) -> String {
        match principal {
            PublishPrincipal::App { slug } => ParticipantId::for_app(slug, &self.source),
            PublishPrincipal::Surface {
                slug, component, ..
            } => surface_principal(slug, component),
            PublishPrincipal::System { component } => ParticipantId::for_system(component),
            PublishPrincipal::Conversation { id, .. } => ParticipantId::for_conversation(id),
        }
        .as_str()
        .to_owned()
    }

    /// Draw one token from the per-(sender, channel) send-rate bucket, creating
    /// it on first use at the channel's resolved rate. Returns `true` when the
    /// publish is admitted, `false` when the rate limit refused it.
    ///
    /// The grain is deliberate: a sender's aggregate allowance is this rate
    /// times the channels its ACLs cover, and that channel set is bounded
    /// because no publisher can mint a channel to widen its own budget. Channels
    /// that are not operator-declared config are created by the server on its
    /// own initiative — a conversation's chat family at conversation creation —
    /// with no path from a publish to a creation. If peer-initiated creation is
    /// ever added, a bus-wide per-sender backstop bucket must land with it.
    ///
    /// This gate is not surface-specific — it runs on every scheme — so it
    /// reports a plain admit/deny rather than a [`SurfaceSendVerdict`].
    fn draw_send_rate(&self, sender: &str, channel: &super::ChannelEntry) -> bool {
        let outcome = {
            let mut buckets = self
                .send_rate_buckets
                .lock()
                .expect("messaging: send_rate_buckets lock poisoned");
            buckets
                .entry((sender.to_owned(), channel.uuid))
                .or_insert_with(|| channel.resolved_channel.send_rate.bucket())
                .try_consume()
        };
        match outcome {
            TokenBucketOutcome::Granted => true,
            TokenBucketOutcome::GrantedAfterSuppression { suppressed } => {
                warn!(
                    sender,
                    channel = %channel.address,
                    suppressed,
                    "send rate limit lifted"
                );
                true
            }
            TokenBucketOutcome::Denied { first } => {
                *self
                    .publish_rate_limited
                    .lock()
                    .expect("messaging: publish_rate_limited lock poisoned")
                    .entry(sender.to_owned())
                    .or_insert(0) += 1;
                if first {
                    warn!(sender, channel = %channel.address, "rate-limiting sender");
                }
                false
            }
        }
    }

    /// Count a denied publish under `(sender, kind)`. A no-op for results that
    /// carry no [`DenialKind`].
    fn record_publish_denial(&self, sender: &str, result: &PublishResult) {
        if let Some(kind) = result.signal_kind() {
            *self
                .publish_denied
                .lock()
                .expect("messaging: publish_denied lock poisoned")
                .entry((sender.to_owned(), kind.as_str().to_owned()))
                .or_insert(0) += 1;
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn publish_gated(
        &self,
        origin: PublishOrigin,
        principal: PublishPrincipal<'_>,
        sender: &str,
        addr: &str,
        body: &str,
        urgency: super::Urgency,
        reply_to: Option<&str>,
        deliver_after: Option<DateTime<Utc>>,
        delivery_deadline: Option<DateTime<Utc>>,
        impetus: Option<Impetus>,
    ) -> PublishResult {
        // 1. Validate address shape, then resolve. Shape errors return
        //    `MalformedAddress`; well-formed
        //    addresses that don't resolve return `UnknownChannel`. The bare
        //    channel name (prefix stripped) is captured here for the layer-2
        //    ACL check below.
        //    An `App` principal's `to` is attacker-influenceable, so it passes the
        //    unreserved-char shape gate. A `System`-substrate publish targeting a
        //    reserved `/`-namespaced channel (`brenn:tools/*`, `brenn:tool-results/*`)
        //    legitimately needs to skip the charset shape gate, because those
        //    addresses the gate rejects — so the exemption is scoped to exactly
        //    (System principal ∧ reserved namespace): a plain prefix-strip there,
        //    the full charset gate everywhere else. System publishes to ordinary
        //    operator channels (catalog, error relay) get the same shape gate as
        //    every other principal. `directory.resolve` stays the authoritative
        //    existence check and the layer-2 ACL still gates below.
        //
        //    The scheme comes from the address itself: one ladder serves every
        //    pub/sub scheme, and the scheme is what the shape gate and the
        //    layer-2 ACL are asked about.
        let scheme = match ChannelScheme::of(addr) {
            Some(s) => s,
            None => return PublishResult::MalformedAddress(addr.to_string()),
        };
        let reserved_system_target = matches!(principal, PublishPrincipal::System { .. })
            && addr
                .strip_prefix(ChannelScheme::Brenn.prefix())
                .is_some_and(crate::tools::is_reserved_channel);
        let channel_name = if reserved_system_target {
            match addr.strip_prefix(ChannelScheme::Brenn.prefix()) {
                Some(name) if !name.is_empty() => name,
                _ => return PublishResult::MalformedAddress(addr.to_string()),
            }
        } else {
            match well_formed_name(addr, scheme) {
                Some(name) => name,
                None => return PublishResult::MalformedAddress(addr.to_string()),
            }
        };
        let channel = match self.directory.resolve(addr) {
            Some(c) => c,
            None => return PublishResult::UnknownChannel(addr.to_string()),
        };
        let capabilities = channel.capabilities();

        // AUTHZ WARNING (security-5): the per-channel sender authorization
        // (allowlist) below is live. The automation fire path
        // (`automation/fire.rs`, `fire_one`) re-checks this same policy at fire
        // time. Automation jobs store `action.to` at create time and fire later;
        // a policy tightened after job creation would be stale at fire time
        // unless that re-check is present.

        // 2. Sender authority + publish authorization, resolved per principal
        //    source. Layer-1: gate on the `MessagingPublish` grant
        //    specifically — NOT `messaging_enabled()` (the participation `OR`).
        //    This is the publish/subscribe split: a
        //    `messaging_subscribe`-only sender is `MissingSender`
        //    here. Yields the policy (for the layer-2 ACL), the stored principal
        //    string, and the optional `Conversation`-origin send budget:
        //    `Some(budget)` for an app (read in the `Conversation` arm of step 5;
        //    falls back to the global default for an app with no `[app.messaging]`
        //    block), `None` for the always-`System` surface arm.
        //
        //    The grant the layer-1 gate demands follows the target's scheme:
        //    `ephemeral:` traffic is authorized by `EphemeralPublish`, `local:`
        //    by `LocalPublish`, every other pub/sub scheme by `MessagingPublish`.
        let grant = match scheme {
            ChannelScheme::Ephemeral => AppCapability::EphemeralPublish,
            ChannelScheme::Local => AppCapability::LocalPublish,
            _ => AppCapability::MessagingPublish,
        };
        let (policy, conversation_send_budget) = match principal {
            PublishPrincipal::App { slug } => {
                let app = match resolve_publish_sender(&self.apps, slug, grant) {
                    Some(a) => a,
                    None => return PublishResult::MissingSender,
                };
                (&app.policy, Some(app.messaging_send_budget()))
            }
            PublishPrincipal::Surface { slug, .. } => {
                // Surfaces are not in `self.apps`; their boot-resolved policy
                // lives in the unified `subscribers` registry, the same
                // authority the delivery-time gate reads via `subscriber_policy`.
                //
                // Keyed at the surface grain for a component publish too: a
                // component's grants are its config-declared bindings, and boot
                // validation already proved each one is covered by the surface's
                // own ACLs. The sub-identity finer-grains attribution and budget,
                // not authority — there is no per-instance policy blob to
                // hand-maintain.
                let policy = match self
                    .targets
                    .registration(&SubscriberEntryKind::Surface(slug.to_string()))
                    .map(|r| r.policy.as_ref())
                    .filter(|p| p.has_grant(grant))
                {
                    Some(p) => p,
                    None => return PublishResult::MissingSender,
                };
                (
                    policy,
                    // Surface is always paired with a `System` origin (see
                    // `publish_from_surface`), which never reads the budget below.
                    // `None` makes that pairing structural: a future `Surface` +
                    // `Conversation` misuse panics loudly at the `.expect()` in
                    // step 5 rather than silently seeding a `remaining = 0` row.
                    None,
                )
            }
            PublishPrincipal::System { component } => {
                // System components are not in `self.apps`; their code-built
                // policy lives in the unified `subscribers` registry, the same
                // authority the delivery-time gate reads via `subscriber_policy`.
                let policy = match self
                    .targets
                    .registration(&SubscriberEntryKind::System(component.to_string()))
                    .map(|r| r.policy.as_ref())
                    .filter(|p| p.has_grant(grant))
                {
                    Some(p) => p,
                    None => return PublishResult::MissingSender,
                };
                (
                    policy,
                    // System is always paired with a `System` origin, which never
                    // reads the budget below — same structural `None` as Surface.
                    None,
                )
            }
            PublishPrincipal::Conversation { app_slug, .. } => {
                // Authority is the app's derived harness policy, not its
                // authored one: the app's LLM holds no chat-tree grant unless an
                // operator wrote one, and this arm must not lend it any. A
                // missing app is `MissingSender`; the grant check against the
                // harness policy is defensive — the four transport grants are
                // there by construction — and a failure is a resolution bug, not
                // a quiet bypass.
                let policy = match self
                    .apps
                    .get(app_slug)
                    .map(|app| &app.chat_harness_policy)
                    .filter(|p| p.has_grant(grant))
                {
                    Some(p) => p,
                    None => return PublishResult::MissingSender,
                };
                (
                    policy,
                    // Paired with a `System` origin (see
                    // `publish_from_conversation`), which never reads the budget
                    // below — same structural `None` as Surface and System.
                    None,
                )
            }
        };
        // Layer-2: the target scheme's per-channel publish ACL against the bare
        // channel name captured at gate 1. This is a pure in-memory policy read
        // against the already-resolved channel and runs BEFORE the budget
        // decrement / DB work, so an out-of-scope publish consumes no budget and
        // takes no lock.
        if !publish_acl_allows(policy, scheme, channel_name) {
            return PublishResult::AclDenied(addr.to_string());
        }

        // 2a. Impetus is capability-gated, not capability-scoped: it
        //     is carried authority, meaningful on every scheme, so no channel
        //     class refuses it. What refuses it is a policy without
        //     `MintImpetus` — and the refusal is of the whole publish, never of
        //     the field alone. Stripping and accepting would turn an
        //     unauthorized claim into a silently-downgraded success, and the
        //     redemption side reads the stored field as proof the claim was
        //     authorized when it was made.
        //
        //     Placed after layer-1 and layer-2, so an unauthorized sender
        //     attaching impetus hears about its own missing grant or ACL rather
        //     than learning the channel exists. Validate-only, so it precedes
        //     both spending gates.
        //
        //     Logged here: no caller boundary reports this denial.
        if impetus.is_some() && !policy.has_grant(AppCapability::MintImpetus) {
            log_component_security_event(
                SecurityEventType::ImpetusMintDenied,
                &sanitize_untrusted_str(sender, MAX_LOGGED_UNTRUSTED_BYTES),
                &format!(
                    "impetus claimed without the mint grant; publish refused whole; address={}",
                    sanitize_untrusted_str(addr, MAX_LOGGED_UNTRUSTED_BYTES)
                ),
            );
            return PublishResult::ImpetusUnauthorized;
        }

        // 3. Body length.
        if let Err(e) = check_body_size(body, self.defaults.max_body_bytes) {
            return PublishResult::BodyTooLarge {
                len: e.len,
                max: e.max,
            };
        }

        // 3a. Resolve reply_to (if any): shape → visibility → resolve. Shape
        //    errors return `MalformedAddress`. The visibility gate runs BEFORE
        //    resolution so an out-of-visibility reply_to fails identically
        //    whether or not the channel exists — closing the success/failure
        //    existence oracle a plain resolve would open. Visibility is the
        //    union of the sender's publish allowlist and its delivery scope:
        //    channels it could name in `to`, plus channels it could legitimately
        //    learn about as a subscriber (a reply target is a channel the sender
        //    expects to hear replies on). Out-of-scope → `AclDenied`;
        //    in-scope-but-unresolved → `UnknownChannel`.
        //
        //    Validate-only (spends no token), so it runs ahead of both spending
        //    gates below: a publish doomed by a malformed or out-of-scope
        //    reply_to costs no surface-budget and no rate token.
        //
        //    Runs after the layer-1 grant gate and the layer-2 publish ACL, and
        //    that order is load-bearing: these arms name the *reply* address, so
        //    a sender unauthorized on the publish target must meet
        //    `MissingSender`/`AclDenied` for the target first rather than an
        //    outcome that turns on whether some other channel exists.
        //
        //    Every scheme runs it. A reply address is metadata the consumer reads
        //    back — the bus routes nothing with it — so what the carrying channel
        //    is made of decides nothing here; only the target's own `brenn:`
        //    shape does.
        let reply_to_target = if let Some(rt_addr) = reply_to {
            let rt_name = match well_formed_name(rt_addr, ChannelScheme::Brenn) {
                Some(name) => name,
                None => return PublishResult::MalformedAddress(rt_addr.to_string()),
            };
            let visible = reply_to_visible(policy, ChannelScheme::Brenn, rt_name, rt_addr);
            if !visible {
                return PublishResult::AclDenied(rt_addr.to_string());
            }
            match self.directory.resolve(rt_addr) {
                Some(c) => Some(store::ReplyTarget {
                    uuid: c.uuid,
                    address: c.address.clone(),
                }),
                None => return PublishResult::UnknownChannel(rt_addr.to_string()),
            }
        } else {
            None
        };

        // 3b. Surface send budget, keyed by principal — the surface's own kernel
        //     identity or one component kind on it. Every durable publish a
        //     surface makes under its own identity (bound outputs and error
        //     reports alike) draws from the process-lifetime bucket of whichever
        //     principal made it. That keying *is* the blast-radius scoping: a
        //     component's retry loop drains its own kind's bucket, leaving its
        //     siblings and the kernel's own reports able to publish.
        //
        //     Consulted only after the ACL/scope gates *and* the body-size check,
        //     so a rejected publish (out-of-scope or oversized) costs no budget —
        //     the same rule as the conversation budget step below, which the
        //     process-lifetime, reconnect-resistant bucket makes load-bearing: an
        //     oversized-publish loop must not silently drain the budget that
        //     carries the surface's own error reports. Keyed by principal, not
        //     connection, so a reconnecting session inherits the drained bucket.
        //     The bucket emits its own first-denial / recovery transition warns,
        //     attributed to the principal.
        // Platform-origin surface telemetry (geometry/status/stamps) is exempt:
        // it skips only this step and passes every other gate. See
        // `PublishPrincipal::Surface`.
        //
        // Scoped to durable channels by capability: this bucket bounds what a
        // surface writes into the server's *persistent* substrate, and its
        // sustained rate is sized for that (single-digit publishes per minute).
        // A non-durable channel writes nothing to disk and carries traffic
        // orders of magnitude faster; it is bounded by the per-(sender, channel)
        // send-rate gate below, which every scheme runs.
        if capabilities.durable
            && let PublishPrincipal::Surface {
                slug,
                component,
                platform: false,
            } = principal
            && matches!(
                self.draw_surface_send_budget(
                    SurfaceSendDraw {
                        slug,
                        component,
                        principal: sender,
                        channel: Some(addr),
                        tokens: 1,
                    },
                    "durable publishes"
                ),
                SurfaceSendVerdict::Denied
            )
        {
            return PublishResult::BudgetExhausted;
        }

        // 3c. Per-(sender, channel) send-rate gate — the one unified rate limit,
        //     on every scheme. Every validate-only gate (shape, authorization,
        //     size, reply_to) runs ahead of it, so a publish doomed by any of
        //     those costs no token. Of the two spending gates, the surface send
        //     budget (3b) is drawn first: a durable surface publish that is then
        //     rate-limited here has already spent one surface-budget token. That
        //     ordering is deliberate — the surface budget only gates durable
        //     surface writes, the common case is a publish that passes both, and
        //     a rate token refills far faster than the scarce surface budget.
        if !self.draw_send_rate(sender, &channel) {
            return PublishResult::RateLimited;
        }

        // 4a. A release time at or before now schedules nothing: the message
        //     enters retention immediately. Deciding that once, here, is what
        //     keeps the park decision single — every commit path below reads
        //     this value, so a row carries a release time iff it holds no
        //     retention position. Deciding it twice against two clock reads is
        //     how a message ends up visible on one test and parked on the other.
        let deliver_after = deliver_after.filter(|da| *da > Utc::now());

        // 5. Per-conversation send budget. It bounds what a conversation sends,
        //    not where the bytes land, so every scheme draws on it. The
        //    `UPDATE ... WHERE remaining > 0` row count is the authoritative
        //    gate; a `System` origin has no budget and touches no row at all
        //    (no INSERT, no FK exposure).
        let remaining_budget = match origin {
            PublishOrigin::Conversation { id } => {
                let budget = conversation_send_budget.expect(
                    "Conversation origin requires a send budget — only App principals \
                     produce Conversation-origin publishes",
                );
                let conn = self.db.lock().await;
                match decrement_send_budget(&conn, id, budget) {
                    BudgetDecrement::Ok { remaining } => Some(remaining),
                    BudgetDecrement::Exhausted => return PublishResult::BudgetExhausted,
                }
            }
            PublishOrigin::System => None,
        };

        let publish_ts_ns = db::utc_to_ns(Utc::now());

        // 6. Commit into the channel's retention through its store. One fork for
        //    every scheme — whether the bytes land in a table or a ring, and
        //    whether the deferred set is rows or a map, is the store's business
        //    and no caller's. The deferred cap is the store's too, so a durable
        //    schedule is refused at the same channel-wide bound a ring one is.
        let store = self.store_for(&channel);
        let message = store::NewMessage {
            source: self.source.as_ref().to_owned(),
            sender: sender.to_owned(),
            body: body.to_string(),
            urgency,
            envelope_type: scheme,
            reply_to: reply_to_target,
            delivery_deadline,
            impetus,
            publish_ts_ns,
        };
        let (message_id, retained_seq) = match deliver_after {
            Some(release_at) => match store.park(message, release_at).await {
                Ok(parked) => (parked.message_uuid, None),
                Err(brenn_queue::QuotaExceeded { cap }) => {
                    // The cap is only knowable at the park, after the budget
                    // draw: a sender retrying against a full deferred set must
                    // not drain its budget without landing a message.
                    if let PublishOrigin::Conversation { id } = origin {
                        let budget = conversation_send_budget
                            .expect("Conversation origin requires a send budget");
                        let conn = self.db.lock().await;
                        refund_send_budget(&conn, id, budget);
                    }
                    return PublishResult::DeferredQuotaExceeded { cap };
                }
            },
            None => {
                let outcome = store.append(message).await;
                self.enact_overflow_events(&channel, &outcome.overflow);
                (
                    outcome.committed.message_uuid,
                    Some(outcome.committed.seq.0),
                )
            }
        };

        // 7. Surface feed: hand the committed envelope to attached surface
        //    subscriptions as a row-less live delivery. After the commit —
        //    nothing is owed to a disconnected session.
        //
        //    A parked message is not fed: the feed is the wire analogue of
        //    publish-time delivery for a message that entered retention, and a
        //    parked message is not observable to any subscriber, replay, query, or
        //    feed before its release. Holding no retention position is exactly
        //    that state, so the assigned position is the condition — and it is
        //    what the fed row's wire cursor is minted from. The release sweep
        //    fans out what it moves into retention.
        //
        //    The targets are resolved here, under that condition, rather than
        //    before the commit: a parked publish would only walk the subscriber
        //    list to discard the answer. Transportability is the whole condition
        //    on the class side: a confined channel has no wire and reaches no
        //    session, and every channel that does have one is fed the same way
        //    whether its retention is a table or a ring.
        if let Some(retained_seq) = retained_seq
            && capabilities.transportable
        {
            let feed_targets = self.attached_surface_feed_targets(&channel);
            if !feed_targets.is_empty() {
                let envelope = Arc::new(surface_feed_envelope(
                    message_id,
                    self.source.as_ref().to_owned(),
                    channel.address.clone(),
                    sender.to_owned(),
                    publish_ts_ns,
                    body.to_owned(),
                    reply_to.map(|s| s.to_owned()),
                    delivery_deadline,
                    deliver_after,
                    impetus,
                    urgency,
                    scheme,
                ));
                self.fan_out_surface_feed(
                    &feed_targets,
                    envelope,
                    i64::try_from(retained_seq)
                        .expect("messaging: retention position out of range"),
                )
                .await;
            }
        }

        // 8. Signal the background dispatcher. All dispatch is off-stack (R1) —
        //    a parked message's release, a deadline sweep, and an immediate
        //    delivery all wake the same loop.
        self.dispatch_kick();

        PublishResult::Ok {
            message_id,
            address: channel.address.clone(),
            remaining_budget,
        }
    }

    /// The surface subscribers on a channel that a retained message is fanned
    /// out to live, resolved through the same registry and the same
    /// delivery-time ACL gate as the push targets.
    pub(crate) fn resolve_surface_feed_targets(
        &self,
        channel_address: &str,
        subscribers: &[crate::messaging::SubscriberEntry],
    ) -> Vec<SurfaceFeedTarget> {
        self.targets
            .surface_feed_targets(channel_address, subscribers)
    }

    /// `entry`'s surface feed targets, or empty when no attached session holds a
    /// subscription for any of them.
    ///
    /// The empty answer is the caller's licence to skip building the owned,
    /// body-copying feed envelope at all, so the two questions are asked
    /// together: a fan-out with no attached holder does nothing but allocate.
    /// Callers that fan a whole batch memoize the target resolution themselves
    /// and re-ask the attachment question per entry, since a session can detach
    /// mid-batch.
    pub(crate) fn attached_surface_feed_targets(
        &self,
        entry: &ChannelEntry,
    ) -> Vec<SurfaceFeedTarget> {
        let targets =
            self.resolve_surface_feed_targets(&entry.address, entry.subscribers.as_slice());
        if targets.is_empty()
            || !self
                .router
                .any_surface_session_subscribed(&entry.address, &targets)
        {
            return Vec::new();
        }
        targets
    }

    /// Hand a message that has just entered retention to every attached,
    /// subscribed surface session, as a row-less live fan-out.
    ///
    /// This is the whole of a surface's delivery trigger. A surface holds no
    /// position, so nothing that walks positions can name it; what reaches an
    /// attached session reaches it here, and what a detached (or queue-full)
    /// session missed it recovers from its own wire cursor at the next resume.
    ///
    /// Runs after the commit transaction's lock is released: the router touches
    /// no DB and only enqueues onto attached sessions.
    pub(crate) async fn fan_out_surface_feed(
        &self,
        targets: &[SurfaceFeedTarget],
        envelope: Arc<super::MessageEnvelope>,
        retained_seq: i64,
    ) {
        for target in targets {
            if !target.push_enabled {
                self.router
                    .deliver_context(&target.kind, &envelope, retained_seq)
                    .await;
                continue;
            }
            match self
                .router
                .deliver(&target.kind, &envelope, retained_seq)
                .await
            {
                // Delivered, or nothing attached and subscribed. A surface is
                // owed nothing while away; the suffix above its cursor is what
                // it resumes to.
                Ok(_) => {}
                Err(e) => {
                    let subscriber = target.subscriber();
                    warn!(
                        subscriber = %subscriber.as_str(),
                        channel = %envelope.channel,
                        retained_seq,
                        error = %e,
                        "surface live fan-out failed; sessions recover at their next resume"
                    );
                }
            }
        }
    }
}

/// Build the row-less envelope a just-committed message is fanned out to
/// surface sessions as. Single definition of the envelope shape shared by the
/// ad-hoc publish and both batch flush paths, so a new envelope field is wired
/// in one place rather than three.
///
/// `scheme` is the channel's own, carried verbatim: a fed envelope is
/// indistinguishable from the one a subscriber reads back out of retention, and
/// retention stamps the channel's scheme.
#[allow(clippy::too_many_arguments)]
fn surface_feed_envelope(
    message_id: Uuid,
    source: String,
    channel: String,
    sender: String,
    publish_ts_ns: i64,
    body: String,
    reply_to: Option<String>,
    delivery_deadline: Option<DateTime<Utc>>,
    deliver_after: Option<DateTime<Utc>>,
    impetus: Option<Impetus>,
    urgency: Urgency,
    scheme: ChannelScheme,
) -> super::MessageEnvelope {
    super::MessageEnvelope {
        message_id,
        source,
        channel,
        sender,
        publish_ts: db::ns_to_utc(publish_ts_ns),
        body,
        reply_to,
        delivery_deadline,
        deliver_after,
        impetus,
        urgency,
        envelope_type: scheme,
    }
}

impl Messenger {
    /// Insert one message row under a caller-owned transaction.
    ///
    /// Target-blind, like the durable store's own `append`: the row is written
    /// once and nothing per-subscriber is, because who reads it is decided by
    /// each subscriber's own position at its own read.
    ///
    /// The caller holds the DB lock (`conn` / the transaction) and commits it
    /// after all `insert_message` calls for the batch.
    ///
    /// Always inserts with no impetus.
    #[allow(clippy::too_many_arguments)]
    fn insert_message(
        &self,
        tx: &rusqlite::Transaction<'_>,
        channel: &Arc<super::ChannelEntry>,
        envelope_type: ChannelScheme,
        source: &str,
        sender: &str,
        body: &str,
        urgency: super::Urgency,
        publish_ts_ns: i64,
        reply_to_uuid: Option<Uuid>,
        deliver_after: Option<DateTime<Utc>>,
        delivery_deadline: Option<DateTime<Utc>>,
    ) -> InsertedMessage {
        insert_message_in_tx(
            tx,
            channel.uuid,
            source,
            sender,
            body,
            urgency,
            envelope_type,
            reply_to_uuid,
            delivery_deadline,
            deliver_after,
            None,
            publish_ts_ns,
        )
    }
}

impl Messenger {
    /// Unified ingress entry point. Replaces `AppState::submit_event` for
    /// mqtt, webhook, and automation error-report callers (design §2.3).
    ///
    /// Inserts durably then signals the background dispatcher (R1). All
    /// delivery is off-stack; the dispatcher decides whether to inject into
    /// a live bridge or eager-wake a sleeping one.
    ///
    /// **No budget, no channel resolve, no sender gate** — ingress bypasses
    /// all of `publish`'s §2.3 gates.
    pub async fn submit_ingress(
        &self,
        conversation_id: i64,
        app_slug: &str,
        source: &str,
        summary: &str,
        payload: &str,
        urgency: Urgency,
    ) {
        let publish_ts_ns = db::utc_to_ns(Utc::now());
        let subscriber = ParticipantId::for_conversation(conversation_id);

        // 1. Durably insert message + push (at-least-once: before any signal).
        // TODO(ingress-retirement): publish onto a real bus channel instead of
        // writing channel-less ingress rows.
        let _push_id = {
            let conn = self.db.lock().await;
            let (_message_id, push_id) = insert_ingress_message(
                &conn,
                &subscriber,
                app_slug,
                source,
                summary,
                payload,
                urgency,
                publish_ts_ns,
            );
            push_id
        };

        // 2. Signal the background dispatcher. All dispatch is off-stack (R1).
        self.dispatch_kick();
    }
}

impl Messenger {
    /// Host-originated transport ingress publish.
    ///
    /// Unlike `publish`, this entry point is for host-side transport adaptors
    /// (webhook, mqtt) that have already performed admission (signature
    /// verification, replay protection). It bypasses all CC-facing gates
    /// (sender lookup, send-budget decrement, body-length check) and stamps the
    /// channel's own `transport_type` on the stored message row.
    ///
    /// Returns once the durable DB insert of the message + pending-push rows
    /// commits. Panics on any DB error (fail-fast; axum's per-task panic handler
    /// converts this to a 500 for the HTTP caller — satisfying the "never 2xx if
    /// durable enqueue failed" contract without an explicit `Err` path). A
    /// host-built malformed envelope or an unresolvable channel likewise panics
    /// (fail-fast, CLAUDE.md). There are no business-logic rejections (no budget /
    /// sender gate) for this host-originated entry point.
    ///
    /// `source` and `sender` are stamped verbatim on the message row (e.g.
    /// `source = "webhook:<slug>"`, `sender = key_id`).
    pub async fn publish_transport_ingress(
        &self,
        channel: Arc<super::ChannelEntry>,
        source: &str,
        sender: &str,
        body: &str,
        urgency: Urgency,
    ) {
        let publish_ts_ns = db::utc_to_ns(Utc::now());

        // Accept-side validation: deserialize the body JSON into the channel's
        // transport-typed struct to verify the host built a structurally valid
        // envelope. A deserialize failure is a host-internal bug — panic.
        match channel.transport_type {
            ChannelScheme::Webhook => {
                serde_json::from_str::<super::WebhookEnvelope>(body).unwrap_or_else(|e| {
                    panic!(
                        "publish_transport_ingress: host built a malformed WebhookEnvelope for \
                         channel '{}' — this is a host-internal bug, not an attacker input: {e}",
                        channel.address
                    )
                });
            }
            ChannelScheme::Mqtt => {
                serde_json::from_str::<super::MqttEnvelope>(body).unwrap_or_else(|e| {
                    panic!(
                        "publish_transport_ingress: host built a malformed MqttEnvelope for \
                         channel '{}' — this is a host-internal bug, not an attacker input: {e}",
                        channel.address
                    )
                });
            }
            other => {
                panic!(
                    "publish_transport_ingress: called with unexpected transport type {:?} for \
                     channel '{}' — only Webhook and Mqtt are valid for this entry point",
                    other, channel.address
                );
            }
        }

        // DB work: insert the message row under one lock (step 6). No budget
        // decrement, no sender gate, no body-length gate. No deliver_after /
        // delivery_deadline for transport ingress — both are None (always immediate).
        {
            let conn = self.db.lock().await;
            let tx = conn
                .unchecked_transaction()
                .expect("messaging: begin transport ingress tx");
            self.insert_message(
                &tx,
                &channel,
                channel.transport_type,
                source,
                sender,
                body,
                urgency,
                publish_ts_ns,
                None, // no reply_to
                None, // no deliver_after — transport ingress is always immediate
                None, // no delivery_deadline
            );
            tx.commit().expect("messaging: commit transport ingress tx");
        }

        // Signal the background dispatcher. All dispatch is off-stack (R1).
        self.dispatch_kick();
    }
}

/// One buffered publish from a WASM activation. `channel_address` is the
/// resolved bus channel (attenuation already enforced at the ports import);
/// `body` is the message payload.
pub struct WasmPublish<'a> {
    pub channel_address: &'a str,
    pub body: &'a str,
    /// Sender urgency intent for this publish.
    pub urgency: super::Urgency,
    /// Reply channel address, set only for async tool-call requests (the caller's
    /// result inbox `brenn:tool-results/<slug>`). `None` for ordinary port
    /// publishes. Host-resolved to a channel reference at flush; the address must
    /// resolve in the directory (a miss is a host-wiring bug, not attacker input).
    pub reply_to: Option<&'a str>,
    /// Requested release time for a deferred publish (`ports.publish-deferred`),
    /// or `None` for an immediate one. A value in the future parks the message
    /// until it; a past/absent value commits immediately. Mutually exclusive with
    /// `reply_to` in practice (tool requests never defer).
    pub deliver_after: Option<DateTime<Utc>>,
}

impl Messenger {
    /// Flush a WASM activation's buffered publishes atomically.
    ///
    /// Host-originated: no budget gate, no sender gate, no body-size gate
    /// (all enforced at the WASM ports import). Panics on any DB error or
    /// unresolvable channel address (boot-validated; a miss is a host-internal bug).
    ///
    /// All messages in the batch are inserted in one transaction and committed
    /// together. A panic mid-flush unwinds through the `Transaction` Drop
    /// guard, rolling back — none of the batch is visible: a flush lands
    /// whole or not at all.
    ///
    /// Each publish carries its own urgency: port-configured default (for `publish`)
    /// or guest-supplied (for `publish-with-urgency`).
    pub async fn publish_from_wasm(&self, consumer_slug: &str, publishes: &[WasmPublish<'_>]) {
        if publishes.is_empty() {
            return;
        }

        info!(
            consumer_slug = consumer_slug,
            publish_count = publishes.len(),
            "publish_from_wasm: flushing WASM activation publishes"
        );

        let sender = super::ParticipantId::for_wasm(consumer_slug)
            .as_str()
            .to_owned();
        let source = self.source.as_ref();
        // One clock read for the whole flush's park-vs-immediate decisions, so
        // every entry in the batch is judged against the same instant.
        let flush_now = Utc::now();

        // Deferred durable surface feeds: built under the lock, fanned out
        // after it is released.
        let mut surface_feeds: Vec<(Arc<super::MessageEnvelope>, i64, Vec<SurfaceFeedTarget>)> =
            Vec::new();
        // Non-durable outputs: recorded in call order, committed to their stores
        // after the durable transaction commits and its lock is released. The
        // third element is the release time for a deferred entry (`Some` parks,
        // `None` enters retention immediately).
        let mut nondurable_pending: Vec<(
            Arc<super::ChannelEntry>,
            store::NewMessage,
            Option<DateTime<Utc>>,
        )> = Vec::new();
        // Schedules the deferred cap refused, on either substrate. Reported
        // after the lock is released so the durable transaction is not held
        // across the logging.
        let mut refused: Vec<(String, u64)> = Vec::new();

        {
            let conn = self.db.lock().await;
            let tx = conn
                .unchecked_transaction()
                .expect("publish_from_wasm: begin tx");

            // Per-flush memoization of the surface-feed resolution: the directory is
            // immutable and the lock is held throughout, so targets cannot change
            // mid-flush. The dominant case is all publishes targeting one channel.
            let mut targets_cache: HashMap<&str, ResolvedChannelTargets> = HashMap::new();

            // Monotonic publish_ts_ns assignment: each message gets
            // max(prev_ts + 1, now) to guarantee strictly increasing timestamps
            // within the activation (call-order visibility contract).
            let mut prev_ts: Option<i64> = None;

            for publish in publishes {
                let channel_addr = publish.channel_address;
                let entry = self.directory.resolve(channel_addr).unwrap_or_else(|| {
                    panic!(
                        "publish_from_wasm: channel {channel_addr:?} not in directory — \
                         boot validation should have caught this (slug={consumer_slug})"
                    )
                });

                // Resolve the optional reply_to address to the target it names.
                // Host-resolved (the guest never named it — `queue_async` derived
                // the caller's own inbox), so an unresolvable address is a
                // host-wiring bug, not attacker input: fail fast. Resolved for
                // both substrates here: a reply address is metadata either
                // retention carries.
                let reply_to_target = publish.reply_to.map(|addr| {
                    let target = self.directory.resolve(addr).unwrap_or_else(|| {
                        panic!(
                            "publish_from_wasm: reply_to channel {addr:?} not in directory \
                             — boot validation should have caught this (slug={consumer_slug})"
                        )
                    });
                    store::ReplyTarget {
                        uuid: target.uuid,
                        address: target.address.clone(),
                    }
                });

                // Non-durable: skip the durable transaction machinery; ring
                // append is deferred to after the lock.
                if !entry.capabilities().durable {
                    let scheme = ChannelScheme::of(channel_addr).unwrap_or_else(|| {
                        panic!(
                            "publish_from_wasm: channel {channel_addr:?} carries no scheme prefix \
                             (slug={consumer_slug})"
                        )
                    });
                    let release = publish.deliver_after.filter(|da| *da > flush_now);
                    let message = store::NewMessage {
                        source: source.to_owned(),
                        sender: sender.clone(),
                        body: publish.body.to_string(),
                        urgency: publish.urgency,
                        envelope_type: scheme,
                        reply_to: reply_to_target,
                        delivery_deadline: None,
                        impetus: None,
                        publish_ts_ns: db::utc_to_ns(Utc::now()),
                    };
                    nondurable_pending.push((entry, message, release));
                    continue;
                }

                let (channel, feed_targets) =
                    targets_cache.entry(channel_addr).or_insert_with(|| {
                        let feed = self.resolve_surface_feed_targets(
                            &entry.address,
                            entry.subscribers.as_slice(),
                        );
                        (entry.clone(), feed)
                    });

                let now_ns = db::utc_to_ns(Utc::now());
                let publish_ts_ns = match prev_ts {
                    None => now_ns,
                    Some(prev) => std::cmp::max(prev + 1, now_ns),
                };
                prev_ts = Some(publish_ts_ns);

                let release = publish.deliver_after.filter(|da| *da > flush_now);
                // Refused → the schedule is dropped and the flush carries on, the
                // same answer the non-durable arm below gives.
                if release.is_some()
                    && let Some(cap) = db::deferred_cap_refusal(
                        &tx,
                        channel.uuid,
                        channel.resolved_channel.retain_depth,
                    )
                {
                    refused.push((channel.address.clone(), cap));
                    continue;
                }
                let inserted = self.insert_message(
                    &tx,
                    channel,
                    ChannelScheme::Brenn,
                    source,
                    &sender,
                    publish.body,
                    publish.urgency,
                    publish_ts_ns,
                    reply_to_target.as_ref().map(|target| target.uuid),
                    release,
                    None, // no delivery_deadline
                );
                // A parked message is not observable before release, so it must
                // not be fed now.
                if release.is_none()
                    && !feed_targets.is_empty()
                    && self
                        .router
                        .any_surface_session_subscribed(&channel.address, feed_targets)
                {
                    surface_feeds.push((
                        Arc::new(surface_feed_envelope(
                            inserted.uuid,
                            source.to_owned(),
                            channel.address.clone(),
                            sender.clone(),
                            publish_ts_ns,
                            publish.body.to_owned(),
                            publish.reply_to.map(|s| s.to_owned()),
                            None,
                            None,
                            None,
                            publish.urgency,
                            ChannelScheme::Brenn,
                        )),
                        inserted.retained_seq.expect(
                            "publish: an unparked durable message holds a retention position",
                        ),
                        feed_targets.clone(),
                    ));
                }
            }

            tx.commit().expect("publish_from_wasm: commit tx");
            debug!(
                consumer_slug = consumer_slug,
                publish_count = publishes.len(),
                "publish_from_wasm: batch committed"
            );
        }

        // Durable surface feeds, fanned out after the lock is released.
        for (envelope, seq, targets) in surface_feeds {
            self.fan_out_surface_feed(&targets, envelope, seq).await;
        }

        // Surface feed targets are memoized per address as the durable half
        // above memoizes its own: a surface subscriber list is boot-resolved, so
        // it cannot change mid-batch, and the dominant case is a batch fanning
        // one port. Whether a session is attached is still asked per entry.
        let mut ring_targets_cache: HashMap<String, Vec<SurfaceFeedTarget>> = HashMap::new();
        for (entry, message, release) in nondurable_pending {
            let store = self.store_for(&entry);
            match release {
                Some(release_at) => {
                    if let Err(brenn_queue::QuotaExceeded { cap }) =
                        store.park(message, release_at).await
                    {
                        refused.push((entry.address.clone(), cap));
                    }
                }
                None => {
                    let feed_targets = ring_targets_cache
                        .entry(entry.address.clone())
                        .or_insert_with(|| {
                            self.resolve_surface_feed_targets(
                                &entry.address,
                                entry.subscribers.as_slice(),
                            )
                        });
                    let attached = !feed_targets.is_empty()
                        && self
                            .router
                            .any_surface_session_subscribed(&entry.address, feed_targets);
                    // A fed envelope is indistinguishable from the one a
                    // subscriber reads back out of retention, so it carries the
                    // reply address the ring is about to retain.
                    let fed = attached.then(|| {
                        (
                            message.body.clone(),
                            message.sender.clone(),
                            message.publish_ts_ns,
                            message.urgency,
                            message.envelope_type,
                            message
                                .reply_to
                                .as_ref()
                                .map(|target| target.address.clone()),
                        )
                    });
                    let outcome = store.append(message).await;
                    self.enact_overflow_events(&entry, &outcome.overflow);
                    if let Some((body, sender, publish_ts_ns, urgency, scheme, reply_to)) = fed {
                        let envelope = Arc::new(surface_feed_envelope(
                            outcome.committed.message_uuid,
                            source.to_owned(),
                            entry.address.clone(),
                            sender,
                            publish_ts_ns,
                            body,
                            reply_to,
                            None,
                            None,
                            None,
                            urgency,
                            scheme,
                        ));
                        self.fan_out_surface_feed(
                            feed_targets,
                            envelope,
                            i64::try_from(outcome.committed.seq.0)
                                .expect("messaging: retention position out of range"),
                        )
                        .await;
                    }
                }
            }
        }

        // Deferred-cap refusals from both substrates, reported once the durable
        // lock is released. A flush has no error channel back to the guest, so a
        // refused schedule is logged and counted (a dropped schedule is a
        // component that never wakes again — a health check can read the
        // counter) and the schedule is gone.
        //
        // TODO(deferred-flush-drop-signal): surface the drop on the consumer's
        // error-report path rather than only the host log. The surface batch
        // flush ends in the twin of this loop, reporting its own refusals under
        // its own identity fields; one signal path serves both, so the two loops
        // move together.
        for (channel, cap) in refused {
            self.record_dropped_deferred(consumer_slug, &channel);
            warn!(
                consumer_slug = consumer_slug,
                channel = channel.as_str(),
                cap,
                "publish_from_wasm: deferred publish dropped — channel deferred set at its \
                 retain_depth cap"
            );
        }

        self.dispatch_kick();
    }
}

/// One entry of a surface activation's flush. `channel_address` is the bound
/// output's boot-resolved address (the caller resolved port → channel against
/// its own declaration set); `urgency` is already the per-call override or the
/// port's configured default, resolved by the caller from the *server's* output
/// map.
pub struct SurfaceBatchPublish<'a> {
    pub channel_address: &'a str,
    pub body: &'a str,
    pub urgency: super::Urgency,
    /// This entry's publish timestamp, assigned by the caller in call order
    /// across the whole flush — so call order is visible across the class
    /// boundary and not merely within each substrate. Nanosecond precision; the
    /// durable row persists it verbatim as `publish_ts_ns`.
    pub publish_ts_ns: i64,
    /// When set, park this entry until then instead of committing it into
    /// retention.
    ///
    /// **Already judged against the flush's single clock read**: the caller reads
    /// the clock once for the whole flush and passes `None` for a time that has
    /// already passed, so every entry of one flush — both substrates — decides
    /// park-vs-immediate against the same instant. This entry point re-reads no
    /// clock and re-compares nothing.
    pub deliver_after: Option<DateTime<Utc>>,
}

impl Messenger {
    /// Apply one surface activation's flush, whatever mix of channels it names —
    /// in call order, each entry at its own urgency and its caller-assigned
    /// timestamp.
    ///
    /// **Where a message lands is decided here, not by the caller.** The batch
    /// arrives as one list; the channel's own capabilities put each entry in a
    /// table or a ring, exactly as they do for a single publish. The durable
    /// entries commit first, as one transaction, then the non-durable ones; call
    /// order holds within each, and nothing is promised between them beyond the
    /// stamps below — a shared commit instant was never the guarantee.
    ///
    /// **Stamps arrive assigned, not minted here.** The caller stamps the whole
    /// flush monotonically in call order in one pass, so the ordering contract
    /// holds across the class boundary; a stamp minted inside this transaction
    /// could only order the durable half against itself.
    ///
    /// The all-or-nothing guarantee is the point: an activation's publishes were
    /// buffered and released together by the kernel's flush-on-ok rule, so a
    /// partially-applied batch would publish a state no component ever asked to
    /// exist. A panic mid-batch unwinds through the `Transaction` drop guard and
    /// rolls the whole thing back.
    ///
    /// **The send budget is not drawn here** — the caller draws once for the whole
    /// batch via [`Messenger::draw_surface_send_budget_for_batch`] before calling,
    /// because a per-entry draw could refuse the tail of an atomic flush.
    ///
    /// Every other gate runs, per entry, and **panics rather than returning**: for
    /// a bound output, address shape, directory existence, the scheme's publish
    /// grant and ACL, and the body cap are all boot-validated and boot-static, and
    /// the caller has already answered its client a violation for every
    /// client-reachable way to name something else. Reaching a failure here means
    /// the server's own output map disagrees with its directory or its policy —
    /// publishing anyway would be routing traffic no operator authorized.
    ///
    /// **Caller precondition, unchecked here: `component`, when set, must be a
    /// declared instance's name.** It is interpolated straight into the sender
    /// identity (`surface:<slug>#<component>`), and nothing below re-derives or
    /// re-admits it — this entry point deliberately draws no budget (see above),
    /// so there is no lookup left to catch a fabricated name. A caller that skips
    /// the declaration check commits durable rows under a sub-identity no operator
    /// declared, which is the one attribution the surface identity model exists to
    /// make impossible. `None` is the surface's own bare identity and needs no
    /// admission — it is the identity a caller already has. Any caller owes the
    /// admission check against the boot-resolved declaration set.
    ///
    /// **Deferral is per-entry and its refusal is not an error.** An entry
    /// carrying a release time is parked against the channel's own
    /// `retain_depth` cap — inside the same transaction for a durable channel,
    /// at the store for a ring one — and until it releases it holds no retention
    /// position, so nothing observes it. An entry the cap refuses has its
    /// *schedule* dropped: nothing stored, a warn naming the channel and the cap,
    /// a counter, and the rest of the batch carries on. Aborting instead would
    /// discard entries the component published unconditionally over one it merely
    /// scheduled, and a post-activation flush has no error channel back to the
    /// guest to carry either outcome.
    ///
    /// Returns the number of entries whose schedule was refused that way, so the
    /// caller counts as published only what published.
    pub async fn publish_batch_from_surface(
        &self,
        slug: &str,
        component: Option<&str>,
        publishes: &[SurfaceBatchPublish<'_>],
    ) -> usize {
        if publishes.is_empty() {
            return 0;
        }

        // Layer-1, once per batch: the surface's boot-resolved policy, keyed at
        // the surface grain for a component publish exactly as `publish_core`
        // does — a component's grants *are* its config-declared bindings, which
        // boot proved covered by the surface's own ACLs. Which grant each entry
        // needs is its channel's scheme's business, checked per entry below.
        let policy = self
            .targets
            .registration(&SubscriberEntryKind::Surface(slug.to_string()))
            .map(|r| r.policy.as_ref())
            .unwrap_or_else(|| {
                panic!(
                    "publish_batch_from_surface: surface {slug:?} has no registered policy — a \
                     bound output implies one, so this is a broken boot invariant"
                )
            });

        let sender_id = surface_principal(slug, component);
        let sender = sender_id.as_str().to_owned();
        let source = self.source.as_ref();

        info!(
            surface = %slug,
            principal = %sender,
            publish_count = publishes.len(),
            "publish_batch_from_surface: applying activation flush"
        );

        // The split is the channel's, not the caller's: a bound output's
        // capabilities decide whether its entry belongs in the transaction
        // below or on a ring.
        let mut durable: Vec<&SurfaceBatchPublish<'_>> = Vec::new();
        let mut nondurable: Vec<(&SurfaceBatchPublish<'_>, Arc<ChannelEntry>)> = Vec::new();
        for publish in publishes {
            let addr = publish.channel_address;
            let channel = self.directory.resolve(addr).unwrap_or_else(|| {
                panic!(
                    "publish_batch_from_surface: bound output {addr:?} of surface {slug:?} is not \
                     in the directory — boot validation proves every bound output exists, so this \
                     is a broken boot invariant"
                )
            });
            if channel.capabilities().durable {
                durable.push(publish);
            } else {
                nondurable.push((publish, channel));
            }
        }

        // Deferred durable surface feeds: built under the lock, fanned out
        // after release. See `publish_from_wasm`.
        let mut surface_feeds: Vec<(Arc<super::MessageEnvelope>, i64, Vec<SurfaceFeedTarget>)> =
            Vec::new();
        // Schedules the deferred cap refused, warned about after the lock is
        // released so the transaction is not held across the logging.
        let mut refused: Vec<(String, u64)> = Vec::new();

        {
            let conn = self.db.lock().await;
            let tx = conn
                .unchecked_transaction()
                .expect("publish_batch_from_surface: begin tx");

            // Per-batch memoization of the surface-feed resolution, as in
            // `publish_from_wasm`: the directory is immutable and the lock is held
            // throughout, so targets cannot change mid-batch, and the dominant
            // case is a batch fanning one port.
            let mut targets_cache: HashMap<&str, ResolvedChannelTargets> = HashMap::new();

            for publish in &durable {
                let addr = publish.channel_address;
                let (channel, feed_targets) = targets_cache.entry(addr).or_insert_with(|| {
                    let name = well_formed_name(addr, ChannelScheme::Brenn).unwrap_or_else(|| {
                        panic!(
                            "publish_batch_from_surface: bound output {addr:?} of surface \
                                     {slug:?} is not a well-formed brenn: address — boot resolved \
                                     it, so this is a broken boot invariant"
                        )
                    });
                    assert!(
                        publish_acl_allows(policy, ChannelScheme::Brenn, name),
                        "publish_batch_from_surface: surface {slug:?} has no brenn_publish ACL \
                             covering bound output {addr:?} — boot validation proves every bound \
                             output is policy-covered, so this is a broken boot invariant"
                    );
                    let ch = self.directory.resolve(addr).unwrap_or_else(|| {
                        panic!(
                            "publish_batch_from_surface: bound output {addr:?} of surface \
                                 {slug:?} is not in the directory — boot validation proves every \
                                 bound output exists, so this is a broken boot invariant"
                        )
                    });
                    let feed =
                        self.resolve_surface_feed_targets(&ch.address, ch.subscribers.as_slice());
                    (ch, feed)
                });

                // The caller answered the client a violation for an over-cap body
                // before reaching here (the kernel's own buffer-time gate already
                // returned the component `invalid-payload`), so a breach at this
                // point is the transport and bus caps disagreeing — the same
                // config-wiring bug the single-publish path screams about, except
                // that here there is no per-entry outcome to carry it back and an
                // oversized row would already be committed with its siblings.
                if let Err(e) = check_body_size(publish.body, self.defaults.max_body_bytes) {
                    panic!(
                        "publish_batch_from_surface: entry body is {} bytes over the {} cap for \
                         surface {slug:?} — the session handler rejects an over-cap entry as a \
                         violation before this point, so the two caps disagree",
                        e.len, e.max
                    );
                }

                // Refused → the schedule is dropped and the batch carries on.
                if let Some(release_at) = publish.deliver_after {
                    if let Some(cap) = db::deferred_cap_refusal(
                        &tx,
                        channel.uuid,
                        channel.resolved_channel.retain_depth,
                    ) {
                        refused.push((channel.address.clone(), cap));
                        continue;
                    }
                    self.insert_message(
                        &tx,
                        channel,
                        ChannelScheme::Brenn,
                        source,
                        &sender,
                        publish.body,
                        publish.urgency,
                        publish.publish_ts_ns,
                        None, // no reply_to — not exposed to surfaces
                        Some(release_at),
                        None, // no delivery_deadline
                    );
                    // Not fed now: a parked message is not observable before
                    // release, and the release sweep does its own fan-out.
                    continue;
                }

                let inserted = self.insert_message(
                    &tx,
                    channel,
                    ChannelScheme::Brenn,
                    source,
                    &sender,
                    publish.body,
                    publish.urgency,
                    publish.publish_ts_ns,
                    None, // no reply_to — not exposed to surfaces
                    None,
                    None, // no delivery_deadline
                );
                if !feed_targets.is_empty()
                    && self
                        .router
                        .any_surface_session_subscribed(&channel.address, feed_targets)
                {
                    surface_feeds.push((
                        Arc::new(surface_feed_envelope(
                            inserted.uuid,
                            source.to_owned(),
                            channel.address.clone(),
                            sender.clone(),
                            publish.publish_ts_ns,
                            publish.body.to_owned(),
                            None,
                            None,
                            None,
                            None,
                            publish.urgency,
                            ChannelScheme::Brenn,
                        )),
                        inserted.retained_seq.expect(
                            "publish: an unparked durable message holds a retention position",
                        ),
                        feed_targets.clone(),
                    ));
                }
            }

            tx.commit().expect("publish_batch_from_surface: commit tx");
        }

        // Durable surface feeds, fanned out after the lock is released.
        for (envelope, seq, targets) in surface_feeds {
            self.fan_out_surface_feed(&targets, envelope, seq).await;
        }

        // The ring half, after the durable lock is gone. Each entry takes the
        // prepaid entry points — same gates, same envelope mint, same live
        // feed — because the batch was already paid for as a whole and nothing
        // downstream may refuse it. Its destination is memoized per address, as
        // the durable half above memoizes its targets and for the same reason:
        // the dominant case is a batch fanning one port.
        let mut destinations: HashMap<&str, PrepaidDestination> = HashMap::new();
        for (publish, channel) in &nondurable {
            let destination = destinations
                .entry(publish.channel_address)
                .or_insert_with(|| {
                    self.resolve_prepaid(&sender_id, policy, publish.channel_address)
                });
            let prepaid = PrepaidEntry {
                body: publish.body,
                urgency: publish.urgency,
                publish_ts: db::ns_to_utc(publish.publish_ts_ns),
            };
            match publish.deliver_after {
                Some(release_at) => {
                    if let Err(brenn_queue::QuotaExceeded { cap }) =
                        self.park_prepaid(destination, prepaid, release_at)
                    {
                        refused.push((channel.address.clone(), cap));
                    }
                }
                None => {
                    let appended = self.publish_prepaid(destination, prepaid).await;
                    self.enact_overflow_events(channel, &appended.overflow);
                }
            }
        }

        // A dropped schedule is a component that never wakes when it meant to, so
        // it is loud and counted even though it is not an error — a health check
        // reads the counter, an operator reads the line.
        //
        // TODO(deferred-flush-drop-signal): the WASM flush ends in the twin of
        // this loop; the consumer-visible drop signal that TODO adds serves both,
        // so the two move together.
        for (channel, cap) in &refused {
            self.record_dropped_deferred(&sender, channel);
            warn!(
                surface = %slug,
                principal = %sender,
                channel = %channel,
                cap,
                "publish_batch_from_surface: deferred publish dropped — channel deferred set at \
                 its retain_depth cap"
            );
        }

        self.dispatch_kick();
        refused.len()
    }
}

/// Validate the shape of a `brenn:` channel address: the prefix must be
/// present, the remainder non-empty and drawn from the URL-safe
/// unreserved-character class. Thin wrapper over `well_formed_name` for the
/// external callers that only need the yes/no shape verdict.
pub fn is_well_formed_address(addr: &str) -> bool {
    well_formed_name(addr, ChannelScheme::Brenn).is_some()
}

/// Outcome of a single `dispatch_row` call. Successful delivery
/// returns the `push_id` so the caller can batch mark-delivered writes;
/// `Parked` covers all three "leave undelivered" cases (no active
/// bridge, no-wake subscription, bridge-died-mid-send).
#[derive(Debug, PartialEq, Eq)]
pub enum DispatchOutcome {
    /// The bridge accepted the send. Caller must mark this push delivered.
    Delivered(i64),
    /// The push remains undelivered. Drain-on-wake (or the deadline /
    /// deliver-after scanners) will pick it up. `woke` reports whether this
    /// dispatch actually fired an eager wake, for the dispatcher's own
    /// debug log.
    Parked { woke: bool },
}

#[cfg(test)]
mod tests;
