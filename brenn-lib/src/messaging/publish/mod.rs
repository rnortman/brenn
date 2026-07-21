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
pub const SURFACE_SEND_REFILL: Duration = Duration::from_secs(15);

use crate::auth::user::get_user_by_username;
use crate::conversation::get_or_create_singleton_conversation;

use super::db::{
    self, BudgetDecrement, InsertedMessage, PendingPushInsert, decrement_send_budget,
    delete_pending_push_by_id, insert_ingress_message, insert_message_with_pushes_in_tx,
};
use super::gates::{
    check_body_size, publish_acl_allows, reply_to_visible, resolve_publish_sender, well_formed_name,
};
use super::{
    ChannelScheme, EphemeralPublishResult, Messenger, ParticipantId, SubscriberEntryKind, Urgency,
    WakeEconomics, WakeMin,
    config::{Depth, NoiseLevel},
};
use crate::access::AppCapability;
use crate::obs::security::DenialKind;

/// Per-channel memoized resolution for a batch flush: the channel entry, its
/// push targets, and its fold-0 surface context-feed targets (design §6).
type ResolvedChannelTargets = (
    Arc<super::ChannelEntry>,
    Vec<PushTarget>,
    Vec<SubscriberEntryKind>,
);

/// Outcome of `Messenger::publish`. Maps directly to the success / failure
/// JSON returned to CC by the `MessageSend` PostToolUse intercept.
///
/// The six variants match the design (§6.3 / §3.2). `MalformedAddress`
/// covers shape errors on either `to` or `reply_to` (missing
/// `brenn:` prefix, disallowed characters). `UnknownChannel` covers
/// well-formed addresses that don't resolve to a registered channel,
/// for either `to` or `reply_to`.
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
    /// Sender app holds no `MessagingPublish` grant (Phase-2 publish/subscribe
    /// split, design §2.5): the publish path now gates on `MessagingPublish`
    /// specifically, not the participation `OR`. A `messaging_subscribe`-only app
    /// is `MissingSender` here. This is a layer-1 (grant) absence — distinct from
    /// `AclDenied`, which is a layer-2 (ACL scope) denial with the grant held.
    MissingSender,
    /// Sender app holds `MessagingPublish` but the target `brenn:` channel is not
    /// covered by any `brenn_publish` ACL matcher (layer-2 deny, design §2.2).
    /// Distinct from `MissingSender` (layer-1 grant absence) so the LLM-facing
    /// error and automation-outcome class can name the *allowlist*, not the
    /// grant. Carries the offending address (`brenn:<channel>`). Budget is not
    /// consumed.
    AclDenied(String),
    /// Body length > `max_body_bytes`. Budget is not consumed.
    BodyTooLarge { len: usize, max: usize },
}

impl PublishResult {
    /// Kind tag for every denial that warrants an intercept-level security
    /// signal, mirroring `EphemeralPublishResult::signal_kind`. A caller that
    /// signals durable denials derives the log `kind` field from this method.
    ///
    /// `Ok` and `BudgetExhausted` return `None`: `BudgetExhausted` is a normal
    /// operational condition with its own LLM-facing recovery path, not a
    /// policy denial (the analog of ephemeral `RateLimited`).
    pub fn signal_kind(&self) -> Option<DenialKind> {
        match self {
            Self::MalformedAddress(_) => Some(DenialKind::MalformedAddress),
            Self::UnknownChannel(_) => Some(DenialKind::UnknownChannel),
            Self::MissingSender => Some(DenialKind::MissingSender),
            Self::AclDenied(_) => Some(DenialKind::AclDenied),
            Self::BodyTooLarge { .. } => Some(DenialKind::BodyTooLarge),
            Self::Ok { .. } | Self::BudgetExhausted => None,
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

/// Outcome of `Messenger::publish_any`: the union of the durable and ephemeral
/// pipeline results. The scheme of the target address selects the arm — a
/// `brenn:` (or any non-`ephemeral:`) address routes to `publish` (`Durable`);
/// an `ephemeral:` address routes to the `EphemeralBus` (`Ephemeral`).
#[derive(Debug)]
pub enum AnyPublishResult {
    Durable(PublishResult),
    Ephemeral(EphemeralPublishResult),
}

/// Identifies the publisher for the send-budget gate.
#[derive(Debug, Clone, Copy)]
pub enum PublishOrigin {
    /// LLM/automation publish attributed to a conversation; the per-conversation
    /// send budget applies (decrement_send_budget).
    Conversation { id: i64 },
    /// In-process system publisher (no conversation). No send budget — flood
    /// protection is upstream at the caller. `BudgetExhausted` is
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
}

impl Messenger {
    /// Scheme-dispatching publish entry point above the two pipelines. The
    /// app-facing surface (LLM `MessageSend` intercept, later automation/WASM)
    /// calls this; the target address scheme picks the pipeline.
    ///
    /// `ephemeral:` addresses route to the `EphemeralBus`; everything else routes
    /// to `publish` unchanged (including its own handling of unknown schemes as
    /// `MalformedAddress`). The ephemeral arm rejects the durable-only option
    /// fields fail-fast rather than silently dropping them, resolves the sender
    /// slug to its policy (layer-1 grant gate), then calls the bus with the
    /// resolved principal. `origin` is durable-only (budget bookkeeping) and
    /// unused on the ephemeral arm.
    #[allow(clippy::too_many_arguments)]
    pub async fn publish_any(
        &self,
        origin: PublishOrigin,
        sender_app_slug: &str,
        addr: &str,
        body: &str,
        urgency: super::Urgency,
        reply_to: Option<&str>,
        deliver_after: Option<DateTime<Utc>>,
        delivery_deadline: Option<DateTime<Utc>>,
    ) -> AnyPublishResult {
        if !matches!(ChannelScheme::of(addr), Some(ChannelScheme::Ephemeral)) {
            return AnyPublishResult::Durable(
                self.publish(
                    origin,
                    sender_app_slug,
                    addr,
                    body,
                    urgency,
                    reply_to,
                    deliver_after,
                    delivery_deadline,
                )
                .await,
            );
        }

        // Ephemeral arm. Durable-only options are unsupported on `ephemeral:`
        // targets — reject fail-fast rather than silently dropping.
        if reply_to.is_some() {
            return AnyPublishResult::Ephemeral(EphemeralPublishResult::UnsupportedOption {
                field: "reply_to",
            });
        }
        if deliver_after.is_some() {
            return AnyPublishResult::Ephemeral(EphemeralPublishResult::UnsupportedOption {
                field: "deliver_after",
            });
        }
        if delivery_deadline.is_some() {
            return AnyPublishResult::Ephemeral(EphemeralPublishResult::UnsupportedOption {
                field: "delivery_deadline",
            });
        }

        // Layer-1 sender gate: app exists and holds `EphemeralPublish`. Mirrors
        // the durable split — layer-2 (ACL) lives inside `EphemeralBus::publish`.
        let app = match resolve_publish_sender(
            &self.apps,
            sender_app_slug,
            AppCapability::EphemeralPublish,
        ) {
            Some(a) => a,
            None => return AnyPublishResult::Ephemeral(EphemeralPublishResult::MissingSender),
        };

        let sender = ParticipantId::for_app(sender_app_slug, &self.source);
        AnyPublishResult::Ephemeral(self.ephemeral_bus.publish(
            &sender,
            &app.policy,
            addr,
            body,
            urgency,
        ))
    }

    /// Publish a message on behalf of a CC subprocess.
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
    /// `reply_to`/`deliver_after`/`delivery_deadline` — not exposed to surfaces in
    /// v1.
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
    pub fn draw_surface_send_budget_for_batch(
        &self,
        slug: &str,
        component: &str,
        tokens: u32,
    ) -> SurfaceSendVerdict {
        let principal = ParticipantId::for_surface_component(slug, component);
        self.draw_surface_send_budget(
            SurfaceSendDraw {
                slug,
                component: Some(component),
                principal: principal.as_str(),
                channel: None,
                tokens,
            },
            "activation batches",
        )
    }

    /// Shared durable-publish gate sequence behind `publish`,
    /// `publish_from_surface`, and `publish_from_system`. The `principal`
    /// selects the layer-1 authority source (app vs surface vs system); every
    /// other gate is identical across arms.
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
    ) -> PublishResult {
        // 1. Validate address shape, then resolve. Shape errors return
        //    `MalformedAddress` (per design §3.2 / §6.3); well-formed
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
            match well_formed_name(addr, ChannelScheme::Brenn) {
                Some(name) => name,
                None => return PublishResult::MalformedAddress(addr.to_string()),
            }
        };
        let channel = match self.directory.resolve(addr) {
            Some(c) => c,
            None => return PublishResult::UnknownChannel(addr.to_string()),
        };

        // AUTHZ WARNING (security-5): the per-channel sender authorization
        // (allowlist) below is now LIVE (Phase 2, design §2.2). The automation
        // fire path (`automation/fire.rs`, `fire_one`) re-checks this same policy
        // at fire time (design §2.3, Seam B). Automation jobs store `action.to` at
        // create time and fire later; a policy tightened after job creation would
        // be stale at fire time unless that re-check is present.

        // 2. Sender authority + Phase-2 publish authorization (design §2.2, Seam
        //    A), resolved per principal source. Layer-1: gate on the
        //    `MessagingPublish` grant specifically — NOT `messaging_enabled()`
        //    (the participation `OR`). This is the publish/subscribe split
        //    (design §2.5): a `messaging_subscribe`-only sender is `MissingSender`
        //    here. Yields the policy (for the layer-2 ACL), the stored principal
        //    string, and the optional `Conversation`-origin send budget:
        //    `Some(budget)` for an app (read in the `Conversation` arm of step 5;
        //    falls back to the global default for an app with no `[app.messaging]`
        //    block), `None` for the always-`System` surface arm.
        let (policy, sender, conversation_send_budget) = match principal {
            PublishPrincipal::App { slug } => {
                let app =
                    match resolve_publish_sender(&self.apps, slug, AppCapability::MessagingPublish)
                    {
                        Some(a) => a,
                        None => return PublishResult::MissingSender,
                    };
                (
                    &app.policy,
                    ParticipantId::for_app(slug, &self.source)
                        .as_str()
                        .to_owned(),
                    Some(app.messaging_send_budget()),
                )
            }
            PublishPrincipal::Surface {
                slug, component, ..
            } => {
                // Surfaces are not in `self.apps`; their boot-resolved policy
                // lives in the unified `subscribers` registry, the same
                // authority the delivery-time gate reads via `subscriber_policy`.
                //
                // Keyed at the surface grain (`instance: None`) for a component
                // publish too: a component's grants are its config-declared
                // bindings, and boot validation already proved each one is covered
                // by the surface's own ACLs. The sub-identity finer-grains
                // attribution and budget, not authority — there is no per-instance
                // policy blob to hand-maintain, so the instance-grain registrations
                // boot installs for the delivery gate carry this same policy.
                let policy = match self
                    .subscribers
                    .get(&SubscriberEntryKind::Surface {
                        slug: slug.to_string(),
                        instance: None,
                    })
                    .map(|r| r.policy.as_ref())
                    .filter(|p| p.has_grant(AppCapability::MessagingPublish))
                {
                    Some(p) => p,
                    None => return PublishResult::MissingSender,
                };
                (
                    policy,
                    match component {
                        Some(instance) => ParticipantId::for_surface_component(slug, instance),
                        None => ParticipantId::for_surface(slug),
                    }
                    .as_str()
                    .to_owned(),
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
                    .subscribers
                    .get(&SubscriberEntryKind::System(component.to_string()))
                    .map(|r| r.policy.as_ref())
                    .filter(|p| p.has_grant(AppCapability::MessagingPublish))
                {
                    Some(p) => p,
                    None => return PublishResult::MissingSender,
                };
                (
                    policy,
                    ParticipantId::for_system(component).as_str().to_owned(),
                    // System is always paired with a `System` origin, which never
                    // reads the budget below — same structural `None` as Surface.
                    None,
                )
            }
        };
        // Layer-2: per-channel `brenn_publish` ACL against the bare channel name
        // captured at gate 1. This is a pure in-memory policy read against the
        // already-resolved channel and runs BEFORE the budget decrement / DB work,
        // so an out-of-scope publish consumes no budget and takes no lock.
        if !publish_acl_allows(policy, ChannelScheme::Brenn, channel_name) {
            return PublishResult::AclDenied(addr.to_string());
        }

        // 3. Body length.
        if let Err(e) = check_body_size(body, self.defaults.max_body_bytes) {
            return PublishResult::BodyTooLarge {
                len: e.len,
                max: e.max,
            };
        }

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
        if let PublishPrincipal::Surface {
            slug,
            component,
            platform: false,
        } = principal
            && matches!(
                self.draw_surface_send_budget(
                    SurfaceSendDraw {
                        slug,
                        component,
                        principal: &sender,
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

        // 4. Resolve reply_to (if any): shape → visibility → resolve. Shape
        //    errors return `MalformedAddress`. The visibility gate runs BEFORE
        //    resolution so an out-of-visibility reply_to fails identically
        //    whether or not the channel exists — closing the success/failure
        //    existence oracle a plain resolve would open. Visibility is the
        //    union of the sender's publish allowlist and its delivery scope:
        //    channels it could name in `to`, plus channels it could legitimately
        //    learn about as a subscriber (a reply target is a channel the sender
        //    expects to hear replies on). Out-of-scope → `AclDenied`;
        //    in-scope-but-unresolved → `UnknownChannel`.
        let reply_to_uuid = if let Some(rt_addr) = reply_to {
            let rt_name = match well_formed_name(rt_addr, ChannelScheme::Brenn) {
                Some(name) => name,
                None => return PublishResult::MalformedAddress(rt_addr.to_string()),
            };
            let visible = reply_to_visible(policy, ChannelScheme::Brenn, rt_name, rt_addr);
            if !visible {
                return PublishResult::AclDenied(rt_addr.to_string());
            }
            match self.directory.resolve(rt_addr) {
                Some(c) => Some(c.uuid),
                None => return PublishResult::UnknownChannel(rt_addr.to_string()),
            }
        } else {
            None
        };

        // 5. DB work: budget decrement (creates row if needed) + resolve push
        //    targets + insert message + insert pending pushes + push-window
        //    retirement, all in a single lock scope. A single SQLite mutex
        //    serializes concurrent publishers; the `UPDATE ... WHERE remaining > 0`
        //    row count is the authoritative gate. The budget decrement applies
        //    only to a `Conversation` origin; a `System` origin has no send
        //    budget and skips the row entirely (no INSERT, no FK exposure).
        let publish_ts_ns = db::utc_to_ns(Utc::now());
        let (message, remaining_budget, context_targets) = {
            let conn = self.db.lock().await;
            let remaining = match origin {
                PublishOrigin::Conversation { id } => {
                    let budget = conversation_send_budget.expect(
                        "Conversation origin requires a send budget — only App principals \
                         produce Conversation-origin publishes",
                    );
                    match decrement_send_budget(&conn, id, budget) {
                        BudgetDecrement::Ok { remaining } => Some(remaining),
                        BudgetDecrement::Exhausted => return PublishResult::BudgetExhausted,
                    }
                }
                PublishOrigin::System => None,
            };
            // Resolution must happen under the same lock as the insert to avoid a TOCTOU
            // window (a channel may gain subscribers between resolution and insert).
            let push_targets =
                self.resolve_push_targets(&conn, &channel.address, channel.subscribers.as_slice());
            let context_targets =
                self.resolve_context_targets(&channel.address, channel.subscribers.as_slice());
            let tx = conn
                .unchecked_transaction()
                .expect("messaging: begin publish tx");
            let (inserted, plan) = self.insert_pushes(
                &tx,
                &channel,
                &push_targets,
                ChannelScheme::Brenn,
                self.source.as_ref(),
                &sender,
                body,
                urgency,
                publish_ts_ns,
                reply_to_uuid,
                deliver_after,
                delivery_deadline,
            );
            tx.commit().expect("messaging: commit publish tx");
            self.retire_windows(&conn, &plan);
            (inserted, remaining, context_targets)
        };

        // Durable depth-0 context feed: hand the committed envelope to attached
        // fold-0 surface subscriptions as a row-less live delivery (design §6).
        // After the lock is released — nothing owed to a disconnected session.
        //
        // A deferred row (`deliver_after` in the future) is not fed yet: the feed
        // is the wire analogue of publish-time delivery for immediate rows, and a
        // deferred message must not be observable before its release (the push
        // path parks such a row on `release_after`). The fold-0 subscriber is owed
        // nothing now; its retained window covers the row at the next attach.
        let feed_due = deliver_after.is_none_or(|da| da <= Utc::now());
        if feed_due
            && !context_targets.is_empty()
            && self
                .router
                .any_context_session_attached(&channel.address, &context_targets)
        {
            let envelope = context_feed_envelope(
                message.uuid,
                self.source.as_ref().to_owned(),
                channel.address.clone(),
                sender.clone(),
                publish_ts_ns,
                body.to_owned(),
                reply_to.map(|s| s.to_owned()),
                delivery_deadline,
                deliver_after,
                urgency,
            );
            self.fan_out_context_feed(&context_targets, &envelope, message.id)
                .await;
        }

        // 7. Kick background tasks if we inserted suppressed or
        //    deadline-bearing rows.
        if let Some(da) = deliver_after
            && da > Utc::now()
        {
            self.dispatch_kick();
        }
        if delivery_deadline.is_some() {
            self.dispatch_kick();
        }

        // 8. Signal the background dispatcher. All dispatch is off-stack (R1).
        self.dispatch_kick();

        PublishResult::Ok {
            message_id: message.uuid,
            address: channel.address.clone(),
            remaining_budget,
        }
    }

    /// Resolve push targets for an outbound publish: per-subscriber, find
    /// the (singleton-app, allowed_user) → conversation_id mapping that
    /// the dispatcher will inject into. Also resolves the noise level for
    /// each subscriber from `SubscriberEntry.noise`.
    ///
    /// Accepts a caller-held `&Connection` so resolution and the subsequent
    /// insert happen under the same lock acquisition — avoiding a TOCTOU window
    /// where a channel could gain subscribers between resolution and insert.
    ///
    /// `channel_address` is the channel's stored address (`mqtt:`/`brenn:`/
    /// `webhook:`); it backs the **delivery-time ACL gate** (design §2.2,
    /// "Enforcement point A"). Every `App`/`Wasm` subscriber — regardless of how
    /// the subscription was created — is re-authorized against its current
    /// `AppPolicy` via `subscriber_policy` + `allows_channel_access`. A subscriber whose
    /// policy no longer covers the channel (ACL removed, transport grant gone, or —
    /// a wiring bug — no policy at all) is **skipped**: it is not pushed and not
    /// persisted as a pending push, with a `warn` revocation signal. The gate is
    /// uniform; there is no static/dynamic branch.
    fn resolve_push_targets(
        &self,
        conn: &rusqlite::Connection,
        channel_address: &str,
        subscribers: &[crate::messaging::SubscriberEntry],
    ) -> Vec<PushTarget> {
        let mut targets = Vec::with_capacity(subscribers.len());
        for sub in subscribers {
            let push_depth = sub.push_depth;
            // depth-0 subs aren't push targets: no push row is ever created for
            // them. A fold-0 *surface* subscriber instead gets a row-less
            // deliver-if-attached context feed via `resolve_context_targets` +
            // `WakeRouter::deliver_context`, run after this transaction commits.
            if !push_depth.is_push_enabled() {
                continue;
            }
            // Delivery-time ACL gate (design §2.2 Point A), uniform over App + Wasm,
            // static + dynamic. A missing policy for a live subscriber is a host
            // wiring bug — fail closed (deny) rather than panic on the delivery path.
            let allowed = self
                .subscriber_policy(&sub.kind)
                .is_some_and(|p| p.allows_channel_access(channel_address));
            if !allowed {
                warn!(
                    app = %sub.kind.slug(),
                    channel = %channel_address,
                    "subscription delivery denied — ACL not satisfied"
                );
                continue;
            }
            // Declared wake economics for this subscriber, resolved per participant
            // (App from `self.apps`, others from the registry) — never inferred from
            // the identity prefix. `Eager` subscribers are woken on every publish;
            // `UrgencyGated` subscribers consult `wake_min`. A subscriber that just
            // passed the ACL gate always resolves here (same source), so a `None` is
            // a host-wiring invariant violation, not a routine outcome — surface it
            // and skip delivery, exactly like the missing-app/user cases below.
            // Silently defaulting to `UrgencyGated` here would re-park a live `Eager`
            // subscriber — the precise stranding this resolution exists to prevent —
            // and hide the wiring bug behind an ordinary designed-park.
            let wake = match self.subscriber_wake_economics(&sub.kind) {
                Some(w) => w,
                None => {
                    warn!(
                        subscriber = ?sub.kind,
                        channel = %channel_address,
                        "subscriber passed ACL gate but has no wake-economics \
                         registration — host wiring bug; skipping delivery"
                    );
                    continue;
                }
            };
            // Only `UrgencyGated` targets ever consult a wake threshold; an
            // `Eager` target carries `None`, making "no eager delivery reads a
            // wake_min" a type-enforced invariant on the delivery path rather
            // than a convention. `SubscriberEntry.wake_min` already carries
            // `Some` iff `UrgencyGated`; forward it unchanged.
            let push_wake_min = match wake {
                WakeEconomics::UrgencyGated => sub.wake_min,
                WakeEconomics::Eager => None,
            };
            match &sub.kind {
                SubscriberEntryKind::App(slug) => {
                    // These three lookups should always succeed for a subscriber
                    // that just passed the ACL gate: its policy resolved via
                    // `subscriber_policy`, so the app is wired. A `None` here is a
                    // host-wiring invariant violation, NOT a deny-by-default
                    // outcome — surface it (errhandling-1) so a wiring bug after
                    // the gate is distinguishable from a successful delivery and
                    // from a normal ACL revocation.
                    let app = match self.apps.get(slug) {
                        Some(a) => a,
                        None => {
                            warn!(
                                app = %slug,
                                channel = %channel_address,
                                "subscriber passed ACL gate but app not found in apps map — \
                                 host wiring bug; skipping delivery"
                            );
                            continue;
                        }
                    };
                    let noise = sub.noise;
                    // Singleton + 1 allowed_user is enforced by config validation.
                    let username = match app.allowed_users.first() {
                        Some(u) => u.clone(),
                        None => {
                            warn!(
                                app = %slug,
                                channel = %channel_address,
                                "resolved app has no allowed_users — host wiring/config bug; \
                                 skipping delivery"
                            );
                            continue;
                        }
                    };
                    let user = match get_user_by_username(conn, &username) {
                        Some(u) => u,
                        None => {
                            warn!(
                                app = %slug,
                                channel = %channel_address,
                                username = %username,
                                "allowed_user not found in users table — host wiring bug; \
                                 skipping delivery"
                            );
                            continue;
                        }
                    };
                    let conversation = get_or_create_singleton_conversation(conn, user.id, slug);
                    targets.push(PushTarget {
                        subscriber: ParticipantId::for_conversation(conversation.id),
                        app_slug: slug.clone(),
                        push_depth,
                        noise,
                        wake,
                        wake_min: push_wake_min,
                    });
                }
                SubscriberEntryKind::Wasm(slug) => {
                    // WASM consumers do not go through self.apps / singleton-conversation.
                    // Push target subscriber is the wasm: ParticipantId directly.
                    // Noise is read from SubscriberEntry.noise — populated by
                    // finalize_directory_with_subscribers from the resolved
                    // [[wasm_consumer.subscription]] noise level (design §2.5 #3).
                    targets.push(PushTarget {
                        subscriber: ParticipantId::for_wasm(slug),
                        app_slug: slug.clone(),
                        push_depth,
                        noise: sub.noise,
                        wake,
                        wake_min: push_wake_min,
                    });
                }
                SubscriberEntryKind::Surface { slug, instance } => {
                    // Surfaces reach durable dispatch via the surface:
                    // ParticipantId directly (no self.apps / singleton
                    // conversation). The push window is keyed on the subscribing
                    // principal — a component instance's own sub-identity, or the
                    // bare surface for the kernel's layout subscription — so each
                    // principal's lag is tracked and bounded independently.
                    // Eager-wake is derived from wake_min downstream (WakeRouter),
                    // same as every other kind.
                    let subscriber = match instance {
                        Some(instance) => ParticipantId::for_surface_component(slug, instance),
                        None => ParticipantId::for_surface(slug),
                    };
                    targets.push(PushTarget {
                        app_slug: subscriber.as_surface_subscriber_key().to_owned(),
                        subscriber,
                        push_depth,
                        noise: sub.noise,
                        wake,
                        wake_min: push_wake_min,
                    });
                }
                SubscriberEntryKind::System(component) => {
                    // System-substrate subscribers reach durable dispatch via the
                    // system: ParticipantId directly (no self.apps / singleton
                    // conversation), parked-and-woken like the Wasm arm.
                    targets.push(PushTarget {
                        subscriber: ParticipantId::for_system(component),
                        app_slug: component.clone(),
                        push_depth,
                        noise: sub.noise,
                        wake,
                        wake_min: push_wake_min,
                    });
                }
            }
        }
        targets
    }

    /// The fold-0 (depth-0) surface subscribers on a channel — the row-less
    /// context-feed targets (design §6). A depth-0 subscription creates no push
    /// row, so `resolve_push_targets` skips it; a surface session nonetheless
    /// gets a live deliver-if-attached fan-out of durable messages while
    /// attached. Only surface subscribers take the feed: a depth-0 App/Wasm/
    /// System subscriber has no wire session to deliver to live.
    ///
    /// Runs the same delivery-time ACL gate as `resolve_push_targets` — a
    /// subscriber whose policy no longer covers the channel is not fed. Returns
    /// the surface subscriber keys; the caller builds the envelope once and hands
    /// each to `WakeRouter::deliver_context` after commit.
    fn resolve_context_targets(
        &self,
        channel_address: &str,
        subscribers: &[crate::messaging::SubscriberEntry],
    ) -> Vec<SubscriberEntryKind> {
        let mut out = Vec::new();
        for sub in subscribers {
            if sub.push_depth.is_push_enabled() {
                continue;
            }
            if !matches!(sub.kind, SubscriberEntryKind::Surface { .. }) {
                continue;
            }
            let allowed = self
                .subscriber_policy(&sub.kind)
                .is_some_and(|p| p.allows_channel_access(channel_address));
            if !allowed {
                debug!(
                    subscriber = ?sub.kind,
                    channel = %channel_address,
                    "depth-0 surface context feed denied — ACL not satisfied"
                );
                continue;
            }
            out.push(sub.kind.clone());
        }
        out
    }

    /// Fan a just-committed durable envelope to fold-0 surface subscribers as a
    /// row-less live feed (design §6). Called after the publish transaction
    /// commits and its lock is released — `deliver_context` touches no DB and
    /// only enqueues onto attached sessions, so nothing owed to a disconnected
    /// one. A no-op when there are no context targets.
    async fn fan_out_context_feed(
        &self,
        targets: &[SubscriberEntryKind],
        envelope: &super::MessageEnvelope,
        seq: i64,
    ) {
        // Build the shared envelope once; each `deliver_context` clones only the
        // `Arc` (a refcount bump), never the payload, however many fold-0
        // subscribers the channel carries.
        let shared = Arc::new(envelope.clone());
        for key in targets {
            self.router.deliver_context(key, &shared, seq).await;
        }
    }
}

/// Build the row-less durable depth-0 context-feed envelope for a just-committed
/// message (design §6). Single definition of the envelope shape shared by the
/// ad-hoc publish and both batch flush paths, so a new envelope field is wired
/// in one place rather than three.
#[allow(clippy::too_many_arguments)]
fn context_feed_envelope(
    message_id: Uuid,
    source: String,
    channel: String,
    sender: String,
    publish_ts_ns: i64,
    body: String,
    reply_to: Option<String>,
    delivery_deadline: Option<DateTime<Utc>>,
    deliver_after: Option<DateTime<Utc>>,
    urgency: Urgency,
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
        urgency,
        envelope_type: ChannelScheme::Brenn,
    }
}

/// Resolved push-target metadata used inside `publish`.
struct PushTarget {
    subscriber: ParticipantId,
    app_slug: String,
    push_depth: Depth,
    /// Noise level for this subscription (used for push-overflow handling).
    noise: NoiseLevel,
    /// Declared wake economics for this subscriber. `Eager` ⇒ every push row is
    /// created eager (`wake_min` ignored); `UrgencyGated` ⇒ `eager_wake` gated by
    /// `wake_min.wakes(urgency)`.
    wake: WakeEconomics,
    /// Wake-min threshold for this subscription. `Some` iff `wake` is
    /// `UrgencyGated` (the only case that consults it); `None` for `Eager`
    /// targets, so the delivery path cannot read a threshold for a subscriber
    /// whose economics never gate on one.
    wake_min: Option<WakeMin>,
}

/// Per-target entry in a `RetirementPlan`.
///
/// Carries the push-row id and per-subscriber metadata. Channel address and
/// uuid are stored once per plan (in `RetirementPlan`) rather than once per
/// entry, reducing allocations under the global DB lock from N×M to M (where
/// N = subscriber count, M = message count per flush).
struct RetirementPlanEntry {
    push_id: i64,
    app_slug: String,
    subscriber: super::ParticipantId,
    push_depth: super::config::Depth,
    noise: super::config::NoiseLevel,
}

/// Retirement plan for one published message: channel identity (shared across all
/// targets) plus per-target entries. Passed to `retire_windows` after commit.
struct RetirementPlan {
    /// Channel address (for push-window keying). Cloned once per message, not per target.
    channel_address: String,
    channel_uuid: Uuid,
    entries: Vec<RetirementPlanEntry>,
}

impl Messenger {
    /// Insert one message + its pending-push rows under a caller-owned
    /// transaction. Returns the `InsertedMessage` and the retirement plan
    /// for the push-window overflow step (`retire_windows`).
    ///
    /// The caller is responsible for:
    /// - Holding the DB lock (`conn` / the transaction).
    /// - Committing the transaction after all `insert_pushes` calls for the batch.
    /// - Calling `retire_windows` for each returned plan after the commit.
    ///
    /// `track_in_window` is `false` for parked (`deliver_after` in the future)
    /// rows — those must not be retired before they are ever delivered (design §3).
    #[allow(clippy::too_many_arguments)]
    fn insert_pushes(
        &self,
        tx: &rusqlite::Transaction<'_>,
        channel: &super::ChannelEntry,
        push_targets: &[PushTarget],
        envelope_type: ChannelScheme,
        source: &str,
        sender: &str,
        body: &str,
        urgency: super::Urgency,
        publish_ts_ns: i64,
        reply_to_uuid: Option<Uuid>,
        deliver_after: Option<DateTime<Utc>>,
        delivery_deadline: Option<DateTime<Utc>>,
    ) -> (InsertedMessage, RetirementPlan) {
        // Build pending-push rows + retirement correlation.
        // track_in_window is false for parked (future deliver_after) rows —
        // those must not be retired before they are ever delivered (design §3).
        let mut push_target_indices: Vec<(usize, bool)> = Vec::with_capacity(push_targets.len());
        let mut pushes: Vec<PendingPushInsert> = Vec::with_capacity(push_targets.len());
        for (tgt_idx, tgt) in push_targets.iter().enumerate() {
            if !tgt.push_depth.is_push_enabled() {
                continue;
            }
            // Resolve eager_wake from this subscriber's declared wake economics.
            // `Eager` subscribers (parked WASM/system consumers, attached surface
            // sessions) are always woken on publish — waking them is cheap, so
            // urgency never gates delivery. `UrgencyGated` subscribers (LLM
            // conversations, whose wake spawns a subprocess) are woken only when the
            // message's urgency meets their `wake_min` threshold; below-threshold
            // rows park until the subscriber's next natural wake. Gating eager
            // subscribers on `wake_min` was the stranded-surface-push bug — a
            // below-threshold publish parked invisibly for a live, attached surface
            // session.
            //
            // The `release_after IS NULL` predicate in `load_all_dispatchable_pushes`
            // (bus.rs) already excludes still-suppressed deferred rows from the
            // dispatcher scan, so eager_wake=1 on a deferred row cannot fire
            // prematurely. After `release_due_pushes` clears `release_after`, the
            // row becomes visible to the scan and eager_wake=1 rows are dispatched
            // immediately — this is the correct deferred-deliver-then-wake path.
            // Setting eager_wake=false here would permanently freeze the row as
            // parked, making it invisible to the dispatcher forever (correctness-2).
            let release_after = deliver_after.filter(|da| *da > Utc::now());
            let eager_wake = match (tgt.wake, tgt.wake_min) {
                (WakeEconomics::Eager, _) => true,
                (WakeEconomics::UrgencyGated, Some(wm)) => wm.wakes(urgency),
                (WakeEconomics::UrgencyGated, None) => unreachable!(
                    "UrgencyGated push target carries no wake_min — \
                     resolve_push_targets invariant violated"
                ),
            };
            if !eager_wake {
                // Reachable only for `UrgencyGated` subscribers — a designed park
                // (conversation economics), not stranding, but now a traced
                // decision where the stranded-surface-push failure was silent at
                // every level.
                tracing::debug!(
                    subscriber = %tgt.subscriber.as_str(),
                    channel = %channel.address,
                    ?urgency,
                    wake_min = tgt.wake_min.map(|w| w.as_str()),
                    "push row created without eager wake — parked pending subscriber's next wake",
                );
            }
            let track_in_window = release_after.is_none();
            pushes.push(PendingPushInsert {
                target_subscriber: tgt.subscriber.clone(),
                target_app_slug: tgt.app_slug.clone(),
                eager_wake,
                release_after,
                delivery_deadline,
            });
            push_target_indices.push((tgt_idx, track_in_window));
        }

        let inserted = insert_message_with_pushes_in_tx(
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
            publish_ts_ns,
            &pushes,
        );

        // Build the retirement plan. Channel address + uuid are stored once per
        // message; per-target entries hold only the push_id and subscriber metadata.
        debug_assert_eq!(
            inserted.push_ids.len(),
            push_target_indices.len(),
            "push_ids and push_target_indices must be in sync"
        );
        let mut entries = Vec::with_capacity(inserted.push_ids.len());
        for (push_id, &(tgt_idx, track_in_window)) in
            inserted.push_ids.iter().zip(&push_target_indices)
        {
            if !track_in_window {
                continue;
            }
            let tgt = &push_targets[tgt_idx];
            entries.push(RetirementPlanEntry {
                push_id: *push_id,
                app_slug: tgt.app_slug.clone(),
                subscriber: tgt.subscriber.clone(),
                push_depth: tgt.push_depth,
                noise: tgt.noise,
            });
        }

        (
            inserted,
            RetirementPlan {
                channel_address: channel.address.clone(),
                channel_uuid: channel.uuid,
                entries,
            },
        )
    }

    /// Apply push-window overflow retirement for one message's plan.
    ///
    /// Must be called after the enclosing transaction has been committed, while
    /// still holding the DB lock (`conn`). The DELETEs are autocommit (point
    /// deletes by primary key) — they run in the same lock scope but outside any
    /// explicit transaction.
    fn retire_windows(&self, conn: &rusqlite::Connection, plan: &RetirementPlan) {
        for entry in &plan.entries {
            if let Some(retired_id) = self.record_push_and_check_overflow(
                &super::PushRegistration {
                    channel: &plan.channel_address,
                    channel_uuid: plan.channel_uuid,
                    app_slug: &entry.app_slug,
                    subscriber: &entry.subscriber,
                    push_depth: entry.push_depth,
                    noise: entry.noise,
                },
                entry.push_id,
                conn,
            ) {
                delete_pending_push_by_id(conn, retired_id);
            }
        }
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
        // envelope. A deserialize failure is a host-internal bug — panic (§2.4).
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

        // DB work: resolve push targets + insert message + pending pushes +
        // push-window retirement, all under one lock (step 6). No budget
        // decrement, no sender gate, no body-length gate. No deliver_after /
        // delivery_deadline for transport ingress — both are None (always immediate).
        {
            let conn = self.db.lock().await;
            let push_targets =
                self.resolve_push_targets(&conn, &channel.address, channel.subscribers.as_slice());
            let tx = conn
                .unchecked_transaction()
                .expect("messaging: begin transport ingress tx");
            let (_, plan) = self.insert_pushes(
                &tx,
                &channel,
                &push_targets,
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
            self.retire_windows(&conn, &plan);
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
}

impl Messenger {
    /// Flush a WASM activation's buffered publishes atomically.
    ///
    /// Host-originated: no budget gate, no sender gate, no body-size gate
    /// (all enforced at the WASM ports import). Panics on any DB error or
    /// unresolvable channel address (boot-validated; a miss is a host-internal bug).
    ///
    /// All messages in the batch are inserted in one transaction, committed
    /// together, then push-window retirement runs. A panic mid-flush unwinds
    /// through the `Transaction` Drop guard, rolling back — none of the batch
    /// is visible (all-or-nothing, design §2.3). In-memory window state is
    /// mutated only in `retire_windows` (post-commit), so a pre-commit panic
    /// leaves it consistent with the rolled-back DB.
    ///
    /// Each publish carries its own urgency: port-configured default (for `publish`)
    /// or guest-supplied (for `publish-with-urgency`). Design §2.6.
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

        let mut all_retirements: Vec<RetirementPlan> = Vec::with_capacity(publishes.len());
        // Deferred durable depth-0 context feeds (design §6): built under the
        // lock, fanned out after it is released. Each is one committed envelope +
        // its seq + the fold-0 surface subscribers on its channel.
        let mut context_feeds: Vec<(super::MessageEnvelope, i64, Vec<SubscriberEntryKind>)> =
            Vec::new();

        {
            let conn = self.db.lock().await;
            let tx = conn
                .unchecked_transaction()
                .expect("publish_from_wasm: begin tx");

            // Per-flush memoization of resolve_push_targets: the directory is immutable
            // and the lock is held throughout, so targets cannot change mid-flush. The
            // dominant case is all publishes targeting one channel — this eliminates
            // 256× redundant `get_or_create_singleton_conversation` DB queries for that case.
            let mut targets_cache: HashMap<&str, ResolvedChannelTargets> = HashMap::new();

            // Monotonic publish_ts_ns assignment: each message gets
            // max(prev_ts + 1, now) to guarantee strictly increasing timestamps
            // within the activation (call-order visibility contract, design §2.3).
            let mut prev_ts: Option<i64> = None;

            for publish in publishes {
                let channel_addr = publish.channel_address;
                let (channel, push_targets, context_targets) =
                    targets_cache.entry(channel_addr).or_insert_with(|| {
                        let ch = self.directory.resolve(channel_addr).unwrap_or_else(|| {
                            panic!(
                                "publish_from_wasm: channel {channel_addr:?} not in directory — \
                                 boot validation should have caught this (slug={consumer_slug})"
                            )
                        });
                        assert_eq!(
                            ch.transport_type,
                            ChannelScheme::Brenn,
                            "publish_from_wasm: channel {channel_addr:?} has transport_type={tt:?}; \
                             only Brenn channels are permitted this slice (slug={consumer_slug})",
                            tt = ch.transport_type,
                        );
                        let targets =
                            self.resolve_push_targets(&conn, &ch.address, ch.subscribers.as_slice());
                        let context =
                            self.resolve_context_targets(&ch.address, ch.subscribers.as_slice());
                        (ch, targets, context)
                    });

                let now_ns = db::utc_to_ns(Utc::now());
                let publish_ts_ns = match prev_ts {
                    None => now_ns,
                    Some(prev) => std::cmp::max(prev + 1, now_ns),
                };
                prev_ts = Some(publish_ts_ns);

                // Resolve the optional reply_to address to a channel reference.
                // Host-resolved (the guest never named it — `queue_async` derived
                // the caller's own inbox), so an unresolvable address is a
                // host-wiring bug, not attacker input: fail fast.
                let reply_to_uuid = publish.reply_to.map(|addr| {
                    self.directory
                        .resolve(addr)
                        .unwrap_or_else(|| {
                            panic!(
                                "publish_from_wasm: reply_to channel {addr:?} not in directory \
                                 — boot validation should have caught this (slug={consumer_slug})"
                            )
                        })
                        .uuid
                });

                let (inserted, plan) = self.insert_pushes(
                    &tx,
                    channel,
                    push_targets,
                    ChannelScheme::Brenn,
                    source,
                    &sender,
                    publish.body,
                    publish.urgency,
                    publish_ts_ns,
                    reply_to_uuid,
                    None, // no deliver_after
                    None, // no delivery_deadline
                );
                if !context_targets.is_empty()
                    && self
                        .router
                        .any_context_session_attached(&channel.address, context_targets)
                {
                    context_feeds.push((
                        context_feed_envelope(
                            inserted.uuid,
                            source.to_owned(),
                            channel.address.clone(),
                            sender.clone(),
                            publish_ts_ns,
                            publish.body.to_owned(),
                            publish.reply_to.map(|s| s.to_owned()),
                            None,
                            None,
                            publish.urgency,
                        ),
                        inserted.id,
                        context_targets.clone(),
                    ));
                }
                all_retirements.push(plan);
            }

            tx.commit().expect("publish_from_wasm: commit tx");
            debug!(
                consumer_slug = consumer_slug,
                publish_count = publishes.len(),
                "publish_from_wasm: batch committed; retiring push windows"
            );

            // retire_windows runs post-commit (in-memory deque + autocommit DELETEs).
            // A panic here would leave in-memory window state partially updated vs DB;
            // the preceding log line records slug + channel for on-call triage.
            for (i, plan) in all_retirements.iter().enumerate() {
                debug!(
                    consumer_slug = consumer_slug,
                    channel = plan.channel_address.as_str(),
                    publish_index = i,
                    "publish_from_wasm: retiring push windows post-commit"
                );
                self.retire_windows(&conn, plan);
            }
        }

        // Durable depth-0 context feeds, fanned out after the lock is released
        // (design §6). Nothing owed to a disconnected session.
        for (envelope, seq, targets) in &context_feeds {
            self.fan_out_context_feed(targets, envelope, *seq).await;
        }

        self.dispatch_kick();
    }
}

/// One durable entry of a surface activation's flush. `channel_address` is the
/// bound output's boot-resolved address (the caller resolved port → channel
/// against its own declaration set); `urgency` is already the per-call override
/// or the port's configured default, resolved by the caller from the *server's*
/// output map.
pub struct SurfaceBatchPublish<'a> {
    pub channel_address: &'a str,
    pub body: &'a str,
    pub urgency: super::Urgency,
    /// This entry's publish timestamp, assigned by the caller in call order
    /// across the *whole* flush before it was split by substrate — so call order
    /// stays visible across the class boundary, which a stamp minted per
    /// substrate could not promise. Nanosecond precision; the durable row
    /// persists it verbatim as `publish_ts_ns`.
    pub publish_ts_ns: i64,
}

impl Messenger {
    /// Apply the durable entries of one surface activation's flush — all in one
    /// transaction, in call order, each at its own urgency and its caller-assigned
    /// timestamp.
    ///
    /// **Stamps arrive assigned, not minted here.** The caller stamps the whole
    /// flush monotonically in call order in one pass *before* splitting it by
    /// substrate, so the ordering contract holds across the class boundary; a
    /// stamp minted inside this transaction could only order the durable half
    /// against itself.
    ///
    /// The all-or-nothing guarantee is the point: an activation's publishes were
    /// buffered and released together by the kernel's flush-on-ok rule, so a
    /// partially-applied batch would publish a state no component ever asked to
    /// exist. A panic mid-batch unwinds through the `Transaction` drop guard and
    /// rolls the whole thing back; in-memory window state is touched only in
    /// `retire_windows`, post-commit, so it stays consistent with the rolled-back
    /// DB.
    ///
    /// **The send budget is not drawn here** — the caller draws once for the whole
    /// batch via [`Messenger::draw_surface_send_budget_for_batch`] before calling,
    /// because a per-entry draw could refuse the tail of an atomic flush.
    ///
    /// Every other gate runs, per entry, and **panics rather than returning**: for
    /// a bound output, address shape, directory existence, the `MessagingPublish`
    /// grant, the `brenn_publish` ACL, and the body cap are all boot-validated and
    /// boot-static, and the caller has already answered its client a violation for
    /// every client-reachable way to name something else. Reaching a failure here
    /// means the server's own output map disagrees with its directory or its
    /// policy — publishing anyway would be routing traffic no operator authorized.
    ///
    /// **Caller precondition, unchecked here: `component` must be a declared
    /// instance's name.** It is interpolated straight into the sender identity
    /// (`surface:<slug>#<component>`), and nothing below re-derives or re-admits
    /// it — the single-publish path gets that guarantee for free from its
    /// budget-map lookup panic, but this entry point deliberately draws no budget
    /// (see above), so there is no lookup left to catch a fabricated name. A
    /// caller that skips the declaration check commits durable rows under a
    /// sub-identity no operator declared, which is the one attribution the surface
    /// identity model exists to make impossible. `handle_publish_batch` admits it
    /// against the boot-resolved declaration set and kills the connection
    /// otherwise; any future caller owes the same check.
    pub async fn publish_batch_from_surface(
        &self,
        slug: &str,
        component: &str,
        publishes: &[SurfaceBatchPublish<'_>],
    ) {
        if publishes.is_empty() {
            return;
        }

        // Layer-1, once per batch: the surface's boot-resolved policy, keyed at
        // the surface grain for a component publish exactly as `publish_core`
        // does — a component's grants *are* its config-declared bindings, which
        // boot proved covered by the surface's own ACLs.
        let policy = self
            .subscribers
            .get(&SubscriberEntryKind::Surface {
                slug: slug.to_string(),
                instance: None,
            })
            .map(|r| r.policy.as_ref())
            .filter(|p| p.has_grant(AppCapability::MessagingPublish))
            .unwrap_or_else(|| {
                panic!(
                    "publish_batch_from_surface: surface {slug:?} has no registered policy with \
                     MessagingPublish — a bound durable output implies both, so this is a broken \
                     boot invariant"
                )
            });

        let sender = ParticipantId::for_surface_component(slug, component)
            .as_str()
            .to_owned();
        let source = self.source.as_ref();

        info!(
            surface = %slug,
            principal = %sender,
            publish_count = publishes.len(),
            "publish_batch_from_surface: applying activation flush"
        );

        let mut all_retirements: Vec<RetirementPlan> = Vec::with_capacity(publishes.len());
        // Deferred durable depth-0 context feeds (design §6): built under the
        // lock, fanned out after release. See `publish_from_wasm`.
        let mut context_feeds: Vec<(super::MessageEnvelope, i64, Vec<SubscriberEntryKind>)> =
            Vec::new();

        {
            let conn = self.db.lock().await;
            let tx = conn
                .unchecked_transaction()
                .expect("publish_batch_from_surface: begin tx");

            // Per-batch memoization of resolve_push_targets, as in
            // `publish_from_wasm`: the directory is immutable and the lock is held
            // throughout, so targets cannot change mid-batch, and the dominant
            // case is a batch fanning one port.
            let mut targets_cache: HashMap<&str, ResolvedChannelTargets> = HashMap::new();

            for publish in publishes {
                let addr = publish.channel_address;
                let (channel, push_targets, context_targets) =
                    targets_cache.entry(addr).or_insert_with(|| {
                        let name =
                            well_formed_name(addr, ChannelScheme::Brenn).unwrap_or_else(|| {
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
                        let targets = self.resolve_push_targets(
                            &conn,
                            &ch.address,
                            ch.subscribers.as_slice(),
                        );
                        let context =
                            self.resolve_context_targets(&ch.address, ch.subscribers.as_slice());
                        (ch, targets, context)
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

                let (inserted, plan) = self.insert_pushes(
                    &tx,
                    channel,
                    push_targets,
                    ChannelScheme::Brenn,
                    source,
                    &sender,
                    publish.body,
                    publish.urgency,
                    publish.publish_ts_ns,
                    None, // no reply_to — not exposed to surfaces
                    None, // no deliver_after — an activation flush is immediate
                    None, // no delivery_deadline
                );
                if !context_targets.is_empty()
                    && self
                        .router
                        .any_context_session_attached(&channel.address, context_targets)
                {
                    context_feeds.push((
                        context_feed_envelope(
                            inserted.uuid,
                            source.to_owned(),
                            channel.address.clone(),
                            sender.clone(),
                            publish.publish_ts_ns,
                            publish.body.to_owned(),
                            None,
                            None,
                            None,
                            publish.urgency,
                        ),
                        inserted.id,
                        context_targets.clone(),
                    ));
                }
                all_retirements.push(plan);
            }

            tx.commit().expect("publish_batch_from_surface: commit tx");

            // Post-commit: in-memory deque + autocommit DELETEs. The log line
            // above records surface + principal + count for triage if a panic
            // here leaves window state partially updated against the DB.
            for plan in &all_retirements {
                self.retire_windows(&conn, plan);
            }
        }

        // Durable depth-0 context feeds, fanned out after the lock is released
        // (design §6). Nothing owed to a disconnected session.
        for (envelope, seq, targets) in &context_feeds {
            self.fan_out_context_feed(targets, envelope, *seq).await;
        }

        self.dispatch_kick();
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
    /// The row was delivered (or is owned by another actor that will deliver
    /// it) and is already marked delivered — the caller must **not** re-mark
    /// it. Used by the Surface arm, where the atomic push-row claim *is* the
    /// mark: a concurrent session can unclaim a row it owns (re-parking it for
    /// a later drain), and a dispatcher re-mark would race that unclaim and
    /// silently retire an undelivered row.
    DeliveredNoRemark,
    /// The push remains undelivered. Drain-on-wake (or the deadline /
    /// deliver-after scanners) will pick it up. `woke` reports whether this
    /// dispatch actually fired an eager wake — the truthful signal the
    /// dispatcher supervisor arms its wake cooldown from (no decoupled mirror).
    Parked { woke: bool },
}

#[cfg(test)]
mod tests;
