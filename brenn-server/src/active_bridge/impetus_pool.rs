//! The conversation's impetus pool: what unattended bus activity spends, and
//! what attention restores.
//!
//! One stock per conversation, stored in `messaging_send_budget` and shared with
//! the outbound draws that also use it (the LLM's own `BrennSend`, PWA push,
//! MQTT egress). This module is the conversation-side half: the two
//! motions that turn a send budget into a general bound on unattended activity.
//!
//! - **Draw.** Every turn-provoking bus injection — an accepted `send` or
//!   `compact` off the command channel, each ambience-injected batch — costs one
//!   unit. The metered thing is the CC turn, so a batch of forty messages costs
//!   exactly what a batch of one costs, and a handoff that failed costs nothing.
//! - **Redeem.** A message whose envelope carries [`Impetus`] is evidence that
//!   live user interaction produced it, checked at publish time against the
//!   minting capability. Redeeming it resets the pool to full. Nothing an LLM, a
//!   WASM component, or a transport ingress can reach mints impetus, and no
//!   machinery republish carries one forward, so no cycle of bus legs can refill
//!   the pools it runs on: it depletes them and halts.
//!
//! Exhaustion is not a drop. A command is refused with an error the peer can
//! read; an ambience batch is held with its positions unadvanced and delivers
//! after the next refill.

use tracing::{debug, info, warn};

use brenn_lib::messaging::{Impetus, MessageEnvelope};
use brenn_messaging_store::db::{
    BudgetDecrement, decrement_send_budget, read_send_budget, reset_send_budget,
};

use super::ActiveBridge;

impl ActiveBridge {
    /// The pool's ceiling: the app's `messaging_send_budget`, per-app override
    /// or global default.
    fn impetus_ceiling(&self) -> u32 {
        self.app_config_default_send_budget()
    }

    /// Restore the pool to full, then deliver whatever an exhausted pool was
    /// holding.
    ///
    /// A door that refills without deciding an admission of its own goes through
    /// here rather than through
    /// [`ActiveBridge::reset_impetus_pool`] alone: a held ambience backlog is
    /// owed to a conversation that just became attended, and the alternative to
    /// delivering it on the reviving turn is waiting for unrelated bus traffic to
    /// wake the conversation again — arbitrarily long on a slow channel.
    ///
    /// Must not be called from inside a delivery: it takes the delivery lock.
    pub(crate) async fn refill_impetus_pool(&self) {
        self.reset_impetus_pool().await;
        self.deliver_refilled_backlog().await;
    }

    /// Deliver whatever an exhausted pool was holding, now that it can pay.
    ///
    /// Split out of [`ActiveBridge::refill_impetus_pool`] for the door that
    /// redeems impetus on a command of its own: the backlog draws a unit, so a
    /// conversation whose ceiling is one would have its reviving command refused
    /// by the batch it revived. The command's admission is decided on the
    /// restored pool first, and the backlog runs after.
    ///
    /// The backlog lands on CC's stdin in whichever order it races the reviving
    /// turn's own text; that race is the one the startup drain already runs.
    ///
    /// Must not be called from inside a delivery: it takes the delivery lock.
    pub(crate) async fn deliver_refilled_backlog(&self) {
        if let Err(e) = super::deliver_conversation_backlog(self).await {
            warn!(
                conversation_id = self.conversation_id,
                error = %e,
                "the refilled conversation's held backlog did not reach the harness — \
                 it stays owed for the next wake"
            );
        }
    }

    /// Restore the pool to full. The reset stands even if whatever provoked it
    /// then fails to reach the harness: the interaction happened.
    pub(crate) async fn reset_impetus_pool(&self) {
        let ceiling = self.impetus_ceiling();
        let conn = self.db.lock().await;
        reset_send_budget(&conn, self.conversation_id, ceiling);
        drop(conn);
        info!(
            conversation_id = self.conversation_id,
            ceiling, "impetus pool restored to full"
        );
    }

    /// Restore the pool to full if any envelope in the batch carries impetus.
    ///
    /// Batch-wide by design: the pool pays per CC turn, and one turn is what a
    /// batch becomes. Reset-only — a drain calls this while it holds the
    /// delivery lock, and it is itself the delivery a refill would provoke.
    pub(crate) async fn redeem_batch_impetus(&self, batch: &[MessageEnvelope]) {
        if batch.iter().any(carries_impetus) {
            self.reset_impetus_pool().await;
        }
    }

    /// Whether the pool can pay for one turn-provoking injection.
    ///
    /// A conversation with no row has never drawn, so it holds the ceiling — and
    /// a ceiling of zero says no, which is what makes `send_budget = 0` mean
    /// attended-only rather than unmetered.
    ///
    /// Consulted before the handoff and spent after it, so another draw can land
    /// in between: an outbound one (the LLM's own publish, PWA push, MQTT
    /// egress), or another turn-provoking injection, since the door and the
    /// drains serialize separately — the door runs on the adapter's command
    /// task, the drains under the delivery lock, and nothing orders the two
    /// against each other. At the floor that lets two injections run for one
    /// unit; the draw absorbs it (see [`ActiveBridge::draw_impetus_pool`]) and
    /// the bound holds — every injection still costs at least one unit and the
    /// pool cannot go below zero, so a cycle still depletes and halts.
    pub(crate) async fn impetus_pool_has_room(&self) -> bool {
        let ceiling = self.impetus_ceiling();
        let conn = self.db.lock().await;
        let remaining = read_send_budget(&conn, self.conversation_id).unwrap_or(ceiling);
        remaining > 0
    }

    /// Spend one unit on a turn that reached the harness.
    ///
    /// Called only after a successful handoff. A drain treats a failed handoff as
    /// a transient and retries the same batch on a later wake, so drawing before
    /// the handoff would spend the conversation's runway of real turns on
    /// attempts against a dying bridge instead.
    ///
    /// An exhausted pool here is the check-then-draw race, not a bug: the floor
    /// is zero, the turn already happened, and the next injection is the one
    /// that gets refused or held.
    pub(crate) async fn draw_impetus_pool(&self) {
        let ceiling = self.impetus_ceiling();
        let conn = self.db.lock().await;
        match decrement_send_budget(&conn, self.conversation_id, ceiling) {
            BudgetDecrement::Ok { remaining } => {
                debug!(
                    conversation_id = self.conversation_id,
                    remaining, "impetus pool drawn for a bus-provoked turn"
                );
            }
            BudgetDecrement::Exhausted => {
                debug!(
                    conversation_id = self.conversation_id,
                    "impetus pool was emptied between the check and the draw"
                );
            }
        }
    }
}

/// Whether this envelope carries redeemable impetus — the single predicate
/// all redemption sites share.
///
/// Matched exhaustively on purpose: a second [`Impetus`] variant is a
/// compile-time decision here instead of sites quietly disagreeing about
/// what it redeems.
pub(crate) fn carries_impetus(envelope: &MessageEnvelope) -> bool {
    match envelope.impetus {
        Some(Impetus::Replenish) => true,
        None => false,
    }
}
