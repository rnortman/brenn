//! The host's publish of a delivery status to the channel a message's sender
//! named for it.
//!
//! The entry takes a typed [`StatusBody`] and encodes it itself, so what
//! it publishes can only be a status, and it publishes only where the
//! message's sender may publish at report time. It widens no policy: it runs
//! every common gate under the sender's current authority, stamped with the
//! one host identity, [`delivery::DELIVERY_STATUS_SENDER`].

use brenn_envelope::delivery::{self, StatusBody};
use brenn_messaging_store::blob::HeldAttachments;

use super::{PublishOrigin, PublishPrincipal, PublishResult};
use crate::{ChannelScheme, MessageBody, Messenger, Urgency};

/// What became of a status publish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryStatusPublish {
    /// The status is committed on the channel.
    Published,
    /// The bus refused it; nothing was committed.
    Refused(DeliveryStatusRefusal),
}

/// Why the bus refused a status. Each is something an operator's
/// configuration, a reload or a restart can cause after the message was
/// admitted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryStatusRefusal {
    /// The channel is no longer in the directory.
    ChannelGone,
    /// Neither an app in the current table nor a live WASM consumer has the
    /// message's sender as its identity and holds `MessagingPublish`: the app
    /// was removed, lost the grant, or the server's source changed across a
    /// restart; or the consumer was retired, or lost the grant.
    SenderGone,
    /// The sender's `brenn:` publish ACL no longer covers the channel.
    SenderNotPermitted,
    /// The channel's send rate for the status sender refused it.
    RateLimited,
    /// The status is over the bus's body cap, which was lowered across a
    /// restart with the message still on its egress channel.
    BodyTooLarge { len: usize, max: usize },
}

impl Messenger {
    /// Publish `status` to `status_to` as
    /// [`delivery::DELIVERY_STATUS_SENDER`], on the authority of
    /// `concerning_sender`, the stored sender of a message that named
    /// `status_to`. The body is the encoded status, untyped text. No send
    /// budget is drawn; the channel's send rate is, under the status sender.
    ///
    /// # Panics
    ///
    /// On a `status_to` that is not a well-formed `brenn:` channel address —
    /// the admission stored only those — and on any refusal this principal
    /// cannot meet.
    pub async fn publish_delivery_status(
        &self,
        concerning_sender: &str,
        status_to: &str,
        status: &StatusBody,
    ) -> DeliveryStatusPublish {
        assert!(
            ChannelScheme::of(status_to) == Some(ChannelScheme::Brenn),
            "publish_delivery_status: status_to {status_to:?} is not a brenn: address; the \
             MQTT admission stores no other"
        );
        let result = self
            .publish_core(
                PublishOrigin::System,
                PublishPrincipal::DeliveryStatus {
                    sender: concerning_sender,
                },
                status_to,
                MessageBody::text(delivery::encode(status)),
                Urgency::Normal,
                None,
                None,
                None,
                None,
                HeldAttachments::none(),
                None,
            )
            .await;
        let refused = DeliveryStatusPublish::Refused;
        match result {
            PublishResult::Ok { .. } => DeliveryStatusPublish::Published,
            PublishResult::UnknownChannel(_) => refused(DeliveryStatusRefusal::ChannelGone),
            PublishResult::MissingSender => refused(DeliveryStatusRefusal::SenderGone),
            PublishResult::AclDenied(_) => refused(DeliveryStatusRefusal::SenderNotPermitted),
            PublishResult::RateLimited => refused(DeliveryStatusRefusal::RateLimited),
            PublishResult::BodyTooLarge { len, max } => {
                refused(DeliveryStatusRefusal::BodyTooLarge { len, max })
            }
            unreachable @ (PublishResult::MalformedAddress(_)
            | PublishResult::BudgetExhausted
            | PublishResult::DeferredQuotaExceeded { .. }
            | PublishResult::ImpetusUnauthorized
            | PublishResult::MqttRefused(_)) => {
                let about = match status {
                    StatusBody::Message(message) => format!("message {}", message.message_id),
                    StatusBody::Restarted(_) => "a restart".to_owned(),
                };
                panic!(
                    "publish_delivery_status: {unreachable:?} for a status to {status_to:?} \
                     about {about} on the authority of {concerning_sender:?}; the admission \
                     stores only well-formed addresses, and the rest cannot arise for a \
                     System-origin status with no options"
                )
            }
        }
    }
}
