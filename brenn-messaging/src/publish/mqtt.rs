//! The front door for a bus publish to an MQTT topic.
//!
//! An admitted publish to `mqtt:<client>:<topic>` is committed to the
//! client's egress channel, `brenn:egress/mqtt/<client>`, with the payload as
//! its body (or its one attachment) and the MQTT options as `MqttPublish`
//! transport metadata.
//! [`Messenger::admit_mqtt`] is the one decision on whether it may be; the
//! common publish gates (body cap, send rate, send budget, commit) follow it
//! inside `publish_gated`, which also writes an inline payload over the body
//! cap to the blob store once every gate has passed.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::gates::{publish_acl_allows, well_formed_name};
use brenn_envelope::grants::AppCapability;
use brenn_envelope::{AttachmentName, MessageBody, check_content_type, delivery};
use brenn_lib::access::AppPolicy;
use brenn_lib::mqtt::{MqttClientDirectory, MqttLifetime, MqttPublishOptions, parse_topic_name};
use brenn_messaging_store::blob::{BlobError, BlobStore, HeldAttachments};

use super::{PublishOrigin, PublishPrincipal, PublishResult};
use crate::{
    ChannelEntry, ChannelScheme, Messenger, MqttExpiry, MqttPublishTransport, Urgency,
    mqtt_egress_address,
};

/// The name of the one attachment an MQTT payload rides as when it is not the
/// body.
const PAYLOAD_ATTACHMENT: &str = "payload";

/// Why the MQTT admission refused a publish on its options or payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MqttRefusal {
    /// The stated `reply_to` is not a topic the publisher may name as its
    /// Response Topic. `address` is the address as stated.
    ReplyTo {
        address: String,
        why: ReplyToRefusal,
    },
    /// A `delivery_deadline` was stated, which a broker cannot honour.
    DeliveryDeadline,
    /// The stated `status_to` is not a channel the publisher may name for its
    /// delivery statuses. `address` is the address as stated.
    StatusTo {
        address: String,
        why: StatusToRefusal,
    },
    /// QoS above 2.
    InvalidQos { qos: u8 },
    /// A stated content type that is not a well-formed media type.
    InvalidContentType { reason: String },
    /// Neither the body alone nor exactly one attachment beside an empty body.
    PayloadShape,
    /// The payload is over the client's `outbound_payload_cap_bytes`.
    PayloadOverClientCap { len: u64, cap: u32 },
    /// An inline payload over the bus's body cap must ride as a blob, and the
    /// messenger has no blob store.
    NeedsBlobStore { len: u64, max_body: usize },
    /// An inline payload over the bus's body cap must ride as a blob, and the
    /// blob store refused it.
    BlobRefused { reason: String },
}

/// Why the MQTT admission refused a publish's `status_to`, in the order the
/// admission asks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatusToRefusal {
    /// Not a well-formed `brenn:` channel address.
    Malformed,
    /// The publisher holds no `MessagingPublish` grant.
    GrantAbsent,
    /// The publisher's `brenn:` publish ACL does not cover the channel.
    AclDenied,
    /// No such channel is in the directory.
    Unknown,
    /// The largest status about a message to this topic, `len` bytes, is
    /// over the bus's body cap, `max`.
    StatusOverBodyCap { len: usize, max: usize },
}

/// Why the MQTT admission refused a publish's `reply_to`, in the order the
/// admission asks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplyToRefusal {
    /// Not a well-formed `mqtt:<client>:<topic>` naming a concrete topic.
    /// `detail` is `parse_topic_name`'s error text, which tells a wildcard
    /// from a malformed address.
    Malformed { detail: String },
    /// A well-formed address on a client other than the target's.
    OtherClient,
    /// The publisher's MQTT publish ACL does not cover the reply topic.
    AclDenied,
}

/// The payload of a conversation's MQTT publish.
#[derive(Debug)]
pub enum MqttPayload {
    /// The bytes themselves, carried as the message body, or as one blob
    /// attachment when they are over the bus's body cap.
    Inline(Vec<u8>),
    /// Held attachments, carried beside an empty body; the admission accepts
    /// exactly one.
    Attached(HeldAttachments),
}

/// A committed MQTT publish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqttCommitted {
    pub message_id: Uuid,
    /// `Some` for a `Conversation` origin (the conversation's send budget left
    /// after this publish); `None` for every other origin, none of which has a
    /// send budget.
    pub remaining_budget: Option<u32>,
}

/// What `publish_gated` is handed for an MQTT publish: its options, and an
/// inline payload over the bus's body cap, which rides as a blob written after
/// every gate has passed. With `over_cap` set the body is empty and there are
/// no attachments.
pub(crate) struct MqttTarget<'a> {
    pub(crate) options: &'a MqttPublishOptions,
    pub(crate) over_cap: Option<Vec<u8>>,
}

/// An admitted MQTT publish: the client's egress channel to commit to, and
/// what the stored transport metadata is built from.
pub(crate) struct MqttAdmitted {
    pub(crate) channel: Arc<ChannelEntry>,
    client: String,
    topic: String,
    options: MqttPublishOptions,
    /// The bare reply topic parsed from `options.reply_to`, on `client`.
    response_topic: Option<String>,
}

impl MqttAdmitted {
    /// The transport metadata for a message that may first be sent at
    /// `good_from`.
    pub(crate) fn transport(&self, good_from: DateTime<Utc>) -> MqttPublishTransport {
        mqtt_transport(
            &self.client,
            &self.topic,
            &self.options,
            self.response_topic.as_deref(),
            good_from,
        )
    }
}

/// The transport metadata of an MQTT publish to `client`'s `topic` that may
/// first be sent at `good_from` — its release time when parked, its commit
/// otherwise: the options as stated, the admitted bare reply topic, and an
/// expiry of `good_from` plus the lifetime. The one rule for every sender's
/// committed row.
pub(crate) fn mqtt_transport(
    client: &str,
    topic: &str,
    options: &MqttPublishOptions,
    response_topic: Option<&str>,
    good_from: DateTime<Utc>,
) -> MqttPublishTransport {
    let expires = match options.lifetime {
        MqttLifetime::Seconds(s) => {
            MqttExpiry::At(good_from + chrono::Duration::seconds(i64::from(s.get())))
        }
        MqttLifetime::Forever => MqttExpiry::Never,
    };
    MqttPublishTransport {
        client_slug: client.to_owned(),
        topic: topic.to_owned(),
        qos: options.qos,
        retain: options.retain,
        content_type: options.content_type.clone(),
        expires,
        status_to: options.status_to.clone(),
        response_topic: response_topic.map(str::to_owned),
    }
}

/// The length of an MQTT publish's payload as it is about to be committed:
/// the body's, or its one attachment's beside an empty body. `None` for any
/// other shape.
pub(crate) fn mqtt_payload_len(body: &MessageBody, attachments: &HeldAttachments) -> Option<u64> {
    if attachments.is_empty() {
        return Some(body.as_bytes().len() as u64);
    }
    let refs = attachments.refs();
    match refs.iter().next() {
        Some((_, blob)) if refs.len() == 1 && body.as_bytes().is_empty() => Some(blob.size),
        _ => None,
    }
}

impl Messenger {
    /// Install the table of declared MQTT clients the admission reads. Called
    /// once, at boot, after the MQTT service is built; the table itself is kept
    /// current by reload. A messenger with none installed knows no client.
    ///
    /// # Panics
    ///
    /// Panics on a second call: two client tables would be two answers to
    /// "is this client registered".
    pub fn install_mqtt_clients(&self, clients: Arc<dyn MqttClientDirectory>) {
        if self.mqtt_clients.set(clients).is_err() {
            panic!("Messenger::install_mqtt_clients called twice");
        }
    }

    /// Decide whether `policy` may publish a payload of `payload_len` bytes to
    /// the MQTT topic address `addr` with `options`. Spends nothing.
    /// `payload_len` is [`mqtt_payload_len`]'s answer: `None` when the payload
    /// is neither the body alone nor one attachment beside an empty body.
    ///
    /// The checks run in a fixed order, and the order is part of the answer: a
    /// publisher refused on its grant or ACL learns nothing of the client.
    /// `publish_gated` answers steps 1 and 2 itself, in this order, before it
    /// calls here; they are answered again for a caller that arrives with a
    /// policy and no layer-1 gate of its own.
    ///
    /// 1. `addr` is a well-formed `mqtt:` topic name → `MalformedAddress`.
    /// 2. the policy holds `MqttPublish` → `MissingSender`.
    /// 3. `allows_mqtt_publish(client, topic)` → `AclDenied`.
    /// 4. the client is registered and its egress channel is in the
    ///    directory → `UnknownChannel`. Either can be absent for a moment
    ///    while a reload adds or removes the client, and "unknown client" is
    ///    the true answer in that moment.
    /// 5. no `delivery_deadline` → `MqttRefused(DeliveryDeadline)`.
    /// 6. a stated `options.reply_to` is a concrete `mqtt:` topic name on the
    ///    target's client that the publisher's MQTT publish ACL covers →
    ///    `MqttRefused(ReplyTo)`, in [`ReplyToRefusal`]'s order. It reads the
    ///    stated address and the policy only, so it reveals nothing of the
    ///    directory or the client table; placed after steps 1–4, a publish
    ///    refused there answers the same with or without a reply address.
    /// 7. a stated `status_to` is a channel the publisher could publish to
    ///    itself, and a status about this topic fits the body cap →
    ///    `MqttRefused(StatusTo)`. Its own checks run in [`StatusToRefusal`]'s
    ///    order; the ACL precedes the directory, so a channel the publisher may
    ///    not publish to answers the same whether or not it exists.
    /// 8. QoS ≤ 2, a stated content type well formed.
    /// 9. the payload is the body, or one attachment beside an empty body.
    /// 10. the payload's length is within the client's outbound cap.
    ///
    /// A release time needs no admission: whether to park is the commit's
    /// decision, as for every scheme.
    ///
    /// A commit needs a third thing besides the client and its channel: the
    /// egress reader's position on the channel. That is not checked here but
    /// at the commit or the park, with the write, so a publish admitted before
    /// a reload closes the client's channel and committed after is still
    /// refused.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn admit_mqtt(
        &self,
        policy: &AppPolicy,
        addr: &str,
        options: &MqttPublishOptions,
        payload_len: Option<u64>,
        delivery_deadline: Option<DateTime<Utc>>,
    ) -> Result<MqttAdmitted, PublishResult> {
        let refuse = |r: MqttRefusal| Err(PublishResult::MqttRefused(r));

        let Ok(parsed) = parse_topic_name(addr) else {
            return Err(PublishResult::MalformedAddress(addr.to_string()));
        };
        if !policy.has_grant(AppCapability::MqttPublish) {
            return Err(PublishResult::MissingSender);
        }
        if !policy.allows_mqtt_publish(&parsed.client, &parsed.topic) {
            return Err(PublishResult::AclDenied(addr.to_string()));
        }
        let Some(cap) = self
            .mqtt_clients
            .get()
            .and_then(|clients| clients.outbound_payload_cap(&parsed.client))
        else {
            return Err(PublishResult::UnknownChannel(addr.to_string()));
        };
        let Some(channel) = self.directory.resolve(&mqtt_egress_address(&parsed.client)) else {
            return Err(PublishResult::UnknownChannel(addr.to_string()));
        };

        // A stated option the broker cannot carry is refused, never silently dropped.
        if delivery_deadline.is_some() {
            return refuse(MqttRefusal::DeliveryDeadline);
        }

        let response_topic = match options.reply_to.as_deref() {
            None => None,
            Some(address) => match admit_reply_to(policy, address, &parsed.client) {
                Ok(topic) => Some(topic),
                Err(why) => {
                    return refuse(MqttRefusal::ReplyTo {
                        address: address.to_owned(),
                        why,
                    });
                }
            },
        };

        if let Some(address) = options.status_to.as_deref()
            && let Err(why) = self.admit_status_to(policy, address, &parsed.client, &parsed.topic)
        {
            return refuse(MqttRefusal::StatusTo {
                address: address.to_owned(),
                why,
            });
        }

        if options.qos > 2 {
            return refuse(MqttRefusal::InvalidQos { qos: options.qos });
        }
        if let Some(ct) = options.content_type.as_deref()
            && let Err(e) = check_content_type(ct)
        {
            return refuse(MqttRefusal::InvalidContentType {
                reason: e.to_string(),
            });
        }

        let Some(len) = payload_len else {
            return refuse(MqttRefusal::PayloadShape);
        };
        if len > u64::from(cap) {
            return refuse(MqttRefusal::PayloadOverClientCap { len, cap });
        }

        Ok(MqttAdmitted {
            channel,
            client: parsed.client,
            topic: parsed.topic,
            options: options.clone(),
            response_topic,
        })
    }

    /// Step 7 of [`Self::admit_mqtt`]: may `policy` name `address` as the
    /// `status_to` of a publish to `client`'s `topic`. The right is the publish
    /// right, not the wider visibility a `reply_to` is held to.
    fn admit_status_to(
        &self,
        policy: &AppPolicy,
        address: &str,
        client: &str,
        topic: &str,
    ) -> Result<(), StatusToRefusal> {
        let Some(name) = well_formed_name(address, ChannelScheme::Brenn) else {
            return Err(StatusToRefusal::Malformed);
        };
        if !policy.has_grant(AppCapability::MessagingPublish) {
            return Err(StatusToRefusal::GrantAbsent);
        }
        if !publish_acl_allows(policy, ChannelScheme::Brenn, name) {
            return Err(StatusToRefusal::AclDenied);
        }
        if self.directory.resolve(address).is_none() {
            return Err(StatusToRefusal::Unknown);
        }
        let len = delivery::max_encoded_len(client, topic);
        let max = self.max_body_bytes();
        if len > max {
            return Err(StatusToRefusal::StatusOverBodyCap { len, max });
        }
        Ok(())
    }

    /// Decide whether a component under `policy` may publish `payload_len`
    /// bytes to `mqtt:<client>:<topic>` with `options`. Spends nothing. The
    /// decision is [`Self::admit_mqtt`]'s, with no `delivery_deadline`; a
    /// component's options state no `reply_to`. The commit is the activation's
    /// flush, through [`Self::publish_from_wasm`].
    pub fn admit_mqtt_from_wasm(
        &self,
        policy: &AppPolicy,
        client: &str,
        topic: &str,
        options: &MqttPublishOptions,
        payload_len: u64,
    ) -> Result<(), PublishResult> {
        self.admit_mqtt(
            policy,
            &format!("mqtt:{client}:{topic}"),
            options,
            Some(payload_len),
            None,
        )
        .map(|_| ())
    }

    /// Decide whether the app `app_slug` may publish `payload_len` bytes to the
    /// MQTT topic address `addr` with `options`, against its policy in this
    /// messenger's app table as it stands now. Spends nothing. An app the table
    /// does not hold is `MissingSender`; otherwise the decision is
    /// [`Self::admit_mqtt`]'s, with no `delivery_deadline`; a `reply_to` in
    /// `options` is judged as for any sender.
    pub fn admit_mqtt_for_app(
        &self,
        app_slug: &str,
        addr: &str,
        options: &MqttPublishOptions,
        payload_len: u64,
    ) -> Result<(), PublishResult> {
        let apps = self.apps.load();
        let Some(app) = apps.get(app_slug) else {
            return Err(PublishResult::MissingSender);
        };
        self.admit_mqtt(&app.policy, addr, options, Some(payload_len), None)
            .map(|_| ())
    }

    /// Publish `payload` to the MQTT topic address `addr` on behalf of an app's
    /// conversation, through the common publish gates with the MQTT admission
    /// at layer 2. Returns at commit, with the message on the client's egress
    /// channel. A commit that finds no egress reader's position on the channel
    /// — the client's channel was closed by a reload — writes nothing, refunds
    /// the send budget and is answered `UnknownChannel`, as for an unknown
    /// client.
    ///
    /// An inline payload within the bus's body cap becomes a body typed by
    /// `options.content_type` when its bytes are valid for that type, and an
    /// `application/octet-stream` body of the same bytes otherwise, so text
    /// reads as text to an observer of the channel and other bytes are still
    /// carried exactly. An inline payload over the body cap is written to the
    /// blob store, typed by `options.content_type` or
    /// `application/octet-stream`, and rides as one attachment named
    /// `payload` beside an empty body; the blob is written only after the
    /// admission, the send rate and the send budget have all passed, so a
    /// refused publish writes no blob. The MQTT Content Type sent is
    /// `options.content_type`, never the body's or the blob's type. An
    /// attached payload rides beside an empty body.
    ///
    /// A `deliver_after` in the future parks the publish on the egress channel
    /// until then, invisible to the egress reader, counted against the
    /// channel's deferred cap (`DeferredQuotaExceeded` when full, budget
    /// refunded) and refused as above when the reader holds no position; its
    /// lifetime counts from `deliver_after`. One at or before now publishes at
    /// once.
    ///
    /// # Panics
    ///
    /// Panics on a `System` origin: an app's MQTT publish is a `Conversation`
    /// origin (an LLM tool publish) or an `AutomationFire` origin (a job's
    /// fire), never `System`.
    pub async fn publish_mqtt(
        &self,
        origin: PublishOrigin,
        sender_app_slug: &str,
        addr: &str,
        payload: MqttPayload,
        options: &MqttPublishOptions,
        deliver_after: Option<DateTime<Utc>>,
    ) -> Result<MqttCommitted, PublishResult> {
        assert!(
            !matches!(origin, PublishOrigin::System),
            "Messenger::publish_mqtt called with PublishOrigin::System — an app's MQTT \
             publish is a Conversation or an AutomationFire origin, never System"
        );
        let principal = PublishPrincipal::App {
            slug: sender_app_slug,
        };
        let (body, attachments, over_cap) = match payload {
            MqttPayload::Inline(bytes) if bytes.len() > self.max_body_bytes() => {
                (MessageBody::text(""), HeldAttachments::none(), Some(bytes))
            }
            MqttPayload::Inline(bytes) => {
                // A content type the admission will refuse is carried as
                // octet-stream here; that body only goes as far as the refusal.
                let body = MessageBody::carrying(options.content_type.as_deref(), &bytes);
                (body, HeldAttachments::none(), None)
            }
            MqttPayload::Attached(attachments) => (MessageBody::text(""), attachments, None),
        };
        let result = self
            .publish_core(
                origin,
                principal,
                addr,
                body,
                Urgency::Normal,
                None,
                deliver_after,
                None,
                None,
                attachments,
                Some(MqttTarget { options, over_cap }),
            )
            .await;
        match result {
            PublishResult::Ok {
                message_id,
                remaining_budget,
                ..
            } => Ok(MqttCommitted {
                message_id,
                remaining_budget,
            }),
            refused => Err(refused),
        }
    }
}

/// Step 6 of [`Messenger::admit_mqtt`]: may `policy` name `address` as the
/// Response Topic of a publish through `client`. Answers the bare reply topic.
fn admit_reply_to(
    policy: &AppPolicy,
    address: &str,
    client: &str,
) -> Result<String, ReplyToRefusal> {
    let parsed = parse_topic_name(address).map_err(|e| ReplyToRefusal::Malformed {
        detail: e.to_string(),
    })?;
    if parsed.client != client {
        return Err(ReplyToRefusal::OtherClient);
    }
    if !policy.allows_mqtt_publish(&parsed.client, &parsed.topic) {
        return Err(ReplyToRefusal::AclDenied);
    }
    Ok(parsed.topic)
}

/// Write `bytes` to a new blob in `store` on the blocking pool and return it
/// as the one attachment `payload`.
pub(super) async fn write_payload_blob(
    store: Arc<BlobStore>,
    bytes: Vec<u8>,
    options: &MqttPublishOptions,
) -> Result<HeldAttachments, PublishResult> {
    let content_type = options
        .content_type
        .clone()
        .unwrap_or_else(|| "application/octet-stream".to_string());
    let hold = tokio::task::spawn_blocking(move || write_blob(&store, &content_type, &bytes))
        .await
        .expect("the MQTT payload blob write panicked")
        .map_err(|reason| PublishResult::MqttRefused(MqttRefusal::BlobRefused { reason }))?;
    let name = AttachmentName::new(PAYLOAD_ATTACHMENT)
        .expect("`payload` is a well-formed attachment name");
    Ok(HeldAttachments::new([(name, hold)]).expect("one attachment is within every bound"))
}

/// Create, write and seal one blob holding `bytes`. A refusal the store
/// states for budget or size, and an I/O error (logged at `warn`), are
/// returned as their text.
///
/// # Panics
///
/// On a content type the store refuses (the admission checked it first) and
/// on any other refusal one whole write cannot meet.
fn write_blob(
    store: &Arc<BlobStore>,
    content_type: &str,
    bytes: &[u8],
) -> Result<brenn_messaging_store::blob::BlobHold, String> {
    let refused = |e: BlobError| match e {
        BlobError::TooLarge { .. } | BlobError::BudgetExhausted { .. } => e.to_string(),
        BlobError::Io(ref io) => {
            tracing::warn!(error = %io, "blob store write failed for an MQTT payload");
            e.to_string()
        }
        other => panic!("writing an MQTT payload blob: {other}"),
    };
    let mut writer = store.create(content_type).map_err(refused)?;
    writer.write(0, bytes).map_err(refused)?;
    Ok(writer
        .seal()
        .unwrap_or_else(|_| panic!("a blob written whole in one write seals")))
}
