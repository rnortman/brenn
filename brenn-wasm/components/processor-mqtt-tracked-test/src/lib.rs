// Tracked-MQTT-publish test fixture, built on the guest SDK.
//
// Ports, as its class declares them: `in inbound`, `out out`, `io delivery`,
// `optional out spare`, `optional in broker`, `io state`.
//
// An activation whose first new message on `inbound` starts with `TRACKED`
// makes one `brenn_guest::mqtt::publish_tracked` call whose every argument is
// in the body (see `publish_tracked`), the status port's name included, so a
// test can name any port. An admitted publish buffers `published <id>` on
// `out` and the bare id on `state`, the record later activations match
// against. A refusal fails the activation with the SDK's rendering of it.
// `TRACKED_THEN_ERR` publishes the same way and then fails the activation, so
// the host's discard of the buffered publish shows.
//
// An activation with new messages on `delivery` reads each with
// `brenn_guest::delivery::read` and buffers one line per message on `out`:
//
//   - `other`: not from the host's status sender;
//   - `error <diagnostic>`: from it, and undecodable;
//   - `status matched=<bool> kind=<kind> id=<id>`, matched against the ids on
//     `state`; a `handed` adds ` never=<bool>`, what
//     `outcome_will_never_come` answers against the newest state on `broker`,
//     or ` never=-` when `broker` carries none;
//   - `restarted client=<client or -> found_at_ms=<ms>`.
//
// Then one line per message on `broker`, read with
// `brenn_guest::mqtt::connection::read`: `connection none`,
// `connection client=<client> found_at_ms=<ms>`, or
// `connection error <diagnostic>`.
//
// The fixture parses no JSON and spells no sender.

use core::num::NonZeroU32;

use brenn_guest::delivery::{self, DeliveryKind, StatusBody};
use brenn_guest::mqtt::{self, Lifetime, MessageId, TrackedPublish, connection};
use brenn_guest::{Activation, Body, Error, Message, Processor};

struct ProcessorMqttTrackedTest;

/// Sentinel selecting one tracked publish.
const TRACKED: &str = "TRACKED";

/// Sentinel selecting one tracked publish and then a failed activation.
const TRACKED_THEN_ERR: &str = "TRACKED_THEN_ERR";

/// The tracked publish. The body is the sentinel line, then one field per
/// line: client, topic, QoS, retain (`0`/`1`), lifetime (`forever` or a number
/// of seconds), content type or `-`, status port or `-`, reply topic or `-`,
/// and the payload as the remainder (which may span lines).
///
/// A lifetime of `0` is not representable in the SDK's `Lifetime`; it goes
/// through the raw binding, so a test sees the host's own refusal of it.
fn publish_tracked(text: &str) -> Result<MessageId, Error> {
    let mut lines = text.splitn(10, '\n');
    let _sentinel = lines.next();
    let mut field = |name: &str| {
        lines
            .next()
            .unwrap_or_else(|| panic!("TRACKED: missing field {name}"))
    };
    let client = field("client");
    let topic = field("topic");
    let qos: u8 = field("qos").parse().expect("TRACKED: qos is a number");
    let retain = match field("retain") {
        "0" => false,
        "1" => true,
        other => panic!("TRACKED: retain must be 0 or 1, got {other:?}"),
    };
    let lifetime = field("lifetime");
    let content_type = field("content type");
    let status_port = field("status port");
    let reply_topic = field("reply topic");
    let (content_type, status_port, reply_topic) = (
        (content_type != "-").then_some(content_type),
        (status_port != "-").then_some(status_port),
        (reply_topic != "-").then_some(reply_topic),
    );
    let payload = lines.next().unwrap_or("").as_bytes();

    let lifetime = match lifetime {
        "forever" => Lifetime::Forever,
        "0" => return publish_zero_lifetime(client, topic, payload, qos, retain),
        n => Lifetime::Seconds(
            NonZeroU32::new(n.parse().expect("TRACKED: lifetime is a number"))
                .expect("zero is handled above"),
        ),
    };
    let mut options = TrackedPublish::new(qos, retain, lifetime);
    if let Some(ct) = content_type {
        options = options.content_type(ct);
    }
    if let Some(port) = status_port {
        options = options.status_port(port);
    }
    if let Some(topic) = reply_topic {
        options = options.reply_topic(topic);
    }
    Ok(mqtt::publish_tracked(client, topic, payload, &options)?)
}

/// A tracked publish with a lifetime of zero seconds, through the raw
/// binding; the host's refusal is rendered as the SDK renders an
/// `invalid-publish`.
fn publish_zero_lifetime(
    client: &str,
    topic: &str,
    payload: &[u8],
    qos: u8,
    retain: bool,
) -> Result<MessageId, Error> {
    use brenn_guest::bindings::brenn::processor0_1_0::mqtt as raw;
    let options = raw::TrackedPublishOptions {
        content_type: None,
        qos,
        retain,
        lifetime: raw::MessageLifetime::Seconds(0),
        status_port: None,
        reply_topic: None,
    };
    match raw::mqtt_publish_tracked(client, topic, payload, options) {
        Ok(id) => Ok(MessageId::parse_str(&id).expect("the host returns a UUID")),
        Err(raw::TrackedPublishError::InvalidPublish(m)) => {
            Err(Error::from(mqtt::TrackedPublishError::InvalidPublish(m)))
        }
        Err(other) => Err(Error::failed(format!("unexpected refusal: {other:?}"))),
    }
}

/// The ids this component's publishes returned, as recorded on `state`.
fn recorded_ids(a: &Activation) -> Vec<MessageId> {
    let Some(window) = a.port_windows().find(|w| w.port() == "state") else {
        return Vec::new();
    };
    window
        .context_messages()
        .iter()
        .chain(window.new_messages())
        .map(|m| {
            let text = m.body().as_text().expect("a recorded id is text");
            MessageId::parse_str(text).expect("a recorded id is a UUID")
        })
        .collect()
}

/// Every message on `broker`, oldest first.
fn broker_messages(a: &Activation) -> Vec<&Message> {
    let Some(window) = a.port_windows().find(|w| w.port() == "broker") else {
        return Vec::new();
    };
    window
        .context_messages()
        .iter()
        .chain(window.new_messages())
        .collect()
}

fn kind_word(kind: &DeliveryKind) -> &'static str {
    match kind {
        DeliveryKind::Handed => "handed",
        DeliveryKind::Delivered => "delivered",
        DeliveryKind::Rejected { .. } => "rejected",
        DeliveryKind::Expired => "expired",
        DeliveryKind::Unknown { .. } => "unknown",
        DeliveryKind::Dropped { .. } => "dropped",
    }
}

/// The report path: one line on `out` per new message on `delivery`, then one
/// per message on `broker`.
fn report(a: &Activation, statuses: &[Message]) -> Result<Option<Body>, Error> {
    let ids = recorded_ids(a);
    let broker = broker_messages(a);
    let newest_state = broker
        .iter()
        .rev()
        .find_map(|m| connection::read(m).ok().flatten());

    for message in statuses {
        let line = match delivery::read(message) {
            Ok(None) => String::from("other"),
            Err(e) => format!("error {e}"),
            Ok(Some(received)) => match received.status {
                StatusBody::Message(status) => {
                    let mut line = format!(
                        "status matched={} kind={} id={}",
                        ids.contains(&status.message_id),
                        kind_word(&status.kind),
                        status.message_id,
                    );
                    if status.kind == DeliveryKind::Handed {
                        match &newest_state {
                            Some(state) => line.push_str(&format!(
                                " never={}",
                                connection::outcome_will_never_come(received.published_ms, state)
                            )),
                            None => line.push_str(" never=-"),
                        }
                    }
                    line
                }
                StatusBody::Restarted(restarted) => format!(
                    "restarted client={} found_at_ms={}",
                    restarted.mqtt.as_ref().map_or("-", |m| m.client.as_str()),
                    restarted.found_at_ms,
                ),
            },
        };
        brenn_guest::publish("out", &line)?;
    }

    for message in broker {
        let line = match connection::read(message) {
            Ok(None) => String::from("connection none"),
            Ok(Some(state)) => format!(
                "connection client={} found_at_ms={}",
                state.client, state.previous_session.found_at_ms
            ),
            Err(e) => format!("connection error {e}"),
        };
        brenn_guest::publish("out", &line)?;
    }
    Ok(None)
}

impl Processor for ProcessorMqttTrackedTest {
    fn receive(a: Activation) -> Result<Option<Body>, Error> {
        let inbound = a
            .port_windows()
            .find(|w| w.port() == "inbound")
            .and_then(|w| w.new_messages().first());
        if let Some(text) = inbound.and_then(|m| m.body().as_text().ok())
            && text.starts_with(TRACKED)
        {
            let id = publish_tracked(text)?;
            brenn_guest::publish("out", &format!("published {id}"))?;
            brenn_guest::publish("state", &id.to_string())?;
            if text.starts_with(TRACKED_THEN_ERR) {
                return Err(Error::failed(
                    "tracked-then-err: publish admitted, now failing",
                ));
            }
            return Ok(None);
        }

        let statuses = a
            .port_windows()
            .find(|w| w.port() == "delivery")
            .map(|w| w.new_messages())
            .unwrap_or_default();
        if !statuses.is_empty() {
            return report(&a, statuses);
        }
        Ok(None)
    }
}

brenn_guest::export_processor!(ProcessorMqttTrackedTest);
