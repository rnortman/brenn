//! Policy builders for tests that stand up delivery.
//!
//! These live here rather than in a consumer's test scaffolding because the
//! delivery-time ACL gate is enforced in this crate: every crate that drives a
//! subscriber through it needs the same policy shape, and crates below the
//! server cannot reach the server's test support.

use std::collections::HashMap;

use crate::access::AppPolicy;
use crate::messaging::ChannelEntry;

/// Build an `AppPolicy` that authorizes *delivery* on each given channel address
/// — the delivery-time ACL gate requires every subscriber's policy to cover the
/// channel it reads. For each address, insert the transport grant its scheme
/// gates on and an exact covering matcher.
/// `DynamicSubscribe` is intentionally **not** granted — delivery authorization
/// must not depend on the runtime-tool grant. A malformed address panics (test
/// fixtures pass valid addresses).
pub fn delivery_policy_for_addresses<'a>(
    addresses: impl IntoIterator<Item = &'a str>,
) -> AppPolicy {
    use crate::access::acl::{ChannelMatcher, MqttSubMatcher, WebhookMatcher};
    use crate::messaging::ChannelScheme;
    use brenn_envelope::grants::AppCapability;

    let mut policy = AppPolicy::default();
    for address in addresses {
        match ChannelScheme::split(address) {
            Some((ChannelScheme::Mqtt, _)) => {
                let parsed = crate::mqtt::address::parse_mqtt_address(address)
                    .expect("delivery_policy_for_addresses: valid mqtt address");
                policy.grants.insert(AppCapability::MqttSubscribe);
                policy.acls.mqtt_subscribe.push(MqttSubMatcher {
                    client: parsed.client,
                    topic_filter: parsed.topic,
                });
            }
            Some((ChannelScheme::Brenn, channel)) => {
                policy.grants.insert(AppCapability::MessagingSubscribe);
                policy
                    .acls
                    .brenn_subscribe
                    .push(ChannelMatcher::Exact(channel.to_string()));
            }
            Some((ChannelScheme::Webhook, endpoint)) => {
                policy.grants.insert(AppCapability::Webhook);
                policy.acls.webhook.push(WebhookMatcher {
                    endpoint: endpoint.to_string(),
                });
            }
            // Ephemeral and local delivery gate on their own transport grants,
            // not on `MessagingSubscribe` — a ring-backed input needs the right
            // one or the delivery-time gate denies it.
            Some((ChannelScheme::Ephemeral, channel)) => {
                policy.grants.insert(AppCapability::EphemeralSubscribe);
                policy
                    .acls
                    .ephemeral_subscribe
                    .push(ChannelMatcher::Exact(channel.to_string()));
            }
            Some((ChannelScheme::Local, channel)) => {
                policy.grants.insert(AppCapability::LocalSubscribe);
                policy
                    .acls
                    .local_subscribe
                    .push(ChannelMatcher::Exact(channel.to_string()));
            }
            Some((ChannelScheme::PwaPush, _)) | None => {
                panic!("delivery_policy_for_addresses: unrecognized address prefix in {address:?}");
            }
        }
    }
    policy
}

/// Build a `wasm_policies` map (slug → delivery policy) from a set of channel
/// entries: each `Wasm(slug)` subscriber gets a policy authorizing delivery on
/// every channel address it subscribes to. The delivery-time ACL gate denies
/// any `Wasm` subscriber whose policy does not cover the channel.
pub fn wasm_policies_from_entries(entries: &[ChannelEntry]) -> HashMap<String, AppPolicy> {
    use crate::messaging::SubscriberEntryKind;

    let mut by_slug: HashMap<String, Vec<String>> = HashMap::new();
    for entry in entries {
        for sub in &entry.subscribers {
            if let SubscriberEntryKind::Wasm(slug) = &sub.kind {
                by_slug
                    .entry(slug.clone())
                    .or_default()
                    .push(entry.address.clone());
            }
        }
    }
    by_slug
        .into_iter()
        .map(|(slug, addrs)| {
            let policy = delivery_policy_for_addresses(addrs.iter().map(|a| a.as_str()));
            (slug, policy)
        })
        .collect()
}
