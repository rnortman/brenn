//! DOM-free port-delivery validation and the shared malformed-body fault report.
//!
//! Every component state machine takes the same first two steps on a port
//! delivery: reject a wrong port or an unparseable envelope (a `ContractViolation`
//! the DOM glue panics on — shell/proto skew or operator misconfig), then, on a
//! well-formed envelope whose *body* violates the component's convention, emit a
//! [`FaultReport`] the glue logs and carries on from. Sharing these here keeps the
//! operator log line format identical across every component, so a buggy publisher
//! is grep-able the same way regardless of which component caught it.
//!
//! Host-tested; no wasm dependency, so a component's `logic.rs` calls it directly
//! under the host test sweep.

use brenn_envelope::MessageEnvelope;

/// A rejected port delivery: the wire boundary itself was violated. The DOM glue
/// panics on any of these (shell/proto version skew or operator misconfig).
#[derive(Debug, Clone, PartialEq)]
pub enum ContractViolation {
    /// Event arrived on a port the component does not bind.
    WrongPort { port: String },
    /// `envelope_json` did not parse as a [`MessageEnvelope`].
    BadEnvelope(String),
}

/// Everything the DOM glue needs to emit the operator-visible malformed-body log
/// line, extracted from the envelope so all formatting stays host-tested and
/// identical across components.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultReport {
    pub channel: String,
    pub sender: String,
    pub message_id: String,
    pub reason: String,
}

impl FaultReport {
    /// Build a report from the delivering envelope and a validation reason.
    pub fn new(envelope: &MessageEnvelope, reason: String) -> Self {
        FaultReport {
            channel: envelope.channel.clone(),
            sender: envelope.sender.clone(),
            message_id: envelope.message_id.to_string(),
            reason,
        }
    }

    /// The operator log line. `context` names what was malformed (e.g.
    /// `"protobar body"`, `"meeting body"`, `"mode-clock config"`) so a buggy
    /// publisher is identifiable — and the surrounding format is shared so it
    /// never drifts between components.
    pub fn log_message(&self, context: &str) -> String {
        format!(
            "malformed {} on {} from {} (message_id {}): {}",
            context, self.channel, self.sender, self.message_id, self.reason
        )
    }
}

/// Validate the wire boundary of a port delivery: the event must have arrived on
/// one of `expected_ports`, and `envelope_json` must parse as a [`MessageEnvelope`].
/// Returns the parsed envelope for body-convention parsing, or the
/// [`ContractViolation`] the glue panics on.
pub fn parse_delivery(
    port: &str,
    expected_ports: &[&str],
    envelope_json: &str,
) -> Result<MessageEnvelope, ContractViolation> {
    if !expected_ports.contains(&port) {
        return Err(ContractViolation::WrongPort {
            port: port.to_string(),
        });
    }
    serde_json::from_str(envelope_json).map_err(|e| ContractViolation::BadEnvelope(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_surface_test_fixtures::sample_envelope_json;

    #[test]
    fn wrong_port_is_rejected() {
        assert_eq!(
            parse_delivery("other", &["config"], &sample_envelope_json("x")),
            Err(ContractViolation::WrongPort {
                port: "other".to_string()
            })
        );
    }

    #[test]
    fn any_expected_port_is_accepted() {
        for port in ["agenda", "acks"] {
            let envelope = parse_delivery(port, &["agenda", "acks"], &sample_envelope_json("body"))
                .expect("a bound port parses");
            assert_eq!(envelope.body, "body");
        }
    }

    #[test]
    fn unparseable_envelope_is_a_contract_violation() {
        assert!(matches!(
            parse_delivery("config", &["config"], "not json"),
            Err(ContractViolation::BadEnvelope(_))
        ));
    }

    #[test]
    fn log_message_names_publisher_and_context() {
        let envelope: MessageEnvelope = serde_json::from_str(&sample_envelope_json("x")).unwrap();
        let report = FaultReport::new(&envelope, "bad thing".to_string());
        let line = report.log_message("protobar body");
        assert!(line.contains("malformed protobar body"));
        assert!(line.contains("ephemeral:demo"));
        assert!(line.contains("surface:deskbar"));
        assert!(line.contains("bad thing"));
    }
}
