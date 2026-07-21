//! Codec for the `messaging_messages.envelope_type` DB column.

use crate::messaging::ChannelScheme;

/// Codec for the `messaging_messages.envelope_type` DB column — the one place
/// the storage-only `ingress` row-kind still exists alongside the bus scheme
/// tags. This is **not** an address classifier: bus dispatch matches on
/// [`ChannelScheme`]. It exists only to decode/encode the raw column string at
/// the rusqlite boundary, where a stored row may be either a scheme-tagged bus
/// message or an `ingress` row-kind.
pub(crate) enum EnvelopeTypeColumn {
    /// A bus message row; the tag is a live [`ChannelScheme`].
    Bus(ChannelScheme),
    /// An `ingress` row-kind (channel-less; carries `ingress_source` /
    /// `ingress_summary`). Live: repo_sync writes these rows.
    // TODO(ingress-retirement): remove this variant once repo_sync publishes
    // onto a real bus channel and the ingress rows are migrated/deleted; the
    // codec then collapses to a bare `ChannelScheme`.
    Ingress,
}

impl EnvelopeTypeColumn {
    /// The string value stored in `messaging_messages.envelope_type`.
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            EnvelopeTypeColumn::Bus(scheme) => scheme.as_str(),
            EnvelopeTypeColumn::Ingress => "ingress",
        }
    }

    /// Decode the raw column string. `None` on unrecognized values; callers at
    /// host-written boundaries panic on `None` (the host wrote every row).
    pub(crate) fn parse(s: &str) -> Option<Self> {
        if s == "ingress" {
            Some(EnvelopeTypeColumn::Ingress)
        } else {
            ChannelScheme::parse(s).map(EnvelopeTypeColumn::Bus)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_column_round_trips_all_bus_schemes_and_ingress() {
        let cases = [
            (EnvelopeTypeColumn::Bus(ChannelScheme::Brenn), "brenn"),
            (EnvelopeTypeColumn::Bus(ChannelScheme::Webhook), "webhook"),
            (EnvelopeTypeColumn::Bus(ChannelScheme::Mqtt), "mqtt"),
            (
                EnvelopeTypeColumn::Bus(ChannelScheme::Ephemeral),
                "ephemeral",
            ),
            (EnvelopeTypeColumn::Bus(ChannelScheme::PwaPush), "pwa_push"),
            (EnvelopeTypeColumn::Ingress, "ingress"),
        ];
        for (col, tag) in cases {
            assert_eq!(col.as_str(), tag);
            let decoded = EnvelopeTypeColumn::parse(tag).expect("parse known tag");
            assert_eq!(decoded.as_str(), tag);
        }
    }

    #[test]
    fn envelope_column_parse_unknown_returns_none() {
        assert!(EnvelopeTypeColumn::parse("bogus").is_none());
        assert!(EnvelopeTypeColumn::parse("").is_none());
        assert!(EnvelopeTypeColumn::parse("BRENN").is_none());
    }
}
