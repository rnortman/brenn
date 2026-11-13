//! The `messaging_messages.transport` column: a [`TransportMeta`] bound as its
//! JSON text, NULL for a row that carries none.

use rusqlite::Row;
use uuid::Uuid;

use brenn_lib::messaging::TransportMeta;

/// The value a write binds for `transport`.
pub(crate) fn transport_column(transport: &TransportMeta) -> String {
    serde_json::to_string(transport).expect("transport metadata serializes to JSON")
}

/// Decode the `transport` value (column `col`) of message `message_id`.
///
/// # Panics
///
/// The host wrote every row, so a value that does not parse as
/// [`TransportMeta`] is corruption or a host bug; panics naming the message.
pub(crate) fn read_transport(
    row: &Row<'_>,
    col: usize,
    message_id: Uuid,
) -> rusqlite::Result<Option<TransportMeta>> {
    let text: Option<String> = row.get(col)?;
    Ok(text.map(|text| {
        serde_json::from_str(&text).unwrap_or_else(|e| {
            panic!(
                "messaging: message {message_id} has a stored transport that does not parse \
                 ({text:?}): {e} — host wrote every row"
            )
        })
    }))
}
