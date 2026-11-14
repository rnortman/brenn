//! The `messaging_messages.attachments` column: an [`Attachments`] map bound as
//! its JSON text, NULL for a row that names no blob.

use rusqlite::Row;
use uuid::Uuid;

use brenn_lib::messaging::Attachments;

/// The value a write binds for `attachments`: NULL when the map is empty.
pub(crate) fn attachments_column(attachments: &Attachments) -> Option<String> {
    (!attachments.is_empty()).then(|| {
        serde_json::to_string(attachments).expect("attachment references serialize to JSON")
    })
}

/// Decode the `attachments` value (column `col`) of message `message_id`.
///
/// # Panics
///
/// The host wrote every row, so a value that does not parse as
/// [`Attachments`] is corruption or a host bug; panics naming the message.
pub(crate) fn read_attachments(
    row: &Row<'_>,
    col: usize,
    message_id: Uuid,
) -> rusqlite::Result<Attachments> {
    let text: Option<String> = row.get(col)?;
    Ok(text.map_or_else(Attachments::default, |text| {
        parse_attachments(&text).unwrap_or_else(|e| {
            panic!(
                "messaging: message {message_id} has stored attachments that do not parse \
                 ({text:?}): {e} — host wrote every row"
            )
        })
    }))
}

/// Parse a stored `attachments` value.
pub(crate) fn parse_attachments(text: &str) -> serde_json::Result<Attachments> {
    serde_json::from_str(text)
}
