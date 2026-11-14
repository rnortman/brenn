//! The `messaging_messages.body` / `content_type` column pair: how a
//! [`MessageBody`] is bound for a write and decoded from a read.
//!
//! The body column is declared `TEXT`, and holds two storage classes: a text
//! body is bound as text and lands as a TEXT value; a non-text body is bound as
//! bytes and lands as a BLOB value, which SQLite keeps byte-exact under TEXT
//! affinity. The full-text index, its view and its triggers key on that storage
//! class (`typeof(body) = 'text'`), so the class a row carries is the text rule
//! as it stood when the row was written.

use rusqlite::Row;
use rusqlite::types::{ToSql, ToSqlOutput, ValueRef};
use uuid::Uuid;

use brenn_lib::messaging::MessageBody;

/// A body bound to the `body` column: TEXT for a text body, BLOB otherwise.
pub(crate) struct BodyValue<'a>(&'a MessageBody);

impl ToSql for BodyValue<'_> {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(match self.0.as_text() {
            Some(text) => ValueRef::Text(text.as_bytes()),
            None => ValueRef::Blob(self.0.as_bytes()),
        }))
    }
}

/// The two values a write binds for `body`: the `body` column's value and the
/// `content_type` column's value (NULL for an untyped body).
pub(crate) fn body_columns(body: &MessageBody) -> (BodyValue<'_>, Option<&str>) {
    (BodyValue(body), body.content_type())
}

/// Decode the body of message `message_id` from its `body` (column
/// `body_col`) and `content_type` (column `ct_col`) values.
///
/// # Panics
///
/// The host wrote every row, so a row that does not decode is corruption or a
/// text rule changed without migrating the rows it reclassifies. Panics, naming
/// the message, if the body is neither TEXT nor BLOB, if the content type and
/// bytes are refused by [`MessageBody::from_parts`], or if the value's storage
/// class disagrees with the body kind its content type makes it.
pub(crate) fn read_body(
    row: &Row<'_>,
    body_col: usize,
    ct_col: usize,
    message_id: Uuid,
) -> rusqlite::Result<MessageBody> {
    let content_type: Option<String> = row.get(ct_col)?;
    let (stored_as_text, bytes) = match row.get_ref(body_col)? {
        ValueRef::Text(bytes) => (true, bytes),
        ValueRef::Blob(bytes) => (false, bytes),
        other => panic!(
            "messaging: message {message_id} has a body of storage class {:?}; \
             the host writes only TEXT or BLOB",
            other.data_type()
        ),
    };
    let body = MessageBody::from_parts(content_type.as_deref(), bytes).unwrap_or_else(|e| {
        panic!(
            "messaging: message {message_id} has a stored body that does not decode \
             (content type {content_type:?}): {e}"
        )
    });
    assert_eq!(
        body.is_text(),
        stored_as_text,
        "messaging: message {message_id} is stored as {} but its content type {content_type:?} \
         makes it a {} body; the text rule changed without migrating this row",
        if stored_as_text { "TEXT" } else { "BLOB" },
        if body.is_text() { "text" } else { "non-text" },
    );
    Ok(body)
}
