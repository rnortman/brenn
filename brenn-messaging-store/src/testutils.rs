//! Test helpers over this crate's tables, for its own suites and the crates
//! above it.
//!
//! Available under `#[cfg(test)]` or when the `testutils` feature is enabled.

/// Seed the FK rows (user id 1, conversation `conversation_id`) required by
/// tests that insert into tables referencing `users`/`conversations`.
pub fn ensure_user_and_conv(conn: &rusqlite::Connection, conversation_id: i64) {
    conn.execute(
        "INSERT OR IGNORE INTO users (id, username, password_hash, created_at) \
         VALUES (1, 'u', 'h', '2024-01-01')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT OR IGNORE INTO conversations (id, user_id, status, created_at, updated_at) \
         VALUES (?1, 1, 'active', '2024-01-01', '2024-01-01')",
        [conversation_id],
    )
    .unwrap();
}

/// Create, write and seal one blob holding `bytes`.
pub fn seal_bytes(
    store: &std::sync::Arc<crate::blob::BlobStore>,
    content_type: &str,
    bytes: &[u8],
) -> crate::blob::BlobHold {
    let mut writer = store.create(content_type).expect("create blob");
    writer.write(0, bytes).expect("write blob");
    writer.seal().expect("seal blob")
}

/// Attachments naming each hold under its name, with a clone of each hold.
pub fn held(entries: &[(&str, &crate::blob::BlobHold)]) -> crate::blob::HeldAttachments {
    crate::blob::HeldAttachments::new(entries.iter().map(|(name, hold)| {
        (
            brenn_envelope::AttachmentName::new(*name).expect("attachment name"),
            (*hold).clone(),
        )
    }))
    .expect("attachments")
}

/// Whether the blob `id` has a file in the store directory `dir`.
pub fn holds_file(dir: &std::path::Path, id: &brenn_envelope::BlobId) -> bool {
    dir.join(id.as_str()).exists()
}

/// Cut the blob `id`'s file in the store directory `dir` to zero bytes behind
/// the store's back, so a read of any of its bytes panics.
pub fn empty_file(dir: &std::path::Path, id: &brenn_envelope::BlobId) {
    std::fs::OpenOptions::new()
        .write(true)
        .open(dir.join(id.as_str()))
        .expect("the blob's file")
        .set_len(0)
        .expect("truncating the blob's file");
}

/// The name of every entry in the store directory `dir`, sorted: the store's
/// own records and one file per blob.
pub fn blob_files(dir: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .expect("the blob directory")
        .map(|entry| {
            entry
                .expect("a blob directory entry")
                .file_name()
                .into_string()
                .expect("a UTF-8 file name")
        })
        .collect();
    names.sort();
    names
}
