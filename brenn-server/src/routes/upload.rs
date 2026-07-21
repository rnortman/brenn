//! Upload and attachment serving routes.
//!
//! `POST /app/{slug}/upload` — multipart file upload to the app's working directory.
//! `GET /app/{slug}/attachment/{upload_id}/{filename}` — serve an attached file.

use std::sync::Arc;

use axum::Extension;
use axum::extract::{Multipart, Path as AxumPath, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Json, Response};
use brenn_lib::auth::session::Session;
use brenn_lib::config::AppConfig;
use brenn_lib::conversation;
use brenn_lib::db::Db;
use brenn_lib::obs::security::{SecurityEventType, log_and_alert_security_event};
use brenn_lib::ws_types::AttachmentMeta;
use indexmap::IndexMap;
use serde::Serialize;
use tokio::time::Instant;
use tracing::warn;
use ts_rs::TS;
use uuid::Uuid;

use crate::client_ip::ClientIp;
use crate::state::{AppState, PendingUpload, PendingUploads};

/// Maximum original filename length in bytes (leaves room for UUID prefix).
const MAX_FILENAME_BYTES: usize = 200;

/// Response from a successful upload.
#[derive(Serialize, TS)]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub struct UploadResponse {
    pub upload_id: String,
    pub filename: String,
    pub media_type: String,
    #[ts(type = "number")]
    pub size: u64,
}

/// A file that has been received, validated, and written to disk.
pub struct WrittenFile {
    pub upload_id: Uuid,
    pub filename: String,
    pub disk_filename: String,
    pub media_type: String,
    pub size: u64,
    pub path: std::path::PathBuf,
}

/// Response from a target upload (files stored, ready for RunTarget via WS).
#[derive(Serialize, TS)]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub struct TargetUploadResponse {
    /// Upload IDs for each file (pass to RunTarget WS message).
    pub upload_ids: Vec<String>,
    /// Original filenames (for display).
    pub files: Vec<String>,
}

/// POST /app/{slug}/upload — accept a file upload via multipart/form-data.
///
/// When no `target` field is present (or target is "chat"), behaves as a single-file
/// upload for chat attachments. When `target` is set to an app-defined attachment target,
/// the handler runs and returns a `TargetUploadResponse`.
pub async fn upload(
    AxumPath(slug): AxumPath<String>,
    Extension(session): Extension<Session>,
    Extension(ClientIp(ip)): Extension<ClientIp>,
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<Response, (StatusCode, String)> {
    // Validate app and access.
    let app = match state.apps.get(&slug) {
        Some(app) => app,
        None => {
            log_and_alert_security_event(
                &state.alert_dispatcher,
                SecurityEventType::UnrecognizedUrl,
                ip,
                &format!("POST /app/{slug}/upload — unknown app"),
            );
            return Err((StatusCode::NOT_FOUND, "Unknown app".into()));
        }
    };
    if !app.user_has_access(&session.user.username) {
        log_and_alert_security_event(
            &state.alert_dispatcher,
            SecurityEventType::AuthFailure,
            ip,
            &format!(
                "user {} denied upload access to app {}",
                session.user.username, slug
            ),
        );
        return Err((StatusCode::FORBIDDEN, "Access denied".into()));
    }

    // Buffer all multipart fields before validation. Browsers don't guarantee
    // field ordering, so `target` may arrive after `file` fields.
    let mut files: Vec<(String, Vec<u8>)> = Vec::new();
    let mut target_field: Option<String> = None;

    // Map multipart errors to an HTTP status: 413 propagates to the client so
    // the browser can surface a specific message; all other errors become 400
    // (not 500 — IO/decode failures on untrusted input are client-side errors).
    let map_multipart_err = |e: axum::extract::multipart::MultipartError| {
        let status = if e.status() == StatusCode::PAYLOAD_TOO_LARGE {
            StatusCode::PAYLOAD_TOO_LARGE
        } else {
            StatusCode::BAD_REQUEST
        };
        (status, e.to_string())
    };

    while let Some(field) = multipart.next_field().await.map_err(|e| {
        warn!(error = %e, "upload: failed to read multipart field");
        map_multipart_err(e)
    })? {
        let name = field.name().unwrap_or("").to_string();
        match name.as_str() {
            "file" => {
                let original_filename = field.file_name().unwrap_or("").to_string();
                let bytes = field.bytes().await.map_err(|e| {
                    warn!(error = %e, "upload: failed to read file bytes");
                    map_multipart_err(e)
                })?;
                files.push((original_filename, bytes.to_vec()));
            }
            "target" => {
                let value = field.text().await.map_err(|e| {
                    warn!(error = %e, "upload: failed to read target field");
                    map_multipart_err(e)
                })?;
                target_field = Some(value);
            }
            _ => {
                // Ignore unknown fields (don't fail2ban — browsers may add extras).
            }
        }
    }

    // Resolve target. If target is specified and not "chat", look up the app-defined target.
    let target = match &target_field {
        Some(name) if name != "chat" => {
            match app.attachment_targets.iter().find(|t| t.name == *name) {
                Some(t) => Some(t),
                None => {
                    log_and_alert_security_event(
                        &state.alert_dispatcher,
                        SecurityEventType::SchemaViolation,
                        ip,
                        &format!("upload: unknown target {name:?} for app {slug}"),
                    );
                    return Err((StatusCode::BAD_REQUEST, "Unknown target".into()));
                }
            }
        }
        _ => None,
    };

    // Validate file count.
    if files.is_empty() {
        log_and_alert_security_event(
            &state.alert_dispatcher,
            SecurityEventType::SchemaViolation,
            ip,
            "upload: missing file field",
        );
        return Err((StatusCode::BAD_REQUEST, "Missing file field".into()));
    }
    if files.len() > 1 {
        // Multi-file only allowed for targets with multi=true.
        let multi_allowed = target.is_some_and(|t| t.multi);
        if !multi_allowed {
            log_and_alert_security_event(
                &state.alert_dispatcher,
                SecurityEventType::SchemaViolation,
                ip,
                "upload: multiple files without multi-file target",
            );
            return Err((StatusCode::BAD_REQUEST, "Multiple files not allowed".into()));
        }
    }

    // Validate, sanitize, and write each file to disk.
    let attachments_dir = app.working_dir.join("attachments");
    tokio::fs::create_dir_all(&attachments_dir)
        .await
        .map_err(|e| {
            warn!(
                error = %e,
                dir = %attachments_dir.display(),
                "upload: failed to create attachments directory"
            );
            (StatusCode::INTERNAL_SERVER_ERROR, "Server error".into())
        })?;

    let mut written_files: Vec<WrittenFile> = Vec::with_capacity(files.len());

    for (raw_filename, bytes) in &files {
        if bytes.is_empty() {
            return Err((StatusCode::BAD_REQUEST, "Empty file".into()));
        }

        let filename = match sanitize_filename(raw_filename) {
            Some(f) => f,
            None => {
                log_and_alert_security_event(
                    &state.alert_dispatcher,
                    SecurityEventType::MalformedMessage,
                    ip,
                    &format!("upload: bad filename: {raw_filename:?}"),
                );
                return Err((StatusCode::BAD_REQUEST, "Invalid filename".into()));
            }
        };

        // Validate extension against target's accept list.
        if let Some(t) = target {
            let ext = filename
                .rfind('.')
                .map(|i| filename[i..].to_lowercase())
                .unwrap_or_default();
            if !t.accept.iter().any(|a| a.to_lowercase() == ext) {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!("File type {ext:?} not accepted by target {:?}", t.name),
                ));
            }
        }

        let detected_type = detect_media_type(bytes);

        // Content-type spoofing check (informational).
        let extension_type = media_type_from_extension(&filename);
        if let Some(ext_type) = extension_type
            && ext_type != detected_type
            && detected_type != "application/octet-stream"
            && detected_type != "text/plain"
        {
            log_and_alert_security_event(
                &state.alert_dispatcher,
                SecurityEventType::SchemaViolation,
                ip,
                &format!(
                    "upload: extension suggests {ext_type} but content is {detected_type} for {filename}"
                ),
            );
        }

        let upload_id = Uuid::new_v4();
        let disk_filename = format!("{upload_id}_{filename}");
        let tmp_path = attachments_dir.join(format!(".tmp_{upload_id}"));
        let final_path = attachments_dir.join(&disk_filename);

        tokio::fs::write(&tmp_path, bytes).await.map_err(|e| {
            warn!(error = %e, "upload: failed to write temp file");
            cleanup_written_files(&written_files);
            (StatusCode::INTERNAL_SERVER_ERROR, "Server error".into())
        })?;

        tokio::fs::rename(&tmp_path, &final_path)
            .await
            .map_err(|e| {
                warn!(error = %e, "upload: failed to rename temp file");
                let tmp = tmp_path.clone();
                tokio::spawn(async move {
                    if let Err(e) = tokio::fs::remove_file(&tmp).await {
                        tracing::warn!(error = %e, "upload: failed to clean up temp file");
                    }
                });
                cleanup_written_files(&written_files);
                (StatusCode::INTERNAL_SERVER_ERROR, "Server error".into())
            })?;

        written_files.push(WrittenFile {
            upload_id,
            filename,
            disk_filename,
            media_type: detected_type,
            size: bytes.len() as u64,
            path: final_path,
        });
    }

    // Route: target handler or chat attachment?
    if target.is_some() {
        // Target upload: store files in PendingUploads. The actual command
        // execution happens when the frontend sends a RunTarget WS message.
        let filenames: Vec<String> = written_files.iter().map(|f| f.filename.clone()).collect();
        let upload_ids: Vec<String> = written_files
            .iter()
            .map(|f| f.upload_id.to_string())
            .collect();

        {
            let mut pending_guard = state.pending_uploads.lock().await;
            for wf in written_files {
                pending_guard.insert(
                    wf.upload_id,
                    PendingUpload {
                        app_slug: slug.clone(),
                        filename: wf.filename,
                        disk_filename: wf.disk_filename,
                        media_type: wf.media_type,
                        size: wf.size,
                        uploaded_at: Instant::now(),
                        uploader_user_id: session.user.id,
                    },
                );
            }
        }

        let resp = TargetUploadResponse {
            upload_ids,
            files: filenames,
        };
        Ok(Json(resp).into_response())
    } else {
        // Chat attachment: single file, register in pending uploads.
        assert!(
            written_files.len() == 1,
            "chat upload must have exactly one file"
        );
        let wf = written_files.into_iter().next().unwrap();

        let pending = PendingUpload {
            app_slug: slug,
            filename: wf.filename.clone(),
            disk_filename: wf.disk_filename.clone(),
            media_type: wf.media_type.clone(),
            size: wf.size,
            uploaded_at: Instant::now(),
            uploader_user_id: session.user.id,
        };
        state
            .pending_uploads
            .lock()
            .await
            .insert(wf.upload_id, pending);

        let resp = UploadResponse {
            upload_id: wf.upload_id.to_string(),
            filename: wf.filename,
            media_type: wf.media_type,
            size: wf.size,
        };
        Ok(Json(resp).into_response())
    }
}

/// Best-effort cleanup of already-written files on partial upload failure.
/// Spawns async tasks so the error response isn't delayed.
fn cleanup_written_files(files: &[WrittenFile]) {
    if files.is_empty() {
        return;
    }
    warn!(
        count = files.len(),
        "upload: cleaning up {} previously written file(s) after partial failure",
        files.len()
    );
    for file in files {
        let path = file.path.clone();
        tokio::spawn(async move {
            if let Err(e) = tokio::fs::remove_file(&path).await {
                tracing::warn!(
                    error = %e,
                    path = %path.display(),
                    "upload: failed to clean up file after partial failure"
                );
            }
        });
    }
}

/// GET /app/{slug}/attachment/{upload_id}/{filename} — serve an attached file.
pub async fn serve_attachment(
    AxumPath((slug, upload_id_str, filename)): AxumPath<(String, String, String)>,
    Extension(session): Extension<Session>,
    Extension(ClientIp(ip)): Extension<ClientIp>,
    State(state): State<AppState>,
) -> Result<Response, StatusCode> {
    // Validate app and access.
    let app = match state.apps.get(&slug) {
        Some(app) => app,
        None => {
            log_and_alert_security_event(
                &state.alert_dispatcher,
                SecurityEventType::UnrecognizedUrl,
                ip,
                &format!("GET /app/{slug}/attachment/... — unknown app"),
            );
            return Err(StatusCode::NOT_FOUND);
        }
    };
    if !app.user_has_access(&session.user.username) {
        log_and_alert_security_event(
            &state.alert_dispatcher,
            SecurityEventType::AuthFailure,
            ip,
            &format!(
                "user {} denied attachment access to app {}",
                session.user.username, slug
            ),
        );
        return Err(StatusCode::FORBIDDEN);
    }

    // Parse upload_id as UUID.
    let upload_id: Uuid = match upload_id_str.parse() {
        Ok(id) => id,
        Err(_) => {
            log_and_alert_security_event(
                &state.alert_dispatcher,
                SecurityEventType::SchemaViolation,
                ip,
                &format!("attachment: invalid upload_id: {upload_id_str}"),
            );
            return Err(StatusCode::NOT_FOUND);
        }
    };

    // Look up in DB to get media_type and verify the attachment exists.
    let disk_filename = format!("{upload_id}_{filename}");
    let (media_type, db_disk_filename) = {
        let conn = state.db.lock().await;
        let mut stmt = conn
            .prepare(
                "SELECT media_type, disk_filename FROM message_attachments WHERE upload_id = ?1",
            )
            .expect("failed to prepare attachment lookup");
        stmt.query_row([upload_id.to_string()], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|_| StatusCode::NOT_FOUND)?
    };

    // Verify filename matches (prevents URL manipulation).
    if db_disk_filename != disk_filename {
        return Err(StatusCode::NOT_FOUND);
    }

    // Resolve file path (always under working_dir/attachments/).
    let file_path = app.working_dir.join("attachments").join(&disk_filename);

    // Belt+suspenders containment check.
    let canonical = file_path
        .canonicalize()
        .map_err(|_| StatusCode::NOT_FOUND)?;
    let canonical_working_dir = app
        .working_dir
        .canonicalize()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if !canonical.starts_with(&canonical_working_dir) {
        log_and_alert_security_event(
            &state.alert_dispatcher,
            SecurityEventType::MalformedMessage,
            ip,
            &format!("attachment path traversal attempt: {disk_filename}"),
        );
        return Err(StatusCode::NOT_FOUND);
    }

    let bytes = tokio::fs::read(&canonical).await.map_err(|e| {
        warn!(error = %e, path = %canonical.display(), "attachment: read failed");
        StatusCode::NOT_FOUND
    })?;

    // Content-Disposition: inline for images, attachment for others.
    let disposition = if media_type.starts_with("image/") {
        "inline".to_string()
    } else {
        format!("attachment; filename=\"{filename}\"")
    };

    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, media_type),
            (header::CONTENT_DISPOSITION, disposition),
            (header::CACHE_CONTROL, "private, max-age=86400".to_string()),
        ],
        bytes,
    )
        .into_response())
}

// ---------------------------------------------------------------------------
// Filename sanitization
// ---------------------------------------------------------------------------

/// Sanitize a user-provided filename. Returns `None` if the filename is
/// unsalvageable (empty after sanitization, contains null bytes, etc.).
fn sanitize_filename(raw: &str) -> Option<String> {
    // Strip any path components — take only the final segment.
    let name = raw.rsplit(['/', '\\']).next().unwrap_or(raw);

    // Reject null bytes, control characters, and quotes (which would break
    // Content-Disposition headers).
    if name.bytes().any(|b| b == 0 || b < 0x20 || b == b'"') {
        return None;
    }

    // Reject empty.
    let name = name.trim();
    if name.is_empty() {
        return None;
    }

    // Truncate to MAX_FILENAME_BYTES (on a char boundary).
    let truncated = if name.len() > MAX_FILENAME_BYTES {
        let mut end = MAX_FILENAME_BYTES;
        while end > 0 && !name.is_char_boundary(end) {
            end -= 1;
        }
        &name[..end]
    } else {
        name
    };

    if truncated.is_empty() {
        return None;
    }

    Some(truncated.to_string())
}

// ---------------------------------------------------------------------------
// Media type detection
// ---------------------------------------------------------------------------

/// Detect media type from file content (magic bytes).
fn detect_media_type(bytes: &[u8]) -> String {
    // JPEG: FF D8 FF
    if bytes.len() >= 3 && bytes[0] == 0xFF && bytes[1] == 0xD8 && bytes[2] == 0xFF {
        return "image/jpeg".to_string();
    }
    // PNG: 89 50 4E 47 0D 0A 1A 0A
    if bytes.len() >= 8 && bytes[..8] == [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A] {
        return "image/png".to_string();
    }
    // GIF: GIF87a or GIF89a (full 6-byte magic, not just "GIF")
    if bytes.len() >= 6 && (&bytes[..6] == b"GIF87a" || &bytes[..6] == b"GIF89a") {
        return "image/gif".to_string();
    }
    // WebP: RIFF....WEBP
    if bytes.len() >= 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        return "image/webp".to_string();
    }
    // PDF: %PDF
    if bytes.len() >= 4 && &bytes[..4] == b"%PDF" {
        return "application/pdf".to_string();
    }

    // Text: check if valid UTF-8 with no null bytes (check a reasonable prefix).
    // Null bytes are valid UTF-8 but indicate binary content.
    let check_len = bytes.len().min(8192);
    if std::str::from_utf8(&bytes[..check_len]).is_ok() && !bytes[..check_len].contains(&0) {
        return "text/plain".to_string();
    }

    "application/octet-stream".to_string()
}

/// Guess media type from file extension (for spoofing detection only).
fn media_type_from_extension(filename: &str) -> Option<&'static str> {
    let ext = filename.rsplit('.').next()?.to_ascii_lowercase();
    match ext.as_str() {
        "jpg" | "jpeg" => Some("image/jpeg"),
        "png" => Some("image/png"),
        "gif" => Some("image/gif"),
        "webp" => Some("image/webp"),
        "pdf" => Some("application/pdf"),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Attachment resolution for SendMessage
// ---------------------------------------------------------------------------

/// Resolved attachment ready for persistence and CC notification.
#[derive(Debug, Clone)]
pub struct ResolvedAttachment {
    pub upload_id: Uuid,
    pub filename: String,
    pub disk_filename: String,
    pub media_type: String,
    pub size: u64,
}

impl ResolvedAttachment {
    pub fn to_meta(&self) -> AttachmentMeta {
        AttachmentMeta {
            upload_id: self.upload_id.to_string(),
            filename: self.filename.clone(),
            media_type: self.media_type.clone(),
            size: self.size,
        }
    }

    /// Format the CC notification line for this attachment.
    pub fn cc_notification(&self) -> String {
        let human_size = human_file_size(self.size);
        format!(
            "[Attached file: attachments/{} ({}, {})]",
            self.disk_filename, self.media_type, human_size
        )
    }
}

/// Resolve attachment references from a SendMessage against the pending uploads registry.
///
/// Returns `Ok(vec)` on success (may be empty), `Err(reason)` on failure.
/// On failure, the caller should send an error to the client and/or log for fail2ban.
pub async fn resolve_attachments(
    refs: &[brenn_lib::ws_types::AttachmentRef],
    app_slug: &str,
    user_id: i64,
    working_dir: &std::path::Path,
    pending: &PendingUploads,
) -> Result<Vec<ResolvedAttachment>, String> {
    if refs.is_empty() {
        return Ok(vec![]);
    }

    let mut resolved = Vec::with_capacity(refs.len());
    let mut pending_guard = pending.lock().await;

    for r in refs {
        let upload_id: Uuid = r
            .upload_id
            .parse()
            .map_err(|_| format!("invalid upload_id: {}", r.upload_id))?;

        let entry = pending_guard
            .get(&upload_id)
            .ok_or_else(|| format!("upload_id not found: {upload_id}"))?;

        // App slug must match.
        if entry.app_slug != app_slug {
            return Err(format!(
                "upload_id {upload_id} belongs to app {}, not {app_slug}",
                entry.app_slug
            ));
        }

        // Uploader must match sender.
        if entry.uploader_user_id != user_id {
            return Err(format!(
                "upload_id {upload_id} was uploaded by user {}, not {user_id}",
                entry.uploader_user_id
            ));
        }

        // Verify file still exists on disk.
        let file_path = working_dir.join("attachments").join(&entry.disk_filename);
        if !file_path.exists() {
            return Err(format!(
                "upload_id {upload_id}: file missing from disk: {}",
                entry.disk_filename
            ));
        }

        resolved.push(ResolvedAttachment {
            upload_id,
            filename: entry.filename.clone(),
            disk_filename: entry.disk_filename.clone(),
            media_type: entry.media_type.clone(),
            size: entry.size,
        });
    }

    // Remove resolved uploads from pending (they'll be persisted to DB).
    for r in &resolved {
        pending_guard.remove(&r.upload_id);
    }

    Ok(resolved)
}

fn human_file_size(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{bytes} B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    }
}

// ---------------------------------------------------------------------------
// Orphan cleanup
// ---------------------------------------------------------------------------

/// Background task: periodically clean up orphaned upload files and expired pending entries.
///
/// Runs every 10 minutes. Deletes files in `{working_dir}/attachments/` that are:
/// - Older than 24 hours (by mtime)
/// - Not referenced in `message_attachments`
///
/// Also prunes expired entries from the in-memory pending uploads map.
pub async fn orphan_cleanup_loop(
    apps: Arc<IndexMap<String, AppConfig>>,
    pending: PendingUploads,
    db: Db,
) {
    use std::time::{Duration, SystemTime};
    use tokio::time;

    let mut interval = time::interval(Duration::from_secs(600));
    let max_age = Duration::from_secs(24 * 3600);

    loop {
        interval.tick().await;

        // Prune expired pending uploads.
        {
            let mut guard = pending.lock().await;
            let now = Instant::now();
            guard.retain(|_, v| {
                now.duration_since(v.uploaded_at) < tokio::time::Duration::from_secs(24 * 3600)
            });
        }

        // Scan each app's attachments directory.
        for app in apps.values() {
            let dir = app.working_dir.join("attachments");
            let mut entries = match tokio::fs::read_dir(&dir).await {
                Ok(e) => e,
                Err(_) => continue, // Directory may not exist yet.
            };

            while let Ok(Some(entry)) = entries.next_entry().await {
                let filename = entry.file_name();
                let filename_str = filename.to_string_lossy();

                // Skip temp files (they'll be cleaned up by their creator or next cycle).
                if filename_str.starts_with(".tmp_") {
                    continue;
                }

                // Extract UUID from filename: {uuid}_{original_name}
                let uuid_str = match filename_str.find('_') {
                    Some(pos) => &filename_str[..pos],
                    None => continue, // Not our file format.
                };

                // Check mtime.
                let metadata = match entry.metadata().await {
                    Ok(m) => m,
                    Err(_) => continue,
                };
                let modified = match metadata.modified() {
                    Ok(m) => m,
                    Err(_) => continue,
                };
                let age = SystemTime::now()
                    .duration_since(modified)
                    .unwrap_or_default();

                if age < max_age {
                    continue; // Not old enough.
                }

                // Check if referenced in DB.
                let referenced = {
                    let conn = db.lock().await;
                    conversation::attachment_exists(&conn, uuid_str)
                };

                if !referenced {
                    if let Err(e) = tokio::fs::remove_file(entry.path()).await {
                        warn!(
                            error = %e,
                            file = %filename_str,
                            "orphan cleanup: failed to remove file"
                        );
                    } else {
                        tracing::debug!(file = %filename_str, "orphan cleanup: removed orphan");
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ts_rs_export() {
        let cfg = ts_rs::Config::default();
        UploadResponse::export(&cfg).expect("UploadResponse export failed");
        TargetUploadResponse::export(&cfg).expect("TargetUploadResponse export failed");
    }

    #[test]
    fn sanitize_normal_filename() {
        assert_eq!(sanitize_filename("receipt.jpg"), Some("receipt.jpg".into()));
    }

    #[test]
    fn sanitize_strips_path() {
        assert_eq!(
            sanitize_filename("../../../etc/passwd"),
            Some("passwd".into())
        );
        assert_eq!(
            sanitize_filename("C:\\Users\\evil\\file.exe"),
            Some("file.exe".into())
        );
    }

    #[test]
    fn sanitize_rejects_null_bytes() {
        assert_eq!(sanitize_filename("file\0.txt"), None);
    }

    #[test]
    fn sanitize_rejects_control_chars() {
        assert_eq!(sanitize_filename("file\x01.txt"), None);
    }

    #[test]
    fn sanitize_rejects_quotes() {
        assert_eq!(sanitize_filename("file\"name.txt"), None);
    }

    #[test]
    fn sanitize_rejects_empty() {
        assert_eq!(sanitize_filename(""), None);
        assert_eq!(sanitize_filename("   "), None);
        assert_eq!(sanitize_filename("/"), None);
    }

    #[test]
    fn sanitize_truncates_long_filename() {
        let long_name = "a".repeat(300);
        let result = sanitize_filename(&long_name).unwrap();
        assert!(result.len() <= MAX_FILENAME_BYTES);
        assert_eq!(result.len(), MAX_FILENAME_BYTES);
    }

    #[test]
    fn detect_jpeg() {
        let bytes = [0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10];
        assert_eq!(detect_media_type(&bytes), "image/jpeg");
    }

    #[test]
    fn detect_png() {
        let bytes = [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00];
        assert_eq!(detect_media_type(&bytes), "image/png");
    }

    #[test]
    fn detect_gif87a() {
        assert_eq!(detect_media_type(b"GIF87a\x00\x00"), "image/gif");
    }

    #[test]
    fn detect_gif89a() {
        assert_eq!(detect_media_type(b"GIF89a\x00\x00"), "image/gif");
    }

    #[test]
    fn detect_gif_prefix_is_not_enough() {
        // "GIFted" should not be detected as GIF — need full magic.
        assert_eq!(detect_media_type(b"GIFted programmer"), "text/plain");
    }

    #[test]
    fn detect_pdf() {
        assert_eq!(detect_media_type(b"%PDF-1.4"), "application/pdf");
    }

    #[test]
    fn detect_text() {
        assert_eq!(detect_media_type(b"hello world"), "text/plain");
    }

    #[test]
    fn detect_binary() {
        let bytes = [0x00, 0x01, 0x02, 0x03];
        assert_eq!(detect_media_type(&bytes), "application/octet-stream");
    }

    #[test]
    fn human_size_formatting() {
        assert_eq!(human_file_size(500), "500 B");
        assert_eq!(human_file_size(1536), "1.5 KB");
        assert_eq!(human_file_size(1_572_864), "1.5 MB");
    }

    // -----------------------------------------------------------------------
    // Integration tests for upload and serve_attachment routes
    // -----------------------------------------------------------------------

    use std::net::SocketAddr;
    use std::sync::Arc;

    use axum::body::Body;
    use axum::extract::connect_info::MockConnectInfo;
    use axum::http::{Request, StatusCode};
    use brenn_lib::db;
    use indexmap::IndexMap;
    use tower::ServiceExt;

    use crate::router::build_router;
    use crate::test_support::app_config::default_test_app_config;
    use crate::test_support::http::{body_string, multipart_body, setup_authenticated_user};
    use crate::test_support::state::{test_app_with_working_dir, test_state_with_apps};

    #[tokio::test]
    async fn upload_returns_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let (app, db) = test_app_with_working_dir(dir.path().to_path_buf());
        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        let (content_type, body) = multipart_body("hello.txt", b"Hello world");

        let response = app
            .oneshot(
                Request::post("/app/test/upload")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .header("content-type", content_type)
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body_str = body_string(response.into_body()).await;
        let resp: serde_json::Value = serde_json::from_str(&body_str).unwrap();
        assert_eq!(resp["filename"], "hello.txt");
        assert_eq!(resp["media_type"], "text/plain");
        assert_eq!(resp["size"], 11);
        assert!(resp["upload_id"].as_str().is_some());

        // Verify file exists on disk.
        let upload_id = resp["upload_id"].as_str().unwrap();
        let disk_path = dir
            .path()
            .join("attachments")
            .join(format!("{upload_id}_hello.txt"));
        assert!(
            disk_path.exists(),
            "file should exist at {}",
            disk_path.display()
        );
    }

    #[tokio::test]
    async fn upload_detects_jpeg_media_type() {
        let dir = tempfile::tempdir().unwrap();
        let (app, db) = test_app_with_working_dir(dir.path().to_path_buf());
        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        // Fake JPEG magic bytes.
        let mut jpeg_bytes = vec![0xFF, 0xD8, 0xFF, 0xE0];
        jpeg_bytes.extend_from_slice(&[0u8; 100]);
        let (content_type, body) = multipart_body("photo.jpg", &jpeg_bytes);

        let response = app
            .oneshot(
                Request::post("/app/test/upload")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .header("content-type", content_type)
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body_str = body_string(response.into_body()).await;
        let resp: serde_json::Value = serde_json::from_str(&body_str).unwrap();
        assert_eq!(resp["media_type"], "image/jpeg");
    }

    #[tokio::test]
    async fn upload_requires_auth() {
        let dir = tempfile::tempdir().unwrap();
        let (app, _db) = test_app_with_working_dir(dir.path().to_path_buf());
        let (content_type, body) = multipart_body("test.txt", b"data");

        let response = app
            .oneshot(
                Request::post("/app/test/upload")
                    .header("content-type", content_type)
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        // Should redirect to login (302) since no session cookie.
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
    }

    #[tokio::test]
    async fn upload_unknown_app_returns_404() {
        let dir = tempfile::tempdir().unwrap();
        let (app, db) = test_app_with_working_dir(dir.path().to_path_buf());
        let (session_token, _csrf) = setup_authenticated_user(&db).await;
        let (content_type, body) = multipart_body("test.txt", b"data");

        let response = app
            .oneshot(
                Request::post("/app/nonexistent/upload")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .header("content-type", content_type)
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn serve_attachment_after_upload_and_persist() {
        let dir = tempfile::tempdir().unwrap();
        // We need to share the state across two requests, so build an AppState
        // with the test working_dir directly.
        let mut apps = IndexMap::new();
        let mut cfg = default_test_app_config("test", "Test App");
        cfg.working_dir = dir.path().to_path_buf();
        apps.insert("test".to_string(), cfg);
        let db = db::init_db_memory();
        let state = test_state_with_apps(&db, Arc::new(apps));

        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        // Step 1: Upload a file.
        let (content_type, body) = multipart_body("note.txt", b"Hello from test");
        let app = build_router(state.clone(), None, 0, 2576)
            .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))));
        let response = app
            .oneshot(
                Request::post("/app/test/upload")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .header("content-type", content_type)
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let upload_resp: serde_json::Value =
            serde_json::from_str(&body_string(response.into_body()).await).unwrap();
        let upload_id = upload_resp["upload_id"].as_str().unwrap();

        // Step 2: Simulate persistence (normally done by persist_and_send).
        // Insert a message and attachment into DB.
        {
            let conn = db.lock().await;
            let user_id = conn
                .query_row("SELECT id FROM users LIMIT 1", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap();
            let conv_id =
                brenn_lib::conversation::create_conversation(&conn, user_id, "test", false);
            let (msg_id, _seq) = brenn_lib::conversation::append_message(
                &conn,
                conv_id,
                brenn_lib::conversation::MessageDirection::Outgoing,
                "user",
                None,
                None,
                r#"{"type":"user","message":{"role":"user","content":"test"}}"#,
                Some(user_id),
                None,
                None,
            );
            brenn_lib::conversation::insert_attachments(
                &conn,
                &[brenn_lib::conversation::StoredAttachment {
                    upload_id: upload_id.to_string(),
                    message_id: msg_id,
                    filename: "note.txt".to_string(),
                    media_type: "text/plain".to_string(),
                    size: 15,
                    disk_filename: format!("{upload_id}_note.txt"),
                }],
            );
        }

        // Step 3: Serve the attachment.
        let app2 = build_router(state, None, 0, 2576)
            .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))));
        let response = app2
            .oneshot(
                Request::get(format!("/app/test/attachment/{upload_id}/note.txt"))
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "text/plain"
        );
        let served_body = body_string(response.into_body()).await;
        assert_eq!(served_body, "Hello from test");
    }

    #[tokio::test]
    async fn serve_attachment_not_found_without_db_record() {
        let dir = tempfile::tempdir().unwrap();
        let (app, db) = test_app_with_working_dir(dir.path().to_path_buf());
        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        let response = app
            .oneshot(
                Request::get(format!(
                    "/app/test/attachment/{}/fake.txt",
                    uuid::Uuid::new_v4()
                ))
                .header("cookie", format!("brenn_session={session_token}"))
                .body(Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
