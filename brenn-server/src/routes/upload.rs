//! Upload and attachment serving routes.
//!
//! `POST /app/{slug}/upload` — multipart file upload to the app's working
//! directory, or to its secrets directory when the upload is flagged secret.
//! `GET /app/{slug}/attachment/{upload_id}/{filename}` — serve an attached file.

use axum::Extension;
use axum::extract::{Multipart, Path as AxumPath, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Json, Response};
use brenn_db::Db;
use brenn_db::auth::session::Session;
use brenn_db::conversation;
use brenn_obs::security::{SecurityEventType, log_and_alert_security_event};
use brenn_ws_types::AttachmentMeta;
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
///
/// A `secret` field with value `"1"` marks a chat upload secret: its bytes go to
/// one 0600 file in [`AppConfig::secrets_dir`](brenn_lib::config::AppConfig::secrets_dir)
/// instead of `working_dir/attachments/`. Any other value, or `secret` with a
/// non-chat target, is rejected.
pub async fn upload(
    AxumPath(slug): AxumPath<String>,
    Extension(session): Extension<Session>,
    Extension(ClientIp(ip)): Extension<ClientIp>,
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<Response, (StatusCode, String)> {
    // Validate app and access.
    let apps = state.apps.load();
    let app = match apps.get(&slug) {
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
    let mut secret_field: Option<String> = None;

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
            "secret" => {
                let value = field.text().await.map_err(|e| {
                    warn!(error = %e, "upload: failed to read secret field");
                    map_multipart_err(e)
                })?;
                secret_field = Some(value);
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

    let secret = match secret_field.as_deref() {
        None => false,
        Some("1") => true,
        Some(other) => {
            log_and_alert_security_event(
                &state.alert_dispatcher,
                SecurityEventType::SchemaViolation,
                ip,
                &format!("upload: invalid secret value {other:?} for app {slug}"),
            );
            return Err((StatusCode::BAD_REQUEST, "Invalid secret field".into()));
        }
    };
    // Targets run commands over working-directory files; a secret never goes there.
    if secret && let Some(t) = target {
        log_and_alert_security_event(
            &state.alert_dispatcher,
            SecurityEventType::SchemaViolation,
            ip,
            &format!(
                "upload: secret combined with target {:?} for app {slug}",
                t.name
            ),
        );
        return Err((
            StatusCode::BAD_REQUEST,
            "Secret uploads cannot use a target".into(),
        ));
    }

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
    let attachments_dir = if secret {
        let dir = app.secrets_dir();
        ensure_secrets_dir(&dir).await.map_err(|e| {
            warn!(
                error = %e,
                dir = %dir.display(),
                "upload: failed to prepare secrets directory"
            );
            (StatusCode::INTERNAL_SERVER_ERROR, "Server error".into())
        })?;
        dir
    } else {
        let dir = app.attachments_dir();
        // TODO(security-host-paths-follow-agent-symlinks): create_dir_all
        // follows a symlink at `attachments/` or any ancestor; the upload lands at its target.
        tokio::fs::create_dir_all(&dir).await.map_err(|e| {
            warn!(
                error = %e,
                dir = %dir.display(),
                "upload: failed to create attachments directory"
            );
            (StatusCode::INTERNAL_SERVER_ERROR, "Server error".into())
        })?;
        dir
    };

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

        // 0o666 is the default `tokio::fs::write` would use: the umask decides.
        let mode = if secret { 0o600 } else { 0o666 };
        write_new_file(&tmp_path, bytes, mode).await.map_err(|e| {
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
                        secret: false,
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
            secret,
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

/// Make `dir` a real 0700 directory: create it with that mode if missing, then
/// set the mode unconditionally so a directory created wider (by an operator
/// or an earlier process) is tightened rather than trusted. A symlink or any
/// non-directory at `dir` itself is refused. Symlinks in its ancestors are
/// followed, and each later call resolves the path afresh.
// TODO(security-host-paths-follow-agent-symlinks): confine
// host-side writes under agent-writable trees to their sanctioned root.
async fn ensure_secrets_dir(dir: &std::path::Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    match tokio::fs::DirBuilder::new().mode(0o700).create(dir).await {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e),
    }
    let meta = tokio::fs::symlink_metadata(dir).await?;
    if !meta.file_type().is_dir() {
        return Err(std::io::Error::other(format!(
            "{} is not a directory",
            dir.display()
        )));
    }
    tokio::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).await
}

/// Write `bytes` to a new file created with `mode` (before the umask).
/// `create_new` refuses any existing path, so a planted file or symlink at
/// `path` is an error rather than a write-through. A failed write removes the
/// partial file: the sweep never touches temp names, so nothing else would.
async fn write_new_file(path: &std::path::Path, bytes: &[u8], mode: u32) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;

    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(mode)
        .open(path)
        .await?;
    let written = match file.write_all(bytes).await {
        Ok(()) => file.flush().await,
        Err(e) => Err(e),
    };
    drop(file);
    if written.is_err()
        && let Err(e) = tokio::fs::remove_file(path).await
    {
        warn!(error = %e, path = %path.display(), "upload: failed to remove partial temp file");
    }
    written
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
    let apps = state.apps.load();
    let app = match apps.get(&slug) {
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
    let (media_type, db_disk_filename, secret) = {
        let conn = state.db.lock().await;
        let mut stmt = conn
            .prepare(
                "SELECT media_type, disk_filename, secret \
                 FROM message_attachments WHERE upload_id = ?1",
            )
            .expect("failed to prepare attachment lookup");
        stmt.query_row([upload_id.to_string()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, bool>(2)?,
            ))
        })
        .map_err(|_| StatusCode::NOT_FOUND)?
    };

    // No route serves a secret; the frontend never builds this URL, so one
    // arriving is crafted.
    if secret {
        log_and_alert_security_event(
            &state.alert_dispatcher,
            SecurityEventType::MalformedMessage,
            ip,
            &format!("attachment: secret requested: {upload_id}"),
        );
        return Err(StatusCode::NOT_FOUND);
    }

    // Verify filename matches (prevents URL manipulation).
    if db_disk_filename != disk_filename {
        return Err(StatusCode::NOT_FOUND);
    }

    let file_path = app.attachments_dir().join(&disk_filename);

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
    // Null bytes are valid UTF-8 but indicate binary content. A cut prefix may
    // end inside a multi-byte character; that incomplete tail is not an error.
    let check_len = bytes.len().min(8192);
    let prefix = &bytes[..check_len];
    let utf8 = match std::str::from_utf8(prefix) {
        Ok(_) => true,
        Err(e) => e.error_len().is_none() && check_len < bytes.len(),
    };
    if utf8 && !prefix.contains(&0) {
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
    /// The file is in the app's secrets directory.
    pub secret: bool,
    /// The path CC is told: `attachments/<disk>` relative to its working
    /// directory, or a secret's absolute agent-visible path.
    pub cc_path: String,
}

impl ResolvedAttachment {
    pub fn to_meta(&self) -> AttachmentMeta {
        AttachmentMeta {
            upload_id: self.upload_id.to_string(),
            filename: self.filename.clone(),
            media_type: self.media_type.clone(),
            size: self.size,
            secret: self.secret,
        }
    }

    /// Format the CC notification line for this attachment. A secret's line
    /// carries its path, type and size and the handling instruction, never
    /// its contents.
    pub fn cc_notification(&self) -> String {
        let human_size = human_file_size(self.size);
        if self.secret {
            format!(
                "[Attached secret: {} ({}, {}) — this file is a secret: do not read, print \
                 or display its contents; move it out of this directory to where it is \
                 needed (the directory is an inbox Brenn prunes, not a store) and tell the \
                 user where it went]",
                self.cc_path, self.media_type, human_size
            )
        } else {
            format!(
                "[Attached file: {} ({}, {})]",
                self.cc_path, self.media_type, human_size
            )
        }
    }
}

/// Resolve attachment references from a SendMessage against the pending uploads registry.
///
/// Returns `Ok(vec)` on success (may be empty), `Err(reason)` on failure.
/// On failure, the caller should send an error to the client and/or log for fail2ban.
pub async fn resolve_attachments(
    refs: &[brenn_ws_types::AttachmentRef],
    app_slug: &str,
    user_id: i64,
    app_config: &brenn_lib::config::AppConfig,
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
        let file_path = if entry.secret {
            app_config.secrets_dir().join(&entry.disk_filename)
        } else {
            app_config.attachments_dir().join(&entry.disk_filename)
        };
        if !file_path.exists() {
            return Err(format!(
                "upload_id {upload_id}: file missing from disk: {}",
                entry.disk_filename
            ));
        }

        let cc_path = if entry.secret {
            // The home mapping always covers `state_dir`; no mapping means the
            // config invariant is broken and the agent would get a path it
            // cannot open.
            let agent_path = app_config
                .path_mapper
                .to_container(&file_path)
                .unwrap_or_else(|| {
                    panic!(
                        "secrets path {} has no container mapping",
                        file_path.display()
                    )
                });
            agent_path
                .into_os_string()
                .into_string()
                .unwrap_or_else(|p| panic!("secrets path {p:?} is not UTF-8"))
        } else {
            // CC runs in `working_dir`, so it is told the path relative to it.
            let relative = file_path
                .strip_prefix(&app_config.working_dir)
                .unwrap_or_else(|_| {
                    panic!(
                        "attachment path {} is not under the working directory",
                        file_path.display()
                    )
                });
            relative
                .to_str()
                .unwrap_or_else(|| panic!("attachment path {relative:?} is not UTF-8"))
                .to_string()
        };

        resolved.push(ResolvedAttachment {
            upload_id,
            filename: entry.filename.clone(),
            disk_filename: entry.disk_filename.clone(),
            media_type: entry.media_type.clone(),
            size: entry.size,
            secret: entry.secret,
            cc_path,
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
/// Runs every 10 minutes. For each app, sweeps `{working_dir}/attachments/` and
/// the app's secrets directory with [`sweep_orphans`].
///
/// Also prunes expired entries from the in-memory pending uploads map.
pub async fn orphan_cleanup_loop(
    apps: brenn_lib::config::AppTable,
    pending: PendingUploads,
    db: Db,
) {
    use std::time::Duration;
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

        // Snapshot is per cycle: a reload between cycles changes `working_dir`
        // and the next cycle sweeps the new one.
        let apps = apps.load();
        for app in apps.values() {
            sweep_orphans(&app.attachments_dir(), &db, max_age).await;
            sweep_orphans(&app.secrets_dir(), &db, max_age).await;
        }
    }
}

/// Delete the files in `dir` that Brenn wrote and nothing references: a name
/// of the form `<uuid>_<name>` (the UUID in canonical hyphenated form), an
/// mtime older than `max_age`, and an upload id absent from
/// `message_attachments`. Any other name — a temp file, or a file the agent
/// put there — is not Brenn's to remove. A missing `dir` is skipped silently;
/// any other failure to list it ends this sweep of `dir` with a warning, and a
/// file whose metadata cannot be read is left in place with a warning.
// TODO(security-host-paths-follow-agent-symlinks): read_dir follows a symlink at
// `dir` or any ancestor; the sweep then deletes matching files at its target.
pub(crate) async fn sweep_orphans(dir: &std::path::Path, db: &Db, max_age: std::time::Duration) {
    use std::time::SystemTime;

    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
        Err(e) => {
            warn!(error = %e, dir = %dir.display(), "orphan cleanup: cannot read directory");
            return;
        }
    };

    loop {
        let entry = match entries.next_entry().await {
            Ok(Some(entry)) => entry,
            Ok(None) => break,
            Err(e) => {
                warn!(error = %e, dir = %dir.display(), "orphan cleanup: cannot list directory");
                break;
            }
        };
        let filename = entry.file_name();
        let filename_str = filename.to_string_lossy();

        let uuid_str = match filename_str.split_once('_') {
            Some((prefix, _)) => prefix,
            None => continue,
        };
        match Uuid::parse_str(uuid_str) {
            Ok(id) if id.hyphenated().to_string() == uuid_str => {}
            _ => continue,
        }

        let modified = match entry.metadata().await.and_then(|m| m.modified()) {
            Ok(m) => m,
            Err(e) => {
                warn!(
                    error = %e,
                    file = %filename_str,
                    "orphan cleanup: cannot read file metadata"
                );
                continue;
            }
        };
        let age = SystemTime::now()
            .duration_since(modified)
            .unwrap_or_default();
        if age < max_age {
            continue;
        }

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
    fn detect_text_with_character_across_prefix_end() {
        // "€" is three bytes; place it across the 8192-byte prefix boundary.
        let mut bytes = vec![b'a'; 8191];
        bytes.extend_from_slice("€".as_bytes());
        bytes.extend_from_slice(b"tail");
        assert_eq!(detect_media_type(&bytes), "text/plain");
    }

    #[test]
    fn detect_truncated_character_at_end_is_binary() {
        // The same incomplete sequence is invalid when it ends the whole file.
        let mut bytes = b"abc".to_vec();
        bytes.extend_from_slice(&"€".as_bytes()[..2]);
        assert_eq!(detect_media_type(&bytes), "application/octet-stream");
    }

    #[test]
    fn detect_invalid_byte_inside_prefix_is_binary() {
        let mut bytes = vec![b'a'; 100];
        bytes.push(0xFF);
        bytes.extend_from_slice(&[b'a'; 9000]);
        assert_eq!(detect_media_type(&bytes), "application/octet-stream");
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
    use indexmap::IndexMap;
    use tower::ServiceExt;

    use crate::router::build_router;
    use crate::test_support::app_config::default_test_app_config;
    use crate::test_support::http::{
        body_string, multipart_body, multipart_body_with_fields, setup_authenticated_user,
    };
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
        let db = crate::test_support::init_db_memory();
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
                brenn_db::conversation::create_conversation(&conn, user_id, "test", false);
            let (msg_id, _seq) = brenn_db::conversation::append_message(
                &conn,
                conv_id,
                brenn_db::conversation::MessageDirection::Outgoing,
                "user",
                None,
                None,
                r#"{"type":"user","message":{"role":"user","content":"test"}}"#,
                Some(user_id),
                None,
                None,
            );
            brenn_db::conversation::insert_attachments(
                &conn,
                &[brenn_db::conversation::StoredAttachment {
                    upload_id: upload_id.to_string(),
                    message_id: msg_id,
                    filename: "note.txt".to_string(),
                    media_type: "text/plain".to_string(),
                    size: 15,
                    disk_filename: format!("{upload_id}_note.txt"),
                    secret: false,
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

    // -----------------------------------------------------------------------
    // Secret attachments
    // -----------------------------------------------------------------------

    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};

    use brenn_lib::config::{AppConfig, PathMapper, PathMapping};

    use crate::test_support::app_config::{single_app, test_app_config_under};
    use crate::test_support::files_under;

    fn secret_test_state(cfg: AppConfig) -> (AppState, Db) {
        let db = crate::test_support::init_db_memory();
        let state = test_state_with_apps(&db, single_app(cfg));
        (state, db)
    }

    async fn post_upload(
        state: &AppState,
        session_token: &str,
        filename: &str,
        content: &[u8],
        fields: &[(&str, &str)],
    ) -> Response {
        let (content_type, body) = multipart_body_with_fields(filename, content, fields);
        build_router(state.clone(), None, 0, 2576)
            .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))))
            .oneshot(
                Request::post("/app/test/upload")
                    .header("cookie", format!("brenn_session={session_token}"))
                    .header("content-type", content_type)
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    fn mode_of(path: &Path) -> u32 {
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    #[tokio::test]
    async fn upload_secret_lands_in_secrets_dir_0600() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_app_config_under(dir.path());
        let secrets_dir = cfg.secrets_dir();
        let working_dir = cfg.working_dir.clone();
        let (state, db) = secret_test_state(cfg);
        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        let response = post_upload(
            &state,
            &session_token,
            "token.json",
            b"{\"refresh\":\"r-1\"}",
            &[("secret", "1")],
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let resp: serde_json::Value =
            serde_json::from_str(&body_string(response.into_body()).await).unwrap();
        assert_eq!(resp["filename"], "token.json");
        assert_eq!(resp["media_type"], "text/plain");
        assert_eq!(resp["size"], 17);
        let upload_id: Uuid = resp["upload_id"].as_str().unwrap().parse().unwrap();

        let path = secrets_dir.join(format!("{upload_id}_token.json"));
        assert_eq!(std::fs::read(&path).unwrap(), b"{\"refresh\":\"r-1\"}");
        assert_eq!(mode_of(&path), 0o600);
        assert_eq!(mode_of(&secrets_dir), 0o700);
        assert_eq!(files_under(&secrets_dir), vec![path]);
        assert!(
            files_under(&working_dir).is_empty(),
            "nothing may land under the working directory"
        );

        let pending = state.pending_uploads.lock().await;
        assert!(pending[&upload_id].secret, "the pending entry is secret");
    }

    #[tokio::test]
    async fn upload_secret_tightens_loose_secrets_dir() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_app_config_under(dir.path());
        let secrets_dir = cfg.secrets_dir();
        std::fs::create_dir(&secrets_dir).unwrap();
        std::fs::set_permissions(&secrets_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        let (state, db) = secret_test_state(cfg);
        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        let response =
            post_upload(&state, &session_token, "k.txt", b"key", &[("secret", "1")]).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(mode_of(&secrets_dir), 0o700);
    }

    #[tokio::test]
    async fn upload_secret_refuses_symlinked_secrets_dir() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_app_config_under(dir.path());
        let elsewhere = cfg.working_dir.join("elsewhere");
        std::fs::create_dir(&elsewhere).unwrap();
        std::os::unix::fs::symlink(&elsewhere, cfg.secrets_dir()).unwrap();
        let (state, db) = secret_test_state(cfg);
        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        let response =
            post_upload(&state, &session_token, "k.txt", b"key", &[("secret", "1")]).await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(files_under(&elsewhere).is_empty());
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn upload_secret_with_target_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = test_app_config_under(dir.path());
        cfg.attachment_targets = vec![brenn_lib::config::AttachmentTarget {
            name: "import".to_string(),
            label: "Import".to_string(),
            accept: vec![".txt".to_string()],
            multi: false,
            handler: brenn_lib::config::AttachmentHandlerConfig::Command {
                program: "true".to_string(),
                args: vec![],
                file_roles: std::collections::HashMap::new(),
                timeout_secs: 5,
                cc_instructions: None,
            },
        }];
        let root = dir.path().to_path_buf();
        let (state, db) = secret_test_state(cfg);
        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        let response = post_upload(
            &state,
            &session_token,
            "k.txt",
            b"key",
            &[("target", "import"), ("secret", "1")],
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(logs_contain("security_event=true"));
        assert!(logs_contain("upload: secret combined with target"));
        assert!(files_under(&root).is_empty(), "nothing is written");
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn upload_secret_bad_value_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_app_config_under(dir.path());
        let root = dir.path().to_path_buf();
        let (state, db) = secret_test_state(cfg);
        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        let response = post_upload(
            &state,
            &session_token,
            "k.txt",
            b"key",
            &[("secret", "true")],
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(logs_contain("security_event=true"));
        assert!(logs_contain("upload: invalid secret value"));
        assert!(files_under(&root).is_empty(), "nothing is written");
    }

    /// Insert a user message in a new conversation carrying `atts`.
    async fn persist_attachments(db: &Db, atts: &[(Uuid, &str, bool)]) {
        let conn = db.lock().await;
        let user_id = conn
            .query_row("SELECT id FROM users LIMIT 1", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap();
        let conv_id = brenn_db::conversation::create_conversation(&conn, user_id, "test", false);
        let (msg_id, _seq) = brenn_db::conversation::append_message(
            &conn,
            conv_id,
            brenn_db::conversation::MessageDirection::Outgoing,
            "user",
            None,
            None,
            r#"{"type":"user","message":{"role":"user","content":"test"}}"#,
            Some(user_id),
            None,
            None,
        );
        let stored: Vec<_> = atts
            .iter()
            .map(
                |(id, name, secret)| brenn_db::conversation::StoredAttachment {
                    upload_id: id.to_string(),
                    message_id: msg_id,
                    filename: name.to_string(),
                    media_type: "text/plain".to_string(),
                    size: 3,
                    disk_filename: format!("{id}_{name}"),
                    secret: *secret,
                },
            )
            .collect();
        brenn_db::conversation::insert_attachments(&conn, &stored);
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn serve_attachment_refuses_secret() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_app_config_under(dir.path());
        let secrets_dir = cfg.secrets_dir();
        let (state, db) = secret_test_state(cfg);
        let (session_token, _csrf) = setup_authenticated_user(&db).await;

        let response =
            post_upload(&state, &session_token, "k.txt", b"key", &[("secret", "1")]).await;
        let resp: serde_json::Value =
            serde_json::from_str(&body_string(response.into_body()).await).unwrap();
        let upload_id: Uuid = resp["upload_id"].as_str().unwrap().parse().unwrap();
        persist_attachments(&db, &[(upload_id, "k.txt", true)]).await;

        let response = build_router(state, None, 0, 2576)
            .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))))
            .oneshot(
                Request::get(format!("/app/test/attachment/{upload_id}/k.txt"))
                    .header("cookie", format!("brenn_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(logs_contain("security_event=true"));
        assert!(logs_contain("attachment: secret requested"));
        let path = secrets_dir.join(format!("{upload_id}_k.txt"));
        assert_eq!(std::fs::read(&path).unwrap(), b"key", "file untouched");
    }

    /// Three files in `dir`, all older than a day: one referenced, one
    /// unreferenced Brenn-named, one with a name Brenn never writes. Only
    /// the unreferenced Brenn-named file is removed.
    async fn assert_sweep_keeps_referenced_and_foreign(dir: &Path, db: &Db) {
        std::fs::create_dir_all(dir).unwrap();
        let referenced = Uuid::new_v4();
        let orphan = Uuid::new_v4();
        let referenced_path = dir.join(format!("{referenced}_kept.txt"));
        let orphan_path = dir.join(format!("{orphan}_x"));
        let foreign_path = dir.join("foo_bar.json");
        let old = std::time::SystemTime::now() - std::time::Duration::from_secs(48 * 3600);
        for path in [&referenced_path, &orphan_path, &foreign_path] {
            std::fs::write(path, b"abc").unwrap();
            std::fs::File::options()
                .write(true)
                .open(path)
                .unwrap()
                .set_modified(old)
                .unwrap();
        }
        persist_attachments(db, &[(referenced, "kept.txt", false)]).await;

        sweep_orphans(dir, db, std::time::Duration::from_secs(24 * 3600)).await;

        assert!(referenced_path.exists(), "referenced file is never removed");
        assert!(!orphan_path.exists(), "unreferenced Brenn file is removed");
        assert!(foreign_path.exists(), "a name Brenn never wrote is skipped");
    }

    #[tokio::test]
    async fn orphan_cleanup_sweeps_unreferenced_secret_and_keeps_referenced() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_app_config_under(dir.path());
        let secrets_dir = cfg.secrets_dir();
        let attachments_dir = cfg.attachments_dir();
        let (_state, db) = secret_test_state(cfg);
        setup_authenticated_user(&db).await;

        assert_sweep_keeps_referenced_and_foreign(&secrets_dir, &db).await;
        assert_sweep_keeps_referenced_and_foreign(&attachments_dir, &db).await;
    }

    #[tokio::test]
    async fn orphan_cleanup_spares_young_and_non_canonical_names() {
        let dir = tempfile::tempdir().unwrap();
        let db = crate::test_support::init_db_memory();
        let young = dir.path().join(format!("{}_new.txt", Uuid::new_v4()));
        let simple = dir.path().join(format!("{}_x", Uuid::new_v4().simple()));
        std::fs::write(&young, b"a").unwrap();
        std::fs::write(&simple, b"a").unwrap();

        sweep_orphans(dir.path(), &db, std::time::Duration::from_secs(3600)).await;
        assert!(young.exists(), "a file younger than max_age stays");
        sweep_orphans(dir.path(), &db, std::time::Duration::ZERO).await;
        assert!(!young.exists());
        assert!(
            simple.exists(),
            "only canonical hyphenated UUIDs are Brenn's"
        );
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn orphan_cleanup_warns_on_unreadable_dir_but_not_missing_one() {
        let dir = tempfile::tempdir().unwrap();
        let db = crate::test_support::init_db_memory();

        sweep_orphans(&dir.path().join("absent"), &db, std::time::Duration::ZERO).await;
        assert!(
            !logs_contain("orphan cleanup"),
            "a missing dir is skipped silently"
        );

        let not_a_dir = dir.path().join("file");
        std::fs::write(&not_a_dir, b"a").unwrap();
        sweep_orphans(&not_a_dir, &db, std::time::Duration::ZERO).await;
        assert!(logs_contain("orphan cleanup: cannot read directory"));
    }

    #[tokio::test]
    async fn resolve_attachments_secret_checks_secrets_dir_and_builds_container_path() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("home");
        let mut cfg = default_test_app_config("test", "Test App");
        cfg.working_dir = home.join("repos").join("ws");
        cfg.state_dir = home.join(".config").join("brenn").join("test");
        cfg.path_mapper = PathMapper::container(vec![PathMapping {
            host_root: home.clone(),
            container_root: PathBuf::from("/home/agent"),
        }]);
        std::fs::create_dir_all(cfg.secrets_dir()).unwrap();

        let upload_id = Uuid::new_v4();
        let disk_filename = format!("{upload_id}_token.json");
        let pending: PendingUploads = Default::default();
        pending.lock().await.insert(
            upload_id,
            PendingUpload {
                app_slug: "test".to_string(),
                filename: "token.json".to_string(),
                disk_filename: disk_filename.clone(),
                media_type: "text/plain".to_string(),
                size: 2048,
                uploaded_at: Instant::now(),
                uploader_user_id: 7,
                secret: true,
            },
        );
        let refs = [brenn_ws_types::AttachmentRef {
            upload_id: upload_id.to_string(),
        }];

        let err = resolve_attachments(&refs, "test", 7, &cfg, &pending)
            .await
            .expect_err("the file is not in the secrets directory yet");
        assert!(err.contains("file missing from disk"), "got {err}");

        // Present under the working directory is not present for a secret.
        std::fs::create_dir_all(cfg.attachments_dir()).unwrap();
        std::fs::write(cfg.attachments_dir().join(&disk_filename), b"x").unwrap();
        resolve_attachments(&refs, "test", 7, &cfg, &pending)
            .await
            .expect_err("a secret resolves only from the secrets directory");

        std::fs::write(cfg.secrets_dir().join(&disk_filename), b"x").unwrap();
        let resolved = resolve_attachments(&refs, "test", 7, &cfg, &pending)
            .await
            .expect("resolves from the secrets directory");
        assert_eq!(resolved.len(), 1);
        let r = &resolved[0];
        assert!(r.secret);
        assert_eq!(
            r.cc_path,
            format!("/home/agent/.config/brenn/test/secrets/{disk_filename}")
        );
        assert!(r.to_meta().secret);
        let line = r.cc_notification();
        assert!(line.starts_with("[Attached secret: "), "got {line}");
        assert!(line.contains(&r.cc_path), "got {line}");
        assert!(line.contains("2.0 KB"), "got {line}");
        assert!(!line.contains("attachments/"), "got {line}");
        assert!(
            line.contains("do not read, print or display its contents"),
            "got {line}"
        );
        assert!(line.contains("move it out of this directory"), "got {line}");
        assert!(
            pending.lock().await.is_empty(),
            "resolved entry leaves pending"
        );
    }

    #[tokio::test]
    async fn resolve_attachments_plain_is_relative_to_working_dir() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = default_test_app_config("test", "Test App");
        // A trailing separator must not change the relative path.
        cfg.working_dir = PathBuf::from(format!("{}/", dir.path().display()));
        std::fs::create_dir_all(cfg.attachments_dir()).unwrap();

        let upload_id = Uuid::new_v4();
        let disk_filename = format!("{upload_id}_notes.txt");
        std::fs::write(cfg.attachments_dir().join(&disk_filename), b"x").unwrap();
        let pending: PendingUploads = Default::default();
        pending.lock().await.insert(
            upload_id,
            PendingUpload {
                app_slug: "test".to_string(),
                filename: "notes.txt".to_string(),
                disk_filename: disk_filename.clone(),
                media_type: "text/plain".to_string(),
                size: 2048,
                uploaded_at: Instant::now(),
                uploader_user_id: 7,
                secret: false,
            },
        );
        let refs = [brenn_ws_types::AttachmentRef {
            upload_id: upload_id.to_string(),
        }];

        let resolved = resolve_attachments(&refs, "test", 7, &cfg, &pending)
            .await
            .expect("resolves from the attachments directory");
        let r = &resolved[0];
        assert!(!r.secret);
        assert_eq!(r.cc_path, format!("attachments/{disk_filename}"));
        assert_eq!(
            r.cc_notification(),
            format!("[Attached file: attachments/{disk_filename} (text/plain, 2.0 KB)]")
        );
    }

    #[tokio::test]
    async fn write_new_file_refuses_planted_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target");
        std::fs::write(&target, b"original").unwrap();
        let tmp = dir.path().join(".tmp_x");
        std::os::unix::fs::symlink(&target, &tmp).unwrap();

        let err = write_new_file(&tmp, b"secret bytes", 0o600)
            .await
            .expect_err("an existing path is refused");
        assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(&target).unwrap(), b"original");
        assert!(
            std::fs::symlink_metadata(&tmp)
                .unwrap()
                .file_type()
                .is_symlink(),
            "the planted symlink is left as it was"
        );
    }
}
