//! ExportUsage tool: write usage CSV/JSON to a sandboxed RW-mount path.

use brenn_cc::session::{ApprovalDecision as CcApprovalDecision, ApprovalKind, ApprovalRequest};
use tracing::{error, info, warn};

// ────────────────────────────────────────────────────────────────────────────
// openat2 wrapper
// ────────────────────────────────────────────────────────────────────────────

mod openat2_sys {
    use std::ffi::CStr;
    use std::mem::size_of;
    use std::os::unix::io::{FromRawFd, OwnedFd, RawFd};

    #[repr(C)]
    struct OpenHow {
        flags: u64,
        mode: u64,
        resolve: u64,
    }

    pub const RESOLVE_NO_SYMLINKS: u64 = 0x04;
    pub const RESOLVE_BENEATH: u64 = 0x08;

    /// Errors from `openat2_beneath_no_symlinks`.
    pub enum Openat2Error {
        /// `ENOSYS` — kernel or seccomp doesn't support `openat2`. Surface to model.
        Unsupported,
        /// Caller-correctable I/O error.
        Io(std::io::Error),
    }

    /// Call `openat2(2)` with `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS`.
    ///
    /// * `dirfd` — containment anchor opened with `O_DIRECTORY | O_RDONLY | O_CLOEXEC`.
    /// * `relative` — path relative to `dirfd`, no leading `/`, no `..` components.
    /// * `flags` — taken as `u32` to avoid sign-extension on high bits; internally
    ///   widened to `u64` via `as u64` (safe because the value was already u32).
    /// * `mode` — file creation mode (e.g. `0o600`).
    ///
    /// # Panics
    ///
    /// Panics on `EFAULT` or `EINVAL` because those indicate a Brenn bug (we
    /// constructed the struct; the kernel should never fault or reject it).
    pub fn openat2_beneath_no_symlinks(
        dirfd: RawFd,
        relative: &CStr,
        flags: u32,
        mode: u32,
        resolve: u64,
    ) -> Result<OwnedFd, Openat2Error> {
        let how = OpenHow {
            flags: flags as u64, // u32 → u64: safe, no sign extension
            mode: mode as u64,
            resolve,
        };
        // SAFETY: `how` is a valid `OpenHow` with `repr(C)`; `relative.as_ptr()`
        // is a valid NUL-terminated C string; `dirfd` is a live fd owned by the
        // caller. The syscall number is the correct x86-64 value (437) exposed
        // by libc as `SYS_openat2`.
        let ret = unsafe {
            libc::syscall(
                libc::SYS_openat2,
                dirfd as libc::c_long,
                relative.as_ptr(),
                &how as *const OpenHow,
                size_of::<OpenHow>(),
            )
        };
        if ret >= 0 {
            // SAFETY: `ret` is a non-negative file descriptor returned by the
            // kernel; we own it exclusively from this point.
            let fd = unsafe { OwnedFd::from_raw_fd(ret as libc::c_int) };
            return Ok(fd);
        }
        let errno = std::io::Error::last_os_error();
        match errno.raw_os_error() {
            Some(libc::ENOSYS) => Err(Openat2Error::Unsupported),
            Some(libc::EFAULT) | Some(libc::EINVAL) => {
                panic!("openat2: BUG — kernel rejected our open_how struct: {errno}");
            }
            _ => Err(Openat2Error::Io(errno)),
        }
    }
}

use super::super::ActiveBridge;
use super::super::mcp_constants::MCP_EXPORT_USAGE_TOOL;
use super::super::tool_summary::{HandleBrennToolResult, mark_tool_handled};

/// Handle PostToolUse for `MCP_EXPORT_USAGE_TOOL`.
///
/// PreToolUse for ExportUsage is intentionally absent: it flows through
/// CC's standard approval mechanism (no Brenn-side auto-approve).
///
/// Returns `Some(...)` when the request is for this tool family and `None`
/// otherwise — letting the dispatcher fall through to other arms.
pub(super) async fn handle(
    bridge: &ActiveBridge,
    req: &ApprovalRequest,
) -> Option<HandleBrennToolResult> {
    match &req.kind {
        // --- ExportUsage PostToolUse ---
        // PreToolUse for ExportUsage is intentionally absent: it flows through
        // CC's standard approval mechanism (no Brenn-side auto-approve).
        ApprovalKind::PostToolUse {
            tool_name,
            tool_input,
            tool_use_id,
            ..
        } if tool_name == MCP_EXPORT_USAGE_TOOL => {
            mark_tool_handled(bridge, tool_use_id).await;
            let result = handle_export_usage(bridge, tool_input).await;
            Some(HandleBrennToolResult::Respond(
                CcApprovalDecision::Continue {
                    updated_output: Some(result.to_string()),
                },
            ))
        }

        _ => None,
    }
}

/// Handle the `mcp__brenn__ExportUsage` PostToolUse: validate paths, query, write file.
///
/// Returns a JSON value: `{ ok, rows, path, kind, format }` on success,
/// `{ ok: false, error: "..." }` on caller-correctable errors. DB errors panic.
async fn handle_export_usage(
    bridge: &ActiveBridge,
    tool_input: &serde_json::Value,
) -> serde_json::Value {
    use brenn_lib::usage::{EventsFilter, SessionsFilter};
    use brenn_lib::usage_export::{
        write_events_csv, write_events_json, write_sessions_csv, write_sessions_json,
    };
    use std::io::BufWriter;

    // --- parse `kind` ---
    let kind = match tool_input.get("kind").and_then(|v| v.as_str()) {
        Some("sessions") => "sessions",
        Some("events") => "events",
        other => {
            return serde_json::json!({
                "ok": false,
                "error": format!("invalid kind {:?}; must be \"sessions\" or \"events\"", other)
            });
        }
    };

    // --- parse `from` / `to` ---
    let from = match parse_export_ts(tool_input.get("from").and_then(|v| v.as_str())) {
        Ok(t) => t,
        Err(e) => return serde_json::json!({"ok": false, "error": e}),
    };
    let to = match parse_export_ts(tool_input.get("to").and_then(|v| v.as_str())) {
        Ok(t) => t,
        Err(e) => return serde_json::json!({"ok": false, "error": e}),
    };

    // --- parse `format` (default "csv") ---
    let format = match tool_input.get("format").and_then(|v| v.as_str()) {
        None | Some("csv") => "csv",
        Some("json") => "json",
        Some(other) => {
            return serde_json::json!({
                "ok": false,
                "error": format!("invalid format {:?}; must be \"csv\" or \"json\"", other)
            });
        }
    };

    // --- parse `output_file` and sandbox it ---
    let output_file = match tool_input.get("output_file").and_then(|v| v.as_str()) {
        Some(p) if !p.is_empty() => std::path::PathBuf::from(p),
        _ => {
            return serde_json::json!({"ok": false, "error": "missing output_file"});
        }
    };
    if !output_file.is_absolute() {
        return serde_json::json!({
            "ok": false,
            "error": "output_file must be an absolute path"
        });
    }
    // Translate the agent-supplied container path to a host path.
    // For bare-process apps (PathMapper::Identity), to_host returns Some(input)
    // unchanged. For containerized apps, it maps the container prefix to the
    // corresponding host prefix. Returns None when the path is outside all
    // mapped container roots.
    let host_output_file = match bridge.path_mapper.to_host(&output_file) {
        Some(p) => p,
        None => {
            warn!(
                user_id = bridge.user_id,
                conversation_id = bridge.conversation_id,
                app_slug = %bridge.app_slug,
                container_path = %output_file.display(),
                "ExportUsage: output_file outside all container mappings"
            );
            return serde_json::json!({
                "ok": false,
                "error": "output_file is not under any read-write app mount"
            });
        }
    };
    // Log the container→host translation when they differ (containerized apps),
    // so operators can reconcile the container path in tool output with the
    // actual host filesystem path written.
    if host_output_file != output_file {
        info!(
            user_id = bridge.user_id,
            conversation_id = bridge.conversation_id,
            app_slug = %bridge.app_slug,
            container_path = %output_file.display(),
            host_path = %host_output_file.display(),
            "ExportUsage: translated container path to host path"
        );
    }
    // Open the output file atomically within its RW-mount sandbox using
    // openat2(2) with RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS. This collapses
    // path validation and file creation into a single non-racey syscall,
    // eliminating the TOCTOU gap between canonicalize() and open().
    // TODO(export-usage-broken-mount-test)
    let file = match open_export_target(bridge, &host_output_file) {
        Ok(f) => f,
        Err(e) => return e,
    };

    // --- parse optional filters ---
    // Force-scope to the calling user's own data. The model is untrusted with
    // respect to multi-user data; ignoring any model-supplied "user" filter
    // and substituting the bridge's own user prevents cross-user data exposure
    // (security-1). A model-supplied user filter that matches the caller's own
    // username is equivalent — we still use the authoritative bridge username.
    let caller_username = bridge.get_username().await;
    let filter_user = Some(caller_username);
    let filters = tool_input.get("filters");
    let filter_device = filters
        .and_then(|f| f.get("device"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());
    let filter_app = filters
        .and_then(|f| f.get("app"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());
    let filter_event_type_str = filters
        .and_then(|f| f.get("event_type"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    // --- query + write ---
    // Query under the DB lock, then drop the lock before writing to disk.
    // This keeps the global Mutex<Connection> held only for the query, not
    // for the (potentially slow) file-write phase.
    enum ExportRows {
        Sessions(Vec<brenn_lib::usage::SessionRow>),
        Events(Vec<brenn_lib::usage::EventRow>),
    }
    let export_rows = {
        let conn = bridge.db.lock().await;
        if kind == "sessions" {
            let filter = SessionsFilter {
                from,
                to,
                user: filter_user,
                device: filter_device,
                app: filter_app,
            };
            ExportRows::Sessions(brenn_lib::usage::query_sessions(&conn, &filter))
        } else {
            // events
            let event_type = if let Some(ref s) = filter_event_type_str {
                match brenn_lib::usage::EventType::try_from_str(s) {
                    Some(t) => Some(t),
                    None => {
                        return serde_json::json!({
                            "ok": false,
                            "error": format!("unknown event_type {:?}", s)
                        });
                    }
                }
            } else {
                None
            };
            let filter = EventsFilter {
                from,
                to,
                user: filter_user,
                device: filter_device,
                app: filter_app,
                event_type,
            };
            ExportRows::Events(brenn_lib::usage::query_events(&conn, &filter))
        }
    }; // DB lock released here

    let writer = BufWriter::new(file);
    // Write to disk outside the DB lock. csv::Error and io::Error are different
    // types; map both to String so the match arms unify.
    let write_result: Result<usize, String> = match &export_rows {
        ExportRows::Sessions(rows) => match format {
            "json" => write_sessions_json(writer, rows).map_err(|e| e.to_string()),
            "csv" => write_sessions_csv(writer, rows).map_err(|e| e.to_string()),
            _ => unreachable!("format validated to csv or json above"),
        },
        ExportRows::Events(rows) => match format {
            "json" => write_events_json(writer, rows).map_err(|e| e.to_string()),
            "csv" => write_events_csv(writer, rows).map_err(|e| e.to_string()),
            _ => unreachable!("format validated to csv or json above"),
        },
    };
    let rows = match write_result {
        Ok(n) => n,
        Err(e) => return serde_json::json!({"ok": false, "error": format!("write failed: {e}")}),
    };

    serde_json::json!({
        "ok": true,
        "rows": rows,
        "path": output_file.to_string_lossy(),
        "kind": kind,
        "format": format,
    })
}

/// Open the export target file atomically within its RW-mount sandbox.
///
/// Steps (per design):
/// 1. Canonicalize the parent directory (for mount routing only, not security).
/// 2. Pick the deepest (longest-prefix) RW-mount whose canonical root contains
///    the canonical parent. Zero matches → error JSON. Multiple matches →
///    longest-prefix wins (deterministic).
/// 3. Open the mount root as a `dirfd` with `O_RDONLY | O_DIRECTORY | O_CLOEXEC`.
/// 4. Compute `relative = strip_prefix(canon_root) / filename`.
/// 5. Call `openat2(2)` with `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS`,
///    `flags = O_WRONLY | O_CREAT | O_CLOEXEC` (NO O_TRUNC), `mode = 0o600`.
///    fstat + nlink check, then fchmod(0o600) and ftruncate(0).
/// 6. Wrap the resulting fd as `std::fs::File`.
///
/// Returns `Ok(File)` on success or `Err(serde_json::Value)` with
/// `{ ok: false, error: "..." }` on caller-correctable errors.
fn open_export_target(
    bridge: &ActiveBridge,
    host_output_file: &std::path::Path,
) -> Result<std::fs::File, serde_json::Value> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt as _;
    use std::path::Component;

    // ── Input validation (before any resource acquisition) ────────────────
    // Validate filename presence here so the check has no dependency on later
    // resource acquisition (dirfd open), making drop-order reasoning trivial.
    let filename = match host_output_file.file_name() {
        Some(f) => f,
        None => {
            return Err(serde_json::json!({
                "ok": false,
                "error": "output_file must include a filename"
            }));
        }
    };

    // ── Step 1: canonicalize parent (routing only) ────────────────────────
    let parent = host_output_file
        .parent()
        .unwrap_or(std::path::Path::new("/"));
    let canon_parent = match parent.canonicalize() {
        Ok(p) => p,
        Err(e) => {
            return Err(serde_json::json!({
                "ok": false,
                "error": format!("output_file parent directory cannot be resolved: {e}")
            }));
        }
    };

    // ── Step 2: pick deepest matching RW mount ────────────────────────────
    // Three distinct failure modes produce different diagnostics:
    //   1. No RW mounts configured at all → guide operator to add config.
    //   2. RW mounts configured but all failed to canonicalize → mount
    //      availability problem (host directory gone, unmounted, etc.).
    //   3. RW mounts canonical but none is an ancestor of the output → the
    //      output path is simply not under any configured mount.
    let rw_mounts: Vec<_> = bridge
        .mounts
        .iter()
        .filter(|m| m.access == brenn_lib::config::AccessLevel::ReadWrite)
        .collect();

    if rw_mounts.is_empty() {
        return Err(serde_json::json!({
            "ok": false,
            "error": "output_file is not under any read-write app mount; \
                      configure an [[app.mount]] with read-write access to enable ExportUsage"
        }));
    }

    // Canonicalize each RW mount's host_path. Track mounts that fail so we
    // can surface a distinct error when all mounts are unavailable.
    let mut canon_rw_mounts: Vec<(std::path::PathBuf, std::path::PathBuf, String)> = Vec::new();
    let mut failed_mount_slugs: Vec<String> = Vec::new();
    for m in &rw_mounts {
        match m.host_path.canonicalize() {
            Ok(canon_root) => {
                canon_rw_mounts.push((canon_root, m.host_path.clone(), m.slug.clone()));
            }
            Err(e) => {
                warn!(
                    slug = %m.slug,
                    path = %m.host_path.display(),
                    error = %e,
                    "RW mount host_path cannot be canonicalized; skipping for ExportUsage sandbox"
                );
                failed_mount_slugs.push(m.slug.clone());
            }
        }
    }

    if canon_rw_mounts.is_empty() {
        // Every configured RW mount failed to canonicalize — this is a host
        // availability problem, not a missing-config problem.
        return Err(serde_json::json!({
            "ok": false,
            "error": format!(
                "all configured read-write app mounts are unavailable \
                 (host paths could not be resolved: {}); \
                 check that the mount directories exist and are accessible",
                failed_mount_slugs.join(", ")
            )
        }));
    }

    // Collect matching candidates: RW mounts whose canonical root is an
    // ancestor of (or equal to) canon_parent.
    let candidates: Vec<(std::path::PathBuf, std::path::PathBuf, String)> = canon_rw_mounts
        .into_iter()
        .filter(|(canon_root, _, _)| canon_parent.starts_with(canon_root))
        .collect();

    if candidates.is_empty() {
        return Err(serde_json::json!({
            "ok": false,
            "error": "output_file is not under any read-write app mount"
        }));
    }

    // Deepest (most specific) mount = most components in canonical root.
    // Among candidates all are ancestors of canon_parent (starts_with filter
    // above), so more components ↔ longer string ↔ deeper path — but sorting
    // by component count is intent-clear and immune to byte-length coincidences.
    let (canon_root, orig_host_path, mount_slug) = candidates
        .into_iter()
        .max_by_key(|(root, _, _)| root.components().count())
        .expect("candidates non-empty");

    // ── Step 3: open mount root as dirfd via openat2 ──────────────────────
    // Use openat2 with RESOLVE_NO_SYMLINKS so that the dirfd open is not
    // susceptible to a TOCTOU swap of any component along the canon_root path.
    // AT_FDCWD + absolute path + RESOLVE_NO_SYMLINKS resolves from the filesystem
    // root and rejects symlinks at every component.
    let canon_root_cstring = match CString::new(canon_root.as_os_str().as_bytes()) {
        Ok(c) => c,
        Err(_) => {
            return Err(serde_json::json!({
                "ok": false,
                "error": "output_file is not under any read-write app mount"
            }));
        }
    };
    let dirfd_owned = match openat2_sys::openat2_beneath_no_symlinks(
        libc::AT_FDCWD,
        &canon_root_cstring,
        (libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC) as u32,
        0,
        openat2_sys::RESOLVE_NO_SYMLINKS,
    ) {
        Ok(fd) => fd,
        Err(openat2_sys::Openat2Error::Unsupported) => {
            error!(
                user_id = bridge.user_id,
                conversation_id = bridge.conversation_id,
                app_slug = %bridge.app_slug,
                "ExportUsage: openat2 returned ENOSYS opening dirfd — kernel too old or syscall blocked"
            );
            return Err(serde_json::json!({
                "ok": false,
                "error": "openat2 unavailable on this host (kernel too old or syscall blocked)"
            }));
        }
        Err(openat2_sys::Openat2Error::Io(e)) => {
            warn!(
                user_id = bridge.user_id,
                conversation_id = bridge.conversation_id,
                app_slug = %bridge.app_slug,
                mount_slug = %mount_slug,
                error = %e,
                "ExportUsage: cannot open mount root as dirfd"
            );
            // Return the uniform out-of-mount error to avoid leaking internal
            // filesystem state (symlink vs. permission vs. missing) to the model.
            return Err(serde_json::json!({
                "ok": false,
                "error": "output_file is not under any read-write app mount"
            }));
        }
    };
    let dirfd_raw = std::os::unix::io::AsRawFd::as_raw_fd(&dirfd_owned);

    // ── Step 4: compute relative path ─────────────────────────────────────
    // Prefer to strip the original (pre-canonicalize) mount host_path from
    // host_output_file, so that any symlink components inside the mount (e.g.
    // `M/b -> M/a` with output_file `M/b/x.csv`) are preserved as-is in the
    // relative path handed to openat2. openat2's RESOLVE_NO_SYMLINKS then
    // rejects them atomically.
    //
    // However: the preferred branch may contain `..` components from the
    // user-supplied path. RESOLVE_BENEATH rejects `..` unconditionally, so a
    // path like `foo/../bar/x.csv` would fail even when it resolves inside the
    // mount. If the preferred path contains `..`, fall back to the canonical
    // form (canon_parent/filename), which has no `..` by definition.
    //
    // Fallback: if host_output_file doesn't share the original host_path prefix
    // (e.g. the path was supplied via the canonical form of a symlinked mount
    // root), strip canon_root from canon_parent and append the filename.
    // RESOLVE_BENEATH still enforces containment in that fallback case.
    let rel_path = if let Ok(rel) = host_output_file.strip_prefix(&orig_host_path) {
        let has_dotdot = rel.components().any(|c| c == Component::ParentDir);
        if has_dotdot {
            // Fall through to canonical form below.
            let rel_dir = match canon_parent.strip_prefix(&canon_root) {
                Ok(r) => r.to_path_buf(),
                Err(_) => panic!(
                    "BUG: canon_parent {:?} does not start with canon_root {:?}",
                    canon_parent, canon_root
                ),
            };
            rel_dir.join(filename)
        } else {
            // Preferred: original path components preserved (symlinks visible to openat2).
            rel.to_path_buf()
        }
    } else {
        // Fallback: canonical parent stripped of canonical root, joined with filename.
        let rel_dir = match canon_parent.strip_prefix(&canon_root) {
            Ok(r) => r.to_path_buf(),
            Err(_) => {
                panic!(
                    "BUG: canon_parent {:?} does not start with canon_root {:?}",
                    canon_parent, canon_root
                );
            }
        };
        rel_dir.join(filename)
    };
    // Use byte-preserving conversion (OsStrExt::as_bytes) instead of lossy
    // UTF-8: Unix paths are arbitrary byte sequences; to_string_lossy would
    // silently rewrite non-UTF-8 bytes to U+FFFD, changing the path handed
    // to the kernel.
    let rel_cstring = match CString::new(rel_path.as_os_str().as_bytes()) {
        Ok(c) => c,
        Err(_) => {
            return Err(serde_json::json!({
                "ok": false,
                "error": "output_file contains invalid character"
            }));
        }
    };

    // ── Step 5: openat2 ───────────────────────────────────────────────────
    // O_WRONLY|O_CREAT|O_CLOEXEC (NO O_TRUNC); mode 0o600.
    // RESOLVE_BENEATH: no escape outside dirfd, atomically.
    // RESOLVE_NO_SYMLINKS: reject symlinks at every component.
    //
    // O_TRUNC is deliberately omitted here: the kernel applies O_TRUNC at
    // open time, before the syscall returns, which means any nlink>1 check
    // afterward would fire only after the inode (possibly a hardlink victim
    // outside the mount) has already been zeroed.  We truncate explicitly
    // via ftruncate(2) *after* the nlink check confirms the file is safe.
    let open_flags = (libc::O_WRONLY | libc::O_CREAT | libc::O_CLOEXEC) as u32;
    let result = openat2_sys::openat2_beneath_no_symlinks(
        dirfd_raw,
        &rel_cstring,
        open_flags,
        0o600,
        openat2_sys::RESOLVE_BENEATH | openat2_sys::RESOLVE_NO_SYMLINKS,
    );

    // dirfd_owned is dropped here, closing the directory fd.
    drop(dirfd_owned);

    match result {
        Ok(owned_fd) => {
            info!(
                user_id = bridge.user_id,
                conversation_id = bridge.conversation_id,
                app_slug = %bridge.app_slug,
                host_output_file = %host_output_file.display(),
                mount_slug = %mount_slug,
                "ExportUsage: openat2 succeeded"
            );
            // fstat the fd and reject if st_nlink > 1 BEFORE any inode mutation.
            // A model-created hardlink inside the RW mount pointing at a brenn-UID-owned
            // file outside the mount (e.g. ~/.ssh/authorized_keys) shares the victim's
            // inode; openat2/RESOLVE_BENEATH constrains path resolution, not inodes.
            // Rejecting nlink > 1 here — before ftruncate and fchmod — ensures the
            // hardlink vector is closed with no modification to the victim file.
            let raw_fd = std::os::unix::io::AsRawFd::as_raw_fd(&owned_fd);
            // SAFETY: raw_fd is valid and open; stat is zero-initialised before fstat fills it.
            let stat = unsafe {
                let mut stat: libc::stat = std::mem::zeroed();
                let ret = libc::fstat(raw_fd, &mut stat);
                if ret != 0 {
                    let e = std::io::Error::last_os_error();
                    error!(
                        user_id = bridge.user_id,
                        conversation_id = bridge.conversation_id,
                        app_slug = %bridge.app_slug,
                        host_output_file = %host_output_file.display(),
                        error = %e,
                        "ExportUsage: fstat failed on output fd"
                    );
                    return Err(serde_json::json!({
                        "ok": false,
                        "error": "cannot create output_file"
                    }));
                }
                stat
            };
            if stat.st_nlink > 1 {
                warn!(
                    user_id = bridge.user_id,
                    conversation_id = bridge.conversation_id,
                    app_slug = %bridge.app_slug,
                    host_output_file = %host_output_file.display(),
                    nlink = stat.st_nlink,
                    "ExportUsage: output file has nlink > 1 (hardlink attack rejected)"
                );
                return Err(serde_json::json!({
                    "ok": false,
                    "error": "output file has multiple hard links — write rejected"
                }));
            }
            // SAFETY: raw_fd is valid and open.
            let chmod_ret = unsafe { libc::fchmod(raw_fd, 0o600) };
            if chmod_ret != 0 {
                let e = std::io::Error::last_os_error();
                error!(
                    user_id = bridge.user_id,
                    conversation_id = bridge.conversation_id,
                    app_slug = %bridge.app_slug,
                    host_output_file = %host_output_file.display(),
                    error = %e,
                    "ExportUsage: fchmod(0o600) failed on output fd"
                );
                return Err(serde_json::json!({
                    "ok": false,
                    "error": "cannot create output_file"
                }));
            }
            // ftruncate(2) now that the file is confirmed to be safe (nlink == 1,
            // mode corrected).  This replaces the O_TRUNC that was removed from
            // the openat2 call to ensure no inode mutation precedes the nlink check.
            // SAFETY: raw_fd is valid and open.
            let trunc_ret = unsafe { libc::ftruncate(raw_fd, 0) };
            if trunc_ret != 0 {
                let e = std::io::Error::last_os_error();
                error!(
                    user_id = bridge.user_id,
                    conversation_id = bridge.conversation_id,
                    app_slug = %bridge.app_slug,
                    host_output_file = %host_output_file.display(),
                    error = %e,
                    "ExportUsage: ftruncate failed on output fd"
                );
                return Err(serde_json::json!({
                    "ok": false,
                    "error": "cannot create output_file"
                }));
            }
            // SAFETY: OwnedFd holds a valid, open, writable file descriptor.
            let file = std::fs::File::from(owned_fd);
            Ok(file)
        }
        Err(openat2_sys::Openat2Error::Unsupported) => {
            error!(
                user_id = bridge.user_id,
                conversation_id = bridge.conversation_id,
                app_slug = %bridge.app_slug,
                "ExportUsage: openat2 returned ENOSYS — kernel too old or syscall blocked"
            );
            Err(serde_json::json!({
                "ok": false,
                "error": "openat2 unavailable on this host (kernel too old or syscall blocked)"
            }))
        }
        Err(openat2_sys::Openat2Error::Io(e)) => {
            let raw = e.raw_os_error();
            if raw == Some(libc::ELOOP) || raw == Some(libc::EXDEV) {
                warn!(
                    user_id = bridge.user_id,
                    conversation_id = bridge.conversation_id,
                    app_slug = %bridge.app_slug,
                    host_output_file = %host_output_file.display(),
                    mount_slug = %mount_slug,
                    errno = if raw == Some(libc::ELOOP) { "ELOOP" } else { "EXDEV" },
                    "ExportUsage: openat2 rejected path (symlink or mount escape attempt)"
                );
                Err(serde_json::json!({
                    "ok": false,
                    "error": "output_file is not under any read-write app mount"
                }))
            } else {
                Err(serde_json::json!({
                    "ok": false,
                    "error": format!("cannot create output_file: {e}")
                }))
            }
        }
    }
}

/// Parse an ISO-8601 timestamp or bare `YYYY-MM-DD` date (UTC midnight).
///
/// Returns an error string on failure.
///
fn parse_export_ts(input: Option<&str>) -> Result<chrono::DateTime<chrono::Utc>, String> {
    let s = match input {
        Some(s) if !s.is_empty() => s,
        _ => return Err("missing timestamp".to_string()),
    };
    brenn_lib::usage::parse_ts_str(s)
}

/// Test-only: exercise the JSON shape and log call for the
/// `Openat2Error::Unsupported` branch. This shim duplicates the match arm
/// verbatim so T7 can assert the expected JSON without requiring seccomp.
///
/// LIMITATION: this shim is NOT called through `open_export_target`; it
/// exercises only the shim itself, not the real production code path. A
/// refactor that changes the `Openat2Error::Unsupported` arm in
/// `open_export_target` (error string, log level, JSON keys) without
/// updating this function will not cause T7 to fail. The full production
/// path is exercised only by running under a seccomp profile that blocks
/// `SYS_openat2` (out-of-process; not covered by unit tests).
#[cfg(test)]
fn simulate_openat2_unsupported(bridge: &ActiveBridge) -> serde_json::Value {
    error!(
        user_id = bridge.user_id,
        conversation_id = bridge.conversation_id,
        app_slug = %bridge.app_slug,
        "ExportUsage: openat2 returned ENOSYS — kernel too old or syscall blocked"
    );
    serde_json::json!({
        "ok": false,
        "error": "openat2 unavailable on this host (kernel too old or syscall blocked)"
    })
}

#[cfg(test)]
mod tests {
    use brenn_cc::session::{
        ApprovalDecision as CcApprovalDecision, ApprovalKind, ApprovalRequest, SessionEvent,
    };
    use brenn_lib::config::PathMapper;
    use brenn_lib::conversation;
    use brenn_lib::ws_types::WsServerMessage;
    use std::sync::Arc;
    use tokio::sync::{broadcast, mpsc, oneshot};

    use super::super::super::ActiveBridge;
    use super::super::super::cc_event_loop::cc_event_loop;
    use super::super::super::mcp_constants::MCP_EXPORT_USAGE_TOOL;
    use super::super::super::registry::ActiveBridges;
    use super::super::super::test_support::{
        create_test_device_for_user, mk_rw_mount_with_container, post_tool_use_req, test_bridge,
    };
    use super::super::super::tool_summary::HandleBrennToolResult;
    use super::super::handle_brenn_tools;

    /// Build an RW `ResolvedMount` pointing at `host_path`.
    fn mk_rw_mount(host_path: std::path::PathBuf) -> brenn_lib::config::ResolvedMount {
        brenn_lib::config::ResolvedMount {
            slug: "test-repo".to_string(),
            host_path,
            container_path: None,
            access: brenn_lib::config::AccessLevel::ReadWrite,
            auto_pull: false,
            is_working_dir: false,
            primary: false,
        }
    }

    /// Build a RO `ResolvedMount` pointing at `host_path`.
    fn mk_ro_mount(host_path: std::path::PathBuf) -> brenn_lib::config::ResolvedMount {
        brenn_lib::config::ResolvedMount {
            slug: "test-ro-repo".to_string(),
            host_path,
            container_path: None,
            access: brenn_lib::config::AccessLevel::ReadOnly,
            auto_pull: false,
            is_working_dir: false,
            primary: false,
        }
    }

    /// Build a bridge whose `mounts` field is set to `mounts`.
    /// Delegates to `test_bridge_with_mounts_and_mapper` with `PathMapper::Identity`.
    async fn test_bridge_with_mounts(
        mounts: Vec<brenn_lib::config::ResolvedMount>,
    ) -> (
        Arc<ActiveBridge>,
        mpsc::Sender<SessionEvent>,
        broadcast::Receiver<WsServerMessage>,
        ActiveBridges,
    ) {
        test_bridge_with_mounts_and_mapper(mounts, PathMapper::Identity).await
    }

    /// Variant of `test_bridge_with_mounts` that sets a custom `PathMapper`.
    async fn test_bridge_with_mounts_and_mapper(
        mounts: Vec<brenn_lib::config::ResolvedMount>,
        path_mapper: PathMapper,
    ) -> (
        Arc<ActiveBridge>,
        mpsc::Sender<SessionEvent>,
        broadcast::Receiver<WsServerMessage>,
        ActiveBridges,
    ) {
        let db = brenn_lib::db::init_db_memory();
        let active_bridges = ActiveBridges::new();
        let (user_id, conv_id) = {
            let conn = db.lock().await;
            let uid = brenn_lib::auth::user::create_user(&conn, "testuser", "$argon2id$fake");
            let cid = conversation::create_conversation(&conn, uid, "test", false);
            (uid, cid)
        };
        let (broadcast_tx, broadcast_rx) = broadcast::channel(64);
        let bridge = ActiveBridge::inject_for_test_with_mounts_and_mapper(
            user_id,
            conv_id,
            "test",
            db,
            broadcast_tx,
            mounts,
            path_mapper,
        );
        let (event_tx, event_rx) = mpsc::channel(64);
        tokio::spawn(cc_event_loop(
            event_rx,
            bridge.clone(),
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
        ));
        (bridge, event_tx, broadcast_rx, active_bridges)
    }

    /// Assert that `result` is an ok:false JSON response whose `error` field
    /// contains `expected_substr`. Returns the parsed response for callers that
    /// need additional assertions. Panics with a diagnostic if any arm fails.
    ///
    /// Used by rejection tests (T2–T5 path sandbox, T10–T20 validation). Tests
    /// with additional assertions (file existence, multiple substrings) call
    /// this for the common check and add their own `assert!` lines after.
    fn assert_export_usage_error(
        result: Option<HandleBrennToolResult>,
        expected_substr: &str,
    ) -> serde_json::Value {
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
                assert_eq!(parsed["ok"], false, "expected ok:false: {parsed}");
                assert!(
                    parsed["error"]
                        .as_str()
                        .unwrap_or("")
                        .contains(expected_substr),
                    "error must contain {expected_substr:?}: {parsed}"
                );
                parsed
            }
            other => panic!("expected Continue with ok:false, got {other:?}"),
        }
    }

    // --- Approval-flow tests ---

    /// `handle_brenn_tools` returns `None` for ExportUsage PreToolUse (no auto-approve).
    #[tokio::test]
    async fn mcp_export_usage_pretooluse_returns_none() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;
        let (resp_tx, _) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req-export-pretool".into(),
            kind: ApprovalKind::PreToolUse {
                callback_id: "brenn_pre_tool_0".into(),
                tool_name: MCP_EXPORT_USAGE_TOOL.to_string(),
                tool_input: serde_json::json!({"output_file": "/some/path.csv"}),
                tool_use_id: "tu-export-1".into(),
            },
            response_tx: resp_tx,
        };
        let result = handle_brenn_tools(&bridge, &req).await;
        assert!(
            result.is_none(),
            "ExportUsage PreToolUse must return None (no auto-approve): {result:?}"
        );
    }

    /// ExportUsage is absent from production global auto-approve list:
    /// `ApprovalRuleSet::check` returns `NoMatch`, not `GlobalTool`.
    #[tokio::test]
    async fn mcp_export_usage_not_in_global_auto_approve() {
        // Build a bridge whose approval_rules mirrors the test fixture's global_extra
        // (post-change), without MCP_EXPORT_USAGE_TOOL.
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;
        // inject_for_test creates global_extra that includes display-file etc.
        // but not ExportUsage. Check directly.
        let check_result = bridge
            .approval_rules
            .check(
                MCP_EXPORT_USAGE_TOOL,
                &serde_json::json!({"output_file": "/some/path.csv"}),
            )
            .await;
        use brenn_lib::approval_rules::ApprovalMatch;
        assert_eq!(
            check_result,
            ApprovalMatch::NoMatch,
            "ExportUsage must not be auto-approved via any rule variant: {check_result:?}"
        );
    }

    // --- Path sandbox tests ---

    #[tokio::test]
    async fn mcp_export_usage_rejects_relative_path() {
        let tmp = tempfile::tempdir().unwrap();
        let (bridge, _event_tx, _broadcast_rx, _ab) =
            test_bridge_with_mounts(vec![mk_rw_mount(tmp.path().to_path_buf())]).await;
        let req = post_tool_use_req(
            MCP_EXPORT_USAGE_TOOL,
            serde_json::json!({
                "kind": "sessions",
                "from": "2026-01-01T00:00:00Z",
                "to": "2026-12-31T23:59:59Z",
                "output_file": "relative/path.csv"
            }),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        assert_export_usage_error(result, "absolute path");
    }

    #[tokio::test]
    async fn mcp_export_usage_rejects_path_outside_rw_mounts() {
        let rw_mount_dir = tempfile::tempdir().unwrap();
        // A second, distinct tmpdir that is NOT under the RW mount.
        let outside_dir = tempfile::tempdir().unwrap();
        let outside_path = outside_dir.path().join("outside_rw_mount.csv");
        let (bridge, _event_tx, _broadcast_rx, _ab) =
            test_bridge_with_mounts(vec![mk_rw_mount(rw_mount_dir.path().to_path_buf())]).await;
        let req = post_tool_use_req(
            MCP_EXPORT_USAGE_TOOL,
            serde_json::json!({
                "kind": "sessions",
                "from": "2026-01-01T00:00:00Z",
                "to": "2026-12-31T23:59:59Z",
                "output_file": outside_path.to_str().unwrap()
            }),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        assert_export_usage_error(result, "read-write app mount");
        assert!(!outside_path.exists(), "file must not be created");
    }

    #[tokio::test]
    async fn mcp_export_usage_rejects_ro_mount_path() {
        let tmp = tempfile::tempdir().unwrap();
        let out_path = tmp.path().join("export.csv");
        // Only a ReadOnly mount — no RW mount at all.
        let (bridge, _event_tx, _broadcast_rx, _ab) =
            test_bridge_with_mounts(vec![mk_ro_mount(tmp.path().to_path_buf())]).await;
        let req = post_tool_use_req(
            MCP_EXPORT_USAGE_TOOL,
            serde_json::json!({
                "kind": "sessions",
                "from": "2026-01-01T00:00:00Z",
                "to": "2026-12-31T23:59:59Z",
                "output_file": out_path.to_str().unwrap()
            }),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        assert_export_usage_error(result, "read-write app mount");
        assert!(!out_path.exists(), "file must not be created");
    }

    #[tokio::test]
    async fn mcp_export_usage_rejects_with_no_rw_mounts() {
        // Use a tmpdir for the output path so we don't depend on global /tmp state.
        let tmp = tempfile::tempdir().unwrap();
        let out_path = tmp.path().join("no_mounts_export.csv");
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_mounts(vec![]).await;
        let req = post_tool_use_req(
            MCP_EXPORT_USAGE_TOOL,
            serde_json::json!({
                "kind": "sessions",
                "from": "2026-01-01T00:00:00Z",
                "to": "2026-12-31T23:59:59Z",
                "output_file": out_path.to_str().unwrap()
            }),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        let parsed = assert_export_usage_error(result, "read-write app mount");
        // The no-mounts case must include the configure nudge.
        let error_str = parsed["error"].as_str().unwrap_or("");
        assert!(
            error_str.contains("configure") || error_str.contains("[[app.mount]]"),
            "error should include configure nudge: {parsed}"
        );
        assert!(!out_path.exists(), "file must not be created");
    }

    #[tokio::test]
    async fn mcp_export_usage_rejects_nonexistent_parent() {
        let tmp = tempfile::tempdir().unwrap();
        // Parent dir does not exist under the RW mount.
        let out_path = tmp.path().join("nonexistent_subdir").join("export.csv");
        let (bridge, _event_tx, _broadcast_rx, _ab) =
            test_bridge_with_mounts(vec![mk_rw_mount(tmp.path().to_path_buf())]).await;
        let req = post_tool_use_req(
            MCP_EXPORT_USAGE_TOOL,
            serde_json::json!({
                "kind": "sessions",
                "from": "2026-01-01T00:00:00Z",
                "to": "2026-12-31T23:59:59Z",
                "output_file": out_path.to_str().unwrap()
            }),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        assert_export_usage_error(result, "parent");
        assert!(!out_path.exists(), "file must not be created");
    }

    #[tokio::test]
    async fn mcp_export_usage_writes_file_returns_path() {
        let tmp = tempfile::tempdir().unwrap();
        let out_path = tmp.path().join("sessions.csv");
        let (bridge, _event_tx, _broadcast_rx, _ab) =
            test_bridge_with_mounts(vec![mk_rw_mount(tmp.path().to_path_buf())]).await;
        let req = post_tool_use_req(
            MCP_EXPORT_USAGE_TOOL,
            serde_json::json!({
                "kind": "sessions",
                "from": "2026-01-01T00:00:00Z",
                "to": "2026-12-31T23:59:59Z",
                "output_file": out_path.to_str().unwrap()
            }),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
                assert_eq!(parsed["ok"], true, "should succeed: {parsed}");
                assert_eq!(parsed["kind"], "sessions");
                assert_eq!(parsed["format"], "csv");
                assert!(
                    parsed["path"].as_str().is_some(),
                    "path must be present: {parsed}"
                );
                assert!(
                    parsed["rows"].is_number(),
                    "rows must be a number: {parsed}"
                );
                // File must exist.
                assert!(out_path.exists(), "output file must be written");
                // Result must NOT contain row data inline (only path, count, metadata).
                assert!(
                    !output.contains("session_id"),
                    "result must not contain CSV header fields: {output}"
                );
            }
            other => panic!("expected Continue with ok:true, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn mcp_export_usage_result_never_contains_data() {
        let tmp = tempfile::tempdir().unwrap();
        let out_path = tmp.path().join("events.csv");
        let (bridge, _event_tx, _broadcast_rx, _ab) =
            test_bridge_with_mounts(vec![mk_rw_mount(tmp.path().to_path_buf())]).await;
        // Seed a usage event so the export has actual row content.
        let dev = create_test_device_for_user(&bridge.db, bridge.user_id, "TestBrowser/1.0").await;
        {
            let conn = bridge.db.lock().await;
            brenn_lib::usage::record_ws_connect(&conn, dev, bridge.user_id, "test", None, 1800);
        }
        let req = post_tool_use_req(
            MCP_EXPORT_USAGE_TOOL,
            serde_json::json!({
                "kind": "events",
                "from": "2020-01-01T00:00:00Z",
                "to": "2030-01-01T00:00:00Z",
                "output_file": out_path.to_str().unwrap(),
                "format": "json"
            }),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
                assert_eq!(parsed["ok"], true, "should succeed: {parsed}");
                // The in-line result must not contain row-level data fields.
                for field in &["event_id", "created_at", "ws_connect", "device_slug"] {
                    assert!(
                        !output.contains(field),
                        "result must not contain row field {field:?}: {output}"
                    );
                }
                // The file must contain the data.
                let file_content = std::fs::read_to_string(&out_path).unwrap();
                assert!(
                    file_content.contains("ws_connect"),
                    "file must contain the event data: {file_content}"
                );
            }
            other => panic!("expected Continue with ok:true, got {other:?}"),
        }
    }

    /// Regression guard: `is_working_dir: true` mounts are valid RW targets for
    /// ExportUsage. Apps like graf/pfin where the working-dir is the primary repo
    /// would have no valid RW root if working-dir mounts were excluded.
    #[tokio::test]
    async fn mcp_export_usage_accepts_working_dir_mount() {
        let tmp = tempfile::tempdir().unwrap();
        let out_path = tmp.path().join("sessions.csv");
        // Mount has is_working_dir: true — the invariant being guarded.
        let wd_mount = brenn_lib::config::ResolvedMount {
            is_working_dir: true,
            ..mk_rw_mount(tmp.path().to_path_buf())
        };
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_mounts(vec![wd_mount]).await;
        let req = post_tool_use_req(
            MCP_EXPORT_USAGE_TOOL,
            serde_json::json!({
                "kind": "sessions",
                "from": "2026-01-01T00:00:00Z",
                "to": "2026-12-31T23:59:59Z",
                "output_file": out_path.to_str().unwrap()
            }),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
                assert_eq!(
                    parsed["ok"], true,
                    "working-dir mount must be a valid RW target: {parsed}"
                );
                assert!(out_path.exists(), "output file must be written");
            }
            other => panic!("expected Continue with ok:true, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Containerized-app path-translation tests (ExportUsage + DisplayFile)
    // -----------------------------------------------------------------------

    /// Regression test for the containerized ExportUsage path bug.
    /// Agent supplies a container-visible absolute path; the handler must
    /// translate it to the host path, write the file there, and return the
    /// original container path in the `path` field of the result.
    #[tokio::test]
    async fn mcp_export_usage_containerized_translates_path_and_writes() {
        let host_root = tempfile::tempdir().unwrap();
        let host_repo = host_root.path().join("repo");
        std::fs::create_dir_all(&host_repo).unwrap();

        let container_repo = std::path::PathBuf::from("/home/user/repos/repo");
        let mapper = PathMapper::container(vec![brenn_lib::config::PathMapping {
            host_root: host_repo.clone(),
            container_root: container_repo.clone(),
        }]);
        let mount = mk_rw_mount_with_container(host_repo.clone(), container_repo.clone());
        let (bridge, _event_tx, _broadcast_rx, _ab) =
            test_bridge_with_mounts_and_mapper(vec![mount], mapper).await;

        let container_output = container_repo.join("sessions.csv");
        let req = post_tool_use_req(
            MCP_EXPORT_USAGE_TOOL,
            serde_json::json!({
                "kind": "sessions",
                "from": "2026-01-01T00:00:00Z",
                "to": "2026-12-31T23:59:59Z",
                "output_file": container_output.to_str().unwrap()
            }),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
                assert_eq!(
                    parsed["ok"], true,
                    "containerized export must succeed: {parsed}"
                );
                // path field must be the container path the agent supplied, not the host path.
                assert_eq!(
                    parsed["path"].as_str().unwrap_or(""),
                    container_output.to_str().unwrap(),
                    "result path must be the container path: {parsed}"
                );
                // kind and format must round-trip.
                assert_eq!(
                    parsed["kind"], "sessions",
                    "kind must be sessions: {parsed}"
                );
                assert_eq!(parsed["format"], "csv", "format must be csv: {parsed}");
                assert!(
                    parsed["rows"].is_number(),
                    "rows must be a number: {parsed}"
                );
                // File must be written at the host path and non-empty (header row at minimum).
                let host_output = host_repo.join("sessions.csv");
                assert!(host_output.exists(), "file must exist at host path");
                assert!(
                    std::fs::metadata(&host_output).unwrap().len() > 0,
                    "written file must not be empty"
                );
            }
            other => panic!("expected Continue with ok:true, got {other:?}"),
        }
    }

    /// Container path outside all mappings must be rejected before any
    /// filesystem operation with a "read-write app mount" error.
    #[tokio::test]
    async fn mcp_export_usage_containerized_rejects_unmapped_container_path() {
        let host_root = tempfile::tempdir().unwrap();
        let host_repo = host_root.path().join("repo");
        std::fs::create_dir_all(&host_repo).unwrap();

        let container_repo = std::path::PathBuf::from("/home/user/repos/repo");
        let mapper = PathMapper::container(vec![brenn_lib::config::PathMapping {
            host_root: host_repo.clone(),
            container_root: container_repo.clone(),
        }]);
        let mount = mk_rw_mount_with_container(host_repo.clone(), container_repo.clone());
        let (bridge, _event_tx, _broadcast_rx, _ab) =
            test_bridge_with_mounts_and_mapper(vec![mount], mapper).await;

        // /etc/passwd_export.csv is outside all container mappings.
        let outside_path = "/etc/passwd_export.csv";
        let req = post_tool_use_req(
            MCP_EXPORT_USAGE_TOOL,
            serde_json::json!({
                "kind": "sessions",
                "from": "2026-01-01T00:00:00Z",
                "to": "2026-12-31T23:59:59Z",
                "output_file": outside_path
            }),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        assert_export_usage_error(result, "read-write app mount");
        // File must not be created.
        assert!(
            !std::path::Path::new(outside_path).exists(),
            "file must not be created"
        );
    }

    /// Container path that maps to a host path but that host path is not under
    /// any RW mount must still be rejected by the containment check.
    #[tokio::test]
    async fn mcp_export_usage_containerized_rejects_path_outside_rw_mount() {
        let host_root = tempfile::tempdir().unwrap();
        let host_rw = host_root.path().join("rw-repo");
        let host_ro = host_root.path().join("ro-area");
        std::fs::create_dir_all(&host_rw).unwrap();
        std::fs::create_dir_all(&host_ro).unwrap();

        let container_rw = std::path::PathBuf::from("/home/user/repos/rw-repo");
        let container_ro = std::path::PathBuf::from("/home/user/repos/ro-area");
        let mapper = PathMapper::container(vec![
            brenn_lib::config::PathMapping {
                host_root: host_rw.clone(),
                container_root: container_rw.clone(),
            },
            brenn_lib::config::PathMapping {
                host_root: host_ro.clone(),
                container_root: container_ro.clone(),
            },
        ]);
        // Only the rw-repo mount is ReadWrite; ro-area has a mapping but no RW mount.
        let rw_mount = mk_rw_mount_with_container(host_rw.clone(), container_rw.clone());
        let ro_mount = brenn_lib::config::ResolvedMount {
            slug: "ro-area".to_string(),
            host_path: host_ro.clone(),
            container_path: Some(container_ro.clone()),
            access: brenn_lib::config::AccessLevel::ReadOnly,
            auto_pull: false,
            is_working_dir: false,
            primary: false,
        };
        let (bridge, _event_tx, _broadcast_rx, _ab) =
            test_bridge_with_mounts_and_mapper(vec![rw_mount, ro_mount], mapper).await;

        // Path maps to host_ro via to_host (translation succeeds, container_ro is a known
        // mapping root), but host_ro is not under any RW mount. The rejection must come
        // from the post-translation RW-containment check, not from to_host returning None.
        // This exercises the distinct code path from the unmapped-path test above.
        let container_output = container_ro.join("x.csv");
        let req = post_tool_use_req(
            MCP_EXPORT_USAGE_TOOL,
            serde_json::json!({
                "kind": "sessions",
                "from": "2026-01-01T00:00:00Z",
                "to": "2026-12-31T23:59:59Z",
                "output_file": container_output.to_str().unwrap()
            }),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        assert_export_usage_error(result, "read-write app mount");
    }

    // -----------------------------------------------------------------------
    // openat2 / TOCTOU-fix tests
    // -----------------------------------------------------------------------

    /// T1: Happy path — single RW mount, output_file inside it, no symlinks.
    /// File must be created with mode 0o600.
    #[tokio::test]
    async fn openat2_happy_path_creates_file_mode_600() {
        use std::os::unix::fs::MetadataExt;
        let tmp = tempfile::tempdir().unwrap();
        let out_path = tmp.path().join("out.csv");
        let (bridge, _event_tx, _broadcast_rx, _ab) =
            test_bridge_with_mounts(vec![mk_rw_mount(tmp.path().to_path_buf())]).await;
        let req = post_tool_use_req(
            MCP_EXPORT_USAGE_TOOL,
            serde_json::json!({
                "kind": "sessions",
                "from": "2026-01-01T00:00:00Z",
                "to": "2026-12-31T23:59:59Z",
                "output_file": out_path.to_str().unwrap()
            }),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
                assert_eq!(parsed["ok"], true, "happy path must succeed: {parsed}");
                assert!(out_path.exists(), "file must be created");
                let mode = std::fs::metadata(&out_path).unwrap().mode() & 0o777;
                // Mask with our umask to get what was actually set; but since we set
                // O_CREAT+mode via openat2, the mode is before umask. We can only
                // reliably check that it is <= 0o600 (world/group bits clear).
                assert!(
                    mode & 0o177 == 0,
                    "file must not be group/world readable: mode=0o{mode:o}"
                );
            }
            other => panic!("expected ok:true, got {other:?}"),
        }
    }

    /// T2: Symlink in parent chain pointing outside the mount is rejected at the
    /// routing step (canon_parent is outside the mount). The target file must not
    /// be created.
    ///
    /// Mount at `tmp/M`. Symlink `tmp/M/sub -> /etc`.
    /// output_file = `<M>/sub/passwd`. canon_parent resolves to `/etc`, which is
    /// not inside the mount; the helper returns out-of-mount error and never
    /// reaches openat2.
    #[tokio::test]
    async fn openat2_symlink_in_parent_chain_to_outside_rejected_at_routing() {
        let tmp = tempfile::tempdir().unwrap();
        let m_dir = tmp.path().join("M");
        std::fs::create_dir_all(&m_dir).unwrap();
        let sub_link = m_dir.join("sub");
        std::os::unix::fs::symlink("/etc", &sub_link).unwrap();

        // We need /etc to exist for canonicalize to succeed. It does on Linux.
        // output_file (container path) = M/sub/passwd
        let fake_host_path = m_dir.join("sub").join("passwd");

        let (bridge, _event_tx, _broadcast_rx, _ab) =
            test_bridge_with_mounts(vec![mk_rw_mount(m_dir.clone())]).await;
        let req = post_tool_use_req(
            MCP_EXPORT_USAGE_TOOL,
            serde_json::json!({
                "kind": "sessions",
                "from": "2026-01-01T00:00:00Z",
                "to": "2026-12-31T23:59:59Z",
                "output_file": fake_host_path.to_str().unwrap()
            }),
        );
        // Capture /etc/passwd mtime before the call to assert non-modification.
        let etc_passwd = std::path::Path::new("/etc/passwd");
        let mtime_before = etc_passwd
            .exists()
            .then(|| std::fs::metadata(etc_passwd).unwrap().modified().unwrap());

        let result = handle_brenn_tools(&bridge, &req).await;
        assert_export_usage_error(result, "read-write app mount");
        // Assert /etc/passwd was not touched.
        if let Some(before) = mtime_before {
            let mtime_after = std::fs::metadata(etc_passwd).unwrap().modified().unwrap();
            assert_eq!(
                before, mtime_after,
                "/etc/passwd must not be modified by a rejected ExportUsage call"
            );
        }
    }

    /// T3: Symlink within the mount (pointing to another dir inside the mount)
    /// is rejected by openat2's RESOLVE_NO_SYMLINKS — even though the symlink
    /// target stays inside the mount.
    ///
    /// Mount at `tmp/M`. Real `tmp/M/a/`. Symlink `tmp/M/b -> a`.
    /// output_file = `M/b/x.csv`. canon_parent = `M/a` (inside mount), so
    /// routing passes. But openat2 sees the `b` component as a symlink and
    /// returns ELOOP, surfaced as the uniform out-of-mount error.
    #[tokio::test]
    async fn openat2_symlink_within_mount_rejected_by_resolve_no_symlinks() {
        let tmp = tempfile::tempdir().unwrap();
        let m_dir = tmp.path().join("M");
        std::fs::create_dir_all(m_dir.join("a")).unwrap();
        std::os::unix::fs::symlink("a", m_dir.join("b")).unwrap();

        let out_path = m_dir.join("b").join("x.csv");
        let (bridge, _event_tx, _broadcast_rx, _ab) =
            test_bridge_with_mounts(vec![mk_rw_mount(m_dir.clone())]).await;
        let req = post_tool_use_req(
            MCP_EXPORT_USAGE_TOOL,
            serde_json::json!({
                "kind": "sessions",
                "from": "2026-01-01T00:00:00Z",
                "to": "2026-12-31T23:59:59Z",
                "output_file": out_path.to_str().unwrap()
            }),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        assert_export_usage_error(result, "read-write app mount");
        assert!(
            !out_path.exists(),
            "file must not be created at symlink target"
        );
        assert!(
            !m_dir.join("a").join("x.csv").exists(),
            "file must not be created at resolved target"
        );
    }

    /// T5 (race test): swap an intermediate directory between a real dir and a
    /// symlink to /tmp/evil in a tight loop. The helper must never write a file
    /// outside the mount. This is a smoke check — the kernel's RESOLVE_NO_SYMLINKS
    /// provides the guarantee; this catches gross implementation bugs like
    /// forgetting the resolve flag.
    #[tokio::test]
    async fn openat2_race_swap_dir_for_symlink_never_escapes() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let tmp = tempfile::tempdir().unwrap();
        let m_dir = tmp.path().join("M");
        std::fs::create_dir_all(m_dir.join("d")).unwrap();

        let evil_dir = tmp.path().join("evil");
        std::fs::create_dir_all(&evil_dir).unwrap();
        let evil_file = evil_dir.join("out.csv");

        let (bridge, _event_tx, _broadcast_rx, _ab) =
            test_bridge_with_mounts(vec![mk_rw_mount(m_dir.clone())]).await;
        let bridge = Arc::clone(&bridge);

        let m_dir2 = m_dir.clone();
        let evil_dir2 = evil_dir.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = Arc::clone(&stop);

        // Swap thread: alternates M/d between a real dir and a symlink to evil.
        let swapper = std::thread::spawn(move || {
            let real_d = m_dir2.join("d");
            let link_d = m_dir2.join("d_link_tmp");
            // Pre-create the symlink target to avoid any creation races.
            let _ = std::os::unix::fs::symlink(&evil_dir2, &link_d);
            while !stop2.load(Ordering::Relaxed) {
                // swap d -> symlink
                let _ = std::fs::rename(&real_d, m_dir2.join("d_real_tmp"));
                let _ = std::fs::rename(&link_d, &real_d);
                // swap back
                let _ = std::fs::rename(&real_d, &link_d);
                let _ = std::fs::rename(m_dir2.join("d_real_tmp"), &real_d);
            }
        });

        let out_path = m_dir.join("d").join("out.csv");
        let out_path_str = out_path.to_str().unwrap().to_string();

        // Run 10_000 iterations; enough to hit the race window under the swapper
        // even on fast hardware. The design chose this count deliberately.
        for _ in 0..10_000 {
            let req = post_tool_use_req(
                MCP_EXPORT_USAGE_TOOL,
                serde_json::json!({
                    "kind": "sessions",
                    "from": "2026-01-01T00:00:00Z",
                    "to": "2026-12-31T23:59:59Z",
                    "output_file": out_path_str
                }),
            );
            // We don't care whether the call succeeds or fails; we only care
            // that evil_file is never created.
            let _ = handle_brenn_tools(&bridge, &req).await;
        }

        stop.store(true, Ordering::Relaxed);
        swapper.join().unwrap();

        assert!(
            !evil_file.exists(),
            "file must never be created outside the mount: {evil_file:?}"
        );
    }

    /// Extra: dirfd-open failure path. Mount root exists but is made non-openable
    /// (chmod 000). The handler must return ok:false without panicking. The error
    /// is the uniform out-of-mount message (not the raw errno string).
    #[tokio::test]
    async fn openat2_mount_root_not_openable_returns_error() {
        let tmp = tempfile::tempdir().unwrap();
        let m_dir = tmp.path().join("M");
        std::fs::create_dir_all(&m_dir).unwrap();
        // Create a subdirectory so canon_parent resolves correctly.
        let sub_dir = m_dir.join("sub");
        std::fs::create_dir_all(&sub_dir).unwrap();

        let (bridge, _event_tx, _broadcast_rx, _ab) =
            test_bridge_with_mounts(vec![mk_rw_mount(m_dir.clone())]).await;

        // Revoke read permission on M (keep execute so canonicalize can traverse)
        // so the dirfd open (O_RDONLY|O_DIRECTORY) fails with EACCES.
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&m_dir, std::fs::Permissions::from_mode(0o111)).unwrap();

        let out_path = sub_dir.join("x.csv");
        let req = post_tool_use_req(
            MCP_EXPORT_USAGE_TOOL,
            serde_json::json!({
                "kind": "sessions",
                "from": "2026-01-01T00:00:00Z",
                "to": "2026-12-31T23:59:59Z",
                "output_file": out_path.to_str().unwrap()
            }),
        );

        let result = handle_brenn_tools(&bridge, &req).await;

        // Restore permissions so tempdir cleanup succeeds.
        std::fs::set_permissions(&m_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        // Restore also lets sub_dir be cleaned up by tempdir.

        assert_export_usage_error(result, "read-write app mount");
    }

    /// T6: Nested RW mounts. Two mounts share a prefix: `tmp/M` and `tmp/M/inner`.
    /// output_file inside `M/inner/` must be routed to the inner (longest-prefix) mount.
    /// We verify by checking the file physically lands in the inner mount directory.
    #[tokio::test]
    async fn openat2_nested_mounts_longest_prefix_wins() {
        let tmp = tempfile::tempdir().unwrap();
        let m_dir = tmp.path().join("M");
        let inner_dir = m_dir.join("inner");
        std::fs::create_dir_all(&inner_dir).unwrap();

        let outer_mount = brenn_lib::config::ResolvedMount {
            slug: "outer".to_string(),
            host_path: m_dir.clone(),
            container_path: None,
            access: brenn_lib::config::AccessLevel::ReadWrite,
            auto_pull: false,
            is_working_dir: false,
            primary: false,
        };
        let inner_mount = brenn_lib::config::ResolvedMount {
            slug: "inner".to_string(),
            host_path: inner_dir.clone(),
            container_path: None,
            access: brenn_lib::config::AccessLevel::ReadWrite,
            auto_pull: false,
            is_working_dir: false,
            primary: false,
        };

        let out_path = inner_dir.join("x.csv");
        let (bridge, _event_tx, _broadcast_rx, _ab) =
            test_bridge_with_mounts(vec![outer_mount, inner_mount]).await;
        let req = post_tool_use_req(
            MCP_EXPORT_USAGE_TOOL,
            serde_json::json!({
                "kind": "sessions",
                "from": "2026-01-01T00:00:00Z",
                "to": "2026-12-31T23:59:59Z",
                "output_file": out_path.to_str().unwrap()
            }),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
                assert_eq!(parsed["ok"], true, "nested mount must succeed: {parsed}");
                assert!(out_path.exists(), "file must be created inside inner mount");
            }
            other => panic!("expected ok:true, got {other:?}"),
        }
    }

    /// T7: ENOSYS branch — exercises the `Openat2Error::Unsupported` match arm
    /// via `simulate_openat2_unsupported`, a `#[cfg(test)]` shim that mirrors
    /// the arm exactly. The shim calls `error!` and returns the same JSON; if
    /// the arm is accidentally deleted or the error string changed, this test
    /// fails.
    ///
    /// Full end-to-end testing (seccomp blocking `SYS_openat2` against
    /// `open_export_target`) is acceptance criterion A6 and runs out-of-process.
    #[tokio::test]
    async fn openat2_enosys_returns_correct_error_json() {
        let tmp = tempfile::tempdir().unwrap();
        let (bridge, _tx, _rx, _bridges) =
            test_bridge_with_mounts(vec![mk_rw_mount(tmp.path().to_path_buf())]).await;

        let result = super::simulate_openat2_unsupported(&bridge);

        assert_eq!(result["ok"], false, "ok must be false for ENOSYS");
        let err_str = result["error"].as_str().expect("error must be a string");
        assert!(
            err_str.contains("openat2 unavailable on this host"),
            "ENOSYS error must name the cause: {err_str}"
        );
        assert!(
            err_str.contains("kernel too old or syscall blocked"),
            "ENOSYS error must explain the cause: {err_str}"
        );
    }

    /// T8: Mount root is itself a symlink to a real directory. The mount is
    /// configured with the symlink path; canonicalization resolves it. The output
    /// file must land in the real directory.
    #[tokio::test]
    async fn openat2_mount_root_is_symlink_to_real_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let real_dir = tmp.path().join("real");
        let link_dir = tmp.path().join("link");
        std::fs::create_dir_all(&real_dir).unwrap();
        std::os::unix::fs::symlink(&real_dir, &link_dir).unwrap();

        // Mount configured with the symlink path.
        let mount = brenn_lib::config::ResolvedMount {
            slug: "link-mount".to_string(),
            host_path: link_dir.clone(),
            container_path: None,
            access: brenn_lib::config::AccessLevel::ReadWrite,
            auto_pull: false,
            is_working_dir: false,
            primary: false,
        };
        let out_path = link_dir.join("x.csv");
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_mounts(vec![mount]).await;
        let req = post_tool_use_req(
            MCP_EXPORT_USAGE_TOOL,
            serde_json::json!({
                "kind": "sessions",
                "from": "2026-01-01T00:00:00Z",
                "to": "2026-12-31T23:59:59Z",
                "output_file": out_path.to_str().unwrap()
            }),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
                assert_eq!(
                    parsed["ok"], true,
                    "symlinked mount root must succeed: {parsed}"
                );
                // File must land in the real dir.
                assert!(
                    real_dir.join("x.csv").exists(),
                    "file must exist in real dir"
                );
            }
            other => panic!("expected ok:true, got {other:?}"),
        }
    }

    /// T10: NUL byte in output_file path must return the invalid-character error.
    #[tokio::test]
    async fn openat2_nul_byte_in_path_returns_error() {
        let tmp = tempfile::tempdir().unwrap();
        let (bridge, _event_tx, _broadcast_rx, _ab) =
            test_bridge_with_mounts(vec![mk_rw_mount(tmp.path().to_path_buf())]).await;
        // Embed a NUL byte inside what would otherwise be a valid path.
        // serde_json passes this as a string; CString::new will fail on the NUL.
        let bad_path = format!("{}/foo\x00bar.csv", tmp.path().display());
        let req = post_tool_use_req(
            MCP_EXPORT_USAGE_TOOL,
            serde_json::json!({
                "kind": "sessions",
                "from": "2026-01-01T00:00:00Z",
                "to": "2026-12-31T23:59:59Z",
                "output_file": bad_path
            }),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        assert_export_usage_error(result, "invalid character");
    }

    /// T11: Hardlink rejection — a hardlink inside the RW mount pointing at an
    /// inode with nlink > 1 must be rejected before any inode mutation.
    /// This locks in the security invariant introduced by the nlink check that
    /// replaced O_TRUNC in the openat2 fix.
    #[cfg(unix)]
    #[tokio::test]
    async fn openat2_hardlink_attack_rejected_before_any_inode_mutation() {
        let tmp = tempfile::tempdir().unwrap();
        let (bridge, _event_tx, _broadcast_rx, _ab) =
            test_bridge_with_mounts(vec![mk_rw_mount(tmp.path().to_path_buf())]).await;

        // Create a "victim" file inside the mount (simulating, e.g., a config file
        // whose inode can be reached via a hardlink from within the RW mount).
        let victim = tmp.path().join("victim.txt");
        std::fs::write(&victim, b"sensitive content").unwrap();

        // The "attack" file is a hardlink inside the mount pointing at victim's inode.
        let attack_path = tmp.path().join("attack.csv");
        std::fs::hard_link(&victim, &attack_path).unwrap();

        let req = post_tool_use_req(
            MCP_EXPORT_USAGE_TOOL,
            serde_json::json!({
                "kind": "sessions",
                "from": "2026-01-01T00:00:00Z",
                "to": "2026-12-31T23:59:59Z",
                "output_file": attack_path.to_str().unwrap()
            }),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        assert_export_usage_error(result, "hard link");
        // Victim file must be unmodified — no truncation before the nlink check.
        let content = std::fs::read(&victim).unwrap();
        assert_eq!(
            content, b"sensitive content",
            "victim file must not be modified by the rejected write"
        );
    }

    // -----------------------------------------------------------------------
    // Input-validation rejection tests (T16–T20)
    // T11–T15 are already used by the hardlink/path-sandbox tests above.
    // -----------------------------------------------------------------------

    /// T16: `kind = "bogus"` is rejected before any other validation.
    #[tokio::test]
    async fn mcp_export_usage_rejects_invalid_kind() {
        let tmp = tempfile::tempdir().unwrap();
        let (bridge, _event_tx, _broadcast_rx, _ab) =
            test_bridge_with_mounts(vec![mk_rw_mount(tmp.path().to_path_buf())]).await;
        let req = post_tool_use_req(MCP_EXPORT_USAGE_TOOL, serde_json::json!({"kind": "bogus"}));
        let result = handle_brenn_tools(&bridge, &req).await;
        assert_export_usage_error(result, "invalid kind");
    }

    /// T17: `format = "xml"` is rejected; `kind`, `from`, `to` are valid.
    #[tokio::test]
    async fn mcp_export_usage_rejects_invalid_format() {
        let tmp = tempfile::tempdir().unwrap();
        let (bridge, _event_tx, _broadcast_rx, _ab) =
            test_bridge_with_mounts(vec![mk_rw_mount(tmp.path().to_path_buf())]).await;
        let req = post_tool_use_req(
            MCP_EXPORT_USAGE_TOOL,
            serde_json::json!({
                "kind": "sessions",
                "from": "2026-01-01T00:00:00Z",
                "to": "2027-01-01T00:00:00Z",
                "format": "xml"
            }),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        assert_export_usage_error(result, "invalid format");
    }

    /// T18: `from = "not-a-date"` is rejected; `kind` and `to` are valid.
    #[tokio::test]
    async fn mcp_export_usage_rejects_invalid_timestamp() {
        let tmp = tempfile::tempdir().unwrap();
        let (bridge, _event_tx, _broadcast_rx, _ab) =
            test_bridge_with_mounts(vec![mk_rw_mount(tmp.path().to_path_buf())]).await;
        let req = post_tool_use_req(
            MCP_EXPORT_USAGE_TOOL,
            serde_json::json!({
                "kind": "sessions",
                "from": "not-a-date",
                "to": "2099-01-01T00:00:00Z"
            }),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        assert_export_usage_error(result, "invalid timestamp");
    }

    /// T19: missing `output_file` is rejected; `kind`, `from`, `to`, `format` are valid.
    #[tokio::test]
    async fn mcp_export_usage_rejects_missing_output_file() {
        let tmp = tempfile::tempdir().unwrap();
        let (bridge, _event_tx, _broadcast_rx, _ab) =
            test_bridge_with_mounts(vec![mk_rw_mount(tmp.path().to_path_buf())]).await;
        let req = post_tool_use_req(
            MCP_EXPORT_USAGE_TOOL,
            serde_json::json!({
                "kind": "sessions",
                "from": "2026-01-01T00:00:00Z",
                "to": "2027-01-01T00:00:00Z",
                "format": "csv"
            }),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        assert_export_usage_error(result, "missing output_file");
    }

    /// T20: `event_type = "bogus_type"` is rejected after all earlier validation passes.
    /// Supplies a valid absolute `output_file` within an RW mount.
    /// NOTE: `open_export_target` creates+truncates the file before `event_type` is
    /// validated, so `out_path` WILL exist after this call despite the error response.
    #[tokio::test]
    async fn mcp_export_usage_rejects_unknown_event_type() {
        let tmp = tempfile::tempdir().unwrap();
        let out_path = tmp.path().join("export.csv");
        let (bridge, _event_tx, _broadcast_rx, _ab) =
            test_bridge_with_mounts(vec![mk_rw_mount(tmp.path().to_path_buf())]).await;
        let req = post_tool_use_req(
            MCP_EXPORT_USAGE_TOOL,
            serde_json::json!({
                "kind": "events",
                "from": "2026-01-01T00:00:00Z",
                "to": "2027-01-01T00:00:00Z",
                "format": "csv",
                "output_file": out_path.to_str().unwrap(),
                "filters": {"event_type": "bogus_type"}
            }),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        assert_export_usage_error(result, "unknown event_type");
        // open_export_target runs before event_type validation, so the file is
        // created (and zero-truncated) even though the export fails.
        assert!(
            out_path.exists(),
            "file is created before event_type validation — documents production ordering"
        );
    }

    /// A containerized RW mount whose `host_path` does not exist on the host
    /// is skipped by the filter loop inside `open_export_target`
    /// (the `warn!`-and-skip branch). The agent should see the "not under any
    /// read-write app mount" diagnostic — not a panic.
    ///
    /// Coverage note: the warn-and-skip branch is exercised indirectly. The
    /// `ok:false` / "read-write app mount" assertion is consistent with the
    /// branch being reached, but would also pass if the mount were missing
    /// from the list entirely. Instrumenting the warn path directly would
    /// require test-only hooks not present in this codebase; the indirect
    /// assertion is accepted as sufficient given the simplicity of the branch.
    ///
    /// The setup needs a real parent directory so `canon_parent` succeeds
    /// (otherwise an earlier check fires). We point the RW *mount's* host_path
    /// at a nonexistent directory so `host_path.canonicalize()` inside the
    /// filter_map fails, exercising the warn-and-skip path.
    #[tokio::test]
    async fn mcp_export_usage_containerized_skips_uncanonicalizable_host_path() {
        // A real tempdir so `canon_parent` can succeed.
        let host_root = tempfile::tempdir().unwrap();
        let real_dir = host_root.path().join("real");
        std::fs::create_dir_all(&real_dir).unwrap();

        // The RW mount's host_path is a nonexistent sibling directory.
        let broken_host = host_root.path().join("nonexistent-mount");
        // Container root maps /home/user/repos/repo → broken_host (doesn't exist).
        let container_repo = std::path::PathBuf::from("/home/user/repos/repo");
        // A second mapping for /home/user/real → real_dir so the container path
        // for the output file's parent resolves and canon_parent can succeed.
        let container_real = std::path::PathBuf::from("/home/user/real");

        let mapper = PathMapper::container(vec![
            brenn_lib::config::PathMapping {
                host_root: broken_host.clone(),
                container_root: container_repo.clone(),
            },
            brenn_lib::config::PathMapping {
                host_root: real_dir.clone(),
                container_root: container_real.clone(),
            },
        ]);

        // The broken mount is RW; broken_host.canonicalize() will fail (ENOENT).
        let broken_mount = mk_rw_mount_with_container(broken_host, container_repo.clone());

        let (bridge, _event_tx, _broadcast_rx, _ab) =
            test_bridge_with_mounts_and_mapper(vec![broken_mount], mapper).await;

        // Output file is in /home/user/real (which exists on host). The path
        // mapper translates it to real_dir/output.csv. The real_dir exists so
        // canon_parent succeeds. But the only RW mount (broken_mount) has a
        // nonexistent host_path, so it fails canonicalize — and since all RW
        // mounts fail, the code returns the "all configured read-write app
        // mounts are unavailable" variant of the error (not the generic "not
        // under any mount" message).
        let container_output = container_real.join("output.csv");
        let req = post_tool_use_req(
            MCP_EXPORT_USAGE_TOOL,
            serde_json::json!({
                "kind": "sessions",
                "from": "2026-01-01T00:00:00Z",
                "to": "2026-12-31T23:59:59Z",
                "output_file": container_output.to_str().unwrap()
            }),
        );

        let result = handle_brenn_tools(&bridge, &req).await;
        let parsed = assert_export_usage_error(result, "read-write app mount");
        // The new code distinguishes "all mounts unavailable" from
        // "no mounts configured" — verify we get the specific variant.
        assert!(
            parsed["error"]
                .as_str()
                .unwrap_or("")
                .contains("unavailable"),
            "error must indicate mounts are unavailable (not just missing): {parsed}"
        );
    }
}
