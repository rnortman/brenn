//! Device tools: DeviceList, DeviceGet, DeviceAssignSlug. Plus the visibility-set resolver shared by these three.

use brenn_cc::session::{ApprovalDecision as CcApprovalDecision, ApprovalKind, ApprovalRequest};
use tracing::warn;

use super::super::ActiveBridge;
use super::super::mcp_constants::{
    MCP_DEVICE_ASSIGN_SLUG_TOOL, MCP_DEVICE_GET_TOOL, MCP_DEVICE_LIST_TOOL,
};
use super::super::tool_summary::{HandleBrennToolResult, mark_tool_handled};

enum UserScope {
    /// Username explicitly provided — scope all operations to this user.
    Explicit(i64),
    /// No username — use tool-specific default (bridge.user_id for mutations,
    /// full visibility set for queries).
    BridgeOwner,
}

/// # Preconditions
///
/// Must be called inside the `bridge.db.lock().await` block so the DB lookup and
/// downstream operations are serialized.
fn resolve_user_scope(
    bridge: &ActiveBridge,
    conn: &rusqlite::Connection,
    tool_input: &serde_json::Value,
) -> Result<UserScope, HandleBrennToolResult> {
    let username = tool_input
        .get("username")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());

    if bridge.shared.load(std::sync::atomic::Ordering::Relaxed) && username.is_none() {
        warn!(
            app = %bridge.app_slug,
            "device tool called on shared bridge without username: rejecting"
        );
        return Err(HandleBrennToolResult::Respond(
            CcApprovalDecision::Continue {
                updated_output: Some(r#"{"error":"not_allowed_on_shared_bridge"}"#.to_string()),
            },
        ));
    }

    match username {
        None => Ok(UserScope::BridgeOwner),
        Some(uname) => {
            // When allowed_users is non-empty, validate the requested username is in the list.
            if !bridge.allowed_users.is_empty() && !bridge.allowed_users.iter().any(|u| u == uname)
            {
                warn!(
                    app = %bridge.app_slug,
                    username = %uname,
                    "device tool called with username not in allowed_users: rejecting"
                );
                return Err(HandleBrennToolResult::Respond(
                    CcApprovalDecision::Continue {
                        updated_output: Some(
                            serde_json::json!({"error": "user_not_in_app"}).to_string(),
                        ),
                    },
                ));
            }
            match brenn_lib::auth::user::get_user_by_username(conn, uname) {
                Some(u) => Ok(UserScope::Explicit(u.id)),
                None => {
                    warn!(
                        app = %bridge.app_slug,
                        username = %uname,
                        "device tool called with unknown username: rejecting"
                    );
                    Err(HandleBrennToolResult::Respond(
                        CcApprovalDecision::Continue {
                            updated_output: Some(
                                serde_json::json!({"error": "user_not_found"}).to_string(),
                            ),
                        },
                    ))
                }
            }
        }
    }
}

/// Resolve the acting user ID for write-mutation tools (SetUserTimezone, DeviceAssignSlug).
///
/// Returns `Ok(user_id)` when resolved; `Err(HandleBrennToolResult)` on any error
/// (shared-bridge with no username, unknown username, user not in app).
///
/// Must be called inside the `bridge.db.lock().await` block.
pub(super) fn resolve_user_scope_for_write(
    bridge: &ActiveBridge,
    conn: &rusqlite::Connection,
    tool_input: &serde_json::Value,
) -> Result<i64, HandleBrennToolResult> {
    let scope = resolve_user_scope(bridge, conn, tool_input)?;
    Ok(match scope {
        UserScope::Explicit(uid) => uid,
        UserScope::BridgeOwner => bridge.user_id,
    })
}

/// Handle PreToolUse + PostToolUse arms for the Device tool family
/// (`MCP_DEVICE_LIST_TOOL`, `MCP_DEVICE_GET_TOOL`, `MCP_DEVICE_ASSIGN_SLUG_TOOL`).
///
/// Returns `Some(...)` when the request is for one of these tools and `None`
/// otherwise — letting the dispatcher fall through to other arms.
pub(super) async fn handle(
    bridge: &ActiveBridge,
    req: &ApprovalRequest,
) -> Option<HandleBrennToolResult> {
    match &req.kind {
        // --- DeviceList PreToolUse (auto-approve, read-only) ---
        ApprovalKind::PreToolUse { tool_name, .. } if tool_name == MCP_DEVICE_LIST_TOOL => {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Allow {
                updated_input: None,
            }))
        }

        // --- DeviceList PostToolUse ---
        ApprovalKind::PostToolUse {
            tool_name,
            tool_input,
            tool_use_id,
            ..
        } if tool_name == MCP_DEVICE_LIST_TOOL => {
            mark_tool_handled(bridge, tool_use_id).await;
            let limit = tool_input
                .get("limit")
                .and_then(|v| v.as_i64())
                .unwrap_or(10)
                .clamp(1, 100) as usize;

            let output = {
                let conn = bridge.db.lock().await;
                let scope = match resolve_user_scope(bridge, &conn, tool_input) {
                    Ok(s) => s,
                    Err(e) => return Some(e),
                };
                let visibility = match scope {
                    UserScope::Explicit(uid) => vec![uid],
                    UserScope::BridgeOwner => {
                        resolve_device_visibility_set(&conn, &bridge.allowed_users)
                    }
                };

                let limit_plus_one = limit + 1;
                let mut device_ids = brenn_lib::auth::device::list_device_ids_for_visibility_set(
                    &conn,
                    &visibility,
                    limit_plus_one,
                );
                let truncated = device_ids.len() > limit;
                if truncated {
                    device_ids.truncate(limit);
                }
                let records =
                    brenn_lib::auth::device::fetch_device_records(&conn, &device_ids, &visibility);
                if truncated {
                    serde_json::json!({"devices": records, "truncated": true})
                } else {
                    serde_json::json!({"devices": records})
                }
            };

            Some(HandleBrennToolResult::Respond(
                CcApprovalDecision::Continue {
                    updated_output: Some(output.to_string()),
                },
            ))
        }

        // --- DeviceGet PreToolUse (auto-approve, read-only) ---
        ApprovalKind::PreToolUse { tool_name, .. } if tool_name == MCP_DEVICE_GET_TOOL => {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Allow {
                updated_input: None,
            }))
        }

        // --- DeviceGet PostToolUse ---
        ApprovalKind::PostToolUse {
            tool_name,
            tool_input,
            tool_use_id,
            ..
        } if tool_name == MCP_DEVICE_GET_TOOL => {
            mark_tool_handled(bridge, tool_use_id).await;
            let device_arg = match tool_input.get("device").and_then(|v| v.as_str()) {
                Some(d) if !d.is_empty() => d.to_string(),
                _ => {
                    return Some(HandleBrennToolResult::Respond(
                        CcApprovalDecision::Continue {
                            updated_output: Some(r#"{"error":"missing_device_arg"}"#.to_string()),
                        },
                    ));
                }
            };
            let limit = tool_input
                .get("limit")
                .and_then(|v| v.as_i64())
                .unwrap_or(10)
                .clamp(1, 100) as usize;

            let output = {
                let conn = bridge.db.lock().await;
                let scope = match resolve_user_scope(bridge, &conn, tool_input) {
                    Ok(s) => s,
                    Err(e) => return Some(e),
                };
                let visibility = match scope {
                    UserScope::Explicit(uid) => vec![uid],
                    UserScope::BridgeOwner => {
                        resolve_device_visibility_set(&conn, &bridge.allowed_users)
                    }
                };
                let mut device_ids = brenn_lib::auth::device::resolve_device_ids_for_get(
                    &conn,
                    &device_arg,
                    &visibility,
                );
                if device_ids.is_empty() {
                    serde_json::json!({"error": "not_found"})
                } else {
                    let truncated = device_ids.len() > limit;
                    if truncated {
                        device_ids.truncate(limit);
                    }
                    let records = brenn_lib::auth::device::fetch_device_records(
                        &conn,
                        &device_ids,
                        &visibility,
                    );
                    if truncated {
                        serde_json::json!({"devices": records, "truncated": true})
                    } else {
                        serde_json::json!({"devices": records})
                    }
                }
            };

            Some(HandleBrennToolResult::Respond(
                CcApprovalDecision::Continue {
                    updated_output: Some(output.to_string()),
                },
            ))
        }

        // --- DeviceAssignSlug PreToolUse (auto-approve) ---
        ApprovalKind::PreToolUse { tool_name, .. } if tool_name == MCP_DEVICE_ASSIGN_SLUG_TOOL => {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Allow {
                updated_input: None,
            }))
        }

        // --- DeviceAssignSlug PostToolUse ---
        ApprovalKind::PostToolUse {
            tool_name,
            tool_input,
            tool_use_id,
            ..
        } if tool_name == MCP_DEVICE_ASSIGN_SLUG_TOOL => {
            // mark_tool_handled unconditionally — both success and refusal paths
            // send a response, so the tool is "handled" either way.
            mark_tool_handled(bridge, tool_use_id).await;
            let device_arg = match tool_input.get("device").and_then(|v| v.as_str()) {
                Some(d) if !d.is_empty() => d.to_string(),
                _ => {
                    return Some(HandleBrennToolResult::Respond(
                        CcApprovalDecision::Continue {
                            updated_output: Some(r#"{"error":"missing_device_arg"}"#.to_string()),
                        },
                    ));
                }
            };
            let slug = match tool_input.get("slug").and_then(|v| v.as_str()) {
                Some(s) => s.to_string(),
                None => {
                    return Some(HandleBrennToolResult::Respond(
                        CcApprovalDecision::Continue {
                            updated_output: Some(r#"{"error":"missing_slug_arg"}"#.to_string()),
                        },
                    ));
                }
            };

            let output = {
                let conn = bridge.db.lock().await;
                let effective_user_id =
                    match resolve_user_scope_for_write(bridge, &conn, tool_input) {
                        Ok(uid) => uid,
                        Err(e) => return Some(e),
                    };
                match brenn_lib::auth::device::resolve_device_for_assign(
                    &conn,
                    &device_arg,
                    effective_user_id,
                ) {
                    Err(err_json) => err_json,
                    Ok(device_id) => brenn_lib::auth::device::assign_device_slug(
                        &conn,
                        device_id,
                        &slug,
                        effective_user_id,
                    ),
                }
            };

            Some(HandleBrennToolResult::Respond(
                CcApprovalDecision::Continue {
                    updated_output: Some(output.to_string()),
                },
            ))
        }

        _ => None,
    }
}

/// Resolve the app-visibility user_id set for device tool queries.
///
/// When `allowed_users` is empty, returns all user ids in the `users` table.
/// Otherwise resolves each username to its id (skipping unknowns).
fn resolve_device_visibility_set(
    conn: &rusqlite::Connection,
    allowed_users: &[String],
) -> Vec<i64> {
    if allowed_users.is_empty() {
        // Open app — all users.
        let mut stmt = conn
            .prepare("SELECT id FROM users")
            .expect("prepare resolve_device_visibility_set all");
        stmt.query_map([], |row| row.get(0))
            .expect("query all user ids")
            .map(|r| r.expect("read user id"))
            .collect()
    } else {
        allowed_users
            .iter()
            .filter_map(|username| {
                brenn_lib::auth::user::get_user_by_username(conn, username).map(|u| u.id)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use brenn_cc::session::ApprovalDecision as CcApprovalDecision;
    use rusqlite::OptionalExtension;

    use super::super::super::mcp_constants::{
        MCP_DEVICE_ASSIGN_SLUG_TOOL, MCP_DEVICE_GET_TOOL, MCP_DEVICE_LIST_TOOL,
    };
    use super::super::super::test_support::{
        create_test_device_for_user, post_tool_use_req, test_bridge,
        test_bridge_with_allowed_users, test_shared_bridge,
    };
    use super::super::super::tool_summary::HandleBrennToolResult;
    use super::super::handle_brenn_tools;

    #[tokio::test]
    async fn device_assign_slug_happy_path_by_id() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;

        // Create a device for the bridge's user.
        let device_id =
            create_test_device_for_user(&bridge.db, bridge.user_id, "Mozilla/5.0 Chrome/125").await;

        let req = post_tool_use_req(
            MCP_DEVICE_ASSIGN_SLUG_TOOL,
            serde_json::json!({"device": device_id.to_string(), "slug": "laptop"}),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
                assert_eq!(parsed["ok"], true);
                assert_eq!(parsed["assigned_slug"], "laptop");
                assert_eq!(parsed["device_id"], device_id);
            }
            other => panic!("expected Continue with ok, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn device_assign_slug_happy_path_by_guessed_slug() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;
        let device_id =
            create_test_device_for_user(&bridge.db, bridge.user_id, "Mozilla/5.0 Chrome/125").await;

        // Get the guessed_slug.
        let guessed_slug = {
            let conn = bridge.db.lock().await;
            brenn_lib::auth::device::load_device(&conn, device_id).guessed_slug
        };

        let req = post_tool_use_req(
            MCP_DEVICE_ASSIGN_SLUG_TOOL,
            serde_json::json!({"device": guessed_slug, "slug": "desktop"}),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
                assert_eq!(parsed["ok"], true);
                assert_eq!(parsed["assigned_slug"], "desktop");
            }
            other => panic!("expected Continue with ok, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn device_assign_slug_rejects_assigned_slug_arg() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;
        let device_id =
            create_test_device_for_user(&bridge.db, bridge.user_id, "Mozilla/5.0 Chrome/125").await;

        // First assign a slug.
        {
            let conn = bridge.db.lock().await;
            brenn_lib::auth::device::assign_device_slug(&conn, device_id, "phone", bridge.user_id);
        }

        // Now try to use the assigned slug "phone" as the device arg — should reject.
        let req = post_tool_use_req(
            MCP_DEVICE_ASSIGN_SLUG_TOOL,
            serde_json::json!({"device": "phone", "slug": "mobile"}),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
                assert_eq!(
                    parsed["error"], "ambiguous_or_not_found",
                    "assigned slug as device arg must be rejected: {parsed}"
                );
            }
            other => panic!("expected Continue with error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn device_assign_slug_same_user_collision() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;
        let device_a =
            create_test_device_for_user(&bridge.db, bridge.user_id, "Mozilla/5.0 Chrome/125").await;
        let device_b =
            create_test_device_for_user(&bridge.db, bridge.user_id, "Mozilla/5.0 Firefox/126")
                .await;

        // Assign "my-device" to device_a.
        {
            let conn = bridge.db.lock().await;
            brenn_lib::auth::device::assign_device_slug(
                &conn,
                device_a,
                "my-device",
                bridge.user_id,
            );
        }

        // Try to assign same slug to device_b — should get slug_collision.
        let req = post_tool_use_req(
            MCP_DEVICE_ASSIGN_SLUG_TOOL,
            serde_json::json!({"device": device_b.to_string(), "slug": "my-device"}),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
                assert_eq!(
                    parsed["error"], "slug_collision",
                    "duplicate slug within same user must produce slug_collision: {parsed}"
                );
                assert_eq!(parsed["conflicting_device_id"], device_a);
            }
            other => panic!("expected Continue with slug_collision, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn device_assign_slug_clear_with_empty_string() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;
        let device_id =
            create_test_device_for_user(&bridge.db, bridge.user_id, "Mozilla/5.0 Chrome/125").await;

        // Assign then clear.
        {
            let conn = bridge.db.lock().await;
            brenn_lib::auth::device::assign_device_slug(&conn, device_id, "laptop", bridge.user_id);
        }

        let req = post_tool_use_req(
            MCP_DEVICE_ASSIGN_SLUG_TOOL,
            serde_json::json!({"device": device_id.to_string(), "slug": ""}),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
                assert_eq!(parsed["ok"], true);
                assert!(
                    parsed["assigned_slug"].is_null(),
                    "clearing slug must produce null assigned_slug: {parsed}"
                );
            }
            other => panic!("expected Continue with ok, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn device_assign_slug_idempotent() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;
        let device_id =
            create_test_device_for_user(&bridge.db, bridge.user_id, "Mozilla/5.0 Chrome/125").await;

        // Assign slug.
        {
            let conn = bridge.db.lock().await;
            brenn_lib::auth::device::assign_device_slug(&conn, device_id, "laptop", bridge.user_id);
        }

        // Assign same slug again — must return ok without unique-index error.
        let req = post_tool_use_req(
            MCP_DEVICE_ASSIGN_SLUG_TOOL,
            serde_json::json!({"device": device_id.to_string(), "slug": "laptop"}),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
                assert_eq!(
                    parsed["ok"], true,
                    "idempotent assign must succeed: {parsed}"
                );
            }
            other => panic!("expected Continue with ok, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn device_assign_slug_no_current_keyword() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;
        let _device_id =
            create_test_device_for_user(&bridge.db, bridge.user_id, "Mozilla/5.0 Chrome/125").await;

        // "current" is not a valid device identifier.
        let req = post_tool_use_req(
            MCP_DEVICE_ASSIGN_SLUG_TOOL,
            serde_json::json!({"device": "current", "slug": "mine"}),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
                assert_eq!(
                    parsed["error"], "ambiguous_or_not_found",
                    "\"current\" keyword must not resolve: {parsed}"
                );
            }
            other => panic!("expected Continue with error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn device_assign_slug_no_membership_for_bridge_user() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;

        // Create a second user + device NOT associated with the bridge user.
        let other_user_id = {
            let conn = bridge.db.lock().await;
            brenn_lib::auth::user::create_user(&conn, "other", "$argon2id$fake")
        };
        let other_device_id =
            create_test_device_for_user(&bridge.db, other_user_id, "Mozilla/5.0 Firefox/126").await;

        let other_guessed_slug = {
            let conn = bridge.db.lock().await;
            brenn_lib::auth::device::load_device(&conn, other_device_id).guessed_slug
        };

        // The bridge user has no membership on other_device — must get no_membership.
        let req = post_tool_use_req(
            MCP_DEVICE_ASSIGN_SLUG_TOOL,
            serde_json::json!({"device": other_guessed_slug, "slug": "stolen"}),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
                assert_eq!(
                    parsed["error"], "no_membership",
                    "bridge user must not be able to assign slug on a device they never used: {parsed}"
                );
            }
            other => panic!("expected Continue with no_membership, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn device_list_returns_app_visibility_set() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;

        // Create two devices for the bridge user (open app — all users visible).
        let _dev_a =
            create_test_device_for_user(&bridge.db, bridge.user_id, "Mozilla/5.0 Chrome/125").await;
        let _dev_b =
            create_test_device_for_user(&bridge.db, bridge.user_id, "Mozilla/5.0 Firefox/126")
                .await;

        let req = post_tool_use_req(MCP_DEVICE_LIST_TOOL, serde_json::json!({}));
        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
                let devices = parsed["devices"]
                    .as_array()
                    .expect("should have devices array");
                assert_eq!(devices.len(), 2, "should return 2 devices: {parsed}");
            }
            other => panic!("expected Continue with devices, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn device_get_returns_multiple_matches_across_app_users() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;

        // Two users each name their device "phone".
        let alice_id = bridge.user_id;
        let bob_id = {
            let conn = bridge.db.lock().await;
            brenn_lib::auth::user::create_user(&conn, "bob2", "$argon2id$fake")
        };

        let alice_device =
            create_test_device_for_user(&bridge.db, alice_id, "Mozilla/5.0 Chrome/125").await;
        let bob_device =
            create_test_device_for_user(&bridge.db, bob_id, "Mozilla/5.0 Firefox/126").await;

        {
            let conn = bridge.db.lock().await;
            brenn_lib::auth::device::assign_device_slug(&conn, alice_device, "phone", alice_id);
            brenn_lib::auth::device::assign_device_slug(&conn, bob_device, "phone", bob_id);
        }

        let req = post_tool_use_req(MCP_DEVICE_GET_TOOL, serde_json::json!({"device": "phone"}));
        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
                let devices = parsed["devices"]
                    .as_array()
                    .expect("should have devices array");
                assert_eq!(
                    devices.len(),
                    2,
                    "DeviceGet for shared assigned slug must return both matches: {parsed}"
                );
            }
            other => panic!("expected Continue with two devices, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn device_get_membership_outside_app_visibility_set_not_found() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;

        // test_bridge uses open app (allowed_users empty) — all users are visible.
        // We need to test with a restricted app. Directly test the helper function.
        let conn = bridge.db.lock().await;

        // Create a device for a user NOT in the restricted visibility set.
        let other_user_id = brenn_lib::auth::user::create_user(&conn, "outsider", "$argon2id$fake");
        let other_resolved = brenn_lib::auth::device::resolve_or_create_device(
            &conn,
            None,
            other_user_id,
            "Mozilla/5.0 Chrome/125",
        );
        let other_guessed_slug =
            brenn_lib::auth::device::load_device(&conn, other_resolved.id).guessed_slug;

        // Visibility set contains only bridge user — not the outsider.
        let visibility = vec![bridge.user_id];
        let matches = brenn_lib::auth::device::resolve_device_ids_for_get(
            &conn,
            &other_guessed_slug,
            &visibility,
        );
        assert!(
            matches.is_empty(),
            "device outside visibility set must not be found: {matches:?}"
        );
    }

    #[tokio::test]
    async fn device_list_truncates_with_flag() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;

        // Create 3 devices; request limit=2 → should return 2 records + truncated:true.
        for i in 0..3u8 {
            create_test_device_for_user(
                &bridge.db,
                bridge.user_id,
                &format!("Mozilla/5.0 Chrome/{i}"),
            )
            .await;
        }

        // limit=2 with 3 devices: LIMIT 3 query returns 3, len(3) > 2 → truncate to 2, set flag.
        let req = post_tool_use_req(MCP_DEVICE_LIST_TOOL, serde_json::json!({"limit": 2}));
        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
                let devices = parsed["devices"]
                    .as_array()
                    .expect("should have devices array");
                assert_eq!(
                    devices.len(),
                    2,
                    "DeviceList must return exactly limit=2 records: {parsed}"
                );
                assert_eq!(
                    parsed["truncated"], true,
                    "DeviceList must set truncated:true when results exceed limit: {parsed}"
                );
            }
            other => panic!("expected Continue with truncated devices, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn device_get_caps_at_ten_with_truncated_flag() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;

        // Create 3 users each assigning the slug "shared" to their device.
        // Use limit=2 → DeviceGet returns 2 records + truncated:true.
        for i in 0..3u8 {
            let uid = {
                let conn = bridge.db.lock().await;
                brenn_lib::auth::user::create_user(
                    &conn,
                    &format!("shared_user_{i}"),
                    "$argon2id$fake",
                )
            };
            let device_id =
                create_test_device_for_user(&bridge.db, uid, &format!("Mozilla/5.0 Safari/{i}"))
                    .await;
            let conn = bridge.db.lock().await;
            brenn_lib::auth::device::assign_device_slug(&conn, device_id, "shared", uid);
        }

        // limit=2 with 3 matches: DeviceGet returns 2 records + truncated:true.
        let req = post_tool_use_req(
            MCP_DEVICE_GET_TOOL,
            serde_json::json!({"device": "shared", "limit": 2}),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
                let devices = parsed["devices"]
                    .as_array()
                    .expect("should have devices array");
                assert_eq!(
                    devices.len(),
                    2,
                    "DeviceGet must return exactly limit=2 records: {parsed}"
                );
                assert_eq!(
                    parsed["truncated"], true,
                    "DeviceGet must set truncated:true when results exceed limit: {parsed}"
                );
            }
            other => panic!("expected Continue with truncated devices, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn device_assign_slug_refused_on_shared_bridge() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_shared_bridge().await;
        let device_id =
            create_test_device_for_user(&bridge.db, bridge.user_id, "Mozilla/5.0 Chrome/125").await;
        // No username on shared bridge → refused.
        let req = post_tool_use_req(
            MCP_DEVICE_ASSIGN_SLUG_TOOL,
            serde_json::json!({"device": device_id.to_string(), "slug": "laptop"}),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).expect("valid JSON");
                assert_eq!(
                    parsed["error"], "not_allowed_on_shared_bridge",
                    "DeviceAssignSlug must be refused on shared bridge without username: {parsed}"
                );
            }
            other => panic!("expected Continue with error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn device_list_refused_on_shared_bridge() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_shared_bridge().await;
        // No username on shared bridge → refused.
        let req = post_tool_use_req(MCP_DEVICE_LIST_TOOL, serde_json::json!({}));
        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).expect("valid JSON");
                assert_eq!(
                    parsed["error"], "not_allowed_on_shared_bridge",
                    "DeviceList must be refused on shared bridge without username: {parsed}"
                );
            }
            other => panic!("expected Continue with error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn device_get_refused_on_shared_bridge() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_shared_bridge().await;
        let device_id =
            create_test_device_for_user(&bridge.db, bridge.user_id, "Mozilla/5.0 Chrome/125").await;
        // No username on shared bridge → refused.
        let req = post_tool_use_req(
            MCP_DEVICE_GET_TOOL,
            serde_json::json!({"device": device_id.to_string()}),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).expect("valid JSON");
                assert_eq!(
                    parsed["error"], "not_allowed_on_shared_bridge",
                    "DeviceGet must be refused on shared bridge without username: {parsed}"
                );
            }
            other => panic!("expected Continue with error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn device_assign_slug_shared_bridge_with_username() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_shared_bridge().await;

        // Create a device for the bridge's user; add an explicit user "alice" as the target.
        let alice_id = {
            let conn = bridge.db.lock().await;
            brenn_lib::auth::user::create_user(&conn, "alice", "$argon2id$fake")
        };
        let device_id =
            create_test_device_for_user(&bridge.db, alice_id, "Mozilla/5.0 Chrome/125").await;

        // Provide username on shared bridge → should succeed, using alice's namespace.
        let req = post_tool_use_req(
            MCP_DEVICE_ASSIGN_SLUG_TOOL,
            serde_json::json!({"device": device_id.to_string(), "slug": "alice-laptop", "username": "alice"}),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).expect("valid JSON");
                assert_eq!(
                    parsed["ok"], true,
                    "DeviceAssignSlug with username on shared bridge must succeed: {parsed}"
                );
                assert_eq!(parsed["assigned_slug"], "alice-laptop");
            }
            other => panic!("expected Continue with ok, got {other:?}"),
        }

        // Verify slug is stored under alice_id, not bridge.user_id.
        let conn = bridge.db.lock().await;
        let slug_owner: Option<i64> = conn
            .query_row(
                "SELECT user_id FROM device_users WHERE device_id = ?1 AND assigned_slug = 'alice-laptop'",
                rusqlite::params![device_id],
                |row| row.get(0),
            )
            .optional()
            .expect("query device_users");
        assert_eq!(
            slug_owner,
            Some(alice_id),
            "slug must be stored under alice_id, not bridge.user_id"
        );
    }

    #[tokio::test]
    async fn device_get_shared_bridge_with_username() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_shared_bridge().await;

        // Create two users with devices; verify DeviceGet scopes to the specified user.
        let alice_id = {
            let conn = bridge.db.lock().await;
            brenn_lib::auth::user::create_user(&conn, "alice", "$argon2id$fake")
        };
        let bob_id = {
            let conn = bridge.db.lock().await;
            brenn_lib::auth::user::create_user(&conn, "bob", "$argon2id$fake")
        };
        let alice_device =
            create_test_device_for_user(&bridge.db, alice_id, "Mozilla/5.0 Chrome/125").await;
        let bob_device =
            create_test_device_for_user(&bridge.db, bob_id, "Mozilla/5.0 Firefox/126").await;

        // DeviceGet scoped to alice should see only alice's device (positive case).
        let req = post_tool_use_req(
            MCP_DEVICE_GET_TOOL,
            serde_json::json!({"device": alice_device.to_string(), "username": "alice"}),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).expect("valid JSON");
                let devices = parsed["devices"].as_array().expect("devices array");
                assert_eq!(
                    devices.len(),
                    1,
                    "DeviceGet with username must return only that user's device: {parsed}"
                );
                assert_eq!(devices[0]["id"], alice_device);
            }
            other => panic!("expected Continue with devices, got {other:?}"),
        }

        // DeviceGet scoped to alice but requesting bob's device → not_found (negative / isolation case).
        let req_negative = post_tool_use_req(
            MCP_DEVICE_GET_TOOL,
            serde_json::json!({"device": bob_device.to_string(), "username": "alice"}),
        );
        let result_negative = handle_brenn_tools(&bridge, &req_negative).await;
        match result_negative {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).expect("valid JSON");
                assert_eq!(
                    parsed["error"], "not_found",
                    "DeviceGet with username=alice must not return bob's device: {parsed}"
                );
            }
            other => panic!("expected Continue with not_found, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn device_list_shared_bridge_with_username() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_shared_bridge().await;

        // Create two users with devices; verify DeviceList scopes to the specified user.
        let alice_id = {
            let conn = bridge.db.lock().await;
            brenn_lib::auth::user::create_user(&conn, "alice", "$argon2id$fake")
        };
        let bob_id = {
            let conn = bridge.db.lock().await;
            brenn_lib::auth::user::create_user(&conn, "bob", "$argon2id$fake")
        };
        let _alice_device =
            create_test_device_for_user(&bridge.db, alice_id, "Mozilla/5.0 Chrome/125").await;
        let _bob_device =
            create_test_device_for_user(&bridge.db, bob_id, "Mozilla/5.0 Firefox/126").await;

        // DeviceList scoped to alice should see only alice's devices.
        let req = post_tool_use_req(
            MCP_DEVICE_LIST_TOOL,
            serde_json::json!({"username": "alice"}),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).expect("valid JSON");
                let devices = parsed["devices"].as_array().expect("devices array");
                assert_eq!(
                    devices.len(),
                    1,
                    "DeviceList with username must return only that user's devices: {parsed}"
                );
            }
            other => panic!("expected Continue with devices, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn device_assign_slug_username_scopes_to_specified_user() {
        // Non-shared bridge: username param causes the operation to use the specified user's namespace.
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;

        let other_id = {
            let conn = bridge.db.lock().await;
            brenn_lib::auth::user::create_user(&conn, "other_user", "$argon2id$fake")
        };
        let other_device =
            create_test_device_for_user(&bridge.db, other_id, "Mozilla/5.0 Chrome/125").await;

        // Specifying username="other_user" must operate on other_user's namespace, not bridge.user_id.
        let req = post_tool_use_req(
            MCP_DEVICE_ASSIGN_SLUG_TOOL,
            serde_json::json!({
                "device": other_device.to_string(),
                "slug": "other-laptop",
                "username": "other_user"
            }),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).expect("valid JSON");
                assert_eq!(
                    parsed["ok"], true,
                    "DeviceAssignSlug with explicit username must succeed on specified user's device: {parsed}"
                );
                assert_eq!(parsed["assigned_slug"], "other-laptop");
            }
            other => panic!("expected Continue with ok, got {other:?}"),
        }

        // Verify slug is stored under other_id, not bridge.user_id.
        let conn = bridge.db.lock().await;
        let slug_owner: Option<i64> = conn
            .query_row(
                "SELECT user_id FROM device_users WHERE device_id = ?1 AND assigned_slug = 'other-laptop'",
                rusqlite::params![other_device],
                |row| row.get(0),
            )
            .optional()
            .expect("query device_users");
        assert_eq!(
            slug_owner,
            Some(other_id),
            "slug must be stored under other_id ({other_id}), not bridge.user_id ({})",
            bridge.user_id
        );
    }

    #[tokio::test]
    async fn device_assign_slug_username_not_in_allowed_users() {
        // Bridge with non-empty allowed_users; providing a username not in the list → user_not_in_app.
        let (bridge, _event_tx, _broadcast_rx, _ab) =
            test_bridge_with_allowed_users(vec!["testuser".to_string()]).await;

        let device_id =
            create_test_device_for_user(&bridge.db, bridge.user_id, "Mozilla/5.0 Chrome/125").await;

        // Insert "outsider" into the DB — the allowed_users check must fire against a real
        // existing user to confirm the guard runs before the DB lookup, not after.
        {
            let conn = bridge.db.lock().await;
            brenn_lib::auth::user::create_user(&conn, "outsider", "$argon2id$fake");
        }

        // "outsider" exists in the DB but is not in allowed_users → user_not_in_app.
        let req = post_tool_use_req(
            MCP_DEVICE_ASSIGN_SLUG_TOOL,
            serde_json::json!({
                "device": device_id.to_string(),
                "slug": "stolen",
                "username": "outsider"
            }),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).expect("valid JSON");
                assert_eq!(
                    parsed["error"], "user_not_in_app",
                    "username not in allowed_users must return user_not_in_app: {parsed}"
                );
            }
            other => panic!("expected Continue with user_not_in_app error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn device_list_username_not_found_in_db() {
        // DeviceList with a username that does not exist in the DB → user_not_found.
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;

        let req = post_tool_use_req(
            MCP_DEVICE_LIST_TOOL,
            serde_json::json!({"username": "nobody_exists"}),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).expect("valid JSON");
                assert_eq!(
                    parsed["error"], "user_not_found",
                    "unknown username must return user_not_found: {parsed}"
                );
            }
            other => panic!("expected Continue with user_not_found error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn device_get_username_not_in_allowed_users() {
        // DeviceGet on a restricted bridge: username not in allowed_users → user_not_in_app.
        let (bridge, _event_tx, _broadcast_rx, _ab) =
            test_bridge_with_allowed_users(vec!["testuser".to_string()]).await;

        let device_id =
            create_test_device_for_user(&bridge.db, bridge.user_id, "Mozilla/5.0 Chrome/125").await;

        // Create outsider in DB to ensure the allowed_users check fires first.
        {
            let conn = bridge.db.lock().await;
            brenn_lib::auth::user::create_user(&conn, "outsider", "$argon2id$fake");
        }

        let req = post_tool_use_req(
            MCP_DEVICE_GET_TOOL,
            serde_json::json!({"device": device_id.to_string(), "username": "outsider"}),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).expect("valid JSON");
                assert_eq!(
                    parsed["error"], "user_not_in_app",
                    "DeviceGet with username not in allowed_users must return user_not_in_app: {parsed}"
                );
            }
            other => panic!("expected Continue with user_not_in_app error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn device_list_username_not_in_allowed_users() {
        // DeviceList on a restricted bridge: username not in allowed_users → user_not_in_app.
        let (bridge, _event_tx, _broadcast_rx, _ab) =
            test_bridge_with_allowed_users(vec!["testuser".to_string()]).await;

        // Create outsider in DB to ensure the allowed_users check fires first.
        {
            let conn = bridge.db.lock().await;
            brenn_lib::auth::user::create_user(&conn, "outsider", "$argon2id$fake");
        }

        let req = post_tool_use_req(
            MCP_DEVICE_LIST_TOOL,
            serde_json::json!({"username": "outsider"}),
        );
        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).expect("valid JSON");
                assert_eq!(
                    parsed["error"], "user_not_in_app",
                    "DeviceList with username not in allowed_users must return user_not_in_app: {parsed}"
                );
            }
            other => panic!("expected Continue with user_not_in_app error, got {other:?}"),
        }
    }
}
