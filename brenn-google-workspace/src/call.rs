//! `api-request` calls on the instance's one host.

use std::collections::BTreeMap;

use brenn_provider_kit::{ApiCall, Body, Query, QueryValue};
use serde_json::Value;

use crate::settings::Settings;

/// A GET on the instance's host for `account`.
pub fn get_call(settings: &Settings, account: Option<&str>, path: String, query: Query) -> ApiCall {
    new_call(settings, "GET", account, path, query, None)
}

/// A `method` call on the instance's host for `account` carrying `body` as
/// JSON text, which `api-request` sends as `application/json` when no
/// `Content-Type` is given.
pub fn json_call(
    settings: &Settings,
    method: &str,
    account: Option<&str>,
    path: String,
    query: Query,
    body: Value,
) -> ApiCall {
    let body = serde_json::to_string(&body).expect("a JSON value serializes");
    new_call(
        settings,
        method,
        account,
        path,
        query,
        Some(Body::Text(body)),
    )
}

/// A DELETE on the instance's host for `account`, with no body.
pub fn delete_call(
    settings: &Settings,
    account: Option<&str>,
    path: String,
    query: Query,
) -> ApiCall {
    new_call(settings, "DELETE", account, path, query, None)
}

/// One `api-request` call's arguments. `account`, when the caller named one,
/// is passed on every call a request makes, so a request never mixes
/// accounts. Every Google call adds `prettyPrint=false` to `query`: Google's
/// indentation would count against the read limit and the bus cap like any
/// other byte.
pub fn new_call(
    settings: &Settings,
    method: &str,
    account: Option<&str>,
    path: String,
    mut query: Query,
    body: Option<Body>,
) -> ApiCall {
    query.insert(
        "prettyPrint".to_string(),
        QueryValue::One("false".to_string()),
    );
    ApiCall {
        provider: settings.provider.clone(),
        account: account.map(str::to_string),
        method: method.to_string(),
        host: settings.host.clone(),
        path,
        query,
        headers: BTreeMap::new(),
        body,
    }
}
