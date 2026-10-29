//! The connect flow's in-memory half: pending flows keyed by `state`, the
//! authorize URL a flow sends the user to, and the reading of the provider's
//! redirect back.
//!
//! A pending flow lives only in this process. A restart, an expiry or a use
//! ends it; the user then starts again from the assistant.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use base64ct::{Base64UrlUnpadded, Encoding};
use brenn_lib::config::OAuthProviderConfig;
use brenn_tool_registry::ActingUser;
use rand::RngExt;

/// How long a started flow waits for its callback, a login in between
/// included.
pub const FLOW_TTL: Duration = Duration::from_secs(10 * 60);

/// Most live flows one user may hold.
pub const MAX_FLOWS_PER_USER: usize = 4;

/// Most live flows in total.
pub const MAX_FLOWS_TOTAL: usize = 64;

/// One started connect, waiting for the provider's redirect.
#[derive(Clone, Debug)]
pub struct PendingFlow {
    pub provider: String,
    pub user: ActingUser,
    pub conversation_id: Option<i64>,
    /// The PKCE verifier whose S256 challenge the authorize URL carried.
    pub verifier: String,
    /// Every scope requested, base scopes first.
    pub scopes: Vec<String>,
    pub created_at: Instant,
}

/// The live flows, keyed by `state`.
#[derive(Default)]
pub struct PendingFlows {
    inner: Mutex<HashMap<String, PendingFlow>>,
}

impl PendingFlows {
    /// Record `flow` under `state`, first dropping expired flows and then, at
    /// either cap, the oldest of the user's flows or the oldest overall.
    pub fn insert(&self, state: String, flow: PendingFlow) {
        self.insert_at(state, flow, Instant::now());
    }

    fn insert_at(&self, state: String, flow: PendingFlow, now: Instant) {
        let mut map = self.map();
        map.retain(|_, f| now.duration_since(f.created_at) < FLOW_TTL);
        let user_id = flow.user.id;
        while map.values().filter(|f| f.user.id == user_id).count() >= MAX_FLOWS_PER_USER {
            let oldest = oldest_key(&map, |f| f.user.id == user_id);
            map.remove(&oldest);
        }
        while map.len() >= MAX_FLOWS_TOTAL {
            let oldest = oldest_key(&map, |_| true);
            map.remove(&oldest);
        }
        map.insert(state, flow);
    }

    /// Remove and return the live flow under `state`. An expired flow is
    /// removed and not returned.
    pub fn take(&self, state: &str) -> Option<PendingFlow> {
        self.take_at(state, Instant::now())
    }

    fn take_at(&self, state: &str, now: Instant) -> Option<PendingFlow> {
        self.map()
            .remove(state)
            .filter(|f| now.duration_since(f.created_at) < FLOW_TTL)
    }

    fn map(&self) -> std::sync::MutexGuard<'_, HashMap<String, PendingFlow>> {
        self.inner.lock().expect("pending flows lock poisoned")
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.map().len()
    }
}

fn oldest_key(map: &HashMap<String, PendingFlow>, pick: impl Fn(&PendingFlow) -> bool) -> String {
    map.iter()
        .filter(|(_, f)| pick(f))
        .min_by_key(|(_, f)| f.created_at)
        .map(|(k, _)| k.clone())
        .expect("a cap is only exceeded by a non-empty set")
}

/// A fresh `state`: 32 CSPRNG bytes, lowercase hex.
pub fn mint_state() -> String {
    let mut bytes = [0u8; 32];
    rand::rng().fill(&mut bytes);
    hex::encode(bytes)
}

/// Whether `state` has the shape [`mint_state`] produces.
pub fn state_well_formed(state: &str) -> bool {
    state.len() == 64
        && state
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// A fresh PKCE verifier: 32 CSPRNG bytes, base64url without padding.
pub fn mint_verifier() -> String {
    let mut bytes = [0u8; 32];
    rand::rng().fill(&mut bytes);
    Base64UrlUnpadded::encode_string(&bytes)
}

/// The S256 challenge for `verifier`.
pub fn s256_challenge(verifier: &str) -> String {
    Base64UrlUnpadded::encode_string(&brenn_lib::util::sha256(verifier.as_bytes()))
}

/// The authorize URL for a flow: the provider's `authorize_url` with the
/// standard parameters, then `login_hint`, then the provider's extras.
pub fn authorize_url(
    provider: &OAuthProviderConfig,
    scopes: &[String],
    state: &str,
    verifier: &str,
    login_hint: Option<&str>,
) -> String {
    let mut url =
        url::Url::parse(&provider.raw.authorize_url).expect("authorize_url was validated at boot");
    {
        let mut q = url.query_pairs_mut();
        q.append_pair("response_type", "code")
            .append_pair("client_id", &provider.client_id)
            .append_pair("redirect_uri", &provider.redirect_uri)
            .append_pair("scope", &scopes.join(" "))
            .append_pair("state", state)
            .append_pair("code_challenge", &s256_challenge(verifier))
            .append_pair("code_challenge_method", "S256");
        if let Some(hint) = login_hint {
            q.append_pair("login_hint", hint);
        }
        for (k, v) in &provider.raw.extra_authorize_params {
            q.append_pair(k, v);
        }
    }
    url.into()
}

/// The values of a provider's redirect back that Brenn reads.
#[derive(Debug, PartialEq, Eq)]
pub struct CallbackQuery {
    pub state: String,
    pub code: Option<String>,
    pub error: Option<String>,
    pub error_description: Option<String>,
    /// Every other key, in arrival order: the provider's vocabulary, ignored.
    pub ignored: Vec<String>,
}

/// Why a redirect's query cannot be from a provider answering a flow Brenn
/// started.
#[derive(Debug, PartialEq, Eq)]
pub struct QueryViolation(pub String);

/// Read a callback query. Only the values Brenn minted (`state`) or relies on
/// (`code`, `error`) are held to a shape; every other key is collected into
/// [`CallbackQuery::ignored`].
pub fn parse_callback_query(raw: Option<&str>) -> Result<CallbackQuery, QueryViolation> {
    let mut state = None;
    let mut code = None;
    let mut error = None;
    let mut error_description = None;
    let mut ignored = Vec::new();
    for (key, value) in url::form_urlencoded::parse(raw.unwrap_or("").as_bytes()) {
        let slot = match &*key {
            "state" => &mut state,
            "code" => &mut code,
            "error" => &mut error,
            "error_description" => &mut error_description,
            _ => {
                ignored.push(key.into_owned());
                continue;
            }
        };
        if slot.is_some() {
            return Err(QueryViolation(format!("duplicate `{key}`")));
        }
        *slot = Some(value.into_owned());
    }
    let state = state.ok_or_else(|| QueryViolation("missing `state`".to_string()))?;
    if !state_well_formed(&state) {
        return Err(QueryViolation(format!("malformed `state` {state:?}")));
    }
    Ok(CallbackQuery {
        state,
        code,
        error,
        error_description,
        ignored,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn flow(user_id: i64, created_at: Instant) -> PendingFlow {
        PendingFlow {
            provider: "google".to_string(),
            user: ActingUser {
                id: user_id,
                username: format!("user{user_id}"),
            },
            conversation_id: Some(1),
            verifier: "v".to_string(),
            scopes: vec![],
            created_at,
        }
    }

    #[test]
    fn take_is_single_use() {
        let flows = PendingFlows::default();
        flows.insert("s".to_string(), flow(1, Instant::now()));
        assert!(flows.take("s").is_some());
        assert!(flows.take("s").is_none());
    }

    #[test]
    fn an_expired_flow_is_not_returned() {
        let flows = PendingFlows::default();
        let t0 = Instant::now();
        flows.insert_at("s".to_string(), flow(1, t0), t0);
        assert!(flows.take_at("s", t0 + FLOW_TTL).is_none());
        assert_eq!(flows.len(), 0, "taking removes the expired flow");
    }

    #[test]
    fn insert_sweeps_expired_flows() {
        let flows = PendingFlows::default();
        let t0 = Instant::now();
        flows.insert_at("old".to_string(), flow(1, t0), t0);
        let later = t0 + FLOW_TTL + Duration::from_secs(1);
        flows.insert_at("new".to_string(), flow(2, later), later);
        assert_eq!(flows.len(), 1);
        assert!(flows.take_at("new", later).is_some());
    }

    #[test]
    fn per_user_cap_drops_that_users_oldest() {
        let flows = PendingFlows::default();
        let t0 = Instant::now();
        flows.insert_at("other".to_string(), flow(2, t0), t0);
        for i in 0..=MAX_FLOWS_PER_USER {
            let at = t0 + Duration::from_secs(i as u64 + 1);
            flows.insert_at(format!("u{i}"), flow(1, at), at);
        }
        let now = t0 + Duration::from_secs(10);
        assert!(flows.take_at("u0", now).is_none(), "the user's oldest went");
        assert!(
            flows.take_at("other", now).is_some(),
            "the older flow of another user stays"
        );
        for i in 1..=MAX_FLOWS_PER_USER {
            assert!(flows.take_at(&format!("u{i}"), now).is_some());
        }
    }

    #[test]
    fn global_cap_drops_the_oldest_overall() {
        let flows = PendingFlows::default();
        let t0 = Instant::now();
        for i in 0..=MAX_FLOWS_TOTAL {
            let at = t0 + Duration::from_millis(i as u64);
            flows.insert_at(format!("s{i}"), flow(i as i64, at), at);
        }
        assert_eq!(flows.len(), MAX_FLOWS_TOTAL);
        let now = t0 + Duration::from_secs(1);
        assert!(flows.take_at("s0", now).is_none());
        assert!(flows.take_at(&format!("s{MAX_FLOWS_TOTAL}"), now).is_some());
    }

    #[test]
    fn minted_values_have_their_shapes() {
        let s = mint_state();
        assert!(state_well_formed(&s));
        assert_ne!(s, mint_state());
        let v = mint_verifier();
        assert_eq!(v.len(), 43);
        assert!(!state_well_formed("ABCD"));
        assert!(!state_well_formed(&"A".repeat(64)));
    }

    #[test]
    fn s256_matches_rfc7636_appendix_b() {
        assert_eq!(
            s256_challenge("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
    }

    #[test]
    fn authorize_url_carries_every_parameter() {
        let (_, mut provider) =
            crate::testutil::provider_config("google", "https://auth.example.com");
        provider.raw.extra_authorize_params =
            BTreeMap::from([("access_type".to_string(), "offline".to_string())]);
        let scopes = vec!["openid".to_string(), "mail".to_string()];
        let url = authorize_url(&provider, &scopes, "st", "ver", Some("alice@example.com"));
        let url = url::Url::parse(&url).unwrap();
        assert_eq!(url.path(), "/authorize");
        let pairs: Vec<(String, String)> = url.query_pairs().into_owned().collect();
        let expect = |k: &str, v: &str| (k.to_string(), v.to_string());
        assert_eq!(
            pairs,
            vec![
                expect("response_type", "code"),
                expect("client_id", "client-id"),
                expect(
                    "redirect_uri",
                    "https://brenn.example.com/oauth/google/callback"
                ),
                expect("scope", "openid mail"),
                expect("state", "st"),
                expect("code_challenge", &s256_challenge("ver")),
                expect("code_challenge_method", "S256"),
                expect("login_hint", "alice@example.com"),
                expect("access_type", "offline"),
            ]
        );
        assert!(
            !pairs.iter().any(|(_, v)| v == "client-secret"),
            "the secret never leaves"
        );
    }

    fn state() -> String {
        "ab".repeat(32)
    }

    #[test]
    fn callback_query_reads_brenns_keys_and_ignores_the_rest() {
        let raw = format!(
            "state={}&code=c1&scope=a+b&iss=https%3A%2F%2Fx&hd=example.com",
            state()
        );
        let q = parse_callback_query(Some(&raw)).unwrap();
        assert_eq!(q.state, state());
        assert_eq!(q.code.as_deref(), Some("c1"));
        assert_eq!(q.error, None);
        assert_eq!(q.ignored, vec!["scope", "iss", "hd"]);

        let raw = format!(
            "error=access_denied&error_description=no+thanks&error_uri=x&state={}",
            state()
        );
        let q = parse_callback_query(Some(&raw)).unwrap();
        assert_eq!(q.error.as_deref(), Some("access_denied"));
        assert_eq!(q.error_description.as_deref(), Some("no thanks"));
        assert_eq!(q.ignored, vec!["error_uri"]);
    }

    #[test]
    fn callback_query_violations() {
        let s = state();
        for raw in [
            String::new(),
            "code=c".to_string(),
            format!("state={s}&state={s}"),
            "state=xyz".to_string(),
            format!("state={}", "AB".repeat(32)),
            format!("state={s}&code=a&code=b"),
            format!("state={s}&error=a&error=b"),
        ] {
            assert!(
                parse_callback_query(Some(&raw)).is_err(),
                "{raw:?} must be refused"
            );
        }
        assert!(parse_callback_query(None).is_err());
    }
}
