//! The per-app cache of the models CC reported at init, and the allow-list
//! filter every wire-bound model list passes through.
//!
//! Which models an account is offered is a property of the account, so the
//! cache is refreshed by every spawn *and* by every profile swap. Both of them
//! reach it through [`ModelCache::record_app_models`].
//!
//! The cache and the `app_models` DB table hold what CC reported, unfiltered —
//! the cache is a record of fact and the allow-list is policy. Filtering
//! happens on read, at every site that builds a wire-bound model list, so a
//! config change takes effect on restart with no cache migration and lifting a
//! restriction never has to recover dropped entries.

use std::collections::HashMap;
use std::sync::Arc;

use brenn_cc::session::ModelOption;
use brenn_db::Db;
use brenn_lib::config::AppConfig;
use brenn_ws_types::ModelInfo;
use indexmap::IndexMap;
use tokio::sync::RwLock;
use tracing::warn;

/// The three handles needed to record an app's model set: the in-memory cache,
/// the DB behind it, and the app table the allow-list is read from.
#[derive(Clone)]
pub(crate) struct ModelCache {
    pub db: Db,
    pub apps: Arc<IndexMap<String, AppConfig>>,
    pub cached: Arc<RwLock<HashMap<String, Vec<ModelInfo>>>>,
}

impl ModelCache {
    /// Convert CC's reported model options, warn about allow-list entries CC did
    /// not report, and write the set to the memory cache and the DB.
    ///
    /// Returns the unfiltered `ModelInfo` view; callers that send it to a client
    /// filter it through the app's allow-list first.
    pub(crate) async fn record_app_models(
        &self,
        app_slug: &str,
        models: &[ModelOption],
    ) -> Vec<ModelInfo> {
        let model_infos: Vec<ModelInfo> = models
            .iter()
            .map(|m| ModelInfo {
                value: m.value.clone(),
                display_name: m.display_name.clone(),
                description: m.description.clone(),
            })
            .collect();
        if model_infos.is_empty() {
            return model_infos;
        }

        // Alias spellings in `models` are CC-defined and unverifiable until
        // now. An entry CC did not report is invisible everywhere downstream —
        // it just never appears in the picker — so say so once per spawn,
        // which is the only chance to connect the symptom to the config.
        let allow_list = self.allow_list(app_slug);
        let unreported = unreported_allow_entries(allow_list.as_deref(), &model_infos);
        if !unreported.is_empty() {
            warn!(
                app_slug = %app_slug,
                unreported = ?unreported,
                configured = ?allow_list,
                reported = ?model_infos.iter().map(|m| m.value.as_str()).collect::<Vec<_>>(),
                "app `models` names aliases CC did not report; they cannot be offered"
            );
        }

        self.cached
            .write()
            .await
            .insert(app_slug.to_string(), model_infos.clone());
        {
            let conn = self.db.lock().await;
            brenn_db::save_app_models(&conn, app_slug, &model_infos);
        }
        model_infos
    }

    /// Record the reported set and return it restricted to the app's
    /// allow-list — what a picker may show.
    pub(crate) async fn record_and_filter(
        &self,
        app_slug: &str,
        models: &[ModelOption],
    ) -> Vec<ModelInfo> {
        let reported = self.record_app_models(app_slug, models).await;
        filter_models(self.allow_list(app_slug).as_deref(), &reported)
    }

    /// This app's model allow-list, or `None` when it offers everything CC
    /// reports.
    pub(crate) fn allow_list(&self, app_slug: &str) -> Option<Vec<String>> {
        self.apps.get(app_slug).and_then(|a| a.models.clone())
    }
}

/// CC-reported models restricted to the app's allow-list.
///
/// `None` allow-list = unrestricted (the reported list passes through
/// unchanged). Preserves CC's reported order; allow-list entries CC never
/// reported (typos, or aliases retired since the last spawn) simply do not
/// appear. The result can be empty.
pub(crate) fn filter_models(allow: Option<&[String]>, reported: &[ModelInfo]) -> Vec<ModelInfo> {
    reported
        .iter()
        .filter(|mi| brenn_lib::config::model_allowed(allow, &mi.value))
        .cloned()
        .collect()
}

/// Allow-list entries CC did not report, in allow-list order.
pub(crate) fn unreported_allow_entries<'a>(
    allow: Option<&'a [String]>,
    reported: &[ModelInfo],
) -> Vec<&'a str> {
    let Some(allow) = allow else {
        return Vec::new();
    };
    allow
        .iter()
        .map(String::as_str)
        .filter(|a| !reported.iter().any(|mi| mi.value == *a))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn values(models: &[ModelInfo]) -> Vec<&str> {
        models.iter().map(|m| m.value.as_str()).collect()
    }

    fn mi(value: &str) -> ModelInfo {
        ModelInfo {
            value: value.to_string(),
            display_name: format!("Display {value}"),
            description: format!("Description of {value}"),
        }
    }

    fn opt(value: &str) -> ModelOption {
        ModelOption {
            value: value.to_string(),
            display_name: format!("Display {value}"),
            description: format!("Description of {value}"),
        }
    }

    /// The swap's own path through the cache: record what the new account
    /// reported, hand back only what the app allows.
    #[tokio::test]
    async fn record_and_filter_caches_the_reported_set_and_returns_the_allowed_one() {
        let db = crate::test_support::init_db_memory();
        let mut app = brenn_lib::config::test_app_config("testapp");
        app.models = Some(vec!["sonnet".to_string(), "typo".to_string()]);
        let apps = Arc::new(IndexMap::from([("testapp".to_string(), app)]));
        let cache = ModelCache {
            db,
            apps,
            cached: Arc::new(RwLock::new(HashMap::new())),
        };

        let reported = [opt("sonnet"), opt("haiku")];
        let filtered = cache.record_and_filter("testapp", &reported).await;
        assert_eq!(
            values(&filtered),
            vec!["sonnet"],
            "the picker is offered the allow-list intersection"
        );

        assert_eq!(
            values(
                cache
                    .cached
                    .read()
                    .await
                    .get("testapp")
                    .expect("the app was recorded")
            ),
            vec!["sonnet", "haiku"],
            "the cache is a record of fact: unfiltered, in CC's order"
        );
        let conn = cache.db.lock().await;
        assert_eq!(
            values(&brenn_db::load_app_models(&conn, "testapp")),
            vec!["sonnet", "haiku"],
            "the DB carries the same unfiltered record across restarts"
        );
    }

    #[test]
    fn none_allow_list_is_passthrough() {
        let reported = vec![mi("default"), mi("sonnet"), mi("haiku")];
        let got = filter_models(None, &reported);
        assert_eq!(values(&got), vec!["default", "sonnet", "haiku"]);
        assert_eq!(got[0].display_name, "Display default");
        assert_eq!(got[0].description, "Description of default");
    }

    #[test]
    fn intersection_preserves_reported_order() {
        let reported = vec![mi("default"), mi("sonnet"), mi("haiku")];
        // Allow-list order is the reverse of the reported order; the reported
        // order is what survives.
        let allow = vec!["haiku".to_string(), "default".to_string()];
        let got = filter_models(Some(&allow), &reported);
        assert_eq!(values(&got), vec!["default", "haiku"]);
    }

    #[test]
    fn allow_entries_absent_from_reported_drop_out() {
        let reported = vec![mi("default"), mi("sonnet")];
        let allow = vec!["sonnet".to_string(), "opus[1m]".to_string()];
        let got = filter_models(Some(&allow), &reported);
        assert_eq!(values(&got), vec!["sonnet"]);
    }

    #[test]
    fn result_can_be_empty() {
        let reported = vec![mi("default"), mi("sonnet")];
        let allow = vec!["opus[1m]".to_string()];
        assert!(filter_models(Some(&allow), &reported).is_empty());
    }

    #[test]
    fn empty_reported_yields_empty() {
        let allow = vec!["default".to_string()];
        assert!(filter_models(Some(&allow), &[]).is_empty());
        assert!(filter_models(None, &[]).is_empty());
    }

    #[test]
    fn everything_reported_leaves_nothing_unreported() {
        let reported = vec![mi("default"), mi("sonnet"), mi("haiku")];
        let allow = vec!["haiku".to_string(), "default".to_string()];
        assert!(unreported_allow_entries(Some(&allow), &reported).is_empty());
    }

    #[test]
    fn unreported_names_exactly_the_missing_subset() {
        let reported = vec![mi("default"), mi("sonnet")];
        let allow = vec![
            "sonnet".to_string(),
            "opus[1m]".to_string(),
            "typo".to_string(),
        ];
        assert_eq!(
            unreported_allow_entries(Some(&allow), &reported),
            vec!["opus[1m]", "typo"],
            "only the entries CC never reported, in allow-list order"
        );
    }

    #[test]
    fn no_allow_list_reports_nothing_unreported() {
        let reported = vec![mi("default")];
        assert!(unreported_allow_entries(None, &reported).is_empty());
        assert!(unreported_allow_entries(None, &[]).is_empty());
    }
}
