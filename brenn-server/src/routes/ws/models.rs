//! Per-app model allow-list filtering.
//!
//! The `cached_models` map and the `app_models` DB table hold what CC
//! reported, unfiltered — the cache is a record of fact and the allow-list is
//! policy. Filtering happens on read, at every site that builds a wire-bound
//! model list, so a config change takes effect on restart with no cache
//! migration and lifting a restriction never has to recover dropped entries.

use brenn_ws_types::ModelInfo;

/// CC-reported models restricted to the app's allow-list.
///
/// `None` allow-list = unrestricted (the reported list passes through
/// unchanged). Preserves CC's reported order; allow-list entries CC never
/// reported (typos, or aliases retired since the last spawn) simply do not
/// appear. The result can be empty.
pub(super) fn filter_models(allow: Option<&[String]>, reported: &[ModelInfo]) -> Vec<ModelInfo> {
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
