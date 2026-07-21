use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::error::{Error, Result};
use crate::pricing::{Family, PriceTable, PriceVector};

/// Loaded configuration (prices + project roots override).
#[derive(Debug, Clone)]
pub struct Config {
    pub prices: PriceTable,
    pub project_roots: Vec<PathBuf>,
}

impl Config {
    pub fn defaults() -> Self {
        Self {
            prices: PriceTable::defaults(),
            project_roots: vec![],
        }
    }
}

/// Raw TOML shape for the config file.
///
/// Config file example:
/// ```toml
/// # Exact model overrides
/// [prices."claude-sonnet-4-6"]
/// input = 3.00
/// cache_write_5m = 3.75
/// cache_write_1h = 6.00
/// cache_read = 0.30
/// output = 15.00
///
/// # Family fallback defaults
/// [prices.family_defaults.opus]
/// input = 5.00
/// cache_write_5m = 6.25
/// cache_write_1h = 10.00
/// cache_read = 0.50
/// output = 25.00
///
/// project_roots = ["/custom/path/to/claude/projects"]
/// ```
#[derive(Debug, Deserialize, Default)]
struct TomlConfig {
    #[serde(default)]
    prices: TomlPrices,
    #[serde(default)]
    project_roots: Vec<PathBuf>,
}

#[derive(Debug, Deserialize, Default)]
struct TomlPrices {
    /// Exact model name overrides.
    #[serde(flatten)]
    models: BTreeMap<String, TomlPriceVector>,
    /// Family-level fallback defaults.
    #[serde(default)]
    family_defaults: TomlFamilyDefaults,
}

#[derive(Debug, Deserialize, Default)]
struct TomlFamilyDefaults {
    opus: Option<TomlPriceVector>,
    sonnet: Option<TomlPriceVector>,
    haiku: Option<TomlPriceVector>,
}

#[derive(Debug, Deserialize)]
struct TomlPriceVector {
    input: f64,
    cache_write_5m: f64,
    cache_write_1h: f64,
    cache_read: f64,
    output: f64,
}

impl From<TomlPriceVector> for PriceVector {
    fn from(t: TomlPriceVector) -> Self {
        PriceVector {
            input: t.input,
            cache_write_5m: t.cache_write_5m,
            cache_write_1h: t.cache_write_1h,
            cache_read: t.cache_read,
            output: t.output,
        }
    }
}

/// Load config from a TOML file, merging over defaults.
/// If `path` is `None`, returns defaults.
pub fn load(path: Option<&Path>) -> Result<Config> {
    let Some(p) = path else {
        return Ok(Config::defaults());
    };

    let text = std::fs::read_to_string(p).map_err(|e| Error::Io {
        path: p.to_path_buf(),
        source: e,
    })?;

    let raw: TomlConfig = toml::from_str(&text).map_err(|e| Error::Config(e.to_string()))?;

    let mut cfg = Config::defaults();

    // Apply exact model overrides. The `family_defaults` key is captured by
    // the typed field in `TomlPrices` (serde-flatten consumes typed fields
    // first), so it never reaches this map.
    for (key, pv) in raw.prices.models {
        cfg.prices.exact.insert(key, pv.into());
    }

    // Apply family default overrides
    if let Some(v) = raw.prices.family_defaults.opus {
        cfg.prices.family_defaults.insert(Family::Opus, v.into());
    }
    if let Some(v) = raw.prices.family_defaults.sonnet {
        cfg.prices.family_defaults.insert(Family::Sonnet, v.into());
    }
    if let Some(v) = raw.prices.family_defaults.haiku {
        cfg.prices.family_defaults.insert(Family::Haiku, v.into());
    }

    if !raw.project_roots.is_empty() {
        cfg.project_roots = raw.project_roots;
    }

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_toml(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f
    }

    #[test]
    fn defaults_roundtrip() {
        let cfg = load(None).unwrap();
        // Sonnet should be present
        let v = cfg.prices.lookup("claude-sonnet-4-6").unwrap();
        assert!((v.input - 3.0).abs() < 1e-9);
    }

    #[test]
    fn override_one_model() {
        let toml = r#"
[prices."claude-sonnet-4-6"]
input = 99.0
cache_write_5m = 1.0
cache_write_1h = 2.0
cache_read = 0.5
output = 50.0
"#;
        let f = write_toml(toml);
        let cfg = load(Some(f.path())).unwrap();
        let v = cfg.prices.lookup("claude-sonnet-4-6").unwrap();
        assert!((v.input - 99.0).abs() < 1e-9);
        // Other models unchanged
        let v2 = cfg.prices.lookup("claude-opus-4-7").unwrap();
        assert!((v2.input - 5.0).abs() < 1e-9);
    }

    #[test]
    fn override_family_default() {
        let toml = r#"
[prices.family_defaults.opus]
input = 7.0
cache_write_5m = 8.0
cache_write_1h = 12.0
cache_read = 0.7
output = 35.0
"#;
        let f = write_toml(toml);
        let cfg = load(Some(f.path())).unwrap();
        // Unknown opus model should use new default
        let v = cfg.prices.lookup("claude-opus-future-unknown").unwrap();
        assert!((v.input - 7.0).abs() < 1e-9);
    }

    #[test]
    fn project_roots_override() {
        let toml = r#"project_roots = ["/tmp/custom/projects"]"#;
        let f = write_toml(toml);
        let cfg = load(Some(f.path())).unwrap();
        assert_eq!(
            cfg.project_roots,
            vec![PathBuf::from("/tmp/custom/projects")]
        );
    }

    #[test]
    fn malformed_toml_returns_error() {
        let toml = "this is [ not valid toml ~~~";
        let f = write_toml(toml);
        let err = load(Some(f.path())).unwrap_err();
        assert!(
            matches!(err, Error::Config(_)),
            "expected Config error, got {err:?}"
        );
    }
}
