use std::path::PathBuf;

use serde::Deserialize;
pub use tracing::level_filters::LevelFilter;

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LoggingConfig {
    /// Directory for all log files (diagnostic, security, and CC transcripts).
    pub log_dir: PathBuf,
    /// Console log level (trace/debug/info/warn/error).
    #[serde(deserialize_with = "deserialize_level_filter")]
    pub console_level: LevelFilter,
    /// File log level.
    #[serde(deserialize_with = "deserialize_level_filter")]
    pub file_level: LevelFilter,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            log_dir: PathBuf::from("/var/log/brenn"),
            console_level: LevelFilter::INFO,
            file_level: LevelFilter::DEBUG,
        }
    }
}

/// Deserialize a `LevelFilter` from a string like "info", "debug", etc.
fn deserialize_level_filter<'de, D>(deserializer: D) -> Result<LevelFilter, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    s.parse::<LevelFilter>()
        .map_err(|e| serde::de::Error::custom(format!("invalid log level \"{s}\": {e}")))
}
