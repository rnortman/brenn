use serde::Deserialize;

/// Start hook configuration for an app.
/// Hook strings are passed to `sh -c`, so arguments, pipes, etc. work naturally.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct StartHooksConfig {
    /// Scripts to run on the host before CC starts. Cwd = working_dir.
    #[serde(default)]
    pub host: Vec<String>,
    /// Scripts to run inside the container before CC starts. Cwd = container_working_dir.
    /// Only valid for containerized apps.
    #[serde(default)]
    pub container: Vec<String>,
}

/// Hook scripts that run after a successful repo pull advances HEAD.
/// Same shape as `StartHooksConfig`. Scripts run via `sh -c`.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct PostPullHooksConfig {
    /// Scripts to run on the host after a successful repo pull.
    #[serde(default)]
    pub host: Vec<String>,
    /// Scripts to run inside the container after a successful repo pull.
    /// Only valid for containerized apps.
    #[serde(default)]
    pub container: Vec<String>,
}

/// Hook scripts that run once at server startup after all startup pulls succeed.
/// Same shape as `StartHooksConfig`. Scripts run via `sh -c`.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct StartupHooksConfig {
    /// Scripts to run on the host at server startup.
    #[serde(default)]
    pub host: Vec<String>,
    /// Scripts to run inside the container at server startup.
    /// Only valid for containerized apps.
    #[serde(default)]
    pub container: Vec<String>,
}
