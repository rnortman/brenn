use std::path::PathBuf;

/// All hard errors that abort the run.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error reading {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("config parse error: {0}")]
    Config(String),

    #[error("invalid date: {0}")]
    Date(String),

    #[error("invalid duration: {0}")]
    Duration(String),

    #[error("project root not found: {0}")]
    MissingRoot(PathBuf),

    #[error("unknown session ID: {0}")]
    UnknownSession(String),

    #[error("no sessions found in any configured project root")]
    NoSessions,
}

pub type Result<T> = std::result::Result<T, Error>;
