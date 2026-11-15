//! Work handed from async code to tokio's blocking pool.

use tokio::task::JoinError;

/// Run `f` on the blocking pool; a panic in it is resumed here.
pub(crate) async fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    joined(tokio::task::spawn_blocking(f).await)
}

/// A blocking task's value, its panic resumed here. Nothing in this crate
/// aborts a blocking task, so one that did not complete is a bug.
pub(crate) fn joined<T>(result: Result<T, JoinError>) -> T {
    match result {
        Ok(value) => value,
        Err(e) => match e.try_into_panic() {
            Ok(panic) => std::panic::resume_unwind(panic),
            Err(e) => panic!("a blocking-pool task did not complete: {e}"),
        },
    }
}
