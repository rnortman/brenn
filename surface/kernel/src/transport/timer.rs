//! Per-target timer shim: a single `sleep(Duration)` future the driver arms for
//! backoff, the handshake timeout, and the liveness deadline.
//!
//! Confined to the transport layer so the core and driver carry no `cfg` logic.
//! No `gloo-timers` dependency — the shim is trivial and dependencies are
//! deliberate choices.

use std::time::Duration;

/// Native sleep: delegates to the tokio timer wheel.
#[cfg(not(target_arch = "wasm32"))]
pub async fn sleep(duration: Duration) {
    tokio::time::sleep(duration).await;
}

/// Browser sleep: a `setTimeout` bridged to a future. The callback fires a
/// oneshot the future awaits; a [`TimeoutGuard`] calls `clearTimeout` if the
/// future is dropped before the timer fires, so re-arming the driver's wakeup
/// every loop pass does not orphan a `setTimeout` (and its retained closure) for
/// the full delay each time.
#[cfg(target_arch = "wasm32")]
pub async fn sleep(duration: Duration) {
    use wasm_bindgen::JsCast;
    use wasm_bindgen::closure::Closure;

    let millis = duration.as_millis().min(i32::MAX as u128) as i32;
    let window = web_sys::window().expect("surface kernel requires a Window global");
    let (tx, rx) = futures_channel::oneshot::channel::<()>();
    let callback = Closure::once(move || {
        let _ = tx.send(());
    });
    let handle = window
        .set_timeout_with_callback_and_timeout_and_arguments_0(
            callback.as_ref().unchecked_ref(),
            millis,
        )
        .expect("setTimeout is available in the browser");
    let _guard = TimeoutGuard {
        window: window.clone(),
        handle,
        _callback: callback,
    };
    // A dropped sender (only on teardown) resolves this immediately; either way
    // the guard then cancels the timer.
    let _ = rx.await;
}

/// Cancels a pending `setTimeout` on drop and holds its callback alive until then.
#[cfg(target_arch = "wasm32")]
struct TimeoutGuard {
    window: web_sys::Window,
    handle: i32,
    _callback: wasm_bindgen::closure::Closure<dyn FnMut()>,
}

#[cfg(target_arch = "wasm32")]
impl Drop for TimeoutGuard {
    fn drop(&mut self) {
        // Harmless no-op if the timer already fired.
        self.window.clear_timeout_with_handle(self.handle);
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sleep_resolves_after_the_requested_delay() {
        let start = tokio::time::Instant::now();
        sleep(Duration::from_millis(10)).await;
        assert!(start.elapsed() >= Duration::from_millis(10));
    }
}
