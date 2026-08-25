//! Catching a deliberate panic and rendering its message.
//!
//! A panic is normally a death. In a few places it is a *verdict* instead: the
//! asserts are the single source of the refusal text, and a tool that reports
//! rather than dies catches the unwind and prints what the assert said. Those
//! places need two things, and both are here so that there is one answer to
//! each: suppress the default hook's own `thread '…' panicked at …` line so the
//! output does not carry two competing texts, and pull the message out of the
//! payload.

use std::any::Any;
use std::cell::Cell;
use std::panic::{PanicHookInfo, UnwindSafe};
use std::sync::OnceLock;

thread_local! {
    /// Set only for the duration of a [`catch_quietly`] call on this thread.
    static SUPPRESS_HOOK: Cell<bool> = const { Cell::new(false) };
}

static HOOK_INSTALLED: OnceLock<()> = OnceLock::new();

/// Install the delegating hook once, for the whole process.
///
/// Taking the hook and putting it back around each `catch_unwind` is the
/// obvious shape and it is wrong: the hook is process-global, the test harness
/// runs threads in parallel, and two overlapping swaps lose the real hook for
/// the rest of the process — every later panic anywhere then prints nothing.
/// So the real hook is captured once and wrapped, and the suppression is a
/// thread-local flag: nothing is ever taken away from another thread.
fn install_hook() {
    HOOK_INSTALLED.get_or_init(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info: &PanicHookInfo<'_>| {
            if SUPPRESS_HOOK.with(Cell::get) {
                return;
            }
            previous(info);
        }));
    });
}

/// Run `f`, catching a panic without letting the default hook print it.
///
/// Only this thread goes quiet, and only for this call: a panic on any other
/// thread reports normally throughout. The flag is restored rather than
/// cleared, so an inner call returning does not un-quiet an outer one still
/// running.
///
/// The caller decides what the payload means — [`panic_message`] renders the
/// ones that carry text, and a payload that carries none is the caller's to
/// re-panic on.
pub fn catch_quietly<R, F: FnOnce() -> R + UnwindSafe>(f: F) -> Result<R, Box<dyn Any + Send>> {
    install_hook();
    let previously_suppressed = SUPPRESS_HOOK.with(|s| s.replace(true));
    let outcome = std::panic::catch_unwind(f);
    SUPPRESS_HOOK.with(|s| s.set(previously_suppressed));
    outcome
}

/// The text of a panic payload, if it carries any.
///
/// `String` is what a formatted `panic!("…{x}")` produces and `&'static str`
/// what a bare literal produces; those two are every panic that says something.
/// `None` means the payload came from `panic_any` with some other type, which no
/// assert produces.
pub fn panic_message(payload: &(dyn Any + Send)) -> Option<&str> {
    if let Some(message) = payload.downcast_ref::<String>() {
        return Some(message.as_str());
    }
    payload.downcast_ref::<&'static str>().copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_formatted_panic_renders_its_message() {
        let payload = catch_quietly(|| panic!("fmt {}", 42)).unwrap_err();
        assert_eq!(panic_message(&*payload), Some("fmt 42"));
    }

    #[test]
    fn a_literal_panic_renders_its_message() {
        let payload = catch_quietly(|| panic!("bare literal")).unwrap_err();
        assert_eq!(panic_message(&*payload), Some("bare literal"));
    }

    #[test]
    fn a_payload_carrying_no_text_renders_nothing() {
        let payload = catch_quietly(|| std::panic::panic_any(42u64)).unwrap_err();
        assert_eq!(panic_message(&*payload), None);
    }

    #[test]
    fn a_value_passes_through_when_nothing_panics() {
        assert_eq!(catch_quietly(|| 7).unwrap(), 7);
    }

    /// Nesting must not un-quiet an outer window: an inner call restores what
    /// it found rather than clearing, so the outer call's remaining statements
    /// panic as quietly as its first ones did.
    #[test]
    fn a_nested_call_leaves_the_outer_window_suppressed() {
        let still_suppressed = catch_quietly(|| {
            let _ = catch_quietly(|| panic!("inner"));
            SUPPRESS_HOOK.with(Cell::get)
        });
        assert!(still_suppressed.unwrap());
    }

    /// The suppression is this thread's and this call's. Were it the process's,
    /// a panic anywhere else during the window would print nothing — which is a
    /// red test run with no message and no location.
    #[test]
    fn suppression_ends_with_the_call() {
        let _ = catch_quietly(|| panic!("quiet"));
        assert!(!SUPPRESS_HOOK.with(Cell::get));
    }
}
