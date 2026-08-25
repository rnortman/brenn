//! What `catch_quietly` does to the panic hook, observed rather than asserted
//! off the internal flag.
//!
//! Two failures matter and neither is visible from inside the module's unit
//! tests: an install that never suppresses (the caught refusal prints the raw
//! `thread '…' panicked at …` line beside the rendered report, two competing
//! texts), and an install that suppresses everything (no panic output anywhere
//! in the process for the rest of the run — a red suite with no message and no
//! location). Both are answered by whether the *previous* hook ran, so this
//! test installs a recorder as that previous hook.
//!
//! Its own process, because the wrapper is installed once per process behind a
//! `OnceLock`: a recorder installed after some other test's first
//! `catch_quietly` would never be wrapped, and the assertions below would pass
//! or fail on test ordering.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};

use brenn_lib::panic_util::{catch_quietly, panic_message};

static HOOK_CALLS: AtomicUsize = AtomicUsize::new(0);

fn calls() -> usize {
    HOOK_CALLS.load(Ordering::SeqCst)
}

#[test]
fn the_real_hook_runs_outside_the_quiet_window_and_not_inside_it() {
    // Installed before any `catch_quietly` call in this process, so it is the
    // hook the delegating wrapper captures and delegates to. It counts and then
    // prints, because a recorder that only counts would also swallow this
    // test's own assertion failures.
    let printing = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        HOOK_CALLS.fetch_add(1, Ordering::SeqCst);
        printing(info);
    }));

    let payload = catch_quietly(|| panic!("a refusal")).unwrap_err();
    assert_eq!(panic_message(&*payload), Some("a refusal"));
    assert_eq!(
        calls(),
        0,
        "the hook printed inside the quiet window, so the caller's rendered \
         report is not the only text the operator sees",
    );

    let outside = catch_unwind(AssertUnwindSafe(|| panic!("an ordinary bug")));
    assert!(outside.is_err());
    assert_eq!(
        calls(),
        1,
        "the hook stayed silent after the window closed, so every later panic \
         in this process reports nothing",
    );
}
