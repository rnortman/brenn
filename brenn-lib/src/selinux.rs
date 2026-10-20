//! SELinux socket-creation labelling through procfs, the interface libselinux
//! wraps; no libselinux dependency.
//!
//! A listening socket is labelled at creation with the creating thread's
//! socket-creation context, or the process domain when none is set. Setting it
//! lets a server create a socket that a differently-confined client may
//! `connectto` without widening the server's own domain.

use std::io::{self, Write};
use std::os::unix::net::UnixListener;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

/// The per-thread SELinux socket-creation context, as procfs exposes it.
pub const SOCKCREATE_ATTR: &str = "/proc/thread-self/attr/sockcreate";

fn write_attr(bytes: &[u8]) -> io::Result<()> {
    std::fs::OpenOptions::new()
        .write(true)
        .open(SOCKCREATE_ATTR)?
        .write_all(bytes)
}

/// Clears the calling thread's socket-creation context when dropped.
struct ClearOnDrop;

impl Drop for ClearOnDrop {
    fn drop(&mut self) {
        // A lone newline is "no context" to the kernel.
        if let Err(e) = write_attr(b"\n") {
            panic!(
                "clearing the socket-creation context at {SOCKCREATE_ATTR} failed ({e}); \
                 this thread would label every socket it creates"
            );
        }
    }
}

/// Run `f` on the calling thread with its socket-creation context set to
/// `context`, and clear it again before returning, whether `f` returns or
/// panics. Sockets `f` creates carry `context` instead of the process domain.
/// `f` must create its sockets on this thread.
///
/// Errors: `context` is empty or holds a NUL or newline, which the kernel
/// would read as "no context" and accept, so the write is refused here
/// (`InvalidInput`); or the kernel refused the context — invalid for the
/// loaded policy, no SELinux on this host, or this domain lacks
/// `process { setsockcreate }`. The error names the path and the context.
/// Nothing was changed and `f` was not called.
///
/// Panics if the context cannot be cleared afterwards: a thread left with
/// the attribute set would label every socket the process later creates on
/// it, and tokio reuses worker threads.
pub fn with_sockcreate_context<T>(context: &str, f: impl FnOnce() -> T) -> io::Result<T> {
    if context.is_empty() || context.contains(['\0', '\n']) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "setting the socket-creation context failed ({context:?} is not a context: \
                 empty, or holds a NUL or newline, which {SOCKCREATE_ATTR} reads as \"no \
                 context\")"
            ),
        ));
    }
    let mut bytes = Vec::with_capacity(context.len() + 1);
    bytes.extend_from_slice(context.as_bytes());
    bytes.push(0);
    write_attr(&bytes).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!(
                "setting the socket-creation context failed (writing {context:?} to \
                 {SOCKCREATE_ATTR}: {e})"
            ),
        )
    })?;
    let guard = ClearOnDrop;
    let out = f();
    drop(guard);
    Ok(out)
}

/// Bind a listening unix socket at `path`, labelled `context` when one is
/// given, else with the process domain. Std, blocking, on the calling thread:
/// `socket()`, `bind()` and `listen()` all run inside the guard.
///
/// Errors: the guard's (the kernel refused the context), or the bind's —
/// which with a context includes the policy refusing this domain
/// `unix_stream_socket { create bind listen }` on a socket of that label.
/// The guard's error names the context and the attribute, the bind's the
/// path and any context; callers add no path of their own.
pub fn bind_listener(path: &Path, context: Option<&str>) -> io::Result<UnixListener> {
    let at = path.display();
    let Some(context) = context else {
        return UnixListener::bind(path).map_err(|e| {
            io::Error::new(e.kind(), format!("binding a listener at {at} failed ({e})"))
        });
    };
    with_sockcreate_context(context, || UnixListener::bind(path))?.map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("binding a listener at {at} under context {context:?} failed ({e})"),
        )
    })
}

/// Distinguishes probe directories of concurrent probes in one process.
static PROBE_SEQ: AtomicU64 = AtomicU64::new(0);

/// Prove this process can bind a listener labelled `context`: bind one on a
/// throwaway path in a fresh private directory under the temp dir, through
/// `bind_listener`, and remove both. Its error is `bind_listener`'s.
///
/// Panics if the directory cannot be created or removed: the probe runs on the
/// boot thread, and an unusable temp dir is its own boot failure.
pub fn probe_sockcreate(context: &str) -> io::Result<()> {
    let dir = std::env::temp_dir().join(format!(
        "brenn-sockcreate-probe-{}-{}",
        std::process::id(),
        PROBE_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    probe_sockcreate_in(&dir, context)
}

/// `probe_sockcreate` with the throwaway directory at `dir`.
fn probe_sockcreate_in(dir: &Path, context: &str) -> io::Result<()> {
    if let Err(e) = crate::runtime_dir::recreate_private_dir(dir) {
        panic!("socket-creation probe: {e}");
    }
    // The listener, if bound, closes here, before its directory goes.
    let result = bind_listener(&dir.join("probe.sock"), Some(context)).map(drop);
    if let Err(e) = std::fs::remove_dir_all(dir) {
        panic!("socket-creation probe: removing {}: {e}", dir.display());
    }
    result
}

/// A context string as procfs or an xattr returns it, without the trailing
/// NUL or newline.
#[cfg(any(test, feature = "testutils"))]
fn trim_context(bytes: &[u8]) -> String {
    String::from_utf8(bytes.to_vec())
        .expect("an SELinux context is UTF-8")
        .trim_end_matches(['\0', '\n'])
        .to_string()
}

/// For tests that need a context the kernel accepts: the calling process's
/// own context with its level replaced by `s0`, which its range dominates and
/// which differs from an unconfined session's `s0-s0:c0.c1023`. `None`, with a
/// line on stderr naming `test`, where `/sys/fs/selinux/enforce` is not
/// readable (no SELinux).
///
/// TODO(test-selinux-positive-paths-silently-skip): no CI lane has SELinux, so
/// the tests this gates are reported as passed there without running.
#[cfg(any(test, feature = "testutils"))]
pub fn test_context(test: &str) -> Option<String> {
    if std::fs::read("/sys/fs/selinux/enforce").is_err() {
        eprintln!("{test}: skipped, /sys/fs/selinux/enforce is not readable (no SELinux)");
        return None;
    }
    let current = trim_context(
        &std::fs::read("/proc/thread-self/attr/current").expect("reading the process context"),
    );
    let fields: Vec<&str> = current.splitn(4, ':').collect();
    assert_eq!(fields.len(), 4, "{current}");
    Some(format!("{}:{}:{}:s0", fields[0], fields[1], fields[2]))
}

/// For tests: the SELinux label of the socket `fd`, read from its
/// `security.selinux` attribute.
#[cfg(any(test, feature = "testutils"))]
pub fn socket_label(fd: std::os::fd::RawFd) -> String {
    let name = c"security.selinux";
    let mut buf = vec![0u8; 512];
    // SAFETY: `fd` is open for the call, `name` is NUL-terminated and
    // `buf` is valid for `buf.len()` bytes.
    let n = unsafe { libc::fgetxattr(fd, name.as_ptr(), buf.as_mut_ptr().cast(), buf.len()) };
    assert!(n >= 0, "fgetxattr: {}", io::Error::last_os_error());
    buf.truncate(n as usize);
    trim_context(&buf)
}

#[cfg(test)]
mod tests {
    use std::os::fd::AsRawFd;
    use std::os::unix::net::{UnixDatagram, UnixStream};

    use super::*;

    /// The calling thread's socket-creation context, or `None` where the
    /// attribute refuses reads (no LSM serving it).
    fn read_sockcreate() -> Option<String> {
        match std::fs::read(SOCKCREATE_ATTR) {
            Ok(bytes) => Some(trim_context(&bytes)),
            Err(_) => None,
        }
    }

    #[test]
    fn an_invalid_context_is_refused_and_names_the_attribute() {
        let mut called = false;
        let err = with_sockcreate_context("garbage", || called = true).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains(SOCKCREATE_ATTR), "{msg}");
        assert!(msg.contains("garbage"), "{msg}");
        assert!(msg.contains("setting the socket-creation context"), "{msg}");
        assert!(!called);
        if let Some(after) = read_sockcreate() {
            assert_eq!(after, "");
        }
    }

    /// The kernel accepts these as "clear the context", so a configured
    /// value of this shape would bind an unlabelled socket; refused on every
    /// host, before anything is written.
    #[test]
    fn a_context_the_kernel_reads_as_none_is_refused() {
        for context in ["", "\n", "\0", "\nsystem_u:system_r:container_t:s0", "a\0b"] {
            let mut called = false;
            let err = with_sockcreate_context(context, || called = true).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "{context:?}");
            let msg = err.to_string();
            assert!(msg.contains("setting the socket-creation context"), "{msg}");
            assert!(msg.contains(&format!("{context:?}")), "{msg}");
            assert!(!called);
        }
        let err = probe_sockcreate("").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "{err}");
    }

    #[test]
    fn a_probe_of_an_invalid_context_names_the_setting_step() {
        let err = probe_sockcreate("garbage").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("setting the socket-creation context"), "{msg}");
        assert!(msg.contains("garbage"), "{msg}");
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("probe");
        probe_sockcreate_in(&dir, "garbage").unwrap_err();
        assert!(!dir.exists());
    }

    #[test]
    fn without_a_context_the_listener_binds_unlabelled() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.sock");
        let _listener = bind_listener(&path, None).unwrap();
        UnixStream::connect(&path).unwrap();
        let err = bind_listener(&path, None).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AddrInUse);
        let msg = err.to_string();
        assert_eq!(msg.matches("s.sock").count(), 1, "{msg}");
    }

    #[test]
    fn the_context_applies_to_the_calling_thread_only_while_set() {
        let Some(ctx) = test_context("the_context_applies_to_the_calling_thread") else {
            return;
        };
        let seen = with_sockcreate_context(&ctx, read_sockcreate).unwrap();
        assert_eq!(seen.as_deref(), Some(ctx.as_str()));
        assert_eq!(read_sockcreate().as_deref(), Some(""));
    }

    #[test]
    fn a_panic_inside_clears_the_context() {
        let Some(ctx) = test_context("a_panic_inside_clears_the_context") else {
            return;
        };
        let unwound = std::panic::catch_unwind(|| {
            with_sockcreate_context(&ctx, || panic!("inside")).unwrap();
        });
        assert!(unwound.is_err());
        assert_eq!(read_sockcreate().as_deref(), Some(""));
    }

    #[test]
    fn a_labelled_listener_carries_the_context_and_accepts() {
        let Some(ctx) = test_context("a_labelled_listener_carries_the_context") else {
            return;
        };
        let own = trim_context(&std::fs::read("/proc/thread-self/attr/current").unwrap());
        assert_ne!(
            own, ctx,
            "the derived context must differ from the process's own"
        );
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("l.sock");
        let listener = bind_listener(&path, Some(&ctx)).unwrap();
        assert_eq!(socket_label(listener.as_raw_fd()), ctx);
        assert_eq!(read_sockcreate().as_deref(), Some(""));
        UnixStream::connect(&path).unwrap();
        let after = UnixDatagram::unbound().unwrap();
        assert_eq!(socket_label(after.as_raw_fd()), own);
    }

    #[test]
    fn a_probe_of_a_valid_context_succeeds_and_leaves_nothing() {
        let Some(ctx) = test_context("a_probe_of_a_valid_context") else {
            return;
        };
        probe_sockcreate(&ctx).unwrap();
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("probe");
        probe_sockcreate_in(&dir, &ctx).unwrap();
        assert!(!dir.exists());
    }
}
