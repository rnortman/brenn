/// Extract a panic message from a `catch_unwind` payload.
///
/// Accepts `String` (from `panic!("...", args)`) and `&'static str`
/// (from `panic!("literal")`) payloads.
pub(crate) fn unwrap_panic_msg(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = payload.downcast_ref::<&str>() {
        s.to_string()
    } else {
        panic!("panic payload was neither String nor &str");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic;

    #[test]
    fn string_payload() {
        let payload = panic::catch_unwind(|| panic!("fmt {}", 42)).unwrap_err();
        assert_eq!(unwrap_panic_msg(payload), "fmt 42");
    }

    #[test]
    fn static_str_payload() {
        let payload = panic::catch_unwind(|| panic!("bare literal")).unwrap_err();
        assert_eq!(unwrap_panic_msg(payload), "bare literal");
    }

    #[test]
    fn unknown_payload_panics() {
        let payload = panic::catch_unwind(|| std::panic::panic_any(42u64)).unwrap_err();
        let result = panic::catch_unwind(panic::AssertUnwindSafe(|| unwrap_panic_msg(payload)));
        assert!(result.is_err());
    }
}
