/// Extract a panic message from a `catch_unwind` payload, or panic if it
/// carries no text.
pub(crate) fn unwrap_panic_msg(payload: Box<dyn std::any::Any + Send>) -> String {
    match crate::panic_util::panic_message(&*payload) {
        Some(message) => message.to_string(),
        None => panic!("panic payload was neither String nor &str"),
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
