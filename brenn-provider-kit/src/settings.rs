//! Reading an instance's `config` values through a lookup. Each component
//! names its own keys; a refusal names the key, since nothing a misconfigured
//! instance answered would be right.

/// The value of `key`, which must be present and non-empty.
pub fn required(get: &impl Fn(&str) -> Option<String>, key: &str) -> Result<String, String> {
    match get(key) {
        None => Err(format!("config {key} is required")),
        Some(value) if value.is_empty() => Err(format!("config {key} is empty")),
        Some(value) => Ok(value),
    }
}

/// The `host[:port]` at `key`: [`required`], and containing no `/`.
pub fn host(get: &impl Fn(&str) -> Option<String>, key: &str) -> Result<String, String> {
    let value = required(get, key)?;
    if value.contains('/') {
        return Err(format!(
            "config {key}: {value:?} is not a host[:port]; it contains `/`"
        ));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn lookup(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: BTreeMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |key| map.get(key).cloned()
    }

    #[test]
    fn a_required_key_is_present_and_not_empty() {
        let get = lookup(&[("provider", "google"), ("blank", "")]);
        assert_eq!(required(&get, "provider"), Ok("google".to_string()));
        assert_eq!(
            required(&get, "missing"),
            Err("config missing is required".to_string())
        );
        assert_eq!(
            required(&get, "blank"),
            Err("config blank is empty".to_string())
        );
    }

    #[test]
    fn a_host_carries_no_path() {
        let get = lookup(&[
            ("plain", "maps.example.com"),
            ("port", "127.0.0.1:8080"),
            ("pathed", "maps.example.com/v1"),
            ("blank", ""),
        ]);
        assert_eq!(host(&get, "plain"), Ok("maps.example.com".to_string()));
        assert_eq!(host(&get, "port"), Ok("127.0.0.1:8080".to_string()));
        assert_eq!(
            host(&get, "pathed"),
            Err(
                "config pathed: \"maps.example.com/v1\" is not a host[:port]; it contains `/`"
                    .to_string()
            )
        );
        assert_eq!(
            host(&get, "blank"),
            Err("config blank is empty".to_string())
        );
        assert_eq!(
            host(&get, "missing"),
            Err("config missing is required".to_string())
        );
    }
}
