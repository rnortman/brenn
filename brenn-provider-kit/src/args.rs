//! Reading a tool's JSON arguments. Every refusal is the request's
//! `invalid_args` detail and names the argument.

use serde_json::{Map, Value};

/// A tool's arguments, once known to be an object.
pub type Args = Map<String, Value>;

/// `args` as an object whose every key is in `allowed`.
pub fn object<'a>(args: &'a Value, allowed: &[&str]) -> Result<&'a Args, String> {
    let Value::Object(map) = args else {
        return Err("the arguments must be a JSON object".to_string());
    };
    if let Some(unknown) = map.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(format!(
            "unknown argument `{unknown}`; this tool takes {}",
            allowed
                .iter()
                .map(|key| format!("`{key}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    Ok(map)
}

/// A string argument of at most `max_chars` characters (Unicode scalar
/// values, as JSON Schema's `maxLength` counts them) that is not empty or
/// whitespace only, checked before anything else is done with it.
pub fn capped<'a>(args: &'a Args, key: &str, max_chars: usize) -> Result<Option<&'a str>, String> {
    match args.get(key) {
        None => Ok(None),
        Some(Value::String(value)) => {
            if value.chars().count() > max_chars {
                return Err(format!("`{key}` is longer than {max_chars} characters"));
            }
            if value.trim().is_empty() {
                return Err(format!("`{key}` is empty"));
            }
            Ok(Some(value))
        }
        Some(_) => Err(format!("`{key}` must be a string")),
    }
}

/// An integer argument in `min..=max`. A number written with a zero
/// fraction or an exponent (`5.0`, `1e1`) is an integer, as JSON Schema's
/// `integer` type counts it.
pub fn integer(args: &Args, key: &str, min: u64, max: u64) -> Result<Option<u64>, String> {
    match args.get(key) {
        None => Ok(None),
        Some(value) => match value.as_u64().or_else(|| value.as_f64().and_then(integral)) {
            Some(n) if (min..=max).contains(&n) => Ok(Some(n)),
            _ => Err(format!("`{key}` must be an integer from {min} to {max}")),
        },
    }
}

/// `n` as a `u64` when it is a whole number in `u64`'s range, which every
/// such `f64` represents exactly. `-0.0` is `0`.
fn integral(n: f64) -> Option<u64> {
    // 2^64, the first whole number past `u64::MAX`.
    const PAST_U64: f64 = 18_446_744_073_709_551_616.0;
    (n.fract() == 0.0 && (0.0..PAST_U64).contains(&n)).then_some(n as u64)
}

/// The argument `key` as the one of `values` that `name` spells as given;
/// absent is `None`. The refusal is "`key` must be one of `a`, `b`", the
/// spellings in `values`' order.
pub fn choice<T: Copy>(
    args: &Args,
    key: &str,
    values: &[T],
    name: impl Fn(T) -> &'static str,
) -> Result<Option<T>, String> {
    let refusal = || {
        let names: Vec<String> = values
            .iter()
            .map(|value| format!("`{}`", name(*value)))
            .collect();
        format!("`{key}` must be one of {}", names.join(", "))
    };
    match args.get(key) {
        None => Ok(None),
        Some(Value::String(given)) => values
            .iter()
            .find(|value| name(**value) == given)
            .map(|value| Some(*value))
            .ok_or_else(refusal),
        Some(_) => Err(refusal()),
    }
}

/// A boolean argument; absent is `false`.
pub fn flag(args: &Args, key: &str) -> Result<bool, String> {
    match args.get(key) {
        None => Ok(false),
        Some(Value::Bool(value)) => Ok(*value),
        Some(_) => Err(format!("`{key}` must be true or false")),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn args(value: Value) -> Args {
        match value {
            Value::Object(map) => map,
            other => panic!("not an object: {other}"),
        }
    }

    #[test]
    fn an_object_names_an_unknown_key_and_what_the_tool_takes() {
        assert_eq!(
            object(&json!([]), &["q"]),
            Err("the arguments must be a JSON object".to_string())
        );
        assert_eq!(
            object(&json!({ "q": "x", "z": 1 }), &["q", "max"]),
            Err("unknown argument `z`; this tool takes `q`, `max`".to_string())
        );
        let value = json!({ "q": "x" });
        assert_eq!(object(&value, &["q"]), Ok(&args(value.clone())));
    }

    #[test]
    fn capped_counts_characters_not_bytes() {
        // Four characters of two, three and four UTF-8 bytes each.
        let wide = "é€😀é".repeat(4);
        assert_eq!(wide.chars().count(), 16);
        assert!(wide.len() > 16);
        let at_cap = args(json!({ "q": wide }));
        assert_eq!(capped(&at_cap, "q", 16), Ok(Some(wide.as_str())));
        let over = args(json!({ "q": format!("{wide}é") }));
        assert_eq!(
            capped(&over, "q", 16),
            Err("`q` is longer than 16 characters".to_string())
        );
    }

    #[test]
    fn capped_refuses_a_blank_or_a_non_string_and_admits_an_absent_value() {
        for blank in ["", " ", "\t\n", "\u{85}", "\u{3000}"] {
            assert_eq!(
                capped(&args(json!({ "q": blank })), "q", 16),
                Err("`q` is empty".to_string()),
                "{blank:?}"
            );
        }
        assert_eq!(
            capped(&args(json!({ "q": 7 })), "q", 16),
            Err("`q` must be a string".to_string())
        );
        assert_eq!(capped(&args(json!({})), "q", 16), Ok(None));
    }

    #[test]
    fn integer_admits_its_range_only() {
        let range = |value: Value| integer(&args(json!({ "max": value })), "max", 1, 5);
        assert_eq!(range(json!(1)), Ok(Some(1)));
        assert_eq!(range(json!(5)), Ok(Some(5)));
        for (whole, n) in [("2.0", 2), ("1e0", 1), ("5.000", 5), ("0.5e1", 5)] {
            let value: Value = serde_json::from_str(whole).unwrap();
            assert!(value.is_f64(), "{whole}");
            assert_eq!(range(value), Ok(Some(n)), "{whole}");
        }
        assert_eq!(integer(&args(json!({ "n": -0.0 })), "n", 0, 5), Ok(Some(0)));
        for refused in [
            json!(0),
            json!(6),
            json!(-1),
            json!(2.5),
            json!(6.0),
            json!(-1.0),
            json!(1e20),
            json!(f64::MAX),
            json!("3"),
        ] {
            assert_eq!(
                range(refused.clone()),
                Err("`max` must be an integer from 1 to 5".to_string()),
                "{refused}"
            );
        }
        assert_eq!(integer(&args(json!({})), "max", 1, 5), Ok(None));
        // A whole float past `u64::MAX` is refused, not saturated to it.
        assert_eq!(
            integer(&args(json!({ "n": 2f64.powi(64) })), "n", 0, u64::MAX),
            Err(format!("`n` must be an integer from 0 to {}", u64::MAX))
        );
    }

    #[test]
    fn a_choice_is_one_of_its_values_and_absent_is_none() {
        const VALUES: [&str; 3] = ["drive", "walk", "transit"];
        let spelled = |value: &'static str| value;
        for value in VALUES {
            assert_eq!(
                choice(&args(json!({ "mode": value })), "mode", &VALUES, spelled),
                Ok(Some(value))
            );
        }
        assert_eq!(choice(&args(json!({})), "mode", &VALUES, spelled), Ok(None));
        for refused in [
            json!("fly"),
            json!("Drive"),
            json!(""),
            json!(1),
            json!(null),
        ] {
            assert_eq!(
                choice(&args(json!({ "mode": refused })), "mode", &VALUES, spelled),
                Err("`mode` must be one of `drive`, `walk`, `transit`".to_string()),
                "{refused}"
            );
        }
    }

    /// The value answered is the one whose spelling matched, not the
    /// spelling itself.
    #[test]
    fn a_choice_answers_the_value_its_name_spells() {
        let numbered = |n: u8| ["one", "two"][usize::from(n)];
        assert_eq!(
            choice(&args(json!({ "n": "two" })), "n", &[0u8, 1], numbered),
            Ok(Some(1))
        );
        assert_eq!(
            choice(&args(json!({ "n": "three" })), "n", &[0u8, 1], numbered),
            Err("`n` must be one of `one`, `two`".to_string())
        );
    }

    #[test]
    fn a_flag_is_a_boolean_and_absent_is_false() {
        assert_eq!(flag(&args(json!({ "all": true })), "all"), Ok(true));
        assert_eq!(flag(&args(json!({ "all": false })), "all"), Ok(false));
        assert_eq!(flag(&args(json!({})), "all"), Ok(false));
        assert_eq!(
            flag(&args(json!({ "all": "yes" })), "all"),
            Err("`all` must be true or false".to_string())
        );
    }
}
