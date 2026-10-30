//! Argument validation for virtual tool calls.
//!
//! One validator for every entry point into tool execution. It checks the
//! subset of JSON Schema the virtual-tool catalog uses: the arguments are an
//! object, every `required` key is present, no key outside `properties`
//! appears, and each value's JSON type is one the property's `type` names.
//! It does not recurse into nested objects or arrays and ignores `enum`,
//! `format` and every other keyword.

use std::collections::HashSet;
use std::sync::{LazyLock, Mutex};

use serde_json::Value;

/// The JSON Schema primitive type names the validator recognizes.
const KNOWN_TYPE_NAMES: [&str; 7] = [
    "string", "integer", "number", "boolean", "object", "array", "null",
];

/// The JSON Schema type name of a value. An integral number is `integer`;
/// any other number is `number`.
fn json_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(n) if n.is_i64() || n.is_u64() => "integer",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// The `(tool, field, type keyword)` triples already warned about. A schema's
/// `type` keywords are fixed for the life of the descriptor, so one warning
/// per triple says everything a repeat would.
static WARNED_TYPES: LazyLock<Mutex<HashSet<(String, String, String)>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

/// Warn, once per process for each `(tool, field, type keyword)`, that a
/// property's `type` keyword is not one this validator models.
fn warn_unmodelled_type(tool: &str, field: &str, type_field: &Value, what: &str) {
    let key = (tool.to_string(), field.to_string(), type_field.to_string());
    let first = WARNED_TYPES
        .lock()
        .expect("unmodelled-type warning set poisoned")
        .insert(key);
    if first {
        tracing::warn!(tool, field, type_keyword = %type_field, "{what}");
    }
}

/// The type names a property's `type` keyword accepts, or `None` when the
/// keyword does not constrain the type: absent, an unrecognized name, a list
/// with no recognized names, or a shape that is neither string nor list.
/// Unrecognized names are skipped with a warning, so an out-of-tree schema
/// using a keyword this validator does not model is not refused.
fn accepted_types(
    tool: &str,
    type_field: Option<&Value>,
    field: &str,
) -> Option<Vec<&'static str>> {
    let known = |name: &str| KNOWN_TYPE_NAMES.iter().copied().find(|k| *k == name);
    let type_field = type_field?;
    match type_field {
        Value::String(name) => {
            let found = known(name);
            if found.is_none() {
                warn_unmodelled_type(
                    tool,
                    field,
                    type_field,
                    "unknown schema type name; type check skipped",
                );
            }
            found.map(|k| vec![k])
        }
        Value::Array(items) => {
            let accepted: Vec<&'static str> = items
                .iter()
                .filter_map(|item| item.as_str().and_then(known))
                .collect();
            if accepted.len() != items.len() {
                warn_unmodelled_type(
                    tool,
                    field,
                    type_field,
                    "unknown schema type element; element skipped",
                );
            }
            (!accepted.is_empty()).then_some(accepted)
        }
        _ => {
            warn_unmodelled_type(
                tool,
                field,
                type_field,
                "uninterpretable schema type; type check skipped",
            );
            None
        }
    }
}

/// Validate a call of `tool` against the tool's input schema.
///
/// `Err` carries a message naming the offending key and what was allowed,
/// written for the model that made the call. `tool` names the tool in the
/// warning for a `type` keyword the validator does not model.
///
/// The schema has the shape `brenn_envelope::tool_schema::check_input_schema`
/// admits — every schema offered to an agent is held to it before any call is
/// validated — so a present `properties` that is not an object, or a present
/// `required` that is not an array of strings, panics. An absent `properties`
/// allows no arguments; an absent `required` requires none.
pub fn validate_tool_input(tool: &str, schema: &Value, args: &Value) -> Result<(), String> {
    let Value::Object(args) = args else {
        return Err(format!(
            "arguments must be an object, got {}",
            json_type_name(args)
        ));
    };
    let empty = serde_json::Map::new();
    let properties = schema.get("properties").map_or(&empty, |props| {
        props.as_object().unwrap_or_else(|| {
            panic!("BUG: tool {tool}'s schema reached validation with a non-object `properties`")
        })
    });
    let mut allowed: Vec<&str> = properties.keys().map(String::as_str).collect();
    allowed.sort_unstable();

    let required: Vec<&str> = schema.get("required").map_or_else(Vec::new, |req| {
        req.as_array()
            .and_then(|names| names.iter().map(Value::as_str).collect::<Option<Vec<_>>>())
            .unwrap_or_else(|| {
                panic!(
                    "BUG: tool {tool}'s schema reached validation with a `required` that is not \
                     an array of strings"
                )
            })
    });
    let missing: Vec<&str> = required
        .into_iter()
        .filter(|k| !args.contains_key(*k))
        .collect();
    if !missing.is_empty() {
        return Err(format!(
            "missing required argument(s): {missing:?} (allowed: {allowed:?})"
        ));
    }

    let unknown: Vec<&str> = args
        .keys()
        .map(String::as_str)
        .filter(|k| !properties.contains_key(*k))
        .collect();
    if !unknown.is_empty() {
        return Err(format!(
            "unknown argument(s): {unknown:?} (allowed: {allowed:?}); \
             check the tool's inputSchema for the correct names"
        ));
    }

    for (key, value) in args {
        let Some(accepted) =
            accepted_types(tool, properties.get(key).and_then(|p| p.get("type")), key)
        else {
            continue;
        };
        let actual = json_type_name(value);
        // A JSON integer is also a valid `number`.
        let ok =
            accepted.contains(&actual) || (actual == "integer" && accepted.contains(&"number"));
        if !ok {
            return Err(format!(
                "argument '{key}' must be {}, got {actual}",
                accepted.join("|")
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn schema(props: Value, required: &[&str]) -> Value {
        json!({"type": "object", "properties": props, "required": required})
    }

    fn check(props: Value, args: Value) -> Result<(), String> {
        validate_tool_input("Tool", &schema(props, &[]), &args)
    }

    #[test]
    fn string_field_accepts_string() {
        assert_eq!(
            check(
                json!({"name": {"type": "string"}}),
                json!({"name": "alice"})
            ),
            Ok(())
        );
    }

    #[test]
    fn string_field_rejects_null() {
        let err = check(json!({"name": {"type": "string"}}), json!({"name": null})).unwrap_err();
        assert_eq!(err, "argument 'name' must be string, got null");
    }

    #[test]
    fn integer_field_rejects_bool() {
        let err = check(
            json!({"count": {"type": "integer"}}),
            json!({"count": true}),
        )
        .unwrap_err();
        assert!(err.contains("'count'"), "{err}");
    }

    #[test]
    fn integer_field_rejects_fraction() {
        let err = check(json!({"count": {"type": "integer"}}), json!({"count": 1.5})).unwrap_err();
        assert_eq!(err, "argument 'count' must be integer, got number");
    }

    #[test]
    fn number_accepts_integer() {
        assert_eq!(
            check(json!({"n": {"type": "number"}}), json!({"n": 3})),
            Ok(())
        );
        assert_eq!(
            check(json!({"n": {"type": "number"}}), json!({"n": 3.5})),
            Ok(())
        );
    }

    #[test]
    fn union_with_null() {
        let props = json!({"deliver_after": {"type": ["string", "null"]}});
        assert_eq!(
            check(
                props.clone(),
                json!({"deliver_after": "2026-01-01T00:00:00Z"})
            ),
            Ok(())
        );
        assert_eq!(check(props.clone(), json!({"deliver_after": null})), Ok(()));
        let err = check(props, json!({"deliver_after": 42})).unwrap_err();
        assert_eq!(
            err,
            "argument 'deliver_after' must be string|null, got integer"
        );
    }

    #[test]
    fn integer_number_union_rejects_bool() {
        let err = check(
            json!({"count": {"type": ["integer", "number"]}}),
            json!({"count": true}),
        )
        .unwrap_err();
        assert!(err.contains("'count'"), "{err}");
    }

    #[test]
    fn unknown_type_names_skip_the_check() {
        assert_eq!(
            check(json!({"m": {"type": {"weird": true}}}), json!({"m": "x"})),
            Ok(())
        );
        assert_eq!(
            check(json!({"m": {"type": "strng"}}), json!({"m": 1})),
            Ok(())
        );
        assert_eq!(
            check(json!({"m": {"type": ["strng", 7]}}), json!({"m": 1})),
            Ok(())
        );
        // A recognized element still constrains the list it sits in.
        assert!(check(json!({"m": {"type": ["strng", "string"]}}), json!({"m": 1})).is_err());
    }

    /// An unmodelled `type` keyword is warned about once per tool and field,
    /// naming the tool, however many calls reach it.
    #[tracing_test::traced_test]
    #[test]
    fn unmodelled_type_warns_once_naming_the_tool() {
        let schema = schema(json!({"m": {"type": "strng"}}), &[]);
        for _ in 0..3 {
            assert_eq!(
                validate_tool_input("WarnOnceTool", &schema, &json!({"m": 1})),
                Ok(())
            );
        }
        assert_eq!(
            validate_tool_input("OtherWarnOnceTool", &schema, &json!({"m": 1})),
            Ok(())
        );
        logs_assert(|lines: &[&str]| {
            let count = |tool: &str| {
                lines
                    .iter()
                    .filter(|l| l.contains(&format!("tool=\"{tool}\"")))
                    .filter(|l| l.contains("unknown schema type name"))
                    .count()
            };
            match (count("WarnOnceTool"), count("OtherWarnOnceTool")) {
                (1, 1) => Ok(()),
                other => Err(format!("expected one warning per tool, got {other:?}")),
            }
        });
    }

    #[test]
    fn property_without_type_is_unchecked() {
        assert_eq!(
            check(json!({"m": {"description": "any"}}), json!({"m": [1]})),
            Ok(())
        );
        assert_eq!(
            check(json!({"m": "not an object"}), json!({"m": [1]})),
            Ok(())
        );
    }

    #[test]
    fn enum_content_is_not_checked() {
        let props = json!({"wake": {"type": "string", "enum": ["morning", "evening"]}});
        assert_eq!(check(props, json!({"wake": "noon"})), Ok(()));
    }

    #[test]
    fn missing_required() {
        let err = validate_tool_input(
            "Tool",
            &schema(
                json!({"body": {"type": "string"}, "to": {"type": "string"}}),
                &["to"],
            ),
            &json!({"body": "x"}),
        )
        .unwrap_err();
        assert_eq!(
            err,
            r#"missing required argument(s): ["to"] (allowed: ["body", "to"])"#
        );
    }

    #[test]
    fn unknown_argument() {
        let err = check(json!({"body": {"type": "string"}}), json!({"bogus": "x"})).unwrap_err();
        assert!(
            err.starts_with(r#"unknown argument(s): ["bogus"] (allowed: ["body"])"#),
            "{err}"
        );
    }

    #[test]
    fn schema_without_properties_admits_only_empty_object() {
        let schema = json!({"type": "object"});
        assert_eq!(validate_tool_input("Tool", &schema, &json!({})), Ok(()));
        assert!(validate_tool_input("Tool", &schema, &json!({"a": 1})).is_err());
    }

    #[test]
    fn non_object_arguments() {
        for (args, ty) in [
            (json!(["a"]), "array"),
            (json!(null), "null"),
            (json!("s"), "string"),
            (json!(1), "integer"),
        ] {
            assert_eq!(
                validate_tool_input("Tool", &schema(json!({}), &[]), &args),
                Err(format!("arguments must be an object, got {ty}"))
            );
        }
    }

    /// Every `type` shape the catalog uses against every kind of JSON value:
    /// the validator answers, never panics, and accepts exactly the matching
    /// kinds.
    #[test]
    fn catalog_type_shapes_never_panic() {
        let shapes = [
            json!({"type": "string"}),
            json!({"type": "integer"}),
            json!({"type": "number"}),
            json!({"type": "boolean"}),
            json!({"type": "object"}),
            json!({"type": "array"}),
            json!({"type": "string", "enum": ["a", "b"]}),
            json!({"type": ["string", "null"]}),
            json!({"type": ["integer", "null"]}),
        ];
        let values = [
            json!("hello"),
            json!(0),
            json!(1),
            json!(-1),
            json!(u64::MAX),
            json!(2.5),
            json!(true),
            json!(false),
            json!(null),
            json!([]),
            json!({}),
            json!([1, "x"]),
            json!({"a": 1}),
        ];
        for shape in &shapes {
            for value in &values {
                let result = check(json!({"field": shape}), json!({"field": value}));
                let accepted = accepted_types("Tool", shape.get("type"), "field").unwrap();
                let actual = json_type_name(value);
                let expect_ok = accepted.contains(&actual)
                    || (actual == "integer" && accepted.contains(&"number"));
                assert_eq!(result.is_ok(), expect_ok, "shape={shape} value={value}");
            }
        }
    }

    #[test]
    #[should_panic(
        expected = "BUG: tool Tool's schema reached validation with a non-object \
                               `properties`"
    )]
    fn a_non_object_properties_panics() {
        validate_tool_input(
            "Tool",
            &json!({"type": "object", "properties": ["q"]}),
            &json!({}),
        )
        .unwrap_or_else(|err| panic!("refused instead of panicking: {err}"));
    }

    #[test]
    #[should_panic(
        expected = "BUG: tool Tool's schema reached validation with a `required` \
                               that is not an array of strings"
    )]
    fn a_non_array_required_panics() {
        validate_tool_input(
            "Tool",
            &json!({"type": "object", "properties": {"q": {}}, "required": "q"}),
            &json!({"q": 1}),
        )
        .unwrap_or_else(|err| panic!("refused instead of panicking: {err}"));
    }

    #[test]
    fn absent_properties_and_required_allow_nothing_and_require_nothing() {
        let schema = json!({"type": "object"});
        assert_eq!(validate_tool_input("Tool", &schema, &json!({})), Ok(()));
        let err = validate_tool_input("Tool", &schema, &json!({"q": 1})).unwrap_err();
        assert!(
            err.starts_with("unknown argument(s): [\"q\"] (allowed: [])"),
            "{err}"
        );
    }
}
