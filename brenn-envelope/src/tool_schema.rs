//! The shape a tool's `input_schema` must have.
//!
//! Every schema offered to an agent is read in two places: the host checks a
//! call's arguments against `properties` and `required`, and the MCP listing
//! copies the schema as-is into `inputSchema`, where MCP requires a root
//! `"type": "object"`. Both readers rely on the shape [`check_input_schema`]
//! enforces, so a schema that passes it can never make them guess. Lives here
//! because the config language (which refuses a provided tool's schema at
//! compile time) and the host (which holds every offered schema to it at boot)
//! both depend on this crate.

use serde_json::Value;

/// The head of every refusal of a provided tool's `input_schema` that
/// [`check_input_schema`] rejects, so the operator reads the same sentence
/// wherever the schema is refused. Callers append `: ` and the check's error.
pub const INPUT_SCHEMA_SHAPE_REFUSAL: &str =
    "`input_schema` is a JSON schema of the shape a tool's arguments are checked against";

/// Check that `schema` has the shape a tool's arguments are checked against.
///
/// The root is an object whose `"type"` is the string `"object"`;
/// `properties`, if present, is an object whose every value is an object;
/// `required`, if present, is an array of strings, each a key of
/// `properties`. Nothing else is constrained: `$schema`,
/// `additionalProperties`, descriptions, nested keywords and the type names
/// inside properties stay free.
///
/// The `Err` text names the offending key and what was expected.
pub fn check_input_schema(schema: &Value) -> Result<(), String> {
    let Some(root) = schema.as_object() else {
        return Err("the schema is not a JSON object".to_string());
    };
    if root.get("type").and_then(Value::as_str) != Some("object") {
        return Err("the root has no `\"type\": \"object\"`".to_string());
    }
    let properties = match root.get("properties") {
        None => None,
        Some(Value::Object(props)) => {
            if let Some((key, _)) = props.iter().find(|(_, v)| !v.is_object()) {
                return Err(format!("`properties.{key:?}` is not an object"));
            }
            Some(props)
        }
        Some(_) => return Err("`properties` is not an object".to_string()),
    };
    match root.get("required") {
        None => Ok(()),
        Some(Value::Array(names)) => {
            for name in names {
                let Some(name) = name.as_str() else {
                    return Err(format!("`required` holds {name}, which is not a string"));
                };
                if !properties.is_some_and(|p| p.contains_key(name)) {
                    return Err(format!(
                        "`required` names {name:?}, which is not a key of `properties`"
                    ));
                }
            }
            Ok(())
        }
        Some(_) => Err("`required` is not an array of strings".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::check_input_schema;
    use serde_json::json;

    fn refused(schema: serde_json::Value) -> String {
        check_input_schema(&schema).expect_err("schema should be refused")
    }

    #[test]
    fn the_bare_object_schema_is_accepted() {
        assert_eq!(check_input_schema(&json!({ "type": "object" })), Ok(()));
    }

    #[test]
    fn keywords_the_rule_does_not_constrain_are_accepted() {
        let schema = json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "description": "search",
            "additionalProperties": false,
            "properties": {
                "q": { "type": "string", "minLength": 1 },
                "n": { "type": "made-up-type" }
            },
            "required": ["q"]
        });
        assert_eq!(check_input_schema(&schema), Ok(()));
    }

    #[test]
    fn a_non_object_schema_is_refused() {
        assert_eq!(refused(json!([])), "the schema is not a JSON object");
    }

    #[test]
    fn a_root_without_type_is_refused() {
        assert_eq!(
            refused(json!({ "properties": {} })),
            "the root has no `\"type\": \"object\"`"
        );
    }

    #[test]
    fn a_root_whose_type_is_not_object_is_refused() {
        assert_eq!(
            refused(json!({ "type": "array" })),
            "the root has no `\"type\": \"object\"`"
        );
        assert_eq!(
            refused(json!({ "type": ["object"] })),
            "the root has no `\"type\": \"object\"`"
        );
    }

    #[test]
    fn properties_as_an_array_is_refused() {
        assert_eq!(
            refused(json!({ "type": "object", "properties": [] })),
            "`properties` is not an object"
        );
    }

    #[test]
    fn a_property_value_that_is_a_string_is_refused() {
        assert_eq!(
            refused(json!({ "type": "object", "properties": { "q": "string" } })),
            "`properties.\"q\"` is not an object"
        );
    }

    #[test]
    fn a_property_key_is_quoted_with_its_control_characters_escaped() {
        assert_eq!(
            refused(json!({ "type": "object", "properties": { "q\nx\u{1b}": "string" } })),
            "`properties.\"q\\nx\\u{1b}\"` is not an object"
        );
    }

    #[test]
    fn required_as_a_string_is_refused() {
        assert_eq!(
            refused(json!({
                "type": "object",
                "properties": { "q": { "type": "string" } },
                "required": "q"
            })),
            "`required` is not an array of strings"
        );
    }

    #[test]
    fn required_with_a_non_string_is_refused() {
        assert_eq!(
            refused(json!({
                "type": "object",
                "properties": { "q": { "type": "string" } },
                "required": ["q", 3]
            })),
            "`required` holds 3, which is not a string"
        );
    }

    #[test]
    fn required_naming_an_undeclared_key_is_refused() {
        assert_eq!(
            refused(json!({
                "type": "object",
                "properties": { "q": { "type": "string" } },
                "required": ["r"]
            })),
            "`required` names \"r\", which is not a key of `properties`"
        );
    }

    #[test]
    fn required_without_properties_is_refused() {
        assert_eq!(
            refused(json!({ "type": "object", "required": ["q"] })),
            "`required` names \"q\", which is not a key of `properties`"
        );
    }
}
