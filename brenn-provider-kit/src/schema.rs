//! Tying a tool's `input_schema`, which the model is shown, to the bounds its
//! argument parser enforces. The host enforces none of the schema's bounds, so
//! a schema that drifts from its parser misleads the model until the parser
//! refuses; each component's host test reads its spec with [`input_schema`]
//! and checks every tool with [`check`] against a [`ToolBounds`] built from
//! the constants its parsers use.

use serde_json::{Map, Value, json};

/// Tool `name`'s `input_schema` in a `.brenn` spec's text: the one-line JSON
/// of the `input_schema = """…""";` statement inside `provide <name> { … }`.
/// Panics naming the tool when the spec declares no such block or statement,
/// declares the block twice, or the statement is not JSON.
pub fn input_schema(spec: &str, name: &str) -> Value {
    let opening = format!("provide {name} {{");
    let lines: Vec<&str> = spec.lines().map(str::trim).collect();
    let starts: Vec<usize> = (0..lines.len()).filter(|&i| lines[i] == opening).collect();
    let start = match starts.as_slice() {
        [start] => *start,
        [] => panic!("the spec has no `provide {name}` block"),
        _ => panic!("the spec declares `provide {name}` {} times", starts.len()),
    };
    let text = lines[start + 1..]
        .iter()
        .take_while(|line| **line != "}")
        .find_map(|line| line.strip_prefix("input_schema = \"\"\""))
        .unwrap_or_else(|| panic!("the spec's `provide {name}` block has no input_schema"));
    let text = text
        .strip_suffix("\"\"\";")
        .unwrap_or_else(|| panic!("`provide {name}`'s input_schema is not on one line"));
    serde_json::from_str(text)
        .unwrap_or_else(|error| panic!("`provide {name}`'s input_schema is not JSON: {error}"))
}

/// The `pattern` that says what [`crate::capped`] refuses as blank: a value
/// matches only if it holds a character that is whitespace to neither Rust
/// nor ECMA-262. `str::trim` uses Unicode `White_Space`, ECMA's `\s` is its
/// WhiteSpace and LineTerminator; the two differ in U+0085 (NEL), whitespace
/// only to Rust, and U+FEFF, whitespace only to ECMA. A plain `\S` would admit
/// a NEL-only value the parser refuses, so U+0085 is excluded by name. U+FEFF
/// is left: the schema refuses a value that is only U+FEFF which the parser
/// admits, the one difference left, and it never promises what the parser
/// refuses.
pub(crate) const NON_BLANK: &str = r"[^\s\u0085]";

/// The `pattern` of an ASCII token id of 1 to `max_chars` characters.
pub(crate) fn token_pattern(max_chars: usize) -> String {
    format!("^[A-Za-z0-9_-]{{1,{max_chars}}}$")
}

/// What a parser enforces on one argument, and so exactly the keywords its
/// property's schema states.
#[derive(Clone, Debug, PartialEq)]
pub enum Bound<'a> {
    /// `{type: string, minLength: 1, maxLength: N, pattern: NON_BLANK[, format]}`.
    Text {
        max_chars: usize,
        format: Option<&'a str>,
    },
    /// `{type: string, pattern: token_pattern(max_chars)}`: an ASCII token id.
    Token { max_chars: usize },
    /// `{type: string, pattern: P}`, for a pattern that is its component's.
    Pattern(String),
    /// `{type: integer, minimum, maximum}`. The parser admits a whole number
    /// written as a float (`5.0`), as the schema's `integer` does.
    Integer { min: u64, max: u64 },
    /// `{type: number, minimum, maximum}`.
    Number { min: f64, max: f64 },
    /// `{type: string, enum: [..]}`, in order.
    Enum(Vec<&'a str>),
    /// `{type: boolean}`.
    Bool,
    /// `{type: array, minItems, maxItems[, uniqueItems: true], items}`.
    Array {
        min: usize,
        max: usize,
        unique: bool,
        item: Box<Bound<'a>>,
    },
}

impl Bound<'_> {
    /// The property schema this bound is stated as.
    fn expected(&self) -> Map<String, Value> {
        let value = match self {
            Bound::Text { max_chars, format } => {
                let mut text = json!({ "type": "string", "minLength": 1,
                    "maxLength": max_chars, "pattern": NON_BLANK });
                if let Some(format) = format {
                    text["format"] = json!(format);
                }
                text
            }
            Bound::Token { max_chars } => {
                json!({ "type": "string", "pattern": token_pattern(*max_chars) })
            }
            Bound::Pattern(pattern) => json!({ "type": "string", "pattern": pattern }),
            Bound::Integer { min, max } => {
                json!({ "type": "integer", "minimum": min, "maximum": max })
            }
            Bound::Number { min, max } => {
                json!({ "type": "number", "minimum": min, "maximum": max })
            }
            Bound::Enum(values) => json!({ "type": "string", "enum": values }),
            Bound::Bool => json!({ "type": "boolean" }),
            Bound::Array {
                min,
                max,
                unique,
                item,
            } => {
                let mut array = json!({ "type": "array", "minItems": min, "maxItems": max,
                    "items": Value::Object(item.expected()) });
                if *unique {
                    array["uniqueItems"] = json!(true);
                }
                array
            }
        };
        match value {
            Value::Object(map) => map,
            _ => unreachable!("every bound is stated as an object"),
        }
    }
}

/// Every argument a tool's parser admits, with its bound, and the arguments
/// it requires.
#[derive(Clone, Debug, PartialEq)]
pub struct ToolBounds<'a> {
    pub properties: Vec<(&'a str, Bound<'a>)>,
    pub required: &'a [&'a str],
}

/// `schema` states exactly `bounds`: `type: object`, `additionalProperties:
/// false`, the property set and `required` as sets, and per property the
/// keywords the bound names, with no other keyword present. Every difference
/// found is in the error, one per `; `.
pub fn check(schema: &Value, bounds: &ToolBounds) -> Result<(), String> {
    let mut found = Vec::new();
    let Some(schema) = schema.as_object() else {
        return Err("the schema is not an object".to_string());
    };
    for key in schema.keys() {
        if !["type", "properties", "required", "additionalProperties"].contains(&key.as_str()) {
            found.push(format!(
                "the schema states `{key}`, which no parser bound names"
            ));
        }
    }
    if schema.get("type") != Some(&json!("object")) {
        found.push("the schema's `type` is not `object`".to_string());
    }
    if schema.get("additionalProperties") != Some(&json!(false)) {
        found.push("the schema's `additionalProperties` is not `false`".to_string());
    }
    let empty = Map::new();
    let properties = match schema.get("properties") {
        Some(Value::Object(properties)) => properties,
        _ => {
            found.push("the schema's `properties` is not an object".to_string());
            &empty
        }
    };
    for name in properties.keys() {
        if !bounds.properties.iter().any(|(bound, _)| bound == name) {
            found.push(format!(
                "the schema states `{name}`, which the parser does not admit"
            ));
        }
    }
    for (name, bound) in &bounds.properties {
        match properties.get(*name) {
            Some(Value::Object(property)) => compare(name, &bound.expected(), property, &mut found),
            Some(_) => found.push(format!("`{name}`'s schema is not an object")),
            None => found.push(format!("the schema does not state `{name}`")),
        }
    }
    let required: Vec<&str> = match schema.get("required") {
        None => Vec::new(),
        Some(Value::Array(names)) => names
            .iter()
            .filter_map(|name| {
                if name.is_string() {
                    name.as_str()
                } else {
                    found.push(format!("the schema's `required` holds {name}, not a name"));
                    None
                }
            })
            .collect(),
        Some(_) => {
            found.push("the schema's `required` is not an array".to_string());
            Vec::new()
        }
    };
    for (at, name) in required.iter().enumerate() {
        if !bounds.required.contains(name) {
            found.push(format!(
                "the schema requires `{name}`, which the parser does not"
            ));
        }
        if required[..at].contains(name) {
            found.push(format!("the schema requires `{name}` twice"));
        }
    }
    for name in bounds.required {
        if !required.contains(name) {
            found.push(format!(
                "the parser requires `{name}`, which the schema does not"
            ));
        }
    }
    if found.is_empty() {
        Ok(())
    } else {
        Err(found.join("; "))
    }
}

/// Pushes every keyword in which `actual` differs from `expected` at `path`.
fn compare(
    path: &str,
    expected: &Map<String, Value>,
    actual: &Map<String, Value>,
    found: &mut Vec<String>,
) {
    for key in actual.keys() {
        if !expected.contains_key(key) {
            found.push(format!(
                "`{path}` states `{key}`, which its parser bound does not name"
            ));
        }
    }
    for (key, want) in expected {
        match (want, actual.get(key)) {
            (_, None) => found.push(format!("`{path}` does not state `{key}`: {want}")),
            (Value::Object(want), Some(Value::Object(have))) => {
                compare(&format!("{path}.{key}"), want, have, found)
            }
            (want, Some(have)) if !same(want, have) => found.push(format!(
                "`{path}`'s `{key}` is {have}, its parser's bound is {want}"
            )),
            _ => {}
        }
    }
}

/// JSON equality with numbers compared by value, so a schema's `-90` states
/// the bound `-90.0`.
fn same(want: &Value, have: &Value) -> bool {
    match (want, have) {
        (Value::Number(want), Value::Number(have)) => want.as_f64() == have.as_f64(),
        (Value::Array(want), Value::Array(have)) => {
            want.len() == have.len() && want.iter().zip(have).all(|(w, h)| same(w, h))
        }
        _ => want == have,
    }
}

/// Argument values that hold a parser to a bound: the parser must admit each
/// of `admitted`, and refuse each of `refused` naming the argument.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Probes {
    pub admitted: Vec<Value>,
    pub refused: Vec<Value>,
}

/// The characters the longest token probe cycles through: one of each class
/// [`token_pattern`] admits.
const TOKEN_PROBE: &str = "Az09_-";

/// The probes of `bound`, or `None` for a bound whose probes are its
/// component's: a `Pattern` and an `Array`. A `Text` bound's longest admitted
/// value is two-byte characters, so only a parser counting characters, as
/// `maxLength` does, admits it; its blank
/// probe holds U+0085, whitespace to the parser and not to ECMA's `\s`. A
/// token's refused values are one too long, empty, and one per character
/// class the pattern excludes.
pub fn probes(bound: &Bound) -> Option<Probes> {
    let probes = match bound {
        Bound::Text { max_chars, .. } => Probes {
            admitted: vec![json!("é".repeat(*max_chars))],
            refused: vec![
                json!("é".repeat(max_chars + 1)),
                json!(""),
                json!(" \t\n\u{85}\u{3000}"),
                json!(1),
            ],
        },
        Bound::Token { max_chars: max } => {
            let max = *max;
            let token = |n: usize| -> String { TOKEN_PROBE.chars().cycle().take(n).collect() };
            Probes {
                admitted: vec![json!(token(max)), json!(token(1))],
                refused: [
                    token(max + 1).as_str(),
                    "",
                    "a b",
                    "a/b",
                    "a.b",
                    "a%b",
                    "a\u{1}b",
                    "é",
                ]
                .into_iter()
                .map(|text| json!(text))
                .chain([json!(1)])
                .collect(),
            }
        }
        Bound::Integer { min, max } => {
            let mut refused = vec![
                json!(max + 1),
                json!(*min as f64 + 0.5),
                json!(min.to_string()),
            ];
            if *min > 0 {
                refused.push(json!(min - 1));
            }
            Probes {
                admitted: vec![json!(min), json!(max), json!(*max as f64)],
                refused,
            }
        }
        Bound::Number { min, max } => Probes {
            admitted: vec![json!(min), json!(max)],
            refused: vec![json!(min - 1.0), json!(max + 1.0), json!(min.to_string())],
        },
        Bound::Enum(values) => Probes {
            admitted: values.iter().map(|value| json!(value)).collect(),
            refused: vec![json!(format!("{}-", values.concat())), json!(1)],
        },
        Bound::Bool => Probes {
            admitted: vec![json!(true), json!(false)],
            refused: vec![json!("true"), json!(1)],
        },
        Bound::Pattern(_) | Bound::Array { .. } => return None,
    };
    Some(probes)
}

/// Holds `tool`'s parser to its required argument `key`: `plan` admits
/// `base`, and refuses `base` without `key` with an error naming `key`.
/// Panics naming the tool otherwise.
pub fn hold_required(
    tool: &str,
    key: &str,
    base: &Value,
    plan: impl Fn(&Value) -> Result<(), String>,
) {
    admits(tool, base, &plan);
    let mut args = base.clone();
    let Some(map) = args.as_object_mut() else {
        panic!("`{tool}`'s base arguments {base} are not an object");
    };
    map.remove(key);
    refuses(tool, key, &args, &plan);
}

/// Holds `tool`'s parser to one argument's probes: `plan` admits `base`,
/// admits `base` with `key` set to each of `probes.admitted`, and refuses
/// `base` with `key` set to each of `probes.refused` with an error naming
/// `key`. Panics naming the tool and the value otherwise.
pub fn hold_probes(
    tool: &str,
    key: &str,
    probes: &Probes,
    base: &Value,
    plan: impl Fn(&Value) -> Result<(), String>,
) {
    admits(tool, base, &plan);
    let with = |value: &Value| {
        let mut args = base.clone();
        args[key] = value.clone();
        args
    };
    for value in &probes.admitted {
        admits(tool, &with(value), &plan);
    }
    for value in &probes.refused {
        refuses(tool, key, &with(value), &plan);
    }
}

/// Panics unless `plan` admits `args`.
fn admits(tool: &str, args: &Value, plan: &impl Fn(&Value) -> Result<(), String>) {
    if let Err(error) = plan(args) {
        panic!("`{tool}` refuses {args}: {error}");
    }
}

/// Panics unless `plan` refuses `args` with an error naming `key`.
fn refuses(tool: &str, key: &str, args: &Value, plan: &impl Fn(&Value) -> Result<(), String>) {
    match plan(args) {
        Ok(()) => panic!("`{tool}` admits {args}"),
        Err(error) => assert!(
            error.contains(&format!("`{key}`")),
            "`{tool}` refuses {args} without naming `{key}`: {error}"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SPEC: &str = r#"
spec fixture {
  provide fixture-lookup {
    description = "Looks up.";
    input_schema = """{ "type": "object", "properties": { "q": { "type": "string" } }, "additionalProperties": false }""";
  }
  provide fixture-search {
    description = "Has no schema.";
  }
  provide fixture-later {
    input_schema = """{ "type": "object" }""";
  }
}
"#;

    #[test]
    fn the_reader_finds_a_tools_schema_inside_its_block() {
        let schema = input_schema(SPEC, "fixture-lookup");
        assert_eq!(schema["properties"]["q"], json!({ "type": "string" }));
    }

    #[test]
    #[should_panic(expected = "the spec has no `provide fixture-absent` block")]
    fn the_reader_panics_naming_an_undeclared_tool() {
        input_schema(SPEC, "fixture-absent");
    }

    #[test]
    #[should_panic(expected = "the spec's `provide fixture-search` block has no input_schema")]
    fn the_reader_does_not_take_the_next_blocks_schema() {
        input_schema(SPEC, "fixture-search");
    }

    #[test]
    #[should_panic(expected = "the spec declares `provide fixture-lookup` 2 times")]
    fn the_reader_panics_on_a_tool_declared_twice() {
        let twice = format!("{SPEC}\n  provide fixture-lookup {{\n  }}\n");
        input_schema(&twice, "fixture-lookup");
    }

    #[test]
    fn a_token_pattern_states_its_length() {
        assert_eq!(token_pattern(128), "^[A-Za-z0-9_-]{1,128}$");
    }

    const KINDS: &[&str] = &["daily", "hourly"];

    /// One property of every bound, and the schema that states them exactly.
    fn every_row() -> (ToolBounds<'static>, Value) {
        let bounds = ToolBounds {
            properties: vec![
                (
                    "q",
                    Bound::Text {
                        max_chars: 1024,
                        format: None,
                    },
                ),
                (
                    "at",
                    Bound::Text {
                        max_chars: 64,
                        format: Some("date-time"),
                    },
                ),
                ("id", Bound::Token { max_chars: 128 }),
                ("max", Bound::Integer { min: 1, max: 25 }),
                (
                    "lat",
                    Bound::Number {
                        min: -90.0,
                        max: 90.0,
                    },
                ),
                ("kind", Bound::Enum(KINDS.to_vec())),
                ("all", Bound::Bool),
                (
                    "ids",
                    Bound::Array {
                        min: 1,
                        max: 50,
                        unique: true,
                        item: Box::new(Bound::Pattern(token_pattern(8))),
                    },
                ),
                (
                    "tags",
                    Bound::Array {
                        min: 0,
                        max: 5,
                        unique: false,
                        item: Box::new(Bound::Bool),
                    },
                ),
            ],
            required: &["q", "kind"],
        };
        let schema: Value = serde_json::from_str(
            r#"{ "type": "object", "properties": {
                "q": { "type": "string", "minLength": 1, "maxLength": 1024, "pattern": "[^\\s\\u0085]" },
                "at": { "type": "string", "minLength": 1, "maxLength": 64, "pattern": "[^\\s\\u0085]", "format": "date-time" },
                "id": { "type": "string", "pattern": "^[A-Za-z0-9_-]{1,128}$" },
                "max": { "type": "integer", "minimum": 1, "maximum": 25 },
                "lat": { "type": "number", "minimum": -90, "maximum": 90 },
                "kind": { "type": "string", "enum": ["daily", "hourly"] },
                "all": { "type": "boolean" },
                "ids": { "type": "array", "minItems": 1, "maxItems": 50, "uniqueItems": true,
                         "items": { "type": "string", "pattern": "^[A-Za-z0-9_-]{1,8}$" } },
                "tags": { "type": "array", "minItems": 0, "maxItems": 5, "items": { "type": "boolean" } }
              }, "required": ["kind", "q"], "additionalProperties": false }"#,
        )
        .unwrap();
        (bounds, schema)
    }

    /// `check`'s error after `edit` is applied to the exact schema.
    fn refusal(edit: impl FnOnce(&mut Value)) -> String {
        let (bounds, mut schema) = every_row();
        edit(&mut schema);
        check(&schema, &bounds).unwrap_err()
    }

    #[test]
    fn check_accepts_each_rows_exact_form() {
        let (bounds, schema) = every_row();
        assert_eq!(check(&schema, &bounds), Ok(()));
    }

    #[test]
    fn check_refuses_a_missing_keyword() {
        assert_eq!(
            refusal(|schema| {
                schema["properties"]["q"]
                    .as_object_mut()
                    .unwrap()
                    .remove("maxLength");
            }),
            "`q` does not state `maxLength`: 1024"
        );
        assert_eq!(
            refusal(|schema| {
                schema["properties"]["ids"]
                    .as_object_mut()
                    .unwrap()
                    .remove("uniqueItems");
            }),
            "`ids` does not state `uniqueItems`: true"
        );
    }

    #[test]
    fn check_refuses_a_keyword_no_bound_names() {
        assert_eq!(
            refusal(|schema| schema["properties"]["id"]["minLength"] = json!(1)),
            "`id` states `minLength`, which its parser bound does not name"
        );
        assert_eq!(
            refusal(|schema| schema["properties"]["ids"]["items"]["default"] = json!("a")),
            "`ids.items` states `default`, which its parser bound does not name"
        );
        assert_eq!(
            refusal(|schema| schema["properties"]["tags"]["uniqueItems"] = json!(false)),
            "`tags` states `uniqueItems`, which its parser bound does not name"
        );
        assert_eq!(
            refusal(|schema| schema["description"] = json!("a tool")),
            "the schema states `description`, which no parser bound names"
        );
    }

    #[test]
    fn check_refuses_a_wrong_value() {
        assert_eq!(
            refusal(|schema| schema["properties"]["q"]["maxLength"] = json!(1000)),
            "`q`'s `maxLength` is 1000, its parser's bound is 1024"
        );
        assert_eq!(
            refusal(|schema| schema["properties"]["lat"]["minimum"] = json!(-89.5)),
            "`lat`'s `minimum` is -89.5, its parser's bound is -90.0"
        );
        assert_eq!(
            refusal(|schema| schema["properties"]["kind"]["enum"] = json!(["hourly", "daily"])),
            r#"`kind`'s `enum` is ["hourly","daily"], its parser's bound is ["daily","hourly"]"#
        );
        assert_eq!(
            refusal(|schema| schema["properties"]["max"]["type"] = json!("number")),
            r#"`max`'s `type` is "number", its parser's bound is "integer""#
        );
        assert_eq!(
            refusal(|schema| schema["additionalProperties"] = json!(true)),
            "the schema's `additionalProperties` is not `false`"
        );
    }

    #[test]
    fn the_text_row_pins_non_blank_exactly() {
        assert_eq!(NON_BLANK, r"[^\s\u0085]");
        assert_eq!(
            refusal(|schema| schema["properties"]["q"]["pattern"] = json!(r"\S")),
            r#"`q`'s `pattern` is "\\S", its parser's bound is "[^\\s\\u0085]""#
        );
    }

    #[test]
    fn check_refuses_a_wrong_property_set() {
        assert_eq!(
            refusal(|schema| schema["properties"]["page"] = json!({ "type": "string" })),
            "the schema states `page`, which the parser does not admit"
        );
        assert_eq!(
            refusal(|schema| {
                schema["properties"].as_object_mut().unwrap().remove("all");
            }),
            "the schema does not state `all`"
        );
    }

    #[test]
    fn check_refuses_a_wrong_required() {
        assert_eq!(
            refusal(|schema| schema["required"] = json!(["q", "kind", "max"])),
            "the schema requires `max`, which the parser does not"
        );
        assert_eq!(
            refusal(|schema| schema["required"] = json!(["q"])),
            "the parser requires `kind`, which the schema does not"
        );
        assert_eq!(
            refusal(|schema| schema["required"] = json!(["q", "kind", "q"])),
            "the schema requires `q` twice"
        );
    }

    #[test]
    fn check_refuses_a_schema_not_shaped_as_an_object_schema() {
        let (bounds, _) = every_row();
        assert_eq!(
            check(&json!([]), &bounds),
            Err("the schema is not an object".to_string())
        );
        assert_eq!(
            refusal(|schema| schema["type"] = json!("array")),
            "the schema's `type` is not `object`"
        );
        assert_eq!(
            refusal(|schema| {
                schema.as_object_mut().unwrap().remove("type");
            }),
            "the schema's `type` is not `object`"
        );
        // A boolean subschema is valid JSON Schema and states no bound.
        assert_eq!(
            refusal(|schema| schema["properties"]["q"] = json!(true)),
            "`q`'s schema is not an object"
        );
        assert_eq!(
            refusal(|schema| schema["properties"] = json!([])),
            [
                "the schema's `properties` is not an object",
                "the schema does not state `q`",
                "the schema does not state `at`",
                "the schema does not state `id`",
                "the schema does not state `max`",
                "the schema does not state `lat`",
                "the schema does not state `kind`",
                "the schema does not state `all`",
                "the schema does not state `ids`",
                "the schema does not state `tags`",
            ]
            .join("; ")
        );
        assert_eq!(
            refusal(|schema| schema["required"] = json!("q")),
            "the schema's `required` is not an array; \
             the parser requires `q`, which the schema does not; \
             the parser requires `kind`, which the schema does not"
        );
        assert_eq!(
            refusal(|schema| schema["required"] = json!(["q", "kind", 7])),
            "the schema's `required` holds 7, not a name"
        );
    }

    /// A fixture tool's parser: the kit's own parser for `bound` reading
    /// `key`, which is required when `required` is set.
    fn parser(bound: &Bound, required: bool) -> impl Fn(&Value) -> Result<(), String> {
        move |args: &Value| {
            let args = crate::object(args, &["key"])?;
            match bound {
                Bound::Text { max_chars, .. } => {
                    crate::capped(args, "key", *max_chars).map(drop)?
                }
                Bound::Integer { min, max } => crate::integer(args, "key", *min, *max).map(drop)?,
                Bound::Bool => crate::flag(args, "key").map(drop)?,
                other => panic!("the kit has no parser for {other:?}"),
            }
            if required && !args.contains_key("key") {
                return Err("`key` is required".to_string());
            }
            Ok(())
        }
    }

    #[test]
    fn the_kits_parsers_hold_to_their_bounds_probes() {
        for bound in [
            Bound::Text {
                max_chars: 16,
                format: None,
            },
            Bound::Integer { min: 1, max: 25 },
            Bound::Integer { min: 0, max: 3 },
            Bound::Bool,
        ] {
            let probes = probes(&bound).unwrap();
            hold_probes(
                "fixture-lookup",
                "key",
                &probes,
                &json!({}),
                parser(&bound, false),
            );
        }
    }

    #[test]
    fn a_required_argument_holds_when_its_absence_is_refused_naming_it() {
        let bound = Bound::Bool;
        hold_required(
            "fixture-lookup",
            "key",
            &json!({ "key": true }),
            parser(&bound, true),
        );
    }

    #[test]
    #[should_panic(expected = "`fixture-lookup` admits {}")]
    fn an_optional_argument_does_not_hold_as_required() {
        let bound = Bound::Bool;
        hold_required(
            "fixture-lookup",
            "key",
            &json!({ "key": true }),
            parser(&bound, false),
        );
    }

    #[test]
    // The first refused probe is one two-byte character past the bound.
    #[should_panic(expected = r#"`fixture-lookup` admits {"key":"éé"#)]
    fn a_parser_wider_than_its_bound_does_not_hold() {
        let probes = probes(&Bound::Text {
            max_chars: 16,
            format: None,
        })
        .unwrap();
        let wider = Bound::Text {
            max_chars: 17,
            format: None,
        };
        hold_probes(
            "fixture-lookup",
            "key",
            &probes,
            &json!({}),
            parser(&wider, false),
        );
    }

    #[test]
    #[should_panic(expected = "without naming `key`: refused")]
    fn a_refusal_that_does_not_name_the_argument_does_not_hold() {
        let probes = probes(&Bound::Bool).unwrap();
        let unnamed = |args: &Value| match args.get("key") {
            None | Some(Value::Bool(_)) => Ok(()),
            Some(_) => Err("refused".to_string()),
        };
        hold_probes("fixture-lookup", "key", &probes, &json!({}), unnamed);
    }

    #[test]
    #[should_panic(expected = "`fixture-lookup` refuses {}: `key` is required")]
    fn a_base_the_parser_refuses_does_not_hold() {
        let bound = Bound::Bool;
        let probes = probes(&bound).unwrap();
        hold_probes(
            "fixture-lookup",
            "key",
            &probes,
            &json!({}),
            parser(&bound, true),
        );
    }

    #[test]
    fn a_text_probe_is_wider_in_bytes_than_in_characters() {
        let probes = probes(&Bound::Text {
            max_chars: 4,
            format: None,
        })
        .unwrap();
        assert_eq!(probes.admitted, [json!("éééé")]);
        assert_eq!(probes.refused[0], json!("ééééé"));
    }

    #[test]
    fn token_probes_hold_the_length_and_every_excluded_class() {
        let probes = probes(&Bound::Token { max_chars: 8 }).unwrap();
        assert_eq!(probes.admitted, [json!("Az09_-Az"), json!("A")]);
        let is_token = |value: &Value| {
            value.as_str().is_some_and(|text| {
                (1..=8).contains(&text.len())
                    && text
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
            })
        };
        assert!(probes.admitted.iter().all(is_token));
        assert!(!probes.refused.iter().any(is_token));
        assert!(probes.refused.contains(&json!("Az09_-Az0")));
    }

    #[test]
    fn number_and_enum_probes_sit_on_and_past_the_bound() {
        let number = probes(&Bound::Number {
            min: -90.0,
            max: 90.0,
        })
        .unwrap();
        assert_eq!(number.admitted, [json!(-90.0), json!(90.0)]);
        assert_eq!(number.refused, [json!(-91.0), json!(91.0), json!("-90")]);
        let choices = probes(&Bound::Enum(vec!["daily", "hourly"])).unwrap();
        assert_eq!(choices.admitted, [json!("daily"), json!("hourly")]);
        assert_eq!(choices.refused, [json!("dailyhourly-"), json!(1)]);
    }

    #[test]
    fn a_components_own_pattern_or_array_has_no_kit_probes() {
        assert_eq!(probes(&Bound::Pattern("^[a-z]{1,8}$".to_string())), None);
        assert_eq!(probes(&Bound::Pattern(token_pattern(8))), None);
        assert_eq!(
            probes(&Bound::Array {
                min: 1,
                max: 2,
                unique: true,
                item: Box::new(Bound::Bool),
            }),
            None
        );
    }

    #[test]
    fn a_tool_with_nothing_required_may_omit_required() {
        let bounds = ToolBounds {
            properties: vec![("all", Bound::Bool)],
            required: &[],
        };
        let schema = json!({ "type": "object", "properties": { "all": { "type": "boolean" } },
            "additionalProperties": false });
        assert_eq!(check(&schema, &bounds), Ok(()));
    }
}
