//! Reading a nested `device-location` call's answer: the user's sharing
//! devices, most recent fix first, or the text that says how to turn sharing
//! on. Nothing here inherits `api-request`'s wording.

use serde_json::Value;

use crate::answer::{NestedError, device_location_not_granted};
use crate::call::{ErrorKind, Failure, internal};

/// One sharing device's fix as `device-location` answers it: what a provider
/// reads of the entry, nothing it does not use.
#[derive(Clone, Debug, PartialEq)]
pub struct DeviceFix {
    pub device: String,
    pub latitude: f64,
    pub longitude: f64,
    pub age_seconds: u64,
    pub stale: bool,
}

/// `device-location`'s answer: the user's sharing devices, or the tool's
/// text on turning sharing on when none shares.
#[derive(Clone, Debug, PartialEq)]
pub enum DeviceLocationAnswer {
    /// At least one device shares: the most recent fix, then the others,
    /// most recent first.
    Sharing {
        most_recent: DeviceFix,
        older: Vec<DeviceFix>,
    },
    /// No device shares; the tool's sentence on how the user turns it on.
    NoneSharing { how_to_enable: String },
}

/// `device-location`'s answer read, or the request's error answer. A refusal
/// of the nested call (the served call carried no user) is the request's
/// `denied`; a grant withdrawn after the call was issued is the same `denied`
/// a call that could not be issued gets; any other error, and an answer not
/// in the tool's shape, is the component's or the host's fault.
pub fn read_device_location_result(
    outcome: Result<Value, NestedError>,
) -> Result<DeviceLocationAnswer, Failure> {
    let value = match outcome {
        Ok(value) => value,
        Err(NestedError { kind, detail }) => {
            return Err(match kind.as_str() {
                "denied" => Failure {
                    kind: ErrorKind::Denied,
                    detail: format!("device-location refused the nested call: {detail}"),
                },
                "not_granted" => Failure {
                    kind: ErrorKind::Denied,
                    detail: device_location_not_granted(&format!("not_granted: {detail}")),
                },
                "invalid_args" => internal(format!(
                    "device-location rejected the wrapper's request: {detail}"
                )),
                "internal" => internal(format!(
                    "the host did not deliver device-location's answer: {detail}"
                )),
                other => internal(format!("{other}: {detail}")),
            });
        }
    };
    let Value::Object(answer) = value else {
        return Err(not_the_shape("the answer is not an object"));
    };
    let Some(Value::Array(entries)) = answer.get("devices") else {
        return Err(not_the_shape("`devices` is not an array"));
    };
    let mut devices = entries
        .iter()
        .enumerate()
        .map(|(i, entry)| read_fix(i, entry))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter();
    let how_to_enable = match answer.get("how_to_enable") {
        None => None,
        Some(Value::String(text)) => Some(text.clone()),
        Some(_) => return Err(not_the_shape("`how_to_enable` is not a string")),
    };
    if let Some(most_recent) = devices.next() {
        return Ok(DeviceLocationAnswer::Sharing {
            most_recent,
            older: devices.collect(),
        });
    }
    let Some(how_to_enable) = how_to_enable else {
        return Err(not_the_shape(
            "`devices` is empty and `how_to_enable` is absent",
        ));
    };
    Ok(DeviceLocationAnswer::NoneSharing { how_to_enable })
}

/// Entry `i` of `devices`, read.
fn read_fix(i: usize, entry: &Value) -> Result<DeviceFix, Failure> {
    let Value::Object(entry) = entry else {
        return Err(not_the_shape(&format!("`devices[{i}]` is not an object")));
    };
    let field = |name: &str| {
        entry
            .get(name)
            .ok_or_else(|| not_the_shape(&format!("`devices[{i}].{name}` is absent")))
    };
    let mistyped =
        |name: &str, what: &str| not_the_shape(&format!("`devices[{i}].{name}` is not {what}"));
    let number = |name: &str| -> Result<f64, Failure> {
        field(name)?
            .as_f64()
            .ok_or_else(|| mistyped(name, "a number"))
    };
    Ok(DeviceFix {
        device: field("device")?
            .as_str()
            .ok_or_else(|| mistyped("device", "a string"))?
            .to_string(),
        latitude: number("latitude")?,
        longitude: number("longitude")?,
        age_seconds: field("age_seconds")?
            .as_u64()
            .ok_or_else(|| mistyped("age_seconds", "a non-negative integer"))?,
        stale: field("stale")?
            .as_bool()
            .ok_or_else(|| mistyped("stale", "a boolean"))?,
    })
}

fn not_the_shape(why: &str) -> Failure {
    internal(format!(
        "device-location's answer is not the tool's shape: {why}"
    ))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::testutil::{HOW_TO_ENABLE, device_location_answer, failure, nested, no_devices};

    #[test]
    fn devices_are_read_most_recent_first() {
        let answer = read_device_location_result(device_location_answer(&[
            ("phone", 51.5, -0.12, 30, false),
            ("laptop", 48.85, 2.35, 4000, true),
        ]))
        .unwrap();
        assert_eq!(
            answer,
            DeviceLocationAnswer::Sharing {
                most_recent: DeviceFix {
                    device: "phone".to_string(),
                    latitude: 51.5,
                    longitude: -0.12,
                    age_seconds: 30,
                    stale: false,
                },
                older: vec![DeviceFix {
                    device: "laptop".to_string(),
                    latitude: 48.85,
                    longitude: 2.35,
                    age_seconds: 4000,
                    stale: true,
                }],
            }
        );
    }

    #[test]
    fn no_sharing_device_carries_the_tools_text() {
        assert_eq!(
            read_device_location_result(no_devices()),
            Ok(DeviceLocationAnswer::NoneSharing {
                how_to_enable: HOW_TO_ENABLE.to_string(),
            })
        );
    }

    #[test]
    fn each_nested_error_maps_without_api_request_wording() {
        assert_eq!(
            read_device_location_result(nested("denied", "no user")),
            Err(failure(
                ErrorKind::Denied,
                "device-location refused the nested call: no user"
            ))
        );
        let withdrawn = read_device_location_result(nested("not_granted", "gone")).unwrap_err();
        assert_eq!(withdrawn.kind, ErrorKind::Denied);
        assert!(
            withdrawn.detail.starts_with(
                "device-location could not be issued: not_granted: gone. The instance's grants \
                 must include `tool device-location {}`"
            ),
            "{}",
            withdrawn.detail
        );
        assert_eq!(
            read_device_location_result(nested("invalid_args", "bad device")),
            Err(failure(
                ErrorKind::Internal,
                "device-location rejected the wrapper's request: bad device"
            ))
        );
        assert_eq!(
            read_device_location_result(nested("internal", "lost")),
            Err(failure(
                ErrorKind::Internal,
                "the host did not deliver device-location's answer: lost"
            ))
        );
        assert_eq!(
            read_device_location_result(nested("rate_limited", "slow down")),
            Err(failure(ErrorKind::Internal, "rate_limited: slow down"))
        );
    }

    #[test]
    fn an_answer_not_in_the_tools_shape_is_internal_naming_the_field() {
        let entry = json!({
            "device": "phone", "latitude": 1.0, "longitude": 2.0, "age_seconds": 3, "stale": false,
        });
        let with = |name: &str, value: Option<Value>| {
            let mut e = entry.clone();
            match value {
                Some(v) => e[name] = v,
                None => {
                    e.as_object_mut().unwrap().remove(name);
                }
            }
            json!({ "devices": [e], "stale_after_seconds": 900 })
        };
        for (answer, why) in [
            (json!([]), "the answer is not an object"),
            (json!({}), "`devices` is not an array"),
            (
                json!({ "devices": [] }),
                "`devices` is empty and `how_to_enable` is absent",
            ),
            (
                json!({ "devices": [], "how_to_enable": 1 }),
                "`how_to_enable` is not a string",
            ),
            (json!({ "devices": [1] }), "`devices[0]` is not an object"),
            (with("device", None), "`devices[0].device` is absent"),
            (
                with("device", Some(json!(1))),
                "`devices[0].device` is not a string",
            ),
            (with("latitude", None), "`devices[0].latitude` is absent"),
            (
                with("latitude", Some(json!("1"))),
                "`devices[0].latitude` is not a number",
            ),
            (
                with("longitude", Some(json!(null))),
                "`devices[0].longitude` is not a number",
            ),
            (
                with("age_seconds", Some(json!(-1))),
                "`devices[0].age_seconds` is not a non-negative integer",
            ),
            (with("stale", None), "`devices[0].stale` is absent"),
            (
                with("stale", Some(json!("no"))),
                "`devices[0].stale` is not a boolean",
            ),
        ] {
            assert_eq!(
                read_device_location_result(Ok(answer.clone())),
                Err(failure(
                    ErrorKind::Internal,
                    &format!("device-location's answer is not the tool's shape: {why}")
                )),
                "{answer}"
            );
        }
    }
}
