//! PWA push address parsing and canonical forms.
//!
//! Address grammar:
//! - `pwa_push:<user>` — fan-out to all of a user's subscribed devices.
//! - `pwa_push:<user>@<device>` — exactly one device for one user.
//!
//! Both `<user>` and `<device>` must match `^[A-Za-z0-9._~\-]+$`
//! (RFC 3986 unreserved characters). `@` is the sentinel delimiter and is
//! excluded from the charset, making splitting unambiguous.

use std::fmt;

pub const PREFIX: &str = crate::messaging::PWA_PUSH_ADDRESS_PREFIX;

/// A parsed pwa_push address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PwaPushAddress {
    /// `pwa_push:<user>` — fan-out to all of a user's subscribed devices.
    User { user: String },
    /// `pwa_push:<user>@<device>` — exactly one device for one user.
    Device { user: String, device: String },
}

impl PwaPushAddress {
    /// Returns the canonical string form of this address.
    pub fn to_canonical_string(&self) -> String {
        match self {
            Self::User { user } => canonical_user_address(user),
            Self::Device { user, device } => canonical_device_address(user, device),
        }
    }

    /// Returns the username component.
    pub fn user(&self) -> &str {
        match self {
            Self::User { user } | Self::Device { user, .. } => user,
        }
    }
}

impl fmt::Display for PwaPushAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_canonical_string())
    }
}

/// Error from [`parse_pwa_push_address`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    /// String does not start with `pwa_push:`.
    MissingPrefix,
    /// `<user>` part is empty.
    EmptyUser,
    /// `<device>` part is empty (address had a trailing `@` with nothing after it).
    EmptyDevice,
    /// `<user>` or `<device>` contains characters outside the allowed charset.
    InvalidChar { part: &'static str, ch: char },
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingPrefix => write!(f, "address must start with '{PREFIX}'"),
            Self::EmptyUser => write!(f, "user part is empty"),
            Self::EmptyDevice => write!(f, "device part is empty after '@'"),
            Self::InvalidChar { part, ch } => {
                write!(f, "{part} contains invalid character {ch:?}")
            }
        }
    }
}

impl std::error::Error for ParseError {}

fn validate_part(part: &str, name: &'static str) -> Result<(), ParseError> {
    if let Some(bad) = part
        .chars()
        .find(|c| !crate::messaging::is_unreserved_char(*c))
    {
        return Err(ParseError::InvalidChar {
            part: name,
            ch: bad,
        });
    }
    Ok(())
}

/// Parse a `pwa_push:` address string into a [`PwaPushAddress`].
///
/// Case is **preserved** — the parser does not normalize `<user>` or `<device>`
/// to lowercase. Callers that need a stable canonical address (e.g. before
/// calling `ensure_pwa_channel`) are responsible for resolving the username
/// against the `users` table case-insensitively and rebuilding the address from
/// the stored case. `PwaPushService::send()` performs this normalization in its
/// address-canonicalize step; any new publish entry point must do the same.
///
/// # Errors
///
/// Returns [`ParseError`] if the address is malformed. Callers on hot paths
/// (e.g. DB writes via `ensure_pwa_channel`) MUST call this before proceeding.
pub fn parse_pwa_push_address(s: &str) -> Result<PwaPushAddress, ParseError> {
    let rest = s.strip_prefix(PREFIX).ok_or(ParseError::MissingPrefix)?;

    // Split on the FIRST `@` only. Because `@` is not in the unreserved set
    // and we validate both parts, a username containing `@` will be caught by
    // `validate_part` — there is no ambiguity.
    match rest.split_once('@') {
        None => {
            // User-only form: `pwa_push:<user>`
            if rest.is_empty() {
                return Err(ParseError::EmptyUser);
            }
            validate_part(rest, "user")?;
            Ok(PwaPushAddress::User {
                user: rest.to_owned(),
            })
        }
        Some((user, device)) => {
            // Device form: `pwa_push:<user>@<device>`
            if user.is_empty() {
                return Err(ParseError::EmptyUser);
            }
            if device.is_empty() {
                return Err(ParseError::EmptyDevice);
            }
            validate_part(user, "user")?;
            validate_part(device, "device")?;
            Ok(PwaPushAddress::Device {
                user: user.to_owned(),
                device: device.to_owned(),
            })
        }
    }
}

/// Canonical address string for the user fan-out form.
pub fn canonical_user_address(user: &str) -> String {
    format!("{PREFIX}{user}")
}

/// Canonical address string for the device-specific form.
pub fn canonical_device_address(user: &str, device: &str) -> String {
    format!("{PREFIX}{user}@{device}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_user_address_round_trip() {
        let addr = "pwa_push:alice";
        let parsed = parse_pwa_push_address(addr).expect("should parse");
        assert_eq!(
            parsed,
            PwaPushAddress::User {
                user: "alice".into()
            }
        );
        assert_eq!(parsed.to_canonical_string(), addr);
        assert_eq!(format!("{parsed}"), addr);
    }

    #[test]
    fn parse_device_address_round_trip() {
        let addr = "pwa_push:alice@laptop";
        let parsed = parse_pwa_push_address(addr).expect("should parse");
        assert_eq!(
            parsed,
            PwaPushAddress::Device {
                user: "alice".into(),
                device: "laptop".into(),
            }
        );
        assert_eq!(parsed.to_canonical_string(), addr);
        assert_eq!(format!("{parsed}"), addr);
    }

    #[test]
    fn malformed_addresses_rejected() {
        let cases: &[(&str, ParseError)] = &[
            // Missing prefix
            ("brenn:foo", ParseError::MissingPrefix),
            ("alice", ParseError::MissingPrefix),
            ("", ParseError::MissingPrefix),
            // Empty user
            ("pwa_push:", ParseError::EmptyUser),
            // Empty device after @
            ("pwa_push:alice@", ParseError::EmptyDevice),
            // Bad char in user (space)
            (
                "pwa_push:rand all",
                ParseError::InvalidChar {
                    part: "user",
                    ch: ' ',
                },
            ),
            // Bad char in device (space)
            (
                "pwa_push:alice@my device",
                ParseError::InvalidChar {
                    part: "device",
                    ch: ' ',
                },
            ),
            // Colon in user
            (
                "pwa_push:rand:all",
                ParseError::InvalidChar {
                    part: "user",
                    ch: ':',
                },
            ),
            // Slash in device
            (
                "pwa_push:alice@lap/top",
                ParseError::InvalidChar {
                    part: "device",
                    ch: '/',
                },
            ),
            // Colon in device
            (
                "pwa_push:alice@dev:ice",
                ParseError::InvalidChar {
                    part: "device",
                    ch: ':',
                },
            ),
        ];

        for (input, expected_err) in cases {
            let result = parse_pwa_push_address(input);
            assert_eq!(result, Err(expected_err.clone()), "input: {input:?}");
        }
    }

    #[test]
    fn at_sign_in_username_rejected() {
        // `@` is not in the unreserved charset; a bare username that looks like
        // it has an `@` would split into (user="", device=...) which is EmptyUser,
        // or valid user + rest. Here we test that `user@bad@device` form
        // splits on the FIRST `@` — left part "user" is valid, right part
        // "bad@device" would contain `@` which is invalid as a device.
        let result = parse_pwa_push_address("pwa_push:user@bad@device");
        assert_eq!(
            result,
            Err(ParseError::InvalidChar {
                part: "device",
                ch: '@',
            })
        );
    }

    #[test]
    fn valid_unreserved_chars_accepted() {
        // Ensure all allowed RFC 3986 unreserved chars work in both parts.
        let addr = "pwa_push:User.name_1~test-foo@Dev.ice_2~bar-baz";
        let parsed = parse_pwa_push_address(addr).expect("should parse");
        assert_eq!(
            parsed,
            PwaPushAddress::Device {
                user: "User.name_1~test-foo".into(),
                device: "Dev.ice_2~bar-baz".into(),
            }
        );
    }

    #[test]
    fn user_helper_builds_correct_address() {
        assert_eq!(canonical_user_address("alice"), "pwa_push:alice");
    }

    #[test]
    fn device_helper_builds_correct_address() {
        assert_eq!(
            canonical_device_address("alice", "phone"),
            "pwa_push:alice@phone"
        );
    }

    #[test]
    fn parse_preserves_case() {
        let addr = "pwa_push:Alice@MyPhone";
        let parsed = parse_pwa_push_address(addr).expect("should parse");
        assert_eq!(
            parsed,
            PwaPushAddress::Device {
                user: "Alice".into(),
                device: "MyPhone".into(),
            }
        );
    }
}
