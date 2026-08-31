//! The reserved `local:brenn/*` control-plane bodies chrome parses and
//! publishes, spelled for both universes this crate compiles in.
//!
//! The kernel's spellings live in `brenn-surface-schema`, which the guest
//! crate universe does not carry: the two graphs resolve their third-party
//! dependencies independently, so a type carrying the host graph's serde derive
//! satisfies nothing on the guest side. These are the same wire shapes, spelled
//! again here — and the equality is asserted rather than assumed, by the
//! host-side parity tests at the foot of this module.
//!
//! Only the planes chrome speaks are here. A body it never reads has no
//! business being re-spelled.

// TODO(surface-guest-wire-crate): this module is the largest re-spelling in the
// tree — eight wire shapes, `CONTROL_PLANE_VERSION` and the two theme strings,
// held by the six parity tests at the foot of the file. A shared guest-side
// wire crate replaces all of it.

use serde::{Deserialize, Serialize};

/// The `v` every control-plane body carries.
pub const CONTROL_PLANE_VERSION: u8 = 1;

/// The link state the kernel reports on `local:brenn/link-state`, as the *page*
/// experiences the connection: a consumer renders it and must not reason about
/// sockets to do so.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LinkState {
    Connecting,
    Connected,
    Reconnecting,
    Reloading,
    /// Terminal. The plane carries `{v, state}` only, so a server-supplied
    /// fatal detail never reaches the on-screen banner.
    Fatal,
}

/// The body published on `local:brenn/link-state`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkStateBody {
    pub v: u8,
    pub state: LinkState,
}

/// Mount state of one component instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InstanceState {
    /// Wired and delivering. For a rendering instance that means its host
    /// element is mounted; for a headless one, that its activation entry is
    /// registered.
    Mounted,
    /// Dead: never loaded, refused, or trapped. Delivery to it has stopped.
    Failed,
    /// Declared and not yet wired.
    Pending,
}

/// The body published on `local:brenn/surface-state`: the page-local mirror of
/// the mount/failure facts the kernel reports, and the set chrome arranges.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SurfaceStateBody {
    pub v: u8,
    pub instances: Vec<SurfaceStateInstance>,
}

/// One instance's mount state on `local:brenn/surface-state`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SurfaceStateInstance {
    pub instance: String,
    pub kind: String,
    pub state: InstanceState,
    /// Short failure reason when `state` is `Failed`; `None` otherwise.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// How loud a [`ToastBody`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ToastSeverity {
    Info,
    Warning,
    Error,
}

impl ToastSeverity {
    /// The wire-protocol string for this severity.
    pub fn as_wire_str(self) -> &'static str {
        match self {
            ToastSeverity::Info => "info",
            ToastSeverity::Warning => "warning",
            ToastSeverity::Error => "error",
        }
    }
}

/// Who raised a toast. Chrome renders the kernel's own notices distinguishably,
/// because an operator reading one needs to know whose voice it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ToastSource {
    Kernel,
}

impl ToastSource {
    /// The wire-protocol string for this source.
    pub fn as_wire_str(self) -> &'static str {
        match self {
            ToastSource::Kernel => "kernel",
        }
    }
}

/// The body published on `local:brenn/toast`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToastBody {
    pub v: u8,
    pub severity: ToastSeverity,
    pub text: String,
    pub source: ToastSource,
}

/// The two legal [`ThemeBody`] `theme` values, and the two `data-theme`
/// attribute values chrome writes.
pub const THEME_DARK: &str = "dark";
/// See [`THEME_DARK`].
pub const THEME_LIGHT: &str = "light";

/// The body published on `local:brenn/theme`: the runtime theme axis a producer
/// asks chrome to apply.
///
/// `theme` stays a string so chrome owns wire-string parsing: an unrecognized
/// value is dropped-and-reported, never rejected at deserialize time, so a bad
/// theme cannot brick delivery of a well-formed envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThemeBody {
    pub v: u8,
    pub theme: String,
}

/// The action a [`TakeoverBody`] asks chrome to take on the takeover overlay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TakeoverAction {
    Request,
    Release,
}

/// The body published on `local:brenn/takeover`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TakeoverBody {
    pub v: u8,
    pub action: TakeoverAction,
    /// The requesting instance. The kernel's local router injects it from its
    /// own port wiring, overwriting whatever the publisher supplied, so a
    /// component cannot name another instance as the holder.
    pub instance: String,
}

/// The body published on `local:brenn/overlay-state`: chrome's overlay
/// holdership after the fold that changed it. Published on every transition and
/// only on a transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OverlayStateBody {
    pub v: u8,
    /// The instance holding the fullscreen overlay, or `None` when none is.
    pub holder: Option<String>,
    /// The page-monotonic millisecond reading at the transition.
    pub since_stamp: u64,
}

/// The two spellings are equal, asserted rather than assumed.
///
/// Host-only: `brenn-surface-schema` is a host-graph crate, and this crate's
/// wasm32 build is exactly the build that cannot link it — which is why these
/// types exist. The assertion is one-directional per body only in appearance:
/// serializing the kernel's spelling and deserializing chrome's proves the
/// field names and the enum wire strings both ways, since an unknown field
/// would be silently accepted but a missing one would not, and the round trip
/// back through the kernel's type closes that gap.
#[cfg(all(test, not(target_arch = "wasm32")))]
mod parity_tests {
    use super::*;
    use brenn_surface_schema as proto;

    /// Serialize the kernel's `their` to JSON, read it as chrome's `Ours`, then
    /// write chrome's back and read it as the kernel's again — proving both
    /// spellings accept and emit the same document.
    fn round_trip<Theirs, Ours>(theirs: &Theirs)
    where
        Theirs: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
        Ours: serde::Serialize + serde::de::DeserializeOwned,
    {
        let json = serde_json::to_string(theirs).expect("the kernel's body serializes");
        let ours: Ours = serde_json::from_str(&json).expect("chrome's spelling reads it");
        let back = serde_json::to_string(&ours).expect("chrome's body serializes");
        let theirs_again: Theirs =
            serde_json::from_str(&back).expect("the kernel's spelling reads chrome's");
        assert_eq!(*theirs, theirs_again, "round trip through {json}");
        assert_eq!(json, back, "the two spellings emit the same document");
    }

    #[test]
    fn the_control_plane_version_is_the_kernels() {
        assert_eq!(CONTROL_PLANE_VERSION, proto::CONTROL_PLANE_VERSION);
        assert_eq!(THEME_DARK, proto::THEME_DARK);
        assert_eq!(THEME_LIGHT, proto::THEME_LIGHT);
    }

    #[test]
    fn every_link_state_is_the_kernels() {
        for state in [
            proto::LinkState::Connecting,
            proto::LinkState::Connected,
            proto::LinkState::Reconnecting,
            proto::LinkState::Reloading,
            proto::LinkState::Fatal,
        ] {
            round_trip::<_, LinkStateBody>(&proto::LinkStateBody {
                v: proto::CONTROL_PLANE_VERSION,
                state,
            });
        }
    }

    #[test]
    fn every_instance_state_is_the_kernels() {
        for state in [
            proto::InstanceState::Mounted,
            proto::InstanceState::Failed,
            proto::InstanceState::Pending,
        ] {
            round_trip::<_, SurfaceStateBody>(&proto::SurfaceStateBody {
                v: proto::CONTROL_PLANE_VERSION,
                instances: vec![
                    proto::SurfaceStateInstance {
                        instance: "panel".to_string(),
                        kind: "protobar".to_string(),
                        state,
                        reason: None,
                    },
                    proto::SurfaceStateInstance {
                        instance: "other".to_string(),
                        kind: "meeting".to_string(),
                        state,
                        reason: Some("trapped".to_string()),
                    },
                ],
            });
        }
    }

    #[test]
    fn every_toast_severity_and_source_is_the_kernels() {
        for severity in [
            proto::ToastSeverity::Info,
            proto::ToastSeverity::Warning,
            proto::ToastSeverity::Error,
        ] {
            round_trip::<_, ToastBody>(&proto::ToastBody {
                v: proto::CONTROL_PLANE_VERSION,
                severity,
                text: "a notice".to_string(),
                source: proto::ToastSource::Kernel,
            });
        }
    }

    /// The `data-toast-severity` / `data-toast-source` attribute words are the
    /// serde words. The styling hook is a rendered attribute, so a divergence
    /// here is an unstyled toast and no error anywhere.
    #[test]
    fn the_toast_wire_strings_are_what_serde_emits() {
        for severity in [
            ToastSeverity::Info,
            ToastSeverity::Warning,
            ToastSeverity::Error,
        ] {
            assert_eq!(
                serde_json::to_value(severity).expect("a severity serializes"),
                serde_json::json!(severity.as_wire_str()),
            );
        }
        let source = ToastSource::Kernel;
        assert_eq!(
            serde_json::to_value(source).expect("a source serializes"),
            serde_json::json!(source.as_wire_str()),
        );
    }

    #[test]
    fn the_theme_body_is_the_kernels() {
        round_trip::<_, ThemeBody>(&proto::ThemeBody {
            v: proto::CONTROL_PLANE_VERSION,
            theme: proto::THEME_LIGHT.to_string(),
        });
    }

    #[test]
    fn every_takeover_action_is_the_kernels() {
        for action in [
            proto::TakeoverAction::Request,
            proto::TakeoverAction::Release,
        ] {
            round_trip::<_, TakeoverBody>(&proto::TakeoverBody {
                v: proto::CONTROL_PLANE_VERSION,
                action,
                instance: "panel".to_string(),
            });
        }
    }

    #[test]
    fn the_overlay_state_body_is_the_kernels() {
        for holder in [None, Some("panel".to_string())] {
            round_trip::<_, OverlayStateBody>(&proto::OverlayStateBody {
                v: proto::CONTROL_PLANE_VERSION,
                holder,
                since_stamp: 1_234,
            });
        }
    }
}
