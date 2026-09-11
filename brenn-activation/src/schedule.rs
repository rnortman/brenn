//! The async activation gate: a mount is owed, or a port is ready.
//!
//! Both hosts decide the same question — should this instance run now? — and
//! before this module each decided it in a loop of its own. The answer is one
//! function, so a host cannot compile a gate that has forgotten a disjunct, and
//! the mount guarantee the crate docs state is machinery rather than prose on
//! one side only.
//!
//! Sans-I/O by construction: this module reads no store and no clock. A caller
//! answers [`PortReadiness`] from whatever holds its positions — the surface's
//! in-page channel stores, the backend's ring and durable store — folds
//! [`PortReadiness::is_ready`] over its own port set however it wants to, and
//! [`readiness`] decides what the combination means.
//!
//! The split is deliberate. What the two hosts share is the *disjunct* — an owed
//! mount, or a ready port — and the per-port conjunction; the fold over a port
//! set is each host's own, because each holds its ports in a different shape and
//! each is entitled to stop at the first ready one. A caller that had to hand
//! over a port collection would end up fabricating entries to make the
//! conjunction come out right, which is a gate written twice again with extra
//! steps.
//!
//! What is *not* here is the sync mount: a surface instance holding the `dom`
//! grant is mounted by its registration, in the same task that created its host
//! element, and its debt is [`MountDebt::Settled`] before this gate ever sees
//! it. See the crate docs, "Host-specific behaviours".

/// Where one mount stands with the activation every mount is guaranteed.
///
/// A component may not publish from its connect-time code, so the first thing it
/// ever schedules — the first tick of a deferred self-publish chain, the first
/// state report — has to come from the tail of an activation. An activation that
/// only happens when a bound channel happens to hold history is not something a
/// component can build on, so one is owed unconditionally.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum MountDebt {
    /// Registered with nothing to window against yet: a surface instance that
    /// registered before the page's first bindings document. The debt is not
    /// incurred until a document lands.
    ///
    /// No backend counterpart — a consumer's positions exist before its task
    /// starts, so a backend mount is [`MountDebt::Owed`] from the first
    /// instruction of the consumer.
    #[default]
    Unwired,
    /// Owed, and not yet assembled.
    Owed,
    /// Settled: this mount has had its activation. A later bindings document
    /// does not revive it — the debt is per mount, and a re-registration, a
    /// consumer restart or a process restart is a new mount.
    Settled,
}

/// One bound input port's standing at the moment the gate is asked.
///
/// The three are independent and every one of them is a host's own answer: a
/// port can be allowed and push-enabled with nothing to serve, or hold a backlog
/// its instance is not allowed to read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PortReadiness {
    /// Whether this instance may read this port at all. The backend's ACL
    /// verdict; `true` on the surface, whose bindings document decided it by
    /// binding the port.
    pub allowed: bool,
    /// Whether the port's binding wakes its instance — `push_depth > 0`. A
    /// sampled port holds no position, is never delivered to, and so can never
    /// be the reason an instance runs.
    pub push_enabled: bool,
    /// Whether the port's position is behind something its channel still holds.
    pub deliverable: bool,
}

impl PortReadiness {
    /// Whether this port on its own is a reason to activate: allowed to be read,
    /// bound to wake its instance, and behind something its channel still holds.
    /// All three, or the port is not why anyone runs.
    #[must_use]
    pub fn is_ready(self) -> bool {
        self.allowed && self.push_enabled && self.deliverable
    }
}

/// Why an instance is being activated.
///
/// Carried into assembly because the two shapes differ in what an empty window
/// means: a delivery activation with nothing new is elided, and a mount
/// activation with nothing new is the guarantee being kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wake {
    /// The once-per-mount guaranteed activation. Every allowed port is windowed
    /// whether or not it holds anything new, and the deferred windows ride along
    /// as always, so a component reconciles from what it is shown rather than
    /// replaying what it missed.
    Mount,
    /// An ordinary delivery: some allowed, push-enabled port is owed a message
    /// its channel still holds.
    Delivery,
}

/// The whole async activation gate: `Mount` if the debt is owed, else
/// `Delivery` if any port is ready, else nothing.
///
/// `any_port_ready` is the caller's own fold of [`PortReadiness::is_ready`] over
/// its port set — `any`, and the caller may stop at the first one.
///
/// The mount debt is the one thing here that makes an instance with nothing to
/// deliver ready. Empty activations are elided everywhere else; this is the
/// deliberate, once-per-mount exception, and the debt is settled at assembly —
/// by the host, whichever reason it picked the instance for — so no second empty
/// activation follows.
///
/// A mount wins over a delivery when both hold: the assembly is the same
/// assembly either way, and the guarantee is one activation per mount, not one
/// activation of a particular shape. A host that answered `Delivery` there would
/// owe a second, empty activation for a debt already discharged.
#[must_use]
pub fn readiness(mount: MountDebt, any_port_ready: bool) -> Option<Wake> {
    if mount == MountDebt::Owed {
        return Some(Wake::Mount);
    }
    any_port_ready.then_some(Wake::Delivery)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every combination of debt × allowed × push-enabled × deliverable, which
    /// is the gate in full. Each host used to hold its own version of this
    /// table; a disjunct dropped from either was a component that never ran, and
    /// a disjunct added was an empty activation every turn.
    #[test]
    fn the_gate_is_owed_or_ready_and_nothing_else() {
        for mount in [MountDebt::Unwired, MountDebt::Owed, MountDebt::Settled] {
            for allowed in [false, true] {
                for push_enabled in [false, true] {
                    for deliverable in [false, true] {
                        let port = PortReadiness {
                            allowed,
                            push_enabled,
                            deliverable,
                        };
                        assert_eq!(
                            port.is_ready(),
                            allowed && push_enabled && deliverable,
                            "{port:?}"
                        );
                        let expected = if mount == MountDebt::Owed {
                            Some(Wake::Mount)
                        } else if port.is_ready() {
                            Some(Wake::Delivery)
                        } else {
                            None
                        };
                        assert_eq!(
                            readiness(mount, port.is_ready()),
                            expected,
                            "{mount:?} + {port:?}"
                        );
                    }
                }
            }
        }
    }

    /// A mount is owed whatever the ports say, including no ports at all — the
    /// headless component with one sampled input, which is the shape that found
    /// the divergence this module closes.
    #[test]
    fn an_owed_mount_activates_over_ports_that_would_never_wake_anyone() {
        assert_eq!(readiness(MountDebt::Owed, false), Some(Wake::Mount));
        let sampled = PortReadiness {
            allowed: true,
            push_enabled: false,
            deliverable: true,
        };
        // A sampled port holds no position, so nothing about it can ever be
        // owed: it is not ready under either debt.
        assert!(!sampled.is_ready());
        assert_eq!(
            readiness(MountDebt::Owed, sampled.is_ready()),
            Some(Wake::Mount)
        );
        assert_eq!(readiness(MountDebt::Settled, sampled.is_ready()), None);
    }

    /// A denied port's backlog is not a reason to run. The backend's ACL verdict
    /// is the trust plane's answer, and an instance woken by a port it may not
    /// read would be assembled an activation with nothing in it.
    #[test]
    fn a_denied_port_never_wakes_its_instance() {
        let denied = PortReadiness {
            allowed: false,
            push_enabled: true,
            deliverable: true,
        };
        assert!(!denied.is_ready());
        assert_eq!(readiness(MountDebt::Settled, denied.is_ready()), None);
        // The mount debt is unconditional even so: the activation happens, every
        // allowed port windows (here, none of them), and the denial warning is
        // the host's own.
        assert_eq!(
            readiness(MountDebt::Owed, denied.is_ready()),
            Some(Wake::Mount)
        );
    }

    /// The fold each host writes is an `any`, and this is the shape it takes:
    /// one ready port among idle ones runs the instance, and every other port is
    /// windowed along with it.
    #[test]
    fn one_ready_port_among_idle_ones_is_a_delivery() {
        let idle = PortReadiness {
            allowed: true,
            push_enabled: true,
            deliverable: false,
        };
        let ready = PortReadiness {
            allowed: true,
            push_enabled: true,
            deliverable: true,
        };
        let fold = |ports: [PortReadiness; 3]| ports.into_iter().any(PortReadiness::is_ready);
        assert_eq!(
            readiness(MountDebt::Settled, fold([idle, idle, idle])),
            None
        );
        assert_eq!(
            readiness(MountDebt::Settled, fold([idle, ready, idle])),
            Some(Wake::Delivery)
        );
    }

    /// An unwired mount is not an owed one. A surface registration admitted
    /// before the page's first bindings document has nothing to window, so the
    /// debt is incurred by the document rather than by the registration.
    #[test]
    fn an_unwired_mount_is_not_yet_owed() {
        assert_eq!(MountDebt::default(), MountDebt::Unwired);
        assert_eq!(readiness(MountDebt::Unwired, false), None);
        // Still deliverable, though: an unwired instance holds no bindings, so
        // this combination is unreachable on the surface — the gate answers the
        // ports rather than inventing a fourth state.
        assert_eq!(readiness(MountDebt::Unwired, true), Some(Wake::Delivery));
    }
}
