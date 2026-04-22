/// Lifecycle state of a workflow instance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i64)]
pub enum InstanceStatus {
    /// Created, no steps executed.
    Pending = 0,
    /// At least one step executed and not terminal.
    Running = 1,
    /// Completed successfully.
    Completed = 2,
    /// Failed on step execution.
    Failed = 3,
    /// Cancelled by caller.
    Cancelled = 4,
}

impl InstanceStatus {
    /// Convert the status to its stable i64 discriminant.
    pub const fn as_i64(self) -> i64 {
        self as i64
    }

    /// Whether this status is terminal.
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    /// Whether a transition to `next` is valid.
    pub const fn can_transition_to(self, next: Self) -> bool {
        match (self, next) {
            (Self::Pending, Self::Running)
            | (Self::Pending, Self::Completed)
            | (Self::Pending, Self::Cancelled)
            // First-step script errors fail the instance directly.
            | (Self::Pending, Self::Failed)
            | (Self::Running, Self::Running)
            | (Self::Running, Self::Completed)
            | (Self::Running, Self::Failed)
            | (Self::Running, Self::Cancelled) => true,
            (Self::Completed, _)
            | (Self::Failed, _)
            | (Self::Cancelled, _)
            | (Self::Pending, _)
            | (Self::Running, _) => false,
        }
    }

    /// Parse an i64 discriminant.
    pub const fn try_from_i64(value: i64) -> Option<Self> {
        match value {
            0 => Some(Self::Pending),
            1 => Some(Self::Running),
            2 => Some(Self::Completed),
            3 => Some(Self::Failed),
            4 => Some(Self::Cancelled),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::InstanceStatus;

    #[test]
    fn status_discriminant_round_trip() {
        for (raw, expected) in [
            (0_i64, InstanceStatus::Pending),
            (1_i64, InstanceStatus::Running),
            (2_i64, InstanceStatus::Completed),
            (3_i64, InstanceStatus::Failed),
            (4_i64, InstanceStatus::Cancelled),
        ] {
            let parsed = InstanceStatus::try_from_i64(raw).unwrap();
            assert_eq!(parsed, expected);
            assert_eq!(parsed.as_i64(), raw);
        }
        assert!(InstanceStatus::try_from_i64(99).is_none());
    }

    #[test]
    fn terminal_flags_are_correct() {
        assert!(!InstanceStatus::Pending.is_terminal());
        assert!(!InstanceStatus::Running.is_terminal());
        assert!(InstanceStatus::Completed.is_terminal());
        assert!(InstanceStatus::Failed.is_terminal());
        assert!(InstanceStatus::Cancelled.is_terminal());
    }

    #[test]
    fn transition_matrix_is_exhaustive() {
        let states = [
            InstanceStatus::Pending,
            InstanceStatus::Running,
            InstanceStatus::Completed,
            InstanceStatus::Failed,
            InstanceStatus::Cancelled,
        ];

        for from in states {
            for to in states {
                let allowed = from.can_transition_to(to);
                let expected = matches!(
                    (from, to),
                    (InstanceStatus::Pending, InstanceStatus::Running)
                        | (InstanceStatus::Pending, InstanceStatus::Completed)
                        | (InstanceStatus::Pending, InstanceStatus::Cancelled)
                        | (InstanceStatus::Pending, InstanceStatus::Failed)
                        | (InstanceStatus::Running, InstanceStatus::Running)
                        | (InstanceStatus::Running, InstanceStatus::Completed)
                        | (InstanceStatus::Running, InstanceStatus::Failed)
                        | (InstanceStatus::Running, InstanceStatus::Cancelled)
                );
                assert_eq!(allowed, expected, "{from:?} -> {to:?}");
            }
        }
    }
}
