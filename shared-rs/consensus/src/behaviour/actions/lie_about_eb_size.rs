//! `lie-about-eb-size` — mutate the advertised `eb_size` on outbound
//! `MsgLeiosBlockOffer`.
//!
//! Sets `leios.offer_eb_size = Linear { scale_num, scale_den, offset }`; the
//! LeiosNotify offer actuator then rewrites each offer's `eb_size` via
//! [`EbSizePolicy::apply`](crate::behaviour::tree::control::EbSizePolicy::apply)
//! (the `i128` size math). The original duplex-follower bug shape (size-zero)
//! is `scale_num = 0, scale_den = 1, offset = 0`. Returns `Running` while
//! installed.

use crate::behaviour::tree::actions::LeafAction;
use crate::behaviour::tree::control::{ControlSignal, EbSizePolicy};
use crate::behaviour::tree::env::{ConsensusCtx, TickCtx};
use crate::behaviour::tree::Status;

/// Installs the linear `eb_size` rewrite policy.
#[derive(Debug, Clone, Copy)]
pub struct LieAboutEbSize {
    scale_num: u32,
    /// Clamped to `>= 1` (a `0` denominator is interpreted as `1`).
    scale_den: u32,
    offset: i32,
}

impl LieAboutEbSize {
    /// `scale_den` of `0` is clamped to `1`.
    pub fn new(scale_num: u32, scale_den: u32, offset: i32) -> Self {
        Self {
            scale_num,
            scale_den: scale_den.max(1),
            offset,
        }
    }

    /// The policy this action installs.
    fn policy(&self) -> EbSizePolicy {
        EbSizePolicy::Linear {
            scale_num: self.scale_num,
            scale_den: self.scale_den,
            offset: self.offset,
        }
    }
}

impl LeafAction<ConsensusCtx, ControlSignal> for LieAboutEbSize {
    fn contribute(&mut self, _ctx: &TickCtx, out: &mut ControlSignal) -> Status {
        out.leios.offer_eb_size = self.policy();
        Status::Running
    }

    fn set_param(&mut self, field: &str, value: &toml::Value) {
        let Some(v) = value.as_integer() else {
            return;
        };
        match field {
            "scale_num" => self.scale_num = v.clamp(0, u32::MAX as i64) as u32,
            // Same clamp as `new`: a 0 denominator is interpreted as 1.
            "scale_den" => self.scale_den = v.clamp(1, u32::MAX as i64) as u32,
            "offset" => self.offset = v.clamp(i32::MIN as i64, i32::MAX as i64) as i32,
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::behaviour::tree::env::{DynamicEnv, NativeChainState};

    fn installed_policy(action: &mut LieAboutEbSize) -> EbSizePolicy {
        let env = DynamicEnv::new();
        let state = NativeChainState::default();
        let ctx = TickCtx {
            env: &env,
            state: &state,
            seed: 0,
            action_params: None,
        };
        let mut out = ControlSignal::default();
        let s = action.contribute(&ctx, &mut out);
        assert_eq!(s, Status::Running);
        out.leios.offer_eb_size
    }

    /// The size that the installed policy would advertise for `eb_size`.
    fn size_via(action: &mut LieAboutEbSize, eb_size: u32) -> u32 {
        installed_policy(action).apply(eb_size)
    }

    // Ported size-math cases from the merged hook behaviour.

    #[test]
    fn zero_constructor_yields_zero_size() {
        assert_eq!(size_via(&mut LieAboutEbSize::new(0, 1, 0), 12_345), 0);
    }

    #[test]
    fn identity_no_op() {
        assert_eq!(size_via(&mut LieAboutEbSize::new(1, 1, 0), 12_345), 12_345);
    }

    #[test]
    fn off_by_constant() {
        assert_eq!(size_via(&mut LieAboutEbSize::new(1, 1, 100), 1000), 1100);
    }

    #[test]
    fn halving() {
        assert_eq!(size_via(&mut LieAboutEbSize::new(1, 2, 0), 1000), 500);
    }

    #[test]
    fn doubling() {
        assert_eq!(size_via(&mut LieAboutEbSize::new(2, 1, 0), 1000), 2000);
    }

    #[test]
    fn clamp_below_zero() {
        assert_eq!(size_via(&mut LieAboutEbSize::new(1, 1, -100), 50), 0);
    }

    #[test]
    fn clamp_above_u32_max() {
        assert_eq!(
            size_via(&mut LieAboutEbSize::new(4, 1, 0), u32::MAX),
            u32::MAX
        );
    }

    #[test]
    fn zero_denominator_treated_as_one() {
        // Constructor clamps scale_den 0 -> 1, so this is identity.
        assert_eq!(size_via(&mut LieAboutEbSize::new(1, 0, 0), 777), 777);
    }

    #[test]
    fn installs_linear_policy() {
        let p = installed_policy(&mut LieAboutEbSize::new(3, 4, -5));
        assert_eq!(
            p,
            EbSizePolicy::Linear {
                scale_num: 3,
                scale_den: 4,
                offset: -5,
            }
        );
    }
}
