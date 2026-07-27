//! `announce-size-lie` — lie about the `announced_eb` size in a produced RB
//! header (the CIP-0164 EB-announcement fast pulse).
//!
//! Sets `leios.announce_eb_size = Linear { scale_num, scale_den, offset }`; the
//! producer applies it via
//! [`EbSizePolicy::apply`](crate::behaviour::tree::control::EbSizePolicy::apply)
//! when it bakes `announced_eb = (hash, size)` into the header. Because the size
//! lives inside the signed header, this is a production-time lie (unlike the
//! send-time `lie-about-eb-size` on the offer path). With `offset = 0`,
//! `scale_num = 0` yields the `size = 0` connection-drop probe (the policy is
//! linear, so a nonzero `offset` still adds through: size becomes `offset`).
//! Returns `Running` while installed.

use crate::behaviour::tree::actions::LeafAction;
use crate::behaviour::tree::control::{ControlSignal, EbSizePolicy};
use crate::behaviour::tree::env::TickCtx;
use crate::behaviour::tree::Status;

/// Installs the linear `announced_eb` size rewrite policy.
#[derive(Debug, Clone, Copy)]
pub struct AnnounceSizeLie {
    scale_num: u32,
    /// Clamped to `>= 1` (a `0` denominator is interpreted as `1`).
    scale_den: u32,
    offset: i32,
}

impl AnnounceSizeLie {
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

impl LeafAction for AnnounceSizeLie {
    fn contribute(&mut self, _ctx: &TickCtx, out: &mut ControlSignal) -> Status {
        out.leios.announce_eb_size = self.policy();
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

    fn installed_policy(action: &mut AnnounceSizeLie) -> EbSizePolicy {
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
        // The announce-path policy is set; the offer-path policy is untouched.
        assert_eq!(out.leios.offer_eb_size, EbSizePolicy::Honest);
        out.leios.announce_eb_size
    }

    #[test]
    fn size_zero_is_the_connection_drop_probe() {
        assert_eq!(installed_policy(&mut AnnounceSizeLie::new(0, 1, 0)).apply(9999), 0);
    }

    #[test]
    fn doubling_over_declares() {
        assert_eq!(installed_policy(&mut AnnounceSizeLie::new(2, 1, 0)).apply(1000), 2000);
    }

    #[test]
    fn honest_default_leaves_announce_size_untouched() {
        assert_eq!(ControlSignal::default().leios.announce_eb_size, EbSizePolicy::Honest);
    }

    #[test]
    fn installs_linear_policy() {
        assert_eq!(
            installed_policy(&mut AnnounceSizeLie::new(3, 4, -5)),
            EbSizePolicy::Linear {
                scale_num: 3,
                scale_den: 4,
                offset: -5,
            }
        );
    }
}
