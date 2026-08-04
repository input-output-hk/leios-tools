//! `fake-eb-announce` — announce an EB without winning the slot.
//!
//! Sets `leios.fake_announce = true`. The node emits a
//! `MsgLeiosBlockAnnouncement` for a fabricated EB every slot the action is
//! active, **regardless of stake or the production lottery** — i.e. it announces
//! for elections it was never elected to. Real nodes reject the header at the
//! VRF/KES check (only the elected slot leader can sign a valid RB header);
//! net-rs's fake validation may accept it, so this is the sharpest test of the
//! announce-path authorization gate. Returns `Running` while installed.

use crate::behaviour::tree::actions::LeafAction;
use crate::behaviour::tree::control::ControlSignal;
use crate::behaviour::tree::env::{ConsensusCtx, TickCtx};
use crate::behaviour::tree::Status;

/// Announce without an election win. No parameters.
#[derive(Debug, Clone, Copy, Default)]
pub struct FakeAnnounce;

impl LeafAction<ConsensusCtx, ControlSignal> for FakeAnnounce {
    fn contribute(&mut self, _ctx: &TickCtx, out: &mut ControlSignal) -> Status {
        out.leios.fake_announce = true;
        Status::Running
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::behaviour::tree::env::{DynamicEnv, NativeChainState};

    #[test]
    fn sets_fake_announce_flag() {
        let env = DynamicEnv::new();
        let state = NativeChainState::default();
        let ctx = TickCtx {
            env: &env,
            state: &state,
            seed: 0,
            action_params: None,
        };
        let mut out = ControlSignal::default();
        let s = FakeAnnounce.contribute(&ctx, &mut out);
        assert_eq!(s, Status::Running);
        assert!(out.leios.fake_announce);
        // Honest default never fabricates announcements.
        assert!(!ControlSignal::default().leios.fake_announce);
    }
}
