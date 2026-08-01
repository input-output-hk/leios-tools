//! `withhold-eb-block-announce` — suppress EB announcements for produced EBs.
//!
//! Sets `leios.withhold_announce = true`. Normal behaviour announces every
//! produced EB via `MsgLeiosBlockAnnouncement`; this action turns that off, so
//! the node still produces the EB and serves its body (`MsgLeiosBlockOffer`)
//! but never sends the fast discovery pulse — censoring the announcement path.
//! Distinct from `announce-dangling` (which withholds the *body*, not the
//! announcement). Returns `Running` while installed.

use crate::behaviour::tree::actions::LeafAction;
use crate::behaviour::tree::control::ControlSignal;
use crate::behaviour::tree::env::{ConsensusCtx, TickCtx};
use crate::behaviour::tree::Status;

/// Withhold the EB announcement. No parameters.
#[derive(Debug, Clone, Copy, Default)]
pub struct WithholdEbAnnounce;

impl LeafAction<ConsensusCtx, ControlSignal> for WithholdEbAnnounce {
    fn contribute(&mut self, _ctx: &TickCtx, out: &mut ControlSignal) -> Status {
        out.leios.withhold_announce = true;
        Status::Running
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::behaviour::tree::env::{DynamicEnv, NativeChainState};

    #[test]
    fn sets_withhold_flag() {
        let env = DynamicEnv::new();
        let state = NativeChainState::default();
        let ctx = TickCtx {
            env: &env,
            state: &state,
            seed: 0,
            action_params: None,
        };
        let mut out = ControlSignal::default();
        let s = WithholdEbAnnounce.contribute(&ctx, &mut out);
        assert_eq!(s, Status::Running);
        assert!(out.leios.withhold_announce);
        // Honest default announces (does not withhold).
        assert!(!ControlSignal::default().leios.withhold_announce);
    }
}
