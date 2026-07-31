//! `announce-equivocate` — emit two conflicting EB announcements per election.
//!
//! Sets `leios.announce_equivocate = true`. When the producer announces an EB
//! it emits a *second* `MsgLeiosBlockAnnouncement` whose header commits to a
//! different `announced_eb` for the same slot — an OCIN equivocation (the ≤ 2
//! distinct-announcements-per-election rule). Drives the equivocation-detection
//! path on honest peers. Returns `Running` while installed.

use crate::behaviour::tree::actions::LeafAction;
use crate::behaviour::tree::control::ControlSignal;
use crate::behaviour::tree::env::{ConsensusCtx, TickCtx};
use crate::behaviour::tree::Status;

/// Emit a second, conflicting announcement. No parameters.
#[derive(Debug, Clone, Copy, Default)]
pub struct AnnounceEquivocate;

impl LeafAction<ConsensusCtx, ControlSignal> for AnnounceEquivocate {
    fn contribute(&mut self, _ctx: &TickCtx, out: &mut ControlSignal) -> Status {
        out.leios.announce_equivocate = true;
        Status::Running
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::behaviour::tree::env::{DynamicEnv, NativeChainState};

    #[test]
    fn sets_equivocate_flag() {
        let env = DynamicEnv::new();
        let state = NativeChainState::default();
        let ctx = TickCtx {
            env: &env,
            state: &state,
            seed: 0,
            action_params: None,
        };
        let mut out = ControlSignal::default();
        let s = AnnounceEquivocate.contribute(&ctx, &mut out);
        assert_eq!(s, Status::Running);
        assert!(out.leios.announce_equivocate);
        // Honest default announces once.
        assert!(!ControlSignal::default().leios.announce_equivocate);
    }
}
