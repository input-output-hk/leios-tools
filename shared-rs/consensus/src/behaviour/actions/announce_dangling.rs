//! `announce-dangling` — announce an EB but never serve its body.
//!
//! Sets `leios.announce_dangling = true`. The producer still emits
//! `MsgLeiosBlockAnnouncement` (the header's `announced_eb` commitment) but
//! suppresses the EB-body `BlockOffer`/inject, so peers try to fetch a body
//! that never comes, waste the fetch window, and can miss the voting deadline.
//! A censorship/DoS via the announcement itself — no EB body required. Returns
//! `Running` while installed.

use crate::behaviour::tree::actions::LeafAction;
use crate::behaviour::tree::control::ControlSignal;
use crate::behaviour::tree::env::TickCtx;
use crate::behaviour::tree::Status;

/// Announce without serving the body. No parameters.
#[derive(Debug, Clone, Copy, Default)]
pub struct AnnounceDangling;

impl LeafAction for AnnounceDangling {
    fn contribute(&mut self, _ctx: &TickCtx, out: &mut ControlSignal) -> Status {
        out.leios.announce_dangling = true;
        Status::Running
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::behaviour::tree::env::{DynamicEnv, NativeChainState};

    #[test]
    fn sets_dangling_flag() {
        let env = DynamicEnv::new();
        let state = NativeChainState::default();
        let ctx = TickCtx {
            env: &env,
            state: &state,
            seed: 0,
            action_params: None,
        };
        let mut out = ControlSignal::default();
        let s = AnnounceDangling.contribute(&ctx, &mut out);
        assert_eq!(s, Status::Running);
        assert!(out.leios.announce_dangling);
        // Honest default serves the body.
        assert!(!ControlSignal::default().leios.announce_dangling);
    }
}
