//! `announce-slot-skew` — fabricate an EB announcement with a skewed header slot.
//!
//! Like `fake-eb-announce` (sets `leios.fake_announce = true`, so the node
//! emits a `MsgLeiosBlockAnnouncement` for a fabricated EB every slot,
//! regardless of stake/lottery), but additionally offsets the fabricated
//! header's **slot** by `slot_offset` (signed: `> 0` = future, `< 0` = far
//! past). This probes the announcement freshness/timing checks that the Haskell
//! diffusion PR does **not** yet implement — the ChainSync-`MsgRollForward`-style
//! "not from the (near/far) future", "not from the far past", and "age ≤ L"
//! bounds. Near-future → expected `threadDelay`; far-future → expected
//! disconnect; far-past → expected reject. Until those gates land, a skewed
//! announcement diffuses. Returns `Running` while installed.

use crate::behaviour::tree::actions::LeafAction;
use crate::behaviour::tree::control::ControlSignal;
use crate::behaviour::tree::env::TickCtx;
use crate::behaviour::tree::Status;

/// Announce with a slot offset applied to the fabricated header.
#[derive(Debug, Clone, Copy, Default)]
pub struct AnnounceSlotSkew {
    /// Signed offset added to the current slot for the fabricated header
    /// (`> 0` future, `< 0` past).
    pub slot_offset: i64,
}

impl LeafAction for AnnounceSlotSkew {
    fn contribute(&mut self, _ctx: &TickCtx, out: &mut ControlSignal) -> Status {
        out.leios.fake_announce = true;
        out.leios.announce_slot_offset = self.slot_offset;
        Status::Running
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::behaviour::tree::env::{DynamicEnv, NativeChainState};

    #[test]
    fn sets_fake_announce_and_offset() {
        let env = DynamicEnv::new();
        let state = NativeChainState::default();
        let ctx = TickCtx {
            env: &env,
            state: &state,
            seed: 0,
            action_params: None,
        };
        let mut out = ControlSignal::default();
        let s = AnnounceSlotSkew { slot_offset: 40 }.contribute(&ctx, &mut out);
        assert_eq!(s, Status::Running);
        assert!(out.leios.fake_announce);
        assert_eq!(out.leios.announce_slot_offset, 40);
        // Honest default never fabricates or skews.
        assert!(!ControlSignal::default().leios.fake_announce);
        assert_eq!(ControlSignal::default().leios.announce_slot_offset, 0);
    }
}
