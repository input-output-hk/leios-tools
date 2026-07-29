//! `announce-flood` — flood the network with fabricated EB announcements.
//!
//! Sets `leios.fake_announce = true` (fabricate without winning the slot) and
//! `leios.announce_flood_count = count`, so the node emits `count` *distinct*
//! `MsgLeiosBlockAnnouncement`s per tick (each fabrication draws a fresh random
//! issuer + EB hash). Optionally skews the slot via `slot_offset`.
//!
//! This is the announcement-channel analog of transaction flooding: it stresses
//! the receiver-side accounting (per CIP-0164 there are never more than ~10,000
//! elections younger than the immutable tip and ~50 active upstream peers, and
//! an honest server sends at most 2 announcements per election). The Haskell
//! diffusion PR does **not** yet implement junk-message detection or that
//! ≤2-per-election server discipline, so a high `count` is a DoS vector until
//! those gates land. Returns `Running` while installed.

use crate::behaviour::tree::actions::LeafAction;
use crate::behaviour::tree::control::ControlSignal;
use crate::behaviour::tree::env::TickCtx;
use crate::behaviour::tree::Status;

/// Emit `count` fabricated announcements per tick (junk-message DoS).
#[derive(Debug, Clone, Copy)]
pub struct AnnounceFlood {
    /// Fabricated announcements per tick (`>= 1`; `1` ≈ `fake-eb-announce`).
    pub count: u32,
    /// Signed slot offset applied to each fabricated header (see
    /// `announce-slot-skew`); `0` = current slot.
    pub slot_offset: i64,
}

impl LeafAction for AnnounceFlood {
    fn contribute(&mut self, _ctx: &TickCtx, out: &mut ControlSignal) -> Status {
        out.leios.fake_announce = true;
        // At least one; the actuator also clamps, but keep the control honest.
        out.leios.announce_flood_count = self.count.max(1);
        out.leios.announce_slot_offset = self.slot_offset;
        Status::Running
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::behaviour::tree::env::{DynamicEnv, NativeChainState};

    #[test]
    fn sets_flood_count() {
        let env = DynamicEnv::new();
        let state = NativeChainState::default();
        let ctx = TickCtx {
            env: &env,
            state: &state,
            seed: 0,
            action_params: None,
        };
        let mut out = ControlSignal::default();
        let s = AnnounceFlood {
            count: 500,
            slot_offset: 0,
        }
        .contribute(&ctx, &mut out);
        assert_eq!(s, Status::Running);
        assert!(out.leios.fake_announce);
        assert_eq!(out.leios.announce_flood_count, 500);
        // A zero count is clamped to a single fabrication.
        let mut out2 = ControlSignal::default();
        AnnounceFlood {
            count: 0,
            slot_offset: 0,
        }
        .contribute(&ctx, &mut out2);
        assert_eq!(out2.leios.announce_flood_count, 1);
        // Honest default floods nothing.
        assert_eq!(ControlSignal::default().leios.announce_flood_count, 0);
    }
}
