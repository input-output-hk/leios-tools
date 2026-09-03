//! `vote-flood` — flood the committee with votes over fabricated RB hashes.
//!
//! Sets `leios.vote_flood_count = count`, so the node emits `count` votes per
//! tick over *fresh, distinct* announcing-RB hashes it never saw — bypassing the
//! honest election gate (saw the announcement → fetched + validated the EB →
//! in-window). Voting requires no stake, only a current committee seat + BLS
//! key, so each vote is a real signature a receiver admits on
//! membership+signature.
//!
//! Probes the "valid remote votes accumulate without a bounded lifecycle" threat:
//! each distinct hash forces a new retained entry (`seenVotes` / `pointStates`)
//! on the receiver, which exposes no pruning/GC — monotonic node-wide growth
//! (memory-exhaustion vector). Exact-duplicate votes are suppressed downstream,
//! hence distinct hashes. Returns `Running` while installed.

use crate::behaviour::tree::actions::LeafAction;
use crate::behaviour::tree::control::ControlSignal;
use crate::behaviour::tree::env::{ConsensusCtx, TickCtx};
use crate::behaviour::tree::Status;

/// Emit `count` votes over fabricated distinct hashes per tick.
#[derive(Debug, Clone, Copy)]
pub struct VoteFlood {
    /// Fabricated votes per tick (`>= 1`).
    pub count: u32,
}

impl LeafAction<ConsensusCtx, ControlSignal> for VoteFlood {
    fn contribute(&mut self, _ctx: &TickCtx, out: &mut ControlSignal) -> Status {
        // At least one; the actuator also clamps, but keep the control honest.
        out.leios.vote_flood_count = self.count.max(1);
        Status::Running
    }

    /// Live-retune the flood count without rebuilding the tree.
    fn set_param(&mut self, field: &str, value: &toml::Value) {
        let Some(v) = value.as_integer() else {
            return;
        };
        if field == "count" {
            // Stored raw; `contribute` clamps to >= 1 at actuation.
            self.count = v.clamp(0, u32::MAX as i64) as u32;
        }
    }
}
