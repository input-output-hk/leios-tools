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
        let s = VoteFlood { count: 500 }.contribute(&ctx, &mut out);
        assert_eq!(s, Status::Running);
        assert_eq!(out.leios.vote_flood_count, 500);
        // A zero count is clamped to a single vote.
        let mut out2 = ControlSignal::default();
        VoteFlood { count: 0 }.contribute(&ctx, &mut out2);
        assert_eq!(out2.leios.vote_flood_count, 1);
        // Honest default floods nothing.
        assert_eq!(ControlSignal::default().leios.vote_flood_count, 0);
    }

    #[test]
    fn set_param_retunes_count() {
        let mut a = VoteFlood { count: 100 };
        a.set_param("count", &toml::Value::Integer(500));
        assert_eq!(a.count, 500);
        // Negative count clamps to 0 (contribute then lifts to >= 1).
        a.set_param("count", &toml::Value::Integer(-5));
        assert_eq!(a.count, 0);
        // Unknown fields are ignored.
        a.set_param("count", &toml::Value::Integer(7));
        a.set_param("slot_offset", &toml::Value::Integer(40));
        assert_eq!(a.count, 7);
    }
}
