//! `drop-inbound-peers` — stochastically reset inbound peer connections.
//!
//! Each slot, with `probability`, sets `praos.drop_inbound = true`, asking the
//! actuator to drop every inbound peer so the remote reconnects and re-runs
//! ChainSync intersection. The draw is a deterministic per-`(seed, slot)` hash
//! (no clock / OS entropy), so it replays identically. Returns `Running` while
//! installed.

use crate::behaviour::tree::actions::LeafAction;
use crate::behaviour::tree::control::ControlSignal;
use crate::behaviour::tree::env::{ConsensusCtx, TickCtx};
use crate::behaviour::tree::Status;

/// Resets inbound peers each slot with the configured probability.
#[derive(Debug, Clone, Copy)]
pub struct DropInboundPeers {
    seed: u64,
    /// Per-slot probability, clamped to `[0, 1]`.
    probability: f64,
}

impl DropInboundPeers {
    pub fn new(seed: u64, probability: f64) -> Self {
        Self {
            seed,
            probability: probability.clamp(0.0, 1.0),
        }
    }

    /// Deterministic per-`(seed, slot)` draw against `probability`.
    fn draws(&self, slot: u64) -> bool {
        if self.probability <= 0.0 || slot == 0 {
            return false;
        }
        // Bypass the draw at the top end: `u64 / u64::MAX as f64` can round to
        // exactly 1.0, and `<` would then refuse to drop on the rare maxed-out
        // hash — violating "always drop at probability = 1.0".
        if self.probability >= 1.0 {
            return true;
        }
        let mut h = blake2b_simd::Params::new().hash_length(8).to_state();
        h.update(&self.seed.to_le_bytes());
        h.update(&slot.to_le_bytes());
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&h.finalize().as_bytes()[..8]);
        let draw = (u64::from_le_bytes(buf) as f64) / (u64::MAX as f64);
        draw < self.probability
    }
}

impl LeafAction<ConsensusCtx, ControlSignal> for DropInboundPeers {
    fn contribute(&mut self, ctx: &TickCtx, out: &mut ControlSignal) -> Status {
        if self.draws(ctx.state.current_slot) {
            out.praos.drop_inbound = true;
        }
        Status::Running
    }

    fn set_param(&mut self, field: &str, value: &toml::Value) {
        if field == "probability" {
            // Accept a float or an integer; clamp to [0, 1] as `new` does.
            let p = value
                .as_float()
                .or_else(|| value.as_integer().map(|i| i as f64));
            if let Some(p) = p {
                self.probability = p.clamp(0.0, 1.0);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::behaviour::tree::env::{DynamicEnv, NativeChainState};

    fn drops_at(action: &mut DropInboundPeers, slot: u64) -> bool {
        let env = DynamicEnv::new();
        let state = NativeChainState {
            current_slot: slot,
            ..Default::default()
        };
        let ctx = TickCtx {
            env: &env,
            state: &state,
            seed: 0,
            action_params: None,
        };
        let mut out = ControlSignal::default();
        let s = action.contribute(&ctx, &mut out);
        assert_eq!(s, Status::Running);
        out.praos.drop_inbound
    }

    #[test]
    fn never_drops_at_zero_probability() {
        let mut a = DropInboundPeers::new(123, 0.0);
        for slot in 1..50 {
            assert!(!drops_at(&mut a, slot));
        }
    }

    #[test]
    fn always_drops_at_probability_one() {
        let mut a = DropInboundPeers::new(123, 1.0);
        for slot in 1..50 {
            assert!(drops_at(&mut a, slot));
        }
    }

    #[test]
    fn deterministic_for_a_given_seed_and_slot() {
        let mut a = DropInboundPeers::new(0xFEED, 0.5);
        let mut b = DropInboundPeers::new(0xFEED, 0.5);
        for slot in 1..200 {
            assert_eq!(drops_at(&mut a, slot), drops_at(&mut b, slot));
        }
    }
}
