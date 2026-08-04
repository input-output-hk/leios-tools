//! `rb-header-equivocator` — produce `ways` RB variants and route a different
//! one to each peer bucket (CIP-0164 RB-header equivocation).
//!
//! Sets `praos.production = Equivocate { ways }` (the wrapper signs `ways`
//! variants) and `praos.outbound = EquivocateRouting { slot, ways, seed }` (the
//! per-peer send actuator routes each peer's bucket a distinct variant). The
//! bucket assignment itself ([`equivocation_bucket`]) is a deterministic lookup
//! the actuator performs — not a decision — so it lives here for reuse and is
//! unit-tested below.

use crate::behaviour::tree::actions::LeafAction;
use crate::behaviour::tree::control::{ControlSignal, OutboundControl};
use crate::behaviour::tree::env::{ConsensusCtx, TickCtx};
use crate::behaviour::tree::Status;
use crate::behaviour::RbProductionStrategy;
use crate::peer::PeerId;

/// Equivocates RB headers `ways`-ways, seeded for deterministic peer bucketing.
#[derive(Debug, Clone, Copy)]
pub struct RbHeaderEquivocator {
    ways: u8,
    seed: u64,
}

impl RbHeaderEquivocator {
    /// `ways` is clamped to a minimum of 2 (1 degenerates to honest).
    pub fn new(ways: u8, seed: u64) -> Self {
        Self {
            ways: ways.max(2),
            seed,
        }
    }
}

impl LeafAction<ConsensusCtx, ControlSignal> for RbHeaderEquivocator {
    fn contribute(&mut self, ctx: &TickCtx, out: &mut ControlSignal) -> Status {
        out.praos.production = RbProductionStrategy::Equivocate { ways: self.ways };
        out.praos.outbound = OutboundControl::EquivocateRouting {
            slot: ctx.state.current_slot,
            ways: self.ways,
            seed: self.seed,
        };
        Status::Running
    }

    fn set_param(&mut self, field: &str, value: &toml::Value) {
        if field == "ways" {
            if let Some(v) = value.as_integer() {
                // Clamp to >= 2, same as `new`: a value < 2 wouldn't equivocate,
                // so this action can't be tuned down to honest (use a Selector
                // with an honest leaf for that).
                self.ways = v.clamp(2, u8::MAX as i64) as u8;
            }
        }
    }
}

/// Deterministic peer-to-bucket assignment: `blake2b_8(seed || peer) % ways`,
/// in `0..ways`. The send actuator routes the variant at this index to `peer`.
/// Mixing through Blake2b keeps adjacent peer ids out of adjacent buckets.
pub fn equivocation_bucket(seed: u64, ways: u8, peer: PeerId) -> usize {
    let ways = ways.max(2);
    let mut h = blake2b_simd::Params::new().hash_length(8).to_state();
    h.update(&seed.to_le_bytes());
    h.update(&peer.0.to_le_bytes());
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&h.finalize().as_bytes()[..8]);
    (u64::from_le_bytes(buf) % ways as u64) as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::behaviour::tree::env::{DynamicEnv, NativeChainState};

    fn run(action: &mut RbHeaderEquivocator, slot: u64) -> (Status, ControlSignal) {
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
        (s, out)
    }

    #[test]
    fn emits_equivocate_and_routing_for_current_slot() {
        let (s, out) = run(&mut RbHeaderEquivocator::new(3, 0xABCD), 100);
        assert_eq!(s, Status::Running);
        assert_eq!(
            out.praos.production,
            RbProductionStrategy::Equivocate { ways: 3 }
        );
        assert_eq!(
            out.praos.outbound,
            OutboundControl::EquivocateRouting {
                slot: 100,
                ways: 3,
                seed: 0xABCD,
            }
        );
    }

    #[test]
    fn ways_clamps_to_minimum_of_two() {
        let (_, out) = run(&mut RbHeaderEquivocator::new(1, 0), 1);
        assert_eq!(
            out.praos.production,
            RbProductionStrategy::Equivocate { ways: 2 }
        );
    }

    #[test]
    fn set_param_overrides_ways_with_clamp() {
        let mut a = RbHeaderEquivocator::new(2, 0);
        a.set_param("ways", &toml::Value::Integer(5));
        assert_eq!(
            run(&mut a, 1).1.praos.production,
            RbProductionStrategy::Equivocate { ways: 5 }
        );
        // Clamps to >= 2.
        a.set_param("ways", &toml::Value::Integer(1));
        assert_eq!(
            run(&mut a, 1).1.praos.production,
            RbProductionStrategy::Equivocate { ways: 2 }
        );
        // Set a known-good baseline, then confirm two no-op cases leave it intact:
        // an unknown field, and the right field with a wrong-typed value.
        a.set_param("ways", &toml::Value::Integer(3));
        a.set_param("nope", &toml::Value::Integer(9)); // unknown field → ignored
        a.set_param("ways", &toml::Value::Boolean(true)); // wrong type → ignored
        assert_eq!(
            run(&mut a, 1).1.praos.production,
            RbProductionStrategy::Equivocate { ways: 3 } // still the baseline
        );
    }

    #[test]
    fn buckets_are_deterministic_and_in_range() {
        let seed = 0xDEADBEEF;
        let ways = 4;
        for p in 0..200u64 {
            let b = equivocation_bucket(seed, ways, PeerId(p));
            assert!(b < ways as usize);
            // Deterministic: same inputs → same bucket.
            assert_eq!(b, equivocation_bucket(seed, ways, PeerId(p)));
        }
    }

    #[test]
    fn buckets_partition_peers_across_all_ways() {
        let seed = 0x1234;
        let ways = 3u8;
        let mut counts = [0usize; 3];
        for p in 0..300u64 {
            counts[equivocation_bucket(seed, ways, PeerId(p))] += 1;
        }
        // Every bucket gets a non-trivial share (deterministic spread).
        for c in counts {
            assert!(c > 50, "uneven bucketing: {counts:?}");
        }
    }
}
