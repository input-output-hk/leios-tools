//! `tx-flood` — flood the local node's tx generator at a high rate.
//!
//! Sets `mempool.tx_flood_rate` (txs/sec). The net-node actuator drives the tx
//! generator at this rate while the action is active, so the node injects
//! garbage txs (or replays its magazine, if `--tx-source` is set) far faster
//! than the network drains them. This overflows the count-bounded mempool
//! (evict-oldest), displacing honest txs before they can be included — the
//! T25 / censorship-by-displacement probe on the net-rs local cluster.
//!
//! `rate` is an integer (txs/sec); fractional precision is irrelevant for a
//! flood and keeps the control signal `Eq`. Returns `Running` while installed.

use crate::behaviour::tree::actions::LeafAction;
use crate::behaviour::tree::control::ControlSignal;
use crate::behaviour::tree::env::{ConsensusCtx, TickCtx};
use crate::behaviour::tree::Status;

/// Drive the tx generator at `rate` txs/sec, optionally holding until a
/// coordinated fire slot.
#[derive(Debug, Clone, Copy)]
pub struct TxFlood {
    pub rate: u32,
    /// Optional "fire at slot X" gate. `None` fires immediately at `rate`; a
    /// value holds the generator until `current_slot() >= X` (#1068 shoal volley).
    pub fire_at_slot: Option<u64>,
}

impl LeafAction<ConsensusCtx, ControlSignal> for TxFlood {
    fn contribute(&mut self, _ctx: &TickCtx, out: &mut ControlSignal) -> Status {
        out.mempool.tx_flood_rate = self.rate;
        out.mempool.fire_at_slot = self.fire_at_slot;
        Status::Running
    }

    /// Live-retune the flood rate or the coordinated fire slot without
    /// rebuilding the tree.
    fn set_param(&mut self, field: &str, value: &toml::Value) {
        match field {
            "rate" => {
                if let Some(v) = value.as_integer() {
                    self.rate = v.clamp(0, u32::MAX as i64) as u32;
                }
            }
            "fire_at_slot" => {
                if let Some(v) = value.as_integer() {
                    self.fire_at_slot = (v >= 0).then_some(v as u64);
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::behaviour::tree::env::{DynamicEnv, NativeChainState};

    #[test]
    fn sets_flood_rate() {
        let env = DynamicEnv::new();
        let state = NativeChainState::default();
        let ctx = TickCtx {
            env: &env,
            state: &state,
            seed: 0,
            action_params: None,
        };
        let mut out = ControlSignal::default();
        let s = TxFlood {
            rate: 1000,
            fire_at_slot: Some(42),
        }
        .contribute(&ctx, &mut out);
        assert_eq!(s, Status::Running);
        assert_eq!(out.mempool.tx_flood_rate, 1000);
        assert_eq!(out.mempool.fire_at_slot, Some(42));
        // Honest default floods nothing and holds no gate.
        assert_eq!(ControlSignal::default().mempool.tx_flood_rate, 0);
        assert_eq!(ControlSignal::default().mempool.fire_at_slot, None);
    }

    #[test]
    fn set_param_retunes_rate_and_fire_slot() {
        let mut a = TxFlood {
            rate: 1000,
            fire_at_slot: None,
        };
        a.set_param("fire_at_slot", &toml::Value::Integer(123));
        assert_eq!(a.fire_at_slot, Some(123));
        a.set_param("fire_at_slot", &toml::Value::Integer(-1)); // negative ignored -> cleared
        assert_eq!(a.fire_at_slot, None);
        a.set_param("rate", &toml::Value::Integer(5000));
        assert_eq!(a.rate, 5000);
        a.set_param("nope", &toml::Value::Integer(1));
        assert_eq!(a.rate, 5000);
    }
}
