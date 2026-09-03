//! `eb-burst` — withhold a batch of EBs, then release them simultaneously (T23).
//!
//! Implements the "withhold then release large number of EBs" threat. Sets three
//! Leios control fields so the actuator runs a two-phase attack:
//!   * `eb_burst_withhold_slots` — the silent accumulation window (slots).
//!   * `eb_burst_count` — how many Dummy EBs to fabricate + buffer (`>= 1`).
//!   * `eb_burst_n_txs` — fabricated tx-closure size per buffered EB.
//!
//! During the withhold window net-rs fabricates + pins `count` Dummy EBs (each an
//! `n_txs`-tx *servable* closure) into a release buffer **without announcing**;
//! when the window elapses it flushes every buffered `(announcement, body,
//! closure)` in one tick — a concentrated fetch storm of old-but-servable EBs
//! that probes the (proposed, not-yet-landed) freshest-first delivery discipline.
//! Returns `Running` while installed.

use crate::behaviour::tree::actions::LeafAction;
use crate::behaviour::tree::control::ControlSignal;
use crate::behaviour::tree::env::{ConsensusCtx, TickCtx};
use crate::behaviour::tree::Status;

/// Withhold `count` Dummy EBs over `withhold_slots`, then release them together.
#[derive(Debug, Clone, Copy)]
pub struct EbBurst {
    /// Silent accumulation window in slots before the single-tick release.
    pub withhold_slots: u32,
    /// Dummy EBs to fabricate + buffer, released together (`>= 1`).
    pub count: u32,
    /// Fabricated tx-closure size per buffered EB.
    pub n_txs: u32,
}

impl LeafAction<ConsensusCtx, ControlSignal> for EbBurst {
    fn contribute(&mut self, _ctx: &TickCtx, out: &mut ControlSignal) -> Status {
        out.leios.eb_burst_withhold_slots = self.withhold_slots;
        // At least one; the actuator also clamps, but keep the control honest.
        out.leios.eb_burst_count = self.count.max(1);
        out.leios.eb_burst_n_txs = self.n_txs;
        Status::Running
    }

    /// Live-retune the withhold window / batch size / closure size without
    /// rebuilding the tree.
    fn set_param(&mut self, field: &str, value: &toml::Value) {
        let Some(v) = value.as_integer() else {
            return;
        };
        match field {
            // Stored raw; `contribute` clamps `count` to >= 1 at actuation.
            "withhold_slots" => self.withhold_slots = v.clamp(0, u32::MAX as i64) as u32,
            "count" => self.count = v.clamp(0, u32::MAX as i64) as u32,
            "n_txs" => self.n_txs = v.clamp(0, u32::MAX as i64) as u32,
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::behaviour::tree::env::{DynamicEnv, NativeChainState};

    #[test]
    fn sets_burst_fields() {
        let env = DynamicEnv::new();
        let state = NativeChainState::default();
        let ctx = TickCtx {
            env: &env,
            state: &state,
            seed: 0,
            action_params: None,
        };
        let mut out = ControlSignal::default();
        let s = EbBurst {
            withhold_slots: 20,
            count: 50,
            n_txs: 8,
        }
        .contribute(&ctx, &mut out);
        assert_eq!(s, Status::Running);
        assert_eq!(out.leios.eb_burst_withhold_slots, 20);
        assert_eq!(out.leios.eb_burst_count, 50);
        assert_eq!(out.leios.eb_burst_n_txs, 8);
        // A zero count is clamped to a single EB.
        let mut out2 = ControlSignal::default();
        EbBurst {
            withhold_slots: 0,
            count: 0,
            n_txs: 0,
        }
        .contribute(&ctx, &mut out2);
        assert_eq!(out2.leios.eb_burst_count, 1);
        // Honest default bursts nothing.
        assert_eq!(ControlSignal::default().leios.eb_burst_count, 0);
    }

    #[test]
    fn set_param_retunes_all_three() {
        let mut a = EbBurst {
            withhold_slots: 10,
            count: 10,
            n_txs: 4,
        };
        a.set_param("withhold_slots", &toml::Value::Integer(60));
        a.set_param("count", &toml::Value::Integer(500));
        a.set_param("n_txs", &toml::Value::Integer(16));
        assert_eq!(a.withhold_slots, 60);
        assert_eq!(a.count, 500);
        assert_eq!(a.n_txs, 16);
        // Negatives clamp to 0; unknown fields are ignored.
        a.set_param("count", &toml::Value::Integer(-5));
        a.set_param("bogus", &toml::Value::Integer(9));
        assert_eq!(a.count, 0);
    }
}
