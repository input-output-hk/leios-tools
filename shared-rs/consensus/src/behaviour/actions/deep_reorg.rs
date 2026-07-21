//! `deep-reorg` — periodically force a self-reorg.
//!
//! On every slot that is a multiple of `every_slots`, sets
//! `praos.reorg_depth = Some(depth)`, asking the actuator to roll the adopted
//! chain back `depth` blocks and fork. Periodicity self-gates off the slot (the
//! BT grammar has no modulo), so this is an action, not a `Condition`. Returns
//! `Running` while installed regardless of whether this slot is due.

use crate::behaviour::tree::actions::LeafAction;
use crate::behaviour::tree::control::ControlSignal;
use crate::behaviour::tree::env::{ConsensusCtx, TickCtx};
use crate::behaviour::tree::Status;

/// Forces a `depth`-block reorg every `every_slots` slots.
#[derive(Debug, Clone, Copy)]
pub struct DeepReorg {
    every_slots: u64,
    depth: u64,
    /// Last slot a reorg fired, so a slot ticked more than once doesn't fire
    /// twice. Starts at `u64::MAX` (no slot has fired).
    last_fired: u64,
}

impl DeepReorg {
    pub fn new(every_slots: u64, depth: u64) -> Self {
        Self {
            every_slots: every_slots.max(1),
            depth,
            last_fired: u64::MAX,
        }
    }

    /// Whether a reorg is due at `slot` (and record it). Slot 0 and zero depth
    /// are inert.
    fn due(&mut self, slot: u64) -> bool {
        if self.depth == 0 || slot == 0 {
            return false;
        }
        if slot.is_multiple_of(self.every_slots) && slot != self.last_fired {
            self.last_fired = slot;
            return true;
        }
        false
    }
}

impl LeafAction<ConsensusCtx, ControlSignal> for DeepReorg {
    fn contribute(&mut self, ctx: &TickCtx, out: &mut ControlSignal) -> Status {
        if self.due(ctx.state.current_slot) {
            out.praos.reorg_depth = Some(self.depth);
        }
        Status::Running
    }

    fn reset(&mut self) {
        // Re-arm so a re-selected subtree can fire on a slot it already fired
        // on before being halted.
        self.last_fired = u64::MAX;
    }

    fn set_param(&mut self, field: &str, value: &toml::Value) {
        // Only the named field changes; `last_fired` (run-state) is preserved.
        if let Some(v) = value.as_integer() {
            let v = v.max(0) as u64;
            match field {
                "every_slots" => self.every_slots = v.max(1),
                "depth" => self.depth = v,
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::behaviour::tree::env::{DynamicEnv, NativeChainState};

    fn reorg_at(action: &mut DeepReorg, slot: u64) -> Option<u64> {
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
        out.praos.reorg_depth
    }

    #[test]
    fn fires_every_n_slots_once_each() {
        let mut a = DeepReorg::new(50, 10);
        assert_eq!(reorg_at(&mut a, 1), None);
        assert_eq!(reorg_at(&mut a, 49), None);
        assert_eq!(reorg_at(&mut a, 50), Some(10));
        // Ticking the same slot again does not re-fire.
        assert_eq!(reorg_at(&mut a, 50), None);
        assert_eq!(reorg_at(&mut a, 100), Some(10));
    }

    #[test]
    fn zero_depth_is_inert() {
        let mut a = DeepReorg::new(10, 0);
        for slot in [0, 10, 20, 100] {
            assert_eq!(reorg_at(&mut a, slot), None);
        }
    }

    #[test]
    fn slot_zero_never_fires() {
        let mut a = DeepReorg::new(1, 5);
        assert_eq!(reorg_at(&mut a, 0), None);
        assert_eq!(reorg_at(&mut a, 1), Some(5));
    }

    #[test]
    fn set_param_changes_depth_preserving_fire_state() {
        let mut a = DeepReorg::new(50, 10);
        assert_eq!(reorg_at(&mut a, 50), Some(10)); // fires; last_fired = 50
        a.set_param("depth", &toml::Value::Integer(20));
        // Same slot must NOT re-fire — run-state (last_fired) is preserved.
        assert_eq!(reorg_at(&mut a, 50), None);
        // The next due slot fires with the new depth.
        assert_eq!(reorg_at(&mut a, 100), Some(20));
        // every_slots is retunable too, clamped to >= 1.
        a.set_param("every_slots", &toml::Value::Integer(0));
        assert_eq!(reorg_at(&mut a, 101), Some(20));
    }
}
