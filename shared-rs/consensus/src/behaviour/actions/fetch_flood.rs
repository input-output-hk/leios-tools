//! `fetch-flood` (T22) — re-request recently-announced EBs over LeiosFetch.
//!
//! Sets `leios.fetch_flood_rate` and `leios.fetch_flood_window_slots` on the
//! control signal, so the consumer re-issues `MsgLeiosBlockRequest` for every EB
//! announced within the last `window_slots` slots, `rate` times per second, to
//! every connected upstream — **even after it already holds the body**. Each
//! in-window EB is flooded independently, so N EBs in the window produce
//! N × `rate` requests per second; an EB stops being re-requested once it falls
//! out of the `window_slots` window.
//!
//! This is the preliminary T22 method (`threats/t22/attack-plan.md`): overwhelm
//! a node with repeated / known-info fetch requests so honest requests are left
//! unanswered, delaying EB propagation. This action only publishes the rate and
//! window on the control signal; the consumer's tick loop enumerates the
//! in-window announced EBs and fans the requests out. Returns `Running` while
//! installed.

use crate::behaviour::tree::actions::LeafAction;
use crate::behaviour::tree::control::ControlSignal;
use crate::behaviour::tree::env::{ConsensusCtx, TickCtx};
use crate::behaviour::tree::Status;

/// Re-request each in-window announced EB `rate` times/sec for `window_slots`.
#[derive(Debug, Clone, Copy)]
pub struct FetchFlood {
    /// LeiosFetch requests per second, per in-window announced EB.
    pub rate: u32,
    /// Slots after announcement to keep re-requesting an EB.
    pub window_slots: u64,
}

impl LeafAction<ConsensusCtx, ControlSignal> for FetchFlood {
    fn contribute(&mut self, _ctx: &TickCtx, out: &mut ControlSignal) -> Status {
        // At least one request per second while armed; a zero rate would install
        // the behaviour but flood nothing, which is never the intent.
        out.leios.fetch_flood_rate = self.rate.max(1);
        out.leios.fetch_flood_window_slots = self.window_slots;
        Status::Running
    }

    /// Live-retune the rate / window without rebuilding the tree.
    fn set_param(&mut self, field: &str, value: &toml::Value) {
        let Some(v) = value.as_integer() else {
            return;
        };
        match field {
            // Stored raw; `contribute` clamps to >= 1 at actuation.
            "rate" => self.rate = v.clamp(0, u32::MAX as i64) as u32,
            "window_slots" => self.window_slots = v.max(0) as u64,
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::behaviour::tree::env::{DynamicEnv, NativeChainState};

    fn ctx<'a>(env: &'a DynamicEnv, state: &'a NativeChainState) -> TickCtx<'a> {
        TickCtx {
            env,
            state,
            seed: 0,
            action_params: None,
        }
    }

    #[test]
    fn sets_rate_and_window() {
        let env = DynamicEnv::new();
        let state = NativeChainState::default();
        let mut out = ControlSignal::default();
        let s = FetchFlood {
            rate: 2,
            window_slots: 80,
        }
        .contribute(&ctx(&env, &state), &mut out);
        assert_eq!(s, Status::Running);
        assert_eq!(out.leios.fetch_flood_rate, 2);
        assert_eq!(out.leios.fetch_flood_window_slots, 80);

        // A zero rate is clamped up to a single request/sec while armed.
        let mut out2 = ControlSignal::default();
        FetchFlood {
            rate: 0,
            window_slots: 40,
        }
        .contribute(&ctx(&env, &state), &mut out2);
        assert_eq!(out2.leios.fetch_flood_rate, 1);
        assert_eq!(out2.leios.fetch_flood_window_slots, 40);

        // Honest default floods nothing.
        assert_eq!(ControlSignal::default().leios.fetch_flood_rate, 0);
    }

    #[test]
    fn set_param_retunes() {
        let mut a = FetchFlood {
            rate: 2,
            window_slots: 80,
        };
        a.set_param("rate", &toml::Value::Integer(5));
        a.set_param("window_slots", &toml::Value::Integer(120));
        assert_eq!(a.rate, 5);
        assert_eq!(a.window_slots, 120);
        // Negatives clamp; unknown fields ignored.
        a.set_param("rate", &toml::Value::Integer(-3));
        a.set_param("nonsense", &toml::Value::Integer(9));
        assert_eq!(a.rate, 0);
        assert_eq!(a.window_slots, 120);
    }
}
