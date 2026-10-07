//! `net-flood` (T35) — the isolated fetch-flood actuator signal.
//!
//! Unlike `fetch-flood`, which re-requests EBs over *this node's own* upstream
//! peers, net-flood drives an embedded flood engine that opens *its own* downstream
//! connections to named target pools and drains their serve capacity. It is only
//! acted on by a node configured `run_mode = flood-only` (an isolated attacker that
//! is not also a participating peer, so it doesn't distort the measurement).
//!
//! This leaf only publishes the parameters + an `active` flag on the control
//! signal; the node's actuator starts/stops the embedded net-flood engine when
//! `net_flood_active` flips. Returns `Running` while installed; deselecting it (or
//! `go_honest`) leaves the default signal (`active = false`), which stops the flood.
//! The pool→address registry and network magic live in the node's own config —
//! this action carries only pool *names*.

use crate::behaviour::tree::actions::LeafAction;
use crate::behaviour::tree::control::ControlSignal;
use crate::behaviour::tree::env::{ConsensusCtx, TickCtx};
use crate::behaviour::tree::Status;

/// Drive an embedded net-flood engine against named target pools.
#[derive(Debug, Clone)]
pub struct NetFlood {
    /// `"eb"` (LeiosFetch EB bodies) or `"praos"` (BlockFetch block ranges).
    pub attack: String,
    /// Target pool names (from the node's registry); empty = every pool.
    pub pools: Vec<String>,
    /// Downstream connections to open per relay.
    pub connections_per_relay: u32,
    /// praos only: block-range width per request.
    pub range_width: u32,
    /// praos only: blocks back from the tip to target.
    pub tip_offset: u32,
}

impl LeafAction<ConsensusCtx, ControlSignal> for NetFlood {
    fn contribute(&mut self, _ctx: &TickCtx, out: &mut ControlSignal) -> Status {
        out.leios.net_flood_active = true;
        out.leios.net_flood_attack = self.attack.clone();
        out.leios.net_flood_pools = self.pools.clone();
        // At least one connection while armed; a zero would install the behaviour
        // but open nothing, which is never the intent.
        out.leios.net_flood_connections_per_relay = self.connections_per_relay.max(1);
        out.leios.net_flood_range_width = self.range_width.max(1);
        out.leios.net_flood_tip_offset = self.tip_offset;
        Status::Running
    }

    /// Live-retune parameters without rebuilding the tree.
    fn set_param(&mut self, field: &str, value: &toml::Value) {
        match field {
            "attack" => {
                if let Some(s) = value.as_str() {
                    self.attack = s.to_string();
                }
            }
            "pools" => {
                if let Some(arr) = value.as_array() {
                    self.pools = arr
                        .iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect();
                }
            }
            "connections_per_relay" => {
                if let Some(v) = value.as_integer() {
                    self.connections_per_relay = v.clamp(0, u32::MAX as i64) as u32;
                }
            }
            "range_width" => {
                if let Some(v) = value.as_integer() {
                    self.range_width = v.clamp(0, u32::MAX as i64) as u32;
                }
            }
            "tip_offset" => {
                if let Some(v) = value.as_integer() {
                    self.tip_offset = v.clamp(0, u32::MAX as i64) as u32;
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

    fn ctx<'a>(env: &'a DynamicEnv, state: &'a NativeChainState) -> TickCtx<'a> {
        TickCtx {
            env,
            state,
            seed: 0,
            action_params: None,
        }
    }

    #[test]
    fn sets_params_and_active() {
        let env = DynamicEnv::new();
        let state = NativeChainState::default();
        let mut out = ControlSignal::default();
        let s = NetFlood {
            attack: "praos".to_string(),
            pools: vec!["IOG1".to_string(), "IOG2".to_string()],
            connections_per_relay: 150,
            range_width: 5,
            tip_offset: 3,
        }
        .contribute(&ctx(&env, &state), &mut out);
        assert_eq!(s, Status::Running);
        assert!(out.leios.net_flood_active);
        assert_eq!(out.leios.net_flood_attack, "praos");
        assert_eq!(out.leios.net_flood_pools, vec!["IOG1", "IOG2"]);
        assert_eq!(out.leios.net_flood_connections_per_relay, 150);
        assert_eq!(out.leios.net_flood_range_width, 5);
        assert_eq!(out.leios.net_flood_tip_offset, 3);

        // Zero connections clamp up to one while armed.
        let mut out2 = ControlSignal::default();
        NetFlood {
            attack: "eb".to_string(),
            pools: vec![],
            connections_per_relay: 0,
            range_width: 0,
            tip_offset: 0,
        }
        .contribute(&ctx(&env, &state), &mut out2);
        assert_eq!(out2.leios.net_flood_connections_per_relay, 1);
        assert_eq!(out2.leios.net_flood_range_width, 1);

        // Honest default is inactive.
        assert!(!ControlSignal::default().leios.net_flood_active);
    }

    #[test]
    fn set_param_retunes() {
        let mut a = NetFlood {
            attack: "eb".to_string(),
            pools: vec![],
            connections_per_relay: 100,
            range_width: 1,
            tip_offset: 3,
        };
        a.set_param("attack", &toml::Value::String("praos".to_string()));
        a.set_param("connections_per_relay", &toml::Value::Integer(300));
        a.set_param("range_width", &toml::Value::Integer(8));
        a.set_param(
            "pools",
            &toml::Value::Array(vec![toml::Value::String("PIR0".to_string())]),
        );
        assert_eq!(a.attack, "praos");
        assert_eq!(a.connections_per_relay, 300);
        assert_eq!(a.range_width, 8);
        assert_eq!(a.pools, vec!["PIR0"]);
        // Negatives clamp; unknown fields ignored.
        a.set_param("connections_per_relay", &toml::Value::Integer(-3));
        a.set_param("nonsense", &toml::Value::Integer(9));
        assert_eq!(a.connections_per_relay, 0);
    }
}
