//! `blacklist-blocks` — blacklist an RB at an offset from a supplied chain tip.
//!
//! Reads the common cluster `tip` from the behaviour environment and writes it,
//! together with the configured ancestor offset, into the Praos control signal.
//! The Praos actuator resolves that pair against its chain tree and installs the
//! resulting block hash into the blacklist. `offset = 0` blacklists the tip.

use crate::behaviour::tree::actions::LeafAction;
use crate::behaviour::tree::control::ControlSignal;
use crate::behaviour::tree::env::{ConsensusCtx, TickCtx};
use crate::behaviour::tree::{EnvValue, Status};
use crate::types::hash_from_hex;

/// Blacklist the ancestor `blocks_offset` blocks behind `env.tip`.
#[derive(Debug, Clone, Default)]
pub struct BlacklistBlocks {
    /// Number of parent links from the supplied tip (`0` = the tip itself).
    pub blocks_offset: u64,
}
impl BlacklistBlocks {
    pub fn new(blocks_offset: u64) -> Self {
        Self {
            blocks_offset,
        }
    }
}

impl LeafAction<ConsensusCtx, ControlSignal> for BlacklistBlocks {
    fn contribute(&mut self, ctx: &TickCtx, out: &mut ControlSignal) -> Status {
        let Some(EnvValue::Str(tip)) = ctx.env.get("tip") else {
            tracing::warn!(
                "BlacklistBlocks::contribute: missing or non-string 'tip' environment: {:?}",
                ctx.env.get("tip")
            );
            return Status::Failure;
        };
        let Ok(tip) = hash_from_hex::<32>(tip) else {
            tracing::warn!("BlacklistBlocks::contribute: invalid 'tip' environment");
            return Status::Failure;
        };
        out.praos.rb_blacklist_tip = tip;
        out.praos.rb_blacklist_offset = self.blocks_offset;
        Status::Running
    }

    /// Live-retune the ancestor offset without rebuilding the tree.
    fn set_param(&mut self, field: &str, value: &toml::Value) {
        if field != "offset" {
            return;
        }
        if let Some(offset) = value.as_integer() {
            self.blocks_offset = offset.max(0) as u64;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::behaviour::tree::env::{DynamicEnv, NativeChainState};

    fn contribute(action: &mut BlacklistBlocks, tip: Option<EnvValue>) -> (Status, ControlSignal) {
        let mut env = DynamicEnv::new();
        if let Some(tip) = tip {
            env.insert("tip", tip);
        }
        let state = NativeChainState::default();
        let ctx = TickCtx {
            env: &env,
            state: &state,
            ..Default::default()
        };
        let mut out = ControlSignal::default();
        let status = action.contribute(&ctx, &mut out);
        (status, out)
    }

    #[test]
    fn contributes_tip_and_offset() {
        let mut action = BlacklistBlocks::new(7);
        let (status, out) = contribute(
            &mut action,
            Some(EnvValue::Str("03".repeat(32))),
        );
        assert_eq!(status, Status::Running);
        assert_eq!(out.praos.rb_blacklist_tip, [3u8; 32]);
        assert_eq!(out.praos.rb_blacklist_offset, 7);
    }

    #[test]
    fn contribute_fails_for_missing_or_invalid_tip() {
        let mut action = BlacklistBlocks::new(2);
        assert_eq!(contribute(&mut action, None).0, Status::Failure);
        assert_eq!(
            contribute(&mut action, Some(EnvValue::Str("not-hex".to_string()))).0,
            Status::Failure
        );
        assert_eq!(
            contribute(&mut action, Some(EnvValue::U64(3))).0,
            Status::Failure
        );
    }

    #[test]
    fn set_param_updates_offset() {
        let mut a = BlacklistBlocks::new(1);
        a.set_param("offset", &toml::Value::Integer(9));
        assert_eq!(a.blocks_offset, 9);
        a.set_param("offset", &toml::Value::Integer(-1));
        assert_eq!(a.blocks_offset, 0);
        a.set_param("nope", &toml::Value::Integer(1));
        assert_eq!(a.blocks_offset, 0);
    }
}

