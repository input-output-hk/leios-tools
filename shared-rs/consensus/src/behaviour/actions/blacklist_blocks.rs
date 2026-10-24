//! `blacklist-blocks` — mark a fixed set of RB block hashes as blacklisted.
//!
//! Writes the configured block hashes into `praos.rb_blacklist`. The Praos
//! actuator ([`PraosState::apply_control`](crate::praos::PraosState::apply_control))
//! pushes that set all the way down into the chain tree's `blacklist` field, so
//! the blacklisted blocks — and every heir of theirs — can never win chain
//! selection (the chain carrying them is not adopted) nor be produced upon.
//!
//! The set is a *union* contributor: each active `blacklist-blocks` leaf adds
//! its hashes to the slot's control signal, so several lists compose. An empty
//! list (or the action deselected) leaves `rb_blacklist` at its honest default,
//! which clears the chain tree's blacklist on the next actuation. Returns
//! `Running` while installed.

use std::collections::BTreeSet;

use crate::behaviour::tree::actions::LeafAction;
use crate::behaviour::tree::control::ControlSignal;
use crate::behaviour::tree::env::{ConsensusCtx, TickCtx};
use crate::behaviour::tree::Status;

/// Blacklist a fixed list of 32-byte RB block hashes.
#[derive(Debug, Clone, Default)]
pub struct BlacklistBlocks {
    /// The block hashes to blacklist.
    pub blocks: Vec<[u8; 32]>,
}

impl BlacklistBlocks {
    /// Build from any iterator of 32-byte hashes.
    pub fn new(blocks: impl IntoIterator<Item = [u8; 32]>) -> Self {
        Self {
            blocks: blocks.into_iter().collect(),
        }
    }
}

impl LeafAction<ConsensusCtx, ControlSignal> for BlacklistBlocks {
    fn contribute(&mut self, _ctx: &TickCtx, out: &mut ControlSignal) -> Status {
        // Union into the slot's blacklist so multiple active contributors
        // New action replaces the list.
        out.praos.rb_blacklist = self.blocks.clone();
        Status::Running
    }

    /// Live-retune the blacklisted set without rebuilding the tree. Accepts a
    /// TOML array of 64-char hex hashes; malformed entries are skipped.
    fn set_param(&mut self, field: &str, value: &toml::Value) {
        if field != "blocks" {
            return;
        }
        let Some(arr) = value.as_array() else {
            return;
        };
        self.blocks = Vec::new();
        for v in arr {
            if let Some(s) = v.as_str() {
                if let Ok(h) = crate::types::hash_from_hex::<32>(s) {
                    self.blocks.push(h);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contributes_blocks_to_rb_blacklist() {
        let ctx = TickCtx::default();
        let mut out = ControlSignal::default();
        let s = BlacklistBlocks::new([[3u8; 32], [7u8; 32]]).contribute(&ctx, &mut out);
        assert_eq!(s, Status::Running);
        assert!(out.praos.rb_blacklist.contains(&[3u8; 32]));
        assert!(out.praos.rb_blacklist.contains(&[7u8; 32]));
        // Honest default blacklists nothing.
        assert!(ControlSignal::default().praos.rb_blacklist.is_empty());
    }

    #[test]
    fn contribute_replaces_old_value() {
        let ctx = TickCtx::default();
        let mut out = ControlSignal::default();
        out.praos.rb_blacklist.push([1u8; 32]);
        BlacklistBlocks::new([[2u8; 32]]).contribute(&ctx, &mut out);
        assert!(!out.praos.rb_blacklist.contains(&[1u8; 32]));
        assert!(out.praos.rb_blacklist.contains(&[2u8; 32]));
    }

    #[test]
    fn set_param_replaces_blocks_from_hex_array() {
        let mut a = BlacklistBlocks::new([[1u8; 32]]);
        let hex = "03".repeat(32); // 64 hex chars → [3u8; 32]
        a.set_param("blocks", &toml::Value::Array(vec![toml::Value::String(hex)]));
        assert_eq!(a.blocks.len(), 1);
        assert!(a.blocks.contains(&[3u8; 32]));
        // Unknown field is ignored.
        a.set_param("nope", &toml::Value::Integer(1));
        assert!(a.blocks.contains(&[3u8; 32]));
    }
}

