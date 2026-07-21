//! `t22` — filter EB/tx processing via a deterministic checksum threshold.
//!
//! Sets `mempool.tx_filter = ChecksumThreshold { vote, non_voting, hide_eb_tx }`;
//! the mempool actuator then applies the threshold policy (the per-EB checksum
//! decision lives in the actuator, which has the EB hash and node id). Returns
//! `Running` while installed.

use crate::behaviour::tree::actions::LeafAction;
use crate::behaviour::tree::control::{ControlSignal, TxFilterPolicy};
use crate::behaviour::tree::env::{ConsensusCtx, TickCtx};
use crate::behaviour::tree::Status;

/// Installs the checksum-threshold tx filter.
#[derive(Debug, Clone, Copy, Default)]
pub struct T22 {
    vote_threshold: u8,
    non_voting_threshold: u8,
    hide_eb_tx_received: bool,
}

impl T22 {
    pub fn new(vote_threshold: u8, non_voting_threshold: u8, hide_eb_tx_received: bool) -> Self {
        Self {
            vote_threshold,
            non_voting_threshold,
            hide_eb_tx_received,
        }
    }
}

impl LeafAction<ConsensusCtx, ControlSignal> for T22 {
    fn contribute(&mut self, _ctx: &TickCtx, out: &mut ControlSignal) -> Status {
        out.mempool.tx_filter = TxFilterPolicy::ChecksumThreshold {
            vote: self.vote_threshold,
            non_voting: self.non_voting_threshold,
            hide_eb_tx: self.hide_eb_tx_received,
        };
        Status::Running
    }

    fn set_param(&mut self, field: &str, value: &toml::Value) {
        match field {
            "vote_threshold" => {
                if let Some(v) = value.as_integer() {
                    self.vote_threshold = v.clamp(0, u8::MAX as i64) as u8;
                }
            }
            "non_voting_threshold" => {
                if let Some(v) = value.as_integer() {
                    self.non_voting_threshold = v.clamp(0, u8::MAX as i64) as u8;
                }
            }
            "hide_eb_tx_received" => {
                if let Some(b) = value.as_bool() {
                    self.hide_eb_tx_received = b;
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

    fn run(action: &mut T22) -> (Status, ControlSignal) {
        let env = DynamicEnv::new();
        let state = NativeChainState::default();
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
    fn installs_configured_threshold_filter() {
        let (s, out) = run(&mut T22::new(42, 99, true));
        assert_eq!(s, Status::Running);
        assert_eq!(
            out.mempool.tx_filter,
            TxFilterPolicy::ChecksumThreshold {
                vote: 42,
                non_voting: 99,
                hide_eb_tx: true,
            }
        );
    }

    #[test]
    fn touches_only_the_mempool_domain() {
        let (_, out) = run(&mut T22::new(1, 2, false));
        assert_eq!(out.praos, Default::default());
        assert_eq!(out.leios, Default::default());
    }
}
