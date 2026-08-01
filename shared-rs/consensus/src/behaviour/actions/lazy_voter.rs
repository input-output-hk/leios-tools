//! `lazy-voter` — abstain from every CIP-0164 vote.
//!
//! Sets `leios.vote = Abstain(reason)`; the vote actuator then declines to cast
//! a vote, surfacing `reason` in telemetry. Measures committee resilience to
//! silent stakeholders.

use crate::behaviour::tree::actions::LeafAction;
use crate::behaviour::tree::control::{ControlSignal, VotePolicy};
use crate::behaviour::tree::env::{ConsensusCtx, TickCtx};
use crate::behaviour::tree::Status;
use crate::leios::NoVoteReason;

/// Abstains from voting with `reason` (default `Declined`).
#[derive(Debug, Clone, Copy)]
pub struct LazyVoter {
    pub reason: NoVoteReason,
}

impl LazyVoter {
    pub fn new(reason: NoVoteReason) -> Self {
        Self { reason }
    }
}

impl Default for LazyVoter {
    fn default() -> Self {
        Self {
            reason: NoVoteReason::Declined,
        }
    }
}

impl LeafAction<ConsensusCtx, ControlSignal> for LazyVoter {
    fn contribute(&mut self, _ctx: &TickCtx, out: &mut ControlSignal) -> Status {
        out.leios.vote = VotePolicy::Abstain(self.reason);
        Status::Running
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::behaviour::tree::env::{DynamicEnv, NativeChainState};

    fn run(action: &mut LazyVoter) -> (Status, ControlSignal) {
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
    fn abstains_with_configured_reason() {
        let (s, out) = run(&mut LazyVoter::new(NoVoteReason::WrongEB));
        assert_eq!(s, Status::Running);
        assert_eq!(out.leios.vote, VotePolicy::Abstain(NoVoteReason::WrongEB));
    }

    #[test]
    fn default_reason_is_declined() {
        let (_, out) = run(&mut LazyVoter::default());
        assert_eq!(out.leios.vote, VotePolicy::Abstain(NoVoteReason::Declined));
    }

    #[test]
    fn contributes_nothing_outside_leios_vote() {
        let (_, out) = run(&mut LazyVoter::default());
        assert_eq!(out.praos, Default::default());
        assert_eq!(out.mempool, Default::default());
    }
}
