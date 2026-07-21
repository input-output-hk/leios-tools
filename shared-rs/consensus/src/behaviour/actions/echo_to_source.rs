//! `echo-to-source` — open the LeiosNotify no-echo gate.
//!
//! Sets `leios.echo_to_source = true`, so EB / EB-tx offers fetched from a peer
//! are reflected back to that same peer (a CIP-0164 violation the honest gate
//! suppresses). Compose under a `Join` with `lie-about-eb-size` to reproduce
//! the duplex-follower bug. Returns `Running` while installed.

use crate::behaviour::tree::actions::LeafAction;
use crate::behaviour::tree::control::ControlSignal;
use crate::behaviour::tree::env::{ConsensusCtx, TickCtx};
use crate::behaviour::tree::Status;

/// Opens the no-echo gate. No parameters — composition is the only knob.
#[derive(Debug, Clone, Copy, Default)]
pub struct EchoToSource;

impl LeafAction<ConsensusCtx, ControlSignal> for EchoToSource {
    fn contribute(&mut self, _ctx: &TickCtx, out: &mut ControlSignal) -> Status {
        out.leios.echo_to_source = true;
        Status::Running
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::behaviour::tree::env::{DynamicEnv, NativeChainState};

    #[test]
    fn opens_the_echo_gate() {
        let env = DynamicEnv::new();
        let state = NativeChainState::default();
        let ctx = TickCtx {
            env: &env,
            state: &state,
            seed: 0,
            action_params: None,
        };
        let mut out = ControlSignal::default();
        let s = EchoToSource.contribute(&ctx, &mut out);
        assert_eq!(s, Status::Running);
        assert!(out.leios.echo_to_source);
        // Honest default (no tick) keeps the gate closed.
        assert!(!ControlSignal::default().leios.echo_to_source);
    }
}
