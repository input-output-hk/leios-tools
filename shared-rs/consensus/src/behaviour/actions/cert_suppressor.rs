//! `cert-suppressor` — omit the certificate on CertRBs this node produces.
//!
//! Sets `praos.suppress_cert = true`, so when this node would attach a
//! certificate for its parent RB's announced EB it instead produces a plain
//! TxRB. Under the strict parent-only cert rule an EB gets exactly one shot at
//! certification — its immediate child RB — so an adversary that produces that
//! child and drops the cert permanently kills the EB's certification, inflating
//! cert-landing loss WITHOUT affecting quorum. No parameters — composition and
//! the adversary's stake (how many child RBs it produces) are the only knobs.
//! Returns `Running` while installed.

use crate::behaviour::tree::actions::LeafAction;
use crate::behaviour::tree::control::ControlSignal;
use crate::behaviour::tree::env::{ConsensusCtx, TickCtx};
use crate::behaviour::tree::Status;

/// Suppresses the cert on any CertRB this node produces. No parameters.
#[derive(Debug, Clone, Copy, Default)]
pub struct CertSuppressor;

impl LeafAction<ConsensusCtx, ControlSignal> for CertSuppressor {
    fn contribute(&mut self, _ctx: &TickCtx, out: &mut ControlSignal) -> Status {
        out.praos.suppress_cert = true;
        Status::Running
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::behaviour::tree::env::{DynamicEnv, NativeChainState};

    #[test]
    fn sets_suppress_cert() {
        let env = DynamicEnv::new();
        let state = NativeChainState::default();
        let ctx = TickCtx {
            env: &env,
            state: &state,
            seed: 0,
            action_params: None,
        };
        let mut out = ControlSignal::default();
        let s = CertSuppressor.contribute(&ctx, &mut out);
        assert_eq!(s, Status::Running);
        assert!(out.praos.suppress_cert);
        // Honest default (no tick) keeps certs on.
        assert!(!ControlSignal::default().praos.suppress_cert);
    }
}
