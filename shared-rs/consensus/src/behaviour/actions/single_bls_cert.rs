//! `single-bls-cert` — forge the `leios_certificate` on CertRBs this node
//! produces so its `signers` bitfield names the WHOLE committee while its
//! aggregate is only this node's OWN BLS vote signature.
//!
//! Sets `praos.forge_single_bls_cert = true`. The consuming I/O wrapper then
//! builds a certificate `[all-committee bitfield, own-key aggregate]` instead of
//! aggregating the votes it actually collected, and ships it WITHOUT the usual
//! pre-publish self-verify drop.
//!
//! This is a soundness audit of the ledger's `verifyLeiosCert`. The
//! all-committee bitfield clears the weight gate (`InsufficientWeight`), so the
//! only thing left between the block and adoption is whether the verifier binds
//! the aggregate to the named signer set:
//!   - honest REJECT (`InvalidSignature`) — aggregate verification holds;
//!   - honest ADOPT — the verifier trusts the bitfield's weight without
//!     checking the aggregate, so one committee key can forge full-quorum
//!     certificates (a certificate-forgery break).
//!
//! No parameters — the probe is a single, well-defined forgery. Returns
//! `Running` while installed.

use crate::behaviour::tree::actions::LeafAction;
use crate::behaviour::tree::control::ControlSignal;
use crate::behaviour::tree::env::{ConsensusCtx, TickCtx};
use crate::behaviour::tree::Status;

/// Forges a single-key certificate with an all-committee bitfield on any CertRB
/// this node produces. No parameters.
#[derive(Debug, Clone, Copy, Default)]
pub struct SingleBlsCert;

impl LeafAction<ConsensusCtx, ControlSignal> for SingleBlsCert {
    fn contribute(&mut self, _ctx: &TickCtx, out: &mut ControlSignal) -> Status {
        out.praos.forge_single_bls_cert = true;
        Status::Running
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::behaviour::tree::env::{DynamicEnv, NativeChainState};

    #[test]
    fn sets_forge_single_bls_cert() {
        let env = DynamicEnv::new();
        let state = NativeChainState::default();
        let ctx = TickCtx {
            env: &env,
            state: &state,
            seed: 0,
            action_params: None,
        };
        let mut out = ControlSignal::default();
        let s = SingleBlsCert.contribute(&ctx, &mut out);
        assert_eq!(s, Status::Running);
        assert!(out.praos.forge_single_bls_cert);
        // Honest default (no tick) keeps the cert honest.
        assert!(!ControlSignal::default().praos.forge_single_bls_cert);
    }
}
