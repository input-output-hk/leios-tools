//! `wrong-context-cert` (T14 variant #3) — forge the `leios_certificate` on
//! CertRBs this node produces so it is well-formed in every structural way but
//! its aggregate signs the WRONG message.
//!
//! Sets `praos.forge_wrong_context_cert = true`. The consuming I/O wrapper then
//! builds a certificate with a correct-length, all-committee `signers` bitfield
//! and a genuine 48-byte G1 aggregate, but that aggregate is a signature over a
//! deterministically-derived WRONG hash (the announcing RB hash, bitwise
//! complemented) rather than over the announcing RB the certificate rides on.
//! It ships WITHOUT the usual pre-publish self-verify drop.
//!
//! `verifyLeiosCert` reconstructs the aggregate public key from the named
//! signer set and verifies the aggregate against the announcing RB's hash — the
//! message each vote actually signed. A valid aggregate over the wrong hash
//! therefore fails with `InvalidSignature`, which makes the carrying RB an
//! `InvalidBlock`. This is the dearest-to-reject invalid-cert variant, the high
//! end of T14's cost-asymmetry axis: rejection requires full aggregate-pubkey
//! reconstruction and a pairing check, unlike `wrong-len-cert`'s cheap length
//! compare. The defect is purely the message/context binding — the signature
//! itself is a genuine, verifying BLS aggregate over the wrong hash.
//!
//! No parameters — the probe is a single, well-defined context lie. Returns
//! `Running` while installed.

use crate::behaviour::tree::actions::LeafAction;
use crate::behaviour::tree::control::ControlSignal;
use crate::behaviour::tree::env::{ConsensusCtx, TickCtx};
use crate::behaviour::tree::Status;

/// Forges a wrong-context (wrong-message) certificate on any CertRB this node
/// produces. No parameters.
#[derive(Debug, Clone, Copy, Default)]
pub struct WrongContextCert;

impl LeafAction<ConsensusCtx, ControlSignal> for WrongContextCert {
    fn contribute(&mut self, _ctx: &TickCtx, out: &mut ControlSignal) -> Status {
        out.praos.forge_wrong_context_cert = true;
        Status::Running
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::behaviour::tree::env::{DynamicEnv, NativeChainState};

    #[test]
    fn sets_forge_wrong_context_cert() {
        let env = DynamicEnv::new();
        let state = NativeChainState::default();
        let ctx = TickCtx {
            env: &env,
            state: &state,
            seed: 0,
            action_params: None,
        };
        let mut out = ControlSignal::default();
        let s = WrongContextCert.contribute(&ctx, &mut out);
        assert_eq!(s, Status::Running);
        assert!(out.praos.forge_wrong_context_cert);
        // Honest default (no tick) keeps the cert honest.
        assert!(!ControlSignal::default().praos.forge_wrong_context_cert);
    }
}
