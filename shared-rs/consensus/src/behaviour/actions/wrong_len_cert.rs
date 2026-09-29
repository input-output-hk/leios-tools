//! `wrong-len-cert` (T14 variant #2) — forge the `leios_certificate` on CertRBs
//! this node produces so its `signers` bitfield is a DELIBERATELY WRONG length
//! (`⌈committee_size/8⌉ + 1` bytes) while otherwise well-formed.
//!
//! Sets `praos.forge_wrong_len_cert = true`. The consuming I/O wrapper then
//! builds a certificate whose bitfield names the whole committee but carries one
//! extra trailing byte, over a valid G1 aggregate, and ships it WITHOUT the
//! usual pre-publish self-verify drop.
//!
//! This is the cheapest-to-reject invalid-cert variant. `verifyLeiosCert`
//! compares the bitfield length against the committee size FIRST — before any
//! BLS math — and answers `MalformedSigners` on a mismatch, which makes the
//! carrying RB an `InvalidBlock`. It is the low end of T14's cost-asymmetry
//! axis (a length compare versus the full aggregate-pubkey reconstruction the
//! `single-bls-cert` variant forces).
//!
//! No parameters — the probe is a single, well-defined length lie. Returns
//! `Running` while installed.

use crate::behaviour::tree::actions::LeafAction;
use crate::behaviour::tree::control::ControlSignal;
use crate::behaviour::tree::env::{ConsensusCtx, TickCtx};
use crate::behaviour::tree::Status;

/// Forges a wrong-length-bitfield certificate on any CertRB this node produces.
/// No parameters.
#[derive(Debug, Clone, Copy, Default)]
pub struct WrongLenCert;

impl LeafAction<ConsensusCtx, ControlSignal> for WrongLenCert {
    fn contribute(&mut self, _ctx: &TickCtx, out: &mut ControlSignal) -> Status {
        out.praos.forge_wrong_len_cert = true;
        Status::Running
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::behaviour::tree::env::{DynamicEnv, NativeChainState};

    #[test]
    fn sets_forge_wrong_len_cert() {
        let env = DynamicEnv::new();
        let state = NativeChainState::default();
        let ctx = TickCtx {
            env: &env,
            state: &state,
            seed: 0,
            action_params: None,
        };
        let mut out = ControlSignal::default();
        let s = WrongLenCert.contribute(&ctx, &mut out);
        assert_eq!(s, Status::Running);
        assert!(out.praos.forge_wrong_len_cert);
        // Honest default (no tick) keeps the cert honest.
        assert!(!ControlSignal::default().praos.forge_wrong_len_cert);
    }
}
