//! `non-member-cert` (T14 variant #4) — forge the `leios_certificate` on
//! CertRBs this node produces so its `signers` bitfield has the CORRECT length
//! but names an EMPTY effective signer set.
//!
//! Sets `praos.forge_non_member_cert = true`. The consuming I/O wrapper then
//! builds a certificate whose `signers` bitfield is the correct
//! `⌈committee_size/8⌉` bytes (so it passes the ledger's length check, unlike
//! `wrong-len-cert`) but sets ONLY out-of-range bits — bit indices
//! `>= committee_size` living in the padding of the final byte — and NO in-range
//! bit. It ships WITHOUT the usual pre-publish self-verify drop.
//!
//! `verifyLeiosCert` applies an `idx < committee_size` guard when it decodes the
//! bitfield into named signers, so every out-of-range bit is dropped and the
//! effective signer set is EMPTY. A certificate that names zero committee seats
//! carries zero committee weight, so the ledger rejects it for
//! `InsufficientWeight` — a THIRD distinct invalid-cert verdict, after
//! `wrong-len-cert`'s `MalformedSigners` and `wrong-context-cert` /
//! `single-bls-cert`'s `InvalidSignature`. The aggregate is never reached: the
//! empty signer set fails the weight gate before any BLS math.
//!
//! Edge case: a committee whose size is a multiple of 8 has NO padding bits
//! within the correct length, so no out-of-range bit can be set — the wrapper
//! returns no certificate there. The devnet's 4-seat committee has 4 padding
//! bits, so the probe is exercisable there.
//!
//! No parameters — the probe is a single, well-defined empty-signer-set lie.
//! Returns `Running` while installed.

use crate::behaviour::tree::actions::LeafAction;
use crate::behaviour::tree::control::ControlSignal;
use crate::behaviour::tree::env::{ConsensusCtx, TickCtx};
use crate::behaviour::tree::Status;

/// Forges a non-member (empty effective signer set) certificate on any CertRB
/// this node produces. No parameters.
#[derive(Debug, Clone, Copy, Default)]
pub struct NonMemberCert;

impl LeafAction<ConsensusCtx, ControlSignal> for NonMemberCert {
    fn contribute(&mut self, _ctx: &TickCtx, out: &mut ControlSignal) -> Status {
        out.praos.forge_non_member_cert = true;
        Status::Running
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::behaviour::tree::env::{DynamicEnv, NativeChainState};

    #[test]
    fn sets_forge_non_member_cert() {
        let env = DynamicEnv::new();
        let state = NativeChainState::default();
        let ctx = TickCtx {
            env: &env,
            state: &state,
            seed: 0,
            action_params: None,
        };
        let mut out = ControlSignal::default();
        let s = NonMemberCert.contribute(&ctx, &mut out);
        assert_eq!(s, Status::Running);
        assert!(out.praos.forge_non_member_cert);
        // Honest default (no tick) keeps the cert honest.
        assert!(!ControlSignal::default().praos.forge_non_member_cert);
    }
}
