//! Fake-EB pen-test family — announce a fabricated EB on produced RBs.
//!
//! Two leaves, distinguished by whether the referenced txs can be fetched:
//!
//! - [`PhantomTxEb`] — "Phantom Tx EB": manifest of `n_txs` nonexistent tx
//!   hashes, no bodies pinned. Honest voters fetch the EB, fail to fetch its
//!   txs, and decline `MissingTX`. The fake EB has no backing and should never
//!   reach quorum. Probes missing-tx handling + fetch resource waste.
//! - [`DummyTxEb`] — "Dummy Tx EB": manifest of `n_txs` fabricated txs whose
//!   bodies the adversary pins so they ARE servable. Honest voters fetch the EB
//!   and its txs successfully — no `MissingTX` — so the EB clears the
//!   availability gate despite referencing txs never in any honest mempool.
//!   Probes soundness.
//!
//! Both set `praos.fake_eb`; actuation (manifest construction, and for Dummy
//! the fabricated-body pinning) lives in net-node (adversarial-tools). `n_txs`
//! is the manifest size — the only knob besides the adversary's stake (i.e. how
//! many RBs it produces). Both return `Running` while installed.

use crate::behaviour::tree::actions::LeafAction;
use crate::behaviour::tree::control::{ControlSignal, FakeEbKind};
use crate::behaviour::tree::env::{ConsensusCtx, TickCtx};
use crate::behaviour::tree::Status;

/// Phantom Tx EB — announces an EB of `n_txs` unfetchable phantom txs.
#[derive(Debug, Clone, Copy)]
pub struct PhantomTxEb {
    n_txs: u32,
}

impl PhantomTxEb {
    pub fn new(n_txs: u32) -> Self {
        Self { n_txs }
    }
}

impl LeafAction<ConsensusCtx, ControlSignal> for PhantomTxEb {
    fn contribute(&mut self, _ctx: &TickCtx, out: &mut ControlSignal) -> Status {
        out.praos.fake_eb = Some(FakeEbKind::Phantom { n_txs: self.n_txs });
        Status::Running
    }
}

/// Dummy Tx EB — announces an EB of `n_txs` fabricated txs with servable bodies.
#[derive(Debug, Clone, Copy)]
pub struct DummyTxEb {
    n_txs: u32,
}

impl DummyTxEb {
    pub fn new(n_txs: u32) -> Self {
        Self { n_txs }
    }
}

impl LeafAction<ConsensusCtx, ControlSignal> for DummyTxEb {
    fn contribute(&mut self, _ctx: &TickCtx, out: &mut ControlSignal) -> Status {
        out.praos.fake_eb = Some(FakeEbKind::Dummy { n_txs: self.n_txs });
        Status::Running
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::behaviour::tree::env::{DynamicEnv, NativeChainState};

    fn ctx<'a>(env: &'a DynamicEnv, state: &'a NativeChainState) -> TickCtx<'a> {
        TickCtx {
            env,
            state,
            seed: 0,
            action_params: None,
        }
    }

    #[test]
    fn phantom_sets_phantom_kind() {
        let env = DynamicEnv::new();
        let state = NativeChainState::default();
        let mut out = ControlSignal::default();
        let s = PhantomTxEb::new(8).contribute(&ctx(&env, &state), &mut out);
        assert_eq!(s, Status::Running);
        assert_eq!(out.praos.fake_eb, Some(FakeEbKind::Phantom { n_txs: 8 }));
        // Honest default (no tick) announces no fake EB.
        assert_eq!(ControlSignal::default().praos.fake_eb, None);
    }

    #[test]
    fn dummy_sets_dummy_kind() {
        let env = DynamicEnv::new();
        let state = NativeChainState::default();
        let mut out = ControlSignal::default();
        let s = DummyTxEb::new(5).contribute(&ctx(&env, &state), &mut out);
        assert_eq!(s, Status::Running);
        assert_eq!(out.praos.fake_eb, Some(FakeEbKind::Dummy { n_txs: 5 }));
    }
}
