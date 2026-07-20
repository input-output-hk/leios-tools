//! `fake-eb` — announce a fabricated EB with phantom transactions (pen-test).
//!
//! Sets `praos.fake_eb_txs = Some(n_txs)`, so on any RB this node produces it
//! announces an EB whose manifest is `n_txs` random, nonexistent tx hashes. The
//! EB body (the manifest) is served on fetch, but the referenced txs exist
//! nowhere — so honest voters fetch the EB, fail to fetch its txs, and decline
//! `MissingTX`. Probes missing-tx handling + resource waste; the fake EB should
//! never reach quorum. `n_txs` is the manifest size (the only knob besides the
//! adversary's stake, i.e. how many RBs it produces). Returns `Running` while
//! installed.

use crate::behaviour::tree::actions::LeafAction;
use crate::behaviour::tree::control::ControlSignal;
use crate::behaviour::tree::env::TickCtx;
use crate::behaviour::tree::Status;

/// Announces a fake EB with `n_txs` phantom transactions on produced RBs.
#[derive(Debug, Clone, Copy)]
pub struct FakeEbAnnouncer {
    n_txs: u32,
}

impl FakeEbAnnouncer {
    pub fn new(n_txs: u32) -> Self {
        Self { n_txs }
    }
}

impl LeafAction for FakeEbAnnouncer {
    fn contribute(&mut self, _ctx: &TickCtx, out: &mut ControlSignal) -> Status {
        out.praos.fake_eb_txs = Some(self.n_txs);
        Status::Running
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::behaviour::tree::env::{DynamicEnv, NativeChainState};

    #[test]
    fn sets_fake_eb_txs() {
        let env = DynamicEnv::new();
        let state = NativeChainState::default();
        let ctx = TickCtx {
            env: &env,
            state: &state,
            seed: 0,
            action_params: None,
        };
        let mut out = ControlSignal::default();
        let s = FakeEbAnnouncer::new(8).contribute(&ctx, &mut out);
        assert_eq!(s, Status::Running);
        assert_eq!(out.praos.fake_eb_txs, Some(8));
        // Honest default (no tick) announces no fake EB.
        assert_eq!(ControlSignal::default().praos.fake_eb_txs, None);
    }
}
