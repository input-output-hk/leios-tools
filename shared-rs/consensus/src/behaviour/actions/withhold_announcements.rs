//! `withhold-announcements` — delay (or suppress) tx announcements to peers.
//!
//! Sets `mempool.announce_filter = WithholdAnnouncements { delay_for_slots,
//! only_ours }`; the mempool actuator then honours the policy in
//! `peek_unannounced_for_peer` (a tx younger than `delay_for_slots` is held
//! back; `u64::MAX` withholds it forever). Returns `Running` while installed.

use crate::behaviour::tree::actions::LeafAction;
use crate::behaviour::tree::control::{ControlSignal, TxAnnouncePolicy};
use crate::behaviour::tree::env::{ConsensusCtx, TickCtx};
use crate::behaviour::tree::Status;

/// Installs the withhold-announcements tx filter.
#[derive(Debug, Clone, Copy, Default)]
pub struct WithholdAnnouncements {
    withholding_slots: u64,
    tx_producer_only: bool,
}

impl WithholdAnnouncements {
    pub fn new(delay_for_slots: u64, only_ours: bool) -> Self {
        Self {
            withholding_slots: delay_for_slots,
            tx_producer_only: only_ours,
        }
    }
}

impl LeafAction<ConsensusCtx, ControlSignal> for WithholdAnnouncements {
    fn contribute(&mut self, _ctx: &TickCtx, out: &mut ControlSignal) -> Status {
        out.mempool.announce_filter = TxAnnouncePolicy::WithholdAnnouncements {
            withholding_slots: self.withholding_slots,
            tx_producer_only: self.tx_producer_only,
        };
        Status::Running
    }

    fn set_param(&mut self, field: &str, value: &toml::Value) {
        match field {
            "withholding_slots" => {
                if let Some(v) = value.as_integer() {
                    self.withholding_slots = v.max(0) as u64;
                }
            }
            "tx_producer_only" => {
                if let Some(b) = value.as_bool() {
                    self.tx_producer_only = b;
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

    fn run(action: &mut WithholdAnnouncements) -> (Status, ControlSignal) {
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
    fn installs_configured_announce_filter() {
        let (s, out) = run(&mut WithholdAnnouncements::new(5, true));
        assert_eq!(s, Status::Running);
        assert_eq!(
            out.mempool.announce_filter,
            TxAnnouncePolicy::WithholdAnnouncements {
                withholding_slots: 5,
                tx_producer_only: true,
            }
        );
    }

    #[test]
    fn touches_only_the_mempool_domain() {
        let (_, out) = run(&mut WithholdAnnouncements::new(3, false));
        assert_eq!(out.praos, Default::default());
        assert_eq!(out.leios, Default::default());
    }
}

