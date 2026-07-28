//! Leaf actions — the control-signal contributors.
//!
//! A leaf [`Action`](super::behaviour::BehaviourKind::Action), when its branch
//! is active this tick, writes its slice of the slot's
//! [`ControlSignal`](super::control::ControlSignal) and returns a [`Status`].
//! It makes no consensus calls and never branches its status on env/state — per
//! the gating house rule, a leaf returns `Running` while active and all flow
//! gating lives in explicit `Condition` behaviours. The honest fallback leaf
//! returns `Success`.
//!
//! Leaves are constructed from the action registry: a config names a leaf by
//! `kind` (+ params) via [`ActionSpec`], and [`build_action`] returns the
//! matching boxed [`LeafAction`].

use super::control::ControlSignal;
use super::env::TickCtx;
use super::Status;
use crate::behaviour::actions as catalogue;
use crate::behaviour::registry::ActionSpec;

/// The contract every leaf action honours.
///
/// `contribute` writes the leaf's `ControlSignal` slice and returns its status;
/// `reset` is called when the action is halted (a reactive abort) so a stateful
/// action can drop any carried progress. `Debug + Send` so the compiled tree is
/// inspectable and movable across tasks.
pub trait LeafAction: std::fmt::Debug + Send {
    /// Write this leaf's slice of `out` and return its status.
    fn contribute(&mut self, ctx: &TickCtx, out: &mut ControlSignal) -> Status;

    /// Stop contributing and reset progress. Default: nothing to reset.
    fn reset(&mut self) {}

    /// Apply a live override to one tunable parameter, coercing the TOML scalar
    /// to the field's type. Called by the tick (before [`contribute`]) for each
    /// override addressed to this leaf, so a running attack can be retuned
    /// without rebuilding the tree — all other leaf state (counters, RNG) is
    /// preserved. Default: ignore (a leaf with no tunable params, or an unknown
    /// field). [`contribute`]: LeafAction::contribute
    fn set_param(&mut self, _field: &str, _value: &toml::Value) {}
}

/// The honest leaf: contributes nothing (leaves `ControlSignal` at default) and
/// returns `Success`. The fallback branch of a `Selector`.
#[derive(Debug, Default, Clone, Copy)]
pub struct HonestAction;

impl LeafAction for HonestAction {
    fn contribute(&mut self, _ctx: &TickCtx, _out: &mut ControlSignal) -> Status {
        Status::Success
    }
}

/// Materialise an [`ActionSpec`] into a boxed [`LeafAction`].
///
/// `seed` is the deterministic seed for actions that make per-peer/per-slot
/// random choices (equivocation routing buckets, the inbound-drop draw).
pub fn build_action(spec: &ActionSpec, seed: u64) -> Box<dyn LeafAction> {
    match spec {
        ActionSpec::RbHeaderEquivocator { ways } => {
            Box::new(catalogue::RbHeaderEquivocator::new(*ways, seed))
        }
        ActionSpec::LazyVoter { reason } => Box::new(catalogue::LazyVoter::new(*reason)),
        ActionSpec::T22 {
            vote_threshold,
            non_voting_threshold,
            hide_eb_tx_received,
        } => Box::new(catalogue::T22::new(
            *vote_threshold,
            *non_voting_threshold,
            *hide_eb_tx_received,
        )),
        ActionSpec::WithholdAnnouncements {
            withholding_slots,
            tx_producer_only,
        } => Box::new(catalogue::WithholdAnnouncements::new(
            *withholding_slots,
            *tx_producer_only,
        )),
        ActionSpec::DeepReorg { every_slots, depth } => {
            Box::new(catalogue::DeepReorg::new(*every_slots, *depth))
        }
        ActionSpec::DropInboundPeers { probability } => {
            Box::new(catalogue::DropInboundPeers::new(seed, *probability))
        }
        ActionSpec::LieAboutEbSize {
            scale_num,
            scale_den,
            offset,
        } => Box::new(catalogue::LieAboutEbSize::new(
            *scale_num, *scale_den, *offset,
        )),
        ActionSpec::EchoToSource => Box::new(catalogue::EchoToSource),
        ActionSpec::CertSuppressor => Box::new(catalogue::CertSuppressor),
        ActionSpec::PhantomTxEb { n_txs } => Box::new(catalogue::PhantomTxEb::new(*n_txs)),
        ActionSpec::DummyTxEb { n_txs } => Box::new(catalogue::DummyTxEb::new(*n_txs)),
        ActionSpec::AnnounceSizeLie {
            scale_num,
            scale_den,
            offset,
        } => Box::new(catalogue::AnnounceSizeLie::new(
            *scale_num, *scale_den, *offset,
        )),
        ActionSpec::AnnounceDangling => Box::new(catalogue::AnnounceDangling),
        ActionSpec::AnnounceEquivocate => Box::new(catalogue::AnnounceEquivocate),
        ActionSpec::FakeAnnounce => Box::new(catalogue::FakeAnnounce),
        ActionSpec::WithholdEbAnnounce => Box::new(catalogue::WithholdEbAnnounce),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::behaviour::tree::control::VotePolicy;
    use crate::behaviour::tree::env::{DynamicEnv, NativeChainState};
    use crate::leios::NoVoteReason;

    fn tick_once(action: &mut dyn LeafAction) -> (Status, ControlSignal) {
        let env = DynamicEnv::new();
        let state = NativeChainState::default();
        let ctx = TickCtx {
            env: &env,
            state: &state,
            seed: 7,
            action_params: None,
        };
        let mut out = ControlSignal::default();
        let s = action.contribute(&ctx, &mut out);
        (s, out)
    }

    #[test]
    fn honest_contributes_nothing_and_succeeds() {
        let mut a = HonestAction;
        let (s, out) = tick_once(&mut a);
        assert_eq!(s, Status::Success);
        assert_eq!(out, ControlSignal::default());
    }

    #[test]
    fn build_action_dispatches_by_kind() {
        let mut a = build_action(
            &ActionSpec::LazyVoter {
                reason: NoVoteReason::Declined,
            },
            0,
        );
        let (s, out) = tick_once(a.as_mut());
        assert_eq!(s, Status::Running);
        assert_eq!(out.leios.vote, VotePolicy::Abstain(NoVoteReason::Declined));
    }
}
