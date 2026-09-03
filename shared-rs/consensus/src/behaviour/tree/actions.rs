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
use super::env::{ConsensusCtx, TreeContext};
use super::Status;
use crate::behaviour::actions as catalogue;
use crate::behaviour::registry::ActionSpec;

/// The contract every leaf action honours, generic over the tree
/// instantiation's context family `C` (what it reads) and effect `E` (what it
/// writes). The consensus instantiation binds `C = ConsensusCtx`
/// (view = `TickCtx`) and `E = ControlSignal`.
///
/// `contribute` writes the leaf's slice of `out` and returns its status;
/// `reset` is called when the action is halted (a reactive abort) so a stateful
/// action can drop any carried progress. `Debug + Send` so the compiled tree is
/// inspectable and movable across tasks.
pub trait LeafAction<C: TreeContext, E>: std::fmt::Debug + Send {
    /// Write this leaf's slice of `out` and return its status.
    fn contribute(&mut self, ctx: &C::View<'_>, out: &mut E) -> Status;

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

/// The honest leaf: contributes nothing (leaves the effect at default) and
/// returns `Success`. The fallback branch of a `Selector`. Blanket over every
/// context/effect — it never reads the context nor writes the effect.
#[derive(Debug, Default, Clone, Copy)]
pub struct HonestAction;

impl<C: TreeContext, E> LeafAction<C, E> for HonestAction {
    fn contribute(&mut self, _ctx: &C::View<'_>, _out: &mut E) -> Status {
        Status::Success
    }
}

/// Materialise an [`ActionSpec`] into a boxed consensus [`LeafAction`].
///
/// `seed` is the deterministic seed for actions that make per-peer/per-slot
/// random choices (equivocation routing buckets, the inbound-drop draw).
pub fn build_action(
    spec: &ActionSpec,
    seed: u64,
) -> Box<dyn LeafAction<ConsensusCtx, ControlSignal>> {
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
        ActionSpec::WithholdTxs {
            withholding_slots,
            tx_producer_only,
            withhold_tx_submission,
            withhold_leios_fetch,
        } => Box::new(catalogue::WithholdTxs::new(
            *withholding_slots,
            *tx_producer_only,
            *withhold_tx_submission,
            *withhold_leios_fetch,
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
        ActionSpec::HollowEb { declared_bytes } => {
            Box::new(catalogue::HollowEb::new(*declared_bytes))
        }
        ActionSpec::LoadedTxEb { take } => Box::new(catalogue::LoadedTxEb::new(*take)),
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
        ActionSpec::AnnounceSlotSkew { slot_offset } => Box::new(catalogue::AnnounceSlotSkew {
            slot_offset: *slot_offset,
        }),
        ActionSpec::AnnounceFlood { count, slot_offset } => Box::new(catalogue::AnnounceFlood {
            count: *count,
            slot_offset: *slot_offset,
        }),
        ActionSpec::WithholdEbAnnounce => Box::new(catalogue::WithholdEbAnnounce),
        ActionSpec::TxFlood { rate } => Box::new(catalogue::TxFlood { rate: *rate }),
        ActionSpec::VoteFlood { count } => Box::new(catalogue::VoteFlood { count: *count }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::behaviour::tree::control::VotePolicy;
    use crate::behaviour::tree::env::{ConsensusCtx, DynamicEnv, NativeChainState, TickCtx};
    use crate::leios::NoVoteReason;

    fn tick_once(
        action: &mut dyn LeafAction<ConsensusCtx, ControlSignal>,
    ) -> (Status, ControlSignal) {
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
