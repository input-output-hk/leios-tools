//! The decision → actuation seam.
//!
//! [`ControlSignal`] is produced once per slot by [`BehaviourTree::tick`] and
//! read by the consensus actuators. It is **domain-grouped by actuator**
//! (`praos` / `leios` / `mempool`); each active leaf writes its slice, and
//! same-field conflicts between two active leaves are reconciled in the tick
//! (last active contributor in traversal order wins) — never by the actuator.
//!
//! `ControlSignal::default()` is the honest node: no perturbation. A behaviour
//! that reuses an existing capability adds no field here; only a genuinely new
//! effect kind does (see `leaf-action.contract.md`).
//!
//! [`BehaviourTree::tick`]: super::behaviour::BehaviourTree::tick

use std::collections::BTreeSet;

use crate::behaviour::RbProductionStrategy;
use crate::leios::NoVoteReason;
use crate::peer::PeerId;
use crate::production::BodyPath;

/// The full per-slot control signal emitted by a tick.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ControlSignal {
    pub praos: PraosControl,
    pub leios: LeiosControl,
    pub mempool: MempoolControl,
}

/// Praos-domain actuator inputs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PraosControl {
    /// RB production strategy (`Normal` | `Suppress` | `Equivocate { ways }`).
    pub production: RbProductionStrategy,
    /// Per-peer outbound control (equivocation routing / partition).
    pub outbound: OutboundControl,
    /// Force a self-reorg of this depth this slot, if `Some`.
    pub reorg_depth: Option<u64>,
    /// Reset inbound peers this slot.
    pub drop_inbound: bool,
    /// Override the producer's body-path choice, if `Some`.
    pub body_path: Option<BodyPath>,
    /// Suppress the certificate on any CertRB this node would produce this slot
    /// (the `cert-suppressor` action): the adversary omits the cert for its
    /// parent RB's announced EB, killing that EB's certification (strict
    /// parent-only cert rule) without touching quorum.
    pub suppress_cert: bool,
    /// Announce a FAKE EB on any RB this node produces this slot, with this many
    /// phantom (nonexistent) transactions in its manifest (the `fake-eb`
    /// pen-test action). The EB body (manifest) is served, but the referenced
    /// txs exist nowhere, so honest voters fetch the EB, fail to fetch the txs,
    /// and decline `MissingTX`. `None` = honest.
    pub fake_eb_txs: Option<u32>,
}

/// Leios-domain actuator inputs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LeiosControl {
    /// CIP-0164 voting policy (`Honest` | `Abstain(reason)`).
    pub vote: VotePolicy,
    /// Rewrite `eb_size` on outbound `MsgLeiosBlockOffer`.
    pub offer_eb_size: EbSizePolicy,
    /// `false` = honest no-echo gate; `true` = reflect offers back to source.
    pub echo_to_source: bool,
}

/// Mempool-domain actuator inputs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MempoolControl {
    /// EB/tx processing filter.
    pub tx_filter: TxFilterPolicy,
}

/// Whether to cast CIP-0164 votes honestly or abstain.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum VotePolicy {
    #[default]
    Honest,
    Abstain(NoVoteReason),
}

/// Per-peer outbound rewriting requested by the tick.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum OutboundControl {
    #[default]
    None,
    /// Route a different RB-header variant to each peer bucket (lookup, not a
    /// decision: the actuator computes the bucket from `seed`).
    EquivocateRouting { slot: u64, ways: u8, seed: u64 },
    /// Suppress delivery to this set of peers (partition / mute).
    DropTo(BTreeSet<PeerId>),
}

/// How to rewrite `eb_size` on outbound `MsgLeiosBlockOffer`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum EbSizePolicy {
    #[default]
    Honest,
    /// `(eb_size * scale_num / scale_den) + offset`, clamped to `u32`.
    Linear {
        scale_num: u32,
        scale_den: u32,
        offset: i32,
    },
}

impl EbSizePolicy {
    /// Apply this policy to an honest `eb_size`, yielding the size to advertise
    /// on the wire. `Honest` is the identity; `Linear` computes
    /// `(eb_size * scale_num / scale_den) + offset` with `i128` intermediates
    /// (well-defined across the whole `u32` range and any `i32` offset),
    /// clamped to `[0, u32::MAX]`. A `scale_den` of `0` is treated as `1`.
    pub fn apply(&self, eb_size: u32) -> u32 {
        match self {
            EbSizePolicy::Honest => eb_size,
            EbSizePolicy::Linear {
                scale_num,
                scale_den,
                offset,
            } => {
                let den = (*scale_den).max(1) as i128;
                let scaled = (eb_size as i128) * (*scale_num as i128) / den;
                let with_offset = scaled + (*offset as i128);
                with_offset.clamp(0, u32::MAX as i128) as u32
            }
        }
    }
}

/// EB/tx processing filter (the t22 checksum-threshold policy).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum TxFilterPolicy {
    #[default]
    None,
    ChecksumThreshold {
        vote: u8,
        non_voting: u8,
        hide_eb_tx: bool,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_the_honest_node() {
        let d = ControlSignal::default();
        assert_eq!(d.praos.production, RbProductionStrategy::Normal);
        assert_eq!(d.praos.outbound, OutboundControl::None);
        assert_eq!(d.praos.reorg_depth, None);
        assert!(!d.praos.drop_inbound);
        assert_eq!(d.praos.body_path, None);
        assert!(!d.praos.suppress_cert);
        assert_eq!(d.leios.vote, VotePolicy::Honest);
        assert_eq!(d.leios.offer_eb_size, EbSizePolicy::Honest);
        assert!(!d.leios.echo_to_source);
        assert_eq!(d.mempool.tx_filter, TxFilterPolicy::None);
    }

    #[test]
    fn sub_policies_default_to_honest_variants() {
        assert_eq!(VotePolicy::default(), VotePolicy::Honest);
        assert_eq!(OutboundControl::default(), OutboundControl::None);
        assert_eq!(EbSizePolicy::default(), EbSizePolicy::Honest);
        assert_eq!(TxFilterPolicy::default(), TxFilterPolicy::None);
    }
}
