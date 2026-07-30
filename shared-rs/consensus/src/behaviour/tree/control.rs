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
    /// Announce a fabricated EB on any RB this node produces this slot (the
    /// fake-EB pen-test family). `None` = honest; `Some(kind)` picks which
    /// variant — see [`FakeEbKind`].
    pub fake_eb: Option<FakeEbKind>,
}

/// Which fabricated-EB pen-test to run, and its manifest size. The family
/// announces an EB the adversary never legitimately assembled; the variants
/// differ in whether the referenced txs can be fetched by honest voters,
/// which probes different parts of the pipeline:
///
/// - [`Phantom`](FakeEbKind::Phantom) — "Phantom Tx EB": the manifest lists
///   `n_txs` nonexistent tx hashes and the adversary pins **no** bodies. Honest
///   voters fetch the EB, fail to fetch its txs, and decline `MissingTX`. The
///   EB has no backing and must never reach quorum. Probes missing-tx handling
///   and fetch resource-waste.
/// - [`Dummy`](FakeEbKind::Dummy) — "Dummy Tx EB": the manifest lists `n_txs`
///   fabricated txs AND the adversary pins matching (servable) bodies. Honest
///   voters fetch the EB and its txs **successfully** — no `MissingTX` — so the
///   EB clears the availability gate despite referencing txs that were never in
///   any honest mempool. Probes soundness: does anything downstream reject an
///   available-but-fabricated EB, or does it reach quorum?
///
/// A third variant, "Mega Tx EB" (oversized manifest / declared size, for
/// resource-exhaustion robustness), is planned (see the fake-eb pen-test plan
/// in the leios-adversarial-tools repo, which houses the net-node actuation).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FakeEbKind {
    /// Phantom Tx EB — `n_txs` unfetchable phantom txs; no bodies pinned.
    Phantom { n_txs: u32 },
    /// Dummy Tx EB — `n_txs` fabricated txs with servable bodies pinned.
    Dummy { n_txs: u32 },
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
    /// Rewrite the `announced_eb` **size** baked into a produced RB header — the
    /// announce-path counterpart of `offer_eb_size`. Applied at production (the
    /// size lives inside the signed header, not a separate wire field), so the
    /// `MsgLeiosBlockAnnouncement` advertises a size that disagrees with the
    /// true EB body. Independent of `offer_eb_size`: lying on one surface but
    /// not the other is itself a probe.
    pub announce_eb_size: EbSizePolicy,
    /// `true` = announce an EB (`MsgLeiosBlockAnnouncement`) but never serve its
    /// body (suppress the `BlockOffer`/inject) — a dangling/phantom
    /// announcement: peers try to fetch, waste the window, and the voting
    /// deadline can lapse. Censorship/DoS via the announcement itself.
    pub announce_dangling: bool,
    /// `true` = emit a second `MsgLeiosBlockAnnouncement` with a different
    /// `announced_eb` for the same election (OCIN equivocation, ≤ 2 rule).
    pub announce_equivocate: bool,
    /// `true` = emit a `MsgLeiosBlockAnnouncement` for a fabricated EB **without
    /// winning the slot** — the node holds no stake and never produced the RB,
    /// yet announces anyway (decoupled from the production lottery). Real nodes
    /// reject the header at the VRF/KES check (only the elected leader can sign
    /// a valid RB header); net-rs's fake validation may accept it. The sharpest
    /// probe of the authorization gate.
    pub fake_announce: bool,
    /// `true` = **withhold** the EB announcement for EBs this node produces (the
    /// `withhold-eb-block-announce` action). Normal behaviour (`false`) is to
    /// announce every produced EB; this suppresses only the announcement — the
    /// EB body / `MsgLeiosBlockOffer` is unaffected — censoring the fast
    /// discovery pulse. Default `false` keeps `ControlSignal::default()` honest.
    pub withhold_announce: bool,
    /// Signed slot offset applied to a **fabricated** announcement's header slot
    /// (`> 0` future, `< 0` far past) — the `announce-slot-skew` action. Probes
    /// the not-from-future / far-past / age ≤ L timing checks the Haskell
    /// diffusion PR does not yet enforce. Only meaningful with `fake_announce`;
    /// default `0` (no skew).
    pub announce_slot_offset: i64,
    /// Number of **fabricated** announcements to emit per tick when
    /// `fake_announce` — the `announce-flood` action. `0`/`1` = a single
    /// fabrication (honest-shaped); `> 1` = flood / junk-message DoS, exploiting
    /// the absent junk-detection + ≤ 2-per-election server discipline. Default
    /// `0` keeps `ControlSignal::default()` honest.
    pub announce_flood_count: u32,
}

/// Mempool-domain actuator inputs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MempoolControl {
    /// EB/tx processing filter.
    pub tx_filter: TxFilterPolicy,
    pub announce_filter: TxAnnouncePolicy,
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum TxAnnouncePolicy {
    #[default]
    None,

    /// Never announce (some of) txs
    WithholdAnnouncements {
        /// Delay in announcements for slots
        /// 0 -- dont' delay (effectively no filtering);
        /// Set to MAX_INT to never announce
        /// If transaction is removed from mempool before delay has elapsed - it's not announced
        /// at all.
        withholding_slots: u64,

        /// true if policy applies only to generated by current node
        tx_producer_only: bool
    }
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
        assert_eq!(d.praos.fake_eb, None);
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
