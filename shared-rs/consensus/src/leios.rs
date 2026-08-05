//! Leios consensus state machine (CIP-0164 Linear Leios variant).
//!
//! `LeiosState` owns:
//! - the per-EB election state (via [`Elections`])
//! - the Leios-fetch state (per-EB tx-hash manifests, in-flight fetches,
//!   pending bitmap requests)
//! - the local node's voting configuration
//!
//! Sans-IO: every public method either mutates state and returns a
//! `Vec<LeiosEffect>` describing what the caller's I/O layer should
//! dispatch — fetch an EB or its txs, ask for votes, record a manifest
//! for serving back to peers, hand a block to the validator, emit a
//! vote, raise telemetry — or returns a pure query result.
//!
//! Vote bodies are not built here.  When the local node is eligible to
//! vote, the state emits an [`LeiosEffect::EmitVote`] carrying logical
//! args (PV flag, NPV eligibility signature); the I/O layer encodes the
//! wire-format vote body and sends it.  Same principle as `praos`:
//! this crate is format-agnostic.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::ops::Not;
use std::time::{Duration, Instant};

use serde::Serialize;
use tracing::info;

use crate::aggregation::QuorumFormed;
use crate::behaviour::tree::control::{ControlSignal, TxFilterPolicy, VotePolicy};
use crate::committee;
use crate::config::CommitteeSelection;
use crate::elections::{Elections, SlotEffect};
use crate::fetch::{
    CandidateTracker, EbFetchPolicy, EbTxsFetchPolicy, LowestRttFirst, PeerRtt, UniformRtt,
};
use crate::mempool::{EbKey, TxBody, TxId};
use crate::peer::PeerId;
use crate::pipeline::PipelineConfig;
use crate::types::{hex_prefix, Point};
use crate::types::Vote;

/// How long an in-flight fetch entry remains "active" before being
/// considered stale and eligible for retry.
pub const IN_FLIGHT_TTL: Duration = Duration::from_secs(15);

// ---------------------------------------------------------------------------
// Voting configuration
// ---------------------------------------------------------------------------

/// Per-node voting configuration.  The committee selection rule, the
/// node's own stake, total network stake, and the number of persistent
/// committee seats this node holds (pre-computed at startup from the
/// stake registry).  `*_vote_bytes` are the encoded body sizes the I/O
/// layer should pad each emitted vote body to.
#[derive(Debug, Clone)]
pub struct VotingConfig {
    pub committee_selection: CommitteeSelection,
    pub stake: u64,
    pub total_stake: u64,
    pub persistent_vote_bytes: usize,
    pub non_persistent_vote_bytes: usize,
    /// Number of PV seats this node holds in the persistent committee.
    /// `0` means no PV vote is emitted regardless of EB.
    pub persistent_seats: u32,
    /// Should the election be left eligible across the voting window
    /// when a slot's vote attempt didn't succeed?  Covers both paths:
    ///
    /// - **predicate failure** (`WrongEB` / `LateRBHeader` /
    ///   `MissingTX`): a later slot may see chain-tip / mempool state
    ///   that flips the predicate to success.  `LateEB` stays permanent
    ///   either way.
    /// - **lottery loss** (no PV seat and no NPV win): the NPV trial
    ///   re-runs with a fresh per-slot VRF input, so a non-seated voter
    ///   has independent retries each slot.
    ///
    /// `true` (default) matches the CIP-0164 reading that the voting
    /// window is the licence to vote at any in-window moment.
    /// `false` mirrors `linear_leios.rs`'s single-shot behaviour: one
    /// decision per (voter, EB), one NPV trial, no retries.  Useful
    /// for like-for-like comparisons against the older sim and for
    /// adversarial simulations.
    pub retry_vote_in_window: bool,
    /// Should the voting predicates be evaluated at all?
    ///
    /// `true` (default) runs the CIP-0164 vote predicates on every
    /// `SlotEffect::EligibleToVote`, emits structured `NoVote` /
    /// `EmitVote` effects, and logs the per-EB voting decision.
    ///
    /// `false` skips the predicate entirely: no `decide_vote` call, no
    /// `EmitVote` / `NoVote` effects, no `voting on eb` / `no vote on
    /// eb` log lines.  The election still records `EligibleToVote`
    /// (so observers can see the lottery fire), but this node neither
    /// produces a vote nor logs a decision.
    ///
    /// Intended for nodes that *cannot* meaningfully participate in
    /// voting because either:
    ///
    /// - they hold no stake (so they'd never need a cert), or
    /// - they have no view of the network's stake distribution (so
    ///   they can't compute the stake-weighted quorum threshold even
    ///   if they observed every vote on the wire),
    ///
    /// without polluting operator logs with spurious `WrongEB`
    /// abstentions caused by the wrapper never populating
    /// [`ChainTipContext`].  The decision is the I/O wrapper's; this
    /// crate just respects the flag.
    pub evaluate_votes: bool,
}

// ---------------------------------------------------------------------------
// Voting predicates
// ---------------------------------------------------------------------------

/// Why a CIP-0164 voting predicate rejected this EB at decision time.
/// Surfaced via [`LeiosEffect::NoVote`] so the I/O layer can emit
/// matching telemetry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NoVoteReason {
    /// The EB was first seen too late to fit within its voting window
    /// (`announced_slot + 3·Δhdr + L_vote`).
    LateEB,
    /// The chain-tip RB header arrived later than `eb_slot + Δhdr`,
    /// past the equivocation-resistant cutoff.
    LateRBHeader,
    /// The adopted chain-tip RB announces a *different* EB than this one.
    /// A phase mismatch: the voter has a tip, but the tip has advanced to
    /// an RB that endorses a later EB, so the EB under vote is no longer
    /// the one the tip references. Distinct from [`NoVoteReason::NoChainTip`]
    /// (no adopted tip at all), which was previously folded in here.
    WrongEB,
    /// No adopted chain tip is available yet (`rb_header_arrival_slot`
    /// is unset), so the announcement predicate cannot be evaluated —
    /// e.g. a follower that hasn't populated its `ChainTipContext`, or a
    /// node still catching up. Split out from [`NoVoteReason::WrongEB`]
    /// so that `WrongEB` means specifically the announcement *mismatch*,
    /// making a drifting tip/EB phase mismatch directly identifiable in
    /// telemetry rather than shadowed by the empty-context case.
    NoChainTip,
    /// At least one transaction the EB references is unknown locally.
    /// Only meaningful in TX-by-references mode; the wrapper supplies
    /// the `tx_known` callback that drives this check.
    MissingTX {
        /// Total number of required TX for EB
        required: usize,
        /// Number of pinned TX, available in the mempool
        present_pinned: usize,
        /// Number of unpinned TX, available in the mempool
        present_unpinned: usize,
    },
    /// The EB body has not been validated locally yet — either the
    /// body hasn't been received (vote-placeholder election) or the
    /// body is present but the validator hasn't ratified it
    /// (`on_validated_eb` not yet fired).  All referenced TXs may
    /// already be available; the distinction from `MissingTX` is that
    /// the tx-set is *not* the blocker — local validation is.  CIP-0164
    /// requires the voter to validate the EB body before voting; this
    /// blocks the local emission while cert assembly from peer votes
    /// can still proceed via the `aggregation` module.
    EBValidating,
    /// The local node has detected RB-header equivocation at the EB's
    /// announcement slot — two distinct RB headers signed by the same
    /// issuer at the same slot.  CIP-0164: honest voters abstain from
    /// voting for any EB associated with an equivocating slot.  The
    /// 3·Δhdr `EquivocationCheck` phase is the collection window for
    /// this detection; once voting opens, the flag is consulted from
    /// [`ChainTipContext::equivocating_slots`].
    EquivocatingRB,
    /// Voluntary abstention — the voter chose not to cast a vote for
    /// policy reasons rather than because a CIP-0164 predicate
    /// rejected the EB.  Never emitted by the honest voting
    /// predicates; only via a [`crate::behaviour::Behaviour`] override
    /// (e.g. `LazyVoter`).  Useful as a telemetry distinguisher so
    /// sweeps can tell adversarial-stake abstentions apart from real
    /// `WrongEB` / `MissingTX` / `LateRBHeader` predicate failures.
    Declined,
}

impl NoVoteReason {
    /// Stable, field-free tag for telemetry and aggregation (kebab-case, matching
    /// the serde `rename_all`). Use this for the wire/telemetry string instead of
    /// `format!("{self:?}")`: the Debug form of the struct variant `MissingTX`
    /// includes its fields (e.g. `"MissingTX { required: 1, .. }"`), which breaks
    /// exact-match aggregation downstream. `tag()` always returns just the variant
    /// name, so consumers can match it exactly.
    pub fn tag(&self) -> &'static str {
        match self {
            NoVoteReason::LateEB => "late-eb",
            NoVoteReason::LateRBHeader => "late-rb-header",
            NoVoteReason::WrongEB => "wrong-eb",
            NoVoteReason::NoChainTip => "no-chain-tip",
            NoVoteReason::MissingTX { .. } => "missing-tx",
            NoVoteReason::EBValidating => "eb-validating",
            NoVoteReason::EquivocatingRB => "equivocating-rb",
            NoVoteReason::Declined => "declined",
        }
    }
}

/// Result of evaluating the CIP-0164 voting predicates for one EB.
///
/// `Ok((emit_pv, npv_signature))` means the voter cast at least one
/// body: `emit_pv` flags the persistent committee body, and
/// `npv_signature` carries the eligibility signature when the
/// non-persistent lottery hit (`None` otherwise).  `Err(reason)` means
/// abstention with a structured cause; see [`NoVoteReason`].
pub type VoteDecision = Result<(bool, Option<Vec<u8>>), NoVoteReason>;

/// Chain-tip metadata the I/O wrapper feeds into [`LeiosState`] so the
/// `LateRBHeader` / `WrongEB` voting predicates can run.
///
/// The wrapper updates this whenever the adopted chain tip changes —
/// typically right after handling a `PraosEffect::InjectBlock` or
/// observing a successful fork switch.  Defaults to "no chain tip yet"
/// (both fields `None`); voting predicates treat that as `WrongEB`.
#[derive(Debug, Clone, Default)]
pub struct ChainTipContext {
    /// Slots at which RB-header equivocation has been detected.  Fed
    /// by the wrapper from [`crate::praos::PraosState::equivocating_rb_slots`]
    /// on every chain-tip refresh.  CIP-0164 voters consult this in
    /// `decide_vote`: a hit means abstain (no vote for any EB
    /// associated with that slot).
    pub equivocating_slots: std::collections::BTreeSet<u64>,
    /// Slot at which the wrapper received the adopted RB's header.
    pub rb_header_arrival_slot: Option<u64>,
    /// EB hash announced by the adopted RB header, if any.
    pub eb_announcement: Option<[u8; 32]>,
    /// Hash of the adopted RB header itself (the block carrying
    /// `eb_announcement`).  Carried into the emitted vote as its
    /// `announcing_rb_hash`, binding the vote to this specific RB rather
    /// than the equivocable EB hash.  `None` when no RB tip is known.
    pub tip_rb_hash: Option<[u8; 32]>,
    /// Production slot of the adopted RB.  Drives chain-progress
    /// pruning in [`LeiosState::on_slot`]: under the strict
    /// parent-only cert rule, every EB announced at a slot < tip_rb_slot
    /// is permanently dead (no future RB can certify it).  Per-EB state
    /// — elections, manifests, offers, fetches — can be dropped
    /// immediately once the chain has moved past.
    pub tip_rb_slot: Option<u64>,
}

// ---------------------------------------------------------------------------
// Effect type
// ---------------------------------------------------------------------------

/// What the I/O layer should do as a result of a state mutation.
#[derive(Debug, Clone)]
pub enum LeiosEffect {
    /// Request the EB body from the listed peers.  `peers` is the
    /// [`EbFetchPolicy`]'s ranked picks — the I/O wrapper dispatches
    /// one fetch per peer (so a `BroadcastN` policy fans out, while
    /// `LowestRttFirst` gives a single peer).
    FetchLeiosBlock { point: Point, peers: Vec<PeerId> },
    /// Request transactions in an EB selected by `bitmap` (sparse map of
    /// 64-bit segments keyed by 16-bit segment index).  `peers` chosen
    /// by [`EbTxsFetchPolicy`].
    FetchLeiosBlockTxs {
        point: Point,
        bitmap: BTreeMap<u16, u64>,
        peers: Vec<PeerId>,
    },
    /// Record the EB-tx manifest in the network-side store so this
    /// node can serve EB-tx requests back to peers. `source` is the
    /// peer that supplied the EB body (`None` if self-produced); the
    /// LeiosNotify server skips re-offering the resulting BlockTxsOffer
    /// back to that peer.
    RecordLeiosEbManifest {
        source: Option<PeerId>,
        point: Point,
        eb_key: Option<EbKey>,
        tx_hashes: Vec<TxId>,
    },

    /// The local node is eligible to vote for this EB.  The I/O layer
    /// should build the vote body (or bodies, when both PV and NPV
    /// apply) using the wire-format vote-body encoder, then ship them
    /// in one batch.  The state machine has already committed to the
    /// vote: it has marked the election as voted before emitting this
    /// effect.
    EmitVote {
        eb_slot: u64,
        eb_hash: [u8; 32],
        /// Hash of the RB that announced this EB (`chain_tip_ctx.tip_rb_hash`).
        /// Becomes the emitted vote's `announcing_rb_hash`.  Zero-filled
        /// when the tip RB hash is unknown (should not happen once an EB
        /// is announced, but keeps the effect total).
        announcing_rb_hash: [u8; 32],
        /// True if a PV (persistent committee) body should be emitted.
        emit_pv: bool,
        /// Some(sig) if an NPV body should be emitted, carrying this
        /// eligibility signature.  None for PV-only or no-vote cases.
        npv_signature: Option<Vec<u8>>,
    },

    /// The local node entered the Voting phase for this EB but a
    /// CIP-0164 voting predicate failed; no vote was emitted.  Sibling
    /// to [`LeiosEffect::EmitVote`] — the election is `mark_voted` either way, so
    /// this fires at most once per EB.
    NoVote {
        eb_slot: u64,
        eb_hash: [u8; 32],
        reason: NoVoteReason,
    },

    /// Submit a fetched EB body for ledger validation.
    ValidateEb { point: Point },

    /// Telemetry event the I/O layer should forward to its sink.
    EmitTelemetry(LeiosTelemetryEvent),
}

/// Structured telemetry events emitted by the state machine.  The I/O
/// layer adapts each into its own event type before persisting.
#[derive(Debug, Clone)]
pub enum LeiosTelemetryEvent {
    QuorumReached {
        eb_slot: u64,
        voted_weight: u64,
        voters: usize,
        /// Expected total committee weight (quorum denominator) — lets the
        /// coordinator compute the quorum margin `voted_weight / expected_weight`.
        expected_weight: u64,
    },
    ElectionExpired {
        eb_slot: u64,
        had_quorum: bool,
        /// Final accumulated voted weight at end-of-round (incl. failed
        /// elections) — the true quorum-margin numerator.
        voted_weight: u64,
        voters: usize,
        /// Quorum denominator (`expected_total_weight`).
        expected_weight: u64,
    },
    LeiosElectionInfo {
        eb_slot: u64,
        perm_committee: bool,
    },
}

// ---------------------------------------------------------------------------
// EB-tx response matching
// ---------------------------------------------------------------------------

/// Result of matching a `LeiosBlockTxsReceived` response against the
/// cached manifest.  The I/O layer forwards `matched_bodies` to the
/// validator and re-issues a fetch for `remaining_bitmap` against a
/// different peer if non-empty.
#[derive(Debug, Clone)]
pub struct EbTxMatchOutcome {
    /// Bodies whose blake2b-256 hash maps to a requested manifest
    /// index, in ascending manifest-index order.
    pub matched_bodies: Vec<TxBody>,
    /// Number of indices that the original request bitmap selected.
    /// Zero means "manifest not cached at request time" (fallback path).
    pub requested: usize,
    /// Indices we requested but didn't receive a matching body for.
    pub remaining_bitmap: BTreeMap<u16, u64>,
}

/// Snapshot of [`LeiosState`]'s internal collection sizes plus
/// `CandidateTracker` sub-counts.  Emitted per slot via telemetry so
/// any unbounded growth shows up as a non-flat trend line in JSONL.
#[derive(Debug, Clone, Serialize)]
pub struct LeiosStateSizes {
    pub elections: usize,
    pub eb_tx_hashes: usize,
    pub eb_tx_hash_entries_total: usize,
    pub pending_eb_tx_fetches: usize,
    pub pending_eb_tx_bitmaps_total: usize,
    pub endorsed_unvalidated_ebs: usize,
    pub in_flight: usize,
    pub equivocating_slots: usize,
    pub cand_block_offers: usize,
    pub cand_eb_offers: usize,
    pub cand_eb_txs_offers: usize,
    pub cand_pending_block: usize,
    pub cand_pending_eb: usize,
    pub cand_pending_eb_txs: usize,
    pub cand_eb_txs_attempts: usize,
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/// Leios consensus state for one node.
///
/// Fields are `pub` so adapter code can read and selectively
/// manipulate them.  Treat them as state-machine internals: prefer the
/// public methods, which preserve invariants and emit the right
/// effects.
pub struct LeiosState {
    pub node_id: String,
    pub elections: Elections,
    pub voting_config: VotingConfig,
    pub pipeline: PipelineConfig,

    /// Per-EB ordered tx-hash list, decoded from the EB manifest on
    /// `on_eb_received`.  Tagged with the EB's announced slot so stale
    /// entries can be pruned in `on_slot`.
    pub eb_tx_hashes: BTreeMap<[u8; 32], (u64, Vec<TxId>)>,
    /// EB hashes whose body has validated locally, keyed to the EB's
    /// slot for pruning. Because elections are created at *announcement*
    /// (via [`Self::set_chain_tip_context`]) — which can happen after
    /// the body validates (notably for a producer validating its own
    /// EB) — `on_validated_eb` records the hash here so a later
    /// `announce_from_rb` can start the election already body-validated.
    /// Without this the validated signal would be lost whenever
    /// validation precedes announcement, and every voter would abstain
    /// with `EBValidating`.
    validated_eb_bodies: BTreeMap<[u8; 32], u64>,

    /// Per-EB requested bitmap.  Set when a `FetchLeiosBlockTxs` is
    /// emitted; used at response time to verify which manifest indices
    /// were actually fulfilled.
    /// Map: RB-hash -> (slot, bitmap)
    pub pending_eb_tx_fetches: BTreeMap<[u8; 32], (u64, BTreeMap<u16, u64>)>,

    /// If txs were not fully retrieved from leios nodes, EB Point is written
    /// so the retrieval attempt can be repeated next slot.
    /// Inserted when `retry_eb_tx_fetch` ran out of peers.
    /// Removed, when txs fetch is restarted, and when all txs are fetched.
    /// Pruned when EB (corresponding to the Point) becomes outdated.
    postponed_eb_tx_requests: HashSet<Point>,

    /// EB hashes the local node holds a chain-committed cert for whose
    /// body it has not validated locally.  Producer-side EB-safety
    /// gate: while non-empty, the next self-produced RB must emit an
    /// empty body and no fresh EB announcement (the cert it carries is
    /// independent of body inclusion).  Insertion via
    /// `on_chain_endorsement`; cleared by `on_validated_eb` and by
    /// pipeline-aging in `on_slot`.  Values are EB slot, tagged so the
    /// entry can age out symmetrically with the election lifecycle.
    pub endorsed_unvalidated_ebs: BTreeMap<[u8; 32], u64>,
    /// Leios points / fetch gates currently in flight.
    pub in_flight: BTreeMap<Point, Instant>,

    /// Chain-tip metadata fed in by the I/O wrapper.  Used by the
    /// `LateRBHeader` / `WrongEB` voting predicates.
    pub chain_tip_ctx: ChainTipContext,

    /// Offer maps + pending dedup + EB-txs retry tracking.  The I/O
    /// wrapper records every observed offer here via `on_*_offered`
    /// before a fetch decision is made.
    pub candidates: CandidateTracker,

    /// Strategy for picking peer(s) to fetch each EB body from.
    pub eb_policy: Box<dyn EbFetchPolicy + Send + Sync>,
    /// Strategy for picking peer(s) to fetch EB transactions from.
    pub eb_txs_policy: Box<dyn EbTxsFetchPolicy + Send + Sync>,
    /// Live per-peer RTT lookup, consulted by every fetch policy.
    pub rtt: Box<dyn PeerRtt + Send + Sync>,

    /// The control signal applied by the BT tick each slot (`apply_control`).
    /// The whole signal is stored (not just the Leios slice) so this state's
    /// actuators can read cross-domain fields — e.g. the t22 EB-processing
    /// filter reads `control.mempool.tx_filter`. Honest by default.
    pub control: ControlSignal,
}

/// Auxiliary enum for the `tx_known` callback passed into `LeiosState::on_slot`.
/// Gives extended info about tx status in mempool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxAvailability {
    Absent,

    /// pinned -- true if pinned in mempool, i.e. specified in eb_pinned struct
    /// ours -- tx is generated by current node
    /// gen_slot -- slot number when the tx was included into mempool
    Present { pinned: bool, ours: bool, gen_slot: u64 },
}

impl LeiosState {
    /// Construct a new state with the default fetch policy
    /// ([`LowestRttFirst`] for both fetch-bearing traffic classes) and a
    /// zero-RTT [`UniformRtt`] oracle.
    pub fn new(
        node_id: String,
        elections: Elections,
        voting_config: VotingConfig,
        pipeline: PipelineConfig,
    ) -> Self {
        Self::with_fetch(
            node_id,
            elections,
            voting_config,
            pipeline,
            Box::new(LowestRttFirst),
            Box::new(LowestRttFirst),
            Box::new(UniformRtt(Duration::ZERO)),
        )
    }

    /// Construct a new state with explicit fetch routing handles.
    #[allow(clippy::too_many_arguments)]
    pub fn with_fetch(
        node_id: String,
        elections: Elections,
        voting_config: VotingConfig,
        pipeline: PipelineConfig,
        eb_policy: Box<dyn EbFetchPolicy + Send + Sync>,
        eb_txs_policy: Box<dyn EbTxsFetchPolicy + Send + Sync>,
        rtt: Box<dyn PeerRtt + Send + Sync>,
    ) -> Self {
        Self {
            node_id,
            elections,
            voting_config,
            pipeline,
            eb_tx_hashes: BTreeMap::new(),
            validated_eb_bodies: BTreeMap::new(),
            pending_eb_tx_fetches: BTreeMap::new(),
            postponed_eb_tx_requests: HashSet::new(),
            endorsed_unvalidated_ebs: BTreeMap::new(),
            in_flight: BTreeMap::new(),
            chain_tip_ctx: ChainTipContext::default(),
            candidates: CandidateTracker::new(),
            eb_policy,
            eb_txs_policy,
            rtt,
            control: ControlSignal::default(),
        }
    }

    /// Apply the per-slot [`ControlSignal`] produced by the BT tick, storing it
    /// for the actuators in this state to read (the vote policy and the t22
    /// EB-processing filter). Called once per slot by the I/O wrapper.
    pub fn apply_control(&mut self, control: &ControlSignal) {
        self.control = control.clone();
    }

    /// Update the chain-tip metadata used by the voting predicates.
    /// The I/O wrapper calls this whenever the adopted chain tip
    /// changes (e.g., after a successful fork switch or self-produced
    /// block).
    pub fn set_chain_tip_context(&mut self, ctx: ChainTipContext) {
        // When the adopted tip RB announces an EB, ensure an election
        // exists for it, keyed by the announcing RB.  Idempotent: a
        // no-op once the election is created.  This is the sole election
        // creation point — votes aggregate against it.
        if let (Some(rb_hash), Some(rb_slot), Some(eb_hash)) =
            (ctx.tip_rb_hash, ctx.tip_rb_slot, ctx.eb_announcement)
        {
            self.elections.announce_from_rb(rb_hash, rb_slot, eb_hash);
            // If we already validated this EB's body before its
            // announcing RB became the tip (common for a producer
            // validating its own EB), carry that state onto the
            // freshly-created election.
            if self.validated_eb_bodies.contains_key(&eb_hash) {
                self.elections.mark_body_validated(&eb_hash);
            }
        }
        self.chain_tip_ctx = ctx;
    }

    /// Replace the EB fetch policy.
    pub fn set_eb_policy(&mut self, policy: Box<dyn EbFetchPolicy + Send + Sync>) {
        self.eb_policy = policy;
    }

    /// Replace the EB-txs fetch policy.
    pub fn set_eb_txs_policy(&mut self, policy: Box<dyn EbTxsFetchPolicy + Send + Sync>) {
        self.eb_txs_policy = policy;
    }

    /// Take postponed requests to restart them.
    pub fn take_postponed_eb_tx_requests(&mut self) -> HashSet<Point> {
        std::mem::take(&mut self.postponed_eb_tx_requests)
    }


    /// Build the sparse CIP-0164 bitmap of manifest indices whose tx
    /// bodies are *not* locally available, ready to feed
    /// [`Self::on_eb_txs_offered`] or directly into the wire
    /// `MsgLeiosBlockTxsRequest`.
    ///
    /// Looks up the EB's manifest in [`Self::eb_tx_hashes`] (populated
    /// by `on_eb_received`).  Returns an empty bitmap when:
    /// - the manifest isn't cached yet (EB body fetch still in flight),
    /// - every referenced tx is already in the local mempool
    ///   ([`crate::mempool::MempoolState::has_tx`] true for all),
    /// - the EB has no referenced txs.
    ///
    /// Returning an empty bitmap for "no manifest yet" deliberately
    /// suppresses the fetch — re-issuing with a full
    /// [`crate::bitmap::select_all`] would trigger a retry storm
    /// against a peer that doesn't have the bodies either.  The peer's
    /// notify-loop will re-offer once the manifest is cached.
    pub fn missing_eb_tx_bitmap(
        &self,
        eb_hash: &[u8; 32],
        mempool: &crate::mempool::MempoolState,
    ) -> std::collections::BTreeMap<u16, u64> {
        let Some((_, tx_hashes)) = self.eb_tx_hashes.get(eb_hash) else {
            return std::collections::BTreeMap::new();
        };
        let missing: Vec<u32> = tx_hashes
            .iter()
            .enumerate()
            .filter_map(|(i, tx_id)| mempool.has_tx(tx_id).not().then_some(i as u32))
            .collect();
        crate::bitmap::from_indices(&missing)
    }

    /// Replace the per-peer RTT oracle.
    pub fn set_rtt(&mut self, rtt: Box<dyn PeerRtt + Send + Sync>) {
        self.rtt = rtt;
    }

    // -- Slot tick ----------------------------------------------------------

    /// Cleaning procedure for all left-overs from previous EB and EB tx fetching attempts,
    /// Main reason for the procedure: to avoid memory leaks.
    fn prune_chain_progress(&mut self, min_keep: u64) {
        self.eb_tx_hashes.retain(|_, (s, _)| *s >= min_keep);
        self.postponed_eb_tx_requests.retain(|p| p.get_slot().is_some_and(|s| s >= min_keep));
        self.pending_eb_tx_fetches
            .retain(|_, (slot, _)| *slot >= min_keep);
        self.endorsed_unvalidated_ebs.retain(|_, s| *s >= min_keep);
        self.validated_eb_bodies.retain(|_, s| *s >= min_keep);
        // Pruned elections emit their final voting tally so the quorum-
        // margin sensor sees end-of-round weight (incl. elections that
        // never reached quorum), not the always-~τ formation-time reading.
        for eff in self.elections.prune_below_slot(min_keep) {
            if let crate::elections::SlotEffect::Expired {
                eb_slot,
                had_quorum,
                voted_weight,
                voters,
                expected_weight,
                ..
            } = eff
            {
                fx.push(LeiosEffect::EmitTelemetry(
                    LeiosTelemetryEvent::ElectionExpired {
                        eb_slot,
                        had_quorum,
                        voted_weight,
                        voters,
                        expected_weight,
                    },
                ));
            }
        }
        self.candidates.prune_below_slot(min_keep);
    }

    /// Drive the election state machine forward, decide voting for any
    /// election entering the Voting phase, and prune stale Leios-fetch
    /// state.  Returns the effects to dispatch.
    ///
    /// `tx_known` is the wrapper's predicate for "do we have this TX
    /// locally?" — used by the CIP-0164 `MissingTX` voting check in
    /// TX-by-references mode. It should return `TxAvailability::PresentPinned`
    /// for any TX that is in the mempool, and `TxAvailability::PresentUnpinned`
    /// for any TX that is in the mempool, but not pinned (yet?).
    ///
    /// Wrappers without a mempool surface yet can pass
    /// `&|_| TxAvaialability::PresentPinned` (or Unpinned); in that case
    /// the predicate is a no-op.
    pub fn on_slot(&mut self, slot: u64, tx_known: &dyn Fn(&TxId) -> TxAvailability) -> Vec<LeiosEffect> {
        let mut fx: Vec<LeiosEffect> = Vec::new();
        for eff in self.elections.on_slot(slot) {
            match eff {
                SlotEffect::EligibleToVote {
                    rb_hash,
                    eb_hash,
                    eb_slot,
                    eb_seen_slot,
                } => {
                    // Vote-evaluation gate.  A node configured as
                    // `evaluate_votes = false` (typically a stake=0
                    // follower or any node without a voter stake table)
                    // skips the entire predicate path: no `decide_vote`
                    // call, no `EmitVote` / `NoVote` effect, no log.
                    // Mark the election done so it doesn't re-fire the
                    // same `EligibleToVote` every slot for the rest of
                    // the voting window — there's nothing further this
                    // node can do for it.
                    if !self.voting_config.evaluate_votes {
                        self.elections.mark_voted(&rb_hash);
                        continue;
                    }
                    let honest_vote =
                        self.decide_vote(&rb_hash, &eb_hash, eb_slot, eb_seen_slot, slot, tx_known);
                    // Vote actuator: the BT control signal may override the
                    // honest predicate with a policy abstention (e.g. the
                    // lazy-voter action sets `leios.vote = Abstain(reason)`).
                    let resolved = match &self.control.leios.vote {
                        VotePolicy::Honest => honest_vote,
                        VotePolicy::Abstain(reason) => Err(*reason),
                    };
                    match resolved {
                        Ok((emit_pv, npv_signature)) if emit_pv || npv_signature.is_some() => {
                            info!(
                                node_id = %self.node_id,
                                eb_slot,
                                emit_pv,
                                emit_npv = npv_signature.is_some(),
                                "voting on eb"
                            );
                            self.elections.mark_voted(&rb_hash);
                            // The election is keyed by its announcing RB,
                            // so that hash *is* the vote's target.
                            fx.push(LeiosEffect::EmitVote {
                                eb_slot,
                                eb_hash,
                                announcing_rb_hash: rb_hash,
                                emit_pv,
                                npv_signature,
                            });
                        }
                        Ok(_) => {
                            // Lottery loss: no PV seat and no NPV win.
                            // The NPV trial uses a per-slot VRF input, so
                            // a non-seated voter has up to `vote_window`
                            // independent chances to win across the
                            // window.  Don't mark_voted by default — the
                            // election re-fires next slot and runs a
                            // fresh trial.
                            //
                            // `retry_vote_in_window = false` collapses
                            // this to one trial per (voter, EB), matching
                            // `linear_leios.rs`'s single-shot lottery.
                            if !self.voting_config.retry_vote_in_window {
                                self.elections.mark_voted(&rb_hash);
                            }
                        }
                        Err(reason) => {
                            info!(
                                node_id = %self.node_id,
                                eb_slot,
                                ?reason,
                                "no vote on eb"
                            );
                            // Only `LateEB` is permanent — `eb_seen_slot`
                            // is fixed once observed, and a late EB stays
                            // late.  `WrongEB` / `LateRBHeader` /
                            // `MissingTX` are transient: the chain tip
                            // may catch up next slot, or the missing TX
                            // may arrive.  Suppress `mark_voted` so the
                            // election re-fires `EligibleToVote` while
                            // it's still in the Voting phase, giving
                            // slow propagation a chance to land.
                            //
                            // `retry_vote_in_window = false` collapses
                            // every reason to permanent — single-shot
                            // semantics matching `linear_leios.rs`.
                            if matches!(reason, NoVoteReason::LateEB)
                                || !self.voting_config.retry_vote_in_window
                            {
                                self.elections.mark_voted(&rb_hash);
                            }
                            fx.push(LeiosEffect::NoVote {
                                eb_slot,
                                eb_hash,
                                reason,
                            });
                        }
                    }
                }
                SlotEffect::Expired {
                    eb_slot,
                    had_quorum,
                    voted_weight,
                    voters,
                    expected_weight,
                    ..
                } => {
                    fx.push(LeiosEffect::EmitTelemetry(
                        LeiosTelemetryEvent::ElectionExpired {
                            eb_slot,
                            had_quorum,
                            voted_weight,
                            voters,
                            expected_weight,
                        },
                    ));
                }
            }
        }
        // Chain-progress prune (Linear Leios with strict parent-only
        // cert rule): once the adopted chain tip moves to RB N+1, the
        // EB announced by RB N is permanently dead — no future RB can
        // certify it because the cert target is fixed to the parent.
        // So per-EB state at slot < tip_rb_slot is dead weight no
        // matter how recent it is.  Conversely, on a stuck chain the
        // tip's own EB stays at slot == tip_rb_slot and is preserved
        // (it can still be attached as a cert by whatever child RB
        // eventually arrives).  Before the local node has adopted any
        // RB (`tip_rb_slot = None`), no prune fires — every received
        // EB is potentially relevant to the chain we'll soon adopt.
        if let Some(min_keep) = self.chain_tip_ctx.tip_rb_slot {
            self.prune_chain_progress(min_keep);
        }
        fx
    }

    /// Decide the vote outcome for an EB entering the Voting phase.
    ///
    /// Runs the CIP-0164 voting predicates first; on failure returns
    /// the relevant `NoVoteReason` and no vote is emitted.  On success
    /// returns `(emit_pv, npv_signature)` — the same shape as the
    /// underlying lottery decision.  `(false, None)` means the local
    /// node was eligible to enter the Voting window but neither held a
    /// persistent seat nor won an NPV trial; the caller treats this as
    /// silent "lottery loss" (no telemetry, no `mark_voted`).
    fn decide_vote(
        &self,
        rb_hash: &[u8; 32],
        eb_hash: &[u8; 32],
        eb_slot: u64,
        eb_seen_slot: u64,
        eb_current_slot: u64,
        tx_known: &dyn Fn(&TxId) -> TxAvailability,
    ) -> VoteDecision {
        // Predicate 1: LateEB.  The EB must have arrived before its
        // voting window closes.  The phase machine already filters out
        // most late arrivals (EligibleToVote only fires during Voting),
        // but check explicitly for telemetry parity.
        let voting_end = 3 * self.pipeline.delta_hdr + self.pipeline.vote_window;
        if eb_seen_slot.saturating_sub(eb_slot) >= voting_end {
            return Err(NoVoteReason::LateEB);
        }

        // Predicate 2/3: chain tip must be set, must reference this EB,
        // and its header must have arrived within the equivocation
        // cutoff (`eb_slot + Δhdr`).
        let Some(rb_arrival) = self.chain_tip_ctx.rb_header_arrival_slot else {
            return Err(NoVoteReason::NoChainTip);
        };
        // This election must be the one announced by the *adopted* RB
        // tip.  Keying on the announcing RB (not the EB) is what makes
        // this equivocation-safe: an equivocating RB that announced the
        // same EB has a different `rb_hash`, so its election never
        // matches the tip and we never vote for it.
        if self.chain_tip_ctx.tip_rb_hash.as_ref() != Some(rb_hash) {
            return Err(NoVoteReason::WrongEB);
        }
        if rb_arrival >= eb_slot + self.pipeline.delta_hdr {
            return Err(NoVoteReason::LateRBHeader);
        }

        // CIP-0164: "It has not detected any equivocating RB header
        // for the same slot."  The 3·Δhdr EquivocationCheck pipeline
        // phase is the collection window; voting only opens after it
        // closes, at which point the wrapper's chain-tip refresh has
        // populated `equivocating_slots` from PraosState.  A hit
        // means abstain — refuse to vote for any EB associated with
        // an equivocating slot, regardless of which EB the chain tip
        // ultimately picks.
        if self.chain_tip_ctx.equivocating_slots.contains(&eb_slot) {
            return Err(NoVoteReason::EquivocatingRB);
        }

        // Predicate 4: MissingTX.  Only checked when we have a manifest
        // for this EB — TX-by-references mode populates `eb_tx_hashes`
        // on `on_eb_received`.  Inlined-TX EBs (no manifest) skip the
        // check; the validator will reject the EB body if it references
        // unknown TXs.
        if let Some((_, tx_hashes)) = self.eb_tx_hashes.get(eb_hash) {
            let mut present = [0, 0]; // [pinned,unpinned]
            let mut origin = [0,0]; // [ours,outside]
            let mut absent = 0;
            let mut decline = false;

            for h in tx_hashes {
                let (pinned, ours, _gen_slot) = match tx_known(h) {
                    TxAvailability::Absent => {
                        decline = true;
                        absent += 1;
                        continue
                    },
                    TxAvailability::Present { pinned, ours, gen_slot } => (pinned, ours, gen_slot)
                };
                origin[ours as usize] += 1;
                present[pinned as usize] += 1;
            }

            let point = Point::Specific {
                hash: rb_hash.clone(),
                slot: eb_slot,
            };
            info!("Voting point {point}, EB {}, seen at {eb_seen_slot}, current {eb_current_slot}; total txs {}, \"\
                   present (pinned {}/unpinned {}) (ours {}/outside {}), absent {absent}",
                hex_prefix(eb_hash), present.iter().sum::<usize>(),
                present[true as usize], present[false as usize], origin[true as usize], origin[false as usize],
            );

            if decline {
                return Err(NoVoteReason::MissingTX {
                    required: tx_hashes.len(),
                    present_pinned: present[true as usize],
                    present_unpinned: present[false as usize],
                });
            }
        }

        // Predicate 5: EBValidating.  Voting attests EB-body validity,
        // so the local node must have ratified the body
        // (`on_validated_eb`) before emitting a vote.  Vote-placeholder
        // elections (created from received peer votes when the body
        // hadn't arrived) and elections with a body still in the
        // validator queue both fail this check.  Distinct from
        // `MissingTX`: the tx-set may already be complete.
        if !self.elections.is_announced(rb_hash) {
            return Err(NoVoteReason::EBValidating);
        }

        // Predicates passed — run the lottery.
        //
        // CIP-0164 partitions pools by stake-ordering into persistent
        // (indices [1, n₁]) and non-persistent candidates (indices
        // [i*, |P|]) — these ranges are disjoint, so a pool with a
        // persistent seat is *not* eligible as an NPV candidate.  A
        // pool emits exactly one vote per election: PV xor NPV.
        let emit_pv = self.voting_config.persistent_seats > 0;
        let n_npv = self
            .voting_config
            .committee_selection
            .non_persistent_voters();
        let npv_signature = if emit_pv || n_npv == 0 {
            None
        } else {
            let sig =
                committee::npv_eligibility_signature(self.node_id.as_bytes(), eb_hash, eb_slot);
            let wins = committee::count_npv_wins(
                &sig,
                self.voting_config.stake,
                self.voting_config.total_stake,
                n_npv,
            );
            if wins > 0 {
                Some(sig)
            } else {
                None
            }
        };
        Ok((emit_pv, npv_signature))
    }

    // -- Network event handlers ---------------------------------------------

    /// A peer offered an EB.  Records the offer, then — if no fetch
    /// for this point is already in flight — runs the [`EbFetchPolicy`]
    /// against the current candidate set and emits one
    /// `FetchLeiosBlock` carrying the chosen peers.  Idempotent: a
    /// repeat offer from the same peer is silently absorbed.
    /// Deterministic per-(EB, node) checksum percentile in `0..100`, used by
    /// the t22 EB-processing filter. Ported verbatim from the old t22 hook so
    /// behaviour is identical.
    fn eb_checksum_pct(&self, point: &Point) -> u8 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        if let Point::Specific { hash, .. } = point {
            hash.hash(&mut hasher);
        }
        self.node_id.hash(&mut hasher);
        (hasher.finish() % 100) as u8
    }

    /// EB-processing actuator (t22): whether to process this EB offer/manifest
    /// under the control signal's tx-filter policy. Honest (`None`) processes
    /// everything; `ChecksumThreshold` processes when the checksum percentile
    /// is below the seat-dependent threshold.
    fn should_process_eb(&self, point: &Point) -> bool {
        match &self.control.mempool.tx_filter {
            TxFilterPolicy::None => true,
            TxFilterPolicy::ChecksumThreshold {
                vote, non_voting, ..
            } => {
                let threshold = if self.voting_config.persistent_seats > 0 {
                    *vote
                } else {
                    *non_voting
                };
                self.eb_checksum_pct(point) < threshold
            }
        }
    }

    /// Whether a received EB's manifest processing is hidden (t22 with
    /// `hide_eb_tx`): only when the policy sets the flag and the EB is filtered.
    fn should_hide_eb_received(&self, point: &Point) -> bool {
        match &self.control.mempool.tx_filter {
            TxFilterPolicy::ChecksumThreshold {
                hide_eb_tx: true, ..
            } => !self.should_process_eb(point),
            _ => false,
        }
    }

    pub fn on_eb_offered(&mut self, point: Point, peer: PeerId, now: Instant) -> Vec<LeiosEffect> {
        // EB-processing filter (t22): the control signal's checksum-threshold
        // policy may drop processing of this EB offer.
        if !self.should_process_eb(&point) {
            tracing::debug!(node_id = %self.node_id, %point, peer = peer.0, "t22: filtered EB offer (checksum-threshold)");
            return Vec::new();
        }
        self.evict_stale_in_flight(now);
        self.candidates.note_eb_offered(point.clone(), peer);
        if self.in_flight.contains_key(&point) {
            return Vec::new();
        }
        let candidates = self.candidates.eb_candidates(&point);
        let peers = self.eb_policy.pick(&point, &candidates, self.rtt.as_ref());
        if peers.is_empty() {
            return Vec::new();
        }
        self.in_flight.insert(point.clone(), now);
        info!(
            node_id = %self.node_id,
            %point,
            peer_count = peers.len(),
            "fetching leios block"
        );
        vec![LeiosEffect::FetchLeiosBlock { point, peers }]
    }

    /// A peer offered EB transactions.  Caller has already computed the
    /// bitmap of indices it doesn't already have (from its mempool).
    /// Per-slot gate dedups concurrent offers; per-peer attempts map
    /// drives retry-after-partial-response in
    /// [`Self::retry_eb_tx_fetch`].
    pub fn on_eb_txs_offered(
        &mut self,
        point: Point,
        peer: PeerId,
        bitmap: BTreeMap<u16, u64>,
        now: Instant,
    ) -> Vec<LeiosEffect> {
        // EB-processing filter (t22): drop tx-offer processing for a filtered EB.
        if !self.should_process_eb(&point) {
            tracing::debug!(node_id = %self.node_id, %point, peer = peer.0, "t22: filtered EB-txs offer (checksum-threshold)");
            return Vec::new();
        }

        self.evict_stale_in_flight(now);
        if !matches!(&point, Point::Specific { .. }) {
            return Vec::new();
        }
        self.candidates.note_eb_txs_offered(point.clone(), peer);
        self.initiate_eb_txs_fetch(&point, bitmap, now)
    }

    /// Initiate fetch restarting, if all previous attempts were unsuscessful: maybe some peers
    /// got new information?
    /// This function depends on the new bitmap, so it's not implemented in full inside
    /// shared consensus. Instead, it's initiated from outer loop -- where Ledger is avaialble;
    /// That outer loop should:
    /// * take all postponed fetches using `take_postponed_eb_requests`,
    /// * recalculate new `bitmap`s (which are unavailable at this level)
    /// * return control/information back onto shared consensus level using this function.
    pub fn retry_postponed_eb_txs_fetches(
        &mut self,
        point: &Point,
        bitmap: BTreeMap<u16, u64>,
        now: Instant,
    ) -> Vec<LeiosEffect> {
        info!("leios_store: restarting fetch for {point}");

        let mut fx = Vec::new();

        self.candidates.clear_eb_txs_attempts(&point);
        fx.append(&mut (self.initiate_eb_txs_fetch(point, bitmap, now)));

        fx
    }

    /// Initiate EB Tx fetch for `point`. Requires `bitmap` of requested transactions
    /// (should be computed before call).
    /// This function is either called from `on_eb_tx_offered`, or independently, from on_slot
    /// (in case of repeated tx fetch attempts).
    pub fn initiate_eb_txs_fetch(
        &mut self,
        point: &Point,
        bitmap: BTreeMap<u16, u64>,
        now: Instant,
    ) -> Vec<LeiosEffect> {
        if !self.should_process_eb(point) {
            return Vec::new();
        }

        // Empty bitmap means the consumer has nothing to fetch (either
        // all txs known locally, or — more commonly — the EB body /
        // manifest hasn't arrived yet so we can't yet name the missing
        // indices). Don't engage the per-slot gate or emit a fetch; wait
        // for the next offer once the manifest lands.
        if bitmap.is_empty() {
            return Vec::new();
        }
        // Per-slot gate keeps multiple offers from triggering parallel
        // fetches; the synthetic hash distinguishes the gate from an
        // EB-body fetch on the same slot.
        let Point::Specific { slot, .. } = point else {
            return Vec::new();
        };
        let gate_key = Point::Specific {
            slot: *slot,
            hash: [0xFE; 32],
        };
        if self.in_flight.contains_key(&gate_key) {
            return Vec::new();
        }
        let candidates = self.candidates.eb_txs_candidates(&point);
        let peers = self
            .eb_txs_policy
            .pick(&point, &bitmap, &candidates, self.rtt.as_ref());
        if peers.is_empty() {
            return Vec::new();
        }
        self.in_flight.insert(gate_key, now);
        if let Point::Specific { slot, hash } = &point {
            self.pending_eb_tx_fetches
                .insert(*hash, (*slot, bitmap.clone()));
        }
        self.candidates.start_eb_txs_fetch(point.clone(), &peers);
        info!(
            node_id = %self.node_id,
            %point,
            bitmap_segments = bitmap.len(),
            peer_count = peers.len(),
            "fetching leios block txs"
        );
        vec![LeiosEffect::FetchLeiosBlockTxs {
            point: point.clone(),
            bitmap,
            peers,
        }]
    }

    /// An EB body arrived.  `manifest_hashes` is the decoded tx-hash
    /// list (or `None` if the body didn't decode as an overflow EB —
    /// e.g., a header-only block).  `source` is the peer that supplied
    /// the EB (`None` for self-produced).  Always emits a `ValidateEb`.
    pub fn on_eb_received(
        &mut self,
        source: Option<PeerId>,
        point: Point,
        manifest_hashes: Option<(Option<EbKey>, Vec<TxId>)>,
    ) -> Vec<LeiosEffect> {
        // EB-processing filter (t22 with `hide_eb_tx`): drop manifest + validate
        // processing for a filtered EB.
        if self.should_hide_eb_received(&point) {
            tracing::debug!(node_id = %self.node_id, %point, "t22: filtered EB-received processing (hide_eb_tx)");
            return Vec::new();
        }
        self.in_flight.remove(&point);
        let mut fx = Vec::new();
        if let (Some((eb_key, tx_hashes)), Point::Specific { slot, hash }) = (manifest_hashes, &point) {
            self.eb_tx_hashes.insert(*hash, (*slot, tx_hashes.clone()));
            fx.push(LeiosEffect::RecordLeiosEbManifest {
                source,
                point: point.clone(),
                eb_key,
                tx_hashes,
            });
        }
        fx.push(LeiosEffect::ValidateEb { point });
        fx
    }

    /// Votes arrived inline (CIP-0164 prototype: full vote bodies are
    /// delivered directly via `LeiosNotify`, no offer/fetch round-trip).
    /// Each vote's compact `voter_id` index is resolved to its node id
    /// via the deterministic voter registry, then fed straight into
    /// aggregation — the mocked `bool` signature needs no ledger
    /// validation step.  Returns telemetry effects for any quorums newly
    /// formed.  Votes whose `voter_id` is outside the local registry
    /// (e.g. a foreign voter from an external relay) carry no resolvable
    /// weight and are skipped; gossip re-serving is the I/O layer's job.
    pub fn on_votes_received(&mut self, votes: Vec<Vote>) -> Vec<LeiosEffect> {
        // Resolve each compact voter index to its node id (owned, so the
        // borrowed `ValidatedVote` views below can lend the bytes).
        let resolved: Vec<(String, Vote)> = votes
            .into_iter()
            .filter_map(|v| {
                self.elections
                    .voter_id_at(v.voter_id)
                    .map(|id| (id.to_string(), v))
            })
            .collect();
        let bodies: Vec<ValidatedVote> = resolved
            .iter()
            .map(|(node_id, v)| ValidatedVote {
                voter_id: node_id.as_bytes(),
                // Prototype votes carry no PV/NPV tag or eligibility
                // signature; treat every inline vote as a committee (PV)
                // vote weighted by the registry.
                tag: 0,
                eligibility_signature: None,
                announcing_rb_hash: &v.announcing_rb_hash,
            })
            .collect();
        self.on_validated_votes(bodies)
    }

    /// EB transactions arrived; clear the per-slot in-flight gate so
    /// subsequent offers can drive fresh fetches.  Bodies are matched
    /// by the I/O layer via [`Self::match_eb_tx_response`].
    pub fn on_eb_txs_received(&mut self, point: &Point, count: usize, peer_id: PeerId) {
        if let Point::Specific { slot, .. } = point {
            let gate_key = Point::Specific {
                slot: *slot,
                hash: [0xFE; 32],
            };
            self.in_flight.remove(&gate_key);
        }
        // Clear the candidate tracker's pending entry so a retry
        // (driven by the wrapper's match outcome) can be issued
        // against a fresh peer.  Without this the retry path keeps
        // hitting `start_eb_txs_fetch == false` and either skipping
        // (if respected) or piling on concurrent fetches (the actual
        // pre-fix symptom: 25-node cluster smoke storm).
        self.candidates.finish_eb_txs_fetch(point);
        info!(
            node_id = %self.node_id,
            origin = %peer_id,
            %point,
            count,
            "leios block txs received"
        );
    }

    // -- Validation outcomes ------------------------------------------------

    /// EB validation succeeded; create an election for it.  Also
    /// clears the EB-safety gate entry — a body that just validated
    /// can no longer make a future RB body unsafe.
    pub fn on_validated_eb(&mut self, point: Point) {
        let (slot, hash) = match &point {
            Point::Specific { slot, hash } => (*slot, *hash),
            Point::Origin => return,
        };
        // The body is in hand and valid.  Flip `body_validated_locally`
        // on every election that already announced this EB, AND record
        // the hash so an election announced *later* (validation can
        // precede announcement — e.g. a producer validating its own EB)
        // starts out body-validated via `set_chain_tip_context`.
        self.validated_eb_bodies.insert(hash, slot);
        self.elections.mark_body_validated(&hash);
        self.endorsed_unvalidated_ebs.remove(&hash);
    }

    /// The local node has observed a chain-committed cert for the EB
    /// at `(eb_slot, eb_hash)` (a self-produced or peer RB whose body
    /// the wrapper is in the process of applying).  If the EB body
    /// has not been validated locally, the producer-side EB-safety
    /// gate fires from this point until either the body validates
    /// (`on_validated_eb`) or the EB ages out of its pipeline.
    /// Idempotent.
    pub fn on_chain_endorsement(&mut self, eb_slot: u64, eb_hash: [u8; 32]) {
        if self.elections.eb_body_validated(&eb_hash) {
            return;
        }
        self.endorsed_unvalidated_ebs
            .entry(eb_hash)
            .or_insert(eb_slot);
    }

    /// Producer-side EB-safety predicate consumed by
    /// [`crate::production::BodyPath::decide`].  True iff the local
    /// node holds a chain-committed cert for at least one EB whose
    /// body it has not validated locally; in that state the next
    /// self-produced RB must emit an empty body and no fresh EB
    /// announcement.
    pub fn has_endorsed_unvalidated_eb(&self) -> bool {
        !self.endorsed_unvalidated_ebs.is_empty()
    }

    /// Vote validation succeeded; attribute each vote to its EB
    /// election.  `vote_bodies` is an iterator yielding decoded vote
    /// bodies — the I/O layer decoded them already (the vote-body wire
    /// format lives outside this crate).  Returns telemetry effects for
    /// any quorums newly formed.
    pub fn on_validated_votes<'a, I>(&mut self, vote_bodies: I) -> Vec<LeiosEffect>
    where
        I: IntoIterator<Item = ValidatedVote<'a>>,
    {
        let mut fx = Vec::new();
        for body in vote_bodies {
            let voter_id_str = String::from_utf8_lossy(body.voter_id).into_owned();
            let weight =
                self.elections
                    .weight_for(&voter_id_str, body.tag, body.eligibility_signature);
            if weight == 0 {
                continue;
            }
            // Dedup key: voter_id || tag.
            let mut key = body.voter_id.to_vec();
            key.push(body.tag);
            if let Some(formed) = self
                .elections
                .record_vote(body.announcing_rb_hash, key, weight)
            {
                let QuorumFormed {
                    eb_slot,
                    voted_weight,
                    voters,
                    expected_weight,
                } = formed;
                fx.push(LeiosEffect::EmitTelemetry(
                    LeiosTelemetryEvent::QuorumReached {
                        eb_slot,
                        voted_weight,
                        voters,
                        expected_weight,
                    },
                ));
            }
        }
        fx
    }

    // -- Pure utility -------------------------------------------------------

    /// Match a body list against the cached EB manifest.  Each body is
    /// hashed (blake2b-256) by the caller before being passed in;
    /// bodies whose hash maps to a requested manifest index are
    /// returned in manifest order.  Bodies that don't match the
    /// manifest are dropped silently.
    ///
    /// The hashing is the caller's responsibility because it depends
    /// on the concrete body wire format; this crate is format-agnostic.
    pub fn match_eb_tx_response(
        &mut self,
        point: &Point,
        bodies_with_hashes: &[(TxBody, TxId)],
    ) -> EbTxMatchOutcome {
        // A response means the in-flight fetch for this point is done;
        // clear the candidate tracker's pending guard so a retry (which
        // the caller may issue immediately after inspecting the
        // outcome's remaining_bitmap) can pick a fresh peer.
        self.candidates.finish_eb_txs_fetch(point);
        let (eb_slot, hash) = match point {
            Point::Specific { slot, hash } => (*slot, *hash),
            Point::Origin => {
                return EbTxMatchOutcome {
                    matched_bodies: Vec::new(),
                    requested: 0,
                    remaining_bitmap: BTreeMap::new(),
                };
            }
        };
        let Some((_, manifest)) = self.eb_tx_hashes.get(&hash) else {
            // Manifest unknown — pass bodies through; the validator
            // hashes them and any unrelated tx will sit unused.
            return EbTxMatchOutcome {
                matched_bodies: bodies_with_hashes.iter().map(|(b, _)| b.clone()).collect(),
                requested: 0,
                remaining_bitmap: BTreeMap::new(),
            };
        };
        // Build hash → manifest-index map.
        let manifest_index: BTreeMap<&TxId, usize> =
            manifest.iter().enumerate().map(|(i, h)| (h, i)).collect();
        // Match each body, sorting by manifest index.
        let mut matched: BTreeMap<usize, TxBody> = BTreeMap::new();
        for (body, body_hash) in bodies_with_hashes {
            if let Some(&idx) = manifest_index.get(body_hash) {
                matched.entry(idx).or_insert_with(|| body.clone());
            }
        }
        let matched_bodies: Vec<TxBody> = matched.values().cloned().collect();

        // Compute `requested` and `remaining_bitmap`.
        let (requested, remaining_bitmap) = match self.pending_eb_tx_fetches.get(&hash).cloned() {
            Some((_, bitmap)) => {
                let requested_indices: BTreeSet<u32> = bitmap_to_indices(&bitmap);
                let matched_indices: BTreeSet<u32> = matched.keys().map(|i| *i as u32).collect();
                let remaining: Vec<u32> = requested_indices
                    .difference(&matched_indices)
                    .copied()
                    .collect();
                let remaining_bitmap = indices_to_bitmap(&remaining);
                if remaining.is_empty() {
                    self.pending_eb_tx_fetches.remove(&hash);
                } else {
                    self.pending_eb_tx_fetches
                        .insert(hash, (eb_slot, remaining_bitmap.clone()));
                }
                (requested_indices.len(), remaining_bitmap)
            }
            None => (0, BTreeMap::new()),
        };
        EbTxMatchOutcome {
            matched_bodies,
            requested,
            remaining_bitmap,
        }
    }

    /// Re-issue a `FetchLeiosBlockTxs` for the unfilled bitmap (a
    /// previous response was partial).  The candidate set excludes
    /// peers that already attempted this EB-txs fetch (tracked by the
    /// candidate tracker), so the policy will land on a fresh peer or
    /// emit nothing if every candidate has been tried.
    ///
    /// Respects the candidate tracker's pending-fetch guard: if a
    /// fetch is already in flight for `point`, no new fetch is
    /// issued.  The guard is cleared by [`Self::on_eb_txs_received`].
    pub fn retry_eb_tx_fetch(
        &mut self,
        point: Point,
        bitmap: BTreeMap<u16, u64>,
    ) -> Vec<LeiosEffect> {
        if bitmap.is_empty() {
            self.postponed_eb_tx_requests.remove(&point);
            return Vec::new();
        }
        let candidates = self.candidates.eb_txs_candidates(&point);
        let peers = self
            .eb_txs_policy
            .pick(&point, &bitmap, &candidates, self.rtt.as_ref());
        if peers.is_empty() {
            // If not all txs are known, but we asked all peers at the moment, we should
            // try to repeat the request in a slot, when new data could become available for
            // the peers (by calling `take_postponed_eb_tx_requests` and further processing
            // the data).
            // This repeated processing may happen only for L_hdr*3 + L_vote slots, since
            // after this number of slots the election (voting) process will have no sense.
            self.postponed_eb_tx_requests.insert(point);
            return Vec::new();
        }
        if !self.candidates.start_eb_txs_fetch(point.clone(), &peers) {
            // Already in flight; the pending response will trigger the
            // next retry attempt via on_eb_txs_received → match → retry.
            return Vec::new();
        }
        vec![LeiosEffect::FetchLeiosBlockTxs {
            point,
            bitmap,
            peers,
        }]
    }

    // -- Queries ------------------------------------------------------------

    /// If the EB at `eb_hash` has reached quorum and entered
    /// CertEligible, return its announced slot; otherwise `None`.
    /// Linear Leios producers use this with the EB announced by the
    /// parent RB to decide whether to attach a cert.
    pub fn eb_certifiable_slot(&self, eb_hash: &[u8; 32]) -> Option<u64> {
        self.elections.eb_certifiable_slot(eb_hash)
    }

    /// The EB context `(eb_hash, eb_slot)` a vote's `announcing_rb_hash` refers
    /// to, if an election exists for it — what the vote's signer BLS-signed.
    /// Lets a receiver verify inbound votes that carry only the RB hash.
    pub fn eb_context(&self, announcing_rb_hash: &[u8; 32]) -> Option<([u8; 32], u64)> {
        self.elections.eb_context(announcing_rb_hash)
    }

    /// Snapshot every internal collection's size.  Used both by
    /// [`Self::log_state_sizes`] and by per-slot telemetry adapters.
    pub fn state_sizes(&self) -> LeiosStateSizes {
        let eb_tx_hash_entries_total: usize =
            self.eb_tx_hashes.values().map(|(_, hs)| hs.len()).sum();
        let pending_eb_tx_bitmaps_total: usize = self
            .pending_eb_tx_fetches
            .values()
            .map(|(_, bm)| bm.len())
            .sum();
        let (
            cand_block_offers,
            cand_eb_offers,
            cand_eb_txs_offers,
            cand_pending_block,
            cand_pending_eb,
            cand_pending_eb_txs,
            cand_eb_txs_attempts,
        ) = self.candidates.state_sizes();
        LeiosStateSizes {
            elections: self.elections.count(),
            eb_tx_hashes: self.eb_tx_hashes.len(),
            eb_tx_hash_entries_total,
            pending_eb_tx_fetches: self.pending_eb_tx_fetches.len(),
            pending_eb_tx_bitmaps_total,
            endorsed_unvalidated_ebs: self.endorsed_unvalidated_ebs.len(),
            in_flight: self.in_flight.len(),
            equivocating_slots: self.chain_tip_ctx.equivocating_slots.len(),
            cand_block_offers,
            cand_eb_offers,
            cand_eb_txs_offers,
            cand_pending_block,
            cand_pending_eb,
            cand_pending_eb_txs,
            cand_eb_txs_attempts,
        }
    }

    /// Emit an `info!` line summarising the sizes of every internal
    /// collection.  Used by adapters to monitor memory growth — if any
    /// collection grows without bound across consecutive lines, that's
    /// the leak.
    pub fn log_state_sizes(&self) {
        let s = self.state_sizes();
        info!(
            node_id = %self.node_id,
            elections = s.elections,
            eb_tx_hashes = s.eb_tx_hashes,
            eb_tx_hash_entries_total = s.eb_tx_hash_entries_total,
            pending_eb_tx_fetches = s.pending_eb_tx_fetches,
            pending_eb_tx_bitmaps_total = s.pending_eb_tx_bitmaps_total,
            endorsed_unvalidated_ebs = s.endorsed_unvalidated_ebs,
            in_flight = s.in_flight,
            equivocating_slots = s.equivocating_slots,
            cand_block_offers = s.cand_block_offers,
            cand_eb_offers = s.cand_eb_offers,
            cand_eb_txs_offers = s.cand_eb_txs_offers,
            cand_pending_block = s.cand_pending_block,
            cand_pending_eb = s.cand_pending_eb,
            cand_pending_eb_txs = s.cand_pending_eb_txs,
            cand_eb_txs_attempts = s.cand_eb_txs_attempts,
            "leios state sizes"
        );
    }

    // -- Internal helpers ---------------------------------------------------

    fn evict_stale_in_flight(&mut self, now: Instant) {
        self.in_flight
            .retain(|_, started| now.duration_since(*started) < IN_FLIGHT_TTL);
    }
}

/// One decoded vote body to pass into [`LeiosState::on_validated_votes`].
/// Borrowed slices avoid copies; the caller owns the storage.
pub struct ValidatedVote<'a> {
    pub voter_id: &'a [u8],
    pub tag: u8,
    pub eligibility_signature: Option<&'a [u8]>,
    /// Hash of the RB that announced the EB this vote is for — the
    /// election key.  The election (and its slot) already exist from
    /// the announcement, so the vote carries no EB hash or slot.
    pub announcing_rb_hash: &'a [u8; 32],
}

// ---------------------------------------------------------------------------
// Bitmap helpers (sparse 16-bit-segment / 64-bit-word format)
// ---------------------------------------------------------------------------

pub fn bitmap_to_indices(bitmap: &BTreeMap<u16, u64>) -> BTreeSet<u32> {
    let mut out = BTreeSet::new();
    for (&segment, &word) in bitmap {
        for bit in 0..64 {
            if word & (1u64 << bit) != 0 {
                out.insert((segment as u32) * 64 + bit as u32);
            }
        }
    }
    out
}

fn indices_to_bitmap(indices: &[u32]) -> BTreeMap<u16, u64> {
    let mut out: BTreeMap<u16, u64> = BTreeMap::new();
    for &idx in indices {
        let segment = (idx / 64) as u16;
        let bit = idx % 64;
        *out.entry(segment).or_insert(0) |= 1u64 << bit;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mempool::{TxBody, TxId};

    fn pipeline() -> PipelineConfig {
        PipelineConfig {
            delta_hdr: 1,
            vote_window: 5,
            diffuse_window: 5,
            dedup_window: 10,
        }
    }

    #[test]
    fn apply_control_stores_leios_domain_slice() {
        use crate::behaviour::tree::control::{ControlSignal, VotePolicy};
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(0), pipeline());
        assert_eq!(state.control.leios.vote, VotePolicy::Honest);
        let mut cs = ControlSignal::default();
        cs.leios.vote = VotePolicy::Abstain(NoVoteReason::Declined);
        cs.leios.echo_to_source = true;
        state.apply_control(&cs);
        assert_eq!(
            state.control.leios.vote,
            VotePolicy::Abstain(NoVoteReason::Declined)
        );
        assert!(state.control.leios.echo_to_source);
    }

    fn elections_for(node_id: &str) -> Elections {
        use crate::elections::ElectionsConfig;
        Elections::new(ElectionsConfig {
            node_id: node_id.to_string(),
            pipeline: pipeline(),
            committee_selection: CommitteeSelection::EveryoneVotes,
            persistent_committee: BTreeMap::new(),
            stake_registry: BTreeMap::new(),
            total_stake: 0,
            expected_total_weight: 100,
            quorum_weight_fraction: 0.75,
        })
    }

    fn cfg(persistent_seats: u32) -> VotingConfig {
        VotingConfig {
            committee_selection: CommitteeSelection::EveryoneVotes,
            stake: 100,
            total_stake: 1000,
            persistent_vote_bytes: 130,
            non_persistent_vote_bytes: 180,
            persistent_seats,
            retry_vote_in_window: true,
            evaluate_votes: true,
        }
    }

    fn h(b: u8) -> [u8; 32] {
        [b; 32]
    }

    fn tx_id(b: u8) -> TxId {
        TxId::new_with_slice(&[b; 32])
    }

    fn point(slot: u64, b: u8) -> Point {
        Point::Specific { slot, hash: h(b) }
    }

    /// Default "all txs known" callback: predicates ignore MissingTX
    /// unless the test populates `eb_tx_hashes` and supplies its own.
    fn tx_all(_: &TxId) -> TxAvailability {
        TxAvailability::Present { pinned: true, ours: true, gen_slot: 0 }
    }

    /// Set chain-tip context that satisfies the LateRBHeader / WrongEB
    /// predicates for an EB at `eb_slot` whose hash is `eb_hash`.
    /// Arrival slot = `eb_slot - 1` (within `eb_slot + Δhdr` for Δhdr=1).
    /// These tests use `eb_hash` as the announcing RB hash too (rb == eb),
    /// so the election key, the tip's `tip_rb_hash`, and the EB hash all
    /// line up on `h(..)`.  `tip_rb_slot` is left `None` so the
    /// chain-progress prune doesn't fire; tests create elections
    /// explicitly via `announce_from_rb`.
    fn tip_for(state: &mut LeiosState, eb_slot: u64, eb_hash: [u8; 32]) {
        state.set_chain_tip_context(ChainTipContext {
            rb_header_arrival_slot: Some(eb_slot.saturating_sub(1)),
            eb_announcement: Some(eb_hash),
            tip_rb_hash: Some(eb_hash),
            ..Default::default()
        });
    }

    #[test]
    fn vote_when_body_validated_before_announcement() {
        // Regression: the EB body can validate before its announcing RB
        // becomes the chain tip (a producer validating its own EB). The
        // election, created at announcement via the chain-tip path, must
        // start body-validated so the node votes rather than abstaining
        // with EBValidating.
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(1), pipeline());
        state.on_slot(10, &tx_all);
        // Body validates first — no election exists yet.
        state.on_validated_eb(point(10, 1));
        assert_eq!(state.elections.count(), 0);
        // Announcement arrives via the chain-tip path and creates the
        // election, which must inherit the already-validated body.
        state.set_chain_tip_context(ChainTipContext {
            rb_header_arrival_slot: Some(9),
            eb_announcement: Some(h(1)),
            tip_rb_hash: Some(h(1)),
            tip_rb_slot: Some(10),
            ..Default::default()
        });
        assert!(
            state.elections.is_announced(&h(1)),
            "election must inherit the pre-announcement body validation"
        );
        // Voting window → a real vote, not EBValidating.
        let fx = state.on_slot(13, &tx_all);
        assert_eq!(fx.len(), 1);
        assert!(matches!(fx[0], LeiosEffect::EmitVote { .. }));
    }

    #[test]
    fn pv_vote_emitted_when_seated() {
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(1), pipeline());
        state.on_slot(10, &tx_all);
        state.elections.announce_from_rb(h(1), 10, h(1));
        state.on_validated_eb(point(10, 1));
        tip_for(&mut state, 10, h(1));
        // Voting phase begins at elapsed=3 (3*delta_hdr).
        let fx = state.on_slot(13, &tx_all);
        assert_eq!(fx.len(), 1);
        match &fx[0] {
            LeiosEffect::EmitVote {
                eb_slot,
                eb_hash,
                announcing_rb_hash,
                emit_pv,
                npv_signature,
            } => {
                assert_eq!(*eb_slot, 10);
                assert_eq!(eb_hash, &h(1));
                assert_eq!(announcing_rb_hash, &h(1));
                assert!(*emit_pv);
                assert!(npv_signature.is_none());
            }
            other => panic!("expected EmitVote, got {other:?}"),
        }
        assert!(state.elections.voted(&h(1)));
    }

    #[test]
    fn evaluate_votes_false_skips_predicate_and_marks_election_done() {
        // A node configured with `evaluate_votes = false` (e.g. a
        // stake=0 follower without a voter stake registry) must:
        //   - run no `decide_vote` predicate (so the empty-context
        //     fallback can't synthesise spurious `WrongEB` effects);
        //   - emit neither `EmitVote` nor `NoVote`;
        //   - still `mark_voted` so the election doesn't keep
        //     re-firing `EligibleToVote` for the whole window.
        let mut voting = cfg(1); // seated voter — would normally PV-vote
        voting.evaluate_votes = false;
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), voting, pipeline());
        state.on_slot(10, &tx_all);
        state.elections.announce_from_rb(h(1), 10, h(1));
        // Deliberately do NOT call `tip_for` — leave ChainTipContext
        // default, which would otherwise drive `decide_vote` straight
        // into the `WrongEB` branch.
        let fx = state.on_slot(13, &tx_all);
        assert!(
            fx.is_empty(),
            "gated node must not emit vote effects, got {fx:?}"
        );
        assert!(
            state.elections.voted(&h(1)),
            "gated node must mark the election done to suppress re-firing"
        );

        // Re-arming the lottery on the next slot must still be a no-op.
        let fx2 = state.on_slot(14, &tx_all);
        assert!(fx2.is_empty());
    }

    #[test]
    fn no_vote_when_no_seats_and_no_npv() {
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(0), pipeline());
        state.on_slot(10, &tx_all);
        state.elections.announce_from_rb(h(1), 10, h(1));
        state.on_validated_eb(point(10, 1));
        tip_for(&mut state, 10, h(1));
        let fx = state.on_slot(13, &tx_all);
        // Lottery loss (no PV seat, no NPV win) is silent — no effect,
        // no `mark_voted` so EligibleToVote can re-fire next slot.
        assert!(fx.is_empty());
        assert!(!state.elections.voted(&h(1)));
    }

    #[test]
    fn wfa_ls_pv_seated_voter_skips_npv_trial() {
        // CIP-0164 partitions persistent and non-persistent pools by
        // stake-ordering: a pool with a persistent seat is NOT eligible
        // for an NPV signature on the same EB.  Verify the partition
        // is honoured at emission time.
        let mut voting = cfg(5); // 5 persistent seats
        voting.committee_selection = CommitteeSelection::WfaLs {
            persistent_voters: 480,
            non_persistent_voters: 120,
        };
        voting.stake = 400; // 40% — would near-certainly win NPV if eligible
        use crate::elections::ElectionsConfig;
        let elections = Elections::new(ElectionsConfig {
            node_id: "n0".to_string(),
            pipeline: pipeline(),
            committee_selection: voting.committee_selection.clone(),
            persistent_committee: BTreeMap::new(),
            stake_registry: BTreeMap::new(),
            total_stake: 1000,
            expected_total_weight: 600,
            quorum_weight_fraction: 0.75,
        });
        let mut state = LeiosState::new("n0".into(), elections, voting, pipeline());
        state.on_slot(10, &tx_all);
        state.elections.announce_from_rb(h(1), 10, h(1));
        state.on_validated_eb(point(10, 1));
        tip_for(&mut state, 10, h(1));
        let fx = state.on_slot(13, &tx_all);
        assert_eq!(fx.len(), 1);
        match &fx[0] {
            LeiosEffect::EmitVote {
                emit_pv,
                npv_signature,
                ..
            } => {
                assert!(*emit_pv);
                assert!(
                    npv_signature.is_none(),
                    "PV-seated voter must not also emit NPV signature"
                );
            }
            other => panic!("expected EmitVote, got {other:?}"),
        }
    }

    #[test]
    fn wfa_ls_non_seated_voter_can_emit_npv() {
        // The complementary case: a voter with no persistent seat that
        // wins the NPV lottery emits an NPV-only vote.
        let mut voting = cfg(0); // no persistent seats
        voting.committee_selection = CommitteeSelection::WfaLs {
            persistent_voters: 480,
            non_persistent_voters: 120,
        };
        voting.stake = 400;
        use crate::elections::ElectionsConfig;
        let elections = Elections::new(ElectionsConfig {
            node_id: "n0".to_string(),
            pipeline: pipeline(),
            committee_selection: voting.committee_selection.clone(),
            persistent_committee: BTreeMap::new(),
            stake_registry: BTreeMap::new(),
            total_stake: 1000,
            expected_total_weight: 600,
            quorum_weight_fraction: 0.75,
        });
        let mut state = LeiosState::new("n0".into(), elections, voting, pipeline());
        state.on_slot(10, &tx_all);
        state.elections.announce_from_rb(h(1), 10, h(1));
        state.on_validated_eb(point(10, 1));
        tip_for(&mut state, 10, h(1));
        let fx = state.on_slot(13, &tx_all);
        assert_eq!(fx.len(), 1);
        match &fx[0] {
            LeiosEffect::EmitVote {
                emit_pv,
                npv_signature,
                ..
            } => {
                assert!(!*emit_pv, "no persistent seats → no PV vote");
                assert!(npv_signature.is_some(), "non-seated NPV win expected");
            }
            other => panic!("expected EmitVote, got {other:?}"),
        }
    }

    #[test]
    fn on_eb_offered_dedups_via_in_flight() {
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(0), pipeline());
        let now = Instant::now();
        let peer = PeerId(1);
        let fx = state.on_eb_offered(point(10, 1), peer, now);
        assert_eq!(fx.len(), 1);
        match &fx[0] {
            LeiosEffect::FetchLeiosBlock { peers, .. } => {
                assert_eq!(*peers, vec![peer]);
            }
            other => panic!("expected FetchLeiosBlock, got {other:?}"),
        }
        // Second offer from the same peer: dedup'd.
        let fx = state.on_eb_offered(point(10, 1), peer, now);
        assert!(fx.is_empty());
    }

    #[test]
    fn on_eb_received_emits_record_and_validate() {
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(0), pipeline());
        let manifest = vec![tx_id(0xA0), tx_id(0xA1)];
        let fx = state.on_eb_received(None, point(10, 1), Some((None, manifest.clone())));
        assert_eq!(fx.len(), 2);
        assert!(matches!(fx[0], LeiosEffect::RecordLeiosEbManifest { .. }));
        assert!(matches!(fx[1], LeiosEffect::ValidateEb { .. }));
        assert_eq!(
            state.eb_tx_hashes.get(&h(1)).map(|(_, v)| v),
            Some(&manifest)
        );
    }

    #[test]
    fn on_eb_received_validate_only_when_no_manifest() {
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(0), pipeline());
        let fx = state.on_eb_received(None, point(10, 1), None);
        assert_eq!(fx.len(), 1);
        assert!(matches!(fx[0], LeiosEffect::ValidateEb { .. }));
    }

    // -- t22 EB-processing filter actuator (control.mempool.tx_filter) --------

    fn with_tx_filter(state: &mut LeiosState, vote: u8, non_voting: u8, hide_eb_tx: bool) {
        use crate::behaviour::tree::control::{ControlSignal, TxFilterPolicy};
        let mut cs = ControlSignal::default();
        cs.mempool.tx_filter = TxFilterPolicy::ChecksumThreshold {
            vote,
            non_voting,
            hide_eb_tx,
        };
        state.apply_control(&cs);
    }

    #[test]
    fn tx_filter_threshold_100_processes_eb_offer() {
        // checksum percentile is always < 100, so threshold 100 always processes.
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(1), pipeline());
        with_tx_filter(&mut state, 100, 100, false);
        let fx = state.on_eb_offered(point(10, 1), PeerId(1), Instant::now());
        assert_eq!(fx.len(), 1);
        assert!(matches!(fx[0], LeiosEffect::FetchLeiosBlock { .. }));
    }

    #[test]
    fn tx_filter_threshold_0_drops_eb_offer() {
        // checksum percentile is always >= 0, so threshold 0 never processes —
        // the offer is dropped despite the honest path having a candidate peer.
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(1), pipeline());
        with_tx_filter(&mut state, 0, 0, false);
        let fx = state.on_eb_offered(point(10, 1), PeerId(1), Instant::now());
        assert!(fx.is_empty());
    }

    #[test]
    fn tx_filter_threshold_0_drops_eb_txs_offer() {
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(1), pipeline());
        with_tx_filter(&mut state, 0, 0, false);
        let mut bitmap = BTreeMap::new();
        bitmap.insert(0u16, 1u64);
        let fx = state.on_eb_txs_offered(point(10, 1), PeerId(1), bitmap, Instant::now());
        assert!(fx.is_empty());
    }

    #[test]
    fn tx_filter_hide_eb_tx_drops_received_processing() {
        // hide_eb_tx + filtered (threshold 0) => on_eb_received produces nothing.
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(1), pipeline());
        with_tx_filter(&mut state, 0, 0, true);
        let fx = state.on_eb_received(None, point(10, 1), Some((None, vec![tx_id(0xA0)])));
        assert!(fx.is_empty());
    }

    #[test]
    fn tx_filter_without_hide_still_processes_received() {
        // Without hide_eb_tx, on_eb_received is unaffected by the filter
        // (the filter only gates offer/txs-offer fetching, not validation).
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(1), pipeline());
        with_tx_filter(&mut state, 0, 0, false);
        let fx = state.on_eb_received(None, point(10, 1), None);
        assert_eq!(fx.len(), 1);
        assert!(matches!(fx[0], LeiosEffect::ValidateEb { .. }));
    }

    #[test]
    fn quorum_emits_telemetry() {
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(0), pipeline());
        state.on_slot(10, &tx_all);
        state.elections.announce_from_rb(h(1), 10, h(1));
        let body_a = ValidatedVote {
            voter_id: b"a",
            tag: 0,
            eligibility_signature: None,
            announcing_rb_hash: &h(1),
        };
        // Stub committee weight: weight_for() returns 0 by default;
        // we'd need PV seats for non-zero weight.  Instead just verify
        // shape: with weight 0, no telemetry fires.
        let fx = state.on_validated_votes(std::iter::once(body_a));
        assert!(fx.is_empty());
    }

    #[test]
    fn slot_tick_prunes_stale_eb_tx_state_via_chain_progress() {
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(0), pipeline());
        state.eb_tx_hashes.insert(h(1), (10, vec![tx_id(0xAA)]));
        state
            .pending_eb_tx_fetches
            .insert(h(1), (10, BTreeMap::new()));
        // No chain tip set → no prune, EB stays even far past the
        // old time-based expiry.
        state.on_slot(1_000_000, &tx_all);
        assert_eq!(state.eb_tx_hashes.len(), 1);
        assert_eq!(state.pending_eb_tx_fetches.len(), 1);
        // Chain advances past slot 10 → EB pruned.
        state.set_chain_tip_context(ChainTipContext {
            tip_rb_slot: Some(11),
            ..Default::default()
        });
        state.on_slot(20, &tx_all);
        assert!(state.eb_tx_hashes.is_empty());
        assert!(state.pending_eb_tx_fetches.is_empty());
    }

    #[test]
    fn match_eb_tx_response_returns_in_manifest_order() {
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(0), pipeline());
        // Manifest: [hashA, hashB, hashC] at slot 10.
        let ha = tx_id(0xA0);
        let hb = tx_id(0xA1);
        let hc = tx_id(0xA2);
        state
            .eb_tx_hashes
            .insert(h(1), (10, vec![ha.clone(), hb.clone(), hc.clone()]));
        // Body order arrives as [b, a, c] but should come out [a, b, c].
        let bodies = vec![
            (TxBody::new_with_vec(b"body-b".to_vec()), hb),
            (TxBody::new_with_vec(b"body-a".to_vec()), ha),
            (TxBody::new_with_vec(b"body-c".to_vec()), hc),
        ];
        let outcome = state.match_eb_tx_response(&point(10, 1), &bodies);
        assert_eq!(
            outcome.matched_bodies,
            vec![
                TxBody::new_with_vec(b"body-a".to_vec()),
                TxBody::new_with_vec(b"body-b".to_vec()),
                TxBody::new_with_vec(b"body-c".to_vec())
            ]
        );
    }

    #[test]
    fn match_eb_tx_response_drops_unmatched_bodies() {
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(0), pipeline());
        let ha = tx_id(0xA0);
        state.eb_tx_hashes.insert(h(1), (10, vec![ha.clone()]));
        let bogus = tx_id(0xFF);
        let bodies = vec![
            (TxBody::new_with_vec(b"body-a".to_vec()), ha),
            (TxBody::new_with_vec(b"bogus".to_vec()), bogus),
        ];
        let outcome = state.match_eb_tx_response(&point(10, 1), &bodies);
        assert_eq!(
            outcome.matched_bodies,
            vec![TxBody::new_with_vec(b"body-a".to_vec())]
        );
    }

    #[test]
    fn on_eb_txs_offered_gates_per_slot() {
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(0), pipeline());
        let now = Instant::now();
        let peer = PeerId(1);
        let mut bitmap = BTreeMap::new();
        bitmap.insert(0u16, 0b111u64);

        let fx = state.on_eb_txs_offered(point(10, 1), peer, bitmap.clone(), now);
        assert_eq!(fx.len(), 1);
        assert!(matches!(fx[0], LeiosEffect::FetchLeiosBlockTxs { .. }));
        assert!(state.pending_eb_tx_fetches.contains_key(&h(1)));

        // Same-slot offer for a different EB hash from a new peer:
        // dedup'd via the per-slot gate.
        let fx2 = state.on_eb_txs_offered(point(10, 2), PeerId(2), bitmap, now);
        assert!(fx2.is_empty());
    }

    #[test]
    fn on_eb_txs_offered_origin_point_is_noop() {
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(0), pipeline());
        let fx = state.on_eb_txs_offered(Point::Origin, PeerId(1), BTreeMap::new(), Instant::now());
        assert!(fx.is_empty());
    }

    #[test]
    fn on_eb_txs_received_clears_gate_for_subsequent_fetch() {
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(0), pipeline());
        let now = Instant::now();
        let mut bitmap = BTreeMap::new();
        bitmap.insert(0u16, 0b1u64);
        let peer_id = PeerId(1);
        let _ = state.on_eb_txs_offered(point(10, 1), peer_id, bitmap.clone(), now);

        state.on_eb_txs_received(&point(10, 1), 1, peer_id);

        // A fresh same-slot offer for a different EB hash from a new peer
        // is no longer gated.
        let fx = state.on_eb_txs_offered(point(10, 2), PeerId(2), bitmap, now);
        assert_eq!(fx.len(), 1);
    }

    #[test]
    fn on_votes_received_resolves_index_and_forms_quorum() {
        // A committee where index-0 voter "voter-a" alone meets quorum.
        use crate::elections::ElectionsConfig;
        let mut persistent = BTreeMap::new();
        persistent.insert("voter-a".to_string(), 100u32);
        let mut registry = BTreeMap::new();
        registry.insert("voter-a".to_string(), 100u64);
        let elections = Elections::new(ElectionsConfig {
            node_id: "n0".to_string(),
            pipeline: pipeline(),
            committee_selection: CommitteeSelection::EveryoneVotes,
            persistent_committee: persistent,
            stake_registry: registry,
            total_stake: 100,
            expected_total_weight: 100,
            quorum_weight_fraction: 0.75,
        });
        let mut state = LeiosState::new("n0".into(), elections, cfg(0), pipeline());
        state.on_slot(10, &tx_all);
        state.elections.announce_from_rb(h(1), 10, h(1));
        // "voter-a" is index 0 in the sorted registry.
        let vote = Vote {
            announcing_rb_hash: h(1),
            voter_id: 0,
            vote_signature: vec![0xAB; 48],
        };
        let fx = state.on_votes_received(vec![vote]);
        assert!(fx.iter().any(|e| matches!(
            e,
            LeiosEffect::EmitTelemetry(LeiosTelemetryEvent::QuorumReached { .. })
        )));
    }

    #[test]
    fn on_votes_received_skips_unknown_voter() {
        // Empty registry → no index resolves; the vote is dropped.
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(0), pipeline());
        let vote = Vote {
            announcing_rb_hash: h(1),
            voter_id: 7,
            vote_signature: vec![0xAB; 48],
        };
        let fx = state.on_votes_received(vec![vote]);
        assert!(fx.is_empty());
    }

    #[test]
    fn on_votes_received_empty_is_noop() {
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(0), pipeline());
        let fx = state.on_votes_received(Vec::new());
        assert!(fx.is_empty());
    }

    #[test]
    fn on_validated_eb_marks_body_validated() {
        // Elections are created at announcement (`announce_from_rb`),
        // not at body validation. `on_validated_eb` only flips the
        // body-validated flag on the matching election.
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(0), pipeline());
        state.on_slot(10, &tx_all);
        // No election yet → on_validated_eb creates nothing.
        state.on_validated_eb(point(10, 1));
        assert_eq!(state.elections.count(), 0);
        // Announce (rb == eb == h(1)) creates it, body not yet validated.
        state.elections.announce_from_rb(h(1), 10, h(1));
        assert_eq!(state.elections.count(), 1);
        assert!(!state.elections.is_announced(&h(1)));
        // Now validating the body flips the flag.
        state.on_validated_eb(point(10, 1));
        assert!(state.elections.is_announced(&h(1)));
    }

    #[test]
    fn on_validated_eb_origin_is_noop() {
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(0), pipeline());
        state.on_validated_eb(Point::Origin);
        assert_eq!(state.elections.count(), 0);
    }

    #[test]
    fn retry_eb_tx_fetch_with_bitmap_emits_fetch() {
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(0), pipeline());
        // The retry path consults eb_txs_candidates, so we need at
        // least one peer that hasn't been attempted.
        state
            .candidates
            .note_eb_txs_offered(point(10, 1), PeerId(1));
        let mut bitmap = BTreeMap::new();
        bitmap.insert(0u16, 0b10u64);
        let fx = state.retry_eb_tx_fetch(point(10, 1), bitmap.clone());
        assert_eq!(fx.len(), 1);
        match &fx[0] {
            LeiosEffect::FetchLeiosBlockTxs {
                point: p,
                bitmap: b,
                peers,
            } => {
                assert_eq!(*p, point(10, 1));
                assert_eq!(b, &bitmap);
                assert_eq!(*peers, vec![PeerId(1)]);
            }
            other => panic!("expected FetchLeiosBlockTxs, got {other:?}"),
        }
    }

    #[test]
    fn retry_eb_tx_fetch_empty_bitmap_is_noop() {
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(0), pipeline());
        let fx = state.retry_eb_tx_fetch(point(10, 1), BTreeMap::new());
        assert!(fx.is_empty());
    }

    #[test]
    fn match_eb_tx_response_reports_remaining_bitmap() {
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(0), pipeline());
        let ha = tx_id(0xA0);
        let hb = tx_id(0xA1);
        state.eb_tx_hashes.insert(h(1), (10, vec![ha.clone(), hb]));
        // Pretend we requested both indices 0 and 1.
        let mut requested = BTreeMap::new();
        requested.insert(0u16, 0b11u64);
        state.pending_eb_tx_fetches.insert(h(1), (10, requested));
        // Only body for index 0 (ha) arrives.
        let outcome = state.match_eb_tx_response(
            &point(10, 1),
            &[(TxBody::new_with_vec(b"body-a".to_vec()), ha)],
        );
        assert_eq!(
            outcome.matched_bodies,
            vec![TxBody::new_with_vec(b"body-a".to_vec())]
        );
        assert_eq!(outcome.requested, 2);
        // Index 1 still missing.
        let mut expected_remaining = BTreeMap::new();
        expected_remaining.insert(0u16, 0b10u64);
        assert_eq!(outcome.remaining_bitmap, expected_remaining);
        // pending_eb_tx_fetches updated to remaining-only.
        assert_eq!(
            state
                .pending_eb_tx_fetches
                .get(&h(1))
                .map(|(_, b)| b.clone()),
            Some(expected_remaining)
        );
    }

    #[test]
    fn match_eb_tx_response_clears_pending_when_complete() {
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(0), pipeline());
        let ha = tx_id(0xA0);
        state.eb_tx_hashes.insert(h(1), (10, vec![ha.clone()]));
        let mut requested = BTreeMap::new();
        requested.insert(0u16, 0b1u64);
        state.pending_eb_tx_fetches.insert(h(1), (10, requested));
        let outcome = state.match_eb_tx_response(
            &point(10, 1),
            &[(TxBody::new_with_vec(b"body-a".to_vec()), ha)],
        );
        assert!(outcome.remaining_bitmap.is_empty());
        assert!(!state.pending_eb_tx_fetches.contains_key(&h(1)));
    }

    #[test]
    fn match_eb_tx_response_unknown_manifest_passes_bodies_through() {
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(0), pipeline());
        let outcome = state.match_eb_tx_response(
            &point(10, 1),
            &[(TxBody::new_with_vec(b"some-body".to_vec()), tx_id(0xAA))],
        );
        assert_eq!(
            outcome.matched_bodies,
            vec![TxBody::new_with_vec(b"some-body".to_vec())]
        );
        assert_eq!(outcome.requested, 0);
        assert!(outcome.remaining_bitmap.is_empty());
    }

    #[test]
    fn match_eb_tx_response_origin_point_returns_empty() {
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(0), pipeline());
        let outcome = state.match_eb_tx_response(&Point::Origin, &[]);
        assert!(outcome.matched_bodies.is_empty());
        assert_eq!(outcome.requested, 0);
    }

    #[test]
    fn quorum_emits_telemetry_with_weighted_voter() {
        // Set up an Elections with a persistent committee that hands node
        // "voter-a" enough seats to single-handedly form quorum.
        use crate::elections::ElectionsConfig;
        let mut persistent = BTreeMap::new();
        persistent.insert("voter-a".to_string(), 100u32);
        let elections = Elections::new(ElectionsConfig {
            node_id: "n0".to_string(),
            pipeline: pipeline(),
            committee_selection: CommitteeSelection::EveryoneVotes,
            persistent_committee: persistent,
            stake_registry: BTreeMap::new(),
            total_stake: 0,
            expected_total_weight: 100,
            quorum_weight_fraction: 0.75,
        });
        let mut state = LeiosState::new("n0".into(), elections, cfg(0), pipeline());
        state.on_slot(10, &tx_all);
        state.elections.announce_from_rb(h(1), 10, h(1));

        let vote = ValidatedVote {
            voter_id: b"voter-a",
            tag: 0,
            eligibility_signature: None,
            announcing_rb_hash: &h(1),
        };
        let fx = state.on_validated_votes(std::iter::once(vote));
        assert!(fx.iter().any(|e| matches!(
            e,
            LeiosEffect::EmitTelemetry(LeiosTelemetryEvent::QuorumReached { .. })
        )));
    }

    // -- CIP-0164 voting predicates -----------------------------------------

    /// Helper: assert that `fx` has exactly one effect, a NoVote with
    /// the given reason for `eb_hash`.
    fn assert_no_vote(fx: &[LeiosEffect], eb_hash: [u8; 32], expected: NoVoteReason) {
        assert_eq!(fx.len(), 1, "expected one NoVote, got {fx:?}");
        match &fx[0] {
            LeiosEffect::NoVote {
                eb_hash: h, reason, ..
            } => {
                assert_eq!(*h, eb_hash);
                // Compare by tag so struct-variant field values (e.g. MissingTX
                // counts) don't have to be hardcoded in the expected value.
                assert_eq!(reason.tag(), expected.tag(), "reason {reason:?} != {expected:?}");
            }
            other => panic!("expected NoVote, got {other:?}"),
        }
    }

    #[test]
    fn no_vote_no_chain_tip_when_chain_tip_not_set() {
        // Default ChainTipContext has no rb_header_arrival_slot — predicate
        // returns NoChainTip before any other check (distinct from the
        // WrongEB announcement-mismatch case).
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(1), pipeline());
        state.on_slot(10, &tx_all);
        state.elections.announce_from_rb(h(1), 10, h(1));
        let fx = state.on_slot(13, &tx_all);
        assert_no_vote(&fx, h(1), NoVoteReason::NoChainTip);
        // NoChainTip is transient: do NOT mark_voted, so subsequent slots can
        // re-evaluate as the chain tip catches up.
        assert!(!state.elections.voted(&h(1)));
    }

    #[test]
    fn no_vote_wrong_eb_when_chain_tip_announces_other_eb() {
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(1), pipeline());
        state.on_slot(10, &tx_all);
        state.elections.announce_from_rb(h(1), 10, h(1));
        // Chain tip RB references h(2), not the EB we're voting on.
        state.set_chain_tip_context(ChainTipContext {
            rb_header_arrival_slot: Some(10),
            eb_announcement: Some(h(2)),
            ..Default::default()
        });
        let fx = state.on_slot(13, &tx_all);
        assert_no_vote(&fx, h(1), NoVoteReason::WrongEB);
    }

    #[test]
    fn no_vote_late_rb_header() {
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(1), pipeline());
        state.on_slot(10, &tx_all);
        state.elections.announce_from_rb(h(1), 10, h(1));
        // delta_hdr=1, so RB header must arrive before slot 11.  Set
        // arrival to slot 11 — exactly at the cutoff, predicate fails.
        state.set_chain_tip_context(ChainTipContext {
            rb_header_arrival_slot: Some(11),
            eb_announcement: Some(h(1)),
            tip_rb_hash: Some(h(1)),
            ..Default::default()
        });
        let fx = state.on_slot(13, &tx_all);
        assert_no_vote(&fx, h(1), NoVoteReason::LateRBHeader);
    }

    #[test]
    fn no_vote_missing_tx() {
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(1), pipeline());
        state.on_slot(10, &tx_all);
        state.elections.announce_from_rb(h(1), 10, h(1));
        tip_for(&mut state, 10, h(1));
        // Manifest references hashes 0xA0, 0xA1.  tx_known returns false
        // for everything → predicate fires on the first one.
        state
            .eb_tx_hashes
            .insert(h(1), (10, vec![tx_id(0xA0), tx_id(0xA1)]));
        let no_txs = |_: &TxId| TxAvailability::Absent;
        let fx = state.on_slot(13, &no_txs);
        assert_no_vote(
            &fx,
            h(1),
            NoVoteReason::MissingTX {
                required: 2,
                present_pinned: 0,
                present_unpinned: 0,
            },
        );
    }

    #[test]
    fn on_slot_prunes_candidate_tracker_below_chain_tip() {
        // Chain-progress prune: anything at slot < tip_rb_slot is dead
        // under the strict parent-only cert rule.  With tip_rb_slot=8,
        // slot-5 entries drop and slot-8 entries stay.
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(1), pipeline());
        let peer = crate::peer::PeerId(1);
        state.candidates.note_eb_offered(point(5, 0xAA), peer);
        state.candidates.note_eb_offered(point(8, 0xBB), peer);
        state.candidates.note_eb_txs_offered(point(5, 0xCC), peer);
        state.candidates.note_eb_txs_offered(point(8, 0xDD), peer);

        state.set_chain_tip_context(ChainTipContext {
            tip_rb_slot: Some(8),
            ..Default::default()
        });
        let _ = state.on_slot(30, &tx_all);

        assert!(state.candidates.eb_candidates(&point(5, 0xAA)).is_empty());
        assert!(state
            .candidates
            .eb_txs_candidates(&point(5, 0xCC))
            .is_empty());
        assert_eq!(state.candidates.eb_candidates(&point(8, 0xBB)), vec![peer]);
        assert_eq!(
            state.candidates.eb_txs_candidates(&point(8, 0xDD)),
            vec![peer]
        );
    }

    #[test]
    fn on_slot_skips_prune_without_chain_tip() {
        // Before adopting any RB (`tip_rb_slot = None`), no prune fires
        // — incoming EB / vote offers may belong to the chain we're
        // about to adopt.
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(1), pipeline());
        let peer = crate::peer::PeerId(1);
        state.candidates.note_eb_offered(point(5, 0xAA), peer);
        let _ = state.on_slot(1000, &tx_all);
        assert_eq!(state.candidates.eb_candidates(&point(5, 0xAA)), vec![peer]);
    }

    #[test]
    fn no_vote_equivocating_rb_slot() {
        // CIP-0164: when RB-header equivocation has been detected at
        // the EB's slot, the voter abstains — regardless of which EB
        // the chain tip ultimately picks.
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(1), pipeline());
        state.on_slot(10, &tx_all);
        state.elections.announce_from_rb(h(1), 10, h(1));
        // Chain tip references this EB and the header arrived on time;
        // the only blocker is the equivocation flag.
        let mut ctx = ChainTipContext {
            rb_header_arrival_slot: Some(10),
            eb_announcement: Some(h(1)),
            tip_rb_hash: Some(h(1)),
            ..Default::default()
        };
        ctx.equivocating_slots.insert(10);
        state.set_chain_tip_context(ctx);
        let fx = state.on_slot(13, &tx_all);
        assert_no_vote(&fx, h(1), NoVoteReason::EquivocatingRB);
    }

    #[test]
    fn no_vote_late_eb() {
        // Construct an EbElection in Voting phase with a seen_slot far
        // past the voting window. Direct field manipulation is the
        // cleanest way to set up the late-arrival corner case — the
        // public `announce` path can't reach it because the phase
        // machine would already filter the EB out.
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(1), pipeline());
        state.on_slot(10, &tx_all);
        state.elections.announce_from_rb(h(1), 10, h(1));
        tip_for(&mut state, 10, h(1));
        // voting_end = 3·1 + 5 = 8. Setting seen_slot = announced + 8 trips LateEB.
        state.elections.election_mut(&h(1)).unwrap().seen_slot = 18;
        let fx = state.on_slot(13, &tx_all);
        assert_no_vote(&fx, h(1), NoVoteReason::LateEB);
    }

    #[test]
    fn no_vote_late_eb_marks_voted_to_suppress_repeat() {
        // LateEB is the only NoVote reason that mark_voted suppresses,
        // because `eb_seen_slot` is fixed at receipt — once late, always
        // late.  Other NoVote reasons (WrongEB, LateRBHeader, MissingTX)
        // are transient and intentionally re-fire next slot so a slow
        // chain-tip update or a delayed TX still gets a chance to vote.
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(1), pipeline());
        state.on_slot(10, &tx_all);
        state.elections.announce_from_rb(h(1), 10, h(1));
        tip_for(&mut state, 10, h(1));
        state.elections.election_mut(&h(1)).unwrap().seen_slot = 18;
        // First slot in Voting fires NoVote(LateEB) and marks voted.
        let fx_first = state.on_slot(13, &tx_all);
        assert_eq!(fx_first.len(), 1);
        assert!(matches!(
            fx_first[0],
            LeiosEffect::NoVote {
                reason: NoVoteReason::LateEB,
                ..
            }
        ));
        assert!(state.elections.voted(&h(1)));
        // Subsequent slots stay quiet.
        let fx_second = state.on_slot(14, &tx_all);
        assert!(fx_second.is_empty());
    }

    #[test]
    fn no_vote_transient_reasons_re_fire_each_slot() {
        // WrongEB / LateRBHeader / MissingTX leave the election unvoted
        // so EligibleToVote re-fires every slot of the Voting window —
        // gives the chain tip / mempool a chance to catch up.
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(1), pipeline());
        state.on_slot(10, &tx_all);
        state.elections.announce_from_rb(h(1), 10, h(1));
        // No chain tip → NoChainTip.
        let fx_first = state.on_slot(13, &tx_all);
        assert_no_vote(&fx_first, h(1), NoVoteReason::NoChainTip);
        assert!(!state.elections.voted(&h(1)));
        // Next slot: still no chain tip → NoChainTip again, not suppressed.
        let fx_second = state.on_slot(14, &tx_all);
        assert_no_vote(&fx_second, h(1), NoVoteReason::NoChainTip);
    }

    // -- missing_eb_tx_bitmap ---------------------------------------------

    #[test]
    fn missing_eb_tx_bitmap_empty_when_manifest_unknown() {
        let state = LeiosState::new("n0".into(), elections_for("n0"), cfg(1), pipeline());
        let mempool = crate::mempool::MempoolState::new(1024);
        let bitmap = state.missing_eb_tx_bitmap(&h(0xAA), &mempool);
        assert!(bitmap.is_empty());
    }

    #[test]
    fn missing_eb_tx_bitmap_returns_indices_not_in_mempool() {
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(1), pipeline());
        let manifest = vec![tx_id(1), tx_id(2), tx_id(3), tx_id(4)];
        state.eb_tx_hashes.insert(h(0xAB), (50, manifest));

        let mut mempool = crate::mempool::MempoolState::new(4096);
        // Local mempool has txs 0 (tx_id(1)) and 2 (tx_id(3)); 1 (tx_id(2)) and 3 (tx_id(4)) are missing.
        mempool.admit_validated(tx_id(1), TxBody::new_with_vec(vec![0u8; 16]), 16, 0, false);
        mempool.admit_validated(tx_id(3), TxBody::new_with_vec(vec![0u8; 16]), 16, 0, false);

        let bitmap = state.missing_eb_tx_bitmap(&h(0xAB), &mempool);
        let indices: Vec<u32> = crate::bitmap::iter_indices(&bitmap).collect();
        assert_eq!(indices, vec![1, 3]);
    }

    #[test]
    fn missing_eb_tx_bitmap_empty_when_all_held() {
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(1), pipeline());
        let manifest = vec![tx_id(5), tx_id(6)];
        state.eb_tx_hashes.insert(h(0xCD), (50, manifest));

        let mut mempool = crate::mempool::MempoolState::new(4096);
        mempool.admit_validated(tx_id(5), TxBody::new_with_vec(vec![0u8; 8]), 8, 0, false);
        mempool.admit_validated(tx_id(6), TxBody::new_with_vec(vec![0u8; 8]), 8, 0, false);

        let bitmap = state.missing_eb_tx_bitmap(&h(0xCD), &mempool);
        assert!(bitmap.is_empty());
    }

    // -- BT control-signal actuation ---------------------------------------

    #[test]
    fn control_vote_abstain_forces_no_vote() {
        // The BT control signal's vote-abstain policy (lazy-voter) overrides
        // the honest predicate, exactly as the old decide_vote hook did.
        let mut state = LeiosState::new("n0".into(), elections_for("n0"), cfg(1), pipeline());
        let mut cs = ControlSignal::default();
        cs.leios.vote = VotePolicy::Abstain(NoVoteReason::WrongEB);
        state.apply_control(&cs);
        state.elections.announce_from_rb(h(1), 10, h(1));
        tip_for(&mut state, 10, h(1));
        let fx = state.on_slot(13, &tx_all);
        // Despite the seated PV that honest would have emitted, the
        // abstain policy forced NoVote { WrongEB }.
        assert_eq!(fx.len(), 1);
        match &fx[0] {
            LeiosEffect::NoVote { reason, .. } => {
                assert_eq!(*reason, NoVoteReason::WrongEB);
            }
            other => panic!("expected NoVote, got {other:?}"),
        }
    }
}
