mod attackers;
pub use attackers::register_actors;
use rand_distr::Distribution;
use tokio::sync::mpsc;

use std::{
    // N.B. HashSet is only used for membership tests (contains/insert), never
    // iterated in order-sensitive paths, so it does not affect determinism.
    collections::{BTreeMap, BTreeSet, HashSet, VecDeque},
    sync::Arc,
};

use rand_chacha::ChaChaRng;

use crate::{
    clock::{Clock, Timestamp},
    config::{
        CommitteeSelectionAlgorithm, EBPropagationCriteria, LeiosVariant, MempoolSamplingStrategy,
        NodeBehaviours, NodeConfiguration, NodeId, RelayStrategy, SimConfiguration,
        TransactionConfig,
    },
    events::EventTracker,
    model::{
        BlockId, Endorsement, EndorserBlockId, LinearEndorserBlock as EndorserBlock,
        LinearRankingBlock as RankingBlock, LinearRankingBlockHeader as RankingBlockHeader,
        NoVoteReason, Transaction, TransactionId, TransactionLostReason, VoteBundle, VoteBundleId,
    },
    rng::{DrawSite, Rng},
    sim::{
        NodeImpl, SimMessage as _,
        linear_leios::attackers::{EBWithholdingEvent, EBWithholdingSender},
        linear_wire::{CpuTask, Message, TimedEvent},
        lottery::{LotteryConfig, LotteryKind, MockLotteryResults, vrf_probabilities},
    },
};

enum TransactionView {
    Pending {
        seen_slot: u64,
    },
    Received {
        tx: Arc<Transaction>,
        seen_slot: u64,
    },
}

enum RankingBlockView {
    HeaderPending,
    Pending {
        header: RankingBlockHeader,
        header_seen: Timestamp,
    },
    Requested {
        header: RankingBlockHeader,
        header_seen: Timestamp,
    },
    Received {
        rb: Arc<RankingBlock>,
        header_seen: Timestamp,
    },
}
impl RankingBlockView {
    fn header(&self) -> Option<&RankingBlockHeader> {
        match self {
            Self::HeaderPending => None,
            Self::Pending { header, .. } => Some(header),
            Self::Requested { header, .. } => Some(header),
            Self::Received { rb, .. } => Some(&rb.header),
        }
    }
    fn header_seen(&self) -> Option<Timestamp> {
        match self {
            Self::HeaderPending => None,
            Self::Pending { header_seen, .. } => Some(*header_seen),
            Self::Requested { header_seen, .. } => Some(*header_seen),
            Self::Received { header_seen, .. } => Some(*header_seen),
        }
    }
}

#[derive(Default)]
struct NodePraosState {
    peer_heads: BTreeMap<NodeId, u64>,
    blocks: BTreeMap<BlockId, RankingBlockView>,
    block_ids_by_slot: BTreeMap<u64, BlockId>,
}

#[derive(Debug)]
enum EndorserBlockView {
    Pending,
    Requested,
    Received {
        eb: Arc<EndorserBlock>,
        seen: Timestamp,
        all_txs_seen: bool,
        validated: bool,
    },
}

enum VoteBundleView {
    Requested,
    Received { votes: Arc<VoteBundle> },
}

#[derive(Default)]
struct NodeLeiosState {
    ebs: BTreeMap<EndorserBlockId, EndorserBlockView>,
    ebs_by_rb: BTreeMap<BlockId, EndorserBlockId>,
    eb_peer_announcements: BTreeMap<EndorserBlockId, Vec<NodeId>>,
    votes: BTreeMap<VoteBundleId, VoteBundleView>,
    votes_by_eb: BTreeMap<EndorserBlockId, BTreeMap<NodeId, usize>>,
    /// Running sum of `votes_by_eb`, so a quorum crossing is detected in
    /// O(1) as each bundle is counted rather than by re-summing every
    /// voter on every arrival.
    vote_weight_by_eb: BTreeMap<EndorserBlockId, u64>,
    /// EBs this node has already seen reach quorum, so the crossing is
    /// reported once per (node, EB).
    ///
    /// This set alone cannot carry that guarantee, because pruning drops
    /// its entries along with the rest of a pruned EB's state.  What
    /// carries it past a prune is `pruned_ebs`: `count_votes` refuses to
    /// count into a tombstoned EB at all, so an EB's entry is only ever
    /// removed from here once nothing can put it back.
    quorum_reached_ebs: BTreeSet<EndorserBlockId>,
    endorsed_ebs: BTreeMap<EndorserBlockId, u64>,
    incomplete_onchain_ebs: BTreeSet<EndorserBlockId>,
    missing_txs: BTreeMap<TransactionId, Vec<EndorserBlockId>>,
    /// EBs whose state has been pruned.  A tombstone rather than a plain
    /// deletion, so the paths that would otherwise rebuild that state from
    /// a late arrival -- header announcements, on-chain endorsements, and
    /// vote counting -- can tell "never seen" from "seen and finished
    /// with".
    pruned_ebs: BTreeSet<EndorserBlockId>,
}

#[derive(Default)]
struct LedgerState {
    spent_inputs: BTreeSet<u64>,
    seen_blocks: BTreeSet<BlockId>,
}

pub struct LinearLeiosNode {
    id: NodeId,
    sim_config: Arc<SimConfiguration>,
    queued: EventResult,
    tracker: EventTracker,
    clock: Clock,
    lottery: LotteryConfig,
    consumers: Vec<NodeId>,
    current_slot: u64,
    txs: BTreeMap<TransactionId, TransactionView>,
    mempool: Mempool,
    ledger_state: Option<(BlockId, LedgerState)>,
    praos: NodePraosState,
    leios: NodeLeiosState,
    behaviours: NodeBehaviours,

    eb_withholding_sender: Option<EBWithholdingSender>,
    eb_withholding_event_source: Option<mpsc::UnboundedReceiver<EBWithholdingEvent>>,
}

type EventResult = super::EventResult<LinearLeiosNode>;

impl NodeImpl for LinearLeiosNode {
    type Message = Message;
    type Task = CpuTask;
    type TimedEvent = TimedEvent;
    type CustomEvent = EBWithholdingEvent;

    fn new(
        config: &NodeConfiguration,
        sim_config: Arc<SimConfiguration>,
        tracker: EventTracker,
        _rng: ChaChaRng,
        clock: Clock,
    ) -> Self {
        // Linear Leios is fully stateless-RNG; `_rng` from NodeImpl::new
        // is ignored. Randomness is derived on demand from
        // Rng::new(sim_config.seed) with explicit (node, slot, site)
        // context. See sim-core/src/rng.rs.
        let lottery = LotteryConfig::Random {
            stake: config.stake,
            total_stake: sim_config.total_stake,
        };
        let mempool_max_size_bytes = sim_config.mempool_size_bytes;
        let tx_generated_backlog_max_size = sim_config.tx_generated_backlog_max_size;
        let tx_peer_backlog_max_size = sim_config.tx_peer_backlog_max_size;

        Self {
            id: config.id,
            sim_config,
            queued: EventResult::default(),
            tracker,
            clock,
            lottery,
            consumers: config.consumers.clone(),
            current_slot: 0,
            txs: BTreeMap::new(),
            mempool: Mempool::new(
                mempool_max_size_bytes,
                tx_generated_backlog_max_size,
                tx_peer_backlog_max_size,
            ),
            ledger_state: None,
            praos: NodePraosState::default(),
            leios: NodeLeiosState::default(),
            behaviours: config.behaviours.clone(),
            eb_withholding_sender: None,
            eb_withholding_event_source: None,
        }
    }

    fn custom_event_source(&mut self) -> Option<mpsc::UnboundedReceiver<Self::CustomEvent>> {
        self.eb_withholding_event_source.take()
    }

    fn handle_new_slot(&mut self, slot: u64) -> EventResult {
        self.current_slot = slot;
        self.try_generate_rb(slot);
        self.prune_old_txs(slot);
        self.prune_old_leios_state(slot);

        if slot.is_multiple_of(60) && self.id.to_inner() == 0 {
            self.log_memory_stats(slot);
        }

        std::mem::take(&mut self.queued)
    }

    fn handle_new_tx(&mut self, tx: Arc<Transaction>) -> EventResult {
        self.generate_tx(tx);
        std::mem::take(&mut self.queued)
    }

    fn handle_message(&mut self, from: NodeId, msg: Self::Message) -> EventResult {
        match msg {
            // TX propagation
            Message::AnnounceTx(id) => self.receive_announce_tx(from, id),
            Message::RequestTx(id) => self.receive_request_tx(from, id),
            Message::Tx(tx) => self.receive_tx(from, tx),

            // RB header propagation
            Message::AnnounceRBHeader(id) => self.receive_announce_rb_header(from, id),
            Message::RequestRBHeader(id) => self.receive_request_rb_header(from, id),
            Message::RBHeader(header, has_body, has_eb) => {
                self.receive_rb_header(from, header, has_body, has_eb)
            }

            // RB body propagation
            Message::AnnounceRB(id) => self.receive_announce_rb(from, id),
            Message::RequestRB(id) => self.receive_request_rb(from, id),
            Message::RB(rb) => self.receive_rb(from, rb),

            // EB body propagation
            Message::AnnounceEB(id) => self.receive_announce_eb(from, id),
            Message::RequestEB(id) => self.receive_request_eb(from, id),
            Message::EB(rb) => self.receive_eb(from, rb),

            // Vote propagation
            Message::AnnounceVotes(id) => self.receive_announce_votes(from, id),
            Message::RequestVotes(id) => self.receive_request_votes(from, id),
            Message::Votes(votes) => self.receive_votes(from, votes),

            // Per-vote variants are emitted exclusively by the shared-consensus
            // adapter; a sim run uses one adapter throughout so this
            // arm is unreachable.
            Message::AnnounceVote(_) | Message::RequestVote(_) | Message::Vote(_) => {
                unreachable!("linear_leios.rs does not exchange per-vote messages");
            }
            // EB-tx fetch.  For `LinearWithTxReferences` an EB only carries tx
            // ids on the wire, so a node that receives an EB referencing txs it
            // lacks actively fetches the bodies from the EB sender (which holds
            // them in `eb.txs`).  Mirrors shared-consensus and the Haskell
            // LeiosFetch mini-protocol.  We request directly from the sender
            // rather than gossiping availability, so AnnounceEBTxs is unused.
            Message::AnnounceEBTxs(_) => {
                unreachable!(
                    "linear_leios.rs requests EB-txs directly from the sender; it does not gossip availability"
                );
            }
            Message::RequestEBTxs(id, bitmap) => self.receive_request_eb_txs(from, id, bitmap),
            Message::EBTxs(id, txs) => self.receive_eb_txs(from, id, txs),
        }
        std::mem::take(&mut self.queued)
    }

    fn handle_cpu_task(&mut self, task: Self::Task) -> EventResult {
        match task {
            CpuTask::TransactionValidated(from, tx) => self.propagate_tx(from, tx),
            CpuTask::RBBlockGenerated(rb, eb) => self.finish_generating_rb(rb, eb),
            CpuTask::RBHeaderValidated(from, header, has_body, has_eb) => {
                self.finish_validating_rb_header(from, header, has_body, has_eb)
            }
            CpuTask::RBBlockValidated(rb) => self.finish_validating_rb(rb),
            CpuTask::EBHeaderValidated(from, eb) => self.finish_validating_eb_header(from, eb),
            CpuTask::EBBlockValidated(eb, seen) => self.finish_validating_eb(eb, seen),
            CpuTask::VTBundleGenerated(votes, eb) => self.finish_generating_vote_bundle(votes, &eb),
            CpuTask::VTBundleValidated(from, votes) => {
                self.finish_validating_vote_bundle(from, votes)
            }
            CpuTask::VoteGenerated(_) | CpuTask::VoteValidated(_, _) => {
                unreachable!("linear_leios.rs does not schedule per-vote CpuTasks");
            }
            CpuTask::RBBlockApplied(_) | CpuTask::EBBlockApplied(_) => {
                unreachable!(
                    "linear_leios.rs collapses validate+apply; ledger-apply CpuTasks are shared-consensus only"
                );
            }
        }
        std::mem::take(&mut self.queued)
    }

    fn handle_timed_event(&mut self, event: Self::TimedEvent) -> EventResult {
        match event {
            TimedEvent::TryVote(eb, seen) => self.vote_for_endorser_block(&eb, seen),
        }
        std::mem::take(&mut self.queued)
    }

    fn handle_custom_event(&mut self, event: Self::CustomEvent) -> EventResult {
        match event {
            EBWithholdingEvent::NewEB(eb, withheld_txs) => {
                self.receive_withheld_eb(eb, withheld_txs)
            }
            EBWithholdingEvent::DisseminateEB(eb, withheld_txs) => {
                self.disseminate_withheld_eb(eb, withheld_txs)
            }
        }
        std::mem::take(&mut self.queued)
    }
}

// TX pruning
impl LinearLeiosNode {
    // Remove old transactions from the txs map to bound memory growth.
    // A TX must meet both conditions to be pruned:
    // - Old enough that no new EB could reference it, and any EB that did
    //   reference it has already been received and validated (age > max_age).
    // - No longer in the mempool. A TX still in the mempool could be included
    //   in a future EB if previous EBs failed to get enough votes, so we must
    //   keep it in the txs map for has_tx() / sample_from_mempool() lookups.
    fn prune_old_txs(&mut self, current_slot: u64) {
        let Some(max_age) = self.sim_config.linear_tx_max_age_slots else {
            return;
        };
        let mempool = &self.mempool;
        self.txs.retain(|id, view| {
            let seen_slot = match view {
                TransactionView::Pending { seen_slot } => *seen_slot,
                TransactionView::Received { seen_slot, .. } => *seen_slot,
            };
            if current_slot.saturating_sub(seen_slot) <= max_age {
                return true;
            }
            mempool.contains(id)
        });
    }

    fn prune_old_leios_state(&mut self, _current_slot: u64) {
        // Only prune state for EBs that have been superseded: a strictly newer
        // EB has been endorsed on-chain, so the older ones will never be needed.
        let latest_endorsed_slot = self.leios.endorsed_ebs.values().copied().max();
        let Some(latest_slot) = latest_endorsed_slot else {
            return;
        };

        let expired: Vec<EndorserBlockId> = self
            .leios
            .votes_by_eb
            .keys()
            .filter(|eb_id| eb_id.slot < latest_slot)
            .copied()
            .collect();

        if expired.is_empty() {
            return;
        }

        for eb_id in &expired {
            // Don't prune EBs that are still being validated by the CPU
            if matches!(
                self.leios.ebs.get(eb_id),
                Some(EndorserBlockView::Received {
                    validated: false,
                    ..
                })
            ) {
                continue;
            }
            // Don't prune EBs that are on-chain but not yet fully validated.
            if self.leios.incomplete_onchain_ebs.contains(eb_id) {
                continue;
            }
            self.leios.endorsed_ebs.remove(eb_id);
            self.leios.votes_by_eb.remove(eb_id);
            self.leios.vote_weight_by_eb.remove(eb_id);
            self.leios.quorum_reached_ebs.remove(eb_id);
            self.leios.ebs.remove(eb_id);
            self.leios.eb_peer_announcements.remove(eb_id);
            self.leios.pruned_ebs.insert(*eb_id);
        }

        self.leios.votes.retain(|_, view| match view {
            VoteBundleView::Received { votes } => votes.ebs.keys().any(|eb| eb.slot >= latest_slot),
            VoteBundleView::Requested => true,
        });

        self.leios.ebs_by_rb.retain(|_, eb_id| {
            eb_id.slot >= latest_slot || self.leios.incomplete_onchain_ebs.contains(eb_id)
        });
    }

    fn log_memory_stats(&self, slot: u64) {
        let num_nodes = self.sim_config.nodes.len();

        // Process RSS from /proc/self/status
        let rss_mb = std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.starts_with("VmRSS:"))
                    .and_then(|l| l.split_whitespace().nth(1))
                    .and_then(|v| v.parse::<u64>().ok())
            })
            .map(|kb| kb as f64 / 1024.0)
            .unwrap_or(0.0);

        // txs: count received (hold Arc<Transaction>) vs pending (tiny)
        let txs_total = self.txs.len();
        let txs_received = self
            .txs
            .values()
            .filter(|v| matches!(v, TransactionView::Received { .. }))
            .count();
        // BTreeMap overhead ~48B/entry, Received: Arc ptr(8) + u64(8), unique Transaction ~56B
        let txs_bytes = txs_total * 64 + txs_received * 72;

        // mempool
        let (mp_len, mp_bytes, mp_local_bl, mp_peer_bl) = self.mempool.stats();
        let mempool_bytes = mp_bytes as usize + mp_len * 96 + (mp_local_bl + mp_peer_bl) * 8;

        // praos.blocks
        let praos_blocks = self.praos.blocks.len();
        let praos_tx_refs: usize = self
            .praos
            .blocks
            .values()
            .filter_map(|v| match v {
                RankingBlockView::Received { rb, .. } => Some(rb.transactions.len()),
                _ => None,
            })
            .sum();
        let praos_bytes = praos_blocks * 96 + praos_tx_refs * 56;

        // ledger_state
        let ledger_count: usize = self.ledger_state.is_some().into();
        let ledger_inputs = self
            .ledger_state
            .as_ref()
            .map(|(_, ls)| ls.spent_inputs.len())
            .unwrap_or(0);
        let ledger_bytes = ledger_inputs * 16;

        // leios.ebs: count entries, sum tx refs
        let ebs_count = self.leios.ebs.len();
        let ebs_tx_refs: usize = self
            .leios
            .ebs
            .values()
            .filter_map(|v| match v {
                EndorserBlockView::Received { eb, .. } => Some(eb.txs.len()),
                _ => None,
            })
            .sum();
        let ebs_bytes = ebs_count * 64 + ebs_tx_refs * 8;

        // leios.votes
        let votes_count = self.leios.votes.len();
        let votes_bytes = votes_count * 80;

        // leios.votes_by_eb
        let votes_by_eb_count = self.leios.votes_by_eb.len();
        let votes_by_eb_voters: usize = self.leios.votes_by_eb.values().map(|m| m.len()).sum();
        let votes_by_eb_bytes = votes_by_eb_count * 64 + votes_by_eb_voters * 32;

        // leios.eb_peer_announcements
        let eb_announce_count = self.leios.eb_peer_announcements.len();
        let eb_announce_peers: usize = self
            .leios
            .eb_peer_announcements
            .values()
            .map(|v| v.len())
            .sum();
        let eb_announce_bytes = eb_announce_count * 64 + eb_announce_peers * 8;

        // smaller collections
        let ebs_by_rb_count = self.leios.ebs_by_rb.len();
        let endorsed_count = self.leios.endorsed_ebs.len();
        let pruned_count = self.leios.pruned_ebs.len();
        let missing_txs_count = self.leios.missing_txs.len();
        let small_bytes =
            ebs_by_rb_count * 48 + endorsed_count * 40 + pruned_count * 32 + missing_txs_count * 64;

        let node_total = txs_bytes
            + mempool_bytes
            + praos_bytes
            + ledger_bytes
            + ebs_bytes
            + votes_bytes
            + votes_by_eb_bytes
            + eb_announce_bytes
            + small_bytes;
        let all_nodes_mb = (node_total * num_nodes) as f64 / (1024.0 * 1024.0);

        tracing::info!(
            "Memory stats at slot {} (node 0, x{} nodes):\n\
             \x20 txs:              {:>8} entries  ~ {:>6.1} MB  (received: {})\n\
             \x20 mempool:          {:>8} entries  ~ {:>6.1} MB  (backlog: {}/{} local, {}/{} peer)\n\
             \x20 praos.blocks:     {:>8} entries  ~ {:>6.1} MB  (tx_refs: {})\n\
             \x20 ledger_states:    {:>8} entries  ~ {:>6.1} MB  (spent_inputs: {})\n\
             \x20 leios.ebs:        {:>8} entries  ~ {:>6.1} MB  (tx_refs: {})\n\
             \x20 leios.votes:      {:>8} entries  ~ {:>6.1} MB\n\
             \x20 leios.votes_by_eb:{:>8} entries  ~ {:>6.1} MB  (voters: {})\n\
             \x20 leios.eb_announce:{:>8} entries  ~ {:>6.1} MB  (peers: {})\n\
             \x20 leios.ebs_by_rb:  {:>8} entries\n\
             \x20 leios.endorsed:   {:>8} entries\n\
             \x20 leios.pruned:     {:>8} entries\n\
             \x20 leios.missing_txs:{:>8} entries\n\
             \x20 ---\n\
             \x20 Estimated total: ~ {:.1} MB  (x {} = ~ {:.1} MB)\n\
             \x20 Process RSS: {:.0} MB",
            slot,
            num_nodes,
            txs_total,
            txs_bytes as f64 / 1e6,
            txs_received,
            mp_len,
            mempool_bytes as f64 / 1e6,
            mp_local_bl,
            self.mempool.tx_generated_backlog_max_size.unwrap_or(0),
            mp_peer_bl,
            self.mempool.tx_peer_backlog_max_size.unwrap_or(0),
            praos_blocks,
            praos_bytes as f64 / 1e6,
            praos_tx_refs,
            ledger_count,
            ledger_bytes as f64 / 1e6,
            ledger_inputs,
            ebs_count,
            ebs_bytes as f64 / 1e6,
            ebs_tx_refs,
            votes_count,
            votes_bytes as f64 / 1e6,
            votes_by_eb_count,
            votes_by_eb_bytes as f64 / 1e6,
            votes_by_eb_voters,
            eb_announce_count,
            eb_announce_bytes as f64 / 1e6,
            eb_announce_peers,
            ebs_by_rb_count,
            endorsed_count,
            pruned_count,
            missing_txs_count,
            node_total as f64 / 1e6,
            num_nodes,
            all_nodes_mb,
            rss_mb,
        );

        // Network queue stats (aggregated across all shards by the sequential engine)
        if let Some(ref collector) = self.sim_config.network_stats {
            let (connections, active, msgs, bytes) = collector.totals();
            tracing::info!(
                "Network queue stats (all shards):\n\
                 \x20 connections: {} total, {} active\n\
                 \x20 queued messages: {}\n\
                 \x20 queued bytes: {:.1} MB",
                connections,
                active,
                msgs,
                bytes as f64 / (1024.0 * 1024.0),
            );
        }
    }
}

// Transaction propagation
impl LinearLeiosNode {
    fn receive_announce_tx(&mut self, from: NodeId, id: TransactionId) {
        if self.txs.get(&id).is_none_or(|t| {
            self.sim_config.relay_strategy == RelayStrategy::RequestFromAll
                && matches!(t, TransactionView::Pending { .. })
        }) {
            self.txs.insert(
                id,
                TransactionView::Pending {
                    seen_slot: self.current_slot,
                },
            );
            self.queued.send_to(from, Message::RequestTx(id));
        }
    }

    fn receive_request_tx(&mut self, from: NodeId, id: TransactionId) {
        if let Some(TransactionView::Received { tx, .. }) = self.txs.get(&id) {
            self.tracker.track_transaction_sent(tx, self.id, from);
            self.queued.send_to(from, Message::Tx(tx.clone()));
        }
    }

    fn receive_tx(&mut self, from: NodeId, tx: Arc<Transaction>) {
        self.tracker
            .track_transaction_received(tx.id, from, self.id);
        self.queued
            .schedule_cpu_task(CpuTask::TransactionValidated(from, tx));
    }

    /// A peer asked for the bodies of some txs referenced by an EB we hold.
    /// Every node that received the EB has the bodies in `eb.txs`, so pull the
    /// bitmap-addressed entries straight out and reply with `EBTxs`.
    fn receive_request_eb_txs(
        &mut self,
        from: NodeId,
        id: EndorserBlockId,
        bitmap: BTreeMap<u16, u64>,
    ) {
        let Some(EndorserBlockView::Received { eb, .. }) = self.leios.ebs.get(&id) else {
            return;
        };
        let bodies: Vec<Arc<Transaction>> = shared_consensus::bitmap::iter_indices(&bitmap)
            .filter_map(|i| eb.txs.get(i as usize).cloned())
            .collect();
        if bodies.is_empty() {
            return;
        }
        self.queued.send_to(from, Message::EBTxs(id, bodies));
    }

    /// Bodies for an EB's referenced txs arrived.  Feed each through the normal
    /// tx-validation pipeline (exactly like `receive_tx`): `propagate_tx` admits
    /// them, re-announces to peers (closing the gap for other stuck nodes), and
    /// `acknowledge_tx` releases any EB that was gated on them.
    fn receive_eb_txs(&mut self, from: NodeId, _id: EndorserBlockId, txs: Vec<Arc<Transaction>>) {
        for tx in txs {
            // Skip bodies we already hold — they may have arrived via normal
            // diffusion between our request and this reply.  Mirrors
            // shared_consensus::receive_eb_txs; avoids a redundant
            // TransactionValidated task and a spurious "received" event.
            if self.has_tx(tx.id) {
                continue;
            }
            self.tracker
                .track_transaction_received(tx.id, from, self.id);
            self.queued
                .schedule_cpu_task(CpuTask::TransactionValidated(from, tx));
        }
    }

    fn generate_tx(&mut self, tx: Arc<Transaction>) {
        self.tracker.track_transaction_generated(&tx, self.id);
        if self.mempool.is_generated_backlog_full() {
            self.tracker
                .track_transaction_lost(tx.id, TransactionLostReason::GeneratedBacklogFull);
            return;
        }
        self.propagate_tx(self.id, tx);
    }

    fn propagate_tx(&mut self, from: NodeId, tx: Arc<Transaction>) {
        let id = tx.id;
        let seen_slot = self
            .txs
            .get(&id)
            .map(|v| match v {
                TransactionView::Pending { seen_slot } => *seen_slot,
                TransactionView::Received { seen_slot, .. } => *seen_slot,
            })
            .unwrap_or(self.current_slot);
        if self
            .txs
            .insert(
                id,
                TransactionView::Received {
                    tx: tx.clone(),
                    seen_slot,
                },
            )
            .is_some_and(|tv| matches!(tv, TransactionView::Received { .. }))
        {
            return;
        }

        let is_local = from == self.id;
        let result = self.try_add_tx_to_mempool(&tx, is_local);

        if matches!(result, InsertResult::PeerBacklogFull) {
            // Peer backlog full. If a pending EB references this TX
            // (it's in missing_txs), keep it in self.txs so has_tx
            // returns true and try_validating_eb's scan can succeed.
            // Otherwise drop it to bound memory.
            if !self.leios.missing_txs.contains_key(&id) {
                self.txs.remove(&id);
                return;
            }
            // Fall through: TX stays in self.txs, acknowledge below.
        }

        let referenced_by_eb = self.acknowledge_tx(&tx);
        let added_to_mempool = matches!(result, InsertResult::AddedToMempool);

        // If we added the TX to our mempool, we want to propagate it so our peers can as well.
        // If it was referenced by an EB, we want to propagate it so our peers have the full EB.
        // TODO: should send to producers instead (make configurable)
        if referenced_by_eb || added_to_mempool {
            for peer in &self.consumers {
                if *peer == from {
                    continue;
                }
                self.queued.send_to(*peer, Message::AnnounceTx(id));
            }
        }
    }

    fn has_tx(&self, tx_id: TransactionId) -> bool {
        matches!(self.txs.get(&tx_id), Some(TransactionView::Received { .. }))
    }

    fn acknowledge_tx(&mut self, tx: &Transaction) -> bool {
        let Some(eb_ids) = self.leios.missing_txs.remove(&tx.id) else {
            return false;
        };
        for eb_id in eb_ids {
            self.try_validating_eb(eb_id);
        }
        true
    }
}

// Ranking block propagation
impl LinearLeiosNode {
    fn try_generate_rb(&mut self, slot: u64) {
        let Some(vrf) = self.run_vrf(
            LotteryKind::GenerateRB,
            self.sim_config.block_generation_probability,
            slot,
            DrawSite::RbLottery,
        ) else {
            return;
        };

        let parent = self.latest_rb_id();
        let endorsement = parent.and_then(|rb_id| {
            let earliest_endorse_time = self
                .sim_config
                .voting_window()
                .inclusion_deadline_at(rb_id.slot);

            if earliest_endorse_time > Timestamp::from_secs(slot) {
                // This RB was generated too quickly after another; hasn't been time to gather all the votes.
                // No endorsement.
                return None;
            }

            let eb_id = *self.leios.ebs_by_rb.get(&rb_id)?;
            let votes = self.leios.votes_by_eb.get(&eb_id)?;
            let total_votes = votes.values().copied().sum::<usize>();
            if (total_votes as u64) < self.sim_config.vote_threshold() {
                // Not enough votes. No endorsement.
                return None;
            }
            let votes = votes.clone();

            if let Some(eb) = self.get_validated_eb(eb_id) {
                // If we're endorsing this EB, clear its TXs out of the mempool now
                // so that we don't include them in new blocks.
                self.remove_eb_txs_from_mempool(&eb);
            } else if !self.leios.pruned_ebs.contains(&eb_id) {
                // We haven't finished validating this EB, maybe even haven't received it and its contents.
                // That won't stop us from generating the endorsement, though it'll make us produce an empty block.
                // Skip if the EB was already validated and pruned — no conflict risk.
                self.leios.incomplete_onchain_ebs.insert(eb_id);
            }

            self.leios.endorsed_ebs.entry(eb_id).or_insert(slot);

            Some(Endorsement {
                eb: eb_id,
                size_bytes: self.sim_config.sizes.cert(votes.len()),
                votes: votes.clone(),
            })
        });

        // If we haven't validated any EBs from the current chain, we have no way to tell whether
        // including a TX would introduce conflicts. So, don't include ANY TXs, just to be safe.
        let produce_empty_block = !self.leios.incomplete_onchain_ebs.is_empty();

        let mut rb_transactions = vec![];
        if !produce_empty_block && self.sim_config.praos_fallback && endorsement.is_none() {
            if let TransactionConfig::Mock(config) = &self.sim_config.transactions {
                // Add one transaction, the right size for the extra RB payload
                let tx = config.mock_tx(config.rb_size);
                self.tracker.track_transaction_generated(&tx, self.id);
                rb_transactions.push(Arc::new(tx));
            } else {
                self.sample_from_mempool(
                    &mut rb_transactions,
                    self.sim_config.max_block_size,
                    true,
                    slot,
                    0,
                );
            }
        }

        let mut eb_transactions = vec![];
        let mut withheld_txs = vec![];
        if !produce_empty_block {
            // If we are performing a "withheld TX" attack, we will include a bunch of brand-new TXs in this EB.
            // They will get disseminated through the network at the same time as the EB.
            withheld_txs = self.generate_withheld_txs(slot);
            eb_transactions.extend(withheld_txs.iter().cloned());

            if let TransactionConfig::Mock(config) = &self.sim_config.transactions {
                // Add one transaction, the right size for the extra RB payload
                let extra_size =
                    config.eb_size - withheld_txs.iter().map(|tx| tx.bytes).sum::<u64>();
                if extra_size > 0 {
                    let tx = config.mock_tx(extra_size);
                    self.tracker.track_transaction_generated(&tx, self.id);
                    eb_transactions.push(Arc::new(tx));
                }
            } else {
                self.sample_from_mempool(
                    &mut eb_transactions,
                    self.sim_config.max_eb_size,
                    false,
                    slot,
                    1,
                );
            }
        }
        let (eb_announcement, eb) = if eb_transactions.is_empty() {
            (None, None)
        } else {
            let eb_id = EndorserBlockId {
                slot,
                pipeline: 0,
                producer: self.id,
            };
            let eb = EndorserBlock {
                slot,
                producer: self.id,
                bytes: self.sim_config.sizes.linear_eb(&eb_transactions),
                txs: eb_transactions,
            };
            (Some(eb_id), Some((eb, withheld_txs)))
        };

        let rb = RankingBlock {
            header: RankingBlockHeader {
                id: BlockId {
                    slot,
                    producer: self.id,
                },
                vrf,
                parent,
                bytes: self.sim_config.sizes.block_header,
                eb_announcement,
            },
            transactions: rb_transactions,
            endorsement,
        };

        self.tracker.track_praos_block_lottery_won(rb.header.id);
        self.queued
            .schedule_cpu_task(CpuTask::RBBlockGenerated(rb, eb));
    }

    fn finish_generating_rb(
        &mut self,
        rb: RankingBlock,
        eb: Option<(EndorserBlock, Vec<Arc<Transaction>>)>,
    ) {
        self.tracker.track_linear_rb_generated(&rb);
        self.publish_rb(Arc::new(rb), false);
        if let Some((eb, withheld_txs)) = eb {
            self.tracker.track_linear_eb_generated(&eb);
            self.finish_generating_eb(eb, withheld_txs);
        }
    }

    fn publish_rb(&mut self, rb: Arc<RankingBlock>, already_sent_header: bool) {
        self.remove_rb_txs_from_mempool(&rb);
        for peer in &self.consumers {
            if self
                .praos
                .peer_heads
                .get(peer)
                .is_none_or(|&s| s < rb.header.id.slot)
            {
                let message = if already_sent_header {
                    Message::AnnounceRB(rb.header.id)
                } else {
                    Message::AnnounceRBHeader(rb.header.id)
                };
                self.queued.send_to(*peer, message);
                self.praos.peer_heads.insert(*peer, rb.header.id.slot);
            }
        }
        let header_seen = self
            .praos
            .blocks
            .get(&rb.header.id)
            .and_then(|rb| rb.header_seen())
            .unwrap_or(self.clock.now());
        if let Some(eb_id) = rb.header.eb_announcement {
            self.leios.ebs_by_rb.insert(rb.header.id, eb_id);
        }
        self.praos
            .blocks
            .insert(rb.header.id, RankingBlockView::Received { rb, header_seen });
    }

    fn receive_announce_rb_header(&mut self, from: NodeId, id: BlockId) {
        let should_request = match self.praos.blocks.get(&id) {
            None => true,
            Some(RankingBlockView::HeaderPending) => {
                self.sim_config.relay_strategy == RelayStrategy::RequestFromAll
            }
            _ => false,
        };
        if should_request {
            self.praos
                .blocks
                .insert(id, RankingBlockView::HeaderPending);
            self.queued.send_to(from, Message::RequestRBHeader(id));
        }
    }

    fn receive_request_rb_header(&mut self, from: NodeId, id: BlockId) {
        if let Some(rb) = self.praos.blocks.get(&id)
            && let Some(header) = rb.header()
        {
            // If we already have this RB's body,
            // let the requester know that it's ready to fetch.
            let have_body = matches!(rb, RankingBlockView::Received { .. });
            // If we already have the EB announced by this RB,
            // let the requester know that they can fetch it.
            // But if we are maliciously withholding the EB, do not let them know.
            let have_eb = header.eb_announcement.is_some_and(|eb_id| {
                matches!(
                    self.leios.ebs.get(&eb_id),
                    Some(EndorserBlockView::Received { .. })
                )
            }) && !self.should_withhold_ebs();
            self.queued
                .send_to(from, Message::RBHeader(header.clone(), have_body, have_eb));
        }
    }

    fn receive_rb_header(
        &mut self,
        from: NodeId,
        header: RankingBlockHeader,
        has_body: bool,
        has_eb: bool,
    ) {
        self.queued
            .schedule_cpu_task(CpuTask::RBHeaderValidated(from, header, has_body, has_eb));
    }

    fn finish_validating_rb_header(
        &mut self,
        from: NodeId,
        header: RankingBlockHeader,
        has_body: bool,
        has_eb: bool,
    ) {
        if let Some(old_block_id) = self.praos.block_ids_by_slot.get(&header.id.slot) {
            // SLOT BATTLE!!! lower VRF wins
            if let Some(old_header) = self.praos.blocks.get(old_block_id).and_then(|b| b.header()) {
                if old_header.vrf <= header.vrf {
                    // We like our block better than this new one.
                    return;
                }

                // Forget we ever saw that other block
                if let Some(RankingBlockView::Received { rb, .. }) =
                    self.praos.blocks.remove(old_block_id)
                    && let Some(endorsement) = &rb.endorsement
                {
                    self.leios.incomplete_onchain_ebs.remove(&endorsement.eb);
                }
            }
        }
        self.praos
            .block_ids_by_slot
            .insert(header.id.slot, header.id);
        self.praos.blocks.insert(
            header.id,
            RankingBlockView::Pending {
                header: header.clone(),
                header_seen: self.clock.now(),
            },
        );

        let head = self.praos.peer_heads.entry(from).or_default();
        if *head < header.id.slot {
            *head = header.id.slot
        }
        for peer in &self.consumers {
            if *peer == from {
                continue;
            }
            self.queued
                .send_to(*peer, Message::AnnounceRBHeader(header.id));
        }
        if has_body {
            self.queued.send_to(from, Message::RequestRB(header.id));
        }

        // Get ready to fetch the announced EB (if we don't have it already)
        let Some(eb_id) = header.eb_announcement else {
            return;
        };
        if matches!(
            self.leios.ebs.get(&eb_id),
            Some(EndorserBlockView::Received { .. })
        ) {
            return;
        }
        if self.leios.pruned_ebs.contains(&eb_id) {
            return;
        }

        let eb_peer_announcements = self.leios.eb_peer_announcements.entry(eb_id).or_default();
        if has_eb {
            eb_peer_announcements.push(from);
        }

        // TODO: freshest first
        let peers_to_request_from = match self.sim_config.relay_strategy {
            RelayStrategy::RequestFromFirst => eb_peer_announcements
                .first()
                .iter()
                .copied()
                .copied()
                .collect::<Vec<_>>(),
            RelayStrategy::RequestFromAll => eb_peer_announcements.clone(),
        };

        if peers_to_request_from.is_empty() {
            // nobody we know has this EB yet, wait for someone to announce it
            self.leios.ebs.insert(eb_id, EndorserBlockView::Pending);
        } else {
            for peer in peers_to_request_from {
                self.queued.send_to(peer, Message::RequestEB(eb_id));
            }
            self.leios.ebs.insert(eb_id, EndorserBlockView::Requested);
        }
    }

    fn receive_announce_rb(&mut self, from: NodeId, id: BlockId) {
        let (header, header_seen) = match self.praos.blocks.get(&id) {
            Some(RankingBlockView::Pending {
                header,
                header_seen,
                ..
            }) => (header.clone(), *header_seen),
            Some(RankingBlockView::Requested {
                header,
                header_seen,
            }) => {
                if self.sim_config.relay_strategy == RelayStrategy::RequestFromAll {
                    (header.clone(), *header_seen)
                } else {
                    return;
                }
            }
            _ => return,
        };

        self.praos.blocks.insert(
            id,
            RankingBlockView::Requested {
                header,
                header_seen,
            },
        );
        self.queued.send_to(from, Message::RequestRB(id));
    }

    fn receive_request_rb(&mut self, from: NodeId, id: BlockId) {
        if let Some(RankingBlockView::Received { rb, .. }) = self.praos.blocks.get(&id) {
            self.tracker.track_linear_rb_sent(rb, self.id, from);
            self.queued.send_to(from, Message::RB(rb.clone()));
        }
    }

    fn receive_rb(&mut self, from: NodeId, rb: Arc<RankingBlock>) {
        self.tracker.track_linear_rb_received(&rb, from, self.id);
        self.queued.schedule_cpu_task(CpuTask::RBBlockValidated(rb));
    }

    fn finish_validating_rb(&mut self, rb: Arc<RankingBlock>) {
        let header_seen = self
            .praos
            .blocks
            .get(&rb.header.id)
            .and_then(|rb| rb.header_seen())
            .unwrap_or(self.clock.now());
        self.praos.blocks.insert(
            rb.header.id,
            RankingBlockView::Received {
                rb: rb.clone(),
                header_seen,
            },
        );
        if let Some(endorsement) = &rb.endorsement {
            self.leios
                .endorsed_ebs
                .entry(endorsement.eb)
                .or_insert(rb.header.id.slot);
            if !self.is_eb_validated(endorsement.eb)
                && !self.leios.pruned_ebs.contains(&endorsement.eb)
            {
                self.leios.incomplete_onchain_ebs.insert(endorsement.eb);
            }
        }

        self.publish_rb(rb, true);
    }

    fn latest_rb(&self) -> Option<(&Arc<RankingBlock>, Timestamp)> {
        self.praos.blocks.iter().rev().find_map(|(_, rb)| {
            if let RankingBlockView::Received { rb, header_seen } = rb {
                Some((rb, *header_seen))
            } else {
                None
            }
        })
    }

    fn latest_rb_id(&self) -> Option<BlockId> {
        self.latest_rb().map(|(rb, _)| rb.header.id)
    }
}

// EB operations
impl LinearLeiosNode {
    fn finish_generating_eb(&mut self, eb: EndorserBlock, withheld_txs: Vec<Arc<Transaction>>) {
        let eb_id = eb.id();
        let eb = Arc::new(eb);
        self.leios.ebs.insert(
            eb_id,
            EndorserBlockView::Received {
                eb: eb.clone(),
                seen: self.clock.now(),
                all_txs_seen: true,
                validated: true,
            },
        );

        if self.should_withhold_ebs() {
            // We're an evil attacker, holding onto this EB until just long enough to collect votes.
            // Send it out-of-band to our evil buddies.
            self.share_new_withheld_eb(&eb, withheld_txs);
        } else {
            // We're a "well-behaved" node who will tell all our peers about this EB immediately.
            for peer in &self.consumers {
                self.queued.send_to(*peer, Message::AnnounceEB(eb_id));
                // If we were withholding some of the EB's transactions, start disseminating them now.
                for tx in &withheld_txs {
                    self.queued.send_to(*peer, Message::AnnounceTx(tx.id));
                }
            }
        }
        self.vote_for_endorser_block(&eb, self.clock.now());
    }

    fn receive_announce_eb(&mut self, from: NodeId, id: EndorserBlockId) {
        self.leios
            .eb_peer_announcements
            .entry(id)
            .or_default()
            .push(from);
        let should_request = match self.leios.ebs.get(&id) {
            Some(EndorserBlockView::Pending) => true,
            Some(EndorserBlockView::Requested) => {
                self.sim_config.relay_strategy == RelayStrategy::RequestFromAll
            }
            _ => false,
        };
        if should_request {
            // TODO: freshest first
            self.leios.ebs.insert(id, EndorserBlockView::Requested);
            self.queued.send_to(from, Message::RequestEB(id));
        }
    }

    fn receive_request_eb(&mut self, from: NodeId, id: EndorserBlockId) {
        if let Some(EndorserBlockView::Received { eb, .. }) = self.leios.ebs.get(&id) {
            self.tracker.track_linear_eb_sent(eb, self.id, from);
            self.queued.send_to(from, Message::EB(eb.clone()));
        }
    }

    fn receive_eb(&mut self, from: NodeId, eb: Arc<EndorserBlock>) {
        self.tracker.track_eb_received(eb.id(), from, self.id);
        self.queued
            .schedule_cpu_task(CpuTask::EBHeaderValidated(from, eb));
    }

    fn finish_validating_eb_header(&mut self, from: NodeId, eb: Arc<EndorserBlock>) {
        if let Some(EndorserBlockView::Received { .. }) = self.leios.ebs.get(&eb.id()) {
            // already received this EB
            return;
        }
        let seen = self.clock.now();
        // Indices (into `eb.txs`) of referenced bodies we don't yet hold.  The
        // inline `Linear` variant carries bodies in the EB, so nothing is ever
        // missing there.  Computed once and reused for both the gate-tracking
        // map and the RequestEBTxs bitmap below.
        let missing_indices: Vec<u32> = if matches!(self.sim_config.variant, LeiosVariant::Linear) {
            vec![]
        } else {
            eb.txs
                .iter()
                .enumerate()
                .filter(|(_, tx)| !self.has_tx(tx.id))
                .map(|(i, _)| i as u32)
                .collect()
        };
        self.leios.ebs.insert(
            eb.id(),
            EndorserBlockView::Received {
                eb: eb.clone(),
                seen,
                all_txs_seen: missing_indices.is_empty(),
                validated: false,
            },
        );

        let should_propagate_now = match self.sim_config.linear_eb_propagation_criteria {
            EBPropagationCriteria::EbReceived => true,
            EBPropagationCriteria::TxsReceived => missing_indices.is_empty(),
            EBPropagationCriteria::FullyValid => false,
        };
        if should_propagate_now {
            for peer in &self.consumers {
                if *peer == from {
                    continue;
                }
                self.queued.send_to(*peer, Message::AnnounceEB(eb.id()));
            }
        }

        if missing_indices.is_empty() {
            self.queued
                .schedule_cpu_task(CpuTask::EBBlockValidated(eb.clone(), seen));
        } else {
            // Record which EB each missing tx gates, then actively fetch the
            // bodies from the EB sender.  It forwarded us the EB, so it holds
            // every body in `eb.txs`; one RequestEBTxs resolves the set.
            // Without this the referenced txs are never re-requested and the
            // MissingTX gate in `should_vote_for` never clears.
            for &idx in &missing_indices {
                self.leios
                    .missing_txs
                    .entry(eb.txs[idx as usize].id)
                    .or_default()
                    .push(eb.id());
            }
            let bitmap = shared_consensus::bitmap::from_indices(&missing_indices);
            self.queued
                .send_to(from, Message::RequestEBTxs(eb.id(), bitmap));
        }

        if matches!(
            self.sim_config.variant,
            LeiosVariant::LinearWithTxReferences
        ) {
            // If the EB references any TXs which we already have, but are not in our mempool,
            // either we must have failed to add them to the mempool due to conflicts,
            // or they haven't reached the mempool _yet_.
            // Announce those TXs to our peers, since either way we didn't before.
            let mempool_ids = self.mempool.ids().collect::<HashSet<_>>();
            for tx in &eb.txs {
                if !self.has_tx(tx.id) || mempool_ids.contains(&tx.id) {
                    continue;
                }
                for peer in &self.consumers {
                    self.queued.send_to(*peer, Message::AnnounceTx(tx.id));
                }
            }
        }
    }

    fn try_validating_eb(&mut self, eb_id: EndorserBlockId) {
        let Some(EndorserBlockView::Received {
            eb,
            seen,
            all_txs_seen: false,
            validated: false,
        }) = self.leios.ebs.get(&eb_id)
        else {
            return;
        };
        let all_seen = eb.txs.iter().all(|tx| self.has_tx(tx.id));
        if all_seen {
            let eb = eb.clone();
            let seen = *seen;
            self.leios.ebs.insert(
                eb_id,
                EndorserBlockView::Received {
                    eb: eb.clone(),
                    seen,
                    all_txs_seen: true,
                    validated: false,
                },
            );
            if matches!(
                self.sim_config.linear_eb_propagation_criteria,
                EBPropagationCriteria::TxsReceived
            ) {
                // We have received all transactions, but haven't validated the entirety of the EB yet.
                // Propagate it now anyway.
                for peer in &self.consumers {
                    self.queued.send_to(*peer, Message::AnnounceEB(eb_id));
                }
            }
            self.queued
                .schedule_cpu_task(CpuTask::EBBlockValidated(eb, seen));
        }
    }

    fn finish_validating_eb(&mut self, eb: Arc<EndorserBlock>, seen: Timestamp) {
        if self.leios.incomplete_onchain_ebs.remove(&eb.id()) {
            self.remove_eb_txs_from_mempool(&eb);
        }
        let Some(EndorserBlockView::Received { validated, .. }) = self.leios.ebs.get_mut(&eb.id())
        else {
            tracing::warn!("EB {} was pruned before CPU validation completed", eb.id());
            return;
        };
        *validated = true;
        if matches!(
            self.sim_config.linear_eb_propagation_criteria,
            EBPropagationCriteria::FullyValid
        ) {
            // We have received all transactions, but haven't validated the entirety of the EB yet.
            // Propagate it now anyway.
            for peer in &self.consumers {
                self.queued.send_to(*peer, Message::AnnounceEB(eb.id()));
            }
        }
        self.vote_for_endorser_block(&eb, seen);
    }

    fn is_eb_validated(&self, eb_id: EndorserBlockId) -> bool {
        self.get_validated_eb(eb_id).is_some()
    }

    fn get_validated_eb(&self, eb_id: EndorserBlockId) -> Option<Arc<EndorserBlock>> {
        match self.leios.ebs.get(&eb_id) {
            Some(EndorserBlockView::Received {
                eb,
                validated: true,
                ..
            }) => Some(eb.clone()),
            _ => None,
        }
    }
}

// EB withholding:
// an attack on Linear Leios where one or more stake pools deliberately wait to
// propagate an EB until there is just barely enough time for honest nodes to vote on it.
// This increases the odds that an honest RB producer won't have the parent RB's EB yet,
// meaning they will need to publish a completely empty RB.
impl LinearLeiosNode {
    // This is called during simulation setup.
    // It tells this node that it should withhold EBs,
    // and sets up a side channel with all other nodes performing the same attack.
    pub fn register_as_eb_withholder(
        &mut self,
        sender: EBWithholdingSender,
    ) -> mpsc::UnboundedSender<EBWithholdingEvent> {
        self.eb_withholding_sender = Some(sender);
        let (sink, source) = mpsc::unbounded_channel();
        self.eb_withholding_event_source = Some(source);
        sink
    }

    fn should_withhold_ebs(&self) -> bool {
        self.eb_withholding_sender.is_some()
    }

    fn share_new_withheld_eb(
        &mut self,
        eb: &Arc<EndorserBlock>,
        withheld_txs: Vec<Arc<Transaction>>,
    ) {
        let sender = self.eb_withholding_sender.as_ref().unwrap();
        sender.send(eb.clone(), withheld_txs);
    }

    fn receive_withheld_eb(&mut self, eb: Arc<EndorserBlock>, withheld_txs: Vec<Arc<Transaction>>) {
        self.leios.ebs.insert(
            eb.id(),
            EndorserBlockView::Received {
                eb: eb.clone(),
                seen: self.clock.now(),
                all_txs_seen: true,
                validated: true,
            },
        );
        for tx in withheld_txs {
            // Add the peer's withheld TXs to the list we know of,
            // but not to our mempools
            self.txs.insert(
                tx.id,
                TransactionView::Received {
                    tx,
                    seen_slot: self.current_slot,
                },
            );
        }
        // If an attacker receives an EB over a side channel,
        // it will skip validation and will not disseminate it to peers.
        // It will, however, try to vote for the EB immediately.
        self.vote_for_endorser_block(&eb, self.clock.now());
    }

    fn disseminate_withheld_eb(
        &mut self,
        eb_id: EndorserBlockId,
        withheld_txs: Vec<TransactionId>,
    ) {
        for peer in &self.consumers {
            self.queued.send_to(*peer, Message::AnnounceEB(eb_id));
            for tx_id in &withheld_txs {
                self.queued.send_to(*peer, Message::AnnounceTx(*tx_id));
            }
        }
    }
}

// TX withholding:
// an attack where a stake pool generates EBs with previously unknown
// transactions, so that they cannot propagate in advance.
// We implement this by generating the transactions at the same time as the EB itself.
impl LinearLeiosNode {
    fn generate_withheld_txs(&mut self, slot: u64) -> Vec<Arc<Transaction>> {
        if !self.behaviours.withhold_txs {
            return vec![];
        }
        let withhold_tx_config = self.sim_config.attacks.late_tx.as_ref().unwrap();
        let slot_ts = Timestamp::from_secs(slot);
        if withhold_tx_config.start_time.is_some_and(|s| slot_ts < s) {
            return vec![];
        }
        if withhold_tx_config.stop_time.is_some_and(|s| slot_ts > s) {
            return vec![];
        }
        let rng = Rng::new(self.sim_config.seed);
        if !rng.draw_bool(
            self.id,
            slot,
            DrawSite::WithholdDecision,
            withhold_tx_config.probability,
        ) {
            return vec![];
        }

        let mut count_rng = rng.seeded_chacha(self.id, slot, DrawSite::WithholdTxCount);
        let txs_to_generate = withhold_tx_config.txs_to_generate.sample(&mut count_rng) as u64;
        let mut txs = vec![];
        for i in 0..txs_to_generate {
            let tx = match &self.sim_config.transactions {
                TransactionConfig::Real(cfg) => {
                    let mut tx_rng =
                        rng.seeded_chacha(self.id, slot, DrawSite::TxGenBody { tx_idx: i });
                    cfg.new_tx(&mut tx_rng, None)
                }
                TransactionConfig::Mock(cfg) => cfg.mock_tx(cfg.eb_size / txs_to_generate),
            };
            self.tracker.track_transaction_generated(&tx, self.id);
            let tx = Arc::new(tx);
            self.txs.insert(
                tx.id,
                TransactionView::Received {
                    tx: tx.clone(),
                    seen_slot: self.current_slot,
                },
            );
            txs.push(tx);
        }
        txs
    }
}

// Voting
impl LinearLeiosNode {
    fn vote_for_endorser_block(&mut self, eb: &Arc<EndorserBlock>, seen: Timestamp) {
        // CIP-0164 §Equivocation Detection: voting begins 3 * L_hdr after
        // the slot of the announcing RB, not after the EB was built.
        let equivocation_cutoff_time = self.sim_config.voting_window().gate_at(eb.slot);
        if self.clock.now() < equivocation_cutoff_time {
            // If we haven't waited long enough to detect equivocations,
            // schedule voting later.
            self.queued.schedule_event(
                equivocation_cutoff_time,
                TimedEvent::TryVote(eb.clone(), seen),
            );
            return;
        }
        if !self.try_vote_for_endorser_block(eb, seen) && self.sim_config.emit_conformance_events {
            self.tracker
                .track_linear_no_vote_generated(self.id, eb.id());
        }
    }

    fn try_vote_for_endorser_block(&mut self, eb: &Arc<EndorserBlock>, seen: Timestamp) -> bool {
        let vrf_wins = match self.sim_config.committee_selection {
            CommitteeSelectionAlgorithm::WfaLs => {
                let eb_id = eb.id();
                vrf_probabilities(self.sim_config.vote_probability)
                    .enumerate()
                    .filter_map(|(trial, f)| {
                        self.run_vrf(
                            LotteryKind::GenerateVote,
                            f,
                            eb.slot,
                            DrawSite::VoteVrf {
                                eb_id,
                                trial: trial as u16,
                            },
                        )
                    })
                    .count()
            }
            CommitteeSelectionAlgorithm::Everyone => 1,
            // CIP-0164 PRs #1196 / #1250: certificate weight is the voter's own
            // stake; the receiver sums these and compares against
            // `quorum_weight_fraction × total_active_stake`.
            CommitteeSelectionAlgorithm::TopStakeFraction
            | CommitteeSelectionAlgorithm::TopStakeSeats => {
                if self.sim_config.vote_eligible_nodes.contains(&self.id) {
                    self.sim_config.nodes[self.id.to_inner()].stake as usize
                } else {
                    0
                }
            }
        };
        if vrf_wins == 0 {
            return false;
        }

        let id = VoteBundleId {
            slot: eb.slot,
            pipeline: 0,
            producer: self.id,
        };
        self.tracker.track_vote_lottery_won(id);

        if let Err(reason) = self.should_vote_for(eb, seen) {
            self.tracker
                .track_no_vote(eb.slot, 0, self.id, eb.id(), reason);
            return false;
        }

        let mut ebs = BTreeMap::new();
        ebs.insert(eb.id(), vrf_wins);
        let votes = VoteBundle {
            id,
            bytes: self.sim_config.sizes.vote_bundle(1),
            ebs,
        };
        self.queued
            .schedule_cpu_task(CpuTask::VTBundleGenerated(votes, eb.clone()));
        true
    }

    fn should_vote_for(&self, eb: &EndorserBlock, seen: Timestamp) -> Result<(), NoVoteReason> {
        let eb_must_be_received_by = self.sim_config.voting_window().deadline_at(eb.slot);
        if seen > eb_must_be_received_by || self.clock.now() > eb_must_be_received_by {
            // Arrival alone is insufficient: validation must finish by
            // t0 + 3 * L_hdr + L_vote, including CPU queueing.
            return Err(NoVoteReason::LateEB);
        }
        let Some((rb, header_seen)) = self.latest_rb() else {
            // We only vote for whichever EB we was referenced by the head of the current chain.
            return Err(NoVoteReason::WrongEB);
        };
        if rb.header.eb_announcement != Some(eb.id()) {
            // We only vote for whichever EB we was referenced by the head of the current chain.
            return Err(NoVoteReason::WrongEB);
        }
        let rb_header_must_be_received_by =
            self.sim_config.voting_window().header_deadline_at(eb.slot);
        if header_seen >= rb_header_must_be_received_by {
            // The RB header must be received more quickly
            return Err(NoVoteReason::LateRBHeader);
        }

        if self.sim_config.variant == LeiosVariant::LinearWithTxReferences {
            for tx in &eb.txs {
                if !self.has_tx(tx.id) {
                    // We won't vote for an EB if we don't have all the TXs it references
                    // NB: this should be redundant; in this variant, we wait for TXs before validating
                    return Err(NoVoteReason::MissingTX);
                }
            }
        }
        Ok(())
    }

    fn finish_generating_vote_bundle(&mut self, votes: VoteBundle, eb: &EndorserBlock) {
        // Signing may have waited behind other CPU tasks after eligibility
        // was checked. Do not publish a vote outside its voting window.
        if self.clock.now() > self.sim_config.voting_window().deadline_at(eb.slot) {
            self.tracker
                .track_no_vote(eb.slot, 0, self.id, eb.id(), NoVoteReason::LateEB);
            return;
        }
        self.tracker.track_votes_generated(&votes);
        self.count_votes(&votes);
        let id = votes.id;
        let votes = Arc::new(votes);
        self.leios
            .votes
            .insert(votes.id, VoteBundleView::Received { votes });
        self.diffuse_vote_bundle(id, None);
    }

    /// Send a vote bundle onwards to consumers.
    ///
    /// `from` is the peer it arrived from, and is skipped unless
    /// `vote-transport-echo-to-source` is set on a push transport; `None`
    /// when we produced it.  Under `announce-then-request` this sends the
    /// 8-byte id and the peer asks for the body; under the push strategies
    /// it sends the body.
    fn diffuse_vote_bundle(&mut self, id: VoteBundleId, from: Option<NodeId>) {
        let push = self.sim_config.vote_transport.is_push();
        // The echo is a push-path artefact: it models the Haskell node's
        // notify server, which pushes bodies and has no per-peer
        // provenance to exclude the sender with.  The announce path has
        // no such server and no such gap, and re-announcing to the peer
        // that just handed us the body only buys a request and a second
        // copy of a bundle we are holding.  Leaving the guard outside the
        // push branch silently made the flag change
        // `announce-then-request` -- the published baseline -- which is
        // both undocumented and a behaviour nobody asked to measure.
        let echo = push && self.sim_config.vote_transport_echo_to_source;
        let body = if push {
            match self.leios.votes.get(&id) {
                Some(VoteBundleView::Received { votes }) => Some(votes.clone()),
                // Nothing to push until the bundle is actually held.
                _ => return,
            }
        } else {
            None
        };
        // Bounded fanout: push the body to at most `k` peers rather than
        // every consumer.  Only the push path floods, so only the push
        // path is limited; the announce path sends a body when asked and
        // produces no copies to cut.
        //
        // The subset is chosen by ranking peers on a hash of
        // `(sender, bundle, peer)` under the global seed.  That is a pure
        // function of the seed, so it is identical across runs, platforms
        // and shard layouts, and independent of arrival order and of how
        // many draws this node has already made -- none of which is true
        // of drawing from a per-node stream.  Keying on the bundle means
        // each vote takes its own subgraph, so the union over a committee
        // still covers the network even though one vote does not.
        let selected: Option<BTreeSet<NodeId>> = match (&body, self.sim_config.vote_push_fanout) {
            (Some(_), Some(k)) => {
                let eligible: Vec<NodeId> = self
                    .consumers
                    .iter()
                    .copied()
                    .filter(|peer| echo || Some(*peer) != from)
                    .collect();
                if eligible.len() as u64 <= k {
                    None
                } else {
                    let rng = Rng::new(self.sim_config.seed);
                    let mut ranked: Vec<(u64, NodeId)> = eligible
                        .into_iter()
                        .map(|peer| (rng.draw_u64_with_context(&(self.id, id, peer)), peer))
                        .collect();
                    ranked.sort_unstable();
                    Some(
                        ranked
                            .into_iter()
                            .take(k as usize)
                            .map(|(_, p)| p)
                            .collect(),
                    )
                }
            }
            _ => None,
        };
        for peer in &self.consumers {
            if !echo && Some(*peer) == from {
                continue;
            }
            if selected.as_ref().is_some_and(|set| !set.contains(peer)) {
                continue;
            }
            match &body {
                Some(votes) => {
                    self.tracker.track_votes_sent(votes, self.id, *peer);
                    self.queued.send_to(*peer, Message::Votes(votes.clone()));
                }
                None => {
                    let announcement = Message::AnnounceVotes(id);
                    self.tracker.track_votes_announced(
                        id,
                        self.id,
                        *peer,
                        announcement.bytes_size(),
                    );
                    self.queued.send_to(*peer, announcement);
                }
            }
        }
    }

    fn receive_announce_votes(&mut self, from: NodeId, id: VoteBundleId) {
        self.tracker.track_votes_announcement_received(
            id,
            from,
            self.id,
            Message::AnnounceVotes(id).bytes_size(),
        );
        let should_request = match self.leios.votes.get(&id) {
            None => true,
            Some(VoteBundleView::Requested) => {
                self.sim_config.relay_strategy == RelayStrategy::RequestFromAll
            }
            _ => false,
        };
        if should_request {
            self.leios.votes.insert(id, VoteBundleView::Requested);
            let request = Message::RequestVotes(id);
            self.tracker
                .track_votes_requested(id, self.id, from, request.bytes_size());
            self.queued.send_to(from, request);
        }
    }

    fn receive_request_votes(&mut self, from: NodeId, id: VoteBundleId) {
        self.tracker.track_votes_request_received(
            id,
            from,
            self.id,
            Message::RequestVotes(id).bytes_size(),
        );
        if let Some(VoteBundleView::Received { votes }) = self.leios.votes.get(&id) {
            self.tracker.track_votes_sent(votes, self.id, from);
            self.queued.send_to(from, Message::Votes(votes.clone()));
        }
    }

    fn receive_votes(&mut self, from: NodeId, votes: Arc<VoteBundle>) {
        self.tracker.track_votes_received(&votes, from, self.id);
        // A bundle already held costs nothing beyond the bytes: real nodes test
        // the seen-set rather than verifying again.  Only the push strategies
        // can recognise a redundant body this early; the announce path has
        // already asked for the body it is being handed, so a copy it did not
        // need is only discovered after validating it (see
        // `finish_validating_vote_bundle`), and that path is left exactly as
        // it was.
        let strategy = self.sim_config.vote_transport;
        let already_have = strategy.is_push()
            && match self.leios.votes.get(&votes.id) {
                // Fully held: a duplicate under every push strategy.
                Some(VoteBundleView::Received { .. }) => true,
                // Still validating.  Only early suppression calls this a
                // duplicate; late suppression pays for the verification, as
                // the Haskell node does.
                Some(VoteBundleView::Requested) => strategy.suppresses_in_flight(),
                None => false,
            };
        if already_have {
            // Report a dropped arrival once, before scheduling any verification.
            self.tracker.track_votes_duplicate(&votes, from, self.id);
            return;
        }
        if strategy.suppresses_in_flight() {
            // Mark it in flight so copies arriving while this one validates
            // are recognised as duplicates rather than validated again.
            self.leios
                .votes
                .entry(votes.id)
                .or_insert(VoteBundleView::Requested);
        }
        self.queued
            .schedule_cpu_task(CpuTask::VTBundleValidated(from, votes));
    }

    fn finish_validating_vote_bundle(&mut self, from: NodeId, votes: Arc<VoteBundle>) {
        let id = votes.id;
        let already_held = self
            .leios
            .votes
            .insert(
                id,
                VoteBundleView::Received {
                    votes: votes.clone(),
                },
            )
            .is_some_and(|v| matches!(v, VoteBundleView::Received { .. }));
        if already_held {
            // Late deduplication and request-from-all can validate multiple
            // copies concurrently. Only the first completion is accepted.
            self.tracker.track_votes_duplicate(&votes, from, self.id);
            return;
        } else if votes
            .ebs
            .keys()
            .all(|eb| self.leios.pruned_ebs.contains(eb))
        {
            // Pruning can remove the cached copy while another validation
            // is queued. This obsolete arrival contributes no tally and
            // must not be reported as a new acceptance. Preserve its relay
            // behavior so this accounting correction does not change load.
            self.tracker.track_votes_duplicate(&votes, from, self.id);
        } else {
            self.tracker.track_votes_accepted(id, from, self.id);
            self.count_votes(&votes);
        }
        self.diffuse_vote_bundle(id, Some(from));
    }

    fn count_votes(&mut self, votes: &VoteBundle) {
        for (eb_id, count) in votes.ebs.iter() {
            if self.leios.pruned_ebs.contains(eb_id) {
                // Pruning drops this EB's tally *and* the record that its
                // quorum was already reported, so counting a late bundle
                // into a pruned EB would rebuild the tally from zero,
                // cross the threshold a second time, and report a second
                // crossing for the same (node, EB).  The summary reads
                // each crossing as one observer and weights it by that
                // node's stake, so the same node landing in the sample
                // twice skews the stake-weighted median and 95th
                // percentile the study reports.
                //
                // The tombstone is what makes "at most once per (node,
                // EB)" structural rather than merely asserted by the
                // `quorum_reached_ebs` doc comment: a pruned EB is
                // already endorsed on-chain and can never be needed
                // again, so a vote for it is not merely uncounted here,
                // it is unwanted.  Same guard, same reason, as the
                // pruned-EB checks on the header-announcement and
                // endorsement paths.
                continue;
            }
            *self
                .leios
                .votes_by_eb
                .entry(*eb_id)
                .or_default()
                .entry(votes.id.producer)
                .or_default() += count;
            let tally = self.leios.vote_weight_by_eb.entry(*eb_id).or_default();
            *tally += *count as u64;
            let weight = *tally;
            if weight >= self.sim_config.vote_threshold()
                && self.leios.quorum_reached_ebs.insert(*eb_id)
            {
                self.tracker
                    .track_eb_quorum_reached(self.id, *eb_id, weight);
            }
        }
    }
}

// Ledger/mempool operations
impl LinearLeiosNode {
    fn try_add_tx_to_mempool(&mut self, tx: &Arc<Transaction>, is_local: bool) -> InsertResult {
        self.resolve_ledger_state(self.latest_rb_id());
        if self
            .ledger_state
            .as_ref()
            .is_some_and(|(_, ls)| ls.spent_inputs.contains(&tx.input_id))
        {
            // This TX conflicts with something already on-chain
            return InsertResult::Backlogged;
        }

        let prev_local_max = self.mempool.max_local_backlog_len_seen;
        let prev_peer_max = self.mempool.max_peer_backlog_len_seen;
        let result = self.mempool.try_insert(tx.clone(), is_local);
        if self.mempool.max_local_backlog_len_seen > prev_local_max {
            self.tracker
                .track_local_backlog_max(self.id, self.mempool.max_local_backlog_len_seen);
        }
        if self.mempool.max_peer_backlog_len_seen > prev_peer_max {
            self.tracker
                .track_peer_backlog_max(self.id, self.mempool.max_peer_backlog_len_seen);
        }
        if matches!(result, InsertResult::PeerBacklogFull) {
            self.tracker
                .track_transaction_lost(tx.id, TransactionLostReason::PeerBacklogFull);
        }
        result
    }

    fn sample_from_mempool(
        &mut self,
        txs: &mut Vec<Arc<Transaction>>,
        max_size: u64,
        remove: bool,
        slot: u64,
        shuffle_call: u32,
    ) {
        let mut size = txs.iter().map(|tx| tx.bytes).sum::<u64>();
        let mut candidates: Vec<_> = self.mempool.ids().collect();
        if matches!(
            self.sim_config.mempool_strategy,
            MempoolSamplingStrategy::Random
        ) {
            let rng = Rng::new(self.sim_config.seed);
            rng.context_shuffle(&mut candidates, self.id, slot, shuffle_call);
        } else {
            candidates.reverse();
        }

        // Fill with as many pending transactions as can fit
        let mut removed_ids = vec![];
        while let Some(id) = candidates.pop() {
            let Some(TransactionView::Received { tx, .. }) = self.txs.get(&id) else {
                panic!("missing a TX in our mempool");
            };
            if size + tx.bytes > max_size {
                break;
            }
            size += tx.bytes;
            txs.push(tx.clone());
            if remove {
                removed_ids.push(tx.id);
            }
        }
        self.mempool.remove_txs(removed_ids);
        for newly_queued_tx in self.mempool.promote_from_backlogs() {
            for peer in &self.consumers {
                self.queued
                    .send_to(*peer, Message::AnnounceTx(newly_queued_tx));
            }
        }
    }

    fn remove_rb_txs_from_mempool(&mut self, rb: &RankingBlock) {
        let mut txs = rb.transactions.clone();
        if let Some(endorsement) = &rb.endorsement
            && let Some(EndorserBlockView::Received { eb, .. }) =
                self.leios.ebs.get(&endorsement.eb)
        {
            txs.extend(eb.txs.iter().cloned());
        }
        self.remove_txs_from_mempool(&txs);
    }

    fn remove_eb_txs_from_mempool(&mut self, eb: &EndorserBlock) {
        self.remove_txs_from_mempool(&eb.txs);
    }

    fn remove_txs_from_mempool(&mut self, txs: &[Arc<Transaction>]) {
        let inputs = txs.iter().map(|tx| tx.input_id).collect::<HashSet<_>>();
        self.mempool.remove_conflicting_txs(&inputs);
        for newly_queued_tx in self.mempool.promote_from_backlogs() {
            for peer in &self.consumers {
                self.queued
                    .send_to(*peer, Message::AnnounceTx(newly_queued_tx));
            }
        }
    }

    fn resolve_ledger_state(&mut self, rb_ref: Option<BlockId>) {
        let Some(block_id) = rb_ref else {
            return;
        };
        if self
            .ledger_state
            .as_ref()
            .is_some_and(|(id, _)| *id == block_id)
        {
            return;
        }

        let mut state = self.ledger_state.take().map(|(_, s)| s).unwrap_or_default();

        let mut block_queue = vec![block_id];
        while let Some(block_id) = block_queue.pop() {
            if !state.seen_blocks.insert(block_id) {
                continue;
            }
            let Some(RankingBlockView::Received { rb, .. }) = self.praos.blocks.get(&block_id)
            else {
                continue;
            };
            if let Some(parent) = rb.header.parent {
                block_queue.push(parent);
            }
            for tx in &rb.transactions {
                state.spent_inputs.insert(tx.input_id);
            }

            if let Some(endorsement) = &rb.endorsement
                && let Some(EndorserBlockView::Received { eb, .. }) =
                    self.leios.ebs.get(&endorsement.eb)
            {
                for tx in &eb.txs {
                    if self.has_tx(tx.id) {
                        state.spent_inputs.insert(tx.input_id);
                    }
                }
            }
        }

        self.ledger_state = Some((block_id, state));
    }
}

// Common utilities
impl LinearLeiosNode {
    #[allow(unused)]
    pub fn mock_lottery(&mut self, results: Arc<MockLotteryResults>) {
        self.lottery = LotteryConfig::Mock { results };
    }
    // Simulates the output of a VRF using this node's stake (if any).
    //
    // Pure function of (seed, node, slot, site): no per-node RNG state,
    // so whether this node drew 0 or 600 random values in an earlier
    // slot cannot affect this draw. Mirrors real VRF semantics (stateless
    // per key+message).
    fn run_vrf(
        &self,
        kind: LotteryKind,
        success_rate: f64,
        slot: u64,
        site: DrawSite,
    ) -> Option<u64> {
        let rng = Rng::new(self.sim_config.seed);
        self.lottery
            .run(kind, success_rate, &rng, self.id, slot, site)
    }
}

#[derive(Debug)]
enum InsertResult {
    AddedToMempool,
    Backlogged,
    PeerBacklogFull,
}

struct Mempool {
    next_id: u64,
    mempool_size_bytes: u64,
    max_size_bytes: u64,
    mempool: BTreeMap<u64, Arc<Transaction>>,
    input_ids: HashSet<u64>,
    tx_ids: HashSet<TransactionId>,
    local_backlog: VecDeque<Arc<Transaction>>,
    peer_backlog: VecDeque<Arc<Transaction>>,
    tx_generated_backlog_max_size: Option<usize>,
    tx_peer_backlog_max_size: Option<usize>,
    max_local_backlog_len_seen: usize,
    max_peer_backlog_len_seen: usize,
}
impl Mempool {
    fn new(
        max_size_bytes: u64,
        tx_generated_backlog_max_size: Option<usize>,
        tx_peer_backlog_max_size: Option<usize>,
    ) -> Self {
        Self {
            next_id: 0,
            mempool_size_bytes: 0,
            max_size_bytes,
            mempool: BTreeMap::new(),
            input_ids: HashSet::new(),
            tx_ids: HashSet::new(),
            local_backlog: VecDeque::new(),
            peer_backlog: VecDeque::new(),
            tx_generated_backlog_max_size,
            tx_peer_backlog_max_size,
            max_local_backlog_len_seen: 0,
            max_peer_backlog_len_seen: 0,
        }
    }

    fn is_generated_backlog_full(&self) -> bool {
        self.tx_generated_backlog_max_size
            .is_some_and(|max| self.local_backlog.len() >= max)
    }

    fn stats(&self) -> (usize, u64, usize, usize) {
        (
            self.mempool.len(),
            self.mempool_size_bytes,
            self.local_backlog.len(),
            self.peer_backlog.len(),
        )
    }

    fn is_peer_backlog_full(&self) -> bool {
        self.tx_peer_backlog_max_size
            .is_some_and(|max| self.peer_backlog.len() >= max)
    }

    fn try_insert(&mut self, tx: Arc<Transaction>, is_local: bool) -> InsertResult {
        let new_bytes = self.mempool_size_bytes + tx.bytes;
        if !self.mempool.is_empty() && new_bytes > self.max_size_bytes {
            // mempool would be full, add to backlog (if not capped)
            if is_local {
                self.tx_ids.insert(tx.id);
                self.local_backlog.push_back(tx);
                if self.local_backlog.len() > self.max_local_backlog_len_seen {
                    self.max_local_backlog_len_seen = self.local_backlog.len();
                }
            } else if self.is_peer_backlog_full() {
                return InsertResult::PeerBacklogFull;
            } else {
                self.tx_ids.insert(tx.id);
                self.peer_backlog.push_back(tx);
                if self.peer_backlog.len() > self.max_peer_backlog_len_seen {
                    self.max_peer_backlog_len_seen = self.peer_backlog.len();
                }
            }
            return InsertResult::Backlogged;
        }
        if self.input_ids.contains(&tx.input_id) {
            // conflicts with something already in the mempool
            return InsertResult::Backlogged;
        }

        self.mempool_size_bytes = new_bytes;
        self.input_ids.insert(tx.input_id);
        let id = self.new_id();
        self.tx_ids.insert(tx.id);
        self.mempool.insert(id, tx);
        InsertResult::AddedToMempool
    }

    fn ids(&self) -> impl Iterator<Item = TransactionId> {
        self.mempool.values().map(|tx| tx.id)
    }

    fn contains(&self, id: &TransactionId) -> bool {
        self.tx_ids.contains(id)
    }

    // Removes a set of TXs from the mempool by transaction ID.
    fn remove_txs(&mut self, ids: impl IntoIterator<Item = TransactionId>) {
        let id_set: HashSet<TransactionId> = ids.into_iter().collect();
        if id_set.is_empty() {
            return;
        }
        self.mempool.retain(|_, tx| {
            if !id_set.contains(&tx.id) {
                return true;
            }
            self.mempool_size_bytes -= tx.bytes;
            self.input_ids.remove(&tx.input_id);
            self.tx_ids.remove(&tx.id);
            false
        });
    }

    // Removes TXs from the mempool and both backlogs that conflict with the given input IDs.
    fn remove_conflicting_txs(&mut self, conflicting_inputs: &HashSet<u64>) {
        self.mempool.retain(|_, tx| {
            if !conflicting_inputs.contains(&tx.input_id) {
                return true;
            }
            self.mempool_size_bytes -= tx.bytes;
            self.input_ids.remove(&tx.input_id);
            self.tx_ids.remove(&tx.id);
            false
        });
        for backlog in [&mut self.local_backlog, &mut self.peer_backlog] {
            backlog.retain(|tx| {
                if conflicting_inputs.contains(&tx.input_id) {
                    self.tx_ids.remove(&tx.id);
                    false
                } else {
                    true
                }
            });
        }
    }

    // Promotes TXs from both backlogs into the mempool while space permits.
    // Returns the IDs of newly promoted TXs.
    fn promote_from_backlogs(&mut self) -> Vec<TransactionId> {
        let mut newly_added = vec![];
        Self::promote_backlog(
            &mut self.local_backlog,
            &mut self.mempool,
            &mut self.input_ids,
            &mut self.tx_ids,
            &mut self.mempool_size_bytes,
            self.max_size_bytes,
            &mut self.next_id,
            &mut newly_added,
        );
        Self::promote_backlog(
            &mut self.peer_backlog,
            &mut self.mempool,
            &mut self.input_ids,
            &mut self.tx_ids,
            &mut self.mempool_size_bytes,
            self.max_size_bytes,
            &mut self.next_id,
            &mut newly_added,
        );
        newly_added
    }

    #[allow(clippy::too_many_arguments)]
    fn promote_backlog(
        backlog: &mut VecDeque<Arc<Transaction>>,
        mempool: &mut BTreeMap<u64, Arc<Transaction>>,
        input_ids: &mut HashSet<u64>,
        tx_ids: &mut HashSet<TransactionId>,
        mempool_size_bytes: &mut u64,
        max_size_bytes: u64,
        next_id: &mut u64,
        newly_added: &mut Vec<TransactionId>,
    ) {
        while let Some(tx) = backlog.front() {
            if input_ids.contains(&tx.input_id) {
                let tx = backlog.pop_front().unwrap();
                tx_ids.remove(&tx.id);
                continue;
            }
            let new_size = *mempool_size_bytes + tx.bytes;
            if new_size > max_size_bytes {
                break;
            }
            let tx = backlog.pop_front().unwrap();
            *mempool_size_bytes = new_size;
            input_ids.insert(tx.input_id);
            newly_added.push(tx.id);
            let id = *next_id;
            *next_id += 1;
            mempool.insert(id, tx);
        }
    }

    fn new_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}

#[cfg(test)]
mod mempool_tests {
    use std::sync::Arc;

    use crate::model::{Transaction, TransactionId};

    use super::{InsertResult, Mempool};

    struct TxFactory {
        next_id: u64,
    }
    impl TxFactory {
        fn new() -> Self {
            Self { next_id: 0 }
        }
        fn tx(&mut self, bytes: u64) -> Arc<Transaction> {
            let id = self.next_id;
            self.next_id += 1;
            Arc::new(Transaction {
                id: TransactionId::new(id),
                shard: 0,
                bytes,
                input_id: id,
                overcollateralization_factor: 0,
            })
        }
        fn txs<const N: usize>(&mut self, bytes: [u64; N]) -> [Arc<Transaction>; N] {
            bytes.map(|b| self.tx(b))
        }
    }

    fn assert_added(result: InsertResult) {
        assert!(
            matches!(result, InsertResult::AddedToMempool),
            "expected AddedToMempool, got {result:?}"
        );
    }

    fn assert_backlogged(result: InsertResult) {
        assert!(
            matches!(result, InsertResult::Backlogged),
            "expected Backlogged, got {result:?}"
        );
    }

    #[test]
    fn should_fill_as_space_is_available() {
        let mut txs = TxFactory::new();
        let [tx1, tx2, tx3] = txs.txs([5, 5, 5]);
        let mut mempool = Mempool::new(10, None, None);
        assert_added(mempool.try_insert(tx1.clone(), true));
        assert_added(mempool.try_insert(tx2.clone(), true));

        // new TX doesn't fit
        assert_backlogged(mempool.try_insert(tx3.clone(), true));
        assert_eq!(mempool.ids().collect::<Vec<_>>(), vec![tx1.id, tx2.id]);

        // until we remove a TX, and the backlog promotes
        mempool.remove_txs([tx2.id]);
        let added = mempool.promote_from_backlogs();
        assert_eq!(added, vec![tx3.id]);
        assert_eq!(mempool.ids().collect::<Vec<_>>(), vec![tx1.id, tx3.id]);
    }

    #[test]
    fn should_track_max_backlog_len_separately() {
        let mut txs = TxFactory::new();
        let mut mempool = Mempool::new(5, None, None);
        // Fill mempool
        assert_added(mempool.try_insert(txs.tx(5), true));
        assert_eq!(mempool.max_local_backlog_len_seen, 0);
        assert_eq!(mempool.max_peer_backlog_len_seen, 0);

        // Add local to backlog
        assert_backlogged(mempool.try_insert(txs.tx(5), true));
        assert_eq!(mempool.local_backlog.len(), 1);
        assert_eq!(mempool.max_local_backlog_len_seen, 1);

        // Add peer to backlog
        assert_backlogged(mempool.try_insert(txs.tx(5), false));
        assert_eq!(mempool.peer_backlog.len(), 1);
        assert_eq!(mempool.max_peer_backlog_len_seen, 1);

        // Add another local
        assert_backlogged(mempool.try_insert(txs.tx(5), true));
        assert_eq!(mempool.local_backlog.len(), 2);
        assert_eq!(mempool.max_local_backlog_len_seen, 2);
        // Peer max unchanged
        assert_eq!(mempool.max_peer_backlog_len_seen, 1);
    }

    #[test]
    fn should_report_generated_backlog_full_only_for_local() {
        let mut txs = TxFactory::new();
        let mut mempool = Mempool::new(5, Some(2), None);
        // Fill mempool
        assert_added(mempool.try_insert(txs.tx(5), true));
        assert!(!mempool.is_generated_backlog_full());

        // Add peer txs to backlog — should NOT affect is_generated_backlog_full
        assert_backlogged(mempool.try_insert(txs.tx(5), false));
        assert_backlogged(mempool.try_insert(txs.tx(5), false));
        assert_backlogged(mempool.try_insert(txs.tx(5), false));
        assert!(!mempool.is_generated_backlog_full());

        // Add local to backlog — not yet full
        assert_backlogged(mempool.try_insert(txs.tx(5), true));
        assert!(!mempool.is_generated_backlog_full());

        // Add second local — now full
        assert_backlogged(mempool.try_insert(txs.tx(5), true));
        assert!(mempool.is_generated_backlog_full());
    }

    #[test]
    fn should_drop_peer_txs_when_peer_backlog_full() {
        let mut txs = TxFactory::new();
        let mut mempool = Mempool::new(5, None, Some(2));
        // Fill mempool
        assert_added(mempool.try_insert(txs.tx(5), true));

        // Add peer txs up to cap
        assert_backlogged(mempool.try_insert(txs.tx(5), false));
        assert_backlogged(mempool.try_insert(txs.tx(5), false));
        assert!(mempool.is_peer_backlog_full());

        // Next peer tx should be rejected
        assert!(matches!(
            mempool.try_insert(txs.tx(5), false),
            InsertResult::PeerBacklogFull
        ));
        assert_eq!(mempool.peer_backlog.len(), 2);

        // Local txs should still be accepted to their backlog
        assert_backlogged(mempool.try_insert(txs.tx(5), true));
        assert_eq!(mempool.local_backlog.len(), 1);
    }

    #[test]
    fn should_promote_from_both_backlogs() {
        let mut txs = TxFactory::new();
        let [tx1, tx2, tx3, tx4] = txs.txs([5, 5, 5, 5]);
        let mut mempool = Mempool::new(10, None, None);

        // Fill mempool
        assert_added(mempool.try_insert(tx1.clone(), true));
        assert_added(mempool.try_insert(tx2.clone(), true));

        // Add one local and one peer to backlog
        assert_backlogged(mempool.try_insert(tx3.clone(), true));
        assert_backlogged(mempool.try_insert(tx4.clone(), false));

        // Remove both mempool txs to make space for both backlog txs
        mempool.remove_txs([tx1.id, tx2.id]);
        let promoted = mempool.promote_from_backlogs();

        // Both should be promoted (local first, then peer)
        assert_eq!(promoted, vec![tx3.id, tx4.id]);
        assert_eq!(mempool.local_backlog.len(), 0);
        assert_eq!(mempool.peer_backlog.len(), 0);
    }
}
