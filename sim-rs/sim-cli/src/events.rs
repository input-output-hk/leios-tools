use std::{collections::BTreeMap, path::PathBuf, pin::Pin, time::Duration};

use aggregate::TraceAggregator;
use anyhow::Result;
use async_compression::tokio::write::GzipEncoder;
use average::Variance;
use itertools::Itertools as _;
use liveness::LivenessMonitor;
use pretty_bytes_rust::{PrettyBytesOptions, pretty_bytes};
use serde::Serialize;
use sim_core::{
    clock::Timestamp,
    config::{LeiosVariant, NodeId, SimConfiguration, VotingWindow},
    events::{BlockRef, Event, Node, VOTE_VALIDATION_TASK},
    model::{BlockId, NoVoteReason, TransactionId, TransactionLostReason},
};
use tokio::{
    fs::{self, File},
    io::{AsyncWrite, AsyncWriteExt as _, BufWriter},
    sync::mpsc,
};
use tracing::{info, info_span};

mod aggregate;
mod liveness;

type InputBlockId = sim_core::model::InputBlockId<Node>;
type EndorserBlockId = sim_core::model::EndorserBlockId<Node>;
type VoteBundleId = sim_core::model::VoteBundleId<Node>;

type TraceSink = Pin<Box<dyn AsyncWrite + Send + Sync + 'static>>;

#[derive(Clone, Serialize)]
struct OutputEvent {
    time_s: Timestamp,
    message: Event,
}

#[derive(Clone, Copy)]
enum OutputFormat {
    JsonStream,
    CborStream,
}

pub struct EventMonitor {
    variant: LeiosVariant,
    node_ids: Vec<NodeId>,
    pool_ids: Vec<NodeId>,
    /// Stake per node, indexed by node id.  The quorum-timing
    /// distribution is weighted by it, because the certificate is built by
    /// a ranking block producer and ranking block producers are drawn by
    /// stake.
    node_stake: Vec<u64>,
    /// When voting may start, when it must be over, and when a
    /// certificate may first be included -- all as offsets from the slot
    /// of the ranking block that announced the EB.
    voting_window: VotingWindow,
    maximum_ib_age: u64,
    maximum_eb_age: u64,
    vote_threshold: u64,
    vote_weight_is_stake: bool,
    events_source: LivenessMonitor,
    output_path: Option<PathBuf>,
    aggregate: bool,
}

impl EventMonitor {
    pub fn new(
        config: &SimConfiguration,
        events_source: mpsc::UnboundedReceiver<(Event, Timestamp)>,
        output_path: Option<PathBuf>,
    ) -> Self {
        let node_ids = config.nodes.iter().map(|p| p.id).collect();
        let pool_ids = config
            .nodes
            .iter()
            .filter_map(|p| if p.stake > 0 { Some(p.id) } else { None })
            .collect();
        let stage_length = config.stage_length;
        let maximum_ib_age = stage_length * 3;
        let mut node_stake = vec![0u64; config.nodes.len()];
        for node in &config.nodes {
            if let Some(stake) = node_stake.get_mut(node.id.to_inner()) {
                *stake = node.stake;
            }
        }
        Self {
            variant: config.variant,
            node_ids,
            pool_ids,
            node_stake,
            voting_window: config.voting_window(),
            maximum_ib_age,
            maximum_eb_age: config.max_eb_age,
            vote_threshold: config.vote_threshold(),
            vote_weight_is_stake: config.vote_weight_is_stake(),
            events_source: LivenessMonitor::new(config, events_source),
            output_path,
            aggregate: config.aggregate_events,
        }
    }

    // Monitor and report any events emitted by the simulation,
    // including any aggregated stats at the end.
    pub async fn run(mut self) -> Result<()> {
        let mut blocks_published: BTreeMap<NodeId, u64> = BTreeMap::new();
        let mut blocks_rejected: BTreeMap<NodeId, u64> = BTreeMap::new();
        let mut blocks: BTreeMap<u64, (NodeId, u64)> = BTreeMap::new();
        let mut txs: BTreeMap<TransactionId, Transaction> = BTreeMap::new();
        let mut ibs: BTreeMap<InputBlockId, InputBlock> = BTreeMap::new();
        let mut ebs: BTreeMap<EndorserBlockId, EndorserBlock> = BTreeMap::new();
        let mut seen_ibs: BTreeMap<NodeId, f64> = BTreeMap::new();
        let mut ibs_containing_tx: BTreeMap<TransactionId, f64> = BTreeMap::new();
        let mut ebs_containing_ib: BTreeMap<InputBlockId, f64> = BTreeMap::new();
        let mut votes_per_bundle: BTreeMap<VoteBundleId, f64> = BTreeMap::new();
        let mut votes_per_pool: BTreeMap<NodeId, f64> =
            self.pool_ids.iter().copied().map(|id| (id, 0.0)).collect();
        let mut eb_votes: BTreeMap<EndorserBlockId, f64> = BTreeMap::new();
        let mut vote_timing = VoteTiming::new(
            self.node_ids.len(),
            self.node_stake.clone(),
            self.voting_window,
        );

        let mut last_timestamp = Timestamp::zero();
        let mut total_slots = 0u64;
        let mut total_votes = 0u64;
        let mut leios_blocks_with_endorsements = 0u64;
        let mut total_leios_txs = 0u64;
        let mut total_leios_bytes = 0u64;
        let mut tx_messages = MessageStats::default();
        let mut ib_messages = MessageStats::default();
        let mut eb_messages = MessageStats::default();
        let mut vote_messages = MessageStats {
            accepted: matches!(
                self.variant,
                LeiosVariant::Linear | LeiosVariant::LinearWithTxReferences
            )
            .then_some(0),
            ..MessageStats::default()
        };
        let mut vote_wire = VoteWireStats::default();
        let mut no_vote_reasons: BTreeMap<NoVoteReason, u64> = BTreeMap::new();
        let mut txs_dropped_generated_backlog_full: u64 = 0;
        let mut txs_dropped_peer_backlog_full: u64 = 0;
        let mut max_local_backlog_len: usize = 0;
        let mut max_peer_backlog_len: usize = 0;

        // Pretty print options for bytes
        let pbo = Some(PrettyBytesOptions {
            use_1024_instead_of_1000: Some(false),
            number_of_decimal: Some(2),
            remove_zero_decimal: Some(true),
        });

        if let Some(path) = &self.output_path
            && let Some(parent) = path.parent()
        {
            fs::create_dir_all(parent).await?;
        }

        let mut output = match self.output_path.as_mut() {
            Some(path) => {
                let file = File::create(&path).await?;

                let mut gzipped = false;
                if path
                    .extension()
                    .and_then(|e| e.to_str())
                    .is_some_and(|ext| ext == "gz")
                {
                    path.set_extension("");
                    gzipped = true;
                }

                let file: TraceSink = if gzipped {
                    let encoder = GzipEncoder::new(file);
                    Box::pin(BufWriter::new(encoder))
                } else {
                    Box::pin(BufWriter::new(file))
                };

                let format = if path
                    .extension()
                    .and_then(|e| e.to_str())
                    .is_some_and(|ext| ext == "cbor")
                {
                    OutputFormat::CborStream
                } else {
                    OutputFormat::JsonStream
                };
                if self.aggregate {
                    OutputTarget::AggregatedEventStream {
                        aggregation: TraceAggregator::new(),
                        format,
                        file,
                    }
                } else {
                    OutputTarget::EventStream { format, file }
                }
            }
            None => OutputTarget::None,
        };
        // Buffer events in timestamp buckets and flush sorted once we're
        // confident all events for a given timestamp have arrived.  With
        // multi-shard CMB, shards advance at different rates so events
        // can arrive out of timestamp order.  A 1-second window is far
        // larger than any inter-shard skew (bounded by min_latency,
        // typically a few ms).
        let mut buffered: std::collections::BTreeMap<Timestamp, Vec<OutputEvent>> =
            std::collections::BTreeMap::new();
        let mut high_watermark = Timestamp::zero();
        let flush_window = Duration::from_secs(1);

        let has_output = !matches!(output, OutputTarget::None);

        while let Some((event, time)) = self.events_source.recv().await {
            last_timestamp = time;
            if has_output {
                let output_event = OutputEvent {
                    time_s: time,
                    message: event.clone(),
                };
                buffered.entry(time).or_default().push(output_event);
                if time > high_watermark {
                    high_watermark = time;
                    let cutoff = if high_watermark >= Timestamp::zero() + flush_window {
                        high_watermark - flush_window
                    } else {
                        Timestamp::zero()
                    };
                    flush_buffered(&mut buffered, cutoff, &mut output).await?;
                }
            }
            match event {
                Event::GlobalSlot { slot: number } => {
                    info!("Slot {number} has begun.");
                    total_slots = number + 1;
                    if number % 60 == 0 {
                        let buffered_events: usize = buffered.values().map(|v| v.len()).sum();
                        let (lm_txs, lm_ibs, lm_ebs, lm_queue) = self.events_source.stats();
                        info!(
                            "EventMonitor stats at slot {}:\n\
                             \x20 monitor.txs: {} entries\n\
                             \x20 monitor.ibs: {} entries\n\
                             \x20 monitor.ebs: {} entries\n\
                             \x20 monitor.votes_per_bundle: {} entries\n\
                             \x20 monitor.eb_votes: {} entries\n\
                             \x20 monitor.ibs_containing_tx: {} entries\n\
                             \x20 monitor.ebs_containing_ib: {} entries\n\
                             \x20 buffered output events: {}\n\
                             \x20 liveness.txs: {} entries\n\
                             \x20 liveness.ibs: {} entries\n\
                             \x20 liveness.ebs: {} entries\n\
                             \x20 liveness.queue: {} entries",
                            number,
                            txs.len(),
                            ibs.len(),
                            ebs.len(),
                            votes_per_bundle.len(),
                            eb_votes.len(),
                            ibs_containing_tx.len(),
                            ebs_containing_ib.len(),
                            buffered_events,
                            lm_txs,
                            lm_ibs,
                            lm_ebs,
                            lm_queue,
                        );
                        self.report_protocol_summary(
                            Some(number),
                            last_timestamp,
                            total_slots,
                            total_votes,
                            total_leios_txs,
                            total_leios_bytes,
                            leios_blocks_with_endorsements,
                            &txs,
                            &ibs,
                            &ebs,
                            &seen_ibs,
                            &ibs_containing_tx,
                            &ebs_containing_ib,
                            &votes_per_bundle,
                            &votes_per_pool,
                            &eb_votes,
                            &no_vote_reasons,
                            &tx_messages,
                            &ib_messages,
                            &eb_messages,
                            &vote_messages,
                            &vote_wire,
                            &vote_timing,
                            &pbo,
                        );
                    }
                }
                Event::Slot { .. } => {}
                Event::CpuTaskScheduled { .. } => {}
                Event::CpuTaskFinished { task_type, .. } => {
                    // Verification is counted where the work actually
                    // finishes.  Deriving it from arrivals minus duplicates
                    // is wrong for every transport that only discovers a copy
                    // is redundant after verifying it; counting it at
                    // scheduling is wrong the other way, since a task queued
                    // behind the node's core limit has cost nothing yet and
                    // need never run at all -- and the arm whose thesis is
                    // CPU cost is exactly the one that would inflate.
                    if task_type == VOTE_VALIDATION_TASK {
                        vote_messages.verifications += 1;
                    }
                }
                Event::Cpu { .. } => {}
                Event::TXGenerated { id, size_bytes, .. } => {
                    txs.insert(id, Transaction::new(size_bytes, time));
                }
                Event::TXSent { .. } => {
                    tx_messages.sent += 1;
                }
                Event::TXReceived { .. } => {
                    tx_messages.received += 1;
                }
                Event::TXLost { reason, .. } => match reason {
                    TransactionLostReason::GeneratedBacklogFull => {
                        txs_dropped_generated_backlog_full += 1;
                    }
                    TransactionLostReason::PeerBacklogFull => {
                        txs_dropped_peer_backlog_full += 1;
                    }
                    _ => {}
                },
                Event::TXLocalBacklogMax { max_len, .. } => {
                    if max_len > max_local_backlog_len {
                        max_local_backlog_len = max_len;
                    }
                }
                Event::TXPeerBacklogMax { max_len, .. } => {
                    if max_len > max_peer_backlog_len {
                        max_peer_backlog_len = max_len;
                    }
                }
                Event::RBLotteryWon { .. } => {}
                Event::RBGenerated {
                    id: BlockId { slot, producer },
                    vrf,
                    endorsement,
                    transactions,
                    ..
                } => {
                    info!(
                        "Pool {} produced a praos block in slot {slot} with {} tx(s).",
                        producer,
                        transactions.len()
                    );
                    if let Some(endorsement) = endorsement {
                        total_leios_bytes += endorsement.size_bytes;
                        leios_blocks_with_endorsements += 1;

                        let mut block_leios_txs = vec![];
                        let mut eb_queue = vec![endorsement.eb.id.clone()];
                        while let Some(eb_id) = eb_queue.pop() {
                            let eb = ebs.get_mut(&eb_id).unwrap();
                            if eb.included_in_block.is_some() {
                                continue;
                            }
                            eb.included_in_block = Some(time);

                            eb_queue.extend(eb.ebs.iter().cloned());

                            for ib_id in &eb.ibs {
                                let ib = ibs.get_mut(ib_id).unwrap();
                                if ib.included_in_block.is_none() {
                                    ib.included_in_block = Some(time);
                                }
                                for tx_id in &ib.txs {
                                    block_leios_txs.push(*tx_id);
                                    let tx = txs.get_mut(tx_id).unwrap();
                                    if tx.included_in_block.is_none() {
                                        tx.included_in_block = Some(time);
                                        tx.tx_type = Some(TransactionType::Leios);
                                    }
                                }
                            }
                            for tx_id in &eb.txs {
                                block_leios_txs.push(*tx_id);
                                let tx = txs.get_mut(tx_id).unwrap();
                                if tx.included_in_block.is_none() {
                                    tx.included_in_block = Some(time);
                                    tx.tx_type = Some(TransactionType::Leios);
                                }
                            }
                        }

                        total_leios_txs += block_leios_txs.len() as u64;
                        if matches!(
                            self.variant,
                            LeiosVariant::FullWithTxReferences | LeiosVariant::FullWithoutIbs
                        ) {
                            // In variants where transactions are referenced by Leios blocks but not embedded in IBs,
                            // referenced TXs need to be persisted separately. So count those referenced TX sizes
                            // against Leios's "space efficiency".
                            total_leios_bytes += block_leios_txs
                                .iter()
                                .map(|tx_id| txs.get(tx_id).unwrap().bytes)
                                .sum::<u64>();
                        }
                        let unique_block_leios_txs =
                            block_leios_txs.iter().copied().sorted().dedup().count();
                        info!(
                            "This block had an additional {} leios tx(s) ({} unique).",
                            block_leios_txs.len(),
                            unique_block_leios_txs,
                        );
                    }
                    for tx_id in &transactions {
                        let tx = txs.get_mut(tx_id).unwrap();
                        if tx.included_in_block.is_none() {
                            tx.included_in_block = Some(time);
                            tx.tx_type = Some(TransactionType::Praos);
                        }
                    }
                    if let Some((old_producer, old_vrf)) = blocks.get(&slot) {
                        if *old_vrf > vrf {
                            *blocks_published.entry(producer.id).or_default() += 1;
                            *blocks_published.entry(*old_producer).or_default() -= 1;
                            *blocks_rejected.entry(*old_producer).or_default() += 1;
                            blocks.insert(slot, (producer.id, vrf));
                        } else {
                            *blocks_rejected.entry(producer.id).or_default() += 1;
                        }
                    } else {
                        *blocks_published.entry(producer.id).or_default() += 1;
                        blocks.insert(slot, (producer.id, vrf));
                    }
                }
                Event::RBSent { .. } => {}
                Event::RBReceived { .. } => {}
                Event::IBLotteryWon { .. } => {}
                Event::IBGenerated {
                    id,
                    header_bytes,
                    size_bytes,
                    transactions,
                    shard,
                    ..
                } => {
                    ibs.insert(
                        id.clone(),
                        InputBlock::new(size_bytes, time, transactions.clone()),
                    );
                    total_leios_bytes += size_bytes;
                    let mut tx_bytes = header_bytes;
                    for tx_id in &transactions {
                        *ibs_containing_tx.entry(*tx_id).or_default() += 1.;
                        let tx = txs.get_mut(tx_id).unwrap();
                        tx_bytes += tx.bytes;
                        if tx.included_in_ib.is_none() {
                            tx.included_in_ib = Some(time);
                        }
                    }
                    *seen_ibs.entry(id.producer.id).or_default() += 1.;
                    info!(
                        "Pool {} generated an IB in shard {} with {} transaction(s) in slot {} ({}).",
                        id.producer,
                        shard,
                        transactions.len(),
                        id.slot,
                        pretty_bytes(tx_bytes, pbo.clone()),
                    )
                }
                Event::NoIBGenerated { .. } => {}
                Event::IBSent { .. } => {
                    ib_messages.sent += 1;
                }
                Event::IBReceived { recipient, .. } => {
                    ib_messages.received += 1;
                    *seen_ibs.entry(recipient.id).or_default() += 1.;
                }
                Event::EBLotteryWon { .. } => {}
                Event::EBGenerated {
                    id,
                    transactions,
                    input_blocks,
                    endorser_blocks,
                    size_bytes,
                    ..
                } => {
                    ebs.insert(
                        id.clone(),
                        EndorserBlock::new(
                            time,
                            transactions.iter().map(|tx| tx.id).collect(),
                            input_blocks.iter().map(|ib| ib.id.clone()).collect(),
                            endorser_blocks.iter().map(|eb| eb.id.clone()).collect(),
                        ),
                    );
                    total_leios_bytes += size_bytes;
                    for BlockRef { id: tx_id } in &transactions {
                        let tx = txs.get_mut(tx_id).unwrap();
                        if tx.included_in_eb.is_none() {
                            tx.included_in_eb = Some(time);
                        }
                    }
                    for BlockRef { id: ib_id } in &input_blocks {
                        let ib = ibs.get_mut(ib_id).unwrap();
                        if ib.included_in_eb.is_none() {
                            ib.included_in_eb = Some(time);
                        }
                        *ebs_containing_ib.entry(ib_id.clone()).or_default() += 1.0;
                        for tx_id in &ib.txs {
                            let tx = txs.get_mut(tx_id).unwrap();
                            if tx.included_in_eb.is_none() {
                                tx.included_in_eb = Some(time);
                            }
                        }
                    }
                    info!(
                        "Pool {} generated an EB with {} IB(s) and {} TX(s) in slot {}.",
                        id.producer,
                        input_blocks.len(),
                        transactions.len(),
                        id.slot,
                    )
                }
                Event::NoEBGenerated { .. } => {}
                Event::EBSent { .. } => {
                    eb_messages.sent += 1;
                }
                Event::EBReceived { .. } => {
                    eb_messages.received += 1;
                }
                Event::VTLotteryWon { .. } => {}
                Event::VTBundleGenerated { id, votes, .. } => {
                    vote_timing.bundle_generated(id.clone(), id.producer.id, time);
                    for (eb, count) in votes.0 {
                        total_votes += count as u64;
                        *votes_per_bundle.entry(id.clone()).or_default() += count as f64;
                        *eb_votes.entry(eb).or_default() += count as f64;
                        *votes_per_pool.entry(id.producer.id).or_default() += count as f64;
                    }
                }
                Event::NoVTBundleGenerated { .. } => {}
                Event::VTBundleNotGenerated { reason, .. } => {
                    *no_vote_reasons.entry(reason).or_default() += 1;
                }
                Event::VTBundleSent { msg_size_bytes, .. } => {
                    vote_messages.sent += 1;
                    vote_wire.body_bytes += msg_size_bytes;
                }
                Event::VTBundleAnnounced { msg_size_bytes, .. } => {
                    vote_wire.announcements += 1;
                    vote_wire.announcement_bytes += msg_size_bytes;
                }
                Event::VTBundleRequested { msg_size_bytes, .. } => {
                    vote_wire.requests += 1;
                    vote_wire.request_bytes += msg_size_bytes;
                }
                Event::VTBundleReceived { id, recipient, .. } => {
                    vote_messages.received += 1;
                    vote_timing.bundle_received(&id, recipient.id, time);
                }
                Event::VTBundleAccepted { .. } => {
                    *vote_messages.accepted.get_or_insert(0) += 1;
                }
                Event::VTBundleAnnouncementReceived { .. }
                | Event::VTBundleRequestReceived { .. } => {}
                Event::EBQuorumReached { id, node, .. } => {
                    vote_timing.record_quorum(id, node.id, time);
                }
                Event::VTBundleDuplicate { msg_size_bytes, .. } => {
                    // The arrival was already counted by the VTBundleReceived
                    // emitted for it; this only records that it was dropped
                    // rather than accepted, and what the bytes cost.
                    vote_messages.duplicates += 1;
                    vote_messages.duplicate_bytes += msg_size_bytes;
                }

                // CIP-0164 per-vote events (shared-consensus adapter).
                Event::VoteGenerated {
                    eb, voter, weight, ..
                } => {
                    total_votes += weight as u64;
                    *eb_votes.entry(eb.clone()).or_default() += weight as f64;
                    *votes_per_pool.entry(voter.id).or_default() += weight as f64;
                }
                Event::VoteSent { .. } => {
                    vote_messages.sent += 1;
                }
                Event::VoteReceived { .. } => {
                    vote_messages.received += 1;
                }
                Event::PartitionStarted {
                    name, link_count, ..
                } => {
                    info!("Network partition '{name}' activated: {link_count} edge(s) cut.");
                }
                Event::PartitionHealed { name, link_count } => {
                    info!("Network partition '{name}' healed: {link_count} edge(s) restored.");
                }
            }
        }

        // Flush all remaining buffered events.
        flush_buffered(&mut buffered, Timestamp::max(), &mut output).await?;

        output.flush().await?;

        let mut finalized_txs = 0;
        let mut finalized_tx_bytes = 0;
        let mut pending_txs = 0;
        let mut pending_tx_bytes = 0;
        for tx in txs.values() {
            if tx.tx_type.is_some() {
                finalized_txs += 1;
                finalized_tx_bytes += tx.bytes;
            } else {
                pending_txs += 1;
                pending_tx_bytes += tx.bytes;
            }
        }

        info_span!("praos").in_scope(|| {
            info!("{} transactions(s) were generated in total.", txs.len());
            info!("{} naive praos block(s) were published.", blocks.len());
            info!(
                "{} slot(s) had no naive praos blocks.",
                total_slots - blocks.len() as u64
            );
            info!("{} transaction(s) ({}) finalized in a naive praos block.", finalized_txs, pretty_bytes(finalized_tx_bytes, pbo.clone()));
            info!(
                "{} transaction(s) ({}) did not reach a naive praos block.",
                pending_txs,
                pretty_bytes(
                    pending_tx_bytes,
                    pbo.clone(),
                ),
            );

            for id in &self.node_ids {
                if let Some(published) = blocks_published.get(id) {
                    info!("Pool {id} published {published} naive praos block(s)");
                }
                if let Some(rejected) = blocks_rejected.get(id) {
                    info!("Pool {id} failed to publish {rejected} naive praos block(s) due to slot battles.");
                }
            }
        });

        self.report_protocol_summary(
            None,
            last_timestamp,
            total_slots,
            total_votes,
            total_leios_txs,
            total_leios_bytes,
            leios_blocks_with_endorsements,
            &txs,
            &ibs,
            &ebs,
            &seen_ibs,
            &ibs_containing_tx,
            &ebs_containing_ib,
            &votes_per_bundle,
            &votes_per_pool,
            &eb_votes,
            &no_vote_reasons,
            &tx_messages,
            &ib_messages,
            &eb_messages,
            &vote_messages,
            &vote_wire,
            &vote_timing,
            &pbo,
        );

        if max_local_backlog_len > 0 {
            info!("Maximum local tx backlog length: {max_local_backlog_len}");
        }
        if max_peer_backlog_len > 0 {
            info!("Maximum peer tx backlog length: {max_peer_backlog_len}");
        }
        if txs_dropped_generated_backlog_full > 0 {
            info!(
                "{txs_dropped_generated_backlog_full} generated transaction(s) were dropped because the generated tx backlog was full."
            );
        }
        if txs_dropped_peer_backlog_full > 0 {
            info!(
                "{txs_dropped_peer_backlog_full} peer transaction(s) were dropped because the peer tx backlog was full."
            );
        }

        Ok(())
    }

    /// Emit the leios + network protocol summary. Shared by the end-of-run
    /// report and the periodic per-slot report so the two can never drift;
    /// all figures are cumulative-so-far. Each span is emitted as a single
    /// multi-line log entry so the level/span prefix only appears on the
    /// first line, matching the EventMonitor and memory stats blocks.
    #[allow(clippy::too_many_arguments)]
    fn report_protocol_summary(
        &self,
        slot: Option<u64>,
        last_timestamp: Timestamp,
        total_slots: u64,
        total_votes: u64,
        total_leios_txs: u64,
        total_leios_bytes: u64,
        leios_blocks_with_endorsements: u64,
        txs: &BTreeMap<TransactionId, Transaction>,
        ibs: &BTreeMap<InputBlockId, InputBlock>,
        ebs: &BTreeMap<EndorserBlockId, EndorserBlock>,
        seen_ibs: &BTreeMap<NodeId, f64>,
        ibs_containing_tx: &BTreeMap<TransactionId, f64>,
        ebs_containing_ib: &BTreeMap<InputBlockId, f64>,
        votes_per_bundle: &BTreeMap<VoteBundleId, f64>,
        votes_per_pool: &BTreeMap<NodeId, f64>,
        eb_votes: &BTreeMap<EndorserBlockId, f64>,
        no_vote_reasons: &BTreeMap<NoVoteReason, u64>,
        tx_messages: &MessageStats,
        ib_messages: &MessageStats,
        eb_messages: &MessageStats,
        vote_messages: &MessageStats,
        vote_wire: &VoteWireStats,
        vote_timing: &VoteTiming,
        pbo: &Option<PrettyBytesOptions>,
    ) {
        let mut praos_txs = 0u64;
        let mut praos_tx_bytes = 0u64;
        let mut leios_txs = 0u64;
        let mut leios_tx_bytes = 0u64;
        for tx in txs.values() {
            match tx.tx_type {
                Some(TransactionType::Praos) => {
                    praos_txs += 1;
                    praos_tx_bytes += tx.bytes;
                }
                Some(TransactionType::Leios) => {
                    leios_txs += 1;
                    leios_tx_bytes += tx.bytes;
                }
                None => {}
            }
        }

        let has_ibs = self.variant.has_ibs();
        let times_to_reach_ib: Vec<_> = txs
            .values()
            .filter_map(|tx| Some(tx.included_in_ib? - tx.generated))
            .collect();
        let times_to_reach_eb: Vec<_> = txs
            .values()
            .filter_map(|tx| Some(tx.included_in_eb? - tx.generated))
            .collect();
        let times_to_reach_block: Vec<_> = txs
            .values()
            .filter_map(|tx| Some(tx.included_in_block? - tx.generated))
            .collect();
        let eb_expiration_cutoff = last_timestamp
            .checked_sub_duration(Duration::from_secs(self.maximum_eb_age))
            .unwrap_or_default();
        let expired_ebs = ebs
            .values()
            .filter(|eb| {
                eb.included_in_eb.is_none()
                    && eb.included_in_block.is_none()
                    && eb.generated < eb_expiration_cutoff
            })
            .count();
        let empty_ebs = ebs.values().filter(|eb| eb.is_empty()).count();
        let ib_expiration_cutoff = last_timestamp
            .checked_sub_duration(Duration::from_secs(self.maximum_ib_age))
            .unwrap_or_default();
        let expired_ibs = ibs
            .values()
            .filter(|ib| ib.included_in_eb.is_none() && ib.generated < ib_expiration_cutoff)
            .count();
        let bundle_count = votes_per_bundle.len();
        let txs_per_eb = compute_stats(ebs.values().map(|eb| eb.txs.len() as f64));
        let eb_time_stats = compute_stats(times_to_reach_eb.iter().map(|t| t.as_secs_f64()));
        let block_time_stats = compute_stats(times_to_reach_block.iter().map(|t| t.as_secs_f64()));
        let votes_per_pool_stats = compute_stats(votes_per_pool.values().copied());
        let uncertified_ebs = ebs
            .keys()
            .filter(|id| eb_votes.get(id).copied().unwrap_or(0.0) < self.vote_threshold as f64)
            .count();
        let votes_per_eb = compute_stats(eb_votes.values().copied());
        let votes_per_bundle_stats = compute_stats(votes_per_bundle.values().copied());

        let mut lines: Vec<String> = Vec::new();
        if has_ibs {
            let txs_per_ib = compute_stats(ibs.values().map(|ib| ib.txs.len() as f64));
            let bytes_per_ib = compute_stats(ibs.values().map(|ib| ib.bytes as f64));
            let ibs_per_tx = compute_stats(ibs_containing_tx.values().copied());
            let ibs_received = compute_stats(
                self.node_ids
                    .iter()
                    .map(|id| seen_ibs.get(id).copied().unwrap_or_default()),
            );
            lines.push(format!(
                "{} IB(s) were generated, on average {:.3} IB(s) per slot.",
                ibs.len(),
                ibs.len() as f64 / total_slots as f64
            ));
            lines.push(format!(
                "{} out of {} transaction(s) were included in at least one IB.",
                times_to_reach_ib.len(),
                txs.len(),
            ));
            lines.push(format!(
                "Each transaction was included in an average of {:.3} IB(s) (stddev {:.3}).",
                ibs_per_tx.mean, ibs_per_tx.std_dev,
            ));
            lines.push(format!(
                "Each IB contained an average of {:.3} transaction(s) (stddev {:.3}) and an average of {} (stddev {}). {} IB(s) were empty.",
                txs_per_ib.mean, txs_per_ib.std_dev,
                pretty_bytes(bytes_per_ib.mean.trunc() as u64, pbo.clone()), pretty_bytes(bytes_per_ib.std_dev.trunc() as u64, pbo.clone()),
                ibs.values().filter(|ib| ib.is_empty()).count(),
            ));
            lines.push(format!(
                "Each node received an average of {:.3} IB(s) (stddev {:.3}).",
                ibs_received.mean, ibs_received.std_dev,
            ));
        }
        let avg_age = txs.values().filter_map(|tx| {
            if tx.tx_type.is_none() {
                Some((last_timestamp - tx.generated).as_secs_f64())
            } else {
                None
            }
        });
        let avg_age_stats = compute_stats(avg_age);
        lines.push(format!(
            "The average age of the pending transactions is {:.3}s (stddev {:.3}).",
            avg_age_stats.mean, avg_age_stats.std_dev,
        ));
        lines.push(format!(
            "{} EB(s) were generated; on average there were {:.3} EB(s) per slot.",
            ebs.len(),
            ebs.len() as f64 / total_slots as f64
        ));
        lines.push(format!(
            "Each EB contained an average of {:.3} transaction(s) (stddev {:.3}). {} EB(s) were empty.",
            txs_per_eb.mean, txs_per_eb.std_dev, empty_ebs
        ));
        if has_ibs {
            let ibs_per_eb = compute_stats(ebs.values().map(|eb| eb.ibs.len() as f64));
            let ebs_per_ib = compute_stats(ebs_containing_ib.values().copied());
            lines.push(format!(
                "Each EB contained an average of {:.3} IB(s) (stddev {:.3}). {} EB(s) were empty.",
                ibs_per_eb.mean, ibs_per_eb.std_dev, empty_ebs
            ));
            lines.push(format!(
                "Each IB was included in an average of {:.3} EB(s) (stddev {:.3}).",
                ebs_per_ib.mean, ebs_per_ib.std_dev,
            ));
            lines.push(format!(
                "{} out of {} IBs were included in at least one EB.",
                ibs.values()
                    .filter(|ib| ib.included_in_eb.is_some())
                    .count(),
                ibs.len(),
            ));
            lines.push(format!(
                "{} out of {} IBs expired before they reached an EB.",
                expired_ibs,
                ibs.len(),
            ));
        }
        lines.push(format!(
            "{} out of {} EBs expired before an EB from their stage reached an RB.",
            expired_ebs,
            ebs.len(),
        ));
        lines.push(format!(
            "{} out of {} transaction(s) were included in at least one EB.",
            times_to_reach_eb.len(),
            txs.len(),
        ));
        let vote_unit = if self.vote_weight_is_stake {
            "stake weight"
        } else {
            "vote(s)"
        };
        // Linear stake-weighted committees sign one body per voter and EB;
        // a pool's stake is certificate weight, not a count of signatures.
        let vote_count = if self.vote_weight_is_stake {
            bundle_count as u64
        } else {
            total_votes
        };
        lines.push(format!("{vote_count} total votes were generated."));
        if self.vote_weight_is_stake {
            lines.push(format!("Total generated voting weight: {total_votes}; quorum requires {} of total active stake per EB.", self.vote_threshold));
        }
        lines.push(format!(
            "Each stake pool produced an average of {:.3} {vote_unit} (stddev {:.3}).",
            votes_per_pool_stats.mean, votes_per_pool_stats.std_dev
        ));
        lines.push(format!(
            "Each EB received an average of {:.3} {vote_unit} (stddev {:.3}).",
            votes_per_eb.mean, votes_per_eb.std_dev
        ));
        lines.push(format!("There were {bundle_count} bundle(s) of votes. Each bundle contained {:.3} {vote_unit} (stddev {:.3}).",
            votes_per_bundle_stats.mean, votes_per_bundle_stats.std_dev));
        if !no_vote_reasons.is_empty() {
            let total: u64 = no_vote_reasons.values().sum();
            lines.push(format!(
                "{total} vote(s) were not generated due to validation failures:"
            ));
            for (reason, count) in no_vote_reasons {
                lines.push(format!("  {reason:?}: {count}"));
            }
        }
        if uncertified_ebs > 0 {
            lines.push(format!(
                "{uncertified_ebs} out of {} EB(s) did not reach the vote threshold ({}).",
                ebs.len(),
                self.vote_threshold
            ));
        }
        // Whether a quorum forms inside the voting period is the question the
        // vote transports are being compared on, so the summary reports, per
        // EB, when a node's own tally crossed the threshold -- validation
        // included, since that is what a node has to do before a vote counts.
        //
        // Two things about that measurement are worth stating outright,
        // because both were wrong before and both flatter the protocol when
        // they are:
        //
        //  * It is measured from t0, the start of the slot of the ranking
        //    block that announced the EB, which is the only anchor CIP-0164
        //    gives -- the EB carries no slot of its own.  Measuring from the
        //    EB being generated instead starts the clock after the producer's
        //    EB-assembly CPU time, which grows with the EB, and hides most of
        //    a fixed 3 * L_hdr wait inside a number that is then compared
        //    against L_vote alone.
        //  * It is reported per observer rather than as a minimum over nodes.
        //    A minimum is the luckiest node in the network; the node that
        //    matters is the ranking block producer that builds the
        //    certificate, and that pool is drawn by stake.
        //
        // Where CIP-0164 is ambiguous the stricter reading is taken, i.e. the
        // one that makes the protocol look worse: the votes must have
        // *arrived* by the deadline, not merely have been cast by it.  The
        // looser reading -- votes may still be in flight until a certificate
        // could first be included -- is reported next to it rather than
        // instead of it.  The deadlines are used as wall-clock durations, as
        // the specification says to; at a one second slot length the "slots"
        // wording elsewhere in it comes to the same thing.
        //
        // Only the linear variants report a node-side quorum crossing, so only
        // they can say how many EBs got one.  The counts are printed whenever
        // there were EBs at all, including when the answer is none: "no EB ever
        // gathered a quorum" is the result this study exists to detect, and a
        // line that disappears in that case reads as success.
        let reports_quorum = matches!(
            self.variant,
            LeiosVariant::Linear | LeiosVariant::LinearWithTxReferences
        );
        if reports_quorum && !ebs.is_empty() {
            let window = self.voting_window;
            let gate_s = window.gate.as_secs_f64();
            let deadline_s = window.deadline.as_secs_f64();
            let inclusion_s = window.inclusion_deadline.as_secs_f64();
            lines.push(format!(
                "Quorum timing runs from t0, the start of the slot of the ranking block that announced the EB: voting opens at the gate t0+3*L_hdr = {gate_s:.3}s, the votes are due by t0+3*L_hdr+L_vote = {deadline_s:.3}s, and a certificate for the EB cannot be included before t0+3*L_hdr+L_vote+L_diff = {inclusion_s:.3}s."
            ));
            for (label, quantile) in QUORUM_OBSERVERS {
                let outcome = vote_timing.quorum_outcome(ebs, *quantile);
                let mut line = format!(
                    "Quorum at {label}: {} of {} EB(s) reached one, {} of them by the {deadline_s:.3}s deadline and {} by the {inclusion_s:.3}s inclusion deadline.",
                    outcome.reached(),
                    ebs.len(),
                    outcome.in_window,
                    outcome.by_inclusion,
                );
                if let (Some(dist), Some(diffusion), Some((margin_mean, margin_worst))) = (
                    outcome.dist.as_ref(),
                    outcome.diffusion_mean_s,
                    outcome.margin_s(window),
                ) {
                    // The diffusion figure is a mean of per-EB parts, not a
                    // slice off the mean, so it is worded as its own average
                    // rather than as a decomposition of the one before it:
                    // the two do not subtract whenever any EB reached a
                    // quorum before the gate, which happens because a
                    // producer votes for its own block without waiting.
                    line.push_str(&format!(
                        " Average {:.3}s from t0 (median {:.3}, p95 {:.3}, max {:.3}); the part of each EB's wait that fell after the gate averaged {diffusion:.3}s; margin against the deadline averaged {margin_mean:+.3}s and was {margin_worst:+.3}s at worst.",
                        dist.mean, dist.median, dist.p95, dist.max,
                    ));
                }
                lines.push(line);
            }
        }
        if let Some(arrivals) = vote_timing.arrival_delays() {
            lines.push(format!(
                "Each of {} vote arrival(s) took an average of {:.3}s (median {:.3}, p95 {:.3}, max {:.3}) to get from the voter to a node.",
                arrivals.count, arrivals.mean, arrivals.median, arrivals.p95, arrivals.max,
            ));
        }
        if let Some(line) = vote_timing.coverage_line() {
            lines.push(line);
        }
        lines.push(format!(
            "{} L1 block(s) had a Leios endorsement.",
            leios_blocks_with_endorsements
        ));
        lines.push(format!(
            "{} tx(s) ({}) were referenced by a Leios endorsement.",
            leios_txs,
            pretty_bytes(leios_tx_bytes, pbo.clone())
        ));
        lines.push(format!(
            "{} tx(s) ({}) were included directly in a Praos block.",
            praos_txs,
            pretty_bytes(praos_tx_bytes, pbo.clone())
        ));
        lines.push(format!(
            "Spatial efficiency: {}/{} ({:.3}%) of Leios bytes were unique transactions.",
            pretty_bytes(leios_tx_bytes, pbo.clone()),
            pretty_bytes(total_leios_bytes, pbo.clone()),
            (leios_tx_bytes as f64 / total_leios_bytes as f64) * 100.
        ));
        lines.push(format!(
            "{} tx(s) ({:.3}%) referenced by a Leios endorsement were redundant.",
            total_leios_txs - leios_txs,
            (total_leios_txs - leios_txs) as f64 / total_leios_txs as f64 * 100.
        ));
        if has_ibs {
            let ib_time_stats = compute_stats(times_to_reach_ib.iter().map(|t| t.as_secs_f64()));
            lines.push(format!(
                "Each transaction took an average of {:.3}s (stddev {:.3}) to be included in an IB.",
                ib_time_stats.mean, ib_time_stats.std_dev,
            ));
        }
        lines.push(format!(
            "Each transaction took an average of {:.3}s (stddev {:.3}) to be included in an EB.",
            eb_time_stats.mean, eb_time_stats.std_dev,
        ));
        lines.push(format!(
            "Each transaction took an average of {:.3}s (stddev {:.3}) to be included in a block.",
            block_time_stats.mean, block_time_stats.std_dev,
        ));
        let (protocol_header, network_header) = match slot {
            Some(n) => (
                format!("Protocol stats at slot {n}:"),
                format!("Network stats at slot {n}:"),
            ),
            None => (
                "Final protocol stats:".to_string(),
                "Final network stats:".to_string(),
            ),
        };
        info_span!("leios").in_scope(|| info!("{}\n  {}", protocol_header, lines.join("\n  ")));

        let mut lines = vec![tx_messages.summary_line("TX")];
        if has_ibs {
            lines.push(ib_messages.summary_line("IB"));
        }
        lines.push(eb_messages.summary_line("EB"));
        // "Vote body", not "Vote": the count is bodies and always was, and
        // the announcements and requests on the same mini-protocol are on
        // the line below rather than folded into it.
        lines.push(vote_messages.summary_line("Vote body"));
        // Complete control-message accounting is implemented for Linear only.
        if matches!(
            self.variant,
            LeiosVariant::Linear | LeiosVariant::LinearWithTxReferences
        ) && !vote_wire.is_empty()
        {
            lines.push(vote_wire.summary_line(vote_messages.sent));
        }
        info_span!("network").in_scope(|| info!("{}\n  {}", network_header, lines.join("\n  ")));
    }
}

#[derive(Clone)]
struct Transaction {
    bytes: u64,
    generated: Timestamp,
    included_in_ib: Option<Timestamp>,
    included_in_eb: Option<Timestamp>,
    included_in_block: Option<Timestamp>,
    tx_type: Option<TransactionType>,
}
impl Transaction {
    fn new(bytes: u64, generated: Timestamp) -> Self {
        Self {
            bytes,
            generated,
            included_in_ib: None,
            included_in_eb: None,
            included_in_block: None,
            tx_type: None,
        }
    }
}

#[derive(Clone, Copy)]
enum TransactionType {
    Leios,
    Praos,
}

struct InputBlock {
    bytes: u64,
    generated: Timestamp,
    txs: Vec<TransactionId>,
    included_in_eb: Option<Timestamp>,
    included_in_block: Option<Timestamp>,
}
impl InputBlock {
    fn new(bytes: u64, generated: Timestamp, txs: Vec<TransactionId>) -> Self {
        Self {
            bytes,
            generated,
            txs,
            included_in_eb: None,
            included_in_block: None,
        }
    }
    fn is_empty(&self) -> bool {
        self.txs.is_empty()
    }
}
struct EndorserBlock {
    generated: Timestamp,
    txs: Vec<TransactionId>,
    ibs: Vec<InputBlockId>,
    ebs: Vec<EndorserBlockId>,
    included_in_eb: Option<Timestamp>,
    included_in_block: Option<Timestamp>,
}
impl EndorserBlock {
    fn new(
        generated: Timestamp,
        txs: Vec<TransactionId>,
        ibs: Vec<InputBlockId>,
        ebs: Vec<EndorserBlockId>,
    ) -> Self {
        Self {
            generated,
            txs,
            ibs,
            ebs,
            included_in_eb: None,
            included_in_block: None,
        }
    }
    fn is_empty(&self) -> bool {
        self.txs.is_empty() && self.ibs.is_empty() && self.ebs.is_empty()
    }
}

#[derive(Default)]
struct MessageStats {
    sent: u64,
    received: u64,
    /// Arrivals the recipient did not need: it already held the item, or it
    /// finished verifying a copy of one it had already accepted, or the
    /// votes refer only to pruned EBs. The push transports produce redundant
    /// copies by design; so does the
    /// announce-then-request path under `relay-strategy: request-from-all`,
    /// which asks every peer that announces a bundle and then throws the
    /// later answers away.
    duplicates: u64,
    duplicate_bytes: u64,
    /// Completed first acceptance, available for Linear bundle transports.
    /// None means the protocol does not emit acceptance events.
    accepted: Option<u64>,
    /// Verifications of arrivals of this item that actually ran to
    /// completion.  Counted off the CPU task finishing, rather than derived
    /// as `received - duplicates` or taken off the task being scheduled.
    /// Deriving it hides the difference the study is about, since a
    /// transport that only recognises a redundant copy after verifying it
    /// does strictly more work for the same delivery; counting the
    /// scheduling reports work that has not been done, because a task can
    /// sit behind the node's core limit and never run.
    verifications: u64,
}
impl MessageStats {
    fn summary_line(&self, name: &str) -> String {
        let percent_received = if self.sent == 0 {
            0.0
        } else {
            self.received as f64 / self.sent as f64 * 100.0
        };
        let mut line = format!(
            "{} {} message(s) were sent. {} of them were received ({:.3}%).",
            self.sent, name, self.received, percent_received
        );
        if self.accepted.is_some() || self.duplicates > 0 || self.verifications > 0 {
            let percent_duplicate = if self.received == 0 {
                0.0
            } else {
                self.duplicates as f64 / self.received as f64 * 100.0
            };
            line.push_str(&format!(
                " {} of those ({:.3}%) were redundant or obsolete arrivals, costing {:.2} MB; ",
                self.duplicates,
                percent_duplicate,
                self.duplicate_bytes as f64 / 1e6,
            ));
            if let Some(accepted) = self.accepted {
                // An arrival remains pending until accepted or classified as
                // a duplicate. Verify-first duplicates may still be queued.
                let pending = self.received.saturating_sub(self.duplicates + accepted);
                line.push_str(&format!("{accepted} accepted; {pending} pending; "));
            }
            line.push_str(&format!("{} verification(s) completed", self.verifications));
            if let Some(accepted) = self.accepted.filter(|n| *n > 0) {
                line.push_str(&format!(
                    " ({:.2} per accepted {})",
                    self.verifications as f64 / accepted as f64,
                    name
                ));
            }
            line.push('.');
        }
        line
    }
}

/// What one vote transport spends on the wire to deliver the same votes.
///
/// The bodies are the `Vote` counts in `MessageStats`; these are the bytes
/// behind them, plus the announcement and request messages that the Vote
/// mini-protocol charges 8 bytes each for and that no counter used to see.
/// Leaving them out flattered `announce-then-request`, which sends an
/// announcement per link and a request per body on top of the bodies
/// themselves, and so sends the most messages of any arm while appearing to
/// send the fewest.
#[derive(Default)]
struct VoteWireStats {
    /// Bytes of vote bodies sent.
    body_bytes: u64,
    announcements: u64,
    announcement_bytes: u64,
    requests: u64,
    request_bytes: u64,
}
impl VoteWireStats {
    /// True when nothing at all went out on the vote mini-protocol.  The
    /// variants that do not use the bundle path never emit these events,
    /// and a line of zeroes there would say nothing.
    fn is_empty(&self) -> bool {
        self.body_bytes == 0 && self.announcements == 0 && self.requests == 0
    }

    /// `bodies` is the body count from `MessageStats`, kept as its own
    /// figure rather than folded into the total, so the number quoted
    /// before this line existed is still on the page and still means the
    /// same thing.
    fn summary_line(&self, bodies: u64) -> String {
        let total = bodies + self.announcements + self.requests;
        let total_bytes = self.body_bytes + self.announcement_bytes + self.request_bytes;
        format!(
            "Vote mini-protocol traffic sent: {total} message(s), {:.2} MB \
             = {bodies} bod{} ({:.2} MB) + {} announcement(s) ({:.2} MB) + {} request(s) ({:.2} MB).",
            total_bytes as f64 / 1e6,
            if bodies == 1 { "y" } else { "ies" },
            self.body_bytes as f64 / 1e6,
            self.announcements,
            self.announcement_bytes as f64 / 1e6,
            self.requests,
            self.request_bytes as f64 / 1e6,
        )
    }
}

/// A node's tally for one endorser block crossing the quorum threshold:
/// when it happened, as a delay from that EB's `t0`, and the weight the
/// node carries in the distribution.
#[derive(Clone, Copy, Debug)]
struct QuorumSample {
    delay_us: u64,
    weight: u64,
}

/// What one observer saw of the run's endorser blocks.
struct QuorumOutcome {
    /// EBs where the observer held a quorum by the voting deadline.
    in_window: usize,
    /// EBs where it held one by the certificate-inclusion deadline.
    by_inclusion: usize,
    /// Delay from `t0` to the quorum, in seconds, over the EBs where the
    /// observer ever got one.
    dist: Option<DistStats>,
    /// Mean over those same EBs of the part of each EB's wait that fell
    /// after the gate, each floored at zero before the mean is taken.
    ///
    /// The flooring has to happen per EB and not on the mean.  A block's
    /// producer votes for its own block without waiting out its
    /// equivocation-detection period, so an EB really can reach a quorum
    /// before the gate, and those EBs contribute zero diffusion rather than
    /// a negative amount that cancels the diffusion of the EBs that did
    /// wait.  Flooring the mean instead reported 0.000s of diffusion for
    /// per-EB delays of 2.0s and 4.0s against a 3.0s gate, where the answer
    /// is 0.5s.
    diffusion_mean_s: Option<f64>,
}

impl QuorumOutcome {
    /// EBs where the observer held a quorum at any point in the run.
    fn reached(&self) -> usize {
        self.dist.as_ref().map_or(0, |d| d.count)
    }

    /// Seconds of slack against the voting deadline, on average and at
    /// worst.
    ///
    /// Negative means the quorum turned up after the deadline had already
    /// passed.  Both figures are linear in the per-EB delays -- the mean
    /// margin is the margin of the mean, and the worst margin is the one
    /// against the slowest EB -- so unlike the diffusion split they are the
    /// same computed either way.
    fn margin_s(&self, window: VotingWindow) -> Option<(f64, f64)> {
        let dist = self.dist.as_ref()?;
        let deadline = window.deadline.as_secs_f64();
        Some((deadline - dist.mean, deadline - dist.max))
    }
}

/// Whose view of the quorum the summary reports, as a share of the
/// network's weight, with the label that share is printed under.
///
/// The certificate is built by a ranking block producer -- "only RB
/// producers create certificates when they are about to produce a new
/// ranking block" -- and which pool that is comes out of a stake-weighted
/// lottery, so the quantiles are over stake rather than over nodes.  The
/// first entry is the old minimum-over-nodes figure, kept because it is
/// what earlier runs reported, and labelled so nobody mistakes it for the
/// certifier's view again.
const QUORUM_OBSERVERS: &[(&str, f64)] = &[
    (
        "the first node anywhere (the luckiest node, not the one that certifies)",
        0.0,
    ),
    (
        "the stake-weighted median node (an even chance the block producer that certifies had it)",
        0.5,
    ),
    (
        "the 95th-percentile node by stake (all but the slowest twentieth of the stake)",
        0.95,
    ),
];

/// Vote timing for the vote-transport study: how long an EB takes to gather
/// a quorum, and how long a vote takes to get anywhere.
struct VoteTiming {
    node_count: usize,
    /// Distinct nodes a vote has to reach to count as having reached 95%.
    coverage_target: usize,
    /// The voting window every EB gets, as offsets from `t0`.
    window: VotingWindow,
    /// Weight each node carries in the quorum-timing distribution: its
    /// stake, since a certificate is built by a stake-drawn ranking block
    /// producer.  All ones when no node holds stake, so a stakeless test
    /// topology degrades to a plain distribution over nodes instead of
    /// dividing by zero.
    node_weights: Vec<u64>,
    total_weight: u64,
    /// Per EB, one sample per node whose own tally crossed the quorum
    /// threshold.  The simulator reports each crossing once per (node,
    /// EB), so a node appears at most once here; the vector is one
    /// `(delay, weight)` pair per node, i.e. 24 KB per EB at 1500 nodes.
    quorums: BTreeMap<EndorserBlockId, Vec<QuorumSample>>,
    bundles: BTreeMap<VoteBundleId, BundleDiffusion>,
    /// Generation-to-arrival delay of every vote arrival, duplicates
    /// included: those are arrivals the transport paid for too.
    arrivals: DelayHistogram,
}

impl VoteTiming {
    fn new(node_count: usize, node_weights: Vec<u64>, window: VotingWindow) -> Self {
        let staked: u64 = node_weights.iter().sum();
        let (node_weights, total_weight) = if staked == 0 {
            (vec![1; node_count], node_count as u64)
        } else {
            (node_weights, staked)
        };
        Self {
            node_count,
            coverage_target: (node_count as f64 * 0.95).ceil() as usize,
            window,
            node_weights,
            total_weight,
            quorums: BTreeMap::new(),
            bundles: BTreeMap::new(),
            arrivals: DelayHistogram::default(),
        }
    }

    fn bundle_generated(&mut self, id: VoteBundleId, producer: NodeId, generated: Timestamp) {
        let mut bundle = BundleDiffusion::new(generated, self.node_count);
        // Its producer holds a bundle from the moment the bundle exists.
        bundle.record(
            producer,
            Duration::ZERO,
            self.node_count,
            self.coverage_target,
        );
        self.bundles.insert(id, bundle);
    }

    fn bundle_received(&mut self, id: &VoteBundleId, recipient: NodeId, arrived: Timestamp) {
        let (node_count, coverage_target) = (self.node_count, self.coverage_target);
        let Some(bundle) = self.bundles.get_mut(id) else {
            return;
        };
        let delay = elapsed(bundle.generated, arrived);
        bundle.record(recipient, delay, node_count, coverage_target);
        self.arrivals.record(delay);
    }

    /// One node's own tally for `eb` reached the quorum threshold at
    /// `reached`.
    ///
    /// The delay is taken from `t0`, the start of the slot of the ranking
    /// block that announced the EB, and *not* from the EB being generated.
    /// The two differ by the producer's EB-assembly CPU time, which grows
    /// with the EB, so measuring from generation makes a bigger EB look
    /// like it certifies sooner.  `t0` is also what every deadline in
    /// CIP-0164 is stated against.
    fn record_quorum(&mut self, eb: EndorserBlockId, node: NodeId, reached: Timestamp) {
        let delay = elapsed(VotingWindow::anchor(eb.slot), reached);
        let weight = self
            .node_weights
            .get(node.to_inner())
            .copied()
            .unwrap_or_default();
        self.quorums.entry(eb).or_default().push(QuorumSample {
            delay_us: delay.as_micros().min(u64::MAX as u128) as u64,
            weight,
        });
    }

    /// What the run looked like to the node at `quantile` of the
    /// network's weight: for each EB, the delay from `t0` by which nodes
    /// holding that share of the weight had a quorum.  EBs where that much
    /// weight never got one are absent from the distribution and are not
    /// counted as in the window.
    fn quorum_outcome(
        &self,
        ebs: &BTreeMap<EndorserBlockId, EndorserBlock>,
        quantile: f64,
    ) -> QuorumOutcome {
        let mut delays = Vec::new();
        let mut diffusions = Vec::new();
        let mut in_window = 0;
        let mut by_inclusion = 0;
        let gate_s = self.window.gate.as_secs_f64();
        for (id, samples) in &self.quorums {
            if !ebs.contains_key(id) {
                continue;
            }
            let mut samples = samples.clone();
            // Sorting on the delay alone is enough for a deterministic
            // answer: tied samples carry the same delay, and the delay is
            // the only thing read back out, so the order the shards
            // reported the crossings in cannot change the result.
            samples.sort_unstable_by_key(|s| s.delay_us);
            let Some(delay) = weighted_quantile(&samples, self.total_weight, quantile) else {
                continue;
            };
            // The deadline is treated as one the votes have to have
            // *arrived* by, not merely to have been cast by.  CIP-0164 is
            // ambiguous here -- §Step 4 collects the quorum "during the
            // voting period" while §Vote Propagation gives votes the
            // diffusion period as well -- and this is the stricter of the
            // two readings, so it is the one that makes the protocol look
            // worse.  The looser reading is reported alongside it as the
            // count against the inclusion deadline.
            if delay <= self.window.deadline {
                in_window += 1;
            }
            if delay <= self.window.inclusion_deadline {
                by_inclusion += 1;
            }
            delays.push(delay.as_secs_f64());
            // Per EB, and floored here rather than after averaging: see
            // `QuorumOutcome::diffusion_mean_s`.
            diffusions.push((delay.as_secs_f64() - gate_s).max(0.0));
        }
        // Summed in EB order over a `BTreeMap`, so the floating-point result
        // does not depend on the order the shards reported the crossings in.
        let diffusion_mean_s = if diffusions.is_empty() {
            None
        } else {
            Some(diffusions.iter().sum::<f64>() / diffusions.len() as f64)
        };
        QuorumOutcome {
            in_window,
            by_inclusion,
            dist: compute_dist(delays),
            diffusion_mean_s,
        }
    }

    /// Seconds each vote took to reach 95% of the nodes.  Votes that never
    /// got that far are absent.
    fn coverage_delays(&self) -> impl Iterator<Item = f64> + '_ {
        self.bundles
            .values()
            .filter_map(|b| b.coverage_delay_s(self.coverage_target))
    }

    /// How many bundles reached 95% of the nodes, how long that took, and
    /// how many never got there.  `None` only when the run produced no vote
    /// bundles at all.
    ///
    /// The counts sit outside the distribution deliberately.  With them
    /// inside it, a run where no bundle ever reached 95% of the nodes -- a
    /// starved or partitioned one, the failure this study exists to
    /// detect -- printed no line at all, and a missing line reads as
    /// success.  It is the same reasoning as the quorum counts, which are
    /// printed whenever there were EBs at all.
    fn coverage_line(&self) -> Option<String> {
        if self.bundles.is_empty() {
            return None;
        }
        let coverage = compute_dist(self.coverage_delays());
        let reached = coverage.as_ref().map_or(0, |c| c.count);
        let mut line = format!(
            "{reached} out of {} vote bundle(s) reached 95% of nodes ({} of {}); {} never did.",
            self.bundles.len(),
            self.coverage_target,
            self.node_count,
            self.bundles.len() - reached,
        );
        if let Some(coverage) = coverage {
            line.push_str(&format!(
                " Those took an average of {:.3}s (median {:.3}, p95 {:.3}, max {:.3}) to get there.",
                coverage.mean, coverage.median, coverage.p95, coverage.max,
            ));
        }
        Some(line)
    }

    fn arrival_delays(&self) -> Option<DistStats> {
        self.arrivals.stats()
    }
}

/// How one vote bundle spread.
///
/// A percentile needs the samples, so while a bundle is still spreading this
/// holds one first-arrival delay per node that has it, plus a
/// one-bit-per-node arrival set: 6.2 KB at 1500 nodes.  Once every node has
/// it the samples collapse to the single 95%-coverage figure and both
/// buffers are freed, so what is held tracks the bundles still in flight
/// (one EB's worth, a few thousand, so tens of MB) rather than every bundle
/// of the run.  The bound to watch is a bundle that never reaches every
/// node -- a partitioned node, or the end of the run arriving first -- which
/// keeps its 6.2 KB until the run ends.
struct BundleDiffusion {
    generated: Timestamp,
    /// First-arrival delay in microseconds, one entry per node holding it.
    delays: Vec<u32>,
    /// Bit set of the nodes already counted in `delays`.
    seen: Vec<u64>,
    /// Delay by which the bundle had reached 95% of nodes, in microseconds.
    coverage_us: Option<u32>,
}

impl BundleDiffusion {
    fn new(generated: Timestamp, node_count: usize) -> Self {
        Self {
            generated,
            delays: Vec::new(),
            seen: vec![0; node_count.div_ceil(64)],
            coverage_us: None,
        }
    }

    fn record(&mut self, node: NodeId, delay: Duration, node_count: usize, coverage_target: usize) {
        let index = node.to_inner();
        let (word, bit) = (index / 64, 1u64 << (index % 64));
        // An empty set means every node already had it and the samples have
        // been reduced; later arrivals are duplicates and add nothing here.
        let Some(seen) = self.seen.get_mut(word) else {
            return;
        };
        if *seen & bit != 0 {
            return;
        }
        *seen |= bit;
        self.delays
            .push(delay.as_micros().min(u32::MAX as u128) as u32);
        if self.delays.len() >= node_count {
            self.coverage_us = nth_smallest(&mut self.delays, coverage_target);
            self.delays = Vec::new();
            self.seen = Vec::new();
        }
    }

    fn coverage_delay_s(&self, coverage_target: usize) -> Option<f64> {
        if let Some(us) = self.coverage_us {
            return Some(us as f64 / 1e6);
        }
        // Still spreading: read the same figure off the arrivals so far, if
        // enough of them have happened.
        let mut delays = self.delays.clone();
        nth_smallest(&mut delays, coverage_target).map(|us| us as f64 / 1e6)
    }
}

/// The delay by which nodes carrying `quantile` of the network's weight
/// held a quorum, or `None` if that much weight never did.  `samples` must
/// be sorted by delay.
///
/// A `quantile` of zero returns the first sample there is, whatever weight
/// its node carries: that is the "first node anywhere" figure, and a relay
/// with no stake getting there first still counts as one.
fn weighted_quantile(
    samples: &[QuorumSample],
    total_weight: u64,
    quantile: f64,
) -> Option<Duration> {
    let target = quantile * total_weight as f64;
    let mut cumulative = 0u64;
    for sample in samples {
        cumulative += sample.weight;
        if cumulative as f64 >= target {
            return Some(Duration::from_micros(sample.delay_us));
        }
    }
    None
}

/// The `k`th smallest sample, 1-based, or `None` if there are fewer than
/// `k` of them.
fn nth_smallest(samples: &mut [u32], k: usize) -> Option<u32> {
    if k == 0 || samples.len() < k {
        return None;
    }
    let (_, nth, _) = samples.select_nth_unstable(k - 1);
    Some(*nth)
}

/// One millisecond per bucket, up to two minutes.
const DELAY_BUCKET_COUNT: usize = 120_000;

/// A bounded distribution of delays.
///
/// A 1500-node run produces tens of millions of vote arrivals, far too many
/// to keep a sample each just to read a percentile off them, so arrivals are
/// counted into a fixed histogram of 1 ms buckets covering [0s, 120s):
/// 120_000 u64 buckets, 960 KB, whatever the length of the run.  Anything
/// slower lands in the last bucket, which only matters if a vote takes two
/// minutes to arrive -- thirty times the voting period the study is about.
/// Count, sum and max are exact, so the mean and the max are neither
/// quantised nor dependent on the order the shards hand their events over;
/// the median and 95th percentile are exact to the 1 ms bucket.
struct DelayHistogram {
    buckets: Box<[u64]>,
    count: u64,
    total_us: u128,
    max_us: u64,
}

impl Default for DelayHistogram {
    fn default() -> Self {
        Self {
            buckets: vec![0; DELAY_BUCKET_COUNT].into_boxed_slice(),
            count: 0,
            total_us: 0,
            max_us: 0,
        }
    }
}

impl DelayHistogram {
    fn record(&mut self, delay: Duration) {
        let us = delay.as_micros().min(u64::MAX as u128) as u64;
        self.count += 1;
        self.total_us += us as u128;
        self.max_us = self.max_us.max(us);
        let bucket = ((us / 1000) as usize).min(DELAY_BUCKET_COUNT - 1);
        self.buckets[bucket] += 1;
    }

    /// Nearest-rank percentile, reported at the middle of the bucket it
    /// falls in.
    fn percentile_s(&self, percentile: f64) -> f64 {
        let rank = (percentile * self.count as f64).ceil().max(1.0) as u64;
        let mut seen = 0u64;
        for (bucket, count) in self.buckets.iter().enumerate() {
            seen += count;
            if seen >= rank {
                return (bucket as f64 + 0.5) / 1000.0;
            }
        }
        self.max_us as f64 / 1e6
    }

    fn stats(&self) -> Option<DistStats> {
        if self.count == 0 {
            return None;
        }
        Some(DistStats {
            count: self.count as usize,
            mean: self.total_us as f64 / self.count as f64 / 1e6,
            median: self.percentile_s(0.5),
            p95: self.percentile_s(0.95),
            max: self.max_us as f64 / 1e6,
        })
    }
}

/// Elapsed time between two points, floored at zero.
fn elapsed(from: Timestamp, to: Timestamp) -> Duration {
    if to > from { to - from } else { Duration::ZERO }
}

struct Stats {
    mean: f64,
    std_dev: f64,
}

fn compute_stats<Iter: IntoIterator<Item = f64>>(data: Iter) -> Stats {
    let v: Variance = data.into_iter().collect();
    Stats {
        mean: v.mean(),
        std_dev: v.population_variance().sqrt(),
    }
}

struct DistStats {
    count: usize,
    mean: f64,
    median: f64,
    p95: f64,
    max: f64,
}

/// Count, mean, median, 95th percentile and max of a set of samples, all in
/// seconds.  Every sample is retained, which is affordable because the
/// callers are bounded by the number of EBs or of vote bundles in the run,
/// not by the number of vote arrivals (see `DelayHistogram` for that).
fn compute_dist<Iter: IntoIterator<Item = f64>>(data: Iter) -> Option<DistStats> {
    let mut samples: Vec<f64> = data.into_iter().collect();
    if samples.is_empty() {
        return None;
    }
    samples.sort_by(f64::total_cmp);
    let rank = |percentile: f64| {
        let index = (percentile * samples.len() as f64).ceil().max(1.0) as usize - 1;
        samples[index.min(samples.len() - 1)]
    };
    Some(DistStats {
        count: samples.len(),
        mean: samples.iter().sum::<f64>() / samples.len() as f64,
        median: rank(0.5),
        p95: rank(0.95),
        max: samples[samples.len() - 1],
    })
}

/// Flush all buffered events with timestamp strictly less than `up_to`.
/// Within each timestamp bucket, events are sorted by originating node ID
/// so that the output is deterministic regardless of cross-shard arrival order.
async fn flush_buffered(
    buffered: &mut std::collections::BTreeMap<Timestamp, Vec<OutputEvent>>,
    up_to: Timestamp,
    output: &mut OutputTarget,
) -> Result<()> {
    // Collect keys to flush (can't mutate while iterating).
    let keys: Vec<Timestamp> = buffered.range(..up_to).map(|(k, _)| *k).collect();
    for ts in keys {
        let mut events = buffered.remove(&ts).unwrap();
        events.sort_by_key(|e| e.message.node_id());
        for ev in events {
            output.write(ev).await?;
        }
    }
    Ok(())
}

#[allow(clippy::large_enum_variant)]
enum OutputTarget {
    AggregatedEventStream {
        aggregation: TraceAggregator,
        format: OutputFormat,
        file: TraceSink,
    },
    EventStream {
        format: OutputFormat,
        file: TraceSink,
    },
    None,
}

impl OutputTarget {
    async fn write(&mut self, event: OutputEvent) -> Result<()> {
        match self {
            Self::AggregatedEventStream {
                aggregation,
                format,
                file,
            } => {
                if let Some(summary) = aggregation.process(event) {
                    Self::write_line(*format, file, summary).await?;
                }
            }
            Self::EventStream { format, file } => {
                Self::write_line(*format, file, event).await?;
            }
            Self::None => {}
        }
        Ok(())
    }

    async fn write_line<T: Serialize, W: AsyncWrite + Unpin>(
        format: OutputFormat,
        file: &mut W,
        event: T,
    ) -> Result<()> {
        match format {
            OutputFormat::JsonStream => {
                let mut string = serde_json::to_string(&event)?;
                string.push('\n');
                file.write_all(string.as_bytes()).await?;
            }
            OutputFormat::CborStream => {
                let bytes = minicbor_serde::to_vec(&event)?;
                file.write_all(&bytes).await?;
            }
        }
        Ok(())
    }

    async fn flush(self) -> Result<()> {
        match self {
            Self::AggregatedEventStream {
                aggregation,
                format,
                mut file,
            } => {
                if let Some(summary) = aggregation.finish() {
                    Self::write_line(format, &mut file, summary).await?;
                }
                file.shutdown().await?;
            }
            Self::EventStream { mut file, .. } => {
                file.shutdown().await?;
            }
            Self::None => {}
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use sim_core::{clock::Timestamp, config::NodeId, events::Node};

    use super::*;

    /// The CIP-0164 example parameters: L_hdr = 1s, L_vote = 4s,
    /// L_diff = 7s, so the gate is at 3s, the votes are due at 7s and a
    /// certificate may first be included at 14s.
    fn window() -> VotingWindow {
        VotingWindow {
            header_deadline: Duration::from_secs(1),
            gate: Duration::from_secs(3),
            deadline: Duration::from_secs(7),
            inclusion_deadline: Duration::from_secs(14),
        }
    }

    fn node(index: usize) -> Node {
        Node {
            id: NodeId::new(index),
            name: Arc::new(format!("node-{index}")),
        }
    }

    fn eb_id(slot: u64) -> EndorserBlockId {
        EndorserBlockId {
            slot,
            pipeline: 0,
            producer: node(0),
        }
    }

    /// One EB, generated `after_slot_ms` into its slot, as the monitor
    /// holds it.
    fn ebs_at(slot: u64, after_slot_ms: u64) -> BTreeMap<EndorserBlockId, EndorserBlock> {
        let mut ebs = BTreeMap::new();
        ebs.insert(
            eb_id(slot),
            EndorserBlock::new(
                Timestamp::from_secs(slot) + Duration::from_millis(after_slot_ms),
                vec![],
                vec![],
                vec![],
            ),
        );
        ebs
    }

    fn timing(stakes: Vec<u64>) -> VoteTiming {
        VoteTiming::new(stakes.len(), stakes, window())
    }

    fn bundle_id(producer: usize) -> VoteBundleId {
        VoteBundleId {
            slot: 10,
            pipeline: 0,
            producer: node(producer),
        }
    }

    /// A quorum crossing at `slot` + `ms`, seen by node `index`.
    fn crossed(timing: &mut VoteTiming, slot: u64, index: usize, ms: u64) {
        timing.record_quorum(
            eb_id(slot),
            NodeId::new(index),
            Timestamp::from_secs(slot) + Duration::from_millis(ms),
        );
    }

    #[test]
    fn quorum_is_measured_from_the_announcing_slot_not_from_eb_generation() {
        // The 200-node run that prompted this: the EB was generated 128ms
        // into slot 70 (its producer's assembly CPU time), and the first
        // node had a quorum at 73.007s.  Measured from generation that is
        // 2.879s, which reads as comfortably inside a 4s voting period and
        // is in fact 3s of equivocation-detection wait plus 7ms of vote
        // diffusion.
        let mut timing = timing(vec![1, 1]);
        crossed(&mut timing, 70, 0, 3007);
        crossed(&mut timing, 70, 1, 3058);
        let outcome = timing.quorum_outcome(&ebs_at(70, 128), 0.0);
        let dist = outcome.dist.as_ref().expect("no quorum recorded");
        assert_eq!(dist.count, 1);
        assert!((dist.mean - 3.007).abs() < 1e-9, "{}", dist.mean);
        assert!(
            (outcome.diffusion_mean_s.unwrap() - 0.007).abs() < 1e-9,
            "the vote-diffusion part is what is left after the gate"
        );
    }

    /// The diffusion figure is a mean of per-EB parts, each floored at zero,
    /// and not the mean floored: an EB whose producer's own vote carried it
    /// past the threshold before the gate contributes nothing to diffusion,
    /// but it must not cancel out an EB that waited.
    #[test]
    fn diffusion_is_floored_per_eb_and_then_averaged() {
        let mut timing = timing(vec![1]);
        // 2.0s is before the 3.0s gate, 4.0s is a second past it.
        crossed(&mut timing, 10, 0, 2000);
        crossed(&mut timing, 20, 0, 4000);
        let mut ebs = ebs_at(10, 0);
        ebs.extend(ebs_at(20, 0));
        let outcome = timing.quorum_outcome(&ebs, 0.0);
        assert_eq!(outcome.reached(), 2);
        assert!(
            (outcome.dist.as_ref().unwrap().mean - 3.0).abs() < 1e-9,
            "the two EBs average out to exactly the gate"
        );
        let diffusion = outcome.diffusion_mean_s.unwrap();
        assert!(
            (diffusion - 0.5).abs() < 1e-9,
            "(0.0 + 1.0) / 2, not max(3.0 - 3.0, 0.0); got {diffusion}"
        );
    }

    /// Every EB before the gate is the case where flooring the mean and
    /// flooring per EB agree, and both say zero.
    #[test]
    fn diffusion_is_zero_when_every_quorum_beat_the_gate() {
        let mut timing = timing(vec![1]);
        crossed(&mut timing, 10, 0, 1000);
        crossed(&mut timing, 20, 0, 2000);
        let mut ebs = ebs_at(10, 0);
        ebs.extend(ebs_at(20, 0));
        let outcome = timing.quorum_outcome(&ebs, 0.0);
        assert_eq!(outcome.diffusion_mean_s, Some(0.0));
    }

    #[test]
    fn an_observer_with_no_quorum_anywhere_reports_no_diffusion_figure() {
        let mut timing = timing(vec![2, 8]);
        crossed(&mut timing, 10, 0, 3100);
        // Only a fifth of the stake ever tallied a quorum, so the median
        // observer has no EBs to average over -- and no zero to print as if
        // it had measured one.
        assert_eq!(
            timing.quorum_outcome(&ebs_at(10, 0), 0.5).diffusion_mean_s,
            None
        );
    }

    #[test]
    fn margin_is_reported_against_the_full_window_not_the_voting_period() {
        let mut timing = timing(vec![1]);
        crossed(&mut timing, 10, 0, 3500);
        let outcome = timing.quorum_outcome(&ebs_at(10, 0), 0.0);
        let (mean, worst) = outcome.margin_s(window()).unwrap();
        // 7s deadline from t0, quorum at 3.5s: 3.5s of slack, not the
        // 0.5s that comparing against L_vote alone would report.
        assert!((mean - 3.5).abs() < 1e-9, "{mean}");
        assert!((worst - 3.5).abs() < 1e-9, "{worst}");
        assert_eq!(outcome.in_window, 1);
        assert_eq!(outcome.by_inclusion, 1);
    }

    #[test]
    fn a_late_quorum_is_counted_late_and_its_margin_goes_negative() {
        let mut timing = timing(vec![1]);
        // Past the 7s voting deadline, inside the 14s inclusion deadline.
        crossed(&mut timing, 10, 0, 9000);
        let outcome = timing.quorum_outcome(&ebs_at(10, 0), 0.0);
        assert_eq!(outcome.reached(), 1);
        assert_eq!(outcome.in_window, 0, "9s is past the 7s deadline");
        assert_eq!(outcome.by_inclusion, 1, "but not past the 14s one");
        let (mean, worst) = outcome.margin_s(window()).unwrap();
        assert!((mean + 2.0).abs() < 1e-9, "{mean}");
        assert!((worst + 2.0).abs() < 1e-9, "{worst}");
    }

    #[test]
    fn observers_are_quantiles_of_stake_not_of_nodes() {
        // node-0 is a stakeless relay that hears everything first; the two
        // pools hold all the stake and get there later.
        let mut timing = timing(vec![0, 5, 5]);
        crossed(&mut timing, 10, 0, 1000);
        crossed(&mut timing, 10, 1, 4000);
        crossed(&mut timing, 10, 2, 5000);
        let ebs = ebs_at(10, 0);
        let at = |q: f64| timing.quorum_outcome(&ebs, q).dist.unwrap().mean;
        assert!(
            (at(0.0) - 1.0).abs() < 1e-9,
            "the luckiest node, relay or not"
        );
        assert!(
            (at(0.5) - 4.0).abs() < 1e-9,
            "half the stake only has it once the first pool does"
        );
        assert!((at(0.95) - 5.0).abs() < 1e-9, "and 95% of it once both do");
    }

    #[test]
    fn an_observer_that_never_gets_a_quorum_is_not_counted_as_being_in_the_window() {
        // Only a fifth of the stake ever tallies a quorum.
        let mut timing = timing(vec![2, 8]);
        crossed(&mut timing, 10, 0, 3100);
        let ebs = ebs_at(10, 0);
        let median = timing.quorum_outcome(&ebs, 0.5);
        assert_eq!(median.reached(), 0);
        assert_eq!(median.in_window, 0);
        assert_eq!(median.by_inclusion, 0);
        assert!(median.dist.is_none());
        assert!(median.margin_s(window()).is_none());
        assert_eq!(timing.quorum_outcome(&ebs, 0.0).reached(), 1);
    }

    #[test]
    fn quorum_statistics_do_not_depend_on_the_order_crossings_are_reported() {
        let stakes = vec![3, 1, 4, 1, 5];
        let arrivals = [(0, 5000), (1, 3200), (2, 4100), (3, 3050), (4, 6000)];
        let ebs = ebs_at(10, 90);
        let mut forwards = timing(stakes.clone());
        for (node, ms) in arrivals {
            crossed(&mut forwards, 10, node, ms);
        }
        let mut backwards = timing(stakes);
        for (node, ms) in arrivals.into_iter().rev() {
            crossed(&mut backwards, 10, node, ms);
        }
        for quantile in [0.0, 0.5, 0.95] {
            let a = forwards.quorum_outcome(&ebs, quantile);
            let b = backwards.quorum_outcome(&ebs, quantile);
            assert_eq!(a.in_window, b.in_window);
            assert_eq!(a.by_inclusion, b.by_inclusion);
            assert_eq!(
                a.dist.unwrap().mean.to_bits(),
                b.dist.unwrap().mean.to_bits(),
                "bit-identical at quantile {quantile}"
            );
        }
    }

    #[test]
    fn an_eb_the_monitor_never_saw_generated_is_left_out() {
        let mut timing = timing(vec![1]);
        crossed(&mut timing, 10, 0, 3100);
        let outcome = timing.quorum_outcome(&BTreeMap::new(), 0.0);
        assert_eq!(outcome.reached(), 0);
    }

    #[test]
    fn a_topology_without_stake_weights_every_node_the_same() {
        let mut timing = timing(vec![0, 0, 0, 0]);
        for (index, ms) in [(0, 3010), (1, 3020), (2, 3030), (3, 3040)] {
            crossed(&mut timing, 10, index, ms);
        }
        let ebs = ebs_at(10, 0);
        let median = timing.quorum_outcome(&ebs, 0.5).dist.unwrap().mean;
        assert!((median - 3.02).abs() < 1e-9, "{median}");
    }

    #[test]
    fn weighted_quantile_walks_the_samples_in_delay_order() {
        let samples = [(1_000_000, 1), (2_000_000, 8), (3_000_000, 1)]
            .map(|(delay_us, weight)| QuorumSample { delay_us, weight });
        let at = |q| weighted_quantile(&samples, 10, q);
        assert_eq!(at(0.0), Some(Duration::from_secs(1)));
        assert_eq!(at(0.1), Some(Duration::from_secs(1)));
        assert_eq!(at(0.5), Some(Duration::from_secs(2)));
        assert_eq!(at(0.9), Some(Duration::from_secs(2)));
        assert_eq!(at(0.95), Some(Duration::from_secs(3)));
        assert_eq!(at(1.0), Some(Duration::from_secs(3)));
        // Two of the twelve units of weight never reported a crossing.
        assert_eq!(weighted_quantile(&samples, 12, 1.0), None);
        assert_eq!(weighted_quantile(&[], 10, 0.0), None);
    }

    /// The failing case the coverage line exists to report: bundles were
    /// produced and not one of them got anywhere.  The line has to say so.
    #[test]
    fn the_coverage_line_reports_total_failure_instead_of_disappearing() {
        let mut timing = timing(vec![1; 20]);
        timing.bundle_generated(bundle_id(0), NodeId::new(0), Timestamp::from_secs(10));
        timing.bundle_generated(bundle_id(1), NodeId::new(1), Timestamp::from_secs(10));
        let line = timing.coverage_line().expect("two bundles were produced");
        assert!(
            line.starts_with(
                "0 out of 2 vote bundle(s) reached 95% of nodes (19 of 20); 2 never did."
            ),
            "{line}"
        );
        assert!(
            !line.contains("average"),
            "nothing got there, so there is no time to average: {line}"
        );
    }

    #[test]
    fn the_coverage_line_times_the_bundles_that_did_get_there() {
        // Four nodes, so 95% coverage means all four of them.
        let mut timing = timing(vec![1; 4]);
        timing.bundle_generated(bundle_id(0), NodeId::new(0), Timestamp::from_secs(10));
        for (node, ms) in [(1, 100), (2, 200), (3, 300)] {
            timing.bundle_received(
                &bundle_id(0),
                NodeId::new(node),
                Timestamp::from_secs(10) + Duration::from_millis(ms),
            );
        }
        // A second bundle that only ever reaches its own producer.
        timing.bundle_generated(bundle_id(1), NodeId::new(1), Timestamp::from_secs(10));
        let line = timing.coverage_line().unwrap();
        assert!(
            line.starts_with(
                "1 out of 2 vote bundle(s) reached 95% of nodes (4 of 4); 1 never did."
            ),
            "{line}"
        );
        assert!(line.contains("average of 0.300s"), "{line}");
    }

    #[test]
    fn there_is_no_coverage_line_when_no_votes_were_cast() {
        assert_eq!(timing(vec![1; 4]).coverage_line(), None);
    }

    /// Finding the same arrival redundant twice used to push duplicates past
    /// received, which saturated accepted to zero and printed a share above
    /// 100% and an "inf" verification rate.
    #[test]
    fn the_message_line_never_reports_more_duplicates_than_arrivals() {
        let stats = MessageStats {
            sent: 12,
            received: 12,
            duplicates: 4,
            duplicate_bytes: 4_000_000,
            accepted: Some(8),
            verifications: 12,
        };
        let line = stats.summary_line("Vote body");
        assert_eq!(
            line,
            "12 Vote body message(s) were sent. 12 of them were received (100.000%). \
             4 of those (33.333%) were redundant or obsolete arrivals, costing 4.00 MB; \
             8 accepted; 0 pending; 12 verification(s) completed (1.50 per accepted Vote body)."
        );
    }

    #[test]
    fn pending_verifications_are_not_accepted_votes() {
        let stats = MessageStats {
            sent: 105,
            received: 105,
            duplicates: 84,
            accepted: Some(14),
            verifications: 14,
            ..MessageStats::default()
        };
        let line = stats.summary_line("Vote body");
        assert!(
            line.contains(
                "14 accepted; 7 pending; 14 verification(s) completed (1.00 per accepted Vote body)"
            ),
            "{line}"
        );
        let pending_only = MessageStats {
            sent: 3,
            received: 3,
            accepted: Some(0),
            ..MessageStats::default()
        }
        .summary_line("Vote body");
        assert!(
            pending_only.contains("0 accepted; 3 pending; 0 verification(s) completed."),
            "{pending_only}"
        );
    }

    /// A run that verified nothing has no per-accepted rate to report, and
    /// printing one divided by zero as "inf" reads as a measurement.
    #[test]
    fn the_message_line_omits_the_rate_when_nothing_was_accepted() {
        let stats = MessageStats {
            sent: 3,
            received: 0,
            duplicates: 0,
            duplicate_bytes: 0,
            accepted: Some(0),
            verifications: 2,
        };
        let line = stats.summary_line("Vote body");
        assert!(line.contains("2 verification(s) completed."), "{line}");
        assert!(!line.contains("inf"), "{line}");
    }

    /// The comparison finding 3 is about: the same 100 bodies delivered, and
    /// announce-then-request sends far more messages to do it.
    #[test]
    fn the_wire_line_counts_the_control_messages_the_body_count_leaves_out() {
        let announce = VoteWireStats {
            body_bytes: 100_000,
            announcements: 300,
            announcement_bytes: 2400,
            requests: 100,
            request_bytes: 800,
        };
        assert_eq!(
            announce.summary_line(100),
            "Vote mini-protocol traffic sent: 500 message(s), 0.10 MB = 100 bodies (0.10 MB) \
             + 300 announcement(s) (0.00 MB) + 100 request(s) (0.00 MB)."
        );
        let push = VoteWireStats {
            body_bytes: 300_000,
            ..VoteWireStats::default()
        };
        assert_eq!(
            push.summary_line(300),
            "Vote mini-protocol traffic sent: 300 message(s), 0.30 MB = 300 bodies (0.30 MB) \
             + 0 announcement(s) (0.00 MB) + 0 request(s) (0.00 MB)."
        );
        assert!(
            VoteWireStats::default().is_empty(),
            "a variant that sends nothing on the vote protocol prints no line"
        );
    }
}
