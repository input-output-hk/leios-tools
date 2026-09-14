use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

use rand::{RngCore, SeedableRng};
use rand_chacha::ChaChaRng;
use tokio::sync::mpsc;

use crate::{
    clock::{Clock, MockClockCoordinator, Timestamp},
    config::{
        CommitteeSelectionAlgorithm, LeiosVariant, NodeId, RawLinkInfo, RawNode, RawTopology,
        RelayStrategy, SimConfiguration, TransactionConfig, VoteTransport,
    },
    events::{Event, EventTracker},
    model::{LinearEndorserBlock, LinearRankingBlock, Transaction, VoteBundle},
    sim::{
        EventResult, NodeImpl,
        linear_leios::LinearLeiosNode,
        linear_wire::{CpuTask, Message, TimedEvent},
        lottery::{LotteryKind, MockLotteryResults},
    },
};

fn new_sim_config(topology: RawTopology) -> Arc<SimConfiguration> {
    new_sim_config_with(topology, |_| {})
}

fn new_sim_config_with(
    topology: RawTopology,
    customize: impl FnOnce(&mut crate::config::RawParameters),
) -> Arc<SimConfiguration> {
    let mut params: crate::config::RawParameters =
        serde_yaml::from_slice(include_bytes!("../../../../parameters/config.default.yaml"))
            .unwrap();
    params.leios_variant = crate::config::LeiosVariant::LinearWithTxReferences;
    // every transaction fills up exactly half of an RB
    let tx_size = params.rb_body_max_size_bytes / 2;
    params.tx_size_bytes_distribution = crate::config::DistributionConfig::Constant {
        value: tx_size as f64,
    };
    params.tx_max_size_bytes = tx_size;
    // it takes two votes to certify an EB.  Pin the committee to two
    // virtual voters and demand 100% to reach a threshold of exactly 2,
    // independent of the default-config probabilities.
    params.persistent_voters = 2.0;
    params.non_persistent_voters = 0.0;
    params.quorum_weight_fraction = 1.0;
    customize(&mut params);
    let topology = topology.into();
    Arc::new(SimConfiguration::build(params, topology).unwrap())
}

fn new_sim(
    sim_config: Arc<SimConfiguration>,
    event_tx: mpsc::UnboundedSender<(Event, Timestamp)>,
    clock: Clock,
) -> (
    HashMap<NodeId, LinearLeiosNode>,
    HashMap<NodeId, Arc<MockLotteryResults>>,
) {
    let tracker = EventTracker::new(event_tx, clock.clone(), &sim_config.nodes);
    let mut rng = ChaChaRng::seed_from_u64(sim_config.seed);
    let mut lottery = HashMap::new();
    let nodes = sim_config
        .nodes
        .iter()
        .map(|config| {
            let rng = ChaChaRng::seed_from_u64(rng.next_u64());
            let mut node = LinearLeiosNode::new(
                config,
                sim_config.clone(),
                tracker.clone(),
                rng,
                clock.clone(),
            );
            let lottery_results = Arc::new(MockLotteryResults::default());
            node.mock_lottery(lottery_results.clone());
            lottery.insert(config.id, lottery_results);
            (config.id, node)
        })
        .collect();
    (nodes, lottery)
}

fn new_topology(nodes: Vec<(&'static str, RawNode)>) -> RawTopology {
    RawTopology {
        nodes: nodes
            .into_iter()
            .map(|(name, node)| (name.to_string(), node))
            .collect(),
    }
}
fn new_node(stake: Option<u64>, producers: Vec<&'static str>) -> RawNode {
    RawNode {
        stake,
        location: crate::config::RawNodeLocation::Cluster {
            cluster: "all".into(),
        },
        cpu_core_count: Some(4),
        tx_conflict_fraction: None,
        tx_generation_weight: None,
        producers: producers
            .iter()
            .map(|n| {
                (
                    n.to_string(),
                    RawLinkInfo {
                        latency_ms: 0.0,
                        bandwidth_bytes_per_second: None,
                        tcp_envelope: None,
                    },
                )
            })
            .collect(),
        adversarial: None,
        behaviours: vec![],
    }
}

struct TestDriver {
    pub config: Arc<SimConfiguration>,
    rng: ChaChaRng,
    slot: u64,
    time: MockClockCoordinator,
    nodes: HashMap<NodeId, LinearLeiosNode>,
    lottery: HashMap<NodeId, Arc<MockLotteryResults>>,
    queued: HashMap<NodeId, EventResult<LinearLeiosNode>>,
    events: BTreeMap<Timestamp, Vec<(NodeId, TimedEvent)>>,
    tracked: mpsc::UnboundedReceiver<(Event, Timestamp)>,
}

impl TestDriver {
    fn new(topology: RawTopology) -> Self {
        Self::new_with_config(new_sim_config(topology))
    }

    fn new_with_config(config: Arc<SimConfiguration>) -> Self {
        let rng = ChaChaRng::seed_from_u64(config.seed);
        let slot = 0;
        let time = MockClockCoordinator::new();
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (nodes, lottery) = new_sim(config.clone(), event_tx, time.clock());
        Self {
            config,
            rng,
            slot,
            time,
            nodes,
            lottery,
            queued: HashMap::new(),
            events: BTreeMap::new(),
            tracked: event_rx,
        }
    }

    /// Everything the nodes have reported to the event tracker since the
    /// last call.  The run summary is built from these and from nothing
    /// else, so this is where a test looks to see what a run would report.
    pub fn drain_tracked_events(&mut self) -> Vec<Event> {
        let mut events = vec![];
        while let Ok((event, _)) = self.tracked.try_recv() {
            events.push(event);
        }
        events
    }

    /// How many votes of its own `node` has queued but not yet signed.
    /// Nothing is dequeued, so this can be asked repeatedly while time
    /// advances.
    pub fn queued_vote_generations(&self, node: NodeId) -> usize {
        self.queued
            .get(&node)
            .map(|q| {
                q.tasks
                    .iter()
                    .filter(|t| matches!(t, CpuTask::VTBundleGenerated(..)))
                    .count()
            })
            .unwrap_or(0)
    }

    /// How many vote-bundle verifications `node` has queued but not run.
    /// One per arrival the node decided to pay for.
    pub fn queued_vote_validations(&self, node: NodeId) -> usize {
        self.queued
            .get(&node)
            .map(|q| {
                q.tasks
                    .iter()
                    .filter(|t| matches!(t, CpuTask::VTBundleValidated(..)))
                    .count()
            })
            .unwrap_or(0)
    }

    pub fn id_for(&self, name: &str) -> NodeId {
        self.config
            .nodes
            .iter()
            .find_map(|n| if n.name == name { Some(n.id) } else { None })
            .unwrap()
    }

    pub fn now(&self) -> Timestamp {
        self.time.now()
    }

    pub fn produce_tx(&mut self, node_id: NodeId, conflict: bool) -> Arc<Transaction> {
        let TransactionConfig::Real(tx_config) = &self.config.transactions else {
            panic!("unexpected TX config")
        };
        let tx = Arc::new(tx_config.new_tx(&mut self.rng, Some(if conflict { 1.0 } else { 0.0 })));
        let node = self.nodes.get_mut(&node_id).unwrap();
        let events = node.handle_new_tx(tx.clone());
        self.process_events(node_id, events);
        tx
    }

    pub fn produce_txs<const N: usize>(
        &mut self,
        node_id: NodeId,
        conflict: bool,
    ) -> [Arc<Transaction>; N] {
        [(); N].map(|_| self.produce_tx(node_id, conflict))
    }

    pub fn win_next_rb_lottery(&mut self, node_id: NodeId, result: u64) {
        self.lottery
            .get(&node_id)
            .unwrap()
            .configure_win(LotteryKind::GenerateRB, result);
    }

    pub fn win_next_vote_lottery(&mut self, node_id: NodeId, result: u64) {
        self.lottery
            .get(&node_id)
            .unwrap()
            .configure_win(LotteryKind::GenerateVote, result);
    }

    pub fn next_slot(&mut self) {
        self.advance_time_to(Timestamp::from_secs(self.slot + 1));
    }

    pub fn advance_time_to(&mut self, timestamp: Timestamp) {
        let mut now = self.time.now();
        while now < timestamp {
            let next_slot = self.slot + 1;
            let next_slot_time = Timestamp::from_secs(next_slot);
            let mut next_event = timestamp.min(next_slot_time);
            if let Some((event_time, _)) = self.events.first_key_value() {
                next_event = next_event.min(*event_time);
            }
            self.time.advance_time(next_event);
            now = next_event;

            let mut updates: HashMap<NodeId, EventResult<LinearLeiosNode>> = HashMap::new();
            if now == next_slot_time {
                for (node_id, node) in &mut self.nodes {
                    let events = node.handle_new_slot(next_slot);
                    updates.entry(*node_id).or_default().merge(events);
                }
                self.slot = next_slot;
            }
            if let Some(events) = self.events.remove(&next_event) {
                for (node_id, event) in events {
                    let node = self.nodes.get_mut(&node_id).unwrap();
                    let events = node.handle_timed_event(event);
                    updates.entry(node_id).or_default().merge(events);
                }
            }

            for (node, events) in updates {
                self.process_events(node, events);
            }
        }
    }

    /// Drive `node` all the way to a vote bundle of its own: it mints an RB
    /// with an EB, then votes for that EB off the head of its own chain.
    /// Nothing is diffused on the way, so the only messages the caller has to
    /// reason about afterwards are the ones the vote bundle itself produced.
    pub fn produce_vote_bundle(&mut self, node: NodeId) -> Arc<VoteBundle> {
        let _txs: [_; 3] = self.produce_txs(node, false);
        // Producers obey the same equivocation gate as every other voter.
        self.win_next_rb_lottery(node, 0);
        self.win_next_vote_lottery(node, 0);
        self.next_slot();
        let (_rb, eb) = self.expect_cpu_task_matching(node, is_new_rb_task);
        let eb = eb.expect("node did not produce EB");

        self.advance_time_to(self.config.voting_window().gate_at(eb.slot));
        let votes = self.expect_cpu_task_matching(node, is_new_vote_task);
        assert_eq!(*votes.ebs.first_key_value().unwrap().0, eb.id());
        votes
    }

    pub fn expect_tx_sent(&mut self, from: NodeId, to: NodeId, tx: Arc<Transaction>) {
        self.expect_message(from, to, Message::AnnounceTx(tx.id));
        self.expect_message(to, from, Message::RequestTx(tx.id));
        self.expect_message(from, to, Message::Tx(tx.clone()));
        self.expect_cpu_task(to, CpuTask::TransactionValidated(from, tx));
    }

    pub fn expect_tx_not_sent(&mut self, from: NodeId, to: NodeId, tx: Arc<Transaction>) {
        self.expect_no_message(from, to, Message::AnnounceTx(tx.id));
    }

    pub fn expect_rb_and_eb_sent(
        &mut self,
        from: NodeId,
        to: NodeId,
        rb: Arc<LinearRankingBlock>,
        eb: Option<Arc<LinearEndorserBlock>>,
    ) {
        self.expect_message(from, to, Message::AnnounceRBHeader(rb.header.id));
        self.expect_message(to, from, Message::RequestRBHeader(rb.header.id));
        self.expect_message(
            from,
            to,
            Message::RBHeader(rb.header.clone(), true, eb.is_some()),
        );
        self.expect_cpu_task(
            to,
            CpuTask::RBHeaderValidated(from, rb.header.clone(), true, eb.is_some()),
        );
        self.expect_message(to, from, Message::RequestRB(rb.header.id));
        self.expect_message(from, to, Message::RB(rb.clone()));
        self.expect_cpu_task(to, CpuTask::RBBlockValidated(rb));
        if let Some(eb) = eb {
            self.expect_message(to, from, Message::RequestEB(eb.id()));
            self.expect_message(from, to, Message::EB(eb.clone()));
            self.expect_cpu_task(to, CpuTask::EBHeaderValidated(from, eb));
        }
    }

    pub fn expect_eb_validated(&mut self, node: NodeId, eb: Arc<LinearEndorserBlock>) {
        self.expect_cpu_task(node, CpuTask::EBBlockValidated(eb, self.time.now()));
    }

    pub fn expect_vote_bundle_sent(&mut self, from: NodeId, to: NodeId, votes: Arc<VoteBundle>) {
        self.deliver_vote_bundle(from, to, votes.clone());
        self.expect_cpu_task(to, CpuTask::VTBundleValidated(from, votes));
    }

    fn deliver_vote_bundle(&mut self, from: NodeId, to: NodeId, votes: Arc<VoteBundle>) {
        if !self.config.vote_transport.is_push() {
            self.expect_message(from, to, Message::AnnounceVotes(votes.id));
            self.expect_message(to, from, Message::RequestVotes(votes.id));
        }
        self.expect_message(from, to, Message::Votes(votes));
    }

    pub fn expect_message(
        &mut self,
        from: NodeId,
        to: NodeId,
        message: <LinearLeiosNode as NodeImpl>::Message,
    ) {
        let queued = self.queued.entry(from).or_default();
        let mut found = false;
        queued.messages.retain(|(t, msg)| {
            if t == &to && msg == &message {
                found = true;
                false
            } else {
                true
            }
        });
        assert!(
            found,
            "message {message:?} was not sent from {from} to {to}\npending messages: {:?}",
            queued
                .messages
                .iter()
                .filter(|(t, _)| t == &to)
                .collect::<Vec<_>>(),
        );
        let events = self
            .nodes
            .get_mut(&to)
            .unwrap()
            .handle_message(from, message);
        self.process_events(to, events);
    }

    pub fn expect_no_message(
        &mut self,
        from: NodeId,
        to: NodeId,
        message: <LinearLeiosNode as NodeImpl>::Message,
    ) {
        let Some(queued) = self.queued.get(&from) else {
            return;
        };
        for (t, m) in &queued.messages {
            assert_ne!((t, m), (&to, &message));
        }
    }

    pub fn expect_cpu_task(&mut self, node: NodeId, task: <LinearLeiosNode as NodeImpl>::Task) {
        self.expect_cpu_task_matching(node, |t| if *t == task { Some(t.clone()) } else { None });
    }

    pub fn expect_cpu_task_matching<T, M>(&mut self, node: NodeId, matcher: M) -> T
    where
        M: Fn(&<LinearLeiosNode as NodeImpl>::Task) -> Option<T>,
    {
        let queued = self.queued.entry(node).or_default();
        let mut result = None;
        let mut events = EventResult::default();
        queued.tasks.retain(|t| {
            if result.is_some() {
                return true;
            }
            result = matcher(t);
            if result.is_some() {
                events = self
                    .nodes
                    .get_mut(&node)
                    .unwrap()
                    .handle_cpu_task(t.clone());
            }
            result.is_none()
        });
        self.process_events(node, events);
        result.expect("no CPU tasks matching filter")
    }

    fn process_events(&mut self, node: NodeId, mut events: EventResult<LinearLeiosNode>) {
        for (timestamp, event) in events.timed_events.drain(..) {
            self.events
                .entry(timestamp)
                .or_default()
                .push((node, event));
        }
        self.queued.entry(node).or_default().merge(events);
    }
}

fn is_new_rb_task(
    task: &CpuTask,
) -> Option<(Arc<LinearRankingBlock>, Option<Arc<LinearEndorserBlock>>)> {
    match task {
        CpuTask::RBBlockGenerated(rb, eb) => Some((
            Arc::new(rb.clone()),
            eb.as_ref().map(|(eb, _)| Arc::new(eb.clone())),
        )),
        _ => None,
    }
}

/// The (sender, recipient) of every vote bundle reported as one the
/// recipient did not need.
fn duplicate_reports(events: &[Event]) -> Vec<(NodeId, NodeId)> {
    events
        .iter()
        .filter_map(|event| match event {
            Event::VTBundleDuplicate {
                sender, recipient, ..
            } => Some((sender.id, recipient.id)),
            _ => None,
        })
        .collect()
}

/// Every reported vote-protocol message, as
/// (kind, sender, recipient, bytes).  Announcements and requests are the two
/// that used to go uncounted, which is what made the arm that sends the most
/// messages look like the one that sends the fewest.
fn vote_wire_reports(events: &[Event]) -> Vec<(&'static str, NodeId, NodeId, u64)> {
    events
        .iter()
        .filter_map(|event| match event {
            Event::VTBundleSent {
                sender,
                recipient,
                msg_size_bytes,
                ..
            } => Some(("body", sender.id, recipient.id, *msg_size_bytes)),
            Event::VTBundleAnnounced {
                sender,
                recipient,
                msg_size_bytes,
                ..
            } => Some(("announce", sender.id, recipient.id, *msg_size_bytes)),
            Event::VTBundleRequested {
                sender,
                recipient,
                msg_size_bytes,
                ..
            } => Some(("request", sender.id, recipient.id, *msg_size_bytes)),
            _ => None,
        })
        .collect()
}

/// The (node, tally) of every reported quorum crossing.
fn quorum_reports(events: &[Event]) -> Vec<(NodeId, u64)> {
    events
        .iter()
        .filter_map(|event| match event {
            Event::EBQuorumReached { node, votes, .. } => Some((node.id, *votes)),
            _ => None,
        })
        .collect()
}

fn is_new_vote_task(task: &CpuTask) -> Option<Arc<VoteBundle>> {
    match task {
        CpuTask::VTBundleGenerated(vote, _) => Some(Arc::new(vote.clone())),
        _ => None,
    }
}

#[test]
fn should_produce_rbs_without_ebs() {
    let topology = new_topology(vec![
        ("node-1", new_node(Some(1000), vec!["node-2"])),
        ("node-2", new_node(Some(1000), vec!["node-1"])),
    ]);
    let mut sim = TestDriver::new(topology);
    let node1 = sim.id_for("node-1");
    let node2 = sim.id_for("node-2");

    // Node 1 produces a transaction, node 2 should request it
    let tx1 = sim.produce_tx(node1, false);
    sim.expect_tx_sent(node1, node2, tx1.clone());

    // Node 2 produces a transaction, node 1 should request it
    let tx2 = sim.produce_tx(node2, false);
    sim.expect_tx_sent(node2, node1, tx2.clone());

    // When node 1 produces an RB, it should include both TXs
    sim.win_next_rb_lottery(node1, 0);
    sim.next_slot();
    let (new_rb, new_eb) = sim.expect_cpu_task_matching(node1, is_new_rb_task);
    assert_eq!(new_rb.transactions, vec![tx1, tx2]);
    assert_eq!(new_eb, None);

    sim.expect_rb_and_eb_sent(node1, node2, new_rb, None);
}

#[test]
fn should_produce_rbs_and_ebs() {
    let topology = new_topology(vec![
        ("node-1", new_node(Some(1000), vec!["node-2"])),
        ("node-2", new_node(Some(1000), vec!["node-1"])),
    ]);
    let mut sim = TestDriver::new(topology);
    let node1 = sim.id_for("node-1");
    let node2 = sim.id_for("node-2");

    // Node 1 produces three transactions, Node 2 should request them all
    let [tx1_1, tx1_2, tx1_3] = sim.produce_txs(node1, false);
    for tx in [&tx1_1, &tx1_2, &tx1_3] {
        sim.expect_tx_sent(node1, node2, tx.clone());
    }

    sim.win_next_rb_lottery(node1, 0);
    sim.next_slot();
    let (new_rb, new_eb) = sim.expect_cpu_task_matching(node1, is_new_rb_task);
    assert_eq!(new_rb.transactions, vec![tx1_1, tx1_2]);
    let new_eb = new_eb.expect("no EB produced");
    assert_eq!(new_eb.txs, vec![tx1_3]);

    sim.expect_rb_and_eb_sent(node1, node2, new_rb, Some(new_eb.clone()));
    sim.expect_eb_validated(node2, new_eb);
}

#[test]
fn should_not_propagate_conflicting_transactions() {
    let topology = new_topology(vec![
        ("node-1", new_node(Some(1000), vec!["node-2"])),
        ("node-2", new_node(Some(1000), vec!["node-1"])),
    ]);
    let mut sim = TestDriver::new(topology);
    let node1 = sim.id_for("node-1");
    let node2 = sim.id_for("node-2");

    // Node 1 and 2 produce conflicting transactions
    let tx1 = sim.produce_tx(node1, false);
    let tx2 = sim.produce_tx(node2, true);

    // Each node should send its TX to the other node,
    sim.expect_tx_sent(node1, node2, tx1.clone());
    sim.expect_tx_sent(node2, node1, tx2.clone());

    // When node 1 produces an RB, it should include only its own TX
    sim.win_next_rb_lottery(node1, 0);
    sim.next_slot();
    let (new_rb, new_eb) = sim.expect_cpu_task_matching(node1, is_new_rb_task);
    assert_eq!(new_rb.transactions, vec![tx1]);
    assert_eq!(new_eb, None);
}

#[test]
fn should_repropagate_conflicting_transactions_from_eb() {
    let topology = new_topology(vec![
        ("node-1", new_node(Some(1000), vec!["node-2"])),
        ("node-2", new_node(Some(1000), vec!["node-1", "node-3"])),
        ("node-3", new_node(Some(1000), vec!["node-2"])),
    ]);
    let mut sim = TestDriver::new(topology);
    let node1 = sim.id_for("node-1");
    let node2 = sim.id_for("node-2");
    let node3 = sim.id_for("node-3");

    // Node 1 produces 3 transactions
    let [tx1_1, tx1_2, tx1_3] = sim.produce_txs(node1, false);

    // Node 2 produces a transaction which conflicts with Node 1's final transaction
    let tx2 = sim.produce_tx(node2, true);
    // Node 2 sends its transactions to nodes 1 and 3
    sim.expect_tx_sent(node2, node1, tx2.clone());
    sim.expect_tx_sent(node2, node3, tx2.clone());

    // Node 1 sends all of its transactions to node 2
    sim.expect_tx_sent(node1, node2, tx1_1.clone());
    sim.expect_tx_sent(node1, node2, tx1_2.clone());
    sim.expect_tx_sent(node1, node2, tx1_3.clone());

    // Node 2 sends the first two transactions to node 3, but not the conflicting third
    sim.expect_tx_sent(node2, node3, tx1_1.clone());
    sim.expect_tx_sent(node2, node3, tx1_2.clone());
    sim.expect_tx_not_sent(node2, node3, tx1_3.clone());

    // Now, Node 1 produces an RB (with an EB, because there are enough transactions)
    sim.win_next_rb_lottery(node1, 0);
    sim.next_slot();
    let (rb, eb) = sim.expect_cpu_task_matching(node1, is_new_rb_task);
    let eb = eb.expect("node did not produce EB");
    assert_eq!(rb.transactions, vec![tx1_1, tx1_2]);
    assert_eq!(eb.txs, vec![tx1_3.clone()]);

    // That RB and EB propagate from node 1 to node 2
    sim.expect_rb_and_eb_sent(node1, node2, rb.clone(), Some(eb.clone()));
    // Node 2 fully validates the EB, because node 1 has all TXs
    sim.expect_eb_validated(node2, eb.clone());
    // And Node 2 propagates it to Node 3
    sim.expect_rb_and_eb_sent(node2, node3, rb.clone(), Some(eb.clone()));

    // and NOW Node 2 will tell Node 3 about the EB's conflicting TX
    sim.expect_tx_sent(node2, node3, tx1_3);
    sim.expect_eb_validated(node3, eb);
}

#[test]
fn should_vote_for_eb() {
    let topology = new_topology(vec![
        ("node-1", new_node(Some(1000), vec!["node-2"])),
        ("node-2", new_node(Some(1000), vec!["node-1"])),
    ]);
    let mut sim = TestDriver::new(topology);
    let node1 = sim.id_for("node-1");
    let node2 = sim.id_for("node-2");

    let txs: [_; 3] = sim.produce_txs(node1, false);
    for tx in &txs {
        sim.expect_tx_sent(node1, node2, tx.clone());
    }

    sim.win_next_rb_lottery(node1, 0);
    sim.next_slot();
    let (rb, eb) = sim.expect_cpu_task_matching(node1, is_new_rb_task);
    let eb = eb.expect("node did not produce EB");

    sim.expect_rb_and_eb_sent(node1, node2, rb.clone(), Some(eb.clone()));
    sim.expect_eb_validated(node2, eb.clone());

    sim.win_next_vote_lottery(node2, 0);
    sim.advance_time_to(sim.now() + (sim.config.header_diffusion_time * 3));
    let vote = sim.expect_cpu_task_matching(node2, is_new_vote_task);
    assert_eq!(*vote.ebs.first_key_value().unwrap().0, eb.id());
}

#[test]
fn should_fetch_referenced_txs_for_eb() {
    // Node 1 produces an EB referencing txs that Node 2 never received (as if
    // they were generated on the far side of a partition).  In the references
    // variant Node 2 cannot vote until it has every referenced body, so it must
    // fetch the missing ones from the EB sender via RequestEBTxs/EBTxs, then
    // validate the EB.  This is the on-demand fetch the variant needs to recover
    // after a partition heal — without it the EB stays stuck below quorum.
    let topology = new_topology(vec![
        ("node-1", new_node(Some(1000), vec!["node-2"])),
        ("node-2", new_node(Some(1000), vec!["node-1"])),
    ]);
    let mut sim = TestDriver::new(topology);
    let node1 = sim.id_for("node-1");
    let node2 = sim.id_for("node-2");

    // Node 1 generates the txs but we deliberately do NOT diffuse them to
    // Node 2 (skip expect_tx_sent), so Node 2 is missing every referenced tx.
    let _txs: [_; 3] = sim.produce_txs(node1, false);

    sim.win_next_rb_lottery(node1, 0);
    sim.next_slot();
    let (rb, eb) = sim.expect_cpu_task_matching(node1, is_new_rb_task);
    let eb = eb.expect("node did not produce EB");
    assert!(!eb.txs.is_empty(), "EB should reference at least one tx");

    // RB+EB diffuse to Node 2.  Validating the EB header, Node 2 finds it is
    // missing every referenced tx, so it issues a RequestEBTxs to the sender.
    sim.expect_rb_and_eb_sent(node1, node2, rb.clone(), Some(eb.clone()));

    // The fetch round-trip: Node 2 -> Node 1 RequestEBTxs(all indices), Node 1
    // replies with the bodies straight out of its stored EB.
    let indices: Vec<u32> = (0..eb.txs.len() as u32).collect();
    let bitmap = shared_consensus::bitmap::from_indices(&indices);
    sim.expect_message(node2, node1, Message::RequestEBTxs(eb.id(), bitmap));
    sim.expect_message(node1, node2, Message::EBTxs(eb.id(), eb.txs.clone()));

    // The bodies run through the normal validation pipeline; once the last one
    // lands, the gated EB is released and validated.
    for tx in &eb.txs {
        sim.expect_cpu_task(node2, CpuTask::TransactionValidated(node1, tx.clone()));
    }
    sim.expect_eb_validated(node2, eb);
}

#[test]
fn should_not_include_tx_twice() {
    let topology = new_topology(vec![
        ("node-1", new_node(Some(1000), vec!["node-2"])),
        ("node-2", new_node(Some(1000), vec!["node-1"])),
    ]);
    let mut sim = TestDriver::new(topology);
    let node1 = sim.id_for("node-1");
    let node2 = sim.id_for("node-2");

    let [rb_tx1, rb_tx2, eb_tx] = sim.produce_txs(node1, false);
    for tx in [&rb_tx1, &rb_tx2, &eb_tx] {
        sim.expect_tx_sent(node1, node2, tx.clone());
    }

    sim.win_next_vote_lottery(node1, 0);
    sim.win_next_vote_lottery(node2, 0);

    // Node 1 produces an RB containing two transactions, and an EB containing a third
    sim.win_next_rb_lottery(node1, 0);
    sim.next_slot();
    let (rb, eb) = sim.expect_cpu_task_matching(node1, is_new_rb_task);
    let eb = eb.expect("node did not produce EB");
    assert!(eb.txs.contains(&eb_tx));

    // Node 2 receives and validates the RB and EB
    sim.expect_rb_and_eb_sent(node1, node2, rb.clone(), Some(eb.clone()));
    sim.expect_eb_validated(node2, eb.clone());

    // Nodes 1 and 2 both vote for the EB
    sim.advance_time_to(sim.now() + (sim.config.header_diffusion_time * 3));
    let votes_1 = sim.expect_cpu_task_matching(node1, is_new_vote_task);
    sim.expect_vote_bundle_sent(node1, node2, votes_1);
    let votes_2 = sim.expect_cpu_task_matching(node2, is_new_vote_task);
    sim.expect_vote_bundle_sent(node2, node1, votes_2);

    // After enough time has elapsed to include the EB in a new RB, Node 2 mints a new RB
    sim.advance_time_to(
        sim.now()
            + Duration::from_secs(sim.config.linear_diffuse_stage_length)
            + Duration::from_secs(sim.config.linear_vote_stage_length),
    );
    sim.win_next_rb_lottery(node2, 0);
    sim.next_slot();
    let (rb, new_eb) = sim.expect_cpu_task_matching(node2, is_new_rb_task);

    // This RB endorses the previous EB (including its transaction on the chain)
    assert_eq!(rb.endorsement.as_ref().map(|e| e.eb), Some(eb.id()));

    // And it does not include any transactions of its own
    assert!(rb.transactions.is_empty());
    assert_eq!(new_eb, None);
}

#[test]
fn everyone_committee_should_vote_for_eb() {
    let topology = new_topology(vec![
        ("node-1", new_node(Some(1000), vec!["node-2"])),
        ("node-2", new_node(Some(1000), vec!["node-1"])),
    ]);
    let config = new_sim_config_with(topology, |params| {
        params.committee_selection_algorithm = CommitteeSelectionAlgorithm::Everyone;
    });
    let mut sim = TestDriver::new_with_config(config);
    let node1 = sim.id_for("node-1");
    let node2 = sim.id_for("node-2");

    let txs: [_; 3] = sim.produce_txs(node1, false);
    for tx in &txs {
        sim.expect_tx_sent(node1, node2, tx.clone());
    }

    // Node 1 produces an RB and EB
    sim.win_next_rb_lottery(node1, 0);
    sim.next_slot();
    let (rb, eb) = sim.expect_cpu_task_matching(node1, is_new_rb_task);
    let eb = eb.expect("node did not produce EB");

    sim.expect_rb_and_eb_sent(node1, node2, rb.clone(), Some(eb.clone()));
    sim.expect_eb_validated(node2, eb.clone());

    // Both nodes should vote without needing win_next_vote_lottery
    sim.advance_time_to(sim.now() + (sim.config.header_diffusion_time * 3));
    let votes_1 = sim.expect_cpu_task_matching(node1, is_new_vote_task);
    assert_eq!(*votes_1.ebs.first_key_value().unwrap().1, 1);
    let votes_2 = sim.expect_cpu_task_matching(node2, is_new_vote_task);
    assert_eq!(*votes_2.ebs.first_key_value().unwrap().1, 1);
}

#[test]
fn top_stake_fraction_should_select_voters() {
    let topology = new_topology(vec![
        ("big", new_node(Some(500), vec!["medium", "small"])),
        ("medium", new_node(Some(300), vec!["big", "small"])),
        ("small", new_node(Some(200), vec!["big", "medium"])),
    ]);
    let config = new_sim_config_with(topology, |params| {
        params.committee_selection_algorithm = CommitteeSelectionAlgorithm::TopStakeFraction;
        params.committee_stake_fraction_threshold = 0.75;
        // σ_c > τ is the CIP-164 PR #1196 invariant; relax τ here
        // since this test exercises membership selection, not quorum
        // reachability (the make_sim_config default of 1.0 would
        // trigger the startup check).
        params.quorum_weight_fraction = 0.5;
    });

    // big (500) + medium (300) = 800 >= 750 (75% of 1000), so small is excluded
    let mut sim = TestDriver::new_with_config(config);
    let big = sim.id_for("big");
    let medium = sim.id_for("medium");
    let small = sim.id_for("small");

    assert!(sim.config.vote_eligible_nodes.contains(&big));
    assert!(sim.config.vote_eligible_nodes.contains(&medium));
    assert!(!sim.config.vote_eligible_nodes.contains(&small));

    let txs: [_; 3] = sim.produce_txs(big, false);
    for tx in &txs {
        sim.expect_tx_sent(big, medium, tx.clone());
        sim.expect_tx_sent(big, small, tx.clone());
    }

    // big produces an RB and EB
    sim.win_next_rb_lottery(big, 0);
    sim.next_slot();
    let (rb, eb) = sim.expect_cpu_task_matching(big, is_new_rb_task);
    let eb = eb.expect("node did not produce EB");

    sim.expect_rb_and_eb_sent(big, medium, rb.clone(), Some(eb.clone()));
    sim.expect_rb_and_eb_sent(big, small, rb.clone(), Some(eb.clone()));
    sim.expect_eb_validated(medium, eb.clone());
    sim.expect_eb_validated(small, eb.clone());

    // big and medium should vote; small should not
    sim.advance_time_to(sim.now() + (sim.config.header_diffusion_time * 3));
    let votes_big = sim.expect_cpu_task_matching(big, is_new_vote_task);
    assert_eq!(*votes_big.ebs.first_key_value().unwrap().0, eb.id());
    let votes_medium = sim.expect_cpu_task_matching(medium, is_new_vote_task);
    assert_eq!(*votes_medium.ebs.first_key_value().unwrap().0, eb.id());

    // small should have no vote task queued
    let has_vote_task = sim
        .queued
        .get(&small)
        .map(|q| {
            q.tasks
                .iter()
                .any(|t| matches!(t, CpuTask::VTBundleGenerated(..)))
        })
        .unwrap_or(false);
    assert!(
        !has_vote_task,
        "small node should not have generated a vote"
    );
}

/// CIP-164 PR #1196: under TopStakeFraction the per-voter contribution
/// to a VoteBundle is the voter's own stake, and the absolute quorum
/// threshold is `quorum_weight_fraction × total_active_stake`.
///
/// Topology: stakes 600/300/100, σ_c = 0.95 → all three eligible.
/// τ = 0.75 → threshold 750 stake-units.
/// - "big" alone (600) < 750 — no quorum.
/// - "big" + "medium" (900) ≥ 750 — quorum.  Reached with 2/3 voters
///   even though no single voter is a majority.
#[test]
fn top_stake_fraction_uses_stake_weighted_quorum() {
    let topology = new_topology(vec![
        ("big", new_node(Some(600), vec!["medium", "small"])),
        ("medium", new_node(Some(300), vec!["big", "small"])),
        ("small", new_node(Some(100), vec!["big", "medium"])),
    ]);
    let config = new_sim_config_with(topology, |params| {
        params.committee_selection_algorithm = CommitteeSelectionAlgorithm::TopStakeFraction;
        params.committee_stake_fraction_threshold = 0.95;
        params.quorum_weight_fraction = 0.75;
    });

    // Quorum denominator is total active stake; threshold = τ × total.
    assert_eq!(config.expected_total_weight, 1000);
    assert_eq!(config.vote_threshold(), 750);

    let mut sim = TestDriver::new_with_config(config);
    let big = sim.id_for("big");
    let medium = sim.id_for("medium");
    let small = sim.id_for("small");

    // big produces an RB and EB.
    let txs: [_; 3] = sim.produce_txs(big, false);
    for tx in &txs {
        sim.expect_tx_sent(big, medium, tx.clone());
        sim.expect_tx_sent(big, small, tx.clone());
    }
    sim.win_next_rb_lottery(big, 0);
    sim.next_slot();
    let (rb, eb) = sim.expect_cpu_task_matching(big, is_new_rb_task);
    let eb = eb.expect("node did not produce EB");

    sim.expect_rb_and_eb_sent(big, medium, rb.clone(), Some(eb.clone()));
    sim.expect_rb_and_eb_sent(big, small, rb.clone(), Some(eb.clone()));
    sim.expect_eb_validated(medium, eb.clone());
    sim.expect_eb_validated(small, eb.clone());

    sim.advance_time_to(sim.now() + (sim.config.header_diffusion_time * 3));

    // Per-voter weight in the VoteBundle is the voter's stake.
    let votes_big = sim.expect_cpu_task_matching(big, is_new_vote_task);
    assert_eq!(*votes_big.ebs.first_key_value().unwrap().1, 600);
    let votes_medium = sim.expect_cpu_task_matching(medium, is_new_vote_task);
    assert_eq!(*votes_medium.ebs.first_key_value().unwrap().1, 300);
    let votes_small = sim.expect_cpu_task_matching(small, is_new_vote_task);
    assert_eq!(*votes_small.ebs.first_key_value().unwrap().1, 100);

    // Sanity: head-count majority of small voters ({medium, small} =
    // 2/3 of nodes) carries only 400 stake, below the 750 threshold.
    // The PR #1196 security property is that such a coalition does
    // NOT certify.
    let low_stake_majority = (votes_medium.ebs.first_key_value().unwrap().1
        + votes_small.ebs.first_key_value().unwrap().1) as u64;
    assert!(low_stake_majority < sim.config.vote_threshold());
}

/// PR #1196 invariant σ_c > τ must be enforced at config load.
#[test]
fn sim_config_rejects_top_stake_fraction_when_sigma_c_le_tau() {
    let topology = new_topology(vec![
        ("big", new_node(Some(600), vec!["medium"])),
        ("medium", new_node(Some(400), vec!["big"])),
    ]);
    let mut params: crate::config::RawParameters =
        serde_yaml::from_slice(include_bytes!("../../../../parameters/config.default.yaml"))
            .unwrap();
    params.leios_variant = crate::config::LeiosVariant::LinearWithTxReferences;
    params.committee_selection_algorithm = CommitteeSelectionAlgorithm::TopStakeFraction;
    params.committee_stake_fraction_threshold = 0.75;
    params.quorum_weight_fraction = 0.80; // σ_c=0.75 ≤ τ=0.80

    let topology: crate::config::Topology = topology.into();
    let err = SimConfiguration::build(params, topology).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("σ_c") && msg.contains("τ"),
        "expected σ_c/τ violation, got: {msg}"
    );
}

/// CIP-0164 PR #1250 selects a fixed count of pools but retains stake
/// weights and a quorum measured against all active stake.
///
/// Topology: stakes 500/300/200 plus a zero-stake relay, 2 seats.
/// pool-a and pool-b are seated; pool-c has stake but misses the cut,
/// and the relay is not a pool at all.
#[test]
fn top_stake_seats_should_seat_only_the_top_pools() {
    let topology = new_topology(vec![
        (
            "pool-a",
            new_node(Some(500), vec!["pool-b", "pool-c", "relay"]),
        ),
        (
            "pool-b",
            new_node(Some(300), vec!["pool-a", "pool-c", "relay"]),
        ),
        (
            "pool-c",
            new_node(Some(200), vec!["pool-a", "pool-b", "relay"]),
        ),
        ("relay", new_node(None, vec!["pool-a", "pool-b", "pool-c"])),
    ]);
    let config = new_sim_config_with(topology, |params| {
        params.committee_selection_algorithm = CommitteeSelectionAlgorithm::TopStakeSeats;
        params.committee_seat_count = 2;
        params.quorum_weight_fraction = 0.75;
    });

    // The unseated pool's 200 stake remains in the quorum denominator.
    assert_eq!(config.expected_total_weight, 1000);
    assert_eq!(config.vote_threshold(), 750);

    let mut sim = TestDriver::new_with_config(config);
    let pool_a = sim.id_for("pool-a");
    let pool_b = sim.id_for("pool-b");
    let pool_c = sim.id_for("pool-c");
    let relay = sim.id_for("relay");

    assert!(sim.config.vote_eligible_nodes.contains(&pool_a));
    assert!(sim.config.vote_eligible_nodes.contains(&pool_b));
    assert!(!sim.config.vote_eligible_nodes.contains(&pool_c));
    assert!(!sim.config.vote_eligible_nodes.contains(&relay));

    let txs: [_; 3] = sim.produce_txs(pool_a, false);
    for tx in &txs {
        for peer in [pool_b, pool_c, relay] {
            sim.expect_tx_sent(pool_a, peer, tx.clone());
        }
    }

    sim.win_next_rb_lottery(pool_a, 0);
    sim.next_slot();
    let (rb, eb) = sim.expect_cpu_task_matching(pool_a, is_new_rb_task);
    let eb = eb.expect("node did not produce EB");

    for peer in [pool_b, pool_c, relay] {
        sim.expect_rb_and_eb_sent(pool_a, peer, rb.clone(), Some(eb.clone()));
        sim.expect_eb_validated(peer, eb.clone());
    }

    sim.advance_time_to(sim.now() + (sim.config.header_diffusion_time * 3));

    // One body per pool, but its certificate weight is that pool's stake.
    let votes_a = sim.expect_cpu_task_matching(pool_a, is_new_vote_task);
    assert_eq!(*votes_a.ebs.first_key_value().unwrap().0, eb.id());
    assert_eq!(*votes_a.ebs.first_key_value().unwrap().1, 500);
    let votes_b = sim.expect_cpu_task_matching(pool_b, is_new_vote_task);
    assert_eq!(*votes_b.ebs.first_key_value().unwrap().1, 300);

    // The unseated pool and the relay stay silent.
    for (node, label) in [(pool_c, "pool-c"), (relay, "relay")] {
        let has_vote_task = sim
            .queued
            .get(&node)
            .map(|q| {
                q.tasks
                    .iter()
                    .any(|t| matches!(t, CpuTask::VTBundleGenerated(..)))
            })
            .unwrap_or(false);
        assert!(!has_vote_task, "{label} should not have generated a vote");
    }
}

/// The simulator uses node identifiers as its pool-ID surrogate.  Two pools of equal stake, one seat: the lower
/// identifier takes it.  Node ids follow the topology's name order, so
/// pool-a is id 0 and pool-b is id 1.
#[test]
fn top_stake_seats_breaks_ties_by_pool_id() {
    let topology = new_topology(vec![
        ("pool-a", new_node(Some(400), vec!["pool-b"])),
        ("pool-b", new_node(Some(400), vec!["pool-a"])),
    ]);
    let config = new_sim_config_with(topology, |params| {
        params.committee_selection_algorithm = CommitteeSelectionAlgorithm::TopStakeSeats;
        params.committee_seat_count = 1;
        params.quorum_weight_fraction = 0.75;
    });

    assert_eq!(config.expected_total_weight, 800);
    assert_eq!(config.vote_threshold(), 600);
    // Even unanimous approval by this 400-stake committee cannot certify.
    let seated_stake: u64 = config
        .nodes
        .iter()
        .filter(|n| config.vote_eligible_nodes.contains(&n.id))
        .map(|n| n.stake)
        .sum();
    assert!(seated_stake < config.vote_threshold());

    let sim = TestDriver::new_with_config(config);
    let pool_a = sim.id_for("pool-a");
    let pool_b = sim.id_for("pool-b");
    assert!(pool_a < pool_b, "test assumes pool-a has the lower id");
    assert!(sim.config.vote_eligible_nodes.contains(&pool_a));
    assert!(!sim.config.vote_eligible_nodes.contains(&pool_b));
}

/// Fewer pools than seats is the expected case, not an error: the
/// 1500-node pseudo-mainnet topology has 458 stake-holding nodes, so a
/// 900-seat request seats 458. Every pool is seated and the quorum
/// denominator remains total active stake.
#[test]
fn top_stake_seats_seats_every_pool_when_short() {
    let topology = new_topology(vec![
        (
            "pool-a",
            new_node(Some(500), vec!["pool-b", "pool-c", "relay"]),
        ),
        (
            "pool-b",
            new_node(Some(300), vec!["pool-a", "pool-c", "relay"]),
        ),
        (
            "pool-c",
            new_node(Some(200), vec!["pool-a", "pool-b", "relay"]),
        ),
        ("relay", new_node(None, vec!["pool-a", "pool-b", "pool-c"])),
    ]);
    let config = new_sim_config_with(topology, |params| {
        params.committee_selection_algorithm = CommitteeSelectionAlgorithm::TopStakeSeats;
        params.committee_seat_count = 900;
        params.quorum_weight_fraction = 0.75;
    });

    // Three pools hold stake, so three of the 900 seats are filled.
    assert_eq!(config.vote_eligible_nodes.len(), 3);
    assert_eq!(config.expected_total_weight, 1000);
    assert_eq!(config.vote_threshold(), 750);
    assert!(config.vote_threshold() <= config.expected_total_weight);

    let sim = TestDriver::new_with_config(config);
    for name in ["pool-a", "pool-b", "pool-c"] {
        assert!(
            sim.config.vote_eligible_nodes.contains(&sim.id_for(name)),
            "{name} should be seated"
        );
    }
    // A zero-stake relay is not a pool and never takes a seat.
    assert!(
        !sim.config
            .vote_eligible_nodes
            .contains(&sim.id_for("relay"))
    );
}

/// Zero seats means nobody can vote. Reject that configuration.
#[test]
fn sim_config_rejects_top_stake_seats_with_zero_seats() {
    let topology = new_topology(vec![
        ("pool-a", new_node(Some(600), vec!["pool-b"])),
        ("pool-b", new_node(Some(400), vec!["pool-a"])),
    ]);
    let mut params: crate::config::RawParameters =
        serde_yaml::from_slice(include_bytes!("../../../../parameters/config.default.yaml"))
            .unwrap();
    params.leios_variant = crate::config::LeiosVariant::LinearWithTxReferences;
    params.committee_selection_algorithm = CommitteeSelectionAlgorithm::TopStakeSeats;
    params.committee_seat_count = 0;

    let topology: crate::config::Topology = topology.into();
    let err = SimConfiguration::build(params, topology).unwrap_err();
    assert!(
        err.to_string().contains("committee-seat-count"),
        "expected a seat-count complaint, got: {err}"
    );
}

/// A two-node topology and default parameters, ready for a build that is
/// expected to succeed or fail on the committee mode alone.
fn seat_count_params(
    variant: LeiosVariant,
) -> (crate::config::RawParameters, crate::config::Topology) {
    let topology = new_topology(vec![
        ("pool-a", new_node(Some(600), vec!["pool-b"])),
        ("pool-b", new_node(Some(400), vec!["pool-a"])),
    ]);
    let mut params: crate::config::RawParameters =
        serde_yaml::from_slice(include_bytes!("../../../../parameters/config.default.yaml"))
            .unwrap();
    params.leios_variant = variant;
    params.committee_selection_algorithm = CommitteeSelectionAlgorithm::TopStakeSeats;
    params.committee_seat_count = 2;
    // One shard group, so `full-without-ibs` clears the unrelated sharding
    // check that runs before the committee mode is looked at and the build
    // fails, or succeeds, on the committee mode alone.
    params.ib_shard_group_count = 1;
    (params, topology.into())
}

/// `top-stake-seats` seats a committee and sets a stake-based quorum
/// denominator, but only the linear node reads that seating when it decides
/// whether, and with what weight, it votes.  Every other variant runs its own
/// lottery and never looks at it, so the votes it casts are measured against
/// a denominator nobody voted into and the certification figures mean
/// nothing.  Rejecting only shared-consensus left four variants that built
/// fine, ran, and reported those figures without a word.
#[test]
fn sim_config_rejects_top_stake_seats_for_variants_that_ignore_it() {
    for variant in [
        LeiosVariant::Short,
        LeiosVariant::Full,
        LeiosVariant::FullWithoutIbs,
        LeiosVariant::FullWithTxReferences,
        LeiosVariant::SharedConsensus,
    ] {
        let (params, topology) = seat_count_params(variant);
        let err = SimConfiguration::build(params, topology).unwrap_err();
        assert!(
            err.to_string().contains("top-stake-seats"),
            "{variant:?} ignores the seating, so pairing it with top-stake-seats \
             has to be an error; got: {err}"
        );
    }
}

/// ...and the two that do honour it still build, so the guard rejects the
/// pairing rather than the mode.
#[test]
fn sim_config_accepts_top_stake_seats_for_the_linear_variants() {
    for variant in [LeiosVariant::Linear, LeiosVariant::LinearWithTxReferences] {
        let (params, topology) = seat_count_params(variant);
        let config = SimConfiguration::build(params, topology)
            .unwrap_or_else(|e| panic!("{variant:?} honours the seating and must build: {e}"));
        assert_eq!(config.expected_total_weight, 1000, "{variant:?}");
    }
}

/// `vote-diffusion-strategy` is the Haskell simulator's request-ordering key
/// and is never read here; `vote-transport` is the one this simulator reads.
/// The default config used to carry the transport's whole explanation above
/// the former, and to promise that the former's values are accepted as
/// aliases of `announce-then-request`.  They are not -- `VoteTransport`
/// declares no aliases -- so a reader who followed that comment got a
/// deserialization error.  Both halves of the claim are pinned here so the
/// documentation cannot drift back onto the wrong key unnoticed.
#[test]
fn the_request_ordering_key_is_not_the_vote_transport() {
    for value in ["peer-order", "freshest-first", "oldest-first"] {
        assert!(
            serde_yaml::from_str::<VoteTransport>(value).is_err(),
            "{value} belongs to vote-diffusion-strategy; vote-transport has no such alias, \
             and documenting one sends readers into a parse error"
        );
    }
    for value in ["announce-then-request", "push", "push-late-dedupe"] {
        serde_yaml::from_str::<VoteTransport>(value)
            .unwrap_or_else(|e| panic!("{value} is a documented transport and must parse: {e}"));
    }

    // And the Haskell key has no say over the transport, whatever it is set to.
    let mut config: serde_yaml::Value =
        serde_yaml::from_slice(include_bytes!("../../../../parameters/config.default.yaml"))
            .unwrap();
    let key = serde_yaml::Value::from("vote-diffusion-strategy");
    assert!(
        config.get(&key).is_some(),
        "the default config still ships the Haskell key this test is about"
    );
    config
        .as_mapping_mut()
        .unwrap()
        .insert(key, serde_yaml::Value::from("oldest-first"));
    let params: crate::config::RawParameters = serde_yaml::from_value(config).unwrap();
    assert_eq!(
        params.vote_transport,
        VoteTransport::AnnounceThenRequest,
        "moving the Haskell simulator's request-ordering key must not move this \
         simulator's vote transport"
    );
}

/// The default strategy sends the 8-byte id and waits to be asked for the
/// body.  The body must never go out unsolicited, and the historic
/// announce/request/deliver exchange must still deliver it.
#[test]
fn announce_then_request_should_not_push_vote_bundle() {
    let topology = new_topology(vec![
        ("node-1", new_node(Some(1000), vec!["node-2"])),
        ("node-2", new_node(Some(1000), vec!["node-1"])),
    ]);
    let mut sim = TestDriver::new(topology);
    let node1 = sim.id_for("node-1");
    let node2 = sim.id_for("node-2");
    assert_eq!(
        sim.config.vote_transport,
        VoteTransport::AnnounceThenRequest,
        "announce-then-request should be the default strategy"
    );

    let _ = sim.drain_tracked_events();
    let votes = sim.produce_vote_bundle(node1);

    // The body is not queued: node 2 has to ask for it first.
    sim.expect_no_message(node1, node2, Message::Votes(votes.clone()));
    sim.expect_vote_bundle_sent(node1, node2, votes.clone());

    // Delivering one body over this transport costs three messages, not one.
    // Only the body was ever counted, so the arm that spends the most
    // messages was the one that appeared to spend the fewest.
    let events = sim.drain_tracked_events();
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Event::VTBundleAnnouncementReceived { .. }))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Event::VTBundleRequestReceived { .. }))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Event::VTBundleAccepted { .. }))
            .count(),
        1
    );
    assert_eq!(
        vote_wire_reports(&events),
        vec![
            ("announce", node1, node2, 8),
            ("request", node2, node1, 8),
            ("body", node1, node2, votes.bytes),
        ]
    );
}

/// The push arms send the body and nothing else, so their message count is
/// their body count.  That is the comparison the announcement and request
/// counters exist to make honest.
#[test]
fn push_should_send_no_control_messages() {
    let topology = new_topology(vec![
        ("node-1", new_node(Some(1000), vec!["node-2"])),
        ("node-2", new_node(Some(1000), vec!["node-1"])),
    ]);
    let config = new_sim_config_with(topology, |params| {
        params.vote_transport = VoteTransport::Push;
    });
    let mut sim = TestDriver::new_with_config(config);
    let node1 = sim.id_for("node-1");
    let node2 = sim.id_for("node-2");

    let _ = sim.drain_tracked_events();
    let votes = sim.produce_vote_bundle(node1);
    sim.expect_message(node1, node2, Message::Votes(votes.clone()));

    assert_eq!(
        vote_wire_reports(&sim.drain_tracked_events()),
        vec![("body", node1, node2, votes.bytes)]
    );
}

/// Under `push` the body itself is the first thing a peer sees.  There is no
/// announcement to answer and no request round-trip.
#[test]
fn push_should_send_vote_bundle_body_directly() {
    let topology = new_topology(vec![
        ("node-1", new_node(Some(1000), vec!["node-2"])),
        ("node-2", new_node(Some(1000), vec!["node-1"])),
    ]);
    let config = new_sim_config_with(topology, |params| {
        params.vote_transport = VoteTransport::Push;
    });
    let mut sim = TestDriver::new_with_config(config);
    let node1 = sim.id_for("node-1");
    let node2 = sim.id_for("node-2");

    let votes = sim.produce_vote_bundle(node1);

    sim.expect_no_message(node1, node2, Message::AnnounceVotes(votes.id));
    sim.expect_message(node1, node2, Message::Votes(votes.clone()));
    sim.expect_cpu_task(node2, CpuTask::VTBundleValidated(node1, votes));
}

/// A pushed bundle is forwarded once it validates, but never back down the
/// link it arrived on.
#[test]
fn push_should_forward_vote_bundle_but_not_to_source() {
    // A line, so node 2 has one consumer besides the peer it hears from.
    let topology = new_topology(vec![
        ("node-1", new_node(Some(1000), vec!["node-2"])),
        ("node-2", new_node(Some(1000), vec!["node-1", "node-3"])),
        ("node-3", new_node(Some(1000), vec!["node-2"])),
    ]);
    let config = new_sim_config_with(topology, |params| {
        params.vote_transport = VoteTransport::Push;
    });
    let mut sim = TestDriver::new_with_config(config);
    let node1 = sim.id_for("node-1");
    let node2 = sim.id_for("node-2");
    let node3 = sim.id_for("node-3");

    let votes = sim.produce_vote_bundle(node1);

    // Node 1 pushes to its only consumer, which validates the bundle.
    sim.expect_message(node1, node2, Message::Votes(votes.clone()));
    sim.expect_cpu_task(node2, CpuTask::VTBundleValidated(node1, votes.clone()));

    // Node 2 passes it on to node 3, and does not echo it back to node 1.
    sim.expect_no_message(node2, node1, Message::Votes(votes.clone()));
    sim.expect_message(node2, node3, Message::Votes(votes));
}

/// The Haskell node's notify server has no per-peer provenance, so it sends
/// a bundle straight back to the peer it came from.  Setting
/// `vote-diffusion-echo-to-source` reproduces that waste so it can be
/// measured.
#[test]
fn push_should_echo_vote_bundle_to_source_when_configured() {
    let topology = new_topology(vec![
        ("node-1", new_node(Some(1000), vec!["node-2"])),
        ("node-2", new_node(Some(1000), vec!["node-1", "node-3"])),
        ("node-3", new_node(Some(1000), vec!["node-2"])),
    ]);
    let config = new_sim_config_with(topology, |params| {
        params.vote_transport = VoteTransport::Push;
        params.vote_transport_echo_to_source = true;
    });
    let mut sim = TestDriver::new_with_config(config);
    let node1 = sim.id_for("node-1");
    let node2 = sim.id_for("node-2");
    let node3 = sim.id_for("node-3");

    let votes = sim.produce_vote_bundle(node1);

    sim.expect_message(node1, node2, Message::Votes(votes.clone()));
    sim.expect_cpu_task(node2, CpuTask::VTBundleValidated(node1, votes.clone()));

    // Every consumer gets the body, the source peer included.  Node 1 already
    // holds the bundle, so its copy is dropped on arrival.
    sim.expect_message(node2, node3, Message::Votes(votes.clone()));
    sim.expect_message(node2, node1, Message::Votes(votes));
}

/// The echo models the Haskell node's notify server, which pushes bodies and
/// has no per-peer provenance to exclude the sender with.
/// `announce-then-request` has no such server: it would re-announce to the
/// peer that just handed it the body, buying a request and a second copy of
/// a bundle it is already holding.  That is not the gap the flag exists to
/// measure, and `announce-then-request` is the published baseline, so the
/// flag leaves it exactly as it was.
#[test]
fn announce_then_request_should_ignore_the_echo_flag() {
    let topology = new_topology(vec![
        ("node-1", new_node(Some(1000), vec!["node-2"])),
        ("node-2", new_node(Some(1000), vec!["node-1", "node-3"])),
        ("node-3", new_node(Some(1000), vec!["node-2"])),
    ]);
    let config = new_sim_config_with(topology, |params| {
        params.vote_transport_echo_to_source = true;
    });
    let mut sim = TestDriver::new_with_config(config);
    let node1 = sim.id_for("node-1");
    let node2 = sim.id_for("node-2");
    let node3 = sim.id_for("node-3");
    assert_eq!(
        sim.config.vote_transport,
        VoteTransport::AnnounceThenRequest,
        "the flag is set on the baseline transport, which is what it must not change"
    );

    let _ = sim.drain_tracked_events();
    let votes = sim.produce_vote_bundle(node1);
    sim.expect_vote_bundle_sent(node1, node2, votes.clone());

    // Node 2 announces onwards to node 3 and says nothing back to node 1.
    sim.expect_no_message(node2, node1, Message::AnnounceVotes(votes.id));
    sim.expect_no_message(node2, node1, Message::Votes(votes.clone()));
    assert_eq!(
        vote_wire_reports(&sim.drain_tracked_events()),
        vec![
            ("announce", node1, node2, 8),
            ("request", node2, node1, 8),
            ("body", node1, node2, votes.bytes),
            ("announce", node2, node3, 8),
        ],
        "one announcement onwards and nothing back down the link it came from"
    );
}

/// A second copy of a bundle a node already holds costs only the bytes: it is
/// not validated again and it is not forwarded again.
#[test]
fn push_should_drop_duplicate_vote_bundle() {
    // A triangle, so the bundle reaches node 2 twice: once from its producer
    // and once by way of node 3.
    let topology = new_topology(vec![
        ("node-1", new_node(Some(1000), vec!["node-2", "node-3"])),
        ("node-2", new_node(Some(1000), vec!["node-1", "node-3"])),
        ("node-3", new_node(Some(1000), vec!["node-1", "node-2"])),
    ]);
    let config = new_sim_config_with(topology, |params| {
        params.vote_transport = VoteTransport::Push;
    });
    let mut sim = TestDriver::new_with_config(config);
    let node1 = sim.id_for("node-1");
    let node2 = sim.id_for("node-2");
    let node3 = sim.id_for("node-3");

    let votes = sim.produce_vote_bundle(node1);

    // Node 1 pushes to both peers, and each forwards to the other.
    sim.expect_message(node1, node2, Message::Votes(votes.clone()));
    sim.expect_cpu_task(node2, CpuTask::VTBundleValidated(node1, votes.clone()));
    sim.expect_message(node1, node3, Message::Votes(votes.clone()));
    sim.expect_cpu_task(node3, CpuTask::VTBundleValidated(node1, votes.clone()));

    // Drain node 2's forward so anything queued to node 3 afterwards is new.
    sim.expect_message(node2, node3, Message::Votes(votes.clone()));

    // Node 3's copy now lands on node 2, which already holds the bundle.
    let _ = sim.drain_tracked_events();
    sim.expect_message(node3, node2, Message::Votes(votes.clone()));

    let events = sim.drain_tracked_events();
    assert_eq!(
        duplicate_reports(&events),
        vec![(node3, node2)],
        "dropping a duplicate has to be reported, or the arm looks cheaper than it is"
    );
    assert_eq!(
        sim.queued_vote_validations(node2),
        0,
        "duplicate vote bundle should not be validated a second time"
    );
    sim.expect_no_message(node2, node1, Message::Votes(votes.clone()));
    sim.expect_no_message(node2, node3, Message::Votes(votes));
}

/// `push-late-dedupe` marks a bundle only once it is fully held, so a copy
/// that arrives while the first one is still being verified is verified too.
/// That second signature check is the whole difference between this arm and
/// `push`: identical delivery, identical bytes, more CPU.  It has to show up
/// as a duplicate, and it must not be forwarded a second time.
#[test]
fn push_late_dedupe_should_verify_a_duplicate_that_arrives_while_validating() {
    // A triangle, so the bundle reaches node 2 twice: once from its producer
    // and once by way of node 3.
    let topology = new_topology(vec![
        ("node-1", new_node(Some(1000), vec!["node-2", "node-3"])),
        ("node-2", new_node(Some(1000), vec!["node-1", "node-3"])),
        ("node-3", new_node(Some(1000), vec!["node-1", "node-2"])),
    ]);
    let config = new_sim_config_with(topology, |params| {
        params.vote_transport = VoteTransport::PushLateDedupe;
    });
    let mut sim = TestDriver::new_with_config(config);
    let node1 = sim.id_for("node-1");
    let node2 = sim.id_for("node-2");
    let node3 = sim.id_for("node-3");

    let votes = sim.produce_vote_bundle(node1);

    // Node 1 pushes to both peers.  Node 2's copy is left in flight: its
    // verification is queued and deliberately not run yet.
    sim.expect_message(node1, node2, Message::Votes(votes.clone()));
    sim.expect_message(node1, node3, Message::Votes(votes.clone()));
    sim.expect_cpu_task(node3, CpuTask::VTBundleValidated(node1, votes.clone()));

    // Node 3 forwards its copy to node 2, which is still validating the first.
    let _ = sim.drain_tracked_events();
    sim.expect_message(node3, node2, Message::Votes(votes.clone()));
    assert_eq!(
        sim.queued_vote_validations(node2),
        2,
        "late dedupe pays for a copy that arrives inside the validation window"
    );
    assert!(
        duplicate_reports(&sim.drain_tracked_events()).is_empty(),
        "nothing is known to be redundant yet: the arrival is on its way to being verified"
    );

    // The first copy is accepted and passed on.
    sim.expect_cpu_task(node2, CpuTask::VTBundleValidated(node1, votes.clone()));
    sim.expect_message(node2, node3, Message::Votes(votes.clone()));

    // The second was verified for nothing.  It is reported once it is known
    // to be redundant, and it is not forwarded again.
    let _ = sim.drain_tracked_events();
    sim.expect_cpu_task(node2, CpuTask::VTBundleValidated(node3, votes.clone()));
    assert_eq!(
        duplicate_reports(&sim.drain_tracked_events()),
        vec![(node3, node2)],
        "a verification spent on a bundle we already had is the cost this arm exists to measure"
    );
    sim.expect_no_message(node2, node1, Message::Votes(votes));
}

/// Once a `push-late-dedupe` node fully holds a bundle it behaves like
/// `push`: a further copy costs the bytes and nothing else.
#[test]
fn push_late_dedupe_should_drop_a_duplicate_it_already_holds() {
    let topology = new_topology(vec![
        ("node-1", new_node(Some(1000), vec!["node-2", "node-3"])),
        ("node-2", new_node(Some(1000), vec!["node-1", "node-3"])),
        ("node-3", new_node(Some(1000), vec!["node-1", "node-2"])),
    ]);
    let config = new_sim_config_with(topology, |params| {
        params.vote_transport = VoteTransport::PushLateDedupe;
    });
    let mut sim = TestDriver::new_with_config(config);
    let node1 = sim.id_for("node-1");
    let node2 = sim.id_for("node-2");
    let node3 = sim.id_for("node-3");

    let votes = sim.produce_vote_bundle(node1);

    // Both peers take the bundle all the way, so each fully holds it.
    sim.expect_message(node1, node2, Message::Votes(votes.clone()));
    sim.expect_cpu_task(node2, CpuTask::VTBundleValidated(node1, votes.clone()));
    sim.expect_message(node1, node3, Message::Votes(votes.clone()));
    sim.expect_cpu_task(node3, CpuTask::VTBundleValidated(node1, votes.clone()));

    // Node 3's forward lands on a node that is done with this bundle.
    let _ = sim.drain_tracked_events();
    sim.expect_message(node3, node2, Message::Votes(votes.clone()));
    assert_eq!(
        duplicate_reports(&sim.drain_tracked_events()),
        vec![(node3, node2)]
    );
    assert_eq!(
        sim.queued_vote_validations(node2),
        0,
        "a bundle we already hold is recognised before it costs a signature check"
    );
}

/// Under `relay-strategy: request-from-all` a node asks every peer that
/// announces a bundle, so the announce-then-request path is handed several
/// bodies, verifies each of them and keeps one.  The copies it did not need
/// are as real as any pushed duplicate and are reported the same way.
#[test]
fn request_from_all_should_report_the_body_it_verified_and_did_not_need() {
    let topology = new_topology(vec![
        ("node-1", new_node(Some(1000), vec!["node-2", "node-3"])),
        ("node-2", new_node(Some(1000), vec!["node-1", "node-3"])),
        ("node-3", new_node(Some(1000), vec!["node-1", "node-2"])),
    ]);
    let config = new_sim_config_with(topology, |params| {
        params.relay_strategy = RelayStrategy::RequestFromAll;
    });
    assert_eq!(
        config.vote_transport,
        VoteTransport::AnnounceThenRequest,
        "this is the announce path, not a push arm"
    );
    let mut sim = TestDriver::new_with_config(config);
    let node1 = sim.id_for("node-1");
    let node2 = sim.id_for("node-2");
    let node3 = sim.id_for("node-3");

    let votes = sim.produce_vote_bundle(node1);

    // Node 2 goes first, so that it holds the body and can serve it on.
    sim.expect_message(node1, node2, Message::AnnounceVotes(votes.id));
    sim.expect_message(node2, node1, Message::RequestVotes(votes.id));
    sim.expect_message(node1, node2, Message::Votes(votes.clone()));
    sim.expect_cpu_task(node2, CpuTask::VTBundleValidated(node1, votes.clone()));

    // Node 3 hears about the bundle from both peers before either answers,
    // and asks both.
    sim.expect_message(node1, node3, Message::AnnounceVotes(votes.id));
    sim.expect_message(node2, node3, Message::AnnounceVotes(votes.id));
    sim.expect_message(node3, node1, Message::RequestVotes(votes.id));
    sim.expect_message(node3, node2, Message::RequestVotes(votes.id));

    // Two bodies arrive, and both are verified.
    let _ = sim.drain_tracked_events();
    sim.expect_message(node1, node3, Message::Votes(votes.clone()));
    sim.expect_message(node2, node3, Message::Votes(votes.clone()));
    assert_eq!(
        sim.queued_vote_validations(node3),
        2,
        "each answer to a request is verified on arrival"
    );
    assert!(
        duplicate_reports(&sim.drain_tracked_events()).is_empty(),
        "an arrival is only redundant once something else has been accepted"
    );

    sim.expect_cpu_task(node3, CpuTask::VTBundleValidated(node1, votes.clone()));
    let _ = sim.drain_tracked_events();
    sim.expect_cpu_task(node3, CpuTask::VTBundleValidated(node2, votes.clone()));
    assert_eq!(
        duplicate_reports(&sim.drain_tracked_events()),
        vec![(node2, node3)],
        "the announce path does deliver a body twice under request-from-all"
    );
}

/// A node reports the moment its own tally of votes for an EB crosses the
/// quorum threshold, once, so the summary can measure how long a quorum
/// takes to form.  The test config takes two votes to certify.
#[test]
fn quorum_should_be_reported_when_the_tally_crosses_the_threshold() {
    let topology = new_topology(vec![
        ("node-1", new_node(Some(1000), vec!["node-2"])),
        ("node-2", new_node(Some(1000), vec!["node-1"])),
    ]);
    let mut sim = TestDriver::new(topology);
    let node1 = sim.id_for("node-1");
    let node2 = sim.id_for("node-2");
    assert_eq!(sim.config.vote_threshold(), 2);

    let txs: [_; 3] = sim.produce_txs(node1, false);
    for tx in &txs {
        sim.expect_tx_sent(node1, node2, tx.clone());
    }

    sim.win_next_vote_lottery(node1, 0);
    sim.win_next_vote_lottery(node2, 0);
    sim.win_next_rb_lottery(node1, 0);
    sim.next_slot();
    let (rb, eb) = sim.expect_cpu_task_matching(node1, is_new_rb_task);
    let eb = eb.expect("node did not produce EB");
    sim.expect_rb_and_eb_sent(node1, node2, rb, Some(eb.clone()));
    sim.expect_eb_validated(node2, eb.clone());
    sim.advance_time_to(sim.now() + (sim.config.header_diffusion_time * 3));

    // One vote each.  Neither node has a quorum from its own vote alone.
    let _ = sim.drain_tracked_events();
    let votes_1 = sim.expect_cpu_task_matching(node1, is_new_vote_task);
    let votes_2 = sim.expect_cpu_task_matching(node2, is_new_vote_task);
    assert!(
        quorum_reports(&sim.drain_tracked_events()).is_empty(),
        "one vote out of the two it takes is not a quorum"
    );

    // Node 1 counts node 2's vote on top of its own and has a quorum.
    sim.expect_vote_bundle_sent(node2, node1, votes_2);
    assert_eq!(
        quorum_reports(&sim.drain_tracked_events()),
        vec![(node1, 2)],
        "the second vote is the one that crosses the threshold"
    );

    // Node 2 gets there too, and neither reports a crossing twice.
    sim.expect_vote_bundle_sent(node1, node2, votes_1);
    assert_eq!(
        quorum_reports(&sim.drain_tracked_events()),
        vec![(node2, 2)]
    );
}

/// Instrument both arrival after pruning and validation spanning a prune,
/// preserving all existing CPU work and forwarding, without new acceptances.
#[test]
fn obsolete_vote_telemetry_preserves_transport_work() {
    for transport in [
        VoteTransport::AnnounceThenRequest,
        VoteTransport::Push,
        VoteTransport::PushLateDedupe,
    ] {
        for queued_before_prune in [false, true] {
            check_obsolete_vote(transport, queued_before_prune);
        }
    }
}

fn check_obsolete_vote(transport: VoteTransport, queued_before_prune: bool) {
    let topology = new_topology(vec![
        ("node-1", new_node(Some(1000), vec!["node-2", "node-3"])),
        ("node-2", new_node(Some(1000), vec!["node-1", "node-3"])),
        ("node-3", new_node(Some(1000), vec!["node-1", "node-2"])),
    ]);
    // One vote certifies, so a single late bundle is enough to re-cross the
    // threshold on a tally that has been reset to zero.
    let config = new_sim_config_with(topology, |params| {
        params.quorum_weight_fraction = 0.5;
        params.vote_transport = transport;
    });
    let mut sim = TestDriver::new_with_config(config);
    let node1 = sim.id_for("node-1");
    let node2 = sim.id_for("node-2");
    let node3 = sim.id_for("node-3");
    assert_eq!(sim.config.vote_threshold(), 1);

    let txs: [_; 3] = sim.produce_txs(node1, false);
    for tx in &txs {
        sim.expect_tx_sent(node1, node2, tx.clone());
        sim.expect_tx_sent(node1, node3, tx.clone());
    }

    // Node 1 mints the EB; all voters wait out the same gate.
    sim.win_next_rb_lottery(node1, 0);
    sim.win_next_vote_lottery(node1, 0);
    sim.next_slot();
    let (rb, eb) = sim.expect_cpu_task_matching(node1, is_new_rb_task);
    let eb = eb.expect("node did not produce EB");
    sim.expect_rb_and_eb_sent(node1, node2, rb.clone(), Some(eb.clone()));
    sim.expect_eb_validated(node2, eb.clone());
    // Node 3 will vote at the gate, so its lottery is armed before it can
    // draw.
    sim.win_next_vote_lottery(node3, 0);
    sim.expect_rb_and_eb_sent(node1, node3, rb, Some(eb.clone()));
    sim.expect_eb_validated(node3, eb.clone());

    sim.advance_time_to(sim.config.voting_window().gate_at(eb.slot));
    let votes_1 = sim.expect_cpu_task_matching(node1, is_new_vote_task);
    // Node 1's vote is all it takes: node 2 has a quorum, and says so once.
    let _ = sim.drain_tracked_events();
    sim.expect_vote_bundle_sent(node1, node2, votes_1);
    assert_eq!(
        quorum_reports(&sim.drain_tracked_events()),
        vec![(node2, 1)],
        "one vote is the threshold here, so the first one crosses it"
    );

    // Keep node 3's vote either in transit or queued for validation.
    sim.advance_time_to(sim.config.voting_window().gate_at(eb.slot));
    let votes_3 = sim.expect_cpu_task_matching(node3, is_new_vote_task);
    assert_eq!(*votes_3.ebs.first_key_value().unwrap().0, eb.id());

    if queued_before_prune {
        sim.deliver_vote_bundle(node3, node2, votes_3.clone());
        assert_eq!(sim.queued_vote_validations(node2), 1);
    }

    // Node 2 certifies the EB in a ranking block of its own, which is what
    // makes the EB old news and prunes its tally.
    sim.advance_time_to(sim.config.voting_window().inclusion_deadline_at(eb.slot));
    sim.win_next_rb_lottery(node2, 0);
    sim.next_slot();
    let (rb_2, _) = sim.expect_cpu_task_matching(node2, is_new_rb_task);
    assert_eq!(
        rb_2.endorsement.as_ref().map(|e| e.eb),
        Some(eb.id()),
        "the prune only happens once the EB is endorsed on-chain, so a block \
         without the endorsement would leave this test asserting nothing"
    );

    sim.drain_tracked_events();
    if !queued_before_prune {
        sim.deliver_vote_bundle(node3, node2, votes_3.clone());
    }
    assert_eq!(
        sim.queued_vote_validations(node2),
        1,
        "measurement must preserve verification"
    );
    sim.expect_cpu_task(node2, CpuTask::VTBundleValidated(node3, votes_3.clone()));
    let events = sim.drain_tracked_events();
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Event::VTBundleAccepted { .. }))
    );
    assert_eq!(duplicate_reports(&events), vec![(node3, node2)]);
    assert!(quorum_reports(&events).is_empty());
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Event::VTBundleObsoleteReceived { .. }))
            .count(),
        usize::from(!queued_before_prune)
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(
                e,
                Event::VTBundleObsoleteValidated {
                    already_held: false,
                    ..
                }
            ))
            .count(),
        1
    );
    let outgoing = vote_wire_reports(&events);
    let forwarded: Vec<_> = outgoing
        .iter()
        .filter(|(kind, sender, _, _)| *sender == node2 && (*kind == "body" || *kind == "announce"))
        .collect();
    assert_eq!(forwarded.len(), 1, "measurement must preserve forwarding");
    let bytes = forwarded[0].3;
    assert!(events.iter().any(|e| matches!(e, Event::VTBundleObsoleteSent { node, bodies, announcements, msg_size_bytes, .. }
        if node.id == node2 && *bodies == u64::from(transport.is_push()) && *announcements == u64::from(!transport.is_push()) && *msg_size_bytes == bytes)));

    // Keep the cached copy and account for a later body request as well.
    let response = sim
        .nodes
        .get_mut(&node2)
        .unwrap()
        .handle_message(node1, Message::RequestVotes(votes_3.id));
    assert_eq!(
        response.messages.len(),
        1,
        "measurement must preserve the held copy"
    );
    let events = sim.drain_tracked_events();
    assert!(events.iter().any(|e| matches!(e, Event::VTBundleObsoleteSent { bodies: 1, announcements: 0, msg_size_bytes, .. } if *msg_size_bytes == votes_3.bytes)));

    // An already-held obsolete arrival still follows its original transport's
    // deduplication order: push skips verification; pull discovers it after CPU.
    let response = sim
        .nodes
        .get_mut(&node2)
        .unwrap()
        .handle_message(node3, Message::Votes(votes_3.clone()));
    assert_eq!(response.tasks.len(), usize::from(!transport.is_push()));
    sim.process_events(node2, response);
    if !transport.is_push() {
        sim.expect_cpu_task(node2, CpuTask::VTBundleValidated(node3, votes_3));
    }
    let events = sim.drain_tracked_events();
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Event::VTBundleObsoleteReceived { .. }))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(
                e,
                Event::VTBundleObsoleteValidated {
                    already_held: true,
                    ..
                }
            ))
            .count(),
        usize::from(!transport.is_push())
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Event::VTBundleObsoleteSent { .. }))
    );
}

#[test]
fn voting_window_is_anchored_on_the_announcing_slot() {
    let topology = new_topology(vec![
        ("node-1", new_node(Some(1000), vec!["node-2"])),
        ("node-2", new_node(Some(1000), vec!["node-1"])),
    ]);
    let config = new_sim_config(topology);
    let window = config.voting_window();
    // CIP-0164 states every one of these as an offset from the slot of
    // the ranking block that announced the EB, and nothing is measured
    // from the EB being generated -- the EB has no slot of its own.
    assert_eq!(window.header_deadline, config.header_diffusion_time);
    assert_eq!(window.gate, config.header_diffusion_time * 3);
    assert_eq!(
        window.deadline,
        window.gate + Duration::from_secs(config.linear_vote_stage_length)
    );
    assert_eq!(
        window.inclusion_deadline,
        window.deadline + Duration::from_secs(config.linear_diffuse_stage_length)
    );
    let t0 = Timestamp::from_secs(70);
    assert_eq!(window.header_deadline_at(70), t0 + window.header_deadline);
    assert_eq!(window.gate_at(70), t0 + window.gate);
    assert_eq!(window.deadline_at(70), t0 + window.deadline);
    assert_eq!(
        window.inclusion_deadline_at(70),
        t0 + window.inclusion_deadline
    );
}

#[test]
fn no_vote_is_cast_before_the_gate() {
    let topology = new_topology(vec![
        ("node-1", new_node(Some(1000), vec!["node-2"])),
        ("node-2", new_node(Some(1000), vec!["node-1"])),
    ]);
    let mut sim = TestDriver::new(topology);
    let node1 = sim.id_for("node-1");
    let node2 = sim.id_for("node-2");

    let txs: [_; 3] = sim.produce_txs(node1, false);
    for tx in &txs {
        sim.expect_tx_sent(node1, node2, tx.clone());
    }

    sim.win_next_rb_lottery(node1, 0);
    sim.win_next_vote_lottery(node1, 0);
    sim.next_slot();
    let (rb, eb) = sim.expect_cpu_task_matching(node1, is_new_rb_task);
    let eb = eb.expect("node did not produce EB");
    sim.expect_rb_and_eb_sent(node1, node2, rb, Some(eb.clone()));
    // The lottery has to be armed before the EB validates, not after.  A node
    // draws the moment it decides it may vote, so with the gate deleted
    // altogether node 2 would draw on validation -- and lose, on an empty
    // queue -- and every assertion below would still pass.  Arming it first
    // means the only thing standing between node 2 and a vote is the gate,
    // which is what this test is for.
    sim.win_next_vote_lottery(node2, 0);
    sim.expect_eb_validated(node2, eb.clone());
    assert_eq!(
        sim.queued_vote_generations(node2),
        0,
        "validating the EB must not itself produce a vote: the gate has not passed"
    );

    // The EB is validated and the lottery is won, so the only thing left
    // between node 2 and a vote is the equivocation-detection period.
    assert_eq!(
        sim.queued_vote_generations(node1),
        0,
        "producer must also wait"
    );
    let gate = sim.config.voting_window().gate_at(eb.slot);
    sim.advance_time_to(gate - Duration::from_millis(1));
    assert_eq!(
        sim.queued_vote_generations(node2),
        0,
        "a vote a millisecond before t0 + 3 * L_hdr would be one no honest node may cast"
    );
    assert_eq!(sim.queued_vote_generations(node1), 0);
    sim.advance_time_to(gate);
    assert_eq!(sim.queued_vote_generations(node1), 1);
    assert_eq!(
        sim.queued_vote_generations(node2),
        1,
        "and the gate is exactly when it may"
    );
}

/// A star, so the centre has `spokes` consumers to choose between.
fn new_star_topology(spokes: usize) -> RawTopology {
    let mut nodes = vec![("node-1", new_node(Some(1000), vec![]))];
    for name in SPOKE_NAMES.iter().take(spokes) {
        nodes.push((*name, new_node(Some(1000), vec!["node-1"])));
    }
    new_topology(nodes)
}

const SPOKE_NAMES: [&str; 6] = ["node-2", "node-3", "node-4", "node-5", "node-6", "node-7"];

/// Who did `from` push a vote body to, without consuming the messages?
fn vote_push_targets(sim: &TestDriver, from: NodeId) -> std::collections::BTreeSet<NodeId> {
    sim.queued
        .get(&from)
        .map(|q| {
            q.messages
                .iter()
                .filter(|(_, m)| matches!(m, Message::Votes(_)))
                .map(|(to, _)| *to)
                .collect()
        })
        .unwrap_or_default()
}

/// Bounded fanout pushes to `vote-push-fanout` peers, not to every consumer.
/// This is the whole point of the arm: the duplicate flood is proportional
/// to the fanout, so capping it is what removes the redundant verifications.
#[test]
fn bounded_fanout_pushes_to_exactly_the_configured_number_of_peers() {
    let config = new_sim_config_with(new_star_topology(4), |params| {
        params.vote_transport = VoteTransport::Push;
        params.vote_push_fanout = Some(2);
    });
    let mut sim = TestDriver::new_with_config(config);
    let node1 = sim.id_for("node-1");

    sim.produce_vote_bundle(node1);

    let targets = vote_push_targets(&sim, node1);
    assert_eq!(
        targets.len(),
        2,
        "expected the body on exactly 2 of 4 links, got {targets:?}"
    );
}

/// Asking for more peers than exist is not an error and not a truncation:
/// every consumer gets the body, exactly as with no limit at all.
#[test]
fn fanout_larger_than_the_peer_count_pushes_to_every_consumer() {
    let config = new_sim_config_with(new_star_topology(4), |params| {
        params.vote_transport = VoteTransport::Push;
        params.vote_push_fanout = Some(10);
    });
    let mut sim = TestDriver::new_with_config(config);
    let node1 = sim.id_for("node-1");

    sim.produce_vote_bundle(node1);

    assert_eq!(vote_push_targets(&sim, node1).len(), 4);
}

/// The subset is a pure function of the seed, so two runs of the same
/// configuration choose the same peers.  Without this the arm could not be
/// compared against any other, because each run would diffuse differently.
#[test]
fn bounded_fanout_chooses_the_same_peers_on_every_run() {
    let choose = || {
        let config = new_sim_config_with(new_star_topology(4), |params| {
            params.vote_transport = VoteTransport::Push;
            params.vote_push_fanout = Some(2);
        });
        let mut sim = TestDriver::new_with_config(config);
        let node1 = sim.id_for("node-1");
        sim.produce_vote_bundle(node1);
        vote_push_targets(&sim, node1)
    };
    assert_eq!(choose(), choose());
}

/// Turning the limit off restores the unbounded behaviour byte for byte,
/// so the default arm is unaffected by this parameter existing.
#[test]
fn no_fanout_limit_pushes_to_every_consumer() {
    let config = new_sim_config_with(new_star_topology(4), |params| {
        params.vote_transport = VoteTransport::Push;
        params.vote_push_fanout = None;
    });
    let mut sim = TestDriver::new_with_config(config);
    let node1 = sim.id_for("node-1");

    sim.produce_vote_bundle(node1);

    assert_eq!(vote_push_targets(&sim, node1).len(), 4);
}

/// A limit that cannot take effect is rejected rather than ignored: a run
/// that quietly used the default would still print numbers, and nothing in
/// the output would say the limit was dropped.
#[test]
fn sim_config_rejects_a_fanout_limit_that_could_not_apply() {
    // (name, transport, fanout, variant, expected error fragment)
    let cases = [
        (
            "zero",
            VoteTransport::Push,
            0u64,
            crate::config::LeiosVariant::LinearWithTxReferences,
            "vote-push-fanout is 0",
        ),
        (
            "not a push transport",
            VoteTransport::AnnounceThenRequest,
            2,
            crate::config::LeiosVariant::LinearWithTxReferences,
            "would have no effect",
        ),
        (
            "not a linear variant",
            VoteTransport::Push,
            2,
            crate::config::LeiosVariant::Short,
            "linear Leios variants only",
        ),
    ];
    for (name, transport, fanout, variant, expected) in cases {
        let mut params: crate::config::RawParameters =
            serde_yaml::from_slice(include_bytes!("../../../../parameters/config.default.yaml"))
                .unwrap();
        params.leios_variant = variant;
        params.vote_transport = transport;
        params.vote_push_fanout = Some(fanout);
        let err = SimConfiguration::build(params, new_star_topology(4).into()).expect_err(
            &format!("{name}: a fanout limit that cannot apply must be rejected"),
        );
        let msg = format!("{err:#}");
        assert!(
            msg.contains(expected),
            "{name}: expected an error mentioning {expected:?}, got {msg:?}"
        );
    }
}

/// Arrival before the deadline does not excuse validation finishing after it.
#[test]
fn eb_validation_must_finish_by_the_voting_deadline() {
    for late in [false, true] {
        let topology = new_topology(vec![
            ("node-1", new_node(Some(1000), vec!["node-2"])),
            ("node-2", new_node(Some(1000), vec!["node-1"])),
        ]);
        let mut sim = TestDriver::new(topology);
        let node1 = sim.id_for("node-1");
        let node2 = sim.id_for("node-2");
        let txs: [_; 3] = sim.produce_txs(node1, false);
        for tx in &txs {
            sim.expect_tx_sent(node1, node2, tx.clone());
        }
        sim.win_next_rb_lottery(node1, 0);
        sim.next_slot();
        let (rb, eb) = sim.expect_cpu_task_matching(node1, is_new_rb_task);
        let eb = eb.unwrap();
        sim.expect_rb_and_eb_sent(node1, node2, rb, Some(eb.clone()));
        let seen = sim.now();
        sim.win_next_vote_lottery(node2, 0);
        let deadline = sim.config.voting_window().deadline_at(eb.slot);
        sim.advance_time_to(deadline + Duration::from_millis(u64::from(late)));
        sim.expect_cpu_task(node2, CpuTask::EBBlockValidated(eb, seen));
        assert_eq!(sim.queued_vote_generations(node2), usize::from(!late));
    }
}

/// Signing also consumes queued CPU time after the eligibility check.
#[test]
fn vote_signing_must_finish_by_the_voting_deadline() {
    for (late, conformance) in [(false, false), (false, true), (true, false), (true, true)] {
        let topology = new_topology(vec![
            ("node-1", new_node(Some(1000), vec!["node-2"])),
            ("node-2", new_node(Some(1000), vec!["node-1"])),
        ]);
        let mut config = new_sim_config(topology);
        Arc::get_mut(&mut config).unwrap().emit_conformance_events = conformance;
        let mut sim = TestDriver::new_with_config(config);
        let node1 = sim.id_for("node-1");
        let _txs: [_; 3] = sim.produce_txs(node1, false);
        sim.win_next_rb_lottery(node1, 0);
        sim.win_next_vote_lottery(node1, 0);
        sim.next_slot();
        let (_, eb) = sim.expect_cpu_task_matching(node1, is_new_rb_task);
        let eb = eb.unwrap();
        sim.advance_time_to(sim.config.voting_window().gate_at(eb.slot));
        assert_eq!(sim.queued_vote_generations(node1), 1);
        let deadline = sim.config.voting_window().deadline_at(eb.slot);
        sim.advance_time_to(deadline + Duration::from_millis(u64::from(late)));
        sim.drain_tracked_events();
        sim.expect_cpu_task_matching(node1, is_new_vote_task);
        let events = sim.drain_tracked_events();
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, Event::VTBundleGenerated { .. }))
                .count(),
            usize::from(!late)
        );
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(
                    e,
                    Event::VTBundleNotGenerated {
                        reason: crate::model::NoVoteReason::LateEB,
                        ..
                    }
                ))
                .count(),
            usize::from(late)
        );
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, Event::NoVTBundleGenerated { .. }))
                .count(),
            usize::from(late && conformance)
        );
        if late {
            assert!(vote_wire_reports(&events).is_empty());
        }
    }
}

#[test]
fn accepted_votes_exclude_pending_and_validated_duplicates() {
    let topology = new_topology(vec![
        ("node-1", new_node(Some(1000), vec!["node-2"])),
        ("node-2", new_node(Some(1000), vec!["node-1"])),
    ]);
    let config = new_sim_config_with(topology, |p| {
        p.vote_transport = VoteTransport::PushLateDedupe
    });
    let mut sim = TestDriver::new_with_config(config);
    let node1 = sim.id_for("node-1");
    let node2 = sim.id_for("node-2");
    let votes = sim.produce_vote_bundle(node1);
    assert!(
        !sim.drain_tracked_events()
            .iter()
            .any(|e| matches!(e, Event::VTBundleAccepted { .. })),
        "local generation is not received acceptance"
    );
    sim.expect_message(node1, node2, Message::Votes(votes.clone()));
    let queued = sim
        .nodes
        .get_mut(&node2)
        .unwrap()
        .handle_message(node1, Message::Votes(votes.clone()));
    sim.process_events(node2, queued);
    assert_eq!(sim.queued_vote_validations(node2), 2);
    assert!(
        !sim.drain_tracked_events()
            .iter()
            .any(|e| matches!(e, Event::VTBundleAccepted { .. }))
    );
    sim.expect_cpu_task(node2, CpuTask::VTBundleValidated(node1, votes.clone()));
    let events = sim.drain_tracked_events();
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Event::VTBundleAccepted { .. }))
            .count(),
        1
    );
    assert!(duplicate_reports(&events).is_empty());
    sim.expect_cpu_task(node2, CpuTask::VTBundleValidated(node1, votes));
    let events = sim.drain_tracked_events();
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Event::VTBundleAccepted { .. }))
    );
    assert_eq!(duplicate_reports(&events), vec![(node1, node2)]);
}

#[test]
fn unsupported_vote_transport_options_are_rejected_without_a_fanout_cap() {
    for variant in [
        LeiosVariant::Short,
        LeiosVariant::Full,
        LeiosVariant::FullWithoutIbs,
        LeiosVariant::FullWithTxReferences,
        LeiosVariant::SharedConsensus,
        LeiosVariant::Linear,
        LeiosVariant::LinearWithTxReferences,
    ] {
        for (transport, echo) in [
            (VoteTransport::Push, false),
            (VoteTransport::PushLateDedupe, false),
            (VoteTransport::AnnounceThenRequest, true),
            (VoteTransport::AnnounceThenRequest, false),
        ] {
            let (mut params, topology) = seat_count_params(variant);
            params.committee_selection_algorithm = CommitteeSelectionAlgorithm::WfaLs;
            params.vote_transport = transport;
            params.vote_transport_echo_to_source = echo;
            params.vote_push_fanout = None;
            let supported = matches!(
                variant,
                LeiosVariant::Linear | LeiosVariant::LinearWithTxReferences
            ) || (!transport.is_push() && !echo);
            let result = SimConfiguration::build(params, topology);
            assert_eq!(
                result.is_ok(),
                supported,
                "{variant:?}, {transport:?}, echo={echo}: {result:?}"
            );
            if let Err(error) = result {
                assert!(error.to_string().contains("linear Leios variants only"));
            }
        }
    }
}
