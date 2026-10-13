//! Runtime tests for network-layer partitions.
//!
//! A minimal ping harness: every node pings all its peers each slot and
//! records the pings it receives.  A cut link manifests as missing
//! `recv` events for that directed edge during the partition window.

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use rand_chacha::{ChaChaRng, rand_core::SeedableRng};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    clock::{Clock, Timestamp},
    config::{
        CpuTimeConfig, Direction, NodeConfiguration, NodeId, RawLinkInfo, RawNode, RawNodeLocation,
        RawParameters, RawPartitionScenario, RawPartitionSelector, RawTopology, SimConfiguration,
    },
    events::{Event, EventTracker},
    model::Transaction,
    sim::{EventResult, MiniProtocol, NodeImpl, SimCpuTask, SimMessage},
};

// ---------------------------------------------------------------------------
// Minimal ping node
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Ping {
    from: NodeId,
    slot: u64,
}

impl SimMessage for Ping {
    fn protocol(&self) -> MiniProtocol {
        MiniProtocol::Block
    }
    fn bytes_size(&self) -> u64 {
        100
    }
}

/// Uninhabited: this harness never schedules CPU tasks.
enum NoTask {}

impl SimCpuTask for NoTask {
    fn name(&self) -> String {
        match *self {}
    }
    fn extra(&self) -> String {
        match *self {}
    }
    fn times(&self, _config: &CpuTimeConfig) -> Vec<Duration> {
        match *self {}
    }
}

struct PingNode {
    id: NodeId,
    peers: Vec<NodeId>,
    tracker: EventTracker,
}

impl NodeImpl for PingNode {
    type Message = Ping;
    type Task = NoTask;
    type TimedEvent = ();
    type CustomEvent = ();

    fn new(
        config: &NodeConfiguration,
        sim_config: Arc<SimConfiguration>,
        tracker: EventTracker,
        _rng: ChaChaRng,
        _clock: Clock,
    ) -> Self {
        let peers = sim_config
            .links
            .iter()
            .filter_map(|l| {
                if l.nodes.0 == config.id {
                    Some(l.nodes.1)
                } else if l.nodes.1 == config.id {
                    Some(l.nodes.0)
                } else {
                    None
                }
            })
            .collect();
        PingNode {
            id: config.id,
            peers,
            tracker,
        }
    }

    fn handle_new_slot(&mut self, slot: u64) -> EventResult<Self> {
        let mut result = EventResult::default();
        for &peer in &self.peers {
            result.send_to(
                peer,
                Ping {
                    from: self.id,
                    slot,
                },
            );
        }
        result
    }

    fn handle_new_tx(&mut self, _tx: Arc<Transaction>) -> EventResult<Self> {
        EventResult::default()
    }

    fn handle_message(&mut self, _from: NodeId, msg: Self::Message) -> EventResult<Self> {
        self.tracker.track_test_event(
            self.id,
            "recv",
            &format!("from={},slot={}", msg.from, msg.slot),
        );
        EventResult::default()
    }

    fn handle_cpu_task(&mut self, _task: Self::Task) -> EventResult<Self> {
        EventResult::default()
    }
}

// ---------------------------------------------------------------------------
// Config helpers
// ---------------------------------------------------------------------------

fn node(producers: &[&str]) -> RawNode {
    RawNode {
        stake: Some(250),
        location: RawNodeLocation::Cluster {
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
                        always_forward_votes: false,
                        latency_ms: 5.0,
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

/// 4 fully-connected nodes a, b, c, d.
fn quad_topology() -> RawTopology {
    RawTopology {
        nodes: vec![
            ("a".into(), node(&["b", "c", "d"])),
            ("b".into(), node(&["a", "c", "d"])),
            ("c".into(), node(&["a", "b", "d"])),
            ("d".into(), node(&["a", "b", "c"])),
        ]
        .into_iter()
        .collect(),
    }
}

const NUM_SLOTS: u64 = 6;

fn build_config(shard_count: usize, scenarios: Vec<RawPartitionScenario>) -> Arc<SimConfiguration> {
    let mut params: RawParameters =
        serde_yaml::from_slice(include_bytes!("../../../../parameters/config.default.yaml"))
            .unwrap();
    params.leios_variant = crate::config::LeiosVariant::Linear;
    params.simulate_transactions = false;
    params.shard_count = shard_count;
    params.partition_scenarios = scenarios;
    let topology = quad_topology().into();
    let mut config = SimConfiguration::build(params, topology).unwrap();
    config.slots = Some(NUM_SLOTS);
    Arc::new(config)
}

fn id_of(config: &SimConfiguration, name: &str) -> NodeId {
    config.nodes.iter().find(|n| n.name == name).unwrap().id
}

fn isolate(name: &str, start_s: f64, stop_s: Option<f64>) -> RawPartitionScenario {
    RawPartitionScenario {
        name: format!("isolate-{name}"),
        selector: RawPartitionSelector::Isolate {
            nodes: vec![name.to_string()],
            direction: Direction::Both,
        },
        start_time_s: start_s,
        stop_time_s: stop_s,
    }
}

// ---------------------------------------------------------------------------
// Run helpers
// ---------------------------------------------------------------------------

async fn run_sequential(config: Arc<SimConfiguration>) -> Vec<(Event, Timestamp)> {
    let (tx, rx) = mpsc::unbounded_channel();
    let mut rng = ChaChaRng::seed_from_u64(config.seed);
    let runner = crate::sim::sequential::build_for_test::<PingNode>(config, tx, &mut rng);
    let token = CancellationToken::new();
    tokio::task::spawn_blocking(move || runner.run(token))
        .await
        .unwrap()
        .unwrap();
    drain(rx)
}

async fn run_actor(config: Arc<SimConfiguration>) -> Vec<(Event, Timestamp)> {
    let (tx, rx) = mpsc::unbounded_channel();
    let sim = crate::sim::actor::ActorSimulation::new_generic::<PingNode, _>(
        config,
        tx,
        crate::sim::actor::no_additional_actors,
    )
    .unwrap();
    let token = CancellationToken::new();
    sim.run(token).await.unwrap();
    drain(rx)
}

fn drain(mut rx: mpsc::UnboundedReceiver<(Event, Timestamp)>) -> Vec<(Event, Timestamp)> {
    let mut events = vec![];
    while let Ok(e) = rx.try_recv() {
        events.push(e);
    }
    events
}

/// Slots at which a ping originating from `from` was received by anyone.
fn recv_slots_from(events: &[(Event, Timestamp)], from: NodeId) -> BTreeSet<u64> {
    let needle = format!("from={from},");
    events
        .iter()
        .filter_map(|(e, _)| match e {
            Event::TestNodeEvent {
                event_type, detail, ..
            } if event_type == "recv" && detail.starts_with(&needle) => detail
                .rsplit("slot=")
                .next()
                .and_then(|s| s.parse::<u64>().ok()),
            _ => None,
        })
        .collect()
}

/// Slots at which `to` received a ping originating from `from`.
fn recv_slots_from_to(events: &[(Event, Timestamp)], from: NodeId, to_name: &str) -> BTreeSet<u64> {
    let needle = format!("from={from},");
    events
        .iter()
        .filter_map(|(e, _)| match e {
            Event::TestNodeEvent {
                node,
                event_type,
                detail,
            } if event_type == "recv" && node == to_name && detail.starts_with(&needle) => detail
                .rsplit("slot=")
                .next()
                .and_then(|s| s.parse::<u64>().ok()),
            _ => None,
        })
        .collect()
}

fn count_started(events: &[(Event, Timestamp)]) -> usize {
    events
        .iter()
        .filter(|(e, _)| matches!(e, Event::PartitionStarted { .. }))
        .count()
}

fn count_healed(events: &[(Event, Timestamp)]) -> usize {
    events
        .iter()
        .filter(|(e, _)| matches!(e, Event::PartitionHealed { .. }))
        .count()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Test #4 — single-shard, both engines: pings on a cut link are dropped
/// during the window; links outside the cut keep flowing.
#[tokio::test]
async fn cut_drops_pings_during_window_sequential() {
    // Isolate "a" from t=2s onward (no heal).
    let config = build_config(1, vec![isolate("a", 2.0, None)]);
    let a = id_of(&config, "a");
    let b = id_of(&config, "b");
    let events = run_sequential(config).await;

    // Pings from a reach peers only for slots sent before the cut (0, 1).
    let from_a = recv_slots_from(&events, a);
    assert!(
        from_a.contains(&0) && from_a.contains(&1),
        "pre-cut pings missing: {from_a:?}"
    );
    assert!(
        from_a.iter().all(|&s| s < 2),
        "a's pings leaked past the cut: {from_a:?}"
    );

    // b is not isolated from c/d, so its pings keep flowing throughout.
    let from_b = recv_slots_from(&events, b);
    assert!(
        from_b.contains(&3) && from_b.contains(&4),
        "non-cut link b stopped flowing: {from_b:?}"
    );
}

#[tokio::test]
async fn cut_drops_pings_during_window_actor() {
    let config = build_config(1, vec![isolate("a", 2.0, None)]);
    let a = id_of(&config, "a");
    let b = id_of(&config, "b");
    let events = run_actor(config).await;

    let from_a = recv_slots_from(&events, a);
    assert!(
        from_a.contains(&0) && from_a.contains(&1),
        "pre-cut pings missing: {from_a:?}"
    );
    assert!(
        from_a.iter().all(|&s| s < 2),
        "a's pings leaked past the cut: {from_a:?}"
    );

    let from_b = recv_slots_from(&events, b);
    assert!(
        from_b.contains(&3) && from_b.contains(&4),
        "non-cut link b stopped flowing: {from_b:?}"
    );
}

/// Test #5 — heal at `stop-time-s`: pings flow again after the window.
#[tokio::test]
async fn heal_restores_flow_sequential() {
    // Cut window [2s, 4s): slots 2,3 dropped; 0,1 and 4,5 delivered.
    let config = build_config(1, vec![isolate("a", 2.0, Some(4.0))]);
    let a = id_of(&config, "a");
    let events = run_sequential(config).await;

    let from_a = recv_slots_from(&events, a);
    assert!(
        from_a.contains(&0) && from_a.contains(&1),
        "pre-cut pings missing: {from_a:?}"
    );
    assert!(
        !from_a.contains(&2) && !from_a.contains(&3),
        "in-window pings leaked: {from_a:?}"
    );
    assert!(from_a.contains(&4), "post-heal ping missing: {from_a:?}");
}

/// Overlapping scenarios refcount shared links: a shorter scenario's heal
/// must not reopen links a longer-running scenario still cuts.
#[tokio::test]
async fn overlapping_heal_does_not_reopen_longer_cut_sequential() {
    // Outer window isolates "a" over [1s, 5s); an inner window covers the
    // same links over [2s, 3s).  Before refcounting, the inner heal at
    // t=3 reopened "a" for the rest of the outer window.
    let mut inner = isolate("a", 2.0, Some(3.0));
    inner.name = "isolate-a-inner".into();
    let config = build_config(1, vec![isolate("a", 1.0, Some(5.0)), inner]);
    let a = id_of(&config, "a");
    let events = run_sequential(config).await;

    let from_a = recv_slots_from(&events, a);
    assert!(from_a.contains(&0), "pre-cut ping missing: {from_a:?}");
    for slot in 1..5 {
        assert!(
            !from_a.contains(&slot),
            "ping at slot {slot} leaked while the outer window was active: {from_a:?}"
        );
    }
    assert!(from_a.contains(&5), "post-heal ping missing: {from_a:?}");

    // Both scenarios still emit their own telemetry pairs.
    assert_eq!(count_started(&events), 2);
    assert_eq!(count_healed(&events), 2);
}

/// Test #8 — `direction: from-to` cuts only the forward edge.
#[tokio::test]
async fn direction_from_to_cuts_only_one_way_sequential() {
    // Cut a → b from t=2s. b stops hearing a; a keeps hearing b.
    let config = build_config(
        1,
        vec![RawPartitionScenario {
            name: "a-to-b".into(),
            selector: RawPartitionSelector::SetToSet {
                from: vec!["a".into()],
                to: vec!["b".into()],
                direction: Direction::FromTo,
            },
            start_time_s: 2.0,
            stop_time_s: None,
        }],
    );
    let a = id_of(&config, "a");
    let b = id_of(&config, "b");
    let events = run_sequential(config).await;

    // a → b cut: b receives a's pings only pre-cut.
    let a_to_b = recv_slots_from_to(&events, a, "b");
    assert!(
        a_to_b.iter().all(|&s| s < 2),
        "a → b leaked past the cut: {a_to_b:?}"
    );
    // b → a NOT cut: a keeps receiving b's pings after the cut.
    let b_to_a = recv_slots_from_to(&events, b, "a");
    assert!(
        b_to_a.contains(&3) && b_to_a.contains(&4),
        "reverse edge b → a was wrongly cut: {b_to_a:?}"
    );
}

/// Test #9 — telemetry fires exactly once per window edge, including under
/// multi-shard (only the emitter shard emits).
#[tokio::test]
async fn telemetry_emitted_exactly_once() {
    // Windowed cut → one started + one healed, single shard.
    let events = run_sequential(build_config(1, vec![isolate("a", 2.0, Some(4.0))])).await;
    assert_eq!(count_started(&events), 1);
    assert_eq!(count_healed(&events), 1);

    // 2-shard sequential: still exactly one of each (shard 0 is emitter).
    let events = run_sequential(build_config(2, vec![isolate("a", 2.0, Some(4.0))])).await;
    assert_eq!(
        count_started(&events),
        1,
        "duplicate PartitionStarted across shards"
    );
    assert_eq!(
        count_healed(&events),
        1,
        "duplicate PartitionHealed across shards"
    );
}

/// Determinism with a partition active: two sequential runs produce
/// identical (node, type, detail) event sequences.
#[tokio::test]
async fn partition_run_is_deterministic_sequential() {
    // Disable rayon (parallel_threshold = MAX) so node event emission order
    // is itself deterministic, letting us compare the full ordered stream —
    // partition telemetry included — rather than just by-node sets.
    let build = || {
        let mut config = build_config(1, vec![isolate("a", 2.0, Some(4.0))]);
        Arc::get_mut(&mut config).unwrap().parallel_threshold = usize::MAX;
        config
    };
    let a_events = run_sequential(build()).await;
    let b_events = run_sequential(build()).await;
    let canon = |events: &[(Event, Timestamp)]| -> Vec<(String, Timestamp)> {
        events
            .iter()
            .filter_map(|(e, t)| {
                let label = match e {
                    Event::TestNodeEvent { node, detail, .. } => format!("{node}:{detail}"),
                    Event::PartitionStarted {
                        name, link_count, ..
                    } => {
                        format!("started:{name}:{link_count}")
                    }
                    Event::PartitionHealed { name, link_count } => {
                        format!("healed:{name}:{link_count}")
                    }
                    _ => return None,
                };
                Some((label, *t))
            })
            .collect()
    };
    assert_eq!(canon(&a_events), canon(&b_events));
}

/// Test #6 — a cut that spans shard boundaries is enforced, and the
/// observable delivery pattern matches the single-shard run (Gate B on the
/// cross-shard send path).  With 4 shards over 4 nodes, every one of a's
/// links is cross-shard.
#[tokio::test]
async fn cut_enforced_across_shards_sequential() {
    let single = build_config(1, vec![isolate("a", 2.0, None)]);
    let multi = build_config(4, vec![isolate("a", 2.0, None)]);
    let a = id_of(&single, "a");

    let single_a = recv_slots_from(&run_sequential(single).await, a);
    let multi_a = recv_slots_from(&run_sequential(multi).await, a);

    assert!(
        multi_a.iter().all(|&s| s < 2),
        "cross-shard cut leaked a's pings: {multi_a:?}"
    );
    assert_eq!(
        single_a, multi_a,
        "multi-shard cut differs from single-shard"
    );
}

#[tokio::test]
async fn cut_enforced_across_shards_actor() {
    let config = build_config(4, vec![isolate("a", 2.0, None)]);
    let a = id_of(&config, "a");
    let from_a = recv_slots_from(&run_actor(config).await, a);
    assert!(
        from_a.iter().all(|&s| s < 2),
        "cross-shard cut leaked a's pings (actor): {from_a:?}"
    );
}

/// Test #7 — multi-shard determinism with a partition active: two 4-shard
/// sequential runs agree on per-node event sets (including timestamps) and
/// on partition telemetry counts.  Compares by-node sets rather than the
/// raw stream because shards race through the event mpsc.
#[tokio::test]
async fn multi_shard_partition_deterministic_sequential() {
    let a = run_sequential(build_config(4, vec![isolate("a", 2.0, Some(4.0))])).await;
    let b = run_sequential(build_config(4, vec![isolate("a", 2.0, Some(4.0))])).await;
    let canon = |events: &[(Event, Timestamp)]| {
        let mut by_node: std::collections::BTreeMap<String, BTreeSet<String>> =
            std::collections::BTreeMap::new();
        for (e, t) in events {
            if let Event::TestNodeEvent { node, detail, .. } = e {
                by_node
                    .entry(node.clone())
                    .or_default()
                    .insert(format!("{detail}@{t:?}"));
            }
        }
        by_node
    };
    assert_eq!(
        canon(&a),
        canon(&b),
        "multi-shard partition run not deterministic"
    );
    assert_eq!(count_started(&a), 1);
    assert_eq!(count_healed(&a), 1);
}

/// Regression: with no `partition-scenarios`, no partition telemetry is
/// emitted and every link flows for the whole run.
#[tokio::test]
async fn no_scenarios_leaves_all_links_active() {
    let config = build_config(1, vec![]);
    let a = id_of(&config, "a");
    let events = run_sequential(config).await;
    assert_eq!(count_started(&events), 0);
    assert_eq!(count_healed(&events), 0);
    let from_a = recv_slots_from(&events, a);
    assert!(
        from_a.contains(&2) && from_a.contains(&3) && from_a.contains(&4),
        "links should stay active with no scenarios: {from_a:?}"
    );
}
