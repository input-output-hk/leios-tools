//! Optional vote-only accounting. It observes events without retaining the event
//! stream or changing simulated node behavior. Buckets tolerate out-of-order
//! events from simulation shards; no timestamp-based eviction is required.
use std::collections::HashMap;

use anyhow::{Context, Result};
use serde::Serialize;
use sim_core::{clock::Timestamp, config::SimConfiguration, events::Event};

use super::VoteBundleId;

fn bundle_key(id: &VoteBundleId) -> (u64, u64, usize) {
    (id.slot, id.pipeline, id.producer.id.to_inner())
}

#[derive(Default, Serialize)]
struct MessageTotals {
    messages: u64,
    bytes: u64,
}

#[derive(Default, Serialize)]
struct DirectionTotals {
    bodies: MessageTotals,
    announcements: MessageTotals,
    requests: MessageTotals,
}

#[derive(Serialize)]
struct NodeTraffic {
    id: usize,
    name: String,
    stake: u64,
    sent: DirectionTotals,
    received: DirectionTotals,
    /// Index is the second since t=0. Each entry is [sent bytes, received bytes]
    /// in [second, second + 1). Missing trailing entries contain zero traffic.
    seconds: Vec<[u64; 2]>,
}

#[derive(Serialize)]
pub(super) struct VoteTraffic {
    format_version: u8,
    seed: u64,
    requested_slots: Option<u64>,
    observed_until_s: Timestamp,
    window_seconds: u64,
    accounting: &'static str,
    nodes: Vec<NodeTraffic>,
    #[serde(skip)]
    body_sizes: HashMap<(u64, u64, usize), u64>,
}

impl VoteTraffic {
    pub fn new(config: &SimConfiguration) -> Self {
        Self {
            format_version: 1,
            seed: config.seed,
            requested_slots: config.slots,
            observed_until_s: Timestamp::zero(),
            window_seconds: 1,
            accounting: "Vote mini-protocol bytes only, including duplicates. Sent at enqueue; received at delivery. No TCP/IP framing. One-second fixed buckets, not instantaneous link throughput. Stake distinguishes BP from relay only in topologies with separate BP nodes.",
            nodes: config
                .nodes
                .iter()
                .map(|n| NodeTraffic {
                    id: n.id.to_inner(),
                    name: n.name.clone(),
                    stake: n.stake,
                    sent: DirectionTotals::default(),
                    received: DirectionTotals::default(),
                    seconds: Vec::new(),
                })
                .collect(),
            body_sizes: HashMap::new(),
        }
    }

    pub fn process(&mut self, event: &Event, time: Timestamp) -> Result<()> {
        self.observed_until_s = self.observed_until_s.max(time);
        // Classification events (duplicates, obsolete work, accepted votes) do
        // not add bytes: their original sends/arrivals already account for them.
        let (node, receiving, kind, bytes) = match event {
            Event::VTBundleGenerated { id, size_bytes, .. } => {
                self.body_sizes.insert(bundle_key(id), *size_bytes);
                return Ok(());
            }
            Event::VTBundleSent {
                sender,
                msg_size_bytes,
                ..
            } => (sender, false, 0, *msg_size_bytes),
            Event::VTBundleReceived { id, recipient, .. } => {
                let bytes = *self
                    .body_sizes
                    .get(&bundle_key(id))
                    .context("vote arrival has no generated body size")?;
                (recipient, true, 0, bytes)
            }
            Event::VTBundleAnnounced {
                sender,
                msg_size_bytes,
                ..
            } => (sender, false, 1, *msg_size_bytes),
            Event::VTBundleAnnouncementReceived {
                recipient,
                msg_size_bytes,
                ..
            } => (recipient, true, 1, *msg_size_bytes),
            Event::VTBundleRequested {
                sender,
                msg_size_bytes,
                ..
            } => (sender, false, 2, *msg_size_bytes),
            Event::VTBundleRequestReceived {
                recipient,
                msg_size_bytes,
                ..
            } => (recipient, true, 2, *msg_size_bytes),
            _ => return Ok(()),
        };
        self.record(node.id.to_inner(), receiving, kind, bytes, time);
        Ok(())
    }

    fn record(&mut self, id: usize, receiving: bool, kind: usize, bytes: u64, time: Timestamp) {
        let node = &mut self.nodes[id]; // Configured node IDs are contiguous vector indices.
        let totals = if receiving {
            &mut node.received
        } else {
            &mut node.sent
        };
        let messages = match kind {
            0 => &mut totals.bodies,
            1 => &mut totals.announcements,
            _ => &mut totals.requests,
        };
        messages.messages += 1;
        messages.bytes += bytes;
        let second = (time - Timestamp::zero()).as_secs() as usize;
        if node.seconds.len() <= second {
            node.seconds.resize(second + 1, [0; 2]);
        }
        node.seconds[second][usize::from(receiving)] += bytes;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sim_core::{
        config::{NodeId, RawParameters, RawTopology},
        events::Node,
    };
    use std::{sync::Arc, time::Duration};

    #[test]
    fn counts_deliveries_separately_and_ignores_classification_events() {
        let params: RawParameters =
            serde_yaml::from_str(include_str!("../../../parameters/config.default.yaml")).unwrap();
        let topology: RawTopology = serde_yaml::from_str("nodes:\n  bp:\n    stake: 1000\n    location: [0, 0]\n    producers: {}\n  relay:\n    location: [0, 0]\n    producers: {}\n").unwrap();
        let config = SimConfiguration::build(params, topology.into()).unwrap();
        let mut report = VoteTraffic::new(&config);
        let bp = Node {
            id: NodeId::new(0),
            name: Arc::new("bp".into()),
        };
        let relay = Node {
            id: NodeId::new(1),
            name: Arc::new("relay".into()),
        };
        let id = VoteBundleId {
            slot: 0,
            pipeline: 0,
            producer: bp.clone(),
        };
        report.body_sizes.insert(bundle_key(&id), 94);
        let sent = Event::VTBundleSent {
            id: id.clone(),
            slot: 0,
            pipeline: 0,
            producer: bp.clone(),
            sender: bp.clone(),
            recipient: relay.clone(),
            msg_size_bytes: 94,
        };
        let received = Event::VTBundleReceived {
            id: id.clone(),
            slot: 0,
            pipeline: 0,
            producer: bp.clone(),
            sender: bp.clone(),
            recipient: relay.clone(),
        };
        // Cross-shard arrival order must not change fixed time bins.
        report.process(&received, Timestamp::from_secs(2)).unwrap();
        report.process(&received, Timestamp::from_secs(2)).unwrap();
        report
            .process(&sent, Timestamp::from_secs(1) - Duration::from_millis(1))
            .unwrap();
        report.process(&sent, Timestamp::from_secs(1)).unwrap();
        report
            .process(
                &Event::VTBundleDuplicate {
                    id: id.clone(),
                    producer: bp.clone(),
                    sender: bp.clone(),
                    recipient: relay.clone(),
                    msg_size_bytes: 94,
                },
                Timestamp::from_secs(2),
            )
            .unwrap();
        report
            .process(
                &Event::VTBundleObsoleteReceived {
                    id: id.clone(),
                    node: relay.clone(),
                    msg_size_bytes: 94,
                },
                Timestamp::from_secs(2),
            )
            .unwrap();
        report
            .process(
                &Event::VTBundleAnnounced {
                    id: id.clone(),
                    sender: bp.clone(),
                    recipient: relay.clone(),
                    msg_size_bytes: 8,
                },
                Timestamp::from_secs(0),
            )
            .unwrap();
        report
            .process(
                &Event::VTBundleRequestReceived {
                    id,
                    sender: relay,
                    recipient: bp,
                    msg_size_bytes: 40,
                },
                Timestamp::from_secs(1),
            )
            .unwrap();
        assert_eq!(report.nodes[0].sent.bodies.bytes, 188);
        assert_eq!(report.nodes[1].received.bodies.messages, 2);
        assert_eq!(report.nodes[0].seconds, vec![[102, 0], [94, 40]]);
        assert_eq!(report.nodes[1].seconds, vec![[0, 0], [0, 0], [0, 188]]);
        assert_eq!(report.observed_until_s, Timestamp::from_secs(2));
        assert_eq!(report.nodes[1].sent.bodies.bytes, 0);
    }
}
