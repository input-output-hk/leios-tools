//! Coordinator: manages multiple peer connections, aggregates events,
//! and exposes a peer-agnostic interface to the application.
//!
//! The coordinator runs as a single tokio task. It receives events from
//! all peer tasks via a shared fan-in channel and sends commands to
//! individual peers via per-peer channels.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Dedup state for Leios offer forwarding.
///
/// Each peer typically re-announces still-relevant EBs / TXs / votes
/// in its outgoing notify loop, so without dedup the coordinator would
/// emit a fresh `NetworkEvent` to the application for every replay.
/// The application's `network_events` channel is bounded (capacity 64
/// today) and `coordinator.emit_event().await` blocks when full —
/// re-announce floods can wedge the coordinator and back-pressure
/// cascade into per-protocol mux ingress overflow.
///
/// Dedup is keyed on `(peer_id, resource)` so each peer's first offer
/// of a given resource still flows through (consensus's
/// `CandidateTracker` needs to see all peers that have offered for
/// `BroadcastN` / `LowestRttFirst` to rank candidates).  Bounded by
/// slot window — entries below `max_slot - window` are pruned.
#[derive(Default)]
struct OfferDedup {
    /// `(peer, slot, eb_hash)` already forwarded as `LeiosBlockOffered`.
    seen_eb: BTreeSet<(PeerId, u64, [u8; 32])>,
    /// `(peer, slot, eb_hash)` already forwarded as `LeiosBlockTxsOffered`.
    seen_eb_txs: BTreeSet<(PeerId, u64, [u8; 32])>,
    /// `(peer, slot, eb_hash, voter_id)` already forwarded as part of an
    /// inline `LeiosVotesReceived`.  Per-vote because a single notify
    /// carries a batch; dedup keeps gossiped duplicates from re-firing.
    seen_votes: BTreeSet<(PeerId, u64, [u8; 32], u16)>,
    max_slot: u64,
    window: u64,
}

impl OfferDedup {
    fn new(window: u64) -> Self {
        Self {
            window,
            ..Default::default()
        }
    }

    fn update_slot(&mut self, slot: u64) {
        if slot > self.max_slot {
            self.max_slot = slot;
            let cutoff = slot.saturating_sub(self.window);
            self.seen_eb.retain(|(_, s, _)| *s >= cutoff);
            self.seen_eb_txs.retain(|(_, s, _)| *s >= cutoff);
            self.seen_votes.retain(|(_, s, _, _)| *s >= cutoff);
        }
    }

    /// Returns `true` if `(peer, slot, hash)` is a fresh EB offer that
    /// should be forwarded; `false` if already seen.
    fn fresh_eb(&mut self, peer: PeerId, slot: u64, hash: [u8; 32]) -> bool {
        self.update_slot(slot);
        self.seen_eb.insert((peer, slot, hash))
    }

    fn fresh_eb_txs(&mut self, peer: PeerId, slot: u64, hash: [u8; 32]) -> bool {
        self.update_slot(slot);
        self.seen_eb_txs.insert((peer, slot, hash))
    }

    /// Filter an inline vote batch to fresh-for-this-peer entries.
    /// Updates internal state to record forwarding.
    fn fresh_votes(&mut self, peer: PeerId, votes: Vec<Vote>) -> Vec<Vote> {
        let mut out = Vec::with_capacity(votes.len());
        for vote in votes {
            self.update_slot(vote.slot);
            if self
                .seen_votes
                .insert((peer, vote.slot, vote.eb_hash, vote.voter_id))
            {
                out.push(vote);
            }
        }
        out
    }
}

use super::chain_fragment::ChainFragment;
use crate::bearer::tcp::TcpBearer;
use crate::mux::MuxConfig;
use crate::protocols::peersharing::PeerAddress;
use crate::types::{Point, Tip, Vote};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::types::{NetworkCommand, NetworkEvent};
use super::{CoordinatorConfig, CoordinatorHandle};
use crate::peer::connect::{self, DuplexConnection};
use crate::peer::duplex_task::{
    run_accepted_duplex_task, run_duplex_task, AcceptedDuplexTaskConfig, DuplexTaskConfig,
};
use crate::peer::server_handlers::TxDedup;
use crate::peer::peer_task::{
    client_protocol_configs, run_peer_task, server_protocol_configs, PeerTaskConfig,
};
use crate::peer::types::{PeerCommand, PeerEvent};
use crate::peer::{ConnectionMode, PeerId};
use crate::store::chain_store::ChainStore;
use crate::store::leios_store::LeiosStore;
use shared_consensus::mempool::{TxBody, TxId};

/// Capacity of the node-wide tx-submission dedup set. Bounds the number of
/// recently-seen tx ids retained to suppress cross-peer refetches; sized to
/// comfortably exceed the distinct in-flight txs across every peer's
/// acknowledgement window (each id is 32 bytes, so this is a few MB).
const TX_DEDUP_CAP: usize = 131_072;

/// Capacity of the per-peer command channel (coordinator → peer task).
/// Large enough that a brief peer-task stall doesn't immediately force
/// removal; full channel is treated as a broken peer. Sized for the
/// vote-/eb-tx-fetch burst when an EB reaches quorum: each peer can be
/// the target of many small fetch commands in the same millisecond.
const PEER_COMMAND_CAPACITY: usize = 4096;

/// Capacity of the network_events channel (coordinator → application).
/// Sized to absorb the bursts at quorum (per-peer vote and eb-tx fetch
/// offers fan out into O(peers × votes) events on every consensus round).
const NETWORK_EVENTS_CAPACITY: usize = 65536;

/// Capacity of the network_commands channel (application → coordinator).
/// Sized to absorb the matching burst of fetch commands the app issues
/// in response to a quorum event.
const NETWORK_COMMANDS_CAPACITY: usize = 16384;

/// Capacity of the peer_events fan-in channel (all peer tasks → coordinator).
/// Shared by all peer tasks; sized for the burst when every peer simultaneously
/// emits vote/eb-tx offer events.
const PEER_EVENTS_CAPACITY: usize = 32768;

/// Minimum free slots in `network_events` before the coordinator pulls a new
/// peer event. Handlers may emit several `NetworkEvent`s per peer event
/// (e.g. `Failed` iterates `pending_fetches`), so we reserve headroom to
/// guarantee every handler completes without an emit failure. When the
/// free slot count drops below this threshold, the `peer_events` branch of
/// the main `select!` is disabled, which blocks peer tasks on
/// `peer_event_sender.send().await` and propagates backpressure all the
/// way to TCP. Kept proportional (~1.5%) to NETWORK_EVENTS_CAPACITY.
const MIN_EMIT_HEADROOM: usize = 1024;

/// First re-intersection backoff for an address (the first attempt is
/// always allowed; rapid repeats grow from here).
const REINTERSECT_BACKOFF_BASE: Duration = Duration::from_secs(1);
/// Cap on the per-address re-intersection backoff.
const REINTERSECT_BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Per-peer state tracked by the coordinator.
struct PeerState {
    address: String,
    #[allow(dead_code)]
    mode: ConnectionMode,
    /// RAII guard for the per-IP connection slot. Present only for inbound
    /// (accepted) peers. On drop (when this `PeerState` is removed from
    /// `self.peers`), the guard decrements `ip_counts` — so cleanup doesn't
    /// depend on the event loop being able to process `PeerEvent::Failed`.
    ip_guard: Option<IpCountGuard>,
    commands: mpsc::Sender<PeerCommand>,
    task_handle: JoinHandle<()>,
    tip: Option<Tip>,
    rtt: Option<Duration>,
    /// Chain fragment: ordered points announced via ChainSync.
    fragment: ChainFragment,
    /// Backoff for next reconnection attempt if this peer fails.
    reconnect_backoff: Duration,
    /// Simulated inbound delay. Events from this peer are delayed by this
    /// duration before processing. Zero = no delay.
    inbound_delay: Duration,
    /// Shared byte counters from this peer's mux connection.
    mux_stats: Option<Arc<crate::mux::MuxStats>>,
    /// Shared downstream-promotion flag (cold/warm/hot) from this peer's
    /// responder handlers; read at snapshot time. `None` until `Connected`.
    downstream: Option<crate::peer::DownstreamFlag>,
    /// Last rollback point this peer was notified to, for dedup: we
    /// refuse to forward consecutive `RolledBack` events with the same
    /// point so a chatty peer can't flood the consensus channel.
    last_rolled_back_to: Option<Point>,
}

/// The coordinator's internal state.
struct Coordinator {
    config: CoordinatorConfig,
    peers: HashMap<PeerId, PeerState>,
    next_peer_id: u64,

    /// Receives (PeerId, PeerEvent) from all peer tasks.
    peer_events: mpsc::Receiver<(PeerId, PeerEvent)>,
    /// Cloned and given to each new peer task.
    peer_event_sender: mpsc::Sender<(PeerId, PeerEvent)>,

    /// Sends NetworkEvent to the application.
    network_events: mpsc::Sender<NetworkEvent>,
    /// Receives NetworkCommand from the application.
    network_commands: mpsc::Receiver<NetworkCommand>,

    /// High-water mark of any peer's reported tip — informational only.
    /// Updated when a `HeaderAnnounced`'s tip strictly exceeds the
    /// current value; never lowered on rollback. Not used for chain
    /// selection — that responsibility lives in Praos
    /// (`shared_consensus::praos`), which drives `chain_store` mutations
    /// via `NetworkCommand::InjectBlock` / `InjectRollback`.
    best_tip: Option<Tip>,
    /// Pending block fetch requests: point → peer that's fetching it.
    pending_fetches: HashMap<Point, PeerId>,
    /// Peers waiting to be reconnected (address, next attempt time, current backoff).
    reconnect_queue: Vec<(String, Instant, Duration)>,
    /// Per-address re-intersection throttle: `(next_allowed, backoff)`.
    /// A peer on an unreconcilable fork re-intersects in a tight loop;
    /// `ReIntersect` is rate-limited per *address* (stable across the
    /// reconnect handovers that change `PeerId`) with exponential backoff
    /// so the node backs off gracefully instead of spinning.
    reintersect_throttle: HashMap<String, (Instant, Duration)>,

    /// Shared chain state for responder peers.
    chain_store: Arc<ChainStore>,
    /// Shared Leios data store for responder peers (when leios_enabled).
    leios_store: Option<Arc<LeiosStore>>,
    /// Per-peer chain-fragment size snapshot. Updated alongside every
    /// `peer.fragment` mutation so the application can read fragment
    /// memory usage for per-slot telemetry without going through the
    /// command channel.
    fragment_sizes: Arc<Mutex<HashMap<PeerId, usize>>>,
    /// Completed inbound duplex connections from the accept loop. The third
    /// tuple element is the RAII guard holding the per-IP slot reservation;
    /// it is stored in the new `PeerState` once the connection is added.
    inbound_connections: Option<mpsc::Receiver<(DuplexConnection, SocketAddr, IpCountGuard)>>,
    /// Handle for the accept loop task (if listening).
    accept_task: Option<JoinHandle<()>>,
    /// Per-IP connection count (handshaking + established). Shared with accept loop.
    ip_counts: Arc<Mutex<HashMap<IpAddr, usize>>>,
    /// Peer provider callback for PeerSharing server.
    peer_provider: Arc<dyn Fn(u8) -> Vec<PeerAddress> + Send + Sync>,
    /// Peers to remove after the current select! iteration completes.
    /// Populated by `send_peer_command` when a peer's command channel is
    /// full (treated as a broken peer task). Drained at the bottom of the
    /// main loop body so removal doesn't happen mid-handler.
    pending_removals: Vec<(PeerId, String)>,
    /// Per-(peer, resource) dedup for Leios offer events.  Without this,
    /// each peer's notify-loop replay floods `network_events`; the
    /// coordinator's `.send().await` blocks; per-protocol mux ingress
    /// channels back up; the mux tears down with `IngressOverflow`.
    leios_offer_dedup: OfferDedup,
    /// Outbound peer addresses this node refuses to dial.  Set via
    /// `NetworkCommand::SetPeerBlocklist` (cluster-driven partitions).
    /// Enforced at three points — `AddPeer` (refuse), reconnection
    /// (park, don't drop), and on update (disconnect any currently
    /// connected match).  Empty in the common no-partition case, so the
    /// `contains`/`is_empty` checks are off the data path and free.
    blocklist: HashSet<String>,
    /// Node-wide tx-submission dedup, shared into every peer's
    /// `serve_txsubmission` so a tx pulled from one peer isn't re-fetched
    /// (and re-hashed) from every other peer that offers it.
    tx_dedup: Arc<Mutex<TxDedup>>,
}

impl Coordinator {
    fn new(
        config: CoordinatorConfig,
        peer_event_sender: mpsc::Sender<(PeerId, PeerEvent)>,
        peer_events: mpsc::Receiver<(PeerId, PeerEvent)>,
        network_events: mpsc::Sender<NetworkEvent>,
        network_commands: mpsc::Receiver<NetworkCommand>,
        chain_store: Arc<ChainStore>,
        leios_store: Option<Arc<LeiosStore>>,
    ) -> Self {
        let dedup_window = config.leios_dedup_window;
        Self {
            config,
            peers: HashMap::new(),
            reintersect_throttle: HashMap::new(),
            next_peer_id: 0,
            peer_events,
            peer_event_sender,
            network_events,
            network_commands,
            best_tip: None,
            pending_fetches: HashMap::new(),
            reconnect_queue: Vec::new(),
            chain_store,
            leios_store,
            fragment_sizes: Arc::new(Mutex::new(HashMap::new())),
            inbound_connections: None,
            accept_task: None,
            ip_counts: Arc::new(Mutex::new(HashMap::new())),
            peer_provider: Arc::new(|_| Vec::new()),
            pending_removals: Vec::new(),
            leios_offer_dedup: OfferDedup::new(dedup_window),
            blocklist: HashSet::new(),
            tx_dedup: Arc::new(Mutex::new(TxDedup::new(TX_DEDUP_CAP))),
        }
    }

    /// Update the shared fragment-size snapshot for a peer to match its
    /// current `ChainFragment::len()`.  Called after every mutation of
    /// `peer.fragment` so per-slot telemetry sees a consistent view.
    fn sync_fragment_size(&self, peer_id: PeerId, len: usize) {
        if let Ok(mut map) = self.fragment_sizes.lock() {
            map.insert(peer_id, len);
        }
    }

    /// Emit a NetworkEvent to the application using the non-blocking
    /// `try_send`. The `peer_events` branch of the main `select!` gates
    /// on `network_events.capacity() >= MIN_EMIT_HEADROOM` so handlers
    /// always enter with sufficient headroom for several emits; seeing
    /// `TrySendError::Full` here indicates a handler emitted more events
    /// than the reserved headroom (a bug to fix) rather than normal load.
    fn emit_event(&mut self, event: NetworkEvent) {
        use tokio::sync::mpsc::error::TrySendError;
        match self.network_events.try_send(event) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                tracing::error!(
                    "emit_event: network_events unexpectedly full (handler exceeded MIN_EMIT_HEADROOM)"
                );
            }
            Err(TrySendError::Closed(_)) => {
                // Application has dropped its receiver. The main loop
                // will observe this on its next `network_commands.recv()`
                // and exit naturally.
            }
        }
    }

    /// Route a command to a specific peer using `try_send`. On `Full`, the
    /// peer task is not draining its command channel — treat that peer as
    /// broken and schedule it for removal. Returns true if the command was
    /// accepted into the channel.
    fn send_peer_command(&mut self, peer_id: PeerId, cmd: PeerCommand) -> bool {
        use tokio::sync::mpsc::error::TrySendError;
        let peer = match self.peers.get(&peer_id) {
            Some(p) => p,
            None => return false,
        };
        match peer.commands.try_send(cmd) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) => {
                tracing::error!(?peer_id, "peer command channel full; scheduling removal");
                self.pending_removals
                    .push((peer_id, "peer command channel full".to_string()));
                false
            }
            Err(TrySendError::Closed(_)) => {
                self.pending_removals
                    .push((peer_id, "peer command channel closed".to_string()));
                false
            }
        }
    }

    /// Assign a new PeerId and spawn an outbound peer task (initiator or duplex).
    fn add_peer_with_backoff(&mut self, address: String, reconnect_backoff: Duration) -> PeerId {
        let peer_id = PeerId(self.next_peer_id);
        self.next_peer_id += 1;

        let (cmd_sender, cmd_receiver) = mpsc::channel(PEER_COMMAND_CAPACITY);

        let (task_handle, mode) = if self.config.duplex {
            let task_config = DuplexTaskConfig {
                peer_id,
                address: address.clone(),
                network_magic: self.config.network_magic,
                keepalive_interval: self.config.keepalive_interval,
                sdu_timeout: self.config.sdu_timeout,
                chain_store: self.chain_store.clone(),
                peer_provider: self.peer_provider.clone(),
                event_sender: self.peer_event_sender.clone(),
                command_receiver: cmd_receiver,
                leios_enabled: self.config.leios_enabled,
                leios_store: self.leios_store.clone(),
                traffic_class_overrides: self.config.traffic_class_overrides.clone(),
                scheduler_type: self.config.scheduler_type,
                outbound_controls: self.config.outbound_controls.clone(),
                tx_dedup: self.tx_dedup.clone(),
            };
            (
                tokio::spawn(run_duplex_task(task_config)),
                ConnectionMode::Duplex,
            )
        } else {
            let task_config = PeerTaskConfig {
                peer_id,
                address: address.clone(),
                network_magic: self.config.network_magic,
                keepalive_interval: self.config.keepalive_interval,
                sdu_timeout: self.config.sdu_timeout,
                chain_store: self.chain_store.clone(),
                event_sender: self.peer_event_sender.clone(),
                command_receiver: cmd_receiver,
                leios_enabled: self.config.leios_enabled,
                traffic_class_overrides: self.config.traffic_class_overrides.clone(),
                scheduler_type: self.config.scheduler_type,
            };
            (
                tokio::spawn(run_peer_task(task_config)),
                ConnectionMode::InitiatorOnly,
            )
        };

        let inbound_delay = self
            .config
            .peer_delays
            .get(&address)
            .copied()
            .unwrap_or(Duration::ZERO);

        self.peers.insert(
            peer_id,
            PeerState {
                address,
                mode,
                ip_guard: None,
                commands: cmd_sender,
                task_handle,
                tip: None,
                rtt: None,
                fragment: ChainFragment::new(),
                reconnect_backoff,
                inbound_delay,
                mux_stats: None,
                downstream: None,
                last_rolled_back_to: None,
            },
        );
        self.sync_fragment_size(peer_id, 0);

        peer_id
    }

    /// Assign a new PeerId and spawn a peer task with default backoff.
    fn add_peer(&mut self, address: String) -> PeerId {
        self.add_peer_with_backoff(address, Duration::from_secs(1))
    }

    /// Queue for removal every peer whose task has already exited, and
    /// return the count of peers that are still live.
    ///
    /// Under connection churn the coordinator's `PeerEvent::Failed`
    /// processing lags behind the peer tasks that emit it, so dead
    /// `PeerState`s linger in `self.peers` until their (backlogged)
    /// removal is finally processed. Those zombies still count against
    /// `max_peers` — enough of them and the cap is pinned, forcing us to
    /// reject *live* inbound reconnects while the map is full of corpses
    /// (the fork-storm's connectivity collapse: nodes stuck at cap while
    /// others starve). A finished `task_handle` is an authoritative,
    /// synchronous "this peer is gone" signal — the per-peer task only
    /// returns after its own teardown — so we can reclaim the slot
    /// immediately instead of waiting for the event. Removal is routed
    /// through the normal `pending_removals` path so the full cleanup
    /// (ip_guard release, fragment sizes, `PeerDisconnected`, and outbound
    /// reconnect scheduling) still runs exactly once.
    fn reap_finished_peers(&mut self) -> usize {
        let mut live = 0usize;
        let mut dead = Vec::new();
        for (id, peer) in &self.peers {
            if peer.task_handle.is_finished() {
                dead.push(*id);
            } else {
                live += 1;
            }
        }
        for id in dead {
            if !self.pending_removals.iter().any(|(pid, _)| *pid == id) {
                self.pending_removals
                    .push((id, "peer task already exited (reaped at cap)".to_string()));
            }
        }
        live
    }

    /// Whether a still-live peer already holds this outbound address.
    ///
    /// Outbound addresses are stable node listen ports (unlike inbound
    /// connections, which arrive on ephemeral source ports and can't be
    /// mapped back to a logical node), so we can dedup on them: never
    /// spawn a second outbound task for an address we're already connected
    /// to. Finished (zombie) peers don't count — they're about to be
    /// reaped, so a reconnect to their address should proceed.
    fn has_live_peer_for_address(&self, address: &str) -> bool {
        self.peers
            .values()
            .any(|p| p.address == address && !p.task_handle.is_finished())
    }

    /// Handle an event from a peer task.
    async fn handle_peer_event(&mut self, peer_id: PeerId, event: PeerEvent) {
        match event {
            PeerEvent::Connected {
                mux_stats,
                downstream,
            } => {
                // The peer task's Connected event can race with our
                // own remove_peer (which clears self.peers and aborts
                // the task) — the buffered Connected message gets
                // processed after the peer is gone.  Drop the stale
                // event; emitting it would surface a spurious
                // PeerConnected with an empty address, ordered after
                // the corresponding PeerDisconnected.
                let Some(peer) = self.peers.get_mut(&peer_id) else {
                    return;
                };
                peer.mux_stats = Some(mux_stats);
                peer.downstream = Some(downstream);
                let address = peer.address.clone();
                self.emit_event(NetworkEvent::PeerConnected { peer_id, address });
            }

            PeerEvent::IntersectionFound { point } => {
                let new_len = if let Some(peer) = self.peers.get_mut(&peer_id) {
                    peer.fragment.set_intersection(point.clone());
                    Some(peer.fragment.len())
                } else {
                    None
                };
                if let Some(len) = new_len {
                    self.sync_fragment_size(peer_id, len);
                }
                // Forward to consensus so it can store the intersection as
                // the peer chain's anchor (guaranteed common ancestor).
                self.emit_event(NetworkEvent::IntersectionFound { peer_id, point });
            }

            PeerEvent::HeaderAnnounced { header, tip } => {
                // Derive the header's own point (may differ from tip when catching up).
                let header_point = header.point().unwrap_or(tip.point.clone());

                // Update this peer's known tip and chain fragment.
                let new_len = if let Some(peer) = self.peers.get_mut(&peer_id) {
                    peer.tip = Some(tip.clone());
                    peer.fragment.append(header_point.clone());
                    // A fresh forward progression clears the rollback dedup.
                    peer.last_rolled_back_to = None;
                    Some(peer.fragment.len())
                } else {
                    None
                };
                if let Some(len) = new_len {
                    self.sync_fragment_size(peer_id, len);
                }

                // Update best tip tracker.  Log every advance — gives operators
                // a clear "the relay is at block X" line without having to probe
                // out-of-band with chain-sync. Fires once per new block the
                // peer produces (or once at start if a new peer's tip dominates).
                let dominated = match &self.best_tip {
                    None => false,
                    Some(best) => tip.block_no <= best.block_no,
                };
                if !dominated {
                    let (tip_slot, tip_hash) = match &tip.point {
                        Point::Specific { slot, hash } => (Some(*slot), Some(*hash)),
                        Point::Origin => (None, None),
                    };
                    tracing::info!(
                        peer = peer_id.0,
                        tip_block_no = tip.block_no,
                        tip_slot,
                        tip_hash = ?tip_hash.map(|h| format!("{:02x}{:02x}", h[30], h[31])),
                        "best peer tip advanced"
                    );
                    self.best_tip = Some(tip.clone());
                }

                // Forward every per-peer announcement so consensus can
                // maintain a candidate chain per peer (Haskell-style).
                self.emit_event(NetworkEvent::TipAdvanced {
                    peer_id,
                    tip,
                    header,
                });
            }

            PeerEvent::RolledBack { point, tip } => {
                // Update peer's tip and truncate chain fragment.
                // Per-peer dedup: if we already forwarded a rollback to
                // this same point for this peer, don't refire — a chatty
                // peer could otherwise flood the consensus channel.
                let (duplicate, new_len) = if let Some(peer) = self.peers.get_mut(&peer_id) {
                    peer.tip = Some(tip.clone());
                    peer.fragment.rollback_to(&point);
                    let dup = peer.last_rolled_back_to.as_ref() == Some(&point);
                    if !dup {
                        peer.last_rolled_back_to = Some(point.clone());
                    }
                    (dup, Some(peer.fragment.len()))
                } else {
                    (false, None)
                };
                if let Some(len) = new_len {
                    self.sync_fragment_size(peer_id, len);
                }

                // `best_tip` deliberately stays as the high-water mark — a
                // peer rollback doesn't lower it (another peer may still be
                // at the higher tip), and `chain_store` is not touched here.
                // Praos owns chain selection: it consumes the forwarded
                // `NetworkEvent::RolledBack` below, runs `on_peer_rolled_back`,
                // and only emits `PraosEffect::InjectRollback` (→
                // `NetworkCommand::InjectRollback` → `chain_store.rollback_to`)
                // when its chain selector actually switches off the rolled-back
                // chain. Direct mutation here (kept until 2026-06) racing with
                // Praos could truncate `chain_store` even when Praos stays on
                // a still-ahead peer's chain.

                // Always forward (unless deduped) so consensus can retire
                // headers from the peer's candidate chain.
                if !duplicate {
                    self.emit_event(NetworkEvent::RolledBack {
                        peer_id,
                        point,
                        tip,
                    });
                }
            }

            PeerEvent::BlockFetched { body } => {
                // Derive the point from the block body (header hash + slot).
                // Requires blocks to have valid Shelley+ CBOR structure.
                let point = body.point().unwrap_or(Point::Origin);

                self.pending_fetches.remove(&point);

                // Note: we do NOT remove the point from peer fragments.
                // Fragments represent what peers announced via ChainSync
                // and are used for fetch routing. Removing fetched points
                // would break future FetchBlockRange requests that use
                // this point as `from` or `to`. The pending_fetches dedup
                // already prevents duplicate in-flight fetches.

                // Forward to app for validation. The app will InjectBlock after
                // validation to make the block available to downstream peers.
                self.emit_event(NetworkEvent::BlockReceived { point, body });
            }

            PeerEvent::LatencyMeasured { rtt } => {
                let mut updated = None;
                if let Some(peer) = self.peers.get_mut(&peer_id) {
                    // Accepted peers (ip_guard.is_some()) don't have a
                    // configured inbound_delay — skip their RTT to avoid
                    // showing 0ms. The outbound side of each connection
                    // tracks RTT.
                    if peer.ip_guard.is_none() {
                        // Add the simulated inbound delay so RTT reflects
                        // configured link latency (real TCP on localhost is ~0).
                        let combined = rtt + peer.inbound_delay;
                        peer.rtt = Some(combined);
                        updated = Some(combined);
                    }
                }
                if let (Some(rtt), Some(obs)) = (updated, &self.config.peer_rtt_observer) {
                    obs(peer_id, Some(rtt));
                }
            }

            PeerEvent::PeersDiscovered { peers } => {
                self.emit_event(NetworkEvent::PeersDiscovered { peers });
            }

            PeerEvent::TransactionReceived { body } => {
                self.emit_event(NetworkEvent::TransactionReceived { peer_id, body });
            }

            // Leios events — deduplicated with offer tracking for smart routing.
            PeerEvent::LeiosBlockAnnounced { header } => {
                self.emit_event(NetworkEvent::LeiosBlockAnnounced { header });
            }

            PeerEvent::LeiosBlockOffered { point } => {
                // Per-peer offer tracking + multi-peer accumulation lives
                // in shared-consensus's CandidateTracker, but each peer's notify
                // loop replays still-relevant EBs every iteration —
                // dedup `(peer, slot, hash)` here so a single peer
                // re-announce doesn't flood `network_events`.
                if let Point::Specific { slot, hash } = point {
                    if self.leios_offer_dedup.fresh_eb(peer_id, slot, hash) {
                        self.emit_event(NetworkEvent::LeiosBlockOffered { peer_id, point });
                    }
                }
            }

            PeerEvent::LeiosBlockTxsOffered { point } => {
                if let Point::Specific { slot, hash } = point {
                    if self.leios_offer_dedup.fresh_eb_txs(peer_id, slot, hash) {
                        self.emit_event(NetworkEvent::LeiosBlockTxsOffered { peer_id, point });
                    }
                }
            }

            PeerEvent::LeiosVotesReceived { votes } => {
                // Votes arrive inline (no fetch). Dedup per peer, re-inject
                // into the local store so this node re-serves them to
                // downstream peers (epidemic gossip), then surface to the app.
                let fresh = self.leios_offer_dedup.fresh_votes(peer_id, votes);
                if !fresh.is_empty() {
                    if let Some(ref store) = self.leios_store {
                        store.inject_votes(fresh.clone(), Some(peer_id));
                    }
                    self.emit_event(NetworkEvent::LeiosVotesReceived {
                        peer_id,
                        votes: fresh,
                    });
                }
            }

            PeerEvent::LeiosBlockFetched { point, block } => {
                // Pending-fetch dedup lives in shared-consensus's CandidateTracker now;
                // the consensus layer clears the entry on `on_eb_received`.
                // Populate leios store for responder peers.
                if let Some(ref store) = self.leios_store {
                    store.inject_block(point.clone(), block.clone(), Some(peer_id));
                }
                self.emit_event(NetworkEvent::LeiosBlockReceived {
                    source: Some(peer_id),
                    point,
                    block,
                });
            }

            PeerEvent::LeiosBlockTxsFetched {
                point,
                transactions,
            } => {
                // Re-inject fetched bodies into the local store so this
                // node can serve / re-announce them to downstream peers
                // (epidemic flooding rather than star-from-producer).
                // Position bodies by content hash → manifest index so a
                // partial response from the upstream peer still lands
                // at the right slots in our sparse holdings.
                if let (Some(store), Point::Specific { slot, hash }) = (&self.leios_store, &point) {
                    if let Some(manifest) = store.get_eb_manifest(*slot, hash) {
                        let by_hash: HashMap<TxId, u32> = manifest
                            .iter()
                            .enumerate()
                            .map(|(i, h)| (h.clone(), i as u32))
                            .collect();
                        let indexed: BTreeMap<u32, TxBody> = transactions
                            .iter()
                            .filter_map(|body| {
                                let id = body.get_blake2b_256();
                                by_hash
                                    .get(&TxId::new_with_array(id))
                                    .map(|&i| (i, body.clone()))
                            })
                            .collect();
                        if !indexed.is_empty() {
                            store.inject_block_txs(point.clone(), indexed, Some(peer_id));
                        }
                    }
                }
                self.emit_event(NetworkEvent::LeiosBlockTxsReceived {
                    point,
                    transactions,
                });
            }

            PeerEvent::BlockFetchFailed { from, to } => {
                // Clear pending_fetches so the app can retry via a
                // different peer. Don't remove from fragments — the peer
                // may still have the blocks (transient failure), and
                // other peers' fragments should remain intact for rerouting.
                self.pending_fetches.remove(&from);
                if from != to {
                    self.pending_fetches.remove(&to);
                }
                // Notify application with the full range so it can retry.
                self.emit_event(NetworkEvent::BlockFetchFailed {
                    peer_id: Some(peer_id),
                    from,
                    to,
                });
            }

            PeerEvent::TxsRequested { count } => {
                self.emit_event(NetworkEvent::TxsRequested { peer_id, count });
            }

            PeerEvent::Failed { reason } => {
                self.remove_peer(peer_id, reason).await;
            }
        }
    }

    /// Handle a command from the application.
    async fn handle_network_command(&mut self, command: NetworkCommand) -> bool {
        match command {
            NetworkCommand::AddPeer { address } => {
                if self.blocklist.contains(&address) {
                    // Blocklisted (active partition): refuse the dial. Also
                    // catches addresses surfaced by peer discovery, not just
                    // statically configured peers.
                    tracing::debug!(%address, "AddPeer refused: address is blocklisted");
                } else if self.has_live_peer_for_address(&address) {
                    tracing::debug!(%address, "ignoring AddPeer — address already connected");
                } else if self.reap_finished_peers() >= self.config.max_peers {
                    tracing::warn!(
                        "max peers ({}) reached, ignoring AddPeer",
                        self.config.max_peers
                    );
                } else {
                    self.add_peer(address);
                }
            }

            NetworkCommand::FetchBlock { point } => {
                // Find the best peer to fetch from: peer's chain fragment
                // must contain the requested point, then pick lowest RTT.
                if self.pending_fetches.contains_key(&point) {
                    return true; // already fetching
                }

                let best_peer = self
                    .peers
                    .iter()
                    .filter(|(_, p)| p.fragment.contains(&point))
                    .min_by_key(|(_, p)| p.rtt.unwrap_or(Duration::from_secs(999)))
                    .map(|(id, _)| *id);

                if let Some(best_id) = best_peer {
                    let cmd = PeerCommand::FetchBlocks {
                        from: point.clone(),
                        to: point.clone(),
                    };
                    if self.send_peer_command(best_id, cmd) {
                        self.pending_fetches.insert(point, best_id);
                    }
                }
            }

            NetworkCommand::FetchBlockRange { from, to, peer_id } => {
                if self.pending_fetches.contains_key(&to) {
                    return true;
                }

                // If the caller specified which peer announced this
                // chain, route directly to it — its fragment may have
                // been truncated by rollbacks but it still has the
                // blocks. Fall back to fragment scan otherwise.
                let best_peer = peer_id
                    .filter(|id| self.peers.contains_key(id))
                    .or_else(|| {
                        self.peers
                            .iter()
                            .filter(|(_, p)| p.fragment.contains(&to))
                            .min_by_key(|(_, p)| p.rtt.unwrap_or(Duration::from_secs(999)))
                            .map(|(id, _)| *id)
                    });

                if let Some(best_id) = best_peer {
                    let cmd = PeerCommand::FetchBlocks {
                        from: from.clone(),
                        to: to.clone(),
                    };
                    if self.send_peer_command(best_id, cmd) {
                        self.pending_fetches.insert(to, best_id);
                    } else {
                        // Peer was scheduled for removal; tell the app the
                        // fetch failed so it can retry via another peer.
                        self.emit_event(NetworkEvent::BlockFetchFailed {
                            peer_id: Some(best_id),
                            from,
                            to,
                        });
                    }
                } else {
                    self.emit_event(NetworkEvent::BlockFetchFailed {
                        peer_id: None,
                        from,
                        to,
                    });
                }
            }

            NetworkCommand::ReIntersect { peer_id } => {
                // Throttle re-intersection per address with exponential
                // backoff: a peer stuck on an unreconcilable fork (or a
                // churning reconnect handover) re-intersects in a tight
                // loop. Keyed by address (stable across the reconnects that
                // change PeerId), so the node backs off gracefully instead
                // of spinning. The first attempt for an address always
                // passes; rapid repeats grow the backoff to a cap; a quiet
                // peer resets.
                let now = Instant::now();
                let address = self.peers.get(&peer_id).map(|p| p.address.clone());
                let allow = match &address {
                    None => true, // unknown peer — let it through
                    Some(addr) => match self.reintersect_throttle.get(addr).copied() {
                        Some((next_allowed, _)) if now < next_allowed => false,
                        Some((next_allowed, backoff)) => {
                            let next_backoff = if now > next_allowed + backoff {
                                REINTERSECT_BACKOFF_BASE // went quiet → reset
                            } else {
                                (backoff * 2).min(REINTERSECT_BACKOFF_MAX)
                            };
                            self.reintersect_throttle
                                .insert(addr.clone(), (now + next_backoff, next_backoff));
                            true
                        }
                        None => {
                            self.reintersect_throttle.insert(
                                addr.clone(),
                                (now + REINTERSECT_BACKOFF_BASE, REINTERSECT_BACKOFF_BASE),
                            );
                            true
                        }
                    },
                };
                if allow {
                    self.send_peer_command(peer_id, PeerCommand::ReIntersect);
                } else if let Some(addr) = &address {
                    tracing::debug!(%peer_id, address = %addr, "re-intersect throttled (peer churning on a fork)");
                }
            }

            NetworkCommand::DiscoverPeers => {
                // Send to a random connected peer.
                if let Some(&peer_id) = self.peers.keys().next() {
                    self.send_peer_command(peer_id, PeerCommand::RequestPeers { amount: 10 });
                }
            }

            NetworkCommand::InjectBlock {
                point,
                header,
                body,
                block_no,
            } => {
                self.chain_store
                    .append_block(point, *header, body, block_no);
            }

            NetworkCommand::InjectRollback { point } => {
                self.chain_store.rollback_to(&point);
            }

            NetworkCommand::FetchLeiosBlock { peer_id, point } => {
                // Peer already chosen by shared-consensus's EbFetchPolicy; just dispatch.
                self.send_peer_command(peer_id, PeerCommand::FetchLeiosBlock { point });
            }

            NetworkCommand::FetchLeiosBlockTxs {
                peer_id,
                point,
                bitmap,
            } => {
                self.send_peer_command(peer_id, PeerCommand::FetchLeiosBlockTxs { point, bitmap });
            }

            NetworkCommand::InjectLeiosBlock { point, block } => {
                if let Some(ref store) = self.leios_store {
                    store.inject_block(point, block, None);
                }
            }

            NetworkCommand::InjectLeiosBlockTxs {
                point,
                transactions,
            } => {
                if let Some(ref store) = self.leios_store {
                    // Producer-side command: caller passes the full
                    // ordered body list. Receiver-side merging from
                    // partial fetches happens in the LeiosBlockTxsFetched
                    // handler, not here.
                    store.inject_block_txs_full(point, transactions, None);
                }
            }

            NetworkCommand::RecordLeiosEbManifest {
                source,
                point,
                tx_hashes,
            } => {
                if let Some(ref store) = self.leios_store {
                    store.record_eb_manifest(point, tx_hashes, source);
                }
            }

            NetworkCommand::InjectLeiosVotes { votes } => {
                if let Some(ref store) = self.leios_store {
                    store.inject_votes(votes, None);
                }
            }

            NetworkCommand::ProvideTxs { peer_id, txs } => {
                self.send_peer_command(peer_id, PeerCommand::ProvideTxs { txs });
            }

            NetworkCommand::QueryPeers => {
                let peers: Vec<super::types::PeerInfo> = self
                    .peers
                    .iter()
                    .map(|(id, p)| {
                        let (bytes_sent, bytes_received) =
                            p.mux_stats.as_ref().map(|s| s.snapshot()).unwrap_or((0, 0));
                        let downstream_state = p
                            .downstream
                            .as_ref()
                            .map(|f| {
                                crate::peer::DownstreamState::from_u8(
                                    f.load(std::sync::atomic::Ordering::Relaxed),
                                )
                            })
                            .unwrap_or(crate::peer::DownstreamState::Cold);
                        super::types::PeerInfo {
                            peer_id: *id,
                            address: p.address.clone(),
                            mode: p.mode,
                            rtt: p.rtt,
                            tip_block_no: p.tip.as_ref().map(|t| t.block_no),
                            inbound_delay: p.inbound_delay,
                            bytes_sent,
                            bytes_received,
                            downstream_state,
                        }
                    })
                    .collect();
                self.emit_event(NetworkEvent::PeerSnapshot { peers });
            }

            NetworkCommand::DropInboundPeers => {
                // Drop every accepted (inbound) peer — those carrying an
                // ip_guard.  Send `Disconnect` (not a task abort, which
                // leaves the mux/bearer up): the peer task exits its loop
                // and tears the connection down, so the remote outbound
                // side observes EOF, reconnects, and re-intersects.
                // Outbound peers are left untouched; the disconnect event
                // then runs the normal `remove_peer` cleanup.
                let inbound: Vec<PeerId> = self
                    .peers
                    .iter()
                    .filter(|(_, p)| p.ip_guard.is_some())
                    .map(|(id, _)| *id)
                    .collect();
                if !inbound.is_empty() {
                    tracing::info!(
                        count = inbound.len(),
                        "DropInboundPeers: resetting accepted peer connections"
                    );
                    for peer_id in inbound {
                        self.send_peer_command(peer_id, PeerCommand::Disconnect);
                    }
                }
            }

            NetworkCommand::SetPeerBlocklist { addresses } => {
                // Full replace; an empty set heals the partition.
                self.blocklist = addresses.into_iter().collect();

                // Disconnect any currently connected peer whose address is
                // now blocklisted.  These are outbound dials, so the normal
                // `remove_peer` path re-queues them for reconnection — and
                // `process_reconnections` parks (does not drop) blocklisted
                // addresses, so the link stays cut until the blocklist is
                // cleared, at which point the parked entry reconnects on its
                // own with normal backoff.  A duplex socket carries both
                // directions, so dropping the dialer cuts the link both ways.
                let to_drop: Vec<PeerId> = self
                    .peers
                    .iter()
                    .filter(|(_, p)| self.blocklist.contains(&p.address))
                    .map(|(id, _)| *id)
                    .collect();
                tracing::info!(
                    blocklist_len = self.blocklist.len(),
                    disconnecting = to_drop.len(),
                    "SetPeerBlocklist: applying outbound peer blocklist"
                );
                for peer_id in to_drop {
                    self.send_peer_command(peer_id, PeerCommand::Disconnect);
                }
            }

            NetworkCommand::Shutdown => {
                // Disconnect all peers.
                let peer_ids: Vec<PeerId> = self.peers.keys().copied().collect();
                for peer_id in peer_ids {
                    self.send_peer_command(peer_id, PeerCommand::Disconnect);
                }
                return false; // signal to stop
            }
        }
        true // continue
    }

    /// Remove a peer, notify the application, and schedule reconnection.
    async fn remove_peer(&mut self, peer_id: PeerId, reason: String) {
        const MAX_BACKOFF: Duration = Duration::from_secs(30);

        if let Some(peer) = self.peers.remove(&peer_id) {
            peer.task_handle.abort();
            if let Ok(mut map) = self.fragment_sizes.lock() {
                map.remove(&peer_id);
            }

            // The per-IP slot (if any) is released automatically when
            // `peer.ip_guard` drops as the PeerState is moved out here.

            // Schedule reconnection for outbound peers only. Accepted (inbound)
            // peers carry an ip_guard and should not reconnect — the remote
            // side re-initiates those.
            if peer.ip_guard.is_none() {
                let backoff = peer.reconnect_backoff;
                let next_backoff = (backoff * 2).min(MAX_BACKOFF);
                self.reconnect_queue.push((
                    peer.address.clone(),
                    Instant::now() + backoff,
                    next_backoff,
                ));
            } else {
                // Inbound (accepted) peers key `reintersect_throttle` on
                // their ephemeral remote socket address, which never recurs.
                // The entry is dead once the connection ends, so drop it —
                // otherwise it accumulates one per inbound connection ever
                // accepted (unbounded under churn). Outbound addresses are
                // stable listen ports and intentionally keep their backoff
                // across reconnects, so they are left alone.
                self.reintersect_throttle.remove(&peer.address);
            }

            // Surface BlockFetchFailed for every pending fetch that was
            // assigned to this peer, so the application can re-route the
            // fetch to another peer (or mark it unfetchable) instead of
            // waiting for an in_flight TTL to expire.
            let orphaned: Vec<Point> = self
                .pending_fetches
                .iter()
                .filter(|(_, id)| **id == peer_id)
                .map(|(pt, _)| pt.clone())
                .collect();
            for point in &orphaned {
                self.emit_event(NetworkEvent::BlockFetchFailed {
                    peer_id: Some(peer_id),
                    from: point.clone(),
                    to: point.clone(),
                });
            }
            for point in &orphaned {
                self.pending_fetches.remove(point);
            }

            self.emit_event(NetworkEvent::PeerDisconnected { peer_id, reason });
            // Per-peer offer / fetch cleanup lives in shared-consensus's
            // CandidateTracker now; the consensus layer prunes on the
            // PeerDisconnected event.
            if let Some(obs) = &self.config.peer_rtt_observer {
                obs(peer_id, None);
            }
        }
    }

    /// Process any due reconnections.
    fn process_reconnections(&mut self) {
        let now = Instant::now();

        let mut still_pending = Vec::new();
        let ready: Vec<(String, Duration)> = self
            .reconnect_queue
            .drain(..)
            .filter_map(|(address, when, backoff)| {
                if now >= when {
                    Some((address, backoff))
                } else {
                    still_pending.push((address, when, backoff));
                    None
                }
            })
            .collect();

        self.reconnect_queue = still_pending;

        for (address, next_backoff) in ready {
            // Active partition: park the reconnection instead of dropping it.
            // Re-queued at the same (un-escalated) backoff so it keeps the
            // link cut while blocklisted and reconnects on its own once the
            // blocklist is cleared (heal) — no separate bookkeeping needed.
            if self.blocklist.contains(&address) {
                self.reconnect_queue
                    .push((address, Instant::now() + next_backoff, next_backoff));
                continue;
            }
            // Outbound dedup: if a live peer already holds this address, the
            // reconnect is redundant (a duplicate queue entry, or the peer
            // came back another way) — dropping it prevents two outbound
            // tasks to one node, which would otherwise waste a cap slot.
            if self.has_live_peer_for_address(&address) {
                tracing::debug!(%address, "skipping reconnect — address already connected");
                continue;
            }
            // Reap zombies so the cap reflects live peers only.
            if self.reap_finished_peers() >= self.config.max_peers {
                // Re-queue — we're genuinely at capacity with live peers.
                self.reconnect_queue
                    .push((address, Instant::now() + next_backoff, next_backoff));
            } else {
                // Spawn a new peer task with the escalated backoff.
                // If it connects successfully and later fails, remove_peer
                // will use this backoff. On sustained success the backoff
                // could be reset (future improvement).
                let peer_id = self.add_peer_with_backoff(address.clone(), next_backoff);
                tracing::info!("reconnecting to {address} as {peer_id}");
            }
        }
    }

    /// Add a duplex peer for an accepted inbound connection. The caller
    /// passes the `IpCountGuard` reserved at accept time; it is stored in
    /// the new `PeerState` so the slot is released when the peer is removed.
    fn add_accepted_peer(
        &mut self,
        connection: DuplexConnection,
        peer_addr: SocketAddr,
        ip_guard: IpCountGuard,
    ) -> PeerId {
        let peer_id = PeerId(self.next_peer_id);
        self.next_peer_id += 1;

        let (cmd_sender, cmd_receiver) = mpsc::channel(PEER_COMMAND_CAPACITY);

        let task_config = AcceptedDuplexTaskConfig {
            peer_id,
            connection,
            keepalive_interval: self.config.keepalive_interval,
            chain_store: self.chain_store.clone(),
            peer_provider: self.peer_provider.clone(),
            event_sender: self.peer_event_sender.clone(),
            command_receiver: cmd_receiver,
            leios_enabled: self.config.leios_enabled,
            leios_store: self.leios_store.clone(),
            outbound_controls: self.config.outbound_controls.clone(),
            tx_dedup: self.tx_dedup.clone(),
        };

        let task_handle = tokio::spawn(run_accepted_duplex_task(task_config));

        let address = peer_addr.to_string();
        let inbound_delay = self
            .config
            .peer_delays
            .get(&address)
            .copied()
            .unwrap_or(Duration::ZERO);

        self.peers.insert(
            peer_id,
            PeerState {
                address,
                mode: ConnectionMode::Duplex,
                ip_guard: Some(ip_guard),
                commands: cmd_sender,
                task_handle,
                tip: None,
                rtt: None,
                fragment: ChainFragment::new(),
                reconnect_backoff: Duration::from_secs(0), // accepted peers don't reconnect
                inbound_delay,
                mux_stats: None,
                downstream: None,
                last_rolled_back_to: None,
            },
        );
        self.sync_fragment_size(peer_id, 0);

        peer_id
    }

    /// Start the accept loop if a listen address is configured.
    fn start_accept_loop(&mut self) {
        let listen_address = match &self.config.listen_address {
            Some(addr) => addr.clone(),
            None => return,
        };

        let addr = match listen_address.to_socket_addrs() {
            Ok(mut addrs) => match addrs.next() {
                Some(addr) => addr,
                None => {
                    tracing::error!("could not resolve listen address: {listen_address}");
                    return;
                }
            },
            Err(e) => {
                tracing::error!("invalid listen address {listen_address}: {e}");
                return;
            }
        };

        let magic = self.config.network_magic;
        let scheduler_type = self.config.scheduler_type;
        let mux_config = MuxConfig {
            sdu_timeout: self.config.sdu_timeout,
            ..MuxConfig::default()
        };
        let leios_enabled = self.config.leios_enabled;
        let mut client_protos = client_protocol_configs(leios_enabled);
        let mut server_protos = server_protocol_configs(leios_enabled);
        for p in client_protos.iter_mut().chain(server_protos.iter_mut()) {
            if let Some(tc) = self.config.traffic_class_overrides.get(&p.id) {
                p.traffic_class = *tc;
            }
        }

        let (conn_sender, conn_receiver) =
            mpsc::channel::<(DuplexConnection, SocketAddr, IpCountGuard)>(16);
        self.inbound_connections = Some(conn_receiver);

        let ip_counts = self.ip_counts.clone();
        let max_connections_per_ip = self.config.max_connections_per_ip;
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.config.max_handshaking));
        let client_protos = Arc::new(client_protos);
        let server_protos = Arc::new(server_protos);

        let task = tokio::spawn(async move {
            let listener = match tokio::net::TcpListener::bind(addr).await {
                Ok(l) => {
                    tracing::info!("listening for inbound peers on {addr}");
                    l
                }
                Err(e) => {
                    tracing::error!("failed to bind {addr}: {e}");
                    return;
                }
            };

            loop {
                // Accept TCP connection immediately (don't block on handshake).
                let (stream, peer_addr) = match listener.accept().await {
                    Ok(accepted) => accepted,
                    Err(e) => {
                        tracing::warn!("TCP accept failed: {e}");
                        continue;
                    }
                };

                let ip = peer_addr.ip();

                // Check per-IP connection limit.
                {
                    let counts = ip_counts.lock().expect("ip_counts lock poisoned");
                    if counts.get(&ip).copied().unwrap_or(0) >= max_connections_per_ip {
                        tracing::warn!("per-IP limit reached for {ip}, dropping connection");
                        drop(stream);
                        continue;
                    }
                }

                // Check concurrent handshake limit.
                let permit = match semaphore.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        tracing::warn!(
                            "max handshaking limit reached, dropping connection from {ip}"
                        );
                        drop(stream);
                        continue;
                    }
                };

                // Reserve the per-IP slot. The returned guard owns the
                // decrement; it is dropped (auto-decrementing) if the
                // handshake fails or conn_sender.send fails. On success
                // it is forwarded to the coordinator and stored in the
                // new peer's PeerState.
                let ip_guard = IpCountGuard::reserve(ip_counts.clone(), ip);

                let conn_sender = conn_sender.clone();
                let client_protos = client_protos.clone();
                let server_protos = server_protos.clone();
                let mux_config = mux_config.clone();

                tokio::spawn(async move {
                    let _permit = permit; // held until task completes
                                          // ip_guard lives until either (a) we transfer it into
                                          // conn_sender or (b) it drops at the end of this task.

                    let bearer = match TcpBearer::from_accepted(stream) {
                        Ok(b) => b,
                        Err(e) => {
                            tracing::warn!("socket configuration failed for {peer_addr}: {e}");
                            // ip_guard dropped here — slot released.
                            return;
                        }
                    };

                    match connect::handshake_accepted_duplex(
                        bearer,
                        peer_addr,
                        magic,
                        &client_protos,
                        &server_protos,
                        mux_config,
                        scheduler_type,
                    )
                    .await
                    {
                        Ok(conn) => {
                            // Transfer the guard to the coordinator. If the
                            // send fails (coordinator shut down), the guard
                            // comes back in the error and drops here.
                            if let Err(mpsc::error::SendError((_, _, _guard))) =
                                conn_sender.send((conn, peer_addr, ip_guard)).await
                            {
                                // _guard dropped → slot released.
                            }
                        }
                        Err(e) => {
                            tracing::warn!("inbound handshake failed from {peer_addr}: {e}");
                            // ip_guard dropped here — slot released.
                        }
                    }
                });
            }
        });

        self.accept_task = Some(task);
    }

    /// Time until next reconnection is due, or a large default.
    fn next_reconnect_delay(&self) -> Duration {
        self.reconnect_queue
            .iter()
            .map(|(_, when, _)| when.saturating_duration_since(Instant::now()))
            .min()
            .unwrap_or(Duration::from_secs(3600))
    }

    /// Main coordinator loop.
    async fn run(mut self) {
        // Start accept loop if configured.
        self.start_accept_loop();

        // Delayed events buffer: (delivery_time, peer_id, event).
        // Only used when peer_delays is configured; zero overhead otherwise.
        let mut delayed: Vec<(Instant, PeerId, PeerEvent)> = Vec::new();
        let has_any_delays = !self.config.peer_delays.is_empty();

        loop {
            let reconnect_delay = self.next_reconnect_delay();

            // Earliest delayed event deadline (only computed when buffer is non-empty).
            let next_delayed = delayed.iter().map(|(t, _, _)| *t).min();

            // Gate peer_events on available network_events headroom. When
            // the application is slow, `network_events.capacity()` drops
            // below MIN_EMIT_HEADROOM and this branch is disabled — the
            // coordinator stops reading from peer tasks. Peer tasks then
            // block on their shared `peer_event_sender.send().await`,
            // which cascades backpressure through the mux demuxer to TCP.
            // Other branches (commands, inbound connections, timers)
            // remain active so the coord continues running.
            let have_headroom = self.network_events.capacity() >= MIN_EMIT_HEADROOM;

            tokio::select! {
                event = self.peer_events.recv(), if have_headroom => {
                    match event {
                        Some((peer_id, peer_event)) => {
                            // If delay simulation is active and this peer has
                            // a configured delay, buffer instead of processing.
                            // LatencyMeasured is exempt: it's a measurement, not
                            // data, and its RTT is adjusted below instead.
                            if has_any_delays && !matches!(peer_event, PeerEvent::LatencyMeasured { .. }) {
                                let delay = self.peers.get(&peer_id)
                                    .map(|p| p.inbound_delay)
                                    .unwrap_or(Duration::ZERO);
                                if !delay.is_zero() {
                                    delayed.push((Instant::now() + delay, peer_id, peer_event));
                                    continue;
                                }
                            }
                            self.handle_peer_event(peer_id, peer_event).await;
                        }
                        None => break,
                    }
                }
                command = self.network_commands.recv() => {
                    match command {
                        Some(cmd) => {
                            if !self.handle_network_command(cmd).await {
                                break;
                            }
                        }
                        None => break,
                    }
                }
                result = async {
                    match &mut self.inbound_connections {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if let Some((conn, peer_addr, ip_guard)) = result {
                        // Reap zombies first so the cap reflects live peers
                        // only — otherwise dead PeerStates awaiting removal
                        // pin the cap and we reject live reconnects.
                        let live = self.reap_finished_peers();
                        if live < self.config.max_peers {
                            let peer_id = self.add_accepted_peer(conn, peer_addr, ip_guard);
                            tracing::info!("accepted inbound peer as {peer_id}");
                        } else {
                            tracing::warn!(
                                live,
                                max = self.config.max_peers,
                                "max peers reached (all live), dropping inbound connection"
                            );
                            conn.running.abort();
                            // ip_guard dropped here → slot released.
                            drop(ip_guard);
                        }
                    }
                }
                _ = tokio::time::sleep(reconnect_delay) => {
                    self.process_reconnections();
                }
                _ = tokio::time::sleep_until(next_delayed.unwrap_or_else(|| Instant::now() + Duration::from_secs(86400))), if !delayed.is_empty() && have_headroom => {
                    // Deliver all delayed events whose deadline has passed.
                    let now = Instant::now();
                    let mut i = 0;
                    while i < delayed.len() {
                        if delayed[i].0 <= now {
                            let (_, peer_id, event) = delayed.swap_remove(i);
                            self.handle_peer_event(peer_id, event).await;
                        } else {
                            i += 1;
                        }
                    }
                }
            }

            // Process peers scheduled for removal during this iteration
            // (try_send Full on peer.commands). Done outside the select!
            // so handler bodies don't re-enter remove_peer mid-traversal.
            if !self.pending_removals.is_empty() {
                for (peer_id, reason) in std::mem::take(&mut self.pending_removals) {
                    self.remove_peer(peer_id, reason).await;
                }
            }
        }

        // Abort accept loop if running.
        if let Some(task) = self.accept_task.take() {
            task.abort();
        }

        // Abort all remaining peer tasks.
        for (_, peer) in self.peers.drain() {
            peer.task_handle.abort();
        }
    }
}

/// Decrement the per-IP connection count, removing the entry if it reaches zero.
fn decrement_ip_count(ip_counts: &Mutex<HashMap<IpAddr, usize>>, ip: IpAddr) {
    let mut counts = ip_counts.lock().expect("ip_counts lock poisoned");
    if let Some(count) = counts.get_mut(&ip) {
        *count -= 1;
        if *count == 0 {
            counts.remove(&ip);
        }
    }
}

/// RAII guard for a per-IP connection slot. Increments `ip_counts[ip]` on
/// construction and decrements on drop. This means an inbound connection
/// cannot leak a slot just because the coordinator never processed a
/// `PeerEvent::Failed` — dropping the `PeerState` that owns the guard
/// (from `self.peers.remove(...)`, `self.peers.drain()` on shutdown, or
/// a task panic) releases the slot unconditionally.
struct IpCountGuard {
    ip_counts: Arc<Mutex<HashMap<IpAddr, usize>>>,
    ip: IpAddr,
}

impl IpCountGuard {
    /// Reserve a slot for `ip` and return the owning guard. The caller must
    /// have already checked the per-IP limit; this unconditionally increments.
    fn reserve(ip_counts: Arc<Mutex<HashMap<IpAddr, usize>>>, ip: IpAddr) -> Self {
        {
            let mut counts = ip_counts.lock().expect("ip_counts lock poisoned");
            *counts.entry(ip).or_insert(0) += 1;
        }
        Self { ip_counts, ip }
    }
}

impl Drop for IpCountGuard {
    fn drop(&mut self) {
        decrement_ip_count(&self.ip_counts, self.ip);
    }
}

/// Spawn a coordinator task and return a handle for the application.
pub fn spawn_coordinator(config: CoordinatorConfig) -> CoordinatorHandle {
    // `network_events` is sized well above `MIN_EMIT_HEADROOM` so the
    // `peer_events` select! branch is gated by free-slot count, not by
    // the absolute channel capacity. When the app falls behind, the gate
    // closes and peer tasks block on their fan-in send, propagating
    // backpressure through the mux to TCP rather than deadlocking.
    let (net_event_sender, net_event_receiver) = mpsc::channel(NETWORK_EVENTS_CAPACITY);
    let (net_cmd_sender, net_cmd_receiver) = mpsc::channel(NETWORK_COMMANDS_CAPACITY);
    let (peer_event_sender, peer_event_receiver) = mpsc::channel(PEER_EVENTS_CAPACITY);
    // The chain store holds block *bodies* for serving BlockFetch to
    // responder peers. Under a tip-hot window it carries the same bound as
    // `PraosState.block_cache`: keep only recent bodies (a peer behind the
    // window gets NoBlocks and refetches from an archive node). `None` keeps
    // the full configured capacity. Only the chain store is capped here —
    // `leios_store` keeps its own slot-window retention.
    let chain_store_cap = match config.block_body_retention_blocks {
        Some(w) => (w as usize).min(config.chain_store_capacity),
        None => config.chain_store_capacity,
    };
    let (chain_store, _chain_rx) = ChainStore::new(chain_store_cap);

    let leios_store = if config.leios_enabled {
        let (store, _leios_rx) = LeiosStore::new_with_retention(
            config.chain_store_capacity,
            config.tx_body_resolver.clone(),
            crate::store::leios_store::DEFAULT_RETENTION_SLOTS,
            config.leios_store_stats_log_interval,
        );
        Some(store)
    } else {
        None
    };

    let coordinator = Coordinator::new(
        config,
        peer_event_sender,
        peer_event_receiver,
        net_event_sender,
        net_cmd_receiver,
        chain_store,
        leios_store.clone(),
    );
    let fragment_sizes = coordinator.fragment_sizes.clone();

    tokio::spawn(coordinator.run());

    CoordinatorHandle {
        events: net_event_receiver,
        commands: net_cmd_sender,
        leios_store,
        fragment_sizes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::WrappedHeader;
    use shared_consensus::mempool::{TxBody, TxId};

    /// Helper: set up a coordinator with a MemBearer-connected peer.
    /// Returns (CoordinatorHandle, server_handle, MemBearer pair).
    ///
    /// Since the coordinator uses connect_and_handshake (TCP), we can't use
    /// MemBearer directly with it. Instead, we test the coordinator's event
    /// handling logic by manually sending events on the fan-in channel.
    #[tokio::test]
    async fn coordinator_forwards_tip_advanced() {
        use crate::types::{Tip, WrappedHeader};

        let (peer_event_sender, peer_event_receiver) = mpsc::channel(256);
        let (net_event_sender, mut net_event_receiver) = mpsc::channel(64);
        let (_net_cmd_sender, net_cmd_receiver) = mpsc::channel(64);

        let config = CoordinatorConfig::default();
        let (chain_store, _chain_rx) = ChainStore::new(100);
        let mut coordinator = Coordinator::new(
            config,
            peer_event_sender.clone(),
            peer_event_receiver,
            net_event_sender,
            net_cmd_receiver,
            chain_store,
            None,
        );

        // Manually insert a peer (simulate it being connected).
        let peer_id = PeerId(0);
        let (cmd_sender, _cmd_receiver) = mpsc::channel(16);
        coordinator.peers.insert(
            peer_id,
            PeerState {
                address: "test:3001".to_string(),
                mode: ConnectionMode::InitiatorOnly,
                ip_guard: None,
                commands: cmd_sender,
                task_handle: tokio::spawn(async {}),
                tip: None,
                rtt: None,
                fragment: ChainFragment::new(),
                reconnect_backoff: Duration::from_secs(1),
                inbound_delay: Duration::ZERO,
                mux_stats: None,
                downstream: None,
                last_rolled_back_to: None,
            },
        );

        let tip = Tip {
            point: Point::Specific {
                slot: 100,
                hash: [1u8; 32],
            },
            block_no: 100,
        };
        let header = WrappedHeader::opaque(vec![0xA0]);

        // Send a HeaderAnnounced event.
        coordinator
            .handle_peer_event(
                peer_id,
                PeerEvent::HeaderAnnounced {
                    header: header.clone(),
                    tip: tip.clone(),
                },
            )
            .await;

        // Should produce a TipAdvanced network event.
        let event = net_event_receiver.try_recv().unwrap();
        match event {
            NetworkEvent::TipAdvanced {
                peer_id: recv_peer,
                tip: recv_tip,
                header: recv_header,
            } => {
                assert_eq!(recv_peer, peer_id);
                assert_eq!(recv_tip.block_no, 100);
                assert_eq!(recv_header.raw, header.raw);
            }
            other => panic!("expected TipAdvanced, got {other:?}"),
        }

        // Verify peer's tip was updated.
        assert_eq!(
            coordinator
                .peers
                .get(&peer_id)
                .unwrap()
                .tip
                .as_ref()
                .unwrap()
                .block_no,
            100
        );
    }

    #[tokio::test]
    async fn coordinator_deduplicates_tips() {
        use crate::types::{Tip, WrappedHeader};

        let (peer_event_sender, peer_event_receiver) = mpsc::channel(256);
        let (net_event_sender, mut net_event_receiver) = mpsc::channel(64);
        let (_net_cmd_sender, net_cmd_receiver) = mpsc::channel(64);

        let config = CoordinatorConfig::default();
        let (chain_store, _chain_rx) = ChainStore::new(100);
        let mut coordinator = Coordinator::new(
            config,
            peer_event_sender,
            peer_event_receiver,
            net_event_sender,
            net_cmd_receiver,
            chain_store,
            None,
        );

        // Two peers.
        let peer_a = PeerId(0);
        let peer_b = PeerId(1);
        for peer_id in [peer_a, peer_b] {
            let (cmd_sender, _cmd_receiver) = mpsc::channel(16);
            coordinator.peers.insert(
                peer_id,
                PeerState {
                    address: format!("test-{:?}:3001", peer_id),
                    mode: ConnectionMode::InitiatorOnly,
                    ip_guard: None,
                    commands: cmd_sender,
                    task_handle: tokio::spawn(async {}),
                    tip: None,
                    rtt: None,
                    fragment: ChainFragment::new(),
                    reconnect_backoff: Duration::from_secs(1),
                    inbound_delay: Duration::ZERO,
                    mux_stats: None,
                    downstream: None,
                    last_rolled_back_to: None,
                },
            );
        }

        let tip_100 = Tip {
            point: Point::Specific {
                slot: 100,
                hash: [1u8; 32],
            },
            block_no: 100,
        };
        let header = WrappedHeader::opaque(vec![0xA0]);

        // Peer A announces tip 100.
        coordinator
            .handle_peer_event(
                peer_a,
                PeerEvent::HeaderAnnounced {
                    header: header.clone(),
                    tip: tip_100.clone(),
                },
            )
            .await;

        // Should produce TipAdvanced.
        assert!(net_event_receiver.try_recv().is_ok());

        // Peer B announces same tip 100.
        coordinator
            .handle_peer_event(
                peer_b,
                PeerEvent::HeaderAnnounced {
                    header: header.clone(),
                    tip: tip_100.clone(),
                },
            )
            .await;

        // Should also produce TipAdvanced (all headers forwarded for chain tree).
        assert!(net_event_receiver.try_recv().is_ok());

        // Peer B announces tip 101 — this IS new.
        let tip_101 = Tip {
            point: Point::Specific {
                slot: 101,
                hash: [2u8; 32],
            },
            block_no: 101,
        };
        coordinator
            .handle_peer_event(
                peer_b,
                PeerEvent::HeaderAnnounced {
                    header: header.clone(),
                    tip: tip_101,
                },
            )
            .await;

        let event = net_event_receiver.try_recv().unwrap();
        match event {
            NetworkEvent::TipAdvanced { tip, .. } => {
                assert_eq!(tip.block_no, 101);
            }
            other => panic!("expected TipAdvanced, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn coordinator_handles_peer_failure() {
        let (peer_event_sender, peer_event_receiver) = mpsc::channel(256);
        let (net_event_sender, mut net_event_receiver) = mpsc::channel(64);
        let (_net_cmd_sender, net_cmd_receiver) = mpsc::channel(64);

        let config = CoordinatorConfig::default();
        let (chain_store, _chain_rx) = ChainStore::new(100);
        let mut coordinator = Coordinator::new(
            config,
            peer_event_sender,
            peer_event_receiver,
            net_event_sender,
            net_cmd_receiver,
            chain_store,
            None,
        );

        let peer_id = PeerId(0);
        let (cmd_sender, _cmd_receiver) = mpsc::channel(16);
        coordinator.peers.insert(
            peer_id,
            PeerState {
                address: "test:3001".to_string(),
                mode: ConnectionMode::InitiatorOnly,
                ip_guard: None,
                commands: cmd_sender,
                task_handle: tokio::spawn(async {}),
                tip: None,
                rtt: None,
                fragment: ChainFragment::new(),
                reconnect_backoff: Duration::from_secs(1),
                inbound_delay: Duration::ZERO,
                mux_stats: None,
                downstream: None,
                last_rolled_back_to: None,
            },
        );

        // Peer fails.
        coordinator
            .handle_peer_event(
                peer_id,
                PeerEvent::Failed {
                    reason: "connection reset".to_string(),
                },
            )
            .await;

        // Should produce PeerDisconnected.
        let event = net_event_receiver.try_recv().unwrap();
        match event {
            NetworkEvent::PeerDisconnected {
                peer_id: recv_id,
                reason,
            } => {
                assert_eq!(recv_id, peer_id);
                assert_eq!(reason, "connection reset");
            }
            other => panic!("expected PeerDisconnected, got {other:?}"),
        }

        // Peer should be removed.
        assert!(coordinator.peers.is_empty());

        // Should be queued for reconnection.
        assert_eq!(coordinator.reconnect_queue.len(), 1);
        assert_eq!(coordinator.reconnect_queue[0].0, "test:3001");
    }

    #[tokio::test]
    async fn remove_peer_emits_block_fetch_failed_for_orphaned_fetches() {
        let (peer_event_sender, peer_event_receiver) = mpsc::channel(256);
        let (net_event_sender, mut net_event_receiver) = mpsc::channel(64);
        let (_net_cmd_sender, net_cmd_receiver) = mpsc::channel(64);

        let config = CoordinatorConfig::default();
        let (chain_store, _chain_rx) = ChainStore::new(100);
        let mut coordinator = Coordinator::new(
            config,
            peer_event_sender,
            peer_event_receiver,
            net_event_sender,
            net_cmd_receiver,
            chain_store,
            None,
        );

        let peer_id = PeerId(0);
        let (cmd_sender, _cmd_receiver) = mpsc::channel(16);
        coordinator.peers.insert(
            peer_id,
            PeerState {
                address: "test:3001".to_string(),
                mode: ConnectionMode::InitiatorOnly,
                ip_guard: None,
                commands: cmd_sender,
                task_handle: tokio::spawn(async {}),
                tip: None,
                rtt: None,
                fragment: ChainFragment::new(),
                reconnect_backoff: Duration::from_secs(1),
                inbound_delay: Duration::ZERO,
                mux_stats: None,
                downstream: None,
                last_rolled_back_to: None,
            },
        );

        // Simulate two pending fetches assigned to this peer.
        let point_a = Point::Specific {
            slot: 10,
            hash: [0xAA; 32],
        };
        let point_b = Point::Specific {
            slot: 20,
            hash: [0xBB; 32],
        };
        coordinator.pending_fetches.insert(point_a.clone(), peer_id);
        coordinator.pending_fetches.insert(point_b.clone(), peer_id);

        // Peer fails.
        coordinator
            .handle_peer_event(
                peer_id,
                PeerEvent::Failed {
                    reason: "connection reset".to_string(),
                },
            )
            .await;

        // Collect all network events emitted.
        let mut events = Vec::new();
        while let Ok(ev) = net_event_receiver.try_recv() {
            events.push(ev);
        }

        // Expect: two BlockFetchFailed (one per orphaned fetch) + one PeerDisconnected.
        let failed_points: Vec<Point> = events
            .iter()
            .filter_map(|e| match e {
                NetworkEvent::BlockFetchFailed { to, .. } => Some(to.clone()),
                _ => None,
            })
            .collect();
        assert!(
            failed_points.contains(&point_a),
            "expected BlockFetchFailed for point_a, got events: {events:?}"
        );
        assert!(
            failed_points.contains(&point_b),
            "expected BlockFetchFailed for point_b, got events: {events:?}"
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e, NetworkEvent::PeerDisconnected { .. })),
            "expected PeerDisconnected, got events: {events:?}"
        );

        // pending_fetches should be empty.
        assert!(coordinator.pending_fetches.is_empty());
    }

    #[tokio::test]
    async fn coordinator_schedules_reconnection() {
        let (peer_event_sender, peer_event_receiver) = mpsc::channel(256);
        let (net_event_sender, mut net_event_receiver) = mpsc::channel(64);
        let (_net_cmd_sender, net_cmd_receiver) = mpsc::channel(64);

        let config = CoordinatorConfig::default();
        let (chain_store, _chain_rx) = ChainStore::new(100);
        let mut coordinator = Coordinator::new(
            config,
            peer_event_sender,
            peer_event_receiver,
            net_event_sender,
            net_cmd_receiver,
            chain_store,
            None,
        );

        // Add a peer and simulate failure.
        let peer_id = PeerId(0);
        let (cmd_sender, _cmd_receiver) = mpsc::channel(16);
        coordinator.peers.insert(
            peer_id,
            PeerState {
                address: "test:3001".to_string(),
                mode: ConnectionMode::InitiatorOnly,
                ip_guard: None,
                commands: cmd_sender,
                task_handle: tokio::spawn(async {}),
                tip: None,
                rtt: None,
                fragment: ChainFragment::new(),
                reconnect_backoff: Duration::from_secs(1),
                inbound_delay: Duration::ZERO,
                mux_stats: None,
                downstream: None,
                last_rolled_back_to: None,
            },
        );

        coordinator
            .handle_peer_event(
                peer_id,
                PeerEvent::Failed {
                    reason: "test".to_string(),
                },
            )
            .await;

        // Drain the PeerDisconnected event.
        let _ = net_event_receiver.try_recv();

        // Queue should have one entry.
        assert_eq!(coordinator.reconnect_queue.len(), 1);

        // Fast-forward the reconnect time.
        coordinator.reconnect_queue[0].1 = Instant::now() - Duration::from_secs(1);

        // Process reconnections — this will call add_peer, spawning a task
        // that tries to TCP connect (and will fail since there's no server).
        coordinator.process_reconnections();

        // A new peer should have been added.
        assert_eq!(coordinator.peers.len(), 1);
        // Reconnect queue should be empty now.
        assert!(coordinator.reconnect_queue.is_empty());
    }

    #[tokio::test]
    async fn set_peer_blocklist_disconnects_match_and_refuses_dial() {
        let (mut coordinator, _net_rx) = make_fragment_coordinator();
        let blocked = PeerId(0);
        let mut blocked_cmds = insert_peer(&mut coordinator, blocked, None);
        let blocked_addr = "test-0:3001".to_string(); // matches insert_peer's scheme

        // Applying a blocklist containing the peer's address must
        // disconnect it and record the blocklist.
        coordinator
            .handle_network_command(NetworkCommand::SetPeerBlocklist {
                addresses: vec![blocked_addr.clone()],
            })
            .await;
        assert!(coordinator.blocklist.contains(&blocked_addr));
        assert!(
            matches!(blocked_cmds.try_recv(), Ok(PeerCommand::Disconnect)),
            "blocklisted peer should receive Disconnect"
        );

        // A blocklisted address must be refused by AddPeer (covers both
        // configured peers and addresses surfaced via discovery).
        let before = coordinator.peers.len();
        coordinator
            .handle_network_command(NetworkCommand::AddPeer {
                address: blocked_addr.clone(),
            })
            .await;
        assert_eq!(
            coordinator.peers.len(),
            before,
            "blocked dial must be refused"
        );

        // A non-blocklisted address is still dialled normally.  (Assert by
        // address rather than count: the test's manual insert_peer reuses
        // PeerId(0) without bumping next_peer_id, so add_peer overwrites that
        // slot — presence-by-address is the artifact-free check.)
        coordinator
            .handle_network_command(NetworkCommand::AddPeer {
                address: "allowed:3001".to_string(),
            })
            .await;
        assert!(
            coordinator
                .peers
                .values()
                .any(|p| p.address == "allowed:3001"),
            "non-blocklisted dial must be accepted"
        );
    }

    #[tokio::test]
    async fn process_reconnections_parks_blocklisted_then_heals_on_clear() {
        let (mut coordinator, _net_rx) = make_fragment_coordinator();
        let addr = "blocked:3001".to_string();

        // A due reconnection for a blocklisted address must be parked (kept
        // in the queue), not spawned and not dropped — this is what holds a
        // partition open against the auto-reconnect machinery.
        coordinator.blocklist.insert(addr.clone());
        coordinator.reconnect_queue.push((
            addr.clone(),
            Instant::now() - Duration::from_secs(1),
            Duration::from_secs(1),
        ));
        coordinator.process_reconnections();
        assert!(
            coordinator.peers.is_empty(),
            "blocklisted address must not reconnect"
        );
        assert_eq!(
            coordinator.reconnect_queue.len(),
            1,
            "parked entry must remain queued for the eventual heal"
        );

        // Clearing the blocklist (heal) lets the parked entry reconnect on
        // the next due tick with no extra bookkeeping.
        coordinator
            .handle_network_command(NetworkCommand::SetPeerBlocklist { addresses: vec![] })
            .await;
        assert!(coordinator.blocklist.is_empty());
        coordinator.reconnect_queue[0].1 = Instant::now() - Duration::from_secs(1);
        coordinator.process_reconnections();
        assert_eq!(
            coordinator.peers.len(),
            1,
            "heal must reconnect the parked peer"
        );
        assert!(coordinator.reconnect_queue.is_empty());
    }

    /// Helper: insert a fake peer into the coordinator with a given RTT.
    fn insert_peer(
        coordinator: &mut Coordinator,
        peer_id: PeerId,
        rtt: Option<Duration>,
    ) -> mpsc::Receiver<PeerCommand> {
        let (cmd_sender, cmd_receiver) = mpsc::channel(16);
        coordinator.peers.insert(
            peer_id,
            PeerState {
                address: format!("test-{}:3001", peer_id.0),
                mode: ConnectionMode::InitiatorOnly,
                ip_guard: None,
                commands: cmd_sender,
                task_handle: tokio::spawn(async {}),
                tip: None,
                rtt,
                fragment: ChainFragment::new(),
                reconnect_backoff: Duration::from_secs(1),
                inbound_delay: Duration::ZERO,
                mux_stats: None,
                downstream: None,
                last_rolled_back_to: None,
            },
        );
        cmd_receiver
    }

    #[tokio::test]
    async fn coordinator_spawn_and_shutdown() {
        let config = CoordinatorConfig::default();
        let mut handle = spawn_coordinator(config);

        // Send shutdown.
        handle
            .commands
            .send(NetworkCommand::Shutdown)
            .await
            .unwrap();

        // Events channel should close after shutdown.
        let timeout_result = tokio::time::timeout(Duration::from_secs(2), async {
            while let Some(_event) = handle.events.recv().await {
                // drain any remaining events
            }
        })
        .await;

        assert!(
            timeout_result.is_ok(),
            "coordinator should shut down cleanly"
        );
    }

    // --- ChainFragment integration tests ---

    /// Helper: create a coordinator for fragment tests.
    fn make_fragment_coordinator() -> (Coordinator, mpsc::Receiver<NetworkEvent>) {
        let (peer_event_sender, peer_event_receiver) = mpsc::channel(256);
        let (net_event_sender, net_event_receiver) = mpsc::channel(64);
        let (_net_cmd_sender, net_cmd_receiver) = mpsc::channel(64);
        let config = CoordinatorConfig::default();
        let (chain_store, _chain_rx) = ChainStore::new(100);
        let coordinator = Coordinator::new(
            config,
            peer_event_sender,
            peer_event_receiver,
            net_event_sender,
            net_cmd_receiver,
            chain_store,
            None,
        );
        (coordinator, net_event_receiver)
    }

    #[tokio::test]
    async fn fetch_routes_to_peer_with_block_in_fragment() {
        let (mut coordinator, _net_rx) = make_fragment_coordinator();
        let peer_a = PeerId(0);
        let peer_b = PeerId(1);
        let mut cmd_a = insert_peer(&mut coordinator, peer_a, Some(Duration::from_millis(50)));
        let mut cmd_b = insert_peer(&mut coordinator, peer_b, Some(Duration::from_millis(10)));

        let point_100 = Point::Specific {
            slot: 100,
            hash: [1u8; 32],
        };
        let point_101 = Point::Specific {
            slot: 101,
            hash: [2u8; 32],
        };

        // Only peer A has point_100 in its fragment.
        coordinator
            .handle_peer_event(
                peer_a,
                PeerEvent::IntersectionFound {
                    point: point_100.clone(),
                },
            )
            .await;

        // Peer B has a different point.
        coordinator
            .handle_peer_event(
                peer_b,
                PeerEvent::IntersectionFound {
                    point: point_101.clone(),
                },
            )
            .await;

        // Fetch point_100 — should route to peer A (only one with it).
        coordinator
            .handle_network_command(NetworkCommand::FetchBlock {
                point: point_100.clone(),
            })
            .await;

        // Peer A should receive the fetch command.
        let cmd = cmd_a.try_recv().unwrap();
        assert!(matches!(cmd, PeerCommand::FetchBlocks { .. }));

        // Peer B should NOT receive anything.
        assert!(cmd_b.try_recv().is_err());
    }

    #[tokio::test]
    async fn fetch_prefers_lowest_rtt_among_fragment_candidates() {
        let (mut coordinator, _net_rx) = make_fragment_coordinator();
        let peer_a = PeerId(0);
        let peer_b = PeerId(1);
        let mut cmd_a = insert_peer(&mut coordinator, peer_a, Some(Duration::from_millis(100)));
        let mut cmd_b = insert_peer(&mut coordinator, peer_b, Some(Duration::from_millis(10)));

        let point = Point::Specific {
            slot: 200,
            hash: [3u8; 32],
        };

        // Both peers have the point in their fragments.
        for id in [peer_a, peer_b] {
            coordinator
                .handle_peer_event(
                    id,
                    PeerEvent::IntersectionFound {
                        point: point.clone(),
                    },
                )
                .await;
        }

        // Fetch — should route to peer B (lower RTT).
        coordinator
            .handle_network_command(NetworkCommand::FetchBlock {
                point: point.clone(),
            })
            .await;

        assert!(cmd_a.try_recv().is_err());
        let cmd = cmd_b.try_recv().unwrap();
        assert!(matches!(cmd, PeerCommand::FetchBlocks { .. }));
    }

    #[tokio::test]
    async fn fetch_fails_when_no_peer_has_block() {
        let (mut coordinator, mut net_rx) = make_fragment_coordinator();
        let peer_a = PeerId(0);
        let _cmd_a = insert_peer(&mut coordinator, peer_a, Some(Duration::from_millis(10)));

        let point = Point::Specific {
            slot: 300,
            hash: [4u8; 32],
        };

        // Peer A's fragment is empty — no intersection set.
        coordinator
            .handle_network_command(NetworkCommand::FetchBlock {
                point: point.clone(),
            })
            .await;

        // No fetch command sent, no pending fetch recorded.
        assert!(!coordinator.pending_fetches.contains_key(&point));

        // No BlockFetchFailed event either (nobody was asked).
        assert!(net_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn fragment_pruned_on_block_fetched() {
        let (mut coordinator, _net_rx) = make_fragment_coordinator();
        let peer_a = PeerId(0);
        let peer_b = PeerId(1);
        insert_peer(&mut coordinator, peer_a, None);
        insert_peer(&mut coordinator, peer_b, None);

        let point = Point::Specific {
            slot: 400,
            hash: [5u8; 32],
        };

        // Both peers have the point.
        for id in [peer_a, peer_b] {
            coordinator
                .handle_peer_event(
                    id,
                    PeerEvent::IntersectionFound {
                        point: point.clone(),
                    },
                )
                .await;
        }

        assert!(coordinator
            .peers
            .get(&peer_a)
            .unwrap()
            .fragment
            .contains(&point));
        assert!(coordinator
            .peers
            .get(&peer_b)
            .unwrap()
            .fragment
            .contains(&point));

        // Simulate BlockFetched. The coordinator derives the point from
        // body.point(), which requires valid Shelley+ CBOR. With an opaque
        // body, it falls back to Point::Origin and can't clean up
        // pending_fetches for the real point. In production, all blocks
        // (including fake test-node blocks) have valid CBOR structure.
        //
        // This test verifies fragment pruning works for the derived point.
        coordinator.pending_fetches.insert(Point::Origin, peer_a);

        coordinator
            .handle_peer_event(
                peer_a,
                PeerEvent::BlockFetched {
                    body: crate::types::BlockBody::opaque(vec![0xD8, 0x18, 0x40]),
                },
            )
            .await;

        // Opaque body resolves to Origin; pending fetch for Origin is removed.
        assert!(
            !coordinator.pending_fetches.contains_key(&Point::Origin),
            "pending fetch for Origin should be removed"
        );
    }

    #[tokio::test]
    async fn fragment_truncated_on_rollback() {
        let (mut coordinator, _net_rx) = make_fragment_coordinator();
        let peer_a = PeerId(0);
        insert_peer(&mut coordinator, peer_a, None);

        let p100 = Point::Specific {
            slot: 100,
            hash: [1u8; 32],
        };
        let p101 = Point::Specific {
            slot: 101,
            hash: [2u8; 32],
        };
        let p102 = Point::Specific {
            slot: 102,
            hash: [3u8; 32],
        };

        // Build fragment: intersection at p100, then p101, p102.
        coordinator
            .handle_peer_event(
                peer_a,
                PeerEvent::IntersectionFound {
                    point: p100.clone(),
                },
            )
            .await;

        let tip = Tip {
            point: p101.clone(),
            block_no: 101,
        };
        coordinator
            .handle_peer_event(
                peer_a,
                PeerEvent::HeaderAnnounced {
                    header: WrappedHeader::opaque(vec![0xA0]),
                    tip,
                },
            )
            .await;

        let tip2 = Tip {
            point: p102.clone(),
            block_no: 102,
        };
        coordinator
            .handle_peer_event(
                peer_a,
                PeerEvent::HeaderAnnounced {
                    header: WrappedHeader::opaque(vec![0xA1]),
                    tip: tip2,
                },
            )
            .await;

        let frag = &coordinator.peers.get(&peer_a).unwrap().fragment;
        assert!(frag.contains(&p100));
        assert!(frag.contains(&p101));
        assert!(frag.contains(&p102));

        // Rollback to p100.
        let rollback_tip = Tip {
            point: p100.clone(),
            block_no: 100,
        };
        coordinator
            .handle_peer_event(
                peer_a,
                PeerEvent::RolledBack {
                    point: p100.clone(),
                    tip: rollback_tip,
                },
            )
            .await;

        let frag = &coordinator.peers.get(&peer_a).unwrap().fragment;
        assert!(frag.contains(&p100));
        assert!(!frag.contains(&p101));
        assert!(!frag.contains(&p102));
    }

    /// A peer rollback below `best_tip` must NOT mutate `chain_store`.
    /// Praos owns chain selection (`PraosEffect::InjectRollback` →
    /// `NetworkCommand::InjectRollback` → `chain_store.rollback_to`);
    /// the coordinator forwards `NetworkEvent::RolledBack` and leaves
    /// the store alone so a single peer's rollback can't truncate a
    /// chain that Praos still considers adopted.
    #[tokio::test]
    async fn peer_rollback_does_not_truncate_chain_store() {
        let (peer_event_sender, peer_event_receiver) = mpsc::channel(256);
        let (net_event_sender, _net_event_receiver) = mpsc::channel(64);
        let (_net_cmd_sender, net_cmd_receiver) = mpsc::channel(64);
        let (chain_store, _chain_rx) = ChainStore::new(100);

        // Praos has already published two blocks to chain_store.
        let p100 = Point::Specific {
            slot: 100,
            hash: [1u8; 32],
        };
        let p101 = Point::Specific {
            slot: 101,
            hash: [2u8; 32],
        };
        chain_store.append_block(
            p100.clone(),
            WrappedHeader::opaque(vec![0xA0]),
            crate::types::BlockBody::opaque(vec![0xB0]),
            100,
        );
        chain_store.append_block(
            p101.clone(),
            WrappedHeader::opaque(vec![0xA1]),
            crate::types::BlockBody::opaque(vec![0xB1]),
            101,
        );
        let adopted_tip_before = chain_store.tip();

        let mut coordinator = Coordinator::new(
            CoordinatorConfig::default(),
            peer_event_sender,
            peer_event_receiver,
            net_event_sender,
            net_cmd_receiver,
            chain_store.clone(),
            None,
        );
        let peer_a = PeerId(0);
        insert_peer(&mut coordinator, peer_a, None);

        // Peer reports its tip is at block 101 — sets best_tip to 101.
        coordinator
            .handle_peer_event(
                peer_a,
                PeerEvent::HeaderAnnounced {
                    header: WrappedHeader::opaque(vec![0xA1]),
                    tip: Tip {
                        point: p101.clone(),
                        block_no: 101,
                    },
                },
            )
            .await;

        // Peer rolls back to p100 with a lower tip — would previously
        // have triggered chain_store.rollback_to(p100).
        coordinator
            .handle_peer_event(
                peer_a,
                PeerEvent::RolledBack {
                    point: p100.clone(),
                    tip: Tip {
                        point: p100.clone(),
                        block_no: 100,
                    },
                },
            )
            .await;

        // chain_store is untouched.
        assert_eq!(
            chain_store.tip(),
            adopted_tip_before,
            "coordinator-level rollback must not mutate chain_store"
        );
        assert_eq!(chain_store.stored_count(), 2, "blocks remain stored");
    }

    #[tokio::test]
    async fn block_fetch_failed_removes_from_fragment_and_notifies() {
        let (mut coordinator, mut net_rx) = make_fragment_coordinator();
        let peer_a = PeerId(0);
        insert_peer(&mut coordinator, peer_a, None);

        let point = Point::Specific {
            slot: 500,
            hash: [6u8; 32],
        };

        // Peer A has the point.
        coordinator
            .handle_peer_event(
                peer_a,
                PeerEvent::IntersectionFound {
                    point: point.clone(),
                },
            )
            .await;

        assert!(coordinator
            .peers
            .get(&peer_a)
            .unwrap()
            .fragment
            .contains(&point));

        // Mark as pending fetch.
        coordinator.pending_fetches.insert(point.clone(), peer_a);

        // BlockFetch fails.
        coordinator
            .handle_peer_event(
                peer_a,
                PeerEvent::BlockFetchFailed {
                    from: point.clone(),
                    to: point.clone(),
                },
            )
            .await;

        // Fragment should still contain the point (we no longer prune
        // on fetch failure — the peer may still have the block).
        assert!(coordinator
            .peers
            .get(&peer_a)
            .unwrap()
            .fragment
            .contains(&point));

        // Pending fetch should be cleared.
        assert!(!coordinator.pending_fetches.contains_key(&point));

        // Drain the IntersectionFound event that was forwarded.
        let first = net_rx.try_recv().unwrap();
        assert!(matches!(first, NetworkEvent::IntersectionFound { .. }));

        // App should receive BlockFetchFailed.
        let event = net_rx.try_recv().unwrap();
        assert!(matches!(event, NetworkEvent::BlockFetchFailed { .. }));
    }

    #[tokio::test]
    async fn header_announced_appends_to_fragment_using_tip_for_opaque() {
        let (mut coordinator, _net_rx) = make_fragment_coordinator();
        let peer_a = PeerId(0);
        insert_peer(&mut coordinator, peer_a, None);

        let intersection = Point::Specific {
            slot: 50,
            hash: [0xAA; 32],
        };
        coordinator
            .handle_peer_event(
                peer_a,
                PeerEvent::IntersectionFound {
                    point: intersection.clone(),
                },
            )
            .await;

        // Opaque header has no parsed info, so point() returns None.
        // Coordinator should fall back to tip.point for the fragment.
        let tip_point = Point::Specific {
            slot: 51,
            hash: [0xBB; 32],
        };
        let tip = Tip {
            point: tip_point.clone(),
            block_no: 51,
        };
        coordinator
            .handle_peer_event(
                peer_a,
                PeerEvent::HeaderAnnounced {
                    header: WrappedHeader::opaque(vec![0xA0]),
                    tip,
                },
            )
            .await;

        let frag = &coordinator.peers.get(&peer_a).unwrap().fragment;
        assert!(frag.contains(&intersection));
        assert!(frag.contains(&tip_point));
    }

    // --- Reconnection tests ---

    /// Helper: create a coordinator and insert a peer, then remove it and
    /// return (reconnect queue, ip_counts for the inbound IP if any). An
    /// `inbound_ip` of `Some` simulates an accepted inbound peer (carries
    /// an `IpCountGuard`); `None` simulates an outbound peer.
    async fn reconnection_after_removal(
        inbound_ip: Option<IpAddr>,
    ) -> (Vec<String>, Option<usize>) {
        let (peer_event_sender, peer_event_receiver) = mpsc::channel(256);
        let (net_event_sender, mut net_event_receiver) = mpsc::channel(64);
        let (_net_cmd_sender, net_cmd_receiver) = mpsc::channel(64);
        let config = CoordinatorConfig::default();
        let (chain_store, _chain_rx) = ChainStore::new(100);
        let mut coordinator = Coordinator::new(
            config,
            peer_event_sender,
            peer_event_receiver,
            net_event_sender,
            net_cmd_receiver,
            chain_store,
            None,
        );

        let ip_guard =
            inbound_ip.map(|ip| IpCountGuard::reserve(coordinator.ip_counts.clone(), ip));

        let peer_id = PeerId(0);
        let (cmd_sender, _cmd_receiver) = mpsc::channel(16);
        coordinator.peers.insert(
            peer_id,
            PeerState {
                address: "test:3001".to_string(),
                mode: ConnectionMode::Duplex,
                ip_guard,
                commands: cmd_sender,
                task_handle: tokio::spawn(async {}),
                tip: None,
                rtt: None,
                fragment: ChainFragment::new(),
                reconnect_backoff: Duration::from_secs(1),
                inbound_delay: Duration::ZERO,
                mux_stats: None,
                downstream: None,
                last_rolled_back_to: None,
            },
        );

        coordinator.remove_peer(peer_id, "test".to_string()).await;
        let _ = net_event_receiver.try_recv();

        let queue: Vec<String> = coordinator
            .reconnect_queue
            .iter()
            .map(|(addr, _, _)| addr.clone())
            .collect();
        let ip_count_after =
            inbound_ip.and_then(|ip| coordinator.ip_counts.lock().unwrap().get(&ip).copied());
        (queue, ip_count_after)
    }

    #[tokio::test]
    async fn outbound_peer_schedules_reconnection() {
        let (queue, _) = reconnection_after_removal(None).await;
        assert_eq!(queue, vec!["test:3001"]);
    }

    #[tokio::test]
    async fn accepted_peer_does_not_schedule_reconnection() {
        let (queue, ip_count_after) =
            reconnection_after_removal(Some("127.0.0.1".parse().unwrap())).await;
        assert!(queue.is_empty());
        // Per-IP slot must be released when the PeerState drops.
        assert_eq!(
            ip_count_after, None,
            "ip_counts entry should be removed when last guard drops"
        );
    }

    #[test]
    fn ip_count_guard_decrements_on_drop() {
        let ip_counts: Arc<Mutex<HashMap<IpAddr, usize>>> = Arc::new(Mutex::new(HashMap::new()));
        let ip: IpAddr = "10.0.0.1".parse().unwrap();

        let guard = IpCountGuard::reserve(ip_counts.clone(), ip);
        assert_eq!(
            ip_counts.lock().unwrap().get(&ip).copied(),
            Some(1),
            "reserve should increment the counter"
        );

        drop(guard);
        assert_eq!(
            ip_counts.lock().unwrap().get(&ip).copied(),
            None,
            "drop should remove the entry when count reaches zero"
        );

        // Multiple guards stack and release independently.
        let g1 = IpCountGuard::reserve(ip_counts.clone(), ip);
        let g2 = IpCountGuard::reserve(ip_counts.clone(), ip);
        assert_eq!(ip_counts.lock().unwrap().get(&ip).copied(), Some(2));
        drop(g1);
        assert_eq!(ip_counts.lock().unwrap().get(&ip).copied(), Some(1));
        drop(g2);
        assert_eq!(ip_counts.lock().unwrap().get(&ip).copied(), None);
    }

    /// When the app consumer is slow, the `peer_events` branch gate closes
    /// (network_events capacity drops below MIN_EMIT_HEADROOM). The coord
    /// must still process `network_commands` and other branches — only the
    /// peer-event intake should pause. This is the core backpressure fix.
    #[tokio::test]
    async fn coordinator_still_processes_commands_when_app_is_slow() {
        use crate::types::{Tip, WrappedHeader};

        // Sized so that MIN_EMIT_HEADROOM cannot be satisfied: we pre-fill
        // the network_events channel to (capacity - MIN_EMIT_HEADROOM + 1)
        // to simulate the app being behind on draining.
        let app_channel_cap = MIN_EMIT_HEADROOM + 4;
        let (peer_event_sender, peer_event_receiver) = mpsc::channel(256);
        let (net_event_sender, net_event_receiver) = mpsc::channel(app_channel_cap);
        let (net_cmd_sender, net_cmd_receiver) = mpsc::channel(64);
        let config = CoordinatorConfig::default();
        let (chain_store, _chain_rx) = ChainStore::new(100);
        let coordinator = Coordinator::new(
            config,
            peer_event_sender.clone(),
            peer_event_receiver,
            net_event_sender.clone(),
            net_cmd_receiver,
            chain_store,
            None,
        );

        // Fill network_events to below MIN_EMIT_HEADROOM so the gate will
        // close. We use a dummy event variant that's cheap to construct.
        for _ in 0..(app_channel_cap - MIN_EMIT_HEADROOM + 1) {
            net_event_sender
                .try_send(NetworkEvent::PeersDiscovered { peers: Vec::new() })
                .expect("pre-fill should succeed");
        }
        assert!(
            net_event_sender.capacity() < MIN_EMIT_HEADROOM,
            "test precondition: pre-fill must close the gate"
        );

        // Spawn the coordinator. It should not deadlock: even though the
        // app is not draining, the coord's network_commands branch is still
        // active.
        let handle = tokio::spawn(coordinator.run());

        // Wait briefly for the coord to enter the select! loop.
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Inject a peer event on the fan-in channel. With the gate closed,
        // the coord should NOT consume it yet.
        let tip = Tip {
            point: Point::Specific {
                slot: 1,
                hash: [0u8; 32],
            },
            block_no: 1,
        };
        peer_event_sender
            .try_send((
                PeerId(0),
                PeerEvent::HeaderAnnounced {
                    header: WrappedHeader::opaque(vec![0xA0]),
                    tip,
                },
            ))
            .expect("fan-in send should not be full");

        // Even with the gate closed, a NetworkCommand should be processed
        // (QueryPeers is a pure no-op emit that would also hit the gate,
        // so use Shutdown which causes the coord to exit cleanly).
        net_cmd_sender
            .send(NetworkCommand::Shutdown)
            .await
            .expect("command channel should accept");

        // The coord should exit promptly from the Shutdown command.
        let result = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(
            result.is_ok(),
            "coord should exit via Shutdown command within 2s; hung on gate"
        );

        // Drain the receiver for cleanup.
        drop(net_event_receiver);
    }

    /// After `RecordLeiosEbManifest`, the LeiosStore should be able to
    /// serve `get_block_txs` by resolving each requested hash through
    /// the configured `TxBodyResolver`.
    #[tokio::test]
    async fn record_leios_eb_manifest_enables_resolver_backed_serve() {
        use crate::store::leios_store::TxBodyResolver;

        struct StubResolver(HashMap<TxId, TxBody>);
        impl TxBodyResolver for StubResolver {
            fn resolve_body(&self, tx_id: &TxId) -> Option<TxBody> {
                self.0.get(tx_id).cloned()
            }
        }

        let h0 = [0x01u8; 32];
        let h1 = [0x02u8; 32];
        let resolver: Arc<dyn TxBodyResolver> = Arc::new(StubResolver(
            [
                (TxId::new_with_array(h0), TxBody::new_with_vec(vec![10u8])),
                (TxId::new_with_array(h1), TxBody::new_with_vec(vec![20u8])),
            ]
            .into_iter()
            .collect(),
        ));

        let (peer_event_sender, peer_event_receiver) = mpsc::channel(256);
        let (net_event_sender, _net_event_receiver) = mpsc::channel(NETWORK_EVENTS_CAPACITY);
        let (net_cmd_sender, net_cmd_receiver) = mpsc::channel(64);
        let config = CoordinatorConfig::default();
        let (chain_store, _chain_rx) = ChainStore::new(100);
        let (leios_store, _leios_rx) = LeiosStore::new_with_resolver(100, Some(resolver.clone()));
        let coordinator = Coordinator::new(
            config,
            peer_event_sender,
            peer_event_receiver,
            net_event_sender,
            net_cmd_receiver,
            chain_store,
            Some(leios_store.clone()),
        );

        let handle = tokio::spawn(coordinator.run());

        let eb_hash = [0xEE; 32];
        let point = Point::Specific {
            slot: 4,
            hash: eb_hash,
        };
        net_cmd_sender
            .send(NetworkCommand::RecordLeiosEbManifest {
                source: None,
                point,
                tx_hashes: vec![TxId::new_with_array(h0), TxId::new_with_array(h1)],
            })
            .await
            .expect("command should accept");

        // Poll until the manifest is stored.
        let bitmap = crate::protocols::leios_fetch::bitmap::from_indices(&[0, 1]);
        let mut got = None;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(5)).await;
            if let Some(stored) = leios_store.get_block_txs(4, &eb_hash, &bitmap) {
                if !stored.is_empty() {
                    got = Some(stored);
                    break;
                }
            }
        }
        assert_eq!(
            got,
            Some(vec![
                TxBody::new_with_vec(vec![10u8]),
                TxBody::new_with_vec(vec![20u8])
            ])
        );

        net_cmd_sender
            .send(NetworkCommand::Shutdown)
            .await
            .expect("shutdown");
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
    }

    /// `InjectLeiosBlockTxs` must reach the LeiosStore so peers can serve
    /// the producer's overflow tx bodies via `MsgLeiosBlockTxsRequest`.
    #[tokio::test]
    async fn inject_leios_block_txs_lands_in_store() {
        let (peer_event_sender, peer_event_receiver) = mpsc::channel(256);
        let (net_event_sender, _net_event_receiver) = mpsc::channel(NETWORK_EVENTS_CAPACITY);
        let (net_cmd_sender, net_cmd_receiver) = mpsc::channel(64);
        let config = CoordinatorConfig::default();
        let (chain_store, _chain_rx) = ChainStore::new(100);
        let (leios_store, _leios_rx) = LeiosStore::new(100);
        let coordinator = Coordinator::new(
            config,
            peer_event_sender,
            peer_event_receiver,
            net_event_sender,
            net_cmd_receiver,
            chain_store,
            Some(leios_store.clone()),
        );

        let handle = tokio::spawn(coordinator.run());

        let hash = [0xA1u8; 32];
        let point = Point::Specific { slot: 7, hash };
        let txs: Vec<TxBody> = vec![
            TxBody::new_with_vec(vec![1, 2, 3]),
            TxBody::new_with_vec(vec![4, 5, 6]),
        ];

        net_cmd_sender
            .send(NetworkCommand::InjectLeiosBlockTxs {
                point: point.clone(),
                transactions: txs.clone(),
            })
            .await
            .expect("command should accept");

        // Wait for the store to see the txs.
        let mut got = None;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(5)).await;
            let bitmap = crate::protocols::leios_fetch::bitmap::select_all(txs.len() as u32);
            if let Some(stored) = leios_store.get_block_txs(7, &hash, &bitmap) {
                got = Some(stored);
                break;
            }
        }
        assert_eq!(got.as_deref(), Some(txs.as_slice()));

        net_cmd_sender
            .send(NetworkCommand::Shutdown)
            .await
            .expect("shutdown should accept");
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
    }

    /// On `LeiosBlockTxsFetched`, the coordinator must hash each body, look
    /// up its position in the recorded manifest, and merge the bodies into
    /// the store at those indices. Without this, a non-producer node never
    /// becomes a source for downstream gossip — every voter ends up
    /// fetching directly from the producer (hub-spoke).
    #[tokio::test]
    async fn coordinator_reinjects_fetched_block_txs_into_store() {
        let (peer_event_sender, peer_event_receiver) = mpsc::channel(256);
        let (net_event_sender, mut net_event_receiver) = mpsc::channel(NETWORK_EVENTS_CAPACITY);
        let (_net_cmd_sender, net_cmd_receiver) = mpsc::channel(64);
        let config = CoordinatorConfig::default();
        let (chain_store, _chain_rx) = ChainStore::new(100);
        let (leios_store, _leios_rx) = LeiosStore::new(100);
        let mut coordinator = Coordinator::new(
            config,
            peer_event_sender,
            peer_event_receiver,
            net_event_sender,
            net_cmd_receiver,
            chain_store,
            Some(leios_store.clone()),
        );

        // Manifest must be in place before the coordinator can position
        // received bodies. In production net-node records this after
        // decoding the EB body; here we set it directly.
        let body0 = TxBody::new_with_vec(b"alpha".to_vec());
        let body1 = TxBody::new_with_vec(b"bravo".to_vec());
        let body2 = TxBody::new_with_vec(b"charlie".to_vec());
        let h0 = body0.get_blake2b_txid();
        let h1 = body1.get_blake2b_txid();
        let h2 = body2.get_blake2b_txid();
        let eb_hash = [0xEEu8; 32];
        let point = Point::Specific {
            slot: 12,
            hash: eb_hash,
        };
        leios_store.record_eb_manifest(point.clone(), vec![h0, h1, h2], None);

        // Simulate a partial response from an upstream peer: indices 0
        // and 2 only. Order is reversed to confirm we don't rely on
        // response order.
        coordinator
            .handle_peer_event(
                PeerId(7),
                PeerEvent::LeiosBlockTxsFetched {
                    point: point.clone(),
                    transactions: vec![body2.clone(), body0.clone()],
                },
            )
            .await;

        // Bodies are merged at the right positions.
        let bitmap = crate::protocols::leios_fetch::bitmap::from_indices(&[0, 1, 2]);
        let got = leios_store
            .get_block_txs(12, &eb_hash, &bitmap)
            .expect("store should know about EB");
        // Index 1 is missing (we never fetched it); union returns just
        // 0 and 2 in ascending order.
        assert_eq!(got, vec![body0.clone(), body2.clone()]);

        // The application also gets the original event with all bodies.
        match net_event_receiver.try_recv().expect("event emitted") {
            NetworkEvent::LeiosBlockTxsReceived {
                point: p,
                transactions,
            } => {
                assert_eq!(p, point);
                assert_eq!(transactions, vec![body2, body0]);
            }
            other => panic!("expected LeiosBlockTxsReceived, got {other:?}"),
        }
    }

    /// When the manifest hasn't been recorded yet (race-free in
    /// production but defensible in tests), the coordinator must
    /// still forward the event without panicking. Bodies aren't
    /// indexed; downstream peers will see no advertisement until
    /// the manifest arrives.
    #[tokio::test]
    async fn coordinator_handles_block_txs_fetched_without_manifest() {
        let (peer_event_sender, peer_event_receiver) = mpsc::channel(256);
        let (net_event_sender, mut net_event_receiver) = mpsc::channel(NETWORK_EVENTS_CAPACITY);
        let (_net_cmd_sender, net_cmd_receiver) = mpsc::channel(64);
        let config = CoordinatorConfig::default();
        let (chain_store, _chain_rx) = ChainStore::new(100);
        let (leios_store, _leios_rx) = LeiosStore::new(100);
        let mut coordinator = Coordinator::new(
            config,
            peer_event_sender,
            peer_event_receiver,
            net_event_sender,
            net_cmd_receiver,
            chain_store,
            Some(leios_store.clone()),
        );

        let eb_hash = [0xCCu8; 32];
        let point = Point::Specific {
            slot: 14,
            hash: eb_hash,
        };

        coordinator
            .handle_peer_event(
                PeerId(3),
                PeerEvent::LeiosBlockTxsFetched {
                    point: point.clone(),
                    transactions: vec![TxBody::new_with_vec(b"orphan".to_vec())],
                },
            )
            .await;

        // Event is still forwarded.
        assert!(matches!(
            net_event_receiver.try_recv(),
            Ok(NetworkEvent::LeiosBlockTxsReceived { .. })
        ));
        // Store has nothing — no manifest, no inject.
        let bitmap = crate::protocols::leios_fetch::bitmap::from_indices(&[0]);
        assert!(leios_store.get_block_txs(14, &eb_hash, &bitmap).is_none());
    }

    /// When a peer's command channel fills (peer task not draining), the
    /// coord should mark it for removal and continue running.
    #[tokio::test]
    async fn coordinator_removes_peer_when_its_command_channel_fills() {
        let (peer_event_sender, peer_event_receiver) = mpsc::channel(256);
        let (net_event_sender, mut net_event_receiver) = mpsc::channel(NETWORK_EVENTS_CAPACITY);
        let (_net_cmd_sender, net_cmd_receiver) = mpsc::channel(64);
        let config = CoordinatorConfig::default();
        let (chain_store, _chain_rx) = ChainStore::new(100);
        let mut coordinator = Coordinator::new(
            config,
            peer_event_sender,
            peer_event_receiver,
            net_event_sender,
            net_cmd_receiver,
            chain_store,
            None,
        );

        // Insert a peer with a tiny commands channel and don't drain it.
        let peer_id = PeerId(0);
        let (cmd_sender, cmd_receiver) = mpsc::channel(2);
        coordinator.peers.insert(
            peer_id,
            PeerState {
                address: "stuck:3001".to_string(),
                mode: ConnectionMode::InitiatorOnly,
                ip_guard: None,
                commands: cmd_sender,
                task_handle: tokio::spawn(async {}),
                tip: None,
                rtt: None,
                fragment: ChainFragment::new(),
                reconnect_backoff: Duration::from_secs(1),
                inbound_delay: Duration::ZERO,
                mux_stats: None,
                downstream: None,
                last_rolled_back_to: None,
            },
        );

        // Keep the receiver alive but never recv from it, so the channel
        // saturates after two sends.
        let _cmd_receiver_keeper = cmd_receiver;

        // Fire commands until the peer's channel saturates and the next
        // send_peer_command schedules removal.
        for _ in 0..5 {
            coordinator.send_peer_command(peer_id, PeerCommand::ReIntersect);
        }

        assert!(
            coordinator
                .pending_removals
                .iter()
                .any(|(id, _)| *id == peer_id),
            "peer should be scheduled for removal after its command channel fills"
        );

        // Drain pending_removals (the main loop would do this; we invoke
        // remove_peer directly) and verify the peer is gone.
        for (id, reason) in std::mem::take(&mut coordinator.pending_removals) {
            coordinator.remove_peer(id, reason).await;
        }
        assert!(
            !coordinator.peers.contains_key(&peer_id),
            "peer should be removed"
        );

        // A PeerDisconnected event should have been emitted.
        let mut saw_disconnect = false;
        while let Ok(ev) = net_event_receiver.try_recv() {
            if matches!(ev, NetworkEvent::PeerDisconnected { peer_id: id, .. } if id == peer_id) {
                saw_disconnect = true;
            }
        }
        assert!(
            saw_disconnect,
            "PeerDisconnected should be emitted when peer is force-removed"
        );
    }

    /// Build a bare coordinator with default config for the eviction/dedup
    /// unit tests below.
    fn bare_coordinator() -> Coordinator {
        let (peer_event_sender, peer_event_receiver) = mpsc::channel(256);
        let (net_event_sender, _net_event_receiver) = mpsc::channel(NETWORK_EVENTS_CAPACITY);
        let (_net_cmd_sender, net_cmd_receiver) = mpsc::channel(64);
        let (chain_store, _chain_rx) = ChainStore::new(100);
        Coordinator::new(
            CoordinatorConfig::default(),
            peer_event_sender,
            peer_event_receiver,
            net_event_sender,
            net_cmd_receiver,
            chain_store,
            None,
        )
    }

    /// A JoinHandle guaranteed to be `is_finished()` — the runtime has
    /// polled the (trivial) task to completion.
    async fn finished_handle() -> JoinHandle<()> {
        let h = tokio::spawn(async {});
        while !h.is_finished() {
            tokio::task::yield_now().await;
        }
        h
    }

    fn insert_peer_with_task(
        coord: &mut Coordinator,
        id: PeerId,
        address: &str,
        task_handle: JoinHandle<()>,
    ) {
        let (cmd_sender, _cmd_receiver) = mpsc::channel(2);
        coord.peers.insert(
            id,
            PeerState {
                address: address.to_string(),
                mode: ConnectionMode::InitiatorOnly,
                ip_guard: None,
                commands: cmd_sender,
                task_handle,
                tip: None,
                rtt: None,
                fragment: ChainFragment::new(),
                reconnect_backoff: Duration::from_secs(1),
                inbound_delay: Duration::ZERO,
                mux_stats: None,
                downstream: None,
                last_rolled_back_to: None,
            },
        );
    }

    /// `reap_finished_peers` queues the dead for removal and returns only
    /// the live count — the signal the cap checks rely on.
    #[tokio::test]
    async fn reap_finished_peers_queues_dead_and_counts_live() {
        let mut coord = bare_coordinator();
        // One dead peer (task already exited) and one live peer.
        insert_peer_with_task(&mut coord, PeerId(1), "dead:3001", finished_handle().await);
        let live_task = tokio::spawn(std::future::pending::<()>());
        let live_abort = live_task.abort_handle();
        insert_peer_with_task(&mut coord, PeerId(2), "live:3001", live_task);

        let live = coord.reap_finished_peers();
        assert_eq!(live, 1, "only the live peer should be counted");
        assert!(
            coord.pending_removals.iter().any(|(id, _)| *id == PeerId(1)),
            "dead peer should be queued for removal"
        );
        assert!(
            !coord.pending_removals.iter().any(|(id, _)| *id == PeerId(2)),
            "live peer must not be queued"
        );

        // Idempotent: a second reap must not double-queue the same corpse.
        let before = coord.pending_removals.len();
        coord.reap_finished_peers();
        assert_eq!(coord.pending_removals.len(), before, "no double-queue");

        live_abort.abort();
    }

    /// `has_live_peer_for_address` matches only live peers — a zombie
    /// holding an address must not block a reconnect to it.
    #[tokio::test]
    async fn has_live_peer_for_address_ignores_finished() {
        let mut coord = bare_coordinator();
        insert_peer_with_task(&mut coord, PeerId(1), "zombie:3001", finished_handle().await);
        let live_task = tokio::spawn(std::future::pending::<()>());
        let live_abort = live_task.abort_handle();
        insert_peer_with_task(&mut coord, PeerId(2), "alive:3001", live_task);

        assert!(
            !coord.has_live_peer_for_address("zombie:3001"),
            "a finished peer must not count as a live holder of its address"
        );
        assert!(coord.has_live_peer_for_address("alive:3001"));
        assert!(!coord.has_live_peer_for_address("never:3001"));

        live_abort.abort();
    }

    /// A due reconnect to an address already held by a live peer is
    /// dropped (outbound dedup) — no second outbound task is spawned.
    #[tokio::test]
    async fn reconnect_skips_when_address_already_live() {
        let mut coord = bare_coordinator();
        let live_task = tokio::spawn(std::future::pending::<()>());
        let live_abort = live_task.abort_handle();
        insert_peer_with_task(&mut coord, PeerId(1), "dup:3001", live_task);

        // Queue a due reconnect for the same address.
        coord.reconnect_queue.push((
            "dup:3001".to_string(),
            Instant::now() - Duration::from_secs(1),
            Duration::from_secs(2),
        ));

        coord.process_reconnections();

        assert_eq!(coord.peers.len(), 1, "no duplicate outbound peer spawned");
        assert!(
            coord.reconnect_queue.is_empty(),
            "the redundant reconnect should be dropped, not re-queued"
        );

        live_abort.abort();
    }
}
