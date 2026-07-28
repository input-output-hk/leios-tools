//! Event and command types for the coordinator ↔ application boundary.

use std::collections::BTreeMap;

use crate::peer::{ConnectionMode, PeerId};
use crate::protocols::peersharing::PeerAddress;
use crate::protocols::txsubmission::PendingTx;
use crate::types::{BlockBody, Point, Tip, Vote, WrappedHeader};
use shared_consensus::mempool::{TxBody, TxId};
use std::time::Duration;

// ---------------------------------------------------------------------------
// Coordinator ↔ Application
// ---------------------------------------------------------------------------

/// Events sent from the coordinator to the application.
///
/// Most events carry `peer_id` so chain-selecting consumers (net-node
/// consensus) can track per-peer candidate chains. Consumers that don't
/// care about peer identity (net-cli `multi-follow`) can destructure it
/// with `peer_id: _`.
#[derive(Debug)]
pub enum NetworkEvent {
    /// A peer connected successfully.
    PeerConnected { peer_id: PeerId, address: String },

    /// A peer disconnected (error or graceful).
    PeerDisconnected { peer_id: PeerId, reason: String },

    /// A peer announced a new chain tip. Emitted per-peer, not deduplicated
    /// across peers — consensus needs to track each peer's candidate chain
    /// independently.
    TipAdvanced {
        peer_id: PeerId,
        tip: Tip,
        header: WrappedHeader,
    },

    /// ChainSync found an intersection with a peer — the common ancestor
    /// between the local chain and the peer's chain. Consensus stores this
    /// as the peer chain's anchor (guaranteed common ancestor).
    /// If the `initial` flag is set, consensus treats this point as the ultimate lower bound for
    /// block backtracking; nothing earlier is available.
    IntersectionFound { peer_id: PeerId, point: Point, initial: bool },

    /// A peer rolled its chain back to a point. Emitted for every peer
    /// rollback, not just those affecting the local best tip.
    RolledBack {
        peer_id: PeerId,
        point: Point,
        tip: Tip,
    },

    /// A requested block was fetched.
    BlockReceived { point: Point, body: BlockBody },

    /// A requested block fetch failed (peer responded with NoBlocks,
    /// the connection died, or no peer had the fragment).  Carries the
    /// responsible peer when one was actually attempted, so the
    /// application can put it on cooldown and re-route via the fetch
    /// policy.  `None` means no peer was reachable for the requested
    /// fragment — there's no one to cooldown.
    BlockFetchFailed {
        peer_id: Option<PeerId>,
        from: Point,
        to: Point,
    },

    /// New peers discovered via PeerSharing. `from` is the peer that
    /// answered the share request — recursion drivers use it to walk the
    /// graph outward from a specific answering peer.
    PeersDiscovered {
        from: PeerId,
        peers: Vec<PeerAddress>,
    },

    /// A transaction was received from an inbound peer (via TxSubmission server).
    /// `era` is the tx's HardFork era, carried so the app can re-announce it
    /// with its original era rather than a fixed constant.
    TransactionReceived {
        peer_id: PeerId,
        body: TxBody,
        era: u16,
    },

    /// TxSubmission client: a peer requested `count` tx ids (blocking mode).
    TxsRequested { peer_id: PeerId, count: u16 },

    /// Leios: an EB was announced via an RB header.
    LeiosBlockAnnounced { header: WrappedHeader },

    /// Leios: an endorser block is available for download from a peer.
    LeiosBlockOffered { peer_id: PeerId, point: Point },

    /// Leios: an EB's transactions are available for download from a peer.
    LeiosBlockTxsOffered { peer_id: PeerId, point: Point },

    /// Leios: a fetched endorser block arrived. `source` is the peer
    /// it was fetched from (`None` for self-produced EBs, which the
    /// node feeds through the same receive path so manifest-recording
    /// fires identically).
    LeiosBlockReceived {
        source: Option<PeerId>,
        point: Point,
        block: Vec<u8>,
    },

    /// Leios: votes delivered inline by a peer (no fetch round-trip).
    LeiosVotesReceived { peer_id: PeerId, votes: Vec<Vote> },

    /// Leios: fetched transactions for an EB arrived.
    LeiosBlockTxsReceived {
        peer_id: PeerId,
        point: Point,
        transactions: Vec<TxBody>,
    },

    /// Response to `QueryPeers`: snapshot of all connected peers.
    PeerSnapshot { peers: Vec<PeerInfo> },
}

/// Snapshot of a single peer's state, for telemetry reporting.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub peer_id: PeerId,
    pub address: String,
    pub mode: ConnectionMode,
    pub rtt: Option<Duration>,
    pub tip_block_no: Option<u64>,
    pub inbound_delay: Duration,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    /// How far this (downstream) peer has promoted us as its upstream:
    /// cold (connected) / warm (keepalive) / hot (pulling our chain).
    pub downstream_state: crate::peer::DownstreamState,
}

/// Commands sent from the application to the coordinator.
///
/// `#[non_exhaustive]`: new command variants are added as the protocol
/// grows (e.g. `SetPeerBlocklist`), so downstream matches must carry a
/// wildcard arm rather than enumerate every variant — keeps adding a
/// variant a non-breaking change.
#[derive(Debug)]
#[non_exhaustive]
pub enum NetworkCommand {
    /// Add a peer by address. The coordinator will connect and manage it.
    /// Configured/seed peers use this: they reconnect indefinitely even if a
    /// dial has never yet succeeded (a relay down at startup must be retried).
    AddPeer { address: String },

    /// Add a *speculative* (discovery-sourced) peer by address. Identical to
    /// `AddPeer` in the protocols it runs, but with a different reconnection
    /// policy: until the address has connected at least once, the coordinator
    /// will NOT keep reconnecting it (a first-dial failure frees the slot
    /// instead of clogging the reconnect queue with never-connectable NAT
    /// addresses). A successful connect *promotes* it — thereafter it
    /// reconnects like any `AddPeer`. Re-trying a never-connected speculative
    /// peer is the discovery layer's job (bounded background re-dial), not the
    /// coordinator's.
    AddDiscoveredPeer { address: String },

    /// Fetch a specific block. The coordinator picks the best peer.
    FetchBlock { point: Point },

    /// Fetch a range of blocks (from..=to inclusive). When `peer_id` is
    /// set, the coordinator routes directly to that peer (the one that
    /// announced the chain). Otherwise falls back to fragment-based
    /// peer selection.
    FetchBlockRange {
        from: Point,
        to: Point,
        peer_id: Option<PeerId>,
    },

    /// Request peers from connected nodes (triggers PeerSharing). Targets
    /// a single arbitrary connected peer.
    DiscoverPeers,

    /// Request peers from a specific connected peer (targeted PeerSharing).
    /// Used by the discovery driver to recurse outward from a known peer,
    /// rather than the arbitrary peer `DiscoverPeers` picks. If `peer_id`
    /// is no longer connected the command is a no-op.
    DiscoverPeersFrom { peer_id: PeerId, amount: u8 },

    /// Ask a specific peer to re-run ChainSync intersection with fresh
    /// candidates from the current local chain. Used when a previous
    /// intersection became stale due to a local fork switch.
    ReIntersect { peer_id: PeerId },

    /// Inject a block into the chain store (for responder peers to serve).
    /// Used by block generators or other local producers.
    InjectBlock {
        point: Point,
        header: Box<WrappedHeader>,
        body: BlockBody,
        block_no: u64,
    },

    /// Roll back the chain store to a point (for responder peers).
    InjectRollback { point: Point },

    /// Fetch a Leios block from a specific peer (chosen by shared-consensus's
    /// EbFetchPolicy).  The coordinator routes directly to that peer.
    FetchLeiosBlock { peer_id: PeerId, point: Point },

    /// Fetch selective transactions from an EB on a specific peer
    /// (chosen by shared-consensus's EbTxsFetchPolicy).
    FetchLeiosBlockTxs {
        peer_id: PeerId,
        point: Point,
        bitmap: BTreeMap<u16, u64>,
    },

    /// Inject a Leios block into the Leios store (for responder peers to serve).
    InjectLeiosBlock { point: Point, block: Vec<u8> },

    /// Inject the transactions of a Leios block into the Leios store
    /// (for responder peers to serve via `MsgLeiosBlockTxsRequest`).
    InjectLeiosBlockTxs {
        point: Point,
        transactions: Vec<TxBody>,
    },

    /// Record the ordered tx-hash list of an EB whose body the receiver
    /// has already fetched and decoded. Lets the responder side serve
    /// `MsgLeiosBlockTxsRequest` by resolving each requested hash via the
    /// configured `TxBodyResolver` (typically the local mempool). `source`
    /// is the peer that supplied the EB body (`None` if self-produced);
    /// the resulting `BlockTxsOffer` is tagged with it so LeiosNotify
    /// skips re-offering tx availability back to that peer.
    RecordLeiosEbManifest {
        source: Option<PeerId>,
        point: Point,
        tx_hashes: Vec<TxId>,
    },

    /// Inject votes into the Leios store (for responder peers to re-serve
    /// inline via `MsgLeiosVotes`).
    InjectLeiosVotes { votes: Vec<Vote> },

    /// Provide transactions to a specific peer via TxSubmission.
    ProvideTxs {
        peer_id: PeerId,
        txs: Vec<PendingTx>,
    },

    /// Request a snapshot of all connected peers (for telemetry).
    QueryPeers,

    /// Drop all currently-accepted (inbound) peer connections.  The
    /// remote (outbound) side observes the disconnect and reconnects,
    /// re-running ChainSync intersection from scratch.  Used by the
    /// `DropInboundPeers` behaviour to mimic a relay that resets
    /// inbound connections (the reconnect-handover trigger).
    DropInboundPeers,

    /// Replace this node's outbound peer blocklist (full replace; an
    /// empty set heals).  While an address is blocklisted the
    /// coordinator refuses to dial it (`AddPeer` and peer discovery),
    /// parks any pending reconnection to it, and disconnects it if it is
    /// currently connected.  Because a duplex connection carries both
    /// directions over a single socket, dropping the dialing side cuts
    /// the link in both directions — this is the enforcement primitive
    /// behind cluster-driven network partitions.
    SetPeerBlocklist { addresses: Vec<String> },

    /// Shut down all peers and stop the coordinator.
    Shutdown,
}
