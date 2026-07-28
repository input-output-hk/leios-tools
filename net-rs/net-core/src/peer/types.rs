//! Event and command types for per-peer task communication.
//!
//! Channel boundary: Peer tasks ↔ Coordinator.
//! For the Coordinator ↔ Application boundary, see `multi_peer::types`.

use std::time::Duration;

use std::collections::BTreeMap;

use crate::mux::MuxStats;
use crate::protocols::peersharing::PeerAddress;
use crate::protocols::txsubmission::PendingTx;
use crate::types::{BlockBody, Point, Tip, Vote, WrappedHeader};
use shared_consensus::mempool::TxBody;
use std::sync::Arc;
// ---------------------------------------------------------------------------
// Peer ↔ Coordinator
// ---------------------------------------------------------------------------

/// Events sent from a per-peer task to the coordinator.
///
/// These abstract over raw protocol messages — the peer task translates
/// protocol-level details (e.g. `MsgRollForward`) into coordinator-relevant
/// events. Used by both initiator and responder peer tasks.
#[derive(Debug)]
pub enum PeerEvent {
    /// Connection established and handshake completed. `downstream` is the
    /// shared per-connection promotion flag the responder handlers escalate
    /// (cold→warm→hot); the coordinator reads it at snapshot time.
    Connected {
        mux_stats: Arc<MuxStats>,
        downstream: super::DownstreamFlag,
        /// The peer's advertised `peer_sharing` (1 = shares, 0 = declines).
        peer_sharing: u8,
    },

    /// ChainSync: intersection found during `find_intersection`.
    IntersectionFound { point: Point, initial: bool },

    /// ChainSync: peer announced a new header (from `MsgRollForward`).
    HeaderAnnounced { header: WrappedHeader, tip: Tip },

    /// ChainSync: peer rolled back (from `MsgRollBackward`).
    RolledBack { point: Point, tip: Tip },

    /// BlockFetch: a requested block arrived.
    BlockFetched { body: BlockBody },

    /// KeepAlive: measured round-trip time.
    LatencyMeasured { rtt: Duration },

    /// PeerSharing: received peer addresses.
    PeersDiscovered { peers: Vec<PeerAddress> },

    /// TxSubmission server: received a transaction from a client.
    /// `era` is the tx's HardFork era, carried so it can be re-announced
    /// on the wire with its original era rather than a fixed constant.
    TransactionReceived { body: TxBody, era: u16 },

    /// TxSubmission client: peer requested `count` tx ids (blocking mode).
    TxsRequested { count: u16 },

    /// LeiosNotify: server announced an RB header with EB announcement.
    LeiosBlockAnnounced { header: WrappedHeader },

    /// LeiosNotify: an endorser block is available for download.
    LeiosBlockOffered { point: Point },

    /// LeiosNotify: an EB's transactions are available.
    LeiosBlockTxsOffered { point: Point },

    /// LeiosNotify: votes delivered inline (no fetch round-trip).
    LeiosVotesReceived { votes: Vec<Vote> },

    /// LeiosFetch: a requested endorser block arrived.
    LeiosBlockFetched { point: Point, block: Vec<u8> },

    /// LeiosFetch: requested transactions for an EB arrived.
    LeiosBlockTxsFetched {
        point: Point,
        transactions: Vec<TxBody>,
    },

    /// BlockFetch: peer responded with NoBlocks for a requested range.
    BlockFetchFailed { from: Point, to: Point },

    /// Peer misbehaved or connection broke.
    Failed { reason: String },
}

/// Commands sent from the coordinator to a per-peer task.
#[derive(Debug)]
pub enum PeerCommand {
    /// Fetch a range of blocks via BlockFetch.
    FetchBlocks { from: Point, to: Point },

    /// Request peer addresses via PeerSharing.
    RequestPeers { amount: u8 },

    /// Fetch an endorser block via LeiosFetch.
    FetchLeiosBlock { point: Point },

    /// Fetch selective transactions from an EB via LeiosFetch (bitmap addressing).
    FetchLeiosBlockTxs {
        point: Point,
        bitmap: BTreeMap<u16, u64>,
    },

    /// Provide transactions to this peer via TxSubmission.
    ProvideTxs { txs: Vec<PendingTx> },

    /// Re-run ChainSync intersection with fresh candidates from the
    /// current local chain. Used when the previous intersection became
    /// stale due to a local fork switch.
    ReIntersect,

    /// Gracefully disconnect this peer.
    Disconnect,
}
