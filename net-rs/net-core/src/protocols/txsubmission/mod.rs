//! TxSubmission mini-protocol (protocol ID 4, version 2).
//!
//! Pull-based transaction dissemination between full nodes. The server
//! (transaction consumer) requests transaction IDs from the client
//! (transaction provider), then selectively requests full transactions.

pub mod codec;

use shared_consensus::mempool::{MempoolTx, TxBody, TxId};
use std::collections::VecDeque;
use std::time::Duration;
use shared_consensus::PeerId;
use tokio::sync::mpsc;

use crate::protocols::{Agency, Protocol, ProtocolError, Runner};

/// TxSubmission protocol ID in the multiplexer.
pub const PROTOCOL_ID: u16 = 4;

/// Ingress buffer limit for TxSubmission (per spec).
pub const INGRESS_LIMIT: usize = 721_424;

/// Max message size in StInit and StIdle.
pub const SIZE_LIMIT_SMALL: usize = 5_760;

/// Max message size in StTxIdsBlocking, StTxIdsNonBlocking, StTxs.
pub const SIZE_LIMIT_LARGE: usize = 2_500_000;

/// Timeout for StTxIdsNonBlocking (client must reply promptly).
pub const TIMEOUT_NON_BLOCKING: Duration = Duration::from_secs(10);

/// Timeout for StTxs (client must reply promptly).
pub const TIMEOUT_TXS: Duration = Duration::from_secs(10);

/// Maximum number of unacknowledged tx ids (flow control window).
pub const MAX_UNACKED: usize = 10;

/// Maximum size of a single encoded tx body.
pub const MAX_TX_SIZE: usize = 2_500_000;

/// HFC era index stamped on tx-ids / bodies we *originate* locally (i.e. txs
/// we generate ourselves, not ones we received and re-announce).
///
/// cardano-node wraps every `GenTxId` and tx body as `[era, ..]`, where `era`
/// is the HardFork-combinator era index: Byron=0, Shelley=1, Allegra=2, Mary=3,
/// Alonzo=4, Babbage=5, Conway=6, **Dijkstra=7**. Valid indices are 0..=7; a
/// value of 8 is past the end of the era list and makes cardano-node's
/// `decodeNS` reject the id/body ("invalid index 8") and drop the connection.
///
/// Received txs carry their peer's real era end-to-end via [`PendingTx::era`]
/// and are echoed back with that era (see [`EraTxBody`] / [`EraTxId`]); this
/// constant is only the fallback for txs we produce ourselves, which on the
/// current all-Dijkstra dev net is Dijkstra = 7.
pub const ORIGIN_ERA: u16 = 7;

/// Highest valid HardFork-combinator era index (Dijkstra = 7 today). Any era
/// index above this is past the end of cardano-node's era list, so a tx-id or
/// body stamped with it makes the peer's `decodeNS` fail ("invalid index N")
/// and drop the connection.
///
/// We re-announce received txs with their original era ([`PendingTx::era`]).
/// A misbehaving upstream can hand us an out-of-range era, which — passed
/// through verbatim — would get us dropped by an innocent downstream peer. So
/// we normalise on the way out ([`wire_era`]) rather than propagate the poison.
///
/// Bump this (and [`ORIGIN_ERA`]) at each hard fork as new eras are added.
pub const MAX_ERA: u16 = 7;

/// Normalise an era index for the wire: a received era within range is echoed
/// verbatim; an out-of-range era (> [`MAX_ERA`]) is a poisoned/garbage value we
/// must not re-announce, so it falls back to [`ORIGIN_ERA`]. Applied to every
/// era we put on the wire so the advertised id-era and the delivered body-era
/// stay consistent for a given tx.
fn wire_era(era: u16) -> u16 {
    if era > MAX_ERA {
        ORIGIN_ERA
    } else {
        era
    }
}

// --- Types ---
pub const TX_ID_SIZE: usize = 32;

/// A transaction ID as it travels on the NtN wire: the raw [`TxId`] plus
/// the HFC `era` index cardano-node prefixes it with (`[era, bytes]`).
///
/// The era is wire framing, not part of the id's identity, so it lives
/// here rather than on [`TxId`] (which stays a plain byte wrapper). It
/// must round-trip: a `MsgRequestTxs` we send back has to echo the exact
/// era the peer advertised in `MsgReplyTxIds`, or the peer won't match
/// the id against its mempool.
#[derive(Debug, Clone)]
pub struct EraTxId {
    pub era: u16,
    pub tx_id: TxId,
}

/// A transaction ID paired with its serialized size (for flow control).
/// `era` is retained from the wire so the follow-up `MsgRequestTxs` can
/// echo it; see [`EraTxId`].
#[derive(Debug, Clone)]
pub struct TxIdAndSize {
    pub tx_id: TxId,
    pub size: u32,
    pub era: u16,
}

/// A pending transaction waiting to be announced and sent.
#[derive(Debug, Clone)]
pub struct PendingTx {
    pub tx: MempoolTx,
    /// HardFork era index for this tx, carried end-to-end so it is echoed
    /// back on the wire exactly as the tx was received (rather than being
    /// re-stamped with a fixed constant). For txs we originate locally this
    /// is [`ORIGIN_ERA`].
    pub era: u16,
}

/// A transaction body tagged with its HardFork era, as it appears in
/// `MsgReplyTxs` on the wire (`[era, #6.24(bytes)]`). Mirrors [`EraTxId`] so
/// the era survives a receive-then-re-announce round trip.
#[derive(Debug, Clone)]
pub struct EraTxBody {
    pub era: u16,
    pub body: TxBody,
}

// --- State machine ---

/// TxSubmission protocol states.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State {
    /// Client sends MsgInit to start the protocol.
    StInit,
    /// Server requests tx ids or full transactions.
    StIdle,
    /// Client provides tx ids (blocking — must have at least one).
    StTxIdsBlocking,
    /// Client provides tx ids (non-blocking — may be empty).
    StTxIdsNonBlocking,
    /// Client provides full transactions.
    StTxs,
    /// Protocol complete.
    StDone,
}

/// TxSubmission protocol messages.
#[derive(Debug, Clone)]
pub enum Message {
    /// Client initiates the protocol. [6]
    MsgInit,
    /// Server requests tx ids (blocking). [0, true, ack, req]
    MsgRequestTxIdsBlocking { ack: u16, req: u16 },
    /// Server requests tx ids (non-blocking). [0, false, ack, req]
    MsgRequestTxIdsNonBlocking { ack: u16, req: u16 },
    /// Client replies with tx ids and their sizes. [1, [...]]
    MsgReplyTxIds { tx_ids: Vec<TxIdAndSize> },
    /// Server requests full transactions by id. [2, [...]]
    /// Ids are era-tagged ([`EraTxId`]) so they match the form the peer
    /// announced them in.
    MsgRequestTxs { tx_ids: Vec<EraTxId> },
    /// Client replies with full transactions. [3, [...]]
    /// Bodies are era-tagged ([`EraTxBody`]) so the era survives a
    /// receive-then-re-announce round trip.
    MsgReplyTxs { txs: Vec<EraTxBody> },
    /// Client terminates (only valid in StTxIdsBlocking). [4]
    MsgDone,
}

// --- Protocol trait ---

/// The TxSubmission protocol definition.
pub struct TxSubmission;

impl Protocol for TxSubmission {
    type State = State;
    type Message = Message;

    fn initial_state() -> State {
        State::StInit
    }

    fn agency(state: &State) -> Agency {
        match state {
            State::StInit => Agency::Client,
            State::StIdle => Agency::Server,
            State::StTxIdsBlocking => Agency::Client,
            State::StTxIdsNonBlocking => Agency::Client,
            State::StTxs => Agency::Client,
            State::StDone => Agency::Nobody,
        }
    }

    fn transition(state: &State, msg: &Message) -> Result<State, ProtocolError> {
        match (state, msg) {
            (State::StInit, Message::MsgInit) => Ok(State::StIdle),

            (State::StIdle, Message::MsgRequestTxIdsBlocking { .. }) => Ok(State::StTxIdsBlocking),
            (State::StIdle, Message::MsgRequestTxIdsNonBlocking { .. }) => {
                Ok(State::StTxIdsNonBlocking)
            }
            (State::StIdle, Message::MsgRequestTxs { .. }) => Ok(State::StTxs),

            (State::StTxIdsBlocking, Message::MsgReplyTxIds { .. }) => Ok(State::StIdle),
            (State::StTxIdsBlocking, Message::MsgDone) => Ok(State::StDone),

            (State::StTxIdsNonBlocking, Message::MsgReplyTxIds { .. }) => Ok(State::StIdle),

            (State::StTxs, Message::MsgReplyTxs { .. }) => Ok(State::StIdle),

            _ => Err(ProtocolError::InvalidMessage(format!(
                "{msg:?} not valid in state {state:?}"
            ))),
        }
    }

    fn size_limit(state: &State) -> usize {
        match state {
            State::StInit | State::StIdle => SIZE_LIMIT_SMALL,
            State::StTxIdsBlocking | State::StTxIdsNonBlocking | State::StTxs => SIZE_LIMIT_LARGE,
            State::StDone => 0,
        }
    }

    fn timeout(state: &State) -> Option<Duration> {
        match state {
            State::StInit => None,
            State::StIdle => None,
            State::StTxIdsBlocking => None, // client may block waiting for txs
            State::StTxIdsNonBlocking => Some(TIMEOUT_NON_BLOCKING),
            State::StTxs => Some(TIMEOUT_TXS),
            State::StDone => None,
        }
    }
}

// --- Client helpers ---

/// Acknowledge the oldest `ack` announced tx ids, dropping any of them that
/// are still sitting in `pending_bodies`.
///
/// A tx leaves the flow-control window on ack. If the consumer acked it
/// without ever requesting its body — the steady state for a deduplicating
/// consumer that fetches each tx from a single peer and acks it on all the
/// others — its body would otherwise be stranded in `pending_bodies` forever,
/// an unbounded per-peer leak of `Arc<[u8]>` bodies. `announced` is a strict
/// FIFO of the unacked window; `pending_bodies` is the not-yet-requested
/// subset, so an acked id may or may not still be present.
fn ack_and_prune(
    announced: &mut VecDeque<PendingTx>,
    pending_bodies: &mut VecDeque<PendingTx>,
    ack: u16,
) {
    for _ in 0..ack {
        let Some(acked) = announced.pop_front() else {
            break;
        };
        if let Some(pos) = pending_bodies.iter().position(|p| p.tx.tx_id == acked.tx.tx_id) {
            pending_bodies.remove(pos);
        }
    }
}

/// The canonical Praos `TxId` a tx must be announced under on the TxSubmission
/// wire: blake2b-256 of the transaction *body* (the ledger-effect id the peer
/// re-derives from the delivered body). This differs from the node's internal
/// `PendingTx::tx_id`, which is the `TxHash` (whole-tx hash) that Leios EBs key
/// on — so we translate here, at the Praos wire boundary.
///
/// Returns `None` for a tx that doesn't parse as a CBOR transaction: such a tx
/// has no canonical Praos id and must not be announced. Previously this fell
/// back to the whole-blob hash, which silently announced the tx under the
/// *wrong* id namespace (the TxHash Leios keys on) — so a peer could fetch it
/// by that id but never match it back, stranding it. A silent fallback here is
/// how that class of bug hid; a parse failure is now surfaced, not papered over.
fn praos_tx_id(tx: &PendingTx) -> Option<TxId> {
    net_codec::wire_tx_id(tx.tx.body.get_slice()).map(TxId::new_with_array)
}

/// Build the `MsgReplyTxIds` payload for a batch of txs, tracking exactly the
/// announced ones in `announced`/`pending_bodies`. Any tx with no canonical
/// Praos id (unparseable — see [`praos_tx_id`]) is dropped with a warning
/// rather than announced under the wrong id namespace; it also stays out of the
/// tracking deques so the ack FIFO and body-lookup set match what went on wire.
fn announce_txs(
    new_txs: &[PendingTx],
    announced: &mut VecDeque<PendingTx>,
    pending_bodies: &mut VecDeque<PendingTx>,
) -> Vec<TxIdAndSize> {
    let txs_announcement = new_txs.iter().map(
        |x| format!("{}:{},{}", x.tx.tx_id.hex_short(), x.tx.ours, x.tx.gen_slot)
    ).collect::<Vec<_>>();
    tracing::info!("Announcing Txs (len={}): {:?}", txs_announcement.len(), txs_announcement);

    let mut reply = Vec::with_capacity(new_txs.len());
    for tx in new_txs {
        match praos_tx_id(tx) {
            Some(tx_id) => {
                reply.push(TxIdAndSize {
                    tx_id,
                    size: tx.tx.size,
                    era: wire_era(tx.era),
                });
                announced.push_back(tx.clone());
                pending_bodies.push_back(tx.clone());
            }
            None => {
                tracing::warn!(
                    size = tx.tx.size,
                    "dropping tx with no canonical Praos wire id (unparseable body); \
                     not announcing on TxSubmission"
                );
            }
        }
    }
    reply
}

/// Run the client (tx provider) side of the TxSubmission protocol.
///
/// Sends MsgInit, then responds to server requests by pulling transactions
/// from `tx_receiver`. When the channel closes and all pending txs have been
/// sent and acknowledged, sends MsgDone and returns.
pub async fn run_client(
    runner: &mut Runner<TxSubmission>,
    tx_receiver: &mut mpsc::Receiver<PendingTx>,
    request_sender: Option<mpsc::Sender<u16>>,
    peer_id: PeerId,
) -> Result<(), ProtocolError> {
    // Send MsgInit to transition StInit -> StIdle.
    runner.send(&Message::MsgInit).await?;

    // FIFO of announced tx ids (announced but not yet acked by server).
    let mut announced: VecDeque<PendingTx> = VecDeque::new();
    // Tx ids that have been announced but not yet requested for full body.
    // We keep the full PendingTx so we can look up the body later.
    let mut pending_bodies: VecDeque<PendingTx> = VecDeque::new();

    loop {
        let msg = runner.recv().await?;

        match msg {
            Message::MsgRequestTxIdsBlocking { ack, req } => {
                // Acknowledge the first `ack` announced tx ids (and drop any
                // acked-but-unrequested bodies — see `ack_and_prune`).
                ack_and_prune(&mut announced, &mut pending_bodies, ack);

                // The blocking reply must announce at least one tx id (or send
                // MsgDone) — an empty `MsgReplyTxIds` violates StTxIdsBlocking and
                // desyncs the peer. Since `announce_txs` drops unparseable txs, a
                // batch of only-unparseable txs yields an empty reply; loop and
                // block for more until we can announce at least one.
                let reply = loop {
                    // Collect available txs from the channel + pending_bodies.
                    let mut new_txs: Vec<PendingTx> = Vec::new();

                    // Drain any already-buffered pending bodies into new_txs first.
                    while new_txs.len() < req as usize {
                        match tx_receiver.try_recv() {
                            Ok(tx) => new_txs.push(tx),
                            Err(_) => break,
                        }
                    }

                    // If still empty, notify the application that we need txs,
                    // then block waiting for at least one.
                    if new_txs.is_empty() {
                        if let Some(ref sender) = request_sender {
                            let _ = sender.try_send(req);
                        }
                        match tx_receiver.recv().await {
                            Some(tx) => new_txs.push(tx),
                            None => {
                                // Channel closed, no more txs — terminate.
                                runner.send(&Message::MsgDone).await?;
                                return Ok(());
                            }
                        }
                        // Try to fill up to req.
                        while new_txs.len() < req as usize {
                            match tx_receiver.try_recv() {
                                Ok(tx) => new_txs.push(tx),
                                Err(_) => break,
                            }
                        }
                    }

                    // Announce only txs with a well-defined canonical Praos id, and
                    // track exactly those for ack/body lookup (announcing fewer ids
                    // than we track would drift the ack FIFO).
                    let reply = announce_txs(&new_txs, &mut announced, &mut pending_bodies);

                    if !reply.is_empty() {
                        break reply;
                    }
                    // Every tx in this batch was unparseable and dropped; loop to
                    // block for more rather than send an empty blocking reply.
                };

                runner
                    .send(&Message::MsgReplyTxIds { tx_ids: reply })
                    .await?;
            }

            Message::MsgRequestTxIdsNonBlocking { ack, req } => {
                // Acknowledge the first `ack` announced tx ids (and drop any
                // acked-but-unrequested bodies — see `ack_and_prune`).
                ack_and_prune(&mut announced, &mut pending_bodies, ack);

                // Collect available txs (non-blocking).
                let mut new_txs: Vec<PendingTx> = Vec::new();
                while new_txs.len() < req as usize {
                    match tx_receiver.try_recv() {
                        Ok(tx) => new_txs.push(tx),
                        Err(_) => break,
                    }
                }

                let reply = announce_txs(&new_txs, &mut announced, &mut pending_bodies);

                drop(new_txs);

                runner
                    .send(&Message::MsgReplyTxIds { tx_ids: reply })
                    .await?;
            }

            Message::MsgRequestTxs { tx_ids } => {
                // Look up requested tx bodies from the pending set.
                let mut txs = Vec::new();
                for requested_id in &tx_ids {
                    // The peer requests by the canonical Praos TxId we announced
                    // (`praos_tx_id`), which differs from our internal TxHash key.
                    if let Some(pos) = pending_bodies
                        .iter()
                        .position(|p| praos_tx_id(p).as_ref() == Some(&requested_id.tx_id))
                    {
                        let pending = pending_bodies.remove(pos).expect("position valid");
                        txs.push(EraTxBody {
                            era: wire_era(pending.era),
                            body: pending.tx.body,
                        });
                    }
                    // Per spec: omitted txs are treated as never announced.
                }

                runner.send(&Message::MsgReplyTxs { txs }).await?;
            }

            other => {
                return Err(ProtocolError::InvalidMessage(format!(
                    "client received unexpected message: {other:?}"
                )));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bearer::mem::MemBearer;
    use crate::mux::scheduler::{RoundRobin, TrafficClass};
    use crate::mux::{
        CodecRecv, CodecSend, Mux, MuxConfig, ProtocolConfig, MODE_INITIATOR, MODE_RESPONDER,
    };
    use crate::protocols::Role;

    #[test]
    fn agency_correct() {
        assert_eq!(TxSubmission::agency(&State::StInit), Agency::Client);
        assert_eq!(TxSubmission::agency(&State::StIdle), Agency::Server);
        assert_eq!(
            TxSubmission::agency(&State::StTxIdsBlocking),
            Agency::Client
        );
        assert_eq!(
            TxSubmission::agency(&State::StTxIdsNonBlocking),
            Agency::Client
        );
        assert_eq!(TxSubmission::agency(&State::StTxs), Agency::Client);
        assert_eq!(TxSubmission::agency(&State::StDone), Agency::Nobody);
    }

    #[test]
    fn valid_transitions() {
        // StInit -> StIdle
        assert_eq!(
            TxSubmission::transition(&State::StInit, &Message::MsgInit).unwrap(),
            State::StIdle
        );

        // StIdle -> StTxIdsBlocking
        assert_eq!(
            TxSubmission::transition(
                &State::StIdle,
                &Message::MsgRequestTxIdsBlocking { ack: 0, req: 5 }
            )
            .unwrap(),
            State::StTxIdsBlocking
        );

        // StIdle -> StTxIdsNonBlocking
        assert_eq!(
            TxSubmission::transition(
                &State::StIdle,
                &Message::MsgRequestTxIdsNonBlocking { ack: 0, req: 5 }
            )
            .unwrap(),
            State::StTxIdsNonBlocking
        );

        // StIdle -> StTxs
        assert_eq!(
            TxSubmission::transition(
                &State::StIdle,
                &Message::MsgRequestTxs {
                    tx_ids: vec![EraTxId {
                        era: ORIGIN_ERA,
                        tx_id: TxId::new_with_array([0x42; 32])
                    }]
                }
            )
            .unwrap(),
            State::StTxs
        );

        // StTxIdsBlocking -> StIdle (reply)
        assert_eq!(
            TxSubmission::transition(
                &State::StTxIdsBlocking,
                &Message::MsgReplyTxIds { tx_ids: vec![] }
            )
            .unwrap(),
            State::StIdle
        );

        // StTxIdsBlocking -> StDone
        assert_eq!(
            TxSubmission::transition(&State::StTxIdsBlocking, &Message::MsgDone).unwrap(),
            State::StDone
        );

        // StTxIdsNonBlocking -> StIdle
        assert_eq!(
            TxSubmission::transition(
                &State::StTxIdsNonBlocking,
                &Message::MsgReplyTxIds { tx_ids: vec![] }
            )
            .unwrap(),
            State::StIdle
        );

        // StTxs -> StIdle
        assert_eq!(
            TxSubmission::transition(&State::StTxs, &Message::MsgReplyTxs { txs: vec![] }).unwrap(),
            State::StIdle
        );
    }

    #[test]
    fn invalid_transitions() {
        // MsgDone only valid in StTxIdsBlocking
        assert!(TxSubmission::transition(&State::StTxIdsNonBlocking, &Message::MsgDone).is_err());
        assert!(TxSubmission::transition(&State::StIdle, &Message::MsgDone).is_err());
        assert!(TxSubmission::transition(&State::StInit, &Message::MsgDone).is_err());

        // MsgInit only valid in StInit
        assert!(TxSubmission::transition(&State::StIdle, &Message::MsgInit).is_err());

        // Server messages not valid in client states
        assert!(TxSubmission::transition(
            &State::StTxIdsBlocking,
            &Message::MsgRequestTxs {
                tx_ids: vec![EraTxId {
                    era: ORIGIN_ERA,
                    tx_id: TxId::new_with_array([0u8; 32])
                }]
            }
        )
        .is_err());
    }

    #[test]
    fn size_limits() {
        assert_eq!(TxSubmission::size_limit(&State::StInit), SIZE_LIMIT_SMALL);
        assert_eq!(TxSubmission::size_limit(&State::StIdle), SIZE_LIMIT_SMALL);
        assert_eq!(
            TxSubmission::size_limit(&State::StTxIdsBlocking),
            SIZE_LIMIT_LARGE
        );
        assert_eq!(
            TxSubmission::size_limit(&State::StTxIdsNonBlocking),
            SIZE_LIMIT_LARGE
        );
        assert_eq!(TxSubmission::size_limit(&State::StTxs), SIZE_LIMIT_LARGE);
    }

    #[test]
    fn timeouts() {
        assert_eq!(TxSubmission::timeout(&State::StInit), None);
        assert_eq!(TxSubmission::timeout(&State::StIdle), None);
        assert_eq!(TxSubmission::timeout(&State::StTxIdsBlocking), None);
        assert_eq!(
            TxSubmission::timeout(&State::StTxIdsNonBlocking),
            Some(TIMEOUT_NON_BLOCKING)
        );
        assert_eq!(TxSubmission::timeout(&State::StTxs), Some(TIMEOUT_TXS));
        assert_eq!(TxSubmission::timeout(&State::StDone), None);
    }

    fn test_config() -> MuxConfig {
        MuxConfig {
            sdu_timeout: std::time::Duration::from_secs(2),
            ..MuxConfig::default()
        }
    }

    fn make_txsubmission_mux_pair() -> (
        (CodecSend, CodecRecv),
        (CodecSend, CodecRecv),
        crate::mux::RunningMux,
        crate::mux::RunningMux,
    ) {
        let (bearer_a, bearer_b) = MemBearer::pair();

        let proto = ProtocolConfig {
            id: PROTOCOL_ID,
            traffic_class: TrafficClass::Priority,
            ingress_limit: INGRESS_LIMIT,
            egress_queue_size: 16,
        };

        let mut mux_a = Mux::new(test_config(), RoundRobin::default(), MODE_INITIATOR);
        let (send_a, recv_a) = mux_a.register(&proto);
        let running_a = mux_a.run(bearer_a);

        let mut mux_b = Mux::new(test_config(), RoundRobin::default(), MODE_RESPONDER);
        let (send_b, recv_b) = mux_b.register(&proto);
        let running_b = mux_b.run(bearer_b);

        (
            (CodecSend::new(send_a), CodecRecv::new(recv_a)),
            (CodecSend::new(send_b), CodecRecv::new(recv_b)),
            running_a,
            running_b,
        )
    }

    fn make_test_tx(id_byte: u8, size: usize) -> PendingTx {
        // A parseable tx array `[body, {}, true, null]` so the body-only wire
        // id (`praos_tx_id`) is well-defined; body bytes vary by `id_byte` so
        // distinct test txs get distinct wire ids. Reserve a few bytes for the
        // CBOR framing so the total stays ~= `size`.
        let body_len = size.saturating_sub(6).max(1);
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(4)
            .and_then(|e| e.bytes(&vec![id_byte; body_len]))
            .and_then(|e| e.map(0))
            .and_then(|e| e.bool(true))
            .and_then(|e| e.null())
            .expect("encoding test tx into a Vec is infallible");
        PendingTx {
            tx_id: TxId::new_with_array([id_byte; 32]),
            body: TxBody::new_with_vec(e.into_writer()),
            size: size as u32,
            era: ORIGIN_ERA,
        }
    }

    #[test]
    fn praos_tx_id_is_body_hash_not_whole_tx() {
        // A real tx is `[body, witness_set, is_valid?, aux]`. The Praos TxId we
        // announce on the wire must be blake2b(body element), NOT the whole-tx
        // hash we key on internally (TxHash), nor the arbitrary stored tx_id.
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(3).unwrap();
        e.map(1).unwrap();
        e.u8(0).unwrap().array(0).unwrap(); // body: {0: []}
        e.map(0).unwrap(); // witness set
        e.null().unwrap(); // aux data
        let tx = e.into_writer();
        let pending = PendingTx {
            tx_id: TxId::new_with_array([0xAB; 32]), // arbitrary internal (TxHash) key
            body: TxBody::new_with_slice(&tx),
            size: tx.len() as u32,
            era: ORIGIN_ERA,
        };
        let announced = praos_tx_id(&pending).expect("parseable tx has a canonical id");
        assert_eq!(
            announced,
            TxId::new_with_array(net_codec::wire_tx_id(&tx).unwrap()),
            "announced id must be the canonical body-element TxId"
        );
        assert_ne!(
            announced, pending.tx_id,
            "must not be the internal TxHash key"
        );
        assert_ne!(
            announced,
            TxId::new_with_array(pending.body.get_blake2b_256()),
            "must not be the whole-tx hash"
        );
    }

    #[test]
    fn wire_era_normalises_out_of_range() {
        // In-range eras (including the boundary) are echoed verbatim.
        assert_eq!(wire_era(0), 0);
        assert_eq!(wire_era(ORIGIN_ERA), ORIGIN_ERA);
        assert_eq!(wire_era(MAX_ERA), MAX_ERA);
        // Out-of-range eras — a poisoned/garbage value from a misbehaving
        // upstream — fall back to ORIGIN_ERA rather than being re-announced and
        // getting us dropped downstream ("invalid index N").
        assert_eq!(wire_era(MAX_ERA + 1), ORIGIN_ERA);
        assert_eq!(wire_era(u16::MAX), ORIGIN_ERA);
    }

    #[test]
    fn ack_and_prune_drops_acked_unrequested_bodies() {
        // Announce three txs; none requested yet.
        let mut announced: VecDeque<PendingTx> = VecDeque::new();
        let mut pending: VecDeque<PendingTx> = VecDeque::new();
        for b in [1u8, 2, 3] {
            let tx = make_test_tx(b, 64);
            announced.push_back(tx.clone());
            pending.push_back(tx);
        }

        // Consumer acks the first two WITHOUT requesting their bodies (the
        // deduplicating-consumer steady state). Both must leave both queues —
        // otherwise their bodies leak in `pending_bodies` forever.
        ack_and_prune(&mut announced, &mut pending, 2);
        assert_eq!(announced.len(), 1, "ack pops the window");
        assert_eq!(
            pending.len(),
            1,
            "acked-but-unrequested bodies must be pruned, not stranded"
        );
        assert_eq!(pending.front().unwrap().tx_id, make_test_tx(3, 64).tx_id);
    }

    #[test]
    fn ack_and_prune_leaves_still_windowed_and_already_requested() {
        let mut announced: VecDeque<PendingTx> = VecDeque::new();
        let mut pending: VecDeque<PendingTx> = VecDeque::new();
        for b in [1u8, 2] {
            let tx = make_test_tx(b, 64);
            announced.push_back(tx.clone());
            pending.push_back(tx);
        }
        // Tx 1's body was already requested (removed from pending) before ack.
        pending.pop_front();
        // Ack tx 1: it's gone from pending already — prune is a no-op there,
        // and tx 2 stays (still inside the unacked window).
        ack_and_prune(&mut announced, &mut pending, 1);
        assert_eq!(announced.len(), 1);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending.front().unwrap().tx_id, make_test_tx(2, 64).tx_id);
    }

    #[tokio::test]
    async fn txsubmission_client_server_exchange() {
        let ((cs, cr), (ss, sr), ra, rb) = make_txsubmission_mux_pair();

        let tx1 = make_test_tx(0x01, 1500);
        let tx2 = make_test_tx(0x02, 2000);
        let tx1_id = tx1.tx_id.clone();
        let tx2_id = tx2.tx_id.clone();

        // Server: drive the protocol by requesting tx ids, then txs.
        let server = tokio::spawn(async move {
            let mut runner = Runner::<TxSubmission>::new(Role::Server, ss, sr);

            // Receive MsgInit.
            let msg = runner.recv().await.unwrap();
            assert!(matches!(msg, Message::MsgInit));

            // Request tx ids (blocking), ack 0, request up to 10.
            runner
                .send(&Message::MsgRequestTxIdsBlocking { ack: 0, req: 10 })
                .await
                .unwrap();

            let msg = runner.recv().await.unwrap();
            let announced_ids = match msg {
                Message::MsgReplyTxIds { tx_ids } => {
                    assert_eq!(tx_ids.len(), 2);
                    assert_eq!(tx_ids[0].size, 1500);
                    assert_eq!(tx_ids[1].size, 2000);
                    tx_ids
                        .into_iter()
                        .map(|t| EraTxId {
                            era: t.era,
                            tx_id: t.tx_id,
                        })
                        .collect::<Vec<_>>()
                }
                other => panic!("expected MsgReplyTxIds, got {other:?}"),
            };

            // Request full transactions.
            runner
                .send(&Message::MsgRequestTxs {
                    tx_ids: announced_ids,
                })
                .await
                .unwrap();

            let msg = runner.recv().await.unwrap();
            match msg {
                Message::MsgReplyTxs { txs } => {
                    assert_eq!(txs.len(), 2);
                }
                other => panic!("expected MsgReplyTxs, got {other:?}"),
            }

            // Ack the 2 txs and request more (blocking). Client should send MsgDone.
            runner
                .send(&Message::MsgRequestTxIdsBlocking { ack: 2, req: 10 })
                .await
                .unwrap();

            let msg = runner.recv().await.unwrap();
            assert!(matches!(msg, Message::MsgDone));
        });

        // Client: use run_client with a channel.
        let client = tokio::spawn(async move {
            let mut runner = Runner::<TxSubmission>::new(Role::Client, cs, cr);
            let (tx_sender, mut tx_receiver) = tokio::sync::mpsc::channel(16);

            // Pre-load txs.
            tx_sender
                .send(PendingTx {
                    tx_id: tx1_id,
                    body: tx1.body.clone(),
                    size: 1500,
                    era: ORIGIN_ERA,
                })
                .await
                .unwrap();
            tx_sender
                .send(PendingTx {
                    tx_id: tx2_id,
                    body: tx2.body.clone(),
                    size: 2000,
                    era: ORIGIN_ERA,
                })
                .await
                .unwrap();
            // Close channel so client knows no more txs.
            drop(tx_sender);

            run_client(&mut runner, &mut tx_receiver, None)
                .await
                .unwrap();
        });

        client.await.unwrap();
        server.await.unwrap();
        ra.abort();
        rb.abort();
    }

    #[tokio::test]
    async fn txsubmission_non_blocking_empty_reply() {
        let ((cs, cr), (ss, sr), ra, rb) = make_txsubmission_mux_pair();

        // Server: send non-blocking request, expect empty reply, then blocking
        // request which triggers MsgDone.
        let server = tokio::spawn(async move {
            let mut runner = Runner::<TxSubmission>::new(Role::Server, ss, sr);

            let msg = runner.recv().await.unwrap();
            assert!(matches!(msg, Message::MsgInit));

            // Non-blocking request: no txs available, should get empty reply.
            runner
                .send(&Message::MsgRequestTxIdsNonBlocking { ack: 0, req: 5 })
                .await
                .unwrap();

            let msg = runner.recv().await.unwrap();
            match msg {
                Message::MsgReplyTxIds { tx_ids } => assert!(tx_ids.is_empty()),
                other => panic!("expected empty MsgReplyTxIds, got {other:?}"),
            }

            // Blocking request: channel closed, should get MsgDone.
            runner
                .send(&Message::MsgRequestTxIdsBlocking { ack: 0, req: 5 })
                .await
                .unwrap();

            let msg = runner.recv().await.unwrap();
            assert!(matches!(msg, Message::MsgDone));
        });

        let client = tokio::spawn(async move {
            let mut runner = Runner::<TxSubmission>::new(Role::Client, cs, cr);
            let (_tx_sender, mut tx_receiver) = tokio::sync::mpsc::channel::<PendingTx>(16);
            // Drop sender immediately — no txs to send.
            drop(_tx_sender);

            run_client(&mut runner, &mut tx_receiver, None)
                .await
                .unwrap();
        });

        client.await.unwrap();
        server.await.unwrap();
        ra.abort();
        rb.abort();
    }
}
