//! Issue #116: announcement-before-offer ordering on each LeiosNotify connection.
//!
//! Exercise the production responder, codec and mux without sockets or live nodes.
//! Headers have parseable wire bytes and the correct EB commitment; their dummy
//! signatures are not intended for consensus validation.

use std::sync::Arc;
use std::time::Duration;

use net_core::bearer::mem::MemBearer;
use net_core::mux::scheduler::{PriorityWfq, TrafficClass};
use net_core::mux::{
    CodecRecv, CodecSend, Mux, MuxConfig, ProtocolConfig, RunningMux, MODE_INITIATOR,
    MODE_RESPONDER,
};
use net_core::peer::{
    server_handlers::{serve_leios_notify, OutboundControls},
    PeerId,
};
use net_core::protocols::leios_notify::{self, Message};
use net_core::store::leios_store::LeiosStore;
use net_core::types::{Point, Vote, WrappedHeader};
use shared_consensus::behaviour::tree::{ControlSignal, EquivocationVariants};

struct Connection {
    send: CodecSend,
    recv: CodecRecv,
    server: tokio::task::JoinHandle<()>,
    client_mux: RunningMux,
    server_mux: RunningMux,
}

impl Connection {
    async fn open(store: Arc<LeiosStore>, peer: PeerId) -> Self {
        Self::open_with_controls(store, peer, None).await
    }

    async fn open_with_controls(
        store: Arc<LeiosStore>,
        peer: PeerId,
        controls: Option<OutboundControls>,
    ) -> Self {
        let protocol = ProtocolConfig {
            id: leios_notify::PROTOCOL_ID,
            traffic_class: TrafficClass::Default(1),
            ingress_limit: leios_notify::INGRESS_LIMIT,
            egress_queue_size: 16,
        };
        // This single-protocol harness has no KeepAlive task. Allow the
        // notification responder's 50-second idle reply to arrive first.
        let config = MuxConfig {
            sdu_timeout: Duration::from_secs(120),
            ..MuxConfig::default()
        };
        let (client_bearer, server_bearer) = MemBearer::pair();
        let mut client = Mux::new(config.clone(), PriorityWfq::default(), MODE_INITIATOR);
        let (client_send, client_recv) = client.register(&protocol);
        let mut server = Mux::new(config, PriorityWfq::default(), MODE_RESPONDER);
        let (server_send, server_recv) = server.register(&protocol);
        let client_mux = client.run(client_bearer);
        let server_mux = server.run(server_bearer);
        let server = tokio::spawn(async move {
            serve_leios_notify(
                CodecSend::new(server_send),
                CodecRecv::new(server_recv),
                store,
                peer,
                controls.as_ref(),
            )
            .await
            .unwrap();
        });
        let mut connection = Self {
            send: CodecSend::new(client_send),
            recv: CodecRecv::new(client_recv),
            server,
            client_mux,
            server_mux,
        };

        // Receiving a reply proves that the first request was processed and
        // the cursor snapped past the backlog. Paused Tokio time advances to
        // the idle reply automatically; no scheduling sleeps are needed.
        connection.request().await;
        let first = tokio::time::timeout(
            Duration::from_secs(60),
            connection.recv.recv::<Message>(leios_notify::SIZE_LIMIT),
        )
        .await
        .expect("connection did not finish its initial idle request")
        .expect("initial response must decode");
        assert!(
            matches!(&first, Message::MsgLeiosVotes { votes } if votes.is_empty()),
            "startup must skip the pre-existing backlog, got {first:?}"
        );
        connection
    }

    async fn request(&self) {
        self.send
            .send(&Message::MsgLeiosNotificationRequestNext)
            .await
            .expect("request must enter the live mux");
    }

    async fn next(&mut self) -> Message {
        self.request().await;
        tokio::time::timeout(
            Duration::from_secs(1),
            self.recv.recv::<Message>(leios_notify::SIZE_LIMIT),
        )
        .await
        .expect("eligible notification was suppressed or stranded")
        .expect("notification must decode")
    }

    async fn close(&mut self) {
        self.send.send(&Message::MsgQuit).await.unwrap();
        let reply = tokio::time::timeout(
            Duration::from_secs(1),
            self.recv.recv::<Message>(leios_notify::SIZE_LIMIT),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(reply, Message::MsgDone), "got {reply:?}");
        tokio::time::timeout(Duration::from_secs(1), &mut self.server)
            .await
            .unwrap()
            .unwrap();
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Also clean up when a regression assertion panics.
        self.server.abort();
        self.client_mux.abort();
        self.server_mux.abort();
    }
}

fn announced_block(slot: u64) -> (Point, WrappedHeader, Vec<u8>) {
    let body = vec![0xa0];
    let hash = net_codec::blake2b_256(&body);
    let size = u32::try_from(body.len()).unwrap();
    let point = Point::Specific { slot, hash };
    let mut inner = minicbor::Encoder::new(Vec::new());
    inner.array(2).unwrap(); // [header_body, signature]
    inner.array(12).unwrap();
    inner.u64(1).unwrap(); // block number
    inner.u64(slot).unwrap();
    inner.null().unwrap(); // previous header hash
    inner.bytes(&[1; 32]).unwrap(); // issuer verification key
    inner.bytes(&[2; 32]).unwrap(); // VRF verification key
    inner.array(2).unwrap();
    inner.bytes(&[3; 64]).unwrap(); // VRF output
    inner.bytes(&[4; 80]).unwrap(); // VRF proof
    inner.u32(1).unwrap(); // ranking-block body size
    inner.bytes(&[5; 32]).unwrap(); // ranking-block body hash
    inner.array(4).unwrap(); // operational certificate
    inner.bytes(&[6; 32]).unwrap();
    inner.u64(0).unwrap();
    inner.u64(0).unwrap();
    inner.bytes(&[7; 64]).unwrap();
    inner.array(2).unwrap(); // protocol version
    inner.u32(11).unwrap();
    inner.u32(0).unwrap();
    inner.bool(false).unwrap(); // certified_eb
    inner.array(2).unwrap(); // announced_eb
    inner.bytes(&hash).unwrap();
    inner.u32(size).unwrap();
    inner.bytes(&[8; 64]).unwrap(); // dummy header signature
    let mut outer = minicbor::Encoder::new(Vec::new());
    outer.array(2).unwrap();
    outer.u8(8).unwrap(); // Dijkstra era tag
    outer.tag(minicbor::data::Tag::new(24)).unwrap();
    outer.bytes(&inner.into_writer()).unwrap();
    let header = WrappedHeader::new(outer.into_writer());
    let parsed = header.parsed.as_ref().expect("fixture header must parse");
    assert_eq!(parsed.slot, slot);
    assert_eq!(parsed.announced_eb, Some((hash, size)));
    assert_ne!(
        header.point(),
        Some(point.clone()),
        "RB and EB identities differ"
    );
    (point, header, body)
}

fn marker(id: u8) -> Vote {
    Vote {
        announcing_rb_hash: [id; 32],
        voter_id: u16::from(id),
        vote_signature: vec![id; 48],
    }
}

fn assert_announcement(message: Message, expected: &WrappedHeader) {
    match message {
        Message::MsgLeiosBlockAnnouncement { header } => {
            assert_eq!(header.raw, expected.raw, "replay the original header bytes");
            assert_eq!(header.parsed, expected.parsed);
        }
        other => {
            panic!("matching announcement must precede the offer on this connection; got {other:?}")
        }
    }
}

fn assert_body(message: Message, point: &Point, size: usize) {
    match message {
        Message::MsgLeiosBlockOffer {
            point: offered,
            eb_size,
        } => {
            assert_eq!(&offered, point);
            assert_eq!(usize::try_from(eb_size).unwrap(), size);
        }
        other => panic!("eligible body offer must arrive after its announcement; got {other:?}"),
    }
}

fn assert_marker(message: Message, expected: &Vote) {
    match message {
        Message::MsgLeiosVotes { votes } => assert_eq!(votes, vec![expected.clone()]),
        other => panic!("unrelated votes must progress without an unannounced or duplicate offer; got {other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn announcement_then_body_is_delivered_with_matching_wire_identity() {
    let (store, _watch) = LeiosStore::new(100);
    let mut connection = Connection::open(store.clone(), PeerId(7)).await;
    let (point, header, body) = announced_block(21);
    store.announce_block(header.clone(), None);
    store.inject_block(point.clone(), body.clone(), None);
    assert_announcement(connection.next().await, &header);
    assert_body(connection.next().await, &point, body.len());
    connection.close().await;
}

#[tokio::test(start_paused = true)]
async fn reconnect_retry_replays_announcement_before_body_offer() {
    let (store, _watch) = LeiosStore::new(100);
    let mut original = Connection::open(store.clone(), PeerId(7)).await;
    let (point, header, body) = announced_block(21);
    store.announce_block(header.clone(), None);
    store.inject_block(point.clone(), body.clone(), None);
    assert_announcement(original.next().await, &header);
    assert_body(original.next().await, &point, body.len());
    original.close().await;
    drop(original);

    // Same PeerId, genuinely new connection. Its initial idle reply proves it
    // skipped both old notifications before the body-only retry is injected.
    let mut reconnected = Connection::open(store.clone(), PeerId(7)).await;
    store.reoffer_unfetched(24);
    assert_announcement(reconnected.next().await, &header);
    assert_body(reconnected.next().await, &point, body.len());

    // Retry suppression must still work once this connection has the offer.
    store.reoffer_unfetched(27);
    let sentinel = marker(1);
    store.inject_votes(vec![sentinel.clone()], None);
    assert_marker(reconnected.next().await, &sentinel);
    reconnected.close().await;
}

#[tokio::test(start_paused = true)]
async fn relayed_body_recovers_header_filtered_back_to_its_source() {
    let (store, _watch) = LeiosStore::new(100);
    let header_source = PeerId(7);
    let body_source = PeerId(8);
    let mut connection = Connection::open(store.clone(), header_source).await;
    let (point, header, body) = announced_block(21);
    // The header came from this peer, while the body came from another peer.
    // Only the header is source-filtered; the body is otherwise eligible.
    store.announce_block(header.clone(), Some(header_source));
    store.inject_block(point.clone(), body.clone(), Some(body_source));
    assert_announcement(connection.next().await, &header);
    assert_body(connection.next().await, &point, body.len());
    connection.close().await;
}

#[tokio::test(start_paused = true)]
async fn source_body_offer_stays_filtered_while_other_notifications_progress() {
    let (store, _watch) = LeiosStore::new(100);
    let source = PeerId(7);
    let mut connection = Connection::open(store.clone(), source).await;
    let (point, header, body) = announced_block(21);
    store.announce_block(header, Some(source));
    store.inject_block(point, body, Some(source));
    let sentinel = marker(2);
    store.inject_votes(vec![sentinel.clone()], None);
    assert_marker(connection.next().await, &sentinel);
    connection.close().await;
}

async fn check_late_header_releases_offer(transactions: bool) {
    let (store, _watch) = LeiosStore::new(100);
    let mut connection = Connection::open(store.clone(), PeerId(7)).await;
    let (point, header, body) = announced_block(21);
    if transactions {
        // A transaction-closure offer is independent of a body offer. This
        // empty manifest has an empty closure; no body offer is injected.
        store.inject_block_txs_full(point.clone(), vec![], Some(PeerId(8)));
    } else {
        store.inject_block(point.clone(), body.clone(), Some(PeerId(8)));
    }
    let before = marker(3);
    store.inject_votes(vec![before.clone()], None);
    // Receipt of the later marker is a barrier: the server must have scanned
    // past the offer without delivering it or blocking unrelated traffic.
    assert_marker(connection.next().await, &before);

    // Only the header is added now: passing requires retaining the earlier
    // offer. Dropping it and relying on another retry cannot satisfy this.
    store.announce_block(header.clone(), Some(PeerId(8)));
    assert_announcement(connection.next().await, &header);
    let offer = connection.next().await;
    if transactions {
        assert!(
            matches!(&offer, Message::MsgLeiosBlockTxsOffer { point: offered } if offered == &point),
            "deferred transaction offer must arrive after its announcement; got {offer:?}"
        );
    } else {
        assert_body(offer, &point, body.len());
    }
    let after = marker(4);
    store.inject_votes(vec![after.clone()], None);
    assert_marker(connection.next().await, &after);
    connection.close().await;
}

#[tokio::test(start_paused = true)]
async fn late_header_releases_deferred_body_without_blocking_votes() {
    check_late_header_releases_offer(false).await;
}

#[tokio::test(start_paused = true)]
async fn late_header_releases_deferred_transactions_without_a_body_offer() {
    check_late_header_releases_offer(true).await;
}

#[tokio::test(start_paused = true)]
async fn missing_header_does_not_block_another_block_and_evicted_body_is_not_offered() {
    let (store, _) = LeiosStore::new(1);
    let mut connection = Connection::open(store.clone(), PeerId(7)).await;
    let (missing, late_header, body) = announced_block(21);
    let (ready, header, other_body) = announced_block(22);
    store.inject_block(missing.clone(), body, Some(PeerId(8)));
    store.announce_block(header.clone(), None);
    store.inject_block(ready.clone(), other_body, None);
    assert_announcement(connection.next().await, &header);
    assert_body(connection.next().await, &ready, 1);
    // The original body was evicted by capacity. Its late header can travel,
    // but the responder must not advertise body availability that has gone.
    store.announce_block(late_header.clone(), None);
    assert_announcement(connection.next().await, &late_header);
    let barrier = marker(91);
    store.inject_votes(vec![barrier.clone()], None);
    assert_marker(connection.next().await, &barrier);
    connection.close().await;
}

fn controlled() -> (OutboundControls, tokio::sync::watch::Sender<ControlSignal>) {
    let (tx, control) = tokio::sync::watch::channel(ControlSignal::default());
    (
        OutboundControls {
            control,
            variants: Arc::new(std::sync::Mutex::new(EquivocationVariants::default())),
        },
        tx,
    )
}

#[tokio::test(start_paused = true)]
async fn withholding_control_releases_only_local_offers_without_faking_sent_announcements() {
    let (store, _) = LeiosStore::new(10);
    let (controls, tx) = controlled();
    let mut connection =
        Connection::open_with_controls(store.clone(), PeerId(7), Some(controls)).await;
    let (local, local_header, body) = announced_block(21);
    let (relayed, relayed_header, remote_body) = announced_block(22);
    store.inject_block(local.clone(), body, None);
    store.inject_block(relayed.clone(), remote_body, Some(PeerId(8)));
    let barrier = marker(92);
    store.inject_votes(vec![barrier.clone()], None);
    assert_marker(connection.next().await, &barrier);
    // A control-only wakeup must release the deferred local offer.
    let (reply, _) = tokio::join!(connection.next(), async {
        tokio::task::yield_now().await;
        tx.send_modify(|c| c.leios.withhold_announce = true);
    });
    assert_body(reply, &local, 1);
    let barrier = marker(93);
    store.inject_votes(vec![barrier.clone()], None);
    assert_marker(connection.next().await, &barrier); // remote offer still gated
    tx.send_modify(|c| c.leios.withhold_announce = false);
    store.inject_block_txs_full(local.clone(), vec![], None);
    let barrier = marker(94);
    store.inject_votes(vec![barrier.clone()], None);
    assert_marker(connection.next().await, &barrier); // body did not imply announcement
    store.announce_block(local_header.clone(), None);
    assert_announcement(connection.next().await, &local_header);
    assert!(
        matches!(connection.next().await, Message::MsgLeiosBlockTxsOffer { point } if point == local)
    );
    store.announce_block(relayed_header.clone(), Some(PeerId(8)));
    assert_announcement(connection.next().await, &relayed_header);
    assert_body(connection.next().await, &relayed, 1);
    connection.close().await;
}

#[tokio::test(start_paused = true)]
async fn deferred_offer_rechecks_echo_and_size_controls_at_delivery() {
    let (store, _) = LeiosStore::new(10);
    let (controls, tx) = controlled();
    tx.send_modify(|c| c.leios.echo_to_source = true);
    let mut connection =
        Connection::open_with_controls(store.clone(), PeerId(7), Some(controls)).await;
    let (point, header, body) = announced_block(21);
    store.inject_block(point.clone(), body, Some(PeerId(7)));
    let barrier = marker(96);
    store.inject_votes(vec![barrier.clone()], None);
    assert_marker(connection.next().await, &barrier); // body is now deferred
    store.announce_block(header.clone(), Some(PeerId(7)));
    assert_announcement(connection.next().await, &header);
    tx.send_modify(|c| c.leios.echo_to_source = false);
    let barrier = marker(97);
    store.inject_votes(vec![barrier.clone()], None);
    assert_marker(connection.next().await, &barrier); // queued body rechecks no-echo
    tx.send_modify(|c| {
        c.leios.echo_to_source = true;
        c.leios.offer_eb_size = shared_consensus::behaviour::tree::control::EbSizePolicy::Linear {
            scale_num: 0,
            scale_den: 1,
            offset: 0,
        };
    });
    assert_body(connection.next().await, &point, 0);
    connection.close().await;
}

#[tokio::test(start_paused = true)]
async fn repeated_headers_coalesce_but_distinct_equivocation_headers_survive() {
    let (store, _) = LeiosStore::new(10);
    let mut connection = Connection::open(store.clone(), PeerId(7)).await;
    let (point, header, body) = announced_block(21);
    let mut other = header.clone();
    let issuer_offset = other.raw.windows(32).position(|b| b == [1; 32]).unwrap();
    other.raw[issuer_offset] = 9;
    let other = WrappedHeader::new(other.raw);
    assert_ne!(other.point(), header.point());
    store.announce_block(header.clone(), None);
    store.inject_block(point.clone(), body, None);
    assert_announcement(connection.next().await, &header);
    assert_body(connection.next().await, &point, 1);
    store.announce_block(header, Some(PeerId(8)));
    store.announce_block(other.clone(), None);
    assert_announcement(connection.next().await, &other);
    let barrier = marker(95);
    store.inject_votes(vec![barrier.clone()], None);
    assert_marker(connection.next().await, &barrier);
    connection.close().await;
}

#[tokio::test(start_paused = true)]
async fn deferred_transactions_survive_eviction_and_refetch() {
    let (store, _) = LeiosStore::new(1);
    let mut connection = Connection::open(store.clone(), PeerId(7)).await;
    let (point, header, body) = announced_block(21);
    let txs = vec![shared_consensus::mempool::TxBody::new_with_vec(vec![1])];
    store.inject_block(point.clone(), body, Some(PeerId(8)));
    store.inject_block_txs_full(point.clone(), txs.clone(), Some(PeerId(8)));
    let pending = marker(101);
    store.inject_votes(vec![pending.clone()], None);
    assert_marker(connection.next().await, &pending);

    // Capacity eviction removes both the body and transaction cache while
    // retaining their log entries. A marker proves the responder saw the gap.
    let (other, _, other_body) = announced_block(22);
    store.inject_block(other, other_body, Some(PeerId(7)));
    let evicted = marker(102);
    store.inject_votes(vec![evicted.clone()], None);
    assert_marker(connection.next().await, &evicted);

    store.announce_block(header.clone(), Some(PeerId(8)));
    assert_announcement(connection.next().await, &header);
    let unavailable = marker(103);
    store.inject_votes(vec![unavailable.clone()], None);
    assert_marker(connection.next().await, &unavailable);

    // Re-fetching only the transactions coalesces into the already-read log
    // entry. Delivery must resume from the retained unsent intent.
    store.inject_block_txs_full(point.clone(), txs.clone(), Some(PeerId(8)));
    let restored = marker(104);
    store.inject_votes(vec![restored.clone()], None);
    let offer = connection.next().await;
    assert!(
        matches!(&offer, Message::MsgLeiosBlockTxsOffer { point: offered } if offered == &point),
        "restored transactions must release the deferred offer; got {offer:?}"
    );
    assert_marker(connection.next().await, &restored);

    store.inject_block_txs_full(point, txs, Some(PeerId(8)));
    let duplicate = marker(105);
    store.inject_votes(vec![duplicate.clone()], None);
    assert_marker(connection.next().await, &duplicate);
    connection.close().await;
}
