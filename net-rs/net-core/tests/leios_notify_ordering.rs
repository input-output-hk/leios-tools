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
use net_core::peer::{server_handlers::serve_leios_notify, PeerId};
use net_core::protocols::leios_notify::{self, Message};
use net_core::store::leios_store::LeiosStore;
use net_core::types::{Point, Vote, WrappedHeader};

struct Connection {
    send: CodecSend,
    recv: CodecRecv,
    server: tokio::task::JoinHandle<()>,
    client_mux: RunningMux,
    server_mux: RunningMux,
}

impl Connection {
    async fn open(store: Arc<LeiosStore>, peer: PeerId) -> Self {
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
        let server = tokio::spawn(serve_leios_notify(
            CodecSend::new(server_send),
            CodecRecv::new(server_recv),
            store,
            peer,
            None,
        ));
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
