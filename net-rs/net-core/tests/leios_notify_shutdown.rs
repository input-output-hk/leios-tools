use bytes::Bytes;
use net_core::bearer::{mem::MemBearer, Bearer};
use net_core::mux::scheduler::{PriorityWfq, TrafficClass};
use net_core::mux::{
    wire, CodecRecv, CodecSend, Mux, MuxConfig, ProtocolConfig, MODE_INITIATOR, MODE_RESPONDER,
};
use net_core::peer::{server_handlers::serve_leios_notify, PeerId};
use net_core::store::leios_store::LeiosStore;
use std::time::Duration;

fn protocol(id: u16, traffic_class: TrafficClass) -> ProtocolConfig {
    ProtocolConfig {
        id,
        traffic_class,
        ingress_limit: 65_536,
        egress_queue_size: 256,
    }
}

async fn check_quit(requests: usize) {
    // Keep the writer blocked until Quit has been handled. Both priority
    // protocols have output staged before the notification responder exits.
    let (client, server) = MemBearer::pair_with_buffer(64);
    let (mut reader, mut writer) = client.split();
    let mut mux = Mux::new(MuxConfig::default(), PriorityWfq::default(), MODE_RESPONDER);
    let (cs, _cr) = mux.register(&protocol(2, TrafficClass::Priority));
    let (ks, _kr) = mux.register(&protocol(8, TrafficClass::Priority));
    let (ns, nr) = mux.register(&protocol(18, TrafficClass::Default(1)));
    cs.send(Bytes::from(vec![0; 256])).await.unwrap();
    ks.send(Bytes::from(vec![0; 256])).await.unwrap();
    let running = mux.run(server);
    let (store, _watch) = LeiosStore::new(100);
    let responder = tokio::spawn(serve_leios_notify(
        CodecSend::new(ns),
        CodecRecv::new(nr),
        store,
        PeerId(1),
        None,
    ));
    tokio::time::sleep(Duration::from_millis(1)).await;
    // Send requests followed immediately by Quit.
    let mut payload = [0x81, 0x03].repeat(requests);
    payload.extend_from_slice(&[0x81, 0x01]);
    for segment in wire::segment_payload(18, MODE_INITIATOR, 0, &payload, 12288) {
        wire::write_segment(&mut writer, &segment).await.unwrap();
    }
    tokio::time::timeout(Duration::from_secs(1), responder)
        .await
        .unwrap()
        .unwrap();
    let first = wire::read_segment(&mut reader, 12288).await.unwrap();
    let second = wire::read_segment(&mut reader, 12288).await.unwrap();
    assert!(first.header.protocol != 18 && second.header.protocol != 18);
    let replies = tokio::time::timeout(Duration::from_secs(1), async {
        for _ in 0..requests {
            let reply = wire::read_segment(&mut reader, 12288).await.unwrap();
            assert_eq!(reply.header.protocol, 18);
            assert_eq!(reply.payload.as_ref(), &[0x81, 0x02]);
        }
        let reply = wire::read_segment(&mut reader, 12288).await.unwrap();
        assert_eq!(reply.header.protocol, 18);
        assert_eq!(reply.payload.as_ref(), &[0x81, 0x00]);
    })
    .await;
    running.abort();
    replies.expect("shutdown replies were dropped after responder returned");
}

#[tokio::test(start_paused = true)]
async fn quit_ack_is_preserved_while_other_protocols_have_pending_output() {
    check_quit(0).await;
}

#[tokio::test(start_paused = true)]
async fn cancellations_and_ack_are_preserved_with_other_protocol_output() {
    check_quit(100).await;
}
