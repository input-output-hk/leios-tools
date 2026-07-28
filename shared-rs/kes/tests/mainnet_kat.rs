//! Cross-implementation known-answer test against a real cardano-node block
//! header (mainnet Praos block 7854823, the same fixture the VRF crate uses).
//!
//! This proves — through our wrapper, over real node-produced bytes — that:
//!   1. our [`ocert`] `OCertSignable` layout matches cardano-protocol's, so a
//!      real stake pool's cold-key signature over its op-cert verifies; and
//!   2. our [`verify`] accepts a real Sum6 KES signature over the serialized
//!      header body at the relative KES period.
//!
//! Wire-compatibility with the real node — not just self-consistency — is the
//! whole point of adopting a third-party KES crate.

use minicbor::Decoder;
use shared_kes::{ocert, verify, SIG_SIZE};

/// Raw header CBOR: `[header_body, body_signature]` (hex text).
const HEADER_HEX: &str = include_str!("vectors/mainnet-praos-7854823.cbor");
/// Mainnet Shelley genesis `slotsPerKESPeriod`.
const MAINNET_SLOTS_PER_KES_PERIOD: u64 = 129_600;

fn arr<const N: usize>(b: &[u8]) -> [u8; N] {
    b.try_into()
        .unwrap_or_else(|_| panic!("expected {N} bytes, got {}", b.len()))
}

#[test]
fn mainnet_header_ocert_and_kes_verify() {
    let raw = hex::decode(HEADER_HEX.trim()).expect("fixture is valid hex");

    // Outer: [header_body, body_signature]. Capture the exact header_body
    // bytes (what KES signs) by skipping over it and slicing.
    let mut d = Decoder::new(&raw);
    assert_eq!(d.array().expect("outer array"), Some(2));
    let hb_start = d.position();
    d.skip().expect("skip header_body");
    let hb_end = d.position();
    let header_body = &raw[hb_start..hb_end];
    let body_sig: [u8; SIG_SIZE] = arr(d.bytes().expect("body_signature bytes"));

    // header_body = array(10): the 8 base fields, then nested operational_cert
    // (array 4) and protocol_version (array 2).
    let mut h = Decoder::new(header_body);
    assert_eq!(h.array().expect("header_body array"), Some(10));
    let block_number = h.u64().expect("block_number");
    let slot = h.u64().expect("slot");
    h.skip().expect("prev_hash");
    let issuer_vkey: [u8; 32] = arr(h.bytes().expect("issuer_vkey"));
    let _vrf_vkey = h.bytes().expect("vrf_vkey");
    h.skip().expect("vrf_result");
    let _body_size = h.u64().expect("block_body_size");
    let _body_hash = h.bytes().expect("block_body_hash");
    assert_eq!(h.array().expect("operational_cert array"), Some(4));
    let hot_vkey: [u8; 32] = arr(h.bytes().expect("hot_vkey"));
    let counter = h.u64().expect("sequence_number");
    let ocert_kes_period = h.u64().expect("kes_period");
    let sigma: [u8; 64] = arr(h.bytes().expect("sigma"));

    assert_eq!(block_number, 7_854_823, "fixture provenance");

    // (1) The real cold key's op-cert signature verifies — our OCertSignable
    // (hot_vkey ‖ counter_be64 ‖ kes_period_be64) matches cardano's.
    ocert::verify_ocert(&issuer_vkey, &hot_vkey, counter, ocert_kes_period, &sigma)
        .expect("real mainnet op-cert cold signature must verify");

    // (2) The real Sum6 KES body signature verifies at the relative period
    // t = current_kes_period − ocert_kes_period.
    let current_kes_period = ocert::kes_period(slot, MAINNET_SLOTS_PER_KES_PERIOD);
    let t = u32::try_from(current_kes_period - ocert_kes_period).expect("t within u32");
    verify(t, &hot_vkey, header_body, &body_sig)
        .expect("real mainnet KES body signature must verify");
}
