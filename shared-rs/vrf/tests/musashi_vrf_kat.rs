//! Cross-implementation known-answer test against a real musashi (Leios era-8)
//! block header, using the epoch nonce our node actually forges with.
//!
//! This is the decisive check that our Praos VRF leader-election path is
//! byte-compatible with the live testnet: if a *real* block's VRF proof
//! verifies under our `mk_vrf_input` construction and the epoch nonce we pull
//! from Kleioscan, then our nonce, our VRF input, and our verify are all
//! correct — and production uses the same code, so our forged blocks' VRF is
//! correct too. A wrong nonce or input construction would make this fail.

use shared_vrf::praos::{leader_value, mk_vrf_input};
use shared_vrf::verify;

fn arr<const N: usize>(h: &str) -> [u8; N] {
    hex::decode(h).unwrap().try_into().unwrap()
}

/// Real musashi block: slot 2,453,577, block_no 83600, era 8 (epoch 28).
/// Fetched via `net-cli block-fetch`; VRF fields are header_body[4] (vrf_vkey)
/// and header_body[5] = [output(64), proof(80)].
#[test]
fn musashi_epoch28_block_vrf_verifies_with_kleioscan_nonce() {
    let slot: u64 = 2_453_577;
    let vrf_vkey: [u8; 32] =
        arr("4f94c32ce5d5e422cfae9815d74212987db81998b47c27e54892136a4ab4eaaa");
    let vrf_output: [u8; 64] = arr(
        "179337025cb26b5c97bf478cce788f5250a3f062b44dcfb4b86ae8c1fea393ef\
         82973cb7f6b029bdb864aa6d3495651a951aca6cd7a20d235cec7cf1cc441e97",
    );
    let vrf_proof: [u8; 80] = arr(
        "2cbfcea78cf250b04aeb6aea6ec58a20805a61b46fd794d3e164b1bd655c93a5\
         afca786a491ab995ab64740b9814b93a205a8ae8466cf1916c71eba8d3ed3254\
         d3997476b68d0e30f70feab92af0f907",
    );
    // Epoch-28 nonce from GET /musashi/epoch-nonce?epoch=28 — the same value
    // our node logs as "resolved epoch nonce" and forges with.
    let epoch_nonce: [u8; 32] =
        arr("ff1647ac49ac94c388e5d6db8becb69d45797d26004091c65105ff60ed171826");

    // Our VRF input construction, over our nonce.
    let alpha = mk_vrf_input(slot, &epoch_nonce);

    // The real block's proof MUST verify — this is the byte-compat assertion.
    let out = verify(&vrf_vkey, &alpha, &vrf_proof).expect(
        "real musashi block VRF proof must verify under our mk_vrf_input + epoch nonce; \
         a failure means our nonce or VRF input construction diverges from the network",
    );

    // And the recovered output must match the one committed in the header.
    assert_eq!(
        out, vrf_output,
        "verified VRF output must equal the block's vrf_output"
    );

    // Sanity: the leader-value derivation runs (this block's producer was a
    // real slot leader, so its leader_value is below its stake threshold).
    let lv = leader_value(&out);
    eprintln!("musashi epoch-28 leader_value = {}", hex::encode(lv));
}
