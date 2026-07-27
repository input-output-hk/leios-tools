//! Real-node interop KAT: a genuine Cardano **mainnet** Praos block header,
//! verified through this crate's primitive.
//!
//! The primitive's own known-answer vectors (in `src/lib.rs`) are the
//! libsodium draft-03 test vectors — they prove the raw VRF is wire-correct
//! but say nothing about how Cardano *builds* the VRF input from
//! `(slot, epoch_nonce)` or *derives* the leader value from the output.
//! Those two Cardano-specific layers are where a "wrong but self-consistent"
//! implementation hides, and they are only provable against data a real node
//! produced — not against our own cluster.
//!
//! This test closes that for the **verify** side. It takes mainnet block
//! **7854823** (epoch 368, Babbage/Praos), reconstructs the Praos VRF input
//! `alpha = blake2b256(be64(slot) ‖ epoch_nonce)`, and checks that our
//! `verify` accepts the header's real proof and returns its real output. A
//! wrong `alpha` construction would change `hash_to_curve` and make the real
//! proof fail — so this simultaneously validates the input construction.
//!
//! Provenance: header CBOR and context (epoch nonce, slot) are from
//! input-output-hk/acropolis `block_vrf_validator` test `test_7854823_block`
//! (Apache-2.0); the raw VRF fields below were extracted from the header CBOR
//! in `tests/vectors/mainnet-praos-7854823.cbor`.
//!
//! The Praos input and leader-value constructions are reproduced here as
//! *test-local reference helpers* — this crate stays a pure primitive; the
//! real home for them is the Cardano-aware layer (see the block-production
//! plan, item 5).

use shared_vrf::{verify, Proof, PublicKey};

/// Absolute slot of mainnet block 7854823.
const SLOT: u64 = 73_614_529;
/// Epoch-368 nonce (Acropolis `test_7854823_block`).
const EPOCH_NONCE_HEX: &str = "8dad163edf4607452fec9c5955d593fb598ca728bae162138f88da6667bba79b";
/// VRF verification key, extracted from the header (`header_body[4]`).
const VRF_VKEY_HEX: &str = "b5b6a6b790e6926f6ba856255aaf3eb5192cc80c03eeec7469cd3f64344bafb7";
/// VRF output, extracted from the header's `vrf_result` (`header_body[5][0]`).
const VRF_OUTPUT_HEX: &str = "3f42ef44b8525ee45ca45a094191f00b359e9205a400b0607c87fe488f3b44442afc9e7c3e28ded9080c71d84eb7840ecb2e7240721ee243b45a761048ad6c53";
/// VRF proof, extracted from the header's `vrf_result` (`header_body[5][1]`).
const VRF_PROOF_HEX: &str = "54ac3b2bc94b54705b951146a83cba92041fdf537644628c64e9549365e74c10502fb7c23f34d7daa8327843f728513b225044a9390f86fb2a83951c4a39a13c17472c71cf50d74f1ce12d04dc85e902";

/// Reference: the Praos VRF input (cardano-base / Acropolis `mk_vrf_input`).
/// `alpha = blake2b256(be64(slot) ‖ epoch_nonce)` — single call, **no** input
/// domain tag (that is the older TPraos scheme).
fn praos_vrf_input(slot: u64, epoch_nonce: &[u8; 32]) -> [u8; 32] {
    let mut data = Vec::with_capacity(8 + 32);
    data.extend_from_slice(&slot.to_be_bytes());
    data.extend_from_slice(epoch_nonce);
    let h = blake2b_simd::Params::new().hash_length(32).hash(&data);
    h.as_bytes().try_into().expect("32-byte blake2b")
}

/// Reference: leader value = blake2b256(0x4C ‖ vrf_output) — the "L" tag from
/// pallas-primitives `derive_tagged_vrf_output`.
fn leader_value(vrf_output: &[u8]) -> [u8; 32] {
    let mut data = Vec::with_capacity(1 + vrf_output.len());
    data.push(0x4C); // "L"
    data.extend_from_slice(vrf_output);
    let h = blake2b_simd::Params::new().hash_length(32).hash(&data);
    h.as_bytes().try_into().expect("32-byte blake2b")
}

fn arr<const N: usize>(hex_str: &str) -> [u8; N] {
    hex::decode(hex_str)
        .expect("valid hex")
        .try_into()
        .unwrap_or_else(|v: Vec<u8>| panic!("expected {N} bytes, got {}", v.len()))
}

#[test]
fn mainnet_praos_header_verifies() {
    let epoch_nonce: [u8; 32] = arr(EPOCH_NONCE_HEX);
    let vkey: PublicKey = arr(VRF_VKEY_HEX);
    let proof: Proof = arr(VRF_PROOF_HEX);
    let expected_output: [u8; 64] = arr(VRF_OUTPUT_HEX);

    // Reconstruct the Praos VRF input the real node used, and verify the
    // real header's proof against it through our primitive.
    let alpha = praos_vrf_input(SLOT, &epoch_nonce);
    let output = verify(&vkey, &alpha, &proof)
        .expect("mainnet Praos proof must verify (input construction + primitive)");

    // The verifier's output must equal the output committed in the header.
    assert_eq!(
        output.as_slice(),
        expected_output.as_slice(),
        "verified VRF output must match the header's vrf_result output"
    );

    // The leader-value derivation is deterministic from the output; assert it
    // is stable (a fixed known answer) so a change to the tag/derivation is
    // caught. This is the value the (still-to-be-reconciled) threshold compare
    // consumes — see the block-production plan.
    let lv = leader_value(&output);
    assert_eq!(
        hex::encode(lv),
        "0002ec470f640cb176dad080df96783290cceaa84211660544118539e25c3339",
        "leader value derivation changed"
    );
    // Sanity: this block *won* its slot, so the leader value is small
    // (leading zero bytes) — it sat below the leadership threshold.
    assert!(
        lv[0] == 0x00 && lv[1] == 0x02,
        "winning block has a small leader value"
    );
}

/// A wrong VRF input (e.g. the TPraos-style construction, or a bad byte
/// layout) must make the real proof fail — the property that makes the test
/// above a real check of the input construction, not just of `verify`.
#[test]
fn wrong_input_construction_rejects_mainnet_proof() {
    let epoch_nonce: [u8; 32] = arr(EPOCH_NONCE_HEX);
    let vkey: PublicKey = arr(VRF_VKEY_HEX);
    let proof: Proof = arr(VRF_PROOF_HEX);

    // Little-endian slot instead of big-endian — a plausible wrong layout.
    let mut data = Vec::new();
    data.extend_from_slice(&SLOT.to_le_bytes());
    data.extend_from_slice(&epoch_nonce);
    let wrong_alpha: [u8; 32] = blake2b_simd::Params::new()
        .hash_length(32)
        .hash(&data)
        .as_bytes()
        .try_into()
        .unwrap();

    assert!(
        verify(&vkey, &wrong_alpha, &proof).is_err(),
        "a wrong input construction must not verify the real proof"
    );
}
