//! Cardano Praos leader-election glue on top of the raw draft-03 VRF.
//!
//! The crate root (`lib.rs`) is a pure VRF primitive: prove/verify over
//! arbitrary bytes. This module adds the three Cardano-specific pieces the
//! block-production leader lottery needs, all of which must match the real
//! node byte-for-byte:
//!
//! 1. [`mk_vrf_input`] — build the VRF input (`alpha`) from `(slot, nonce)`.
//! 2. [`leader_value`] — derive the leader value from the VRF output.
//! 3. [`is_slot_leader`] — the stake-weighted leadership threshold check.
//!
//! Proving is the node's job (it holds the VRF secret key); this module is
//! key-free. The threshold uses the *same* fixed-point Taylor comparison as
//! cardano-base — via `pallas-math`, pinned to the version Acropolis
//! validates the real chain with — rather than the f64 approximation in
//! `shared_consensus::lottery`, so the win/lose boundary matches a real
//! validator exactly. See the block-production plan, item 5.

use blake2b_simd::Params as Blake2b;
use dashu_int::UBig;
use pallas_math::math::{ExpOrdering, FixedDecimal, FixedPrecision};

/// Domain-separation tag for the leader VRF derivation (`"L"`).
/// (The nonce derivation uses `"N"` = `0x4E`, which we do not need here.)
const LEADER_TAG: u8 = 0x4C;

fn blake2b256(parts: &[&[u8]]) -> [u8; 32] {
    let mut state = Blake2b::new().hash_length(32).to_state();
    for p in parts {
        state.update(p);
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(state.finalize().as_bytes());
    out
}

/// Build the Praos VRF input for a slot:
/// `alpha = blake2b256( be64(slot) ‖ epoch_nonce )`.
///
/// This is the current (Babbage+) **Praos** construction: a single VRF call
/// with **no** input domain tag. It is *not* the older TPraos scheme (two
/// calls with the input XOR-tagged by `seed_l`/`seed_eta`), which is what the
/// `pallas_vrf` README pseudocode shows — following that would produce
/// proofs no Praos node accepts. Mirrors Acropolis `mk_vrf_input`.
pub fn mk_vrf_input(slot: u64, epoch_nonce: &[u8; 32]) -> [u8; 32] {
    blake2b256(&[&slot.to_be_bytes(), epoch_nonce])
}

/// Derive the leader value from a VRF output:
/// `leader_value = blake2b256( 0x4C ‖ vrf_output )`.
///
/// This is the "L"-tagged derivation from cardano-base /
/// `pallas-primitives::derive_tagged_vrf_output`, inlined (four spec-fixed
/// lines) rather than pulling `pallas-primitives`. The result is the 32-byte
/// certified natural fed to [`is_slot_leader`].
pub fn leader_value(vrf_output: &[u8]) -> [u8; 32] {
    blake2b256(&[&[LEADER_TAG], vrf_output])
}

/// Decide slot leadership: is this pool a leader for the slot whose VRF
/// produced `leader_value`?
///
/// Checks `p < 1 − (1 − f)^σ`, where `p = certNat / 2^256`
/// (`certNat` = `leader_value` as a big-endian natural), `σ = stake /
/// total_stake`, and `f = f_num / f_den` is the active-slot coefficient.
/// Rearranged as cardano-base does to avoid the transcendental:
/// `1/q < exp(−σ·ln(1−f))` with `q = 1 − p`, evaluated with the
/// fixed-point Taylor comparison `exp_cmp` (identical parameters to
/// Acropolis `validate_vrf_leader_value`, so the boundary matches the real
/// node). Returns `false` for zero stake or zero total stake.
pub fn is_slot_leader(
    leader_value: &[u8; 32],
    stake: u64,
    total_stake: u64,
    f_num: u64,
    f_den: u64,
) -> bool {
    if stake == 0 || total_stake == 0 {
        return false;
    }
    // certNat and certNatMax = 2^(len*8) = 2^256 for the 32-byte Praos value.
    let certified_leader_vrf = &FixedDecimal::from(&leader_value[..]);
    let output_size_bits = leader_value.len() * 8;
    let cert_nat_max = FixedDecimal::from(UBig::ONE << output_size_bits);

    let sigma = FixedDecimal::from(UBig::from(stake)) / FixedDecimal::from(UBig::from(total_stake));
    let f = FixedDecimal::from(UBig::from(f_num)) / FixedDecimal::from(UBig::from(f_den));

    let denominator = &cert_nat_max - certified_leader_vrf;
    let recip_q = &cert_nat_max / &denominator;
    let c = (&FixedDecimal::from(1u64) - &f).ln();
    let x = -(sigma * c);
    matches!(x.exp_cmp(1000, 3, &recip_q).estimation, ExpOrdering::LT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{verify, Proof, PublicKey};

    // Mainnet block 7854823 (epoch 368, Babbage/Praos) — the real-node
    // vector from Acropolis `test_7854823_block`. VRF fields were extracted
    // from the header CBOR retained at `tests/vectors/mainnet-praos-7854823.cbor`;
    // stake/total from the test's SPDD; f = 1/20. Acropolis asserts this
    // block validates as a leader, so `is_slot_leader` here must agree — a
    // real-node check of the whole leader path (input → verify → leader
    // value → threshold).
    const SLOT: u64 = 73_614_529;
    const NONCE: &str = "8dad163edf4607452fec9c5955d593fb598ca728bae162138f88da6667bba79b";
    const VKEY: &str = "b5b6a6b790e6926f6ba856255aaf3eb5192cc80c03eeec7469cd3f64344bafb7";
    const OUTPUT: &str = "3f42ef44b8525ee45ca45a094191f00b359e9205a400b0607c87fe488f3b44442afc9e7c3e28ded9080c71d84eb7840ecb2e7240721ee243b45a761048ad6c53";
    const PROOF: &str = "54ac3b2bc94b54705b951146a83cba92041fdf537644628c64e9549365e74c10502fb7c23f34d7daa8327843f728513b225044a9390f86fb2a83951c4a39a13c17472c71cf50d74f1ce12d04dc85e902";
    const POOL_STAKE: u64 = 64_590_523_391_239;
    const TOTAL_STAKE: u64 = 25_069_171_797_357_766;

    fn arr<const N: usize>(h: &str) -> [u8; N] {
        hex::decode(h).unwrap().try_into().unwrap()
    }

    #[test]
    fn mk_vrf_input_reconstructs_the_verified_alpha() {
        // The input we build must be the one the real header's proof was
        // signed over: feeding it to the primitive `verify` must accept the
        // real proof and return the real output.
        let nonce: [u8; 32] = arr(NONCE);
        let vkey: PublicKey = arr(VKEY);
        let proof: Proof = arr(PROOF);
        let alpha = mk_vrf_input(SLOT, &nonce);
        let out = verify(&vkey, &alpha, &proof).expect("mainnet proof verifies via mk_vrf_input");
        assert_eq!(hex::encode(out), OUTPUT);
    }

    #[test]
    fn leader_value_matches_real_header() {
        let out: [u8; 64] = arr(OUTPUT);
        assert_eq!(
            hex::encode(leader_value(&out)),
            "0002ec470f640cb176dad080df96783290cceaa84211660544118539e25c3339"
        );
    }

    #[test]
    fn mainnet_block_is_a_leader() {
        // Acropolis validates this block as a leader; we must agree.
        let out: [u8; 64] = arr(OUTPUT);
        let lv = leader_value(&out);
        assert!(is_slot_leader(&lv, POOL_STAKE, TOTAL_STAKE, 1, 20));
    }

    #[test]
    fn wrong_input_layout_rejects_real_proof() {
        // A wrong VRF-input layout (little-endian slot instead of big-endian)
        // must fail to verify the real header's proof. This is what makes
        // `mk_vrf_input`'s exact byte layout load-bearing rather than
        // incidental — a self-consistent-but-wrong input is silent until it
        // meets a real proof.
        let nonce: [u8; 32] = arr(NONCE);
        let vkey: PublicKey = arr(VKEY);
        let proof: Proof = arr(PROOF);
        let wrong = blake2b256(&[&SLOT.to_le_bytes(), &nonce]);
        assert!(verify(&vkey, &wrong, &proof).is_err());
    }

    #[test]
    fn huge_leader_value_is_not_a_leader() {
        // certNat ≈ certNatMax → p ≈ 1, far above any realistic threshold.
        let lv = [0xffu8; 32];
        assert!(!is_slot_leader(&lv, POOL_STAKE, TOTAL_STAKE, 1, 20));
    }

    #[test]
    fn zero_stake_is_not_a_leader() {
        let out: [u8; 64] = arr(OUTPUT);
        let lv = leader_value(&out);
        assert!(!is_slot_leader(&lv, 0, TOTAL_STAKE, 1, 20));
        assert!(!is_slot_leader(&lv, POOL_STAKE, 0, 1, 20));
    }
}
