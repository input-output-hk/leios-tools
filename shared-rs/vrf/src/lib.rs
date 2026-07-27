//! Cardano Praos draft-03 VRF — the prover + verifier the Leios
//! block-production leader lottery needs.
//!
//! This is a thin, **byte-oriented** wrapper over `pallas_vrf`
//! (`github.com/txpipe/vrf`): draft-irtf-cfrg-vrf-03,
//! ECVRF-ED25519-SHA512-Elligator2 — the exact VRF cardano-base FFIs from
//! libsodium for Praos leader election. `pallas_vrf` exposes forked
//! `curve25519-dalek` types (`CompressedEdwardsY`, `Scalar`) in its key and
//! proof structs; this wrapper keeps them out of the public surface so
//! consumers (the node's lottery draw seam, `shared-consensus`) depend only
//! on fixed-size arrays matching cardano-base's wire formats:
//!
//! - 32-byte secret seed and 32-byte verification key,
//! - 80-byte proof,
//! - 64-byte VRF output.
//!
//! For Praos leader election the VRF input (`alpha`) is the epoch nonce
//! concatenated with the slot (and the domain-separation tag); this crate
//! is agnostic to how `alpha` is built and just evaluates over the bytes.
//!
//! Dependency choice (git-pin vs vendor) is recorded in the block-production
//! plan under "VRF dependency — pin vs vendor".

use std::fmt;

use pallas_vrf::vrf03::{PublicKey03, SecretKey03, VrfProof03};

/// Byte size of the secret seed.
pub const SEED_SIZE: usize = 32;
/// Byte size of the verification (public) key.
pub const PUBLIC_KEY_SIZE: usize = 32;
/// Byte size of the proof.
pub const PROOF_SIZE: usize = 80;
/// Byte size of the VRF output (the hash the leader value is derived from).
pub const OUTPUT_SIZE: usize = 64;

/// A 32-byte VRF secret seed.
pub type Seed = [u8; SEED_SIZE];
/// A 32-byte VRF verification key.
pub type PublicKey = [u8; PUBLIC_KEY_SIZE];
/// An 80-byte VRF proof.
pub type Proof = [u8; PROOF_SIZE];
/// A 64-byte VRF output.
pub type Output = [u8; OUTPUT_SIZE];

/// A VRF proof failed to decode or verify. The wrapper deliberately does
/// not distinguish decode from verification failure: for leader election a
/// malformed and an invalid proof are both simply "not a valid win".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VrfError;

impl fmt::Display for VrfError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VRF proof failed to decode or verify")
    }
}

impl std::error::Error for VrfError {}

/// Derive the 32-byte verification key from a 32-byte secret seed.
pub fn public_key(seed: &Seed) -> PublicKey {
    let sk = SecretKey03::from_bytes(seed);
    PublicKey03::from(&sk).to_bytes()
}

/// Evaluate the VRF over `alpha` with `seed`, returning the 80-byte proof
/// and the 64-byte output. This is the prover: only the holder of `seed`
/// can produce it, and anyone with the verification key can check it via
/// [`verify`].
pub fn prove(seed: &Seed, alpha: &[u8]) -> (Proof, Output) {
    let sk = SecretKey03::from_bytes(seed);
    let pk = PublicKey03::from(&sk);
    let proof = VrfProof03::generate(&pk, &sk, alpha);
    (proof.to_bytes(), proof.proof_to_hash())
}

/// Verify a proof against `public_key` and `alpha`, returning the 64-byte
/// output on success. Fails with [`VrfError`] if the proof does not decode
/// or does not verify.
pub fn verify(public_key: &PublicKey, alpha: &[u8], proof: &Proof) -> Result<Output, VrfError> {
    let pk = PublicKey03::from_bytes(public_key);
    let proof = VrfProof03::from_bytes(proof).map_err(|_| VrfError)?;
    proof.verify(&pk, alpha).map_err(|_| VrfError)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arr<const N: usize>(hex_str: &str) -> [u8; N] {
        let v = hex::decode(hex_str).expect("valid hex");
        v.try_into()
            .unwrap_or_else(|v: Vec<u8>| panic!("expected {N} bytes, got {}", v.len()))
    }

    /// prove → verify round-trips, and the output the prover returns
    /// matches the output the verifier recomputes.
    #[test]
    fn prove_verify_round_trip() {
        let seed: Seed = [7u8; SEED_SIZE];
        let alpha = b"leios/praos/leader-election";
        let (proof, output) = prove(&seed, alpha);
        let vk = public_key(&seed);
        let verified = verify(&vk, alpha, &proof).expect("proof verifies");
        assert_eq!(verified, output, "verifier output must match prover output");
    }

    /// A proof for one alpha must not verify against a different alpha.
    #[test]
    fn wrong_alpha_fails_verification() {
        let seed: Seed = [7u8; SEED_SIZE];
        let (proof, _) = prove(&seed, b"alpha-one");
        let vk = public_key(&seed);
        assert_eq!(verify(&vk, b"alpha-two", &proof), Err(VrfError));
    }

    /// A tampered proof does not verify.
    #[test]
    fn tampered_proof_fails_verification() {
        let seed: Seed = [9u8; SEED_SIZE];
        let alpha = b"payload";
        let (mut proof, _) = prove(&seed, alpha);
        proof[0] ^= 0xff;
        let vk = public_key(&seed);
        assert!(verify(&vk, alpha, &proof).is_err());
    }

    /// Known-answer vectors from the draft-03 libsodium suite
    /// (input-output-hk/libsodium `test/default/vrf.c`), the same vectors
    /// cardano-base checks against. Proving wire-compatibility *through our
    /// wrapper* — not just self-consistency — is the whole point of pinning
    /// a third-party crate whose prover Acropolis never exercises.
    ///
    /// Columns: (secret seed, verification key, proof, output, alpha).
    #[test]
    fn cardano_base_known_answer_vectors() {
        let vectors: &[(&str, &str, &str, &str, &str)] = &[
            // Standard draft-03 vector 1: empty message.
            (
                "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60",
                "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a",
                "b6b4699f87d56126c9117a7da55bd0085246f4c56dbc95d20172612e9d38e8d7ca65e573a126ed88d4e30a46f80a666854d675cf3ba81de0de043c3774f061560f55edc256a787afe701677c0f602900",
                "5b49b554d05c0cd5a5325376b3387de59d924fd1e13ded44648ab33c21349a603f25b84ec5ed887995b33da5e3bfcb87cd2f64521c4c62cf825cffabbe5d31cc",
                "",
            ),
            // Standard draft-03 vector 3: alpha = 0xaf82.
            (
                "c5aa8df43f9f837bedb7442f31dcb7b166d38535076f094b85ce3a2e0b4458f7",
                "fc51cd8e6218a1a38da47ed00230f0580816ed13ba3303ac5deb911548908025",
                "dfa2cba34b611cc8c833a6ea83b8eb1bb5e2ef2dd1b0c481bc42ff36ae7847f6ab52b976cfd5def172fa412defde270c8b8bdfbaae1c7ece17d9833b1bcf31064fff78ef493f820055b561ece45e1009",
                "2031837f582cd17a9af9e0c7ef5a6540e3453ed894b62c293686ca3c1e319dde9d0aa489a4b59a9594fc2328bc3deff3c8a0929a369a72b1180a596e016b5ded",
                "af82",
            ),
        ];

        for (i, (seed_hex, vk_hex, proof_hex, output_hex, alpha_hex)) in vectors.iter().enumerate()
        {
            let seed: Seed = arr(seed_hex);
            let expected_vk: PublicKey = arr(vk_hex);
            let expected_proof: Proof = arr(proof_hex);
            let expected_output: Output = arr(output_hex);
            let alpha = hex::decode(alpha_hex).expect("valid hex");

            // Key derivation matches cardano-base.
            assert_eq!(public_key(&seed), expected_vk, "vector {i}: vkey");

            // The prover reproduces the exact wire proof and output.
            let (proof, output) = prove(&seed, &alpha);
            assert_eq!(proof, expected_proof, "vector {i}: proof bytes");
            assert_eq!(output, expected_output, "vector {i}: output");

            // The verifier accepts the canonical proof and returns the output.
            let verified = verify(&expected_vk, &alpha, &expected_proof)
                .unwrap_or_else(|_| panic!("vector {i}: canonical proof must verify"));
            assert_eq!(verified, expected_output, "vector {i}: verified output");
        }
    }
}
