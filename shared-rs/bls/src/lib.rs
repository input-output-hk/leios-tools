//! Cardano Leios BLS — votes and (later) the CIP-0164 certificate.
//!
//! A byte-oriented wrapper over IOG's `leios_crypto_benchmarks` (the Leios
//! crypto reference in `ouroboros-leios/crypto-benchmarks.rs`): **BLS12-381,
//! minimal-signature-size** — signatures in G1 (48 bytes), verification keys
//! in G2 (96 bytes), secret key a 32-byte scalar — over `blst`, the same C
//! curve library the Haskell `cardano-node` uses. Adopting the reference keeps
//! our votes and certificates byte-compatible with the real node.
//!
//! The Leios construction (from the reference's `bls_vote.rs`): every
//! signature uses domain-separation tag `DST = b"Leios"`, with the election id
//! — the 8-byte big-endian slot — passed as blst's *augmentation*, so a vote
//! over endorser-block hash `eb` at slot `s` is `sign(eb, DST, aug = s_be)`.
//!
//! Key material comes from cardano-cli `bls.skey`/`bls.vkey` envelopes, whose
//! `cborHex` payload is a CBOR byte string of the raw key
//! ([`secret_key_from_cardano`] / [`public_key_from_cardano`]).

use std::fmt;

use blst::min_sig::{PublicKey, SecretKey, Signature};
use leios_crypto_benchmarks::bls_vote::{check_pop, gen_sig, make_pop, verify_sig};

/// Byte size of a BLS secret key (a scalar).
pub const SECRET_KEY_SIZE: usize = 32;
/// Byte size of a compressed G2 verification key.
pub const PUBLIC_KEY_SIZE: usize = 96;
/// Byte size of a compressed G1 signature (a single vote or the aggregate).
pub const SIGNATURE_SIZE: usize = 48;

/// The election id a vote is bound to: the 8-byte big-endian slot, passed as
/// the blst augmentation. (Reference: `primitive::Eid::bytes`.)
fn eid_bytes(slot: u64) -> [u8; 8] {
    slot.to_be_bytes()
}

/// A BLS operation failed: a key/signature did not decode, or a check failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlsError(String);

impl fmt::Display for BlsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BLS error: {}", self.0)
    }
}

impl std::error::Error for BlsError {}

/// Strip the fixed CBOR byte-string header from a cardano key envelope's
/// `cborHex` payload, returning the raw `len`-byte key. These keys are always
/// encoded as `0x58 <len> <data>` (a one-byte-length CBOR byte string).
fn strip_cbor_bstr(cbor: &[u8], len: usize) -> Result<&[u8], BlsError> {
    if cbor.len() == len + 2 && cbor[0] == 0x58 && cbor[1] as usize == len {
        Ok(&cbor[2..])
    } else {
        Err(BlsError(format!(
            "expected a CBOR byte string of {len} bytes (0x58{len:02x}…)"
        )))
    }
}

/// Load a secret key from a cardano `bls.skey` `cborHex` payload (a CBOR byte
/// string of the 32-byte scalar).
pub fn secret_key_from_cardano(cbor: &[u8]) -> Result<SecretKey, BlsError> {
    let raw = strip_cbor_bstr(cbor, SECRET_KEY_SIZE)?;
    SecretKey::from_bytes(raw).map_err(|e| BlsError(format!("invalid bls secret key: {e:?}")))
}

/// Load a verification key from a cardano `bls.vkey` `cborHex` payload (a CBOR
/// byte string of the 96-byte compressed G2 point).
pub fn public_key_from_cardano(cbor: &[u8]) -> Result<PublicKey, BlsError> {
    let raw = strip_cbor_bstr(cbor, PUBLIC_KEY_SIZE)?;
    PublicKey::from_bytes(raw).map_err(|e| BlsError(format!("invalid bls public key: {e:?}")))
}

/// Load a verification key from its raw 96-byte compressed encoding.
pub fn public_key_from_bytes(bytes: &[u8; PUBLIC_KEY_SIZE]) -> Result<PublicKey, BlsError> {
    PublicKey::from_bytes(bytes).map_err(|e| BlsError(format!("invalid bls public key: {e:?}")))
}

/// Derive the verification key (compressed G2) for a secret key.
pub fn public_key_of(sk: &SecretKey) -> [u8; PUBLIC_KEY_SIZE] {
    sk.sk_to_pk().to_bytes()
}

/// Sign a Leios vote: the endorser-block hash `eb_hash` under `DST = b"Leios"`
/// augmented by the election id (`slot`). Returns the 48-byte G1 signature —
/// the `vote_signature` on the wire (reference `bls_vote::gen_sig`).
pub fn sign_vote(sk: &SecretKey, eb_hash: &[u8; 32], slot: u64) -> [u8; SIGNATURE_SIZE] {
    gen_sig(sk, &eid_bytes(slot), eb_hash).to_bytes()
}

/// Verify a single Leios vote signature against a voter's verification key.
pub fn verify_vote(
    pk: &PublicKey,
    eb_hash: &[u8; 32],
    slot: u64,
    sig: &[u8; SIGNATURE_SIZE],
) -> Result<bool, BlsError> {
    let s =
        Signature::from_bytes(sig).map_err(|e| BlsError(format!("invalid signature: {e:?}")))?;
    Ok(verify_sig(pk, &eid_bytes(slot), eb_hash, &s))
}

/// Verify a single Leios vote from a voter's raw 96-byte verification key —
/// the byte-oriented form a node uses to check inbound votes against the
/// committee's registered `bls_key`s. Returns `Ok(false)` for a valid-but-
/// wrong signature, `Err` if the key or signature does not decode.
pub fn verify_vote_bytes(
    pubkey: &[u8; PUBLIC_KEY_SIZE],
    eb_hash: &[u8; 32],
    slot: u64,
    sig: &[u8; SIGNATURE_SIZE],
) -> Result<bool, BlsError> {
    let pk = public_key_from_bytes(pubkey)?;
    verify_vote(&pk, eb_hash, slot, sig)
}

/// Generate a proof of possession for a secret key: the two-signature PoP the
/// Leios registration uses (reference `bls_vote::make_pop`). Returns
/// `(mu1, mu2)` as 48-byte G1 signatures.
pub fn make_proof_of_possession(sk: &SecretKey) -> ([u8; SIGNATURE_SIZE], [u8; SIGNATURE_SIZE]) {
    let (mu1, mu2) = make_pop(sk);
    (mu1.to_bytes(), mu2.to_bytes())
}

/// Verify a proof of possession against a verification key.
pub fn verify_proof_of_possession(
    pk: &PublicKey,
    mu1: &[u8; SIGNATURE_SIZE],
    mu2: &[u8; SIGNATURE_SIZE],
) -> Result<bool, BlsError> {
    let s1 = Signature::from_bytes(mu1).map_err(|e| BlsError(format!("invalid pop mu1: {e:?}")))?;
    let s2 = Signature::from_bytes(mu2).map_err(|e| BlsError(format!("invalid pop mu2: {e:?}")))?;
    Ok(check_pop(pk, &s1, &s2))
}

/// A node's BLS vote signing key — an opaque owner of the secret scalar that
/// signs Leios votes and proofs of possession, keeping `blst` out of the
/// caller's types. Loaded from a cardano `bls.skey` envelope.
pub struct VoteSigner {
    sk: SecretKey,
}

impl VoteSigner {
    /// Load from a cardano `bls.skey` `cborHex` payload.
    pub fn from_cardano(cbor: &[u8]) -> Result<Self, BlsError> {
        Ok(Self {
            sk: secret_key_from_cardano(cbor)?,
        })
    }

    /// Deterministically generate a signer from 32 bytes of input key material
    /// (RFC-9380 `KeyGen`). For dev/test identities; real nodes load a
    /// registered `bls.skey` via [`from_cardano`](Self::from_cardano).
    pub fn generate(ikm: &[u8; 32]) -> Self {
        Self {
            sk: SecretKey::key_gen(ikm, &[]).expect("32-byte ikm yields a valid key"),
        }
    }

    /// The 96-byte compressed G2 verification key (the pool's `bls.vkey`).
    pub fn public_key(&self) -> [u8; PUBLIC_KEY_SIZE] {
        public_key_of(&self.sk)
    }

    /// Sign a vote for endorser block `eb_hash` at election `slot`.
    pub fn sign_vote(&self, eb_hash: &[u8; 32], slot: u64) -> [u8; SIGNATURE_SIZE] {
        sign_vote(&self.sk, eb_hash, slot)
    }

    /// Generate this key's proof of possession `(mu1, mu2)`.
    pub fn proof_of_possession(&self) -> ([u8; SIGNATURE_SIZE], [u8; SIGNATURE_SIZE]) {
        make_proof_of_possession(&self.sk)
    }
}

impl fmt::Debug for VoteSigner {
    /// Redacts the secret; shows only the public key prefix.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let pk = self.public_key();
        f.debug_struct("VoteSigner")
            .field("public_key", &hex_prefix(&pk))
            .finish_non_exhaustive()
    }
}

/// First 4 bytes of a key as hex, for redacted `Debug`.
fn hex_prefix(b: &[u8]) -> String {
    b.iter().take(4).map(|x| format!("{x:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hexd(s: &str) -> Vec<u8> {
        hex::decode(s).expect("valid hex")
    }

    /// A deterministic test key from 32 bytes of IKM.
    fn test_key(ikm: [u8; 32]) -> SecretKey {
        SecretKey::key_gen(&ikm, &[]).expect("key_gen")
    }

    /// Sign → verify round-trips; the same signature must not verify for a
    /// different slot or a different EB hash.
    #[test]
    fn vote_sign_verify_round_trip() {
        let sk = test_key([1u8; 32]);
        let pk = PublicKey::from_bytes(&public_key_of(&sk)).unwrap();
        let eb = [0xABu8; 32];
        let sig = sign_vote(&sk, &eb, 42);

        assert_eq!(verify_vote(&pk, &eb, 42, &sig), Ok(true));
        assert_eq!(verify_vote(&pk, &eb, 43, &sig), Ok(false), "wrong slot");
        assert_eq!(
            verify_vote(&pk, &[0xCDu8; 32], 42, &sig),
            Ok(false),
            "wrong EB hash"
        );
    }

    /// A vote must not verify against a different key.
    #[test]
    fn vote_rejects_wrong_key() {
        let sk = test_key([2u8; 32]);
        let other = PublicKey::from_bytes(&public_key_of(&test_key([3u8; 32]))).unwrap();
        let eb = [0x11u8; 32];
        let sig = sign_vote(&sk, &eb, 7);
        assert_eq!(verify_vote(&other, &eb, 7, &sig), Ok(false));
    }

    /// Proof of possession round-trips, and does not verify against a
    /// different key.
    #[test]
    fn proof_of_possession_round_trip() {
        let sk = test_key([4u8; 32]);
        let pk = PublicKey::from_bytes(&public_key_of(&sk)).unwrap();
        let (mu1, mu2) = make_proof_of_possession(&sk);
        assert_eq!(verify_proof_of_possession(&pk, &mu1, &mu2), Ok(true));

        let other = PublicKey::from_bytes(&public_key_of(&test_key([5u8; 32]))).unwrap();
        assert_eq!(verify_proof_of_possession(&other, &mu1, &mu2), Ok(false));
    }

    /// `VoteSigner` loaded from a cardano `bls.skey` envelope signs a vote
    /// that verifies against its own derived public key.
    #[test]
    fn vote_signer_from_cardano_signs_verifiable_vote() {
        // A cardano bls.skey envelope payload: CBOR bstr (0x5820) of a scalar.
        let scalar = test_key([8u8; 32]).to_bytes();
        let mut cbor = vec![0x58, 0x20];
        cbor.extend_from_slice(&scalar);
        let signer = VoteSigner::from_cardano(&cbor).expect("load signer");

        let pk = PublicKey::from_bytes(&signer.public_key()).unwrap();
        let eb = [0x7Eu8; 32];
        let sig = signer.sign_vote(&eb, 99);
        assert_eq!(verify_vote(&pk, &eb, 99, &sig), Ok(true));

        let (mu1, mu2) = signer.proof_of_possession();
        assert_eq!(verify_proof_of_possession(&pk, &mu1, &mu2), Ok(true));
    }

    /// A real registered-pool `bls.vkey` (public) loads from its cardano
    /// envelope and round-trips to the same 96-byte G2 point.
    #[test]
    fn real_pool_vkey_loads() {
        let vkey_cbor = hexd(
            "586095a21af6abc2213c9d907598e642ff8838223d601f5c062dbd34ce78039a813\
             f3e242f7ad98ad5d3329e60b08cf325fc07cfaae46b53fe417ecf3b782595b8df5e\
             6bc843e8403acf9251c821f52c223b28898b611849bf2ccba80a46814dc3e0",
        );
        let pk = public_key_from_cardano(&vkey_cbor).expect("load vkey");
        assert_eq!(&pk.to_bytes()[..], &vkey_cbor[2..]);
    }
}
