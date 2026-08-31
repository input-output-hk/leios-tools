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

use blst::min_sig::{AggregatePublicKey, AggregateSignature, PublicKey, SecretKey, Signature};
use blst::BLST_ERROR;
use leios_crypto_benchmarks::bls_vote::{
    check_pop, gen_cert_fa_pure, gen_sig, make_pop, verify_cert_fa_pure, verify_sig,
};

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
///
/// Internal: the public entry point is [`VoteSigner::from_cardano`], which keeps
/// the `blst` secret-key type out of the caller's hands.
pub(crate) fn secret_key_from_cardano(cbor: &[u8]) -> Result<SecretKey, BlsError> {
    let raw = strip_cbor_bstr(cbor, SECRET_KEY_SIZE)?;
    SecretKey::from_bytes(raw).map_err(|e| BlsError(format!("invalid bls secret key: {e:?}")))
}

/// Load a verification key from a cardano `bls.vkey` `cborHex` payload (a CBOR
/// byte string of the 96-byte compressed G2 point).
///
/// Internal: the public, byte-oriented entry point is
/// [`public_key_bytes_from_cardano`].
pub(crate) fn public_key_from_cardano(cbor: &[u8]) -> Result<PublicKey, BlsError> {
    let raw = strip_cbor_bstr(cbor, PUBLIC_KEY_SIZE)?;
    PublicKey::from_bytes(raw).map_err(|e| BlsError(format!("invalid bls public key: {e:?}")))
}

/// Load a verification key from a cardano `bls.vkey` `cborHex` payload, as the
/// raw 96-byte compressed G2 encoding. Validates that the payload decodes to a
/// valid point; returns its canonical compressed bytes — the `bls_key` a node
/// registers and later checks votes against.
pub fn public_key_bytes_from_cardano(cbor: &[u8]) -> Result<[u8; PUBLIC_KEY_SIZE], BlsError> {
    Ok(public_key_from_cardano(cbor)?.to_bytes())
}

/// Load a verification key from its raw 96-byte compressed encoding.
pub(crate) fn public_key_from_bytes(bytes: &[u8; PUBLIC_KEY_SIZE]) -> Result<PublicKey, BlsError> {
    PublicKey::from_bytes(bytes).map_err(|e| BlsError(format!("invalid bls public key: {e:?}")))
}

/// Derive the verification key (compressed G2) for a secret key.
pub(crate) fn public_key_of(sk: &SecretKey) -> [u8; PUBLIC_KEY_SIZE] {
    sk.sk_to_pk().to_bytes()
}

/// Sign a Leios vote: the endorser-block hash `eb_hash` under `DST = b"Leios"`
/// augmented by the election id (`slot`). Returns the 48-byte G1 signature —
/// the `vote_signature` on the wire (reference `bls_vote::gen_sig`).
///
/// Internal: the public entry point is [`VoteSigner::sign_vote`].
pub(crate) fn sign_vote(sk: &SecretKey, eb_hash: &[u8; 32], slot: u64) -> [u8; SIGNATURE_SIZE] {
    gen_sig(sk, &eid_bytes(slot), eb_hash).to_bytes()
}

/// Verify a single Leios vote signature against a voter's verification key.
///
/// Internal: the public, byte-oriented entry point is [`verify_vote_bytes`].
pub(crate) fn verify_vote(
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

/// Aggregate a quorum's vote signatures into the single 48-byte G1 aggregate
/// that is a CIP-0164 `leios_certificate`'s `aggregated_signature`. Under the
/// testnet's StakeCentile committee every voter is persistent (no NPV), so the
/// certificate reduces to this pure aggregate over the members' `sigma_m`
/// (reference `bls_vote::gen_cert_fa_pure`). Errors on an empty set or a
/// signature that does not decode.
pub fn aggregate_votes(sigs: &[[u8; SIGNATURE_SIZE]]) -> Result<[u8; SIGNATURE_SIZE], BlsError> {
    let parsed: Vec<Signature> = sigs
        .iter()
        .map(|s| {
            Signature::from_bytes(s).map_err(|e| BlsError(format!("invalid signature: {e:?}")))
        })
        .collect::<Result<_, _>>()?;
    let refs: Vec<&Signature> = parsed.iter().collect();
    gen_cert_fa_pure(&refs)
        .map(|s| s.to_bytes())
        .map_err(|e| BlsError(format!("aggregation failed: {e:?}")))
}

/// Verify a certificate's aggregate signature against the set of signing
/// members' verification keys, for endorser block `eb_hash` at election `slot`
/// — the persistent-only (StakeCentile) certificate check (reference
/// `bls_vote::verify_cert_fa_pure`). `pubkeys` must be exactly the members
/// whose votes were aggregated. Returns `Ok(false)` for a valid-but-wrong
/// certificate; `Err` if a key or the aggregate does not decode.
pub fn verify_certificate(
    pubkeys: &[[u8; PUBLIC_KEY_SIZE]],
    eb_hash: &[u8; 32],
    slot: u64,
    aggregate: &[u8; SIGNATURE_SIZE],
) -> Result<bool, BlsError> {
    let pks: Vec<PublicKey> = pubkeys
        .iter()
        .map(public_key_from_bytes)
        .collect::<Result<_, _>>()?;
    let refs: Vec<&PublicKey> = pks.iter().collect();
    let agg = Signature::from_bytes(aggregate)
        .map_err(|e| BlsError(format!("invalid aggregate signature: {e:?}")))?;
    Ok(verify_cert_fa_pure(&refs, &eid_bytes(slot), eb_hash, &agg))
}

// ---------------------------------------------------------------------------
// The deployed cardano-node construction
// ---------------------------------------------------------------------------
//
// The functions above follow IOG's `leios_crypto_benchmarks` reference: DST
// `b"Leios"`, the election id as blst's augmentation, and the EB hash as the
// message. The node actually deployed on the Leios testnets signs votes
// through `cardano-base`'s BLS12381 DSIGN instance instead
// (`Cardano.Crypto.DSIGN.BLS12381.Internal`), which differs in all three:
//
// ```haskell
// -- ouroboros-consensus  LeiosDemoTypes.signLeiosVote
// voteSignature = signDSIGN leiosSignContext announcingRbHash sk
// -- cardano-base  Cardano.Crypto.DSIGN.BLS12381.Internal
// minSigPoPDST = BLS12381SignContext (Just "BLS_SIG_BLS12381G1_XMD:SHA-256_SSWU_RO_POP_") Nothing
// ```
//
//   * DST is the IETF minimal-signature-size proof-of-possession tag, not `b"Leios"`;
//   * there is NO augmentation — the election is identified by the message itself;
//   * the message is the **announcing RB hash**, not the EB hash.
//
// A vote built the reference way is therefore rejected by every real node, and
// a certificate aggregating one fails the ledger's `verifyLeiosCert` with
// `InvalidSignature`. Both constructions are kept: the reference one for the
// simulator and for CIP-conformance work, and the pair below for talking to a
// live network.

/// The domain-separation tag a deployed cardano-node signs Leios votes under:
/// `cardano-base`'s `minSigPoPDST`, the IETF minimal-signature-size
/// proof-of-possession tag. There is no augmentation in this scheme.
pub const CARDANO_VOTE_DST: &[u8] = b"BLS_SIG_BLS12381G1_XMD:SHA-256_SSWU_RO_POP_";

/// The bytes actually signed for an announcing RB hash.
///
/// `signDSIGN` signs `getSignableRepresentation msg`, and `RbHash`'s instance
/// is `toStrictByteString . encodeRbHash` — a **CBOR byte string**, not the raw
/// hash:
///
/// ```haskell
/// encodeRbHash (MkRbHash bytes) = CBOR.encodeBytes bytes
/// instance SignableRepresentation RbHash where
///   getSignableRepresentation point = toStrictByteString $ encodeRbHash point
/// ```
///
/// For a 32-byte hash that is the one-byte-length form `0x58 0x20 ‖ hash`, 34
/// bytes in total. Signing the bare 32 bytes produces a signature no node
/// accepts — the two-byte header is not cosmetic.
fn rb_hash_signable(announcing_rb_hash: &[u8; 32]) -> [u8; 34] {
    let mut msg = [0u8; 34];
    msg[0] = 0x58; // CBOR major type 2 (byte string), one-byte length follows
    msg[1] = 32; // the length
    msg[2..].copy_from_slice(announcing_rb_hash);
    msg
}

/// Sign a Leios vote the way a deployed cardano-node does: the announcing RB
/// hash under [`CARDANO_VOTE_DST`], with no augmentation.
///
/// Internal: the public entry point is [`VoteSigner::sign_rb_vote`].
pub(crate) fn sign_rb_vote(sk: &SecretKey, announcing_rb_hash: &[u8; 32]) -> [u8; SIGNATURE_SIZE] {
    sk.sign(&rb_hash_signable(announcing_rb_hash), CARDANO_VOTE_DST, &[])
        .to_bytes()
}

/// Verify a deployed-node Leios vote: `announcing_rb_hash` signed under
/// [`CARDANO_VOTE_DST`] by the voter's registered key. Returns `Ok(false)` for
/// a valid-but-wrong signature; `Err` if the key or signature does not decode.
pub fn verify_rb_vote_bytes(
    pubkey: &[u8; PUBLIC_KEY_SIZE],
    announcing_rb_hash: &[u8; 32],
    sig: &[u8; SIGNATURE_SIZE],
) -> Result<bool, BlsError> {
    let pk = public_key_from_bytes(pubkey)?;
    let signature =
        Signature::from_bytes(sig).map_err(|e| BlsError(format!("invalid signature: {e:?}")))?;
    // `true`/`true`: group-check both the signature and the key. These arrive
    // from the network, so neither is trusted to be on the curve.
    Ok(
        signature.verify(
            true,
            &rb_hash_signable(announcing_rb_hash),
            CARDANO_VOTE_DST,
            &[],
            &pk,
            true,
        ) == BLST_ERROR::BLST_SUCCESS,
    )
}

/// Aggregate deployed-node vote signatures into the 48-byte G1 aggregate that
/// rides in a `leios_certificate`. Plain G1 point addition, matching
/// `aggregateSigsDSIGN`. Errors on an empty set or a signature that does not
/// decode.
pub fn aggregate_rb_votes(sigs: &[[u8; SIGNATURE_SIZE]]) -> Result<[u8; SIGNATURE_SIZE], BlsError> {
    if sigs.is_empty() {
        return Err(BlsError("cannot aggregate zero signatures".into()));
    }
    let parsed: Vec<Signature> = sigs
        .iter()
        .map(|s| {
            Signature::from_bytes(s).map_err(|e| BlsError(format!("invalid signature: {e:?}")))
        })
        .collect::<Result<_, _>>()?;
    let refs: Vec<&Signature> = parsed.iter().collect();
    AggregateSignature::aggregate(&refs, true)
        .map(|agg| agg.to_signature().to_bytes())
        .map_err(|e| BlsError(format!("aggregation failed: {e:?}")))
}

/// Verify a `leios_certificate`'s aggregate against the signing members' keys,
/// the way the ledger's `verifyLeiosCert` does: aggregate the verification
/// keys, then check the aggregate signature over `announcing_rb_hash` under
/// [`CARDANO_VOTE_DST`].
///
/// `pubkeys` must be exactly the members whose votes were aggregated — the ones
/// the certificate's signers bitfield names. Their proofs of possession are
/// assumed already checked (at committee selection), as in the ledger.
pub fn verify_rb_certificate(
    pubkeys: &[[u8; PUBLIC_KEY_SIZE]],
    announcing_rb_hash: &[u8; 32],
    aggregate: &[u8; SIGNATURE_SIZE],
) -> Result<bool, BlsError> {
    if pubkeys.is_empty() {
        return Err(BlsError("cannot verify against zero keys".into()));
    }
    let pks: Vec<PublicKey> = pubkeys
        .iter()
        .map(public_key_from_bytes)
        .collect::<Result<_, _>>()?;
    let refs: Vec<&PublicKey> = pks.iter().collect();
    let agg_pk = AggregatePublicKey::aggregate(&refs, true)
        .map_err(|e| BlsError(format!("key aggregation failed: {e:?}")))?
        .to_public_key();
    let sig = Signature::from_bytes(aggregate)
        .map_err(|e| BlsError(format!("invalid aggregate signature: {e:?}")))?;
    Ok(
        sig.verify(
            true,
            &rb_hash_signable(announcing_rb_hash),
            CARDANO_VOTE_DST,
            &[],
            &agg_pk,
            true,
        ) == BLST_ERROR::BLST_SUCCESS,
    )
}

/// Generate a proof of possession for a secret key: the two-signature PoP the
/// Leios registration uses (reference `bls_vote::make_pop`). Returns
/// `(mu1, mu2)` as 48-byte G1 signatures.
///
/// Internal: the public entry point is [`VoteSigner::proof_of_possession`].
pub(crate) fn make_proof_of_possession(
    sk: &SecretKey,
) -> ([u8; SIGNATURE_SIZE], [u8; SIGNATURE_SIZE]) {
    let (mu1, mu2) = make_pop(sk);
    (mu1.to_bytes(), mu2.to_bytes())
}

/// Verify a proof of possession against a verification key.
///
/// Internal: the public, byte-oriented entry point is
/// [`verify_proof_of_possession_bytes`].
pub(crate) fn verify_proof_of_possession(
    pk: &PublicKey,
    mu1: &[u8; SIGNATURE_SIZE],
    mu2: &[u8; SIGNATURE_SIZE],
) -> Result<bool, BlsError> {
    let s1 = Signature::from_bytes(mu1).map_err(|e| BlsError(format!("invalid pop mu1: {e:?}")))?;
    let s2 = Signature::from_bytes(mu2).map_err(|e| BlsError(format!("invalid pop mu2: {e:?}")))?;
    Ok(check_pop(pk, &s1, &s2))
}

/// Verify a proof of possession from a voter's raw 96-byte verification key —
/// the byte-oriented form a node uses to check a registering pool's PoP against
/// its `bls_key`. Returns `Ok(false)` for a valid-but-wrong PoP, `Err` if the
/// key or either signature does not decode.
pub fn verify_proof_of_possession_bytes(
    pubkey: &[u8; PUBLIC_KEY_SIZE],
    mu1: &[u8; SIGNATURE_SIZE],
    mu2: &[u8; SIGNATURE_SIZE],
) -> Result<bool, BlsError> {
    let pk = public_key_from_bytes(pubkey)?;
    verify_proof_of_possession(&pk, mu1, mu2)
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

    /// Sign a vote for endorser block `eb_hash` at election `slot`, using the
    /// `leios_crypto_benchmarks` reference construction. A live network will
    /// not accept this — see [`sign_rb_vote`](Self::sign_rb_vote).
    pub fn sign_vote(&self, eb_hash: &[u8; 32], slot: u64) -> [u8; SIGNATURE_SIZE] {
        sign_vote(&self.sk, eb_hash, slot)
    }

    /// Sign a vote the way a deployed cardano-node does: over the announcing
    /// RB hash, under [`CARDANO_VOTE_DST`], with no augmentation. This is the
    /// signature a real network verifies and aggregates into a certificate.
    pub fn sign_rb_vote(&self, announcing_rb_hash: &[u8; 32]) -> [u8; SIGNATURE_SIZE] {
        sign_rb_vote(&self.sk, announcing_rb_hash)
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

    /// A quorum's vote signatures aggregate into a certificate that verifies
    /// against exactly the signing members' keys — and fails for the wrong
    /// election, a missing signer, or a tampered aggregate.
    #[test]
    fn certificate_aggregate_verifies_against_signer_set() {
        let eb = [0x33u8; 32];
        let slot = 77u64;
        let signers: Vec<VoteSigner> = (0..4u8)
            .map(|i| VoteSigner::generate(&[i + 20; 32]))
            .collect();
        let sigs: Vec<[u8; 48]> = signers.iter().map(|s| s.sign_vote(&eb, slot)).collect();
        let pubkeys: Vec<[u8; 96]> = signers.iter().map(|s| s.public_key()).collect();

        let agg = aggregate_votes(&sigs).expect("aggregate");
        assert_eq!(verify_certificate(&pubkeys, &eb, slot, &agg), Ok(true));

        // Wrong election parameters, or a missing signer's key, all fail.
        assert_eq!(verify_certificate(&pubkeys, &eb, slot + 1, &agg), Ok(false));
        assert_eq!(
            verify_certificate(&pubkeys, &[0u8; 32], slot, &agg),
            Ok(false)
        );
        assert_eq!(
            verify_certificate(&pubkeys[..3], &eb, slot, &agg),
            Ok(false),
            "missing a signer's key"
        );

        // A tampered aggregate never verifies.
        let mut bad = agg;
        bad[0] ^= 0xff;
        assert_ne!(verify_certificate(&pubkeys, &eb, slot, &bad), Ok(true));
    }

    #[test]
    fn aggregate_rejects_empty_set() {
        assert!(aggregate_votes(&[]).is_err());
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
        // The public byte-oriented loader yields the same canonical 96 bytes.
        let pk_bytes = public_key_bytes_from_cardano(&vkey_cbor).expect("load vkey bytes");
        assert_eq!(&pk_bytes[..], &vkey_cbor[2..]);
    }

    /// The byte-oriented PoP verify (raw 96-byte key) agrees with signing, and
    /// rejects a different key — the public surface a node uses at registration.
    #[test]
    fn proof_of_possession_bytes_round_trip() {
        let signer = VoteSigner::generate(&[6u8; 32]);
        let (mu1, mu2) = signer.proof_of_possession();
        assert_eq!(
            verify_proof_of_possession_bytes(&signer.public_key(), &mu1, &mu2),
            Ok(true)
        );
        let other = VoteSigner::generate(&[7u8; 32]);
        assert_eq!(
            verify_proof_of_possession_bytes(&other.public_key(), &mu1, &mu2),
            Ok(false)
        );
    }

    /// The deployed-node construction, end to end: sign the announcing RB
    /// hash, aggregate a quorum, verify the aggregate against the signers'
    /// keys — exactly the shape `verifyLeiosCert` checks on chain.
    #[test]
    fn cardano_rb_vote_and_certificate_round_trip() {
        let rb = [0x5au8; 32];
        let signers: Vec<VoteSigner> =
            (0..4).map(|i| VoteSigner::generate(&[i as u8 + 1; 32])).collect();
        let sigs: Vec<[u8; SIGNATURE_SIZE]> =
            signers.iter().map(|s| s.sign_rb_vote(&rb)).collect();
        let keys: Vec<[u8; PUBLIC_KEY_SIZE]> =
            signers.iter().map(|s| s.public_key()).collect();

        for (sig, key) in sigs.iter().zip(&keys) {
            assert_eq!(verify_rb_vote_bytes(key, &rb, sig), Ok(true));
        }

        let agg = aggregate_rb_votes(&sigs).expect("aggregate");
        assert_eq!(verify_rb_certificate(&keys, &rb, &agg), Ok(true));

        // A different RB hash, or a key set that is not exactly the signers,
        // must fail — the two ways a producer gets a certificate wrong.
        assert_eq!(verify_rb_certificate(&keys, &[0x99u8; 32], &agg), Ok(false));
        assert_eq!(verify_rb_certificate(&keys[..3], &rb, &agg), Ok(false));
    }

    /// The two constructions are NOT interchangeable. This is the bug that
    /// made every certificate net-rs published get rejected on chain, so pin
    /// it: a reference-construction vote must not verify as a deployed-node
    /// one, and vice versa.
    #[test]
    fn reference_and_cardano_constructions_are_distinct() {
        let rb = [0x5au8; 32];
        let signer = VoteSigner::generate(&[9u8; 32]);
        let pk = signer.public_key();

        // Same signer, same 32-byte message, different DST/augmentation.
        let reference = signer.sign_vote(&rb, 0);
        let cardano = signer.sign_rb_vote(&rb);
        assert_ne!(reference, cardano);

        assert_eq!(verify_rb_vote_bytes(&pk, &rb, &reference), Ok(false));
        assert_eq!(verify_vote_bytes(&pk, &rb, 0, &cardano), Ok(false));
    }

    /// The DST is a wire constant: it is what a real node hashes the message
    /// under, so a typo here silently invalidates every vote we cast.
    /// The signed bytes are a CBOR byte string, not the bare hash. Pin the
    /// exact 34-byte encoding: this two-byte header is the difference between
    /// a vote the network counts and one it silently drops.
    #[test]
    fn rb_hash_signable_is_a_cbor_byte_string() {
        let msg = rb_hash_signable(&[0xabu8; 32]);
        assert_eq!(msg.len(), 34);
        assert_eq!(&msg[..2], &[0x58, 0x20]);
        assert_eq!(&msg[2..], &[0xabu8; 32]);
    }

    #[test]
    fn cardano_dst_matches_cardano_base_min_sig_pop() {
        assert_eq!(
            CARDANO_VOTE_DST,
            b"BLS_SIG_BLS12381G1_XMD:SHA-256_SSWU_RO_POP_"
        );
    }
}
