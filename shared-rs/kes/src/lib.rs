//! Cardano Sum6 KES — the forward-secure signer + verifier the Leios
//! block-production forge needs to sign block-header bodies.
//!
//! This is a thin, **byte-oriented** wrapper over `kes-summed-ed25519`
//! (`github.com/input-output-hk/kes`): the MMM binary-sum composition
//! (Malkin–Micciancio–Miner, "Composition and Efficiency Tradeoffs for
//! Forward-Secure Digital Signatures") over Ed25519 that cardano-base's
//! `Cardano.Crypto.KES` implements. Cardano uses **Sum6** — 2⁶ = 64 KES
//! periods (one period ≈ 129 600 slots ≈ 1.5 days on mainnet).
//!
//! The upstream crate's signing-key and signature types are lifetime-bound
//! views over caller-owned byte buffers; this wrapper owns the buffer and
//! exposes only fixed-size arrays matching cardano's wire formats:
//!
//! - 32-byte verification key (the op-cert `hot_vkey`),
//! - 448-byte Sum6 signature (the header `body_signature`),
//! - a 32-byte secret seed for deterministic key generation.
//!
//! The operational-certificate glue (the 48-byte cold-key signable and the
//! Ed25519 issuer key) lives in [`ocert`].
//!
//! Adopt-vs-vendor rationale is recorded in the block-production plan under
//! "KES dependency".
//!
//! ## Byte-compatibility note
//!
//! The upstream crate serializes a *secret* key as `Self::SIZE` key bytes
//! followed by a 4-byte big-endian period counter — 4 bytes longer than
//! cardano-node's `.skey` payload. That trailer never crosses the wire (only
//! the verification key and signature do, and both are byte-identical to
//! cardano-base), but it means [`KesSigner::as_bytes`] is 4 bytes longer than
//! a cardano `.skey`; see [`ocert`] and the plan for the ±4 conversion.

use kes_summed_ed25519::kes::{Sum6Kes, Sum6KesSig};
use kes_summed_ed25519::traits::{KesSig, KesSk};
use kes_summed_ed25519::PublicKey;

use std::fmt;

pub mod ocert;

/// Byte size of a KES verification key (`hot_vkey`).
pub const VKEY_SIZE: usize = 32;
/// Byte size of a Sum6 KES signature (`SIGMA_SIZE + 6·(2·PUBLIC_KEY_SIZE)`
/// = 64 + 384). This is the header `body_signature` length.
pub const SIG_SIZE: usize = 448;
/// Byte size of the secret seed used to generate a KES key.
pub const SEED_SIZE: usize = 32;
/// Byte size of the owned secret-key buffer: the Sum6 key material
/// (`INDIVIDUAL_SECRET_SIZE + 6·32 + 6·64` = 32 + 192 + 384 = 608) plus the
/// upstream crate's 4-byte big-endian period trailer.
pub const SK_SIZE: usize = 608 + 4;
/// Number of KES periods a Sum6 key can sign in: periods `0..=63`.
pub const MAX_PERIOD: u32 = 63;

/// A 32-byte KES verification key.
pub type Vkey = [u8; VKEY_SIZE];
/// A 448-byte Sum6 KES signature.
pub type Sig = [u8; SIG_SIZE];

/// A KES operation failed: a byte buffer did not decode, a signature did not
/// verify, or a key was evolved past its final period. As with the VRF
/// wrapper, decode and verification failure are deliberately not
/// distinguished — for header validation both are simply "not acceptable".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KesError;

impl fmt::Display for KesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "KES operation failed to decode, verify, or evolve")
    }
}

impl std::error::Error for KesError {}

/// An owned Sum6 KES signing key, evolvable forward through its 64 periods.
///
/// Holds an owned copy of the key bytes (secret material + 4-byte period
/// trailer) and caches the derived verification key. The upstream
/// `Sum6Kes<'a>` is a lifetime-bound view that **zeroizes its borrowed buffer
/// on drop** (forward secrecy); it can never own the key across calls. So
/// this wrapper keeps the authoritative copy and only ever lends the upstream
/// type a throwaway scratch buffer it is free to zeroize. Evolution results
/// are copied back into the owned buffer before the scratch is dropped.
///
/// The key never leaves this type except as a signature or its public
/// verification key.
pub struct KesSigner {
    buf: [u8; SK_SIZE],
    vkey: Vkey,
}

impl KesSigner {
    /// Generate a fresh KES key at period 0 from a 32-byte seed. The seed is
    /// expanded exactly as cardano-base does, so equal seeds yield equal keys
    /// (and equal verification keys) — the property tests and the node's
    /// deterministic key loading both rely on.
    pub fn generate(seed: &[u8; SEED_SIZE]) -> Self {
        let mut scratch = [0u8; SK_SIZE];
        // `keygen` zeroizes the seed buffer, so hand it a copy.
        let mut seed_buf = *seed;
        let mut buf = [0u8; SK_SIZE];
        // The returned `Sum6Kes` borrows `scratch`; copy the generated key
        // out into our owned `buf` and read the vkey, then let `_sk` drop —
        // which zeroizes `scratch`, not `buf`.
        let vkey = {
            let (sk, pk) = Sum6Kes::keygen(&mut scratch, &mut seed_buf);
            buf.copy_from_slice(sk.as_bytes());
            vkey_bytes(&pk)
        };
        Self { buf, vkey }
    }

    /// Import a serialized KES key: the [`SK_SIZE`]-byte owned-buffer form
    /// (secret material + 4-byte big-endian period trailer). Errors if it does
    /// not decode as a Sum6 key. The verification key is recovered from the
    /// key material, so the imported signer knows its own `hot_vkey`.
    pub fn from_bytes(buf: &[u8; SK_SIZE]) -> Result<Self, KesError> {
        // Reject an out-of-range period trailer up front: Sum6 has periods
        // `0..=MAX_PERIOD`, so a larger trailer is not a valid key and would
        // make `period()`/`sign()` ill-defined. (Also covers the trailer this
        // wrapper appends in `from_cardano_skey`.)
        let mut p = [0u8; 4];
        p.copy_from_slice(&buf[SK_SIZE - 4..]);
        if u32::from_be_bytes(p) > MAX_PERIOD {
            return Err(KesError);
        }
        // `from_bytes` borrows and (on drop) zeroizes its buffer, so decode a
        // scratch copy to read the vkey and keep our own owned copy.
        let mut scratch = *buf;
        let vkey = {
            let sk = Sum6Kes::from_bytes(&mut scratch).map_err(|_| KesError)?;
            vkey_bytes(&sk.to_pk())
        };
        Ok(Self { buf: *buf, vkey })
    }

    /// Import a cardano-node `kes.skey` payload — the 608-byte raw Sum6 secret
    /// key with **no** period trailer — as a key at KES period `period`.
    /// cardano-cli `node key-gen-KES` writes the key at period 0; this appends
    /// the 4-byte big-endian period trailer the upstream crate stores after
    /// the key material (the documented ±4 conversion). Errors if `raw` is not
    /// 608 bytes or does not decode.
    pub fn from_cardano_skey(raw: &[u8], period: u32) -> Result<Self, KesError> {
        const RAW_SIZE: usize = SK_SIZE - 4;
        if raw.len() != RAW_SIZE {
            return Err(KesError);
        }
        let mut buf = [0u8; SK_SIZE];
        buf[..RAW_SIZE].copy_from_slice(raw);
        buf[RAW_SIZE..].copy_from_slice(&period.to_be_bytes());
        Self::from_bytes(&buf)
    }

    /// The current KES period (`0..=63`), read from the key's period trailer.
    pub fn period(&self) -> u32 {
        let mut p = [0u8; 4];
        p.copy_from_slice(&self.buf[SK_SIZE - 4..]);
        u32::from_be_bytes(p)
    }

    /// The 32-byte verification key — the op-cert `hot_vkey`. Constant across
    /// evolution (evolving the secret key does not change the public key).
    pub fn vkey(&self) -> Vkey {
        self.vkey
    }

    /// Evolve the key forward to `target` period. No-op if already at or past
    /// `target`. Errors if `target` exceeds the key's final period
    /// ([`MAX_PERIOD`]) — a KES key cannot sign beyond period 63.
    ///
    /// Forward security: evolution destroys the ability to sign for earlier
    /// periods, so this only moves forward.
    pub fn evolve_to(&mut self, target: u32) -> Result<(), KesError> {
        if target > MAX_PERIOD {
            return Err(KesError);
        }
        // Operate on a scratch copy; the upstream key zeroizes it on drop.
        let mut scratch = self.buf;
        {
            let mut sk = Sum6Kes::from_bytes(&mut scratch).map_err(|_| KesError)?;
            while sk.get_period() < target {
                sk.update().map_err(|_| KesError)?;
            }
            // Persist the evolved bytes into our owned buffer before `sk`
            // drops and zeroizes `scratch`.
            self.buf.copy_from_slice(sk.as_bytes());
        }
        Ok(())
    }

    /// Sign `msg` (the serialized header body) with the key at its current
    /// period, returning the 448-byte Sum6 signature. A verifier accepts it
    /// via [`verify`] at the same period against [`vkey`](Self::vkey).
    pub fn sign(&self, msg: &[u8]) -> Sig {
        // Sign over a scratch copy: `from_bytes` needs `&mut`, and the
        // upstream key zeroizes its buffer on drop — never let it touch the
        // owned key.
        let mut scratch = self.buf;
        let sk = Sum6Kes::from_bytes(&mut scratch).expect("owned buffer is always a valid KES key");
        sk.sign(msg).to_bytes()
    }

    /// The raw secret-key buffer (secret material + 4-byte period trailer),
    /// [`SK_SIZE`] bytes. Note this is 4 bytes longer than a cardano-node
    /// `.skey` payload — see the crate-level byte-compatibility note.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }
}

impl Drop for KesSigner {
    /// Best-effort scrub of the owned key material on drop.
    fn drop(&mut self) {
        self.buf.fill(0);
    }
}

impl fmt::Debug for KesSigner {
    /// Redacts the secret buffer; shows only the public vkey and period.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KesSigner")
            .field("vkey", &hex_short(&self.vkey))
            .field("period", &self.period())
            .finish_non_exhaustive()
    }
}

/// Verify a Sum6 signature `sig` over `msg` against verification key `vkey` at
/// KES `period`. Returns `Ok(())` iff the signature is valid — this is exactly
/// the check a real node runs on a block header's `body_signature`.
///
/// `period` is the *relative* KES period `t = current_kes_period −
/// ocert_kes_period`: the number of evolutions from the key's op-cert start
/// period. A fresh op-cert whose start period equals the current period signs
/// (and verifies) at `t = 0`.
pub fn verify(period: u32, vkey: &Vkey, msg: &[u8], sig: &Sig) -> Result<(), KesError> {
    // Sum6 only spans periods `0..=MAX_PERIOD`; a larger relative period means
    // an expired op-cert, which must fail rather than depend on upstream
    // behavior for an out-of-range period.
    if period > MAX_PERIOD {
        return Err(KesError);
    }
    let pk = PublicKey::from_bytes(vkey).map_err(|_| KesError)?;
    let s = Sum6KesSig::from_bytes(sig).map_err(|_| KesError)?;
    s.verify(period, &pk, msg).map_err(|_| KesError)
}

/// Copy a KES `PublicKey` into a fixed 32-byte array.
fn vkey_bytes(pk: &PublicKey) -> Vkey {
    let mut out = [0u8; VKEY_SIZE];
    out.copy_from_slice(pk.as_bytes());
    out
}

/// First 4 bytes of a key as hex, for redacted `Debug`.
fn hex_short(b: &[u8]) -> String {
    b.iter().take(4).map(|x| format!("{x:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Generation is deterministic in the seed, and equal seeds yield equal
    /// verification keys — the node's deterministic key loading depends on it.
    #[test]
    fn generate_is_deterministic() {
        let seed = [3u8; SEED_SIZE];
        let a = KesSigner::generate(&seed);
        let b = KesSigner::generate(&seed);
        assert_eq!(a.vkey(), b.vkey());
        assert_eq!(a.as_bytes(), b.as_bytes());
        assert_eq!(a.period(), 0);
    }

    /// A generated key survives a serialize → `from_bytes` round-trip: same
    /// vkey, same bytes, still signs verifiably.
    #[test]
    fn from_bytes_round_trips() {
        let signer = KesSigner::generate(&[13u8; SEED_SIZE]);
        let buf: &[u8; SK_SIZE] = signer.as_bytes().try_into().unwrap();
        let reloaded = KesSigner::from_bytes(buf).expect("re-import");
        assert_eq!(reloaded.vkey(), signer.vkey());
        assert_eq!(reloaded.as_bytes(), signer.as_bytes());
        let msg = b"reload";
        assert!(verify(0, &reloaded.vkey(), msg, &reloaded.sign(msg)).is_ok());
    }

    /// A cardano-style 608-byte raw key (no period trailer) imports with the
    /// vkey recovered; a wrong length is rejected.
    #[test]
    fn from_cardano_skey_appends_period() {
        let signer = KesSigner::generate(&[17u8; SEED_SIZE]);
        let raw = &signer.as_bytes()[..SK_SIZE - 4];
        let imported = KesSigner::from_cardano_skey(raw, 0).expect("import raw");
        assert_eq!(
            imported.vkey(),
            signer.vkey(),
            "vkey recovered from raw key"
        );
        assert_eq!(imported.period(), 0);
        assert!(KesSigner::from_cardano_skey(&raw[..600], 0).is_err());
    }

    /// Sign at period 0 → verify at period 0 round-trips; the same signature
    /// must not verify at a different period or against a different message.
    #[test]
    fn sign_verify_round_trip() {
        let signer = KesSigner::generate(&[7u8; SEED_SIZE]);
        let msg = b"leios/praos/header-body";
        let sig = signer.sign(msg);
        assert_eq!(sig.len(), SIG_SIZE);

        assert!(verify(0, &signer.vkey(), msg, &sig).is_ok());
        assert_eq!(verify(1, &signer.vkey(), msg, &sig), Err(KesError));
        assert_eq!(
            verify(0, &signer.vkey(), b"other message", &sig),
            Err(KesError)
        );
    }

    /// Evolving to period `t` lets the key sign for period `t`, and that
    /// signature verifies only at `t`. The vkey is stable across evolution.
    #[test]
    fn evolve_then_sign_verifies_at_that_period() {
        let mut signer = KesSigner::generate(&[9u8; SEED_SIZE]);
        let vkey0 = signer.vkey();
        signer.evolve_to(3).expect("within 0..=63");
        assert_eq!(signer.period(), 3);
        assert_eq!(signer.vkey(), vkey0, "vkey is stable across evolution");

        let msg = b"period-3 body";
        let sig = signer.sign(msg);
        assert!(verify(3, &signer.vkey(), msg, &sig).is_ok());
        assert_eq!(verify(0, &signer.vkey(), msg, &sig), Err(KesError));
    }

    /// A key cannot be evolved past its final period.
    #[test]
    fn cannot_evolve_past_final_period() {
        let mut signer = KesSigner::generate(&[1u8; SEED_SIZE]);
        assert_eq!(signer.evolve_to(MAX_PERIOD + 1), Err(KesError));
        // Evolving right up to the final period is fine.
        assert!(signer.evolve_to(MAX_PERIOD).is_ok());
        assert_eq!(signer.period(), MAX_PERIOD);
    }

    /// An out-of-range period trailer is rejected on import, and `verify`
    /// rejects an out-of-range relative period rather than trusting upstream.
    #[test]
    fn out_of_range_period_is_rejected() {
        let signer = KesSigner::generate(&[23u8; SEED_SIZE]);
        let mut buf: [u8; SK_SIZE] = signer.as_bytes().try_into().unwrap();
        // A valid key round-trips; bump the trailer past MAX_PERIOD and it must
        // no longer import.
        assert!(KesSigner::from_bytes(&buf).is_ok());
        buf[SK_SIZE - 4..].copy_from_slice(&(MAX_PERIOD + 1).to_be_bytes());
        assert!(KesSigner::from_bytes(&buf).is_err());

        let msg = b"anything";
        let sig = signer.sign(msg);
        assert_eq!(verify(MAX_PERIOD + 1, &signer.vkey(), msg, &sig), Err(KesError));
    }

    /// A tampered signature does not verify.
    #[test]
    fn tampered_signature_fails() {
        let signer = KesSigner::generate(&[5u8; SEED_SIZE]);
        let msg = b"payload";
        let mut sig = signer.sign(msg);
        sig[0] ^= 0xff;
        assert!(verify(0, &signer.vkey(), msg, &sig).is_err());
    }

    /// Debug never leaks secret bytes.
    #[test]
    fn debug_redacts_secret() {
        let signer = KesSigner::generate(&[2u8; SEED_SIZE]);
        let s = format!("{signer:?}");
        assert!(s.contains("KesSigner"));
        assert!(s.contains("period"));
        // The 608-byte secret material must not appear; a full hex dump would
        // be very long, so assert the rendered form is short.
        assert!(s.len() < 200, "Debug output looks like it leaks key bytes");
    }
}
