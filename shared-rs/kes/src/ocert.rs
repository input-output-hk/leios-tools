//! Operational-certificate glue.
//!
//! A Cardano operational certificate authorizes a KES "hot" key with the
//! stake pool's "cold" Ed25519 key. In a block header it is the 4-tuple
//!
//! ```text
//! operational_cert = ( hot_vkey        : bstr .size 32   ; KES verification key
//!                    , sequence_number : uint            ; the op-cert counter
//!                    , kes_period      : uint            ; start KES period
//!                    , sigma           : bstr .size 64 ) ; cold-key signature
//! ```
//!
//! and the header's `issuer_vkey` is the cold key's Ed25519 verification key.
//! The cold key signs a flat 48-byte message (NOT the CBOR tuple):
//!
//! ```text
//! OCertSignable = rawKESvkey(32) ‖ counter(u64, big-endian) ‖ kes_period(u64, big-endian)
//! ```
//!
//! (cardano-protocol `OCert.hs`, `OCertSignable`/`SignableRepresentation`).
//! The counter and period are raw 8-byte big-endian here, distinct from the
//! CBOR `uint`s in the on-wire tuple — a width/endianness trap this module
//! centralizes. This module produces the raw parts; CBOR assembly of the
//! 4-tuple stays with the header encoder that owns the rest of the layout.

use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};

use crate::{KesError, Vkey};

/// Byte length of the cold-key signable message.
pub const SIGNABLE_SIZE: usize = 48;
/// Byte length of a cold Ed25519 verification key (the header `issuer_vkey`).
pub const COLD_VKEY_SIZE: usize = 32;
/// Byte length of the cold-key signature (`sigma`).
pub const COLD_SIG_SIZE: usize = 64;

/// A cold Ed25519 verification key (the header `issuer_vkey`).
pub type ColdVkey = [u8; COLD_VKEY_SIZE];
/// A cold-key operational-certificate signature (`sigma`).
pub type ColdSig = [u8; COLD_SIG_SIZE];

/// The KES period a slot falls in: `slot / slots_per_kes_period`
/// (`slotsPerKESPeriod` from the Shelley genesis; 129 600 on mainnet).
pub fn kes_period(slot: u64, slots_per_kes_period: u64) -> u64 {
    slot / slots_per_kes_period
}

/// Build the 48-byte `OCertSignable`: `hot_vkey(32) ‖ counter(BE64) ‖
/// kes_period(BE64)`. This is exactly the message the cold key signs and a
/// validator reconstructs to check `sigma`.
pub fn signable(hot_vkey: &Vkey, counter: u64, kes_period: u64) -> [u8; SIGNABLE_SIZE] {
    let mut m = [0u8; SIGNABLE_SIZE];
    m[..32].copy_from_slice(hot_vkey);
    m[32..40].copy_from_slice(&counter.to_be_bytes());
    m[40..48].copy_from_slice(&kes_period.to_be_bytes());
    m
}

/// A stake pool's cold Ed25519 signing key. Long-lived and offline in a real
/// deployment; here it is generated deterministically from a seed so a node's
/// issuer identity is stable across restarts (CIP-0164 equivocation detection
/// keys on a stable issuer, not a fresh key per block).
pub struct ColdKey {
    sk: SigningKey,
}

impl ColdKey {
    /// Deterministically derive the cold key from a 32-byte seed.
    pub fn from_seed(seed: &[u8; 32]) -> Self {
        Self {
            sk: SigningKey::from_bytes(seed),
        }
    }

    /// The cold verification key — the header `issuer_vkey`.
    pub fn vkey(&self) -> ColdVkey {
        self.sk.verifying_key().to_bytes()
    }

    /// Sign an operational certificate: the cold key signs the 48-byte
    /// [`signable`] authorizing `hot_vkey` for `counter`/`kes_period`.
    /// Returns `sigma`.
    pub fn sign_ocert(&self, hot_vkey: &Vkey, counter: u64, kes_period: u64) -> ColdSig {
        let m = signable(hot_vkey, counter, kes_period);
        self.sk.sign(&m).to_bytes()
    }
}

/// Verify an operational certificate exactly as a node does: reconstruct the
/// 48-byte signable and check `sigma` against the cold `issuer_vkey`. Uses
/// strict (canonical) Ed25519 verification.
pub fn verify_ocert(
    issuer_vkey: &ColdVkey,
    hot_vkey: &Vkey,
    counter: u64,
    kes_period: u64,
    sigma: &ColdSig,
) -> Result<(), KesError> {
    let vk = VerifyingKey::from_bytes(issuer_vkey).map_err(|_| KesError)?;
    let m = signable(hot_vkey, counter, kes_period);
    let sig = Signature::from_bytes(sigma);
    vk.verify_strict(&m, &sig).map_err(|_| KesError)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::KesSigner;

    #[test]
    fn kes_period_divides_slot() {
        assert_eq!(kes_period(0, 129_600), 0);
        assert_eq!(kes_period(129_599, 129_600), 0);
        assert_eq!(kes_period(129_600, 129_600), 1);
        assert_eq!(kes_period(300_000, 129_600), 2);
    }

    #[test]
    fn signable_layout_is_be() {
        let hot = [0xABu8; 32];
        let m = signable(&hot, 0x0102, 0x0304);
        assert_eq!(&m[..32], &hot);
        assert_eq!(&m[32..40], &[0, 0, 0, 0, 0, 0, 0x01, 0x02]);
        assert_eq!(&m[40..48], &[0, 0, 0, 0, 0, 0, 0x03, 0x04]);
    }

    /// The cold key signs an op-cert authorizing a real KES hot vkey, and the
    /// node-side verify accepts it — and rejects any field tampering.
    #[test]
    fn ocert_sign_verify_round_trip() {
        let cold = ColdKey::from_seed(&[11u8; 32]);
        let kes = KesSigner::generate(&[22u8; 32]);
        let hot = kes.vkey();
        let (counter, period) = (5u64, 42u64);

        let sigma = cold.sign_ocert(&hot, counter, period);
        assert!(verify_ocert(&cold.vkey(), &hot, counter, period, &sigma).is_ok());

        // Wrong counter, period, hot vkey, or issuer all fail.
        assert!(verify_ocert(&cold.vkey(), &hot, counter + 1, period, &sigma).is_err());
        assert!(verify_ocert(&cold.vkey(), &hot, counter, period + 1, &sigma).is_err());
        assert!(verify_ocert(&cold.vkey(), &[0u8; 32], counter, period, &sigma).is_err());
        let other = ColdKey::from_seed(&[99u8; 32]);
        assert!(verify_ocert(&other.vkey(), &hot, counter, period, &sigma).is_err());
    }

    #[test]
    fn cold_key_is_deterministic() {
        let a = ColdKey::from_seed(&[7u8; 32]);
        let b = ColdKey::from_seed(&[7u8; 32]);
        assert_eq!(a.vkey(), b.vkey());
    }
}
