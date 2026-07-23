//! Cardano consensus value types.
//!
//! Protocol identity types — what a "block" is named on the chain —
//! not transport types. Wire codecs live with the type because every
//! consumer must agree on the on-wire encoding, but the type itself
//! carries no I/O.

use std::fmt;

use minicbor::decode::Error as DecodeError;
use minicbor::encode::Error as EncodeError;
use minicbor::{Decoder, Encoder};

/// A point on the chain: either the genesis (origin) or a specific slot+hash.
///
/// Wire format:
///   origin   = []                    (empty array)
///   specific = [slotNo, headerHash]  (2-element array)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Point {
    Origin,
    Specific { slot: u64, hash: [u8; 32] },
}

impl fmt::Display for Point {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Point::Origin => write!(f, "origin"),
            Point::Specific { slot, hash } => {
                write!(f, "{}/{}", slot, hex_prefix(hash))
            }
        }
    }
}

impl Point{
    pub fn get_slot(&self) -> Option<u64> {
        match self {
            Point::Origin => None,
            Point::Specific { slot, .. } => Some(*slot),
        }
    }

    pub fn get_hash(&self) -> Option<[u8; 32]> {
        match self {
            Point::Origin => None,
            Point::Specific { hash, .. } => Some(*hash),
        }
    }
}

impl minicbor::Encode<()> for Point {
    fn encode<W: minicbor::encode::Write>(
        &self,
        e: &mut Encoder<W>,
        _ctx: &mut (),
    ) -> Result<(), EncodeError<W::Error>> {
        match self {
            Point::Origin => {
                e.array(0)?;
            }
            Point::Specific { slot, hash } => {
                e.array(2)?;
                e.u64(*slot)?;
                e.bytes(hash)?;
            }
        }
        Ok(())
    }
}

impl<'a> minicbor::Decode<'a, ()> for Point {
    fn decode(d: &mut Decoder<'a>, _ctx: &mut ()) -> Result<Self, DecodeError> {
        let len = d.array()?;
        match len {
            Some(0) => Ok(Point::Origin),
            None => {
                // Indefinite-length array — check if immediately closed (origin)
                // or has elements (specific).
                if d.datatype()? == minicbor::data::Type::Break {
                    d.skip()?; // consume the break
                    return Ok(Point::Origin);
                }
                let slot = d.u64()?;
                let hash_bytes = d.bytes()?;
                if hash_bytes.len() != 32 {
                    return Err(DecodeError::message(format!(
                        "point hash must be 32 bytes, got {}",
                        hash_bytes.len()
                    )));
                }
                let mut hash = [0u8; 32];
                hash.copy_from_slice(hash_bytes);
                // Consume the break marker.
                if d.datatype()? == minicbor::data::Type::Break {
                    d.skip()?;
                }
                Ok(Point::Specific { slot, hash })
            }
            Some(2) => {
                let slot = d.u64()?;
                let hash_bytes = d.bytes()?;
                if hash_bytes.len() != 32 {
                    return Err(DecodeError::message(format!(
                        "point hash must be 32 bytes, got {}",
                        hash_bytes.len()
                    )));
                }
                let mut hash = [0u8; 32];
                hash.copy_from_slice(hash_bytes);
                Ok(Point::Specific { slot, hash })
            }
            Some(other) => Err(DecodeError::message(format!(
                "expected point array of length 0 or 2, got {other}"
            ))),
        }
    }
}

/// Chain tip: a point plus the block number.
///
/// Wire format: [point, blockNo]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tip {
    pub point: Point,
    pub block_no: u64,
}

impl fmt::Display for Tip {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@block#{}", self.point, self.block_no)
    }
}

impl minicbor::Encode<()> for Tip {
    fn encode<W: minicbor::encode::Write>(
        &self,
        e: &mut Encoder<W>,
        _ctx: &mut (),
    ) -> Result<(), EncodeError<W::Error>> {
        e.array(2)?;
        self.point.encode(e, &mut ())?;
        e.u64(self.block_no)?;
        Ok(())
    }
}

impl<'a> minicbor::Decode<'a, ()> for Tip {
    fn decode(d: &mut Decoder<'a>, _ctx: &mut ()) -> Result<Self, DecodeError> {
        let _len = d.array()?;
        let point = Point::decode(d, &mut ())?;
        let block_no = d.u64()?;
        Ok(Tip { point, block_no })
    }
}

/// A Leios vote, delivered inline (no offer/fetch round-trip).
///
/// Mirrors the current LeiosNotify wire shape
/// (`vote = [announcing_rb_hash: hash32, voter_id: word16,
/// vote_signature: bytes .size 48]`):
///
/// - `announcing_rb_hash` — hash of the ranking block (RB) that
///   announced the endorser block being voted on. Identifying the vote
///   target by the *announcing RB* rather than the EB hash binds the
///   vote to a single chain position + issuer: an equivocated
///   announcement (same EB offered under two RB headers) yields two
///   distinct vote targets, so a vote can't be replayed across the fork.
/// - `voter_id` — compact voter index; resolves to a registered pool
///   via the deterministic voter registry (see `committee.rs`).
/// - `vote_signature` — BLS signature bytes (48 bytes in the live
///   deployment; length not validated by this prototype).
///
/// The vote no longer carries an explicit slot: retention/pruning is
/// derived from the announcing RB (see the network store), and the
/// election id is resolved from the RB, not carried on the wire.
///
/// The CBOR codec lives in the network I/O layer (this crate stays
/// format-agnostic); this is the logical value every consumer agrees on.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Vote {
    pub announcing_rb_hash: [u8; 32],
    pub voter_id: u16,
    pub vote_signature: Vec<u8>,
}

/// Hex-encode the first 8 bytes of a hash for display.
pub fn hex_prefix(hash: &[u8]) -> String {
    hash.iter()
        .take(8)
        .map(|b| format!("{b:02x}"))
        .collect::<String>()
}

pub fn short_hash(h: &[u8; 32]) -> String {
    format!("{:02x}{:02x}", h[30], h[31])
}

/// Decode a hex string into a fixed-size byte array.
///
/// Expects exactly `LEN * 2` ASCII hex digits (two per byte). Returns `Err`
/// if the input has the wrong length or contains a non-hex character.
pub fn hash_from_hex<const LEN: usize>(s: &str) -> Result<[u8; LEN], String> {
    if s.len() != LEN*2 {
        return Err(format!("expected hex string of length {}, got {}", LEN*2, s.len()));
    }
    let mut hash = [0u8; LEN];
    for (i, pair) in s.as_bytes().chunks_exact(2).enumerate() {
        let hex = std::str::from_utf8(pair).map_err(|e| format!("invalid UTF-8 in hex string: {}", e))?;
        hash[i] = u8::from_str_radix(hex, 16).map_err(|e| format!("invalid hex digit in string: {}", e))?;
    }
    Ok(hash)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_origin_round_trip() {
        let point = Point::Origin;
        let encoded = minicbor::to_vec(&point).unwrap();
        assert_eq!(encoded, &[0x80]); // definite empty array
        let decoded: Point = minicbor::decode(&encoded).unwrap();
        assert_eq!(decoded, point);
    }

    #[test]
    fn point_specific_round_trip() {
        let point = Point::Specific {
            slot: 12345,
            hash: [0xAB; 32],
        };
        let encoded = minicbor::to_vec(&point).unwrap();
        let decoded: Point = minicbor::decode(&encoded).unwrap();
        assert_eq!(decoded, point);
    }

    #[test]
    fn point_specific_rejects_wrong_hash_length() {
        // Manually constructed [slot, 16-byte-hash] should fail.
        let mut bytes = Vec::new();
        let mut e = Encoder::new(&mut bytes);
        e.array(2).unwrap();
        e.u64(1).unwrap();
        e.bytes(&[0u8; 16]).unwrap();
        assert!(minicbor::decode::<Point>(&bytes).is_err());
    }

    #[test]
    fn point_display() {
        assert_eq!(format!("{}", Point::Origin), "origin");
        let p = Point::Specific {
            slot: 42,
            hash: [0xAB; 32],
        };
        assert_eq!(format!("{p}"), "42/abababababababab");
    }

    #[test]
    fn hash_from_hex_round_trip() {
        let hash = [0xABu8; 32];
        // Full hex encoding of the hash (64 chars).
        let hex: String = hash.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(hash_from_hex(&hex), Ok(hash));
        // Uppercase hex is accepted too.
        assert_eq!(hash_from_hex(&hex.to_uppercase()), Ok(hash));
    }

    #[test]
    fn hash_from_hex_rejects_malformed() {
        // Wrong length.
        assert_eq!(hash_from_hex::<32>("deadbeef").is_err(), true);
        assert_eq!(hash_from_hex::<32>(&"a".repeat(63)).is_err(), true);
        // Correct length but a non-hex character.
        let bad = format!("{}z", "a".repeat(63));
        assert_eq!(hash_from_hex::<32>(&bad).is_err(), true);
    }
}
