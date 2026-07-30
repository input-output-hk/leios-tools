//! Honest Cardano ledger-object CBOR codec for the Leios network stack.
//!
//! This crate owns the wire (de)serialization of Cardano *ledger objects* —
//! era-tagged headers and block bodies, plus the chain-position helpers
//! (`Point`/`Tip` and the `FindIntersect` point array). It is deliberately
//! separate from net-core's *mini-protocol* framing: this is the "what the
//! bytes mean" layer, not the "how messages are multiplexed" layer.
//!
//! - `Point` and `Tip`: chain position types used by ChainSync and BlockFetch
//!   (canonical codec lives in `shared-consensus`; re-exported here).
//! - `WrappedHeader` (in `header`): era-tagged block headers with optional parsed fields.
//! - `BlockBody` (in `block`): raw block bodies with optional Leios metadata.
//!
//! The header/block parsers take untrusted wire input; every length read from
//! the wire is bounded before allocation (see `MAX_*` constants and the
//! per-decoder checks) per the net-rs "Security audit" discipline.

mod block;
mod eb;
mod encode;
mod header;

pub use block::{BlockBody, LeiosBlockInfo};
pub use eb::{decode_overflow_eb, encode_overflow_eb};
pub use encode::{
    encode_block_body, encode_header_inner, wrap_block, wrap_header, HeaderBody, OperationalCert,
    DIJKSTRA_BLOCK_ERA, DIJKSTRA_HEADER_ERA,
};
pub use header::{HeaderInfo, WrappedHeader};

/// Blake2b-256 of arbitrary bytes — matches the EB-key derivation used across
/// the wire and the `tx_from_received_bytes` tx-id derivation.
pub fn blake2b_256(bytes: &[u8]) -> [u8; 32] {
    let result = blake2b_simd::Params::new().hash_length(32).hash(bytes);
    let mut out = [0u8; 32];
    out.copy_from_slice(result.as_bytes());
    out
}

/// Canonical Cardano transaction id for a full wire transaction.
///
/// In Cardano the `TxId` is Blake2b-256 of the **transaction body only** — the
/// first element of the serialized transaction `[body, witness_set, is_valid,
/// auxiliary_data]` — *not* the hash of the whole tx. LeiosFetch (and N2N
/// TxSubmission) deliver the full tx, so hashing the whole blob yields a
/// non-canonical id that no other node will recognise (it fails to match the
/// EB manifest and trips `ProtocolErrorTxNotRequested` on re-announce).
///
/// This extracts the raw CBOR bytes of the body element (verbatim, as they were
/// serialized) and hashes those. Returns `None` if `tx` isn't a CBOR array with
/// at least one element.
pub fn wire_tx_id(tx: &[u8]) -> Option<[u8; 32]> {
    let mut d = minicbor::Decoder::new(tx);
    // Outer transaction array: `[body, witness_set, is_valid, aux_data]`
    // (fixed- or indefinite-length; we only need to step past the header).
    d.array().ok()?;
    let start = d.position();
    d.skip().ok()?; // skip the transaction_body element
    let end = d.position();
    tx.get(start..end).map(blake2b_256)
}

use minicbor::decode::Error as DecodeError;
use minicbor::encode::Error as EncodeError;
use minicbor::{Decode, Decoder, Encode, Encoder};

pub use shared_consensus::{Point, Tip, Vote};

/// Maximum number of points in a FindIntersect message.
pub const MAX_POINTS: usize = 2048;

/// Maximum header size (matches ChainSync per-state size limit).
pub const MAX_HEADER_SIZE: usize = 65_535;

/// Maximum block body size (matches BlockFetch StStreaming size limit).
pub const MAX_BLOCK_SIZE: usize = 2_500_000;

// --- Helpers ---

/// Decode an array of points, handling both definite and indefinite length.
/// Enforces MAX_POINTS to prevent unbounded allocation.
pub fn decode_points(d: &mut Decoder<'_>) -> Result<Vec<Point>, DecodeError> {
    let len = d.array()?;
    match len {
        Some(n) => {
            if n as usize > MAX_POINTS {
                return Err(DecodeError::message(format!(
                    "points array has {n} entries, maximum is {MAX_POINTS}"
                )));
            }
            let mut points = Vec::with_capacity(n as usize);
            for _ in 0..n {
                points.push(Point::decode(d, &mut ())?);
            }
            Ok(points)
        }
        None => {
            // Indefinite-length array.
            let mut points = Vec::new();
            loop {
                if d.datatype()? == minicbor::data::Type::Break {
                    d.skip()?; // consume the break
                    break;
                }
                if points.len() >= MAX_POINTS {
                    return Err(DecodeError::message(format!(
                        "points array exceeds maximum of {MAX_POINTS}"
                    )));
                }
                points.push(Point::decode(d, &mut ())?);
            }
            Ok(points)
        }
    }
}

/// Encode an array of points as a definite-length CBOR array.
pub fn encode_points<W: minicbor::encode::Write>(
    e: &mut Encoder<W>,
    points: &[Point],
) -> Result<(), EncodeError<W::Error>> {
    e.array(points.len() as u64)?;
    for point in points {
        point.encode(e, &mut ())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_tx_id_hashes_body_element_only() {
        // A minimal full tx: [body, witness_set, is_valid, aux_data].
        // body element is itself a CBOR map {0: [], 1: []} here — the exact
        // shape doesn't matter, only that it's element 0.
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(4).unwrap();
        // element 0: transaction_body
        e.map(2).unwrap();
        e.u8(0).unwrap().array(0).unwrap();
        e.u8(1).unwrap().array(0).unwrap();
        // element 1: witness_set
        e.map(0).unwrap();
        // element 2: is_valid
        e.bool(true).unwrap();
        // element 3: aux_data
        e.null().unwrap();
        let tx = e.into_writer();

        // Independently serialize just the body to get its raw bytes.
        let mut be = minicbor::Encoder::new(Vec::new());
        be.map(2).unwrap();
        be.u8(0).unwrap().array(0).unwrap();
        be.u8(1).unwrap().array(0).unwrap();
        let body = be.into_writer();

        let id = wire_tx_id(&tx).expect("valid tx");
        assert_eq!(id, blake2b_256(&body), "txid must hash the body element");
        assert_ne!(
            id,
            blake2b_256(&tx),
            "txid must NOT hash the whole tx (the bug we're fixing)"
        );
    }

    #[test]
    fn wire_tx_id_rejects_non_array() {
        assert!(wire_tx_id(&[0x01]).is_none()); // a bare int, not a tx array
    }

    #[test]
    fn point_origin_round_trip() {
        let point = Point::Origin;
        let encoded = minicbor::to_vec(&point).unwrap();
        assert_eq!(encoded, &[0x80]); // definite empty array
        let decoded: Point = minicbor::decode(&encoded).unwrap();
        assert_eq!(decoded, point);
    }

    #[test]
    fn point_origin_indefinite_decode() {
        // Indefinite-length empty array: 0x9f 0xff
        let cbor = &[0x9f, 0xff];
        let decoded: Point = minicbor::decode(cbor).unwrap();
        assert_eq!(decoded, Point::Origin);
    }

    #[test]
    fn point_specific_round_trip() {
        let hash = [0xab; 32];
        let point = Point::Specific { slot: 12345, hash };
        let encoded = minicbor::to_vec(&point).unwrap();
        let decoded: Point = minicbor::decode(&encoded).unwrap();
        assert_eq!(decoded, point);
    }

    #[test]
    fn point_bad_hash_length() {
        // Encode a point with a 16-byte hash (too short).
        let mut buf = Vec::new();
        let mut e = minicbor::Encoder::new(&mut buf);
        e.array(2).unwrap();
        e.u64(100).unwrap();
        e.bytes(&[0u8; 16]).unwrap();
        let result: Result<Point, _> = minicbor::decode(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn point_display() {
        assert_eq!(format!("{}", Point::Origin), "origin");
        let hash = [0xab; 32];
        let p = Point::Specific { slot: 42, hash };
        assert_eq!(format!("{p}"), "42/abababababababab");
    }

    #[test]
    fn tip_round_trip() {
        let tip = Tip {
            point: Point::Specific {
                slot: 999,
                hash: [0x01; 32],
            },
            block_no: 500,
        };
        let encoded = minicbor::to_vec(&tip).unwrap();
        let decoded: Tip = minicbor::decode(&encoded).unwrap();
        assert_eq!(decoded, tip);
    }

    #[test]
    fn tip_origin_round_trip() {
        let tip = Tip {
            point: Point::Origin,
            block_no: 0,
        };
        let encoded = minicbor::to_vec(&tip).unwrap();
        let decoded: Tip = minicbor::decode(&encoded).unwrap();
        assert_eq!(decoded, tip);
    }

    #[test]
    fn decode_points_definite() {
        let p1 = Point::Origin;
        let p2 = Point::Specific {
            slot: 10,
            hash: [0xff; 32],
        };
        let mut buf = Vec::new();
        let mut e = minicbor::Encoder::new(&mut buf);
        encode_points(&mut e, &[p1.clone(), p2.clone()]).unwrap();

        let mut d = minicbor::Decoder::new(&buf);
        let points = decode_points(&mut d).unwrap();
        assert_eq!(points, vec![p1, p2]);
    }

    #[test]
    fn decode_points_indefinite() {
        // Build an indefinite-length array of one origin point.
        let buf = vec![
            0x9f, // begin indefinite array
            0x80, // origin point (empty definite array)
            0xff, // break
        ];

        let mut d = minicbor::Decoder::new(&buf);
        let points = decode_points(&mut d).unwrap();
        assert_eq!(points, vec![Point::Origin]);
    }

    #[test]
    fn decode_points_oversized_rejected() {
        // Craft an array claiming MAX_POINTS + 1 elements.
        let mut buf = Vec::new();
        let mut e = minicbor::Encoder::new(&mut buf);
        e.array((MAX_POINTS + 1) as u64).unwrap();
        // Don't need to write actual points — length check happens first.

        let mut d = minicbor::Decoder::new(&buf);
        let result = decode_points(&mut d);
        assert!(result.is_err());
    }
}
