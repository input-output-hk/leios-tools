//! Endorser-block "overflow" manifest codec (CIP-0164 prototype).
//!
//! The prototype encodes an EB body as a CBOR map `endorser_block =
//! { tx_hash => tx_size }`, where the key is a 32-byte transaction hash and
//! the value is the transaction's byte size. This is honest wire (de)coding:
//! the producer builds the manifest from its mempool, hashes the bytes to
//! derive the EB key, and downstream nodes decode it to recover the referenced
//! tx hash set in wire order (sizes are ignored — the consumer only needs the
//! hash set + order for bitmap indexing).

use crate::blake2b_256;
use shared_consensus::mempool::{EbKey, TxId};
use shared_consensus::Point;

/// Maximum number of tx-hash entries accepted in an overflow EB manifest.
///
/// The blob is untrusted wire data: a CBOR map header can claim an arbitrary
/// entry count, so we must bound it before reserving/allocating (otherwise a
/// 9-byte blob claiming `2^64-1` entries triggers a capacity-overflow panic).
/// Mirrors net-core's `leios_fetch::MAX_TRANSACTIONS` — an EB manifest cannot
/// reference more transactions than a LeiosFetch response can carry. Defined
/// locally because net-codec is below net-core in the layering and cannot
/// depend on it.
pub const MAX_OVERFLOW_EB_TXS: usize = 65_536;

/// Decode an `endorser_block = { tx_hash => tx_size }` manifest map, returning
/// the referenced tx hashes in wire order. Returns `None` if the blob isn't a
/// well-formed manifest map (32-byte keys, integer values) or declares more
/// than [`MAX_OVERFLOW_EB_TXS`] entries. Sizes are ignored.
pub fn decode_overflow_eb(point: &Point, blob: &[u8]) -> Option<(Option<EbKey>, Vec<TxId>)> {
    fn read_hash(dec: &mut minicbor::Decoder) -> Option<TxId> {
        let bytes = dec.bytes().ok()?;
        if bytes.len() != 32 {
            return None;
        }
        let mut h = [0u8; 32];
        h.copy_from_slice(bytes);
        let _size = dec.u32().ok()?; // tx size — unused
        Some(TxId::new_with_array(h))
    }
    let mut dec = minicbor::Decoder::new(blob);
    let entries = dec.map().ok()?;
    let mut hashes = Vec::new();
    match entries {
        Some(n) => {
            // Bound the declared length before reserving — `n` is attacker
            // controlled and unrelated to the actual blob size.
            if n as usize > MAX_OVERFLOW_EB_TXS {
                return None;
            }
            hashes.reserve(n as usize);
            for _ in 0..n {
                hashes.push(read_hash(&mut dec)?);
            }
        }
        None => loop {
            // Indefinite-length map: read until the break marker, capping the
            // entry count so a never-terminating map can't grow without bound.
            if dec.datatype().ok()? == minicbor::data::Type::Break {
                dec.skip().ok()?;
                break;
            }
            if hashes.len() >= MAX_OVERFLOW_EB_TXS {
                return None;
            }
            hashes.push(read_hash(&mut dec)?);
        },
    }

    let eb_hash = blake2b_256(blob);
    let eb_key = match point {
        Point::Origin => None,
        Point::Specific { slot, .. } => Some(EbKey::new(*slot, eb_hash)),
    };
    Some((eb_key, hashes))
}

/// Encode an endorser-block manifest as the prototype
/// `endorser_block = { tx_hash => tx_size }` map. Sizes are `0` (the produce
/// path doesn't track per-tx sizes); the hash order is preserved for bitmap
/// indexing. Pure over the manifest so the caller can hash the bytes to derive
/// the EB key before committing the mempool drain.
pub fn encode_overflow_eb(manifest: &[TxId]) -> Vec<u8> {
    let mut data = Vec::new();
    let mut enc = minicbor::Encoder::new(&mut data);
    let _ = enc.map(manifest.len() as u64);
    for h in manifest {
        let _ = enc.bytes(h.get_bytes()).and_then(|e| e.u32(0));
    }
    data
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blake2b_256;

    const BLANK_POINT: Point = Point::Specific {
        slot: 0,
        hash: [0u8; 32],
    };

    #[test]
    fn encode_overflow_eb_is_deterministic() {
        let manifest = vec![
            TxId::new_with_array([0x10u8; 32]),
            TxId::new_with_array([0x20u8; 32]),
        ];
        let a = encode_overflow_eb(&manifest);
        let b = encode_overflow_eb(&manifest);
        assert_eq!(a, b);
        assert_eq!(blake2b_256(&a), blake2b_256(&b));
    }

    #[test]
    fn decode_overflow_eb_round_trip() {
        let manifest = vec![
            TxId::new_with_array([0x10u8; 32]),
            TxId::new_with_array([0x20u8; 32]),
        ];
        let data = encode_overflow_eb(&manifest);
        let (_eb_key, hashes) = decode_overflow_eb(&BLANK_POINT, &data).expect("decode");
        assert_eq!(hashes, manifest);
    }

    #[test]
    fn decode_overflow_eb_rejects_garbage() {
        assert!(decode_overflow_eb(&BLANK_POINT, &[0xFF, 0xFF]).is_none());
        assert!(decode_overflow_eb(&BLANK_POINT, &[]).is_none());
    }

    #[test]
    fn decode_overflow_eb_rejects_oversized_length() {
        // A definite-length map header claiming 2^64-1 entries must be
        // rejected before any reserve/allocation (no panic, no OOM).
        // `bb` = map(uint64 length), followed by 8x 0xff.
        let blob = [0xbb, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];
        assert!(decode_overflow_eb(&BLANK_POINT, &blob).is_none());
    }

    #[test]
    fn encode_overflow_eb_layout() {
        let manifest = vec![
            TxId::new_with_array([0xAAu8; 32]),
            TxId::new_with_array([0xBBu8; 32]),
        ];
        let data = encode_overflow_eb(&manifest);
        // Decode the manifest map: { hash => size }, hashes in order.
        let mut dec = minicbor::Decoder::new(&data);
        let n = dec.map().unwrap().unwrap();
        assert_eq!(n, 2);
        assert_eq!(dec.bytes().unwrap(), &[0xAA; 32]);
        assert_eq!(dec.u32().unwrap(), 0);
        assert_eq!(dec.bytes().unwrap(), &[0xBB; 32]);
        assert_eq!(dec.u32().unwrap(), 0);
    }
}
