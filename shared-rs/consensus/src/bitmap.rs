//! Sparse `BTreeMap<u16, u64>` bitmap used by `MsgLeiosBlockTxsRequest`.
//!
//! Per CIP-0164: each entry maps a 16-bit segment index to a 64-bit
//! mask, and the offset of the mask's **first bit** is `64 * segment`.
//! The "first bit" is the **most-significant** bit (bit 63): transaction
//! `i` is selected iff bit `63 - (i % 64)` of `bitmap[i / 64]` is set —
//! i.e. bits are numbered **MSB-first** within each 64-bit segment, so
//! the lowest tx index in a segment occupies the high bit. This matches
//! the Haskell reference node on the wire (an earlier LSB-first reading
//! here silently mis-served every `MsgLeiosBlockTxsRequest`: a request
//! for txs `0..8` arrives as mask `0xFF00_0000_0000_0000`, which the
//! LSB reading decoded as indices `56..63`). Empty bitmap selects
//! nothing; use [`select_all`] for "every tx".
//!
//! Lives in shared-consensus because the trait surface
//! ([`crate::fetch::EbTxsFetchPolicy::pick`] and friends) already speaks
//! the encoded `BTreeMap<u16, u64>` form.  Net-core re-exports these
//! helpers as `net_core::protocols::leios_fetch::bitmap` for wire-codec
//! callers.

use std::collections::BTreeMap;

/// Number of bits per segment. Absolute index `64*seg + i` maps to
/// segment `seg`, bit `63 - i` (MSB-first — see the module docs).
const SEGMENT_BITS: u32 = 64;

/// The segment bit that carries absolute `index` (MSB-first within the
/// segment: index `64*seg` is bit 63, `64*seg + 63` is bit 0).
#[inline]
fn seg_bit(index: u32) -> u32 {
    (SEGMENT_BITS - 1) - (index % SEGMENT_BITS)
}

/// Build a sparse bitmap with the given indices set.
pub fn from_indices(indices: &[u32]) -> BTreeMap<u16, u64> {
    let mut bitmap: BTreeMap<u16, u64> = BTreeMap::new();
    for &index in indices {
        let segment = (index / SEGMENT_BITS) as u16;
        let entry = bitmap.entry(segment).or_insert(0);
        *entry |= 1u64 << seg_bit(index);
    }
    bitmap
}

/// Build a bitmap with indices `0..count` set (every tx selected).
pub fn select_all(count: u32) -> BTreeMap<u16, u64> {
    let mut bitmap: BTreeMap<u16, u64> = BTreeMap::new();
    let full_segments = count / SEGMENT_BITS;
    let remainder = count % SEGMENT_BITS;
    for seg in 0..full_segments {
        bitmap.insert(seg as u16, u64::MAX);
    }
    if remainder > 0 {
        // MSB-first: the first `remainder` indices are the top `remainder`
        // bits. e.g. remainder 8 -> 0xFF00_0000_0000_0000.
        let mask = !((1u64 << (SEGMENT_BITS - remainder)) - 1);
        bitmap.insert(full_segments as u16, mask);
    }
    bitmap
}

/// True iff `index` is selected by the bitmap.
pub fn contains(bitmap: &BTreeMap<u16, u64>, index: u32) -> bool {
    let segment = (index / SEGMENT_BITS) as u16;
    bitmap
        .get(&segment)
        .map(|mask| mask & (1u64 << seg_bit(index)) != 0)
        .unwrap_or(false)
}

/// Iterate the set indices in ascending order.
pub fn iter_indices(bitmap: &BTreeMap<u16, u64>) -> impl Iterator<Item = u32> + '_ {
    bitmap.iter().flat_map(|(&segment, &mask)| {
        let base = segment as u32 * SEGMENT_BITS;
        // MSB-first: bit 63 is index 0 of the segment. Walk bits high→low
        // so the yielded absolute indices ascend.
        (0..SEGMENT_BITS).rev().filter_map(move |bit| {
            if mask & (1u64 << bit) != 0 {
                Some(base + (SEGMENT_BITS - 1 - bit))
            } else {
                None
            }
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_indices_packs_bits_into_segments() {
        // MSB-first: index 0 -> bit 63, index 1 -> bit 62, index 63 -> bit 0.
        let bitmap = from_indices(&[0, 1, 63, 64, 65, 200]);
        assert_eq!(bitmap[&0], (1u64 << 63) | (1u64 << 62) | (1u64 << 0));
        assert_eq!(bitmap[&1], (1u64 << 63) | (1u64 << 62));
        // index 200 -> segment 3, bit 63 - (200 % 64) = 63 - 8 = 55.
        assert_eq!(bitmap[&3], 1u64 << 55);
        assert_eq!(bitmap.len(), 3);
    }

    #[test]
    fn from_indices_empty_input_empty_bitmap() {
        assert!(from_indices(&[]).is_empty());
    }

    #[test]
    fn from_indices_duplicate_index_is_idempotent() {
        let a = from_indices(&[5, 5, 5]);
        let b = from_indices(&[5]);
        assert_eq!(a, b);
    }

    #[test]
    fn select_all_zero_is_empty() {
        assert!(select_all(0).is_empty());
    }

    #[test]
    fn select_all_partial_segment_uses_high_bits() {
        // MSB-first: the first 3 indices occupy the top 3 bits.
        let bitmap = select_all(3);
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap[&0], (1u64 << 63) | (1u64 << 62) | (1u64 << 61));
    }

    #[test]
    fn select_all_exact_segment_boundary() {
        let bitmap = select_all(64);
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap[&0], u64::MAX);
    }

    #[test]
    fn select_all_multi_segment() {
        let bitmap = select_all(130);
        assert_eq!(bitmap.len(), 3);
        assert_eq!(bitmap[&0], u64::MAX);
        assert_eq!(bitmap[&1], u64::MAX);
        // MSB-first: the 2 remaining indices are the top 2 bits.
        assert_eq!(bitmap[&2], (1u64 << 63) | (1u64 << 62));
    }

    #[test]
    fn contains_matches_selection() {
        let bitmap = from_indices(&[0, 63, 64, 200]);
        assert!(contains(&bitmap, 0));
        assert!(contains(&bitmap, 63));
        assert!(contains(&bitmap, 64));
        assert!(contains(&bitmap, 200));
        assert!(!contains(&bitmap, 1));
        assert!(!contains(&bitmap, 65));
        assert!(!contains(&bitmap, 199));
        assert!(!contains(&bitmap, 1000));
    }

    #[test]
    fn iter_indices_returns_indices_in_order() {
        let inputs = vec![200u32, 1, 64, 0, 63, 65];
        let bitmap = from_indices(&inputs);
        let collected: Vec<u32> = iter_indices(&bitmap).collect();
        assert_eq!(collected, vec![0, 1, 63, 64, 65, 200]);
    }

    #[test]
    fn iter_indices_round_trips_through_from_indices() {
        let original: Vec<u32> = (0..150).filter(|i| i % 7 == 0).collect();
        let bitmap = from_indices(&original);
        let recovered: Vec<u32> = iter_indices(&bitmap).collect();
        assert_eq!(recovered, original);
    }

    #[test]
    fn select_all_then_iter_yields_all() {
        let bitmap = select_all(70);
        let collected: Vec<u32> = iter_indices(&bitmap).collect();
        let expected: Vec<u32> = (0..70).collect();
        assert_eq!(collected, expected);
    }

    #[test]
    fn empty_bitmap_iterates_to_nothing() {
        let bitmap: BTreeMap<u16, u64> = BTreeMap::new();
        assert_eq!(iter_indices(&bitmap).count(), 0);
    }
}
