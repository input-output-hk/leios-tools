//! CBOR encoding for LeiosFetch messages (CIP-0164 prototype,
//! cardano-blueprint `leios-prototype`).
//!
//! Wire format:
//!   msgLeiosBlockRequest    = [0, point]              point = [slot, hash32]
//!   msgLeiosBlock           = [1, endorser_block]     endorser_block = { hash => word32 }
//!   msgLeiosBlockTxsRequest = [2, point, bitmaps]     bitmaps = { word16 => word64 }
//!   msgLeiosBlockTxs        = [3, point, bitmaps, tx_list]   tx_list = [ *tx ]
//!   msgClientDone           = [9]
//!
//! `endorser_block` and each `tx` are carried as raw CBOR (opaque
//! pass-through): the codec splices/captures their bytes verbatim, so
//! the exact map/tx shape lives with the consumer, not here.

use std::collections::BTreeMap;

use super::{Message, MAX_BITMAP_ENTRIES, MAX_BLOCK_SIZE, MAX_TRANSACTIONS, MAX_TRANSACTION_SIZE};
use crate::types::Point;
use minicbor::decode::Error as DecodeError;
use minicbor::encode::Error as EncodeError;
use minicbor::{Decoder, Encoder};
use shared_consensus::mempool::TxBody;

impl minicbor::Encode<()> for Message {
    fn encode<W: minicbor::encode::Write>(
        &self,
        e: &mut Encoder<W>,
        _ctx: &mut (),
    ) -> Result<(), EncodeError<W::Error>> {
        match self {
            Message::MsgLeiosBlockRequest { point } => {
                e.array(2)?;
                e.u32(0)?;
                minicbor::Encode::encode(point, e, &mut ())?;
            }
            Message::MsgLeiosBlock { block } => {
                e.array(2)?;
                e.u32(1)?;
                // `block` is the raw CBOR `endorser_block` map — splice verbatim.
                encode_raw(e, block)?;
            }
            Message::MsgLeiosBlockTxsRequest { point, bitmap } => {
                e.array(3)?;
                e.u32(2)?;
                minicbor::Encode::encode(point, e, &mut ())?;
                encode_bitmap(e, bitmap)?;
            }
            Message::MsgLeiosBlockTxs {
                point,
                bitmap,
                transactions,
            } => {
                e.array(4)?;
                e.u32(3)?;
                minicbor::Encode::encode(point, e, &mut ())?;
                encode_bitmap(e, bitmap)?;
                encode_tx_list(e, transactions)?;
            }
            Message::MsgDone => {
                e.array(1)?;
                e.u32(9)?;
            }
        }
        Ok(())
    }
}

impl<'a> minicbor::Decode<'a, ()> for Message {
    fn decode(d: &mut Decoder<'a>, _ctx: &mut ()) -> Result<Self, DecodeError> {
        let _array_len = d.array()?;
        let tag = d.u32()?;

        match tag {
            0 => {
                let point = Point::decode(d, &mut ())?;
                Ok(Message::MsgLeiosBlockRequest { point })
            }
            1 => {
                // endorser_block is a CBOR map { tx_hash => tx_size };
                // captured as raw bytes for opaque pass-through.
                let block = decode_raw_bounded(d, MAX_BLOCK_SIZE, "endorser block")?;
                Ok(Message::MsgLeiosBlock { block })
            }
            2 => {
                let point = Point::decode(d, &mut ())?;
                let bitmap = decode_bitmap(d)?;
                Ok(Message::MsgLeiosBlockTxsRequest { point, bitmap })
            }
            3 => {
                let point = Point::decode(d, &mut ())?;
                let bitmap = decode_bitmap(d)?;
                let transactions = decode_tx_list(d)?;
                Ok(Message::MsgLeiosBlockTxs {
                    point,
                    bitmap,
                    transactions,
                })
            }
            9 => Ok(Message::MsgDone),
            other => Err(DecodeError::message(format!(
                "unknown leios_fetch message tag: {other}"
            ))),
        }
    }
}

// --- Encode helpers ---

fn encode_bitmap<W: minicbor::encode::Write>(
    e: &mut Encoder<W>,
    bitmap: &BTreeMap<u16, u64>,
) -> Result<(), EncodeError<W::Error>> {
    // Indefinite-length map per the leios-prototype CDDL:
    //   bitmaps = { * base.word16 => base.word64 }  ; indefinite-length
    // The prototype relay's parser rejects the definite-length form
    // and immediately RSTs the bearer on receipt.
    e.begin_map()?;
    for (index, bits) in bitmap {
        e.u16(*index)?;
        e.u64(*bits)?;
    }
    e.end()?;
    Ok(())
}

/// Encode a tx list `[ tx, ... ]`, encoding each tx into CBOR bytes string.
fn encode_tx_list<W: minicbor::encode::Write>(
    e: &mut Encoder<W>,
    txs: &[TxBody],
) -> Result<(), EncodeError<W::Error>> {
    e.array(txs.len() as u64)?;
    for tx in txs {
        e.bytes(tx.get_slice())?;
    }
    Ok(())
}

/// Splice a raw, already-CBOR-encoded value into the output verbatim.
fn encode_raw<W: minicbor::encode::Write>(
    e: &mut Encoder<W>,
    raw: &[u8],
) -> Result<(), EncodeError<W::Error>> {
    e.writer_mut().write_all(raw).map_err(EncodeError::write)
}

// --- Decode helpers ---

fn check_max_size(raw: &[u8], max_size: usize, name: &str) -> Result<(), DecodeError> {
    if raw.len() > max_size {
        return Err(DecodeError::message(format!(
            "{name} is {} bytes, maximum is {max_size}",
            raw.len()
        )));
    }
    Ok(())
}

/// Capture the next CBOR value's raw bytes verbatim (opaque pass-through),
/// enforcing a size bound.
fn decode_raw_bounded(
    d: &mut Decoder<'_>,
    max_size: usize,
    name: &str,
) -> Result<Vec<u8>, DecodeError> {
    let start = d.position();
    d.skip()?;
    let raw = &d.input()[start..d.position()];
    check_max_size(raw, max_size, name)?;
    Ok(raw.to_vec())
}

fn decode_raw_tx_body(d: &mut Decoder<'_>) -> Result<TxBody, DecodeError> {
    let raw = d.bytes()?;
    check_max_size(raw, MAX_TRANSACTION_SIZE, "transaction body")?;
    Ok(TxBody::new_with_slice(raw))
}

/// Decode a tx list, converting each tx's CBOR to bytes (count- and
/// size-bounded).
fn decode_tx_list(d: &mut Decoder<'_>) -> Result<Vec<TxBody>, DecodeError> {
    let len = d.array()?;
    match len {
        Some(n) => {
            let n = n as usize;
            if n > MAX_TRANSACTIONS {
                return Err(DecodeError::message(format!(
                    "transaction list has {n} entries, maximum is {MAX_TRANSACTIONS}"
                )));
            }
            let mut items = Vec::with_capacity(n);
            for _ in 0..n {
                items.push(decode_raw_tx_body(d)?);
            }
            Ok(items)
        }
        None => {
            let mut items = Vec::new();
            loop {
                if d.datatype()? == minicbor::data::Type::Break {
                    d.skip()?;
                    break;
                }
                if items.len() >= MAX_TRANSACTIONS {
                    return Err(DecodeError::message(format!(
                        "transaction list exceeds maximum of {MAX_TRANSACTIONS}"
                    )));
                }
                items.push(decode_raw_tx_body(d)?);
            }
            Ok(items)
        }
    }
}

/// Decode the bitmap map { u16 => u64 } with bounds checking.
fn decode_bitmap(d: &mut Decoder<'_>) -> Result<BTreeMap<u16, u64>, DecodeError> {
    let len = d.map()?;
    match len {
        Some(n) => {
            let n = n as usize;
            if n > MAX_BITMAP_ENTRIES {
                return Err(DecodeError::message(format!(
                    "bitmap has {n} entries, maximum is {MAX_BITMAP_ENTRIES}"
                )));
            }
            let mut bitmap = BTreeMap::new();
            for _ in 0..n {
                let index = d.u16()?;
                let bits = d.u64()?;
                bitmap.insert(index, bits);
            }
            Ok(bitmap)
        }
        None => {
            let mut bitmap = BTreeMap::new();
            loop {
                if d.datatype()? == minicbor::data::Type::Break {
                    d.skip()?;
                    break;
                }
                if bitmap.len() >= MAX_BITMAP_ENTRIES {
                    return Err(DecodeError::message(format!(
                        "bitmap exceeds maximum of {MAX_BITMAP_ENTRIES} entries"
                    )));
                }
                let index = d.u16()?;
                let bits = d.u64()?;
                bitmap.insert(index, bits);
            }
            Ok(bitmap)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Point;
    use minicbor::Decode;
    use std::convert::Infallible;

    fn round_trip(msg: &Message) -> Result<Message, DecodeError> {
        let encoded = minicbor::to_vec(msg).unwrap();
        minicbor::decode(&encoded)
    }

    fn test_hash() -> [u8; 32] {
        let mut h = [0u8; 32];
        h[0] = 0xAB;
        h[31] = 0xCD;
        h
    }

    // --- Round-trip tests ---

    #[test]
    fn block_request_round_trip() {
        let msg = Message::MsgLeiosBlockRequest {
            point: Point::Specific {
                slot: 42,
                hash: test_hash(),
            },
        };
        let decoded = round_trip(&msg).unwrap();
        match decoded {
            Message::MsgLeiosBlockRequest { point } => {
                assert_eq!(
                    point,
                    Point::Specific {
                        slot: 42,
                        hash: test_hash(),
                    }
                );
            }
            other => panic!("expected MsgLeiosBlockRequest, got {other:?}"),
        }
    }

    #[test]
    fn block_round_trip() {
        // `block` is raw CBOR — a `{ hash => size }` manifest map.  Built
        // here as a 1-entry map: 0xA1 (map(1)) + 0x5820<32B hash> + 0x18 0x2A (42).
        let mut block = vec![0xA1, 0x58, 0x20];
        block.extend_from_slice(&test_hash());
        block.extend_from_slice(&[0x18, 0x2A]);
        let msg = Message::MsgLeiosBlock {
            block: block.clone(),
        };
        let decoded = round_trip(&msg).unwrap();
        match decoded {
            // The raw manifest bytes round-trip verbatim.
            Message::MsgLeiosBlock { block: got } => assert_eq!(got, block),
            other => panic!("expected MsgLeiosBlock, got {other:?}"),
        }
    }

    #[test]
    fn block_txs_request_round_trip() {
        let mut bitmap = BTreeMap::new();
        bitmap.insert(0u16, 0xFFu64);
        bitmap.insert(5u16, 0x8000_0000_0000_0001u64);

        let msg = Message::MsgLeiosBlockTxsRequest {
            point: Point::Specific {
                slot: 100,
                hash: test_hash(),
            },
            bitmap,
        };
        let decoded = round_trip(&msg).unwrap();
        match decoded {
            Message::MsgLeiosBlockTxsRequest { point, bitmap } => {
                assert_eq!(
                    point,
                    Point::Specific {
                        slot: 100,
                        hash: test_hash(),
                    }
                );
                assert_eq!(bitmap.len(), 2);
                assert_eq!(bitmap[&0], 0xFF);
                assert_eq!(bitmap[&5], 0x8000_0000_0000_0001);
            }
            other => panic!("expected MsgLeiosBlockTxsRequest, got {other:?}"),
        }
    }

    #[test]
    fn block_txs_request_empty_bitmap_round_trip() {
        let msg = Message::MsgLeiosBlockTxsRequest {
            point: Point::Specific {
                slot: 0,
                hash: [0; 32],
            },
            bitmap: BTreeMap::new(),
        };
        let decoded = round_trip(&msg).unwrap();
        match decoded {
            Message::MsgLeiosBlockTxsRequest { bitmap, .. } => {
                assert!(bitmap.is_empty());
            }
            other => panic!("expected MsgLeiosBlockTxsRequest, got {other:?}"),
        }
    }

    /// A single value used as an opaque tx in tests (uint).
    fn tx(n: u8) -> TxBody {
        TxBody::new_with_vec(vec![n])
    }

    fn block_txs_round_trip_impl(
        original_transactions: Vec<TxBody>,
        original_bitmap: BTreeMap<u16, u64>,
        round_trip: &dyn Fn(&Message) -> Result<Message, DecodeError>,
    ) -> Result<(), DecodeError> {
        let msg = Message::MsgLeiosBlockTxs {
            point: Point::Specific {
                slot: 7,
                hash: test_hash(),
            },
            bitmap: original_bitmap.clone(),
            transactions: original_transactions.clone(),
        };
        let decoded = round_trip(&msg)?;
        match decoded {
            Message::MsgLeiosBlockTxs {
                point,
                bitmap,
                transactions,
            } => {
                assert_eq!(
                    point,
                    Point::Specific {
                        slot: 7,
                        hash: test_hash()
                    }
                );
                assert_eq!(bitmap, original_bitmap);
                if transactions != original_transactions {
                    return Err(DecodeError::message(format!(
                        "Round trip transactions mismatch: got {transactions:?}, expected {original_transactions:?}"
                    )));
                }
            }
            other => panic!("expected MsgLeiosBlockTxs, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn block_txs_round_trip() {
        let mut bitmap = BTreeMap::new();
        bitmap.insert(0u16, 0x3u64);

        block_txs_round_trip_impl(vec![tx(1), tx(2)], bitmap, &round_trip).unwrap();
    }

    /// (simplified flawed implementation, to demonstrate CBOR decoding bug)
    fn non_cbor_encode_leios_block(
        m: &Message,
        e: &mut Encoder<Vec<u8>>,
        _ctx: &mut (),
    ) -> Result<(), EncodeError<Infallible>> {
        if let Message::MsgLeiosBlockTxs {
            point,
            transactions,
            ..
        } = m
        {
            e.array(4)?;
            e.u32(3)?;
            minicbor::Encode::encode(point, e, &mut ())?;
            non_cbor_encode_tx_list(e, transactions)?;
            Ok(())
        } else {
            Err(EncodeError::message(
                "only MsgLeiosBlockTxs is supported in this test",
            ))
        }
    }

    /// (simplified flawed implementation, to demonstrate CBOR decoding bug)
    fn non_cbor_decode_leios_block(d: &mut Decoder, _ctx: &mut ()) -> Result<Message, DecodeError> {
        let _array_len = d.array()?;
        let tag = d.u32()?;

        if tag == 3 {
            let point = Point::decode(d, &mut ())?;
            let transactions = non_cbor_decode_tx_list(d)?;
            Ok(Message::MsgLeiosBlockTxs {
                point,
                bitmap: BTreeMap::new(),
                transactions,
            })
        } else {
            Err(DecodeError::message(format!(
                "unknown or unsupported leios_fetch message tag: {tag}"
            )))
        }
    }

    /// Encode a tx list `[ tx, ... ]`, splicing each tx's raw CBOR verbatim
    /// (txs are opaque pass-through — `tx.tx` may be any CBOR shape).
    /// (simplified flawed implementation, to demonstrate CBOR decoding bug)
    fn non_cbor_encode_tx_list<W: minicbor::encode::Write>(
        e: &mut Encoder<W>,
        txs: &[TxBody],
    ) -> Result<(), EncodeError<W::Error>> {
        e.array(txs.len() as u64)?;
        for tx in txs {
            encode_raw(e, tx.get_slice())?;
        }
        Ok(())
    }

    /// Decode a tx list, capturing each tx's raw CBOR (count- and
    /// size-bounded).
    /// (simplified flawed implementation, to demonstrate CBOR decoding bug)
    fn non_cbor_decode_tx_list(d: &mut Decoder<'_>) -> Result<Vec<TxBody>, DecodeError> {
        let len = d.array()?;
        match len {
            Some(n) => {
                let n = n as usize;
                let mut items = Vec::with_capacity(n);
                for _ in 0..n {
                    items.push(TxBody::new_with_vec(decode_raw_bounded(
                        d,
                        MAX_TRANSACTION_SIZE,
                        "transaction",
                    )?));
                }
                Ok(items)
            }
            None => Err(DecodeError::message(
                "indefinite-length tx list is not supported in this test",
            ))?,
        }
    }

    /// Use simplified flawed implementations to round-trip `message`.
    fn non_cbor_round_trip(message: &Message) -> Result<Message, DecodeError> {
        let mut e = Encoder::new(Vec::new());
        let _encoded = non_cbor_encode_leios_block(message, &mut e, &mut ());
        let writer = e.into_writer();
        let mut d = Decoder::new(&writer);
        non_cbor_decode_leios_block(&mut d, &mut ())
    }

    /// This test emulates a bug that quickly degrades network performance. The bug is
    /// caused by inconsistent data representation inside `TxBody`: half the code
    /// treats it as CBOR-encoded, while the other half treats it as raw bytes.
    ///
    /// There were two traditional ways of CBOR decoding:
    /// (a) as some incorrect TxBody, which is decoded from CBOR --- but there is no way to check,
    ///     this results in bad performance.
    /// (b) as non-complete CBOR, this also degrades performance (TxBody is not received, despite
    ///     it is delivered).
    ///
    /// However, the bug also manifested as decoding errors, which produced random warnings.
    /// This code reproduces these manifestations (to make sure that the bug is found).

    #[test]
    fn non_cbor_block_txs_round_trip() {
        let value1 = TxBody::new_with_slice(
            "Lorem Ipsum es simplemente el texto de relleno de las imprentas y archivos de texto."
                .as_bytes(),
        );

        // We need some non-ASCII characters to trigger the bug.
        let value2 = TxBody::new_with_slice(
            "Съешь ещё этих мягких французских булок, да выпей же чаю".as_bytes(),
        );

        // Sub-test 1. Correct CBOR, different transactions.
        let txs = vec![value1.clone(), value2.clone()];

        // ... Must be correct in corrected func.
        block_txs_round_trip_impl(txs.clone(), BTreeMap::new(), &round_trip).unwrap();

        // ... Inconsistent CBOR encoding: different txs.
        let result = block_txs_round_trip_impl(txs.clone(), BTreeMap::new(), &non_cbor_round_trip);
        assert!(result
            .unwrap_err()
            .to_string()
            .starts_with("decode error: Round trip transactions mismatch"));

        // Sub-test 2. Incorrect CBOR
        let value1a = TxBody::new_with_slice("Lorem Ipsum".as_bytes());
        let txs = vec![value1a.clone(), value2.clone()];

        // ... Must be correct in corrected func.
        block_txs_round_trip_impl(txs.clone(), BTreeMap::new(), &round_trip).unwrap();

        // ... CBOR format cannot be correctly decoded.
        let result = block_txs_round_trip_impl(txs.clone(), BTreeMap::new(), &non_cbor_round_trip);
        assert_eq!(result.unwrap_err().position(), Some(78));

        // Sub-test 3. 'utf-8': incorrect utf-8 sequence.
        let value1b = TxBody::new_with_slice([0x64, 0xD0, 0x9F, 0xD0, 0xD0].as_slice());
        let txs = vec![value1b.clone(), value2.clone()];

        // ... Must be correct in corrected func.
        block_txs_round_trip_impl(txs.clone(), BTreeMap::new(), &round_trip).unwrap();

        // ... and leads to unrelated decoding errors.
        let result = block_txs_round_trip_impl(txs.clone(), BTreeMap::new(), &non_cbor_round_trip);
        assert_eq!(
            result.unwrap_err().to_string(),
            "invalid utf-8 at position 39"
        );
    }

    #[test]
    fn block_txs_empty_round_trip() {
        let msg = Message::MsgLeiosBlockTxs {
            point: Point::Specific {
                slot: 0,
                hash: [0; 32],
            },
            bitmap: BTreeMap::new(),
            transactions: vec![],
        };
        let decoded = round_trip(&msg).unwrap();
        match decoded {
            Message::MsgLeiosBlockTxs { transactions, .. } => assert!(transactions.is_empty()),
            other => panic!("expected MsgLeiosBlockTxs, got {other:?}"),
        }
    }

    #[test]
    fn done_round_trip() {
        let decoded = round_trip(&Message::MsgDone).unwrap();
        assert!(matches!(decoded, Message::MsgDone));
    }

    // --- Error cases ---

    #[test]
    fn unknown_tag_fails() {
        let bad = &[0x81, 0x18, 0x63]; // [99]
        let result: Result<Message, _> = minicbor::decode(bad);
        assert!(result.is_err());
    }

    #[test]
    fn decode_indefinite_outer_array() {
        // MsgDone [9] as indefinite: 0x9f 0x09 0xff
        let cbor = &[0x9f, 0x09, 0xff];
        let decoded: Message = minicbor::decode(cbor).unwrap();
        assert!(matches!(decoded, Message::MsgDone));
    }

    #[test]
    fn wrong_hash_length_fails() {
        // MsgLeiosBlockRequest [0, [0, bytes(16)]] — point with hash too short
        let mut buf = Vec::new();
        let mut e = minicbor::Encoder::new(&mut buf);
        e.array(2).unwrap();
        e.u32(0).unwrap();
        e.array(2).unwrap();
        e.u64(0).unwrap();
        e.bytes(&[0u8; 16]).unwrap(); // 16 bytes, not 32

        let result: Result<Message, _> = minicbor::decode(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn bitmap_exceeds_max_fails() {
        let mut buf = Vec::new();
        let mut e = minicbor::Encoder::new(&mut buf);
        e.array(3).unwrap();
        e.u32(2).unwrap();
        // encode point sub-array
        e.array(2).unwrap();
        e.u64(0).unwrap();
        e.bytes(&[0u8; 32]).unwrap();
        let n = MAX_BITMAP_ENTRIES + 1;
        e.map(n as u64).unwrap();
        for i in 0..n {
            e.u16(i as u16).unwrap();
            e.u64(1).unwrap();
        }

        let result: Result<Message, _> = minicbor::decode(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn transaction_list_exceeds_max_fails() {
        let mut buf = Vec::new();
        let mut e = minicbor::Encoder::new(&mut buf);
        e.array(4).unwrap();
        e.u32(3).unwrap();
        // point
        e.array(2).unwrap();
        e.u64(0).unwrap();
        e.bytes(&[0u8; 32]).unwrap();
        // empty bitmap
        e.map(0).unwrap();
        // oversized tx list
        let n = MAX_TRANSACTIONS + 1;
        e.array(n as u64).unwrap();
        for _ in 0..n {
            e.u8(1).unwrap();
        }

        let result: Result<Message, _> = minicbor::decode(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn truncated_message_fails() {
        let msg = Message::MsgLeiosBlockRequest {
            point: Point::Specific {
                slot: 1,
                hash: test_hash(),
            },
        };
        let encoded = minicbor::to_vec(&msg).unwrap();
        let truncated = &encoded[..3];
        let result: Result<Message, _> = minicbor::decode(truncated);
        assert!(result.is_err());
    }
}
