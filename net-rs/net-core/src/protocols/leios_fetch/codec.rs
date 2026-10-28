//! CBOR encoding for LeiosFetch messages (CIP-0164 prototype,
//! cardano-blueprint `leios-prototype`).
//!
//! Wire format:
//!   msgLeiosBlockRequest    = [1, point]              point = [slot, hash32]
//!   msgLeiosBlock           = [2, endorser_block]     endorser_block = { hash => word32 }
//!   msgLeiosBlockTxsRequest = [3, point, bitmaps]     bitmaps = { word16 => word64 }
//!   msgLeiosBlockTxs        = [4, tx_list]   tx_list = [ * #6.24(bytes .cbor block_transaction) ]
//!   msgClientDone           = [0]
//!
//! The manifest is raw CBOR; transaction bytes are opaque inside tag 24.
//! The transaction's trailing validity flag is part of its manifest identity.

use std::collections::BTreeMap;

use super::{MAX_BITMAP_ENTRIES, MAX_BLOCK_SIZE, MAX_TRANSACTION_SIZE, MAX_TRANSACTIONS, Message};
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
                e.u32(1)?;
                minicbor::Encode::encode(point, e, &mut ())?;
            }
            Message::MsgLeiosBlock { block } => {
                e.array(2)?;
                e.u32(2)?;
                // `block` is the raw CBOR `endorser_block` map — splice verbatim.
                encode_raw(e, block)?;
            }
            Message::MsgLeiosBlockTxsRequest { point, bitmap } => {
                e.array(3)?;
                e.u32(3)?;
                minicbor::Encode::encode(point, e, &mut ())?;
                encode_bitmap(e, bitmap)?;
            }
            Message::MsgLeiosBlockTxs { transactions } => {
                e.array(2)?;
                e.u32(4)?;
                encode_tx_list(e, transactions)?;
            }
            Message::MsgDone => {
                e.array(1)?;
                e.u32(0)?;
            }
        }
        Ok(())
    }
}

impl<'a> minicbor::Decode<'a, ()> for Message {
    fn decode(d: &mut Decoder<'a>, _ctx: &mut ()) -> Result<Self, DecodeError> {
        let array_len = d.array()?;
        let tag = d.u32()?;
        let expected_len = match tag {
            0 => 1,
            1 | 2 | 4 => 2,
            3 => 3,
            _ => return Err(DecodeError::message("unknown LeiosFetch message tag")),
        };
        if array_len != Some(expected_len) {
            return Err(DecodeError::message("incorrect LeiosFetch message arity"));
        }

        match tag {
            1 => {
                let point = Point::decode(d, &mut ())?;
                Ok(Message::MsgLeiosBlockRequest { point })
            }
            2 => {
                // endorser_block is a CBOR map { tx_hash => tx_size };
                // captured as raw bytes for opaque pass-through.
                let block = decode_raw_bounded(d, MAX_BLOCK_SIZE, "endorser block")?;
                Ok(Message::MsgLeiosBlock { block })
            }
            3 => {
                let point = Point::decode(d, &mut ())?;
                let bitmap = decode_bitmap(d)?;
                Ok(Message::MsgLeiosBlockTxsRequest { point, bitmap })
            }
            4 => {
                let transactions = decode_tx_list(d)?;
                Ok(Message::MsgLeiosBlockTxs { transactions })
            }
            0 => Ok(Message::MsgDone),
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

/// Encode the w42/w43 cbor-in-cbor transaction list without changing its bytes.
fn encode_tx_list<W: minicbor::encode::Write>(
    e: &mut Encoder<W>,
    txs: &[TxBody],
) -> Result<(), EncodeError<W::Error>> {
    e.array(txs.len() as u64)?;
    for tx in txs {
        e.tag(minicbor::data::Tag::new(24))?;
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
    if d.tag()?.as_u64() != 24 {
        return Err(DecodeError::message(
            "expected tag 24 for Leios transaction",
        ));
    }
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

    #[test]
    fn block_txs_preserve_opaque_bytes() {
        let transactions = vec![
            tx(1),
            TxBody::new_with_slice(b"not CBOR"),
            TxBody::new_with_slice(&[0x64, 0xd0, 0x9f, 0xd0, 0xd0]),
        ];
        let decoded = round_trip(&Message::MsgLeiosBlockTxs {
            transactions: transactions.clone(),
        })
        .unwrap();
        let Message::MsgLeiosBlockTxs {
            transactions: actual,
        } = decoded
        else {
            panic!("expected transactions");
        };
        assert_eq!(actual, transactions);
    }

    #[test]
    fn block_txs_empty_round_trip() {
        let msg = Message::MsgLeiosBlockTxs {
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
        // MsgDone [0] as indefinite: 0x9f 0x00 0xff
        let cbor = &[0x9f, 0x00, 0xff];
        assert!(minicbor::decode::<Message>(cbor).is_err());
    }

    #[test]
    fn wrong_hash_length_fails() {
        // MsgLeiosBlockRequest [1, [0, bytes(16)]] — point with hash too short
        let mut buf = Vec::new();
        let mut e = minicbor::Encoder::new(&mut buf);
        e.array(2).unwrap();
        e.u32(1).unwrap();
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
        e.u32(3).unwrap();
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
        let mut e = Encoder::new(Vec::new());
        e.array(2)
            .unwrap()
            .u32(4)
            .unwrap()
            .array((MAX_TRANSACTIONS + 1) as u64)
            .unwrap();
        let result = minicbor::decode::<Message>(&e.into_writer());
        assert!(result.unwrap_err().to_string().contains("maximum"));
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

    // Exact CBOR vectors derived from the Haskell encoder.
    // ouroboros-consensus c54515e35498e921cbd651a2c19c3360a41d35b8
    #[test]
    fn fetch_request_wire_vector() {
        let mut bytes = vec![0x82, 0x01, 0x82, 0x18, 0x2a, 0x58, 0x20];
        bytes.extend_from_slice(&[0xab; 32]);
        let msg: Message = minicbor::decode(&bytes).unwrap();
        assert!(matches!(msg, Message::MsgLeiosBlockRequest { .. }));
        assert_eq!(minicbor::to_vec(&msg).unwrap(), bytes);
    }

    #[test]
    fn fetch_reply_and_transaction_wire_vectors() {
        // Haskell encodeLeiosFetch at a337d69f4946d5a1b20ecaaeae635dd47822e3d2.
        let point = Point::Specific {
            slot: 42,
            hash: [0xab; 32],
        };
        let mut request = vec![0x83, 0x03, 0x82, 0x18, 0x2a, 0x58, 0x20];
        request.extend_from_slice(&[0xab; 32]);
        request.extend_from_slice(&[0xbf, 0xff]); // Haskell encodes an indefinite map
        let response = vec![0x82, 0x04, 0x80];
        let vectors = [
            (Message::MsgDone, vec![0x81, 0x00]),
            (
                Message::MsgLeiosBlock { block: vec![0xa0] },
                vec![0x82, 0x02, 0xa0],
            ),
            (
                Message::MsgLeiosBlockTxsRequest {
                    point: point.clone(),
                    bitmap: BTreeMap::new(),
                },
                request,
            ),
            (
                Message::MsgLeiosBlockTxs {
                    transactions: vec![],
                },
                response,
            ),
        ];
        for (expected, bytes) in vectors {
            let decoded: Message = minicbor::decode(&bytes).unwrap();
            assert_eq!(
                std::mem::discriminant(&decoded),
                std::mem::discriminant(&expected)
            );
            assert_eq!(minicbor::to_vec(&expected).unwrap(), bytes);
            assert_eq!(minicbor::to_vec(&decoded).unwrap(), bytes);
        }
    }
}

#[cfg(test)]
mod w43_vectors {
    use super::*;

    #[test]
    fn captured_w43_transaction_request() {
        let raw = include_bytes!("../../../test_data/leios-fetch-w43-request.cbor");
        let message: Message = minicbor::decode(raw).unwrap();
        let Message::MsgLeiosBlockTxsRequest { bitmap, .. } = &message else {
            panic!("expected transaction request");
        };
        assert_eq!(bitmap, &BTreeMap::from([(0, 1_u64 << 63)]));
        assert_eq!(minicbor::to_vec(message).unwrap(), raw);
    }

    #[test]
    fn rejects_legacy_response_and_wrong_transaction_tag() {
        for raw in [
            &[0x84, 0x04, 0x80, 0xa0, 0x80][..],
            &[0x82, 0x04, 0x81, 0xd8, 0x17, 0x40],
            &[0x82, 0x04, 0x81, 0x40],
        ] {
            assert!(minicbor::decode::<Message>(raw).is_err());
        }
    }

    #[test]
    fn bounds_transaction_bytes_and_indefinite_list_count() {
        let message = Message::MsgLeiosBlockTxs {
            transactions: vec![TxBody::new_with_vec(vec![0; MAX_TRANSACTION_SIZE + 1])],
        };
        let encoded = minicbor::to_vec(message).unwrap();
        assert!(
            minicbor::decode::<Message>(&encoded)
                .unwrap_err()
                .to_string()
                .contains("maximum")
        );
        let mut raw = vec![0x82, 0x04, 0x9f];
        for _ in 0..=MAX_TRANSACTIONS {
            raw.extend_from_slice(&[0xd8, 0x18, 0x40]);
        }
        raw.push(0xff);
        assert!(
            minicbor::decode::<Message>(&raw)
                .unwrap_err()
                .to_string()
                .contains("maximum")
        );
    }

    #[test]
    fn captured_w43_transaction_response() {
        let raw = include_bytes!("../../../test_data/leios-fetch-w43-response.cbor");
        let message: Message = minicbor::decode(raw).unwrap();
        let Message::MsgLeiosBlockTxs { transactions, .. } = &message else {
            panic!("expected transaction response");
        };
        assert_eq!(transactions.len(), 1);
        assert_eq!(
            transactions[0].get_slice(),
            include_bytes!("../../../../net-codec/test_data/leios-transaction-w43.cbor")
        );
        assert_eq!(minicbor::to_vec(message).unwrap(), raw);
    }
}
