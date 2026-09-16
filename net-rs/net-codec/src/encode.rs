//! Era-8 (Dijkstra / Leios) block and header **encoding** — the counterpart to
//! the decoders in [`crate::header`] and [`crate::block`], so the wire format
//! (field order, the merged `block_body`, and the era-tag wrappers) lives in
//! one place rather than being hand-rolled by each producer.
//!
//! Wire shapes (confirmed against real musashi blocks/headers):
//!   - ChainSync header:  `[era_header, #6.24([header_body, body_signature])]`
//!   - BlockFetch block:  `#6.24([era_block, [header, block_body]])`
//!   - `block_body = [transactions, leios_certificate/nil,
//!                    peras_certificate/nil]`  (3 fields, w36+)
//!   - `header_body` is the fixed 12-field Dijkstra array (10 base fields +
//!     `leios_certified: bool` + `leios_announcement: [hash32,uint]/nil`).
//!
//! The block-body hash the header commits to is `blake2b256(cbor(block_body))`;
//! [`HeaderBody::block_body_hash`] must be set to that (see the round-trip test).

use minicbor::Encoder;

/// BlockFetch era wrapper tag for the era-8 Leios block: `#6.24([8, block])`.
pub const DIJKSTRA_BLOCK_ERA: u32 = 8;

/// ChainSync era wrapper tag for the era-8 header: `[7, #6.24(header)]`.
///
/// EMPIRICAL: real musashi headers wrap one era-index below the block (7 vs 8),
/// and the relays reject an era-8-wrapped header (decode failure). Whether the
/// one-less header index is a general Cardano property or a trait of this
/// testnet's node, we match what the network accepts.
pub const DIJKSTRA_HEADER_ERA: u32 = DIJKSTRA_BLOCK_ERA - 1;

/// The operational certificate 4-tuple carried in `header_body[8]`.
#[derive(Debug, Clone)]
pub struct OperationalCert {
    pub hot_vkey: [u8; 32],
    pub counter: u64,
    pub kes_period: u64,
    pub sigma: [u8; 64],
}

/// The 12 fields of a Dijkstra `header_body`, in wire order.
#[derive(Debug, Clone)]
pub struct HeaderBody {
    pub block_number: u64,
    pub slot: u64,
    pub prev_hash: Option<[u8; 32]>,
    pub issuer_vkey: [u8; 32],
    pub vrf_vkey: [u8; 32],
    pub vrf_output: [u8; 64],
    pub vrf_proof: [u8; 80],
    pub block_body_size: u32,
    pub block_body_hash: [u8; 32],
    pub op_cert: OperationalCert,
    pub protocol_version: (u32, u32),
    pub leios_certified: bool,
    /// `[endorser_block_hash, size]` when this header announces an EB, else nil.
    pub announced_eb: Option<([u8; 32], u32)>,
}

impl HeaderBody {
    /// Encode the 12-field `header_body` CBOR — the exact bytes the KES body
    /// signature covers.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut e = Encoder::new(&mut buf);
        let _ = e
            .array(12)
            .and_then(|e| e.u64(self.block_number))
            .and_then(|e| e.u64(self.slot));
        match self.prev_hash {
            Some(h) => {
                let _ = e.bytes(&h);
            }
            None => {
                let _ = e.null();
            }
        }
        let _ = e
            .bytes(&self.issuer_vkey)
            .and_then(|e| e.bytes(&self.vrf_vkey))
            .and_then(|e| e.array(2)) // vrf_result: [output(64), proof(80)]
            .and_then(|e| e.bytes(&self.vrf_output))
            .and_then(|e| e.bytes(&self.vrf_proof))
            .and_then(|e| e.u32(self.block_body_size))
            .and_then(|e| e.bytes(&self.block_body_hash))
            .and_then(|e| e.array(4)) // operational_cert
            .and_then(|e| e.bytes(&self.op_cert.hot_vkey))
            .and_then(|e| e.u64(self.op_cert.counter))
            .and_then(|e| e.u64(self.op_cert.kes_period))
            .and_then(|e| e.bytes(&self.op_cert.sigma))
            .and_then(|e| e.array(2)) // protocol_version [major, minor]
            .and_then(|e| e.u32(self.protocol_version.0))
            .and_then(|e| e.u32(self.protocol_version.1))
            .and_then(|e| e.bool(self.leios_certified)); // leios_certified
        match self.announced_eb {
            Some((eb_hash, size)) => {
                let _ = e
                    .array(2)
                    .and_then(|e| e.bytes(&eb_hash))
                    .and_then(|e| e.u32(size));
            }
            None => {
                let _ = e.null();
            }
        }
        buf
    }
}

/// Encode the era-8 `block_body = [transactions, leios_certificate/nil,
/// peras_certificate/nil]`.
///
/// `transactions` are already-encoded `transaction` items, appended verbatim;
/// pass an empty slice for an honest empty body. `leios_cert` is the 2-tuple
/// `[signers, aggregated_signature]` CBOR when certifying, else `None`.
/// `peras_certificate` is always nil here.
///
/// THREE fields, not four. Verified against 161 real w36 blocks pulled from the
/// proto-devnet nodes' VolatileDB: every one decodes as `[list, nil, nil]`.
/// There is no leading `invalid_transactions` slot — the era-8 restructure moved
/// per-transaction validity into the `transaction` item itself, which is now the
/// 4-tuple `[transaction_body, witness_set, auxiliary_data/nil, is_valid]`.
///
/// Emitting the old 4-field body (a leading `nil` where the node expects the
/// transaction list) is not a soft mismatch: cardano-node's BlockFetch decoder
/// aborts the whole connection with
/// `DeserialiseFailure <offset> "expected list len or indef"`, so our blocks
/// never diffuse and any EB they announce is never certified.
pub fn encode_block_body(transactions: &[&[u8]], leios_cert: Option<&[u8]>) -> Vec<u8> {
    let mut buf = Vec::new();
    let _ = Encoder::new(&mut buf).array(3); // 3-field block_body
    let _ = Encoder::new(&mut buf).array(transactions.len() as u64); // [0] transactions
    for tx in transactions {
        buf.extend_from_slice(tx);
    }
    match leios_cert {
        Some(c) => buf.extend_from_slice(c), // [1] leios_certificate (verbatim)
        None => {
            let _ = Encoder::new(&mut buf).null();
        }
    }
    let _ = Encoder::new(&mut buf).null(); // [2] peras_certificate = nil
    buf
}

/// Assemble the inner header `[header_body, body_signature]` (what the block
/// embeds and the ChainSync header wraps).
pub fn encode_header_inner(header_body: &[u8], body_signature: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    let _ = Encoder::new(&mut buf).array(2);
    buf.extend_from_slice(header_body);
    let _ = Encoder::new(&mut buf).bytes(body_signature);
    buf
}

/// ChainSync wire header: `[era, #6.24(header_inner)]`.
pub fn wrap_header(header_inner: &[u8], era: u32) -> Vec<u8> {
    let mut buf = Vec::new();
    let _ = Encoder::new(&mut buf)
        .array(2)
        .and_then(|e| e.u32(era))
        .and_then(|e| e.tag(minicbor::data::Tag::new(24)))
        .and_then(|e| e.bytes(header_inner));
    buf
}

/// BlockFetch wire block: `#6.24([era, [header_inner, block_body]])`.
pub fn wrap_block(header_inner: &[u8], block_body: &[u8], era: u32) -> Vec<u8> {
    // Inner: [era, [header, block_body]].
    let mut inner = Vec::new();
    let _ = Encoder::new(&mut inner).array(2).and_then(|e| e.u32(era));
    let _ = Encoder::new(&mut inner).array(2);
    inner.extend_from_slice(header_inner);
    inner.extend_from_slice(block_body);
    // Wrap in #6.24(bytes).
    let mut buf = Vec::new();
    let _ = Encoder::new(&mut buf)
        .tag(minicbor::data::Tag::new(24))
        .and_then(|e| e.bytes(&inner));
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{blake2b_256, HeaderInfo, WrappedHeader};

    fn sample_header_body(body_hash: [u8; 32], body_size: u32) -> HeaderBody {
        HeaderBody {
            block_number: 83957,
            slot: 2_463_362,
            prev_hash: Some([0xAB; 32]),
            issuer_vkey: [0x11; 32],
            vrf_vkey: [0x22; 32],
            vrf_output: [0x33; 64],
            vrf_proof: [0x44; 80],
            block_body_size: body_size,
            block_body_hash: body_hash,
            op_cert: OperationalCert {
                hot_vkey: [0x55; 32],
                counter: 0,
                kes_period: 8,
                sigma: [0x66; 64],
            },
            protocol_version: (12, 0x50),
            leios_certified: false,
            announced_eb: None,
        }
    }

    #[test]
    fn empty_block_body_matches_w36_shape_and_hash() {
        // Honest empty body: [[], nil, nil] = 83 80 f6 f6.
        //
        // Both vectors are lifted from real w36 blocks, not assumed: every one
        // of the 61 blocks in a proto-devnet node's VolatileDB satisfies
        // `blake2b256(block_body_bytes) == header_body[7]` and
        // `len(block_body_bytes) == header_body[6]`, and the empty ones are
        // byte-identical to this.
        //
        // The previous vector here (`84 f6 80 f6 f6`, hash 22e62b67…) was the
        // pre-w36 4-field body. It was captured from musashi before the
        // 2026-09-07 respin put that chain on w36 too, so it no longer
        // describes any live chain.
        let body = encode_block_body(&[], None);
        assert_eq!(body, [0x83, 0x80, 0xf6, 0xf6]);
        assert_eq!(
            hex::encode(blake2b_256(&body)),
            "c93a1b1497883589d7ec8abb158c9b66c311ac8d45be74610bf889443daec967"
        );
    }

    #[test]
    fn block_body_places_cert_at_index_1() {
        let cert = {
            let mut c = Vec::new();
            let _ = Encoder::new(&mut c)
                .array(2)
                .and_then(|e| e.bytes(&[0x63, 0xff, 0xff]))
                .and_then(|e| e.bytes(&[0x11u8; 48]));
            c
        };
        let body = encode_block_body(&[], Some(&cert));
        // Decode: [[], cert, nil]
        let mut d = minicbor::Decoder::new(&body);
        assert_eq!(d.array().unwrap(), Some(3));
        d.skip().unwrap(); // [0] transactions
        let cert_start = d.position();
        d.skip().unwrap(); // [1] leios_certificate
        assert_eq!(&body[cert_start..d.position()], cert.as_slice());
    }

    #[test]
    fn header_round_trips_through_decoder_with_era7_wrapper() {
        let body = encode_block_body(&[], None);
        let hb = sample_header_body(blake2b_256(&body), body.len() as u32);
        let hb_bytes = hb.encode();
        let inner = encode_header_inner(&hb_bytes, &[0u8; 448]);
        let header = wrap_header(&inner, DIJKSTRA_HEADER_ERA);

        // First bytes: array(2), era tag 7, tag 24 …
        assert_eq!(&header[..2], &[0x82, 0x07]);

        // Decoder recovers the semantic fields.
        let info = HeaderInfo::parse(&header).expect("header parses");
        assert_eq!(info.slot, hb.slot);
        assert_eq!(info.prev_hash, hb.prev_hash);
        assert_eq!(info.block_body_hash, hb.block_body_hash);

        // Point hash is over the inner header, independent of the era wrapper.
        let wh = WrappedHeader::new(header.clone());
        assert!(wh.point().is_some());
    }

    #[test]
    fn block_round_trips_and_carries_no_cert_when_empty() {
        let body = encode_block_body(&[], None);
        let hb = sample_header_body(blake2b_256(&body), body.len() as u32);
        let inner = encode_header_inner(&hb.encode(), &[0u8; 448]);
        let block = wrap_block(&inner, &body, DIJKSTRA_BLOCK_ERA);

        // #6.24([8, [header, body]]) — an empty era-8 body has no cert.
        assert_eq!(&block[..2], &[0xd8, 0x18]);
        let bb = crate::BlockBody::new(block);
        // praos_inspect sees the era-8 [header, wrapper] shape (2 block fields).
        let info = bb.praos_inspect();
        assert_eq!(info.field_count, 2);
        assert_eq!(info.tx_count, 0);
    }
}
