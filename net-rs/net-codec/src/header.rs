//! Era-tagged block headers with optional parsed Shelley+ fields.

use minicbor::decode::Error as DecodeError;
use minicbor::encode::Error as EncodeError;
use minicbor::{Decoder, Encoder};

use crate::{Point, MAX_HEADER_SIZE};

/// Number of base fields in a Shelley+ header_body array (before Leios extensions).
const HEADER_BODY_BASE_FIELDS: u64 = 10;

// --- HeaderInfo ---

/// Parsed fields from a Shelley+ block header.
///
/// Extracted from the header_body array inside the era-tagged, #6.24-wrapped
/// header CBOR. Fields we don't need at the network layer (VRF proofs,
/// operational cert, protocol version, body signature) are skipped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderInfo {
    /// Era tag (2=Shelley, 3=Allegra, 4=Mary, 5=Alonzo, 6=Babbage, 7=Conway).
    pub era: u8,
    /// Block number.
    pub block_number: u64,
    /// Slot number.
    pub slot: u64,
    /// Hash of the previous block (None for genesis-successor).
    pub prev_hash: Option<[u8; 32]>,
    /// Issuer verification key hash (32 bytes).
    pub issuer_vkey: [u8; 32],
    /// Block body size in bytes.
    pub body_size: u32,
    /// Hash of the block body.
    pub block_body_hash: [u8; 32],
    /// CIP-0164 Leios: announced endorser block hash and byte size.
    pub announced_eb: Option<([u8; 32], u32)>,
    /// CIP-0164 Leios: whether this RB certifies the previous RB's EB.
    pub certified_eb: Option<bool>,
}

impl HeaderInfo {
    /// Try to parse Shelley+ header fields from raw WrappedHeader bytes.
    ///
    /// Returns None for Byron headers (era 0/1) or if parsing fails.
    /// Parsing failures are silent — this is best-effort extraction.
    ///
    /// Wire structure:
    /// ```text
    /// [era_tag, #6.24(bytes .cbor [header_body, body_signature])]
    /// ```
    pub fn parse(raw: &[u8]) -> Option<Self> {
        Self::try_parse(raw).ok()
    }

    fn try_parse(raw: &[u8]) -> Result<Self, DecodeError> {
        let mut d = Decoder::new(raw);

        // Outer: [era_tag, #6.24(bytes)]
        let _outer_len = d.array()?;
        let era = d.u32()? as u8;

        // Byron (era 0 or 1) has a different structure — skip it.
        if era < 2 {
            return Err(DecodeError::message("Byron header"));
        }

        // Unwrap #6.24 tag → get inner CBOR bytes.
        let tag = d.tag()?;
        if tag.as_u64() != 24 {
            return Err(DecodeError::message(format!(
                "expected CBOR tag 24, got {}",
                tag.as_u64()
            )));
        }
        let inner_bytes = d.bytes()?;

        // Inner: [header_body, body_signature]
        let mut d = Decoder::new(inner_bytes);
        let _inner_len = d.array()?;

        // header_body is itself an array
        let body_len = match d.array()? {
            Some(n) => n,
            None => return Err(DecodeError::message("indefinite header_body")),
        };

        if body_len < HEADER_BODY_BASE_FIELDS {
            return Err(DecodeError::message(format!(
                "header_body has {body_len} fields, expected at least {HEADER_BODY_BASE_FIELDS}"
            )));
        }

        // Field 0: block_number
        let block_number = d.u64()?;
        // Field 1: slot
        let slot = d.u64()?;
        // Field 2: prev_hash (hash32 or null)
        let prev_hash = parse_optional_hash(&mut d)?;
        // Field 3: issuer_vkey (bytes 32)
        let issuer_vkey = parse_hash32(&mut d)?;
        // Field 4: vrf_vkey — skip
        d.skip()?;
        // Field 5: vrf_result — skip
        d.skip()?;
        // Field 6: body_size
        let body_size = d.u32()?;
        // Field 7: block_body_hash
        let block_body_hash = parse_hash32(&mut d)?;
        // Field 8: operational_cert — skip
        d.skip()?;
        // Field 9: protocol_version — skip
        d.skip()?;

        // CIP-0164 Leios extensions — optional trailing fields.  Be
        // liberal about layout: array length alone isn't enough because
        // implementations disagree on how to group `announced_eb`.  The
        // CIP draft we follow encodes it as two flat fields (hash, size),
        // so a 12-element body means "10 base + announced_eb (flat)";
        // the leios-prototype dev relay (and likely a later draft)
        // encodes it as a single tuple `[hash, size]`, so an 11-element
        // body can mean "10 base + announced_eb (as tuple)" instead of
        // the "10 base + certified_eb bool" we'd otherwise assume.
        //
        // Dispatch on the CBOR datatype of each trailing field rather
        // than its position:
        //
        //   bool          → certified_eb           (consumes 1 element)
        //   array(2)      → announced_eb tuple     (consumes 1 element)
        //   bytes(32)     → announced_eb flat hash; the next element is
        //                   the u32 size            (consumes 2 elements)
        //   null          → announced_eb absent    (consumes 1 element)
        //
        // The `null` case is the finalized Dijkstra layout (cardano-ledger
        // #5889): a fixed array(12) ending in `certified_eb : bool,
        // announced_eb : [hash32, uint] / nil`.
        //
        // Any other shape falls through as an error rather than being
        // silently misread.
        let mut announced_eb: Option<([u8; 32], u32)> = None;
        let mut certified_eb: Option<bool> = None;
        let mut remaining = body_len - HEADER_BODY_BASE_FIELDS;
        while remaining > 0 {
            match d.datatype()? {
                minicbor::data::Type::Bool => {
                    if certified_eb.is_some() {
                        return Err(DecodeError::message(
                            "duplicate certified_eb extension in header_body",
                        ));
                    }
                    certified_eb = Some(d.bool()?);
                    remaining -= 1;
                }
                minicbor::data::Type::Array | minicbor::data::Type::ArrayIndef => {
                    // Tuple shape `[hash, size]`.  Accept both definite
                    // and indefinite encodings to stay liberal; the
                    // indefinite form needs the trailing break marker
                    // (CBOR 0xff) explicitly consumed.
                    if announced_eb.is_some() {
                        return Err(DecodeError::message(
                            "duplicate announced_eb extension in header_body",
                        ));
                    }
                    let arr_len = d.array()?;
                    let eb_hash = parse_hash32(&mut d)?;
                    let eb_size = d.u32()?;
                    match arr_len {
                        Some(2) => {}
                        Some(other) => {
                            return Err(DecodeError::message(format!(
                                "announced_eb tuple expected array(2), got array({other})"
                            )));
                        }
                        None => {
                            if d.datatype()? != minicbor::data::Type::Break {
                                return Err(DecodeError::message(
                                    "announced_eb indefinite tuple has more than 2 elements",
                                ));
                            }
                            d.skip()?;
                        }
                    }
                    announced_eb = Some((eb_hash, eb_size));
                    remaining -= 1;
                }
                minicbor::data::Type::Bytes => {
                    if announced_eb.is_some() {
                        return Err(DecodeError::message(
                            "duplicate announced_eb extension in header_body",
                        ));
                    }
                    if remaining < 2 {
                        return Err(DecodeError::message(
                            "flat announced_eb expects hash followed by size, only 1 slot left",
                        ));
                    }
                    let eb_hash = parse_hash32(&mut d)?;
                    let eb_size = d.u32()?;
                    announced_eb = Some((eb_hash, eb_size));
                    remaining -= 2;
                }
                minicbor::data::Type::Null => {
                    // The finalized Dijkstra-era layout (cardano-ledger
                    // #5889, deployed on the 2026w27 leios-prototype
                    // testnet) makes the body a fixed array(12) whose last
                    // field is `leios_announcement : leios_announcement /
                    // nil` — an explicit `null` when the block announces no
                    // EB.  Earlier draft encodings simply omitted the
                    // field.  Treat `null` as "no announced_eb".
                    if announced_eb.is_some() {
                        return Err(DecodeError::message(
                            "duplicate announced_eb extension in header_body",
                        ));
                    }
                    d.skip()?;
                    remaining -= 1;
                }
                other => {
                    return Err(DecodeError::message(format!(
                        "unexpected header_body extension field type: {other:?}"
                    )));
                }
            }
        }

        Ok(HeaderInfo {
            era,
            block_number,
            slot,
            prev_hash,
            issuer_vkey,
            body_size,
            block_body_hash,
            announced_eb,
            certified_eb,
        })
    }
}

/// Parse a 32-byte hash from the decoder.
fn parse_hash32(d: &mut Decoder<'_>) -> Result<[u8; 32], DecodeError> {
    let bytes = d.bytes()?;
    if bytes.len() != 32 {
        return Err(DecodeError::message(format!(
            "expected 32-byte hash, got {} bytes",
            bytes.len()
        )));
    }
    let mut hash = [0u8; 32];
    hash.copy_from_slice(bytes);
    Ok(hash)
}

/// Parse an optional hash: either a 32-byte bstr or null.
fn parse_optional_hash(d: &mut Decoder<'_>) -> Result<Option<[u8; 32]>, DecodeError> {
    let dt = d.datatype()?;
    if dt == minicbor::data::Type::Null {
        d.skip()?;
        return Ok(None);
    }
    parse_hash32(d).map(Some)
}

// --- Header hash ---

/// Compute Blake2b-256 hash of header CBOR bytes.
/// Compute the block header hash (Blake2b-256).
///
/// The input is an era-tagged header in ChainSync wire format:
/// `[era_tag, #6.24(header_cbor)]`. The hash is `Blake2b-256(header_cbor)` —
/// over the bytes inside the `#6.24` bstr, which is the CBOR encoding of
/// `[header_body, body_signature]`. This matches the Cardano convention
/// where neither the era tag nor the `#6.24` wrapping are part of the
/// header identity.
pub(crate) fn header_hash(header_bytes: &[u8]) -> [u8; 32] {
    let hash_input = extract_header_cbor(header_bytes).unwrap_or(header_bytes);
    let result = blake2b_simd::Params::new().hash_length(32).hash(hash_input);
    let mut hash = [0u8; 32];
    hash.copy_from_slice(result.as_bytes());
    hash
}

/// Extract the inner header CBOR bytes from `[era_tag, #6.24(header_cbor)]`.
/// Returns `header_cbor` — the bytes inside the `#6.24` bstr.
fn extract_header_cbor(header_bytes: &[u8]) -> Option<&[u8]> {
    let mut d = minicbor::Decoder::new(header_bytes);
    d.array().ok()?;
    d.skip().ok()?; // skip era_tag
    d.tag().ok()?; // skip #6.24 tag
    d.bytes().ok() // extract the inner bstr content
}

// --- WrappedHeader ---

/// An era-tagged block header stored as raw CBOR bytes, with optional parsed fields.
///
/// For Shelley+ headers, `parsed` contains extracted header fields including slot,
/// block number, issuer, and CIP-0164 Leios extensions. For Byron headers or
/// unparseable headers, `parsed` is None.
///
/// Wire format: raw bytes are passed through unchanged. The parsed fields are
/// derived from the raw bytes and not separately encoded.
#[derive(Debug, Clone)]
pub struct WrappedHeader {
    /// Raw CBOR bytes of the header (wire format).
    pub raw: Vec<u8>,
    /// Parsed header fields (Shelley+ only). None for Byron or parse failure.
    pub parsed: Option<HeaderInfo>,
}

impl WrappedHeader {
    /// Create a WrappedHeader from raw bytes, attempting to parse Shelley+ fields.
    pub fn new(raw: Vec<u8>) -> Self {
        let parsed = HeaderInfo::parse(&raw);
        Self { raw, parsed }
    }

    /// Create a WrappedHeader from raw bytes without parsing.
    /// Use for test fixtures with trivial CBOR that isn't a real header.
    pub fn opaque(raw: Vec<u8>) -> Self {
        Self { raw, parsed: None }
    }

    /// Derive the chain Point (slot + header hash) from this header.
    ///
    /// Computes Blake2b-256 of the raw header CBOR for the block hash
    /// and combines with the parsed slot number.
    /// Returns None for Byron headers or unparseable data.
    pub fn point(&self) -> Option<Point> {
        let info = self.parsed.as_ref()?;
        let hash = header_hash(&self.raw);
        Some(Point::Specific {
            slot: info.slot,
            hash,
        })
    }
}

impl PartialEq for WrappedHeader {
    fn eq(&self, other: &Self) -> bool {
        self.raw == other.raw
    }
}

impl Eq for WrappedHeader {}

impl minicbor::Encode<()> for WrappedHeader {
    fn encode<W: minicbor::encode::Write>(
        &self,
        e: &mut Encoder<W>,
        _ctx: &mut (),
    ) -> Result<(), EncodeError<W::Error>> {
        e.writer_mut()
            .write_all(&self.raw)
            .map_err(EncodeError::write)?;
        Ok(())
    }
}

impl<'a> minicbor::Decode<'a, ()> for WrappedHeader {
    fn decode(d: &mut Decoder<'a>, _ctx: &mut ()) -> Result<Self, DecodeError> {
        let start = d.position();
        d.skip()?;
        let end = d.position();
        let len = end - start;
        if len > MAX_HEADER_SIZE {
            return Err(DecodeError::message(format!(
                "header too large: {len} bytes exceeds limit {MAX_HEADER_SIZE}"
            )));
        }
        let raw = d
            .input()
            .get(start..end)
            .ok_or_else(|| DecodeError::message("failed to extract header bytes"))?;
        Ok(WrappedHeader::new(raw.to_vec()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use minicbor::Encoder;

    /// Build a fake Shelley+ header with the given fields for testing.
    /// Produces valid CBOR matching the wire format: [era_tag, #6.24(bytes .cbor [header_body, sig])]
    #[allow(clippy::too_many_arguments)]
    fn build_test_header(
        era: u8,
        block_number: u64,
        slot: u64,
        prev_hash: Option<[u8; 32]>,
        issuer_vkey: [u8; 32],
        body_size: u32,
        block_body_hash: [u8; 32],
        announced_eb: Option<([u8; 32], u32)>,
        certified_eb: Option<bool>,
    ) -> Vec<u8> {
        use minicbor::encode::Error as EncodeError;
        use std::io::Write as _;
        // Build header_body array
        let field_count = HEADER_BODY_BASE_FIELDS
            + if announced_eb.is_some() { 2 } else { 0 }
            + if certified_eb.is_some() { 1 } else { 0 };

        let mut body_buf = Vec::new();
        let mut be = Encoder::new(&mut body_buf);
        be.array(field_count).unwrap();
        be.u64(block_number).unwrap(); // 0
        be.u64(slot).unwrap(); // 1
        match prev_hash {
            // 2
            Some(h) => {
                be.bytes(&h).unwrap();
            }
            None => {
                be.null().unwrap();
            }
        }
        be.bytes(&issuer_vkey).unwrap(); // 3
        be.bytes(&[0u8; 32]).unwrap(); // 4: vrf_vkey (dummy)
        be.array(2).unwrap(); // 5: vrf_result (dummy)
        be.bytes(&[0u8; 32]).unwrap();
        be.bytes(&[0u8; 32]).unwrap();
        be.u32(body_size).unwrap(); // 6
        be.bytes(&block_body_hash).unwrap(); // 7
        be.array(4).unwrap(); // 8: operational_cert (dummy)
        be.bytes(&[0u8; 32]).unwrap();
        be.u64(0).unwrap();
        be.u64(0).unwrap();
        be.bytes(&[0u8; 64]).unwrap();
        be.array(2).unwrap(); // 9: protocol_version (dummy)
        be.u32(10).unwrap();
        be.u32(0).unwrap();

        if let Some((eb_hash, eb_size)) = announced_eb {
            be.bytes(&eb_hash).unwrap(); // 10
            be.u32(eb_size).unwrap(); // 11
        }
        if let Some(cert) = certified_eb {
            be.bool(cert).unwrap(); // 12 (or 10 if no EB)
        }

        // Build inner: [header_body, body_signature]
        let mut inner_buf = Vec::new();
        let mut ie = Encoder::new(&mut inner_buf);
        ie.array(2).unwrap();
        ie.writer_mut()
            .write_all(&body_buf)
            .map_err(EncodeError::write)
            .unwrap();
        ie.bytes(&[0u8; 64]).unwrap(); // dummy signature

        // Build outer: [era_tag, #6.24(inner_bytes)]
        let mut outer_buf = Vec::new();
        let mut oe = Encoder::new(&mut outer_buf);
        oe.array(2).unwrap();
        oe.u32(era as u32).unwrap();
        oe.tag(minicbor::data::Tag::new(24)).unwrap();
        oe.bytes(&inner_buf).unwrap();

        outer_buf
    }

    #[test]
    fn wrapped_header_round_trip() {
        // Use an era-tagged header stub: [6, #6.24(h'deadbeef')]
        let mut buf = Vec::new();
        let mut e = minicbor::Encoder::new(&mut buf);
        e.array(2).unwrap();
        e.u32(6).unwrap(); // Conway era tag
        e.tag(minicbor::data::Tag::new(24)).unwrap();
        e.bytes(&[0xde, 0xad, 0xbe, 0xef]).unwrap();

        let header = WrappedHeader::opaque(buf.clone());
        let encoded = minicbor::to_vec(&header).unwrap();
        assert_eq!(encoded, buf);

        let decoded: WrappedHeader = minicbor::decode(&encoded).unwrap();
        assert_eq!(decoded.raw, buf);
    }

    #[test]
    fn parse_conway_header_base_fields() {
        let raw = build_test_header(
            7,                // Conway
            12345,            // block_number
            67890,            // slot
            Some([0xAA; 32]), // prev_hash
            [0xBB; 32],       // issuer_vkey
            1024,             // body_size
            [0xCC; 32],       // block_body_hash
            None,             // no Leios EB
            None,             // no Leios certified_eb
        );

        let header = WrappedHeader::new(raw);
        let info = header.parsed.expect("should parse Conway header");
        assert_eq!(info.era, 7);
        assert_eq!(info.block_number, 12345);
        assert_eq!(info.slot, 67890);
        assert_eq!(info.prev_hash, Some([0xAA; 32]));
        assert_eq!(info.issuer_vkey, [0xBB; 32]);
        assert_eq!(info.body_size, 1024);
        assert_eq!(info.block_body_hash, [0xCC; 32]);
        assert_eq!(info.announced_eb, None);
        assert_eq!(info.certified_eb, None);
    }

    #[test]
    fn parse_header_with_announced_eb() {
        let eb_hash = [0xDD; 32];
        let raw = build_test_header(
            7,
            100,
            200,
            Some([0xAA; 32]),
            [0xBB; 32],
            2048,
            [0xCC; 32],
            Some((eb_hash, 65536)),
            None,
        );

        let info = HeaderInfo::parse(&raw).expect("should parse");
        assert_eq!(info.announced_eb, Some((eb_hash, 65536)));
        assert_eq!(info.certified_eb, None);
    }

    #[test]
    fn parse_header_with_certified_eb_only() {
        let raw = build_test_header(
            7,
            100,
            200,
            Some([0xAA; 32]),
            [0xBB; 32],
            2048,
            [0xCC; 32],
            None,
            Some(true),
        );

        let info = HeaderInfo::parse(&raw).expect("should parse");
        assert_eq!(info.announced_eb, None);
        assert_eq!(info.certified_eb, Some(true));
    }

    /// Build a header where `announced_eb` is encoded as a single
    /// `array(2)` tuple `[hash, size]` rather than two flat trailing
    /// fields.  This matches the layout sent by the public Leios dev
    /// relay (leios-prototype) and a later CIP-0164 draft; we accept it
    /// liberally via CBOR type dispatch.
    #[allow(clippy::too_many_arguments)]
    fn build_test_header_eb_tuple(
        era: u8,
        block_number: u64,
        slot: u64,
        prev_hash: Option<[u8; 32]>,
        issuer_vkey: [u8; 32],
        body_size: u32,
        block_body_hash: [u8; 32],
        announced_eb: ([u8; 32], u32),
        certified_eb: Option<bool>,
    ) -> Vec<u8> {
        use minicbor::encode::Error as EncodeError;
        use std::io::Write as _;
        let field_count = HEADER_BODY_BASE_FIELDS + 1 + if certified_eb.is_some() { 1 } else { 0 };

        let mut body_buf = Vec::new();
        let mut be = Encoder::new(&mut body_buf);
        be.array(field_count).unwrap();
        be.u64(block_number).unwrap();
        be.u64(slot).unwrap();
        match prev_hash {
            Some(h) => {
                be.bytes(&h).unwrap();
            }
            None => {
                be.null().unwrap();
            }
        }
        be.bytes(&issuer_vkey).unwrap();
        be.bytes(&[0u8; 32]).unwrap();
        be.array(2).unwrap();
        be.bytes(&[0u8; 32]).unwrap();
        be.bytes(&[0u8; 32]).unwrap();
        be.u32(body_size).unwrap();
        be.bytes(&block_body_hash).unwrap();
        be.array(4).unwrap();
        be.bytes(&[0u8; 32]).unwrap();
        be.u64(0).unwrap();
        be.u64(0).unwrap();
        be.bytes(&[0u8; 64]).unwrap();
        be.array(2).unwrap();
        be.u32(10).unwrap();
        be.u32(0).unwrap();

        // announced_eb as tuple [hash, size]
        be.array(2).unwrap();
        be.bytes(&announced_eb.0).unwrap();
        be.u32(announced_eb.1).unwrap();
        if let Some(cert) = certified_eb {
            be.bool(cert).unwrap();
        }

        let mut inner_buf = Vec::new();
        let mut ie = Encoder::new(&mut inner_buf);
        ie.array(2).unwrap();
        ie.writer_mut()
            .write_all(&body_buf)
            .map_err(EncodeError::write)
            .unwrap();
        ie.bytes(&[0u8; 64]).unwrap();

        let mut outer_buf = Vec::new();
        let mut oe = Encoder::new(&mut outer_buf);
        oe.array(2).unwrap();
        oe.u32(era as u32).unwrap();
        oe.tag(minicbor::data::Tag::new(24)).unwrap();
        oe.bytes(&inner_buf).unwrap();
        outer_buf
    }

    #[test]
    fn parse_header_with_announced_eb_as_tuple() {
        // Relay variant: announced_eb encoded as array(2) → header_body is
        // array(11).  Without type dispatch we'd misread the trailing
        // array as a (non-existent) certified_eb bool and fail.
        let eb_hash = [0x77; 32];
        let raw = build_test_header_eb_tuple(
            7,
            49907,
            1_001_160,
            Some([0xAA; 32]),
            [0xBB; 32],
            2048,
            [0xCC; 32],
            (eb_hash, 65536),
            None,
        );

        let info = HeaderInfo::parse(&raw).expect("tuple-encoded announced_eb must parse");
        assert_eq!(info.block_number, 49907);
        assert_eq!(info.announced_eb, Some((eb_hash, 65536)));
        assert_eq!(info.certified_eb, None);
    }

    #[test]
    fn parse_header_with_announced_eb_as_indefinite_tuple() {
        // A peer using indefinite-length CBOR for the tuple sends
        // 0x9f <hash> <size> 0xff instead of 0x82 <hash> <size>.
        // The trailing 0xff break must be consumed; otherwise the next
        // `Decoder::array()` (for the outer inner array) would see it
        // and fail.  Patch a known-good tuple-encoded header byte-by-
        // byte: locate the 0x82 (definite array(2)) for the announced
        // tuple, replace with 0x9f (indefinite), and append a 0xff
        // before the trailing body_signature.
        let eb_hash = [0x99; 32];
        let mut raw = build_test_header_eb_tuple(
            7,
            49907,
            1_001_160,
            Some([0xAA; 32]),
            [0xBB; 32],
            2048,
            [0xCC; 32],
            (eb_hash, 65536),
            None,
        );

        // Find the announced_eb tuple's 0x82 marker — the only
        // standalone 0x82 followed by 0x58 0x20 0x99... (bytes(32) of
        // 0x99) in the buffer.  Replace the 0x82 with 0x9f and
        // insert a 0xff break after the u32 size.
        let needle = [0x82u8, 0x58, 0x20, 0x99, 0x99];
        let idx = raw
            .windows(needle.len())
            .position(|w| w == needle)
            .expect("must find tuple marker");
        raw[idx] = 0x9f;
        // After 0x82 we have: 0x58 0x20 + 32-byte hash + u32 size (1a XX XX XX XX = 5 bytes).
        // Insert 0xff break after that.
        let after_tuple = idx + 1 + 2 + 32 + 5;
        raw.insert(after_tuple, 0xff);

        // The outer #6.24-wrapped bstr length needs to grow by 1 too.
        // Locate the inner bstr length prefix (5903 7f ... or 5903 81 ...
        // since these tests use 0x5903XX two-byte lengths) and bump it.
        // Skip outer `[era, ...]`: bytes 0..2 are array(2)+era. Bytes 2..3
        // are 0xd8 0x18 (tag 24). Bytes 4..7 are 0x59 <len_hi> <len_lo>
        // (or 0x58 <len> for shorter). Bump whichever applies.
        match raw[4] {
            0x58 => {
                raw[5] = raw[5].wrapping_add(1);
            }
            0x59 => {
                let len = u16::from_be_bytes([raw[5], raw[6]]) + 1;
                raw[5] = (len >> 8) as u8;
                raw[6] = (len & 0xff) as u8;
            }
            other => panic!("unexpected bstr length prefix {other:#04x}"),
        }

        let info = HeaderInfo::parse(&raw)
            .expect("indefinite-length tuple-encoded announced_eb must parse");
        assert_eq!(info.block_number, 49907);
        assert_eq!(info.announced_eb, Some((eb_hash, 65536)));
        assert_eq!(info.certified_eb, None);
    }

    #[test]
    fn parse_header_with_announced_eb_tuple_plus_certified_eb() {
        // 12-field array: 10 base + announced_eb tuple + certified_eb bool.
        let eb_hash = [0x88; 32];
        let raw = build_test_header_eb_tuple(
            7,
            50000,
            1_001_500,
            Some([0xAA; 32]),
            [0xBB; 32],
            4096,
            [0xCC; 32],
            (eb_hash, 32768),
            Some(true),
        );

        let info = HeaderInfo::parse(&raw).expect("tuple + cert layout must parse");
        assert_eq!(info.announced_eb, Some((eb_hash, 32768)));
        assert_eq!(info.certified_eb, Some(true));
    }

    #[test]
    fn parse_header_with_both_leios_extensions() {
        let eb_hash = [0xEE; 32];
        let raw = build_test_header(
            7,
            100,
            200,
            Some([0xAA; 32]),
            [0xBB; 32],
            2048,
            [0xCC; 32],
            Some((eb_hash, 32768)),
            Some(false),
        );

        let info = HeaderInfo::parse(&raw).expect("should parse");
        assert_eq!(info.announced_eb, Some((eb_hash, 32768)));
        assert_eq!(info.certified_eb, Some(false));
    }

    #[test]
    fn parse_header_genesis_prev_hash() {
        let raw = build_test_header(2, 0, 0, None, [0x11; 32], 0, [0x22; 32], None, None);

        let info = HeaderInfo::parse(&raw).expect("should parse Shelley header");
        assert_eq!(info.era, 2);
        assert_eq!(info.prev_hash, None);
    }

    #[test]
    fn parse_byron_header_returns_none() {
        // Byron header: [0, [word32, #6.24(...)]]
        let mut buf = Vec::new();
        let mut e = Encoder::new(&mut buf);
        e.array(2).unwrap();
        e.u32(0).unwrap(); // Byron regular
        e.array(2).unwrap();
        e.u32(764824073).unwrap();
        e.tag(minicbor::data::Tag::new(24)).unwrap();
        e.bytes(&[0x80]).unwrap();

        assert!(HeaderInfo::parse(&buf).is_none());
    }

    #[test]
    fn parse_invalid_cbor_returns_none() {
        assert!(HeaderInfo::parse(&[0xFF]).is_none());
        assert!(HeaderInfo::parse(&[]).is_none());
    }

    #[test]
    fn parse_trivial_cbor_returns_none() {
        // Trivial CBOR used in test fixtures (e.g., [42])
        let buf = minicbor::to_vec([42u64]).unwrap();
        assert!(HeaderInfo::parse(&buf).is_none());
    }

    #[test]
    fn wrapped_header_new_parses_valid_header() {
        let raw = build_test_header(
            7,
            999,
            5000,
            Some([0xAA; 32]),
            [0xBB; 32],
            512,
            [0xCC; 32],
            None,
            None,
        );
        let header = WrappedHeader::new(raw);
        assert!(header.parsed.is_some());
        assert_eq!(header.parsed.as_ref().unwrap().slot, 5000);
    }

    #[test]
    fn wrapped_header_opaque_skips_parsing() {
        let raw = build_test_header(
            7,
            999,
            5000,
            Some([0xAA; 32]),
            [0xBB; 32],
            512,
            [0xCC; 32],
            None,
            None,
        );
        let header = WrappedHeader::opaque(raw);
        assert!(header.parsed.is_none());
    }

    #[test]
    fn wrapped_header_point_returns_slot_and_hash() {
        let raw = build_test_header(
            7,
            999,
            5000,
            Some([0xAA; 32]),
            [0xBB; 32],
            512,
            [0xCC; 32],
            None,
            None,
        );
        let header = WrappedHeader::new(raw.clone());
        let point = header.point().expect("should derive point");
        match point {
            crate::Point::Specific { slot, hash } => {
                assert_eq!(slot, 5000);
                // Hash should be blake2b-256 of the raw header bytes.
                let expected = header_hash(&raw);
                assert_eq!(hash, expected);
            }
            _ => panic!("expected Point::Specific"),
        }
    }

    #[test]
    fn wrapped_header_point_returns_none_for_opaque() {
        let header = WrappedHeader::opaque(vec![0xA0]);
        assert!(header.point().is_none());
    }

    #[test]
    fn wrapped_header_encode_decode_preserves_parsed() {
        let raw = build_test_header(
            7,
            42,
            100,
            Some([0xAA; 32]),
            [0xBB; 32],
            256,
            [0xCC; 32],
            Some(([0xDD; 32], 1024)),
            Some(true),
        );
        let header = WrappedHeader::new(raw);
        assert!(header.parsed.is_some());

        // Encode → decode should re-parse and get the same fields
        let encoded = minicbor::to_vec(&header).unwrap();
        let decoded: WrappedHeader = minicbor::decode(&encoded).unwrap();
        assert_eq!(decoded.parsed, header.parsed);
    }

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    /// Build a header in the finalized Dijkstra layout (cardano-ledger
    /// #5889): a fixed `array(12)` header_body whose two trailing fields
    /// are `certified_eb : bool` FIRST, then `announced_eb :
    /// [hash32, uint] / nil` LAST.  This is the wire shape sent by the
    /// 2026w27 leios-prototype testnet.
    #[allow(clippy::too_many_arguments)]
    fn build_test_header_dijkstra(
        era: u8,
        block_number: u64,
        slot: u64,
        prev_hash: Option<[u8; 32]>,
        issuer_vkey: [u8; 32],
        body_size: u32,
        block_body_hash: [u8; 32],
        certified_eb: bool,
        announced_eb: Option<([u8; 32], u32)>,
    ) -> Vec<u8> {
        use minicbor::encode::Error as EncodeError;
        use std::io::Write as _;

        let mut body_buf = Vec::new();
        let mut be = Encoder::new(&mut body_buf);
        be.array(HEADER_BODY_BASE_FIELDS + 2).unwrap(); // always 12
        be.u64(block_number).unwrap();
        be.u64(slot).unwrap();
        match prev_hash {
            Some(h) => {
                be.bytes(&h).unwrap();
            }
            None => {
                be.null().unwrap();
            }
        }
        be.bytes(&issuer_vkey).unwrap();
        be.bytes(&[0u8; 32]).unwrap();
        be.array(2).unwrap();
        be.bytes(&[0u8; 32]).unwrap();
        be.bytes(&[0u8; 32]).unwrap();
        be.u32(body_size).unwrap();
        be.bytes(&block_body_hash).unwrap();
        be.array(4).unwrap();
        be.bytes(&[0u8; 32]).unwrap();
        be.u64(0).unwrap();
        be.u64(0).unwrap();
        be.bytes(&[0u8; 64]).unwrap();
        be.array(2).unwrap();
        be.u32(12).unwrap(); // protocol major version 12 (Dijkstra)
        be.u32(0).unwrap();

        // Trailing Leios fields, Dijkstra order: certified_eb THEN announced_eb.
        be.bool(certified_eb).unwrap();
        match announced_eb {
            Some((eb_hash, eb_size)) => {
                be.array(2).unwrap();
                be.bytes(&eb_hash).unwrap();
                be.u32(eb_size).unwrap();
            }
            None => {
                be.null().unwrap();
            }
        }

        let mut inner_buf = Vec::new();
        let mut ie = Encoder::new(&mut inner_buf);
        ie.array(2).unwrap();
        ie.writer_mut()
            .write_all(&body_buf)
            .map_err(EncodeError::write)
            .unwrap();
        ie.bytes(&[0u8; 64]).unwrap();

        let mut outer_buf = Vec::new();
        let mut oe = Encoder::new(&mut outer_buf);
        oe.array(2).unwrap();
        oe.u32(era as u32).unwrap();
        oe.tag(minicbor::data::Tag::new(24)).unwrap();
        oe.bytes(&inner_buf).unwrap();
        outer_buf
    }

    #[test]
    fn parse_dijkstra_header_no_eb() {
        // certified_eb=false, announced_eb=nil → the fixed-12 layout with a
        // trailing CBOR null that older parsers rejected outright.
        let raw = build_test_header_dijkstra(
            6,
            100,
            200,
            Some([0xAA; 32]),
            [0xBB; 32],
            2048,
            [0xCC; 32],
            false,
            None,
        );
        let info = HeaderInfo::parse(&raw).expect("Dijkstra no-EB header must parse");
        assert_eq!(info.certified_eb, Some(false));
        assert_eq!(info.announced_eb, None);
    }

    #[test]
    fn parse_dijkstra_header_with_eb() {
        // certified_eb=true, announced_eb=[hash32, size] as array(2), LAST.
        let eb_hash = [0x77; 32];
        let raw = build_test_header_dijkstra(
            6,
            6202,
            126_440,
            Some([0xAA; 32]),
            [0xBB; 32],
            4,
            [0xCC; 32],
            true,
            Some((eb_hash, 65536)),
        );
        let info = HeaderInfo::parse(&raw).expect("Dijkstra EB header must parse");
        assert_eq!(info.certified_eb, Some(true));
        assert_eq!(info.announced_eb, Some((eb_hash, 65536)));
    }

    /// Real header captured from the 2026w27 leios-prototype dev relay
    /// (leios-node.play.dev.cardano.org), block 0 of the 2026-07-01 respin.
    /// Era 6, fixed array(12) header_body, trailing `f4 f6` =
    /// `certified_eb=false, announced_eb=nil`.  Wire-locks the Dijkstra
    /// layout (cardano-ledger #5889) against a live-network sample.
    #[test]
    fn parse_real_dijkstra_relay_header() {
        const RELAY_HEADER: &str = "8206d818590330828c0017f65820e9716933be318d9d700786213ed6aea5c68ea000f06aa753fd4f24c43dfd3e55582056bc41b070f03712217873d6993b1c42f38030ee2ec5a2b1f37da70f8d81a1d38258407cc17f9894b8730ffb0c8639d036bde764d531d41afbe50a6a1d310821ba7bbeb6877359d99a3d909e6e4475aedd2885b89ef82ea485b7b61ffadf0586b06f335850375477fdb4a2a972979beaec169bd5e6702acdc704b42fa401ef0af52aed1d9628f6a5d17d3c4db096ed490987d95be09fb9b0789559de2cc00b013821487396d92f6b658773294a3f342688bc72bd0704582029571d16f081709b3c48651860077bebf9340abb3fc7133443c54f1f5a5edcf1845820be96953dc057cf17fc360f8927ffc4606ffb7f95e4d32c71c9d1e01b04622081000058403431b1476c0af7b52878c4d43e59f92471b74b02b54aeca0af637b604fc3e5a750bc8ed627550bbe2f9baeacfa072ef6c2f56408ab5a0a84c91f8e886bdf3c0f820c00f4f65901c03895fb429176938da031ba1b7232580b5d50ef324ecfcb51f2889a09f775f15dee08940e925aba982e1fea3582f1b88f2ac8e4cf39fe8ee85352b0479cc8a8010db898100a2023805598a6dbbca20cfb751d044f7e70dc15f43948af5b17dacfc2c720c37232ec22128d25050e8aa9c329c999c70747c2a8247a0245a8380d70daab2b5b5e782155da5fdfe3fe7310ccf7e9c3abce721edbf273b8a773d3fc8f72811bf37519d78c991ca5440918fe4355ff1a64c262b2241d04970327d156d21f2d41fb42ab0cd3a1b20210fa75da2f337d3ac2844c1b5c538a3dfd0d3367d465ae2a8f5727e352e6414823128494bd9cfeca2dfed64e60c807be91dbe871696ec5cb0e12d7eefc30534d312b91fded38cad2daeca90689ddb826109443d4e66bfcad27cb4f4e18f7d0dcfd1882228e4db1dc56da72c0380530707658e8e4bbcb14df4ca7494f36adc1439125bf229fd345cdf400448589ba08bc20bf6e7c7a487e08d8ff05ca0584368127fcc58debeae0fcb29e270607f9fdf6499b4cb47a6a9440b411adb7aa6311210068bbbf6a5bbe456e80fd22efc82ff1574af1e8fcfe42a12d76dee5c7e6114e79aefbd40c819a62a566288039d4030316186ae881";
        let raw = hex(RELAY_HEADER);
        let info = HeaderInfo::parse(&raw).expect("real Dijkstra relay header must parse");
        assert_eq!(info.era, 6);
        assert_eq!(info.block_number, 0);
        assert_eq!(info.slot, 23);
        assert_eq!(info.prev_hash, None); // genesis successor
        assert_eq!(info.body_size, 4);
        assert_eq!(info.certified_eb, Some(false));
        assert_eq!(info.announced_eb, None);
    }
}
