//! Raw block bodies with optional Leios metadata (CIP-0164).

use std::sync::atomic::{AtomicUsize, Ordering};

use minicbor::decode::Error as DecodeError;
use minicbor::encode::Error as EncodeError;
use minicbor::{Decoder, Encoder};
use tracing::warn;

use shared_consensus::praos::{LeiosCertSummary, ParsedBodyInfo};

use crate::{Point, MAX_BLOCK_SIZE};

/// Number of base fields in a Conway+ block array (before Leios extensions):
/// `[header, tx_bodies, tx_witnesses, aux_data, invalid_transactions]`.
/// Confirmed against live musashi blocks (every base body decodes as `array(5)`
/// with no certificate); the `leios_certificate` is the 6th element.
const BLOCK_BASE_FIELDS: u64 = 5;

/// First era tag whose block wraps the transaction list one level deeper.
/// Pre-Leios eras (Conway = 6, Dijkstra = 7) encode `tx_bodies` directly as
/// block field 1; Leios (era ≥ 8) encodes field 1 as a body wrapper
/// `[?, tx_list, ?, ?]` where `tx_list` holds `[tx_body, witness_set, aux]`
/// triples.
const LEIOS_ERA: u32 = 8;

/// Soft cap on the number of `praos_inspect` parse-failure WARN lines
/// emitted per process.  Without this, `praos_inspect()` silently
/// returns `ParsedBodyInfo::default()` on any decode error and the
/// failure is invisible — see the dev-relay's post-slot-1309596
/// EB-announcing blocks for the original motivation.
const BODY_PARSE_FAIL_WARN_BUDGET: usize = 5;

/// Bytes to capture from the start of a failing body for hex dump.
/// Large enough to expose the CBOR tag, era field, top-level array
/// length, and the first few merged_block fields — enough to identify
/// an unrecognised wire shape on first contact.
const BODY_PARSE_FAIL_PROBE_BYTES: usize = 256;

static BODY_PARSE_FAIL_WARNS: AtomicUsize = AtomicUsize::new(0);

/// Count (and skip over) a CBOR array's elements, definite or indefinite.
fn count_and_skip_array(d: &mut Decoder) -> Result<u32, DecodeError> {
    match d.array()? {
        Some(n) => {
            let n =
                u32::try_from(n).map_err(|_| DecodeError::message("array length exceeds u32"))?;
            for _ in 0..n {
                d.skip()?;
            }
            Ok(n)
        }
        None => {
            let mut n: u32 = 0;
            while d.datatype()? != minicbor::data::Type::Break {
                d.skip()?;
                n = n.saturating_add(1);
            }
            d.skip()?; // consume the break
            Ok(n)
        }
    }
}

/// Count the transactions in a Leios (era ≥ 8) block body wrapper.
///
/// The wrapper is `[?, tx_list, ?, ?]` (null placeholders around the payload);
/// `tx_list` is the array of `[tx_body, witness_set, aux]` triples. Return the
/// length of that inner tx list — the real transaction count — by descending
/// into the first array-typed element of the wrapper.
fn count_leios_tx_list(d: &mut Decoder) -> Result<u32, DecodeError> {
    use minicbor::data::Type;
    fn is_array(t: Type) -> bool {
        matches!(t, Type::Array | Type::ArrayIndef)
    }

    let mut count = 0u32;
    let mut found = false;
    match d.array()? {
        Some(n) => {
            for _ in 0..n {
                if !found && is_array(d.datatype()?) {
                    count = count_and_skip_array(d)?;
                    found = true;
                } else {
                    d.skip()?;
                }
            }
        }
        None => loop {
            let t = d.datatype()?;
            if t == Type::Break {
                d.skip()?; // consume the break
                break;
            }
            if !found && is_array(t) {
                count = count_and_skip_array(d)?;
                found = true;
            } else {
                d.skip()?;
            }
        },
    }
    Ok(count)
}

// --- LeiosBlockInfo ---

/// Leios metadata parsed from a block body (CIP-0164).
///
/// The EB certificate is extracted from real blocks received via BlockFetch.
/// The Shelley+ block structure is:
///   `#6.24(bytes .cbor [era_tag, [header, tx_bodies, tx_witnesses, aux_data,
///                                 invalid_transactions, ?eb_certificate]])`
/// Base field count = 5; a 6th element is the EB certificate.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LeiosBlockInfo {
    /// Opaque EB certificate bytes, if present in this block.
    pub eb_certificate: Option<Vec<u8>>,
}

impl LeiosBlockInfo {
    /// Try to parse Leios metadata from raw BlockBody bytes.
    ///
    /// Returns None for Byron blocks or blocks without an EB certificate.
    /// Parsing failures are silent — this is best-effort extraction.
    pub fn parse(raw: &[u8]) -> Option<Self> {
        Self::try_parse(raw).ok()
    }

    fn try_parse(raw: &[u8]) -> Result<Self, DecodeError> {
        let mut d = Decoder::new(raw);

        // Unwrap #6.24 tag
        let tag = d.tag()?;
        if tag.as_u64() != 24 {
            return Err(DecodeError::message(format!(
                "expected CBOR tag 24, got {}",
                tag.as_u64()
            )));
        }
        let inner_bytes = d.bytes()?;

        // Inner: [era_tag, era_block]
        let mut inner = Decoder::new(inner_bytes);
        let _outer_len = inner.array()?;
        let era = inner.u32()?;

        // Byron (era 0 or 1) — no Leios support
        if era < 2 {
            return Err(DecodeError::message("Byron block"));
        }

        let block_len = match inner.array()? {
            Some(n) => n,
            None => return Err(DecodeError::message("indefinite block array")),
        };

        // Extract the opaque leios_certificate bytes, era-aware.
        let (cert_start, cert_end) = if era >= LEIOS_ERA {
            // Era-8 Leios: block = [header, block_body].
            //
            // Two block_body layouts exist and are told apart by arity, so a
            // chain from either era parses:
            //   w36+ (3 fields): [transactions, leios_certificate/nil,
            //                     peras_certificate/nil]
            //   pre-w36 (4):     [invalid_transactions/nil, transactions,
            //                     leios_certificate/nil, peras_certificate/nil]
            // In both the certificate is the field just after `transactions`;
            // nil means no cert. Keying off arity matters because the 4-field
            // reader applied to a 3-field body silently returns the
            // PERAS certificate as if it were the Leios one.
            if block_len < 2 {
                return Err(DecodeError::message("era-8 block missing block_body"));
            }
            inner.skip()?; // field 0: header
            let bb_len = match inner.array()? {
                Some(n) => n,
                None => return Err(DecodeError::message("indefinite block_body")),
            };
            let tx_field_index = match bb_len {
                3 => 0, // w36+: transactions first
                n if n >= 4 => 1, // pre-w36: invalid_transactions leads
                _ => {
                    return Err(DecodeError::message("block_body missing leios_certificate slot"))
                }
            };
            for _ in 0..=tx_field_index {
                inner.skip()?; // up to and including `transactions`
            }
            if inner.datatype()? == minicbor::data::Type::Null {
                return Err(DecodeError::message("no leios certificate"));
            }
            let start = inner.position();
            inner.skip()?; // leios_certificate
            (start, inner.position())
        } else {
            // Era-7 (flat Conway) backward-compat: block =
            //   [header, tx_bodies, tx_witnesses, aux_data, invalid_transactions,
            //    ?eb_certificate]
            if block_len <= BLOCK_BASE_FIELDS {
                return Err(DecodeError::message("no Leios extension fields"));
            }
            for _ in 0..BLOCK_BASE_FIELDS {
                inner.skip()?;
            }
            let start = inner.position();
            inner.skip()?; // the trailing eb_certificate
            (start, inner.position())
        };

        let cert_bytes = inner_bytes
            .get(cert_start..cert_end)
            .ok_or_else(|| DecodeError::message("failed to extract certificate bytes"))?;

        Ok(LeiosBlockInfo {
            eb_certificate: Some(cert_bytes.to_vec()),
        })
    }
}

// --- BlockBody ---

/// A full block stored as raw CBOR bytes (including the #6.24 tag wrapper),
/// with optional parsed Leios metadata.
///
/// For Shelley+ blocks with a CIP-0164 EB certificate, `leios` contains the
/// extracted certificate bytes. For blocks without one, `leios` is None.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockBody {
    /// Raw CBOR bytes of the block.
    pub raw: Vec<u8>,
    /// Parsed Leios metadata (Shelley+ only). None if no certificate or parse failure.
    pub leios: Option<LeiosBlockInfo>,
}

impl BlockBody {
    /// Create a BlockBody from raw bytes, attempting to parse Leios metadata.
    pub fn new(raw: Vec<u8>) -> Self {
        let leios = LeiosBlockInfo::parse(&raw);
        Self { raw, leios }
    }

    /// Create a BlockBody from raw bytes without parsing.
    /// Use for test fixtures with trivial CBOR that isn't a real block.
    pub fn opaque(raw: Vec<u8>) -> Self {
        Self { raw, leios: None }
    }

    /// Derive the chain Point (slot + header hash) from this block's raw bytes.
    ///
    /// Extracts the header from the block, parses it for the slot number,
    /// and computes Blake2b-256 of the header CBOR for the block hash.
    /// Returns None for Byron blocks or unparseable data.
    pub fn point(&self) -> Option<Point> {
        self.try_point().ok()
    }

    /// Extract the header from this block body as a WrappedHeader.
    ///
    /// Returns None for Byron blocks or unparseable data.
    pub fn header(&self) -> Option<crate::WrappedHeader> {
        let buf = self.try_extract_header().ok()?;
        Some(crate::WrappedHeader::new(buf))
    }

    /// One-pass inspection of a Conway+ Praos `merged_block`.
    ///
    /// Returns a populated [`ParsedBodyInfo`] when the block parses;
    /// `ParsedBodyInfo::default()` (i.e. all zeros / `None`) on any
    /// decode failure (Byron, opaque test fixtures, malformed CBOR).
    ///
    /// Effective `merged_block` layout on the dev-relay leios-prototype
    /// chain (cardano-ledger `DijkstraBlockBody`); both trailing slots
    /// are always emitted via `encodeNullStrictMaybe`-style optionality:
    ///
    /// ```text
    /// merged_block = [
    ///   header,
    ///   transaction_bodies,
    ///   transaction_witness_sets,
    ///   auxiliary_data_set,
    ///   invalid_transactions,
    ///   ? eb_certificate,
    ///   ? peras_cert,
    /// ]
    /// ```
    ///
    /// Field-count mapping:
    ///   5 — base Conway body, no Leios/Peras trailing slots
    ///   6 — first trailing slot present (`eb_certificate`)
    ///   7 — both trailing slots present (`eb_certificate` + `peras_cert`)
    ///
    /// Each trailing slot is parsed as one of three CBOR shapes:
    ///   - `null` (`f6`) — absent.
    ///   - `array(0)` (`80`) — unit placeholder: "the producer's on
    ///     this branch but the cert encoding isn't implemented yet"
    ///     (per Sebastian 2026-06-15). Counted via the corresponding
    ///     `*_pending` flag on `ParsedBodyInfo`.
    ///   - For `eb_certificate` only: a real `array(4)` cert, decoded
    ///     by `try_decode_leios_cert` into `LeiosCertSummary`.
    ///
    /// `eb_certificate` is `Some` iff the first trailing optional
    /// decodes as the CIP-0164 `leios_certificate = [slot_no,
    /// endorser_block_hash : hash32, signers : bytes,
    /// aggregated_signature : bytes .size 48]`. Only `slot_no` and
    /// `endorser_block_hash` are surfaced; the bitfield and BLS
    /// signature stay in the raw bytes.
    ///
    /// `peras_cert` has no known CDDL shape beyond `null` / `array(0)`;
    /// anything else fails the body parse (surfaced via the
    /// BODY_PARSE_FAIL_WARN with raw-byte hex prefix).
    pub fn praos_inspect(&self) -> ParsedBodyInfo {
        match self.try_praos_inspect() {
            Ok(info) => info,
            Err(e) => {
                // `praos_inspect()`'s contract is best-effort, but a
                // body we can't decode is otherwise invisible to
                // operators (returns `field_count=0` and looks like an
                // empty block).  Dump a hex prefix so an unrecognised
                // wire shape can be identified on first contact.
                // Throttled like the trailing-optional shape mismatch
                // above — process-global, not per-peer.
                if BODY_PARSE_FAIL_WARNS.fetch_add(1, Ordering::Relaxed)
                    < BODY_PARSE_FAIL_WARN_BUDGET
                {
                    let take = self.raw.len().min(BODY_PARSE_FAIL_PROBE_BYTES);
                    warn!(
                        body_bytes = self.raw.len(),
                        error = %e,
                        raw_prefix_hex = %hex_prefix(Some(&self.raw[..take])),
                        "praos body parse failed; dumping raw prefix"
                    );
                }
                ParsedBodyInfo::default()
            }
        }
    }

    fn try_praos_inspect(&self) -> Result<ParsedBodyInfo, DecodeError> {
        let mut d = Decoder::new(&self.raw);

        let tag = d.tag()?;
        if tag.as_u64() != 24 {
            return Err(DecodeError::message("expected CBOR tag 24"));
        }
        let inner_bytes = d.bytes()?;

        let mut inner = Decoder::new(inner_bytes);
        let _outer_len = inner.array()?;
        let era = inner.u32()?;
        if era < 2 {
            return Err(DecodeError::message("Byron block"));
        }
        let block_len = match inner.array()? {
            Some(n) => n,
            None => return Err(DecodeError::message("indefinite block array")),
        };
        let field_count = u32::try_from(block_len)
            .map_err(|_| DecodeError::message("merged_block length exceeds u32"))?;
        if block_len < 2 {
            return Ok(ParsedBodyInfo {
                field_count,
                ..ParsedBodyInfo::default()
            });
        }

        // Field 0: header — skip.
        inner.skip()?;

        // Era-8 Leios: field 1 is the `block_body` array, and BOTH the
        // transaction list and the certificate slots live inside it. Parse it
        // here rather than falling through to the trailing-optional probes
        // below, which describe the pre-Leios (era-7 flat Conway) shape where
        // the cert really was a trailing field of the *block* array.
        //
        // Getting this wrong was silent: for era-8 `block_len == 2`, so
        // `base_remaining` and `trailing` both came out 0, the cert probe never
        // ran, and `eb_certificate` was None for EVERY era-8 block whether or
        // not it carried one. That fed `certified_eb` and the EB-application
        // path in shared-consensus praos, and made the `body_cert_eb_slot`
        // telemetry structurally unable to report a cert.
        if era >= LEIOS_ERA {
            return Self::inspect_leios_body(&mut inner, field_count);
        }

        // Field 1 (era < 8): `tx_bodies` (`[* transaction_body]`) directly —
        // count its length. Accepts definite and indefinite arrays: dev-relay
        // blocks around the Leios era encode arrays as `9f … ff`, and returning
        // Err would silently default the whole body info (`field_count = 0`).
        let tx_count = count_and_skip_array(&mut inner)?;
        // Skip the rest of the Conway base: tx_witness_sets,
        // auxiliary_data_set, invalid_transactions.  We treat the count
        // permissively to keep working if the era/CDDL adds another
        // mandatory field — at worst the trailing-optional probes below
        // see the wrong field and bail to `None`, never corrupt state.
        let base_remaining = block_len.saturating_sub(2).min(3);
        for _ in 0..base_remaining {
            inner.skip()?;
        }

        let trailing = block_len.saturating_sub(2 + base_remaining);

        let mut eb_certificate = None;
        let mut eb_certificate_pending = false;
        let mut peras_cert_pending = false;

        // Trailing optional 1: `eb_certificate` (CIP-0164 `leios_certificate`).
        // Three observed shapes:
        //   - CBOR `null` (`f6`)   → Absent. Older `encodeNullStrictMaybe`
        //     style from era-7 Dijkstra producers.
        //   - CBOR `array(0)` (`80`) → Pending. The leios-prototype's
        //     "unit" placeholder (Sebastian, 2026-06-15): "there is a
        //     cert here but the encoding isn't finished yet". Counted
        //     separately from Absent — both leave eb_certificate as
        //     None but `eb_certificate_pending` flips.
        //   - `array(4) [slot, hash, signers, sig]` → real cert.
        //   - anything else → parse failure (surfaced via caller's
        //     hex dump).
        if trailing >= 1 {
            match classify_cert_slot(&mut inner)? {
                CertSlotState::Absent => {}
                CertSlotState::Pending | CertSlotState::PresentWithoutIdentity => {
                    eb_certificate_pending = true;
                }
                CertSlotState::Other => {
                    eb_certificate = Some(try_decode_leios_cert(&mut inner)?);
                }
            }
        }

        // Trailing optional 2: `peras_cert`. We don't yet know its CDDL
        // shape. Accept the same Absent (`null`) / Pending (`[]`)
        // sentinels as the eb_certificate slot; anything else fails
        // the parse so an unknown layout surfaces in the WARN's hex
        // dump rather than getting silently misinterpreted.
        if trailing >= 2 {
            match classify_cert_slot(&mut inner)? {
                CertSlotState::Absent => {}
                CertSlotState::Pending | CertSlotState::PresentWithoutIdentity => {
                    peras_cert_pending = true;
                }
                CertSlotState::Other => {
                    return Err(DecodeError::message(
                        "peras_cert slot has unknown shape; layout not yet known",
                    ));
                }
            }
        }

        Ok(ParsedBodyInfo {
            tx_count,
            field_count,
            eb_certificate,
            eb_certificate_pending,
            peras_cert_pending,
        })
    }

    /// Inspect an era-8 `block_body`, positioned at the body array.
    ///
    /// Two layouts, told apart by arity so a chain from either era parses:
    ///   w36+ (3 fields): `[transactions, leios_cert/nil, peras_cert/nil]`
    ///   pre-w36 (4):     `[invalid_transactions/nil, transactions,
    ///                      leios_cert/nil, peras_cert/nil]`
    ///
    /// In both, the certificate is the field immediately after `transactions`
    /// and the peras slot the one after that.
    fn inspect_leios_body(
        inner: &mut Decoder,
        field_count: u32,
    ) -> Result<ParsedBodyInfo, DecodeError> {
        let bb_len = match inner.array()? {
            Some(n) => n,
            None => return Err(DecodeError::message("indefinite era-8 block_body")),
        };
        let tx_field_index = match bb_len {
            3 => 0,           // w36+: transactions first
            n if n >= 4 => 1, // pre-w36: invalid_transactions leads
            _ => {
                // Too short to be either layout; report what we know rather
                // than guessing at the slots.
                return Ok(ParsedBodyInfo {
                    field_count,
                    ..ParsedBodyInfo::default()
                });
            }
        };
        for _ in 0..tx_field_index {
            inner.skip()?;
        }
        let tx_count = count_and_skip_array(inner)?;

        let mut eb_certificate = None;
        let mut eb_certificate_pending = false;
        let mut peras_cert_pending = false;

        // `leios_certificate`. Shapes, as for the era-7 slot: `null` = Absent,
        // `array(0)` = the prototype's "pending" placeholder, `array(4)`
        // `[slot, hash, signers, sig]` = a real cert, anything else = parse
        // failure surfaced via the caller's hex dump.
        if bb_len >= tx_field_index + 2 {
            match classify_cert_slot(inner)? {
                CertSlotState::Absent => {}
                // Both mean "a cert is here but it names no EB". The consumer
                // treats `pending` as certified and resolves the EB from the
                // parent RB's announcement, which is exactly right for the
                // w36 2-tuple.
                CertSlotState::Pending | CertSlotState::PresentWithoutIdentity => {
                    eb_certificate_pending = true
                }
                CertSlotState::Other => {
                    eb_certificate = Some(try_decode_leios_cert(inner)?);
                }
            }
        }

        // `peras_certificate`. CDDL shape still unknown, so accept only the
        // Absent / Pending sentinels and fail the parse otherwise, rather than
        // silently misinterpreting a layout we have not seen.
        if bb_len >= tx_field_index + 3 {
            match classify_cert_slot(inner)? {
                CertSlotState::Absent => {}
                CertSlotState::Pending | CertSlotState::PresentWithoutIdentity => {
                    peras_cert_pending = true
                }
                CertSlotState::Other => {
                    return Err(DecodeError::message(
                        "peras_cert slot has unknown shape; layout not yet known",
                    ));
                }
            }
        }

        Ok(ParsedBodyInfo {
            tx_count,
            field_count,
            eb_certificate,
            eb_certificate_pending,
            peras_cert_pending,
        })
    }

    /// Extract the header from this block in ChainSync wire format:
    /// `[era_tag, #6.24(header_cbor)]`.
    ///
    /// Inside the block, the header is stored as raw CBOR `[header_body, sig]`.
    /// This method wraps it in `#6.24` to match the ChainSync wire format,
    /// ensuring consistent hashing and downstream compatibility.
    fn try_extract_header(&self) -> Result<Vec<u8>, DecodeError> {
        let mut d = Decoder::new(&self.raw);

        // Unwrap #6.24 tag
        let tag = d.tag()?;
        if tag.as_u64() != 24 {
            return Err(DecodeError::message("expected CBOR tag 24"));
        }
        let inner_bytes = d.bytes()?;

        // Inner: [era_tag, era_block]
        let mut inner = Decoder::new(inner_bytes);
        let _outer_len = inner.array()?;
        let era = inner.u32()?;

        if era < 2 {
            return Err(DecodeError::message("Byron block"));
        }

        // era_block: [header, tx_bodies, ...]
        // Record position before/after header to extract its raw bytes.
        let _block_len = inner.array()?;
        let header_start = inner.position();
        inner.skip()?; // skip header
        let header_end = inner.position();

        let header_inner_bytes = inner_bytes
            .get(header_start..header_end)
            .ok_or_else(|| DecodeError::message("failed to extract header bytes"))?;

        // Reconstruct in ChainSync wire format: [era_tag, #6.24(header_cbor)]
        let mut header_buf = Vec::new();
        let mut he = Encoder::new(&mut header_buf);
        he.array(2)
            .map_err(|_| DecodeError::message("encode error"))?;
        he.u32(era)
            .map_err(|_| DecodeError::message("encode error"))?;
        he.tag(minicbor::data::Tag::new(24))
            .map_err(|_| DecodeError::message("encode error"))?;
        he.bytes(header_inner_bytes)
            .map_err(|_| DecodeError::message("encode error"))?;

        Ok(header_buf)
    }

    fn try_point(&self) -> Result<Point, DecodeError> {
        let header_buf = self.try_extract_header()?;

        // Parse header for slot.
        let info = crate::HeaderInfo::parse(&header_buf)
            .ok_or_else(|| DecodeError::message("failed to parse header"))?;

        // Compute Blake2b-256 of the full header CBOR for the block hash.
        let hash = crate::header::header_hash(&header_buf);

        Ok(Point::Specific {
            slot: info.slot,
            hash,
        })
    }
}

impl minicbor::Encode<()> for BlockBody {
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

impl<'a> minicbor::Decode<'a, ()> for BlockBody {
    fn decode(d: &mut Decoder<'a>, _ctx: &mut ()) -> Result<Self, DecodeError> {
        let start = d.position();
        d.skip()?;
        let end = d.position();
        let len = end - start;
        if len > MAX_BLOCK_SIZE {
            return Err(DecodeError::message(format!(
                "block too large: {len} bytes exceeds limit {MAX_BLOCK_SIZE}"
            )));
        }
        let raw = d
            .input()
            .get(start..end)
            .ok_or_else(|| DecodeError::message("failed to extract block bytes"))?;
        Ok(BlockBody::new(raw.to_vec()))
    }
}

/// Format the given bytes as a lowercase hex string, or `"none"` when
/// the caller passed `None`. The caller controls the slice length;
/// `hex_prefix` itself imposes no cap.
fn hex_prefix(bytes: Option<&[u8]>) -> String {
    use std::fmt::Write as _;
    match bytes {
        Some(b) => {
            let mut s = String::with_capacity(b.len() * 2);
            for x in b {
                let _ = write!(s, "{x:02x}");
            }
            s
        }
        None => "none".to_string(),
    }
}

/// Three-state classification of a trailing-optional cert slot on
/// the dev-relay's Leios-prototype chain.
enum CertSlotState {
    /// CBOR `null` (`f6`) — no cert declared at this slot.
    Absent,
    /// CBOR `array(0)` (`80`) — the "unit" placeholder per
    /// Sebastian (2026-06-15): block asserts a cert exists at
    /// this slot but the cert encoding isn't implemented yet.
    /// Counted separately from `Absent` so we can see how often
    /// this signal fires on the chain.
    Pending,
    /// CBOR `array(2)` — the real w36 on-chain shape,
    /// `[signers_bitfield, aggregated_signature]`. A certificate IS
    /// present, but it carries no EB slot/hash, so the EB it certifies
    /// can only be resolved from the parent RB's announcement
    /// (`parent_announced_eb_for_cert`). Verified against a cert-bearing
    /// block pulled from a w36 node's VolatileDB: `[bytes[1], bytes[48]]`,
    /// with the header's `leios_certified` bit set.
    PresentWithoutIdentity,
    /// Anything else: a real cert (or an unknown shape that the
    /// caller will decode / fail on).
    Other,
}

/// Classify and conditionally consume the next CBOR item.  `Absent`
/// and `Pending` consume the sentinel and leave the decoder pointing
/// at the next field; `Other` leaves the decoder where it was so the
/// caller can attempt a structured decode.
fn classify_cert_slot(d: &mut Decoder<'_>) -> Result<CertSlotState, DecodeError> {
    use minicbor::data::Type;
    match d.datatype()? {
        Type::Null => {
            d.skip()?;
            Ok(CertSlotState::Absent)
        }
        Type::Array | Type::ArrayIndef => {
            let mut probe = d.probe();
            match probe.array() {
                Ok(Some(0)) => {
                    d.array()?; // consume the `array(0)` from the real decoder
                    Ok(CertSlotState::Pending)
                }
                // The w36 wire cert: `[signers_bitfield, aggregated_signature]`.
                // No eb_slot/eb_hash to lift, so skip it whole and let the
                // caller resolve the EB from the parent's announcement.
                Ok(Some(2)) => {
                    d.skip()?;
                    Ok(CertSlotState::PresentWithoutIdentity)
                }
                _ => Ok(CertSlotState::Other),
            }
        }
        _ => Ok(CertSlotState::Other),
    }
}

/// Attempt to decode the next CBOR element as a CIP-0164
/// `leios_certificate`:
///
/// ```cddl
/// leios_certificate = [
///   slot_no               : uint
/// , endorser_block_hash   : hash32
/// , signers               : bytes
/// , aggregated_signature  : leios_bls_signature
/// ]
/// leios_bls_signature      = bytes .size 48
/// endorser_block_hash      = bytes .size 32
/// ```
///
/// Validates the array length, the eb_hash size, and the BLS signature
/// size; the variable-length `signers` bitfield is accepted as any
/// bytes.  Returns `Err` on any deviation so the caller can either
/// surface the body as un-decodable or, for the optional-cert path,
/// distinguish "absent" (CBOR null, handled upstream) from "malformed".
fn try_decode_leios_cert(d: &mut Decoder<'_>) -> Result<LeiosCertSummary, DecodeError> {
    match d.array()? {
        Some(4) => {}
        Some(other) => {
            return Err(DecodeError::message(format!(
                "leios_certificate expected array(4), got array({other})"
            )));
        }
        None => {
            return Err(DecodeError::message(
                "leios_certificate indefinite array not supported",
            ));
        }
    }
    let eb_slot = d.u64()?;
    let eb_hash_bytes = d.bytes()?;
    if eb_hash_bytes.len() != 32 {
        return Err(DecodeError::message(format!(
            "leios_certificate eb_hash expected 32 bytes, got {}",
            eb_hash_bytes.len()
        )));
    }
    let mut eb_hash = [0u8; 32];
    eb_hash.copy_from_slice(eb_hash_bytes);
    // signers: variable-length bytes bitfield over the committee.
    // Reject non-bytes types but accept any length.
    let _signers = d.bytes()?;
    // aggregated_signature: leios_bls_signature = bytes .size 48.
    let agg_sig = d.bytes()?;
    if agg_sig.len() != 48 {
        return Err(DecodeError::message(format!(
            "leios_certificate aggregated_signature expected 48 bytes, got {}",
            agg_sig.len()
        )));
    }
    Ok(LeiosCertSummary { eb_slot, eb_hash })
}

#[cfg(test)]
mod tests {
    use super::*;
    use minicbor::Encoder;

    /// Build a fake Shelley+ block body for testing.
    /// Produces: #6.24(bytes .cbor [era_tag, [header, txs, witnesses, aux, ?cert]])
    fn build_test_block(era: u8, eb_certificate: Option<&[u8]>) -> Vec<u8> {
        build_test_block_with_tx_count(era, 0, eb_certificate)
    }

    /// Like `build_test_block` but with a configurable `tx_bodies` array length.
    fn build_test_block_with_tx_count(
        era: u8,
        tx_count: u64,
        eb_certificate: Option<&[u8]>,
    ) -> Vec<u8> {
        use std::io::Write as _;
        let field_count = BLOCK_BASE_FIELDS + if eb_certificate.is_some() { 1 } else { 0 };

        // Build inner block array: [header, txs, witnesses, aux, ?cert]
        let mut block_buf = Vec::new();
        let mut be = Encoder::new(&mut block_buf);
        be.array(field_count).unwrap();
        be.bytes(&[0x80]).unwrap(); // dummy header
        be.array(tx_count).unwrap(); // tx_bodies (variable length, empty entries)
        for _ in 0..tx_count {
            be.null().unwrap();
        }
        be.array(0).unwrap(); // empty tx_witnesses
        be.null().unwrap(); // null auxiliary_data
        be.array(0).unwrap(); // invalid_transactions (Conway base field 5)
        if let Some(cert) = eb_certificate {
            be.bytes(cert).unwrap();
        }

        // Build outer: [era_tag, block_array]
        let mut inner_buf = Vec::new();
        let mut ie = Encoder::new(&mut inner_buf);
        ie.array(2).unwrap();
        ie.u32(era as u32).unwrap();
        ie.writer_mut().write_all(&block_buf).unwrap();

        // Wrap in #6.24
        let mut outer_buf = Vec::new();
        let mut oe = Encoder::new(&mut outer_buf);
        oe.tag(minicbor::data::Tag::new(24)).unwrap();
        oe.bytes(&inner_buf).unwrap();

        outer_buf
    }

    /// Like `build_test_block_with_tx_count`, but appends `trailing` as
    /// already-encoded CBOR for the 6th block field (rather than
    /// bstr-wrapping it). Used to embed a bare `leios_certificate` array,
    /// which `praos_inspect` decodes in place.
    fn build_test_block_with_raw_trailing(era: u8, tx_count: u64, trailing: &[u8]) -> Vec<u8> {
        use std::io::Write as _;
        let mut block_buf = Vec::new();
        let mut be = Encoder::new(&mut block_buf);
        be.array(BLOCK_BASE_FIELDS + 1).unwrap();
        be.bytes(&[0x80]).unwrap(); // dummy header
        be.array(tx_count).unwrap(); // tx_bodies
        for _ in 0..tx_count {
            be.null().unwrap();
        }
        be.array(0).unwrap(); // tx_witnesses
        be.null().unwrap(); // auxiliary_data
        be.array(0).unwrap(); // invalid_transactions (Conway base field 5)
        be.writer_mut().write_all(trailing).unwrap(); // raw 6th field

        let mut inner_buf = Vec::new();
        let mut ie = Encoder::new(&mut inner_buf);
        ie.array(2).unwrap();
        ie.u32(era as u32).unwrap();
        ie.writer_mut().write_all(&block_buf).unwrap();

        let mut outer_buf = Vec::new();
        let mut oe = Encoder::new(&mut outer_buf);
        oe.tag(minicbor::data::Tag::new(24)).unwrap();
        oe.bytes(&inner_buf).unwrap();
        outer_buf
    }

    /// Build a Leios (era ≥ 8) block whose body field 1 is the wrapper
    /// `[null, tx_list, null, null]`, with `tx_count` triples in `tx_list`.
    /// Mirrors the shape observed live on the dev testnet (each triple is
    /// `[tx_body, witness_set, aux]`).
    fn build_leios_block_with_tx_count(era: u8, tx_count: u64) -> Vec<u8> {
        use std::io::Write as _;

        // Inner block: [header, body_wrapper]
        let mut block_buf = Vec::new();
        let mut be = Encoder::new(&mut block_buf);
        be.array(2).unwrap();
        be.bytes(&[0x80]).unwrap(); // dummy header
                                    // body wrapper: [null, tx_list, null, null]
        be.array(4).unwrap();
        be.null().unwrap();
        be.array(tx_count).unwrap(); // tx_list
        for _ in 0..tx_count {
            // each tx is a triple [body, witnesses, aux] — contents don't
            // matter for the count, only the outer array length.
            be.array(3).unwrap();
            be.map(0).unwrap();
            be.map(0).unwrap();
            be.null().unwrap();
        }
        be.null().unwrap();
        be.null().unwrap();

        // Outer: [era_tag, block]
        let mut inner_buf = Vec::new();
        let mut ie = Encoder::new(&mut inner_buf);
        ie.array(2).unwrap();
        ie.u32(era as u32).unwrap();
        ie.writer_mut().write_all(&block_buf).unwrap();

        // Wrap in #6.24
        let mut outer_buf = Vec::new();
        let mut oe = Encoder::new(&mut outer_buf);
        oe.tag(minicbor::data::Tag::new(24)).unwrap();
        oe.bytes(&inner_buf).unwrap();
        outer_buf
    }

    /// Build an era-8 block whose `block_body` has `bb_len` fields, carrying
    /// `tx_count` txs and optionally a real `leios_certificate`.
    ///
    /// bb_len 3 => [transactions, cert/nil, nil]            (w36+)
    /// bb_len 4 => [nil, transactions, cert/nil, nil]       (pre-w36)
    fn build_era8_block_body(bb_len: u64, tx_count: u64, cert: Option<&[u8]>) -> Vec<u8> {
        use std::io::Write as _;

        let mut block_buf = Vec::new();
        let mut be = Encoder::new(&mut block_buf);
        be.array(2).unwrap();
        be.bytes(&[0x80]).unwrap(); // dummy header
        be.array(bb_len).unwrap();
        if bb_len >= 4 {
            be.null().unwrap(); // [0] invalid_transactions (pre-w36 only)
        }
        be.array(tx_count).unwrap(); // transactions
        for _ in 0..tx_count {
            be.array(3).unwrap();
            be.map(0).unwrap();
            be.map(0).unwrap();
            be.null().unwrap();
        }
        match cert {
            Some(c) => {
                be.writer_mut().write_all(c).unwrap();
            }
            None => {
                be.null().unwrap();
            }
        }
        be.null().unwrap(); // peras_certificate

        let mut inner_buf = Vec::new();
        let mut ie = Encoder::new(&mut inner_buf);
        ie.array(2).unwrap();
        ie.u32(8).unwrap();
        ie.writer_mut().write_all(&block_buf).unwrap();

        let mut outer_buf = Vec::new();
        let mut oe = Encoder::new(&mut outer_buf);
        oe.tag(minicbor::data::Tag::new(24)).unwrap();
        oe.bytes(&inner_buf).unwrap();
        outer_buf
    }

    /// A real `leios_certificate`: `[slot, hash32, signers, sig]`.
    fn sample_leios_cert(eb_slot: u64) -> Vec<u8> {
        let mut c = Vec::new();
        let mut e = Encoder::new(&mut c);
        e.array(4).unwrap();
        e.u64(eb_slot).unwrap();
        e.bytes(&[0xABu8; 32]).unwrap();
        e.bytes(&[0x63, 0xff, 0xff]).unwrap();
        e.bytes(&[0x11u8; 48]).unwrap();
        c
    }

    #[test]
    fn era8_praos_inspect_extracts_cert_from_inside_block_body() {
        // Regression: for era-8 the block array is [header, block_body], so
        // block_len == 2 and the old trailing-optional probe computed
        // trailing == 0 and NEVER looked at the cert slot. eb_certificate came
        // back None for every era-8 block, certified or not -- which fed
        // `certified_eb` and the EB-application path in shared-consensus and
        // made body_cert_eb_slot telemetry unable to ever report a cert.
        for bb_len in [3u64, 4] {
            let cert = sample_leios_cert(4242);
            let raw = build_era8_block_body(bb_len, 2, Some(&cert));
            let info = BlockBody::opaque(raw).praos_inspect();
            assert_eq!(info.tx_count, 2, "bb_len {bb_len}: tx count");
            let got = info
                .eb_certificate
                .unwrap_or_else(|| panic!("bb_len {bb_len}: cert must be extracted"));
            assert_eq!(got.eb_slot, 4242, "bb_len {bb_len}: cert eb_slot");
            assert!(
                !info.eb_certificate_pending,
                "bb_len {bb_len}: a real cert is not 'pending'"
            );
        }
    }

    /// A REAL cert-bearing w36 block, lifted verbatim from a proto-devnet
    /// node's VolatileDB (blockNo 48, slot 1008) and re-wrapped in `#6.24` as
    /// BlockFetch delivers it.
    ///
    /// `block_body` is `[transactions, cert, nil]` (arity 3) and the cert is
    /// the w36 2-tuple `[signers_bitfield bytes[1], aggregated_signature
    /// bytes[48]]` -- NOT the `array(4)` `[slot, hash, signers, sig]` the
    /// decoder used to assume. There is no EB identity in it, so
    /// `eb_certificate_pending` is the correct report and the consumer
    /// resolves the EB from the parent RB's announcement.
    const REAL_W36_CERT_BLOCK_HEX: &[&str] = &[
        "d818590390820882828c18301903f05820e8dd4281b5a356784f966e9fe3d95187a954955f0d718f8cfbdd80ea999ca0",
        "3c582060d8abbb95bf48767dae3a6d3f8473206b72542f40e9d8945040000c3fa681c858207d58e5c6c021e33b057a6c",
        "6bb28b0b7a42444a76b67a770f53017ca2c68de8f58258404cc96f5bf599fb11c17491a7f115c8fe705412122ddfc98c",
        "ce4bfff2e23460c082603e39a7ce97214c0203d508d06f214a98bd5d63c98cc1fd77b8f85c3e7f655850e147b7e51f91",
        "e479bd72d679deb8179158a98f662eebaa5364af00079b25e62bff744cabd199f2aa1d8eaf72c8c36f23af7ddecc89a2",
        "2dadf14a82082f1db42b6f2ea94dede9fd502210da8288e1310b183858200d6df56fc0c49e6baade44d779510360e963",
        "e3168992aff9c448ad8b392675b7845820f1292ffddd67c4d99bf2a3d6590392b78fa8b52104fab81d94e7865a25f237",
        "72000058403fc90e7862c6d469b866d95db82baa85725b9f9de78365d68d0b19d78f291e3d163a3e5cb39c9ed858f308",
        "ac2bbd65cbb606593b0d2257405cc3dda7b3c4bd01820c00f5f65901c0283a68b42aafae1f8a790b4b2498a7d03492fa",
        "36cb166953343bbca74860b67584473c19ec66d0218c3510e61ef210745b60c49cd24127cccb5094493c7db40a487366",
        "0f9fc6b2ea810d9ceae28140ed2f6f3c697314f70b2479f04f6f9f76088dc7f2ee12a0edb7401e80d08e733dfab43be4",
        "1c8596e1bee1e9b26f11a7c9e6d3c96a82805e79c89c99702f20b733b7305e851eea4c31221bd10a17098178deb03dab",
        "10ccf6bf390a2d1aace82dbeca2152047d29e904f5b47a117a3b23e74eb3b04429129e7539939fddbf4543fec7126952",
        "b313b7520bf02a5dab7d0581e41594e926377f8f0a1fa7b7504354ef0d17cf243f2cec6dc533a180a334503e1725c803",
        "69fb8f0aa749b80821ec4628fa5dffadcd0d47285321eae4c252262b4dbae9ac959bc2c19492a77d05da7c804c18c0a3",
        "07f45d46e8a229390e3565b5849b3f96213f0c22fa061257599faa79051461e9bfa567d49b943afa7faadf2264d53b4e",
        "f155c714f1a637331add5c1a227124e69765f5c04f39dc78d1571fb39bc08d20030839289546235a8b2ab844f381088d",
        "488cfb8828ad62f5d422039f87a58d08f5bb8a9e021472bf8f32eaaff1dd128003cdfffab53655527e0c4a8eb8838082",
        "41e05830b5e115beaac498cd011531fa05e3615572143a557ab9118c99dce693d33bd6747de51ff7bb660f617b380a32",
        "c2edcc7df6",
    ];

    #[test]
    fn real_w36_cert_bearing_block_is_reported_as_certified() {
        let hex_str: String = REAL_W36_CERT_BLOCK_HEX.concat();
        let raw = hex::decode(hex_str).expect("valid hex");
        let info = BlockBody::opaque(raw).praos_inspect();

        // Before the era-8 fix this asserted nothing useful: the cert slot was
        // never probed, so a genuinely certified block reported no cert at all.
        assert!(
            info.eb_certificate_pending,
            "a real w36 cert-bearing block must be reported as carrying a cert"
        );
        assert!(
            info.eb_certificate.is_none(),
            "the w36 2-tuple carries no eb_slot/eb_hash, so none must be invented"
        );
        assert_eq!(info.field_count, 2, "era-8 block is [header, block_body]");
    }

    #[test]
    fn era8_praos_inspect_reports_no_cert_when_slot_is_nil() {
        for bb_len in [3u64, 4] {
            let raw = build_era8_block_body(bb_len, 1, None);
            let info = BlockBody::opaque(raw).praos_inspect();
            assert_eq!(info.tx_count, 1, "bb_len {bb_len}: tx count");
            assert!(
                info.eb_certificate.is_none() && !info.eb_certificate_pending,
                "bb_len {bb_len}: nil cert slot means absent, not pending"
            );
        }
    }

    #[test]
    fn leios_era8_counts_inner_tx_list_not_wrapper() {
        // Regression: era-8 block body field 1 is a 4-slot wrapper
        // `[null, tx_list, null, null]`. The count must be the tx_list length
        // (here 436, as seen on the dev testnet), NOT the wrapper's length (4).
        let raw = build_leios_block_with_tx_count(8, 436);
        let info = BlockBody::opaque(raw).praos_inspect();
        assert_eq!(
            info.tx_count, 436,
            "must count the inner tx_list, not the wrapper"
        );

        // A different count to prove it isn't hard-wired.
        let raw = build_leios_block_with_tx_count(8, 7);
        assert_eq!(BlockBody::opaque(raw).praos_inspect().tx_count, 7);

        // Empty Leios block.
        let raw = build_leios_block_with_tx_count(8, 0);
        assert_eq!(BlockBody::opaque(raw).praos_inspect().tx_count, 0);
    }

    #[test]
    fn pre_leios_still_counts_tx_bodies_directly() {
        // Era 7 (Dijkstra) keeps the flat layout: field 1 IS tx_bodies.
        let raw = build_test_block_with_tx_count(7, 5, None);
        assert_eq!(BlockBody::opaque(raw).praos_inspect().tx_count, 5);
    }

    #[test]
    fn block_body_round_trip() {
        // Simulate #6.24(bytes): CBOR tag 24 wrapping some bytes.
        let mut buf = Vec::new();
        let mut e = minicbor::Encoder::new(&mut buf);
        e.tag(minicbor::data::Tag::new(24)).unwrap();
        e.bytes(&[0x01, 0x02, 0x03]).unwrap();

        let body = BlockBody::opaque(buf.clone());
        let encoded = minicbor::to_vec(&body).unwrap();
        assert_eq!(encoded, buf);

        let decoded: BlockBody = minicbor::decode(&encoded).unwrap();
        assert_eq!(decoded.raw, buf);
    }

    #[test]
    fn parse_block_body_no_certificate() {
        let raw = build_test_block(7, None);
        assert!(LeiosBlockInfo::parse(&raw).is_none());
    }

    #[test]
    fn parse_block_body_with_certificate() {
        let cert_data = vec![0xCA, 0xFE, 0xBA, 0xBE];
        let raw = build_test_block(7, Some(&cert_data));
        let info = LeiosBlockInfo::parse(&raw).expect("should parse");
        let cert = info.eb_certificate.expect("should have certificate");
        // Certificate is stored as an opaque CBOR span (bytes item with header).
        // Verify the content is there by decoding the bstr.
        let mut d = Decoder::new(&cert);
        let decoded = d.bytes().unwrap();
        assert_eq!(decoded, &cert_data);
    }

    #[test]
    fn parse_block_body_byron_returns_none() {
        let raw = build_test_block(0, None);
        assert!(LeiosBlockInfo::parse(&raw).is_none());
    }

    #[test]
    fn parse_block_body_invalid_returns_none() {
        assert!(LeiosBlockInfo::parse(&[0xFF]).is_none());
        assert!(LeiosBlockInfo::parse(&[]).is_none());
    }

    #[test]
    fn block_body_new_parses_certificate() {
        let cert_data = vec![0x01, 0x02, 0x03];
        let raw = build_test_block(7, Some(&cert_data));
        let body = BlockBody::new(raw);
        assert!(body.leios.is_some());
        assert!(body.leios.unwrap().eb_certificate.is_some());
    }

    #[test]
    fn block_body_opaque_skips_parsing() {
        let raw = build_test_block(7, Some(&[0x01]));
        let body = BlockBody::opaque(raw);
        assert!(body.leios.is_none());
    }

    /// Build an era-8 Leios block `#6.24([8, [header, block_body]])` with a
    /// dummy (skippable) header. `cert` is the raw leios_certificate CBOR for
    /// `block_body[2]`, or None for a nil cert slot.
    fn build_era8_block(cert: Option<&[u8]>) -> Vec<u8> {
        let body = crate::encode_block_body(&[], cert);
        let dummy_header_inner = [0x80u8]; // empty array — parse skips field 0
        crate::wrap_block(&dummy_header_inner, &body, crate::DIJKSTRA_BLOCK_ERA)
    }

    #[test]
    fn parse_era8_block_extracts_cert_at_block_body_index_2() {
        // On-chain 2-tuple cert [signers, sig48].
        let mut cert = Vec::new();
        let _ = Encoder::new(&mut cert)
            .array(2)
            .and_then(|e| e.bytes(&[0x63, 0xff, 0xff]))
            .and_then(|e| e.bytes(&[0x11u8; 48]));
        let block = build_era8_block(Some(&cert));
        let info = LeiosBlockInfo::parse(&block).expect("era-8 block with cert parses");
        assert_eq!(
            info.eb_certificate.expect("cert present"),
            cert,
            "the leios_certificate at block_body[2] is extracted verbatim"
        );
    }

    #[test]
    fn parse_era8_block_nil_cert_returns_none() {
        let block = build_era8_block(None);
        assert!(
            LeiosBlockInfo::parse(&block).is_none(),
            "a nil block_body[2] means no certificate"
        );
    }

    /// Build a block with a real parseable Shelley+ header for point() testing.
    fn build_block_with_header(era: u8, slot: u64) -> Vec<u8> {
        use std::io::Write as _;

        // Build header_body: [block_number, slot, prev_hash, issuer_vkey,
        //   vrf_vkey, vrf_result, body_size, block_body_hash, op_cert, proto_ver]
        let mut hb_buf = Vec::new();
        let mut hb = Encoder::new(&mut hb_buf);
        hb.array(10).unwrap();
        hb.u64(42).unwrap(); // block_number
        hb.u64(slot).unwrap(); // slot
        hb.bytes(&[0xAA; 32]).unwrap(); // prev_hash
        hb.bytes(&[0xBB; 32]).unwrap(); // issuer_vkey
        hb.bytes(&[0u8; 32]).unwrap(); // vrf_vkey
        hb.array(2).unwrap(); // vrf_result
        hb.bytes(&[0u8; 32]).unwrap();
        hb.bytes(&[0u8; 32]).unwrap();
        hb.u32(1024).unwrap(); // body_size
        hb.bytes(&[0xCC; 32]).unwrap(); // block_body_hash
        hb.array(4).unwrap(); // op_cert
        hb.bytes(&[0u8; 32]).unwrap();
        hb.u64(0).unwrap();
        hb.u64(0).unwrap();
        hb.bytes(&[0u8; 64]).unwrap();
        hb.array(2).unwrap(); // proto_ver
        hb.u32(10).unwrap();
        hb.u32(0).unwrap();

        // Build header: [header_body, body_signature]
        let mut header_buf = Vec::new();
        let mut hi = Encoder::new(&mut header_buf);
        hi.array(2).unwrap();
        hi.writer_mut().write_all(&hb_buf).unwrap();
        hi.bytes(&[0u8; 64]).unwrap(); // dummy signature

        // Block array: [header, txs, witnesses, aux]
        // Note: real Cardano blocks store the header directly (no #6.24 wrapping).
        let mut block_buf = Vec::new();
        let mut be = Encoder::new(&mut block_buf);
        be.array(4).unwrap();
        be.writer_mut().write_all(&header_buf).unwrap();
        be.array(0).unwrap(); // txs
        be.array(0).unwrap(); // witnesses
        be.null().unwrap(); // aux

        // Outer: [era_tag, block_array]
        let mut inner_buf = Vec::new();
        let mut ie = Encoder::new(&mut inner_buf);
        ie.array(2).unwrap();
        ie.u32(era as u32).unwrap();
        ie.writer_mut().write_all(&block_buf).unwrap();

        // Wrap in #6.24
        let mut outer_buf = Vec::new();
        let mut oe = Encoder::new(&mut outer_buf);
        oe.tag(minicbor::data::Tag::new(24)).unwrap();
        oe.bytes(&inner_buf).unwrap();

        outer_buf
    }

    #[test]
    fn block_body_point_extracts_slot_and_hash() {
        let raw = build_block_with_header(7, 67890);
        let body = BlockBody::new(raw);
        let point = body
            .point()
            .expect("should derive point from Shelley+ block");
        match point {
            Point::Specific { slot, hash } => {
                assert_eq!(slot, 67890);
                // Hash should be Blake2b-256 of the reconstructed header CBOR.
                // Just verify it's nonzero (deterministic but hard to precompute).
                assert_ne!(hash, [0u8; 32]);
            }
            Point::Origin => panic!("expected Specific point"),
        }
    }

    #[test]
    fn block_body_header_extracts_matching_point() {
        let raw = build_block_with_header(7, 99999);
        let body = BlockBody::new(raw);
        let header = body.header().expect("should extract header");
        let body_point = body.point().expect("should derive point");
        let header_point = header.point().expect("header should have point");
        assert_eq!(body_point, header_point);
    }

    #[test]
    fn block_body_header_byron_returns_none() {
        let raw = build_test_block(0, None);
        let body = BlockBody::new(raw);
        assert!(body.header().is_none());
    }

    #[test]
    fn block_body_point_byron_returns_none() {
        let raw = build_test_block(0, None);
        let body = BlockBody::new(raw);
        assert!(body.point().is_none());
    }

    #[test]
    fn block_body_point_invalid_returns_none() {
        let body = BlockBody::opaque(vec![0xFF]);
        assert!(body.point().is_none());
    }

    #[test]
    fn praos_inspect_empty() {
        let raw = build_test_block_with_tx_count(7, 0, None);
        let body = BlockBody::new(raw);
        let info = body.praos_inspect();
        assert_eq!(info.tx_count, 0);
        // Conway base: [header, tx_bodies, tx_witnesses, aux, invalid_transactions].
        assert_eq!(info.field_count, 5);
        assert!(info.eb_certificate.is_none());
    }

    #[test]
    fn praos_inspect_several() {
        let raw = build_test_block_with_tx_count(7, 5, None);
        let body = BlockBody::new(raw);
        assert_eq!(body.praos_inspect().tx_count, 5);
    }

    #[test]
    fn praos_inspect_with_eb_certificate_blob() {
        // Conway base (5 fields) + a real CIP-0164 `leios_certificate`
        // array(4) in the trailing slot → 6 fields. `praos_inspect`
        // descends into the cert slot (unlike `LeiosBlockInfo::parse`,
        // which keeps it opaque), so the fixture must be a bare array
        // `[slot, eb_hash, signers, sig]`, matching the on-wire shape.
        let eb_slot = 4242u64;
        let eb_hash = [0x11u8; 32];
        let signers = [0x0Fu8; 2];
        let agg_sig = [0x22u8; 48];
        let mut cert = Vec::new();
        let mut ce = Encoder::new(&mut cert);
        ce.array(4).unwrap();
        ce.u64(eb_slot).unwrap();
        ce.bytes(&eb_hash).unwrap();
        ce.bytes(&signers).unwrap();
        ce.bytes(&agg_sig).unwrap();

        let raw = build_test_block_with_raw_trailing(7, 3, &cert);
        let body = BlockBody::new(raw);
        let info = body.praos_inspect();
        assert_eq!(info.tx_count, 3);
        assert_eq!(info.field_count, 6);
        let summary = info.eb_certificate.expect("cert decoded");
        assert_eq!(summary.eb_slot, eb_slot);
        assert_eq!(summary.eb_hash, eb_hash);
    }

    #[test]
    fn praos_inspect_handles_indefinite_tx_bodies() {
        // Dev-relay Leios-era blocks encode tx_bodies as an indefinite
        // CBOR array (`9f ... ff`).  The base parser must walk those
        // correctly rather than bailing out with `field_count=0`.
        use std::io::Write as _;
        let tx_count = 3u32;
        let mut block_buf = Vec::new();
        let mut be = Encoder::new(&mut block_buf);
        be.array(5).unwrap(); // Conway base, no Leios extensions
        be.bytes(&[0x80]).unwrap(); // 0 header
        be.begin_array().unwrap(); // 1 tx_bodies (indefinite)
        for _ in 0..tx_count {
            be.null().unwrap();
        }
        be.end().unwrap();
        be.array(0).unwrap(); // 2 tx_witness_sets
        be.null().unwrap(); // 3 auxiliary_data_set
        be.array(0).unwrap(); // 4 invalid_transactions

        let mut inner_buf = Vec::new();
        let mut ie = Encoder::new(&mut inner_buf);
        ie.array(2).unwrap();
        ie.u32(7).unwrap();
        ie.writer_mut().write_all(&block_buf).unwrap();

        let mut outer_buf = Vec::new();
        let mut oe = Encoder::new(&mut outer_buf);
        oe.tag(minicbor::data::Tag::new(24)).unwrap();
        oe.bytes(&inner_buf).unwrap();

        let body = BlockBody::new(outer_buf);
        let info = body.praos_inspect();
        assert_eq!(info.tx_count, tx_count);
        assert_eq!(info.field_count, 5);
        assert!(info.eb_certificate.is_none());
    }

    #[test]
    fn praos_inspect_with_real_leios_certificate_shape() {
        // Conway-era body layout: 5 base fields including
        // `invalid_transactions`, then a `leios_certificate` trailing
        // optional encoded inline as an `array(4)`.
        use std::io::Write as _;
        let tx_count = 2u64;
        let mut block_buf = Vec::new();
        let mut be = Encoder::new(&mut block_buf);
        be.array(6).unwrap(); // 5 Conway base + 1 cert
        be.bytes(&[0x80]).unwrap(); // 0 header
        be.array(tx_count).unwrap(); // 1 tx_bodies
        for _ in 0..tx_count {
            be.null().unwrap();
        }
        be.array(0).unwrap(); // 2 tx_witness_sets
        be.null().unwrap(); // 3 auxiliary_data_set
        be.array(0).unwrap(); // 4 invalid_transactions
                              // 5 leios_certificate = [slot_no, endorser_block_hash : hash32,
                              //                        signers : bytes, aggregated_signature : bytes .size 48]
        be.array(4).unwrap();
        be.u64(12345).unwrap();
        be.bytes(&[0xAB; 32]).unwrap();
        be.bytes(&[0xCC; 8]).unwrap();
        be.bytes(&[0xDD; 48]).unwrap();

        let mut inner_buf = Vec::new();
        let mut ie = Encoder::new(&mut inner_buf);
        ie.array(2).unwrap();
        ie.u32(7).unwrap();
        ie.writer_mut().write_all(&block_buf).unwrap();

        let mut outer_buf = Vec::new();
        let mut oe = Encoder::new(&mut outer_buf);
        oe.tag(minicbor::data::Tag::new(24)).unwrap();
        oe.bytes(&inner_buf).unwrap();

        let body = BlockBody::new(outer_buf);
        let info = body.praos_inspect();
        assert_eq!(info.tx_count, 2);
        assert_eq!(info.field_count, 6);
        let cert = info.eb_certificate.expect("cert shape should match");
        assert_eq!(cert.eb_slot, 12345);
        assert_eq!(cert.eb_hash, [0xAB; 32]);
    }

    #[test]
    fn praos_inspect_rejects_short_aggregated_signature() {
        // aggregated_signature must be exactly bytes .size 48 per
        // `leios_bls_signature`; anything else fails the body parse.
        use std::io::Write as _;
        let mut block_buf = Vec::new();
        let mut be = Encoder::new(&mut block_buf);
        be.array(6).unwrap();
        be.bytes(&[0x80]).unwrap();
        be.array(0).unwrap();
        be.array(0).unwrap();
        be.null().unwrap();
        be.array(0).unwrap();
        be.array(4).unwrap();
        be.u64(7).unwrap();
        be.bytes(&[0x11; 32]).unwrap();
        be.bytes(&[0x22; 4]).unwrap();
        be.bytes(&[0x33; 16]).unwrap(); // too short

        let mut inner_buf = Vec::new();
        let mut ie = Encoder::new(&mut inner_buf);
        ie.array(2).unwrap();
        ie.u32(7).unwrap();
        ie.writer_mut().write_all(&block_buf).unwrap();

        let mut outer_buf = Vec::new();
        let mut oe = Encoder::new(&mut outer_buf);
        oe.tag(minicbor::data::Tag::new(24)).unwrap();
        oe.bytes(&inner_buf).unwrap();

        // praos_inspect falls back to default on parse failure.
        let body = BlockBody::new(outer_buf);
        let info = body.praos_inspect();
        assert_eq!(info.field_count, 0);
        assert!(info.eb_certificate.is_none());
    }

    /// Build a Dijkstra-style fc=7 block where each trailing optional
    /// is encoded from raw CBOR bytes (so callers can splice null,
    /// `array(0)`, or arbitrary other shapes).
    fn fc7_block_with_raw_trailing(cert_bytes: &[u8], peras_bytes: &[u8]) -> BlockBody {
        use std::io::Write as _;
        let mut block_buf = Vec::new();
        let mut be = Encoder::new(&mut block_buf);
        be.array(7).unwrap();
        be.bytes(&[0x80]).unwrap();
        be.array(0).unwrap();
        be.array(0).unwrap();
        be.null().unwrap();
        be.array(0).unwrap();
        be.writer_mut().write_all(cert_bytes).unwrap();
        be.writer_mut().write_all(peras_bytes).unwrap();

        let mut inner_buf = Vec::new();
        let mut ie = Encoder::new(&mut inner_buf);
        ie.array(2).unwrap();
        ie.u32(7).unwrap();
        ie.writer_mut().write_all(&block_buf).unwrap();

        let mut outer_buf = Vec::new();
        let mut oe = Encoder::new(&mut outer_buf);
        oe.tag(minicbor::data::Tag::new(24)).unwrap();
        oe.bytes(&inner_buf).unwrap();

        BlockBody::new(outer_buf)
    }

    #[test]
    fn praos_inspect_accepts_null_trailing_optionals() {
        // fc=7 with both trailing optionals as CBOR null — the
        // `encodeNullStrictMaybe` shape from era-7 Dijkstra producers.
        // Counts as Absent (not Pending).
        let body = fc7_block_with_raw_trailing(&[0xf6], &[0xf6]);
        let info = body.praos_inspect();
        assert_eq!(info.tx_count, 0);
        assert_eq!(info.field_count, 7);
        assert!(info.eb_certificate.is_none());
        assert!(!info.eb_certificate_pending);
        assert!(!info.peras_cert_pending);
    }

    #[test]
    fn praos_inspect_flags_unit_array_trailing_optionals() {
        // fc=7 with both trailing optionals as `array(0)` — the
        // "unit" placeholder used by era-8 producers (Sebastian
        // 2026-06-15): "cert intent declared, encoding TBD". Should
        // count as Pending (separate from Absent).
        let body = fc7_block_with_raw_trailing(&[0x80], &[0x80]);
        let info = body.praos_inspect();
        assert_eq!(info.tx_count, 0);
        assert_eq!(info.field_count, 7);
        assert!(info.eb_certificate.is_none());
        assert!(info.eb_certificate_pending);
        assert!(info.peras_cert_pending);
    }

    #[test]
    fn praos_inspect_rejects_unknown_peras_slot() {
        // peras_cert layout is unknown — anything that isn't `null` or
        // `array(0)` should fail the body parse so an unknown shape
        // surfaces rather than getting silently misinterpreted.
        //
        // Splice in `array(1) [u32(42)]` for the peras slot:
        //   0x81 = array(1); 0x18 0x2a = u8(42).
        let body = fc7_block_with_raw_trailing(&[0xf6], &[0x81, 0x18, 0x2a]);
        let info = body.praos_inspect();
        // Parse failure ⇒ default (field_count=0).
        assert_eq!(info.field_count, 0);
    }

    #[test]
    fn praos_inspect_byron_returns_default() {
        let raw = build_test_block_with_tx_count(0, 0, None);
        let body = BlockBody::new(raw);
        let info = body.praos_inspect();
        // Byron path returns DecodeError → default
        assert_eq!(info.tx_count, 0);
        assert_eq!(info.field_count, 0);
    }

    #[test]
    fn praos_inspect_invalid_returns_default() {
        let body = BlockBody::opaque(vec![0xFF]);
        let info = body.praos_inspect();
        assert_eq!(info.tx_count, 0);
        assert_eq!(info.field_count, 0);
    }
}
