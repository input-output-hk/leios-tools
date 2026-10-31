//! Strict wire validation for Dijkstra headers received through LeiosNotify.
//!
//! Matches the week-41 Cardano.Protocol.Leios.BlockHeader decoder and Dijkstra
//! CDDL in cardano-ledger 505fa72260632fca6df33e18e18719730bf9a460. The general
//! HeaderInfo parser remains a best-effort inspector for other eras/layouts.

use minicbor::{data::Type, decode::Error, Decoder};

use crate::{HeaderInfo, DIJKSTRA_HEADER_ERA, MAX_HEADER_SIZE};

/// Validate the complete wire structure before relaying a peer announcement.
///
/// Checks the era, wrappers, record arities, field types, byte lengths and full
/// consumption of both CBOR layers. Returns metadata derived from these bytes,
/// never from a caller-supplied `WrappedHeader::parsed` value. This does not
/// validate signatures, leadership, chain membership or consensus rules.
pub fn validate_leios_header(raw: &[u8]) -> Result<HeaderInfo, Error> {
    if raw.len() > MAX_HEADER_SIZE {
        return Err(Error::message("Leios header exceeds size limit"));
    }
    let mut outer = Decoder::new(raw);
    // The hard-fork sum wrapper uses a definite pair and zero-based era index.
    if outer.array()? != Some(2) || outer.u32()? != DIJKSTRA_HEADER_ERA {
        return Err(Error::message("expected Dijkstra header wrapper"));
    }
    if outer.tag()?.as_u64() != 24 {
        return Err(Error::message("expected CBOR tag 24"));
    }
    let inner = outer.bytes()?;
    finish_bytes(&outer)?;

    let mut d = Decoder::new(inner);
    let header = record(&mut d, 2)?;
    let body = record(&mut d, 12)?;
    let block_number = d.u64()?;
    let slot = d.u64()?;
    let prev_hash = optional_hash(&mut d)?;
    let issuer_vkey = fixed_bytes::<32>(&mut d)?;
    fixed_bytes::<32>(&mut d)?; // VRF verification key
    let vrf = record(&mut d, 2)?;
    fixed_bytes::<64>(&mut d)?; // VRF output
    fixed_bytes::<80>(&mut d)?; // VRF proof
    finish_record(&mut d, vrf)?;
    let body_size = d.u32()?;
    let block_body_hash = fixed_bytes::<32>(&mut d)?;
    let cert = record(&mut d, 4)?;
    fixed_bytes::<32>(&mut d)?; // KES verification key
    d.u64()?; // operational certificate sequence number
    d.u64()?; // KES period
    fixed_bytes::<64>(&mut d)?; // operational certificate signature
    finish_record(&mut d, cert)?;
    let version = record(&mut d, 2)?;
    d.u32()?; // highest supported major version
    d.u32()?; // software tag
    finish_record(&mut d, version)?;
    let certified_eb = Some(d.bool()?);
    let announced_eb = if d.datatype()? == Type::Null {
        d.null()?;
        None
    } else {
        let announcement = record(&mut d, 2)?;
        let hash = fixed_bytes::<32>(&mut d)?;
        let size = d.u32()?;
        finish_record(&mut d, announcement)?;
        Some((hash, size))
    };
    finish_record(&mut d, body)?;
    fixed_bytes::<448>(&mut d)?; // Sum6KES header signature
    finish_record(&mut d, header)?;
    finish_bytes(&d)?;
    Ok(HeaderInfo {
        era: DIJKSTRA_HEADER_ERA as u8,
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

// Haskell's decodeRecordNamed accepts both definite and indefinite records.
// Indefinite records must end immediately after their exact field count.
fn record(d: &mut Decoder<'_>, count: u64) -> Result<Option<u64>, Error> {
    let len = d.array()?;
    if len.is_some_and(|len| len != count) {
        return Err(Error::message("incorrect Leios header record length"));
    }
    Ok(len)
}

fn finish_record(d: &mut Decoder<'_>, len: Option<u64>) -> Result<(), Error> {
    if len.is_none() {
        if d.datatype()? != Type::Break {
            return Err(Error::message("excess Leios header record fields"));
        }
        d.skip()?;
    }
    Ok(())
}

fn fixed_bytes<const N: usize>(d: &mut Decoder<'_>) -> Result<[u8; N], Error> {
    d.bytes()?
        .try_into()
        .map_err(|_| Error::message("incorrect Leios header byte length"))
}

fn optional_hash(d: &mut Decoder<'_>) -> Result<Option<[u8; 32]>, Error> {
    if d.datatype()? == Type::Null {
        d.null()?;
        Ok(None)
    } else {
        fixed_bytes(d).map(Some)
    }
}

fn finish_bytes(d: &Decoder<'_>) -> Result<(), Error> {
    if d.position() != d.input().len() {
        return Err(Error::message("trailing bytes in Leios header"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{wrap_header, WrappedHeader};

    fn captured() -> Vec<u8> {
        hex::decode(include_str!("../test_data/leios-header-w41.hex").trim()).unwrap()
    }

    fn inner(raw: &[u8]) -> Vec<u8> {
        let mut d = Decoder::new(raw);
        d.array().unwrap();
        d.u32().unwrap();
        d.tag().unwrap();
        d.bytes().unwrap().to_vec()
    }

    fn fields(raw: &[u8]) -> Vec<Vec<u8>> {
        let mut d = Decoder::new(raw);
        let n = d.array().unwrap().unwrap();
        (0..n)
            .map(|_| {
                let start = d.position();
                d.skip().unwrap();
                raw[start..d.position()].to_vec()
            })
            .collect()
    }

    fn array(fields: &[Vec<u8>]) -> Vec<u8> {
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(fields.len() as u64).unwrap();
        let mut raw = e.into_writer();
        for field in fields {
            raw.extend(field);
        }
        raw
    }

    fn bytes(n: usize) -> Vec<u8> {
        let mut e = minicbor::Encoder::new(Vec::new());
        e.bytes(&vec![0; n]).unwrap();
        e.into_writer()
    }

    fn change_body(field: usize, replacement: Vec<u8>) -> Vec<u8> {
        let mut header = fields(&inner(&captured()));
        let mut body = fields(&header[0]);
        body[field] = replacement;
        header[0] = array(&body);
        wrap_header(&array(&header), DIJKSTRA_HEADER_ERA)
    }

    #[test]
    fn captured_w41_header_validates_and_round_trips_without_changing_bytes() {
        let raw = captured();
        let info = validate_leios_header(&raw).unwrap();
        assert_eq!(info.block_number, 13);
        assert_eq!(info.slot, 270);
        assert!(info.announced_eb.is_some());
        let header: WrappedHeader = minicbor::decode(&raw).unwrap();
        assert_eq!(header.parsed, Some(info));
        assert_eq!(minicbor::to_vec(&header).unwrap(), raw);
    }

    #[test]
    fn rejects_missing_or_wrongly_typed_mandatory_fields() {
        for field in 0..12 {
            if field != 2 && field != 11 {
                assert!(
                    validate_leios_header(&change_body(field, vec![0xf6])).is_err(),
                    "field {field}"
                );
            }
        }
        // These two fields are nullable, but do not accept other CBOR types.
        for field in [2, 11] {
            assert!(validate_leios_header(&change_body(field, vec![0xf4])).is_err());
            assert!(validate_leios_header(&change_body(field, vec![0xf6])).is_ok());
        }
    }

    #[test]
    fn rejects_wrong_crypto_lengths_and_nested_record_shapes() {
        for field in [2, 3, 4, 7] {
            for len in [0, 31, 33] {
                assert!(validate_leios_header(&change_body(field, bytes(len))).is_err());
            }
        }
        let header = fields(&inner(&captured()));
        let body = fields(&header[0]);
        for (field, subfield, length) in
            [(5, 0, 64), (5, 1, 80), (8, 0, 32), (8, 3, 64), (11, 0, 32)]
        {
            for len in [0, length - 1, length + 1] {
                let mut record = fields(&body[field]);
                record[subfield] = bytes(len);
                assert!(
                    validate_leios_header(&change_body(field, array(&record))).is_err(),
                    "field {field}/{subfield}"
                );
            }
        }
        for field in [5, 8, 9, 11] {
            let mut record = fields(&body[field]);
            record.push(vec![0]);
            assert!(validate_leios_header(&change_body(field, array(&record))).is_err());
            record.pop();
            record.pop();
            assert!(validate_leios_header(&change_body(field, array(&record))).is_err());
        }
        for len in [0, 64, 447, 449] {
            let changed = array(&[header[0].clone(), bytes(len)]);
            assert!(validate_leios_header(&wrap_header(&changed, DIJKSTRA_HEADER_ERA)).is_err());
        }
    }

    #[test]
    fn rejects_invalid_integer_fields_and_legacy_extension_layouts() {
        let header = fields(&inner(&captured()));
        let body = fields(&header[0]);
        let mut e = minicbor::Encoder::new(Vec::new());
        e.u64(u64::from(u32::MAX) + 1).unwrap();
        let too_large = e.into_writer();
        assert!(validate_leios_header(&change_body(6, too_large.clone())).is_err());
        for (field, subfield) in [(9, 0), (9, 1), (11, 1)] {
            let mut record = fields(&body[field]);
            record[subfield] = too_large.clone();
            assert!(validate_leios_header(&change_body(field, array(&record))).is_err());
        }
        for field in [0, 1, 6] {
            assert!(validate_leios_header(&change_body(field, vec![0x20])).is_err());
        }
        for subfield in [1, 2] {
            let mut cert = fields(&body[8]);
            cert[subfield] = vec![0x20];
            assert!(validate_leios_header(&change_body(8, array(&cert))).is_err());
        }
        let mut swapped = body.clone();
        swapped.swap(10, 11);
        let raw = wrap_header(
            &array(&[array(&swapped), header[1].clone()]),
            DIJKSTRA_HEADER_ERA,
        );
        assert!(
            HeaderInfo::parse(&raw).is_some(),
            "legacy inspector tolerates swapped extensions"
        );
        assert!(validate_leios_header(&raw).is_err());
        let mut short = body.clone();
        short.pop();
        assert!(validate_leios_header(&wrap_header(
            &array(&[array(&short), header[1].clone()]),
            DIJKSTRA_HEADER_ERA
        ))
        .is_err());
    }

    #[test]
    fn rejects_wrong_wrappers_truncation_and_trailing_bytes() {
        let raw = captured();
        for end in 0..raw.len() {
            assert!(validate_leios_header(&raw[..end]).is_err(), "prefix {end}");
        }
        for era in [0, 6, 8, 263, u32::MAX] {
            assert!(validate_leios_header(&wrap_header(&inner(&raw), era)).is_err());
        }
        for (offset, replacement) in [(0, 0x81), (0, 0x83), (3, 23)] {
            let mut changed = raw.clone();
            changed[offset] = replacement;
            assert!(validate_leios_header(&changed).is_err());
        }
        let mut changed = raw.clone();
        changed.push(0);
        assert!(validate_leios_header(&changed).is_err());
        let mut changed = inner(&raw);
        changed.push(0);
        assert!(validate_leios_header(&wrap_header(&changed, DIJKSTRA_HEADER_ERA)).is_err());
        let mut header = fields(&inner(&raw));
        header.push(vec![0]);
        assert!(validate_leios_header(&wrap_header(&array(&header), DIJKSTRA_HEADER_ERA)).is_err());
        assert!(validate_leios_header(&vec![0; MAX_HEADER_SIZE + 1]).is_err());
    }

    #[test]
    fn accepts_indefinite_records_with_exact_field_counts() {
        fn indefinite(raw: &[u8]) -> Vec<u8> {
            if Decoder::new(raw).datatype().unwrap() != Type::Array {
                return raw.to_vec();
            }
            let mut out = vec![0x9f];
            for field in fields(raw) {
                out.extend(indefinite(&field));
            }
            out.push(0xff);
            out
        }
        let raw = captured();
        let changed = indefinite(&inner(&raw));
        assert_eq!(
            validate_leios_header(&wrap_header(&changed, DIJKSTRA_HEADER_ERA)).unwrap(),
            validate_leios_header(&raw).unwrap()
        );
        let mut excess = changed.clone();
        excess.insert(excess.len() - 1, 0);
        assert!(validate_leios_header(&wrap_header(&excess, DIJKSTRA_HEADER_ERA)).is_err());
        assert!(validate_leios_header(&wrap_header(
            &changed[..changed.len() - 1],
            DIJKSTRA_HEADER_ERA
        ))
        .is_err());
    }
}
