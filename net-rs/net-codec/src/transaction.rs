//! Transaction identity at the ranking-block / Leios boundary.
use minicbor::{data::Type, decode::Error, Decoder};
use std::borrow::Cow;

/// Convert an era-8 inline transaction into the Leios representation, preserving
/// the encoded body, witnesses and auxiliary data verbatim. Since w42 the Leios
/// representation includes the trailing validity flag. Return it separately too:
/// a failed script does not spend the ordinary inputs. Historical three-field
/// inline transactions are treated as valid when deriving their current identity.
pub fn inline_leios_transaction(era: u32, tx: &[u8]) -> Result<(Cow<'_, [u8]>, bool), Error> {
    if era != 8 {
        return Err(Error::message("unsupported inline transaction era"));
    }
    let mut d = Decoder::new(tx);
    let count = d.array()?;
    if !matches!(count, Some(3 | 4) | None) {
        return Err(Error::message("unsupported inline transaction shape"));
    }
    let start = d.position();
    for field in 0..3 {
        if field < 2 && !matches!(d.datatype()?, Type::Map | Type::MapIndef) {
            return Err(Error::message(
                "transaction body and witnesses must be maps",
            ));
        }
        if d.datatype()? == Type::Break {
            return Err(Error::message("truncated transaction"));
        }
        if field == 2
            && !matches!(
                d.datatype()?,
                Type::Map | Type::MapIndef | Type::Null | Type::Tag
            )
        {
            return Err(Error::message("unsupported auxiliary data"));
        }
        d.skip()?;
    }
    let end = d.position();
    let has_validity = count == Some(4) || (count.is_none() && d.datatype()? != Type::Break);
    let valid = if has_validity { d.bool()? } else { true };
    if count.is_none() {
        if d.datatype()? != Type::Break {
            return Err(Error::message("extra transaction fields"));
        }
        d.skip()?;
    }
    if d.position() != tx.len() {
        return Err(Error::message("trailing transaction data"));
    }
    if count == Some(4) && tx.first() == Some(&0x84) {
        return Ok((Cow::Borrowed(tx), valid));
    }
    let mut leios = Vec::with_capacity(end - start + 2);
    leios.push(0x84);
    leios.extend_from_slice(&tx[start..end]);
    leios.push(if valid { 0xf5 } else { 0xf4 });
    Ok((Cow::Owned(leios), valid))
}

/// Encode a validated mempool transaction for a Dijkstra ranking block.
/// Mempool form is [body, witnesses, auxiliary_data]; the legacy submission
/// form [body, witnesses, true, auxiliary_data] is also accepted. In-block
/// form is [body, witnesses, auxiliary_data, true]. Preserve signed fields.
/// This does not validate scripts: the caller must have accepted the transaction.
pub fn mempool_leios_transaction(tx: &[u8]) -> Result<Vec<u8>, Error> {
    let mut d = Decoder::new(tx);
    let count = d.array()?;
    if !matches!(count, Some(3 | 4) | None) {
        return Err(Error::message("unsupported mempool transaction shape"));
    }
    let start = d.position();
    for _ in 0..2 {
        if !matches!(d.datatype()?, Type::Map | Type::MapIndef) {
            return Err(Error::message(
                "transaction body and witnesses must be maps",
            ));
        }
        d.skip()?;
    }
    let witness_end = d.position();
    let legacy = count == Some(4) || (count.is_none() && d.datatype()? == Type::Bool);
    if legacy && !d.bool()? {
        return Err(Error::message("invalid script flag in mempool transaction"));
    }
    let aux_start = d.position();
    if !matches!(
        d.datatype()?,
        Type::Map | Type::MapIndef | Type::Null | Type::Tag
    ) {
        return Err(Error::message("unsupported auxiliary data"));
    }
    d.skip()?;
    let aux_end = d.position();
    if count.is_none() {
        if d.datatype()? != Type::Break {
            return Err(Error::message("extra mempool transaction fields"));
        }
        d.skip()?;
    }
    if d.position() != tx.len() {
        return Err(Error::message("trailing mempool transaction data"));
    }
    let mut inline = Vec::with_capacity(tx.len() + 1);
    inline.push(0x84);
    inline.extend_from_slice(&tx[start..witness_end]);
    inline.extend_from_slice(&tx[aux_start..aux_end]);
    inline.push(0xf5);
    Ok(inline)
}

/// Encode an accepted transaction for a ranking block. Accept submission form
/// and current Leios block form, retaining signed components and rejecting a
/// known phase-2 failure rather than silently replacing its validity flag.
pub fn leios_inline_transaction(tx: &[u8]) -> Result<Vec<u8>, Error> {
    if let Ok(inline) = mempool_leios_transaction(tx) {
        return Ok(inline);
    }
    let (inline, valid) = inline_leios_transaction(8, tx)?;
    if !valid {
        return Err(Error::message(
            "failed-script transaction cannot be selected as valid",
        ));
    }
    Ok(inline.into_owned())
}

/// Convert internal Leios bytes to Dijkstra TxSubmission form. The validity
/// flag belongs to the producer; failed-script transactions cannot be submitted.
/// Keep body, witness and auxiliary bytes unchanged.
pub fn leios_mempool_transaction(tx: &[u8]) -> Result<Vec<u8>, Error> {
    let (inline, valid) = inline_leios_transaction(8, tx)?;
    if !valid {
        return Err(Error::message(
            "failed-script transaction cannot be submitted",
        ));
    }
    let fields = inline
        .get(1..inline.len().saturating_sub(1))
        .ok_or_else(|| Error::message("truncated Leios transaction"))?;
    let mut mempool = Vec::with_capacity(fields.len() + 1);
    mempool.push(0x83);
    mempool.extend_from_slice(fields);
    Ok(mempool)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn outbound_matches_captured_haskell_inline() {
        let leios = include_bytes!("../test_data/haskell-leios-tx.cbor");
        let inline = include_bytes!("../test_data/haskell-inline-tx.cbor");
        assert_eq!(leios_inline_transaction(leios).unwrap(), inline);
        let parts = &leios[1..];
        let mut d = Decoder::new(parts);
        d.skip().unwrap();
        d.skip().unwrap();
        let split = d.position();
        let legacy = [
            vec![0x84],
            parts[..split].to_vec(),
            vec![0xf5],
            parts[split..].to_vec(),
        ]
        .concat();
        assert_eq!(leios_inline_transaction(&legacy).unwrap(), inline);
        assert_eq!(
            leios_inline_transaction(&[0x9f, 0xa0, 0xa0, 0xf6, 0xff]).unwrap(),
            [0x84, 0xa0, 0xa0, 0xf6, 0xf5]
        );
        for bad in [
            &[0x84, 0xa0, 0xa0, 0xf4, 0xf6][..],
            &[0x84, 0xa0, 0xa0, 0xf6, 0xf4],
            &[0x83, 0xa0, 0xa0],
            &[0x83, 0xa0, 0xa0, 0xf6, 0x00],
            &[0x9f, 0xa0, 0xa0, 0xf6],
            &[0x83, 0x01, 0xa0, 0xf6],
        ] {
            assert!(leios_inline_transaction(bad).is_err());
        }
    }
    #[test]
    fn captured_haskell_inline_identity() {
        let inline = include_bytes!("../test_data/haskell-inline-tx.cbor");
        let expected = include_bytes!("../test_data/haskell-leios-tx.cbor");
        let (leios, valid) = inline_leios_transaction(8, inline).unwrap();
        assert!(valid);
        assert_eq!(leios.as_ref(), inline);
        assert_ne!(crate::blake2b_256(expected), crate::blake2b_256(&leios));
        assert_eq!(crate::wire_tx_id(inline), crate::wire_tx_id(&leios));
        assert_eq!(
            crate::blake2b_256(&leios),
            [
                244, 96, 0, 115, 116, 185, 178, 85, 99, 168, 0, 84, 246, 227, 18, 152, 8, 88, 25,
                127, 197, 79, 205, 21, 159, 14, 34, 187, 111, 179, 234, 96
            ]
        );
        assert_eq!(
            crate::wire_tx_id(inline).unwrap(),
            [
                50, 161, 65, 226, 78, 248, 52, 181, 27, 31, 30, 92, 178, 55, 198, 16, 35, 21, 189,
                28, 195, 170, 32, 236, 207, 122, 152, 127, 246, 175, 122, 218
            ]
        );
    }
    #[test]
    fn layouts_and_invalid_scripts() {
        for bytes in [
            &[0x84, 0xa0, 0xa0, 0xf6, 0xf5][..],
            &[0x9f, 0xa0, 0xa0, 0xf6, 0xf5, 0xff],
        ] {
            assert_eq!(
                inline_leios_transaction(8, bytes).unwrap(),
                (Cow::Borrowed(&[0x84, 0xa0, 0xa0, 0xf6, 0xf5][..]), true)
            );
        }
        let old = [0x83, 0xa0, 0xa0, 0xf6];
        assert_eq!(
            inline_leios_transaction(8, &old).unwrap().0.as_ref(),
            [0x84, 0xa0, 0xa0, 0xf6, 0xf5]
        );
        assert!(
            !inline_leios_transaction(8, &[0x84, 0xa0, 0xa0, 0xf6, 0xf4])
                .unwrap()
                .1
        );
        for bytes in [
            &[][..],
            &[0x84, 0xa0, 0xa0, 0xf6],
            &[0x85, 0xa0, 0xa0, 0xf6, 0xf5, 0xf6],
            &[0x83, 0xa0, 0xa0, 0xf6, 0xf5],
            &[0x9f, 0xa0, 0xff],
            &[0x84, 0x01, 0x02, 0x03, 0xf5],
            &[0x84, 0xa0, 0xa0, 0xf5, 0xf6],
        ] {
            assert!(inline_leios_transaction(8, bytes).is_err());
        }
        assert!(inline_leios_transaction(7, &old).is_err());
        assert!(inline_leios_transaction(9, &old).is_err());
    }
}

#[cfg(test)]
mod w43_vectors {
    use super::*;

    #[test]
    fn captured_w43_leios_identity_includes_validity() {
        let raw = include_bytes!("../test_data/leios-transaction-w43.cbor");
        let (leios, valid) = inline_leios_transaction(8, raw).unwrap();
        assert!(valid);
        assert_eq!(leios.as_ref(), raw);
        let mempool = [vec![0x83], raw[1..raw.len() - 1].to_vec()].concat();
        assert_eq!(leios_inline_transaction(&mempool).unwrap(), raw);
        assert_eq!(leios_inline_transaction(raw).unwrap(), raw);
        assert_eq!(leios_mempool_transaction(raw).unwrap(), mempool);
        assert_eq!(
            crate::blake2b_256(raw),
            [
                66, 36, 179, 193, 192, 73, 206, 153, 187, 130, 103, 81, 21, 26, 71, 32, 24, 24,
                236, 25, 158, 224, 240, 183, 127, 84, 135, 153, 175, 235, 71, 98
            ]
        );
        let mut failed = raw.to_vec();
        *failed.last_mut().unwrap() = 0xf4;
        assert!(!inline_leios_transaction(8, &failed).unwrap().1);
        assert!(leios_mempool_transaction(&failed).is_err());
        assert!(leios_inline_transaction(&failed).is_err());
        assert_ne!(crate::blake2b_256(raw), crate::blake2b_256(&mempool));
        assert_eq!(crate::wire_tx_id(raw), crate::wire_tx_id(&mempool));
    }
}
