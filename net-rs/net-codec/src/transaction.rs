//! Transaction identity at the ranking-block / Leios boundary.
use minicbor::{data::Type, decode::Error, Decoder};
use std::borrow::Cow;

/// Convert an era-8 inline transaction into the Leios representation, preserving
/// the encoded body, witnesses and auxiliary data verbatim. The validity flag is
/// returned separately: a failed script does not spend the ordinary inputs.
/// Older three-field era-8 transactions are already in Leios form.
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
    if !has_validity {
        return Ok((Cow::Borrowed(tx), valid));
    }
    let mut leios = Vec::with_capacity(end - start + 1);
    leios.push(0x83);
    leios.extend_from_slice(&tx[start..end]);
    Ok((Cow::Owned(leios), valid))
}

/// Encode a validated mempool transaction for a Dijkstra ranking block.
/// Mempool form is [body, witnesses, auxiliary_data]; the legacy submission
/// form [body, witnesses, true, auxiliary_data] is also accepted. In-block
/// form is [body, witnesses, auxiliary_data, true]. Preserve signed fields.
/// This does not validate scripts: the caller must have accepted the transaction.
pub fn leios_inline_transaction(tx: &[u8]) -> Result<Vec<u8>, Error> {
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
            &[0x84, 0xa0, 0xa0, 0xf6, 0xf5],
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
        assert_eq!(leios.as_ref(), expected);
        assert_ne!(crate::blake2b_256(inline), crate::blake2b_256(&leios));
        assert_eq!(crate::wire_tx_id(inline), crate::wire_tx_id(&leios));
        assert_eq!(
            crate::blake2b_256(&leios),
            [
                64, 162, 78, 135, 210, 249, 194, 67, 194, 130, 151, 174, 5, 34, 98, 138, 40, 95,
                176, 142, 4, 99, 60, 75, 90, 108, 96, 31, 179, 56, 253, 40
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
                (Cow::Borrowed(&[0x83, 0xa0, 0xa0, 0xf6][..]), true)
            );
        }
        let old = [0x83, 0xa0, 0xa0, 0xf6];
        assert_eq!(inline_leios_transaction(8, &old).unwrap().0.as_ref(), old);
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
