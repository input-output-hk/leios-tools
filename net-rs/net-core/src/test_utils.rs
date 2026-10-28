//! Parser-valid announcement fixtures; signatures are deliberately synthetic.
use crate::types::{Point, WrappedHeader};

pub(crate) fn leios_header(point: &Point, size: u32) -> WrappedHeader {
    let Point::Specific { slot, hash } = *point else {
        panic!("fixture requires a point")
    };
    let mut inner = minicbor::Encoder::new(Vec::new());
    inner.array(2).unwrap(); // [header_body, signature]
    inner.array(12).unwrap();
    inner.u64(1).unwrap(); // block number
    inner.u64(slot).unwrap();
    inner.null().unwrap(); // previous header hash
    inner.bytes(&[1; 32]).unwrap(); // issuer verification key
    inner.bytes(&[2; 32]).unwrap(); // VRF verification key
    inner.array(2).unwrap();
    inner.bytes(&[3; 64]).unwrap(); // VRF output
    inner.bytes(&[4; 80]).unwrap(); // VRF proof
    inner.u32(1).unwrap(); // ranking-block body size
    inner.bytes(&[5; 32]).unwrap(); // ranking-block body hash
    inner.array(4).unwrap(); // operational certificate
    inner.bytes(&[6; 32]).unwrap();
    inner.u64(0).unwrap();
    inner.u64(0).unwrap();
    inner.bytes(&[7; 64]).unwrap();
    inner.array(2).unwrap(); // protocol version
    inner.u32(11).unwrap();
    inner.u32(0).unwrap();
    inner.bool(false).unwrap(); // certified_eb
    inner.array(2).unwrap(); // announced_eb
    inner.bytes(&hash).unwrap();
    inner.u32(size).unwrap();
    inner.bytes(&[8; 448]).unwrap(); // structurally valid dummy KES signature
    let mut outer = minicbor::Encoder::new(Vec::new());
    outer.array(2).unwrap();
    outer.u32(net_codec::DIJKSTRA_HEADER_ERA).unwrap();
    outer.tag(minicbor::data::Tag::new(24)).unwrap();
    outer.bytes(&inner.into_writer()).unwrap();
    let header = WrappedHeader::new(outer.into_writer());
    let parsed = header.parsed.as_ref().expect("fixture header must parse");
    assert_eq!(parsed.slot, slot);
    assert_eq!(parsed.announced_eb, Some((hash, size)));
    assert_ne!(
        header.point(),
        Some(point.clone()),
        "RB and EB identities differ"
    );
    header
}
