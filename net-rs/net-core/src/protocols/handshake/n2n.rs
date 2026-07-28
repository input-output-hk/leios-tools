//! Node-to-Node handshake version data.
//!
//! The N2N version data is a CBOR array of 4 elements (for v11+):
//!   [networkMagic, diffusionMode, peerSharing, query]

use std::collections::BTreeMap;

use super::VersionTable;

/// N2N protocol version numbers currently supported.
pub const VERSION_V14: u64 = 14;
pub const VERSION_V15: u64 = 15;
pub const VERSION_V16: u64 = 16;

/// Well-known network magic values.
pub const MAINNET_MAGIC: u64 = 764824073;
pub const TESTNET_MAGIC: u64 = 1097911063;
pub const PREVIEW_MAGIC: u64 = 2;
pub const PREPROD_MAGIC: u64 = 1;

/// Parsed N2N version data.
///
/// The wire form is a CBOR array. Versions 11–15 use a 4-element array
/// `[networkMagic, diffusionMode, peerSharing, query]`. Version 16 uses a
/// 5-element array that appends one trailing boolean (`v16_flag`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionData {
    pub network_magic: u64,
    /// True = initiator-only mode, False = initiator-and-responder.
    pub initiator_only_diffusion_mode: bool,
    /// 0 or 1: whether this node will run the PeerSharing protocol.
    pub peer_sharing: u8,
    /// True = query mode (send all supported versions and terminate).
    pub query: bool,
    /// v16-only trailing boolean. `Some(_)` selects the 5-element v16 encoding
    /// (`v16.nodeToNodeVersionData`); `None` selects the 4-element v14/v15
    /// encoding, so v14/v15 params round-trip byte-for-byte.
    ///
    /// Its semantics are not yet pinned down in the public network spec
    /// (`docs/praos-network.md` leaves `v16.nodeToNodeVersionData` as a forward
    /// reference); it is observed as `false` on the Leios dev testnet. Modelled
    /// as a raw boolean so the codec round-trips today's wire bytes without
    /// asserting a meaning it can't yet justify.
    pub v16_flag: Option<bool>,
}

impl VersionData {
    /// Encode to CBOR bytes (for inclusion in the version table).
    pub fn encode(&self) -> Vec<u8> {
        minicbor::to_vec(self).expect("VersionData encoding cannot fail")
    }

    /// Decode from CBOR bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        minicbor::decode(bytes).map_err(|e| format!("failed to decode N2N version data: {e}"))
    }
}

impl minicbor::Encode<()> for VersionData {
    fn encode<W: minicbor::encode::Write>(
        &self,
        e: &mut minicbor::Encoder<W>,
        _ctx: &mut (),
    ) -> Result<(), minicbor::encode::Error<W::Error>> {
        // v16 appends one trailing boolean; v14/v15 stop at 4 elements.
        match self.v16_flag {
            Some(flag) => {
                e.array(5)?;
                e.u64(self.network_magic)?;
                e.bool(self.initiator_only_diffusion_mode)?;
                e.u8(self.peer_sharing)?;
                e.bool(self.query)?;
                e.bool(flag)?;
            }
            None => {
                e.array(4)?;
                e.u64(self.network_magic)?;
                e.bool(self.initiator_only_diffusion_mode)?;
                e.u8(self.peer_sharing)?;
                e.bool(self.query)?;
            }
        }
        Ok(())
    }
}

impl<'a> minicbor::Decode<'a, ()> for VersionData {
    fn decode(
        d: &mut minicbor::Decoder<'a>,
        _ctx: &mut (),
    ) -> Result<Self, minicbor::decode::Error> {
        let len = d.array()?.ok_or_else(|| {
            minicbor::decode::Error::message("expected definite-length array for version data")
        })?;

        let network_magic = d.u64()?;
        let initiator_only_diffusion_mode = d.bool()?;

        // v7-v10 only have 2 fields; v11-v15 have 4; v16 appends a 5th boolean.
        let (peer_sharing, query) = if len >= 4 {
            (d.u8()?, d.bool()?)
        } else {
            (0, false)
        };
        let v16_flag = if len >= 5 { Some(d.bool()?) } else { None };

        Ok(Self {
            network_magic,
            initiator_only_diffusion_mode,
            peer_sharing,
            query,
            v16_flag,
        })
    }
}

/// The versions this node supports, ascending.
pub const SUPPORTED_VERSIONS: &[u64] = &[VERSION_V14, VERSION_V15, VERSION_V16];

/// Build a version table proposing all supported versions with the given
/// parameters. v14/v15 use the 4-element encoding; v16 uses the 5-element one.
pub fn version_table(data: &VersionData) -> VersionTable {
    version_table_for(data, SUPPORTED_VERSIONS)
}

/// Build a version table proposing exactly `versions` with the given
/// parameters. Each entry is encoded in the form required by its version
/// (v16 carries the trailing `v16_flag`, defaulting to `false` when the
/// caller left it unset; v14/v15 use the 4-element form). Any other version
/// number (including unknown future versions) is encoded in the 4-element
/// form, since only v16's 5-element shape is currently defined.
pub fn version_table_for(data: &VersionData, versions: &[u64]) -> VersionTable {
    let mut table = BTreeMap::new();
    for &v in versions {
        let entry = VersionData {
            v16_flag: if v == VERSION_V16 {
                Some(data.v16_flag.unwrap_or(false))
            } else {
                None
            },
            ..data.clone()
        };
        table.insert(v, entry.encode());
    }
    table
}

/// Standard N2N negotiation: find the highest common version, decode params,
/// check network magic matches. Returns the accepted version and negotiated
/// params, or a refuse reason.
///
/// `server_data` provides the server's own capabilities. Per the spec:
/// - diffusion mode = initiator-only if EITHER side proposes it (logical OR)
/// - peer sharing = inherited from remote (client)
/// - query = inherited from client
pub fn negotiate(
    client_versions: &VersionTable,
    server_data: &VersionData,
) -> Result<(u64, Vec<u8>), super::RefuseReason> {
    // Our supported versions.
    let our_versions: Vec<u64> = SUPPORTED_VERSIONS.to_vec();

    // Find highest common version.
    let common: Vec<u64> = our_versions
        .iter()
        .copied()
        .filter(|v| client_versions.contains_key(v))
        .collect();

    if common.is_empty() {
        return Err(super::RefuseReason::VersionMismatch(our_versions));
    }

    let best_version = *common.last().unwrap(); // safe: common is non-empty

    // Decode the client's params for the selected version.
    let client_params_bytes = &client_versions[&best_version];
    let client_data = VersionData::decode(client_params_bytes)
        .map_err(|e| super::RefuseReason::HandshakeDecodeError(best_version, e))?;

    // Check network magic.
    if client_data.network_magic != server_data.network_magic {
        return Err(super::RefuseReason::Refused(
            best_version,
            format!(
                "network magic mismatch: client={}, server={}",
                client_data.network_magic, server_data.network_magic
            ),
        ));
    }

    // Build negotiated version data per spec:
    // - diffusion mode = initiator-only if EITHER side proposes it (OR)
    // - peer sharing = inherited from remote (client)
    // - query = inherited from client
    // - v16_flag: only present when v16 is the negotiated version; mirror the
    //   client's value (defaulting to false) so the response uses the correct
    //   5-element v16 encoding.
    let negotiated = VersionData {
        network_magic: server_data.network_magic,
        initiator_only_diffusion_mode: client_data.initiator_only_diffusion_mode
            || server_data.initiator_only_diffusion_mode,
        peer_sharing: client_data.peer_sharing,
        query: client_data.query,
        v16_flag: if best_version == VERSION_V16 {
            Some(client_data.v16_flag.unwrap_or(false))
        } else {
            None
        },
    };

    Ok((best_version, negotiated.encode()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_data_round_trip() {
        let data = VersionData {
            network_magic: MAINNET_MAGIC,
            initiator_only_diffusion_mode: false,
            peer_sharing: 1,
            query: false,
            v16_flag: None,
        };
        let encoded = data.encode();
        let decoded = VersionData::decode(&encoded).unwrap();
        assert_eq!(data, decoded);
    }

    fn server_data(magic: u64) -> VersionData {
        VersionData {
            network_magic: magic,
            initiator_only_diffusion_mode: false,
            peer_sharing: 1,
            query: false,
            v16_flag: None,
        }
    }

    #[test]
    fn negotiate_success() {
        let client_data = VersionData {
            network_magic: MAINNET_MAGIC,
            initiator_only_diffusion_mode: false,
            peer_sharing: 1,
            query: false,
            v16_flag: None,
        };
        let client_table = version_table(&client_data);

        let (version, params) = negotiate(&client_table, &server_data(MAINNET_MAGIC)).unwrap();
        assert_eq!(version, VERSION_V16); // highest common
        let negotiated = VersionData::decode(&params).unwrap();
        assert_eq!(negotiated.network_magic, MAINNET_MAGIC);
        // v16 negotiation must round-trip through the 5-element encoding.
        assert_eq!(negotiated.v16_flag, Some(false));
    }

    #[test]
    fn negotiate_downgrades_when_client_lacks_v16() {
        // A client that only offers v14/v15 must negotiate v15, and the
        // response must use the 4-element (v16_flag = None) encoding.
        let client_data = server_data(MAINNET_MAGIC);
        let client_table = version_table_for(&client_data, &[VERSION_V14, VERSION_V15]);

        let (version, params) = negotiate(&client_table, &server_data(MAINNET_MAGIC)).unwrap();
        assert_eq!(version, VERSION_V15);
        let negotiated = VersionData::decode(&params).unwrap();
        assert_eq!(negotiated.v16_flag, None);
    }

    #[test]
    fn negotiate_diffusion_mode_or() {
        // If client proposes initiator-only, negotiated should be true
        // even if server says false (logical OR per spec).
        let client_data = VersionData {
            network_magic: MAINNET_MAGIC,
            initiator_only_diffusion_mode: true,
            peer_sharing: 1,
            query: false,
            v16_flag: None,
        };
        let client_table = version_table(&client_data);

        let (_, params) = negotiate(&client_table, &server_data(MAINNET_MAGIC)).unwrap();
        let negotiated = VersionData::decode(&params).unwrap();
        assert!(negotiated.initiator_only_diffusion_mode);

        // If server proposes initiator-only but client doesn't, still true.
        let client_data2 = VersionData {
            network_magic: MAINNET_MAGIC,
            initiator_only_diffusion_mode: false,
            peer_sharing: 1,
            query: false,
            v16_flag: None,
        };
        let client_table2 = version_table(&client_data2);
        let server_initiator_only = VersionData {
            network_magic: MAINNET_MAGIC,
            initiator_only_diffusion_mode: true,
            peer_sharing: 1,
            query: false,
            v16_flag: None,
        };
        let (_, params2) = negotiate(&client_table2, &server_initiator_only).unwrap();
        let negotiated2 = VersionData::decode(&params2).unwrap();
        assert!(negotiated2.initiator_only_diffusion_mode);
    }

    #[test]
    fn negotiate_magic_mismatch() {
        let client_data = VersionData {
            network_magic: MAINNET_MAGIC,
            initiator_only_diffusion_mode: false,
            peer_sharing: 1,
            query: false,
            v16_flag: None,
        };
        let client_table = version_table(&client_data);

        let result = negotiate(&client_table, &server_data(TESTNET_MAGIC));
        assert!(matches!(
            result,
            Err(super::super::RefuseReason::Refused(_, _))
        ));
    }

    #[test]
    fn negotiate_no_common_version() {
        let mut client_table = BTreeMap::new();
        let data = VersionData {
            network_magic: MAINNET_MAGIC,
            initiator_only_diffusion_mode: false,
            peer_sharing: 1,
            query: false,
            v16_flag: None,
        };
        client_table.insert(99, data.encode()); // unsupported version

        let result = negotiate(&client_table, &server_data(MAINNET_MAGIC));
        assert!(matches!(
            result,
            Err(super::super::RefuseReason::VersionMismatch(_))
        ));
    }

    #[test]
    fn version_data_v7_v10_format_decode() {
        // v7-v10 used only 2 fields: [magic, diffusionMode].
        // Our decoder should handle this gracefully.
        let v10_cbor = minicbor::to_vec((MAINNET_MAGIC, false)).unwrap();
        let decoded = VersionData::decode(&v10_cbor).unwrap();
        assert_eq!(decoded.network_magic, MAINNET_MAGIC);
        assert!(!decoded.initiator_only_diffusion_mode);
        assert_eq!(decoded.peer_sharing, 0); // default
        assert!(!decoded.query); // default
        assert_eq!(decoded.v16_flag, None);
    }

    #[test]
    fn version_data_all_fields_set() {
        let data = VersionData {
            network_magic: PREPROD_MAGIC,
            initiator_only_diffusion_mode: true,
            peer_sharing: 1,
            query: true,
            v16_flag: None,
        };
        let encoded = data.encode();
        let decoded = VersionData::decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn version_data_decode_from_live_bytes() {
        // The exact bytes the server returned for V15 params:
        // [764824073, false, 0, false]
        let live_bytes: &[u8] = &[0x84, 0x1a, 0x2d, 0x96, 0x4a, 0x09, 0xf4, 0x00, 0xf4];
        let decoded = VersionData::decode(live_bytes).unwrap();
        assert_eq!(decoded.network_magic, MAINNET_MAGIC);
        assert!(!decoded.initiator_only_diffusion_mode);
        assert_eq!(decoded.peer_sharing, 0);
        assert!(!decoded.query);
        assert_eq!(decoded.v16_flag, None);
    }

    #[test]
    fn version_data_v16_exact_wire_bytes() {
        // Byte-for-byte match against params captured live from a v16-capable
        // node on the Leios dev testnet (magic=164, initOnly=false,
        // peerSharing=1, query=false, v16_flag=false).
        let v4 = VersionData {
            network_magic: 164,
            initiator_only_diffusion_mode: false,
            peer_sharing: 1,
            query: false,
            v16_flag: None,
        };
        let v16 = VersionData {
            v16_flag: Some(false),
            ..v4.clone()
        };
        // 4-element form for v14/v15, 5-element form for v16.
        assert_eq!(v4.encode(), vec![0x84, 0x18, 0xa4, 0xf4, 0x01, 0xf4]);
        assert_eq!(v16.encode(), vec![0x85, 0x18, 0xa4, 0xf4, 0x01, 0xf4, 0xf4]);
    }

    #[test]
    fn version_data_v16_round_trip() {
        let data = VersionData {
            network_magic: MAINNET_MAGIC,
            initiator_only_diffusion_mode: true,
            peer_sharing: 1,
            query: false,
            v16_flag: Some(true),
        };
        let decoded = VersionData::decode(&data.encode()).unwrap();
        assert_eq!(decoded, data);
        assert_eq!(decoded.v16_flag, Some(true));
    }

    #[test]
    fn version_data_invalid_cbor() {
        let bad = &[0xFF, 0xFF];
        assert!(VersionData::decode(bad).is_err());
    }

    #[test]
    fn version_table_has_ascending_keys() {
        let data = VersionData {
            network_magic: MAINNET_MAGIC,
            initiator_only_diffusion_mode: false,
            peer_sharing: 0,
            query: false,
            v16_flag: None,
        };
        let table = version_table(&data);
        let keys: Vec<u64> = table.keys().copied().collect();
        assert_eq!(keys, vec![14, 15, 16]); // ascending order

        // v14/v15 use the 4-element form; v16 uses the 5-element form.
        assert_eq!(table[&VERSION_V14].len(), table[&VERSION_V15].len());
        assert_eq!(table[&VERSION_V16].len(), table[&VERSION_V14].len() + 1);
    }

    #[test]
    fn version_table_for_subset() {
        let data = server_data(MAINNET_MAGIC);
        let table = version_table_for(&data, &[VERSION_V15, VERSION_V16]);
        let keys: Vec<u64> = table.keys().copied().collect();
        assert_eq!(keys, vec![15, 16]);
        // v15 decodes with no v16_flag; v16 decodes with one.
        assert_eq!(
            VersionData::decode(&table[&VERSION_V15]).unwrap().v16_flag,
            None
        );
        assert_eq!(
            VersionData::decode(&table[&VERSION_V16]).unwrap().v16_flag,
            Some(false)
        );
    }
}
