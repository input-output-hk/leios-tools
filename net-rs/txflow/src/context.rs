//! Chain context: what materialise needs to know about the target network.
//!
//! The context is a YAML file. It names the network, gives the protocol
//! parameters (inline or from `cardano-cli query protocol-parameters` JSON),
//! and offers funding UTxOs with the key that spends them. Materialise uses
//! the offered UTxOs in order, taking only as many as the experiment needs.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{in_file, read_file, Error, Result};

/// The context file as written.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ContextFile {
    network: NetworkName,
    /// Network magic, recorded in the manifest (not used to build).
    magic: u32,
    /// Era of the target network, recorded in the manifest. Transactions are
    /// built in the Conway format whatever this says (see research D2).
    era: String,
    #[serde(default)]
    pparams: Option<ProtocolParams>,
    #[serde(default)]
    pparams_file: Option<PathBuf>,
    funding: FundingFile,
}

// No `Debug`: it holds the signing key.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FundingFile {
    #[serde(default)]
    signing_key_file: Option<PathBuf>,
    #[serde(default)]
    signing_key_hex: Option<String>,
    /// Where funding change goes. Default: the funding key's enterprise address.
    #[serde(default)]
    change_address: Option<String>,
    utxos: Vec<OfferedUtxoFile>,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
struct OfferedUtxoFile {
    /// `<tx id hex>#<index>`
    utxo: String,
    lovelace: u64,
}

/// A `cardano-cli` text envelope (`*.skey`).
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TextEnvelope {
    #[serde(rename = "type")]
    kind: String,
    cbor_hex: String,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NetworkName {
    Testnet,
    Mainnet,
}

/// The protocol parameters materialise uses. Field aliases accept the names
/// `cardano-cli query protocol-parameters` prints; other fields are ignored.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ProtocolParams {
    #[serde(alias = "txFeePerByte")]
    pub min_fee_a: u64,
    #[serde(alias = "txFeeFixed")]
    pub min_fee_b: u64,
    #[serde(alias = "maxTxSize")]
    pub max_tx_size: u64,
    #[serde(alias = "utxoCostPerByte")]
    pub coins_per_utxo_byte: u64,
}

/// A UTxO offered for funding the experiment.
#[derive(Clone, Debug, PartialEq)]
pub struct OfferedUtxo {
    pub tx_id: [u8; 32],
    pub index: u64,
    pub lovelace: u64,
}

/// A 32-byte Ed25519 secret key. `Debug` never prints it.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretKeyBytes(pub [u8; 32]);

impl std::fmt::Debug for SecretKeyBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretKeyBytes(<redacted>)")
    }
}

/// The loaded chain context.
#[derive(Clone, Debug)]
pub struct ChainContext {
    pub network: NetworkName,
    pub magic: u32,
    pub era: String,
    pub pparams: ProtocolParams,
    /// Key that spends the offered UTxOs.
    pub funding_key: SecretKeyBytes,
    /// Bech32 address for funding change, if not the funding key's enterprise address.
    pub change_address: Option<String>,
    pub offered: Vec<OfferedUtxo>,
}

impl ChainContext {
    /// Load a context file. Relative paths inside it resolve against its directory.
    pub fn load(path: &Path) -> Result<ChainContext> {
        let text = read_file(path)?;
        let base = path.parent().unwrap_or_else(|| Path::new("."));
        ChainContext::from_yaml(&text, base).map_err(|e| in_file(path, e))
    }

    /// Parse a context. Relative paths resolve against `base`.
    pub fn from_yaml(text: &str, base: &Path) -> Result<ChainContext> {
        let file: ContextFile = crate::spec::from_yaml_strict(text)?;
        // Mainnet is the only network with this magic; anything else is a testnet.
        const MAINNET_MAGIC: u32 = 764_824_073;
        if (file.network == NetworkName::Mainnet) != (file.magic == MAINNET_MAGIC) {
            return Err(Error::Context(format!(
                "network `{:?}` does not match magic {}",
                file.network, file.magic
            )));
        }
        let pparams = match (file.pparams, file.pparams_file) {
            (Some(inline), None) => inline,
            (None, Some(p)) => {
                let path = base.join(p);
                let json = read_file(&path)?;
                serde_json::from_str(&json).map_err(|e| in_file(&path, Error::Json(e)))?
            }
            _ => {
                return Err(Error::Context(
                    "give exactly one of `pparams` and `pparams_file`".to_string(),
                ))
            }
        };
        let key_hex = match (file.funding.signing_key_hex, file.funding.signing_key_file) {
            (Some(hex), None) => hex,
            (None, Some(p)) => {
                let path = base.join(p);
                let json = read_file(&path)?;
                let envelope: TextEnvelope =
                    serde_json::from_str(&json).map_err(|e| in_file(&path, Error::Json(e)))?;
                // A `.vkey` (and stake or pool keys) also start with `5820`;
                // only accept payment signing keys.
                match envelope.kind.as_str() {
                    "PaymentSigningKeyShelley_ed25519" | "GenesisUTxOSigningKey_ed25519" => {}
                    kind if kind.contains("Extended") => {
                        return Err(Error::Context(format!(
                            "signing key file has type `{kind}`: extended keys are not supported \
                             yet, use a `PaymentSigningKeyShelley_ed25519`"
                        )))
                    }
                    kind => {
                        return Err(Error::Context(format!(
                            "signing key file has type `{kind}`, expected \
                             `PaymentSigningKeyShelley_ed25519`"
                        )))
                    }
                }
                // A cardano-cli payment skey is CBOR bytes(32): `5820` then the key.
                envelope
                    .cbor_hex
                    .strip_prefix("5820")
                    .ok_or_else(|| {
                        Error::Context("signing key file is not a 32-byte Ed25519 key".to_string())
                    })?
                    .to_string()
            }
            _ => {
                return Err(Error::Context(
                    "give exactly one of `signing_key_file` and `signing_key_hex`".to_string(),
                ))
            }
        };
        let funding_key = SecretKeyBytes(decode_32(&key_hex, "signing key")?);
        let mut offered = Vec::new();
        let mut seen = BTreeSet::new();
        for utxo in &file.funding.utxos {
            let (id, index) = utxo.utxo.split_once('#').ok_or_else(|| {
                Error::Context(format!("`{}` is not `<tx id>#<index>`", utxo.utxo))
            })?;
            let index: u64 = index
                .parse()
                .map_err(|_| Error::Context(format!("bad index in `{}`", utxo.utxo)))?;
            let entry = OfferedUtxo {
                tx_id: decode_32(id, "tx id")?,
                index,
                lovelace: utxo.lovelace,
            };
            if !seen.insert((entry.tx_id, entry.index)) {
                return Err(Error::Context(format!("`{}` is offered twice", utxo.utxo)));
            }
            offered.push(entry);
        }
        Ok(ChainContext {
            network: file.network,
            magic: file.magic,
            era: file.era,
            pparams,
            funding_key,
            change_address: file.funding.change_address,
            offered,
        })
    }
}

fn decode_32(text: &str, what: &str) -> Result<[u8; 32]> {
    let bytes = hex::decode(text).map_err(|e| Error::Context(format!("{what}: {e}")))?;
    bytes
        .try_into()
        .map_err(|_| Error::Context(format!("{what} must be 32 bytes")))
}
