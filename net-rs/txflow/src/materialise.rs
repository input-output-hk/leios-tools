//! Bind an IR to a chain context and build every transaction offline.
//!
//! Wallets all bind to one run key, derived from the funding key, the IR
//! hash and the seed, so a rerun of the same experiment and context yields
//! byte-identical transactions. Funding transactions spend the offered UTxOs
//! and create one output per IR source at the run address; experiment
//! transactions follow in topological order. Nothing is submitted.

use std::collections::BTreeMap;

use pallas_addresses::{
    Address, Network, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart,
};
use pallas_crypto::hash::{Hash, Hasher};
use pallas_crypto::key::ed25519::SecretKey;
use pallas_txbuilder::{BuildConway, Input, Output, StagingTransaction};
use serde::Serialize;

use crate::context::{ChainContext, NetworkName, OfferedUtxo, ProtocolParams, SecretKeyBytes};
use crate::derive;
use crate::error::{Error, Result};
use crate::ir::{Amount, AmountKeyword, Ir};

/// CIP-20 transaction message label, used to attribute txs on chain.
pub const METADATA_LABEL: u64 = 674;
/// At most this many IR sources are created by one funding transaction.
const SOURCES_PER_FUNDING_TX: usize = 200;
/// Upper bound on one input's serialised size: `[bytes(32), uint]`.
const INPUT_SIZE_BOUND: u64 = 44;
/// Upper bound on a change output, which may go to a base address.
const CHANGE_SIZE_BOUND: u64 = 100;
/// Upper bound on everything in a funding tx besides inputs and outputs:
/// headers, fee, metadata hash and metadata, one vkey witness.
const TX_OVERHEAD_BOUND: u64 = 512;
/// Upper bound on the serialised size of an ADA-only output to an enterprise
/// address (map header, address bytes, coin), used for the min-UTxO rule.
const OUTPUT_SIZE_BOUND: u64 = 65;
/// Constant overhead the ledger adds to an output's size for min-UTxO.
const OUTPUT_OVERHEAD: u64 = 160;
/// Fee iterations before giving up. Two or three are enough in practice.
const FEE_ROUNDS: usize = 8;

/// The materialised experiment: every transaction, signed, in release order.
#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct Manifest {
    pub version: u32,
    pub experiment: String,
    pub ir_hash: String,
    pub run_id: String,
    pub seed: u64,
    /// Era of the target network, from the chain context.
    pub era: String,
    /// Era format the transactions were built in (Pallas builds Conway only).
    pub tx_format: String,
    pub network: NetworkName,
    pub network_magic: u32,
    /// The protocol parameters the fees and min-UTxO were computed with.
    pub pparams: ProtocolParams,
    /// `blake2b256:<hex>` over `pparams` (FR-014), to compare runs at a glance.
    pub pparams_hash: String,
    /// Enterprise address of the funding key: where the offered UTxOs are spent from.
    pub funding_address: String,
    /// Where the last funding tx returns change, if not `funding_address`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub change_address: Option<String>,
    pub run_address: String,
    /// Offered UTxOs the funding transactions spend, as `<tx id>#<index>`.
    pub funding_utxos: Vec<String>,
    pub txs: Vec<ManifestTx>,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct ManifestTx {
    /// Position in `txs`; also written into the tx's metadata.
    pub index: usize,
    /// IR name, or `fund/<k>` for funding transactions.
    pub name: String,
    pub kind: TxKind,
    pub tx_id: String,
    pub fee: u64,
    pub size: usize,
    /// Inputs spent: IR reference and the UTxO it resolved to.
    pub inputs: Vec<ResolvedInput>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub reference_inputs: Vec<ResolvedInput>,
    /// Lovelace of each output, in output order.
    pub outputs: Vec<u64>,
    /// Funding txs: the IR source each output holds, by output index (the
    /// last output, change, is not a source).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<String>,
    /// Conflict sets (contested outputs) this tx spends from.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub conflict_sets: Vec<String>,
    /// Conflict sets among its ancestors: it is invalid if an ancestor loses.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub orphan_on: Vec<String>,
    pub cbor_hex: String,
}

/// An input as named in the IR and as spent on chain.
#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct ResolvedInput {
    /// IR reference; for funding txs, the offered UTxO or `fund/<k>.<ix>`.
    #[serde(rename = "ref")]
    pub reference: String,
    /// `<tx id>#<index>`.
    pub utxo: String,
}

#[derive(Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TxKind {
    Funding,
    Experiment,
}

/// One output of a transaction being built.
struct PlannedOutput {
    address: Address,
    /// `None` for the change output.
    lovelace: Option<u64>,
}

/// What [`build_signed`] returns.
struct Built {
    tx_id: [u8; 32],
    bytes: Vec<u8>,
    fee: u64,
    outputs: Vec<u64>,
}

/// Build and sign every transaction of `ir` against `context`.
pub fn materialise(
    experiment: &str,
    ir: &Ir,
    seed: u64,
    context: &ChainContext,
) -> Result<Manifest> {
    let derived = derive::derive(ir)?;
    let pp = &context.pparams;
    let network = match context.network {
        NetworkName::Testnet => Network::Testnet,
        NetworkName::Mainnet => Network::Mainnet,
    };

    let funding_key = SecretKey::from(context.funding_key.0);
    let funding_address = key_address(&funding_key, network);
    // Intermediate funding txs are spent by the next one, which only the
    // funding key signs, so only the last funding tx pays `change_address`.
    let final_change = match &context.change_address {
        Some(text) => change_address(text, network)?,
        None => funding_address.clone(),
    };
    let run_key = SecretKey::from(run_key(&context.funding_key, &derived.hash, seed).0);
    let run_address = key_address(&run_key, network);
    let ir_prefix = derived
        .hash
        .strip_prefix("blake2b256:")
        .and_then(|h| h.get(..16))
        .ok_or_else(|| Error::Materialise("unexpected IR hash format".to_string()))?
        .to_string();
    let min_lovelace = min_for(&run_address, pp)?;

    let mut txs: Vec<ManifestTx> = Vec::new();
    // Reference text (`source` or `tx.N`) → spendable input and its value.
    let mut utxos: BTreeMap<String, (Input, u64)> = BTreeMap::new();

    // --- Funding ----------------------------------------------------------
    let sources: Vec<(&String, u64)> = ir.sources.iter().map(|(n, s)| (n, s.lovelace)).collect();
    for (name, lovelace) in &sources {
        if *lovelace < min_lovelace {
            return Err(Error::Materialise(format!(
                "source `{name}` holds {lovelace} lovelace, below the minimum {min_lovelace}"
            )));
        }
    }
    let total_sources = sources
        .iter()
        .try_fold(0u64, |acc, (_, l)| acc.checked_add(*l))
        .ok_or_else(|| Error::Materialise("source total overflows".to_string()))?;
    let (mut selected, chunk_sizes) = plan_funding(
        &context.offered,
        sources.len(),
        total_sources,
        min_for(&final_change, pp)?,
        pp,
    )?;
    // Spent as a set; sorted so the manifest does not depend on offer order.
    selected.sort_by_key(|u| (u.tx_id, u.index));
    let run_id = run_id(
        &derived.hash,
        seed,
        pp,
        &selected,
        &final_change,
        &funding_address,
        context.magic,
    );
    let mut chunks: Vec<&[(&String, u64)]> = Vec::new();
    let mut rest: &[(&String, u64)] = &sources;
    for size in &chunk_sizes {
        let (chunk, tail) = rest.split_at((*size).min(rest.len()));
        chunks.push(chunk);
        rest = tail;
    }

    let mut carry: Vec<(Input, u64)> = selected
        .iter()
        .map(|u| (Input::new(Hash::new(u.tx_id), u.index), u.lovelace))
        .collect();
    let mut carry_names: Vec<ResolvedInput> = selected
        .iter()
        .map(|u| ResolvedInput {
            reference: utxo_text(u),
            utxo: utxo_text(u),
        })
        .collect();
    for (k, chunk) in chunks.iter().enumerate() {
        let index = txs.len();
        let mut planned: Vec<PlannedOutput> = chunk
            .iter()
            .map(|(_, lovelace)| PlannedOutput {
                address: run_address.clone(),
                lovelace: Some(*lovelace),
            })
            .collect();
        let change_to = if k + 1 == chunks.len() {
            &final_change
        } else {
            &funding_address
        };
        planned.push(PlannedOutput {
            address: change_to.clone(),
            lovelace: None,
        });
        let built = build_signed(
            &carry,
            &[],
            &planned,
            &metadata(&ir_prefix, &run_id, index),
            &funding_key,
            pp,
        )
        .map_err(|e| in_tx(&format!("fund/{k}"), e))?;
        let tx_hash = Hash::new(built.tx_id);
        for (i, (name, lovelace)) in chunk.iter().enumerate() {
            utxos.insert((*name).clone(), (Input::new(tx_hash, i as u64), *lovelace));
        }
        let change_index = chunk.len();
        let change = built
            .outputs
            .get(change_index)
            .copied()
            .ok_or_else(|| Error::Materialise("funding change output missing".to_string()))?;
        let name = format!("fund/{k}");
        let mut entry = manifest_tx(
            index,
            &name,
            TxKind::Funding,
            &built,
            carry_names,
            vec![],
            vec![],
        );
        entry.sources = chunk.iter().map(|(name, _)| (*name).clone()).collect();
        txs.push(entry);
        carry = vec![(Input::new(tx_hash, change_index as u64), change)];
        carry_names = vec![ResolvedInput {
            reference: format!("{name}.{change_index}"),
            utxo: format!("{}#{change_index}", hex::encode(built.tx_id)),
        }];
    }

    // --- Experiment -------------------------------------------------------
    let member_of = derive::membership(&derived.conflict_sets);
    for name in &derived.topo_order {
        let tx = ir
            .txs
            .get(name)
            .ok_or_else(|| Error::Graph(format!("missing tx `{name}`")))?;
        let index = txs.len();
        let inputs = resolve_all(&utxos, &tx.inputs)?;
        let reference_inputs = resolve_all(&utxos, &tx.reference_inputs)?;
        let refs: Vec<Input> = reference_inputs
            .iter()
            .map(|(input, _)| input.clone())
            .collect();
        let mut planned = Vec::new();
        for output in &tx.outputs {
            let lovelace = match output.lovelace {
                Amount::Fixed(l) => {
                    if l < min_lovelace {
                        return Err(Error::Materialise(format!(
                            "tx `{name}`: output of {l} lovelace is below the minimum {min_lovelace}"
                        )));
                    }
                    Some(l)
                }
                Amount::Keyword(AmountKeyword::Min) => Some(min_lovelace),
                Amount::Keyword(AmountKeyword::Change) => None,
            };
            planned.push(PlannedOutput {
                address: run_address.clone(),
                lovelace,
            });
        }
        let built = build_signed(
            &inputs,
            &refs,
            &planned,
            &metadata(&ir_prefix, &run_id, index),
            &run_key,
            pp,
        )
        .map_err(|e| in_tx(name, e))?;
        let tx_hash = Hash::new(built.tx_id);
        for (i, lovelace) in built.outputs.iter().enumerate() {
            utxos.insert(
                format!("{name}.{i}"),
                (Input::new(tx_hash, i as u64), *lovelace),
            );
        }
        let mut entry = manifest_tx(
            index,
            name,
            TxKind::Experiment,
            &built,
            resolved(&tx.inputs, &inputs),
            resolved(&tx.reference_inputs, &reference_inputs),
            tx.labels.clone(),
        );
        entry.conflict_sets = member_of
            .get(name.as_str())
            .map(|sets| sets.iter().cloned().collect())
            .unwrap_or_default();
        entry.orphan_on = derived.orphan_on.get(name).cloned().unwrap_or_default();
        txs.push(entry);
    }

    Ok(Manifest {
        version: 0,
        experiment: experiment.to_string(),
        ir_hash: derived.hash,
        run_id,
        seed,
        era: context.era.clone(),
        tx_format: "conway".to_string(),
        network: context.network,
        network_magic: context.magic,
        pparams: pp.clone(),
        pparams_hash: pparams_hash(pp),
        funding_address: bech32(&funding_address)?,
        change_address: match &context.change_address {
            Some(_) => Some(bech32(&final_change)?),
            None => None,
        },
        run_address: bech32(&run_address)?,
        funding_utxos: selected.iter().map(utxo_text).collect(),
        txs,
    })
}

/// The run key that owns every source and experiment output: blake2b256 over
/// a tag, the funding key, the IR hash and the seed. `txflow run-key` writes
/// it out so leftover funds can be spent with other tools.
pub fn run_key(funding_key: &SecretKeyBytes, ir_hash: &str, seed: u64) -> SecretKeyBytes {
    let mut hasher = Hasher::<256>::new();
    hasher.input(b"txflow/run-key/v0");
    hasher.input(&funding_key.0);
    hasher.input(ir_hash.as_bytes());
    hasher.input(&seed.to_be_bytes());
    SecretKeyBytes(*hasher.finalize())
}

/// Name the transaction a materialise error happened in.
fn in_tx(name: &str, error: Error) -> Error {
    match error {
        Error::Materialise(message) => Error::Materialise(format!("tx `{name}`: {message}")),
        other => other,
    }
}

/// First 8 bytes of blake2b256 over the IR hash, the seed, the protocol
/// parameters used, the funding UTxOs spent (with their lovelace), the change
/// and funding addresses and the network magic: the same inputs give the same id.
/// `funding` must already be sorted, so offer order does not matter.
fn run_id(
    ir_hash: &str,
    seed: u64,
    pp: &ProtocolParams,
    funding: &[&OfferedUtxo],
    change_address: &Address,
    funding_address: &Address,
    magic: u32,
) -> String {
    let mut hasher = Hasher::<256>::new();
    hasher.input(b"txflow/run-id/v0");
    hasher.input(ir_hash.as_bytes());
    hasher.input(&seed.to_be_bytes());
    hasher.input(pparams_hash(pp).as_bytes());
    for utxo in funding {
        hasher.input(&utxo.tx_id);
        hasher.input(&utxo.index.to_be_bytes());
        hasher.input(&utxo.lovelace.to_be_bytes());
    }
    hasher.input(&change_address.to_vec());
    hasher.input(&funding_address.to_vec());
    hasher.input(&magic.to_be_bytes());
    let digest = hasher.finalize();
    hex::encode(digest.as_ref().iter().take(8).copied().collect::<Vec<u8>>())
}

/// Enterprise address (payment key only) of `key`.
fn key_address(key: &SecretKey, network: Network) -> Address {
    let key_hash = Hasher::<224>::hash(key.public_key().as_ref());
    Address::Shelley(ShelleyAddress::new(
        network,
        ShelleyPaymentPart::key_hash(key_hash),
        ShelleyDelegationPart::Null,
    ))
}

fn bech32(address: &Address) -> Result<String> {
    address
        .to_bech32()
        .map_err(|e| Error::Materialise(format!("address encoding: {e}")))
}

fn utxo_text(utxo: &&OfferedUtxo) -> String {
    format!("{}#{}", hex::encode(utxo.tx_id), utxo.index)
}

/// Choose the offered UTxOs to spend and how many sources each funding tx
/// creates, so that every funding tx stays under `max_tx_size`. The first
/// funding tx spends all selected UTxOs, so it has room for fewer sources.
fn plan_funding<'a>(
    offered: &'a [OfferedUtxo],
    sources: usize,
    total_sources: u64,
    min_change: u64,
    pp: &ProtocolParams,
) -> Result<(Vec<&'a OfferedUtxo>, Vec<usize>)> {
    if sources == 0 {
        return Ok((Vec::new(), Vec::new()));
    }
    let rest_capacity = funding_capacity(1, pp);
    if rest_capacity == 0 {
        return Err(Error::Materialise(format!(
            "max_tx_size {} is too small for a funding transaction",
            pp.max_tx_size
        )));
    }
    let mut reserved_txs: usize = 1;
    for _ in 0..8 {
        let reserve = max_fee(pp)?
            .checked_mul(reserved_txs as u64)
            .ok_or_else(|| Error::Materialise("fee reserve overflows".to_string()))?;
        let needed = total_sources
            .checked_add(reserve)
            .and_then(|n| n.checked_add(min_change))
            .ok_or_else(|| Error::Materialise("funding total overflows".to_string()))?;
        let selected = select_offered(offered, needed)?;
        let first_capacity = funding_capacity(selected.len(), pp);
        if first_capacity == 0 {
            return Err(Error::Materialise(format!(
                "the {} offered UTxOs needed do not fit in one funding transaction; \
                 offer fewer, larger UTxOs",
                selected.len()
            )));
        }
        let mut sizes = vec![first_capacity.min(sources)];
        let mut left = sources.saturating_sub(first_capacity);
        while left > 0 {
            sizes.push(rest_capacity.min(left));
            left = left.saturating_sub(rest_capacity);
        }
        if sizes.len() <= reserved_txs {
            return Ok((selected, sizes));
        }
        reserved_txs = sizes.len();
    }
    Err(Error::Materialise(
        "funding plan did not converge".to_string(),
    ))
}

/// How many sources a funding tx with `inputs` inputs can create.
fn funding_capacity(inputs: usize, pp: &ProtocolParams) -> usize {
    let fixed = TX_OVERHEAD_BOUND
        .saturating_add(CHANGE_SIZE_BOUND)
        .saturating_add(INPUT_SIZE_BOUND.saturating_mul(inputs as u64));
    let room = pp.max_tx_size.saturating_sub(fixed) / OUTPUT_SIZE_BOUND;
    usize::try_from(room)
        .unwrap_or(usize::MAX)
        .min(SOURCES_PER_FUNDING_TX)
}

/// The smallest prefix of `offered` whose value covers `needed`.
fn select_offered(offered: &[OfferedUtxo], needed: u64) -> Result<Vec<&OfferedUtxo>> {
    let mut selected = Vec::new();
    let mut total: u64 = 0;
    for utxo in offered {
        if total >= needed {
            break;
        }
        total = total.saturating_add(utxo.lovelace);
        selected.push(utxo);
    }
    if total < needed {
        return Err(Error::Materialise(format!(
            "offered UTxOs hold {total} lovelace; the experiment needs {needed} \
             (sources, a fee reserve per funding tx, and change)"
        )));
    }
    Ok(selected)
}

fn resolved(refs: &[String], inputs: &[(Input, u64)]) -> Vec<ResolvedInput> {
    refs.iter()
        .zip(inputs)
        .map(|(r, (input, _))| ResolvedInput {
            reference: r.clone(),
            utxo: format!("{}#{}", hex::encode(input.tx_hash.0), input.txo_index),
        })
        .collect()
}

fn resolve_all(
    utxos: &BTreeMap<String, (Input, u64)>,
    refs: &[String],
) -> Result<Vec<(Input, u64)>> {
    refs.iter()
        .map(|r| {
            // Validated by derive; this only fails on an internal ordering bug.
            utxos
                .get(r)
                .cloned()
                .ok_or_else(|| Error::Materialise(format!("`{r}` is not built yet")))
        })
        .collect()
}

/// `(160 + output size) * coinsPerUTxOByte` for an ADA-only output to
/// `address`, sized with the largest coin encoding so it is an upper bound.
fn min_for(address: &Address, pp: &ProtocolParams) -> Result<u64> {
    // Map header, two keys, the address's bytes header (2) and bytes, a 9-byte coin.
    let size = 1 + 1 + 2 + address.to_vec().len() as u64 + 1 + 9;
    (OUTPUT_OVERHEAD + size)
        .checked_mul(pp.coins_per_utxo_byte)
        .ok_or_else(|| Error::Materialise("min UTxO overflows".to_string()))
}

/// Parse `change_address`: a Shelley payment address on `network`.
fn change_address(text: &str, network: Network) -> Result<Address> {
    let address =
        Address::from_bech32(text).map_err(|e| Error::Context(format!("change_address: {e}")))?;
    match &address {
        Address::Shelley(shelley)
            if shelley.network() == network
                && matches!(shelley.payment(), ShelleyPaymentPart::Key(_)) =>
        {
            Ok(address)
        }
        Address::Shelley(shelley) if matches!(shelley.payment(), ShelleyPaymentPart::Script(_)) => {
            Err(Error::Context(format!(
                "change_address `{text}` is a script address; use a key address"
            )))
        }
        Address::Shelley(_) => Err(Error::Context(format!(
            "change_address `{text}` is for another network"
        ))),
        _ => Err(Error::Context(format!(
            "change_address `{text}` is not a Shelley payment address"
        ))),
    }
}

/// `blake2b256:<hex>` over the protocol parameters used.
fn pparams_hash(pp: &ProtocolParams) -> String {
    let mut hasher = Hasher::<256>::new();
    for value in [
        pp.min_fee_a,
        pp.min_fee_b,
        pp.max_tx_size,
        pp.coins_per_utxo_byte,
    ] {
        hasher.input(&value.to_be_bytes());
    }
    format!("blake2b256:{}", hex::encode(hasher.finalize()))
}

/// Fee of a transaction of the maximum size.
fn max_fee(pp: &ProtocolParams) -> Result<u64> {
    pp.min_fee_a
        .checked_mul(pp.max_tx_size)
        .and_then(|f| f.checked_add(pp.min_fee_b))
        .ok_or_else(|| Error::Materialise("fee overflows".to_string()))
}

/// Build, sign and balance: the change output takes inputs minus outputs minus
/// fee, and the fee is raised until it covers `a * size + b` of the signed tx.
fn build_signed(
    inputs: &[(Input, u64)],
    reference_inputs: &[Input],
    planned: &[PlannedOutput],
    metadata: &[u8],
    key: &SecretKey,
    pp: &ProtocolParams,
) -> Result<Built> {
    let min_change = match planned.iter().find(|o| o.lovelace.is_none()) {
        Some(change) => min_for(&change.address, pp)?,
        None => return Err(Error::Materialise("no change output".to_string())),
    };
    let total_in = inputs
        .iter()
        .try_fold(0u64, |acc, (_, l)| acc.checked_add(*l))
        .ok_or_else(|| Error::Materialise("input total overflows".to_string()))?;
    let fixed = planned
        .iter()
        .filter_map(|o| o.lovelace)
        .try_fold(0u64, |acc, l| acc.checked_add(l))
        .ok_or_else(|| Error::Materialise("output total overflows".to_string()))?;

    let mut fee: u64 = 0;
    for _ in 0..FEE_ROUNDS {
        let change = total_in
            .checked_sub(fixed)
            .and_then(|c| c.checked_sub(fee))
            .ok_or_else(|| {
                Error::Materialise(format!(
                    "inputs hold {total_in} lovelace, outputs need {fixed} plus fee {fee}"
                ))
            })?;
        if change < min_change {
            return Err(Error::Materialise(format!(
                "change of {change} lovelace is below the minimum {min_change} \
                 (inputs {total_in}, outputs {fixed}, fee {fee})"
            )));
        }
        let mut staging = StagingTransaction::new();
        for (input, _) in inputs {
            staging = staging.input(input.clone());
        }
        // Pallas sorts spent inputs but not reference inputs; sort both so
        // the tx id does not depend on the order the IR lists them in.
        let mut sorted_refs: Vec<&Input> = reference_inputs.iter().collect();
        sorted_refs.sort_by_key(|i| (i.tx_hash.0, i.txo_index));
        for input in sorted_refs {
            staging = staging.reference_input(input.clone());
        }
        let mut amounts = Vec::new();
        for output in planned {
            let lovelace = output.lovelace.unwrap_or(change);
            amounts.push(lovelace);
            staging = staging.output(Output::new(output.address.clone(), lovelace));
        }
        let staging = staging.fee(fee).add_auxiliary_data(metadata.to_vec());
        // Pallas drops metadata it cannot decode without an error, and the
        // metadata is what keeps the members of a conflict set distinct.
        if staging.auxiliary_data.is_none() {
            return Err(Error::Materialise(
                "the attribution metadata does not decode".to_string(),
            ));
        }
        let signed = staging
            .build_conway_raw()
            .and_then(|built| built.sign(key))
            .map_err(|e| Error::Materialise(format!("build: {e}")))?;
        let size = signed.tx_bytes.0.len();
        let required = pp
            .min_fee_a
            .checked_mul(size as u64)
            .and_then(|f| f.checked_add(pp.min_fee_b))
            .ok_or_else(|| Error::Materialise("fee overflows".to_string()))?;
        if fee >= required {
            if size as u64 > pp.max_tx_size {
                return Err(Error::Materialise(format!(
                    "{size} bytes exceeds the maximum transaction size {}",
                    pp.max_tx_size
                )));
            }
            return Ok(Built {
                tx_id: signed.tx_hash.0,
                bytes: signed.tx_bytes.0,
                fee,
                outputs: amounts,
            });
        }
        fee = required;
    }
    Err(Error::Materialise("fee did not converge".to_string()))
}

fn manifest_tx(
    index: usize,
    name: &str,
    kind: TxKind,
    built: &Built,
    inputs: Vec<ResolvedInput>,
    reference_inputs: Vec<ResolvedInput>,
    labels: Vec<String>,
) -> ManifestTx {
    ManifestTx {
        index,
        name: name.to_string(),
        kind,
        tx_id: hex::encode(built.tx_id),
        fee: built.fee,
        size: built.bytes.len(),
        inputs,
        reference_inputs,
        outputs: built.outputs.clone(),
        sources: vec![],
        labels,
        conflict_sets: vec![],
        orphan_on: vec![],
        cbor_hex: hex::encode(&built.bytes),
    }
}

/// Shelley-format auxiliary data: `{674: {"msg": ["txflow:<ir>", "<run>#<index>"]}}`.
fn metadata(ir_prefix: &str, run_id: &str, index: usize) -> Vec<u8> {
    let mut out = Vec::new();
    cbor_head(&mut out, 5, 1); // map(1)
    cbor_head(&mut out, 0, METADATA_LABEL);
    cbor_head(&mut out, 5, 1); // map(1)
    cbor_text(&mut out, "msg");
    cbor_head(&mut out, 4, 2); // array(2)
    cbor_text(&mut out, &format!("txflow:{ir_prefix}"));
    cbor_text(&mut out, &format!("{run_id}#{index}"));
    out
}

fn cbor_text(out: &mut Vec<u8>, text: &str) {
    cbor_head(out, 3, text.len() as u64);
    out.extend_from_slice(text.as_bytes());
}

/// CBOR major type and argument, shortest encoding.
fn cbor_head(out: &mut Vec<u8>, major: u8, value: u64) {
    let m = major << 5;
    if value < 24 {
        out.push(m | value as u8);
    } else if value <= u64::from(u8::MAX) {
        out.push(m | 24);
        out.push(value as u8);
    } else if value <= u64::from(u16::MAX) {
        out.push(m | 25);
        out.extend_from_slice(&(value as u16).to_be_bytes());
    } else if value <= u64::from(u32::MAX) {
        out.push(m | 26);
        out.extend_from_slice(&(value as u32).to_be_bytes());
    } else {
        out.push(m | 27);
        out.extend_from_slice(&value.to_be_bytes());
    }
}
