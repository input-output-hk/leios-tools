//! Graph IR: the version- and network-free description of an experiment.
//!
//! Inputs are the primitive. Each transaction lists the outputs it spends
//! (`inputs`) and the outputs it only reads (`reference_inputs`). Conflicts,
//! orphans and reference contention are derived from those lists by
//! [`crate::derive`]; they are never declared.
//!
//! This MVP supports plain payments: no scripts, tokens, datums or mint.
//! Unknown fields are rejected so an unsupported feature fails loudly.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// The graph IR. Maps are `BTreeMap`s so serialisation is canonical.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Ir {
    pub version: u32,
    pub wallets: Vec<String>,
    pub sources: BTreeMap<String, Source>,
    pub txs: BTreeMap<String, Tx>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub derived: Option<Derived>,
}

/// An abstract funding output the materialiser must provide.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Source {
    pub owner: String,
    pub lovelace: u64,
}

/// One symbolic transaction.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Tx {
    /// Outputs spent. On the ledger inputs are a set, so order is not significant.
    pub inputs: Vec<String>,
    /// Outputs read but not spent.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reference_inputs: Vec<String>,
    pub outputs: Vec<OutputSpec>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<String>,
}

/// One output of a symbolic transaction.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct OutputSpec {
    /// Owning wallet.
    pub to: String,
    pub lovelace: Amount,
}

/// How much an output holds.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq)]
#[serde(untagged)]
pub enum Amount {
    /// An exact amount of lovelace.
    Fixed(u64),
    /// `min` or `change`.
    Keyword(AmountKeyword),
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum AmountKeyword {
    /// The minimum the ledger accepts for this output.
    Min,
    /// Whatever is left after fees and the other outputs. Exactly one per tx.
    Change,
}

/// Relations computed from the inputs, plus the content hash.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct Derived {
    pub topo_order: Vec<String>,
    /// Contested output → transactions that spend it (two or more).
    pub conflict_sets: BTreeMap<String, Vec<String>>,
    /// Output spent by some transactions and only read by others.
    pub reference_contention: BTreeMap<String, Contention>,
    /// Transaction → conflict sets among its ancestors.
    pub orphan_on: BTreeMap<String, Vec<String>>,
    /// `blake2b256:<hex>` over the canonical JSON of the IR without `derived`.
    pub hash: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct Contention {
    pub spent_by: Vec<String>,
    pub read_by: Vec<String>,
}

/// A parsed input reference.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Ref {
    /// A named source.
    Source(String),
    /// Output `index` of transaction `tx`.
    Output { tx: String, index: usize },
}

impl Ref {
    /// Parse `name` (a source) or `tx.N` (output N of tx). `N` has no leading
    /// zeros, so each output has exactly one spelling and string keys are safe.
    pub fn parse(text: &str) -> Result<Ref> {
        if let Some((tx, index)) = text.rsplit_once('.') {
            if !index.is_empty() && index.chars().all(|c| c.is_ascii_digit()) {
                if index.len() > 1 && index.starts_with('0') {
                    return Err(Error::Graph(format!(
                        "`{text}`: output index has a leading zero"
                    )));
                }
                let index: usize = index
                    .parse()
                    .map_err(|_| Error::Graph(format!("bad output index in `{text}`")))?;
                return Ok(Ref::Output {
                    tx: tx.to_string(),
                    index,
                });
            }
        }
        if text.is_empty() {
            return Err(Error::Graph("empty reference".to_string()));
        }
        Ok(Ref::Source(text.to_string()))
    }
}
