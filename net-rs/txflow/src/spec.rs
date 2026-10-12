//! Experiment spec: what a person writes.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::ir::{Source, Tx};

/// An experiment file. `schedule` and `expect` are carried but not yet interpreted.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct ExperimentSpec {
    pub version: u32,
    pub experiment: String,
    pub seed: u64,
    #[serde(default)]
    pub params: BTreeMap<String, i64>,
    pub wallets: BTreeMap<String, WalletSpec>,
    #[serde(default)]
    pub patterns: Vec<PatternInstance>,
    #[serde(default)]
    pub sources: BTreeMap<String, Source>,
    #[serde(default)]
    pub txs: BTreeMap<String, Tx>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<serde_yaml::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expect: Option<serde_yaml::Value>,
}

/// A symbolic wallet. In this MVP all wallets bind to the run key.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct WalletSpec {}

/// One use of a pattern.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct PatternInstance {
    pub id: String,
    #[serde(rename = "use")]
    pub pattern: String,
    /// How many independent instances to emit. Default 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub count: Option<serde_yaml::Value>,
    #[serde(default)]
    pub with: BTreeMap<String, serde_yaml::Value>,
}

impl ExperimentSpec {
    pub fn from_yaml(text: &str) -> Result<ExperimentSpec> {
        from_yaml_strict(text)
    }
}

/// Parse YAML, rejecting a key repeated in any map. Deserializing straight
/// into a `BTreeMap` keeps the last value and drops the earlier ones
/// silently, so a copy-pasted tx or source would vanish from the graph.
pub(crate) fn from_yaml_strict<T: serde::de::DeserializeOwned>(text: &str) -> Result<T> {
    // A generic value rejects duplicate keys, with their location.
    serde_yaml::from_str::<serde_yaml::Value>(text)?;
    Ok(serde_yaml::from_str(text)?)
}
