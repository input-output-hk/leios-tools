//! Expand an experiment spec into a graph IR.

use std::collections::{BTreeMap, BTreeSet};

use crate::derive;
use crate::error::{Error, Result};
use crate::expr;
use crate::ir::{Ir, Source, Tx};
use crate::patterns::{self, Args, Flags, MAX_ELEMENTS};
use crate::spec::ExperimentSpec;

/// Expand patterns, merge explicit sources and txs, validate, and derive relations.
pub fn expand(spec: &ExperimentSpec) -> Result<Ir> {
    if spec.version != 0 {
        return Err(Error::Spec(format!("unsupported version {}", spec.version)));
    }
    if spec.wallets.is_empty() {
        return Err(Error::Spec("at least one wallet is required".to_string()));
    }
    // With one wallet it is the default owner; with several, each pattern names one.
    let default_wallet = match spec.wallets.keys().collect::<Vec<_>>().as_slice() {
        [only] => Some((*only).clone()),
        _ => None,
    };
    for wallet in spec.wallets.keys() {
        check_name(wallet)?;
    }
    let mut ids = BTreeSet::new();
    let mut namespaces = BTreeSet::new();
    let mut total_nodes: u64 = 0;

    let mut sources: BTreeMap<String, Source> = BTreeMap::new();
    let mut txs: BTreeMap<String, Tx> = BTreeMap::new();

    for instance in &spec.patterns {
        check_name(&instance.id)?;
        if !ids.insert(instance.id.as_str()) {
            return Err(Error::Spec(format!(
                "pattern id `{}` is used twice",
                instance.id
            )));
        }
        let count = match &instance.count {
            Some(value) => {
                expr::eval(value, &spec.params).map_err(|e| in_arg(&instance.id, "count", e))?
            }
            None => 1,
        };
        if count > patterns::MAX_SIZE {
            return Err(Error::Spec(format!(
                "pattern `{}`: count is {count}, the maximum is {}",
                instance.id,
                patterns::MAX_SIZE
            )));
        }
        if count == 0 {
            return Err(Error::Spec(format!(
                "pattern `{}`: count must be at least 1",
                instance.id
            )));
        }
        let mut with = instance.with.clone();
        let flags = Flags {
            conflict: take_string(&mut with, "conflict")
                .map_err(|e| in_pattern(&instance.id, e))?,
        };
        let owner =
            match take_string(&mut with, "wallet").map_err(|e| in_pattern(&instance.id, e))? {
                Some(wallet) => wallet,
                None => default_wallet.clone().ok_or_else(|| {
                    Error::Spec(format!(
                        "pattern `{}`: several wallets are declared, so `wallet` is required",
                        instance.id
                    ))
                })?,
            };
        if !spec.wallets.contains_key(&owner) {
            return Err(Error::Spec(format!(
                "pattern `{}`: unknown wallet `{owner}`",
                instance.id
            )));
        }
        let mut values = BTreeMap::new();
        for (key, value) in &with {
            values.insert(
                key.clone(),
                expr::eval(value, &spec.params).map_err(|e| in_arg(&instance.id, key, e))?,
            );
        }
        let fragment = patterns::generate(
            &instance.pattern,
            Args::new(&instance.pattern, values),
            &flags,
            &owner,
        )
        .map_err(|e| in_pattern(&instance.id, e))?;
        // Every source, tx, input and output costs memory; count them all.
        let nodes = (fragment.sources.len()
            + fragment
                .txs
                .values()
                .map(|t| 1 + t.inputs.len() + t.reference_inputs.len() + t.outputs.len())
                .sum::<usize>()) as u64;
        total_nodes = total_nodes.saturating_add(nodes.saturating_mul(count));
        if total_nodes > MAX_ELEMENTS {
            return Err(Error::Spec(format!(
                "pattern `{}`: the experiment would exceed {MAX_ELEMENTS} graph elements \
                 (sources, txs, inputs and outputs)",
                instance.id
            )));
        }
        for k in 0..count {
            // Named by whether `count` is written, not by its value, so that
            // scaling `count` through a param keeps names stable.
            let prefix = if instance.count.is_none() {
                format!("{}/", instance.id)
            } else {
                format!("{}_{k}/", instance.id)
            };
            // `x` with count 2 and an instance named `x_0` would share `x_0/`.
            if !namespaces.insert(prefix.clone()) {
                return Err(Error::Spec(format!(
                    "pattern `{}`: namespace `{prefix}` is already used by another instance",
                    instance.id
                )));
            }
            for (name, source) in &fragment.sources {
                insert_unique(&mut sources, format!("{prefix}{name}"), source.clone())?;
            }
            for (name, tx) in &fragment.txs {
                let mut tx = tx.clone();
                tx.inputs = tx.inputs.iter().map(|r| format!("{prefix}{r}")).collect();
                tx.reference_inputs = tx
                    .reference_inputs
                    .iter()
                    .map(|r| format!("{prefix}{r}"))
                    .collect();
                insert_unique(&mut txs, format!("{prefix}{name}"), tx)?;
            }
        }
    }

    for (name, source) in &spec.sources {
        check_name(name)?;
        insert_unique(&mut sources, name.clone(), source.clone())?;
    }
    for (name, tx) in &spec.txs {
        check_name(name)?;
        insert_unique(&mut txs, name.clone(), tx.clone())?;
    }

    // Canonical form: inputs and reference inputs are sets, so list them sorted.
    for tx in txs.values_mut() {
        tx.inputs.sort();
        tx.reference_inputs.sort();
    }

    let mut ir = Ir {
        version: 0,
        wallets: spec.wallets.keys().cloned().collect(),
        sources,
        txs,
        derived: None,
    };
    ir.derived = Some(derive::derive(&ir)?);
    Ok(ir)
}

/// Name the pattern instance an error came from.
fn in_pattern(id: &str, error: Error) -> Error {
    match error {
        Error::Spec(message) => Error::Spec(format!("pattern `{id}`: {message}")),
        other => other,
    }
}

/// Name the pattern argument an expression error came from.
fn in_arg(id: &str, arg: &str, error: Error) -> Error {
    match error {
        Error::Spec(message) => Error::Spec(format!("pattern `{id}`, `{arg}`: {message}")),
        other => other,
    }
}

fn take_string(
    with: &mut BTreeMap<String, serde_yaml::Value>,
    key: &str,
) -> Result<Option<String>> {
    match with.remove(key) {
        None => Ok(None),
        Some(serde_yaml::Value::String(s)) => Ok(Some(s)),
        Some(other) => Err(Error::Spec(format!(
            "`{key}` must be a string, got {other:?}"
        ))),
    }
}

fn check_name(name: &str) -> Result<()> {
    let mut chars = name.chars();
    let first_ok = chars.next().is_some_and(|c| c.is_ascii_lowercase());
    if !first_ok || !chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_') {
        return Err(Error::Spec(format!(
            "name `{name}` must match [a-z][a-z0-9_]*"
        )));
    }
    Ok(())
}

fn insert_unique<V>(map: &mut BTreeMap<String, V>, name: String, value: V) -> Result<()> {
    if map.contains_key(&name) {
        return Err(Error::Spec(format!("`{name}` is defined twice")));
    }
    map.insert(name, value);
    Ok(())
}
