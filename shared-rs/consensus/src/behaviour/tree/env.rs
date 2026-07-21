//! Environment, chain state, and the per-tick context.
//!
//! [`DynamicEnv`] is the resolved env: a name-keyed map of typed values (so
//! arbitrary params can be declared in TOML, overlaid across includes, and
//! addressed by REST `:key`). Keys may be dotted for owner-namespaced params
//! (`network_shape.packet_delay`) vs. shared (`trigger_slot`).
//!
//! [`NativeChainState`] is the read-only node metrics, rebuilt each tick and
//! passed by `&`. [`TickCtx`] bundles the pure inputs a tick may read.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

/// Externally-mutable parameters (config + later REST), read by conditions and
/// actions (FR-010). Keys are dotted for owner-namespaced params.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DynamicEnv(pub BTreeMap<String, EnvValue>);

/// A typed env value. The small union lets conditions type-check references.
#[derive(Debug, Clone, PartialEq)]
pub enum EnvValue {
    U64(u64),
    F64(f64),
    Str(String),
    Bool(bool),
}

/// Guarded handle to the env. The tick reads it; a later REST surface writes
/// it. `std` (not tokio) — this crate is sans-IO.
pub type EnvHandle = Arc<RwLock<DynamicEnv>>;

/// Runtime override store for Action-leaf parameters, keyed
/// `"<behaviour_id>.<field>"`. A live coordinator/REST update writes it and the
/// tick reads it (sans-IO — a guarded in-memory map, like [`EnvHandle`]). Values
/// are TOML scalars so any leaf param round-trips, including signed integers
/// (e.g. an `offset`) that [`EnvValue`] cannot hold. An absent key means "use the
/// leaf's compiled default", so overriding one field preserves all other leaf
/// state (counters, RNG progression) — only the named value changes.
pub type ActionParamStore = Arc<RwLock<BTreeMap<String, toml::Value>>>;

impl EnvValue {
    /// A human-readable type name, for validation error messages.
    pub fn type_name(&self) -> &'static str {
        match self {
            EnvValue::U64(_) => "integer",
            EnvValue::F64(_) => "float",
            EnvValue::Str(_) => "string",
            EnvValue::Bool(_) => "bool",
        }
    }

    /// View as a number (`U64`/`F64`), for numeric comparison.
    pub fn as_number(&self) -> Option<f64> {
        match self {
            EnvValue::U64(v) => Some(*v as f64),
            EnvValue::F64(v) => Some(*v),
            _ => None,
        }
    }

    /// View as a string.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            EnvValue::Str(s) => Some(s),
            _ => None,
        }
    }

    /// View as a bool.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            EnvValue::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// True for `U64`/`F64`.
    pub fn is_numeric(&self) -> bool {
        matches!(self, EnvValue::U64(_) | EnvValue::F64(_))
    }
}

impl DynamicEnv {
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn get(&self, key: &str) -> Option<&EnvValue> {
        self.0.get(key)
    }

    pub fn insert(&mut self, key: impl Into<String>, value: EnvValue) {
        self.0.insert(key.into(), value);
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }
}

/// Node metrics tracked by the host and read-only to the tree (FR-011). Rebuilt
/// each tick. New fields are added as conditions require them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NativeChainState {
    pub current_slot: u64,
    pub current_epoch: u64,
    pub mempool_tx_count: usize,
}

/// The chain-state field names a `cardano.*` reference may name. Centralised so
/// condition validation and evaluation agree on the set.
pub const CHAIN_FIELDS: &[&str] = &["current_slot", "current_epoch", "mempool_tx_count"];

impl NativeChainState {
    /// Read a known numeric chain field by its `cardano.<name>` short name.
    /// Returns `None` for an unknown field.
    pub fn numeric_field(&self, name: &str) -> Option<u64> {
        match name {
            "current_slot" => Some(self.current_slot),
            "current_epoch" => Some(self.current_epoch),
            "mempool_tx_count" => Some(self.mempool_tx_count as u64),
            _ => None,
        }
    }
}

/// Everything a tick may read — pure inputs, no I/O, no clock.
pub struct TickCtx<'a> {
    /// Read from the [`EnvHandle`] by the caller before ticking.
    pub env: &'a DynamicEnv,
    /// Rebuilt each tick, read-only.
    pub state: &'a NativeChainState,
    /// Root seed for deterministic leaf choices.
    pub seed: u64,
    /// Optional live overrides for Action-leaf params, keyed `"<id>.<field>"`.
    /// `None` (the default) means every leaf uses its compiled params.
    pub action_params: Option<&'a ActionParamStore>,
}

impl<'a> TickCtx<'a> {
    /// Build a context with no live action-param overrides.
    pub fn new(env: &'a DynamicEnv, state: &'a NativeChainState, seed: u64) -> Self {
        Self {
            env,
            state,
            seed,
            action_params: None,
        }
    }

    /// A live override for `<behaviour_id>.<field>`, if one is set. Returns the
    /// raw TOML scalar; the leaf coerces it to its own field type.
    pub fn action_override(&self, behaviour_id: &str, field: &str) -> Option<toml::Value> {
        let store = self.action_params?;
        let map = store.read().ok()?;
        map.get(&format!("{behaviour_id}.{field}")).cloned()
    }
}

/// Read-side seam for live Action-leaf parameter overrides. The generic tick
/// applies overrides through this trait so it never reaches into a concrete
/// context's internals; each tree instantiation's per-tick view implements it.
pub trait ParamOverrides {
    /// All live overrides addressed to `behaviour_id`, as `(field, value)`
    /// pairs (store keys `"<behaviour_id>.<field>"`). Empty when nothing is
    /// addressed to that id.
    fn action_overrides(&self, behaviour_id: &str) -> Vec<(String, toml::Value)>;
}

impl ParamOverrides for TickCtx<'_> {
    fn action_overrides(&self, behaviour_id: &str) -> Vec<(String, toml::Value)> {
        let Some(store) = self.action_params else {
            return Vec::new();
        };
        let prefix = format!("{behaviour_id}.");
        // Collect this leaf's overrides under the lock (a `BTreeMap` prefix
        // range — no full scan), then release the guard before returning so the
        // caller never holds the read lock across leaf `set_param` calls.
        let Ok(map) = store.read() else {
            return Vec::new();
        };
        map.range(prefix.clone()..)
            .take_while(|(k, _)| k.starts_with(&prefix))
            .map(|(k, v)| (k[prefix.len()..].to_string(), v.clone()))
            .collect()
    }
}

/// A tree instantiation's read-model family: maps the tick lifetime to the
/// concrete context type Conditions and Actions read this tick. A marker type
/// implements this to bind the engine to a concrete context; the per-tick view
/// may borrow (hence the lifetime-generic associated type), and must expose live
/// action-parameter overrides via [`ParamOverrides`].
pub trait TreeContext {
    /// The borrowed per-tick context view.
    type View<'a>: ParamOverrides;
}

/// The consensus tree instantiation: Conditions and Actions read a [`TickCtx`].
/// An uninhabited marker used only as a type parameter, never constructed.
#[derive(Debug, Clone, Copy)]
pub enum ConsensusCtx {}

impl TreeContext for ConsensusCtx {
    type View<'a> = TickCtx<'a>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_get_and_insert() {
        let mut env = DynamicEnv::new();
        env.insert("trigger_slot", EnvValue::U64(345600));
        env.insert("drop_rate", EnvValue::F64(0.15));
        env.insert("target", EnvValue::Str("pool-a".into()));
        env.insert("armed", EnvValue::Bool(true));

        assert_eq!(env.get("trigger_slot"), Some(&EnvValue::U64(345600)));
        assert_eq!(
            env.get("drop_rate").and_then(EnvValue::as_number),
            Some(0.15)
        );
        assert_eq!(env.get("target").and_then(EnvValue::as_str), Some("pool-a"));
        assert_eq!(env.get("armed").and_then(EnvValue::as_bool), Some(true));
        assert_eq!(env.get("missing"), None);
    }

    #[test]
    fn dotted_owner_namespaced_keys() {
        let mut env = DynamicEnv::new();
        env.insert("network_shape.packet_delay", EnvValue::U64(20));
        env.insert("trigger_slot", EnvValue::U64(10));
        // Shared and owner-namespaced keys coexist and are addressed verbatim.
        assert_eq!(
            env.get("network_shape.packet_delay"),
            Some(&EnvValue::U64(20))
        );
        assert!(env.contains_key("trigger_slot"));
        assert!(!env.contains_key("network_shape"));
    }

    #[test]
    fn chain_state_known_fields_resolve() {
        let s = NativeChainState {
            current_slot: 42,
            current_epoch: 1,
            mempool_tx_count: 7,
        };
        assert_eq!(s.numeric_field("current_slot"), Some(42));
        assert_eq!(s.numeric_field("current_epoch"), Some(1));
        assert_eq!(s.numeric_field("mempool_tx_count"), Some(7));
        assert_eq!(s.numeric_field("peers"), None);
        // CHAIN_FIELDS and numeric_field agree on the known set.
        for f in CHAIN_FIELDS {
            assert!(s.numeric_field(f).is_some(), "{f} should resolve");
        }
    }

    #[test]
    fn env_value_type_names() {
        assert_eq!(EnvValue::U64(1).type_name(), "integer");
        assert_eq!(EnvValue::F64(1.0).type_name(), "float");
        assert_eq!(EnvValue::Str("x".into()).type_name(), "string");
        assert_eq!(EnvValue::Bool(true).type_name(), "bool");
    }

    #[test]
    fn action_override_reads_store_by_id_and_field() {
        let env = DynamicEnv::new();
        let state = NativeChainState::default();
        let store: ActionParamStore = Arc::new(RwLock::new(BTreeMap::from([(
            "equivocate.ways".to_string(),
            toml::Value::Integer(4),
        )])));
        let ctx = TickCtx {
            env: &env,
            state: &state,
            seed: 0,
            action_params: Some(&store),
        };
        assert_eq!(
            ctx.action_override("equivocate", "ways"),
            Some(toml::Value::Integer(4))
        );
        assert_eq!(ctx.action_override("equivocate", "missing"), None);
        assert_eq!(ctx.action_override("other", "ways"), None);
        // No store → never an override.
        assert_eq!(
            TickCtx::new(&env, &state, 0).action_override("equivocate", "ways"),
            None
        );
    }
}
