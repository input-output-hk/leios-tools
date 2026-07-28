//! Config parsing, validation, and compilation to a [`BehaviourTree`].
//!
//! The engine loads a **self-contained** TOML config: a `[run]` block
//! (`name`/`seed`/`root`), `[env]` / `[env.<owner>]` parameters, and id-keyed
//! `[behaviours.<id>]` tables. Cross-file `includes` are resolved upstream by
//! the `bt.py --resolve` build step (research D13 amendment); a config that
//! still carries a non-empty `includes` is rejected here.
//!
//! Parsing and validation happen at load, before activation, so `tick` never
//! fails for config reasons. References expand to independent instances at
//! compile time (see [`super::behaviour`]).
//!
//! This module is **sans-IO**: it takes the config text (the consumer reads the
//! file) and returns a compiled tree. No filesystem access here.

use std::collections::BTreeMap;
use std::fmt;

use super::actions::{build_action, HonestAction, LeafAction};
use super::behaviour::{Behaviour, BehaviourId, BehaviourKind, BehaviourTree};
use super::condition::{Condition, ConditionExpr};
use super::env::{ConsensusCtx, EnvValue, TreeContext};
use crate::behaviour::registry::{child_seed, ActionSpec};

/// A precise load/validation error naming the offending id/field.
#[derive(Debug, Clone, PartialEq)]
pub enum ConfigError {
    /// The TOML did not parse.
    Toml(String),
    /// The config still carries unresolved `includes` (run `bt.py --resolve`).
    UnresolvedIncludes(Vec<String>),
    /// No `[run]` block, or it is missing `seed`/`root`/`name`.
    Run(String),
    /// `run.root` names a behaviour that is not defined.
    UnknownRoot(String),
    /// A `children`/`child` reference does not resolve to a defined behaviour.
    DanglingChild { parent: String, child: String },
    /// A reference cycle in the behaviour graph (would expand infinitely).
    Cycle(Vec<String>),
    /// A behaviour has an unknown `type`.
    UnknownType { id: String, ty: String },
    /// A composite/decorator has the wrong number of children, or a bad `count`.
    BadArity { id: String, msg: String },
    /// A behaviour table is missing/has a mistyped field.
    BadField { id: String, msg: String },
    /// A `Condition` expression failed to parse or type-check.
    Condition { id: String, msg: String },
    /// An `Action` `spec` failed to deserialise into a known `ActionSpec`.
    ActionSpec { id: String, msg: String },
    /// An `[env]` value had an unsupported type.
    Env { key: String, msg: String },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::Toml(m) => write!(f, "invalid TOML: {m}"),
            ConfigError::UnresolvedIncludes(xs) => write!(
                f,
                "config not resolved — run `bt.py --resolve` (unresolved includes: {})",
                xs.join(", ")
            ),
            ConfigError::Run(m) => write!(f, "invalid [run]: {m}"),
            ConfigError::UnknownRoot(r) => write!(f, "run.root {r:?} is not a defined behaviour"),
            ConfigError::DanglingChild { parent, child } => {
                write!(
                    f,
                    "behaviour {parent:?} references undefined child {child:?}"
                )
            }
            ConfigError::Cycle(path) => write!(f, "reference cycle: {}", path.join(" -> ")),
            ConfigError::UnknownType { id, ty } => {
                write!(f, "behaviour {id:?} has unknown type {ty:?}")
            }
            ConfigError::BadArity { id, msg } => write!(f, "behaviour {id:?}: {msg}"),
            ConfigError::BadField { id, msg } => write!(f, "behaviour {id:?}: {msg}"),
            ConfigError::Condition { id, msg } => {
                write!(f, "behaviour {id:?} condition: {msg}")
            }
            ConfigError::ActionSpec { id, msg } => write!(f, "behaviour {id:?} action spec: {msg}"),
            ConfigError::Env { key, msg } => write!(f, "env {key:?}: {msg}"),
        }
    }
}

impl std::error::Error for ConfigError {}

/// The run's identity and entry behaviour.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Run {
    pub name: String,
    pub seed: u64,
    pub root: BehaviourId,
}

/// Optional per-owner module documentation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ModuleMeta {
    pub revision: u32,
}

/// A structurally-parsed (but not yet expanded or domain-bound) behaviour
/// definition. `Condition` and `Action` keep their **raw** source — the
/// expression text and the `(kind, params)` of the action `spec` — so that a
/// single parsed config can be compiled against any tree instantiation via
/// [`BtConfig::compile_with`]. The domain binders (consensus, cluster, …) turn
/// the raw source into a concrete [`Condition`]/[`LeafAction`] at compile time.
#[derive(Debug, Clone)]
pub enum RawBehaviour {
    Selector(Vec<BehaviourId>),
    Sequence(Vec<BehaviourId>),
    Join(Vec<BehaviourId>),
    ForTicks {
        count: u32,
        child: BehaviourId,
    },
    /// Pure timing gate — childless and contribution-free, so it is
    /// domain-neutral and needs no binder.
    Wait {
        count: u32,
    },
    /// Raw condition expression source, bound by a condition binder.
    Condition(String),
    Honest,
    /// Raw action `kind` + the full `spec` params table (which also carries
    /// `kind`), bound by an action binder.
    Action {
        kind: String,
        params: toml::Table,
    },
}

/// The on-disk config in typed form. Compile it into a [`BehaviourTree`].
#[derive(Debug, Clone)]
pub struct BtConfig {
    pub run: Option<Run>,
    pub env: BTreeMap<String, EnvValue>,
    pub behaviours: BTreeMap<BehaviourId, RawBehaviour>,
    pub metadata: BTreeMap<String, ModuleMeta>,
    /// Always empty after a successful [`parse`](Self::parse) (a non-empty value
    /// is rejected); retained so the field round-trips the schema.
    pub includes: Vec<String>,
}

impl BtConfig {
    /// Parse a self-contained config from TOML text for the **consensus**
    /// instantiation. Rejects a non-empty `includes`, and eagerly checks that
    /// every `Action` spec deserialises into a known [`ActionSpec`] and every
    /// `Condition` expression parses, so those load-time errors surface at parse
    /// (as before the generic-compiler refactor). Does not yet check
    /// references/cycles — call [`validate`](Self::validate) or
    /// [`compile`](Self::compile).
    pub fn parse(text: &str) -> Result<BtConfig, ConfigError> {
        let cfg = BtConfig::parse_generic(text)?;
        // Consensus-eager binding checks (behaviour-preserving: these errors
        // fired at parse time before actions/conditions were stored raw).
        for (id, raw) in &cfg.behaviours {
            match raw {
                RawBehaviour::Action { params, .. } => {
                    let _spec: ActionSpec = toml::Value::Table(params.clone()).try_into().map_err(
                        |e: toml::de::Error| ConfigError::ActionSpec {
                            id: id.0.clone(),
                            msg: e.to_string(),
                        },
                    )?;
                }
                RawBehaviour::Condition(src) => {
                    ConditionExpr::parse(src).map_err(|m| ConfigError::Condition {
                        id: id.0.clone(),
                        msg: m,
                    })?;
                }
                _ => {}
            }
        }
        Ok(cfg)
    }

    /// Parse a self-contained config **structurally**, keeping `Action`/
    /// `Condition` leaves as raw source. Domain-neutral: it does not bind or
    /// validate the leaf vocabulary, so a second instantiation (e.g. a cluster
    /// manoeuvre) reuses the exact grammar and then binds its own actions and
    /// conditions via [`compile_with`](Self::compile_with).
    pub fn parse_generic(text: &str) -> Result<BtConfig, ConfigError> {
        let root: toml::Table =
            toml::from_str(text).map_err(|e| ConfigError::Toml(e.to_string()))?;

        // includes: must be absent or empty.
        if let Some(v) = root.get("includes") {
            let arr = v.as_array().ok_or_else(|| {
                ConfigError::Toml("`includes` must be an array of strings".to_string())
            })?;
            if !arr.is_empty() {
                let names = arr
                    .iter()
                    .map(|x| x.as_str().unwrap_or("?").to_string())
                    .collect();
                return Err(ConfigError::UnresolvedIncludes(names));
            }
        }

        let run = match root.get("run") {
            None => None,
            Some(v) => Some(parse_run(v)?),
        };

        let mut env = BTreeMap::new();
        if let Some(toml::Value::Table(t)) = root.get("env") {
            flatten_env("", t, &mut env)?;
        }

        let mut behaviours = BTreeMap::new();
        if let Some(toml::Value::Table(t)) = root.get("behaviours") {
            walk_behaviours("", t, &mut behaviours)?;
        }

        let mut metadata = BTreeMap::new();
        if let Some(toml::Value::Table(t)) = root.get("metadata") {
            for (owner, v) in t {
                let revision = v
                    .as_table()
                    .and_then(|mt| mt.get("revision"))
                    .and_then(toml::Value::as_integer)
                    .unwrap_or(0)
                    .max(0) as u32;
                metadata.insert(owner.clone(), ModuleMeta { revision });
            }
        }

        Ok(BtConfig {
            run,
            env,
            behaviours,
            metadata,
            includes: Vec::new(),
        })
    }

    /// Enforce every validation rule (spec FR-013): one `[run]`; root defined;
    /// children resolve; no cycles; arity; consensus condition refs resolve and
    /// type-check. The structural rules are domain-neutral
    /// ([`validate_structure`](Self::validate_structure)); the condition typing
    /// is consensus-specific (`env.*`/`cardano.*`).
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.validate_structure()?;
        for (id, raw) in &self.behaviours {
            if let RawBehaviour::Condition(src) = raw {
                let expr = ConditionExpr::parse(src).map_err(|m| ConfigError::Condition {
                    id: id.0.clone(),
                    msg: m,
                })?;
                expr.validate(&self.env)
                    .map_err(|m| ConfigError::Condition {
                        id: id.0.clone(),
                        msg: m,
                    })?;
            }
        }
        Ok(())
    }

    /// Domain-neutral structural validation shared by every instantiation: one
    /// `[run]`; root defined; children resolve; composite/decorator arity; no
    /// reference cycles. Leaf-vocabulary binding (conditions, actions) is left
    /// to the domain binders in [`compile_with`](Self::compile_with).
    pub fn validate_structure(&self) -> Result<(), ConfigError> {
        let run = self
            .run
            .as_ref()
            .ok_or_else(|| ConfigError::Run("missing [run] (name, seed, root)".to_string()))?;

        if !self.behaviours.contains_key(&run.root) {
            return Err(ConfigError::UnknownRoot(run.root.0.clone()));
        }

        for (id, raw) in &self.behaviours {
            match raw {
                RawBehaviour::Selector(ch)
                | RawBehaviour::Sequence(ch)
                | RawBehaviour::Join(ch) => {
                    if ch.is_empty() {
                        return Err(ConfigError::BadArity {
                            id: id.0.clone(),
                            msg: "composite needs at least one child".to_string(),
                        });
                    }
                    for c in ch {
                        self.require_defined(id, c)?;
                    }
                }
                RawBehaviour::ForTicks { count, child } => {
                    if *count < 1 {
                        return Err(ConfigError::BadArity {
                            id: id.0.clone(),
                            msg: "ForTicks count must be >= 1".to_string(),
                        });
                    }
                    self.require_defined(id, child)?;
                }
                // `Wait`'s count is structural (domain-neutral), so it is checked
                // here rather than in a binder.
                RawBehaviour::Wait { count } => {
                    if *count < 1 {
                        return Err(ConfigError::BadArity {
                            id: id.0.clone(),
                            msg: "Wait count must be >= 1".to_string(),
                        });
                    }
                }
                // Condition/Action references are validated by the domain binder
                // (consensus does it eagerly in `validate`), not structurally.
                RawBehaviour::Condition(_) | RawBehaviour::Honest | RawBehaviour::Action { .. } => {
                }
            }
        }

        // Cycle detection from the root (references expand, so a cycle would be
        // infinite). `on_path` tracks the current DFS stack.
        let mut on_path: Vec<BehaviourId> = Vec::new();
        self.check_acyclic(&run.root, &mut on_path)?;
        Ok(())
    }

    /// Validate, then expand the references from `root` into an owned
    /// consensus [`BehaviourTree`] (each reference becomes an independent
    /// instance, with its own node-local state and a deterministic per-instance
    /// action seed).
    ///
    /// A thin wrapper over [`compile_with`](Self::compile_with): it validates
    /// (structural + consensus condition typing) and binds the consensus leaf
    /// vocabulary — `build_action` for actions, [`ConditionExpr`] for
    /// conditions — so the consensus behaviour is unchanged.
    pub fn compile(&self) -> Result<BehaviourTree, ConfigError> {
        self.validate()?;
        self.compile_with(
            |_kind, params, seed| {
                let spec: ActionSpec = toml::Value::Table(params.clone()).try_into().map_err(
                    |e: toml::de::Error| ConfigError::ActionSpec {
                        id: String::new(),
                        msg: e.to_string(),
                    },
                )?;
                Ok(build_action(&spec, seed))
            },
            |src| {
                let expr = ConditionExpr::parse(src).map_err(|m| ConfigError::Condition {
                    id: String::new(),
                    msg: m,
                })?;
                Ok(Box::new(expr) as Box<dyn Condition<ConsensusCtx>>)
            },
        )
    }

    /// Validate the structure, then expand the references from `root` into an
    /// owned [`BehaviourTree<C, E>`], binding each `Action`/`Condition` leaf
    /// through the supplied binders. This is the generic compile entry that lets
    /// a second instantiation reuse the whole TOML grammar and tree walker
    /// (FR-001) while supplying its own leaf vocabulary:
    ///
    /// - `action_binder(kind, params, seed)` turns a raw action `spec` (its
    ///   `kind` string and full params table) plus a deterministic per-instance
    ///   `seed` into a boxed [`LeafAction<C, E>`];
    /// - `condition_binder(expr_src)` turns a raw condition expression into a
    ///   boxed [`Condition<C>`] (it may parse via [`ConditionExpr`] and bind its
    ///   own [`ValueResolver`](super::condition::ValueResolver)).
    ///
    /// A binder error is tagged with the offending behaviour id. Structural
    /// validation (root/children/cycles/arity) is domain-neutral and always
    /// runs first.
    pub fn compile_with<C, E, FA, FC>(
        &self,
        action_binder: FA,
        condition_binder: FC,
    ) -> Result<BehaviourTree<C, E>, ConfigError>
    where
        C: TreeContext,
        FA: Fn(&str, &toml::Table, u64) -> Result<Box<dyn LeafAction<C, E>>, ConfigError>,
        FC: Fn(&str) -> Result<Box<dyn Condition<C>>, ConfigError>,
    {
        self.validate_structure()?;
        let run = self
            .run
            .as_ref()
            .ok_or_else(|| ConfigError::Run("missing [run]".to_string()))?;
        let mut on_path: Vec<BehaviourId> = Vec::new();
        let mut action_counter: usize = 0;
        let root = self.expand_with(
            &run.root,
            run.seed,
            &mut on_path,
            &mut action_counter,
            &action_binder,
            &condition_binder,
        )?;
        Ok(BehaviourTree::new(run.name.clone(), run.seed, root))
    }

    /// Convenience: parse and compile in one call.
    pub fn compile_str(text: &str) -> Result<BehaviourTree, ConfigError> {
        BtConfig::parse(text)?.compile()
    }

    fn require_defined(
        &self,
        parent: &BehaviourId,
        child: &BehaviourId,
    ) -> Result<(), ConfigError> {
        if self.behaviours.contains_key(child) {
            Ok(())
        } else {
            Err(ConfigError::DanglingChild {
                parent: parent.0.clone(),
                child: child.0.clone(),
            })
        }
    }

    fn children_of<'a>(&'a self, id: &BehaviourId) -> Vec<&'a BehaviourId> {
        match self.behaviours.get(id) {
            Some(RawBehaviour::Selector(ch))
            | Some(RawBehaviour::Sequence(ch))
            | Some(RawBehaviour::Join(ch)) => ch.iter().collect(),
            Some(RawBehaviour::ForTicks { child, .. }) => vec![child],
            _ => Vec::new(),
        }
    }

    fn check_acyclic(
        &self,
        id: &BehaviourId,
        on_path: &mut Vec<BehaviourId>,
    ) -> Result<(), ConfigError> {
        if on_path.contains(id) {
            let mut path: Vec<String> = on_path.iter().map(|b| b.0.clone()).collect();
            path.push(id.0.clone());
            return Err(ConfigError::Cycle(path));
        }
        // An undefined child is reported by `require_defined`; skip here.
        if !self.behaviours.contains_key(id) {
            return Ok(());
        }
        on_path.push(id.clone());
        for child in self.children_of(id) {
            self.check_acyclic(child, on_path)?;
        }
        on_path.pop();
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn expand_with<C, E, FA, FC>(
        &self,
        id: &BehaviourId,
        seed: u64,
        on_path: &mut Vec<BehaviourId>,
        action_counter: &mut usize,
        action_binder: &FA,
        condition_binder: &FC,
    ) -> Result<Behaviour<C, E>, ConfigError>
    where
        C: TreeContext,
        FA: Fn(&str, &toml::Table, u64) -> Result<Box<dyn LeafAction<C, E>>, ConfigError>,
        FC: Fn(&str) -> Result<Box<dyn Condition<C>>, ConfigError>,
    {
        if on_path.contains(id) {
            let mut path: Vec<String> = on_path.iter().map(|b| b.0.clone()).collect();
            path.push(id.0.clone());
            return Err(ConfigError::Cycle(path));
        }
        let raw = self
            .behaviours
            .get(id)
            .ok_or_else(|| ConfigError::DanglingChild {
                parent: on_path.last().map(|b| b.0.clone()).unwrap_or_default(),
                child: id.0.clone(),
            })?;
        on_path.push(id.clone());

        let kind = match raw {
            RawBehaviour::Selector(ch) => BehaviourKind::Selector(self.expand_all(
                ch,
                seed,
                on_path,
                action_counter,
                action_binder,
                condition_binder,
            )?),
            RawBehaviour::Sequence(ch) => BehaviourKind::Sequence(self.expand_all(
                ch,
                seed,
                on_path,
                action_counter,
                action_binder,
                condition_binder,
            )?),
            RawBehaviour::Join(ch) => BehaviourKind::join(self.expand_all(
                ch,
                seed,
                on_path,
                action_counter,
                action_binder,
                condition_binder,
            )?),
            RawBehaviour::ForTicks { count, child } => {
                let c = self.expand_with(
                    child,
                    seed,
                    on_path,
                    action_counter,
                    action_binder,
                    condition_binder,
                )?;
                BehaviourKind::for_ticks(*count, c)
            }
            // Domain-neutral: no binder needed.
            RawBehaviour::Wait { count } => BehaviourKind::wait(*count),
            RawBehaviour::Condition(src) => {
                let cond = condition_binder(src).map_err(|e| attach_id(e, id))?;
                BehaviourKind::Condition(cond)
            }
            RawBehaviour::Honest => BehaviourKind::Action(Box::new(HonestAction)),
            RawBehaviour::Action { kind, params } => {
                let action_seed = child_seed(seed, *action_counter);
                *action_counter += 1;
                let action =
                    action_binder(kind, params, action_seed).map_err(|e| attach_id(e, id))?;
                BehaviourKind::Action(action)
            }
        };

        on_path.pop();
        Ok(Behaviour::new(id.clone(), kind))
    }

    #[allow(clippy::too_many_arguments)]
    fn expand_all<C, E, FA, FC>(
        &self,
        ids: &[BehaviourId],
        seed: u64,
        on_path: &mut Vec<BehaviourId>,
        action_counter: &mut usize,
        action_binder: &FA,
        condition_binder: &FC,
    ) -> Result<Vec<Behaviour<C, E>>, ConfigError>
    where
        C: TreeContext,
        FA: Fn(&str, &toml::Table, u64) -> Result<Box<dyn LeafAction<C, E>>, ConfigError>,
        FC: Fn(&str) -> Result<Box<dyn Condition<C>>, ConfigError>,
    {
        ids.iter()
            .map(|c| {
                self.expand_with(
                    c,
                    seed,
                    on_path,
                    action_counter,
                    action_binder,
                    condition_binder,
                )
            })
            .collect()
    }
}

/// Tag a binder error with the offending behaviour id when the binder left the
/// id blank (binders receive only the raw source, not the id).
fn attach_id(e: ConfigError, id: &BehaviourId) -> ConfigError {
    match e {
        ConfigError::Condition { id: ref i, msg } if i.is_empty() => ConfigError::Condition {
            id: id.0.clone(),
            msg,
        },
        ConfigError::ActionSpec { id: ref i, msg } if i.is_empty() => ConfigError::ActionSpec {
            id: id.0.clone(),
            msg,
        },
        ConfigError::BadField { id: ref i, msg } if i.is_empty() => ConfigError::BadField {
            id: id.0.clone(),
            msg,
        },
        other => other,
    }
}

fn parse_run(v: &toml::Value) -> Result<Run, ConfigError> {
    let t = v
        .as_table()
        .ok_or_else(|| ConfigError::Run("[run] must be a table".to_string()))?;
    let name = t
        .get("name")
        .and_then(toml::Value::as_str)
        .ok_or_else(|| ConfigError::Run("missing string `name`".to_string()))?
        .to_string();
    let seed_i = t
        .get("seed")
        .and_then(toml::Value::as_integer)
        .ok_or_else(|| ConfigError::Run("missing integer `seed`".to_string()))?;
    if seed_i < 0 {
        return Err(ConfigError::Run("`seed` must be non-negative".to_string()));
    }
    let root = t
        .get("root")
        .and_then(toml::Value::as_str)
        .ok_or_else(|| ConfigError::Run("missing string `root`".to_string()))?
        .to_string();
    Ok(Run {
        name,
        seed: seed_i as u64,
        root: BehaviourId(root),
    })
}

fn flatten_env(
    prefix: &str,
    table: &toml::Table,
    out: &mut BTreeMap<String, EnvValue>,
) -> Result<(), ConfigError> {
    for (k, v) in table {
        let key = if prefix.is_empty() {
            k.clone()
        } else {
            format!("{prefix}.{k}")
        };
        let value = match v {
            toml::Value::Integer(i) if *i >= 0 => EnvValue::U64(*i as u64),
            toml::Value::Integer(i) => EnvValue::F64(*i as f64),
            toml::Value::Float(f) => EnvValue::F64(*f),
            toml::Value::String(s) => EnvValue::Str(s.clone()),
            toml::Value::Boolean(b) => EnvValue::Bool(*b),
            toml::Value::Table(t) => {
                flatten_env(&key, t, out)?;
                continue;
            }
            other => {
                return Err(ConfigError::Env {
                    key,
                    msg: format!("unsupported env value type: {}", other.type_str()),
                });
            }
        };
        out.insert(key, value);
    }
    Ok(())
}

fn walk_behaviours(
    prefix: &str,
    table: &toml::Table,
    out: &mut BTreeMap<BehaviourId, RawBehaviour>,
) -> Result<(), ConfigError> {
    for (k, v) in table {
        let id = if prefix.is_empty() {
            k.clone()
        } else {
            format!("{prefix}.{k}")
        };
        let t = v.as_table().ok_or_else(|| ConfigError::BadField {
            id: id.clone(),
            msg: "behaviour entry must be a table".to_string(),
        })?;
        if t.contains_key("type") {
            out.insert(BehaviourId(id.clone()), parse_behaviour(&id, t)?);
        } else {
            // A namespace table — recurse for dotted ids.
            walk_behaviours(&id, t, out)?;
        }
    }
    Ok(())
}

fn parse_behaviour(id: &str, t: &toml::Table) -> Result<RawBehaviour, ConfigError> {
    let ty = t
        .get("type")
        .and_then(toml::Value::as_str)
        .ok_or_else(|| ConfigError::BadField {
            id: id.to_string(),
            msg: "missing string `type`".to_string(),
        })?;

    match ty {
        "Selector" => Ok(RawBehaviour::Selector(children(id, t)?)),
        "Sequence" => Ok(RawBehaviour::Sequence(children(id, t)?)),
        "Join" => Ok(RawBehaviour::Join(children(id, t)?)),
        "ForTicks" => {
            let count_i = t
                .get("count")
                .and_then(toml::Value::as_integer)
                .ok_or_else(|| ConfigError::BadField {
                    id: id.to_string(),
                    msg: "ForTicks needs integer `count`".to_string(),
                })?;
            if !(1..=i64::from(u32::MAX)).contains(&count_i) {
                return Err(ConfigError::BadArity {
                    id: id.to_string(),
                    msg: format!("ForTicks count {count_i} out of range 1..=u32::MAX"),
                });
            }
            let child = t
                .get("child")
                .and_then(toml::Value::as_str)
                .ok_or_else(|| ConfigError::BadField {
                    id: id.to_string(),
                    msg: "ForTicks needs string `child`".to_string(),
                })?;
            Ok(RawBehaviour::ForTicks {
                count: count_i as u32,
                child: BehaviourId(child.to_string()),
            })
        }
        "Wait" => {
            let count_i = t
                .get("count")
                .and_then(toml::Value::as_integer)
                .ok_or_else(|| ConfigError::BadField {
                    id: id.to_string(),
                    msg: "Wait needs integer `count`".to_string(),
                })?;
            if !(1..=i64::from(u32::MAX)).contains(&count_i) {
                return Err(ConfigError::BadArity {
                    id: id.to_string(),
                    msg: format!("Wait count {count_i} out of range 1..=u32::MAX"),
                });
            }
            Ok(RawBehaviour::Wait {
                count: count_i as u32,
            })
        }
        "Condition" => {
            let expr_str = t
                .get("expression")
                .and_then(toml::Value::as_str)
                .ok_or_else(|| ConfigError::BadField {
                    id: id.to_string(),
                    msg: "Condition needs string `expression`".to_string(),
                })?;
            // Kept as raw source; the domain condition binder parses/validates
            // it. Consensus [`parse`] eagerly re-parses to preserve the parse-
            // time syntax check.
            Ok(RawBehaviour::Condition(expr_str.to_string()))
        }
        "HonestAction" => Ok(RawBehaviour::Honest),
        "Action" => {
            let spec_v = t.get("spec").ok_or_else(|| ConfigError::BadField {
                id: id.to_string(),
                msg: "Action needs a `spec` table".to_string(),
            })?;
            let params = spec_v
                .as_table()
                .ok_or_else(|| ConfigError::BadField {
                    id: id.to_string(),
                    msg: "Action `spec` must be a table".to_string(),
                })?
                .clone();
            // Kept as raw `(kind, params)`; the domain action binder builds the
            // concrete leaf from them. `kind` itself is structural, so an absent
            // or non-string one is rejected here rather than reaching a binder as
            // an empty kind (which every binder would have to re-validate).
            let kind = params
                .get("kind")
                .and_then(toml::Value::as_str)
                .ok_or_else(|| ConfigError::BadField {
                    id: id.to_string(),
                    msg: "Action `spec` must have a string `kind`".to_string(),
                })?
                .to_string();
            Ok(RawBehaviour::Action { kind, params })
        }
        other => Err(ConfigError::UnknownType {
            id: id.to_string(),
            ty: other.to_string(),
        }),
    }
}

fn children(id: &str, t: &toml::Table) -> Result<Vec<BehaviourId>, ConfigError> {
    let arr = t
        .get("children")
        .and_then(toml::Value::as_array)
        .ok_or_else(|| ConfigError::BadField {
            id: id.to_string(),
            msg: "composite needs an array `children`".to_string(),
        })?;
    arr.iter()
        .map(|v| {
            v.as_str()
                .map(|s| BehaviourId(s.to_string()))
                .ok_or_else(|| ConfigError::BadField {
                    id: id.to_string(),
                    msg: "children must be strings".to_string(),
                })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::behaviour::tree::control::{EbSizePolicy, OutboundControl, VotePolicy};
    use crate::behaviour::tree::env::{
        DynamicEnv, NativeChainState, ParamOverrides, TickCtx, TreeContext,
    };
    use crate::behaviour::tree::Status;
    use crate::behaviour::RbProductionStrategy;
    use crate::leios::NoVoteReason;

    const GATED: &str = r#"
[run]
name = "slot-trigger equivocator"
seed = 1234567
root = "root"

[env]
trigger_slot = 345600

[behaviours.root]
type = "Selector"
children = ["attack", "honest"]

[behaviours.attack]
type = "Sequence"
children = ["cond", "equivocate"]

[behaviours.cond]
type = "Condition"
expression = "cardano.current_slot >= env.trigger_slot"

[behaviours.equivocate]
type = "Action"
spec = { kind = "rb-header-equivocator", ways = 2 }

[behaviours.honest]
type = "HonestAction"
"#;

    fn tick_at(
        tree: &mut BehaviourTree,
        slot: u64,
    ) -> (Status, super::super::control::ControlSignal) {
        let mut env = DynamicEnv::new();
        env.insert("trigger_slot", EnvValue::U64(345600));
        let state = NativeChainState {
            current_slot: slot,
            ..Default::default()
        };
        tree.tick(&TickCtx {
            env: &env,
            state: &state,
            seed: tree.seed(),
            action_params: None,
        })
    }

    #[test]
    fn parses_run_env_and_behaviours() {
        let cfg = BtConfig::parse(GATED).unwrap();
        let run = cfg.run.as_ref().unwrap();
        assert_eq!(run.name, "slot-trigger equivocator");
        assert_eq!(run.seed, 1234567);
        assert_eq!(run.root, BehaviourId("root".into()));
        assert_eq!(cfg.env.get("trigger_slot"), Some(&EnvValue::U64(345600)));
        assert_eq!(cfg.behaviours.len(), 5);
    }

    #[test]
    fn compiles_and_switches_honest_to_adversarial_at_trigger() {
        let mut tree = BtConfig::compile_str(GATED).unwrap();

        let (s_before, out_before) = tick_at(&mut tree, 345_599);
        assert_eq!(s_before, Status::Success, "honest before trigger");
        assert_eq!(out_before.praos.production, RbProductionStrategy::Normal);

        let (s_at, out_at) = tick_at(&mut tree, 345_600);
        assert_eq!(s_at, Status::Running, "adversarial at trigger");
        assert_eq!(
            out_at.praos.production,
            RbProductionStrategy::Equivocate { ways: 2 }
        );
        assert!(matches!(
            out_at.praos.outbound,
            OutboundControl::EquivocateRouting {
                slot: 345_600,
                ways: 2,
                ..
            }
        ));
    }

    #[test]
    fn rejects_nonempty_includes() {
        let text = r#"
includes = ["long-range-fork.bt"]
[run]
name = "x"
seed = 1
root = "honest"
[behaviours.honest]
type = "HonestAction"
"#;
        match BtConfig::parse(text) {
            Err(ConfigError::UnresolvedIncludes(xs)) => {
                assert_eq!(xs, vec!["long-range-fork.bt".to_string()]);
            }
            other => panic!("expected UnresolvedIncludes, got {other:?}"),
        }
    }

    #[test]
    fn empty_includes_is_accepted() {
        let text = r#"
includes = []
[run]
name = "x"
seed = 1
root = "honest"
[behaviours.honest]
type = "HonestAction"
"#;
        assert!(BtConfig::compile_str(text).is_ok());
    }

    #[test]
    fn missing_run_is_rejected() {
        let text = r#"
[behaviours.honest]
type = "HonestAction"
"#;
        let cfg = BtConfig::parse(text).unwrap();
        assert!(matches!(cfg.validate(), Err(ConfigError::Run(_))));
    }

    #[test]
    fn unknown_root_is_rejected() {
        let text = r#"
[run]
name = "x"
seed = 1
root = "nope"
[behaviours.honest]
type = "HonestAction"
"#;
        match BtConfig::compile_str(text) {
            Err(ConfigError::UnknownRoot(r)) => assert_eq!(r, "nope"),
            other => panic!("expected UnknownRoot, got {other:?}"),
        }
    }

    #[test]
    fn dangling_child_is_rejected() {
        let text = r#"
[run]
name = "x"
seed = 1
root = "root"
[behaviours.root]
type = "Sequence"
children = ["ghost"]
"#;
        match BtConfig::compile_str(text) {
            Err(ConfigError::DanglingChild { parent, child }) => {
                assert_eq!(parent, "root");
                assert_eq!(child, "ghost");
            }
            other => panic!("expected DanglingChild, got {other:?}"),
        }
    }

    #[test]
    fn reference_cycle_is_rejected() {
        let text = r#"
[run]
name = "x"
seed = 1
root = "a"
[behaviours.a]
type = "Sequence"
children = ["b"]
[behaviours.b]
type = "Sequence"
children = ["a"]
"#;
        match BtConfig::compile_str(text) {
            Err(ConfigError::Cycle(path)) => {
                assert!(path.contains(&"a".to_string()) && path.contains(&"b".to_string()));
            }
            other => panic!("expected Cycle, got {other:?}"),
        }
    }

    #[test]
    fn unknown_behaviour_type_is_rejected() {
        let text = r#"
[run]
name = "x"
seed = 1
root = "root"
[behaviours.root]
type = "Wat"
"#;
        match BtConfig::parse(text) {
            Err(ConfigError::UnknownType { id, ty }) => {
                assert_eq!(id, "root");
                assert_eq!(ty, "Wat");
            }
            other => panic!("expected UnknownType, got {other:?}"),
        }
    }

    #[test]
    fn empty_composite_is_rejected() {
        let text = r#"
[run]
name = "x"
seed = 1
root = "root"
[behaviours.root]
type = "Selector"
children = []
"#;
        match BtConfig::compile_str(text) {
            Err(ConfigError::BadArity { id, .. }) => assert_eq!(id, "root"),
            other => panic!("expected BadArity, got {other:?}"),
        }
    }

    #[test]
    fn wait_stages_a_sequence_delay() {
        // `Sequence[ Wait(2), <attack> ]` compiles and yields the relative-delay
        // shape: honest (no perturbation) for 2 ticks, then the attack.
        let text = r#"
[run]
name = "staged"
seed = 1
root = "s"
[behaviours.s]
type = "Sequence"
children = ["w", "atk"]
[behaviours.w]
type = "Wait"
count = 2
[behaviours.atk]
type = "Action"
spec = { kind = "cert-suppressor" }
"#;
        let mut tree = BtConfig::compile_str(text).unwrap();
        let env = crate::behaviour::tree::env::DynamicEnv::new();
        let state = crate::behaviour::tree::env::NativeChainState::default();
        let ctx = crate::behaviour::tree::env::TickCtx {
            env: &env,
            state: &state,
            seed: 0,
            action_params: None,
        };
        // Wait(2): 2 full slots elapse (attack inactive), then it activates.
        assert!(!tree.tick(&ctx).1.praos.suppress_cert);
        assert!(!tree.tick(&ctx).1.praos.suppress_cert);
        // Tick 3: wait done → attack active.
        assert!(tree.tick(&ctx).1.praos.suppress_cert);
    }

    #[test]
    fn wait_count_zero_is_rejected() {
        let text = r#"
[run]
name = "x"
seed = 1
root = "w"
[behaviours.w]
type = "Wait"
count = 0
"#;
        match BtConfig::compile_str(text) {
            Err(ConfigError::BadArity { msg, .. }) => {
                assert!(msg.contains("Wait count"), "{msg}")
            }
            other => panic!("expected BadArity, got {other:?}"),
        }
    }

    #[test]
    fn condition_with_undefined_env_is_rejected() {
        let text = r#"
[run]
name = "x"
seed = 1
root = "c"
[behaviours.c]
type = "Condition"
expression = "cardano.current_slot >= env.nope"
"#;
        match BtConfig::compile_str(text) {
            Err(ConfigError::Condition { id, msg }) => {
                assert_eq!(id, "c");
                assert!(msg.contains("undefined env reference"), "{msg}");
            }
            other => panic!("expected Condition error, got {other:?}"),
        }
    }

    #[test]
    fn condition_type_mismatch_is_rejected() {
        let text = r#"
[run]
name = "x"
seed = 1
root = "c"
[env]
label = "pool-a"
[behaviours.c]
type = "Condition"
expression = "cardano.current_slot >= env.label"
"#;
        match BtConfig::compile_str(text) {
            Err(ConfigError::Condition { msg, .. }) => {
                assert!(msg.contains("type mismatch"), "{msg}")
            }
            other => panic!("expected Condition type mismatch, got {other:?}"),
        }
    }

    #[test]
    fn bad_action_spec_is_rejected() {
        let text = r#"
[run]
name = "x"
seed = 1
root = "a"
[behaviours.a]
type = "Action"
spec = { kind = "not-a-real-action" }
"#;
        assert!(matches!(
            BtConfig::parse(text),
            Err(ConfigError::ActionSpec { .. })
        ));
    }

    #[test]
    fn dotted_owner_namespaced_ids_and_env() {
        let text = r#"
[run]
name = "x"
seed = 1
root = "root"
[env.network_shape]
packet_delay = 20
[behaviours.root]
type = "Sequence"
children = ["network_shape.shape"]
[behaviours.network_shape.shape]
type = "HonestAction"
"#;
        let cfg = BtConfig::parse(text).unwrap();
        assert!(cfg
            .behaviours
            .contains_key(&BehaviourId("network_shape.shape".into())));
        assert_eq!(
            cfg.env.get("network_shape.packet_delay"),
            Some(&EnvValue::U64(20))
        );
        assert!(cfg.compile().is_ok());
    }

    #[test]
    fn references_expand_to_independent_instances() {
        // Two sites reference the same lazy-voter def; each must be its own
        // instance (a Join ticks both every tick).
        let text = r#"
[run]
name = "dup"
seed = 1
root = "root"
[behaviours.root]
type = "Join"
children = ["lazy", "lazy"]
[behaviours.lazy]
type = "Action"
spec = { kind = "lazy-voter" }
"#;
        let tree = BtConfig::compile_str(text).unwrap();
        // The compiled Join must own two distinct child instances.
        // (Structural check via Debug: two "lazy" nodes under the join.)
        let dbg = format!("{tree:?}");
        assert_eq!(
            dbg.matches("LazyVoter").count(),
            2,
            "expected two instances"
        );
    }

    #[test]
    fn duplex_follower_join_composes_two_leios_actions() {
        let text = r#"
[run]
name = "duplex-follower-bug"
seed = 1
root = "root"
[behaviours.root]
type = "Join"
children = ["echo", "lie"]
[behaviours.echo]
type = "Action"
spec = { kind = "echo-to-source" }
[behaviours.lie]
type = "Action"
spec = { kind = "lie-about-eb-size", scale_num = 0, scale_den = 1, offset = 0 }
"#;
        let mut tree = BtConfig::compile_str(text).unwrap();
        let env = DynamicEnv::new();
        let state = NativeChainState::default();
        let (s, out) = tree.tick(&TickCtx {
            env: &env,
            state: &state,
            seed: 1,
            action_params: None,
        });
        assert_eq!(s, Status::Running);
        assert!(out.leios.echo_to_source);
        assert_eq!(
            out.leios.offer_eb_size,
            EbSizePolicy::Linear {
                scale_num: 0,
                scale_den: 1,
                offset: 0
            }
        );
    }

    #[test]
    fn lazy_voter_default_reason_parses() {
        let text = r#"
[run]
name = "x"
seed = 1
root = "a"
[behaviours.a]
type = "Action"
spec = { kind = "lazy-voter" }
"#;
        let mut tree = BtConfig::compile_str(text).unwrap();
        let env = DynamicEnv::new();
        let state = NativeChainState::default();
        let (_, out) = tree.tick(&TickCtx {
            env: &env,
            state: &state,
            seed: 1,
            action_params: None,
        });
        assert_eq!(out.leios.vote, VotePolicy::Abstain(NoVoteReason::Declined));
    }

    // ---- Determinism (T018) ----

    #[test]
    fn same_config_and_seed_yield_identical_signal_sequence() {
        let mut a = BtConfig::compile_str(GATED).unwrap();
        let mut b = BtConfig::compile_str(GATED).unwrap();
        for slot in [345_598, 345_599, 345_600, 345_601, 345_602] {
            let ra = tick_at(&mut a, slot);
            let rb = tick_at(&mut b, slot);
            assert_eq!(ra.0, rb.0, "status diverged at slot {slot}");
            assert_eq!(ra.1, rb.1, "control signal diverged at slot {slot}");
        }
    }

    // ---- Generic entrypoints: a non-consensus instantiation ----
    //
    // A minimal second instantiation proves `parse_generic` + `compile_with`
    // are usable without any consensus type: `Effect` is a counter, the
    // context view carries one number the condition compares against, and the
    // binders understand a single leaf kind and a single condition form.

    /// The toy instantiation's per-tick view: one readable number.
    #[derive(Debug)]
    struct ToyView(u64);

    impl ParamOverrides for ToyView {
        fn action_overrides(&self, _behaviour_id: &str) -> Vec<(String, toml::Value)> {
            Vec::new()
        }
    }

    /// The toy instantiation marker.
    #[derive(Debug)]
    enum ToyCtx {}

    impl TreeContext for ToyCtx {
        type View<'a> = ToyView;
    }

    /// Leaf that bumps the effect counter while active.
    #[derive(Debug)]
    struct Bump(u64);

    impl LeafAction<ToyCtx, u64> for Bump {
        fn contribute(&mut self, _ctx: &ToyView, out: &mut u64) -> Status {
            *out += self.0;
            Status::Running
        }
    }

    /// Condition `value >= n`.
    #[derive(Debug)]
    struct AtLeast(u64);

    impl Condition<ToyCtx> for AtLeast {
        fn eval(&self, ctx: &ToyView) -> bool {
            ctx.0 >= self.0
        }
    }

    fn toy_action(
        kind: &str,
        params: &toml::Table,
        _seed: u64,
    ) -> Result<Box<dyn LeafAction<ToyCtx, u64>>, ConfigError> {
        match kind {
            "bump" => {
                let by = params
                    .get("by")
                    .and_then(toml::Value::as_integer)
                    .unwrap_or(1);
                Ok(Box::new(Bump(by as u64)))
            }
            other => Err(ConfigError::ActionSpec {
                id: String::new(), // left blank: `attach_id` fills the behaviour id
                msg: format!("unknown toy kind {other:?}"),
            }),
        }
    }

    fn toy_condition(src: &str) -> Result<Box<dyn Condition<ToyCtx>>, ConfigError> {
        // Only `value >= <int>` is understood.
        match src
            .strip_prefix("value >= ")
            .and_then(|n| n.trim().parse().ok())
        {
            Some(n) => Ok(Box::new(AtLeast(n))),
            None => Err(ConfigError::Condition {
                id: String::new(), // left blank: `attach_id` fills the behaviour id
                msg: format!("unsupported toy condition {src:?}"),
            }),
        }
    }

    const TOY: &str = r#"
[run]
name = "toy"
seed = 9
root = "root"
[behaviours.root]
type = "Selector"
children = ["gate", "work"]
[behaviours.gate]
type = "Condition"
expression = "value >= 10"
[behaviours.work]
type = "Action"
spec = { kind = "bump", by = 3 }
"#;

    #[test]
    fn compile_with_builds_a_non_consensus_instantiation() {
        let cfg = BtConfig::parse_generic(TOY).unwrap();
        let mut tree = cfg.compile_with(toy_action, toy_condition).unwrap();

        // Gate false (value < 10) → the selector falls through to the leaf,
        // which bumps the effect by its `by` param.
        let (status, out) = tree.tick(&ToyView(0));
        assert_eq!(status, Status::Running);
        assert_eq!(out, 3);

        // Gate true → the condition succeeds and the leaf never contributes.
        let (status, out) = tree.tick(&ToyView(10));
        assert_eq!(status, Status::Success);
        assert_eq!(out, 0);
    }

    #[test]
    fn compile_with_tags_binder_errors_with_the_behaviour_id() {
        // An action kind the toy binder doesn't know: the binder returns a
        // blank-id error and `attach_id` fills in the offending behaviour.
        let text = r#"
[run]
name = "toy"
seed = 9
root = "root"
[behaviours.root]
type = "Action"
spec = { kind = "nope" }
"#;
        let cfg = BtConfig::parse_generic(text).unwrap();
        let err = cfg.compile_with(toy_action, toy_condition).unwrap_err();
        match err {
            ConfigError::ActionSpec { id, msg } => {
                assert_eq!(id, "root", "binder error must carry the behaviour id");
                assert!(msg.contains("nope"), "msg should name the kind: {msg}");
            }
            other => panic!("expected ActionSpec, got {other:?}"),
        }

        // Same for a condition the toy binder can't parse.
        let text = r#"
[run]
name = "toy"
seed = 9
root = "root"
[behaviours.root]
type = "Condition"
expression = "cardano.current_slot >= 1"
"#;
        let cfg = BtConfig::parse_generic(text).unwrap();
        let err = cfg.compile_with(toy_action, toy_condition).unwrap_err();
        match err {
            ConfigError::Condition { id, .. } => assert_eq!(id, "root"),
            other => panic!("expected Condition, got {other:?}"),
        }
    }

    #[test]
    fn parse_generic_is_structural_where_consensus_parse_is_eager() {
        // A leaf kind that means nothing to consensus: `parse_generic` accepts
        // it (structure is fine; the binder decides), while the consensus
        // `parse` rejects it eagerly.
        assert!(BtConfig::parse_generic(TOY).is_ok());
        assert!(BtConfig::parse(TOY).is_err());

        // Structural validation still applies to both: an undefined child is a
        // hard error regardless of instantiation.
        let missing_child = r#"
[run]
name = "toy"
seed = 9
root = "root"
[behaviours.root]
type = "Sequence"
children = ["ghost"]
"#;
        let cfg = BtConfig::parse_generic(missing_child).unwrap();
        assert!(cfg.validate_structure().is_err());
        assert!(cfg.compile_with(toy_action, toy_condition).is_err());
    }

    #[test]
    fn action_spec_without_a_string_kind_is_a_structural_error() {
        let text = r#"
[run]
name = "toy"
seed = 9
root = "root"
[behaviours.root]
type = "Action"
spec = { by = 3 }
"#;
        match BtConfig::parse_generic(text) {
            Err(ConfigError::BadField { id, msg }) => {
                assert_eq!(id, "root");
                assert!(msg.contains("kind"), "msg should mention `kind`: {msg}");
            }
            other => panic!("expected BadField for the missing kind, got {other:?}"),
        }
    }
}
