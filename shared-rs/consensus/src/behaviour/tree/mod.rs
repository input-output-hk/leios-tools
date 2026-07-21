//! Behaviour-tree engine — the single decision abstraction for adversarial
//! nodes.
//!
//! A [`BehaviourTree`] is ticked once per slot advance (the slot-driven tick).
//! The tick is the **only** place decisions are made: it evaluates
//! [`Condition`](condition::ConditionExpr)s over the read-only chain state and
//! the (guarded) env, resolves the active leaf set under the composite
//! semantics, and accumulates each active leaf's contribution into one
//! [`ControlSignal`](control::ControlSignal) value. The consensus actuators
//! later consume that `ControlSignal` mechanically — there is no second
//! decision path.
//!
//! This engine is sans-IO and deterministic (no clock reads, no `thread_rng`,
//! `BTreeMap`/`BTreeSet` in ordered paths), in keeping with the crate's
//! discipline. Grammar and operational semantics:
//! `specs/001-behavior-tree-engine/design/bt-grammar-and-semantics.md`.

pub mod actions;
pub mod behaviour;
pub mod condition;
pub mod config;
pub mod control;
pub mod env;
pub mod variants;

pub use behaviour::{Behaviour, BehaviourId, BehaviourKind, BehaviourTree};
pub use condition::{CompareOp, Condition, ConditionExpr, ValueRef};
pub use config::{BtConfig, ConfigError, ModuleMeta, Run};
pub use control::{
    ControlSignal, EbSizePolicy, LeiosControl, MempoolControl, OutboundControl, PraosControl,
    TxFilterPolicy, VotePolicy,
};
pub use env::{
    ActionParamStore, ConsensusCtx, DynamicEnv, EnvHandle, EnvValue, NativeChainState,
    ParamOverrides, TickCtx, TreeContext,
};
pub use variants::{EquivocationVariants, RbVariant};

/// The status a behaviour returns when ticked (spec FR-001).
///
/// Every behaviour, on every tick, returns exactly one of these to its parent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// The behaviour achieved its goal this tick (or its condition held).
    Success,
    /// The behaviour could not achieve its goal (or its condition did not hold).
    Failure,
    /// The behaviour has not finished; it should be ticked again next tick.
    Running,
}

#[cfg(test)]
mod tests {
    use super::Status;

    #[test]
    fn status_has_three_distinct_values() {
        assert_ne!(Status::Success, Status::Failure);
        assert_ne!(Status::Success, Status::Running);
        assert_ne!(Status::Failure, Status::Running);
    }

    #[test]
    fn status_is_copy_and_eq() {
        let s = Status::Running;
        let t = s; // Copy
        assert_eq!(s, t);
    }
}
