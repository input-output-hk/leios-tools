//! Adversarial / experimental node behaviour.
//!
//! Behaviour is driven by the **behaviour-tree engine** in [`tree`]: a tree is
//! ticked once per slot and emits a [`tree::ControlSignal`] that the consensus
//! state machines (`LeiosState`/`PraosState`/`MempoolState`) apply via
//! `apply_control`, and that the I/O actuators read. Leaf actions live in
//! [`actions`]; their serialisable kinds are in the [`registry`] ([`ActionSpec`]);
//! [`selection`] assigns a behaviour to a subset of nodes.
//!
//! ## Determinism
//!
//! Everything here is deterministic — sim-rs replays runs from a seed. No
//! `Instant::now()` in tick logic; randomness derives from the run seed via
//! `blake2b_simd` ([`registry::child_seed`] / [`seed_from_node_id`]).

pub mod actions;
pub mod registry;
pub mod selection;
pub mod tree;

pub use registry::{seed_from_node_id, ActionSpec};
pub use selection::{resolve_assignments, BehaviourSelection};

/// Production-time strategy for self-produced RBs, carried in
/// [`tree::control::PraosControl::production`] and read by the producer.
/// Honest nodes use [`RbProductionStrategy::Normal`].
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RbProductionStrategy {
    /// Produce one honest RB (the default).
    #[default]
    Normal,
    /// Produce no RB this slot — the lottery win is wasted. Selective
    /// withholding drops blocks without equivocating.
    Suppress,
    /// Produce `ways` RBs for the same lottery, all differing in body content.
    /// The producer signs them, adopts the first locally, and records the full
    /// set in the equivocation-variant store ([`tree::EquivocationVariants`]);
    /// the per-peer send actuator then routes a different variant to each peer
    /// bucket, so honest peers detect the equivocation (CIP-0164). `ways >= 2`.
    Equivocate { ways: u8 },
}
