//! `txflow`: declarative transaction-flow experiments.
//!
//! An experiment spec ([`spec`]) expands into a version- and network-free
//! graph IR ([`ir`], [`expand`]). Relations such as conflict sets are derived
//! from inputs ([`derive`]). [`materialise`] binds the IR to a chain context
//! ([`context`]) and builds and signs every transaction offline.
//!
//! See `specs/002-tx-flow-experiments/` for the design.

pub mod context;
pub mod derive;
pub mod error;
pub mod expand;
pub mod expr;
pub mod ir;
pub mod materialise;
pub mod patterns;
pub mod spec;
