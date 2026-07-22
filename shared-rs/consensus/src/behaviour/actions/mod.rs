//! The action catalogue — the shipped adversary mechanics re-homed as
//! behaviour-tree [`LeafAction`](super::tree::actions::LeafAction)s.
//!
//! Each leaf, when active this tick, writes its slice of the slot's
//! [`ControlSignal`](super::tree::control::ControlSignal) and returns a status
//! (always `Running` while active — flow gating lives in `Condition`
//! behaviours, per the gating house rule). One file per action so a contributor
//! can add one without touching the others.
//!
//! These coexist with the legacy hook-trait behaviours in
//! [`super::behaviours`] during the migration; the hook versions are removed in
//! a later phase.

pub mod announce_dangling;
pub mod announce_equivocate;
pub mod announce_size_lie;
pub mod cert_suppressor;
pub mod fake_eb;
pub mod deep_reorg;
pub mod drop_inbound;
pub mod echo_to_source;
pub mod fake_announce;
pub mod lazy_voter;
pub mod lie_about_eb_size;
pub mod rb_equivocator;
pub mod t22;

pub use announce_dangling::AnnounceDangling;
pub use announce_equivocate::AnnounceEquivocate;
pub use announce_size_lie::AnnounceSizeLie;
pub use cert_suppressor::CertSuppressor;
pub use fake_eb::{DummyTxEb, PhantomTxEb};
pub use deep_reorg::DeepReorg;
pub use drop_inbound::DropInboundPeers;
pub use echo_to_source::EchoToSource;
pub use fake_announce::FakeAnnounce;
pub use lazy_voter::LazyVoter;
pub use lie_about_eb_size::LieAboutEbSize;
pub use rb_equivocator::{equivocation_bucket, RbHeaderEquivocator};
pub use t22::T22;
