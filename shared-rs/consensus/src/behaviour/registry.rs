//! Action registry — the serialisable [`ActionSpec`] (leaf-action kind +
//! params) the behaviour-tree engine deserialises from config, plus the
//! deterministic seeding helpers ([`child_seed`], [`seed_from_node_id`]).

use serde::{Deserialize, Serialize};

use crate::leios::NoVoteReason;

/// Serialisable description of a behaviour-tree **leaf action** — the
/// action-kind discriminant plus its parameters. This is the action registry
/// for the BT engine ([`super::tree`]): a `[behaviours.<id>]` of `type =
/// "Action"` carries a `spec` that deserialises into one of these, and
/// [`build_action`](super::tree::actions::build_action) materialises the
/// matching [`LeafAction`](super::tree::actions::LeafAction).
///
/// Composition (honest fallback, AND/OR) is expressed by the tree structure
/// itself (`Action(honest)` / `Join` / `Sequence`), so there is no `Honest` or
/// `Composite` leaf variant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ActionSpec {
    #[serde(rename = "rb-header-equivocator")]
    RbHeaderEquivocator {
        #[serde(default = "default_equivocator_ways")]
        ways: u8,
    },
    #[serde(rename = "lazy-voter")]
    LazyVoter {
        #[serde(default = "default_lazy_reason")]
        reason: NoVoteReason,
    },
    #[serde(rename = "t22")]
    T22 {
        vote_threshold: u8,
        non_voting_threshold: u8,
        hide_eb_tx_received: bool,
    },
    #[serde(rename = "deep-reorg")]
    DeepReorg { every_slots: u64, depth: u64 },
    #[serde(rename = "drop-inbound-peers")]
    DropInboundPeers { probability: f64 },
    #[serde(rename = "lie-about-eb-size")]
    LieAboutEbSize {
        #[serde(default = "default_lie_scale")]
        scale_num: u32,
        #[serde(default = "default_lie_scale")]
        scale_den: u32,
        #[serde(default)]
        offset: i32,
    },
    #[serde(rename = "echo-to-source")]
    EchoToSource,
    #[serde(rename = "cert-suppressor")]
    CertSuppressor,
    /// Phantom Tx EB — fabricated EB of `n_txs` unfetchable phantom txs.
    #[serde(rename = "phantom-tx-eb")]
    PhantomTxEb {
        #[serde(default = "default_fake_eb_txs")]
        n_txs: u32,
    },
    /// Dummy Tx EB — fabricated EB of `n_txs` servable dummy txs.
    #[serde(rename = "dummy-tx-eb")]
    DummyTxEb {
        #[serde(default = "default_fake_eb_txs")]
        n_txs: u32,
    },
}

fn default_fake_eb_txs() -> u32 {
    8
}

fn default_lazy_reason() -> NoVoteReason {
    NoVoteReason::Declined
}

fn default_equivocator_ways() -> u8 {
    2
}

fn default_lie_scale() -> u32 {
    1
}

/// Mix `seed` with `child_index` to give each composite child a
/// distinct deterministic stream.  Uses Blake2b to avoid linear
/// correlations between sibling seeds.
pub(crate) fn child_seed(seed: u64, idx: usize) -> u64 {
    let mut h = blake2b_simd::Params::new().hash_length(8).to_state();
    h.update(&seed.to_le_bytes());
    h.update(&(idx as u64).to_le_bytes());
    let out = h.finalize();
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&out.as_bytes()[..8]);
    u64::from_le_bytes(buf)
}

/// Derive a deterministic u64 seed from a node identifier string.  Use
/// when the per-node config supplies no explicit RNG seed but the
/// behaviour still needs a stable starting point across re-runs.
pub fn seed_from_node_id(node_id: &str) -> u64 {
    let mut h = blake2b_simd::Params::new().hash_length(8).to_state();
    h.update(node_id.as_bytes());
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&h.finalize().as_bytes()[..8]);
    u64::from_le_bytes(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn action_spec_round_trips() {
        let spec = ActionSpec::RbHeaderEquivocator { ways: 2 };
        let json = serde_json::to_string(&spec).unwrap();
        let back: ActionSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ActionSpec::RbHeaderEquivocator { ways: 2 });
    }

    #[test]
    fn child_seed_distinct_per_index() {
        let s = 0xCAFEBABE;
        let a = child_seed(s, 0);
        let b = child_seed(s, 1);
        let c = child_seed(s, 2);
        assert_ne!(a, b);
        assert_ne!(b, c);
        assert_ne!(a, c);
    }

    #[test]
    fn child_seed_deterministic() {
        let s = 0xC0FFEE;
        assert_eq!(child_seed(s, 7), child_seed(s, 7));
    }
}
