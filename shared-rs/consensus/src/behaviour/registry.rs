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
    #[serde(rename = "withhold-txs")]
    WithholdTxs {
        withholding_slots: u64,
        tx_producer_only: bool,
        /// Whether to withhold txs on the tx-submission protocol.
        #[serde(default = "default_true")]
        withhold_tx_submission: bool,
        /// Whether to withhold txs on the Leios fetch protocol.
        #[serde(default)]
        withhold_leios_fetch: bool,
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
    /// Hollow EB — empty manifest announced with a false declared size.
    /// Probes whether the receiver gates/pre-allocates on the advertised
    /// `eb_size` before fetching the (empty) body.
    #[serde(rename = "hollow-eb")]
    HollowEb {
        #[serde(default = "default_hollow_bytes")]
        declared_bytes: u64,
    },
    /// Loaded Tx EB — fabricated EB whose manifest is sourced from the node's
    /// configured attack magazine (`eb_magazine_path`): `take` real, offline-
    /// authored txs (e.g. double-spends or theft txs) pinned + served like
    /// Dummy. The magazine decides the payload; the node stays threat-agnostic.
    #[serde(rename = "loaded-tx-eb")]
    LoadedTxEb {
        #[serde(default = "default_fake_eb_txs")]
        take: u32,
    },
    #[serde(rename = "announce-size-lie")]
    AnnounceSizeLie {
        #[serde(default = "default_lie_scale")]
        scale_num: u32,
        #[serde(default = "default_lie_scale")]
        scale_den: u32,
        #[serde(default)]
        offset: i32,
    },
    #[serde(rename = "announce-dangling")]
    AnnounceDangling,
    #[serde(rename = "announce-equivocate")]
    AnnounceEquivocate,
    #[serde(rename = "fake-eb-announce")]
    FakeAnnounce,
    /// Fabricate an announcement with a skewed header slot (future / far past).
    #[serde(rename = "announce-slot-skew")]
    AnnounceSlotSkew {
        #[serde(default)]
        slot_offset: i64,
    },
    /// Flood the network with `count` fabricated announcements per tick.
    #[serde(rename = "announce-flood")]
    AnnounceFlood {
        #[serde(default = "default_flood_count")]
        count: u32,
        #[serde(default)]
        slot_offset: i64,
    },
    /// vote-flood — emit `count` votes over fabricated distinct RB hashes per
    /// tick, bypassing the election gate (unbounded seenVotes/pointStates probe).
    #[serde(rename = "vote-flood")]
    VoteFlood {
        #[serde(default = "default_flood_count")]
        count: u32,
    },
    #[serde(rename = "withhold-eb-block-announce")]
    WithholdEbAnnounce,
    /// Flood the local tx generator at `rate` txs/sec (garbage or magazine).
    #[serde(rename = "tx-flood")]
    TxFlood {
        #[serde(default = "default_tx_flood_rate")]
        rate: u32,
    },
}

fn default_fake_eb_txs() -> u32 {
    8
}

fn default_true() -> bool {
    true
}

fn default_hollow_bytes() -> u64 {
    100_000
}

fn default_tx_flood_rate() -> u32 {
    1000
}

fn default_flood_count() -> u32 {
    100
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
