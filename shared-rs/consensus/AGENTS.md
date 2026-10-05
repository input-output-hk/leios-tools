# Agent Instructions for shared-consensus

These instructions apply to this crate and its descendants, in addition to the
repository-root `../../AGENTS.md`. Paths below are relative to this crate unless
stated otherwise. This file adapts the discipline in `CLAUDE.md` for Codex.

Sans-IO Cardano consensus core. Hosts the protocol pieces that every
Cardano-Leios implementation must agree on: longest-chain selection
(Praos), Linear Leios EB elections + voting (CIP-0164), committee
selection (wFA + LS), pipeline phase math, vote aggregation.

This crate is shared by **`net-rs/` and `sim-rs/`**, not owned by either. Both consume it via
`path = "../../shared-rs/consensus"` from their member-crate directories. It lives under the `shared-rs/`
workspace so additional shared crates (e.g. `tcp-model`) can sit
alongside it without a circular dependency on either consumer.

## Discipline

These rules are why this crate exists. Breaking them defeats the point
of the extraction. Everything below applies to the behavior subsystem as well. Read
`src/behaviour/mod.rs` and `src/behaviour/tree/` for its current contracts; parts
of `src/behaviour/README.md` still describe the earlier hook-based API.

### 1. Sans-IO

No `tokio`, no networking, no clock reads, no file I/O.

- Time is **injected** as `Instant` parameters on methods that need it
  (`on_block_received`, `retry_select_chain`, …). The state machine
  never calls `Instant::now()` or `SystemTime::now()` itself.
- Randomness is **deterministic and stake-keyed** — committee choices use
  `committee.rs` helpers seeded by `(eb_hash, voter_id)` style inputs; behavior
  choices use the run seed and deterministic per-decision keys. No
  `from_entropy` / `thread_rng`.
- Tracing (`info!`, `warn!`) is allowed; it's a side-effect-free sink
  from the state machine's perspective.

### 2. Effect emission, not callbacks

State machines mutate themselves and return `Vec<Effect>`. The caller's
I/O wrapper drains the vector and dispatches each effect to the right
channel (network, validator, telemetry).

- **Public methods**: return `Vec<PraosEffect>` / `Vec<LeiosEffect>`.
- **Internal helpers** (private `*_internal` methods): take
  `fx: &mut Vec<…>` to append to the running batch — avoids allocating
  per call inside the state machine.
- Don't add a callback / closure parameter to "report something back."
  Add a new effect variant.

### 3. Determinism

`sim-rs` replays runs from a seed. shared-consensus must not introduce
non-deterministic ordering.

- `BTreeMap` / `BTreeSet` everywhere. No `HashMap` iteration in hot
  paths.
- Existing `HashMap` usage is keyed on EB hashes and only iterated
  during pure quorum checks where order doesn't change the outcome —
  audit before adding any new `HashMap`.
- Effect order in `Elections::on_slot` is part of the contract:
  every `EligibleToVote` (sorted by `eb_hash`) first, then every
  `Expired`. Don't shuffle.

### 4. Format-agnostic

Block bodies, headers, vote bodies cross the crate boundary as opaque
`Vec<u8>`. CBOR parsing is the I/O wrapper's job.

- `CachedBlock` carries `header: Vec<u8>, body: Vec<u8>`.
- `LeiosEffect::EmitVote` carries logical args (`emit_pv: bool`,
  `npv_signature: Option<Vec<u8>>`); the wrapper builds the wire body.
- The exception is `types.rs` — `Point` and `Tip` carry their own
  `minicbor` impls because their wire format is fixed across all
  Cardano implementations. Don't add more wire types here.

### 5. Comments stay consumer-neutral

Don't name `net-node` or `sim-rs` (or any future consumer) in
shared-consensus doc comments. Describe the contract from this crate's
own perspective — "the I/O wrapper" / "the caller", never "net-node
uses this for X."

## Module map

```
lib.rs              re-exports Point, Tip, PeerId, CommitteeSelection, StakeEntry

types.rs            Point, Tip with minicbor codec
peer.rs             PeerId(u64) wrapper
config.rs           CommitteeSelection enum, StakeEntry
pipeline.rs         PipelineConfig — phase math (Voting/CertEligible/Expired)
committee.rs        Committee selection (WfaLs, EveryoneVotes, StakeCentile), NPV lottery
lottery.rs          Praos f_block stake-weighted threshold formula
aggregation.rs      record_vote, QuorumFormed
bitmap.rs           sparse BTreeMap<u16, u64> for MsgLeiosBlockTxsRequest
chain_tree.rs       in-memory chain DAG, best-tip selection, prune_below
peer_chain.rs       per-peer announced fragment (cap-bounded VecDeque)
fetch.rs            per-channel fetch policies + CandidateTracker + PeerRtt
elections.rs        Elections sans-IO state machine — slot ticks → SlotEffect
praos.rs            PraosState — chain state + selection → PraosEffect
leios.rs            LeiosState — EB voting + tx fetch state → LeiosEffect
mempool.rs          MempoolState — bounded tx pool + EB-pinned bodies
                    → MempoolEffect
production.rs       BodyPath::decide — inline-RB vs EB vs empty-for-safety
behaviour/          Behavior trees, action registry, control signals, and
                    node selection; see behaviour/mod.rs and behaviour/tree/
```

## Behaviours

The behavior-tree engine in `src/behaviour/tree/` ticks once per slot and emits
`ControlSignal`. `LeiosState`, `PraosState`, and `MempoolState` apply its relevant
fields through `apply_control`; the caller's I/O actuators consume network and
production controls. Preserve that separation when adding a behavior.

- Action implementations live in `src/behaviour/actions/`; serializable kinds
  are `ActionSpec` in `src/behaviour/registry.rs`. Register new tree leaves in
  `src/behaviour/tree/actions.rs` and test their effects.
- The tree is generic over context and effect. Keep one grammar/parser;
  alternative instantiations bind their own action and condition vocabularies
  through `BtConfig::compile_with`.
- The compiler accepts self-contained TOML text. File reads and cross-file
  include resolution belong to the caller/authoring tools. Reject unresolved
  includes; validate before activation so ticking does not fail for config reasons.
- `.bt` authoring sources and the resolver live in the private sibling repo.
  Generated consumer TOML files are not committed to this public repository.
- Preserve deterministic randomness: use the run seed and per-decision keys
  through `blake2b_simd` / the registry's seed helpers. No clock reads or OS entropy.
- The earlier `Arc<Mutex<Box<dyn Behaviour>>>` hook design in historical docs is
  superseded by the tree/control interface in source. Do not reintroduce it.

## When adding a new method

1. Decide: pure query, or state mutation?
2. State mutation → return `Vec<Effect>` (or `()` if no effects ever).
3. Need wall-clock? Take `now: Instant` as a parameter.
4. Need iteration? `BTreeMap`/`BTreeSet`, not `HashMap`/`HashSet`.
5. Need wire bytes in/out? Carry as `Vec<u8>`; never decode CBOR.
6. Add a unit test that constructs the state, calls the method, and
   asserts on the returned effects.

## Tests

`cargo test` runs the full unit-test suite. There are **no integration
tests** — every state machine is tested directly in its own module's
`#[cfg(test)] mod tests` block. Mocking is trivial because effects are
just enum variants you compare against.

Common test helpers in each module:

- `elections.rs::test_pipeline()` — minimal `PipelineConfig`.
- `aggregation.rs::make_election(slot)` — fresh `EbElection` for the
  given slot.
- `praos.rs::install_validated_block(state, slot, seed, block_no, prev_seed)` —
  pre-populate `chain_tree + block_cache + validated` to skip driving
  every scenario through the public API.
- `leios.rs::elections_for(node_id)`, `cfg(persistent_seats)` — minimal
  `Elections` and `VotingConfig` builders.

## Building

From this crate directory:

```sh
cargo test
cargo clippy --all-targets -- -D warnings
```

From the repository root, build and test the entire shared workspace:

```sh
cargo build --manifest-path shared-rs/Cargo.toml --workspace
cargo test --manifest-path shared-rs/Cargo.toml --workspace
cargo clippy --manifest-path shared-rs/Cargo.toml --workspace --all-targets -- -D warnings
cargo fmt --manifest-path shared-rs/Cargo.toml --all --check
```

The repository root is the appropriate Codex workspace for edits spanning
consumers and shared crates. If an authorized edit is outside the current
sandbox, request the tool's required permission rather than using Claude's
`dangerouslyDisableSandbox` option.

## Consumer Compatibility

Both the simulator adapter (`../../sim-rs/sim-core/src/sim/shared_consensus.rs`)
and the private Piranha node already consume this crate. Check their call sites
when changing effects, configuration, or public method signatures; simulator
adoption is no longer merely a future plan. Run tests in this workspace and
check the affected consumer workspaces separately.

Keep doc comments consumer-neutral. Add abstractions when concrete consumers
need them, rather than pre-designing speculative `Mempool` or `Ledger` traits.
The historical extraction/branch narrative remains in `CLAUDE.md`.
