# Red Team implementation notes — T22–T26

Companion to [`threat-coverage.md`](./threat-coverage.md). Covers threats **T22–T26** from the
Ouroboros Leios [threat model](https://github.com/input-output-hk/ouroboros-leios/blob/main/docs/threat-model.md):
how the Red Team would implement each, and which environment is the right vehicle.

---

## T22 — Selectively withhold data from honest nodes  *(Data withholding)*

- **Capability:** network-position control + modest stake. **Impact:** *allows* certification but
  *delays* propagation — feed enough committee members that the EB certifies, but starve the broader
  honest population, opening a timing gap where certified blocks arrive too late → potential Praos
  timing violations. **Mitigation:** `L_diff` sizing, worst-case diffusion validation.
- **Distinct from T21:** T21 starves the *committee* to deny certification outright; T22 *deliberately
  lets certification happen* and attacks *downstream* propagation.
- **Gating: TOPOLOGY-gated (strongly).** The attack *is* a positioning attack — we must be the
  relay/chokepoint feeding the honest population while still reaching the committee. Stake needed is
  only "modest" and is trivially obtained in either real environment.
- **Red Team by environment:**
  - **sim-rs** — study `L_diff` and worst-case diffusion timing: how large a withhold window causes a
    Praos-timing violation. Parameter study, no positioning.
  - **Piranha (local)** — the mechanism already exists (per-peer selective withholding, the "t22"
    behaviour). Prove "feed set A, starve set B" on a small controlled cluster.
  - **Piranha + ignite** — ⭐ **primary vehicle.** Build a topology where our Piranha man-in-the-middle node is the
    upstream/relay for the honest population but committee members have an independent path (or we
    relay to them promptly). Assign the modest stake needed. Reproducible; measure the propagation
    gap and any Praos-timing impact against real Haskell nodes.
  - **Piranha + testnet** — the target, but **positioning is the blocker**: we can't choose to be the
    honest population's upstream on a live net. Attempt opportunistically after rehearsing on ignite.
- **Recommended path:** develop + prove on **ignite** → attempt on **testnet** for the prize.

---

## T23 — Withhold then release a large number of EBs  *(Protocol bursts)*

- **Capability:** stake proportional to burst magnitude (≈ one-third stake ⇒ ~720 endorser blocks /
  9+ gigabytes over 12 hours). **Impact:**
  bandwidth saturation, processing delays, potential Praos timing disruption. **Mitigation:**
  freshest-first delivery, traffic prioritization.
- **Gating: stake-heavy but NOT topology-gated.** The producer accumulates endorser blocks it is
  eligible to produce, then floods them out — high stake and broad connectivity, not a *specific*
  position. **Piranha does not fabricate the endorser blocks** (that needs VRF eligibility + a KES
  signature, and `cardano-cli` cannot make one either). Instead a **real Haskell Leios
  block-producing node under our control, with our own stake, produces them**, and Piranha acts as
  its relay to buffer and burst-release. The one Piranha gap is that burst buffer.
- **Red Team by environment:**
  - **sim-rs** — ⭐ model the burst dynamics at scale: saturation curves, freshest-first behaviour,
    Praos-diffusion delay as a function of burst size and stake. Cheapest way to map the envelope.
  - **Piranha (local, Piranha)** — drive the accumulate-then-release *dynamics* with faked endorser
    blocks against our faked-validation cluster; limited by local bandwidth realism.
  - **Piranha + ignite** — ⭐ **primary vehicle**: run a genesis-staked real Leios node as the endorser-block
    producer (no registration / ~2-epoch activation delay), place Piranha as its egress relay, and
    burst; controlled bandwidth + Praos-timing measurement, no public-spam concern.
  - **Piranha + testnet** — ⭐⭐ **the target**: same shape, but the producer is a real testnet stake pool
    we register and fund from the faucet (stake-activation delay applies), and we burst against the
    live network — mind coordination/permission (multi-gigabyte bursts onto a shared network).
- **Recommended path:** map the envelope in **sim-rs** → stand up the staked producer + Piranha burst
  relay in **ignite** → reproduce on **testnet** (registered pool, with coordination) for the prize.

---

## T24 — Submit duplicate transactions  *(Transaction-based DoS)*

- **Capability:** network bandwidth. **Impact:** resource waste. **Mitigation:** pull-based diffusion,
  validation.
- **Gating: none — stake-independent, topology-independent.** Any client can submit from anywhere.
- **Red Team by environment:**
  - **sim-rs** — limited; dedup is a real-node property.
  - **Piranha (local)** — submit duplicates via TxSubmission; confirm our own dedup path (mechanism).
  - **Piranha + testnet** — ⭐⭐ target; attemptable directly. Measure whether real pull-based diffusion
    dedups and how much waste survives.
  - **Piranha + ignite** — safe rehearsal against real nodes with tunable mempool params; no unique
    advantage since there's no topology/stake dependence.
- **Recommended path:** straight at **testnet**; use **Piranha/ignite** only to build/rehearse the
  submitter. (Low-severity threat.)

---

## T25 — Submit invalid transactions  *(Transaction-based DoS)*

- **Capability:** network bandwidth. **Impact:** consume processing before the tx is discarded.
  **Mitigation:** validate before propagation.
- **Gating: none — stake-independent, topology-independent.**
- **Red Team by environment:**
  - **sim-rs** — validation-cost modelling only.
  - **Piranha (local)** — craft + submit invalid txs; but Piranha validation is faked, so fidelity of
    "rejected before propagation" is limited.
  - **Piranha + testnet** — ⭐⭐ target; attemptable directly against real validation.
  - **Piranha + ignite** — safe rehearsal vs. real Haskell validation.
- **Recommended path:** straight at **testnet**; rehearse on **ignite** if we want repeatable
  measurement of processing cost.

---

## T26 — Submit conflicting transactions  *(Transaction-based DoS)*

- **Capability:** transaction fees per conflict. **Impact:** nodes process alternatives when only one
  can succeed → wasted effort. **Mitigation:** Linear Leios keeps conflicts out of permanent storage.
- **Gating: none for stake/topology — but needs real fees + real ledger.** Faucet test-ADA covers the
  fees; the conflict-resolution and Linear-Leios behaviour only appear on real nodes.
- **Red Team by environment:**
  - **sim-rs** — limited; conflict resolution needs ledger semantics.
  - **Piranha (local)** — submit conflicting txs (mechanism), but Piranha's faked ledger can't show real
    "only one succeeds" resolution.
  - **Piranha + testnet** — ⭐⭐ target; faucet test-ADA funds the per-conflict fees; observe real
    conflict processing and Linear-Leios's handling.
  - **Piranha + ignite** — controlled real-ledger environment with tunable params; good for
    reproducible measurement of the wasted-processing effect.
- **Recommended path:** straight at **testnet** (faucet-funded); **ignite** for controlled/repeatable
  measurement of the processing waste.

---

## Current tooling readiness & gap analysis (Piranha)

Grounded in a survey of the `Piranha` code on branch `cet/gp-search` (2026-07-07). Piranha is
today a **fake-crypto prototype**. The organizing constraint is:

> Piranha can (a) **withhold, relay, reorder, or drop** data that *real* nodes produce, and
> (b) **submit deliberately malformed / garbage** data. It **cannot produce valid artifacts**
> (transactions, endorser blocks, votes, blocks) that a real Haskell node will accept — there
> is no signing, no ledger/UTxO, and no transaction-body codec.

So a threat is *near-ready* if it only needs withholding/relaying real data or submitting
invalid data, and *blocked* if it needs Piranha to produce **valid** data that real nodes accept.

Note: **per-node stake is configurable** in Piranha (`net-node/src/config.rs:333` `ProductionConfig.stake`,
selection ratio at `production.rs:226`), so the stake precondition is met in every environment
(Piranha config, ignite assignment, testnet faucet). Stake is not a blocker anywhere.

### Capability snapshot

| Capability | State | Evidence |
|---|---|---|
| Per-node stake / VRF selection *ratio* | Implemented (real ratio, fake proof) | `config.rs:333`, `production.rs:226` |
| Per-peer selective withholding (drop-to-peer, t22 tx filter, per-peer routing) | Implemented | engine `behaviour/tree/control.rs` (`DropTo`, `EquivocateRouting`), `server_handlers.rs:268`, `behaviours/t22.toml` |
| Coarse network partitioning | Implemented | `NetworkCommand::SetPeerBlocklist` (`types.rs:224`), `partition_scenarios.rs` |
| TxSubmission wire (ship opaque transaction bytes) | Implemented | `net-node/src/mempool.rs` + TxSubmission |
| Transaction-body CBOR codec | Absent (bodies are opaque blobs) | `net-codec/block.rs`, `production.rs:337` |
| Valid transaction construction (body + witnesses) | Stubbed-fake (random bytes, empty witness set) | `mempool.rs:444`, `production.rs:341` |
| Key management / signing (Ed25519 / KES / BLS) | Absent (zeros / empty sigs) | `production.rs:311`, `consensus/leios/mod.rs:502` |
| UTxO / ledger state | Absent (`FakeLedger` sleeps) | `validation.rs:190` |
| Transaction / block / EB / vote validation | Stubbed-fake (timed no-op, no signature checks) | `validation.rs:217`, `mempool.rs:415` |
| Real endorser-block production (valid, accepted by real nodes) | Stubbed-fake (valid CBOR structure, zeroed crypto) | `production.rs:149` |
| Endorser-block burst (accumulate then release) | Absent | no delayed-release buffer exists |

### Per-threat readiness, gap, and rough effort

**T22 — Selectively withhold from honest nodes.** *Ready-ish (lowest-hanging topology attack).*
The attack withholds/relays data that *real* nodes produce, so it needs **no** transaction or
endorser-block validity from Piranha. We already have per-peer withholding (the `t22` attack,
drop-to-peer, the transaction filter), stake config, and partitioning. **Gap:** logic to
distinguish committee vs. other honest nodes (feed one, starve the other) plus an ignite
topology that places our relay in the man-in-the-middle position. **Effort: LOW–MEDIUM** (the
mechanism exists; add targeting + ignite placement). Already "in progress."

**T25 — Submit invalid transactions.** *Ready (cheapest transaction denial-of-service).*
Piranha *already* emits invalid (random-byte) transactions and can flood them over TxSubmission,
so a first cut is nearly free. **Gap:** to actually reach the cryptographic-validation stage on
a real node (rather than being rejected at CBOR decode), we need transactions that *decode* but
*fail validation* — i.e. a minimal transaction-body encoder. **Effort: LOW** for a garbage flood;
**MEDIUM** to craft decodable-but-invalid transactions that maximize wasted validation work.

**T24 — Submit duplicate transactions.** *Gated on one valid transaction.*
A real node rejects garbage before any deduplication, so the duplicates must be **valid** to
exercise the dedup path. We have the submission fan-out and duplication is trivial; the missing
piece is a single valid signed transaction. **Effort: LOW–MEDIUM via the cardano-cli shortcut**
below; HIGH if built natively.

**T26 — Submit conflicting transactions.** *Gated on valid conflicting transactions.*
Needs two or more valid transactions spending the same input(s), plus fees. **Gap:** valid
transaction construction + a known spendable UTxO + fee calculation. **Effort: MEDIUM via
cardano-cli** (build two transactions spending the same faucet UTxO, submit both); HIGH native.

**T23 — Withhold then release a large number of endorser blocks.** *Feasible via a real producing
node — Piranha need not fabricate endorser blocks.*
Producing a valid endorser block needs VRF leader-election eligibility (registered stake + VRF key +
being elected) plus a KES signature — Piranha has none of this, and `cardano-cli` cannot make an
endorser block (it is a consensus artifact, not a wallet one). But we do not fabricate them: we run
a **genuine Haskell Leios block-producing node under our control, with our own stake**, and it emits
valid endorser blocks by construction. The adversarial part is then Piranha acting as that node's
**relay/upstream**, buffering its endorser blocks and releasing them in a timed burst.
`cardano-cli`'s role shrinks to generating the node's VRF/KES/cold keys and stake-pool registration.
**The one real Piranha gap is the withhold-then-release burst buffer — MEDIUM effort — not
endorser-block crypto.** **Effort:** MEDIUM in **ignite** (allocate stake in genesis, so no
registration / ~2-epoch activation delay, and place Piranha as the node's egress); heavier on
**testnet** (real pool registration, stake-activation delay, public-spam care). Against our own
Piranha-Cluster (faked validation) we can also drive the dynamics with fake endorser blocks once
the buffer exists; sim-rs models the dynamics today.

### "How much work to produce valid transactions in Piranha?"

Piranha has **none** of the transaction stack (no body codec, no keys/signing, no UTxO, no fees).
Two paths:

- **Path A — cardano-cli shortcut (recommended to unblock T24 and T26 quickly).** Build and sign
  valid transactions *out of band* with `cardano-cli` against a faucet-funded address, then feed
  the resulting bytes into Piranha's **existing** TxSubmission path (Piranha already ships opaque
  transaction bytes). The Piranha change is small — accept externally supplied transaction bytes
  into the mempool/submission path. **Effort: LOW–MEDIUM** (mostly scripting + faucet/UTxO
  plumbing). This unblocks T24 and T26 without building a transaction stack into Piranha.
- **Path B — native transaction stack in Piranha (larger, reusable).** Conway `transaction_body`
  CBOR encoder + Ed25519 keys and witness sets + a UTxO source (query a node via LocalStateQuery
  or track a faucet UTxO) + fee/parameter handling and assembly. **Rough order: a few
  engineer-weeks** for a minimal "valid, accepted testnet transaction." Worth it only if we want
  Piranha self-contained; unnecessary just to land T24–T26.

T25 needs **no** valid transaction at all (invalid is the point) — only an optional minimal
decodable-but-invalid encoder if we want to drive real validation cost rather than early rejects.

### Lowest-hanging fruit (ranked)

1. **T25 — invalid transactions.** Piranha already emits them; flood via TxSubmission. Near-free
   (optional minimal encoder to reach real validation cost).
2. **T22 — withhold from honest nodes.** Withholding + the `t22` attack already built; add
   committee-vs-honest targeting and ignite placement. Already in progress.
3. **T24 — duplicate transactions.** One cardano-cli transaction + existing duplication/submission.
4. **T26 — conflicting transactions.** Two cardano-cli transactions on the same UTxO + submission.
5. **T23 — burst endorser blocks.** Don't fabricate endorser blocks — run a real staked Leios
   node as the producer and use Piranha as the withhold-then-burst relay. The only Piranha gap is
   the burst buffer (MEDIUM). Easiest in ignite (genesis stake); sim-rs models the dynamics today.

---

## Summary

| Threat | Topology-gated? | Primary vehicle | Live-testnet route |
|---|---|---|---|
| T22 withhold from honest nodes | **Yes** (man-in-the-middle position) | **ignite** (place man-in-the-middle + stake) | testnet, once positioned |
| T23 burst-release EBs | No (stake-heavy) | **ignite** (genesis-staked real Leios node produces EBs; Piranha bursts); sim for envelope | testnet (register stake + activate) |
| T24 duplicate txs | No | **testnet** | testnet directly |
| T25 invalid txs | No | **testnet** | testnet directly |
| T26 conflicting txs | No (needs fees+ledger) | **testnet** (faucet fees) | testnet directly |

**One-line rule:** *topology-gated → ignite is the enabler and testnet the eventual target;
everything else → go straight at testnet (stake via faucet where needed), with ignite/sim as
safe rehearsal and modelling.*
