# Feature Specification: Declarative Transaction-Flow Experiments (`txflow`)

**Feature Branch**: `002-tx-flow-experiments`

**Created**: 2026-10-12

**Status**: Draft

**Input**: Declarative, repeatable transaction workloads for Leios experiments. Rather
than scripts around `cardano-cli`, an experiment describes the transaction flow to
create: the transactions, the outputs each one spends and produces, chain lengths and
conflicts. It contains no concrete transaction ids, so it can be rerun on any network at
any time. A minimal example is six transactions in two chains, a1→a2→a3 and b1→b2→b3,
where each aᵢ conflicts with bᵢ. The same description should capture
application-specific patterns, such as a DEX's order-and-batch flow, so they can be
recreated on a Leios network at their observed size or scaled up. Two further uses
follow: deriving an experiment from the transaction flow at a dApp's script address (the
UTxOs held there over time, and the transactions that created and spent them), so real
application behaviour can be replayed on other networks or on future protocol versions
such as Leios; and rerunning the same experiments as the Leios prototype evolves.
Existing tools such as tx3 describe single transactions and are reused for that purpose;
the graph layer is built here.

Prior art and the reasons for building rather than reusing are in
[`research.md`](./research.md). File formats are in [`data-model.md`](./data-model.md).

## Context

Today the repository's only submitter is `net-cli submit`
(`net-rs/net-cli/src/submit.rs`). It speaks N2N TxSubmission but sends random-byte
bodies, so it creates no real transactions. Real load on Leios devnets comes from
upstream `tx-firehose` (N2C self-payment chains at a target rate). Neither can express
a structured workload: named transactions, dependencies between them, deliberate
conflicts, or where and when each is released.

`txflow` adds that layer. An experiment is described in four artefacts, each a file
that can be inspected and diffed:

```
experiment spec ──expand──▶ graph IR ──materialise──▶ manifest ──replay──▶ network
(patterns, params)         (symbolic DAG)  (+ funding, seed)  (signed txs)  (schedule)  └▶ results
```

The spec and the IR are network- and version-independent. The manifest is one concrete
binding of the IR to a network. The schedule says how the manifest is released.

**Inputs are the primitive.** Each transaction names the outputs it spends and the
outputs it only reads. Conflicts (two transactions spend one output), orphans (an
ancestor did not land) and reference contention (one transaction spends what another
reads) are never declared; `txflow` derives them from the inputs. This lifts the sim-rs
limitation where each transaction has a single abstract `input_id` and can only conflict
with the transaction generated just before it (`sim-rs/sim-core/src/sim/tx.rs`).

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Run a hand-written conflict experiment on a devnet (Priority: P1) 🎯 MVP

A researcher writes a short experiment file describing two chains of three plain
payments, a1→a2→a3 and b1→b2→b3, where each aᵢ conflicts with bᵢ. They point `txflow`
at a funded key on the proto-devnet and a schedule that sends side a to one node and
side b to another at the same instant. `txflow` funds the experiment, builds and signs
all six transactions offline, submits them, and writes a results file in which every
event is labelled `a1` … `b3`, not by hash.

**Why this priority**: It exercises every layer (spec, IR, materialiser, replay,
results) on the smallest useful case. It is also something no current tool can do:
tx-firehose cannot express conflicts or send to two nodes at once, and `net-cli submit`
sends no real transactions.

**Independent Test**: On the proto-devnet, run the example from
[`data-model.md`](./data-model.md#worked-example). Check that the results file has one
submission event per transaction, that each conflict set is reported, and that the
chain observer records at most one transaction from each conflict set on chain.

**Acceptance Scenarios**:

1. **Given** the example spec and a chain-context file (protocol parameters, funding key
   and UTxO), **When** the user runs materialise, **Then** the funding transaction and the
   six experiment transactions are built, validated and signed with no network access,
   nothing is submitted, and a manifest lists each by symbolic name with its tx id, CBOR,
   size and conflict sets.
2. **Given** that manifest, **When** the user runs fund, **Then** the funding
   transaction is submitted and `txflow` waits until it is observed on chain.
3. **Given** the funded manifest and a two-target schedule, **When** the user runs
   replay, **Then** a-side transactions go only to the first target and b-side only to the
   second, within the configured offset; every event the transport can report is written
   as one JSON line keyed by symbolic name; and after the observation window each
   transaction has one outcome (included, lost conflict, orphaned, not seen).
4. **Given** the same spec, seed, protocol parameters and funding UTxO, **When**
   materialise runs twice, **Then** both manifests are byte-identical.
5. **Given** a spec whose graph has a cycle, an input naming an output that does not
   exist, or an output that cannot cover its fee, **When** the user runs expand or
   materialise, **Then** `txflow` stops before anything is submitted and names the
   offending transaction and field.

---

### User Story 2 - Describe workloads by pattern and scale them (Priority: P2)

A researcher describes a workload as instances of named patterns (chain, fan-out,
fan-in, conflicting chains, conflict set, diamond, order→batch) with parameters such
as chain length, inputs and outputs per transaction, number of conflicts, and a scale
factor, instead of listing each transaction.

**Why this priority**: Hand-written graphs stop being practical beyond a few dozen
transactions. Patterns make "the same experiment, 10× larger" a one-number change.

**Independent Test**: Expand a spec using each pattern and compare the IR against
checked-in snapshots; change a `count` and check that instances are added without
changing any instance's structure.

**Acceptance Scenarios**:

1. **Given** `conflicting_chains` with `length: 3, conflict: per_step`, **When**
   expanded, **Then** the IR has six transactions, three shared conflict inputs, and
   three conflict sets of size two.
2. **Given** the same pattern with `conflict: root`, **When** expanded, **Then** only the
   first pair shares an input, and the IR marks later transactions as descendants of a
   conflict rather than as conflicts themselves.
3. **Given** two instances of the same pattern, or one instance with `count: 2`,
   **When** expanded, **Then** their transaction names are namespaced by instance
   (`<id>/…`, or `<id>_<k>/…` for counted instances) and do not collide.

---

### User Story 3 - Rerun experiments against new prototype builds (Priority: P3)

As the Leios prototype changes, a researcher reruns a stored set of experiments
against each new node build and compares results with the previous run. Experiments
can declare expected outcomes, so a rerun yields pass/fail as well as metrics.

**Why this priority**: The experiment definitions are the durable asset. Rerunning
them unchanged is what makes results comparable across versions.

**Independent Test**: Run one stored experiment against two node builds; check that both
results files record the node version, protocol parameters, IR hash and schedule hash,
that both hashes are identical, and that declared expectations are evaluated and reported.

**Acceptance Scenarios**:

1. **Given** a stored spec, **When** it is run against a network whose protocol
   parameters differ from the last run, **Then** a new manifest is built from the same IR
   and the IR hash in both results is identical.
2. **Given** an expectation "exactly one transaction of each conflict set is on chain",
   **When** the run finishes, **Then** the report states pass or fail per conflict set.

---

### User Story 4 - Import a dApp's on-chain activity as an experiment (Priority: P4)

A researcher points `txflow` at one or more script addresses (or a minting policy) and a
slot range in a chain-history database. `txflow` reconstructs the transaction flow there:
the UTxOs held at the address over time, and the transactions that created and spent
them, together with the outside outputs those transactions consumed. It produces an IR
from that flow where tx ids are replaced
by names, addresses by roles (script, user, batcher), external inputs by sources, and
observed timing by a schedule. Where it recognises a known pattern, it emits a
parameterised pattern instance instead of only the literal graph, so the result can be
scaled by contention as well as by copying.

**Why this priority**: It turns real application behaviour (for example a DEX's
order→batch flow) into repeatable, scalable test cases for Leios, without hand-modelling.
It depends on stories 1 and 2.

**Independent Test**: Import a known slot range from the Musashi db-sync and check that
the IR's transaction count, edges and conflict sets match the source, that no tx id or
address appears in it, and that it materialises on the proto-devnet at the chosen fidelity.

**Acceptance Scenarios**:

1. **Given** a script address and slot range, **When** imported at `shape` fidelity,
   **Then** the IR preserves input/output counts, sizes and dependency edges, and contains
   no tx ids or addresses.
2. **Given** an imported order→batch flow, **When** pattern fitting succeeds, **Then**
   the spec contains an `order_batch` instance whose parameters reproduce the observed
   counts, and scaling `users` increases contention on the same pool output.
3. **Given** a script that cannot be redeployed unchanged on the target network,
   **When** imported at `scripts` fidelity, **Then** `txflow` reports it and offers a
   stand-in validator of matching size and execution budget.

---

### User Story 5 - Use tx3 templates for dApp-shaped transactions (Priority: P5)

A graph transaction can name a tx3 template (`file.tx3#tx`) and arguments instead of a
plain payment shape. `txflow` binds the template's inputs to the UTxOs given by graph
edges and builds the transaction locally.

**Why this priority**: Realistic dApp transactions without hand-coding each shape. It is
an extension point, not needed for the MVP.

**Independent Test**: Express one transaction of an `order_batch` instance as a tx3
template and check that the materialised transaction spends exactly the inputs the graph
names.

**Acceptance Scenarios**:

1. **Given** a transaction with `template` set, **When** materialised, **Then** every
   template input is bound to a graph reference and no input is chosen by the template's
   own selection rules.

### Edge Cases

- **Conflict versus orphan**: if only a1 and b1 share an input, a2 and b2 are not
  conflicts; they become invalid when their parent loses. The IR and results must
  distinguish the two.
- **Same-node conflicts**: sending both halves of a conflict to one node makes that
  node keep only the first. Over N2C the second is reported as rejected; over N2N the
  node requests or ignores the body without saying why, and only observation shows the
  result. Either is a valid outcome to record, not an error.
- **Partial funding**: if any funding transaction is not observed on chain within a
  timeout, nothing else is submitted.
- **Rejected ancestor**: the replayer still submits descendants of a transaction that
  failed (the network's reaction is the experiment). For N2C releases the schedule can set
  `skip_descendants_of_rejected`; N2N reports no rejections, so the option has no effect
  there.
- **Metadata cost**: experiment tags add bytes; manifests record each tx's size including
  tags so sizes are never compared against untagged runs by mistake.
- **Era or parameter change** between materialise and replay: `fund` and `replay` take a
  fresh chain-context file and refuse if its era or protocol-parameter hash differs from
  the manifest's.
- **Key reuse**: sources are paid to a run key derived from the funding key, IR hash and
  seed, not to the funding key's address, so other tools and other runs using the funding
  key cannot spend them between `fund` and `replay`. Two runs that name the same funding
  UTxO conflict at funding time, and only one can be funded.
- **Large experiments**: funding outputs are split across as many chained funding
  transactions as `maxTxSize` requires.
- **Long names**: transaction metadata carries only fixed-size ids (IR hash prefix, run
  id, transaction index), never names or labels, so Cardano's 64-byte metadata string
  limit cannot be hit.

## Requirements *(mandatory)*

### Functional Requirements

**Experiment spec and IR**

- **FR-001**: Users MUST be able to describe an experiment as a file containing
  parameters, wallets, pattern instances and/or explicit transactions, a schedule, and
  optional expectations.
- **FR-002**: `txflow` MUST expand a spec into a graph IR in which each transaction has a
  symbolic name and each input refers either to a named funding source or to an output of
  another transaction by name and index.
- **FR-003**: The IR MUST NOT contain tx ids, addresses, network magic or era-specific
  encodings.
- **FR-004**: `txflow` MUST derive conflict sets from outputs spent by two or more
  transactions, reference contention from outputs spent by one transaction and read by
  another, and orphan relations from ancestors in a conflict set. Shared reference inputs
  MUST NOT form a conflict.
- **FR-005**: `txflow` MUST reject an IR that has a cycle, a dangling reference or a
  transaction with no inputs, and MUST list every derived conflict set when it expands
  or validates an IR, so an unintended shared input is visible before materialising.
- **FR-006**: The IR MUST have a canonical serialisation and a content hash that is
  stable across platforms and runs, and that changes when any referenced validator
  definition or tx3 template file changes. The schedule MUST have its own hash.

**Patterns**

- **FR-007**: `txflow` MUST provide the patterns `independent`, `chain`, `fan_out`,
  `fan_in`, `conflicting_chains`, `conflict_set` and `diamond` in the MVP, and
  `order_batch` and `contended_state` once script support exists.
- **FR-008**: Pattern parameters MUST include counts (inputs, outputs, length, width,
  conflicts). Scaling MUST be explicit: a spec scales by referencing a parameter in
  `count` or in a pattern argument, and no parameter is applied implicitly.
- **FR-009**: Each pattern MUST have a checked-in snapshot test of its expanded IR.

**Materialisation**

- **FR-010**: `txflow` MUST fund an experiment with funding transactions that create
  exactly the sources the IR requires, splitting them across as many chained funding
  transactions as the maximum transaction size needs, and paying them to a run key
  derived from the funding key, IR hash and seed.
- **FR-011**: `txflow` MUST build, validate and sign the funding transactions and every
  experiment transaction offline, in topological order, with explicit inputs and fees,
  before anything is submitted. Materialise MUST NOT perform network I/O; it reads
  protocol parameters, era and funding UTxOs from a chain-context file.
- **FR-012**: Given the same IR, seed, protocol parameters and funding UTxOs, materialise
  MUST produce a byte-identical manifest. The manifest MUST NOT contain wall-clock data.
- **FR-013**: Each transaction MUST carry metadata with fixed-size identifiers only (IR
  hash prefix, run id, transaction index), and MAY carry the tx-firehose colour tag
  (label 1022) so existing mempool tooling keeps working.
- **FR-014**: The manifest MUST record, per transaction: symbolic name, index, tx id,
  CBOR, size, resolved inputs, all conflict sets it belongs to, the conflict sets it is
  orphaned by, and labels; and for the run: network magic, era, protocol-parameter hash,
  IR hash, seed and run id.
- **FR-015**: `txflow` MUST validate every built transaction locally (balance, fee,
  size limits, phase-1 rules) before writing the manifest.

**Funding, schedule and replay**

- **FR-016**: A separate `fund` step MUST submit the funding transactions and wait until
  they are observed on chain; replay MUST refuse to start until funding is confirmed.
- **FR-017**: The schedule MUST select transactions by name or label and assign each
  selection a target node, a transport and a release offset.
- **FR-018**: Replay MUST support N2N TxSubmission (via the repository's net-rs stack),
  including holding open connections to several targets and releasing to them at the
  same instant.
- **FR-019**: Replay SHOULD support N2C LocalTxSubmission to a node socket, so the node's
  accept or reject decision and reason are recorded.
- **FR-020**: Replay MUST write one JSON line per event keyed by symbolic name and tx id,
  limited to what the transport reports (N2N: offered, body requested; N2C: accepted,
  rejected with reason), plus `on_chain` events from an observer, with a monotonic
  timestamp anchored to wall-clock time and slot at replay start.
- **FR-021**: At the end of the observation window, replay MUST assign every submitted
  transaction one outcome (included, lost conflict, orphaned, lost reference, not seen),
  with additional causes when more than one applies.
- **FR-022**: `fund` and `replay` MUST refuse to run if the era or protocol-parameter hash
  in a fresh chain-context file differs from the manifest's.

**Results and reruns**

- **FR-023**: Every results file MUST record the node version reported by the target(s)
  where available, the network, the IR hash, the schedule hash, the seed and the `txflow`
  version.
- **FR-024**: Specs MUST be able to declare expectations over results (inclusion of
  conflict-set members, inclusion latency bounds in slots, absence of both halves of a
  conflict in one block), and a run MUST report each as pass or fail.

**Import**

- **FR-025**: `txflow` MUST import the transactions touching a set of script addresses or
  policies over a slot range from a cardano-db-sync database into an IR and schedule.
- **FR-026**: Import MUST support `shape`, `values` and `scripts` fidelity levels.
- **FR-027**: Import MUST replace tx ids with names and addresses with roles, and
  represent outputs from outside the selection as sources.
- **FR-028**: Import SHOULD fit recognised patterns (at least `order_batch`) and emit a
  pattern instance alongside the literal graph.

**tx3 templates**

- **FR-029**: A graph transaction MAY name a tx3 template and arguments; `txflow` MUST
  bind every template input to a graph reference and build the transaction itself,
  without delegating input selection to a tx3 resolver.

### Key Entities

- **Experiment spec**: what a person writes or the importer produces. Parameters,
  wallets, pattern instances, explicit transactions, schedule, expectations, seed.
- **Pattern**: a named, parameterised generator of an IR fragment.
- **Graph IR**: sources, validators, policies, transactions (inputs, reference inputs,
  collateral, mint, outputs, labels, optional template), and derived relations (conflict
  sets, reference contention, orphans). Version- and network-free; has a content hash.
- **Chain context**: era, protocol parameters and funding UTxOs for one network, read by
  materialise so it needs no network access.
- **Source**: an abstract funding output the materialiser must provide (owner, value).
- **Manifest**: one concrete binding of an IR to a chain context: funding transactions,
  signed experiment transactions, name→tx id map, run metadata. No wall-clock data.
- **Schedule**: selections of transactions mapped to target, transport and offset.
- **Results**: append-only event log for one replay, plus the expectation report.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: The a1..a3 / b1..b3 per-step conflict experiment can be written in at
  most 20 lines and run end to end on the proto-devnet with one command per stage.
- **SC-002**: Materialising a 1,000-transaction chain needs no network access and
  completes in under 10 seconds on a developer laptop.
- **SC-003**: Two materialisations with identical inputs produce identical manifests
  (byte comparison) in 100% of CI runs.
- **SC-004**: A stored experiment reruns against a new node build with no edits to
  the spec, and its results can be compared with the previous run by IR hash and schedule
  hash.
- **SC-005**: Every transaction in a results file can be traced to its symbolic name and
  its pattern instance without consulting any other file.

## Assumptions

- `txflow` is a new crate and binary in the `net-rs/` workspace so replay can use
  `net-core`'s N2N stack directly. It needs additive changes to `net-core`: a per-tx
  event when a peer requests a body (today `txsubmission::run_client` only drains a
  channel), and later a Unix-socket bearer for N2C (net-rs is TCP-only today). Piranha
  consumes `net-core` by path, so each of those changes must also be built against
  `net-node` before merging.
- Transactions are built with Pallas (`pallas-txbuilder`, `pallas-validate`); tx3
  crates are used only to evaluate templates (story 5). **Risk:** Pallas 1.4.0 builds and
  validates Conway transactions only, while the Leios devnets run the Dijkstra era. The
  first implementation task is a spike to check whether a Conway-encoded payment is
  accepted in Dijkstra; if not, Dijkstra support has to be added to Pallas (upstream) or
  built here. See `research.md` D2.
- Protocol parameters and funding UTxOs reach materialise as a chain-context file. The
  MVP produces it with a one-off query (`cardano-cli query`, Ogmios or db-sync); a net-rs
  N2C LocalStateQuery client can replace that later.
- The first target network is the proto-devnet in `ouroboros-leios/demo/proto-devnet`;
  Musashi follows. Funding comes from a payment key with sufficient balance; no faucet
  integration in the MVP.
- Plain payments only in the MVP. Script patterns use an "always succeeds" validator
  with a configurable execution budget until real validators are chosen.
- The MVP uses the same key for both sides of a conflict. Per-side wallets are a spec
  option, not a requirement.
- Chain observation (which transactions landed, in which RB/EB) uses an existing source
  (db-sync or node logs) rather than a new chain follower in the MVP. It also confirms
  funding (FR-016).
- tx-firehose and tx-centrifuge remain the tools for steady background load; `txflow`
  is for structured foreground workloads and does not replace them.

## Out of Scope

- Driving sim-rs from the IR. The IR is designed so this is possible later (it would
  replace the single `input_id` and `tx-conflict-fraction` model), but it is not part of
  this feature.
- Event-triggered releases (after acceptance, on RB or EB announcement). The schedule
  format reserves a `trigger` field; only time offsets are implemented first.
- Trace variants in the style of cooked-validators (tweaks applied "somewhere" or
  "everywhere"); noted as a likely follow-up.
- Importing from sources other than db-sync (Kupo, Blockfrost, node logs).
