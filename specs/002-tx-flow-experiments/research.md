# Research: Declarative Transaction-Flow Experiments

Survey date 2026-10-12. Maintenance status is from each project's latest commit and
newest tag on that date (release dates were not checked). The question: does existing
open-source tooling already let us define a transaction experiment as an abstract
UTxO graph, bind it to a funded wallet and replay it?

**Finding:** no single tool does this. The parts exist separately; the graph layer
(named transactions, edges between them, derived conflicts, a separate release schedule)
does not exist anywhere we looked.

## D1. Build the graph layer ourselves; use tx3 only for single-transaction shapes

- **Decision**: `txflow` owns the spec, IR, schedule and results formats. tx3 templates
  may describe what one graph transaction looks like (spec story 5).
- **Rationale**: tx3 ([tx3-lang/tx3](https://github.com/tx3-lang/tx3), active, 0.25.0
  Sep 2026) describes one transaction per template. It has no construct linking one
  template's output to another's input, no conflicts, no repetition and no timing, and
  its spec delegates coin selection and balancing to the resolver. We need the opposite:
  explicit inputs and tx ids known before submission. A template input can be pinned with
  `ref:`, which is exactly the hook a graph layer needs. `trix test`
  ([tx3-lang/trix](https://github.com/tx3-lang/trix)) is the nearest scenario runner but
  runs txs strictly in sequence with a block wait between them, allows one signer, and
  cannot reference earlier outputs.
- **Alternatives considered**:
  - *Extend the tx3 language with multi-tx graphs*: rejected. It changes the language's
    model, and a fork would have to track a fast-moving upstream.
  - *Extend `trix test`*: a reasonable upstream contribution later (output-to-input
    references between steps), but it would still lack conflicts and concurrent
    multi-target release.

## D2. Build transactions with Pallas

- **Decision**: materialise with `pallas-txbuilder` and check with `pallas-validate`.
- **Rationale**: [Pallas](https://github.com/txpipe/pallas) (active, v1.4.0) builds from
  explicit inputs with an explicit fee and no coin selection, so tx ids can be computed
  offline, chains of unconfirmed transactions cost nothing extra, and conflicting
  siblings are built from the same input. It is Rust, like the rest of the repo, and
  `shared-rs` already depends on parts of Pallas (`pallas_vrf`, `pallas-math`).
- **Risk, Dijkstra era**: Pallas 1.4.0 has no Dijkstra support. `pallas-txbuilder`
  exposes only `BuildConway`, and neither `pallas-traverse` nor `pallas-validate` mention
  Dijkstra (checked in the published 1.4.0 crates). The Leios devnets run Dijkstra
  (`net-core`'s `ORIGIN_ERA` is 7). A plain payment's body may decode the same in both
  eras, but that is unverified, so the first implementation task is a spike: build a
  Conway payment with Pallas, submit it to the proto-devnet over N2C, and see whether it
  is accepted. If it is not, the options are adding Dijkstra to Pallas upstream or a
  thin Dijkstra encoder in `txflow` for the subset of fields the IR uses.
- **Alternatives considered**:
  - *Evolution SDK* (IntersectMBO, TS): `chainResult()` gives the same chaining
    primitive, but adds a TypeScript runtime.
  - *cardano-cli*: one process per tx, needs the node socket for every build; the
    approach the user explicitly wants to move away from.

## D3. Replay through net-rs N2N, N2C second

- **Decision**: the first transport is N2N TxSubmission using `net-core`; N2C follows.
- **Rationale**: N2N lets one process hold connections to several peers and release to
  them at the same instant, which is what cross-node conflict experiments need. Its
  limit: TxSubmission has no reply for a mempool rejection, so over N2N we only see that
  a tx id was offered and whether its body was requested. Whether it was admitted comes
  from observation (db-sync, node logs). N2C gives the node's accept/reject and reason,
  which is why it remains a requirement (FR-019). It also
  turns the existing `net-cli submit` path (`net-rs/net-cli/src/submit.rs`) into a real
  submitter by feeding it manifest CBOR instead of random bytes. net-rs has no N2C client
  yet.
- **Alternatives considered**: N2C only, as tx-firehose does. Simpler, but one node per
  socket, so no simultaneous multi-node release.

## D4. Keep tx-firehose and tx-centrifuge for background load

- **Decision**: `txflow` does not replace them.
- **Rationale**: tx-firehose (cardano-node `leios-prototype`, `bench/tx-firehose`) is
  already wired into the proto-devnet, dozen-devnet and Antithesis setups and tags txs
  with a colour (metadata label 1022) for `mempool-monitor`. tx-centrifuge
  ([cardano-node#6494](https://github.com/IntersectMBO/cardano-node/pull/6494), closed
  unmerged but used for Leios testnet baseline load; final repo location not verified)
  adds multi-workload rate control. Neither has named transactions or conflicts. Running
  them underneath a `txflow` experiment gives realistic background load and stays
  comparable with other teams' runs.

## D5. Materialise offline from a chain-context file

- **Decision**: materialise reads era, protocol parameters and funding UTxOs from a file
  and does no network I/O. Submitting funding is a separate `fund` step.
- **Rationale**: building fees and min-UTxO needs protocol parameters, and the funding
  UTxOs must be known; net-rs has no LocalStateQuery client (it is TCP-only). Taking them
  as a file keeps materialise deterministic and testable in CI, and lets every error
  (including fee shortfalls) surface before anything is spent. A one-off query is an
  acceptable dependency on `cardano-cli`/Ogmios/db-sync; wrapping every tx in
  `cardano-cli` is what the user wants to avoid.
- **Alternatives considered**: query and submit funding inside materialise (rejected:
  errors in later txs would surface after funds were already locked in sources).

## D6. Design ideas borrowed

| Source | What we take |
|---|---|
| cardano-node `bench/tx-generator` (`Cardano.Benchmarking.Script.Types`) | Pattern vocabulary (`Split`, `SplitN`, `NtoM`, `Sequence`, `Take`); "dump to file, replay later" (`DumpToFile`) |
| Hydra `hydra-cluster` `Dataset` | Materialised output as a file of pre-signed txs per client |
| Bitcoin Core `MiniWallet` (also in warnet) | Builder granularity: `create_self_transfer_multi`, `create_self_transfer_chain`, conflicts by spending one UTxO twice |
| cooked-validators (Tweag, v10) | Adversarial variants as transforms over a baseline trace ("double-spend twin of tx X"; apply somewhere/everywhere). Follow-up, not MVP |
| Atlas (GeniusYield) | One scenario, several backends (emulator or privnet) |
| spamoor (ethpandaops) | Named, parameterised scenarios; several per run; funded child wallets from one key; `blob-conflicting --conflict-delay` |
| Aptos transaction-emitter | Workload taxonomy with conflicting vs non-conflicting as a first-class knob |
| `trix test` | `[[expect]]` assertions after a run |
| SundaeSwap `sundae-contracts/lucid/main.ts` | Reference order→scoop flow for `order_batch` |

## D7. Import from db-sync

- **Decision**: the importer reads cardano-db-sync.
- **Rationale**: the project already runs a Leios-aware db-sync for Musashi, and db-sync
  holds inputs, outputs, scripts, datums and metadata in joinable tables. Mainnet db-sync
  has the same schema for importing existing dApps.
- **Limitation**: chain data only shows what was included. Lost races and never-included
  orders are absent, so imported conflict rates are lower bounds.

## Other tools checked and not used

Dolos (devnet backend, possible future target), Oura (possible chain observer), Balius
(could host a batcher bot), Yaci DevKit (fast local devnet), Lucid Evolution, Mesh,
Scalus, Aiken, Helios, cardano-client-lib, Ogmios, Kupo, cardano-testnet, plutus-simple-model,
clb, tx-fuzz, flood, CometBFT load test. None provides an abstract multi-transaction
workload description. CIP-57 blueprints type validators and datums, not transaction
shapes. No CIP or paper found that defines a UTxO workload graph format.

## Not verified

tx-centrifuge's final location; whether a tx3 TRP resolver accepts references to
unconfirmed outputs (D1 avoids depending on it); whether a Leios node accepts a
Conway-encoded payment in the Dijkstra era (D2 spike); Lucid Evolution and cardano-client-lib
chaining API names; whether the SundaeSwap scooper source is public.
