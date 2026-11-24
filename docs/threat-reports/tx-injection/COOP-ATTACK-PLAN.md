# TX Magazine dispatching — architecture note (#1068)

> **UPDATE — fire mechanism migrated.** The slot-gated fire below was originally
> a `fire_at_slot` action param on `tx-flood`, forwarded through
> `DynamicConfig.fire_at_slot` to a gate in the net-node tx generator. That
> duplicated the behaviour tree's slot Condition, so it was replaced: `magazine-flood`
> now gates the tx-flood leaf with `Condition(cardano.current_slot >= env.trigger_slot)`,
> and the campaign fires by setting that env var live via
> `set-param scope = "env" key = "trigger_slot"`. The `fire_at_slot` field and its
> net-node plumbing were removed from this repo. Wherever the design below says
> `fire_at_slot`, read `env.trigger_slot`; the timing behaviour (hold until slot X,
> then burst) is unchanged. `tx-flood` still carries an unused `fire_at_slot` field
> in the public `leios-tools` crate.

## Context

Issue [input-output-hk/ouroboros-leios#1068](https://github.com/input-output-hk/ouroboros-leios/issues/1068)
extends the magazine tooling so 10 piranha nodes (PIR1..10) fire *cooperative,
honest* txs that spend Red Team funds, in two shapes (related, shuffled),
staged and replenished across the shoal with one command each — and without
managing ssh keys.

**This note now sits on top of PR #74 (remote node control).** #74 already
builds the remote control plane we originally scoped (a Nomad-like pull model),
so this plan drops that work and reduces to what #74 does *not* cover: moving
magazine bytes, a prepare phase, and a slot-gated fire.

Standing decisions (unchanged):

- **Shoal is outbound-only** → distribution and control are **pull**, not push.
  (#74 implements exactly this.)
- **Attack is a timed volley at slot X** → the barrier resolves to a single
  fire-slot; slot-granularity alignment, sub-slot precision out of scope.
- **Attacks are behaviour trees** → the shoal attack is a BT delivered via
  `load_tree`; the magazine is a *resource that BT needs*, fetched in a prepare
  step before the BT arms.

## Baseline: what PR #74 delivers (verified against its branch)

- **HTTP pull control plane.** Nodes poll
  `GET /api/node-interface/get-commands?node_id=<id>` (~500 ms) on net-cluster's
  existing server; net-cluster enqueues per-node `ServerCommand`s
  (`net-node/src/remote_control.rs`, `net-cluster/src/server.rs`). Outbound-only,
  no ssh, no new listener.
- **Attacks stay behaviour trees.** `ServerCommand::DynamicConfigUpdate` carries
  the same `bt_control` payload the stdin path used; the node applies it into the
  BT runtime — `LoadTree` → `bt_runtime.load_tree_in_place`, `SetParam`,
  `GoHonest` (PR `net-node/src/main.rs:71-105`, dispatched at `:1607`). The BT
  drives production every slot (`bt_runtime.tick`, `:733`).
- **Config-declared registry.** Remote nodes are `[[remote_nodes]]`
  (`node_id`/`address`/`stake`) in the cluster config; the config *is* the
  registry. Remote node starts with just `--node-id X --aggregator-addr URL`.
- **Session reset.** `ServerCommand::Restart` (soft, in-place) + `cluster_id`
  change detection re-baselines a node.

What #74 does **not** cover (our remaining scope):

- **No artifact transfer.** The channel carries JSON commands, not bytes;
  `tx_source` is still only a *path string*. The magazine bytes have no way onto
  a remote box.
- **No ready signal.** No per-command ack; liveness is inferred from telemetry.
  A fire barrier ("all N ready → pick X") needs an explicit ready event.
- **No prepare phase.** Commands actuate the BT immediately; there is no
  fetch-a-resource step before arming.
- **`after` cursor is a stub** (PR `remote_control.rs:87-91` TODO) — the node
  re-reads the whole window each poll; cross-restart resync is incomplete.

Ledger facts (unchanged, still ours to honor):

- Ledger `maxTxSize` = 16384 bytes (`configs/musashi-shelley-genesis.json:67`) —
  ~20k txs ⇒ ~320 MB/node worst case, ~6 MB typical (300 B txs). `MAX_TX_SIZE
  = 2_500_000` in `net-core` is a separate wire cap. The node treats blobs as
  **opaque**; validity is entirely the offline builder's job — a bad tx is
  rejected at the first honest Haskell node and never diffuses.

## Architecture (on top of #74)

Control + BT actuation are #74. We add an **artifact channel** and a **prepare
phase**; the fire trigger and slot-gate ride the existing BT command path.

```
            net-cluster (#74 command queue  +  NEW artifact endpoint + barrier)
              ▲ get-commands poll (#74)      │ NEW: GET magazine bytes
              │ NEW: ready signal            ▼ (HTTP, node pulls)
   PIR1 ─ PIR2 ─ … ─ PIR10   (net-node, #74 remote_control poll loop)
     per node: poll get-commands → load_tree (BT configured)
             → PREPARE: GET magazine, reopen tx_source idle → emit READY
             → fire cmd sets fire_at_slot=X → SlotClock gate → burst
```

**Prepare** — a BT that needs a magazine declares it; on receiving `load_tree`
the node enters prepare: GET its magazine (stream to disk, resumable via the
existing `.cursor`), `reopen` the tx-source idle (`tx_rate=0`), then emit
**ready**. Attacks that need no resource skip prepare.

**Fire** — net-cluster waits for all N ready, picks `X = current_slot + margin`
(margin ≥ poll interval), and dispatches a fire command
(`DynamicConfigUpdate{fire_at_slot=X}`). Each node's slot-gate holds the replay loop until
`current_slot() >= X`, then bursts. Slot-granularity alignment; sub-slot out of
scope.

**Replenish** — magazines are spent on fire. Next campaign re-arms: new
`load_tree`/prepare → GET fresh bytes → `reopen` (no reboot) → ready.

## Lifecycle (state machine)

The happy-path flow above hides the edges (a stalled pull, a node dying in
`Ready`, a cluster restart mid-campaign). The state machine makes them explicit
and gives one lifecycle every attack shares — a resource-needing attack passes
through `Preparing`, a resourceless one skips it. It is also the net-node ↔
net-cluster contract: who drives each transition.

### net-node

```
                 load_tree (needs resource)      magazine loaded, ready emitted
   Idle ───────────────► Configured ─────► Preparing ───────────────► Ready
    ▲  \                     │ load_tree                                 │
    │   \ load_tree          │ (no resource)                    fire_at_slot=X
    │    \(no resource)      ▼                                           ▼
    │     └───────────────► Ready ◄─────────────────────────────────  Firing
    │                                                                    │
    └──────────── go_honest / Restart / cluster_id change ───────────────┤
    ▲                                                              EOF    ▼
    └──────────────────────── (replenish) ◄───────────────────────────  Done
```

| From | Event | To | Notes |
|---|---|---|---|
| Idle | `load_tree` + `FetchMagazine` | Preparing | attack needs a magazine |
| Idle | `load_tree`, no `FetchMagazine` | Ready | prepare is a no-op; armed immediately |
| Preparing | fetched + hash-verified + cursor reset + `Restart` re-opens idle | Ready | then emits `ready` event; `tx_rate=0` |
| Preparing | fetch fail / timeout | Idle | report error; await next command |
| Ready | `DynamicConfigUpdate{fire_at_slot=X}` | Firing | slot-gate holds until `current_slot() >= X` |
| Firing | magazine exhausted (cursor EOF) | Done | |
| Done | replenish (`FetchMagazine`) | Preparing | fresh bytes + cursor reset + `Restart`, no reboot |
| any | `go_honest` / `Restart` / `cluster_id` change | Idle | #74 session reset; re-arm from scratch |

Legend — *Idle*: polling `get-commands`, honest, no attack armed; *Configured*:
`load_tree` received, resource need assessed; *Preparing*: fetching + loading the
magazine, `tx_rate=0`; *Ready*: magazine loaded idle, `ready` event emitted,
awaiting fire; *Firing*: `fire_at_slot` set, slot-gate then bursting; *Done*:
magazine exhausted; *`load_tree`/`FetchMagazine`/`go_honest`/`Restart`*: #74
`ServerCommand`s (`FetchMagazine` is the one new variant); *`fire_at_slot`*: new
`DynamicConfigUpdate` field; *`ready`*: new telemetry event on `/events`, scoped
by `cluster_id`; *`cluster_id` change*: #74's cluster-restart signal.

### net-cluster (per campaign)

| From | Event | To | Notes |
|---|---|---|---|
| Idle | operator starts campaign | Arming | dispatch `load_tree` to the shoal |
| Arming | all N nodes report `Ready` | Armed | the **barrier** |
| Arming | ready timeout with < N ready | Arming/Firing | design choice: keep waiting (default) or fire the ready subset (operator opt-in) |
| Armed | pick `X = current_slot + margin` | Firing | dispatch `DynamicConfigUpdate{fire_at_slot=X}` to all |
| Firing | nodes report Done / drain elapsed | Spent | |
| Spent | replenish | Arming | rebuild + re-dispatch `load_tree` |
| any | cluster (re)start | Idle | fresh `cluster_id`; all nodes drop to Idle and re-arm |

Legend — *Arming*: `load_tree` dispatched, awaiting `ready` from all N; *Armed*:
all N `Ready`, safe to fire; *Firing*: fire-slot dispatched; *Spent*: magazines
consumed, awaiting replenish; *barrier*: the all-`Ready` gate that authorizes
fire; *N*: shoal size (10).

### Edge cases (design intent)

- **Stalled `Preparing`** (slow ~300 MB pull): the cluster barrier waits; it
  never fires a partial shoal unless the operator opts into the ready-subset
  timeout. A node that fails the fetch drops to `Idle` and is re-armed next poll.
- **Node dies in `Ready`/`Firing` and reconnects:** it restarts in `Idle`
  (local runtime state is lost), so the cluster must re-dispatch `load_tree`;
  the barrier will not show it `Ready` until it re-prepares.
- **`cluster_id` change mid-campaign:** every node self-resets to `Idle` (#74),
  so the whole shoal re-arms — no half-fired campaign survives a cluster restart.
- **Ready signal is the missing #74 piece:** the barrier depends on a `Ready`
  event #74 does not emit today (scope item 3). Without it the cluster cannot
  know the shoal is armed — this is why the ready signal is on the critical path.

## Scope on top of #74

Provided by #74 (reuse, do not rebuild): the node poll loop
(`remote_control.rs`), `ServerCommand`/`net-node-interface`, `get-commands`,
`[[remote_nodes]]` registry, BT actuation via `load_tree`, `Restart`.

New work (this note):

1. **Cooperative magazine shapes** — `tools/build-shoal-magazines.sh` + `tools/shoal.json`.
   Shell + `cardano-cli` generalizing `prep-magazine.sh`; `magazine.py` stays the
   pure packer/id tool. `related` and `shuffled` — both DONE (offline-verified;
   live `cardano-cli` path untested here).
2. **Artifact channel** — a `GET /api/node-interface/magazine?node_id=` endpoint
   (range/stream for ~300 MB) on net-cluster + the node-side fetch. Belongs
   beside `get-commands` (one of the agreed #74 changes below).
3. **Prepare phase + ready signal** — a fetch-resource step between `load_tree`
   and BT arm, plus a `ready` **telemetry event** (on #74's existing outbound
   `/events` channel, scoped by `cluster_id`) the barrier consumes. No new ack
   path — see "#74 changes required".
4. **Slot-gate on firing** (`net-node/src/mempool.rs`) — a `fire_at_slot` gate
   before the Poisson loop using the `SlotClock` already held.
5. **Cursor-reset for replenish** (`net-node/src/tx_source.rs`) — #74's `Restart`
   already re-opens `tx_source` (`main()` loops `run_node()` → `FileTxSource::open`,
   PR `main.rs:181-207,495`), so a full reopen API is **not** needed. Replenish =
   fresh bytes at the path + cursor reset + `Restart`. Only the cursor-reset is
   new.
6. **net-cluster barrier + Makefile** — track per-node ready (from `/events`),
   compute X, dispatch fire; `make magazines`/`campaign`/`fire`/`replenish`.

### #74 changes required (agreed)

Building on #74 needs four small additions to its surfaces — agreed — everything
else is our new logic layered on top:

1. **`fire_at_slot: Option<u64>` on `DynamicConfigUpdate`** (`net-node-interface/
   src/config.rs`, mirrors the existing `tx_rate` at `:25`) — must reach the tx
   generator's watch channel the same way `tx_rate` does. Carries the fire
   trigger; no new command.
2. **A fetch trigger** — a `ServerCommand::FetchMagazine { handle }` variant
   (his enum, `:67`) or a `magazine` field on `BaseConfigUpdate`. Tells the node
   which magazine to pull. A variant is cleaner.
3. **`GET /api/node-interface/magazine?node_id=`** (`net-cluster/src/server.rs`)
   — a streamed, Range-capable download beside `get-commands`. Purely additive.
4. **A `ready` telemetry event** (`net-node/src/telemetry.rs`) — emitted at the
   end of prepare, `{node_id, cluster_id, handle}`, on the existing `/events`
   sink. The barrier counts one per node scoped to the current `cluster_id`.

Reopen comes free via #74's `Restart` (item 5 above). The only genuinely
protocol-touching item is #2; #1/#4 are field/event additions, #3 is a new route.

## Implementation steps (ordered)

#74-independent work first (proceeds regardless of merge state), #74-dependent
Rust after.

**1. Related builder + shuffle pairing** (`tools/build-shoal-magazines.sh`,
`tools/shoal.json`)
- `build-shoal-magazines.sh` (related shape): one shared fan-out of `N*W` outputs (node `n`
  owns indices `[n*W, n*W+W)`, disjoint → no cross-node conflict), per-lane chains
  packed into `<node_id>.mag` (+ `.txids`). Self-contained = related.
- **Roster from #74, not `shoal.json`.** The node list is authoritative in #74's
  `[[remote_nodes]]`; the builder takes it via `--nodes` (the Makefile extracts it
  from the cluster config). `shoal.json` shrinks to the **shuffle pairing only**
  (consumed by step 2) — no duplicate node array, so the two can't drift.
- Offline-verified: `bash -n`, arg validation, fan-out index disjointness. Live
  `cardano-cli` path unverified here (no testnet).

**2. Shuffled builder** (`tools/build-shoal-magazines.sh --shape shuffled`) — DONE
- **Genuine cross-magazine spend dependency**, built in two layers so the ring
  pairing has no build cycle: *producers* (node Y, lane l → 1 tx spending
  fan-out[Y,l], lives in Y's magazine) then *consumers* (node X's chain roots
  spend `producer[shuffle[X], l]`, live in X's magazine). Each node's magazine =
  its producers (referenced by its ring-predecessor) + its consumers (referencing
  its partner). Pairing read from `shoal.json` via `--shuffle`.
- Offline-verified: `bash -n`, `--shape` validation, partner-in-roster check,
  10-node ring pairing parse. Live `cardano-cli` path unverified here.
- Funding note: shuffled needs `(1 + chain-depth)` fees/lane (a producer + the
  consumer chain), vs `chain-depth` for related — raise `--per-lane-ada` if a lane
  exhausts.

*(Steps 3-6 are #74-dependent — do them on this branch against #74's interfaces;
they compile only with #74's crates present.)*

**3. Slot-gate** (`net-node/src/mempool.rs`, `net-node-interface/src/config.rs`) — DONE
- `fire_at_slot: Option<u64>` added to `DynamicConfigUpdate` + `DynamicConfig`
  (#74 change #1, mirrors `tx_rate`), merged in `apply()`, `None` default at all
  construction sites. `spawn_tx_generator` takes `mut slot_clock` and, at loop
  top, holds on `select!(slot tick, config change)` while `current_slot() < X`;
  once reached it's a no-op and firing proceeds.
- Verified: 2 unit tests (holds before X / releases at X), full `cargo test -p
  net-node` (187) green; no new clippy/fmt in the added lines. Built against
  `leios-tools` `main`.

**4. Cursor-reset for replenish** (`net-node/src/tx_source.rs`) — DONE
- No reopen API needed: #74's `Restart` already re-opens `tx_source`
  (`main.rs:181-207,495`). Added `pub fn reset_cursor(path)` — removes the
  `<magazine>.cursor` sidecar so a fresh magazine at the same path replays from
  the start. Replenish = fetch fresh bytes + `reset_cursor` + `Restart`.
- Verified: unit test (consume → replace bytes → reset → replays replacement in
  full; idempotent when absent); `cargo test -p net-node` 188 green; additions
  clippy/fmt-clean. `#[allow(dead_code)]` until step 5 wires the call.

**5. Artifact channel + prepare + ready** — DONE
- **Trigger = `ServerCommand::FetchMagazine { handle }`** (`net-node-interface`,
  #74 change #2). Handled in `net-node/src/main.rs`: `prepare_magazine()` GETs the
  endpoint → `std::fs::write` to the `tx_source` path → `reset_cursor` → validates
  the bytes parse (peek one record) → emits `MagazineReady` → restarts **forced
  idle** via `WithNewConfig{node_params_set:["transactions.tx_rate=0.0"]}` so the
  node holds even if its own config armed a non-zero rate — readiness then truly
  means "armed and held", and only the fire barrier releases it. Attacks that send
  no `FetchMagazine` skip prepare.
- **Bytes = `GET /api/node-interface/magazine?node_id=<id>[&handle=]`**
  (`net-cluster/src/server.rs`, #74 change #3), beside `get-commands`, served from
  `magazines_dir` (`net-cluster/magazines`, where `make campaign` writes
  `<node_id>.mag`). Path-traversal-guarded (rejects `/`, `\`, `..`). Test covers
  serve / 400 / traversal / 404.
- **Ready = `NodeEvent::MagazineReady { node, handle }`** (`telemetry.rs`, #74
  change #4) on the existing `/events` sink; the barrier (step 6) consumes it.
- **Verified:** all 3 crates build; net-node 188 + net-cluster 249 tests green;
  additions clippy/fmt-clean.
- **v1 simplifications (follow-ups):** magazine is buffered in memory both sides
  (no streaming / HTTP Range yet); `prepare_magazine` blocks the node's select
  loop during the fetch; `ready` carries `{node, handle}` — `cluster_id` scoping
  is added with the barrier (step 6). Handle scheme still open ("Artifact
  identity") — endpoint accepts either `handle` or defaults to `<node_id>.mag`.

**6. Barrier + Makefile** — DONE, then **`/fire` retired** (superseded by the
manoeuvre-BT follow-up item 6): the barrier now runs inside the `magazine-campaign`
manoeuvre (arm → `sensor.magazine_ready>=1.0` → `set-param fire_at_slot`), not
`POST /fire`. `/prepare`, the `magazine_ready` set, and `MagazineReady` remain.
- net-cluster (`server.rs`): `magazine_ready` set populated from `MagazineReady`
  events in `receive_events`; the surviving endpoint beside `get-commands` is
  `POST /api/node-interface/prepare {nodes}` (enqueues `FetchMagazine` per node,
  clears stale readiness, and rejects nodes not registered in the command window).
  The fire barrier is no longer a `POST /fire` endpoint (retired): it now runs
  inside the `magazine-campaign` manoeuvre (arm `magazine-flood`, wait
  `sensor.magazine_ready >= 1.0`, then `set-param fire_at_slot`), driven by
  `POST /api/manoeuvre` (see step 6). Tests cover prepare-dispatch, readiness
  gating, and unknown-node rejection.
- The tx-injection Makefile (internal): `magazines` (build-shoal-magazines), `campaign`
  (prepare), `fire` (`POST /api/manoeuvre` running `magazine-campaign`, which
  computes the fire slot cluster-side — no `FIRE_SLOT`), `replenish` (rebuild +
  re-prepare), driving the REST API via curl/jq.
- **Verified:** net-cluster 250 tests green; additions clippy/fmt-clean.
- **v1 caveats:** endpoints are operator-triggered (Makefile), not an auto-barrier;
  the caller computes `FIRE_SLOT` (cluster doesn't derive current slot); the
  magazine flood is driven directly via `DynamicConfigUpdate{tx_rate, fire_at_slot}`
  on the tx generator — no separate `.bt` needed for the basic shoal flood;
  `ready` is scoped by node only (add `cluster_id` scoping when campaigns overlap).

**End-to-end gate:** local net-cluster + 2 nodes → load_tree → prepare → ready →
fire@X → both burst in the same slot in `cluster-events.jsonl`. Then 10 nodes.

## Open decisions (resolve in build, not blocking)

- **Shuffled = deliberate cross-magazine dependency** (#1068's definition).
  `pirX`'s txs spend `pirY`'s tx outputs, so they validate only once `pirY`'s
  have diffused. That is the *point*; fired simultaneously it forces cross-node
  tx diffusion (a tx arriving before its parent is a temporary missing-input
  orphan). `related` stays independent; only `shuffled` carries cross-refs.
- **Big-magazine handling.** Baseline: HTTP streamed/range download to disk +
  resumable `.cursor`. Revisit chunking only if ~320 MB pulls prove slow.
- **Success metric / intended effect — undecided.** #1068 specifies cooperative
  honest txs but not the degradation demonstrated (mempool pressure, diffusion
  stress, EB effects, or plain load). Drives verification; settle before the
  end-to-end run.

## bd breakdown (epic → subtasks, linked to #1068)

- epic: TX magazine dispatching (on #74)
  - build-shoal-magazines.sh `related` shape (+ UTXO model) — DONE
  - build-shoal-magazines.sh `shuffled` shape — DONE
  - net-node: slot-gate on firing (`fire_at_slot`) — DONE
  - net-node: cursor-reset for replenish (Restart re-opens tx_source) — DONE
  - net-cluster + net-node: artifact channel (`GET magazine`) — DONE
  - net-node: prepare phase + `ready` signal — DONE
  - net-cluster: fire barrier (all-ready → X → dispatch) — DONE
  - Makefile: magazines / campaign / fire / replenish — DONE
  - (reuse from #74: poll loop, ServerCommand, registry, BT actuation)

## Verification

- **Builder:** `magazine.py info`/`ids` on related+shuffled mags; assert
  self-contained vs cross-referenced graphs; sign+submit a handful to a testnet
  relay, confirm accepted (not rejected at first honest node).
- **Slot-gate:** unit test in `mempool.rs` — no txs before X, burst at X
  (injected fixed-genesis `SlotClock`).
- **Cursor-reset:** unit test in `tx_source.rs` — after reset, a replaced magazine
  at the same path replays from the start (reopen itself is #74's `Restart`).
- **End-to-end (local first):** net-cluster + 2 nodes → load_tree → prepare →
  ready → fire@X; both burst in the same slot in `cluster-events.jsonl`.
- **Success metric: TBD** — see Open decisions.

## Local smoke test

The internal `run-shoal-smoke.sh` script drives the whole manoeuvre-BT pipeline
on a local net-cluster with **garbage** magazines (net-rs local has fake
validation, so garbage diffuses) — no testnet, no `cardano-cli`. It builds the
release binaries, generates 10 garbage magazines, launches the idle 10-node
cluster (`tx_rate=0`), then prepares each node (FetchMagazine → stage the
magazine, restart idle, report `MagazineReady`) → waits for the barrier
`sensor.magazine_ready == 1.0` → runs the `magazine-campaign` manoeuvre (arm
`magazine-flood` → set `fire_at_slot`). It asserts from the **per-node logs**
(the `TXGenerated` telemetry event is unused and the aggregator drops slotless
events, so the merged JSONL is not a reliable fire counter): **zero txs
generated before fire**, then the shoal fires and drains. Takes ~1–2 min; it
self-tears-down on exit. Expected tail:

```
SMOKE PASS: barrier held (0 tx pre-fire), then 10/10 nodes fired the magazine (5000 txs).
```

**What it proves — and does not.** It exercises prepare → ready → **readiness
barrier** → arm → fire → drain across the whole shoal. It does **not** currently
exercise the coordinated *future-slot* gate: the local cluster spawns nodes with
no `--config` file, so a node boots at the default `genesis_time_unix = 0`, and
the param-only magazine-prepare restart re-merges onto that default
(`net-node/src/main.rs:193`), leaving the node's clock at genesis 0. The
orchestrator computes `fire_at_slot` from the base config's real genesis, so it
lands in the node's past and the gate opens immediately — nodes fire on *arm*
(near-simultaneous, but not slot-aligned). On the real fleet, where nodes are
launched *with* their `--config`, boot genesis is real and
survives the prepare restart, so the gate holds to the shared slot as designed.
To make the local smoke a true slot-alignment test, preserve the node's genesis
across the FetchMagazine restart (`net-node/src/main.rs:1688`).

## Note on process

Branch `kp/tx-magazine-dispatching` is cut from #74's branch. When #74 merges,
replant onto `main`:
`git rebase --onto main <#74-base> kp/tx-magazine-dispatching`
(where `<#74-base>` is the commit this branch was forked from; this repo
squash-merges, so #74's commits get a new identity on `main`).

Tracked in `bd`, specified by this note — not speckit. The remaining work is a
shoal builder over `prep-magazine.sh`, a slot-gate, a cursor-reset, and an
artifact/prepare/ready layer on top of #74's control plane.

## Follow-up: drive the campaign through the existing manoeuvre BT

**Context.** Steps 5–6 built the fire flow as bespoke REST (`POST /prepare`,
`POST /fire`) driving `DynamicConfigUpdate` directly — no behaviour tree. That
works and is slot-gated, but it sits *beside* net-cluster's attack tooling
instead of inside it, and doesn't match the reviewer's "attacks are BTs" model
(PR #73 comment). The code shows net-cluster's **manoeuvre BT** + attack-control
already provide most of what a coordinated campaign needs, so the magazine attack
can become a first-class, UI-visible, composable attack for far less than a
from-scratch BT.

**Two BTs, don't conflate them.**
- **net-cluster manoeuvre BT** (`net-cluster/src/manoeuvre/`) — *orchestration*.
  Leaf actions `PartitionAction`/`HealAction`/`HoldAction`/`ArmAttackAction` emit
  a `ManoeuvreEffect` (`reconcile.rs:43`) that *dispatches* to nodes. Reuses the
  `shared_consensus::behaviour::tree` framework with a `ClusterContext`.
- **net-node `bt_runtime`** — *actuation*. Interprets `ActionSpec::TxFlood`
  (leios-tools `shared-rs/consensus/src/behaviour/registry.rs:134`) →
  `MempoolControl.tx_flood_rate` (`tree/control.rs:159`) → arms the generator
  (`net-node/src/main.rs:798`).

`fire_at_slot` must live in the **node** vocabulary (leios-tools); the manoeuvre
BT can only *dispatch* an attack the node BT actuates.

**Existing machinery to reuse (verified):**
- Arm on a group: `ArmAttackAction` → `ArmIntent{group, attack_id, indices}`
  (`reconcile.rs:30`) → `load_tree <attack_id>.bt` (`main.rs:546-549`).
- Cluster knows the slot: `read_slot_params(&base_config)` → genesis + slot_ms
  (`main.rs:767`), so it can compute `X = current_slot + margin`.
- Live param inject across nodes: `params::set_at_path` (`main.rs:494`) +
  `PUT /api/attack/parameters` → `set_param` (`server.rs:1231`) — the coordinated
  absolute-slot inject.
- Manoeuvre conditions gate on `sensor.<name>` / `slot` / `elapsed_ticks`
  (`conditions.rs`).

**Mapping the campaign onto existing machinery:**

| Step | Existing mechanism | New work |
|---|---|---|
| arm the flood | `ArmAttackAction`/`start_attack` → `load_tree magazine-flood.bt` | the `.bt` file |
| compute fire slot X | `read_slot_params` + margin (cluster-side) | — reuse |
| inject X to all nodes | `set_param fire_at_slot=X` (existing REST param path) | see caveat |
| node holds until X | generator slot-gate (step 3, done) | `fire_at_slot` in node vocab (leios-tools) |
| prepare (fetch magazine) | *no manoeuvre effect for resource-fetch* | keep `FetchMagazine` pre-step |
| barrier (all-ready) | manoeuvre `Condition sensor.magazine_ready >= 1.0` | add a `magazine_ready` sensor |

Legend — *arm*: dispatch an attack behaviour to a node group; *set_param*: live
BT-action-param update across nodes; *sensor*: a cluster-computed metric a
manoeuvre condition can read; *X*: the single absolute fire slot, identical
across the shoal.

**Decision: fully-BT.** The whole campaign runs as one manoeuvre `.bt` — arm →
gate on readiness → set the fire slot — all inside the manoeuvre/attack tooling.
This needs a small, *reusable* addition to the manoeuvre vocabulary (a live
`set_param` effect), and it retires the bespoke `/fire`. The self-contained
inject works because a manoeuvre leaf action's `contribute` gets the
`ClusterView`, which exposes the orchestrator slot (`context.rs:119`
`pub slot: u64`) — so `SetParamAction` computes `fire_at_slot = ctx.slot + margin`
live, and every node gets the same absolute X. (Hybrid — keep `/fire` cluster-side,
skip the effect — is the fallback if item 4 slips; footnote only.)

**Work items (ordered):**

1. **leios-tools — node vocabulary** (cross-repo, core; own PR on `main`) — DONE
   (control.rs/registry.rs/actions.rs/tx_flood.rs + net-node actuator; tests green;
   uncommitted on leios-tools `main`, needs a branch + PR).
   - `MempoolControl` (`tree/control.rs`): add `fire_at_slot: Option<u64>`
     (default `None`; extend the default-off test).
   - `ActionSpec::TxFlood` (`registry.rs`) + `actions/tx_flood.rs`: carry an
     optional `fire_at_slot`, set `control.mempool.fire_at_slot`.
   - net-node actuator (`main.rs:~812`): in the tx-flood arm block that does
     `dyn_tx.send_if_modified(|c| c.tx_rate = rate)`, also set
     `c.fire_at_slot = control.mempool.fire_at_slot`. (`DynamicConfig.fire_at_slot`
     already exists from step 3.)
   - Test: `TxFlood{rate, fire_at_slot}` → `MempoolControl.fire_at_slot`; actuator
     writes it into `dyn_tx`.

2. **`net-cluster/behaviours/magazine-flood.toml`** — `spec = {kind="tx-flood",
   rate=R}` (fire slot set live, not baked). Auto-discovered by `/api/attacks`,
   arm-able via `start_attack`/`ArmAttackAction`. — DONE (+ discovery test in
   `attacks.rs`).

3. **`magazine_ready` sensor** (`net-cluster/src/sensors.rs`) — DONE. Activity
   sensor `value = ready/expected`; new `SensorInputs.{magazine_ready_count,
   shoal_size}` fed by `gather_sensor_inputs` from the step-6 ready set + a
   `shoal_size` state field set at `prepare`. Auto-listed in `all_sensor_names`
   (vocabulary JSON re-blessed); a manoeuvre `Condition sensor.magazine_ready
   >= 1.0` gates. Tests: sensor fraction + full suite green.

4. **`SetParamAction` manoeuvre effect** (`net-cluster/src/manoeuvre/`) — DONE.
   `ManoeuvreEffect.set_params: Vec<SetParamIntent>` (`reconcile.rs`); a
   `SetParamAction` leaf whose `contribute` computes `value = ctx.slot +
   slot_margin` and emits a `SetParamIntent{group, behaviour_id, key, value,
   indices}`; `"set-param"` spec-parse + `CLUSTER_ACTION_KINDS`; threaded through
   `StepOutcome` with one-time `dedup_set_params`; dispatched in `main.rs` via the
   existing `param_tx`/`ParamDispatch` `set_param` path. Reusable beyond magazines.
   Tests: leaf slot-relative intent, spec-parse; vocabulary re-blessed.

5. **`magazine-campaign` manoeuvre** (`net-cluster/manoeuvres/magazine-campaign.toml`)
   — DONE: `Sequence[arm-attack magazine-flood → Selector[Condition
   sensor.magazine_ready>=1.0, hold] → set-param fire_at_slot slot_margin=20]` on
   the `shoal` group. Loads/compiles (the shipped-manoeuvre load test now checks
   `arm-attack` against real discovered attacks — a latent gap this fixed).

6. **Retire `/fire`; keep `FetchMagazine`** — DONE. Dropped `POST
   /api/node-interface/fire` + `fire_shoal`/`FireRequest` (superseded by items
   4–5); the barrier server-test became a prepare-dispatch test. `POST /prepare`
   stays (FetchMagazine + records `shoal_size`). Makefile `fire` now starts the
   `magazine-campaign` manoeuvre (`POST /api/manoeuvre`) instead of the bespoke
   endpoint. All net-cluster tests green.

Sizing: ~1.5–2.5 days + a cross-repo leios-tools PR cycle. Steps 1 (leios-tools)
and 3 (sensor) are the two mediums; 2/5 are small; 4 is medium but reusable.

**Kept from steps 3–6:** the generator slot-gate (`fire_at_slot`), `reset_cursor`,
`FetchMagazine` + `prepare_magazine`, and `MagazineReady` (now surfaced as the
`magazine_ready` sensor). The slot-gate itself never moves — still in the
generator, now fed by the BT instead of a raw `DynamicConfigUpdate`.

**Verification:** unit-test the leios-tools `TxFlood`→`MempoolControl.fire_at_slot`
mapping and the net-node actuator propagation; unit-test the `magazine_ready`
sensor fraction; unit-test `SetParamAction` emits `fire_at_slot = ctx.slot + margin`
from a fixed `ClusterView`; drive a local net-cluster + 2 nodes through the
manoeuvre (arm → ready-gate → set fire slot) and confirm both burst in the same
slot in `cluster-events.jsonl`.
