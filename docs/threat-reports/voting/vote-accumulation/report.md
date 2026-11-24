# Vote-accumulation (phantom-vote flood) — findings report

**Status:** CONFIRMED (implemented + run). Two live runs against the local proto-devnet.
**Date:** 2026-09-03
**Target:** cardano-node **w34** (`prototype-2026w34`, `ouroboros-consensus-3.0.1.0`) — 3 Haskell committee nodes (ports 4001/4002/4003) on the local proto-devnet.
**Driver:** net-rs (Piranha), holding the proto-devnet committee BLS key + a registry seat.
**Threat (external analysis):** *Valid remote votes accumulate without a bounded lifecycle (Major, Unresolved).* A holder of a current committee key can sign votes for unlimited distinct, attacker-chosen ranking-block (RB) hashes. Admission checks committee membership + signature but **not** whether the target is a known, current announcement. Each accepted vote is retained forever in a node-wide `seenVotes` set with a fresh `pointStates` entry; there is no GC.

The design + mechanism analysis is in [`plan.md`](plan.md); this report records the implementation state and the measured results.

---

## 1. Verdict

**Confirmed on this build.** The three Haskell committee nodes accept BLS-valid votes over fabricated RB hashes that were never announced, retain them with no lifecycle, and grow memory monotonically with the cumulative distinct-vote count. Nodes stay up — this is a slow memory-exhaustion vector, not a crash.

## 2. What is signed, and why growth is unbounded

A `Vote` is three fields — `announcing_rb_hash: [u8;32]`, `voter_id: u16`, `vote_signature` (48-byte BLS12-381 min-sig) — with **no** EB hash, slot, or epoch on the wire (`../leios-tools/shared-rs/consensus/src/types.rs:181`). The signed message is **only** the 32-byte RB hash (`../leios-tools/shared-rs/bls/src/lib.rs:228-243`). Therefore any 32-byte value is a legally-signable target for a committee-key holder — no knowledge of any real RB is required. Distinct hashes force distinct retained entries on the receiver; duplicates are suppressed, so the signer uses a fresh random hash per vote.

## 3. Implementation

Wired end-to-end following the `announce-flood` template (five hook points). Behaviour name: **`vote-flood`**.

| Hook | Location | Change |
|---|---|---|
| Leaf action | `../leios-tools/shared-rs/consensus/src/behaviour/actions/vote_flood.rs` | new — sets `leios.vote_flood_count = count` |
| Control field | `../leios-tools/.../behaviour/tree/control.rs:154` | `vote_flood_count: u32` (default 0 = honest) |
| ActionSpec | `../leios-tools/.../behaviour/registry.rs:126` | `#[serde(rename="vote-flood")]` |
| Dispatch | `../leios-tools/.../behaviour/tree/actions.rs:131` | `ActionSpec::VoteFlood → catalogue::VoteFlood` |
| Actuation | `net-node/src/main.rs:618` | per-tick call, capped at `MAX_VOTE_FLOOD = 10_000` |
| Emitter | `net-node/src/consensus/leios/mod.rs:818` | new `emit_phantom_votes(count)` |

Legend: `EB` = endorser block; `RB` = ranking block (the announced Praos base block a vote names); `BLS` = the committee signature scheme (BLS12-381 min-sig); `ActionSpec` = the behaviour-registry enum variant; a `../leios-tools/` path is the sibling path-dep crate, all other paths are this repo.

`emit_phantom_votes` signs `count` fresh random hashes with the committee BLS key and injects them via the same `NetworkCommand::InjectLeiosVotes` path honest votes take — differing only in that the hashes are fabricated and the honest election gate is skipped. It **chunks into ≤500-vote batches** (`VOTES_PER_BATCH`) to stay under the LeiosNotify wire `SIZE_LIMIT` (65,535 B); a single un-chunked batch of ~1,000 votes had been rejected wholesale as `ExceededSizeLimit` in the first run.

Behaviour sources: `behaviours/{attacks,lib}/vote-flood.bt` → generated `net-rs/net-cluster/behaviours/vote-flood.toml` via `make -C behaviours configs`.

> **Build dependency:** the leaf/control/spec/dispatch changes live in the sibling **leios-tools** checkout (path dep). They must be committed/merged on leios-tools `main` for this repo's CI to compile — see §8.

## 4. Run A — acceptance + first confirmation

At `count=200`/tick (~18 KB batches, land cleanly):

- **Admission gap:** committee `LeiosVoteAcquired` went **12 → 129,012 per node** — votes for fabricated, never-announced RB hashes are accepted (no known-target / epoch / window check).
- **Unbounded growth:** RSS climbed with the vote count — 160 MB @ 12 votes → 254 @ 38.8k → 273 @ 61.8k → 294 @ 98k → 310 @ 129k (**~1.2 KB/vote**).
- **No lifecycle:** after disarm (70 s), RSS held at 310/305/308 MB — **zero reclaim**.

## 5. Run B — maximum sustained pressure (`count=10_000`/tick)

Armed at the per-tick cap and sampled RSS of the three `cardano-node` processes once per minute for ~17 min. Baseline = pre-flood.

| t (min) | node1 MB | node2 MB | node3 MB | node1 Δ | node2 Δ | node3 Δ |
|---|---|---|---|---|---|---|
| 0 (base) | 418 | 304 | 299 | +0 | +0 | +0 |
| 1 | 514 | 334 | 318 | +96 | +29 | +19 |
| 3 | 625 | 369 | 359 | +207 | +65 | +60 |
| 5 | 657 | 416 | 401 | +239 | +111 | +102 |
| 6 | 678 | 420 | 402 | +260 | +116 | +103 |
| 8 | 555 | 368 | 359 | +137 | +63 | +60 |
| 10 | 555 | 373 | 433 | +137 | +69 | +134 |
| 13 | 593 | 443 | 416 | +175 | +139 | +117 |
| 15 | 673 | 439 | 426 | +255 | +135 | +127 |
| 16 | 694 | 440 | 429 | +276 | +136 | +129 |
| 17 | 686 | 401 | 469 | +268 | +97 | +170 |

Legend: `RSS` = resident set size (physical memory) of the `cardano-node` process; Δ = change vs. the pre-flood baseline; node1/2/3 = the three committee nodes (pids 68354/68361/68367).

### Growth rate (linear fit over the armed window)

| node | slope | R² |
|---|---|---|
| node1 | +4.9 MB/min | 0.19 |
| node2 | +5.3 MB/min | 0.52 |
| node3 | +6.8 MB/min | 0.75 |
| **combined** | **+17.0 MB/min ≈ +1.0 GB/hour** | — |

Legend: slope = fitted RSS growth per minute; R² = fraction of variance the straight line explains (1.0 = perfect fit).

### Reading the shape

RSS is a **sawtooth around a rising floor**, not a clean line: GHC allocates in page chunks and periodically returns pages to the OS on major GC, so per-minute deltas swing ±40–120 MB (e.g. node1 dropped −123 MB at t=8, then recovered to a fresh high of 694 MB by t=16). The **retained vote set itself never shrinks** — the reliable signal is the rising *minimum* after each GC, which climbs on all three nodes across the run. The low R² on node1 reflects its larger GC dips, not an absence of growth.

## 6. Prediction — memory growth vs. time

Growth is **linear in cumulative distinct votes accepted**, which at a fixed flood rate is linear in time:

- **Aggregate ≈ +1 GB/hour** across the three committee nodes at `count=10_000`/tick — the number to plan around.
- Predict the **floor**, not any single reading; RSS jitter (±100 MB) will mispredict a specific minute.
- The rate **scales with the flood rate** (`count × ticks/sec`): 5× the count ≈ 5× the slope, until net-rs's own per-tick BLS signing saturates.
- The most portable predictor is the **per-vote slope from Run A (~1.2 KB/vote)**. Fitting RSS-floor-vs-cumulative-vote-count (rather than vs. clock) removes the tick-rate and GC-phase confounds and gives a cleaner time-to-OOM projection; recommended before pushing toward exhaustion.

## 7. Caps and how to increase pressure

Two limits, only one binding:

- **`MAX_VOTE_FLOOD = 10_000`/tick** (`net-node/src/main.rs:619`) — the real ceiling. Run B was pinned here; a `PUT` above 10,000 is clamped back with a warn log. Raising it requires a one-line code change + `cargo build --release -p net-node` + net-rs restart. The practical limit then becomes net-rs's own CPU: `emit_phantom_votes` signs `count` BLS signatures **synchronously in the tick loop**, so an over-large `count` monopolises net-rs's event loop (the reason the cap exists). Bump moderately (≈25–50k) and watch tick health.
- **LeiosNotify `SIZE_LIMIT` (64 KB) / `VOTES_PER_BATCH = 500`** — **not** a ceiling. Chunking already lets any `count` reach the peers; this only shaped Run A's first attempt.

Other axes (no cap bump): more committee seats / a second signer (only helps if the Haskell side dedups by `(voter,hash)`); faster slots = more ticks/sec (a devnet genesis param — **off-limits** per the devnet guardrail).

## 8. Recovery + caveats

- **Disarm:** `curl -X POST http://127.0.0.1:9100/api/attack/stop`. Stops new votes; accumulated state is **not** reclaimed (no GC).
- **Release memory:** restart the affected Haskell nodes from their DBs under `caffeinate -i` — **no respin, no genesis/stake change** (devnet guardrail). net-rs restart alone stops the flood.
- **RSS is a lossy proxy** for `seenVotes`/`pointStates` cardinality (no Haskell metric exposed on this build); growth-tracks-count + zero-reclaim is the black-box confirmation.
- **Haskell internals are a black box** — `seenVotes`/`pointStates` and the absent GC are the external analysis's claim (`LeiosVoteState.hs:81-108`, `LeiosDemoTypes.hs:1162`), inferred here, not read directly.
- **Blast radius:** three local BP processes + net-rs; nothing leaves the machine.

## 9. Reproduction

```bash
# arm at max sustained pressure
curl -X POST http://127.0.0.1:9100/api/attack -H 'content-type: application/json' \
 -d '{"id":"vote-flood","selection":{"kind":"all"},
      "parameters":[{"toml_path":"behaviours.vote-flood.spec.count","value":10000}]}'

# retune live
curl -X PUT http://127.0.0.1:9100/api/attack/parameters -H 'content-type: application/json' \
 -d '{"toml_path":"behaviours.vote-flood.spec.count","value":200}'

# watch acceptance on the committee
grep -Ec "LeiosVote|vote" "$WORKING_DIR"/node{1,2,3}/node.log

# RSS sampler (real cardano-node pids, one per minute)
for pid in <node1> <node2> <node3>; do ps -o rss= -p "$pid"; done

# disarm
curl -X POST http://127.0.0.1:9100/api/attack/stop
```

Note: on this devnet `$WORKING_DIR/node$i/node.pid` holds a shell-wrapper PID (a ~0.5 MB `bash`), not the `cardano-node` process — the real node is that wrapper's child. Resolve it by name for a correct RSS sample (`pgrep -f 'cardano-node run'`, or the child of the pidfile PID) rather than reading the pidfile directly.
