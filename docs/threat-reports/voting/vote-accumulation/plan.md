# Vote-accumulation (phantom-vote flood) → unbounded `seenVotes` / `pointStates` growth

**Status:** RUN — CONFIRMED (2026-09-03). This document began as a pre-run plan; §1–§10 are the original design and the **RESULTS — CONFIRMED** section at the end records the executed outcome. The consolidated findings live in [`report.md`](report.md).
**Date:** 2026-09-03
**Target:** cardano-node **w34** (`prototype-2026w34`; `11.1.0.164`, `ouroboros-consensus-3.0.1.0`) on the local proto-devnet
**Threat (external analysis):** *Valid remote votes accumulate without a bounded lifecycle (Major, Unresolved).* A peer holding a current committee key can sign votes for unlimited distinct, attacker-chosen ranking-block (RB) hashes. Admission checks committee membership + signature but NOT whether the target is a known, current announcement in the correct epoch/window. Each accepted vote stays in a process-wide `seenVotes` set; each new hash creates a `pointStates` entry; `LeiosVoteState` exposes no pruning (explicit GC FIXME). Cited Haskell source: `LeiosVoteState.hs:81-108`, `LeiosDemoTypes.hs:1162`, `Network/NodeToNode.hs:574`.

---

## 1. Objective + hypothesis

**Objective.** From net-rs (Piranha), sign and diffuse valid committee votes over a stream of *distinct, fabricated* RB hashes that correspond to no real announcement, and measure whether the three Haskell committee nodes grow memory monotonically with the number of distinct votes accepted.

**Hypothesis.** Because a Leios vote signature binds *only* the 32-byte RB hash (no EB identity, no slot, no epoch — see §2), and admission does not check the hash against a known current announcement, each distinct fabricated hash net-rs signs will be (a) accepted, (b) retained forever in `seenVotes`, and (c) allocate a fresh `pointStates` entry. Observable as **monotonic RSS growth on all three nodes that tracks the cumulative distinct-vote count**, with no plateau while armed and no reclaim after the votes stop — versus a flat/bounded RSS under honest voting.

**Non-goal / safety.** This is a memory-exhaustion primitive. We measure the *slope* (RSS per distinct vote) at modest rates and extrapolate; we do **not** drive any node to OOM. See §7.

## 2. Mechanism (what a vote is, what is signed, why growth is unbounded)

### 2.1 Wire/logical vote

`Vote` is three fields (`leios-tools/shared-rs/consensus/src/types.rs:181`):

```rust
pub struct Vote {
    pub announcing_rb_hash: [u8; 32],
    pub voter_id: u16,          // compact index into the shared committee registry
    pub vote_signature: Vec<u8>, // 48-byte BLS12-381 min-sig G1 signature
}
```

There is **no** EB hash, slot, or epoch on the wire — "the election id is resolved from the RB, not carried" (`types.rs:175-180`).

### 2.2 What a committee member signs

The deployed-node construction (`leios-tools/shared-rs/bls/src/lib.rs:228-243`):

```rust
fn rb_hash_signable(announcing_rb_hash: &[u8;32]) -> [u8;34] { 0x58, 0x20, ‖ hash }   // CBOR byte string
fn sign_rb_vote(sk, announcing_rb_hash) = sk.sign(rb_hash_signable, CARDANO_VOTE_DST, &[])
// CARDANO_VOTE_DST = b"BLS_SIG_BLS12381G1_XMD:SHA-256_SSWU_RO_POP_"  (min-sig PoP DST, NO augmentation)
```

Mirrors ouroboros-consensus `LeiosDemoTypes.signLeiosVote` (comment at `bls/src/lib.rs:186-206`). **The entire signed message is the RB hash.** Nothing ties the signature to a real announcement, a real EB, the current slot, or the current epoch. Therefore any 32-byte value is a legally-signable target for a holder of a current committee BLS key plus a `voter_id` in the registry — no knowledge of any real RB is required.

### 2.3 Why distinct hashes ⇒ unbounded growth (Haskell side)

Per the external analysis (source not in our repos — treat as the target's black box): admission verifies committee membership + signature, then unconditionally inserts into the node-wide `seenVotes` set and creates a `pointStates` entry for each new hash, with no epoch/window check and no GC (`LeiosVoteState.hs:81-108`, `LeiosDemoTypes.hs:1162`). Exact-duplicate votes are suppressed, so the signer must use **distinct** hashes to force new entries. Each accepted vote also incurs signature verify + map insert + STM commit + relay to downstream peers — CPU/relay amplification on top of the memory growth.

net-rs's own outbound side corroborates the "distinct hashes" requirement: `LeiosStore::inject_votes` dedups by `(announcing_rb_hash, voter_id)` and only gossips genuinely-new votes (`leios-tools/net-rs/net-core/src/store/leios_store.rs:392-424`). So a fresh hash per vote is exactly what produces a fresh wire notification on our side too.

### 2.4 Diffusion path (net-rs already reaches the Haskell nodes this way)

`emit_vote` builds a `Vote` and sends `NetworkCommand::InjectLeiosVotes{votes}` (`net-rs/net-node/src/consensus/leios/mod.rs:785-806`) → coordinator `store.inject_votes(votes, None)` (`.../multi_peer/coordinator.rs:1160`) → `LeiosNotification::Votes` pushed to all peers. `inject_votes` has **no known-election gate**, so fabricated-hash votes are offered to the committee exactly like honest ones — and honest net-rs votes already land on the Haskell side (we've observed `LeiosVoteAcquired`/`LeiosCertified`).

## 3. Capability assessment — can net-rs drive this today?

**Not with an existing knob; a small, mechanical wiring is required (≈1 new file + 4 edits, all mirroring `announce-flood`).**

- The only vote control today is `VotePolicy::{Honest, Abstain(reason)}` (`control.rs:168`), surfaced by the `lazy-voter` action. There is **no** flood/phantom-vote variant.
- The honest emitter `LeiosConsensus::emit_vote` (`consensus/leios/mod.rs:753`) is reached only via `LeiosEffect::EmitVote` from the honest state machine, which gates on: saw the announcement → fetched + validated the EB → committee-eligible → inside the voting window. So today net-rs signs only hashes of RBs it actually saw. It will not sign an arbitrary attacker-chosen hash.
- **But** the actuation seam is already 95% built: `emit_vote` takes an arbitrary `announcing_rb_hash`, signs it via `self.vote_signer.sign_rb_vote`, and injects it. We need a path that calls the sign+inject step over fabricated hashes, bypassing the election gate.

### 3.1 Wiring sketch (`vote-flood`, mirrors `announce-flood` one-for-one)

`announce-flood` is the exact template — it emits `count` fabricated messages per tick decoupled from stake/production. The five hook points:

1. **Leaf action** — new `leios-tools/shared-rs/consensus/src/behaviour/actions/vote_flood.rs` (copy `announce_flood.rs`):
   ```rust
   pub struct VoteFlood { pub count: u32 }
   impl LeafAction<..> for VoteFlood {
       fn contribute(&mut self, _ctx, out) -> Status { out.leios.vote_flood_count = self.count.max(1); Status::Running }
       fn set_param(&mut self, field, value) { if field=="count" { self.count = value.as_integer()... } }
   }
   ```
   Register in `behaviour/actions/mod.rs`.
2. **Control field** — add `pub vote_flood_count: u32` to `LeiosControl` (`control.rs:102-149`, default `0` = honest; mirrors `announce_flood_count:148`). Update the `default_is_the_honest_node` test.
3. **ActionSpec** — add `#[serde(rename="vote-flood")] VoteFlood { #[serde(default="default_vote_flood_count")] count: u32 }` to `registry.rs` (mirror `AnnounceFlood:117-122`).
4. **Dispatch** — arm in `behaviour/tree/actions.rs build_action`: `ActionSpec::VoteFlood{count} => Box::new(catalogue::VoteFlood{count:*count})` (mirror `:125-128`).
5. **net-node actuation** — in the tick loop of `net-rs/net-node/src/main.rs` (adjacent to the announce-flood block `:834-865`, but **without** the `fake_announce`/stake predicate — voting needs no stake, only a committee key + registry seat), add:
   ```rust
   let vf = control.leios.vote_flood_count;
   if leios && vf > 0 {
       let count = vf.min(MAX_VOTE_FLOOD /* e.g. 10_000 */);
       consensus.emit_phantom_votes(count).await;   // new method, §3.2
   }
   ```
   Plus a `.bt` source (`behaviours/attacks/vote-flood.bt` + `behaviours/lib/vote-flood.bt`) and `make configs` in `behaviours/` to emit `net-rs/net-cluster/behaviours/vote-flood.toml`.

### 3.2 New net-node method (the only genuinely new logic)

Add to `LeiosConsensus` (`consensus/leios/mod.rs`, next to `emit_vote:753`) a gate-free sibling:

```rust
pub async fn emit_phantom_votes(&mut self, count: u32) {
    let node_id = self.state.node_id.clone();
    let Some(voter_id) = self.state.elections.voter_index(&node_id) else { return };  // need a registry seat
    let Some(signer) = self.vote_signer.as_ref() else { return };                     // need the BLS key
    let mut votes = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let mut h = [0u8; 32];
        self.rng.fill(&mut h);                    // FRESH distinct hash each iteration
        let sig = signer.sign_rb_vote(&h).to_vec();
        votes.push(Vote { announcing_rb_hash: h, voter_id, vote_signature: sig });
    }
    let _ = self.commands.send(NetworkCommand::InjectLeiosVotes { votes }).await;
}
```

This reuses `sign_rb_vote` and `InjectLeiosVotes` verbatim — the same code path honest votes take — differing only in that the hashes are fabricated and the election gate is skipped. (An RNG field or a `StdRng` from entropy is the only new dependency; the block producer already carries one, `production.rs:230`.) A per-tick cap (`MAX_VOTE_FLOOD`) prevents the flood monopolising net-rs's own tick loop, exactly as announce-flood caps at 10,000 (`main.rs:847`).

**Effort:** one new leaf file, one new method, four ~2-line edits, one `.bt` + `make configs`. No protocol, codec, or key changes. `cargo test -p net-node` and the `behaviours/` round-trip test (`make -C behaviours test`) must stay green.

## 4. The exact attack

Once wired: net-rs, holding the proto-devnet committee BLS key (configured via the node's key path, loaded at `main.rs:301-307`) and its registry `voter_id`, emits `count` freshly-randomised, individually-signed votes per slot tick to the three Haskell committee peers (`:4001/:4002/:4003`). Each vote is valid (committee key + correct DST/message form) but names an RB hash that was never announced. Rate = `count × ticks/sec`. Sweep `count` to trace RSS-vs-vote-count slope.

## 5. Measurement

### 5.1 Signals

| Signal | Source | How |
|---|---|---|
| Node RSS (KB) | OS | `ps -o rss= -p $(pgrep -f 'cardano-node run')` per node, sampled every 5-10 s (`node.pid` is a shell-wrapper PID, not the `cardano-node` process — see the report's PID note) |
| Distinct votes injected | net-rs | `count × elapsed_ticks` — the flood path `emit_phantom_votes` emits **no** `VTBundleGenerated` (that event fires only for honest `emit_vote`, `consensus/leios/mod.rs:798`); cross-check against the `behaviour: emitting phantom vote flood` log-line count |
| Votes accepted (Haskell) | `node.log` | count inbound-vote / `LeiosVoteAcquired`-class lines in `$WORKING_DIR/node{1,2,3}/node.log` |
| `seenVotes`/`pointStates` cardinality | — | **No direct metric in our repos** (internal Haskell state). Inferred black-box from RSS slope vs cumulative distinct votes. If cardano-node exposes an EKG/Prometheus gauge for the Leios vote store, scrape it — *unconfirmed on this build*, check first. |

Legend: `RSS` = resident set size (physical memory), reported by `ps` in KB. `pointStates`/`seenVotes` = the two Haskell node-wide vote containers named in the analysis.

### 5.2 Procedure

1. Confirm devnet healthy and net-rs honest (baseline). Record RSS of all three nodes for ~5 min → establishes honest slope (should be ~flat; honest votes are bounded by real elections).
2. Arm `vote-flood` at a chosen `count`. Sample RSS + cumulative vote count every 5-10 s for a fixed window (e.g. 10-15 min or until a hard RSS budget, whichever first — §7).
3. Disarm. Continue sampling ~5 min to see whether RSS reclaims (it should **not**, given "no GC").
4. Repeat for a `count` sweep (§8).

### 5.3 Pass/fail

- **PASS (threat confirmed):** RSS on all three nodes rises monotonically and roughly linearly with cumulative distinct votes while armed, with no plateau, and does **not** fall after disarm — i.e. a positive, sustained RSS-per-vote slope well above the honest-baseline noise band.
- **FAIL / bounded (threat mitigated on this build):** RSS plateaus while armed, or reclaims after disarm — evidence of a cap or GC. Record the plateau level and the vote count at which it appears.
- **Control:** the honest baseline (or a `lazy-voter` run) over the same duration must stay flat/bounded; the attack's slope must be clearly separated from it.

## 6. Reproduction commands

```bash
# --- build (one-time, after wiring §3) ---
make -C behaviours test && make -C behaviours configs      # regenerate net-cluster/behaviours/*.toml (adds vote-flood.toml)
cd net-rs && cargo test -p net-node && cargo build --release -p net-node -p net-cluster
# restart the proto-devnet net-rs so it picks up the new binary + behaviour
#   (Haskell nodes untouched; do NOT touch genesis/stake — devnet guardrail)

WORKING_DIR="${WORKING_DIR:-$HOME/leios-proto-devnet}"

# --- baseline RSS sampler (run in a side terminal for the whole experiment) ---
while :; do
  ts=$(date -u +%FT%TZ)
  for i in 1 2 3; do
    rss=$(ps -o rss= -p "$(cat "$WORKING_DIR/node$i/node.pid")" 2>/dev/null | tr -d ' ')
    echo "$ts,node$i,$rss"
  done
  sleep 10
done | tee rss.csv

# --- arm the flood (attack API on :9100) ---
curl -X POST http://127.0.0.1:9100/api/attack -H 'content-type: application/json' \
 -d '{"id":"vote-flood","selection":{"kind":"all"},
      "parameters":[{"toml_path":"behaviours.vote-flood.spec.count","value":50}]}'

# --- live retune during the run (sweep) ---
curl -X PUT http://127.0.0.1:9100/api/attack/parameters -H 'content-type: application/json' \
 -d '{"toml_path":"behaviours.vote-flood.spec.count","value":500}'

# --- watch acceptance on the committee ---
grep -Ec "LeiosVote|vote" "$WORKING_DIR"/node{1,2,3}/node.log

# --- disarm (stop injecting; observe whether RSS reclaims) ---
curl -X POST http://127.0.0.1:9100/api/attack/stop
```

(The `POST /api/attack` + `PUT /api/attack/parameters` shapes and `:9100` match the oversized-EB report, [`../../eb-attacks/oversized-eb/report.md`](../../eb-attacks/oversized-eb/report.md) lines 158–168.)

## 7. Safety / recovery

- **Cap the run.** Set a hard RSS budget per node (e.g. baseline + 2 GB, or ≤ 50-60 % of machine RAM) and stop the moment any node crosses it. The sampler above makes this easy to watch; wire an auto-`stop` if desired.
- **Start small, extrapolate.** Measure RSS-per-vote at `count = 1, 10, 50` before going higher. The point of the experiment is the *slope*, not exhaustion — do not run to OOM.
- **Recovery.** `POST /api/attack/stop` halts injection. If RSS stays pinned (expected — no GC), restart the affected Haskell node from its DB: `kill $(cat $WORKING_DIR/node$i/node.pid)` then relaunch it (via `proto-devnet/scripts/run.sh`, under `caffeinate -i` per the node-sleep-crash note). **No respin, no genesis/stake change** (devnet guardrail). net-rs restart alone is enough to stop the flood.
- **Blast radius.** Only the three local Haskell BP processes and net-rs are involved; nothing leaves the machine.

## 8. Sweep matrix

| Run | Behaviour | `count`/tick | Duration | Purpose |
|---|---|---|---|---|
| C0 | honest (none) | — | 5-10 min | baseline RSS slope (control) |
| C1 | `lazy-voter` | — | 5-10 min | control: committee still votes without us |
| A1 | `vote-flood` | 1 | 5 min | per-vote RSS delta, low noise |
| A2 | `vote-flood` | 10 | 5 min | linearity check |
| A3 | `vote-flood` | 50 | 10 min | slope at moderate rate |
| A4 | `vote-flood` | 500 | ≤ budget | high rate — stop at RSS budget |

Legend: `count`/tick = fabricated distinct votes signed+injected per slot tick; run stops at the RSS budget regardless of duration.

## 9. Open questions

1. **Production threshold unmeasured.** How many distinct votes to a visible OOM? Derive from the measured RSS-per-vote slope; do not measure directly.
2. **Haskell `seenVotes` dedup key — `(voter, hash)` or hash-only?** net-rs dedups `(announcing_rb_hash, voter_id)` (`leios_store.rs:405`). If the Haskell node dedups by hash alone, one `voter_id` and distinct hashes suffice (our plan); if by `(voter, hash)`, still fine with a single key. Confirm by re-signing one hash and checking whether the second copy is suppressed.
3. **Epoch/voting-window binding.** The signature binds *only* the RB hash — no slot/epoch (§2.2). So the sole gate is admission's (claimed-absent) known-current-announcement check. Confirm votes for hashes far outside any live window are still retained (the crux of "unbounded").
4. **Does retention require a resolvable `voter_id`?** `emit_vote`/`emit_phantom_votes` need a registry seat to produce a weight-resolvable vote. Whether the Haskell side inserts into `seenVotes`/`pointStates` *before* resolving `voter_id → weight` (i.e. whether an unresolved voter still grows the containers) is unknown — check by injecting with an out-of-registry `voter_id`.
5. **Relay amplification.** Do the committee nodes re-gossip accepted fabricated votes to each other (multiplying the memory hit across the mesh, as the analysis's "repeated relay work" implies)? Watch cross-node vote counts in the three logs.
6. **CPU vs memory.** Separate the signature-verify/STM cost from the container growth — if RSS is bounded but CPU saturates, the primitive is still a DoS, just a different one.

## 10. File-grounded pointers

| Concern | File:line |
|---|---|
| Vote logical type | `leios-tools/shared-rs/consensus/src/types.rs:181` |
| What is signed (RB-hash-only, DST, no aug) | `leios-tools/shared-rs/bls/src/lib.rs:228-243` (+ Haskell note `:186-206`) |
| Honest emitter (sign + inject) | `net-rs/net-node/src/consensus/leios/mod.rs:753-807` |
| Vote diffusion (no election gate) | `leios-tools/net-rs/net-core/src/store/leios_store.rs:392-424`; `.../multi_peer/coordinator.rs:1160` |
| Only existing vote knob (`VotePolicy`) | `leios-tools/shared-rs/consensus/src/behaviour/tree/control.rs:104,168` |
| Wiring template (`announce-flood`) | leaf `.../behaviour/actions/announce_flood.rs`; control `control.rs:148`; spec `registry.rs:117`; dispatch `tree/actions.rs:125`; actuation `net-node/src/main.rs:834-865` |
| BLS key loaded on devnet | `net-node/src/main.rs:301-307`; `proto-devnet/scripts/run.sh:128` |
| Node pids / logs / attack API | `$WORKING_DIR/node{1,2,3}/node.{pid,log}`; aggregator+attack API `:9100` (`run.sh:29,220-257`) |
| Analysis-cited Haskell (target black box) | `LeiosVoteState.hs:81-108`, `LeiosDemoTypes.hs:1162`, `Network/NodeToNode.hs:574` |

Legend: `RB` = ranking block (the announced Praos base block a vote names); `BLS` = the committee signature scheme (BLS12-381 min-sig); `DST` = the BLS domain-separation tag; `API` = the attack-control HTTP API on `:9100`; `File:line` cites this repo unless prefixed `../leios-tools/` (sibling path dep) or naming a Haskell source (the target's black box).

---

## RESULTS — CONFIRMED (2026-09-03, proto-devnet, cardano-node w34)

Wired `vote-flood` end-to-end (leios-tools leaf/control/spec/dispatch + net-node
`emit_phantom_votes` + per-tick actuation + `hollow-eb`-style behaviour) and ran it.

**One delivery gotcha, then it lands:** at `count=1000`/tick the vote batch (~90 KB)
exceeded the LeiosNotify `SIZE_LIMIT` (65,535 B) → `ExceededSizeLimit`, votes dropped.
At `count=200`/tick (~18 KB) the votes land.

**Verdict: confirmed.**
- **Admission gap:** committee `LeiosVoteAcquired` went 12 → **129,012 per node** — the
  Haskell nodes accept votes for fabricated RB hashes never announced (no
  known-target / epoch / window check).
- **Unbounded growth:** node RSS climbed monotonically with the vote count —
  160 MB @ 12 votes → 254 @ 38.8k → 273 @ 61.8k → 294 @ 98k → 310 @ 129k
  (~1.2 KB/vote; intermediate flat steps are GHC page-chunk allocation, trend is up).
- **No lifecycle:** after disarm (votes stopped 70 s), RSS held at 310/305/308 MB —
  **zero reclaim**. Retained votes are never pruned (the "GC FIXME").
- **Liveness:** nodes stayed up; slow memory-exhaustion vector, not a crash.

**Caveat:** RSS is a lossy proxy for `seenVotes`/`pointStates` cardinality (no Haskell
metric); growth-tracks-count + zero-reclaim is the black-box confirmation.

**Recovery:** disarm; RSS does not reclaim on its own, so restart the Haskell nodes
from their DBs under `caffeinate` to release the accumulated vote state (no respin).

**Stronger flood (future):** chunk `emit_phantom_votes` into ≤64 KB batches to exceed
the ~700-vote/tick LeiosNotify cap and drive faster exhaustion.
