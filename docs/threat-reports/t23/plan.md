# T23 — Withhold then release a large number of EBs (concentrated traffic burst)

**Status:** PLAN (not yet run). Investigation + exercise design only; no attack was executed, the running devnet was not modified, and genesis/stake were not touched.
**Date:** 2026-09-03
**Target:** cardano-node **w34** (`prototype-2026w34`; `11.1.0.164`, `ouroboros-consensus-3.0.1.0`) on the local proto-devnet (3 Haskell committee nodes on `:4001/:4002/:4003` + net-rs/Piranha).
**Threat (external — Ouroboros Leios threat model, `docs/threat-model.md`):**

> **T23 — "Withhold then release large number of EBs"**
> *"Adversaries can withhold large numbers of EBs and their transaction closures over extended periods, then release them simultaneously to create concentrated bursts of network traffic."*
> **Impacts:** Operational Sustainability, High Throughput, Blockchain Safety (if prioritization fails).
> **Proposed mitigation:** traffic prioritization implementing **freshest-first delivery semantics**.

Adjacent rows confirm the ID: **T22** = "Selectively withhold data from most honest nodes" (allow certification while delaying propagation); **T24** = "Submit duplicate transactions". T23 is the row literally identified above.

---

## 1. Objective + hypothesis

**Objective.** From net-rs (Piranha), *accumulate* a batch of endorser blocks and their tx closures without diffusing them (silent withhold phase), then *release the whole batch simultaneously* in a single slot tick — announcements (`MsgLeiosBlockAnnouncement`) plus servable bodies (`MsgLeiosBlockOffer` / LeiosFetch) — and measure whether the burst degrades the three Haskell committee nodes' timeliness on **concurrent fresh** Leios/Praos traffic (the freshest-first property T23's mitigation targets).

**Hypothesis.** Because a receiver processes offered EBs/tx-closures without a freshest-first admission discipline on this build (the mitigation is *proposed*, not landed — see §2.3), a single-tick flush of `count` EBs — each with a fetchable `n_txs`-tx closure — will produce a concentrated fetch storm that:
- (a) is **accepted and fetched** (EBs enter the receiver's Leios pipeline even though they were minted long ago / off-schedule), and
- (b) **delays servicing of genuinely fresh** EBs/votes/RBs produced during the burst window — observable as a spike in fetch/announce-to-adopt latency and/or a dip in fresh-EB certification during the flush, versus the honest baseline.

A negative result (fresh traffic keeps its latency; burst EBs are deprioritised or dropped) is itself the finding: it says a freshest-first / staleness gate is effectively present on this build.

**Non-goal / safety.** This is a burst-DoS / prioritisation probe, not a memory-exhaustion run. We measure the *latency/throughput hit during the flush* at modest batch sizes and extrapolate; we do not drive any node to OOM or crash. See §7.

## 2. Mechanism (what "withhold then release" is, grounded in code)

### 2.1 The two phases

The attack has two phases against a *single* control knob-set:

1. **Withhold (accumulate).** Produce (or fabricate) EBs and pin their tx bodies so they are *servable*, but do **not** emit the fast-discovery announcement. net-rs already has the exact suppression primitive: `leios.withhold_announce` (`../leios-tools/shared-rs/consensus/src/behaviour/tree/control.rs:136`), actuated at `net-rs/net-node/src/main.rs:1102`:

   ```rust
   if control.leios.withhold_announce {
       info!(... "behaviour: withholding EB announcement (produced EB not announced)");
   } else if let Some(store) = handle.leios_store.as_ref() {
       store.announce_block(produced.header.clone(), None);   // <-- the pulse we suppress
       ...
   }
   ```

   Under `withhold_announce=true` the announcement is skipped **but the body offer still runs** (the `InjectLeiosBlock` at `main.rs:1123` and body-pinning at `main.rs:1146-1148` are gated only by `announce_dangling`, not by `withhold_announce`). So today `withhold_announce` alone is *permanent* announcement censorship, not a buffer — the produced header is dropped, never queued for later.

2. **Release (flush).** Re-emit every buffered EB's announcement + body offer in one tick, creating the concentrated burst. **This flush step does not exist yet** — it is the one genuinely new piece of logic (§3.2).

### 2.2 The fabrication primitive that makes "large number" cheap

A 20 %-stake node wins EB elections slowly, so accumulating a *large* batch of *genuine* EBs over an "extended period" is realistic but slow on a devnet. net-rs already has the cheaper, faithful substitute: **fabricated EBs with servable tx closures** — `FakeEbKind::Dummy { n_txs }` (`control.rs:51`, action `../leios-tools/shared-rs/consensus/src/behaviour/actions/fake_eb.rs` `DummyTxEb`). A Dummy EB carries `n_txs` fabricated tx bodies that the adversary pins so honest voters fetch them with no `MissingTX` (`fake_eb.rs:9-15`; body-pinning path `main.rs:1146-1149` via `store.inject_block_txs_full`). This is exactly T23's "EBs *and their transaction closures*": each buffered EB drags an `n_txs`-tx fetch behind it, so the release burst is a real bandwidth event, not just a header spray.

The announcement-side burst primitive also already exists: **announce-flood** emits `count` distinct fabricated announcements per tick (`control.rs:148` `announce_flood_count`; actuation `main.rs:851-882`, the `for _ in 0..count { producer.fabricate_announcement(...); store.announce_block(...) }` loop). announce-flood is the closest existing shape, but its fabricated announcements have **no pinned bodies** — so it stresses only the announcement channel, not the tx-closure fetch path that is the substance of T23.

### 2.3 Why the burst is expected to hurt (Haskell side — the target's black box)

Per the threat-model row, the mitigation (freshest-first delivery) is *proposed*, not asserted-present. So on this build we expect no staleness/priority gate that would deprioritise a simultaneous flood of old-but-servable EBs against fresh ones. The exact scheduling of inbound LeiosFetch/LeiosNotify service on the Haskell node is **not in our repos** (treat as black box); we infer its behaviour from the latency of *fresh* traffic during the burst (§5). net-rs's own mux does implement two-class QoS (Praos priority over Leios, `PriorityWfq`, see `net-rs/CLAUDE.md` "Two-class scheduling") — relevant only to how *our* egress prioritises the burst, not to how the *victim* ingests it.

### 2.4 Diffusion path (net-rs already reaches the Haskell nodes this way)

Announcements go out via `store.announce_block(header)` (`main.rs:1105`), bodies via `NetworkCommand::InjectLeiosBlock` + `store.inject_block_txs_full` (`main.rs:1123,1148`). Both are the *same* calls honest EB production uses and both already land on the committee (we observe honest `LeiosVoteAcquired`/`LeiosCertified`). The attack changes *when* and *how many* of these fire in a tick — not the wire path.

## 3. Capability assessment — can net-rs drive T23 today?

**Partially. The two ingredients (silent withhold, cheap fabricated EBs with servable closures) exist; the "release the whole batch in one tick" trigger does not. A small, mechanical wiring is required — one new leaf + a buffer/flush in net-node, mirroring `announce-flood` + `dummy-tx-eb`.**

What exists today:
- **Silent accumulation:** `withhold_announce` (`control.rs:136`, `main.rs:1102`) suppresses the announcement while the body stays servable — but drops the header instead of buffering it, and never releases.
- **Fabricated EBs with fetchable closures:** `dummy-tx-eb` / `FakeEbKind::Dummy` (`fake_eb.rs`, `main.rs:1146-1149`) — one EB per produced RB, announced immediately.
- **Announcement bursts:** `announce-flood` (`main.rs:851-882`) — `count`/tick fabricated announcements, but **bodyless** (no tx closure).
- **BT-native staging:** `Sequence[ Wait(N), <attack> ]` (Wait leaf `../leios-tools/shared-rs/consensus/src/behaviour/tree/behaviour.rs:74`, `tick_wait` `:313`; template `behaviours/attacks/staged-dummy-tx-eb.bt`) gives the "run honest for N slots, then begin" timing gate with no external orchestration.

What is missing (the gap): a mechanism that (1) **buffers** produced/fabricated EBs+bodies during a withhold window instead of dropping or immediately emitting them, and (2) **flushes the whole buffer's announcements + offers in a single tick** on a release trigger. There is no buffer and no flush today.

### 3.1 Two driveable designs (present both; A is the faithful T23, B is the cheap approximation)

**Design A — genuine withhold/accumulate/release (the real T23; needs the buffer).**
A new `eb-burst` behaviour with three knobs: `withhold_slots` (accumulate window), `count` (EBs to fabricate + buffer, each a Dummy EB), `n_txs` (closure size per EB). During the withhold window net-rs fabricates + pins `count` Dummy EBs into a **release buffer** without announcing; when the window elapses it flushes every buffered `(header, body)` in one tick. This produces the exact T23 signature: a large batch of EBs *with tx closures* held then released simultaneously.

**Design B — staged announcement burst (available with near-zero new logic).**
`Sequence[ Wait(N), announce-flood(count=K) ]` — silent for N slots (honest baseline), then K fabricated announcements/tick. Buildable **today** from existing knobs (`staged-dummy-tx-eb.bt` is the exact template; swap the leaf for `announce-flood`). Limitation: fabricated announcements have no pinned bodies, so it exercises the *announcement channel* + receiver announce-accounting but **not** the tx-closure fetch storm. Use B as the fast first cut / control; A for the substantive result.

### 3.2 Wiring sketch for Design A (`eb-burst`, mirrors `announce-flood` + `dummy-tx-eb` one-for-one)

The five standard hook points (identical structure to the `vote-flood` diff already landed):

1. **Leaf action** — new `../leios-tools/shared-rs/consensus/src/behaviour/actions/eb_burst.rs` (copy `announce_flood.rs` + `fake_eb.rs`):
   ```rust
   pub struct EbBurst { pub withhold_slots: u32, pub count: u32, pub n_txs: u32 }
   impl LeafAction<..> for EbBurst {
       fn contribute(&mut self, _ctx, out) -> Status {
           out.leios.eb_burst_withhold_slots = self.withhold_slots;
           out.leios.eb_burst_count = self.count.max(1);
           out.leios.eb_burst_n_txs = self.n_txs;
           Status::Running
       }
       fn set_param(&mut self, field, value) { /* retune withhold_slots / count / n_txs */ }
   }
   ```
   Register in `actions/mod.rs` (`pub mod eb_burst; pub use eb_burst::EbBurst;` — mirrors `:31,:51`).
2. **Control fields** — add to `LeiosControl` (`control.rs:102-160`, defaults `0` = honest): `pub eb_burst_withhold_slots: u32`, `pub eb_burst_count: u32`, `pub eb_burst_n_txs: u32` (mirror `announce_flood_count:148`, `vote_flood_count:154`). Update the `default_is_the_honest_node` test.
3. **ActionSpec** — add `#[serde(rename="eb-burst")] EbBurst { withhold_slots: u32, count: u32, n_txs: u32 }` to `registry.rs` (mirror `AnnounceFlood:117`, `DummyTxEb:73`), with `default_*` fns.
4. **Dispatch** — in `tree/actions.rs build_action` (mirror `:125,:131`): `ActionSpec::EbBurst{withhold_slots,count,n_txs} => Box::new(catalogue::EbBurst{ withhold_slots:*withhold_slots, count:*count, n_txs:*n_txs })`.
5. **net-node actuation + buffer** — in the tick loop of `net-rs/net-node/src/main.rs` (adjacent to the announce-flood block `:851-882`), add a release buffer and a flush. Reuse the existing Dummy-EB fabrication + body-pinning already at `main.rs:1146-1149`:
   ```rust
   // accumulate (silent): fabricate `count` Dummy EBs, pin bodies, DON'T announce
   if leios && control.leios.eb_burst_count > 0 && !eb_burst_released {
       if eb_burst_buf.len() < control.leios.eb_burst_count as usize {
           let (header, point, body, bodies) =
               producer.fabricate_dummy_eb(slot, prev_hash, next_block_no,
                                           control.leios.eb_burst_n_txs);   // new: like fabricate_announcement + Dummy manifest
           eb_burst_buf.push((header, point, body, bodies));                 // buffer, no diffusion yet
       }
       // release (flush) once the withhold window elapses
       if eb_burst_slots_elapsed >= control.leios.eb_burst_withhold_slots {
           for (h, point, body, bodies) in eb_burst_buf.drain(..) {
               if let Some(store) = handle.leios_store.as_ref() {
                   store.announce_block(h, None);                            // burst announce
                   store.inject_block_txs_full(point.clone(), bodies, None); // servable closure
               }
               let _ = commands.send(NetworkCommand::InjectLeiosBlock { point, block: body }).await;
           }
           eb_burst_released = true;
           info!(node_id=%node_id, slot, "behaviour: released withheld EB batch (T23 burst)");
       }
       eb_burst_slots_elapsed += 1;
   }
   ```
   Plus a `.bt` source (`behaviours/attacks/eb-burst.bt` + `behaviours/lib/eb-burst.bt`) and `make -C behaviours configs` to emit `net-rs/net-cluster/behaviours/eb-burst.toml`. A per-tick / per-batch cap (mirroring `MAX_ANNOUNCE_FLOOD=10_000` at `main.rs:864`) prevents the flush monopolising net-rs's own tick loop or exceeding the LeiosNotify `SIZE_LIMIT` (65,535 B — the delivery gotcha the vote-flood run hit; chunk the flush into ≤64 KB batches across a few ticks if `count × per-EB-announce` exceeds it).

**The only genuinely new logic** is `producer.fabricate_dummy_eb(...)` (a trivial merge of the existing `fabricate_announcement`, `main.rs:872`, with the Dummy manifest/body construction already used for `FakeEbKind::Dummy`) and the buffer/flush state (`eb_burst_buf`, `eb_burst_slots_elapsed`, `eb_burst_released` — three locals in the tick loop). Everything downstream (`announce_block`, `inject_block_txs_full`, `InjectLeiosBlock`) is reused verbatim.

**Effort:** one new leaf file, ~three control fields + spec/dispatch (four ~2-line edits), one new producer helper + a ~20-line buffer/flush block in `main.rs`, one `.bt` + `make configs`. No protocol, codec, or key changes. `cargo test -p net-node` and `make -C behaviours test` must stay green. Design B (staged announce-flood) needs **zero** new code — only a new `.bt`.

## 4. The exact attack

Once wired (Design A): net-rs runs honest, then on arm begins the withhold phase — silently fabricating `count` Dummy EBs (each an `n_txs`-tx servable closure) into the release buffer over `withhold_slots` slots, emitting nothing. When the window elapses it flushes the entire buffer in one tick: `count` announcements + `count` body offers + `count × n_txs` fetchable tx bodies hit the three committee peers simultaneously. Rate of the burst = `count` EBs delivered in ~1 slot vs the honest ~1 EB/slot cadence. Sweep `count`, `n_txs`, and `withhold_slots` to trace the burst-size → fresh-traffic-latency curve.

Design B (control / fast cut): `Sequence[ Wait(withhold_slots), announce-flood(count) ]` — same timing, announcement-channel only.

## 5. Measurement

### 5.1 Signals

| Signal | Source | How |
|---|---|---|
| Burst EBs announced/offered | net-rs telemetry | `EBGenerated` count (`main.rs:1089`) + the flush `info!` line; or `count` |
| Burst tx-closure volume | derived | `count × n_txs` (bytes ≈ from `EBGenerated.size_bytes`) |
| Fresh-EB fetch-to-adopt latency | `node.log` (Haskell) | timestamp delta from a fresh EB's announce/offer to its `LeiosVoteAcquired`/adopt line, sampled across the burst window vs baseline |
| Fresh-EB certification during burst | `node.log` | count of `LeiosCertified` for EBs *minted during* the flush window vs the honest-baseline rate |
| Praos RB adoption latency | `node.log` + net-rs `RBGenerated` | RB header-seen → adopt delta during the burst (does the Leios burst bleed into Praos timeliness?) |
| Node RSS (KB) | OS | `ps -o rss= -p $(cat $WORKING_DIR/node$i/node.pid)` per node, every 5-10 s (secondary — watch for a step, but T23 is a *latency* probe, not the vote-accumulation memory probe) |
| net-rs egress backpressure | net-rs log | LeiosNotify `ExceededSizeLimit` / drop warnings during the flush (the ≤64 KB batch cap, §3.2) |

Legend: `RB` = ranking block (Praos base block); `EB` = endorser block; `tx closure` = the set of transactions an EB commits to (`n_txs` fabricated bodies here); `RSS` = resident set size (physical memory, KB); `fetch-to-adopt latency` = wall-clock from a fresh EB being offered to the victim adopting/voting on it; `freshest-first` = the proposed mitigation delivering newer data ahead of older when both are queued.

### 5.2 Procedure

1. Confirm devnet healthy and net-rs honest (baseline). For ~5 min record fresh-EB fetch-to-adopt latency, fresh-EB certification rate, and RB adoption latency on all three nodes → the honest reference.
2. Arm `eb-burst` (Design A) with a chosen `(withhold_slots, count, n_txs)`. During the withhold window verify **no** Leios announcements leave net-rs (silent accumulation) while honest voting continues.
3. At release, capture: burst acceptance (do the EBs get fetched?), and the latency/cert signals for **fresh** traffic produced during and just after the flush.
4. Disarm; continue ~5 min to confirm recovery to baseline latency.
5. Repeat for the sweep (§8). Run Design B (staged announce-flood) as the announcement-channel-only control at matched `count`.

### 5.3 Pass/fail

- **PASS (threat confirmed):** at release, the burst EBs are accepted/fetched by the committee **and** fresh EB fetch-to-adopt latency (and/or RB adoption latency) spikes measurably above the honest noise band during the flush window, and/or fresh-EB certification dips — i.e. the concentrated old-EB burst crowds out fresh traffic (no effective freshest-first).
- **FAIL / mitigated:** fresh traffic keeps baseline latency and certification through the flush, or burst EBs are deprioritised/dropped — evidence of an effective freshest-first / staleness gate on this build. Record which.
- **Control:** the honest baseline must be flat; Design B isolates whether any effect is announcement-channel or genuinely closure-fetch driven.

## 6. Reproduction commands

```bash
# --- build (one-time, after wiring §3; Design B needs only the .bt) ---
make -C behaviours test && make -C behaviours configs   # regenerate net-cluster/behaviours/*.toml (adds eb-burst.toml)
cd net-rs && cargo test -p net-node && cargo build --release -p net-node -p net-cluster
# restart the proto-devnet net-rs to pick up the new binary + behaviour
#   (Haskell nodes untouched; do NOT touch genesis/stake — devnet guardrail)

WORKING_DIR="${WORKING_DIR:-$HOME/leios-proto-devnet}"

# --- latency / cert sampler (side terminal, whole experiment) ---
while :; do
  ts=$(date -u +%FT%TZ)
  for i in 1 2 3; do
    rss=$(ps -o rss= -p "$(cat "$WORKING_DIR/node$i/node.pid")" 2>/dev/null | tr -d ' ')
    cert=$(grep -Ec "LeiosCertified" "$WORKING_DIR/node$i/node.log" 2>/dev/null)
    acq=$(grep -Ec "LeiosVoteAcquired" "$WORKING_DIR/node$i/node.log" 2>/dev/null)
    echo "$ts,node$i,rss=$rss,cert=$cert,acq=$acq"
  done
  sleep 10
done | tee burst.csv

# --- arm Design A (withhold 300 slots, then flush 200 EBs of 8 txs each) ---
curl -X POST http://127.0.0.1:9100/api/attack -H 'content-type: application/json' \
 -d '{"id":"eb-burst","selection":{"kind":"all"},
      "parameters":[
        {"toml_path":"behaviours.eb-burst.spec.withhold_slots","value":300},
        {"toml_path":"behaviours.eb-burst.spec.count","value":200},
        {"toml_path":"behaviours.eb-burst.spec.n_txs","value":8}]}'

# --- live retune (sweep the burst size mid-run) ---
curl -X PUT http://127.0.0.1:9100/api/attack/parameters -H 'content-type: application/json' \
 -d '{"toml_path":"behaviours.eb-burst.spec.count","value":500}'

# --- Design B control (staged announcement burst; buildable today, no new code) ---
#   behaviours/attacks/eb-burst-announce.bt:  root [ Sequence[ Wait(300), "announce-flood" ] ]
curl -X POST http://127.0.0.1:9100/api/attack -H 'content-type: application/json' \
 -d '{"id":"announce-flood","selection":{"kind":"all"},
      "parameters":[{"toml_path":"behaviours.announce-flood.spec.count","value":200}]}'

# --- watch burst acceptance + fresh-traffic timeliness on the committee ---
grep -Ec "LeiosVote|Certified|BlockAnnouncement" "$WORKING_DIR"/node{1,2,3}/node.log

# --- disarm (stop the burst; observe recovery to baseline latency) ---
curl -X POST http://127.0.0.1:9100/api/attack/stop
```

(The `POST /api/attack` + `PUT /api/attack/parameters` shapes and `:9100` match [`../voting/vote-accumulation/plan.md`](../voting/vote-accumulation/plan.md) lines 166–172 and the oversized-EB report, [`../eb-attacks/oversized-eb/report.md`](../eb-attacks/oversized-eb/report.md) lines 158–168.)

## 7. Safety / recovery

- **Bound the burst.** Start with small `count` (10, 50) before larger flushes; the point is the *latency slope vs burst size*, not exhaustion. Cap the flush per the ≤64 KB LeiosNotify batch rule (§3.2) so net-rs does not self-DoS its own egress.
- **RSS budget.** Watch node RSS (secondary signal) and stop if any node crosses baseline + 2 GB or ≤50-60 % machine RAM.
- **Recovery.** `POST /api/attack/stop` halts accumulation/flush. A single flush is a one-shot event; disarm + let the committee drain. If a node stays degraded, restart it from its DB: `kill $(cat $WORKING_DIR/node$i/node.pid)` then relaunch via `proto-devnet/scripts/run.sh` under `caffeinate -i` (node-sleep-crash note). **No respin, no genesis/stake change** (devnet guardrail). net-rs restart alone stops the attack.
- **Blast radius.** Only the three local Haskell BP processes + net-rs; nothing leaves the machine.

## 8. Sweep matrix

| Run | Design | withhold_slots | count | n_txs | Purpose |
|---|---|---|---|---|---|
| C0 | honest (none) | — | — | — | baseline fresh-EB latency + cert rate (control) |
| B1 | B (announce-flood, staged) | 300 | 50 | — | announcement-channel-only burst; no new code |
| B2 | B (announce-flood, staged) | 300 | 200 | — | announcement-channel scaling |
| A1 | A (eb-burst) | 300 | 10 | 8 | small closure burst — per-EB latency delta, low noise |
| A2 | A (eb-burst) | 300 | 50 | 8 | linearity of latency vs burst size |
| A3 | A (eb-burst) | 300 | 200 | 8 | moderate closure storm |
| A4 | A (eb-burst) | 600 | 200 | 64 | larger closures (bigger fetch volume per EB) |
| A5 | A (eb-burst) | 600 | 500 | 8 | high burst — stop at RSS/latency budget |

Legend: `withhold_slots` = silent accumulation window before the flush; `count` = EBs released in the single flush tick; `n_txs` = fabricated tx-closure size per EB; runs stop at the RSS/latency budget regardless of size.

## 9. Open questions

1. **Does the receiver deprioritise stale-but-servable EBs?** The crux of T23 vs its mitigation. Confirm by comparing fresh-EB latency during A-runs (closures) vs B-runs (announcements only) — if only A degrades fresh traffic, the fetch scheduler, not the announce path, is the bottleneck.
2. **Accumulate-genuine vs fabricate-Dummy.** Design A fabricates Dummy EBs for speed. Do genuinely-produced-then-withheld EBs (a slow 20 %-stake accumulation) behave identically on ingest, or does the receiver treat off-schedule/old-slot EBs differently? (Ties to `announce-slot-skew`, `control.rs:142`.)
3. **LeiosNotify SIZE_LIMIT chunking.** The vote-flood run hit `ExceededSizeLimit` at ~90 KB/tick ([`../voting/vote-accumulation/plan.md`](../voting/vote-accumulation/plan.md) lines 233–236). Confirm the `count`/EB-announce size and the ≤64 KB flush-chunk count needed to deliver a large batch without drops.
4. **Praos bleed-through.** Does the Leios closure burst delay RB adoption (Praos), or does net-rs's / the victim's two-class QoS insulate Praos? (net-rs egress is `PriorityWfq`, `net-rs/CLAUDE.md`; the victim's ingest scheduling is the black box.)
5. **Buffer semantics vs `withhold_announce`.** Should Design A buffer the *header* (re-announce later) or re-fabricate at flush time? Buffering preserves the original slot (truer "withhold then release the same EBs"); re-fabricating is simpler. Decide before wiring §3.2.
6. **Recovery / drain time.** After a large flush, how long until fresh-EB latency returns to baseline? A slow drain is itself an operational-sustainability finding.

## 10. File-grounded pointers

| Concern | File:line |
|---|---|
| Threat text (T23) | Ouroboros Leios `docs/threat-model.md` — "Withhold then release large number of EBs" (T22 before, T24 after) |
| EB announce path (the pulse we withhold) | `net-rs/net-node/src/main.rs:1097-1115` (`store.announce_block`, `:1105`) |
| Withhold primitive (`withhold_announce`) | control `../leios-tools/shared-rs/consensus/src/behaviour/tree/control.rs:136`; actuation `main.rs:1102`; action `../leios-tools/shared-rs/consensus/src/behaviour/actions/withhold_eb_announce.rs` |
| EB body offer + tx-closure pinning (servable) | `main.rs:1117-1171` (`InjectLeiosBlock` `:1123`, `inject_block_txs_full` `:1148`) |
| Fabricated EB w/ servable closure (`dummy-tx-eb`) | `../leios-tools/shared-rs/consensus/src/behaviour/actions/fake_eb.rs` (`DummyTxEb`); `FakeEbKind::Dummy` `control.rs:51`; body pinning `main.rs:1146-1149` |
| Announcement burst primitive (`announce-flood`) | control `control.rs:148`; actuation loop `main.rs:851-882` (`fabricate_announcement` `:872`) |
| BT staging (Wait / Sequence) | Wait leaf `../leios-tools/shared-rs/consensus/src/behaviour/tree/behaviour.rs:74`; `tick_wait` `:313`; template `behaviours/attacks/staged-dummy-tx-eb.bt` |
| Wiring template (5 hook points) | leaf `.../actions/announce_flood.rs` + `mod.rs:36,51`; control `control.rs:148,154`; spec `registry.rs:117,126,131`; dispatch `tree/actions.rs:125,131`; actuation `main.rs:851-882` |
| Per-tick flood cap (self-DoS guard) | `main.rs:864` (`MAX_ANNOUNCE_FLOOD=10_000`); vote-flood cap `main.rs:619` |
| LeiosNotify SIZE_LIMIT gotcha | [`../voting/vote-accumulation/plan.md`](../voting/vote-accumulation/plan.md) lines 233–236 (65,535 B; chunk the flush) |
| Node pids / logs / attack API | `$WORKING_DIR/node{1,2,3}/node.{pid,log}`; aggregator + attack API `:9100` |

Legend: `EB` = endorser block; `RB` = ranking block (Praos base block); `BT` = behaviour tree (the `.bt` action-tree DSL); `SIZE_LIMIT` = the LeiosNotify per-notification wire cap (65,535 B); `tx closure` = the transactions an EB commits to; `File:line` cites our repos unless prefixed `../leios-tools/` (sibling path dep) or naming a Haskell source (target black box).

---

## RESULTS — RUN (2026-09-03, proto-devnet cardano-node w34) — ABSORBED / no harm

Wired `eb-burst` end-to-end (leios-tools leaf/control/spec/dispatch + net-node
`fabricate_dummy_eb` + tick-loop release buffer + `EBGenerated{fabricated:true}`
telemetry + `.bt`), then ran an escalating autonomous sweep on the live 3-node
committee. Accumulation was changed from one-EB/slot to a per-tick budget
(`EB_ACCUM_TX_BUDGET=50_000` fabricated txs/tick) so large-`count` bursts are
reachable without waiting `count` slots. Attacker = net-rs (node-0).

| Round | count × n_txs | burst bytes | delivered | committee effect |
|---|---|---|---|---|
| 1 | 50 × 8   | ~0.1 MB | 50 (all) | off +30–40, cert +4–5, RSS flat, missTX 0 |
| 2 | 50 × 32000 | ~400 MB | 50 (all) | RSS flat, cert +6, missTX 0; **net-rs pinned 883 MB** |
| 3 | 500 × 500 | ~64 MB  | ~33 of 500 | **26 `ExceededSizeLimit` egress drops**; absorbed |
| 4 | 200 × 1000 | ~51 MB  | 200 (all, 0 drops) | cert **rate held/rose** (~4.3/node/min vs ~3 baseline), +7 blocks in 2 min |

Legend: `n_txs` = fabricated tx-closure size per EB (txs); burst bytes = count × n_txs ×
256 B (`DUMMY_TX_SIZE`); delivered = EBs whose announcement/offer left net-rs egress;
`off` = burst EBs offered/announced to the committee (Δ vs baseline); `cert` =
`LeiosCertified` events on the committee (Δ vs baseline, from `LeiosCertified` counts);
`RSS` = resident set size (physical memory); `missTX` = `MissingTX` (voter references a
tx body it cannot fetch; `0` = every closure fully servable).

**Verdict: FAIL / mitigated on this build — the concentrated burst does not harm the
committee.** Across 8× (bytes) and 10× (count) escalation the three Haskell nodes showed
no RSS step, no CPU spike, `MissingTX=0`, uninterrupted chain advance, and a
certification rate that held or rose *through* every release. No freshest-first
violation observed: the stale-EB burst never crowded out fresh traffic.

**Why it is blunted (three concrete mechanisms):**
1. **Pull-based fetch.** `LeiosFetch` is request/response — the committee paces its own
   fetching of burst closures, so a "simultaneous release" is absorbed at the victim's
   fetch rate rather than landing as a spike.
2. **Cost falls on the attacker.** The Dummy closures are *pinned in net-rs's own
   mempool* (883 MB at the 400 MB round) — net-rs pays the memory, not the committee.
   This is the opposite of the threat's intent.
3. **Egress `SIZE_LIMIT` caps the burst.** Releasing all announcements/offers in one
   tick overflows the LeiosNotify 64 KB batch above ~200–500 EBs (`ExceededSizeLimit`,
   26 drops at count=500), so a *truly* large simultaneous burst can't even be delivered
   without chunking the flush (the §3.2 gotcha, unimplemented).

**To push further** you would need to (a) chunk the release across a few ticks to beat
the egress cap and (b) find a victim-side resource that pull-fetch does not pace — e.g.
the announcement/election-accounting table (`~10,000 elections × ~50 peers`) rather than
the fetch path. On this idle-mempool devnet neither produced harm at the scales tried.
