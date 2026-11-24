# T23 — Withhold then release large number of EBs — initial report

**Status:** RUN — **no measurable harm across all scales tried** (fabricated + real EBs, up to 100 EBs / 3.6 h stale / 150 ms link). No freshest-first *delivery* gate exists at ingestion (stale EBs are accepted + fetched), **but the voting-window expiry is an effective backstop at the certification layer — 0 of 100 long-stale released EBs were certified** (RESULTS 3). Runs: fabricated-EB sweep (flawed model, §1–§6), real-EB count=20 (RESULTS 2), real-EB count=100 under 150 ms delay (RESULTS 3).
**Date:** 2026-09-03 / 2026-09-04
**Target:** cardano-node **w34** (`prototype-2026w34`, `ouroboros-consensus-3.0.1.0`) — 3 Haskell committee nodes on the local proto-devnet.
**Driver:** net-rs (Piranha), node-0.
**Threat (Ouroboros Leios threat model `docs/threat-model.md`):** *"Adversaries can withhold large numbers of EBs and their transaction closures over extended periods, then release them simultaneously to create concentrated bursts of network traffic."* Proposed mitigation: freshest-first delivery.

Design + mechanism analysis: [`plan.md`](plan.md). This report records the implementation and the first experimental result.

---

## 1. Verdict

The `eb-burst` attack works mechanically — net-rs silently accumulates a batch of
fabricated Dummy EBs (each with a servable tx closure) and releases the whole batch's
announcements + offers in one tick — **but it produced no measurable harm to the Haskell
committee** across an escalating sweep. Chain liveness, certification rate, node RSS and
CPU were undisturbed through every release. This is the plan's **FAIL / mitigated**
branch: on this harness the concentrated burst does not crowd out fresh traffic.

## 2. Implementation (`eb-burst`)

Wired end-to-end, mirroring `announce-flood` + `dummy-tx-eb`:

| Hook | Location |
|---|---|
| Leaf action | `../leios-tools/.../behaviour/actions/eb_burst.rs` (+ `actions/mod.rs`) |
| Control fields | `.../tree/control.rs` — `eb_burst_{withhold_slots,count,n_txs}` (default 0) |
| ActionSpec | `.../registry.rs` — `#[serde(rename="eb-burst")]` |
| Dispatch | `.../tree/actions.rs` |
| Producer helper | `net-node/src/production.rs` — `fabricate_dummy_eb()` (lottery-free servable Dummy EB) |
| Actuation + buffer | `net-node/src/main.rs` — tick-loop accumulate/flush; per-tick fabrication budget `EB_ACCUM_TX_BUDGET=50_000`; release emits `EBGenerated{fabricated:true}` telemetry |
| Behaviour | `behaviours/{attacks,lib}/eb-burst.bt` → `net-cluster/behaviours/eb-burst.toml` |

Tests: `eb_burst.rs` (2), `fabricate_dummy_eb_is_servable_and_lottery_free` (net-node).

## 3. Sweep result

Escalating rounds against the live 3-node committee (attacker = net-rs). `DUMMY_TX_SIZE
= 256 B`; manifest ≈ 37 B/tx.

| Round | count × n_txs | EB body | closure | burst total | delivered | committee effect |
|---|---|---|---|---|---|---|
| 1 | 50 × 8 | 0.3 KB | 2 KB | ~0.1 MB | all 50 | absorbed |
| 2 | 50 × 32000 | 1.18 MB | 8 MB | ~400 MB | all 50 | absorbed; **net-rs pinned 883 MB** |
| 3 | 500 × 500 | 18.5 KB | 128 KB | ~64 MB | ~33 of 500 | **26 `ExceededSizeLimit` egress drops**; absorbed |
| 4 | 200 × 1000 | 37 KB | 256 KB | ~51 MB | all 200 (0 drops) | cert **rate held/rose** (~4.3 vs ~3 /node/min); +7 blocks/2 min |

Legend: `n_txs` = fabricated tx-closure size per EB (txs); EB body = announced manifest bytes (`EBGenerated.size_bytes`); closure = fetchable tx bodies (n_txs × 256 B); delivered = EBs whose announcement left net-rs egress; `cert` = `LeiosCertified` events on the committee; `RSS` = resident set size (physical memory); `MissingTX` = voter references a tx body it cannot fetch (`0` = every closure fully servable); honest EBs on this devnet ≈ 3 KB body / 77–87 txs.

Across every round: `MissingTX = 0`, no RSS step, no CPU spike, uninterrupted chain
advance, certification rate steady-to-rising through the flush.

## 4. Why it was absorbed (ranked)

1. **The harness lacks the resource T23 attacks.** All four nodes talk over **loopback**;
   bandwidth is effectively unlimited, so a "network-traffic burst" has nothing to
   congest. Likely the dominant reason — a limitation of the local devnet, not evidence
   the threat is unreal.
2. **Pull-based `LeiosFetch`.** The announcement pushes only a small header (EB hash +
   advertised size — so even R2's 8 MB EBs announced with 0 drops); the manifest and
   closure are *requested* at the committee's own pace. A simultaneous release becomes N
   fetch requests the victim schedules itself — no spike.
3. **Cost falls on the attacker.** Dummy closures pin in **net-rs's** mempool (883 MB @
   400 MB) and serve on demand; the fabricated (ledger-invalid, unauthorized) EBs are
   fetched-validated-discarded, so nothing accumulates on the committee (RSS flat).
4. **Delivery self-caps.** net-rs's egress `SIZE_LIMIT` drops announcements past
   ~200–500 per tick (§3 R3), so a truly massive simultaneous burst can't even be sent
   without chunking the flush (unimplemented).
5. **Scale.** Even fully delivered, tens of MB is trivial for the nodes, and the idle-ish
   mempool leaves little fresh traffic to crowd out.

## 5. Caveats — what this run did NOT rule out

- Sampled RSS/CPU/cert **counts** at ~1-min cadence, **not** fine-grained fresh-EB
  fetch-to-adopt **latency** (T23's primary signal). A transient flush-time blip could
  hide under that resolution.
- Fabricated EBs are **unauthorized** (random issuer) with **invalid** txs — the committee
  may reject them early, so the intended "valid withheld EBs" path may not be exercised.
- **One-shot** burst; sustained multi-minute pressure not tried.
- Loopback, idle devnet — not the geo-distributed, bandwidth-bounded setting T23 targets.

## 6. How to actually test T23 (follow-up)

1. **Bound inter-node bandwidth** (not just delay — `make netshape` adds latency only).
   Give the burst a link to congest.
2. **Latency-resolved measurement:** timestamp fresh-EB announce/offer → adopt/vote and
   compare the flush window vs baseline, per the plan §5.
3. **Chunk the release flush** into ≤64 KB announcement batches to beat the egress cap and
   deliver `count` ≫ 500.
4. **Sustained / repeating** burst rather than one-shot.
5. Consider **authorized/valid** withheld EBs (real production wins) to exercise the
   genuine path, if the goal is fidelity over the cheap fabricated substitute.

---

## RESULTS 2 — REAL-EB run (2026-09-04) — the corrected attack

The first run used *fabricated* (unauthorized, ledger-invalid) Dummy EBs — a flawed
model. Reimplemented so net-rs **withholds its own genuine EBs**: each EB net-rs forges
on its real EB-election wins (valid txs, real closure snapshotted at produce time) is
buffered instead of diffused; at `count` it flushes the whole batch. Ran `count=20`.

**Run:** net-rs accumulated 20 real EBs over ~29 min (slots ~113882→115602; VRF variance,
~1 EB/45–300 s), then released all 20 in one tick (slot 115602, **0 egress drops** — 20
EBs fit the announcement batch). The withheld EBs were stale by up to ~1700 slots
(~29 min) at release.

**Findings:**
- **The committee accepts and fetches the stale EBs.** ~24 announcements / 28 offers / 24
  tx-offers at release, and **15 `LeiosBlockAcquired/Stored/TxsAcquired`** in the release
  window — the 20 ~29-min-old EBs were received, fetched, and stored, **not** rejected.
- **No freshest-first / staleness gate exists on this build.** Zero
  age/freshness/expiry/stale markers anywhere — the committee processes arbitrarily-old
  released EBs exactly like fresh ones. This is the vulnerability T23's proposed
  mitigation targets, confirmed **absent**.
- **Still no measurable harm at this scale.** Fresh-EB certification continued at the
  normal ~4/node/min *through* the release (`LeiosCertified` at 00:09 after the 00:04:33
  burst), `MissingTX=0`, no CPU spike, chain advancing (block 5055→5080). Committee RSS
  rose only on node1 (+48 MB over 6 min, within the honest-load trend; node2/3 flat).

**Interpretation.** The corrected attack removes the earlier confound (fabricated/invalid
EBs that could be rejected): these are valid, authorized, fetched-and-stored EBs. It
confirms the real gap — **no staleness discipline** — but at count=20 on a loopback devnet
the burst is too small, and bandwidth too abundant, to translate "no gate" into degraded
fresh-traffic timeliness. The harm requires (a) a far larger accumulated batch (needs more
attacker stake or much longer withholding) and/or (b) bandwidth-bounded inter-node links,
plus (c) latency-resolved fetch-to-adopt measurement to catch a sub-minute contention blip.

**Open (worth a follow-up):** do the ~29-min-stale EBs get *certified* late? If a released
long-stale EB still gathers a committee certificate, that is a distinct safety finding
beyond the traffic-burst framing — not confirmed in this run.

---

## RESULTS 3 — REAL-EB burst at scale, under 150 ms link delay (2026-09-04)

Ran `count=100` real self-produced EBs under a 150 ms inter-node netshape delay, and
added release-time logging of each withheld EB's announcing-RB point to answer the
certification question directly.

**Run:** net-rs accumulated **100 real EBs over ~3.7 h** (arm slot ~174318 → release slot
187456), then released all 100 in one tick (20:02:07, **0 egress drops**). Staleness at
release was severe and broadly spread:

| staleness | EBs |
|---|---|
| < 5 min | 2 |
| 5–30 min | 9 |
| 30–60 min | 16 |
| 1–2 h | 29 |
| > 2 h | 44 |

Legend: staleness = wall-clock age of each withheld EB at release (release slot − mint
slot); EBs = number of released EBs falling in that staleness bucket.

Oldest EB: **219 min (~3.6 h / ~13,138 slots) stale.** 73 of 100 were > 1 h stale.

**Findings:**
- **The stale EBs are NOT certified.** Of the 100 released EBs, **0 appear in any
  committee `LeiosCertified`** (checked against node1's own cert log; all 100 *do* appear
  in node1.log, so they were received/processed — just never certified). The 20 certs in
  the release window were all fresh EBs.
- **Why:** there is no explicit "reject stale EB" log, but certification requires committee
  *votes within the election window*, which has long expired for these EBs. So an
  **implicit staleness barrier at the voting/certification layer** blocks stale EBs from
  certifying — even though the fetch/ingestion layer has no freshest-first gate (RESULTS 2).
- **The burst is paced, not a shock.** The 100 announcements/offers arrived at the
  committee at ~16–20/min (spread over minutes), not 100 at once — pull-based `LeiosFetch`
  plus the 150 ms RTT throttle it to the victim's fetch rate.
- **No harm to fresh traffic.** Fresh certification continued at the normal rate straight
  through the release (`LeiosCertified` 1046 → 1068 → 1079 across the burst window),
  `MissingTX=0`, no CPU spike, committee RSS flat-to-down (node1 637→621, GC), chain
  advancing (block 6055→6066).

**Bottom line.** Even at 100 EBs, up to 3.6 h stale, under a 150 ms delay, T23 produced **no
measurable harm**. The traffic-burst threat is bounded on this build: stale released EBs
cost a little (paced) fetch bandwidth but **cannot be certified** — no safety or liveness
impact. The gap T23's mitigation names (no freshest-first *delivery* gate) is real at the
ingestion layer, but the **voting-window expiry** provides an effective backstop at the
certification layer that keeps the attack from translating into chain harm.
