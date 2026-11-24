# Fake-EB pen-test family — Phantom / Dummy / Mega Tx EB

**Threat coverage:** T13 *"Reference invalid transactions in EB"* (Phantom, Dummy)
and T11 *"Create invalid EB"* / resource-exhaustion DoS (Mega). Related: T25
*"Submit invalid transactions"*, T31 *"Honest demand exceeds SPO storage"*.

**What the family does:** an adversary announces an *endorser block* (EB) it never
legitimately assembled, riding on any ranking block (RB) it produces. The EB's
manifest references transactions that were never in any honest mempool. The three
variants differ only in **whether — and how — the referenced txs can be fetched**,
which routes the attack through a different part of the CIP-0164 pipeline:

| variant | name | manifest txs | bodies pinned? | honest voter sees | probes |
|---|---|---|---|---|---|
| A | **Phantom Tx EB** | nonexistent hashes | no | tx fetch fails → `MissingTX` decline | missing-tx handling, fetch waste |
| B | **Dummy Tx EB** | fabricated hashes | yes (servable) | tx fetch succeeds → no `MissingTX` | **soundness** — does an available-but-fabricated EB reach quorum? |
| C | **Mega Tx EB** | huge count / oversized declared size | either | (see below) | **robustness / DoS** — bounds, memory, fetch amplification |

Legend: `MissingTX` = the CIP-0164 no-vote reason raised when a voter cannot fetch
every tx an EB's manifest references; *quorum* = the τ=0.75 stake-weighted vote
threshold an EB must clear to be certified; *manifest* = the CBOR list
`[slot, [tx_hash, …]]` that is the EB body; *pinned* = a tx body held in the
adversary's `eb_pinned` compartment so its `TxBodyResolver` serves it on fetch.

The adversary fires on **its own** produced RBs, so reach is stake-bounded (how
often it wins the RB lottery). `n_txs` (manifest size) is the only other knob.

---

## Shared infrastructure

All three variants share one seam so they compose and stay legible:

- **Control (leios-tools `consensus`)** — `PraosControl.fake_eb: Option<FakeEbKind>`
  (`None` = honest). `FakeEbKind` is the enum `{ Phantom { n_txs }, Dummy { n_txs }
  [, Mega { … } planned ] }` in `behaviour/tree/control.rs`.
- **Leaves (leios-tools `consensus`)** — one `LeafAction` per variant in
  `behaviour/actions/fake_eb.rs` (`PhantomTxEb`, `DummyTxEb`), each setting
  `out.praos.fake_eb`. `ActionSpec` variants `phantom-tx-eb` / `dummy-tx-eb`
  (kebab-tagged, default `n_txs = 8`) dispatched in `behaviour/tree/actions.rs`.
- **Actuation (adversarial-tools `net-node`)** — `production.rs::try_produce_block`
  matches on `fake_eb` and builds the EB announcement (`announced_eb`). The
  honest EB path (mempool overflow) is unchanged when `fake_eb == None`.
- **Behaviour trees (adversarial-tools)** — `behaviours/lib/<id>.bt` leaf +
  `behaviours/attacks/<id>.bt` wrapper; `make behaviors` compiles them to
  `net-rs/net-cluster/behaviours/<id>.toml`, discoverable via `GET /api/attacks`
  and in net-ui. `run { name = "…" }` sets the display label; the filename stem
  is the launch `id`.

**How to run** (any variant): start a cluster (`make start`), then
`POST /api/attack {"id":"<id>","selection":{"kind":"stake-fraction","fraction":0.3}}`
(or launch from net-ui). Watch `missing_tx_rate`, `vote_decline_rate`,
`eb_quorum_deficit`, and `wrong_eb_rate` in `GET /api/sensors`.

---

## A — Phantom Tx EB   ✅ implemented & verified

**Mechanism.** The manifest is `n_txs` random 32-byte hashes; the adversary pins
**no** bodies. Honest voters fetch the EB manifest, attempt to fetch the
referenced txs, fail (they exist nowhere), and decline `MissingTX`. The fake EB
has no backing and never reaches quorum.

**Expected signature.** `missing_tx_rate` ↑ (= `vote_decline_rate`), tracking the
adversary's RB share; `wrong_eb_rate` stays 0 (voters see missing txs, not a
malformed EB); `eb_quorum_deficit` rises modestly (fake EBs fail quorum, honest
EBs still certify → chain degrades, does not collapse).

**Verification (2026-07-20, 30-node baseline, 30% stake).** Clean baseline →
sharp onset at attack start: `missing_tx_rate` 0.00 → 0.59 (plateau),
`wrong_eb_rate` stayed 0.00, `eb_quorum_deficit` raw 1.00 → 0.87. Honest node-8
declined the fabricated EB with `MissingTX { required: 8, present_pinned: 0,
present_unpinned: 0 }` — the designed mechanism. Isolated, repeatable.

---

## B — Dummy Tx EB   ✅ implemented & verified — **soundness finding**

**Mechanism.** The adversary fabricates `n_txs` well-formed dummy tx bodies
(`mempool::make_fake_tx`, parsable CBOR so `wire_tx_id` is well-defined), pins
each via `Mempool::merge_eb_body` (lands in `eb_pinned`, keyed by tx hash), and
announces an EB over their hashes. Because the bodies are pinned, the adversary's
`TxBodyResolver` serves them on `LeiosFetch BlockTxs`, and the CIP-0164
`MissingTX` predicate counts them as `present_pinned`. Honest voters therefore
fetch the EB **and** its txs successfully — the availability gate passes.

**The question it answers.** With availability satisfied, does anything downstream
reject an EB whose txs were never validated / never in any honest mempool, or does
it **reach quorum**? net-node's validation is timing-only (no ledger-rule tx
validation — see `validation.rs`), so the working hypothesis is that a Dummy Tx EB
**can be certified** — a soundness gap that a real ledger-validating node would
close. That contrast is the finding.

**Expected signature.** `missing_tx_rate` stays ~0 (txs are present), so the
decline signal that dominates A is **absent**. Either (i) the dummy EB certifies
(→ watch `eb_cert_landing_deficit` / `eb_quorum_deficit` behave *normally* despite
the EB being fabricated — the soundness result), or (ii) declines appear under a
*different* reason (e.g. an EB-validation path), which would itself be the finding.
Distinguishing A from B by sensor shape is the point: A = `missing_tx_rate` spike;
B = no `missing_tx_rate` spike, fabricated EB nonetheless admitted.

**Verification (2026-07-20, 30-node baseline, 30% stake) — the dummy EB
certified.** Adversary node-2 produced a Dummy Tx EB `130538395/d37d2adb` (8
fabricated txs, never in any honest mempool). Honest voters fetched the EB and its
txs successfully and **it reached quorum**: `voted_weight=778 ≥ threshold=750,
voters=7`. Throughout the attack `missing_tx_rate` stayed **0.00** — the decline
signal that dominates Phantom (A) was entirely absent, and no EB was declined for
`MissingTX`. This is the predicted soundness gap: the availability gate
(`MissingTX`) alone does **not** stop a fabricated EB from certifying, because
net-node's voting path performs no ledger-level tx-content validation
(`validation.rs` is timing-only). A real ledger-validating node would reject the
fabricated txs; the finding is that availability ≠ validity, and only content
validation closes it.

**Honest framing / scope.** This demonstrates the property in *this* net-rs
simulator (fake validation). It does not by itself prove a live Leios node is
vulnerable — that depends on whether the production voter validates EB tx content
before voting. The pen-test pinpoints exactly where such validation must live.

---

## C — Mega Tx EB   📋 planned (this document only)

**Goal.** Probe **robustness and resource bounds**, not the vote logic: does a
single fabricated EB with an enormous manifest (or an enormous *declared* size)
cause unbounded allocation, fetch amplification, slot-time overruns, or storage
blow-up on honest nodes — i.e. an asymmetric DoS where a low-stake adversary
imposes disproportionate work on the network?

**Two orthogonal knobs (implement as fields on `FakeEbKind::Mega`):**

1. **`n_txs` very large** (e.g. 10⁴–10⁶) — a manifest listing a huge number of
   txs. Sub-modes mirror A/B: *phantom-mega* (unfetchable — probes the fetch
   retry/bitmap path and per-EB tracking under load) or *dummy-mega* (servable —
   probes actual body transfer + storage). Stresses: manifest decode allocation,
   the `LeiosFetch` bitmap addressing, per-EB memory in `eb_pinned` /
   `eb_manifests`, and honest fetch scheduling.
2. **`declared_bytes` oversized** — announce a small real manifest but advertise a
   massive `eb_size` on the RB header / `MsgLeiosBlockOffer` (composes with the
   existing `lie-about-eb-size` leaf). Probes whether receivers pre-allocate or
   gate on the *declared* size before fetching.

**Key checks / expected outcomes.**

- **Allocation bounds (per `net-rs` security-audit rule 1).** Every wire length —
  CBOR manifest array length, `eb_size` — must be checked against a maximum before
  allocation. *Pass* = the mega EB is rejected/capped with a clean error and honest
  nodes stay live; *fail* = memory spike / OOM / decode panic. This directly tests
  that audit invariant against a hostile EB.
- **Fetch amplification.** One adversary RB → every honest committee member tries
  to fetch a giant EB body + N tx bodies. Measure work-per-adversary-RB (bytes
  transferred, fetch requests) as the amplification factor.
- **Slot-time / liveness.** Does honest per-slot processing (decode + fetch +
  vote) overrun the slot budget, inducing lag/reorgs? Watch `rb_reorg_rate`,
  `tip_disagreement`, `chain_growth_deficit`, `instability_worst`.
- **Storage.** For dummy-mega, does `eb_pinned` / the LeiosStore grow without a
  retention cap? (Relates to T31.)

**Implementation sketch (when built):**

- `FakeEbKind::Mega { n_txs: u32, declared_bytes: Option<u64>, servable: bool }`
  (or a small sub-enum). Add `mega-tx-eb` `ActionSpec` + a `MegaTxEb` leaf.
- `production.rs`: extend the `match fake_eb` with a Mega arm — build the large
  manifest; if `servable`, pin bodies as in Dummy (mind the cost — cap or stream);
  if `declared_bytes.is_some()`, set the announced `eb_size` to the lie (reuse the
  `EbSizePolicy` seam rather than duplicating it).
- **Bench harness, not just sensors.** Robustness needs resource telemetry
  (peak RSS, fetch bytes, decode time) alongside the badness sensors — add a
  lightweight per-node resource sample to the run, or drive a single-victim
  two-node setup for a clean measurement.
- **Safety.** Start with modest `n_txs` and ramp; a true mega manifest can wedge
  the local cluster. Gate the extreme sizes behind an explicit parameter so a
  casual net-ui launch can't accidentally OOM the operator's machine.

**Open design question.** Whether Mega is one leaf with knobs or three
(`mega-count`, `mega-declared`, both) — decide when implementing; the plan keeps
them as knobs on one variant for now.

---

## Status summary

| variant | consensus leaf | net-node actuation | BT config | verified |
|---|---|---|---|---|
| A Phantom Tx EB | ✅ | ✅ | ✅ `phantom-tx-eb` | ✅ 2026-07-20 |
| B Dummy Tx EB | ✅ | ✅ | ✅ `dummy-tx-eb` | ✅ 2026-07-20 — certified (soundness finding) |
| C Mega Tx EB | 📋 | 📋 | 📋 | 📋 |

Legend: ✅ done · ⏳ in progress · 📋 planned.
