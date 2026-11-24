# T27 — Conclusion (consolidated)

**Threat:** T27 *"Mempool partitioning via network control"* — Effect: *"Inconsistent
mempools, conflicting EBs"*; Resources: *"Network infrastructure control"*;
Mitigation: *"Limited: directional flow difference."* The body predicts
*"conflicting EBs that don't reach quorum (in time), wasting voting resources,"*
and notes Linear Leios *"prevents conflicting transactions from reaching permanent
storage,"* limiting impact to *"temporary and mostly local resource waste."*

**Status:** historical simulator-phase summary, last updated 2026-06-29.

> Superseded by the [final T27 report](t27.md). Use that report for current
> findings and evidence limitations, including qualifications of the safety,
> finalization, and partial-partition claims. The original interpretations below
> are retained as historical context.

---

## Observations

A complete network partition of Linear Leios produces a **transient liveness
stall that recovers** — **not** a safety break and **not** a sustained denial of
service. With a conflicting-tx workload added, **double-spends are blocked from
permanent storage ("only one succeeds"), and the partition does not turn them into
finalized conflicting EBs.** This **corroborates the threat model's "Limited /
temporary and local" rating** in the simulator.

The predicted *"conflicting EBs that don't reach quorum, wasting voting
resources"* effect splits into two halves — both now tested:

| half of the T27 effect | tested? | result |
|---|---|---|
| **Vote-waste** — EBs fail quorum, votes wasted during the cut | ✅ yes | **transient**: EBs pile up below quorum, certification stalls during the cut, then drains and recovers after heal |
| **Conflicting transactions** — *"only one reaches permanent storage"* | ✅ yes | with a 10% conflict workload, **redundant refs stay 0.000%**, `spent_inputs` enforces one winner per input, finalization = finalizable-max; cost is loser dead-weight (pending-age 539 s), not a safety break — see [partition-120kb-conflict.md](partition-120kb-conflict.md) |

**The adversary's only action is the network partition** — nothing is produced or
injected. The conflict half is a **background-workload property** (`tx-conflict-fraction`),
not an extra attack step.

---

## Evidence (runs and their role)

| report | scenario | key result | role / status |
|---|---|---|---|
| [network-partition.md](network-partition.md) | 120 kB/s, complete EU isolation, slots 300–700, A/B vs baseline | **97.6%** finalized (baseline 97.7%); WrongEB **1,800** (< baseline 5,396); redundant refs **0.016%**; recovers after heal | **primary finding** (below cert ceiling → cleanest A/B) |
| [partition-150kb-eu-isolate.md](partition-150kb-eu-isolate.md) | 150 kB/s, complete EU isolation, slots 300–500, single run | ~**97.7%** effective finalization; redundant refs **0.000%**; WrongEB 4,012; recovers | **higher-load confirmation** — but at/above cert ceiling, so partly saturation-confounded |
| [partition-120kb-conflict.md](partition-120kb-conflict.md) | 120 kB/s **+ 10% tx-conflicts**, complete EU isolation, slots 300–900 | **0.000%** redundant; `spent_inputs` 90,844 (one winner/input); finalization = finalizable-max; pending-age 539 s (loser dead-weight) | **closes the conflicting-tx half**: double-spends blocked from permanent storage, no conflict-driven safety break |
| [eu-na-partition-run.md](eu-na-partition-run.md) | leaky EU↔NA `set-to-set` cut | barely registers (~0.3% vote dip); gossip reroutes around it | **negative control** — shows why a *complete* bipartition is required to exercise T27 at all |

> **Note on an early run.** An initial complete-partition run at saturated load
> reported "does not recover," but that was a **pre-fix simulator bug** (the EB-tx
> fetch was `unreachable!()`), not a protocol property — the fixed simulator
> recovers, as the runs above show. That report and its companion recovery write-up
> have been removed as obsolete.

---

## Limitations — what the conclusion does *not* cover

- **Conflict run is not a clean A/B (window confound).** The conflict run used the
  300–900 overlay; the primary conflict-free finding used 300–700. The qualitative
  result (only-one-succeeds, 0% redundant) is window-independent and solid, but the
  *magnitude* isn't a clean comparison. A same-window, same-seed conflict-vs-no-conflict
  pair would remove this.
- **Complete bipartition may not be the worst case for vote-splitting** — cutting
  all edges lowers each side's stake/production (why WrongEB *falls*); a partial
  cut keeping both sides productive could stress it harder. Untested.
- **No sustained or repeated partitioning** — "transient" is for a fixed 200–400
  slot window; a longer/recurring cut could turn the stall effectively permanent.
- **Single seed, single topology** — seed 0, one topology; no statistical confidence.
- **Recovery depends on the EB-tx fetch fix** faithfully modelling the real node's
  `LeiosFetch`; not validated against the Haskell implementation.
- **150 kB/s conflates partition damage with saturation** (36/64 EBs expire pre-RB
  even absent a cut) — use the 120 kB/s steady run for clean attribution.

---

## Summary

In the simulator, T27 behaves as the threat model predicts: **transient, local,
recoverable — no safety break, and double-spends are blocked from permanent
storage even under a complete partition.** Both halves of the documented effect
are now tested. The remaining work is **real-implementation confirmation** (net-rs
vs the Haskell Leios testnet) for T24–T26, which the simulator can't reach; T27's
network-partition effect stays a simulator result.
