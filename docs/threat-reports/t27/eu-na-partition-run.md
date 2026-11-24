# T27 §S2 — EU↔NA network partition run

A network-layer partition (T27 §S2) applied to the pseudo-mainnet topology to
observe how Linear Leios behaves when the network splits and heals.

## Setup

| | |
|---|---|
| Topology | `data/simulation/pseudo-mainnet/topology-v4-mainnet.yaml` (2685 nodes, 30,414 connections, 1 Gbps/link) |
| Params | `sim-cli/configs/mainnet.yaml` (`linear-with-tx-references`, sequential engine, 6 shards, `zero-latency-clusters`) |
| Overlay | `eu-na.yaml` — `set-to-set` EU↔NA, `direction: both` |
| Slots | 1000 |
| Offered load | ~133 tx/s (`tx-generation` 7.5 ms), 1500 B/tx, active slots 60–960 |
| Partition window | **activated slot 99, healed slot 199** — 53,744 directed edges cut/restored |

Run: `sim-rs` (this repo) over the pseudo-mainnet topology, 1000 slots, with the
`mainnet` base parameters plus the internal `eu-na.yaml` overlay (EU↔NA
`set-to-set` cut).

## Result in one line

The partition fired correctly and produced a clear, transient **vote collapse**
on EBs generated during the window; the run as a whole is dominated by a
**structural throughput ceiling** unrelated to the partition, so timing/backlog
degradation must not be attributed to the cut without a baseline comparison.

## Partition-attributable effects (transient, around the window)

Tracking cumulative `votes_per_bundle` and differencing per 60-slot report:

| interval | Δ vote bundles | note |
|---|---|---|
| 120→180 | +901 | partition active — EBs gather one side's committee only |
| **180→240** | **+1** | an entire EB collected ~one vote bundle |
| 240→300 | +5407 | post-heal recovery surge (released vote backlog) |

- **Vote collapse.** The EB produced at slot 142 (mid-partition, by node-75)
  gathered only **901 voters** at slot 180 (vs ~4500 baseline) — roughly one
  partition's committee, because votes could not cross the cut. Node-0's own
  state at slot 240 shows `leios.votes: 1 (voters: 1)` for the EB it held.
- **First quorum failure.** "1 out of 7 EB(s) did not reach the vote threshold"
  first appears at slot 240 — that in-window EB never reached quorum.
- **`WrongEB` validation failures** begin after the heal (901 at slot 360, then
  1802, 2689, 4491 — steps of ~901), consistent with the two sides having
  diverged on which EB to vote for during the split.
- **EB expiry rises across the window:** 0/5 → 2/6 → 4/7.
- **Recovery.** The +5407 vote surge in 240→300 is the network rejoining and
  votes flowing again.

These vote signals are the part that can be confidently pinned on the partition.

## Structural ceiling (NOT the partition)

Degradation builds steadily and keeps worsening long after the heal at slot 199,
so it is a property of the configuration, not the cut:

- **17% of transactions never finalize.** 120,006 generated → 99,601 finalized
  (149 MB), **20,405 (30.6 MB) never reached a block.**
- **Backlog grows monotonically and never drains:** mempool 7.7k → 28k entries;
  pending-tx age 29 s → **115 s**; avg tx→block time 28 s → **99 s**.
- **~Half of all EBs expire uncertified:** 18 of 34 EBs expired before reaching
  an RB; only 16 carried a Leios endorsement.

### Why

Finalization is gated on **ranking blocks** carrying EB certificates:

- `rb-generation-probability: 0.05` → ~1 RB every 20 slots (36 in the run). Every
  EB needs a *later* RB to certify it, so the RB rate bounds certification.
- The certification window is tight: `linear-diffuse-stage (7)` +
  `linear-vote-stage (4)` = 11 slots minimum before a cert exists, then a wait
  for the next Poisson-spaced RB, bounded above by EB max-age. EBs whose next RB
  lands too early (not vote-ready) or too late (aged out / superseded) never
  certify — hence the ~50% expiry, which is structural at these parameters, not
  a defect.
- Net effective finalization ≈ 16 certified EBs × ~6000 tx ≈ **96k tx ≈ 107 tx/s**,
  below the **133 tx/s** offered → unbounded backlog.

Raw capacity is not the wall (0.05 EB/slot × 8000 tx/EB = 400 tx/slot ≫ offered);
**certification *success* is**. Lowering the tx rate below ~107 tx/s would stop
the backlog but would not change the ~50% expiry — that needs the cadence
parameters (`rb-generation-probability`, EB max-age, stage lengths).

### Not bandwidth-bound

Links are a uniform 125 MB/s (1 Gbps) and are modeled, but the network kept up:
delivery ~100% (EB 100%, Vote 100%, TX 99.99%), `queued bytes` stayed sub-MB.
12 MB EB bodies add ~96 ms/hop of transmission, contributing to EB diffusion
latency (a secondary factor in expiry) but not the ceiling.

## Caveat / next step

Most timing and backlog degradation here would also appear in a **no-partition
baseline**, because the config is offered-load-saturated. To isolate the
partition's true contribution, run the identical config + seed **without**
`-p "$T27/eu-na.yaml"` and diff: uncertified-EB count, `WrongEB` failures, and the slope
of pending-age/mempool during slots 99–250. For a cleaner T27 demonstration,
first pick a tx rate comfortably under ~107 tx/s so the baseline is in steady
state and the partition's effect is not buried under a pre-existing backlog.

## Notes

- Capture in normal mode, not `-a` (aggregated output drops the partition
  markers).
- Within-engine A/B only — the sequential engine stamps partition telemetry
  timestamps slightly late vs the actor engine, so do not cross-compare engines
  on timing.
- The `Process RSS: 0 MB` line is a broken probe; the `Estimated total` is a
  naive per-node × N projection that over-counts Arc-shared payloads.
