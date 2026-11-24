# Adversarial Campaign — 2026-09-20 (agent-driven, combined behaviours)

Gradient-descent / combinatorial search for novel Leios attacks, driven by the
assistant (not the Python fuzzer). Each attack is a **combination of existing
behaviour-tree leaves** authored in the `.bt` language, translated by
`behaviours/bt.py`, and actuated **live** on the running net-rs node via the
net-cluster control API. No Rust changes to the node.

## Environment

- **Devnet:** `proto-devnet` (internal) — 3 honest Haskell BP pools + net-rs as a single
  ~20% forging pool (sub-25%, so every result is in the valid-adversary range by
  construction) + overflow centrifuge. Booted fresh from clean genesis (fresh-run
  gate; same discipline as `make smoke` `SMOKE_FRESH=1`).
- **Baseline gate:** a full `make smoke` PASS on the fresh chain (net-rs EBs
  certify and EB-only txs settle) establishes the honest baseline before any
  attack. A light forge-health check is used only after a mid-campaign restart.
- **Control surface:** net-cluster's control server (internal tooling). Trees are
  swapped live onto net-rs (re-spawning the net-node child, seconds; the chain is
  untouched), single parameters are tuned live with no respawn, attacks are
  stopped live (net-rs returns to honest), and sensors/stats are read for
  measurement.

## Measurement / objectives

Optimising toward three notions of "compromise" (all three in scope this run):

1. **Node crash / DoS** — honest Haskell node or net-rs crash, hang, connection
   reset, or unbounded resource growth.
2. **Chain quality / forks** — reorg churn, orphaning, fork depth on the honest
   majority (`rb_reorg`, fork badness, `fork-report`).
3. **Consensus liveness/safety** — disproportionate EB-certification / settlement
   denial at sub-25% stake (`eb_quorum_deficit`, `cert_suppression_rate`,
   `fabricated_eb_certified_rate`).

A finding is only credible on a fresh chain and confirmed by the attack's own
production log line (not merely a temporally-near disconnect — see
[`../attack-results.md`](../attack-results.md), EB Size Lie methodology note).

## The loop (per trial)

1. Stop (ensure honest), snapshot baseline sensors + node liveness.
2. Swap the resolved tree in live; confirm net-rs applied it
   (`dynamic config updated` / a production log line for the armed leaf).
3. Observe for the soak window: sensors deltas, net-rs log, all-node liveness,
   fork-report.
4. Stop; record verdict in `results.md`. Sweep key params live if promising.

## Attack battery (field-disjoint Join combinations)

Each `Join` composes leaves that write **distinct** `ControlSignal` fields (so
they actuate together without one clobbering another). Sources are the internal
`behaviours/attacks/combo-*.bt` trees; resolved TOML regenerated with the
behaviour-tree translator (`bt.py --resolve`).

| id | tree | control fields | objective |
|---|---|---|---|
| A1 `combo-flood-storm` | `Join[announce-flood, vote-flood]` | `leios.announce_flood_count` + `leios.vote_flood_count` | crash/DoS (unbounded receiver state) |
| A2 `combo-hollow-fetch` | `Join[hollow-eb, fetch-flood]` | `praos.fake_eb=Hollow` + `leios.fetch_flood_*` | crash/DoS (pre-alloc × fetch amp) |
| A3 `combo-equiv-certsupp` | `Join[rb-header-equivocator, cert-suppressor]` | `praos.production`+`praos.outbound` + `praos.suppress_cert` | forks + liveness |
| A4 `combo-reorg-dummy-sizelie` | `Join[deep-reorg, dummy-tx-eb, announce-size-lie]` | `praos.reorg_depth` + `praos.fake_eb=Dummy` + `leios.announce_eb_size` | all three |
| A5 `combo-fetchflood-withhold` | `Join[fetch-flood, withhold-eb-block-announce]` | `leios.fetch_flood_*` + `leios.withhold_announce` | liveness/DoS |

Legend: `announce_flood_count` = fabricated announcements emitted per tick;
`vote_flood_count` = fabricated votes per tick; `fake_eb` = fabricated-EB variant
announced on produced RBs; `fetch_flood_*` = LeiosFetch re-request rate/window;
`reorg_depth` = self-reorg blocks rolled back; `announce_eb_size` = false size in
the RB header's EB announcement; `withhold_announce` = suppress own EB
announcement.

## Replay

The campaign is driven by the internal net-cluster control tooling: resolve a
combo tree to TOML, swap it in live against the running devnet, and measure via
the sensors. Results, verdicts, and any parameter sweeps are logged in `results.md`.
