# Corrected Linear Leios vote diffusion results

All **108 runs** completed: two network sizes, two committee modes, three matched seeds, and announce/request plus two push deduplication orders crossed with unlimited/22/16/8 fanout. Simulator revision `0769c07310fba223a09d8082e2744013c6856a43`.

The subsequent review changes add obsolete-work telemetry while preserving
transport behavior. These archived runs have **not** been rerun with that
instrumentation, so their obsolete-work breakdown is unknown. The
[study guide](../vote-diffusion-study.md#measuring-obsolete-vote-work) explains
what the new counters measure and why they are subsets of the existing totals.

## Transport terminology

**Push** means sending the vote body directly to peers, without waiting for a
request. **Mark seen** means remembering a vote's identifier so later copies can
be discarded. Both push modes verify the first copy before accepting its vote.

| Term used in the figures | Simulator setting | What happens when copies arrive? |
|---|---|---|
| Announce / request | `announce-then-request` | Announce the identifier; send the body when the peer requests it. |
| Push: mark seen on arrival | `push` | Remember the identifier immediately, so copies arriving while verification is queued or running are discarded too. |
| Push: mark seen after verification | `push-late-dedupe` | Remember the identifier after verification completes. Copies arriving in the meantime can each trigger verification; later copies are discarded. |

Earlier descriptions called the two push modes **early deduplication** and
**late deduplication**. “Late” referred to this recording step, not delayed
sending. In particular, the second mode does **not** reverify every duplicate:
it skips copies of votes already verified. The results tables retain the exact
configuration values above for reproduction. The two committee models are
explained in the [study guide](../vote-diffusion-study.md#committee-reference).

## Why compare the two duplicate-handling orders?

Direct streaming delivers the same vote along several peer connections. The
question is whether those copies consume only bandwidth, or also repeat the
expensive signature check and delay other work on the node's CPU. This is why
the experiment crosses fanout with duplicate handling, in addition to comparing
push against announce/request.

**What the inspected Haskell prototype does.** The audit used cardano-node's
`leios-prototype` at
[`afa091b4`](https://github.com/IntersectMBO/cardano-node/blob/afa091b4af2795d1d9c46e59145ed16127760f7b/cabal.project#L95),
which pins ouroboros-consensus `7abeda65`. At that revision,
[`LeiosVoteState.addVote`](https://github.com/IntersectMBO/ouroboros-consensus/blob/7abeda6501282c8a2491ccf66c16f051d74fb895/ouroboros-consensus/src/ouroboros-consensus/LeiosVoteState.hs#L109)
does the following:

1. Check the shared set of successfully verified votes; return if already known.
2. Validate the vote, including its BLS signature, outside the atomic state update.
3. Check the set again atomically, then store and count the vote only if another
   handler has not already done so.

Two peer handlers can both pass step 1 before either reaches step 3. Both then
pay for verification, but only one counts and relays the vote. The second check
prevents double counting; it does not recover the duplicate verification work.
The source comment explicitly identifies concurrent duplicates as a remaining
source of redundant checks.

**What sim-rs does.** The existing announce/request baseline records a pending
request and normally avoids fetching the body again; if multiple bodies are
requested, it schedules verification for each and detects redundant copies on
completion. This PR adds two push alternatives in
[`receive_votes` and `finish_validating_vote_bundle`](../../sim-core/src/sim/linear_leios.rs):

- `push` records an in-flight identifier before scheduling verification. Further
  copies are discarded while that check is queued or running.
- `push-late-dedupe` checks for an already verified copy on arrival but does not
  reserve the identifier while verification is pending. Concurrent copies can
  each schedule a check; only the first completion counts the vote.

For example, if three copies arrive before the first check completes, these
settings schedule one versus three verifications, with one accepted vote in
either case. Verification is modeled as CPU work, not actual BLS execution.

**How far the comparison transfers.** The second setting is motivated by the
Haskell ordering above; the first measures the benefit of suppressing pending
copies. They are simulator alternatives, not two measured Haskell versions.
The prototype's
[notification handlers and queues](https://github.com/IntersectMBO/ouroboros-consensus/blob/7abeda6501282c8a2491ccf66c16f051d74fb895/ouroboros-consensus-diffusion/src/ouroboros-consensus-diffusion/Ouroboros/Consensus/Network/NodeToNode.hs#L580)
process votes serially per peer and share outgoing notification capacity with
other messages, dropping votes when no notification credit is available. Those
mechanisms affect which duplicates reach verification and are not reproduced
here. This was source inspection at a pinned revision, not a Haskell runtime
benchmark or a claim about the latest prototype. The measured amplification and
endorsement changes below therefore apply to the simulator.

## Findings

- **Unrestricted push trades bandwidth for about half a second.** Across all 12 paired size/committee/seed comparisons, push that marks votes seen on arrival retained the announce/request arm's Q95 attainment and L1 endorsement counts. Its conditional mean Q95 time was 0.356–0.530s earlier, with 7.81–7.88× the vote mini-protocol bytes. These are equal counts, not an EB-identity comparison.
- **At 1500 nodes, marking votes seen after verification reduces endorsement counts only in the everyone-votes arm.** With unrestricted push, the stake-weighted reference reached Q95 for 58/72 generated EBs and produced 25 L1 endorsements across the three seeds with either push setting. Marking seen after verification still incurred 9.69 completed verifications per accepted arrival and increased conditional mean Q95 times from 3.334–3.339s to 3.796–3.814s. The reference has 458 eligible pools, so it also generates fewer vote bodies than the 1500-voter stress arm.
- **Fanout 22 relieves some verification load, at a cost in availability.** In the 1500-node everyone-votes arm that marks seen after verification, it reduced total completed verifications by 30.0% and wire bytes by 29.4%; Q50 attainment increased from 37/72 to 50/72 EBs and L1 endorsements from 13 to 18. Q95 attainment fell from 37/72 to zero. In the stake-weighted arm with the same handling of copies, fanout 22 cut wire bytes by 40.3% and verifications by 43.2%, but Q50 attainment fell from 58/72 to 57/72 and endorsements from 25 to 19; Q95 again fell to zero.
- **None of the tested bounded fanouts preserves quorum availability at nodes holding 95% of stake.** All 72 runs with fanout 22/16/8 had zero EBs reaching Q95. Fanout 22 often retained Q50, while 16 and 8 never reached Q50 in these runs. Fanout 8 produced zero L1 endorsements in every arm. A first-node quorum can still exist; zero Q95 does not mean nobody obtained a quorum.

These results support unlimited simple vote streaming as feasible under the modeled load, with a bandwidth/latency trade-off. They do not establish a safe bounded-fanout setting or predict the Haskell node's exact performance. Lower fanout reduces verification work but, in this matrix, does not preserve the broad availability of unrestricted diffusion.

## Reading the measurements

`t0` is the start of the ranking-block slot that announced the EB. Chart times
and deadlines are measured from that point.

A **quorum at one node** requires verified votes totaling 75% of active stake in
the reference arm, or 75% of node votes in the everyone-votes stress arm.

We separately measure **how widely nodes have collected a quorum**:

- **Q50:** nodes collectively holding 50% of network stake each have a quorum for the EB.
- **Q95:** nodes collectively holding 95% of network stake each have a quorum for the EB.

Q50/Q95 are shorthand defined for this study, not additional certificate
thresholds. Earlier wording called this “observer coverage”; no separate observer
role is implied. Both measurements weight the receiving nodes by stake, even in
the everyone-votes arm. Neither measures the fraction of votes inside one
certificate. The separate vote-body delivery statistic is unweighted by stake.

The [CIP's network characteristics](https://github.com/cardano-foundation/CIPs/blob/master/CIP-0164/README.md#protocol-parameters)
also discuss propagation to approximately 95% of honest stake. That related
propagation target does not define this study's particular quorum statistic.
Failing Q95 therefore does not, by itself, establish that certification stopped
or that a certificate is invalid.

Counts include every generated EB; missed quorums remain in the denominator. Timing means include only EBs whose quorum became available at the stated share of stake. All reported Q50/Q95 quorums in this batch occurred by 7s, so their counts by 7s, by 14s and at run end coincide. This does not make 7s and 14s interchangeable protocol constraints. L1 endorsements count generated ranking blocks carrying an endorsement. This is neither a count of EBs reaching quorum somewhere nor a separately verified count on the final canonical chain.

## Availability versus verification cost at 1500 nodes

![Verification work and Q50/Q95 quorum availability versus fanout, for the two 1500-node committees](figures/fanout-overview.png)

**Figure 1.** Reducing fanout lowers completed verifications per accepted arrival
in both committees (top panels). At fanout 22, Q50 attainment rises only in the
everyone-votes stress arm; Q95 falls to zero in both committees (bottom panels).
The small hollow marks show individual seeds and the labeled lines pool all
three. The shaded column highlights fanout 22. Lines connect tested settings;
they do not establish behavior between those settings.

The table sums each arm's three seeds (72 generated EBs). Q50/Q95 counts are attainment by 14s; the same counts were attained by 7s. `Verify / accepted` divides total completed verifications by total accepted arrivals.

| Committee | Transport | Fanout | First-node quorum | Q50 | Q95 | L1 endorsements | Verify / accepted |
|---|---|---|---:|---:|---:|---:|---:|
| top-stake-seats | push | all | 58/72 | 58/72 | 58/72 | 25 | 1.00 |
| top-stake-seats | push-late-dedupe | all | 58/72 | 58/72 | 58/72 | 25 | 9.69 |
| top-stake-seats | push-late-dedupe | 22 | 58/72 | 57/72 | 0/72 | 19 | 5.71 |
| top-stake-seats | push-late-dedupe | 16 | 57/72 | 0/72 | 0/72 | 2 | 4.23 |
| everyone | push | all | 57/72 | 57/72 | 57/72 | 25 | 1.00 |
| everyone | push-late-dedupe | all | 37/72 | 37/72 | 37/72 | 13 | 11.63 |
| everyone | push-late-dedupe | 22 | 50/72 | 50/72 | 0/72 | 18 | 7.15 |
| everyone | push-late-dedupe | 16 | 52/72 | 0/72 | 0/72 | 1 | 5.42 |

## Transport comparisons

![Paired quorum times and vote traffic for announce/request versus unrestricted push](figures/transport-comparison.png)

**Figure 2.** Each segment compares the same seed. Push consistently moves Q95
earlier (left), while sending about eight times the vote mini-protocol bytes
(right). The row labels retain missed EBs: the time advantage is conditional on
attainment, not a claim that every EB met the deadline.

These compare unlimited-fanout push with announce/request for the same topology, committee and seed. Time differences are between conditional per-EB Q95 means; matching counts do not prove matching EB identities.

| Nodes | Committee | Push / announce traffic | Push − announce Q95 mean (s) | Equal Q95 attainment counts | Equal L1 endorsement counts |
|---:|---|---:|---:|---:|---:|
| 750 | top-stake-seats | 7.81–7.81× | -0.399–-0.382 | 3/3 seeds | 3/3 seeds |
| 750 | everyone | 7.82–7.82× | -0.356–-0.356 | 3/3 seeds | 3/3 seeds |
| 1500 | top-stake-seats | 7.88–7.88× | -0.530–-0.515 | 3/3 seeds | 3/3 seeds |
| 1500 | everyone | 7.88–7.88× | -0.478–-0.476 | 3/3 seeds | 3/3 seeds |

## Endorsements and validation order

![L1 endorsement counts at each fanout, split by committee and duplicate-check order](figures/endorsement-comparison.png)

**Figure 3.** Under unrestricted push, marking votes seen after verification
reduces endorsements from 25 to 13 in the everyone-votes stress arm, while the stake-weighted reference
retains 25 with either setting. Fanout 22 raises the stress arm's count with that
policy to 18, but reduces the reference to 19. This explains why the local recovery
in the overloaded arm is insufficient to recommend fanout 22: Figure 1 also
shows its loss of Q95 availability. Bars sum three seeds and count generated L1
blocks carrying endorsements. They do not measure inclusion on the final
canonical chain.

## Fanout and validation order

Each row combines three seeds. Q95 attainment is the sum of per-run EB counts. Timing ranges cover available run means among EBs that attained Q95 quorum; missing EBs remain visible in the attainment columns. Verification amplification is total completed verifications divided by total accepted arrivals. Traffic is the range of per-run decimal GB, rounded in the simulator logs.

| Nodes | Committee | Transport | Fanout | Q95 reached | Q95 by 7s | Q95 by 14s | Q95 mean s range | Wire GB range | Verify / accepted | Pending arrivals |
|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| 750 | top-stake-seats | announce-then-request | all | 44/57 | 44/57 | 44/57 | 3.600–3.621 | 0.655–0.749 | 1.00 | 0 |
| 750 | top-stake-seats | push | all | 44/57 | 44/57 | 44/57 | 3.218–3.222 | 5.116–5.850 | 1.00 | 0 |
| 750 | top-stake-seats | push | 22 | 0/57 | 0/57 | 0/57 | none reached | 3.213–3.651 | 1.00 | 0 |
| 750 | top-stake-seats | push | 16 | 0/57 | 0/57 | 0/57 | none reached | 2.340–2.638 | 1.00 | 0 |
| 750 | top-stake-seats | push | 8 | 0/57 | 0/57 | 0/57 | none reached | 1.173–1.323 | 1.00 | 0 |
| 750 | top-stake-seats | push-late-dedupe | all | 44/57 | 44/57 | 44/57 | 3.323–3.334 | 5.118–5.855 | 6.85 | 0 |
| 750 | top-stake-seats | push-late-dedupe | 22 | 0/57 | 0/57 | 0/57 | none reached | 3.213–3.651 | 3.82 | 0 |
| 750 | top-stake-seats | push-late-dedupe | 16 | 0/57 | 0/57 | 0/57 | none reached | 2.340–2.638 | 2.60 | 0 |
| 750 | top-stake-seats | push-late-dedupe | 8 | 0/57 | 0/57 | 0/57 | none reached | 1.173–1.323 | 1.24 | 0 |
| 750 | everyone | announce-then-request | all | 41/57 | 41/57 | 41/57 | 3.663–3.664 | 2.245–2.547 | 1.00 | 0 |
| 750 | everyone | push | all | 41/57 | 41/57 | 41/57 | 3.307–3.308 | 17.560–19.915 | 1.00 | 0 |
| 750 | everyone | push | 22 | 0/57 | 0/57 | 0/57 | none reached | 11.011–12.487 | 1.00 | 0 |
| 750 | everyone | push | 16 | 0/57 | 0/57 | 0/57 | none reached | 8.026–8.966 | 1.00 | 0 |
| 750 | everyone | push | 8 | 0/57 | 0/57 | 0/57 | none reached | 4.024–4.495 | 1.00 | 0 |
| 750 | everyone | push-late-dedupe | all | 37/57 | 37/57 | 37/57 | 3.864–3.873 | 17.040–19.526 | 10.29 | 0 |
| 750 | everyone | push-late-dedupe | 22 | 0/57 | 0/57 | 0/57 | none reached | 10.955–12.488 | 6.49 | 0 |
| 750 | everyone | push-late-dedupe | 16 | 0/57 | 0/57 | 0/57 | none reached | 7.985–8.918 | 4.86 | 0 |
| 750 | everyone | push-late-dedupe | 8 | 0/57 | 0/57 | 0/57 | none reached | 4.010–4.481 | 2.55 | 0 |
| 1500 | top-stake-seats | announce-then-request | all | 58/72 | 58/72 | 58/72 | 3.849–3.869 | 3.543–4.200 | 1.00 | 0 |
| 1500 | top-stake-seats | push | all | 58/72 | 58/72 | 58/72 | 3.334–3.339 | 27.906–33.086 | 1.00 | 0 |
| 1500 | top-stake-seats | push | 22 | 0/72 | 0/72 | 0/72 | none reached | 16.665–19.525 | 1.00 | 0 |
| 1500 | top-stake-seats | push | 16 | 0/72 | 0/72 | 0/72 | none reached | 12.036–14.223 | 1.00 | 0 |
| 1500 | top-stake-seats | push | 8 | 0/72 | 0/72 | 0/72 | none reached | 6.036–7.133 | 1.00 | 0 |
| 1500 | top-stake-seats | push-late-dedupe | all | 58/72 | 58/72 | 58/72 | 3.796–3.814 | 27.802–32.814 | 9.69 | 0 |
| 1500 | top-stake-seats | push-late-dedupe | 22 | 0/72 | 0/72 | 0/72 | none reached | 16.660–19.516 | 5.71 | 0 |
| 1500 | top-stake-seats | push-late-dedupe | 16 | 0/72 | 0/72 | 0/72 | none reached | 12.036–14.223 | 4.23 | 0 |
| 1500 | top-stake-seats | push-late-dedupe | 8 | 0/72 | 0/72 | 0/72 | none reached | 6.036–7.133 | 2.13 | 0 |
| 1500 | everyone | announce-then-request | all | 57/72 | 57/72 | 57/72 | 4.002–4.003 | 11.480–13.612 | 1.00 | 0 |
| 1500 | everyone | push | all | 57/72 | 57/72 | 57/72 | 3.525–3.526 | 90.488–107.295 | 1.00 | 0 |
| 1500 | everyone | push | 22 | 0/72 | 0/72 | 0/72 | none reached | 53.997–63.528 | 1.00 | 0 |
| 1500 | everyone | push | 16 | 0/72 | 0/72 | 0/72 | none reached | 39.020–46.146 | 1.00 | 0 |
| 1500 | everyone | push | 8 | 0/72 | 0/72 | 0/72 | none reached | 19.568–23.116 | 1.00 | 0 |
| 1500 | everyone | push-late-dedupe | all | 37/72 | 37/72 | 37/72 | 5.076–5.147 | 74.410–79.140 | 11.63 | 0 |
| 1500 | everyone | push-late-dedupe | 22 | 0/72 | 0/72 | 0/72 | none reached | 50.608–55.802 | 7.15 | 0 |
| 1500 | everyone | push-late-dedupe | 16 | 0/72 | 0/72 | 0/72 | none reached | 37.048–42.742 | 5.42 | 0 |
| 1500 | everyone | push-late-dedupe | 8 | 0/72 | 0/72 | 0/72 | none reached | 19.534–23.073 | 2.95 | 0 |

## Scope and interpretation

- The fixed-size mode is stake-weighted with quorum against total active stake. Its 900-seat request seats all 216 available pools at 750 nodes and all 458 at 1500 nodes. The everyone mode supplies the larger vote-volume stress test but uses node-count quorum. These modes differ in both voting weight and vote volume.
- A fanout result applies to the tested topology, seed and validation order. The tables supersede the previous general recommendation that 22 is usable and lower limits are unusable; the share of stake at nodes with a quorum and actual endorsement counts matter. Three seeds do not establish a universal safe limit.
- Quorum by 7s is an early-attainment metric. Voting closes at 7s; vote diffusion has an additional allowance until the 14s inclusion boundary.
- The fixed 400-slot horizon can leave the newest EBs unfinished. Means are conditional on attaining quorum. Equal counts do not establish equal EB identities, and these summary-only runs do not contain per-EB traces.
- Shared header approximations can affect differences between arms through CPU queueing, header eligibility and subsequent votes. Missing per-peer serialization, notification credits and shared queues limit quantitative transfer to Haskell. Full node parity remains outside this voting-strategy study.

## Provenance and reproduction

Upstream config: `ouroboros-leios` `f307ed5fa7077a32eb470ca3832a34092882bfe3`. Binary SHA-256: `cafe6ca9f9f4b36432a9682cbb23f001b3dac93f8679867e887e5201282ba873`. Offered load: 6 ms interarrival (~167 tx/s), 1500-byte transactions starting at 60s. Each simulation ran 400 slots with echo disabled, four cores per node and 10 Mb/s links. Runs executed sequentially using a frozen executable and checksum-verified inputs.

The [study guide](../vote-diffusion-study.md) documents the matrix. [results.json](results.json) contains all per-run metrics and matched comparisons; [runs.csv](runs.csv) records completed runs. Reproducibility artifacts:

- [inputs.tar.gz](inputs.tar.gz): all 113 original input files (including both topologies and all 108 per-run overlays), plus the empty tracked-source patch. [input-sha256.json](input-sha256.json) checks the original bytes inside this archive. Extract this archive to inspect the exact configurations used.
- [summary-logs.tar.gz](summary-logs.tar.gz): the 108 original summary logs, checked by [log-sha256.json](log-sha256.json). These are summaries, not per-event traces.
- [extract-results.py](extract-results.py): the summary parser, without runner controls or publication side effects. It can regenerate the results from these artifacts. No additional simulation is needed for this check.
- [binary.sha256](binary.sha256) and [revision.txt](revision.txt): the original executable hash and source revision. The executable remains local. Rebuilding on another platform need not reproduce its binary hash.

To verify and re-extract in a new directory, from this report directory:

```sh
mkdir /tmp/leios-results-check
tar -xzf inputs.tar.gz -C /tmp/leios-results-check
tar -xzf summary-logs.tar.gz -C /tmp/leios-results-check
cp runs.csv revision.txt upstream-revision.txt binary.sha256 /tmp/leios-results-check/
python3 extract-results.py --archive /tmp/leios-results-check
```

Raw simulator runs used the frozen executable at revision `0769c073` with no tracked source diff. Inputs and executable checksums were verified before execution and again after the batch. All 108 logs have final protocol/network summaries, no logged error/panic, consistent acceptance accounting and consistent quorum/deadline counts. Parsed results were independently regenerated from the published archives. `passed` means the simulation completed successfully, not that every EB reached quorum.

### Regenerating the figures

[plot-results.py](plot-results.py) reads only the committed `results.json` and
writes the three PNGs in `figures/`. It uses per-seed paired comparisons, summed
counts, and `sum(verifications) / sum(accepted)` for pooled verification cost;
no plotted measurements are entered manually. The figures were rendered with
Python 3.13 and Matplotlib 3.11.2:

```sh
python3 -m venv /tmp/leios-figure-env
/tmp/leios-figure-env/bin/pip install matplotlib==3.11.2
/tmp/leios-figure-env/bin/python plot-results.py
```

The source revision above remains the provenance of the 108 experiments. The
subsequent PR scope cleanup removes an unused transport mode and limits complete
vote-traffic reporting to Linear; it does not change the three studied Linear
transport modes, their inputs, or these archived results.
The cleanup passes 155 Rust tests (one ignored). Twenty paired 8-node, 40-slot
release runs against the frozen study binary produced identical complete
protocol and network summaries. They covered both committees, two seeds, all
three transports, and both push orders with unlimited and bounded fanout, using
uneven stake and 100 ms verification to exercise concurrent duplicates.
