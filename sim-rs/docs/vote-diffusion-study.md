# Linear Leios vote diffusion study

The question is whether simple vote streaming remains feasible with a large
committee, and whether selective fetching or reduced fanout improves timing,
bandwidth or verification cost. The comparison covers Linear Leios. Full Haskell
node parity is not a prerequisite for this experiment.

## Committee reference

[CIP PR #1196](https://github.com/cardano-foundation/CIPs/pull/1196) replaced
weighted Fait Accompli with a stake-based committee. The subsequent
[open PR #1250](https://github.com/cardano-foundation/CIPs/pull/1250), checked at
[`e173ea52`](https://github.com/cardano-scaling/CIPs/blob/e173ea5250e99e93db3436205f32f755169f1866/CIP-0164/README.md#committee-structure),
proposes a fixed committee size. These are distinct selection rules:

| Simulator mode | Eligible voters | Vote weight | Quorum denominator |
|---|---|---|---|
| `everyone` | Every topology node, including relays | 1 | Number of nodes |
| `top-stake-seats` | Top N stake-holding pools | Pool's active stake | Total active stake |
| `top-stake-fraction` | Pools covering the configured stake fraction | Pool's active stake | Total active stake |

The fixed-size reference uses `committee-seat-count: 900` and
`quorum-weight-fraction: 0.75`. Fewer than 900 pools means all available pools
are seated. Unseated stake remains in the denominator: a committee holding less
than 75% of total active stake cannot certify, even if all its members vote.
The simulator uses node identifiers to break equal-stake ties.

The 750/1500-node everyone-votes scenarios are deliberate vote-volume stress
tests, also discussed in the CIP proposal. They do not implement stake quorum.
The supplied topologies have fewer than 900 stake-holding pools, so the
fixed-size reference alone does not exercise 900 distinct voters. Keep both
committee modes and report the actual eligible pool count and stake coverage.
A 1500-node everyone-votes run requires 1125 votes for its count-based quorum;
that threshold is separate from how many votes actually get cast.

## Workload and timing

`study-linear-tx-load.yaml` uses a **6 ms interarrival interval**, approximately
167 transactions per second in total, with 1500-byte transactions from 60 to
960 seconds. It overrides the upstream study's 10 ms interval. This preserves
the previous experiment's load; the earlier description of 6 tx/s was wrong.
It represents configured offered load, not achieved throughput.

With the study configuration, voting opens at `t0 + 3 * L_hdr = t0 + 3s` and
closes at `t0 + 3 * L_hdr + L_vote = t0 + 7s`. Both the EB producer and other
voters obey this window. Receiving an EB before the deadline does not suffice:
validation and vote signing must finish by the deadline too. Late completion
is reported under `LateEB`. Vote diffusion and certificate inclusion have their
separate `L_diff` allowance; the summary prints the inclusion boundary as well.
A quorum-by-7s metric measures earlier attainment, not the entire inclusion
allowance.

## Corrections and status of earlier findings

Results previously reported on PR #1 came from revision `6e24ebe8`. The
[corrected 108-run study](vote-diffusion-results-20260910/README.md) now provides
measurements of the fixed simulator; earlier figures below are historical.

- `top-stake-seats` previously gave each pool weight 1 and used filled seats
  as its quorum denominator. It now uses pool stake and total active stake.
- Accepted arrivals are counted from completed first acceptance, excluding
  locally generated votes, redundant copies and votes for pruned EBs. Pending
  arrivals are reported separately. `received - duplicates` previously counted
  unfinished verifications as accepted and understated verification cost per
  acceptance. The obsolete-vote classification preserves existing relay behavior.
- Vote announcement/request receives are emitted on delivery. Aggregate output
  no longer credits the recipient when the sender merely queues a message.
- The matrix runner sets fanout and transport independently in each generated
  overlay, so a fanout sweep cannot override `push-late-dedupe` with `push`.
- Producers now wait for the voting gate. Both validation and signing completion
  are checked against the deadline. These behavior changes require rerunning
  comparisons before quoting the old timings as measurements of current code.

The earlier everyone-votes comparison reported 1500-node quorum timings of
about 3.525s for push versus 4.002s for announce/request at the 95%-stake observer,
and a traffic ratio of about 7.9. Those remain historical observations for their
configured arms. Equal certified-block counts do not prove identical certified
EB identities; that claim requires comparing identifiers in traces.

The old fanout conclusion is **superseded by the corrected matrix**. Fanout 22
can retain median-observer quorum while losing the 95%-stake observer entirely.
In the 1500-node everyone-votes late-deduplication arm it reduced verification
work and increased L1 endorsements from 13 to 18 across three seeds, but Q95
attainment fell from 37/72 EBs to zero. The stake-weighted reference likewise
lost Q95 with every tested bounded fanout. See the corrected report for the full
matrix, missed-quorum counts and the distinction between observer coverage and
vote-body coverage. These results do not establish a safe fanout limit.

## Validation of these fixes

- `cargo test --workspace --locked --offline`: 155 passed, one ignored (after removing the unused no-deduplication mode and its test).
- Twenty 8-node, 40-slot smoke runs: two seeds, both committee modes,
  announce/request, and both push dedupe orders with unlimited and bounded
  fanout. These check execution and accounting, not mainnet-scale feasibility.
- An overloaded trace reconciled exactly with its summary: 56 distinct relevant
  acceptances, 14 pending arrivals and 98 completed verifications.
- An uneven-stake trace counted eight generated vote bodies separately from
  2400 total voting weight. All 16 reported observer quorums met 750 stake out
  of 1000 total active stake.
- A dry-run check covered 40 planned matrix entries, checking that fanout and
  validation order were crossed under matching seeds, sizes and committees.

The [750/1500-node performance study](vote-diffusion-results-20260910/README.md)
is now complete. The checks above established correctness before those reruns.

## Running the corrected matrix

From `sim-rs`, pass the upstream study configuration (the earlier runs used
`ouroboros-leios` revision `f307ed5`):

```sh
./scripts/vote-diffusion-study.sh \
  <ouroboros-leios>/analysis/sims/2026w18/experiments/config.yaml \
  /tmp/vote-study-corrected 0 1 2
```

The output directory must be new and its parent must exist. Each run saves a
summary, a final parameter overlay, and a row in `runs.csv`. The directory also
contains copies of the base configuration, workload, engine configuration and
topologies, plus the source revision, tracked diff and built binary SHA-256. No trace files are
produced. Run status `passed` means the simulator exited successfully, not that
all EBs achieved quorum. A failed simulation makes the script exit nonzero.

Defaults:

| Setting | Default |
|---|---|
| Seeds | `0` unless supplied as arguments |
| `VOTE_STUDY_SIZES` | `750 1500` |
| `VOTE_STUDY_COMMITTEES` | `everyone top-stake-seats` |
| `VOTE_STUDY_FANOUTS` | `all 22 16 8` |
| `VOTE_STUDY_SLOTS` | `400` |
| `VOTE_STUDY_DRY_RUN` | `0` |

For each size, committee and seed, the runner executes announce/request once,
then both `push` and `push-late-dedupe` at every fanout. Echo-to-source is held
false for this matrix. The default is **36 runs per seed**, or 108 for the
three-seed command above. Runs execute sequentially and can take many hours.
Use a smaller matrix first, or preview it without building or running:

```sh
VOTE_STUDY_DRY_RUN=1 \
VOTE_STUDY_SIZES='1500' \
VOTE_STUDY_COMMITTEES='top-stake-seats' \
VOTE_STUDY_FANOUTS='all 22' \
./scripts/vote-diffusion-study.sh <study-config.yaml> /tmp/vote-study-plan 0 1 2
```

This plans 15 runs: five arms per seed. Rerun the command with a new output
directory and without `VOTE_STUDY_DRY_RUN=1` to execute them. The command preserves
the supplied config rather than silently fetching a moving upstream revision.

For an individual experiment, reuse the corresponding generated overlay from
that plan directory, or extract the exact overlay from the published input
archive. The runner produces the complete committee, seed, transport and fanout
settings together; separate transport presets are unnecessary.

Record quorum attainment and misses per EB, observer stake percentiles, vote
bodies generated, actual eligible stake, total protocol bytes, completed
verifications, accepted arrivals and pending arrivals. Keep a fixed scenario
and seed across each comparison. The 95%-stake observer statistic is a mean of
per-EB observer quantiles; it is not a worst-case deadline guarantee. Preserve
miss counts alongside conditional timing averages.

## Limits on transfer to the Haskell node

The Haskell source audit used cardano-node `afa091b4`, whose `cabal.project`
pins ouroboros-consensus `7abeda65`. Its seen-check, verification and insertion
order motivates `push-late-dedupe`. The simulator does not reproduce the node's
per-peer serial handling, shared notification queue or credit-dependent vote
drops. The audit was source inspection, not a node runtime benchmark.

The simulator's ranking-header identifier announcement/request/body exchange
also differs from the node's full-header Leios announcements. Haskell validates
those announcements before forwarding; its separate ChainSync path remains
relevant too. A simple one-latency network calculation omits queueing and
validation and cannot establish that no real header misses `L_hdr`.

A shared header approximation does not necessarily cancel between arms:
verification load changes CPU queueing, which changes header eligibility and
subsequent votes cast. This feedback can change the transport comparison as
well as absolute rates. Header and flow-control sensitivity are follow-up
checks for transferring quantitative backlog or certification claims to the
node. The present matrix answers voting-strategy feasibility within the stated
model; it does not predict the prototype's exact certification loss.
