# Linear Leios vote diffusion study

The question is whether simple vote streaming remains feasible with a large
committee, and whether selective fetching or reduced fanout improves timing,
bandwidth or verification cost. The comparison covers Linear Leios. Full Haskell
node parity is not a prerequisite for this experiment.

Start with the [focused findings](vote-diffusion-followup-20260915/README.md)
for the BP-protection comparison, control-size sensitivity and per-node traffic.
The [original 108-run report](vote-diffusion-results-20260910/README.md) also
covers both committees and duplicate-handling orders.

## Transport terminology

The [transport definitions in the report](vote-diffusion-results-20260910/README.md#transport-terminology)
map terminology to the exact configuration values. `push` marks a vote's ID
seen on arrival and suppresses copies even while verification is pending.
`push-late-dedupe` marks it seen after verification, so copies received while the
first check is pending can trigger additional checks. Both discard copies of
already verified votes. This is the meaning of “early” and “late” deduplication
in earlier descriptions; neither term describes when the vote is sent.

The [Haskell source context and sim-rs comparison](vote-diffusion-results-20260910/README.md#why-compare-the-two-duplicate-handling-orders)
explain why both settings are tested: the inspected prototype records a vote as
known only after verification, so concurrent copies can repeat that work without
counting the vote twice. The alternative suppresses pending copies to measure
how much verification work they cause. Neither arm is a Haskell runtime benchmark.

## Committee reference

[CIP PR #1196](https://github.com/cardano-foundation/CIPs/pull/1196) replaced
weighted Fait Accompli with a stake-based committee. The subsequent
[PR #1250](https://github.com/cardano-foundation/CIPs/pull/1250), checked at
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
The simulator warns when seated stake cannot reach the quorum; such configurations
remain available for deliberate failure experiments. Node identifiers (topology
name order) break equal-stake ties.

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

## Forwarding and accounting

Bounded push ranks consumers using a hash of the seed, sender, vote bundle and
peer. Each vote takes its own subgraph. The rule has no repair mechanism and
does not guarantee delivery to every node.

`vote-push-fanout-protects-producers: true` prioritizes links explicitly marked
`always-forward-votes` in the topology. Protected links count toward the same
total cap: one protected BP at cap 22 leaves 21 places for other consumers.
The study runner marks each BP's two upstream connections. Source exclusion
still applies. Protection defaults to false, retaining the archived forwarding
rule. Protection without a cap is accepted with a warning because unrestricted
push already forwards to all consumers.

The original fanout results measured a rule that could omit BP recipients.
Those measurements describe that rule, rather than a general requirement to
send to every peer. The [matched follow-up](vote-diffusion-followup-20260915/README.md)
tests protection at the same caps and restores Q95 in its single-seed reference
scenario. That does not establish a universally safe cap.

Accepted arrivals count completed first acceptance, excluding locally generated
votes, redundant copies and votes for pruned EBs. Pending arrivals are separate;
received bodies reconcile as redundant plus accepted plus pending. Verification
counts include actual completed work, including obsolete work. Announcement and
request receives are recorded on delivery, not when the sender queues them.

## Measuring obsolete vote work

The new obsolete-work metrics preserve the existing transport behavior. An
obsolete bundle is one whose referenced EBs are all in the measuring node's
`pruned_ebs` set. This is local state, not simply a vote arriving after its
signing deadline. Nodes still verify, cache and forward the same messages.

The summary reports the following **subsets of the existing totals**:

- Arrivals obsolete when received, their bytes, and how many had already been
  generated or successfully processed at that receiving node.
- Verifications obsolete when completed. A validation can span a prune, so
  this count need not match arrivals already obsolete on receipt.
- First and repeat completed processing at a node. A repeat verification with
  no held copy before insertion is additionally counted as a **cache reinsertion**.
  Concurrent duplicate completions with a held copy are repeats without a
  cache reinsertion. Locally generated votes count as prior processing too.
- Bodies and announcements sent while obsolete at the sender, including bodies
  served in response to requests. Their bytes remain in the normal traffic total.

The monitor remembers prior processing in a bitset; simulated nodes never read
it. No CPU work or traffic is subtracted. The `VTBundleObsolete*` events also
appear in raw traces. Older logs have no obsolete-work breakdown: the extractor
leaves the field absent rather than inventing zeros.

This distinguishes first late processing from repeated work after the cache
has forgotten a vote. The pinned Haskell `LeiosVoteState` retains its seen-vote
set and has no garbage collection yet; it can still verify a previously unseen
late vote. Consequently, dropping every pruned-EB vote would be a separate
modeling change, not an accounting fix. Completed verification totals count
work the simulator actually performed, including work on obsolete votes. The
open modeling question is whether forgetting vote IDs during pruning matches
the intended node behavior. The new measurements do not establish
how much pruning affected the published 108 runs; those archived logs lack this
breakdown. A full matrix rerun is deferred until review or targeted evidence
justifies it, especially before making a behavior change or new effect-size claim.

## Validation

The implementation passes 168 Rust tests (one ignored), 17 runner/extractor
tests, four traffic-summary tests and nine real CLI tests. The checks cover
stake quorum, timing gates, duplicate and obsolete processing, exact archived
result extraction, message-size transmission/accounting, output aliases,
cancellation, capture completion and retry. The original 108 numeric results
still re-extract unchanged, and the historical per-node tables reproduce byte
for byte from their bundled evidence.

```sh
cargo test --workspace --locked --offline
python3 scripts/test-vote-diffusion-study.py
python3 scripts/test-summarize-vote-traffic.py
python3 scripts/test-vote-traffic-cli.py --binary target/release/sim-cli
```

## Running the matrix

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
produced. A private copy of the built executable is retained as `sim-cli`.
The runner writes all planned rows before execution, then records start/end UTC,
elapsed seconds, exit code and status after each run. Input and log checksum
manifests accompany the output; the frozen executable and inputs are checked
before each run and the full set is checked after the batch. Run status `passed`
means the configured simulation finished successfully, not that all EBs achieved quorum. A
failed simulation makes the script exit nonzero.

Set `VOTE_STUDY_CONFIG_REVISION` to the source revision of an exported base
configuration. Otherwise the runner records its enclosing Git revision if
available, or explicitly records that it is unknown. The input bytes are saved
in either case. `revision.txt` and `source.patch` identify the simulator source;
`defaults.yaml` saves the shared defaults for provenance. It is not passed as
an overlay: sim-cli applies its own TCP default after loading those defaults,
and the runner preserves that behavior.

To extract a fresh run directory, including a smaller matrix:

```sh
python3 docs/vote-diffusion-results-20260910/extract-results.py /tmp/vote-study-corrected
```

This checks that directory's own manifests and reads only its topology sizes.
Missing metrics or incomplete/failed runs produce a nonzero exit status and are
listed in `results.json`; they are never counted as parsed results. Use
`--archive` only when re-extracting the frozen September 10 archives against
the manifests shipped beside the extractor. Run the interface regression checks
with `python3 scripts/test-vote-diffusion-study.py`. Python 3.11 or later is
required by the runner and extractor.

Defaults:

| Setting | Default |
|---|---|
| Seeds | `0` unless supplied as arguments |
| `VOTE_STUDY_SIZES` | `750 1500` |
| `VOTE_STUDY_COMMITTEES` | `everyone top-stake-seats` |
| `VOTE_STUDY_FANOUTS` | `all 22 16 8` |
| `VOTE_STUDY_TRANSPORTS` | `announce-then-request push push-late-dedupe` |
| `VOTE_STUDY_ANNOUNCEMENT_BYTES` | `8` |
| `VOTE_STUDY_REQUEST_BYTES` | `8` |
| `VOTE_STUDY_NODE_TRAFFIC` | `0` (set `1` to save per-node vote traffic) |
| `VOTE_STUDY_FANOUT_PROTECTS_PRODUCERS` | `false` |
| `VOTE_STUDY_SLOTS` | `400` |
| `VOTE_STUDY_DRY_RUN` | `0` |
| `VOTE_STUDY_CONFIG_REVISION` | Detected from base config, or explicitly unknown |

For each size, committee and seed, the runner normally executes announce/request once,
then both `push` and `push-late-dedupe` at every fanout. Echo-to-source is held
false for this matrix. `VOTE_STUDY_FANOUT_PROTECTS_PRODUCERS=false` preserves
sampling across all consumers, as in the 2026-09-10 fanout rows. With `true`,
protected BP connections take places within the same total cap: one protected
BP at fanout 22 leaves 21 places for other consumers. Source exclusion still
applies. A node with more protected consumers than the cap is rejected.

The runner marks each BP's two upstream entries with `always-forward-votes: true`
in both saved study topologies. Routing uses those explicit markers, not stake.
For other topologies, mark the consumer's own relay entries under `producers`.
Enabling protection with a bounded cap requires marked links and at least one
unprotected consumer. Without a cap, protection is accepted with a warning for
compatibility with the archived unrestricted-push overlay; unlimited push already
forwards to every consumer. Node/peer roles are prepared once per node.

Run once with each protection value into separate output directories. Run names
include protection and both control-message sizes, `runs.csv` records all three,
and the extractor distinguishes these settings. Unlimited push and announce/request use
`bpfalse` because no bounded selection applies. The default is **36 runs per seed**, or 108 for the
three-seed command above. Runs execute sequentially and can take many hours.
`VOTE_STUDY_TRANSPORTS` selects a subset, including a single transport.
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

Record quorum attainment and misses per EB, Q50/Q95 attainment, vote
bodies generated, actual eligible stake, total protocol bytes, completed
verifications, accepted arrivals and pending arrivals. Keep a fixed scenario
and seed across each comparison. The reported Q95 time is a mean of per-EB times
at which nodes holding 95% of stake each have a quorum; it is not a worst-case
deadline guarantee. Preserve miss counts alongside conditional timing averages.

## Limits on transfer to the Haskell node

The [pinned Haskell audit in the report](vote-diffusion-results-20260910/README.md#why-compare-the-two-duplicate-handling-orders)
links the exact node and consensus revisions and explains the checks before and
after verification that motivate `push-late-dedupe`. The simulator does not
reproduce the node's per-peer serial handling, shared notification queue or
credit-dependent vote drops. The audit was source inspection, not a node runtime benchmark.

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

## Per-node vote traffic

The [focused report](vote-diffusion-followup-20260915/README.md) provides BP/relay
comparisons and captures all 1500 nodes in each of its ten cases. The earlier
[single unrestricted-push capture](vote-traffic-20260915/README.md) remains
available with its original provenance.

The archived 108 runs saved network totals, not individual-node traffic. The
report's [Wire GB range](vote-diffusion-results-20260910/README.md#fanout-and-validation-order)
is the sum sent across the network, with each send counted once. It is not a
per-node value or a sum of sends plus receives.

`sim-cli --vote-traffic report.json` saves an optional, vote-only report without
retaining or serializing a full event trace. It supports the two Linear Leios
variants. Each configured node has body, announcement and request counts/bytes
in both directions, plus fixed one-second byte buckets indexed from simulation
time zero. Duplicate and obsolete bodies remain included in those totals;
classification events do not add the same traffic twice. State scales with
node-seconds rather than the number of message copies. Body sizes are carried
on arrivals, so the recorder needs no bundle-size cache.
The destination must not already exist or alias the event output. A temporary
file is opened before the simulation starts and published only after successful
completion. Monitor failures cancel the simulation, and failed/interrupted
captures leave the destination available for retry. Ctrl-C on an ordinary run
without capture still saves events and exits successfully. The study runner
requires the full-duration completion marker before marking any run passed,
including runs without traffic capture.

Send timestamps mean **queued for transmission**; receive timestamps mean
**delivered**. A peak is the busiest fixed one-second bucket, not instantaneous
NIC throughput or a sliding-window maximum. Sending to multiple links can
produce a node-wide rate above one link's bandwidth. These are modeled vote
mini-protocol bytes only; they exclude TCP/IP framing and other protocols.
The report records the requested slot count and last observed event time.
The summarizer requires a final completion marker and an exactly matching
finite duration before computing mean rates. Stake distinguishes BPs from relays only in topologies that model
the BP as a separate node.

For one run with the original study inputs, use the study configuration extracted
from `vote-diffusion-results-20260910/inputs.tar.gz` as the first argument:

```sh
VOTE_STUDY_SIZES=1500 VOTE_STUDY_COMMITTEES=top-stake-seats \
VOTE_STUDY_TRANSPORTS=push VOTE_STUDY_FANOUTS=all \
VOTE_STUDY_NODE_TRAFFIC=1 VOTE_STUDY_SLOTS=400 \
VOTE_STUDY_CONFIG_REVISION=f307ed5fa7077a32eb470ca3832a34092882bfe3 \
  scripts/vote-diffusion-study.sh /tmp/study-config.yaml /tmp/vote-node-traffic 0

python3 scripts/summarize-vote-traffic.py \
  /tmp/vote-node-traffic/1500-top-stake-seats-push-fall-bpfalse-a8-r8-s0.vote-traffic.json \
  --duration 400 \
  --log /tmp/vote-node-traffic/1500-top-stake-seats-push-fall-bpfalse-a8-r8-s0.txt \
  --output /tmp/vote-node-traffic/breakdown
```

The runner hashes captured reports in `vote-traffic-sha256.json`. The summarizer
checks per-node time buckets against totals and reconciles send counts/bytes
and body arrivals against the final global log summary. Version 1 archives
also require the original successful `runs.csv` through `--legacy-runs`; their
logs predate the completion marker. It writes `nodes.csv` and `summary.md`,
with BP/relay totals and percentiles across nodes. Its input also accepts `.json.gz`.
A report from one unrestricted-push run explains traffic distribution for that
case; it does not test whether the protected-BP fanout rule restores quorum.

The runner checks the executable's embedded revision against `revision.txt`,
rebuilds the CLI if a cached version is stale, and stops if they still differ.
The build script tracks common Git refs in linked worktrees. Historical binary
metadata remains unchanged in archived results.

Capture regression checks exercise the actual CLI, including existing outputs,
path aliases, monitor failure, interruption and retry:

```sh
python3 scripts/test-vote-traffic-cli.py --binary target/release/sim-cli
python3 scripts/test-summarize-vote-traffic.py
```

## Focused fanout and control-size follow-up

The [published pilot](vote-diffusion-followup-20260915/README.md) completes the
ten cases below for seed 0, with reconciled per-node traffic and a comparison
against the five corresponding archived default cases.

`vote-announcement-size-bytes` and `vote-request-size-bytes` independently set
Linear Leios vote-bundle control-message sizes. Both default to 8. Sizes include
the identifier and application framing and exclude TCP/IP. They are carried by
the messages through the network scheduler and arrival accounting, so changing
them changes transmission time as well as the reported bytes. Other protocol
messages keep their existing sizes. Nondefault sizes on unsupported Leios
variants and zero sizes are rejected.

The generic runner exposes these as `VOTE_STUDY_ANNOUNCEMENT_BYTES` and
`VOTE_STUDY_REQUEST_BYTES`. Every run name, overlay, runs.csv row and extraction
key records both sizes; paired comparisons use a matching control-size baseline.
Old CSVs are interpreted with the historical 8/8 defaults.

The focused runner freezes ten cases per seed into one matrix and runs them with
one checked executable: push at fanout 22/16/8 with and without BP protection,
unlimited push, and announce/request at 8/8, 40/40 and 64/64 bytes. Sizes above 8
are sensitivity assumptions, not verified encodings. Its scope is 1500 nodes
and stake-weighted voting; it does not repeat the everyone-votes stress test.

```sh
# First extract study-config.yaml from the original input archive.
VOTE_STUDY_CONFIG_REVISION=f307ed5fa7077a32eb470ca3832a34092882bfe3 \
  python3 scripts/vote-diffusion-followup.py /tmp/study-config.yaml /tmp/vote-followup 0
python3 scripts/summarize-vote-diffusion-followup.py /tmp/vote-followup \
  --output /tmp/vote-followup/breakdown
```

The default duration is 400 slots; `VOTE_STUDY_SLOTS` changes it. A dry run uses
`VOTE_STUDY_DRY_RUN=1`. Per-node capture defaults on for this focused matrix and
can be disabled with `VOTE_STUDY_NODE_TRAFFIC=0`. All inputs, scripts, row names,
logs, capture files and the executable checksum are retained. One seed is a
pilot comparison; repeat promising cases across seeds before broad claims.

The focused summarizer verifies capture checksums, completed logs, topology
membership and control-message sizes, then reconciles every per-node report
against the final network totals. Its tables compare BP protection under a
fixed total cap and executed control-size variants against the same unrestricted
push baseline. Separate per-node CSVs retain the traffic distribution for every
case. It also compares unprotected 8/8 outcomes with matching archived runs.
