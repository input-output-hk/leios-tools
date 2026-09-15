# Vote traffic at individual nodes

One representative unrestricted-push run: **1,500 nodes, stake-weighted committee,
seed 0, 400 simulated seconds**. The topology contains 458 separate BPs and
1,042 relays. This is a breakdown of one case, not an additional fanout study.

The run sent **32.684092 GB** of vote bodies across the network.
The configured 10 Mbit/s bandwidth is per link, not a node-wide uplink limit.
Summing the 1,500 per-node records reproduces the global byte and message totals.
Relays account for **98.82% of sent vote bytes**. The median BP sent **0.841 MB**,
the median relay **25.970 MB**, and the busiest relay **153.114 MB** over the run.
Median received traffic was **1.581 MB per BP** and **30.577 MB per relay**.

The body sends/arrivals, accepted votes, completed verifications, generated EBs
and votes, L1 endorsements, and first/Q50/Q95 quorum summaries match the archived
case exactly; see [comparison.json](comparison.json).

Duration: 400 simulated seconds. Decimal GB/MB and Mbit/s. BP means a stake-holding node in a topology that separates BPs from relays.

Traffic includes duplicate bodies plus vote announcements and requests. Sends are recorded when queued; receives when delivered. No TCP/IP framing or other protocols are included. Peaks are the busiest fixed one-second buckets aligned to simulation time zero, not instantaneous or sliding-window link throughput. A per-node send peak sums all its outgoing links.

| Role | Nodes | Sent GB | Received GB | Median sent MB/node | p95 sent MB/node | Max sent MB/node |
|---|---:|---:|---:|---:|---:|---:|
| BP | 458 | 0.385164 | 0.721920 | 0.841 | 0.841 | 0.841 |
| relay | 1042 | 32.298928 | 31.962172 | 25.970 | 52.622 | 153.114 |
| all | 1500 | 32.684092 | 32.684092 | 24.273 | 47.613 | 153.114 |

Percentiles below are across nodes, using each node’s own busiest one-second window. Nodes can peak at different times.

| Role | Median node peak sent Mbit/s | p95 | Max | Median node peak received Mbit/s | p95 | Max |
|---|---:|---:|---:|---:|---:|---:|
| BP | 0.333 | 0.333 | 0.333 | 0.626 | 0.649 | 0.663 |
| relay | 10.287 | 20.846 | 60.649 | 12.111 | 14.435 | 16.424 |
| all | 9.614 | 18.858 | 60.649 | 11.301 | 14.105 | 16.424 |

The [per-node CSV](nodes.csv) includes separate body/control counts, sent/received totals, mean rates, and each peak’s window start. Raw JSON retains every node’s one-second buckets.

## Capture and provenance

- [Per-node CSV](nodes.csv): one row per node, with counts, bytes, mean rates and peaks.
- [Raw per-node report](vote-traffic.json.gz): counts and one-second buckets.
- [Simulator log](run.log.gz), [run manifest](runs.csv), [input archive](inputs.tar.gz),
  and [input checksums](input-sha256.json).
- Source revision: `9ff6600f44c07816498aca9db31c23c557e99be2`; the source patch in the input archive is empty.
  [Binary checksum](binary.sha256) identifies the frozen executable.
  Its [embedded version](binary.txt) says `5a88d7a`: Cargo reused the identical
  executable built with the capture changes immediately before their commit.
  `revision.txt` records the committed source that supplied those changes.
  This is a historical provenance limitation. The current runner verifies the
  embedded revision, rebuilds stale executables, and rejects a remaining mismatch.
- Study configuration, topology, workload and engine inputs are byte-identical to
  the [published inputs](../vote-diffusion-results-20260910/inputs.tar.gz).
  The overlay adds `vote-push-fanout-protects-producers: true`, which has no effect
  with unlimited fanout. Vote bodies are 94 bytes. There are no vote control
  messages in this push case; announce/request still uses 8-byte controls.
- [Artifact checksums](artifact-sha256.json) cover every other file in this directory.

## Reproduce

Build the recorded source revision, extract `inputs.tar.gz` to a new directory,
then run from `sim-rs` (substitute that directory for `/tmp/vote-inputs`):

```sh
cargo build --release --locked --bin sim-cli
target/release/sim-cli /tmp/vote-inputs/topology-1500.yaml -s 400 \
  -p /tmp/vote-inputs/study-config.yaml -p /tmp/vote-inputs/workload.yaml \
  -p /tmp/vote-inputs/engine.yaml \
  -p /tmp/vote-inputs/1500-top-stake-seats-push-fall-s0.yaml \
  --vote-traffic /tmp/vote-traffic.json > /tmp/vote-traffic.log 2>&1
python3 scripts/summarize-vote-traffic.py /tmp/vote-traffic.json \
  --duration 400 --log /tmp/vote-traffic.log --output /tmp/vote-breakdown
# When summarizing the archived version 1 report with the current script, add:
# --legacy-runs docs/vote-traffic-20260915/runs.csv
```

The study runner can perform the same capture with
`VOTE_STUDY_TRANSPORTS=push VOTE_STUDY_FANOUTS=all VOTE_STUDY_NODE_TRAFFIC=1`;
see the [complete command](../vote-diffusion-study.md#per-node-vote-traffic).

Validation: 162 Rust tests passed (one ignored), ten runner/extractor checks,
and two report-summary checks. Two paired 8-node, 80-slot tests covered push and
announce/request with capture enabled/disabled; every existing protocol and
network summary matched, and per-node traffic reconciled with both logs.
