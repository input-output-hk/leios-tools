# Vote traffic at individual nodes

One representative unrestricted-push run: **1,500 nodes, stake-weighted committee,
seed 0, 400 simulated seconds**. The topology contains 458 separate BPs and
1,042 relays. This is a breakdown of one case, not an additional fanout study.
The [subsequent ten-run pilot](../vote-diffusion-followup-20260915/README.md)
adds matched fanout and control-size comparisons, with per-node captures using
an executable whose embedded revision matches its recorded source.

The run sent **32.684092 GB** of vote bodies across the network.
The configured 10 Mbit/s bandwidth is per link, not a node-wide uplink limit.
Summing the 1,500 per-node records reproduces the global byte and message totals.
Relays account for **98.82% of sent vote bytes**. The median BP sent **0.841 MB**,
the median relay **25.970 MB**, and the busiest relay **153.114 MB** over the run.
Median received traffic was **1.581 MB per BP** and **30.577 MB per relay**.

The body sends/arrivals, accepted votes, completed verifications, generated EBs
and votes, L1 endorsements, and first/Q50/Q95 quorum summaries match the archived
case exactly; see `comparison.json` in the evidence archive.

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

## Evidence and provenance

[Download the original evidence archive](evidence.tar.gz). It preserves the
original README, per-node CSV, raw report, logs, inputs, run record, comparison,
revision information and checksums byte for byte. They are bundled to keep the
PR focused on simulator changes and findings.

The recorded source is `9ff6600f44c07816498aca9db31c23c557e99be2`, while the
executable reports `5a88d7a`. This historical version mismatch remains disclosed
in the saved README; the current runner rejects such mismatches. The source
patch is empty and the binary hash is retained. Protection was set true in this
unrestricted-push capture, where it has no effect. Bodies are 94 bytes and this
case sends no control messages.

The archive SHA-256 is
`96ebe7c6e8d1cb1611539e84eb38ea9cb0166a9812498dc408d32ea30e99b3c4`.
The `artifact-sha256.json` inside it verifies every original file, including the
original README rather than this wrapper.

To reproduce the tables with the current summarizer, from `sim-rs`:

```sh
capture_dir=$(mktemp -d)
tar -xzf docs/vote-traffic-20260915/evidence.tar.gz -C "$capture_dir"
gzip -dc "$capture_dir/run.log.gz" > "$capture_dir/run.log"
python3 scripts/summarize-vote-traffic.py "$capture_dir/vote-traffic.json.gz" \
  --duration 400 --log "$capture_dir/run.log" \
  --legacy-runs "$capture_dir/runs.csv" --output "$capture_dir/reproduced"
```

The generated `nodes.csv` and `summary.md` match the saved tables byte for byte.
See the [study guide](../vote-diffusion-study.md#per-node-vote-traffic) for current
capture commands. The original 108 runs remain in the
[main archive](../vote-diffusion-results-20260910/README.md).
