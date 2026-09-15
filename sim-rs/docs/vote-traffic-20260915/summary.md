# Per-node vote traffic

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
