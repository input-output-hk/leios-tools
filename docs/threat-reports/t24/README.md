# T24 — Submit duplicate transactions

**Status:** final for the proto-devnet scope. Three fresh runs agree (2026-11-23, w43 Haskell nodes, cardano-node 11.2.0.164).

**Result:** the receiver's deduplication protects the node-to-node path and not the local submission path.
- **Node-to-node TxSubmission:** a repeated or concurrently offered tx id cost node1 nothing beyond the first copy. It requested the body once and validated it once, whether the tx was re-announced 20 times on one connection or offered on three connections within a millisecond: **MITIGATED**.
- **Local submission** (`cardano-cli transaction submit`, the path wallets and HTTP submit services use): every resubmission of the same tx went through ledger validation before rejection (`AllInputsAreSpent`): **WASTE**, post-admission.
  - The waste is bounded at one validation per duplicate, about 0.15 ms. Receiver work equals the attacker's bytes: no amplification.
  - Nothing was re-announced downstream: node2 and node3 each fetched each tx from node1 once.

## Threat

The threat model defines T24 as a client-side attack ([`threat-model.md`](https://github.com/input-output-hk/ouroboros-leios/blob/main/docs/threat-model.md), "Transaction-Based Denial of Service"): duplicate transactions submitted "as regular client nodes" through "normal transaction submission mechanisms", costing network bandwidth, with pull-based diffusion and validation as the mitigations. The threat model names no submission path, so both client paths to a Haskell node are tested. Duplicates *inside an endorser block* are threat T11 ([`../t11/report.md`](../t11/report.md), variant `duplicate`) and are out of scope here.

## Method

The harness is the internal `make -C proto-devnet t24` target. Its design is in the internal `specs/008-t24-duplicate-txsubmission` spec and its usage in the internal `proto-devnet/README.md` (T24 section).

- **Devnet:** each run resets the proto-devnet: three Haskell block producers on w43, with the net-rs observer at `NETRS_FORGE=0`, so genesis and stake are unchanged. The overflow centrifuge is off. The receiver under test is `node1`.
- **Funding:** one fan-out tx gives each role a fresh key, so every tx spends an independent input. The payloads are built with the unchanged `make-magazine.sh --mode duplicate`.
- **Vectors:**

  | Vector | What repeats | Attacker |
  |---|---|---|
  | `local-repeat` | One signed tx, submitted 20 times back to back to node1's local socket | `cardano-cli`, unchanged |
  | `n2n-single` | One tx id, announced 20 times on one TxSubmission connection, at 20 tx/s | S: zero-stake net-node with the duplicate-admission flag |
  | `n2n-concurrent` | One tx, offered once on each of 3 connections, released together | C0–C2: three stock zero-stake net-nodes |

  Legend: *local socket* = the node's LocalTxSubmission interface; *duplicate-admission flag* = `NET_NODE_FLAG_ALLOW_DUPLICATE_TXS`, which makes net-node re-admit and re-announce a tx id it already holds (leios-tools PR #123); *stock* = no flag.

- **Controls and baseline:** each path has a positive control, a separate tx offered once that must be admitted and settled. A stock attacker B with a 20-copy magazine gives the baseline: it announced the tx once, so stock net-node cannot exercise T24.
- **Evidence:** both sides are recorded.
  - *Attacker side:* every TxSubmission message (`net_core::txsubmission::client` tracing, leios-tools PR #123), plus the CLI's records.
  - *Receiver side:* node1's TxSubmission and Mempool traces (`DEVNET_TRACE_PROFILE=t24`), plus a `tx-exists` mempool poll.
- **Classification:** an offline classifier (`proto-devnet/scripts/t24_report.py`) assigns each vector MITIGATED, WASTE, DUPLICATE-ADMITTED, REJECTED-BY-DISCONNECT or INCONCLUSIVE.
  - Only repeats made while the tx was in flight or in node1's mempool count (FR-011).
  - Missing, contradictory or ambiguous evidence gives INCONCLUSIVE.

## Results

Three counted runs, each on a fresh devnet with fresh transactions and the same configuration (`T24_N` = 20, `T24_K` = 3, `T24_TX_RATE` = 20, `T24_LOCAL_SPACING` = 0). In every run, both controls were admitted and settled, the baseline was 1 announcement, and each of the five attackers had exactly one connection to node1, on both sides.

| Run | local-repeat | n2n-single | n2n-concurrent | Protected path |
|---|---|---|---|---|
| `20261123T223534Z` | WASTE (19 in scope; 19 validations, 3.05 ms) | MITIGATED (20 in scope; 0 extra) | MITIGATED (2 in scope; 1 fetch, 1 validation) | `n2n` |
| `20261123T224117Z` | WASTE (19; 19, 2.70 ms) | MITIGATED (20; 0) | MITIGATED (2; 1, 1) | `n2n` |
| `20261123T224449Z` | WASTE (19; 19, 2.74 ms) | MITIGATED (20; 0) | MITIGATED (2; 1, 1) | `n2n` |

Legend: *in scope* = repeats made while the tx was in node1's mempool (FR-011); *validations* = node1 mempool attempts beyond the first; *extra* = body requests or validations beyond the first; *fetch* = node1's body requests for the tx across all three connections; *Protected path* = `report.json` `comparison.protected_path`. All WASTE verdicts are post-admission.

The classes are identical in all three runs, so the result is repeatable (FR-017, SC-005). Each archived evidence bundle (retained internally) re-classifies to the same verdicts from its raw evidence with the offline classifier (`proto-devnet/scripts/t24_report.py`).

### Cost per duplicate (SC-004)

| Path | Bytes the receiver received | Validation time | Receiver work per attacker byte |
|---|---|---|---|
| Local | 239 (the whole signed tx) | 0.142–0.161 ms | 1.0 |
| Node-to-node, one connection | 43 (an announcement) | 0 | 0.0 |
| Node-to-node, three connections | 43 per extra offer | 0 | 0.0 |

Legend: *Receiver work per attacker byte* = (extra body bytes + extra validations × signed tx size) ÷ the bytes the attacker sent for the duplicates. 1.0 means the receiver did as much work as the attacker sent; 0.0 means none beyond the first copy. Ranges are over the three runs.

### What node1 did

- **One connection (n2n-single):** S announced the tx 21 times, each time in its own `MsgReplyTxIds`: 20 magazine copies and 1 echo, sent after node1 gossiped the tx back. Node1 fetched and validated it once; the other 20 announcements cost it only the 43-byte announcement.
- **Three connections (n2n-concurrent):**
  - In run `20261123T224449Z` all three offers reached node1 within 0.12 ms, before it admitted the tx. It fetched from one attacker only.
  - In the uncounted run `20261123T221257Z`, a second offer arrived while the first body was in flight. Node1 did not fetch it again, although V2's `txInflightMultiplicity` = 2 would allow that.
  - In the other runs the offers spread over 3.7–16.5 ms, and node1 admitted the tx before the second one arrived.
- **Local resubmission:** node1 accepted the tx once. It attempted and rejected each resubmission with `MempoolRejectedByLedger`, reason `AllInputsAreSpent`, about 0.15 ms each. No rejection by tx id happened before the ledger.
- **Downstream:** in every vector, node2 and node3 each fetched the tx from node1 exactly once, after its first admission. No duplicate caused a further downstream fetch.

## Limitations

- **Privileged position:** node1 pulls node-to-node txs only from its own topology peers (bead `9zq.4`, August 2026), so the attackers are trustable local roots. That is more privileged than an arbitrary client; the unprivileged node-to-node case was not measured.
- **Same-reply repeats not exercised:** at 20 tx/s node1 answered each copy before the next arrived, so no reply ever carried the same tx id twice.
- **Local access path:** the run used node1's local socket. Deployments expose the same LocalTxSubmission path through HTTP submit services; a remote client was not measured.
- **Single host, single receiver:** one host clock for all evidence, one receiver (`node1`) on w43, measured, not read: the source available locally is w38.
- **Downstream visibility:** downstream re-announcements carry no tx ids in the Haskell traces, so downstream cost is measured as body requests only.
- **ID-level rejection markers:** no w43 example exists, so the markers are assumptions. They did not matter here: every local rejection named a ledger rule.
- **Volume not tested separately (reviewer's decision):** a local duplicate costs node1 one rejected validation, about 0.15 ms, plus 239 bytes. That is no more than admitting a valid tx. The local vector's sequential `cardano-cli` loop (one process per submission) sustained 33–34 submissions per second across the three counted runs (25–46 ms per submit, `raw/cli-submits.jsonl` in each evidence archive); that bound belongs to the harness loop, not to the local path — parallel clients or the HTTP submit services above are not bound by it. Transaction load is covered by T27 ([`../t27/CONCLUSION.md`](../t27/CONCLUSION.md)).

## Harness validation

Runs listed here check the harness (the internal `specs/008-t24-duplicate-txsubmission` spec) and do not count toward a result.

### Run `20261123T202946Z`: first n2n-single run (T031)

**Procedure**: a `T24_VECTORS=n2n-single` run of the harness (`proto-devnet/scripts/t24.sh`) with defaults (N = 20, `T24_TX_RATE` = 0.5, deadline 600 s). The devnet was fresh (`make reset`; `NETRS_FORGE=0`, `FLOOD_DISABLE=1`). Attackers B (port 4020, flag off) and S (port 4021, flag on) are each a single-node net-cluster and a local root of node1.

**Preconditions, all met**:
- Each attacker had exactly one connection to node1, on both the attacker's and node1's side.
- The n2n control was admitted and settled.
- Baseline: B's 20 copies gave 1 announcement, so the stock net-node does not exercise T24 (FR-007).
- Node1 sent each attacker its first `MsgRequestTxIds` about 114 s after the attacker started: the local-root reconnect backoff, then `TxSubmissionInitDelay`.

**Observed**:
- S announced the attack tx three times, at 20:33:47.029, 20:33:47.031 and 20:33:49.206.
- Node1 requested the body once (47.029) and validated it once (added at 47.030). It requested and validated nothing for the two repeats.
- B, which never held the attack tx, announced it at 47.0308, as did S 18 µs later. Node1 had gossiped it back to both, so S's second announcement is an echo.
- Node2 and node3 each fetched the tx from node1 once.
- The tx left node1's mempool in a block at 20:33:50.02, about 3 s after admission. S admitted magazine copies 4–21 after that, but net-node's mempool refuses a tx already applied to its chain even with the duplicate flag, so none was announced. Only 2 of the 19 intended repeats fell while the tx was in the mempool.

**Harness defects found and fixed** (commit `6cb9da96`):
- Attacker `generated transaction` lines (target `net_node::mempool`) were not parsed, so every announcement looked like an echo.
- The receiver's `MuxErrored` when the harness stopped S was classified as REJECTED-BY-DISCONNECT.
- Mempool polls were stamped before the query, up to tens of ms early.
- The phase waited out the full deadline for announcements that could no longer happen.

The classifier wrote REJECTED-BY-DISCONNECT, which was wrong. After the fixes, and with S's stop time set from B's recorded stop (a scratch copy only; the retained bundle is unchanged), the bundle classifies as MITIGATED: 2 in-scope repeats, no extra body request or validation, 43 announcement bytes per duplicate.

**Open for the next run**: the attack tx stays in node1's mempool for only a few seconds, so at 0.5 tx/s most copies arrive after settlement and are lost. All N copies need to reach node1 inside that window.

### Run `20261123T214200Z`: n2n-single at 20 tx/s (T031)

**Procedure**: as for the previous run, on a fresh devnet, with the harness fixes (`6cb9da96`) and the new default attacker rate of 20 tx/s (`a2e63fee`). It took about 4.5 minutes, from reset to teardown.
**Preconditions, all met**:
- Each attacker had exactly one connection to node1, on both sides.
- The n2n control was admitted and settled.
- Baseline: B's 20 copies gave 1 announcement.

**Observed**:
- Node1 opened both TxSubmission sessions about 2 minutes after the attackers started.
- S admitted all 21 magazine records and announced the attack tx 21 times between 21:45:51.590 and 21:45:52.616, each time in a separate `MsgReplyTxIds`: 20 magazine copies and 1 echo after node1 gossiped the tx back.
- Node1 requested the body once (51.5899) and validated it once (added at 51.5907). It did nothing for the 20 repeats. All of them fell while the tx was in node1's mempool, which lasted until 21:45:55.005 (in a block).
- Node2 and node3 each fetched the tx from node1 once.
- No disconnect was attributed to the repeats; the teardown disconnects come after `stop_requested_at`.

**Verdict**: MITIGATED.
- 20 in-scope repeats, none excluded.
- No extra body request or validation.
- 43 announcement bytes per duplicate; receiver work per attacker byte 0.0.
- Re-classifying the retained bundle offline reproduces the verdicts.

**Not exercised**: repeats inside one reply. At 20 tx/s node1 replied to each copy before the next arrived, so the same-reply case (spec edge case) did not occur.

### Run `20261123T215827Z`: first local-repeat run (T036)

**Procedure**: a `T24_VECTORS=local-repeat` run of the harness (`proto-devnet/scripts/t24.sh`) with defaults (N = 20, `T24_LOCAL_SPACING` = 1 s), on a fresh devnet (`NETRS_FORGE=0`, `FLOOD_DISABLE=1`), with no attacker processes. Every call is `cardano-cli dijkstra transaction submit` against node1's local socket. It took about 3 minutes, from reset to teardown.

**Preconditions, all met**: the local control was accepted, admitted and settled.

**Observed**:
- The first submission of the attack tx was accepted (0.44 ms from `AttemptAdd` to `AddedTx`).
- The 19 resubmissions, at 1 s intervals from 22:00:52.6 to 22:01:10.6, were all made while the tx was in node1's mempool; it was still there when observation ended.
- Every resubmission got a full mempool attempt and was rejected by the ledger: `Mempool.RejectedTx` with `errdetails = MempoolRejectedByLedger`, reason `AllInputsAreSpent`. The CLI exited 1 with `ShelleyTxValidationError … (DijkstraApplyTxError (AllInputsAreSpent :| []))`. Research R1 expected `BadInputsUTxO` from the w38 source; w43 reports `AllInputsAreSpent`.
- Each rejection took 0.16–0.35 ms (median 0.215 ms) from attempt to rejection on node1's thread, 4.2 ms in total.
- No resubmission was rejected by tx id before ledger evaluation.
- Node2 and node3 each fetched the tx from node1 once, after the first submission. No resubmission triggered a further downstream fetch.

**Verdict**: WASTE, post-admission.
- 19 in-scope duplicates, 19 extra validations.
- 239 bytes submitted per duplicate; receiver work per attacker byte 1.0.
- Re-classifying the retained bundle offline reproduces the verdict.

### Run `20261123T221257Z`: first n2n-concurrent run (T041)

**Procedure**: a `T24_VECTORS=n2n-concurrent T24_K=3` run of the harness (`proto-devnet/scripts/t24.sh`), on a fresh devnet. Three stock attackers C0–C2 (ports 4030–4032, flag off) each held one copy of the attack tx; C0 also held the n2n control, after the attack tx. They were released back to back at 22:19:28.775. The run took about 7.5 minutes, from reset to teardown.

**Preconditions, all met**:
- Each attacker had exactly one connection to node1, on both sides.
- The n2n control (offered by C0) was admitted and settled.
- Node1 opened the sessions at different times: C2's at 22:16:53, C1's and C0's at 22:19:25–28. This is node1's per-root reconnect backoff.

**Observed**:
- The three offers reached node1 within 0.825 ms: C0 at 29.115687, C2 at 29.115900 and C1 at 29.116512. All of them came before admission.
- Node1 requested the body from C0 only (29.115989) and validated it once (added at 29.117163).
- C1's offer arrived while C0's body was in flight, and node1 did not fetch it from C1 either. Its V2 policy would allow a second concurrent fetch (`txInflightMultiplicity` = 2); this run made only one.
- The tx left node1's mempool in a block at 22:19:40.
- Node2 and node3 each fetched it from node1 once.

**Verdict**: MITIGATED.
- 2 in-scope repeats, both in flight; no extra body request or validation.
- Re-classifying the retained bundle offline reproduces the verdict.

**Release timing**: research R4 expected the release to spread the offers over up to about 1 s, because net-node applies a config change only when it polls, once a second. Here they arrived within 1 ms. The report measures overlap rather than assuming it, and the limitation stays in the report.

### Runs `20261123T222153Z` and `20261123T223024Z`: first full runs, not counted (T047)

Both runs were full `t24` runs with all three vectors, `T24_TX_RATE` = 20 and the earlier `T24_LOCAL_SPACING` default of 1 s.

| Run | local-repeat | n2n-single | n2n-concurrent | Comparison |
|---|---|---|---|---|
| `20261123T222153Z` | INCONCLUSIVE: 0 in scope, 19 after the tx left the mempool | MITIGATED: 20 in scope | MITIGATED: 3 offers before admission, 1 fetch | absent |
| `20261123T223024Z` | WASTE post-admission: 19 in scope, 3.9 ms | MITIGATED: 20 in scope | MITIGATED: 3 offers before admission, 1 fetch | protected path `n2n` |

Legend: *in scope* = resubmissions or repeats made while the tx was in node1's mempool; *offers before admission* / *fetch* = C attackers' offers that reached node1 before it admitted the tx, and the body requests it made; *Comparison* = `report.json` `comparison`, which needs conclusive verdicts on both paths.

In `20261123T222153Z` node1 admitted the local attack tx at 22:25:48.054, and it was in a block at 22:25:49.013. The first resubmission, 1 s after the first submission, came too late, and so did the other 18. Its INCONCLUSIVE is the FR-011 rule doing its job, but it exposed a harness weakness. Local resubmissions are now back to back by default (`T24_LOCAL_SPACING` = 0; commit `e00ccee1`), so the three counted runs below use that configuration. All controls were admitted and settled in both runs, and every attacker had exactly one connection to node1.

## Regression gate (T050)

The `net-core` change (TxSubmission client tracing) is built into every net-node, so the repository smoke gate (a fresh devnet with net-rs forging, `NETRS_FORGE=1`) was run with the user's go-ahead. Both runs used the same build: this branch at `8a0cf096`, `leios-tools` at `a9f1291`, net-node sha256 `4e370f6fcdb5…`.

| Run | Verdict | Detail |
|---|---|---|
| 1 | FAIL | One hard consensus error, `AppliedManifestMissing`, and 7 of the required 8 mature Rust proposals |
| 2 | PASS | 10 Rust proposals, 8 validated and voted to quorum, 5 certificates, 3 cross-implementation certificates, no errors |

Legend: *mature Rust proposals* = net-rs endorser blocks old enough to judge; *cross-implementation certificates* = a certificate produced by one implementation for the other's endorser block.

Run 1's error is not from T24's changes:
- net-rs fetched EB `897/4fcccb76…` at slot 897.
- After it saw EB 1016, the shared-consensus mempool's 100-slot EB retention (`DEFAULT_EB_RETENTION_SLOTS`) pruned that manifest.
- The ranking block carrying EB 897's certificate was applied about 120 slots after the EB, and its manifest was gone.

That retention code is untouched by T24, and run 2 passed on the same binary. The defect is filed as bead `leios_adversarial_tools-r3p`.
