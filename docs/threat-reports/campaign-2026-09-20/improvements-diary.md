# Tooling Improvements Diary — campaign 2026-09-20

Running list of stability + efficacy gaps found while driving the campaign.
Grouped, most-impactful first. To be actioned at the next tooling break.

## Stability (things that broke or misled)

1. **net-node control channel wedges after a heavy flood.** After A1
   (announce-flood 200 + vote-flood 200/slot, ~400 fabricated msgs/slot for ~6
   min), net-node stopped applying **any** subsequent `load_tree`/`go_honest`
   command (verified: tx-flood rate=1000 left mempool at 7 txs; A3/A4 never
   actuated). net-cluster kept logging "Pushing command … to node-0" and the API
   returned 202 / "applied=[0]", but nothing took effect. **Impact:** silent
   dead-actuation for the rest of the run; wasted ~4 attacks before caught.
   **Fix ideas:** (a) net-node should ACK applied trees and net-cluster surface a
   real per-node applied/failed status (today "applied" = "dispatched", a lie);
   (b) make the stdin command reader robust to backpressure during floods (bound
   the outbound fabricated-message queue so it can't starve the command reader);
   (c) a heartbeat/liveness field in telemetry for the BT control plane.

2. **`/api/attack/inline` returns 202 without confirming actuation.** The API
   validates TOML and queues, but success ≠ the node ran it. **Fix:** an
   actuation-confirmed response (poll until the node reports the tree active), or
   a `GET /api/attack` field showing the node-confirmed active tree hash + last
   tick that referenced it.

3. **Stopping the `make smoke` background task tore down the whole detached
   devnet** (Haskell nodes + net-cluster + centrifuge), not just the smoke poll
   loop — process-group kill reached the nohup'd children. **Impact:** lost the
   just-passed fresh chain; had to restart. **Fix:** `make up`/`smoke` should
   fully detach the devnet (setsid/new session) so a harness `TaskStop` on the
   smoke can't reap the cluster; or a `make smoke-nowait` that exits on PASS
   leaving the devnet up.

4. **net-rs has no tx source after `make inject` completes / on plain `make
   up`** (`tx_rate=0, tx_source=null`), and the centrifuge feeds the Haskell
   nodes, not net-rs. So net-rs's mempool empties and it stops producing EBs
   (and mostly RBs), silently no-op'ing every production-path attack. **Fix:** a
   campaign mode that keeps net-rs fed continuously (persistent tx_source, or
   route centrifuge txs to net-rs, or a built-in low-rate self-generator), so
   production-path attacks always have substrate.

## Efficacy (measurement / workflow)

5. **`eb_quorum_deficit` (and cert sensors) are untrustworthy when net-rs is
   isolated.** A disconnected net-rs is blind to honest votes/certs and reports a
   false total quorum collapse (looked exactly like a consensus break in A3).
   **Fix:** a sensor that measures cert liveness from **honest-node** ground
   truth (`LeiosKernel.Certified` timing), and an explicit "adversary isolated"
   signal so the deficit can be discounted. The honest-tip-convergence check
   (all honest tips == same hash) should be a first-class sensor too.

6. **No single actuation-confirmed trial harness.** Each attack needed ad-hoc
   bash: fire → confirm actuation → soak → capture honest cert timing + tip
   convergence + memory + crash → stop → record. **Fix:** one `run-trial.sh
   <tree> <soak>` that does all of it and appends a structured row, refusing to
   score a trial whose actuation was never confirmed (would have caught A3/A4
   immediately).

7. **Inline Python in bash keeps breaking on f-string quote escaping** (hit 3×).
   **Fix:** all JSON/sensor parsing via committed `*.py` helpers (done for
   `sensors.py`; extend to mempool/production/cert-timing helpers).

8. **Honest-node fault monitors storm** (one notification per disconnect ×3
   nodes ×~3s). **Fix:** a summarizing watcher that emits a rate-limited rollup
   ("N disconnects on nodes X in last 30s") and only escalates on
   crash/panic/OOM signatures.

9. **net-rs under-produces RBs on an empty mempool** (forged only ~3 RBs in 35
   min vs ~20 expected for 20% stake). Is empty-block forging suppressed by
   design? If so, any test needing net-rs blocks must guarantee mempool content.
   Worth confirming and documenting.

## Notes for method

- Run **production-path attacks before heavy floods** — the flood isolates
  net-rs and appears to trigger the control-channel wedge, contaminating
  everything after.
- Always confirm cert liveness from honest `LeiosKernel.Certified` timing +
  honest-tip convergence, never from net-rs's `eb_quorum_deficit` alone.

## Added after the dummy-tx-EB crash finding (00:28Z)

10. **Crash-inducing attacks destroy the devnet and force a full restart
    (~7–10 min each).** `dummy-tx-eb` kills all 3 honest nodes from one EB, so any
    trial containing it (A4, loaded-tx-eb, and any dummy combo) is one-shot: run
    it, confirm the crash, then rebuild. **Fix/process:** batch all crash-class
    attacks to the END of a devnet's life; keep a fast `make fresh` path;
    consider a single-honest-node "canary" devnet for crash-class screening so a
    full 3-node rebuild isn't needed to confirm a decode crash.

11. **`tx-flood` action generates 0 txs when net-rs is in EB-only inject mode
    (`provide_txs=false`).** So it cannot be used to self-sustain production after
    an inject. **Fix:** either make `tx-flood` independent of `provide_txs`, or
    document that sustained net-rs production needs `provide_txs=true` +
    `tx_rate>0` (self-gen) — but see #12.

12. **Risk: net-rs self-generated txs may themselves be undecodable by the
    Haskell nodes** (same `Leios.hs:312` path), i.e. running net-rs honestly with
    a high self-gen `tx_rate` might crash the committee without any attack.
    Unverified but plausible given the dummy crash. **Action:** verify whether
    net-rs's own generated txs are ledger/CBOR-valid Leios txs; if not, sustained
    honest production requires a real-tx magazine, and the crash surface is even
    broader than the fake-EB family.

13. **A healthy fresh net-rs forges RBs every ~20s even with an empty mempool;**
    a wedged/isolated net-rs (post-flood) forges almost nothing. RB production —
    not mempool — is what most production-path *fabrication* attacks need
    (equivocator, dummy/phantom/hollow fake-EBs, size-lie), so a fresh devnet is
    sufficient substrate for them without any tx feeding. Only honest-EB-overflow
    tests need real mempool txs.

14. **`trial.sh` works and would have caught A3/A4** (it scores actuation from a
    marker delta and refuses to credit an unconfirmed trial). Keep hardening it:
    add honest-tip-convergence pass/fail, memory sampling, and an
    auto-`make fresh` when it detects `procs<3` (crash) so the campaign
    self-heals.

## Added after the intermittency discovery (01:00Z)

15. **The dummy-tx-EB crash is INTERMITTENT — we need a repro harness that hunts
    the race.** Malformed Leios txs are usually declined gracefully (logged
    "Failed to deserialise Leios tx", node survives — seen 140× on one node);
    the fatal `error()` at `Leios.hs:312` in a linked diffusion thread fires only
    sometimes, but when it does it kills the whole committee at once. **Need:** a
    dedicated repro loop that keeps a dummy-EB armed for a long window on a
    disposable devnet and records (announces, fatal-crashes, graceful-declines)
    to estimate the per-announce crash probability and, ideally, correlate the
    fatal path with a concurrency/timing condition. A single-honest-node canary
    (diary #10) makes this cheap to iterate.
16. **Two decode paths for the same bytes must be found in the Haskell source.**
    One returns/declines (graceful); one calls `error()` (fatal, linked thread).
    Isolating which diffusion/fetch/vote context takes the fatal branch is the
    key to a minimal deterministic repro and to the fix. (leios-tools/Haskell
    consensus, `Ouroboros.Consensus.Shelley.Ledger.Leios`.)
17. **Trials that depend on net-rs's blocks being ADOPTED are all defeated by
    self-isolation** (A1/A2/A3/B6/size-lie/phantom all negative): honest nodes
    detect misbehavior and drop net-rs, so its equivocation/floods/size-lies
    never harm the honest majority. The productive attack class is the one where
    honest nodes must **fetch + decode adversary bytes before they can reject
    them** — i.e. the fake-EB *content* path. Future search should concentrate
    there (malformed tx bodies, malformed EB bodies, malformed vote/cert bytes)
    rather than on protocol-shape misbehavior.
