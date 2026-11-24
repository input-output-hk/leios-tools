# Campaign 2026-09-20 — Results Log

Living log. One section per trial. Verdicts follow [`../attack-results.md`](../attack-results.md):
**positive** (goal achieved / disproportionate harm), **negative** (defended /
proportional / noise floor), **partial** (mixed/notable). Every trial is on the
same fresh chain unless a restart is noted.

Adversary = net-rs, a single ~20% forging pool (sub-25%, valid range). Control
via `POST /api/attack/inline` (live behaviour-tree swap on net-node, no chain
restart); measured via `trial.sh`/`probe.sh` against **honest-node ground truth**
(`LeiosKernel.Certified` timing + tip convergence), not net-rs's own sensors.

## Summary table

| Attack (combination) | Class | Verdict | One-line evidence |
|---|---|---|---|
| **dummy-tx-eb** | fake-EB content (fetch+decode) | **POSITIVE — crash** | first dummy EB after a net-rs (re)start crashes **all 3 BPs** (2/2), `Leios.hs:312` `error()` on undecodable tx |
| phantom-tx-eb | fake-EB (unfetchable) | negative | no bodies served → honest never fetch/decode → no crash; cert alive, tips converged |
| dummy-min (n_txs=1) | fake-EB content | negative (this run) | fetched+declined 96×, no crash on a warmed node |
| A1 announce-flood+vote-flood | junk flood | negative (+robustness note) | honest isolate net-rs; chain healthy; invalid announce tears down whole mux conn |
| A2 hollow-eb+fetch-flood | pre-alloc×fetch DoS | negative | honest CPU ~idle under 600 req/slot; 10 MB size-lie → no alloc spike |
| A3 rb-equivocator+cert-suppressor | consensus/fork | negative | tips converged, cert alive 9/9/10 (net-rs self-isolates) |
| B6 tx+announce+vote flood | broad DoS | negative/partial | no fork/crash; cert reduced (net-rs forged 0 EBs under own flood) |
| announce-size-lie | connection reset | negative | some net-rs disconnects; honest cert alive 9/10/10 |
| eb-burst | withhold+burst timing | negative | cert alive 10/10/12, tips converged |
| announce-dangling | announce-without-serve censorship | negative | cert alive 8/6/5, tips converged |
| combo-dummy-fetchflood | crash race-amplification | no amplification | 20 min concurrent load, no crash |

**Legend:** POSITIVE = attack achieved compromise; negative = honest majority
unharmed. BP = block-producing node. "cert alive N/N/N" = honest
`LeiosKernel.Certified` events per node during the trial window (>0 each = Leios
certification continued). "tips converged" = all 3 honest nodes at one identical
tip hash (no safety fork). The single positive is a **decode-path crash**, not a
consensus-margin effect; everything else is defended because honest nodes detect
protocol misbehavior and isolate net-rs.

## Baseline

- **Devnet:** `make smoke SMOKE_FRESH=1` on a clean genesis (3 Haskell pools +
  net-rs 20% + centrifuge).
- **Forge gate:** **PASS** (23:21Z) — full fresh `make smoke` reached
  **147/1200 EB-only txs settled** across 9 net-rs EBs (net-rs forge → Haskell
  vote → certify → embed → apply). Gate is 100.
- **Restart note:** stopping the smoke task also tore down the detached devnet;
  brought it back with `make up-bg` (resumed the just-passed chain, ~10 min old)
  and re-verified forge health (light check, per the restart allowance). Chain
  block 43, net-rs forging, 3 Haskell nodes at tip, 100% vote participation,
  0 faults. The campaign runs on this resumed-fresh chain.
- **Honest baseline sensors (23:24Z):** all badness ≈ 0; `vote_participation`
  100%; `missing_tx_rate` 0; `cert_suppression_rate` 0; `fabricated_eb_certified_rate`
  0; `certificate_equivocation` 0. `rb_reorg_rate` briefly 7 at depth 0 = restart
  re-selection churn (not real forks; `bounded_rollback_violation` depth 0).

## ★ HEADLINE FINDING — dummy-tx-EB crashes the entire honest committee (POSITIVE)

**Verdict: net-cluster = POSITIVE, severe.** A single `dummy-tx-eb` announcement
from the 20% net-rs adversary **crashed all three honest Haskell BP nodes
simultaneously**. Reproduces + extends bead `leios_adversarial_tools-blu`
(fake-EB CBOR panic), now at **`Leios.hs:312`** in the w36 /
ouroboros-consensus-3.0.1.0 build (was :283), and shows it takes down the whole
committee, not one node.

**Timeline (2026-09-21, fresh-ish resumed chain, net-rs EB-only producer):**
- `00:27:57.058` net-rs: `behaviour: announcing DUMMY Tx EB (servable fabricated txs) slot=4923 n_txs=8`
- `00:28:00.035` node1, `00:28:00.035` node2, `00:28:00.039` node3 — **all three crash within 4 ms of each other, ~3 s after the announcement.**
- After: `pgrep cardano-node = 0/3`, all ports down.

**Crash (identical on all three nodes):**
```
cardano-node: ExceptionInLinkedThread "ThreadId ..." Failed to deserialise Leios tx:
  DecoderErrorDeserialiseFailure "Leios Tx" (DeserialiseFailure 1 "expected map len or indef")
  error, called at ouroboros-consensus-cardano/src/shelley/Ouroboros/Consensus/Shelley/Ledger/Leios.hs:312:17
Startup.DiffusionInit.DiffusionErrored
```

**Mechanism.** `dummy-tx-eb` fabricates `n_txs=8` bogus txs and (per the
2026-09-03 offer fix) pins + offers their bodies so honest voters fetch them.
The honest node then decodes the fetched bytes as a Leios tx, the CBOR decode
fails ("expected map len or indef"), and the handler calls `error()` — a partial
function — **inside a linked diffusion thread**, so the exception is unhandled and
the whole node process dies instead of declining the tx. One malformed tx inside
one fetched EB is sufficient; the fetch fans to the whole committee, so all
nodes hit it together.

**Severity.** Total honest-network liveness kill (all BPs down) by a **single
sub-25% adversary** with **one** EB. No stake threshold — it is a
robustness/soundness defect in the Leios tx decode path (`error()` on
attacker-controlled bytes), not a consensus-margin effect. Attribution is
unambiguous: net-rs's own `announcing DUMMY Tx EB` log 3 s before the synchronous
triple crash.

**Fix direction (implementation team):** the Leios-tx deserialise path at
`Leios.hs:312` must return a decline/Left on `DecoderError`, not `error()`; a
peer must never be able to crash a node with undecodable tx bytes inside an EB.

**✅ TRIGGER ISOLATED — the crash fires on the FIRST dummy EB after a net-rs
(re)start (reproducible 2/2) (02:04Z).** The fatal path is not random: it hits on
an honest node's **first contact** with a dummy EB from a freshly-(re)started
net-rs, and stops firing once the node has "warmed up" (thereafter the same
malformed tx is declined gracefully).

- **Confirmed fatal 2/2 on first-dummy-after-restart:**
  - 00:27:57 — first dummy ~3 s after `make inject` (fresh net-node) → all 3 crash 00:28:00.
  - 02:04:37 — first dummy (`dummy_announces=1`, slot 5622) ~10 s after `make
    netrs-down/up` → all 3 crash 02:04:40 (within 11 ms of each other), procs 0/3.
- **Non-fatal on a warmed-up net-node:** ~50 min of continuous dummy (plain,
  n_txs=1, and load-amplified with fetch-flood), tens of announces, **352
  graceful `Failed to deserialise Leios tx` declines on node1 alone, zero
  crashes.**

**Reliable repro recipe:** (1) restart net-rs (fresh net-node); (2) arm
`dummy-tx-eb`; (3) its **first** announced EB crashes the whole honest committee.
Interpretation: on first contact the malformed Leios-tx decode runs in the
initial diffusion path (the linked thread that `error()`s at `Leios.hs:312`);
after warm-up the decode reaches the graceful decline instead. Still fixed the
same way — never `error()` on attacker-supplied bytes — but the trigger is now a
deterministic recipe, not a rare fluke, which makes it both easy to demonstrate
and easy to hit accidentally (any honest node that (re)starts and then meets an
adversary's fabricated EB).

**Cross-ref:** bead `leios_adversarial_tools-blu`; prior musashi crash of live
IOG committee nodes via the same family ([`../attack-results.md`](../attack-results.md), Dummy Tx EB,
2026-09-03). This is the local-devnet reproduction, and it is worse than the
earlier framing: the whole committee, not a subset, and from one EB.

## Crash-surface boundary — phantom-tx-EB does NOT crash (control for the dummy finding)

**Verdict: net-cluster = negative** (no crash, no consensus harm) — and that is
the point: it isolates *why* dummy crashes. Fresh chain, net-rs forging, trial via
`trial.sh` (actuation confirmed, +1 PHANTOM).

- **All 3 honest nodes survived** (procs 3/3, crash-line delta 0/0/0) — vs dummy's
  instant triple crash.
- Certification **stayed alive**: honest `LeiosKernel.Certified` delta 9/6/6 over
  the 170 s window; honest tips converged (all block 29, hash `e7c13acb`).
- The net-rs sensor again cried wolf (`eb_quorum_deficit 19/19`, `rb_reorg 14`)
  purely from net-rs isolation (faults +34) — disproved by the honest ground
  truth above. Confirms the A3 methodology note.

**Boundary established.** phantom pins **no** bodies (unfetchable), so honest
voters decline `MissingTX` and **never fetch/decode** the fabricated txs → no
`Leios.hs:312` decode → no crash. dummy pins **servable** bodies, so honest
voters **do** fetch and decode them → crash. **The crash requires the adversary
to serve decodable-looking-but-malformed tx bytes that a voter actually fetches.**
That is exactly what `dummy-tx-eb` (and `loaded-tx-eb` with garbage payload)
does, and what `phantom` does not. Fix at `Leios.hs:312` (decline, not `error()`)
closes it for the whole family.

## Trials

### A1 — combo-flood-storm (announce-flood 200 + vote-flood 200) — 23:25Z

**Verdict:** net-cluster = **negative** for honest-network compromise; **notable
robustness observation** (invalid Leios announcement tears down the whole peer
connection). Actuation confirmed: net-rs log `emitting phantom vote flood ... count=200`
and `fabricated EB announcement(s) (unelected / no stake) ... count=200` every slot.

**What happened.** The honest majority was unharmed — the chain kept advancing
normally throughout (block 43 → 53, ~10 blocks in 4 min, ~expected rate; 100%
vote participation before churn). The flood's whole effect landed on **net-rs's
own connectivity**: all three honest nodes repeatedly dropped net-rs (~20
disconnect events each) via two mechanisms, both originating from net-rs
(:4010):

1. **ChainSync `ExceededTimeLimit` (mini-protocol 2)** — net-rs, busy fabricating
   200 votes + 200 announcements per slot, failed to serve ChainSync headers
   within the protocol timeout, so honest peers timed out and shut the peer down.
2. **`ReactToAnnouncementError (ErrInvalid HeaderInvalid)` (Leios announcement,
   mini-protocol 18)** — honest nodes validated net-rs's fabricated EB
   announcements, found them invalid, and **terminated the entire multiplexed
   connection**: ChainSync (2), BlockFetch (8), and Leios (18) all shut down
   together with `ShutdownPeer`, not just the offending announcement.

**Interpretation.** Mechanism (2) is the interesting one: a single invalid Leios
announcement collapses *all* protocols with that peer, not only the announcement
stream. As an attack on the honest majority it is **self-defeating** — it only
isolates the adversary (honest nodes correctly reject junk and drop the sender),
and the honest chain is untouched. But it is a **connection-level fragility
worth flagging**: in a relay-fronted topology where a victim depends on the
adversary as its sole upstream (the `DEVNET_VICTIM` shape), one malformed
announcement severs the victim's ChainSync/BlockFetch too, not just its Leios
feed. Escalation candidate for a later trial (fake-announce alone behind a
relay, victim isolation).

**Memory / unbounded-growth:** modest growth over 4 min, all procs alive
(cardano=3, net-node=1): node1 598→659 MB (+61), node2 375→478 MB (+103),
node3 373→481 MB (+108); net-node 44→72 MB. Suggestive of retained
vote/announcement state, but **not runaway** — the honest nodes' own repeated
disconnects (above) discard net-rs's state each time, capping the flood's
footprint. A stealthier low-rate variant that stays *under* the disconnect
threshold is the refinement to test for true unbounded growth.

**Legend:** `announce-flood count` = fabricated EB announcements/slot; `vote-flood
count` = fabricated votes/slot; mini-protocol nums: 2 = ChainSync, 8 = BlockFetch,
18 = Leios announcement; RSS = resident set size.

### A2 — combo-hollow-fetch (hollow-eb 10 MB declared + fetch-flood rate 20) — 23:31Z

**Verdict:** net-cluster = **negative.** Actuation confirmed: net-rs log
`fetch-flood re-requesting announced EBs (T22) ... requests=500–600 targets=5–6
rate=20 capped=false` every slot; 2 hollow-EB announcements in the window.

**What happened.** Honest nodes absorbed it with no distress over 4 min:

| node | RSS t0 → t+240s | %CPU |
|---|---|---|
| node1 (BP, net-rs peer) | 676 → 712 MB (+36) | ~9–10 |
| node2 | 520 → 548 MB (+28) | <1 |
| node3 | 513 → 546 MB (+33) | <1 |

Chain advanced 10 blocks in 4 min (healthy); all 3 nodes alive.

**Interpretation.** Two would-be levers both fail: (1) the **fetch-flood** (600
requests/slot fanned to all peers) does **not** saturate honest CPU — node1 sat
at ~10%, the others idle — so the honest serve loop caps/handles repeated
requests cheaply. (2) the **hollow-EB's 10 MB false declared size** caused **no
allocation spike** on receivers — memory drift stayed flat and bounded, so the
honest node does **not** pre-allocate a fetch buffer from the advertised
`eb_size` (the exact defense the probe targets), and/or rejects the hollow
announcement outright as in A1. Consistent with the prior phantom-tx-eb result:
the Haskell nodes defend correctly and the adversary self-isolates via disconnect
churn. Refinement if revisited: sweep `declared_bytes` to GB scale, but with
honest rejection + only ~2 announces/window the expected value is low.

**Legend:** `fetch-flood rate` = LeiosFetch re-requests/sec per in-window EB;
`declared_bytes` = false EB size advertised by hollow-eb; `%CPU` = per-process
CPU at the sample instant.

### A3 — combo-equiv-certsupp (rb-header-equivocator ways=2 + cert-suppressor) — 23:37Z

**Verdict:** net-cluster = **negative** for consensus compromise. **Caught a
false alarm** — the net-rs sensor initially read total certification collapse; the
honest ground truth disproves it.

**The trap.** During A3 the net-rs `/api/sensors` showed **eb_quorum_deficit
19/19 EBs missed quorum (badness 1.0)**, `eb_certifying 0/19`, and
`rb_reorg_rate 17 rollbacks/block` — a picture that looks exactly like a
consensus break. It is not. net-rs logged **173 faults**: the equivocated RB
header variants make the honest nodes reject net-rs and disconnect it, so net-rs
is **blind to the honest votes/certs** and reports a false deficit (the exact
isolation-artifact CLAUDE.md and `attack-results.md` warn about).

**Honest ground truth disproves harm.**
- **Tips converged:** all three honest nodes at the identical tip (block 95,
  hash `c05bcd56`) — no safety fork; the equivocation does not split the honest set.
- **Certification never stopped:** honest `LeiosKernel.Certified` events per minute
  ran at the **same rate through A3 (23:37–23:44) as at baseline (23:25–23:36)** —
  node1 e.g. 3,2,4,1,1,4,3,2/min during A3 vs 2–7/min before; node2/node3 likewise.
- Chain advanced normally (block 76 → 95).

**Interpretation.** A 20% header-equivocator self-isolates (honest nodes detect
conflicting headers and drop it); `cert-suppressor` contributes nothing because
net-rs, disconnected, rarely produces the child RB of a quorum'd EB and can't see
quorum anyway. Consistent with the prior negative reorg-amplified-cert-suppression
result. **Methodology confirmed for the rest of the campaign: net-rs
`eb_quorum_deficit` is untrustworthy whenever net-rs is disconnected — always
confirm cert liveness from honest `LeiosKernel.Certified` timing.**

**Legend:** `ways` = RB header variants per won slot; `LeiosKernel.Certified` =
honest-node event marking an EB reaching a certificate; `faults` = net-rs
disconnect/error log lines.

> **CORRECTION (23:52Z): A3 and A4 did not actuate — results above for A3 are
> void.** Diagnosis: the last behaviour-actuation marker of *any* kind is A2's at
> 23:32. After that, net-node applied **no** tree swap — a control-channel wedge
> (`tx-flood` at rate 1000 left net-rs's mempool at 7 txs; A3/A4 produced zero
> equivocation/dummy/reorg markers). Compounding it, net-rs had **no tx source**
> (post-inject config `tx_rate=0, tx_source=null`; the centrifuge floods the
> Haskell nodes, not net-rs), so it produced only 3 RBs / 2 EBs total — the
> production-path actions (equivocator, cert-suppressor, deep-reorg, dummy-tx-eb,
> announce-size-lie) had no substrate. The "173 faults / 19-of-19 quorum miss /
> 17 rollbacks" seen during A3 were ambient net-rs churn, **not** the equivocator.
> **Only A1 and A2 are valid** (both production-independent — fabricated
> floods/fetch fire every tick). **Fix for the re-run:** restart net-rs with a
> tx-source magazine (EB-only inject) so it forges EBs + regains a live control
> channel, and run production-path attacks **before** the heavy floods (the A1
> flood is the likely trigger of net-rs isolation/wedge).

> **A3 RE-RUN (valid, 00:42Z, fresh chain, net-rs forging, ACTUATED +4):
> confirmed NEGATIVE.** Certification alive throughout (honest cert delta
> 9/9/10), honest tips converged (block 41, hash `fa306a9c`), 3/3 nodes alive.
> RB-header equivocation self-isolates net-rs (faults, `rb_reorg` churn, the
> `19/19` sensor artifact) but never forks the honest majority nor stops honest
> certification. This supersedes the void A3 above and matches the campaign
> pattern: honest nodes detect + isolate protocol misbehavior; the sole fatal
> seam is the tx-**decode** path (dummy).

### B6 — combo-triple-flood (tx-flood 2000 + announce-flood 300 + vote-flood 300) — 00:46Z

**Verdict: net-cluster = negative/partial.** ACTUATED +345 flood markers.
Honest consensus intact: tips converged (block 51, hash `3fd50e436f84`), 3/3
alive, no crash. Certification continued but at a **reduced rate** (honest cert
delta 4/4/4 vs ~9 in the calmer A3 window). That drop is largely **net-rs
self-isolation** — under its own 900-msg/slot flood net-rs forged **0 EBs**
(faults +211), cutting total EB supply — plus some honest processing load; it is
not an honest-side failure. No fork, no cert stop, no crash. Broadest DoS in the
palette still does not compromise the honest majority.

**Legend:** cert delta = honest `LeiosKernel.Certified` count change over the
170 s window per node; NO-PRODUCTION = net-rs forged 0 EBs during the trial.

### announce-size-lie (scale_num=0) — 00:50Z

**Verdict: net-cluster = negative.** ACTUATED +48, net-rs produced 3 EBs.
Certification fully alive (honest cert delta 9/10/10), tips converged (block 60,
hash `424cba2c`), 3/3 alive, no crash. Causes some net-rs disconnects (faults
+78 — the connection-reset mechanism seen on musashi) but the honest majority
absorbs it and keeps certifying. On this local devnet it is not a compromise.

### dummy-tx-EB rarity quantification (01:01–01:32Z, sustained n_txs=8)

30 min of continuously-armed dummy on the fresh devnet: **15 dummy announces,
324 honest deserialise-failures on node1 alone, ZERO further crashes.** Nodes
3/3 alive, tips converged (block 132), certifying at 01:32. Confirms the fatal
`Leios.hs:312` path is **rare** — graceful decline dominates. The one fatal
event (00:27) is real and unambiguous but low-probability per announce under
these conditions. Next: amplify concurrent decode load (dummy + fetch-flood) to
test whether the race is load-triggerable.

### combo-dummy-fetchflood (race-amplification probe) — 01:33Z

**No amplification.** 20 min of dummy(n_txs=8) + fetch-flood(rate 30) under
concurrent decode load produced **no fatal crash**; honest nodes stayed up and
kept certifying. Concurrent LeiosFetch pressure does not obviously raise the
fatal-path probability. Cumulative: the fatal `Leios.hs:312` crash has fired
**once** across ~50 min of continuous dummy (plain + load-amplified), many tens
of announces and hundreds of graceful decode-declines. Next hypothesis:
first-dummy-after-net-rs-restart (the 00:27 crash was ~3 s after a fresh net-rs
inject).

### combo-drop-dummy (connection-freshness crash probe) — 02:19Z

**Verdict: inconclusive (self-defeating).** `drop-inbound-peers(probability=1.0)`
fully isolated net-rs, so it announced **0** dummy EBs and honest nodes saw **0**
deserialise attempts — the dummy never reached the committee. Confirms the
connection-freshness hypothesis needs a *reconnect-without-isolation* primitive
(force honest peers to re-establish while net-rs stays reachable), which the
current palette lacks. Follow-up: a lower drop probability, or a dedicated
"reset-connections" action, to test whether a fresh honest→net-rs connection
(not a net-rs process restart) is the true trigger. Devnet unharmed (3/3 alive).

### lazy-voter — 02:55Z (inconclusive/negative, substrate-confounded)

net-rs abstains from voting. Nodes healthy (3/3, no crash, no fork). Honest cert
delta low (2/1/1) and `vote_participation` → "no vote rounds in window", but this
reflects **net-rs not producing EBs** in the window (magazine likely drained),
not abstention harm — with honest stake 80% > 0.75 quorum, net-rs's abstention
cannot block certification. Clean attribution needs sustained net-rs EB
production (diary #4/#11). Recorded as negative with that caveat. **This marks
the productive frontier for the current tooling** — remaining single behaviors
(withhold-txs, drop-inbound alone, long-range-fork) give confounded results
without the substrate-control + actuation-confirmation fixes in the diary.
